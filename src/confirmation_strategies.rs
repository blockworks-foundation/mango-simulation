use std::{
    str::FromStr,
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};

use chrono::Utc;
use dashmap::DashMap;
use log::debug;
use solana_client::{nonblocking::rpc_client::RpcClient, rpc_config::RpcBlockConfig};
use solana_sdk::{
    commitment_config::{CommitmentConfig, CommitmentLevel},
    signature::Signature,
};
use solana_transaction_status::{RewardType, UiConfirmedBlock};

use crate::states::{BlockData, TransactionConfirmRecord, TransactionSendRecord};

use async_channel::Sender;
use tokio::{sync::mpsc::UnboundedReceiver, task::JoinHandle, time::Instant};

pub async fn process_blocks(
    block: UiConfirmedBlock,
    tx_confirm_records: Sender<TransactionConfirmRecord>,
    tx_block_data: Sender<BlockData>,
    transaction_map: Arc<DashMap<Signature, (TransactionSendRecord, Instant)>>,
    slot: u64,
) {
    let mut mm_transaction_count: u64 = 0;
    let rewards = &block.rewards.unwrap();
    let slot_leader = match rewards
        .iter()
        .find(|r| r.reward_type == Some(RewardType::Fee))
    {
        Some(x) => x.pubkey.clone(),
        None => "".to_string(),
    };

    if let Some(transactions) = block.transactions {
        let nb_transactions = transactions.len();
        let mut cu_consumed: u64 = 0;
        for solana_transaction_status::EncodedTransactionWithStatusMeta {
            transaction, meta, ..
        } in transactions
        {
            if let solana_transaction_status::EncodedTransaction::Json(transaction) = transaction {
                for signature in transaction.signatures {
                    let signature = Signature::from_str(&signature).unwrap();

                    let transaction_record_op = {
                        let rec = transaction_map.get(&signature);
                        match rec {
                            Some(x) => Some(x.clone()),
                            None => None,
                        }
                    };
                    // add CU in counter
                    if let Some(meta) = &meta {
                        match meta.compute_units_consumed {
                            solana_transaction_status::option_serializer::OptionSerializer::Some(x) => {
                                cu_consumed = cu_consumed.saturating_add(x);
                            },
                            _ => {},
                        }
                    }
                    if let Some(transaction_record) = transaction_record_op {
                        let transaction_record = transaction_record.0;
                        mm_transaction_count += 1;

                        let _ = tx_confirm_records.send(TransactionConfirmRecord {
                            signature: transaction_record.signature.to_string(),
                            confirmed_slot: Some(slot),
                            confirmed_at: Some(Utc::now().to_string()),
                            sent_at: transaction_record.sent_at.to_string(),
                            sent_slot: transaction_record.sent_slot,
                            successful: if let Some(meta) = &meta {
                                meta.status.is_ok()
                            } else {
                                false
                            },
                            error: if let Some(meta) = &meta {
                                meta.err.as_ref().map(|x| x.to_string())
                            } else {
                                None
                            },
                            block_hash: Some(block.blockhash.clone()),
                            market: transaction_record.market.to_string(),
                            market_maker: transaction_record.market_maker.to_string(),
                            slot_processed: Some(slot),
                            slot_leader: Some(slot_leader.clone()),
                            timed_out: false,
                            priority_fees: transaction_record.priority_fees,
                        });
                    }

                    transaction_map.remove(&signature);
                }
            }
        }
        // push block data
        {
            let _ = tx_block_data.send(BlockData {
                block_hash: block.blockhash,
                block_leader: slot_leader,
                block_slot: slot,
                block_time: if let Some(time) = block.block_time {
                    time as u64
                } else {
                    0
                },
                number_of_mm_transactions: mm_transaction_count,
                total_transactions: nb_transactions as u64,
                cu_consumed: cu_consumed,
            });
        }
    }
}

pub fn confirmations_by_blocks(
    client: Arc<RpcClient>,
    mut tx_record_rx: UnboundedReceiver<TransactionSendRecord>,
    tx_confirm_records: Sender<TransactionConfirmRecord>,
    tx_block_data: Sender<BlockData>,
    from_slot: u64,
) -> Vec<JoinHandle<()>> {
    let transaction_map = Arc::new(DashMap::new());
    let do_exit = Arc::new(AtomicBool::new(false));

    let map_filler_jh = {
        let transaction_map = transaction_map.clone();
        let do_exit = do_exit.clone();
        tokio::spawn(async move {
            loop {
                match tx_record_rx.recv().await {
                    Some(tx_record) => {
                        debug!(
                            "add to queue len={} sig={}",
                            transaction_map.len() + 1,
                            tx_record.signature
                        );
                        transaction_map.insert(tx_record.signature, (tx_record, Instant::now()));
                    }
                    None => {
                        do_exit.store(true, Ordering::Relaxed);
                        break;
                    }
                }
            }
        })
    };

    let cleaner_jh = {
        let transaction_map = transaction_map.clone();
        let do_exit = do_exit.clone();
        let tx_confirm_records = tx_confirm_records.clone();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_secs(60)).await;
                {
                    transaction_map.retain(|signature, (sent_record, instant)| {
                        let retain = instant.elapsed() > Duration::from_secs(120);

                        // add to timeout if not retaining
                        if !retain {
                            let _ = tx_confirm_records.send(TransactionConfirmRecord {
                                signature: signature.to_string(),
                                confirmed_slot: None,
                                confirmed_at: None,
                                sent_at: sent_record.sent_at.to_string(),
                                sent_slot: sent_record.sent_slot,
                                successful: false,
                                error: Some("timeout".to_string()),
                                block_hash: None,
                                market: sent_record.market.to_string(),
                                market_maker: sent_record.market_maker.to_string(),
                                slot_processed: None,
                                slot_leader: None,
                                timed_out: true,
                                priority_fees: sent_record.priority_fees,
                            });
                        }

                        retain
                    });

                    // if exit and all the transactions are processed
                    if do_exit.load(Ordering::Relaxed) && transaction_map.len() == 0 {
                        break;
                    }
                }
            }
        })
    };

    let block_confirmation_jh = {
        let do_exit = do_exit.clone();
        tokio::spawn(async move {
            let mut start_block = from_slot;
            let mut start_instant = tokio::time::Instant::now();
            let refresh_in = Duration::from_secs(10);
            let commitment_confirmation = CommitmentConfig {
                commitment: CommitmentLevel::Confirmed,
            };
            loop {
                if do_exit.load(Ordering::Relaxed) && transaction_map.len() == 0 {
                    break;
                }

                let wait_duration = tokio::time::Instant::now() - start_instant;
                if wait_duration < refresh_in {
                    tokio::time::sleep(refresh_in - wait_duration).await;
                }
                start_instant = tokio::time::Instant::now();

                let block_slots = client
                    .get_blocks_with_commitment(start_block, None, commitment_confirmation)
                    .await
                    .unwrap();
                if block_slots.is_empty() {
                    continue;
                }
                start_block = *block_slots.last().unwrap();

                let blocks = block_slots.iter().map(|slot| {
                    client.get_block_with_config(
                        *slot,
                        RpcBlockConfig {
                            encoding: None,
                            transaction_details: None,
                            rewards: None,
                            commitment: Some(commitment_confirmation),
                            max_supported_transaction_version: None,
                        },
                    )
                });
                let blocks = futures::future::join_all(blocks).await;
                for block_slot in blocks.iter().zip(block_slots) {
                    let block = match block_slot.0 {
                        Ok(x) => x,
                        Err(_) => continue,
                    };
                    let tx_confirm_records = tx_confirm_records.clone();
                    let tx_block_data = tx_block_data.clone();
                    let transaction_map = transaction_map.clone();
                    process_blocks(
                        block.clone(),
                        tx_confirm_records,
                        tx_block_data,
                        transaction_map,
                        block_slot.1,
                    )
                    .await;
                }
            }
        })
    };
    vec![map_filler_jh, cleaner_jh, block_confirmation_jh]
}
