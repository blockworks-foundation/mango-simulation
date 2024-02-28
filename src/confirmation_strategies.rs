use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc,
    },
    time::Duration,
};

use chrono::Utc;
use dashmap::DashMap;
use log::{debug, warn};
use solana_client::{nonblocking::rpc_client::RpcClient, rpc_config::RpcBlockConfig};
use solana_lite_rpc_core::notifications::NotificationMsg;
use solana_sdk::{
    commitment_config::{CommitmentConfig, CommitmentLevel},
    signature::Signature,
    slot_history::Slot,
};
use solana_transaction_status::{
    option_serializer::OptionSerializer, RewardType, TransactionDetails, UiConfirmedBlock,
    UiTransactionEncoding,
};

use crate::states::{BlockData, TransactionConfirmRecord, TransactionSendRecord};

use tokio::{
    sync::broadcast::Sender, sync::mpsc::UnboundedReceiver, task::JoinHandle, time::Instant,
};

pub async fn process_blocks(
    block: &UiConfirmedBlock,
    tx_confirm_records: Sender<TransactionConfirmRecord>,
    tx_block_data: Sender<BlockData>,
    transaction_map: Arc<DashMap<Signature, (TransactionSendRecord, Instant)>>,
    slot: u64,
    commitment: CommitmentLevel,
) {
    let mut mm_transaction_count: u64 = 0;
    let rewards = block.rewards.as_ref().unwrap();
    let slot_leader = match rewards
        .iter()
        .find(|r| r.reward_type == Some(RewardType::Fee))
    {
        Some(x) => x.pubkey.clone(),
        None => "".to_string(),
    };

    if let Some(transactions) = &block.transactions {
        let nb_transactions = transactions.len();
        let mut mm_cu_consumed: u64 = 0;
        let mut total_cu_consumed: u64 = 0;
        for solana_transaction_status::EncodedTransactionWithStatusMeta {
            transaction, meta, ..
        } in transactions
        {
            let tx_cu_consumed =
                meta.as_ref()
                    .map_or(0, |meta| match meta.compute_units_consumed {
                        OptionSerializer::Some(cu_consumed) => cu_consumed,
                        _ => 0,
                    });
            let transaction = match transaction.decode() {
                Some(tx) => tx,
                None => {
                    continue;
                }
            };
            for signature in &transaction.signatures {
                // add CU in counter
                total_cu_consumed = total_cu_consumed.saturating_add(tx_cu_consumed);
                if let Some((_, (transaction_record, _))) = transaction_map.remove(signature) {
                    mm_transaction_count += 1;
                    mm_cu_consumed = mm_cu_consumed.saturating_add(tx_cu_consumed);

                    match tx_confirm_records.send(TransactionConfirmRecord {
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
                        market: transaction_record.market.map(|x| x.to_string()),
                        market_maker: transaction_record.market_maker.map(|x| x.to_string()),
                        keeper_instruction: transaction_record.keeper_instruction,
                        slot_processed: Some(slot),
                        slot_leader: Some(slot_leader.clone()),
                        timed_out: false,
                        priority_fees: transaction_record.priority_fees,
                    }) {
                        Ok(_) => {}
                        Err(e) => {
                            warn!("Tx confirm record channel broken {}", e.to_string());
                        }
                    }
                }
            }
        }
        // push block data
        {
            let _ = tx_block_data.send(BlockData {
                block_hash: block.blockhash.clone(),
                block_leader: slot_leader,
                block_slot: slot,
                block_time: if let Some(time) = block.block_time {
                    time as u64
                } else {
                    0
                },
                number_of_mango_simulation_txs: mm_transaction_count,
                total_transactions: nb_transactions as u64,
                cu_consumed: total_cu_consumed,
                cu_consumed_by_mango_simulations: mm_cu_consumed,
                commitment,
            });
        }
    }
}

async fn get_blocks_with_retry(
    client: Arc<RpcClient>,
    start_block: u64,
    commitment_confirmation: CommitmentConfig,
) -> Result<Vec<Slot>, ()> {
    const N_TRY_REQUEST_BLOCKS: u64 = 4;
    for _ in 0..N_TRY_REQUEST_BLOCKS {
        let block_slots = client
            .get_blocks_with_commitment(start_block, None, commitment_confirmation)
            .await;

        match block_slots {
            Ok(slots) => {
                return Ok(slots);
            }
            Err(error) => {
                warn!("Failed to download blocks: {}, retry", error);
            }
        }
    }
    Err(())
}

pub fn confirmation_by_lite_rpc_notification_stream(
    tx_record_rx: UnboundedReceiver<TransactionSendRecord>,
    notification_stream: UnboundedReceiver<NotificationMsg>,
    tx_confirm_records: tokio::sync::broadcast::Sender<TransactionConfirmRecord>,
    tx_block_data: tokio::sync::broadcast::Sender<BlockData>,
    exit_signal: Arc<AtomicBool>,
) -> Vec<JoinHandle<()>> {
    let transaction_map: Arc<DashMap<String, (TransactionSendRecord, Instant)>> =
        Arc::new(DashMap::new());

    let confirming_task = {
        let transaction_map = transaction_map.clone();
        let tx_confirm_records = tx_confirm_records.clone();
        let exit_signal = exit_signal.clone();
        tokio::spawn(async move {
            let mut tx_record_rx = tx_record_rx;
            let mut notification_stream = notification_stream;

            while !transaction_map.is_empty() || !exit_signal.load(Ordering::Relaxed) {
                tokio::select! {
                    transaction_record = tx_record_rx.recv() => {
                        if let Some(transaction_record) = transaction_record{
                            transaction_map
                            .insert(transaction_record.signature.to_string(), (transaction_record, Instant::now()));
                        }

                    },
                    notification = notification_stream.recv() => {
                        if let Some(notification) = notification {

                            match notification {
                                NotificationMsg::BlockNotificationMsg(block_notification) => {
                                    if block_notification.commitment != CommitmentLevel::Finalized {
                                        continue;
                                    }
                                    let _ = tx_block_data.send(BlockData {
                                        block_hash: block_notification.blockhash.to_string(),
                                        block_leader: block_notification.block_leader,
                                        block_slot: block_notification.slot,
                                        block_time: block_notification.block_time,
                                        number_of_mango_simulation_txs: block_notification.transaction_found,
                                        total_transactions: block_notification.total_transactions,
                                        cu_consumed: block_notification.total_cu_consumed,
                                        cu_consumed_by_mango_simulations: block_notification.cu_consumed_by_txs,
                                        commitment: block_notification.commitment,
                                    });
                                }
                                NotificationMsg::UpdateTransactionMsg(tx_update_notifications) => {

                                    for tx_notification in tx_update_notifications {
                                        if tx_notification.commitment != CommitmentLevel::Finalized {
                                            continue;
                                        }

                                        if let Some(value) = transaction_map.get(&tx_notification.signature) {
                                            let (tx_sent_record, _) = value.clone();
                                            let error = match &tx_notification.transaction_status {
                                                Err(e) => {
                                                    Some(e.to_string())
                                                },
                                                _ => None
                                            };
                                            let _ = tx_confirm_records.send(TransactionConfirmRecord {
                                                signature: tx_notification.signature.clone(),
                                                confirmed_slot: Some(tx_notification.slot),
                                                confirmed_at: Some(Utc::now().to_string()),
                                                sent_at: tx_sent_record.sent_at.to_string(),
                                                sent_slot: tx_sent_record.sent_slot,
                                                successful: tx_notification.transaction_status.is_ok(),
                                                error,
                                                block_hash: Some(tx_notification.blockhash),
                                                market: tx_sent_record.market.map(|x| x.to_string()),
                                                market_maker: tx_sent_record.market_maker.map(|x| x.to_string()),
                                                keeper_instruction: tx_sent_record.keeper_instruction.clone(),
                                                slot_processed: Some(tx_notification.slot),
                                                slot_leader: Some(tx_notification.leader.to_string()),
                                                timed_out: false,
                                                priority_fees: tx_sent_record.priority_fees,
                                            });
                                        }

                                        transaction_map.remove(&tx_notification.signature);
                                    }
                                },
                                _ => {
                                    // others do nothing
                                }
                            }
                        }
                    },
                    _ = tokio::time::sleep(Duration::from_secs(1)) => {
                        // timeout
                        continue;
                    }
                }
            }
            log::info!("stopped processing the transactions");
        })
    };

    let cleaner_jh = {
        let transaction_map = transaction_map;
        let exit_signal = exit_signal;
        let tx_confirm_records = tx_confirm_records;
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_secs(60)).await;
                {
                    let mut to_remove = vec![];

                    for tx_data in transaction_map.iter() {
                        let sent_record = &tx_data.0;
                        let instant = tx_data.1;
                        let signature = tx_data.key();
                        let remove = instant.elapsed() > Duration::from_secs(120);

                        // add to timeout if not retaining
                        if remove {
                            let _ = tx_confirm_records.send(TransactionConfirmRecord {
                                signature: signature.to_string(),
                                confirmed_slot: None,
                                confirmed_at: None,
                                sent_at: sent_record.sent_at.to_string(),
                                sent_slot: sent_record.sent_slot,
                                successful: false,
                                error: Some("timeout".to_string()),
                                block_hash: None,
                                market: sent_record.market.map(|x| x.to_string()),
                                market_maker: sent_record.market_maker.map(|x| x.to_string()),
                                keeper_instruction: sent_record.keeper_instruction.clone(),
                                slot_processed: None,
                                slot_leader: None,
                                timed_out: true,
                                priority_fees: sent_record.priority_fees,
                            });
                            to_remove.push(signature.clone());
                        }
                    }

                    for signature in to_remove {
                        transaction_map.remove(&signature);
                    }

                    // if exit and all the transactions are processed
                    if exit_signal.load(Ordering::Relaxed) && transaction_map.is_empty() {
                        break;
                    }
                }
            }
        })
    };
    vec![confirming_task, cleaner_jh]
}

#[deprecated]
pub fn confirmations_by_blocks(
    client: Arc<RpcClient>,
    mut tx_record_rx: UnboundedReceiver<TransactionSendRecord>,
    tx_confirm_records: tokio::sync::broadcast::Sender<TransactionConfirmRecord>,
    tx_block_data: tokio::sync::broadcast::Sender<BlockData>,
    from_slot: u64,
    exit_signal: Arc<AtomicBool>,
) -> Vec<JoinHandle<()>> {
    let transaction_map = Arc::new(DashMap::new());

    let map_filler_jh = {
        let transaction_map = transaction_map.clone();
        let exit_signal = exit_signal.clone();
        tokio::spawn(async move {
            loop {
                match tokio::time::timeout(tokio::time::Duration::from_secs(1), tx_record_rx.recv())
                    .await
                {
                    Ok(tx_record) => match tx_record {
                        Some(tx_record) => {
                            debug!(
                                "add to queue len={} sig={}",
                                transaction_map.len() + 1,
                                tx_record.signature
                            );
                            transaction_map
                                .insert(tx_record.signature, (tx_record, Instant::now()));
                        }
                        None => {
                            exit_signal.store(true, Ordering::Relaxed);
                            break;
                        }
                    },
                    Err(_) => {
                        // on timeout just check if services are being stopped
                        if exit_signal.load(Ordering::Relaxed) {
                            break;
                        }
                    }
                }
            }
        })
    };

    let cleaner_jh = {
        let transaction_map = transaction_map.clone();
        let exit_signal = exit_signal.clone();
        let tx_confirm_records = tx_confirm_records.clone();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_secs(60)).await;
                {
                    let mut to_remove = vec![];

                    for tx_data in transaction_map.iter() {
                        let sent_record = &tx_data.0;
                        let instant = tx_data.1;
                        let signature = tx_data.key();
                        let remove = instant.elapsed() > Duration::from_secs(120);

                        // add to timeout if not retaining
                        if remove {
                            let _ = tx_confirm_records.send(TransactionConfirmRecord {
                                signature: signature.to_string(),
                                confirmed_slot: None,
                                confirmed_at: None,
                                sent_at: sent_record.sent_at.to_string(),
                                sent_slot: sent_record.sent_slot,
                                successful: false,
                                error: Some("timeout".to_string()),
                                block_hash: None,
                                market: sent_record.market.map(|x| x.to_string()),
                                market_maker: sent_record.market_maker.map(|x| x.to_string()),
                                keeper_instruction: sent_record.keeper_instruction.clone(),
                                slot_processed: None,
                                slot_leader: None,
                                timed_out: true,
                                priority_fees: sent_record.priority_fees,
                            });
                            to_remove.push(*signature);
                        }
                    }

                    for signature in to_remove {
                        transaction_map.remove(&signature);
                    }

                    // if exit and all the transactions are processed
                    if exit_signal.load(Ordering::Relaxed) && transaction_map.len() == 0 {
                        break;
                    }
                }
            }
        })
    };

    let block_confirmation_jh = {
        let exit_signal = exit_signal;
        tokio::spawn(async move {
            let mut start_block = from_slot;
            let mut start_instant = tokio::time::Instant::now();
            let refresh_in = Duration::from_secs(10);
            let commitment_confirmation = CommitmentConfig {
                commitment: CommitmentLevel::Confirmed,
            };
            loop {
                if exit_signal.load(Ordering::Relaxed) && transaction_map.len() == 0 {
                    break;
                }

                let wait_duration = tokio::time::Instant::now() - start_instant;
                if wait_duration < refresh_in {
                    tokio::time::sleep(refresh_in - wait_duration).await;
                }
                start_instant = tokio::time::Instant::now();

                let block_slots =
                    get_blocks_with_retry(client.clone(), start_block, commitment_confirmation)
                        .await;
                if block_slots.is_err() {
                    break;
                }

                let block_slots = block_slots.unwrap();
                if block_slots.is_empty() {
                    continue;
                }
                start_block = *block_slots.last().unwrap() + 1;

                let blocks = block_slots.iter().map(|slot| {
                    client.get_block_with_config(
                        *slot,
                        RpcBlockConfig {
                            encoding: Some(UiTransactionEncoding::Base64),
                            transaction_details: Some(TransactionDetails::Full),
                            rewards: Some(true),
                            commitment: Some(commitment_confirmation),
                            max_supported_transaction_version: Some(0),
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
                        block,
                        tx_confirm_records,
                        tx_block_data,
                        transaction_map,
                        block_slot.1,
                        commitment_confirmation.commitment,
                    )
                    .await;
                }
            }
        })
    };
    vec![map_filler_jh, cleaner_jh, block_confirmation_jh]
}
