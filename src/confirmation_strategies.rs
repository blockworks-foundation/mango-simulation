use std::{
    collections::HashMap,
    ops::Div,
    str::FromStr,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc, RwLock,
    },
    thread::{sleep, Builder},
    time::Duration,
};

use chrono::Utc;
use crossbeam_channel::{Receiver, TryRecvError};
use log::{debug, error, info, trace};
use solana_client::{rpc_client::RpcClient, rpc_config::RpcBlockConfig};
use solana_program::pubkey::Pubkey;
use solana_sdk::{
    commitment_config::{CommitmentConfig, CommitmentLevel},
    signature::Signature,
};
use solana_transaction_status::RewardType;

use crate::{
    helpers::seconds_since,
    rotating_queue::RotatingQueue,
    states::{BlockData, TransactionConfirmRecord, TransactionSendRecord},
};

pub fn process_signature_confirmation_batch(
    rpc_client: &RpcClient,
    batch: &Vec<TransactionSendRecord>,
    not_confirmed: &Arc<RwLock<Vec<TransactionSendRecord>>>,
    confirmed: &Arc<RwLock<Vec<TransactionConfirmRecord>>>,
    timeouts: Arc<RwLock<Vec<TransactionSendRecord>>>,
    timeout: u64,
) {
    match rpc_client.get_signature_statuses(&batch.iter().map(|t| t.signature).collect::<Vec<_>>())
    {
        Ok(statuses) => {
            trace!("batch result {:?}", statuses);
            for (i, s) in statuses.value.iter().enumerate() {
                let tx_record = &batch[i];
                match s {
                    Some(s) => {
                        if s.confirmation_status.is_none() {
                            not_confirmed.write().unwrap().push(tx_record.clone());
                        } else {
                            let mut lock = confirmed.write().unwrap();
                            (*lock).push(TransactionConfirmRecord {
                                signature: tx_record.signature.to_string(),
                                sent_slot: tx_record.sent_slot,
                                sent_at: tx_record.sent_at.to_string(),
                                confirmed_at: Utc::now().to_string(),
                                confirmed_slot: s.slot,
                                successful: s.err.is_none(),
                                error: match &s.err {
                                    Some(e) => e.to_string(),
                                    None => "".to_string(),
                                },
                                block_hash: Pubkey::default().to_string(),
                                slot_leader: Pubkey::default().to_string(),
                                market: tx_record.market.to_string(),
                                market_maker: tx_record.market_maker.to_string(),
                                slot_processed: tx_record.sent_slot,
                                timed_out: false,
                                priority_fees: tx_record.priority_fees,
                            });

                            debug!(
                                "confirmed sig={} duration={:?}",
                                tx_record.signature,
                                seconds_since(tx_record.sent_at)
                            );
                        }
                    }
                    None => {
                        if seconds_since(tx_record.sent_at) > timeout as i64 {
                            debug!(
                                "could not confirm tx {} within {} seconds, dropping it",
                                tx_record.signature, timeout
                            );
                            let mut lock = timeouts.write().unwrap();
                            (*lock).push(tx_record.clone())
                        } else {
                            not_confirmed.write().unwrap().push(tx_record.clone());
                        }
                    }
                }
            }
        }
        Err(err) => {
            error!("could not confirm signatures err={}", err);
            not_confirmed.write().unwrap().extend_from_slice(batch);
            sleep(Duration::from_millis(500));
        }
    }
}

pub fn confirmation_by_querying_rpc(
    recv_limit: usize,
    rpc_client: Arc<RpcClient>,
    tx_record_rx: &Receiver<TransactionSendRecord>,
    tx_confirm_records: Arc<RwLock<Vec<TransactionConfirmRecord>>>,
    tx_timeout_records: Arc<RwLock<Vec<TransactionSendRecord>>>,
) {
    const TIMEOUT: u64 = 30;
    let mut recv_until_confirm = recv_limit;
    let not_confirmed: Arc<RwLock<Vec<TransactionSendRecord>>> = Arc::new(RwLock::new(Vec::new()));
    loop {
        let has_signatures_to_confirm = { not_confirmed.read().unwrap().len() > 0 };
        if has_signatures_to_confirm {
            // collect all not confirmed records in a new buffer

            const BATCH_SIZE: usize = 256;
            let to_confirm = {
                let mut lock = not_confirmed.write().unwrap();
                let to_confirm = (*lock).clone();
                (*lock).clear();
                to_confirm
            };

            info!(
                "break from reading channel, try to confirm {} in {} batches",
                to_confirm.len(),
                (to_confirm.len() / BATCH_SIZE)
                    + if to_confirm.len() % BATCH_SIZE > 0 {
                        1
                    } else {
                        0
                    }
            );

            let confirmed = tx_confirm_records.clone();
            let timeouts = tx_timeout_records.clone();
            for batch in to_confirm.rchunks(BATCH_SIZE).map(|x| x.to_vec()) {
                process_signature_confirmation_batch(
                    &rpc_client,
                    &batch,
                    &not_confirmed,
                    &confirmed,
                    timeouts.clone(),
                    TIMEOUT,
                );
            }
            // multi threaded implementation of confirming batches
            // let mut confirmation_handles = Vec::new();
            // for batch in to_confirm.rchunks(BATCH_SIZE).map(|x| x.to_vec()) {
            //     let rpc_client = rpc_client.clone();
            //     let not_confirmed = not_confirmed.clone();
            //     let confirmed = tx_confirm_records.clone();

            //     let join_handle = Builder::new().name("solana-transaction-confirmation".to_string()).spawn(move || {
            //         process_signature_confirmation_batch(&rpc_client, &batch, &not_confirmed, &confirmed, TIMEOUT)
            //     }).unwrap();
            //     confirmation_handles.push(join_handle);
            // };
            // for confirmation_handle in confirmation_handles {
            //     let (errors, timeouts) = confirmation_handle.join().unwrap();
            //     error_count += errors;
            //     timeout_count += timeouts;
            // }
            // sleep(Duration::from_millis(100)); // so the confirmation thread does not spam a lot the rpc node
        }
        {
            if recv_until_confirm == 0 && not_confirmed.read().unwrap().len() == 0 {
                break;
            }
        }
        // context for writing all the not_confirmed_transactions
        if recv_until_confirm > 0 {
            let mut lock = not_confirmed.write().unwrap();
            loop {
                match tx_record_rx.try_recv() {
                    Ok(tx_record) => {
                        debug!(
                            "add to queue len={} sig={}",
                            (*lock).len() + 1,
                            tx_record.signature
                        );
                        (*lock).push(tx_record);

                        recv_until_confirm -= 1;
                    }
                    Err(TryRecvError::Empty) => {
                        debug!("channel emptied");
                        sleep(Duration::from_millis(100));
                        break; // still confirm remaining transctions
                    }
                    Err(TryRecvError::Disconnected) => {
                        {
                            info!("channel disconnected {}", recv_until_confirm);
                        }
                        debug!("channel disconnected");
                        break; // still confirm remaining transctions
                    }
                }
            }
        }
    }
}

pub fn confirmations_by_blocks(
    clients: RotatingQueue<Arc<RpcClient>>,
    current_slot: &AtomicU64,
    recv_limit: usize,
    tx_record_rx: Receiver<TransactionSendRecord>,
    tx_confirm_records: Arc<RwLock<Vec<TransactionConfirmRecord>>>,
    tx_timeout_records: Arc<RwLock<Vec<TransactionSendRecord>>>,
    tx_block_data: Arc<RwLock<Vec<BlockData>>>,
) {
    let mut recv_until_confirm = recv_limit;
    let transaction_map = Arc::new(RwLock::new(
        HashMap::<Signature, TransactionSendRecord>::new(),
    ));
    let last_slot = current_slot.load(Ordering::Acquire);

    while recv_until_confirm != 0 {
        match tx_record_rx.try_recv() {
            Ok(tx_record) => {
                let mut transaction_map = transaction_map.write().unwrap();
                debug!(
                    "add to queue len={} sig={}",
                    transaction_map.len() + 1,
                    tx_record.signature
                );
                transaction_map.insert(tx_record.signature, tx_record);
                recv_until_confirm -= 1;
            }
            Err(TryRecvError::Empty) => {
                debug!("channel emptied");
                sleep(Duration::from_millis(100));
            }
            Err(TryRecvError::Disconnected) => {
                {
                    info!("channel disconnected {}", recv_until_confirm);
                }
                debug!("channel disconnected");
                break; // still confirm remaining transctions
            }
        }
    }
    println!("finished mapping all the trasactions");
    sleep(Duration::from_secs(30));
    let commitment_confirmation = CommitmentConfig {
        commitment: CommitmentLevel::Confirmed,
    };
    let block_res = clients
        .get()
        .get_blocks_with_commitment(last_slot, None, commitment_confirmation)
        .unwrap();

    let nb_blocks = block_res.len();
    let nb_thread: usize = 16;
    println!("processing {} blocks", nb_blocks);

    let mut join_handles = Vec::new();
    for slot_batch in block_res
        .chunks(if nb_blocks > nb_thread {
            nb_blocks.div(nb_thread)
        } else {
            nb_blocks
        })
        .map(|x| x.to_vec())
    {
        let map = transaction_map.clone();
        let client = clients.get().clone();
        let tx_confirm_records = tx_confirm_records.clone();
        let tx_block_data = tx_block_data.clone();
        let joinble = Builder::new()
            .name("getting blocks and searching transactions".to_string())
            .spawn(move || {
                for slot in slot_batch {
                    // retry search for block 10 times
                    let mut block = None;
                    for _i in 0..=10 {
                        let block_res = client
                        .get_block_with_config(
                            slot,
                            RpcBlockConfig {
                                encoding: None,
                                transaction_details: None,
                                rewards: None,
                                commitment: Some(commitment_confirmation),
                                max_supported_transaction_version: None,
                            },
                        );

                        match block_res {
                            Ok(x) => {
                                block = Some(x);
                                break;
                            },
                            _=>{
                                // do nothing
                            }
                        }
                    }
                    let block = match block {
                        Some(x) => x,
                        None => continue,
                    };
                    let mut mm_transaction_count: u64 = 0;
                    let rewards = &block.rewards.unwrap();
                    let slot_leader =  match rewards
                            .iter()
                            .find(|r| r.reward_type == Some(RewardType::Fee))
                            {
                                Some(x) => x.pubkey.clone(),
                                None=> "".to_string(),
                            };

                    if let Some(transactions) = block.transactions {
                        let nb_transactions = transactions.len();
                        let mut cu_consumed : u64 = 0;
                        for solana_transaction_status::EncodedTransactionWithStatusMeta {
                            transaction,
                            meta,
                            ..
                        } in transactions
                        {
                            if let solana_transaction_status::EncodedTransaction::Json(
                                transaction,
                            ) = transaction
                            {
                                for signature in transaction.signatures {
                                    let signature = Signature::from_str(&signature).unwrap();

                                    let transaction_record_op = {
                                        let map = map.read().unwrap();
                                        let rec = map.get(&signature);
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
                                        let mut lock = tx_confirm_records.write().unwrap();
                                        mm_transaction_count += 1;

                                        (*lock).push(TransactionConfirmRecord {
                                            signature: transaction_record.signature.to_string(),
                                            confirmed_slot: slot, // TODO: should be changed to correct slot
                                            confirmed_at: Utc::now().to_string(),
                                            sent_at: transaction_record.sent_at.to_string(),
                                            sent_slot: transaction_record.sent_slot,
                                            successful: if let Some(meta) = &meta {
                                                meta.status.is_ok()
                                            } else {
                                                false
                                            },
                                            error: if let Some(meta) = &meta {
                                                match &meta.err {
                                                    Some(x) => x.to_string(),
                                                    None => "".to_string(),
                                                }
                                            } else {
                                                "".to_string()
                                            },
                                            block_hash: block.blockhash.clone(),
                                            market: transaction_record.market.to_string(),
                                            market_maker: transaction_record.market_maker.to_string(),
                                            slot_processed: slot,
                                            slot_leader: slot_leader.clone(),
                                            timed_out: false,
                                            priority_fees: transaction_record.priority_fees,
                                        })
                                    }

                                    map.write().unwrap().remove(&signature);
                                }
                            }
                        }
                        // push block data
                        {
                            let mut blockstats_writer = tx_block_data.write().unwrap();
                            blockstats_writer.push(BlockData {
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
                            })
                        }
                    }
                }
            })
            .unwrap();
        join_handles.push(joinble);
    }
    for handle in join_handles {
        handle.join().unwrap();
    }

    let mut timeout_writer = tx_timeout_records.write().unwrap();
    for x in transaction_map.read().unwrap().iter() {
        timeout_writer.push(x.1.clone())
    }

    // sort all blocks by slot and print info
    {
        let mut blockstats_writer = tx_block_data.write().unwrap();
        blockstats_writer.sort_by(|a, b| a.block_slot.partial_cmp(&b.block_slot).unwrap());
        for block_stat in blockstats_writer.iter() {
            info!(
                "block {} at slot {} contains {} transactions and consumerd {} CUs",
                block_stat.block_hash,
                block_stat.block_slot,
                block_stat.total_transactions,
                block_stat.cu_consumed,
            );
        }
    }
}
