use chrono::{DateTime, Utc};
use fixed::types::I80F48;
use log::*;
use mango::{
    instruction::{cancel_all_perp_orders, place_perp_order2},
    matching::Side,
    state::{MangoCache, MangoGroup, PerpMarket},
};
use mango_common::Loadable;
use rayon::ThreadBuilder;
use serde::Serialize;
use serde_json;
mod rotating_queue;
use rotating_queue::RotatingQueue;

use solana_bench_mango::{
    cli,
    mango::{AccountKeys, MangoConfig},
};
use solana_client::{
    connection_cache::ConnectionCache, rpc_client::RpcClient, rpc_config::RpcBlockConfig,
    tpu_client::TpuClient,
};
use solana_program::native_token::LAMPORTS_PER_SOL;
use solana_runtime::bank::RewardType;
use solana_sdk::{
    clock::{Slot, DEFAULT_MS_PER_SLOT},
    commitment_config::{CommitmentConfig, CommitmentLevel},
    hash::Hash,
    instruction::Instruction,
    message::Message,
    pubkey::Pubkey,
    signature::{Keypair, Signature, Signer},
    stake::instruction,
    transaction::Transaction,
};
use solana_transaction_status::{EncodedConfirmedBlock, TransactionDetails, UiTransactionEncoding};

use core::time;
use std::{
    cell::Ref,
    collections::HashMap,
    collections::{HashSet, VecDeque},
    fs,
    ops::{Add, Div, Mul},
    str::FromStr,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        mpsc::{channel, Receiver, Sender, TryRecvError},
        Arc, RwLock,
    },
    thread::{sleep, Builder, JoinHandle},
    time::{Duration, Instant},
};

use csv;

fn load_from_rpc<T: Loadable>(rpc_client: &RpcClient, pk: &Pubkey) -> T {
    let acc = rpc_client.get_account(pk).unwrap();
    return T::load_from_bytes(acc.data.as_slice()).unwrap().clone();
}

fn get_latest_blockhash(rpc_client: &RpcClient) -> Hash {
    loop {
        match rpc_client.get_latest_blockhash() {
            Ok(blockhash) => return blockhash,
            Err(err) => {
                info!("Couldn't get last blockhash: {:?}", err);
                sleep(Duration::from_secs(1));
            }
        };
    }
}

fn get_new_latest_blockhash(client: &Arc<RpcClient>, blockhash: &Hash) -> Option<Hash> {
    let start = Instant::now();
    while start.elapsed().as_secs() < 5 {
        if let Ok(new_blockhash) = client.get_latest_blockhash() {
            if new_blockhash != *blockhash {
                debug!("Got new blockhash ({:?})", blockhash);
                return Some(new_blockhash);
            }
        }
        debug!("Got same blockhash ({:?}), will retry...", blockhash);

        // Retry ~twice during a slot
        sleep(Duration::from_millis(DEFAULT_MS_PER_SLOT / 2));
    }
    None
}

#[derive(Clone, Serialize)]
struct TransactionSendRecord {
    pub signature: Signature,
    pub sent_at: DateTime<Utc>,
    pub sent_slot: Slot,
    pub market_maker: Pubkey,
    pub market: Pubkey,
}

#[derive(Clone, Serialize)]
struct TransactionConfirmRecord {
    pub signature: String,
    pub sent_slot: Slot,
    pub sent_at: String,
    pub confirmed_slot: Slot,
    pub confirmed_at: String,
    pub successful: bool,
    pub slot_leader: String,
    pub error: String,
    pub market_maker: String,
    pub market: String,
    pub block_hash: String,
    pub slot_processed: Slot,
    pub timed_out: bool,
}

#[derive(Clone)]
struct PerpMarketCache {
    pub order_base_lots: i64,
    pub price: I80F48,
    pub price_quote_lots: i64,
    pub mango_program_pk: Pubkey,
    pub mango_group_pk: Pubkey,
    pub mango_cache_pk: Pubkey,
    pub perp_market_pk: Pubkey,
    pub perp_market: PerpMarket,
}

struct TransactionInfo {
    pub signature: Signature,
    pub transactionSendTime: DateTime<Utc>,
    pub send_slot: Slot,
    pub confirmationRetries: u32,
    pub error: String,
    pub confirmationBlockHash: Pubkey,
    pub leaderConfirmingTransaction: Pubkey,
    pub timeout: bool,
    pub market_maker: Pubkey,
    pub market: Pubkey,
}

fn poll_blockhash_and_slot(
    exit_signal: &Arc<AtomicBool>,
    blockhash: Arc<RwLock<Hash>>,
    slot: &AtomicU64,
    client: &Arc<RpcClient>,
    _id: &Pubkey,
) {
    let mut blockhash_last_updated = Instant::now();
    //let mut last_error_log = Instant::now();
    loop {
        let old_blockhash = *blockhash.read().unwrap();
        if exit_signal.load(Ordering::Relaxed) {
            break;
        }

        let new_slot = client.get_slot().unwrap();
        {
            slot.store(new_slot, Ordering::Release);
        }

        if let Some(new_blockhash) = get_new_latest_blockhash(client, &old_blockhash) {
            {
                *blockhash.write().unwrap() = new_blockhash;
            }
            blockhash_last_updated = Instant::now();
        } else {
            if blockhash_last_updated.elapsed().as_secs() > 120 {
                break;
            }
        }

        sleep(Duration::from_millis(100));
    }
}

fn pk_from_str(str: &str) -> solana_program::pubkey::Pubkey {
    return solana_program::pubkey::Pubkey::from_str(str).unwrap();
}

fn pk_from_str_like<T: ToString>(str_like: T) -> solana_program::pubkey::Pubkey {
    return pk_from_str(&str_like.to_string());
}

fn seconds_since(dt: DateTime<Utc>) -> i64 {
    Utc::now().signed_duration_since(dt).num_seconds()
}

fn create_ask_bid_transaction(
    c: &PerpMarketCache,
    mango_account_pk: Pubkey,
    mango_account_signer_pk: Pubkey,
) -> Transaction {
    let offset = rand::random::<i8>() as i64;
    let spread = rand::random::<u8>() as i64;
    debug!(
        "price:{:?} price_quote_lots:{:?} order_base_lots:{:?} offset:{:?} spread:{:?}",
        c.price, c.price_quote_lots, c.order_base_lots, offset, spread
    );
    let cancel_ix: Instruction = serde_json::from_str(
        &serde_json::to_string(
            &cancel_all_perp_orders(
                &pk_from_str_like(&c.mango_program_pk),
                &pk_from_str_like(&c.mango_group_pk),
                &pk_from_str_like(&mango_account_pk),
                &(pk_from_str_like(&mango_account_signer_pk)),
                &(pk_from_str_like(&c.perp_market_pk)),
                &c.perp_market.bids,
                &c.perp_market.asks,
                10,
            )
            .unwrap(),
        )
        .unwrap(),
    )
    .unwrap();

    let place_bid_ix: Instruction = serde_json::from_str(
        &serde_json::to_string(
            &place_perp_order2(
                &pk_from_str_like(&c.mango_program_pk),
                &pk_from_str_like(&c.mango_group_pk),
                &pk_from_str_like(&mango_account_pk),
                &(pk_from_str_like(&mango_account_signer_pk)),
                &pk_from_str_like(&c.mango_cache_pk),
                &(pk_from_str_like(&c.perp_market_pk)),
                &c.perp_market.bids,
                &c.perp_market.asks,
                &c.perp_market.event_queue,
                None,
                &[],
                Side::Bid,
                c.price_quote_lots + offset - spread,
                c.order_base_lots,
                i64::MAX,
                1,
                mango::matching::OrderType::Limit,
                false,
                None,
                64,
                mango::matching::ExpiryType::Absolute,
            )
            .unwrap(),
        )
        .unwrap(),
    )
    .unwrap();

    let place_ask_ix: Instruction = serde_json::from_str(
        &serde_json::to_string(
            &place_perp_order2(
                &pk_from_str_like(&c.mango_program_pk),
                &pk_from_str_like(&c.mango_group_pk),
                &pk_from_str_like(&mango_account_pk),
                &(pk_from_str_like(&mango_account_signer_pk)),
                &pk_from_str_like(&c.mango_cache_pk),
                &(pk_from_str_like(&c.perp_market_pk)),
                &c.perp_market.bids,
                &c.perp_market.asks,
                &c.perp_market.event_queue,
                None,
                &[],
                Side::Ask,
                c.price_quote_lots + offset + spread,
                c.order_base_lots,
                i64::MAX,
                2,
                mango::matching::OrderType::Limit,
                false,
                None,
                64,
                mango::matching::ExpiryType::Absolute,
            )
            .unwrap(),
        )
        .unwrap(),
    )
    .unwrap();
    Transaction::new_unsigned(Message::new(
        &[cancel_ix, place_bid_ix, place_ask_ix],
        Some(&mango_account_signer_pk),
    ))
}

fn send_mm_transactions(
    quotes_per_second: u64,
    perp_market_caches: &Vec<PerpMarketCache>,
    tx_record_sx: &Sender<TransactionSendRecord>,
    tpu_client_pool: Arc<RotatingQueue<Arc<TpuClient>>>,
    mango_account_pk: Pubkey,
    mango_account_signer: &Keypair,
    blockhash: Arc<RwLock<Hash>>,
    slot: &AtomicU64,
) {
    // update quotes 2x per second
    for _ in 0..quotes_per_second {
        for c in perp_market_caches.iter() {
            let mut tx =
                create_ask_bid_transaction(c, mango_account_pk, mango_account_signer.pubkey());

            if let Ok(recent_blockhash) = blockhash.read() {
                tx.sign(&[mango_account_signer], *recent_blockhash);
            }
            let tpu_client = tpu_client_pool.get();
            tpu_client.send_transaction(&tx);
            let sent = tx_record_sx.send(TransactionSendRecord {
                signature: tx.signatures[0],
                sent_at: Utc::now(),
                sent_slot: slot.load(Ordering::Acquire),
                market_maker: mango_account_signer.pubkey(),
                market: c.perp_market_pk,
            });
            if sent.is_err() {
                println!(
                    "sending error on channel : {}",
                    sent.err().unwrap().to_string()
                );
            }
        }
    }
}

fn send_mm_transactions_batched(
    txs_batch_size: usize,
    quotes_per_second: u64,
    perp_market_caches: &Vec<PerpMarketCache>,
    tx_record_sx: &Sender<TransactionSendRecord>,
    tpu_client_pool: Arc<RotatingQueue<Arc<TpuClient>>>,
    mango_account_pk: Pubkey,
    mango_account_signer: &Keypair,
    blockhash: Arc<RwLock<Hash>>,
    slot: &AtomicU64,
) {
    let mut transactions = Vec::<_>::with_capacity(txs_batch_size);
    // update quotes 2x per second
    for _ in 0..quotes_per_second {
        for c in perp_market_caches.iter() {
            for _ in 0..txs_batch_size {
                transactions.push(create_ask_bid_transaction(
                    c,
                    mango_account_pk,
                    mango_account_signer.pubkey(),
                ));
            }

            if let Ok(recent_blockhash) = blockhash.read() {
                for tx in &mut transactions {
                    tx.sign(&[mango_account_signer], *recent_blockhash);
                }
            }
            let tpu_client = tpu_client_pool.get();
            if tpu_client
                .try_send_transaction_batch(&transactions)
                .is_err()
            {
                error!("Sending batch failed");
                continue;
            }

            for tx in &transactions {
                let sent = tx_record_sx.send(TransactionSendRecord {
                    signature: tx.signatures[0],
                    sent_at: Utc::now(),
                    sent_slot: slot.load(Ordering::Acquire),
                    market_maker: mango_account_signer.pubkey(),
                    market: c.perp_market_pk,
                });
                if sent.is_err() {
                    error!(
                        "sending error on channel : {}",
                        sent.err().unwrap().to_string()
                    );
                }
            }
            transactions.clear();
        }
    }
}

fn process_signature_confirmation_batch(
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

fn confirmation_by_querying_rpc(
    recv_limit: usize,
    rpc_client: Arc<RpcClient>,
    tx_record_rx: &Receiver<TransactionSendRecord>,
    tx_confirm_records: Arc<RwLock<Vec<TransactionConfirmRecord>>>,
    tx_timeout_records: Arc<RwLock<Vec<TransactionSendRecord>>>,
) {
    const TIMEOUT: u64 = 30;
    let mut error: bool = false;
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
                        error = true;
                        break; // still confirm remaining transctions
                    }
                }
            }
        }
    }
}

#[derive(Clone, Serialize)]
struct BlockData {
    pub block_hash: String,
    pub block_slot: Slot,
    pub block_leader: String,
    pub total_transactions: u64,
    pub number_of_mm_transactions: u64,
    pub block_time: u64,
    pub cu_consumed: u64,
}

fn confirmations_by_blocks(
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
                    for i in 0..=10 {
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
                            version,
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

fn write_transaction_data_into_csv(
    transaction_save_file: String,
    tx_confirm_records: Arc<RwLock<Vec<TransactionConfirmRecord>>>,
    tx_timeout_records: Arc<RwLock<Vec<TransactionSendRecord>>>,
) {
    if transaction_save_file.is_empty() {
        return;
    }
    let mut writer = csv::Writer::from_path(transaction_save_file).unwrap();
    {
        let rd_lock = tx_confirm_records.read().unwrap();
        for confirm_record in rd_lock.iter() {
            writer.serialize(confirm_record).unwrap();
        }

        let timeout_lk = tx_timeout_records.read().unwrap();
        for timeout_record in timeout_lk.iter() {
            writer
                .serialize(TransactionConfirmRecord {
                    block_hash: "".to_string(),
                    confirmed_at: "".to_string(),
                    confirmed_slot: 0,
                    error: "Timeout".to_string(),
                    market: timeout_record.market.to_string(),
                    market_maker: timeout_record.market_maker.to_string(),
                    sent_at: timeout_record.sent_at.to_string(),
                    sent_slot: timeout_record.sent_slot,
                    signature: timeout_record.signature.to_string(),
                    slot_leader: "".to_string(),
                    slot_processed: 0,
                    successful: false,
                    timed_out: true,
                })
                .unwrap();
        }
    }
    writer.flush().unwrap();
}

fn write_block_data_into_csv(block_data_csv: String, tx_block_data: Arc<RwLock<Vec<BlockData>>>) {
    if block_data_csv.is_empty() {
        return;
    }
    let mut writer = csv::Writer::from_path(block_data_csv).unwrap();
    let data = tx_block_data.read().unwrap();

    for d in data.iter().filter(|x| x.number_of_mm_transactions > 0) {
        writer.serialize(d).unwrap();
    }
    writer.flush().unwrap();
}

fn main() {
    solana_logger::setup_with_default("solana=info");
    solana_metrics::set_panic_hook("bench-mango", /*version:*/ None);

    let matches = cli::build_args(solana_version::version!()).get_matches();
    let cli_config = cli::extract_args(&matches);

    let cli::Config {
        json_rpc_url,
        websocket_url,
        id,
        account_keys,
        mango_keys,
        duration,
        quotes_per_second,
        transaction_save_file,
        block_data_save_file,
        airdrop_accounts,
        mango_cluster,
        txs_batch_size,
        ..
    } = &cli_config;

    let transaction_save_file = transaction_save_file.clone();
    let block_data_save_file = block_data_save_file.clone();
    let airdrop_accounts = *airdrop_accounts;

    info!("Connecting to the cluster");

    let account_keys_json = fs::read_to_string(account_keys).expect("unable to read accounts file");
    let account_keys_parsed: Vec<AccountKeys> =
        serde_json::from_str(&account_keys_json).expect("accounts JSON was not well-formatted");

    let mango_keys_json = fs::read_to_string(mango_keys).expect("unable to read mango keys file");
    let mango_keys_parsed: MangoConfig =
        serde_json::from_str(&mango_keys_json).expect("mango JSON was not well-formatted");

    let mango_group_id = mango_cluster;
    let mango_group_config = mango_keys_parsed
        .groups
        .iter()
        .find(|g| g.name == *mango_group_id)
        .unwrap();

    let number_of_tpu_clients: usize = 1;
    let rpc_clients = RotatingQueue::<Arc<RpcClient>>::new(number_of_tpu_clients, || {
        Arc::new(RpcClient::new_with_commitment(
            json_rpc_url.to_string(),
            CommitmentConfig::confirmed(),
        ))
    });

    let tpu_client_pool = Arc::new(RotatingQueue::<Arc<TpuClient>>::new(
        number_of_tpu_clients,
        || {
            Arc::new(
                TpuClient::new_with_connection_cache(
                    rpc_clients.get().clone(),
                    &websocket_url,
                    solana_client::tpu_client::TpuClientConfig::default(),
                    Arc::new(ConnectionCache::default()),
                )
                .unwrap(),
            )
        },
    ));

    info!(
        "accounts:{:?} markets:{:?} quotes_per_second:{:?} expected_tps:{:?} duration:{:?}",
        account_keys_parsed.len(),
        mango_group_config.perp_markets.len(),
        quotes_per_second,
        account_keys_parsed.len()
            * mango_group_config.perp_markets.len()
            * quotes_per_second.clone() as usize,
        duration
    );

    // continuosly fetch blockhash
    let rpc_client = Arc::new(RpcClient::new_with_commitment(
        json_rpc_url.to_string(),
        CommitmentConfig::confirmed(),
    ));
    let exit_signal = Arc::new(AtomicBool::new(false));
    let blockhash = Arc::new(RwLock::new(get_latest_blockhash(&rpc_client.clone())));
    let current_slot = Arc::new(AtomicU64::new(0));
    let blockhash_thread = {
        let exit_signal = exit_signal.clone();
        let blockhash = blockhash.clone();
        let client = rpc_client.clone();
        let id = id.pubkey();
        let current_slot = current_slot.clone();
        Builder::new()
            .name("solana-blockhash-poller".to_string())
            .spawn(move || {
                poll_blockhash_and_slot(
                    &exit_signal,
                    blockhash.clone(),
                    current_slot.as_ref(),
                    &client,
                    &id,
                );
            })
            .unwrap()
    };

    // fetch group
    let mango_group_pk = Pubkey::from_str(mango_group_config.public_key.as_str()).unwrap();
    let mango_group = load_from_rpc::<MangoGroup>(&rpc_client, &mango_group_pk);
    let mango_program_pk = Pubkey::from_str(mango_group_config.mango_program_id.as_str()).unwrap();
    let mango_cache_pk = Pubkey::from_str(mango_group.mango_cache.to_string().as_str()).unwrap();
    let mango_cache = load_from_rpc::<MangoCache>(&rpc_client, &mango_cache_pk);

    let perp_market_caches: Vec<PerpMarketCache> = mango_group_config
        .perp_markets
        .iter()
        .enumerate()
        .map(|(market_index, perp_maket_config)| {
            let perp_market_pk = Pubkey::from_str(perp_maket_config.public_key.as_str()).unwrap();
            let perp_market = load_from_rpc::<PerpMarket>(&rpc_client, &perp_market_pk);

            // fetch price
            let base_decimals = mango_group_config.tokens[market_index].decimals;
            let quote_decimals = mango_group_config.tokens[0].decimals;

            let base_unit = I80F48::from_num(10u64.pow(base_decimals as u32));
            let quote_unit = I80F48::from_num(10u64.pow(quote_decimals as u32));
            let price = mango_cache.price_cache[market_index].price;
            println!(
                "market index {} price of  : {}",
                market_index, mango_cache.price_cache[market_index].price
            );

            let price_quote_lots: i64 = price
                .mul(quote_unit)
                .mul(I80F48::from_num(perp_market.base_lot_size))
                .div(I80F48::from_num(perp_market.quote_lot_size))
                .div(base_unit)
                .to_num();
            let order_base_lots: i64 = base_unit
                .div(I80F48::from_num(perp_market.base_lot_size))
                .to_num();

            PerpMarketCache {
                order_base_lots,
                price,
                price_quote_lots,
                mango_program_pk,
                mango_group_pk,
                mango_cache_pk,
                perp_market_pk,
                perp_market,
            }
        })
        .collect();

    let (tx_record_sx, tx_record_rx) = channel::<TransactionSendRecord>();

    let mm_threads: Vec<JoinHandle<()>> = account_keys_parsed
        .iter()
        .map(|account_keys| {
            let _exit_signal = exit_signal.clone();
            // having a tpu client for each MM
            let tpu_client_pool = tpu_client_pool.clone();

            let blockhash = blockhash.clone();
            let current_slot = current_slot.clone();
            let duration = duration.clone();
            let quotes_per_second = quotes_per_second.clone();
            let perp_market_caches = perp_market_caches.clone();
            let mango_account_pk =
                Pubkey::from_str(account_keys.mango_account_pks[0].as_str()).unwrap();
            let mango_account_signer =
                Keypair::from_bytes(account_keys.secret_key.as_slice()).unwrap();

            if airdrop_accounts {
                println!("Transfering 1 SOL to {}", mango_account_signer.pubkey());
                let inx = solana_sdk::system_instruction::transfer(
                    &id.pubkey(),
                    &mango_account_signer.pubkey(),
                    LAMPORTS_PER_SOL,
                );

                let mut tx = Transaction::new_unsigned(Message::new(&[inx], Some(&id.pubkey())));

                if let Ok(recent_blockhash) = blockhash.read() {
                    tx.sign(&[id], *recent_blockhash);
                }
                rpc_client
                    .send_and_confirm_transaction_with_spinner(&tx)
                    .unwrap();
            }

            info!(
                "wallet:{:?} https://testnet.mango.markets/account?pubkey={:?}",
                mango_account_signer.pubkey(),
                mango_account_pk
            );
            //sleep(Duration::from_secs(10));

            let tx_record_sx = tx_record_sx.clone();
            let txs_batch_size: Option<usize> = *txs_batch_size;

            Builder::new()
                .name("solana-client-sender".to_string())
                .spawn(move || {
                    for _i in 0..duration.as_secs() {
                        let start = Instant::now();
                        // send market maker transactions
                        if let Some(txs_batch_size) = txs_batch_size.clone() {
                            send_mm_transactions_batched(
                                txs_batch_size,
                                quotes_per_second,
                                &perp_market_caches,
                                &tx_record_sx,
                                tpu_client_pool.clone(),
                                mango_account_pk,
                                &mango_account_signer,
                                blockhash.clone(),
                                current_slot.as_ref(),
                            );
                        } else {
                            send_mm_transactions(
                                quotes_per_second,
                                &perp_market_caches,
                                &tx_record_sx,
                                tpu_client_pool.clone(),
                                mango_account_pk,
                                &mango_account_signer,
                                blockhash.clone(),
                                current_slot.as_ref(),
                            );
                        }

                        let elapsed_millis: u64 = start.elapsed().as_millis() as u64;
                        if elapsed_millis < 950 {
                            // 50 ms is reserved for other stuff
                            sleep(Duration::from_millis(950 - elapsed_millis));
                        } else {
                            warn!(
                                "time taken to send transactions is greater than 1000ms {}",
                                elapsed_millis
                            );
                        }
                    }
                })
                .unwrap()
        })
        .collect();

    drop(tx_record_sx);
    let duration = duration.clone();
    let quotes_per_second = quotes_per_second.clone();
    let account_keys_parsed = account_keys_parsed.clone();
    let tx_confirm_records: Arc<RwLock<Vec<TransactionConfirmRecord>>> =
        Arc::new(RwLock::new(Vec::new()));
    let tx_timeout_records: Arc<RwLock<Vec<TransactionSendRecord>>> =
        Arc::new(RwLock::new(Vec::new()));

    let tx_block_data = Arc::new(RwLock::new(Vec::<BlockData>::new()));

    let confirmation_thread = Builder::new()
        .name("solana-client-sender".to_string())
        .spawn(move || {
            let recv_limit = account_keys_parsed.len()
                * perp_market_caches.len()
                * duration.as_secs() as usize
                * quotes_per_second as usize;

            //confirmation_by_querying_rpc(recv_limit, rpc_client.clone(), &tx_record_rx, tx_confirm_records.clone(), tx_timeout_records.clone());
            confirmations_by_blocks(
                rpc_clients,
                &current_slot,
                recv_limit,
                tx_record_rx,
                tx_confirm_records.clone(),
                tx_timeout_records.clone(),
                tx_block_data.clone(),
            );

            let confirmed: Vec<TransactionConfirmRecord> = {
                let lock = tx_confirm_records.write().unwrap();
                (*lock).clone()
            };
            let total_signed = account_keys_parsed.len()
                * perp_market_caches.len()
                * duration.as_secs() as usize
                * quotes_per_second as usize;
            info!(
                "confirmed {} signatures of {} rate {}%",
                confirmed.len(),
                total_signed,
                (confirmed.len() * 100) / total_signed
            );
            let error_count = confirmed.iter().filter(|tx| !tx.error.is_empty()).count();
            info!(
                "errors counted {} rate {}%",
                error_count,
                (error_count as usize * 100) / total_signed
            );
            let timeouts: Vec<TransactionSendRecord> = {
                let timeouts = tx_timeout_records.clone();
                let lock = timeouts.write().unwrap();
                (*lock).clone()
            };
            info!(
                "timeouts counted {} rate {}%",
                timeouts.len(),
                (timeouts.len() * 100) / total_signed
            );

            // let mut confirmation_times = confirmed
            //     .iter()
            //     .map(|r| {
            //         r.confirmed_at
            //             .signed_duration_since(r.sent_at)
            //             .num_milliseconds()
            //     })
            //     .collect::<Vec<_>>();
            // confirmation_times.sort();
            // info!(
            //     "confirmation times min={} max={} median={}",
            //     confirmation_times.first().unwrap(),
            //     confirmation_times.last().unwrap(),
            //     confirmation_times[confirmation_times.len() / 2]
            // );

            write_transaction_data_into_csv(
                transaction_save_file,
                tx_confirm_records,
                tx_timeout_records,
            );

            write_block_data_into_csv(block_data_save_file, tx_block_data);
        })
        .unwrap();

    for t in mm_threads {
        if let Err(err) = t.join() {
            error!("mm join failed with: {:?}", err);
        }
    }

    info!("joined all mm_threads");

    if let Err(err) = confirmation_thread.join() {
        error!("confirmation join fialed with: {:?}", err);
    }

    info!("joined confirmation thread");

    exit_signal.store(true, Ordering::Relaxed);

    if let Err(err) = blockhash_thread.join() {
        error!("blockhash join failed with: {:?}", err);
    }
}
