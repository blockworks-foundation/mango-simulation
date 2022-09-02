use chrono::{DateTime, Utc};
use fixed::types::I80F48;
use log::*;
use mango::{
    instruction::{cancel_all_perp_orders, place_perp_order2},
    matching::Side,
    state::{MangoCache, MangoGroup, PerpMarket},
};
use mango_common::Loadable;
use serde_json;

use solana_bench_mango::{
    cli,
    mango::{AccountKeys, MangoConfig},
};
use solana_client::{
    connection_cache::ConnectionCache, rpc_client::RpcClient, rpc_config::RpcBlockConfig,
    tpu_client::TpuClient,
};
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
use solana_transaction_status::{TransactionDetails, UiTransactionEncoding};

use core::time;
use std::{
    collections::HashMap,
    collections::VecDeque,
    fs,
    ops::{Div, Mul},
    str::FromStr,
    sync::{
        atomic::{AtomicBool, Ordering},
        mpsc::{channel, Sender, TryRecvError, Receiver},
        Arc, RwLock,
    },
    thread::{sleep, Builder, JoinHandle},
    time::{Duration, Instant},
};

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

#[derive(Clone)]
struct TransactionSendRecord {
    pub signature: Signature,
    pub sent_at: DateTime<Utc>,
    pub sent_slot: Slot,
    pub market_maker: Pubkey,
    pub market: Pubkey,
}

#[derive(Clone)]
struct TransactionConfirmRecord {
    pub signature: Signature,
    pub sent_slot: Slot,
    pub sent_at: DateTime<Utc>,
    pub confirmed_slot: Slot,
    pub confirmed_at: DateTime<Utc>,
    pub error: Option<String>,
}

#[derive(Clone)]
struct TransactionTimeoutRecord {
    pub signature: Signature,
    pub sent_at: DateTime<Utc>,
    pub sent_slot: Slot,
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
    pub signature : Signature,
    pub transactionSendTime : DateTime<Utc>,
    pub send_slot : Slot,
    pub confirmationRetries : u32,
    pub error: String,
    pub confirmationBlockHash : Pubkey,
    pub leaderConfirmingTransaction: Pubkey,
    pub timeout : bool,
    pub market_maker: Pubkey,
    pub market: Pubkey,
} 

struct RotatingQueue<T> {
    deque: Arc<RwLock<VecDeque<T>>>,
}

impl<T: Clone> RotatingQueue<T> {
    pub fn new<F>(size: usize, creator_functor: F) -> Self
    where
        F: Fn() -> T,
    {
        let mut item = Self {
            deque: Arc::new(RwLock::new(VecDeque::<T>::new())),
        };
        {
            let mut deque = item.deque.write().unwrap();
            for _i in 0..size {
                deque.push_back(creator_functor());
            }
        }
        item
    }

    pub fn get(&self) -> T {
        let mut deque = self.deque.write().unwrap();
        let current = deque.pop_front().unwrap();
        deque.push_back(current.clone());
        current
    }
}

fn poll_blockhash_and_slot(
    exit_signal: &Arc<AtomicBool>,
    blockhash: Arc<RwLock<Hash>>,
    slot: Arc<RwLock<Slot>>,
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
            *slot.write().unwrap() = new_slot;
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

fn send_mm_transactions(
    quotes_per_second: u64,
    perp_market_caches: &Vec<PerpMarketCache>,
    tx_record_sx: &Sender<TransactionSendRecord>,
    tpu_client_pool: Arc<RotatingQueue<Arc<TpuClient>>>,
    mango_account_pk: Pubkey,
    mango_account_signer: &Keypair,
    blockhash: Arc<RwLock<Hash>>,
    slot: Arc<RwLock<Slot>>,
) {
    // update quotes 2x per second
    for _ in 0..quotes_per_second {
        for c in perp_market_caches.iter() {
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
                        &(pk_from_str_like(&mango_account_signer.pubkey())),
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
                        &(pk_from_str_like(&mango_account_signer.pubkey())),
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
                        &(pk_from_str_like(&mango_account_signer.pubkey())),
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

            let mut tx = Transaction::new_unsigned(Message::new(
                &[cancel_ix, place_bid_ix, place_ask_ix],
                Some(&mango_account_signer.pubkey()),
            ));

            if let Ok(recent_blockhash) = blockhash.read() {
                tx.sign(&[mango_account_signer], *recent_blockhash);
            }
            let tpu_client = tpu_client_pool.get();
            tpu_client.send_transaction(&tx);
            tx_record_sx
                .send(TransactionSendRecord {
                    signature: tx.signatures[0],
                    sent_at: Utc::now(),
                    sent_slot : *slot.read().unwrap(),
                    market_maker: mango_account_signer.pubkey(),
                    market: c.perp_market_pk,
                })
                .unwrap();
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
                                signature: tx_record.signature,
                                sent_slot: tx_record.sent_slot,
                                sent_at: tx_record.sent_at,
                                confirmed_at: Utc::now(),
                                confirmed_slot: s.slot,
                                error: s.err.as_ref().map(|e| {
                                    let err_msg = e.to_string();
                                    debug!(
                                        "tx {} returned an error {}",
                                        tx_record.signature, err_msg,
                                    );
                                    err_msg
                                }),
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
    recv_limit : usize,
    tx_confirm_records : Arc<RwLock<Vec<TransactionConfirmRecord>>>,
    tx_timeout_records: Arc<RwLock<Vec<TransactionSendRecord>>>,
    rpc_client : Arc<RpcClient>,
    tx_record_rx: &Receiver<TransactionSendRecord>
) {
    const TIMEOUT: u64 = 30;
    let mut error: bool = false;
    let mut recv_until_confirm = recv_limit;
    let not_confirmed: Arc<RwLock<Vec<TransactionSendRecord>>> =
        Arc::new(RwLock::new(Vec::new()));
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

fn confirmations_by_blocks(
    client: Arc<RpcClient>, 
    current_slot: Arc<RwLock<Slot>>,
    recv_limit : usize,
    timeout : AtomicBool,
    tx_record_rx: &Receiver<TransactionSendRecord>,
) {
    let mut last_slot = 0;
    let mut recv_until_confirm = recv_limit;
    let finished_all_transactions = AtomicBool::new(false);
    
    
    while recv_until_confirm > 0 && 
            !timeout.load(Ordering::Relaxed) && 
            !finished_all_transactions.load(Ordering::Relaxed) {

        let current_slot = { *current_slot.read().unwrap() };
        if last_slot != current_slot {
            let block = client.get_block(current_slot).unwrap();
            
        }
        else {
            sleep(Duration::from_millis(50));
        }
        last_slot = current_slot;
    }
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
        ..
    } = &cli_config;

    info!("Connecting to the cluster");

    let account_keys_json = fs::read_to_string(account_keys).expect("unable to read accounts file");
    let account_keys_parsed: Vec<AccountKeys> =
        serde_json::from_str(&account_keys_json).expect("accounts JSON was not well-formatted");

    let mango_keys_json = fs::read_to_string(mango_keys).expect("unable to read mango keys file");
    let mango_keys_parsed: MangoConfig =
        serde_json::from_str(&mango_keys_json).expect("mango JSON was not well-formatted");

    let mango_group_id = "testnet.0";
    let mango_group_config = mango_keys_parsed
        .groups
        .iter()
        .find(|g| g.name == mango_group_id)
        .unwrap();

    let number_of_tpu_clients: usize = 2 * (*quotes_per_second as usize);
    let rpc_client_for_tpu_client =
        RotatingQueue::<Arc<RpcClient>>::new(number_of_tpu_clients, || {
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
                    rpc_client_for_tpu_client.get().clone(),
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
    let current_slot = Arc::new(RwLock::new(rpc_client.get_slot().unwrap()));
    let blockhash_thread = {
        let exit_signal = exit_signal.clone();
        let blockhash = blockhash.clone();
        let current_slot = current_slot.clone();
        let client = rpc_client.clone();
        let id = id.pubkey();
        Builder::new()
            .name("solana-blockhash-poller".to_string())
            .spawn(move || {
                poll_blockhash_and_slot(&exit_signal, blockhash.clone(), current_slot.clone(), &client, &id);
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
            let base_decimals = mango_group_config.tokens[market_index + 1].decimals;
            let quote_decimals = mango_group_config.tokens[0].decimals;
            let base_unit = I80F48::from_num(10u64.pow(base_decimals as u32));
            let quote_unit = I80F48::from_num(10u64.pow(quote_decimals as u32));
            let price = mango_cache.price_cache[market_index].price;
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

            info!(
                "wallet:{:?} https://testnet.mango.markets/account?pubkey={:?}",
                mango_account_signer.pubkey(),
                mango_account_pk
            );

            let tx_record_sx = tx_record_sx.clone();

            Builder::new()
                .name("solana-client-sender".to_string())
                .spawn(move || {
                    for _i in 0..duration.as_secs() {
                        let start = Instant::now();
                        // send market maker transactions
                        send_mm_transactions(
                            quotes_per_second,
                            &perp_market_caches,
                            &tx_record_sx,
                            tpu_client_pool.clone(),
                            mango_account_pk,
                            &mango_account_signer,
                            blockhash.clone(),
                            current_slot.clone()
                        );

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

    let confirmation_thread = Builder::new()
        .name("solana-client-sender".to_string())
        .spawn(move || {
            let recv_limit = account_keys_parsed.len()
                                        * perp_market_caches.len()
                                        * duration.as_secs() as usize
                                        * quotes_per_second as usize;

            confirmation_by_querying_rpc(recv_limit, tx_confirm_records.clone(), tx_timeout_records.clone(), rpc_client.clone(), &tx_record_rx);

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
            let error_count = confirmed.iter().filter(|tx| tx.error.is_some()).count();
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

            let mut confirmation_times = confirmed
                .iter()
                .map(|r| {
                    r.confirmed_at
                        .signed_duration_since(r.sent_at)
                        .num_milliseconds()
                })
                .collect::<Vec<_>>();
            confirmation_times.sort();
            info!(
                "confirmation times min={} max={} median={}",
                confirmation_times.first().unwrap(),
                confirmation_times.last().unwrap(),
                confirmation_times[confirmation_times.len() / 2]
            );

            let mut slots = confirmed
                .iter()
                .map(|r| r.confirmed_slot)
                .collect::<Vec<_>>();
            slots.sort();
            slots.dedup();
            info!(
                "slots min={} max={} num={}",
                slots.first().unwrap(),
                slots.last().unwrap(),
                slots.len()
            );

            info!("slot csv stats");
            let mut num_tx_confirmed_by_slot = HashMap::new();
            for r in confirmed.iter() {
                *num_tx_confirmed_by_slot
                    .entry(r.confirmed_slot)
                    .or_insert(0) += 1;
            }

            sleep(Duration::from_secs(5));
            let mut block_by_slot = HashMap::new();
            for slot in slots.iter() {
                let maybe_block = rpc_client.get_block_with_config(
                    *slot,
                    RpcBlockConfig {
                        encoding: Some(UiTransactionEncoding::Base64),
                        transaction_details: Some(TransactionDetails::Signatures),
                        rewards: Some(true),
                        commitment: Some(CommitmentConfig {
                            commitment: CommitmentLevel::Confirmed,
                        }),
                        max_supported_transaction_version: None,
                    },
                );
                block_by_slot.insert(*slot, maybe_block);
            }

            println!("slot,leader_id,block_time,block_txs,bench_txs");
            for slot in slots.iter() {
                let bench_txs = num_tx_confirmed_by_slot[slot];
                match &block_by_slot[slot] {
                    Ok(block) => {
                        let leader_pk = &block
                            .rewards
                            .as_ref()
                            .unwrap()
                            .iter()
                            .find(|r| r.reward_type == Some(RewardType::Fee))
                            .unwrap()
                            .pubkey;

                        println!(
                            "{},{},{:?},{},{}",
                            slot,
                            leader_pk,
                            block.block_time.as_ref().unwrap(),
                            block.signatures.as_ref().unwrap().len(),
                            bench_txs
                        );
                    }
                    Err(err) => {
                        error!(
                            "could not fetch slot={} bench_txs={} err={}",
                            *slot, bench_txs, err
                        );
                    }
                }
            }

            info!("tx csv stats");
            println!("send_time,send_slot,confirmed_time,confirmed_slot,block_time");

            for tx in confirmed.iter() {
                let slot = tx.confirmed_slot;
                match &block_by_slot[&slot] {
                    Ok(block) => {
                        println!(
                            "{:?},{},{:?},{},{}",
                            tx.sent_at,
                            tx.sent_slot,
                            tx.confirmed_at,
                            tx.confirmed_slot,
                            block.block_time.as_ref().unwrap()
                        );
                    }
                    Err(err) => {
                        println!(
                            "{:?},{},{:?},{},",
                            tx.sent_at, tx.sent_slot, tx.confirmed_at, tx.confirmed_slot
                        );
                    }
                }
            }
            for tx in timeouts.iter() {
                println!("{:?},{},,,", tx.sent_at, tx.sent_slot);
            }
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
