use std::{
    str::FromStr,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc, RwLock,
    },
    thread::{sleep, Builder, JoinHandle},
    time::{Duration, Instant},
};

use chrono::Utc;
use crossbeam_channel::Sender;
use log::{debug, error, info, warn};
use mango::{
    instruction::{cancel_all_perp_orders, place_perp_order2},
    matching::Side,
};
use solana_client::tpu_client::TpuClient;
use solana_program::pubkey::Pubkey;
use solana_sdk::{
    hash::Hash, instruction::Instruction, message::Message, signature::Keypair, signer::Signer,
    transaction::Transaction,
};

use crate::{
    helpers::{to_sdk_instruction, to_sp_pk},
    mango::AccountKeys,
    rotating_queue::RotatingQueue,
    states::{PerpMarketCache, TransactionSendRecord},
};

pub fn create_ask_bid_transaction(
    c: &PerpMarketCache,
    mango_account_pk: Pubkey,
    mango_account_signer: &Keypair,
) -> Transaction {
    let mango_account_signer_pk = to_sp_pk(&mango_account_signer.pubkey());
    let offset = rand::random::<i8>() as i64;
    let spread = rand::random::<u8>() as i64;
    debug!(
        "price:{:?} price_quote_lots:{:?} order_base_lots:{:?} offset:{:?} spread:{:?}",
        c.price, c.price_quote_lots, c.order_base_lots, offset, spread
    );
    let cancel_ix: Instruction = to_sdk_instruction(
        cancel_all_perp_orders(
            &c.mango_program_pk,
            &c.mango_group_pk,
            &mango_account_pk,
            &mango_account_signer_pk,
            &c.perp_market_pk,
            &c.perp_market.bids,
            &c.perp_market.asks,
            10,
        )
        .unwrap(),
    );

    let place_bid_ix: Instruction = to_sdk_instruction(
        place_perp_order2(
            &c.mango_program_pk,
            &c.mango_group_pk,
            &mango_account_pk,
            &mango_account_signer_pk,
            &c.mango_cache_pk,
            &c.perp_market_pk,
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
    );

    let place_ask_ix: Instruction = to_sdk_instruction(
        place_perp_order2(
            &c.mango_program_pk,
            &c.mango_group_pk,
            &mango_account_pk,
            &mango_account_signer_pk,
            &c.mango_cache_pk,
            &c.perp_market_pk,
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
    );

    Transaction::new_unsigned(Message::new(
        &[cancel_ix, place_bid_ix, place_ask_ix],
        Some(&mango_account_signer.pubkey()),
    ))
}

pub fn send_mm_transactions(
    quotes_per_second: u64,
    perp_market_caches: &Vec<PerpMarketCache>,
    tx_record_sx: &Sender<TransactionSendRecord>,
    tpu_client_pool: Arc<RotatingQueue<Arc<TpuClient>>>,
    mango_account_pk: Pubkey,
    mango_account_signer: &Keypair,
    blockhash: Arc<RwLock<Hash>>,
    slot: &AtomicU64,
) {
    let mango_account_signer_pk = to_sp_pk(&mango_account_signer.pubkey());
    // update quotes 2x per second
    for _ in 0..quotes_per_second {
        for c in perp_market_caches.iter() {
            let mut tx = create_ask_bid_transaction(c, mango_account_pk, &mango_account_signer);

            if let Ok(recent_blockhash) = blockhash.read() {
                tx.sign(&[mango_account_signer], *recent_blockhash);
            }
            let tpu_client = tpu_client_pool.get();
            tpu_client.send_transaction(&tx);
            let sent = tx_record_sx.send(TransactionSendRecord {
                signature: tx.signatures[0],
                sent_at: Utc::now(),
                sent_slot: slot.load(Ordering::Acquire),
                market_maker: mango_account_signer_pk,
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

pub fn send_mm_transactions_batched(
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

    let mango_account_signer_pk = to_sp_pk(&mango_account_signer.pubkey());
    // update quotes 2x per second
    for _ in 0..quotes_per_second {
        for c in perp_market_caches.iter() {
            for _ in 0..txs_batch_size {
                transactions.push(create_ask_bid_transaction(
                    c,
                    mango_account_pk,
                    &mango_account_signer,
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
                    market_maker: mango_account_signer_pk,
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

pub fn start_market_making_threads(
    account_keys_parsed: Vec<AccountKeys>,
    perp_market_caches: Vec<PerpMarketCache>,
    tx_record_sx: Sender<TransactionSendRecord>,
    exit_signal: Arc<AtomicBool>,
    blockhash: Arc<RwLock<Hash>>,
    current_slot: Arc<AtomicU64>,
    tpu_client_pool: Arc<RotatingQueue<Arc<TpuClient>>>,
    duration: &Duration,
    quotes_per_second: u64,
    txs_batch_size: Option<usize>,
) -> Vec<JoinHandle<()>> {
    account_keys_parsed
        .iter()
        .map(|account_keys| {
            let _exit_signal = exit_signal.clone();
            // having a tpu client for each MM
            let tpu_client_pool = tpu_client_pool.clone();

            let blockhash = blockhash.clone();
            let current_slot = current_slot.clone();
            let duration = duration.clone();
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
            //sleep(Duration::from_secs(10));

            let tx_record_sx = tx_record_sx.clone();

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
        .collect()
}
