use std::{
    str::FromStr,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

use chrono::Utc;
use iter_tools::Itertools;
use log::{debug, info, warn};
use mango::{
    instruction::{cancel_all_perp_orders, place_perp_order2},
    matching::Side,
};
use rand::{distributions::Uniform, prelude::Distribution, seq::SliceRandom};
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_program::pubkey::Pubkey;
use solana_sdk::{
    compute_budget, hash::Hash, instruction::Instruction, message::Message, signature::Keypair,
    signer::Signer, transaction::Transaction,
};
use tokio::{
    sync::RwLock,
    task::{self, JoinHandle},
};

use crate::{
    helpers::{to_sdk_instruction, to_sp_pk},
    mango::AccountKeys,
    states::{PerpMarketCache, TransactionSendRecord},
    tpu_manager::TpuManager,
};

pub fn create_ask_bid_transaction(
    c: &PerpMarketCache,
    mango_account_pk: Pubkey,
    mango_account_signer: &Keypair,
    prioritization_fee: u64,
) -> Transaction {
    let mango_account_signer_pk = to_sp_pk(&mango_account_signer.pubkey());
    let offset = rand::random::<i8>() as i64;
    let spread = rand::random::<u8>() as i64;
    debug!(
        "price:{:?} price_quote_lots:{:?} order_base_lots:{:?} offset:{:?} spread:{:?}",
        c.price, c.price_quote_lots, c.order_base_lots, offset, spread
    );
    let mut instructions = vec![];
    if prioritization_fee > 0 {
        let pfees =
            compute_budget::ComputeBudgetInstruction::set_compute_unit_price(prioritization_fee);
        instructions.push(pfees);
    }

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
    instructions.push(cancel_ix);

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
            Utc::now().timestamp_micros() as u64,
            mango::matching::OrderType::Limit,
            false,
            None,
            64,
            mango::matching::ExpiryType::Absolute,
        )
        .unwrap(),
    );
    instructions.push(place_bid_ix);

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
            Utc::now().timestamp_micros() as u64,
            mango::matching::OrderType::Limit,
            false,
            None,
            64,
            mango::matching::ExpiryType::Absolute,
        )
        .unwrap(),
    );
    instructions.push(place_ask_ix);

    Transaction::new_unsigned(Message::new(
        instructions.as_slice(),
        Some(&mango_account_signer.pubkey()),
    ))
}

fn generate_random_fees(
    prioritization_fee_proba: u8,
    n: usize,
    min_fee: u64,
    max_fee: u64,
) -> Vec<u64> {
    let mut rng = rand::thread_rng();
    let range = Uniform::from(min_fee..max_fee);
    let range_probability = Uniform::from(1..100);
    (0..n)
        .map(|_| {
            if prioritization_fee_proba == 0 {
                0
            } else if range_probability.sample(&mut rng) <= prioritization_fee_proba {
                range.sample(&mut rng)
            } else {
                0
            }
        })
        .collect()
}

#[allow(clippy::too_many_arguments)]
pub async fn send_mm_transactions(
    quotes_per_second: u64,
    perp_market_caches: &[PerpMarketCache],
    tpu_manager: TpuManager,
    mango_account_pk: Pubkey,
    mango_account_signer: &Keypair,
    blockhash: Arc<RwLock<Hash>>,
    slot: &AtomicU64,
    prioritization_fee_proba: u8,
) {
    let mango_account_signer_pk = to_sp_pk(&mango_account_signer.pubkey());
    // update quotes 2x per second
    for _ in 0..quotes_per_second {
        let prioritization_fee_by_market = generate_random_fees(
            prioritization_fee_proba,
            perp_market_caches.len(),
            100,
            1000,
        );
        let mut batch_to_send = Vec::with_capacity(perp_market_caches.len());
        for (i, c) in perp_market_caches.iter().enumerate() {
            let prioritization_fee = prioritization_fee_by_market[i];
            let mut tx = create_ask_bid_transaction(
                c,
                mango_account_pk,
                mango_account_signer,
                prioritization_fee,
            );

            let recent_blockhash = *blockhash.read().await;
            tx.sign(&[mango_account_signer], recent_blockhash);

            let record = TransactionSendRecord {
                signature: tx.signatures[0],
                sent_at: Utc::now(),
                sent_slot: slot.load(Ordering::Acquire),
                market_maker: Some(mango_account_signer_pk),
                market: Some(c.perp_market_pk),
                priority_fees: prioritization_fee,
                keeper_instruction: None,
            };
            batch_to_send.push((tx, record));
        }

        let tpu_manager = tpu_manager.clone();
        task::spawn(async move {
            if !tpu_manager.send_transaction_batch(&batch_to_send).await {
                println!("sending failed on tpu client");
            }
        });
    }
}

#[allow(clippy::too_many_arguments)]
pub fn start_market_making_threads(
    account_keys_parsed: Vec<AccountKeys>,
    perp_market_caches: Vec<PerpMarketCache>,
    exit_signal: Arc<AtomicBool>,
    blockhash: Arc<RwLock<Hash>>,
    current_slot: Arc<AtomicU64>,
    tpu_manager: TpuManager,
    duration: &Duration,
    quotes_per_second: u64,
    prioritization_fee_proba: u8,
    number_of_markers_per_mm: u8,
) -> Vec<JoinHandle<()>> {
    let mut rng = rand::thread_rng();
    account_keys_parsed
        .iter()
        .map(|account_keys| {
            let exit_signal = exit_signal.clone();
            let blockhash = blockhash.clone();
            let current_slot = current_slot.clone();
            let duration = *duration;
            let perp_market_caches = perp_market_caches.clone();
            let mango_account_pk =
                Pubkey::from_str(account_keys.mango_account_pks[0].as_str()).unwrap();
            let mango_account_signer =
                Keypair::from_bytes(account_keys.secret_key.as_slice()).unwrap();
            let tpu_manager = tpu_manager.clone();

            info!(
                "wallet: {:?} mango account: {:?}",
                mango_account_signer.pubkey(),
                mango_account_pk
            );
            let perp_market_caches = perp_market_caches
                .choose_multiple(&mut rng, number_of_markers_per_mm as usize)
                .cloned()
                .collect_vec();

            tokio::spawn(async move {
                for _i in 0..duration.as_secs() {
                    if exit_signal.load(Ordering::Relaxed) {
                        break;
                    }

                    let start = Instant::now();

                    // send market maker transactions
                    send_mm_transactions(
                        quotes_per_second,
                        &perp_market_caches,
                        tpu_manager.clone(),
                        mango_account_pk,
                        &mango_account_signer,
                        blockhash.clone(),
                        current_slot.as_ref(),
                        prioritization_fee_proba,
                    )
                    .await;

                    let elapsed_millis: u64 = start.elapsed().as_millis() as u64;
                    if elapsed_millis < 1000 {
                        tokio::time::sleep(Duration::from_millis(1000 - elapsed_millis)).await;
                    } else {
                        warn!(
                            "time taken to send transactions is greater than 1000ms {}",
                            elapsed_millis
                        );
                    }
                }
            })
        })
        .collect()
}

fn create_cancel_all_orders(
    perp_market: &PerpMarketCache,
    mango_account_pk: Pubkey,
    mango_account_signer: &Keypair,
) -> Transaction {
    let mango_account_signer_pk = to_sp_pk(&mango_account_signer.pubkey());

    let cb_instruction = compute_budget::ComputeBudgetInstruction::set_compute_unit_limit(1000000);
    let pf_instruction = compute_budget::ComputeBudgetInstruction::set_compute_unit_price(1000);

    let instruction: Instruction = to_sdk_instruction(
        cancel_all_perp_orders(
            &perp_market.mango_program_pk,
            &perp_market.mango_group_pk,
            &mango_account_pk,
            &mango_account_signer_pk,
            &perp_market.perp_market_pk,
            &perp_market.bids,
            &perp_market.asks,
            255,
        )
        .unwrap(),
    );

    Transaction::new_unsigned(Message::new(
        &[cb_instruction, pf_instruction, instruction],
        Some(&mango_account_signer.pubkey()),
    ))
}

pub async fn clean_market_makers(
    rpc_client: Arc<RpcClient>,
    account_keys_parsed: &[AccountKeys],
    perp_market_caches: &Vec<PerpMarketCache>,
    blockhash: Arc<RwLock<Hash>>,
) {
    info!("Cleaning previous transactions by market makers");

    for account_keys_parsed in account_keys_parsed.chunks(10) {
        let mut tasks = vec![];
        for market_maker in account_keys_parsed {
            let mango_account_pk =
                Pubkey::from_str(market_maker.mango_account_pks[0].as_str()).unwrap();

            for perp_market in perp_market_caches {
                let market_maker = market_maker.clone();
                let perp_market = perp_market.clone();
                let rpc_client = rpc_client.clone();
                let blockhash = blockhash.clone();

                let task = tokio::spawn(async move {
                    let mango_account_signer =
                        Keypair::from_bytes(market_maker.secret_key.as_slice()).unwrap();

                    for _ in 0..10 {
                        let mut tx = create_cancel_all_orders(
                            &perp_market,
                            mango_account_pk,
                            &mango_account_signer,
                        );

                        let recent_blockhash = *blockhash.read().await;
                        tx.sign(&[&mango_account_signer], recent_blockhash);
                        let sig = tx.signatures[0];
                        // send and confirm the transaction with an RPC
                        if let Ok(res) = tokio::time::timeout(
                            Duration::from_secs(10),
                            rpc_client.send_and_confirm_transaction(&tx),
                        )
                        .await
                        {
                            match res {
                                Ok(_) => break,
                                Err(e) => info!("Error occured while doing cancel all for ma : {}, sig : {} perp market : {} error : {}", mango_account_pk, sig, perp_market.perp_market_pk, e),
                            }
                        }
                    }
                });
                tasks.push(task);
            }
        }

        futures::future::join_all(tasks).await;
    }
    info!("finished cleaning market makers");
}
