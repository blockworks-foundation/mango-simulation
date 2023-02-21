use std::{
    ops::{Div, Mul},
    str::FromStr,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc, RwLock,
    },
    thread::{sleep, Builder, JoinHandle},
    time::{Duration, Instant},
};

use chrono::{DateTime, Utc};
use fixed::types::I80F48;
use log::{debug, info};
use mango::state::{MangoCache, MangoGroup, PerpMarket};
use mango_common::Loadable;
use solana_client::rpc_client::RpcClient;
use solana_program::{clock::DEFAULT_MS_PER_SLOT, pubkey::Pubkey};
use solana_sdk::hash::Hash;

use crate::{
    mango::GroupConfig,
    states::{BlockData, PerpMarketCache, TransactionConfirmRecord, TransactionSendRecord},
};

// as there are similar modules solana_sdk and solana_program
// solana internals use solana_sdk but external dependancies like mango use solana program
// that is why we have some helper methods
pub fn to_sdk_pk(pubkey: &Pubkey) -> solana_sdk::pubkey::Pubkey {
    solana_sdk::pubkey::Pubkey::from(pubkey.to_bytes())
}

pub fn to_sp_pk(pubkey: &solana_sdk::pubkey::Pubkey) -> Pubkey {
    Pubkey::new_from_array(pubkey.to_bytes())
}

pub fn to_sdk_accountmetas(
    vec: Vec<solana_program::instruction::AccountMeta>,
) -> Vec<solana_sdk::instruction::AccountMeta> {
    vec.iter()
        .map(|x| solana_sdk::instruction::AccountMeta {
            pubkey: to_sdk_pk(&x.pubkey),
            is_signer: x.is_signer,
            is_writable: x.is_writable,
        })
        .collect::<Vec<solana_sdk::instruction::AccountMeta>>()
}

pub fn to_sdk_instruction(
    instruction: solana_program::instruction::Instruction,
) -> solana_sdk::instruction::Instruction {
    solana_sdk::instruction::Instruction {
        program_id: to_sdk_pk(&instruction.program_id),
        accounts: to_sdk_accountmetas(instruction.accounts),
        data: instruction.data,
    }
}

pub fn load_from_rpc<T: Loadable>(rpc_client: &RpcClient, pk: &Pubkey) -> T {
    let acc = rpc_client.get_account(&to_sdk_pk(pk)).unwrap();
    return T::load_from_bytes(acc.data.as_slice()).unwrap().clone();
}

pub fn get_latest_blockhash(rpc_client: &RpcClient) -> Hash {
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

pub fn get_new_latest_blockhash(client: Arc<RpcClient>, blockhash: &Hash) -> Option<Hash> {
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

pub fn poll_blockhash_and_slot(
    exit_signal: Arc<AtomicBool>,
    blockhash: Arc<RwLock<Hash>>,
    slot: &AtomicU64,
    client: Arc<RpcClient>,
) {
    let mut blockhash_last_updated = Instant::now();
    //let mut last_error_log = Instant::now();
    loop {
        let client = client.clone();
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

pub fn seconds_since(dt: DateTime<Utc>) -> i64 {
    Utc::now().signed_duration_since(dt).num_seconds()
}

pub fn write_transaction_data_into_csv(
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
                    priority_fees: timeout_record.priority_fees,
                })
                .unwrap();
        }
    }
    writer.flush().unwrap();
}

pub fn write_block_data_into_csv(
    block_data_csv: String,
    tx_block_data: Arc<RwLock<Vec<BlockData>>>,
) {
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

pub fn start_blockhash_polling_service(
    exit_signal: Arc<AtomicBool>,
    blockhash: Arc<RwLock<Hash>>,
    current_slot: Arc<AtomicU64>,
    client: Arc<RpcClient>,
) -> JoinHandle<()> {
    Builder::new()
        .name("solana-blockhash-poller".to_string())
        .spawn(move || {
            poll_blockhash_and_slot(
                exit_signal,
                blockhash.clone(),
                current_slot.as_ref(),
                client,
            );
        })
        .unwrap()
}

pub fn get_mango_market_perps_cache(
    rpc_client: Arc<RpcClient>,
    mango_group_config: &GroupConfig,
) -> Vec<PerpMarketCache> {
    // fetch group
    let mango_group_pk = Pubkey::from_str(mango_group_config.public_key.as_str()).unwrap();
    let mango_group = load_from_rpc::<MangoGroup>(&rpc_client, &mango_group_pk);
    let mango_program_pk = Pubkey::from_str(mango_group_config.mango_program_id.as_str()).unwrap();
    let mango_cache_pk = Pubkey::from_str(mango_group.mango_cache.to_string().as_str()).unwrap();
    let mango_cache = load_from_rpc::<MangoCache>(&rpc_client, &mango_cache_pk);

    mango_group_config
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

            let root_bank = &mango_group_config.tokens[market_index].root_key;
            let root_bank = Pubkey::from_str(root_bank.as_str()).unwrap();
            let node_banks = mango_group_config.tokens[market_index]
                .node_keys
                .iter()
                .map(|x| Pubkey::from_str(x.as_str()).unwrap())
                .collect();
            PerpMarketCache {
                order_base_lots,
                price,
                price_quote_lots,
                mango_program_pk,
                mango_group_pk,
                mango_cache_pk,
                perp_market_pk,
                perp_market,
                root_bank,
                node_banks,
            }
        })
        .collect()
}
