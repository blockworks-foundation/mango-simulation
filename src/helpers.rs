use std::{
    ops::{Div, Mul},
    str::FromStr,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc,
    },
    time::{Duration, Instant},
};

use chrono::{DateTime, Utc};
use fixed::types::I80F48;
use log::{debug, info};
use mango::state::{MangoCache, MangoGroup, PerpMarket};
use mango_common::Loadable;
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_program::{clock::DEFAULT_MS_PER_SLOT, pubkey::Pubkey};
use solana_sdk::hash::Hash;
use tokio::{sync::RwLock, task::JoinHandle};

use crate::{mango::GroupConfig, states::PerpMarketCache};

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

pub async fn load_from_rpc<T: Loadable>(rpc_client: &RpcClient, pk: &Pubkey) -> T {
    let acc = rpc_client.get_account(&to_sdk_pk(pk)).await.unwrap();
    *T::load_from_bytes(acc.data.as_slice()).unwrap()
}

pub async fn get_latest_blockhash(rpc_client: &RpcClient) -> Hash {
    loop {
        match rpc_client.get_latest_blockhash().await {
            Ok(blockhash) => return blockhash,
            Err(err) => {
                info!("Couldn't get last blockhash: {:?}", err);
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        };
    }
}

pub async fn get_new_latest_blockhash(client: Arc<RpcClient>, blockhash: &Hash) -> Option<Hash> {
    let start = Instant::now();
    while start.elapsed().as_secs() < 5 {
        if let Ok(new_blockhash) = client.get_latest_blockhash().await {
            if new_blockhash != *blockhash {
                debug!("Got new blockhash ({:?})", blockhash);
                return Some(new_blockhash);
            }
        }
        debug!("Got same blockhash ({:?}), will retry...", blockhash);

        // Retry ~twice during a slot
        tokio::time::sleep(Duration::from_millis(DEFAULT_MS_PER_SLOT / 2)).await;
    }
    None
}

pub async fn poll_blockhash_and_slot(
    exit_signal: Arc<AtomicBool>,
    blockhash: Arc<RwLock<Hash>>,
    slot: &AtomicU64,
    client: Arc<RpcClient>,
) {
    let mut blockhash_last_updated = Instant::now();
    //let mut last_error_log = Instant::now();
    loop {
        let client = client.clone();
        let old_blockhash = *blockhash.read().await;
        if exit_signal.load(Ordering::Relaxed) {
            info!("Stopping blockhash thread");
            break;
        }

        match client.get_slot().await {
            Ok(new_slot) => slot.store(new_slot, Ordering::Release),
            Err(e) => {
                info!("Failed to download slot: {}, skip", e);
                continue;
            }
        }

        if let Some(new_blockhash) = get_new_latest_blockhash(client, &old_blockhash).await {
            {
                *blockhash.write().await = new_blockhash;
            }
            blockhash_last_updated = Instant::now();
        } else if blockhash_last_updated.elapsed().as_secs() > 120 {
            break;
        }

        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

pub fn seconds_since(dt: DateTime<Utc>) -> i64 {
    Utc::now().signed_duration_since(dt).num_seconds()
}

pub fn start_blockhash_polling_service(
    exit_signal: Arc<AtomicBool>,
    blockhash: Arc<RwLock<Hash>>,
    current_slot: Arc<AtomicU64>,
    client: Arc<RpcClient>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        poll_blockhash_and_slot(
            exit_signal,
            blockhash.clone(),
            current_slot.as_ref(),
            client,
        )
        .await;
    })
}

pub async fn get_mango_market_perps_cache(
    rpc_client: Arc<RpcClient>,
    mango_group_config: &GroupConfig,
    mango_program_pk: &Pubkey,
) -> Vec<PerpMarketCache> {
    // fetch group
    let mango_group_pk = Pubkey::from_str(mango_group_config.public_key.as_str()).unwrap();
    let mango_group = load_from_rpc::<MangoGroup>(&rpc_client, &mango_group_pk).await;
    let mango_cache_pk = Pubkey::from_str(mango_group.mango_cache.to_string().as_str()).unwrap();
    let mango_cache = load_from_rpc::<MangoCache>(&rpc_client, &mango_cache_pk).await;
    let mut ret = vec![];
    for market_index in 0..mango_group_config.perp_markets.len() {
        let perp_maket_config = &mango_group_config.perp_markets[market_index];
        let perp_market_pk = Pubkey::from_str(perp_maket_config.public_key.as_str()).unwrap();
        let perp_market = load_from_rpc::<PerpMarket>(&rpc_client, &perp_market_pk).await;

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
        let price_oracle =
            Pubkey::from_str(mango_group_config.oracles[market_index].public_key.as_str()).unwrap();
        ret.push(PerpMarketCache {
            order_base_lots,
            price,
            price_quote_lots,
            mango_program_pk: *mango_program_pk,
            mango_group_pk,
            mango_cache_pk,
            perp_market_pk,
            perp_market,
            root_bank,
            node_banks,
            price_oracle,
            bids: perp_market.bids,
            asks: perp_market.asks,
        });
    }
    ret
}
