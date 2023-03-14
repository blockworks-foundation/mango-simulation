use {
    log::info,
    serde_json,
    mango_simulation::{
        cli,
        confirmation_strategies::confirmations_by_blocks,
        crank,
        helpers::{
            get_latest_blockhash, get_mango_market_perps_cache, start_blockhash_polling_service,
            to_sdk_pk,
        },
        keeper::start_keepers,
        mango::{AccountKeys, MangoConfig},
        market_markers::start_market_making_threads,
        result_writer::initialize_result_writers,
        states::PerpMarketCache,
        stats::MangoSimulationStats,
        websocket_source::KeeperConfig,
    },
    solana_client::{
        connection_cache::ConnectionCache, nonblocking::rpc_client::RpcClient as NbRpcClient,
        rpc_client::RpcClient, tpu_client::TpuClient,
    },
    solana_program::pubkey::Pubkey,
    solana_sdk::commitment_config::CommitmentConfig,
    std::{
        fs,
        net::{IpAddr, Ipv4Addr},
        str::FromStr,
        sync::atomic::{AtomicBool, AtomicU64, Ordering},
        sync::Arc,
        time::Duration,
    },
    tokio::{sync::RwLock, task::JoinHandle},
};

#[tokio::main(flavor = "multi_thread", worker_threads = 10)]
pub async fn main() -> anyhow::Result<()> {
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
        mango_cluster,
        txs_batch_size,
        priority_fees_proba,
        keeper_authority,
        number_of_markers_per_mm,
        ..
    } = &cli_config;
    let number_of_markers_per_mm = *number_of_markers_per_mm;

    let transaction_save_file = transaction_save_file.clone();
    let block_data_save_file = block_data_save_file.clone();

    info!(
        "Connecting to the cluster {}, {}",
        json_rpc_url, websocket_url
    );

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

    let rpc_client = Arc::new(RpcClient::new_with_commitment(
        json_rpc_url.to_string(),
        CommitmentConfig::confirmed(),
    ));

    let nb_rpc_client = Arc::new(NbRpcClient::new_with_commitment(
        json_rpc_url.to_string(),
        CommitmentConfig::confirmed(),
    ));

    let connection_cache = ConnectionCache::new_with_client_options(
        4,
        None,
        Some((id, IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)))),
        None,
    );
    let quic_connection_cache = if let ConnectionCache::Quic(connection_cache) = connection_cache {
        Some(connection_cache)
    } else {
        None
    };

    let tpu_client = Arc::new(
        TpuClient::new_with_connection_cache(
            rpc_client.clone(),
            &websocket_url,
            solana_client::tpu_client::TpuClientConfig::default(),
            quic_connection_cache.unwrap(),
        )
        .unwrap(),
    );

    info!(
        "accounts:{:?} markets:{:?} quotes_per_second:{:?} expected_tps:{:?} duration:{:?}",
        account_keys_parsed.len(),
        mango_group_config.perp_markets.len(),
        quotes_per_second,
        account_keys_parsed.len()
            * number_of_markers_per_mm as usize
            * quotes_per_second.clone() as usize,
        duration
    );

    let nb_users = account_keys_parsed.len();

    let mango_sim_stats = MangoSimulationStats::new(
        nb_users,
        *quotes_per_second as usize,
        number_of_markers_per_mm as usize,
        duration.as_secs() as usize,
    );

    // continuosly fetch blockhash
    let rpc_client = Arc::new(RpcClient::new_with_commitment(
        json_rpc_url.to_string(),
        CommitmentConfig::confirmed(),
    ));
    let exit_signal = Arc::new(AtomicBool::new(false));
    let latest_blockhash = get_latest_blockhash(&rpc_client.clone()).await;
    let blockhash = Arc::new(RwLock::new(latest_blockhash));
    let current_slot = Arc::new(AtomicU64::new(0));
    let blockhash_thread = start_blockhash_polling_service(
        exit_signal.clone(),
        blockhash.clone(),
        current_slot.clone(),
        rpc_client.clone(),
    );
    let mango_program_pk = Pubkey::from_str(mango_group_config.mango_program_id.as_str()).unwrap();
    let perp_market_caches: Vec<PerpMarketCache> =
        get_mango_market_perps_cache(rpc_client.clone(), mango_group_config, &mango_program_pk);

    let quote_root_bank =
        Pubkey::from_str(mango_group_config.tokens.last().unwrap().root_key.as_str()).unwrap();
    let quote_node_banks = mango_group_config
        .tokens
        .last()
        .unwrap()
        .node_keys
        .iter()
        .map(|x| Pubkey::from_str(x.as_str()).unwrap())
        .collect();
    // start keeper if keeper authority is present
    let keepers_jl = if let Some(keeper_authority) = keeper_authority {
        let jl = start_keepers(
            exit_signal.clone(),
            tpu_client.clone(),
            perp_market_caches.clone(),
            blockhash.clone(),
            keeper_authority,
            quote_root_bank,
            quote_node_banks,
        );
        Some(jl)
    } else {
        None
    };

    let (tx_record_sx, tx_record_rx) = tokio::sync::mpsc::unbounded_channel();
    let from_slot = current_slot.load(Ordering::Relaxed);
    let keeper_config = KeeperConfig {
        program_id: to_sdk_pk(&mango_program_pk),
        rpc_url: json_rpc_url.clone(),
        websocket_url: websocket_url.clone(),
    };

    crank::start(
        keeper_config,
        exit_signal.clone(),
        blockhash.clone(),
        tpu_client.clone(),
        mango_group_config,
        id,
    );

    let warmup_duration = Duration::from_secs(10);
    info!("waiting for keepers to warmup for {warmup_duration:?}");
    tokio::time::sleep(warmup_duration).await;

    let mut tasks: Vec<JoinHandle<()>> = start_market_making_threads(
        account_keys_parsed.clone(),
        perp_market_caches.clone(),
        tx_record_sx.clone(),
        exit_signal.clone(),
        blockhash.clone(),
        current_slot.clone(),
        tpu_client.clone(),
        &duration,
        *quotes_per_second,
        *priority_fees_proba,
        number_of_markers_per_mm,
    );

    info!("Number of MM threads {}", tasks.len());
    drop(tx_record_sx);

    tasks.push(blockhash_thread);
    if let Some(keepers_jl) = keepers_jl {
        tasks.push(keepers_jl);
    }

    let (tx_status_sx, tx_status_rx) = async_channel::unbounded();
    let (block_status_sx, block_status_rx) = async_channel::unbounded();

    let mut writers_jh = initialize_result_writers(
        transaction_save_file,
        block_data_save_file,
        tx_status_rx.clone(),
        block_status_rx.clone(),
    );
    tasks.append(&mut writers_jh);

    let stats_handle =
        mango_sim_stats.update_from_tx_status_stream(tx_status_rx.clone(), exit_signal.clone());
    tasks.push(stats_handle);

    let mut confirmation_threads = confirmations_by_blocks(
        nb_rpc_client,
        tx_record_rx,
        tx_status_sx,
        block_status_sx,
        from_slot,
    );
    tasks.append(&mut confirmation_threads);

    let nb_tasks = tasks.len();
    for i in 0..nb_tasks {
        println!("waiting for {}", i);
        let task = tasks.remove(0);
        let _ = task.await;
    }
    mango_sim_stats.report("Mango simulation stats");
    Ok(())
}
