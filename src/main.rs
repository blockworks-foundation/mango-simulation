use {
    log::info,
    mango_simulation::{
        cli,
        confirmation_strategies::confirmations_by_blocks,
        crank::{self, KeeperConfig},
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
        tpu_manager::TpuManager,
    },
    serde_json,
    solana_client::{nonblocking::rpc_client::RpcClient as NbRpcClient, rpc_client::RpcClient},
    solana_program::pubkey::Pubkey,
    solana_sdk::{commitment_config::CommitmentConfig, signer::keypair::Keypair},
    std::{
        fs,
        str::FromStr,
        sync::atomic::{AtomicBool, AtomicU64, Ordering},
        sync::Arc,
        time::Duration,
    },
    tokio::{sync::RwLock, task::JoinHandle},
};

const METRICS_NAME: &str = "mango-bencher";

#[tokio::main(flavor = "multi_thread", worker_threads = 10)]
pub async fn main() -> anyhow::Result<()> {
    solana_logger::setup_with_default("info");
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

    let nb_rpc_client = Arc::new(NbRpcClient::new_with_commitment(
        json_rpc_url.to_string(),
        CommitmentConfig::confirmed(),
    ));

    let nb_users = account_keys_parsed.len();

    let mut mango_sim_stats = MangoSimulationStats::new(
        nb_users,
        *quotes_per_second as usize,
        number_of_markers_per_mm as usize,
        duration.as_secs() as usize,
    );

    let (tx_record_sx, tx_record_rx) = tokio::sync::mpsc::unbounded_channel();

    let tpu_manager = TpuManager::new(
        nb_rpc_client.clone(),
        websocket_url.clone(),
        solana_client::tpu_client::TpuClientConfig::default().fanout_slots,
        Keypair::from_bytes(id.to_bytes().as_slice()).unwrap(),
        mango_sim_stats.clone(),
        tx_record_sx.clone(),
    )
    .await;

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
            tpu_manager.clone(),
            perp_market_caches.clone(),
            blockhash.clone(),
            current_slot.clone(),
            keeper_authority,
            quote_root_bank,
            quote_node_banks,
            0,
        );
        Some(jl)
    } else {
        None
    };
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
        current_slot.clone(),
        tpu_manager.clone(),
        mango_group_config,
        id,
        0,
    );

    let warmup_duration = Duration::from_secs(10);
    info!("waiting for keepers to warmup for {warmup_duration:?}");
    tokio::time::sleep(warmup_duration).await;

    let mm_tasks: Vec<JoinHandle<()>> = start_market_making_threads(
        account_keys_parsed.clone(),
        perp_market_caches.clone(),
        exit_signal.clone(),
        blockhash.clone(),
        current_slot.clone(),
        tpu_manager,
        &duration,
        *quotes_per_second,
        *priority_fees_proba,
        number_of_markers_per_mm,
    );

    info!("Number of MM threads {}", mm_tasks.len());
    drop(tx_record_sx);
    let mut tasks = vec![];
    tasks.push(blockhash_thread);

    let (tx_status_sx, tx_status_rx) = tokio::sync::broadcast::channel(1024);
    let (block_status_sx, block_status_rx) = tokio::sync::broadcast::channel(1024);

    let mut writers_jh = initialize_result_writers(
        transaction_save_file,
        block_data_save_file,
        tx_status_rx,
        block_status_rx,
    );
    tasks.append(&mut writers_jh);

    let stats_handle = mango_sim_stats.update_from_tx_status_stream(tx_status_sx.subscribe());
    tasks.push(stats_handle);

    let mut confirmation_threads = confirmations_by_blocks(
        nb_rpc_client,
        tx_record_rx,
        tx_status_sx,
        block_status_sx,
        from_slot,
        exit_signal.clone(),
    );
    tasks.append(&mut confirmation_threads);

    if let Some(keepers_jl) = keepers_jl {
        tasks.push(keepers_jl);
    }

    {
        let exit_signal = exit_signal.clone();
        let mut mango_sim_stats = mango_sim_stats.clone();
        let reporting_thread = tokio::spawn(async move {
            loop {
                if exit_signal.load(Ordering::Relaxed) {
                    break;
                }
                tokio::time::sleep(Duration::from_secs(60)).await;
                mango_sim_stats.report(false, METRICS_NAME);
            }
        });
        tasks.push(reporting_thread);
    }

    // when all market makers tasks are joined that means we are ready to exit
    // we start stopping all other process
    // some processes like confirmation of transactions will take some time and will get additional 2 minutes
    // to confirm remaining transactions
    futures::future::join_all(mm_tasks).await;
    info!("finished market making, joining all other services");
    println!("finished market making, joining all other services");
    exit_signal.store(true, Ordering::Relaxed);

    futures::future::join_all(tasks).await;
    mango_sim_stats.report(true, METRICS_NAME);
    Ok(())
}
