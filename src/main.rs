use {
    log::info,
    mango_simulation::{
        cli,
        confirmation_strategies::confirmation_by_lite_rpc_notification_stream,
        crank::{self, KeeperConfig},
        helpers::{
            get_latest_blockhash, get_mango_market_perps_cache, start_blockhash_polling_service,
            to_sdk_pk,
        },
        keeper::start_keepers,
        mango::{AccountKeys, MangoConfig},
        market_markers::{clean_market_makers, start_market_making_threads},
        result_writer::initialize_result_writers,
        states::PerpMarketCache,
        stats::MangoSimulationStats,
        tpu_manager::TpuManager,
    },
    solana_client::nonblocking::rpc_client::RpcClient as NbRpcClient,
    solana_lite_rpc_core::{
        block_store::BlockStore,
        notifications::NotificationMsg,
        quic_connection_utils::QuicConnectionParameters,
        tx_store::{empty_tx_store, TxStore},
    },
    solana_lite_rpc_services::{
        block_listenser::BlockListener,
        tpu_utils::tpu_service::{TpuService, TpuServiceConfig},
        transaction_replayer::TransactionReplayer,
        transaction_service::{TransactionService, TransactionServiceBuilder},
        tx_sender::TxSender,
    },
    solana_program::pubkey::Pubkey,
    solana_sdk::{commitment_config::CommitmentConfig, signer::keypair::Keypair},
    std::{
        fs,
        str::FromStr,
        sync::atomic::{AtomicBool, AtomicU64, Ordering},
        sync::Arc,
        time::Duration,
    },
    tokio::sync::mpsc::{unbounded_channel, UnboundedSender},
    tokio::{sync::RwLock, task::JoinHandle},
};

const METRICS_NAME: &str = "mango-bencher";

async fn configure_transaction_service(
    rpc_client: Arc<NbRpcClient>,
    identity: Keypair,
    block_store: BlockStore,
    tx_store: TxStore,
    notifier: UnboundedSender<NotificationMsg>,
) -> (TransactionService, JoinHandle<anyhow::Result<()>>) {
    let slot = rpc_client.get_slot().await.expect("GetSlot should work");
    let tpu_config = TpuServiceConfig {
        fanout_slots: 12,
        number_of_leaders_to_cache: 1024,
        clusterinfo_refresh_time: Duration::from_secs(60 * 60),
        leader_schedule_update_frequency: Duration::from_secs(10),
        maximum_transaction_in_queue: 200_000,
        maximum_number_of_errors: 10,
        quic_connection_params: QuicConnectionParameters {
            connection_timeout: Duration::from_secs(1),
            connection_retry_count: 10,
            finalize_timeout: Duration::from_millis(200),
            max_number_of_connections: 10,
            unistream_timeout: Duration::from_millis(500),
            write_timeout: Duration::from_secs(1),
            number_of_transactions_per_unistream: 10,
        },
    };

    let tpu_service = TpuService::new(
        tpu_config,
        Arc::new(identity),
        slot,
        rpc_client.clone(),
        tx_store.clone(),
    )
    .await
    .expect("Should be able to create TPU");

    let tx_sender = TxSender::new(tx_store.clone(), tpu_service.clone());
    let block_listenser =
        BlockListener::new(rpc_client.clone(), tx_store.clone(), block_store.clone());
    let replayer = TransactionReplayer::new(
        tpu_service.clone(),
        tx_store.clone(),
        Duration::from_secs(2),
    );
    let builder = TransactionServiceBuilder::new(
        tx_sender,
        replayer,
        block_listenser,
        tpu_service,
        1_000_000,
    );
    builder.start(Some(notifier), block_store, 10, Duration::from_secs(90))
}

#[tokio::main(flavor = "multi_thread", worker_threads = 10)]
pub async fn main() -> anyhow::Result<()> {
    solana_logger::setup_with_default("info");
    solana_metrics::set_panic_hook("bench-mango", /*version:*/ None);

    let version = solana_version::version!();
    let matches = cli::build_args(version).get_matches();
    let cli_config = cli::extract_args(&matches);

    let cli::Config {
        json_rpc_url,
        websocket_url,
        identity,
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
        keeper_prioritization,
        ..
    } = &cli_config;
    let number_of_markers_per_mm = *number_of_markers_per_mm;
    let keeper_prioritization = *keeper_prioritization;

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
        .expect("Mango group config should exist");

    let nb_rpc_client = Arc::new(NbRpcClient::new_with_commitment(
        json_rpc_url.to_string(),
        CommitmentConfig::finalized(),
    ));

    let tx_store = empty_tx_store();
    let block_store = BlockStore::new(&nb_rpc_client)
        .await
        .expect("Blockstore should be created");

    let (notif_sx, notif_rx) = unbounded_channel();
    let (transaction_service, tx_service_jh) = configure_transaction_service(
        nb_rpc_client.clone(),
        Keypair::from_bytes(identity.to_bytes().as_slice()).unwrap(),
        block_store,
        tx_store,
        notif_sx,
    )
    .await;

    let nb_users = account_keys_parsed.len();

    let mut mango_sim_stats = MangoSimulationStats::new(
        nb_users,
        *quotes_per_second as usize,
        number_of_markers_per_mm as usize,
        duration.as_secs() as usize,
    );

    let (tx_record_sx, tx_record_rx) = tokio::sync::mpsc::unbounded_channel();

    // continuosly fetch blockhash
    let exit_signal = Arc::new(AtomicBool::new(false));
    let latest_blockhash = get_latest_blockhash(&nb_rpc_client.clone()).await;
    let blockhash = Arc::new(RwLock::new(latest_blockhash));
    let current_slot = Arc::new(AtomicU64::new(0));
    let blockhash_thread = start_blockhash_polling_service(
        exit_signal.clone(),
        blockhash.clone(),
        current_slot.clone(),
        nb_rpc_client.clone(),
    );

    let tpu_manager = TpuManager::new(
        transaction_service,
        mango_sim_stats.clone(),
        tx_record_sx.clone(),
    )
    .await?;

    info!(
        "accounts:{:?} markets:{:?} quotes_per_second:{:?} expected_tps:{:?} duration:{:?}",
        account_keys_parsed.len(),
        number_of_markers_per_mm,
        quotes_per_second,
        account_keys_parsed.len() * number_of_markers_per_mm as usize * *quotes_per_second as usize,
        duration
    );

    let mango_program_pk = Pubkey::from_str(mango_group_config.mango_program_id.as_str())
        .expect("Mango program should be able to convert into pubkey");
    let perp_market_caches: Vec<PerpMarketCache> =
        get_mango_market_perps_cache(nb_rpc_client.clone(), mango_group_config, &mango_program_pk)
            .await;

    let quote_root_bank =
        Pubkey::from_str(mango_group_config.tokens.last().unwrap().root_key.as_str())
            .expect("Quote root bank should be able to convert into pubkey");
    let quote_node_banks = mango_group_config
        .tokens
        .last()
        .unwrap()
        .node_keys
        .iter()
        .map(|x| {
            Pubkey::from_str(x.as_str()).expect("Token mint should be able to convert into pubkey")
        })
        .collect();

    clean_market_makers(
        nb_rpc_client.clone(),
        &account_keys_parsed,
        &perp_market_caches,
        blockhash.clone(),
    )
    .await;

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
            keeper_prioritization,
        );
        Some(jl)
    } else {
        None
    };

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
        identity,
        keeper_prioritization,
    );

    let warmup_duration = Duration::from_secs(20);
    info!("waiting for keepers to warmup for {warmup_duration:?}");
    tokio::time::sleep(warmup_duration).await;

    let mm_tasks: Vec<JoinHandle<()>> = start_market_making_threads(
        account_keys_parsed.clone(),
        perp_market_caches.clone(),
        exit_signal.clone(),
        blockhash.clone(),
        current_slot.clone(),
        tpu_manager.clone(),
        duration,
        *quotes_per_second,
        *priority_fees_proba,
        number_of_markers_per_mm,
    );

    info!("Number of MM threads {}", mm_tasks.len());
    drop(tx_record_sx);
    let mut tasks = vec![blockhash_thread];

    let (tx_status_sx, tx_status_rx) = tokio::sync::broadcast::channel(1000000);
    let (block_status_sx, block_status_rx) = tokio::sync::broadcast::channel(1000000);

    let stats_handle = mango_sim_stats.update_from_tx_status_stream(tx_status_rx);
    tasks.push(stats_handle);

    let mut writers_jh = initialize_result_writers(
        transaction_save_file,
        block_data_save_file,
        tx_status_sx.subscribe(),
        block_status_rx,
    );
    tasks.append(&mut writers_jh);

    let mut confirmation_threads = confirmation_by_lite_rpc_notification_stream(
        tx_record_rx,
        notif_rx,
        tx_status_sx,
        block_status_sx,
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
                mango_sim_stats.report(false, METRICS_NAME).await;
            }
        });
        tasks.push(reporting_thread);
    }

    // when all market makers tasks are joined that means we are ready to exit
    // we start stopping all other process
    // some processes like confirmation of transactions will take some time and will get additional 2 minutes
    // to confirm remaining transactions
    let market_makers_wait_task = {
        let exit_signal = exit_signal.clone();
        tokio::spawn(async move {
            futures::future::join_all(mm_tasks).await;
            info!("finished market making, joining all other services");
            exit_signal.store(true, Ordering::Relaxed);
        })
    };

    tasks.push(market_makers_wait_task);

    let transaction_service = tokio::spawn(async move {
        let _ = tx_service_jh.await;
        info!("Transaction service joined");
    });

    tokio::select! {
        _ = futures::future::join_all(tasks) => {},
        _ = transaction_service => {},
    };

    mango_sim_stats.report(true, METRICS_NAME).await;
    Ok(())
}
