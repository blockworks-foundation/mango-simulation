use std::{
    fs::File,
    io::Read,
    str::FromStr,
    sync::{Arc, RwLock},
    thread::{Builder, JoinHandle},
    time::Duration,
};

// use solana_client::rpc_client::RpcClient;
use crate::{
    account_write_filter::{self, AccountWriteRoute},
    grpc_plugin_source::{self, FilterConfig, SourceConfig},
    mango::GroupConfig,
    mango_v3_perp_crank_sink::MangoV3PerpCrankSink,
    metrics, blockhash_poller, transaction_sender,
};
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_sdk::{instruction::Instruction, pubkey::Pubkey, signature::Keypair};

fn start_crank_thread(identity: Keypair, group: GroupConfig) -> JoinHandle<()> {
    let perp_queue_pks: Vec<_> = group
        .perp_markets
        .iter()
        .map(|m| {
            (
                Pubkey::from_str(&m.public_key).unwrap(),
                Pubkey::from_str(&m.events_key).unwrap(),
            )
        })
        .collect();
    let group_pk = Pubkey::from_str(&group.public_key).unwrap();
    let cache_pk = Pubkey::from_str(&group.cache_pk).unwrap();
    let mango_program_id = Pubkey::from_str(&group.mango_program_id).unwrap();
    let filter_config = FilterConfig {
        program_ids: vec![],
        account_ids: group.perp_markets.iter().map(|m| m.events_key.clone()).collect(),
    };

    return Builder::new()
        .name("crank".to_string())
        .spawn(move || {
            let config: SourceConfig = {
                let mut file = File::open("source.toml").expect("source.toml file in cwd");
                let mut contents = String::new();
                file.read_to_string(&mut contents)
                    .expect("source.toml to contain data");
                toml::from_str(&contents).unwrap()
            };

            let metrics_tx = metrics::start(
                metrics::MetricsConfig {
                    output_stdout: true,
                    output_http: false,
                },
                "crank".into(),
            );

            let rpc_client = Arc::new(RpcClient::new("".into()));

            // TODO await future
            let blockhash = blockhash_poller::init(rpc_client.clone());

            // Event queue updates can be consumed by client connections
            let (instruction_sender, instruction_receiver) =
                async_channel::unbounded::<Vec<Instruction>>();

                transaction_sender::init(
                  instruction_receiver,
                  blockhash,
                  rpc_client,
                  identity
              );
            let routes = vec![AccountWriteRoute {
                matched_pubkeys: perp_queue_pks
                    .iter()
                    .map(|(_, evq_pk)| evq_pk.clone())
                    .collect(),
                sink: Arc::new(MangoV3PerpCrankSink::new(
                    perp_queue_pks,
                    group_pk,
                    cache_pk,
                    mango_program_id,
                    instruction_sender.clone(),
                )),
                timeout_interval: Duration::default(),
            }];

            let (account_write_queue_sender, slot_queue_sender) =
                account_write_filter::init(routes, metrics_tx.clone()).expect("filter initializes");

            // TODO figure out how to start tokio stuff here
            grpc_plugin_source::process_events(
                &config,
                &filter_config,
                account_write_queue_sender,
                slot_queue_sender,
                metrics_tx.clone(),
            );
            // .await;
        })
        .expect("launch crank thread");

    // TODO also implement websocket handler
    //   websocket_source::process_events(
    //     &config.source,
    //     account_write_queue_sender,
    //     slot_queue_sender,
    // )
}
