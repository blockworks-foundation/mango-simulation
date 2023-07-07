use crate::{
    helpers::to_sp_pk,
    mango::GroupConfig,
    mango_v3_perp_crank_sink::MangoV3PerpCrankSink,
    noop,
    states::{KeeperInstruction, TransactionSendRecord},
    tpu_manager::TpuManager,
};

use mango_feeds_connector::{
    account_write_filter::{self, AccountWriteRoute},
    metrics, websocket_source, FilterConfig, MetricsConfig, SnapshotSourceConfig, SourceConfig,
};

use async_channel::unbounded;
use chrono::Utc;
use log::*;
use solana_sdk::{
    compute_budget::ComputeBudgetInstruction, hash::Hash, instruction::Instruction, pubkey::Pubkey,
    signature::Keypair, signer::Signer, transaction::Transaction,
};
use std::{
    str::FromStr,
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};
use tokio::sync::RwLock;

#[derive(Debug, Clone)]
pub struct KeeperConfig {
    pub program_id: Pubkey,
    pub rpc_url: String,
    pub websocket_url: String,
}

#[allow(clippy::too_many_arguments)]
pub fn start(
    config: KeeperConfig,
    exit_signal: Arc<AtomicBool>,
    blockhash: Arc<RwLock<Hash>>,
    current_slot: Arc<AtomicU64>,
    tpu_manager: TpuManager,
    group: &GroupConfig,
    identity: &Keypair,
    prioritization_fee: u64,
) {
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
    let cache_pk = Pubkey::from_str(&group.cache_key).unwrap();
    let mango_program_id = Pubkey::from_str(&group.mango_program_id).unwrap();
    let filter_config = FilterConfig {
        program_ids: vec![group.mango_program_id.clone()],
        account_ids: group
            .perp_markets
            .iter()
            .map(|m| m.events_key.clone())
            .collect(),
    };

    let (instruction_sender, instruction_receiver) = unbounded::<(Pubkey, Vec<Instruction>)>();
    let identity = Keypair::from_bytes(identity.to_bytes().as_slice()).unwrap();
    tokio::spawn(async move {
        info!(
            "crank-tx-sender signing with keypair pk={:?}",
            identity.pubkey()
        );

        loop {
            if exit_signal.load(Ordering::Acquire) {
                break;
            }

            if let Ok((market, mut ixs)) = instruction_receiver.recv().await {
                // add priority fees
                ixs.push(ComputeBudgetInstruction::set_compute_unit_price(
                    prioritization_fee,
                ));
                // add timestamp to guarantee unique transactions
                ixs.push(noop::timestamp());

                let tx = Transaction::new_signed_with_payer(
                    &ixs,
                    Some(&identity.pubkey()),
                    &[&identity],
                    *blockhash.read().await,
                );

                let tx_send_record = TransactionSendRecord {
                    signature: tx.signatures[0],
                    sent_at: Utc::now(),
                    sent_slot: current_slot.load(Ordering::Acquire),
                    market_maker: None,
                    market: Some(to_sp_pk(&market)),
                    priority_fees: prioritization_fee,
                    keeper_instruction: Some(KeeperInstruction::ConsumeEvents),
                };

                let tpu_manager = tpu_manager.clone();
                tpu_manager.send_transaction(&tx, tx_send_record).await;
            }
        }
    });

    tokio::spawn(async move {
        let metrics_tx = metrics::start(
            MetricsConfig {
                output_stdout: true,
                output_http: false,
            },
            "crank".into(),
        );

        let routes = vec![AccountWriteRoute {
            matched_pubkeys: perp_queue_pks
                .iter()
                .map(|(_, evq_pk)| {
                    mango_feeds_connector::solana_sdk::pubkey::Pubkey::new_from_array(
                        evq_pk.to_bytes(),
                    )
                })
                .collect(),
            sink: Arc::new(MangoV3PerpCrankSink::new(
                perp_queue_pks,
                group_pk,
                cache_pk,
                mango_program_id,
                instruction_sender,
            )),
            timeout_interval: Duration::default(),
        }];

        info!("matched_pks={:?}", routes[0].matched_pubkeys);

        let (account_write_queue_sender, slot_queue_sender) =
            account_write_filter::init(routes, metrics_tx.clone()).expect("filter initializes");

        // info!("start processing grpc events");

        // grpc_plugin_source::process_events(
        //     &config,
        //     &filter_config,
        //     account_write_queue_sender,
        //     slot_queue_sender,
        //     metrics_tx.clone(),
        // ).await;

        info!(
            "start processing websocket events program_id={:?} ws_url={:?}",
            config.program_id, config.websocket_url
        );

        websocket_source::process_events(
            &SourceConfig {
                dedup_queue_size: 0,
                grpc_sources: vec![],
                snapshot: SnapshotSourceConfig {
                    rpc_http_url: config.rpc_url,
                    program_id: config.program_id.to_string(),
                },
                rpc_ws_url: config.websocket_url,
            },
            &filter_config,
            account_write_queue_sender,
            slot_queue_sender,
        )
        .await;
    });
}
