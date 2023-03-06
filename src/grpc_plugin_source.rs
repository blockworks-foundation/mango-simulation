use futures::stream::once;
use geyser::geyser_client::GeyserClient;
use jsonrpc_core::futures::StreamExt;
use jsonrpc_core_client::transports::http;

use serde::Deserialize;
use solana_account_decoder::{UiAccount, UiAccountEncoding};
use solana_client::rpc_config::{RpcAccountInfoConfig, RpcProgramAccountsConfig};
use solana_client::rpc_response::{OptionalContext, RpcKeyedAccount};
use solana_rpc::rpc::rpc_accounts::AccountsDataClient;
use solana_rpc::rpc::rpc_accounts_scan::AccountsScanClient;
use solana_sdk::{account::Account, commitment_config::CommitmentConfig, pubkey::Pubkey};

use futures::{future, future::FutureExt};
use tonic::{
    metadata::MetadataValue,
    transport::{Certificate, Channel, ClientTlsConfig, Identity},
    Request,
};

use log::*;
use std::{collections::HashMap, env, str::FromStr, time::Duration};

pub mod geyser {
    tonic::include_proto!("geyser");
}

pub mod solana {
    pub mod storage {
        pub mod confirmed_block {
            tonic::include_proto!("solana.storage.confirmed_block");
        }
    }
}

pub use geyser::*;
pub use solana::storage::confirmed_block::*;

use crate::{
    chain_data::{AccountWrite, SlotStatus, SlotUpdate},
    metrics::{MetricType, Metrics},
    AnyhowWrap,
};

#[derive(Clone, Debug, Deserialize)]
pub struct SourceConfig {
    pub dedup_queue_size: usize,
    pub grpc_sources: Vec<GrpcSourceConfig>,
    pub snapshot: SnapshotSourceConfig,
    pub rpc_ws_url: String,
}

#[derive(Clone, Debug, Deserialize)]
pub struct GrpcSourceConfig {
    pub name: String,
    pub connection_string: String,
    pub retry_connection_sleep_secs: u64,
    pub token: String,
    pub tls: Option<TlsConfig>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct TlsConfig {
    pub ca_cert_path: String,
    pub client_cert_path: String,
    pub client_key_path: String,
    pub domain_name: String,
}

#[derive(Clone, Debug, Deserialize)]
pub struct FilterConfig {
    pub program_ids: Vec<String>,
    pub account_ids: Vec<String>,
}

#[derive(Clone, Debug, Deserialize)]
pub struct SnapshotSourceConfig {
    pub rpc_http_url: String,
    pub program_id: String,
}

//use solana_geyser_connector_plugin_grpc::compression::zstd_decompress;

struct SnapshotData {
    slot: u64,
    accounts: Vec<(String, Option<UiAccount>)>,
}
enum Message {
    GrpcUpdate(geyser::SubscribeUpdate),
    Snapshot(SnapshotData),
}

async fn get_snapshot_gpa(
    rpc_http_url: String,
    program_id: String,
) -> anyhow::Result<OptionalContext<Vec<RpcKeyedAccount>>> {
    let rpc_client = http::connect_with_options::<AccountsScanClient>(&rpc_http_url, true)
        .await
        .map_err_anyhow()?;

    let account_info_config = RpcAccountInfoConfig {
        encoding: Some(UiAccountEncoding::Base64),
        commitment: Some(CommitmentConfig::finalized()),
        data_slice: None,
        min_context_slot: None,
    };
    let program_accounts_config = RpcProgramAccountsConfig {
        filters: None,
        with_context: Some(true),
        account_config: account_info_config.clone(),
    };

    info!("requesting snapshot {}", program_id);
    let account_snapshot = rpc_client
        .get_program_accounts(program_id.clone(), Some(program_accounts_config.clone()))
        .await
        .map_err_anyhow()?;
    info!("snapshot received {}", program_id);
    Ok(account_snapshot)
}

async fn get_snapshot_gma(
    rpc_http_url: String,
    ids: Vec<String>,
) -> anyhow::Result<solana_client::rpc_response::Response<Vec<Option<UiAccount>>>> {
    let rpc_client = http::connect_with_options::<AccountsDataClient>(&rpc_http_url, true)
        .await
        .map_err_anyhow()?;

    let account_info_config = RpcAccountInfoConfig {
        encoding: Some(UiAccountEncoding::Base64),
        commitment: Some(CommitmentConfig::finalized()),
        data_slice: None,
        min_context_slot: None,
    };

    info!("requesting snapshot {:?}", ids);
    let account_snapshot = rpc_client
        .get_multiple_accounts(ids.clone(), Some(account_info_config))
        .await
        .map_err_anyhow()?;
    info!("snapshot received {:?}", ids);
    Ok(account_snapshot)
}

async fn feed_data_geyser(
    grpc_config: &GrpcSourceConfig,
    snapshot_config: &SnapshotSourceConfig,
    filter_config: &FilterConfig,
    sender: async_channel::Sender<Message>,
) -> anyhow::Result<()> {
    let connection_string = match &grpc_config.connection_string.chars().next().unwrap() {
        '$' => env::var(&grpc_config.connection_string[1..])
            .expect("reading connection string from env"),
        _ => grpc_config.connection_string.clone(),
    };
    let rpc_http_url = match &snapshot_config.rpc_http_url.chars().next().unwrap() {
        '$' => env::var(&snapshot_config.rpc_http_url[1..])
            .expect("reading connection string from env"),
        _ => snapshot_config.rpc_http_url.clone(),
    };
    info!("connecting to {} tls={:?}", connection_string, grpc_config.tls);
    let endpoint = Channel::from_shared(connection_string)?;
    let channel = if let Some(tls) = &grpc_config.tls {
        endpoint.tls_config(make_tls_config(&tls))?
    } else {
        endpoint
    }
    .connect()
    .await?;
    let token: MetadataValue<_> = grpc_config.token.parse()?;
    let mut client = GeyserClient::with_interceptor(channel, move |mut req: Request<()>| {
        req.metadata_mut().insert("x-token", token.clone());
        Ok(req)
    });

    // If account_ids are provided, snapshot will be gMA. If only program_ids, then only the first id will be snapshot
    // TODO: handle this better
    if filter_config.program_ids.len() > 1 {
        warn!("only one program id is supported for gPA snapshots")
    }
    let mut accounts = HashMap::new();
    accounts.insert(
        "client".to_owned(),
        SubscribeRequestFilterAccounts {
            account: filter_config.account_ids.clone(),
            owner: filter_config.program_ids.clone(),
        },
    );
    let mut slots = HashMap::new();
    slots.insert("client".to_owned(), SubscribeRequestFilterSlots {});
    let blocks = HashMap::new();
    let blocks_meta = HashMap::new();
    let transactions = HashMap::new();
    

    let request = SubscribeRequest {
        accounts,
        blocks,
        blocks_meta,
        slots,
        transactions,
    };
    info!("Going to send request: {:?}", request);

    let response = client.subscribe(once(async move { request })).await?;
    let mut update_stream = response.into_inner();

    // We can't get a snapshot immediately since the finalized snapshot would be for a
    // slot in the past and we'd be missing intermediate updates.
    //
    // Delay the request until the first slot we received all writes for becomes rooted
    // to avoid that problem - partially. The rooted slot will still be larger than the
    // finalized slot, so add a number of slots as a buffer.
    //
    // If that buffer isn't sufficient, there'll be a retry.

    // The first slot that we will receive _all_ account writes for
    let mut first_full_slot: u64 = u64::MAX;

    // If a snapshot should be performed when ready.
    let mut snapshot_needed = true;

    // The highest "rooted" slot that has been seen.
    let mut max_rooted_slot = 0;

    // Data for slots will arrive out of order. This value defines how many
    // slots after a slot was marked "rooted" we assume it'll not receive
    // any more account write information.
    //
    // This is important for the write_version mapping (to know when slots can
    // be dropped).
    let max_out_of_order_slots = 40;

    // Number of slots that we expect "finalized" commitment to lag
    // behind "rooted". This matters for getProgramAccounts based snapshots,
    // which will have "finalized" commitment.
    let mut rooted_to_finalized_slots = 30;

    let mut snapshot_gma = future::Fuse::terminated();
    let mut snapshot_gpa = future::Fuse::terminated();

    // The plugin sends a ping every 5s or so
    let fatal_idle_timeout = Duration::from_secs(60);

    // Highest slot that an account write came in for.
    let mut newest_write_slot: u64 = 0;

    struct WriteVersion {
        // Write version seen on-chain
        global: u64,
        // The per-pubkey per-slot write version
        slot: u32,
    }

    // map slot -> (pubkey -> WriteVersion)
    //
    // Since the write_version is a private indentifier per node it can't be used
    // to deduplicate events from multiple nodes. Here we rewrite it such that each
    // pubkey and each slot has a consecutive numbering of writes starting at 1.
    //
    // That number will be consistent for each node.
    let mut slot_pubkey_writes = HashMap::<u64, HashMap<[u8; 32], WriteVersion>>::new();

    loop {
        tokio::select! {
            update = update_stream.next() => {
                use geyser::{subscribe_update::UpdateOneof};
                let mut update = update.ok_or(anyhow::anyhow!("geyser plugin has closed the stream"))??;
                match update.update_oneof.as_mut().expect("invalid grpc") {
                    UpdateOneof::Slot(slot_update) => {
                        let status = slot_update.status;
                        if status == SubscribeUpdateSlotStatus::Finalized as i32 {
                            if first_full_slot == u64::MAX {
                                // TODO: is this equivalent to before? what was highesy_write_slot?
                                first_full_slot = slot_update.slot + 1;
                            }
                            if slot_update.slot > max_rooted_slot {
                                max_rooted_slot = slot_update.slot;

                                // drop data for slots that are well beyond rooted
                                slot_pubkey_writes.retain(|&k, _| k >= max_rooted_slot - max_out_of_order_slots);
                            }

                            if snapshot_needed && max_rooted_slot - rooted_to_finalized_slots > first_full_slot {
                                snapshot_needed = false;
                                if filter_config.account_ids.len() > 0 {
                                    snapshot_gma = tokio::spawn(get_snapshot_gma(rpc_http_url.clone(), filter_config.account_ids.clone())).fuse();
                                } else if filter_config.program_ids.len() > 0 {
                                    snapshot_gpa = tokio::spawn(get_snapshot_gpa(rpc_http_url.clone(), filter_config.program_ids[0].clone())).fuse();
                                }
                            }
                        }
                    },
                    UpdateOneof::Account(info) => {
                        if info.slot < first_full_slot {
                            // Don't try to process data for slots where we may have missed writes:
                            // We could not map the write_version correctly for them.
                            continue;
                        }

                        if info.slot > newest_write_slot {
                            newest_write_slot = info.slot;
                        } else if max_rooted_slot > 0 && info.slot < max_rooted_slot - max_out_of_order_slots {
                            anyhow::bail!("received write {} slots back from max rooted slot {}", max_rooted_slot - info.slot, max_rooted_slot);
                        }

                        let pubkey_writes = slot_pubkey_writes.entry(info.slot).or_default();
                        let mut write = match info.account.clone() {
                            Some(x) => x,
                            None => {
                                // TODO: handle error
                                continue;
                            },
                        };

                        let pubkey_bytes = Pubkey::new(&write.pubkey).to_bytes();
                        let write_version_mapping = pubkey_writes.entry(pubkey_bytes).or_insert(WriteVersion {
                            global: write.write_version,
                            slot: 1, // write version 0 is reserved for snapshots
                        });

                        // We assume we will receive write versions for each pubkey in sequence.
                        // If this is not the case, logic here does not work correctly because
                        // a later write could arrive first.
                        if write.write_version < write_version_mapping.global {
                            anyhow::bail!("unexpected write version: got {}, expected >= {}", write.write_version, write_version_mapping.global);
                        }

                        // Rewrite the update to use the local write version and bump it
                        write.write_version = write_version_mapping.slot as u64;
                        write_version_mapping.slot += 1;
                    },
                    UpdateOneof::Ping(_) => {},
                    UpdateOneof::Block(_) => {},
                    UpdateOneof::BlockMeta(_) => {},
                    UpdateOneof::Transaction(_) => {},
                }
                sender.send(Message::GrpcUpdate(update)).await.expect("send success");
            },
            snapshot = &mut snapshot_gma => {
                let snapshot = snapshot??;
                info!("snapshot is for slot {}, first full slot was {}", snapshot.context.slot, first_full_slot);
                if snapshot.context.slot >= first_full_slot {
                    let accounts: Vec<(String, Option<UiAccount>)> = filter_config.account_ids.iter().zip(snapshot.value).map(|x| (x.0.clone(), x.1)).collect();
                    sender
                    .send(Message::Snapshot(SnapshotData {
                        accounts,
                        slot: snapshot.context.slot,
                    }))
                    .await
                    .expect("send success");
                } else {
                    info!(
                        "snapshot is too old: has slot {}, expected {} minimum",
                        snapshot.context.slot,
                        first_full_slot
                    );
                    // try again in another 10 slots
                    snapshot_needed = true;
                    rooted_to_finalized_slots += 10;
                }
            },
            snapshot = &mut snapshot_gpa => {
                let snapshot = snapshot??;
                if let OptionalContext::Context(snapshot_data) = snapshot {
                    info!("snapshot is for slot {}, first full slot was {}", snapshot_data.context.slot, first_full_slot);
                    if snapshot_data.context.slot >= first_full_slot {
                        let accounts: Vec<(String, Option<UiAccount>)> = snapshot_data.value.iter().map(|x| {
                            let deref = x.clone();
                            (deref.pubkey, Some(deref.account))
                        }).collect();
                        sender
                        .send(Message::Snapshot(SnapshotData {
                            accounts,
                            slot: snapshot_data.context.slot,
                        }))
                        .await
                        .expect("send success");
                    } else {
                        info!(
                            "snapshot is too old: has slot {}, expected {} minimum",
                            snapshot_data.context.slot,
                            first_full_slot
                        );
                        // try again in another 10 slots
                        snapshot_needed = true;
                        rooted_to_finalized_slots += 10;
                    }
                } else {
                    anyhow::bail!("bad snapshot format");
                }
            },
            _ = tokio::time::sleep(fatal_idle_timeout) => {
                anyhow::bail!("geyser plugin hasn't sent a message in too long");
            }
        }
    }
}

fn make_tls_config(config: &TlsConfig) -> ClientTlsConfig {
    let server_root_ca_cert = match &config.ca_cert_path.chars().next().unwrap() {
        '$' => env::var(&config.ca_cert_path[1..])
            .expect("reading server root ca cert from env")
            .into_bytes(),
        _ => std::fs::read(&config.ca_cert_path).expect("reading server root ca cert from file"),
    };
    let server_root_ca_cert = Certificate::from_pem(server_root_ca_cert);
    let client_cert = match &config.client_cert_path.chars().next().unwrap() {
        '$' => env::var(&config.client_cert_path[1..])
            .expect("reading client cert from env")
            .into_bytes(),
        _ => std::fs::read(&config.client_cert_path).expect("reading client cert from file"),
    };
    let client_key = match &config.client_key_path.chars().next().unwrap() {
        '$' => env::var(&config.client_key_path[1..])
            .expect("reading client key from env")
            .into_bytes(),
        _ => std::fs::read(&config.client_key_path).expect("reading client key from file"),
    };
    let client_identity = Identity::from_pem(client_cert, client_key);
    let domain_name = match &config.domain_name.chars().next().unwrap() {
        '$' => env::var(&config.domain_name[1..]).expect("reading domain name from env"),
        _ => config.domain_name.clone(),
    };
    ClientTlsConfig::new()
        .ca_certificate(server_root_ca_cert)
        .identity(client_identity)
        .domain_name(domain_name)
}

pub async fn process_events(
    config: &SourceConfig,
    filter_config: &FilterConfig,
    account_write_queue_sender: async_channel::Sender<AccountWrite>,
    slot_queue_sender: async_channel::Sender<SlotUpdate>,
    metrics_sender: Metrics,
) {
    // Subscribe to geyser
    let (msg_sender, msg_receiver) = async_channel::bounded::<Message>(config.dedup_queue_size);
    for grpc_source in config.grpc_sources.clone() {
        let msg_sender = msg_sender.clone();
        let snapshot_source = config.snapshot.clone();
        let metrics_sender = metrics_sender.clone();
        let filter_config = filter_config.clone();

        tokio::spawn(async move {
            let mut metric_retries = metrics_sender.register_u64(
                format!("grpc_source_{}_connection_retries", grpc_source.name,),
                MetricType::Counter,
            );
            let metric_connected =
                metrics_sender.register_bool(format!("grpc_source_{}_status", grpc_source.name));

            // Continuously reconnect on failure
            loop {
                metric_connected.set(true);
                let out = feed_data_geyser(
                    &grpc_source,
                    &snapshot_source,
                    &filter_config,
                    msg_sender.clone(),
                );
                let result = out.await;
                assert!(result.is_err());
                if let Err(err) = result {
                    warn!(
                        "error during communication with the geyser plugin. retrying. {:?}",
                        err
                    );
                }

                metric_connected.set(false);
                metric_retries.increment();

                tokio::time::sleep(std::time::Duration::from_secs(
                    grpc_source.retry_connection_sleep_secs,
                ))
                .await;
            }
        });
    }

    // slot -> (pubkey -> write_version)
    //
    // To avoid unnecessarily sending requests to SQL, we track the latest write_version
    // for each (slot, pubkey). If an already-seen write_version comes in, it can be safely
    // discarded.
    let mut latest_write = HashMap::<u64, HashMap<[u8; 32], u64>>::new();

    // Number of slots to retain in latest_write
    let latest_write_retention = 50;

    let mut metric_account_writes =
        metrics_sender.register_u64("grpc_account_writes".into(), MetricType::Counter);
    let mut metric_account_queue =
        metrics_sender.register_u64("grpc_account_write_queue".into(), MetricType::Gauge);
    let mut metric_dedup_queue =
        metrics_sender.register_u64("grpc_dedup_queue".into(), MetricType::Gauge);
    let mut metric_slot_queue =
        metrics_sender.register_u64("grpc_slot_update_queue".into(), MetricType::Gauge);
    let mut metric_slot_updates =
        metrics_sender.register_u64("grpc_slot_updates".into(), MetricType::Counter);
    let mut metric_snapshots =
        metrics_sender.register_u64("grpc_snapshots".into(), MetricType::Counter);
    let mut metric_snapshot_account_writes =
        metrics_sender.register_u64("grpc_snapshot_account_writes".into(), MetricType::Counter);

    loop {
        metric_dedup_queue.set(msg_receiver.len() as u64);
        let msg = msg_receiver.recv().await.expect("sender must not close");
        use geyser::subscribe_update::UpdateOneof;
        match msg {
            Message::GrpcUpdate(update) => {
                match update.update_oneof.expect("invalid grpc") {
                    UpdateOneof::Account(info) => {
                        let update = match info.account.clone() {
                            Some(x) => x,
                            None => {
                                // TODO: handle error
                                continue;
                            }
                        };
                        assert!(update.pubkey.len() == 32);
                        assert!(update.owner.len() == 32);

                        metric_account_writes.increment();
                        metric_account_queue.set(account_write_queue_sender.len() as u64);

                        // Skip writes that a different server has already sent
                        let pubkey_writes = latest_write.entry(info.slot).or_default();
                        let pubkey_bytes = Pubkey::new(&update.pubkey).to_bytes();
                        let writes = pubkey_writes.entry(pubkey_bytes).or_insert(0);
                        if update.write_version <= *writes {
                            continue;
                        }
                        *writes = update.write_version;
                        latest_write.retain(|&k, _| k >= info.slot - latest_write_retention);
                        // let mut uncompressed: Vec<u8> = Vec::new();
                        // zstd_decompress(&update.data, &mut uncompressed).unwrap();
                        account_write_queue_sender
                            .send(AccountWrite {
                                pubkey: Pubkey::new(&update.pubkey),
                                slot: info.slot,
                                write_version: update.write_version,
                                lamports: update.lamports,
                                owner: Pubkey::new(&update.owner),
                                executable: update.executable,
                                rent_epoch: update.rent_epoch,
                                data: update.data,
                                // TODO: what should this be? related to account deletes?
                                is_selected: true,
                            })
                            .await
                            .expect("send success");
                    }
                    UpdateOneof::Slot(update) => {
                        metric_slot_updates.increment();
                        metric_slot_queue.set(slot_queue_sender.len() as u64);

                        let status =
                            SubscribeUpdateSlotStatus::from_i32(update.status).map(|v| match v {
                                SubscribeUpdateSlotStatus::Processed => SlotStatus::Processed,
                                SubscribeUpdateSlotStatus::Confirmed => SlotStatus::Confirmed,
                                SubscribeUpdateSlotStatus::Finalized => SlotStatus::Rooted,
                            });
                        if status.is_none() {
                            error!("unexpected slot status: {}", update.status);
                            continue;
                        }
                        let slot_update = SlotUpdate {
                            slot: update.slot,
                            parent: update.parent,
                            status: status.expect("qed"),
                        };

                        slot_queue_sender
                            .send(slot_update)
                            .await
                            .expect("send success");
                    }
                    UpdateOneof::Ping(_) => {}
                    UpdateOneof::Block(_) => {}
                    UpdateOneof::BlockMeta(_) => {}
                    UpdateOneof::Transaction(_) => {}
                }
            }
            Message::Snapshot(update) => {
                metric_snapshots.increment();
                info!("processing snapshot...");
                for account in update.accounts.iter() {
                    metric_snapshot_account_writes.increment();
                    metric_account_queue.set(account_write_queue_sender.len() as u64);

                    match account {
                        (key, Some(ui_account)) => {
                            // TODO: Resnapshot on invalid data?
                            let pubkey = Pubkey::from_str(key).unwrap();
                            let account: Account = ui_account.decode().unwrap();
                            account_write_queue_sender
                                .send(AccountWrite::from(pubkey, update.slot, 0, account))
                                .await
                                .expect("send success");
                        }
                        (key, None) => warn!("account not found {}", key),
                    }
                }
                info!("processing snapshot done");
            }
        }
    }
}
