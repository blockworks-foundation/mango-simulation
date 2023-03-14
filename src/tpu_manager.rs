use log::info;
use solana_client::nonblocking::rpc_client::RpcClient;
use solana_client::{connection_cache::ConnectionCache, nonblocking::tpu_client::TpuClient};
use solana_quic_client::{QuicConfig, QuicConnectionManager, QuicPool};
use solana_sdk::signature::Keypair;
use std::{
    net::{IpAddr, Ipv4Addr},
    sync::{
        atomic::{AtomicU32, Ordering},
        Arc,
    },
};
use tokio::sync::RwLock;

pub type QuicTpuClient = TpuClient<QuicPool, QuicConnectionManager, QuicConfig>;
pub type QuicConnectionCache = ConnectionCache;

const TPU_CONNECTION_CACHE_SIZE: usize = 8;

#[derive(Clone)]
pub struct TpuManager {
    error_count: Arc<AtomicU32>,
    rpc_client: Arc<RpcClient>,
    // why arc twice / one is so that we clone rwlock and other so that we can clone tpu client
    tpu_client: Arc<RwLock<Arc<QuicTpuClient>>>,
    pub ws_addr: String,
    fanout_slots: u64,
    identity: Arc<Keypair>,
}

impl TpuManager {
    pub async fn new(
        rpc_client: Arc<RpcClient>,
        ws_addr: String,
        fanout_slots: u64,
        identity: Keypair,
    ) -> anyhow::Result<Self> {
        let connection_cache = ConnectionCache::new_with_client_options(
            4,
            None,
            Some((&identity, IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)))),
            None,
        );
        let quic_connection_cache =
            if let ConnectionCache::Quic(connection_cache) = connection_cache {
                Some(connection_cache)
            } else {
                None
            };

        let tpu_client = Arc::new(
            TpuClient::new_with_connection_cache(
                rpc_client.clone(),
                &ws_addr,
                solana_client::tpu_client::TpuClientConfig { fanout_slots },
                quic_connection_cache.unwrap(),
            )
            .await
            .unwrap(),
        );

        Ok(Self {
            rpc_client,
            tpu_client: Arc::new(RwLock::new(tpu_client)),
            ws_addr,
            fanout_slots,
            error_count: Default::default(),
            identity: Arc::new(identity),
        })
    }

    pub async fn reset_tpu_client(&self) -> anyhow::Result<()> {
        let identity = Keypair::from_bytes(&self.identity.to_bytes()).unwrap();
        let connection_cache = ConnectionCache::new_with_client_options(
            4,
            None,
            Some((&identity, IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)))),
            None,
        );
        let quic_connection_cache =
            if let ConnectionCache::Quic(connection_cache) = connection_cache {
                Some(connection_cache)
            } else {
                None
            };

        let tpu_client = Arc::new(
            TpuClient::new_with_connection_cache(
                self.rpc_client.clone(),
                &self.ws_addr,
                solana_client::tpu_client::TpuClientConfig {
                    fanout_slots: self.fanout_slots,
                },
                quic_connection_cache.unwrap(),
            )
            .await
            .unwrap(),
        );
        self.error_count.store(0, Ordering::Relaxed);
        *self.tpu_client.write().await = tpu_client;
        Ok(())
    }

    pub async fn reset(&self) -> anyhow::Result<()> {
        self.error_count.fetch_add(1, Ordering::Relaxed);

        if self.error_count.load(Ordering::Relaxed) > 5 {
            self.reset_tpu_client().await?;
            info!("TPU Reset after 5 errors");
        }

        Ok(())
    }

    async fn get_tpu_client(&self) -> Arc<QuicTpuClient> {
        self.tpu_client.read().await.clone()
    }

    pub async fn try_send_wire_transaction_batch(
        &self,
        wire_transactions: Vec<Vec<u8>>,
    ) -> anyhow::Result<()> {
        let tpu_client = self.get_tpu_client().await;
        match tpu_client
            .try_send_wire_transaction_batch(wire_transactions)
            .await
        {
            Ok(_) => Ok(()),
            Err(err) => {
                self.reset().await?;
                Err(err.into())
            }
        }
    }
}
