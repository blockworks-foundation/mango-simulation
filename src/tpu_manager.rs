use log::warn;
use solana_client::connection_cache::ConnectionCache;
use solana_lite_rpc_services::transaction_service::TransactionService;
use solana_sdk::transaction::Transaction;

use tokio::sync::mpsc::UnboundedSender;

use crate::{states::TransactionSendRecord, stats::MangoSimulationStats};
pub type QuicConnectionCache = ConnectionCache;

#[derive(Clone)]
pub struct TpuManager {
    // why arc twice / one is so that we clone rwlock and other so that we can clone tpu client
    transaction_service: TransactionService,
    stats: MangoSimulationStats,
    tx_send_record: UnboundedSender<TransactionSendRecord>,
}

impl TpuManager {
    pub async fn new(
        transaction_service: TransactionService,
        stats: MangoSimulationStats,
        tx_send_record: UnboundedSender<TransactionSendRecord>,
    ) -> anyhow::Result<Self> {
        Ok(Self {
            transaction_service,
            stats,
            tx_send_record,
        })
    }

    pub async fn send_transaction(
        &self,
        transaction: &solana_sdk::transaction::Transaction,
        transaction_sent_record: TransactionSendRecord,
    ) -> bool {
        self.stats
            .inc_send(&transaction_sent_record.keeper_instruction);

        let tx_sent_record = self.tx_send_record.clone();
        let sent = tx_sent_record.send(transaction_sent_record);
        if sent.is_err() {
            warn!(
                "sending error on channel : {}",
                sent.err().unwrap().to_string()
            );
        }
        let transaction = bincode::serialize(transaction).unwrap();

        let res = self
            .transaction_service
            .send_transaction(transaction, None)
            .await;

        if let Err(e) = &res {
            print!("error sending txs on custom tpu {e:?}");
        }
        res.is_ok()
    }

    pub async fn send_transaction_batch(
        &self,
        batch: &Vec<(Transaction, TransactionSendRecord)>,
    ) -> bool {
        let mut value = true;
        for (tx, record) in batch {
            value &= self.send_transaction(tx, record.clone()).await;
        }
        value
    }
}
