use std::sync::{
    atomic::{AtomicBool, AtomicU64, Ordering},
    Arc,
};

use crate::states::TransactionConfirmRecord;
use solana_metrics::datapoint_info;
use tokio::task::JoinHandle;

#[derive(Default)]
pub struct MangoSimulationStats {
    recv_limit: usize,
    num_confirmed_txs: Arc<AtomicU64>,
    num_error_txs: Arc<AtomicU64>,
    num_timeout_txs: Arc<AtomicU64>,
    num_successful: Arc<AtomicU64>,
    num_sent: Arc<AtomicU64>,
}

impl MangoSimulationStats {
    pub fn new(
        nb_market_makers: usize,
        quotes_per_second: usize,
        nb_markets_per_mm: usize,
        duration_in_sec: usize,
    ) -> Self {
        Self {
            recv_limit: nb_market_makers * quotes_per_second * nb_markets_per_mm * duration_in_sec,
            num_confirmed_txs: Arc::new(AtomicU64::new(0)),
            num_error_txs: Arc::new(AtomicU64::new(0)),
            num_timeout_txs: Arc::new(AtomicU64::new(0)),
            num_successful: Arc::new(AtomicU64::new(0)),
            num_sent: Arc::new(AtomicU64::new(0)),
        }
    }

    pub fn update_from_tx_status_stream(
        &self,
        tx_confirm_record_reciever: async_channel::Receiver<TransactionConfirmRecord>,
        do_exit: Arc<AtomicBool>,
    ) -> JoinHandle<()> {
        let num_confirmed_txs = self.num_confirmed_txs.clone();
        let num_error_txs = self.num_error_txs.clone();
        let num_successful = self.num_successful.clone();
        let num_timeout_txs = self.num_timeout_txs.clone();
        tokio::spawn(async move {
            loop {
                if do_exit.load(Ordering::Relaxed) {
                    break;
                }

                if let Ok(tx_data) = tx_confirm_record_reciever.recv().await {
                    if let Some(_) = tx_data.confirmed_at {
                        num_confirmed_txs.fetch_add(1, Ordering::Relaxed);
                        if let Some(_) = tx_data.error {
                            num_error_txs.fetch_add(1, Ordering::Relaxed);
                        } else {
                            num_successful.fetch_add(1, Ordering::Relaxed);
                        }
                    } else {
                        num_timeout_txs.fetch_add(1, Ordering::Relaxed);
                    }
                } else {
                    break;
                }
            }
        })
    }

    pub fn report(&self, name: &'static str) {
        let num_sent = self.num_sent.load(Ordering::Relaxed);
        let num_confirmed_txs = self.num_confirmed_txs.load(Ordering::Relaxed);
        let num_successful = self.num_successful.load(Ordering::Relaxed);
        let num_error_txs = self.num_error_txs.load(Ordering::Relaxed);
        let num_timeout_txs = self.num_timeout_txs.load(Ordering::Relaxed);

        datapoint_info!(
            name,
            ("recv_limit", self.recv_limit, i64),
            ("num_txs_sent", num_sent, i64),
            ("num_confirmed_txs", num_confirmed_txs, i64),
            ("num_successful_txs", num_successful, i64),
            ("num_error_txs", num_error_txs, i64),
            ("num_timeout_txs", num_timeout_txs, i64),
            (
                "percent_confirmed_txs",
                (num_confirmed_txs * 100) / num_sent,
                f64
            ),
            (
                "percent_successful_txs",
                (num_confirmed_txs * 100) / num_sent,
                f64
            ),
            ("percent_error_txs", (num_error_txs * 100) / num_sent, f64),
            (
                "percent_timeout_txs",
                (num_timeout_txs * 100) / num_sent,
                f64
            ),
        );
    }
}
