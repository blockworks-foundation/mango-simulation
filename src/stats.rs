use std::{
    sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc,
    },
    time::Instant,
};

use crate::states::{KeeperInstruction, TransactionConfirmRecord};
use solana_metrics::datapoint_info;
use tokio::task::JoinHandle;

#[derive(Default, Clone, Debug)]
pub struct Counters {
    num_confirmed_txs: Arc<AtomicU64>,
    num_error_txs: Arc<AtomicU64>,
    num_timeout_txs: Arc<AtomicU64>,
    num_successful: Arc<AtomicU64>,
    num_sent: Arc<AtomicU64>,

    // sent transasctions
    num_market_makers_txs: Arc<AtomicU64>,
    num_consume_events_txs: Arc<AtomicU64>,
    num_cache_price_txs: Arc<AtomicU64>,
    num_update_and_cache_quote_bank_txs: Arc<AtomicU64>,
    num_update_root_banks_txs: Arc<AtomicU64>,
    num_cache_root_banks_txs: Arc<AtomicU64>,
    num_update_perp_cache_txs: Arc<AtomicU64>,
    num_update_funding_txs: Arc<AtomicU64>,

    // successful transasctions
    succ_market_makers_txs: Arc<AtomicU64>,
    succ_consume_events_txs: Arc<AtomicU64>,
    succ_cache_price_txs: Arc<AtomicU64>,
    succ_update_and_cache_quote_bank_txs: Arc<AtomicU64>,
    succ_update_root_banks_txs: Arc<AtomicU64>,
    succ_cache_root_banks_txs: Arc<AtomicU64>,
    succ_update_perp_cache_txs: Arc<AtomicU64>,
    succ_update_funding_txs: Arc<AtomicU64>,
}

#[derive(Debug, Clone)]
pub struct MangoSimulationStats {
    recv_limit: usize,
    counters: Counters,
    instant: Instant,
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
            counters: Counters::default(),
            instant: Instant::now(),
        }
    }

    pub fn update_from_tx_status_stream(
        &self,
        tx_confirm_record_reciever: tokio::sync::broadcast::Receiver<TransactionConfirmRecord>,
        do_exit: Arc<AtomicBool>,
    ) -> JoinHandle<()> {
        let counters = self.counters.clone();
        tokio::spawn(async move {
            let mut tx_confirm_record_reciever = tx_confirm_record_reciever;
            loop {
                if do_exit.load(Ordering::Relaxed) {
                    break;
                }

                if let Ok(tx_data) = tx_confirm_record_reciever.recv().await {
                    if let Some(_) = tx_data.confirmed_at {
                        counters.num_confirmed_txs.fetch_add(1, Ordering::Relaxed);
                        if let Some(_) = tx_data.error {
                            counters.num_error_txs.fetch_add(1, Ordering::Relaxed);
                        } else {
                            counters.num_successful.fetch_add(1, Ordering::Relaxed);

                            if let Some(keeper_instruction) = tx_data.keeper_instruction {
                                match keeper_instruction {
                                    KeeperInstruction::CachePrice => counters
                                        .succ_cache_price_txs
                                        .fetch_add(1, Ordering::Relaxed),
                                    KeeperInstruction::CacheRootBanks => counters
                                        .succ_cache_root_banks_txs
                                        .fetch_add(1, Ordering::Relaxed),
                                    KeeperInstruction::ConsumeEvents => counters
                                        .succ_consume_events_txs
                                        .fetch_add(1, Ordering::Relaxed),
                                    KeeperInstruction::UpdateAndCacheQuoteRootBank => counters
                                        .succ_update_and_cache_quote_bank_txs
                                        .fetch_add(1, Ordering::Relaxed),
                                    KeeperInstruction::UpdateFunding => counters
                                        .succ_update_funding_txs
                                        .fetch_add(1, Ordering::Relaxed),
                                    KeeperInstruction::UpdatePerpCache => counters
                                        .succ_update_perp_cache_txs
                                        .fetch_add(1, Ordering::Relaxed),
                                    KeeperInstruction::UpdateRootBanks => counters
                                        .succ_update_root_banks_txs
                                        .fetch_add(1, Ordering::Relaxed),
                                };
                            } else {
                                counters
                                    .succ_market_makers_txs
                                    .fetch_add(1, Ordering::Relaxed);
                            }
                        }
                    } else {
                        counters.num_timeout_txs.fetch_add(1, Ordering::Relaxed);
                    }
                } else {
                    break;
                }
            }
        })
    }

    pub fn inc_send(&self, keeper_instruction: &Option<KeeperInstruction>) {
        self.counters.num_sent.fetch_add(1, Ordering::Relaxed);

        if let Some(keeper_instruction) = keeper_instruction {
            match keeper_instruction {
                KeeperInstruction::CachePrice => self
                    .counters
                    .num_cache_price_txs
                    .fetch_add(1, Ordering::Relaxed),
                KeeperInstruction::CacheRootBanks => self
                    .counters
                    .num_cache_root_banks_txs
                    .fetch_add(1, Ordering::Relaxed),
                KeeperInstruction::ConsumeEvents => self
                    .counters
                    .num_consume_events_txs
                    .fetch_add(1, Ordering::Relaxed),
                KeeperInstruction::UpdateAndCacheQuoteRootBank => self
                    .counters
                    .num_update_and_cache_quote_bank_txs
                    .fetch_add(1, Ordering::Relaxed),
                KeeperInstruction::UpdateFunding => self
                    .counters
                    .num_update_funding_txs
                    .fetch_add(1, Ordering::Relaxed),
                KeeperInstruction::UpdatePerpCache => self
                    .counters
                    .num_update_perp_cache_txs
                    .fetch_add(1, Ordering::Relaxed),
                KeeperInstruction::UpdateRootBanks => self
                    .counters
                    .num_update_root_banks_txs
                    .fetch_add(1, Ordering::Relaxed),
            };
        } else {
            self.counters
                .num_market_makers_txs
                .fetch_add(1, Ordering::Relaxed);
        }
    }

    pub fn report(&mut self, is_final: bool, name: &'static str) {
        let num_sent = self.counters.num_sent.load(Ordering::Relaxed);
        let num_confirmed_txs = self.counters.num_confirmed_txs.load(Ordering::Relaxed);
        let num_successful = self.counters.num_successful.load(Ordering::Relaxed);
        let num_error_txs = self.counters.num_error_txs.load(Ordering::Relaxed);
        let num_timeout_txs = self.counters.num_timeout_txs.load(Ordering::Relaxed);
        let time_diff = std::time::Instant::now() - self.instant;

        let num_market_makers_txs = self.counters.num_market_makers_txs.load(Ordering::Relaxed);
        let num_consume_events_txs = self.counters.num_consume_events_txs.load(Ordering::Relaxed);
        let num_cache_price_txs = self.counters.num_cache_price_txs.load(Ordering::Relaxed);
        let num_update_and_cache_quote_bank_txs = self
            .counters
            .num_update_and_cache_quote_bank_txs
            .load(Ordering::Relaxed);
        let num_update_root_banks_txs = self
            .counters
            .num_update_root_banks_txs
            .load(Ordering::Relaxed);
        let num_cache_root_banks_txs = self
            .counters
            .num_cache_root_banks_txs
            .load(Ordering::Relaxed);
        let num_update_perp_cache_txs = self
            .counters
            .num_update_perp_cache_txs
            .load(Ordering::Relaxed);
        let num_update_funding_txs = self.counters.num_update_funding_txs.load(Ordering::Relaxed);

        let succ_market_makers_txs = self.counters.succ_market_makers_txs.load(Ordering::Relaxed);
        let succ_consume_events_txs = self
            .counters
            .succ_consume_events_txs
            .load(Ordering::Relaxed);
        let succ_cache_price_txs = self.counters.succ_cache_price_txs.load(Ordering::Relaxed);
        let succ_update_and_cache_quote_bank_txs = self
            .counters
            .succ_update_and_cache_quote_bank_txs
            .load(Ordering::Relaxed);
        let succ_update_root_banks_txs = self
            .counters
            .succ_update_root_banks_txs
            .load(Ordering::Relaxed);
        let succ_cache_root_banks_txs = self
            .counters
            .succ_cache_root_banks_txs
            .load(Ordering::Relaxed);
        let succ_update_perp_cache_txs = self
            .counters
            .succ_update_perp_cache_txs
            .load(Ordering::Relaxed);
        let succ_update_funding_txs = self
            .counters
            .succ_update_funding_txs
            .load(Ordering::Relaxed);

        println!("\n\n{} at {} secs", name, time_diff.as_secs());
        if !is_final {
            println!("Recently sent transactions could not yet be confirmed and would be confirmed shortly.\n");
        }
        println!(
            "Number of expected marker making transactions : {}",
            self.recv_limit
        );
        println!(
            "Number of transactions sent : {} (including keeper)",
            num_sent
        );

        println!(
            "Market Maker transactions : Sent {}, Successful {}",
            num_market_makers_txs, succ_market_makers_txs
        );
        println!(
            "Keeper Cosume Events : Sent {}, Successful {}",
            num_consume_events_txs, succ_consume_events_txs
        );
        println!(
            "Keeper Cache Price : Sent {}, Successful {}",
            num_cache_price_txs, succ_cache_price_txs
        );
        println!(
            "Keeper Update and Cache Quote Bank : Sent {}, Successful {}",
            num_update_and_cache_quote_bank_txs, succ_update_and_cache_quote_bank_txs
        );
        println!(
            "Keeper Update Root Banks : Sent {}, Successful {}",
            num_update_root_banks_txs, succ_update_root_banks_txs
        );
        println!(
            "Keeper Cache Root Banks : Sent {}, Successful {}",
            num_cache_root_banks_txs, succ_cache_root_banks_txs
        );
        println!(
            "Keeper Update Perp Cache : Sent {}, Successful {}",
            num_update_perp_cache_txs, succ_update_perp_cache_txs
        );
        println!(
            "Keeper Update Funding : Sent {}, Successful {}",
            num_update_funding_txs, succ_update_funding_txs
        );

        println!(
            "Transactions confirmed {}%",
            (num_confirmed_txs * 100).checked_div(num_sent).unwrap_or(0)
        );
        println!(
            "Transactions successful {}%",
            (num_successful * 100).checked_div(num_sent).unwrap_or(0)
        );
        println!(
            "Transactions timed out {}%",
            (num_timeout_txs * 100).checked_div(num_sent).unwrap_or(0)
        );
        println!("\n\n");

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
                (num_confirmed_txs * 100).checked_div(num_sent).unwrap_or(0),
                f64
            ),
            (
                "percent_successful_txs",
                (num_confirmed_txs * 100).checked_div(num_sent).unwrap_or(0),
                f64
            ),
            (
                "percent_error_txs",
                (num_error_txs * 100).checked_div(num_sent).unwrap_or(0),
                f64
            ),
            (
                "percent_timeout_txs",
                (num_timeout_txs * 100).checked_div(num_sent).unwrap_or(0),
                f64
            ),
            ("keeper_consume_events_sent", num_consume_events_txs, i64),
            ("keeper_consume_events_success", succ_consume_events_txs, i64),
            ("keeper_cache_price_sent", num_cache_price_txs, i64),
            ("keeper_cache_price_success", succ_cache_price_txs, i64),
            ("keeper_update_and_cache_qb_sent", num_update_and_cache_quote_bank_txs, i64),
            ("keeper_update_and_cache_qb_succ", succ_update_and_cache_quote_bank_txs, i64),
            ("keeper_update_root_banks_sent", num_update_root_banks_txs, i64),
            ("keeper_update_root_banks_succ", succ_update_root_banks_txs, i64),
            ("keeper_cache_root_banks_sent", num_cache_root_banks_txs, i64),
            ("keeper_cache_root_banks_succ", succ_cache_root_banks_txs, i64),
            ("keeper_update_perp_cache_sent", num_update_perp_cache_txs, i64),
            ("keeper_update_perp_cache_succ", succ_update_perp_cache_txs, i64),
            ("keeper_update_funding_sent", num_update_funding_txs, i64),
            ("keeper_update_funding_succ", succ_update_funding_txs, i64),
        );
        self.counters = Counters::default();
    }
}
