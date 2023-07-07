use std::{
    collections::HashMap,
    sync::Mutex,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Instant,
};

use crate::states::{KeeperInstruction, TransactionConfirmRecord};
use iter_tools::Itertools;
use solana_metrics::datapoint_info;
use tokio::{sync::RwLock, task::JoinHandle};

// Non atomic version of counters
#[derive(Clone, Default, Debug)]
struct NACounters {
    num_confirmed_txs: u64,
    num_error_txs: u64,
    num_timeout_txs: u64,
    num_successful: u64,
    num_sent: u64,

    // sent transasctions
    num_market_makers_txs: u64,
    num_consume_events_txs: u64,
    num_cache_price_txs: u64,
    num_update_and_cache_quote_bank_txs: u64,
    num_update_root_banks_txs: u64,
    num_cache_root_banks_txs: u64,
    num_update_perp_cache_txs: u64,
    num_update_funding_txs: u64,

    // successful transasctions
    succ_market_makers_txs: u64,
    succ_consume_events_txs: u64,
    succ_cache_price_txs: u64,
    succ_update_and_cache_quote_bank_txs: u64,
    succ_update_root_banks_txs: u64,
    succ_cache_root_banks_txs: u64,
    succ_update_perp_cache_txs: u64,
    succ_update_funding_txs: u64,

    // errors section
    errors: HashMap<String, u64>,
}

impl NACounters {
    pub fn diff(&self, other: &NACounters) -> NACounters {
        let mut new_error_count = HashMap::new();
        for (error, count) in &self.errors {
            if let Some(v) = other.errors.get(error) {
                new_error_count.insert(error.clone(), *count - *v);
            } else {
                new_error_count.insert(error.clone(), *count);
            }
        }
        NACounters {
            num_confirmed_txs: self.num_confirmed_txs - other.num_confirmed_txs,
            num_error_txs: self.num_error_txs - other.num_error_txs,
            num_timeout_txs: self.num_timeout_txs - other.num_timeout_txs,
            num_successful: self.num_successful - other.num_successful,
            num_sent: self.num_sent - other.num_sent,
            num_market_makers_txs: self.num_market_makers_txs - other.num_market_makers_txs,
            num_consume_events_txs: self.num_consume_events_txs - other.num_consume_events_txs,
            num_cache_price_txs: self.num_cache_price_txs - other.num_cache_price_txs,
            num_update_and_cache_quote_bank_txs: self.num_update_and_cache_quote_bank_txs
                - other.num_update_and_cache_quote_bank_txs,
            num_update_root_banks_txs: self.num_update_root_banks_txs
                - other.num_update_root_banks_txs,
            num_cache_root_banks_txs: self.num_cache_root_banks_txs
                - other.num_cache_root_banks_txs,
            num_update_perp_cache_txs: self.num_update_perp_cache_txs
                - other.num_update_perp_cache_txs,
            num_update_funding_txs: self.num_update_funding_txs - other.num_update_funding_txs,
            succ_market_makers_txs: self.succ_market_makers_txs - other.succ_market_makers_txs,
            succ_consume_events_txs: self.succ_consume_events_txs - other.succ_consume_events_txs,
            succ_cache_price_txs: self.succ_cache_price_txs - other.succ_cache_price_txs,
            succ_update_and_cache_quote_bank_txs: self.succ_update_and_cache_quote_bank_txs
                - other.succ_update_and_cache_quote_bank_txs,
            succ_update_root_banks_txs: self.succ_update_root_banks_txs
                - other.succ_update_root_banks_txs,
            succ_cache_root_banks_txs: self.succ_cache_root_banks_txs
                - other.succ_cache_root_banks_txs,
            succ_update_perp_cache_txs: self.succ_update_perp_cache_txs
                - other.succ_update_perp_cache_txs,
            succ_update_funding_txs: self.succ_update_funding_txs - other.succ_update_funding_txs,
            errors: new_error_count,
        }
    }
}

#[derive(Default, Clone, Debug)]
struct Counters {
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

    // Errors
    errors: Arc<RwLock<HashMap<String, u64>>>,
}

impl Counters {
    pub async fn to_na_counters(&self) -> NACounters {
        NACounters {
            num_confirmed_txs: self.num_confirmed_txs.load(Ordering::Relaxed),
            num_error_txs: self.num_error_txs.load(Ordering::Relaxed),
            num_timeout_txs: self.num_timeout_txs.load(Ordering::Relaxed),
            num_successful: self.num_successful.load(Ordering::Relaxed),
            num_sent: self.num_sent.load(Ordering::Relaxed),

            // sent transasctions
            num_market_makers_txs: self.num_market_makers_txs.load(Ordering::Relaxed),
            num_consume_events_txs: self.num_consume_events_txs.load(Ordering::Relaxed),
            num_cache_price_txs: self.num_cache_price_txs.load(Ordering::Relaxed),
            num_update_and_cache_quote_bank_txs: self
                .num_update_and_cache_quote_bank_txs
                .load(Ordering::Relaxed),
            num_update_root_banks_txs: self.num_update_root_banks_txs.load(Ordering::Relaxed),
            num_cache_root_banks_txs: self.num_cache_root_banks_txs.load(Ordering::Relaxed),
            num_update_perp_cache_txs: self.num_update_perp_cache_txs.load(Ordering::Relaxed),
            num_update_funding_txs: self.num_update_funding_txs.load(Ordering::Relaxed),

            // successful transasctions
            succ_market_makers_txs: self.succ_market_makers_txs.load(Ordering::Relaxed),
            succ_consume_events_txs: self.succ_consume_events_txs.load(Ordering::Relaxed),
            succ_cache_price_txs: self.succ_cache_price_txs.load(Ordering::Relaxed),
            succ_update_and_cache_quote_bank_txs: self
                .succ_update_and_cache_quote_bank_txs
                .load(Ordering::Relaxed),
            succ_update_root_banks_txs: self.succ_update_root_banks_txs.load(Ordering::Relaxed),
            succ_cache_root_banks_txs: self.succ_cache_root_banks_txs.load(Ordering::Relaxed),
            succ_update_perp_cache_txs: self.succ_update_perp_cache_txs.load(Ordering::Relaxed),
            succ_update_funding_txs: self.succ_update_funding_txs.load(Ordering::Relaxed),
            errors: self.errors.read().await.clone(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct MangoSimulationStats {
    recv_limit: usize,
    counters: Counters,
    previous_counters: Arc<Mutex<NACounters>>,
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
            previous_counters: Arc::new(Mutex::new(NACounters::default())),
        }
    }

    pub fn update_from_tx_status_stream(
        &self,
        tx_confirm_record_reciever: tokio::sync::broadcast::Receiver<TransactionConfirmRecord>,
    ) -> JoinHandle<()> {
        let counters = self.counters.clone();
        let regex = regex::Regex::new(r"Error processing Instruction \d+: ").unwrap();
        tokio::spawn(async move {
            let mut tx_confirm_record_reciever = tx_confirm_record_reciever;
            while let Ok(tx_data) = tx_confirm_record_reciever.recv().await {
                if tx_data.confirmed_at.is_some() {
                    counters.num_confirmed_txs.fetch_add(1, Ordering::Relaxed);
                    if let Some(error) = tx_data.error {
                        let error = regex.replace_all(&error, "").to_string();
                        counters.num_error_txs.fetch_add(1, Ordering::Relaxed);
                        let mut lock = counters.errors.write().await;
                        if let Some(value) = lock.get_mut(&error) {
                            *value += 1;
                        } else {
                            lock.insert(error, 1);
                        }
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

    pub async fn report(&mut self, is_final: bool, name: &'static str) {
        let time_diff = std::time::Instant::now() - self.instant;
        let counters = self.counters.to_na_counters().await;
        let diff = {
            let mut prev_counter_lock = self.previous_counters.lock().unwrap();
            let diff = counters.diff(&prev_counter_lock);
            *prev_counter_lock = counters.clone();
            diff
        };

        println!("\n\n{} at {} secs", name, time_diff.as_secs());
        if !is_final {
            println!("Recently sent transactions could not yet be confirmed and would be confirmed shortly.\n
            diff is wrt previous report");
        }
        println!(
            "Number of expected marker making transactions : {}",
            self.recv_limit
        );
        println!(
            "Number of transactions Sent:({}) (including keeper) (Diff:({}))",
            counters.num_sent, diff.num_sent,
        );

        println!(
            "Market Maker transactions : Sent({}), Successful({}) (Diff : Sent({}), Successful({}))",
            counters.num_market_makers_txs,
            counters.succ_market_makers_txs,
            diff.num_market_makers_txs,
            diff.succ_market_makers_txs,
        );
        println!(
            "Keeper Cosume Events : Sent({}), Successful({}) (Diff : Sent({}), Successful({}))",
            counters.num_consume_events_txs,
            counters.succ_consume_events_txs,
            diff.num_consume_events_txs,
            diff.succ_consume_events_txs
        );
        println!(
            "Keeper Cache Price : Sent({}), Successful({}) (Diff : Sent({}), Successful({}))",
            counters.num_cache_price_txs,
            counters.succ_cache_price_txs,
            diff.num_cache_price_txs,
            diff.succ_cache_price_txs
        );
        println!(
            "Keeper Update and Cache Quote Bank : Sent({}), Successful({}) (Diff : Sent({}), Successful({}))",
            counters.num_update_and_cache_quote_bank_txs, counters.succ_update_and_cache_quote_bank_txs, diff.num_update_and_cache_quote_bank_txs, diff.succ_update_and_cache_quote_bank_txs
        );
        println!(
            "Keeper Update Root Banks : Sent({}), Successful({}) (Diff : Sent({}), Successful({}))",
            counters.num_update_root_banks_txs,
            counters.succ_update_root_banks_txs,
            diff.num_update_root_banks_txs,
            diff.succ_update_root_banks_txs
        );
        println!(
            "Keeper Cache Root Banks : Sent({}), Successful({}) (Diff : Sent({}), Successful({}))",
            counters.num_cache_root_banks_txs,
            counters.succ_cache_root_banks_txs,
            diff.num_cache_root_banks_txs,
            diff.succ_cache_root_banks_txs
        );
        println!(
            "Keeper Update Perp Cache : Sent({}), Successful({}) (Diff : Sent({}), Successful({}))",
            counters.num_update_perp_cache_txs,
            counters.succ_update_perp_cache_txs,
            diff.num_update_perp_cache_txs,
            diff.succ_update_perp_cache_txs
        );
        println!(
            "Keeper Update Funding : Sent({}), Successful({}) (Diff : Sent({}), Successful({}))",
            counters.num_update_funding_txs,
            counters.succ_update_funding_txs,
            diff.num_update_funding_txs,
            diff.succ_update_funding_txs
        );

        println!(
            "Transactions confirmed : {}%",
            (counters.num_confirmed_txs * 100)
                .checked_div(counters.num_sent)
                .unwrap_or(0)
        );
        println!(
            "Transactions successful : {}%",
            (counters.num_successful * 100)
                .checked_div(counters.num_sent)
                .unwrap_or(0)
        );
        println!(
            "Transactions timed out : {}%",
            (counters.num_timeout_txs * 100)
                .checked_div(counters.num_sent)
                .unwrap_or(0)
        );
        let top_5_errors = counters
            .errors
            .iter()
            .sorted_by(|x, y| (*y.1).cmp(x.1))
            .take(5)
            .enumerate()
            .collect_vec();
        let mut errors_to_print: String = String::new();
        for (idx, (error, count)) in top_5_errors {
            println!("Error #{idx} : {error} ({count})");
            errors_to_print += format!("{error}({count}),").as_str();
        }
        println!("\n");

        if !is_final {
            datapoint_info!(
                name,
                ("num_txs_sent", diff.num_sent, i64),
                ("num_confirmed_txs", diff.num_confirmed_txs, i64),
                ("num_successful_txs", diff.num_successful, i64),
                ("num_error_txs", diff.num_error_txs, i64),
                ("num_timeout_txs", diff.num_timeout_txs, i64),
                (
                    "percent_confirmed_txs",
                    (diff.num_confirmed_txs * 100)
                        .checked_div(diff.num_sent)
                        .unwrap_or(0),
                    f64
                ),
                (
                    "percent_successful_txs",
                    (diff.num_confirmed_txs * 100)
                        .checked_div(diff.num_sent)
                        .unwrap_or(0),
                    f64
                ),
                (
                    "percent_error_txs",
                    (diff.num_error_txs * 100)
                        .checked_div(diff.num_sent)
                        .unwrap_or(0),
                    f64
                ),
                (
                    "percent_timeout_txs",
                    (diff.num_timeout_txs * 100)
                        .checked_div(diff.num_sent)
                        .unwrap_or(0),
                    f64
                ),
                (
                    "keeper_consume_events_sent",
                    diff.num_consume_events_txs,
                    i64
                ),
                (
                    "keeper_consume_events_success",
                    diff.succ_consume_events_txs,
                    i64
                ),
                ("keeper_cache_price_sent", diff.num_cache_price_txs, i64),
                ("keeper_cache_price_success", diff.succ_cache_price_txs, i64),
                (
                    "keeper_update_and_cache_qb_sent",
                    diff.num_update_and_cache_quote_bank_txs,
                    i64
                ),
                (
                    "keeper_update_and_cache_qb_succ",
                    diff.succ_update_and_cache_quote_bank_txs,
                    i64
                ),
                (
                    "keeper_update_root_banks_sent",
                    diff.num_update_root_banks_txs,
                    i64
                ),
                (
                    "keeper_update_root_banks_succ",
                    diff.succ_update_root_banks_txs,
                    i64
                ),
                (
                    "keeper_cache_root_banks_sent",
                    diff.num_cache_root_banks_txs,
                    i64
                ),
                (
                    "keeper_cache_root_banks_succ",
                    diff.succ_cache_root_banks_txs,
                    i64
                ),
                (
                    "keeper_update_perp_cache_sent",
                    diff.num_update_perp_cache_txs,
                    i64
                ),
                (
                    "keeper_update_perp_cache_succ",
                    diff.succ_update_perp_cache_txs,
                    i64
                ),
                (
                    "keeper_update_funding_sent",
                    diff.num_update_funding_txs,
                    i64
                ),
                (
                    "keeper_update_funding_succ",
                    diff.succ_update_funding_txs,
                    i64
                ),
                ("top_5_errors", errors_to_print, String)
            );
        }
    }
}
