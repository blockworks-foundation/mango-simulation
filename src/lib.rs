pub mod account_write_filter;
pub mod blockhash_poller;
pub mod cli;
pub mod chain_data;
pub mod confirmation_strategies;
pub mod crank;
pub mod grpc_plugin_source;
pub mod helpers;
pub mod mango;
pub mod mango_v3_perp_crank_sink;
pub mod market_markers;
pub mod metrics;
pub mod rotating_queue;
pub mod states;
pub mod transaction_sender;

trait AnyhowWrap {
  type Value;
  fn map_err_anyhow(self) -> anyhow::Result<Self::Value>;
}

impl<T, E: std::fmt::Debug> AnyhowWrap for Result<T, E> {
  type Value = T;
  fn map_err_anyhow(self) -> anyhow::Result<Self::Value> {
      self.map_err(|err| anyhow::anyhow!("{:?}", err))
  }
}