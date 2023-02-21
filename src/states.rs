use chrono::{DateTime, Utc};
use fixed::types::I80F48;
use mango::state::PerpMarket;
use serde::Serialize;
use solana_program::{pubkey::Pubkey, slot_history::Slot};
use solana_sdk::signature::Signature;

#[derive(Clone, Serialize)]
pub struct TransactionSendRecord {
    pub signature: Signature,
    pub sent_at: DateTime<Utc>,
    pub sent_slot: Slot,
    pub market_maker: Pubkey,
    pub market: Pubkey,
    pub priority_fees: u64,
}

#[derive(Clone, Serialize)]
pub struct TransactionConfirmRecord {
    pub signature: String,
    pub sent_slot: Slot,
    pub sent_at: String,
    pub confirmed_slot: Slot,
    pub confirmed_at: String,
    pub successful: bool,
    pub slot_leader: String,
    pub error: String,
    pub market_maker: String,
    pub market: String,
    pub block_hash: String,
    pub slot_processed: Slot,
    pub timed_out: bool,
    pub priority_fees: u64,
}

#[derive(Clone)]
pub struct PerpMarketCache {
    pub order_base_lots: i64,
    pub price: I80F48,
    pub price_quote_lots: i64,
    pub mango_program_pk: Pubkey,
    pub mango_group_pk: Pubkey,
    pub mango_cache_pk: Pubkey,
    pub perp_market_pk: Pubkey,
    pub perp_market: PerpMarket,
    pub root_bank: Pubkey,
    pub node_banks: Vec<Pubkey>,
}

pub struct _TransactionInfo {
    pub signature: Signature,
    pub transaction_send_time: DateTime<Utc>,
    pub send_slot: Slot,
    pub confirmation_retries: u32,
    pub error: String,
    pub confirmation_blockhash: Pubkey,
    pub leader_confirming_transaction: Pubkey,
    pub timeout: bool,
    pub market_maker: Pubkey,
    pub market: Pubkey,
}

#[derive(Clone, Serialize)]
pub struct BlockData {
    pub block_hash: String,
    pub block_slot: Slot,
    pub block_leader: String,
    pub total_transactions: u64,
    pub number_of_mm_transactions: u64,
    pub block_time: u64,
    pub cu_consumed: u64,
}
