extern crate serde;
extern crate serde_derive;
use solana_sdk::signature::Keypair;

#[derive(Clone, Debug, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct AccountKeys {
    pub public_key: String,
    pub secret_key: Vec<u8>,
    pub mango_account_pks: Vec<String>,
}

impl AccountKeys {
    pub fn to_keypair(&self) -> Keypair {
        Keypair::from_bytes(self.secret_key.as_slice()).unwrap()
    }
}

#[derive(Debug, serde::Deserialize)]
pub struct MangoConfig {
    pub groups: Vec<GroupConfig>,
}

#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct GroupConfig {
    pub name: String,
    pub public_key: String,
    pub mango_program_id: String,
    pub serum_program_id: String,
    pub oracles: Vec<OracleConfig>,
    pub tokens: Vec<TokenConfig>,
    pub perp_markets: Vec<MarketConfig>,
    pub spot_markets: Vec<MarketConfig>,
}

#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OracleConfig {
    pub symbol: String,
    pub public_key: String,
}

#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TokenConfig {
    pub symbol: String,
    pub mint_key: String,
    pub decimals: u64,
    pub root_key: String,
    pub node_keys: Vec<String>,
}

#[derive(Debug, serde::Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MarketConfig {
    pub name: String,
    pub public_key: String,
    pub base_symbol: String,
    pub base_decimals: u64,
    pub quote_decimals: u64,
    pub market_index: u64,
    pub bids_key: String,
    pub asks_key: String,
    pub events_key: String,
}
