use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, RwLock,
    },
    thread::{Builder, JoinHandle},
};

use solana_client::tpu_client::TpuClient;
use solana_quic_client::{QuicConfig, QuicConnectionManager, QuicPool};
use solana_sdk::{
    compute_budget::ComputeBudgetInstruction, hash::Hash, instruction::Instruction,
    message::Message, signature::Keypair, signer::Signer, transaction::Transaction,
};

use crate::{helpers::to_sdk_instruction, states::PerpMarketCache};

fn create_root_bank_update_instructions(perp_markets: &Vec<PerpMarketCache>) -> Vec<Instruction> {
    perp_markets
        .iter()
        .map(|perp_market| {
            let ix = mango::instruction::update_root_bank(
                &perp_market.mango_program_pk,
                &perp_market.mango_group_pk,
                &perp_market.mango_cache_pk,
                &perp_market.root_bank,
                perp_market.node_banks.as_slice(),
            )
            .unwrap();
            to_sdk_instruction(ix)
        })
        .collect()
}

fn create_update_price_cache_instructions(perp_markets: &Vec<PerpMarketCache>) -> Vec<Instruction> {
    perp_markets
        .iter()
        .map(|perp_market| {
            let ix = mango::instruction::cache_prices(
                &perp_market.mango_program_pk,
                &perp_market.mango_group_pk,
                &perp_market.mango_cache_pk,
                perp_market.node_banks.as_slice(),
            )
            .unwrap();
            to_sdk_instruction(ix)
        })
        .collect()
}

pub fn send_transaction(
    tpu_client: Arc<TpuClient<QuicPool, QuicConnectionManager, QuicConfig>>,
    ixs: &[Instruction],
    blockhash: Arc<RwLock<Hash>>,
    payer: &Keypair,
) {
    let mut tx = Transaction::new_unsigned(Message::new(ixs, Some(&payer.pubkey())));
    if let Ok(recent_blockhash) = blockhash.read() {
        tx.sign(&[payer], *recent_blockhash);
    }
    tpu_client.send_transaction(&tx);
}

pub fn start_root_bank_keeper(
    exit_signal: Arc<AtomicBool>,
    tpu_client: Arc<TpuClient<QuicPool, QuicConnectionManager, QuicConfig>>,
    perp_markets: Vec<PerpMarketCache>,
    blockhash: Arc<RwLock<Hash>>,
    authority: &Keypair,
) -> JoinHandle<()> {
    let authority = Keypair::from_bytes(&authority.to_bytes()).unwrap();
    Builder::new()
        .name("updating root bank keeper".to_string())
        .spawn(move || {
            let tpu_client = tpu_client.clone();
            let mut root_update_ixs = create_root_bank_update_instructions(&perp_markets);
            let mut cache_price = create_update_price_cache_instructions(&perp_markets);

            let blockhash = blockhash.clone();

            // add prioritization instruction
            let prioritization_ix = ComputeBudgetInstruction::set_compute_unit_price(100);
            root_update_ixs.insert(0, prioritization_ix.clone());
            cache_price.insert(0, prioritization_ix);
            loop {
                if exit_signal.load(Ordering::Relaxed) {
                    break;
                }
                send_transaction(
                    tpu_client.clone(),
                    root_update_ixs.as_slice(),
                    blockhash.clone(),
                    &authority,
                );
                send_transaction(
                    tpu_client.clone(),
                    cache_price.as_slice(),
                    blockhash.clone(),
                    &authority,
                );
            }
        })
        .unwrap()
}
