use log::warn;
use solana_sdk::compute_budget::ComputeBudgetInstruction;
use tokio::spawn;

use {
    crate::{
        helpers::to_sdk_instruction,
        noop,
        states::{KeeperInstruction, PerpMarketCache, TransactionSendRecord},
        tpu_manager::TpuManager,
    },
    chrono::Utc,
    iter_tools::Itertools,
    solana_program::pubkey::Pubkey,
    solana_sdk::{
        hash::Hash, instruction::Instruction, message::Message, signature::Keypair, signer::Signer,
        transaction::Transaction,
    },
    std::sync::{
        atomic::{AtomicBool, AtomicU64, Ordering},
        Arc,
    },
    tokio::{sync::RwLock, task::JoinHandle},
};

fn create_root_bank_update_instructions(perp_markets: &[PerpMarketCache]) -> Vec<Instruction> {
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

fn create_update_fundings_instructions(perp_markets: &[PerpMarketCache]) -> Vec<Instruction> {
    perp_markets
        .iter()
        .map(|perp_market| {
            let ix = mango::instruction::update_funding(
                &perp_market.mango_program_pk,
                &perp_market.mango_group_pk,
                &perp_market.mango_cache_pk,
                &perp_market.perp_market_pk,
                &perp_market.bids,
                &perp_market.asks,
            )
            .unwrap();
            to_sdk_instruction(ix)
        })
        .collect()
}

fn create_cache_root_bank_instruction(perp_markets: &[PerpMarketCache]) -> Instruction {
    let mango_program_pk = perp_markets[0].mango_program_pk;
    let mango_group_pk = perp_markets[0].mango_group_pk;
    let mango_cache_pk = perp_markets[0].mango_cache_pk;
    let root_banks = perp_markets.iter().map(|x| x.root_bank).collect_vec();

    let ix = mango::instruction::cache_root_banks(
        &mango_program_pk,
        &mango_group_pk,
        &mango_cache_pk,
        root_banks.as_slice(),
    )
    .unwrap();
    to_sdk_instruction(ix)
}

fn create_update_price_cache_instructions(perp_markets: &[PerpMarketCache]) -> Instruction {
    let mango_program_pk = perp_markets[0].mango_program_pk;
    let mango_group_pk = perp_markets[0].mango_group_pk;
    let mango_cache_pk = perp_markets[0].mango_cache_pk;
    let price_oracles = perp_markets.iter().map(|x| x.price_oracle).collect_vec();

    let ix = mango::instruction::cache_prices(
        &mango_program_pk,
        &mango_group_pk,
        &mango_cache_pk,
        price_oracles.as_slice(),
    )
    .unwrap();
    to_sdk_instruction(ix)
}

fn create_cache_perp_markets_instructions(perp_markets: &[PerpMarketCache]) -> Instruction {
    let mango_program_pk = perp_markets[0].mango_program_pk;
    let mango_group_pk = perp_markets[0].mango_group_pk;
    let mango_cache_pk = perp_markets[0].mango_cache_pk;
    let perp_market_pks = perp_markets.iter().map(|x| x.perp_market_pk).collect_vec();
    let ix = mango::instruction::cache_perp_markets(
        &mango_program_pk,
        &mango_group_pk,
        &mango_cache_pk,
        perp_market_pks.as_slice(),
    )
    .unwrap();
    to_sdk_instruction(ix)
}

pub fn prepare_transaction(
    mut ixs: Vec<Instruction>,
    recent_blockhash: &Hash,
    current_slot: Arc<AtomicU64>,
    payer: &Keypair,
    prioritization_fee: u64,
    keeper_instruction: KeeperInstruction,
) -> (Transaction, TransactionSendRecord) {
    // add a noop with a current timestamp to ensure unique txs
    ixs.push(noop::timestamp());
    // add priority fees
    ixs.push(ComputeBudgetInstruction::set_compute_unit_price(
        prioritization_fee,
    ));
    let mut tx = Transaction::new_unsigned(Message::new(&ixs, Some(&payer.pubkey())));
    tx.sign(&[payer], *recent_blockhash);

    let tx_send_record = TransactionSendRecord {
        signature: tx.signatures[0],
        sent_at: Utc::now(),
        sent_slot: current_slot.load(Ordering::Acquire),
        market_maker: None,
        market: None,
        priority_fees: prioritization_fee,
        keeper_instruction: Some(keeper_instruction),
    };
    (tx, tx_send_record)
}

pub fn create_update_and_cache_quote_banks(
    perp_markets: &[PerpMarketCache],
    quote_root_bank: Pubkey,
    quote_node_banks: Vec<Pubkey>,
) -> Vec<Instruction> {
    let mango_program_pk = perp_markets[0].mango_program_pk;
    let mango_group_pk = perp_markets[0].mango_group_pk;
    let mango_cache_pk = perp_markets[0].mango_cache_pk;

    let ix_update = mango::instruction::update_root_bank(
        &mango_program_pk,
        &mango_group_pk,
        &mango_cache_pk,
        &quote_root_bank,
        quote_node_banks.as_slice(),
    )
    .unwrap();
    let ix_cache = mango::instruction::cache_root_banks(
        &mango_program_pk,
        &mango_group_pk,
        &mango_cache_pk,
        &[quote_root_bank],
    )
    .unwrap();
    vec![to_sdk_instruction(ix_update), to_sdk_instruction(ix_cache)]
}

#[allow(clippy::too_many_arguments)]
pub fn start_keepers(
    exit_signal: Arc<AtomicBool>,
    tpu_manager: TpuManager,
    perp_markets: Vec<PerpMarketCache>,
    blockhash: Arc<RwLock<Hash>>,
    current_slot: Arc<AtomicU64>,
    authority: &Keypair,
    quote_root_bank: Pubkey,
    quote_node_banks: Vec<Pubkey>,
    prioritization_fee: u64,
) -> JoinHandle<()> {
    let authority = Keypair::from_bytes(&authority.to_bytes()).unwrap();
    tokio::spawn(async move {
        let current_slot = current_slot.clone();

        let root_update_ixs = create_root_bank_update_instructions(&perp_markets);
        let cache_prices = vec![create_update_price_cache_instructions(&perp_markets)];
        let update_perp_cache = vec![create_cache_perp_markets_instructions(&perp_markets)];
        let cache_root_bank_ix = vec![create_cache_root_bank_instruction(&perp_markets)];
        let update_funding_ix = create_update_fundings_instructions(&perp_markets);
        let quote_root_bank_ix =
            create_update_and_cache_quote_banks(&perp_markets, quote_root_bank, quote_node_banks);

        while !exit_signal.load(Ordering::Relaxed) {
            let recent_blockhash = blockhash.read().await.to_owned();

            let mut tx_batch = vec![];
            tx_batch.push(prepare_transaction(
                cache_prices.clone(),
                &recent_blockhash,
                current_slot.clone(),
                &authority,
                prioritization_fee,
                KeeperInstruction::CachePrice,
            ));

            tx_batch.push(prepare_transaction(
                quote_root_bank_ix.clone(),
                &recent_blockhash,
                current_slot.clone(),
                &authority,
                prioritization_fee,
                KeeperInstruction::UpdateAndCacheQuoteRootBank,
            ));

            for updates in update_funding_ix.chunks(3) {
                tx_batch.push(prepare_transaction(
                    updates.to_vec(),
                    &recent_blockhash,
                    current_slot.clone(),
                    &authority,
                    prioritization_fee,
                    KeeperInstruction::UpdateFunding,
                ));
            }
            tx_batch.push(prepare_transaction(
                root_update_ixs.clone(),
                &recent_blockhash,
                current_slot.clone(),
                &authority,
                prioritization_fee,
                KeeperInstruction::UpdateRootBanks,
            ));

            tx_batch.push(prepare_transaction(
                update_perp_cache.clone(),
                &recent_blockhash,
                current_slot.clone(),
                &authority,
                prioritization_fee,
                KeeperInstruction::UpdatePerpCache,
            ));

            tx_batch.push(prepare_transaction(
                cache_root_bank_ix.clone(),
                &recent_blockhash,
                current_slot.clone(),
                &authority,
                prioritization_fee,
                KeeperInstruction::CacheRootBanks,
            ));

            let start_slot = current_slot.load(Ordering::Relaxed);
            let start_time = Utc::now();
            let tpu_manager = tpu_manager.clone();
            spawn(async move {
                if !tpu_manager.send_transaction_batch(&tx_batch).await {
                    warn!("issue when sending batch started slot={start_slot} time={start_time} hash={recent_blockhash:?}");
                }
            });

            std::thread::sleep(std::time::Duration::from_secs(1));
        }
    })
}
