use std::{
    cell::RefCell,
    collections::{BTreeMap, HashSet},
    convert::TryFrom,
    mem::size_of,
};

use arrayref::array_ref;
use async_channel::Sender;
use async_trait::async_trait;
use log::*;
use mango::{
    instruction::consume_events,
    queue::{AnyEvent, EventQueueHeader, EventType, FillEvent, OutEvent, Queue},
};
use mango_feeds_connector::solana_sdk::account::ReadableAccount;
use solana_sdk::{instruction::Instruction, pubkey::Pubkey};

use bytemuck::cast_ref;

use mango_feeds_connector::{account_write_filter::AccountWriteSink, chain_data::AccountData};

use crate::helpers::{to_sdk_instruction, to_sp_pk};

const MAX_BACKLOG: usize = 2;
const MAX_ACCS_PER_TX: usize = 24;
const MAX_EVENTS_PER_TX: usize = 50;

pub struct MangoV3PerpCrankSink {
    mkt_pks_by_evq_pks: BTreeMap<Pubkey, Pubkey>,
    group_pk: Pubkey,
    cache_pk: Pubkey,
    mango_v3_program: Pubkey,
    instruction_sender: Sender<(Pubkey, Vec<Instruction>)>,
}

impl MangoV3PerpCrankSink {
    pub fn new(
        pks: Vec<(Pubkey, Pubkey)>,
        group_pk: Pubkey,
        cache_pk: Pubkey,
        mango_v3_program: Pubkey,
        instruction_sender: Sender<(Pubkey, Vec<Instruction>)>,
    ) -> Self {
        Self {
            mkt_pks_by_evq_pks: pks
                .iter()
                .map(|(mkt_pk, evq_pk)| (*evq_pk, *mkt_pk))
                .collect(),
            group_pk,
            cache_pk,
            mango_v3_program,
            instruction_sender,
        }
    }
}

// couldn't compile the correct struct size / math on m1, fixed sizes resolve this issue
const EVENT_SIZE: usize = 200; //size_of::<AnyEvent>();
const QUEUE_LEN: usize = 256;
type EventQueueEvents = [AnyEvent; QUEUE_LEN];

#[async_trait]
impl AccountWriteSink for MangoV3PerpCrankSink {
    async fn process(
        &self,
        pk: &mango_feeds_connector::solana_sdk::pubkey::Pubkey,
        account: &AccountData,
    ) -> Result<(), String> {
        let account = &account.account;

        let (ix, mkt_pk): (Result<Instruction, String>, Pubkey) = {
            const HEADER_SIZE: usize = size_of::<EventQueueHeader>();
            let header_data = array_ref![account.data(), 0, HEADER_SIZE];
            let header = RefCell::<EventQueueHeader>::new(*bytemuck::from_bytes(header_data));
            let seq_num = header.clone().into_inner().seq_num;
            // trace!("evq {} seq_num {}", mkt.name, header.seq_num);

            const QUEUE_SIZE: usize = EVENT_SIZE * QUEUE_LEN;
            let events_data = array_ref![account.data(), HEADER_SIZE, QUEUE_SIZE];
            let events = RefCell::<EventQueueEvents>::new(*bytemuck::from_bytes(events_data));
            let event_queue = Queue {
                header: header.borrow_mut(),
                buf: events.borrow_mut(),
            };

            // only crank if at least 1 fill or a sufficient events of other categories are buffered
            let contains_fill_events = event_queue
                .iter()
                .any(|e| e.event_type == EventType::Fill as u8);
            let len = event_queue.iter().count();
            let has_backlog = len > MAX_BACKLOG;
            debug!("evq {pk:?} seq_num={seq_num} len={len} contains_fill_events={contains_fill_events} has_backlog={has_backlog}");

            if !contains_fill_events && !has_backlog {
                return Err("throttled".into());
            }

            trace!("evq {pk:?} seq_num={seq_num} len={len} contains_fill_events={contains_fill_events} has_backlog={has_backlog}");

            let mut mango_accounts = HashSet::new();
            event_queue.iter().take(MAX_EVENTS_PER_TX).for_each(|e| {
                if mango_accounts.len() < MAX_ACCS_PER_TX {
                    match EventType::try_from(e.event_type).expect("mango v4 event") {
                        EventType::Fill => {
                            let fill: &FillEvent = cast_ref(e);
                            mango_accounts.insert(fill.maker);
                            mango_accounts.insert(fill.taker);
                        }
                        EventType::Out => {
                            let out: &OutEvent = cast_ref(e);
                            mango_accounts.insert(out.owner);
                        }
                        EventType::Liquidate => {}
                    }
                }
            });

            let pk = solana_sdk::pubkey::Pubkey::new_from_array(pk.to_bytes());
            let mkt_pk = self
                .mkt_pks_by_evq_pks
                .get(&pk)
                .unwrap_or_else(|| panic!("{pk:?} is a known public key"));

            let ix = to_sdk_instruction(
                consume_events(
                    &to_sp_pk(&self.mango_v3_program),
                    &to_sp_pk(&self.group_pk),
                    &to_sp_pk(&self.cache_pk),
                    &to_sp_pk(mkt_pk),
                    &to_sp_pk(&pk),
                    &mut mango_accounts.iter().copied().collect::<Vec<_>>(),
                    MAX_EVENTS_PER_TX,
                )
                .unwrap(),
            );

            (Ok(ix), *mkt_pk)
        };

        // info!(
        //     "evq={pk:?} count={} limit=10",
        //     event_queue.iter().count()
        // );

        if let Err(e) = self.instruction_sender.send((mkt_pk, vec![ix?])).await {
            return Err(e.to_string());
        }

        Ok(())
    }
}
