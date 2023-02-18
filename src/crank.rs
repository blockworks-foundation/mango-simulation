use std::{thread::{JoinHandle, Builder}, sync::{Arc, RwLock}, str::FromStr, time::Duration};

// use solana_client::rpc_client::RpcClient;
use solana_sdk::{pubkey::Pubkey, signature::Keypair, instruction::Instruction};

use crate::{mango::GroupConfig, account_write_filter::AccountWriteRoute, mango_v3_perp_crank_sink::MangoV3PerpCrankSink};




// pub async fn send_tx_loop(
//   ixs_rx: async_channel::Receiver<Vec<Instruction>>,
//   blockhash: Arc<RwLock<Hash>>,
//   client: Arc<RpcClient>,
//   keypair: Keypair,
// ) {
//   info!("signing with keypair pk={:?}", keypair.pubkey());
//   let cfg = RpcSendTransactionConfig {
//       skip_preflight: true,
//       ..RpcSendTransactionConfig::default()
//   };
//   loop {
//       if let Ok(ixs) = ixs_rx.recv().await {
//         // TODO add priority fee
//           let tx = Transaction::new_signed_with_payer(
//               &ixs,
//               Some(&keypair.pubkey()),
//               &[&keypair],
//               *blockhash.read().unwrap(),
//           );
//           // TODO: collect metrics
//           info!("send tx={:?} ok={:?}", tx.signatures[0], client.send_transaction_with_config(&tx, cfg).await);
//       }
//   }
// }


fn start_crank_thread(
  identity: Keypair,
  group: GroupConfig
) -> JoinHandle<()> {

  let perp_queue_pks: Vec<_> = group.perp_markets.iter().map(|m| (Pubkey::from_str(&m.public_key).unwrap(), Pubkey::from_str(&m.events_key).unwrap())).collect();
  let group_pk = Pubkey::from_str(&group.public_key).unwrap();

  return Builder::new()
  .name("crank".to_string())
  .spawn(move || {



       // Event queue updates can be consumed by client connections
       let (instruction_sender, instruction_receiver) = async_channel::unbounded::<Vec<Instruction>>();


        
        let routes = vec![
       AccountWriteRoute {
        matched_pubkeys: perp_queue_pks
            .iter()
            .map(|(_, evq_pk)| evq_pk.clone())
            .collect(),
        sink: Arc::new(MangoV3PerpCrankSink::new(
            perp_queue_pks,
            group_pk,
            instruction_sender.clone(),
        )),
        timeout_interval: Duration::default(),
    }];

  }).expect("launch crank thread")

} 
