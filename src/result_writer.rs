use async_channel::Receiver;
use tokio::task::JoinHandle;

use crate::states::{BlockData, TransactionConfirmRecord};

pub fn initialize_result_writers(
    transaction_save_file: String,
    block_data_save_file: String,
    tx_data: Receiver<TransactionConfirmRecord>,
    block_data: Receiver<BlockData>,
) -> Vec<JoinHandle<()>> {
    let mut tasks = vec![];

    if !transaction_save_file.is_empty() {
        let tx_data_jh = tokio::spawn(async move {
            let mut writer = csv::Writer::from_path(transaction_save_file).unwrap();
            loop {
                if let Ok(record) = tx_data.recv().await {
                    writer.serialize(record).unwrap();
                } else {
                    break;
                }
            }
            writer.flush().unwrap();
        });
        tasks.push(tx_data_jh);
    }

    if !block_data_save_file.is_empty() {
        let block_data_jh = tokio::spawn(async move {
            let mut writer = csv::Writer::from_path(block_data_save_file).unwrap();
            loop {
                if let Ok(record) = block_data.recv().await {
                    writer.serialize(record).unwrap();
                } else {
                    break;
                }
            }
            writer.flush().unwrap();
        });
        tasks.push(block_data_jh);
    }
    tasks
}
