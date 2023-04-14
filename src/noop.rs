use std::str::FromStr;

use solana_sdk::{pubkey::Pubkey, instruction::Instruction};

pub fn instruction(data: Vec<u8>) -> Instruction {
  Instruction {
      program_id: Pubkey::from_str("noopb9bkMVfRPU8AsbpTUg8AQkHtKwMYZiFUjNRtMmV").unwrap(),
      accounts: vec![],
      data,
  }
}