#!/usr/bin/env bash
#
# Run a minimal Solana cluster.  Ctrl-C to exit.
#
# Before running this script ensure standard Solana programs are available
# in the PATH, or that `cargo build` ran successfully
#
# Prefer possible `cargo build` binaries over PATH binaries

# ctrl-c trap to stop child processes
trap ctrl_c INT
function ctrl_c() {
    echo "Kill them all"
    pkill -P $$
    exit 1
}

outDir=$PWD

export RUST_LOG=${RUST_LOG:-solana=info,solana_runtime::message_processor=debug} # if RUST_LOG is unset, default to info
export RUST_BACKTRACE=1
dataDir=$outDir/config
ledgerDir=$outDir/config/ledger
binDir=$(dirname $0)/../bin

set -x
if ! solana address; then
  echo Generating default keypair
  solana-keygen new --no-passphrase
fi
validator_identity="$dataDir/validator-identity.json"
if [[ -e $validator_identity ]]; then
  echo "Use existing validator keypair"
else
  solana-keygen new --no-passphrase -so "$validator_identity"
fi
validator_vote_account="$dataDir/validator-vote-account.json"
if [[ -e $validator_vote_account ]]; then
  echo "Use existing validator vote account keypair"
else
  solana-keygen new --no-passphrase -so "$validator_vote_account"
fi
validator_stake_account="$dataDir/validator-stake-account.json"
if [[ -e $validator_stake_account ]]; then
  echo "Use existing validator stake account keypair"
else
  solana-keygen new --no-passphrase -so "$validator_stake_account"
fi

if [[ -e "$ledgerDir"/genesis.bin || -e "$ledgerDir"/genesis.tar.bz2 ]]; then
  echo "Use existing genesis"
else
  echo $SPL_GENESIS_ARGS
  # shellcheck disable=SC2086
  solana-genesis \
    --hashes-per-tick sleep \
    --faucet-lamports 500000000000000000 \
    --bootstrap-validator \
      "$validator_identity" \
      "$validator_vote_account" \
      "$validator_stake_account" \
    --ledger "$ledgerDir" \
    --cluster-type "development" \
    --bpf-program TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA BPFLoader2111111111111111111111111111111111 $binDir/spl_token-3.5.0.so \
    --bpf-program ATokenGPvbdGVxr1b2hvZbsiqW5xWH25efTNsLJA8knL BPFLoader2111111111111111111111111111111111 $binDir/spl_associated-token-account-1.1.1.so \
    --bpf-program DGKy8w8RtRsWB48qHa4yCd3AeP5uv4m3Qn7LU8z93RWV BPFLoader2111111111111111111111111111111111 $binDir/mango.so \
    --bpf-program 3WAiypER8fm6vHjUPRiigGifq6ueSY645aYGH5Jj14pU BPFLoader2111111111111111111111111111111111 $binDir/serum_dex.so \
    --bpf-program EoUiQKGpM4jsdb5oRnYnuWMaE4Gcey72QjEBbFxhk23C BPFLoader2111111111111111111111111111111111 $binDir/pyth_mock.so \
    --bpf-program noopb9bkMVfRPU8AsbpTUg8AQkHtKwMYZiFUjNRtMmV BPFLoader2111111111111111111111111111111111 $binDir/spl_noop.so \
    --bpf-program Memo1UhkJRfHyvLMcVucJwxXeuD728EqVDDwQDxFMNo BPFLoader1111111111111111111111111111111111 $binDir/spl_memo-1.0.0.so \
    --bpf-program MemoSq4gqABAXKb96qnH8TysNcWxMyWCqXgDLGmfcHr BPFLoader2111111111111111111111111111111111 $binDir/spl_memo-3.0.0.so \
    $SOLANA_RUN_SH_GENESIS_ARGS
fi

solana-faucet &
faucet=$!

args=(
  --identity "$validator_identity"
  --vote-account "$validator_vote_account"
  --ledger "$ledgerDir"
  --gossip-port 8001
  --full-rpc-api
  --rpc-port 8899
  --rpc-faucet-address 127.0.0.1:9900
  --log "$dataDir/validator.log"
  --enable-rpc-transaction-history
  --enable-extended-tx-metadata-storage
  --init-complete-file "$dataDir"/init-completed
  --snapshot-compression none
  --require-tower
  --no-wait-for-vote-to-start-leader
  --no-os-network-limits-test
  --allow-private-addr
)
# shellcheck disable=SC2086
solana-validator "${args[@]}" $SOLANA_RUN_SH_VALIDATOR_ARGS &
validator=$!

wait "$validator"
