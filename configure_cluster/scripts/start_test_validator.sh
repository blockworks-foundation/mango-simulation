#!/bin/bash

binDir=$(dirname $0)/../bin
solana-test-validator --account MangoCzJ36AjZyKwVj3VnYU4GTonjfVEnJmvvWaxLac $binDir/mango-mint.json --bpf-program noopb9bkMVfRPU8AsbpTUg8AQkHtKwMYZiFUjNRtMmV $binDir/spl_noop.so &
pid="$!"

# handle ctrl-c
trap cleanup INT EXIT KILL 2

cleanup()
{
    echo "cleanup $pid"
    kill -9 $pid
}

sleep 5

solana program deploy bin/mango.so -ul --program-id $binDir/mango.json
solana program deploy bin/serum_dex.so -ul --program-id $binDir/serum_dex.json
solana program deploy bin/pyth_mock.so -ul --program-id $binDir/pyth_mock.json

# idle waiting for abort
wait $pid
