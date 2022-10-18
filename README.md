# Mango bencher - stress solana cluster by simulating mango markets

This project is use to stress a solana cluster like devnet, testnet or local solana cluster by simulating mango markets. This code requires ids.json which describe mango group for a cluster and accounts.json file which has preconfigured user accounts in mango.

The code then will create transaction request (q) requests per seconds for (n) seconds per perp market perp user. Each transaction request will contains remove following instruction CancelAllPerpOrders and two PlacePerpOrder (one for bid and another for ask).

## Build

Clone repo
```sh
git clone https://github.com/godmodegalactus/mango_bencher
```

Update submodules
```sh
git submodule update --init --recursive
```

Set solana version in deps (replace v1.14.3 with require solana version)
```sh
cd deps/solana && git checkout v1.14.3
```

Build
```sh
cargo build -j8
```

## Run

```sh
cargo run --bin solana-bench-mango -- -u http://localhost:8899 --identity authority.json --accounts accounts-20.json  --mango ids.json --mango-cluster localnet --duration 10 -q 2 --transaction_save_file tlog.csv --block_data_save_file blog.csv
```

help will give following results
```
USAGE:
    solana-bench-mango [FLAGS] [OPTIONS] --accounts <FILENAME> --mango <FILENAME>

FLAGS:
        --airdrop-accounts    Airdrop all MM accounts before stating
    -h, --help                Prints help information
    -V, --version             Prints version information

OPTIONS:
    -a, --accounts <FILENAME>                 Read account keys from JSON file generated with mango-client-v3
    -b, --block_data_save_file <FILENAME>     To save details of all block containing mm transactions
    -C, --config <FILEPATH>                   Configuration file to use [default:
                                              /home/galactus/.config/solana/cli/config.yml]
    -d, --duration <SECS>                     Seconds to run benchmark, then exit; default is forever
    -n, --entrypoint <HOST:PORT>              Rendezvous with the cluster at this entry point; defaults to
                                              127.0.0.1:8001
    -i, --identity <PATH>                     File containing a client identity (keypair)
    -u, --url <URL_OR_MONIKER>                URL for Solana's JSON RPC or moniker (or their first letter): [mainnet-
                                              beta, testnet, devnet, localhost]
    -c, --mango-cluster <STR>                 Name of mango cluster from ids.json
    -m, --mango <FILENAME>                    Read mango keys from JSON file generated with mango-client-v3
    -q, --qoutes_per_second <QPS>             Number of quotes per second
    -t, --transaction_save_file <FILENAME>    To save details of all transactions during a run
        --ws <URL>                            WebSocket URL for the solana cluster

```