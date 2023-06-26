# Mango Simulation - test solana cluster by simulating mango markets

This project is use to stress a solana cluster like devnet, testnet or local
solana cluster by simulating mango markets. This code requires ids.json which
describe mango group for a cluster and accounts.json file which has
preconfigured user accounts in mango.

The code then will create transaction request (q) requests per seconds for (n)
seconds per perp market perp user. Each transaction request will contains remove
following instruction CancelAllPerpOrders and two PlacePerpOrder (one for bid
and another for ask).

For the best results to avoid limits by quic it is better to fill the argument
"identity" of a valid staked validator for the cluster you are testing with.

## Build

```sh
git clone https://github.com/blockworks-foundation/mango-simulation.git
cd mango-simulation
cargo build --release
```

## Configure cluster

Install all nodejs dependencies

```sh
yarn install
```

To start a local cluster. This will start a validator with all the data in
config directory.

```sh
sh configure_cluster/scripts/configure_local.sh
```

To create a configuration. This will create two files `ids.json` which contain
cluster information and `account.json` which contains user information.

```sh
# open a new terminal as the previous one will continue running a solana validator
# this command will hang for around a minute, just wait for it to finish
NB_USERS=50 yarn ts-node configure_cluster/configure_mango_v3.ts
```

## Run


To run against your local validator:
```sh
cargo run --bin mango-simulation -- -u http://127.0.0.1:8899 --identity config/validator-identity.json --keeper-authority authority.json --accounts accounts.json  --mango ids.json --mango-cluster localnet --duration 10 -q 2 --transaction-save-file tlog.csv --block-data-save-file blog.csv
```

You can also run the simulation against testnet, but you will need to run configure_mango 

Details for each argument:
```
USAGE:
    mango-simulation [OPTIONS] --accounts <FILENAME> --mango <FILENAME>

FLAGS:
    -h, --help       Prints help information
    -V, --version    Prints version information

OPTIONS:
    -a, --accounts <FILENAME>                 Read account keys from JSON file generated with mango-client-v3
        --batch-size <UINT>                   If specified, transactions are send in batches of specified size
    -b, --block-data-save-file <FILENAME>     To save details of all block containing mm transactions
    -C, --config <FILEPATH>                   Configuration file to use [default:
                                              /home/galactus/.config/solana/cli/config.yml]
    -d, --duration <SECS>                     Seconds to run benchmark, then exit; default is forever
    -n, --entrypoint <HOST:PORT>              Rendezvous with the cluster at this entry point; defaults to
                                              127.0.0.1:8001
    -i, --identity <FILEPATH>                 Identity used in the QUIC connection. Identity with a lot of stake has a
                                              better chance to send transaction to the leader
    -u, --url <URL_OR_MONIKER>                URL for Solana's JSON RPC or moniker (or their first letter): [mainnet-
                                              beta, testnet, devnet, localhost]
    -k, --keeper-authority <FILEPATH>         If specified, authority keypair would be used to pay for keeper
                                              transactions
    -c, --mango-cluster <STR>                 Name of mango cluster from ids.json
    -m, --mango <FILENAME>                    Read mango keys from JSON file generated with mango-client-v3
        --markets-per-mm <UINT>               Number of markets a market maker will trade on at a time
        --prioritization-fees <UINT>          Takes percentage of transaction we want to add random prioritization fees
                                              to, prioritization fees are random number between 100-1000
    -q, --quotes-per-second <QPS>             Number of quotes per second
    -t, --transaction-save-file <FILENAME>    To save details of all transactions during a run
        --ws <URL>                            WebSocket URL for the solana cluster

```
