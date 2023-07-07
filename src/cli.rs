use {
    clap::{crate_description, crate_name, App, Arg, ArgMatches},
    solana_clap_utils::input_validators::{is_url, is_url_or_moniker, is_valid_percentage},
    solana_cli_config::{ConfigInput, CONFIG_FILE},
    solana_sdk::signature::{read_keypair_file, Keypair},
    std::{net::SocketAddr, process::exit, time::Duration},
};

/// Holds the configuration for a single run of the benchmark
pub struct Config {
    pub entrypoint_addr: SocketAddr,
    pub json_rpc_url: String,
    pub websocket_url: String,
    pub identity: Keypair,
    pub duration: Duration,
    pub quotes_per_second: u64,
    pub account_keys: String,
    pub mango_keys: String,
    pub transaction_save_file: String,
    pub block_data_save_file: String,
    pub mango_cluster: String,
    pub txs_batch_size: Option<usize>,
    pub priority_fees_proba: u8,
    pub keeper_prioritization: u64,
    pub keeper_authority: Option<Keypair>,
    pub number_of_markers_per_mm: u8,
}

impl Default for Config {
    fn default() -> Config {
        Config {
            entrypoint_addr: SocketAddr::from(([127, 0, 0, 1], 8001)),
            json_rpc_url: ConfigInput::default().json_rpc_url,
            websocket_url: ConfigInput::default().websocket_url,
            identity: Keypair::new(),
            duration: Duration::new(std::u64::MAX, 0),
            quotes_per_second: 1,
            account_keys: String::new(),
            mango_keys: String::new(),
            transaction_save_file: String::new(),
            block_data_save_file: String::new(),
            mango_cluster: "testnet.0".to_string(),
            txs_batch_size: None,
            priority_fees_proba: 0,
            keeper_authority: None,
            number_of_markers_per_mm: 5,
            keeper_prioritization: 1000,
        }
    }
}

/// Defines and builds the CLI args for a run of the benchmark
pub fn build_args(version: &str) -> App<'_, '_> {
    App::new(crate_name!())
        .about(crate_description!())
        .version(version)
        .arg({
            let arg = Arg::with_name("config-file")
                .short("C")
                .long("config")
                .value_name("FILEPATH")
                .takes_value(true)
                .global(true)
                .help("Configuration file to use");
            if let Some(ref config_file) = *CONFIG_FILE {
                arg.default_value(config_file)
            } else {
                arg
            }
        })
        .arg(
            Arg::with_name("json-rpc-url")
                .short("u")
                .long("url")
                .value_name("URL_OR_MONIKER")
                .takes_value(true)
                .global(true)
                .validator(is_url_or_moniker)
                .help(
                    "URL for Solana's JSON RPC or moniker (or their first letter): \
                     [mainnet-beta, testnet, devnet, localhost]",
                ),
        )
        .arg(
            Arg::with_name("websocket-url")
                .long("ws")
                .value_name("URL")
                .takes_value(true)
                .global(true)
                .validator(is_url)
                .help("WebSocket URL for the solana cluster"),
        )
        .arg(
            Arg::with_name("entrypoint")
                .short("n")
                .long("entrypoint")
                .value_name("HOST:PORT")
                .takes_value(true)
                .help(
                    "Rendezvous with the cluster at this entry point; defaults to 127.0.0.1:8001",
                ),
        )
        .arg(
            Arg::with_name("identity")
                .short("i")
                .long("identity")
                .value_name("FILEPATH")
                .takes_value(true)
                .help("Identity used in the QUIC connection. Identity with a lot of stake has a \
                better chance to send transaction to the leader"),
        )
        .arg(
            Arg::with_name("duration")
                .short("d")
                .long("duration")
                .value_name("SECS")
                .takes_value(true)
                .help("Seconds to run benchmark, then exit; default is forever"),
        )
        .arg(
            Arg::with_name("quotes-per-second")
                .short("q")
                .long("quotes-per-second")
                .value_name("QPS")
                .takes_value(true)
                .help("Number of quotes per second"),
        )
        .arg(
            Arg::with_name("account-keys")
                .short("a")
                .long("accounts")
                .value_name("FILENAME")
                .required(true)
                .takes_value(true)
                .help("Read account keys from JSON file generated with mango-client-v3"),
        )
        .arg(
            Arg::with_name("mango-keys")
                .short("m")
                .long("mango")
                .value_name("FILENAME")
                .required(true)
                .takes_value(true)
                .help("Read mango keys from JSON file generated with mango-client-v3"),
        )
        .arg(
            Arg::with_name("transaction-save-file")
                .short("tsf")
                .long("transaction-save-file")
                .value_name("FILENAME")
                .takes_value(true)
                .required(false)
                .help("To save details of all transactions during a run"),
        )
        .arg(
            Arg::with_name("block-data-save-file")
                .short("bdsf")
                .long("block-data-save-file")
                .value_name("FILENAME")
                .takes_value(true)
                .required(false)
                .help("To save details of all block containing mm transactions"),
        )
        .arg(
            Arg::with_name("mango-cluster")
                .short("c")
                .long("mango-cluster")
                .value_name("STR")
                .takes_value(true)
                .help("Name of mango cluster from ids.json"),
        )
        .arg(
            Arg::with_name("batch-size")
                .long("batch-size")
                .value_name("UINT")
                .takes_value(true)
                .required(false)
                .help("If specified, transactions are send in batches of specified size"),
        )
        .arg(
            Arg::with_name("prioritization-fees")
                .long("prioritization-fees")
                .value_name("UINT")
                .min_values(1)
                .validator(is_valid_percentage)
                .takes_value(true)
                .required(false)
                .help("Takes percentage of transaction we want to add random prioritization fees to, prioritization fees are random number between 100-1000")
        )
        .arg(
            Arg::with_name("keeper-authority")
                .long("keeper-authority")
                .short("ka")
                .value_name("FILEPATH")
                .takes_value(true)
                .required(false)
                .help(
                    "If specified, authority keypair would be used to pay for keeper transactions",
                ),
        )
        .arg(
            Arg::with_name("markets-per-mm")
                .long("markets-per-mm")
                .value_name("UINT")
                .takes_value(true)
                .required(false)
                .help("Number of markets a market maker will trade on at a time"),
        )
        .arg(
            Arg::with_name("keeper-prioritization-fees")
                .long("keeper-prioritization-fees")
                .value_name("UINT")
                .min_values(0)
                .takes_value(true)
                .required(false)
                .help("Prioritization fees set for all keeper instructions (1000 by default)")
        )
}

/// Parses a clap `ArgMatches` structure into a `Config`
/// # Arguments
/// * `matches` - command line arguments parsed by clap
/// # Panics
/// Panics if there is trouble parsing any of the arguments
pub fn extract_args(matches: &ArgMatches) -> Config {
    let mut args = Config::default();

    let config = if let Some(config_file) = matches.value_of("config-file") {
        solana_cli_config::Config::load(config_file).unwrap_or_default()
    } else {
        solana_cli_config::Config::default()
    };
    let (_, json_rpc_url) = ConfigInput::compute_json_rpc_url_setting(
        matches.value_of("json-rpc-url").unwrap_or(""),
        &config.json_rpc_url,
    );
    args.json_rpc_url = json_rpc_url;

    let (_, websocket_url) = ConfigInput::compute_websocket_url_setting(
        matches.value_of("websocket-url").unwrap_or(""),
        &config.websocket_url,
        matches.value_of("json-rpc-url").unwrap_or(""),
        &config.json_rpc_url,
    );
    args.websocket_url = websocket_url;

    let (_, id_path) = ConfigInput::compute_keypair_path_setting(
        matches.value_of("identity").unwrap_or(""),
        &config.keypair_path,
    );
    if let Ok(id) = read_keypair_file(id_path) {
        args.identity = id;
    } else if matches.is_present("identity") {
        panic!("could not parse identity path");
    }

    if let Some(addr) = matches.value_of("entrypoint") {
        args.entrypoint_addr = solana_net_utils::parse_host_port(addr).unwrap_or_else(|e| {
            eprintln!("failed to parse entrypoint address: {}", e);
            exit(1)
        });
    }

    if let Some(duration) = matches.value_of("duration") {
        args.duration = Duration::new(
            duration.to_string().parse().expect("can't parse duration"),
            0,
        );
    }

    if let Some(qps) = matches.value_of("quotes-per-second") {
        args.quotes_per_second = qps.parse().expect("can't parse quotes-per-second");
    }

    args.account_keys = matches.value_of("account-keys").unwrap().to_string();
    args.mango_keys = matches.value_of("mango-keys").unwrap().to_string();
    args.transaction_save_file = match matches.value_of("transaction-save-file") {
        Some(x) => x.to_string(),
        None => String::new(),
    };
    args.block_data_save_file = match matches.value_of("block-data-save-file") {
        Some(x) => x.to_string(),
        None => String::new(),
    };

    args.mango_cluster = match matches.value_of("mango-cluster") {
        Some(x) => x.to_string(),
        None => "testnet.0".to_string(),
    };
    args.txs_batch_size = matches
        .value_of("batch-size")
        .map(|batch_size_str| batch_size_str.parse().expect("can't parse batch-size"));

    args.priority_fees_proba = match matches.value_of("prioritization-fees") {
        Some(x) => x
            .parse()
            .expect("Percentage of transactions having prioritization fees"),
        None => 0,
    };
    let (_, kp_auth_path) = ConfigInput::compute_keypair_path_setting(
        matches.value_of("keeper-authority").unwrap_or(""),
        &config.keypair_path,
    );

    args.keeper_authority = read_keypair_file(kp_auth_path).ok();

    args.number_of_markers_per_mm = match matches.value_of("markets-per-mm") {
        Some(x) => x
            .parse()
            .expect("can't parse number of markets per market maker"),
        None => 5,
    };

    args.keeper_prioritization = match matches.value_of("keeper-prioritization-fees") {
        Some(x) => x.parse().expect("can't parse keeper prioritization fees"),
        None => 1000,
    };
    args
}
