//! `tokor` — operator CLI for the Tokor engine (Phase 1: data foundation).
//!
//! Hard restriction: this binary performs ONLY read-only public-data work
//! (wallet activity ingestion, market metadata, classification, position
//! reconstruction, reconciliation, reporting). No order placement, no
//! authentication, no key material — enforced by the dependency graph
//! (no CLOB order routes exist in this crate).

mod commands;
mod config;
mod markets;

use std::path::PathBuf;

use config::AppConfig;

struct Cli {
    config_path: PathBuf,
    db_override: Option<PathBuf>,
    command: Command,
}

enum Command {
    ConfigValidate,
    IngestWallet {
        full: bool,
        limit: Option<u64>,
        max_pages: Option<u64>,
    },
    IngestActivity {
        full: bool,
        limit: Option<u64>,
        max_pages: Option<u64>,
    },
    IngestMarkets {
        limit: Option<u64>,
    },
    ClassifyMarkets {
        reset: bool,
    },
    ReconstructPositions,
    ReconcilePositions,
    IngestPositions,
    DeriveFees {
        categories: Option<PathBuf>,
    },
    DeriveParquet {
        dataset: String,
        out: PathBuf,
    },
    RecordMarket {
        tokens: Vec<String>,
    },
    ObserveSource {
        probes: u64,
    },
    ShadowRun {
        limit: Option<u64>,
        scenario: Option<String>,
    },
    Report,
}

fn usage() -> String {
    "USAGE:
  tokor --config config/default.toml <COMMAND>

COMMANDS:
  config-validate                        Validate the configuration and exit.
  ingest-wallet [--full|--resume] [--limit N] [--max-pages N]
                                         Acquire wallet activity (Data API v2).
  ingest-activity [--full|--resume] [--limit N] [--max-pages N]
                                         Acquire ALL wallet activity types
                                         (Phase 1.5: merges, redeems, rebates...).
  ingest-markets [--limit N]             Enrich market metadata (CLOB + Gamma).
  classify-markets [--reset]             Run the crypto classifier; --reset re-evaluates
                                         all recorded classifications (use after re-enrichment).
  reconstruct-positions                  Rebuild FIFO positions from persisted fills.
  reconcile-positions                    Compare reconstruction against /v2/positions.
  ingest-positions                       Snapshot /v2/positions (OPEN+CLOSED+REDEEMABLE)
                                         into positions_snapshot (Phase 1.5).
  derive-fees [--categories PATH]        Classify fills under the verified fee model
                                         (default: data/derived/event_fee_categories.json).
  derive-parquet <fees|activity|inventory> --out PATH
                                         Emit Phase 1.5 parquet datasets.
  record-market <TOKEN_ID>...            Record public CLOB market data for the
                                         given tokens until ctrl-c (Phase 2 §3;
                                         read-only WS + REST, no keys).
  observe-source [--probes N]            Independent latency probe of the
                                         wallet-feed → decision chain (Phase 2
                                         §4; default 30 probes).
  shadow-run [--limit N] [--scenario optimistic|baseline|conservative]
                                         Replay stored fills through the
                                         shadow-exec simulators over recorded
                                         market data (Phase 2 §5; baseline
                                         default; submitted ≠ executed).
  report                                 Print a data-foundation status report."
        .to_string()
}

fn parse_args() -> Result<Cli, String> {
    let mut args = std::env::args().skip(1).peekable();
    let mut config_path = PathBuf::from("config/default.toml");
    let mut db_override = None;
    let mut positional = Vec::new();
    let mut flags: Vec<(String, Option<String>)> = Vec::new();

    while let Some(a) = args.next() {
        match a.as_str() {
            "--config" => {
                config_path = args.next().ok_or("--config requires a value")?.into();
            }
            "--db" => {
                db_override = Some(PathBuf::from(args.next().ok_or("--db requires a value")?));
            }
            s if s.starts_with("--") => {
                // Value flags (--limit N, --out PATH, --probes N) carry the
                // next token unless it is itself a flag. Boolean flags take
                // nothing.
                let value = args.next_if(|a| !a.starts_with("--"));
                flags.push((s.to_string(), value));
            }
            _ => positional.push(a),
        }
    }

    let command = match positional.first().map(|s| s.as_str()) {
        Some("config-validate") => Command::ConfigValidate,
        Some("ingest-wallet") => Command::IngestWallet {
            full: !flags.iter().any(|(f, _)| f == "--resume"),
            limit: flag_value_u64(&flags, "--limit"),
            max_pages: flag_value_u64(&flags, "--max-pages"),
        },
        Some("ingest-activity") => Command::IngestActivity {
            full: !flags.iter().any(|(f, _)| f == "--resume"),
            limit: flag_value_u64(&flags, "--limit"),
            max_pages: flag_value_u64(&flags, "--max-pages"),
        },
        Some("ingest-markets") => Command::IngestMarkets {
            limit: flag_value_u64(&flags, "--limit"),
        },
        Some("classify-markets") => Command::ClassifyMarkets {
            reset: flags.iter().any(|(f, _)| f == "--reset"),
        },
        Some("reconstruct-positions") => Command::ReconstructPositions,
        Some("reconcile-positions") => Command::ReconcilePositions,
        Some("ingest-positions") => Command::IngestPositions,
        Some("derive-fees") => Command::DeriveFees {
            categories: flag_value_path(&flags, "--categories"),
        },
        Some("derive-parquet") => Command::DeriveParquet {
            dataset: positional
                .get(1)
                .cloned()
                .ok_or("derive-parquet needs a dataset: fees|activity|inventory")?,
            out: flag_value_path(&flags, "--out").ok_or("derive-parquet needs --out PATH")?,
        },
        Some("record-market") => {
            if positional.len() < 2 {
                return Err("record-market needs at least one TOKEN_ID\n{}".to_string() + &usage());
            }
            Command::RecordMarket {
                tokens: positional[1..].to_vec(),
            }
        }
        Some("observe-source") => Command::ObserveSource {
            probes: flag_value_u64(&flags, "--probes").unwrap_or(30),
        },
        Some("shadow-run") => Command::ShadowRun {
            limit: flag_value_u64(&flags, "--limit"),
            scenario: flag_value(&flags, "--scenario"),
        },
        Some("report") => Command::Report,
        Some(other) => return Err(format!("unknown command {other:?}\n{}", usage())),
        None => return Err(usage()),
    };

    Ok(Cli {
        config_path,
        db_override,
        command,
    })
}

fn flag_value_u64(flags: &[(String, Option<String>)], name: &str) -> Option<u64> {
    let idx = flags.iter().position(|(f, _)| f == name)?;
    flags[idx].1.as_deref().and_then(|v| v.parse::<u64>().ok())
}

fn flag_value_path(flags: &[(String, Option<String>)], name: &str) -> Option<PathBuf> {
    let idx = flags.iter().position(|(f, _)| f == name)?;
    flags[idx].1.as_deref().map(PathBuf::from)
}

fn flag_value(flags: &[(String, Option<String>)], name: &str) -> Option<String> {
    let idx = flags.iter().position(|(f, _)| f == name)?;
    flags[idx].1.clone()
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .init();

    let cli = match parse_args() {
        Ok(c) => c,
        Err(e) => {
            eprintln!("{e}");
            std::process::exit(2);
        }
    };

    let cfg = match AppConfig::load(&cli.config_path) {
        Ok(c) => c,
        Err(e) => {
            eprintln!("config error: {e}");
            std::process::exit(2);
        }
    };
    let cfg = AppConfig {
        db_path: cli.db_override.clone().unwrap_or(cfg.db_path.clone()),
        ..cfg
    };

    let outcome: Result<String, String> = match &cli.command {
        Command::ConfigValidate => Ok(format!(
            "config OK: wallet {}, db {}, data_api {}",
            cfg.wallet_address,
            cfg.db_path.display(),
            cfg.data_api_base
        )),
        Command::IngestWallet {
            full,
            limit,
            max_pages,
        } => commands::ingest_wallet(&cfg, *full, *limit, *max_pages).await,
        Command::IngestActivity {
            full,
            limit,
            max_pages,
        } => commands::ingest_activity(&cfg, *full, *limit, *max_pages).await,
        Command::IngestMarkets { limit } => commands::ingest_markets(&cfg, *limit).await,
        Command::ClassifyMarkets { reset } => commands::classify_markets(&cfg, *reset),
        Command::ReconstructPositions => commands::reconstruct_positions(&cfg),
        Command::ReconcilePositions => commands::reconcile_positions(&cfg).await,
        Command::IngestPositions => commands::ingest_positions(&cfg).await,
        Command::DeriveFees { categories } => commands::derive_fees(
            &cfg,
            categories.as_deref().unwrap_or(std::path::Path::new(
                "data/derived/event_fee_categories.json",
            )),
        ),
        Command::DeriveParquet { dataset, out } => commands::derive_parquet(&cfg, dataset, out),
        Command::Report => commands::report(&cfg),
        Command::RecordMarket { tokens } => commands::record_market(&cfg, tokens).await,
        Command::ObserveSource { probes } => commands::observe_source(&cfg, *probes).await,
        Command::ShadowRun { limit, scenario } => commands::shadow_run(
            &cfg,
            limit.unwrap_or(1_000),
            scenario.as_deref().unwrap_or("baseline"),
        ),
    };

    match outcome {
        Ok(msg) => {
            println!("{msg}");
        }
        Err(e) => {
            eprintln!("error: {e}");
            std::process::exit(1);
        }
    }
}
