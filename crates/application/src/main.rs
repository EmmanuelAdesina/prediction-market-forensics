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
    IngestMarkets {
        limit: Option<u64>,
    },
    ClassifyMarkets {
        reset: bool,
    },
    ReconstructPositions,
    ReconcilePositions,
    Report,
}

fn usage() -> String {
    "USAGE:
  tokor --config config/default.toml <COMMAND>

COMMANDS:
  config-validate                        Validate the configuration and exit.
  ingest-wallet [--full|--resume] [--limit N] [--max-pages N]
                                         Acquire wallet activity (Data API v2).
  ingest-markets [--limit N]             Enrich market metadata (CLOB + Gamma).
  classify-markets [--reset]             Run the crypto classifier; --reset re-evaluates
                                         all recorded classifications (use after re-enrichment).
  reconstruct-positions                  Rebuild FIFO positions from persisted fills.
  reconcile-positions                    Compare reconstruction against /v2/positions.
  report                                 Print a data-foundation status report."
        .to_string()
}

fn parse_args() -> Result<Cli, String> {
    let mut args = std::env::args().skip(1);
    let mut config_path = PathBuf::from("config/default.toml");
    let mut db_override = None;
    let mut positional = Vec::new();
    let mut flags = Vec::new();

    while let Some(a) = args.next() {
        match a.as_str() {
            "--config" => {
                config_path = args.next().ok_or("--config requires a value")?.into();
            }
            "--db" => {
                db_override = Some(PathBuf::from(args.next().ok_or("--db requires a value")?));
            }
            s if s.starts_with("--") => flags.push(s.to_string()),
            _ => positional.push(a),
        }
    }

    let command = match positional.first().map(|s| s.as_str()) {
        Some("config-validate") => Command::ConfigValidate,
        Some("ingest-wallet") => Command::IngestWallet {
            full: !flags.iter().any(|f| f == "--resume"),
            limit: flag_value_u64(&flags, "--limit"),
            max_pages: flag_value_u64(&flags, "--max-pages"),
        },
        Some("ingest-markets") => Command::IngestMarkets {
            limit: flag_value_u64(&flags, "--limit"),
        },
        Some("classify-markets") => Command::ClassifyMarkets {
            reset: flags.iter().any(|f| f == "--reset"),
        },
        Some("reconstruct-positions") => Command::ReconstructPositions,
        Some("reconcile-positions") => Command::ReconcilePositions,
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

fn flag_value_u64(flags: &[String], name: &str) -> Option<u64> {
    let idx = flags.iter().position(|f| f == name)?;
    flags.get(idx + 1).and_then(|v| v.parse::<u64>().ok())
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
        Command::IngestMarkets { limit } => commands::ingest_markets(&cfg, *limit).await,
        Command::ClassifyMarkets { reset } => commands::classify_markets(&cfg, *reset),
        Command::ReconstructPositions => commands::reconstruct_positions(&cfg),
        Command::ReconcilePositions => commands::reconcile_positions(&cfg).await,
        Command::Report => commands::report(&cfg),
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
