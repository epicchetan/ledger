//! Terminal adapter for Ledger.
//!
//! This binary owns argument parsing, `.env` loading, service construction, and
//! JSON output. Business logic belongs in the library crates: ingest work in
//! `ledger-ingest`, session readiness/loading in `ledger`, and
//! persistence in `ledger-store`.

use anyhow::Result;
use chrono::NaiveDate;
use clap::{Args, Parser, Subcommand};
use ledger::Ledger;
use ledger_ingest::{DatabentoProvider, DbnPreprocessor, IngestConfig, IngestPipeline};
use ledger_store::{CachePrunePolicy, MarketDayFilter, R2LedgerStore};
use std::path::PathBuf;

#[derive(Parser)]
#[command(name = "ledger")]
#[command(about = "Ledger market-day ingestion and replay preparation")]
struct Cli {
    #[arg(long, env = "LEDGER_DATA_DIR", default_value = "data")]
    data_dir: PathBuf,

    #[arg(long, env = "LEDGER_R2_PREFIX", default_value = "ledger/v1")]
    r2_prefix: String,

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    Resolve(MarketDayArgs),
    Ingest(MarketDayArgs),
    #[command(hide = true)]
    Download(MarketDayArgs),
    Status(MarketDayArgs),
    List(ListArgs),
    Session(SessionCommand),
    Cache(CacheCommand),
}

#[derive(Subcommand)]
enum SessionSubcommand {
    Load(MarketDayArgs),
}

#[derive(Args)]
struct SessionCommand {
    #[command(subcommand)]
    command: SessionSubcommand,
}

#[derive(Subcommand)]
enum CacheSubcommand {
    Prune(PruneArgs),
}

#[derive(Args)]
struct CacheCommand {
    #[command(subcommand)]
    command: CacheSubcommand,
}

#[derive(Args, Clone)]
struct MarketDayArgs {
    #[arg(long)]
    symbol: String,
    #[arg(long)]
    date: String,
}

#[derive(Args)]
struct ListArgs {
    #[arg(long)]
    root: Option<String>,
    #[arg(long)]
    symbol: Option<String>,
    #[arg(long)]
    ready: bool,
}

#[derive(Args)]
struct PruneArgs {
    #[arg(long, env = "LEDGER_CACHE_MAX_SESSIONS", default_value_t = 5)]
    max_sessions: usize,
}

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();
    let cli = Cli::parse();

    match cli.command {
        Command::Resolve(args) => {
            let md = ledger_domain::MarketDay::resolve_es(args.symbol, parse_date(&args.date)?)?;
            println!("{}", serde_json::to_string_pretty(&md)?);
        }
        Command::Ingest(args) | Command::Download(args) => {
            let store = R2LedgerStore::from_env(&cli.data_dir, &cli.r2_prefix).await?;
            let pipeline = IngestPipeline::new(
                DatabentoProvider,
                DbnPreprocessor,
                store,
                IngestConfig::default(),
            );
            let report = pipeline
                .ingest_market_day(&args.symbol, parse_date(&args.date)?)
                .await?;
            println!("{}", serde_json::to_string_pretty(&report)?);
        }
        Command::Status(args) => {
            let ledger = Ledger::from_env(&cli.data_dir, &cli.r2_prefix).await?;
            let status = ledger.status(&args.symbol, parse_date(&args.date)?).await?;
            println!("{}", serde_json::to_string_pretty(&status)?);
        }
        Command::List(args) => {
            let ledger = Ledger::from_env(&cli.data_dir, &cli.r2_prefix).await?;
            let rows = ledger.list(MarketDayFilter {
                root: args.root,
                symbol: args.symbol,
                ready: args.ready.then_some(true),
            })?;
            println!("{}", serde_json::to_string_pretty(&rows)?);
        }
        Command::Session(session) => match session.command {
            SessionSubcommand::Load(args) => {
                let ledger = Ledger::from_env(&cli.data_dir, &cli.r2_prefix).await?;
                let inputs = ledger
                    .load_replay_session(&args.symbol, parse_date(&args.date)?)
                    .await?;
                println!("{}", serde_json::to_string_pretty(&inputs)?);
            }
        },
        Command::Cache(cache) => match cache.command {
            CacheSubcommand::Prune(args) => {
                let store = R2LedgerStore::from_env(&cli.data_dir, &cli.r2_prefix).await?;
                let report = store.prune_cache(CachePrunePolicy {
                    max_sessions: args.max_sessions,
                })?;
                println!("{}", serde_json::to_string_pretty(&report)?);
            }
        },
    }

    Ok(())
}

fn parse_date(date: &str) -> Result<NaiveDate> {
    Ok(NaiveDate::parse_from_str(date, "%Y-%m-%d")?)
}
