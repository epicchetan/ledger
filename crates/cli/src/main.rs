//! Terminal adapter for Ledger.
//!
//! This binary owns argument parsing, `.env` loading, service construction, and
//! JSON output. Business logic belongs in the library crates: ingest work in
//! `ledger-ingest`, replay dataset readiness/loading in `ledger`, and
//! persistence in `ledger-store`.

use anyhow::Result;
use chrono::NaiveDate;
use clap::{Args, Parser, Subcommand};
use ledger::{
    Ledger, LedgerProgressEvent, LedgerProgressSink, ValidateReplayDatasetRequest,
    ValidationTrigger,
};
use ledger_store::MarketDayFilter;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Instant;

#[derive(Parser)]
#[command(name = "ledger")]
#[command(about = "Ledger market-day ingestion and replay preparation")]
struct Cli {
    #[arg(long, env = "LEDGER_DATA_DIR", default_value = "data")]
    data_dir: PathBuf,

    #[arg(long, env = "LEDGER_R2_PREFIX", default_value = "ledger/v1")]
    r2_prefix: String,

    #[arg(long, global = true)]
    quiet: bool,

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
    Storage(StorageCommand),
    Session(SessionCommand),
}

#[derive(Subcommand)]
enum StorageSubcommand {
    CleanupTmp {
        #[arg(long)]
        older_than_hours: Option<u64>,
    },
}

#[derive(Args)]
struct StorageCommand {
    #[command(subcommand)]
    command: StorageSubcommand,
}

#[derive(Subcommand)]
enum SessionSubcommand {
    Validate(ValidateArgs),
}

#[derive(Args)]
struct SessionCommand {
    #[command(subcommand)]
    command: SessionSubcommand,
}

#[derive(Args, Clone)]
struct MarketDayArgs {
    #[arg(long)]
    symbol: String,
    #[arg(long)]
    date: String,
}

#[derive(Args, Clone)]
struct ValidateArgs {
    #[command(flatten)]
    market_day: MarketDayArgs,

    #[arg(long, conflicts_with = "replay_all")]
    replay_batches: Option<usize>,

    #[arg(long)]
    replay_all: bool,

    #[arg(long)]
    skip_book_check: bool,
}

#[derive(Args)]
struct ListArgs {
    #[arg(long)]
    root: Option<String>,
    #[arg(long)]
    symbol: Option<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();
    let cli = Cli::parse();
    let progress = Progress::new(cli.quiet);

    match cli.command {
        Command::Resolve(args) => {
            let md = ledger_domain::MarketDay::resolve_es(args.symbol, parse_date(&args.date)?)?;
            println!("{}", serde_json::to_string_pretty(&md)?);
        }
        Command::Ingest(args) | Command::Download(args) => {
            progress.step(format!(
                "ingesting {} {}",
                args.symbol,
                parse_date(&args.date)?
            ));
            let ledger = Ledger::from_env(&cli.data_dir, &cli.r2_prefix).await?;
            let started_at = Instant::now();
            let report = ledger
                .ingest_market_day(&args.symbol, parse_date(&args.date)?)
                .await?;
            progress.done("ingest completed", started_at);
            println!("{}", serde_json::to_string_pretty(&report)?);
        }
        Command::Status(args) => {
            progress.step(format!(
                "checking status for {} {}",
                args.symbol,
                parse_date(&args.date)?
            ));
            let ledger = Ledger::from_env(&cli.data_dir, &cli.r2_prefix).await?;
            let status = ledger.status(&args.symbol, parse_date(&args.date)?).await?;
            println!("{}", serde_json::to_string_pretty(&status)?);
        }
        Command::List(args) => {
            progress.step("listing SQLite catalog market days");
            let ledger = Ledger::from_env(&cli.data_dir, &cli.r2_prefix).await?;
            let rows = ledger
                .list(MarketDayFilter {
                    root: args.root,
                    symbol: args.symbol,
                })
                .await?;
            println!("{}", serde_json::to_string_pretty(&rows)?);
        }
        Command::Storage(storage) => match storage.command {
            StorageSubcommand::CleanupTmp { older_than_hours } => {
                progress.step("cleaning disposable tmp staging files");
                let ledger = Ledger::from_env(&cli.data_dir, &cli.r2_prefix).await?;
                let older_than =
                    older_than_hours.map(|hours| std::time::Duration::from_secs(hours * 60 * 60));
                let report = ledger.store.cleanup_tmp(older_than)?;
                println!("{}", serde_json::to_string_pretty(&report)?);
            }
        },
        Command::Session(session) => match session.command {
            SessionSubcommand::Validate(args) => {
                let date = parse_date(&args.market_day.date)?;
                let ledger = Ledger::from_env(&cli.data_dir, &cli.r2_prefix).await?;
                let report = ledger
                    .validate_replay_dataset_with_progress(
                        ValidateReplayDatasetRequest {
                            symbol: args.market_day.symbol,
                            market_date: date,
                            trigger: ValidationTrigger::Manual,
                            skip_book_check: args.skip_book_check,
                            replay_batches: args.replay_batches,
                            replay_all: args.replay_all,
                        },
                        progress.sink(),
                    )
                    .await?;
                println!("{}", serde_json::to_string_pretty(&report)?);
            }
        },
    }

    Ok(())
}

fn parse_date(date: &str) -> Result<NaiveDate> {
    Ok(NaiveDate::parse_from_str(date, "%Y-%m-%d")?)
}

#[derive(Debug, Clone, Copy)]
struct Progress {
    quiet: bool,
}

impl Progress {
    fn new(quiet: bool) -> Self {
        Self { quiet }
    }

    fn step(&self, message: impl AsRef<str>) {
        if !self.quiet {
            eprintln!("[ledger] {}", message.as_ref());
        }
    }

    fn done(&self, message: impl AsRef<str>, started_at: Instant) {
        if !self.quiet {
            eprintln!(
                "[ledger] {} ({:.2}s)",
                message.as_ref(),
                started_at.elapsed().as_secs_f64()
            );
        }
    }

    fn sink(&self) -> Option<LedgerProgressSink> {
        if self.quiet {
            return None;
        }

        Some(Arc::new(|event| match event {
            LedgerProgressEvent::Step { message } => {
                eprintln!("[ledger] {message}");
            }
            LedgerProgressEvent::Done {
                message,
                elapsed_ms,
            } => {
                eprintln!("[ledger] {message} ({:.2}s)", elapsed_ms as f64 / 1000.0);
            }
        }))
    }
}
