//! Terminal adapter for Ledger.
//!
//! This binary owns argument parsing, `.env` loading, service construction, and
//! JSON output. Business logic belongs in the library crates: ingest work in
//! `ledger-ingest`, replay dataset readiness/loading in `ledger`, and
//! persistence in `ledger-store`.

use anyhow::{bail, Context, Result};
use chrono::NaiveDate;
use clap::{Args, Parser, Subcommand};
use ledger::Ledger;
use ledger_book::OrderBook;
use ledger_domain::{EventStore, ExecutionProfile, MarketDay, VisibilityProfile};
use ledger_ingest::BookCheckReport;
use ledger_ingest::{DatabentoProvider, DbnPreprocessor, IngestConfig, IngestPipeline};
use ledger_replay::ReplaySimulator;
use ledger_store::{CachePrunePolicy, MarketDayFilter, R2LedgerStore};
use serde::Serialize;
use std::path::PathBuf;
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
    Session(SessionCommand),
    Cache(CacheCommand),
}

#[derive(Subcommand)]
enum SessionSubcommand {
    Load(MarketDayArgs),
    Validate(ValidateArgs),
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
            let store = R2LedgerStore::from_env(&cli.data_dir, &cli.r2_prefix).await?;
            let pipeline = IngestPipeline::new(
                DatabentoProvider,
                DbnPreprocessor,
                store,
                IngestConfig::default(),
            );
            let started_at = Instant::now();
            let report = pipeline
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
            progress.step("listing cataloged market days");
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
                progress.step(format!(
                    "loading replay dataset {} {}",
                    args.symbol,
                    parse_date(&args.date)?
                ));
                let ledger = Ledger::from_env(&cli.data_dir, &cli.r2_prefix).await?;
                progress.step(
                    "checking catalog and local replay artifacts; large files are hash-verified",
                );
                let started_at = Instant::now();
                let inputs = ledger
                    .load_replay_dataset(&args.symbol, parse_date(&args.date)?)
                    .await?;
                progress.done("replay dataset loaded", started_at);
                println!("{}", serde_json::to_string_pretty(&inputs)?);
            }
            SessionSubcommand::Validate(args) => {
                let report =
                    validate_replay_dataset(&cli.data_dir, &cli.r2_prefix, args, progress).await?;
                println!("{}", serde_json::to_string_pretty(&report)?);
            }
        },
        Command::Cache(cache) => match cache.command {
            CacheSubcommand::Prune(args) => {
                progress.step(format!(
                    "pruning loaded replay datasets to {}",
                    args.max_sessions
                ));
                let store = R2LedgerStore::from_env(&cli.data_dir, &cli.r2_prefix).await?;
                let started_at = Instant::now();
                let report = store.prune_cache(CachePrunePolicy {
                    max_sessions: args.max_sessions,
                })?;
                progress.done("cache prune completed", started_at);
                println!("{}", serde_json::to_string_pretty(&report)?);
            }
        },
    }

    Ok(())
}

fn parse_date(date: &str) -> Result<NaiveDate> {
    Ok(NaiveDate::parse_from_str(date, "%Y-%m-%d")?)
}

#[derive(Debug, Serialize)]
struct ReplayDatasetValidationReport {
    market_day: MarketDay,
    events_path: PathBuf,
    batches_path: PathBuf,
    trades_path: PathBuf,
    book_check_path: PathBuf,
    event_count: usize,
    batch_count: usize,
    trade_count: usize,
    indexes_valid: bool,
    book_check: Option<BookCheckValidationReport>,
    replay_probe: ReplayProbeReport,
}

#[derive(Debug, Serialize)]
struct BookCheckValidationReport {
    matched: bool,
    expected: BookCheckReport,
    actual: BookCheckReport,
}

#[derive(Debug, Serialize)]
struct ReplayProbeReport {
    requested_batches: usize,
    applied_batches: usize,
    total_batches: usize,
    cursor_ts_ns: u64,
    frame_count: usize,
    fill_count: usize,
    final_book_checksum: String,
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
}

async fn validate_replay_dataset(
    data_dir: &PathBuf,
    r2_prefix: &str,
    args: ValidateArgs,
    progress: Progress,
) -> Result<ReplayDatasetValidationReport> {
    let date = parse_date(&args.market_day.date)?;
    progress.step(format!(
        "validating replay dataset {} {}",
        args.market_day.symbol, date
    ));

    let ledger = Ledger::from_env(data_dir, r2_prefix).await?;

    progress.step("checking catalog and local replay artifacts; large files are hash-verified");
    let load_started_at = Instant::now();
    let dataset = ledger
        .load_replay_dataset(&args.market_day.symbol, date)
        .await?;
    progress.done("replay dataset loaded", load_started_at);

    progress.step("decoding and validating event, batch, and trade artifacts");
    let decode_started_at = Instant::now();
    let event_store = dataset.event_store().await?;
    progress.done(
        format!(
            "decoded {} events, {} batches, {} trades",
            event_store.events.len(),
            event_store.batches.len(),
            event_store.trades.len()
        ),
        decode_started_at,
    );

    let event_count = event_store.events.len();
    let batch_count = event_store.batches.len();
    let trade_count = event_store.trades.len();

    let book_check = if args.skip_book_check {
        progress.step("skipping book-check comparison");
        None
    } else {
        Some(validate_book_check(&dataset.book_check_path, &event_store, progress).await?)
    };

    let replay_probe =
        run_replay_probe(event_store, args.replay_batches, args.replay_all, progress)?;

    Ok(ReplayDatasetValidationReport {
        market_day: dataset.market_day,
        events_path: dataset.events_path,
        batches_path: dataset.batches_path,
        trades_path: dataset.trades_path,
        book_check_path: dataset.book_check_path,
        event_count,
        batch_count,
        trade_count,
        indexes_valid: true,
        book_check,
        replay_probe,
    })
}

async fn validate_book_check(
    book_check_path: &PathBuf,
    event_store: &EventStore,
    progress: Progress,
) -> Result<BookCheckValidationReport> {
    progress.step(format!(
        "reading expected book-check report {}",
        book_check_path.display()
    ));
    let expected_bytes = tokio::fs::read(book_check_path)
        .await
        .with_context(|| format!("reading book-check artifact {}", book_check_path.display()))?;
    let expected: BookCheckReport = serde_json::from_slice(&expected_bytes)
        .with_context(|| format!("decoding book-check artifact {}", book_check_path.display()))?;

    progress.step("running deterministic book-check comparison");
    let started_at = Instant::now();
    let actual = run_book_check_with_progress(event_store, progress)?;
    progress.done("book-check comparison completed", started_at);

    if actual != expected {
        bail!("book-check mismatch: expected {expected:?}, actual {actual:?}");
    }

    Ok(BookCheckValidationReport {
        matched: true,
        expected,
        actual,
    })
}

fn run_book_check_with_progress(
    event_store: &EventStore,
    progress: Progress,
) -> Result<BookCheckReport> {
    let mut book = OrderBook::new();
    let mut bbo_change_count = 0;
    let mut warning_count = 0;
    let total_batches = event_store.batches.len();
    let interval = progress_interval(total_batches);

    for (idx, span) in event_store.batches.iter().enumerate() {
        let out = book.apply_batch(event_store.batch_events(*span));
        if out.bbo_changed.is_some() {
            bbo_change_count += 1;
        }
        warning_count += out.warnings.len();

        let applied = idx + 1;
        if should_report_progress(applied, total_batches, interval) {
            progress.step(format!(
                "book-check applied {applied}/{total_batches} batches"
            ));
        }
    }

    Ok(BookCheckReport {
        event_count: event_store.events.len(),
        batch_count: total_batches,
        trade_count: event_store.trades.len(),
        bbo_change_count,
        warning_count,
        final_book_checksum: book.checksum(),
        final_order_count: book.order_count(),
        final_bid_levels: book.level_count(ledger_domain::BookSide::Bid),
        final_ask_levels: book.level_count(ledger_domain::BookSide::Ask),
    })
}

fn run_replay_probe(
    event_store: EventStore,
    replay_batches: Option<usize>,
    replay_all: bool,
    progress: Progress,
) -> Result<ReplayProbeReport> {
    let total_batches = event_store.batches.len();
    let requested_batches = if replay_all {
        total_batches
    } else {
        replay_batches.unwrap_or(1)
    };
    let applied_target = requested_batches.min(total_batches);

    progress.step(format!(
        "stepping replay simulator for {applied_target}/{total_batches} batches"
    ));
    let started_at = Instant::now();
    let interval = progress_interval(applied_target);
    let mut simulator = ReplaySimulator::new(
        event_store,
        ExecutionProfile::default(),
        VisibilityProfile::truth(),
    );

    for idx in 0..applied_target {
        simulator.step_next_exchange_batch()?;
        let applied = idx + 1;
        if should_report_progress(applied, applied_target, interval) {
            progress.step(format!(
                "replay simulator applied {applied}/{applied_target} batches"
            ));
        }
    }

    let report = simulator.report();
    progress.done("replay simulator probe completed", started_at);

    Ok(ReplayProbeReport {
        requested_batches,
        applied_batches: report.batch_idx,
        total_batches,
        cursor_ts_ns: report.cursor_ts_ns,
        frame_count: report.frames.len(),
        fill_count: report.fills.len(),
        final_book_checksum: report.final_book_checksum,
    })
}

fn progress_interval(total: usize) -> usize {
    if total <= 20 {
        1
    } else {
        (total / 20).max(1)
    }
}

fn should_report_progress(applied: usize, total: usize, interval: usize) -> bool {
    total > 0 && (applied == total || applied % interval == 0)
}
