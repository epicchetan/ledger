//! Terminal adapter for Ledger.
//!
//! This binary owns argument parsing, `.env` loading, service construction, and
//! JSON output. Business logic belongs in the library crates: ingest work in
//! `ledger-ingest`, replay dataset readiness/loading in `ledger`, and
//! persistence in `ledger-store`.

use anyhow::Result;
use chrono::NaiveDate;
use clap::{Args, Parser, Subcommand};
use ledger::projection::{base_projection_registry, resolve_projection_graph};
use ledger::{
    Ledger, LedgerProgressEvent, LedgerProgressSink, ProjectionRunRequest, SessionClockRunRequest,
    SessionRunRequest, ValidateReplayDatasetRequest, ValidationTrigger,
};
use ledger_domain::{ProjectionId, ProjectionSpec, ProjectionVersion};
use ledger_store::MarketDayFilter;
use serde_json::Value;
use std::fs::File;
use std::io::{BufWriter, Write};
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
    Replay(ReplayCommand),
    Projection(ProjectionCommand),
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
    Run(SessionRunArgs),
    ClockRun(SessionClockRunArgs),
}

#[derive(Args)]
struct SessionCommand {
    #[command(subcommand)]
    command: SessionSubcommand,
}

#[derive(Subcommand)]
enum ReplaySubcommand {
    CacheStatus(MarketDayArgs),
    CacheRemove(MarketDayArgs),
}

#[derive(Args)]
struct ReplayCommand {
    #[command(subcommand)]
    command: ReplaySubcommand,
}

#[derive(Subcommand)]
enum ProjectionSubcommand {
    List,
    Manifest(ProjectionManifestArgs),
    Graph(ProjectionGraphArgs),
    Run(ProjectionRunArgs),
}

#[derive(Args)]
struct ProjectionCommand {
    #[command(subcommand)]
    command: ProjectionSubcommand,
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

#[derive(Args, Clone)]
struct SessionRunArgs {
    #[command(flatten)]
    market_day: MarketDayArgs,

    #[arg(long, default_value_t = 1)]
    batches: usize,

    #[arg(long)]
    start_ts_ns: Option<u64>,

    #[arg(long)]
    truth_visibility: bool,
}

#[derive(Args, Clone)]
struct SessionClockRunArgs {
    #[command(flatten)]
    market_day: MarketDayArgs,

    #[arg(long)]
    projection: String,

    #[arg(long, default_value = "{}")]
    params: String,

    #[arg(long)]
    speed: f64,

    #[arg(long)]
    tick_ms: u64,

    #[arg(long)]
    ticks: usize,

    #[arg(long, default_value_t = 500)]
    budget_batches: usize,

    #[arg(long)]
    start_ts_ns: Option<u64>,

    #[arg(long)]
    jsonl: Option<PathBuf>,

    #[arg(long)]
    digest: bool,

    #[arg(long)]
    truth_visibility: bool,
}

#[derive(Args, Clone)]
struct ProjectionManifestArgs {
    #[arg(long)]
    id: String,

    #[arg(long)]
    version: u16,
}

#[derive(Args, Clone)]
struct ProjectionGraphArgs {
    #[arg(long)]
    projection: String,

    #[arg(long, default_value = "{}")]
    params: String,
}

#[derive(Args, Clone)]
struct ProjectionRunArgs {
    #[command(flatten)]
    market_day: MarketDayArgs,

    #[arg(long)]
    projection: String,

    #[arg(long, default_value = "{}")]
    params: String,

    #[arg(long)]
    batches: usize,

    #[arg(long)]
    start_ts_ns: Option<u64>,

    #[arg(long)]
    jsonl: Option<PathBuf>,

    #[arg(long)]
    digest: bool,

    #[arg(long)]
    truth_visibility: bool,
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
            SessionSubcommand::Run(args) => {
                let date = parse_date(&args.market_day.date)?;
                progress.step(format!(
                    "running headless Session with replay feed for {} {}",
                    args.market_day.symbol, date
                ));
                let ledger = Ledger::from_env(&cli.data_dir, &cli.r2_prefix).await?;
                let cache = ledger
                    .replay_cache_status(&args.market_day.symbol, date)
                    .await?;
                if cache.cached {
                    progress.step("replay cache hit; using local ReplayDataset artifacts");
                } else {
                    progress.step("replay cache miss; hydrating ReplayDataset artifacts from R2");
                }
                let started_at = Instant::now();
                let report = ledger
                    .run_session(SessionRunRequest {
                        symbol: args.market_day.symbol,
                        market_date: date,
                        start_ts_ns: args.start_ts_ns,
                        batches: args.batches,
                        truth_visibility: args.truth_visibility,
                    })
                    .await?;
                progress.done("headless Session run completed", started_at);
                println!("{}", serde_json::to_string_pretty(&report)?);
            }
            SessionSubcommand::ClockRun(args) => {
                let date = parse_date(&args.market_day.date)?;
                let spec = parse_projection_arg(&args.projection, &args.params)?;
                progress.step(format!(
                    "running deterministic Session clock for {} over {} {}",
                    spec.id, args.market_day.symbol, date
                ));
                let ledger = Ledger::from_env(&cli.data_dir, &cli.r2_prefix).await?;
                let cache = ledger
                    .replay_cache_status(&args.market_day.symbol, date)
                    .await?;
                if cache.cached {
                    progress.step("replay cache hit; using local ReplayDataset artifacts");
                } else {
                    progress.step("replay cache miss; hydrating ReplayDataset artifacts from R2");
                }
                let started_at = Instant::now();
                let report = ledger
                    .run_session_clock(SessionClockRunRequest {
                        symbol: args.market_day.symbol,
                        market_date: date,
                        start_ts_ns: args.start_ts_ns,
                        projection: spec,
                        speed: args.speed,
                        tick_ms: args.tick_ms,
                        ticks: args.ticks,
                        budget_batches: args.budget_batches,
                        digest: args.digest,
                        truth_visibility: args.truth_visibility,
                    })
                    .await?;
                if let Some(path) = args.jsonl {
                    write_projection_jsonl(&path, &report.frames)?;
                    progress.step(format!("wrote projection frames to {}", path.display()));
                }
                progress.done("deterministic Session clock run completed", started_at);
                println!("{}", serde_json::to_string_pretty(&report)?);
            }
        },
        Command::Replay(replay) => match replay.command {
            ReplaySubcommand::CacheStatus(args) => {
                let date = parse_date(&args.date)?;
                progress.step(format!(
                    "checking replay cache for {} {}",
                    args.symbol, date
                ));
                let ledger = Ledger::from_env(&cli.data_dir, &cli.r2_prefix).await?;
                let status = ledger.replay_cache_status(&args.symbol, date).await?;
                println!("{}", serde_json::to_string_pretty(&status)?);
            }
            ReplaySubcommand::CacheRemove(args) => {
                let date = parse_date(&args.date)?;
                progress.step(format!(
                    "removing replay cache for {} {}",
                    args.symbol, date
                ));
                let ledger = Ledger::from_env(&cli.data_dir, &cli.r2_prefix).await?;
                let report = ledger
                    .delete_replay_dataset_cache(&args.symbol, date)
                    .await?;
                println!("{}", serde_json::to_string_pretty(&report)?);
            }
        },
        Command::Projection(projection) => match projection.command {
            ProjectionSubcommand::List => {
                let output = projection_list_output()?;
                println!("{}", serde_json::to_string_pretty(&output)?);
            }
            ProjectionSubcommand::Manifest(args) => {
                let output = projection_manifest_output(&args.id, args.version)?;
                println!("{}", serde_json::to_string_pretty(&output)?);
            }
            ProjectionSubcommand::Graph(args) => {
                let spec = parse_projection_arg(&args.projection, &args.params)?;
                let output = projection_graph_output(spec)?;
                println!("{}", serde_json::to_string_pretty(&output)?);
            }
            ProjectionSubcommand::Run(args) => {
                let date = parse_date(&args.market_day.date)?;
                let spec = parse_projection_arg(&args.projection, &args.params)?;
                progress.step(format!(
                    "running projection {} over {} {}",
                    spec.id, args.market_day.symbol, date
                ));
                let ledger = Ledger::from_env(&cli.data_dir, &cli.r2_prefix).await?;
                let cache = ledger
                    .replay_cache_status(&args.market_day.symbol, date)
                    .await?;
                if cache.cached {
                    progress.step("replay cache hit; using local ReplayDataset artifacts");
                } else {
                    progress.step("replay cache miss; hydrating ReplayDataset artifacts from R2");
                }
                let started_at = Instant::now();
                let report = ledger
                    .run_projection(ProjectionRunRequest {
                        symbol: args.market_day.symbol,
                        market_date: date,
                        start_ts_ns: args.start_ts_ns,
                        projection: spec,
                        batches: args.batches,
                        digest: args.digest,
                        truth_visibility: args.truth_visibility,
                    })
                    .await?;
                if let Some(path) = args.jsonl {
                    write_projection_jsonl(&path, &report.frames)?;
                    progress.step(format!("wrote projection frames to {}", path.display()));
                }
                progress.done("projection run completed", started_at);
                println!("{}", serde_json::to_string_pretty(&report)?);
            }
        },
    }

    Ok(())
}

fn parse_date(date: &str) -> Result<NaiveDate> {
    Ok(NaiveDate::parse_from_str(date, "%Y-%m-%d")?)
}

fn parse_projection_arg(projection: &str, params: &str) -> Result<ProjectionSpec> {
    let (id, version) = parse_projection_ref(projection)?;
    let params = serde_json::from_str::<Value>(params)?;
    ProjectionSpec::new(id, version, params)
}

fn parse_projection_ref(projection: &str) -> Result<(String, u16)> {
    let (id, version) = projection
        .rsplit_once(":v")
        .ok_or_else(|| anyhow::anyhow!("projection must use `<id>:v<version>` format"))?;
    Ok((id.to_string(), version.parse::<u16>()?))
}

fn projection_list_output() -> Result<Vec<ledger_domain::ProjectionManifest>> {
    Ok(base_projection_registry()?.list_manifests())
}

fn projection_manifest_output(id: &str, version: u16) -> Result<ledger_domain::ProjectionManifest> {
    let registry = base_projection_registry()?;
    Ok(registry
        .manifest(&ProjectionId::new(id)?, ProjectionVersion::new(version)?)?
        .clone())
}

fn projection_graph_output(spec: ProjectionSpec) -> Result<ledger::projection::ProjectionGraph> {
    let registry = base_projection_registry()?;
    resolve_projection_graph(&registry, spec)
}

fn write_projection_jsonl(path: &PathBuf, frames: &[ledger_domain::ProjectionFrame]) -> Result<()> {
    let file = File::create(path)?;
    let mut writer = BufWriter::new(file);
    for frame in frames {
        serde_json::to_writer(&mut writer, frame)?;
        writer.write_all(b"\n")?;
    }
    writer.flush()?;
    Ok(())
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

#[cfg(test)]
mod tests {
    use super::*;
    use ledger::projection::{BARS_ID, BBO_ID, CANONICAL_TRADES_ID, CURSOR_ID};

    #[test]
    fn projection_cli_list_returns_base_manifests() {
        let manifests = projection_list_output().unwrap();
        let ids = manifests
            .iter()
            .map(|manifest| manifest.id.as_str().to_string())
            .collect::<Vec<_>>();

        assert_eq!(ids, vec![CURSOR_ID, BBO_ID, CANONICAL_TRADES_ID, BARS_ID]);
    }

    #[test]
    fn projection_cli_manifest_returns_requested_manifest() {
        let manifest = projection_manifest_output(BBO_ID, 1).unwrap();

        assert_eq!(manifest.id.as_str(), BBO_ID);
        assert_eq!(manifest.version.get(), 1);
        assert_eq!(manifest.output_schema.name.as_str(), "bbo_v1");
    }

    #[test]
    fn projection_cli_graph_resolves_bars_dependency() {
        let spec = parse_projection_arg("bars:v1", r#"{"seconds":60}"#).unwrap();
        let graph = projection_graph_output(spec).unwrap();

        assert!(!graph.cycle);
        assert_eq!(graph.root.id.as_str(), BARS_ID);
        assert_eq!(graph.edges.len(), 1);
        assert_eq!(graph.edges[0].from.id.as_str(), CANONICAL_TRADES_ID);
        assert_eq!(graph.edges[0].to.id.as_str(), BARS_ID);
    }
}
