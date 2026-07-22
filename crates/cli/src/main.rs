//! Terminal adapter for Ledger store.
//!
//! This binary owns argument parsing, `.env` loading, service construction, and
//! JSON output. Object registry, R2, hydration, and local object behavior belong in
//! the `store` crate.

use anyhow::{anyhow, Result};
use clap::{Args, Parser, Subcommand};
use ledger::feed::es_replay;
use ledger::feed::es_replay::{
    es_day_catalog, fetch_es_raw, find_es_replay_artifact_descriptor, prepare_es_replay_artifact,
    EsMboFeedBatch, EsPrepareSummary, EsReplayCells, EsReplayCursor, EsReplayStatus, FetchProgress,
    PrepareProgress, ES_MBO_EVENT_STORE_KIND, RAW_DATABENTO_DBN_ZST_KIND,
};
use ledger::market::MarketDay;
use ledger::projection::{
    Bar, BarsCells, BarsStatus, ProjectionDeliveryEvent, ProjectionDemand, ProjectionSpec,
    ProjectionSubscriptionProjectionRequest, ProjectionSubscriptionRequest,
};
use ledger::session::{LedgerSessionBuilder, LedgerSessionHandle};
use serde::Serialize;
use serde_json::{json, Value};
use std::collections::HashSet;
use std::path::PathBuf;
use std::sync::{
    atomic::{AtomicU64, Ordering},
    Arc,
};
use std::time::Duration;
use store::{ObjectFilter, R2Store, RegisterFileRequest, StoreObjectId, StoreObjectRole};
use tokio::sync::mpsc;

const SESSION_READY_TIMEOUT: Duration = Duration::from_secs(60);
const SESSION_STEP_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Parser)]
#[command(name = "ledger")]
#[command(about = "Ledger store object registry")]
struct Cli {
    #[arg(long, env = "LEDGER_DATA_DIR", default_value = "data")]
    data_dir: PathBuf,

    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    Store(StoreCommand),
    Es(EsCommand),
    Session(SessionCommand),
}

#[derive(Args)]
struct StoreCommand {
    #[command(subcommand)]
    command: StoreSubcommand,
}

#[derive(Subcommand)]
enum StoreSubcommand {
    List(StoreListArgs),
    Show(StoreShowArgs),
    ImportFile(StoreImportFileArgs),
    Hydrate(StoreShowArgs),
    Offload(StoreShowArgs),
    Delete(StoreDeleteArgs),
    LocalStatus,
    LocalPrune,
    Sync(StoreSyncArgs),
    Validate(StoreValidateArgs),
    AbortIncompleteUploads(StoreAbortUploadsArgs),
}

#[derive(Args)]
struct EsCommand {
    #[command(subcommand)]
    command: EsSubcommand,
}

#[derive(Subcommand)]
enum EsSubcommand {
    Days,
    Prepare(EsPrepareArgs),
    Fetch(EsFetchArgs),
}

#[derive(Args)]
struct SessionCommand {
    #[command(subcommand)]
    command: SessionSubcommand,
}

#[derive(Subcommand)]
enum SessionSubcommand {
    RunEsReplay(SessionRunEsReplayArgs),
}

#[derive(Args, Clone)]
struct SessionRunEsReplayArgs {
    #[arg(long)]
    raw_id: String,
    #[arg(long)]
    batches: Option<usize>,
    #[arg(long)]
    realtime: bool,
    #[arg(long, default_value_t = 1.0)]
    speed: f64,
    /// Projections to compute over the feed, e.g. `bars:1m`. Repeatable.
    #[arg(long = "projection")]
    projections: Vec<String>,
    /// Enable the projection delivery subscriber at this FPS for profiling.
    #[arg(long)]
    delivery_fps: Option<u16>,
}

#[derive(Args, Clone)]
struct EsPrepareArgs {
    #[arg(long)]
    raw_id: Option<String>,
    #[arg(long)]
    day: Option<String>,
    #[arg(long)]
    all: bool,
    #[arg(long)]
    force: bool,
}

#[derive(Args, Clone)]
struct EsFetchArgs {
    #[arg(long)]
    day: String,
    #[arg(long)]
    symbol: String,
    #[arg(long, default_value = "GLBX.MDP3")]
    dataset: String,
    #[arg(long)]
    force: bool,
}

#[derive(Args, Clone)]
struct StoreListArgs {
    #[arg(long)]
    role: Option<String>,
    #[arg(long)]
    kind: Option<String>,
    #[arg(long)]
    id_prefix: Option<String>,
}

#[derive(Args, Clone)]
struct StoreShowArgs {
    #[arg(long)]
    id: String,
}

#[derive(Args, Clone)]
struct StoreDeleteArgs {
    #[arg(long)]
    id: String,
    /// Raw objects are paid source data and refuse deletion by default.
    #[arg(long)]
    force_raw: bool,
}

#[derive(Args, Clone)]
struct StoreImportFileArgs {
    #[arg(long)]
    path: PathBuf,
    #[arg(long)]
    role: String,
    #[arg(long)]
    kind: String,
    #[arg(long)]
    file_name: Option<String>,
    #[arg(long)]
    format: Option<String>,
    #[arg(long)]
    media_type: Option<String>,
    #[arg(long, default_value = "{}")]
    metadata_json: String,
}

#[derive(Args, Clone)]
struct StoreSyncArgs {
    #[arg(long)]
    overwrite: bool,
    #[arg(long)]
    dry_run: bool,
}

#[derive(Args, Clone)]
struct StoreAbortUploadsArgs {
    /// Only consider uploads whose key starts with this prefix.
    #[arg(long, default_value = "store/objects")]
    prefix: String,
    /// Actually abort. Without this the command only lists what it would abort.
    #[arg(long)]
    execute: bool,
}

#[derive(Args, Clone)]
struct StoreValidateArgs {
    #[arg(long)]
    id: Option<String>,
    #[arg(long)]
    role: Option<String>,
    #[arg(long)]
    kind: Option<String>,
    #[arg(long)]
    verify_remote: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    dotenvy::dotenv().ok();
    let cli = Cli::parse();

    match cli.command {
        Command::Store(command) => {
            run_store_command(R2Store::from_env(&cli.data_dir).await?, command).await?
        }
        Command::Es(command) => {
            run_es_command(
                R2Store::from_env(&cli.data_dir).await?,
                cli.data_dir,
                command,
            )
            .await?
        }
        Command::Session(command) => {
            let projection_specs = parse_session_projection_specs(&command)?;
            run_session_command(
                R2Store::from_env(&cli.data_dir).await?,
                command,
                projection_specs,
            )
            .await?
        }
    }

    Ok(())
}

async fn run_session_command(
    ledger_store: R2Store,
    command: SessionCommand,
    projection_specs: Vec<(String, ProjectionSpec)>,
) -> Result<()> {
    match command.command {
        SessionSubcommand::RunEsReplay(args) => {
            run_session_es_replay_command(ledger_store, args, projection_specs).await?;
        }
    }
    Ok(())
}

async fn run_session_es_replay_command(
    ledger_store: R2Store,
    args: SessionRunEsReplayArgs,
    projection_specs: Vec<(String, ProjectionSpec)>,
) -> Result<()> {
    let raw_id = StoreObjectId::new(args.raw_id.clone())?;
    let preexisting_artifact_id = existing_es_replay_artifact_id(&ledger_store, &raw_id)?;
    let store = Arc::new(ledger_store);
    let mut builder = LedgerSessionBuilder::new(store)?;
    let cells = builder.es_replay(raw_id.clone())?;
    let requested = projection_specs
        .iter()
        .map(|(_, spec)| spec.clone())
        .collect::<Vec<_>>();
    let projection_cells = builder
        .projections(&cells, &requested)?
        .into_iter()
        .map(|output| output.into_bars())
        .collect::<Vec<_>>();
    let session = builder.start().await?;
    let mut cursor_watch = session.cache().watch_key(cells.cursor.key())?;

    let mut cursor = match tokio::time::timeout(
        SESSION_READY_TIMEOUT,
        wait_for_cursor_value(session.cache(), &mut cursor_watch, &cells.cursor),
    )
    .await
    {
        Ok(result) => result?,
        Err(_) => {
            let feed_status = session
                .runtime()
                .component_status(&es_replay::es_replay_component_id())
                .await
                .map(|status| format!("{status:?}"))
                .unwrap_or_else(|err| format!("status unavailable: {err}"));
            let _ = session.shutdown().await;
            return Err(anyhow!(
                "timed out waiting for ES replay cursor readiness; component_status={feed_status}"
            ));
        }
    };
    let delivery_probe =
        start_delivery_probe(&session, &projection_specs, args.delivery_fps).await?;

    let mode = if args.realtime { "realtime" } else { "step" };
    let start_feed_seq = cursor.feed_seq;

    if args.realtime {
        if !requested_reached(&cursor, start_feed_seq, args.batches) && !cursor.ended {
            if let Some(first_ts) = cursor.next_ts_event_ns {
                session.seek_to(first_ts).await?;
                session.set_speed(args.speed).await?;
                session.play().await?;
                let _ = wait_until_requested_or_ended(
                    session.cache(),
                    &mut cursor_watch,
                    &cells.cursor,
                    start_feed_seq,
                    args.batches,
                )
                .await?;
                session.pause().await?;
            }
        }
    } else {
        while !requested_reached(&cursor, start_feed_seq, args.batches) && !cursor.ended {
            let Some(target) = cursor.next_ts_event_ns else {
                break;
            };
            let last_seen = cursor.feed_seq;
            session.seek_to(target).await?;
            cursor = tokio::time::timeout(
                SESSION_STEP_TIMEOUT,
                wait_for_cursor_progress(
                    session.cache(),
                    &mut cursor_watch,
                    &cells.cursor,
                    last_seen,
                ),
            )
            .await
            .map_err(|_| anyhow!("timed out waiting for ES replay cursor progress"))??;
        }
    }

    if let Err(error) =
        wait_for_projection_catch_up_all(session.runtime(), &projection_cells, &cells.cursor).await
    {
        let _ = session.shutdown().await;
        return Err(error);
    }
    if delivery_probe.is_some() {
        tokio::time::sleep(Duration::from_millis(100)).await;
    }

    let delivery_summary = delivery_probe
        .as_ref()
        .and_then(|probe| probe.summary(&session));

    let summary = session_summary(
        &cells,
        session.runtime(),
        &raw_id,
        mode,
        args.batches,
        start_feed_seq,
        preexisting_artifact_id.as_deref(),
        &projection_cells,
        delivery_summary,
    )
    .await?;
    if let Some(probe) = delivery_probe {
        probe.join.abort();
    }
    session.shutdown().await?;
    print_json(&summary)?;
    Ok(())
}

async fn run_es_command(
    ledger_store: R2Store,
    data_dir: PathBuf,
    command: EsCommand,
) -> Result<()> {
    match command.command {
        EsSubcommand::Days => {
            let catalog = es_day_catalog(&ledger_store)?;
            print_json(&catalog)?;
        }
        EsSubcommand::Prepare(args) => {
            run_es_prepare_command(ledger_store, args).await?;
        }
        EsSubcommand::Fetch(args) => {
            let day = MarketDay::parse(&args.day)?;
            let progress = fetch_progress_channel();
            let staging_dir = data_dir
                .join("tmp")
                .join("fetch")
                .join(&args.symbol)
                .join(day.to_string());
            let fetch = fetch_es_raw(
                &ledger_store,
                day,
                &args.symbol,
                &args.dataset,
                &staging_dir,
                args.force,
                Some(progress),
            )
            .await?;
            // Fetch chains straight into prepare so the day lands replay-ready.
            // Raw ids are content-addressed, so force never propagates: an
            // existing valid artifact is reused, anything else is rebuilt.
            let raw_id = StoreObjectId::new(fetch.raw_object_id.clone())?;
            let prepare = prepare_one(&ledger_store, raw_id, false).await?;
            print_json(&json!({ "fetch": fetch, "prepare": prepare }))?;
        }
    }
    Ok(())
}

async fn run_es_prepare_command(ledger_store: R2Store, args: EsPrepareArgs) -> Result<()> {
    let selector_count = usize::from(args.raw_id.is_some())
        + usize::from(args.day.is_some())
        + usize::from(args.all);
    if selector_count != 1 {
        return Err(anyhow!(
            "exactly one of --raw-id, --day, or --all is required"
        ));
    }

    if let Some(raw_id) = args.raw_id {
        let summary = prepare_one(&ledger_store, StoreObjectId::new(raw_id)?, args.force).await?;
        print_json(&summary)?;
        return Ok(());
    }

    let raw_ids = if let Some(day) = args.day {
        let day = MarketDay::parse(&day)?;
        let catalog = es_day_catalog(&ledger_store)?;
        let entry = catalog
            .days
            .into_iter()
            .find(|entry| entry.market_day == day)
            .ok_or_else(|| anyhow!("unknown ES market day {day}"))?;
        entry
            .raws
            .into_iter()
            .map(|status| status.raw.id)
            .collect::<Vec<_>>()
    } else {
        ledger_store
            .list_objects(ObjectFilter {
                role: Some(StoreObjectRole::Raw),
                kind: Some(RAW_DATABENTO_DBN_ZST_KIND.to_string()),
                id_prefix: None,
            })?
            .into_iter()
            .map(|descriptor| descriptor.id)
            .collect::<Vec<_>>()
    };

    let mut aggregate = EsPrepareAggregate::default();
    for raw_id in raw_ids {
        match prepare_one(&ledger_store, raw_id.clone(), args.force).await {
            Ok(summary) => aggregate.ok.push(summary),
            Err(error) => aggregate.failed.push(EsPrepareFailure {
                raw_object_id: raw_id.to_string(),
                error: error.to_string(),
            }),
        }
    }
    let failed = !aggregate.failed.is_empty();
    print_json(&aggregate)?;
    if failed {
        std::process::exit(1);
    }
    Ok(())
}

async fn prepare_one(
    ledger_store: &R2Store,
    raw_id: StoreObjectId,
    force: bool,
) -> Result<EsPrepareSummary> {
    let progress = prepare_progress_channel();
    let artifact = prepare_es_replay_artifact(ledger_store, &raw_id, force, Some(progress)).await?;
    Ok(artifact.summary(&raw_id))
}

fn existing_es_replay_artifact_id(
    ledger_store: &R2Store,
    raw_id: &StoreObjectId,
) -> Result<Option<String>> {
    let artifacts = ledger_store.list_objects(ObjectFilter {
        role: Some(StoreObjectRole::Artifact),
        kind: Some(ES_MBO_EVENT_STORE_KIND.to_string()),
        id_prefix: None,
    })?;
    Ok(find_es_replay_artifact_descriptor(&artifacts, raw_id)
        .descriptor
        .map(|descriptor| descriptor.id.to_string()))
}

fn parse_session_projection_specs(
    command: &SessionCommand,
) -> Result<Vec<(String, ProjectionSpec)>> {
    match &command.command {
        SessionSubcommand::RunEsReplay(args) => parse_projection_specs(&args.projections),
    }
}

fn parse_projection_specs(raw_specs: &[String]) -> Result<Vec<(String, ProjectionSpec)>> {
    let mut seen = HashSet::new();
    let mut parsed = Vec::new();
    for raw in raw_specs {
        let spec = ProjectionSpec::parse(raw)?;
        let canonical = spec.canonical();
        if !seen.insert(canonical.clone()) {
            return Err(anyhow!("duplicate projection spec `{canonical}`"));
        }
        parsed.push((canonical, spec));
    }
    Ok(parsed)
}

async fn wait_for_cursor_value(
    cache: &cache::CacheReader,
    watch: &mut cache::CellWatch,
    key: &cache::ValueKey<EsReplayCursor>,
) -> Result<EsReplayCursor> {
    loop {
        if let Some(cursor) = cache.read_value(key)? {
            return Ok(cursor);
        }
        watch.changed().await?;
    }
}

async fn wait_for_cursor_progress(
    cache: &cache::CacheReader,
    watch: &mut cache::CellWatch,
    key: &cache::ValueKey<EsReplayCursor>,
    last_seen: u64,
) -> Result<EsReplayCursor> {
    loop {
        if let Some(cursor) = cache.read_value(key)? {
            if cursor.feed_seq > last_seen || cursor.ended {
                return Ok(cursor);
            }
        }
        watch.changed().await?;
    }
}

async fn wait_until_requested_or_ended(
    cache: &cache::CacheReader,
    watch: &mut cache::CellWatch,
    key: &cache::ValueKey<EsReplayCursor>,
    start_feed_seq: u64,
    requested: Option<usize>,
) -> Result<EsReplayCursor> {
    loop {
        let cursor = wait_for_cursor_value(cache, watch, key).await?;
        if requested_reached(&cursor, start_feed_seq, requested) || cursor.ended {
            return Ok(cursor);
        }
        watch.changed().await?;
    }
}

async fn wait_for_projection_catch_up_all(
    runtime: &runtime::RuntimeHandle,
    projections: &[(String, BarsCells)],
    cursor_key: &cache::ValueKey<EsReplayCursor>,
) -> Result<()> {
    for (spec, cells) in projections {
        let mut watch = runtime.cache().watch_key(cells.status.key())?;
        match tokio::time::timeout(
            SESSION_STEP_TIMEOUT,
            wait_for_projection_catch_up(runtime, &mut watch, &cells.status, cursor_key),
        )
        .await
        {
            Ok(result) => {
                result?;
            }
            Err(_) => {
                let processed = runtime
                    .cache()
                    .read_value(&cells.status)?
                    .map(|status| status.processed_batches.to_string())
                    .unwrap_or_else(|| "unpublished".to_string());
                let batch_idx = runtime
                    .cache()
                    .read_value(cursor_key)?
                    .map(|cursor| cursor.batch_idx.to_string())
                    .unwrap_or_else(|| "unpublished".to_string());
                return Err(anyhow!(
                    "timed out waiting for projection `{spec}` catch-up: status.processed_batches={processed} cursor.batch_idx={batch_idx}"
                ));
            }
        }
    }
    Ok(())
}

async fn wait_for_projection_catch_up(
    runtime: &runtime::RuntimeHandle,
    watch: &mut cache::CellWatch,
    key: &cache::ValueKey<BarsStatus>,
    cursor_key: &cache::ValueKey<EsReplayCursor>,
) -> Result<BarsStatus> {
    loop {
        let cursor_key = cursor_key.clone();
        let status_key = key.clone();
        let state = runtime
            .snapshot(move |view| {
                Ok((view.read_value(&cursor_key)?, view.read_value(&status_key)?))
            })
            .await?;
        if let (Some(cursor), Some(status)) = state {
            if status.epoch == cursor.epoch && status.processed_batches == cursor.batch_idx {
                return Ok(status);
            }
        }
        watch.changed().await?;
    }
}

fn requested_reached(
    cursor: &EsReplayCursor,
    start_feed_seq: u64,
    requested: Option<usize>,
) -> bool {
    requested
        .map(|requested| cursor.feed_seq.saturating_sub(start_feed_seq) >= requested as u64)
        .unwrap_or(false)
}

#[allow(clippy::too_many_arguments)]
async fn session_summary(
    cells: &EsReplayCells,
    runtime: &runtime::RuntimeHandle,
    raw_id: &StoreObjectId,
    mode: &str,
    batches_requested: Option<usize>,
    start_feed_seq: u64,
    preexisting_artifact_id: Option<&str>,
    projections: &[(String, BarsCells)],
    projection_delivery: Option<SessionDeliverySummary>,
) -> Result<SessionRunEsReplaySummary> {
    let feed = cells.clone();
    let projection_keys = projections.to_vec();
    let snapshot = runtime
        .snapshot(move |view| {
            let cursor = view.read_value(&feed.cursor)?;
            let status = view.read_value(&feed.status)?;
            let (first_batch, last_batch) = match cursor.as_ref().map(|cursor| cursor.batch_idx) {
                Some(0) | None => (None, None),
                Some(batch_count) => (
                    view.read_array_range(&feed.batches, 0..1)?
                        .into_iter()
                        .next(),
                    view.read_array_range(&feed.batches, batch_count - 1..batch_count)?
                        .into_iter()
                        .next(),
                ),
            };
            let mut projection_state = Vec::with_capacity(projection_keys.len());
            for (spec, cells) in projection_keys {
                projection_state.push((
                    spec,
                    view.read_array(&cells.bars)?,
                    view.read_value(&cells.live)?,
                ));
            }
            Ok(SessionSummarySnapshot {
                cursor,
                status,
                first_batch,
                last_batch,
                projections: projection_state,
            })
        })
        .await?;
    let cursor = snapshot
        .cursor
        .ok_or_else(|| anyhow!("ES replay cursor was not published"))?;
    let status = snapshot
        .status
        .ok_or_else(|| anyhow!("ES replay status was not published"))?;
    let artifact_reused = status
        .artifact_object_id
        .as_deref()
        .zip(preexisting_artifact_id)
        .map(|(actual, preexisting)| actual == preexisting)
        .unwrap_or(false);
    Ok(SessionRunEsReplaySummary {
        raw_object_id: raw_id.to_string(),
        artifact_object_id: status.artifact_object_id,
        artifact_reused,
        mode: mode.to_string(),
        batches_requested,
        batches_emitted: cursor.feed_seq.saturating_sub(start_feed_seq),
        first_ts_event_ns: snapshot
            .first_batch
            .map(|batch| batch.ts_event_ns.to_string()),
        last_ts_event_ns: snapshot
            .last_batch
            .map(|batch| batch.ts_event_ns.to_string()),
        ended: cursor.ended,
        epoch: cursor.epoch,
        feed_seq: cursor.feed_seq,
        projections: projection_summaries(snapshot.projections),
        projection_delivery,
    })
}

fn projection_summaries(
    projections: Vec<(String, Vec<Bar>, Option<Bar>)>,
) -> Vec<SessionProjectionSummary> {
    let mut summaries = Vec::new();
    for (spec, bars, live) in projections {
        let completed_volume = bars.iter().map(|bar| bar.volume).sum::<u64>();
        let completed_trade_count = bars.iter().map(|bar| bar.trade_count).sum::<u64>();
        let volume = completed_volume + live.as_ref().map(|bar| bar.volume).unwrap_or(0);
        let trade_count =
            completed_trade_count + live.as_ref().map(|bar| bar.trade_count).unwrap_or(0);
        let first_bar_start_ns = bars
            .first()
            .map(|bar| bar.interval_start_ns)
            .or_else(|| live.as_ref().map(|bar| bar.interval_start_ns))
            .map(|value| value.to_string());
        let last_bar_start_ns = live
            .as_ref()
            .map(|bar| bar.interval_start_ns)
            .or_else(|| bars.last().map(|bar| bar.interval_start_ns))
            .map(|value| value.to_string());

        summaries.push(SessionProjectionSummary {
            spec,
            completed_bars: bars.len(),
            live_bar: live.is_some(),
            volume,
            trade_count,
            first_bar_start_ns,
            last_bar_start_ns,
        });
    }
    summaries
}

struct SessionSummarySnapshot {
    cursor: Option<EsReplayCursor>,
    status: Option<EsReplayStatus>,
    first_batch: Option<EsMboFeedBatch>,
    last_batch: Option<EsMboFeedBatch>,
    projections: Vec<(String, Vec<Bar>, Option<Bar>)>,
}

#[derive(Debug, Serialize)]
struct SessionRunEsReplaySummary {
    raw_object_id: String,
    artifact_object_id: Option<String>,
    artifact_reused: bool,
    mode: String,
    batches_requested: Option<usize>,
    batches_emitted: u64,
    first_ts_event_ns: Option<String>,
    last_ts_event_ns: Option<String>,
    ended: bool,
    epoch: u64,
    feed_seq: u64,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    projections: Vec<SessionProjectionSummary>,
    #[serde(skip_serializing_if = "Option::is_none")]
    projection_delivery: Option<SessionDeliverySummary>,
}

#[derive(Debug, Serialize)]
struct SessionDeliverySummary {
    events_received: u64,
    dirty_notifications: u64,
    coalesced_dirty_notifications: u64,
    atomic_collects: u64,
    frames_admitted: u64,
    snapshot_frames: u64,
    suffix_frames: u64,
    outbound_backpressure: u64,
}

struct DeliveryProbe {
    events_received: Arc<AtomicU64>,
    join: tokio::task::JoinHandle<()>,
}

impl DeliveryProbe {
    fn summary(&self, session: &LedgerSessionHandle) -> Option<SessionDeliverySummary> {
        let metrics = session.projection_delivery()?.metrics();
        Some(SessionDeliverySummary {
            events_received: self.events_received.load(Ordering::Relaxed),
            dirty_notifications: metrics.dirty_notifications,
            coalesced_dirty_notifications: metrics.coalesced_dirty_notifications,
            atomic_collects: metrics.atomic_collects,
            frames_admitted: metrics.frames_admitted,
            snapshot_frames: metrics.snapshot_frames,
            suffix_frames: metrics.suffix_frames,
            outbound_backpressure: metrics.outbound_backpressure,
        })
    }
}

async fn start_delivery_probe(
    session: &LedgerSessionHandle,
    projection_specs: &[(String, ProjectionSpec)],
    fps: Option<u16>,
) -> Result<Option<DeliveryProbe>> {
    let Some(fps) = fps else {
        return Ok(None);
    };
    if fps == 0 {
        return Err(anyhow!("--delivery-fps must be greater than zero"));
    }
    let delivery = session
        .projection_delivery()
        .ok_or_else(|| anyhow!("--delivery-fps requires at least one projection"))?
        .clone();
    let mut events = session
        .take_projection_events()
        .ok_or_else(|| anyhow!("projection delivery event stream unavailable"))?;
    let subscription = delivery
        .subscribe(ProjectionSubscriptionRequest {
            consumer_instance_id: "ledger-cli-profile".to_string(),
            projections: projection_specs
                .iter()
                .map(|(spec, _)| ProjectionSubscriptionProjectionRequest {
                    spec: spec.clone(),
                    schema_versions: vec![1],
                    requested_max_fps: Some(fps),
                    have: None,
                })
                .collect(),
        })
        .await?;
    let subscription_id = subscription.subscription_id;
    let events_received = Arc::new(AtomicU64::new(0));
    let received = events_received.clone();
    let join = tokio::spawn(async move {
        let mut lease = tokio::time::interval(Duration::from_secs(10));
        loop {
            tokio::select! {
                event = events.recv() => {
                    match event {
                        Some(ProjectionDeliveryEvent::Frame(_)) => {
                            received.fetch_add(1, Ordering::Relaxed);
                        }
                        Some(ProjectionDeliveryEvent::Watermark(_))
                        | Some(ProjectionDeliveryEvent::SubscriptionExpired { .. }) => {}
                        None => return,
                    }
                }
                _ = lease.tick() => {
                    if delivery
                        .demand(
                            subscription_id.clone(),
                            ProjectionDemand {
                                active: true,
                                requested_max_fps: Some(fps),
                            },
                        )
                        .await
                        .is_err()
                    {
                        return;
                    }
                }
            }
        }
    });
    Ok(Some(DeliveryProbe {
        events_received,
        join,
    }))
}

#[derive(Debug, Serialize)]
struct SessionProjectionSummary {
    spec: String,
    completed_bars: usize,
    live_bar: bool,
    /// Totals across completed bars plus the live bar.
    volume: u64,
    trade_count: u64,
    /// interval_start_ns of the first completed bar (or live bar if none
    /// completed), u64-as-string like the other ns fields.
    first_bar_start_ns: Option<String>,
    /// interval_start_ns of the live bar if present, else the last
    /// completed bar.
    last_bar_start_ns: Option<String>,
}

#[derive(Debug, Default, Serialize)]
struct EsPrepareAggregate {
    ok: Vec<EsPrepareSummary>,
    failed: Vec<EsPrepareFailure>,
}

#[derive(Debug, Serialize)]
struct EsPrepareFailure {
    raw_object_id: String,
    error: String,
}

fn prepare_progress_channel() -> mpsc::UnboundedSender<PrepareProgress> {
    let (tx, mut rx) = mpsc::unbounded_channel();
    tokio::spawn(async move {
        while let Some(progress) = rx.recv().await {
            match progress {
                PrepareProgress::Hydrating => eprintln!("prepare hydrating"),
                PrepareProgress::Decoding { records } => {
                    eprintln!("prepare decoding records={records}")
                }
                PrepareProgress::Encoding { events, batches } => {
                    eprintln!("prepare encoding events={events} batches={batches}")
                }
                PrepareProgress::Registering => eprintln!("prepare registering"),
            }
        }
    });
    tx
}

fn fetch_progress_channel() -> mpsc::UnboundedSender<FetchProgress> {
    let (tx, mut rx) = mpsc::unbounded_channel();
    tokio::spawn(async move {
        while let Some(progress) = rx.recv().await {
            match progress {
                FetchProgress::Requesting => eprintln!("fetch requesting"),
                FetchProgress::Downloading => eprintln!("fetch downloading"),
                FetchProgress::Registering => eprintln!("fetch registering"),
            }
        }
    });
    tx
}

async fn run_store_command(ledger_store: R2Store, command: StoreCommand) -> Result<()> {
    match command.command {
        StoreSubcommand::List(args) => {
            let rows = ledger_store.list_objects(store_filter(
                args.role.as_deref(),
                args.kind,
                args.id_prefix,
            )?)?;
            print_json(&rows)?;
        }
        StoreSubcommand::Show(args) => {
            let id = StoreObjectId::new(args.id)?;
            let object = ledger_store.get_object(&id)?;
            print_json(&object)?;
        }
        StoreSubcommand::ImportFile(args) => {
            let metadata_json = serde_json::from_str::<Value>(&args.metadata_json)?;
            let role = StoreObjectRole::parse(&args.role)?;
            let descriptor = ledger_store
                .register_file(RegisterFileRequest {
                    path: &args.path,
                    role,
                    kind: args.kind,
                    file_name: args.file_name,
                    format: args.format,
                    media_type: args.media_type,
                    lineage: Vec::new(),
                    metadata_json,
                })
                .await?;
            print_json(&descriptor)?;
        }
        StoreSubcommand::Hydrate(args) => {
            let id = StoreObjectId::new(args.id)?;
            let hydrated = ledger_store.hydrate(&id).await?;
            print_json(&hydrated)?;
        }
        StoreSubcommand::Offload(args) => {
            let id = StoreObjectId::new(args.id)?;
            let report = ledger_store.offload_object(&id)?;
            print_json(&report)?;
        }
        StoreSubcommand::Delete(args) => {
            let id = StoreObjectId::new(args.id)?;
            let report = ledger_store.delete_object(&id, args.force_raw).await?;
            print_json(&report)?;
        }
        StoreSubcommand::LocalStatus => {
            let status = ledger_store.local_status()?;
            print_json(&status)?;
        }
        StoreSubcommand::LocalPrune => {
            let report = ledger_store.enforce_local_limit(None)?;
            print_json(&report)?;
        }
        StoreSubcommand::Sync(args) => {
            let report = ledger_store
                .sync_registry(args.overwrite, args.dry_run)
                .await?;
            print_json(&report)?;
        }
        StoreSubcommand::Validate(args) => {
            if let Some(id) = args.id {
                let id = StoreObjectId::new(id)?;
                let report = ledger_store
                    .validate_object(&id, args.verify_remote)
                    .await?;
                print_json(&report)?;
            } else {
                let report = ledger_store
                    .validate_all(
                        store_filter(args.role.as_deref(), args.kind, None)?,
                        args.verify_remote,
                    )
                    .await?;
                print_json(&report)?;
            }
        }
        StoreSubcommand::AbortIncompleteUploads(args) => {
            let uploads = ledger_store
                .list_incomplete_multipart_uploads(&args.prefix)
                .await?;
            let mut aborted = Vec::new();
            let mut failed = Vec::new();
            if args.execute {
                for upload in &uploads {
                    match ledger_store
                        .abort_multipart_upload(&upload.key, &upload.upload_id)
                        .await
                    {
                        Ok(()) => aborted.push(upload.upload_id.clone()),
                        Err(error) => failed.push(json!({
                            "upload_id": upload.upload_id,
                            "key": upload.key,
                            "error": error.to_string(),
                        })),
                    }
                }
            }
            print_json(&json!({
                "prefix": args.prefix,
                "executed": args.execute,
                "found": uploads.len(),
                "uploads": uploads,
                "aborted": aborted,
                "failed": failed,
            }))?;
            if !failed.is_empty() {
                std::process::exit(1);
            }
        }
    }

    Ok(())
}

fn store_filter(
    role: Option<&str>,
    kind: Option<String>,
    id_prefix: Option<String>,
) -> Result<ObjectFilter> {
    Ok(ObjectFilter {
        role: role.map(StoreObjectRole::parse).transpose()?,
        kind,
        id_prefix,
    })
}

fn print_json<T: serde::Serialize>(value: &T) -> Result<()> {
    println!("{}", serde_json::to_string_pretty(value)?);
    Ok(())
}
