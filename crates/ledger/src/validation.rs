use anyhow::{bail, Context, Result};
use chrono::NaiveDate;
use ledger_book::OrderBook;
use ledger_domain::{now_ns, EventStore, ExecutionProfile, MarketDay, VisibilityProfile};
use ledger_ingest::{BookCheckReport, IngestReport};
use ledger_replay::ReplaySimulator;
use ledger_store::{ValidationMode, ValidationReportStatus};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::time::Instant;

use crate::{Ledger, LedgerProgress, LedgerProgressSink, ObjectStore, ReplayDataset};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidateReplayDatasetRequest {
    pub symbol: String,
    pub market_date: NaiveDate,
    pub skip_book_check: bool,
    pub replay_batches: Option<usize>,
    pub replay_all: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PrepareReplayDatasetRequest {
    pub symbol: String,
    pub market_date: NaiveDate,
    pub rebuild_replay: bool,
    pub skip_book_check: bool,
    pub replay_batches: Option<usize>,
    pub replay_all: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PrepareReplayDatasetReport {
    pub ingest: IngestReport,
    pub validation: ReplayDatasetValidationReport,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplayDatasetArtifacts {
    pub events_path: PathBuf,
    pub batches_path: PathBuf,
    pub trades_path: PathBuf,
    pub book_check_path: PathBuf,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplayDatasetCounts {
    pub event_count: usize,
    pub batch_count: usize,
    pub trade_count: usize,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ReplayDatasetTrustStatus {
    Missing,
    Downloaded,
    Hydrated,
    Validated,
    ReadyToTrain,
    ReadyWithWarnings,
    Invalid,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplayDatasetValidationReport {
    pub replay_dataset_id: String,
    pub market_day: MarketDay,
    pub artifacts: ReplayDatasetArtifacts,
    pub counts: ReplayDatasetCounts,
    pub indexes_valid: bool,
    pub book_check: Option<BookCheckValidationReport>,
    pub replay_probe: ReplayProbeReport,
    pub status: ReplayDatasetTrustStatus,
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct BookCheckValidationReport {
    pub matched: bool,
    pub expected: BookCheckReport,
    pub actual: BookCheckReport,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplayProbeReport {
    pub requested_batches: usize,
    pub applied_batches: usize,
    pub total_batches: usize,
    pub cursor_ts_ns: u64,
    pub frame_count: usize,
    pub fill_count: usize,
    pub final_book_checksum: String,
}

impl<S: ObjectStore + 'static> Ledger<S> {
    pub async fn validate_replay_dataset(
        &self,
        request: ValidateReplayDatasetRequest,
    ) -> Result<ReplayDatasetValidationReport> {
        self.validate_replay_dataset_with_progress(request, None)
            .await
    }

    pub async fn validate_replay_dataset_with_progress(
        &self,
        request: ValidateReplayDatasetRequest,
        progress_sink: Option<LedgerProgressSink>,
    ) -> Result<ReplayDatasetValidationReport> {
        let progress = LedgerProgress::new(progress_sink);
        progress.step(format!(
            "validating replay dataset {} {}",
            request.symbol, request.market_date
        ));

        progress.step("checking catalog and staging replay artifacts from R2");
        let load_started_at = Instant::now();
        let run_label = format!("validate-{}", now_ns());
        let dataset = self
            .store
            .stage_replay_dataset(&request.symbol, request.market_date, &run_label)
            .await?;
        let dataset = ReplayDataset {
            replay_dataset_id: dataset.replay_dataset_id,
            market_day: dataset.market_day,
            events_path: dataset.events_path,
            batches_path: dataset.batches_path,
            trades_path: dataset.trades_path,
            book_check_path: dataset.book_check_path,
        };
        progress.done("replay dataset loaded", load_started_at);

        let report = validate_loaded_replay_dataset(dataset, request, progress).await?;
        let status = if report.warnings.is_empty() {
            ValidationReportStatus::Valid
        } else {
            ValidationReportStatus::Warning
        };
        self.store
            .record_validation_report(
                &report.market_day,
                &report.replay_dataset_id,
                ValidationMode::Full,
                status,
                stored_validation_report_json(&report),
            )
            .await?;
        self.store
            .cleanup_staged_replay_dataset(&report.market_day, &run_label)
            .await?;
        Ok(report)
    }
}

async fn validate_loaded_replay_dataset(
    dataset: ReplayDataset,
    request: ValidateReplayDatasetRequest,
    progress: LedgerProgress,
) -> Result<ReplayDatasetValidationReport> {
    progress.step("decoding and validating event, batch, and trade artifacts");
    let decode_started_at = Instant::now();
    let event_store = dataset.event_store_with_progress(Some(&progress)).await?;
    progress.done(
        format!(
            "decoded {} events, {} batches, {} trades",
            event_store.events.len(),
            event_store.batches.len(),
            event_store.trades.len()
        ),
        decode_started_at,
    );

    let counts = ReplayDatasetCounts {
        event_count: event_store.events.len(),
        batch_count: event_store.batches.len(),
        trade_count: event_store.trades.len(),
    };

    let book_check = if request.skip_book_check {
        progress.step("skipping book-check comparison");
        None
    } else {
        Some(validate_book_check(&dataset.book_check_path, &event_store, &progress).await?)
    };

    let warnings = validation_warnings(book_check.as_ref());
    let replay_probe = run_replay_probe(
        event_store,
        request.replay_batches,
        request.replay_all,
        &progress,
    )?;
    let status = if warnings.is_empty() {
        ReplayDatasetTrustStatus::ReadyToTrain
    } else {
        ReplayDatasetTrustStatus::ReadyWithWarnings
    };

    Ok(ReplayDatasetValidationReport {
        replay_dataset_id: dataset.replay_dataset_id,
        market_day: dataset.market_day,
        artifacts: ReplayDatasetArtifacts {
            events_path: dataset.events_path,
            batches_path: dataset.batches_path,
            trades_path: dataset.trades_path,
            book_check_path: dataset.book_check_path,
        },
        counts,
        indexes_valid: true,
        book_check,
        replay_probe,
        status,
        warnings,
    })
}

fn stored_validation_report_json(report: &ReplayDatasetValidationReport) -> serde_json::Value {
    serde_json::json!({
        "replay_dataset_id": report.replay_dataset_id,
        "market_day_id": report.market_day.id,
        "counts": report.counts,
        "indexes_valid": report.indexes_valid,
        "book_check": report.book_check,
        "replay_probe": report.replay_probe,
        "status": report.status,
        "warnings": report.warnings,
    })
}

async fn validate_book_check(
    book_check_path: &PathBuf,
    event_store: &EventStore,
    progress: &LedgerProgress,
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
    progress: &LedgerProgress,
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
    progress: &LedgerProgress,
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

fn validation_warnings(book_check: Option<&BookCheckValidationReport>) -> Vec<String> {
    let mut warnings = Vec::new();
    if let Some(book_check) = book_check {
        if book_check.actual.warning_count > 0 {
            warnings.push(format!(
                "book-check reported {} order-book warnings",
                book_check.actual.warning_count
            ));
        }
    }
    warnings
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
