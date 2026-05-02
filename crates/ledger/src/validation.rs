use anyhow::{Context, Result};
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
    pub trigger: ValidationTrigger,
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
pub enum ValidationTrigger {
    Prepare,
    Rebuild,
    Manual,
}

impl ValidationTrigger {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Prepare => "prepare",
            Self::Rebuild => "rebuild",
            Self::Manual => "manual",
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ReplayDatasetTrustStatus {
    Missing,
    RawAvailable,
    ReplayDatasetAvailable,
    ReadyToTrain,
    ReadyWithWarnings,
    Invalid,
}

impl ReplayDatasetTrustStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Missing => "missing",
            Self::RawAvailable => "raw_available",
            Self::ReplayDatasetAvailable => "replay_dataset_available",
            Self::ReadyToTrain => "ready_to_train",
            Self::ReadyWithWarnings => "ready_with_warnings",
            Self::Invalid => "invalid",
        }
    }
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ValidationCheckStatus {
    Pass,
    Warning,
    Fail,
    Skipped,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidationCheck {
    pub id: String,
    pub label: String,
    pub status: ValidationCheckStatus,
    pub summary: String,
}

#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ValidationIssueSeverity {
    Warning,
    Error,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidationIssue {
    pub severity: ValidationIssueSeverity,
    pub code: String,
    pub message: String,
    pub check_id: String,
    pub recommended_action: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplayDatasetValidationReport {
    pub schema_version: u32,
    pub replay_dataset_id: String,
    pub market_day: MarketDay,
    pub trigger: ValidationTrigger,
    pub mode: ValidationMode,
    pub artifacts: ReplayDatasetArtifacts,
    pub counts: Option<ReplayDatasetCounts>,
    pub book_check: Option<BookCheckValidationReport>,
    pub replay_probe: Option<ReplayProbeReport>,
    pub trust_status: ReplayDatasetTrustStatus,
    pub summary: String,
    pub recommended_action: Option<String>,
    pub checks: Vec<ValidationCheck>,
    pub issues: Vec<ValidationIssue>,
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

        let report = match validate_loaded_replay_dataset(
            dataset.clone(),
            request.clone(),
            progress,
        )
        .await
        {
            Ok(report) => report,
            Err(err) => {
                let report = invalid_validation_report(dataset, request, err.to_string());
                self.store
                    .record_validation_report(
                        &report.market_day,
                        &report.replay_dataset_id,
                        report.trigger.as_str(),
                        report.mode.clone(),
                        report_status(&report),
                        report.trust_status.as_str(),
                        &report.summary,
                        warning_count(&report),
                        error_count(&report),
                        stored_validation_report_json(&report),
                    )
                    .await?;
                self.store
                    .cleanup_staged_replay_dataset(&report.market_day, &run_label)
                    .await?;
                return Err(err);
            }
        };
        self.store
            .record_validation_report(
                &report.market_day,
                &report.replay_dataset_id,
                report.trigger.as_str(),
                report.mode.clone(),
                report_status(&report),
                report.trust_status.as_str(),
                &report.summary,
                warning_count(&report),
                error_count(&report),
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
    let mut checks = vec![
        validation_check(
            "artifact_presence",
            "Artifact presence",
            ValidationCheckStatus::Pass,
            "Required replay artifacts were staged from R2.",
        ),
        validation_check(
            "artifact_integrity",
            "Artifact integrity",
            ValidationCheckStatus::Pass,
            "Staged artifact size and checksum verification passed.",
        ),
        validation_check(
            "artifact_decode",
            "Artifact decode",
            ValidationCheckStatus::Pass,
            "Events, batches, and trades decoded successfully.",
        ),
        validation_check(
            "index_integrity",
            "Index integrity",
            ValidationCheckStatus::Pass,
            "EventStore batch and trade indexes rebuilt cleanly.",
        ),
    ];

    let book_check = if request.skip_book_check {
        progress.step("skipping book-check comparison");
        checks.push(validation_check(
            "book_check",
            "Book check",
            ValidationCheckStatus::Skipped,
            "Book-check comparison was skipped for this validation run.",
        ));
        None
    } else {
        let report = validate_book_check(&dataset.book_check_path, &event_store, &progress).await?;
        checks.push(book_check_validation_check(&report));
        Some(report)
    };

    let issues = validation_issues(book_check.as_ref());
    let replay_probe = run_replay_probe(
        event_store,
        request.replay_batches,
        request.replay_all,
        &progress,
    )?;
    checks.push(validation_check(
        "replay_probe",
        "Replay probe",
        ValidationCheckStatus::Pass,
        format!(
            "ReplaySimulator applied {}/{} requested batches.",
            replay_probe.applied_batches, replay_probe.requested_batches
        ),
    ));
    let trust_status = if issues
        .iter()
        .any(|issue| issue.severity == ValidationIssueSeverity::Error)
    {
        ReplayDatasetTrustStatus::Invalid
    } else if issues
        .iter()
        .any(|issue| issue.severity == ValidationIssueSeverity::Warning)
    {
        ReplayDatasetTrustStatus::ReadyWithWarnings
    } else {
        ReplayDatasetTrustStatus::ReadyToTrain
    };
    let warnings = warning_messages(&issues);
    let (summary, recommended_action) = validation_outcome_text(trust_status, &issues);

    Ok(ReplayDatasetValidationReport {
        schema_version: 1,
        replay_dataset_id: dataset.replay_dataset_id,
        market_day: dataset.market_day,
        trigger: request.trigger,
        mode: ValidationMode::Full,
        artifacts: ReplayDatasetArtifacts {
            events_path: dataset.events_path,
            batches_path: dataset.batches_path,
            trades_path: dataset.trades_path,
            book_check_path: dataset.book_check_path,
        },
        counts: Some(counts),
        book_check,
        replay_probe: Some(replay_probe),
        trust_status,
        summary,
        recommended_action,
        checks,
        issues,
        warnings,
    })
}

fn stored_validation_report_json(report: &ReplayDatasetValidationReport) -> serde_json::Value {
    serde_json::json!({
        "schema_version": report.schema_version,
        "replay_dataset_id": report.replay_dataset_id,
        "market_day_id": report.market_day.id,
        "trigger": report.trigger,
        "mode": report.mode,
        "counts": report.counts,
        "book_check": report.book_check,
        "replay_probe": report.replay_probe,
        "trust_status": report.trust_status,
        "summary": report.summary,
        "recommended_action": report.recommended_action,
        "checks": report.checks,
        "issues": report.issues,
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

    Ok(BookCheckValidationReport {
        matched: actual == expected,
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

fn validation_issues(book_check: Option<&BookCheckValidationReport>) -> Vec<ValidationIssue> {
    let mut issues = Vec::new();
    if let Some(book_check) = book_check {
        if !book_check.matched {
            issues.push(ValidationIssue {
                severity: ValidationIssueSeverity::Error,
                code: "book_check.mismatch".to_string(),
                message: "Book-check comparison did not match the persisted artifact.".to_string(),
                check_id: "book_check".to_string(),
                recommended_action: Some("Rebuild ReplayDataset from raw data.".to_string()),
            });
        }
        if book_check.actual.warning_count > 0 {
            issues.push(ValidationIssue {
                severity: ValidationIssueSeverity::Warning,
                code: "book_check.order_book_warnings".to_string(),
                message: format!(
                    "Book-check reported {} order-book warnings.",
                    book_check.actual.warning_count
                ),
                check_id: "book_check".to_string(),
                recommended_action: Some(
                    "Review book-check details before using this day for training.".to_string(),
                ),
            });
        }
    }
    issues
}

fn invalid_validation_report(
    dataset: ReplayDataset,
    request: ValidateReplayDatasetRequest,
    error: String,
) -> ReplayDatasetValidationReport {
    let issues = vec![ValidationIssue {
        severity: ValidationIssueSeverity::Error,
        code: "validation.failed".to_string(),
        message: error,
        check_id: "validation".to_string(),
        recommended_action: Some("Rebuild ReplayDataset from raw data.".to_string()),
    }];
    let warnings = warning_messages(&issues);
    let (summary, recommended_action) =
        validation_outcome_text(ReplayDatasetTrustStatus::Invalid, &issues);

    ReplayDatasetValidationReport {
        schema_version: 1,
        replay_dataset_id: dataset.replay_dataset_id,
        market_day: dataset.market_day,
        trigger: request.trigger,
        mode: ValidationMode::Full,
        artifacts: ReplayDatasetArtifacts {
            events_path: dataset.events_path,
            batches_path: dataset.batches_path,
            trades_path: dataset.trades_path,
            book_check_path: dataset.book_check_path,
        },
        counts: None,
        book_check: None,
        replay_probe: None,
        trust_status: ReplayDatasetTrustStatus::Invalid,
        summary,
        recommended_action,
        checks: vec![validation_check(
            "validation",
            "Validation",
            ValidationCheckStatus::Fail,
            "Validation failed before all checks could complete.",
        )],
        issues,
        warnings,
    }
}

fn report_status(report: &ReplayDatasetValidationReport) -> ValidationReportStatus {
    if error_count(report) > 0 {
        ValidationReportStatus::Invalid
    } else if warning_count(report) > 0 {
        ValidationReportStatus::Warning
    } else {
        ValidationReportStatus::Valid
    }
}

fn warning_count(report: &ReplayDatasetValidationReport) -> usize {
    report
        .issues
        .iter()
        .filter(|issue| issue.severity == ValidationIssueSeverity::Warning)
        .count()
}

fn error_count(report: &ReplayDatasetValidationReport) -> usize {
    report
        .issues
        .iter()
        .filter(|issue| issue.severity == ValidationIssueSeverity::Error)
        .count()
}

fn warning_messages(issues: &[ValidationIssue]) -> Vec<String> {
    issues
        .iter()
        .filter(|issue| issue.severity == ValidationIssueSeverity::Warning)
        .map(|issue| issue.message.clone())
        .collect()
}

fn validation_check(
    id: impl Into<String>,
    label: impl Into<String>,
    status: ValidationCheckStatus,
    summary: impl Into<String>,
) -> ValidationCheck {
    ValidationCheck {
        id: id.into(),
        label: label.into(),
        status,
        summary: summary.into(),
    }
}

fn book_check_validation_check(report: &BookCheckValidationReport) -> ValidationCheck {
    let status = if !report.matched {
        ValidationCheckStatus::Fail
    } else if report.actual.warning_count > 0 {
        ValidationCheckStatus::Warning
    } else {
        ValidationCheckStatus::Pass
    };
    validation_check(
        "book_check",
        "Book check",
        status,
        if report.matched {
            format!(
                "Book-check matched with {} order-book warnings.",
                report.actual.warning_count
            )
        } else {
            "Book-check comparison did not match the persisted artifact.".to_string()
        },
    )
}

fn validation_outcome_text(
    trust_status: ReplayDatasetTrustStatus,
    issues: &[ValidationIssue],
) -> (String, Option<String>) {
    match trust_status {
        ReplayDatasetTrustStatus::ReadyToTrain => (
            "ReplayDataset validated and ready for training.".to_string(),
            None,
        ),
        ReplayDatasetTrustStatus::ReadyWithWarnings => (
            format!(
                "ReplayDataset is usable but has {} validation warning{}.",
                issues
                    .iter()
                    .filter(|issue| issue.severity == ValidationIssueSeverity::Warning)
                    .count(),
                if issues
                    .iter()
                    .filter(|issue| issue.severity == ValidationIssueSeverity::Warning)
                    .count()
                    == 1
                {
                    ""
                } else {
                    "s"
                }
            ),
            Some("Review warnings before using this day for training.".to_string()),
        ),
        ReplayDatasetTrustStatus::Invalid => (
            issues
                .iter()
                .find(|issue| issue.severity == ValidationIssueSeverity::Error)
                .map(|issue| format!("ReplayDataset is invalid: {}", issue.message))
                .unwrap_or_else(|| "ReplayDataset is invalid.".to_string()),
            Some("Rebuild ReplayDataset from raw data.".to_string()),
        ),
        ReplayDatasetTrustStatus::Missing => (
            "No raw data or ReplayDataset is available for this MarketDay.".to_string(),
            Some("Prepare this MarketDay.".to_string()),
        ),
        ReplayDatasetTrustStatus::RawAvailable => (
            "Raw market data is available; ReplayDataset has not been built.".to_string(),
            Some("Build ReplayDataset from raw data.".to_string()),
        ),
        ReplayDatasetTrustStatus::ReplayDatasetAvailable => (
            "ReplayDataset is available but has not been validated.".to_string(),
            Some("Run validation before training.".to_string()),
        ),
    }
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
