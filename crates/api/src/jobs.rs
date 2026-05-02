use crate::error::ApiError;
use crate::state::ApiLedger;
use crate::time::ns_to_datetime;
use chrono::{DateTime, NaiveDate, Utc};
use ledger::{LedgerProgressEvent, LedgerProgressSink};
use ledger_domain::MarketDay;
use ledger_store::{LedgerJobRecord, LedgerJobStatus};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::{future::Future, sync::Arc};
use uuid::Uuid;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum JobKind {
    PrepareReplayDataset,
    BuildReplayDataset,
    ValidateReplayDataset,
    DeleteReplayDataset,
    DeleteRawMarketData,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum JobStatus {
    Queued,
    Running,
    Succeeded,
    Failed,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobRecord {
    pub id: Uuid,
    pub kind: JobKind,
    pub status: JobStatus,
    pub market_day_id: Option<String>,
    pub target: Option<JobTarget>,
    pub created_at: DateTime<Utc>,
    pub started_at: Option<DateTime<Utc>>,
    pub finished_at: Option<DateTime<Utc>>,
    pub progress: Vec<String>,
    pub result: Option<Value>,
    pub error: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobTarget {
    pub market_day_id: String,
    pub symbol: String,
    pub market_date: NaiveDate,
}

impl JobTarget {
    fn from_market_day(md: &MarketDay) -> Self {
        Self {
            market_day_id: md.id.clone(),
            symbol: md.contract_symbol.clone(),
            market_date: md.market_date,
        }
    }
}

pub(crate) fn create_job(
    ledger: &ApiLedger,
    kind: JobKind,
    target: Option<&JobTarget>,
    request_json: Value,
) -> Result<JobRecord, ApiError> {
    if let Some(target) = target {
        if let Some(active) = ledger
            .store
            .catalog
            .active_job_for_market_day(&target.market_day_id)
            .map_err(ApiError::internal)?
        {
            return Err(ApiError::conflict(format!(
                "market day {} already has active job {} ({})",
                target.market_day_id, active.id, active.kind
            )));
        }
    }

    let id = Uuid::new_v4();
    let job = ledger
        .store
        .catalog
        .create_job(
            &id.to_string(),
            kind.as_str(),
            target.map(|target| target.market_day_id.as_str()),
            request_json,
        )
        .map_err(ApiError::internal)?;
    log_job(&kind, id, "queued");
    Ok(api_job_record(job))
}

pub(crate) fn market_day_job_target(
    ledger: &ApiLedger,
    symbol: &str,
    date: NaiveDate,
) -> Result<JobTarget, ApiError> {
    let md = MarketDay::resolve_es(symbol, date).map_err(ApiError::internal)?;
    if ledger
        .store
        .catalog
        .market_day(&md.id)
        .map_err(ApiError::internal)?
        .is_none()
    {
        ledger
            .store
            .catalog
            .upsert_market_day(&md, false)
            .map_err(ApiError::internal)?;
    }
    Ok(JobTarget::from_market_day(&md))
}

pub(crate) fn spawn_job<T, F>(
    ledger: ApiLedger,
    id: Uuid,
    kind: JobKind,
    start_message: &'static str,
    future: F,
) where
    T: Serialize + Send + 'static,
    F: Future<Output = anyhow::Result<T>> + Send + 'static,
{
    let worker_ledger = ledger.clone();
    let worker_kind = kind.clone();
    let handle = tokio::spawn(async move {
        mark_job_running(&worker_ledger, id, &worker_kind, start_message);
        let result = future.await;
        finish_job_with_result(&worker_ledger, id, &worker_kind, result);
    });

    tokio::spawn(async move {
        if let Err(err) = handle.await {
            let message = if err.is_panic() {
                "job task panicked".to_string()
            } else {
                format!("job task failed to join: {err}")
            };
            fail_job(&ledger, id, &kind, message);
        }
    });
}

pub(crate) fn mark_job_running(
    ledger: &ApiLedger,
    id: Uuid,
    kind: &JobKind,
    message: impl Into<String>,
) {
    let message = message.into();
    ledger.store.catalog.mark_job_running(&id.to_string()).ok();
    ledger
        .store
        .catalog
        .append_job_event(&id.to_string(), "info", &message)
        .ok();
    log_job(kind, id, &message);
}

pub(crate) fn complete_job(ledger: &ApiLedger, id: Uuid, kind: &JobKind, result: Value) {
    if let Err(err) = ledger.store.catalog.complete_job(&id.to_string(), result) {
        eprintln!("[ledger-api] job {id} {kind:?}: failed to mark succeeded: {err}");
    }
    if let Err(err) =
        ledger
            .store
            .catalog
            .append_job_event(&id.to_string(), "info", "job completed")
    {
        eprintln!("[ledger-api] job {id} {kind:?}: failed to append completion event: {err}");
    }
    log_job(kind, id, "succeeded");
}

fn fail_job(ledger: &ApiLedger, id: Uuid, kind: &JobKind, error: String) {
    if let Err(err) = ledger.store.catalog.fail_job(&id.to_string(), &error) {
        eprintln!("[ledger-api] job {id} {kind:?}: failed to mark failed: {err}");
    }
    if let Err(err) = ledger
        .store
        .catalog
        .append_job_event(&id.to_string(), "error", "job failed")
    {
        eprintln!("[ledger-api] job {id} {kind:?}: failed to append failure event: {err}");
    }
    log_job(kind, id, &format!("failed: {error}"));
}

fn finish_job_with_result<T>(
    ledger: &ApiLedger,
    id: Uuid,
    kind: &JobKind,
    result: anyhow::Result<T>,
) where
    T: Serialize,
{
    match result {
        Ok(report) => {
            complete_job(
                ledger,
                id,
                kind,
                serde_json::to_value(report).unwrap_or(Value::Null),
            );
        }
        Err(err) => {
            fail_job(ledger, id, kind, format_error_chain(&err));
        }
    }
}

fn format_error_chain(err: &anyhow::Error) -> String {
    err.chain()
        .map(ToString::to_string)
        .collect::<Vec<_>>()
        .join(": ")
}

pub fn progress_sink_for_job(ledger: ApiLedger, id: Uuid) -> LedgerProgressSink {
    Arc::new(move |event| {
        let message = match event {
            LedgerProgressEvent::Step { message } => message,
            LedgerProgressEvent::Done {
                message,
                elapsed_ms,
            } => format!("{message} ({elapsed_ms}ms)"),
        };
        ledger
            .store
            .catalog
            .append_job_event(&id.to_string(), "info", &message)
            .ok();
        let kind = ledger
            .store
            .catalog
            .job(&id.to_string())
            .ok()
            .flatten()
            .map(|job| job.kind)
            .unwrap_or_else(|| "unknown".to_string());
        eprintln!("[ledger-api] job {id} {kind}: {message}");
    })
}

fn log_job(kind: &JobKind, id: Uuid, message: &str) {
    eprintln!("[ledger-api] job {id} {kind:?}: {message}");
}

impl JobKind {
    pub(crate) fn as_str(&self) -> &'static str {
        match self {
            Self::PrepareReplayDataset => "prepare_replay_dataset",
            Self::BuildReplayDataset => "build_replay_dataset",
            Self::ValidateReplayDataset => "validate_replay_dataset",
            Self::DeleteReplayDataset => "delete_replay_dataset",
            Self::DeleteRawMarketData => "delete_raw_market_data",
        }
    }
}

pub(crate) fn api_job_record(record: LedgerJobRecord) -> JobRecord {
    let target = job_target_from_record(&record);
    JobRecord {
        id: Uuid::parse_str(&record.id).unwrap_or_else(|_| Uuid::nil()),
        kind: job_kind_from_str(&record.kind),
        status: match record.status {
            LedgerJobStatus::Queued => JobStatus::Queued,
            LedgerJobStatus::Running => JobStatus::Running,
            LedgerJobStatus::Succeeded => JobStatus::Succeeded,
            LedgerJobStatus::Failed => JobStatus::Failed,
        },
        market_day_id: record.market_day_id.clone(),
        target,
        created_at: ns_to_datetime(record.created_at_ns),
        started_at: record.started_at_ns.map(ns_to_datetime),
        finished_at: record.finished_at_ns.map(ns_to_datetime),
        progress: record
            .events
            .into_iter()
            .map(|event| event.message)
            .collect(),
        result: record.result_json,
        error: record.error,
    }
}

fn job_target_from_record(record: &LedgerJobRecord) -> Option<JobTarget> {
    let symbol = record.request_json.get("symbol")?.as_str()?.to_string();
    let market_date = record
        .request_json
        .get("date")
        .and_then(|value| value.as_str())
        .and_then(|value| NaiveDate::parse_from_str(value, "%Y-%m-%d").ok())?;
    let market_day_id = record
        .market_day_id
        .clone()
        .unwrap_or_else(|| format!("ES-{symbol}-{market_date}"));
    Some(JobTarget {
        market_day_id,
        symbol,
        market_date,
    })
}

fn job_kind_from_str(kind: &str) -> JobKind {
    match kind {
        "prepare_replay_dataset" => JobKind::PrepareReplayDataset,
        "build_replay_dataset" => JobKind::BuildReplayDataset,
        "validate_replay_dataset" => JobKind::ValidateReplayDataset,
        "delete_replay_dataset" => JobKind::DeleteReplayDataset,
        "delete_raw_market_data" => JobKind::DeleteRawMarketData,
        _ => JobKind::PrepareReplayDataset,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::StatusCode;
    use ledger::Ledger;
    use ledger_store::{LedgerStore, LocalStore, ObjectKeyBuilder, R2Config, R2ObjectStore};
    use serde_json::json;

    async fn test_ledger() -> (tempfile::TempDir, ApiLedger) {
        let dir = tempfile::tempdir().unwrap();
        let remote = R2ObjectStore::new(R2Config {
            account_id: "account".to_string(),
            access_key_id: "access".to_string(),
            secret_access_key: "secret".to_string(),
            bucket: "test".to_string(),
            endpoint_url: Some("https://example.invalid".to_string()),
            region: "auto".to_string(),
            multipart_threshold_bytes: 1024,
            multipart_part_size_bytes: 1024,
        })
        .await
        .unwrap();
        let store = LedgerStore::new(
            LocalStore::new(dir.path()),
            Arc::new(remote),
            ObjectKeyBuilder::default(),
        );
        (dir, Ledger::new(store))
    }

    #[tokio::test]
    async fn job_registry_tracks_lifecycle_in_sqlite() {
        let (_dir, ledger) = test_ledger().await;
        let target = market_day_job_target(
            &ledger,
            "ESH6",
            NaiveDate::from_ymd_opt(2026, 3, 12).unwrap(),
        )
        .unwrap();
        let job = create_job(
            &ledger,
            JobKind::PrepareReplayDataset,
            Some(&target),
            json!({"symbol": "ESH6", "date": "2026-03-12"}),
        )
        .unwrap();

        mark_job_running(&ledger, job.id, &JobKind::PrepareReplayDataset, "started");
        complete_job(
            &ledger,
            job.id,
            &JobKind::PrepareReplayDataset,
            json!({"ok": true}),
        );

        let job = ledger
            .store
            .catalog
            .job(&job.id.to_string())
            .unwrap()
            .map(api_job_record)
            .unwrap();
        assert_eq!(job.status, JobStatus::Succeeded);
        assert_eq!(job.market_day_id.as_deref(), Some("ES-ESH6-2026-03-12"));
        assert_eq!(
            job.target
                .as_ref()
                .map(|target| target.market_day_id.as_str()),
            Some("ES-ESH6-2026-03-12")
        );
        assert_eq!(job.progress, vec!["started", "job completed"]);
        assert_eq!(job.result, Some(json!({"ok": true})));
    }

    #[tokio::test]
    async fn active_jobs_returns_only_queued_or_running_jobs() {
        let (_dir, ledger) = test_ledger().await;
        let queued = create_job(&ledger, JobKind::PrepareReplayDataset, None, json!({})).unwrap();
        let completed =
            create_job(&ledger, JobKind::ValidateReplayDataset, None, json!({})).unwrap();
        complete_job(
            &ledger,
            completed.id,
            &JobKind::ValidateReplayDataset,
            json!({"ok": true}),
        );
        let active = ledger
            .store
            .catalog
            .jobs(true, None)
            .unwrap()
            .into_iter()
            .map(api_job_record)
            .collect::<Vec<_>>();

        assert_eq!(active.len(), 1);
        assert_eq!(active[0].id, queued.id);
    }

    #[tokio::test]
    async fn market_day_jobs_conflict_with_active_job() {
        let (_dir, ledger) = test_ledger().await;
        let target = market_day_job_target(
            &ledger,
            "ESH6",
            NaiveDate::from_ymd_opt(2026, 3, 12).unwrap(),
        )
        .unwrap();
        let active = create_job(
            &ledger,
            JobKind::BuildReplayDataset,
            Some(&target),
            json!({"symbol": "ESH6", "date": "2026-03-12"}),
        )
        .unwrap();

        let conflict = create_job(
            &ledger,
            JobKind::DeleteRawMarketData,
            Some(&target),
            json!({"symbol": "ESH6", "date": "2026-03-12"}),
        )
        .unwrap_err();

        assert_eq!(conflict.status, StatusCode::CONFLICT);
        assert!(conflict.message.contains(&active.id.to_string()));
    }

    #[tokio::test]
    async fn market_day_job_history_includes_completed_jobs() {
        let (_dir, ledger) = test_ledger().await;
        let target = market_day_job_target(
            &ledger,
            "ESH6",
            NaiveDate::from_ymd_opt(2026, 3, 12).unwrap(),
        )
        .unwrap();
        let completed = create_job(
            &ledger,
            JobKind::ValidateReplayDataset,
            Some(&target),
            json!({"symbol": "ESH6", "date": "2026-03-12"}),
        )
        .unwrap();
        complete_job(
            &ledger,
            completed.id,
            &JobKind::ValidateReplayDataset,
            json!({"ok": true}),
        );

        let jobs = ledger
            .store
            .catalog
            .jobs_for_market_day(&target.market_day_id, false, Some(10))
            .unwrap()
            .into_iter()
            .map(api_job_record)
            .collect::<Vec<_>>();

        assert_eq!(jobs.len(), 1);
        assert_eq!(jobs[0].id, completed.id);
        assert_eq!(jobs[0].status, JobStatus::Succeeded);
    }
}
