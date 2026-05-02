use axum::{
    extract::{Path, Query, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::{delete, get, post},
    Json, Router,
};
use chrono::{DateTime, NaiveDate, Utc};
use ledger::{
    Ledger, LedgerProgressEvent, LedgerProgressSink, PrepareReplayDatasetRequest,
    ValidateReplayDatasetRequest,
};
use ledger_domain::MarketDay;
use ledger_store::{
    LedgerJobRecord, LedgerJobStatus, MarketDayFilter, MarketDayRecord, R2ObjectStore,
    ReplayDatasetStatus,
};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use std::time::Instant;
use std::{fmt, future::Future, net::SocketAddr, sync::Arc};
use tower_http::cors::CorsLayer;
use uuid::Uuid;

pub type ApiLedger = Ledger<R2ObjectStore>;

#[derive(Clone)]
pub struct ApiState {
    pub ledger: ApiLedger,
}

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

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthResponse {
    pub ok: bool,
    pub service: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidateReplayDatasetBody {
    #[serde(default)]
    pub skip_book_check: bool,
    pub replay_batches: Option<usize>,
    #[serde(default)]
    pub replay_all: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeleteRawMarketDataBody {
    #[serde(default)]
    pub cascade: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateJobResponse {
    pub job: JobRecord,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobResponse {
    pub job: JobRecord,
}

#[derive(Debug, Clone, Deserialize)]
pub struct MarketDayListQuery {
    pub root: Option<String>,
    pub symbol: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct MarketDayStatusQuery {
    #[serde(default)]
    pub verify: bool,
}

#[derive(Debug)]
pub struct ApiError {
    status: StatusCode,
    message: String,
}

impl ApiError {
    fn bad_request(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::BAD_REQUEST,
            message: message.into(),
        }
    }

    fn not_found(message: impl Into<String>) -> Self {
        Self {
            status: StatusCode::NOT_FOUND,
            message: message.into(),
        }
    }

    fn internal(err: anyhow::Error) -> Self {
        Self {
            status: StatusCode::INTERNAL_SERVER_ERROR,
            message: err.to_string(),
        }
    }
}

impl fmt::Display for ApiError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.message)
    }
}

impl std::error::Error for ApiError {}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        (
            self.status,
            Json(json!({
                "error": self.message,
            })),
        )
            .into_response()
    }
}

pub fn router(state: ApiState) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/market-days", get(list_market_days))
        .route("/market-days/{symbol}/{date}", get(market_day_status))
        .route(
            "/market-days/{symbol}/{date}/prepare",
            post(prepare_replay_dataset),
        )
        .route(
            "/market-days/{symbol}/{date}/replay/build",
            post(build_replay_dataset),
        )
        .route(
            "/market-days/{symbol}/{date}/replay/validate",
            post(validate_replay_dataset),
        )
        .route(
            "/market-days/{symbol}/{date}/replay",
            delete(delete_replay_dataset),
        )
        .route(
            "/market-days/{symbol}/{date}/raw",
            delete(delete_raw_market_data),
        )
        .route("/jobs", get(active_jobs))
        .route("/jobs/{id}", get(job_status))
        .layer(CorsLayer::permissive())
        .with_state(state)
}

pub async fn serve(state: ApiState, addr: SocketAddr) -> anyhow::Result<()> {
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, router(state)).await?;
    Ok(())
}

async fn health() -> Json<HealthResponse> {
    Json(HealthResponse {
        ok: true,
        service: "ledger-api".to_string(),
    })
}

async fn list_market_days(
    State(state): State<ApiState>,
    Query(query): Query<MarketDayListQuery>,
) -> Result<Json<Vec<MarketDayRecord>>, ApiError> {
    let started_at = log_start("GET /market-days");
    let rows = state
        .ledger
        .list(MarketDayFilter {
            root: query.root,
            symbol: query.symbol,
        })
        .await
        .map_err(ApiError::internal)?;
    log_done("GET /market-days", started_at);
    Ok(Json(rows))
}

async fn market_day_status(
    State(state): State<ApiState>,
    Path((symbol, date)): Path<(String, String)>,
    Query(query): Query<MarketDayStatusQuery>,
) -> Result<Json<ReplayDatasetStatus>, ApiError> {
    let date = parse_date(&date)?;
    let label = if query.verify {
        format!("GET /market-days/{symbol}/{date}?verify=true")
    } else {
        format!("GET /market-days/{symbol}/{date}")
    };
    let started_at = log_start(&label);
    let status = if query.verify {
        state.ledger.verified_status(&symbol, date).await
    } else {
        state.ledger.status(&symbol, date).await
    }
    .map_err(ApiError::internal)?;
    log_done(&label, started_at);
    Ok(Json(status))
}

async fn prepare_replay_dataset(
    State(state): State<ApiState>,
    Path((symbol, date)): Path<(String, String)>,
) -> Result<Json<CreateJobResponse>, ApiError> {
    let date = parse_date(&date)?;
    let label = format!("POST /market-days/{symbol}/{date}/prepare");
    let started_at = log_start(&label);
    let target = market_day_job_target(&state.ledger, &symbol, date)?;
    let job = create_job(
        &state.ledger,
        JobKind::PrepareReplayDataset,
        Some(&target),
        json!({"symbol": symbol.clone(), "date": date}),
    )
    .map_err(ApiError::internal)?;
    let job_id = job.id;
    let ledger = state.ledger.clone();
    let progress_ledger = state.ledger.clone();

    spawn_job(
        ledger.clone(),
        job_id,
        JobKind::PrepareReplayDataset,
        "prepare started",
        async move {
            let progress = progress_sink_for_job(progress_ledger, job_id);
            ledger
                .prepare_replay_dataset_with_progress(
                    PrepareReplayDatasetRequest {
                        symbol,
                        market_date: date,
                        rebuild_replay: false,
                        skip_book_check: true,
                        replay_batches: Some(1),
                        replay_all: false,
                    },
                    Some(progress),
                )
                .await
        },
    );

    log_done(&label, started_at);
    Ok(Json(CreateJobResponse { job }))
}

async fn build_replay_dataset(
    State(state): State<ApiState>,
    Path((symbol, date)): Path<(String, String)>,
) -> Result<Json<CreateJobResponse>, ApiError> {
    let date = parse_date(&date)?;
    let label = format!("POST /market-days/{symbol}/{date}/replay/build");
    let started_at = log_start(&label);
    let target = market_day_job_target(&state.ledger, &symbol, date)?;
    let job = create_job(
        &state.ledger,
        JobKind::BuildReplayDataset,
        Some(&target),
        json!({"symbol": symbol.clone(), "date": date}),
    )
    .map_err(ApiError::internal)?;
    let job_id = job.id;
    let ledger = state.ledger.clone();
    let progress_ledger = state.ledger.clone();

    spawn_job(
        ledger.clone(),
        job_id,
        JobKind::BuildReplayDataset,
        "replay dataset rebuild started",
        async move {
            let progress = progress_sink_for_job(progress_ledger, job_id);
            ledger
                .prepare_replay_dataset_with_progress(
                    PrepareReplayDatasetRequest {
                        symbol,
                        market_date: date,
                        rebuild_replay: true,
                        skip_book_check: true,
                        replay_batches: Some(1),
                        replay_all: false,
                    },
                    Some(progress),
                )
                .await
        },
    );

    log_done(&label, started_at);
    Ok(Json(CreateJobResponse { job }))
}

async fn validate_replay_dataset(
    State(state): State<ApiState>,
    Path((symbol, date)): Path<(String, String)>,
    Json(body): Json<ValidateReplayDatasetBody>,
) -> Result<Json<CreateJobResponse>, ApiError> {
    let date = parse_date(&date)?;
    let label = format!("POST /market-days/{symbol}/{date}/replay/validate");
    let started_at = log_start(&label);
    let target = market_day_job_target(&state.ledger, &symbol, date)?;
    let job = create_job(
        &state.ledger,
        JobKind::ValidateReplayDataset,
        Some(&target),
        json!({
            "symbol": symbol.clone(),
            "date": date,
            "skip_book_check": body.skip_book_check,
            "replay_batches": body.replay_batches,
            "replay_all": body.replay_all,
        }),
    )
    .map_err(ApiError::internal)?;
    let job_id = job.id;
    let ledger = state.ledger.clone();
    let progress_ledger = state.ledger.clone();

    spawn_job(
        ledger.clone(),
        job_id,
        JobKind::ValidateReplayDataset,
        "validation started",
        async move {
            let progress = progress_sink_for_job(progress_ledger, job_id);
            ledger
                .validate_replay_dataset_with_progress(
                    ValidateReplayDatasetRequest {
                        symbol,
                        market_date: date,
                        skip_book_check: body.skip_book_check,
                        replay_batches: body.replay_batches,
                        replay_all: body.replay_all,
                    },
                    Some(progress),
                )
                .await
        },
    );

    log_done(&label, started_at);
    Ok(Json(CreateJobResponse { job }))
}

async fn delete_replay_dataset(
    State(state): State<ApiState>,
    Path((symbol, date)): Path<(String, String)>,
) -> Result<Json<CreateJobResponse>, ApiError> {
    let date = parse_date(&date)?;
    let label = format!("DELETE /market-days/{symbol}/{date}/replay");
    let started_at = log_start(&label);
    let target = market_day_job_target(&state.ledger, &symbol, date)?;
    let job = create_job(
        &state.ledger,
        JobKind::DeleteReplayDataset,
        Some(&target),
        json!({"symbol": symbol.clone(), "date": date}),
    )
    .map_err(ApiError::internal)?;
    let job_id = job.id;
    let ledger = state.ledger.clone();

    spawn_job(
        ledger.clone(),
        job_id,
        JobKind::DeleteReplayDataset,
        "delete replay dataset started",
        async move {
            ledger
                .delete_remote_replay_dataset(&symbol, date, false)
                .await
        },
    );

    log_done(&label, started_at);
    Ok(Json(CreateJobResponse { job }))
}

async fn delete_raw_market_data(
    State(state): State<ApiState>,
    Path((symbol, date)): Path<(String, String)>,
    body: Option<Json<DeleteRawMarketDataBody>>,
) -> Result<Json<CreateJobResponse>, ApiError> {
    let date = parse_date(&date)?;
    let cascade = body.map(|Json(body)| body.cascade).unwrap_or(false);
    let label = format!("DELETE /market-days/{symbol}/{date}/raw");
    let started_at = log_start(&label);
    let target = market_day_job_target(&state.ledger, &symbol, date)?;
    let job = create_job(
        &state.ledger,
        JobKind::DeleteRawMarketData,
        Some(&target),
        json!({"symbol": symbol.clone(), "date": date, "cascade": cascade}),
    )
    .map_err(ApiError::internal)?;
    let job_id = job.id;
    let ledger = state.ledger.clone();

    spawn_job(
        ledger.clone(),
        job_id,
        JobKind::DeleteRawMarketData,
        "delete raw market data started",
        async move { ledger.delete_raw_market_data(&symbol, date, cascade).await },
    );

    log_done(&label, started_at);
    Ok(Json(CreateJobResponse { job }))
}

async fn job_status(
    State(state): State<ApiState>,
    Path(id): Path<Uuid>,
) -> Result<Json<JobResponse>, ApiError> {
    let label = format!("GET /jobs/{id}");
    let started_at = log_start(&label);
    let job = state
        .ledger
        .store
        .catalog
        .job(&id.to_string())
        .map_err(ApiError::internal)?
        .map(api_job_record)
        .ok_or_else(|| ApiError::not_found(format!("job {id} not found")))?;
    log_done(&label, started_at);
    Ok(Json(JobResponse { job }))
}

async fn active_jobs(State(state): State<ApiState>) -> Result<Json<Vec<JobRecord>>, ApiError> {
    let label = "GET /jobs";
    let started_at = log_start(label);
    let jobs = state
        .ledger
        .store
        .catalog
        .jobs(true)
        .map_err(ApiError::internal)?
        .into_iter()
        .map(api_job_record)
        .collect();
    log_done(label, started_at);
    Ok(Json(jobs))
}

fn create_job(
    ledger: &ApiLedger,
    kind: JobKind,
    target: Option<&JobTarget>,
    request_json: Value,
) -> anyhow::Result<JobRecord> {
    let id = Uuid::new_v4();
    let job = ledger.store.catalog.create_job(
        &id.to_string(),
        kind.as_str(),
        target.map(|target| target.market_day_id.as_str()),
        request_json,
    )?;
    log_job(&kind, id, "queued");
    Ok(api_job_record(job))
}

fn market_day_job_target(
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

fn spawn_job<T, F>(
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

fn mark_job_running(ledger: &ApiLedger, id: Uuid, kind: &JobKind, message: impl Into<String>) {
    let message = message.into();
    ledger.store.catalog.mark_job_running(&id.to_string()).ok();
    ledger
        .store
        .catalog
        .append_job_event(&id.to_string(), "info", &message)
        .ok();
    log_job(kind, id, &message);
}

fn complete_job(ledger: &ApiLedger, id: Uuid, kind: &JobKind, result: Value) {
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

fn parse_date(date: &str) -> Result<NaiveDate, ApiError> {
    NaiveDate::parse_from_str(date, "%Y-%m-%d")
        .map_err(|err| ApiError::bad_request(format!("invalid date {date}: {err}")))
}

fn log_start(label: &str) -> Instant {
    eprintln!("[ledger-api] {label}");
    Instant::now()
}

fn log_done(label: &str, started_at: Instant) {
    eprintln!(
        "[ledger-api] {label} completed ({:.2}s)",
        started_at.elapsed().as_secs_f64()
    );
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
    fn as_str(&self) -> &'static str {
        match self {
            Self::PrepareReplayDataset => "prepare_replay_dataset",
            Self::BuildReplayDataset => "build_replay_dataset",
            Self::ValidateReplayDataset => "validate_replay_dataset",
            Self::DeleteReplayDataset => "delete_replay_dataset",
            Self::DeleteRawMarketData => "delete_raw_market_data",
        }
    }
}

fn api_job_record(record: LedgerJobRecord) -> JobRecord {
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

fn ns_to_datetime(ns: u64) -> DateTime<Utc> {
    let secs = (ns / 1_000_000_000) as i64;
    let nanos = (ns % 1_000_000_000) as u32;
    DateTime::<Utc>::from_timestamp(secs, nanos).unwrap_or(DateTime::<Utc>::UNIX_EPOCH)
}

#[cfg(test)]
mod tests {
    use super::*;
    use ledger_store::{LedgerStore, LocalStore, ObjectKeyBuilder, R2Config, R2ObjectStore};
    use std::sync::Arc;

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
            .jobs(true)
            .unwrap()
            .into_iter()
            .map(api_job_record)
            .collect::<Vec<_>>();

        assert_eq!(active.len(), 1);
        assert_eq!(active[0].id, queued.id);
    }

    #[tokio::test]
    async fn health_reports_ok() {
        let Json(response) = health().await;
        assert!(response.ok);
        assert_eq!(response.service, "ledger-api");
    }
}
