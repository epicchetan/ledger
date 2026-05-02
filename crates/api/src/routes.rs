use crate::dto::{
    CreateJobResponse, DataCenterMarketDay, DeleteRawMarketDataBody, HealthResponse, JobResponse,
    JobsQuery, MarketDayListQuery, MarketDayStatusQuery, ValidateReplayDatasetBody,
};
use crate::error::ApiError;
use crate::jobs::{
    api_job_record, create_job, market_day_job_target, progress_sink_for_job, spawn_job, JobKind,
};
use crate::presenters::{data_center_market_day, data_center_market_day_status};
use crate::state::ApiState;
use axum::{
    extract::{Path, Query, State},
    Json,
};
use chrono::NaiveDate;
use ledger::{PrepareReplayDatasetRequest, ValidateReplayDatasetRequest};
use ledger_domain::MarketDay;
use ledger_store::MarketDayFilter;
use serde_json::json;
use std::time::Instant;
use uuid::Uuid;

pub(crate) async fn health() -> Json<HealthResponse> {
    Json(HealthResponse {
        ok: true,
        service: "ledger-api".to_string(),
    })
}

pub(crate) async fn list_market_days(
    State(state): State<ApiState>,
    Query(query): Query<MarketDayListQuery>,
) -> Result<Json<Vec<DataCenterMarketDay>>, ApiError> {
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
    Ok(Json(rows.into_iter().map(data_center_market_day).collect()))
}

pub(crate) async fn market_day_status(
    State(state): State<ApiState>,
    Path((symbol, date)): Path<(String, String)>,
    Query(query): Query<MarketDayStatusQuery>,
) -> Result<Json<DataCenterMarketDay>, ApiError> {
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
    Ok(Json(data_center_market_day_status(status)))
}

pub(crate) async fn prepare_replay_dataset(
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
    )?;
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

pub(crate) async fn build_replay_dataset(
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
    )?;
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

pub(crate) async fn validate_replay_dataset(
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
    )?;
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

pub(crate) async fn delete_replay_dataset(
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
    )?;
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

pub(crate) async fn delete_raw_market_data(
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
    )?;
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

pub(crate) async fn job_status(
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

pub(crate) async fn jobs(
    State(state): State<ApiState>,
    Query(query): Query<JobsQuery>,
) -> Result<Json<Vec<crate::jobs::JobRecord>>, ApiError> {
    let active = query.active.unwrap_or(true);
    let limit = query.limit.unwrap_or(50);
    let label = format!("GET /jobs?active={active}&limit={limit}");
    let started_at = log_start(&label);
    let jobs = state
        .ledger
        .store
        .catalog
        .jobs(active, Some(limit))
        .map_err(ApiError::internal)?
        .into_iter()
        .map(api_job_record)
        .collect();
    log_done(&label, started_at);
    Ok(Json(jobs))
}

pub(crate) async fn market_day_jobs(
    State(state): State<ApiState>,
    Path((symbol, date)): Path<(String, String)>,
    Query(query): Query<JobsQuery>,
) -> Result<Json<Vec<crate::jobs::JobRecord>>, ApiError> {
    let date = parse_date(&date)?;
    let active = query.active.unwrap_or(false);
    let limit = query.limit.unwrap_or(25);
    let label = format!("GET /market-days/{symbol}/{date}/jobs?active={active}&limit={limit}");
    let started_at = log_start(&label);
    let market_day = MarketDay::resolve_es(&symbol, date).map_err(ApiError::internal)?;
    let jobs = state
        .ledger
        .store
        .catalog
        .jobs_for_market_day(&market_day.id, active, Some(limit))
        .map_err(ApiError::internal)?
        .into_iter()
        .map(api_job_record)
        .collect();
    log_done(&label, started_at);
    Ok(Json(jobs))
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

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn health_reports_ok() {
        let Json(response) = health().await;
        assert!(response.ok);
        assert_eq!(response.service, "ledger-api");
    }
}
