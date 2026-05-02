mod dto;
mod error;
mod jobs;
mod presenters;
mod routes;
mod state;
mod time;

use axum::{
    routing::{delete, get, post},
    Router,
};
use std::net::SocketAddr;
use tower_http::cors::CorsLayer;

pub use dto::*;
pub use error::ApiError;
pub use jobs::{progress_sink_for_job, JobKind, JobRecord, JobStatus, JobTarget};
pub use state::{ApiLedger, ApiState};

pub fn router(state: ApiState) -> Router {
    Router::new()
        .route("/health", get(routes::health))
        .route("/market-days", get(routes::list_market_days))
        .route(
            "/market-days/{symbol}/{date}",
            get(routes::market_day_status),
        )
        .route(
            "/market-days/{symbol}/{date}/jobs",
            get(routes::market_day_jobs),
        )
        .route(
            "/market-days/{symbol}/{date}/prepare",
            post(routes::prepare_replay_dataset),
        )
        .route(
            "/market-days/{symbol}/{date}/replay/build",
            post(routes::build_replay_dataset),
        )
        .route(
            "/market-days/{symbol}/{date}/replay/validate",
            post(routes::validate_replay_dataset),
        )
        .route(
            "/market-days/{symbol}/{date}/replay",
            delete(routes::delete_replay_dataset),
        )
        .route(
            "/market-days/{symbol}/{date}/replay/cache",
            delete(routes::delete_replay_dataset_cache),
        )
        .route(
            "/market-days/{symbol}/{date}/raw",
            delete(routes::delete_raw_market_data),
        )
        .route("/jobs", get(routes::jobs))
        .route("/jobs/{id}", get(routes::job_status))
        .layer(CorsLayer::permissive())
        .with_state(state)
}

pub async fn serve(state: ApiState, addr: SocketAddr) -> anyhow::Result<()> {
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, router(state)).await?;
    Ok(())
}
