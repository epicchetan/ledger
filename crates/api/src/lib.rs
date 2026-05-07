mod dto;
mod error;
mod routes;
mod state;
mod time;

use axum::{routing::get, Router};
use std::net::SocketAddr;
use tower_http::cors::CorsLayer;

pub use dto::*;
pub use error::ApiError;
pub use state::ApiState;

pub fn router(state: ApiState) -> Router {
    Router::new()
        .route("/health", get(routes::health))
        .route("/store/objects", get(routes::list_store_objects))
        .route(
            "/store/objects/{id}",
            get(routes::get_store_object).delete(routes::delete_store_object),
        )
        .layer(CorsLayer::permissive())
        .with_state(state)
}

pub async fn serve(state: ApiState, addr: SocketAddr) -> anyhow::Result<()> {
    let listener = tokio::net::TcpListener::bind(addr).await?;
    axum::serve(listener, router(state)).await?;
    Ok(())
}
