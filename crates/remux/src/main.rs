mod error;
mod hydrate;
mod jobs;
mod methods;
mod rpc;

use anyhow::Result;
use methods::LedgerRemux;
use rpc::{DispatchFn, DispatchFuture, Request};
use std::env;
use std::sync::Arc;
use store::R2Store;

#[tokio::main]
async fn main() {
    if let Err(error) = run().await {
        eprintln!("[ledger-remux] {error:#}");
    }
}

async fn run() -> Result<()> {
    dotenvy::dotenv().ok();
    let data_dir = env::var("LEDGER_DATA_DIR").unwrap_or_else(|_| "data".to_string());
    let store = R2Store::from_env(&data_dir).await?;

    rpc::serve_stdio(|output_tx| {
        let methods = Arc::new(LedgerRemux::new(store, output_tx));
        Arc::new(move |request: Request| {
            let methods = methods.clone();
            Box::pin(async move { methods.handle(request).await }) as DispatchFuture
        }) as DispatchFn
    })
    .await
}
