use anyhow::Context;
use ledger_api::{serve, ApiState};
use std::{env, net::SocketAddr};
use store::R2Store;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenvy::dotenv().ok();

    let data_dir = env::var("LEDGER_DATA_DIR").unwrap_or_else(|_| "data".to_string());
    let addr: SocketAddr = env::var("LEDGER_API_ADDR")
        .unwrap_or_else(|_| "127.0.0.1:3001".to_string())
        .parse()
        .context("parsing LEDGER_API_ADDR")?;

    let store = R2Store::from_env(data_dir).await?;
    let state = ApiState { store };

    eprintln!("[ledger-api] listening on http://{addr}");
    serve(state, addr).await
}
