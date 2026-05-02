use anyhow::Context;
use ledger::Ledger;
use ledger_api::{serve, ApiState};
use std::{env, net::SocketAddr};

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenvy::dotenv().ok();

    let data_dir = env::var("LEDGER_DATA_DIR").unwrap_or_else(|_| "data".to_string());
    let r2_prefix = env::var("LEDGER_R2_PREFIX").unwrap_or_else(|_| "ledger/v1".to_string());
    let addr: SocketAddr = env::var("LEDGER_API_ADDR")
        .unwrap_or_else(|_| "127.0.0.1:3001".to_string())
        .parse()
        .context("parsing LEDGER_API_ADDR")?;

    let ledger = Ledger::from_env(data_dir, r2_prefix).await?;
    let stale = ledger
        .store
        .catalog
        .mark_stale_jobs_failed("API restarted before job completed")?;
    if stale > 0 {
        eprintln!("[ledger-api] marked {stale} stale job(s) failed");
    }
    let state = ApiState { ledger };

    eprintln!("[ledger-api] listening on http://{addr}");
    serve(state, addr).await
}
