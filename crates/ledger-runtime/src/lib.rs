//! Runtime-facing session preparation.
//!
//! This crate is the boundary a UI, API, or command adapter should use when it
//! needs to inspect or load replay inputs. It delegates persistence and cache
//! behavior to `ledger-store` and returns local artifact paths suitable for
//! replay. It does not own Databento ingest, order-book logic, or the replay
//! simulation engine itself.

use anyhow::Result;
use chrono::NaiveDate;
use ledger_core::MarketDay;
use ledger_store::{
    LedgerStore, LoadedSession, MarketDayFilter, MarketDayRecord, ObjectStore, R2LedgerStore,
    SessionStatus,
};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;

#[derive(Clone)]
pub struct Runtime<S: ObjectStore + 'static> {
    pub store: LedgerStore<S>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplayInputs {
    pub market_day: MarketDay,
    pub events_path: PathBuf,
    pub batches_path: PathBuf,
    pub trades_path: PathBuf,
    pub book_check_path: PathBuf,
}

impl Runtime<ledger_store::R2ObjectStore> {
    pub async fn from_env(
        data_dir: impl Into<PathBuf>,
        r2_prefix: impl Into<String>,
    ) -> Result<Self> {
        Ok(Self {
            store: R2LedgerStore::from_env(data_dir, r2_prefix).await?,
        })
    }
}

impl<S: ObjectStore + 'static> Runtime<S> {
    pub fn new(store: LedgerStore<S>) -> Self {
        Self { store }
    }

    pub async fn status(&self, symbol: &str, date: NaiveDate) -> Result<SessionStatus> {
        self.store.session_status(symbol, date).await
    }

    pub fn list(&self, filter: MarketDayFilter) -> Result<Vec<MarketDayRecord>> {
        self.store.list_market_days(filter)
    }

    pub async fn load_session(&self, symbol: &str, date: NaiveDate) -> Result<ReplayInputs> {
        let session: LoadedSession = self.store.load_session(symbol, date).await?;
        Ok(ReplayInputs {
            market_day: session.market_day,
            events_path: session.events_path,
            batches_path: session.batches_path,
            trades_path: session.trades_path,
            book_check_path: session.book_check_path,
        })
    }
}
