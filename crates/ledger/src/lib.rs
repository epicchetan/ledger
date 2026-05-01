//! Ledger-facing replay dataset loading.
//!
//! This crate is the boundary a UI, API, or command adapter should use when it
//! needs to inspect or load replay inputs. It delegates persistence and cache
//! behavior to `ledger-store`, hydrates local replay artifacts into domain
//! event stores, and leaves replay simulation to `ledger-replay`.

use anyhow::{Context, Result};
use chrono::NaiveDate;
use ledger_domain::{decode_batches, decode_events, decode_trades, EventStore, MarketDay};
use ledger_store::{
    LedgerStore, LoadedReplayDataset, MarketDayFilter, MarketDayRecord, ObjectStore, R2LedgerStore,
    ReplayDatasetStatus,
};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};

#[derive(Clone)]
pub struct Ledger<S: ObjectStore + 'static> {
    pub store: LedgerStore<S>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplayDataset {
    pub market_day: MarketDay,
    pub events_path: PathBuf,
    pub batches_path: PathBuf,
    pub trades_path: PathBuf,
    pub book_check_path: PathBuf,
}

impl ReplayDataset {
    pub async fn event_store(&self) -> Result<EventStore> {
        let events_bytes = read_artifact("events", &self.events_path).await?;
        let batches_bytes = read_artifact("batches", &self.batches_path).await?;
        let trades_bytes = read_artifact("trades", &self.trades_path).await?;

        let store = EventStore {
            events: decode_events(&events_bytes).with_context(|| {
                format!("decoding events artifact {}", self.events_path.display())
            })?,
            batches: decode_batches(&batches_bytes).with_context(|| {
                format!("decoding batches artifact {}", self.batches_path.display())
            })?,
            trades: decode_trades(&trades_bytes).with_context(|| {
                format!("decoding trades artifact {}", self.trades_path.display())
            })?,
        };

        store
            .validate()
            .with_context(|| format!("validating replay dataset {}", self.market_day.id))?;
        Ok(store)
    }
}

impl Ledger<ledger_store::R2ObjectStore> {
    pub async fn from_env(
        data_dir: impl Into<PathBuf>,
        r2_prefix: impl Into<String>,
    ) -> Result<Self> {
        Ok(Self {
            store: R2LedgerStore::from_env(data_dir, r2_prefix).await?,
        })
    }
}

impl<S: ObjectStore + 'static> Ledger<S> {
    pub fn new(store: LedgerStore<S>) -> Self {
        Self { store }
    }

    pub async fn status(&self, symbol: &str, date: NaiveDate) -> Result<ReplayDatasetStatus> {
        self.store.replay_dataset_status(symbol, date).await
    }

    pub fn list(&self, filter: MarketDayFilter) -> Result<Vec<MarketDayRecord>> {
        self.store.list_market_days(filter)
    }

    pub async fn load_replay_dataset(
        &self,
        symbol: &str,
        date: NaiveDate,
    ) -> Result<ReplayDataset> {
        let dataset: LoadedReplayDataset = self.store.load_replay_dataset(symbol, date).await?;
        Ok(ReplayDataset {
            market_day: dataset.market_day,
            events_path: dataset.events_path,
            batches_path: dataset.batches_path,
            trades_path: dataset.trades_path,
            book_check_path: dataset.book_check_path,
        })
    }
}

async fn read_artifact(kind: &str, path: &Path) -> Result<Vec<u8>> {
    tokio::fs::read(path)
        .await
        .with_context(|| format!("reading {kind} artifact {}", path.display()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use ledger_domain::{
        build_batches, build_trade_index, encode_batches, encode_events, encode_trades, Bbo,
        BookAction, BookSide, MboEvent, PriceTicks, VisibilityProfile,
    };
    use ledger_replay::ReplaySimulator;
    use tempfile::tempdir;

    fn add(ts: u64, seq: u64, side: BookSide, price: i64, size: u32, order_id: u64) -> MboEvent {
        MboEvent::synthetic(
            ts,
            seq,
            BookAction::Add,
            Some(side),
            Some(PriceTicks(price)),
            size,
            order_id,
            true,
        )
    }

    fn replay_dataset(dir: &Path) -> ReplayDataset {
        ReplayDataset {
            market_day: MarketDay::resolve_es(
                "ESH6",
                NaiveDate::from_ymd_opt(2026, 3, 12).unwrap(),
            )
            .unwrap(),
            events_path: dir.join("events.v1.bin"),
            batches_path: dir.join("batches.v1.bin"),
            trades_path: dir.join("trades.v1.bin"),
            book_check_path: dir.join("book_check.v1.json"),
        }
    }

    async fn write_artifacts(dataset: &ReplayDataset, store: &EventStore) {
        tokio::fs::write(&dataset.events_path, encode_events(&store.events))
            .await
            .unwrap();
        tokio::fs::write(&dataset.batches_path, encode_batches(&store.batches))
            .await
            .unwrap();
        tokio::fs::write(&dataset.trades_path, encode_trades(&store.trades))
            .await
            .unwrap();
    }

    fn event_store(events: Vec<MboEvent>) -> EventStore {
        EventStore {
            batches: build_batches(&events),
            trades: build_trade_index(&events),
            events,
        }
    }

    #[tokio::test]
    async fn replay_dataset_hydrates_event_store() {
        let dir = tempdir().unwrap();
        let dataset = replay_dataset(dir.path());
        let expected = event_store(vec![
            add(100, 1, BookSide::Bid, 100, 2, 1),
            add(101, 2, BookSide::Ask, 101, 3, 2),
        ]);
        write_artifacts(&dataset, &expected).await;

        let actual = dataset.event_store().await.unwrap();

        assert_eq!(actual, expected);
    }

    #[tokio::test]
    async fn replay_dataset_reports_corrupt_artifact_path() {
        let dir = tempdir().unwrap();
        let dataset = replay_dataset(dir.path());
        let empty = EventStore::default();
        tokio::fs::write(&dataset.events_path, b"bad")
            .await
            .unwrap();
        tokio::fs::write(&dataset.batches_path, encode_batches(&empty.batches))
            .await
            .unwrap();
        tokio::fs::write(&dataset.trades_path, encode_trades(&empty.trades))
            .await
            .unwrap();

        let err = dataset.event_store().await.unwrap_err();
        let report = format!("{err:#}");

        assert!(report.contains("decoding events artifact"));
        assert!(report.contains(&dataset.events_path.display().to_string()));
    }

    #[tokio::test]
    async fn replay_dataset_rejects_inconsistent_indexes() {
        let dir = tempdir().unwrap();
        let dataset = replay_dataset(dir.path());
        let mut store = event_store(vec![add(100, 1, BookSide::Bid, 100, 2, 1)]);
        store.batches[0].end_idx = 0;
        write_artifacts(&dataset, &store).await;

        let err = dataset.event_store().await.unwrap_err();
        let report = format!("{err:#}");

        assert!(report.contains("validating replay dataset"));
        assert!(report.contains("batch index mismatch"));
    }

    #[tokio::test]
    async fn hydrated_event_store_drives_replay_simulator() {
        let dir = tempdir().unwrap();
        let dataset = replay_dataset(dir.path());
        let store = event_store(vec![
            add(100, 1, BookSide::Bid, 100, 2, 1),
            add(101, 2, BookSide::Ask, 101, 3, 2),
        ]);
        write_artifacts(&dataset, &store).await;

        let hydrated = dataset.event_store().await.unwrap();
        let mut sim =
            ReplaySimulator::new(hydrated, Default::default(), VisibilityProfile::truth());
        let report = sim.run_until(101).unwrap();

        assert_eq!(
            sim.book().bbo(),
            Some(Bbo {
                bid_price: Some(PriceTicks(100)),
                bid_size: 2,
                ask_price: Some(PriceTicks(101)),
                ask_size: 3,
            })
        );
        assert_eq!(report.frames.len(), 2);
    }
}
