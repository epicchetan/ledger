//! Ledger-facing replay dataset loading.
//!
//! This crate is the boundary a UI, API, or command adapter should use when it
//! needs to inspect or load replay inputs. It delegates persistence and cache
//! behavior to `ledger-store`, materializes replay artifacts into domain
//! event stores, validates replay readiness, and composes replay simulator
//! smoke tests while keeping transport concerns in CLI/API adapters.

use anyhow::{Context, Result};
use chrono::NaiveDate;
use ledger_domain::{decode_batches, decode_events, decode_trades, EventStore, MarketDay};
use ledger_ingest::{
    DatabentoProvider, DbnPreprocessor, IngestConfig, IngestPipeline, IngestProgressEvent,
    IngestProgressSink, IngestReport,
};
use ledger_store::{
    DeleteRawMarketDataReport, DeleteReplayDatasetCacheReport, DeleteReplayDatasetReport,
    LedgerStore, LoadedReplayDataset, MarketDayFilter, MarketDayRecord, R2LedgerStore,
    ReplayDatasetCacheStatus, ReplayDatasetStatus,
};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Instant;

mod progress;
pub mod projection;
mod session;
mod validation;

use projection::{base_projection_registry, ProjectionRegistry};

pub use ledger_replay::{ReplayFeedConfig, ReplayFeedMode};
pub use ledger_store::ObjectStore;
pub use progress::*;
pub use session::*;
pub use validation::*;

#[derive(Clone)]
pub struct Ledger<S: ObjectStore + 'static> {
    pub store: LedgerStore<S>,
    projection_registry: ProjectionRegistry,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ReplayDataset {
    pub replay_dataset_id: String,
    pub market_day: MarketDay,
    pub events_path: PathBuf,
    pub batches_path: PathBuf,
    pub trades_path: PathBuf,
    pub book_check_path: PathBuf,
}

impl ReplayDataset {
    pub async fn event_store(&self) -> Result<EventStore> {
        self.event_store_with_progress(None).await
    }

    pub async fn event_store_with_progress(
        &self,
        progress: Option<&LedgerProgress>,
    ) -> Result<EventStore> {
        if let Some(progress) = progress {
            progress.step(format!(
                "reading events artifact {}",
                self.events_path.display()
            ));
        }
        let started_at = Instant::now();
        let events_bytes = read_artifact("events", &self.events_path).await?;
        if let Some(progress) = progress {
            progress.done("events artifact read", started_at);
            progress.step(format!(
                "reading batches artifact {}",
                self.batches_path.display()
            ));
        }
        let started_at = Instant::now();
        let batches_bytes = read_artifact("batches", &self.batches_path).await?;
        if let Some(progress) = progress {
            progress.done("batches artifact read", started_at);
            progress.step(format!(
                "reading trades artifact {}",
                self.trades_path.display()
            ));
        }
        let started_at = Instant::now();
        let trades_bytes = read_artifact("trades", &self.trades_path).await?;

        if let Some(progress) = progress {
            progress.done("trades artifact read", started_at);
            progress.step("decoding event artifact");
        }
        let started_at = Instant::now();
        let events = decode_events(&events_bytes)
            .with_context(|| format!("decoding events artifact {}", self.events_path.display()))?;
        if let Some(progress) = progress {
            progress.done(format!("decoded {} events", events.len()), started_at);
            progress.step("decoding batch artifact");
        }
        let started_at = Instant::now();
        let batches = decode_batches(&batches_bytes).with_context(|| {
            format!("decoding batches artifact {}", self.batches_path.display())
        })?;
        if let Some(progress) = progress {
            progress.done(format!("decoded {} batches", batches.len()), started_at);
            progress.step("decoding trade artifact");
        }
        let started_at = Instant::now();
        let trades = decode_trades(&trades_bytes)
            .with_context(|| format!("decoding trades artifact {}", self.trades_path.display()))?;
        if let Some(progress) = progress {
            progress.done(format!("decoded {} trades", trades.len()), started_at);
            progress.step("validating event, batch, and trade indexes");
        }
        let started_at = Instant::now();
        let store = EventStore {
            events,
            batches,
            trades,
        };

        store
            .validate()
            .with_context(|| format!("validating replay dataset {}", self.market_day.id))?;
        if let Some(progress) = progress {
            progress.done("event indexes validated", started_at);
        }
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
            projection_registry: base_projection_registry()?,
        })
    }
}

impl<S: ObjectStore + 'static> Ledger<S> {
    pub fn new(store: LedgerStore<S>) -> Self {
        Self {
            store,
            projection_registry: base_projection_registry()
                .expect("base projection registry must be valid"),
        }
    }

    pub fn with_projection_registry(
        store: LedgerStore<S>,
        projection_registry: ProjectionRegistry,
    ) -> Self {
        Self {
            store,
            projection_registry,
        }
    }

    pub fn projection_registry(&self) -> &ProjectionRegistry {
        &self.projection_registry
    }

    pub async fn status(&self, symbol: &str, date: NaiveDate) -> Result<ReplayDatasetStatus> {
        self.store.replay_dataset_status(symbol, date).await
    }

    pub async fn verified_status(
        &self,
        symbol: &str,
        date: NaiveDate,
    ) -> Result<ReplayDatasetStatus> {
        self.store
            .verified_replay_dataset_status(symbol, date)
            .await
    }

    pub async fn list(&self, filter: MarketDayFilter) -> Result<Vec<MarketDayRecord>> {
        self.store.list_market_days(filter).await
    }

    pub async fn load_replay_dataset(
        &self,
        symbol: &str,
        date: NaiveDate,
    ) -> Result<ReplayDataset> {
        let dataset: LoadedReplayDataset = self.store.load_replay_dataset(symbol, date).await?;
        Ok(ReplayDataset {
            replay_dataset_id: dataset.replay_dataset_id,
            market_day: dataset.market_day,
            events_path: dataset.events_path,
            batches_path: dataset.batches_path,
            trades_path: dataset.trades_path,
            book_check_path: dataset.book_check_path,
        })
    }

    pub async fn load_cached_replay_dataset(
        &self,
        symbol: &str,
        date: NaiveDate,
    ) -> Result<ReplayDataset> {
        let dataset: LoadedReplayDataset =
            self.store.load_replay_dataset_cached(symbol, date).await?;
        Ok(ReplayDataset {
            replay_dataset_id: dataset.replay_dataset_id,
            market_day: dataset.market_day,
            events_path: dataset.events_path,
            batches_path: dataset.batches_path,
            trades_path: dataset.trades_path,
            book_check_path: dataset.book_check_path,
        })
    }

    pub async fn replay_cache_status(
        &self,
        symbol: &str,
        date: NaiveDate,
    ) -> Result<ReplayDatasetCacheStatus> {
        self.store.replay_cache_status(symbol, date).await
    }

    pub async fn delete_replay_dataset_cache(
        &self,
        symbol: &str,
        date: NaiveDate,
    ) -> Result<DeleteReplayDatasetCacheReport> {
        self.store.delete_replay_dataset_cache(symbol, date).await
    }

    pub async fn delete_remote_replay_dataset(
        &self,
        symbol: &str,
        date: NaiveDate,
        include_raw: bool,
    ) -> Result<DeleteReplayDatasetReport> {
        self.store
            .delete_remote_replay_dataset(symbol, date, include_raw)
            .await
    }

    pub async fn delete_raw_market_data(
        &self,
        symbol: &str,
        date: NaiveDate,
        cascade: bool,
    ) -> Result<DeleteRawMarketDataReport> {
        self.store
            .delete_raw_market_data(symbol, date, cascade)
            .await
    }
}

impl Ledger<ledger_store::R2ObjectStore> {
    pub async fn ingest_market_day(&self, symbol: &str, date: NaiveDate) -> Result<IngestReport> {
        self.ingest_market_day_with_progress(symbol, date, None)
            .await
    }

    pub async fn ingest_market_day_with_progress(
        &self,
        symbol: &str,
        date: NaiveDate,
        progress_sink: Option<LedgerProgressSink>,
    ) -> Result<IngestReport> {
        let pipeline = IngestPipeline::new(
            DatabentoProvider,
            DbnPreprocessor,
            self.store.clone(),
            IngestConfig::default(),
        );
        pipeline
            .ingest_market_day_with_progress(
                symbol,
                date,
                progress_sink.map(ingest_progress_adapter),
            )
            .await
    }

    pub async fn prepare_replay_dataset(
        &self,
        request: PrepareReplayDatasetRequest,
    ) -> Result<PrepareReplayDatasetReport> {
        self.prepare_replay_dataset_with_progress(request, None)
            .await
    }

    pub async fn prepare_replay_dataset_with_progress(
        &self,
        request: PrepareReplayDatasetRequest,
        progress_sink: Option<LedgerProgressSink>,
    ) -> Result<PrepareReplayDatasetReport> {
        let progress = LedgerProgress::new(progress_sink.clone());
        progress.step(format!(
            "preparing replay dataset {} {}",
            request.symbol, request.market_date
        ));
        if request.rebuild_replay {
            progress.step(
                "rebuild requested; deleting existing replay dataset while preserving raw data",
            );
            self.delete_remote_replay_dataset(&request.symbol, request.market_date, false)
                .await
                .with_context(|| {
                    format!(
                        "deleting existing replay dataset {} {}",
                        request.symbol, request.market_date
                    )
                })?;
        }
        progress.step(
            "running ingest/preprocess pipeline; this may materialize raw data and rebuild artifacts",
        );
        let ingest_started_at = Instant::now();
        let ingest = self
            .ingest_market_day_with_progress(
                &request.symbol,
                request.market_date,
                progress_sink.clone(),
            )
            .await
            .with_context(|| {
                format!(
                    "ingesting market day {} {}",
                    request.symbol, request.market_date
                )
            })?;
        progress.done("ingest/preprocess pipeline completed", ingest_started_at);
        progress.step("ingest and preprocessing completed; validating prepared replay dataset");
        let validation = self
            .validate_replay_dataset_with_progress(
                ValidateReplayDatasetRequest {
                    symbol: request.symbol,
                    market_date: request.market_date,
                    trigger: if request.rebuild_replay {
                        ValidationTrigger::Rebuild
                    } else {
                        ValidationTrigger::Prepare
                    },
                    // Prepare should establish readiness, not perform an audit
                    // book-check pass unless explicitly requested later.
                    skip_book_check: request.skip_book_check,
                    replay_batches: request.replay_batches,
                    replay_all: request.replay_all,
                },
                progress_sink,
            )
            .await?;
        Ok(PrepareReplayDatasetReport { ingest, validation })
    }
}

async fn read_artifact(kind: &str, path: &Path) -> Result<Vec<u8>> {
    tokio::fs::read(path)
        .await
        .with_context(|| format!("reading {kind} artifact {}", path.display()))
}

fn ingest_progress_adapter(sink: LedgerProgressSink) -> IngestProgressSink {
    Arc::new(move |event| match event {
        IngestProgressEvent::Step { message } => sink(LedgerProgressEvent::Step { message }),
        IngestProgressEvent::Done {
            message,
            elapsed_ms,
        } => sink(LedgerProgressEvent::Done {
            message,
            elapsed_ms,
        }),
    })
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
            replay_dataset_id: "test-replay-dataset".to_string(),
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

    #[test]
    fn ledger_new_contains_base_projection_registry() {
        let dir = tempdir().unwrap();
        let store = LedgerStore::new(
            ledger_store::LocalStore::new(dir.path()),
            Arc::new(ledger_store::MemoryObjectStore::new("test")),
            ledger_store::ObjectKeyBuilder::default(),
        );

        let ledger = Ledger::new(store);
        let ids = ledger
            .projection_registry()
            .list_manifests()
            .iter()
            .map(|manifest| manifest.id.as_str().to_string())
            .collect::<Vec<_>>();

        assert_eq!(ids, vec!["cursor", "bbo", "canonical_trades", "bars"]);
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
