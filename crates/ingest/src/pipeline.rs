use anyhow::{anyhow, Context, Result};
use chrono::NaiveDate;
use ledger_domain::{MarketDay, MarketDayStatus, StorageKind};
use ledger_store::{
    IngestStaging, LedgerStore, LoadedReplayDataset, ObjectStore, PutFileRequest, StoredObject,
};
use serde::{Deserialize, Serialize};

use crate::{run_book_check, BookCheckReport, MarketDataProvider, Preprocessor};

#[derive(Clone, Debug)]
pub struct IngestConfig {
    pub dataset: String,
    pub schema: String,
    pub producer: String,
    pub producer_version: String,
}

impl Default for IngestConfig {
    fn default() -> Self {
        Self {
            dataset: "GLBX.MDP3".to_string(),
            schema: "mbo".to_string(),
            producer: "ledger-ingest".to_string(),
            producer_version: option_env!("CARGO_PKG_VERSION")
                .unwrap_or("dev")
                .to_string(),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct IngestReport {
    pub market_day: MarketDay,
    pub raw: StoredObject,
    pub event_store: StoredObject,
    pub batch_index: StoredObject,
    pub trade_index: StoredObject,
    pub book_check: StoredObject,
    pub book_check_report: BookCheckReport,
    pub reused: Vec<String>,
    pub created: Vec<String>,
    pub ready: bool,
}

pub struct IngestPipeline<P, R, S>
where
    P: MarketDataProvider,
    R: Preprocessor,
    S: ObjectStore + 'static,
{
    pub provider: P,
    pub preprocessor: R,
    pub store: LedgerStore<S>,
    pub config: IngestConfig,
}

impl<P, R, S> IngestPipeline<P, R, S>
where
    P: MarketDataProvider,
    R: Preprocessor,
    S: ObjectStore + 'static,
{
    pub fn new(provider: P, preprocessor: R, store: LedgerStore<S>, config: IngestConfig) -> Self {
        Self {
            provider,
            preprocessor,
            store,
            config,
        }
    }

    pub async fn ingest_market_day(&self, symbol: &str, date: NaiveDate) -> Result<IngestReport> {
        let md = MarketDay::resolve_es(symbol, date)?;
        if let Some(record) = self.store.catalog.market_day(&md.id)? {
            if record.ready {
                let loaded = self.store.load_replay_dataset(symbol, date).await?;
                return self
                    .report_from_loaded_replay_dataset(
                        loaded,
                        vec!["ready_replay_dataset".to_string()],
                    )
                    .await;
            }
        }

        let staging = self.store.begin_ingest(&md).await?;
        let result = self.ingest_with_staging(md, &staging).await;
        match result {
            Ok(report) => {
                self.store.finish_ingest(&staging, "ready")?;
                self.store
                    .cleanup_ingest(&report.market_day, &staging)
                    .await?;
                Ok(report)
            }
            Err(err) => {
                self.store.fail_ingest(&staging, &err).ok();
                Err(err)
            }
        }
    }

    async fn ingest_with_staging(
        &self,
        mut md: MarketDay,
        staging: &IngestStaging,
    ) -> Result<IngestReport> {
        let mut reused = Vec::new();
        let mut created = Vec::new();

        self.store
            .mark_market_day_status(&mut md, MarketDayStatus::Downloading, false)?;
        let raw = self
            .ensure_raw(&md, staging, &mut reused, &mut created)
            .await?;

        self.store
            .mark_market_day_status(&mut md, MarketDayStatus::Preprocessing, false)?;
        let preprocessed = self
            .preprocessor
            .preprocess(&staging.raw_path, &staging.artifacts_dir, &self.store.local)
            .await
            .context("preprocessing raw DBN")?;

        let events = self
            .put_artifact(
                &md,
                StorageKind::EventStore,
                &preprocessed.events_path,
                "ledger-events-bin",
                &raw.content_sha256,
                serde_json::json!({"event_count": preprocessed.event_store.events.len()}),
            )
            .await?;
        created.push("event_store".to_string());

        let batches = self
            .put_artifact(
                &md,
                StorageKind::BatchIndex,
                &preprocessed.batches_path,
                "ledger-batches-bin",
                &raw.content_sha256,
                serde_json::json!({"batch_count": preprocessed.event_store.batches.len()}),
            )
            .await?;
        created.push("batch_index".to_string());

        let trades = self
            .put_artifact(
                &md,
                StorageKind::TradeIndex,
                &preprocessed.trades_path,
                "ledger-trades-bin",
                &raw.content_sha256,
                serde_json::json!({"trade_count": preprocessed.event_store.trades.len()}),
            )
            .await?;
        created.push("trade_index".to_string());

        let book_check_report = run_book_check(&preprocessed.event_store)?;
        let book_check_path = staging.artifacts_dir.join("book_check.v1.json");
        self.store
            .local
            .write_atomic(
                &book_check_path,
                &serde_json::to_vec_pretty(&book_check_report)?,
            )
            .await?;
        let book_check = self
            .put_artifact(
                &md,
                StorageKind::BookCheck,
                &book_check_path,
                "json",
                &events.content_sha256,
                serde_json::json!({"input_kind": "event_store"}),
            )
            .await?;
        created.push("book_check".to_string());

        self.store.add_dependency(&events, &raw, "derived_from")?;
        self.store.add_dependency(&batches, &raw, "derived_from")?;
        self.store.add_dependency(&trades, &raw, "derived_from")?;
        self.store
            .add_dependency(&book_check, &events, "validates")?;

        self.store
            .mark_market_day_status(&mut md, MarketDayStatus::Ready, true)?;
        let loaded = self
            .store
            .load_replay_dataset(&md.contract_symbol, md.market_date)
            .await?;
        let mut report = self
            .report_from_loaded_replay_dataset(loaded, reused)
            .await
            .context("building ingest report")?;
        report.created = created;
        Ok(report)
    }

    async fn ensure_raw(
        &self,
        md: &MarketDay,
        staging: &IngestStaging,
        reused: &mut Vec<String>,
        created: &mut Vec<String>,
    ) -> Result<StoredObject> {
        if let Some(raw) = self.store.raw_object(md)? {
            self.store
                .stage_raw_for_ingest(&raw, &staging.raw_path)
                .await?;
            reused.push("raw_dbn_remote".to_string());
            return Ok(raw);
        }

        let legacy_raw_path = self.store.local.legacy_raw_dbn_path(
            &self.config.dataset,
            &self.config.schema,
            &md.root,
            &md.contract_symbol,
            &md.market_date.to_string(),
        );
        if legacy_raw_path.exists() {
            self.store
                .local
                .commit_file_atomic(&legacy_raw_path, &staging.raw_path)
                .await?;
            reused.push("legacy_local_raw_file".to_string());
        } else {
            self.provider
                .download_mbo(md, &staging.raw_path)
                .await
                .context("downloading raw Databento MBO")?;
            created.push("raw_dbn_download".to_string());
        }

        let raw = self
            .store
            .register_raw_object(PutFileRequest {
                market_day: md,
                path: &staging.raw_path,
                kind: StorageKind::RawDbn,
                logical_key: self.store.keys.raw_dbn_logical_key(
                    md,
                    &self.config.dataset,
                    &self.config.schema,
                ),
                format: "dbn.zst",
                schema_version: 1,
                input_sha256: "",
                producer: Some(&self.config.producer),
                producer_version: Some(&self.config.producer_version),
                source_provider: Some("databento"),
                source_dataset: Some(&self.config.dataset),
                source_schema: Some(&self.config.schema),
                source_symbol: Some(&md.contract_symbol),
                metadata_json: serde_json::json!({
                    "data_start_ns": md.data_start_ns,
                    "data_end_ns": md.data_end_ns,
                }),
            })
            .await?;
        Ok(raw)
    }

    async fn put_artifact(
        &self,
        md: &MarketDay,
        kind: StorageKind,
        path: &std::path::Path,
        format: &str,
        input_sha256: &str,
        metadata_json: serde_json::Value,
    ) -> Result<StoredObject> {
        self.store
            .register_replay_artifact(PutFileRequest {
                market_day: md,
                path,
                kind: kind.clone(),
                logical_key: self.store.keys.artifact_logical_key(md, kind.as_str(), 1),
                format,
                schema_version: 1,
                input_sha256,
                producer: Some(&self.config.producer),
                producer_version: Some(&self.config.producer_version),
                source_provider: None,
                source_dataset: None,
                source_schema: None,
                source_symbol: None,
                metadata_json,
            })
            .await
    }

    async fn report_from_loaded_replay_dataset(
        &self,
        loaded: LoadedReplayDataset,
        reused: Vec<String>,
    ) -> Result<IngestReport> {
        let raw = self
            .store
            .raw_object(&loaded.market_day)?
            .ok_or_else(|| anyhow!("ready market day missing raw_dbn"))?;
        let event_store = object_of(&loaded.objects, StorageKind::EventStore)?;
        let batch_index = object_of(&loaded.objects, StorageKind::BatchIndex)?;
        let trade_index = object_of(&loaded.objects, StorageKind::TradeIndex)?;
        let book_check = object_of(&loaded.objects, StorageKind::BookCheck)?;
        let book_check_report =
            serde_json::from_slice(&tokio::fs::read(&loaded.book_check_path).await?)?;
        Ok(IngestReport {
            market_day: loaded.market_day,
            raw,
            event_store,
            batch_index,
            trade_index,
            book_check,
            book_check_report,
            reused,
            created: Vec::new(),
            ready: true,
        })
    }
}

fn object_of(objects: &[StoredObject], kind: StorageKind) -> Result<StoredObject> {
    objects
        .iter()
        .find(|object| object.kind == kind)
        .cloned()
        .ok_or_else(|| anyhow!("loaded replay dataset missing {}", kind.as_str()))
}

#[cfg(test)]
mod tests {
    use super::*;
    use async_trait::async_trait;
    use ledger_domain::{BookAction, BookSide, MboEvent, PriceTicks};
    use ledger_store::{LocalStore, MemoryObjectStore, ObjectKeyBuilder};
    use std::path::Path;
    use std::sync::{Arc, Mutex};

    use crate::SyntheticPreprocessor;

    #[derive(Clone, Default)]
    struct FakeProvider {
        calls: Arc<Mutex<usize>>,
    }

    #[async_trait]
    impl MarketDataProvider for FakeProvider {
        async fn download_mbo(&self, _market_day: &MarketDay, dest: &Path) -> Result<()> {
            *self.calls.lock().unwrap() += 1;
            if let Some(parent) = dest.parent() {
                tokio::fs::create_dir_all(parent).await?;
            }
            tokio::fs::write(dest, b"fake raw").await?;
            Ok(())
        }
    }

    #[tokio::test]
    async fn full_ingest_is_resumable_with_fake_provider() {
        let dir = tempfile::tempdir().unwrap();
        let remote = Arc::new(MemoryObjectStore::new("test"));
        let store = LedgerStore::new(
            LocalStore::new(dir.path()),
            remote,
            ObjectKeyBuilder::default(),
        );
        let provider = FakeProvider::default();
        let events = vec![
            MboEvent::synthetic(
                100,
                1,
                BookAction::Add,
                Some(BookSide::Bid),
                Some(PriceTicks(100)),
                10,
                1,
                true,
            ),
            MboEvent::synthetic(
                101,
                2,
                BookAction::Trade,
                Some(BookSide::Ask),
                Some(PriceTicks(100)),
                2,
                0,
                true,
            ),
        ];
        let pipeline = IngestPipeline::new(
            provider.clone(),
            SyntheticPreprocessor { events },
            store,
            IngestConfig::default(),
        );
        let date = NaiveDate::from_ymd_opt(2026, 3, 12).unwrap();
        let first = pipeline.ingest_market_day("ESH6", date).await.unwrap();
        assert!(first.ready);
        assert_eq!(*provider.calls.lock().unwrap(), 1);
        assert!(!dir
            .path()
            .join("sessions/ES/ESH6/2026-03-12/raw.dbn.zst")
            .exists());

        let second = pipeline.ingest_market_day("ESH6", date).await.unwrap();
        assert!(second.reused.contains(&"ready_replay_dataset".to_string()));
        assert_eq!(*provider.calls.lock().unwrap(), 1);
    }
}
