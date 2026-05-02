use anyhow::{anyhow, Context, Result};
use chrono::NaiveDate;
use ledger_domain::{now_ns, MarketDay, MarketDayStatus, StorageKind};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::{
    is_replay_artifact, required_replay_kinds, sha256_file, CleanupTmpReport, LoadedReplayDataset,
    LocalStore, MarketDayFilter, MarketDayRecord, ObjectKeyBuilder, ObjectMetadata, ObjectStore,
    R2Config, R2ObjectStore, ReplayDatasetObjectStatus, ReplayDatasetRecordStatus,
    ReplayDatasetStatus, SqliteCatalog, StoredObject, ValidationMode, ValidationReportStatus,
};

#[derive(Clone)]
pub struct LedgerStore<S: ObjectStore + 'static> {
    pub local: LocalStore,
    pub remote: Arc<S>,
    pub keys: ObjectKeyBuilder,
    pub catalog: SqliteCatalog,
}

pub type R2LedgerStore = LedgerStore<R2ObjectStore>;

#[derive(Clone, Debug)]
pub struct PutFileRequest<'a> {
    pub market_day: &'a MarketDay,
    pub path: &'a Path,
    pub kind: StorageKind,
    pub logical_key: String,
    pub format: &'a str,
    pub schema_version: i64,
    pub input_sha256: &'a str,
    pub producer: Option<&'a str>,
    pub producer_version: Option<&'a str>,
    pub source_provider: Option<&'a str>,
    pub source_dataset: Option<&'a str>,
    pub source_schema: Option<&'a str>,
    pub source_symbol: Option<&'a str>,
    pub metadata_json: serde_json::Value,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct IngestStaging {
    pub run_id: i64,
    pub run_label: String,
    pub dir: PathBuf,
    pub raw_path: PathBuf,
    pub artifacts_dir: PathBuf,
}

#[derive(Clone, Debug, Serialize, Deserialize, Default)]
pub struct DeleteReplayDatasetReport {
    pub market_day_id: String,
    pub deleted_remote_keys: Vec<String>,
    pub deleted_object_records: usize,
    pub bytes_deleted: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize, Default)]
pub struct DeleteRawMarketDataReport {
    pub market_day_id: String,
    pub deleted_remote_key: Option<String>,
    pub bytes_deleted: u64,
}

impl R2LedgerStore {
    pub async fn from_env(
        data_dir: impl Into<PathBuf>,
        r2_prefix: impl Into<String>,
    ) -> Result<Self> {
        let remote = Arc::new(R2ObjectStore::new(R2Config::from_env()?).await?);
        LedgerStore::open(
            LocalStore::new(data_dir),
            remote,
            ObjectKeyBuilder::new(r2_prefix),
        )
    }
}

impl<S: ObjectStore + 'static> LedgerStore<S> {
    pub fn open(local: LocalStore, remote: Arc<S>, keys: ObjectKeyBuilder) -> Result<Self> {
        let catalog = SqliteCatalog::open(local.catalog_path())?;
        Ok(Self {
            local,
            remote,
            keys,
            catalog,
        })
    }

    pub fn new(local: LocalStore, remote: Arc<S>, keys: ObjectKeyBuilder) -> Self {
        Self::open(local, remote, keys).expect("opening test ledger store")
    }

    pub async fn list_market_days(&self, filter: MarketDayFilter) -> Result<Vec<MarketDayRecord>> {
        self.catalog.list_market_days(&filter)
    }

    pub async fn replay_dataset_status(
        &self,
        symbol: &str,
        date: NaiveDate,
    ) -> Result<ReplayDatasetStatus> {
        self.replay_dataset_status_inner(symbol, date, false).await
    }

    pub async fn verified_replay_dataset_status(
        &self,
        symbol: &str,
        date: NaiveDate,
    ) -> Result<ReplayDatasetStatus> {
        self.replay_dataset_status_inner(symbol, date, true).await
    }

    async fn catalog_market_day(&self, symbol: &str, date: NaiveDate) -> Result<MarketDay> {
        let resolved = MarketDay::resolve_es(symbol, date)?;
        Ok(self
            .catalog
            .market_day(&resolved.id)?
            .map(|record| record.market_day)
            .unwrap_or(resolved))
    }

    pub async fn record_validation_report(
        &self,
        _md: &MarketDay,
        replay_dataset_id: &str,
        mode: ValidationMode,
        status: ValidationReportStatus,
        report_json: serde_json::Value,
    ) -> Result<()> {
        self.catalog.insert_validation_report(
            replay_dataset_id,
            mode.clone(),
            status.clone(),
            report_json.clone(),
        )?;
        Ok(())
    }

    async fn replay_dataset_status_inner(
        &self,
        symbol: &str,
        date: NaiveDate,
        verify_hashes: bool,
    ) -> Result<ReplayDatasetStatus> {
        let md = self.catalog_market_day(symbol, date).await?;
        let Some(record) = self.catalog.market_day(&md.id)? else {
            return Ok(ReplayDatasetStatus {
                market_day: md,
                catalog_found: false,
                raw: None,
                replay_dataset: None,
                replay_artifacts_available: false,
                replay_objects_valid: false,
                last_validation: None,
                objects: Vec::new(),
            });
        };
        let raw = record.raw;
        let replay_dataset = record.replay_dataset;
        let validation = record.last_validation;

        let Some(replay_dataset) = replay_dataset else {
            return Ok(ReplayDatasetStatus {
                market_day: record.market_day,
                catalog_found: true,
                raw,
                replay_dataset: None,
                replay_artifacts_available: false,
                replay_objects_valid: false,
                last_validation: validation,
                objects: Vec::new(),
            });
        };

        let mut objects = Vec::new();
        let mut replay_artifacts_available = true;
        let mut replay_objects_valid = true;
        let artifact_rows = self.catalog.replay_dataset_artifacts(&replay_dataset.id)?;

        for kind in required_replay_kinds() {
            let object = artifact_rows
                .iter()
                .find(|object| object.kind == kind)
                .cloned();
            let Some(object) = object else {
                replay_artifacts_available = false;
                replay_objects_valid = false;
                objects.push(ReplayDatasetObjectStatus {
                    kind,
                    remote_key: None,
                    size_bytes: None,
                    content_sha256: None,
                    object_valid: false,
                });
                continue;
            };
            let remote_valid = if verify_hashes {
                self.remote_object_valid(&object).await?
            } else {
                true
            };
            replay_objects_valid &= remote_valid;
            objects.push(ReplayDatasetObjectStatus {
                kind,
                remote_key: Some(object.remote_key),
                size_bytes: Some(object.size_bytes),
                content_sha256: Some(object.content_sha256),
                object_valid: remote_valid,
            });
        }

        Ok(ReplayDatasetStatus {
            market_day: record.market_day,
            catalog_found: true,
            raw,
            replay_dataset: Some(replay_dataset),
            replay_artifacts_available,
            replay_objects_valid,
            last_validation: validation,
            objects,
        })
    }

    pub async fn load_replay_dataset(
        &self,
        symbol: &str,
        date: NaiveDate,
    ) -> Result<LoadedReplayDataset> {
        let run_label = format!("manual-{}", now_ns());
        self.stage_replay_dataset(symbol, date, &run_label).await
    }

    pub async fn stage_replay_dataset(
        &self,
        symbol: &str,
        date: NaiveDate,
        run_label: &str,
    ) -> Result<LoadedReplayDataset> {
        let md = self.catalog_market_day(symbol, date).await?;
        let replay_dataset = self
            .catalog
            .replay_dataset(&md.id)?
            .ok_or_else(|| anyhow!("market day {} has no replay dataset", md.id))?;
        if replay_dataset.status != ReplayDatasetRecordStatus::Available {
            return Err(anyhow!(
                "replay dataset {} is not available",
                replay_dataset.id
            ));
        }
        let artifacts = self.catalog.replay_dataset_artifacts(&replay_dataset.id)?;

        let events = self
            .require_replay_object(&artifacts, &replay_dataset.id, StorageKind::EventStore)
            .await?;
        let batches = self
            .require_replay_object(&artifacts, &replay_dataset.id, StorageKind::BatchIndex)
            .await?;
        let trades = self
            .require_replay_object(&artifacts, &replay_dataset.id, StorageKind::TradeIndex)
            .await?;
        let book_check = self
            .require_replay_object(&artifacts, &replay_dataset.id, StorageKind::BookCheck)
            .await?;

        let events_path = self
            .stage_replay_dataset_artifact(&md, run_label, &events)
            .await?;
        let batches_path = self
            .stage_replay_dataset_artifact(&md, run_label, &batches)
            .await?;
        let trades_path = self
            .stage_replay_dataset_artifact(&md, run_label, &trades)
            .await?;
        let book_check_path = self
            .stage_replay_dataset_artifact(&md, run_label, &book_check)
            .await?;

        self.catalog.touch_market_day(&md.id).ok();

        Ok(LoadedReplayDataset {
            replay_dataset_id: replay_dataset.id,
            market_day: md,
            events_path,
            batches_path,
            trades_path,
            book_check_path,
            objects: vec![events, batches, trades, book_check],
        })
    }

    pub async fn begin_ingest(&self, md: &MarketDay) -> Result<IngestStaging> {
        let run_id = self.catalog.create_ingest_run(md)?;
        let run_label = format!("{}-{run_id}", self.local.new_ingest_run_label());
        let dir = self.local.ingest_run_dir(md, &run_label);
        let raw_path = self.local.ingest_raw_path(md, &run_label);
        let artifacts_dir = self.local.ingest_artifacts_dir(md, &run_label);
        tokio::fs::create_dir_all(&artifacts_dir).await?;
        Ok(IngestStaging {
            run_id,
            run_label,
            dir,
            raw_path,
            artifacts_dir,
        })
    }

    pub fn finish_ingest(&self, staging: &IngestStaging, status: &str) -> Result<()> {
        self.catalog.finish_ingest_run(staging.run_id, status, None)
    }

    pub fn fail_ingest(&self, staging: &IngestStaging, err: &anyhow::Error) -> Result<()> {
        self.catalog
            .finish_ingest_run(staging.run_id, "error", Some(&err.to_string()))
    }

    pub async fn cleanup_ingest(&self, md: &MarketDay, staging: &IngestStaging) -> Result<()> {
        self.local.cleanup_ingest_run(md, &staging.run_label).await
    }

    pub async fn cleanup_staged_replay_dataset(
        &self,
        md: &MarketDay,
        run_label: &str,
    ) -> Result<()> {
        self.local.cleanup_validate_run(md, run_label).await
    }

    pub fn cleanup_tmp(&self, older_than: Option<std::time::Duration>) -> Result<CleanupTmpReport> {
        self.local.cleanup_tmp(older_than)
    }

    pub async fn delete_remote_replay_dataset(
        &self,
        symbol: &str,
        date: NaiveDate,
        include_raw: bool,
    ) -> Result<DeleteReplayDatasetReport> {
        let md = self.catalog_market_day(symbol, date).await?;
        let Some(replay_dataset) = self.catalog.replay_dataset(&md.id)? else {
            return Ok(DeleteReplayDatasetReport {
                market_day_id: md.id,
                ..Default::default()
            });
        };

        let mut report = DeleteReplayDatasetReport {
            market_day_id: md.id.clone(),
            ..Default::default()
        };
        let mut objects_to_delete = self.catalog.replay_dataset_artifacts(&replay_dataset.id)?;
        if include_raw {
            if let Some(raw) = self.catalog.raw_market_data(&md.id)? {
                objects_to_delete.push(raw.object);
            }
        }

        for object in &objects_to_delete {
            self.remote
                .delete(&object.remote_key)
                .await
                .with_context(|| format!("deleting remote object {}", object.remote_key))?;
            report.deleted_remote_keys.push(object.remote_key.clone());
            report.bytes_deleted += object.size_bytes.max(0) as u64;
        }

        let replay_keys = self
            .catalog
            .remove_replay_dataset(&md.id)
            .unwrap_or_default();
        report.deleted_object_records = replay_keys.len();
        if include_raw {
            if self.catalog.remove_raw_market_data(&md.id)?.is_some() {
                report.deleted_object_records += 1;
            }
        }

        Ok(report)
    }

    pub async fn delete_raw_market_data(
        &self,
        symbol: &str,
        date: NaiveDate,
        cascade: bool,
    ) -> Result<DeleteRawMarketDataReport> {
        let md = self.catalog_market_day(symbol, date).await?;
        if self.catalog.replay_dataset(&md.id)?.is_some() && !cascade {
            return Err(anyhow!(
                "cannot delete raw market data for {} while a replay dataset exists; delete replay first or request cascade",
                md.id
            ));
        }
        if cascade {
            self.delete_remote_replay_dataset(symbol, date, false)
                .await?;
        }
        let Some(raw) = self.catalog.raw_market_data(&md.id)? else {
            return Ok(DeleteRawMarketDataReport {
                market_day_id: md.id,
                ..Default::default()
            });
        };
        self.remote
            .delete(&raw.object.remote_key)
            .await
            .with_context(|| format!("deleting raw object {}", raw.object.remote_key))?;
        let deleted_remote_key = self.catalog.remove_raw_market_data(&md.id)?;
        Ok(DeleteRawMarketDataReport {
            market_day_id: md.id,
            deleted_remote_key,
            bytes_deleted: raw.object.size_bytes.max(0) as u64,
        })
    }

    pub async fn stage_raw_for_ingest(
        &self,
        object: &StoredObject,
        dest: &Path,
    ) -> Result<PathBuf> {
        if object.kind != StorageKind::RawDbn {
            return Err(anyhow!(
                "expected raw_dbn object, got {}",
                object.kind.as_str()
            ));
        }
        self.hydrate_object_to_path(object, dest).await?;
        Ok(dest.to_path_buf())
    }

    pub async fn register_raw_object(&self, req: PutFileRequest<'_>) -> Result<StoredObject> {
        if req.kind != StorageKind::RawDbn {
            return Err(anyhow!(
                "register_raw_object received {}",
                req.kind.as_str()
            ));
        }
        let market_day = req.market_day.clone();
        let object = self.put_file(req).await?;
        self.catalog.upsert_raw_market_data(&market_day, &object)?;
        self.catalog
            .raw_market_data(&market_day.id)?
            .ok_or_else(|| anyhow!("raw market data missing after upsert"))?;
        Ok(object)
    }

    pub async fn register_replay_artifact(&self, req: PutFileRequest<'_>) -> Result<StoredObject> {
        if !is_replay_artifact(&req.kind) {
            return Err(anyhow!(
                "register_replay_artifact received {}",
                req.kind.as_str()
            ));
        }
        let market_day = req.market_day;
        let object = self.put_file(req).await?;
        let _ = market_day;
        Ok(object)
    }

    pub async fn register_replay_dataset(
        &self,
        md: &MarketDay,
        raw: &StoredObject,
        artifacts: &[StoredObject],
    ) -> Result<()> {
        self.catalog.upsert_replay_dataset(md, raw, artifacts)?;
        Ok(())
    }

    pub async fn mark_market_day_status(
        &self,
        md: &mut MarketDay,
        status: MarketDayStatus,
        ready: bool,
    ) -> Result<()> {
        self.catalog.mark_market_day_status(md, status, ready)
    }

    pub async fn raw_object(&self, md: &MarketDay) -> Result<Option<StoredObject>> {
        Ok(self
            .catalog
            .raw_market_data(&md.id)?
            .map(|record| record.object))
    }

    pub fn add_dependency(
        &self,
        object: &StoredObject,
        depends_on: &StoredObject,
        relationship: &str,
    ) -> Result<()> {
        self.catalog
            .add_dependency(object, depends_on, relationship)
    }

    pub async fn put_file(&self, req: PutFileRequest<'_>) -> Result<StoredObject> {
        let size_bytes = std::fs::metadata(req.path)
            .with_context(|| format!("metadata for {}", req.path.display()))?
            .len() as i64;
        let sha = sha256_file(req.path)?;
        let file_name = req
            .path
            .file_name()
            .and_then(|s| s.to_str())
            .unwrap_or("object.bin");
        let producer_version = req.producer_version.unwrap_or("dev");
        let remote_key = match req.kind {
            StorageKind::RawDbn => self.keys.raw_dbn_key(
                req.market_day,
                req.source_dataset.unwrap_or("GLBX.MDP3"),
                req.source_schema.unwrap_or("mbo"),
                &sha,
            ),
            _ => self.keys.artifact_key(
                req.market_day,
                req.kind.as_str(),
                req.schema_version,
                req.input_sha256,
                producer_version,
                file_name,
            ),
        };

        let mut metadata = ObjectMetadata::new(&sha, size_bytes, req.format, req.schema_version);
        metadata
            .user_metadata
            .insert("ledger-logical-key".to_string(), req.logical_key.clone());
        metadata
            .user_metadata
            .insert("ledger-kind".to_string(), req.kind.as_str().to_string());

        if let Some(remote) = self.remote.head(&remote_key).await? {
            if remote.sha256.as_deref() != Some(&sha) || remote.size_bytes != size_bytes {
                return Err(anyhow!(
                    "remote object s3://{}/{} exists but does not match local file {}",
                    self.remote.bucket(),
                    remote_key,
                    req.path.display()
                ));
            }
        } else {
            self.remote
                .put_path(&remote_key, req.path, &metadata)
                .await?;
        }

        let head = self
            .remote
            .head(&remote_key)
            .await?
            .ok_or_else(|| anyhow!("remote object missing after upload: {remote_key}"))?;
        if head.sha256.as_deref() != Some(&sha) {
            return Err(anyhow!("remote sha256 metadata mismatch for {remote_key}"));
        }

        let object = StoredObject {
            kind: req.kind,
            logical_key: req.logical_key,
            format: req.format.to_string(),
            schema_version: req.schema_version,
            content_sha256: sha,
            size_bytes,
            remote_bucket: self.remote.bucket().to_string(),
            remote_key,
            producer: req.producer.map(str::to_string),
            producer_version: req.producer_version.map(str::to_string),
            source_provider: req.source_provider.map(str::to_string),
            source_dataset: req.source_dataset.map(str::to_string),
            source_schema: req.source_schema.map(str::to_string),
            source_symbol: req.source_symbol.map(str::to_string),
            metadata_json: req.metadata_json,
        };
        self.catalog.upsert_object(req.market_day, &object)?;
        Ok(object)
    }

    async fn require_replay_object(
        &self,
        artifacts: &[StoredObject],
        replay_dataset_id: &str,
        kind: StorageKind,
    ) -> Result<StoredObject> {
        artifacts
            .iter()
            .find(|object| object.kind == kind)
            .cloned()
            .ok_or_else(|| {
                anyhow!(
                    "replay dataset {replay_dataset_id} missing {}",
                    kind.as_str()
                )
            })
    }

    async fn stage_replay_dataset_artifact(
        &self,
        md: &MarketDay,
        run_label: &str,
        object: &StoredObject,
    ) -> Result<PathBuf> {
        let dest = self
            .local
            .validate_artifact_path(md, run_label, object.kind.clone())?;
        self.hydrate_object_to_path(object, &dest).await?;
        Ok(dest)
    }

    async fn hydrate_object_to_path(&self, object: &StoredObject, dest: &Path) -> Result<()> {
        self.local
            .hydrate_atomic(dest, |tmp| async move {
                self.remote.get_to_path(&object.remote_key, &tmp).await?;
                Ok(())
            })
            .await?;
        if !self.path_valid_for_object(dest, object)? {
            return Err(anyhow!(
                "hydrated object failed sha/size validation for {}",
                dest.display()
            ));
        }
        Ok(())
    }

    fn path_valid_for_object(&self, path: &Path, object: &StoredObject) -> Result<bool> {
        if !self.path_available_for_object(path, object)? {
            return Ok(false);
        }
        Ok(sha256_file(path)? == object.content_sha256)
    }

    fn path_available_for_object(&self, path: &Path, object: &StoredObject) -> Result<bool> {
        if !path.exists() {
            return Ok(false);
        }
        let metadata = std::fs::metadata(path)?;
        if metadata.len() as i64 != object.size_bytes {
            return Ok(false);
        }
        Ok(true)
    }

    async fn remote_object_valid(&self, object: &StoredObject) -> Result<bool> {
        let Some(remote) = self.remote.head(&object.remote_key).await? else {
            return Ok(false);
        };
        if remote.size_bytes != object.size_bytes {
            return Ok(false);
        }
        Ok(remote
            .sha256
            .as_deref()
            .map_or(true, |sha| sha == object.content_sha256))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::MemoryObjectStore;
    use chrono::NaiveDate;

    async fn sample_store() -> (tempfile::TempDir, LedgerStore<MemoryObjectStore>, MarketDay) {
        let dir = tempfile::tempdir().unwrap();
        let local = LocalStore::new(dir.path());
        let remote = Arc::new(MemoryObjectStore::new("test"));
        let store = LedgerStore::new(local, remote, ObjectKeyBuilder::default());
        let md =
            MarketDay::resolve_es("ESH6", NaiveDate::from_ymd_opt(2026, 3, 12).unwrap()).unwrap();
        (dir, store, md)
    }

    async fn register_test_raw(
        store: &LedgerStore<MemoryObjectStore>,
        md: &MarketDay,
    ) -> StoredObject {
        let raw_path = store.local.ingest_raw_path(md, "raw");
        store.local.write_atomic(&raw_path, b"raw").await.unwrap();
        store
            .register_raw_object(PutFileRequest {
                market_day: md,
                path: &raw_path,
                kind: StorageKind::RawDbn,
                logical_key: store.keys.raw_dbn_logical_key(md, "GLBX.MDP3", "mbo"),
                format: "dbn.zst",
                schema_version: 1,
                input_sha256: "",
                producer: Some("test"),
                producer_version: Some("dev"),
                source_provider: Some("databento"),
                source_dataset: Some("GLBX.MDP3"),
                source_schema: Some("mbo"),
                source_symbol: Some("ESH6"),
                metadata_json: serde_json::json!({}),
            })
            .await
            .unwrap()
    }

    async fn register_test_artifact(
        store: &LedgerStore<MemoryObjectStore>,
        md: &MarketDay,
        kind: StorageKind,
        bytes: &[u8],
    ) -> StoredObject {
        let path = store
            .local
            .ingest_artifacts_dir(md, "test")
            .join(format!("{}.bin", kind.as_str()));
        store.local.write_atomic(&path, bytes).await.unwrap();
        store
            .register_replay_artifact(PutFileRequest {
                market_day: md,
                path: &path,
                kind: kind.clone(),
                logical_key: store.keys.artifact_logical_key(md, kind.as_str(), 1),
                format: "bin",
                schema_version: 1,
                input_sha256: "raw",
                producer: Some("test"),
                producer_version: Some("dev"),
                source_provider: None,
                source_dataset: None,
                source_schema: None,
                source_symbol: None,
                metadata_json: serde_json::json!({}),
            })
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn raw_objects_register_layer_one_without_local_cache() {
        let (_dir, store, mut md) = sample_store().await;
        store
            .mark_market_day_status(&mut md, MarketDayStatus::Downloading, false)
            .await
            .unwrap();
        let raw_path = store.local.ingest_raw_path(&md, "test");
        store.local.write_atomic(&raw_path, b"abc").await.unwrap();
        let raw = store
            .register_raw_object(PutFileRequest {
                market_day: &md,
                path: &raw_path,
                kind: StorageKind::RawDbn,
                logical_key: store.keys.raw_dbn_logical_key(&md, "GLBX.MDP3", "mbo"),
                format: "dbn.zst",
                schema_version: 1,
                input_sha256: "",
                producer: Some("test"),
                producer_version: Some("dev"),
                source_provider: Some("databento"),
                source_dataset: Some("GLBX.MDP3"),
                source_schema: Some("mbo"),
                source_symbol: Some("ESH6"),
                metadata_json: serde_json::json!({}),
            })
            .await
            .unwrap();
        assert_eq!(raw.kind, StorageKind::RawDbn);
        let raw_record = store.catalog.raw_market_data(&md.id).unwrap().unwrap();
        assert_eq!(raw_record.object.remote_key, raw.remote_key);
    }

    #[tokio::test]
    async fn stage_replay_dataset_hydrates_artifacts_into_tmp() {
        let (_dir, store, mut md) = sample_store().await;
        store
            .mark_market_day_status(&mut md, MarketDayStatus::Preprocessing, false)
            .await
            .unwrap();
        let raw = register_test_raw(&store, &md).await;
        let events = register_test_artifact(&store, &md, StorageKind::EventStore, b"events").await;
        let batches = register_test_artifact(&store, &md, StorageKind::BatchIndex, b"x").await;
        let trades = register_test_artifact(&store, &md, StorageKind::TradeIndex, b"x").await;
        let book = register_test_artifact(&store, &md, StorageKind::BookCheck, b"x").await;
        store
            .register_replay_dataset(&md, &raw, &[events.clone(), batches, trades, book])
            .await
            .unwrap();
        store
            .mark_market_day_status(&mut md, MarketDayStatus::Ready, true)
            .await
            .unwrap();
        let dataset = store
            .stage_replay_dataset("ESH6", md.market_date, "test")
            .await
            .unwrap();
        assert_eq!(
            tokio::fs::read(dataset.events_path).await.unwrap(),
            b"events"
        );
    }

    #[tokio::test]
    async fn replay_dataset_status_is_cheap_unless_verified() {
        let (_dir, store, mut md) = sample_store().await;
        store
            .mark_market_day_status(&mut md, MarketDayStatus::Ready, true)
            .await
            .unwrap();
        let raw = register_test_raw(&store, &md).await;
        let mut artifacts = Vec::new();

        for (kind, bytes) in [
            (StorageKind::EventStore, b"events".as_slice()),
            (StorageKind::BatchIndex, b"batches".as_slice()),
            (StorageKind::TradeIndex, b"trades".as_slice()),
            (StorageKind::BookCheck, b"book_check".as_slice()),
        ] {
            let path = store
                .local
                .ingest_artifacts_dir(&md, "status")
                .join(kind.as_str());
            store.local.write_atomic(&path, bytes).await.unwrap();
            let object = StoredObject {
                kind: kind.clone(),
                logical_key: store.keys.artifact_logical_key(&md, kind.as_str(), 1),
                format: "bin".to_string(),
                schema_version: 1,
                content_sha256: "wrong-sha".to_string(),
                size_bytes: bytes.len() as i64,
                remote_bucket: store.remote.bucket().to_string(),
                remote_key: format!("remote/{}", kind.as_str()),
                producer: Some("test".to_string()),
                producer_version: Some("dev".to_string()),
                source_provider: None,
                source_dataset: None,
                source_schema: None,
                source_symbol: None,
                metadata_json: serde_json::json!({}),
            };
            store.catalog.upsert_object(&md, &object).unwrap();
            store
                .remote
                .put_bytes(
                    &object.remote_key,
                    bytes,
                    &ObjectMetadata::new(
                        "actual-remote-sha",
                        bytes.len() as i64,
                        &object.format,
                        object.schema_version,
                    ),
                )
                .await
                .unwrap();
            artifacts.push(object);
        }
        store
            .register_replay_dataset(&md, &raw, &artifacts)
            .await
            .unwrap();

        let cheap = store
            .replay_dataset_status("ESH6", md.market_date)
            .await
            .unwrap();
        let verified = store
            .verified_replay_dataset_status("ESH6", md.market_date)
            .await
            .unwrap();

        assert!(cheap.replay_objects_valid);
        assert!(!verified.replay_objects_valid);
    }

    #[tokio::test]
    async fn sqlite_catalog_drives_list_status_and_stage() {
        let (_dir, store, mut md) = sample_store().await;
        store
            .mark_market_day_status(&mut md, MarketDayStatus::Ready, true)
            .await
            .unwrap();
        let raw = register_test_raw(&store, &md).await;
        let events = register_test_artifact(&store, &md, StorageKind::EventStore, b"events").await;
        let batches =
            register_test_artifact(&store, &md, StorageKind::BatchIndex, b"batches").await;
        let trades = register_test_artifact(&store, &md, StorageKind::TradeIndex, b"trades").await;
        let book = register_test_artifact(&store, &md, StorageKind::BookCheck, b"book").await;
        store
            .register_replay_dataset(&md, &raw, &[events, batches, trades, book])
            .await
            .unwrap();

        let rows = store
            .list_market_days(MarketDayFilter::default())
            .await
            .unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].market_day.id, md.id);
        assert!(rows[0].replay_dataset.is_some());

        let status = store
            .replay_dataset_status("ESH6", md.market_date)
            .await
            .unwrap();
        assert!(status.catalog_found);
        assert!(status.replay_artifacts_available);
        assert!(status.replay_objects_valid);

        let loaded = store
            .stage_replay_dataset("ESH6", md.market_date, "test")
            .await
            .unwrap();
        assert_eq!(
            tokio::fs::read(loaded.events_path).await.unwrap(),
            b"events"
        );
    }

    #[tokio::test]
    async fn fresh_local_catalog_does_not_discover_remote_blobs_without_sqlite_rows() {
        let (_dir, store, md) = sample_store().await;
        register_test_raw(&store, &md).await;
        let fresh_dir = tempfile::tempdir().unwrap();
        let fresh = LedgerStore::new(
            LocalStore::new(fresh_dir.path()),
            store.remote.clone(),
            ObjectKeyBuilder::default(),
        );

        assert!(fresh.raw_object(&md).await.unwrap().is_none());
        let rows = fresh
            .list_market_days(MarketDayFilter::default())
            .await
            .unwrap();
        assert!(rows.is_empty());
    }
}
