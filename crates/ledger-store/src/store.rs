use anyhow::{anyhow, Context, Result};
use chrono::NaiveDate;
use ledger_domain::{MarketDay, MarketDayStatus, StorageKind};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::{
    is_replay_artifact, required_replay_kinds, sha256_file, CachePrunePolicy, CachePruneReport,
    LoadedSession, LocalStore, MarketDayFilter, MarketDayRecord, ObjectKeyBuilder, ObjectMetadata,
    ObjectStore, R2Config, R2ObjectStore, SessionObjectStatus, SessionStatus, SqliteCatalog,
    StoredObject,
};

#[derive(Clone)]
pub struct LedgerStore<S: ObjectStore + 'static> {
    pub local: LocalStore,
    pub remote: Arc<S>,
    pub keys: ObjectKeyBuilder,
    pub catalog: SqliteCatalog,
    pub cache_policy: CachePrunePolicy,
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
            CachePrunePolicy::from_env(),
        )
    }
}

impl<S: ObjectStore + 'static> LedgerStore<S> {
    pub fn open(
        local: LocalStore,
        remote: Arc<S>,
        keys: ObjectKeyBuilder,
        cache_policy: CachePrunePolicy,
    ) -> Result<Self> {
        let catalog = SqliteCatalog::open(local.catalog_path())?;
        Ok(Self {
            local,
            remote,
            keys,
            catalog,
            cache_policy,
        })
    }

    pub fn new(local: LocalStore, remote: Arc<S>, keys: ObjectKeyBuilder) -> Self {
        Self::open(local, remote, keys, CachePrunePolicy::default())
            .expect("opening test ledger store")
    }

    pub fn list_market_days(&self, filter: MarketDayFilter) -> Result<Vec<MarketDayRecord>> {
        self.catalog.list_market_days(&filter)
    }

    pub async fn session_status(&self, symbol: &str, date: NaiveDate) -> Result<SessionStatus> {
        let md = MarketDay::resolve_es(symbol, date)?;
        let Some(record) = self.catalog.market_day(&md.id)? else {
            return Ok(SessionStatus {
                market_day: md,
                catalog_found: false,
                ready: false,
                raw_available_remote: false,
                artifacts_available_remote: false,
                session_loaded_local: false,
                session_cache_valid: false,
                last_accessed_ns: None,
                objects: Vec::new(),
            });
        };

        let raw_available_remote = self
            .catalog
            .object(&record.market_day.id, StorageKind::RawDbn)?
            .is_some();
        let mut objects = Vec::new();
        let mut artifacts_available_remote = true;
        let mut session_loaded_local = true;
        let mut session_cache_valid = true;

        for kind in required_replay_kinds() {
            let object = self.catalog.object(&record.market_day.id, kind.clone())?;
            let Some(object) = object else {
                artifacts_available_remote = false;
                session_loaded_local = false;
                session_cache_valid = false;
                objects.push(SessionObjectStatus {
                    kind,
                    remote_key: None,
                    local_path: None,
                    local_valid: false,
                });
                continue;
            };
            let local_path = self.catalog.cache_path(&object.remote_key)?;
            let local_valid = match local_path.as_deref() {
                Some(path) => self.path_valid_for_object(path, &object)?,
                None => false,
            };
            session_loaded_local &= local_path.is_some();
            session_cache_valid &= local_valid;
            objects.push(SessionObjectStatus {
                kind,
                remote_key: Some(object.remote_key),
                local_path,
                local_valid,
            });
        }

        Ok(SessionStatus {
            market_day: record.market_day,
            catalog_found: true,
            ready: record.ready,
            raw_available_remote,
            artifacts_available_remote,
            session_loaded_local,
            session_cache_valid,
            last_accessed_ns: record.last_accessed_ns,
            objects,
        })
    }

    pub async fn load_session(&self, symbol: &str, date: NaiveDate) -> Result<LoadedSession> {
        let md = MarketDay::resolve_es(symbol, date)?;
        let record = self
            .catalog
            .market_day(&md.id)?
            .ok_or_else(|| anyhow!("market day {} is not in the catalog", md.id))?;
        if !record.ready {
            return Err(anyhow!("market day {} is not ready", md.id));
        }

        let events = self
            .require_replay_object(&record.market_day, StorageKind::EventStore)
            .await?;
        let batches = self
            .require_replay_object(&record.market_day, StorageKind::BatchIndex)
            .await?;
        let trades = self
            .require_replay_object(&record.market_day, StorageKind::TradeIndex)
            .await?;
        let book_check = self
            .require_replay_object(&record.market_day, StorageKind::BookCheck)
            .await?;

        let events_path = self
            .load_session_artifact(&record.market_day, &events)
            .await?;
        let batches_path = self
            .load_session_artifact(&record.market_day, &batches)
            .await?;
        let trades_path = self
            .load_session_artifact(&record.market_day, &trades)
            .await?;
        let book_check_path = self
            .load_session_artifact(&record.market_day, &book_check)
            .await?;

        self.catalog.touch_market_day(&record.market_day.id)?;
        self.prune_cache(self.cache_policy)?;

        Ok(LoadedSession {
            market_day: record.market_day,
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
        self.put_file(req).await
    }

    pub async fn register_replay_artifact(&self, req: PutFileRequest<'_>) -> Result<StoredObject> {
        if !is_replay_artifact(&req.kind) {
            return Err(anyhow!(
                "register_replay_artifact received {}",
                req.kind.as_str()
            ));
        }
        let market_day = req.market_day;
        let path = req.path;
        let object = self.put_file(req).await?;
        self.commit_replay_artifact_to_session(market_day, &object, path)
            .await?;
        Ok(object)
    }

    pub async fn commit_replay_artifact_to_session(
        &self,
        md: &MarketDay,
        object: &StoredObject,
        source_path: &Path,
    ) -> Result<PathBuf> {
        if !is_replay_artifact(&object.kind) {
            return Err(anyhow!(
                "{} is not allowed in the replay session cache",
                object.kind.as_str()
            ));
        }
        let dest = self
            .local
            .session_artifact_path(md, object.kind.clone())
            .with_context(|| format!("session path for {}", object.kind.as_str()))?;
        self.local.commit_file_atomic(source_path, &dest).await?;
        if !self.path_valid_for_object(&dest, object)? {
            return Err(anyhow!(
                "committed replay artifact failed validation: {}",
                dest.display()
            ));
        }
        self.catalog.upsert_session_cache_entry(md, object, &dest)?;
        Ok(dest)
    }

    pub fn mark_market_day_status(
        &self,
        md: &mut MarketDay,
        status: MarketDayStatus,
        ready: bool,
    ) -> Result<()> {
        self.catalog.mark_market_day_status(md, status, ready)
    }

    pub fn raw_object(&self, md: &MarketDay) -> Result<Option<StoredObject>> {
        self.catalog.object(&md.id, StorageKind::RawDbn)
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

    pub fn prune_cache(&self, policy: CachePrunePolicy) -> Result<CachePruneReport> {
        let cached = self.catalog.cached_market_days_lru()?;
        if cached.len() <= policy.max_sessions {
            return Ok(CachePruneReport::default());
        }

        let mut report = CachePruneReport::default();
        let to_delete = cached.len().saturating_sub(policy.max_sessions);
        for (market_day_id, sample_path, _) in cached.into_iter().take(to_delete) {
            let session_dir = sample_path
                .parent()
                .ok_or_else(|| anyhow!("cache path has no parent: {}", sample_path.display()))?
                .to_path_buf();
            let bytes = dir_size(&session_dir)?;
            if session_dir.exists() {
                std::fs::remove_dir_all(&session_dir)
                    .with_context(|| format!("removing session cache {}", session_dir.display()))?;
            }
            self.catalog.remove_session_cache_entries(&market_day_id)?;
            report.deleted_sessions.push(market_day_id);
            report.deleted_dirs.push(session_dir);
            report.bytes_deleted = report.bytes_deleted.saturating_add(bytes);
        }
        Ok(report)
    }

    async fn require_replay_object(
        &self,
        md: &MarketDay,
        kind: StorageKind,
    ) -> Result<StoredObject> {
        self.catalog
            .object(&md.id, kind.clone())?
            .ok_or_else(|| anyhow!("market day {} missing {}", md.id, kind.as_str()))
    }

    async fn load_session_artifact(
        &self,
        md: &MarketDay,
        object: &StoredObject,
    ) -> Result<PathBuf> {
        if let Some(path) = self.catalog.cache_path(&object.remote_key)? {
            if self.path_valid_for_object(&path, object)? {
                self.catalog.upsert_session_cache_entry(md, object, &path)?;
                return Ok(path);
            }
        }

        let dest = self.local.session_artifact_path(md, object.kind.clone())?;
        self.hydrate_object_to_path(object, &dest).await?;
        self.catalog.upsert_session_cache_entry(md, object, &dest)?;
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
        if !path.exists() {
            return Ok(false);
        }
        let metadata = std::fs::metadata(path)?;
        if metadata.len() as i64 != object.size_bytes {
            return Ok(false);
        }
        Ok(sha256_file(path)? == object.content_sha256)
    }
}

fn dir_size(path: &Path) -> Result<u64> {
    if !path.exists() {
        return Ok(0);
    }
    let mut total = 0_u64;
    for entry in std::fs::read_dir(path)? {
        let entry = entry?;
        let path = entry.path();
        let meta = std::fs::metadata(&path)?;
        if meta.is_dir() {
            total = total.saturating_add(dir_size(&path)?);
        } else {
            total = total.saturating_add(meta.len());
        }
    }
    Ok(total)
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

    #[tokio::test]
    async fn raw_objects_are_not_session_cache_entries() {
        let (_dir, store, mut md) = sample_store().await;
        store
            .mark_market_day_status(&mut md, MarketDayStatus::Downloading, false)
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
        assert!(store.catalog.cache_path(&raw.remote_key).unwrap().is_none());
    }

    #[tokio::test]
    async fn load_session_hydrates_missing_artifacts() {
        let (_dir, store, mut md) = sample_store().await;
        store
            .mark_market_day_status(&mut md, MarketDayStatus::Preprocessing, false)
            .unwrap();
        let artifact = store
            .local
            .ingest_artifacts_dir(&md, "test")
            .join("events.v1.bin");
        store
            .local
            .write_atomic(&artifact, b"events")
            .await
            .unwrap();
        let obj = store
            .register_replay_artifact(PutFileRequest {
                market_day: &md,
                path: &artifact,
                kind: StorageKind::EventStore,
                logical_key: store.keys.artifact_logical_key(&md, "event_store", 1),
                format: "ledger-events-bin",
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
            .unwrap();
        for kind in [
            StorageKind::BatchIndex,
            StorageKind::TradeIndex,
            StorageKind::BookCheck,
        ] {
            let path = store
                .local
                .ingest_artifacts_dir(&md, "test")
                .join(format!("{}.bin", kind.as_str()));
            store.local.write_atomic(&path, b"x").await.unwrap();
            store
                .register_replay_artifact(PutFileRequest {
                    market_day: &md,
                    path: &path,
                    kind: kind.clone(),
                    logical_key: store.keys.artifact_logical_key(&md, kind.as_str(), 1),
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
                .unwrap();
        }
        store
            .mark_market_day_status(&mut md, MarketDayStatus::Ready, true)
            .unwrap();
        let cached = store.catalog.cache_path(&obj.remote_key).unwrap().unwrap();
        std::fs::remove_file(&cached).unwrap();
        let session = store.load_session("ESH6", md.market_date).await.unwrap();
        assert_eq!(
            tokio::fs::read(session.events_path).await.unwrap(),
            b"events"
        );
    }
}
