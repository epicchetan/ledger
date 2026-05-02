use anyhow::{Context, Result};
use ledger_domain::{now_ns, MarketDay, StorageKind};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime};
use tokio::io::AsyncWriteExt;

#[derive(Clone, Debug)]
pub struct LocalStore {
    root: PathBuf,
}

impl LocalStore {
    pub fn new(root: impl Into<PathBuf>) -> Self {
        Self { root: root.into() }
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    pub fn catalog_path(&self) -> PathBuf {
        self.root.join("ledger.sqlite")
    }

    pub fn ingest_run_dir(&self, md: &MarketDay, run_id: impl ToString) -> PathBuf {
        self.root
            .join("tmp")
            .join("ingest")
            .join(&md.root)
            .join(&md.contract_symbol)
            .join(md.market_date.to_string())
            .join(run_id.to_string())
    }

    pub fn ingest_raw_path(&self, md: &MarketDay, run_id: impl ToString) -> PathBuf {
        self.ingest_run_dir(md, run_id).join("raw.dbn.zst")
    }

    pub fn ingest_artifacts_dir(&self, md: &MarketDay, run_id: impl ToString) -> PathBuf {
        self.ingest_run_dir(md, run_id).join("artifacts")
    }

    pub fn validate_run_dir(&self, md: &MarketDay, run_id: impl ToString) -> PathBuf {
        self.root
            .join("tmp")
            .join("validate")
            .join(&md.root)
            .join(&md.contract_symbol)
            .join(md.market_date.to_string())
            .join(run_id.to_string())
    }

    pub fn validate_artifact_path(
        &self,
        md: &MarketDay,
        run_id: impl ToString,
        kind: StorageKind,
    ) -> Result<PathBuf> {
        Ok(self
            .validate_run_dir(md, run_id)
            .join(replay_file_name(kind)?))
    }

    pub fn tmp_dir(&self) -> PathBuf {
        self.root.join("tmp")
    }

    pub fn new_ingest_run_label(&self) -> String {
        now_ns().to_string()
    }

    pub async fn cleanup_ingest_run(&self, md: &MarketDay, run_id: impl ToString) -> Result<()> {
        let dir = self.ingest_run_dir(md, run_id);
        if dir.exists() {
            tokio::fs::remove_dir_all(dir).await?;
        }
        Ok(())
    }

    pub async fn cleanup_validate_run(&self, md: &MarketDay, run_id: impl ToString) -> Result<()> {
        let dir = self.validate_run_dir(md, run_id);
        if dir.exists() {
            tokio::fs::remove_dir_all(dir).await?;
        }
        Ok(())
    }

    pub fn cleanup_tmp(&self, older_than: Option<Duration>) -> Result<CleanupTmpReport> {
        let tmp = self.tmp_dir();
        let mut report = CleanupTmpReport {
            root: tmp.clone(),
            ..Default::default()
        };
        if !tmp.exists() {
            return Ok(report);
        }
        cleanup_tmp_entries(&tmp, older_than, &mut report)?;
        Ok(report)
    }

    pub async fn hydrate_atomic<F, Fut>(&self, dest: &Path, hydrate: F) -> Result<()>
    where
        F: FnOnce(PathBuf) -> Fut,
        Fut: std::future::Future<Output = Result<()>>,
    {
        if let Some(parent) = dest.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        let tmp = tmp_path(dest);
        if tmp.exists() {
            tokio::fs::remove_file(&tmp).await.ok();
        }
        hydrate(tmp.clone()).await?;
        tokio::fs::rename(&tmp, dest)
            .await
            .with_context(|| format!("renaming {} -> {}", tmp.display(), dest.display()))?;
        Ok(())
    }

    pub async fn write_atomic(&self, path: &Path, bytes: &[u8]) -> Result<()> {
        if let Some(parent) = path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        let tmp = tmp_path(path);
        let mut file = tokio::fs::File::create(&tmp)
            .await
            .with_context(|| format!("creating {}", tmp.display()))?;
        file.write_all(bytes).await?;
        file.sync_all().await?;
        drop(file);
        tokio::fs::rename(&tmp, path)
            .await
            .with_context(|| format!("renaming {} -> {}", tmp.display(), path.display()))?;
        Ok(())
    }
}

pub fn replay_file_name(kind: StorageKind) -> Result<&'static str> {
    match kind {
        StorageKind::EventStore => Ok("events.v1.bin"),
        StorageKind::BatchIndex => Ok("batches.v1.bin"),
        StorageKind::TradeIndex => Ok("trades.v1.bin"),
        StorageKind::BookCheck => Ok("book_check.v1.json"),
        other => anyhow::bail!("{} is not a replay artifact", other.as_str()),
    }
}

pub fn tmp_path(path: &Path) -> PathBuf {
    let mut s = path.as_os_str().to_owned();
    s.push(".part");
    PathBuf::from(s)
}

#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct CleanupTmpReport {
    pub root: PathBuf,
    pub deleted_files: usize,
    pub deleted_dirs: usize,
    pub bytes_deleted: u64,
}

fn cleanup_tmp_entries(
    dir: &Path,
    older_than: Option<Duration>,
    report: &mut CleanupTmpReport,
) -> Result<bool> {
    let mut empty = true;
    for entry in std::fs::read_dir(dir)? {
        let entry = entry?;
        let path = entry.path();
        let metadata = entry.metadata()?;
        if metadata.is_dir() {
            if cleanup_tmp_entries(&path, older_than, report)? && eligible(&metadata, older_than) {
                std::fs::remove_dir(&path)?;
                report.deleted_dirs += 1;
            } else {
                empty = false;
            }
        } else if eligible(&metadata, older_than) {
            let bytes = metadata.len();
            std::fs::remove_file(&path)?;
            report.deleted_files += 1;
            report.bytes_deleted += bytes;
        } else {
            empty = false;
        }
    }
    Ok(empty)
}

fn eligible(metadata: &std::fs::Metadata, older_than: Option<Duration>) -> bool {
    let Some(older_than) = older_than else {
        return true;
    };
    let Ok(modified_at) = metadata.modified() else {
        return false;
    };
    SystemTime::now()
        .duration_since(modified_at)
        .is_ok_and(|age| age >= older_than)
}
