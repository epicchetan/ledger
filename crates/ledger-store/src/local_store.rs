use anyhow::{Context, Result};
use ledger_core::{now_ns, MarketDay, StorageKind};
use std::path::{Path, PathBuf};
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
        self.root.join("catalog.sqlite")
    }

    pub fn legacy_raw_dbn_path(
        &self,
        dataset: &str,
        schema: &str,
        root: &str,
        symbol: &str,
        market_date: &str,
    ) -> PathBuf {
        self.root
            .join("raw")
            .join("databento")
            .join(dataset)
            .join(schema)
            .join(root)
            .join(symbol)
            .join(format!("{market_date}.dbn.zst"))
    }

    pub fn session_dir(&self, root: &str, symbol: &str, market_date: &str) -> PathBuf {
        self.root
            .join("sessions")
            .join(root)
            .join(symbol)
            .join(market_date)
    }

    pub fn session_artifact_path(&self, md: &MarketDay, kind: StorageKind) -> Result<PathBuf> {
        Ok(self
            .session_dir(&md.root, &md.contract_symbol, &md.market_date.to_string())
            .join(replay_file_name(kind)?))
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

    pub async fn commit_file_atomic(&self, source: &Path, dest: &Path) -> Result<()> {
        if let Some(parent) = dest.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        let tmp = tmp_path(dest);
        if tmp.exists() {
            tokio::fs::remove_file(&tmp).await.ok();
        }
        tokio::fs::copy(source, &tmp).await.with_context(|| {
            format!(
                "copying complete file {} -> {}",
                source.display(),
                tmp.display()
            )
        })?;
        tokio::fs::rename(&tmp, dest)
            .await
            .with_context(|| format!("renaming {} -> {}", tmp.display(), dest.display()))?;
        Ok(())
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

    pub fn legacy_artifacts_dir(&self, root: &str, symbol: &str, market_date: &str) -> PathBuf {
        self.root
            .join("artifacts")
            .join(root)
            .join(symbol)
            .join(market_date)
    }

    pub fn legacy_artifact_path(&self, md: &MarketDay, kind: StorageKind) -> PathBuf {
        let file_name = match kind {
            StorageKind::EventStore => "events.v1.bin",
            StorageKind::BatchIndex => "batches.v1.bin",
            StorageKind::TradeIndex => "trades.v1.bin",
            StorageKind::BookCheck => "book_check.v1.json",
            StorageKind::RawDbn => "raw.dbn.zst",
            StorageKind::Other(_) => "artifact.bin",
        };
        self.legacy_artifacts_dir(&md.root, &md.contract_symbol, &md.market_date.to_string())
            .join(file_name)
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
