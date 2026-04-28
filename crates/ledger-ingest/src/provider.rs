use anyhow::Result;
use async_trait::async_trait;
use ledger_core::MarketDay;
use std::path::Path;

use crate::databento_downloader::{DatabentoDownloader, DatabentoRangeRequest};

#[async_trait]
pub trait MarketDataProvider: Send + Sync {
    async fn download_mbo(&self, market_day: &MarketDay, dest: &Path) -> Result<()>;
}

#[derive(Clone, Debug, Default)]
pub struct DatabentoProvider;

#[async_trait]
impl MarketDataProvider for DatabentoProvider {
    async fn download_mbo(&self, market_day: &MarketDay, dest: &Path) -> Result<()> {
        let mut downloader = DatabentoDownloader::from_env()?;
        let req = DatabentoRangeRequest::es_mbo_market_day(market_day, dest);
        downloader.download_to_file(&req).await?;
        Ok(())
    }
}
