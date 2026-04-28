use anyhow::{Context, Result};
use databento::{
    dbn::{SType, Schema},
    historical::timeseries::GetRangeToFileParams,
    HistoricalClient,
};
use ledger_domain::{ns_to_utc_datetime, MarketDay};
use std::path::{Path, PathBuf};
use time::OffsetDateTime;

#[derive(Clone, Debug)]
pub struct DatabentoRangeRequest {
    pub dataset: String,
    pub symbol: String,
    pub schema: Schema,
    pub stype_in: SType,
    pub start_ns: u64,
    pub end_ns: u64,
    pub out_path: PathBuf,
}

impl DatabentoRangeRequest {
    pub fn es_mbo_market_day(md: &MarketDay, out_path: impl Into<PathBuf>) -> Self {
        Self {
            dataset: "GLBX.MDP3".to_string(),
            symbol: md.contract_symbol.clone(),
            schema: Schema::Mbo,
            stype_in: SType::RawSymbol,
            start_ns: md.data_start_ns,
            end_ns: md.data_end_ns,
            out_path: out_path.into(),
        }
    }
}

pub struct DatabentoDownloader {
    client: HistoricalClient,
}

impl DatabentoDownloader {
    pub fn from_env() -> Result<Self> {
        let client = HistoricalClient::builder()
            .key_from_env()?
            .build()
            .context("building Databento historical client")?;
        Ok(Self { client })
    }

    pub async fn download_to_file(&mut self, req: &DatabentoRangeRequest) -> Result<PathBuf> {
        if let Some(parent) = req.out_path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        let tmp = tmp_path(&req.out_path);
        if tmp.exists() {
            tokio::fs::remove_file(&tmp).await.ok();
        }
        let start = offset_datetime_from_ns(req.start_ns)?;
        let end = offset_datetime_from_ns(req.end_ns)?;
        let params = GetRangeToFileParams::builder()
            .dataset(req.dataset.clone())
            .symbols(req.symbol.as_str())
            .schema(req.schema)
            .stype_in(req.stype_in)
            .date_time_range(start..end)
            .path(tmp.clone())
            .build();
        let _decoder = self
            .client
            .timeseries()
            .get_range_to_file(&params)
            .await
            .with_context(|| {
                format!(
                    "downloading Databento {} {} {:?} {} -> {}",
                    req.dataset,
                    req.symbol,
                    req.schema,
                    ns_to_utc_datetime(req.start_ns),
                    ns_to_utc_datetime(req.end_ns)
                )
            })?;
        tokio::fs::rename(&tmp, &req.out_path)
            .await
            .with_context(|| {
                format!(
                    "renaming downloaded DBN {} -> {}",
                    tmp.display(),
                    req.out_path.display()
                )
            })?;
        Ok(req.out_path.clone())
    }
}

fn offset_datetime_from_ns(ns: u64) -> Result<OffsetDateTime> {
    OffsetDateTime::from_unix_timestamp_nanos(ns as i128)
        .context("converting unix ns to OffsetDateTime")
}

fn tmp_path(path: &Path) -> PathBuf {
    let mut s = path.as_os_str().to_owned();
    s.push(".part");
    PathBuf::from(s)
}
