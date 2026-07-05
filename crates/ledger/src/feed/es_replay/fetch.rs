use crate::market::{utc_datetime_from_ns, MarketDay};
use crate::LedgerError;
use databento::{
    dbn::{SType, Schema},
    historical::timeseries::GetRangeToFileParams,
    HistoricalClient,
};
use serde::Serialize;
use serde_json::{json, Value};
use std::path::{Path, PathBuf};
use store::{
    ObjectFilter, RegisterFileRequest, RemoteStore, Store, StoreObjectDescriptor, StoreObjectRole,
};
use time::OffsetDateTime;
use tokio::sync::mpsc::UnboundedSender;

use super::RAW_DATABENTO_DBN_ZST_KIND;

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "stage", rename_all = "snake_case")]
pub enum FetchProgress {
    Requesting,
    Downloading,
    Registering,
}

#[derive(Debug, Clone, Serialize)]
pub struct EsFetchSummary {
    pub raw_object_id: String,
    pub market_day: String,
    pub source_symbol: String,
    pub dataset: String,
    pub size_bytes: u64,
    pub fetched: bool,
    pub reused: bool,
}

pub async fn fetch_es_raw<S>(
    store: &Store<S>,
    day: MarketDay,
    symbol: &str,
    dataset: &str,
    staging_dir: &Path,
    force: bool,
    progress: Option<UnboundedSender<FetchProgress>>,
) -> Result<EsFetchSummary, LedgerError>
where
    S: RemoteStore + 'static,
{
    let symbol = symbol.trim();
    if symbol.is_empty() {
        return Err(LedgerError::Fetch("symbol is required".to_string()));
    }
    let dataset = if dataset.trim().is_empty() {
        "GLBX.MDP3"
    } else {
        dataset.trim()
    };

    if !force {
        if let Some(existing) = find_existing_raw(store, day, symbol, dataset)? {
            return Ok(fetch_summary(existing, day, symbol, dataset, false));
        }
    }

    let (start_ns, end_ns) = day.es_session_bounds_utc()?;
    if let Some(progress) = &progress {
        let _ = progress.send(FetchProgress::Requesting);
    }
    tokio::fs::create_dir_all(staging_dir)
        .await
        .map_err(|err| LedgerError::Fetch(err.to_string()))?;
    let out_path = staging_dir.join(format!("{}-{}.dbn.zst", day, symbol));
    let tmp_path = part_path(&out_path);
    if tmp_path.exists() {
        tokio::fs::remove_file(&tmp_path).await.ok();
    }

    if let Some(progress) = &progress {
        let _ = progress.send(FetchProgress::Downloading);
    }
    download_databento_range(dataset, symbol, start_ns, end_ns, &tmp_path).await?;
    tokio::fs::rename(&tmp_path, &out_path)
        .await
        .map_err(|err| LedgerError::Fetch(err.to_string()))?;

    if let Some(progress) = &progress {
        let _ = progress.send(FetchProgress::Registering);
    }
    let descriptor = store
        .register_file(RegisterFileRequest {
            path: &out_path,
            role: StoreObjectRole::Raw,
            kind: RAW_DATABENTO_DBN_ZST_KIND.to_string(),
            file_name: Some(format!("{}-{}.dbn.zst", day, symbol)),
            format: Some("dbn.zst".to_string()),
            media_type: None,
            lineage: Vec::new(),
            metadata_json: json!({
                "provider": "databento",
                "dataset": dataset,
                "schema": "mbo",
                "source_symbol": symbol,
                "market_day": day.to_string(),
            }),
        })
        .await?;
    tokio::fs::remove_file(&out_path).await.ok();
    Ok(fetch_summary(descriptor, day, symbol, dataset, true))
}

fn find_existing_raw<S>(
    store: &Store<S>,
    day: MarketDay,
    symbol: &str,
    dataset: &str,
) -> Result<Option<StoreObjectDescriptor>, LedgerError>
where
    S: RemoteStore + 'static,
{
    let raws = store.list_objects(ObjectFilter {
        role: Some(StoreObjectRole::Raw),
        kind: Some(RAW_DATABENTO_DBN_ZST_KIND.to_string()),
        id_prefix: None,
    })?;
    Ok(raws.into_iter().find(|descriptor| {
        metadata_str(descriptor, "market_day") == Some(day.to_string().as_str())
            && metadata_str(descriptor, "source_symbol") == Some(symbol)
            && metadata_str(descriptor, "dataset") == Some(dataset)
            && metadata_str(descriptor, "schema") == Some("mbo")
    }))
}

async fn download_databento_range(
    dataset: &str,
    symbol: &str,
    start_ns: u64,
    end_ns: u64,
    tmp_path: &Path,
) -> Result<(), LedgerError> {
    let mut client = HistoricalClient::builder()
        .key_from_env()
        .map_err(|err| LedgerError::Fetch(err.to_string()))?
        .build()
        .map_err(|err| LedgerError::Fetch(err.to_string()))?;
    let params = GetRangeToFileParams::builder()
        .dataset(dataset.to_string())
        .symbols(symbol)
        .schema(Schema::Mbo)
        .stype_in(SType::RawSymbol)
        .date_time_range(offset_datetime_from_ns(start_ns)?..offset_datetime_from_ns(end_ns)?)
        .path(tmp_path.to_path_buf())
        .build();
    let _decoder = client
        .timeseries()
        .get_range_to_file(&params)
        .await
        .map_err(|err| LedgerError::Fetch(err.to_string()))?;
    Ok(())
}

fn fetch_summary(
    descriptor: StoreObjectDescriptor,
    day: MarketDay,
    symbol: &str,
    dataset: &str,
    fetched: bool,
) -> EsFetchSummary {
    EsFetchSummary {
        raw_object_id: descriptor.id.to_string(),
        market_day: day.to_string(),
        source_symbol: symbol.to_string(),
        dataset: dataset.to_string(),
        size_bytes: descriptor.size_bytes,
        fetched,
        reused: !fetched,
    }
}

fn metadata_str<'a>(descriptor: &'a StoreObjectDescriptor, key: &str) -> Option<&'a str> {
    descriptor.metadata_json.get(key).and_then(Value::as_str)
}

fn offset_datetime_from_ns(ns: u64) -> Result<OffsetDateTime, LedgerError> {
    let datetime = utc_datetime_from_ns(ns)?;
    OffsetDateTime::from_unix_timestamp_nanos(
        i128::from(datetime.timestamp()) * 1_000_000_000
            + i128::from(datetime.timestamp_subsec_nanos()),
    )
    .map_err(|err| LedgerError::Fetch(err.to_string()))
}

fn part_path(path: &Path) -> PathBuf {
    let mut value = path.as_os_str().to_owned();
    value.push(".part");
    PathBuf::from(value)
}
