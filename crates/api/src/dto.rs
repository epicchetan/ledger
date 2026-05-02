use crate::jobs::JobRecord;
use chrono::NaiveDate;
use ledger_domain::{MarketDayStatus, StorageKind};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthResponse {
    pub ok: bool,
    pub service: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ValidateReplayDatasetBody {
    #[serde(default)]
    pub skip_book_check: bool,
    pub replay_batches: Option<usize>,
    #[serde(default)]
    pub replay_all: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeleteRawMarketDataBody {
    #[serde(default)]
    pub cascade: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CreateJobResponse {
    pub job: JobRecord,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JobResponse {
    pub job: JobRecord,
}

#[derive(Debug, Clone, Deserialize)]
pub struct MarketDayListQuery {
    pub root: Option<String>,
    pub symbol: Option<String>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct MarketDayStatusQuery {
    #[serde(default)]
    pub verify: bool,
}

#[derive(Debug, Clone, Deserialize)]
pub struct JobsQuery {
    pub active: Option<bool>,
    pub limit: Option<usize>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataCenterMarketDay {
    pub id: String,
    pub root: String,
    pub contract: String,
    pub market_date: NaiveDate,
    pub timezone: String,
    pub data_start_ns: String,
    pub data_start_iso: String,
    pub data_end_ns: String,
    pub data_end_iso: String,
    pub rth_start_ns: String,
    pub rth_start_iso: String,
    pub rth_end_ns: String,
    pub rth_end_iso: String,
    pub market_day_status: MarketDayStatus,
    pub catalog_found: bool,
    pub raw: DataCenterRawDataLayer,
    pub replay_dataset: DataCenterReplayDatasetLayer,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum DataCenterRawDataStatus {
    Missing,
    Available,
    Error,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataCenterRawDataLayer {
    pub status: DataCenterRawDataStatus,
    pub provider: Option<String>,
    pub dataset: Option<String>,
    pub schema: Option<String>,
    pub source_symbol: Option<String>,
    pub object: Option<DataCenterObjectSummary>,
    pub updated_at_ns: Option<String>,
    pub updated_at_iso: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataCenterObjectSummary {
    pub kind: StorageKind,
    pub logical_key: String,
    pub format: String,
    pub schema_version: i64,
    pub content_sha256: String,
    pub size_bytes: i64,
    pub remote_key: String,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum DataCenterReplayDatasetStatus {
    Missing,
    Building,
    Available,
    Invalid,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataCenterReplayDatasetLayer {
    pub status: DataCenterReplayDatasetStatus,
    pub id: Option<String>,
    pub raw_object_key: Option<String>,
    pub schema_version: Option<i64>,
    pub producer: Option<String>,
    pub producer_version: Option<String>,
    pub artifact_set_hash: Option<String>,
    pub updated_at_ns: Option<String>,
    pub updated_at_iso: Option<String>,
    pub artifacts_available: bool,
    pub objects_valid: bool,
    pub artifacts: Vec<DataCenterReplayArtifact>,
    pub validation: Option<DataCenterValidationSummary>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataCenterReplayArtifact {
    pub kind: StorageKind,
    pub remote_key: Option<String>,
    pub size_bytes: Option<i64>,
    pub content_sha256: Option<String>,
    pub object_valid: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum DataCenterValidationMode {
    Light,
    Full,
}

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum DataCenterValidationStatus {
    Valid,
    Warning,
    Invalid,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DataCenterValidationSummary {
    pub mode: DataCenterValidationMode,
    pub status: DataCenterValidationStatus,
    pub created_at_ns: String,
    pub created_at_iso: String,
    pub event_count: Option<u64>,
    pub batch_count: Option<u64>,
    pub trade_count: Option<u64>,
    pub warnings: Vec<String>,
}
