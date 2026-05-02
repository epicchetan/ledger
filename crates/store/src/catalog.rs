use anyhow::{anyhow, Context, Result};
use chrono::NaiveDate;
use ledger_domain::{now_ns, MarketDay, MarketDayStatus, StorageKind};
use rusqlite::{params, Connection, OptionalExtension};
use serde::{Deserialize, Serialize};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

#[derive(Clone)]
pub struct SqliteCatalog {
    conn: Arc<Mutex<Connection>>,
}

pub type LedgerDb = SqliteCatalog;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct StoredObject {
    pub kind: StorageKind,
    pub logical_key: String,
    pub format: String,
    pub schema_version: i64,
    pub content_sha256: String,
    pub size_bytes: i64,
    pub remote_bucket: String,
    pub remote_key: String,
    pub producer: Option<String>,
    pub producer_version: Option<String>,
    pub source_provider: Option<String>,
    pub source_dataset: Option<String>,
    pub source_schema: Option<String>,
    pub source_symbol: Option<String>,
    pub metadata_json: serde_json::Value,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum StorageLayer {
    Raw,
    Replay,
}

impl StorageLayer {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Raw => "raw",
            Self::Replay => "replay",
        }
    }

    pub fn parse(s: &str) -> Result<Self> {
        Ok(match s {
            "raw" => Self::Raw,
            "replay" => Self::Replay,
            other => return Err(anyhow!("unknown StorageLayer: {other}")),
        })
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum RawMarketDataStatus {
    Missing,
    Available,
    Error,
}

impl RawMarketDataStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Missing => "missing",
            Self::Available => "available",
            Self::Error => "error",
        }
    }

    pub fn parse(s: &str) -> Result<Self> {
        Ok(match s {
            "missing" => Self::Missing,
            "available" => Self::Available,
            "error" => Self::Error,
            other => return Err(anyhow!("unknown RawMarketDataStatus: {other}")),
        })
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ReplayDatasetRecordStatus {
    Missing,
    Building,
    Available,
    Invalid,
}

impl ReplayDatasetRecordStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Missing => "missing",
            Self::Building => "building",
            Self::Available => "available",
            Self::Invalid => "invalid",
        }
    }

    pub fn parse(s: &str) -> Result<Self> {
        Ok(match s {
            "missing" => Self::Missing,
            "building" => Self::Building,
            "available" => Self::Available,
            "invalid" => Self::Invalid,
            other => return Err(anyhow!("unknown ReplayDatasetRecordStatus: {other}")),
        })
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ValidationMode {
    Light,
    Full,
}

impl ValidationMode {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Light => "light",
            Self::Full => "full",
        }
    }

    pub fn parse(s: &str) -> Result<Self> {
        Ok(match s {
            "light" => Self::Light,
            "full" => Self::Full,
            other => return Err(anyhow!("unknown ValidationMode: {other}")),
        })
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ValidationReportStatus {
    Valid,
    Warning,
    Invalid,
}

impl ValidationReportStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Valid => "valid",
            Self::Warning => "warning",
            Self::Invalid => "invalid",
        }
    }

    pub fn parse(s: &str) -> Result<Self> {
        Ok(match s {
            "valid" => Self::Valid,
            "warning" => Self::Warning,
            "invalid" => Self::Invalid,
            other => return Err(anyhow!("unknown ValidationReportStatus: {other}")),
        })
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct RawMarketDataRecord {
    pub market_day_id: String,
    pub object: StoredObject,
    pub provider: String,
    pub dataset: String,
    pub schema: String,
    pub source_symbol: String,
    pub status: RawMarketDataStatus,
    pub created_at_ns: u64,
    pub updated_at_ns: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct ReplayDatasetRecord {
    pub id: String,
    pub market_day_id: String,
    pub raw_object_remote_key: String,
    pub status: ReplayDatasetRecordStatus,
    pub schema_version: i64,
    pub producer: Option<String>,
    pub producer_version: Option<String>,
    pub artifact_set_hash: String,
    pub created_at_ns: u64,
    pub updated_at_ns: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct ValidationReportRecord {
    pub id: i64,
    pub replay_dataset_id: String,
    pub mode: ValidationMode,
    pub status: ValidationReportStatus,
    pub trigger: String,
    pub trust_status: String,
    pub summary: String,
    pub warning_count: i64,
    pub error_count: i64,
    pub report_json: serde_json::Value,
    pub created_at_ns: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ObjectDependency {
    pub object_remote_key: String,
    pub depends_on_remote_key: String,
    pub relationship: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MarketDayRecord {
    pub market_day: MarketDay,
    pub raw: Option<RawMarketDataRecord>,
    pub replay_dataset: Option<ReplayDatasetRecord>,
    pub last_validation: Option<ValidationReportRecord>,
}

#[derive(Clone, Debug, Default)]
pub struct MarketDayFilter {
    pub root: Option<String>,
    pub symbol: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ReplayDatasetObjectStatus {
    pub kind: StorageKind,
    pub remote_key: Option<String>,
    pub size_bytes: Option<i64>,
    pub content_sha256: Option<String>,
    pub object_valid: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ReplayDatasetStatus {
    pub market_day: MarketDay,
    pub catalog_found: bool,
    pub raw: Option<RawMarketDataRecord>,
    pub replay_dataset: Option<ReplayDatasetRecord>,
    pub replay_artifacts_available: bool,
    pub replay_objects_valid: bool,
    pub last_validation: Option<ValidationReportRecord>,
    pub objects: Vec<ReplayDatasetObjectStatus>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LoadedReplayDataset {
    pub replay_dataset_id: String,
    pub market_day: MarketDay,
    pub events_path: PathBuf,
    pub batches_path: PathBuf,
    pub trades_path: PathBuf,
    pub book_check_path: PathBuf,
    pub objects: Vec<StoredObject>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum LedgerJobStatus {
    Queued,
    Running,
    Succeeded,
    Failed,
}

impl LedgerJobStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Queued => "queued",
            Self::Running => "running",
            Self::Succeeded => "succeeded",
            Self::Failed => "failed",
        }
    }

    pub fn parse(s: &str) -> Result<Self> {
        Ok(match s {
            "queued" => Self::Queued,
            "running" => Self::Running,
            "succeeded" => Self::Succeeded,
            "failed" => Self::Failed,
            other => return Err(anyhow!("unknown LedgerJobStatus: {other}")),
        })
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct LedgerJobEvent {
    pub id: i64,
    pub job_id: String,
    pub created_at_ns: u64,
    pub level: String,
    pub message: String,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct LedgerJobRecord {
    pub id: String,
    pub kind: String,
    pub status: LedgerJobStatus,
    pub market_day_id: Option<String>,
    pub created_at_ns: u64,
    pub started_at_ns: Option<u64>,
    pub finished_at_ns: Option<u64>,
    pub request_json: serde_json::Value,
    pub result_json: Option<serde_json::Value>,
    pub error: Option<String>,
    pub events: Vec<LedgerJobEvent>,
}

impl SqliteCatalog {
    pub fn open(path: impl AsRef<Path>) -> Result<Self> {
        let path = path.as_ref();
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)?;
        }
        let conn = Connection::open(path)
            .with_context(|| format!("opening SQLite catalog {}", path.display()))?;
        let catalog = Self {
            conn: Arc::new(Mutex::new(conn)),
        };
        catalog.migrate()?;
        Ok(catalog)
    }

    fn migrate(&self) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute_batch(
            r#"
            PRAGMA foreign_keys = ON;

            CREATE TABLE IF NOT EXISTS market_days (
                id TEXT PRIMARY KEY,
                root TEXT NOT NULL,
                contract_symbol TEXT NOT NULL,
                market_date TEXT NOT NULL,
                timezone TEXT NOT NULL,
                data_start_ns INTEGER NOT NULL,
                data_end_ns INTEGER NOT NULL,
                rth_start_ns INTEGER NOT NULL,
                rth_end_ns INTEGER NOT NULL,
                status TEXT NOT NULL,
                ready INTEGER NOT NULL,
                last_accessed_ns INTEGER,
                created_at_ns INTEGER NOT NULL,
                updated_at_ns INTEGER NOT NULL,
                metadata_json TEXT NOT NULL
            );

            CREATE INDEX IF NOT EXISTS idx_market_days_root_date
                ON market_days(root, market_date);
            CREATE INDEX IF NOT EXISTS idx_market_days_symbol_date
                ON market_days(contract_symbol, market_date);
            CREATE INDEX IF NOT EXISTS idx_market_days_ready
                ON market_days(ready);

            CREATE TABLE IF NOT EXISTS objects (
                remote_key TEXT PRIMARY KEY,
                market_day_id TEXT NOT NULL,
                kind TEXT NOT NULL,
                logical_key TEXT NOT NULL,
                format TEXT NOT NULL,
                schema_version INTEGER NOT NULL,
                content_sha256 TEXT NOT NULL,
                size_bytes INTEGER NOT NULL,
                remote_bucket TEXT NOT NULL,
                producer TEXT,
                producer_version TEXT,
                source_provider TEXT,
                source_dataset TEXT,
                source_schema TEXT,
                source_symbol TEXT,
                metadata_json TEXT NOT NULL,
                created_at_ns INTEGER NOT NULL,
                updated_at_ns INTEGER NOT NULL,
                UNIQUE(market_day_id, kind, logical_key, schema_version),
                FOREIGN KEY(market_day_id) REFERENCES market_days(id)
            );

            CREATE INDEX IF NOT EXISTS idx_objects_market_day
                ON objects(market_day_id);
            CREATE INDEX IF NOT EXISTS idx_objects_kind
                ON objects(kind);
            CREATE INDEX IF NOT EXISTS idx_objects_sha
                ON objects(content_sha256);

            CREATE TABLE IF NOT EXISTS object_dependencies (
                object_remote_key TEXT NOT NULL,
                depends_on_remote_key TEXT NOT NULL,
                relationship TEXT NOT NULL,
                PRIMARY KEY(object_remote_key, depends_on_remote_key, relationship),
                FOREIGN KEY(object_remote_key) REFERENCES objects(remote_key),
                FOREIGN KEY(depends_on_remote_key) REFERENCES objects(remote_key)
            );

            CREATE TABLE IF NOT EXISTS ingest_runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                market_day_id TEXT NOT NULL,
                started_at_ns INTEGER NOT NULL,
                finished_at_ns INTEGER,
                status TEXT NOT NULL,
                error_message TEXT,
                FOREIGN KEY(market_day_id) REFERENCES market_days(id)
            );

            CREATE TABLE IF NOT EXISTS raw_market_data (
                market_day_id TEXT PRIMARY KEY,
                object_remote_key TEXT NOT NULL,
                provider TEXT NOT NULL,
                dataset TEXT NOT NULL,
                schema TEXT NOT NULL,
                source_symbol TEXT NOT NULL,
                status TEXT NOT NULL,
                created_at_ns INTEGER NOT NULL,
                updated_at_ns INTEGER NOT NULL,
                FOREIGN KEY(market_day_id) REFERENCES market_days(id),
                FOREIGN KEY(object_remote_key) REFERENCES objects(remote_key)
            );

            CREATE TABLE IF NOT EXISTS replay_datasets (
                id TEXT PRIMARY KEY,
                market_day_id TEXT NOT NULL UNIQUE,
                raw_object_remote_key TEXT NOT NULL,
                status TEXT NOT NULL,
                schema_version INTEGER NOT NULL,
                producer TEXT,
                producer_version TEXT,
                artifact_set_hash TEXT NOT NULL,
                created_at_ns INTEGER NOT NULL,
                updated_at_ns INTEGER NOT NULL,
                FOREIGN KEY(market_day_id) REFERENCES market_days(id),
                FOREIGN KEY(raw_object_remote_key) REFERENCES objects(remote_key)
            );

            CREATE INDEX IF NOT EXISTS idx_replay_datasets_market_day
                ON replay_datasets(market_day_id);

            CREATE TABLE IF NOT EXISTS replay_dataset_artifacts (
                replay_dataset_id TEXT NOT NULL,
                kind TEXT NOT NULL,
                object_remote_key TEXT NOT NULL,
                metadata_json TEXT NOT NULL,
                created_at_ns INTEGER NOT NULL,
                PRIMARY KEY(replay_dataset_id, kind),
                FOREIGN KEY(replay_dataset_id) REFERENCES replay_datasets(id),
                FOREIGN KEY(object_remote_key) REFERENCES objects(remote_key)
            );

            CREATE TABLE IF NOT EXISTS validation_reports (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                replay_dataset_id TEXT NOT NULL,
                mode TEXT NOT NULL,
                status TEXT NOT NULL,
                trigger TEXT NOT NULL DEFAULT 'manual',
                trust_status TEXT NOT NULL DEFAULT 'replay_dataset_available',
                summary TEXT NOT NULL DEFAULT '',
                warning_count INTEGER NOT NULL DEFAULT 0,
                error_count INTEGER NOT NULL DEFAULT 0,
                report_json TEXT NOT NULL,
                created_at_ns INTEGER NOT NULL,
                FOREIGN KEY(replay_dataset_id) REFERENCES replay_datasets(id)
            );

            CREATE INDEX IF NOT EXISTS idx_validation_reports_dataset
                ON validation_reports(replay_dataset_id, created_at_ns DESC);

            CREATE TABLE IF NOT EXISTS jobs (
                id TEXT PRIMARY KEY,
                kind TEXT NOT NULL,
                status TEXT NOT NULL,
                market_day_id TEXT,
                created_at_ns INTEGER NOT NULL,
                started_at_ns INTEGER,
                finished_at_ns INTEGER,
                progress_json TEXT NOT NULL DEFAULT '[]',
                request_json TEXT NOT NULL DEFAULT '{}',
                result_json TEXT,
                error TEXT,
                FOREIGN KEY(market_day_id) REFERENCES market_days(id)
            );

            CREATE TABLE IF NOT EXISTS job_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                job_id TEXT NOT NULL,
                created_at_ns INTEGER NOT NULL,
                level TEXT NOT NULL,
                message TEXT NOT NULL,
                FOREIGN KEY(job_id) REFERENCES jobs(id)
            );

            CREATE INDEX IF NOT EXISTS idx_jobs_status
                ON jobs(status, created_at_ns DESC);
            CREATE INDEX IF NOT EXISTS idx_job_events_job
                ON job_events(job_id, created_at_ns ASC);
            "#,
        )?;
        conn.execute(
            "ALTER TABLE jobs ADD COLUMN request_json TEXT NOT NULL DEFAULT '{}'",
            [],
        )
        .ok();
        conn.execute(
            "ALTER TABLE validation_reports ADD COLUMN trigger TEXT NOT NULL DEFAULT 'manual'",
            [],
        )
        .ok();
        conn.execute(
            "ALTER TABLE validation_reports ADD COLUMN trust_status TEXT NOT NULL DEFAULT 'replay_dataset_available'",
            [],
        )
        .ok();
        conn.execute(
            "ALTER TABLE validation_reports ADD COLUMN summary TEXT NOT NULL DEFAULT ''",
            [],
        )
        .ok();
        conn.execute(
            "ALTER TABLE validation_reports ADD COLUMN warning_count INTEGER NOT NULL DEFAULT 0",
            [],
        )
        .ok();
        conn.execute(
            "ALTER TABLE validation_reports ADD COLUMN error_count INTEGER NOT NULL DEFAULT 0",
            [],
        )
        .ok();
        conn.execute(
            r#"
            INSERT OR IGNORE INTO raw_market_data (
                market_day_id, object_remote_key, provider, dataset, schema,
                source_symbol, status, created_at_ns, updated_at_ns
            )
            SELECT
                market_day_id,
                remote_key,
                COALESCE(source_provider, 'databento'),
                COALESCE(source_dataset, 'GLBX.MDP3'),
                COALESCE(source_schema, 'mbo'),
                COALESCE(source_symbol, ''),
                'available',
                created_at_ns,
                updated_at_ns
            FROM objects
            WHERE kind = 'raw_dbn'
            "#,
            [],
        )?;
        conn.execute(
            r#"
            INSERT OR IGNORE INTO replay_datasets (
                id, market_day_id, raw_object_remote_key, status, schema_version,
                producer, producer_version, artifact_set_hash, created_at_ns, updated_at_ns
            )
            SELECT
                m.id || ':replay:v1',
                m.id,
                raw.remote_key,
                'available',
                1,
                event_store.producer,
                event_store.producer_version,
                'legacy',
                event_store.created_at_ns,
                event_store.updated_at_ns
            FROM market_days m
            JOIN objects raw ON raw.market_day_id = m.id AND raw.kind = 'raw_dbn'
            JOIN objects event_store ON event_store.market_day_id = m.id AND event_store.kind = 'event_store'
            WHERE EXISTS (SELECT 1 FROM objects o WHERE o.market_day_id = m.id AND o.kind = 'batch_index')
              AND EXISTS (SELECT 1 FROM objects o WHERE o.market_day_id = m.id AND o.kind = 'trade_index')
              AND EXISTS (SELECT 1 FROM objects o WHERE o.market_day_id = m.id AND o.kind = 'book_check')
            "#,
            [],
        )?;
        conn.execute(
            r#"
            INSERT OR IGNORE INTO replay_dataset_artifacts (
                replay_dataset_id, kind, object_remote_key, metadata_json, created_at_ns
            )
            SELECT
                d.id,
                o.kind,
                o.remote_key,
                o.metadata_json,
                o.created_at_ns
            FROM replay_datasets d
            JOIN objects o ON o.market_day_id = d.market_day_id
            WHERE o.kind IN ('event_store', 'batch_index', 'trade_index', 'book_check')
            "#,
            [],
        )?;
        Ok(())
    }

    pub fn upsert_market_day(&self, md: &MarketDay, ready: bool) -> Result<()> {
        let now = now_ns() as i64;
        let conn = self.conn.lock().unwrap();
        conn.execute(
            r#"
            INSERT INTO market_days (
                id, root, contract_symbol, market_date, timezone,
                data_start_ns, data_end_ns, rth_start_ns, rth_end_ns,
                status, ready, created_at_ns, updated_at_ns, metadata_json
            )
            VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?12, ?13)
            ON CONFLICT(id) DO UPDATE SET
                root=excluded.root,
                contract_symbol=excluded.contract_symbol,
                market_date=excluded.market_date,
                timezone=excluded.timezone,
                data_start_ns=excluded.data_start_ns,
                data_end_ns=excluded.data_end_ns,
                rth_start_ns=excluded.rth_start_ns,
                rth_end_ns=excluded.rth_end_ns,
                status=excluded.status,
                ready=excluded.ready,
                updated_at_ns=excluded.updated_at_ns,
                metadata_json=excluded.metadata_json
            "#,
            params![
                md.id,
                md.root,
                md.contract_symbol,
                md.market_date.to_string(),
                md.timezone,
                md.data_start_ns as i64,
                md.data_end_ns as i64,
                md.rth_start_ns as i64,
                md.rth_end_ns as i64,
                md.status.as_str(),
                i64::from(ready),
                now,
                md.metadata_json.to_string(),
            ],
        )?;
        Ok(())
    }

    pub fn mark_market_day_status(
        &self,
        md: &mut MarketDay,
        status: MarketDayStatus,
        ready: bool,
    ) -> Result<()> {
        md.status = status;
        self.upsert_market_day(md, ready)
    }

    pub fn market_day(&self, id: &str) -> Result<Option<MarketDayRecord>> {
        let conn = self.conn.lock().unwrap();
        let record = conn
            .query_row(
                "SELECT id, root, contract_symbol, market_date, timezone,
                    data_start_ns, data_end_ns, rth_start_ns, rth_end_ns,
                    status, ready, last_accessed_ns, metadata_json
             FROM market_days WHERE id = ?1",
                params![id],
                row_to_market_day_record,
            )
            .optional()?;
        drop(conn);
        record
            .map(|record| self.enrich_market_day_record(record))
            .transpose()
    }

    pub fn list_market_days(&self, filter: &MarketDayFilter) -> Result<Vec<MarketDayRecord>> {
        let conn = self.conn.lock().unwrap();
        let mut sql = String::from(
            "SELECT id, root, contract_symbol, market_date, timezone,
                    data_start_ns, data_end_ns, rth_start_ns, rth_end_ns,
                    status, ready, last_accessed_ns, metadata_json
             FROM market_days WHERE 1=1",
        );
        let mut values: Vec<Box<dyn rusqlite::ToSql>> = Vec::new();
        if let Some(root) = &filter.root {
            sql.push_str(" AND root = ?");
            values.push(Box::new(root.clone()));
        }
        if let Some(symbol) = &filter.symbol {
            sql.push_str(" AND contract_symbol = ?");
            values.push(Box::new(symbol.clone()));
        }
        sql.push_str(" ORDER BY market_date DESC, contract_symbol ASC");
        let params = rusqlite::params_from_iter(values.iter().map(|v| v.as_ref()));
        let mut stmt = conn.prepare(&sql)?;
        let rows = stmt.query_map(params, row_to_market_day_record)?;
        let records = rows.collect::<std::result::Result<Vec<_>, _>>()?;
        drop(stmt);
        drop(conn);
        records
            .into_iter()
            .map(|record| self.enrich_market_day_record(record))
            .collect()
    }

    pub fn upsert_object(&self, md: &MarketDay, object: &StoredObject) -> Result<()> {
        self.upsert_market_day(md, md.status == MarketDayStatus::Ready)?;
        let now = now_ns() as i64;
        let conn = self.conn.lock().unwrap();
        conn.execute(
            r#"
            INSERT INTO objects (
                remote_key, market_day_id, kind, logical_key, format, schema_version,
                content_sha256, size_bytes, remote_bucket, producer, producer_version,
                source_provider, source_dataset, source_schema, source_symbol,
                metadata_json, created_at_ns, updated_at_ns
            )
            VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10, ?11, ?12, ?13, ?14, ?15, ?16, ?17, ?17)
            ON CONFLICT(remote_key) DO UPDATE SET
                market_day_id=excluded.market_day_id,
                kind=excluded.kind,
                logical_key=excluded.logical_key,
                format=excluded.format,
                schema_version=excluded.schema_version,
                content_sha256=excluded.content_sha256,
                size_bytes=excluded.size_bytes,
                remote_bucket=excluded.remote_bucket,
                producer=excluded.producer,
                producer_version=excluded.producer_version,
                source_provider=excluded.source_provider,
                source_dataset=excluded.source_dataset,
                source_schema=excluded.source_schema,
                source_symbol=excluded.source_symbol,
                metadata_json=excluded.metadata_json,
                updated_at_ns=excluded.updated_at_ns
            "#,
            params![
                object.remote_key,
                md.id,
                object.kind.as_str(),
                object.logical_key,
                object.format,
                object.schema_version,
                object.content_sha256,
                object.size_bytes,
                object.remote_bucket,
                object.producer,
                object.producer_version,
                object.source_provider,
                object.source_dataset,
                object.source_schema,
                object.source_symbol,
                object.metadata_json.to_string(),
                now,
            ],
        )?;
        Ok(())
    }

    pub fn object(&self, market_day_id: &str, kind: StorageKind) -> Result<Option<StoredObject>> {
        let conn = self.conn.lock().unwrap();
        conn.query_row(
            "SELECT kind, logical_key, format, schema_version, content_sha256,
                    size_bytes, remote_bucket, remote_key, producer, producer_version,
                    source_provider, source_dataset, source_schema, source_symbol, metadata_json
             FROM objects WHERE market_day_id = ?1 AND kind = ?2
             ORDER BY updated_at_ns DESC LIMIT 1",
            params![market_day_id, kind.as_str()],
            row_to_object,
        )
        .optional()
        .map_err(Into::into)
    }

    pub fn objects_for_market_day(&self, market_day_id: &str) -> Result<Vec<StoredObject>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT kind, logical_key, format, schema_version, content_sha256,
                    size_bytes, remote_bucket, remote_key, producer, producer_version,
                    source_provider, source_dataset, source_schema, source_symbol, metadata_json
             FROM objects WHERE market_day_id = ?1
             ORDER BY kind ASC",
        )?;
        let rows = stmt.query_map(params![market_day_id], row_to_object)?;
        rows.collect::<std::result::Result<Vec<_>, _>>()
            .map_err(Into::into)
    }

    pub fn upsert_raw_market_data(&self, md: &MarketDay, object: &StoredObject) -> Result<()> {
        if object.kind != StorageKind::RawDbn {
            return Err(anyhow!(
                "raw market data must reference raw_dbn, got {}",
                object.kind.as_str()
            ));
        }
        let now = now_ns() as i64;
        let provider = object
            .source_provider
            .as_deref()
            .unwrap_or("unknown")
            .to_string();
        let dataset = object
            .source_dataset
            .as_deref()
            .unwrap_or("unknown")
            .to_string();
        let schema = object
            .source_schema
            .as_deref()
            .unwrap_or("unknown")
            .to_string();
        let source_symbol = object
            .source_symbol
            .as_deref()
            .unwrap_or(&md.contract_symbol)
            .to_string();

        let conn = self.conn.lock().unwrap();
        conn.execute(
            r#"
            INSERT INTO raw_market_data (
                market_day_id, object_remote_key, provider, dataset, schema,
                source_symbol, status, created_at_ns, updated_at_ns
            )
            VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?8)
            ON CONFLICT(market_day_id) DO UPDATE SET
                object_remote_key=excluded.object_remote_key,
                provider=excluded.provider,
                dataset=excluded.dataset,
                schema=excluded.schema,
                source_symbol=excluded.source_symbol,
                status=excluded.status,
                updated_at_ns=excluded.updated_at_ns
            "#,
            params![
                md.id,
                object.remote_key,
                provider,
                dataset,
                schema,
                source_symbol,
                RawMarketDataStatus::Available.as_str(),
                now,
            ],
        )?;
        Ok(())
    }

    pub fn raw_market_data(&self, market_day_id: &str) -> Result<Option<RawMarketDataRecord>> {
        let conn = self.conn.lock().unwrap();
        conn.query_row(
            r#"
            SELECT r.market_day_id, r.provider, r.dataset, r.schema,
                   r.source_symbol, r.status, r.created_at_ns, r.updated_at_ns,
                   o.kind, o.logical_key, o.format, o.schema_version, o.content_sha256,
                   o.size_bytes, o.remote_bucket, o.remote_key, o.producer, o.producer_version,
                   o.source_provider, o.source_dataset, o.source_schema, o.source_symbol,
                   o.metadata_json
            FROM raw_market_data r
            JOIN objects o ON o.remote_key = r.object_remote_key
            WHERE r.market_day_id = ?1
            "#,
            params![market_day_id],
            row_to_raw_market_data,
        )
        .optional()
        .map_err(Into::into)
    }

    pub fn upsert_replay_dataset(
        &self,
        md: &MarketDay,
        raw: &StoredObject,
        artifacts: &[StoredObject],
    ) -> Result<ReplayDatasetRecord> {
        if raw.kind != StorageKind::RawDbn {
            return Err(anyhow!(
                "replay dataset must depend on raw_dbn, got {}",
                raw.kind.as_str()
            ));
        }
        let mut artifact_keys: Vec<_> = artifacts.iter().map(|o| o.remote_key.as_str()).collect();
        artifact_keys.sort_unstable();
        let artifact_set_hash = artifact_keys.join("|");
        let id = format!("{}:replay:v1", md.id);
        let now = now_ns() as i64;
        let producer = artifacts.iter().find_map(|o| o.producer.clone());
        let producer_version = artifacts.iter().find_map(|o| o.producer_version.clone());
        let schema_version = artifacts
            .iter()
            .map(|o| o.schema_version)
            .max()
            .unwrap_or(1);

        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction()?;
        tx.execute(
            r#"
            INSERT INTO replay_datasets (
                id, market_day_id, raw_object_remote_key, status, schema_version,
                producer, producer_version, artifact_set_hash, created_at_ns, updated_at_ns
            )
            VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?9)
            ON CONFLICT(market_day_id) DO UPDATE SET
                raw_object_remote_key=excluded.raw_object_remote_key,
                status=excluded.status,
                schema_version=excluded.schema_version,
                producer=excluded.producer,
                producer_version=excluded.producer_version,
                artifact_set_hash=excluded.artifact_set_hash,
                updated_at_ns=excluded.updated_at_ns
            "#,
            params![
                id,
                md.id,
                raw.remote_key,
                ReplayDatasetRecordStatus::Available.as_str(),
                schema_version,
                producer,
                producer_version,
                artifact_set_hash,
                now,
            ],
        )?;
        tx.execute(
            "DELETE FROM replay_dataset_artifacts WHERE replay_dataset_id = ?1",
            params![id],
        )?;
        for artifact in artifacts {
            if !is_replay_artifact(&artifact.kind) {
                return Err(anyhow!(
                    "{} is not a replay dataset artifact",
                    artifact.kind.as_str()
                ));
            }
            tx.execute(
                r#"
                INSERT INTO replay_dataset_artifacts (
                    replay_dataset_id, kind, object_remote_key, metadata_json, created_at_ns
                )
                VALUES (?1, ?2, ?3, ?4, ?5)
                "#,
                params![
                    id,
                    artifact.kind.as_str(),
                    artifact.remote_key,
                    artifact.metadata_json.to_string(),
                    now,
                ],
            )?;
        }
        tx.commit()?;
        drop(conn);
        self.replay_dataset(&md.id)?
            .ok_or_else(|| anyhow!("replay dataset {} missing after upsert", id))
    }

    pub fn replay_dataset(&self, market_day_id: &str) -> Result<Option<ReplayDatasetRecord>> {
        let conn = self.conn.lock().unwrap();
        conn.query_row(
            "SELECT id, market_day_id, raw_object_remote_key, status, schema_version,
                    producer, producer_version, artifact_set_hash, created_at_ns, updated_at_ns
             FROM replay_datasets
             WHERE market_day_id = ?1
             ORDER BY updated_at_ns DESC LIMIT 1",
            params![market_day_id],
            row_to_replay_dataset,
        )
        .optional()
        .map_err(Into::into)
    }

    pub fn replay_dataset_artifacts(&self, replay_dataset_id: &str) -> Result<Vec<StoredObject>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            r#"
            SELECT o.kind, o.logical_key, o.format, o.schema_version, o.content_sha256,
                   o.size_bytes, o.remote_bucket, o.remote_key, o.producer, o.producer_version,
                   o.source_provider, o.source_dataset, o.source_schema, o.source_symbol,
                   o.metadata_json
            FROM replay_dataset_artifacts a
            JOIN objects o ON o.remote_key = a.object_remote_key
            WHERE a.replay_dataset_id = ?1
            ORDER BY a.kind ASC
            "#,
        )?;
        let rows = stmt.query_map(params![replay_dataset_id], row_to_object)?;
        rows.collect::<std::result::Result<Vec<_>, _>>()
            .map_err(Into::into)
    }

    pub fn insert_validation_report(
        &self,
        replay_dataset_id: &str,
        mode: ValidationMode,
        status: ValidationReportStatus,
        trigger: &str,
        trust_status: &str,
        summary: &str,
        warning_count: usize,
        error_count: usize,
        report_json: serde_json::Value,
    ) -> Result<()> {
        let now = now_ns() as i64;
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO validation_reports
             (replay_dataset_id, mode, status, trigger, trust_status, summary,
              warning_count, error_count, report_json, created_at_ns)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9, ?10)",
            params![
                replay_dataset_id,
                mode.as_str(),
                status.as_str(),
                trigger,
                trust_status,
                summary,
                warning_count as i64,
                error_count as i64,
                report_json.to_string(),
                now,
            ],
        )?;
        Ok(())
    }

    pub fn latest_validation_report(
        &self,
        replay_dataset_id: &str,
    ) -> Result<Option<ValidationReportRecord>> {
        let conn = self.conn.lock().unwrap();
        conn.query_row(
            "SELECT id, replay_dataset_id, mode, status, trigger, trust_status, summary,
                    warning_count, error_count, report_json, created_at_ns
             FROM validation_reports
             WHERE replay_dataset_id = ?1
             ORDER BY created_at_ns DESC LIMIT 1",
            params![replay_dataset_id],
            row_to_validation_report,
        )
        .optional()
        .map_err(Into::into)
    }

    pub fn add_dependency(
        &self,
        object: &StoredObject,
        depends_on: &StoredObject,
        relationship: &str,
    ) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT OR IGNORE INTO object_dependencies
             (object_remote_key, depends_on_remote_key, relationship)
             VALUES (?1, ?2, ?3)",
            params![object.remote_key, depends_on.remote_key, relationship],
        )?;
        Ok(())
    }

    pub fn touch_market_day(&self, market_day_id: &str) -> Result<()> {
        let now = now_ns() as i64;
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "UPDATE market_days SET last_accessed_ns = ?1, updated_at_ns = ?1 WHERE id = ?2",
            params![now, market_day_id],
        )?;
        Ok(())
    }

    pub fn remove_objects(&self, remote_keys: &[String]) -> Result<usize> {
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction()?;
        let mut deleted = 0;
        for remote_key in remote_keys {
            tx.execute(
                "DELETE FROM object_dependencies
                 WHERE object_remote_key = ?1 OR depends_on_remote_key = ?1",
                params![remote_key],
            )?;
            deleted += tx.execute(
                "DELETE FROM objects WHERE remote_key = ?1",
                params![remote_key],
            )?;
        }
        tx.commit()?;
        Ok(deleted)
    }

    pub fn remove_replay_dataset(&self, market_day_id: &str) -> Result<Vec<String>> {
        let replay_dataset = self.replay_dataset(market_day_id)?;
        let Some(replay_dataset) = replay_dataset else {
            return Ok(Vec::new());
        };
        let artifacts = self.replay_dataset_artifacts(&replay_dataset.id)?;
        let remote_keys: Vec<_> = artifacts
            .iter()
            .map(|object| object.remote_key.clone())
            .collect();
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction()?;
        tx.execute(
            "DELETE FROM validation_reports WHERE replay_dataset_id = ?1",
            params![replay_dataset.id],
        )?;
        tx.execute(
            "DELETE FROM replay_dataset_artifacts WHERE replay_dataset_id = ?1",
            params![replay_dataset.id],
        )?;
        tx.execute(
            "DELETE FROM replay_datasets WHERE id = ?1",
            params![replay_dataset.id],
        )?;
        for remote_key in &remote_keys {
            tx.execute(
                "DELETE FROM object_dependencies
                 WHERE object_remote_key = ?1 OR depends_on_remote_key = ?1",
                params![remote_key],
            )?;
            tx.execute(
                "DELETE FROM objects WHERE remote_key = ?1",
                params![remote_key],
            )?;
        }
        tx.commit()?;
        Ok(remote_keys)
    }

    pub fn remove_raw_market_data(&self, market_day_id: &str) -> Result<Option<String>> {
        let raw = self.raw_market_data(market_day_id)?;
        let Some(raw) = raw else {
            return Ok(None);
        };
        let remote_key = raw.object.remote_key;
        let mut conn = self.conn.lock().unwrap();
        let tx = conn.transaction()?;
        tx.execute(
            "DELETE FROM raw_market_data WHERE market_day_id = ?1",
            params![market_day_id],
        )?;
        tx.execute(
            "DELETE FROM object_dependencies
             WHERE object_remote_key = ?1 OR depends_on_remote_key = ?1",
            params![remote_key],
        )?;
        tx.execute(
            "DELETE FROM objects WHERE remote_key = ?1",
            params![remote_key],
        )?;
        tx.commit()?;
        Ok(Some(remote_key))
    }

    pub fn create_job(
        &self,
        id: &str,
        kind: &str,
        market_day_id: Option<&str>,
        request_json: serde_json::Value,
    ) -> Result<LedgerJobRecord> {
        let now = now_ns() as i64;
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO jobs
             (id, kind, status, market_day_id, created_at_ns, progress_json, request_json)
             VALUES (?1, ?2, ?3, ?4, ?5, '[]', ?6)",
            params![
                id,
                kind,
                LedgerJobStatus::Queued.as_str(),
                market_day_id,
                now,
                request_json.to_string()
            ],
        )?;
        drop(conn);
        self.job(id)?
            .ok_or_else(|| anyhow!("job {id} missing after insert"))
    }

    pub fn mark_job_running(&self, id: &str) -> Result<()> {
        let now = now_ns() as i64;
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "UPDATE jobs SET status = ?1, started_at_ns = COALESCE(started_at_ns, ?2) WHERE id = ?3",
            params![LedgerJobStatus::Running.as_str(), now, id],
        )?;
        Ok(())
    }

    pub fn complete_job(&self, id: &str, result_json: serde_json::Value) -> Result<()> {
        let now = now_ns() as i64;
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "UPDATE jobs
             SET status = ?1, finished_at_ns = ?2, result_json = ?3, error = NULL
             WHERE id = ?4",
            params![
                LedgerJobStatus::Succeeded.as_str(),
                now,
                result_json.to_string(),
                id
            ],
        )?;
        Ok(())
    }

    pub fn fail_job(&self, id: &str, error: &str) -> Result<()> {
        let now = now_ns() as i64;
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "UPDATE jobs
             SET status = ?1, finished_at_ns = ?2, error = ?3
             WHERE id = ?4",
            params![LedgerJobStatus::Failed.as_str(), now, error, id],
        )?;
        Ok(())
    }

    pub fn append_job_event(&self, job_id: &str, level: &str, message: &str) -> Result<()> {
        let now = now_ns() as i64;
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO job_events (job_id, created_at_ns, level, message)
             VALUES (?1, ?2, ?3, ?4)",
            params![job_id, now, level, message],
        )?;
        Ok(())
    }

    pub fn job(&self, id: &str) -> Result<Option<LedgerJobRecord>> {
        let conn = self.conn.lock().unwrap();
        let mut job = conn
            .query_row(
                "SELECT id, kind, status, market_day_id, created_at_ns, started_at_ns,
                        finished_at_ns, request_json, result_json, error
                 FROM jobs WHERE id = ?1",
                params![id],
                row_to_job_record,
            )
            .optional()?;
        drop(conn);
        if let Some(job) = &mut job {
            job.events = self.job_events(&job.id)?;
        }
        Ok(job)
    }

    pub fn active_job_for_market_day(
        &self,
        market_day_id: &str,
    ) -> Result<Option<LedgerJobRecord>> {
        let conn = self.conn.lock().unwrap();
        let mut job = conn
            .query_row(
                "SELECT id, kind, status, market_day_id, created_at_ns, started_at_ns,
                        finished_at_ns, request_json, result_json, error
                 FROM jobs
                 WHERE market_day_id = ?1 AND status IN ('queued', 'running')
                 ORDER BY created_at_ns DESC
                 LIMIT 1",
                params![market_day_id],
                row_to_job_record,
            )
            .optional()?;
        drop(conn);
        if let Some(job) = &mut job {
            job.events = self.job_events(&job.id)?;
        }
        Ok(job)
    }

    pub fn jobs(&self, active_only: bool, limit: Option<usize>) -> Result<Vec<LedgerJobRecord>> {
        let conn = self.conn.lock().unwrap();
        let base = if active_only {
            "SELECT id, kind, status, market_day_id, created_at_ns, started_at_ns,
                    finished_at_ns, request_json, result_json, error
             FROM jobs WHERE status IN ('queued', 'running')
             ORDER BY created_at_ns DESC"
        } else {
            "SELECT id, kind, status, market_day_id, created_at_ns, started_at_ns,
                    finished_at_ns, request_json, result_json, error
             FROM jobs ORDER BY created_at_ns DESC"
        };
        let mut jobs = if let Some(limit) = limit {
            let sql = format!("{base} LIMIT ?1");
            let mut stmt = conn.prepare(&sql)?;
            let rows = stmt.query_map(params![clamp_job_limit(limit)], row_to_job_record)?;
            rows.collect::<std::result::Result<Vec<_>, _>>()?
        } else {
            let mut stmt = conn.prepare(base)?;
            let rows = stmt.query_map([], row_to_job_record)?;
            rows.collect::<std::result::Result<Vec<_>, _>>()?
        };
        drop(conn);
        for job in &mut jobs {
            job.events = self.job_events(&job.id)?;
        }
        Ok(jobs)
    }

    pub fn jobs_for_market_day(
        &self,
        market_day_id: &str,
        active_only: bool,
        limit: Option<usize>,
    ) -> Result<Vec<LedgerJobRecord>> {
        let conn = self.conn.lock().unwrap();
        let base = if active_only {
            "SELECT id, kind, status, market_day_id, created_at_ns, started_at_ns,
                    finished_at_ns, request_json, result_json, error
             FROM jobs
             WHERE market_day_id = ?1 AND status IN ('queued', 'running')
             ORDER BY created_at_ns DESC"
        } else {
            "SELECT id, kind, status, market_day_id, created_at_ns, started_at_ns,
                    finished_at_ns, request_json, result_json, error
             FROM jobs
             WHERE market_day_id = ?1
             ORDER BY created_at_ns DESC"
        };
        let mut jobs = if let Some(limit) = limit {
            let sql = format!("{base} LIMIT ?2");
            let mut stmt = conn.prepare(&sql)?;
            let rows = stmt.query_map(
                params![market_day_id, clamp_job_limit(limit)],
                row_to_job_record,
            )?;
            rows.collect::<std::result::Result<Vec<_>, _>>()?
        } else {
            let mut stmt = conn.prepare(base)?;
            let rows = stmt.query_map(params![market_day_id], row_to_job_record)?;
            rows.collect::<std::result::Result<Vec<_>, _>>()?
        };
        drop(conn);
        for job in &mut jobs {
            job.events = self.job_events(&job.id)?;
        }
        Ok(jobs)
    }

    pub fn mark_stale_jobs_failed(&self, message: &str) -> Result<usize> {
        let stale = self.jobs(true, None)?;
        let mut count = 0;
        for job in stale {
            self.fail_job(&job.id, message)?;
            self.append_job_event(&job.id, "error", message)?;
            count += 1;
        }
        Ok(count)
    }

    fn job_events(&self, job_id: &str) -> Result<Vec<LedgerJobEvent>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT id, job_id, created_at_ns, level, message
             FROM job_events WHERE job_id = ?1
             ORDER BY created_at_ns ASC, id ASC",
        )?;
        let rows = stmt.query_map(params![job_id], row_to_job_event)?;
        rows.collect::<std::result::Result<Vec<_>, _>>()
            .map_err(Into::into)
    }

    pub fn create_ingest_run(&self, md: &MarketDay) -> Result<i64> {
        self.upsert_market_day(md, false)?;
        let now = now_ns() as i64;
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "INSERT INTO ingest_runs (market_day_id, started_at_ns, status)
             VALUES (?1, ?2, 'running')",
            params![md.id, now],
        )?;
        Ok(conn.last_insert_rowid())
    }

    pub fn finish_ingest_run(&self, run_id: i64, status: &str, error: Option<&str>) -> Result<()> {
        let now = now_ns() as i64;
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "UPDATE ingest_runs
             SET finished_at_ns = ?1, status = ?2, error_message = ?3
             WHERE id = ?4",
            params![now, status, error, run_id],
        )?;
        Ok(())
    }

    fn enrich_market_day_record(&self, record: MarketDayRecord) -> Result<MarketDayRecord> {
        let raw = self.raw_market_data(&record.market_day.id)?;
        let replay_dataset = self.replay_dataset(&record.market_day.id)?;
        let last_validation = replay_dataset
            .as_ref()
            .map(|dataset| self.latest_validation_report(&dataset.id))
            .transpose()?
            .flatten();
        Ok(MarketDayRecord {
            market_day: record.market_day,
            raw,
            replay_dataset,
            last_validation,
        })
    }
}

pub fn is_replay_artifact(kind: &StorageKind) -> bool {
    matches!(
        kind,
        StorageKind::EventStore
            | StorageKind::BatchIndex
            | StorageKind::TradeIndex
            | StorageKind::BookCheck
    )
}

pub fn required_replay_kinds() -> [StorageKind; 4] {
    [
        StorageKind::EventStore,
        StorageKind::BatchIndex,
        StorageKind::TradeIndex,
        StorageKind::BookCheck,
    ]
}

fn row_to_market_day_record(row: &rusqlite::Row<'_>) -> rusqlite::Result<MarketDayRecord> {
    let market_date: String = row.get(3)?;
    let status: String = row.get(9)?;
    let metadata_json: String = row.get(12)?;
    let mut market_day = MarketDay {
        id: row.get(0)?,
        root: row.get(1)?,
        contract_symbol: row.get(2)?,
        market_date: NaiveDate::parse_from_str(&market_date, "%Y-%m-%d").map_err(|err| {
            rusqlite::Error::FromSqlConversionFailure(3, rusqlite::types::Type::Text, Box::new(err))
        })?,
        timezone: row.get(4)?,
        data_start_ns: row.get::<_, i64>(5)? as u64,
        data_end_ns: row.get::<_, i64>(6)? as u64,
        rth_start_ns: row.get::<_, i64>(7)? as u64,
        rth_end_ns: row.get::<_, i64>(8)? as u64,
        status: MarketDayStatus::parse(&status).map_err(|err| {
            rusqlite::Error::FromSqlConversionFailure(9, rusqlite::types::Type::Text, err.into())
        })?,
        metadata_json: serde_json::from_str(&metadata_json).map_err(|err| {
            rusqlite::Error::FromSqlConversionFailure(
                12,
                rusqlite::types::Type::Text,
                Box::new(err),
            )
        })?,
    };
    if row.get::<_, i64>(10)? != 0 {
        market_day.status = MarketDayStatus::Ready;
    }
    Ok(MarketDayRecord {
        market_day,
        raw: None,
        replay_dataset: None,
        last_validation: None,
    })
}

fn row_to_object(row: &rusqlite::Row<'_>) -> rusqlite::Result<StoredObject> {
    let kind: String = row.get(0)?;
    let metadata_json: String = row.get(14)?;
    Ok(StoredObject {
        kind: StorageKind::parse(&kind),
        logical_key: row.get(1)?,
        format: row.get(2)?,
        schema_version: row.get(3)?,
        content_sha256: row.get(4)?,
        size_bytes: row.get(5)?,
        remote_bucket: row.get(6)?,
        remote_key: row.get(7)?,
        producer: row.get(8)?,
        producer_version: row.get(9)?,
        source_provider: row.get(10)?,
        source_dataset: row.get(11)?,
        source_schema: row.get(12)?,
        source_symbol: row.get(13)?,
        metadata_json: serde_json::from_str(&metadata_json).map_err(|err| {
            rusqlite::Error::FromSqlConversionFailure(
                14,
                rusqlite::types::Type::Text,
                Box::new(err),
            )
        })?,
    })
}

fn row_to_raw_market_data(row: &rusqlite::Row<'_>) -> rusqlite::Result<RawMarketDataRecord> {
    let status: String = row.get(5)?;
    let kind: String = row.get(8)?;
    let metadata_json: String = row.get(22)?;
    Ok(RawMarketDataRecord {
        market_day_id: row.get(0)?,
        provider: row.get(1)?,
        dataset: row.get(2)?,
        schema: row.get(3)?,
        source_symbol: row.get(4)?,
        status: RawMarketDataStatus::parse(&status).map_err(|err| {
            rusqlite::Error::FromSqlConversionFailure(5, rusqlite::types::Type::Text, err.into())
        })?,
        created_at_ns: row.get::<_, i64>(6)? as u64,
        updated_at_ns: row.get::<_, i64>(7)? as u64,
        object: StoredObject {
            kind: StorageKind::parse(&kind),
            logical_key: row.get(9)?,
            format: row.get(10)?,
            schema_version: row.get(11)?,
            content_sha256: row.get(12)?,
            size_bytes: row.get(13)?,
            remote_bucket: row.get(14)?,
            remote_key: row.get(15)?,
            producer: row.get(16)?,
            producer_version: row.get(17)?,
            source_provider: row.get(18)?,
            source_dataset: row.get(19)?,
            source_schema: row.get(20)?,
            source_symbol: row.get(21)?,
            metadata_json: serde_json::from_str(&metadata_json).map_err(|err| {
                rusqlite::Error::FromSqlConversionFailure(
                    22,
                    rusqlite::types::Type::Text,
                    Box::new(err),
                )
            })?,
        },
    })
}

fn row_to_replay_dataset(row: &rusqlite::Row<'_>) -> rusqlite::Result<ReplayDatasetRecord> {
    let status: String = row.get(3)?;
    Ok(ReplayDatasetRecord {
        id: row.get(0)?,
        market_day_id: row.get(1)?,
        raw_object_remote_key: row.get(2)?,
        status: ReplayDatasetRecordStatus::parse(&status).map_err(|err| {
            rusqlite::Error::FromSqlConversionFailure(3, rusqlite::types::Type::Text, err.into())
        })?,
        schema_version: row.get(4)?,
        producer: row.get(5)?,
        producer_version: row.get(6)?,
        artifact_set_hash: row.get(7)?,
        created_at_ns: row.get::<_, i64>(8)? as u64,
        updated_at_ns: row.get::<_, i64>(9)? as u64,
    })
}

fn row_to_validation_report(row: &rusqlite::Row<'_>) -> rusqlite::Result<ValidationReportRecord> {
    let mode: String = row.get(2)?;
    let status: String = row.get(3)?;
    let report_json: String = row.get(9)?;
    let status = ValidationReportStatus::parse(&status).map_err(|err| {
        rusqlite::Error::FromSqlConversionFailure(3, rusqlite::types::Type::Text, err.into())
    })?;
    let report_json: serde_json::Value = serde_json::from_str(&report_json).map_err(|err| {
        rusqlite::Error::FromSqlConversionFailure(9, rusqlite::types::Type::Text, Box::new(err))
    })?;
    let mut trigger: String = row.get(4)?;
    if let Some(value) = report_json
        .get("trigger")
        .and_then(serde_json::Value::as_str)
    {
        trigger = value.to_string();
    }
    let mut trust_status: String = row.get(5)?;
    if trust_status == "replay_dataset_available" {
        if let Some(value) = report_json
            .get("trust_status")
            .or_else(|| report_json.get("status"))
            .and_then(serde_json::Value::as_str)
        {
            trust_status = value.to_string();
        }
    }
    let mut summary: String = row.get(6)?;
    if summary.is_empty() {
        summary = report_json
            .get("summary")
            .and_then(serde_json::Value::as_str)
            .map(str::to_string)
            .unwrap_or_else(|| validation_status_summary(&status, &trust_status));
    }
    Ok(ValidationReportRecord {
        id: row.get(0)?,
        replay_dataset_id: row.get(1)?,
        mode: ValidationMode::parse(&mode).map_err(|err| {
            rusqlite::Error::FromSqlConversionFailure(2, rusqlite::types::Type::Text, err.into())
        })?,
        status,
        trigger,
        trust_status,
        summary,
        warning_count: row.get(7)?,
        error_count: row.get(8)?,
        report_json,
        created_at_ns: row.get::<_, i64>(10)? as u64,
    })
}

fn validation_status_summary(status: &ValidationReportStatus, trust_status: &str) -> String {
    match trust_status {
        "ready_to_train" => "ReplayDataset validated and ready for training.",
        "ready_with_warnings" => "ReplayDataset is usable but has validation warnings.",
        "invalid" => "ReplayDataset is invalid.",
        _ => match status {
            ValidationReportStatus::Valid => "ReplayDataset has a persisted validation report.",
            ValidationReportStatus::Warning => {
                "ReplayDataset is usable but has validation warnings."
            }
            ValidationReportStatus::Invalid => "ReplayDataset is invalid.",
        },
    }
    .to_string()
}

fn row_to_job_record(row: &rusqlite::Row<'_>) -> rusqlite::Result<LedgerJobRecord> {
    let status: String = row.get(2)?;
    let request_json: String = row.get(7)?;
    let result_json: Option<String> = row.get(8)?;
    Ok(LedgerJobRecord {
        id: row.get(0)?,
        kind: row.get(1)?,
        status: LedgerJobStatus::parse(&status).map_err(|err| {
            rusqlite::Error::FromSqlConversionFailure(2, rusqlite::types::Type::Text, err.into())
        })?,
        market_day_id: row.get(3)?,
        created_at_ns: row.get::<_, i64>(4)? as u64,
        started_at_ns: row.get::<_, Option<i64>>(5)?.map(|value| value as u64),
        finished_at_ns: row.get::<_, Option<i64>>(6)?.map(|value| value as u64),
        request_json: serde_json::from_str(&request_json).map_err(|err| {
            rusqlite::Error::FromSqlConversionFailure(7, rusqlite::types::Type::Text, Box::new(err))
        })?,
        result_json: result_json
            .map(|json| {
                serde_json::from_str(&json).map_err(|err| {
                    rusqlite::Error::FromSqlConversionFailure(
                        8,
                        rusqlite::types::Type::Text,
                        Box::new(err),
                    )
                })
            })
            .transpose()?,
        error: row.get(9)?,
        events: Vec::new(),
    })
}

fn clamp_job_limit(limit: usize) -> i64 {
    limit.clamp(1, 100) as i64
}

fn row_to_job_event(row: &rusqlite::Row<'_>) -> rusqlite::Result<LedgerJobEvent> {
    Ok(LedgerJobEvent {
        id: row.get(0)?,
        job_id: row.get(1)?,
        created_at_ns: row.get::<_, i64>(2)? as u64,
        level: row.get(3)?,
        message: row.get(4)?,
    })
}
