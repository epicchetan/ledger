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
pub struct ObjectDependency {
    pub object_remote_key: String,
    pub depends_on_remote_key: String,
    pub relationship: String,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MarketDayRecord {
    pub market_day: MarketDay,
    pub ready: bool,
    pub last_accessed_ns: Option<u64>,
}

#[derive(Clone, Debug, Default)]
pub struct MarketDayFilter {
    pub root: Option<String>,
    pub symbol: Option<String>,
    pub ready: Option<bool>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ReplayDatasetObjectStatus {
    pub kind: StorageKind,
    pub remote_key: Option<String>,
    pub local_path: Option<PathBuf>,
    pub local_valid: bool,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ReplayDatasetStatus {
    pub market_day: MarketDay,
    pub catalog_found: bool,
    pub ready: bool,
    pub raw_available_remote: bool,
    pub artifacts_available_remote: bool,
    pub dataset_loaded_local: bool,
    pub dataset_cache_valid: bool,
    pub last_accessed_ns: Option<u64>,
    pub objects: Vec<ReplayDatasetObjectStatus>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LoadedReplayDataset {
    pub market_day: MarketDay,
    pub events_path: PathBuf,
    pub batches_path: PathBuf,
    pub trades_path: PathBuf,
    pub book_check_path: PathBuf,
    pub objects: Vec<StoredObject>,
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

            CREATE TABLE IF NOT EXISTS session_cache_entries (
                remote_key TEXT PRIMARY KEY,
                market_day_id TEXT NOT NULL,
                kind TEXT NOT NULL,
                local_path TEXT NOT NULL,
                content_sha256 TEXT NOT NULL,
                size_bytes INTEGER NOT NULL,
                last_accessed_ns INTEGER NOT NULL,
                created_at_ns INTEGER NOT NULL,
                updated_at_ns INTEGER NOT NULL,
                CHECK(kind IN ('event_store', 'batch_index', 'trade_index', 'book_check')),
                FOREIGN KEY(remote_key) REFERENCES objects(remote_key),
                FOREIGN KEY(market_day_id) REFERENCES market_days(id)
            );

            CREATE INDEX IF NOT EXISTS idx_session_cache_market_day
                ON session_cache_entries(market_day_id);
            CREATE INDEX IF NOT EXISTS idx_session_cache_accessed
                ON session_cache_entries(last_accessed_ns);

            CREATE TABLE IF NOT EXISTS ingest_runs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                market_day_id TEXT NOT NULL,
                started_at_ns INTEGER NOT NULL,
                finished_at_ns INTEGER,
                status TEXT NOT NULL,
                error_message TEXT,
                FOREIGN KEY(market_day_id) REFERENCES market_days(id)
            );
            "#,
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
        conn.query_row(
            "SELECT id, root, contract_symbol, market_date, timezone,
                    data_start_ns, data_end_ns, rth_start_ns, rth_end_ns,
                    status, ready, last_accessed_ns, metadata_json
             FROM market_days WHERE id = ?1",
            params![id],
            row_to_market_day_record,
        )
        .optional()
        .map_err(Into::into)
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
        if let Some(ready) = filter.ready {
            sql.push_str(" AND ready = ?");
            values.push(Box::new(i64::from(ready)));
        }
        sql.push_str(" ORDER BY market_date DESC, contract_symbol ASC");
        let params = rusqlite::params_from_iter(values.iter().map(|v| v.as_ref()));
        let mut stmt = conn.prepare(&sql)?;
        let rows = stmt.query_map(params, row_to_market_day_record)?;
        rows.collect::<std::result::Result<Vec<_>, _>>()
            .map_err(Into::into)
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

    pub fn upsert_replay_dataset_cache_entry(
        &self,
        md: &MarketDay,
        object: &StoredObject,
        local_path: &Path,
    ) -> Result<()> {
        if !is_replay_artifact(&object.kind) {
            return Err(anyhow!(
                "only replay artifacts may be committed to the replay dataset cache, got {}",
                object.kind.as_str()
            ));
        }
        let now = now_ns() as i64;
        let conn = self.conn.lock().unwrap();
        conn.execute(
            r#"
            INSERT INTO session_cache_entries (
                remote_key, market_day_id, kind, local_path, content_sha256,
                size_bytes, last_accessed_ns, created_at_ns, updated_at_ns
            )
            VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?7, ?7)
            ON CONFLICT(remote_key) DO UPDATE SET
                market_day_id=excluded.market_day_id,
                kind=excluded.kind,
                local_path=excluded.local_path,
                content_sha256=excluded.content_sha256,
                size_bytes=excluded.size_bytes,
                last_accessed_ns=excluded.last_accessed_ns,
                updated_at_ns=excluded.updated_at_ns
            "#,
            params![
                object.remote_key,
                md.id,
                object.kind.as_str(),
                local_path.to_string_lossy(),
                object.content_sha256,
                object.size_bytes,
                now,
            ],
        )?;
        drop(conn);
        self.touch_market_day(&md.id)?;
        Ok(())
    }

    pub fn cache_path(&self, remote_key: &str) -> Result<Option<PathBuf>> {
        let conn = self.conn.lock().unwrap();
        conn.query_row(
            "SELECT local_path FROM session_cache_entries WHERE remote_key = ?1",
            params![remote_key],
            |row| row.get::<_, String>(0).map(PathBuf::from),
        )
        .optional()
        .map_err(Into::into)
    }

    pub fn touch_market_day(&self, market_day_id: &str) -> Result<()> {
        let now = now_ns() as i64;
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "UPDATE market_days SET last_accessed_ns = ?1, updated_at_ns = ?1 WHERE id = ?2",
            params![now, market_day_id],
        )?;
        conn.execute(
            "UPDATE session_cache_entries
             SET last_accessed_ns = ?1, updated_at_ns = ?1
             WHERE market_day_id = ?2",
            params![now, market_day_id],
        )?;
        Ok(())
    }

    pub fn cached_market_days_lru(&self) -> Result<Vec<(String, PathBuf, u64)>> {
        let conn = self.conn.lock().unwrap();
        let mut stmt = conn.prepare(
            "SELECT m.id, MIN(c.local_path), COALESCE(m.last_accessed_ns, MIN(c.last_accessed_ns))
             FROM market_days m
             JOIN session_cache_entries c ON c.market_day_id = m.id
             GROUP BY m.id
             ORDER BY COALESCE(m.last_accessed_ns, MIN(c.last_accessed_ns)) ASC",
        )?;
        let rows = stmt.query_map([], |row| {
            let id: String = row.get(0)?;
            let path: String = row.get(1)?;
            let accessed: i64 = row.get(2)?;
            Ok((id, PathBuf::from(path), accessed as u64))
        })?;
        rows.collect::<std::result::Result<Vec<_>, _>>()
            .map_err(Into::into)
    }

    pub fn remove_replay_dataset_cache_entries(&self, market_day_id: &str) -> Result<()> {
        let conn = self.conn.lock().unwrap();
        conn.execute(
            "DELETE FROM session_cache_entries WHERE market_day_id = ?1",
            params![market_day_id],
        )?;
        Ok(())
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
    let ready = row.get::<_, i64>(10)? != 0;
    if ready {
        market_day.status = MarketDayStatus::Ready;
    }
    Ok(MarketDayRecord {
        market_day,
        ready,
        last_accessed_ns: row.get::<_, Option<i64>>(11)?.map(|n| n as u64),
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
