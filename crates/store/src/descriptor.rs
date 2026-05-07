use crate::StoreObjectId;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::PathBuf;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum StoreObjectRole {
    Raw,
    Artifact,
}

impl StoreObjectRole {
    pub fn parse(value: &str) -> anyhow::Result<Self> {
        match value {
            "raw" => Ok(Self::Raw),
            "artifact" => Ok(Self::Artifact),
            other => anyhow::bail!("unknown store object role `{other}`"),
        }
    }

    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Raw => "raw",
            Self::Artifact => "artifact",
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct RemoteObjectLocation {
    pub bucket: String,
    pub key: String,
    pub size_bytes: u64,
    pub sha256: Option<String>,
    pub etag: Option<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct LocalObjectEntry {
    pub relative_path: PathBuf,
    pub size_bytes: u64,
    pub last_accessed_at_ns: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct StoreObjectDescriptor {
    pub id: StoreObjectId,
    pub role: StoreObjectRole,
    pub kind: String,
    pub file_name: String,
    pub content_sha256: String,
    pub size_bytes: u64,
    pub format: Option<String>,
    pub media_type: Option<String>,
    pub remote: Option<RemoteObjectLocation>,
    pub local: Option<LocalObjectEntry>,
    pub lineage: Vec<StoreObjectId>,
    pub metadata_json: serde_json::Value,
    pub created_at_ns: u64,
    pub updated_at_ns: u64,
    pub last_accessed_at_ns: Option<u64>,
}

#[derive(Clone, Debug, Default)]
pub struct ObjectFilter {
    pub role: Option<StoreObjectRole>,
    pub kind: Option<String>,
    pub id_prefix: Option<String>,
}

#[derive(Clone, Debug)]
pub struct StoreConfig {
    pub local_max_bytes: u64,
}

impl Default for StoreConfig {
    fn default() -> Self {
        Self {
            local_max_bytes: 20 * 1024 * 1024 * 1024,
        }
    }
}

impl StoreConfig {
    pub fn from_env() -> anyhow::Result<Self> {
        let local_max_bytes = match std::env::var("LEDGER_STORE_LOCAL_MAX_BYTES") {
            Ok(value) => value
                .parse::<u64>()
                .map_err(|err| anyhow::anyhow!("parsing LEDGER_STORE_LOCAL_MAX_BYTES: {err}"))?,
            Err(std::env::VarError::NotPresent) => Self::default().local_max_bytes,
            Err(err) => return Err(err.into()),
        };
        Ok(Self { local_max_bytes })
    }
}

#[derive(Clone, Debug)]
pub struct RegisterFileRequest<'a> {
    pub path: &'a std::path::Path,
    pub role: StoreObjectRole,
    pub kind: String,
    pub file_name: Option<String>,
    pub format: Option<String>,
    pub media_type: Option<String>,
    pub lineage: Vec<StoreObjectId>,
    pub metadata_json: serde_json::Value,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct HydratedObject {
    pub descriptor: StoreObjectDescriptor,
    pub path: PathBuf,
}

#[derive(Clone, Debug, Serialize, Deserialize, Default)]
pub struct RemoveLocalObjectReport {
    pub id: Option<StoreObjectId>,
    pub removed: bool,
    pub path: Option<PathBuf>,
    pub bytes_removed: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize, Default)]
pub struct DeleteObjectReport {
    pub id: Option<StoreObjectId>,
    pub descriptor_removed: bool,
    pub remote_object_deleted: bool,
    pub remote_descriptor_deleted: bool,
    pub local_deleted: bool,
    pub remote_key: Option<String>,
    pub remote_descriptor_key: Option<String>,
    pub local_path: Option<PathBuf>,
    pub bytes_deleted: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize, Default)]
pub struct LocalStoreStatus {
    pub root: PathBuf,
    pub local_objects: usize,
    pub size_bytes: u64,
    pub max_bytes: u64,
}

#[derive(Clone, Debug, Serialize, Deserialize, Default)]
pub struct LocalStorePruneReport {
    pub max_bytes: u64,
    pub before_bytes: u64,
    pub after_bytes: u64,
    pub removed: Vec<RemoveLocalObjectReport>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum ObjectValidationStatus {
    Valid,
    Warning,
    Invalid,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ObjectValidationReport {
    pub id: StoreObjectId,
    pub status: ObjectValidationStatus,
    pub local_valid: bool,
    pub remote_valid: Option<bool>,
    pub issues: Vec<String>,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StoreValidationReport {
    pub status: ObjectValidationStatus,
    pub checked: usize,
    pub valid: usize,
    pub warning: usize,
    pub invalid: usize,
    pub objects: Vec<ObjectValidationReport>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ObjectMetadata {
    pub sha256: String,
    pub size_bytes: u64,
    pub content_type: Option<String>,
    pub user_metadata: HashMap<String, String>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct RemoteObject {
    pub bucket: String,
    pub key: String,
    pub size_bytes: u64,
    pub sha256: Option<String>,
    pub etag: Option<String>,
    pub metadata: HashMap<String, String>,
}
