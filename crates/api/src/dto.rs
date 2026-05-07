use serde::{Deserialize, Serialize};
use serde_json::Value;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HealthResponse {
    pub ok: bool,
    pub service: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct StoreObjectListQuery {
    pub role: Option<String>,
    pub kind: Option<String>,
    pub id_prefix: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoreObject {
    pub id: String,
    pub role: String,
    pub kind: String,
    pub file_name: String,
    pub content_sha256: String,
    pub size_bytes: u64,
    pub format: Option<String>,
    pub media_type: Option<String>,
    pub remote: Option<StoreRemoteObject>,
    pub local: Option<LocalStoreObject>,
    pub lineage: Vec<String>,
    pub metadata_json: Value,
    pub created_at_ns: String,
    pub created_at_iso: String,
    pub updated_at_ns: String,
    pub updated_at_iso: String,
    pub last_accessed_at_ns: Option<String>,
    pub last_accessed_at_iso: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StoreRemoteObject {
    pub bucket: String,
    pub key: String,
    pub size_bytes: u64,
    pub sha256: Option<String>,
    pub etag: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LocalStoreObject {
    pub relative_path: String,
    pub size_bytes: u64,
    pub last_accessed_at_ns: String,
    pub last_accessed_at_iso: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct DeleteStoreObjectResponse {
    pub id: Option<String>,
    pub descriptor_removed: bool,
    pub remote_object_deleted: bool,
    pub remote_descriptor_deleted: bool,
    pub local_deleted: bool,
    pub remote_key: Option<String>,
    pub remote_descriptor_key: Option<String>,
    pub local_path: Option<String>,
    pub bytes_deleted: u64,
}
