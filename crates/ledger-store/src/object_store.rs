use anyhow::Result;
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::path::Path;
use std::sync::{Arc, Mutex};
use tokio::io::AsyncWriteExt;

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct ObjectMetadata {
    pub sha256: String,
    pub size_bytes: i64,
    pub format: String,
    pub schema_version: i64,
    pub content_type: Option<String>,
    pub user_metadata: HashMap<String, String>,
}

impl ObjectMetadata {
    pub fn new(
        sha256: impl Into<String>,
        size_bytes: i64,
        format: impl Into<String>,
        schema_version: i64,
    ) -> Self {
        Self {
            sha256: sha256.into(),
            size_bytes,
            format: format.into(),
            schema_version,
            content_type: None,
            user_metadata: HashMap::new(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct RemoteObject {
    pub bucket: String,
    pub key: String,
    pub size_bytes: i64,
    pub sha256: Option<String>,
    pub etag: Option<String>,
    pub metadata: HashMap<String, String>,
}

#[async_trait]
pub trait ObjectStore: Send + Sync {
    async fn put_path(
        &self,
        key: &str,
        path: &Path,
        metadata: &ObjectMetadata,
    ) -> Result<RemoteObject>;
    async fn get_to_path(&self, key: &str, dest: &Path) -> Result<RemoteObject>;
    async fn head(&self, key: &str) -> Result<Option<RemoteObject>>;
    async fn put_bytes(
        &self,
        key: &str,
        bytes: &[u8],
        metadata: &ObjectMetadata,
    ) -> Result<RemoteObject>;
    fn bucket(&self) -> &str;
}

#[derive(Clone, Default)]
pub struct MemoryObjectStore {
    bucket: String,
    objects: Arc<Mutex<HashMap<String, (Vec<u8>, ObjectMetadata)>>>,
}

impl MemoryObjectStore {
    pub fn new(bucket: impl Into<String>) -> Self {
        Self {
            bucket: bucket.into(),
            objects: Arc::new(Mutex::new(HashMap::new())),
        }
    }
}

#[async_trait]
impl ObjectStore for MemoryObjectStore {
    async fn put_path(
        &self,
        key: &str,
        path: &Path,
        metadata: &ObjectMetadata,
    ) -> Result<RemoteObject> {
        let bytes = tokio::fs::read(path).await?;
        self.put_bytes(key, &bytes, metadata).await
    }

    async fn get_to_path(&self, key: &str, dest: &Path) -> Result<RemoteObject> {
        let (bytes, metadata) = self
            .objects
            .lock()
            .unwrap()
            .get(key)
            .cloned()
            .ok_or_else(|| anyhow::anyhow!("missing object key {key}"))?;
        if let Some(parent) = dest.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        let mut file = tokio::fs::File::create(dest).await?;
        file.write_all(&bytes).await?;
        file.sync_all().await?;
        Ok(RemoteObject {
            bucket: self.bucket.clone(),
            key: key.to_string(),
            size_bytes: bytes.len() as i64,
            sha256: Some(metadata.sha256.clone()),
            etag: None,
            metadata: metadata.user_metadata.clone(),
        })
    }

    async fn head(&self, key: &str) -> Result<Option<RemoteObject>> {
        Ok(self
            .objects
            .lock()
            .unwrap()
            .get(key)
            .map(|(bytes, metadata)| RemoteObject {
                bucket: self.bucket.clone(),
                key: key.to_string(),
                size_bytes: bytes.len() as i64,
                sha256: Some(metadata.sha256.clone()),
                etag: None,
                metadata: metadata.user_metadata.clone(),
            }))
    }

    async fn put_bytes(
        &self,
        key: &str,
        bytes: &[u8],
        metadata: &ObjectMetadata,
    ) -> Result<RemoteObject> {
        self.objects
            .lock()
            .unwrap()
            .insert(key.to_string(), (bytes.to_vec(), metadata.clone()));
        Ok(RemoteObject {
            bucket: self.bucket.clone(),
            key: key.to_string(),
            size_bytes: bytes.len() as i64,
            sha256: Some(metadata.sha256.clone()),
            etag: None,
            metadata: metadata.user_metadata.clone(),
        })
    }

    fn bucket(&self) -> &str {
        &self.bucket
    }
}
