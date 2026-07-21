use crate::{ObjectMetadata, RemoteObject, RemoteStore};
use anyhow::Result;
use async_trait::async_trait;
use std::collections::HashMap;
use std::path::Path;
use std::sync::{Arc, Mutex};
use tokio::io::AsyncWriteExt;

type MemoryObjects = HashMap<String, (Vec<u8>, ObjectMetadata)>;

#[derive(Clone, Default)]
pub struct MemoryRemote {
    bucket: String,
    objects: Arc<Mutex<MemoryObjects>>,
}

impl MemoryRemote {
    pub fn new() -> Self {
        Self {
            bucket: "test-bucket".to_string(),
            objects: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn contains_key(&self, key: &str) -> bool {
        self.objects.lock().unwrap().contains_key(key)
    }
}

#[async_trait]
impl RemoteStore for MemoryRemote {
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
            .ok_or_else(|| anyhow::anyhow!("missing object {key}"))?;
        if let Some(parent) = dest.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        let mut file = tokio::fs::File::create(dest).await?;
        file.write_all(&bytes).await?;
        file.sync_all().await?;
        Ok(remote_object(
            &self.bucket,
            key,
            bytes.len() as u64,
            &metadata,
        ))
    }

    async fn head(&self, key: &str) -> Result<Option<RemoteObject>> {
        Ok(self
            .objects
            .lock()
            .unwrap()
            .get(key)
            .map(|(bytes, metadata)| {
                remote_object(&self.bucket, key, bytes.len() as u64, metadata)
            }))
    }

    async fn delete(&self, key: &str) -> Result<()> {
        self.objects.lock().unwrap().remove(key);
        Ok(())
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
        Ok(remote_object(
            &self.bucket,
            key,
            bytes.len() as u64,
            metadata,
        ))
    }

    async fn get_bytes(&self, key: &str) -> Result<Vec<u8>> {
        self.objects
            .lock()
            .unwrap()
            .get(key)
            .map(|(bytes, _)| bytes.clone())
            .ok_or_else(|| anyhow::anyhow!("missing object {key}"))
    }

    async fn list_keys(&self, prefix: &str) -> Result<Vec<String>> {
        let mut keys: Vec<String> = self
            .objects
            .lock()
            .unwrap()
            .keys()
            .filter(|key| key.starts_with(prefix))
            .cloned()
            .collect();
        keys.sort();
        Ok(keys)
    }

    fn bucket(&self) -> &str {
        &self.bucket
    }
}

fn remote_object(
    bucket: &str,
    key: &str,
    size_bytes: u64,
    metadata: &ObjectMetadata,
) -> RemoteObject {
    RemoteObject {
        bucket: bucket.to_string(),
        key: key.to_string(),
        size_bytes,
        sha256: Some(metadata.sha256.clone()),
        etag: None,
        metadata: metadata.user_metadata.clone(),
    }
}
