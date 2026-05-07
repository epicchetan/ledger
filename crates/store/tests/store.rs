use anyhow::Result;
use async_trait::async_trait;
use std::collections::HashMap;
use std::path::Path;
use std::sync::{Arc, Mutex};
use store::{
    ObjectMetadata, RegisterFileRequest, RemoteObject, RemoteStore, Store, StoreConfig,
    StoreObjectRole,
};
use tempfile::tempdir;
use tokio::io::AsyncWriteExt;

#[derive(Clone, Default)]
struct TestRemote {
    bucket: String,
    objects: Arc<Mutex<HashMap<String, (Vec<u8>, ObjectMetadata)>>>,
}

impl TestRemote {
    fn new() -> Self {
        Self {
            bucket: "test-bucket".to_string(),
            objects: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    fn contains_key(&self, key: &str) -> bool {
        self.objects.lock().unwrap().contains_key(key)
    }
}

#[async_trait]
impl RemoteStore for TestRemote {
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

#[tokio::test]
async fn register_file_uploads_descriptor_and_uses_shared_local() {
    let data = tempdir().unwrap();
    let remote = Arc::new(TestRemote::new());
    let store = Store::open(
        data.path(),
        StoreConfig {
            local_max_bytes: 1024,
        },
        remote.clone(),
    )
    .unwrap();
    let source = data.path().join("source.dbn.zst");
    tokio::fs::write(&source, b"sample raw bytes")
        .await
        .unwrap();

    let descriptor = store
        .register_file(RegisterFileRequest {
            path: &source,
            role: StoreObjectRole::Raw,
            kind: "databento.dbn.zst".to_string(),
            file_name: None,
            format: Some("dbn.zst".to_string()),
            media_type: None,
            lineage: Vec::new(),
            metadata_json: serde_json::json!({"provider":"databento"}),
        })
        .await
        .unwrap();

    assert_eq!(descriptor.role, StoreObjectRole::Raw);
    assert!(descriptor.local.is_some());
    assert!(descriptor.remote.is_some());
    let remote_key = &descriptor.remote.as_ref().unwrap().key;
    assert!(remote_key.starts_with("store/objects/sha256/"));
    assert!(remote.contains_key(remote_key));
    assert!(remote.contains_key(&format!(
        "store/registry/objects/sha256/{}/{}.json",
        descriptor.id.shard(),
        descriptor.id.sha256()
    )));
    assert_eq!(store.local_status().unwrap().local_objects, 1);
}

#[tokio::test]
async fn hydrate_restores_missing_local_from_remote() {
    let data = tempdir().unwrap();
    let remote = Arc::new(TestRemote::new());
    let store = Store::open(
        data.path(),
        StoreConfig {
            local_max_bytes: 1024,
        },
        remote,
    )
    .unwrap();
    let source = data.path().join("source.bin");
    tokio::fs::write(&source, b"artifact bytes").await.unwrap();
    let descriptor = store
        .register_file(RegisterFileRequest {
            path: &source,
            role: StoreObjectRole::Artifact,
            kind: "runtime.artifact".to_string(),
            file_name: None,
            format: None,
            media_type: None,
            lineage: Vec::new(),
            metadata_json: serde_json::json!({}),
        })
        .await
        .unwrap();
    store.remove_local_copy(&descriptor.id).unwrap();

    let hydrated = store.hydrate(&descriptor.id).await.unwrap();

    assert_eq!(
        tokio::fs::read(&hydrated.path).await.unwrap(),
        b"artifact bytes"
    );
    assert!(hydrated.descriptor.local.is_some());
}

#[tokio::test]
async fn delete_object_removes_descriptor_remote_object_mirror_and_local() {
    let data = tempdir().unwrap();
    let remote = Arc::new(TestRemote::new());
    let store = Store::open(
        data.path(),
        StoreConfig {
            local_max_bytes: 1024,
        },
        remote.clone(),
    )
    .unwrap();
    let source = data.path().join("delete-me.bin");
    tokio::fs::write(&source, b"delete me").await.unwrap();
    let descriptor = store
        .register_file(RegisterFileRequest {
            path: &source,
            role: StoreObjectRole::Artifact,
            kind: "runtime.artifact".to_string(),
            file_name: None,
            format: None,
            media_type: None,
            lineage: Vec::new(),
            metadata_json: serde_json::json!({}),
        })
        .await
        .unwrap();
    let remote_key = descriptor.remote.as_ref().unwrap().key.clone();
    let descriptor_key = format!(
        "store/registry/objects/sha256/{}/{}.json",
        descriptor.id.shard(),
        descriptor.id.sha256()
    );
    let local_path = data
        .path()
        .join("store")
        .join(descriptor.local.as_ref().unwrap().relative_path.as_path());

    let report = store.delete_object(&descriptor.id).await.unwrap();

    assert!(report.descriptor_removed);
    assert!(report.remote_object_deleted);
    assert!(report.remote_descriptor_deleted);
    assert!(report.local_deleted);
    assert!(!remote.contains_key(&remote_key));
    assert!(!remote.contains_key(&descriptor_key));
    assert!(!local_path.exists());
    assert!(store.get_object(&descriptor.id).unwrap().is_none());
}

#[tokio::test]
async fn local_prune_evicts_lru_without_touching_remote() {
    let data = tempdir().unwrap();
    let remote = Arc::new(TestRemote::new());
    let store = Store::open(
        data.path(),
        StoreConfig {
            local_max_bytes: 13,
        },
        remote.clone(),
    )
    .unwrap();

    let first = data.path().join("first.bin");
    let second = data.path().join("second.bin");
    tokio::fs::write(&first, b"first bytes").await.unwrap();
    tokio::fs::write(&second, b"second bytes").await.unwrap();
    let first_descriptor = store
        .register_file(RegisterFileRequest {
            path: &first,
            role: StoreObjectRole::Artifact,
            kind: "runtime.artifact".to_string(),
            file_name: None,
            format: None,
            media_type: None,
            lineage: Vec::new(),
            metadata_json: serde_json::json!({}),
        })
        .await
        .unwrap();
    let second_descriptor = store
        .register_file(RegisterFileRequest {
            path: &second,
            role: StoreObjectRole::Artifact,
            kind: "runtime.artifact".to_string(),
            file_name: None,
            format: None,
            media_type: None,
            lineage: Vec::new(),
            metadata_json: serde_json::json!({}),
        })
        .await
        .unwrap();

    let status = store.local_status().unwrap();

    assert!(status.size_bytes <= 13);
    assert!(remote.contains_key(&first_descriptor.remote.as_ref().unwrap().key));
    assert!(remote.contains_key(&second_descriptor.remote.as_ref().unwrap().key));
}
