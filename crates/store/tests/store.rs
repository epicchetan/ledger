use anyhow::Result;
use async_trait::async_trait;
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;
use store::{
    JsonObjectRegistry, LocalStore, ObjectFilter, ObjectMetadata, RegisterFileRequest,
    RemoteObject, RemoteStore, Store, StoreConfig, StoreObjectDescriptor, StoreObjectId,
    StoreObjectRole,
};
use tempfile::{tempdir, TempDir};
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

fn open_test_store(
    data: &TempDir,
    remote: Arc<TestRemote>,
    local_max_bytes: u64,
) -> Store<TestRemote> {
    Store::open(data.path(), StoreConfig { local_max_bytes }, remote).unwrap()
}

async fn register_test_object(
    store: &Store<TestRemote>,
    data: &TempDir,
    file_name: &str,
    bytes: &[u8],
) -> StoreObjectDescriptor {
    let source = data.path().join(file_name);
    tokio::fs::write(&source, bytes).await.unwrap();
    store
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
        .unwrap()
}

fn local_path(data: &TempDir, descriptor: &StoreObjectDescriptor) -> PathBuf {
    data.path()
        .join("store")
        .join(descriptor.local.as_ref().unwrap().relative_path.as_path())
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
async fn update_metadata_replaces_json_preserves_locations_and_remirrors() {
    let data = tempdir().unwrap();
    let remote = Arc::new(TestRemote::new());
    let store = open_test_store(&data, remote.clone(), 1024 * 1024);
    let descriptor = register_test_object(&store, &data, "metadata.bin", b"metadata bytes").await;
    let remote_before = descriptor.remote.clone();
    let local_before = descriptor.local.clone();
    let descriptor_key = format!(
        "store/registry/objects/sha256/{}/{}.json",
        descriptor.id.shard(),
        descriptor.id.sha256()
    );

    let updated = store
        .update_metadata(
            &descriptor.id,
            serde_json::json!({ "market_day": "2026-03-10" }),
        )
        .await
        .unwrap();

    assert_eq!(
        updated.metadata_json,
        serde_json::json!({ "market_day": "2026-03-10" })
    );
    assert_eq!(updated.remote, remote_before);
    assert_eq!(updated.local, local_before);
    assert!(updated.updated_at_ns >= descriptor.updated_at_ns);
    let mirrored = remote.get_bytes(&descriptor_key).await.unwrap();
    let mirrored: StoreObjectDescriptor = serde_json::from_slice(&mirrored).unwrap();
    assert_eq!(mirrored.metadata_json, updated.metadata_json);
}

#[tokio::test]
async fn update_metadata_errors_for_missing_object() {
    let data = tempdir().unwrap();
    let remote = Arc::new(TestRemote::new());
    let store = open_test_store(&data, remote, 1024 * 1024);
    let missing = StoreObjectId::new(format!("sha256-{}", "0".repeat(64))).unwrap();

    let error = store
        .update_metadata(&missing, serde_json::json!({ "market_day": "2026-03-10" }))
        .await
        .unwrap_err();

    assert!(error.to_string().contains("not found"));
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
async fn touch_valid_local_copy_returns_true_for_valid_local_copy() {
    let data = tempdir().unwrap();
    let remote = Arc::new(TestRemote::new());
    let store = open_test_store(&data, remote, 1024);
    let descriptor = register_test_object(&store, &data, "local.bin", b"local bytes").await;

    assert!(store.touch_valid_local_copy(&descriptor.id).unwrap());
}

#[tokio::test]
async fn touch_valid_local_copy_refreshes_last_accessed_for_valid_local_copy() {
    let data = tempdir().unwrap();
    let remote = Arc::new(TestRemote::new());
    let store = open_test_store(&data, remote, 1024);
    let descriptor = register_test_object(&store, &data, "refresh.bin", b"refresh bytes").await;
    let before = store
        .get_object(&descriptor.id)
        .unwrap()
        .unwrap()
        .last_accessed_at_ns
        .unwrap();

    std::thread::sleep(Duration::from_millis(1));
    assert!(store.touch_valid_local_copy(&descriptor.id).unwrap());

    let after = store
        .get_object(&descriptor.id)
        .unwrap()
        .unwrap()
        .last_accessed_at_ns
        .unwrap();
    assert!(after > before);
}

#[tokio::test]
async fn touch_valid_local_copy_returns_false_when_local_file_is_missing() {
    let data = tempdir().unwrap();
    let remote = Arc::new(TestRemote::new());
    let store = open_test_store(&data, remote, 1024);
    let descriptor = register_test_object(&store, &data, "missing.bin", b"missing bytes").await;
    std::fs::remove_file(local_path(&data, &descriptor)).unwrap();

    assert!(!store.touch_valid_local_copy(&descriptor.id).unwrap());
}

#[tokio::test]
async fn touch_valid_local_copy_returns_false_on_size_mismatch() {
    let data = tempdir().unwrap();
    let remote = Arc::new(TestRemote::new());
    let store = open_test_store(&data, remote, 1024);
    let descriptor = register_test_object(&store, &data, "size.bin", b"size bytes").await;
    std::fs::write(local_path(&data, &descriptor), b"short").unwrap();

    assert!(!store.touch_valid_local_copy(&descriptor.id).unwrap());
}

#[tokio::test]
async fn touch_valid_local_copy_returns_false_on_sha256_mismatch() {
    let data = tempdir().unwrap();
    let remote = Arc::new(TestRemote::new());
    let store = open_test_store(&data, remote, 1024);
    let descriptor = register_test_object(&store, &data, "sha.bin", b"same size").await;
    std::fs::write(local_path(&data, &descriptor), b"Same size").unwrap();

    assert!(!store.touch_valid_local_copy(&descriptor.id).unwrap());
}

#[tokio::test]
async fn touch_valid_local_copy_returns_false_without_local_entry() {
    let data = tempdir().unwrap();
    let remote = Arc::new(TestRemote::new());
    let store = open_test_store(&data, remote, 1024);
    let descriptor = register_test_object(&store, &data, "remote.bin", b"remote only").await;
    store.remove_local_copy(&descriptor.id).unwrap();

    assert!(!store.touch_valid_local_copy(&descriptor.id).unwrap());
}

#[tokio::test]
async fn touch_valid_local_copy_errors_on_unknown_object_id() {
    let data = tempdir().unwrap();
    let remote = Arc::new(TestRemote::new());
    let store = open_test_store(&data, remote, 1024);
    let id = StoreObjectId::new(format!("sha256-{}", "0".repeat(64))).unwrap();

    let err = store.touch_valid_local_copy(&id).unwrap_err();

    assert!(err.to_string().contains("not found"));
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

#[tokio::test]
async fn sync_registry_adds_missing_descriptors_without_local_claim() {
    let remote = Arc::new(TestRemote::new());
    let source_data = tempdir().unwrap();
    let source_store = open_test_store(&source_data, remote.clone(), 1024);
    let registered =
        register_test_object(&source_store, &source_data, "sync-source.bin", b"sync me").await;

    let target_data = tempdir().unwrap();
    let target_store = open_test_store(&target_data, remote.clone(), 1024);
    let report = target_store.sync_registry(false, false).await.unwrap();

    assert!(!report.dry_run);
    assert_eq!(report.scanned, 1);
    assert_eq!(report.added, vec![registered.id.clone()]);
    assert!(report.overwritten.is_empty());
    assert!(report.skipped.is_empty());
    assert!(report.failed.is_empty());

    let synced = target_store.get_object(&registered.id).unwrap().unwrap();
    assert_eq!(synced.kind, registered.kind);
    assert_eq!(synced.remote, registered.remote);
    assert!(synced.local.is_none());
}

#[tokio::test]
async fn sync_registry_skips_existing_without_overwrite() {
    let remote = Arc::new(TestRemote::new());
    let source_data = tempdir().unwrap();
    let source_store = open_test_store(&source_data, remote.clone(), 1024);
    let registered =
        register_test_object(&source_store, &source_data, "sync-skip.bin", b"skip me").await;

    let target_data = tempdir().unwrap();
    let target_store = open_test_store(&target_data, remote.clone(), 1024);
    target_store.sync_registry(false, false).await.unwrap();

    let target_registry = JsonObjectRegistry::new(&LocalStore::new(target_data.path()));
    let mut mutated = target_store.get_object(&registered.id).unwrap().unwrap();
    mutated.kind = "mutated.kind".to_string();
    target_registry.put(&mutated).unwrap();

    let report = target_store.sync_registry(false, false).await.unwrap();

    assert_eq!(report.skipped, vec![registered.id.clone()]);
    assert!(report.added.is_empty());
    assert!(report.overwritten.is_empty());
    let unchanged = target_store.get_object(&registered.id).unwrap().unwrap();
    assert_eq!(unchanged.kind, "mutated.kind");
}

#[tokio::test]
async fn sync_registry_overwrite_restores_mirror_and_keeps_valid_local() {
    let remote = Arc::new(TestRemote::new());
    let data = tempdir().unwrap();
    let store = open_test_store(&data, remote.clone(), 1024);
    let registered = register_test_object(&store, &data, "sync-owr.bin", b"overwrite me").await;

    let registry = JsonObjectRegistry::new(&LocalStore::new(data.path()));
    let mut mutated = registered.clone();
    mutated.kind = "mutated.kind".to_string();
    registry.put(&mutated).unwrap();

    let report = store.sync_registry(true, false).await.unwrap();

    assert_eq!(report.overwritten, vec![registered.id.clone()]);
    assert!(report.added.is_empty());
    let restored = store.get_object(&registered.id).unwrap().unwrap();
    assert_eq!(restored.kind, registered.kind);
    let local = restored.local.expect("valid local copy preserved");
    assert_eq!(
        local.relative_path,
        registered.local.as_ref().unwrap().relative_path
    );
}

#[tokio::test]
async fn sync_registry_dry_run_reports_without_writing() {
    let remote = Arc::new(TestRemote::new());
    let source_data = tempdir().unwrap();
    let source_store = open_test_store(&source_data, remote.clone(), 1024);
    let registered =
        register_test_object(&source_store, &source_data, "sync-dry.bin", b"dry run").await;

    let target_data = tempdir().unwrap();
    let target_store = open_test_store(&target_data, remote.clone(), 1024);
    let report = target_store.sync_registry(false, true).await.unwrap();

    assert!(report.dry_run);
    assert_eq!(report.added, vec![registered.id.clone()]);
    assert!(target_store.get_object(&registered.id).unwrap().is_none());
    assert!(target_store
        .list_objects(ObjectFilter::default())
        .unwrap()
        .is_empty());
}

#[tokio::test]
async fn sync_registry_reports_mismatched_mirror_key_as_failure() {
    let remote = Arc::new(TestRemote::new());
    let source_data = tempdir().unwrap();
    let source_store = open_test_store(&source_data, remote.clone(), 1024);
    let registered =
        register_test_object(&source_store, &source_data, "sync-bad.bin", b"bad mirror").await;

    let real_key = format!(
        "store/registry/objects/sha256/{}/{}.json",
        registered.id.shard(),
        registered.id.sha256()
    );
    let bytes = remote.get_bytes(&real_key).await.unwrap();
    let bogus_key = "store/registry/objects/sha256/00/0000000000000000000000000000000000000000000000000000000000000000.json";
    remote
        .put_bytes(
            bogus_key,
            &bytes,
            &ObjectMetadata {
                sha256: "irrelevant".to_string(),
                size_bytes: bytes.len() as u64,
                content_type: Some("application/json".to_string()),
                user_metadata: HashMap::new(),
            },
        )
        .await
        .unwrap();

    let target_data = tempdir().unwrap();
    let target_store = open_test_store(&target_data, remote.clone(), 1024);
    let report = target_store.sync_registry(false, false).await.unwrap();

    assert_eq!(report.scanned, 2);
    assert_eq!(report.added, vec![registered.id.clone()]);
    assert_eq!(report.failed.len(), 1);
    assert_eq!(report.failed[0].key, bogus_key);
    assert!(report.failed[0].error.contains("declares id"));
}
