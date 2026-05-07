use crate::{
    enforce_local_limit, local_entry, local_path_valid, local_status, remove_local_copy,
    sanitize_file_name, sha256_bytes, sha256_file, DeleteObjectReport, HydratedObject,
    JsonObjectRegistry, LocalStore, LocalStorePruneReport, LocalStoreStatus, ObjectFilter,
    ObjectMetadata, ObjectValidationReport, ObjectValidationStatus, R2Config, R2RemoteStore,
    RegisterFileRequest, RemoteObjectLocation, RemoteStore, RemoveLocalObjectReport, StoreConfig,
    StoreObjectDescriptor, StoreObjectId, StoreObjectRole, StoreValidationReport,
};
use anyhow::{anyhow, Context, Result};
use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::Arc;

#[derive(Clone)]
pub struct Store<S: RemoteStore + 'static> {
    local: LocalStore,
    registry: JsonObjectRegistry,
    remote: Arc<S>,
    config: StoreConfig,
}

pub type R2Store = Store<R2RemoteStore>;

impl R2Store {
    pub async fn from_env(data_dir: impl Into<PathBuf>) -> Result<Self> {
        let remote = Arc::new(R2RemoteStore::new(R2Config::from_env()?).await?);
        Self::open(data_dir, StoreConfig::from_env()?, remote)
    }
}

impl<S: RemoteStore + 'static> Store<S> {
    pub fn open(data_dir: impl Into<PathBuf>, config: StoreConfig, remote: Arc<S>) -> Result<Self> {
        let local = LocalStore::new(data_dir);
        std::fs::create_dir_all(local.registry_objects_dir())?;
        std::fs::create_dir_all(local.local_objects_dir())?;
        std::fs::create_dir_all(local.put_tmp_dir())?;
        std::fs::create_dir_all(local.hydrate_tmp_dir())?;
        let registry = JsonObjectRegistry::new(&local);
        Ok(Self {
            local,
            registry,
            remote,
            config,
        })
    }

    pub async fn register_file(
        &self,
        request: RegisterFileRequest<'_>,
    ) -> Result<StoreObjectDescriptor> {
        let size_bytes = std::fs::metadata(request.path)
            .with_context(|| format!("metadata for {}", request.path.display()))?
            .len();
        let content_sha256 = sha256_file(request.path)?;
        let id = StoreObjectId::from_sha256(&content_sha256)?;
        let now = crate::now_ns();
        let existing = self.registry.get(&id)?;
        let created_at_ns = existing
            .as_ref()
            .map(|descriptor| descriptor.created_at_ns)
            .unwrap_or(now);
        let file_name = request
            .file_name
            .clone()
            .or_else(|| {
                request
                    .path
                    .file_name()
                    .and_then(|value| value.to_str())
                    .map(str::to_string)
            })
            .unwrap_or_else(|| "object.bin".to_string());
        let file_name = sanitize_file_name(&file_name);
        let remote_key = object_object_key(&id, &file_name);
        let metadata = object_metadata(
            &id,
            &request.role,
            &request.kind,
            &content_sha256,
            size_bytes,
            request.media_type.clone(),
        );

        let remote = match self.remote.head(&remote_key).await? {
            Some(remote) => {
                ensure_remote_matches(&remote, &content_sha256, size_bytes, &remote_key)?;
                remote
            }
            None => {
                self.remote
                    .put_path(&remote_key, request.path, &metadata)
                    .await?
            }
        };

        let local_path = self
            .local_file_from_path(&id, &file_name, request.path)
            .await?;
        let local = local_entry(&self.local, &local_path, size_bytes)?;
        let descriptor = StoreObjectDescriptor {
            id: id.clone(),
            role: request.role,
            kind: request.kind,
            file_name,
            content_sha256,
            size_bytes,
            format: request.format,
            media_type: request.media_type,
            remote: Some(RemoteObjectLocation {
                bucket: remote.bucket,
                key: remote.key,
                size_bytes: remote.size_bytes,
                sha256: remote.sha256,
                etag: remote.etag,
            }),
            local: Some(local),
            lineage: request.lineage,
            metadata_json: request.metadata_json,
            created_at_ns,
            updated_at_ns: now,
            last_accessed_at_ns: Some(now),
        };

        self.registry.put(&descriptor)?;
        self.mirror_descriptor(&descriptor).await?;
        self.enforce_local_limit(Some(&id))?;
        Ok(descriptor)
    }

    pub fn list_objects(&self, filter: ObjectFilter) -> Result<Vec<StoreObjectDescriptor>> {
        self.registry.list(filter)
    }

    pub fn get_object(&self, id: &StoreObjectId) -> Result<Option<StoreObjectDescriptor>> {
        self.registry.get(id)
    }

    pub async fn hydrate(&self, id: &StoreObjectId) -> Result<HydratedObject> {
        let mut descriptor = self
            .registry
            .get(id)?
            .ok_or_else(|| anyhow!("store object {id} not found"))?;

        if let Some(local) = &descriptor.local {
            let path = self.local.absolute_from_root(&local.relative_path);
            if local_path_valid(&descriptor, &path)? {
                let now = crate::now_ns();
                descriptor.last_accessed_at_ns = Some(now);
                descriptor.local = Some(local_entry(&self.local, &path, descriptor.size_bytes)?);
                self.registry.put(&descriptor)?;
                return Ok(HydratedObject { descriptor, path });
            }
        }

        let remote = descriptor
            .remote
            .clone()
            .ok_or_else(|| anyhow!("store object {id} has no remote location"))?;
        let local_path = self.local.local_path(id, &descriptor.file_name);
        let tmp_path =
            self.local
                .hydrate_tmp_dir()
                .join(format!("{}-{}", id.as_str(), descriptor.file_name));
        if let Some(parent) = tmp_path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        if tmp_path.exists() {
            tokio::fs::remove_file(&tmp_path).await.ok();
        }
        self.remote.get_to_path(&remote.key, &tmp_path).await?;
        verify_path(&tmp_path, descriptor.size_bytes, &descriptor.content_sha256)?;
        if let Some(parent) = local_path.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        if local_path.exists() {
            tokio::fs::remove_file(&local_path).await?;
        }
        tokio::fs::rename(&tmp_path, &local_path).await?;

        let now = crate::now_ns();
        descriptor.local = Some(local_entry(
            &self.local,
            &local_path,
            descriptor.size_bytes,
        )?);
        descriptor.last_accessed_at_ns = Some(now);
        descriptor.updated_at_ns = now;
        self.registry.put(&descriptor)?;
        self.enforce_local_limit(Some(id))?;
        Ok(HydratedObject {
            descriptor,
            path: local_path,
        })
    }

    pub fn remove_local_copy(&self, id: &StoreObjectId) -> Result<RemoveLocalObjectReport> {
        remove_local_copy(&self.local, &self.registry, id)
    }

    pub async fn delete_object(&self, id: &StoreObjectId) -> Result<DeleteObjectReport> {
        let Some(descriptor) = self.registry.get(id)? else {
            return Ok(DeleteObjectReport {
                id: Some(id.clone()),
                ..Default::default()
            });
        };

        let mut report = DeleteObjectReport {
            id: Some(id.clone()),
            remote_key: descriptor.remote.as_ref().map(|remote| remote.key.clone()),
            remote_descriptor_key: Some(descriptor_key(id)),
            local_path: descriptor
                .local
                .as_ref()
                .map(|local| self.local.absolute_from_root(&local.relative_path)),
            ..Default::default()
        };

        if let Some(remote_key) = &report.remote_key {
            self.remote.delete(remote_key).await?;
            report.remote_object_deleted = true;
            report.bytes_deleted = report.bytes_deleted.saturating_add(descriptor.size_bytes);
        }

        if let Some(remote_descriptor_key) = &report.remote_descriptor_key {
            self.remote.delete(remote_descriptor_key).await?;
            report.remote_descriptor_deleted = true;
        }

        let local = self.remove_local_copy(id)?;
        report.local_deleted = local.removed;
        report.local_path = local.path.or(report.local_path);
        report.bytes_deleted = report.bytes_deleted.saturating_add(local.bytes_removed);

        report.descriptor_removed = self.registry.remove(id)?.is_some();
        Ok(report)
    }

    pub async fn validate_object(
        &self,
        id: &StoreObjectId,
        verify_remote: bool,
    ) -> Result<ObjectValidationReport> {
        let descriptor = self
            .registry
            .get(id)?
            .ok_or_else(|| anyhow!("store object {id} not found"))?;
        let mut issues = Vec::new();

        let local_valid = if let Some(local) = &descriptor.local {
            let path = self.local.absolute_from_root(&local.relative_path);
            let valid = local_path_valid(&descriptor, &path)?;
            if !valid {
                issues.push(format!(
                    "local object entry is missing or invalid: {}",
                    path.display()
                ));
            }
            valid
        } else {
            true
        };

        let mut remote_valid = None;
        if verify_remote {
            let Some(remote) = &descriptor.remote else {
                issues.push("remote location missing".to_string());
                remote_valid = Some(false);
                return Ok(validation_report(
                    descriptor.id,
                    local_valid,
                    remote_valid,
                    issues,
                ));
            };
            let remote_report = self
                .validate_remote_location(remote, &descriptor.content_sha256, descriptor.size_bytes)
                .await?;
            if let Err(err) = remote_report {
                issues.push(err);
                remote_valid = Some(false);
            } else {
                remote_valid = Some(true);
            }
        }

        Ok(validation_report(
            descriptor.id,
            local_valid,
            remote_valid,
            issues,
        ))
    }

    pub async fn validate_all(
        &self,
        filter: ObjectFilter,
        verify_remote: bool,
    ) -> Result<StoreValidationReport> {
        let objects = self.registry.list(filter)?;
        let mut reports = Vec::with_capacity(objects.len());
        for descriptor in objects {
            reports.push(self.validate_object(&descriptor.id, verify_remote).await?);
        }

        let valid = reports
            .iter()
            .filter(|report| report.status == ObjectValidationStatus::Valid)
            .count();
        let warning = reports
            .iter()
            .filter(|report| report.status == ObjectValidationStatus::Warning)
            .count();
        let invalid = reports
            .iter()
            .filter(|report| report.status == ObjectValidationStatus::Invalid)
            .count();
        let status = if invalid > 0 {
            ObjectValidationStatus::Invalid
        } else if warning > 0 {
            ObjectValidationStatus::Warning
        } else {
            ObjectValidationStatus::Valid
        };

        Ok(StoreValidationReport {
            status,
            checked: reports.len(),
            valid,
            warning,
            invalid,
            objects: reports,
        })
    }

    pub async fn validate_remote_location(
        &self,
        remote: &RemoteObjectLocation,
        content_sha256: &str,
        size_bytes: u64,
    ) -> Result<Result<(), String>> {
        let Some(head) = self.remote.head(&remote.key).await? else {
            return Ok(Err(format!("remote object missing: {}", remote.key)));
        };
        if head.size_bytes != size_bytes {
            return Ok(Err(format!(
                "remote size mismatch for {}: expected {}, got {}",
                remote.key, size_bytes, head.size_bytes
            )));
        }
        if let Some(remote_sha) = head.sha256.as_deref() {
            if remote_sha != content_sha256 {
                return Ok(Err(format!(
                    "remote sha256 mismatch for {}: expected {}, got {}",
                    remote.key, content_sha256, remote_sha
                )));
            }
        }
        Ok(Ok(()))
    }

    pub fn local_status(&self) -> Result<LocalStoreStatus> {
        local_status(&self.local, &self.registry, self.config.local_max_bytes)
    }

    pub fn enforce_local_limit(
        &self,
        protected: Option<&StoreObjectId>,
    ) -> Result<LocalStorePruneReport> {
        enforce_local_limit(
            &self.local,
            &self.registry,
            self.config.local_max_bytes,
            protected,
        )
    }

    async fn local_file_from_path(
        &self,
        id: &StoreObjectId,
        file_name: &str,
        source: &Path,
    ) -> Result<PathBuf> {
        let dest = self.local.local_path(id, file_name);
        if local_path_valid_for_parts(
            &dest,
            std::fs::metadata(source)?.len(),
            &sha256_file(source)?,
        )
        .unwrap_or(false)
        {
            return Ok(dest);
        }
        if let Some(parent) = dest.parent() {
            tokio::fs::create_dir_all(parent).await?;
        }
        let tmp = crate::tmp_path(&dest);
        if tmp.exists() {
            tokio::fs::remove_file(&tmp).await.ok();
        }
        tokio::fs::copy(source, &tmp)
            .await
            .with_context(|| format!("copying {} -> {}", source.display(), tmp.display()))?;
        tokio::fs::rename(&tmp, &dest)
            .await
            .with_context(|| format!("renaming {} -> {}", tmp.display(), dest.display()))?;
        Ok(dest)
    }

    async fn mirror_descriptor(&self, descriptor: &StoreObjectDescriptor) -> Result<()> {
        let bytes = serde_json::to_vec_pretty(descriptor)?;
        let metadata = ObjectMetadata {
            sha256: sha256_bytes(&bytes),
            size_bytes: bytes.len() as u64,
            content_type: Some("application/json".to_string()),
            user_metadata: HashMap::from([
                ("ledger-object-id".to_string(), descriptor.id.to_string()),
                (
                    "ledger-object-role".to_string(),
                    descriptor.role.as_str().to_string(),
                ),
                ("ledger-object-kind".to_string(), descriptor.kind.clone()),
            ]),
        };
        self.remote
            .put_bytes(&descriptor_key(&descriptor.id), &bytes, &metadata)
            .await?;
        Ok(())
    }
}

fn validation_report(
    id: StoreObjectId,
    local_valid: bool,
    remote_valid: Option<bool>,
    issues: Vec<String>,
) -> ObjectValidationReport {
    let status = if remote_valid == Some(false) {
        ObjectValidationStatus::Invalid
    } else if !local_valid {
        ObjectValidationStatus::Warning
    } else {
        ObjectValidationStatus::Valid
    };
    ObjectValidationReport {
        id,
        status,
        local_valid,
        remote_valid,
        issues,
    }
}

fn object_object_key(id: &StoreObjectId, file_name: &str) -> String {
    format!(
        "store/objects/sha256/{}/{}/{}",
        id.shard(),
        id.sha256(),
        file_name
    )
}

fn descriptor_key(id: &StoreObjectId) -> String {
    format!(
        "store/registry/objects/sha256/{}/{}.json",
        id.shard(),
        id.sha256()
    )
}

fn object_metadata(
    id: &StoreObjectId,
    role: &StoreObjectRole,
    kind: &str,
    sha256: &str,
    size_bytes: u64,
    content_type: Option<String>,
) -> ObjectMetadata {
    ObjectMetadata {
        sha256: sha256.to_string(),
        size_bytes,
        content_type,
        user_metadata: HashMap::from([
            ("ledger-object-id".to_string(), id.to_string()),
            ("ledger-object-role".to_string(), role.as_str().to_string()),
            ("ledger-object-kind".to_string(), kind.to_string()),
        ]),
    }
}

fn ensure_remote_matches(
    remote: &crate::RemoteObject,
    content_sha256: &str,
    size_bytes: u64,
    key: &str,
) -> Result<()> {
    if remote.size_bytes != size_bytes {
        return Err(anyhow!(
            "remote object {key} exists with size {}, expected {}",
            remote.size_bytes,
            size_bytes
        ));
    }
    if let Some(remote_sha) = remote.sha256.as_deref() {
        if remote_sha != content_sha256 {
            return Err(anyhow!(
                "remote object {key} sha256 {}, expected {}",
                remote_sha,
                content_sha256
            ));
        }
    }
    Ok(())
}

fn verify_path(path: &Path, size_bytes: u64, content_sha256: &str) -> Result<()> {
    let metadata = std::fs::metadata(path)?;
    if metadata.len() != size_bytes {
        return Err(anyhow!(
            "file {} size {}, expected {}",
            path.display(),
            metadata.len(),
            size_bytes
        ));
    }
    let actual = sha256_file(path)?;
    if actual != content_sha256 {
        return Err(anyhow!(
            "file {} sha256 {}, expected {}",
            path.display(),
            actual,
            content_sha256
        ));
    }
    Ok(())
}

fn local_path_valid_for_parts(path: &Path, size_bytes: u64, content_sha256: &str) -> Result<bool> {
    if !path.exists() {
        return Ok(false);
    }
    let metadata = std::fs::metadata(path)?;
    if metadata.len() != size_bytes {
        return Ok(false);
    }
    Ok(sha256_file(path)? == content_sha256)
}
