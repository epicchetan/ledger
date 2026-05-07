use crate::{LocalStore, ObjectFilter, StoreObjectDescriptor, StoreObjectId};
use anyhow::{Context, Result};
use std::path::{Path, PathBuf};

#[derive(Clone, Debug)]
pub struct JsonObjectRegistry {
    root: PathBuf,
}

impl JsonObjectRegistry {
    pub fn new(local: &LocalStore) -> Self {
        Self {
            root: local.registry_objects_dir(),
        }
    }

    pub fn put(&self, descriptor: &StoreObjectDescriptor) -> Result<()> {
        let path = self.descriptor_path(&descriptor.id);
        if let Some(parent) = path.parent() {
            std::fs::create_dir_all(parent)
                .with_context(|| format!("creating {}", parent.display()))?;
        }
        let bytes = serde_json::to_vec_pretty(descriptor)?;
        let tmp = crate::tmp_path(&path);
        std::fs::write(&tmp, bytes).with_context(|| format!("writing {}", tmp.display()))?;
        std::fs::rename(&tmp, &path)
            .with_context(|| format!("renaming {} -> {}", tmp.display(), path.display()))?;
        Ok(())
    }

    pub fn get(&self, id: &StoreObjectId) -> Result<Option<StoreObjectDescriptor>> {
        let path = self.descriptor_path(id);
        if !path.exists() {
            return Ok(None);
        }
        let bytes = std::fs::read(&path).with_context(|| format!("reading {}", path.display()))?;
        Ok(Some(serde_json::from_slice(&bytes)?))
    }

    pub fn list(&self, filter: ObjectFilter) -> Result<Vec<StoreObjectDescriptor>> {
        let mut descriptors = Vec::new();
        if !self.root.exists() {
            return Ok(descriptors);
        }
        self.collect_descriptors(&self.root, &filter, &mut descriptors)?;
        descriptors.sort_by(|a, b| a.id.cmp(&b.id));
        Ok(descriptors)
    }

    pub fn remove(&self, id: &StoreObjectId) -> Result<Option<StoreObjectDescriptor>> {
        let existing = self.get(id)?;
        if existing.is_some() {
            let path = self.descriptor_path(id);
            std::fs::remove_file(&path).with_context(|| format!("removing {}", path.display()))?;
        }
        Ok(existing)
    }

    pub fn descriptor_path(&self, id: &StoreObjectId) -> PathBuf {
        self.root
            .join(id.shard())
            .join(format!("{}.json", id.as_str()))
    }

    fn collect_descriptors(
        &self,
        dir: &Path,
        filter: &ObjectFilter,
        out: &mut Vec<StoreObjectDescriptor>,
    ) -> Result<()> {
        for entry in std::fs::read_dir(dir).with_context(|| format!("reading {}", dir.display()))? {
            let entry = entry?;
            let path = entry.path();
            if path.is_dir() {
                self.collect_descriptors(&path, filter, out)?;
                continue;
            }
            if path.extension().and_then(|ext| ext.to_str()) != Some("json") {
                continue;
            }
            let bytes =
                std::fs::read(&path).with_context(|| format!("reading {}", path.display()))?;
            let descriptor: StoreObjectDescriptor = serde_json::from_slice(&bytes)
                .with_context(|| format!("decoding {}", path.display()))?;
            if !matches_filter(&descriptor, filter) {
                continue;
            }
            out.push(descriptor);
        }
        Ok(())
    }
}

fn matches_filter(descriptor: &StoreObjectDescriptor, filter: &ObjectFilter) -> bool {
    if let Some(role) = &filter.role {
        if &descriptor.role != role {
            return false;
        }
    }
    if let Some(kind) = &filter.kind {
        if &descriptor.kind != kind {
            return false;
        }
    }
    if let Some(id_prefix) = &filter.id_prefix {
        if !descriptor.id.as_str().starts_with(id_prefix) {
            return false;
        }
    }
    true
}
