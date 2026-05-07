use crate::{
    JsonObjectRegistry, LocalObjectEntry, LocalStore, LocalStorePruneReport, LocalStoreStatus,
    RemoveLocalObjectReport, StoreObjectDescriptor, StoreObjectId,
};
use anyhow::{Context, Result};
use std::path::Path;

pub fn local_status(
    local: &LocalStore,
    registry: &JsonObjectRegistry,
    max_bytes: u64,
) -> Result<LocalStoreStatus> {
    let mut status = LocalStoreStatus {
        root: local.local_objects_dir(),
        max_bytes,
        ..Default::default()
    };

    for descriptor in registry.list(Default::default())? {
        let Some(entry) = descriptor.local else {
            continue;
        };
        let path = local.absolute_from_root(&entry.relative_path);
        if !path.exists() {
            continue;
        }
        status.local_objects += 1;
        status.size_bytes += std::fs::metadata(&path)?.len();
    }

    Ok(status)
}

pub fn remove_local_copy(
    local: &LocalStore,
    registry: &JsonObjectRegistry,
    id: &StoreObjectId,
) -> Result<RemoveLocalObjectReport> {
    let Some(mut descriptor) = registry.get(id)? else {
        return Ok(RemoveLocalObjectReport {
            id: Some(id.clone()),
            ..Default::default()
        });
    };
    let Some(entry) = descriptor.local.take() else {
        registry.put(&descriptor)?;
        return Ok(RemoveLocalObjectReport {
            id: Some(id.clone()),
            ..Default::default()
        });
    };
    let path = local.absolute_from_root(&entry.relative_path);
    let bytes_removed = if path.exists() {
        let bytes = std::fs::metadata(&path)?.len();
        std::fs::remove_file(&path).with_context(|| format!("removing {}", path.display()))?;
        remove_empty_parent_dirs(&path, &local.local_objects_dir())?;
        bytes
    } else {
        0
    };
    descriptor.last_accessed_at_ns = None;
    registry.put(&descriptor)?;
    Ok(RemoveLocalObjectReport {
        id: Some(id.clone()),
        removed: bytes_removed > 0,
        path: Some(path),
        bytes_removed,
    })
}

pub fn enforce_local_limit(
    local: &LocalStore,
    registry: &JsonObjectRegistry,
    max_bytes: u64,
    protected: Option<&StoreObjectId>,
) -> Result<LocalStorePruneReport> {
    let before = local_status(local, registry, max_bytes)?;
    let mut report = LocalStorePruneReport {
        max_bytes,
        before_bytes: before.size_bytes,
        after_bytes: before.size_bytes,
        ..Default::default()
    };
    if before.size_bytes <= max_bytes {
        return Ok(report);
    }

    let mut candidates = registry
        .list(Default::default())?
        .into_iter()
        .filter_map(|descriptor| {
            let entry = descriptor.local.clone()?;
            let path = local.absolute_from_root(&entry.relative_path);
            if !path.exists() {
                return None;
            }
            Some((descriptor, entry, path))
        })
        .collect::<Vec<_>>();
    candidates.sort_by_key(|(descriptor, entry, _)| {
        descriptor
            .last_accessed_at_ns
            .unwrap_or(entry.last_accessed_at_ns)
    });

    let mut current = before.size_bytes;
    for (descriptor, entry, path) in candidates {
        if current <= max_bytes {
            break;
        }
        if protected.is_some_and(|id| id == &descriptor.id) {
            continue;
        }
        let bytes = std::fs::metadata(&path)?.len();
        std::fs::remove_file(&path).with_context(|| format!("removing {}", path.display()))?;
        remove_empty_parent_dirs(&path, &local.local_objects_dir())?;

        let mut updated = descriptor.clone();
        updated.local = None;
        updated.last_accessed_at_ns = None;
        registry.put(&updated)?;

        current = current.saturating_sub(bytes);
        report.removed.push(RemoveLocalObjectReport {
            id: Some(descriptor.id),
            removed: true,
            path: Some(local.absolute_from_root(&entry.relative_path)),
            bytes_removed: bytes,
        });
    }

    report.after_bytes = current;
    Ok(report)
}

pub fn local_entry(local: &LocalStore, path: &Path, size_bytes: u64) -> Result<LocalObjectEntry> {
    Ok(LocalObjectEntry {
        relative_path: local.relative_to_root(path)?,
        size_bytes,
        last_accessed_at_ns: crate::now_ns(),
    })
}

fn remove_empty_parent_dirs(path: &Path, stop_at: &Path) -> Result<()> {
    let mut current = path.parent();
    while let Some(dir) = current {
        if dir == stop_at {
            break;
        }
        match std::fs::remove_dir(dir) {
            Ok(()) => current = dir.parent(),
            Err(_) => break,
        }
    }
    Ok(())
}

pub fn local_path_valid(descriptor: &StoreObjectDescriptor, path: &Path) -> Result<bool> {
    if !path.exists() {
        return Ok(false);
    }
    let metadata = std::fs::metadata(path)?;
    if metadata.len() != descriptor.size_bytes {
        return Ok(false);
    }
    Ok(crate::sha256_file(path)? == descriptor.content_sha256)
}
