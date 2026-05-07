use anyhow::{Context, Result};
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Clone, Debug)]
pub struct LocalStore {
    root: PathBuf,
}

impl LocalStore {
    pub fn new(data_dir: impl Into<PathBuf>) -> Self {
        Self {
            root: data_dir.into().join("store"),
        }
    }

    pub fn root(&self) -> &Path {
        &self.root
    }

    pub fn registry_objects_dir(&self) -> PathBuf {
        self.root.join("registry").join("objects")
    }

    pub fn local_objects_dir(&self) -> PathBuf {
        self.root.join("local").join("objects")
    }

    pub fn tmp_dir(&self) -> PathBuf {
        self.root.join("tmp")
    }

    pub fn put_tmp_dir(&self) -> PathBuf {
        self.tmp_dir().join("put")
    }

    pub fn hydrate_tmp_dir(&self) -> PathBuf {
        self.tmp_dir().join("hydrate")
    }

    pub fn local_path(&self, id: &crate::StoreObjectId, file_name: &str) -> PathBuf {
        self.local_objects_dir().join(id.as_str()).join(file_name)
    }

    pub fn relative_to_root(&self, path: &Path) -> Result<PathBuf> {
        Ok(path
            .strip_prefix(&self.root)
            .with_context(|| {
                format!(
                    "path {} is not under store root {}",
                    path.display(),
                    self.root.display()
                )
            })?
            .to_path_buf())
    }

    pub fn absolute_from_root(&self, path: &Path) -> PathBuf {
        self.root.join(path)
    }
}

pub fn now_ns() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system time before unix epoch")
        .as_nanos() as u64
}

pub fn tmp_path(path: &Path) -> PathBuf {
    let mut s = path.as_os_str().to_owned();
    s.push(".part");
    PathBuf::from(s)
}

pub fn sanitize_file_name(input: &str) -> String {
    input
        .chars()
        .map(|c| match c {
            'a'..='z' | 'A'..='Z' | '0'..='9' | '-' | '_' | '.' => c,
            _ => '_',
        })
        .collect()
}
