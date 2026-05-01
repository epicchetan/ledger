use serde::{Deserialize, Serialize};
use std::path::PathBuf;

#[derive(Debug, Clone, Copy)]
pub struct CachePrunePolicy {
    pub max_sessions: usize,
}

impl Default for CachePrunePolicy {
    fn default() -> Self {
        Self { max_sessions: 5 }
    }
}

impl CachePrunePolicy {
    pub fn from_env() -> Self {
        Self {
            max_sessions: std::env::var("LEDGER_CACHE_MAX_SESSIONS")
                .ok()
                .and_then(|s| s.parse().ok())
                .unwrap_or(5),
        }
    }
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct CachePruneReport {
    pub deleted_sessions: Vec<String>,
    pub deleted_dirs: Vec<PathBuf>,
    pub bytes_deleted: u64,
}
