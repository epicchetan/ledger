use indexmap::IndexMap;
use ledger_domain::ProjectionKey;
use std::time::Duration;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ProjectionMetrics {
    nodes: IndexMap<ProjectionKey, ProjectionNodeMetrics>,
}

impl ProjectionMetrics {
    pub fn record_advance(&mut self, key: &ProjectionKey, elapsed: Duration, frames: usize) {
        let node = self.nodes.entry(key.clone()).or_default();
        node.advances += 1;
        node.frames += frames as u64;
        node.total_us += elapsed.as_micros() as u64;
    }

    pub fn record_skip(&mut self, key: &ProjectionKey) {
        self.nodes.entry(key.clone()).or_default().skips += 1;
    }

    pub fn node(&self, key: &ProjectionKey) -> Option<&ProjectionNodeMetrics> {
        self.nodes.get(key)
    }

    pub fn nodes(&self) -> &IndexMap<ProjectionKey, ProjectionNodeMetrics> {
        &self.nodes
    }

    pub fn clear(&mut self) {
        self.nodes.clear();
    }
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct ProjectionNodeMetrics {
    pub advances: u64,
    pub skips: u64,
    pub frames: u64,
    pub total_us: u64,
}
