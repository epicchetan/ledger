use anyhow::Result;
use indexmap::IndexMap;
use ledger_domain::{ProjectionFrameOp, ProjectionKey, ProjectionSpec};
use serde_json::Value;

use super::SessionTick;

#[derive(Debug, Clone)]
pub struct ProjectionContext<'a> {
    tick: &'a SessionTick,
    key: &'a ProjectionKey,
    dependencies: IndexMap<ProjectionKey, Value>,
}

impl<'a> ProjectionContext<'a> {
    pub(crate) fn new(
        tick: &'a SessionTick,
        key: &'a ProjectionKey,
        dependencies: IndexMap<ProjectionKey, Value>,
    ) -> Self {
        Self {
            tick,
            key,
            dependencies,
        }
    }

    pub fn tick(&self) -> &SessionTick {
        self.tick
    }

    pub fn key(&self) -> &ProjectionKey {
        self.key
    }

    pub fn dependency(&self, key: &ProjectionKey) -> Option<&Value> {
        self.dependencies.get(key)
    }

    pub fn dependencies(&self) -> &IndexMap<ProjectionKey, Value> {
        &self.dependencies
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ProjectionFrameDraft {
    pub op: ProjectionFrameOp,
    pub payload: Value,
}

impl ProjectionFrameDraft {
    pub fn replace(payload: Value) -> Self {
        Self {
            op: ProjectionFrameOp::Replace,
            payload,
        }
    }

    pub fn patch(payload: Value) -> Self {
        Self {
            op: ProjectionFrameOp::Patch,
            payload,
        }
    }

    pub fn append(payload: Value) -> Self {
        Self {
            op: ProjectionFrameOp::Append,
            payload,
        }
    }

    pub fn snapshot(payload: Value) -> Self {
        Self {
            op: ProjectionFrameOp::Snapshot,
            payload,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProjectionAdvance {
    NoChange,
    StateChanged,
    NeedsAsyncWork(String),
}

impl ProjectionAdvance {
    pub fn changed(&self) -> bool {
        matches!(self, Self::StateChanged)
    }
}

pub trait ProjectionNode: Send {
    fn key(&self) -> &ProjectionKey;

    fn advance(&mut self, ctx: &ProjectionContext<'_>) -> Result<ProjectionAdvance>;

    fn snapshot(&self) -> Value;

    fn drain_frames(&mut self) -> Result<Vec<ProjectionFrameDraft>>;

    fn reset(&mut self) -> Result<()>;
}

pub trait ProjectionFactory: Send + Sync {
    fn manifest(&self) -> &ledger_domain::ProjectionManifest;

    fn resolve_dependencies(&self, params: &Value) -> Result<Vec<ProjectionSpec>>;

    fn build(&self, spec: ProjectionSpec, key: ProjectionKey) -> Result<Box<dyn ProjectionNode>>;
}
