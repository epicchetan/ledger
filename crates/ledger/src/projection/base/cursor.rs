use anyhow::Result;
use ledger_domain::{
    ProjectionDeliverySemantics, ProjectionFramePolicy, ProjectionKey, ProjectionManifest,
    ProjectionSpec, ProjectionWakePolicy,
};
use serde_json::{json, Value};

use crate::projection::{
    ProjectionAdvance, ProjectionContext, ProjectionFactory, ProjectionFrameDraft, ProjectionNode,
};

use super::{base_manifest, no_params_schema, CURSOR_ID};

#[derive(Debug, Clone)]
pub struct CursorProjectionFactory {
    manifest: ProjectionManifest,
}

impl CursorProjectionFactory {
    pub fn new() -> Self {
        Self {
            manifest: base_manifest(
                CURSOR_ID,
                "Replay Cursor",
                "Emits replay cursor progress for every applied batch.",
                no_params_schema(),
                json!({}),
                vec![],
                "cursor_v1",
                ProjectionWakePolicy::EveryTick,
                ProjectionDeliverySemantics::ReplaceLatest,
                ProjectionFramePolicy::EmitEveryUpdate,
            )
            .expect("cursor projection manifest must be valid"),
        }
    }
}

impl Default for CursorProjectionFactory {
    fn default() -> Self {
        Self::new()
    }
}

impl ProjectionFactory for CursorProjectionFactory {
    fn manifest(&self) -> &ProjectionManifest {
        &self.manifest
    }

    fn resolve_dependencies(&self, _params: &Value) -> Result<Vec<ProjectionSpec>> {
        Ok(vec![])
    }

    fn build(&self, _spec: ProjectionSpec, key: ProjectionKey) -> Result<Box<dyn ProjectionNode>> {
        Ok(Box::new(CursorProjectionNode {
            key,
            payload: cursor_payload(0, 0, 0),
            pending: Vec::new(),
        }))
    }
}

struct CursorProjectionNode {
    key: ProjectionKey,
    payload: Value,
    pending: Vec<ProjectionFrameDraft>,
}

impl ProjectionNode for CursorProjectionNode {
    fn key(&self) -> &ProjectionKey {
        &self.key
    }

    fn advance(&mut self, ctx: &ProjectionContext<'_>) -> Result<ProjectionAdvance> {
        let tick = ctx.tick();
        self.payload = cursor_payload(tick.batch_idx, tick.cursor_ts_ns, tick.applied_batch_idx);
        self.pending
            .push(ProjectionFrameDraft::replace(self.payload.clone()));
        Ok(ProjectionAdvance::StateChanged)
    }

    fn snapshot(&self) -> Value {
        self.payload.clone()
    }

    fn drain_frames(&mut self) -> Result<Vec<ProjectionFrameDraft>> {
        Ok(std::mem::take(&mut self.pending))
    }

    fn reset(&mut self) -> Result<()> {
        self.payload = cursor_payload(0, 0, 0);
        self.pending.clear();
        Ok(())
    }
}

fn cursor_payload(batch_idx: u64, cursor_ts_ns: u64, applied_batch_idx: u64) -> Value {
    json!({
        "batch_idx": batch_idx,
        "cursor_ts_ns": cursor_ts_ns.to_string(),
        "applied_batch_idx": applied_batch_idx,
    })
}
