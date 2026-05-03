use anyhow::Result;
use ledger_domain::{
    Bbo, ProjectionDeliverySemantics, ProjectionFramePolicy, ProjectionKey, ProjectionManifest,
    ProjectionSpec, ProjectionWakePolicy,
};
use serde_json::{json, Value};

use crate::projection::{
    ProjectionAdvance, ProjectionContext, ProjectionFactory, ProjectionFrameDraft, ProjectionNode,
};

use super::{base_manifest, event_mask_with_bbo_changed, no_params_schema, BBO_ID};

#[derive(Debug, Clone)]
pub struct BboProjectionFactory {
    manifest: ProjectionManifest,
}

impl BboProjectionFactory {
    pub fn new() -> Self {
        Self {
            manifest: base_manifest(
                BBO_ID,
                "Best Bid Offer",
                "Emits exchange-truth best bid and offer when the BBO changes.",
                no_params_schema(),
                json!({}),
                vec![],
                "bbo_v1",
                ProjectionWakePolicy::OnEventMask(event_mask_with_bbo_changed()),
                ProjectionDeliverySemantics::ReplaceLatest,
                ProjectionFramePolicy::EmitOnChange,
            )
            .expect("bbo projection manifest must be valid"),
        }
    }
}

impl Default for BboProjectionFactory {
    fn default() -> Self {
        Self::new()
    }
}

impl ProjectionFactory for BboProjectionFactory {
    fn manifest(&self) -> &ProjectionManifest {
        &self.manifest
    }

    fn resolve_dependencies(&self, _params: &Value) -> Result<Vec<ProjectionSpec>> {
        Ok(vec![])
    }

    fn build(&self, _spec: ProjectionSpec, key: ProjectionKey) -> Result<Box<dyn ProjectionNode>> {
        Ok(Box::new(BboProjectionNode {
            key,
            last_bbo: None,
            payload: bbo_payload(None),
            pending: Vec::new(),
        }))
    }
}

struct BboProjectionNode {
    key: ProjectionKey,
    last_bbo: Option<Bbo>,
    payload: Value,
    pending: Vec<ProjectionFrameDraft>,
}

impl ProjectionNode for BboProjectionNode {
    fn key(&self) -> &ProjectionKey {
        &self.key
    }

    fn advance(&mut self, ctx: &ProjectionContext<'_>) -> Result<ProjectionAdvance> {
        let next = ctx.tick().market.bbo_after;
        if next == self.last_bbo {
            return Ok(ProjectionAdvance::NoChange);
        }

        self.last_bbo = next;
        self.payload = bbo_payload(next);
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
        self.last_bbo = None;
        self.payload = bbo_payload(None);
        self.pending.clear();
        Ok(())
    }
}

fn bbo_payload(bbo: Option<Bbo>) -> Value {
    let Some(bbo) = bbo else {
        return json!({
            "bid_price": null,
            "bid_size": 0,
            "ask_price": null,
            "ask_size": 0,
            "spread_ticks": null,
        });
    };

    let bid_price = bbo.bid_price.map(|price| price.0);
    let ask_price = bbo.ask_price.map(|price| price.0);
    let spread_ticks = bid_price.zip(ask_price).map(|(bid, ask)| ask - bid);

    json!({
        "bid_price": bid_price,
        "bid_size": bbo.bid_size,
        "ask_price": ask_price,
        "ask_size": bbo.ask_size,
        "spread_ticks": spread_ticks,
    })
}
