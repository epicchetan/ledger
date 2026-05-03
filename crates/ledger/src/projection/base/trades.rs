use anyhow::Result;
use ledger_domain::{
    BookSide, ProjectionDeliverySemantics, ProjectionFramePolicy, ProjectionKey,
    ProjectionManifest, ProjectionSpec, ProjectionWakePolicy, TradeRecord,
};
use serde_json::{json, Value};

use crate::projection::{
    ProjectionAdvance, ProjectionContext, ProjectionFactory, ProjectionFrameDraft, ProjectionNode,
};

use super::{base_manifest, event_mask_with_trades, no_params_schema, CANONICAL_TRADES_ID};

#[derive(Debug, Clone)]
pub struct CanonicalTradesProjectionFactory {
    manifest: ProjectionManifest,
}

impl CanonicalTradesProjectionFactory {
    pub fn new() -> Self {
        Self {
            manifest: base_manifest(
                CANONICAL_TRADES_ID,
                "Canonical Trades",
                "Emits canonical exchange-truth trade prints from replay batches.",
                no_params_schema(),
                json!({}),
                vec![],
                "canonical_trades_v1",
                ProjectionWakePolicy::OnEventMask(event_mask_with_trades()),
                ProjectionDeliverySemantics::Append,
                ProjectionFramePolicy::EmitEveryUpdate,
            )
            .expect("canonical trades projection manifest must be valid"),
        }
    }
}

impl Default for CanonicalTradesProjectionFactory {
    fn default() -> Self {
        Self::new()
    }
}

impl ProjectionFactory for CanonicalTradesProjectionFactory {
    fn manifest(&self) -> &ProjectionManifest {
        &self.manifest
    }

    fn resolve_dependencies(&self, _params: &Value) -> Result<Vec<ProjectionSpec>> {
        Ok(vec![])
    }

    fn build(&self, _spec: ProjectionSpec, key: ProjectionKey) -> Result<Box<dyn ProjectionNode>> {
        Ok(Box::new(CanonicalTradesProjectionNode {
            key,
            payload: trades_payload(0, 0, &[]),
            pending: Vec::new(),
        }))
    }
}

struct CanonicalTradesProjectionNode {
    key: ProjectionKey,
    payload: Value,
    pending: Vec<ProjectionFrameDraft>,
}

impl ProjectionNode for CanonicalTradesProjectionNode {
    fn key(&self) -> &ProjectionKey {
        &self.key
    }

    fn advance(&mut self, ctx: &ProjectionContext<'_>) -> Result<ProjectionAdvance> {
        let tick = ctx.tick();
        self.payload = trades_payload(
            tick.batch_idx,
            tick.applied_batch_idx,
            &tick.exchange.trades,
        );
        self.pending
            .push(ProjectionFrameDraft::append(self.payload.clone()));
        Ok(ProjectionAdvance::StateChanged)
    }

    fn snapshot(&self) -> Value {
        self.payload.clone()
    }

    fn drain_frames(&mut self) -> Result<Vec<ProjectionFrameDraft>> {
        Ok(std::mem::take(&mut self.pending))
    }

    fn reset(&mut self) -> Result<()> {
        self.payload = trades_payload(0, 0, &[]);
        self.pending.clear();
        Ok(())
    }
}

pub(crate) fn trades_payload(
    batch_idx: u64,
    applied_batch_idx: u64,
    trades: &[TradeRecord],
) -> Value {
    json!({
        "batch_idx": batch_idx,
        "applied_batch_idx": applied_batch_idx,
        "trades": trades.iter().map(trade_payload).collect::<Vec<_>>(),
    })
}

fn trade_payload(trade: &TradeRecord) -> Value {
    json!({
        "ts_event_ns": trade.ts_event_ns.to_string(),
        "sequence": trade.sequence,
        "price": trade.price.0,
        "size": trade.size,
        "side": trade.aggressor_side.map(side_name),
        "order_id": trade.order_id,
    })
}

fn side_name(side: BookSide) -> &'static str {
    match side {
        BookSide::Bid => "bid",
        BookSide::Ask => "ask",
    }
}
