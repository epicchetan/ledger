use anyhow::Result;
use ledger_domain::{
    ProjectionDeliverySemantics, ProjectionFramePolicy, ProjectionKind, ProjectionManifest,
    ProjectionOutputSchema, ProjectionUpdateMode, ProjectionVersion, ProjectionWakeEventMask,
    ProjectionWakePolicy, SourceView, TemporalPolicy,
};
use serde_json::{json, Value};

use super::ProjectionRegistry;

mod bars;
mod bbo;
mod cursor;
mod trades;

pub use bars::*;
pub use bbo::*;
pub use cursor::*;
pub use trades::*;

pub const CURSOR_ID: &str = "cursor";
pub const BBO_ID: &str = "bbo";
pub const CANONICAL_TRADES_ID: &str = "canonical_trades";
pub const BARS_ID: &str = "bars";

pub fn register_base_projections(registry: &mut ProjectionRegistry) -> Result<()> {
    registry.register(CursorProjectionFactory::new())?;
    registry.register(BboProjectionFactory::new())?;
    registry.register(CanonicalTradesProjectionFactory::new())?;
    registry.register(BarsProjectionFactory::new())?;
    Ok(())
}

pub fn base_projection_registry() -> Result<ProjectionRegistry> {
    let mut registry = ProjectionRegistry::new();
    register_base_projections(&mut registry)?;
    Ok(registry)
}

fn base_manifest(
    id: &str,
    name: &str,
    description: &str,
    params_schema: Value,
    default_params: Value,
    dependencies: Vec<ledger_domain::DependencyDecl>,
    output_schema: &str,
    wake_policy: ProjectionWakePolicy,
    delivery_semantics: ProjectionDeliverySemantics,
    frame_policy: ProjectionFramePolicy,
) -> Result<ProjectionManifest> {
    Ok(ProjectionManifest {
        id: ledger_domain::ProjectionId::new(id)?,
        version: ProjectionVersion::new(1)?,
        name: name.to_string(),
        description: description.to_string(),
        kind: ProjectionKind::Base,
        params_schema,
        default_params,
        dependencies,
        output_schema: ProjectionOutputSchema::new(output_schema)?,
        update_mode: ProjectionUpdateMode::Online,
        source_view: Some(SourceView::ExchangeTruth),
        temporal_policy: TemporalPolicy::Causal,
        wake_policy,
        delivery_semantics,
        frame_policy,
    })
}

fn no_params_schema() -> Value {
    json!({
        "type": "object",
        "additionalProperties": false
    })
}

fn bars_params_schema() -> Value {
    json!({
        "type": "object",
        "required": ["seconds"],
        "additionalProperties": false,
        "properties": {
            "seconds": {
                "type": "integer",
                "minimum": 1
            }
        }
    })
}

fn event_mask_with_trades() -> ProjectionWakeEventMask {
    ProjectionWakeEventMask {
        trades: true,
        ..Default::default()
    }
}

fn event_mask_with_bbo_changed() -> ProjectionWakeEventMask {
    ProjectionWakeEventMask {
        bbo_changed: true,
        ..Default::default()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::projection::{ProjectionRuntime, ProjectionRuntimeConfig, SessionTick};
    use ledger_domain::{BookSide, PriceTicks, ProjectionFrameOp, ProjectionId, ProjectionSpec};
    use serde_json::json;

    fn spec(id: &str, params: Value) -> ProjectionSpec {
        ProjectionSpec::new(id, 1, params).unwrap()
    }

    #[test]
    fn base_registry_registers_expected_manifests() {
        let registry = base_projection_registry().unwrap();
        let ids = registry
            .list_manifests()
            .iter()
            .map(|manifest| manifest.id.as_str().to_string())
            .collect::<Vec<_>>();

        assert_eq!(ids, vec![CURSOR_ID, BBO_ID, CANONICAL_TRADES_ID, BARS_ID]);
    }

    #[test]
    fn base_registry_rejects_duplicate_projection() {
        let mut registry = ProjectionRegistry::new();
        register_base_projections(&mut registry).unwrap();

        let err = registry
            .register(CursorProjectionFactory::new())
            .unwrap_err();

        assert!(format!("{err:#}").contains("already registered"));
    }

    #[test]
    fn base_projection_manifests_validate() {
        let registry = base_projection_registry().unwrap();

        for manifest in registry.list_manifests() {
            manifest.validate().unwrap();
            assert_eq!(manifest.kind, ProjectionKind::Base);
            assert_eq!(manifest.update_mode, ProjectionUpdateMode::Online);
            assert_eq!(manifest.temporal_policy, TemporalPolicy::Causal);
        }
    }

    #[test]
    fn cursor_projection_emits_every_tick() {
        let mut runtime = ProjectionRuntime::new(
            base_projection_registry().unwrap(),
            ProjectionRuntimeConfig::default(),
        );
        runtime.subscribe(spec(CURSOR_ID, json!({}))).unwrap();

        let frames = runtime.advance(SessionTick::synthetic(7, 700)).unwrap();

        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0].op, ProjectionFrameOp::Replace);
        assert_eq!(frames[0].payload["batch_idx"], json!(7));
        assert_eq!(frames[0].payload["applied_batch_idx"], json!(6));
        assert_eq!(frames[0].payload["feed_seq"], json!(7));
        assert_eq!(frames[0].payload["feed_ts_ns"], json!("700"));
        assert_eq!(frames[0].payload["source_first_ts_ns"], json!("700"));
        assert_eq!(frames[0].payload["source_last_ts_ns"], json!("700"));
    }

    #[test]
    fn bbo_projection_emits_only_on_bbo_change() {
        let mut runtime = ProjectionRuntime::new(
            base_projection_registry().unwrap(),
            ProjectionRuntimeConfig::default(),
        );
        runtime.subscribe(spec(BBO_ID, json!({}))).unwrap();

        assert!(runtime
            .advance(SessionTick::synthetic(1, 100))
            .unwrap()
            .is_empty());

        let mut tick = SessionTick::synthetic(2, 200);
        tick.flags.bbo_changed = true;
        tick.market.bbo_after = Some(ledger_domain::Bbo {
            bid_price: Some(PriceTicks(100)),
            bid_size: 2,
            ask_price: Some(PriceTicks(101)),
            ask_size: 3,
        });
        let frames = runtime.advance(tick).unwrap();

        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0].payload["bid_price"], json!(100));
        assert_eq!(frames[0].payload["bid_size"], json!(2));
        assert_eq!(frames[0].payload["ask_price"], json!(101));
        assert_eq!(frames[0].payload["ask_size"], json!(3));
        assert_eq!(frames[0].payload["spread_ticks"], json!(1));
    }

    #[test]
    fn canonical_trades_projection_emits_trade_batch() {
        let mut runtime = ProjectionRuntime::new(
            base_projection_registry().unwrap(),
            ProjectionRuntimeConfig::default(),
        );
        runtime
            .subscribe(spec(CANONICAL_TRADES_ID, json!({})))
            .unwrap();
        let mut tick = SessionTick::synthetic(1, 100);
        tick.flags.trades = true;
        tick.market.trades = vec![ledger_domain::TradeRecord {
            ts_event_ns: 100,
            sequence: 17,
            price: PriceTicks(101),
            size: 4,
            aggressor_side: Some(BookSide::Ask),
            order_id: 9,
        }];
        tick.market.trade_count = tick.market.trades.len();

        let frames = runtime.advance(tick).unwrap();

        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0].op, ProjectionFrameOp::Append);
        assert_eq!(frames[0].payload["batch_idx"], json!(1));
        assert_eq!(frames[0].payload["trades"][0]["price"], json!(101));
        assert_eq!(frames[0].payload["trades"][0]["side"], json!("ask"));
    }

    #[test]
    fn bars_projection_builds_one_minute_bar() {
        let mut runtime = ProjectionRuntime::new(
            base_projection_registry().unwrap(),
            ProjectionRuntimeConfig::default(),
        );
        let subscription = runtime
            .subscribe(spec(BARS_ID, json!({ "seconds": 60 })))
            .unwrap();
        let mut tick = SessionTick::synthetic(1, 1_000_000_000);
        tick.flags.trades = true;
        tick.market.trades = vec![
            trade(1_000_000_000, 1, 100, 2),
            trade(2_000_000_000, 2, 103, 1),
            trade(3_000_000_000, 3, 99, 5),
        ];
        tick.market.trade_count = tick.market.trades.len();

        let frames = runtime.advance(tick).unwrap();
        let bars_frames = frames
            .iter()
            .filter(|frame| frame.stamp.projection_key.id.as_str() == BARS_ID)
            .collect::<Vec<_>>();

        assert!(bars_frames.is_empty());
        let payload = runtime.last_payload(&subscription.key).unwrap();
        assert_eq!(payload["current"]["open"], json!(100));
        assert_eq!(payload["current"]["high"], json!(103));
        assert_eq!(payload["current"]["low"], json!(99));
        assert_eq!(payload["current"]["close"], json!(99));
        assert_eq!(payload["current"]["volume"], json!(8));
        assert_eq!(payload["current"]["trade_count"], json!(3));
        assert_eq!(payload["current"]["final"], json!(false));
    }

    #[test]
    fn bars_projection_finalizes_closed_window() {
        let mut runtime = ProjectionRuntime::new(
            base_projection_registry().unwrap(),
            ProjectionRuntimeConfig::default(),
        );
        runtime
            .subscribe(spec(BARS_ID, json!({ "seconds": 60 })))
            .unwrap();

        let mut first = SessionTick::synthetic(1, 1_000_000_000);
        first.flags.trades = true;
        first.market.trades = vec![trade(1_000_000_000, 1, 100, 2)];
        first.market.trade_count = first.market.trades.len();
        let first_frames = runtime.advance(first).unwrap();
        assert!(first_frames
            .iter()
            .all(|frame| frame.stamp.projection_key.id.as_str() != BARS_ID));

        let mut second = SessionTick::synthetic(2, 61_000_000_000);
        second.flags.trades = true;
        second.market.trades = vec![trade(61_000_000_000, 2, 105, 1)];
        second.market.trade_count = second.market.trades.len();
        let frames = runtime.advance(second).unwrap();
        let bars_frames = frames
            .iter()
            .filter(|frame| frame.stamp.projection_key.id.as_str() == BARS_ID)
            .collect::<Vec<_>>();

        assert_eq!(bars_frames.len(), 1);
        assert_eq!(bars_frames[0].op, ProjectionFrameOp::Patch);
        assert_eq!(bars_frames[0].payload["bar_start_ts_ns"], json!("0"));
        assert_eq!(
            bars_frames[0].payload["bar_end_ts_ns"],
            json!("60000000000")
        );
        assert_eq!(bars_frames[0].payload["final"], json!(true));
    }

    #[test]
    fn bars_projection_ignores_empty_trade_batches() {
        let mut runtime = ProjectionRuntime::new(
            base_projection_registry().unwrap(),
            ProjectionRuntimeConfig::default(),
        );
        runtime
            .subscribe(spec(BARS_ID, json!({ "seconds": 60 })))
            .unwrap();
        let mut tick = SessionTick::synthetic(1, 100);
        tick.flags.trades = true;

        let frames = runtime.advance(tick).unwrap();

        assert!(frames
            .iter()
            .all(|frame| frame.stamp.projection_key.id.as_str() != BARS_ID));
    }

    #[test]
    fn projection_dependencies_feed_bars_from_trades() {
        let mut runtime = ProjectionRuntime::new(
            base_projection_registry().unwrap(),
            ProjectionRuntimeConfig::default(),
        );
        let subscription = runtime
            .subscribe(spec(BARS_ID, json!({ "seconds": 60 })))
            .unwrap();

        let order = runtime
            .active_topological_order()
            .iter()
            .map(|key| key.id.as_str().to_string())
            .collect::<Vec<_>>();

        assert_eq!(order, vec![CANONICAL_TRADES_ID, BARS_ID]);
        assert_eq!(runtime.active_node_count(), 2);
        assert_eq!(runtime.ref_count(&subscription.key), Some(1));
        let trade_key = ProjectionSpec::new(CANONICAL_TRADES_ID, 1, json!({}))
            .unwrap()
            .key()
            .unwrap();
        assert_eq!(runtime.ref_count(&trade_key), Some(1));
    }

    fn trade(ts_event_ns: u64, sequence: u64, price: i64, size: u32) -> ledger_domain::TradeRecord {
        ledger_domain::TradeRecord {
            ts_event_ns,
            sequence,
            price: PriceTicks(price),
            size,
            aggressor_side: Some(BookSide::Ask),
            order_id: sequence,
        }
    }

    #[test]
    fn base_registry_exposes_bars_manifest() {
        let registry = base_projection_registry().unwrap();
        let manifest = registry
            .manifest(
                &ProjectionId::new(BARS_ID).unwrap(),
                ProjectionVersion::new(1).unwrap(),
            )
            .unwrap();

        assert_eq!(manifest.dependencies.len(), 1);
        assert_eq!(manifest.dependencies[0].id.as_str(), CANONICAL_TRADES_ID);
    }
}
