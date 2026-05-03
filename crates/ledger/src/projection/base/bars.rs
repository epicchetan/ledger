use anyhow::{ensure, Context, Result};
use ledger_domain::{
    DependencyDecl, DependencyParams, ProjectionDeliverySemantics, ProjectionFramePolicy,
    ProjectionId, ProjectionKey, ProjectionManifest, ProjectionSpec, ProjectionVersion,
    ProjectionWakeEventMask, ProjectionWakePolicy,
};
use serde_json::{json, Value};

use crate::projection::{
    ProjectionAdvance, ProjectionContext, ProjectionFactory, ProjectionFrameDraft, ProjectionNode,
};

use super::{bars_params_schema, base_manifest, BARS_ID, CANONICAL_TRADES_ID};

const NANOS_PER_SECOND: u64 = 1_000_000_000;

#[derive(Debug, Clone)]
pub struct BarsProjectionFactory {
    manifest: ProjectionManifest,
}

impl BarsProjectionFactory {
    pub fn new() -> Self {
        Self {
            manifest: base_manifest(
                BARS_ID,
                "Time Bars",
                "Builds simple exchange-truth time bars from canonical trades.",
                bars_params_schema(),
                json!({ "seconds": 60 }),
                vec![DependencyDecl {
                    id: ProjectionId::new(CANONICAL_TRADES_ID)
                        .expect("canonical trades projection id must be valid"),
                    version: ProjectionVersion::new(1)
                        .expect("canonical trades projection version must be valid"),
                    params: DependencyParams::Static(json!({})),
                    required: true,
                }],
                "bars_v1",
                ProjectionWakePolicy::OnEventMask(ProjectionWakeEventMask::default()),
                ProjectionDeliverySemantics::PatchByKey,
                ProjectionFramePolicy::EmitOnWindowClose,
            )
            .expect("bars projection manifest must be valid"),
        }
    }
}

impl Default for BarsProjectionFactory {
    fn default() -> Self {
        Self::new()
    }
}

impl ProjectionFactory for BarsProjectionFactory {
    fn manifest(&self) -> &ProjectionManifest {
        &self.manifest
    }

    fn resolve_dependencies(&self, _params: &Value) -> Result<Vec<ProjectionSpec>> {
        Ok(vec![ProjectionSpec::new(
            CANONICAL_TRADES_ID,
            1,
            json!({}),
        )?])
    }

    fn build(&self, spec: ProjectionSpec, key: ProjectionKey) -> Result<Box<dyn ProjectionNode>> {
        let seconds = spec
            .params
            .get("seconds")
            .and_then(Value::as_u64)
            .context("bars:v1 requires integer `seconds` param")?;
        ensure!(
            seconds > 0,
            "bars:v1 `seconds` param must be greater than zero"
        );
        let window_ns = seconds
            .checked_mul(NANOS_PER_SECOND)
            .context("bars:v1 `seconds` param overflows nanoseconds")?;
        let canonical_trades_key = ProjectionSpec::new(CANONICAL_TRADES_ID, 1, json!({}))?.key()?;
        Ok(Box::new(BarsProjectionNode {
            key,
            canonical_trades_key,
            window_ns,
            current: None,
            last_final: None,
            pending: Vec::new(),
        }))
    }
}

struct BarsProjectionNode {
    key: ProjectionKey,
    canonical_trades_key: ProjectionKey,
    window_ns: u64,
    current: Option<TimeBar>,
    last_final: Option<TimeBar>,
    pending: Vec<ProjectionFrameDraft>,
}

impl ProjectionNode for BarsProjectionNode {
    fn key(&self) -> &ProjectionKey {
        &self.key
    }

    fn advance(&mut self, ctx: &ProjectionContext<'_>) -> Result<ProjectionAdvance> {
        let Some(payload) = ctx.dependency(&self.canonical_trades_key) else {
            return Ok(ProjectionAdvance::NoChange);
        };
        let trades = parse_trade_payloads(payload)?;
        if trades.is_empty() {
            return Ok(ProjectionAdvance::NoChange);
        }

        let mut changed = false;
        for trade in trades {
            self.apply_trade(trade)?;
            changed = true;
        }

        if changed {
            Ok(ProjectionAdvance::StateChanged)
        } else {
            Ok(ProjectionAdvance::NoChange)
        }
    }

    fn snapshot(&self) -> Value {
        json!({
            "window_ns": self.window_ns.to_string(),
            "current": self.current.as_ref().map(TimeBar::payload),
            "last_final": self.last_final.as_ref().map(TimeBar::payload),
        })
    }

    fn drain_frames(&mut self) -> Result<Vec<ProjectionFrameDraft>> {
        Ok(std::mem::take(&mut self.pending))
    }

    fn reset(&mut self) -> Result<()> {
        self.current = None;
        self.last_final = None;
        self.pending.clear();
        Ok(())
    }
}

impl BarsProjectionNode {
    fn apply_trade(&mut self, trade: BarTrade) -> Result<()> {
        let window_start = (trade.ts_event_ns / self.window_ns) * self.window_ns;
        let window_end = window_start + self.window_ns;

        match &mut self.current {
            None => {
                self.current = Some(TimeBar::new(window_start, window_end, trade));
            }
            Some(current) if window_start == current.bar_start_ts_ns => {
                current.apply(trade);
            }
            Some(current) if window_start > current.bar_start_ts_ns => {
                let mut final_bar = current.clone();
                final_bar.finalized = true;
                self.last_final = Some(final_bar.clone());
                self.pending
                    .push(ProjectionFrameDraft::patch(final_bar.payload()));
                self.current = Some(TimeBar::new(window_start, window_end, trade));
            }
            Some(current) => {
                ensure!(
                    window_start >= current.bar_start_ts_ns,
                    "bars:v1 received out-of-order canonical trade timestamp {}",
                    trade.ts_event_ns
                );
            }
        }

        Ok(())
    }
}

#[derive(Debug, Clone, Copy)]
struct BarTrade {
    ts_event_ns: u64,
    price: i64,
    size: u64,
}

#[derive(Debug, Clone)]
struct TimeBar {
    bar_start_ts_ns: u64,
    bar_end_ts_ns: u64,
    open: i64,
    high: i64,
    low: i64,
    close: i64,
    volume: u64,
    trade_count: u64,
    finalized: bool,
}

impl TimeBar {
    fn new(bar_start_ts_ns: u64, bar_end_ts_ns: u64, trade: BarTrade) -> Self {
        Self {
            bar_start_ts_ns,
            bar_end_ts_ns,
            open: trade.price,
            high: trade.price,
            low: trade.price,
            close: trade.price,
            volume: trade.size,
            trade_count: 1,
            finalized: false,
        }
    }

    fn apply(&mut self, trade: BarTrade) {
        self.high = self.high.max(trade.price);
        self.low = self.low.min(trade.price);
        self.close = trade.price;
        self.volume += trade.size;
        self.trade_count += 1;
    }

    fn payload(&self) -> Value {
        json!({
            "bar_start_ts_ns": self.bar_start_ts_ns.to_string(),
            "bar_end_ts_ns": self.bar_end_ts_ns.to_string(),
            "open": self.open,
            "high": self.high,
            "low": self.low,
            "close": self.close,
            "volume": self.volume,
            "trade_count": self.trade_count,
            "final": self.finalized,
        })
    }
}

fn parse_trade_payloads(payload: &Value) -> Result<Vec<BarTrade>> {
    let trades = payload
        .get("trades")
        .and_then(Value::as_array)
        .context("canonical_trades:v1 payload missing trades array")?;
    trades.iter().map(parse_trade_payload).collect()
}

fn parse_trade_payload(payload: &Value) -> Result<BarTrade> {
    let ts_event_ns = payload
        .get("ts_event_ns")
        .and_then(Value::as_str)
        .context("canonical trade payload missing ts_event_ns")?
        .parse::<u64>()
        .context("canonical trade payload has invalid ts_event_ns")?;
    let price = payload
        .get("price")
        .and_then(Value::as_i64)
        .context("canonical trade payload missing price")?;
    let size = payload
        .get("size")
        .and_then(Value::as_u64)
        .context("canonical trade payload missing size")?;

    Ok(BarTrade {
        ts_event_ns,
        price,
        size,
    })
}
