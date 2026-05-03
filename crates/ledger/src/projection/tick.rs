use ledger_domain::{Bbo, ProjectionWakeEventMask, TradeRecord, UnixNanos};
use ledger_replay::ReplayStepResult;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TruthTick {
    pub applied_batch_idx: u64,
    pub batch_idx: u64,
    pub cursor_ts_ns: UnixNanos,
    pub flags: ProjectionWakeEventMask,
    pub exchange: TruthExchangeTick,
    pub visibility: TruthVisibilityTick,
    pub execution: TruthExecutionTick,
}

impl TruthTick {
    pub fn synthetic(batch_idx: u64, cursor_ts_ns: UnixNanos) -> Self {
        Self {
            applied_batch_idx: batch_idx.saturating_sub(1),
            batch_idx,
            cursor_ts_ns,
            flags: ProjectionWakeEventMask {
                exchange_events: true,
                ..Default::default()
            },
            exchange: TruthExchangeTick {
                event_count: 1,
                trade_count: 0,
                trades: Vec::new(),
                bbo_before: None,
                bbo_after: None,
                bbo_changed: false,
            },
            visibility: TruthVisibilityTick {
                emitted_frame_count: 0,
            },
            execution: TruthExecutionTick { fill_count: 0 },
        }
    }

    pub fn from_replay_step(step: &ReplayStepResult) -> Self {
        let flags = ProjectionWakeEventMask {
            exchange_events: step.event_count > 0,
            trades: !step.trades.is_empty(),
            bbo_changed: step.bbo_changed,
            // Depth is derived from exchange truth; this stays conservative
            // until depth projections need a more selective signal.
            depth_changed: step.event_count > 0,
            visibility_frame: step.emitted_visibility_frames > 0,
            fill_event: step.new_fills > 0,
            order_event: false,
            external_snapshot: false,
        };

        Self {
            applied_batch_idx: step.applied_batch_idx as u64,
            batch_idx: step.batch_idx_after as u64,
            cursor_ts_ns: step.cursor_ts_ns,
            flags,
            exchange: TruthExchangeTick {
                event_count: step.event_count,
                trade_count: step.trades.len(),
                trades: step.trades.clone(),
                bbo_before: step.bbo_before,
                bbo_after: step.bbo_after,
                bbo_changed: step.bbo_changed,
            },
            visibility: TruthVisibilityTick {
                emitted_frame_count: step.emitted_visibility_frames,
            },
            execution: TruthExecutionTick {
                fill_count: step.new_fills,
            },
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TruthExchangeTick {
    pub event_count: usize,
    pub trade_count: usize,
    pub trades: Vec<TradeRecord>,
    pub bbo_before: Option<Bbo>,
    pub bbo_after: Option<Bbo>,
    pub bbo_changed: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TruthVisibilityTick {
    pub emitted_frame_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TruthExecutionTick {
    pub fill_count: usize,
}
