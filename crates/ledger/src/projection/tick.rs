use ledger_domain::{Bbo, ProjectionWakeEventMask, TradeRecord, UnixNanos};
use ledger_replay::{ReplayFeedBatch, ReplayStepResult};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionTick {
    pub feed_seq: u64,
    pub feed_ts_ns: UnixNanos,
    pub source_first_ts_ns: Option<UnixNanos>,
    pub source_last_ts_ns: Option<UnixNanos>,
    pub applied_batch_idx: u64,
    pub batch_idx: u64,
    pub cursor_ts_ns: UnixNanos,
    pub flags: ProjectionWakeEventMask,
    pub market: SessionMarketTick,
    pub visibility: SessionVisibilityTick,
    pub execution: SessionExecutionTick,
}

impl SessionTick {
    pub fn synthetic(batch_idx: u64, cursor_ts_ns: UnixNanos) -> Self {
        Self {
            feed_seq: batch_idx,
            feed_ts_ns: cursor_ts_ns,
            source_first_ts_ns: Some(cursor_ts_ns),
            source_last_ts_ns: Some(cursor_ts_ns),
            applied_batch_idx: batch_idx.saturating_sub(1),
            batch_idx,
            cursor_ts_ns,
            flags: ProjectionWakeEventMask {
                exchange_events: true,
                ..Default::default()
            },
            market: SessionMarketTick {
                event_count: 1,
                trade_count: 0,
                trades: Vec::new(),
                bbo_before: None,
                bbo_after: None,
                bbo_changed: false,
            },
            visibility: SessionVisibilityTick {
                emitted_frame_count: 0,
            },
            execution: SessionExecutionTick { fill_count: 0 },
        }
    }

    pub fn from_replay_feed_batch(batch: &ReplayFeedBatch) -> Self {
        let mut tick = Self::from_replay_step(&batch.replay_step);
        tick.feed_seq = batch.feed_seq;
        tick.feed_ts_ns = batch.feed_ts_ns;
        tick.source_first_ts_ns = batch.source_first_ts_ns;
        tick.source_last_ts_ns = batch.source_last_ts_ns;
        tick
    }

    fn from_replay_step(step: &ReplayStepResult) -> Self {
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
            feed_seq: step.batch_idx_after as u64,
            feed_ts_ns: step.cursor_ts_ns,
            source_first_ts_ns: Some(step.cursor_ts_ns),
            source_last_ts_ns: Some(step.cursor_ts_ns),
            applied_batch_idx: step.applied_batch_idx as u64,
            batch_idx: step.batch_idx_after as u64,
            cursor_ts_ns: step.cursor_ts_ns,
            flags,
            market: SessionMarketTick {
                event_count: step.event_count,
                trade_count: step.trades.len(),
                trades: step.trades.clone(),
                bbo_before: step.bbo_before,
                bbo_after: step.bbo_after,
                bbo_changed: step.bbo_changed,
            },
            visibility: SessionVisibilityTick {
                emitted_frame_count: step.emitted_visibility_frames,
            },
            execution: SessionExecutionTick {
                fill_count: step.new_fills,
            },
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionMarketTick {
    pub event_count: usize,
    pub trade_count: usize,
    pub trades: Vec<TradeRecord>,
    pub bbo_before: Option<Bbo>,
    pub bbo_after: Option<Bbo>,
    pub bbo_changed: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionVisibilityTick {
    pub emitted_frame_count: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionExecutionTick {
    pub fill_count: usize,
}
