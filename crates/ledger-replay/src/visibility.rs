use ledger_domain::{Bbo, Depth, TradeRecord, VisibilityProfile};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VisibilityFrame {
    pub exchange_ts_ns: u64,
    pub visible_ts_ns: u64,
    pub batch_idx: usize,
    pub bbo: Option<Bbo>,
    pub depth: Depth,
    pub trades: Vec<TradeRecord>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct PendingVisibilityFrame {
    pub due_ts_ns: u64,
    pub frame: VisibilityFrame,
}

#[derive(Debug, Clone)]
pub struct VisibilityModel {
    pub profile: VisibilityProfile,
    pending: Vec<PendingVisibilityFrame>,
    emitted: Vec<VisibilityFrame>,
}

impl VisibilityModel {
    pub fn new(profile: VisibilityProfile) -> Self {
        Self {
            profile,
            pending: Vec::new(),
            emitted: Vec::new(),
        }
    }

    pub fn on_batch(
        &mut self,
        exchange_ts_ns: u64,
        batch_idx: usize,
        bbo: Option<Bbo>,
        depth: Depth,
        trades: Vec<TradeRecord>,
    ) {
        let raw_visible_ts_ns =
            exchange_ts_ns.saturating_add(self.profile.market_data_delay.sample_ns());
        let visible_ts_ns = if self.profile.frame_interval_ns == 0 {
            raw_visible_ts_ns
        } else {
            let frame = self.profile.frame_interval_ns;
            ((raw_visible_ts_ns + frame - 1) / frame) * frame
        };
        self.pending.push(PendingVisibilityFrame {
            due_ts_ns: visible_ts_ns,
            frame: VisibilityFrame {
                exchange_ts_ns,
                visible_ts_ns,
                batch_idx,
                bbo,
                depth,
                trades,
            },
        });
        self.pending.sort_by_key(|p| p.due_ts_ns);
    }

    pub fn next_due_ts(&self) -> Option<u64> {
        self.pending.first().map(|p| p.due_ts_ns)
    }

    pub fn emit_due(&mut self, now_ts_ns: u64) -> Vec<VisibilityFrame> {
        let cut = self.pending.partition_point(|p| p.due_ts_ns <= now_ts_ns);
        let due: Vec<_> = self.pending.drain(0..cut).map(|p| p.frame).collect();
        self.emitted.extend(due.clone());
        due
    }

    pub fn emitted(&self) -> &[VisibilityFrame] {
        &self.emitted
    }
}
