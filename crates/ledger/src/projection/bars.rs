use async_trait::async_trait;
use cache::{ArrayKey, Cache, CellDescriptor, CellKind, CellOwner, Key, ValueKey};
use runtime::{ComponentError, ComponentId, RuntimeTask, TaskContext, TaskDescriptor, TaskOutcome};
use serde::{Deserialize, Serialize};

use crate::feed::es_replay::{EsMboFeedBatch, EsReplayCells};
use crate::market::{canonical_trade_print, BookSide, PriceTicks, TradePrint, UnixNanos};
use crate::LedgerError;

pub const SECOND_NS: u64 = 1_000_000_000;
pub const MINUTE_NS: u64 = 60 * SECOND_NS;
pub const HOUR_NS: u64 = 60 * MINUTE_NS;
const REBUILD_CHUNK_BATCHES: usize = 4096;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct BarsParams {
    /// Bar interval in nanoseconds. Always > 0.
    pub interval_ns: u64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct Bar {
    /// Bucket start: ts_event_ns - (ts_event_ns % interval_ns).
    pub interval_start_ns: UnixNanos,
    pub open: PriceTicks,
    pub high: PriceTicks,
    pub low: PriceTicks,
    pub close: PriceTicks,
    /// Total canonical print volume, attributed or not.
    pub volume: u64,
    /// Volume where the aggressor was a buyer (BookSide::Bid).
    pub buy_volume: u64,
    /// Volume where the aggressor was a seller (BookSide::Ask).
    pub sell_volume: u64,
    pub trade_count: u64,
    pub first_ts_event_ns: UnixNanos,
    pub last_ts_event_ns: UnixNanos,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct BarsStatus {
    /// Canonical spec, e.g. "bars:1m".
    pub spec: String,
    /// Feed epoch this projection state reflects.
    pub epoch: u64,
    /// Feed batches folded in; equals cursor.batch_idx when caught up.
    pub processed_batches: usize,
    pub completed_bars: usize,
    /// Timestamp of the last folded print (not batch).
    pub last_ts_event_ns: Option<UnixNanos>,
}

#[derive(Debug, Clone)]
pub struct BarsCells {
    pub bars: ArrayKey<Bar>,
    pub live: ValueKey<Bar>,
    pub status: ValueKey<BarsStatus>,
}

impl BarsCells {
    pub fn register(cache: &Cache, params: BarsParams) -> Result<Self, LedgerError> {
        validate_params(params)?;
        let spec = canonical_spec(params);
        let component_id = projection_component_id(&spec)?;
        let owner = component_id.owner();
        let prefix = component_id.as_str();

        let bars = cache.register_array::<Bar>(
            descriptor(
                &format!("{prefix}.bars"),
                owner.clone(),
                CellKind::Array,
                true,
            )?,
            Vec::new(),
        )?;
        let live = cache.register_value::<Bar>(
            descriptor(
                &format!("{prefix}.live"),
                owner.clone(),
                CellKind::Value,
                true,
            )?,
            None,
        )?;
        let status = cache.register_value::<BarsStatus>(
            descriptor(&format!("{prefix}.status"), owner, CellKind::Value, true)?,
            None,
        )?;

        Ok(Self { bars, live, status })
    }
}

pub struct BarsTask {
    descriptor: TaskDescriptor,
    params: BarsParams,
    spec: String,
    feed: EsReplayCells,
    cells: BarsCells,
    epoch: u64,
    processed_batches: usize,
    completed_bars: usize,
    live: Option<Bar>,
    last_print_ts: Option<UnixNanos>,
    fold: Fold,
}

enum Fold {
    Incremental,
    Rebuilding { epoch: u64, fold_idx: usize },
}

impl BarsTask {
    pub fn new(
        feed: EsReplayCells,
        params: BarsParams,
        cells: BarsCells,
    ) -> Result<Self, LedgerError> {
        validate_params(params)?;
        let spec = canonical_spec(params);
        let component_id = projection_component_id(&spec)?;
        let descriptor = TaskDescriptor::new(component_id, vec![feed.batches.key().clone()]);
        Ok(Self {
            descriptor,
            params,
            spec,
            feed,
            cells,
            epoch: 0,
            processed_batches: 0,
            completed_bars: 0,
            live: None,
            last_print_ts: None,
            fold: Fold::Incremental,
        })
    }

    fn status_snapshot(&self) -> BarsStatus {
        BarsStatus {
            spec: self.spec.clone(),
            epoch: self.epoch,
            processed_batches: self.processed_batches,
            completed_bars: self.completed_bars,
            last_ts_event_ns: self.last_print_ts,
        }
    }

    fn reset_fold(&mut self, epoch: u64) {
        self.epoch = epoch;
        self.processed_batches = 0;
        self.completed_bars = 0;
        self.live = None;
        self.last_print_ts = None;
    }

    fn fold_batches(&mut self, batches: &[EsMboFeedBatch], completed: &mut Vec<Bar>) {
        for batch in batches {
            for event in &batch.events {
                let Some(print) = canonical_trade_print(event) else {
                    continue;
                };
                self.fold_print(print, completed);
            }
        }
    }

    fn fold_print(&mut self, print: TradePrint, completed: &mut Vec<Bar>) {
        let bucket = print.ts_event_ns - (print.ts_event_ns % self.params.interval_ns);
        match self.live.take() {
            None => {
                self.live = Some(new_bar(print, bucket));
            }
            Some(mut bar) if bucket == bar.interval_start_ns => {
                fold_print_into_bar(&mut bar, print);
                self.live = Some(bar);
            }
            Some(bar) if bucket > bar.interval_start_ns => {
                completed.push(bar);
                self.live = Some(new_bar(print, bucket));
            }
            Some(mut bar) => {
                fold_print_into_bar(&mut bar, print);
                self.live = Some(bar);
            }
        }
        self.last_print_ts = Some(print.ts_event_ns);
    }

    async fn run_rebuild_step(
        &mut self,
        ctx: TaskContext<'_>,
        target_epoch: u64,
        fold_idx: usize,
        target_batch_idx: usize,
    ) -> Result<TaskOutcome, ComponentError> {
        let first_chunk = fold_idx == 0;
        let chunk_end = target_batch_idx.min(fold_idx + REBUILD_CHUNK_BATCHES);
        let batches = ctx.read_array_range(&self.feed.batches, fold_idx..chunk_end)?;
        let previous_live = self.live.clone();
        let mut completed = Vec::new();
        self.fold_batches(&batches, &mut completed);
        self.processed_batches = chunk_end;
        self.completed_bars += completed.len();

        let mut batch = ctx.batch();
        if first_chunk {
            batch.replace_array(&self.cells.bars, completed);
        } else if !completed.is_empty() {
            batch.push_array(&self.cells.bars, completed);
        }
        if first_chunk || self.live != previous_live {
            if let Some(live) = &self.live {
                batch.set_value(&self.cells.live, live.clone());
            } else {
                batch.clear_value(&self.cells.live);
            }
        }
        batch.set_value(&self.cells.status, self.status_snapshot());
        ctx.submit(batch).await?;

        if chunk_end < target_batch_idx {
            self.fold = Fold::Rebuilding {
                epoch: target_epoch,
                fold_idx: chunk_end,
            };
            Ok(TaskOutcome::WakeAgain)
        } else {
            self.fold = Fold::Incremental;
            Ok(TaskOutcome::Idle)
        }
    }
}

#[async_trait]
impl RuntimeTask for BarsTask {
    fn descriptor(&self) -> &TaskDescriptor {
        &self.descriptor
    }

    async fn prepare(&mut self, ctx: runtime::TaskPrepareContext) -> Result<(), ComponentError> {
        let mut batch = ctx.batch();
        batch.set_value(&self.cells.status, self.status_snapshot());
        ctx.submit(batch).await
    }

    async fn run_once(&mut self, ctx: TaskContext<'_>) -> Result<TaskOutcome, ComponentError> {
        let Some(cursor) = ctx.read_value(&self.feed.cursor)? else {
            return Ok(TaskOutcome::Idle);
        };

        let mut rebuild_from = match self.fold {
            Fold::Incremental => None,
            Fold::Rebuilding { epoch, fold_idx } if epoch == cursor.epoch => Some(fold_idx),
            Fold::Rebuilding { .. } => Some(0),
        };
        if cursor.epoch != self.epoch || cursor.batch_idx < self.processed_batches {
            rebuild_from = Some(0);
        }
        if let Some(fold_idx) = rebuild_from {
            if fold_idx == 0 {
                self.reset_fold(cursor.epoch);
            }
            return self
                .run_rebuild_step(ctx, cursor.epoch, fold_idx, cursor.batch_idx)
                .await;
        }

        if cursor.batch_idx > self.processed_batches {
            let new_batches =
                ctx.read_array_range(&self.feed.batches, self.processed_batches..cursor.batch_idx)?;
            let previous_live = self.live.clone();
            let mut completed = Vec::new();
            self.fold_batches(&new_batches, &mut completed);
            self.processed_batches = cursor.batch_idx;
            self.completed_bars += completed.len();

            let mut batch = ctx.batch();
            if !completed.is_empty() {
                batch.push_array(&self.cells.bars, completed);
            }
            if self.live != previous_live {
                if let Some(live) = &self.live {
                    batch.set_value(&self.cells.live, live.clone());
                } else {
                    batch.clear_value(&self.cells.live);
                }
            }
            batch.set_value(&self.cells.status, self.status_snapshot());
            ctx.submit(batch).await?;
            return Ok(TaskOutcome::Idle);
        }

        Ok(TaskOutcome::Idle)
    }
}

pub fn canonical_spec(params: BarsParams) -> String {
    if params.interval_ns % HOUR_NS == 0 {
        format!("bars:{}h", params.interval_ns / HOUR_NS)
    } else if params.interval_ns % MINUTE_NS == 0 {
        format!("bars:{}m", params.interval_ns / MINUTE_NS)
    } else if params.interval_ns % SECOND_NS == 0 {
        format!("bars:{}s", params.interval_ns / SECOND_NS)
    } else {
        format!("bars:{}ns", params.interval_ns)
    }
}

fn validate_params(params: BarsParams) -> Result<(), LedgerError> {
    if params.interval_ns == 0 {
        return Err(LedgerError::InvalidProjectionSpec {
            spec: "bars:0ns".to_string(),
            reason: "interval value must be greater than zero".to_string(),
        });
    }
    Ok(())
}

fn projection_component_id(spec: &str) -> Result<ComponentId, LedgerError> {
    let path = spec.replace(':', ".");
    ComponentId::new(format!("projection.{path}")).map_err(|err| {
        LedgerError::InvalidProjectionSpec {
            spec: spec.to_string(),
            reason: err.to_string(),
        }
    })
}

fn descriptor(
    key: &str,
    owner: CellOwner,
    kind: CellKind,
    public_read: bool,
) -> Result<CellDescriptor, LedgerError> {
    Ok(CellDescriptor {
        key: Key::new(key)?,
        owner,
        kind,
        public_read,
    })
}

fn new_bar(print: TradePrint, interval_start_ns: UnixNanos) -> Bar {
    let mut bar = Bar {
        interval_start_ns,
        open: print.price_ticks,
        high: print.price_ticks,
        low: print.price_ticks,
        close: print.price_ticks,
        volume: 0,
        buy_volume: 0,
        sell_volume: 0,
        trade_count: 0,
        first_ts_event_ns: print.ts_event_ns,
        last_ts_event_ns: print.ts_event_ns,
    };
    fold_volume(&mut bar, print);
    bar
}

fn fold_print_into_bar(bar: &mut Bar, print: TradePrint) {
    bar.high = bar.high.max(print.price_ticks);
    bar.low = bar.low.min(print.price_ticks);
    bar.first_ts_event_ns = bar.first_ts_event_ns.min(print.ts_event_ns);
    fold_volume(bar, print);
    if print.ts_event_ns >= bar.last_ts_event_ns {
        bar.close = print.price_ticks;
        bar.last_ts_event_ns = print.ts_event_ns;
    }
}

fn fold_volume(bar: &mut Bar, print: TradePrint) {
    let size = u64::from(print.size);
    bar.volume += size;
    match print.aggressor {
        Some(BookSide::Bid) => bar.buy_volume += size,
        Some(BookSide::Ask) => bar.sell_volume += size,
        None => {}
    }
    bar.trade_count += 1;
}
