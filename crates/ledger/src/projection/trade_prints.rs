use async_trait::async_trait;
use cache::{ArrayKey, Cache, CellDescriptor, CellKind, CellOwner, Key, ValueKey};
use runtime::{ComponentError, ComponentId, RuntimeTask, TaskContext, TaskDescriptor, TaskOutcome};

use crate::feed::es_replay::{EsMboFeedBatch, EsReplayCells};
use crate::market::{canonical_trade_print, TradePrint, UnixNanos};
use crate::LedgerError;

pub(crate) const COMPONENT_ID: &str = "projection.trade_prints";
const REBUILD_CHUNK_BATCHES: usize = 4096;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TradePrintStatus {
    pub(crate) epoch: u64,
    pub(crate) processed_batches: usize,
    pub(crate) print_count: usize,
    pub(crate) revision: u64,
    pub(crate) last_ts_event_ns: Option<UnixNanos>,
}

#[derive(Debug, Clone)]
pub(crate) struct TradePrintCells {
    pub(crate) prints: ArrayKey<TradePrint>,
    pub(crate) status: ValueKey<TradePrintStatus>,
}

impl TradePrintCells {
    fn register(cache: &Cache) -> Result<Self, LedgerError> {
        let owner = CellOwner::new(COMPONENT_ID)?;
        let prints = cache.register_array::<TradePrint>(
            descriptor(
                "projection.trade_prints.prints",
                owner.clone(),
                CellKind::Array,
            )?,
            Vec::new(),
        )?;
        let status = cache.register_value::<TradePrintStatus>(
            descriptor("projection.trade_prints.status", owner, CellKind::Value)?,
            None,
        )?;
        Ok(Self { prints, status })
    }
}

struct CanonicalTradePrintsTask {
    descriptor: TaskDescriptor,
    feed: EsReplayCells,
    cells: TradePrintCells,
    epoch: u64,
    processed_batches: usize,
    print_count: usize,
    revision: u64,
    last_ts_event_ns: Option<UnixNanos>,
    fold: Fold,
}

enum Fold {
    Incremental,
    Rebuilding { epoch: u64, fold_idx: usize },
}

impl CanonicalTradePrintsTask {
    fn new(feed: EsReplayCells, cells: TradePrintCells) -> Result<Self, LedgerError> {
        let component_id = ComponentId::new(COMPONENT_ID)
            .map_err(|error| LedgerError::ProjectionPlan(error.to_string()))?;
        let descriptor = TaskDescriptor::new(component_id, vec![feed.batches.key().clone()]);
        Ok(Self {
            descriptor,
            feed,
            cells,
            epoch: 0,
            processed_batches: 0,
            print_count: 0,
            revision: 0,
            last_ts_event_ns: None,
            fold: Fold::Incremental,
        })
    }

    fn status_snapshot(&self) -> TradePrintStatus {
        TradePrintStatus {
            epoch: self.epoch,
            processed_batches: self.processed_batches,
            print_count: self.print_count,
            revision: self.revision,
            last_ts_event_ns: self.last_ts_event_ns,
        }
    }

    fn reset_fold(&mut self, epoch: u64) {
        self.epoch = epoch;
        self.processed_batches = 0;
        self.print_count = 0;
        self.last_ts_event_ns = None;
    }

    fn canonical_prints(&mut self, batches: &[EsMboFeedBatch]) -> Vec<TradePrint> {
        let prints: Vec<_> = batches
            .iter()
            .flat_map(|batch| batch.events.iter())
            .filter_map(canonical_trade_print)
            .collect();
        if let Some(last) = prints.last() {
            self.last_ts_event_ns = Some(last.ts_event_ns);
        }
        prints
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
        let prints = self.canonical_prints(&batches);
        self.processed_batches = chunk_end;
        self.print_count += prints.len();
        self.revision = self.revision.saturating_add(1);

        let mut batch = ctx.batch();
        if first_chunk {
            batch.replace_array(&self.cells.prints, prints);
        } else if !prints.is_empty() {
            batch.push_array(&self.cells.prints, prints);
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

pub(crate) fn install_trade_prints_projection(
    cache: &Cache,
    feed: EsReplayCells,
) -> Result<super::InstalledProjectionNode, LedgerError> {
    let cells = TradePrintCells::register(cache)?;
    let task = CanonicalTradePrintsTask::new(feed, cells.clone())?;
    Ok(super::InstalledProjectionNode {
        node: super::ProjectionNodeSpec::CanonicalTradePrints,
        output: super::ProjectionOutput::TradePrints(cells),
        task: Box::new(task),
        delivery: None,
    })
}

#[async_trait]
impl RuntimeTask for CanonicalTradePrintsTask {
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
            let chunk_end = cursor
                .batch_idx
                .min(self.processed_batches + REBUILD_CHUNK_BATCHES);
            let batches =
                ctx.read_array_range(&self.feed.batches, self.processed_batches..chunk_end)?;
            let prints = self.canonical_prints(&batches);
            self.processed_batches = chunk_end;
            self.print_count += prints.len();
            self.revision = self.revision.saturating_add(1);

            let mut batch = ctx.batch();
            if !prints.is_empty() {
                batch.push_array(&self.cells.prints, prints);
            }
            batch.set_value(&self.cells.status, self.status_snapshot());
            ctx.submit(batch).await?;
            if chunk_end < cursor.batch_idx {
                return Ok(TaskOutcome::WakeAgain);
            }
        }

        Ok(TaskOutcome::Idle)
    }
}

fn descriptor(key: &str, owner: CellOwner, kind: CellKind) -> Result<CellDescriptor, LedgerError> {
    Ok(CellDescriptor {
        key: Key::new(key)?,
        owner,
        kind,
        public_read: false,
    })
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use cache::Cache;
    use runtime::{ExternalWriteBatch, RuntimeWorker};

    use super::*;
    use crate::feed::es_replay::{feed_owner, EsReplayCursor};
    use crate::market::{BookAction, BookSide, EsMboEvent, PriceTicks};

    const WAKE: Duration = Duration::from_secs(2);

    #[tokio::test]
    async fn canonical_stream_preserves_prints_lineage_and_epoch_replacement() {
        let cache = Cache::new();
        let feed = EsReplayCells::register(&cache).unwrap();
        let cells = TradePrintCells::register(&cache).unwrap();
        let task = CanonicalTradePrintsTask::new(feed.clone(), cells.clone()).unwrap();
        let reader = cache.reader();
        let mut status_watch = reader.watch_key(cells.status.key()).unwrap();
        let (worker, runtime) = RuntimeWorker::new(cache);
        let worker = tokio::spawn(worker.run());
        runtime.install_task(task).await.unwrap();

        let initial = wait_for_status(&reader, &mut status_watch, &cells, |status| {
            status.epoch == 0 && status.processed_batches == 0
        })
        .await;
        assert_eq!(initial.print_count, 0);
        assert_eq!(initial.last_ts_event_ns, None);

        let batches = vec![
            feed_batch(
                0,
                vec![
                    event(10, 1, BookAction::Trade, Some(100), 2, Some(BookSide::Bid)),
                    event(11, 2, BookAction::Fill, Some(100), 2, Some(BookSide::Ask)),
                    event(12, 3, BookAction::Trade, Some(101), 3, None),
                ],
            ),
            feed_batch(
                1,
                vec![event(
                    20,
                    4,
                    BookAction::Add,
                    Some(102),
                    4,
                    Some(BookSide::Ask),
                )],
            ),
            feed_batch(
                2,
                vec![
                    event(30, 5, BookAction::Trade, None, 5, Some(BookSide::Bid)),
                    event(31, 6, BookAction::Trade, Some(103), 0, Some(BookSide::Bid)),
                    event(32, 7, BookAction::Trade, Some(104), 6, Some(BookSide::Ask)),
                ],
            ),
        ];
        write_feed(&runtime, &feed, batches[..1].to_vec(), cursor(0, 1)).await;
        let first = wait_for_status(&reader, &mut status_watch, &cells, |status| {
            status.epoch == 0 && status.processed_batches == 1
        })
        .await;
        assert_eq!(first.print_count, 2);

        write_feed(&runtime, &feed, batches[..2].to_vec(), cursor(0, 2)).await;
        let no_print = wait_for_status(&reader, &mut status_watch, &cells, |status| {
            status.epoch == 0 && status.processed_batches == 2
        })
        .await;
        assert_eq!(no_print.print_count, first.print_count);
        assert!(no_print.revision > first.revision);

        write_feed(&runtime, &feed, batches.clone(), cursor(0, 3)).await;
        let forward = wait_for_status(&reader, &mut status_watch, &cells, |status| {
            status.epoch == 0 && status.processed_batches == 3
        })
        .await;
        let prints = reader.read_array(&cells.prints).unwrap();
        assert_eq!(forward.print_count, prints.len());
        assert_eq!(forward.print_count, 3);
        assert_eq!(forward.last_ts_event_ns, Some(32));
        assert_eq!(
            prints,
            vec![
                TradePrint {
                    ts_event_ns: 10,
                    price_ticks: PriceTicks(100),
                    size: 2,
                    aggressor: Some(BookSide::Bid),
                },
                TradePrint {
                    ts_event_ns: 12,
                    price_ticks: PriceTicks(101),
                    size: 3,
                    aggressor: None,
                },
                TradePrint {
                    ts_event_ns: 32,
                    price_ticks: PriceTicks(104),
                    size: 6,
                    aggressor: Some(BookSide::Ask),
                },
            ]
        );

        write_feed(&runtime, &feed, batches[..2].to_vec(), cursor(1, 2)).await;
        let rebuilt = wait_for_status(&reader, &mut status_watch, &cells, |status| {
            status.epoch == 1 && status.processed_batches == 2
        })
        .await;
        let rebuilt_prints = reader.read_array(&cells.prints).unwrap();
        assert_eq!(rebuilt.print_count, rebuilt_prints.len());
        assert_eq!(rebuilt.print_count, 2);
        assert_eq!(rebuilt.last_ts_event_ns, Some(12));
        assert!(rebuilt.revision > forward.revision);

        let no_print_batches = (0..(REBUILD_CHUNK_BATCHES + 1))
            .map(|batch_idx| {
                feed_batch(
                    batch_idx,
                    vec![event(
                        batch_idx as u64,
                        batch_idx as u64,
                        BookAction::Add,
                        Some(100),
                        1,
                        Some(BookSide::Bid),
                    )],
                )
            })
            .collect::<Vec<_>>();
        write_feed(
            &runtime,
            &feed,
            no_print_batches,
            cursor(2, REBUILD_CHUNK_BATCHES + 1),
        )
        .await;
        let chunked = wait_for_status(&reader, &mut status_watch, &cells, |status| {
            status.epoch == 2 && status.processed_batches == REBUILD_CHUNK_BATCHES + 1
        })
        .await;
        assert_eq!(chunked.print_count, 0);
        assert!(reader.read_array(&cells.prints).unwrap().is_empty());
        assert!(chunked.revision >= rebuilt.revision + 2);

        assert!(!reader.describe(cells.prints.key()).unwrap().public_read);
        assert!(!reader.describe(cells.status.key()).unwrap().public_read);

        runtime.shutdown().await.unwrap();
        worker.await.unwrap().unwrap();
    }

    async fn wait_for_status(
        cache: &cache::CacheReader,
        watch: &mut cache::CellWatch,
        cells: &TradePrintCells,
        mut predicate: impl FnMut(&TradePrintStatus) -> bool,
    ) -> TradePrintStatus {
        tokio::time::timeout(WAKE, async {
            loop {
                if let Some(status) = cache.read_value(&cells.status).unwrap() {
                    if predicate(&status) {
                        return status;
                    }
                }
                watch.changed().await.unwrap();
            }
        })
        .await
        .expect("canonical trade prints should converge")
    }

    async fn write_feed(
        runtime: &runtime::RuntimeHandle,
        feed: &EsReplayCells,
        batches: Vec<EsMboFeedBatch>,
        cursor: EsReplayCursor,
    ) {
        let mut batch = ExternalWriteBatch::new(feed_owner().unwrap());
        batch.replace_array(&feed.batches, batches);
        batch.set_value(&feed.cursor, cursor);
        runtime.submit_external_writes(batch).await.unwrap();
    }

    fn cursor(epoch: u64, batch_idx: usize) -> EsReplayCursor {
        let total_batches = batch_idx.max(3);
        EsReplayCursor {
            epoch,
            feed_seq: batch_idx as u64,
            batch_idx,
            total_batches,
            ts_event_ns: None,
            next_ts_event_ns: None,
            catching_up: false,
            ended: batch_idx == total_batches,
        }
    }

    fn feed_batch(batch_idx: usize, events: Vec<EsMboEvent>) -> EsMboFeedBatch {
        let ts_event_ns = events.last().map(|event| event.ts_event_ns).unwrap_or(0);
        EsMboFeedBatch {
            feed_seq: (batch_idx + 1) as u64,
            batch_idx,
            ts_event_ns,
            source_first_ts_ns: events.first().map(|event| event.ts_event_ns).unwrap_or(0),
            source_last_ts_ns: ts_event_ns,
            events,
        }
    }

    fn event(
        ts_event_ns: u64,
        sequence: u64,
        action: BookAction,
        price_ticks: Option<i64>,
        size: u32,
        side: Option<BookSide>,
    ) -> EsMboEvent {
        EsMboEvent {
            ts_event_ns,
            ts_recv_ns: ts_event_ns,
            sequence,
            action,
            side,
            price_ticks: price_ticks.map(PriceTicks),
            size,
            order_id: sequence,
            flags: 0,
            is_last: true,
        }
    }
}
