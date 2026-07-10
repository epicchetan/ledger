use std::{ops::Range, sync::Arc};

use async_trait::async_trait;
use cache::ValueKey;
use runtime::{ComponentDescriptor, ComponentError, ProcessContext, ProcessPrepareContext};
use store::{RemoteStore, Store, StoreObjectId};

use crate::clock::{ClockMode, ClockState};
use crate::feed::es_replay::{
    es_replay_component_id, prepare_es_replay_artifact, read_event_store_file, EsMboFeedBatch,
    EsReplayArtifact, EsReplayCells, EsReplayCursor, EsReplayStatus,
};
use crate::market::{EsMboBatchSpan, EsMboEventStore, UnixNanos};
use crate::LedgerError;

const CATCHUP_CHUNK_BATCHES: usize = 1024;

pub struct EsReplayFeed<S>
where
    S: RemoteStore + 'static,
{
    descriptor: ComponentDescriptor,
    raw_object_id: StoreObjectId,
    store: Arc<Store<S>>,
    clock_key: ValueKey<ClockState>,
    cells: EsReplayCells,
    artifact: Option<EsReplayArtifact>,
    event_store: Option<EsMboEventStore>,
}

impl<S> EsReplayFeed<S>
where
    S: RemoteStore + 'static,
{
    pub fn new(
        raw_object_id: StoreObjectId,
        store: Arc<Store<S>>,
        clock_key: ValueKey<ClockState>,
        cells: EsReplayCells,
    ) -> Self {
        Self {
            descriptor: ComponentDescriptor::process(es_replay_component_id()),
            raw_object_id,
            store,
            clock_key,
            cells,
            artifact: None,
            event_store: None,
        }
    }
}

#[async_trait]
impl<S> runtime::RuntimeProcess for EsReplayFeed<S>
where
    S: RemoteStore + 'static,
{
    fn descriptor(&self) -> &ComponentDescriptor {
        &self.descriptor
    }

    async fn prepare(&mut self, _ctx: ProcessPrepareContext) -> Result<(), ComponentError> {
        let artifact = prepare_es_replay_artifact(&self.store, &self.raw_object_id, false, None)
            .await
            .map_err(component_message)?;
        let path = artifact.path.clone();
        let event_store = tokio::task::spawn_blocking(move || read_event_store_file(&path))
            .await
            .map_err(|err| ComponentError::Message(err.to_string()))?
            .map_err(component_message)?;

        self.artifact = Some(artifact);
        self.event_store = Some(event_store);
        Ok(())
    }

    async fn run(self: Box<Self>, ctx: ProcessContext) -> Result<(), ComponentError> {
        let Self {
            raw_object_id,
            clock_key,
            cells,
            artifact,
            event_store,
            ..
        } = *self;
        let artifact = artifact.ok_or_else(|| {
            ComponentError::Message("ES replay feed run called before prepare artifact".to_string())
        })?;
        let event_store = event_store.ok_or_else(|| {
            ComponentError::Message(
                "ES replay feed run called before prepare event store".to_string(),
            )
        })?;
        let artifact_object_id = Some(artifact.descriptor.id.to_string());
        let mut shutdown = ctx.shutdown().clone();
        let mut clock_watch = ctx.cache().watch_key(clock_key.key())?;
        let mut state = FeedState::new(&event_store);

        let initial_clock = clock_or_initial(&ctx, &clock_key)?;
        publish_cursor_status(
            &ctx,
            &cells,
            &raw_object_id,
            artifact_object_id.as_deref(),
            &state.cursor(&event_store),
            &initial_clock.snapshot(),
        )
        .await?;
        let mut published_clock_revision = initial_clock.revision;

        loop {
            if shutdown.is_shutdown() {
                return Ok(());
            }

            let clock = clock_or_initial(&ctx, &clock_key)?;
            let now = clock.now_ns();

            if state.regressed(now) {
                let removed = state.regress(now, &event_store);
                let cursor = state.cursor(&event_store);
                let status = status(
                    &raw_object_id,
                    artifact_object_id.as_deref(),
                    cursor.clone(),
                    clock.snapshot(),
                );
                let mut batch = ctx.batch();
                batch
                    .remove_array_range(&cells.batches, removed)
                    .set_value(&cells.cursor, cursor)
                    .set_value(&cells.status, status);
                ctx.submit(batch).await?;
                published_clock_revision = clock.revision;
                continue;
            }

            let chunk = state.build_due_chunk(now, &event_store);
            if !chunk.is_empty() {
                let more_due = event_store
                    .batches
                    .get(state.next_idx + chunk.len())
                    .is_some_and(|span| span.ts_event_ns <= now);
                state.advance_by(chunk.len(), more_due, &event_store);
                state
                    .emit_chunk(
                        chunk,
                        &ctx,
                        &cells,
                        &raw_object_id,
                        artifact_object_id.as_deref(),
                        &event_store,
                        &clock,
                    )
                    .await?;
                published_clock_revision = clock.revision;
                tokio::task::yield_now().await;
                continue;
            }

            // A seek can land between feed batches (or at the current feed
            // extent), leaving no batch mutation with which to acknowledge
            // the new clock. Publish the unchanged cursor and status only
            // after regression/catch-up work is exhausted. Delivery barriers
            // can then distinguish this post-clock state from the identical
            // pre-seek cursor without briefly accepting stale convergence.
            if published_clock_revision < clock.revision {
                publish_cursor_status(
                    &ctx,
                    &cells,
                    &raw_object_id,
                    artifact_object_id.as_deref(),
                    &state.cursor(&event_store),
                    &clock.snapshot(),
                )
                .await?;
                published_clock_revision = clock.revision;
                continue;
            }

            if state.ended(&event_store) || clock.mode == ClockMode::Paused {
                tokio::select! {
                    changed = clock_watch.changed() => {
                        changed?;
                    }
                    changed = shutdown.changed() => {
                        changed?;
                    }
                }
                continue;
            }

            let Some(next_ts) = state.next_ts(&event_store) else {
                continue;
            };
            let Some(deadline) = clock.wall_deadline(next_ts) else {
                continue;
            };
            tokio::select! {
                _ = tokio::time::sleep_until(tokio::time::Instant::from_std(deadline)) => {}
                changed = clock_watch.changed() => {
                    changed?;
                }
                changed = shutdown.changed() => {
                    changed?;
                }
            }
        }
    }
}

struct FeedState {
    epoch: u64,
    next_idx: usize,
    feed_seq: u64,
    last_emitted_ts: Option<UnixNanos>,
    catching_up: bool,
}

impl FeedState {
    fn new(event_store: &EsMboEventStore) -> Self {
        let _ = event_store;
        Self {
            epoch: 0,
            next_idx: 0,
            feed_seq: 0,
            last_emitted_ts: None,
            catching_up: false,
        }
    }

    fn cursor(&self, event_store: &EsMboEventStore) -> EsReplayCursor {
        EsReplayCursor {
            epoch: self.epoch,
            feed_seq: self.feed_seq,
            batch_idx: self.next_idx,
            total_batches: event_store.batches.len(),
            ts_event_ns: self.last_emitted_ts,
            next_ts_event_ns: self.next_ts(event_store),
            catching_up: self.catching_up,
            ended: self.ended(event_store),
        }
    }

    fn next_ts(&self, event_store: &EsMboEventStore) -> Option<UnixNanos> {
        event_store
            .batches
            .get(self.next_idx)
            .map(|batch| batch.ts_event_ns)
    }

    fn ended(&self, event_store: &EsMboEventStore) -> bool {
        self.next_idx == event_store.batches.len()
    }

    fn regressed(&self, now: UnixNanos) -> bool {
        self.last_emitted_ts
            .map(|last_emitted| now < last_emitted)
            .unwrap_or(false)
    }

    fn regress(&mut self, now: UnixNanos, event_store: &EsMboEventStore) -> Range<usize> {
        // All of this feed's emits are FIFO-ordered ahead of this write on the
        // single external-write channel, so at apply time the cache batches
        // array is exactly event_store.batches[0..self.next_idx]. The cut can
        // therefore be computed from the immutable event store.
        let cut =
            event_store.batches[..self.next_idx].partition_point(|span| span.ts_event_ns <= now);
        let removed = cut..self.next_idx;
        self.next_idx = cut;
        self.last_emitted_ts = (cut > 0).then(|| event_store.batches[cut - 1].ts_event_ns);
        self.epoch = self.epoch.saturating_add(1);
        self.catching_up = false;

        removed
    }

    fn build_due_chunk(
        &self,
        now: UnixNanos,
        event_store: &EsMboEventStore,
    ) -> Vec<EsMboFeedBatch> {
        let mut chunk = Vec::new();
        while let Some(span) = event_store.batches.get(self.next_idx + chunk.len()) {
            if span.ts_event_ns > now || chunk.len() == CATCHUP_CHUNK_BATCHES {
                break;
            }
            let feed_seq = self
                .feed_seq
                .saturating_add(chunk.len() as u64)
                .saturating_add(1);
            chunk.push(feed_batch(
                feed_seq,
                self.next_idx + chunk.len(),
                span,
                event_store,
            ));
        }
        chunk
    }

    fn advance_by(&mut self, batch_count: usize, catching_up: bool, event_store: &EsMboEventStore) {
        self.next_idx += batch_count;
        self.feed_seq = self.feed_seq.saturating_add(batch_count as u64);
        self.last_emitted_ts =
            (batch_count > 0).then(|| event_store.batches[self.next_idx - 1].ts_event_ns);
        self.catching_up = catching_up;
    }

    async fn emit_chunk(
        &self,
        feed_batches: Vec<EsMboFeedBatch>,
        ctx: &ProcessContext,
        cells: &EsReplayCells,
        raw_object_id: &StoreObjectId,
        artifact_object_id: Option<&str>,
        event_store: &EsMboEventStore,
        clock: &ClockState,
    ) -> Result<(), ComponentError> {
        let cursor = self.cursor(event_store);
        let status = status(
            raw_object_id,
            artifact_object_id,
            cursor.clone(),
            clock.snapshot(),
        );
        let mut batch = ctx.batch();
        batch
            .push_array(&cells.batches, feed_batches)
            .set_value(&cells.cursor, cursor)
            .set_value(&cells.status, status);
        ctx.submit(batch).await
    }
}

async fn publish_cursor_status(
    ctx: &ProcessContext,
    cells: &EsReplayCells,
    raw_object_id: &StoreObjectId,
    artifact_object_id: Option<&str>,
    cursor: &EsReplayCursor,
    clock: &crate::clock::ClockSnapshot,
) -> Result<(), ComponentError> {
    let mut batch = ctx.batch();
    batch.set_value(&cells.cursor, cursor.clone()).set_value(
        &cells.status,
        EsReplayStatus {
            raw_object_id: raw_object_id.to_string(),
            artifact_object_id: artifact_object_id.map(str::to_string),
            clock: clock.clone(),
            cursor: cursor.clone(),
        },
    );
    ctx.submit(batch).await
}

fn feed_batch(
    feed_seq: u64,
    batch_idx: usize,
    span: &EsMboBatchSpan,
    event_store: &EsMboEventStore,
) -> EsMboFeedBatch {
    let events = event_store.events[span.start_idx..span.end_idx].to_vec();
    let source_first_ts_ns = events
        .first()
        .map(|event| event.ts_event_ns)
        .unwrap_or(span.ts_event_ns);
    let source_last_ts_ns = events
        .last()
        .map(|event| event.ts_event_ns)
        .unwrap_or(span.ts_event_ns);
    EsMboFeedBatch {
        feed_seq,
        batch_idx,
        ts_event_ns: span.ts_event_ns,
        source_first_ts_ns,
        source_last_ts_ns,
        events,
    }
}

fn status(
    raw_object_id: &StoreObjectId,
    artifact_object_id: Option<&str>,
    cursor: EsReplayCursor,
    clock: crate::clock::ClockSnapshot,
) -> EsReplayStatus {
    EsReplayStatus {
        raw_object_id: raw_object_id.to_string(),
        artifact_object_id: artifact_object_id.map(str::to_string),
        clock,
        cursor,
    }
}

fn clock_or_initial(
    ctx: &ProcessContext,
    clock_key: &ValueKey<ClockState>,
) -> Result<ClockState, ComponentError> {
    Ok(ctx
        .read_value(clock_key)?
        .unwrap_or_else(ClockState::initial))
}

fn component_message(error: LedgerError) -> ComponentError {
    ComponentError::Message(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::market::{build_batches, BookAction, BookSide, EsMboEvent, PriceTicks};

    #[test]
    fn catch_up_chunks_advance_per_batch_and_flag_until_final_chunk() {
        let emitted = CATCHUP_CHUNK_BATCHES * 2 + 11;
        let event_store = event_store(emitted + 1);
        let mut state = FeedState::new(&event_store);
        let mut chunk_sizes = Vec::new();
        let mut catching_up = Vec::new();

        while state.next_idx < emitted {
            let chunk = state.build_due_chunk(emitted as u64, &event_store);
            assert!(!chunk.is_empty());
            assert!(chunk.len() <= CATCHUP_CHUNK_BATCHES);
            let more_due = event_store
                .batches
                .get(state.next_idx + chunk.len())
                .is_some_and(|span| span.ts_event_ns <= emitted as u64);
            state.advance_by(chunk.len(), more_due, &event_store);
            chunk_sizes.push(chunk.len());
            catching_up.push(state.cursor(&event_store).catching_up);
        }

        assert_eq!(
            chunk_sizes,
            vec![CATCHUP_CHUNK_BATCHES, CATCHUP_CHUNK_BATCHES, 11]
        );
        assert_eq!(catching_up, vec![true, true, false]);
        assert_eq!(state.feed_seq, emitted as u64);
        assert_eq!(state.next_idx, emitted);
    }

    #[test]
    fn regress_computes_truncation_from_event_store_prefix() {
        let emitted = CATCHUP_CHUNK_BATCHES + 3;
        let event_store = event_store(emitted + 1);
        let mut state = FeedState::new(&event_store);
        state.advance_by(emitted, true, &event_store);

        let removed = state.regress(10, &event_store);

        assert_eq!(removed, 10..emitted);
        assert_eq!(state.next_idx, 10);
        assert_eq!(state.last_emitted_ts, Some(10));
        assert_eq!(state.epoch, 1);
        assert!(!state.catching_up);
    }

    fn event_store(count: usize) -> EsMboEventStore {
        let events = (0..count)
            .map(|idx| {
                let ts_event_ns = (idx + 1) as u64;
                EsMboEvent {
                    ts_event_ns,
                    ts_recv_ns: ts_event_ns,
                    sequence: ts_event_ns,
                    action: BookAction::Trade,
                    side: Some(BookSide::Bid),
                    price_ticks: Some(PriceTicks(100)),
                    size: 1,
                    order_id: ts_event_ns,
                    flags: 0,
                    is_last: true,
                }
            })
            .collect::<Vec<_>>();
        let batches = build_batches(&events);
        EsMboEventStore { events, batches }
    }
}
