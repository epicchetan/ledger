use std::sync::Arc;

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

        publish_cursor_status(
            &ctx,
            &cells,
            &raw_object_id,
            artifact_object_id.as_deref(),
            &state.cursor(&event_store),
            &clock_or_initial(&ctx, &clock_key)?.snapshot(),
        )
        .await?;

        loop {
            if shutdown.is_shutdown() {
                return Ok(());
            }

            let clock = clock_or_initial(&ctx, &clock_key)?;
            let now = clock.now_ns();

            if state.regressed(now) {
                let kept = state.regress(now, &ctx, &cells)?;
                let cursor = state.cursor(&event_store);
                let status = status(
                    &raw_object_id,
                    artifact_object_id.as_deref(),
                    cursor.clone(),
                    clock.snapshot(),
                );
                let mut batch = ctx.batch();
                batch
                    .replace_array(&cells.batches, kept)
                    .set_value(&cells.cursor, cursor)
                    .set_value(&cells.status, status);
                ctx.submit(batch).await?;
                continue;
            }

            while let Some(span) = event_store.batches.get(state.next_idx) {
                if span.ts_event_ns > now {
                    break;
                }
                state
                    .emit(
                        span,
                        &ctx,
                        &cells,
                        &raw_object_id,
                        artifact_object_id.as_deref(),
                        &event_store,
                        &clock,
                    )
                    .await?;
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
}

impl FeedState {
    fn new(event_store: &EsMboEventStore) -> Self {
        let _ = event_store;
        Self {
            epoch: 0,
            next_idx: 0,
            feed_seq: 0,
            last_emitted_ts: None,
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

    fn regress(
        &mut self,
        now: UnixNanos,
        ctx: &ProcessContext,
        cells: &EsReplayCells,
    ) -> Result<Vec<EsMboFeedBatch>, ComponentError> {
        let kept = ctx
            .read_array(&cells.batches)?
            .into_iter()
            .filter(|batch| batch.ts_event_ns <= now)
            .collect::<Vec<_>>();
        // The truncated array is the source of truth: unapplied feed emissions
        // queued before the seek may be dropped by the replace below.
        self.next_idx = kept.last().map(|batch| batch.batch_idx + 1).unwrap_or(0);
        self.last_emitted_ts = kept.last().map(|batch| batch.ts_event_ns);
        self.epoch = self.epoch.saturating_add(1);

        Ok(kept)
    }

    async fn emit(
        &mut self,
        span: &EsMboBatchSpan,
        ctx: &ProcessContext,
        cells: &EsReplayCells,
        raw_object_id: &StoreObjectId,
        artifact_object_id: Option<&str>,
        event_store: &EsMboEventStore,
        clock: &ClockState,
    ) -> Result<(), ComponentError> {
        let feed_seq = self.feed_seq.saturating_add(1);
        let feed_batch = feed_batch(feed_seq, self.next_idx, span, event_store);
        self.next_idx += 1;
        self.feed_seq = feed_seq;
        self.last_emitted_ts = Some(span.ts_event_ns);

        let cursor = self.cursor(event_store);
        let status = status(
            raw_object_id,
            artifact_object_id,
            cursor.clone(),
            clock.snapshot(),
        );
        let mut batch = ctx.batch();
        batch
            .push_array(&cells.batches, vec![feed_batch])
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
