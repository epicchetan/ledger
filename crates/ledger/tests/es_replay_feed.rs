mod support;

use std::path::Path;
use std::sync::{
    atomic::{AtomicBool, Ordering},
    Arc,
};
use std::time::Duration;

use anyhow::Result;
use async_trait::async_trait;
use cache::{Cache, CellDescriptor, CellKind, CellOwner, Key, ValueKey};
use ledger::clock::ClockState;
use ledger::feed::es_replay::{
    es_replay_component_id, EsMboFeedBatch, EsReplayCells, EsReplayCursor, EsReplayFeed,
};
use runtime::{ComponentHandle, ComponentStatus, ExternalWriteBatch, RuntimeHandle, RuntimeWorker};
use store::{
    test_util::MemoryRemote, ObjectMetadata, RemoteObject, RemoteStore, Store, StoreConfig,
    StoreObjectId,
};
use tokio::sync::Notify;
use tokio::task::JoinHandle;
use tokio::time::timeout;

use support::{event, fabricate_prepared_day, store_fixture};

const PARK: Duration = Duration::from_millis(50);
const WAKE: Duration = Duration::from_secs(2);
const TEST_CATCHUP_CHUNK_BATCHES: usize = 1024;

#[tokio::test]
async fn install_process_reports_preparing_during_artifact_prepare_then_running() {
    let tempdir = tempfile::tempdir().unwrap();
    let remote = Arc::new(BlockingRemote::new());
    let store = Arc::new(
        Store::open(
            tempdir.path(),
            StoreConfig {
                local_max_bytes: 1024 * 1024,
            },
            remote.clone(),
        )
        .unwrap(),
    );
    let (raw_id, artifact) =
        fabricate_prepared_day(&store, vec![event(100, 1), event(200, 2)]).await;
    store.remove_local_copy(&artifact.id).unwrap();
    let cache = Cache::new();
    let clock_key = register_clock(&cache, None);
    let cells = EsReplayCells::register(&cache).unwrap();
    let mut cursor_watch = cache.watch_key(cells.cursor.key()).unwrap();
    let (worker, handle) = RuntimeWorker::new(cache.clone());
    let worker_join = tokio::spawn(worker.run());

    let component = install_feed(&handle, store, raw_id, clock_key, cells.clone()).await;

    assert_eq!(
        component.status().await.unwrap(),
        ComponentStatus::Preparing
    );
    remote.release();
    wait_for_cursor(&cache, &mut cursor_watch, &cells.cursor, |cursor| {
        cursor.feed_seq == 0
    })
    .await;
    assert_eq!(component.status().await.unwrap(), ComponentStatus::Running);

    shutdown_worker(handle, worker_join).await;
}

#[tokio::test]
async fn feed_treats_missing_clock_value_as_paused_at_zero_and_emits_nothing() {
    let direct = DirectFeed::start(vec![event(100, 1)]).await;
    let mut cursor_watch = direct.cursor_watch;

    assert!(timeout(PARK, cursor_watch.changed()).await.is_err());
    assert!(direct
        .cache
        .read_array(&direct.cells.batches)
        .unwrap()
        .is_empty());

    shutdown_worker(direct.handle, direct.worker).await;
}

#[tokio::test]
async fn feed_acknowledges_paused_clock_without_emitting_data() {
    let direct = DirectFeed::start(vec![event(100, 1)]).await;
    let mut cursor_watch = direct.cursor_watch;

    submit_clock(
        &direct.handle,
        &direct.clock_key,
        ClockState::initial().seek_to(99),
    )
    .await;

    timeout(PARK, cursor_watch.changed())
        .await
        .unwrap()
        .unwrap();
    let cursor = direct
        .cache
        .read_value(&direct.cells.cursor)
        .unwrap()
        .unwrap();
    assert_eq!(cursor.feed_seq, 0);
    assert_eq!(cursor.batch_idx, 0);
    let status = direct
        .cache
        .read_value(&direct.cells.status)
        .unwrap()
        .unwrap();
    assert_eq!(status.clock.revision, 1);
    assert_eq!(status.clock.session_now_ns, 99);
    assert_eq!(status.cursor, cursor);
    assert!(direct
        .cache
        .read_array(&direct.cells.batches)
        .unwrap()
        .is_empty());

    shutdown_worker(direct.handle, direct.worker).await;
}

#[tokio::test]
async fn feed_wakes_on_clock_cell_write_without_polling() {
    let direct = DirectFeed::start(vec![event(100, 1)]).await;
    let mut cursor_watch = direct.cursor_watch;

    submit_clock(
        &direct.handle,
        &direct.clock_key,
        ClockState::initial().seek_to(100),
    )
    .await;
    let cursor = wait_for_cursor(
        &direct.cache,
        &mut cursor_watch,
        &direct.cells.cursor,
        |cursor| cursor.feed_seq == 1,
    )
    .await;

    assert_eq!(cursor.batch_idx, 1);
    assert_eq!(
        direct
            .cache
            .read_array(&direct.cells.batches)
            .unwrap()
            .len(),
        1
    );

    shutdown_worker(direct.handle, direct.worker).await;
}

#[tokio::test]
async fn feed_emits_ordered_catch_up_burst_when_seek_jumps_past_several_batches() {
    let direct = DirectFeed::start(vec![event(100, 1), event(200, 2), event(300, 3)]).await;
    let mut cursor_watch = direct.cursor_watch;

    submit_clock(
        &direct.handle,
        &direct.clock_key,
        ClockState::initial().seek_to(250),
    )
    .await;
    wait_for_cursor(
        &direct.cache,
        &mut cursor_watch,
        &direct.cells.cursor,
        |cursor| cursor.feed_seq >= 2,
    )
    .await;
    let batches = direct.cache.read_array(&direct.cells.batches).unwrap();

    assert_eq!(batch_indices(&batches), vec![0, 1]);
    assert_eq!(
        batches
            .iter()
            .map(|batch| batch.ts_event_ns)
            .collect::<Vec<_>>(),
        vec![100, 200]
    );

    shutdown_worker(direct.handle, direct.worker).await;
}

#[tokio::test]
async fn forward_seek_emits_due_batches_in_chunks_and_marks_catch_up() {
    let emitted = TEST_CATCHUP_CHUNK_BATCHES * 3 + 17;
    let direct = DirectFeed::start(numbered_events(emitted + 1)).await;
    let mut cursor_watch = direct.cursor_watch;

    submit_clock(
        &direct.handle,
        &direct.clock_key,
        ClockState::initial().seek_to(emitted as u64),
    )
    .await;
    wait_for_cursor(
        &direct.cache,
        &mut cursor_watch,
        &direct.cells.cursor,
        |cursor| cursor.catching_up,
    )
    .await;
    let cursor = wait_for_cursor(
        &direct.cache,
        &mut cursor_watch,
        &direct.cells.cursor,
        |cursor| cursor.batch_idx == emitted,
    )
    .await;
    let batches = direct.cache.read_array(&direct.cells.batches).unwrap();

    assert!(!cursor.catching_up);
    assert_eq!(cursor.feed_seq, emitted as u64);
    assert_eq!(cursor.batch_idx, emitted);
    assert_eq!(cursor.next_ts_event_ns, Some((emitted + 1) as u64));
    assert_eq!(batches.len(), emitted);
    assert_eq!(batch_indices(&batches), (0..emitted).collect::<Vec<_>>());

    shutdown_worker(direct.handle, direct.worker).await;
}

#[tokio::test]
async fn feed_emits_batches_as_session_time_crosses_timestamps_in_realtime() {
    let direct = DirectFeed::start(vec![event(5_000_000, 1), event(15_000_000, 2)]).await;
    let mut cursor_watch = direct.cursor_watch;
    let running = ClockState::initial().seek_to(0).play();

    submit_clock(&direct.handle, &direct.clock_key, running).await;
    let cursor = wait_for_cursor(
        &direct.cache,
        &mut cursor_watch,
        &direct.cells.cursor,
        |cursor| cursor.feed_seq == 2,
    )
    .await;

    assert!(cursor.ended);
    assert_eq!(
        direct
            .cache
            .read_array(&direct.cells.batches)
            .unwrap()
            .len(),
        2
    );

    shutdown_worker(direct.handle, direct.worker).await;
}

#[tokio::test]
async fn feed_updates_cursor_and_status_after_each_emission() {
    let direct = DirectFeed::start(vec![event(100, 1), event(200, 2)]).await;
    let mut cursor_watch = direct.cursor_watch;

    submit_clock(
        &direct.handle,
        &direct.clock_key,
        ClockState::initial().seek_to(100),
    )
    .await;
    let cursor = wait_for_cursor(
        &direct.cache,
        &mut cursor_watch,
        &direct.cells.cursor,
        |cursor| cursor.feed_seq == 1,
    )
    .await;
    let status = direct
        .cache
        .read_value(&direct.cells.status)
        .unwrap()
        .unwrap();

    assert_eq!(status.raw_object_id, direct.raw_id.to_string());
    assert!(status.artifact_object_id.is_some());
    assert_eq!(status.cursor, cursor);
    assert_eq!(cursor.ts_event_ns, Some(100));
    assert_eq!(cursor.next_ts_event_ns, Some(200));

    shutdown_worker(direct.handle, direct.worker).await;
}

#[tokio::test]
async fn backward_seek_truncates_batches_resets_index_bumps_epoch_and_clears_ended() {
    let direct = DirectFeed::start(vec![event(100, 1), event(200, 2), event(300, 3)]).await;
    let mut cursor_watch = direct.cursor_watch;

    submit_clock(
        &direct.handle,
        &direct.clock_key,
        ClockState::initial().seek_to(500),
    )
    .await;
    wait_for_cursor(
        &direct.cache,
        &mut cursor_watch,
        &direct.cells.cursor,
        |cursor| cursor.ended,
    )
    .await;
    submit_clock(
        &direct.handle,
        &direct.clock_key,
        ClockState::initial().seek_to(150),
    )
    .await;
    let cursor = wait_for_cursor(
        &direct.cache,
        &mut cursor_watch,
        &direct.cells.cursor,
        |cursor| cursor.epoch == 1,
    )
    .await;
    let batches = direct.cache.read_array(&direct.cells.batches).unwrap();

    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].ts_event_ns, 100);
    assert_eq!(cursor.feed_seq, 3);
    assert_eq!(cursor.batch_idx, 1);
    assert_eq!(batches.len(), cursor.batch_idx);
    assert_eq!(cursor.next_ts_event_ns, Some(200));
    assert!(!cursor.catching_up);
    assert!(!cursor.ended);
    assert_eq!(
        direct
            .cache
            .read_value(&direct.cells.status)
            .unwrap()
            .unwrap()
            .cursor,
        cursor
    );

    shutdown_worker(direct.handle, direct.worker).await;
}

#[tokio::test]
async fn mid_catch_up_backward_seek_regresses_without_finishing_stale_pass() {
    let total = TEST_CATCHUP_CHUNK_BATCHES * 3 + 7;
    let direct = DirectFeed::start(numbered_events(total)).await;
    let mut cursor_watch = direct.cursor_watch;

    submit_clock(
        &direct.handle,
        &direct.clock_key,
        ClockState::initial().seek_to(total as u64),
    )
    .await;
    wait_for_cursor(
        &direct.cache,
        &mut cursor_watch,
        &direct.cells.cursor,
        |cursor| cursor.catching_up && cursor.batch_idx >= TEST_CATCHUP_CHUNK_BATCHES,
    )
    .await;
    submit_clock(
        &direct.handle,
        &direct.clock_key,
        ClockState::initial().seek_to(10),
    )
    .await;
    let cursor = wait_for_cursor(
        &direct.cache,
        &mut cursor_watch,
        &direct.cells.cursor,
        |cursor| cursor.epoch == 1 && cursor.batch_idx == 10,
    )
    .await;
    let batches = direct.cache.read_array(&direct.cells.batches).unwrap();

    assert_eq!(batches.len(), 10);
    assert!(batches.iter().all(|batch| batch.ts_event_ns <= 10));
    assert!(!cursor.catching_up);
    assert!(cursor.feed_seq < total as u64);

    shutdown_worker(direct.handle, direct.worker).await;
}

#[tokio::test]
async fn feed_parks_after_final_batch_and_backward_seek_revives_it() {
    let direct = DirectFeed::start(vec![event(100, 1), event(200, 2)]).await;
    let mut cursor_watch = direct.cursor_watch;
    let component = direct.component.clone();

    submit_clock(
        &direct.handle,
        &direct.clock_key,
        ClockState::initial().seek_to(500),
    )
    .await;
    wait_for_cursor(
        &direct.cache,
        &mut cursor_watch,
        &direct.cells.cursor,
        |cursor| cursor.ended,
    )
    .await;
    assert!(timeout(PARK, cursor_watch.changed()).await.is_err());
    assert_eq!(component.status().await.unwrap(), ComponentStatus::Running);

    submit_clock(
        &direct.handle,
        &direct.clock_key,
        ClockState::initial().seek_to(50),
    )
    .await;
    wait_for_cursor(
        &direct.cache,
        &mut cursor_watch,
        &direct.cells.cursor,
        |cursor| cursor.epoch == 1 && !cursor.ended,
    )
    .await;
    submit_clock(
        &direct.handle,
        &direct.clock_key,
        ClockState::initial().seek_to(100),
    )
    .await;
    let cursor = wait_for_cursor(
        &direct.cache,
        &mut cursor_watch,
        &direct.cells.cursor,
        |cursor| cursor.feed_seq == 3,
    )
    .await;

    assert_eq!(cursor.ts_event_ns, Some(100));

    shutdown_worker(direct.handle, direct.worker).await;
}

#[tokio::test]
async fn feed_writes_are_attributed_to_feed_component_owner() {
    let direct = DirectFeed::start(vec![event(100, 1)]).await;
    let mut cursor_watch = direct.cursor_watch;

    submit_clock(
        &direct.handle,
        &direct.clock_key,
        ClockState::initial().seek_to(100),
    )
    .await;
    wait_for_cursor(
        &direct.cache,
        &mut cursor_watch,
        &direct.cells.cursor,
        |cursor| cursor.feed_seq == 1,
    )
    .await;

    let descriptor = direct.cache.describe(direct.cells.batches.key()).unwrap();
    assert_eq!(descriptor.owner, es_replay_component_id().owner());
    assert_eq!(
        direct.component.status().await.unwrap(),
        ComponentStatus::Running
    );

    shutdown_worker(direct.handle, direct.worker).await;
}

#[test]
fn feed_cannot_write_the_clock_cell_because_cache_ownership_rejects_it() {
    let cache = Cache::new();
    let clock_key = register_clock(&cache, None);
    let err = cache
        .set_value(
            &es_replay_component_id().owner(),
            &clock_key,
            ClockState::initial(),
        )
        .unwrap_err();

    assert!(matches!(err, cache::CacheError::OwnerMismatch { .. }));
}

struct DirectFeed {
    cache: Cache,
    handle: RuntimeHandle,
    worker: JoinHandle<Result<(), runtime::RuntimeError>>,
    cells: EsReplayCells,
    clock_key: ValueKey<ClockState>,
    raw_id: StoreObjectId,
    cursor_watch: cache::CellWatch,
    component: ComponentHandle,
}

impl DirectFeed {
    async fn start(events: Vec<ledger::market::EsMboEvent>) -> Self {
        let fixture = store_fixture();
        let (raw_id, _artifact) = fabricate_prepared_day(&fixture.store, events).await;
        let cache = Cache::new();
        let clock_key = register_clock(&cache, None);
        let cells = EsReplayCells::register(&cache).unwrap();
        let mut cursor_watch = cache.watch_key(cells.cursor.key()).unwrap();
        let (worker, handle) = RuntimeWorker::new(cache.clone());
        let worker = tokio::spawn(worker.run());
        let component = install_feed(
            &handle,
            fixture.store.clone(),
            raw_id.clone(),
            clock_key.clone(),
            cells.clone(),
        )
        .await;

        wait_for_cursor(&cache, &mut cursor_watch, &cells.cursor, |cursor| {
            cursor.feed_seq == 0
        })
        .await;

        Self {
            cache,
            handle,
            worker,
            cells,
            clock_key,
            raw_id,
            cursor_watch,
            component,
        }
    }
}

async fn install_feed<S>(
    handle: &RuntimeHandle,
    store: Arc<Store<S>>,
    raw_id: StoreObjectId,
    clock_key: ValueKey<ClockState>,
    cells: EsReplayCells,
) -> ComponentHandle
where
    S: RemoteStore + 'static,
{
    timeout(
        WAKE,
        handle.install_process(EsReplayFeed::new(raw_id, store, clock_key, cells)),
    )
    .await
    .unwrap()
    .unwrap()
}

fn register_clock(cache: &Cache, initial: Option<ClockState>) -> ValueKey<ClockState> {
    cache
        .register_value::<ClockState>(
            CellDescriptor {
                key: Key::new("session.clock").unwrap(),
                owner: session_owner(),
                kind: CellKind::Value,
                public_read: true,
            },
            initial,
        )
        .unwrap()
}

async fn submit_clock(handle: &RuntimeHandle, clock_key: &ValueKey<ClockState>, clock: ClockState) {
    let mut batch = ExternalWriteBatch::new(session_owner());
    batch.set_value(clock_key, clock);
    timeout(WAKE, handle.submit_external_writes(batch))
        .await
        .unwrap()
        .unwrap();
}

async fn wait_for_cursor(
    cache: &Cache,
    watch: &mut cache::CellWatch,
    key: &ValueKey<EsReplayCursor>,
    mut predicate: impl FnMut(&EsReplayCursor) -> bool,
) -> EsReplayCursor {
    timeout(WAKE, async {
        loop {
            if let Some(cursor) = cache.read_value(key).unwrap() {
                if predicate(&cursor) {
                    return cursor;
                }
            }
            watch.changed().await.unwrap();
        }
    })
    .await
    .unwrap()
}

async fn shutdown_worker(
    handle: RuntimeHandle,
    worker: JoinHandle<Result<(), runtime::RuntimeError>>,
) {
    timeout(WAKE, handle.shutdown()).await.unwrap().unwrap();
    timeout(WAKE, worker).await.unwrap().unwrap().unwrap();
}

fn session_owner() -> CellOwner {
    CellOwner::new("session").unwrap()
}

fn batch_indices(batches: &[EsMboFeedBatch]) -> Vec<usize> {
    batches.iter().map(|batch| batch.batch_idx).collect()
}

fn numbered_events(count: usize) -> Vec<ledger::market::EsMboEvent> {
    (0..count)
        .map(|idx| event((idx + 1) as u64, (idx + 1) as u64))
        .collect()
}

#[derive(Clone)]
struct BlockingRemote {
    inner: MemoryRemote,
    notify: Arc<Notify>,
    block_get: Arc<AtomicBool>,
}

impl BlockingRemote {
    fn new() -> Self {
        Self {
            inner: MemoryRemote::new(),
            notify: Arc::new(Notify::new()),
            block_get: Arc::new(AtomicBool::new(true)),
        }
    }

    fn release(&self) {
        self.block_get.store(false, Ordering::SeqCst);
        self.notify.notify_waiters();
    }
}

#[async_trait]
impl RemoteStore for BlockingRemote {
    async fn put_path(
        &self,
        key: &str,
        path: &Path,
        metadata: &ObjectMetadata,
    ) -> Result<RemoteObject> {
        self.inner.put_path(key, path, metadata).await
    }

    async fn get_to_path(&self, key: &str, dest: &Path) -> Result<RemoteObject> {
        if self.block_get.load(Ordering::SeqCst) {
            self.notify.notified().await;
        }
        self.inner.get_to_path(key, dest).await
    }

    async fn head(&self, key: &str) -> Result<Option<RemoteObject>> {
        self.inner.head(key).await
    }

    async fn delete(&self, key: &str) -> Result<()> {
        self.inner.delete(key).await
    }

    async fn put_bytes(
        &self,
        key: &str,
        bytes: &[u8],
        metadata: &ObjectMetadata,
    ) -> Result<RemoteObject> {
        self.inner.put_bytes(key, bytes, metadata).await
    }

    async fn get_bytes(&self, key: &str) -> Result<Vec<u8>> {
        self.inner.get_bytes(key).await
    }

    async fn list_keys(&self, prefix: &str) -> Result<Vec<String>> {
        self.inner.list_keys(prefix).await
    }

    fn bucket(&self) -> &str {
        self.inner.bucket()
    }
}
