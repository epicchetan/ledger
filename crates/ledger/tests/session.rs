mod support;

use std::time::Duration;

use async_trait::async_trait;
use cache::{CellDescriptor, CellKind, CellOwner, Key, ValueKey};
use ledger::feed::es_replay::{es_replay_component_id, EsMboFeedBatch, EsReplayCells};
use ledger::session::{LedgerSessionBuilder, LedgerSessionHandle};
use runtime::{
    ComponentDescriptor, ComponentError, ComponentId, ExternalWriteBatch, RuntimeTask, TaskContext,
    TaskDescriptor, TaskOutcome,
};
use tokio::time::timeout;

use support::{event, fabricate_prepared_day, store_fixture, StoreFixture};

const WAKE: Duration = Duration::from_secs(2);

#[tokio::test]
async fn feed_submitted_writes_enter_runtime_through_generic_external_write_path() {
    let mut running = RunningSession::start(vec![event(100, 1)]).await;

    seek_to(&running.session, 100).await;
    wait_for_cursor(
        running.session.cache(),
        &mut running.cursor_watch,
        &running.cells,
        |cursor| cursor.feed_seq == 1,
    )
    .await;

    let batches = running
        .session
        .cache()
        .read_array(&running.cells.batches)
        .unwrap();
    assert_eq!(batches.len(), 1);
    assert_eq!(batches[0].ts_event_ns, 100);

    shutdown_session(running.session).await;
}

#[tokio::test]
async fn task_component_depending_on_batches_runs_when_batches_key_changes() {
    let running = RunningSession::start(vec![event(100, 1)]).await;
    let (output, mut output_watch) = install_count_task(&running.session, &running.cells).await;

    seek_to(&running.session, 100).await;
    wait_for_output(running.session.cache(), &mut output_watch, &output, 1).await;

    shutdown_session(running.session).await;
}

#[tokio::test]
async fn ledger_session_does_not_implement_component_scheduling_policy() {
    let mut running = RunningSession::start(vec![event(100, 1), event(200, 2)]).await;
    let (output, mut output_watch) = install_count_task(&running.session, &running.cells).await;

    seek_to(&running.session, 200).await;
    wait_for_output(running.session.cache(), &mut output_watch, &output, 2).await;
    wait_for_cursor(
        running.session.cache(),
        &mut running.cursor_watch,
        &running.cells,
        |cursor| cursor.feed_seq == 2,
    )
    .await;

    shutdown_session(running.session).await;
}

#[tokio::test]
async fn runtime_handle_shutdown_stops_feed_process_cleanly() {
    let running = RunningSession::start(vec![event(100, 1)]).await;

    assert!(matches!(
        timeout(
            WAKE,
            running
                .session
                .runtime()
                .component_status(&es_replay_component_id())
        )
        .await
        .unwrap()
        .unwrap(),
        runtime::ComponentStatus::Running
    ));

    shutdown_session(running.session).await;
}

#[tokio::test]
async fn session_shutdown_awaits_worker_join_and_surfaces_worker_errors() {
    let fixture = store_fixture();
    let builder = LedgerSessionBuilder::new(fixture.store.clone()).unwrap();
    let session = timeout(WAKE, builder.start()).await.unwrap().unwrap();
    let owner = CellOwner::new("test.owner").unwrap();
    let key = session
        .cache()
        .register_value::<u64>(
            CellDescriptor {
                key: Key::new("test.owner.value").unwrap(),
                owner,
                kind: CellKind::Value,
                public_read: true,
            },
            None,
        )
        .unwrap();
    let mut batch = ExternalWriteBatch::new(CellOwner::new("wrong.owner").unwrap());
    batch.set_value(&key, 1);

    timeout(WAKE, session.runtime().submit_external_writes(batch))
        .await
        .unwrap()
        .unwrap();
    let err = timeout(WAKE, session.shutdown())
        .await
        .unwrap()
        .unwrap_err();

    assert!(err.to_string().contains("cannot mutate cell"));
}

#[tokio::test]
async fn step_driver_synchronizes_on_cursor_watches_without_drain_or_polling() {
    let mut running =
        RunningSession::start(vec![event(100, 1), event(200, 2), event(300, 3)]).await;

    step_batches(
        &running.session,
        &running.cells,
        &mut running.cursor_watch,
        2,
    )
    .await;

    let batches = running
        .session
        .cache()
        .read_array(&running.cells.batches)
        .unwrap();
    assert_eq!(
        batches
            .iter()
            .map(|batch| batch.ts_event_ns)
            .collect::<Vec<_>>(),
        vec![100, 200]
    );

    shutdown_session(running.session).await;
}

struct RunningSession {
    _fixture: StoreFixture,
    session: LedgerSessionHandle,
    cells: EsReplayCells,
    cursor_watch: cache::CellWatch,
}

impl RunningSession {
    async fn start(events: Vec<ledger::market::EsMboEvent>) -> Self {
        let fixture = store_fixture();
        let (raw_id, _artifact) = fabricate_prepared_day(&fixture.store, events).await;
        let mut builder = LedgerSessionBuilder::new(fixture.store.clone()).unwrap();
        let cells = builder.es_replay(raw_id).unwrap();
        let session = timeout(WAKE, builder.start()).await.unwrap().unwrap();
        let mut cursor_watch = session.cache().watch_key(cells.cursor.key()).unwrap();
        wait_for_cursor(session.cache(), &mut cursor_watch, &cells, |cursor| {
            cursor.feed_seq == 0
        })
        .await;

        Self {
            _fixture: fixture,
            session,
            cells,
            cursor_watch,
        }
    }
}

async fn install_count_task(
    session: &LedgerSessionHandle,
    cells: &EsReplayCells,
) -> (ValueKey<usize>, cache::CellWatch) {
    let id = ComponentId::new("task.count_batches").unwrap();
    let output = session
        .cache()
        .register_value::<usize>(
            CellDescriptor {
                key: Key::new("task.count_batches.output").unwrap(),
                owner: id.owner(),
                kind: CellKind::Value,
                public_read: true,
            },
            None,
        )
        .unwrap();
    let output_watch = session.cache().watch_key(output.key()).unwrap();
    timeout(
        WAKE,
        session.runtime().install_task(CountBatchesTask {
            descriptor: TaskDescriptor {
                component: ComponentDescriptor::task(id),
                dependencies: vec![cells.batches.key().clone()],
            },
            input: cells.batches.clone(),
            output: output.clone(),
        }),
    )
    .await
    .unwrap()
    .unwrap();
    (output, output_watch)
}

struct CountBatchesTask {
    descriptor: TaskDescriptor,
    input: cache::ArrayKey<EsMboFeedBatch>,
    output: ValueKey<usize>,
}

#[async_trait]
impl RuntimeTask for CountBatchesTask {
    fn descriptor(&self) -> &TaskDescriptor {
        &self.descriptor
    }

    async fn run_once(&mut self, ctx: TaskContext<'_>) -> Result<TaskOutcome, ComponentError> {
        let count = ctx.read_array(&self.input)?.len();
        let mut batch = ctx.batch();
        batch.set_value(&self.output, count);
        ctx.submit(batch).await?;
        Ok(TaskOutcome::Idle)
    }
}

async fn step_batches(
    session: &LedgerSessionHandle,
    cells: &EsReplayCells,
    cursor_watch: &mut cache::CellWatch,
    count: usize,
) {
    let start = session
        .cache()
        .read_value(&cells.cursor)
        .unwrap()
        .unwrap()
        .feed_seq;
    loop {
        let cursor = session.cache().read_value(&cells.cursor).unwrap().unwrap();
        if cursor.feed_seq.saturating_sub(start) >= count as u64 || cursor.ended {
            return;
        }
        let last_seen = cursor.feed_seq;
        seek_to(session, cursor.next_ts_event_ns.unwrap()).await;
        wait_for_cursor(session.cache(), cursor_watch, cells, |cursor| {
            cursor.feed_seq > last_seen || cursor.ended
        })
        .await;
    }
}

async fn seek_to(session: &LedgerSessionHandle, session_ns: u64) {
    timeout(WAKE, session.seek_to(session_ns))
        .await
        .unwrap()
        .unwrap();
}

async fn wait_for_cursor(
    cache: &cache::Cache,
    watch: &mut cache::CellWatch,
    cells: &EsReplayCells,
    mut predicate: impl FnMut(&ledger::feed::es_replay::EsReplayCursor) -> bool,
) -> ledger::feed::es_replay::EsReplayCursor {
    timeout(WAKE, async {
        loop {
            if let Some(cursor) = cache.read_value(&cells.cursor).unwrap() {
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

async fn wait_for_output(
    cache: &cache::Cache,
    watch: &mut cache::CellWatch,
    key: &ValueKey<usize>,
    expected: usize,
) {
    timeout(WAKE, async {
        loop {
            if cache.read_value(key).unwrap() == Some(expected) {
                return;
            }
            watch.changed().await.unwrap();
        }
    })
    .await
    .unwrap();
}

async fn shutdown_session(session: LedgerSessionHandle) {
    timeout(WAKE, session.shutdown()).await.unwrap().unwrap();
}
