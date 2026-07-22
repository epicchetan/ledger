use std::{
    sync::{
        atomic::{AtomicBool, Ordering},
        Arc, Mutex,
    },
    time::Duration,
};

use async_trait::async_trait;
use cache::{Cache, CellDescriptor, CellKind, CellOwner, Key, ValueKey};
use runtime::{
    ComponentDescriptor, ComponentError, ComponentId, ComponentStatus, ProcessContext,
    RuntimeProcess, RuntimeTask, RuntimeWorker, TaskContext, TaskDescriptor, TaskOutcome,
};
use tokio::sync::Notify;

fn key(value: &str) -> Key {
    Key::new(value).unwrap()
}

fn component_id(value: &str) -> ComponentId {
    ComponentId::new(value).unwrap()
}

fn descriptor(key_path: &str, owner: CellOwner, kind: CellKind) -> CellDescriptor {
    CellDescriptor {
        key: key(key_path),
        owner,
        kind,
        public_read: false,
    }
}

async fn wait_until(mut condition: impl FnMut() -> bool) {
    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            if condition() {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("condition should become true");
}

struct OneShotProcess {
    descriptor: ComponentDescriptor,
    source: ValueKey<i32>,
    value: i32,
}

#[async_trait]
impl RuntimeProcess for OneShotProcess {
    fn descriptor(&self) -> &ComponentDescriptor {
        &self.descriptor
    }

    async fn run(self: Box<Self>, ctx: ProcessContext) -> Result<(), ComponentError> {
        let mut batch = ctx.batch();
        batch.set_value(&self.source, self.value);
        ctx.submit(batch).await
    }
}

struct CopyTask {
    descriptor: TaskDescriptor,
    input: ValueKey<i32>,
    output: ValueKey<i32>,
    log: Arc<Mutex<Vec<i32>>>,
}

#[async_trait]
impl RuntimeTask for CopyTask {
    fn descriptor(&self) -> &TaskDescriptor {
        &self.descriptor
    }

    async fn run_once(&mut self, ctx: TaskContext<'_>) -> Result<TaskOutcome, ComponentError> {
        if let Some(value) = ctx.read_value(&self.input)? {
            self.log.lock().unwrap().push(value);
            let mut batch = ctx.batch();
            batch.set_value(&self.output, value);
            ctx.submit(batch).await?;
        }
        Ok(TaskOutcome::Idle)
    }
}

struct WakeAgainChainTask {
    descriptor: TaskDescriptor,
    remaining_wakes: usize,
    done: ValueKey<bool>,
    first_step: Option<Arc<Notify>>,
}

struct TwoCommitTask {
    descriptor: TaskDescriptor,
    first: ValueKey<i32>,
    second: ValueKey<i32>,
    first_committed: Arc<Notify>,
    release: Arc<Notify>,
}

#[async_trait]
impl RuntimeTask for TwoCommitTask {
    fn descriptor(&self) -> &TaskDescriptor {
        &self.descriptor
    }

    async fn run_once(&mut self, ctx: TaskContext<'_>) -> Result<TaskOutcome, ComponentError> {
        let mut first = ctx.batch();
        first.set_value(&self.first, 1);
        ctx.submit(first).await?;
        self.first_committed.notify_one();
        self.release.notified().await;
        let mut second = ctx.batch();
        second.set_value(&self.second, 1);
        ctx.submit(second).await?;
        Ok(TaskOutcome::Idle)
    }
}

#[async_trait]
impl RuntimeTask for WakeAgainChainTask {
    fn descriptor(&self) -> &TaskDescriptor {
        &self.descriptor
    }

    async fn run_once(&mut self, ctx: TaskContext<'_>) -> Result<TaskOutcome, ComponentError> {
        if let Some(first_step) = self.first_step.take() {
            first_step.notify_waiters();
        }
        if self.remaining_wakes > 0 {
            self.remaining_wakes -= 1;
            Ok(TaskOutcome::WakeAgain)
        } else {
            let mut batch = ctx.batch();
            batch.set_value(&self.done, true);
            ctx.submit(batch).await?;
            Ok(TaskOutcome::Idle)
        }
    }
}

#[tokio::test]
async fn worker_applies_external_writes_between_wake_again_steps() {
    let cache = Cache::new();
    let task_id = component_id("task.chain");
    let session_owner = CellOwner::new("session").unwrap();
    let value = cache
        .register_value(
            descriptor("session.value", session_owner.clone(), CellKind::Value),
            None::<i32>,
        )
        .unwrap();
    let done = cache
        .register_value(
            descriptor("task.chain.done", task_id.owner(), CellKind::Value),
            Some(false),
        )
        .unwrap();
    let mut value_watch = cache.watch_key(value.key()).unwrap();
    let first_step = Arc::new(Notify::new());
    let (worker, handle) = RuntimeWorker::new(cache.clone());
    let worker_join = tokio::spawn(worker.run());
    handle
        .install_task(WakeAgainChainTask {
            descriptor: TaskDescriptor::new(task_id.clone(), Vec::new()),
            remaining_wakes: 64,
            done: done.clone(),
            first_step: Some(first_step.clone()),
        })
        .await
        .unwrap();

    let writer = {
        let handle = handle.clone();
        let value = value.clone();
        tokio::spawn(async move {
            first_step.notified().await;
            let mut batch = runtime::ExternalWriteBatch::new(session_owner);
            batch.set_value(&value, 7);
            handle.submit_external_writes(batch).await.unwrap();
        })
    };
    let queued = {
        let handle = handle.clone();
        let task_id = task_id.clone();
        tokio::spawn(async move { handle.queue_task(&task_id).await })
    };

    tokio::time::timeout(Duration::from_secs(1), value_watch.changed())
        .await
        .expect("external write should be applied before chain completes")
        .unwrap();
    assert_eq!(cache.read_value(&value).unwrap(), Some(7));
    assert_eq!(cache.read_value(&done).unwrap(), Some(false));

    queued.await.unwrap().unwrap();
    writer.await.unwrap();
    wait_until(|| cache.read_value(&done).unwrap() == Some(true)).await;
    handle.shutdown().await.unwrap();
    worker_join.await.unwrap().unwrap();
}

#[tokio::test]
async fn worker_write_arm_allows_wake_again_chains_past_default_budget() {
    let cache = Cache::new();
    let task_id = component_id("task.long_chain");
    let session_owner = CellOwner::new("session").unwrap();
    let trigger = cache
        .register_value(
            descriptor("session.trigger", session_owner.clone(), CellKind::Value),
            None::<i32>,
        )
        .unwrap();
    let done = cache
        .register_value(
            descriptor("task.long_chain.done", task_id.owner(), CellKind::Value),
            Some(false),
        )
        .unwrap();
    let mut done_watch = cache.watch_key(done.key()).unwrap();
    let (worker, handle) = RuntimeWorker::new(cache.clone());
    let worker_join = tokio::spawn(worker.run());
    handle
        .install_task(WakeAgainChainTask {
            descriptor: TaskDescriptor::new(task_id, vec![trigger.key().clone()]),
            remaining_wakes: 1_500,
            done: done.clone(),
            first_step: None,
        })
        .await
        .unwrap();

    let mut batch = runtime::ExternalWriteBatch::new(session_owner);
    batch.set_value(&trigger, 1);
    handle.submit_external_writes(batch).await.unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if cache.read_value(&done).unwrap() == Some(true) {
                return;
            }
            done_watch.changed().await.unwrap();
        }
    })
    .await
    .expect("long WakeAgain chain should complete");

    handle.shutdown().await.unwrap();
    worker_join.await.unwrap().unwrap();
}

#[tokio::test]
async fn worker_applies_process_writes_and_runs_dependent_task() {
    let cache = Cache::new();
    let process_id = component_id("process.source");
    let task_id = component_id("task.copy");
    let source = cache
        .register_value(
            descriptor("process.source.value", process_id.owner(), CellKind::Value),
            None::<i32>,
        )
        .unwrap();
    let output = cache
        .register_value(
            descriptor("task.copy.output", task_id.owner(), CellKind::Value),
            None::<i32>,
        )
        .unwrap();
    let log = Arc::new(Mutex::new(Vec::new()));
    let (worker, handle) = RuntimeWorker::new(cache.clone());
    let worker_join = tokio::spawn(worker.run());

    handle
        .install_task(CopyTask {
            descriptor: TaskDescriptor::new(task_id.clone(), vec![source.key().clone()]),
            input: source.clone(),
            output: output.clone(),
            log: log.clone(),
        })
        .await
        .unwrap();
    handle
        .install_process(OneShotProcess {
            descriptor: ComponentDescriptor::process(process_id),
            source,
            value: 42,
        })
        .await
        .unwrap();

    wait_until(|| cache.read_value(&output).unwrap() == Some(42)).await;

    assert_eq!(log.lock().unwrap().as_slice(), &[42]);
    handle.shutdown().await.unwrap();
    worker_join.await.unwrap().unwrap();
}

struct ShutdownProcess {
    descriptor: ComponentDescriptor,
    stopped: Arc<AtomicBool>,
}

#[async_trait]
impl RuntimeProcess for ShutdownProcess {
    fn descriptor(&self) -> &ComponentDescriptor {
        &self.descriptor
    }

    async fn run(self: Box<Self>, mut ctx: ProcessContext) -> Result<(), ComponentError> {
        while !ctx.shutdown().is_shutdown() {
            ctx.shutdown_mut().changed().await?;
        }
        self.stopped.store(true, Ordering::SeqCst);
        Ok(())
    }
}

#[tokio::test]
async fn shutdown_reaches_runtime_owned_processes() {
    let cache = Cache::new();
    let process_id = component_id("process.shutdown");
    let stopped = Arc::new(AtomicBool::new(false));
    let (worker, handle) = RuntimeWorker::new(cache);
    let worker_join = tokio::spawn(worker.run());
    let process = handle
        .install_process(ShutdownProcess {
            descriptor: ComponentDescriptor::process(process_id),
            stopped: stopped.clone(),
        })
        .await
        .unwrap();

    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            if matches!(process.status().await.unwrap(), ComponentStatus::Running) {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("process should start running");

    handle.shutdown().await.unwrap();
    worker_join.await.unwrap().unwrap();

    assert!(stopped.load(Ordering::SeqCst));
}

struct ShutdownWriteProcess {
    descriptor: ComponentDescriptor,
    source: ValueKey<i32>,
}

#[async_trait]
impl RuntimeProcess for ShutdownWriteProcess {
    fn descriptor(&self) -> &ComponentDescriptor {
        &self.descriptor
    }

    async fn run(self: Box<Self>, mut ctx: ProcessContext) -> Result<(), ComponentError> {
        while !ctx.shutdown().is_shutdown() {
            ctx.shutdown_mut().changed().await?;
        }
        let mut batch = ctx.batch();
        batch.set_value(&self.source, 9);
        ctx.submit(batch).await
    }
}

#[tokio::test]
async fn shutdown_drains_ready_process_writes_before_worker_exits() {
    let cache = Cache::new();
    let process_id = component_id("process.shutdown_writer");
    let task_id = component_id("task.shutdown_copy");
    let source = cache
        .register_value(
            descriptor(
                "process.shutdown_writer.value",
                process_id.owner(),
                CellKind::Value,
            ),
            None::<i32>,
        )
        .unwrap();
    let output = cache
        .register_value(
            descriptor(
                "task.shutdown_copy.output",
                task_id.owner(),
                CellKind::Value,
            ),
            None::<i32>,
        )
        .unwrap();
    let (worker, handle) = RuntimeWorker::new(cache.clone());
    let worker_join = tokio::spawn(worker.run());
    handle
        .install_task(CopyTask {
            descriptor: TaskDescriptor::new(task_id, vec![source.key().clone()]),
            input: source.clone(),
            output: output.clone(),
            log: Arc::new(Mutex::new(Vec::new())),
        })
        .await
        .unwrap();
    let process = handle
        .install_process(ShutdownWriteProcess {
            descriptor: ComponentDescriptor::process(process_id),
            source,
        })
        .await
        .unwrap();

    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            if matches!(process.status().await.unwrap(), ComponentStatus::Running) {
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("process should start running");

    handle.shutdown().await.unwrap();
    worker_join.await.unwrap().unwrap();

    assert_eq!(cache.read_value(&output).unwrap(), Some(9));
}

struct PanicRunProcess {
    descriptor: ComponentDescriptor,
}

#[async_trait]
impl RuntimeProcess for PanicRunProcess {
    fn descriptor(&self) -> &ComponentDescriptor {
        &self.descriptor
    }

    async fn run(self: Box<Self>, _ctx: ProcessContext) -> Result<(), ComponentError> {
        panic!("forced process panic");
    }
}

#[tokio::test]
async fn process_run_panic_marks_component_failed_and_does_not_hang_shutdown() {
    let cache = Cache::new();
    let process_id = component_id("process.panic_run");
    let (worker, handle) = RuntimeWorker::new(cache);
    let worker_join = tokio::spawn(worker.run());
    let process = handle
        .install_process(PanicRunProcess {
            descriptor: ComponentDescriptor::process(process_id),
        })
        .await
        .unwrap();

    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            if let ComponentStatus::Failed(message) = process.status().await.unwrap() {
                assert!(message.contains("panicked"));
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("process panic should be reported");

    handle.shutdown().await.unwrap();
    worker_join.await.unwrap().unwrap();
}

struct PanicPrepareProcess {
    descriptor: ComponentDescriptor,
}

#[async_trait]
impl RuntimeProcess for PanicPrepareProcess {
    fn descriptor(&self) -> &ComponentDescriptor {
        &self.descriptor
    }

    async fn prepare(
        &mut self,
        _ctx: runtime::ProcessPrepareContext,
    ) -> Result<(), ComponentError> {
        panic!("forced process prepare panic");
    }

    async fn run(self: Box<Self>, _ctx: ProcessContext) -> Result<(), ComponentError> {
        Ok(())
    }
}

#[tokio::test]
async fn process_prepare_panic_marks_component_failed_and_does_not_hang_shutdown() {
    let cache = Cache::new();
    let process_id = component_id("process.panic_prepare");
    let (worker, handle) = RuntimeWorker::new(cache);
    let worker_join = tokio::spawn(worker.run());
    let process = handle
        .install_process(PanicPrepareProcess {
            descriptor: ComponentDescriptor::process(process_id),
        })
        .await
        .unwrap();

    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            if let ComponentStatus::Failed(message) = process.status().await.unwrap() {
                assert!(message.contains("panicked"));
                return;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("process prepare panic should be reported");

    handle.shutdown().await.unwrap();
    worker_join.await.unwrap().unwrap();
}

#[tokio::test]
async fn worker_rejects_duplicate_component_ids_across_processes_and_tasks() {
    let cache = Cache::new();
    let id = component_id("component.duplicate");
    let source = cache
        .register_value(
            descriptor("component.duplicate.value", id.owner(), CellKind::Value),
            None::<i32>,
        )
        .unwrap();
    let (worker, handle) = RuntimeWorker::new(cache);
    let worker_join = tokio::spawn(worker.run());

    handle
        .install_task(CopyTask {
            descriptor: TaskDescriptor::new(id.clone(), Vec::new()),
            input: source.clone(),
            output: source.clone(),
            log: Arc::new(Mutex::new(Vec::new())),
        })
        .await
        .unwrap();
    let result = handle
        .install_process(OneShotProcess {
            descriptor: ComponentDescriptor::process(id.clone()),
            source,
            value: 1,
        })
        .await;
    let err = match result {
        Ok(_) => panic!("duplicate process install should fail"),
        Err(error) => error,
    };

    assert!(matches!(err, runtime::RuntimeError::DuplicateComponent(found) if found == id));
    handle.shutdown().await.unwrap();
    worker_join.await.unwrap().unwrap();
}

#[tokio::test]
async fn snapshots_never_observe_half_of_one_external_write_batch() {
    let cache = Cache::new();
    let owner = CellOwner::new("session").unwrap();
    let first = cache
        .register_value(
            descriptor("session.first", owner.clone(), CellKind::Value),
            Some(0_i32),
        )
        .unwrap();
    let second = cache
        .register_value(
            descriptor("session.second", owner.clone(), CellKind::Value),
            Some(0_i32),
        )
        .unwrap();
    let (worker, handle) = RuntimeWorker::new(cache);
    let worker_join = tokio::spawn(worker.run());

    for value in 1..=256 {
        let mut batch = runtime::ExternalWriteBatch::new(owner.clone());
        batch.set_value(&first, value).set_value(&second, value);
        handle.submit_external_writes(batch).await.unwrap();
        let first_key = first.clone();
        let second_key = second.clone();
        let pair = handle
            .snapshot(move |view| {
                Ok((
                    view.read_value(&first_key)?.unwrap(),
                    view.read_value(&second_key)?.unwrap(),
                ))
            })
            .await
            .unwrap();
        assert_eq!(pair.0, pair.1);
    }

    let metrics = handle.snapshot_metrics();
    assert_eq!(metrics.requests, 256);
    assert_eq!(metrics.completed, 256);
    handle.shutdown().await.unwrap();
    worker_join.await.unwrap().unwrap();
}

#[tokio::test]
async fn snapshot_waits_until_a_task_step_with_multiple_local_commits_finishes() {
    let cache = Cache::new();
    let task_id = component_id("task.two_commit");
    let first = cache
        .register_value(
            descriptor("task.two_commit.first", task_id.owner(), CellKind::Value),
            Some(0_i32),
        )
        .unwrap();
    let second = cache
        .register_value(
            descriptor("task.two_commit.second", task_id.owner(), CellKind::Value),
            Some(0_i32),
        )
        .unwrap();
    let first_committed = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let (worker, handle) = RuntimeWorker::new(cache);
    let worker_join = tokio::spawn(worker.run());
    handle
        .install_task(TwoCommitTask {
            descriptor: TaskDescriptor::new(task_id.clone(), Vec::new()),
            first: first.clone(),
            second: second.clone(),
            first_committed: first_committed.clone(),
            release: release.clone(),
        })
        .await
        .unwrap();

    let queued = {
        let handle = handle.clone();
        tokio::spawn(async move { handle.queue_task(&task_id).await })
    };
    first_committed.notified().await;
    let snapshot = {
        let handle = handle.clone();
        let first = first.clone();
        let second = second.clone();
        tokio::spawn(async move {
            handle
                .snapshot(move |view| {
                    Ok((
                        view.read_value(&first)?.unwrap(),
                        view.read_value(&second)?.unwrap(),
                    ))
                })
                .await
        })
    };
    tokio::time::sleep(Duration::from_millis(25)).await;
    assert!(!snapshot.is_finished());
    release.notify_one();

    assert_eq!(snapshot.await.unwrap().unwrap(), (1, 1));
    queued.await.unwrap().unwrap();
    handle.shutdown().await.unwrap();
    worker_join.await.unwrap().unwrap();
}

#[tokio::test]
async fn snapshots_are_served_between_wake_again_steps() {
    let cache = Cache::new();
    let task_id = component_id("task.snapshot_chain");
    let done = cache
        .register_value(
            descriptor("task.snapshot_chain.done", task_id.owner(), CellKind::Value),
            Some(false),
        )
        .unwrap();
    let first_step = Arc::new(Notify::new());
    let (worker, handle) = RuntimeWorker::new(cache);
    let worker_join = tokio::spawn(worker.run());
    handle
        .install_task(WakeAgainChainTask {
            descriptor: TaskDescriptor::new(task_id.clone(), Vec::new()),
            remaining_wakes: 20_000,
            done: done.clone(),
            first_step: Some(first_step.clone()),
        })
        .await
        .unwrap();
    let queued = {
        let handle = handle.clone();
        tokio::spawn(async move { handle.queue_task(&task_id).await })
    };
    first_step.notified().await;
    let observed_done = handle
        .snapshot(move |view| Ok(view.read_value(&done)?.unwrap()))
        .await
        .unwrap();
    assert!(
        !observed_done,
        "snapshot should run before the chain converges"
    );

    queued.await.unwrap().unwrap();
    handle.shutdown().await.unwrap();
    worker_join.await.unwrap().unwrap();
}

#[tokio::test]
async fn snapshot_panic_is_reported_without_stopping_the_runtime() {
    let cache = Cache::new();
    let owner = CellOwner::new("session").unwrap();
    let value = cache
        .register_value(
            descriptor("session.snapshot_value", owner, CellKind::Value),
            Some(7_i32),
        )
        .unwrap();
    let (worker, handle) = RuntimeWorker::new(cache);
    let worker_join = tokio::spawn(worker.run());

    let error = handle
        .snapshot::<(), _>(|_| panic!("forced snapshot panic"))
        .await
        .unwrap_err();
    assert_eq!(error, runtime::RuntimeError::SnapshotPanicked);
    let read = handle
        .snapshot(move |view| Ok(view.read_value(&value)?.unwrap()))
        .await
        .unwrap();
    assert_eq!(read, 7);
    assert_eq!(handle.snapshot_metrics().panicked, 1);

    handle.shutdown().await.unwrap();
    worker_join.await.unwrap().unwrap();
}

#[tokio::test]
async fn snapshot_requests_are_rejected_after_shutdown() {
    let cache = Cache::new();
    let (worker, handle) = RuntimeWorker::new(cache);
    let worker_join = tokio::spawn(worker.run());
    handle.shutdown().await.unwrap();
    worker_join.await.unwrap().unwrap();

    let error = handle.snapshot(|_| Ok(())).await.unwrap_err();
    assert_eq!(error, runtime::RuntimeError::SnapshotIngressClosed);
    assert_eq!(handle.snapshot_metrics().rejected, 1);
}

#[tokio::test]
async fn failed_schema_reconciliation_publishes_none_of_its_staged_cells() {
    let cache = Cache::new();
    let owner = CellOwner::new("projection.atomic").unwrap();
    let first_key = key("projection.atomic.first");
    let (worker, handle) = RuntimeWorker::new(cache);
    let worker_join = tokio::spawn(worker.run());

    let error = handle
        .reconcile_tasks::<(), _>(Vec::new(), move |schema| {
            schema.register_value::<i32>(
                descriptor(first_key.as_str(), owner.clone(), CellKind::Value),
                Some(1),
            )?;
            schema.register_value::<i32>(
                descriptor("projection.atomic.invalid", owner, CellKind::Array),
                Some(2),
            )?;
            Ok(((), Vec::new()))
        })
        .await
        .unwrap_err();
    assert!(matches!(error, runtime::RuntimeError::Cache(_)));
    assert_eq!(
        handle
            .cache()
            .describe(&key("projection.atomic.first"))
            .unwrap_err(),
        cache::CacheError::MissingCell(key("projection.atomic.first"))
    );

    handle.shutdown().await.unwrap();
    worker_join.await.unwrap().unwrap();
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn snapshot_queued_during_schema_reconciliation_observes_complete_new_schema() {
    let cache = Cache::new();
    let owner = CellOwner::new("projection.atomic").unwrap();
    let first_key = key("projection.atomic.first");
    let second_key = key("projection.atomic.second");
    let (worker, handle) = RuntimeWorker::new(cache);
    let worker_join = tokio::spawn(worker.run());
    let (entered_tx, entered_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel();

    let reconciliation = {
        let handle = handle.clone();
        let owner = owner.clone();
        let first_key = first_key.clone();
        let second_key = second_key.clone();
        tokio::spawn(async move {
            handle
                .reconcile_tasks::<(), _>(Vec::new(), move |schema| {
                    schema.register_value::<i32>(
                        descriptor(first_key.as_str(), owner.clone(), CellKind::Value),
                        Some(1),
                    )?;
                    entered_tx.send(()).unwrap();
                    release_rx.recv().unwrap();
                    schema.register_value::<i32>(
                        descriptor(second_key.as_str(), owner, CellKind::Value),
                        Some(2),
                    )?;
                    Ok(((), Vec::new()))
                })
                .await
        })
    };
    tokio::task::spawn_blocking(move || entered_rx.recv().unwrap())
        .await
        .unwrap();
    let snapshot = {
        let handle = handle.clone();
        tokio::spawn(async move {
            handle
                .snapshot(move |view| {
                    Ok((
                        view.describe(&first_key).is_ok(),
                        view.describe(&second_key).is_ok(),
                    ))
                })
                .await
        })
    };
    tokio::task::yield_now().await;
    release_tx.send(()).unwrap();

    reconciliation.await.unwrap().unwrap();
    assert_eq!(snapshot.await.unwrap().unwrap(), (true, true));
    handle.shutdown().await.unwrap();
    worker_join.await.unwrap().unwrap();
}
