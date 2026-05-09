use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use cache::{Cache, CacheError, CellDescriptor, CellKind, CellOwner, Key, ValueKey};
use runtime::{
    ComponentError, ComponentId, ExternalWriteBatch, Runtime, RuntimeError, RuntimeTask,
    TaskContext, TaskDescriptor, TaskWake,
};

fn key(value: &str) -> Key {
    Key::new(value).unwrap()
}

fn task_id(value: &str) -> ComponentId {
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

fn feed_owner() -> CellOwner {
    CellOwner::new("feed.databento.es_mbo").unwrap()
}

fn source_cell(cache: &Cache) -> ValueKey<i32> {
    cache
        .register_value(
            descriptor("feed.databento.es_mbo.value", feed_owner(), CellKind::Value),
            None,
        )
        .unwrap()
}

fn task_cell(cache: &Cache, key_path: &str, id: &ComponentId) -> ValueKey<i32> {
    cache
        .register_value(descriptor(key_path, id.owner(), CellKind::Value), None)
        .unwrap()
}

fn external_set(owner: CellOwner, key: &ValueKey<i32>, value: i32) -> ExternalWriteBatch {
    let mut batch = ExternalWriteBatch::new(owner);
    batch.set_value(key, value);
    batch
}

fn log_snapshot(log: &Arc<Mutex<Vec<String>>>) -> Vec<String> {
    log.lock().unwrap().clone()
}

struct CopyTask {
    descriptor: TaskDescriptor,
    input: ValueKey<i32>,
    output: ValueKey<i32>,
    output_offset: i32,
    log_name: &'static str,
    log: Arc<Mutex<Vec<String>>>,
    fail_after_write: bool,
}

impl CopyTask {
    fn new(
        id: ComponentId,
        dependency: Key,
        input: ValueKey<i32>,
        output: ValueKey<i32>,
        log_name: &'static str,
        log: Arc<Mutex<Vec<String>>>,
    ) -> Self {
        Self {
            descriptor: TaskDescriptor::new(id, vec![dependency]),
            input,
            output,
            output_offset: 0,
            log_name,
            log,
            fail_after_write: false,
        }
    }

    fn with_offset(mut self, output_offset: i32) -> Self {
        self.output_offset = output_offset;
        self
    }

    fn fail_after_write(mut self) -> Self {
        self.fail_after_write = true;
        self
    }
}

#[async_trait]
impl RuntimeTask for CopyTask {
    fn descriptor(&self) -> &TaskDescriptor {
        &self.descriptor
    }

    async fn run_once(&mut self, ctx: TaskContext<'_>) -> Result<(), ComponentError> {
        self.log.lock().unwrap().push(self.log_name.to_string());
        if let Some(value) = ctx.read_value(&self.input)? {
            let mut batch = ctx.batch();
            batch.set_value(&self.output, value + self.output_offset);
            ctx.submit(batch).await?;
        }
        if self.fail_after_write {
            return Err(ComponentError::Message("forced_failure".to_string()));
        }
        Ok(())
    }
}

struct FailingTask {
    descriptor: TaskDescriptor,
    log: Arc<Mutex<Vec<String>>>,
}

#[async_trait]
impl RuntimeTask for FailingTask {
    fn descriptor(&self) -> &TaskDescriptor {
        &self.descriptor
    }

    async fn run_once(&mut self, _ctx: TaskContext<'_>) -> Result<(), ComponentError> {
        self.log.lock().unwrap().push("failing".to_string());
        Err(ComponentError::Message("forced_failure".to_string()))
    }
}

struct IncrementSelfTask {
    descriptor: TaskDescriptor,
    cell: ValueKey<i32>,
}

#[async_trait]
impl RuntimeTask for IncrementSelfTask {
    fn descriptor(&self) -> &TaskDescriptor {
        &self.descriptor
    }

    async fn run_once(&mut self, ctx: TaskContext<'_>) -> Result<(), ComponentError> {
        let next = ctx.read_value(&self.cell)?.unwrap_or(0) + 1;
        let mut batch = ctx.batch();
        batch.set_value(&self.cell, next);
        ctx.submit(batch).await?;
        Ok(())
    }
}

struct WakeLoggingTask {
    descriptor: TaskDescriptor,
    cell: Option<ValueKey<i32>>,
    log: Arc<Mutex<Vec<TaskWake>>>,
}

#[async_trait]
impl RuntimeTask for WakeLoggingTask {
    fn descriptor(&self) -> &TaskDescriptor {
        &self.descriptor
    }

    async fn run_once(&mut self, ctx: TaskContext<'_>) -> Result<(), ComponentError> {
        self.log.lock().unwrap().push(ctx.wake());
        if let Some(cell) = &self.cell {
            let next = ctx.read_value(cell)?.unwrap_or(0) + 1;
            let mut batch = ctx.batch();
            batch.set_value(cell, next);
            ctx.submit(batch).await?;
        }
        Ok(())
    }
}

#[tokio::test]
async fn external_write_changed_key_queues_and_runs_dependent_task() {
    let cache = Cache::new();
    let source = source_cell(&cache);
    let task_id = task_id("task.copy");
    let output = task_cell(&cache, "task.copy.output", &task_id);
    let log = Arc::new(Mutex::new(Vec::new()));
    let mut runtime = Runtime::new(cache.clone());
    runtime
        .register_task(CopyTask::new(
            task_id.clone(),
            source.key().clone(),
            source.clone(),
            output.clone(),
            "copy",
            log.clone(),
        ))
        .await
        .unwrap();

    runtime.submit_external_writes(external_set(feed_owner(), &source, 7));
    let step = runtime.run_once().await.unwrap();

    assert_eq!(step.task_run, Some(task_id.clone()));
    assert_eq!(step.scheduled_tasks, vec![task_id]);
    assert_eq!(cache.read_value(&output).unwrap(), Some(7));
    assert_eq!(log_snapshot(&log), vec!["copy"]);
}

#[tokio::test]
async fn duplicate_dependency_changes_run_task_once() {
    let cache = Cache::new();
    let source = source_cell(&cache);
    let task_id = task_id("task.copy");
    let output = task_cell(&cache, "task.copy.output", &task_id);
    let log = Arc::new(Mutex::new(Vec::new()));
    let mut runtime = Runtime::new(cache.clone());
    runtime
        .register_task(CopyTask::new(
            task_id,
            source.key().clone(),
            source.clone(),
            output,
            "copy",
            log.clone(),
        ))
        .await
        .unwrap();
    let mut batch = ExternalWriteBatch::new(feed_owner());
    batch.set_value(&source, 1).set_value(&source, 2);

    runtime.submit_external_writes(batch);
    runtime.run_until_idle(10).await.unwrap();

    assert_eq!(log_snapshot(&log), vec!["copy"]);
}

#[tokio::test]
async fn task_write_queues_downstream_task() {
    let cache = Cache::new();
    let source = source_cell(&cache);
    let first_id = task_id("task.first");
    let second_id = task_id("task.second");
    let first_output = task_cell(&cache, "task.first.output", &first_id);
    let second_output = task_cell(&cache, "task.second.output", &second_id);
    let log = Arc::new(Mutex::new(Vec::new()));
    let mut runtime = Runtime::new(cache.clone());
    runtime
        .register_task(
            CopyTask::new(
                first_id.clone(),
                source.key().clone(),
                source.clone(),
                first_output.clone(),
                "first",
                log.clone(),
            )
            .with_offset(10),
        )
        .await
        .unwrap();
    runtime
        .register_task(
            CopyTask::new(
                second_id.clone(),
                first_output.key().clone(),
                first_output.clone(),
                second_output.clone(),
                "second",
                log.clone(),
            )
            .with_offset(100),
        )
        .await
        .unwrap();

    runtime.submit_external_writes(external_set(feed_owner(), &source, 1));
    let first_step = runtime.run_once().await.unwrap();
    let second_step = runtime.run_once().await.unwrap();

    assert_eq!(first_step.task_run, Some(first_id));
    assert_eq!(
        first_step.scheduled_tasks,
        vec![task_id("task.first"), second_id.clone()]
    );
    assert_eq!(second_step.task_run, Some(second_id));
    assert_eq!(cache.read_value(&second_output).unwrap(), Some(111));
    assert_eq!(log_snapshot(&log), vec!["first", "second"]);
}

#[tokio::test]
async fn downstream_tasks_run_in_registration_order() {
    let cache = Cache::new();
    let source = source_cell(&cache);
    let first_id = task_id("task.first");
    let second_id = task_id("task.second");
    let first_output = task_cell(&cache, "task.first.output", &first_id);
    let second_output = task_cell(&cache, "task.second.output", &second_id);
    let log = Arc::new(Mutex::new(Vec::new()));
    let mut runtime = Runtime::new(cache.clone());
    runtime
        .register_task(CopyTask::new(
            first_id,
            source.key().clone(),
            source.clone(),
            first_output,
            "first",
            log.clone(),
        ))
        .await
        .unwrap();
    runtime
        .register_task(CopyTask::new(
            second_id,
            source.key().clone(),
            source.clone(),
            second_output,
            "second",
            log.clone(),
        ))
        .await
        .unwrap();

    runtime.submit_external_writes(external_set(feed_owner(), &source, 1));
    runtime.run_until_idle(10).await.unwrap();

    assert_eq!(log_snapshot(&log), vec!["first", "second"]);
}

#[tokio::test]
async fn external_writes_are_applied_before_task_runs() {
    let cache = Cache::new();
    let source = source_cell(&cache);
    let task_id = task_id("task.copy");
    let output = task_cell(&cache, "task.copy.output", &task_id);
    let log = Arc::new(Mutex::new(Vec::new()));
    let mut runtime = Runtime::new(cache.clone());
    runtime
        .register_task(CopyTask::new(
            task_id.clone(),
            source.key().clone(),
            source.clone(),
            output.clone(),
            "copy",
            log,
        ))
        .await
        .unwrap();
    runtime.queue_task(&task_id).unwrap();
    runtime.submit_external_writes(external_set(feed_owner(), &source, 42));

    runtime.run_once().await.unwrap();

    assert_eq!(cache.read_value(&output).unwrap(), Some(42));
}

#[tokio::test]
async fn external_write_batches_are_fifo() {
    let cache = Cache::new();
    let source = source_cell(&cache);
    let mut runtime = Runtime::new(cache.clone());

    runtime.submit_external_writes(external_set(feed_owner(), &source, 1));
    runtime.submit_external_writes(external_set(feed_owner(), &source, 2));
    let step = runtime.run_once().await.unwrap();

    assert_eq!(step.external_write_batches_applied, 2);
    assert_eq!(cache.read_value(&source).unwrap(), Some(2));
}

#[tokio::test]
async fn one_run_once_runs_at_most_one_task() {
    let cache = Cache::new();
    let source = source_cell(&cache);
    let first_id = task_id("task.first");
    let second_id = task_id("task.second");
    let first_output = task_cell(&cache, "task.first.output", &first_id);
    let second_output = task_cell(&cache, "task.second.output", &second_id);
    let log = Arc::new(Mutex::new(Vec::new()));
    let mut runtime = Runtime::new(cache);
    runtime
        .register_task(CopyTask::new(
            first_id.clone(),
            source.key().clone(),
            source.clone(),
            first_output,
            "first",
            log.clone(),
        ))
        .await
        .unwrap();
    runtime
        .register_task(CopyTask::new(
            second_id.clone(),
            source.key().clone(),
            source,
            second_output,
            "second",
            log.clone(),
        ))
        .await
        .unwrap();
    runtime.queue_task(&first_id).unwrap();
    runtime.queue_task(&second_id).unwrap();

    let step = runtime.run_once().await.unwrap();

    assert_eq!(step.task_run, Some(first_id));
    assert!(!step.idle_after);
    assert_eq!(log_snapshot(&log), vec!["first"]);
}

#[tokio::test]
async fn queued_task_receives_manual_wake_reason() {
    let cache = Cache::new();
    let task_id = task_id("task.wake_manual");
    let log = Arc::new(Mutex::new(Vec::new()));
    let mut runtime = Runtime::new(cache);
    runtime
        .register_task(WakeLoggingTask {
            descriptor: TaskDescriptor::new(task_id.clone(), Vec::new()),
            cell: None,
            log: log.clone(),
        })
        .await
        .unwrap();

    runtime.queue_task(&task_id).unwrap();
    runtime.run_once().await.unwrap();

    assert_eq!(log.lock().unwrap().as_slice(), &[TaskWake::Manual]);
}

#[tokio::test]
async fn dependency_scheduled_task_receives_dependency_changed_wake_reason() {
    let cache = Cache::new();
    let source = source_cell(&cache);
    let task_id = task_id("task.wake_dependency");
    let log = Arc::new(Mutex::new(Vec::new()));
    let mut runtime = Runtime::new(cache);
    runtime
        .register_task(WakeLoggingTask {
            descriptor: TaskDescriptor::new(task_id, vec![source.key().clone()]),
            cell: None,
            log: log.clone(),
        })
        .await
        .unwrap();

    runtime.submit_external_writes(external_set(feed_owner(), &source, 1));
    runtime.run_until_idle(10).await.unwrap();

    assert_eq!(
        log.lock().unwrap().as_slice(),
        &[TaskWake::DependencyChanged]
    );
}

#[tokio::test]
async fn running_task_that_changes_its_dependency_receives_rerun_wake_reason() {
    let cache = Cache::new();
    let task_id = task_id("task.wake_rerun");
    let output = task_cell(&cache, "task.wake_rerun.output", &task_id);
    let log = Arc::new(Mutex::new(Vec::new()));
    let mut runtime = Runtime::new(cache);
    runtime
        .register_task(WakeLoggingTask {
            descriptor: TaskDescriptor::new(task_id.clone(), vec![output.key().clone()]),
            cell: Some(output),
            log: log.clone(),
        })
        .await
        .unwrap();

    runtime.queue_task(&task_id).unwrap();
    runtime.run_once().await.unwrap();
    runtime.run_once().await.unwrap();

    assert_eq!(
        log.lock().unwrap().as_slice(),
        &[TaskWake::Manual, TaskWake::Rerun]
    );
}

#[tokio::test]
async fn run_once_returns_idle_when_no_work_exists() {
    let cache = Cache::new();
    let mut runtime = Runtime::new(cache);

    let step = runtime.run_once().await.unwrap();

    assert_eq!(step.external_write_batches_applied, 0);
    assert_eq!(step.task_run, None);
    assert!(step.changed_keys.is_empty());
    assert!(step.scheduled_tasks.is_empty());
    assert!(step.idle_after);
}

#[tokio::test]
async fn run_until_idle_drains_external_writes_and_tasks() {
    let cache = Cache::new();
    let source = source_cell(&cache);
    let task_id = task_id("task.copy");
    let output = task_cell(&cache, "task.copy.output", &task_id);
    let log = Arc::new(Mutex::new(Vec::new()));
    let mut runtime = Runtime::new(cache.clone());
    runtime
        .register_task(CopyTask::new(
            task_id,
            source.key().clone(),
            source.clone(),
            output.clone(),
            "copy",
            log,
        ))
        .await
        .unwrap();

    runtime.submit_external_writes(external_set(feed_owner(), &source, 9));
    let stats = runtime.run_until_idle(10).await.unwrap();

    assert_eq!(stats.steps, 1);
    assert_eq!(stats.external_write_batches_applied, 1);
    assert_eq!(stats.tasks_run, 1);
    assert_eq!(
        stats.changed_keys,
        vec![key("feed.databento.es_mbo.value"), key("task.copy.output")]
    );
    assert_eq!(cache.read_value(&output).unwrap(), Some(9));
}

#[tokio::test]
async fn run_until_idle_returns_limit_exceeded_for_cycle() {
    let cache = Cache::new();
    let task_id = task_id("task.loop");
    let output = task_cell(&cache, "task.loop.output", &task_id);
    let descriptor = TaskDescriptor::new(task_id.clone(), vec![output.key().clone()]);
    let mut runtime = Runtime::new(cache);
    runtime
        .register_task(IncrementSelfTask {
            descriptor,
            cell: output,
        })
        .await
        .unwrap();
    runtime.queue_task(&task_id).unwrap();

    let err = runtime.run_until_idle(1).await.unwrap_err();

    assert_eq!(err, RuntimeError::RunLimitExceeded { max_steps: 1 });
}

#[tokio::test]
async fn task_errors_are_wrapped_with_component_id() {
    let cache = Cache::new();
    let task_id = task_id("task.failing");
    let log = Arc::new(Mutex::new(Vec::new()));
    let mut runtime = Runtime::new(cache);
    runtime
        .register_task(FailingTask {
            descriptor: TaskDescriptor::new(task_id.clone(), Vec::new()),
            log,
        })
        .await
        .unwrap();
    runtime.queue_task(&task_id).unwrap();

    let err = runtime.run_once().await.unwrap_err();

    assert_eq!(
        err,
        RuntimeError::Component {
            id: task_id,
            source: ComponentError::Message("forced_failure".to_string()),
        }
    );
}

#[tokio::test]
async fn task_committed_writes_schedule_dependents_before_error_returns() {
    let cache = Cache::new();
    let source = source_cell(&cache);
    let failing_id = task_id("task.failing");
    let downstream_id = task_id("task.downstream");
    let failing_output = task_cell(&cache, "task.failing.output", &failing_id);
    let downstream_output = task_cell(&cache, "task.downstream.output", &downstream_id);
    let log = Arc::new(Mutex::new(Vec::new()));
    let mut runtime = Runtime::new(cache.clone());
    runtime
        .register_task(
            CopyTask::new(
                failing_id.clone(),
                source.key().clone(),
                source.clone(),
                failing_output.clone(),
                "failing",
                log.clone(),
            )
            .fail_after_write(),
        )
        .await
        .unwrap();
    runtime
        .register_task(CopyTask::new(
            downstream_id.clone(),
            failing_output.key().clone(),
            failing_output,
            downstream_output.clone(),
            "downstream",
            log.clone(),
        ))
        .await
        .unwrap();
    runtime.submit_external_writes(external_set(feed_owner(), &source, 3));

    let err = runtime.run_once().await.unwrap_err();
    assert!(matches!(err, RuntimeError::Component { .. }));
    let step = runtime.run_once().await.unwrap();

    assert_eq!(step.task_run, Some(downstream_id));
    assert_eq!(cache.read_value(&downstream_output).unwrap(), Some(3));
    assert_eq!(log_snapshot(&log), vec!["failing", "downstream"]);
}

#[tokio::test]
async fn external_write_committed_writes_schedule_dependents_before_error_returns() {
    let cache = Cache::new();
    let source = source_cell(&cache);
    let wrong_owner_cell = cache
        .register_value(
            descriptor(
                "task.wrong.output",
                CellOwner::new("task.wrong").unwrap(),
                CellKind::Value,
            ),
            None::<i32>,
        )
        .unwrap();
    let task_id = task_id("task.copy");
    let output = task_cell(&cache, "task.copy.output", &task_id);
    let log = Arc::new(Mutex::new(Vec::new()));
    let mut runtime = Runtime::new(cache.clone());
    runtime
        .register_task(CopyTask::new(
            task_id.clone(),
            source.key().clone(),
            source.clone(),
            output.clone(),
            "copy",
            log,
        ))
        .await
        .unwrap();
    let mut failing_batch = ExternalWriteBatch::new(feed_owner());
    failing_batch
        .set_value(&source, 1)
        .set_value(&wrong_owner_cell, 100);
    runtime.submit_external_writes(failing_batch);
    runtime.submit_external_writes(external_set(feed_owner(), &source, 2));

    let err = runtime.run_once().await.unwrap_err();
    assert!(matches!(
        err,
        RuntimeError::Cache(CacheError::OwnerMismatch { .. })
    ));
    let step = runtime.run_once().await.unwrap();

    assert_eq!(step.external_write_batches_applied, 1);
    assert_eq!(step.task_run, Some(task_id));
    assert_eq!(cache.read_value(&output).unwrap(), Some(2));
}

#[tokio::test]
async fn local_scheduling_allows_running_task_to_requeue_itself() {
    let cache = Cache::new();
    let task_id = task_id("task.loop");
    let output = task_cell(&cache, "task.loop.output", &task_id);
    let descriptor = TaskDescriptor::new(task_id.clone(), vec![output.key().clone()]);
    let mut runtime = Runtime::new(cache.clone());
    runtime
        .register_task(IncrementSelfTask {
            descriptor,
            cell: output.clone(),
        })
        .await
        .unwrap();
    runtime.queue_task(&task_id).unwrap();

    let step = runtime.run_once().await.unwrap();

    assert_eq!(step.task_run, Some(task_id.clone()));
    assert_eq!(step.scheduled_tasks, vec![task_id]);
    assert!(!step.idle_after);
    assert_eq!(cache.read_value(&output).unwrap(), Some(1));
}

#[tokio::test]
async fn direct_cache_writes_do_not_schedule_tasks() {
    let cache = Cache::new();
    let source = source_cell(&cache);
    let task_id = task_id("task.copy");
    let output = task_cell(&cache, "task.copy.output", &task_id);
    let log = Arc::new(Mutex::new(Vec::new()));
    let mut runtime = Runtime::new(cache.clone());
    runtime
        .register_task(CopyTask::new(
            task_id,
            source.key().clone(),
            source.clone(),
            output.clone(),
            "copy",
            log.clone(),
        ))
        .await
        .unwrap();

    cache
        .set_value(&feed_owner(), &source, 5)
        .expect("direct setup write should succeed");
    let stats = runtime.run_until_idle(10).await.unwrap();

    assert_eq!(stats.steps, 0);
    assert_eq!(cache.read_value(&output).unwrap(), None);
    assert!(log_snapshot(&log).is_empty());
}

#[tokio::test]
async fn duplicate_task_registration_is_rejected() {
    let cache = Cache::new();
    let source = source_cell(&cache);
    let task_id = task_id("task.copy");
    let first_output = task_cell(&cache, "task.copy.first", &task_id);
    let log = Arc::new(Mutex::new(Vec::new()));
    let mut runtime = Runtime::new(cache.clone());
    runtime
        .register_task(CopyTask::new(
            task_id.clone(),
            source.key().clone(),
            source.clone(),
            first_output,
            "copy",
            log.clone(),
        ))
        .await
        .unwrap();

    let duplicate = CopyTask::new(
        task_id.clone(),
        source.key().clone(),
        source,
        task_cell(&cache, "task.copy.second", &task_id),
        "copy2",
        log,
    );
    let err = runtime.register_task(duplicate).await.unwrap_err();

    assert_eq!(err, RuntimeError::DuplicateComponent(task_id));
}

#[test]
fn queue_task_rejects_unknown_component_id() {
    let cache = Cache::new();
    let mut runtime = Runtime::new(cache);
    let unknown = task_id("task.unknown");

    let err = runtime.queue_task(&unknown).unwrap_err();

    assert_eq!(err, RuntimeError::MissingComponent(unknown));
}

#[tokio::test]
async fn duplicate_dependencies_in_one_descriptor_are_indexed_once() {
    let cache = Cache::new();
    let source = source_cell(&cache);
    let task_id = task_id("task.copy");
    let output = task_cell(&cache, "task.copy.output", &task_id);
    let log = Arc::new(Mutex::new(Vec::new()));
    let descriptor = TaskDescriptor::new(task_id, vec![source.key().clone(), source.key().clone()]);
    let mut task = CopyTask::new(
        descriptor.component.id.clone(),
        source.key().clone(),
        source.clone(),
        output,
        "copy",
        log.clone(),
    );
    task.descriptor = descriptor;
    let mut runtime = Runtime::new(cache);
    runtime.register_task(task).await.unwrap();

    runtime.submit_external_writes(external_set(feed_owner(), &source, 1));
    runtime.run_until_idle(10).await.unwrap();

    assert_eq!(log_snapshot(&log), vec!["copy"]);
}

#[tokio::test]
async fn empty_dependencies_are_allowed() {
    let cache = Cache::new();
    let task_id = task_id("task.failing");
    let log = Arc::new(Mutex::new(Vec::new()));
    let mut runtime = Runtime::new(cache);
    runtime
        .register_task(FailingTask {
            descriptor: TaskDescriptor::new(task_id.clone(), Vec::new()),
            log: log.clone(),
        })
        .await
        .unwrap();

    runtime.queue_task(&task_id).unwrap();
    let err = runtime.run_once().await.unwrap_err();

    assert!(matches!(err, RuntimeError::Component { .. }));
    assert_eq!(log_snapshot(&log), vec!["failing"]);
}
