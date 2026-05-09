use async_trait::async_trait;
use cache::{ArrayKey, Cache, CacheError, CellDescriptor, CellKind, CellOwner, Key, ValueKey};
use runtime::{
    ComponentError, ComponentId, Runtime, RuntimeError, RuntimeTask, TaskContext, TaskDescriptor,
};

#[derive(Clone, Debug, PartialEq, Eq)]
struct SourceBatch {
    seq: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct TaskState {
    processed: usize,
}

fn key(value: &str) -> Key {
    Key::new(value).unwrap()
}

fn task_id() -> ComponentId {
    ComponentId::new("task.candles_1m").unwrap()
}

#[test]
fn component_id_accepts_key_like_identifier() {
    let id = task_id();

    assert_eq!(id.as_str(), "task.candles_1m");
    assert_eq!(id.owner(), CellOwner::new("task.candles_1m").unwrap());
}

#[test]
fn component_id_rejects_invalid_identifier() {
    assert!(matches!(
        ComponentId::new("Task.Candles"),
        Err(ComponentError::InvalidComponentId(value)) if value == "Task.Candles"
    ));
}

#[test]
fn task_descriptor_holds_id_and_dependencies() {
    let descriptor = TaskDescriptor::new(task_id(), vec![key("feed.databento.es_mbo.batches")]);

    assert_eq!(descriptor.component.id.as_str(), "task.candles_1m");
    assert_eq!(
        descriptor.dependencies,
        vec![key("feed.databento.es_mbo.batches")]
    );
}

struct CountingTask {
    descriptor: TaskDescriptor,
    batches: ArrayKey<SourceBatch>,
    state: ValueKey<TaskState>,
}

#[async_trait]
impl RuntimeTask for CountingTask {
    fn descriptor(&self) -> &TaskDescriptor {
        &self.descriptor
    }

    async fn run_once(&mut self, ctx: TaskContext<'_>) -> Result<(), ComponentError> {
        let batches = ctx.read_array(&self.batches)?;
        let mut batch = ctx.batch();
        batch.set_value(
            &self.state,
            TaskState {
                processed: batches.len(),
            },
        );
        ctx.submit(batch).await?;
        Ok(())
    }
}

#[tokio::test]
async fn task_trait_object_runs_against_context() {
    let cache = Cache::new();
    let feed = CellOwner::new("feed.databento.es_mbo").unwrap();
    let task_id = task_id();
    let batches = cache
        .register_array(
            CellDescriptor {
                key: key("feed.databento.es_mbo.batches"),
                owner: feed,
                kind: CellKind::Array,
                public_read: false,
            },
            vec![SourceBatch { seq: 1 }, SourceBatch { seq: 2 }],
        )
        .unwrap();
    let state = cache
        .register_value::<TaskState>(
            CellDescriptor {
                key: key("task.candles_1m.state"),
                owner: task_id.owner(),
                kind: CellKind::Value,
                public_read: false,
            },
            None,
        )
        .unwrap();
    let descriptor =
        TaskDescriptor::new(task_id.clone(), vec![key("feed.databento.es_mbo.batches")]);
    let mut runtime = Runtime::new(cache.clone());
    runtime
        .register_task(CountingTask {
            descriptor,
            batches,
            state: state.clone(),
        })
        .await
        .unwrap();
    runtime.queue_task(&task_id).unwrap();

    let step = runtime.run_once().await.unwrap();

    assert_eq!(step.task_run, Some(task_id));
    assert_eq!(
        cache.read_value(&state).unwrap(),
        Some(TaskState { processed: 2 })
    );
    assert_eq!(step.changed_keys, vec![key("task.candles_1m.state")]);
}

struct WrongOwnerTask {
    descriptor: TaskDescriptor,
    status: ValueKey<TaskState>,
}

#[async_trait]
impl RuntimeTask for WrongOwnerTask {
    fn descriptor(&self) -> &TaskDescriptor {
        &self.descriptor
    }

    async fn run_once(&mut self, ctx: TaskContext<'_>) -> Result<(), ComponentError> {
        let mut batch = ctx.batch();
        batch.set_value(&self.status, TaskState { processed: 1 });
        ctx.submit(batch).await
    }
}

#[tokio::test]
async fn task_context_rejects_writes_to_cells_owned_by_another_owner() {
    let cache = Cache::new();
    let feed = CellOwner::new("feed.databento.es_mbo").unwrap();
    let status = cache
        .register_value::<TaskState>(
            CellDescriptor {
                key: key("feed.databento.es_mbo.status"),
                owner: feed.clone(),
                kind: CellKind::Value,
                public_read: false,
            },
            None,
        )
        .unwrap();
    let task_id = task_id();
    let mut runtime = Runtime::new(cache);
    runtime
        .register_task(WrongOwnerTask {
            descriptor: TaskDescriptor::new(task_id.clone(), Vec::new()),
            status,
        })
        .await
        .unwrap();
    runtime.queue_task(&task_id).unwrap();

    let err = runtime.run_once().await.unwrap_err();

    assert_eq!(
        err,
        RuntimeError::Component {
            id: task_id,
            source: ComponentError::Cache(CacheError::OwnerMismatch {
                key: key("feed.databento.es_mbo.status"),
                writer: CellOwner::new("task.candles_1m").unwrap(),
                owner: feed,
            }),
        }
    );
}
