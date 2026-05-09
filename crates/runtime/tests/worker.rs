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
    RuntimeProcess, RuntimeTask, RuntimeWorker, TaskContext, TaskDescriptor,
};

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

    async fn run_once(&mut self, ctx: TaskContext<'_>) -> Result<(), ComponentError> {
        if let Some(value) = ctx.read_value(&self.input)? {
            self.log.lock().unwrap().push(value);
            let mut batch = ctx.batch();
            batch.set_value(&self.output, value);
            ctx.submit(batch).await?;
        }
        Ok(())
    }
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
