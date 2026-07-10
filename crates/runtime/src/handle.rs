use std::{any::Any, time::Instant};

use cache::{CacheReadView, CacheReader};
use tokio::sync::{mpsc, oneshot};

use crate::{
    snapshot::{SnapshotMetrics, SnapshotRequest},
    worker::RuntimeCommand,
    ComponentId, ComponentKind, ComponentStatus, ExternalWriteBatch, ExternalWriteSink, RunStats,
    RuntimeError, RuntimeProcess, RuntimeTask, SnapshotMetricsSnapshot,
};

#[derive(Clone)]
pub struct RuntimeHandle {
    cache: CacheReader,
    writes: ExternalWriteSink,
    commands: mpsc::Sender<RuntimeCommand>,
    snapshots: mpsc::Sender<SnapshotRequest>,
    snapshot_metrics: SnapshotMetrics,
}

impl RuntimeHandle {
    pub(crate) fn new(
        cache: CacheReader,
        writes: ExternalWriteSink,
        commands: mpsc::Sender<RuntimeCommand>,
        snapshots: mpsc::Sender<SnapshotRequest>,
        snapshot_metrics: SnapshotMetrics,
    ) -> Self {
        Self {
            cache,
            writes,
            commands,
            snapshots,
            snapshot_metrics,
        }
    }

    pub fn cache(&self) -> &CacheReader {
        &self.cache
    }

    pub async fn snapshot<R, F>(&self, read: F) -> Result<R, RuntimeError>
    where
        R: Send + 'static,
        F: FnOnce(&CacheReadView<'_>) -> Result<R, cache::CacheError> + Send + 'static,
    {
        self.snapshot_metrics.requested();
        let (reply, rx) = oneshot::channel();
        let read = Box::new(move |view: &CacheReadView<'_>| {
            read(view).map(|value| Box::new(value) as Box<dyn Any + Send>)
        });
        let request = SnapshotRequest {
            queued_at: Instant::now(),
            read,
            reply,
        };
        if self.snapshots.send(request).await.is_err() {
            self.snapshot_metrics.rejected();
            return Err(RuntimeError::SnapshotIngressClosed);
        }
        let value = rx.await.map_err(|_| RuntimeError::RuntimeStopped)??;
        value
            .downcast::<R>()
            .map(|value| *value)
            .map_err(|_| RuntimeError::SnapshotTypeMismatch)
    }

    pub fn snapshot_metrics(&self) -> SnapshotMetricsSnapshot {
        self.snapshot_metrics.snapshot()
    }

    pub fn external_write_sink(&self) -> ExternalWriteSink {
        self.writes.clone()
    }

    pub async fn submit_external_writes(
        &self,
        batch: ExternalWriteBatch,
    ) -> Result<(), RuntimeError> {
        self.writes.submit(batch).await
    }

    pub async fn install_process<P>(&self, process: P) -> Result<ComponentHandle, RuntimeError>
    where
        P: RuntimeProcess,
    {
        self.install_boxed_process(Box::new(process)).await
    }

    pub async fn install_boxed_process(
        &self,
        process: Box<dyn RuntimeProcess>,
    ) -> Result<ComponentHandle, RuntimeError> {
        let (reply, rx) = oneshot::channel();
        self.commands
            .send(RuntimeCommand::InstallProcess { process, reply })
            .await
            .map_err(|_| RuntimeError::RuntimeCommandClosed)?;
        rx.await.map_err(|_| RuntimeError::RuntimeStopped)?
    }

    pub async fn install_task<T>(&self, task: T) -> Result<ComponentHandle, RuntimeError>
    where
        T: RuntimeTask,
    {
        self.install_boxed_task(Box::new(task)).await
    }

    pub async fn install_boxed_task(
        &self,
        task: Box<dyn RuntimeTask>,
    ) -> Result<ComponentHandle, RuntimeError> {
        let (reply, rx) = oneshot::channel();
        self.commands
            .send(RuntimeCommand::InstallTask { task, reply })
            .await
            .map_err(|_| RuntimeError::RuntimeCommandClosed)?;
        rx.await.map_err(|_| RuntimeError::RuntimeStopped)?
    }

    pub async fn queue_task(&self, id: &ComponentId) -> Result<bool, RuntimeError> {
        let (reply, rx) = oneshot::channel();
        self.commands
            .send(RuntimeCommand::QueueTask {
                id: id.clone(),
                reply,
            })
            .await
            .map_err(|_| RuntimeError::RuntimeCommandClosed)?;
        rx.await.map_err(|_| RuntimeError::RuntimeStopped)?
    }

    pub async fn stop_process(&self, id: &ComponentId) -> Result<bool, RuntimeError> {
        let (reply, rx) = oneshot::channel();
        self.commands
            .send(RuntimeCommand::StopProcess {
                id: id.clone(),
                reply,
            })
            .await
            .map_err(|_| RuntimeError::RuntimeCommandClosed)?;
        rx.await.map_err(|_| RuntimeError::RuntimeStopped)?
    }

    pub async fn component_status(
        &self,
        id: &ComponentId,
    ) -> Result<ComponentStatus, RuntimeError> {
        let (reply, rx) = oneshot::channel();
        self.commands
            .send(RuntimeCommand::ComponentStatus {
                id: id.clone(),
                reply,
            })
            .await
            .map_err(|_| RuntimeError::RuntimeCommandClosed)?;
        rx.await.map_err(|_| RuntimeError::RuntimeStopped)?
    }

    pub async fn list_components(
        &self,
    ) -> Result<Vec<(ComponentId, ComponentKind, ComponentStatus)>, RuntimeError> {
        let (reply, rx) = oneshot::channel();
        self.commands
            .send(RuntimeCommand::ListComponents { reply })
            .await
            .map_err(|_| RuntimeError::RuntimeCommandClosed)?;
        rx.await.map_err(|_| RuntimeError::RuntimeStopped)?
    }

    pub async fn drain(&self, max_steps: usize) -> Result<RunStats, RuntimeError> {
        let (reply, rx) = oneshot::channel();
        self.commands
            .send(RuntimeCommand::Drain { max_steps, reply })
            .await
            .map_err(|_| RuntimeError::RuntimeCommandClosed)?;
        rx.await.map_err(|_| RuntimeError::RuntimeStopped)?
    }

    pub async fn shutdown(&self) -> Result<(), RuntimeError> {
        let (reply, rx) = oneshot::channel();
        self.commands
            .send(RuntimeCommand::Shutdown { reply })
            .await
            .map_err(|_| RuntimeError::RuntimeCommandClosed)?;
        rx.await.map_err(|_| RuntimeError::RuntimeStopped)?
    }
}

#[derive(Clone)]
pub struct ComponentHandle {
    id: ComponentId,
    kind: ComponentKind,
    runtime: RuntimeHandle,
}

impl ComponentHandle {
    pub(crate) fn new(id: ComponentId, kind: ComponentKind, runtime: RuntimeHandle) -> Self {
        Self { id, kind, runtime }
    }

    pub fn id(&self) -> &ComponentId {
        &self.id
    }

    pub fn kind(&self) -> ComponentKind {
        self.kind
    }

    pub async fn status(&self) -> Result<ComponentStatus, RuntimeError> {
        self.runtime.component_status(&self.id).await
    }
}
