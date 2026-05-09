use std::collections::HashMap;

use cache::Cache;
use tokio::{
    sync::{mpsc, oneshot},
    task::{JoinError, JoinHandle},
};

use crate::{
    handle::{ComponentHandle, RuntimeHandle},
    process::{
        ensure_process_descriptor, ProcessContext, ProcessPrepareContext, ShutdownController,
    },
    ComponentError, ComponentId, ComponentKind, ComponentStatus, ExternalWriteReceiver,
    ExternalWriteSink, RunStats, Runtime, RuntimeError, RuntimeProcess, RuntimeTask,
};

const DEFAULT_EXTERNAL_WRITE_BUFFER: usize = 1024;
const DEFAULT_COMMAND_BUFFER: usize = 128;
const DEFAULT_EVENT_BUFFER: usize = 128;
const DEFAULT_RUN_STEP_BUDGET: usize = 1024;

pub struct RuntimeWorker {
    runtime: Runtime,
    handle: RuntimeHandle,
    commands: mpsc::Receiver<RuntimeCommand>,
    writes: ExternalWriteReceiver,
    events_tx: mpsc::Sender<RuntimeEvent>,
    events_rx: mpsc::Receiver<RuntimeEvent>,
    processes: HashMap<ComponentId, ProcessRecord>,
    run_step_budget: usize,
    shutdown_reply: Option<oneshot::Sender<Result<(), RuntimeError>>>,
}

impl RuntimeWorker {
    pub fn new(cache: Cache) -> (Self, RuntimeHandle) {
        let (writes, write_rx) = ExternalWriteSink::channel(DEFAULT_EXTERNAL_WRITE_BUFFER);
        let (commands_tx, commands_rx) = mpsc::channel(DEFAULT_COMMAND_BUFFER);
        let (events_tx, events_rx) = mpsc::channel(DEFAULT_EVENT_BUFFER);
        let handle = RuntimeHandle::new(cache.clone(), writes.clone(), commands_tx);
        let runtime = Runtime::new_with_prepare_writes(cache, Some(writes));
        let worker = Self {
            runtime,
            handle: handle.clone(),
            commands: commands_rx,
            writes: write_rx,
            events_tx,
            events_rx,
            processes: HashMap::new(),
            run_step_budget: DEFAULT_RUN_STEP_BUDGET,
            shutdown_reply: None,
        };
        (worker, handle)
    }

    pub async fn run(mut self) -> Result<(), RuntimeError> {
        loop {
            tokio::select! {
                Some(command) = self.commands.recv() => {
                    self.handle_command(command).await?;
                }
                Some(batch) = self.writes.recv() => {
                    self.runtime.submit_external_writes(batch);
                    self.runtime.run_until_idle(self.run_step_budget).await?;
                }
                Some(event) = self.events_rx.recv() => {
                    self.handle_event(event);
                }
                else => {
                    return Ok(());
                }
            }

            if self.finish_shutdown_if_ready().await? {
                return Ok(());
            }
        }
    }

    async fn handle_command(&mut self, command: RuntimeCommand) -> Result<(), RuntimeError> {
        match command {
            RuntimeCommand::InstallProcess { process, reply } => {
                let result = self.install_process(process);
                let _ = reply.send(result);
            }
            RuntimeCommand::InstallTask { task, reply } => {
                let result = self.install_task(task).await;
                let _ = reply.send(result);
            }
            RuntimeCommand::QueueTask { id, reply } => {
                let result = async {
                    let queued = self.runtime.queue_task(&id)?;
                    self.runtime.run_until_idle(self.run_step_budget).await?;
                    Ok(queued)
                }
                .await;
                let _ = reply.send(result);
            }
            RuntimeCommand::StopProcess { id, reply } => {
                let result = self.stop_process(&id);
                let _ = reply.send(result);
            }
            RuntimeCommand::ComponentStatus { id, reply } => {
                let result = self.component_status(&id);
                let _ = reply.send(result);
            }
            RuntimeCommand::ListComponents { reply } => {
                let _ = reply.send(Ok(self.list_components()));
            }
            RuntimeCommand::Drain { max_steps, reply } => {
                let result = self.runtime.run_until_idle(max_steps).await;
                let _ = reply.send(result);
            }
            RuntimeCommand::Shutdown { reply } => {
                self.start_shutdown(reply);
            }
        }
        Ok(())
    }

    fn install_process(
        &mut self,
        process: Box<dyn RuntimeProcess>,
    ) -> Result<ComponentHandle, RuntimeError> {
        let descriptor = process.descriptor().clone();
        ensure_process_descriptor(&descriptor)?;
        let id = descriptor.id.clone();
        if self.has_component(&id) {
            return Err(RuntimeError::DuplicateComponent(id));
        }

        let (shutdown, shutdown_rx) = ShutdownController::new();
        let ctx = ProcessPrepareContext::new(
            id.clone(),
            self.runtime.cache().clone(),
            self.handle.external_write_sink(),
            shutdown_rx.clone(),
        );
        self.processes.insert(
            id.clone(),
            ProcessRecord {
                kind: descriptor.kind,
                shutdown,
                shutdown_rx,
                status: ComponentStatus::Preparing,
                join: None,
            },
        );

        let events = self.events_tx.clone();
        let prepare_id = id.clone();
        tokio::spawn(async move {
            let prepare = tokio::spawn(async move {
                let mut process = process;
                match process.prepare(ctx).await {
                    Ok(()) => Ok(process),
                    Err(error) => Err(error),
                }
            });
            let result = flatten_join_result(prepare.await);
            let _ = events
                .send(RuntimeEvent::ProcessPrepared {
                    id: prepare_id,
                    result,
                })
                .await;
        });

        Ok(ComponentHandle::new(
            id,
            ComponentKind::Process,
            self.handle.clone(),
        ))
    }

    async fn install_task(
        &mut self,
        task: Box<dyn RuntimeTask>,
    ) -> Result<ComponentHandle, RuntimeError> {
        let descriptor = task.descriptor().clone();
        let id = descriptor.component.id.clone();
        if self.processes.contains_key(&id) {
            return Err(RuntimeError::DuplicateComponent(id));
        }
        self.runtime.register_boxed_task(task).await?;
        Ok(ComponentHandle::new(
            id,
            ComponentKind::Task,
            self.handle.clone(),
        ))
    }

    fn stop_process(&mut self, id: &ComponentId) -> Result<bool, RuntimeError> {
        if self.runtime.contains_task(id) {
            return Err(RuntimeError::WrongComponentKind {
                id: id.clone(),
                expected: ComponentKind::Process,
                found: ComponentKind::Task,
            });
        }

        let Some(record) = self.processes.get_mut(id) else {
            return Err(RuntimeError::MissingComponent(id.clone()));
        };

        match record.status {
            ComponentStatus::Preparing | ComponentStatus::Running => {
                record.shutdown.shutdown();
                record.status = ComponentStatus::Stopping;
                Ok(true)
            }
            ComponentStatus::Stopping => Ok(false),
            ComponentStatus::Stopped
            | ComponentStatus::Completed
            | ComponentStatus::Ready
            | ComponentStatus::Queued
            | ComponentStatus::Failed(_) => Ok(false),
        }
    }

    fn component_status(&self, id: &ComponentId) -> Result<ComponentStatus, RuntimeError> {
        if let Some(record) = self.processes.get(id) {
            return Ok(record.status.clone());
        }

        self.runtime
            .task_status(id)
            .ok_or_else(|| RuntimeError::MissingComponent(id.clone()))
    }

    fn list_components(&self) -> Vec<(ComponentId, ComponentKind, ComponentStatus)> {
        let mut components = Vec::new();
        for (id, record) in &self.processes {
            components.push((id.clone(), record.kind, record.status.clone()));
        }
        for id in self.runtime.task_ids() {
            if let Some(status) = self.runtime.task_status(&id) {
                components.push((id, ComponentKind::Task, status));
            }
        }
        components
    }

    fn handle_event(&mut self, event: RuntimeEvent) {
        match event {
            RuntimeEvent::ProcessPrepared { id, result } => {
                self.handle_process_prepared(id, result);
            }
            RuntimeEvent::ProcessFinished { id, result } => {
                self.handle_process_finished(id, result);
            }
        }
    }

    fn handle_process_prepared(
        &mut self,
        id: ComponentId,
        result: Result<Box<dyn RuntimeProcess>, ComponentError>,
    ) {
        let Some(record) = self.processes.get_mut(&id) else {
            return;
        };

        if matches!(record.status, ComponentStatus::Stopping) || record.shutdown_rx.is_shutdown() {
            record.status = ComponentStatus::Stopped;
            return;
        }

        let process = match result {
            Ok(process) => process,
            Err(error) => {
                record.status = ComponentStatus::Failed(error.to_string());
                return;
            }
        };

        let ctx = ProcessContext::new(
            id.clone(),
            self.runtime.cache().clone(),
            self.handle.external_write_sink(),
            record.shutdown_rx.clone(),
        );
        record.status = ComponentStatus::Running;
        let events = self.events_tx.clone();
        let run_id = id.clone();
        record.join = Some(tokio::spawn(async move {
            let run = tokio::spawn(async move { process.run(ctx).await });
            let result = flatten_join_result(run.await);
            let _ = events
                .send(RuntimeEvent::ProcessFinished { id: run_id, result })
                .await;
        }));
    }

    fn handle_process_finished(&mut self, id: ComponentId, result: Result<(), ComponentError>) {
        let Some(record) = self.processes.get_mut(&id) else {
            return;
        };

        record.join = None;
        match result {
            Ok(()) if matches!(record.status, ComponentStatus::Stopping) => {
                record.status = ComponentStatus::Stopped;
            }
            Ok(()) => {
                record.status = ComponentStatus::Completed;
            }
            Err(error) => {
                record.status = ComponentStatus::Failed(error.to_string());
            }
        }
    }

    fn start_shutdown(&mut self, reply: oneshot::Sender<Result<(), RuntimeError>>) {
        if self.shutdown_reply.is_some() {
            let _ = reply.send(Err(RuntimeError::RuntimeStopped));
            return;
        }

        for record in self.processes.values_mut() {
            match record.status {
                ComponentStatus::Preparing | ComponentStatus::Running => {
                    record.shutdown.shutdown();
                    record.status = ComponentStatus::Stopping;
                }
                _ => {}
            }
        }

        self.shutdown_reply = Some(reply);
    }

    async fn finish_shutdown_if_ready(&mut self) -> Result<bool, RuntimeError> {
        if self.shutdown_reply.is_none() {
            return Ok(false);
        }

        if self.has_active_processes() {
            return Ok(false);
        }

        while let Ok(batch) = self.writes.try_recv() {
            self.runtime.submit_external_writes(batch);
        }

        if !self.runtime.is_idle() {
            self.runtime.run_until_idle(self.run_step_budget).await?;
        }

        if let Some(reply) = self.shutdown_reply.take() {
            let _ = reply.send(Ok(()));
        }
        Ok(true)
    }

    fn has_component(&self, id: &ComponentId) -> bool {
        self.processes.contains_key(id) || self.runtime.contains_task(id)
    }

    fn has_active_processes(&self) -> bool {
        self.processes.values().any(|record| {
            matches!(
                record.status,
                ComponentStatus::Preparing | ComponentStatus::Running | ComponentStatus::Stopping
            )
        })
    }
}

struct ProcessRecord {
    kind: ComponentKind,
    shutdown: ShutdownController,
    shutdown_rx: crate::ShutdownReceiver,
    status: ComponentStatus,
    join: Option<JoinHandle<()>>,
}

pub(crate) enum RuntimeCommand {
    InstallProcess {
        process: Box<dyn RuntimeProcess>,
        reply: oneshot::Sender<Result<ComponentHandle, RuntimeError>>,
    },
    InstallTask {
        task: Box<dyn RuntimeTask>,
        reply: oneshot::Sender<Result<ComponentHandle, RuntimeError>>,
    },
    QueueTask {
        id: ComponentId,
        reply: oneshot::Sender<Result<bool, RuntimeError>>,
    },
    StopProcess {
        id: ComponentId,
        reply: oneshot::Sender<Result<bool, RuntimeError>>,
    },
    ComponentStatus {
        id: ComponentId,
        reply: oneshot::Sender<Result<ComponentStatus, RuntimeError>>,
    },
    ListComponents {
        reply: oneshot::Sender<
            Result<Vec<(ComponentId, ComponentKind, ComponentStatus)>, RuntimeError>,
        >,
    },
    Drain {
        max_steps: usize,
        reply: oneshot::Sender<Result<RunStats, RuntimeError>>,
    },
    Shutdown {
        reply: oneshot::Sender<Result<(), RuntimeError>>,
    },
}

enum RuntimeEvent {
    ProcessPrepared {
        id: ComponentId,
        result: Result<Box<dyn RuntimeProcess>, ComponentError>,
    },
    ProcessFinished {
        id: ComponentId,
        result: Result<(), ComponentError>,
    },
}

fn flatten_join_result<T>(
    result: Result<Result<T, ComponentError>, JoinError>,
) -> Result<T, ComponentError> {
    match result {
        Ok(result) => result,
        Err(error) => Err(ComponentError::Join(join_error_message(error))),
    }
}

fn join_error_message(error: JoinError) -> String {
    if error.is_panic() {
        "component task panicked".to_string()
    } else if error.is_cancelled() {
        "component task was cancelled".to_string()
    } else {
        error.to_string()
    }
}
