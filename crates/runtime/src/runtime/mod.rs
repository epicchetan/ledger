use std::collections::VecDeque;

use cache::{Cache, Key, WriteEffects};

pub(crate) mod dependency;
mod external_write;
pub(crate) mod task_queue;
pub(crate) mod task_registry;

pub use external_write::{ExternalWriteBatch, ExternalWriteReceiver, ExternalWriteSink};

use crate::{
    ComponentId, ComponentKind, ComponentStatus, RuntimeError, RuntimeTask, TaskContext,
    TaskPrepareContext, TaskWake,
};

use self::{dependency::DependencyIndex, task_queue::TaskQueue, task_registry::TaskRegistry};

pub struct Runtime {
    cache: Cache,
    tasks: TaskRegistry,
    dependencies: DependencyIndex,
    external_writes: VecDeque<ExternalWriteBatch>,
    task_queue: TaskQueue,
    prepare_writes: Option<ExternalWriteSink>,
}

impl Runtime {
    pub fn new(cache: Cache) -> Self {
        Self::new_with_prepare_writes(cache, None)
    }

    pub(crate) fn new_with_prepare_writes(
        cache: Cache,
        prepare_writes: Option<ExternalWriteSink>,
    ) -> Self {
        Self {
            cache,
            tasks: TaskRegistry::new(),
            dependencies: DependencyIndex::new(),
            external_writes: VecDeque::new(),
            task_queue: TaskQueue::new(),
            prepare_writes,
        }
    }

    pub fn cache(&self) -> &Cache {
        &self.cache
    }

    pub async fn register_task<T>(&mut self, task: T) -> Result<(), RuntimeError>
    where
        T: RuntimeTask,
    {
        self.register_boxed_task(Box::new(task)).await
    }

    pub(crate) async fn register_boxed_task(
        &mut self,
        mut task: Box<dyn RuntimeTask>,
    ) -> Result<(), RuntimeError> {
        let descriptor = task.descriptor();
        if descriptor.component.kind != ComponentKind::Task {
            return Err(RuntimeError::WrongComponentKind {
                id: descriptor.component.id.clone(),
                expected: ComponentKind::Task,
                found: descriptor.component.kind,
            });
        }
        let id = descriptor.component.id.clone();
        if self.tasks.contains(&id) {
            return Err(RuntimeError::DuplicateComponent(id));
        }

        let ctx =
            TaskPrepareContext::new(id.clone(), self.cache.clone(), self.prepare_writes.clone());
        task.prepare(ctx)
            .await
            .map_err(|source| RuntimeError::Component {
                id: id.clone(),
                source,
            })?;

        let registration = self.tasks.insert(task)?;
        self.dependencies
            .add_task(&registration.id, &registration.dependencies);
        self.task_queue.insert_task(registration.id);
        Ok(())
    }

    pub fn contains_task(&self, id: &ComponentId) -> bool {
        self.tasks.contains(id)
    }

    pub fn task_ids(&self) -> Vec<ComponentId> {
        self.tasks.ids()
    }

    pub fn submit_external_writes(&mut self, batch: ExternalWriteBatch) {
        if !batch.is_empty() {
            self.external_writes.push_back(batch);
        }
    }

    pub fn queue_task(&mut self, id: &ComponentId) -> Result<bool, RuntimeError> {
        if !self.tasks.contains(id) {
            return Err(RuntimeError::MissingComponent(id.clone()));
        }

        Ok(self.task_queue.enqueue_once(id.clone(), TaskWake::Manual))
    }

    pub fn task_status(&self, id: &ComponentId) -> Option<ComponentStatus> {
        if self.tasks.contains(id) {
            self.task_queue.status(id)
        } else {
            None
        }
    }

    pub fn is_idle(&self) -> bool {
        self.external_writes.is_empty() && self.task_queue.is_empty()
    }

    pub async fn run_once(&mut self) -> Result<RuntimeStep, RuntimeError> {
        if self.is_idle() {
            return Ok(RuntimeStep {
                external_write_batches_applied: 0,
                task_run: None,
                changed_keys: Vec::new(),
                scheduled_tasks: Vec::new(),
                idle_after: true,
            });
        }

        let mut step = RuntimeStep {
            external_write_batches_applied: 0,
            task_run: None,
            changed_keys: Vec::new(),
            scheduled_tasks: Vec::new(),
            idle_after: false,
        };

        let external_count = self.external_writes.len();
        for _ in 0..external_count {
            let Some(batch) = self.external_writes.pop_front() else {
                break;
            };
            let (result, effects) = batch.apply(&self.cache);
            step.external_write_batches_applied += 1;
            record_keys(&mut step.changed_keys, &effects.changed_keys);
            self.schedule_effects(&effects, &mut step.scheduled_tasks);

            if let Err(error) = result {
                return Err(RuntimeError::Cache(error));
            }
        }

        if let Some((task_id, wake)) = self.task_queue.pop_next() {
            let mut task = self.tasks.take(&task_id)?;
            let result = {
                let writes = crate::write::LocalComponentWriteSubmitter::new(
                    &self.cache,
                    &self.dependencies,
                    &mut self.task_queue,
                    &mut step.changed_keys,
                    &mut step.scheduled_tasks,
                );
                let ctx = TaskContext::new(task_id.clone(), self.cache.clone(), wake, writes);
                task.run_once(ctx).await
            };
            self.tasks.put(task_id.clone(), task);
            let success = result.is_ok();
            self.task_queue.finish_running(&task_id, success);
            step.task_run = Some(task_id.clone());

            if let Err(source) = result {
                return Err(RuntimeError::Component {
                    id: task_id,
                    source,
                });
            }
        }

        step.idle_after = self.is_idle();
        Ok(step)
    }

    pub async fn run_until_idle(&mut self, max_steps: usize) -> Result<RunStats, RuntimeError> {
        let mut stats = RunStats {
            steps: 0,
            external_write_batches_applied: 0,
            tasks_run: 0,
            changed_keys: Vec::new(),
        };

        while !self.is_idle() {
            if stats.steps == max_steps {
                return Err(RuntimeError::RunLimitExceeded { max_steps });
            }

            let step = self.run_once().await?;
            stats.steps += 1;
            stats.external_write_batches_applied += step.external_write_batches_applied;
            if step.task_run.is_some() {
                stats.tasks_run += 1;
            }
            record_keys(&mut stats.changed_keys, &step.changed_keys);
        }

        Ok(stats)
    }

    fn schedule_effects(&mut self, effects: &WriteEffects, scheduled_tasks: &mut Vec<ComponentId>) {
        for changed_key in &effects.changed_keys {
            let dependents = self.dependencies.dependents_for(changed_key).to_vec();
            for task_id in dependents {
                if self
                    .task_queue
                    .enqueue_once(task_id.clone(), TaskWake::DependencyChanged)
                {
                    record_task(scheduled_tasks, task_id);
                }
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeStep {
    pub external_write_batches_applied: usize,
    pub task_run: Option<ComponentId>,
    pub changed_keys: Vec<Key>,
    pub scheduled_tasks: Vec<ComponentId>,
    pub idle_after: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunStats {
    pub steps: usize,
    pub external_write_batches_applied: usize,
    pub tasks_run: usize,
    pub changed_keys: Vec<Key>,
}

fn record_keys(target: &mut Vec<Key>, keys: &[Key]) {
    for key in keys {
        if !target.contains(key) {
            target.push(key.clone());
        }
    }
}

fn record_task(target: &mut Vec<ComponentId>, task_id: ComponentId) {
    if !target.contains(&task_id) {
        target.push(task_id);
    }
}
