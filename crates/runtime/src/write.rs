use std::{ops::Range, sync::Mutex};

use cache::{ArrayKey, Cache, CellOwner, Key, ValueKey, WriteEffects};

use crate::{
    runtime::{dependency::DependencyIndex, task_queue::TaskQueue},
    ComponentError, ComponentId, ExternalWriteBatch, ExternalWriteSink, TaskWake,
};

pub struct ComponentWriteContext<'a> {
    component_id: ComponentId,
    cache: Cache,
    submitter: ComponentWriteSubmitter<'a>,
}

impl<'a> ComponentWriteContext<'a> {
    pub(crate) fn external(
        component_id: ComponentId,
        cache: Cache,
        writes: ExternalWriteSink,
    ) -> Self {
        Self {
            component_id,
            cache,
            submitter: ComponentWriteSubmitter::External(writes),
        }
    }

    pub(crate) fn unavailable(component_id: ComponentId, cache: Cache) -> Self {
        Self {
            component_id,
            cache,
            submitter: ComponentWriteSubmitter::Unavailable,
        }
    }

    pub(crate) fn local(
        component_id: ComponentId,
        cache: Cache,
        submitter: LocalComponentWriteSubmitter<'a>,
    ) -> Self {
        Self {
            component_id,
            cache,
            submitter: ComponentWriteSubmitter::Local(Mutex::new(submitter)),
        }
    }

    pub fn component_id(&self) -> &ComponentId {
        &self.component_id
    }

    pub fn owner(&self) -> CellOwner {
        self.component_id.owner()
    }

    pub fn cache(&self) -> &Cache {
        &self.cache
    }

    pub fn batch(&self) -> ExternalWriteBatch {
        ExternalWriteBatch::new(self.owner())
    }

    pub async fn submit(&self, batch: ExternalWriteBatch) -> Result<(), ComponentError> {
        match &self.submitter {
            ComponentWriteSubmitter::External(writes) => writes
                .submit(batch)
                .await
                .map_err(|_| ComponentError::RuntimeIngressClosed),
            ComponentWriteSubmitter::Local(submitter) => submitter
                .lock()
                .map_err(|_| {
                    ComponentError::Message("component write submitter lock poisoned".to_string())
                })?
                .submit(batch),
            ComponentWriteSubmitter::Unavailable => Err(ComponentError::RuntimeIngressClosed),
        }
    }

    pub fn read_value<T>(&self, key: &ValueKey<T>) -> Result<Option<T>, ComponentError>
    where
        T: Clone + Send + Sync + 'static,
    {
        Ok(self.cache.read_value(key)?)
    }

    pub fn read_array<T>(&self, key: &ArrayKey<T>) -> Result<Vec<T>, ComponentError>
    where
        T: Clone + Send + Sync + 'static,
    {
        Ok(self.cache.read_array(key)?)
    }

    pub fn read_array_range<T>(
        &self,
        key: &ArrayKey<T>,
        range: Range<usize>,
    ) -> Result<Vec<T>, ComponentError>
    where
        T: Clone + Send + Sync + 'static,
    {
        Ok(self.cache.read_array_range(key, range)?)
    }
}

enum ComponentWriteSubmitter<'a> {
    External(ExternalWriteSink),
    Local(Mutex<LocalComponentWriteSubmitter<'a>>),
    Unavailable,
}

pub(crate) struct LocalComponentWriteSubmitter<'a> {
    cache: &'a Cache,
    dependencies: &'a DependencyIndex,
    task_queue: &'a mut TaskQueue,
    changed_keys: &'a mut Vec<Key>,
    scheduled_tasks: &'a mut Vec<ComponentId>,
}

impl<'a> LocalComponentWriteSubmitter<'a> {
    pub(crate) fn new(
        cache: &'a Cache,
        dependencies: &'a DependencyIndex,
        task_queue: &'a mut TaskQueue,
        changed_keys: &'a mut Vec<Key>,
        scheduled_tasks: &'a mut Vec<ComponentId>,
    ) -> Self {
        Self {
            cache,
            dependencies,
            task_queue,
            changed_keys,
            scheduled_tasks,
        }
    }

    fn submit(&mut self, batch: ExternalWriteBatch) -> Result<(), ComponentError> {
        if batch.is_empty() {
            return Ok(());
        }

        let (result, effects) = batch.apply(self.cache);
        record_keys(self.changed_keys, &effects.changed_keys);
        self.schedule_effects(&effects);
        result?;
        Ok(())
    }

    fn schedule_effects(&mut self, effects: &WriteEffects) {
        for changed_key in &effects.changed_keys {
            let dependents = self.dependencies.dependents_for(changed_key).to_vec();
            for task_id in dependents {
                if self
                    .task_queue
                    .enqueue_once(task_id.clone(), TaskWake::DependencyChanged)
                {
                    record_task(self.scheduled_tasks, task_id);
                }
            }
        }
    }
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
