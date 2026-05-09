use std::collections::{HashMap, HashSet};

use cache::Key;

use crate::{ComponentId, ComponentKind, RuntimeError, RuntimeTask, TaskDescriptor};

pub(crate) struct TaskRegistration {
    pub(crate) id: ComponentId,
    pub(crate) dependencies: Vec<Key>,
}

#[derive(Default)]
pub(crate) struct TaskRegistry {
    tasks: HashMap<ComponentId, Box<dyn RuntimeTask>>,
}

impl TaskRegistry {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) fn insert(
        &mut self,
        task: Box<dyn RuntimeTask>,
    ) -> Result<TaskRegistration, RuntimeError> {
        let descriptor = task.descriptor();
        ensure_task_descriptor(descriptor)?;
        let id = descriptor.component.id.clone();
        if self.tasks.contains_key(&id) {
            return Err(RuntimeError::DuplicateComponent(id));
        }
        let dependencies = dedupe_keys(&descriptor.dependencies);

        self.tasks.insert(id.clone(), task);
        Ok(TaskRegistration { id, dependencies })
    }

    pub(crate) fn contains(&self, id: &ComponentId) -> bool {
        self.tasks.contains_key(id)
    }

    pub(crate) fn take(&mut self, id: &ComponentId) -> Result<Box<dyn RuntimeTask>, RuntimeError> {
        self.tasks
            .remove(id)
            .ok_or_else(|| RuntimeError::MissingComponent(id.clone()))
    }

    pub(crate) fn put(&mut self, id: ComponentId, task: Box<dyn RuntimeTask>) {
        self.tasks.insert(id, task);
    }

    pub(crate) fn ids(&self) -> Vec<ComponentId> {
        self.tasks.keys().cloned().collect()
    }
}

fn ensure_task_descriptor(descriptor: &TaskDescriptor) -> Result<(), RuntimeError> {
    if descriptor.component.kind == ComponentKind::Task {
        Ok(())
    } else {
        Err(RuntimeError::WrongComponentKind {
            id: descriptor.component.id.clone(),
            expected: ComponentKind::Task,
            found: descriptor.component.kind,
        })
    }
}

fn dedupe_keys(keys: &[Key]) -> Vec<Key> {
    let mut seen = HashSet::new();
    let mut deduped = Vec::new();

    for key in keys {
        if seen.insert(key.clone()) {
            deduped.push(key.clone());
        }
    }

    deduped
}
