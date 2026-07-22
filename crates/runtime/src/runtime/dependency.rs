use std::collections::HashMap;

use cache::Key;

use crate::ComponentId;

#[derive(Debug, Default)]
pub(crate) struct DependencyIndex {
    dependents: HashMap<Key, Vec<ComponentId>>,
    dependencies: HashMap<ComponentId, Vec<Key>>,
}

impl DependencyIndex {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) fn add_task(&mut self, id: &ComponentId, dependencies: &[Key]) {
        self.dependencies.insert(id.clone(), dependencies.to_vec());
        for dependency in dependencies {
            self.dependents
                .entry(dependency.clone())
                .or_default()
                .push(id.clone());
        }
    }

    pub(crate) fn remove_task(&mut self, id: &ComponentId) {
        let Some(dependencies) = self.dependencies.remove(id) else {
            return;
        };
        for dependency in dependencies {
            let remove_key = if let Some(dependents) = self.dependents.get_mut(&dependency) {
                dependents.retain(|dependent| dependent != id);
                dependents.is_empty()
            } else {
                false
            };
            if remove_key {
                self.dependents.remove(&dependency);
            }
        }
    }

    pub(crate) fn dependents_for(&self, key: &Key) -> &[ComponentId] {
        self.dependents.get(key).map(Vec::as_slice).unwrap_or(&[])
    }
}
