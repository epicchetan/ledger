use std::collections::HashMap;

use cache::Key;

use crate::ComponentId;

#[derive(Debug, Default)]
pub(crate) struct DependencyIndex {
    dependents: HashMap<Key, Vec<ComponentId>>,
}

impl DependencyIndex {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) fn add_task(&mut self, id: &ComponentId, dependencies: &[Key]) {
        for dependency in dependencies {
            self.dependents
                .entry(dependency.clone())
                .or_default()
                .push(id.clone());
        }
    }

    pub(crate) fn dependents_for(&self, key: &Key) -> &[ComponentId] {
        self.dependents.get(key).map(Vec::as_slice).unwrap_or(&[])
    }
}
