use std::collections::HashMap;

use crate::{Key, ProjectionId};

#[derive(Debug, Default)]
pub(crate) struct DependencyIndex {
    dependents: HashMap<Key, Vec<ProjectionId>>,
}

impl DependencyIndex {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) fn add_projection(&mut self, id: &ProjectionId, dependencies: &[Key]) {
        for dependency in dependencies {
            self.dependents
                .entry(dependency.clone())
                .or_default()
                .push(id.clone());
        }
    }

    pub(crate) fn dependents_for(&self, key: &Key) -> &[ProjectionId] {
        self.dependents.get(key).map(Vec::as_slice).unwrap_or(&[])
    }
}
