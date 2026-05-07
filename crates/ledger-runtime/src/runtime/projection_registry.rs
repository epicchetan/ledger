use std::collections::{HashMap, HashSet};

use crate::{runtime::RuntimeError, Key, Projection, ProjectionId};

pub(crate) struct ProjectionRegistration {
    pub(crate) id: ProjectionId,
    pub(crate) dependencies: Vec<Key>,
}

#[derive(Default)]
pub(crate) struct ProjectionRegistry {
    projections: HashMap<ProjectionId, Box<dyn Projection>>,
}

impl ProjectionRegistry {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) fn register(
        &mut self,
        projection: Box<dyn Projection>,
    ) -> Result<ProjectionRegistration, RuntimeError> {
        let descriptor = projection.descriptor();
        let id = descriptor.id.clone();
        if self.projections.contains_key(&id) {
            return Err(RuntimeError::DuplicateProjection(id));
        }
        let dependencies = dedupe_keys(&descriptor.dependencies);

        self.projections.insert(id.clone(), projection);
        Ok(ProjectionRegistration { id, dependencies })
    }

    pub(crate) fn contains(&self, id: &ProjectionId) -> bool {
        self.projections.contains_key(id)
    }

    pub(crate) fn get_mut(
        &mut self,
        id: &ProjectionId,
    ) -> Result<&mut Box<dyn Projection>, RuntimeError> {
        self.projections
            .get_mut(id)
            .ok_or_else(|| RuntimeError::MissingProjection(id.clone()))
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
