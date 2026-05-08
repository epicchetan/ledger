use std::collections::VecDeque;

use cache::{Cache, CacheError, Key, WriteEffects};
use thiserror::Error;

mod dependency;
mod external_write;
mod projection_queue;
mod projection_registry;

pub use external_write::ExternalWriteBatch;

use crate::{Projection, ProjectionContext, ProjectionError, ProjectionId};

use self::{
    dependency::DependencyIndex, projection_queue::ProjectionQueue,
    projection_registry::ProjectionRegistry,
};

pub struct Runtime {
    cache: Cache,
    projections: ProjectionRegistry,
    dependencies: DependencyIndex,
    external_writes: VecDeque<ExternalWriteBatch>,
    projection_queue: ProjectionQueue,
}

impl Runtime {
    pub fn new(cache: Cache) -> Self {
        Self {
            cache,
            projections: ProjectionRegistry::new(),
            dependencies: DependencyIndex::new(),
            external_writes: VecDeque::new(),
            projection_queue: ProjectionQueue::new(),
        }
    }

    pub fn cache(&self) -> &Cache {
        &self.cache
    }

    pub fn register_projection<P>(&mut self, projection: P) -> Result<(), RuntimeError>
    where
        P: Projection + 'static,
    {
        self.register_boxed_projection(Box::new(projection))
    }

    pub fn submit_external_writes(&mut self, batch: ExternalWriteBatch) {
        if !batch.is_empty() {
            self.external_writes.push_back(batch);
        }
    }

    pub fn queue_projection(&mut self, id: &ProjectionId) -> Result<bool, RuntimeError> {
        if !self.projections.contains(id) {
            return Err(RuntimeError::MissingProjection(id.clone()));
        }

        Ok(self.projection_queue.enqueue_once(id.clone()))
    }

    pub fn is_idle(&self) -> bool {
        self.external_writes.is_empty() && self.projection_queue.is_empty()
    }

    pub fn run_once(&mut self) -> Result<RuntimeStep, RuntimeError> {
        if self.is_idle() {
            return Ok(RuntimeStep {
                external_write_batches_applied: 0,
                projection_run: None,
                changed_keys: Vec::new(),
                scheduled_projections: Vec::new(),
                idle_after: true,
            });
        }

        let mut step = RuntimeStep {
            external_write_batches_applied: 0,
            projection_run: None,
            changed_keys: Vec::new(),
            scheduled_projections: Vec::new(),
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
            self.schedule_effects(&effects, &mut step.scheduled_projections);

            if let Err(error) = result {
                return Err(error);
            }
        }

        if let Some(projection_id) = self.projection_queue.pop_next() {
            let (result, effects) = self.run_projection(&projection_id)?;
            step.projection_run = Some(projection_id.clone());
            record_keys(&mut step.changed_keys, &effects.changed_keys);
            self.schedule_effects(&effects, &mut step.scheduled_projections);

            if let Err(source) = result {
                return Err(RuntimeError::Projection {
                    id: projection_id,
                    source,
                });
            }
        }

        step.idle_after = self.is_idle();
        Ok(step)
    }

    pub fn run_until_idle(&mut self, max_steps: usize) -> Result<RunStats, RuntimeError> {
        let mut stats = RunStats {
            steps: 0,
            external_write_batches_applied: 0,
            projections_run: 0,
            changed_keys: Vec::new(),
        };

        while !self.is_idle() {
            if stats.steps == max_steps {
                return Err(RuntimeError::RunLimitExceeded { max_steps });
            }

            let step = self.run_once()?;
            stats.steps += 1;
            stats.external_write_batches_applied += step.external_write_batches_applied;
            if step.projection_run.is_some() {
                stats.projections_run += 1;
            }
            record_keys(&mut stats.changed_keys, &step.changed_keys);
        }

        Ok(stats)
    }

    fn register_boxed_projection(
        &mut self,
        projection: Box<dyn Projection>,
    ) -> Result<(), RuntimeError> {
        let registration = self.projections.register(projection)?;
        self.dependencies
            .add_projection(&registration.id, &registration.dependencies);
        Ok(())
    }

    fn run_projection(
        &mut self,
        projection_id: &ProjectionId,
    ) -> Result<(Result<(), ProjectionError>, WriteEffects), RuntimeError> {
        let mut ctx = ProjectionContext::new(&self.cache, projection_id.clone());
        let result = {
            let projection = self.projections.get_mut(projection_id)?;
            projection.run(&mut ctx)
        };
        let effects = ctx.into_effects();
        Ok((result, effects))
    }

    fn schedule_effects(
        &mut self,
        effects: &WriteEffects,
        scheduled_projections: &mut Vec<ProjectionId>,
    ) {
        for changed_key in &effects.changed_keys {
            let dependents = self.dependencies.dependents_for(changed_key).to_vec();
            for projection_id in dependents {
                if self.projection_queue.enqueue_once(projection_id.clone()) {
                    record_projection(scheduled_projections, projection_id);
                }
            }
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RuntimeStep {
    pub external_write_batches_applied: usize,
    pub projection_run: Option<ProjectionId>,
    pub changed_keys: Vec<Key>,
    pub scheduled_projections: Vec<ProjectionId>,
    pub idle_after: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunStats {
    pub steps: usize,
    pub external_write_batches_applied: usize,
    pub projections_run: usize,
    pub changed_keys: Vec<Key>,
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum RuntimeError {
    #[error("duplicate projection `{0}`")]
    DuplicateProjection(ProjectionId),

    #[error("missing projection `{0}`")]
    MissingProjection(ProjectionId),

    #[error(transparent)]
    Cache(#[from] CacheError),

    #[error("projection `{id}` failed: {source}")]
    Projection {
        id: ProjectionId,
        source: ProjectionError,
    },

    #[error("run limit exceeded after {max_steps} steps")]
    RunLimitExceeded { max_steps: usize },
}

fn record_keys(target: &mut Vec<Key>, keys: &[Key]) {
    for key in keys {
        if !target.contains(key) {
            target.push(key.clone());
        }
    }
}

fn record_projection(target: &mut Vec<ProjectionId>, projection_id: ProjectionId) {
    if !target.contains(&projection_id) {
        target.push(projection_id);
    }
}
