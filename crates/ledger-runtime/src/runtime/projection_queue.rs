use std::collections::{HashSet, VecDeque};

use crate::ProjectionId;

#[derive(Debug, Default)]
pub(crate) struct ProjectionQueue {
    order: VecDeque<ProjectionId>,
    queued: HashSet<ProjectionId>,
}

impl ProjectionQueue {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) fn enqueue_once(&mut self, id: ProjectionId) -> bool {
        if !self.queued.insert(id.clone()) {
            return false;
        }

        self.order.push_back(id);
        true
    }

    pub(crate) fn pop_next(&mut self) -> Option<ProjectionId> {
        let id = self.order.pop_front()?;
        self.queued.remove(&id);
        Some(id)
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.order.is_empty()
    }
}
