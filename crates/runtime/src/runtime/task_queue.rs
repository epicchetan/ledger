use std::collections::{HashMap, VecDeque};

use crate::{ComponentId, ComponentStatus, TaskWake};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum TaskQueueState {
    Ready,
    Queued { wake: TaskWake },
    Running { rerun_queued: bool },
    Failed,
}

#[derive(Debug, Default)]
pub(crate) struct TaskQueue {
    order: VecDeque<ComponentId>,
    states: HashMap<ComponentId, TaskQueueState>,
}

impl TaskQueue {
    pub(crate) fn new() -> Self {
        Self::default()
    }

    pub(crate) fn insert_task(&mut self, id: ComponentId) {
        self.states.insert(id, TaskQueueState::Ready);
    }

    pub(crate) fn enqueue_once(&mut self, id: ComponentId, wake: TaskWake) -> bool {
        match self.states.get_mut(&id) {
            Some(TaskQueueState::Ready) => {
                *self.states.get_mut(&id).expect("state exists") = TaskQueueState::Queued { wake };
                self.order.push_back(id);
                true
            }
            Some(TaskQueueState::Queued { .. }) => false,
            Some(TaskQueueState::Running { rerun_queued }) => {
                if *rerun_queued {
                    false
                } else {
                    *rerun_queued = true;
                    true
                }
            }
            Some(TaskQueueState::Failed) | None => false,
        }
    }

    pub(crate) fn pop_next(&mut self) -> Option<(ComponentId, TaskWake)> {
        let id = self.order.pop_front()?;
        let wake = match self.states.get(&id) {
            Some(TaskQueueState::Queued { wake }) => *wake,
            _ => TaskWake::Manual,
        };
        self.states.insert(
            id.clone(),
            TaskQueueState::Running {
                rerun_queued: false,
            },
        );
        Some((id, wake))
    }

    pub(crate) fn finish_running(&mut self, id: &ComponentId, success: bool) {
        let Some(state) = self.states.get(id).copied() else {
            return;
        };

        if !success {
            self.states.insert(id.clone(), TaskQueueState::Failed);
            return;
        }

        match state {
            TaskQueueState::Running { rerun_queued: true } => {
                self.states.insert(
                    id.clone(),
                    TaskQueueState::Queued {
                        wake: TaskWake::Rerun,
                    },
                );
                self.order.push_back(id.clone());
            }
            TaskQueueState::Running {
                rerun_queued: false,
            } => {
                self.states.insert(id.clone(), TaskQueueState::Ready);
            }
            TaskQueueState::Ready | TaskQueueState::Queued { .. } | TaskQueueState::Failed => {}
        }
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.order.is_empty()
    }

    pub(crate) fn status(&self, id: &ComponentId) -> Option<ComponentStatus> {
        match self.states.get(id)? {
            TaskQueueState::Ready => Some(ComponentStatus::Ready),
            TaskQueueState::Queued { .. } => Some(ComponentStatus::Queued),
            TaskQueueState::Running { .. } => Some(ComponentStatus::Running),
            TaskQueueState::Failed => Some(ComponentStatus::Failed("task failed".to_string())),
        }
    }
}
