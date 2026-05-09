use std::fmt;

use cache::{CellOwner, Key};

use crate::ComponentError;

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ComponentId(Key);

impl ComponentId {
    pub fn new(value: impl Into<String>) -> Result<Self, ComponentError> {
        let value = value.into();
        let key = Key::new(value.clone()).map_err(|_| ComponentError::InvalidComponentId(value))?;
        Ok(Self(key))
    }

    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }

    pub fn key(&self) -> &Key {
        &self.0
    }

    pub fn owner(&self) -> CellOwner {
        CellOwner::new(self.as_str()).expect("component id already validates as a cache owner")
    }
}

impl fmt::Display for ComponentId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ComponentKind {
    Process,
    Task,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ComponentDescriptor {
    pub id: ComponentId,
    pub kind: ComponentKind,
}

impl ComponentDescriptor {
    pub fn new(id: ComponentId, kind: ComponentKind) -> Self {
        Self { id, kind }
    }

    pub fn process(id: ComponentId) -> Self {
        Self::new(id, ComponentKind::Process)
    }

    pub fn task(id: ComponentId) -> Self {
        Self::new(id, ComponentKind::Task)
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TaskDescriptor {
    pub component: ComponentDescriptor,
    pub dependencies: Vec<Key>,
}

impl TaskDescriptor {
    pub fn new(id: ComponentId, dependencies: Vec<Key>) -> Self {
        Self {
            component: ComponentDescriptor::task(id),
            dependencies,
        }
    }

    pub fn id(&self) -> &ComponentId {
        &self.component.id
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ComponentStatus {
    Preparing,
    Ready,
    Queued,
    Running,
    Stopping,
    Stopped,
    Completed,
    Failed(String),
}
