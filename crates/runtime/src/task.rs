use async_trait::async_trait;
use cache::{ArrayKey, Cache, CellOwner, ValueKey};

use crate::{
    ComponentError, ComponentId, ComponentWriteContext, ExternalWriteBatch, ExternalWriteSink,
    TaskDescriptor,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum TaskWake {
    DependencyChanged,
    Manual,
    Rerun,
}

#[async_trait]
pub trait RuntimeTask: Send + 'static {
    fn descriptor(&self) -> &TaskDescriptor;

    async fn prepare(&mut self, ctx: TaskPrepareContext) -> Result<(), ComponentError> {
        let _ = ctx;
        Ok(())
    }

    async fn run_once(&mut self, ctx: TaskContext<'_>) -> Result<(), ComponentError>;
}

pub struct TaskPrepareContext {
    writes: ComponentWriteContext<'static>,
}

impl TaskPrepareContext {
    pub(crate) fn new(
        component_id: ComponentId,
        cache: Cache,
        writes: Option<ExternalWriteSink>,
    ) -> Self {
        let writes = match writes {
            Some(writes) => ComponentWriteContext::external(component_id, cache, writes),
            None => ComponentWriteContext::unavailable(component_id, cache),
        };
        Self { writes }
    }

    pub fn component_id(&self) -> &ComponentId {
        self.writes.component_id()
    }

    pub fn owner(&self) -> CellOwner {
        self.writes.owner()
    }

    pub fn cache(&self) -> &Cache {
        self.writes.cache()
    }

    pub fn batch(&self) -> ExternalWriteBatch {
        self.writes.batch()
    }

    pub async fn submit(&self, batch: ExternalWriteBatch) -> Result<(), ComponentError> {
        self.writes.submit(batch).await
    }

    pub fn read_value<T>(&self, key: &ValueKey<T>) -> Result<Option<T>, ComponentError>
    where
        T: Clone + Send + Sync + 'static,
    {
        self.writes.read_value(key)
    }

    pub fn read_array<T>(&self, key: &ArrayKey<T>) -> Result<Vec<T>, ComponentError>
    where
        T: Clone + Send + Sync + 'static,
    {
        self.writes.read_array(key)
    }

    pub fn read_array_range<T>(
        &self,
        key: &ArrayKey<T>,
        range: std::ops::Range<usize>,
    ) -> Result<Vec<T>, ComponentError>
    where
        T: Clone + Send + Sync + 'static,
    {
        self.writes.read_array_range(key, range)
    }
}

pub struct TaskContext<'a> {
    wake: TaskWake,
    writes: ComponentWriteContext<'a>,
}

impl<'a> TaskContext<'a> {
    pub(crate) fn new(
        component_id: ComponentId,
        cache: Cache,
        wake: TaskWake,
        writes: crate::write::LocalComponentWriteSubmitter<'a>,
    ) -> Self {
        let writes = ComponentWriteContext::local(component_id, cache, writes);
        Self { wake, writes }
    }

    pub fn component_id(&self) -> &ComponentId {
        self.writes.component_id()
    }

    pub fn owner(&self) -> CellOwner {
        self.writes.owner()
    }

    pub fn cache(&self) -> &Cache {
        self.writes.cache()
    }

    pub fn wake(&self) -> TaskWake {
        self.wake
    }

    pub fn batch(&self) -> ExternalWriteBatch {
        self.writes.batch()
    }

    pub async fn submit(&self, batch: ExternalWriteBatch) -> Result<(), ComponentError> {
        self.writes.submit(batch).await
    }

    pub fn read_value<T>(&self, key: &ValueKey<T>) -> Result<Option<T>, ComponentError>
    where
        T: Clone + Send + Sync + 'static,
    {
        self.writes.read_value(key)
    }

    pub fn read_array<T>(&self, key: &ArrayKey<T>) -> Result<Vec<T>, ComponentError>
    where
        T: Clone + Send + Sync + 'static,
    {
        self.writes.read_array(key)
    }

    pub fn read_array_range<T>(
        &self,
        key: &ArrayKey<T>,
        range: std::ops::Range<usize>,
    ) -> Result<Vec<T>, ComponentError>
    where
        T: Clone + Send + Sync + 'static,
    {
        self.writes.read_array_range(key, range)
    }
}
