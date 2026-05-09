use async_trait::async_trait;
use cache::{ArrayKey, Cache, CellOwner, ValueKey};
use tokio::sync::watch;

use crate::{
    ComponentDescriptor, ComponentError, ComponentId, ComponentKind, ComponentWriteContext,
    ExternalWriteBatch, ExternalWriteSink,
};

#[async_trait]
pub trait RuntimeProcess: Send + 'static {
    fn descriptor(&self) -> &ComponentDescriptor;

    async fn prepare(&mut self, ctx: ProcessPrepareContext) -> Result<(), ComponentError> {
        let _ = ctx;
        Ok(())
    }

    async fn run(self: Box<Self>, ctx: ProcessContext) -> Result<(), ComponentError>;
}

#[derive(Clone)]
pub(crate) struct ShutdownController {
    tx: watch::Sender<bool>,
}

impl ShutdownController {
    pub(crate) fn new() -> (Self, ShutdownReceiver) {
        let (tx, rx) = watch::channel(false);
        (Self { tx }, ShutdownReceiver { rx })
    }

    pub(crate) fn shutdown(&self) {
        let _ = self.tx.send(true);
    }
}

#[derive(Clone)]
pub struct ShutdownReceiver {
    rx: watch::Receiver<bool>,
}

impl ShutdownReceiver {
    pub fn is_shutdown(&self) -> bool {
        *self.rx.borrow()
    }

    pub async fn changed(&mut self) -> Result<(), ComponentError> {
        self.rx
            .changed()
            .await
            .map_err(|_| ComponentError::RuntimeIngressClosed)
    }
}

pub struct ProcessPrepareContext {
    writes: ComponentWriteContext<'static>,
    shutdown: ShutdownReceiver,
}

impl ProcessPrepareContext {
    pub(crate) fn new(
        component_id: ComponentId,
        cache: Cache,
        writes: ExternalWriteSink,
        shutdown: ShutdownReceiver,
    ) -> Self {
        let writes = ComponentWriteContext::external(component_id, cache, writes);
        Self { writes, shutdown }
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

    pub fn shutdown(&self) -> &ShutdownReceiver {
        &self.shutdown
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

pub struct ProcessContext {
    writes: ComponentWriteContext<'static>,
    shutdown: ShutdownReceiver,
}

impl ProcessContext {
    pub(crate) fn new(
        component_id: ComponentId,
        cache: Cache,
        writes: ExternalWriteSink,
        shutdown: ShutdownReceiver,
    ) -> Self {
        let writes = ComponentWriteContext::external(component_id, cache, writes);
        Self { writes, shutdown }
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

    pub fn shutdown(&self) -> &ShutdownReceiver {
        &self.shutdown
    }

    pub fn shutdown_mut(&mut self) -> &mut ShutdownReceiver {
        &mut self.shutdown
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

pub(crate) fn ensure_process_descriptor(
    descriptor: &ComponentDescriptor,
) -> Result<(), crate::RuntimeError> {
    if descriptor.kind == ComponentKind::Process {
        Ok(())
    } else {
        Err(crate::RuntimeError::WrongComponentKind {
            id: descriptor.id.clone(),
            expected: ComponentKind::Process,
            found: descriptor.kind,
        })
    }
}
