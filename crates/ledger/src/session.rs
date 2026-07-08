use std::sync::Arc;

use cache::{Cache, CellDescriptor, CellKind, CellOwner, Key, ValueKey};
use runtime::{
    ExternalWriteBatch, ExternalWriteSink, RuntimeHandle, RuntimeProcess, RuntimeTask,
    RuntimeWorker,
};
use store::{RemoteStore, Store, StoreObjectId};
use tokio::task::JoinHandle;

use crate::clock::{ClockSnapshot, ClockState};
use crate::feed::es_replay::{EsReplayCells, EsReplayFeed};
use crate::projection::{BarsCells, BarsParams, BarsTask};
use crate::LedgerError;

pub struct LedgerSessionBuilder<S>
where
    S: RemoteStore + 'static,
{
    store: Arc<Store<S>>,
    cache: Cache,
    clock_key: ValueKey<ClockState>,
    tasks: Vec<Box<dyn RuntimeTask>>,
    feeds: Vec<Box<dyn RuntimeProcess>>,
}

impl<S> LedgerSessionBuilder<S>
where
    S: RemoteStore + 'static,
{
    pub fn new(store: Arc<Store<S>>) -> Result<Self, LedgerError> {
        let cache = Cache::new();
        let session_owner = session_owner()?;
        let clock_key = cache.register_value::<ClockState>(
            CellDescriptor {
                key: Key::new("session.clock")?,
                owner: session_owner,
                kind: CellKind::Value,
                public_read: true,
            },
            None,
        )?;
        Ok(Self {
            store,
            cache,
            clock_key,
            tasks: Vec::new(),
            feeds: Vec::new(),
        })
    }

    pub fn es_replay(
        &mut self,
        raw_object_id: StoreObjectId,
    ) -> Result<EsReplayCells, LedgerError> {
        let cells = EsReplayCells::register(&self.cache)?;
        let feed = EsReplayFeed::new(
            raw_object_id,
            self.store.clone(),
            self.clock_key.clone(),
            cells.clone(),
        );
        self.feeds.push(Box::new(feed));
        Ok(cells)
    }

    /// Register a time-bars projection over an es_replay feed's cells.
    pub fn bars(
        &mut self,
        feed: &EsReplayCells,
        params: BarsParams,
    ) -> Result<BarsCells, LedgerError> {
        let cells = BarsCells::register(&self.cache, params)?;
        let task = BarsTask::new(feed.clone(), params, cells.clone())?;
        self.tasks.push(Box::new(task));
        Ok(cells)
    }

    pub async fn start(self) -> Result<LedgerSessionHandle, LedgerError> {
        let (worker, runtime) = RuntimeWorker::new(self.cache);
        let worker = tokio::spawn(worker.run());
        let session_owner = session_owner()?;
        let writes = runtime.external_write_sink();
        let mut batch = ExternalWriteBatch::new(session_owner.clone());
        batch.set_value(&self.clock_key, ClockState::initial());
        writes.submit(batch).await?;

        for task in self.tasks {
            runtime.install_boxed_task(task).await?;
        }

        for feed in self.feeds {
            runtime.install_boxed_process(feed).await?;
        }

        Ok(LedgerSessionHandle {
            runtime,
            worker,
            session_owner,
            clock_key: self.clock_key,
            writes,
        })
    }
}

pub struct LedgerSessionHandle {
    runtime: RuntimeHandle,
    worker: JoinHandle<Result<(), runtime::RuntimeError>>,
    session_owner: CellOwner,
    clock_key: ValueKey<ClockState>,
    writes: ExternalWriteSink,
}

impl LedgerSessionHandle {
    pub fn cache(&self) -> &Cache {
        self.runtime.cache()
    }

    pub fn runtime(&self) -> &RuntimeHandle {
        &self.runtime
    }

    pub fn clock_snapshot(&self) -> Result<ClockSnapshot, LedgerError> {
        Ok(self.current_clock()?.snapshot())
    }

    pub async fn play(&self) -> Result<(), LedgerError> {
        self.write_clock(self.current_clock()?.play()).await
    }

    pub async fn pause(&self) -> Result<(), LedgerError> {
        self.write_clock(self.current_clock()?.pause()).await
    }

    pub async fn set_speed(&self, speed: f64) -> Result<(), LedgerError> {
        let next = self.current_clock()?.with_speed(speed)?;
        self.write_clock(next).await
    }

    pub async fn seek_to(&self, session_ns: u64) -> Result<(), LedgerError> {
        self.write_clock(self.current_clock()?.seek_to(session_ns))
            .await
    }

    pub async fn shutdown(self) -> Result<(), LedgerError> {
        let shutdown = self.runtime.shutdown().await;
        let worker = self.worker.await.map_err(|err| {
            LedgerError::Store(anyhow::anyhow!("runtime worker join failed: {err}"))
        })?;

        match worker {
            Ok(()) => shutdown.map_err(LedgerError::from),
            Err(error) => Err(LedgerError::Runtime(error)),
        }
    }

    fn current_clock(&self) -> Result<ClockState, LedgerError> {
        Ok(self
            .runtime
            .cache()
            .read_value(&self.clock_key)?
            .unwrap_or_else(ClockState::initial))
    }

    async fn write_clock(&self, next: ClockState) -> Result<(), LedgerError> {
        let mut batch = ExternalWriteBatch::new(self.session_owner.clone());
        batch.set_value(&self.clock_key, next);
        self.writes.submit(batch).await?;
        Ok(())
    }
}

fn session_owner() -> Result<CellOwner, LedgerError> {
    Ok(CellOwner::new("session")?)
}
