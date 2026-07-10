use std::sync::Arc;

use cache::{Cache, CacheReader, CellDescriptor, CellKind, CellOwner, Key, ValueKey};
use runtime::{
    ExternalWriteBatch, ExternalWriteSink, RuntimeHandle, RuntimeProcess, RuntimeTask,
    RuntimeWorker,
};
use store::{RemoteStore, Store, StoreObjectId};
use tokio::task::JoinHandle;

use crate::clock::{ClockSnapshot, ClockState};
use crate::feed::es_replay::{EsReplayCells, EsReplayFeed};
use crate::projection::{
    install_bars_projection, next_session_generation, start_projection_delivery, BarsCells,
    BarsParams, ProjectionDeliveryEvent, ProjectionDeliveryHandle, ProjectionDeliverySource,
};
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
    delivery_sources: Vec<Box<dyn ProjectionDeliverySource>>,
    delivery_feed: Option<EsReplayCells>,
    projection_specs: Vec<String>,
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
            delivery_sources: Vec::new(),
            delivery_feed: None,
            projection_specs: Vec::new(),
        })
    }

    /// Setup-only cache access for registering cells before the runtime takes
    /// ownership. Active session handles expose only [`CacheReader`].
    pub fn cache(&self) -> &Cache {
        &self.cache
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
        self.delivery_feed = Some(cells.clone());
        Ok(cells)
    }

    /// Register a time-bars projection over an es_replay feed's cells.
    pub fn bars(
        &mut self,
        feed: &EsReplayCells,
        params: BarsParams,
    ) -> Result<BarsCells, LedgerError> {
        let cells = BarsCells::register(&self.cache, params)?;
        let installed = install_bars_projection(feed.clone(), params, cells.clone())?;
        self.projection_specs.push(installed.spec.canonical());
        self.tasks.push(installed.task);
        self.delivery_sources.push(installed.delivery);
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

        let session_generation = next_session_generation();
        let (delivery, delivery_events, delivery_worker) = match self.delivery_feed {
            Some(feed) if !self.delivery_sources.is_empty() => {
                let (handle, events, worker) = start_projection_delivery(
                    runtime.clone(),
                    session_generation,
                    self.clock_key.clone(),
                    feed,
                    self.delivery_sources,
                );
                (Some(handle), Some(events), Some(worker))
            }
            Some(_) | None => (None, None, None),
        };

        Ok(LedgerSessionHandle {
            runtime,
            worker,
            session_owner,
            clock_key: self.clock_key,
            writes,
            delivery,
            delivery_events: std::sync::Mutex::new(delivery_events),
            delivery_worker,
            projection_specs: self.projection_specs,
            session_generation,
            control: tokio::sync::Mutex::new(()),
        })
    }
}

pub struct LedgerSessionHandle {
    runtime: RuntimeHandle,
    worker: JoinHandle<Result<(), runtime::RuntimeError>>,
    session_owner: CellOwner,
    clock_key: ValueKey<ClockState>,
    writes: ExternalWriteSink,
    delivery: Option<ProjectionDeliveryHandle>,
    delivery_events: std::sync::Mutex<Option<tokio::sync::mpsc::Receiver<ProjectionDeliveryEvent>>>,
    delivery_worker: Option<JoinHandle<Result<(), crate::projection::ProjectionDeliveryError>>>,
    projection_specs: Vec<String>,
    session_generation: u64,
    control: tokio::sync::Mutex<()>,
}

impl LedgerSessionHandle {
    pub fn cache(&self) -> &CacheReader {
        self.runtime.cache()
    }

    pub fn runtime(&self) -> &RuntimeHandle {
        &self.runtime
    }

    pub fn clock_key(&self) -> &ValueKey<ClockState> {
        &self.clock_key
    }

    pub fn session_generation(&self) -> u64 {
        self.session_generation
    }

    pub fn projection_delivery(&self) -> Option<&ProjectionDeliveryHandle> {
        self.delivery.as_ref()
    }

    pub fn take_projection_events(
        &self,
    ) -> Option<tokio::sync::mpsc::Receiver<ProjectionDeliveryEvent>> {
        self.delivery_events
            .lock()
            .expect("projection delivery events lock poisoned")
            .take()
    }

    pub fn clock_snapshot(&self) -> Result<ClockSnapshot, LedgerError> {
        Ok(self.current_clock()?.snapshot())
    }

    pub async fn play(&self) -> Result<(), LedgerError> {
        let _control = self.control.lock().await;
        self.write_clock(self.current_clock()?.play()).await
    }

    pub async fn pause(&self) -> Result<(), LedgerError> {
        let _control = self.control.lock().await;
        self.write_clock(self.current_clock()?.pause()).await
    }

    pub async fn set_speed(&self, speed: f64) -> Result<(), LedgerError> {
        let _control = self.control.lock().await;
        let next = self.current_clock()?.with_speed(speed)?;
        self.write_clock(next).await
    }

    pub async fn seek_to(&self, session_ns: u64) -> Result<(), LedgerError> {
        let _control = self.control.lock().await;
        let next = self.current_clock()?.seek_to(session_ns);
        if let Some(delivery) = &self.delivery {
            delivery
                .begin_seek(next.revision, session_ns, self.projection_specs.clone())
                .await?;
        }
        if let Err(error) = self.write_clock(next.clone()).await {
            if let Some(delivery) = &self.delivery {
                let _ = delivery.cancel_seek(next.revision).await;
            }
            return Err(error);
        }
        Ok(())
    }

    pub async fn shutdown(self) -> Result<(), LedgerError> {
        let delivery_shutdown = match &self.delivery {
            Some(delivery) => delivery.shutdown().await,
            None => Ok(()),
        };
        let runtime_shutdown = self.runtime.shutdown().await;
        let runtime_worker = self.worker.await;
        let delivery_worker = match self.delivery_worker {
            Some(worker) => Some(worker.await),
            None => None,
        };

        let worker = runtime_worker.map_err(|err| {
            LedgerError::Store(anyhow::anyhow!("runtime worker join failed: {err}"))
        })?;
        if let Err(error) = worker {
            return Err(LedgerError::Runtime(error));
        }
        runtime_shutdown?;
        if let Some(result) = delivery_worker {
            let result = result.map_err(|err| {
                LedgerError::Store(anyhow::anyhow!(
                    "projection delivery worker join failed: {err}"
                ))
            })?;
            if let Err(error) = result {
                return Err(LedgerError::ProjectionDelivery(error));
            }
        }
        if let Err(error) = delivery_shutdown {
            // An already-stopped delivery worker is equivalent to a completed
            // shutdown; all runtime/process teardown above still ran.
            if !matches!(error, crate::projection::ProjectionDeliveryError::Stopped) {
                return Err(LedgerError::ProjectionDelivery(error));
            }
        }
        Ok(())
    }

    fn current_clock(&self) -> Result<ClockState, LedgerError> {
        Ok(self
            .runtime
            .cache()
            .read_value(&self.clock_key)?
            .unwrap_or_else(ClockState::initial))
    }

    async fn write_clock(&self, next: ClockState) -> Result<(), LedgerError> {
        let expected_revision = next.revision;
        let mut watch = self.runtime.cache().watch_key(self.clock_key.key())?;
        let mut batch = ExternalWriteBatch::new(self.session_owner.clone());
        batch.set_value(&self.clock_key, next);
        self.writes.submit(batch).await?;
        loop {
            let committed = self
                .runtime
                .cache()
                .read_value(&self.clock_key)?
                .is_some_and(|clock| clock.revision >= expected_revision);
            if committed {
                return Ok(());
            }
            watch
                .changed()
                .await
                .map_err(|_| LedgerError::Runtime(runtime::RuntimeError::RuntimeStopped))?;
        }
    }
}

fn session_owner() -> Result<CellOwner, LedgerError> {
    Ok(CellOwner::new("session")?)
}
