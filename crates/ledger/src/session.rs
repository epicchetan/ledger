use std::collections::HashMap;
use std::sync::Arc;

use cache::{Cache, CacheReader, CellDescriptor, CellKind, CellOwner, Key, ValueKey};
use runtime::{
    ComponentId, ExternalWriteBatch, ExternalWriteSink, RuntimeHandle, RuntimeProcess, RuntimeTask,
    RuntimeWorker,
};
use store::{RemoteStore, Store, StoreObjectId};
use tokio::task::JoinHandle;

use crate::clock::{ClockSnapshot, ClockState};
use crate::feed::es_replay::{EsReplayCells, EsReplayFeed, EsReplayFeedHandle};
use crate::projection::{
    delivery_sources_for_outputs, install_projection_node, next_session_generation,
    public_projection_outputs, start_projection_delivery, ProjectionDeliveryEvent,
    ProjectionDeliveryHandle, ProjectionDeliverySource, ProjectionNodeSpec, ProjectionOutput,
    ProjectionPlan, ProjectionSpec, SessionProjectionOutput,
};
use crate::LedgerError;

#[derive(Clone)]
struct InstalledGraphNode {
    node: ProjectionNodeSpec,
    component_id: ComponentId,
    owned_keys: Vec<Key>,
    output: ProjectionOutput,
}

#[derive(Clone, Default)]
struct InstalledProjectionGraph {
    public_specs: Vec<String>,
    nodes: Vec<InstalledGraphNode>,
    public_outputs: Vec<SessionProjectionOutput>,
}

#[derive(Debug, Clone)]
pub struct SetProjectionsResult {
    pub changed: bool,
    pub epoch: u64,
    pub projections: Vec<SessionProjectionOutput>,
}

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
    replay_feed: Option<EsReplayFeedHandle>,
    projection_specs: Vec<String>,
    projection_nodes: Vec<InstalledGraphNode>,
    projection_outputs: Vec<SessionProjectionOutput>,
    projections_installed: bool,
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
            replay_feed: None,
            projection_specs: Vec::new(),
            projection_nodes: Vec::new(),
            projection_outputs: Vec::new(),
            projections_installed: false,
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
        let (feed, handle) = EsReplayFeed::with_control(
            raw_object_id,
            self.store.clone(),
            self.clock_key.clone(),
            cells.clone(),
        );
        self.feeds.push(Box::new(feed));
        self.delivery_feed = Some(cells.clone());
        self.replay_feed = Some(handle);
        Ok(cells)
    }

    /// Resolve and install the complete immutable projection graph for this
    /// session. Public outputs are returned in request order; internal
    /// dependencies remain private to Ledger.
    pub fn projections(
        &mut self,
        feed: &EsReplayCells,
        requested: &[ProjectionSpec],
    ) -> Result<Vec<SessionProjectionOutput>, LedgerError> {
        if self.projections_installed {
            return Err(LedgerError::ProjectionPlan(
                "session projections have already been installed".to_string(),
            ));
        }

        let plan = ProjectionPlan::resolve(requested)?;
        let mut outputs = HashMap::new();
        for node in plan.nodes() {
            let installed = install_projection_node(&self.cache, feed, node, &outputs, 0)?;
            let graph_node = InstalledGraphNode {
                node: installed.node.clone(),
                component_id: installed.task.descriptor().component.id.clone(),
                owned_keys: installed.output.owned_keys(),
                output: installed.output.clone(),
            };
            let prior = outputs.insert(installed.node.clone(), installed.output);
            if prior.is_some() {
                return Err(LedgerError::ProjectionPlan(format!(
                    "projection node `{}` was installed twice",
                    node.canonical_key()
                )));
            }
            self.tasks.push(installed.task);
            if let Some(delivery) = installed.delivery {
                self.delivery_sources.push(delivery);
            }
            self.projection_nodes.push(graph_node);
        }

        let public = public_projection_outputs(&plan, &outputs)?;
        self.projection_specs = public
            .iter()
            .map(|output| output.canonical_spec().to_string())
            .collect();
        self.projection_outputs = public.clone();
        self.projections_installed = true;
        Ok(public)
    }

    pub async fn start(self) -> Result<LedgerSessionHandle, LedgerError> {
        let graph = InstalledProjectionGraph {
            public_specs: self.projection_specs.clone(),
            nodes: self.projection_nodes.clone(),
            public_outputs: self.projection_outputs.clone(),
        };
        let feed_cells = self.delivery_feed.clone();
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
            Some(feed) => {
                let (handle, events, worker) = start_projection_delivery(
                    runtime.clone(),
                    session_generation,
                    feed,
                    self.delivery_sources,
                );
                (Some(handle), Some(events), Some(worker))
            }
            None => (None, None, None),
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
            replay_feed: self.replay_feed,
            feed_cells,
            graph: std::sync::RwLock::new(graph),
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
    replay_feed: Option<EsReplayFeedHandle>,
    feed_cells: Option<EsReplayCells>,
    graph: std::sync::RwLock<InstalledProjectionGraph>,
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

    pub fn projection_specs(&self) -> Vec<String> {
        self.graph
            .read()
            .expect("projection graph lock poisoned")
            .public_specs
            .clone()
    }

    pub fn projection_outputs(&self) -> Vec<SessionProjectionOutput> {
        self.graph
            .read()
            .expect("projection graph lock poisoned")
            .public_outputs
            .clone()
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
        self.rebuild_locked(next, None).await?;
        Ok(())
    }

    pub async fn set_projections(
        &self,
        requested: Vec<ProjectionSpec>,
    ) -> Result<SetProjectionsResult, LedgerError> {
        let plan = ProjectionPlan::resolve(&requested)?;
        let _control = self.control.lock().await;
        let mut wanted = requested
            .iter()
            .map(ProjectionSpec::canonical)
            .collect::<Vec<_>>();
        let current = self.projection_specs();
        let mut have = current.clone();
        wanted.sort();
        have.sort();
        if wanted == have {
            return Ok(SetProjectionsResult {
                changed: false,
                epoch: self.current_epoch()?,
                projections: self.projection_outputs(),
            });
        }
        let clock = self.current_clock()?;
        let next = clock.seek_to(clock.now_ns());
        let epoch = self.rebuild_locked(next, Some(plan)).await?;
        Ok(SetProjectionsResult {
            changed: true,
            epoch,
            projections: self.projection_outputs(),
        })
    }

    async fn rebuild_locked(
        &self,
        next_clock: ClockState,
        desired_plan: Option<ProjectionPlan>,
    ) -> Result<u64, LedgerError> {
        let feed = self
            .replay_feed
            .as_ref()
            .ok_or_else(|| LedgerError::FeedControl("session has no replay feed".to_string()))?;
        feed.hold().await?;
        self.write_clock(next_clock).await?;
        let epoch = feed.reset().await?;
        self.runtime.drain(usize::MAX).await?;

        if let Some(plan) = desired_plan {
            let delivery = self.delivery.as_ref().ok_or_else(|| {
                LedgerError::ProjectionPlan(
                    "projection delivery is unavailable for graph reconciliation".to_string(),
                )
            })?;
            delivery.suspend_sources().await?;
            let current = self
                .graph
                .read()
                .map_err(|_| LedgerError::ProjectionPlan("graph lock poisoned".to_string()))?
                .clone();
            let desired_nodes = plan.nodes().to_vec();
            let retained = current
                .nodes
                .iter()
                .filter(|node| desired_nodes.contains(&node.node))
                .cloned()
                .map(|node| (node.node.clone(), node))
                .collect::<HashMap<_, _>>();
            let mut obsolete = current
                .nodes
                .iter()
                .filter(|node| !desired_nodes.contains(&node.node))
                .cloned()
                .collect::<Vec<_>>();
            obsolete.reverse();
            let remove = obsolete
                .iter()
                .map(|node| node.component_id.clone())
                .collect::<Vec<_>>();
            let feed_cells = self.feed_cells.clone().ok_or_else(|| {
                LedgerError::ProjectionPlan("session feed cells are unavailable".to_string())
            })?;
            let graph = self
                .runtime
                .reconcile_tasks(remove, move |schema| {
                    for node in &obsolete {
                        schema.unregister_owned(&node.component_id.owner(), &node.owned_keys)?;
                    }

                    let mut outputs = retained
                        .iter()
                        .map(|(node, installed)| (node.clone(), installed.output.clone()))
                        .collect::<HashMap<_, _>>();
                    let mut nodes = Vec::with_capacity(plan.nodes().len());
                    let mut tasks = Vec::new();
                    for node in plan.nodes() {
                        if let Some(installed) = retained.get(node) {
                            nodes.push(installed.clone());
                            continue;
                        }
                        let installed =
                            install_projection_node(schema, &feed_cells, node, &outputs, epoch)
                                .map_err(|error| {
                                    runtime::RuntimeError::GraphReconciliation(error.to_string())
                                })?;
                        let graph_node = InstalledGraphNode {
                            node: installed.node.clone(),
                            component_id: installed.task.descriptor().component.id.clone(),
                            owned_keys: installed.output.owned_keys(),
                            output: installed.output.clone(),
                        };
                        outputs.insert(installed.node, installed.output);
                        tasks.push(installed.task);
                        nodes.push(graph_node);
                    }
                    let public_outputs =
                        public_projection_outputs(&plan, &outputs).map_err(|error| {
                            runtime::RuntimeError::GraphReconciliation(error.to_string())
                        })?;
                    let public_specs = public_outputs
                        .iter()
                        .map(|output| output.canonical_spec().to_string())
                        .collect();
                    Ok((
                        InstalledProjectionGraph {
                            public_specs,
                            nodes,
                            public_outputs,
                        },
                        tasks,
                    ))
                })
                .await?;
            let sources = delivery_sources_for_outputs(&graph.public_outputs);
            delivery.replace_sources(sources).await?;
            *self
                .graph
                .write()
                .map_err(|_| LedgerError::ProjectionPlan("graph lock poisoned".to_string()))? =
                graph;
        }

        feed.release(epoch).await?;
        Ok(epoch)
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

    fn current_epoch(&self) -> Result<u64, LedgerError> {
        let Some(feed) = &self.feed_cells else {
            return Ok(0);
        };
        Ok(self
            .runtime
            .cache()
            .read_value(&feed.cursor)?
            .map(|cursor| cursor.epoch)
            .unwrap_or(0))
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
