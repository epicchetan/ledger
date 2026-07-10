use std::{
    collections::{HashMap, HashSet, VecDeque},
    num::NonZeroU16,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Duration,
};

use async_trait::async_trait;
use cache::{Key, ValueKey};
use runtime::RuntimeHandle;
use tokio::{
    sync::{mpsc, oneshot},
    task::JoinHandle,
    time::Instant,
};

use crate::{
    clock::ClockState,
    feed::es_replay::{EsReplayCells, EsReplayCursor},
    projection::{Bar, BarsCells, BarsStatus},
};

const DELIVERY_COMMAND_BUFFER: usize = 128;
const DELIVERY_CHANGE_BUFFER: usize = 1024;
const DELIVERY_OUTPUT_BUFFER: usize = 128;
const DELIVERY_TICK: Duration = Duration::from_millis(25);
const LEASE_DURATION: Duration = Duration::from_secs(30);
const WATERMARK_INTERVAL: Duration = Duration::from_secs(1);
const MAX_ISSUED_HEADS: usize = 1024;

static NEXT_SESSION_GENERATION: AtomicU64 = AtomicU64::new(1);

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct ProjectionDeliveryMetricsSnapshot {
    pub dirty_notifications: u64,
    pub coalesced_dirty_notifications: u64,
    pub atomic_collects: u64,
    pub frames_admitted: u64,
    pub snapshot_frames: u64,
    pub suffix_frames: u64,
    pub frames_suppressed_during_seek: u64,
    pub outbound_backpressure: u64,
    pub seek_barriers_completed: u64,
    pub total_seek_barrier_ns: u64,
    pub subscriptions: u64,
    pub resyncs: u64,
    pub lease_expirations: u64,
    pub schema_rejections: u64,
}

#[derive(Clone, Default)]
struct ProjectionDeliveryMetrics {
    inner: Arc<ProjectionDeliveryMetricsInner>,
}

#[derive(Default)]
struct ProjectionDeliveryMetricsInner {
    dirty_notifications: AtomicU64,
    coalesced_dirty_notifications: AtomicU64,
    atomic_collects: AtomicU64,
    frames_admitted: AtomicU64,
    snapshot_frames: AtomicU64,
    suffix_frames: AtomicU64,
    frames_suppressed_during_seek: AtomicU64,
    outbound_backpressure: AtomicU64,
    seek_barriers_completed: AtomicU64,
    total_seek_barrier_ns: AtomicU64,
    subscriptions: AtomicU64,
    resyncs: AtomicU64,
    lease_expirations: AtomicU64,
    schema_rejections: AtomicU64,
}

impl ProjectionDeliveryMetrics {
    fn snapshot(&self) -> ProjectionDeliveryMetricsSnapshot {
        let load = |value: &AtomicU64| value.load(Ordering::Relaxed);
        ProjectionDeliveryMetricsSnapshot {
            dirty_notifications: load(&self.inner.dirty_notifications),
            coalesced_dirty_notifications: load(&self.inner.coalesced_dirty_notifications),
            atomic_collects: load(&self.inner.atomic_collects),
            frames_admitted: load(&self.inner.frames_admitted),
            snapshot_frames: load(&self.inner.snapshot_frames),
            suffix_frames: load(&self.inner.suffix_frames),
            frames_suppressed_during_seek: load(&self.inner.frames_suppressed_during_seek),
            outbound_backpressure: load(&self.inner.outbound_backpressure),
            seek_barriers_completed: load(&self.inner.seek_barriers_completed),
            total_seek_barrier_ns: load(&self.inner.total_seek_barrier_ns),
            subscriptions: load(&self.inner.subscriptions),
            resyncs: load(&self.inner.resyncs),
            lease_expirations: load(&self.inner.lease_expirations),
            schema_rejections: load(&self.inner.schema_rejections),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProjectionDeliverySemantics {
    ReplaceLatest,
    AppendOrdered,
    AppendOrderedWithLatest,
    PatchByKey,
    Snapshot,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SeekDeliveryPolicy {
    EmitIntermediate,
    FinalSnapshotOnly,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectionDeliveryDescriptor {
    pub spec: String,
    pub kind: &'static str,
    pub schema_version: u16,
    pub semantics: ProjectionDeliverySemantics,
    pub default_max_fps: NonZeroU16,
    pub max_useful_fps: NonZeroU16,
    pub seek_policy: SeekDeliveryPolicy,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BarsDeliveryPosition {
    pub session_generation: u64,
    pub epoch: u64,
    pub projection_revision: u64,
    pub processed_batches: usize,
    pub completed_bars: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProjectionPosition {
    Bars(BarsDeliveryPosition),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProjectionFrameOperation {
    Snapshot,
    Append,
    Replace,
    Patch,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProjectionFrameReason {
    Initial,
    Cadence,
    Resync,
    SeekFinal,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BarsDeliveryPayload {
    pub bars: Vec<Bar>,
    pub live: Option<Bar>,
    pub status: BarsStatus,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProjectionFramePayload {
    Bars(BarsDeliveryPayload),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectionDeliveryFrame {
    pub subscription_id: String,
    pub session_generation: u64,
    pub spec: String,
    pub kind: &'static str,
    pub schema_version: u16,
    pub frame_sequence: u64,
    pub base: Option<ProjectionPosition>,
    pub head: ProjectionPosition,
    pub operation: ProjectionFrameOperation,
    pub reason: ProjectionFrameReason,
    pub payload: ProjectionFramePayload,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectionWatermarkEntry {
    pub spec: String,
    pub position: ProjectionPosition,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectionWatermark {
    pub subscription_id: String,
    pub session_generation: u64,
    pub feed: Option<EsReplayCursor>,
    pub projections: Vec<ProjectionWatermarkEntry>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ProjectionDeliveryEvent {
    Frame(Box<ProjectionDeliveryFrame>),
    Watermark(ProjectionWatermark),
    SubscriptionExpired { subscription_id: String },
}

#[derive(Debug, Clone)]
pub struct ProjectionSubscriptionRequest {
    pub consumer_instance_id: String,
    pub projections: Vec<ProjectionSubscriptionProjectionRequest>,
}

#[derive(Debug, Clone)]
pub struct ProjectionSubscriptionProjectionRequest {
    pub spec: String,
    pub schema_versions: Vec<u16>,
    pub requested_max_fps: Option<u16>,
    pub have: Option<ProjectionPosition>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectionSubscriptionResponse {
    pub subscription_id: String,
    pub session_generation: u64,
    pub lease_ms: u64,
    pub projections: Vec<ProjectionSubscriptionProjectionResponse>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectionSubscriptionProjectionResponse {
    pub spec: String,
    pub kind: &'static str,
    pub schema_version: u16,
    pub semantics: ProjectionDeliverySemantics,
    pub effective_max_fps: u16,
    pub resume: ProjectionResumeMode,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ProjectionResumeMode {
    Snapshot,
    Suffix,
}

#[derive(Debug, Clone)]
pub struct AppliedProjectionPosition {
    pub spec: String,
    pub head: ProjectionPosition,
}

#[derive(Debug, Clone)]
pub struct ProjectionDemand {
    pub active: bool,
    pub requested_max_fps: Option<u16>,
}

#[derive(Debug, thiserror::Error)]
pub enum ProjectionDeliveryError {
    #[error("projection delivery executor stopped")]
    Stopped,
    #[error("unknown projection `{0}`")]
    UnknownProjection(String),
    #[error("duplicate projection `{0}`")]
    DuplicateProjection(String),
    #[error("unsupported schema for projection `{0}`")]
    UnsupportedSchema(String),
    #[error("invalid projection cadence `{0}`")]
    InvalidCadence(u16),
    #[error("unknown projection subscription `{0}`")]
    UnknownSubscription(String),
    #[error("invalid acknowledgment for projection `{0}`")]
    InvalidAcknowledgment(String),
    #[error("projection `{0}` state is unavailable")]
    ProjectionUnavailable(String),
    #[error(transparent)]
    Runtime(#[from] runtime::RuntimeError),
    #[error(transparent)]
    Cache(#[from] cache::CacheError),
}

pub struct InstalledProjection {
    pub spec: super::ProjectionSpec,
    pub task: Box<dyn runtime::RuntimeTask>,
    pub delivery: Box<dyn ProjectionDeliverySource>,
}

#[derive(Debug, Clone)]
pub enum ProjectionConvergenceKey {
    Bars(ValueKey<BarsStatus>),
}

#[async_trait]
pub trait ProjectionDeliverySource: Send + 'static {
    fn descriptor(&self) -> &ProjectionDeliveryDescriptor;
    fn watch_keys(&self) -> &[Key];
    fn convergence_key(&self) -> ProjectionConvergenceKey;

    async fn collect(
        &mut self,
        runtime: &RuntimeHandle,
        session_generation: u64,
        base: Option<&ProjectionPosition>,
    ) -> Result<CollectedProjectionFrame, ProjectionDeliveryError>;
}

pub struct BarsDeliverySource {
    descriptor: ProjectionDeliveryDescriptor,
    cells: BarsCells,
    watch_keys: Vec<Key>,
}

impl BarsDeliverySource {
    pub fn new(spec: String, cells: BarsCells) -> Self {
        Self {
            descriptor: ProjectionDeliveryDescriptor {
                spec,
                kind: "bars",
                schema_version: 1,
                semantics: ProjectionDeliverySemantics::AppendOrderedWithLatest,
                default_max_fps: NonZeroU16::new(10).expect("non-zero bars FPS"),
                max_useful_fps: NonZeroU16::new(20).expect("non-zero bars FPS"),
                seek_policy: SeekDeliveryPolicy::FinalSnapshotOnly,
            },
            watch_keys: vec![cells.status.key().clone()],
            cells,
        }
    }
}

#[async_trait]
impl ProjectionDeliverySource for BarsDeliverySource {
    fn descriptor(&self) -> &ProjectionDeliveryDescriptor {
        &self.descriptor
    }

    fn watch_keys(&self) -> &[Key] {
        &self.watch_keys
    }

    fn convergence_key(&self) -> ProjectionConvergenceKey {
        ProjectionConvergenceKey::Bars(self.cells.status.clone())
    }

    async fn collect(
        &mut self,
        runtime: &RuntimeHandle,
        session_generation: u64,
        base: Option<&ProjectionPosition>,
    ) -> Result<CollectedProjectionFrame, ProjectionDeliveryError> {
        let cells = self.cells.clone();
        let requested_base = base.cloned();
        let snapshot = runtime
            .snapshot(move |view| {
                let status = view.read_value(&cells.status)?;
                let Some(status) = status else {
                    return Ok(None);
                };
                let valid_base = match requested_base.as_ref() {
                    Some(ProjectionPosition::Bars(position)) => {
                        position.session_generation == session_generation
                            && position.epoch == status.epoch
                            && position.projection_revision <= status.revision
                            && position.processed_batches <= status.processed_batches
                            && position.completed_bars <= status.completed_bars
                    }
                    None => false,
                };
                let from = if valid_base {
                    match requested_base.as_ref() {
                        Some(ProjectionPosition::Bars(position)) => position.completed_bars,
                        None => 0,
                    }
                } else {
                    0
                };
                let bars = view.read_array_range(&cells.bars, from..status.completed_bars)?;
                let live = view.read_value(&cells.live)?;
                Ok(Some((status, bars, live, valid_base)))
            })
            .await?;
        let Some((status, bars, live, valid_base)) = snapshot else {
            return Err(ProjectionDeliveryError::ProjectionUnavailable(
                self.descriptor.spec.clone(),
            ));
        };
        let head = ProjectionPosition::Bars(position_from_status(session_generation, &status));
        Ok(CollectedProjectionFrame {
            base: valid_base.then_some(base.cloned()).flatten(),
            head,
            operation: if valid_base {
                ProjectionFrameOperation::Append
            } else {
                ProjectionFrameOperation::Snapshot
            },
            payload: ProjectionFramePayload::Bars(BarsDeliveryPayload { bars, live, status }),
        })
    }
}

pub(crate) fn next_session_generation() -> u64 {
    NEXT_SESSION_GENERATION.fetch_add(1, Ordering::Relaxed)
}

pub(crate) fn start_projection_delivery(
    runtime: RuntimeHandle,
    session_generation: u64,
    clock_key: ValueKey<ClockState>,
    feed: EsReplayCells,
    sources: Vec<Box<dyn ProjectionDeliverySource>>,
) -> (
    ProjectionDeliveryHandle,
    mpsc::Receiver<ProjectionDeliveryEvent>,
    JoinHandle<Result<(), ProjectionDeliveryError>>,
) {
    let (command_tx, command_rx) = mpsc::channel(DELIVERY_COMMAND_BUFFER);
    let (change_tx, change_rx) = mpsc::channel(DELIVERY_CHANGE_BUFFER);
    let (output_tx, output_rx) = mpsc::channel(DELIVERY_OUTPUT_BUFFER);
    let metrics = ProjectionDeliveryMetrics::default();
    let mut source_records = HashMap::new();
    let mut watcher_tasks = Vec::new();

    for source in sources {
        let spec = source.descriptor().spec.clone();
        for key in source.watch_keys() {
            if let Ok(watch) = runtime.cache().watch_key(key) {
                watcher_tasks.push(spawn_change_watch(
                    watch,
                    change_tx.clone(),
                    Some(spec.clone()),
                ));
            }
        }
        source_records.insert(spec, SourceRecord { source });
    }
    if let Ok(watch) = runtime.cache().watch_key(clock_key.key()) {
        watcher_tasks.push(spawn_change_watch(watch, change_tx.clone(), None));
    }
    if let Ok(watch) = runtime.cache().watch_key(feed.cursor.key()) {
        watcher_tasks.push(spawn_change_watch(watch, change_tx, None));
    }

    let executor = ProjectionDeliveryExecutor {
        runtime,
        session_generation,
        clock_key,
        feed,
        sources: source_records,
        consumers: HashMap::new(),
        command_rx,
        change_rx,
        output_tx,
        watcher_tasks,
        pending_expirations: VecDeque::new(),
        next_subscription_id: 1,
        seek: None,
        last_watermark_at: Instant::now(),
        metrics: metrics.clone(),
    };
    let join = tokio::spawn(executor.run());
    (
        ProjectionDeliveryHandle {
            commands: command_tx,
            session_generation,
            metrics,
        },
        output_rx,
        join,
    )
}

#[derive(Clone)]
pub struct ProjectionDeliveryHandle {
    commands: mpsc::Sender<DeliveryCommand>,
    session_generation: u64,
    metrics: ProjectionDeliveryMetrics,
}

impl ProjectionDeliveryHandle {
    pub fn session_generation(&self) -> u64 {
        self.session_generation
    }

    pub fn metrics(&self) -> ProjectionDeliveryMetricsSnapshot {
        self.metrics.snapshot()
    }

    pub async fn subscribe(
        &self,
        request: ProjectionSubscriptionRequest,
    ) -> Result<ProjectionSubscriptionResponse, ProjectionDeliveryError> {
        let (reply, rx) = oneshot::channel();
        self.send(DeliveryCommand::Subscribe { request, reply })
            .await?;
        rx.await.map_err(|_| ProjectionDeliveryError::Stopped)?
    }

    pub async fn acknowledge(
        &self,
        subscription_id: String,
        applied: Vec<AppliedProjectionPosition>,
    ) -> Result<(), ProjectionDeliveryError> {
        let (reply, rx) = oneshot::channel();
        self.send(DeliveryCommand::Acknowledge {
            subscription_id,
            applied,
            reply,
        })
        .await?;
        rx.await.map_err(|_| ProjectionDeliveryError::Stopped)?
    }

    pub async fn demand(
        &self,
        subscription_id: String,
        demand: ProjectionDemand,
    ) -> Result<(), ProjectionDeliveryError> {
        let (reply, rx) = oneshot::channel();
        self.send(DeliveryCommand::Demand {
            subscription_id,
            demand,
            reply,
        })
        .await?;
        rx.await.map_err(|_| ProjectionDeliveryError::Stopped)?
    }

    pub async fn resync(
        &self,
        subscription_id: String,
        applied: Vec<AppliedProjectionPosition>,
        reason: String,
    ) -> Result<(), ProjectionDeliveryError> {
        let (reply, rx) = oneshot::channel();
        self.send(DeliveryCommand::Resync {
            subscription_id,
            applied,
            reason,
            reply,
        })
        .await?;
        rx.await.map_err(|_| ProjectionDeliveryError::Stopped)?
    }

    pub async fn begin_seek(
        &self,
        expected_clock_revision: u64,
        target_session_ns: u64,
        required_specs: Vec<String>,
    ) -> Result<(), ProjectionDeliveryError> {
        let (reply, rx) = oneshot::channel();
        self.send(DeliveryCommand::BeginSeek {
            expected_clock_revision,
            target_session_ns,
            required_specs,
            reply,
        })
        .await?;
        rx.await.map_err(|_| ProjectionDeliveryError::Stopped)?
    }

    pub async fn cancel_seek(
        &self,
        expected_clock_revision: u64,
    ) -> Result<(), ProjectionDeliveryError> {
        let (reply, rx) = oneshot::channel();
        self.send(DeliveryCommand::CancelSeek {
            expected_clock_revision,
            reply,
        })
        .await?;
        rx.await.map_err(|_| ProjectionDeliveryError::Stopped)?
    }

    pub async fn shutdown(&self) -> Result<(), ProjectionDeliveryError> {
        let (reply, rx) = oneshot::channel();
        self.send(DeliveryCommand::Shutdown { reply }).await?;
        rx.await.map_err(|_| ProjectionDeliveryError::Stopped)
    }

    async fn send(&self, command: DeliveryCommand) -> Result<(), ProjectionDeliveryError> {
        self.commands
            .send(command)
            .await
            .map_err(|_| ProjectionDeliveryError::Stopped)
    }
}

enum DeliveryCommand {
    Subscribe {
        request: ProjectionSubscriptionRequest,
        reply: oneshot::Sender<Result<ProjectionSubscriptionResponse, ProjectionDeliveryError>>,
    },
    Acknowledge {
        subscription_id: String,
        applied: Vec<AppliedProjectionPosition>,
        reply: oneshot::Sender<Result<(), ProjectionDeliveryError>>,
    },
    Demand {
        subscription_id: String,
        demand: ProjectionDemand,
        reply: oneshot::Sender<Result<(), ProjectionDeliveryError>>,
    },
    Resync {
        subscription_id: String,
        applied: Vec<AppliedProjectionPosition>,
        reason: String,
        reply: oneshot::Sender<Result<(), ProjectionDeliveryError>>,
    },
    BeginSeek {
        expected_clock_revision: u64,
        target_session_ns: u64,
        required_specs: Vec<String>,
        reply: oneshot::Sender<Result<(), ProjectionDeliveryError>>,
    },
    CancelSeek {
        expected_clock_revision: u64,
        reply: oneshot::Sender<Result<(), ProjectionDeliveryError>>,
    },
    Shutdown {
        reply: oneshot::Sender<()>,
    },
}

struct SourceRecord {
    source: Box<dyn ProjectionDeliverySource>,
}

struct ConsumerState {
    _consumer_instance_id: String,
    active: bool,
    lease_deadline: Instant,
    projections: HashMap<String, ConsumerProjectionState>,
}

struct ConsumerProjectionState {
    schema_version: u16,
    effective_max_fps: NonZeroU16,
    sent_head: Option<ProjectionPosition>,
    acknowledged_head: Option<ProjectionPosition>,
    available_head: Option<ProjectionPosition>,
    issued_heads: VecDeque<ProjectionPosition>,
    dirty: bool,
    pending_reason: Option<ProjectionFrameReason>,
    last_frame_at: Option<Instant>,
    frame_sequence: u64,
}

struct SeekBarrier {
    expected_clock_revision: u64,
    _target_session_ns: u64,
    required_specs: Vec<String>,
    started_at: Instant,
}

struct DeliveryChange {
    spec: Option<String>,
}

pub struct CollectedProjectionFrame {
    base: Option<ProjectionPosition>,
    head: ProjectionPosition,
    operation: ProjectionFrameOperation,
    payload: ProjectionFramePayload,
}

struct ProjectionDeliveryExecutor {
    runtime: RuntimeHandle,
    session_generation: u64,
    clock_key: ValueKey<ClockState>,
    feed: EsReplayCells,
    sources: HashMap<String, SourceRecord>,
    consumers: HashMap<String, ConsumerState>,
    command_rx: mpsc::Receiver<DeliveryCommand>,
    change_rx: mpsc::Receiver<DeliveryChange>,
    output_tx: mpsc::Sender<ProjectionDeliveryEvent>,
    watcher_tasks: Vec<JoinHandle<()>>,
    pending_expirations: VecDeque<String>,
    next_subscription_id: u64,
    seek: Option<SeekBarrier>,
    last_watermark_at: Instant,
    metrics: ProjectionDeliveryMetrics,
}

impl ProjectionDeliveryExecutor {
    async fn run(mut self) -> Result<(), ProjectionDeliveryError> {
        let mut tick = tokio::time::interval(DELIVERY_TICK);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                biased;
                command = self.command_rx.recv() => {
                    let Some(command) = command else { break };
                    if self.handle_command(command).await? {
                        break;
                    }
                }
                _ = tick.tick() => {
                    self.handle_tick().await?;
                }
                change = self.change_rx.recv() => {
                    let Some(change) = change else { break };
                    self.handle_change(change);
                }
            }
        }
        for task in &self.watcher_tasks {
            task.abort();
        }
        Ok(())
    }

    async fn handle_command(
        &mut self,
        command: DeliveryCommand,
    ) -> Result<bool, ProjectionDeliveryError> {
        match command {
            DeliveryCommand::Subscribe { request, reply } => {
                let result = self.subscribe(request);
                let _ = reply.send(result);
            }
            DeliveryCommand::Acknowledge {
                subscription_id,
                applied,
                reply,
            } => {
                let result = self.acknowledge(&subscription_id, applied);
                let _ = reply.send(result);
            }
            DeliveryCommand::Demand {
                subscription_id,
                demand,
                reply,
            } => {
                let result = self.set_demand(&subscription_id, demand);
                let _ = reply.send(result);
            }
            DeliveryCommand::Resync {
                subscription_id,
                applied,
                reason,
                reply,
            } => {
                let result = self.resync(&subscription_id, applied, reason).await;
                let _ = reply.send(result);
            }
            DeliveryCommand::BeginSeek {
                expected_clock_revision,
                target_session_ns,
                required_specs,
                reply,
            } => {
                let result =
                    self.begin_seek(expected_clock_revision, target_session_ns, required_specs);
                let _ = reply.send(result);
            }
            DeliveryCommand::CancelSeek {
                expected_clock_revision,
                reply,
            } => {
                if self
                    .seek
                    .as_ref()
                    .is_some_and(|seek| seek.expected_clock_revision == expected_clock_revision)
                {
                    self.seek = None;
                }
                let _ = reply.send(Ok(()));
            }
            DeliveryCommand::Shutdown { reply } => {
                let _ = reply.send(());
                return Ok(true);
            }
        }
        Ok(false)
    }

    fn subscribe(
        &mut self,
        request: ProjectionSubscriptionRequest,
    ) -> Result<ProjectionSubscriptionResponse, ProjectionDeliveryError> {
        let mut seen = HashSet::new();
        let mut projection_states = HashMap::new();
        let mut response_projections = Vec::new();
        for projection in request.projections {
            if !seen.insert(projection.spec.clone()) {
                return Err(ProjectionDeliveryError::DuplicateProjection(
                    projection.spec,
                ));
            }
            let source = self.sources.get(&projection.spec).ok_or_else(|| {
                ProjectionDeliveryError::UnknownProjection(projection.spec.clone())
            })?;
            let descriptor = source.source.descriptor();
            if !projection
                .schema_versions
                .contains(&descriptor.schema_version)
            {
                self.metrics
                    .inner
                    .schema_rejections
                    .fetch_add(1, Ordering::Relaxed);
                return Err(ProjectionDeliveryError::UnsupportedSchema(projection.spec));
            }
            let effective_max_fps = effective_fps(descriptor, projection.requested_max_fps)?;
            let resume = if projection.have.is_some() {
                ProjectionResumeMode::Suffix
            } else {
                ProjectionResumeMode::Snapshot
            };
            response_projections.push(ProjectionSubscriptionProjectionResponse {
                spec: projection.spec.clone(),
                kind: descriptor.kind,
                schema_version: descriptor.schema_version,
                semantics: descriptor.semantics,
                effective_max_fps: effective_max_fps.get(),
                resume,
            });
            projection_states.insert(
                projection.spec,
                ConsumerProjectionState {
                    schema_version: descriptor.schema_version,
                    effective_max_fps,
                    sent_head: projection.have.clone(),
                    // `have` remains an untrusted resume hint until the
                    // source validates it against one atomic cache snapshot.
                    acknowledged_head: None,
                    available_head: None,
                    issued_heads: VecDeque::new(),
                    dirty: true,
                    pending_reason: Some(ProjectionFrameReason::Initial),
                    // Give the RPC response a full cadence interval to reach
                    // the consumer before its first asynchronous frame.
                    last_frame_at: Some(Instant::now()),
                    frame_sequence: 0,
                },
            );
        }

        let subscription_id = format!(
            "projection-{}-{}",
            self.session_generation, self.next_subscription_id
        );
        self.next_subscription_id = self.next_subscription_id.saturating_add(1);
        self.consumers.insert(
            subscription_id.clone(),
            ConsumerState {
                _consumer_instance_id: request.consumer_instance_id,
                active: true,
                lease_deadline: Instant::now() + LEASE_DURATION,
                projections: projection_states,
            },
        );
        self.metrics
            .inner
            .subscriptions
            .fetch_add(1, Ordering::Relaxed);
        Ok(ProjectionSubscriptionResponse {
            subscription_id: subscription_id.clone(),
            session_generation: self.session_generation,
            lease_ms: LEASE_DURATION.as_millis() as u64,
            projections: response_projections,
        })
    }

    fn acknowledge(
        &mut self,
        subscription_id: &str,
        applied: Vec<AppliedProjectionPosition>,
    ) -> Result<(), ProjectionDeliveryError> {
        let consumer = self
            .consumers
            .get_mut(subscription_id)
            .ok_or_else(|| ProjectionDeliveryError::UnknownSubscription(subscription_id.into()))?;
        for item in &applied {
            let projection = consumer
                .projections
                .get(&item.spec)
                .ok_or_else(|| ProjectionDeliveryError::UnknownProjection(item.spec.clone()))?;
            let monotonic = projection
                .acknowledged_head
                .as_ref()
                .is_none_or(|head| position_at_or_before(head, &item.head));
            let issued = projection.acknowledged_head.as_ref() == Some(&item.head)
                || projection.issued_heads.contains(&item.head);
            if !monotonic || !issued {
                return Err(ProjectionDeliveryError::InvalidAcknowledgment(
                    item.spec.clone(),
                ));
            }
        }
        for item in applied {
            let projection = consumer
                .projections
                .get_mut(&item.spec)
                .expect("ack projection validated");
            projection.acknowledged_head = Some(item.head.clone());
            while projection
                .issued_heads
                .front()
                .is_some_and(|head| position_at_or_before(head, &item.head))
            {
                projection.issued_heads.pop_front();
            }
        }
        consumer.lease_deadline = Instant::now() + LEASE_DURATION;
        Ok(())
    }

    fn set_demand(
        &mut self,
        subscription_id: &str,
        demand: ProjectionDemand,
    ) -> Result<(), ProjectionDeliveryError> {
        let consumer = self
            .consumers
            .get_mut(subscription_id)
            .ok_or_else(|| ProjectionDeliveryError::UnknownSubscription(subscription_id.into()))?;
        consumer.active = demand.active;
        consumer.lease_deadline = Instant::now() + LEASE_DURATION;
        for (spec, projection) in &mut consumer.projections {
            let descriptor = self.sources[spec].source.descriptor();
            projection.effective_max_fps = effective_fps(descriptor, demand.requested_max_fps)?;
            if demand.active {
                projection.dirty = true;
            }
        }
        Ok(())
    }

    async fn resync(
        &mut self,
        subscription_id: &str,
        applied: Vec<AppliedProjectionPosition>,
        _reason: String,
    ) -> Result<(), ProjectionDeliveryError> {
        self.metrics.inner.resyncs.fetch_add(1, Ordering::Relaxed);
        let mut by_spec = applied
            .into_iter()
            .map(|position| (position.spec, position.head))
            .collect::<HashMap<_, _>>();
        let specs = {
            let consumer = self.consumers.get_mut(subscription_id).ok_or_else(|| {
                ProjectionDeliveryError::UnknownSubscription(subscription_id.into())
            })?;
            consumer.lease_deadline = Instant::now() + LEASE_DURATION;
            for (spec, projection) in &mut consumer.projections {
                projection.sent_head = by_spec.remove(spec);
                projection.acknowledged_head = None;
                projection.issued_heads.clear();
                projection.dirty = true;
                projection.pending_reason = Some(ProjectionFrameReason::Resync);
            }
            consumer.projections.keys().cloned().collect::<Vec<_>>()
        };
        for spec in specs {
            self.collect_projection(subscription_id, &spec, ProjectionFrameReason::Resync)
                .await?;
        }
        Ok(())
    }

    fn begin_seek(
        &mut self,
        expected_clock_revision: u64,
        target_session_ns: u64,
        required_specs: Vec<String>,
    ) -> Result<(), ProjectionDeliveryError> {
        for spec in &required_specs {
            if !self.sources.contains_key(spec) {
                return Err(ProjectionDeliveryError::UnknownProjection(spec.clone()));
            }
        }
        self.seek = Some(SeekBarrier {
            expected_clock_revision,
            _target_session_ns: target_session_ns,
            required_specs,
            started_at: Instant::now(),
        });
        Ok(())
    }

    fn handle_change(&mut self, change: DeliveryChange) {
        if let Some(spec) = change.spec {
            self.metrics
                .inner
                .dirty_notifications
                .fetch_add(1, Ordering::Relaxed);
            for consumer in self.consumers.values_mut() {
                if let Some(projection) = consumer.projections.get_mut(&spec) {
                    if projection.dirty {
                        self.metrics
                            .inner
                            .coalesced_dirty_notifications
                            .fetch_add(1, Ordering::Relaxed);
                    }
                    projection.dirty = true;
                }
            }
        }
    }

    async fn handle_tick(&mut self) -> Result<(), ProjectionDeliveryError> {
        let now = Instant::now();
        let expired = self
            .consumers
            .iter()
            .filter(|(_, consumer)| consumer.lease_deadline <= now)
            .map(|(subscription_id, _)| subscription_id.clone())
            .collect::<Vec<_>>();
        for subscription_id in expired {
            self.consumers.remove(&subscription_id);
            self.metrics
                .inner
                .lease_expirations
                .fetch_add(1, Ordering::Relaxed);
            self.pending_expirations.push_back(subscription_id);
        }
        self.flush_expirations();
        self.complete_seek_if_ready().await?;

        let suppress = self.seek.is_some();
        let mut due = Vec::new();
        for (subscription_id, consumer) in &self.consumers {
            if !consumer.active {
                continue;
            }
            for (spec, projection) in &consumer.projections {
                if !projection.dirty {
                    continue;
                }
                let descriptor = self.sources[spec].source.descriptor();
                if suppress && descriptor.seek_policy == SeekDeliveryPolicy::FinalSnapshotOnly {
                    self.metrics
                        .inner
                        .frames_suppressed_during_seek
                        .fetch_add(1, Ordering::Relaxed);
                    continue;
                }
                let interval =
                    Duration::from_secs_f64(1.0 / f64::from(projection.effective_max_fps.get()));
                if projection
                    .last_frame_at
                    .is_none_or(|last| now.saturating_duration_since(last) >= interval)
                {
                    due.push((
                        subscription_id.clone(),
                        spec.clone(),
                        projection
                            .pending_reason
                            .unwrap_or(ProjectionFrameReason::Cadence),
                    ));
                }
            }
        }
        for (subscription_id, spec, reason) in due {
            self.collect_projection(&subscription_id, &spec, reason)
                .await?;
        }

        if now.saturating_duration_since(self.last_watermark_at) >= WATERMARK_INTERVAL {
            self.emit_watermarks().await?;
            self.last_watermark_at = now;
        }
        Ok(())
    }

    fn flush_expirations(&mut self) {
        while let Some(subscription_id) = self.pending_expirations.pop_front() {
            match self
                .output_tx
                .try_send(ProjectionDeliveryEvent::SubscriptionExpired {
                    subscription_id: subscription_id.clone(),
                }) {
                Ok(()) => {}
                Err(mpsc::error::TrySendError::Full(_)) => {
                    self.pending_expirations.push_front(subscription_id);
                    return;
                }
                Err(mpsc::error::TrySendError::Closed(_)) => {
                    self.pending_expirations.clear();
                    return;
                }
            }
        }
    }

    async fn collect_projection(
        &mut self,
        subscription_id: &str,
        spec: &str,
        reason: ProjectionFrameReason,
    ) -> Result<(), ProjectionDeliveryError> {
        // Admission capacity is known before the expensive owner snapshot.
        // Preserve the exact reason and retry later instead of cloning bars
        // only to discover that the bounded transport queue is already full.
        if self.output_tx.capacity() == 0 {
            self.metrics
                .inner
                .outbound_backpressure
                .fetch_add(1, Ordering::Relaxed);
            let consumer = self.consumers.get_mut(subscription_id).ok_or_else(|| {
                ProjectionDeliveryError::UnknownSubscription(subscription_id.into())
            })?;
            let projection = consumer
                .projections
                .get_mut(spec)
                .ok_or_else(|| ProjectionDeliveryError::UnknownProjection(spec.into()))?;
            projection.dirty = true;
            projection.pending_reason = Some(reason);
            return Ok(());
        }
        let projection = self
            .consumers
            .get(subscription_id)
            .and_then(|consumer| consumer.projections.get(spec))
            .ok_or_else(|| {
                if !self.consumers.contains_key(subscription_id) {
                    ProjectionDeliveryError::UnknownSubscription(subscription_id.into())
                } else {
                    ProjectionDeliveryError::UnknownProjection(spec.into())
                }
            })?;
        let base = projection.sent_head.clone();
        let collected = {
            self.metrics
                .inner
                .atomic_collects
                .fetch_add(1, Ordering::Relaxed);
            let source = self
                .sources
                .get_mut(spec)
                .ok_or_else(|| ProjectionDeliveryError::UnknownProjection(spec.into()))?;
            source
                .source
                .collect(&self.runtime, self.session_generation, base.as_ref())
                .await?
        };
        self.admit_collected(subscription_id, spec, reason, collected)
    }

    fn admit_collected(
        &mut self,
        subscription_id: &str,
        spec: &str,
        reason: ProjectionFrameReason,
        collected: CollectedProjectionFrame,
    ) -> Result<(), ProjectionDeliveryError> {
        let descriptor = self
            .sources
            .get(spec)
            .ok_or_else(|| ProjectionDeliveryError::UnknownProjection(spec.into()))?
            .source
            .descriptor()
            .clone();
        let consumer = self
            .consumers
            .get_mut(subscription_id)
            .ok_or_else(|| ProjectionDeliveryError::UnknownSubscription(subscription_id.into()))?;
        let projection = consumer
            .projections
            .get_mut(spec)
            .ok_or_else(|| ProjectionDeliveryError::UnknownProjection(spec.into()))?;
        let frame_sequence = projection.frame_sequence.saturating_add(1);
        let validated_resume_base = collected.base.clone();
        let frame = ProjectionDeliveryFrame {
            subscription_id: subscription_id.to_string(),
            session_generation: self.session_generation,
            spec: spec.to_string(),
            kind: descriptor.kind,
            schema_version: projection.schema_version,
            frame_sequence,
            base: collected.base,
            head: collected.head.clone(),
            operation: collected.operation,
            reason,
            payload: collected.payload,
        };
        projection.available_head = Some(collected.head.clone());
        if matches!(
            reason,
            ProjectionFrameReason::Initial | ProjectionFrameReason::Resync
        ) {
            projection.acknowledged_head = validated_resume_base;
        }
        match self
            .output_tx
            .try_send(ProjectionDeliveryEvent::Frame(Box::new(frame)))
        {
            Ok(()) => {
                self.metrics
                    .inner
                    .frames_admitted
                    .fetch_add(1, Ordering::Relaxed);
                match collected.operation {
                    ProjectionFrameOperation::Snapshot => {
                        self.metrics
                            .inner
                            .snapshot_frames
                            .fetch_add(1, Ordering::Relaxed);
                    }
                    ProjectionFrameOperation::Append
                    | ProjectionFrameOperation::Replace
                    | ProjectionFrameOperation::Patch => {
                        self.metrics
                            .inner
                            .suffix_frames
                            .fetch_add(1, Ordering::Relaxed);
                    }
                }
                projection.frame_sequence = frame_sequence;
                projection.sent_head = Some(collected.head.clone());
                projection.issued_heads.push_back(collected.head);
                while projection.issued_heads.len() > MAX_ISSUED_HEADS {
                    projection.issued_heads.pop_front();
                }
                projection.dirty = false;
                projection.pending_reason = None;
                projection.last_frame_at = Some(Instant::now());
            }
            Err(mpsc::error::TrySendError::Full(_)) => {
                self.metrics
                    .inner
                    .outbound_backpressure
                    .fetch_add(1, Ordering::Relaxed);
                projection.dirty = true;
                projection.pending_reason = Some(reason);
            }
            Err(mpsc::error::TrySendError::Closed(_)) => {
                // The transport consumer can disappear during session close.
                // Projection truth continues and shutdown remains reachable.
                projection.dirty = false;
            }
        }
        Ok(())
    }

    async fn complete_seek_if_ready(&mut self) -> Result<(), ProjectionDeliveryError> {
        let Some(seek) = self.seek.as_ref() else {
            return Ok(());
        };
        let expected_revision = seek.expected_clock_revision;
        let required_specs = seek.required_specs.clone();
        let clock_key = self.clock_key.clone();
        let cursor_key = self.feed.cursor.clone();
        let feed_status_key = self.feed.status.clone();
        let convergence = required_specs
            .iter()
            .map(|spec| {
                self.sources
                    .get(spec)
                    .expect("seek specs validated")
                    .source
                    .convergence_key()
            })
            .collect::<Vec<_>>();
        let ready = self
            .runtime
            .snapshot(move |view| {
                let clock = view.read_value(&clock_key)?;
                let Some(clock) = clock else {
                    return Ok(false);
                };
                if clock.revision < expected_revision {
                    return Ok(false);
                }
                let feed_status = view.read_value(&feed_status_key)?;
                let Some(feed_status) = feed_status else {
                    return Ok(false);
                };
                let cursor = view.read_value(&cursor_key)?;
                let Some(cursor) = cursor else {
                    return Ok(false);
                };
                if feed_status.clock.revision < expected_revision
                    || feed_status.cursor != cursor
                    || cursor.catching_up
                {
                    return Ok(false);
                }
                for key in convergence {
                    match key {
                        ProjectionConvergenceKey::Bars(key) => {
                            let Some(status) = view.read_value(&key)? else {
                                return Ok(false);
                            };
                            if status.epoch != cursor.epoch
                                || status.processed_batches != cursor.batch_idx
                            {
                                return Ok(false);
                            }
                        }
                    }
                }
                Ok(true)
            })
            .await?;
        if !ready {
            return Ok(());
        }

        let completed_seek = self.seek.take().expect("seek checked above");
        self.metrics
            .inner
            .seek_barriers_completed
            .fetch_add(1, Ordering::Relaxed);
        self.metrics.inner.total_seek_barrier_ns.fetch_add(
            completed_seek
                .started_at
                .elapsed()
                .as_nanos()
                .min(u64::MAX as u128) as u64,
            Ordering::Relaxed,
        );
        let targets = self
            .consumers
            .iter()
            .filter(|(_, consumer)| consumer.active)
            .flat_map(|(subscription_id, consumer)| {
                required_specs
                    .iter()
                    .filter(|spec| consumer.projections.contains_key(*spec))
                    .map(move |spec| (subscription_id.clone(), spec.clone()))
            })
            .collect::<Vec<_>>();
        for (subscription_id, spec) in targets {
            self.collect_projection(&subscription_id, &spec, ProjectionFrameReason::SeekFinal)
                .await?;
        }
        Ok(())
    }

    async fn emit_watermarks(&mut self) -> Result<(), ProjectionDeliveryError> {
        if !self.consumers.values().any(|consumer| consumer.active) {
            return Ok(());
        }
        let cursor_key = self.feed.cursor.clone();
        let keys = self
            .sources
            .iter()
            .map(|(spec, source)| (spec.clone(), source.source.convergence_key()))
            .collect::<Vec<_>>();
        let session_generation = self.session_generation;
        let (feed, projections) = self
            .runtime
            .snapshot(move |view| {
                let feed = view.read_value(&cursor_key)?;
                let mut projections = Vec::with_capacity(keys.len());
                for (spec, key) in keys {
                    match key {
                        ProjectionConvergenceKey::Bars(key) => {
                            if let Some(status) = view.read_value(&key)? {
                                projections.push(ProjectionWatermarkEntry {
                                    spec,
                                    position: ProjectionPosition::Bars(position_from_status(
                                        session_generation,
                                        &status,
                                    )),
                                });
                            }
                        }
                    }
                }
                Ok((feed, projections))
            })
            .await?;
        for (subscription_id, consumer) in &self.consumers {
            if !consumer.active {
                continue;
            }
            let _ =
                self.output_tx
                    .try_send(ProjectionDeliveryEvent::Watermark(ProjectionWatermark {
                        subscription_id: subscription_id.clone(),
                        session_generation: self.session_generation,
                        feed: feed.clone(),
                        projections: projections.clone(),
                    }));
        }
        Ok(())
    }
}

fn spawn_change_watch(
    mut watch: cache::CellWatch,
    changes: mpsc::Sender<DeliveryChange>,
    spec: Option<String>,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        while watch.changed().await.is_ok() {
            if changes
                .send(DeliveryChange { spec: spec.clone() })
                .await
                .is_err()
            {
                return;
            }
        }
    })
}

fn effective_fps(
    descriptor: &ProjectionDeliveryDescriptor,
    requested: Option<u16>,
) -> Result<NonZeroU16, ProjectionDeliveryError> {
    let requested = match requested {
        Some(0) => return Err(ProjectionDeliveryError::InvalidCadence(0)),
        Some(value) => NonZeroU16::new(value).expect("non-zero cadence validated"),
        None => descriptor.default_max_fps,
    };
    Ok(requested.min(descriptor.max_useful_fps))
}

fn position_from_status(session_generation: u64, status: &BarsStatus) -> BarsDeliveryPosition {
    BarsDeliveryPosition {
        session_generation,
        epoch: status.epoch,
        projection_revision: status.revision,
        processed_batches: status.processed_batches,
        completed_bars: status.completed_bars,
    }
}

fn position_at_or_before(left: &ProjectionPosition, right: &ProjectionPosition) -> bool {
    match (left, right) {
        (ProjectionPosition::Bars(left), ProjectionPosition::Bars(right)) => {
            left.session_generation == right.session_generation
                && left.epoch == right.epoch
                && left.projection_revision <= right.projection_revision
                && left.processed_batches <= right.processed_batches
                && left.completed_bars <= right.completed_bars
        }
    }
}
