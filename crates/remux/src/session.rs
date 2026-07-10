use crate::error::RpcError;
use crate::rpc::{send_notification, OutboundSender};
use ledger::clock::{ClockMode, ClockSnapshot};
use ledger::feed::es_replay::{
    es_replay_component_id, EsReplayCells, EsReplayCursor, EsReplayStatus,
};
use ledger::market::{MarketDay, PriceTicks};
use ledger::projection::{Bar, BarsCells, BarsStatus, ProjectionSpec};
use ledger::session::{LedgerSessionBuilder, LedgerSessionHandle};
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_json::{json, Value};
use std::collections::HashSet;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use store::{RemoteStore, Store, StoreObjectDescriptor, StoreObjectId};
use tokio::sync::Mutex;
use tokio::task::JoinHandle;

pub const SESSION_OPEN_METHOD: &str = "remux/ledger/session/open";
pub const SESSION_ATTACH_METHOD: &str = "remux/ledger/session/attach";
pub const SESSION_CLOSE_METHOD: &str = "remux/ledger/session/close";
pub const SESSION_STATUS_METHOD: &str = "remux/ledger/session/status";
pub const SESSION_PLAY_METHOD: &str = "remux/ledger/session/play";
pub const SESSION_PAUSE_METHOD: &str = "remux/ledger/session/pause";
pub const SESSION_SPEED_METHOD: &str = "remux/ledger/session/speed";
pub const SESSION_SEEK_METHOD: &str = "remux/ledger/session/seek";
pub const SESSION_BARS_METHOD: &str = "remux/ledger/session/bars";

const SESSION_CLOSED_NOTIFICATION: &str = "remux/ledger/session/closed";
const SESSION_CLOCK_NOTIFICATION: &str = "remux/ledger/session/clock";
const SESSION_FEED_NOTIFICATION: &str = "remux/ledger/session/feed";
const SESSION_BARS_FRAME_NOTIFICATION: &str = "remux/ledger/session/barsFrame";

#[derive(Clone)]
pub struct SessionRegistry {
    active: Arc<Mutex<Option<ActiveSession>>>,
    lifecycle: Arc<Mutex<()>>,
    next_id: Arc<AtomicU64>,
}

struct ActiveSession {
    id: String,
    raw_id: StoreObjectId,
    handle: LedgerSessionHandle,
    feed: EsReplayCells,
    projections: Vec<ActiveProjection>,
    watchers: Vec<JoinHandle<()>>,
}

#[derive(Debug, Clone)]
struct ActiveProjection {
    spec: String,
    cells: BarsCells,
}

impl Default for SessionRegistry {
    fn default() -> Self {
        Self {
            active: Arc::new(Mutex::new(None)),
            lifecycle: Arc::new(Mutex::new(())),
            next_id: Arc::new(AtomicU64::new(1)),
        }
    }
}

impl SessionRegistry {
    pub async fn open<S>(
        &self,
        store: &Store<S>,
        output_tx: &OutboundSender,
        params: Value,
    ) -> Result<Value, RpcError>
    where
        S: RemoteStore + 'static,
    {
        let params = parse_params::<SessionOpenParams>(params)?;
        let projections = parse_projection_specs(params.projections.unwrap_or_default())?;
        let raw_id = parse_object_id(&params.raw_id)?;
        let descriptor = store
            .get_object(&raw_id)?
            .ok_or_else(|| RpcError::object_not_found(&params.raw_id))?;
        // The scrubber needs a fixed time domain and the ES session calendar
        // (DST-sensitive) lives server-side; raws without a market day open
        // fine and just carry null bounds.
        let (market_day, session_bounds) = raw_session_calendar(&descriptor);

        let _lifecycle = self.lifecycle.lock().await;
        let replaced = self.take_active().await;
        let replaced_id = replaced.as_ref().map(|session| session.id.clone());
        if let Some(session) = replaced {
            session.abort_watchers();
            send_session_closed(output_tx, &session.id, "replaced").await;
            if let Err(error) = session.handle.shutdown().await {
                eprintln!("[ledger-remux] replaced session shutdown failed: {error}");
            }
        }

        let mut builder = LedgerSessionBuilder::new(Arc::new(store.clone()))
            .map_err(|err| RpcError::domain(err.to_string()))?;
        let feed = builder
            .es_replay(raw_id.clone())
            .map_err(|err| RpcError::domain(err.to_string()))?;
        let mut active_projections = Vec::new();
        for (canonical, spec) in &projections {
            match spec {
                ProjectionSpec::Bars(params) => {
                    let cells = builder
                        .bars(&feed, *params)
                        .map_err(|err| RpcError::domain(err.to_string()))?;
                    active_projections.push(ActiveProjection {
                        spec: canonical.clone(),
                        cells,
                    });
                }
            }
        }

        let handle = builder
            .start()
            .await
            .map_err(|err| RpcError::domain(err.to_string()))?;
        let session_id = self.allocate_id();
        {
            let mut active = self.active.lock().await;
            *active = Some(ActiveSession {
                id: session_id.clone(),
                raw_id: raw_id.clone(),
                handle,
                feed: feed.clone(),
                projections: active_projections.clone(),
                watchers: Vec::new(),
            });
        }

        let watchers = self.spawn_watchers(
            session_id.clone(),
            output_tx.clone(),
            feed.clone(),
            active_projections.clone(),
        );
        {
            let mut active = self.active.lock().await;
            if let Some(session) = active.as_mut().filter(|session| session.id == session_id) {
                session.watchers = watchers;
            } else {
                for watcher in watchers {
                    watcher.abort();
                }
            }
        }

        to_value(SessionOpenResultDto {
            session_id,
            raw_id: raw_id.to_string(),
            projections: active_projections
                .into_iter()
                .map(|projection| ProjectionDto {
                    spec: projection.spec,
                })
                .collect(),
            replaced: replaced_id,
            market_day: market_day.as_ref().map(MarketDay::to_string),
            session_start_ns: session_bounds.map(|(start, _)| ns_string(start)),
            session_end_ns: session_bounds.map(|(_, end)| ns_string(end)),
        })
    }

    // Reattach a reloaded client to the running session: read-only — never
    // touches the clock, never closes anything. Returns the open-shaped
    // identity plus the current clock and cursor, because the watchers only
    // notify on change and a paused session would otherwise stay silent
    // forever. The exact server-issued id is the reload capability: matching
    // only raw/spec would let a fresh navigation or another client steal the
    // active replay. A stale id or identity mismatch is an expected typed miss;
    // malformed input and cache/store failures remain real RPC errors.
    pub async fn attach<S>(&self, store: &Store<S>, params: Value) -> Result<Value, RpcError>
    where
        S: RemoteStore + 'static,
    {
        let params = parse_params::<SessionAttachParams>(params)?;
        let requested = parse_projection_specs(params.projections.unwrap_or_default())?;
        let raw_id = parse_object_id(&params.raw_id)?;
        let active = self.active.lock().await;
        let Some(session) = active
            .as_ref()
            .filter(|session| session.id == params.session_id && session.raw_id == raw_id)
        else {
            return to_value(SessionAttachResultDto {
                attached: false,
                session: None,
            });
        };
        let have: HashSet<&str> = session
            .projections
            .iter()
            .map(|projection| projection.spec.as_str())
            .collect();
        let want: HashSet<&str> = requested
            .iter()
            .map(|(canonical, _)| canonical.as_str())
            .collect();
        if have != want {
            return to_value(SessionAttachResultDto {
                attached: false,
                session: None,
            });
        }
        let clock = session
            .handle
            .clock_snapshot()
            .map_err(|err| RpcError::domain(err.to_string()))?;
        let cursor = session
            .handle
            .cache()
            .read_value(&session.feed.cursor)
            .map_err(|err| RpcError::domain(err.to_string()))?;
        let descriptor = store
            .get_object(&raw_id)?
            .ok_or_else(|| RpcError::object_not_found(&params.raw_id))?;
        let (market_day, session_bounds) = raw_session_calendar(&descriptor);
        to_value(SessionAttachResultDto {
            attached: true,
            session: Some(SessionAttachSnapshotDto {
                session_id: session.id.clone(),
                raw_id: raw_id.to_string(),
                projections: session
                    .projections
                    .iter()
                    .map(|projection| ProjectionDto {
                        spec: projection.spec.clone(),
                    })
                    .collect(),
                market_day: market_day.as_ref().map(MarketDay::to_string),
                session_start_ns: session_bounds.map(|(start, _)| ns_string(start)),
                session_end_ns: session_bounds.map(|(_, end)| ns_string(end)),
                clock: ClockSnapshotDto::from(clock),
                cursor: cursor.map(EsReplayCursorDto::from),
            }),
        })
    }

    pub async fn close(
        &self,
        output_tx: &OutboundSender,
        params: Value,
    ) -> Result<Value, RpcError> {
        let params = parse_params::<SessionIdParams>(params)?;
        let _lifecycle = self.lifecycle.lock().await;
        let session = self
            .take_matching(&params.session_id)
            .await?
            .ok_or_else(|| unknown_session(&params.session_id))?;
        session.abort_watchers();
        let shutdown = session.handle.shutdown().await;
        send_session_closed(output_tx, &session.id, "closed").await;
        if let Err(error) = shutdown {
            return Err(RpcError::domain(error.to_string()));
        }
        to_value(SessionCloseResultDto { closed: true })
    }

    pub async fn status(&self, params: Value) -> Result<Value, RpcError> {
        let params = parse_params::<SessionIdParams>(params)?;
        let active = self.active.lock().await;
        let session = matching_session(active.as_ref(), &params.session_id)?;
        let clock = session
            .handle
            .clock_snapshot()
            .map_err(|err| RpcError::domain(err.to_string()))?;
        let component_status = session
            .handle
            .runtime()
            .component_status(&es_replay_component_id())
            .await
            .map_err(|err| RpcError::domain(err.to_string()))?;
        let feed_status = session
            .handle
            .cache()
            .read_value(&session.feed.status)
            .map_err(|err| RpcError::domain(err.to_string()))?;
        let feed_cursor = session
            .handle
            .cache()
            .read_value(&session.feed.cursor)
            .map_err(|err| RpcError::domain(err.to_string()))?;
        let mut projections = Vec::new();
        for projection in &session.projections {
            let status = session
                .handle
                .cache()
                .read_value(&projection.cells.status)
                .map_err(|err| RpcError::domain(err.to_string()))?;
            let live_bar = session
                .handle
                .cache()
                .read_value(&projection.cells.live)
                .map_err(|err| RpcError::domain(err.to_string()))?
                .is_some();
            let completed_bars = status
                .as_ref()
                .map(|status| status.completed_bars)
                .unwrap_or_default();
            projections.push(ProjectionStatusDto {
                spec: projection.spec.clone(),
                status: status.map(BarsStatusDto::from),
                completed_bars,
                live_bar,
            });
        }

        to_value(SessionStatusDto {
            session_id: session.id.clone(),
            raw_id: session.raw_id.to_string(),
            clock: ClockSnapshotDto::from(clock),
            feed: FeedStatusDto {
                component_status: component_status_string(component_status),
                status: feed_status.map(EsReplayStatusDto::from),
                cursor: feed_cursor.map(EsReplayCursorDto::from),
            },
            projections,
        })
    }

    pub async fn play(&self, params: Value) -> Result<Value, RpcError> {
        let params = parse_params::<SessionIdParams>(params)?;
        let active = self.active.lock().await;
        let session = matching_session(active.as_ref(), &params.session_id)?;
        session
            .handle
            .play()
            .await
            .map_err(|err| RpcError::domain(err.to_string()))?;
        ok()
    }

    pub async fn pause(&self, params: Value) -> Result<Value, RpcError> {
        let params = parse_params::<SessionIdParams>(params)?;
        let active = self.active.lock().await;
        let session = matching_session(active.as_ref(), &params.session_id)?;
        session
            .handle
            .pause()
            .await
            .map_err(|err| RpcError::domain(err.to_string()))?;
        ok()
    }

    pub async fn speed(&self, params: Value) -> Result<Value, RpcError> {
        let params = parse_params::<SessionSpeedParams>(params)?;
        let active = self.active.lock().await;
        let session = matching_session(active.as_ref(), &params.session_id)?;
        session
            .handle
            .set_speed(params.speed)
            .await
            .map_err(|err| RpcError::domain(err.to_string()))?;
        ok()
    }

    pub async fn seek(&self, params: Value) -> Result<Value, RpcError> {
        let params = parse_params::<SessionSeekParams>(params)?;
        let session_ns = parse_ns_string("sessionNs", &params.session_ns)?;
        let active = self.active.lock().await;
        let session = matching_session(active.as_ref(), &params.session_id)?;
        session
            .handle
            .seek_to(session_ns)
            .await
            .map_err(|err| RpcError::domain(err.to_string()))?;
        ok()
    }

    pub async fn bars(&self, params: Value) -> Result<Value, RpcError> {
        let params = parse_params::<SessionBarsParams>(params)?;
        let from = params.from.unwrap_or_default();
        let canonical = parse_projection_spec(&params.spec)?.0;
        let active = self.active.lock().await;
        let session = matching_session(active.as_ref(), &params.session_id)?;
        let projection = session
            .projections
            .iter()
            .find(|projection| projection.spec == canonical)
            .ok_or_else(|| unknown_projection(&params.spec))?;
        let frame = read_bars_frame(
            &session.handle,
            &session.id,
            &projection.spec,
            &projection.cells,
            from,
        )?;
        to_value(frame)
    }

    async fn take_active(&self) -> Option<ActiveSession> {
        self.active.lock().await.take()
    }

    async fn take_matching(&self, session_id: &str) -> Result<Option<ActiveSession>, RpcError> {
        let mut active = self.active.lock().await;
        match active.as_ref() {
            Some(session) if session.id == session_id => Ok(active.take()),
            Some(_) | None => Err(unknown_session(session_id)),
        }
    }

    fn allocate_id(&self) -> String {
        let next = self.next_id.fetch_add(1, Ordering::SeqCst);
        format!("session-{next}")
    }

    fn spawn_watchers(
        &self,
        session_id: String,
        output_tx: OutboundSender,
        feed: EsReplayCells,
        projections: Vec<ActiveProjection>,
    ) -> Vec<JoinHandle<()>> {
        let mut watchers = Vec::new();
        watchers.push(tokio::spawn(watch_clock(
            self.active.clone(),
            session_id.clone(),
            output_tx.clone(),
        )));
        watchers.push(tokio::spawn(watch_feed(
            self.active.clone(),
            session_id.clone(),
            output_tx.clone(),
            feed,
        )));
        for projection in projections {
            watchers.push(tokio::spawn(watch_bars(
                self.active.clone(),
                session_id.clone(),
                output_tx.clone(),
                projection,
            )));
        }
        watchers
    }
}

impl ActiveSession {
    fn abort_watchers(&self) {
        for watcher in &self.watchers {
            watcher.abort();
        }
    }
}

async fn watch_clock(
    active: Arc<Mutex<Option<ActiveSession>>>,
    session_id: String,
    output_tx: OutboundSender,
) {
    let (cache, clock_key) = {
        let active = active.lock().await;
        match active.as_ref() {
            Some(session) if session.id == session_id => (
                session.handle.cache().clone(),
                session.handle.clock_key().clone(),
            ),
            _ => return,
        }
    };
    let mut watch = match cache.watch_key(clock_key.key()) {
        Ok(watch) => watch,
        Err(error) => {
            eprintln!("[ledger-remux] failed to watch session clock: {error}");
            return;
        }
    };
    let mut last_revision = None;
    loop {
        let snapshot = {
            let active = active.lock().await;
            match active.as_ref() {
                Some(session) if session.id == session_id => session
                    .handle
                    .cache()
                    .read_value(&clock_key)
                    .unwrap_or(None)
                    .map(|clock| clock.snapshot()),
                _ => return,
            }
        };
        if let Some(snapshot) = snapshot {
            if last_revision != Some(snapshot.revision) {
                last_revision = Some(snapshot.revision);
                send_clock(&output_tx, &session_id, snapshot).await;
            }
        }
        if watch.changed().await.is_err() {
            return;
        }
    }
}

async fn watch_feed(
    active: Arc<Mutex<Option<ActiveSession>>>,
    session_id: String,
    output_tx: OutboundSender,
    feed: EsReplayCells,
) {
    let cache = {
        let active = active.lock().await;
        match active.as_ref() {
            Some(session) if session.id == session_id => session.handle.cache().clone(),
            _ => return,
        }
    };
    let mut watch = match cache.watch_key(feed.cursor.key()) {
        Ok(watch) => watch,
        Err(error) => {
            eprintln!("[ledger-remux] failed to watch session feed cursor: {error}");
            return;
        }
    };
    let mut last_sent: Option<EsReplayCursor> = None;
    loop {
        let cursor = {
            let active = active.lock().await;
            match active.as_ref() {
                Some(session) if session.id == session_id => session
                    .handle
                    .cache()
                    .read_value(&feed.cursor)
                    .unwrap_or(None),
                _ => return,
            }
        };
        if let Some(cursor) = cursor {
            if last_sent.as_ref() != Some(&cursor) {
                last_sent = Some(cursor.clone());
                if let Err(error) = send_notification(
                    &output_tx,
                    SESSION_FEED_NOTIFICATION,
                    json!({
                        "sessionId": session_id,
                        "cursor": EsReplayCursorDto::from(cursor),
                    }),
                )
                .await
                {
                    eprintln!("[ledger-remux] failed to broadcast session feed: {error}");
                }
            }
        }
        if watch.changed().await.is_err() {
            return;
        }
    }
}

async fn watch_bars(
    active: Arc<Mutex<Option<ActiveSession>>>,
    session_id: String,
    output_tx: OutboundSender,
    projection: ActiveProjection,
) {
    let cache = {
        let active = active.lock().await;
        match active.as_ref() {
            Some(session) if session.id == session_id => session.handle.cache().clone(),
            _ => return,
        }
    };
    let mut watch = match cache.watch_key(projection.cells.status.key()) {
        Ok(watch) => watch,
        Err(error) => {
            eprintln!("[ledger-remux] failed to watch session bars status: {error}");
            return;
        }
    };
    let mut sent_epoch = None;
    let mut sent_count = 0usize;
    loop {
        let frame = {
            let active = active.lock().await;
            match active.as_ref() {
                Some(session) if session.id == session_id => read_bars_frame_for_watcher(
                    &session.handle,
                    &session_id,
                    &projection.spec,
                    &projection.cells,
                    &mut sent_epoch,
                    &mut sent_count,
                ),
                _ => return,
            }
        };
        match frame {
            Ok(Some(frame)) => {
                if let Err(error) =
                    send_notification(&output_tx, SESSION_BARS_FRAME_NOTIFICATION, json!(frame))
                        .await
                {
                    eprintln!("[ledger-remux] failed to broadcast session bars frame: {error}");
                }
            }
            Ok(None) | Err(()) => {}
        }
        if watch.changed().await.is_err() {
            return;
        }
    }
}

fn read_bars_frame_for_watcher(
    handle: &LedgerSessionHandle,
    session_id: &str,
    spec: &str,
    cells: &BarsCells,
    sent_epoch: &mut Option<u64>,
    sent_count: &mut usize,
) -> Result<Option<BarsFrameDto>, ()> {
    let status = handle.cache().read_value(&cells.status).map_err(|_| ())?;
    let Some(status) = status else {
        return Ok(None);
    };
    if *sent_epoch != Some(status.epoch) {
        *sent_epoch = Some(status.epoch);
        *sent_count = 0;
    }
    let from = *sent_count;
    let bars = if status.completed_bars > from {
        handle
            .cache()
            .read_array_range(&cells.bars, from..status.completed_bars)
            .map_err(|_| ())?
    } else {
        Vec::new()
    };
    let live = handle.cache().read_value(&cells.live).map_err(|_| ())?;
    *sent_count = status.completed_bars;
    Ok(Some(BarsFrameDto {
        session_id: session_id.to_string(),
        spec: spec.to_string(),
        epoch: status.epoch,
        from,
        bars: bars.into_iter().map(BarDto::from).collect(),
        total: status.completed_bars,
        live: live.map(BarDto::from),
        status: BarsStatusDto::from(status),
    }))
}

fn read_bars_frame(
    handle: &LedgerSessionHandle,
    session_id: &str,
    spec: &str,
    cells: &BarsCells,
    from: usize,
) -> Result<BarsFrameDto, RpcError> {
    let status = handle
        .cache()
        .read_value(&cells.status)
        .map_err(|err| RpcError::domain(err.to_string()))?
        .ok_or_else(|| RpcError::domain(format!("projection {spec} status unavailable")))?;
    if from > status.completed_bars {
        return Err(RpcError::invalid_params(format!(
            "from {from} is greater than completed bars {}",
            status.completed_bars
        )));
    }
    let bars = if status.completed_bars > from {
        handle
            .cache()
            .read_array_range(&cells.bars, from..status.completed_bars)
            .map_err(|err| RpcError::domain(err.to_string()))?
    } else {
        Vec::new()
    };
    let live = handle
        .cache()
        .read_value(&cells.live)
        .map_err(|err| RpcError::domain(err.to_string()))?;
    Ok(BarsFrameDto {
        session_id: session_id.to_string(),
        spec: spec.to_string(),
        epoch: status.epoch,
        from,
        bars: bars.into_iter().map(BarDto::from).collect(),
        total: status.completed_bars,
        live: live.map(BarDto::from),
        status: BarsStatusDto::from(status),
    })
}

fn parse_projection_specs(specs: Vec<String>) -> Result<Vec<(String, ProjectionSpec)>, RpcError> {
    let mut seen = HashSet::new();
    let mut parsed = Vec::new();
    for spec in specs {
        let (canonical, projection) = parse_projection_spec(&spec)?;
        if !seen.insert(canonical.clone()) {
            return Err(RpcError::invalid_params(format!(
                "duplicate projection spec {canonical}"
            )));
        }
        parsed.push((canonical, projection));
    }
    Ok(parsed)
}

fn parse_projection_spec(spec: &str) -> Result<(String, ProjectionSpec), RpcError> {
    let projection =
        ProjectionSpec::parse(spec).map_err(|err| RpcError::invalid_params(err.to_string()))?;
    Ok((projection.canonical(), projection))
}

fn matching_session<'a>(
    active: Option<&'a ActiveSession>,
    session_id: &str,
) -> Result<&'a ActiveSession, RpcError> {
    match active {
        Some(session) if session.id == session_id => Ok(session),
        Some(_) | None => Err(unknown_session(session_id)),
    }
}

fn unknown_session(session_id: &str) -> RpcError {
    RpcError::domain(format!("unknown session {session_id}"))
}

// Market day and ES session bounds from a raw's descriptor — open and attach
// both derive them (the active session doesn't retain them).
fn raw_session_calendar(
    descriptor: &StoreObjectDescriptor,
) -> (Option<MarketDay>, Option<(u64, u64)>) {
    let market_day = descriptor
        .metadata_json
        .get("market_day")
        .and_then(Value::as_str)
        .and_then(|value| MarketDay::parse(value).ok());
    let session_bounds = market_day
        .as_ref()
        .and_then(|day| day.es_session_bounds_utc().ok());
    (market_day, session_bounds)
}

fn unknown_projection(spec: &str) -> RpcError {
    RpcError::domain(format!("unknown projection spec {spec}"))
}

async fn send_session_closed(output_tx: &OutboundSender, session_id: &str, reason: &str) {
    if let Err(error) = send_notification(
        output_tx,
        SESSION_CLOSED_NOTIFICATION,
        json!({
            "sessionId": session_id,
            "reason": reason,
        }),
    )
    .await
    {
        eprintln!("[ledger-remux] failed to broadcast session close: {error}");
    }
}

async fn send_clock(output_tx: &OutboundSender, session_id: &str, clock: ClockSnapshot) {
    if let Err(error) = send_notification(
        output_tx,
        SESSION_CLOCK_NOTIFICATION,
        json!({
            "sessionId": session_id,
            "clock": ClockSnapshotDto::from(clock),
        }),
    )
    .await
    {
        eprintln!("[ledger-remux] failed to broadcast session clock: {error}");
    }
}

fn component_status_string(status: impl std::fmt::Debug) -> String {
    let status = format!("{status:?}");
    match status.as_str() {
        "Preparing" => "preparing".to_string(),
        "Ready" => "ready".to_string(),
        "Queued" => "queued".to_string(),
        "Running" => "running".to_string(),
        "Stopping" => "stopping".to_string(),
        "Stopped" => "stopped".to_string(),
        "Completed" => "completed".to_string(),
        _ => status
            .strip_prefix("Failed(\"")
            .and_then(|reason| reason.strip_suffix("\")"))
            .map(|reason| format!("failed: {reason}"))
            .unwrap_or_else(|| status.to_ascii_lowercase()),
    }
}

fn ok() -> Result<Value, RpcError> {
    to_value(OkDto { ok: true })
}

fn parse_params<T: DeserializeOwned>(params: Value) -> Result<T, RpcError> {
    let params = if params.is_null() {
        serde_json::json!({})
    } else {
        params
    };
    serde_json::from_value(params).map_err(|err| RpcError::invalid_params(err.to_string()))
}

fn parse_object_id(value: &str) -> Result<StoreObjectId, RpcError> {
    StoreObjectId::new(value.to_string())
        .map_err(|err| RpcError::invalid_object_id(value, err.to_string()))
}

fn parse_ns_string(field: &str, value: &str) -> Result<u64, RpcError> {
    value
        .parse::<u64>()
        .map_err(|_| RpcError::invalid_params(format!("{field} must be a u64 string")))
}

fn ns_string(value: u64) -> String {
    value.to_string()
}

fn price_ticks(value: PriceTicks) -> i64 {
    value.0
}

fn to_value<T: Serialize>(value: T) -> Result<Value, RpcError> {
    serde_json::to_value(value).map_err(|err| RpcError::domain(err.to_string()))
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct SessionOpenParams {
    raw_id: String,
    projections: Option<Vec<String>>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct SessionAttachParams {
    session_id: String,
    raw_id: String,
    projections: Option<Vec<String>>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct SessionIdParams {
    session_id: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct SessionSpeedParams {
    session_id: String,
    speed: f64,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct SessionSeekParams {
    session_id: String,
    session_ns: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct SessionBarsParams {
    session_id: String,
    spec: String,
    from: Option<usize>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct SessionOpenResultDto {
    session_id: String,
    raw_id: String,
    projections: Vec<ProjectionDto>,
    replaced: Option<String>,
    market_day: Option<String>,
    session_start_ns: Option<String>,
    session_end_ns: Option<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct SessionAttachResultDto {
    attached: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    session: Option<SessionAttachSnapshotDto>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct SessionAttachSnapshotDto {
    session_id: String,
    raw_id: String,
    projections: Vec<ProjectionDto>,
    market_day: Option<String>,
    session_start_ns: Option<String>,
    session_end_ns: Option<String>,
    clock: ClockSnapshotDto,
    cursor: Option<EsReplayCursorDto>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct ProjectionDto {
    spec: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct SessionCloseResultDto {
    closed: bool,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct OkDto {
    ok: bool,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct SessionStatusDto {
    session_id: String,
    raw_id: String,
    clock: ClockSnapshotDto,
    feed: FeedStatusDto,
    projections: Vec<ProjectionStatusDto>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct FeedStatusDto {
    component_status: String,
    status: Option<EsReplayStatusDto>,
    cursor: Option<EsReplayCursorDto>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct ProjectionStatusDto {
    spec: String,
    status: Option<BarsStatusDto>,
    completed_bars: usize,
    live_bar: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ClockSnapshotDto {
    mode: String,
    speed: f64,
    session_now_ns: String,
    revision: u64,
}

impl From<ClockSnapshot> for ClockSnapshotDto {
    fn from(snapshot: ClockSnapshot) -> Self {
        Self {
            mode: match snapshot.mode {
                ClockMode::Paused => "paused",
                ClockMode::Running => "running",
            }
            .to_string(),
            speed: snapshot.speed,
            session_now_ns: ns_string(snapshot.session_now_ns),
            revision: snapshot.revision,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct EsReplayCursorDto {
    epoch: u64,
    feed_seq: u64,
    batch_idx: usize,
    total_batches: usize,
    ts_event_ns: Option<String>,
    next_ts_event_ns: Option<String>,
    catching_up: bool,
    ended: bool,
}

impl From<EsReplayCursor> for EsReplayCursorDto {
    fn from(cursor: EsReplayCursor) -> Self {
        Self {
            epoch: cursor.epoch,
            feed_seq: cursor.feed_seq,
            batch_idx: cursor.batch_idx,
            total_batches: cursor.total_batches,
            ts_event_ns: cursor.ts_event_ns.map(ns_string),
            next_ts_event_ns: cursor.next_ts_event_ns.map(ns_string),
            catching_up: cursor.catching_up,
            ended: cursor.ended,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct EsReplayStatusDto {
    raw_object_id: String,
    artifact_object_id: Option<String>,
    clock: ClockSnapshotDto,
    cursor: EsReplayCursorDto,
}

impl From<EsReplayStatus> for EsReplayStatusDto {
    fn from(status: EsReplayStatus) -> Self {
        Self {
            raw_object_id: status.raw_object_id,
            artifact_object_id: status.artifact_object_id,
            clock: ClockSnapshotDto::from(status.clock),
            cursor: EsReplayCursorDto::from(status.cursor),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BarsStatusDto {
    spec: String,
    epoch: u64,
    processed_batches: usize,
    completed_bars: usize,
    last_ts_event_ns: Option<String>,
}

impl From<BarsStatus> for BarsStatusDto {
    fn from(status: BarsStatus) -> Self {
        Self {
            spec: status.spec,
            epoch: status.epoch,
            processed_batches: status.processed_batches,
            completed_bars: status.completed_bars,
            last_ts_event_ns: status.last_ts_event_ns.map(ns_string),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BarDto {
    interval_start_ns: String,
    open: i64,
    high: i64,
    low: i64,
    close: i64,
    volume: u64,
    buy_volume: u64,
    sell_volume: u64,
    trade_count: u64,
    first_ts_event_ns: String,
    last_ts_event_ns: String,
}

impl From<Bar> for BarDto {
    fn from(bar: Bar) -> Self {
        Self {
            interval_start_ns: ns_string(bar.interval_start_ns),
            open: price_ticks(bar.open),
            high: price_ticks(bar.high),
            low: price_ticks(bar.low),
            close: price_ticks(bar.close),
            volume: bar.volume,
            buy_volume: bar.buy_volume,
            sell_volume: bar.sell_volume,
            trade_count: bar.trade_count,
            first_ts_event_ns: ns_string(bar.first_ts_event_ns),
            last_ts_event_ns: ns_string(bar.last_ts_event_ns),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct BarsFrameDto {
    session_id: String,
    spec: String,
    epoch: u64,
    from: usize,
    bars: Vec<BarDto>,
    total: usize,
    live: Option<BarDto>,
    status: BarsStatusDto,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::{DOMAIN_ERROR, INVALID_PARAMS};
    use crate::methods::LedgerRemux;
    use crate::rpc::{OutboundMessage, Request};
    use async_trait::async_trait;
    use ledger::feed::es_replay::{
        encode_event_store, EsReplayCursor, ES_MBO_EVENT_STORE_FILE_NAME, ES_MBO_EVENT_STORE_KIND,
        ES_MBO_EVENT_STORE_VERSION, RAW_DATABENTO_DBN_ZST_KIND,
    };
    use ledger::market::{
        build_batches, BookAction, BookSide, EsMboEvent, EsMboEventStore, MarketDay, PriceTicks,
    };
    use serde_json::json;
    use std::collections::{HashMap, HashSet};
    use std::path::Path;
    use std::sync::{Arc, Mutex as StdMutex};
    use store::{
        ObjectMetadata, RegisterFileRequest, RemoteObject, StoreConfig, StoreObjectDescriptor,
        StoreObjectRole,
    };
    use tempfile::{tempdir, TempDir};
    use tokio::io::AsyncWriteExt;
    use tokio::sync::mpsc;
    use tokio::time::{timeout, Duration};

    const WAKE: Duration = Duration::from_secs(3);
    const TEST_CATCHUP_CHUNK_BATCHES: usize = 1024;

    #[derive(Clone, Default)]
    struct TestRemote {
        bucket: String,
        objects: Arc<StdMutex<HashMap<String, (Vec<u8>, ObjectMetadata)>>>,
    }

    impl TestRemote {
        fn new() -> Self {
            Self {
                bucket: "test-bucket".to_string(),
                objects: Arc::new(StdMutex::new(HashMap::new())),
            }
        }
    }

    #[async_trait]
    impl RemoteStore for TestRemote {
        async fn put_path(
            &self,
            key: &str,
            path: &Path,
            metadata: &ObjectMetadata,
        ) -> anyhow::Result<RemoteObject> {
            let bytes = tokio::fs::read(path).await?;
            self.put_bytes(key, &bytes, metadata).await
        }

        async fn get_to_path(&self, key: &str, dest: &Path) -> anyhow::Result<RemoteObject> {
            let (bytes, metadata) = self
                .objects
                .lock()
                .unwrap()
                .get(key)
                .cloned()
                .ok_or_else(|| anyhow::anyhow!("missing object {key}"))?;
            if let Some(parent) = dest.parent() {
                tokio::fs::create_dir_all(parent).await?;
            }
            let mut file = tokio::fs::File::create(dest).await?;
            file.write_all(&bytes).await?;
            file.sync_all().await?;
            Ok(remote_object(
                &self.bucket,
                key,
                bytes.len() as u64,
                &metadata,
            ))
        }

        async fn head(&self, key: &str) -> anyhow::Result<Option<RemoteObject>> {
            Ok(self
                .objects
                .lock()
                .unwrap()
                .get(key)
                .map(|(bytes, metadata)| {
                    remote_object(&self.bucket, key, bytes.len() as u64, metadata)
                }))
        }

        async fn delete(&self, key: &str) -> anyhow::Result<()> {
            self.objects.lock().unwrap().remove(key);
            Ok(())
        }

        async fn put_bytes(
            &self,
            key: &str,
            bytes: &[u8],
            metadata: &ObjectMetadata,
        ) -> anyhow::Result<RemoteObject> {
            self.objects
                .lock()
                .unwrap()
                .insert(key.to_string(), (bytes.to_vec(), metadata.clone()));
            Ok(remote_object(
                &self.bucket,
                key,
                bytes.len() as u64,
                metadata,
            ))
        }

        async fn get_bytes(&self, key: &str) -> anyhow::Result<Vec<u8>> {
            self.objects
                .lock()
                .unwrap()
                .get(key)
                .map(|(bytes, _)| bytes.clone())
                .ok_or_else(|| anyhow::anyhow!("missing object {key}"))
        }

        async fn list_keys(&self, prefix: &str) -> anyhow::Result<Vec<String>> {
            Ok(self
                .objects
                .lock()
                .unwrap()
                .keys()
                .filter(|key| key.starts_with(prefix))
                .cloned()
                .collect())
        }

        fn bucket(&self) -> &str {
            &self.bucket
        }
    }

    #[test]
    fn cursor_dto_serializes_catching_up_with_camel_case() {
        let value = serde_json::to_value(EsReplayCursorDto::from(EsReplayCursor {
            epoch: 2,
            feed_seq: 11,
            batch_idx: 10,
            total_batches: 20,
            ts_event_ns: Some(123),
            next_ts_event_ns: Some(456),
            catching_up: true,
            ended: false,
        }))
        .unwrap();

        assert_eq!(value["catchingUp"], true);
        assert!(value.get("catching_up").is_none());
    }

    #[tokio::test]
    async fn open_validates_specs_and_raw_ids_without_creating_sessions() {
        let fixture = remux_fixture();
        let raw_id = fabricate_prepared_day(&fixture.store, sample_events())
            .await
            .0;

        let invalid_spec = call(
            &fixture.methods,
            SESSION_OPEN_METHOD,
            json!({ "rawId": raw_id.to_string(), "projections": ["bogus"] }),
        )
        .await
        .unwrap_err();
        assert_eq!(invalid_spec.code, INVALID_PARAMS);

        let duplicate = call(
            &fixture.methods,
            SESSION_OPEN_METHOD,
            json!({ "rawId": raw_id.to_string(), "projections": ["bars:60s", "bars:1m"] }),
        )
        .await
        .unwrap_err();
        assert_eq!(duplicate.code, INVALID_PARAMS);

        let malformed = call(
            &fixture.methods,
            SESSION_OPEN_METHOD,
            json!({ "rawId": "bad", "projections": ["bars:1s"] }),
        )
        .await
        .unwrap_err();
        assert_eq!(malformed.code, INVALID_PARAMS);
        assert_eq!(malformed.data, Some(json!({ "id": "bad" })));

        let unknown = call(
            &fixture.methods,
            SESSION_OPEN_METHOD,
            json!({ "rawId": format!("sha256-{}", "0".repeat(64)), "projections": ["bars:1s"] }),
        )
        .await
        .unwrap_err();
        assert_eq!(unknown.message, "objectNotFound");

        let opened = open_session(&fixture.methods, &raw_id, vec!["bars:60s"]).await;
        assert_eq!(opened["sessionId"], "session-1");
        assert_eq!(opened["projections"][0]["spec"], "bars:1m");
        assert_eq!(opened["replaced"], Value::Null);

        // Session bounds come from the raw's market_day metadata so the
        // client's scrubber never re-derives the ES calendar.
        let day = MarketDay::parse("2026-03-10").unwrap();
        let (start_ns, end_ns) = day.es_session_bounds_utc().unwrap();
        assert_eq!(opened["marketDay"], day.to_string());
        assert_eq!(opened["sessionStartNs"], ns_string(start_ns));
        assert_eq!(opened["sessionEndNs"], ns_string(end_ns));

        // Open returns only after projection prepare, so the viewer may hydrate
        // an authoritative frame immediately without waiting for a push.
        let initial_frame = call(
            &fixture.methods,
            SESSION_BARS_METHOD,
            json!({
                "sessionId": opened["sessionId"],
                "spec": "bars:1m",
                "from": 0
            }),
        )
        .await
        .unwrap();
        assert_eq!(initial_frame["from"], 0);
        assert_eq!(initial_frame["total"], 0);
    }

    #[tokio::test]
    async fn open_seek_and_push_frames_match_cache_state() {
        let mut fixture = remux_fixture();
        let raw_id = fabricate_prepared_day(&fixture.store, sample_events())
            .await
            .0;
        let opened = open_session(&fixture.methods, &raw_id, vec!["bars:1s"]).await;
        let session_id = opened["sessionId"].as_str().unwrap().to_string();

        seek(&fixture.methods, &session_id, 3_500_000_000).await;
        let clock = wait_notification(
            &mut fixture.output_rx,
            SESSION_CLOCK_NOTIFICATION,
            |params| {
                params["sessionId"].as_str() == Some(session_id.as_str())
                    && params["clock"]["sessionNowNs"].as_str() == Some("3500000000")
            },
        )
        .await;
        assert_eq!(clock["clock"]["mode"], "paused");

        let feed = wait_notification(
            &mut fixture.output_rx,
            SESSION_FEED_NOTIFICATION,
            |params| {
                params["sessionId"].as_str() == Some(session_id.as_str())
                    && params["cursor"]["ended"].as_bool() == Some(true)
            },
        )
        .await;
        let batch_idx = feed["cursor"]["batchIdx"].as_u64().unwrap();
        let frame = wait_notification(
            &mut fixture.output_rx,
            SESSION_BARS_FRAME_NOTIFICATION,
            |params| {
                params["sessionId"].as_str() == Some(session_id.as_str())
                    && params["spec"].as_str() == Some("bars:1s")
                    && params["status"]["processedBatches"].as_u64() == Some(batch_idx)
                    && params["total"].as_u64() == Some(3)
            },
        )
        .await;

        let cache_bars = direct_bars(&fixture.methods, "bars:1s").await;
        assert_eq!(cache_bars.len(), 3);
        assert_eq!(frame["from"], 0);
        assert_eq!(frame["bars"].as_array().unwrap().len(), cache_bars.len());
        assert_eq!(frame["bars"][0]["open"], 100);
        assert_eq!(frame["bars"][0]["close"], 105);
        assert_eq!(frame["bars"][0]["volume"], 5);
        assert_eq!(frame["bars"][0]["buyVolume"], 2);
        assert_eq!(frame["bars"][0]["sellVolume"], 3);
        assert_eq!(frame["live"]["intervalStartNs"], "3000000000");
    }

    #[tokio::test]
    async fn forward_frames_are_contiguous_and_concatenate_to_cache_array() {
        let mut fixture = remux_fixture();
        let raw_id = fabricate_prepared_day(&fixture.store, sample_events())
            .await
            .0;
        let session_id = open_session_id(&fixture.methods, &raw_id, vec!["bars:1s"]).await;

        seek(&fixture.methods, &session_id, 1_500_000_000).await;
        let first = wait_bars_total(&mut fixture.output_rx, &session_id, 0, 1).await;
        assert_eq!(first["from"], 0);

        seek(&fixture.methods, &session_id, 2_500_000_000).await;
        let second = wait_bars_total(&mut fixture.output_rx, &session_id, 0, 2).await;
        assert_eq!(second["from"], first["total"]);

        let mut starts = frame_starts(&first);
        starts.extend(frame_starts(&second));
        assert_eq!(
            starts,
            direct_bar_start_strings(&fixture.methods, "bars:1s").await
        );
    }

    #[tokio::test]
    async fn forward_seek_bars_frame_notifications_are_bounded_by_chunking() {
        let mut fixture = remux_fixture();
        let batch_count = TEST_CATCHUP_CHUNK_BATCHES * 4 + 123;
        let raw_id = fabricate_prepared_day(&fixture.store, generated_trades(batch_count))
            .await
            .0;
        let session_id = open_session_id(&fixture.methods, &raw_id, vec!["bars:1s"]).await;

        seek(&fixture.methods, &session_id, batch_count as u64).await;
        let frame_count =
            wait_bars_processed_count(&mut fixture.output_rx, &session_id, "bars:1s", batch_count)
                .await;
        let frame_ceiling = batch_count.div_ceil(TEST_CATCHUP_CHUNK_BATCHES) * 4 + 8;

        assert!(frame_count <= frame_ceiling);
        assert!(frame_count < batch_count / 10);
    }

    #[tokio::test]
    async fn control_write_clock_notification_arrives_before_projection_convergence() {
        let mut fixture = remux_fixture();
        let batch_count = TEST_CATCHUP_CHUNK_BATCHES * 4 + 123;
        let raw_id = fabricate_prepared_day(&fixture.store, generated_trades(batch_count))
            .await
            .0;
        let session_id =
            open_session_id(&fixture.methods, &raw_id, vec!["bars:1s", "bars:1m"]).await;

        seek(&fixture.methods, &session_id, batch_count as u64).await;
        set_speed(&fixture.methods, &session_id, 3.0).await;
        wait_clock_speed_before_projection_convergence(
            &mut fixture.output_rx,
            &session_id,
            &["bars:1s", "bars:1m"],
            batch_count,
            3.0,
        )
        .await;
    }

    #[tokio::test]
    async fn backward_seek_resyncs_from_zero_and_forward_reemits_suffix() {
        let mut fixture = remux_fixture();
        let raw_id = fabricate_prepared_day(&fixture.store, sample_events())
            .await
            .0;
        let session_id = open_session_id(&fixture.methods, &raw_id, vec!["bars:1s"]).await;

        seek(&fixture.methods, &session_id, 3_500_000_000).await;
        let full = wait_bars_total(&mut fixture.output_rx, &session_id, 0, 3).await;
        assert_eq!(frame_starts(&full), vec!["0", "1000000000", "2000000000"]);

        seek(&fixture.methods, &session_id, 1_750_000_000).await;
        let regressed = wait_notification(
            &mut fixture.output_rx,
            SESSION_BARS_FRAME_NOTIFICATION,
            |params| {
                params["sessionId"].as_str() == Some(session_id.as_str())
                    && params["epoch"].as_u64() == Some(1)
                    && params["from"].as_u64() == Some(0)
                    && params["total"].as_u64() == Some(1)
            },
        )
        .await;
        assert_eq!(frame_starts(&regressed), vec!["0"]);
        assert_eq!(
            direct_bar_starts(&fixture.methods, "bars:1s").await,
            vec![0]
        );

        seek(&fixture.methods, &session_id, 3_500_000_000).await;
        let reemitted = wait_notification(
            &mut fixture.output_rx,
            SESSION_BARS_FRAME_NOTIFICATION,
            |params| {
                params["sessionId"].as_str() == Some(session_id.as_str())
                    && params["epoch"].as_u64() == Some(1)
                    && params["from"].as_u64() == Some(1)
                    && params["total"].as_u64() == Some(3)
            },
        )
        .await;
        let mut starts = frame_starts(&regressed);
        starts.extend(frame_starts(&reemitted));
        assert_eq!(starts, vec!["0", "1000000000", "2000000000"]);
    }

    #[tokio::test]
    async fn pull_backfill_returns_full_frames_suffixes_and_unknown_spec_errors() {
        let mut fixture = remux_fixture();
        let raw_id = fabricate_prepared_day(&fixture.store, sample_events())
            .await
            .0;
        let session_id = open_session_id(&fixture.methods, &raw_id, vec!["bars:1s"]).await;
        seek(&fixture.methods, &session_id, 3_500_000_000).await;
        let pushed = wait_bars_total(&mut fixture.output_rx, &session_id, 0, 3).await;

        let full = call(
            &fixture.methods,
            SESSION_BARS_METHOD,
            json!({ "sessionId": session_id, "spec": "bars:1s", "from": 0 }),
        )
        .await
        .unwrap();
        assert_eq!(full["from"], 0);
        assert_eq!(full["total"], pushed["total"]);
        assert_eq!(frame_starts(&full), frame_starts(&pushed));

        let suffix = call(
            &fixture.methods,
            SESSION_BARS_METHOD,
            json!({ "sessionId": session_id, "spec": "bars:1s", "from": 2 }),
        )
        .await
        .unwrap();
        assert_eq!(suffix["from"], 2);
        assert_eq!(frame_starts(&suffix), vec!["2000000000"]);

        let unknown = call(
            &fixture.methods,
            SESSION_BARS_METHOD,
            json!({ "sessionId": session_id, "spec": "bars:1m" }),
        )
        .await
        .unwrap_err();
        assert_eq!(unknown.code, DOMAIN_ERROR);
        assert!(unknown.message.contains("unknown projection spec"));
    }

    #[tokio::test]
    async fn control_methods_mutate_clock_and_bad_speed_is_domain_error() {
        let mut fixture = remux_fixture();
        let raw_id = fabricate_prepared_day(&fixture.store, sample_events())
            .await
            .0;
        let session_id = open_session_id(&fixture.methods, &raw_id, Vec::new()).await;

        call(
            &fixture.methods,
            SESSION_PLAY_METHOD,
            json!({ "sessionId": session_id }),
        )
        .await
        .unwrap();
        let play = wait_clock_mode(&mut fixture.output_rx, &session_id, "running").await;
        assert_eq!(play["clock"]["speed"], 1.0);

        call(
            &fixture.methods,
            SESSION_SPEED_METHOD,
            json!({ "sessionId": session_id, "speed": 2.5 }),
        )
        .await
        .unwrap();
        let speed = wait_clock_speed(&mut fixture.output_rx, &session_id, 2.5).await;
        assert_eq!(speed["clock"]["mode"], "running");

        call(
            &fixture.methods,
            SESSION_PAUSE_METHOD,
            json!({ "sessionId": session_id }),
        )
        .await
        .unwrap();
        wait_clock_mode(&mut fixture.output_rx, &session_id, "paused").await;

        seek(&fixture.methods, &session_id, 1_500_000_000).await;
        // Controls return after submitting the write; observe the committed
        // seek through the clock stream before pulling status.
        wait_notification(
            &mut fixture.output_rx,
            SESSION_CLOCK_NOTIFICATION,
            |params| {
                params["sessionId"].as_str() == Some(session_id.as_str())
                    && params["clock"]["sessionNowNs"].as_str() == Some("1500000000")
            },
        )
        .await;
        let status = status(&fixture.methods, &session_id).await;
        assert_eq!(status["clock"]["sessionNowNs"], "1500000000");
        assert_eq!(status["clock"]["speed"], 2.5);

        let bad_speed = call(
            &fixture.methods,
            SESSION_SPEED_METHOD,
            json!({ "sessionId": session_id, "speed": -1.0 }),
        )
        .await
        .unwrap_err();
        assert_eq!(bad_speed.code, DOMAIN_ERROR);
        assert!(bad_speed.message.contains("invalid clock speed"));
    }

    #[tokio::test(flavor = "multi_thread", worker_threads = 4)]
    async fn clock_notifications_reflect_the_committed_control_write() {
        let mut fixture = remux_fixture();
        let raw_id = fabricate_prepared_day(&fixture.store, sample_events())
            .await
            .0;
        let session_id = open_session_id(&fixture.methods, &raw_id, Vec::new()).await;

        // On a multi-thread runtime the control write applies asynchronously.
        // Awaiting each seek's committed value before issuing the next write
        // rules out coalescing, so a stale or missed broadcast fails the
        // notification wait.
        for i in 1..=300u64 {
            let target = i * 1_000_000;
            seek(&fixture.methods, &session_id, target).await;
            wait_notification(
                &mut fixture.output_rx,
                SESSION_CLOCK_NOTIFICATION,
                |params| {
                    params["sessionId"].as_str() == Some(session_id.as_str())
                        && params["clock"]["sessionNowNs"].as_str()
                            == Some(target.to_string().as_str())
                },
            )
            .await;
        }
    }

    #[tokio::test]
    async fn lifecycle_replace_close_and_stale_session_errors() {
        let mut fixture = remux_fixture();
        let raw_id = fabricate_prepared_day(&fixture.store, sample_events())
            .await
            .0;
        let other_raw_id = fabricate_prepared_day(&fixture.store, alternate_events())
            .await
            .0;
        let first = open_session_id(&fixture.methods, &raw_id, vec!["bars:1s"]).await;

        let second_open = open_session(&fixture.methods, &other_raw_id, vec!["bars:1s"]).await;
        assert_eq!(second_open["sessionId"], "session-2");
        assert_eq!(second_open["replaced"], first);
        wait_notification(
            &mut fixture.output_rx,
            SESSION_CLOSED_NOTIFICATION,
            |params| {
                params["sessionId"].as_str() == Some(first.as_str())
                    && params["reason"].as_str() == Some("replaced")
            },
        )
        .await;

        let stale = call(
            &fixture.methods,
            SESSION_STATUS_METHOD,
            json!({ "sessionId": first }),
        )
        .await
        .unwrap_err();
        assert_eq!(stale.code, DOMAIN_ERROR);
        assert!(stale.message.contains("unknown session session-1"));

        let second = second_open["sessionId"].as_str().unwrap();
        let closed = call(
            &fixture.methods,
            SESSION_CLOSE_METHOD,
            json!({ "sessionId": second }),
        )
        .await
        .unwrap();
        assert_eq!(closed["closed"], true);
        wait_notification(
            &mut fixture.output_rx,
            SESSION_CLOSED_NOTIFICATION,
            |params| {
                params["sessionId"].as_str() == Some(second)
                    && params["reason"].as_str() == Some("closed")
            },
        )
        .await;

        let after_close = call(
            &fixture.methods,
            SESSION_PLAY_METHOD,
            json!({ "sessionId": second }),
        )
        .await
        .unwrap_err();
        assert_eq!(after_close.code, DOMAIN_ERROR);
        assert!(after_close.message.contains("unknown session session-2"));
    }

    #[tokio::test]
    async fn attach_resumes_active_session_with_clock_and_cursor() {
        let mut fixture = remux_fixture();
        let raw_id = fabricate_prepared_day(&fixture.store, sample_events())
            .await
            .0;
        let session_id = open_session_id(&fixture.methods, &raw_id, vec!["bars:1s"]).await;

        // Put the session in a distinctive state (paused, so it holds still):
        // a committed seek and a non-default speed, each awaited through the
        // clock stream before the next write (controls return on submit).
        seek(&fixture.methods, &session_id, 1_500_000_000).await;
        wait_notification(
            &mut fixture.output_rx,
            SESSION_CLOCK_NOTIFICATION,
            |params| {
                params["sessionId"].as_str() == Some(session_id.as_str())
                    && params["clock"]["sessionNowNs"].as_str() == Some("1500000000")
            },
        )
        .await;
        set_speed(&fixture.methods, &session_id, 2.5).await;
        wait_clock_speed(&mut fixture.output_rx, &session_id, 2.5).await;
        wait_status(
            &fixture.methods,
            &mut fixture.output_rx,
            &session_id,
            |status| status["feed"]["cursor"].is_object(),
        )
        .await;

        let attached = call(
            &fixture.methods,
            SESSION_ATTACH_METHOD,
            json!({
                "sessionId": session_id,
                "rawId": raw_id.to_string(),
                "projections": ["bars:1s"]
            }),
        )
        .await
        .unwrap();
        assert_eq!(attached["attached"], true);
        let attached = &attached["session"];
        assert_eq!(attached["sessionId"], session_id);
        assert_eq!(attached["rawId"], raw_id.to_string());
        assert_eq!(attached["projections"][0]["spec"], "bars:1s");
        assert_eq!(attached["clock"]["mode"], "paused");
        assert_eq!(attached["clock"]["speed"], 2.5);
        assert_eq!(attached["clock"]["sessionNowNs"], "1500000000");
        assert!(attached["cursor"].is_object());

        // Bounds re-derive from the raw's market day, matching open's result.
        let day = MarketDay::parse("2026-03-10").unwrap();
        let (start_ns, end_ns) = day.es_session_bounds_utc().unwrap();
        assert_eq!(attached["marketDay"], day.to_string());
        assert_eq!(attached["sessionStartNs"], ns_string(start_ns));
        assert_eq!(attached["sessionEndNs"], ns_string(end_ns));

        // Attach is read-only: the session still answers pulls and controls
        // under the same id.
        let frame = call(
            &fixture.methods,
            SESSION_BARS_METHOD,
            json!({ "sessionId": session_id, "spec": "bars:1s", "from": 0 }),
        )
        .await
        .unwrap();
        assert_eq!(frame["from"], 0);
        call(
            &fixture.methods,
            SESSION_PLAY_METHOD,
            json!({ "sessionId": session_id }),
        )
        .await
        .unwrap();
        wait_clock_mode(&mut fixture.output_rx, &session_id, "running").await;

        let running = call(
            &fixture.methods,
            SESSION_ATTACH_METHOD,
            json!({
                "sessionId": session_id,
                "rawId": raw_id.to_string(),
                "projections": ["bars:1s"]
            }),
        )
        .await
        .unwrap();
        assert_eq!(running["attached"], true);
        assert_eq!(running["session"]["clock"]["mode"], "running");
        assert_eq!(running["session"]["clock"]["speed"], 2.5);
        assert!(
            running["session"]["clock"]["sessionNowNs"]
                .as_str()
                .unwrap()
                .parse::<u64>()
                .unwrap()
                >= 1_500_000_000
        );
    }

    #[tokio::test]
    async fn attach_requires_matching_active_session() {
        let fixture = remux_fixture();
        let raw_id = fabricate_prepared_day(&fixture.store, sample_events())
            .await
            .0;
        let other_raw_id = fabricate_prepared_day(&fixture.store, alternate_events())
            .await
            .0;
        let attach = |session: String, raw: String, projections: Vec<&'static str>| {
            call(
                &fixture.methods,
                SESSION_ATTACH_METHOD,
                json!({
                    "sessionId": session,
                    "rawId": raw,
                    "projections": projections
                }),
            )
        };

        // Nothing active yet.
        let none = attach(
            "session-missing".to_string(),
            raw_id.to_string(),
            vec!["bars:1s"],
        )
        .await
        .unwrap();
        assert_eq!(none, json!({ "attached": false }));

        let session_id = open_session_id(&fixture.methods, &raw_id, vec!["bars:1s"]).await;

        // Wrong session id cannot alias the active session even when the raw and
        // specs match.
        let wrong_session = attach(
            "session-missing".to_string(),
            raw_id.to_string(),
            vec!["bars:1s"],
        )
        .await
        .unwrap();
        assert_eq!(wrong_session, json!({ "attached": false }));

        // Wrong raw.
        let wrong_raw = attach(
            session_id.clone(),
            other_raw_id.to_string(),
            vec!["bars:1s"],
        )
        .await
        .unwrap();
        assert_eq!(wrong_raw, json!({ "attached": false }));

        // Right raw, different projection set: must fall back to a rebuild.
        let wrong_specs = attach(
            session_id.clone(),
            raw_id.to_string(),
            vec!["bars:1s", "bars:1m"],
        )
        .await
        .unwrap();
        assert_eq!(wrong_specs, json!({ "attached": false }));

        // The matching raw + spec set attaches.
        let matched = attach(session_id.clone(), raw_id.to_string(), vec!["bars:1s"])
            .await
            .unwrap();
        assert_eq!(matched["attached"], true);
        assert_eq!(matched["session"]["sessionId"], session_id);

        // Closed sessions are gone for attach too.
        call(
            &fixture.methods,
            SESSION_CLOSE_METHOD,
            json!({ "sessionId": session_id }),
        )
        .await
        .unwrap();
        let closed = attach(session_id, raw_id.to_string(), vec!["bars:1s"])
            .await
            .unwrap();
        assert_eq!(closed, json!({ "attached": false }));
    }

    #[tokio::test]
    async fn status_reflects_feed_cursor_and_projection_before_and_after_end() {
        let mut fixture = remux_fixture();
        let raw_id = fabricate_prepared_day(&fixture.store, sample_events())
            .await
            .0;
        let session_id = open_session_id(&fixture.methods, &raw_id, vec!["bars:1s"]).await;

        let initial = wait_status(
            &fixture.methods,
            &mut fixture.output_rx,
            &session_id,
            |status| {
                status["feed"]["componentStatus"].as_str() == Some("running")
                    && status["feed"]["cursor"].is_object()
                    && status["projections"][0]["status"].is_object()
                    && status["projections"][0]["completedBars"] == 0
            },
        )
        .await;
        assert_eq!(initial["sessionId"], session_id);
        assert_eq!(initial["feed"]["componentStatus"], "running");
        assert_eq!(initial["feed"]["cursor"]["batchIdx"], 0);
        assert_eq!(initial["projections"][0]["liveBar"], false);

        seek(&fixture.methods, &session_id, 3_500_000_000).await;
        wait_bars_total(&mut fixture.output_rx, &session_id, 0, 3).await;
        let ended = status(&fixture.methods, &session_id).await;
        assert_eq!(ended["feed"]["cursor"]["ended"], true);
        assert_eq!(ended["projections"][0]["completedBars"], 3);
        assert_eq!(ended["projections"][0]["liveBar"], true);
        assert_eq!(ended["projections"][0]["status"]["processedBatches"], 5);
    }

    struct RemuxFixture {
        _tempdir: TempDir,
        store: Store<TestRemote>,
        methods: LedgerRemux<TestRemote>,
        output_rx: mpsc::Receiver<OutboundMessage>,
    }

    fn remux_fixture() -> RemuxFixture {
        let tempdir = tempdir().unwrap();
        let store = Store::open(
            tempdir.path(),
            StoreConfig {
                local_max_bytes: 1024 * 1024,
            },
            Arc::new(TestRemote::new()),
        )
        .unwrap();
        let (output_tx, output_rx) = mpsc::channel(256);
        let methods = LedgerRemux::new(store.clone(), output_tx);
        RemuxFixture {
            _tempdir: tempdir,
            store,
            methods,
            output_rx,
        }
    }

    async fn call(
        methods: &LedgerRemux<TestRemote>,
        method: &str,
        params: Value,
    ) -> Result<Value, RpcError> {
        timeout(
            WAKE,
            methods.handle(Request {
                method: method.to_string(),
                params,
            }),
        )
        .await
        .unwrap()
    }

    async fn open_session(
        methods: &LedgerRemux<TestRemote>,
        raw_id: &StoreObjectId,
        projections: Vec<&str>,
    ) -> Value {
        call(
            methods,
            SESSION_OPEN_METHOD,
            json!({
                "rawId": raw_id.to_string(),
                "projections": projections,
            }),
        )
        .await
        .unwrap()
    }

    async fn open_session_id(
        methods: &LedgerRemux<TestRemote>,
        raw_id: &StoreObjectId,
        projections: Vec<&str>,
    ) -> String {
        open_session(methods, raw_id, projections).await["sessionId"]
            .as_str()
            .unwrap()
            .to_string()
    }

    async fn seek(methods: &LedgerRemux<TestRemote>, session_id: &str, session_ns: u64) {
        call(
            methods,
            SESSION_SEEK_METHOD,
            json!({ "sessionId": session_id, "sessionNs": session_ns.to_string() }),
        )
        .await
        .unwrap();
    }

    async fn set_speed(methods: &LedgerRemux<TestRemote>, session_id: &str, speed: f64) {
        call(
            methods,
            SESSION_SPEED_METHOD,
            json!({ "sessionId": session_id, "speed": speed }),
        )
        .await
        .unwrap();
    }

    async fn status(methods: &LedgerRemux<TestRemote>, session_id: &str) -> Value {
        call(
            methods,
            SESSION_STATUS_METHOD,
            json!({ "sessionId": session_id }),
        )
        .await
        .unwrap()
    }

    async fn wait_status(
        methods: &LedgerRemux<TestRemote>,
        output_rx: &mut mpsc::Receiver<OutboundMessage>,
        session_id: &str,
        mut predicate: impl FnMut(&Value) -> bool,
    ) -> Value {
        timeout(WAKE, async {
            loop {
                let status = status(methods, session_id).await;
                if predicate(&status) {
                    return status;
                }
                tokio::select! {
                    _ = next_session_notification(output_rx) => {}
                    _ = tokio::task::yield_now() => {}
                }
            }
        })
        .await
        .unwrap()
    }

    async fn wait_notification(
        output_rx: &mut mpsc::Receiver<OutboundMessage>,
        method: &str,
        mut predicate: impl FnMut(&Value) -> bool,
    ) -> Value {
        timeout(WAKE, async {
            loop {
                let value = next_json(output_rx).await;
                if value["method"] == method && predicate(&value["params"]) {
                    return value["params"].clone();
                }
            }
        })
        .await
        .unwrap()
    }

    async fn wait_bars_total(
        output_rx: &mut mpsc::Receiver<OutboundMessage>,
        session_id: &str,
        epoch: u64,
        total: usize,
    ) -> Value {
        wait_notification(output_rx, SESSION_BARS_FRAME_NOTIFICATION, |params| {
            params["sessionId"].as_str() == Some(session_id)
                && params["epoch"].as_u64() == Some(epoch)
                && params["total"].as_u64() == Some(total as u64)
        })
        .await
    }

    async fn wait_bars_processed_count(
        output_rx: &mut mpsc::Receiver<OutboundMessage>,
        session_id: &str,
        spec: &str,
        processed_batches: usize,
    ) -> usize {
        timeout(WAKE, async {
            let mut frame_count = 0usize;
            loop {
                let value = next_json(output_rx).await;
                if value["method"] == SESSION_BARS_FRAME_NOTIFICATION
                    && value["params"]["sessionId"].as_str() == Some(session_id)
                    && value["params"]["spec"].as_str() == Some(spec)
                {
                    frame_count += 1;
                    if value["params"]["status"]["processedBatches"].as_u64()
                        == Some(processed_batches as u64)
                    {
                        return frame_count;
                    }
                }
            }
        })
        .await
        .unwrap()
    }

    async fn wait_clock_speed_before_projection_convergence(
        output_rx: &mut mpsc::Receiver<OutboundMessage>,
        session_id: &str,
        specs: &[&str],
        processed_batches: usize,
        speed: f64,
    ) {
        timeout(WAKE, async {
            let expected = specs.iter().copied().collect::<HashSet<_>>();
            let mut converged = HashSet::new();
            loop {
                let value = next_json(output_rx).await;
                match value["method"].as_str() {
                    Some(SESSION_CLOCK_NOTIFICATION)
                        if value["params"]["sessionId"].as_str() == Some(session_id)
                            && value["params"]["clock"]["speed"].as_f64() == Some(speed) =>
                    {
                        assert!(
                            converged.len() < expected.len(),
                            "all projections converged before the speed clock notification"
                        );
                        return;
                    }
                    Some(SESSION_BARS_FRAME_NOTIFICATION)
                        if value["params"]["sessionId"].as_str() == Some(session_id) =>
                    {
                        if value["params"]["status"]["processedBatches"].as_u64()
                            == Some(processed_batches as u64)
                        {
                            if let Some(spec) = value["params"]["spec"].as_str() {
                                if expected.contains(spec) {
                                    converged.insert(spec.to_string());
                                }
                            }
                        }
                    }
                    _ => {}
                }
            }
        })
        .await
        .unwrap()
    }

    async fn wait_clock_mode(
        output_rx: &mut mpsc::Receiver<OutboundMessage>,
        session_id: &str,
        mode: &str,
    ) -> Value {
        wait_notification(output_rx, SESSION_CLOCK_NOTIFICATION, |params| {
            params["sessionId"].as_str() == Some(session_id)
                && params["clock"]["mode"].as_str() == Some(mode)
        })
        .await
    }

    async fn wait_clock_speed(
        output_rx: &mut mpsc::Receiver<OutboundMessage>,
        session_id: &str,
        speed: f64,
    ) -> Value {
        wait_notification(output_rx, SESSION_CLOCK_NOTIFICATION, |params| {
            params["sessionId"].as_str() == Some(session_id)
                && params["clock"]["speed"].as_f64() == Some(speed)
        })
        .await
    }

    async fn next_session_notification(output_rx: &mut mpsc::Receiver<OutboundMessage>) -> Value {
        loop {
            let value = next_json(output_rx).await;
            if value["method"]
                .as_str()
                .is_some_and(|method| method.starts_with("remux/ledger/session/"))
            {
                return value;
            }
        }
    }

    async fn next_json(output_rx: &mut mpsc::Receiver<OutboundMessage>) -> Value {
        loop {
            match output_rx.recv().await.expect("outbound channel open") {
                OutboundMessage::Json(value) => return value,
                OutboundMessage::Flush(_) => {}
            }
        }
    }

    async fn direct_bars(methods: &LedgerRemux<TestRemote>, spec: &str) -> Vec<Bar> {
        let active = methods.sessions.active.lock().await;
        let session = active.as_ref().expect("active session");
        let projection = session
            .projections
            .iter()
            .find(|projection| projection.spec == spec)
            .expect("projection");
        session
            .handle
            .cache()
            .read_array(&projection.cells.bars)
            .unwrap()
    }

    async fn direct_bar_starts(methods: &LedgerRemux<TestRemote>, spec: &str) -> Vec<u64> {
        direct_bars(methods, spec)
            .await
            .into_iter()
            .map(|bar| bar.interval_start_ns)
            .collect()
    }

    async fn direct_bar_start_strings(
        methods: &LedgerRemux<TestRemote>,
        spec: &str,
    ) -> Vec<String> {
        direct_bar_starts(methods, spec)
            .await
            .into_iter()
            .map(|start| start.to_string())
            .collect()
    }

    fn frame_starts(frame: &Value) -> Vec<String> {
        frame["bars"]
            .as_array()
            .unwrap()
            .iter()
            .map(|bar| bar["intervalStartNs"].as_str().unwrap().to_string())
            .collect()
    }

    async fn fabricate_prepared_day<S>(
        store: &Store<S>,
        events: Vec<EsMboEvent>,
    ) -> (StoreObjectId, StoreObjectDescriptor)
    where
        S: RemoteStore + 'static,
    {
        let tempdir = tempdir().unwrap();
        let raw_path = tempdir.path().join("raw.dbn.zst");
        tokio::fs::write(
            &raw_path,
            format!("dummy raw {}", store::now_ns()).as_bytes(),
        )
        .await
        .unwrap();
        let market_day = MarketDay::parse("2026-03-10").unwrap();
        let raw = store
            .register_file(RegisterFileRequest {
                path: &raw_path,
                role: StoreObjectRole::Raw,
                kind: RAW_DATABENTO_DBN_ZST_KIND.to_string(),
                file_name: Some("raw.dbn.zst".to_string()),
                format: Some("dbn.zst".to_string()),
                media_type: None,
                lineage: Vec::new(),
                // Production raws carry market_day (the day catalog groups on
                // it); session/open derives session bounds from it.
                metadata_json: json!({ "market_day": market_day.to_string() }),
            })
            .await
            .unwrap();

        let batches = build_batches(&events);
        let event_store = EsMboEventStore { events, batches };
        let encoded = encode_event_store(&event_store);
        let artifact_path = tempdir.path().join(ES_MBO_EVENT_STORE_FILE_NAME);
        tokio::fs::write(&artifact_path, encoded).await.unwrap();
        let first_ts_event_ns = event_store
            .events
            .first()
            .map(|event| event.ts_event_ns)
            .unwrap_or_default();
        let last_ts_event_ns = event_store
            .events
            .last()
            .map(|event| event.ts_event_ns)
            .unwrap_or_default();
        let artifact = store
            .register_file(RegisterFileRequest {
                path: &artifact_path,
                role: StoreObjectRole::Artifact,
                kind: ES_MBO_EVENT_STORE_KIND.to_string(),
                file_name: Some(ES_MBO_EVENT_STORE_FILE_NAME.to_string()),
                format: Some("ledger.es_mbo_event_store.v1".to_string()),
                media_type: None,
                lineage: vec![raw.id.clone()],
                metadata_json: json!({
                    "artifact": "es_mbo_event_store",
                    "version": ES_MBO_EVENT_STORE_VERSION,
                    "raw_object_id": raw.id.to_string(),
                    "market_day": market_day.to_string(),
                    "event_count": event_store.events.len() as u64,
                    "batch_count": event_store.batches.len() as u64,
                    "first_ts_event_ns": first_ts_event_ns.to_string(),
                    "last_ts_event_ns": last_ts_event_ns.to_string(),
                }),
            })
            .await
            .unwrap();

        (raw.id, artifact)
    }

    fn sample_events() -> Vec<EsMboEvent> {
        vec![
            trade(100, 1, 100, 2, Some(BookSide::Bid)),
            trade(500_000_000, 2, 105, 3, Some(BookSide::Ask)),
            trade(1_500_000_000, 3, 99, 4, Some(BookSide::Bid)),
            trade(2_500_000_000, 4, 102, 5, None),
            trade(3_500_000_000, 5, 110, 6, Some(BookSide::Ask)),
        ]
    }

    fn alternate_events() -> Vec<EsMboEvent> {
        vec![
            trade(100, 1, 200, 1, Some(BookSide::Bid)),
            trade(1_500_000_000, 2, 201, 1, Some(BookSide::Ask)),
        ]
    }

    fn generated_trades(count: usize) -> Vec<EsMboEvent> {
        (0..count)
            .map(|idx| {
                let sequence = (idx + 1) as u64;
                let side = if idx % 2 == 0 {
                    Some(BookSide::Bid)
                } else {
                    Some(BookSide::Ask)
                };
                trade(
                    sequence,
                    sequence,
                    100 + (idx % 19) as i64,
                    1 + (idx % 7) as u32,
                    side,
                )
            })
            .collect()
    }

    fn trade(
        ts_event_ns: u64,
        sequence: u64,
        price_ticks: i64,
        size: u32,
        aggressor: Option<BookSide>,
    ) -> EsMboEvent {
        EsMboEvent {
            ts_event_ns,
            ts_recv_ns: ts_event_ns,
            sequence,
            action: BookAction::Trade,
            side: aggressor,
            price_ticks: Some(PriceTicks(price_ticks)),
            size,
            order_id: sequence,
            flags: 0,
            is_last: true,
        }
    }

    fn remote_object(
        bucket: &str,
        key: &str,
        size_bytes: u64,
        metadata: &ObjectMetadata,
    ) -> RemoteObject {
        RemoteObject {
            bucket: bucket.to_string(),
            key: key.to_string(),
            size_bytes,
            sha256: Some(metadata.sha256.clone()),
            etag: None,
            metadata: metadata.user_metadata.clone(),
        }
    }
}
