use crate::session_protocol::{
    error_message, open_session_request, parse_required_ns, request_id_from_value,
    SessionClientMessage, SessionErrorCode, SessionFrameCause, SessionServerMessage,
};
use crate::state::{ApiLedger, ApiState};
use axum::{
    extract::{
        ws::{Message, WebSocket, WebSocketUpgrade},
        State,
    },
    response::IntoResponse,
};
use ledger::projection::ProjectionSubscriptionId;
use ledger::{FeedAdvanceBudget, Session, SessionPlaybackState};
use serde_json::{Error as JsonError, Value};
use std::time::Duration;
use tokio::time::{sleep, Instant, Sleep};

const PROTOCOL_NAME: &str = "ledger.session";
const PROTOCOL_VERSION: u16 = 1;
const MAX_ADVANCE_BATCHES: usize = 50_000;
const DEFAULT_PLAY_BUDGET_BATCHES: usize = 500;
const MAX_PLAY_BUDGET_BATCHES: usize = 5_000;
const DEFAULT_PUMP_INTERVAL_MS: u64 = 50;
const MIN_PUMP_INTERVAL_MS: u64 = 16;
const MAX_PUMP_INTERVAL_MS: u64 = 1_000;
const MAX_SPEED: f64 = 10_000.0;

pub(crate) async fn session_ws(
    State(state): State<ApiState>,
    ws: WebSocketUpgrade,
) -> impl IntoResponse {
    ws.on_upgrade(move |socket| run_session_socket(state, socket))
}

async fn run_session_socket(state: ApiState, mut socket: WebSocket) {
    let mut driver = SessionSocketDriver::new(state.ledger);
    if !send_message(&mut socket, &SessionSocketDriver::hello()).await {
        return;
    }

    let started_at = Instant::now();
    let mut next_pump = Box::pin(sleep(Duration::from_secs(86_400)));

    loop {
        tokio::select! {
            maybe_message = socket.recv() => {
                let Some(message) = maybe_message else {
                    break;
                };
                let message = match message {
                    Ok(message) => message,
                    Err(err) => {
                        let error = error_message(None, SessionErrorCode::Internal, format!("websocket receive error: {err}"));
                        let _ = send_message(&mut socket, &error).await;
                        break;
                    }
                };

                if let Message::Ping(payload) = message {
                    if socket.send(Message::Pong(payload)).await.is_err() {
                        return;
                    }
                    continue;
                }

                if matches!(message, Message::Pong(_)) {
                    continue;
                }

                if matches!(message, Message::Close(_)) {
                    break;
                }

                let now_mono_ns = elapsed_ns(started_at);
                let responses = driver.handle_ws_message(message, now_mono_ns).await;
                for response in responses {
                    if !send_message(&mut socket, &response).await {
                        return;
                    }
                }
                schedule_next_pump(&driver, &mut next_pump);
            }
            _ = &mut next_pump, if driver.playing() => {
                let now_mono_ns = elapsed_ns(started_at);
                let responses = driver.pump(now_mono_ns);
                for response in responses {
                    if !send_message(&mut socket, &response).await {
                        return;
                    }
                }
                schedule_next_pump(&driver, &mut next_pump);
            }
        }
    }
}

fn schedule_next_pump(driver: &SessionSocketDriver, next_pump: &mut std::pin::Pin<Box<Sleep>>) {
    if driver.playing() {
        next_pump
            .as_mut()
            .reset(Instant::now() + driver.pump_interval());
    }
}

async fn send_message(socket: &mut WebSocket, message: &SessionServerMessage) -> bool {
    let text = match serde_json::to_string(message) {
        Ok(text) => text,
        Err(err) => {
            eprintln!("[ledger-api] failed to serialize session ws message: {err}");
            return false;
        }
    };
    socket.send(Message::Text(text.into())).await.is_ok()
}

fn elapsed_ns(started_at: Instant) -> u64 {
    let nanos = started_at.elapsed().as_nanos();
    nanos.min(u64::MAX as u128) as u64
}

pub(crate) struct SessionSocketDriver {
    ledger: Option<ApiLedger>,
    session: Option<Session>,
    pump_interval_ms: u64,
    play_budget: FeedAdvanceBudget,
}

impl SessionSocketDriver {
    pub(crate) fn new(ledger: ApiLedger) -> Self {
        Self {
            ledger: Some(ledger),
            session: None,
            pump_interval_ms: DEFAULT_PUMP_INTERVAL_MS,
            play_budget: FeedAdvanceBudget::new(DEFAULT_PLAY_BUDGET_BATCHES)
                .expect("default play budget must be valid"),
        }
    }

    pub(crate) fn hello() -> SessionServerMessage {
        SessionServerMessage::ServerHello {
            protocol: PROTOCOL_NAME.to_string(),
            version: PROTOCOL_VERSION,
            capabilities: vec![
                "open_session".to_string(),
                "subscribe_projection".to_string(),
                "advance".to_string(),
                "playback_clock".to_string(),
                "seek".to_string(),
            ],
        }
    }

    pub(crate) fn playing(&self) -> bool {
        self.session
            .as_ref()
            .map(|session| session.snapshot().playback == SessionPlaybackState::Playing)
            .unwrap_or(false)
    }

    pub(crate) fn pump_interval(&self) -> Duration {
        Duration::from_millis(self.pump_interval_ms)
    }

    pub(crate) async fn handle_ws_message(
        &mut self,
        message: Message,
        now_mono_ns: u64,
    ) -> Vec<SessionServerMessage> {
        match message {
            Message::Text(text) => self.handle_text(&text, now_mono_ns).await,
            Message::Binary(bytes) => self.handle_json_bytes(&bytes, now_mono_ns).await,
            Message::Ping(_) | Message::Pong(_) | Message::Close(_) => Vec::new(),
        }
    }

    pub(crate) async fn handle_text(
        &mut self,
        text: &str,
        now_mono_ns: u64,
    ) -> Vec<SessionServerMessage> {
        match serde_json::from_str::<SessionClientMessage>(text) {
            Ok(message) => self.handle_client_message(message, now_mono_ns).await,
            Err(err) => {
                let request_id = serde_json::from_str::<Value>(text)
                    .ok()
                    .and_then(|value| request_id_from_value(&value));
                vec![json_error(request_id, err)]
            }
        }
    }

    async fn handle_json_bytes(
        &mut self,
        bytes: &[u8],
        now_mono_ns: u64,
    ) -> Vec<SessionServerMessage> {
        match serde_json::from_slice::<SessionClientMessage>(bytes) {
            Ok(message) => self.handle_client_message(message, now_mono_ns).await,
            Err(err) => {
                let request_id = serde_json::from_slice::<Value>(bytes)
                    .ok()
                    .and_then(|value| request_id_from_value(&value));
                vec![json_error(request_id, err)]
            }
        }
    }

    pub(crate) async fn handle_client_message(
        &mut self,
        message: SessionClientMessage,
        now_mono_ns: u64,
    ) -> Vec<SessionServerMessage> {
        match message {
            SessionClientMessage::OpenSession {
                request_id,
                session_id,
                session_kind,
                symbol,
                market_date,
                start_ts_ns,
                feed,
            } => {
                if self.session.is_some() {
                    return vec![error_message(
                        request_id,
                        SessionErrorCode::SessionAlreadyOpen,
                        "this websocket already owns an open session",
                    )];
                }
                let Some(ledger) = self.ledger.clone() else {
                    return vec![error_message(
                        request_id,
                        SessionErrorCode::Internal,
                        "session websocket driver has no ledger",
                    )];
                };

                let mut responses = vec![SessionServerMessage::SessionOpening {
                    request_id: request_id.clone(),
                    session_kind,
                    symbol: symbol.clone(),
                    market_date,
                }];
                let request = match open_session_request(
                    session_id,
                    session_kind,
                    symbol,
                    market_date,
                    start_ts_ns,
                    feed,
                ) {
                    Ok(request) => request,
                    Err(err) => {
                        responses.push(error_message(request_id, err.code, err.message));
                        return responses;
                    }
                };

                match ledger.open_session(request).await {
                    Ok(session) => {
                        let snapshot = session.snapshot();
                        self.session = Some(session);
                        responses.push(SessionServerMessage::SessionOpened {
                            request_id,
                            session: snapshot.into(),
                        });
                    }
                    Err(err) => responses.push(error_message(
                        request_id,
                        SessionErrorCode::LedgerError,
                        format!("{err:#}"),
                    )),
                }
                responses
            }
            SessionClientMessage::SubscribeProjection {
                request_id,
                projection,
            } => {
                let Some(session) = self.session.as_mut() else {
                    return vec![session_not_open(request_id)];
                };
                match session.subscribe_projection(projection) {
                    Ok(subscription) => {
                        let projection_key_display = subscription.key.to_string();
                        let snapshot = session.snapshot();
                        vec![SessionServerMessage::ProjectionSubscribed {
                            request_id,
                            subscription_id: subscription.id.get(),
                            projection_key: subscription.key,
                            projection_key_display,
                            session: snapshot.into(),
                            frames: subscription.initial_frames,
                        }]
                    }
                    Err(err) => vec![error_message(
                        request_id,
                        SessionErrorCode::InvalidProjection,
                        format!("{err:#}"),
                    )],
                }
            }
            SessionClientMessage::UnsubscribeProjection {
                request_id,
                subscription_id,
            } => {
                let Some(session) = self.session.as_mut() else {
                    return vec![session_not_open(request_id)];
                };
                match session.unsubscribe_projection(ProjectionSubscriptionId::new(subscription_id))
                {
                    Ok(()) => vec![SessionServerMessage::ProjectionUnsubscribed {
                        request_id,
                        subscription_id,
                    }],
                    Err(err) => vec![error_message(
                        request_id,
                        SessionErrorCode::InvalidSubscription,
                        format!("{err:#}"),
                    )],
                }
            }
            SessionClientMessage::Advance {
                request_id,
                batches,
            } => {
                if batches == 0 || batches > MAX_ADVANCE_BATCHES {
                    return vec![error_message(
                        request_id,
                        SessionErrorCode::InvalidCommand,
                        format!("advance batches must be between 1 and {MAX_ADVANCE_BATCHES}"),
                    )];
                }
                let Some(session) = self.session.as_mut() else {
                    return vec![session_not_open(request_id)];
                };
                if session.snapshot().playback == SessionPlaybackState::Playing {
                    return vec![error_message(
                        request_id,
                        SessionErrorCode::CommandConflict,
                        "manual advance is only allowed while paused",
                    )];
                }
                match session.advance_feed_batches(batches) {
                    Ok(report) => vec![SessionServerMessage::SessionFrameBatch {
                        request_id,
                        cause: SessionFrameCause::Advance,
                        applied_batches: report.applied_batches,
                        budget_exhausted: false,
                        behind: false,
                        session: report.snapshot.into(),
                        frames: report.projection_frames,
                    }],
                    Err(err) => vec![error_message(
                        request_id,
                        SessionErrorCode::LedgerError,
                        format!("{err:#}"),
                    )],
                }
            }
            SessionClientMessage::Play {
                request_id,
                speed,
                pump_interval_ms,
                budget_batches,
            } => {
                if let Err(message) = validate_speed(speed) {
                    return vec![error_message(
                        request_id,
                        SessionErrorCode::InvalidCommand,
                        message,
                    )];
                }
                let Some(session) = self.session.as_mut() else {
                    return vec![session_not_open(request_id)];
                };
                let mut next_pump_interval_ms = self.pump_interval_ms;
                if let Some(interval) = pump_interval_ms {
                    if !(MIN_PUMP_INTERVAL_MS..=MAX_PUMP_INTERVAL_MS).contains(&interval) {
                        return vec![error_message(
                            request_id,
                            SessionErrorCode::InvalidCommand,
                            format!(
                                "pump_interval_ms must be between {MIN_PUMP_INTERVAL_MS} and {MAX_PUMP_INTERVAL_MS}"
                            ),
                        )];
                    }
                    next_pump_interval_ms = interval;
                }
                let mut next_play_budget = self.play_budget;
                if let Some(budget) = budget_batches {
                    if budget == 0 || budget > MAX_PLAY_BUDGET_BATCHES {
                        return vec![error_message(
                            request_id,
                            SessionErrorCode::InvalidCommand,
                            format!(
                                "budget_batches must be between 1 and {MAX_PLAY_BUDGET_BATCHES}"
                            ),
                        )];
                    }
                    next_play_budget = FeedAdvanceBudget::new(budget)
                        .expect("validated websocket play budget must be valid");
                }
                match session.play(speed, now_mono_ns) {
                    Ok(snapshot) => {
                        self.pump_interval_ms = next_pump_interval_ms;
                        self.play_budget = next_play_budget;
                        vec![SessionServerMessage::SessionPlayback {
                            request_id,
                            session: snapshot.into(),
                        }]
                    }
                    Err(err) => vec![error_message(
                        request_id,
                        SessionErrorCode::LedgerError,
                        format!("{err:#}"),
                    )],
                }
            }
            SessionClientMessage::Pause { request_id } => {
                let Some(session) = self.session.as_mut() else {
                    return vec![session_not_open(request_id)];
                };
                let snapshot = session.pause(now_mono_ns);
                vec![SessionServerMessage::SessionPlayback {
                    request_id,
                    session: snapshot.into(),
                }]
            }
            SessionClientMessage::SetSpeed { request_id, speed } => {
                if let Err(message) = validate_speed(speed) {
                    return vec![error_message(
                        request_id,
                        SessionErrorCode::InvalidCommand,
                        message,
                    )];
                }
                let Some(session) = self.session.as_mut() else {
                    return vec![session_not_open(request_id)];
                };
                match session.set_speed(speed, now_mono_ns) {
                    Ok(snapshot) => vec![SessionServerMessage::SessionPlayback {
                        request_id,
                        session: snapshot.into(),
                    }],
                    Err(err) => vec![error_message(
                        request_id,
                        SessionErrorCode::LedgerError,
                        format!("{err:#}"),
                    )],
                }
            }
            SessionClientMessage::Seek {
                request_id,
                feed_ts_ns,
            } => {
                let target = match parse_required_ns(&feed_ts_ns, "feed_ts_ns") {
                    Ok(target) => target,
                    Err(err) => return vec![error_message(request_id, err.code, err.message)],
                };
                let Some(session) = self.session.as_mut() else {
                    return vec![session_not_open(request_id)];
                };
                match session.seek_to(target) {
                    Ok(report) => vec![SessionServerMessage::SessionFrameBatch {
                        request_id,
                        cause: SessionFrameCause::Seek,
                        applied_batches: 0,
                        budget_exhausted: false,
                        behind: false,
                        session: report.snapshot.into(),
                        frames: report.projection_frames,
                    }],
                    Err(err) => vec![error_message(
                        request_id,
                        SessionErrorCode::LedgerError,
                        format!("{err:#}"),
                    )],
                }
            }
            SessionClientMessage::Snapshot { request_id } => {
                let Some(session) = self.session.as_ref() else {
                    return vec![session_not_open(request_id)];
                };
                vec![SessionServerMessage::SessionSnapshot {
                    request_id,
                    session: session.snapshot().into(),
                }]
            }
            SessionClientMessage::CloseSession { request_id } => {
                self.session = None;
                vec![SessionServerMessage::SessionClosed { request_id }]
            }
        }
    }

    pub(crate) fn pump(&mut self, now_mono_ns: u64) -> Vec<SessionServerMessage> {
        let Some(session) = self.session.as_mut() else {
            return Vec::new();
        };
        if session.snapshot().playback != SessionPlaybackState::Playing {
            return Vec::new();
        }
        match session.pump_clock(now_mono_ns, self.play_budget) {
            Ok(report)
                if report.applied_batches > 0
                    || report.budget_exhausted
                    || report.behind
                    || report.snapshot.playback == SessionPlaybackState::Ended =>
            {
                vec![SessionServerMessage::SessionFrameBatch {
                    request_id: None,
                    cause: SessionFrameCause::PlaybackTick,
                    applied_batches: report.applied_batches,
                    budget_exhausted: report.budget_exhausted,
                    behind: report.behind,
                    session: report.snapshot.into(),
                    frames: report.projection_frames,
                }]
            }
            Ok(_) => Vec::new(),
            Err(err) => vec![error_message(
                None,
                SessionErrorCode::LedgerError,
                format!("{err:#}"),
            )],
        }
    }

    #[cfg(test)]
    fn with_session(session: Session) -> Self {
        Self {
            ledger: None,
            session: Some(session),
            pump_interval_ms: DEFAULT_PUMP_INTERVAL_MS,
            play_budget: FeedAdvanceBudget::new(DEFAULT_PLAY_BUDGET_BATCHES)
                .expect("default play budget must be valid"),
        }
    }
}

fn validate_speed(speed: f64) -> Result<(), String> {
    if !speed.is_finite() || speed <= 0.0 || speed > MAX_SPEED {
        Err(format!(
            "speed must be positive, finite, and less than or equal to {MAX_SPEED}"
        ))
    } else {
        Ok(())
    }
}

fn session_not_open(request_id: Option<String>) -> SessionServerMessage {
    error_message(
        request_id,
        SessionErrorCode::SessionNotOpen,
        "open_session must be sent before this command",
    )
}

fn json_error(request_id: Option<String>, err: JsonError) -> SessionServerMessage {
    error_message(
        request_id,
        SessionErrorCode::InvalidJson,
        format!("invalid websocket message JSON: {err}"),
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::NaiveDate;
    use ledger::{Ledger, ReplayDataset};
    use ledger_domain::{
        build_batches, build_trade_index, encode_batches, encode_events, encode_trades, BookAction,
        BookSide, EventStore, MboEvent, PriceTicks, ProjectionSpec, StorageKind,
    };
    use ledger_replay::{ReplayFeedConfig, ReplayFeedMode};
    use ledger_store::{
        sha256_bytes, LedgerStore, LocalStore, ObjectKeyBuilder, R2Config, R2ObjectStore,
        StoredObject,
    };
    use serde_json::{json, Value};
    use std::path::PathBuf;
    use std::sync::Arc;

    fn add(ts: u64, seq: u64, side: BookSide, price: i64, size: u32, order_id: u64) -> MboEvent {
        MboEvent::synthetic(
            ts,
            seq,
            BookAction::Add,
            Some(side),
            Some(PriceTicks(price)),
            size,
            order_id,
            true,
        )
    }

    fn event_store(events: Vec<MboEvent>) -> EventStore {
        EventStore {
            batches: build_batches(&events),
            trades: build_trade_index(&events),
            events,
        }
    }

    fn session() -> Session {
        let market_day = ledger_domain::MarketDay::resolve_es(
            "ESH6",
            NaiveDate::from_ymd_opt(2026, 3, 12).unwrap(),
        )
        .unwrap();
        let dataset = ReplayDataset {
            replay_dataset_id: "replay-ESH6-2026-03-12".to_string(),
            market_day,
            events_path: PathBuf::from("events.bin"),
            batches_path: PathBuf::from("batches.bin"),
            trades_path: PathBuf::from("trades.bin"),
            book_check_path: PathBuf::from("book_check.json"),
        };
        Session::from_replay_dataset(
            "ws-test".to_string(),
            dataset,
            event_store(vec![
                add(100, 1, BookSide::Bid, 100, 2, 1),
                add(200, 2, BookSide::Ask, 101, 3, 2),
            ]),
            ReplayFeedConfig {
                mode: ReplayFeedMode::ExchangeTruth,
                execution_profile: ledger_domain::ExecutionProfile::default(),
                visibility_profile: ledger_domain::VisibilityProfile::truth(),
            },
        )
    }

    fn bbo_spec() -> ProjectionSpec {
        ProjectionSpec::new("bbo", 1, json!({})).unwrap()
    }

    async fn api_ledger_with_cached_dataset() -> (tempfile::TempDir, ApiLedger) {
        let dir = tempfile::tempdir().unwrap();
        let remote = R2ObjectStore::new(R2Config {
            account_id: "account".to_string(),
            access_key_id: "access".to_string(),
            secret_access_key: "secret".to_string(),
            bucket: "test".to_string(),
            endpoint_url: Some("https://example.invalid".to_string()),
            region: "auto".to_string(),
            multipart_threshold_bytes: 1024,
            multipart_part_size_bytes: 1024,
        })
        .await
        .unwrap();
        let store = LedgerStore::open(
            LocalStore::new(dir.path()),
            Arc::new(remote),
            ObjectKeyBuilder::default(),
        )
        .unwrap();
        let market_day = ledger_domain::MarketDay::resolve_es(
            "ESH6",
            NaiveDate::from_ymd_opt(2026, 3, 12).unwrap(),
        )
        .unwrap();
        store.catalog.upsert_market_day(&market_day, true).unwrap();

        let raw = object(StorageKind::RawDbn, "raw.dbn.zst", b"raw");
        store.catalog.upsert_object(&market_day, &raw).unwrap();

        let events = vec![add(100, 1, BookSide::Bid, 100, 2, 1)];
        let batches = build_batches(&events);
        let trades = build_trade_index(&events);
        let artifacts = vec![
            (
                StorageKind::EventStore,
                encode_events(&events),
                "events.v1.bin",
            ),
            (
                StorageKind::BatchIndex,
                encode_batches(&batches),
                "batches.v1.bin",
            ),
            (
                StorageKind::TradeIndex,
                encode_trades(&trades),
                "trades.v1.bin",
            ),
            (StorageKind::BookCheck, b"{}".to_vec(), "book_check.v1.json"),
        ];
        let stored_artifacts = artifacts
            .iter()
            .map(|(kind, bytes, name)| object(kind.clone(), name, bytes))
            .collect::<Vec<_>>();
        for artifact in &stored_artifacts {
            store.catalog.upsert_object(&market_day, artifact).unwrap();
        }
        let replay_dataset = store
            .catalog
            .upsert_replay_dataset(&market_day, &raw, &stored_artifacts)
            .unwrap();
        for (kind, bytes, _) in artifacts {
            let path = store
                .local
                .replay_cache_artifact_path(&market_day, &replay_dataset.id, kind)
                .unwrap();
            store.local.write_atomic(&path, &bytes).await.unwrap();
        }

        (dir, Ledger::new(store))
    }

    fn object(kind: StorageKind, name: &str, bytes: &[u8]) -> StoredObject {
        StoredObject {
            kind,
            logical_key: name.to_string(),
            format: "test".to_string(),
            schema_version: 1,
            content_sha256: sha256_bytes(bytes),
            size_bytes: bytes.len() as i64,
            remote_bucket: "test".to_string(),
            remote_key: format!("test/{name}"),
            producer: Some("test".to_string()),
            producer_version: Some("1".to_string()),
            source_provider: None,
            source_dataset: None,
            source_schema: None,
            source_symbol: None,
            metadata_json: Value::Null,
        }
    }

    #[tokio::test]
    async fn subscribe_before_open_returns_protocol_error() {
        let mut driver = SessionSocketDriver {
            ledger: None,
            session: None,
            pump_interval_ms: DEFAULT_PUMP_INTERVAL_MS,
            play_budget: FeedAdvanceBudget::new(DEFAULT_PLAY_BUDGET_BATCHES).unwrap(),
        };

        let responses = driver
            .handle_client_message(
                SessionClientMessage::SubscribeProjection {
                    request_id: Some("1".to_string()),
                    projection: bbo_spec(),
                },
                0,
            )
            .await;

        assert!(matches!(
            &responses[0],
            SessionServerMessage::Error {
                code: SessionErrorCode::SessionNotOpen,
                ..
            }
        ));
    }

    #[tokio::test]
    async fn open_session_command_opens_cached_replay_dataset() {
        let (_dir, ledger) = api_ledger_with_cached_dataset().await;
        let mut driver = SessionSocketDriver::new(ledger);

        let responses = driver
            .handle_client_message(
                SessionClientMessage::OpenSession {
                    request_id: Some("open".to_string()),
                    session_id: Some("ws-open".to_string()),
                    session_kind: crate::SessionKindRequest::Replay,
                    symbol: "ESH6".to_string(),
                    market_date: NaiveDate::from_ymd_opt(2026, 3, 12).unwrap(),
                    start_ts_ns: None,
                    feed: crate::SessionOpenFeed::Replay {
                        mode: ReplayFeedMode::ExchangeTruth,
                        visibility: crate::ReplayVisibilityMode::Truth,
                    },
                },
                0,
            )
            .await;

        assert_eq!(responses.len(), 2);
        assert!(matches!(
            &responses[0],
            SessionServerMessage::SessionOpening { .. }
        ));
        let SessionServerMessage::SessionOpened { session, .. } = &responses[1] else {
            panic!("expected session_opened");
        };
        assert_eq!(session.session_id, "ws-open");
        assert_eq!(session.market_day.data_start_ns, "1773266400000000000");
    }

    #[tokio::test]
    async fn subscribe_and_advance_return_projection_frames() {
        let mut driver = SessionSocketDriver::with_session(session());
        let responses = driver
            .handle_client_message(
                SessionClientMessage::SubscribeProjection {
                    request_id: Some("sub".to_string()),
                    projection: bbo_spec(),
                },
                0,
            )
            .await;
        let SessionServerMessage::ProjectionSubscribed {
            subscription_id,
            session,
            frames,
            ..
        } = &responses[0]
        else {
            panic!("expected projection_subscribed");
        };
        assert_eq!(*subscription_id, 1);
        assert_eq!(session.session_id, "ws-test");
        assert_eq!(frames.len(), 1);

        let responses = driver
            .handle_client_message(
                SessionClientMessage::Advance {
                    request_id: Some("adv".to_string()),
                    batches: 1,
                },
                0,
            )
            .await;

        let SessionServerMessage::SessionFrameBatch {
            cause,
            applied_batches,
            frames,
            session,
            ..
        } = &responses[0]
        else {
            panic!("expected session_frame_batch");
        };
        assert_eq!(*cause, SessionFrameCause::Advance);
        assert_eq!(*applied_batches, 1);
        assert_eq!(session.feed_seq, 1);
        assert!(!frames.is_empty());
    }

    #[tokio::test]
    async fn seek_returns_reset_frames_with_new_generation() {
        let mut driver = SessionSocketDriver::with_session(session());
        driver
            .handle_client_message(
                SessionClientMessage::SubscribeProjection {
                    request_id: Some("sub".to_string()),
                    projection: bbo_spec(),
                },
                0,
            )
            .await;
        driver
            .handle_client_message(
                SessionClientMessage::Advance {
                    request_id: Some("adv".to_string()),
                    batches: 1,
                },
                0,
            )
            .await;

        let responses = driver
            .handle_client_message(
                SessionClientMessage::Seek {
                    request_id: Some("seek".to_string()),
                    feed_ts_ns: "100".to_string(),
                },
                0,
            )
            .await;

        let SessionServerMessage::SessionFrameBatch {
            cause,
            frames,
            session,
            ..
        } = &responses[0]
        else {
            panic!("expected seek frame batch");
        };
        assert_eq!(*cause, SessionFrameCause::Seek);
        assert_eq!(session.playback, SessionPlaybackState::Paused);
        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0].stamp.generation, 1);
    }

    #[tokio::test]
    async fn play_pump_emits_due_feed_frames_and_pause_stops_pump() {
        let mut driver = SessionSocketDriver::with_session(session());
        driver
            .handle_client_message(
                SessionClientMessage::SubscribeProjection {
                    request_id: Some("sub".to_string()),
                    projection: bbo_spec(),
                },
                0,
            )
            .await;
        let responses = driver
            .handle_client_message(
                SessionClientMessage::Play {
                    request_id: Some("play".to_string()),
                    speed: 1.0,
                    pump_interval_ms: Some(16),
                    budget_batches: Some(10),
                },
                0,
            )
            .await;
        assert!(matches!(
            &responses[0],
            SessionServerMessage::SessionPlayback { .. }
        ));

        let responses = driver.pump(0);
        let SessionServerMessage::SessionFrameBatch {
            cause,
            applied_batches,
            ..
        } = &responses[0]
        else {
            panic!("expected playback frame batch");
        };
        assert_eq!(*cause, SessionFrameCause::PlaybackTick);
        assert_eq!(*applied_batches, 1);

        driver
            .handle_client_message(
                SessionClientMessage::Pause {
                    request_id: Some("pause".to_string()),
                },
                10,
            )
            .await;
        assert!(driver.pump(1_000).is_empty());
    }

    #[tokio::test]
    async fn close_session_keeps_socket_driver_reusable() {
        let mut driver = SessionSocketDriver::with_session(session());

        let responses = driver
            .handle_client_message(
                SessionClientMessage::CloseSession {
                    request_id: Some("close".to_string()),
                },
                0,
            )
            .await;

        assert!(matches!(
            &responses[0],
            SessionServerMessage::SessionClosed { .. }
        ));
        let responses = driver
            .handle_client_message(
                SessionClientMessage::Snapshot {
                    request_id: Some("snapshot".to_string()),
                },
                0,
            )
            .await;
        assert!(matches!(
            &responses[0],
            SessionServerMessage::Error {
                code: SessionErrorCode::SessionNotOpen,
                ..
            }
        ));
    }

    #[tokio::test]
    async fn play_before_open_does_not_mutate_driver_defaults() {
        let mut driver = SessionSocketDriver {
            ledger: None,
            session: None,
            pump_interval_ms: DEFAULT_PUMP_INTERVAL_MS,
            play_budget: FeedAdvanceBudget::new(DEFAULT_PLAY_BUDGET_BATCHES).unwrap(),
        };

        let responses = driver
            .handle_client_message(
                SessionClientMessage::Play {
                    request_id: Some("play".to_string()),
                    speed: 2.0,
                    pump_interval_ms: Some(16),
                    budget_batches: Some(1),
                },
                0,
            )
            .await;

        assert!(matches!(
            &responses[0],
            SessionServerMessage::Error {
                code: SessionErrorCode::SessionNotOpen,
                ..
            }
        ));
        assert_eq!(driver.pump_interval_ms, DEFAULT_PUMP_INTERVAL_MS);
        assert_eq!(driver.play_budget.max_batches, DEFAULT_PLAY_BUDGET_BATCHES);
    }

    #[tokio::test]
    async fn malformed_json_error_preserves_request_id_when_available() {
        let mut driver = SessionSocketDriver::with_session(session());

        let responses = driver
            .handle_text(
                r#"{"type":"advance","request_id":"bad","batches":"nope"}"#,
                0,
            )
            .await;

        let SessionServerMessage::Error {
            request_id, code, ..
        } = &responses[0]
        else {
            panic!("expected error");
        };
        assert_eq!(request_id.as_deref(), Some("bad"));
        assert_eq!(*code, SessionErrorCode::InvalidJson);
    }
}
