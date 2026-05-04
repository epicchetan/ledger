use chrono::NaiveDate;
use ledger::{OpenSessionRequest, ReplayFeedConfig, ReplayFeedMode, SessionFeedConfig};
use ledger_domain::{
    Bbo, ExecutionProfile, MarketDay, MarketDayStatus, ProjectionFrame, ProjectionKey,
    ProjectionSpec, UnixNanos, VisibilityProfile,
};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::fmt;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum SessionClientMessage {
    OpenSession {
        request_id: Option<String>,
        #[serde(default)]
        session_id: Option<String>,
        session_kind: SessionKindRequest,
        symbol: String,
        market_date: NaiveDate,
        #[serde(default)]
        start_ts_ns: Option<String>,
        feed: SessionOpenFeed,
    },
    SubscribeProjection {
        request_id: Option<String>,
        projection: ProjectionSpec,
    },
    UnsubscribeProjection {
        request_id: Option<String>,
        subscription_id: u64,
    },
    Advance {
        request_id: Option<String>,
        batches: usize,
    },
    Play {
        request_id: Option<String>,
        speed: f64,
        #[serde(default)]
        pump_interval_ms: Option<u64>,
        #[serde(default)]
        budget_batches: Option<usize>,
    },
    Pause {
        request_id: Option<String>,
    },
    SetSpeed {
        request_id: Option<String>,
        speed: f64,
    },
    Seek {
        request_id: Option<String>,
        feed_ts_ns: String,
    },
    Snapshot {
        request_id: Option<String>,
    },
    CloseSession {
        request_id: Option<String>,
    },
}

impl SessionClientMessage {
    pub fn request_id(&self) -> Option<&str> {
        match self {
            Self::OpenSession { request_id, .. }
            | Self::SubscribeProjection { request_id, .. }
            | Self::UnsubscribeProjection { request_id, .. }
            | Self::Advance { request_id, .. }
            | Self::Play { request_id, .. }
            | Self::Pause { request_id }
            | Self::SetSpeed { request_id, .. }
            | Self::Seek { request_id, .. }
            | Self::Snapshot { request_id }
            | Self::CloseSession { request_id } => request_id.as_deref(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SessionKindRequest {
    Replay,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum SessionOpenFeed {
    Replay {
        #[serde(default = "default_replay_feed_mode")]
        mode: ReplayFeedMode,
        #[serde(default)]
        visibility: ReplayVisibilityMode,
    },
}

impl SessionOpenFeed {
    pub fn replay_config(&self) -> ReplayFeedConfig {
        match self {
            Self::Replay { mode, visibility } => ReplayFeedConfig {
                mode: *mode,
                execution_profile: ExecutionProfile::default(),
                visibility_profile: visibility.profile(),
            },
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
#[serde(rename_all = "snake_case")]
pub enum ReplayVisibilityMode {
    #[default]
    Truth,
    Delayed,
}

impl ReplayVisibilityMode {
    fn profile(self) -> VisibilityProfile {
        match self {
            Self::Truth => VisibilityProfile::truth(),
            Self::Delayed => VisibilityProfile::default(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum SessionServerMessage {
    ServerHello {
        protocol: String,
        version: u16,
        capabilities: Vec<String>,
    },
    SessionOpening {
        request_id: Option<String>,
        session_kind: SessionKindRequest,
        symbol: String,
        market_date: NaiveDate,
    },
    SessionOpened {
        request_id: Option<String>,
        session: SessionSnapshotDto,
    },
    ProjectionSubscribed {
        request_id: Option<String>,
        subscription_id: u64,
        projection_key: ProjectionKey,
        projection_key_display: String,
        session: SessionSnapshotDto,
        frames: Vec<ProjectionFrame>,
    },
    ProjectionUnsubscribed {
        request_id: Option<String>,
        subscription_id: u64,
    },
    SessionFrameBatch {
        request_id: Option<String>,
        cause: SessionFrameCause,
        applied_batches: usize,
        budget_exhausted: bool,
        behind: bool,
        session: SessionSnapshotDto,
        frames: Vec<ProjectionFrame>,
    },
    SessionSnapshot {
        request_id: Option<String>,
        session: SessionSnapshotDto,
    },
    SessionPlayback {
        request_id: Option<String>,
        session: SessionSnapshotDto,
    },
    SessionClosed {
        request_id: Option<String>,
    },
    Error {
        request_id: Option<String>,
        code: SessionErrorCode,
        message: String,
    },
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SessionSnapshotDto {
    pub session_id: String,
    pub replay_dataset_id: String,
    pub market_day: SessionMarketDayDto,
    pub feed_seq: u64,
    pub feed_ts_ns: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub next_feed_ts_ns: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_first_ts_ns: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_last_ts_ns: Option<String>,
    pub batch_idx: usize,
    pub total_batches: usize,
    pub playback: ledger::SessionPlaybackState,
    pub speed: f64,
    pub book_checksum: String,
    pub bbo: Option<Bbo>,
    pub frame_count: usize,
    pub fill_count: usize,
}

impl From<ledger::SessionSnapshot> for SessionSnapshotDto {
    fn from(snapshot: ledger::SessionSnapshot) -> Self {
        Self {
            session_id: snapshot.session_id,
            replay_dataset_id: snapshot.replay_dataset_id,
            market_day: snapshot.market_day.into(),
            feed_seq: snapshot.feed_seq,
            feed_ts_ns: snapshot.feed_ts_ns,
            next_feed_ts_ns: snapshot.next_feed_ts_ns,
            source_first_ts_ns: snapshot.source_first_ts_ns,
            source_last_ts_ns: snapshot.source_last_ts_ns,
            batch_idx: snapshot.batch_idx,
            total_batches: snapshot.total_batches,
            playback: snapshot.playback,
            speed: snapshot.speed,
            book_checksum: snapshot.book_checksum,
            bbo: snapshot.bbo,
            frame_count: snapshot.frame_count,
            fill_count: snapshot.fill_count,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SessionMarketDayDto {
    pub id: String,
    pub root: String,
    pub contract_symbol: String,
    pub market_date: NaiveDate,
    pub timezone: String,
    pub data_start_ns: String,
    pub data_end_ns: String,
    pub rth_start_ns: String,
    pub rth_end_ns: String,
    pub status: MarketDayStatus,
    pub metadata_json: Value,
}

impl From<MarketDay> for SessionMarketDayDto {
    fn from(market_day: MarketDay) -> Self {
        Self {
            id: market_day.id,
            root: market_day.root,
            contract_symbol: market_day.contract_symbol,
            market_date: market_day.market_date,
            timezone: market_day.timezone,
            data_start_ns: market_day.data_start_ns.to_string(),
            data_end_ns: market_day.data_end_ns.to_string(),
            rth_start_ns: market_day.rth_start_ns.to_string(),
            rth_end_ns: market_day.rth_end_ns.to_string(),
            status: market_day.status,
            metadata_json: market_day.metadata_json,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SessionFrameCause {
    Advance,
    PlaybackTick,
    Seek,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SessionErrorCode {
    InvalidJson,
    InvalidCommand,
    SessionNotOpen,
    SessionAlreadyOpen,
    UnsupportedSessionKind,
    UnsupportedFeedKind,
    InvalidTimestamp,
    InvalidProjection,
    InvalidSubscription,
    CommandConflict,
    LedgerError,
    Internal,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SessionProtocolError {
    pub code: SessionErrorCode,
    pub message: String,
}

impl SessionProtocolError {
    pub fn new(code: SessionErrorCode, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
        }
    }
}

impl fmt::Display for SessionProtocolError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for SessionProtocolError {}

pub fn open_session_request(
    session_id: Option<String>,
    session_kind: SessionKindRequest,
    symbol: String,
    market_date: NaiveDate,
    start_ts_ns: Option<String>,
    feed: SessionOpenFeed,
) -> Result<OpenSessionRequest, SessionProtocolError> {
    if session_kind != SessionKindRequest::Replay {
        return Err(SessionProtocolError::new(
            SessionErrorCode::UnsupportedSessionKind,
            "only replay sessions are supported",
        ));
    }
    let feed_config = feed.replay_config();
    Ok(OpenSessionRequest {
        session_id,
        symbol,
        market_date,
        start_ts_ns: parse_optional_ns(start_ts_ns, "start_ts_ns")?,
        feed: SessionFeedConfig::Replay(feed_config),
    })
}

pub fn parse_required_ns(value: &str, field: &str) -> Result<UnixNanos, SessionProtocolError> {
    value.parse::<UnixNanos>().map_err(|err| {
        SessionProtocolError::new(
            SessionErrorCode::InvalidTimestamp,
            format!("invalid {field} `{value}`: {err}"),
        )
    })
}

fn parse_optional_ns(
    value: Option<String>,
    field: &str,
) -> Result<Option<UnixNanos>, SessionProtocolError> {
    value
        .as_deref()
        .map(|value| parse_required_ns(value, field))
        .transpose()
}

pub fn error_message(
    request_id: Option<String>,
    code: SessionErrorCode,
    message: impl Into<String>,
) -> SessionServerMessage {
    SessionServerMessage::Error {
        request_id,
        code,
        message: message.into(),
    }
}

pub fn request_id_from_value(value: &Value) -> Option<String> {
    value
        .get("request_id")
        .and_then(Value::as_str)
        .map(ToString::to_string)
}

fn default_replay_feed_mode() -> ReplayFeedMode {
    ReplayFeedMode::ExchangeTruth
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn open_session_protocol_maps_ns_strings_to_ledger_request() {
        let message: SessionClientMessage = serde_json::from_value(json!({
            "type": "open_session",
            "request_id": "1",
            "session_kind": "replay",
            "symbol": "ESH6",
            "market_date": "2026-03-12",
            "start_ts_ns": "1773266400000000000",
            "feed": {
                "kind": "replay",
                "mode": "exchange_truth",
                "visibility": "truth"
            }
        }))
        .unwrap();

        let SessionClientMessage::OpenSession {
            request_id,
            session_id,
            session_kind,
            symbol,
            market_date,
            start_ts_ns,
            feed,
        } = message
        else {
            panic!("expected open_session");
        };

        let request = open_session_request(
            session_id,
            session_kind,
            symbol,
            market_date,
            start_ts_ns,
            feed,
        )
        .unwrap();

        assert_eq!(request_id.as_deref(), Some("1"));
        assert_eq!(request.symbol, "ESH6");
        assert_eq!(request.start_ts_ns, Some(1_773_266_400_000_000_000));
    }

    #[test]
    fn invalid_nanosecond_string_is_a_protocol_error() {
        let err = parse_required_ns("nope", "feed_ts_ns").unwrap_err();

        assert_eq!(err.code, SessionErrorCode::InvalidTimestamp);
        assert!(err.message.contains("feed_ts_ns"));
    }

    #[test]
    fn frame_batch_serializes_with_string_timestamps_from_session_snapshot() {
        let message = error_message(
            Some("7".to_string()),
            SessionErrorCode::SessionNotOpen,
            "open_session must be sent before advance",
        );
        let value = serde_json::to_value(message).unwrap();

        assert_eq!(value["type"], "error");
        assert_eq!(value["request_id"], "7");
        assert_eq!(value["code"], "session_not_open");
    }

    #[test]
    fn session_snapshot_dto_maps_market_day_nanoseconds_to_strings() {
        let market_day =
            MarketDay::resolve_es("ESH6", NaiveDate::from_ymd_opt(2026, 3, 12).unwrap()).unwrap();
        let snapshot = ledger::SessionSnapshot {
            session_id: "session".to_string(),
            replay_dataset_id: "dataset".to_string(),
            market_day,
            feed_seq: 0,
            feed_ts_ns: "1773266400000000000".to_string(),
            next_feed_ts_ns: None,
            source_first_ts_ns: None,
            source_last_ts_ns: None,
            batch_idx: 0,
            total_batches: 0,
            playback: ledger::SessionPlaybackState::Paused,
            speed: 1.0,
            book_checksum: "checksum".to_string(),
            bbo: None,
            frame_count: 0,
            fill_count: 0,
        };

        let value = serde_json::to_value(SessionSnapshotDto::from(snapshot)).unwrap();

        assert_eq!(value["market_day"]["data_start_ns"], "1773266400000000000");
        assert!(value["market_day"]["data_start_ns"].is_string());
    }
}
