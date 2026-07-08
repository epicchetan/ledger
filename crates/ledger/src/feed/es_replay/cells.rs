use crate::clock::ClockSnapshot;
use crate::market::{EsMboEvent, UnixNanos};
use crate::LedgerError;
use cache::{ArrayKey, Cache, CellDescriptor, CellKind, CellOwner, Key, ValueKey};
use serde::{Deserialize, Serialize};

pub const ES_REPLAY_FEED_COMPONENT_ID: &str = "feed.databento.es_replay";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EsMboFeedBatch {
    pub feed_seq: u64,
    pub batch_idx: usize,
    pub ts_event_ns: UnixNanos,
    pub source_first_ts_ns: UnixNanos,
    pub source_last_ts_ns: UnixNanos,
    pub events: Vec<EsMboEvent>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EsReplayCursor {
    pub epoch: u64,
    pub feed_seq: u64,
    pub batch_idx: usize,
    pub total_batches: usize,
    pub ts_event_ns: Option<UnixNanos>,
    pub next_ts_event_ns: Option<UnixNanos>,
    pub ended: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct EsReplayStatus {
    pub raw_object_id: String,
    pub artifact_object_id: Option<String>,
    pub clock: ClockSnapshot,
    pub cursor: EsReplayCursor,
}

#[derive(Debug, Clone)]
pub struct EsReplayCells {
    pub batches: ArrayKey<EsMboFeedBatch>,
    pub cursor: ValueKey<EsReplayCursor>,
    pub status: ValueKey<EsReplayStatus>,
}

impl EsReplayCells {
    pub fn register(cache: &Cache) -> Result<Self, LedgerError> {
        let owner = feed_owner()?;
        let batches = cache.register_array::<EsMboFeedBatch>(
            descriptor(
                "feed.databento.es_replay.batches",
                owner.clone(),
                CellKind::Array,
                false,
            )?,
            Vec::new(),
        )?;
        let cursor = cache.register_value::<EsReplayCursor>(
            descriptor(
                "feed.databento.es_replay.cursor",
                owner.clone(),
                CellKind::Value,
                true,
            )?,
            None,
        )?;
        let status = cache.register_value::<EsReplayStatus>(
            descriptor(
                "feed.databento.es_replay.status",
                owner,
                CellKind::Value,
                true,
            )?,
            None,
        )?;
        Ok(Self {
            batches,
            cursor,
            status,
        })
    }
}

pub fn es_replay_component_id() -> runtime::ComponentId {
    runtime::ComponentId::new(ES_REPLAY_FEED_COMPONENT_ID)
        .expect("ES replay component id is a valid cache key")
}

pub fn feed_owner() -> Result<CellOwner, LedgerError> {
    Ok(CellOwner::new(ES_REPLAY_FEED_COMPONENT_ID)?)
}

fn descriptor(
    key: &str,
    owner: CellOwner,
    kind: CellKind,
    public_read: bool,
) -> Result<CellDescriptor, LedgerError> {
    Ok(CellDescriptor {
        key: Key::new(key)?,
        owner,
        kind,
        public_read,
    })
}
