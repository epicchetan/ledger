use crate::market::{build_batches, EsMboEvent, EsMboEventStore, MarketDay};
use crate::LedgerError;
use serde::Serialize;
use serde_json::{json, Value};
use std::path::PathBuf;
use store::{
    ObjectFilter, RegisterFileRequest, RemoteStore, Store, StoreObjectDescriptor, StoreObjectId,
    StoreObjectRole,
};
use tokio::sync::mpsc::UnboundedSender;

use super::{
    decode_event_store, decode_mbo_events, encode_event_store, read_event_store_file,
    ES_MBO_EVENT_STORE_FILE_NAME, ES_MBO_EVENT_STORE_KIND, ES_MBO_EVENT_STORE_VERSION,
    RAW_DATABENTO_DBN_ZST_KIND,
};

#[derive(Debug, Clone, Serialize)]
pub struct EsReplayArtifact {
    pub descriptor: StoreObjectDescriptor,
    pub path: PathBuf,
    pub reused: bool,
    pub market_day: MarketDay,
    pub event_count: u64,
    pub batch_count: u64,
    pub first_ts_event_ns: u64,
    pub last_ts_event_ns: u64,
    pub warnings: Vec<String>,
}

impl EsReplayArtifact {
    pub fn summary(&self, raw_object_id: &StoreObjectId) -> EsPrepareSummary {
        EsPrepareSummary {
            raw_object_id: raw_object_id.to_string(),
            artifact_object_id: self.descriptor.id.to_string(),
            artifact_reused: self.reused,
            market_day: self.market_day.to_string(),
            event_count: self.event_count,
            batch_count: self.batch_count,
            first_ts_event_ns: self.first_ts_event_ns.to_string(),
            last_ts_event_ns: self.last_ts_event_ns.to_string(),
            warnings: self.warnings.clone(),
        }
    }
}

#[derive(Debug, Clone, Serialize)]
pub struct EsPrepareSummary {
    pub raw_object_id: String,
    pub artifact_object_id: String,
    pub artifact_reused: bool,
    pub market_day: String,
    pub event_count: u64,
    pub batch_count: u64,
    pub first_ts_event_ns: String,
    pub last_ts_event_ns: String,
    pub warnings: Vec<String>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "stage", rename_all = "snake_case")]
pub enum PrepareProgress {
    Hydrating,
    Decoding { records: u64 },
    Encoding { events: u64, batches: u64 },
    Registering,
}

#[derive(Debug, Clone)]
pub struct ArtifactLookup {
    pub descriptor: Option<StoreObjectDescriptor>,
    pub matching_count: usize,
}

pub async fn prepare_es_replay_artifact<S>(
    store: &Store<S>,
    raw_object_id: &StoreObjectId,
    force: bool,
    progress: Option<UnboundedSender<PrepareProgress>>,
) -> Result<EsReplayArtifact, LedgerError>
where
    S: RemoteStore + 'static,
{
    let raw = store
        .get_object(raw_object_id)?
        .ok_or_else(|| LedgerError::InvalidRaw(format!("{raw_object_id} not found")))?;
    ensure_es_raw(&raw)?;

    let artifacts = store.list_objects(ObjectFilter {
        role: Some(StoreObjectRole::Artifact),
        kind: Some(ES_MBO_EVENT_STORE_KIND.to_string()),
        id_prefix: None,
    })?;
    let lookup = find_es_replay_artifact_descriptor(&artifacts, raw_object_id);
    let mut warnings = ambiguity_warnings(&lookup);

    if let Some(descriptor) = lookup.descriptor.clone() {
        if !force {
            // The reuse path still hydrates (and fully decodes) the artifact,
            // so surface it as such instead of sitting on "queued".
            if let Some(progress) = &progress {
                let _ = progress.send(PrepareProgress::Hydrating);
            }
            match hydrate_decode_artifact(store, raw_object_id, descriptor.clone(), &mut warnings)
                .await
            {
                Ok(mut artifact) => {
                    repair_raw_market_day(store, raw_object_id, &artifact.market_day).await?;
                    offload_raw_local_copy(store, raw_object_id, &mut artifact.warnings);
                    return Ok(artifact);
                }
                Err(error) => {
                    warnings.push(format!(
                        "existing artifact {} invalid; rebuilding: {error}",
                        descriptor.id
                    ));
                }
            }
        }
    }

    if let Some(progress) = &progress {
        let _ = progress.send(PrepareProgress::Hydrating);
    }
    let hydrated_raw = store.hydrate(raw_object_id).await?;
    let decode_progress = progress.clone();
    let raw_path = hydrated_raw.path.clone();
    let events = tokio::task::spawn_blocking(move || decode_mbo_events(&raw_path, decode_progress))
        .await
        .map_err(|err| LedgerError::InvalidDbnRecord(err.to_string()))??;
    let batches = build_batches(&events);
    let (market_day, first_ts_event_ns, last_ts_event_ns) = validate_market_day_bounds(&events)?;
    let event_store = EsMboEventStore { events, batches };
    event_store
        .validate()
        .map_err(|err| LedgerError::InvalidArtifact(err.to_string()))?;

    if let Some(progress) = &progress {
        let _ = progress.send(PrepareProgress::Encoding {
            events: event_store.events.len() as u64,
            batches: event_store.batches.len() as u64,
        });
    }
    let event_store_for_encode = event_store.clone();
    let encoded = tokio::task::spawn_blocking(move || {
        let bytes = encode_event_store(&event_store_for_encode);
        let decoded = decode_event_store(&bytes)?;
        if decoded != event_store_for_encode {
            return Err(LedgerError::InvalidArtifact(
                "artifact codec round trip changed event store".to_string(),
            ));
        }
        Ok(bytes)
    })
    .await
    .map_err(|err| LedgerError::InvalidArtifact(err.to_string()))??;

    if let Some(progress) = &progress {
        let _ = progress.send(PrepareProgress::Registering);
    }
    let artifact_path = temp_artifact_path(raw_object_id);
    if let Some(parent) = artifact_path.parent() {
        tokio::fs::create_dir_all(parent)
            .await
            .map_err(anyhow::Error::from)?;
    }
    tokio::fs::write(&artifact_path, &encoded)
        .await
        .map_err(anyhow::Error::from)?;

    let descriptor = store
        .register_file(RegisterFileRequest {
            path: &artifact_path,
            role: StoreObjectRole::Artifact,
            kind: ES_MBO_EVENT_STORE_KIND.to_string(),
            file_name: Some(ES_MBO_EVENT_STORE_FILE_NAME.to_string()),
            format: Some("ledger.es_mbo_event_store.v1".to_string()),
            media_type: None,
            lineage: vec![raw_object_id.clone()],
            metadata_json: artifact_metadata(
                raw_object_id,
                market_day,
                event_store.events.len() as u64,
                event_store.batches.len() as u64,
                first_ts_event_ns,
                last_ts_event_ns,
            ),
        })
        .await?;
    tokio::fs::remove_file(&artifact_path).await.ok();
    repair_raw_market_day(store, raw_object_id, &market_day).await?;
    let hydrated = store.hydrate(&descriptor.id).await?;
    offload_raw_local_copy(store, raw_object_id, &mut warnings);

    Ok(EsReplayArtifact {
        descriptor: hydrated.descriptor,
        path: hydrated.path,
        reused: false,
        market_day,
        event_count: event_store.events.len() as u64,
        batch_count: event_store.batches.len() as u64,
        first_ts_event_ns,
        last_ts_event_ns,
        warnings,
    })
}

// Raws never rest locally: once a valid artifact exists the raw's local copy
// is dead weight (gigabytes of DBN), while R2 keeps the paid bytes for future
// rebuilds. Best-effort — a failed eviction is a warning, never a failed
// prepare, because the artifact is already good.
fn offload_raw_local_copy<S>(
    store: &Store<S>,
    raw_object_id: &StoreObjectId,
    warnings: &mut Vec<String>,
) where
    S: RemoteStore + 'static,
{
    if let Err(error) = store.offload_object(raw_object_id) {
        warnings.push(format!(
            "failed to offload raw {raw_object_id} local copy: {error}"
        ));
    }
}

pub fn find_es_replay_artifact_descriptor(
    artifacts: &[StoreObjectDescriptor],
    raw_object_id: &StoreObjectId,
) -> ArtifactLookup {
    let raw_id = raw_object_id.to_string();
    let mut matches = artifacts
        .iter()
        .filter(|descriptor| {
            descriptor.role == StoreObjectRole::Artifact
                && descriptor.kind == ES_MBO_EVENT_STORE_KIND
                && descriptor.lineage.len() == 1
                && descriptor.lineage[0] == *raw_object_id
                && descriptor
                    .metadata_json
                    .get("raw_object_id")
                    .and_then(Value::as_str)
                    == Some(raw_id.as_str())
                && descriptor
                    .metadata_json
                    .get("version")
                    .and_then(Value::as_u64)
                    == Some(ES_MBO_EVENT_STORE_VERSION)
        })
        .cloned()
        .collect::<Vec<_>>();
    matches.sort_by_key(|descriptor| std::cmp::Reverse(descriptor.updated_at_ns));
    ArtifactLookup {
        matching_count: matches.len(),
        descriptor: matches.into_iter().next(),
    }
}

fn ensure_es_raw(descriptor: &StoreObjectDescriptor) -> Result<(), LedgerError> {
    if descriptor.role != StoreObjectRole::Raw || descriptor.kind != RAW_DATABENTO_DBN_ZST_KIND {
        return Err(LedgerError::InvalidRaw(format!(
            "{} is role={} kind={}",
            descriptor.id,
            descriptor.role.as_str(),
            descriptor.kind
        )));
    }
    Ok(())
}

async fn hydrate_decode_artifact<S>(
    store: &Store<S>,
    _raw_object_id: &StoreObjectId,
    descriptor: StoreObjectDescriptor,
    warnings: &mut Vec<String>,
) -> Result<EsReplayArtifact, LedgerError>
where
    S: RemoteStore + 'static,
{
    let hydrated = store.hydrate(&descriptor.id).await?;
    let event_store = read_event_store_file(&hydrated.path)?;
    let market_day = metadata_market_day(&descriptor)?;
    let event_count =
        metadata_u64(&descriptor, "event_count").unwrap_or(event_store.events.len() as u64);
    let batch_count =
        metadata_u64(&descriptor, "batch_count").unwrap_or(event_store.batches.len() as u64);
    let first_ts_event_ns = metadata_string_u64(&descriptor, "first_ts_event_ns")
        .or_else(|| event_store.events.first().map(|event| event.ts_event_ns))
        .ok_or_else(|| LedgerError::InvalidArtifact("artifact has no events".to_string()))?;
    let last_ts_event_ns = metadata_string_u64(&descriptor, "last_ts_event_ns")
        .or_else(|| event_store.events.last().map(|event| event.ts_event_ns))
        .ok_or_else(|| LedgerError::InvalidArtifact("artifact has no events".to_string()))?;
    warnings.extend(ambiguity_warnings(&ArtifactLookup {
        descriptor: Some(descriptor.clone()),
        matching_count: 1,
    }));
    Ok(EsReplayArtifact {
        descriptor: hydrated.descriptor,
        path: hydrated.path,
        reused: true,
        market_day,
        event_count,
        batch_count,
        first_ts_event_ns,
        last_ts_event_ns,
        warnings: warnings.clone(),
    })
}

// Session membership is judged by ts_recv (delivery time), not ts_event.
// Databento serves a session's MBO with a book snapshot at the open: every
// order already resting in the book is re-added carrying its ORIGINAL
// ts_event, which for a deep or GTC order can be days old. Those snapshot
// records are the opening book state and must be kept, but keying the
// bounds check on ts_event would reject them as foreign-day data. Every
// record is DELIVERED within the session, so ts_recv is the in-session
// clock: it anchors the market day and bounds every event. ts_event is
// still preserved on each event and reported as the artifact's event-time
// span for display.
fn validate_market_day_bounds(events: &[EsMboEvent]) -> Result<(MarketDay, u64, u64), LedgerError> {
    let first = events
        .first()
        .ok_or_else(|| LedgerError::MarketDay("raw has no MBO events".to_string()))?;
    let market_day = MarketDay::resolve_es(first.ts_recv_ns)?;
    let (start, end) = market_day.es_session_bounds_utc()?;
    for event in events {
        if event.ts_recv_ns < start || event.ts_recv_ns >= end {
            return Err(LedgerError::MarketDay(format!(
                "event ts_recv {} is outside {} session [{start}, {end})",
                event.ts_recv_ns, market_day
            )));
        }
    }
    let last_ts_event_ns = events
        .last()
        .map(|event| event.ts_event_ns)
        .unwrap_or(first.ts_event_ns);
    Ok((market_day, first.ts_event_ns, last_ts_event_ns))
}

async fn repair_raw_market_day<S>(
    store: &Store<S>,
    raw_object_id: &StoreObjectId,
    market_day: &MarketDay,
) -> Result<StoreObjectDescriptor, LedgerError>
where
    S: RemoteStore + 'static,
{
    let raw = store
        .get_object(raw_object_id)?
        .ok_or_else(|| LedgerError::InvalidRaw(format!("{raw_object_id} not found")))?;
    let market_day_string = market_day.to_string();
    let mut metadata = match raw.metadata_json.clone() {
        Value::Object(map) => map,
        _ => serde_json::Map::new(),
    };
    if metadata.get("market_day").and_then(Value::as_str) == Some(market_day_string.as_str()) {
        return Ok(raw);
    }
    metadata.insert("market_day".to_string(), Value::String(market_day_string));
    Ok(store
        .update_metadata(raw_object_id, Value::Object(metadata))
        .await?)
}

fn artifact_metadata(
    raw_object_id: &StoreObjectId,
    market_day: MarketDay,
    event_count: u64,
    batch_count: u64,
    first_ts_event_ns: u64,
    last_ts_event_ns: u64,
) -> Value {
    json!({
        "artifact": "es_mbo_event_store",
        "version": ES_MBO_EVENT_STORE_VERSION,
        "raw_object_id": raw_object_id.to_string(),
        "market_day": market_day.to_string(),
        "event_count": event_count,
        "batch_count": batch_count,
        "first_ts_event_ns": first_ts_event_ns.to_string(),
        "last_ts_event_ns": last_ts_event_ns.to_string(),
    })
}

fn metadata_market_day(descriptor: &StoreObjectDescriptor) -> Result<MarketDay, LedgerError> {
    descriptor
        .metadata_json
        .get("market_day")
        .and_then(Value::as_str)
        .ok_or_else(|| {
            LedgerError::InvalidArtifact(format!("artifact {} missing market_day", descriptor.id))
        })?
        .parse()
}

fn metadata_u64(descriptor: &StoreObjectDescriptor, key: &str) -> Option<u64> {
    descriptor.metadata_json.get(key).and_then(Value::as_u64)
}

fn metadata_string_u64(descriptor: &StoreObjectDescriptor, key: &str) -> Option<u64> {
    descriptor
        .metadata_json
        .get(key)
        .and_then(Value::as_str)
        .and_then(|value| value.parse::<u64>().ok())
}

fn temp_artifact_path(raw_object_id: &StoreObjectId) -> PathBuf {
    std::env::temp_dir().join(format!(
        "ledger-es-mbo-event-store-{}-{}.bin",
        raw_object_id.as_str(),
        store::now_ns()
    ))
}

fn ambiguity_warnings(lookup: &ArtifactLookup) -> Vec<String> {
    if lookup.matching_count > 1 {
        vec![format!(
            "{} matching artifacts found; newest descriptor selected",
            lookup.matching_count
        )]
    } else {
        Vec::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::market::BookAction;
    use chrono::NaiveDate;

    const NS_PER_DAY: u64 = 86_400 * 1_000_000_000;

    fn day(y: i32, m: u32, d: u32) -> MarketDay {
        MarketDay(NaiveDate::from_ymd_opt(y, m, d).unwrap())
    }

    fn event(ts_event_ns: u64, ts_recv_ns: u64) -> EsMboEvent {
        EsMboEvent {
            ts_event_ns,
            ts_recv_ns,
            sequence: 0,
            action: BookAction::Add,
            side: None,
            price_ticks: None,
            size: 0,
            order_id: 0,
            flags: 0,
            is_last: true,
        }
    }

    // The snapshot re-add: ts_event days before the session, ts_recv at the
    // open. It must be kept and the day derived from ts_recv.
    #[test]
    fn snapshot_readd_is_in_session_by_recv() {
        let d = day(2026, 3, 11);
        let (start, end) = d.es_session_bounds_utc().unwrap();
        let snapshot = event(start - 3 * NS_PER_DAY, start + 3_000_000);
        let regular = event(start + 1_000_000_000, start + 1_000_000_000);
        let last = event(end - 1, end - 1);
        let events = vec![snapshot.clone(), regular, last];

        let (market_day, first_ts_event, last_ts_event) =
            validate_market_day_bounds(&events).unwrap();
        assert_eq!(market_day, d);
        // first_ts_event reports the (old) ts_event of the snapshot re-add,
        // preserved verbatim; the day still resolves to the delivery session.
        assert_eq!(first_ts_event, snapshot.ts_event_ns);
        assert_eq!(last_ts_event, end - 1);
    }

    // The day is anchored on the first event's ts_recv, never its ts_event.
    #[test]
    fn day_is_derived_from_recv_not_event() {
        let d = day(2026, 3, 11);
        let (start, _end) = d.es_session_bounds_utc().unwrap();
        // ts_event lands a full week earlier (a different session), ts_recv is
        // in the 2026-03-11 session.
        let first = event(start - 7 * NS_PER_DAY, start + 5_000_000);
        let (market_day, _, _) = validate_market_day_bounds(&[first]).unwrap();
        assert_eq!(market_day, d);
    }

    // A record delivered in another session is real cross-session bleed and
    // must hard-error with no artifact.
    #[test]
    fn recv_after_session_end_errors() {
        let d = day(2026, 3, 11);
        let (start, end) = d.es_session_bounds_utc().unwrap();
        let ok = event(start + 1_000_000, start + 1_000_000);
        let bled = event(end + 1_000_000, end + 1_000_000);
        let err = validate_market_day_bounds(&[ok, bled]).unwrap_err();
        assert!(matches!(err, LedgerError::MarketDay(_)), "got {err:?}");
    }

    // A delivery landing in the 17:00-18:00 ET maintenance halt is out of
    // session and must error (here as the first-event day anchor).
    #[test]
    fn recv_in_maintenance_gap_errors() {
        let d = day(2026, 3, 11);
        let (start, _end) = d.es_session_bounds_utc().unwrap();
        // 30 minutes before the open is inside the 17:00-18:00 ET halt.
        let gap = event(
            start - 30 * 60 * 1_000_000_000,
            start - 30 * 60 * 1_000_000_000,
        );
        let err = validate_market_day_bounds(&[gap]).unwrap_err();
        assert!(matches!(err, LedgerError::MarketDay(_)), "got {err:?}");
    }
}
