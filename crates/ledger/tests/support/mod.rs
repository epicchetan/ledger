use std::sync::Arc;

use ledger::feed::es_replay::{
    encode_event_store, ES_MBO_EVENT_STORE_FILE_NAME, ES_MBO_EVENT_STORE_KIND,
    ES_MBO_EVENT_STORE_VERSION, RAW_DATABENTO_DBN_ZST_KIND,
};
use ledger::market::{
    build_batches, BookAction, BookSide, EsMboEvent, EsMboEventStore, MarketDay, PriceTicks,
};
use store::{
    test_util::MemoryRemote, RegisterFileRequest, RemoteStore, Store, StoreConfig,
    StoreObjectDescriptor, StoreObjectId, StoreObjectRole,
};
use tempfile::{tempdir, TempDir};

pub struct StoreFixture {
    pub _tempdir: TempDir,
    pub store: Arc<Store<MemoryRemote>>,
}

pub fn store_fixture() -> StoreFixture {
    let tempdir = tempdir().unwrap();
    let remote = Arc::new(MemoryRemote::new());
    let store = Arc::new(
        Store::open(
            tempdir.path(),
            StoreConfig {
                local_max_bytes: 1024 * 1024,
            },
            remote.clone(),
        )
        .unwrap(),
    );
    StoreFixture {
        _tempdir: tempdir,
        store,
    }
}

pub async fn fabricate_prepared_day<S>(
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
    let raw = store
        .register_file(RegisterFileRequest {
            path: &raw_path,
            role: StoreObjectRole::Raw,
            kind: RAW_DATABENTO_DBN_ZST_KIND.to_string(),
            file_name: Some("raw.dbn.zst".to_string()),
            format: Some("dbn.zst".to_string()),
            media_type: None,
            lineage: Vec::new(),
            metadata_json: serde_json::json!({}),
        })
        .await
        .unwrap();

    let batches = build_batches(&events);
    let event_store = EsMboEventStore { events, batches };
    let encoded = encode_event_store(&event_store);
    let artifact_path = tempdir.path().join(ES_MBO_EVENT_STORE_FILE_NAME);
    tokio::fs::write(&artifact_path, encoded).await.unwrap();
    let market_day = MarketDay::parse("2026-03-10").unwrap();
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
            metadata_json: serde_json::json!({
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

pub fn event(ts_event_ns: u64, sequence: u64) -> EsMboEvent {
    EsMboEvent {
        ts_event_ns,
        ts_recv_ns: ts_event_ns,
        sequence,
        action: BookAction::Add,
        side: Some(BookSide::Bid),
        price_ticks: Some(PriceTicks(18_000 + sequence as i64)),
        size: 1,
        order_id: sequence,
        flags: 0,
        is_last: true,
    }
}

pub fn trade(
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
