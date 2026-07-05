use ledger::feed::es_replay::{decode_event_store, encode_event_store};
use ledger::market::{
    build_batches, BookAction, BookSide, EsMboBatchSpan, EsMboEvent, EsMboEventStore, PriceTicks,
};

#[test]
fn artifact_codec_round_trips_event_store() {
    let events = sample_events();
    let store = EsMboEventStore {
        batches: build_batches(&events),
        events,
    };

    let decoded = decode_event_store(&encode_event_store(&store)).unwrap();

    assert_eq!(decoded, store);
}

#[test]
fn artifact_codec_rejects_wrong_magic() {
    let store = EsMboEventStore {
        events: sample_events(),
        batches: vec![EsMboBatchSpan {
            start_idx: 0,
            end_idx: 2,
            ts_event_ns: 101,
        }],
    };
    let mut bytes = encode_event_store(&store);
    bytes[4] = b'X';

    let error = decode_event_store(&bytes).unwrap_err();

    assert!(error.to_string().contains("magic"));
}

#[test]
fn artifact_codec_rejects_trailing_bytes() {
    let events = sample_events();
    let store = EsMboEventStore {
        batches: build_batches(&events),
        events,
    };
    let mut bytes = encode_event_store(&store);
    bytes.push(0);

    let error = decode_event_store(&bytes).unwrap_err();

    assert!(error.to_string().contains("trailing"));
}

#[test]
fn artifact_codec_rejects_unrebuildable_batches() {
    let events = sample_events();
    let store = EsMboEventStore {
        events,
        batches: vec![EsMboBatchSpan {
            start_idx: 0,
            end_idx: 1,
            ts_event_ns: 100,
        }],
    };

    let error = decode_event_store(&encode_event_store(&store)).unwrap_err();

    assert!(error.to_string().contains("batches"));
}

fn sample_events() -> Vec<EsMboEvent> {
    vec![
        EsMboEvent {
            ts_event_ns: 100,
            ts_recv_ns: 100,
            sequence: 1,
            action: BookAction::Add,
            side: Some(BookSide::Bid),
            price_ticks: Some(PriceTicks(18_000)),
            size: 1,
            order_id: 1,
            flags: 0,
            is_last: false,
        },
        EsMboEvent {
            ts_event_ns: 101,
            ts_recv_ns: 101,
            sequence: 2,
            action: BookAction::Add,
            side: Some(BookSide::Ask),
            price_ticks: Some(PriceTicks(18_001)),
            size: 1,
            order_id: 2,
            flags: 0x80,
            is_last: true,
        },
    ]
}
