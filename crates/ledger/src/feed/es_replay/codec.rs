use crate::market::{
    build_batches, BookAction, BookSide, EsMboBatchSpan, EsMboEvent, EsMboEventStore, PriceTicks,
};
use crate::LedgerError;
use anyhow::Context;
use std::io::{Cursor, Read};

const MAGIC: &[u8] = b"LEDGER_ES_MBO_EVENT_STORE_V1";
const VERSION: u32 = 1;

pub fn encode_event_store(store: &EsMboEventStore) -> Vec<u8> {
    let mut out = Vec::new();
    put_u32(&mut out, MAGIC.len() as u32);
    out.extend_from_slice(MAGIC);
    put_u32(&mut out, VERSION);
    put_u64(&mut out, store.events.len() as u64);
    put_u64(&mut out, store.batches.len() as u64);

    for event in &store.events {
        put_u64(&mut out, event.ts_event_ns);
        put_u64(&mut out, event.ts_recv_ns);
        put_u64(&mut out, event.sequence);
        out.push(action_to_u8(event.action));
        out.push(side_to_u8(event.side));
        put_i64(
            &mut out,
            event.price_ticks.map(|price| price.0).unwrap_or(i64::MAX),
        );
        put_u32(&mut out, event.size);
        put_u64(&mut out, event.order_id);
        out.push(event.flags);
        out.push(u8::from(event.is_last));
    }

    for batch in &store.batches {
        put_u64(&mut out, batch.start_idx as u64);
        put_u64(&mut out, batch.end_idx as u64);
        put_u64(&mut out, batch.ts_event_ns);
    }

    out
}

pub fn decode_event_store(bytes: &[u8]) -> Result<EsMboEventStore, LedgerError> {
    let mut cursor = Cursor::new(bytes);
    let magic_len = get_u32(&mut cursor)? as usize;
    let mut magic = vec![0; magic_len];
    cursor.read_exact(&mut magic).map_err(invalid_artifact)?;
    if magic != MAGIC {
        return Err(LedgerError::InvalidArtifact("invalid magic".to_string()));
    }
    let version = get_u32(&mut cursor)?;
    if version != VERSION {
        return Err(LedgerError::InvalidArtifact(format!(
            "unsupported version {version}"
        )));
    }
    let event_count = usize::try_from(get_u64(&mut cursor)?)
        .map_err(|_| LedgerError::InvalidArtifact("event count overflows usize".to_string()))?;
    let batch_count = usize::try_from(get_u64(&mut cursor)?)
        .map_err(|_| LedgerError::InvalidArtifact("batch count overflows usize".to_string()))?;

    let mut events = Vec::with_capacity(event_count);
    for _ in 0..event_count {
        let ts_event_ns = get_u64(&mut cursor)?;
        let ts_recv_ns = get_u64(&mut cursor)?;
        let sequence = get_u64(&mut cursor)?;
        let action = u8_to_action(get_u8(&mut cursor)?)?;
        let side = u8_to_side(get_u8(&mut cursor)?)?;
        let price = get_i64(&mut cursor)?;
        let size = get_u32(&mut cursor)?;
        let order_id = get_u64(&mut cursor)?;
        let flags = get_u8(&mut cursor)?;
        let is_last = match get_u8(&mut cursor)? {
            0 => false,
            1 => true,
            other => {
                return Err(LedgerError::InvalidArtifact(format!(
                    "invalid is_last byte {other}"
                )))
            }
        };
        events.push(EsMboEvent {
            ts_event_ns,
            ts_recv_ns,
            sequence,
            action,
            side,
            price_ticks: (price != i64::MAX).then_some(PriceTicks(price)),
            size,
            order_id,
            flags,
            is_last,
        });
    }

    let mut batches = Vec::with_capacity(batch_count);
    for _ in 0..batch_count {
        batches.push(EsMboBatchSpan {
            start_idx: usize::try_from(get_u64(&mut cursor)?).map_err(|_| {
                LedgerError::InvalidArtifact("batch start_idx overflows usize".to_string())
            })?,
            end_idx: usize::try_from(get_u64(&mut cursor)?).map_err(|_| {
                LedgerError::InvalidArtifact("batch end_idx overflows usize".to_string())
            })?,
            ts_event_ns: get_u64(&mut cursor)?,
        });
    }

    if cursor.position() != bytes.len() as u64 {
        return Err(LedgerError::InvalidArtifact(
            "artifact has trailing bytes".to_string(),
        ));
    }

    validate_batches(&events, &batches)?;
    let store = EsMboEventStore { events, batches };
    store
        .validate()
        .map_err(|err| LedgerError::InvalidArtifact(err.to_string()))?;
    Ok(store)
}

fn validate_batches(events: &[EsMboEvent], batches: &[EsMboBatchSpan]) -> Result<(), LedgerError> {
    let mut last_end = 0usize;
    for batch in batches {
        if batch.start_idx > batch.end_idx {
            return Err(LedgerError::InvalidArtifact(format!(
                "batch start {} exceeds end {}",
                batch.start_idx, batch.end_idx
            )));
        }
        if batch.end_idx > events.len() {
            return Err(LedgerError::InvalidArtifact(format!(
                "batch end {} exceeds event count {}",
                batch.end_idx,
                events.len()
            )));
        }
        if batch.start_idx < last_end {
            return Err(LedgerError::InvalidArtifact(
                "batches overlap or are unordered".to_string(),
            ));
        }
        last_end = batch.end_idx;
    }
    let rebuilt = build_batches(events);
    if rebuilt != batches {
        return Err(LedgerError::InvalidArtifact(format!(
            "stored batches do not match rebuilt batches: stored {}, rebuilt {}",
            batches.len(),
            rebuilt.len()
        )));
    }
    Ok(())
}

fn put_u64(out: &mut Vec<u8>, value: u64) {
    out.extend_from_slice(&value.to_le_bytes());
}

fn put_i64(out: &mut Vec<u8>, value: i64) {
    out.extend_from_slice(&value.to_le_bytes());
}

fn put_u32(out: &mut Vec<u8>, value: u32) {
    out.extend_from_slice(&value.to_le_bytes());
}

fn get_u8(cursor: &mut Cursor<&[u8]>) -> Result<u8, LedgerError> {
    let mut buf = [0; 1];
    cursor.read_exact(&mut buf).map_err(invalid_artifact)?;
    Ok(buf[0])
}

fn get_u32(cursor: &mut Cursor<&[u8]>) -> Result<u32, LedgerError> {
    let mut buf = [0; 4];
    cursor.read_exact(&mut buf).map_err(invalid_artifact)?;
    Ok(u32::from_le_bytes(buf))
}

fn get_u64(cursor: &mut Cursor<&[u8]>) -> Result<u64, LedgerError> {
    let mut buf = [0; 8];
    cursor.read_exact(&mut buf).map_err(invalid_artifact)?;
    Ok(u64::from_le_bytes(buf))
}

fn get_i64(cursor: &mut Cursor<&[u8]>) -> Result<i64, LedgerError> {
    let mut buf = [0; 8];
    cursor.read_exact(&mut buf).map_err(invalid_artifact)?;
    Ok(i64::from_le_bytes(buf))
}

fn invalid_artifact(error: std::io::Error) -> LedgerError {
    LedgerError::InvalidArtifact(error.to_string())
}

fn action_to_u8(action: BookAction) -> u8 {
    match action {
        BookAction::Add => 1,
        BookAction::Modify => 2,
        BookAction::Cancel => 3,
        BookAction::Clear => 4,
        BookAction::Trade => 5,
        BookAction::Fill => 6,
        BookAction::None => 7,
    }
}

fn u8_to_action(value: u8) -> Result<BookAction, LedgerError> {
    Ok(match value {
        1 => BookAction::Add,
        2 => BookAction::Modify,
        3 => BookAction::Cancel,
        4 => BookAction::Clear,
        5 => BookAction::Trade,
        6 => BookAction::Fill,
        7 => BookAction::None,
        other => {
            return Err(LedgerError::InvalidArtifact(format!(
                "unknown action code {other}"
            )))
        }
    })
}

fn side_to_u8(side: Option<BookSide>) -> u8 {
    match side {
        None => 0,
        Some(BookSide::Bid) => 1,
        Some(BookSide::Ask) => 2,
    }
}

fn u8_to_side(value: u8) -> Result<Option<BookSide>, LedgerError> {
    Ok(match value {
        0 => None,
        1 => Some(BookSide::Bid),
        2 => Some(BookSide::Ask),
        other => {
            return Err(LedgerError::InvalidArtifact(format!(
                "unknown side code {other}"
            )))
        }
    })
}

pub fn read_event_store_file(path: &std::path::Path) -> Result<EsMboEventStore, LedgerError> {
    let bytes =
        std::fs::read(path).with_context(|| format!("reading artifact {}", path.display()))?;
    decode_event_store(&bytes)
}
