use crate::market::{
    dbn_fixed_price_to_es_ticks, BookAction, BookSide, EsMboEvent, ES_TICK_SIZE_FIXED_PRICE,
};
use crate::LedgerError;
use dbn::{
    decode::{DbnDecoder, DecodeRecord},
    MboMsg, UNDEF_PRICE,
};
use std::path::Path;
use tokio::sync::mpsc::UnboundedSender;

use super::PrepareProgress;

const PROGRESS_EVERY_RECORDS: u64 = 1_000_000;

pub fn decode_mbo_events(
    path: &Path,
    progress: Option<UnboundedSender<PrepareProgress>>,
) -> Result<Vec<EsMboEvent>, LedgerError> {
    let mut decoder = DbnDecoder::from_zstd_file(path).map_err(|err| {
        LedgerError::InvalidDbnRecord(format!("opening {}: {err}", path.display()))
    })?;
    let mut events = Vec::new();
    let mut records = 0u64;
    while let Some(msg) = decoder
        .decode_record::<MboMsg>()
        .map_err(|err| LedgerError::InvalidDbnRecord(err.to_string()))?
    {
        records += 1;
        events.push(normalize_mbo(msg)?);
        if records.is_multiple_of(PROGRESS_EVERY_RECORDS) {
            if let Some(progress) = &progress {
                let _ = progress.send(PrepareProgress::Decoding { records });
            }
        }
    }
    Ok(events)
}

pub fn normalize_mbo(msg: &MboMsg) -> Result<EsMboEvent, LedgerError> {
    let action = match msg.action as u8 {
        b'A' => BookAction::Add,
        b'M' => BookAction::Modify,
        b'C' => BookAction::Cancel,
        b'R' => BookAction::Clear,
        b'T' => BookAction::Trade,
        b'F' => BookAction::Fill,
        b'N' => BookAction::None,
        other => {
            return Err(LedgerError::InvalidDbnRecord(format!(
                "unknown MBO action byte {other}"
            )))
        }
    };
    let side = match msg.side as u8 {
        b'B' => Some(BookSide::Bid),
        b'A' => Some(BookSide::Ask),
        b'N' => None,
        other => {
            return Err(LedgerError::InvalidDbnRecord(format!(
                "unknown MBO side byte {other}"
            )))
        }
    };
    let price_ticks = if msg.price == UNDEF_PRICE {
        None
    } else {
        if msg.price % ES_TICK_SIZE_FIXED_PRICE != 0 {
            return Err(LedgerError::InvalidDbnRecord(format!(
                "ES price {} is not aligned to 0.25 tick",
                msg.price
            )));
        }
        dbn_fixed_price_to_es_ticks(msg.price)
    };
    Ok(EsMboEvent {
        ts_event_ns: msg.hd.ts_event,
        ts_recv_ns: msg.ts_recv,
        sequence: msg.sequence as u64,
        action,
        side,
        price_ticks,
        size: msg.size,
        order_id: msg.order_id,
        flags: msg.flags.raw(),
        is_last: msg.flags.is_last(),
    })
}
