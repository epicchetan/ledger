use anyhow::{anyhow, bail, Result};
use std::io::{Cursor, Read};

use crate::{BatchSpan, BookAction, BookSide, MboEvent, PriceTicks, TradeRecord};

const EVENTS_MAGIC: &[u8] = b"LEDGER_EVENTS_V1";
const BATCHES_MAGIC: &[u8] = b"LEDGER_BATCHES_V1";
const TRADES_MAGIC: &[u8] = b"LEDGER_TRADES_V1";

pub fn encode_events(events: &[MboEvent]) -> Vec<u8> {
    let mut out = Vec::new();
    write_header(&mut out, EVENTS_MAGIC, events.len());
    for event in events {
        put_u64(&mut out, event.ts_event_ns);
        put_u64(&mut out, event.ts_recv_ns);
        put_u64(&mut out, event.sequence);
        out.push(action_to_u8(event.action));
        out.push(side_to_u8(event.side));
        put_i64(&mut out, event.price_ticks.map(|p| p.0).unwrap_or(i64::MAX));
        put_u32(&mut out, event.size);
        put_u64(&mut out, event.order_id);
        out.push(event.flags);
        out.push(u8::from(event.is_last));
    }
    out
}

pub fn decode_events(bytes: &[u8]) -> Result<Vec<MboEvent>> {
    let mut cursor = Cursor::new(bytes);
    let count = read_header(&mut cursor, EVENTS_MAGIC)?;
    let mut out = Vec::with_capacity(count);
    for _ in 0..count {
        let ts_event_ns = get_u64(&mut cursor)?;
        let ts_recv_ns = get_u64(&mut cursor)?;
        let sequence = get_u64(&mut cursor)?;
        let action = u8_to_action(get_u8(&mut cursor)?)?;
        let side = u8_to_side(get_u8(&mut cursor)?)?;
        let price = get_i64(&mut cursor)?;
        let size = get_u32(&mut cursor)?;
        let order_id = get_u64(&mut cursor)?;
        let flags = get_u8(&mut cursor)?;
        let is_last = get_u8(&mut cursor)? != 0;
        out.push(MboEvent {
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
    Ok(out)
}

pub fn encode_batches(batches: &[BatchSpan]) -> Vec<u8> {
    let mut out = Vec::new();
    write_header(&mut out, BATCHES_MAGIC, batches.len());
    for batch in batches {
        put_u64(&mut out, batch.start_idx as u64);
        put_u64(&mut out, batch.end_idx as u64);
        put_u64(&mut out, batch.ts_event_ns);
    }
    out
}

pub fn decode_batches(bytes: &[u8]) -> Result<Vec<BatchSpan>> {
    let mut cursor = Cursor::new(bytes);
    let count = read_header(&mut cursor, BATCHES_MAGIC)?;
    let mut out = Vec::with_capacity(count);
    for _ in 0..count {
        out.push(BatchSpan {
            start_idx: get_u64(&mut cursor)? as usize,
            end_idx: get_u64(&mut cursor)? as usize,
            ts_event_ns: get_u64(&mut cursor)?,
        });
    }
    Ok(out)
}

pub fn encode_trades(trades: &[TradeRecord]) -> Vec<u8> {
    let mut out = Vec::new();
    write_header(&mut out, TRADES_MAGIC, trades.len());
    for trade in trades {
        put_u64(&mut out, trade.ts_event_ns);
        put_u64(&mut out, trade.sequence);
        put_i64(&mut out, trade.price.0);
        put_u32(&mut out, trade.size);
        out.push(side_to_u8(trade.aggressor_side));
        put_u64(&mut out, trade.order_id);
    }
    out
}

pub fn decode_trades(bytes: &[u8]) -> Result<Vec<TradeRecord>> {
    let mut cursor = Cursor::new(bytes);
    let count = read_header(&mut cursor, TRADES_MAGIC)?;
    let mut out = Vec::with_capacity(count);
    for _ in 0..count {
        out.push(TradeRecord {
            ts_event_ns: get_u64(&mut cursor)?,
            sequence: get_u64(&mut cursor)?,
            price: PriceTicks(get_i64(&mut cursor)?),
            size: get_u32(&mut cursor)?,
            aggressor_side: u8_to_side(get_u8(&mut cursor)?)?,
            order_id: get_u64(&mut cursor)?,
        });
    }
    Ok(out)
}

fn write_header(out: &mut Vec<u8>, magic: &[u8], count: usize) {
    out.extend_from_slice(&(magic.len() as u32).to_le_bytes());
    out.extend_from_slice(magic);
    put_u64(out, count as u64);
}

fn read_header(cursor: &mut Cursor<&[u8]>, expected: &[u8]) -> Result<usize> {
    let len = get_u32(cursor)? as usize;
    let mut magic = vec![0; len];
    cursor.read_exact(&mut magic)?;
    if magic != expected {
        bail!("invalid artifact magic");
    }
    Ok(get_u64(cursor)? as usize)
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

fn get_u8(cursor: &mut Cursor<&[u8]>) -> Result<u8> {
    let mut buf = [0; 1];
    cursor.read_exact(&mut buf)?;
    Ok(buf[0])
}

fn get_u32(cursor: &mut Cursor<&[u8]>) -> Result<u32> {
    let mut buf = [0; 4];
    cursor.read_exact(&mut buf)?;
    Ok(u32::from_le_bytes(buf))
}

fn get_u64(cursor: &mut Cursor<&[u8]>) -> Result<u64> {
    let mut buf = [0; 8];
    cursor.read_exact(&mut buf)?;
    Ok(u64::from_le_bytes(buf))
}

fn get_i64(cursor: &mut Cursor<&[u8]>) -> Result<i64> {
    let mut buf = [0; 8];
    cursor.read_exact(&mut buf)?;
    Ok(i64::from_le_bytes(buf))
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

fn u8_to_action(value: u8) -> Result<BookAction> {
    Ok(match value {
        1 => BookAction::Add,
        2 => BookAction::Modify,
        3 => BookAction::Cancel,
        4 => BookAction::Clear,
        5 => BookAction::Trade,
        6 => BookAction::Fill,
        7 => BookAction::None,
        other => return Err(anyhow!("unknown action code {other}")),
    })
}

fn side_to_u8(side: Option<BookSide>) -> u8 {
    match side {
        None => 0,
        Some(BookSide::Bid) => 1,
        Some(BookSide::Ask) => 2,
    }
}

fn u8_to_side(value: u8) -> Result<Option<BookSide>> {
    Ok(match value {
        0 => None,
        1 => Some(BookSide::Bid),
        2 => Some(BookSide::Ask),
        other => return Err(anyhow!("unknown side code {other}")),
    })
}
