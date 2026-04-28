use anyhow::{anyhow, Context, Result};
use async_trait::async_trait;
use dbn::{
    decode::{DbnDecoder, DecodeRecord},
    MboMsg, UNDEF_PRICE,
};
use ledger_core::{
    build_batches, build_trade_index, dbn_fixed_price_to_es_ticks, encode_batches, encode_events,
    encode_trades, BatchSpan, BookAction, BookSide, EventStore, MboEvent, TradeRecord,
};
use ledger_store::LocalStore;
use std::path::{Path, PathBuf};

#[derive(Debug, Clone)]
pub struct PreprocessOutput {
    pub event_store: EventStore,
    pub events_path: PathBuf,
    pub batches_path: PathBuf,
    pub trades_path: PathBuf,
}

#[async_trait]
pub trait Preprocessor: Send + Sync {
    async fn preprocess(
        &self,
        raw_dbn_path: &Path,
        artifacts_dir: &Path,
        local: &LocalStore,
    ) -> Result<PreprocessOutput>;
}

#[derive(Clone, Debug, Default)]
pub struct DbnPreprocessor;

#[async_trait]
impl Preprocessor for DbnPreprocessor {
    async fn preprocess(
        &self,
        raw_dbn_path: &Path,
        artifacts_dir: &Path,
        local: &LocalStore,
    ) -> Result<PreprocessOutput> {
        let events = decode_mbo_events(raw_dbn_path)?;
        write_event_artifacts(events, artifacts_dir, local).await
    }
}

pub fn decode_mbo_events(path: &Path) -> Result<Vec<MboEvent>> {
    let mut decoder = DbnDecoder::from_zstd_file(path)
        .with_context(|| format!("opening DBN MBO file {}", path.display()))?;
    let mut events = Vec::new();
    while let Some(msg) = decoder.decode_record::<MboMsg>()? {
        events.push(normalize_mbo(msg)?);
    }
    Ok(events)
}

pub async fn write_event_artifacts(
    events: Vec<MboEvent>,
    artifacts_dir: &Path,
    local: &LocalStore,
) -> Result<PreprocessOutput> {
    let batches = build_batches(&events);
    let trades = build_trade_index(&events);
    let events_path = artifacts_dir.join("events.v1.bin");
    let batches_path = artifacts_dir.join("batches.v1.bin");
    let trades_path = artifacts_dir.join("trades.v1.bin");

    local
        .write_atomic(&events_path, &encode_events(&events))
        .await?;
    local
        .write_atomic(&batches_path, &encode_batches(&batches))
        .await?;
    local
        .write_atomic(&trades_path, &encode_trades(&trades))
        .await?;

    Ok(PreprocessOutput {
        event_store: EventStore {
            events,
            batches,
            trades,
        },
        events_path,
        batches_path,
        trades_path,
    })
}

fn normalize_mbo(msg: &MboMsg) -> Result<MboEvent> {
    let action = match msg.action as u8 {
        b'A' => BookAction::Add,
        b'M' => BookAction::Modify,
        b'C' => BookAction::Cancel,
        b'R' => BookAction::Clear,
        b'T' => BookAction::Trade,
        b'F' => BookAction::Fill,
        b'N' => BookAction::None,
        other => return Err(anyhow!("unknown MBO action byte {other}")),
    };
    let side = match msg.side as u8 {
        b'B' => Some(BookSide::Bid),
        b'A' => Some(BookSide::Ask),
        b'N' => None,
        other => return Err(anyhow!("unknown MBO side byte {other}")),
    };
    let price_ticks = if msg.price == UNDEF_PRICE {
        None
    } else {
        if msg.price % ledger_core::ES_TICK_SIZE_FIXED_PRICE != 0 {
            return Err(anyhow!(
                "ES price {} is not aligned to 0.25 tick",
                msg.price
            ));
        }
        dbn_fixed_price_to_es_ticks(msg.price)
    };
    Ok(MboEvent {
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

#[derive(Clone)]
pub struct SyntheticPreprocessor {
    pub events: Vec<MboEvent>,
}

#[async_trait]
impl Preprocessor for SyntheticPreprocessor {
    async fn preprocess(
        &self,
        _raw_dbn_path: &Path,
        artifacts_dir: &Path,
        local: &LocalStore,
    ) -> Result<PreprocessOutput> {
        write_event_artifacts(self.events.clone(), artifacts_dir, local).await
    }
}

#[allow(dead_code)]
fn _assert_send_sync<T: Send + Sync>() {}
#[allow(dead_code)]
fn _assert_types() {
    _assert_send_sync::<Vec<BatchSpan>>();
    _assert_send_sync::<Vec<TradeRecord>>();
}
