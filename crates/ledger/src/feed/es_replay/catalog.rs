use crate::market::MarketDay;
use crate::LedgerError;
use serde::Serialize;
use std::collections::BTreeMap;
use store::{ObjectFilter, RemoteStore, Store, StoreObjectDescriptor, StoreObjectRole};

use super::{
    find_es_replay_artifact_descriptor, ES_MBO_EVENT_STORE_KIND, RAW_DATABENTO_DBN_ZST_KIND,
};

#[derive(Debug, Clone, Serialize)]
pub struct EsDayCatalog {
    pub days: Vec<EsDayEntry>,
    pub unassigned: Vec<EsRawStatus>,
}

#[derive(Debug, Clone, Serialize)]
pub struct EsDayEntry {
    pub market_day: MarketDay,
    pub raws: Vec<EsRawStatus>,
}

#[derive(Debug, Clone, Serialize)]
pub struct EsRawStatus {
    pub raw: StoreObjectDescriptor,
    pub artifact: Option<StoreObjectDescriptor>,
    pub state: EsRawState,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum EsRawState {
    Unprepared,
    Prepared,
}

pub fn es_day_catalog<S>(store: &Store<S>) -> Result<EsDayCatalog, LedgerError>
where
    S: RemoteStore + 'static,
{
    let mut raws = store.list_objects(ObjectFilter {
        role: Some(StoreObjectRole::Raw),
        kind: Some(RAW_DATABENTO_DBN_ZST_KIND.to_string()),
        id_prefix: None,
    })?;
    let artifacts = store.list_objects(ObjectFilter {
        role: Some(StoreObjectRole::Artifact),
        kind: Some(ES_MBO_EVENT_STORE_KIND.to_string()),
        id_prefix: None,
    })?;

    raws.sort_by(|left, right| {
        source_symbol(left)
            .cmp(&source_symbol(right))
            .then_with(|| left.id.as_str().cmp(right.id.as_str()))
    });

    let mut by_day: BTreeMap<MarketDay, Vec<EsRawStatus>> = BTreeMap::new();
    let mut unassigned = Vec::new();
    for raw in raws {
        let artifact = find_es_replay_artifact_descriptor(&artifacts, &raw.id).descriptor;
        let state = if artifact.is_some() {
            EsRawState::Prepared
        } else {
            EsRawState::Unprepared
        };
        let status = EsRawStatus {
            raw,
            artifact,
            state,
        };
        match status
            .raw
            .metadata_json
            .get("market_day")
            .and_then(|value| value.as_str())
            .map(MarketDay::parse)
        {
            Some(Ok(market_day)) => by_day.entry(market_day).or_default().push(status),
            _ => unassigned.push(status),
        }
    }

    let days = by_day
        .into_iter()
        .rev()
        .map(|(market_day, raws)| EsDayEntry { market_day, raws })
        .collect();
    Ok(EsDayCatalog { days, unassigned })
}

fn source_symbol(descriptor: &StoreObjectDescriptor) -> String {
    descriptor
        .metadata_json
        .get("source_symbol")
        .and_then(|value| value.as_str())
        .unwrap_or("")
        .to_string()
}
