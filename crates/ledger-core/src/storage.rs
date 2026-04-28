use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum StorageKind {
    RawDbn,
    EventStore,
    BatchIndex,
    TradeIndex,
    BookCheck,
    Other(String),
}

impl StorageKind {
    pub fn as_str(&self) -> &str {
        match self {
            Self::RawDbn => "raw_dbn",
            Self::EventStore => "event_store",
            Self::BatchIndex => "batch_index",
            Self::TradeIndex => "trade_index",
            Self::BookCheck => "book_check",
            Self::Other(s) => s.as_str(),
        }
    }
    pub fn parse(s: &str) -> Self {
        match s {
            "raw_dbn" => Self::RawDbn,
            "event_store" => Self::EventStore,
            "batch_index" => Self::BatchIndex,
            "trade_index" => Self::TradeIndex,
            "book_check" => Self::BookCheck,
            other => Self::Other(other.to_string()),
        }
    }
}
