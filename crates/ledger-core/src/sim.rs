use serde::{Deserialize, Serialize};

use crate::{BookSide, PriceTicks, UnixNanos};

pub type SimOrderId = u64;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum LatencyModel {
    FixedNs(u64),
}

impl Default for LatencyModel {
    fn default() -> Self {
        Self::FixedNs(0)
    }
}

impl LatencyModel {
    pub fn fixed_ms(ms: u64) -> Self {
        Self::FixedNs(ms.saturating_mul(1_000_000))
    }

    pub fn sample_ns(&self) -> u64 {
        match self {
            Self::FixedNs(ns) => *ns,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SameTimestampPolicy {
    ExchangeFirstConservative,
    OrderFirst,
}

impl Default for SameTimestampPolicy {
    fn default() -> Self {
        Self::ExchangeFirstConservative
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExecutionProfile {
    pub order_entry_latency: LatencyModel,
    pub cancel_latency: LatencyModel,
    pub same_timestamp_policy: SameTimestampPolicy,
}

impl Default for ExecutionProfile {
    fn default() -> Self {
        Self {
            order_entry_latency: LatencyModel::fixed_ms(75),
            cancel_latency: LatencyModel::fixed_ms(75),
            same_timestamp_policy: SameTimestampPolicy::ExchangeFirstConservative,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct VisibilityProfile {
    pub market_data_delay: LatencyModel,
    pub frame_interval_ns: u64,
    pub depth_levels: usize,
}

impl Default for VisibilityProfile {
    fn default() -> Self {
        Self {
            market_data_delay: LatencyModel::fixed_ms(75),
            frame_interval_ns: 33_000_000,
            depth_levels: 10,
        }
    }
}

impl VisibilityProfile {
    pub fn truth() -> Self {
        Self {
            market_data_delay: LatencyModel::FixedNs(0),
            frame_interval_ns: 0,
            depth_levels: 10,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SimOrderSide {
    Buy,
    Sell,
}

impl SimOrderSide {
    pub fn book_side(self) -> BookSide {
        match self {
            Self::Buy => BookSide::Bid,
            Self::Sell => BookSide::Ask,
        }
    }

    pub fn opposite_aggressor_side_for_fill(self) -> BookSide {
        match self {
            Self::Buy => BookSide::Ask,
            Self::Sell => BookSide::Bid,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SimOrderKind {
    Market,
    Limit { limit_price: PriceTicks },
    StopMarket { stop_price: PriceTicks },
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SimOrderRequest {
    pub side: SimOrderSide,
    pub qty: u32,
    pub kind: SimOrderKind,
    pub decision_ts_ns: UnixNanos,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SimOrderAccepted {
    pub order_id: SimOrderId,
    pub request: SimOrderRequest,
    pub arrival_ts_ns: UnixNanos,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SimOrderStatus {
    PendingArrival,
    Resting,
    Filled,
    Cancelled,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SimLiquidity {
    Taking,
    Making,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct SimFill {
    pub order_id: SimOrderId,
    pub ts_ns: UnixNanos,
    pub side: SimOrderSide,
    pub price: PriceTicks,
    pub qty: u32,
    pub liquidity: SimLiquidity,
}
