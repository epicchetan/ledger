use anyhow::{anyhow, Result};
use chrono::{Datelike, NaiveDate, TimeZone, Utc};
use chrono_tz::America::New_York;
use serde::{Deserialize, Serialize};

use crate::time::{utc_datetime_to_ns, UnixNanos};

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct MarketDay {
    pub id: String,
    pub root: String,
    pub contract_symbol: String,
    pub market_date: NaiveDate,
    pub timezone: String,
    pub data_start_ns: UnixNanos,
    pub data_end_ns: UnixNanos,
    pub rth_start_ns: UnixNanos,
    pub rth_end_ns: UnixNanos,
    pub status: MarketDayStatus,
    pub metadata_json: serde_json::Value,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub enum MarketDayStatus {
    Missing,
    Downloading,
    Downloaded,
    Preprocessing,
    Ready,
    Error,
}

impl MarketDayStatus {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Missing => "missing",
            Self::Downloading => "downloading",
            Self::Downloaded => "downloaded",
            Self::Preprocessing => "preprocessing",
            Self::Ready => "ready",
            Self::Error => "error",
        }
    }

    pub fn parse(s: &str) -> Result<Self> {
        Ok(match s {
            "missing" => Self::Missing,
            "downloading" => Self::Downloading,
            "downloaded" => Self::Downloaded,
            "preprocessing" => Self::Preprocessing,
            "ready" => Self::Ready,
            "error" => Self::Error,
            other => return Err(anyhow!("unknown MarketDayStatus: {other}")),
        })
    }
}

impl MarketDay {
    pub fn resolve_es(contract_symbol: impl Into<String>, market_date: NaiveDate) -> Result<Self> {
        let contract_symbol = contract_symbol.into();
        let root = "ES".to_string();
        let prev = market_date
            .pred_opt()
            .ok_or_else(|| anyhow!("invalid previous date for {market_date}"))?;

        let data_start = New_York
            .with_ymd_and_hms(prev.year(), prev.month(), prev.day(), 18, 0, 0)
            .single()
            .ok_or_else(|| anyhow!("ambiguous/nonexistent ES data start for {market_date}"))?
            .with_timezone(&Utc);
        let data_end = New_York
            .with_ymd_and_hms(
                market_date.year(),
                market_date.month(),
                market_date.day(),
                17,
                0,
                0,
            )
            .single()
            .ok_or_else(|| anyhow!("ambiguous/nonexistent ES data end for {market_date}"))?
            .with_timezone(&Utc);
        let rth_start = New_York
            .with_ymd_and_hms(
                market_date.year(),
                market_date.month(),
                market_date.day(),
                9,
                30,
                0,
            )
            .single()
            .ok_or_else(|| anyhow!("ambiguous/nonexistent ES RTH start for {market_date}"))?
            .with_timezone(&Utc);
        let rth_end = New_York
            .with_ymd_and_hms(
                market_date.year(),
                market_date.month(),
                market_date.day(),
                16,
                0,
                0,
            )
            .single()
            .ok_or_else(|| anyhow!("ambiguous/nonexistent ES RTH end for {market_date}"))?
            .with_timezone(&Utc);

        let id = format!("{root}-{contract_symbol}-{market_date}");
        Ok(Self {
            id,
            root,
            contract_symbol,
            market_date,
            timezone: "America/New_York".to_string(),
            data_start_ns: utc_datetime_to_ns(data_start),
            data_end_ns: utc_datetime_to_ns(data_end),
            rth_start_ns: utc_datetime_to_ns(rth_start),
            rth_end_ns: utc_datetime_to_ns(rth_end),
            status: MarketDayStatus::Missing,
            metadata_json: serde_json::json!({}),
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::time::ns_to_utc_datetime;

    #[test]
    fn resolves_full_es_market_day_across_dst() {
        let md =
            MarketDay::resolve_es("ESH6", NaiveDate::from_ymd_opt(2026, 3, 12).unwrap()).unwrap();
        assert_eq!(md.id, "ES-ESH6-2026-03-12");
        assert_eq!(
            ns_to_utc_datetime(md.data_start_ns).to_rfc3339(),
            "2026-03-11T22:00:00+00:00"
        );
        assert_eq!(
            ns_to_utc_datetime(md.data_end_ns).to_rfc3339(),
            "2026-03-12T21:00:00+00:00"
        );
        assert_eq!(
            ns_to_utc_datetime(md.rth_start_ns).to_rfc3339(),
            "2026-03-12T13:30:00+00:00"
        );
        assert_eq!(
            ns_to_utc_datetime(md.rth_end_ns).to_rfc3339(),
            "2026-03-12T20:00:00+00:00"
        );
    }
}
