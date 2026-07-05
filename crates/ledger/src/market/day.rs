use crate::LedgerError;
use chrono::{DateTime, Datelike, NaiveDate, NaiveTime, TimeZone, Utc};
use chrono_tz::America::New_York;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::str::FromStr;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct MarketDay(pub NaiveDate);

impl MarketDay {
    pub fn resolve_es(ts_event_ns: u64) -> Result<MarketDay, LedgerError> {
        let utc = utc_datetime_from_ns(ts_event_ns)?;
        let local = utc.with_timezone(&New_York);
        let date = local.date_naive();
        let time = local.time();
        let halt_start = NaiveTime::from_hms_opt(17, 0, 0)
            .ok_or_else(|| LedgerError::MarketDay("invalid halt start".to_string()))?;
        let halt_end = NaiveTime::from_hms_opt(18, 0, 0)
            .ok_or_else(|| LedgerError::MarketDay("invalid halt end".to_string()))?;

        if time >= halt_end {
            Ok(MarketDay(date.succ_opt().ok_or_else(|| {
                LedgerError::MarketDay(format!("invalid next date for {date}"))
            })?))
        } else if time < halt_start {
            Ok(MarketDay(date))
        } else {
            Err(LedgerError::MarketDay(format!(
                "timestamp {ts_event_ns} falls in the ES maintenance halt"
            )))
        }
    }

    pub fn es_session_bounds_utc(&self) -> Result<(u64, u64), LedgerError> {
        let market_date = self.0;
        let prev = market_date.pred_opt().ok_or_else(|| {
            LedgerError::MarketDay(format!("invalid previous date for {market_date}"))
        })?;
        let data_start = New_York
            .with_ymd_and_hms(prev.year(), prev.month(), prev.day(), 18, 0, 0)
            .single()
            .ok_or_else(|| {
                LedgerError::MarketDay(format!(
                    "ambiguous/nonexistent ES data start for {market_date}"
                ))
            })?
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
            .ok_or_else(|| {
                LedgerError::MarketDay(format!(
                    "ambiguous/nonexistent ES data end for {market_date}"
                ))
            })?
            .with_timezone(&Utc);
        Ok((
            utc_datetime_to_ns(data_start)?,
            utc_datetime_to_ns(data_end)?,
        ))
    }

    pub fn parse(value: &str) -> Result<Self, LedgerError> {
        value.parse()
    }
}

impl fmt::Display for MarketDay {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl FromStr for MarketDay {
    type Err = LedgerError;

    fn from_str(value: &str) -> Result<Self, Self::Err> {
        NaiveDate::parse_from_str(value, "%Y-%m-%d")
            .map(MarketDay)
            .map_err(|err| LedgerError::MarketDay(format!("invalid market day {value}: {err}")))
    }
}

pub fn utc_datetime_from_ns(ns: u64) -> Result<DateTime<Utc>, LedgerError> {
    let secs = (ns / 1_000_000_000) as i64;
    let nanos = (ns % 1_000_000_000) as u32;
    DateTime::<Utc>::from_timestamp(secs, nanos)
        .ok_or_else(|| LedgerError::MarketDay(format!("invalid unix nanos {ns}")))
}

pub fn utc_datetime_to_ns(datetime: DateTime<Utc>) -> Result<u64, LedgerError> {
    datetime
        .timestamp_nanos_opt()
        .and_then(|ns| u64::try_from(ns).ok())
        .ok_or_else(|| LedgerError::MarketDay(format!("invalid UTC datetime {datetime}")))
}
