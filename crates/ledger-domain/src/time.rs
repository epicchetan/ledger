use chrono::{DateTime, TimeZone, Utc};

pub type UnixNanos = u64;

pub fn now_ns() -> UnixNanos {
    Utc::now()
        .timestamp_nanos_opt()
        .expect("current timestamp should fit i64 nanoseconds") as u64
}

pub fn utc_datetime_to_ns(dt: DateTime<Utc>) -> UnixNanos {
    dt.timestamp_nanos_opt()
        .expect("timestamp should fit i64 nanoseconds") as u64
}

pub fn ns_to_utc_datetime(ns: UnixNanos) -> DateTime<Utc> {
    let secs = (ns / 1_000_000_000) as i64;
    let nanos = (ns % 1_000_000_000) as u32;
    Utc.timestamp_opt(secs, nanos)
        .single()
        .expect("valid unix nanoseconds")
}
