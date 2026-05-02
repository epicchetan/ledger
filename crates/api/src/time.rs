use chrono::{DateTime, SecondsFormat, Utc};

pub(crate) fn ns_to_datetime(ns: u64) -> DateTime<Utc> {
    let secs = (ns / 1_000_000_000) as i64;
    let nanos = (ns % 1_000_000_000) as u32;
    DateTime::<Utc>::from_timestamp(secs, nanos).unwrap_or(DateTime::<Utc>::UNIX_EPOCH)
}

pub(crate) fn ns_string(ns: u64) -> String {
    ns.to_string()
}

pub(crate) fn ns_iso(ns: u64) -> String {
    ns_to_datetime(ns).to_rfc3339_opts(SecondsFormat::Nanos, true)
}
