use chrono::{Datelike, TimeZone};
use chrono_tz::America::New_York;
use ledger::market::{utc_datetime_to_ns, MarketDay};

#[test]
fn resolve_es_handles_session_edges_and_gap() {
    let day = MarketDay::parse("2026-03-10").unwrap();
    let (start, end) = day.es_session_bounds_utc().unwrap();

    assert_eq!(MarketDay::resolve_es(start).unwrap(), day);
    assert_eq!(MarketDay::resolve_es(end - 1).unwrap(), day);
    assert!(MarketDay::resolve_es(end).is_err());
    assert!(MarketDay::resolve_es(ns_et(2026, 3, 10, 17, 30, 0)).is_err());
}

#[test]
fn resolve_es_start_maps_to_next_calendar_date_across_dst() {
    let ts = ns_et(2026, 3, 11, 18, 0, 0);
    let day = MarketDay::resolve_es(ts).unwrap();
    let (start, end) = day.es_session_bounds_utc().unwrap();

    assert_eq!(day.to_string(), "2026-03-12");
    assert_eq!(start, ts);
    assert!(end > start);
}

fn ns_et(year: i32, month: u32, day: u32, hour: u32, minute: u32, second: u32) -> u64 {
    let local = New_York
        .with_ymd_and_hms(year, month, day, hour, minute, second)
        .single()
        .unwrap();
    assert_eq!(local.year(), year);
    utc_datetime_to_ns(local.with_timezone(&chrono::Utc)).unwrap()
}
