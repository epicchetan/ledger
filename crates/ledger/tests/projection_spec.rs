mod support;

use ledger::market::{canonical_trade_print, BookAction, BookSide, PriceTicks, TradePrint};
use ledger::projection::{BarsParams, ProjectionSpec};
use ledger::LedgerError;

use support::{event, trade};

#[test]
fn parse_accepts_bars_intervals() {
    assert_eq!(
        ProjectionSpec::parse("bars:1s").unwrap(),
        ProjectionSpec::Bars(BarsParams {
            interval_ns: 1_000_000_000
        })
    );
    assert_eq!(
        ProjectionSpec::parse("bars:5m").unwrap(),
        ProjectionSpec::Bars(BarsParams {
            interval_ns: 5 * 60 * 1_000_000_000
        })
    );
    assert_eq!(
        ProjectionSpec::parse("bars:2h").unwrap(),
        ProjectionSpec::Bars(BarsParams {
            interval_ns: 2 * 60 * 60 * 1_000_000_000
        })
    );
    assert_eq!(
        ProjectionSpec::parse("bars:90s").unwrap(),
        ProjectionSpec::Bars(BarsParams {
            interval_ns: 90 * 1_000_000_000
        })
    );
}

#[test]
fn parse_canonicalizes_equal_intervals() {
    assert_eq!(
        ProjectionSpec::parse("bars:60s").unwrap().canonical(),
        "bars:1m"
    );
    assert_eq!(
        ProjectionSpec::parse("bars:120m").unwrap().canonical(),
        "bars:2h"
    );
    assert_eq!(
        ProjectionSpec::parse("bars:90s").unwrap().canonical(),
        "bars:90s"
    );
}

#[test]
fn parse_rejects_invalid_projection_specs() {
    for spec in [
        "bars", "bars:", "bars:0s", "bars:1x", "bars:m", "foo:1m", "BARS:1M",
    ] {
        assert!(
            matches!(
                ProjectionSpec::parse(spec),
                Err(LedgerError::InvalidProjectionSpec { .. })
            ),
            "{spec} should be rejected"
        );
    }
}

#[test]
fn canonical_trade_print_accepts_only_trade_with_price_and_size() {
    let print = trade(100, 1, 18_000, 3, Some(BookSide::Bid));
    assert_eq!(
        canonical_trade_print(&print),
        Some(TradePrint {
            ts_event_ns: 100,
            price_ticks: PriceTicks(18_000),
            size: 3,
            aggressor: Some(BookSide::Bid),
        })
    );

    for action in [
        BookAction::Fill,
        BookAction::Add,
        BookAction::Cancel,
        BookAction::Clear,
        BookAction::None,
    ] {
        let mut event = event(100, 1);
        event.action = action;
        event.price_ticks = Some(PriceTicks(18_000));
        event.size = 3;
        assert_eq!(canonical_trade_print(&event), None, "{action:?}");
    }

    let mut missing_price = trade(100, 1, 18_000, 3, Some(BookSide::Ask));
    missing_price.price_ticks = None;
    assert_eq!(canonical_trade_print(&missing_price), None);

    let zero_size = trade(100, 1, 18_000, 0, Some(BookSide::Ask));
    assert_eq!(canonical_trade_print(&zero_size), None);
}
