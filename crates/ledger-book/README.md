# ledger-book

`ledger-book` owns the pure L3 order book used for deterministic book checks and
replay truth.

This crate applies normalized `ledger-core::MboEvent` values to an in-memory
book. It should remain deterministic and independent from storage, ingestion,
runtime, and CLI concerns.

## Owns

- Resting order state by order id.
- Price levels sorted by side and price.
- FIFO order preservation within each price level.
- Add, modify, cancel, clear, trade, fill, and no-op action handling.
- BBO/depth snapshots derived from the current book.
- Book warnings and deterministic checksums.
- Passive/marketable liquidity helpers used by replay execution.

## Must Not Own

- Databento-specific decoding or download logic.
- Filesystem, SQLite, R2, or session-cache behavior.
- Replay timing, latency, visibility, or user order scheduling.
- CLI/API behavior.

## Main Public Concepts

- `OrderBook` is the mutable L3 book.
- `apply_event` applies one event and can publish a BBO change.
- `apply_batch` applies an event batch and publishes at batch granularity.
- `BookEventResult` and `BatchBookOutput` capture trades, BBO movement, and
  warnings produced while applying events.
- `RestingOrder` and `PriceLevel` represent the internal book shape exposed for
  testing and diagnostics.
- `simulate_market_take` and related helpers let replay calculate fills from a
  true book snapshot without mutating the book.

## Testing Expectations

Tests should focus on book behavior: add/modify/cancel/clear semantics, trade
and fill events not directly mutating depth, BBO publication, warning cases,
FIFO preservation, checksum stability, and simulated liquidity consumption.
Tests should not depend on Databento files, R2, SQLite, or CLI commands.
