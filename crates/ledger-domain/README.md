# ledger-domain

`ledger-domain` owns Ledger's shared domain types and small pure helpers.

This crate is the common language used by the rest of the workspace. It should
stay free of provider, storage, CLI, application, and replay orchestration logic.

## Owns

- ES market-day resolution and UTC nanosecond time helpers.
- Normalized MBO event types, book actions, book sides, price ticks, BBO, and
  depth snapshots.
- Event-store indexes: batch spans and trade records.
- Artifact codecs for Ledger's private replay artifact files.
- Shared simulation request/profile/result types.
- Durable object kind names shared by store, ingest, and application layers.

## Must Not Own

- Databento API calls or DBN decoding.
- R2, SQLite, filesystem cache, or staging policy.
- Order-book mutation logic.
- Replay stepping, fill simulation, or visibility scheduling.
- CLI argument parsing or user-facing command behavior.

## Main Public Concepts

- `MarketDay` resolves an ES contract/date into full Globex and RTH UTC
  nanosecond windows.
- `MboEvent` is the normalized market-by-order event consumed by the book and
  replay crates.
- `BatchSpan` groups events by Databento-style event-end semantics.
- `TradeRecord` indexes trade/fill-like events separately from the book event
  stream.
- `EventStore` bundles events, batches, and trades for replay.
- `ExecutionProfile`, `VisibilityProfile`, and `SimOrder*` describe simulator
  configuration and order state in shared terms.

## Testing Expectations

Tests in this crate should be deterministic and pure. They should cover domain
rules such as ES session resolution, fixed-price to tick conversion, batch
building, trade indexing, and artifact encode/decode behavior. They should not
touch the network, R2, SQLite, or local cache directories.
