# ledger

`ledger` is the application-facing layer for replay readiness and replay
dataset loading. It is the crate that exposes the public `Ledger` and
`ReplayDataset` model used by API, CLI, and future active replay-session work.

## Owns

- Status checks for cataloged market days.
- Listing cataloged market days through `ledger-store`.
- Loading replay datasets for a ready market day.
- Returning local paths to replay artifacts:
  - `events.v1.bin`
  - `batches.v1.bin`
  - `trades.v1.bin`
  - `book_check.v1.json`
- Hydrating local replay artifacts into `ledger-domain::EventStore`.

## Does Not Own

- Databento download or DBN preprocessing.
- SQLite schema or R2 object behavior.
- Order-book mutation logic.
- Replay/execution simulation.
- CLI argument parsing or printing.

## Main Types

- `Ledger<S>` wraps a `ledger-store::LedgerStore<S>`.
- `ReplayDataset` is the immutable loaded replay-artifact context for one
  `MarketDay`.

## Boundary

Callers should ask `Ledger` to load a replay dataset, not hydrate individual
files. The store decides whether artifacts are already local or need to be
fetched from R2. `ReplayDataset::event_store` decodes and validates the local
artifacts before handing them to replay.

## Tests

`ledger` tests should cover replay artifact hydration and the bridge from a
loaded `ReplayDataset` into `ledger-replay`.
