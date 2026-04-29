# ledger

`ledger` is the application-facing layer for replay readiness and replay
session loading. It is the crate that should grow toward the public `Ledger`
and `ReplaySession` object model used by API, CLI, and future live-mode
adapters.

## Owns

- Status checks for cataloged market days.
- Listing cataloged sessions through `ledger-store`.
- Loading replay sessions for a ready market day.
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
- `ReplaySession` is the loaded replay-session context.

## Boundary

Callers should ask `Ledger` to load a replay session, not hydrate individual
files. The store decides whether artifacts are already local or need to be
fetched from R2. `ReplaySession::event_store` decodes and validates the local
artifacts before handing them to replay.

## Tests

`ledger` tests should cover replay artifact hydration and the bridge from a
loaded `ReplaySession` into `ledger-replay`.
