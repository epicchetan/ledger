# ledger-runtime

`ledger-runtime` is the runtime-facing layer for replay readiness and session loading.

## Owns

- Status checks for cataloged market days.
- Listing cataloged sessions through `ledger-store`.
- Loading replay inputs for a ready session.
- Returning local paths to replay artifacts:
  - `events.v1.bin`
  - `batches.v1.bin`
  - `trades.v1.bin`
  - `book_check.v1.json`

## Does Not Own

- Databento download or DBN preprocessing.
- SQLite schema or R2 object behavior.
- Order-book mutation logic.
- Replay/execution simulation.
- CLI argument parsing or printing.

## Main Types

- `Runtime<S>` wraps a `ledger-store::LedgerStore<S>`.
- `ReplayInputs` is the runtime output for a loaded session.

## Boundary

Callers should ask runtime to load a session, not hydrate individual files. The store decides whether artifacts are already local or need to be fetched from R2.

## Tests

Runtime is intentionally thin. Most behavior is covered by `ledger-store` and CLI/integration-style tests.
