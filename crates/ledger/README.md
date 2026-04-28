# ledger

`ledger` is the application-facing layer for replay readiness and session
loading. It is the crate that should grow toward the public `Ledger` and
`LedgerSession` object model used by API, CLI, and future live-mode adapters.

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

- `Ledger<S>` wraps a `ledger-store::LedgerStore<S>`.
- `LedgerSession` is the current session-loading output.

## Boundary

Callers should ask `Ledger` to load a session, not hydrate individual files. The
store decides whether artifacts are already local or need to be fetched from R2.

## Tests

`ledger` is intentionally thin for now. Most behavior is covered by
`ledger-store` and CLI/integration-style tests.
