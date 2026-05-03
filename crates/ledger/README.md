# ledger

`ledger` is the backend application core for raw data/replay dataset lifecycle,
validation composition, ingest orchestration, and active feed-driven session
work. It exposes the public `Ledger`, `ReplayDataset`, and `Session` models used
by API and CLI adapters.

## Owns

- Status checks for SQLite-cataloged market days.
- Listing market days through `ledger-store`.
- Preparing replay datasets by ensuring Layer 1 raw data and Layer 2 replay artifacts.
- Validating replay datasets with one shared report shape for CLI/API.
- Deleting durable replay datasets or raw market data through store primitives.
- Running deterministic book-check comparison and replay simulator probes.
- Orchestrating market-day ingest through `ledger-ingest`.
- Staging replay artifacts from R2 into `tmp` for validation.
- Loading cached replay artifacts for active replay-backed `Session` startup.
- Hydrating staged replay artifacts into `ledger-domain::EventStore`.

## Does Not Own

- R2 object mechanics or SQLite schemas.
- Order-book mutation logic.
- Low-level order-book or replay/execution mechanics.
- CLI argument parsing or printing.
- HTTP/WebSocket transport.

## Main Types

- `Ledger<S>` wraps a `ledger-store::LedgerStore<S>`.
- `ReplayDataset` is the immutable staged replay-artifact context for one
  `MarketDay`.
- `ReplayDatasetValidationReport` is the shared validation/trust report used by
  CLI and API.
- `Session` is the active mutable runtime controller. Today it is opened with a
  replay feed over one `ReplayDataset`; live feeds can converge on the same
  session shape later.
- `PrepareReplayDatasetReport` combines the ingest/preprocess report with a
  readiness validation report for job-backed API work.

## Boundary

Callers should ask `Ledger` to prepare and validate replay datasets, not hydrate
individual files or rebuild validation reports themselves. The store decides how
to stage R2-backed artifacts for validation and cache artifacts for active
replay-backed Session startup.

## Tests

`ledger` tests should cover replay artifact hydration, shared validation
composition, and the bridge from a staged `ReplayDataset` into `ledger-replay`.
