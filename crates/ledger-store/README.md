# ledger-store

`ledger-store` is Ledger's storage boundary. It connects the local SQLite catalog, durable R2 object storage, ingest staging, and replay session loading.

## Responsibilities

- Maintain the local SQLite catalog at `data/catalog.sqlite`.
- Store durable blobs through an `ObjectStore` implementation, usually R2.
- Build stable content-addressed object keys for raw DBN files and derived artifacts.
- Stage ingest work under `data/tmp/ingest/...`.
- Load replay sessions under `data/sessions/...`.
- Validate local files by size and SHA256 before reusing them.
- Prune old replay sessions according to the session cache policy.

## Storage Model

R2 is the durable blob store. SQLite is the queryable local catalog. Local session files are a speed-up for replay, not the source of truth.

The catalog tracks all durable objects, including raw DBN files:

- `raw_dbn`
- `event_store`
- `batch_index`
- `trade_index`
- `book_check`

The replay session cache only stores replay artifacts:

- `events.v1.bin`
- `batches.v1.bin`
- `trades.v1.bin`
- `book_check.v1.json`

Raw DBN files are not replay session cache entries. They are downloaded or hydrated into ingest staging when preprocessing needs them, then remain durable in R2 and queryable through SQLite.

## Main APIs

- `LedgerStore::open` wires together local paths, an object store, key building, and cache policy.
- `R2LedgerStore::from_env` builds a production store from environment configuration.
- `session_status` reports catalog readiness and local session validity without loading files.
- `load_session` returns local replay artifact paths, hydrating missing or corrupt artifacts from R2 as needed.
- `begin_ingest`, `stage_raw_for_ingest`, `register_raw_object`, and `register_replay_artifact` support the durable ingest pipeline.
- `prune_cache` removes least-recently-used replay session directories.

## Boundaries

This crate does not decode Databento DBN, normalize MBO records, run the order book, simulate fills, or format CLI output. It provides storage primitives and session loading semantics for `ledger-ingest`, `ledger-runtime`, and `ledger-cli`.
