# ledger-store

`ledger-store` is Ledger's storage boundary. It connects the local SQLite
control plane, durable R2 object storage, ingest staging, replay dataset
staging, and explicit delete/cleanup operations.

## Responsibilities

- Store durable blobs through an `ObjectStore` implementation, usually R2.
- Store searchable catalog state in SQLite.
- Build stable market-day-centered object keys for raw DBN files and replay artifacts.
- Stage ingest work under `data/tmp/ingest/...`.
- Stage validation artifacts under `data/tmp/validate/...`.
- Track Layer 1 raw market data and Layer 2 replay datasets separately.
- Report cheap durable layer state for UI/status reads.
- Delete durable replay datasets and raw data through explicit higher-layer calls.

## Storage Model

SQLite is Ledger's local control plane. R2 is the large-object data plane.
Normal runtime code does not use R2 JSON manifests as a catalog.

R2 has two durable layers:

```text
Layer 1: raw
  raw_dbn object in R2

Layer 2: replay dataset
  event_store, batch_index, trade_index, book_check objects in R2
  derived from a specific raw object
```

Raw DBN files are expensive Layer 1 inputs and should normally be retained.
Replay datasets are cheaper Layer 2 outputs and can be deleted/rebuilt from raw
without redownloading Databento data.

## Main APIs

- `LedgerStore::open` wires together local paths, an object store, key building, and SQLite.
- `R2LedgerStore::from_env` builds a production store from environment configuration.
- `list_market_days` reads SQLite catalog rows.
- `replay_dataset_status` reports raw/replay layer state from SQLite.
- `verified_replay_dataset_status` checks R2 object metadata for replay artifacts.
- `load_replay_dataset` stages replay artifacts under `data/tmp/validate/...`.
- `delete_remote_replay_dataset` removes Layer 2 replay artifacts from R2 and SQLite.
- `delete_raw_market_data` removes Layer 1 raw data and refuses while replay exists unless cascade is requested.
- `cleanup_tmp` removes disposable staging files left by failed or interrupted jobs.

## Boundaries

This crate does not decode Databento DBN, normalize MBO records, run the order
book, simulate fills, or format CLI output. It provides storage primitives and
staging semantics for `ledger-ingest`, `ledger`, and `ledger-cli`.
