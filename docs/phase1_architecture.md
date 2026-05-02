# Phase 1 Architecture

Phase 1 prepares historical ES market days for replay.

```text
ledger-cli ingest
  -> ledger-ingest downloads or stages raw Databento MBO
  -> ledger-ingest preprocesses DBN into replay artifacts
  -> ledger-book runs deterministic book-check
  -> ledger-store uploads durable blobs to R2
  -> ledger-store records catalog rows in SQLite
  -> ledger-store catalogs Layer 1 raw data and Layer 2 ReplayDataset artifacts
  -> ledger stages ReplayDatasets from R2-backed store artifacts
```

## Crate Boundaries

`ledger-domain` owns shared types and codecs. It has no Databento, R2, SQLite, CLI, or application dependency. It defines market-day resolution, normalized MBO events, batch/trade indexes, artifact binary codecs, and shared replay types.

`ledger-store` owns persistence. It manages the SQLite catalog, R2 object
operations, content-addressed object keys, ingest staging directories, Layer 1
raw records, Layer 2 replay dataset records, and validation staging.

`ledger-ingest` owns historical data preparation. It downloads Databento MBO data, preprocesses DBN into Ledger artifacts, runs book-check, and asks `ledger-store` to persist raw/artifact objects.

`ledger-book` owns the pure L3 order book. It applies normalized MBO events and produces deterministic book state/check outputs without knowing about Databento, R2, SQLite, CLI, or replay orchestration.

`ledger-replay` owns replay simulation. It handles replay timing, delayed visibility, execution latency, queue-ahead, fills, and conservative same-timestamp ordering.

`ledger` owns backend application use cases. It asks `ledger-store` for
R2-backed replay artifact paths, returns `ReplayDataset`, hydrates staged artifacts into
`ledger-domain::EventStore`, composes validation reports, runs replay probes
through `ledger-replay`, orchestrates ingest through `ledger-ingest`, and owns
replay dataset preparation/deletion use cases.

`ledger-api` is the HTTP transport adapter for Lens. It parses routes, returns
JSON, persists lifecycle jobs in SQLite, and delegates behavior to `ledger`.
Cheap status reads avoid hashing large artifacts; explicit prepare/build/
validate/delete jobs handle expensive work.

`ledger-cli` is a thin command adapter. It parses terminal arguments, loads
`.env`, constructs Ledger services, prints progress to stderr, and prints JSON
results to stdout. Its `session validate` command calls the same validation
composition used by the API.

## Ingest Output

A prepared market day has two durable layers cataloged in SQLite:

```text
Layer 1
raw_dbn       raw Databento .dbn.zst in R2

Layer 2
event_store   normalized MboEvent artifact
batch_index   F_LAST batch spans
trade_index   trade/fill index
book_check    deterministic order-book report
```

Replay artifacts are staged under `data/tmp/validate/...` for validation.

## Replay Rule

The simulator separates exchange truth, trader visibility, and simulated execution. Same-timestamp handling remains conservative: exchange batches at timestamp `T` process before a user order arriving at `T`.
