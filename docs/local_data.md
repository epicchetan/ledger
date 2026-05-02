# Local Data

Ledger writes generated local state under `LEDGER_DATA_DIR`, which defaults to:

```text
data/
```

This directory is ignored by git.

## Layout

```text
data/
  ledger.sqlite
  cache/
  tmp/
```

## `ledger.sqlite`

`ledger.sqlite` is the local Ledger control-plane database.

It records:

```text
known market days
durable R2 object keys, sizes, and hashes
raw/replay layer records
validation reports
job records and job events
```

Do not delete this file casually. R2 keeps the large blobs, but SQLite owns the
catalog that tells Ledger which blobs matter.

## `tmp/`

`tmp/` is disposable job staging.

Examples:

```text
data/tmp/ingest/ES/ESH6/2026-03-12/<run-id>/
  raw.dbn.zst
  artifacts/

data/tmp/validate/ES/ESH6/2026-03-12/<run-id>/
  events.v1.bin
  batches.v1.bin
  trades.v1.bin
  book_check.v1.json
```

It is safe to remove `tmp/` when API/jobs are stopped.

## `cache/`

`cache/` is disposable local replay performance storage.

Examples:

```text
data/cache/replay/ES/ESH6/2026-03-12/<replay-dataset-id>/
  events.v1.bin
  batches.v1.bin
  trades.v1.bin
  book_check.v1.json
```

Active `ReplaySession` startup uses this as a read-through cache for immutable
ReplayDataset artifacts. R2 remains the durable source of truth, and SQLite
records cache metadata for status display and LRU eviction. The default cap is
10 cached ReplayDatasets and can be changed with
`LEDGER_REPLAY_CACHE_MAX_DATASETS`.

It is safe to remove `cache/` when API/replay sessions are stopped. Ledger will
download needed ReplayDataset artifacts from R2 again on the next replay run.

## Commit Hygiene

Commit:

```text
Cargo.toml
Cargo.lock
README.md
docs/
crates/
.gitignore
```

Do not commit:

```text
.env
data/
target/
.DS_Store
```
