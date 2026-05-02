# Ledger Store Architecture

Ledger storage now separates the local control plane from the durable data
plane:

```text
SQLite = local Ledger control plane
R2     = durable large-object data plane
tmp    = disposable job staging
```

SQLite owns catalog state, object metadata, jobs, validation summaries, and
future journal/session/study state. R2 stores large immutable blobs. Normal code
does not use R2 JSON manifests as a catalog.

## Layers

```text
Layer 1: Raw Market Data
  raw Databento DBN/ZST object in R2
  expensive to recreate because it may require a paid download

Layer 2: ReplayDataset
  event_store, batch_index, trade_index, book_check objects in R2
  derived from one raw object hash
  cheap to delete and rebuild from Layer 1
```

## R2 Layout

```text
ledger/v1/market-days/ES/ESH6/2026-03-12/raw/databento/GLBX.MDP3/mbo/raw.sha256=<sha>.dbn.zst

ledger/v1/market-days/ES/ESH6/2026-03-12/replay/raw=<raw-sha>/events.v1.bin
ledger/v1/market-days/ES/ESH6/2026-03-12/replay/raw=<raw-sha>/batches.v1.bin
ledger/v1/market-days/ES/ESH6/2026-03-12/replay/raw=<raw-sha>/trades.v1.bin
ledger/v1/market-days/ES/ESH6/2026-03-12/replay/raw=<raw-sha>/book_check.v1.json
```

SQLite stores the exact R2 keys, sizes, hashes, schema versions, producers, and
lineage relationships.

## Local Layout

```text
data/
  ledger.sqlite
  tmp/
    ingest/
    validate/
```

`tmp/` is disposable staging. Persistent replay caches are intentionally
deferred until active replay needs them.

## Lifecycle

```text
prepare
  -> ensure market_day row exists
  -> ensure raw DBN exists in R2 and SQLite
  -> build replay artifacts when missing or rebuild is requested
  -> upload immutable R2 objects
  -> write SQLite replay dataset/artifact rows
  -> run validation
  -> write SQLite validation report

validate
  -> look up replay artifact keys from SQLite
  -> stage artifacts under data/tmp/validate/...
  -> decode and run validation/probe
  -> write SQLite validation report

delete replay
  -> delete Layer 2 objects from R2
  -> delete replay dataset/artifact/validation rows from SQLite
  -> preserve Layer 1 raw data
```
