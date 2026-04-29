# Ledger Store Architecture

Ledger separates durable storage, local catalog queries, replay session loading, and ingest staging.

## Responsibilities

`ledger-store` owns four things:

```text
R2              durable immutable blobs
SQLite          local operational catalog
session cache   replay artifacts used by Ledger/replay
tmp/staging     incomplete ingest work
```

The important boundary is that the session cache is not a generic object cache. It contains only replay artifacts. Raw DBN is staged for ingest and stored durably in R2, but it is not kept as a replay cache file.

## Local Layout

```text
data/
  catalog.sqlite
  sessions/
    ES/ESH6/2026-03-12/
      events.v1.bin
      batches.v1.bin
      trades.v1.bin
      book_check.v1.json
  tmp/
    ingest/ES/ESH6/2026-03-12/<run-id>/
      raw.dbn.zst
      artifacts/
        events.v1.bin
        batches.v1.bin
        trades.v1.bin
        book_check.v1.json
```

## SQLite Catalog

SQLite lives at:

```text
data/catalog.sqlite
```

It records:

```text
market_days              known sessions and readiness
objects                  durable R2 objects, including raw_dbn
object_dependencies      artifact lineage
session_cache_entries    local replay artifact files only
ingest_runs              local ingest attempts
```

The catalog is the query surface for CLI and application usage. Commands like `ledger list` and `ledger status` read SQLite.

## R2 Objects

R2 stores immutable blobs under content-addressed keys.

Raw DBN:

```text
ledger/v1/raw/databento/GLBX.MDP3/mbo/ES/ESH6/2026-03-12/raw.sha256=<sha>.dbn.zst
```

Replay artifacts:

```text
ledger/v1/artifacts/ES/ESH6/2026-03-12/<kind>/schema=v1/input=<raw_sha>/producer=<version>/<file>
```

Ledger verifies uploaded and hydrated objects using size and SHA256 metadata. ETag is not treated as content identity.

## Ingest Lifecycle

```text
download or hydrate raw DBN into data/tmp/
  -> preprocess into data/tmp/.../artifacts/
  -> hash each file
  -> upload or reuse immutable R2 object
  -> insert SQLite object rows
  -> commit replay artifacts into data/sessions/
  -> insert SQLite session_cache_entries rows
  -> mark market day ready
  -> delete ingest tmp directory
```

Only complete, verified replay artifacts are committed to `data/sessions/`.

## Replay Session Loading

`Ledger` loads a replay session through `ledger-store`.

```text
load replay session
  -> query SQLite for ready replay artifacts
  -> reuse valid data/sessions files when present
  -> hydrate missing/corrupt artifacts from R2
  -> validate size and SHA256
  -> return ReplaySession with local artifact paths
```

Callers should not request individual artifact hydration. Loading the replay session is the abstraction, and `ReplaySession` can decode those local artifacts into an `EventStore`.

## Cache Pruning

The session cache is pruned by least-recently-used market session. The default limit is:

```text
LEDGER_CACHE_MAX_SESSIONS=5
```

Pruning removes whole session directories under `data/sessions/`. It does not delete R2 objects, SQLite object rows, or raw DBN staging files.

## Catalog Durability

The SQLite catalog is not backed up in this phase. If `catalog.sqlite` is deleted, durable R2 blobs remain, but automatic catalog rebuild from R2 is out of scope. Rerun `ledger ingest` for a session to repopulate the local catalog.
