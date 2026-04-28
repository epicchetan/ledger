# Phase 1 Architecture

Phase 1 prepares historical ES market days for replay.

```text
ledger-cli ingest
  -> ledger-ingest downloads or stages raw Databento MBO
  -> ledger-ingest preprocesses DBN into replay artifacts
  -> ledger-book runs deterministic book-check
  -> ledger-store uploads durable blobs to R2
  -> ledger-store records catalog rows in SQLite
  -> ledger-store commits replay artifacts into the session cache
  -> ledger loads replay sessions from store
```

## Crate Boundaries

`ledger-domain` owns shared types and codecs. It has no Databento, R2, SQLite, CLI, or application dependency. It defines market-day resolution, normalized MBO events, batch/trade indexes, artifact binary codecs, and shared replay types.

`ledger-store` owns persistence. It manages the SQLite catalog, R2 object operations, content-addressed object keys, ingest staging directories, replay session cache, session loading, and session cache pruning.

`ledger-ingest` owns historical data preparation. It downloads Databento MBO data, preprocesses DBN into Ledger artifacts, runs book-check, and asks `ledger-store` to persist raw/artifact objects.

`ledger-book` owns the pure L3 order book. It applies normalized MBO events and produces deterministic book state/check outputs without knowing about Databento, R2, SQLite, CLI, or replay orchestration.

`ledger-replay` owns replay simulation. It handles replay timing, delayed visibility, execution latency, queue-ahead, fills, and conservative same-timestamp ordering.

`ledger` owns replay readiness and session loading. It asks `ledger-store` for ready replay artifact paths and returns those paths to callers.

`ledger-cli` is a thin command adapter. It parses terminal arguments, loads `.env`, constructs store/ingest/Ledger services, and prints JSON.

## Ingest Output

A ready market day has these durable objects cataloged in SQLite:

```text
raw_dbn       raw Databento .dbn.zst in R2
event_store   normalized MboEvent artifact
batch_index   F_LAST batch spans
trade_index   trade/fill index
book_check    deterministic order-book report
```

Only the replay artifacts are committed into `data/sessions/`.

## Replay Rule

The simulator separates exchange truth, trader visibility, and simulated execution. Same-timestamp handling remains conservative: exchange batches at timestamp `T` process before a user order arriving at `T`.
