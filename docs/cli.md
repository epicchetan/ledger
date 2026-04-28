# CLI Reference

The CLI is a thin adapter over the Ledger crates. It loads `.env`, constructs the required services, and prints JSON.

Global options:

```bash
--data-dir <path>      # default: data, env: LEDGER_DATA_DIR
--r2-prefix <prefix>   # default: ledger/v1, env: LEDGER_R2_PREFIX
```

## `resolve`

Resolve an ES contract and market date into Ledger's market-day window.

```bash
cargo run -p ledger-cli -- resolve --symbol ESH6 --date 2026-03-12
```

This does not touch R2, SQLite, or local session files. It only prints the resolved market-day metadata.

## `ingest`

Prepare a market day end to end.

```bash
cargo run -p ledger-cli -- ingest --symbol ESH6 --date 2026-03-12
```

This command:

```text
downloads or reuses raw DBN
preprocesses DBN into replay artifacts
runs book-check
uploads raw/artifacts to R2
writes SQLite catalog rows
commits replay artifacts into data/sessions/
```

Rerunning `ingest` for a ready session should reuse the cataloged durable objects and avoid a Databento redownload.

`download` is a hidden alias for `ingest`.

## `status`

Inspect one session's catalog and local replay-cache state.

```bash
cargo run -p ledger-cli -- status --symbol ESH6 --date 2026-03-12
```

Important fields:

```text
catalog_found              SQLite knows about this market day
ready                      ingest completed and required objects are cataloged
raw_available_remote       raw DBN object is cataloged
artifacts_available_remote replay artifacts are cataloged
session_loaded_local       replay artifacts have local paths
session_cache_valid        local replay artifacts pass size/SHA validation
last_accessed_ns           last session load/access time
```

`status` does not hydrate files. Use `session load` to bring replay artifacts local.

## `list`

Query cataloged sessions.

```bash
cargo run -p ledger-cli -- list --root ES --ready
```

Useful variants:

```bash
cargo run -p ledger-cli -- list --symbol ESH6
cargo run -p ledger-cli -- list --root ES
```

This reads SQLite only. It does not scan R2.

## `session load`

Load replay artifacts for a ready session.

```bash
cargo run -p ledger-cli -- session load --symbol ESH6 --date 2026-03-12
```

This asks `ledger-store` for replay inputs. Store reuses valid files under `data/sessions/` or hydrates missing/corrupt replay artifacts from R2. Raw DBN is not loaded into the session cache.

Returned paths point at:

```text
events.v1.bin
batches.v1.bin
trades.v1.bin
book_check.v1.json
```

## `cache prune`

Prune old loaded replay sessions.

```bash
cargo run -p ledger-cli -- cache prune --max-sessions 5
```

Pruning removes least-recently-used session directories under `data/sessions/`. It does not delete R2 objects or SQLite object rows.
