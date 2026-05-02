# CLI Reference

The CLI is a thin adapter over the Ledger crates. It loads `.env`, constructs the required services, and prints JSON.

Global options:

```bash
--data-dir <path>      # default: data, env: LEDGER_DATA_DIR
--r2-prefix <prefix>   # default: ledger/v1, env: LEDGER_R2_PREFIX
--quiet                # suppress progress logs on stderr
```

Commands print progress to stderr and keep structured results on stdout as JSON.

## `resolve`

Resolve an ES contract and market date into Ledger's market-day window.

```bash
cargo run -p ledger-cli -- resolve --symbol ESH6 --date 2026-03-12
```

This does not touch R2, SQLite, or local replay dataset files. It only prints the resolved market-day metadata.

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
catalogs Layer 1 raw data and Layer 2 ReplayDataset artifacts
```

Rerunning `ingest` for a ready `MarketDay` should reuse the cataloged durable objects and avoid a Databento redownload.

`download` is a hidden alias for `ingest`.

## `status`

Inspect one MarketDay's durable layer state.

```bash
cargo run -p ledger-cli -- status --symbol ESH6 --date 2026-03-12
```

Important fields:

```text
catalog_found              SQLite knows about this market day
raw                        Layer 1 raw data record, if present
replay_dataset             Layer 2 replay dataset record, if present
objects                    replay artifact R2 object statuses
replay_objects_valid       object metadata verification result when requested
last_validation            latest persisted validation report
```

`status` does not hydrate files.

## `list`

Query cataloged market days.

```bash
cargo run -p ledger-cli -- list --root ES
```

Useful variants:

```bash
cargo run -p ledger-cli -- list --symbol ESH6
cargo run -p ledger-cli -- list --root ES
```

This reads SQLite only. It does not scan R2.

## `session validate`

Validate a locally loaded `ReplayDataset`.

```bash
cargo run -p ledger-cli -- session validate --symbol ESH6 --date 2026-03-12
```

This command:

```text
loads or hydrates replay dataset artifacts
decodes events, batches, and trades into an EventStore
validates rebuilt batch/trade indexes against decoded indexes
compares the deterministic book-check report with book_check.v1.json
steps the replay simulator through a small probe
```

By default, the replay probe steps one batch. Useful variants:

```bash
cargo run -p ledger-cli -- session validate --symbol ESH6 --date 2026-03-12 --replay-batches 1000
cargo run -p ledger-cli -- session validate --symbol ESH6 --date 2026-03-12 --replay-all
cargo run -p ledger-cli -- session validate --symbol ESH6 --date 2026-03-12 --skip-book-check
```

`session validate` is a CLI adapter over the same validation composition that
the API uses. It proves replay artifacts decode, index validation holds,
book-check still matches, and replay simulation can consume the hydrated
`EventStore`.

## `replay run`

Run the active `ReplaySession` controller headlessly and print a deterministic
JSON report.

```bash
cargo run -p ledger-cli -- replay run --symbol ESH6 --date 2026-03-12
```

Useful variants:

```bash
cargo run -p ledger-cli -- replay run --symbol ESH6 --date 2026-03-12 --batches 100
cargo run -p ledger-cli -- replay run --symbol ESH6 --date 2026-03-12 --start-ts-ns 1773235800000000000
cargo run -p ledger-cli -- replay run --symbol ESH6 --date 2026-03-12 --truth-visibility
```

This is the headless adapter for agentic/developer validation of replay
feature work. It loads the `ReplayDataset` through the local replay cache,
opens a `ReplaySession`, steps the requested number of batches, and reports
cursor, batch index, checksum, BBO, frame count, and fill count. It does not
persist a training session.

## `replay cache-status`

Inspect whether the current durable `ReplayDataset` is cached locally.

```bash
cargo run -p ledger-cli -- replay cache-status --symbol ESH6 --date 2026-03-12
```

## `replay cache-remove`

Remove the local replay cache for a market day. This only deletes
`data/cache/replay/...`; it does not delete R2 replay artifacts or SQLite
catalog records for the durable `ReplayDataset`.

```bash
cargo run -p ledger-cli -- replay cache-remove --symbol ESH6 --date 2026-03-12
```

## `storage cleanup-tmp`

Delete disposable job staging files under `data/tmp`.

```bash
cargo run -p ledger-cli -- storage cleanup-tmp
cargo run -p ledger-cli -- storage cleanup-tmp --older-than-hours 24
```

Successful API/CLI jobs should clean their own staging directories. This command
is for failed jobs, interrupted runs, and manual disk recovery.
