# Local Data

Ledger writes generated local state under `LEDGER_DATA_DIR`, which defaults to:

```text
data/
```

This directory is ignored by git.

## Layout

```text
data/
  catalog.sqlite
  sessions/
  tmp/
```

## `catalog.sqlite`

`catalog.sqlite` is the local SQLite catalog.

It records:

```text
known market days
durable R2 objects
artifact dependencies
loaded replay dataset files
ingest runs
```

CLI commands like `status` and `list` read this catalog. The catalog is not currently backed up. If it is deleted, R2 blobs remain durable, but Ledger will not automatically rebuild the catalog from R2 in this phase.

## `sessions/`

`sessions/` is the replay dataset cache. The directory name is historical; the
contents are immutable `ReplayDataset` artifacts, not active `ReplaySession`
state.

Example:

```text
data/sessions/ES/ESH6/2026-03-12/
  events.v1.bin
  batches.v1.bin
  trades.v1.bin
  book_check.v1.json
```

These files are local speed-ups for replay and replay dataset loading. They can
be removed and later hydrated again from R2 by running:

```bash
cargo run -p ledger-cli -- session load --symbol ESH6 --date 2026-03-12
```

To decode the local artifacts, validate their indexes, compare `book_check`, and
run a small replay probe:

```bash
cargo run -p ledger-cli -- session validate --symbol ESH6 --date 2026-03-12
```

Raw DBN files do not belong in `sessions/`.

## `tmp/`

`tmp/` is used during ingest.

Example:

```text
data/tmp/ingest/ES/ESH6/2026-03-12/<run-id>/
  raw.dbn.zst
  artifacts/
```

Ingest writes incomplete work here first. After successful ingest, replay artifacts are committed into `sessions/` and the ingest temp directory is deleted.

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
