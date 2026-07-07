# CLI Reference

The CLI is a thin adapter over the `store` and `ledger` crates. It loads `.env`, constructs the store, prints progress to stderr, and keeps structured results on stdout as JSON.

```bash
cargo run -p ledger-cli -- <command>
```

Global options:

```bash
--data-dir <path>   # default: data, env: LEDGER_DATA_DIR
```

Configuration comes from `.env`:

```text
LEDGER_R2_ACCOUNT_ID              R2 account
LEDGER_R2_ACCESS_KEY_ID           R2 credentials
LEDGER_R2_SECRET_ACCESS_KEY       R2 credentials
LEDGER_R2_BUCKET                  R2 bucket
LEDGER_R2_ENDPOINT_URL            optional endpoint override
LEDGER_R2_REGION                  optional, default auto
LEDGER_R2_MULTIPART_THRESHOLD_BYTES   optional multipart tuning
LEDGER_R2_MULTIPART_PART_SIZE_BYTES   optional multipart tuning
LEDGER_STORE_LOCAL_MAX_BYTES      local copy budget for prune
DATABENTO_API_KEY                 required by `es fetch`
```

## ES days

An ES market day is a raw Databento DBN download plus a prepared replay artifact, both durable in R2. The lifecycle verbs:

- **fetch** — download a day from Databento and make it replay-ready (CLI only).
- **install** — make a day's artifact local again (lens, via remux).
- **offload** — drop local copies; R2 keeps every byte (lens or `store offload`).

### `es days`

Print the day catalog: every raw grouped by market day, with its artifact and state.

```bash
cargo run -p ledger-cli -- es days
```

### `es fetch`

Download raw ES MBO data from Databento, register it, and chain straight into prepare so the day lands replay-ready. The raw's local copy is offloaded once the artifact exists; the paid download stays durable in R2.

```bash
cargo run -p ledger-cli -- es fetch --day 2026-03-12 --symbol ESH6
```

Options:

```bash
--dataset <name>   # default: GLBX.MDP3
--force            # re-download even when a matching raw already exists
```

Without `--force`, a raw already registered for the same day/symbol/dataset is reused and no Databento request is made. Prepare always runs unforced: raw ids are content-addressed, so an existing valid artifact is reused and anything missing or invalid is rebuilt.

### `es prepare`

Build or validate replay artifacts from registered raws. Fetch already chains into this; run it directly for maintenance — for example rebuilding artifacts after an `ES_MBO_EVENT_STORE_VERSION` bump.

```bash
cargo run -p ledger-cli -- es prepare --raw-id sha256-...
cargo run -p ledger-cli -- es prepare --day 2026-03-12
cargo run -p ledger-cli -- es prepare --all
```

Exactly one of `--raw-id`, `--day`, or `--all` is required. `--force` rebuilds even when a valid artifact exists. The raw's local copy is offloaded after prepare; batch runs (`--day`, `--all`) report per-raw successes and failures and exit nonzero if any raw failed.

## Store

The store is a content-addressed object registry: SQLite descriptors, an R2 copy of every object, an R2 descriptor mirror, and optional local copies under `data/store`.

### `store list`

```bash
cargo run -p ledger-cli -- store list
cargo run -p ledger-cli -- store list --role raw --kind databento.dbn.zst
cargo run -p ledger-cli -- store list --id-prefix sha256-ab
```

### `store show`

```bash
cargo run -p ledger-cli -- store show --id sha256-...
```

### `store import-file`

Register a local file: upload to R2, mirror the descriptor, record it in SQLite.

```bash
cargo run -p ledger-cli -- store import-file \
  --path path/to/file.dbn.zst \
  --role raw \
  --kind databento.dbn.zst \
  --metadata-json '{"market_day":"2026-03-12","source_symbol":"ESH6"}'
```

`--file-name`, `--format`, and `--media-type` are optional overrides.

### `store hydrate`

Download an object's bytes from R2 into the local store. A valid local copy is a no-op.

```bash
cargo run -p ledger-cli -- store hydrate --id sha256-...
```

### `store offload`

Drop the local copy; the descriptor and R2 object remain. Refuses objects with no remote copy.

```bash
cargo run -p ledger-cli -- store offload --id sha256-...
```

### `store delete`

Delete an object everywhere: descriptor, local copy, R2 object, and R2 descriptor mirror. Raw objects are paid source data and refuse deletion unless `--force-raw` is passed. This is the only raw-delete path — remux never exposes one.

```bash
cargo run -p ledger-cli -- store delete --id sha256-...
cargo run -p ledger-cli -- store delete --id sha256-... --force-raw
```

### `store local-status`

Report the local store root, object count, bytes used, and the configured budget.

```bash
cargo run -p ledger-cli -- store local-status
```

### `store local-prune`

Evict least-recently-accessed local copies until usage fits `LEDGER_STORE_LOCAL_MAX_BYTES`.

```bash
cargo run -p ledger-cli -- store local-prune
```

### `store sync`

Rebuild the SQLite registry from the R2 descriptor mirror — for a fresh machine or a lost database. Local-copy state from the mirror is kept only when the bytes validate on this machine.

```bash
cargo run -p ledger-cli -- store sync --dry-run
cargo run -p ledger-cli -- store sync
cargo run -p ledger-cli -- store sync --overwrite   # replace existing descriptors too
```

### `store validate`

Verify descriptors against reality: local bytes are re-hashed; `--verify-remote` also checks the R2 object's size and checksum.

```bash
cargo run -p ledger-cli -- store validate --id sha256-...
cargo run -p ledger-cli -- store validate --role artifact --verify-remote
```

Without `--id`, validates everything matching the optional `--role`/`--kind` filter.

### `store abort-incomplete-uploads`

List (and with `--execute`, abort) dangling R2 multipart uploads left by interrupted imports.

```bash
cargo run -p ledger-cli -- store abort-incomplete-uploads
cargo run -p ledger-cli -- store abort-incomplete-uploads --execute
```

`--prefix` narrows the scan (default `store/objects`).
