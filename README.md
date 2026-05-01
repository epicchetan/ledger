# Ledger

Ledger prepares historical ES futures market days into durable, replay-ready artifacts.

The main workflow is one command:

```bash
cargo run -p ledger-cli -- ingest --symbol ESH6 --date 2026-03-12
```

That command downloads or reuses raw Databento DBN, preprocesses it, runs a deterministic book check, stores durable blobs in R2, records the session in SQLite, and commits replay artifacts into the local session cache.

## Quick Start

Run tests first:

```bash
cargo test --workspace
```

Resolve a market day:

```bash
cargo run -p ledger-cli -- resolve --symbol ESH6 --date 2026-03-12
```

Ingest the day:

```bash
cargo run -p ledger-cli -- ingest --symbol ESH6 --date 2026-03-12
```

Inspect readiness:

```bash
cargo run -p ledger-cli -- status --symbol ESH6 --date 2026-03-12
```

Load replay artifacts into the local session cache:

```bash
cargo run -p ledger-cli -- session load --symbol ESH6 --date 2026-03-12
```

Validate decoded replay artifacts and run a replay probe:

```bash
cargo run -p ledger-cli -- session validate --symbol ESH6 --date 2026-03-12
```

List cataloged sessions:

```bash
cargo run -p ledger-cli -- list --root ES --ready
```

## Crates

```text
crates/domain   package ledger-domain: shared market-day, event, artifact, and replay types
crates/store    package ledger-store: SQLite catalog, R2 object store, session cache, object keys
crates/ingest   package ledger-ingest: Databento provider, DBN preprocessing, durable ingest pipeline
crates/book     package ledger-book: pure L3 order book
crates/replay   package ledger-replay: replay clock, visibility delay, latency, queue-ahead, and fills
crates/ledger   package ledger: application-facing Ledger and replay session loading boundary
crates/cli      package ledger-cli: thin terminal adapter
lens/           Vite frontend for Ledger Lens
```

## Storage Model

```text
R2              durable immutable blobs
SQLite          local queryable catalog at data/catalog.sqlite
session cache   replay artifacts only under data/sessions/
tmp/staging     raw DBN and incomplete ingest work under data/tmp/
```

Raw DBN files are cataloged and stored durably in R2, but they are not part of the replay session cache. Loading a replay session hydrates only replay artifacts:

```text
events.v1.bin
batches.v1.bin
trades.v1.bin
book_check.v1.json
```

## Environment

`.env` is loaded automatically by the CLI.

```bash
DATABENTO_API_KEY=db-...

LEDGER_R2_ACCOUNT_ID=...
LEDGER_R2_ACCESS_KEY_ID=...
LEDGER_R2_SECRET_ACCESS_KEY=...
LEDGER_R2_BUCKET=ledger
LEDGER_R2_PREFIX=ledger/v1
LEDGER_DATA_DIR=data
LEDGER_CACHE_MAX_SESSIONS=5
```

Optional:

```bash
LEDGER_R2_ENDPOINT_URL=...
LEDGER_R2_REGION=auto
LEDGER_R2_MULTIPART_THRESHOLD_BYTES=1073741824
LEDGER_R2_MULTIPART_PART_SIZE_BYTES=67108864
```

## CLI

See [docs/cli.md](docs/cli.md) for command details.

```bash
cargo run -p ledger-cli -- resolve --symbol ESH6 --date 2026-03-12
cargo run -p ledger-cli -- ingest --symbol ESH6 --date 2026-03-12
cargo run -p ledger-cli -- status --symbol ESH6 --date 2026-03-12
cargo run -p ledger-cli -- list --root ES --ready
cargo run -p ledger-cli -- session load --symbol ESH6 --date 2026-03-12
cargo run -p ledger-cli -- session validate --symbol ESH6 --date 2026-03-12
cargo run -p ledger-cli -- cache prune --max-sessions 5
```

`download` is kept as a hidden alias for `ingest`.

## Local Data

See [docs/local_data.md](docs/local_data.md) for the generated local layout.

Commit source, docs, `Cargo.toml`, and `Cargo.lock`. Do not commit `.env`, `data/`, `target/`, or `.DS_Store`.
