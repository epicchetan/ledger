# ledger-cli

`ledger-cli` is the terminal adapter for Ledger.

## Owns

- CLI argument parsing with `clap`.
- `.env` loading with `dotenvy`.
- Construction of `ledger-store`, `ledger-ingest`, and `ledger-runtime`.
- JSON output for command results.

## Does Not Own

- Databento request logic.
- DBN decoding or preprocessing.
- SQLite schema and R2 persistence.
- Replay session cache policy.
- Order-book or replay simulation logic.

## Commands

Public commands:

```text
resolve
ingest
status
list
session load
cache prune
```

`download` is kept as a hidden alias for `ingest`.

## Boundary

Keep this crate thin. If a command needs meaningful behavior beyond parsing arguments and printing a result, that behavior belongs in one of the library crates.

## Tests

Most command behavior should be verified through the crates it calls. CLI-specific tests should focus on command surface and argument parsing when needed.
