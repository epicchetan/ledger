# ledger-cli

`ledger-cli` is the terminal adapter for Ledger.

## Owns

- CLI argument parsing with `clap`.
- `.env` loading with `dotenvy`.
- Construction of `ledger`.
- JSON output for command results.
- Progress output on stderr.

## Does Not Own

- Databento request logic.
- DBN decoding or preprocessing.
- SQLite schema and R2 persistence.
- R2 object persistence or local materialization policy.
- Order-book or replay simulation logic.

## Commands

Public commands:

```text
resolve
ingest
status
list
session validate
storage cleanup-tmp
```

`download` is kept as a hidden alias for `ingest`.

`session validate` operates on immutable `ReplayDataset` artifacts and delegates
shared validation composition to `ledger`. `storage cleanup-tmp` removes
disposable staging files left by failed or interrupted jobs. The active mutable
`ReplaySession` controller is future work.

## Boundary

Keep this crate thin. If a command needs meaningful behavior beyond parsing
arguments and printing a result, that behavior belongs in one of the library
crates. `session validate` delegates shared validation composition to `ledger`,
so CLI and API validation stay aligned.

## Tests

Most command behavior should be verified through the crates it calls. CLI-specific tests should focus on command surface and argument parsing when needed.
