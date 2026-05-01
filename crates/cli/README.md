# ledger-cli

`ledger-cli` is the terminal adapter for Ledger.

## Owns

- CLI argument parsing with `clap`.
- `.env` loading with `dotenvy`.
- Construction of `ledger-store`, `ledger-ingest`, and `ledger`.
- JSON output for command results.
- Progress output on stderr.

## Does Not Own

- Databento request logic.
- DBN decoding or preprocessing.
- SQLite schema and R2 persistence.
- Replay dataset cache policy.
- Order-book or replay simulation logic.

## Commands

Public commands:

```text
resolve
ingest
status
list
session load
session validate
cache prune
```

`download` is kept as a hidden alias for `ingest`.

The `session` command namespace is still the current CLI surface, but `session
load` and `session validate` operate on immutable `ReplayDataset` artifacts.
The active mutable `ReplaySession` controller is future work.

## Boundary

Keep this crate thin. If a command needs meaningful behavior beyond parsing
arguments and printing a result, that behavior belongs in one of the library
crates. `session validate` is intentionally a local validation adapter that
composes replay dataset hydration, book-check comparison, and a small replay
simulator probe before API/server work begins.

## Tests

Most command behavior should be verified through the crates it calls. CLI-specific tests should focus on command surface and argument parsing when needed.
