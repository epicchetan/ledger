# ledger-cli

`ledger-cli` is the terminal adapter for Ledger store.

## Owns

- CLI argument parsing with `clap`.
- `.env` loading with `dotenvy`.
- Construction of `store`.
- JSON output for command results.

## Does Not Own

- R2 object persistence internals.
- Local object policy.
- Object descriptor storage.
- Runtime, projection, or feed behavior.

## Commands

```text
store list
store show
store import-file
store hydrate
store delete
store local-status
store local-prune
store validate
```

## Boundary

Keep this crate thin. If a command needs meaningful behavior beyond parsing
arguments and printing JSON, that behavior belongs in `store` or a future Ledger
library crate.
