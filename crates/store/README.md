# store

`store` is Ledger's generic object registry and local object layer.

It stores content-addressed raw files and artifacts through R2, tracks local
descriptor JSON under `data/store/registry`, and hydrates local files under
`data/store/local`.

This crate is intentionally independent from runtime cells, feeds, projections,
and market-domain types.
