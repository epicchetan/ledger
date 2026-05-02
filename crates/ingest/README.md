# ledger-ingest

`ledger-ingest` owns the market-data ingestion pipeline. It turns a requested ES
market day into the replay artifacts that the rest of Ledger can load.

## Responsibilities

- Integrate with Databento through the `MarketDataProvider` abstraction.
- Orchestrate raw DBN staging for ingest by calling `ledger-store`.
- Preprocess DBN MBO records into Ledger replay artifacts:
  - `events.v1.bin`
  - `batches.v1.bin`
  - `trades.v1.bin`
- Normalize Databento MBO records into `ledger-domain::MboEvent`.
- Build batch and trade indexes through `ledger-domain`.
- Run book-check through `ledger-book`.
- Produce `IngestReport` values for CLI/application callers.

## Boundaries

This crate should not own persistence policy. R2 uploads, SQLite catalog state,
staging paths, and temp directory layout belong to `ledger-store`.

This crate should not expose application or replay APIs. Staging a ready
`ReplayDataset` for validation or replay belongs to `ledger`, and execution simulation belongs to
`ledger-replay`.

## Main Modules

- `provider`: provider trait plus the Databento implementation.
- `databento_downloader`: Databento range request and download wrapper.
- `preprocess`: DBN decoding, MBO normalization, and artifact writing.
- `book_check`: deterministic order-book validation report.
- `pipeline`: end-to-end ingest orchestration and ingest reports.

## Ingest Shape

`IngestPipeline::ingest_market_day` resolves a `MarketDay`, reuses a ready
`ReplayDataset` when possible, otherwise stages raw input, preprocesses replay
artifacts, uploads and registers durable objects through `ledger-store`, runs
book-check, catalogs the replay dataset, and returns an `IngestReport`.

Raw DBN files are ingest inputs. They are staged through `ledger-store` and kept
durable in R2 as Layer 1 data.

## Testing

Use fake providers and `SyntheticPreprocessor` for deterministic tests. Tests in
this crate should verify provider reuse, resumable ingest behavior, and artifact
production.
