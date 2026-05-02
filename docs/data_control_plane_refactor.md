# Data Control Plane Refactor Spec

## Summary

Move Ledger Data Center back to a simpler and more durable model:

```text
SQLite = local Ledger control plane
R2     = durable large-object data plane
tmp    = disposable job staging
```

SQLite should own the catalog, job state, validation summaries, and later
journaling/session/study state. R2 should store raw DBN files and derived replay
artifacts, but it should not be the active catalog through JSON manifests.

This spec intentionally removes the Data Center-facing `Load ReplayDataset`
action. Loading/hydrating artifacts is an internal implementation detail for
validation now and for active `ReplaySession` later. `Prepare` and
`Rebuild ReplayDataset` must always validate before completing so a finished
job leaves behind a current trust status.

## Goals

- Make Lens/API status reads fast, local, and consistent.
- Persist job state so API restarts and UI polling do not lose progress.
- Keep R2 focused on storing large blobs that do not fit well on disk.
- Keep the data lifecycle simple enough to reason about during early product
  development.
- Preserve the important Layer 1 / Layer 2 distinction:
    - Layer 1: expensive raw Databento DBN.
    - Layer 2: cheaper replay artifacts derived from raw data.
- Remove persistent `data/materialized/...` from the active product model.
- Remove R2 manifests from the active product model.
- Clean up R2 object paths while the dataset is still tiny.
- Preserve existing DBN files without redownloading them from Databento.
- Keep room for a future explicit replay cache when active replay performance
  requires it.

## Non-Goals

- Do not add active `ReplaySession` control.
- Do not add WebSocket projection streaming.
- Do not design journaling tables in full.
- Do not build a general distributed job queue.
- Do not solve remote backup of SQLite yet.
- Do not keep API backwards compatibility for `replay/load`.

## Source of Truth

### SQLite: Control Plane

SQLite is the authoritative local Ledger database for:

```text
known market days
R2 object keys, sizes, hashes, schemas, producers
raw market data records
ReplayDataset records
ReplayDataset artifact records
validation reports and summaries
jobs and job events
future ReplaySession history
future journals, layouts, studies, levels, and simulated orders/fills
```

If SQLite does not know about an object, Ledger should treat that object as
unknown even if a blob might exist in R2. This makes status reads deterministic
and gives Lens one local truth to query.

### R2: Data Plane

R2 stores durable large blobs:

```text
raw DBN/ZST files
events.v1.bin
batches.v1.bin
trades.v1.bin
book_check.v1.json
large validation reports if needed later
future heavy study artifacts
future backup exports
```

R2 object keys should remain deterministic and organized. R2 should not be the
active catalog. No new R2 JSON manifest files should be written in the normal
path.

### R2 Object Layout

Keep one top-level storage layout version:

```text
ledger/v1/...
```

Do not add extra path-level versioning for producer versions or artifact schema
versions unless it earns its keep. SQLite already records schema versions,
producer versions, sizes, hashes, and relationships. Artifact filenames may
still include their binary schema version where that improves readability.

Target raw object layout:

```text
ledger/v1/market-days/ES/ESH6/2026-03-12/raw/databento/GLBX.MDP3/mbo/raw.sha256=<sha>.dbn.zst
```

Target replay artifact layout:

```text
ledger/v1/market-days/ES/ESH6/2026-03-12/replay/raw=<raw-sha>/events.v1.bin
ledger/v1/market-days/ES/ESH6/2026-03-12/replay/raw=<raw-sha>/batches.v1.bin
ledger/v1/market-days/ES/ESH6/2026-03-12/replay/raw=<raw-sha>/trades.v1.bin
ledger/v1/market-days/ES/ESH6/2026-03-12/replay/raw=<raw-sha>/book_check.v1.json
```

This layout optimizes for manual inspection and prefix deletion:

```text
market-days/<root>/<contract>/<date>/raw/...
market-days/<root>/<contract>/<date>/replay/...
```

The old raw and artifact paths may exist temporarily during migration. Normal
code should write only the new layout after this refactor.

### `tmp`: Job Workspace

`data/tmp/...` is disposable staging for active jobs:

```text
data/tmp/ingest/ES/ESH6/2026-03-12/<job-id>/
  raw.dbn.zst
  artifacts/
    events.v1.bin
    batches.v1.bin
    trades.v1.bin
    book_check.v1.json

data/tmp/validate/ES/ESH6/2026-03-12/<job-id>/
  events.v1.bin
  batches.v1.bin
  trades.v1.bin
  book_check.v1.json
```

Successful jobs should clean their temp directory. Failed jobs may leave temp
files for diagnosis, with a later explicit cleanup command.

### No Persistent Materialization Yet

Do not expose or depend on persistent `data/materialized/...` in Data Center.
Replay artifact files may be downloaded into `tmp` for validation. A future
active replay cache can be added later when replay speed and repeated access
patterns are real requirements.

## Local Layout

Target layout:

```text
data/
  ledger.sqlite
  tmp/
    ingest/
    validate/
```

Optional later layout:

```text
data/
  ledger.sqlite
  tmp/
  cache/
    replay/
    studies/
```

`cache/` should not be introduced until active replay or study computation needs
it.

## API Surface

Target Data Center API:

```text
GET    /health
GET    /market-days
GET    /market-days/:symbol/:date
POST   /market-days/:symbol/:date/prepare
POST   /market-days/:symbol/:date/replay/build
POST   /market-days/:symbol/:date/replay/validate
DELETE /market-days/:symbol/:date/replay
DELETE /market-days/:symbol/:date/raw
GET    /jobs
GET    /jobs/:id
```

Remove:

```text
POST /market-days/:symbol/:date/replay/load
```

`Load ReplayDataset` is not a Data Center action. Opening a day as an active
`ReplaySession` will later be a different product action.

## Lens Surface

Data Center actions:

```text
Prepare
Rebuild ReplayDataset
Validate
Delete ReplayDataset
Delete Raw Data
```

Remove:

```text
Load ReplayDataset
```

`Validate` means re-run validation on an existing ReplayDataset. It is a
diagnostic/re-audit action, not a required second step after `Prepare` or
`Rebuild ReplayDataset`.

Lens should render status from the API/SQLite control-plane view:

```text
Missing
Raw Available
ReplayDataset Available
Ready to Train
Ready with Warnings
Invalid
Job Running
```

Lens should not display local materialization state as a primary concept.

## SQLite Schema Direction

The current `SqliteCatalog` schema is already close to this, but the name and
responsibility should change. The target concept is `LedgerDb`, not a cache or
R2-mirror catalog.

The SQL schema should describe searchable control-plane facts and relationships,
not payload bytes. It is appropriate for SQLite to store object keys, hashes,
sizes, lineage, validation summaries, and jobs. It should not store raw DBN
bytes, replay artifact bytes, or large study outputs.

### `market_days`

Owns known market-day rows.

```text
id                 text primary key
root               text not null
contract_symbol    text not null
market_date        text not null
session_start_ns   integer not null
session_end_ns     integer not null
source_provider    text
status             text not null
created_at_ns      integer not null
updated_at_ns      integer not null
```

`status` should be a coarse lifecycle state only. Replay readiness should be
derived from joined raw/replay/validation rows.

### `objects`

Owns durable R2 blob metadata.

```text
id                 text primary key
market_day_id      text not null references market_days(id)
kind               text not null
storage_kind       text not null
remote_key         text not null unique
size_bytes         integer not null
sha256             text not null
format             text not null
schema_version     integer not null
producer           text
producer_version   text
source_provider    text
source_dataset     text
source_schema      text
source_symbol      text
metadata_json      text not null
created_at_ns      integer not null
updated_at_ns      integer not null
```

`remote_key` is the address in R2. SQLite owns the fact that the object exists.

### `raw_market_data`

Owns Layer 1 state.

```text
market_day_id      text primary key references market_days(id)
object_id          text not null references objects(id)
status             text not null
created_at_ns      integer not null
updated_at_ns      integer not null
```

### `replay_datasets`

Owns Layer 2 state.

```text
id                 text primary key
market_day_id      text not null references market_days(id)
raw_object_id      text not null references objects(id)
status             text not null
event_count        integer not null
batch_count        integer not null
trade_count        integer not null
producer           text
producer_version   text
created_at_ns      integer not null
updated_at_ns      integer not null
```

There should be at most one current available replay dataset per market day in
V1. Older versions can be deleted or marked superseded later.

### `replay_dataset_artifacts`

Owns Layer 2 artifact object membership.

```text
replay_dataset_id  text not null references replay_datasets(id)
kind               text not null
object_id          text not null references objects(id)
created_at_ns      integer not null
primary key (replay_dataset_id, kind)
```

Required artifact kinds:

```text
event_store
batch_index
trade_index
book_check
```

### `validation_reports`

Owns latest and historical validation summaries.

```text
id                 text primary key
market_day_id      text not null references market_days(id)
replay_dataset_id  text references replay_datasets(id)
status             text not null
mode               text not null
summary_json       text not null
report_object_id   text references objects(id)
created_at_ns      integer not null
```

`summary_json` should stay small enough for Lens. If a full report becomes
large, store it as an R2 object and reference it through `report_object_id`.

### `jobs`

Owns API/CLI job lifecycle.

```text
id                 text primary key
kind               text not null
status             text not null
market_day_id      text references market_days(id)
created_at_ns      integer not null
started_at_ns      integer
finished_at_ns     integer
request_json       text not null
result_json        text
error              text
```

### `job_events`

Owns durable progress logs.

```text
id                 integer primary key autoincrement
job_id             text not null references jobs(id)
created_at_ns      integer not null
level              text not null
message            text not null
```

Lens should poll jobs from SQLite-backed endpoints. API restart should not lose
the last visible state.

On API startup, any `queued` or `running` job from a previous process should be
marked:

```text
failed: API restarted before job completed
```

This is conservative and honest for a local dev tool.

## Crate-Level Changes

### `ledger-store`

Refactor responsibility:

```text
before: storage boundary + SQLite catalog + R2 manifests + materialization
after:  storage boundary + LedgerDb + R2 blob operations + tmp staging
```

Changes:

- Rename `SqliteCatalog` to `LedgerDb` or add `LedgerDb` as the new public
  wrapper around the existing SQLite implementation.
- Rename `data/catalog.sqlite` to `data/ledger.sqlite`.
- Remove R2 manifest writes from normal paths:
    - `put_market_day_manifest`
    - `put_raw_manifest`
    - `put_replay_dataset_manifest`
    - `put_validation_manifest_record`
- Stop using R2 manifest reads for status/list/load:
    - `list_market_days` should read `LedgerDb`.
    - `replay_dataset_status` should read `LedgerDb`.
    - validation should look up artifact object keys through `LedgerDb`.
- Keep `ObjectStore` for blob operations:
    - `put_file`
    - `get_bytes`
    - `delete`
    - `copy`, if R2/S3 copy is supported cleanly
    - optional future `head`
- Update `ObjectKeyBuilder` to generate the cleaned market-day-centered R2
  paths:
    - raw DBN under `market-days/.../raw/...`
    - replay artifacts under `market-days/.../replay/raw=<sha>/...`
    - no manifest keys in normal code
- Remove or quarantine `manifest.rs` as legacy migration code.
- Replace persistent materialization with temp staging helpers:
    - `stage_replay_dataset_for_validation(job_id, market_day)` downloads
      artifacts into `data/tmp/validate/...`.
    - `stage_raw_for_ingest(job_id, market_day)` downloads or writes raw input
      into `data/tmp/ingest/...`.
- Keep deletion rules:
    - delete replay removes replay artifact objects, replay rows, validation rows
      tied to that replay dataset, and leaves raw intact.
    - delete raw refuses if replay exists unless cascade is requested.
    - cascade deletes replay first, then raw.

### `ledger-ingest`

`IngestPipeline` remains the owner of raw download and replay artifact
preprocessing.

Target `prepare` flow:

```text
resolve MarketDay
upsert market_day row
create job-backed ingest staging under data/tmp/ingest/...

if raw_market_data row exists:
  download raw object from R2 into tmp only if needed for preprocessing
else:
  download Databento raw DBN into tmp
  upload raw DBN to R2
  insert objects + raw_market_data rows

if replay_dataset exists and rebuild=false:
  return existing replay dataset summary
else:
  if rebuild=true:
    delete existing replay dataset rows and R2 artifact objects
  preprocess raw tmp file into replay artifacts under tmp
  upload replay artifacts to R2
  insert objects + replay_datasets + replay_dataset_artifacts rows

run light validation
insert validation report row
cleanup tmp on success
```

`rebuild` should never redownload Databento data if `raw_market_data` exists.
It may download the raw object from R2 into `tmp` to rebuild Layer 2.

Validation is part of the prepare/rebuild contract. A successful prepare or
rebuild job must leave the market day in one of these states:

```text
Ready to Train
Ready with Warnings
Invalid
```

It should not leave a newly built ReplayDataset in an unvalidated state. The
separate `Validate` action exists to rerun validation later when validation
logic changes, corruption is suspected, or a deeper audit is requested.

### `ledger`

`ledger` remains the application orchestration boundary.

Changes:

- Keep `PrepareReplayDatasetRequest`.
- Keep `ValidateReplayDatasetRequest`.
- Keep `ReplayDataset` as immutable artifact context.
- Remove Data Center-facing load operation from the public API path.
- Replace any persistent materialization assumption with temp staging calls.
- Prepare/rebuild should internally stage artifacts from R2 or use fresh job
  artifacts, decode them, validate indexes/book check/replay probe according to
  the selected validation profile, then write validation summary to SQLite
  before returning success.
- Manual validation should use the same validation composition as
  prepare/rebuild and update the persisted validation report for the existing
  ReplayDataset.

Possible method shape:

```rust
impl Ledger<S> {
    pub async fn prepare_replay_dataset(...) -> Result<PrepareReplayDatasetReport>;
    pub async fn rebuild_replay_dataset(...) -> Result<PrepareReplayDatasetReport>;
    pub async fn validate_replay_dataset(...) -> Result<ReplayDatasetValidationReport>;
    pub async fn delete_replay_dataset(...) -> Result<DeleteReplayDatasetReport>;
    pub async fn delete_raw_market_data(...) -> Result<DeleteRawMarketDataReport>;
}
```

If lower-level artifact hydration is still needed for tests or future replay,
keep it as internal naming:

```text
stage_replay_dataset
hydrate_replay_dataset_for_validation
```

Avoid user/API naming like `load` until active `ReplaySession` exists.

### `ledger-api`

Changes:

- Remove `JobKind::LoadReplayDataset`.
- Remove route `POST /market-days/:symbol/:date/replay/load`.
- Replace in-memory `JobRegistry` with SQLite-backed job persistence.
- Keep an in-process task runner for now.
- On job creation:
    - insert `jobs` row as `queued`.
    - return job immediately.
- On job execution:
    - set `running`.
    - append `job_events`.
    - call `ledger`.
    - update DB status/result/error.
- `GET /jobs` should return active jobs by default.
- `GET /jobs?status=all` can be added if useful for debugging.
- `GET /jobs/:id` should return job metadata plus event/progress list.
- API logs should continue printing job progress to stderr for local diagnosis.

Startup behavior:

```text
mark stale queued/running jobs as failed
append job event: "API restarted before job completed"
```

This prevents Lens from polling permanently running jobs after a restart.

### `ledger-cli`

Recommended command shape:

```text
ledger-cli list
ledger-cli status --symbol ESH6 --date 2026-03-12
ledger-cli ingest --symbol ESH6 --date 2026-03-12
ledger-cli session validate --symbol ESH6 --date 2026-03-12
ledger-cli storage cleanup-tmp
```

Remove or demote:

```text
session load
storage publish-manifests
```

Migration commands were intentionally removed after the R2 layout was cleaned.
Future storage diagnostics should be added only when they serve active operation,
not as permanent migration surface area.

### `lens`

Changes:

- Remove `loadReplayDataset` API client function.
- Remove `DatasetAction` value `"load"`.
- Remove row menu item `Load ReplayDataset`.
- Remove copy that says replay artifacts are materialized locally on load.
- Keep active jobs panel above Market Days.
- Poll `/jobs` and `/jobs/:id` for durable job state.
- After a terminal job state, refresh market days from API.

Lens should represent durable data readiness, not local file hydration.

After a successful `Prepare` or `Rebuild ReplayDataset` job, Lens should refresh
and show the persisted validation/trust status immediately. The user should not
need to press `Validate` to discover whether the newly built dataset is usable.

## Migration Notes

The one-off R2 layout migration has been completed and the migration commands
have been removed from the active codebase. The current source of truth is:

```text
SQLite ledger.sqlite = catalog/control plane
R2 clean market-day paths = durable raw and replay blobs
data/tmp = disposable staging only
```

If `data/ledger.sqlite` is deleted, Ledger loses the local catalog and should
not pretend it can reconstruct everything automatically from R2. That is
acceptable for now. A future backup/export plan can protect the DB.

## Operational Rules

- Do not delete `data/ledger.sqlite` casually.
- It is safe to delete `data/tmp` when API/jobs are stopped.
- R2 raw objects are expensive and should be deleted only through explicit
  `Delete Raw Data`.
- Replay artifacts are cheaper and may be rebuilt from raw.
- Migration may delete stale replay artifacts and R2 manifests, but only after
  listing keys and preserving verified DBN objects.
- API and CLI should both go through `ledger` orchestration so validation and
  lifecycle logic do not drift.

## Error Handling

Job errors should include the full error chain.

Examples:

```text
ingesting market day ESH6 2026-03-11:
foreign key failed inserting replay artifact:
missing raw object row
```

Lens should display the job error and keep the market-day row visible. A failed
job should not make the whole Data Center unreadable.

## Test Plan

### Store Tests

- `list_market_days_reads_sqlite_not_r2_manifests`
- `status_reads_sqlite_rows_without_r2_scan`
- `object_keys_use_market_day_centered_layout`
- `prepare_records_raw_and_replay_rows`
- `prepare_persists_validation_report`
- `rebuild_preserves_raw_replaces_replay_artifacts_and_validates`
- `delete_replay_removes_layer_2_and_preserves_raw`
- `delete_raw_refuses_when_replay_exists`
- `delete_raw_with_cascade_removes_replay_then_raw`
- `validation_report_persists_to_sqlite`
- `tmp_staging_is_cleaned_after_success`

### API Tests

- `router_does_not_expose_replay_load`
- `create_job_persists_queued_job`
- `job_progress_persists_events`
- `successful_job_updates_result_and_market_day_status`
- `failed_job_persists_full_error_chain`
- `active_jobs_returns_only_queued_or_running`
- `startup_marks_stale_running_jobs_failed`

### Lens Tests

- Typecheck confirms no `loadReplayDataset` action remains.
- Row menu includes only:
    - Prepare
    - Rebuild ReplayDataset
    - Validate
    - Delete ReplayDataset
    - Delete Raw Data
- Empty/error states remain centered and readable.
- Active jobs panel appears above Market Days only when active jobs exist.

### CLI/Integration Tests

Run:

```bash
cargo fmt --all -- --check
cargo test -p ledger-store
cargo test -p ledger-api
cargo test --workspace
npm run typecheck
npm run lint
npm run build
```

Manual validation:

```bash
cargo run -p ledger-cli -- status --symbol ESH6 --date 2026-03-11
cargo run -p ledger-cli -- ingest --symbol ESH6 --date 2026-03-11
cargo run -p ledger-cli -- session validate --symbol ESH6 --date 2026-03-11 --skip-book-check --replay-batches 1
cargo run -p ledger-api
```

Implementation is not complete until the real CLI/R2 path has been exercised
against the existing two DBN-backed market days. We can use the .env file to handle the R2 calls.

Then in Lens:

```text
Prepare -> observe durable job progress -> row updates
Rebuild ReplayDataset -> raw remains, replay artifacts refresh, validation status updates
Validate -> re-run validation on the existing ReplayDataset
Delete ReplayDataset -> raw remains, replay missing
Delete Raw Data -> blocked unless replay is gone or cascade is explicit
```

## Future Work

Once active replay exists, add an explicit cache layer:

```text
data/cache/replay/<market-day>/<replay-dataset-id>/
```

That cache should be versioned, disposable, and owned by active replay/study
performance needs. It should not be part of the Data Center source-of-truth
model.

Once the database becomes valuable, add backup/export:

```text
ledger.sqlite -> compressed backup -> R2 backups/
```

That is a separate concern from storing replay artifacts.
