# ES Data Management Implementation Spec

## Purpose

Build the data-management layer of the ES feed: load and prepare ES market
data **by trading day**, prove every raw in the store is replay-ready, and
give Lens a Days surface to manage it all from the phone.

The organizing unit is the trading day. The future feed process does
nothing when the session clock sits at a time it has no data for, so the
data layer's whole job is to answer — and change — one question per day:

```text
do we have this MarketDay, and is it prepared for replay?
```

This is the first implementation phase. It deliberately depends on neither
the cache-watch spec nor the runtime: everything here is store reads,
store writes, and pure computation. It is the **feed module** half of the
feed design:

```text
feed module (THIS SPEC)
  domain knowledge that exists with no session running
  functions taking &Store<S>: catalog, fetch, prepare, day derivation
  called by the CLI (pass 1) and the remux adapter as jobs (pass 2)

feed process (ledger_feed_system_implementation_spec.md, later)
  the emitter instantiated into a session on the runtime
  loads the artifact this phase produces, unchanged
```

Two passes, one spec:

```text
pass 1  data layer + CLI
        crates/ledger, Store::update_metadata, `ledger es` commands
        exit: acceptance run over the three real raws

pass 2  jobs + Lens Days screen
        job registry in ledger-remux, es/jobs RPC methods, Days UI
        exit: prepare a day end to end from the phone
```

Pass 2 is expected to feed back into pass-1 shapes (catalog fields, summary
contents). That is intended — the UI drives the requirements — and both
passes land inside this phase, so reshaping is cheap.

## What "Validated" Means Here

The legacy pipeline ran a deep book-replay audit (order-book
reconstruction, warning counts, terminal checksums). That is **not** this
phase, and skipping it is deliberate: per `docs/vision.md` §4, prepare
proves *artifact integrity and deterministic replay-readiness*, not market
quality. The trust-report layer returns later, on top of this contract.

What prepare does enforce — and it comes free, because it is the parse
itself:

```text
input check          the object is role=raw, kind=databento.dbn.zst
                     -> anything else is a hard error before bytes move
full decode          every DBN record decodes as MboMsg, end of file reached
normalization gates  unknown action byte -> hard error
                     unknown side byte   -> hard error
                     price not aligned to 0.25 tick -> hard error
batch integrity      spans rebuilt from events must match stored spans
codec round trip     encode -> decode -> equal, before the artifact registers
market day           every event's ts_recv (delivery time) lies inside the
                     session bounds of the first event's day; anything
                     outside (another session, the 17:00-18:00 ET
                     maintenance halt) is a hard error. The check keys on
                     ts_recv, not ts_event, because Databento delivers an
                     opening book snapshot whose resting-order re-adds carry
                     their original (days-old) ts_event but are delivered
                     in-session
```

A raw that survives prepare is good enough for the feed process. The book
audit, trades, and bars stay in the Legacy Reference until their layers
arrive.

## Crate

```text
crates/ledger        package and lib name: ledger
```

Workspace update:

```toml
[workspace]
members = [
    "crates/cache",
    "crates/runtime",
    "crates/store",
    "crates/ledger",
    "crates/cli",
    "crates/remux",
]
```

Dependencies — note what is absent: no `cache`, no `runtime`. The module
layer must compile without them; they arrive with the feed process phase.

```toml
[dependencies]
anyhow.workspace = true
chrono.workspace = true
chrono-tz.workspace = true
databento.workspace = true
dbn.workspace = true
serde.workspace = true
serde_json.workspace = true
store = { path = "../store" }
thiserror.workspace = true
tokio.workspace = true

[dev-dependencies]
tempfile.workspace = true
```

Add workspace dependencies:

```toml
dbn = "0.55"
databento = "0.48"
chrono-tz = "0.10"
```

`dbn`/`databento` are pinned together at the legacy pipeline's proven pair.
Pass 1 also adds `ledger = { path = "../ledger" }` to `crates/cli`; pass 2
adds it to `crates/remux`.

## Module Layout

```text
crates/ledger/src/
  lib.rs
  error.rs
  feed/
    mod.rs
    es_replay/
      mod.rs           module surface: kind constants, prepare entry point
      artifact.rs      artifact find/build/register/lookup
      catalog.rs       day catalog over store descriptors
      codec.rs         event-store binary codec
      dbn.rs           DBN decode + normalization
      fetch.rs         Databento acquisition for one market day
  market/
    mod.rs
    day.rs             MarketDay + ES session-boundary resolution
    es_mbo.rs          ES MBO payload types
```

Test layout:

```text
crates/ledger/tests/
  artifact_codec.rs
  es_replay_prepare.rs
  es_catalog.rs
crates/store/tests/store.rs   (extend) update_metadata tests
```

`clock.rs`, `session.rs`, `feed/es_replay/feed.rs`, and
`feed/es_replay/cells.rs` are added by the feed process phase, not here.

## Market Day

`docs/vision.md` naming: a **MarketDay** is one cataloged ES trading day,
spanning prev-day 18:00 ET Globex open to 17:00 ET close.

```rust
// market/day.rs
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord,
         Serialize, Deserialize)]
#[serde(transparent)]
pub struct MarketDay(pub chrono::NaiveDate);   // wire format: "YYYY-MM-DD"

impl MarketDay {
    /// Resolve the ES market day containing this event timestamp.
    pub fn resolve_es(ts_event_ns: u64) -> Result<MarketDay, LedgerError>;

    /// The ES session bounds for this day in UTC nanoseconds:
    /// [prev-day 18:00 ET, this-day 17:00 ET).
    pub fn es_session_bounds_utc(&self) -> Result<(u64, u64), LedgerError>;
}
```

Resolution rule (the inverse of the legacy session bounds):

```text
convert ts_event_ns to America/New_York local time (chrono-tz)
local time >= 18:00             -> market_day = next calendar date
local time <  17:00             -> market_day = that calendar date
17:00 <= local time < 18:00     -> hard error: the daily maintenance halt
                                   belongs to no ES session
```

Provenance, precisely: `es_session_bounds_utc` is the direct port of
legacy `MarketDay::resolve_es`, which mapped a (contract symbol, market
date) to session bounds by constructing local wall times — that
construction is where DST discipline lives (`.single()`, hard error on
ambiguous or nonexistent local times, matching the legacy code). The
timestamp-to-day rule above is new to this spec, derived from the same
boundaries; converting a UTC instant to New York local time is always
unambiguous, so its only error is the maintenance-gap rule — there is no
`.single()` path on the derivation side.

## ES MBO Market Types

```rust
// market/es_mbo.rs
pub type UnixNanos = u64;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct PriceTicks(pub i64);

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum BookSide {
    Bid,
    Ask,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum BookAction {
    Add,
    Modify,
    Cancel,
    Clear,
    Trade,
    Fill,
    None,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EsMboEvent {
    pub ts_event_ns: UnixNanos,
    pub ts_recv_ns: UnixNanos,
    pub sequence: u64,
    pub action: BookAction,
    pub side: Option<BookSide>,
    pub price_ticks: Option<PriceTicks>,
    pub size: u32,
    pub order_id: u64,
    pub flags: u8,
    pub is_last: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct EsMboBatchSpan {
    pub start_idx: usize,
    pub end_idx: usize,     // exclusive
    pub ts_event_ns: UnixNanos,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct EsMboEventStore {
    pub events: Vec<EsMboEvent>,
    pub batches: Vec<EsMboBatchSpan>,
}
```

Price semantics (proven in the legacy pipeline): DBN prices are 1e-9
fixed-precision. One ES tick = 0.25 USD = 250_000_000 fixed units.
`PriceTicks` stores the integer tick count (price / 250_000_000), not a
price. All ordering and batching key on `ts_event_ns`; `ts_recv_ns` is
preserved but never used for ordering.

Feed-batch, cursor, and status types belong to the feed process phase.

## Artifact Ownership

Raw objects are the shared substrate; artifacts are feed-owned.

```text
raw
  stored once, content-addressed, shared by every feed that derives from it
  never encodes feed-specific structure

artifact
  owned by exactly one feed
  kind is namespaced by the feed (ledger.es_mbo_event_store.v1)
  codec is private to the feed, with a feed-specific magic header
  lineage points at the raw object(s) it was derived from
```

Feeds do not share artifact formats by accident: distinct codec magics
guarantee distinct bytes, and therefore distinct object ids. If a future
feed wants another feed's normalized events, it should depend on that
artifact kind explicitly rather than defining a byte-identical format.

## ES Replay Artifact

The legacy pipeline wrote four artifacts (events/batches/trades/book-check).
This phase writes one feed-owned artifact:

```text
kind:      ledger.es_mbo_event_store.v1
role:      artifact
file_name: es-mbo-event-store.v1.bin
lineage:   [<raw object id>]
```

Contents: normalized ES MBO events, exchange batch spans, and source
metadata in descriptor `metadata_json`. No trade indexes, book checks,
execution state, or component outputs — those return behind later feeds,
components, and simulators (see Legacy Reference for the proven designs).

Artifact metadata:

```json
{
  "artifact": "es_mbo_event_store",
  "version": 1,
  "raw_object_id": "sha256-...",
  "market_day": "2026-03-10",
  "event_count": 123,
  "batch_count": 45,
  "first_ts_event_ns": "1773235800000000000",
  "last_ts_event_ns": "1773239400000000000"
}
```

The counts and bounds exist so the catalog — and therefore the Days screen
— can display a day's shape without decoding artifact bytes.

Store lookup (shared by prepare and the catalog):

```text
list role=artifact kind=ledger.es_mbo_event_store.v1
filter lineage == [raw object id]           (exact single-parent match)
filter metadata_json.raw_object_id == raw object id
filter metadata_json.version == 1
use the descriptor if present, else the raw is Unprepared
```

Exact-lineage matching is deliberate: a future artifact kind derived from
several raws would *contain* each raw id in its lineage, and a contains
rule would let it masquerade as the single-raw replay artifact.

If multiple matching descriptors exist, pick the newest valid descriptor by
`updated_at_ns` and report the ambiguity in the prepare summary. This
should not normally happen because the artifact bytes are deterministic for
the same raw DBN.

## Artifact Codec

Fixed little-endian binary codec, colocated with the ES replay feed module.
No `bincode`, no schema system. Wire codes follow the proven legacy codec.

Magic:

```text
LEDGER_ES_MBO_EVENT_STORE_V1
```

Layout:

```text
u32 magic_len
[u8; magic_len] magic
u32 version
u64 event_count
u64 batch_count

events:
  repeated event_count times:
    u64 ts_event_ns
    u64 ts_recv_ns
    u64 sequence
    u8  action        (Add=1 Modify=2 Cancel=3 Clear=4 Trade=5 Fill=6 None=7)
    u8  side          (None=0 Bid=1 Ask=2)
    i64 price_ticks   (i64::MAX encodes None)
    u32 size
    u64 order_id
    u8  flags
    u8  is_last

batches:
  repeated batch_count times:
    u64 start_idx
    u64 end_idx       (exclusive)
    u64 ts_event_ns
```

Decode validation:

```text
magic matches
version matches
all batch spans are in bounds
start_idx <= end_idx
batches are non-overlapping and ordered
no trailing bytes
rebuilt batch spans from the decoded events match the stored spans
```

The last rule follows the legacy `EventStore::validate` principle: stored
indexes are non-authoritative caches that must be reproducible from events.

## DBN Preparation

The module entry point, callable by the CLI now and by remux jobs and the
feed process later:

```rust
pub async fn prepare_es_replay_artifact<S>(
    store: &store::Store<S>,
    raw_object_id: &store::StoreObjectId,
    force: bool,
    progress: Option<tokio::sync::mpsc::UnboundedSender<PrepareProgress>>,
) -> Result<EsReplayArtifact, LedgerError>
where
    S: store::RemoteStore + 'static;

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "stage", rename_all = "snake_case")]
pub enum PrepareProgress {
    Hydrating,
    Decoding { records: u64 },     // emitted every 1_000_000 records
    Encoding { events: u64, batches: u64 },
    Registering,
}
```

`EsReplayArtifact` carries the artifact descriptor, the local path, whether
it was reused, the derived market day, counts, and any warnings, plus a
serializable `EsPrepareSummary` for CLI/JSON output.

Preparation behavior:

```text
1. load the raw descriptor; require role=raw and kind=databento.dbn.zst
   (a wrong id fails clearly here, before any bytes move)
2. find matching artifact descriptor (lookup above)
3. if found and not force: hydrate, decode, validate, and return reused —
   after repairing the raw's market_day backfill if it is missing or
   disagrees with the artifact's (same merge rule as step 11)
   (a corrupt existing artifact is reported and rebuilt, not returned)
4. hydrate raw DBN object
5. decode raw DBN MBO records
6. normalize into EsMboEvent
7. build exchange batch spans from is_last
8. derive market_day from the first event's ts_recv, compute its session
   bounds, and require every event's ts_recv_ns to lie in [start, end) —
   one integer range check per event inside the scan that is already
   happening; any event outside is a hard error, never registered.
   ts_recv (delivery time), not ts_event, is the session clock: the
   opening book snapshot re-adds resting orders with days-old ts_event but
   delivers them in-session (see Market Day Metadata)
9. encode EsMboEventStore artifact, round-trip decode as a self-check
10. register artifact through store with role=artifact
11. backfill market_day onto the raw descriptor metadata
12. return descriptor/path/summary
```

The reuse-path repair in step 3 closes a crash window: registering the
artifact (step 10) and backfilling the raw (step 11) are two separate
writes, and a crash between them must not leave the raw unassigned forever
— the rerun reuses the artifact, so reuse must also finish the backfill.

Prepare is idempotent: re-running reuses the existing artifact unless it is
missing, invalid, or `force` is set. `force` exists for deliberate rebuilds
(vision §3's "rebuild from existing raw data"); because the store is
content-addressed and the codec deterministic, a forced rebuild of a
healthy raw re-registers the same object id — a harmless no-op.

Progress is optional and fire-and-forget: send errors are ignored (a
dropped receiver just means nobody is watching). The decode and encode
steps are synchronous CPU work; run them under `tokio::task::spawn_blocking`
so a shared runtime (the remux adapter) stays responsive —
`UnboundedSender::send` is sync and safe from inside the blocking closure.

Decoding (per the proven legacy implementation):

```text
DbnDecoder::from_zstd_file(path)      synchronous; handles zstd internally
while decode_record::<MboMsg>()       until None
no Metadata read                      records decoded blindly
no instrument filtering               files contain exactly one contract by
                                      acquisition contract (single RawSymbol
                                      download); a record that violates ES
                                      tick alignment fails fast anyway
```

Normalization rules (port verbatim from legacy `normalize_mbo`):

```text
action byte:  A->Add  M->Modify  C->Cancel  R->Clear  T->Trade  F->Fill  N->None
              any other byte is a hard error
side byte:    B->Bid  A->Ask  N->None
              any other byte is a hard error
price:        UNDEF_PRICE (== i64::MAX) -> None
              else require price % ES_TICK_SIZE_FIXED_PRICE == 0,
              hard error "ES price {} is not aligned to 0.25 tick"
              price_ticks = price / ES_TICK_SIZE_FIXED_PRICE
timestamps:   ts_event_ns = msg.hd.ts_event, ts_recv_ns = msg.ts_recv
flags:        raw u8 preserved; is_last = flags.is_last() (F_LAST = 0x80)
```

Constants:

```rust
pub const ES_TICK_SIZE_FIXED_PRICE: i64 = 250_000_000;
```

Batch building (port verbatim from legacy `build_batches`):

```text
open a batch at the current start index
close the batch on each event with is_last == true
end_idx is exclusive; span timestamp is the closing event's ts_event_ns
if EOF leaves a partial batch, close it stamped with the last event's ts
```

Decoding a full day loads the entire event vector into memory; that matched
the legacy behavior and is acceptable here. Artifact indexing/mmap is a
future extension of the feed phase.

## Market Day Metadata

The store has no calendar. `market_day` is an app-layer claim in descriptor
`metadata_json`, written by the feed layer. Two writers, two trust levels:

```text
fetch/import (hint)
  stamps the requested market day at acquisition time so the catalog can
  place the raw before it is ever prepared

prepare (authoritative)
  derives the market day from the first event's ts_recv and corrects the hint
```

Session membership keys on **ts_recv (delivery time), not ts_event**. A
Databento session request is served with an opening book snapshot: a
`Clear` followed by an `Add` for every order already resting in the book,
each carrying its *original* ts_event — which for a deep or GTC order can
be days old. Those re-adds are the session's opening book state and must
be kept, but their ts_event legitimately falls before the session. Every
record, snapshot included, is *delivered* within the session, so ts_recv
is the true in-session clock. (Observed in the three real raws: 3.4k-5.6k
snapshot re-adds per file with ts_event back to a resting order days
earlier, every one with ts_recv inside the session and zero events
delivered outside it.)

Every event's ts_recv must lie inside the session bounds of the first
event's market_day; prepare hard-errors otherwise. Checking only first and
last would let a corrupt or unsorted file hide an out-of-session event
mid-file, and the full scan is happening anyway, so the strong invariant
costs one integer range check per event. A file whose *delivery* times
span sessions violates the acquisition contract (one (day, symbol) per
download), and warning-and-continue would mark a multi-day artifact as
replay-ready for a single day — exactly the kind of quiet corruption the
feed clock cannot detect later.

Prepare writes market_day into the artifact metadata and backfills the raw
descriptor via the new store method:

```rust
impl<S: RemoteStore + 'static> Store<S> {
    /// Replace metadata_json, bump updated_at_ns, and re-mirror the
    /// descriptor to R2.
    pub async fn update_metadata(
        &self,
        id: &StoreObjectId,
        metadata_json: serde_json::Value,
    ) -> Result<StoreObjectDescriptor>;
}
```

Backfill merges: prepare reads the raw's existing metadata_json, sets
`market_day` (and leaves the acquisition fields — dataset, provider,
schema, source_symbol — untouched), and calls `update_metadata`.

The three existing raws carry no market_day; running prepare across them is
the backfill.

## Days Catalog

The catalog answers vision §3's first question — *what days do I have?* —
from descriptors alone. It never decodes bytes; prepare owns byte-level
truth.

```rust
// feed/es_replay/catalog.rs
pub fn es_day_catalog<S>(
    store: &store::Store<S>,
) -> Result<EsDayCatalog, LedgerError>
where
    S: store::RemoteStore + 'static;

#[derive(Debug, Clone, Serialize)]
pub struct EsDayCatalog {
    pub days: Vec<EsDayEntry>,        // newest market day first
    pub unassigned: Vec<EsRawStatus>, // raws with no market_day stamp yet
}

#[derive(Debug, Clone, Serialize)]
pub struct EsDayEntry {
    pub market_day: MarketDay,
    pub raws: Vec<EsRawStatus>,       // usually one; multiple symbols possible
}

#[derive(Debug, Clone, Serialize)]
pub struct EsRawStatus {
    pub raw: store::StoreObjectDescriptor,
    pub artifact: Option<store::StoreObjectDescriptor>,
    pub state: EsRawState,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum EsRawState {
    Unprepared,   // raw exists, no valid artifact descriptor
    Prepared,     // matching artifact descriptor exists
}
```

Construction (all `Store` reads are sync, so the catalog is a sync fn):

```text
raws      = list_objects(role=Raw,      kind=databento.dbn.zst)
artifacts = list_objects(role=Artifact, kind=ledger.es_mbo_event_store.v1)
join artifacts to raws by prepare's exact-lineage lookup rule
group raws by metadata_json.market_day; missing/unparsable -> unassigned
sort days newest first; raws within a day by source_symbol then id
```

An unassigned raw is not an error — it is a raw waiting for its first
prepare (which stamps the day). The bucket exists so nothing is invisible:
every raw in the store appears exactly once in the catalog.

`Prepared` here means "a valid-looking artifact descriptor exists". The
deeper ladder from vision §4 (Ready to Train, Ready with Warnings) arrives
with the trust-report layer; the catalog's state enum is designed to grow
those variants without reshaping.

## Databento Fetch

The "load" half: pull one market day of ES MBO from Databento into the
store as a raw. Ports the legacy acquisition contract directly. This
section is separable — nothing else in the spec depends on it — but it is
what makes the Days surface complete ("pull more days" instead of
"days I happened to import").

```rust
// feed/es_replay/fetch.rs
pub async fn fetch_es_raw<S>(
    store: &store::Store<S>,
    day: MarketDay,
    symbol: &str,                 // explicit contract, e.g. "ESH6"
    dataset: &str,                // default "GLBX.MDP3"
    staging_dir: &std::path::Path,
    force: bool,
    progress: Option<tokio::sync::mpsc::UnboundedSender<FetchProgress>>,
) -> Result<EsFetchSummary, LedgerError>
where
    S: store::RemoteStore + 'static;

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "stage", rename_all = "snake_case")]
pub enum FetchProgress {
    Requesting,
    Downloading,
    Registering,
}
```

Behavior:

```text
1. duplicate check: if a raw (role=raw, kind=databento.dbn.zst) with
   matching metadata (market_day, source_symbol, dataset, schema=mbo)
   already exists, return it with fetched=false — unless force
   (paid API; never re-download by accident). The key mirrors the
   metadata written in step 5, so detection matches the contract.
2. range = day.es_session_bounds_utc()
3. Databento historical client (key from DATABENTO_API_KEY, same .env
   convention as the R2 credentials; never printed):
     dataset GLBX.MDP3, Schema::Mbo, SType::RawSymbol, symbols=[symbol]
4. download into staging_dir: <file>.part, then atomic rename (legacy
   contract). Store keeps its own tmp layout private, so staging is the
   caller's: the CLI and the adapter both already hold the data dir and
   pass <data_dir>/tmp/fetch/<symbol>/<day>/. Leftover .part files are
   disposable garbage from interrupted downloads.
5. register_file into the store — it hashes, uploads, and copies the file
   into the store layout from any source path:
     role=raw, kind=databento.dbn.zst
     metadata: { "provider": "databento", "dataset": ...,
                 "schema": "mbo", "source_symbol": ...,
                 "market_day": "<day>" }        <- hint; prepare corrects
6. delete the staged file; return summary (raw id, day, symbol, size,
   fetched|reused)
```

The store is content-addressed, so a forced re-fetch of identical bytes
converges on the same object id. Fetch does not auto-prepare; the CLI and
the Days screen chain the two explicitly.

Contract selection stays explicit (`--symbol`): near expiry two contracts
carry volume, and the roll is a trading decision, not something the data
layer should guess.

## Pass 1 — CLI Surface

Extend `crates/cli` with an `es` command group, following the existing
group idiom (a `Command::Es(EsCommand)` variant wrapping an `EsSubcommand`
enum, dispatched like `run_store_command`):

```text
ledger es days
ledger es prepare (--raw-id <store-object-id> | --day <YYYY-MM-DD> | --all)
                  [--force]
ledger es fetch   --day <YYYY-MM-DD> --symbol <SYM>
                  [--dataset GLBX.MDP3] [--force]
```

```text
es days      print the full EsDayCatalog as JSON (print_json idiom)

es prepare   --raw-id  prepare one raw
             --day     prepare every raw the catalog maps to that day
                       (errors if the day is unknown; unassigned raws are
                        reachable only by --raw-id until first prepare
                        stamps them)
             --all     every role=raw kind=databento.dbn.zst object, in id
                       order, continuing past per-raw failures and
                       reporting them
             progress -> stderr lines; JSON summaries -> stdout

es fetch     fetch one (day, symbol); progress -> stderr; summary -> stdout
```

Per-raw prepare JSON summary:

```json
{
  "raw_object_id": "sha256-...",
  "artifact_object_id": "sha256-...",
  "artifact_reused": false,
  "market_day": "2026-03-10",
  "event_count": 12345678,
  "batch_count": 2345678,
  "first_ts_event_ns": "1773235800000000000",
  "last_ts_event_ns": "1773321000000000000",
  "warnings": []
}
```

`--all` and `--day` print an aggregate after attempting every raw, and the
process exits non-zero if anything failed:

```json
{
  "ok": [ { "...": "per-raw summaries as above" } ],
  "failed": [ { "raw_object_id": "sha256-...", "error": "..." } ]
}
```

### Pass 1 Acceptance Run

The pass is not done until the real data passes:

```text
cargo run -p ledger-cli -- es prepare --all

expected: ok holds three summaries, one per raw (2026-03-10, 2026-03-11,
2026-03-12), each with a registered artifact, plausible event/batch counts,
a stamped market_day, and no warnings; failed is empty and the exit code is
zero; store validate --verify-remote stays all-valid; re-running reports
artifact_reused=true for all three

cargo run -p ledger-cli -- es days

expected: three day entries, newest first, all Prepared, with counts and
sizes visible from descriptors alone; unassigned is empty
```

Fetch acceptance is run on explicit command only (paid API): fetch one
adjacent day (e.g. `--day 2026-03-13 --symbol ESH6`), prepare it, and see a
fourth Prepared day in `es days`.

Workspace validation: `cargo fmt --all` and `cargo test --workspace`.

## Pass 2 — Jobs in the Remux Adapter

Prepare and fetch take seconds to minutes; from the phone they must be
fire-and-forget. `crates/remux` already has the exact idiom in
`hydrate.rs`: the RPC returns immediately, a `tokio::spawn`ed task does the
work, a notification announces completion, and an in-flight set prevents
duplicates. Jobs generalize that pattern; they do not replace hydrate.

New module `crates/remux/src/jobs.rs`:

```rust
pub struct JobRegistry {
    inner: Arc<Mutex<JobTable>>,   // records + monotonic id counter
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct JobRecord {
    pub id: String,                // "job-<n>"
    pub kind: String,              // "es.prepare" | "es.fetch"
    pub subject: String,           // raw id, or "<day> <symbol>" for fetch
    pub state: JobState,           // running { stage, records? }
                                   //   | completed { summary }
                                   //   | failed { error }
    pub started_at_ns: u64,
    pub finished_at_ns: Option<u64>,
}
```

Registry rules:

```text
single-flight   starting a job whose (kind, subject) is already running
                returns the existing job id (alreadyRunning: true), not an
                error — reattach, don't reject
retention       terminal jobs are kept newest-first, capped at 50
crash safety    jobs die with the adapter process; that is acceptable
                because prepare and fetch are idempotent against a
                content-addressed store — rerunning a killed job converges
```

RPC methods (same const + `LedgerRemux::handle` match idiom, camelCase
DTOs, `*_ns` stringified):

```text
remux/ledger/es/days       {}                        -> { days, unassigned }
remux/ledger/es/prepare    { rawId, force? }         -> { jobId, alreadyRunning? }
remux/ledger/es/fetch      { marketDay, symbol }     -> { jobId, alreadyRunning? }
remux/ledger/jobs/list     {}                        -> { jobs: [JobRecord] }
                                                        running first, then
                                                        terminal newest-first
```

Notifications (server -> client, like `store/hydrated`):

```text
remux/ledger/jobs/progress   { jobId, kind, subject, stage, records? }
remux/ledger/jobs/finished   { jobId, ok, summary?, error? }
```

Wiring: the job task clones the store handle (existing idiom), opens an
unbounded progress channel into the module function, and forwards each
`PrepareProgress`/`FetchProgress` two ways: written into the running
record's state (stage + record count) and pushed as a `jobs/progress`
notification (the module already throttles decode progress to 1M-record
steps). A client that reattaches through `jobs/list` therefore sees the
live stage, not a bare "running". On completion the task updates the
registry and emits `jobs/finished`. There is deliberately no UI polling:
progress is pushed, and `jobs/list` exists for reattachment after
suspend/resume, mirroring how lens already refreshes on `hydrated` events
rather than polling.

`crates/remux` gains the `ledger = { path = "../ledger" }` dependency here,
not in pass 1. `LedgerRemux` also grows a `fetch_staging_root: PathBuf`
(precomputed `<data_dir>/tmp/fetch` in `main.rs`, which holds the data dir
before building the store) — today the struct carries only the store,
hydrate jobs, and output sender, so fetch jobs would otherwise have no
staging path to hand `fetch_es_raw`.

## Pass 2 — Lens Days Screen

Lens today renders a single feature (`DataCenter`) from `App.tsx`; there is
no router. This pass introduces the two-screen shape the app layer has been
pointing at: content above, menus and actions in the bottom bar.

```text
lens/src/features/days/
  days.tsx        screen component
  api.ts          requestIpc calls + notification subscriptions
  types.ts        view-model types mirroring the DTOs
  day-list.tsx    presentational day/raw rows

App.tsx           useState<"days" | "objects">("days")
                  renders <Days /> or <DataCenter />; Days is the default

Data Center       retitled "Objects" — same feature folder, same behavior;
                  it is the low-level object view behind the day-level view
```

Bottom bar (viewer-kit `ActionBar`; screen menu via `ActionMenu`, which the
kit ships but lens has not used yet):

```text
left    ActionMenu "Screen": Days | Objects   (both screens carry it)
        ActionButton Refresh (busy while loading)
right   ActionButton "Fetch day..." (Days screen; opens a small dialog with
        day + symbol inputs, DeleteObjectDialog idiom)
        existing host actions (Close tab)
status  summary, mirrored to the host tab via updateHostTab
        (e.g. "3 days - 1 job running")
```

Days screen behavior:

```text
load      on mount: es/days + jobs/list in one Promise.all (refresh idiom)
rows      one card per day: market day, per-raw state chip
          (Prepared/Unprepared), source symbol, sizes from size_bytes,
          event/batch counts from artifact metadata
          unassigned raws render in their own section, same row shape
actions   per-row Prepare / Re-prepare (QuickAction idiom) -> es/prepare;
          the row keys into the running-jobs map the way hydratingIds
          works today
progress  jobs/progress notifications update the row in place: stage label
          plus decode record count; Loader2 spinner idiom
finish    jobs/finished -> toast (sonner) + refresh, mirroring the
          hydrated-event handler
errors    a failed job keeps its error on the row until the next refresh,
          plus toast.error
resume    on subscribeIpcResume / subscribeHostResume, re-run load —
          notifications are not replayed after suspension, so state is
          always re-derived from es/days + jobs/list
```

The Days screen is deliberately the future session entry point ("open
validated day as a Session" — vision §3). No such action ships in this
phase; the row layout should just leave room for it.

### Pass 2 Acceptance Run

From lens on the phone, after pass 1's acceptance:

```text
Days shows the three prepared days with counts and sizes
delete one artifact in Objects -> its day flips to Unprepared
tap Prepare on that day -> progress renders on the row -> day returns to
  Prepared with the same artifact id (deterministic bytes)
kill/suspend lens mid-prepare, reopen -> the running job reattaches via
  jobs/list and finishes normally
optional (paid): Fetch day... pulls 2026-03-13, prepare it, four days listed
```

## Error Types

```rust
#[derive(Debug, thiserror::Error)]
pub enum LedgerError {
    #[error(transparent)]
    Store(#[from] anyhow::Error),

    #[error("not an ES raw object: {0}")]
    InvalidRaw(String),

    #[error("invalid DBN record: {0}")]
    InvalidDbnRecord(String),

    #[error("invalid artifact: {0}")]
    InvalidArtifact(String),

    #[error("market day derivation failed: {0}")]
    MarketDay(String),

    #[error("databento fetch failed: {0}")]
    Fetch(String),
}
```

The feed process phase extends this enum (clock and runtime variants); it
does not replace it. In the adapter, `LedgerError` maps to the existing
`RpcError::domain` convention; job failures carry the same string in
`jobs/finished.error`.

## Tests

Codec and normalization:

```text
artifact codec round trips synthetic EsMboEventStore
artifact codec rejects wrong magic
artifact codec rejects out-of-bounds batch spans
artifact codec rejects spans that do not match rebuilt batches
batch builder closes trailing partial batch
DBN normalization maps action/side/undefined price correctly where testable
DBN normalization rejects misaligned ES prices
market-day derivation: session start (18:00 ET) resolves to the next date,
  end - 1ns resolves to that date, end (17:00 ET) and the maintenance gap
  error; holds across DST transitions (UTC-to-local stays unambiguous —
  the gap is the only derivation error)
es_session_bounds_utc errors on ambiguous/nonexistent local boundary times
es_session_bounds_utc round-trips with resolve_es at both edges
```

Prepare (against a TestRemote-backed store, synthetic DBN fixtures):

```text
prepare rejects an object that is not role=raw kind=databento.dbn.zst
prepare builds, registers, and returns a new artifact with lineage to the raw
prepare is idempotent: second run reuses the artifact (artifact_reused=true)
prepare --force rebuilds and converges on the same object id
prepare rejects a corrupt existing artifact and rebuilds it
prepare stamps market_day on the artifact metadata
prepare backfills market_day onto the raw descriptor without clobbering
  existing acquisition metadata
reuse path repairs a missing or mismatched market_day on the raw
prepare emits progress stages in order on the channel
a raw with any event whose ts_recv is outside the first event's session
  bounds fails prepare with no artifact registered (including an
  out-of-session event hidden mid-file and a maintenance-gap delivery)
a snapshot re-add — in-session ts_recv but a days-old ts_event — is kept,
  and prepare derives the day from ts_recv and succeeds (no event dropped)
```

Catalog:

```text
groups raws by market_day, newest day first
joins artifacts by lineage + kind + version; state flips to Prepared
raws without market_day land in unassigned, never disappear
multiple raws on one day are all listed
```

Store:

```text
update_metadata replaces metadata_json, bumps updated_at_ns, re-mirrors
update_metadata on a missing id errors
update_metadata preserves remote/local entries untouched
```

Jobs (pass 2, registry unit tests):

```text
single-flight: same (kind, subject) returns the running job id
distinct subjects run concurrently
terminal jobs retained newest-first, capped
records transition running -> completed/failed exactly once
progress events update the running record's stage (visible via jobs/list)
```

Fetch's network path is not unit-tested; range computation and the
duplicate-check/force logic (full metadata key) are.

## Success Criteria

Pass 1:

```text
crates/ledger exists and builds with no cache or runtime dependency
codec, normalization, batching, and market-day rules match the legacy
  reference exactly where this spec says "port verbatim"
prepare is idempotent, force-rebuildable, and validated end to end against
  all three real raws (acceptance run)
market_day is stamped on artifacts and backfilled onto raws
Store::update_metadata exists, re-mirrors, and is tested
es days / es prepare / es fetch exist with JSON output and stderr progress
cargo test --workspace passes
```

Pass 2:

```text
job registry with single-flight, retention, and push notifications
es/days, es/prepare, es/fetch, jobs/list methods live in ledger-remux
Days is lens's default screen; Objects remains available via the
  bottom-bar screen menu
a day can be fetched (optional), prepared, watched, and trusted end to end
  from the phone with no CLI involvement
suspend/resume mid-job recovers cleanly from es/days + jobs/list
```

## Legacy Reference

The pre-rebuild pipeline (deleted in `d1931b4`) is the proven reference for
DBN handling through bars. All paths below are readable at commit
`d1931b4^` via `git show 'd1931b4^:<path>'`. Port rules from it; do not
invent behavior it deliberately lacked.

Key sources:

```text
d1931b4^:crates/ingest/src/preprocess.rs        decode + normalize_mbo
d1931b4^:crates/ingest/src/databento_downloader.rs  acquisition pattern
d1931b4^:crates/ingest/src/book_check.rs        book-check report
d1931b4^:crates/domain/src/event.rs             MboEvent, build_batches,
                                                build_trade_index, tick math
d1931b4^:crates/domain/src/artifact_codec.rs    binary codecs + wire codes
d1931b4^:crates/domain/src/market_day.rs        resolve_es, DST handling
d1931b4^:crates/book/src/order_book.rs          L3 book, warnings, checksum
d1931b4^:crates/ledger/src/projection/base/bars.rs  time bars
```

Facts this spec ports directly: the normalization table, hard-error policy
for unknown action/side bytes and misaligned prices,
`ES_TICK_SIZE_FIXED_PRICE = 250_000_000` (DBN prices are 1e-9 fixed;
`UNDEF_PRICE == i64::MAX`), `is_last` from the `F_LAST` (0x80) flag,
half-open batch spans stamped with the closing event's timestamp, trailing
partial batch handling, little-endian codecs with action codes 1..7 and
side codes 0..2, index-rebuild validation, `ts_event_ns` as the only
ordering key, the ES session boundaries (prev-day 18:00 ET -> 17:00 ET,
America/New_York, DST resolved with `.single()` and hard errors), and the
acquisition contract (GLBX.MDP3, Schema::Mbo, SType::RawSymbol, one
contract symbol per request, atomic .part download then rename — now
ported into `fetch.rs`).

Facts preserved for later phases:

```text
trades:      Trade and Fill actions both count as prints; priceless prints
             dropped; aggressor side taken verbatim from the event; no
             dedup, no inference; zero-size prints kept
book:        BTreeMap levels + IndexMap FIFO within level; modify = remove
             and reinsert (loses queue priority, defaults side/price from
             the resting order); modify-of-unknown becomes add + warning;
             oversized cancel removes + warns; Clear wipes book and stamps
             an empty BBO; Trade/Fill never mutate the book
warnings:    MissingSide, MissingPrice, DuplicateOrder, UnknownOrder,
             OversizedCancel (no crossed/locked detection existed)
book-check:  replay all batches, report event/batch/trade/bbo-change/warning
             counts + terminal SHA-256 book checksum (side, price, size,
             then FIFO order_id+size per level)
bars:        time bars only, epoch-anchored windows
             (window_start = ts / window_ns * window_ns), open/high/low/
             close/volume/trade_count, finalize on first trade of a later
             window, out-of-order trade timestamps error; RTH bounds were
             computed but never used to filter prints
gotchas:     no instrument filtering existed (single-symbol files by
             acquisition contract); no Metadata read; spread instruments
             never handled; sequence monotonicity never validated in the
             book path
```
