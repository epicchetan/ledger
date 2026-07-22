# Tick Bars — Implementation Spec

Status: implemented; native device feel-check and real-data profiling pending

Date: 2026-07-22

Baseline: `4070c0c` (`Implement zero-origin projection rebuilds`)

## 1. Purpose

Extend the active Ledger bars projection from time-only aggregation to the two
bar bases Lens actively needs:

```text
time bars:  bars:1m, bars:5m, bars:15m, bars:1h
tick bars:  bars:100t, bars:500t, bars:1000t, bars:2000t
```

Tick bars must use the projection graph, cache ownership, zero-origin rebuild,
bounded Runtime work and cadence-bounded delivery contracts already in place.
They do not introduce another replay runtime, cache, delivery path, history
artifact, checkpoint or UI loading lifecycle.

## 2. Fixed Semantics

1. One tick means one canonical `TradePrint`, regardless of its `size`.
2. A completed `bars:Nt` bar contains exactly `N` canonical prints.
3. Print size contributes to `volume`, `buy_volume` and `sell_volume` exactly
   as it does for time bars.
4. A non-empty partial group is the live bar. At an exact `N`-print boundary
   the completed bar is published and the live cell is empty until another
   print arrives.
5. Tick groups start at canonical print zero in every replay epoch. Chunk,
   cache commit and delivery boundaries cannot change grouping.
6. `interval_start_ns` is the projection-defined bar time anchor: aligned
   bucket start for time bars and the first included print timestamp for tick
   bars.
7. Canonical print order is authoritative. Existing OHLC, timestamp and
   aggressor-volume folding behavior remains shared by both bases.
8. `bars:<positive integer>t` is the canonical public grammar. Zero, missing,
   signed, fractional and unknown units are rejected.
9. The bars delivery payload remains kind `bars`, schema version 1. Tick bars
   have the same payload and base/head contract as time bars.
10. Lens initially exposes exactly 100, 500, 1000 and 2000 tick choices. The
    server parser remains parameterized for any positive tick count.

## 3. Projection Model

`BarsParams` becomes a typed aggregation basis:

```rust
BarsBasis::Time { interval_ns }
BarsBasis::Ticks { prints_per_bar }
```

`ProjectionSpec::Bars`, `ProjectionNodeSpec::Bars`, bars cache cells, public
outputs and delivery sources remain one projection family. The canonical spec
continues to determine component identity and cache keys:

```text
projection.bars.1m.*
projection.bars.500t.*
```

Both bases depend on the one shared canonical-trade-print node. A graph that
later contains several time/tick specs computes canonical prints once and each
distinct bars spec once, independent of receiver count.

## 4. Runtime and Cache Behavior

`BarsTask` retains ownership of:

- epoch detection and reset;
- bounded reads of at most 4,096 canonical prints per task step;
- completed array/live value/status commits;
- projection revisions and source lineage; and
- incremental versus rebuild scheduling.

Only the fold completion policy is parameterized.

Time policy:

```text
same aligned bucket -> update live
later aligned bucket -> complete old live, start new live
```

Tick policy:

```text
start/update live -> if trade_count == prints_per_bar, complete and clear live
```

Changing time to tick bars or tick to time bars uses the existing session
`set_projections` rebuild operation. It preserves clock target, mode and speed,
starts a new epoch from source zero, clears/rebuilds canonical prints, installs
the requested bars node before feed release and lets ordinary Runtime work
converge at full speed.

## 5. Delivery and Reload

`BarsDeliverySource` requires no tick-specific branch. Completed bars remain an
append-only array within an epoch; the current partial group remains the live
value; status and delivery positions retain the same meaning.

Delivery remains presentation-bounded while compute runs independently. Epoch
change produces an authoritative snapshot, then valid appends. There is no
projection-compute spinner or final-only frame.

Attach returns the server-selected tick spec just as it returns a time spec.
Lens creates the matching empty accumulator, subscribes with `have: null`,
accepts the authoritative snapshot and resumes the same server clock/mode/speed.

## 6. Lens Menu

The current interval action becomes a two-column menu immediately left of
playback speed:

```text
Bars          Ticks
1m            100
5m            500
15m           1000
1h            2000
```

Exactly one item is checked across both columns. The trigger uses the canonical
short label (`1m`, `500t`). The two option columns are accessible groups named
`Time bars` and `Tick bars`; the visible tick labels remain `100`, `500`,
`1000` and `2000` beneath the `Ticks` heading.

Selection continues to send an exact singleton public projection set through
the existing latest-choice queue. It does not close/open the session, navigate,
change playback state or create another client lifecycle.

## 7. Chart Coordinate

Tick bars are ordered by canonical-print group, but several groups may start
within one wall-clock second. Lens must not truncate every tick-bar anchor to
an integer second and hand duplicate time keys to Lightweight Charts.

The bars layer derives a stable display-only timestamp in completed-array
order:

1. Convert the exact nanosecond anchor to fractional seconds.
2. If it is not greater than the prior displayed bar time, advance by a small
   deterministic epsilon.
3. Derive the live bar from the same prior completed-bar coordinate so its key
   remains stable when it becomes completed.
4. Continue formatting wall-clock labels from the resulting timestamp at
   second precision.

Server timestamps remain exact and unmodified. The display disambiguation is
only an adapter requirement for the chart library's unique ordered time keys.

## 8. Scope

In scope:

- generalized time/tick `BarsParams`;
- `bars:<n>t` parse/canonical/component identity;
- tick aggregation in bounded normal bars work;
- time/tick graph coexistence and mutable replacement;
- unchanged bars:v1 delivery and Remux serialization;
- two-column Lens menu;
- collision-safe chart coordinates;
- reload and zero-origin correctness tests; and
- focused real-day profiling when an artifact is available.

Out of scope:

- volume, range, imbalance or dollar bars;
- custom Lens values;
- projection Store artifacts or checkpoints;
- chart history windowing;
- multiple chart tabs/panels;
- receiver-owned projection installation/reference counting; and
- a second projection runtime or catch-up mode.

## 9. Test Matrix

Ledger:

- parse and canonicalize supported tick specs;
- reject zero and malformed tick specs;
- time-equivalent specs still canonicalize/deduplicate;
- time and tick nodes share one canonical dependency;
- 99/100/101 prints produce the expected completed/live state for 100t;
- completed tick OHLCV/aggressor volume is exact;
- print size does not affect tick count;
- non-print feed events do not affect tick grouping;
- chunk boundaries do not affect tick grouping;
- backward/forward/same-position epochs reproduce a fresh tick fold;
- time -> tick -> time replacement reuses keys safely and preserves target;
- delivery emits snapshot/append frames for tick specs without a new schema.

Remux:

- open and set accept tick specs;
- attach/status return the current canonical tick spec;
- projection subscribe, ack, demand, resync and unsubscribe work unchanged;
- changing tick choice preserves session identity and clock state.

Lens:

- tick specs create ordinary bars accumulators;
- two-column options dispatch exact canonical specs;
- trigger/heading labels distinguish time and tick choices;
- tick anchors in the same second map to strictly increasing chart keys;
- live/completed coordinate identity is stable;
- reload adopts the server tick spec; and
- interval changes show no compute loading state.

## 10. Acceptance

The phase is complete when supported tick choices can be selected in Lens,
rebuild causally from source zero at the current server time, render exact
ordered OHLC bars through ordinary cadence delivery, survive reload with
playback state intact, and pass the complete Rust/Lens validation suites.

## 11. Implementation Record

Implemented on 2026-07-22 and committed after review.

### 11.1 Ledger

- Generalized `BarsParams` with typed `Time` and `Ticks` bases while retaining
  one `ProjectionSpec::Bars` family, cache shape and delivery source.
- Added `bars:<n>t` parsing/canonicalization and spec-derived task/cache
  identity.
- Added count-based completion to the existing bounded `BarsTask`; one tick is
  one canonical print and print size remains volume only.
- Kept canonical prints as the one shared dependency for simultaneous time and
  tick bars.
- Proved exact 99/100/101 boundaries, exact-boundary empty live state, OHLCV
  and aggressor volume, non-print exclusion, chunk-independent grouping,
  shared dependency installation and time -> tick -> time source replacement.

### 11.2 Remux

- No production transport or DTO branch was needed: tick bars use the existing
  bars:v1 frame, status and base/head contracts.
- Extended the mutable-session integration proof to replace a time projection
  with `bars:2t`, preserve session id/paused mode/5x speed through attach, and
  deliver completed/live tick bars through an ordinary projection snapshot.

### 11.3 Lens

- Added the two-column Bars/Ticks action menu with the surfaced 1m/5m/15m/1h
  and 100t/500t/1000t/2000t choices.
- Made value-bearing action-menu triggers content-sized with the existing
  39px touch target as a minimum, so labels such as `1000t` and `100×` expand
  instead of being compressed into the icon-button width.
- Centralized projection labels/spec metadata so the action trigger and chart
  title use the same canonical short label.
- Reused the bars:v1 parser and accumulator for tick specs.
- Added microsecond-preserving, collision-disambiguated chart time keys. A live
  tick bar recomputed against the same completed tail retains the exact display
  key it will use when completed.

### 11.4 Validation completed

```text
Rust workspace tests: 247 passed
Rust rustfmt:          clean
Rust clippy:           clean with -D warnings
Lens tests:            23 passed across 7 files
Lens typecheck:        passed
Lens lint:             passed
Lens production build: passed
```

The production build retains the existing Vite advisory that the single JS
chunk is slightly above 500 kB; no new build error was introduced.

### 11.5 Validation still pending

- Run the two-column selector and time/tick/time replacement flow on the native
  viewer while paused and at 5x.
- Reload while a tick projection is rebuilding and confirm attach restores the
  same tick choice, session, clock mode/speed and partial chart.
- Profile a prepared real ES day, especially `bars:100t`: canonical print/bar
  counts, rebuild duration, initial snapshot bytes, Lens parse/`setData` time
  and retained memory.
