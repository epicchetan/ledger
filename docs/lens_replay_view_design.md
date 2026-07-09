# Lens Replay View — Design

Lens grows a second screen: a replay view that opens a ledger session for a
selected market day and renders its projections live — a bar chart driven by
the `barsFrame` stream, playback controls driven by the clock stream, and
feed progress driven by the cursor stream. This is the first consumer of the
session transport (`docs/ledger_session_transport_implementation_spec.md`);
the wire protocol is defined there and referenced here, not repeated.

Guiding split, stated once and leaned on throughout: **projection
composition is a server concern; view mapping is a client concern.** If a
future projection depends on another, that dependency lives in ledger's
build graph — lens never models it. Lens sees only "I asked for these specs,
each streams frames of some kind" and answers exactly one question per spec:
*what does this kind draw on the chart, if anything?* Headless projections
are kinds with no layer entry. This is the whole extensibility story; there
is no plugin system, no dependency UI, no layout manager.

## Scope

In scope (two phases, described at the end):

- A **Replay** action on a selected market day, next to the day's existing
  actions, gated on readiness (`state === "ready"` — the readiness module
  already documents ready as "the only state replay can hit play from").
- A replay page that owns one session: open on entry, close on exit.
- Playback controls (play / pause / speed / seek-scrubber) and feed
  progress, wired to the clock and feed streams.
- One main chart; bar projections render on it as the base candlestick
  layer, with tabs picking the active spec (`bars:1s` / `bars:1m` to
  start).
- The client-side session layer: typed API, notification demux, and the
  frame accumulator (the reusable piece future projections inherit).
- One small transport addition: session time bounds in the open result
  (see below).

Out of scope, deliberately:

- Multiple concurrent sessions or comparison views (the transport is
  single-session by design; the UI matches).
- Dependency-aware or configurable projection UI, saved layouts, workspace
  persistence.
- Any new projection kinds. The registry ships with one entry: `bars`.
- Frame batching / backpressure work. The watcher already coalesces; if the
  chart ever stutters during a large seek we revisit then, with data.

## Entry point and navigation

The day action bar (`features/days/day-action-bar.tsx`) gains a **Replay**
button in the day-scoped control group, left of the existing actions,
enabled when the shown day's `state === "ready"`. It acts on the day's
`primary` raw (the same raw the day card headlines and Install acts on) and
navigates with `{ rawId, marketDay, symbol }`.

Lens has no router and two screens don't justify one. `App.tsx` holds a
view state:

```ts
type LensView =
  | { kind: "days" }
  | { kind: "replay"; rawId: string; marketDay: string; symbol: string }
```

`Days` receives an `onReplay(day)` callback; the replay page receives the
params and an `onExit()` callback. If a third screen ever appears, this
becomes a router mechanically.

Session lifecycle binds to the page: open on mount, close on unmount/exit.
The server's replace-on-open semantics backstop leaks — a session that was
never closed dies when the next one opens — and the `closed` notification
(reason `replaced`) tells a stale page it lost the session; it shows a
terminal "session ended" state rather than silently going dark.

## Transport

Nothing new. Lens already speaks JSON-RPC to the `ledger-remux` extension
through the remux host bridge: `requestIpc` for request/response,
`subscribeIpcEvents` for notifications (the jobs feature uses both today).
The session methods and streams ride the same pipe:

| Client need            | Wire                                              |
| ---------------------- | ------------------------------------------------- |
| open / close / status  | `remux/ledger/session/{open,close,status}`        |
| controls               | `remux/ledger/session/{play,pause,speed,seek}`    |
| bars backfill (pull)   | `remux/ledger/session/bars`                       |
| clock stream           | `remux/ledger/session/clock` notification         |
| cursor stream          | `remux/ledger/session/feed` notification          |
| bar frames stream      | `remux/ledger/session/barsFrame` notification     |
| session ended          | `remux/ledger/session/closed` notification        |

Subscription ordering makes hydration free: the page subscribes to events
*before* calling `open`, filters by `sessionId` from the open result, and
every watcher broadcasts its initial state on spawn — so the first clock,
cursor, and frame arrive without a status pull and without a gap. `status`
remains available as a debugging/recovery pull but the page doesn't need it
on the happy path.

### One transport addition: session bounds

The seek scrubber needs a fixed time domain; deriving it from observed data
would make the scrubber's range shift as bars arrive, and duplicating the
ES session calendar (18:00 ET → 17:00 ET, DST-sensitive) in TypeScript is
exactly the kind of logic that should exist once. `MarketDay::
es_session_bounds_utc()` already computes it server-side.

`SessionOpenResultDto` gains three fields, derived from the raw's market
day metadata at open time, all nullable for raws with no market day:

```json
{
  "sessionId": "session-1",
  "rawId": "sha256-…",
  "projections": [{ "spec": "bars:1s" }, { "spec": "bars:1m" }],
  "replaced": null,
  "marketDay": "2025-11-11",
  "sessionStartNs": "1762902000000000000",
  "sessionEndNs": "1762984800000000000"
}
```

This is the only server-side change in this design.

## Client layer: `features/replay/`

Mirrors the days feature's structure (`api.ts`, `types.ts`, components).

**`api.ts` — session client.** Thin typed wrappers over `requestIpc` for
the eight methods, plus `subscribeSession(sessionId, handlers)` which wraps
`subscribeIpcEvents`, filters by method + `sessionId`, and dispatches
parsed, typed events: `clock(ClockSnapshot)`, `cursor(EsReplayCursor)`,
`barsFrame(BarsFrame)`, `closed(reason)`. Parsing follows the defensive
style of `parseProgressEvent` — malformed notifications are dropped, not
thrown. This module is fully projection-agnostic.

**`types.ts` — wire types.** TypeScript mirrors of the DTOs. All
nanosecond timestamps are decimal strings on the wire (they exceed 2^53);
they stay strings in the wire types and convert to `number` milliseconds /
seconds only at the display edge. Prices are integer **ticks**; for ES one
tick is 0.25 points, so `price = ticks * 0.25` (exact in floating point —
ticks are quarter points). The tick size is a constant in the replay
feature for now; it moves to the wire when a second instrument exists.

**`accumulator.ts` — the frame contract, once.** One accumulator instance
per projection stream, implementing the barsFrame protocol exactly as the
transport spec defines it (the validation script's accumulator is the
reference implementation):

- New `epoch` → reset: drop all bars, accept the frame's `from: 0` array.
- Same epoch → contiguous append: frame `from` must equal current length;
  a gap means a dropped notification — recover by pulling
  `session/bars { from: length }` rather than carrying corrupt state.
- Track `live` (the in-progress bar, replaced wholesale each frame) and
  `status` (epoch, processedBatches, completedBars).
- Expose a change signal (subscribe callback) the view layer adapts to
  React with `useSyncExternalStore`.

The epoch/from/total/live contract is what any future frame-shaped
projection reuses; bars is its first — currently only — implementer.

## Projections on the chart: the layer registry

Mobile sets the surface model: the replay page has **one main chart**, and
projections that draw render *onto* it as layers — not as separate views or
panes of the page. A projection kind maps to a chart-layer factory; a kind
with no entry is headless (status chip at most).

```ts
// features/replay/chart-layers.ts
interface ChartLayer {
  attach(chart: IChartApi): void // add series/primitives, subscribe accumulator
  detach(): void
}

type LayerFactory = (session: ReplaySession, spec: string) => ChartLayer

const kindOf = (spec: string) => spec.split(":", 1)[0]

const CHART_LAYERS: Record<string, LayerFactory> = {
  bars: barsLayer, // candlestick base + volume histogram
}
```

The replay page resolves each of the session's specs through this map and
attaches the resulting layers to the one chart. `bars` is the *base* layer
(candles own the price scale); exactly one bar spec is active at a time,
picked by tabs on the chart surface (both accumulators run regardless, so
switching is instant). Future kinds compose onto the same surface: a study
that shares the price/time domain adds a line/band/marker series; exotic
rendering (profiles, heatmaps) uses lightweight-charts series/pane
primitives — custom canvas drawing hooks scoped to the chart; something
that genuinely can't share the price axis gets a pane below (the library
supports panes) before it ever gets a separate screen. Adding a drawable
kind is: write the layer, add one line. That is the entire mechanism by
which "some projections might not need a view" and "some will need UI
stuff" are handled.

What the UI opens: for now, a hardcoded list — `["bars:1s", "bars:1m"]`.
Which specs a replay session should request is a product decision per
projection kind, not something the UI discovers; when headless projections
exist that feed stats panels, they join this list without joining the
registry.

## The replay page

Composition, top to bottom:

- **Header** — market day, symbol, readiness of the pipe (component status
  if the feed fails), exit back to days.
- **The chart** — the one surface, with the session's drawable projections
  attached as layers. Tabs on the chart (segmented control: `1s` / `1m`)
  pick the active bar spec; headless specs show as status chips near the
  header.
- **Transport bar** — the playback surface:
  - Play/pause toggle and speed selector (presets: 1×, 2×, 5×, 10×, 25×,
    100×; the wire takes any positive float).
  - Seek scrubber spanning `[sessionStartNs, sessionEndNs]` from the open
    result, cursor position marked, click/drag to seek.
  - Clock readout: session time (America/New_York, matching the rest of
    lens) and mode.
  - Feed progress: `batchIdx / totalBatches`, plus `ended` as a terminal
    marker on the scrubber.

### Control semantics the UI must honor

The transport's controls are deliberately eventually-consistent, and the
UI's state model follows from that:

- **Rendered state comes from the clock stream, never from button
  presses.** Controls return `{ok: true}` on submission, before the write
  applies. The play button doesn't flip itself; it flips when the clock
  notification with the new mode arrives. (Latency is one runtime
  wake — imperceptible — and this keeps the UI truthful under races,
  e.g. two viewers.)
- The clock stream coalesces by revision; the UI treats each notification
  as current-full-state, never as a delta or a completed queue.
- **Seek is epoch-changing.** A seek (especially backward) rebuilds
  projections: accumulators see an epoch change and reset; the chart
  clears and refills as frames stream in. During catch-up the projection's
  `status.processedBatches` lags `cursor.batchIdx`; the page shows a
  subtle syncing indicator until they converge (the same convergence test
  the validation script uses) instead of pretending stale bars are
  current.
- Scrubbing while playing keeps playing — seek doesn't change mode, and
  the UI shouldn't either.
- Clock notifications arrive on clock *changes* (controls), not
  continuously during playback. While running, the UI extrapolates session
  time from the last committed snapshot — `sessionNowNs + elapsed-wall ×
  speed` — and every notification re-anchors the extrapolation. Button
  presses never move the clock locally.

## The chart surface

**Library: TradingView `lightweight-charts` v5** (Apache-2.0, canvas-based,
~45 kB gzipped, no framework coupling). It is a *renderer*, not a charting
platform: it ships zero indicator math, which is exactly right for us —
all computation is server-side projections; the client only draws. It owns
the time scale, price scales, crosshair, and interaction (mouse and touch:
pinch-zoom and kinetic pan are built in, which matters for the mobile
surface). Streaming is first-class via `setData`/`update`. v5 adds panes
and the series/pane-primitives plugin API the layer registry leans on.
Known costs: a small TradingView attribution logo in the chart corner
(license NOTICE; `attributionLogo` on by default), and no user drawing
tools — out of scope anyway. First new lens dependency of this work.

React integration is deliberately thin: the chart is imperative, created
once in a ref+effect; data flows accumulator → series `update()` outside
the React render path, so frame streams never cause re-render churn. The
library is confined behind the layer boundary — accumulators and the
transport client know nothing about it — so outgrowing it (say, a future
microstructure visualization its primitives can't express) costs replacing
the chart surface, not the feature.

Mapping from the accumulator (the `bars` base layer):

- Completed bars → candlestick series data. On epoch reset or projection
  switch: `setData(all)`. On append: `update(bar)` per new bar.
- The live bar → `update()` with its interval start as the time key; the
  same key replaces the last candle in place, which renders the forming
  bar exactly as trading UIs do.
- `time = intervalStartNs / 1e9` (seconds; safe well past 2^53 ns → the
  division happens on the string via BigInt or fixed-width slicing).
  lightweight-charts renders UTC; display in ET by rendering
  pre-shifted times with a UTC formatter (the standard
  lightweight-charts timezone approach) so axis labels match the clock
  readout.
- `priceFormat: { type: "price", minMove: 0.25 }`, values are
  `ticks * 0.25`.
- Below the candles, a volume histogram from `bar.volume` on a separate
  scale — the data is already in the frame; `buyVolume`/`sellVolume`
  coloring is available but v1 keeps a single neutral series.

Chart follows the data layer only: it subscribes to the accumulator, and
seek/epoch churn arrives as an ordinary reset. It knows nothing about the
transport.

## Failure and edge behavior

- **Open fails** (`object_not_found`, feed build error): toast + stay on
  days. The Replay gate makes this rare, not impossible (offload races).
- **`closed` notification** (`replaced` or server-initiated): terminal
  "session ended elsewhere" state on the page with an exit affordance; no
  auto-reopen.
- **Feed component failure** (`componentStatus: failed: …`): surfaced in
  the header; transport bar disables.
- **Frame gap** (non-contiguous `from`): accumulator self-heals via pull
  backfill, logs to console. Never tears down the page.
- **Bridge reconnect**: viewer-kit exposes host status; on reconnect the
  extension process may have restarted, so the page treats it as
  `closed` (sessions are process-local, not durable).
- **`cursor.ended`**: scrubber pins to the end marker, play button shows
  replay-complete affordance; seeking back restarts normally.

## Phases

**Phase 1 — the pipe, no chart.** Transport addition (session bounds in
open result + test), `features/replay/` client + accumulator + types,
Replay button + view switch, replay page with header, transport bar fully
functional (play/pause/speed/scrubber/clock/progress/syncing indicator),
and status chips where the chart will go — bar counts updating live prove
the frame stream end to end. Exit/close/replaced handling. This phase
demonstrates every semantic in this doc with trivial rendering.

**Phase 2 — the chart.** `lightweight-charts` dependency, the chart
surface + bars base layer (candles + volume, live bar, epoch reset), spec
tabs, layer registry wiring. Pure UI work on top of a proven data layer.

Acceptance for the pair, mirroring the transport spec's real-data
validation: open the CLI-validated raw
(`sha256-f54b79d5…`), seek to `1773266736362343529`, and the 1s chart
shows 274 completed bars plus a live bar; volume total 2365; switching to
1m shows 5; play at 25× advances candles smoothly; scrub backward resets
and refills without artifacts.
