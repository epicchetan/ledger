# Lens Replay Chart — Implementation Spec (Phase 2)

Implements the chart phase of `docs/lens_replay_view_design.md`: the
`lightweight-charts` surface, the layer registry, the bars base layer
(candles + volume, live bar, epoch reset), and spec tabs. Pure lens work on
top of the proven Phase 1 data layer — **no Rust, no wire changes, no
accumulator changes**. The design doc owns the *why*; this spec pins the
*what* precisely enough to implement without further decisions.

## Baseline (what exists, and what must not change)

- `features/replay/use-replay-session.ts` — owns the session lifecycle and
  exposes `ReplaySession { …, projections: BarsAccumulator[], controls }`.
  Untouched except where noted (none).
- `features/replay/accumulator.ts` — `BarsAccumulator` with
  `subscribe`/`getSnapshot` and the epoch/from/total/live frame contract.
  **Untouched.** The chart is a pure consumer of its snapshots.
- `features/replay/types.ts`, `api.ts` — wire types and RPC client.
  **Untouched.** `TICK_SIZE = 0.25` is imported from types.
- `features/replay/replay.tsx` — the page. The `ProjectionCard` grid (the
  Phase 1 placeholder, its own comment says "The Phase 2 chart slot") is
  replaced by the chart. `EndedBanner`, the syncing hooks
  (`useAnyProjectionSyncing`, `useSustained`, `useProjectionLag`), and the
  `ReplayActionBar` wiring all stay.
- `features/replay/replay-action-bar.tsx` — **untouched.** Transport stays
  on the bar; the chart adds no transport controls.

Accumulator facts the layer leans on (verified against the source):

- `bars` arrays are append-only within an epoch **except** the overlap
  branch (backfill pulls / re-sent frames), which rebuilds via
  `slice(0, from).concat(frame.bars)` — so appended arrays share element
  references with their predecessor, and a rewrite breaks reference
  identity at or before the old tail. This is what makes cheap append
  detection sound (see bars layer).
- Every publish carries current `live` and `status`; `live` is replaced
  wholesale per frame, `null` when no bar is forming.
- Post-cleanup, catch-up frames are chunked and coalesced (a 20k-batch
  seek lands in ~4 frames), so the chart needs **no buffering during
  syncing** — each frame is applied as it arrives; catch-up renders as a
  fast fill, which is the desired look. No "buffer and snap on
  convergence" machinery.

## Dependency

`lens/package.json` gains one dependency: `"lightweight-charts": "^5.2.0"`
(current latest; Apache-2.0). Run `npm install` in `lens/`. The TradingView
attribution logo stays at its default (on) — license-polite, and disabling
it is a non-goal. First new lens dependency of this feature; nothing else
is added.

v5 API used: `createChart` (with `autoSize: true`), `chart.addSeries(
CandlestickSeries | HistogramSeries, options)`, `chart.removeSeries`,
`chart.remove`, `series.setData` / `series.update`, `chart.timeScale()`
(`applyOptions`, `getVisibleLogicalRange`, `setVisibleLogicalRange`,
`scrollToRealTime`), `chart.priceScale(id).applyOptions`.

## File layout

New directory `lens/src/features/replay/chart/`:

| File                | Contents                                              |
| ------------------- | ----------------------------------------------------- |
| `layers.ts`         | `ChartLayer`, `LayerContext`, `LayerFactory`, `CHART_LAYERS`, `kindOf` |
| `bars-layer.ts`     | the candlestick + volume layer factory                 |
| `chart-surface.tsx` | React owner of the `IChartApi` lifecycle + `useRemuxTheme` |
| `theme.ts`          | CSS-token → canvas-color resolution, `ChartColors`     |
| `time.ts`           | ns→seconds, ET offset, interval parsing                |

Modified: `replay.tsx` (chart integration + tabs), `package.json` /
`package-lock.json` (dependency). Nothing else.

## Change 1 — `time.ts`: time mapping

lightweight-charts renders timestamps as UTC. Axis labels and crosshair
must read as **ET**, matching the action bar's clock readout, so the layer
feeds the chart **pre-shifted** timestamps (the standard lightweight-charts
timezone approach): `chartTime = utcSeconds + etOffsetSeconds`.

```ts
// Interval starts are whole seconds; BigInt division is exact and avoids
// the 2^53 hazard of the raw ns value.
export function nsToSeconds(ns: string): number {
  return Number(BigInt(ns) / 1_000_000_000n)
}

// ET's UTC offset at a given instant, in seconds (−18000 EST / −14400 EDT).
export function etOffsetSeconds(atMs: number): number { /* via Intl longOffset, parse "GMT-04:00" (bare "GMT" = 0) */ }

// "bars:1s" → 1, "bars:1m" → 60; null for unrecognized specs.
export function intervalSecondsOf(spec: string): number | null
```

**One offset per session, computed once** from
`open.sessionStartNs` (fall back to `Date.now()` when bounds are null —
degenerate raws only). This is safe, not an approximation: an ES session
runs 18:00 ET → 17:00 ET and DST transitions happen 02:00 ET Sunday, when
the market is closed (Fri 17:00 → Sun 18:00) — **no session spans a
transition**. State this invariant in a comment where the offset is
computed.

Shifted times are internal to the chart module; the seek slider, clock
readout, and every wire value stay real. Crosshair/axis then format the
shifted value with UTC formatters:

```ts
const UTC_CLOCK = new Intl.DateTimeFormat("en-US", {
  timeZone: "UTC", hour: "numeric", minute: "2-digit", second: "2-digit", hour12: true,
})
// chart option: localization: { timeFormatter: (t) => UTC_CLOCK.format(new Date((t as number) * 1000)) }
```

## Change 2 — `theme.ts`: token resolution

Canvas `fillStyle` can't evaluate `var()` or `color-mix()`, so chart colors
are resolved from the live theme via a probe element:

```ts
// Resolves any CSS color expression (var(), color-mix(), …) to a computed
// color string the canvas accepts. Probe must be in the document to compute.
export function resolveCssColor(expression: string): string {
  const probe = document.createElement("span")
  probe.style.color = expression
  document.body.appendChild(probe)
  const color = getComputedStyle(probe).color
  probe.remove()
  return color
}

export interface ChartColors {
  background: string  // var(--background) — solid, so the attribution logo picks the right variant
  text: string        // var(--muted-foreground)
  grid: string        // color-mix(in oklab, var(--border) 55%, transparent)
  up: string          // var(--success)
  down: string        // var(--destructive)
  volume: string      // color-mix(in oklab, var(--foreground) 22%, transparent)
}

export function chartColors(): ChartColors  // resolves all of the above, called at chart creation
```

Exact mix percentages are implementer's judgment on device; the token
choices are not (candles = success/destructive, chrome = border/muted).

## Change 3 — `layers.ts`: the registry

The design doc's registry, concretized against what layers actually need:

```ts
import type { IChartApi } from "lightweight-charts"
import type { BarsAccumulator } from "@/features/replay/accumulator"
import type { ChartColors } from "@/features/replay/chart/theme"

// A projection kind's presence on the chart. attach() creates series /
// primitives and subscribes to the data source; detach() must remove every
// series it added and unsubscribe. Layers never touch the transport.
export interface ChartLayer {
  attach(chart: IChartApi): void
  detach(): void
}

export interface LayerContext {
  spec: string
  // Today every drawable kind is bars-frame-shaped; when a second frame
  // kind exists this widens to a union alongside the frame-protocol
  // generalization (see non-goals).
  accumulator: BarsAccumulator
  offsetSeconds: number
  colors: ChartColors
}

export type LayerFactory = (ctx: LayerContext) => ChartLayer

export const kindOf = (spec: string): string => spec.split(":", 1)[0]

export const CHART_LAYERS: Record<string, LayerFactory> = {
  bars: barsLayer,
}
```

A spec whose kind has no entry is headless — the page simply never builds
a layer for it. Adding a drawable kind is: write the factory, add one line.

## Change 4 — `bars-layer.ts`: the base layer

A plain class (no React) implementing `ChartLayer`. Owns two series and a
mapped-data cache parallel to the accumulator's bars array.

**attach(chart):**

1. `candles = chart.addSeries(CandlestickSeries, { upColor, downColor,
   wickUpColor, wickDownColor, borderVisible: false,
   priceFormat: { type: "price", precision: 2, minMove: TICK_SIZE } })`
2. `volume = chart.addSeries(HistogramSeries, { priceScaleId: "volume",
   priceFormat: { type: "volume" }, color: colors.volume,
   lastValueVisible: false, priceLineVisible: false })`, then
   `chart.priceScale("volume").applyOptions({ scaleMargins: { top: 0.82,
   bottom: 0 } })` — the classic under-candle overlay, no extra pane.
3. `chart.timeScale().applyOptions({ timeVisible: true, secondsVisible:
   (intervalSecondsOf(spec) ?? 60) < 60 })` — the 1s tab shows seconds,
   the 1m tab doesn't. Only one bars layer is ever attached, so
   last-writer-wins is moot.
4. Subscribe to the accumulator and immediately apply the current
   snapshot as a full reset (below) — attach must render existing data
   without waiting for the next frame.

**detach():** unsubscribe, `chart.removeSeries` both, null everything.

**Per publish** (`accumulator.getSnapshot()`), classify then apply:

```ts
// Append iff same epoch, no shrink, and the old tail element is the same
// object in the new array (accumulator appends share references; the
// overlap/backfill branch rebuilds objects, breaking identity — exactly
// the case that must force a full reset).
const prev = this.sourceBars
const appendable =
  snap.epoch === this.epoch &&
  snap.bars.length >= prev.length &&
  (prev.length === 0 || snap.bars[prev.length - 1] === prev[prev.length - 1])
```

- **Full reset** (`!appendable` — epoch change, first data, or overlap
  rewrite): remap all bars → `candleData`/`volumeData`, `setData` both
  series. If the *epoch* changed (or first attach), also reset the
  viewport (Change 5's rule).
- **Append** of `k = snap.bars.length - prev.length` new completed bars:
  map and push onto the caches; if `k <= APPEND_UPDATE_MAX` (constant, 4)
  call `series.update()` per bar on both series, else `setData` the
  caches (one call beats thousands of updates on big catch-up frames).
- **Live bar last, always:** if `snap.live`, `update()` its mapped point
  on both series after the completed-bar work. Same interval start ⇒ same
  time key ⇒ lightweight-charts replaces the forming candle in place — the
  standard live-bar rendering. The live point is never written into the
  caches (each frame re-carries it, so a cache `setData` dropping it is
  repaired within the same publish). Protocol guarantees its time ≥ the
  last completed bar's, satisfying `update()`'s monotonicity rule.

**Mapping** (`Bar` → chart points): `time = (nsToSeconds(intervalStartNs) +
offsetSeconds) as UTCTimestamp`; OHLC = `ticks * TICK_SIZE`; volume point
`{ time, value: bar.volume, color: colors.volume }`. Neutral volume color —
`buyVolume`/`sellVolume` coloring is a non-goal.

## Change 5 — viewport rules

Two behaviors, both explicit rather than trusting library defaults:

- **Epoch reset recenters.** After a full-reset `setData` where the epoch
  changed (seek, first fill): `setVisibleLogicalRange({ from: n -
  INITIAL_VISIBLE_BARS, to: n + RIGHT_PAD })` with `INITIAL_VISIBLE_BARS =
  120`, `RIGHT_PAD = 3`, `n` = completed count (+1 if live). Negative
  `from` on sparse data is fine (left whitespace).
- **Appends follow only if the user is at the right edge.** Before
  applying an append, read `getVisibleLogicalRange()`; if `range.to >=
  prevCount - 1` the user was tracking the edge — after applying, call
  `scrollToRealTime()`. Otherwise leave the viewport alone (the user has
  panned back to inspect; a catch-up flood must not yank them). This also
  gives the desired catch-up look: the epoch-reset frame recenters once,
  then the fill streams in at the right edge.

Everything else (pinch-zoom, kinetic pan, crosshair) stays at library
defaults — they're the reason for choosing this library on a mobile
surface.

## Change 6 — `chart-surface.tsx`: the React owner

The only React↔chart bridge. Props:

```ts
interface ChartSurfaceProps {
  layers: ChartLayer[]
  colors: ChartColors
  className?: string
}
```

- **Chart lifecycle effect** (runs once per component instance): create
  the chart on the container div with `autoSize: true`,
  `layout: { background: { type: ColorType.Solid, color:
  colors.background }, textColor: colors.text }`, grid lines in
  `colors.grid`, and the `localization.timeFormatter` from Change 1.
  Cleanup: detach all currently-attached layers, `chart.remove()`.
- **Layer diff effect** (deps: `layers`): detach layers no longer in the
  list, then attach new ones. Attach performs the full initial sync
  (Change 4), so a tab switch is: old layer detached (series removed),
  new layer attached (`setData` from its warm accumulator) — instant, no
  refetch.
- **Theme:** the surface does not restyle in place. `replay.tsx` keys it
  by theme (`<ChartSurface key={theme} …/>`) so a host theme flip remounts
  the chart with freshly resolved colors — rare event, cheap remount,
  zero applyOptions plumbing.

```ts
// data-remux-theme is stamped on <html> by the host (see the
// @custom-variant in index.css); observe it so lens re-skins with the host.
export function useRemuxTheme(): string {
  return useSyncExternalStore(
    (onChange) => {
      const observer = new MutationObserver(onChange)
      observer.observe(document.documentElement, { attributeFilter: ["data-remux-theme"] })
      return () => observer.disconnect()
    },
    () => document.documentElement.getAttribute("data-remux-theme") ?? "dark"
  )
}
```

The frame stream flows accumulator → layer → series entirely outside
React: no state, no re-render per frame. The only per-frame React work in
the whole page remains the existing boolean-snapshot lag stores.

## Change 7 — `replay.tsx` integration

- **Delete** `ProjectionCard`, `Stat`, and their per-card syncing wiring.
  Keep `EndedBanner`, `useAnyProjectionSyncing`/`useSustained`/
  `useProjectionLag`, and all action-bar wiring exactly as they are.
- **Layout:** the single-chart page doesn't scroll. `<main>` becomes a
  non-scrolling column (`lens-safe-top`, `flex min-h-0 flex-1 flex-col
  gap-3 px-3 py-3`; drop `lens-scroll`/`overflow-y-auto`/`space-y-3`);
  `EndedBanner` sits above; the chart card is `flex-1 min-h-0` so the
  chart (autoSize) fills the remaining height and owns its gestures.
- **Chart card** (house style: content, not nav): a bordered card like the
  old projection cards, containing a header row and the surface —
  - Header left: **spec tabs**, a hand-rolled segmented control (two
    buttons, `aria-pressed`, labels are the interval suffix `1s`/`1m` in
    mono) — no new Radix dependency for two buttons. Rendered only when
    more than one bar spec exists.
  - Header right: the status pill, same semantics as the old per-card
    pill — spinner + "syncing" when the page-level aggregate `syncing` is
    true, else the green dot + "live" when the active accumulator has a
    live bar. Live presence subscribes via a boolean-snapshot
    `useSyncExternalStore` (same pattern as `useProjectionLag`) so frame
    churn re-renders nothing.
  - Body: `<ChartSurface key={theme} layers={layers} colors={colors} />`.
- **Wiring:**

```ts
const specs = projections.map((a) => a.spec)
const barSpecs = specs.filter((s) => kindOf(s) === "bars")
const [activeBarSpec, setActiveBarSpec] = useState(barSpecs[0])
const theme = useRemuxTheme()
// offset: once per session from open.sessionStartNs (Change 1)
// colors: useMemo(chartColors, [theme])
// Drawable = registry hit; the bars kind additionally collapses to the
// active tab (both accumulators keep running — switching stays instant).
const layers = useMemo(() => attachedSpecs.map((spec) =>
  CHART_LAYERS[kindOf(spec)]({ spec, accumulator: bySpec(spec), offsetSeconds, colors })
), [activeBarSpec, colors, offsetSeconds, projections])
```

  Guard the null-bounds case (offset falls back per Change 1); guard
  `barSpecs` being empty (render no chart card — headless-only sessions).

## Failure / edge behavior (chart-specific)

- **Empty epoch** (seek to before the first trade): full reset with zero
  bars and `live: null` → `setData([])`, blank chart, no error.
- **Live-only** (first bar still forming): `update()` on an empty series
  is a valid first point.
- **Backfill overlap rewrite:** reference-identity check fails → full
  reset without viewport recenter (epoch unchanged). Correctness over
  cleverness; these are rare.
- **Session ended / replaced:** no chart-specific handling — the page's
  existing phase logic shows the banner; the chart keeps its last state
  underneath, which is correct (the data isn't wrong, the session is
  over).
- **Chart exceptions must not kill the page:** wrap the per-publish apply
  in try/catch that logs (`console.warn`, matching accumulator style) —
  a rendering bug degrades to a stale chart, never a white screen.

## Acceptance criteria

Automated (runnable here):

1. `npm run typecheck && npm run lint && npm run build` green in `lens/`.
2. `package.json` gains exactly one dependency; no accumulator / api /
   types / action-bar / use-replay-session diffs.

Manual, on device (the webview can't render headless — user verifies,
mirroring the design doc's acceptance): open the validated raw's day
(2026-03-12, `sha256-f54b79d5…`), seek to mid-session
(`1773266736362343529` ≈ the validation point):

3. 1s tab: 274 completed candles + a forming live candle with volume
   histogram; 1m tab: 5 candles — matching `transport_validate.py`.
4. Play at 25×: live candle mutates in place; completed candles append;
   right edge follows; panning back stops the following; syncing pill
   never flickers during normal playback.
5. Scrub backward: chart clears and refills (epoch reset), viewport
   recenters once, no stale bars, no viewport yanking during the fill;
   syncing pill during catch-up, clears at convergence.
6. Tab switch is instant both directions and survives a mid-catch-up
   switch.
7. Axis and crosshair times read as ET and agree with the action bar's
   clock readout (including AM/PM).
8. Host theme flip re-skins the chart (remount) with legible colors in
   both themes.

## Non-goals (deliberate, revisit with a real second consumer)

- `buyVolume`/`sellVolume` candle-direction volume coloring (data is
  already in the frame; a later polish pass).
- Extra panes, custom series, or primitives — nothing today needs them;
  they're the documented path for footprint-style kinds when those exist
  server-side.
- Generalizing the frame protocol / accumulator beyond bars — happens
  alongside the first non-bars drawable projection, designed against its
  real payload.
- Viewport persistence across tab switches or sessions; drawing tools;
  proportional (wall-clock) gap rendering; disabling the attribution logo.
- Frame batching/backpressure — chunked catch-up already bounded the
  stream ~1000×; revisit only with observed stutter.

## Verification plan

Lens has no unit-test runner (none configured; adding one is out of
scope), so verification is: the three npm checks above, a close read of
the layer's classify/apply logic against the accumulator contract, and
the on-device manual checklist. The real-data numbers come from
`scratchpad/transport_validate.py`'s validated expectations (274 / 5 bars,
volume 2365).

---

# Revision 1 (Phase 2.1) — full-bleed TradingView surface

Post-implementation revision, user-directed. Reference: the deleted
desktop chart at commit `183fae5` (`lens/src/components/chart.tsx`),
adapted from its hardcoded hexes to the live theme tokens. This section
supersedes the parts of Changes 6–7 it names; everything else above
stands. The attribution-logo line in the non-goals is withdrawn: this is
a personal tool, `attributionLogo: false`.

Baseline facts confirmed against the tree: the action bar already shows
its own `syncing` indicator (so the chart header pill is redundant);
`use-replay-session.ts` originally requested `REPLAY_SPECS =
["bars:1s", "bars:1m"]` (Revision 5 later removes the unused 1s
projection); `--muted` exists in the viewer-kit tokens and lens maps
`--font-mono: var(--rmx-font-mono)` (Menlo/Consolas stack). Canvas font
strings can't evaluate `var()`, so the font family is resolved from
computed style like the colors are.

Free behavior, confirm-only (lightweight-charts defaults, do not
disable): right price scale with `autoScale` fitting the visible bars as
the user pans; time-axis drag/pinch to stretch or squeeze historical
bars; kinetic touch scroll.

## R1 — `replay.tsx`: full-bleed layout, no tabs, no pill

- Delete `SpecTabs`, `ChartStatusPill`, `intervalLabel`, and
  `useHasLiveBar`. Keep `useAnyProjectionSyncing` and friends — the
  action bar still consumes `syncing`.
- Pin the drawn interval: `const ACTIVE_BAR_SPEC = "bars:1m"` at module
  scope replaces the `barSpecs`/`useState` pair; the layers memo
  collapses the bars kind to it exactly as before. (1s initially kept
  accumulating headless; Revision 5 removes it. Re-adding an interval
  picker later is a
  one-line revert.)
- `<main>` loses all padding/gap and the card: it stays
  `lens-safe-top flex min-h-0 flex-1 flex-col`. Inside it, one wrapper
  `<div className="relative min-h-0 flex-1">` holds the chart and every
  overlay. The wrapper (not `main`) is the positioning context so
  absolute children respect the safe-area padding.
- Inside the wrapper: `<ChartSurface key={theme} … className="h-full" />`
  plus the overlays (R4): `ChartLegend` top-left, `JumpToLive`
  bottom-right, and — when `phase === "ended"` — the `EndedBanner`
  floating `absolute inset-x-0 top-0 z-10 m-2` instead of stacking above
  the chart.
- One `ChartUi` instance (R3) per page via `useMemo(() => new ChartUi(),
  [])`, passed into every `LayerContext` and to both overlay components.

## R2 — chart options and theme tokens

`theme.ts` additions:

- `chartFontFamily(): string` — computed `font-family` of a probe
  element with `font-family: var(--font-mono)` (same in-document probe
  trick as `resolveCssColor`); empty result falls back to `monospace`.
- `ChartColors` gains: `crosshair` (`color-mix(in oklab,
  var(--foreground) 20%, transparent)`), `crosshairLabel`
  (`var(--muted)` — solid chip behind axis labels), `priceLine`
  (`color-mix(in oklab, var(--foreground) 25%, transparent)`).

`chart-surface.tsx` — `createChart` options become (additions marked):

- `layout`: background/textColor as today, **plus `fontFamily:
  chartFontFamily()` (resolved once alongside colors — add it to the
  `ChartColors` bundle or pass beside it), `fontSize: 11`,
  `attributionLogo: false`**.
- **`crosshair`: `mode: CrosshairMode.Magnet`; vert and horz lines
  `{ color: colors.crosshair, style: LineStyle.Dashed,
  labelBackgroundColor: colors.crosshairLabel }`.**
- **`rightPriceScale`: `{ borderVisible: false, scaleMargins: { top:
  0.08, bottom: 0.24 } }`** — bottom clears the volume band, which keeps
  its own overlay scale at `{ top: 0.82, bottom: 0 }` from Change 4.
- `grid`, `localization.timeFormatter`: unchanged.
- **`timeScale`: `{ borderVisible: false, rightOffset: 8 }`.** The
  layer's per-spec `timeVisible`/`secondsVisible` applyOptions from
  Change 4 are unchanged and compose with this.

`bars-layer.ts` candle series gains the dashed last-price line:
`priceLineVisible: true, priceLineWidth: 1, priceLineStyle:
LineStyle.Dashed, priceLineColor: colors.priceLine, lastValueVisible:
true`. `RIGHT_PAD` becomes 8 to match `rightOffset`, so the epoch-reset
recenter and the live-edge resting position agree.

## R3 — `ui-state.ts`: the overlay store

New file `chart/ui-state.ts`. One tiny external store keeps overlay
re-renders out of the page: React re-renders only the two overlay
components, and only when their snapshot actually changes.

```ts
export interface LegendData {
  open: number; high: number; low: number; close: number; // price units
  volume: number
  up: boolean // close >= open, drives value color
}

export class ChartUi {
  // getSnapshot-stable fields; set() no-ops on Object.is equality and
  // notifies subscribers otherwise.
  legend: LegendData | null
  atEdge: boolean
  // Assigned by the attached bars layer; null when detached.
  jumpToLive: (() => void) | null
  subscribe(onChange: () => void): () => void
  setLegend(legend: LegendData | null): void
  setAtEdge(atEdge: boolean): void
}
```

Legend equality: replacing the legend with a value-identical object must
not notify (compare fields, not reference) — crosshair moves inside one
bar would otherwise churn the subscriber per pixel.

## R4 — `overlays.tsx`: legend and jump-to-live

New file `chart/overlays.tsx`, both components subscribed via
`useSyncExternalStore` against the `ChartUi` instance.

- `ChartLegend`: `pointer-events-none absolute top-2 left-2 z-10`, mono
  11px row `O H L C V`; O/H/L/C formatted `.toFixed(2)` after the layer's
  tick→price conversion, V via `toLocaleString()`. Labels
  `text-muted-foreground`; values `text-success` when `up`, else
  `text-destructive` (the same tokens the candles resolve). Renders
  nothing while `legend` is null.
- `JumpToLive`: chevron button `absolute right-14 bottom-8 z-10` (clear
  of the price axis), visible only when `!atEdge`; `onClick` invokes
  `jumpToLive` (no-op when null). Styling per the old chart: bordered,
  `bg-card/90`, muted icon, hover accent.

## R5 — `bars-layer.ts`: feeding the store

The layer owns both chart subscriptions (it has the chart, both series,
and the data; the surface stays layer-agnostic):

- On `attach`: `chart.subscribeCrosshairMove(handler)` and
  `chart.timeScale().subscribeVisibleLogicalRangeChange(handler)`; set
  `ui.jumpToLive = () => chart.timeScale().scrollToRealTime()`. On
  `detach`: unsubscribe both, `ui.jumpToLive = null`,
  `ui.setLegend(null)`, `ui.setAtEdge(true)`.
- Crosshair handler: `param.seriesData.get(this.candles)` /
  `.get(this.volume)` → `setLegend({...})` when the point maps to a bar;
  when the crosshair leaves (no point / no data), fall back to the
  latest bar (live if present, else last completed) rather than null.
  Track `hovering` (whether the crosshair currently maps to a bar) on
  the instance.
- Publish path (`apply`): when not hovering, refresh the legend to the
  latest bar so the legend tracks the forming candle — this is the one
  behavior upgrade over the old chart, which only wrote the legend on
  crosshair events. Store equality (R3) makes the per-frame call cheap.
- `atEdge`: computed in the range-change handler as `range == null ||
  range.to >= barCount - 1` where `barCount = candleData.length +
  (live ? 1 : 0)`, and refreshed after resets/appends (the existing
  `atEdge` local in `append` keeps its pre-append read for the follow
  decision; the store write is display-only and may lag it by one
  event).
- Legend values reuse `toCandle`'s price conversion (`* TICK_SIZE`).

## R6 — explicitly out of scope for revision 1

- The old chart's keyboard navigation (arrow/±/Home/End) — dead weight
  in a touch webview.
- Its `DomPrimitive` depth-of-market overlay and `vp` mode — that is the
  footprint phase.
- An interval picker in the action bar (the tabs' successor) — add only
  when 1m-only proves limiting.

## Revised acceptance criteria (delta)

Automated (npm checks) unchanged. Manual, replacing/extending 3–8:

- The chart fills the entire area between the safe-area top and the
  action bar; no card border, no header row, no page padding.
- Only 1m candles draw; the 1s projection still accumulates (no
  regression in `syncing` behavior, which aggregates both).
- No TradingView logo.
- Long-press/drag shows the magnet crosshair with dashed lines and
  themed label chips; the legend tracks it, direction-colored, and falls
  back to the forming candle when released (and keeps updating with it).
- Pinch/drag on the time axis stretches historical bars; the right
  price scale re-fits the visible range while panning.
- Panning left reveals the jump-to-live chevron; tapping it returns to
  the live edge and hides it; during playback at the edge the button
  never appears.
- The dashed last-price line tracks the forming candle's close.
- Theme flip re-skins crosshair, legend, price line, and chip colors.

---

# Revision 2 (Phase 2.2) — transport cleanup, mobile polish

User-directed follow-up to the Phase 2.1 device test (which also surfaced
the oklab color-normalization fix and the sparse-epoch recenter clamp,
both already landed in `theme.ts`/`bars-layer.ts`). Reference for the
interaction decisions: the TradingView mobile UX research (gesture map,
tracking mode, the axis-corner "Auto" affordance, bottom-sheet chrome).

Scope: the boundary now includes `replay-action-bar.tsx` and the
diagnostic harness (`harness.html`, `src/dev/harness.tsx`). Everything
else frozen in Phase 2/2.1 stays frozen.

## R7 — `ui-state.ts`: transport + price-scale state

`ChartUi` gains, with the same equality-gated set/notify discipline:

- `scrubMs: number | null` (+ `setScrubMs`) — the slider's mid-drag
  preview position in session-ms; null when not scrubbing.
- `autoScale: boolean` (+ `setAutoScale`), default true — whether the
  right price scale is still auto-fitting.
- `resetPriceScale: (() => void) | null` — assigned by the attached bars
  layer (like `jumpToLive`); re-engages auto-scale.

## R8 — `use-session-clock.ts`: shared clock extrapolation

New file `features/replay/use-session-clock.ts`. Move the bar's
`useSessionNowMs` hook, `formatEtTime`, and `nsToMs` there verbatim
(exported); the action bar and the new clock overlay both import them.
No behavior change: extrapolate between clock notifications while
running (100ms ticker), verbatim while paused.

## R9 — `overlays.tsx`: clock, auto-scale button, legend title

- `ClockOverlay({ ui, clock, clockReceivedAt })`: `pointer-events-none
  absolute top-2 right-2 z-10`, right-aligned, same quiet styling as the
  old bar readout (mono tabular time, small AM/PM+zone suffix caption).
  Shows `ui.scrubMs` (subscribed) when non-null — the scrub preview —
  else the extrapolated server clock; "--:--:--" with no clock. While
  scrubbing, the time renders `text-foreground` (full emphasis) so the
  preview reads as active.
- `AutoScaleButton({ ui })`: a small "Auto" pill at `absolute right-2
  bottom-8 z-10` (the price-axis corner, TradingView's spot), visible
  only while `ui.autoScale` is false; tapping calls
  `ui.resetPriceScale?.()`. Same chrome as JumpToLive (border,
  `bg-card/90`, muted text, hover accent; `touch-action: manipulation`
  inherited fine — no extra handling). JumpToLive stays at `right-14`,
  so both fit side by side.
- `ChartLegend` gains a `title: string` prop (e.g. `ESH6 · 1m`),
  rendered as a leading cell in `text-foreground/90` — visible even
  while the values are null (symbol/interval chrome lives here now that
  the tabs row is gone). Values keep their current behavior.

## R10 — `bars-layer.ts`: price-scale mode tracking

- On attach: assign `ui.resetPriceScale = () => { chart.priceScale(
  "right").applyOptions({ autoScale: true }); ui.setAutoScale(true) }`;
  null it on detach (and reset `ui.setAutoScale(true)`).
- Detection — lightweight-charts has no mode-change event, so read
  `chart.priceScale("right").options().autoScale` from two probes:
  (a) each publish (covers playback), and (b) a `pointerup` listener on
  `chart.chartElement()` added on attach / removed on detach (covers a
  paused user dragging the axis; double-tap reset also lands here).
  Both write `ui.setAutoScale(...)`; the store's equality gate makes
  the repeated writes free.

## R11 — `chart-surface.tsx`: tracking-mode exit

Add `trackingMode: { exitMode: TrackingModeExitMode.OnTouchEnd }` to the
chart options (value import from lightweight-charts). Long-press still
enters the crosshair; lifting the finger now drops it and the legend
falls back to the live bar — the TradingView-app feel — instead of the
library default (persist until next tap).

## R12 — `replay-action-bar.tsx`: row order, scrub callback, hit target

- Delete `ClockReadout` (and its now-unused local helpers — they moved
  to R8); the bar no longer renders time.
- Button row order: [open-tabs] [back] — spacer (`ml-auto` on the next
  control) — [speed menu] [play/pause]. Play/pause is rightmost (right
  thumb), keeps `tone="primary"`; the speed menu flips to `align="end"`.
- New prop `onScrub: (sessionMs: number | null) => void`: `onChange`
  reports the drag position converted to session-ms (from the bounds);
  `commitSeek` and `onBlur` report null after their existing work. No
  change to seek semantics (commit on release only, mode preserved).
- Slider touch target: `h-1.5` is a 6px hit area that also clips the
  iOS thumb — change the input to `h-7 w-full` (28px). Keep
  `accent-primary`.

## R13 — `replay.tsx`: wiring

- Render `<ClockOverlay ui={ui} clock={clock}
  clockReceivedAt={clockReceivedAt} />` and `<AutoScaleButton ui={ui} />`
  inside the chart wrapper alongside the existing overlays.
- `<ChartLegend ui={ui} title={`${symbol} · ${interval}`} />` where
  interval is `ACTIVE_BAR_SPEC`'s suffix after the colon ("1m").
- Pass `onScrub={(ms) => ui.setScrubMs(ms)}` to the action bar (stable
  via useCallback on `ui`).

## R14 — harness

Extend `src/dev/harness.tsx` so the new overlays are exercisable
headless: render ClockOverlay with a fake running clock (any fixed
`clock`/`clockReceivedAt` pair), AutoScaleButton, and the legend title.
Expose `window.harness.ui` as today.

## Out of scope for this revision

- Holding the scrub preview until the post-seek clock notification
  lands (a sub-100ms snap-back on release is acceptable).
- Interval favorites/pills and the bottom-sheet interval picker.
- Landscape/fullscreen mode; drawing tools; price-scale long-press
  context menu.

## Revised acceptance criteria (delta)

- No time readout in the action bar; the chart's top-right shows the
  session clock, ticking during playback.
- Dragging the slider previews the target time in the top-right readout
  live, mid-drag; releasing seeks there and the readout returns to the
  server clock.
- Play/pause sits rightmost in the bar with the speed menu beside it;
  back/tabs stay left.
- Dragging the price axis vertically un-fits the price scale and makes
  the "Auto" pill appear at the axis corner; tapping it re-fits and
  hides the pill. Double-tapping the price axis does the same.
- Long-press shows the crosshair; lifting the finger dismisses it and
  the legend returns to tracking the forming candle.
- The legend's leading cell always shows `symbol · interval`.

---

# Revision 3 (Phase 2.3) — clock into the legend, chart actions into the bar

User-directed micro-revision after the Phase 2.2 review; implemented
directly (no delegation).

- `ChartLegend` absorbs the clock: line 1 is `<time> <suffix> <symbol ·
  interval>` (time first), line 2 the O H L C V cells. It takes
  `clock`/`clockReceivedAt` and keeps the scrub-preview behavior
  (`ui.scrubMs` wins over the extrapolated server clock, full-emphasis
  while scrubbing). `ClockOverlay` is deleted.
- The floating `JumpToLive`/`AutoScaleButton` chart overlays are
  replaced by `JumpToLiveAction`/`AutoScaleAction` — viewer-kit
  `ActionButton`s rendered inside the replay action bar's left group
  (after back), each `null` unless needed (off-edge / manual price
  scale). They keep their own narrow store subscriptions so the bar
  never re-renders on frame churn; `ReplayActionBar` gains a `ui:
  ChartUi` prop.
- The bar gains a reload button (`reloadHostView()`, `RefreshCw`,
  matching day-action-bar) between open-tabs and back.
- Harness: legend takes the fake clock; the two actions render in the
  stand-in bar strip.

---

# Revision 4 (Phase 2.4) — bar menu, page persistence

User-directed; implemented directly.

- The replay bar's standalone reload/back buttons collapse into a
  hamburger `ActionMenu` (lucide `Menu`, `align="start"`, the terminal
  viewer's pattern) holding "Reload viewer" and "Back to days".
  Open-tabs stays a top-level button. Left group: tabs · menu ·
  conditional chart actions.
- Speed menu panel: `panelClassName="!w-auto !min-w-24"` — the default
  232px panel dwarfs the preset labels; `!important` because viewer-kit's
  stylesheet is unlayered and beats Tailwind utilities otherwise.
- `App.tsx`: days and replay are persistent pages. The view is hydrated
  from `sessionStorage` (`lens.view.v1`, shape-validated) and persisted
  on change, so a webview reload lands back in the replay; mounting
  Replay always opens a fresh session (the pre-reload session is
  replaced server-side), and a failed open still toasts and exits to
  days — there is no sessionless replay page. A cold start (new webview
  context) begins at days.

---

# Revision 5 (Phase 2.5) — session reattach: reloads resume position

User-directed follow-up to Revision 4: a reload should resume the
running replay (position, speed, play state), not restart it. The server
session outlives the webview. Remux's hamburger reload recreates the
native WebView from the host tab URL, so Web Storage does **not** carry
navigation across that reload. The exact server-issued `sessionId` and
replay display identity are therefore persisted in the Remux resource
route. Matching only raw/spec is insufficient: it aliases a fresh
navigation, another tab, or another Remux client onto the active replay.

Server (`crates/remux/src/session.rs`, wired in `methods.rs`):

- New method `remux/ledger/session/attach { sessionId, rawId,
  projections }`.
- Succeeds only when the exact active id, raw, and canonical projection
  set all match. A stale id or identity mismatch is an expected typed
  `{ attached: false }` result; invalid params and store/cache failures
  remain RPC errors and never authorize a destructive fallback open.
- Returns the open-shaped identity (sessionId, rawId, projections,
  marketDay, session bounds re-derived from the raw's descriptor) plus
  the **current clock snapshot and feed cursor** — load-bearing: the
  clock watcher notifies on revision change only, so a paused session
  would otherwise stay silent forever after a reload.
- Read-only: never touches the session and never closes anything.

Frontend:

- After open/attach succeeds, call `updateHostTab` with
  `resourceKind: "ledgerReplay"` and a versioned JSON `resourceId`
  containing `{ sessionId, rawId, marketDay, symbol }`. Remux rebuilds
  `tab.url` with `remuxResourceKind` / `remuxResourceId`, persists the
  tab natively, and uses that URL as the hamburger reload source.
- `App.tsx` parses the initial URL with `parseRemuxViewerRoute`; a valid
  replay resource mounts Replay immediately and supplies its exact
  attach candidate. `sessionStorage` remains only a browser/in-page
  fallback, not the native reload contract.
- Back to days first clears the host resource route with
  `updateHostTab`, then unmounts Replay. Hook cleanup clears the local
  fallback capability and sends the fire-and-forget close, so immediate
  re-entry cannot restore the old replay.
- `types.ts`: discriminated `SessionAttachResult`; `api.ts`:
  `attachSession(sessionId, rawId, projections)`.
- Establish subscribes first and buffers pushes through a hydration
  barrier. On exact attach, seed clock/cursor, pull the full active bars
  frame, then pull status so a running clock is anchored near the end of
  hydration. Apply the seed before draining buffered pushes.
- Clock revisions, cursor `(epoch, feedSeq, batchIdx)`, and projection
  `(epoch, processedBatches, total)` are monotonic gates. A late pull or
  buffered notification can never rewind state. The accumulator also
  invalidates stale in-flight backfills on epoch change.
- Initial hydration is awaited and required; failures enter the normal
  replay error path instead of silently exposing an empty live chart.
- Subscribe to Remux `subscribeHostResume` and repeat exact-id attach +
  hydration after page restore, WebView visibility resume, or socket
  reconnect, because Remux does not replay notifications missed while a
  viewer was suspended.
- `REPLAY_SPECS` is `['bars:1m']`. The removed 1s projection was not
  drawn and existed only to feed the syncing aggregate; hydrating it
  transferred up to a full day of unused JSON bars through the native
  bridge. It can return with an interval picker or a status-only client.
- A typed attach miss clears the stale capability and opens fresh. Any
  thrown attach failure is surfaced and does not replace an ambiguous
  active session.
- A fresh open commits one seek to `sessionStartNs` before hydration;
  Ledger clocks use absolute Unix nanoseconds and otherwise begin at
  zero. Attach never seeks.

Acceptance deltas:

- Reload mid-replay: the chart comes back at the same session time,
  same speed, still playing (or paused) — clock readout correct
  immediately even when paused.
- Back to days → re-enter the same day: fresh session at session start
  (exit closed the old one), unchanged from before.
- Cold start / different day / changed REPLAY_SPECS: fresh open.

Tests: `attach_resumes_active_session_with_clock_and_cursor` covers both
paused and running snapshots; `attach_requires_matching_active_session`
covers no active session, wrong id/raw/specs, success, and closed-session
typed misses. The open validation test also proves an immediate bars pull
is available for hydration.
