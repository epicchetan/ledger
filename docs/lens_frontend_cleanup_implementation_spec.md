# Lens Frontend Cleanup and Viewport Persistence

Status: implementation complete 2026-07-10; on-device acceptance remains. The
completed pass introduces Zustand-owned viewport/UI state, route-v2
persistence, explicit follow mode, a candles-only bars presentation,
responsive/accessibility cleanup, and automated viewport/route coverage.

## Why this pass exists

Ledger already keeps replay truth on the server: session identity, clock,
speed, play/pause mode, feed cursor, projection cache, and projection delivery
position. Lens was treating chart presentation as implicit
`lightweight-charts` state. A native WebView reload therefore restored the
server session but created a new chart camera, and the append path inferred
"follow" from whether the latest bar happened to be visible. The result was:

- reloads reset time and price scales;
- a new completed bar could pull a user-adjusted time range back to real time;
- that horizontal pull also changed the auto-scaled vertical range;
- theme-driven chart remounts discarded manual price ranges;
- the base bars view carried a volume histogram and permanent OHLCV readout
  that are not needed for this projection's current viewer.

This pass makes viewport policy explicit and simplifies the base chart before
adding more projections or layout controls.

## Boundaries and invariants

Server state and viewer state remain separate.

Server-owned:

- session id and raw/projection identity;
- session clock, speed, and running/paused state;
- feed cursor and replay epoch;
- projection cache, head positions, delivery frames, and acknowledgements.

Viewer-owned:

- time zoom and logical range;
- whether the chart follows the newest bar;
- the newest bar's preferred horizontal position while following;
- price auto/manual mode and a manual visible price range;
- future visual preferences such as active interval or optional overlays.

Viewport state must not move into the shared replay session. Two viewers can
have different dimensions and inspect different historical windows while
attached to the same server truth.

Projection frames remain outside React. Zustand coordinates low-frequency
interaction policy; it is not a destination for bars, live candles, delivery
watermarks, or the replay clock.

## Zustand store

`lens/src/features/replay/chart/viewport-store.ts` owns a vanilla Zustand
store so both the imperative chart layer and React controls can consume it.

The durable snapshot is versioned independently from the Remux route:

```ts
interface ReplayViewportSnapshot {
  version: 1;
  time:
    | null
    | {
        mode: "follow";
        visibleBars: number;
        latestFraction: number;
      }
    | {
        mode: "fixed";
        range: { from: number; to: number };
      };
  price:
    | { mode: "auto" }
    | {
        mode: "manual";
        range: { from: number; to: number };
      };
}
```

`latestFraction` is the newest candle's horizontal position from the left.
The jump action writes `0.5`, so the latest candle is centered regardless of
zoom. A fresh replay deliberately retains the right-biased default to show more
history with modest future whitespace; centering is an explicit user choice.

Zustand `persist` writes only this viewport slice to `sessionStorage`, keyed by
raw id, for ordinary browser/in-WebView reloads. The Remux route remains the
authoritative native-reload mechanism.

## Durable Remux route

`ledgerReplay` resource JSON advances from v1 to v2:

```ts
interface ReplayResourceV2 {
  v: 2;
  sessionId: string;
  rawId: string;
  marketDay: string;
  symbol: string;
  viewport: ReplayViewportSnapshot;
}
```

Rules:

- v1 routes remain readable and start with a default viewport;
- v2 viewport input is shape- and finite-number-validated;
- the routed viewport wins over Web Storage during native restoration;
- an attach miss that opens a new session resets the stale routed viewport;
- Zustand changes update the current host tab resource metadata;
- exiting to Days resets and clears the fallback before the existing route
  clear/session-close flow, so re-entering the same day remains a fresh view;
- incoming projection frames never rewrite the route.

## Time-scale policy

Following is explicit, never inferred from `range.to`.

Follow mode:

- appends derive a logical range from `visibleBars`, `latestFraction`, and the
  newest bar index;
- tapping the right-arrow preserves zoom, sets `latestFraction = 0.5`, and
  immediately centers the newest bar;
- new bars continue at the same position;
- interaction in progress suppresses programmatic following.

Fixed mode:

- a user pan, pinch, or time-axis zoom captures the settled logical range;
- inertia is allowed to finish before capture;
- appends never mutate the time scale;
- the right-arrow remains visible even when the newest candle is technically
  still inside the viewport, because visibility is not consent to follow.

Epoch changes:

- a newly attached layer restores a routed/stored viewport after its first
  authoritative `setData`;
- a seek from an already-mounted epoch preserves the current zoom, returns to
  follow mode, and centers the seek destination;
- overlap/resync snapshots in the same epoch preserve the active viewport.

The seek rule is now deterministic across fixed and following source views and
is covered by the pure viewport-policy tests.

## Price-scale policy

The default remains library auto-scale. At the end of a chart gesture:

- auto-scale is recorded as `{ mode: "auto" }`;
- manual mode records `priceScale.getVisibleRange()`;
- first hydration, native reload, and theme/layer remount restore manual ranges
  with the public `setVisibleRange()` API;
- ordinary candle updates do not overwrite a manual range.

This pass chooses fixed manual range as the vertical policy. It preserves the
user's price camera exactly and never shifts it merely to pin a moving last
price. A separate last-price-anchor mode is deferred until there is an explicit
use case; it is not part of this cleanup contract.

## Bars presentation cleanup

The active `bars:1m` projection remains unchanged on the wire. Lens simply
uses less of its payload.

Implemented presentation contract:

- one candlestick series;
- no volume histogram or volume scale;
- no permanent O/H/L/C/V readout;
- top-left chrome is only replay clock plus `symbol · interval`;
- the bottom price-scale margin is symmetric again because no volume band
  needs reserved space;
- crosshair and axis labels remain available for point inspection;
- `Bar.volume`, buy/sell volume, and trade count remain valid projection data
  for future optional overlays and are not removed from the protocol.

An optional volume/inspection projection can return later as a user-selected
layer. It should not be permanently attached to the base candles view.

## Cleanup completion record

### State migration

- Durable viewport policy lives in the persisted vanilla Zustand store.
- Scrub preview and the attached jump callback live in a separate ephemeral
  vanilla Zustand store, preventing slider churn from writing persistence.
- React controls subscribe through narrow `useStore` selectors.
- Transport/session lifecycle remains in `useReplaySession`; bars and delivery
  frames never enter Zustand.

### Layout and interaction

- Removing OHLCV leaves the clock/title readout comfortably clear of the right
  price scale at the target phone widths.
- Below 360 CSS pixels, object-specific Days controls collapse into one menu;
  wider layouts retain the direct delete/back buttons.
- The seek slider exposes an ET `aria-valuetext` and clears scrub state on
  pointer cancellation.
- Fresh replay stays right-biased; the center-latest action is explicit.

### Reliability and tests

- `viewport-policy.test.ts` covers default, fixed, centered-follow, append, and
  interaction-noise range semantics.
- `route.test.ts` covers route-v2 round-trip, v1 compatibility, wrong kinds,
  and malformed viewport rejection.
- `viewport-store.test.ts` covers fallback persistence, routed precedence, and
  deliberate storage clearing.
- The harness exposes both Zustand stores and `resetViewport()` alongside its
  seed/append/reseek helpers.

Headless Chromium validation at 320, 375, 390, and 430 CSS pixels found no
page errors or horizontal overflow. At 320 pixels, a real chart drag entered
fixed mode, an appended bar left that range unchanged, center-latest restored
`latestFraction = 0.5`, a real price-axis drag produced a manual range, and a
reload restored that manual range from Zustand persistence.

### Documentation debt

`lens_replay_chart_implementation_spec.md` remains a historical implementation
record. Revision 6 points here as the superseding contract for the removed
Auto control, volume band, OHLCV legend, and inferred right-edge following.

## Delivery phases

### Phase A — complete

- add Zustand dependency and vanilla viewport store;
- add route v2 persistence with v1 read compatibility;
- make follow/fixed time policy explicit;
- persist and restore manual price ranges;
- remove volume and OHLCV chrome;
- preserve existing server/session behavior.

### Phase B — implemented; device acceptance pending

- touch inertia settles for 160 ms before final viewport capture;
- seek preserves zoom and centers; fresh replay remains right-biased;
- fixed manual range is the chosen vertical policy;
- reload, pause/running, pan, pinch, price-scale, theme, seek, and close/re-enter
  still require the final on-device acceptance run.

### Phase C — complete

- added narrow-width Days actions;
- added slider value text and cancellation;
- completed the chart readout collision cleanup by removing OHLCV;
- replaced the temporary `ChartUi` bridge with an ephemeral Zustand store.

### Phase D — complete

- viewport/route unit tests;
- harness scenarios;
- historical-spec revision note;
- final automated typecheck, test, lint, and build record; on-device acceptance
  remains explicitly pending.

## Acceptance criteria

- Reloading an attached replay restores server time/speed/mode and the most
  recently settled time and price viewport.
- A user pan disables following even if the latest bar remains visible.
- New candles never move a fixed time range.
- Tapping the right-arrow centers the latest candle at the current zoom and
  keeps it centered as candles append.
- A manual price range survives projection snapshots, chart remounts, and
  native reload.
- Back to Days then re-enter starts with default viewport state.
- The chart shows candles only: no volume histogram and no OHLCV row.
- Projection delivery remains bounded and independent of React rendering.
