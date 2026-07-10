/* eslint-disable react-refresh/only-export-components */
// Diagnostic harness (not shipped): renders the replay chart stack with a
// synthetic accumulator so the chart can be exercised in a plain browser,
// outside the remux host. Load /harness.html via `vite dev`.
import { useMemo } from "react"
import { createRoot } from "react-dom/client"

import { BarsAccumulator } from "@/features/replay/accumulator"
import {
  ChartSurface,
  useRemuxTheme,
} from "@/features/replay/chart/chart-surface"
import { CHART_LAYERS, kindOf } from "@/features/replay/chart/layers"
import {
  ChartReadout,
  JumpToLatestAction,
} from "@/features/replay/chart/overlays"
import { chartColors } from "@/features/replay/chart/theme"
import { etOffsetSeconds } from "@/features/replay/chart/time"
import {
  createReplayChartUiStore,
  type ReplayChartUiStore,
} from "@/features/replay/chart/ui-store"
import {
  createReplayViewportStore,
  type ReplayViewportStore,
} from "@/features/replay/chart/viewport-store"
import type { Bar, BarsProjectionFrame, Clock } from "@/features/replay/types"

import "../index.css"

const SPEC = "bars:1m"
const SYMBOL = "ESH6"
const INTERVAL = SPEC.slice(SPEC.indexOf(":") + 1)
// 2026-03-12 09:30 ET (13:30 UTC, EDT) in ns — matches the validation day.
const SESSION_START_NS = 1773322200n * 1_000_000_000n
const INTERVAL_NS = 60n * 1_000_000_000n

// A fixed running clock so the clock overlay's extrapolation ticks headless.
const FAKE_CLOCK: Clock = {
  mode: "running",
  speed: 1,
  sessionNowNs: (SESSION_START_NS + 300n * 1_000_000_000n).toString(),
  revision: 1,
}
const FAKE_CLOCK_RECEIVED_AT = performance.now()

function synthBar(i: number): Bar {
  const startNs = SESSION_START_NS + BigInt(i) * INTERVAL_NS
  const base = 24000 + Math.round(Math.sin(i / 9) * 60) + (i % 7) * 3
  const open = base
  const close = base + ((i % 5) - 2) * 6
  const high = Math.max(open, close) + 4
  const low = Math.min(open, close) - 4
  return {
    intervalStartNs: startNs.toString(),
    open,
    high,
    low,
    close,
    volume: 100 + ((i * 37) % 900),
    buyVolume: 50,
    sellVolume: 50,
    tradeCount: 42,
    firstTsEventNs: startNs.toString(),
    lastTsEventNs: (startNs + INTERVAL_NS - 1n).toString(),
  }
}

function synthFrame(
  epoch: number,
  from: number,
  count: number,
  withLive: boolean
): BarsProjectionFrame {
  const bars = Array.from({ length: count }, (_, k) => synthBar(from + k))
  const total = from + count
  return {
    subscriptionId: "harness",
    sessionGeneration: 1,
    spec: SPEC,
    kind: "bars",
    schemaVersion: 1,
    frameSequence: ++frameSequence,
    base:
      from === 0
        ? null
        : {
            epoch,
            projectionRevision: from,
            processedBatches: from,
            completedBars: from,
          },
    head: {
      epoch,
      projectionRevision: total,
      processedBatches: total,
      completedBars: total,
    },
    operation: from === 0 ? "snapshot" : "append",
    reason: from === 0 ? "initial" : "cadence",
    payload: {
      bars,
      live: withLive ? synthBar(total) : null,
      status: {
        spec: SPEC,
        epoch,
        processedBatches: total,
        completedBars: total,
        revision: total,
        lastTsEventNs: null,
      },
    },
  }
}

const accumulator = new BarsAccumulator(SPEC)
accumulator.beginSubscription("harness", 1, false)
const ui = createReplayChartUiStore()
const viewport = createReplayViewportStore("harness", null)
let frameSequence = 0

declare global {
  interface Window {
    harness: {
      accumulator: BarsAccumulator
      ui: ReplayChartUiStore
      viewport: ReplayViewportStore
      resetViewport: () => void
      seed: (count: number, withLive?: boolean) => void
      append: (count: number) => void
      reseek: (epoch: number, count: number) => void
    }
  }
}

let appended = 0
window.harness = {
  accumulator,
  ui,
  viewport,
  resetViewport: () => {
    viewport.getState().reset()
    void viewport.persist.clearStorage()
  },
  seed: (count, withLive = true) => {
    appended = count
    accumulator.apply(synthFrame(1, 0, count, withLive))
  },
  append: (count) => {
    accumulator.apply(synthFrame(1, appended, count, true))
    appended += count
  },
  reseek: (epoch, count) => {
    appended = count
    accumulator.apply(synthFrame(epoch, 0, count, true))
  },
}

function Harness() {
  const theme = useRemuxTheme()
  const colors = useMemo(() => {
    void theme
    return chartColors()
  }, [theme])
  const offsetSeconds = useMemo(
    () => etOffsetSeconds(Number(SESSION_START_NS / 1_000_000n)),
    []
  )
  const layers = useMemo(
    () =>
      [accumulator]
        .filter((a) => kindOf(a.spec) in CHART_LAYERS)
        .map((a) =>
          CHART_LAYERS[kindOf(a.spec)]({
            spec: a.spec,
            accumulator: a,
            offsetSeconds,
            colors,
            ui,
            viewport,
          })
        ),
    [colors, offsetSeconds]
  )
  return (
    <div className="flex h-full flex-col bg-background text-foreground">
      <main className="lens-safe-top flex min-h-0 flex-1 flex-col">
        <div className="relative min-h-0 flex-1">
          <ChartSurface
            key={theme}
            layers={layers}
            colors={colors}
            className="h-full"
          />
          <ChartReadout
            ui={ui}
            title={`${SYMBOL} · ${INTERVAL}`}
            clock={FAKE_CLOCK}
            clockReceivedAt={FAKE_CLOCK_RECEIVED_AT}
          />
        </div>
      </main>
      {/* Stand-in for the host action bar's footprint, carrying the
      conditional chart actions like the real bar does. */}
      <div
        style={{ height: 88 }}
        className="flex shrink-0 items-start gap-1.5 border-t border-border bg-card px-3 py-2"
      >
        <JumpToLatestAction ui={ui} viewport={viewport} />
      </div>
    </div>
  )
}

createRoot(document.getElementById("root")!).render(<Harness />)
