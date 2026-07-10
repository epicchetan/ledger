import {
  useCallback,
  useEffect,
  useMemo,
} from "react"
import { updateHostTab } from "@remux/viewer-kit/host"
import { toast } from "sonner"

import { Button } from "@/components/ui/button"
import {
  ChartSurface,
  useRemuxTheme,
} from "@/features/replay/chart/chart-surface"
import { CHART_LAYERS, kindOf } from "@/features/replay/chart/layers"
import { ChartLegend } from "@/features/replay/chart/overlays"
import { chartColors } from "@/features/replay/chart/theme"
import { etOffsetSeconds, nsToSeconds } from "@/features/replay/chart/time"
import { ChartUi } from "@/features/replay/chart/ui-state"
import { ReplayActionBar } from "@/features/replay/replay-action-bar"
import { REPLAY_RESOURCE_KIND, replayResourceId } from "@/features/replay/route"
import type { SessionClosedReason } from "@/features/replay/types"
import { useReplaySession } from "@/features/replay/use-replay-session"
import { cn } from "@/lib/utils"

interface ReplayProps {
  resumeSessionId: string | null
  rawId: string
  marketDay: string
  symbol: string
  onExit: () => void
}

// Snapshotted once at module load for the degenerate no-bounds offset fallback
// (see the offset memo); keeps that memo render-pure — the value only picks
// EST vs EDT, and a raw with no market day has no session to be wrong about.
const MODULE_LOAD_MS = Date.now()

// The interval drawn on the chart. Replay requests only this projection until
// an interval picker returns; keeping an unused 1s projection warm made reload
// hydration transfer an entire day of headless bars through the mobile bridge.
const ACTIVE_BAR_SPEC = "bars:1m"

// The interval suffix shown in the legend title (e.g. "1m").
const INTERVAL_LABEL = ACTIVE_BAR_SPEC.slice(ACTIVE_BAR_SPEC.indexOf(":") + 1)

export function Replay({
  resumeSessionId,
  rawId,
  marketDay,
  symbol,
  onExit,
}: ReplayProps) {
  const session = useReplaySession(rawId, resumeSessionId)
  const {
    phase,
    open,
    error,
    endedReason,
    clock,
    clockReceivedAt,
    cursor,
    deliveryState,
  } = session
  const { projections, controls } = session
  const openSessionId = open?.sessionId ?? null
  const routedMarketDay = open?.marketDay ?? marketDay

  // Host tab resource metadata is the reloadable viewer route. Remux rebuilds
  // the native WebView from this URL, so persist the exact resume handle and
  // enough display identity to mount Replay before any client storage exists.
  useEffect(() => {
    if (!openSessionId) return
    void updateHostTab({
      resourceKind: REPLAY_RESOURCE_KIND,
      resourceId: replayResourceId({
        sessionId: openSessionId,
        rawId,
        marketDay: routedMarketDay,
        symbol,
      }),
      status: "Replay",
      title: `${symbol} · ${routedMarketDay}`,
    }).catch((error) => {
      console.warn("[replay] failed to persist host replay route", error)
    })
  }, [openSessionId, rawId, routedMarketDay, symbol])

  // One overlay store per page, fed by the attached bars layer and read by the
  // chart overlays (legend, clock, jump-to-live, auto-scale).
  const ui = useMemo(() => new ChartUi(), [])
  // Report the slider's mid-drag position to the clock overlay's scrub preview.
  const handleScrub = useCallback(
    (ms: number | null) => ui.setScrubMs(ms),
    [ui]
  )

  const theme = useRemuxTheme()
  // Canvas colors are resolved from the live computed styles; the theme string
  // is the trigger to re-resolve them, not a direct input.
  const colors = useMemo(() => {
    void theme
    return chartColors()
  }, [theme])

  // One offset per session: an ES session (18:00 → 17:00 ET) never spans a DST
  // transition — those land 02:00 ET Sunday, when the market is closed — so the
  // offset at session start holds throughout. Null bounds are a degenerate raw;
  // fall back to the module-load instant.
  const offsetSeconds = useMemo(() => {
    const atMs = open?.sessionStartNs
      ? nsToSeconds(open.sessionStartNs) * 1000
      : MODULE_LOAD_MS
    return etOffsetSeconds(atMs)
  }, [open])

  // Drawable = a registry hit; the bars kind additionally collapses to the
  // single drawn interval. The layers drive the chart's series outside React.
  const layers = useMemo(
    () =>
      projections
        .filter((accumulator) => {
          const kind = kindOf(accumulator.spec)
          if (!(kind in CHART_LAYERS)) return false
          return kind === "bars" ? accumulator.spec === ACTIVE_BAR_SPEC : true
        })
        .map((accumulator) =>
          CHART_LAYERS[kindOf(accumulator.spec)]({
            spec: accumulator.spec,
            accumulator,
            offsetSeconds,
            colors,
            ui,
          })
        ),
    [colors, offsetSeconds, projections, ui]
  )

  // Open failure (object gone, feed build error): toast and fall back to days.
  useEffect(() => {
    if (phase !== "error") return
    toast.error("Replay session failed", {
      description: error ?? "The ledger session could not be opened.",
    })
    onExit()
  }, [phase, error, onExit])

  return (
    <div className="flex h-full flex-col bg-background text-foreground">
      <main className="lens-safe-top flex min-h-0 flex-1 flex-col">
        {/* The wrapper is the positioning context so the absolute overlays and
        the floating banner respect the safe-area padding on `main`. */}
        <div className="relative min-h-0 flex-1">
          <ChartSurface
            key={theme}
            layers={layers}
            colors={colors}
            className="h-full"
          />
          <ChartLegend
            ui={ui}
            title={`${symbol} · ${INTERVAL_LABEL}`}
            clock={clock}
            clockReceivedAt={clockReceivedAt}
          />
          {phase === "ended" ? (
            <EndedBanner
              reason={endedReason}
              onExit={onExit}
              className="absolute inset-x-0 top-0 z-10 m-2"
            />
          ) : null}
        </div>
      </main>

      <ReplayActionBar
        ui={ui}
        clock={clock}
        clockReceivedAt={clockReceivedAt}
        cursor={cursor}
        startNs={open?.sessionStartNs ?? null}
        endNs={open?.sessionEndNs ?? null}
        marketDay={open?.marketDay ?? marketDay}
        symbol={symbol}
        deliveryState={deliveryState}
        disabled={phase !== "live"}
        onExit={onExit}
        onPlay={controls.play}
        onPause={controls.pause}
        onSpeed={controls.setSpeed}
        onSeek={controls.seek}
        onScrub={handleScrub}
      />
    </div>
  )
}

// The terminal "session ended" banner. Floats over the chart (positioned by the
// caller) rather than stacking above it, so the full-bleed surface stays intact.
function EndedBanner({
  reason,
  onExit,
  className,
}: {
  reason: SessionClosedReason | null
  onExit: () => void
  className?: string
}) {
  const detail =
    reason === "replaced"
      ? "A newer replay session took over this feed."
      : "The ledger closed this session."
  return (
    <div
      className={cn(
        "flex flex-col gap-2 rounded-lg border border-destructive/40 bg-destructive/10 p-3",
        className
      )}
    >
      <div className="text-sm font-semibold text-foreground">Session ended</div>
      <p className="text-xs leading-5 text-muted-foreground">{detail}</p>
      <div>
        <Button type="button" variant="outline" size="sm" onClick={onExit}>
          Back to days
        </Button>
      </div>
    </div>
  )
}
