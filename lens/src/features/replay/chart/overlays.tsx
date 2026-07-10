import { ActionButton } from "@remux/viewer-kit/ui"
import { ChevronRight } from "lucide-react"
import { useCallback, useSyncExternalStore } from "react"

import type { ChartUi } from "@/features/replay/chart/ui-state"
import type { Clock } from "@/features/replay/types"
import {
  formatEtTime,
  useSessionNowMs,
} from "@/features/replay/use-session-clock"
import { cn } from "@/lib/utils"

// The overlays and bar actions subscribe to the ChartUi store, so the frame
// stream re-renders only these components, and only when their snapshot
// changes.

// Top-left readout, two lines: the session clock (scrub preview while the
// slider is dragged, else the extrapolated server clock) followed by the
// `symbol · interval` title, then the O H L C V cells on their own line so
// nothing wraps or collides at phone widths. Values track the crosshair (or
// the forming candle when released), direction-colored from the same tokens
// the candles resolve.
export function ChartLegend({
  ui,
  title,
  clock,
  clockReceivedAt,
}: {
  ui: ChartUi
  title: string
  clock: Clock | null
  clockReceivedAt: number | null
}) {
  const subscribe = useCallback(
    (onChange: () => void) => ui.subscribe(onChange),
    [ui]
  )
  const legend = useSyncExternalStore(
    subscribe,
    useCallback(() => ui.legend, [ui])
  )
  const scrubMs = useSyncExternalStore(
    subscribe,
    useCallback(() => ui.scrubMs, [ui])
  )
  const serverNowMs = useSessionNowMs(clock, clockReceivedAt)
  const scrubbing = scrubMs !== null
  const ms = scrubbing ? scrubMs : serverNowMs
  const parts = ms === null ? null : formatEtTime(ms)
  return (
    <div className="pointer-events-none absolute top-2 left-2 z-10 flex flex-col gap-1.5 font-mono text-[11px] leading-none">
      <span className="flex items-baseline gap-1.5 whitespace-nowrap">
        <span
          className={cn(
            "tabular-nums",
            scrubbing ? "text-foreground" : "text-foreground/90"
          )}
        >
          {parts?.time ?? "--:--:--"}
        </span>
        {parts ? (
          <span className="text-[0.65rem] font-medium tracking-wide text-muted-foreground">
            {parts.suffix}
          </span>
        ) : null}
        <span className="text-foreground/90">{title}</span>
      </span>
      {legend ? (
        <div className="flex gap-3 whitespace-nowrap">
          <LegendCell label="O" value={legend.open.toFixed(2)} up={legend.up} />
          <LegendCell label="H" value={legend.high.toFixed(2)} up={legend.up} />
          <LegendCell label="L" value={legend.low.toFixed(2)} up={legend.up} />
          <LegendCell
            label="C"
            value={legend.close.toFixed(2)}
            up={legend.up}
          />
          <LegendCell
            label="V"
            value={legend.volume.toLocaleString()}
            up={legend.up}
          />
        </div>
      ) : null}
    </div>
  )
}

function LegendCell({
  label,
  value,
  up,
}: {
  label: string
  value: string
  up: boolean
}) {
  return (
    <span className="text-muted-foreground">
      {label}{" "}
      <span className={up ? "text-success" : "text-destructive"}>{value}</span>
    </span>
  )
}

// Action-bar button: returns to the live edge. Rendered only while the user
// has panned off it, so it occupies no bar space otherwise.
export function JumpToLiveAction({ ui }: { ui: ChartUi }) {
  const subscribe = useCallback(
    (onChange: () => void) => ui.subscribe(onChange),
    [ui]
  )
  const atEdge = useSyncExternalStore(
    subscribe,
    useCallback(() => ui.atEdge, [ui])
  )
  if (atEdge) return null
  return (
    <ActionButton
      icon={<ChevronRight aria-hidden="true" />}
      label="Jump to live edge"
      onClick={() => ui.jumpToLive?.()}
    />
  )
}

// Action-bar button: re-fits the price scale. Rendered only once the user has
// dragged the price axis out of auto-fit.
export function AutoScaleAction({ ui }: { ui: ChartUi }) {
  const subscribe = useCallback(
    (onChange: () => void) => ui.subscribe(onChange),
    [ui]
  )
  const autoScale = useSyncExternalStore(
    subscribe,
    useCallback(() => ui.autoScale, [ui])
  )
  if (autoScale) return null
  return (
    <ActionButton
      icon={<span className="font-mono text-xs font-semibold">Auto</span>}
      label="Re-fit price scale"
      onClick={() => ui.resetPriceScale?.()}
    />
  )
}
