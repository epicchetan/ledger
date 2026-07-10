import { ActionButton } from "@remux/viewer-kit/ui"
import { ChevronRight } from "lucide-react"
import { useStore } from "zustand"

import type { ReplayChartUiStore } from "@/features/replay/chart/ui-store"
import type { ReplayViewportStore } from "@/features/replay/chart/viewport-store"
import type { Clock } from "@/features/replay/types"
import {
  formatEtTime,
  useSessionNowMs,
} from "@/features/replay/use-session-clock"
import { cn } from "@/lib/utils"

// Top-left readout: the session clock (scrub preview while the slider is
// dragged, else the extrapolated server clock) followed by the
// `symbol · interval` title. Projection values belong in opt-in overlays, not
// permanent chart chrome.
export function ChartReadout({
  ui,
  title,
  clock,
  clockReceivedAt,
}: {
  ui: ReplayChartUiStore
  title: string
  clock: Clock | null
  clockReceivedAt: number | null
}) {
  const scrubMs = useStore(ui, (state) => state.scrubMs)
  const serverNowMs = useSessionNowMs(clock, clockReceivedAt)
  const scrubbing = scrubMs !== null
  const ms = scrubbing ? scrubMs : serverNowMs
  const parts = ms === null ? null : formatEtTime(ms)
  return (
    <div className="pointer-events-none absolute top-2 left-2 z-10 font-mono text-[11px] leading-none">
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
    </div>
  )
}

// Action-bar button: opts back into following and centers the newest bar.
// Rendered only while the user has selected a fixed historical viewport.
export function JumpToLatestAction({
  ui,
  viewport,
}: {
  ui: ReplayChartUiStore
  viewport: ReplayViewportStore
}) {
  const followingLatest = useStore(
    viewport,
    (state) => state.viewport.time?.mode !== "fixed"
  )
  const jumpToLatest = useStore(ui, (state) => state.jumpToLatest)
  if (followingLatest) return null
  return (
    <ActionButton
      icon={<ChevronRight aria-hidden="true" />}
      label="Center latest bar"
      onClick={() => jumpToLatest?.()}
    />
  )
}
