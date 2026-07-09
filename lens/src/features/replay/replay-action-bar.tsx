import { openHostOverview } from "@remux/viewer-kit/host"
import {
  ActionBar,
  ActionButton,
  ActionMenu,
  ActionMenuItem,
} from "@remux/viewer-kit/ui"
import {
  ArrowLeft,
  Check,
  Gauge,
  Loader2,
  PanelRightOpen,
  Pause,
  Play,
} from "lucide-react"
import { useEffect, useMemo, useState } from "react"

import type { Clock, Cursor } from "@/features/replay/types"

// The wire takes any positive float; these are the surfaced presets.
const SPEED_PRESETS = [1, 2, 5, 10, 25, 100]

interface ReplayActionBarProps {
  clock: Clock | null
  clockReceivedAt: number | null
  cursor: Cursor | null
  startNs: string | null
  endNs: string | null
  marketDay: string
  symbol: string
  syncing: boolean
  disabled: boolean
  onExit: () => void
  onPlay: () => void
  onPause: () => void
  onSpeed: (speed: number) => void
  onSeek: (sessionNs: string) => void
}

// The bar carries the whole transport (reference: day-action-bar): a
// full-width seek slider rides its own row above the pinned buttons — tab-out,
// back, play/pause, a speed menu, and the ET clock readout pushed right. Feed
// progress, market day/symbol, and the syncing state ride the bar's native
// status caption — the page content above carries no nav of its own.
export function ReplayActionBar({
  clock,
  clockReceivedAt,
  cursor,
  startNs,
  endNs,
  marketDay,
  symbol,
  syncing,
  disabled,
  onExit,
  onPlay,
  onPause,
  onSpeed,
  onSeek,
}: ReplayActionBarProps) {
  const running = clock?.mode === "running"
  const sessionNowMs = useSessionNowMs(clock, clockReceivedAt)

  // While dragging, the thumb follows the pointer; on release we seek and hand
  // the position back to the (extrapolated) clock stream.
  const [drag, setDrag] = useState<number | null>(null)

  const hasBounds = startNs !== null && endNs !== null
  const positionFraction = useMemo(() => {
    if (sessionNowMs === null || startNs === null || endNs === null) return null
    const startMs = nsToMs(startNs)
    const endMs = nsToMs(endNs)
    if (endMs <= startMs) return null
    return clamp01((sessionNowMs - startMs) / (endMs - startMs))
  }, [sessionNowMs, startNs, endNs])

  const sliderFraction = drag ?? positionFraction ?? 0

  const commitSeek = () => {
    if (drag === null || startNs === null || endNs === null) {
      setDrag(null)
      return
    }
    // Seek never changes mode: scrubbing while playing keeps playing.
    onSeek(fractionToNs(drag, startNs, endNs))
    setDrag(null)
  }

  return (
    <ActionBar
      className="lens-action-bar"
      status={
        <TransportStatus
          marketDay={marketDay}
          symbol={symbol}
          cursor={cursor}
          syncing={syncing}
        />
      }
      left={
        <div className="flex w-full min-w-0 flex-col gap-2.5">
          {hasBounds ? (
            <input
              type="range"
              min={0}
              max={1000}
              step={1}
              value={Math.round(sliderFraction * 1000)}
              disabled={disabled}
              aria-label="Seek session time"
              className="h-1.5 w-full cursor-pointer accent-primary disabled:cursor-default disabled:opacity-50"
              onChange={(event) => setDrag(Number(event.target.value) / 1000)}
              onPointerUp={commitSeek}
              onKeyUp={commitSeek}
              onBlur={() => setDrag(null)}
            />
          ) : null}

          <div className="flex w-full items-center gap-1.5">
            <ActionButton
              icon={<PanelRightOpen aria-hidden="true" />}
              label="Open tabs"
              onClick={() => {
                void openHostOverview({ section: "tabs" })
              }}
            />
            <ActionButton
              icon={<ArrowLeft aria-hidden="true" />}
              label="Back to days"
              onClick={onExit}
            />
            <ActionButton
              icon={
                running ? (
                  <Pause aria-hidden="true" />
                ) : (
                  <Play aria-hidden="true" />
                )
              }
              label={running ? "Pause" : "Play"}
              tone="primary"
              disabled={disabled || !clock}
              onClick={() => (running ? onPause() : onPlay())}
            />
            <ActionMenu
              align="start"
              label="Playback speed"
              disabled={disabled}
              icon={
                clock ? (
                  <span className="font-mono text-xs font-semibold tabular-nums">
                    {formatSpeed(clock.speed)}×
                  </span>
                ) : (
                  <Gauge aria-hidden="true" />
                )
              }
            >
              {SPEED_PRESETS.map((preset) => (
                <ActionMenuItem
                  key={preset}
                  icon={
                    clock?.speed === preset ? (
                      <Check aria-hidden="true" />
                    ) : null
                  }
                  label={`${preset}×`}
                  onSelect={() => onSpeed(preset)}
                />
              ))}
            </ActionMenu>

            <ClockReadout sessionNowMs={sessionNowMs} />
          </div>
        </div>
      }
    />
  )
}

// A readout, not a control: quiet weight, digits in tabular mono, the AM/PM +
// timezone suffix demoted to a caption so nothing here reads as tappable.
function ClockReadout({ sessionNowMs }: { sessionNowMs: number | null }) {
  const parts = sessionNowMs === null ? null : formatEtTime(sessionNowMs)
  return (
    <div className="ml-auto flex shrink-0 items-baseline gap-1.5 pr-1">
      <span className="font-mono text-sm text-foreground/90 tabular-nums">
        {parts?.time ?? "--:--:--"}
      </span>
      {parts ? (
        <span className="text-[0.65rem] font-medium tracking-wide text-muted-foreground">
          {parts.suffix}
        </span>
      ) : null}
    </div>
  )
}

// The bar's native status caption: market day + symbol, feed progress, the
// syncing indicator, and the ended marker — one nowrap line, no floating footer
// in content.
function TransportStatus({
  marketDay,
  symbol,
  cursor,
  syncing,
}: {
  marketDay: string
  symbol: string
  cursor: Cursor | null
  syncing: boolean
}) {
  return (
    <span className="inline-flex items-center justify-center gap-1.5">
      <span>{marketDay}</span>
      <span aria-hidden="true">·</span>
      <span className="font-mono">{symbol}</span>
      <span aria-hidden="true">·</span>
      {cursor ? (
        <span className="tabular-nums">
          batch {cursor.batchIdx.toLocaleString()} /{" "}
          {cursor.totalBatches.toLocaleString()}
        </span>
      ) : (
        <span>feed idle</span>
      )}
      {syncing ? (
        <span className="inline-flex items-center gap-1">
          <span aria-hidden="true">·</span>
          <Loader2 className="size-2 animate-spin" aria-hidden="true" />
          syncing
        </span>
      ) : null}
      {cursor?.ended ? (
        <span className="inline-flex items-center gap-1">
          <span aria-hidden="true">·</span>
          ended
        </span>
      ) : null}
    </span>
  )
}

// Clock notifications arrive only when the clock cell changes, not continuously
// during playback, so session time is extrapolated between them: while running,
// now = sessionNow + (wall - receivedAt) * speed; while paused, sessionNow
// verbatim (no timer). A new notification re-anchors both terms.
function useSessionNowMs(
  clock: Clock | null,
  clockReceivedAt: number | null
): number | null {
  const running = clock?.mode === "running"
  const [wallNow, setWallNow] = useState(() => performance.now())

  useEffect(() => {
    if (!running) return
    const id = window.setInterval(() => setWallNow(performance.now()), 100)
    return () => window.clearInterval(id)
  }, [running])

  return useMemo(() => {
    if (!clock || clockReceivedAt === null) return null
    const baseMs = nsToMs(clock.sessionNowNs)
    if (clock.mode !== "running") return baseMs
    return baseMs + (wallNow - clockReceivedAt) * clock.speed
  }, [clock, clockReceivedAt, wallNow])
}

function formatSpeed(speed: number): string {
  return Number.isInteger(speed) ? speed.toString() : speed.toFixed(2)
}

const ET_TIME = new Intl.DateTimeFormat("en-US", {
  timeZone: "America/New_York",
  hour: "2-digit",
  minute: "2-digit",
  second: "2-digit",
  hour12: true,
  // Short zone name tracks DST (EST/EDT) instead of hardcoding one.
  timeZoneName: "short",
})

function formatEtTime(ms: number): { time: string; suffix: string } {
  const parts = ET_TIME.formatToParts(new Date(ms))
  const part = (type: Intl.DateTimeFormatPartTypes) =>
    parts.find((candidate) => candidate.type === type)?.value ?? ""
  return {
    time: `${part("hour")}:${part("minute")}:${part("second")}`,
    suffix: `${part("dayPeriod")} ${part("timeZoneName")}`.trim(),
  }
}

// ns strings exceed 2^53; divide as BigInt, then work in ms-scale numbers.
function nsToMs(ns: string): number {
  return Number(BigInt(ns) / 1000000n)
}

// fraction in [0,1] → an integer nanosecond string (no exponent) within bounds.
// The session-span diff fits a double comfortably (< ~10^14 ns).
function fractionToNs(
  fraction: number,
  startNs: string,
  endNs: string
): string {
  const start = BigInt(startNs)
  const span = BigInt(endNs) - start
  const offset = BigInt(Math.round(clamp01(fraction) * Number(span)))
  return (start + offset).toString()
}

function clamp01(value: number): number {
  return Math.min(1, Math.max(0, value))
}
