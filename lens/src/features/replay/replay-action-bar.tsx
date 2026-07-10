import { openHostOverview, reloadHostView } from "@remux/viewer-kit/host"
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
  Menu,
  PanelRightOpen,
  Pause,
  Play,
  RefreshCw,
} from "lucide-react"
import { useMemo, useState } from "react"

import {
  AutoScaleAction,
  JumpToLiveAction,
} from "@/features/replay/chart/overlays"
import type { ChartUi } from "@/features/replay/chart/ui-state"
import type { Clock, Cursor } from "@/features/replay/types"
import { nsToMs, useSessionNowMs } from "@/features/replay/use-session-clock"

// The wire takes any positive float; these are the surfaced presets.
const SPEED_PRESETS = [1, 2, 5, 10, 25, 100]

interface ReplayActionBarProps {
  ui: ChartUi
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
  onScrub: (sessionMs: number | null) => void
}

// The bar carries the whole transport (reference: day-action-bar): a
// full-width seek slider rides its own row above the pinned buttons. Left group
// is tab-out, reload, back, then the conditional chart actions (jump-to-live,
// price re-fit — their own components with narrow store snapshots, so the bar
// itself never re-renders on frame churn); play/pause sits rightmost (right
// thumb) with the speed menu beside it. The session clock lives on the chart
// now, not here. Feed progress, market day/symbol, and the syncing state ride
// the bar's native status caption — the page content above carries no nav of
// its own.
export function ReplayActionBar({
  ui,
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
  onScrub,
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
      onScrub(null)
      return
    }
    // Seek never changes mode: scrubbing while playing keeps playing.
    onSeek(fractionToNs(drag, startNs, endNs))
    setDrag(null)
    onScrub(null)
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
              className="h-7 w-full cursor-pointer accent-primary disabled:cursor-default disabled:opacity-50"
              onChange={(event) => {
                const fraction = Number(event.target.value) / 1000
                setDrag(fraction)
                if (startNs !== null && endNs !== null) {
                  onScrub(fractionToMs(fraction, startNs, endNs))
                }
              }}
              onPointerUp={commitSeek}
              onKeyUp={commitSeek}
              onBlur={() => {
                setDrag(null)
                onScrub(null)
              }}
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
            <ActionMenu
              align="start"
              icon={<Menu aria-hidden="true" />}
              label="Replay menu"
            >
              <ActionMenuItem
                icon={<RefreshCw aria-hidden="true" />}
                label="Reload viewer"
                onSelect={() => {
                  void reloadHostView()
                }}
              />
              <ActionMenuItem
                icon={<ArrowLeft aria-hidden="true" />}
                label="Back to days"
                onSelect={onExit}
              />
            </ActionMenu>
            <JumpToLiveAction ui={ui} />
            <AutoScaleAction ui={ui} />
            {/* Speed panel: the default 232px width dwarfs the short preset
            labels; !important because viewer-kit's stylesheet is unlayered
            and would otherwise win over the utility. */}
            <ActionMenu
              align="end"
              className="ml-auto"
              panelClassName="!w-auto !min-w-24"
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
          </div>
        </div>
      }
    />
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

function formatSpeed(speed: number): string {
  return Number.isInteger(speed) ? speed.toString() : speed.toFixed(2)
}

// fraction in [0,1] → session-ms within bounds, for the mid-drag scrub preview.
function fractionToMs(
  fraction: number,
  startNs: string,
  endNs: string
): number {
  const startMs = nsToMs(startNs)
  const endMs = nsToMs(endNs)
  return startMs + clamp01(fraction) * (endMs - startMs)
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
