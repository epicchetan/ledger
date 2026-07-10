import type {
  TimeViewport,
  ViewportRange,
} from "@/features/replay/chart/viewport-store"

export const INITIAL_VISIBLE_BARS = 120
export const MIN_VISIBLE_BARS = 30
export const DEFAULT_RIGHT_PAD = 8

export function defaultTimeViewport(barCount: number): TimeViewport {
  const window = Math.min(
    INITIAL_VISIBLE_BARS,
    Math.max(barCount, MIN_VISIBLE_BARS)
  )
  const visibleBars = window + DEFAULT_RIGHT_PAD
  return {
    mode: "follow",
    visibleBars,
    latestFraction: (window - 1) / visibleBars,
  }
}

export function centeredTimeViewport(
  current: TimeViewport | null,
  barCount: number
): TimeViewport {
  return {
    mode: "follow",
    visibleBars: current
      ? visibleBarsOf(current)
      : visibleBarsOf(defaultTimeViewport(barCount)),
    latestFraction: 0.5,
  }
}

export function timeRangeForViewport(
  viewport: TimeViewport,
  barCount: number
): ViewportRange {
  if (viewport.mode === "fixed") return { ...viewport.range }
  const latest = Math.max(0, barCount - 1)
  const from = latest - viewport.visibleBars * viewport.latestFraction
  return { from, to: from + viewport.visibleBars }
}

export function logicalRangeEqual(
  left: ViewportRange,
  right: ViewportRange,
  epsilon = 0.001
): boolean {
  return (
    Math.abs(left.from - right.from) < epsilon &&
    Math.abs(left.to - right.to) < epsilon
  )
}

function visibleBarsOf(viewport: TimeViewport): number {
  return viewport.mode === "follow"
    ? viewport.visibleBars
    : viewport.range.to - viewport.range.from
}
