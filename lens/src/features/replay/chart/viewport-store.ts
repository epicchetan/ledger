import { createStore } from "zustand/vanilla"
import { createJSONStorage, persist } from "zustand/middleware"

export interface ViewportRange {
  from: number
  to: number
}

export type TimeViewport =
  | {
      mode: "follow"
      visibleBars: number
      latestFraction: number
    }
  | {
      mode: "fixed"
      range: ViewportRange
    }

export type PriceViewport =
  | { mode: "auto" }
  | {
      mode: "manual"
      range: ViewportRange
    }

export interface ReplayViewportSnapshot {
  version: 1
  time: TimeViewport | null
  price: PriceViewport
}

interface ReplayViewportState {
  viewport: ReplayViewportSnapshot
  setTime: (time: TimeViewport) => void
  setPrice: (price: PriceViewport) => void
  reset: () => void
}

const DEFAULT_VIEWPORT: ReplayViewportSnapshot = {
  version: 1,
  time: null,
  price: { mode: "auto" },
}

export function createReplayViewportStore(
  rawId: string,
  routedViewport: ReplayViewportSnapshot | null
) {
  const initialViewport = cloneViewport(routedViewport ?? DEFAULT_VIEWPORT)
  return createStore<ReplayViewportState>()(
    persist(
      (set) => ({
        viewport: initialViewport,
        setTime: (time) =>
          set((state) =>
            timeViewportEqual(state.viewport.time, time)
              ? state
              : { viewport: { ...state.viewport, time } }
          ),
        setPrice: (price) =>
          set((state) =>
            priceViewportEqual(state.viewport.price, price)
              ? state
              : { viewport: { ...state.viewport, price } }
          ),
        reset: () => set({ viewport: cloneViewport(DEFAULT_VIEWPORT) }),
      }),
      {
        name: `lens.replay.viewport.v1:${rawId}`,
        storage: createJSONStorage(() => window.sessionStorage),
        partialize: (state) => ({ viewport: state.viewport }),
        version: 1,
        // The native Remux route is newer and more authoritative than the
        // browser fallback. A routed viewport therefore wins hydration; an
        // ordinary in-WebView reload can still recover from sessionStorage.
        merge: (persisted, current) => {
          const persistedViewport = parsePersistedViewport(persisted)
          return {
            ...current,
            viewport: routedViewport
              ? cloneViewport(routedViewport)
              : (persistedViewport ?? current.viewport),
          }
        },
      }
    )
  )
}

export type ReplayViewportStore = ReturnType<typeof createReplayViewportStore>

export function parseReplayViewport(
  value: unknown
): ReplayViewportSnapshot | null {
  if (!isRecord(value) || value.version !== 1) return null
  const time = parseTimeViewport(value.time)
  if (value.time !== null && !time) return null
  const price = parsePriceViewport(value.price)
  if (!price) return null
  return { version: 1, time, price }
}

function parsePersistedViewport(value: unknown): ReplayViewportSnapshot | null {
  if (!isRecord(value)) return null
  return parseReplayViewport(value.viewport)
}

function parseTimeViewport(value: unknown): TimeViewport | null {
  if (value === null) return null
  if (!isRecord(value) || typeof value.mode !== "string") return null
  if (value.mode === "follow") {
    if (
      !isPositiveFinite(value.visibleBars) ||
      !isFiniteNumber(value.latestFraction) ||
      value.latestFraction < 0 ||
      value.latestFraction > 1
    ) {
      return null
    }
    return {
      mode: "follow",
      visibleBars: value.visibleBars,
      latestFraction: value.latestFraction,
    }
  }
  if (value.mode === "fixed") {
    const range = parseRange(value.range)
    return range ? { mode: "fixed", range } : null
  }
  return null
}

function parsePriceViewport(value: unknown): PriceViewport | null {
  if (!isRecord(value) || typeof value.mode !== "string") return null
  if (value.mode === "auto") return { mode: "auto" }
  if (value.mode === "manual") {
    const range = parseRange(value.range)
    return range ? { mode: "manual", range } : null
  }
  return null
}

function parseRange(value: unknown): ViewportRange | null {
  if (!isRecord(value)) return null
  if (
    !isFiniteNumber(value.from) ||
    !isFiniteNumber(value.to) ||
    value.to <= value.from
  ) {
    return null
  }
  return { from: value.from, to: value.to }
}

function timeViewportEqual(
  left: TimeViewport | null,
  right: TimeViewport
): boolean {
  if (!left || left.mode !== right.mode) return false
  if (left.mode === "follow" && right.mode === "follow") {
    return (
      left.visibleBars === right.visibleBars &&
      left.latestFraction === right.latestFraction
    )
  }
  return (
    left.mode === "fixed" &&
    right.mode === "fixed" &&
    rangeEqual(left.range, right.range)
  )
}

function priceViewportEqual(
  left: PriceViewport,
  right: PriceViewport
): boolean {
  if (left.mode !== right.mode) return false
  if (left.mode === "auto" && right.mode === "auto") return true
  return (
    left.mode === "manual" &&
    right.mode === "manual" &&
    rangeEqual(left.range, right.range)
  )
}

function rangeEqual(left: ViewportRange, right: ViewportRange): boolean {
  return left.from === right.from && left.to === right.to
}

function cloneViewport(
  viewport: ReplayViewportSnapshot
): ReplayViewportSnapshot {
  return {
    version: 1,
    time:
      viewport.time?.mode === "follow"
        ? { ...viewport.time }
        : viewport.time
          ? { mode: "fixed", range: { ...viewport.time.range } }
          : null,
    price:
      viewport.price.mode === "auto"
        ? { mode: "auto" }
        : { mode: "manual", range: { ...viewport.price.range } },
  }
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value)
}

function isFiniteNumber(value: unknown): value is number {
  return typeof value === "number" && Number.isFinite(value)
}

function isPositiveFinite(value: unknown): value is number {
  return isFiniteNumber(value) && value > 0
}
