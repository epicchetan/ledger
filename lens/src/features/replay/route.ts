import { parseRemuxViewerRoute } from "@remux/viewer-kit/route"

import {
  parseReplayViewport,
  type ReplayViewportSnapshot,
} from "@/features/replay/chart/viewport-store"

export const REPLAY_RESOURCE_KIND = "ledgerReplay"

interface ReplayResourceV2 {
  v: 2
  sessionId: string
  rawId: string
  marketDay: string
  symbol: string
  viewport: ReplayViewportSnapshot
}

export interface ReplayRoute {
  sessionId: string
  rawId: string
  marketDay: string
  symbol: string
  viewport: ReplayViewportSnapshot | null
}

// Remux reloads a newly-created native WebView from the host tab's persisted
// URL. Resource metadata is therefore the durable viewer route; Web Storage is
// only an in-page/browser fallback and cannot carry native reload identity.
export function readReplayRoute(url: string): ReplayRoute | null {
  const route = parseRemuxViewerRoute(url)
  if (
    route.resourceKind !== REPLAY_RESOURCE_KIND ||
    route.resourceId === null
  ) {
    return null
  }

  try {
    const value: unknown = JSON.parse(route.resourceId)
    if (
      typeof value !== "object" ||
      value === null ||
      !("v" in value) ||
      (value.v !== 1 && value.v !== 2)
    ) {
      return null
    }
    if (
      !("sessionId" in value) ||
      typeof value.sessionId !== "string" ||
      !("rawId" in value) ||
      typeof value.rawId !== "string" ||
      !("marketDay" in value) ||
      typeof value.marketDay !== "string" ||
      !("symbol" in value) ||
      typeof value.symbol !== "string"
    ) {
      return null
    }
    const viewport =
      value.v === 2 && "viewport" in value
        ? parseReplayViewport(value.viewport)
        : null
    if (value.v === 2 && !viewport) return null
    return {
      sessionId: value.sessionId,
      rawId: value.rawId,
      marketDay: value.marketDay,
      symbol: value.symbol,
      viewport,
    }
  } catch {
    return null
  }
}

export function replayResourceId(route: ReplayRoute): string {
  const resource: ReplayResourceV2 = {
    v: 2,
    sessionId: route.sessionId,
    rawId: route.rawId,
    marketDay: route.marketDay,
    symbol: route.symbol,
    viewport:
      route.viewport ??
      ({ version: 1, time: null, price: { mode: "auto" } } as const),
  }
  return JSON.stringify(resource)
}
