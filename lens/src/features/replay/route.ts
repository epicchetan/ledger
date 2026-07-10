import { parseRemuxViewerRoute } from "@remux/viewer-kit/route"

export const REPLAY_RESOURCE_KIND = "ledgerReplay"

interface ReplayResourceV1 {
  v: 1
  sessionId: string
  rawId: string
  marketDay: string
  symbol: string
}

export interface ReplayRoute {
  sessionId: string
  rawId: string
  marketDay: string
  symbol: string
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
      value.v !== 1 ||
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
    return {
      sessionId: value.sessionId,
      rawId: value.rawId,
      marketDay: value.marketDay,
      symbol: value.symbol,
    }
  } catch {
    return null
  }
}

export function replayResourceId(route: ReplayRoute): string {
  const resource: ReplayResourceV1 = { v: 1, ...route }
  return JSON.stringify(resource)
}
