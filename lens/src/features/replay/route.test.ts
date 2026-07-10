import { describe, expect, it } from "vitest"

import {
  readReplayRoute,
  REPLAY_RESOURCE_KIND,
  replayResourceId,
  type ReplayRoute,
} from "@/features/replay/route"

const ROUTE: ReplayRoute = {
  sessionId: "session-1",
  rawId: "raw-1",
  marketDay: "2026-03-12",
  symbol: "ESH6",
  viewport: {
    version: 1,
    time: {
      mode: "fixed",
      range: { from: 100.25, to: 180.25 },
    },
    price: {
      mode: "manual",
      range: { from: 5_980.25, to: 6_025.75 },
    },
  },
}

describe("replay route", () => {
  it("round-trips the v2 viewport", () => {
    expect(readReplayRoute(viewerUrl(replayResourceId(ROUTE)))).toEqual(ROUTE)
  })

  it("accepts a v1 identity with no viewport", () => {
    const v1 = JSON.stringify({
      v: 1,
      sessionId: ROUTE.sessionId,
      rawId: ROUTE.rawId,
      marketDay: ROUTE.marketDay,
      symbol: ROUTE.symbol,
    })
    expect(readReplayRoute(viewerUrl(v1))).toEqual({
      ...ROUTE,
      viewport: null,
    })
  })

  it("rejects malformed v2 ranges", () => {
    const malformed = JSON.stringify({
      v: 2,
      sessionId: ROUTE.sessionId,
      rawId: ROUTE.rawId,
      marketDay: ROUTE.marketDay,
      symbol: ROUTE.symbol,
      viewport: {
        version: 1,
        time: { mode: "fixed", range: { from: 20, to: 10 } },
        price: { mode: "auto" },
      },
    })
    expect(readReplayRoute(viewerUrl(malformed))).toBeNull()
  })

  it("rejects another resource kind", () => {
    const params = new URLSearchParams({
      remuxResourceKind: "other",
      remuxResourceId: replayResourceId(ROUTE),
    })
    expect(readReplayRoute(`https://lens.test/?${params}`)).toBeNull()
  })
})

function viewerUrl(resourceId: string): string {
  const params = new URLSearchParams({
    remuxResourceKind: REPLAY_RESOURCE_KIND,
    remuxResourceId: resourceId,
  })
  return `https://lens.test/?${params}`
}
