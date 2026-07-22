import { describe, expect, it } from "vitest"

import { BarsAccumulator } from "@/features/replay/accumulator"
import { parseProjectionFrame } from "@/features/replay/projection-client"
import type { Bar, BarsProjectionFrame } from "@/features/replay/types"

const BAR: Bar = {
  intervalStartNs: "0",
  open: 100,
  high: 101,
  low: 99,
  close: 100,
  volume: 4,
  buyVolume: 2,
  sellVolume: 2,
  tradeCount: 2,
  firstTsEventNs: "10",
  lastTsEventNs: "90",
}

describe("BarsAccumulator rebuild lineage", () => {
  it("atomically replaces old bars with a cadence snapshot from a new epoch", () => {
    const accumulator = new BarsAccumulator("bars:1m")
    accumulator.beginSubscription("subscription-1", 7, false)

    expect(accumulator.apply(frame(1, 0, [BAR]))).toMatchObject({
      kind: "applied",
      immediateAck: true,
    })

    const replacement = { ...BAR, intervalStartNs: "60000000000", close: 105 }
    expect(accumulator.apply(frame(2, 1, [replacement]))).toMatchObject({
      kind: "applied",
      immediateAck: true,
    })
    expect(accumulator.getSnapshot()).toMatchObject({
      epoch: 1,
      bars: [replacement],
      live: null,
    })
  })

  it("does not accept the retired seekFinal wire reason", () => {
    expect(
      parseProjectionFrame({ ...frame(1, 0, [BAR]), reason: "seekFinal" })
    ).toBeNull()
  })
})

function frame(
  frameSequence: number,
  epoch: number,
  bars: Bar[]
): BarsProjectionFrame {
  const head = {
    epoch,
    projectionRevision: frameSequence,
    processedBatches: bars.length,
    completedBars: bars.length,
  }
  return {
    subscriptionId: "subscription-1",
    sessionGeneration: 7,
    spec: "bars:1m",
    kind: "bars",
    schemaVersion: 1,
    frameSequence,
    base: null,
    head,
    operation: "snapshot",
    reason: "cadence",
    payload: {
      bars,
      live: null,
      status: {
        spec: "bars:1m",
        epoch,
        processedBatches: bars.length,
        completedBars: bars.length,
        revision: frameSequence,
        lastTsEventNs: bars.at(-1)?.lastTsEventNs ?? null,
      },
    },
  }
}
