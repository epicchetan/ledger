import { describe, expect, it } from "vitest"

import { nextBarChartTime } from "@/features/replay/chart/time"

describe("bar chart time", () => {
  it("retains sub-second event time", () => {
    expect(nextBarChartTime("1700000000123456000", null, 0)).toBeCloseTo(
      1_700_000_000.123456,
      6
    )
  })

  it("creates strictly increasing keys for colliding tick-bar anchors", () => {
    const first = nextBarChartTime("1700000000123456000", null, 0)
    const second = nextBarChartTime("1700000000123456000", first, 0)
    const third = nextBarChartTime("1700000000123456001", second, 0)
    expect(second).toBeGreaterThan(first)
    expect(third).toBeGreaterThan(second)
  })

  it("gives a live bar the same key when recomputed against the same completed tail", () => {
    const completed = nextBarChartTime("1700000000000000000", null, -14_400)
    const liveFirst = nextBarChartTime(
      "1700000000000000000",
      completed,
      -14_400
    )
    const liveUpdate = nextBarChartTime(
      "1700000000000000000",
      completed,
      -14_400
    )
    expect(liveUpdate).toBe(liveFirst)
  })
})
