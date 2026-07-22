import { describe, expect, it } from "vitest"

import {
  isTickBarsSpec,
  projectionLabel,
  projectionMenuLabel,
  TICK_BAR_SPECS,
  TIME_BAR_SPECS,
} from "@/features/replay/projection-spec"

describe("replay projection specs", () => {
  it("defines the surfaced time and tick choices", () => {
    expect(TIME_BAR_SPECS).toEqual([
      "bars:1m",
      "bars:5m",
      "bars:15m",
      "bars:1h",
    ])
    expect(TICK_BAR_SPECS).toEqual([
      "bars:100t",
      "bars:500t",
      "bars:1000t",
      "bars:2000t",
    ])
  })

  it("keeps the tick suffix on the trigger but removes it under the column heading", () => {
    expect(projectionLabel("bars:500t")).toBe("500t")
    expect(projectionMenuLabel("bars:500t")).toBe("500")
    expect(projectionMenuLabel("bars:15m")).toBe("15m")
  })

  it("recognizes only canonical tick bars", () => {
    expect(isTickBarsSpec("bars:100t")).toBe(true)
    expect(isTickBarsSpec("bars:1m")).toBe(false)
    expect(isTickBarsSpec("ticks:100")).toBe(false)
  })
})
