import { describe, expect, it } from "vitest"

import {
  centeredTimeViewport,
  defaultTimeViewport,
  logicalRangeEqual,
  timeRangeForViewport,
} from "@/features/replay/chart/viewport-policy"
import type { TimeViewport } from "@/features/replay/chart/viewport-store"

describe("viewport policy", () => {
  it("reproduces the sparse and full default windows", () => {
    expect(timeRangeForViewport(defaultTimeViewport(5), 5)).toEqual({
      from: -25,
      to: 13,
    })
    expect(timeRangeForViewport(defaultTimeViewport(500), 500)).toEqual({
      from: 380,
      to: 508,
    })
  })

  it("keeps a fixed historical range independent of new bars", () => {
    const viewport: TimeViewport = {
      mode: "fixed",
      range: { from: 40, to: 80 },
    }
    expect(timeRangeForViewport(viewport, 100)).toEqual({ from: 40, to: 80 })
    expect(timeRangeForViewport(viewport, 1_000)).toEqual({ from: 40, to: 80 })
  })

  it("centers the latest bar while preserving zoom", () => {
    const fixed: TimeViewport = {
      mode: "fixed",
      range: { from: 40, to: 80 },
    }
    const centered = centeredTimeViewport(fixed, 100)
    expect(centered).toEqual({
      mode: "follow",
      visibleBars: 40,
      latestFraction: 0.5,
    })
    expect(timeRangeForViewport(centered, 100)).toEqual({ from: 79, to: 119 })
    expect(timeRangeForViewport(centered, 101)).toEqual({ from: 80, to: 120 })
  })

  it("uses an epsilon only for interaction noise", () => {
    expect(
      logicalRangeEqual({ from: 10, to: 20 }, { from: 10.0005, to: 20.0005 })
    ).toBe(true)
    expect(
      logicalRangeEqual({ from: 10, to: 20 }, { from: 10.01, to: 20.01 })
    ).toBe(false)
  })
})
