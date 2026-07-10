import { afterEach, describe, expect, it, vi } from "vitest"

import { createReplayViewportStore } from "@/features/replay/chart/viewport-store"

afterEach(() => {
  vi.unstubAllGlobals()
})

describe("replay viewport store", () => {
  it("persists the viewport and clears it on deliberate exit", () => {
    const storage = memoryStorage()
    vi.stubGlobal("window", { sessionStorage: storage })
    const store = createReplayViewportStore("raw-1", null)

    store.getState().setTime({
      mode: "fixed",
      range: { from: 12, to: 42 },
    })
    expect(storage.length).toBe(1)

    store.persist.clearStorage()
    expect(storage.length).toBe(0)
  })

  it("prefers the routed viewport over an older browser fallback", () => {
    const storage = memoryStorage()
    vi.stubGlobal("window", { sessionStorage: storage })
    const first = createReplayViewportStore("raw-2", null)
    first.getState().setTime({
      mode: "fixed",
      range: { from: 1, to: 11 },
    })

    const routed = {
      version: 1 as const,
      time: {
        mode: "follow" as const,
        visibleBars: 50,
        latestFraction: 0.5,
      },
      price: { mode: "auto" as const },
    }
    const restored = createReplayViewportStore("raw-2", routed)
    expect(restored.getState().viewport).toEqual(routed)
  })

  it("rehydrates a manual price range from the browser fallback", () => {
    const storage = memoryStorage()
    vi.stubGlobal("window", { sessionStorage: storage })
    const first = createReplayViewportStore("raw-3", null)
    first.getState().setPrice({
      mode: "manual",
      range: { from: 5_900.25, to: 6_100.75 },
    })

    const restored = createReplayViewportStore("raw-3", null)
    expect(restored.getState().viewport.price).toEqual({
      mode: "manual",
      range: { from: 5_900.25, to: 6_100.75 },
    })
  })
})

function memoryStorage(): Storage {
  const values = new Map<string, string>()
  return {
    get length() {
      return values.size
    },
    clear: () => values.clear(),
    getItem: (key) => values.get(key) ?? null,
    key: (index) => Array.from(values.keys())[index] ?? null,
    removeItem: (key) => {
      values.delete(key)
    },
    setItem: (key, value) => {
      values.set(key, value)
    },
  }
}
