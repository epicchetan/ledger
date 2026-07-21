import { describe, expect, it } from "vitest"

import { isHostTransportUnavailable } from "@/features/replay/delivery-health"

describe("replay delivery health", () => {
  it("does not label initial host startup as a reconnect", () => {
    expect(isHostTransportUnavailable({ type: "idle" }, false)).toBe(false)
    expect(isHostTransportUnavailable({ type: "connecting" }, false)).toBe(
      false
    )
  })

  it("recognizes transport loss after the replay is established", () => {
    expect(isHostTransportUnavailable({ type: "connecting" }, true)).toBe(true)
    expect(
      isHostTransportUnavailable({ type: "reconnecting", attempt: 2 }, true)
    ).toBe(true)
    expect(isHostTransportUnavailable({ type: "closed" }, true)).toBe(true)
    expect(
      isHostTransportUnavailable({ type: "error", message: "offline" }, true)
    ).toBe(true)
  })

  it("clears transport uncertainty once the host is connected", () => {
    expect(
      isHostTransportUnavailable(
        { type: "connected", cwd: null, generation: 4 },
        true
      )
    ).toBe(false)
  })
})
