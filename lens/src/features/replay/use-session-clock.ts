import { useEffect, useMemo, useState } from "react"

import type { Clock } from "@/features/replay/types"

// Shared session-clock extrapolation: the action bar's slider and the chart's
// clock overlay both read from here. Clock notifications arrive only when the
// clock cell changes, not continuously during playback, so session time is
// extrapolated between them: while running, now = sessionNow + (wall -
// receivedAt) * speed; while paused, sessionNow verbatim (no timer). A new
// notification re-anchors both terms.
export function useSessionNowMs(
  clock: Clock | null,
  clockReceivedAt: number | null
): number | null {
  const running = clock?.mode === "running"
  const [wallNow, setWallNow] = useState(() => performance.now())

  useEffect(() => {
    if (!running) return
    const id = window.setInterval(() => setWallNow(performance.now()), 100)
    return () => window.clearInterval(id)
  }, [running])

  return useMemo(() => {
    if (!clock || clockReceivedAt === null) return null
    const baseMs = nsToMs(clock.sessionNowNs)
    if (clock.mode !== "running") return baseMs
    return baseMs + (wallNow - clockReceivedAt) * clock.speed
  }, [clock, clockReceivedAt, wallNow])
}

const ET_TIME = new Intl.DateTimeFormat("en-US", {
  timeZone: "America/New_York",
  hour: "2-digit",
  minute: "2-digit",
  second: "2-digit",
  hour12: true,
  // Short zone name tracks DST (EST/EDT) instead of hardcoding one.
  timeZoneName: "short",
})

export function formatEtTime(ms: number): { time: string; suffix: string } {
  const parts = ET_TIME.formatToParts(new Date(ms))
  const part = (type: Intl.DateTimeFormatPartTypes) =>
    parts.find((candidate) => candidate.type === type)?.value ?? ""
  return {
    time: `${part("hour")}:${part("minute")}:${part("second")}`,
    suffix: `${part("dayPeriod")} ${part("timeZoneName")}`.trim(),
  }
}

// ns strings exceed 2^53; divide as BigInt, then work in ms-scale numbers.
export function nsToMs(ns: string): number {
  return Number(BigInt(ns) / 1000000n)
}
