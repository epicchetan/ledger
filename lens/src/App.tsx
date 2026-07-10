import { useCallback, useEffect, useState } from "react"
import { updateHostTab } from "@remux/viewer-kit/host"

import { Toaster } from "@/components/ui/sonner"
import { Days } from "@/features/days/days"
import type { DayReadiness } from "@/features/days/readiness"
import type { ReplayViewportSnapshot } from "@/features/replay/chart/viewport-store"
import { Replay } from "@/features/replay/replay"
import { readReplayRoute } from "@/features/replay/route"

// Two pages don't justify a router; a view-state switch does. A third page
// turns this into a router mechanically.
type LensView =
  | { kind: "days" }
  | {
      kind: "replay"
      sessionId: string | null
      rawId: string
      marketDay: string
      symbol: string
      viewport: ReplayViewportSnapshot | null
    }

// Remux's hamburger reload creates a fresh native WebView from the host tab
// URL, so its resource route is authoritative. sessionStorage remains a useful
// browser/in-page fallback but is not expected to survive that native remount.
const VIEW_STORAGE_KEY = "lens.view.v1"

function loadStoredView(): LensView {
  const replayRoute = readReplayRoute(window.location.href)
  if (replayRoute) return { kind: "replay", ...replayRoute }

  try {
    const raw = window.sessionStorage.getItem(VIEW_STORAGE_KEY)
    if (!raw) return { kind: "days" }
    const parsed: unknown = JSON.parse(raw)
    if (
      typeof parsed === "object" &&
      parsed !== null &&
      "kind" in parsed &&
      parsed.kind === "replay" &&
      "rawId" in parsed &&
      typeof parsed.rawId === "string" &&
      "marketDay" in parsed &&
      typeof parsed.marketDay === "string" &&
      "symbol" in parsed &&
      typeof parsed.symbol === "string"
    ) {
      return {
        kind: "replay",
        sessionId: null,
        rawId: parsed.rawId,
        marketDay: parsed.marketDay,
        symbol: parsed.symbol,
        viewport: null,
      }
    }
  } catch {
    // Malformed storage falls through to days.
  }
  return { kind: "days" }
}

export function App() {
  const [view, setView] = useState<LensView>(loadStoredView)

  useEffect(() => {
    try {
      window.sessionStorage.setItem(VIEW_STORAGE_KEY, JSON.stringify(view))
    } catch {
      // The Remux host route still owns native replay restoration.
    }
  }, [view])

  const handleReplay = useCallback((day: DayReadiness) => {
    setView({
      kind: "replay",
      sessionId: null,
      rawId: day.primary.raw.raw.id,
      marketDay: day.marketDay,
      symbol: day.primary.symbol,
      viewport: null,
    })
  }, [])

  const handleExit = useCallback(() => {
    // Clear the host-owned reload route before unmounting Replay. Its cleanup
    // then clears the local capability and closes the server session; immediate
    // re-entry cannot accidentally restore the old chart.
    void updateHostTab({
      resourceId: null,
      resourceKind: null,
      status: null,
      title: "Ledger",
    })
      .catch((error) => {
        console.warn("[replay] failed to clear host replay route", error)
      })
      .finally(() => setView({ kind: "days" }))
  }, [])

  return (
    <>
      {view.kind === "days" ? (
        <Days onReplay={handleReplay} />
      ) : (
        <Replay
          key={view.rawId}
          resumeSessionId={view.sessionId}
          rawId={view.rawId}
          marketDay={view.marketDay}
          symbol={view.symbol}
          initialViewport={view.viewport}
          onExit={handleExit}
        />
      )}
      <Toaster />
    </>
  )
}

export default App
