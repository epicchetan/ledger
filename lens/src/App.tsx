import { useCallback, useState } from "react"

import { Toaster } from "@/components/ui/sonner"
import { Days } from "@/features/days/days"
import type { DayReadiness } from "@/features/days/readiness"
import { Replay } from "@/features/replay/replay"

// Two screens don't justify a router; a view-state switch does. A third screen
// turns this into a router mechanically.
type LensView =
  | { kind: "days" }
  | { kind: "replay"; rawId: string; marketDay: string; symbol: string }

export function App() {
  const [view, setView] = useState<LensView>({ kind: "days" })

  const handleReplay = useCallback((day: DayReadiness) => {
    setView({
      kind: "replay",
      rawId: day.primary.raw.raw.id,
      marketDay: day.marketDay,
      symbol: day.primary.symbol,
    })
  }, [])

  const handleExit = useCallback(() => setView({ kind: "days" }), [])

  return (
    <>
      {view.kind === "days" ? (
        <Days onReplay={handleReplay} />
      ) : (
        <Replay
          key={view.rawId}
          rawId={view.rawId}
          marketDay={view.marketDay}
          symbol={view.symbol}
          onExit={handleExit}
        />
      )}
      <Toaster />
    </>
  )
}

export default App
