import { Loader2 } from "lucide-react"
import { useCallback, useEffect, useState, useSyncExternalStore } from "react"
import { toast } from "sonner"

import { Button } from "@/components/ui/button"
import type { BarsAccumulator } from "@/features/replay/accumulator"
import { ReplayActionBar } from "@/features/replay/replay-action-bar"
import {
  TICK_SIZE,
  type Cursor,
  type SessionClosedReason,
} from "@/features/replay/types"
import { useReplaySession } from "@/features/replay/use-replay-session"

interface ReplayProps {
  rawId: string
  marketDay: string
  symbol: string
  onExit: () => void
}

// The lag term must be continuously true this long before it may surface as
// "syncing", and clears the instant it goes false — this kills flicker from the
// status and cursor notifications briefly disagreeing during normal playback.
// `catchingUp` bypasses it entirely (authoritative, both edges immediate).
const SYNC_SUSTAIN_MS = 250

export function Replay({ rawId, marketDay, symbol, onExit }: ReplayProps) {
  const session = useReplaySession(rawId)
  const { phase, open, error, endedReason, clock, clockReceivedAt, cursor } =
    session
  const { projections, controls } = session

  // Open failure (object gone, feed build error): toast and fall back to days.
  useEffect(() => {
    if (phase !== "error") return
    toast.error("Replay session failed", {
      description: error ?? "The ledger session could not be opened.",
    })
    onExit()
  }, [phase, error, onExit])

  const syncing = useAnyProjectionSyncing(projections, cursor)

  return (
    <div className="flex h-full flex-col bg-background text-foreground">
      <main className="lens-safe-top lens-scroll min-h-0 flex-1 space-y-3 overflow-y-auto px-3 py-3">
        {phase === "ended" ? (
          <EndedBanner reason={endedReason} onExit={onExit} />
        ) : null}

        <div className="grid gap-3">
          {projections.map((accumulator) => (
            <ProjectionCard
              key={accumulator.spec}
              accumulator={accumulator}
              cursor={cursor}
            />
          ))}
        </div>
      </main>

      <ReplayActionBar
        clock={clock}
        clockReceivedAt={clockReceivedAt}
        cursor={cursor}
        startNs={open?.sessionStartNs ?? null}
        endNs={open?.sessionEndNs ?? null}
        marketDay={open?.marketDay ?? marketDay}
        symbol={symbol}
        syncing={syncing}
        disabled={phase !== "live"}
        onExit={onExit}
        onPlay={controls.play}
        onPause={controls.pause}
        onSpeed={controls.setSpeed}
        onSeek={controls.seek}
      />
    </div>
  )
}

// The Phase 2 chart slot: one card per spec showing the accumulator's live
// state — proof the frame stream runs end to end. Each card subscribes to its
// own accumulator so frame churn never re-renders the page.
function ProjectionCard({
  accumulator,
  cursor,
}: {
  accumulator: BarsAccumulator
  cursor: Cursor | null
}) {
  const snapshot = useSyncExternalStore(
    accumulator.subscribe,
    accumulator.getSnapshot
  )
  const { status, live } = snapshot
  const processed = status?.processedBatches ?? null
  const rawLag =
    processed !== null && cursor !== null && processed !== cursor.batchIdx
  // Same rule as the page: `catchingUp` shows immediately, the lag term only
  // after it sustains, so cards never flicker during normal playback.
  const sustainedLag = useSustained(rawLag)
  const syncing = (cursor?.catchingUp ?? false) || sustainedLag
  const completed = status?.completedBars ?? snapshot.bars.length

  return (
    <div className="flex flex-col gap-2.5 rounded-lg border border-border bg-card/45 p-3">
      <div className="flex items-center justify-between gap-2">
        <span className="font-mono text-sm font-semibold text-foreground">
          {snapshot.spec}
        </span>
        {syncing ? (
          <span className="flex items-center gap-1 text-[0.7rem] text-muted-foreground">
            <Loader2 className="size-3 animate-spin" aria-hidden="true" />
            syncing
          </span>
        ) : live ? (
          <span className="flex items-center gap-1.5 text-[0.7rem] text-muted-foreground">
            <span
              className="size-1.5 rounded-full bg-success"
              aria-hidden="true"
            />
            live
          </span>
        ) : null}
      </div>

      <div className="grid grid-cols-2 gap-2">
        <Stat label="Completed bars" value={completed.toLocaleString()} />
        <Stat label="Live bar" value={live ? "streaming" : "—"} />
        <Stat
          label="Processed batches"
          value={processed === null ? "—" : processed.toLocaleString()}
        />
        <Stat label="Epoch" value={snapshot.epoch?.toString() ?? "—"} />
      </div>

      {live ? (
        <div className="flex items-center gap-3 border-t border-border/60 pt-2 font-mono text-[0.7rem] text-muted-foreground">
          <span>
            last{" "}
            <span className="text-foreground tabular-nums">
              {formatPrice(live.close)}
            </span>
          </span>
          <span>
            vol{" "}
            <span className="text-foreground tabular-nums">
              {live.volume.toLocaleString()}
            </span>
          </span>
        </div>
      ) : null}
    </div>
  )
}

function Stat({ label, value }: { label: string; value: string }) {
  return (
    <div className="flex flex-col gap-0.5 rounded-md border border-border/60 bg-background/40 px-2 py-1.5">
      <span className="text-[0.65rem] tracking-wide text-muted-foreground uppercase">
        {label}
      </span>
      <span className="font-mono text-sm font-semibold text-foreground tabular-nums">
        {value}
      </span>
    </div>
  )
}

function EndedBanner({
  reason,
  onExit,
}: {
  reason: SessionClosedReason | null
  onExit: () => void
}) {
  const detail =
    reason === "replaced"
      ? "A newer replay session took over this feed."
      : "The ledger closed this session."
  return (
    <div className="flex flex-col gap-2 rounded-lg border border-destructive/40 bg-destructive/10 p-3">
      <div className="text-sm font-semibold text-foreground">Session ended</div>
      <p className="text-xs leading-5 text-muted-foreground">{detail}</p>
      <div>
        <Button type="button" variant="outline" size="sm" onClick={onExit}>
          Back to days
        </Button>
      </div>
    </div>
  )
}

// Syncing = `cursor.catchingUp` (authoritative, undebounced) OR any projection
// lagging the feed cursor sustained past SYNC_SUSTAIN_MS. Lag is aggregated
// across accumulators through a single external-store subscription whose
// snapshot is the boolean itself, so frame churn only re-renders the page when
// the lag state actually flips — not on every frame.
function useAnyProjectionSyncing(
  projections: BarsAccumulator[],
  cursor: Cursor | null
): boolean {
  const rawLag = useProjectionLag(projections, cursor)
  const sustainedLag = useSustained(rawLag)
  return (cursor?.catchingUp ?? false) || sustainedLag
}

function useProjectionLag(
  projections: BarsAccumulator[],
  cursor: Cursor | null
): boolean {
  const subscribe = useCallback(
    (onChange: () => void) => {
      const unsubscribes = projections.map((accumulator) =>
        accumulator.subscribe(onChange)
      )
      return () => {
        for (const unsubscribe of unsubscribes) unsubscribe()
      }
    },
    [projections]
  )
  const getSnapshot = useCallback(() => {
    if (!cursor) return false
    return projections.some((accumulator) => {
      const processed = accumulator.getSnapshot().status?.processedBatches
      return processed != null && processed !== cursor.batchIdx
    })
  }, [projections, cursor])
  return useSyncExternalStore(subscribe, getSnapshot)
}

// True only after `active` has held continuously for SYNC_SUSTAIN_MS; false the
// instant it clears. The false edge resets during render (React's adjust-state-
// during-render pattern) so it never waits a commit and a re-activation always
// re-waits the full delay; the effect only schedules the true edge.
function useSustained(active: boolean): boolean {
  const [elapsed, setElapsed] = useState(false)
  const [tracked, setTracked] = useState(active)
  if (active !== tracked) {
    setTracked(active)
    if (!active) setElapsed(false)
  }
  useEffect(() => {
    if (!active) return
    const id = window.setTimeout(() => setElapsed(true), SYNC_SUSTAIN_MS)
    return () => window.clearTimeout(id)
  }, [active])
  return active && elapsed
}

function formatPrice(ticks: number): string {
  return (ticks * TICK_SIZE).toFixed(2)
}
