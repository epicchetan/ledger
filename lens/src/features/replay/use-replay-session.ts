import { useEffect, useMemo, useRef, useState } from "react"

import { BarsAccumulator } from "@/features/replay/accumulator"
import {
  closeSession,
  fetchSessionBars,
  openSession,
  pauseSession,
  playSession,
  seekSession,
  setSessionSpeed,
  subscribeSessionEvents,
} from "@/features/replay/api"
import type {
  BarsFrame,
  Clock,
  Cursor,
  SessionClosedReason,
  SessionOpenResult,
} from "@/features/replay/types"

// Which projections a replay session requests. A product decision per
// projection kind, not something the UI discovers — headless specs that feed
// stat panels would join this list without joining any chart registry.
const REPLAY_SPECS = ["bars:1s", "bars:1m"]

// Watchers broadcast initial state on spawn, so a handful of notifications can
// land before `open` resolves; the cap just bounds a pathological backlog.
const PREOPEN_BUFFER_CAP = 256

export type ReplayPhase = "opening" | "live" | "ended" | "error"

export interface ReplayControls {
  play: () => void
  pause: () => void
  setSpeed: (speed: number) => void
  seek: (sessionNs: string) => void
}

export interface ReplaySession {
  phase: ReplayPhase
  open: SessionOpenResult | null
  error: string | null
  endedReason: SessionClosedReason | null
  clock: Clock | null
  // performance.now() when the clock notification landed — the anchor the
  // scrubber extrapolates from while running.
  clockReceivedAt: number | null
  cursor: Cursor | null
  projections: BarsAccumulator[]
  controls: ReplayControls
}

function errorMessage(error: unknown) {
  return error instanceof Error ? error.message : "Unknown error"
}

// Owns one session's whole lifecycle: subscribe → open → stream, and
// close-on-unmount. Rendered clock state comes exclusively from the clock
// stream — controls submit and wait for the notification, never flip state
// locally (see the design doc's "Control semantics").
export function useReplaySession(rawId: string): ReplaySession {
  // The live session id, readable by the long-lived subscription and the
  // controls without a stale render capture. Touched only in the effect and in
  // event handlers, never during render.
  const sessionIdRef = useRef<string | null>(null)

  // Accumulators are created once and outlive individual frames; their backfill
  // is wired from the effect once the session id exists.
  const projections = useMemo(
    () => REPLAY_SPECS.map((spec) => new BarsAccumulator(spec)),
    []
  )
  const projectionsBySpec = useMemo(() => {
    const map = new Map<string, BarsAccumulator>()
    for (const accumulator of projections)
      map.set(accumulator.spec, accumulator)
    return map
  }, [projections])

  const [phase, setPhase] = useState<ReplayPhase>("opening")
  const [open, setOpen] = useState<SessionOpenResult | null>(null)
  const [error, setError] = useState<string | null>(null)
  const [endedReason, setEndedReason] = useState<SessionClosedReason | null>(
    null
  )
  const [clockState, setClockState] = useState<{
    clock: Clock
    receivedAt: number
  } | null>(null)
  const [cursor, setCursor] = useState<Cursor | null>(null)

  useEffect(() => {
    let cancelled = false
    sessionIdRef.current = null
    // Until `open` resolves the id is null and every notification is buffered
    // (as an id-filtered thunk) rather than dropped. The page mounts fresh per
    // raw (App keys it by rawId), so initial state needs no reset here.
    const buffer: Array<() => void> = []

    for (const accumulator of projections) {
      accumulator.setBackfill((spec, from) => {
        const id = sessionIdRef.current
        if (!id) return Promise.reject(new Error("session not open"))
        return fetchSessionBars(id, spec, from)
      })
    }

    const applyClock = (id: string, clock: Clock) => {
      if (id !== sessionIdRef.current) return
      setClockState({ clock, receivedAt: performance.now() })
    }
    const applyCursor = (id: string, next: Cursor) => {
      if (id !== sessionIdRef.current) return
      setCursor(next)
    }
    const applyFrame = (frame: BarsFrame) => {
      if (frame.sessionId !== sessionIdRef.current) return
      projectionsBySpec.get(frame.spec)?.ingest(frame)
    }
    const applyClosed = (id: string, reason: SessionClosedReason) => {
      if (id !== sessionIdRef.current) return
      setEndedReason(reason)
      setPhase("ended")
    }

    const route = (deliver: () => void) => {
      if (sessionIdRef.current === null) {
        if (buffer.length < PREOPEN_BUFFER_CAP) buffer.push(deliver)
        return
      }
      deliver()
    }

    const unsubscribe = subscribeSessionEvents({
      clock: (event) => route(() => applyClock(event.sessionId, event.clock)),
      cursor: (event) =>
        route(() => applyCursor(event.sessionId, event.cursor)),
      barsFrame: (frame) => route(() => applyFrame(frame)),
      closed: (event) =>
        route(() => applyClosed(event.sessionId, event.reason)),
    })

    openSession(rawId, REPLAY_SPECS)
      .then((result) => {
        if (cancelled) {
          // Unmounted before the session opened: close the orphan.
          void closeSession(result.sessionId).catch(() => undefined)
          return
        }
        sessionIdRef.current = result.sessionId
        setOpen(result)
        setPhase("live")
        // Drain the pre-open backlog through the same id-filtered path.
        for (const deliver of buffer) deliver()
        buffer.length = 0
      })
      .catch((err) => {
        if (cancelled) return
        setError(errorMessage(err))
        setPhase("error")
      })

    return () => {
      cancelled = true
      unsubscribe()
      const id = sessionIdRef.current
      sessionIdRef.current = null
      // Fire-and-forget; the server also cleans up on replace.
      if (id) void closeSession(id).catch(() => undefined)
    }
  }, [rawId, projections, projectionsBySpec])

  const controls = useMemo<ReplayControls>(
    () => ({
      play: () => {
        const id = sessionIdRef.current
        if (id) void playSession(id).catch(logControlError("play"))
      },
      pause: () => {
        const id = sessionIdRef.current
        if (id) void pauseSession(id).catch(logControlError("pause"))
      },
      setSpeed: (speed) => {
        const id = sessionIdRef.current
        if (id) void setSessionSpeed(id, speed).catch(logControlError("speed"))
      },
      seek: (sessionNs) => {
        const id = sessionIdRef.current
        if (id) void seekSession(id, sessionNs).catch(logControlError("seek"))
      },
    }),
    []
  )

  return {
    phase,
    open,
    error,
    endedReason,
    clock: clockState?.clock ?? null,
    clockReceivedAt: clockState?.receivedAt ?? null,
    cursor,
    projections,
    controls,
  }
}

function logControlError(control: string) {
  return (error: unknown) => console.warn(`[replay] ${control} failed`, error)
}
