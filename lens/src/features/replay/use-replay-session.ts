import { useEffect, useMemo, useRef, useState } from "react"
import { subscribeHostResume } from "@remux/viewer-kit/host"

import { BarsAccumulator } from "@/features/replay/accumulator"
import {
  attachSession,
  closeSession,
  fetchSessionBars,
  fetchSessionStatus,
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
  SessionAttachSnapshot,
  SessionClosedReason,
  SessionOpenResult,
} from "@/features/replay/types"

// Which projections a replay session requests. A product decision per
// projection kind, not something the UI discovers — headless specs that feed
// stat panels would join this list without joining any chart registry.
const REPLAY_SPECS = ["bars:1m"]

// Initial attach/open hydration and host-resume revalidation buffer the push
// stream while a pull snapshot is in flight. Keep the newest events if a
// pathological burst reaches the cap; monotonic frame versions and gap
// backfill make a dropped older event harmless.
const EVENT_BUFFER_CAP = 4096
const RESUME_STORAGE_KEY = "lens.replay.session.v1"

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

interface StoredReplaySession {
  sessionId: string
  rawId: string
  projections: string[]
}

function loadStoredSession(rawId: string): string | null {
  try {
    const raw = window.sessionStorage.getItem(RESUME_STORAGE_KEY)
    if (!raw) return null
    const parsed: unknown = JSON.parse(raw)
    if (
      typeof parsed !== "object" ||
      parsed === null ||
      !("sessionId" in parsed) ||
      typeof parsed.sessionId !== "string" ||
      !("rawId" in parsed) ||
      typeof parsed.rawId !== "string" ||
      !("projections" in parsed) ||
      !Array.isArray(parsed.projections) ||
      !parsed.projections.every((spec) => typeof spec === "string")
    ) {
      window.sessionStorage.removeItem(RESUME_STORAGE_KEY)
      return null
    }
    if (
      parsed.rawId !== rawId ||
      parsed.projections.length !== REPLAY_SPECS.length ||
      parsed.projections.some((spec, index) => spec !== REPLAY_SPECS[index])
    ) {
      window.sessionStorage.removeItem(RESUME_STORAGE_KEY)
      return null
    }
    return parsed.sessionId
  } catch {
    return null
  }
}

function storeSession(sessionId: string, rawId: string): void {
  const stored: StoredReplaySession = {
    sessionId,
    rawId,
    projections: REPLAY_SPECS.slice(),
  }
  try {
    window.sessionStorage.setItem(RESUME_STORAGE_KEY, JSON.stringify(stored))
  } catch {
    // The host route still covers native reload; only the browser-only fallback
    // is unavailable.
  }
}

function clearStoredSession(sessionId: string): void {
  try {
    const raw = window.sessionStorage.getItem(RESUME_STORAGE_KEY)
    if (!raw) return
    const parsed: unknown = JSON.parse(raw)
    if (
      typeof parsed === "object" &&
      parsed !== null &&
      "sessionId" in parsed &&
      parsed.sessionId === sessionId
    ) {
      window.sessionStorage.removeItem(RESUME_STORAGE_KEY)
    }
  } catch {
    window.sessionStorage.removeItem(RESUME_STORAGE_KEY)
  }
}

// Owns one session's whole lifecycle: subscribe → open → stream, and
// close-on-unmount. Rendered clock state comes from authoritative server
// snapshots (push notifications plus attach/status pulls); controls submit and
// wait for one of those snapshots, never flip state locally.
export function useReplaySession(
  rawId: string,
  routedSessionId: string | null = null
): ReplaySession {
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
    let buffering = true
    let established = false
    let resyncing = false
    let resumeRequested = false
    let closed = false
    let resumeCandidate = routedSessionId ?? loadStoredSession(rawId)
    let latestClock: Clock | null = null
    let latestCursor: Cursor | null = null
    sessionIdRef.current = null
    // Until initial hydration completes, notifications are buffered as
    // id-filtered thunks. The same barrier is reused after a Remux host resume,
    // when iOS suspension or a socket reconnect may have dropped events.
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
      if (latestClock && clock.revision <= latestClock.revision) return
      latestClock = clock
      setClockState({ clock, receivedAt: performance.now() })
    }
    const seedClock = (clock: Clock) => {
      if (latestClock && clock.revision < latestClock.revision) return
      latestClock = clock
      setClockState({ clock, receivedAt: performance.now() })
    }
    const applyCursor = (id: string, next: Cursor) => {
      if (id !== sessionIdRef.current) return
      if (!isNewerCursor(next, latestCursor)) return
      latestCursor = next
      setCursor(next)
    }
    const seedCursor = (next: Cursor) => {
      if (isOlderCursor(next, latestCursor)) return
      latestCursor = next
      setCursor(next)
    }
    const applyFrame = (frame: BarsFrame) => {
      if (frame.sessionId !== sessionIdRef.current) return
      projectionsBySpec.get(frame.spec)?.ingest(frame)
    }
    const applyClosed = (id: string, reason: SessionClosedReason) => {
      if (id !== sessionIdRef.current) return
      closed = true
      setEndedReason(reason)
      setPhase("ended")
    }

    const route = (deliver: () => void) => {
      if (buffering || sessionIdRef.current === null) {
        if (buffer.length === EVENT_BUFFER_CAP) buffer.shift()
        buffer.push(deliver)
        return
      }
      deliver()
    }

    const drainBuffer = () => {
      buffering = false
      const pending = buffer.splice(0)
      for (const deliver of pending) deliver()
    }

    const unsubscribe = subscribeSessionEvents({
      clock: (event) => route(() => applyClock(event.sessionId, event.clock)),
      cursor: (event) =>
        route(() => applyCursor(event.sessionId, event.cursor)),
      barsFrame: (frame) => route(() => applyFrame(frame)),
      closed: (event) =>
        route(() => applyClosed(event.sessionId, event.reason)),
    })

    // Only an exact id persisted by this webview authorizes resume. A fresh
    // days→replay navigation has no token and opens immediately; a stale token
    // is a typed miss, while a real attach failure is surfaced rather than
    // destructively replacing an ambiguous active session.
    const establish = async (): Promise<{
      result: SessionOpenResult
      attached: SessionAttachSnapshot | null
    }> => {
      if (resumeCandidate) {
        const outcome = await attachSession(
          resumeCandidate,
          rawId,
          REPLAY_SPECS
        )
        if (outcome.attached) {
          return {
            result: { ...outcome.session, replaced: null },
            attached: outcome.session,
          }
        }
        clearStoredSession(resumeCandidate)
        resumeCandidate = null
      }
      if (cancelled) throw new Error("Replay session establishment cancelled")
      return {
        result: await openSession(rawId, REPLAY_SPECS),
        attached: null,
      }
    }

    // Cache history stays server-side, but a new JS context still needs one
    // authoritative frame. Pull bars first, then status so the running clock is
    // anchored near the end of hydration; buffered pushes newer than either
    // snapshot are replayed afterward and stale ones are rejected by version.
    const hydrate = async (
      id: string,
      fallback: SessionAttachSnapshot | null = null
    ) => {
      buffering = true
      const frames = await Promise.all(
        projections.map((accumulator) =>
          fetchSessionBars(id, accumulator.spec, 0)
        )
      )
      if (cancelled || id !== sessionIdRef.current) return
      for (const frame of frames) {
        projectionsBySpec.get(frame.spec)?.ingest(frame)
      }

      const status = await fetchSessionStatus(id)
      if (cancelled || id !== sessionIdRef.current) return
      seedClock(status.clock)
      const cursor = status.feed.cursor ?? fallback?.cursor ?? null
      if (cursor) seedCursor(cursor)
    }

    const requestResync = () => {
      if (cancelled) return
      // Initial hydration already covers resume signals delivered during page
      // bootstrap. Only coalesce another pass once a live resync is in flight.
      if (!established) return
      if (resyncing) {
        resumeRequested = true
        return
      }
      const id = sessionIdRef.current
      if (!id) return
      resumeRequested = false
      resyncing = true
      buffering = true
      void attachSession(id, rawId, REPLAY_SPECS)
        .then(async (outcome) => {
          if (!outcome.attached) {
            throw new Error("The replay session is no longer active")
          }
          setOpen({ ...outcome.session, replaced: null })
          seedClock(outcome.session.clock)
          if (outcome.session.cursor) seedCursor(outcome.session.cursor)
          await hydrate(id, outcome.session)
          if (!cancelled) drainBuffer()
        })
        .catch((err) => {
          if (cancelled) return
          drainBuffer()
          if (closed) return
          setError(errorMessage(err))
          setPhase("error")
        })
        .finally(() => {
          resyncing = false
          if (resumeRequested && !cancelled) requestResync()
        })
    }

    const unsubscribeResume = subscribeHostResume(() => requestResync())

    void establish()
      .then(async ({ result, attached }) => {
        if (cancelled) {
          clearStoredSession(result.sessionId)
          void closeSession(result.sessionId).catch(() => undefined)
          return
        }
        sessionIdRef.current = result.sessionId
        resumeCandidate = result.sessionId
        storeSession(result.sessionId, rawId)
        setOpen(result)
        if (attached) {
          seedClock(attached.clock)
          if (attached.cursor) seedCursor(attached.cursor)
        } else if (result.sessionStartNs) {
          // Ledger clocks use absolute Unix nanoseconds. A new session starts at
          // zero internally, so commit the market-session start before exposing
          // controls; attach must never perform this seek.
          await seekSession(result.sessionId, result.sessionStartNs)
        }

        await hydrate(result.sessionId, attached)
        if (cancelled || result.sessionId !== sessionIdRef.current) return
        drainBuffer()
        established = true
        if (closed) return
        setPhase("live")
        if (resumeRequested) requestResync()
      })
      .catch((err) => {
        if (cancelled) return
        drainBuffer()
        if (closed) return
        setError(errorMessage(err))
        setPhase("error")
      })

    return () => {
      cancelled = true
      unsubscribe()
      unsubscribeResume()
      const id = sessionIdRef.current
      sessionIdRef.current = null
      // React cleanup is an intentional SPA exit (a document reload destroys
      // the JS context without running it). Clear the capability synchronously
      // before the fire-and-forget close so immediate re-entry always opens.
      const closeId = id ?? resumeCandidate
      if (closeId) {
        clearStoredSession(closeId)
        void closeSession(closeId).catch(() => undefined)
      }
    }
  }, [rawId, routedSessionId, projections, projectionsBySpec])

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

function isNewerCursor(next: Cursor, current: Cursor | null): boolean {
  if (!current) return true
  if (next.epoch !== current.epoch) return next.epoch > current.epoch
  if (next.feedSeq !== current.feedSeq) return next.feedSeq > current.feedSeq
  return next.batchIdx > current.batchIdx
}

function isOlderCursor(next: Cursor, current: Cursor | null): boolean {
  if (!current) return false
  if (next.epoch !== current.epoch) return next.epoch < current.epoch
  if (next.feedSeq !== current.feedSeq) return next.feedSeq < current.feedSeq
  return next.batchIdx < current.batchIdx
}
