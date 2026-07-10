import { useEffect, useMemo, useRef, useState } from "react"
import { subscribeHostResume } from "@remux/viewer-kit/host"

import { BarsAccumulator } from "@/features/replay/accumulator"
import {
  acknowledgeSessionProjections,
  attachSession,
  closeSession,
  demandSessionProjections,
  fetchSessionStatus,
  openSession,
  pauseSession,
  playSession,
  resyncSessionProjections,
  seekSession,
  setSessionSpeed,
  subscribeSessionEvents,
  subscribeSessionProjections,
} from "@/features/replay/api"
import { getBarsProjectionClient } from "@/features/replay/projection-client"
import type {
  BarsPosition,
  BarsProjectionFrame,
  Clock,
  Cursor,
  ProjectionDeliveryState,
  ProjectionWatermark,
  SessionAttachSnapshot,
  SessionClosedReason,
  SessionOpenResult,
} from "@/features/replay/types"

const REPLAY_SPECS = ["bars:1m"]
const REQUESTED_MAX_FPS = 15
const EVENT_BUFFER_CAP = 4096
const RESUME_STORAGE_KEY = "lens.replay.session.v1"
const ACK_INTERVAL_MS = 750
const INITIAL_PROJECTION_TIMEOUT_MS = 5_000
const CONSUMER_INSTANCE_ID = createConsumerInstanceId()

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
  clockReceivedAt: number | null
  cursor: Cursor | null
  deliveryState: ProjectionDeliveryState
  projections: BarsAccumulator[]
  controls: ReplayControls
}

interface StoredReplaySession {
  sessionId: string
  rawId: string
  projections: string[]
}

export function useReplaySession(
  rawId: string,
  routedSessionId: string | null = null
): ReplaySession {
  const sessionIdRef = useRef<string | null>(null)
  const projections = useMemo(() => {
    const client = getBarsProjectionClient()
    return REPLAY_SPECS.map((spec) => client.createAccumulator(spec))
  }, [])
  const projectionsBySpec = useMemo(() => {
    const map = new Map<string, BarsAccumulator>()
    for (const accumulator of projections) map.set(accumulator.spec, accumulator)
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
  const [deliveryState, setDeliveryState] =
    useState<ProjectionDeliveryState>("delivery_pending")

  useEffect(() => {
    let cancelled = false
    let buffering = true
    let established = false
    let hostResyncing = false
    let resumeRequested = false
    let closed = false
    let resumeCandidate = routedSessionId ?? loadStoredSession(rawId)
    let latestClock: Clock | null = null
    let latestCursor: Cursor | null = null
    let subscriptionId: string | null = null
    let sessionGeneration: number | null = null
    let demandTimer: ReturnType<typeof setInterval> | null = null
    let ackTimer: ReturnType<typeof setTimeout> | null = null
    let projectionResyncing = false
    let resyncFrameApplied = false
    let latestWatermark: ProjectionWatermark | null = null
    const pendingAcks = new Map<string, BarsPosition>()
    const buffer: Array<() => void> = []
    sessionIdRef.current = null

    const applyClock = (id: string, next: Clock) => {
      if (id !== sessionIdRef.current) return
      if (latestClock && next.revision <= latestClock.revision) return
      latestClock = next
      setClockState({ clock: next, receivedAt: performance.now() })
    }
    const seedClock = (next: Clock) => {
      if (latestClock && next.revision < latestClock.revision) return
      latestClock = next
      setClockState({ clock: next, receivedAt: performance.now() })
    }
    const seedCursor = (next: Cursor) => {
      if (isOlderCursor(next, latestCursor)) return
      latestCursor = next
      setCursor(next)
    }

    const currentApplied = () =>
      projections.flatMap((accumulator) => {
        const head = accumulator.getAppliedPosition()
        return head ? [{ spec: accumulator.spec, head }] : []
      })

    const requestProjectionResync = (reason: string) => {
      if (cancelled || projectionResyncing || !subscriptionId) return
      projectionResyncing = true
      resyncFrameApplied = false
      setDeliveryState("resyncing")
      void resyncSessionProjections(
        subscriptionId,
        currentApplied(),
        reason
      )
        .then(() => {
          projectionResyncing = false
          if (!cancelled && resyncFrameApplied) {
            setDeliveryState(
              latestWatermark
                ? deriveDeliveryState(latestWatermark, projectionsBySpec)
                : "current"
            )
          }
        })
        .catch((err) => {
          if (!cancelled) {
            console.warn("[replay] projection resync failed", err)
            setDeliveryState("disconnected_unknown")
          }
        })
        .finally(() => {
          projectionResyncing = false
        })
    }

    const flushAcks = () => {
      if (ackTimer) {
        clearTimeout(ackTimer)
        ackTimer = null
      }
      if (!subscriptionId || pendingAcks.size === 0) return
      const id = subscriptionId
      const applied = Array.from(pendingAcks, ([spec, head]) => ({ spec, head }))
      pendingAcks.clear()
      void acknowledgeSessionProjections(id, applied).catch((err) => {
        if (!cancelled) {
          console.warn("[replay] projection acknowledgment failed", err)
          requestProjectionResync("ack_rejected")
        }
      })
    }

    const queueAck = (
      spec: string,
      head: BarsPosition,
      immediate: boolean
    ) => {
      pendingAcks.set(spec, head)
      if (immediate) {
        queueMicrotask(flushAcks)
      } else if (!ackTimer) {
        ackTimer = setTimeout(flushAcks, ACK_INTERVAL_MS)
      }
    }

    const applyProjectionFrame = (frame: BarsProjectionFrame) => {
      if (
        frame.subscriptionId !== subscriptionId ||
        frame.sessionGeneration !== sessionGeneration
      ) {
        return
      }
      const accumulator = projectionsBySpec.get(frame.spec)
      if (!accumulator) return
      const result = accumulator.apply(frame)
      if (result.kind === "base_mismatch") {
        requestProjectionResync("base_mismatch")
      } else if (result.kind === "applied" || result.kind === "duplicate") {
        queueAck(frame.spec, result.head, result.immediateAck)
        if (projectionResyncing && frame.reason === "resync") {
          resyncFrameApplied = true
        }
        if (!projectionResyncing) {
          setDeliveryState(
            latestWatermark
              ? deriveDeliveryState(latestWatermark, projectionsBySpec)
              : "current"
          )
        }
      } else {
        requestProjectionResync("malformed_frame")
      }
    }

    const applyWatermark = (watermark: ProjectionWatermark) => {
      if (
        watermark.subscriptionId !== subscriptionId ||
        watermark.sessionGeneration !== sessionGeneration
      ) {
        return
      }
      latestWatermark = watermark
      if (watermark.feed) seedCursor(watermark.feed)
      if (!projectionResyncing) {
        setDeliveryState(deriveDeliveryState(watermark, projectionsBySpec))
      }
    }

    const applyClosed = (id: string, reason: SessionClosedReason) => {
      if (id !== sessionIdRef.current) return
      closed = true
      setEndedReason(reason)
      setDeliveryState("current")
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
      for (const deliver of buffer.splice(0)) deliver()
    }

    const unsubscribe = subscribeSessionEvents({
      clock: (event) => route(() => applyClock(event.sessionId, event.clock)),
      // The legacy feed notification can run at compute/chunk cadence during a
      // large seek. Projection watermarks carry the same authoritative cursor
      // at presentation cadence, keeping React work independent of replay
      // throughput while the server continues at full speed.
      cursor: () => undefined,
      projectionFrame: (frame) => route(() => applyProjectionFrame(frame)),
      projectionWatermark: (watermark) =>
        route(() => applyWatermark(watermark)),
      closed: (event) =>
        route(() => applyClosed(event.sessionId, event.reason)),
    })

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

    const hydrateStatus = async (
      id: string,
      fallback: SessionAttachSnapshot | null = null
    ) => {
      const status = await fetchSessionStatus(id)
      if (cancelled || id !== sessionIdRef.current) return
      seedClock(status.clock)
      const nextCursor = status.feed.cursor ?? fallback?.cursor ?? null
      if (nextCursor) seedCursor(nextCursor)
    }

    const sendDemand = () => {
      if (!subscriptionId) return
      void demandSessionProjections(
        subscriptionId,
        document.visibilityState !== "hidden",
        REQUESTED_MAX_FPS
      ).catch(() => {
        if (!cancelled) setDeliveryState("disconnected_unknown")
      })
    }

    const installDemandLease = (leaseMs: number) => {
      if (demandTimer) clearInterval(demandTimer)
      const interval = Math.max(5_000, Math.min(15_000, leaseMs / 2))
      demandTimer = setInterval(sendDemand, interval)
      sendDemand()
    }

    const subscribeProjectionStream = async (
      id: string,
      retainAppliedState: boolean
    ) => {
      const response = await subscribeSessionProjections(
        id,
        CONSUMER_INSTANCE_ID,
        projections.map((accumulator) => ({
          spec: accumulator.spec,
          schemaVersions: [1],
          requestedMaxFps: REQUESTED_MAX_FPS,
          have: retainAppliedState ? accumulator.getAppliedPosition() : null,
        }))
      )
      for (const projection of response.projections) {
        if (projection.kind !== "bars" || projection.schemaVersion !== 1) {
          throw new Error(
            `Unsupported projection schema ${projection.kind}:${projection.schemaVersion}`
          )
        }
      }
      subscriptionId = response.subscriptionId
      sessionGeneration = response.sessionGeneration
      pendingAcks.clear()
      for (const accumulator of projections) {
        accumulator.beginSubscription(
          response.subscriptionId,
          response.sessionGeneration,
          retainAppliedState
        )
      }
      latestWatermark = null
      setDeliveryState("delivery_pending")
      installDemandLease(response.leaseMs)
    }

    const resumeProjectionStream = async (id: string) => {
      if (!subscriptionId) {
        await subscribeProjectionStream(id, true)
        return
      }
      try {
        await demandSessionProjections(
          subscriptionId,
          true,
          REQUESTED_MAX_FPS
        )
        await resyncSessionProjections(
          subscriptionId,
          currentApplied(),
          "resume_after_disconnect"
        )
      } catch {
        await subscribeProjectionStream(id, true)
      }
    }

    const requestHostResync = () => {
      if (cancelled || !established) return
      if (hostResyncing) {
        resumeRequested = true
        return
      }
      const id = sessionIdRef.current
      if (!id) return
      hostResyncing = true
      resumeRequested = false
      buffering = true
      setDeliveryState("disconnected_unknown")
      void attachSession(id, rawId, REPLAY_SPECS)
        .then(async (outcome) => {
          if (!outcome.attached) {
            throw new Error("The replay session is no longer active")
          }
          setOpen({ ...outcome.session, replaced: null })
          seedClock(outcome.session.clock)
          if (outcome.session.cursor) seedCursor(outcome.session.cursor)
          await hydrateStatus(id, outcome.session)
          await resumeProjectionStream(id)
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
          hostResyncing = false
          if (resumeRequested && !cancelled) requestHostResync()
        })
    }

    const onVisibilityChange = () => sendDemand()
    document.addEventListener("visibilitychange", onVisibilityChange)
    const unsubscribeResume = subscribeHostResume(requestHostResync)

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
          await seekSession(result.sessionId, result.sessionStartNs)
        }

        await subscribeProjectionStream(result.sessionId, false)
        await hydrateStatus(result.sessionId, attached)
        if (cancelled || result.sessionId !== sessionIdRef.current) return
        drainBuffer()
        await waitForProjectionSnapshots(projections)
        established = true
        if (closed || cancelled) return
        setPhase("live")
        if (resumeRequested) requestHostResync()
      })
      .catch((err) => {
        if (cancelled) return
        drainBuffer()
        if (closed) return
        setError(errorMessage(err))
        setDeliveryState("disconnected_unknown")
        setPhase("error")
      })

    return () => {
      cancelled = true
      unsubscribe()
      unsubscribeResume()
      document.removeEventListener("visibilitychange", onVisibilityChange)
      if (demandTimer) clearInterval(demandTimer)
      if (ackTimer) clearTimeout(ackTimer)
      flushAcks()
      const id = sessionIdRef.current
      sessionIdRef.current = null
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
    deliveryState,
    projections,
    controls,
  }
}

function deriveDeliveryState(
  watermark: ProjectionWatermark,
  accumulators: Map<string, BarsAccumulator>
): ProjectionDeliveryState {
  if (watermark.feed?.catchingUp) return "projection_catching_up"
  for (const projection of watermark.projections) {
    if (
      watermark.feed &&
      (projection.head.epoch < watermark.feed.epoch ||
        (projection.head.epoch === watermark.feed.epoch &&
          projection.head.processedBatches < watermark.feed.batchIdx))
    ) {
      return "projection_catching_up"
    }
    const applied = accumulators.get(projection.spec)?.getAppliedPosition() ?? null
    if (!applied) return "delivery_pending"
    if (
      applied.epoch < projection.head.epoch ||
      (applied.epoch === projection.head.epoch &&
        applied.projectionRevision < projection.head.projectionRevision)
    ) {
      return "viewer_lagging"
    }
  }
  return "current"
}

function waitForProjectionSnapshots(
  accumulators: BarsAccumulator[]
): Promise<void> {
  if (accumulators.every((accumulator) => accumulator.getAppliedPosition())) {
    return Promise.resolve()
  }
  return new Promise((resolve, reject) => {
    const unsubscribers: Array<() => void> = []
    const finish = () => {
      if (!accumulators.every((accumulator) => accumulator.getAppliedPosition())) {
        return
      }
      clearTimeout(timeout)
      for (const unsubscribe of unsubscribers) unsubscribe()
      resolve()
    }
    const timeout = setTimeout(() => {
      for (const unsubscribe of unsubscribers) unsubscribe()
      reject(new Error("Timed out waiting for projection snapshots"))
    }, INITIAL_PROJECTION_TIMEOUT_MS)
    for (const accumulator of accumulators) {
      unsubscribers.push(accumulator.subscribe(finish))
    }
    finish()
  })
}

function createConsumerInstanceId(): string {
  if (typeof crypto !== "undefined" && "randomUUID" in crypto) {
    return crypto.randomUUID()
  }
  return `consumer-${Date.now()}-${Math.random().toString(36).slice(2)}`
}

function errorMessage(error: unknown): string {
  return error instanceof Error ? error.message : "Unknown error"
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
    // The exact native route remains the primary reload capability.
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

function logControlError(control: string) {
  return (error: unknown) => console.warn(`[replay] ${control} failed`, error)
}

function isOlderCursor(next: Cursor, current: Cursor | null): boolean {
  if (!current) return false
  if (next.epoch !== current.epoch) return next.epoch < current.epoch
  if (next.feedSeq !== current.feedSeq) return next.feedSeq < current.feedSeq
  return next.batchIdx < current.batchIdx
}
