import {
  requestIpc,
  subscribeIpcEvents,
  type JsonRpcMessage,
} from "@remux/viewer-kit/ipc"

import { parseProjectionFrame } from "@/features/replay/projection-client"

import type {
  BarsFrame,
  BarsPosition,
  BarsProjectionFrame,
  Clock,
  Cursor,
  ProjectionSubscribeRequest,
  ProjectionSubscribeResult,
  ProjectionWatermark,
  SessionAttachResult,
  SessionClockEvent,
  SessionCloseResult,
  SessionClosedEvent,
  SessionCursorEvent,
  SessionOkResult,
  SessionOpenResult,
  SessionStatus,
} from "@/features/replay/types"

export async function openSession(
  rawId: string,
  projections: string[]
): Promise<SessionOpenResult> {
  return requestIpc<SessionOpenResult>("remux/ledger/session/open", {
    rawId,
    projections,
  })
}

// Reattach by the exact server-issued id persisted across a webview reload.
// A stale id/raw/spec identity returns { attached: false }; real RPC failures
// reject and must not be treated as permission to replace the active session.
export async function attachSession(
  sessionId: string,
  rawId: string,
  projections: string[]
): Promise<SessionAttachResult> {
  return requestIpc<SessionAttachResult>("remux/ledger/session/attach", {
    sessionId,
    rawId,
    projections,
  })
}

export async function closeSession(
  sessionId: string
): Promise<SessionCloseResult> {
  return requestIpc<SessionCloseResult>("remux/ledger/session/close", {
    sessionId,
  })
}

export async function fetchSessionStatus(
  sessionId: string
): Promise<SessionStatus> {
  return requestIpc<SessionStatus>("remux/ledger/session/status", { sessionId })
}

export async function playSession(sessionId: string): Promise<SessionOkResult> {
  return requestIpc<SessionOkResult>("remux/ledger/session/play", { sessionId })
}

export async function pauseSession(
  sessionId: string
): Promise<SessionOkResult> {
  return requestIpc<SessionOkResult>("remux/ledger/session/pause", {
    sessionId,
  })
}

export async function setSessionSpeed(
  sessionId: string,
  speed: number
): Promise<SessionOkResult> {
  return requestIpc<SessionOkResult>("remux/ledger/session/speed", {
    sessionId,
    speed,
  })
}

export async function seekSession(
  sessionId: string,
  sessionNs: string
): Promise<SessionOkResult> {
  return requestIpc<SessionOkResult>("remux/ledger/session/seek", {
    sessionId,
    sessionNs,
  })
}

// Pull backfill: the accumulator calls this to re-sync a spec after a dropped
// notification. The result is a BarsFrame identical in shape to the stream.
export async function fetchSessionBars(
  sessionId: string,
  spec: string,
  from?: number
): Promise<BarsFrame> {
  return requestIpc<BarsFrame>("remux/ledger/session/bars", {
    sessionId,
    spec,
    ...(from === undefined ? {} : { from }),
  })
}

export async function subscribeSessionProjections(
  sessionId: string,
  consumerInstanceId: string,
  projections: ProjectionSubscribeRequest[]
): Promise<ProjectionSubscribeResult> {
  return requestIpc<ProjectionSubscribeResult>(
    "remux/ledger/session/projections/subscribe",
    { sessionId, consumerInstanceId, projections }
  )
}

export async function acknowledgeSessionProjections(
  subscriptionId: string,
  applied: Array<{ spec: string; head: BarsPosition }>
): Promise<SessionOkResult> {
  return requestIpc<SessionOkResult>("remux/ledger/session/projections/ack", {
    subscriptionId,
    applied,
  })
}

export async function demandSessionProjections(
  subscriptionId: string,
  active: boolean,
  requestedMaxFps = 15
): Promise<SessionOkResult> {
  return requestIpc<SessionOkResult>(
    "remux/ledger/session/projections/demand",
    { subscriptionId, active, requestedMaxFps }
  )
}

export async function resyncSessionProjections(
  subscriptionId: string,
  applied: Array<{ spec: string; head: BarsPosition }>,
  reason: string
): Promise<SessionOkResult> {
  return requestIpc<SessionOkResult>(
    "remux/ledger/session/projections/resync",
    { subscriptionId, applied, reason }
  )
}

export interface SessionEventHandlers {
  clock: (event: SessionClockEvent) => void
  cursor: (event: SessionCursorEvent) => void
  projectionFrame: (frame: BarsProjectionFrame) => void
  projectionWatermark: (watermark: ProjectionWatermark) => void
  closed: (event: SessionClosedEvent) => void
}

// Wraps the shared IPC event stream, parsing session notifications defensively
// (malformed → dropped, never thrown — the parseProgressEvent house pattern)
// and fanning them out by kind. Projection- and session-agnostic: the caller
// filters by sessionId, so this can subscribe before `open` resolves.
export function subscribeSessionEvents(handlers: SessionEventHandlers) {
  return subscribeIpcEvents((events) => {
    for (const message of events) {
      const clock = parseClockEvent(message)
      if (clock) {
        handlers.clock(clock)
        continue
      }
      const cursor = parseCursorEvent(message)
      if (cursor) {
        handlers.cursor(cursor)
        continue
      }
      const frame = parseProjectionFrameEvent(message)
      if (frame) {
        handlers.projectionFrame(frame)
        continue
      }
      const watermark = parseProjectionWatermarkEvent(message)
      if (watermark) {
        handlers.projectionWatermark(watermark)
        continue
      }
      const closed = parseClosedEvent(message)
      if (closed) handlers.closed(closed)
    }
  })
}

function parseClockEvent(message: JsonRpcMessage): SessionClockEvent | null {
  if (message.method !== "remux/ledger/session/clock") return null
  if (!isRecord(message.params)) return null
  const { sessionId, clock } = message.params
  if (typeof sessionId !== "string") return null
  const parsed = parseClock(clock)
  return parsed ? { sessionId, clock: parsed } : null
}

function parseCursorEvent(message: JsonRpcMessage): SessionCursorEvent | null {
  if (message.method !== "remux/ledger/session/feed") return null
  if (!isRecord(message.params)) return null
  const { sessionId, cursor } = message.params
  if (typeof sessionId !== "string") return null
  const parsed = parseCursor(cursor)
  return parsed ? { sessionId, cursor: parsed } : null
}

function parseProjectionFrameEvent(
  message: JsonRpcMessage
): BarsProjectionFrame | null {
  if (message.method !== "remux/ledger/session/projections/frame") return null
  return parseProjectionFrame(message.params)
}

function parseProjectionWatermarkEvent(
  message: JsonRpcMessage
): ProjectionWatermark | null {
  if (message.method !== "remux/ledger/session/projections/watermark") {
    return null
  }
  if (!isRecord(message.params)) return null
  const { subscriptionId, sessionGeneration, feed, projections } = message.params
  if (
    typeof subscriptionId !== "string" ||
    !isNumber(sessionGeneration) ||
    !Array.isArray(projections)
  ) {
    return null
  }
  const parsedFeed = feed === null ? null : parseCursor(feed)
  if (feed !== null && !parsedFeed) return null
  const parsedProjections: ProjectionWatermark["projections"] = []
  for (const projection of projections) {
    if (!isRecord(projection) || typeof projection.spec !== "string") return null
    const head = parsePosition(projection.head)
    if (!head) return null
    parsedProjections.push({ spec: projection.spec, head })
  }
  return {
    subscriptionId,
    sessionGeneration,
    feed: parsedFeed,
    projections: parsedProjections,
  }
}

function parseClosedEvent(message: JsonRpcMessage): SessionClosedEvent | null {
  if (message.method !== "remux/ledger/session/closed") return null
  if (!isRecord(message.params)) return null
  const { sessionId, reason } = message.params
  if (typeof sessionId !== "string") return null
  if (reason !== "replaced" && reason !== "closed") return null
  return { sessionId, reason }
}

function parseClock(value: unknown): Clock | null {
  if (!isRecord(value)) return null
  const { mode, speed, sessionNowNs, revision } = value
  if (mode !== "paused" && mode !== "running") return null
  if (
    !isNumber(speed) ||
    typeof sessionNowNs !== "string" ||
    !isNumber(revision)
  ) {
    return null
  }
  return { mode, speed, sessionNowNs, revision }
}

function parseCursor(value: unknown): Cursor | null {
  if (!isRecord(value)) return null
  const { epoch, feedSeq, batchIdx, totalBatches } = value
  const { tsEventNs, nextTsEventNs, ended, catchingUp } = value
  if (
    !isNumber(epoch) ||
    !isNumber(feedSeq) ||
    !isNumber(batchIdx) ||
    !isNumber(totalBatches)
  ) {
    return null
  }
  if (
    !isNullableString(tsEventNs) ||
    !isNullableString(nextTsEventNs) ||
    typeof ended !== "boolean" ||
    typeof catchingUp !== "boolean"
  ) {
    return null
  }
  return {
    epoch,
    feedSeq,
    batchIdx,
    totalBatches,
    tsEventNs,
    nextTsEventNs,
    ended,
    catchingUp,
  }
}

function parsePosition(value: unknown): BarsPosition | null {
  if (!isRecord(value)) return null
  const { epoch, projectionRevision, processedBatches, completedBars } = value
  if (
    !isNumber(epoch) ||
    !isNumber(projectionRevision) ||
    !isNumber(processedBatches) ||
    !isNumber(completedBars)
  ) {
    return null
  }
  return { epoch, projectionRevision, processedBatches, completedBars }
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value)
}

function isNumber(value: unknown): value is number {
  return typeof value === "number" && Number.isFinite(value)
}

function isNullableString(value: unknown): value is string | null {
  return value === null || typeof value === "string"
}
