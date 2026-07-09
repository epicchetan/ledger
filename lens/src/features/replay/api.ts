import {
  requestIpc,
  subscribeIpcEvents,
  type JsonRpcMessage,
} from "@remux/viewer-kit/ipc"

import type {
  Bar,
  BarsFrame,
  BarsStatus,
  Clock,
  Cursor,
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

export interface SessionEventHandlers {
  clock: (event: SessionClockEvent) => void
  cursor: (event: SessionCursorEvent) => void
  barsFrame: (frame: BarsFrame) => void
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
      const frame = parseBarsFrame(message)
      if (frame) {
        handlers.barsFrame(frame)
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

function parseBarsFrame(message: JsonRpcMessage): BarsFrame | null {
  if (message.method !== "remux/ledger/session/barsFrame") return null
  return parseFrame(message.params)
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

function parseFrame(value: unknown): BarsFrame | null {
  if (!isRecord(value)) return null
  const { sessionId, spec, epoch, from, bars, total, live, status } = value
  if (typeof sessionId !== "string" || typeof spec !== "string") return null
  if (!isNumber(epoch) || !isNumber(from) || !isNumber(total)) return null
  if (!Array.isArray(bars)) return null
  const parsedBars: Bar[] = []
  for (const bar of bars) {
    // A single malformed bar taints the frame's contiguity, so drop it whole.
    const parsedBar = parseBar(bar)
    if (!parsedBar) return null
    parsedBars.push(parsedBar)
  }
  const parsedStatus = parseBarsStatus(status)
  if (!parsedStatus) return null
  const parsedLive = live === null ? null : parseBar(live)
  if (live !== null && parsedLive === null) return null
  return {
    sessionId,
    spec,
    epoch,
    from,
    bars: parsedBars,
    total,
    live: parsedLive,
    status: parsedStatus,
  }
}

function parseBar(value: unknown): Bar | null {
  if (!isRecord(value)) return null
  const { intervalStartNs, firstTsEventNs, lastTsEventNs } = value
  const { open, high, low, close } = value
  const { volume, buyVolume, sellVolume, tradeCount } = value
  if (
    typeof intervalStartNs !== "string" ||
    typeof firstTsEventNs !== "string" ||
    typeof lastTsEventNs !== "string"
  ) {
    return null
  }
  if (
    !isNumber(open) ||
    !isNumber(high) ||
    !isNumber(low) ||
    !isNumber(close) ||
    !isNumber(volume) ||
    !isNumber(buyVolume) ||
    !isNumber(sellVolume) ||
    !isNumber(tradeCount)
  ) {
    return null
  }
  return {
    intervalStartNs,
    open,
    high,
    low,
    close,
    volume,
    buyVolume,
    sellVolume,
    tradeCount,
    firstTsEventNs,
    lastTsEventNs,
  }
}

function parseBarsStatus(value: unknown): BarsStatus | null {
  if (!isRecord(value)) return null
  const { spec, epoch, processedBatches, completedBars, lastTsEventNs } = value
  if (
    typeof spec !== "string" ||
    !isNumber(epoch) ||
    !isNumber(processedBatches) ||
    !isNumber(completedBars)
  ) {
    return null
  }
  if (!isNullableString(lastTsEventNs)) return null
  return { spec, epoch, processedBatches, completedBars, lastTsEventNs }
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
