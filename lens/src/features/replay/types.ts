// Wire types for the ledger session transport (JSON-RPC over the viewer-kit IPC
// bridge). Every …Ns value is a decimal string: nanosecond timestamps exceed
// 2^53, so they stay strings on the wire and convert to numbers only at the
// display/math edge.

export type ClockMode = "paused" | "running"

export interface Clock {
  mode: ClockMode
  speed: number
  sessionNowNs: string
  revision: number
}

export interface Cursor {
  epoch: number
  feedSeq: number
  batchIdx: number
  totalBatches: number
  tsEventNs: string | null
  nextTsEventNs: string | null
  ended: boolean
  // True while the feed is emitting a backlog of already-due batches (post-seek
  // catch-up): projection output is converging, not current. Authoritative
  // "syncing" signal — no lag inference needed while this is set.
  catchingUp: boolean
}

export interface BarsStatus {
  spec: string
  epoch: number
  processedBatches: number
  completedBars: number
  lastTsEventNs: string | null
}

export interface Bar {
  intervalStartNs: string
  open: number
  high: number
  low: number
  close: number
  volume: number
  buyVolume: number
  sellVolume: number
  tradeCount: number
  firstTsEventNs: string
  lastTsEventNs: string
}

export interface BarsFrame {
  sessionId: string
  spec: string
  epoch: number
  from: number
  bars: Bar[]
  total: number
  live: Bar | null
  status: BarsStatus
}

export interface SessionProjection {
  spec: string
}

// The three session-bounds fields are null for raws with no market day.
export interface SessionOpenResult {
  sessionId: string
  rawId: string
  projections: SessionProjection[]
  replaced: string | null
  marketDay: string | null
  sessionStartNs: string | null
  sessionEndNs: string | null
}

export interface SessionCloseResult {
  closed: boolean
}

export interface SessionOkResult {
  ok: true
}

export interface SessionFeedStatus {
  componentStatus: string
  status: Record<string, unknown> | null
  cursor: Cursor | null
}

export interface SessionProjectionStatus {
  spec: string
  status: BarsStatus | null
  completedBars: number
  liveBar: boolean
}

export interface SessionStatus {
  sessionId: string
  rawId: string
  clock: Clock
  feed: SessionFeedStatus
  projections: SessionProjectionStatus[]
}

export type SessionClosedReason = "replaced" | "closed"

export interface SessionClockEvent {
  sessionId: string
  clock: Clock
}

export interface SessionCursorEvent {
  sessionId: string
  cursor: Cursor
}

export interface SessionClosedEvent {
  sessionId: string
  reason: SessionClosedReason
}

// Prices ride the wire as integer ES ticks; one tick is 0.25 index points
// (exact in floating point — quarter points). A per-instrument constant until a
// second instrument moves it to the wire.
export const TICK_SIZE = 0.25
