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
  // True while the feed is emitting an already-due prefix after a rebuild.
  // This is diagnostic server state, not viewer loading state.
  catchingUp: boolean
}

export interface BarsStatus {
  spec: string
  epoch: number
  processedBatches: number
  completedBars: number
  revision: number
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

export interface BarsPosition {
  epoch: number
  projectionRevision: number
  processedBatches: number
  completedBars: number
}

export type ProjectionFrameOperation =
  | "snapshot"
  | "append"
  | "replace"
  | "patch"
export type ProjectionFrameReason = "initial" | "cadence" | "resync"

export interface BarsProjectionPayload {
  bars: Bar[]
  live: Bar | null
  status: BarsStatus
}

export interface BarsProjectionFrame {
  subscriptionId: string
  sessionGeneration: number
  spec: string
  kind: "bars"
  schemaVersion: 1
  frameSequence: number
  base: BarsPosition | null
  head: BarsPosition
  operation: ProjectionFrameOperation
  reason: ProjectionFrameReason
  payload: BarsProjectionPayload
}

export interface ProjectionWatermarkEntry {
  spec: string
  head: BarsPosition
}

export interface ProjectionWatermark {
  subscriptionId: string
  sessionGeneration: number
  feed: Cursor | null
  projections: ProjectionWatermarkEntry[]
}

export interface ProjectionSubscribeRequest {
  spec: string
  schemaVersions: number[]
  requestedMaxFps: number
  have: BarsPosition | null
}

export interface ProjectionSubscribeResult {
  subscriptionId: string
  sessionGeneration: number
  leaseMs: number
  projections: Array<{
    spec: string
    kind: string
    schemaVersion: number
    semantics: string
    effectiveMaxFps: number
    resume: "snapshot" | "suffix"
  }>
}

export interface ProjectionUnsubscribeResult {
  unsubscribed: boolean
}

export type ProjectionDeliveryState =
  | "current"
  | "projection_catching_up"
  | "delivery_pending"
  | "viewer_lagging"
  | "resyncing"
  | "disconnected_unknown"

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

export interface SessionSetProjectionsResult {
  sessionId: string
  epoch: number
  projections: SessionProjection[]
  changed: boolean
}

// The open-shaped identity of an already-running session, plus its current
// clock and feed cursor. Attach itself is a discriminated outcome: a stale
// persisted id is an expected miss, while transport/server failures still
// reject the RPC and must not silently replace the session.
export interface SessionAttachSnapshot {
  sessionId: string
  rawId: string
  projections: SessionProjection[]
  marketDay: string | null
  sessionStartNs: string | null
  sessionEndNs: string | null
  clock: Clock
  cursor: Cursor | null
}

export type SessionAttachResult =
  | { attached: false }
  | { attached: true; session: SessionAttachSnapshot }

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
