import type { MarketDayStatus } from "@/features/data-center/types"

export type SessionPlaybackState = "paused" | "playing" | "ended"
export type SessionStatus = "idle" | "connecting" | "opening" | "ready" | "playing" | "ended" | "error"

export interface ProjectionSpec {
  id: string
  version: number
  params: Record<string, unknown>
}

export interface ProjectionKey {
  id: string
  version: number
  params_hash: string
}

export interface ProjectionFrameStamp {
  session_id: string
  replay_dataset_id: string
  generation: number
  projection_key: ProjectionKey
  output_schema: { name: string }
  feed_seq: number
  feed_ts_ns: string
  source_first_ts_ns?: string
  source_last_ts_ns?: string
  batch_idx: number
  cursor_ts_ns: string
  source_view: string | null
  temporal_policy: string
  produced_at_ns: string
  sequence: number
}

export interface ProjectionFrame {
  stamp: ProjectionFrameStamp
  op: "replace" | "patch" | "append" | "snapshot"
  payload: unknown
}

export interface SessionMarketDay {
  id: string
  root: string
  contract_symbol: string
  market_date: string
  timezone: string
  data_start_ns: string
  data_end_ns: string
  rth_start_ns: string
  rth_end_ns: string
  status: MarketDayStatus
  metadata_json: unknown
}

export interface SessionSnapshot {
  session_id: string
  replay_dataset_id: string
  market_day: SessionMarketDay
  feed_seq: number
  feed_ts_ns: string
  next_feed_ts_ns?: string
  source_first_ts_ns?: string
  source_last_ts_ns?: string
  batch_idx: number
  total_batches: number
  playback: SessionPlaybackState
  speed: number
  book_checksum: string
  bbo: unknown
  frame_count: number
  fill_count: number
}

export type SessionClientMessage =
  | {
      type: "open_session"
      request_id?: string
      session_id?: string
      session_kind: "replay"
      symbol: string
      market_date: string
      start_ts_ns?: string
      feed: {
        kind: "replay"
        mode: "exchange_truth"
        visibility: "truth"
      }
    }
  | { type: "subscribe_projection"; request_id?: string; projection: ProjectionSpec }
  | { type: "unsubscribe_projection"; request_id?: string; subscription_id: number }
  | { type: "advance"; request_id?: string; batches: number }
  | { type: "play"; request_id?: string; speed: number; pump_interval_ms?: number; budget_batches?: number }
  | { type: "pause"; request_id?: string }
  | { type: "set_speed"; request_id?: string; speed: number }
  | { type: "seek"; request_id?: string; feed_ts_ns: string }
  | { type: "snapshot"; request_id?: string }
  | { type: "close_session"; request_id?: string }

export type SessionFrameCause = "advance" | "playback_tick" | "seek"

export type SessionServerMessage =
  | { type: "server_hello"; protocol: string; version: number; capabilities: string[] }
  | { type: "session_opening"; request_id?: string; session_kind: "replay"; symbol: string; market_date: string }
  | { type: "session_opened"; request_id?: string; session: SessionSnapshot }
  | {
      type: "projection_subscribed"
      request_id?: string
      subscription_id: number
      projection_key: ProjectionKey
      projection_key_display: string
      session: SessionSnapshot
      frames: ProjectionFrame[]
    }
  | { type: "projection_unsubscribed"; request_id?: string; subscription_id: number }
  | {
      type: "session_frame_batch"
      request_id?: string
      cause: SessionFrameCause
      applied_batches: number
      budget_exhausted: boolean
      behind: boolean
      session: SessionSnapshot
      frames: ProjectionFrame[]
    }
  | { type: "session_snapshot"; request_id?: string; session: SessionSnapshot }
  | { type: "session_playback"; request_id?: string; session: SessionSnapshot }
  | { type: "session_closed"; request_id?: string }
  | { type: "error"; request_id?: string; code: string; message: string }

export function statusFromSnapshot(snapshot: SessionSnapshot | null): SessionStatus {
  if (!snapshot) return "idle"
  if (snapshot.playback === "playing") return "playing"
  if (snapshot.playback === "ended") return "ended"
  return "ready"
}
