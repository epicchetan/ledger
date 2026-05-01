// ── Server → Client ──────────────────────────────────────────────────

export interface BarData {
  time: number
  open: number
  high: number
  low: number
  close: number
  volume: number
}

export interface DomSnapshot {
  bids: [number, number, number][] // [price, size, orderCount]
  asks: [number, number, number][]
  trade?: { price: number; size: number; side: string }
}

export interface VpBar {
  time: number
  levels: [number, number][] // [price, volume], sorted ascending
  poc: number
}

export interface ReplayState {
  playing: boolean
  speed: number
  time: number // ET-shifted epoch seconds
  cursor: number
}

export interface StudyUpdate {
  completions?: unknown[]
  current?: unknown
}

export type ServerMsg =
  | { type: "init"; channel: string; history: unknown[]; current?: unknown }
  | { type: "frame"; replay: ReplayState; studies: Record<string, StudyUpdate> }
  | { type: "reset"; channels: string[] }

// ── Client → Server (WS only — data subscriptions) ──────────────────

export type ClientMsg =
  | { type: "subscribe"; channel: string }
  | { type: "unsubscribe"; channel: string }
