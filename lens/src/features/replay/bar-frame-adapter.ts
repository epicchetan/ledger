import type { CandlestickData, Time } from "lightweight-charts"

import type { ProjectionFrame } from "@/features/replay/session-protocol"

const ES_TICK_SIZE = 0.25
const NANOS_PER_SECOND = 1_000_000_000n

export type ReplayCandle = CandlestickData<Time>

interface BarPayload {
  bar_start_ts_ns: string
  bar_end_ts_ns: string
  open: number
  high: number
  low: number
  close: number
  volume: number
  trade_count: number
  final: boolean
}

interface BarsSnapshotPayload {
  current?: BarPayload | null
  last_final?: BarPayload | null
}

export function applyBarFrames(
  currentBars: Map<number, ReplayCandle>,
  frames: ProjectionFrame[],
  reset = false,
): ReplayCandle[] {
  const nextBars = reset ? new Map<number, ReplayCandle>() : new Map(currentBars)

  for (const frame of frames) {
    if (frame.stamp.projection_key.id !== "bars") continue

    if (frame.op === "snapshot") {
      for (const payload of snapshotBars(frame.payload)) {
        const candle = toCandle(payload)
        nextBars.set(candle.time as number, candle)
      }
      continue
    }

    const payload = parseBarPayload(frame.payload)
    if (!payload) continue

    const candle = toCandle(payload)
    nextBars.set(candle.time as number, candle)
  }

  currentBars.clear()
  for (const [time, candle] of nextBars) {
    currentBars.set(time, candle)
  }

  return [...nextBars.values()].sort((left, right) => (left.time as number) - (right.time as number))
}

function snapshotBars(payload: unknown): BarPayload[] {
  if (!isRecord(payload)) return []

  const snapshot = payload as BarsSnapshotPayload
  return [parseBarPayload(snapshot.last_final), parseBarPayload(snapshot.current)].filter((bar): bar is BarPayload =>
    Boolean(bar),
  )
}

function parseBarPayload(payload: unknown): BarPayload | null {
  if (!isRecord(payload)) return null

  const bar_start_ts_ns = payload.bar_start_ts_ns
  const bar_end_ts_ns = payload.bar_end_ts_ns
  const { open, high, low, close, volume, trade_count } = payload
  const final = payload.final

  if (
    typeof bar_start_ts_ns !== "string" ||
    typeof bar_end_ts_ns !== "string" ||
    typeof open !== "number" ||
    typeof high !== "number" ||
    typeof low !== "number" ||
    typeof close !== "number" ||
    typeof volume !== "number" ||
    typeof trade_count !== "number" ||
    typeof final !== "boolean"
  ) {
    return null
  }

  return {
    bar_start_ts_ns,
    bar_end_ts_ns,
    open,
    high,
    low,
    close,
    volume,
    trade_count,
    final,
  }
}

function toCandle(payload: BarPayload): ReplayCandle {
  return {
    time: nsToChartSeconds(payload.bar_start_ts_ns) as Time,
    open: ticksToDisplayPrice(payload.open),
    high: ticksToDisplayPrice(payload.high),
    low: ticksToDisplayPrice(payload.low),
    close: ticksToDisplayPrice(payload.close),
  }
}

function nsToChartSeconds(ns: string): number {
  return Number(BigInt(ns) / NANOS_PER_SECOND)
}

function ticksToDisplayPrice(ticks: number): number {
  return ticks * ES_TICK_SIZE
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null
}
