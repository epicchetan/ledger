import { useCallback, useEffect, useRef, useState } from "react"

import { applyBarFrames, type ReplayCandle } from "@/features/replay/bar-frame-adapter"
import {
  statusFromSnapshot,
  type SessionClientMessage,
  type SessionServerMessage,
  type SessionSnapshot,
  type SessionStatus,
} from "@/features/replay/session-protocol"
import type { MarketDay } from "@/features/data-center/types"

const WS_URL = import.meta.env.VITE_LEDGER_WS_URL ?? "ws://127.0.0.1:3001/sessions/ws"
const DEFAULT_BAR_SECONDS = 60
const DEFAULT_PLAY_SPEED = 60
const DEFAULT_ADVANCE_BATCHES = 1_000
const DEFAULT_PUMP_INTERVAL_MS = 50
const DEFAULT_PLAY_BUDGET_BATCHES = 500

export interface ReplaySessionController {
  status: SessionStatus
  connected: boolean
  marketDay: MarketDay | null
  snapshot: SessionSnapshot | null
  bars: ReplayCandle[]
  error: string | null
  barSeconds: number
  speed: number
  openReplay: (day: MarketDay) => void
  advance: (batches?: number) => void
  seek: (feedTsNs: string) => void
  play: () => void
  pause: () => void
  setSpeed: (speed: number) => void
  close: () => void
}

export function useSessionSocket(): ReplaySessionController {
  const wsRef = useRef<WebSocket | null>(null)
  const requestSeqRef = useRef(0)
  const barMapRef = useRef(new Map<number, ReplayCandle>())
  const marketDayRef = useRef<MarketDay | null>(null)
  const snapshotRef = useRef<SessionSnapshot | null>(null)
  const [status, setStatus] = useState<SessionStatus>("idle")
  const [connected, setConnected] = useState(false)
  const [marketDay, setMarketDay] = useState<MarketDay | null>(null)
  const [snapshot, setSnapshot] = useState<SessionSnapshot | null>(null)
  const [bars, setBars] = useState<ReplayCandle[]>([])
  const [error, setError] = useState<string | null>(null)
  const [barSeconds] = useState(DEFAULT_BAR_SECONDS)
  const [speed, setSpeedState] = useState(DEFAULT_PLAY_SPEED)

  const nextRequestId = useCallback(() => {
    requestSeqRef.current += 1
    return `lens-${requestSeqRef.current}`
  }, [])

  const clearBars = useCallback(() => {
    barMapRef.current.clear()
    setBars([])
  }, [])

  const setSessionSnapshot = useCallback((nextSnapshot: SessionSnapshot) => {
    snapshotRef.current = nextSnapshot
    setSnapshot(nextSnapshot)
    if (nextSnapshot.playback === "playing") {
      setSpeedState(nextSnapshot.speed)
    }
    setStatus(statusFromSnapshot(nextSnapshot))
  }, [])

  const send = useCallback((message: SessionClientMessage) => {
    const ws = wsRef.current
    if (!ws || ws.readyState !== WebSocket.OPEN) {
      setError("Session socket is not connected.")
      setStatus(snapshotRef.current ? statusFromSnapshot(snapshotRef.current) : "error")
      return false
    }
    ws.send(JSON.stringify(message))
    return true
  }, [])

  const subscribeBars = useCallback(() => {
    send({
      type: "subscribe_projection",
      request_id: nextRequestId(),
      projection: {
        id: "bars",
        version: 1,
        params: { seconds: barSeconds },
      },
    })
  }, [barSeconds, nextRequestId, send])

  const openReplay = useCallback(
    (day: MarketDay) => {
      const previous = wsRef.current
      if (previous) {
        previous.onopen = null
        previous.onmessage = null
        previous.onerror = null
        previous.onclose = null
        previous.close()
      }
      wsRef.current = null
      snapshotRef.current = null
      marketDayRef.current = day
      setMarketDay(day)
      setSnapshot(null)
      clearBars()
      setError(null)
      setConnected(false)
      setStatus("connecting")

      const ws = new WebSocket(WS_URL)
      wsRef.current = ws

      ws.onopen = () => {
        setConnected(true)
        setStatus("opening")
        ws.send(
          JSON.stringify({
            type: "open_session",
            request_id: nextRequestId(),
            session_id: `lens-${day.contract}-${day.marketDate}`,
            session_kind: "replay",
            symbol: day.contract,
            market_date: day.marketDate,
            feed: {
              kind: "replay",
              mode: "exchange_truth",
              visibility: "truth",
            },
          } satisfies SessionClientMessage),
        )
      }

      ws.onmessage = (event) => {
        let message: SessionServerMessage
        try {
          message = JSON.parse(String(event.data)) as SessionServerMessage
        } catch {
          setError("Ledger API sent malformed session data.")
          setStatus(snapshotRef.current ? statusFromSnapshot(snapshotRef.current) : "error")
          return
        }

        switch (message.type) {
          case "server_hello":
            break
          case "session_opening":
            setStatus("opening")
            break
          case "session_opened":
            setSessionSnapshot(message.session)
            subscribeBars()
            break
          case "projection_subscribed":
            setSessionSnapshot(message.session)
            setBars(applyBarFrames(barMapRef.current, message.frames))
            break
          case "session_frame_batch":
            setSessionSnapshot(message.session)
            setBars(applyBarFrames(barMapRef.current, message.frames, message.cause === "seek"))
            break
          case "session_snapshot":
          case "session_playback":
            setSessionSnapshot(message.session)
            break
          case "projection_unsubscribed":
            break
          case "session_closed":
            snapshotRef.current = null
            setSnapshot(null)
            clearBars()
            setStatus("idle")
            break
          case "error":
            setError(message.message)
            setStatus(snapshotRef.current ? statusFromSnapshot(snapshotRef.current) : "error")
            break
        }
      }

      ws.onerror = () => {
        setError("Unable to connect to Ledger session WebSocket.")
        setStatus(snapshotRef.current ? statusFromSnapshot(snapshotRef.current) : "error")
      }

      ws.onclose = () => {
        setConnected(false)
        if (wsRef.current === ws) {
          wsRef.current = null
        }
        if (snapshotRef.current) {
          setStatus(statusFromSnapshot(snapshotRef.current))
        } else if (marketDayRef.current) {
          setStatus("idle")
        }
      }
    },
    [clearBars, nextRequestId, setSessionSnapshot, subscribeBars],
  )

  const advance = useCallback(
    (batches = DEFAULT_ADVANCE_BATCHES) => {
      send({ type: "advance", request_id: nextRequestId(), batches })
    },
    [nextRequestId, send],
  )

  const seek = useCallback(
    (feedTsNs: string) => {
      send({ type: "seek", request_id: nextRequestId(), feed_ts_ns: feedTsNs })
    },
    [nextRequestId, send],
  )

  const play = useCallback(() => {
    send({
      type: "play",
      request_id: nextRequestId(),
      speed,
      pump_interval_ms: DEFAULT_PUMP_INTERVAL_MS,
      budget_batches: DEFAULT_PLAY_BUDGET_BATCHES,
    })
  }, [nextRequestId, send, speed])

  const pause = useCallback(() => {
    send({ type: "pause", request_id: nextRequestId() })
  }, [nextRequestId, send])

  const setSpeed = useCallback(
    (nextSpeed: number) => {
      setSpeedState(nextSpeed)
      send({ type: "set_speed", request_id: nextRequestId(), speed: nextSpeed })
    },
    [nextRequestId, send],
  )

  const close = useCallback(() => {
    send({ type: "close_session", request_id: nextRequestId() })
    wsRef.current?.close()
    wsRef.current = null
    snapshotRef.current = null
    marketDayRef.current = null
    setMarketDay(null)
    setSnapshot(null)
    clearBars()
    setConnected(false)
    setError(null)
    setStatus("idle")
  }, [clearBars, nextRequestId, send])

  useEffect(() => {
    return () => {
      wsRef.current?.close()
    }
  }, [])

  return {
    status,
    connected,
    marketDay,
    snapshot,
    bars,
    error,
    barSeconds,
    speed,
    openReplay,
    advance,
    seek,
    play,
    pause,
    setSpeed,
    close,
  }
}
