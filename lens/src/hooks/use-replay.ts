import { useCallback, useEffect, useRef, useState } from "react"
import type { ClientMsg, ServerMsg } from "@/lib/ws-types"

const WS_URL = "ws://localhost:3001/ws"

export function useReplay(
  onMessage: (msg: ServerMsg) => void,
  onOpen?: () => void,
) {
  const wsRef = useRef<WebSocket | null>(null)
  const onMessageRef = useRef(onMessage)
  const onOpenRef = useRef(onOpen)
  const [connected, setConnected] = useState(false)

  useEffect(() => {
    onMessageRef.current = onMessage
  }, [onMessage])

  useEffect(() => {
    onOpenRef.current = onOpen
  }, [onOpen])

  useEffect(() => {
    const ws = new WebSocket(WS_URL)

    ws.onopen = () => {
      setConnected(true)
      onOpenRef.current?.()
    }

    ws.onmessage = (e) => {
      try {
        const msg = JSON.parse(e.data) as ServerMsg
        onMessageRef.current(msg)
      } catch {
        // ignore malformed messages
      }
    }

    ws.onclose = () => setConnected(false)

    wsRef.current = ws

    return () => {
      ws.close()
      wsRef.current = null
    }
  }, [])

  const send = useCallback((msg: ClientMsg) => {
    const ws = wsRef.current
    if (ws && ws.readyState === WebSocket.OPEN) {
      ws.send(JSON.stringify(msg))
    }
  }, [])

  return { send, connected }
}
