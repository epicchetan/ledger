import { useCallback, useEffect, useRef, useState } from "react"
import type {
  IChartApi,
  ISeriesApi,
  CandlestickData,
  Time,
} from "lightweight-charts"

import { TopBar } from "@/components/top-bar"
import { Chart } from "@/components/chart"
import { VolumeProfile } from "@/components/volume-profile"
import { useReplay } from "@/hooks/use-replay"
import type { BarData, DomSnapshot, VpBar, ClientMsg, ReplayState, ServerMsg, StudyUpdate } from "@/lib/ws-types"

const DEFAULT_CHANNEL = "bars:1m"
const VISIBLE_BARS = 80

function toCandle(bar: BarData): CandlestickData<Time> {
  return {
    time: bar.time as unknown as Time,
    open: bar.open,
    high: bar.high,
    low: bar.low,
    close: bar.close,
  }
}

/** Convert a VpBar into an invisible synthetic candle for time/price axis. */
function vpToCandle(vp: VpBar): CandlestickData<Time> {
  if (vp.levels.length === 0) {
    return { time: vp.time as unknown as Time, open: vp.poc, high: vp.poc, low: vp.poc, close: vp.poc }
  }
  const prices = vp.levels.map((l) => l[0])
  const min = Math.min(...prices)
  const max = Math.max(...prices)
  return { time: vp.time as unknown as Time, open: min, high: max, low: min, close: vp.poc }
}

export function App() {
  const seriesRef = useRef<ISeriesApi<"Candlestick", Time, CandlestickData<Time>> | null>(null)
  const chartRef = useRef<IChartApi | null>(null)
  const sendRef = useRef<(msg: ClientMsg) => void>(() => {})
  const [replayState, setReplayState] = useState<ReplayState | null>(null)
  const [channel, setChannel] = useState(DEFAULT_CHANNEL)
  const [domSnapshot, setDomSnapshot] = useState<DomSnapshot | null>(null)
  const [showDom, setShowDom] = useState(false)
  const channelRef = useRef(DEFAULT_CHANNEL)

  // VP state
  const vpRef = useRef<VolumeProfile | null>(null)
  const vpBarsRef = useRef<VpBar[]>([])

  const isVpMode = channel.startsWith("vp:")

  const applyStudyUpdate = useCallback((ch: string, update: StudyUpdate) => {
    if (ch.startsWith("bars:")) {
      if (update.completions) {
        for (const bar of update.completions as BarData[]) {
          seriesRef.current?.update(toCandle(bar))
        }
      }
      if (update.current) {
        seriesRef.current?.update(toCandle(update.current as BarData))
      }
    } else if (ch.startsWith("vp:")) {
      const updates = [
        ...(update.completions ?? []) as VpBar[],
        ...(update.current ? [update.current as VpBar] : []),
      ]
      for (const bar of updates) {
        const last = vpBarsRef.current.length > 0 ? vpBarsRef.current[vpBarsRef.current.length - 1] : null
        if (last && last.time === bar.time) {
          vpBarsRef.current[vpBarsRef.current.length - 1] = bar
        } else {
          vpBarsRef.current.push(bar)
        }
        seriesRef.current?.update(vpToCandle(bar))
      }
      vpRef.current?.updateBars(updates)
    } else if (ch.startsWith("dom")) {
      if (update.current) {
        setDomSnapshot(update.current as DomSnapshot)
      }
    }
  }, [])

  const handleMessage = useCallback((msg: ServerMsg) => {
    if (msg.type === "init") {
      const ch = msg.channel

      if (ch.startsWith("bars:")) {
        const bars = (msg.history as BarData[]).map(toCandle)
        if (msg.current) bars.push(toCandle(msg.current as BarData))
        seriesRef.current?.setData(bars)

        if (bars.length > VISIBLE_BARS) {
          chartRef.current?.timeScale().scrollToRealTime()
        } else {
          chartRef.current?.timeScale().fitContent()
        }
      } else if (ch.startsWith("vp:")) {
        const vpBars: VpBar[] = Array.isArray(msg.history) ? [...(msg.history as VpBar[])] : []
        if (msg.current) vpBars.push(msg.current as VpBar)
        vpBarsRef.current = vpBars

        seriesRef.current?.setData(vpBars.map(vpToCandle))

        if (vpBars.length > VISIBLE_BARS) {
          chartRef.current?.timeScale().scrollToRealTime()
        } else {
          chartRef.current?.timeScale().fitContent()
        }

        // Attach VP primitive
        if (vpRef.current && seriesRef.current) {
          try { seriesRef.current.detachPrimitive(vpRef.current as never) } catch { /* already detached */ }
        }
        const vp = new VolumeProfile()
        vp.setData(vpBars)
        seriesRef.current?.attachPrimitive(vp as never)
        vpRef.current = vp
      } else if (ch.startsWith("dom")) {
        setDomSnapshot((msg.current as DomSnapshot) ?? null)
      }
    } else if (msg.type === "frame") {
      setReplayState(msg.replay)

      for (const [ch, update] of Object.entries(msg.studies)) {
        applyStudyUpdate(ch, update)
      }
    } else if (msg.type === "reset") {
      // Clear data for reset channels — server will send fresh inits
      for (const ch of msg.channels) {
        if (ch.startsWith("bars:") || ch.startsWith("vp:")) {
          seriesRef.current?.setData([])
        }
      }
    }
  }, [applyStudyUpdate])

  const handleOpen = useCallback(() => {
    sendRef.current({ type: "subscribe", channel: channelRef.current })
    sendRef.current({ type: "subscribe", channel: "dom:50" })
  }, [])

  const { send } = useReplay(handleMessage, handleOpen)

  useEffect(() => {
    sendRef.current = send
  }, [send])

  const handleChannelChange = useCallback((newChannel: string) => {
    const old = channelRef.current
    channelRef.current = newChannel
    setChannel(newChannel)

    // Clean up VP primitive when leaving VP mode
    if (old.startsWith("vp:") && vpRef.current && seriesRef.current) {
      try { seriesRef.current.detachPrimitive(vpRef.current as never) } catch { /* noop */ }
      vpRef.current = null
      vpBarsRef.current = []
    }

    send({ type: "unsubscribe", channel: old })
    send({ type: "subscribe", channel: newChannel })
  }, [send])

  return (
    <div className="flex h-svh flex-col bg-background">
      <TopBar
        replayState={replayState}
        channel={channel}
        onChannelChange={handleChannelChange}
        showDom={showDom}
        onToggleDom={() => setShowDom(s => !s)}
      />
      <Chart
        key={isVpMode ? "vp" : "candles"}
        className="flex-1 min-h-0"
        seriesRef={seriesRef}
        chartRef={chartRef}
        domSnapshot={showDom ? domSnapshot : undefined}
        mode={isVpMode ? "vp" : "candles"}
      />
    </div>
  )
}

export default App
