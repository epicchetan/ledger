import { useEffect, useRef } from "react"
import type { ReactNode } from "react"
import type { CandlestickData, IChartApi, ISeriesApi, Time } from "lightweight-charts"
import { AlertTriangle, Loader2, Play } from "lucide-react"

import { Button } from "@/components/ui/button"
import { Chart } from "@/components/chart"
import type { ReplaySessionController } from "@/features/replay/use-session-socket"

const NANOS_PER_MILLI = 1_000_000n
const feedTimeFormatter = new Intl.DateTimeFormat("en-US", {
  month: "2-digit",
  day: "2-digit",
  hour: "2-digit",
  minute: "2-digit",
  second: "2-digit",
  hour12: false,
  timeZone: "America/New_York",
})

interface ReplayPageProps {
  replay: ReplaySessionController
}

export function ReplayPage({ replay }: ReplayPageProps) {
  const seriesRef = useRef<ISeriesApi<"Candlestick", Time, CandlestickData<Time>> | null>(null)
  const chartRef = useRef<IChartApi | null>(null)
  const previousBarCount = useRef(0)

  useEffect(() => {
    seriesRef.current?.setData(replay.bars)
    if (previousBarCount.current === 0 && replay.bars.length > 0) {
      chartRef.current?.timeScale().fitContent()
    }
    previousBarCount.current = replay.bars.length
  }, [replay.bars])

  return (
    <div className="relative flex min-h-0 flex-1 bg-background">
      <Chart className="min-h-0 flex-1" seriesRef={seriesRef} chartRef={chartRef} />
      <ReplayOverlay replay={replay} />
    </div>
  )
}

function ReplayOverlay({ replay }: ReplayPageProps) {
  if (replay.status === "idle") {
    return (
      <CenteredOverlay
        icon={<Play className="size-4 text-muted-foreground" />}
        title="No Replay Session"
        message="Open a ready ReplayDataset from Data Center."
      />
    )
  }

  if (replay.status === "connecting" || replay.status === "opening") {
    return (
      <CenteredOverlay
        icon={<Loader2 className="size-4 animate-spin text-muted-foreground" />}
        title="Opening Replay"
        message={replay.marketDay ? `${replay.marketDay.contract} ${replay.marketDay.marketDate}` : "Connecting to Ledger."}
      />
    )
  }

  if (replay.status === "error") {
    return (
      <CenteredOverlay
        icon={<AlertTriangle className="size-4 text-red-300" />}
        title="Replay Error"
        message={replay.error ?? "The replay session failed."}
        action={<Button type="button" variant="outline" size="sm" onClick={replay.close}>Clear</Button>}
      />
    )
  }

  if (replay.bars.length === 0) {
    return (
      <CenteredOverlay
        title="Waiting For Bars"
        message="Advance or play the Session until bars:v1 emits its first closed bar."
      />
    )
  }

  return (
    <div className="pointer-events-none absolute top-2 right-3 z-10 flex max-w-[45vw] items-center gap-3 truncate font-mono text-[11px] text-muted-foreground">
      <span>{replay.marketDay ? `${replay.marketDay.contract} ${replay.marketDay.marketDate}` : "Replay"}</span>
      <span>{replay.snapshot ? `Feed ${formatFeedTimeEt(replay.snapshot.feed_ts_ns)} ET` : null}</span>
      <span>{replay.snapshot ? `${replay.snapshot.batch_idx}/${replay.snapshot.total_batches}` : null}</span>
      <span>{replay.bars.length} bars</span>
    </div>
  )
}

function formatFeedTimeEt(feedTsNs: string) {
  const utcMs = Number(BigInt(feedTsNs) / NANOS_PER_MILLI)
  return feedTimeFormatter.format(new Date(utcMs))
}

function CenteredOverlay({
  icon,
  title,
  message,
  action,
}: {
  icon?: ReactNode
  title: string
  message: string
  action?: ReactNode
}) {
  return (
    <div className="pointer-events-none absolute inset-0 z-10 flex items-center justify-center bg-background/70">
      <div className="pointer-events-auto flex max-w-md flex-col items-center border border-border bg-card/80 px-6 py-5 text-center">
        {icon ? <div className="mb-2">{icon}</div> : null}
        <div className="text-sm font-semibold">{title}</div>
        <p className="mt-2 text-xs leading-5 text-muted-foreground">{message}</p>
        {action ? <div className="mt-4">{action}</div> : null}
      </div>
    </div>
  )
}
