import { Pause, Play, X } from "lucide-react"

import { Button } from "@/components/ui/button"
import { DatePicker } from "@/components/date-picker"
import { SpeedPicker } from "@/components/speed-picker"
import { TimePicker } from "@/components/time-picker"
import type { ReplaySessionController } from "@/features/replay/use-session-socket"
import { getETOffsetSeconds } from "@/lib/tz"

const NANOS_PER_MILLI = 1_000_000n

interface ReplayControlsProps {
  replay: ReplaySessionController
}

export function ReplayControls({ replay }: ReplayControlsProps) {
  const hasSession = replay.snapshot !== null
  const playing = replay.status === "playing"
  const opening = replay.status === "connecting" || replay.status === "opening"
  const ended = replay.status === "ended"
  const disabled = opening || !hasSession || ended
  const title = replay.marketDay ? `${replay.marketDay.contract} / ${replay.marketDay.marketDate}` : "No session"
  const feedEtDate = feedTimeToEtDate(replay.snapshot?.feed_ts_ns)

  function seekToUtcDate(date: Date) {
    replay.seek(utcDateToNs(date))
  }

  function handleDateChange(date: Date) {
    if (!feedEtDate) return
    const nextEt = new Date(date)
    nextEt.setUTCHours(feedEtDate.getUTCHours(), feedEtDate.getUTCMinutes(), feedEtDate.getUTCSeconds(), 0)
    seekToUtcDate(etDisplayDateToUtcDate(nextEt))
  }

  return (
    <div className="flex min-w-0 flex-wrap items-center justify-end gap-1.5">
      <div className="hidden min-w-0 max-w-48 truncate px-1 text-xs text-muted-foreground sm:block">{title}</div>
      <span className="hidden px-1 text-[0.68rem] uppercase text-muted-foreground md:inline">Feed ET</span>
      <DatePicker currentDate={feedEtDate ?? new Date()} onDateChange={handleDateChange} disabled={disabled || !feedEtDate} />
      <TimePicker currentTime={feedEtDate ?? new Date()} onTimeChange={seekToUtcDate} disabled={disabled || !feedEtDate} />
      <Button
        type="button"
        variant="ghost"
        size="icon-sm"
        disabled={disabled}
        onClick={playing ? replay.pause : replay.play}
        aria-label={playing ? "Pause replay" : "Play replay"}
      >
        {playing ? <Pause className="size-3.5" /> : <Play className="size-3.5" />}
      </Button>
      <SpeedPicker speed={replay.speed} onSpeedChange={replay.setSpeed} disabled={!hasSession || ended} />
      <Button type="button" variant="ghost" size="icon-sm" disabled={!hasSession && !opening} onClick={replay.close} aria-label="Close replay session">
        <X className="size-3.5" />
      </Button>
    </div>
  )
}

function feedTimeToEtDate(feedTsNs?: string): Date | null {
  if (!feedTsNs) return null
  const utcMs = Number(BigInt(feedTsNs) / NANOS_PER_MILLI)
  return new Date(utcMs + getETOffsetSeconds(utcMs) * 1000)
}

function etDisplayDateToUtcDate(etDate: Date): Date {
  const offset = getETOffsetSeconds(etDate.getTime())
  return new Date(etDate.getTime() - offset * 1000)
}

function utcDateToNs(date: Date): string {
  return (BigInt(date.getTime()) * NANOS_PER_MILLI).toString()
}
