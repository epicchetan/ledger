import { Play, Pause } from "lucide-react"

import { Button } from "@/components/ui/button"
import { api } from "@/lib/api"
import { BarPicker } from "@/components/bar-picker"
import { IndicatorPicker } from "@/components/indicator-picker"
import { DatePicker } from "@/components/date-picker"
import { TimePicker } from "@/components/time-picker"
import { SpeedPicker } from "@/components/speed-picker"
import { getETOffsetSeconds } from "@/lib/tz"
import type { ReplayState } from "@/lib/ws-types"

interface TopBarProps {
  replayState: ReplayState | null
  channel: string
  onChannelChange: (channel: string) => void
  showDom: boolean
  onToggleDom: () => void
}

export function TopBar({ replayState, channel, onChannelChange, showDom, onToggleDom }: TopBarProps) {
  const hasState = replayState != null
  const playing = replayState?.playing ?? false
  const speed = replayState?.speed ?? 1
  const serverTime = replayState?.time ?? 0
  // serverTime is already ET-shifted; create Date whose UTC = ET display
  const currentDate = new Date(serverTime * 1000)

  function handlePlayPause() {
    if (playing) api.pause()
    else api.play()
  }

  function handleDateChange(date: Date) {
    // Combine selected date with current ET time
    const newDate = new Date(date)
    newDate.setUTCHours(
      currentDate.getUTCHours(),
      currentDate.getUTCMinutes(),
      currentDate.getUTCSeconds(),
      0,
    )
    // newDate is ET-shifted; reverse-shift for seek API (needs real UTC)
    const offset = getETOffsetSeconds(newDate.getTime())
    api.seek((newDate.getTime() - offset * 1000) / 1000)
  }

  function handleTimeChange(date: Date) {
    // Time-picker already reverse-shifted to real UTC
    api.seek(date.getTime() / 1000)
  }

  return (
    <div className="flex h-10 items-center justify-between border-b-2 border-[rgba(255,255,255,0.15)] px-3">
      {/* Left — bar type picker */}
      <div className="flex items-center gap-2">
        <BarPicker
          channel={channel}
          onChannelChange={onChannelChange}
          disabled={!hasState}
        />
        <IndicatorPicker
          showDom={showDom}
          onToggleDom={onToggleDom}
          disabled={!hasState}
        />
      </div>

      {/* Right — date, time, play/pause, speed */}
      <div className="flex items-center gap-1.5">
        <DatePicker
          currentDate={currentDate}
          onDateChange={handleDateChange}
          disabled={!hasState}
        />

        <TimePicker
          currentTime={currentDate}
          onTimeChange={handleTimeChange}
          disabled={!hasState}
        />

        <Button
          variant="ghost"
          size="icon-sm"
          onClick={handlePlayPause}
          disabled={!hasState}
        >
          {playing ? <Pause className="size-3.5" /> : <Play className="size-3.5" />}
        </Button>

        <SpeedPicker
          speed={speed}
          onSpeedChange={(v) => api.speed(v)}
          disabled={!hasState}
        />
      </div>
    </div>
  )
}
