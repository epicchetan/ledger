import { useRef, useState } from "react"

import { Button } from "@/components/ui/button"
import {
  Popover,
  PopoverContent,
  PopoverTrigger,
} from "@/components/ui/popover"
import { TimePickerInput } from "@/components/time-picker-input"
import { getETOffsetSeconds } from "@/lib/tz"

// Server sends ET-shifted timestamps, so format in UTC to display ET time
const timeFormatter = new Intl.DateTimeFormat("en-US", {
  hour: "2-digit",
  minute: "2-digit",
  second: "2-digit",
  hour12: false,
  timeZone: "UTC",
})

interface TimePickerProps {
  currentTime: Date
  onTimeChange: (date: Date) => void
  disabled?: boolean
}

export function TimePicker({ currentTime, onTimeChange, disabled }: TimePickerProps) {
  const [open, setOpen] = useState(false)
  const [editDate, setEditDate] = useState(() => new Date(currentTime))
  const hourRef = useRef<HTMLInputElement>(null)
  const minuteRef = useRef<HTMLInputElement>(null)

  function handleOpenChange(isOpen: boolean) {
    if (isOpen) {
      // currentTime's UTC representation is already ET (server pre-shifted)
      setEditDate(new Date(currentTime))
    }
    setOpen(isOpen)
  }

  function commit() {
    // editDate's UTC hours/minutes are the desired ET hours/minutes
    const desired = new Date(currentTime)
    desired.setUTCHours(editDate.getUTCHours(), editDate.getUTCMinutes(), 0, 0)
    // desired is ET-shifted; reverse-shift for seek API (needs real UTC)
    const offset = getETOffsetSeconds(desired.getTime())
    onTimeChange(new Date(desired.getTime() - offset * 1000))
    setOpen(false)
  }

  return (
    <Popover open={open} onOpenChange={handleOpenChange}>
      <PopoverTrigger asChild>
        <Button
          variant="ghost"
          size="sm"
          className="font-mono text-xs"
          disabled={disabled}
        >
          {timeFormatter.format(currentTime)}
        </Button>
      </PopoverTrigger>
      <PopoverContent
        align="end"
        className="w-auto p-2"
        onOpenAutoFocus={(e) => {
          e.preventDefault()
          hourRef.current?.focus()
        }}
      >
        <div
          className="flex items-center"
          onKeyDown={(e) => {
            if (e.key === "Enter") {
              e.preventDefault()
              commit()
            }
          }}
        >
          <TimePickerInput
            ref={hourRef}
            picker="hours"
            date={editDate}
            setDate={setEditDate}
            onRightFocus={() => minuteRef.current?.focus()}
          />
          <span className="font-mono text-xs text-muted-foreground">:</span>
          <TimePickerInput
            ref={minuteRef}
            picker="minutes"
            date={editDate}
            setDate={setEditDate}
            onLeftFocus={() => hourRef.current?.focus()}
          />
        </div>
      </PopoverContent>
    </Popover>
  )
}
