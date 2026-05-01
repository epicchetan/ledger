import { useState } from "react"
import * as chrono from "chrono-node"

import { Button } from "@/components/ui/button"
import {
  Popover,
  PopoverContent,
  PopoverTrigger,
} from "@/components/ui/popover"
import { Input } from "@/components/ui/input"
import { Calendar } from "@/components/ui/calendar"

// Server sends ET-shifted timestamps, so format in UTC to display ET date
const dateFormatter = new Intl.DateTimeFormat("en-US", {
  month: "short",
  day: "numeric",
  year: "numeric",
  timeZone: "UTC",
})

const previewFormatter = new Intl.DateTimeFormat("en-US", {
  weekday: "long",
  month: "long",
  day: "numeric",
  year: "numeric",
  timeZone: "UTC",
})

interface DatePickerProps {
  currentDate: Date
  onDateChange: (date: Date) => void
  disabled?: boolean
}

export function DatePicker({ currentDate, onDateChange, disabled }: DatePickerProps) {
  const [open, setOpen] = useState(false)
  const [inputValue, setInputValue] = useState("")
  const [parsedPreview, setParsedPreview] = useState<Date | null>(null)

  function handleOpenChange(isOpen: boolean) {
    if (isOpen) {
      setInputValue("")
      setParsedPreview(null)
    }
    setOpen(isOpen)
  }

  function handleInputChange(value: string) {
    setInputValue(value)
    const parsed = chrono.parseDate(value, { instant: new Date(), timezone: 0 })
    setParsedPreview(parsed)
  }

  function handleCommit(date: Date) {
    // Normalize to UTC midnight
    const utc = new Date(Date.UTC(date.getFullYear(), date.getMonth(), date.getDate()))
    onDateChange(utc)
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
          {dateFormatter.format(currentDate)}
        </Button>
      </PopoverTrigger>
      <PopoverContent align="start" className="w-auto">
        <div className="flex flex-col gap-2">
          <Input
            placeholder="e.g. march 12, next friday"
            value={inputValue}
            onChange={(e) => handleInputChange(e.target.value)}
            onKeyDown={(e) => {
              if (e.key === "Enter" && parsedPreview) {
                handleCommit(parsedPreview)
              }
            }}
            className="h-7 text-xs"
            autoFocus
          />
          {parsedPreview && (
            <p className="text-xs text-muted-foreground px-1">
              {previewFormatter.format(parsedPreview)}
            </p>
          )}
          <Calendar
            mode="single"
            selected={currentDate}
            onSelect={(date) => {
              if (date) handleCommit(date)
            }}
            defaultMonth={currentDate}
          />
        </div>
      </PopoverContent>
    </Popover>
  )
}
