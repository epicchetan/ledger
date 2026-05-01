import { useState } from "react"

import { Button } from "@/components/ui/button"
import {
  Popover,
  PopoverContent,
  PopoverTrigger,
} from "@/components/ui/popover"
import { Input } from "@/components/ui/input"

interface SpeedPickerProps {
  speed: number
  onSpeedChange: (speed: number) => void
  disabled?: boolean
}

export function SpeedPicker({ speed, onSpeedChange, disabled }: SpeedPickerProps) {
  const [open, setOpen] = useState(false)
  const [inputValue, setInputValue] = useState("")

  function handleOpenChange(isOpen: boolean) {
    if (isOpen) {
      setInputValue("")
    }
    setOpen(isOpen)
  }

  function handleCommit() {
    // Strip trailing "x" and parse, e.g. "5x" → 5, "10" → 10
    const cleaned = inputValue.replace(/x$/i, "").trim()
    const parsed = Number(cleaned)
    if (parsed > 0 && Number.isFinite(parsed)) {
      onSpeedChange(parsed)
      setOpen(false)
    }
  }

  return (
    <Popover open={open} onOpenChange={handleOpenChange}>
      <PopoverTrigger asChild>
        <Button
          variant="ghost"
          size="sm"
          className="font-mono text-xs tabular-nums"
          disabled={disabled}
        >
          {speed}x
        </Button>
      </PopoverTrigger>
      <PopoverContent align="end" className="w-auto">
        <Input
          placeholder="e.g. 5x, 100"
          value={inputValue}
          onChange={(e) => setInputValue(e.target.value)}
          onKeyDown={(e) => {
            if (e.key === "Enter") handleCommit()
          }}
          className="h-7 w-24 font-mono text-xs"
          autoFocus
        />
      </PopoverContent>
    </Popover>
  )
}
