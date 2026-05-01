import React from "react"

import { cn } from "@/lib/utils"

type Picker = "hours" | "minutes"

interface TimePickerInputProps {
  picker: Picker
  date: Date
  setDate: (date: Date) => void
  onRightFocus?: () => void
  onLeftFocus?: () => void
  className?: string
}

function getUTCValue(date: Date, picker: Picker): string {
  const val = picker === "hours" ? date.getUTCHours() : date.getUTCMinutes()
  return val.toString().padStart(2, "0")
}

function clampedUTC(date: Date, value: string, picker: Picker): Date {
  const d = new Date(date)
  const num = parseInt(value, 10)
  if (isNaN(num)) return d
  const max = picker === "hours" ? 23 : 59
  const clamped = Math.max(0, Math.min(max, num))
  if (picker === "hours") d.setUTCHours(clamped)
  else d.setUTCMinutes(clamped)
  return d
}

function arrowValue(current: string, step: number, picker: Picker): string {
  const max = picker === "hours" ? 23 : 59
  let num = parseInt(current, 10) + step
  if (num > max) num = 0
  if (num < 0) num = max
  return num.toString().padStart(2, "0")
}

const TimePickerInput = React.forwardRef<HTMLInputElement, TimePickerInputProps>(
  ({ className, picker, date, setDate, onRightFocus, onLeftFocus }, ref) => {
    const [flag, setFlag] = React.useState(false)

    React.useEffect(() => {
      if (flag) {
        const timer = setTimeout(() => setFlag(false), 2000)
        return () => clearTimeout(timer)
      }
    }, [flag])

    const display = getUTCValue(date, picker)

    function handleKeyDown(e: React.KeyboardEvent<HTMLInputElement>) {
      if (e.key === "Tab" || e.key === "Enter") return
      e.preventDefault()

      if (e.key === "ArrowRight") { onRightFocus?.(); return }
      if (e.key === "ArrowLeft") { onLeftFocus?.(); return }

      if (e.key === "ArrowUp" || e.key === "ArrowDown") {
        const step = e.key === "ArrowUp" ? 1 : -1
        const newVal = arrowValue(display, step, picker)
        if (flag) setFlag(false)
        setDate(clampedUTC(date, newVal, picker))
        return
      }

      if (e.key >= "0" && e.key <= "9") {
        const newVal = !flag ? "0" + e.key : display.slice(1) + e.key
        if (flag) onRightFocus?.()
        setFlag((prev) => !prev)
        setDate(clampedUTC(date, newVal, picker))
      }
    }

    return (
      <input
        ref={ref}
        type="tel"
        inputMode="decimal"
        className={cn(
          "w-7 rounded-sm bg-transparent text-center font-mono text-xs tabular-nums caret-transparent outline-none focus:bg-accent focus:text-accent-foreground",
          className,
        )}
        value={display}
        onChange={(e) => e.preventDefault()}
        onKeyDown={handleKeyDown}
      />
    )
  },
)

TimePickerInput.displayName = "TimePickerInput"
export { TimePickerInput }
