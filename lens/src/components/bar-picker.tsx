import { useState } from "react"

import { Button } from "@/components/ui/button"
import {
  Popover,
  PopoverContent,
  PopoverTrigger,
} from "@/components/ui/popover"
import {
  Command,
  CommandEmpty,
  CommandGroup,
  CommandInput,
  CommandItem,
  CommandList,
} from "@/components/ui/command"

const PRESETS = [
  { group: "Time", label: "1 minute", shortcut: "1m", value: "bars:1m", keywords: "1m one minute time" },
  { group: "Time", label: "5 minutes", shortcut: "5m", value: "bars:5m", keywords: "5m five minutes time" },
  { group: "Time", label: "15 minutes", shortcut: "15m", value: "bars:15m", keywords: "15m fifteen minutes time" },
  { group: "Time", label: "1 hour", shortcut: "1h", value: "bars:1h", keywords: "1h one hour time 60" },
  { group: "Tick", label: "200 tick", shortcut: "200 tick", value: "bars:200t", keywords: "200t 200 tick trade" },
  { group: "Tick", label: "500 tick", shortcut: "500 tick", value: "bars:500t", keywords: "500t 500 tick trade" },
  { group: "Tick", label: "1000 tick", shortcut: "1000 tick", value: "bars:1000t", keywords: "1000t 1000 tick trade" },
  { group: "Tick", label: "2000 tick", shortcut: "2000 tick", value: "bars:2000t", keywords: "2000t 2000 tick trade" },
  { group: "Volume Profile", label: "VP 5 min", shortcut: "VP 5m", value: "vp:5m", keywords: "vp volume profile 5m five minutes" },
  { group: "Volume Profile", label: "VP 15 min", shortcut: "VP 15m", value: "vp:15m", keywords: "vp volume profile 15m fifteen minutes" },
  { group: "Volume Profile", label: "VP 1 hour", shortcut: "VP 1h", value: "vp:1h", keywords: "vp volume profile 1h one hour" },
  { group: "Volume Profile", label: "VP 200 tick", shortcut: "VP 200t", value: "vp:200t", keywords: "vp volume profile 200t tick" },
] as const

const TIME_PRESETS = PRESETS.filter((p) => p.group === "Time")
const TICK_PRESETS = PRESETS.filter((p) => p.group === "Tick")
const VP_PRESETS = PRESETS.filter((p) => p.group === "Volume Profile")

function parseBarSpec(input: string): string | null {
  const s = input.trim().toLowerCase()

  // "vp 3m", "vp 200t", "vp 1h"
  const vpMatch = s.match(/^vp\s+(.+)$/)
  if (vpMatch) {
    const inner = parseBarSpec(vpMatch[1])
    return inner ? inner.replace("bars:", "vp:") : null
  }

  const tick = s.match(/^(\d+)\s*(?:t|ticks?)$/)
  if (tick) return `bars:${tick[1]}t`
  const time = s.match(
    /^(\d+)\s*(?:(m|min|minutes?)|(h|hrs?|hours?)|(s|secs?|seconds?))$/,
  )
  if (time) {
    const n = time[1]
    if (time[2]) return `bars:${n}m`
    if (time[3]) return `bars:${n}h`
    if (time[4]) return `bars:${n}s`
  }
  return null
}

function channelLabel(channel: string): string {
  if (channel.startsWith("vp:")) {
    const spec = channel.replace("vp:", "")
    if (spec.endsWith("t")) return `VP ${spec.slice(0, -1)} tick`
    return `VP ${spec}`
  }
  const spec = channel.replace("bars:", "")
  if (spec.endsWith("t")) return `${spec.slice(0, -1)} tick`
  return spec
}

interface BarPickerProps {
  channel: string
  onChannelChange: (channel: string) => void
  disabled?: boolean
}

export function BarPicker({
  channel,
  onChannelChange,
  disabled,
}: BarPickerProps) {
  const [open, setOpen] = useState(false)
  const [search, setSearch] = useState("")

  function select(value: string) {
    onChannelChange(value)
    setOpen(false)
  }

  function handleOpenChange(isOpen: boolean) {
    if (isOpen) setSearch("")
    setOpen(isOpen)
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
          {channelLabel(channel)}
        </Button>
      </PopoverTrigger>
      <PopoverContent align="start" className="w-40 p-0">
        <Command shouldFilter>
          <CommandInput
            placeholder="e.g. 3m, 200 tick"
            value={search}
            onValueChange={setSearch}
            onKeyDown={(e) => {
              if (e.key === "Enter" && search) {
                const parsed = parseBarSpec(search)
                if (parsed) {
                  e.preventDefault()
                  select(parsed)
                }
              }
            }}
          />
          <CommandList className="thin-scrollbar">
            <CommandEmpty>
              {search && parseBarSpec(search)
                ? "Press Enter to apply"
                : "No match"}
            </CommandEmpty>
            <CommandGroup heading="Time">
              {TIME_PRESETS.map((p) => (
                <CommandItem
                  key={p.value}
                  value={p.value}
                  keywords={[p.keywords]}
                  onSelect={() => select(p.value)}
                >
                  {p.label}
                </CommandItem>
              ))}
            </CommandGroup>
            <CommandGroup heading="Tick">
              {TICK_PRESETS.map((p) => (
                <CommandItem
                  key={p.value}
                  value={p.value}
                  keywords={[p.keywords]}
                  onSelect={() => select(p.value)}
                >
                  {p.label}
                </CommandItem>
              ))}
            </CommandGroup>
            <CommandGroup heading="Volume Profile">
              {VP_PRESETS.map((p) => (
                <CommandItem
                  key={p.value}
                  value={p.value}
                  keywords={[p.keywords]}
                  onSelect={() => select(p.value)}
                >
                  {p.label}
                </CommandItem>
              ))}
            </CommandGroup>
          </CommandList>
        </Command>
      </PopoverContent>
    </Popover>
  )
}
