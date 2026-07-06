import { CalendarDays } from "lucide-react"

import { Button } from "@/components/ui/button"
import { STATE_META, stateLabel } from "@/features/days/day-status"
import type { DayReadiness } from "@/features/days/readiness"
import { cn } from "@/lib/utils"

interface DayListProps {
  days: DayReadiness[]
  unassigned: DayReadiness | null
  selectedKey: string | null
  onSelect: (day: DayReadiness) => void
}

const MONTHS = [
  "January",
  "February",
  "March",
  "April",
  "May",
  "June",
  "July",
  "August",
  "September",
  "October",
  "November",
  "December",
]

// "2024-03-15" -> "March 2024". Null for anything that isn't an ISO market day
// (e.g. the Unassigned pseudo-day), so it stays out of the month grouping.
function monthLabel(marketDay: string): string | null {
  const match = /^(\d{4})-(\d{2})/.exec(marketDay)
  if (!match) return null
  const name = MONTHS[Number(match[2]) - 1]
  return name ? `${name} ${match[1]}` : null
}

// Fold the (already-ordered) days into contiguous month runs so each month gets
// a single header. Preserves the catalog's order rather than imposing one.
function groupByMonth(
  days: DayReadiness[]
): { label: string | null; days: DayReadiness[] }[] {
  const groups: { label: string | null; days: DayReadiness[] }[] = []
  for (const day of days) {
    const label = monthLabel(day.marketDay)
    const last = groups.at(-1)
    if (last && last.label === label) last.days.push(day)
    else groups.push({ label, days: [day] })
  }
  return groups
}

function MonthHeader({ label }: { label: string }) {
  return (
    <div className="px-0.5 pt-1 text-[0.7rem] font-medium tracking-wide text-muted-foreground uppercase">
      {label}
    </div>
  )
}

export function DayList({
  days,
  unassigned,
  selectedKey,
  onSelect,
}: DayListProps) {
  return (
    <div className="space-y-3">
      {groupByMonth(days).map((group, index) => (
        <div key={group.label ?? `unlabeled-${index}`} className="space-y-2">
          {group.label ? <MonthHeader label={group.label} /> : null}
          {group.days.map((day) => (
            <DayCard
              key={day.marketDay}
              day={day}
              selected={day.marketDay === selectedKey}
              onSelect={onSelect}
            />
          ))}
        </div>
      ))}
      {unassigned ? (
        <div className="space-y-2">
          <div className="px-0.5 pt-1 text-[0.7rem] font-medium tracking-wide text-muted-foreground uppercase">
            Unassigned · no market day yet
          </div>
          <DayCard
            day={unassigned}
            selected={unassigned.marketDay === selectedKey}
            onSelect={onSelect}
          />
        </div>
      ) : null}
    </div>
  )
}

// A day collapses to a single tappable line: a calendar glyph, the date, and
// the contract, with the status dot + its label trailing on the right. It is a
// pure selection target — every action lives in the tray that rises from the
// action bar once a day is picked.
function DayCard({
  day,
  selected,
  onSelect,
}: {
  day: DayReadiness
  selected: boolean
  onSelect: (day: DayReadiness) => void
}) {
  const meta = STATE_META[day.state]

  return (
    <Button
      type="button"
      variant="ghost"
      aria-pressed={selected}
      onClick={() => onSelect(day)}
      className={cn(
        "flex h-auto w-full items-center justify-start gap-2 rounded-lg border px-2.5 py-2 text-left",
        selected
          ? "border-ring bg-accent/60 hover:bg-accent/60"
          : "border-border bg-card/45 hover:bg-card/70"
      )}
    >
      <CalendarDays
        aria-hidden="true"
        className="size-4 shrink-0 text-muted-foreground"
      />
      <span className="font-mono text-sm font-semibold text-foreground">
        {day.marketDay}
      </span>
      <span className="rounded-md border border-border px-1.5 py-0.5 font-mono text-[0.65rem] font-normal text-muted-foreground">
        {day.primary.symbol}
      </span>
      {day.extraRawCount > 0 ? (
        <span className="text-[0.7rem] font-normal text-muted-foreground">
          +{day.extraRawCount}
        </span>
      ) : null}
      <span className="ml-auto flex min-w-0 items-center gap-1.5">
        <span
          className={cn(
            "size-2 shrink-0 rounded-full",
            meta.dot,
            meta.pulse && "animate-pulse"
          )}
          aria-hidden="true"
        />
        <span className="truncate text-xs font-normal text-muted-foreground">
          {stateLabel(day.state, day.stage)}
        </span>
      </span>
    </Button>
  )
}
