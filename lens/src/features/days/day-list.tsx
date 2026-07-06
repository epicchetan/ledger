import { Button } from "@/components/ui/button"
import { STATE_META } from "@/features/days/day-status"
import type { DayReadiness } from "@/features/days/readiness"
import { cn } from "@/lib/utils"

interface DayListProps {
  days: DayReadiness[]
  unassigned: DayReadiness | null
  selectedKey: string | null
  onSelect: (day: DayReadiness) => void
}

export function DayList({
  days,
  unassigned,
  selectedKey,
  onSelect,
}: DayListProps) {
  return (
    <div className="space-y-2">
      {days.map((day) => (
        <DayCard
          key={day.marketDay}
          day={day}
          selected={day.marketDay === selectedKey}
          onSelect={onSelect}
        />
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

// A day collapses to a single tappable line: status dot, date, contract. It is
// a pure selection target — every action lives in the tray that rises from the
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
  const meta = STATE_META[day.primary.state]

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
      <span
        className={cn(
          "size-2 shrink-0 rounded-full",
          meta.dot,
          meta.pulse && "animate-pulse"
        )}
        aria-hidden="true"
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
      <span className="ml-auto truncate text-xs font-normal text-muted-foreground">
        {meta.label}
      </span>
    </Button>
  )
}
