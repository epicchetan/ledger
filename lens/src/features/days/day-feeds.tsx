import {
  CalendarDays,
  Check,
  ChevronRight,
  Cloud,
  Download,
  HardDrive,
  Loader2,
} from "lucide-react"
import { useState } from "react"

import { Button } from "@/components/ui/button"
import { DetailRow, RoleBadge } from "@/features/data-center/object-list"
import type { StoreObject } from "@/features/data-center/types"
import { stateLabel } from "@/features/days/day-status"
import type { DayReadiness, RawReadiness } from "@/features/days/readiness"
import type { EsRawStatus } from "@/features/days/types"
import { cn } from "@/lib/utils"

interface DayFeedsProps {
  day: DayReadiness
  openObject: StoreObject | null
  onOpenObject: (object: StoreObject) => void
  onInstall: (raw: EsRawStatus) => void
}

// The body of the expanded action bar: a header (day + feed count) over a
// content region that swaps between the feed view (its objects, with a single
// Install action) and a single object's data. Which object is open is owned by
// the action bar, which also carries the object's back/delete controls, so
// this view stays pure display — entering an object never stacks a modal.
export function DayFeeds({ day, openObject, onOpenObject, onInstall }: DayFeedsProps) {
  const [activeFeedId, setActiveFeedId] = useState<string | null>(null)

  // Fall back to the primary feed so a stale id from a previous day resolves
  // without a reset.
  const activeFeed =
    day.raws.find((feed) => feed.raw.raw.id === activeFeedId) ?? day.primary

  return (
    <div className="flex flex-col gap-2.5">
      <div className="flex items-center justify-between gap-2 px-0.5">
        <div className="flex min-w-0 items-center gap-1.5">
          <CalendarDays
            aria-hidden="true"
            className="size-4 shrink-0 text-muted-foreground"
          />
          <span className="font-mono text-sm font-semibold text-foreground">
            {day.marketDay}
          </span>
        </div>
        <span className="text-[0.7rem] text-muted-foreground">
          {day.raws.length} feed{day.raws.length === 1 ? "" : "s"}
        </span>
      </div>

      {openObject ? (
        <ObjectView object={openObject} />
      ) : (
        <FeedView
          day={day}
          feed={activeFeed}
          onSelectFeed={setActiveFeedId}
          onOpenObject={onOpenObject}
          onInstall={onInstall}
        />
      )}
    </div>
  )
}

// A feed is a raw→artifact replay pipeline. Today every raw is an ES MBO
// download (symbol like "ESH4"), so it reads as "ES Replay"; anything else
// degrades to a generic "Replay" until its naming is worked out.
function feedName(feed: RawReadiness): string {
  return feed.symbol.startsWith("ES") ? "ES Replay" : "Replay"
}

function FeedView({
  day,
  feed,
  onSelectFeed,
  onOpenObject,
  onInstall,
}: {
  day: DayReadiness
  feed: RawReadiness
  onSelectFeed: (id: string) => void
  onOpenObject: (object: StoreObject) => void
  onInstall: (raw: EsRawStatus) => void
}) {
  return (
    <div className="flex flex-col gap-3">
      {day.raws.length > 1 ? (
        <div className="flex flex-wrap gap-1.5">
          {day.raws.map((entry) => {
            const active = entry.raw.raw.id === feed.raw.raw.id
            return (
              <button
                key={entry.raw.raw.id}
                type="button"
                aria-pressed={active}
                onClick={() => onSelectFeed(entry.raw.raw.id)}
                className={cn(
                  "rounded-md border px-2 py-1 text-xs transition-colors",
                  active
                    ? "border-ring bg-accent/50 text-foreground"
                    : "border-border text-muted-foreground hover:bg-card/60"
                )}
              >
                {feedName(entry)} · {entry.symbol}
              </button>
            )
          })}
        </div>
      ) : null}

      <div className="flex items-center justify-between gap-2 px-0.5">
        <div className="flex min-w-0 flex-col">
          <div className="flex items-center gap-2">
            <span className="text-sm font-semibold text-foreground">
              {feedName(feed)}
            </span>
            <span className="rounded border border-border px-1 py-px font-mono text-[0.6rem] text-muted-foreground">
              {feed.symbol}
            </span>
          </div>
          {feed.eventCount !== null ? (
            <span className="mt-0.5 text-[0.7rem] text-muted-foreground tabular-nums">
              {formatCount(feed.eventCount)} events
            </span>
          ) : null}
        </div>
        <InstallButton feed={feed} onInstall={onInstall} />
      </div>

      {feed.error && feed.state === "failed" ? (
        <div className="px-0.5 text-[0.72rem] break-words text-destructive">
          {feed.error}
        </div>
      ) : null}

      <div>
        <div className="mb-1.5 px-0.5 text-[0.65rem] font-medium tracking-wide text-muted-foreground uppercase">
          Objects
        </div>
        <div className="flex flex-col gap-1.5">
          {feed.storageObjects.map((object) => (
            <ObjectRow
              key={object.id}
              object={object}
              onOpen={() => onOpenObject(object)}
            />
          ))}
        </div>
      </div>
    </div>
  )
}

// One verb for the whole lifecycle: the backend hydrates an existing artifact
// or rebuilds a missing/invalid one — this button never has to know which.
// While the job runs it shows the live pipeline stage; a ready feed settles
// into a disabled "Installed".
function InstallButton({
  feed,
  onInstall,
}: {
  feed: RawReadiness
  onInstall: (raw: EsRawStatus) => void
}) {
  if (feed.state === "ready") {
    return (
      <Button
        type="button"
        size="sm"
        variant="outline"
        className="h-8 shrink-0 gap-1.5 text-success"
        disabled
      >
        <Check className="size-3.5" />
        Installed
      </Button>
    )
  }

  if (feed.state === "installing") {
    return (
      <Button type="button" size="sm" className="h-8 shrink-0" disabled>
        <Loader2 className="size-3.5 animate-spin" />
        {stateLabel(feed.state, feed.stage)}
      </Button>
    )
  }

  return (
    <Button
      type="button"
      size="sm"
      className="h-8 shrink-0"
      onClick={() => onInstall(feed.raw)}
    >
      <Download className="size-3.5" />
      {feed.state === "failed" ? "Retry" : "Install"}
    </Button>
  )
}

function ObjectRow({
  object,
  onOpen,
}: {
  object: StoreObject
  onOpen: () => void
}) {
  return (
    <button
      type="button"
      onClick={onOpen}
      className="flex w-full items-center gap-2 rounded-lg border border-border bg-card/40 py-2 pr-2 pl-2.5 text-left transition-colors hover:bg-card/60"
    >
      <div className="min-w-0 flex-1">
        <div className="flex items-baseline justify-between gap-2">
          <span className="min-w-0 truncate text-sm text-foreground">
            {object.fileName}
          </span>
          <span className="shrink-0 font-mono text-xs text-muted-foreground tabular-nums">
            {object.size}
          </span>
        </div>
        <div className="mt-0.5 flex items-center gap-2">
          <RoleBadge role={object.role} />
          <span className="min-w-0 flex-1 truncate text-xs text-muted-foreground">
            {object.kind}
          </span>
          <LocalityIcon object={object} />
        </div>
      </div>
      <ChevronRight
        aria-hidden="true"
        className="size-4 shrink-0 text-muted-foreground/50"
      />
    </button>
  )
}

// Object detail: header + metadata only. Back / delete ride on the action bar
// for as long as this view is open.
function ObjectView({ object }: { object: StoreObject }) {
  return (
    <div className="flex flex-col gap-2.5">
      <div className="flex items-center gap-2 px-0.5">
        <RoleBadge role={object.role} />
        <span className="min-w-0 flex-1 truncate text-sm font-medium text-foreground">
          {object.fileName}
        </span>
        <LocalityIcon object={object} />
      </div>

      <div className="rounded-lg border border-border bg-card/30 p-2.5 text-xs">
        <div className="space-y-0.5">
          <DetailRow label="Kind" value={object.kind} wrap />
          <DetailRow label="Format" value={object.format ?? "-"} />
          <DetailRow label="Media" value={object.mediaType ?? "-"} wrap />
          <DetailRow label="Size" value={object.size} mono />
        </div>
        <div className="my-2 border-t border-border/60" />
        <div className="space-y-0.5">
          <DetailRow label="ID" value={object.id} mono wrap />
          <DetailRow label="SHA-256" value={object.contentSha256} mono wrap />
          <DetailRow label="Bucket" value={object.remote?.bucket ?? "-"} mono />
          <DetailRow label="Key" value={object.remote?.key ?? "-"} mono wrap />
          <DetailRow
            label="Local"
            value={object.local?.relativePath ?? "-"}
            mono
            wrap
          />
        </div>
        <div className="my-2 border-t border-border/60" />
        <div className="space-y-0.5">
          <DetailRow label="Created" value={object.createdAt} mono />
          <DetailRow label="Updated" value={object.updatedAt} mono />
        </div>
      </div>
    </div>
  )
}

// One icon, one question: is it installed here? Everything is always in R2
// (registration uploads before it registers), so the only signal worth a
// glyph is the local copy — disk when present, cloud when it lives remote.
function LocalityIcon({ object }: { object: StoreObject }) {
  return object.local ? (
    <HardDrive aria-hidden="true" className="size-3 shrink-0 text-success" />
  ) : (
    <Cloud
      aria-hidden="true"
      className="size-3 shrink-0 text-muted-foreground"
    />
  )
}

function formatCount(value: number) {
  if (value >= 1_000_000) return `${(value / 1_000_000).toFixed(1)}M`
  if (value >= 1_000) return `${(value / 1_000).toFixed(1)}K`
  return value.toLocaleString()
}
