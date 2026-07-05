import { Loader2, Play, RotateCw } from "lucide-react"

import { Button } from "@/components/ui/button"
import { formatBytes } from "@/features/data-center/api"
import type {
  EsDayEntry,
  EsRawStatus,
  JobRecord,
} from "@/features/days/types"

export function DayList({
  days,
  unassigned,
  jobsBySubject,
  rowErrors,
  onPrepare,
}: {
  days: EsDayEntry[]
  unassigned: EsRawStatus[]
  jobsBySubject: Map<string, JobRecord>
  rowErrors: Map<string, string>
  onPrepare: (raw: EsRawStatus, force: boolean) => void
}) {
  return (
    <div className="space-y-2">
      {days.map((day) => (
        <section
          className="rounded-lg border border-border bg-card/45 p-2.5"
          key={day.marketDay}
        >
          <div className="mb-2 flex items-center justify-between gap-2">
            <h2 className="font-mono text-sm font-semibold">{day.marketDay}</h2>
            <span className="text-[0.7rem] text-muted-foreground">
              {day.raws.length} raw{day.raws.length === 1 ? "" : "s"}
            </span>
          </div>
          <div className="space-y-2">
            {day.raws.map((raw) => (
              <RawRow
                key={raw.raw.id}
                raw={raw}
                job={jobsBySubject.get(raw.raw.id)}
                error={rowErrors.get(raw.raw.id)}
                onPrepare={onPrepare}
              />
            ))}
          </div>
        </section>
      ))}
      {unassigned.length > 0 ? (
        <section className="rounded-lg border border-border bg-card/45 p-2.5">
          <div className="mb-2 flex items-center justify-between gap-2">
            <h2 className="font-mono text-sm font-semibold">Unassigned</h2>
            <span className="text-[0.7rem] text-muted-foreground">
              {unassigned.length} raw{unassigned.length === 1 ? "" : "s"}
            </span>
          </div>
          <div className="space-y-2">
            {unassigned.map((raw) => (
              <RawRow
                key={raw.raw.id}
                raw={raw}
                job={jobsBySubject.get(raw.raw.id)}
                error={rowErrors.get(raw.raw.id)}
                onPrepare={onPrepare}
              />
            ))}
          </div>
        </section>
      ) : null}
    </div>
  )
}

function RawRow({
  raw,
  job,
  error,
  onPrepare,
}: {
  raw: EsRawStatus
  job?: JobRecord
  error?: string
  onPrepare: (raw: EsRawStatus, force: boolean) => void
}) {
  const running = job?.state.status === "running"
  const source = metadataString(raw.raw.metadataJson, "source_symbol") ?? "-"
  const eventCount = metadataNumber(raw.artifact?.metadataJson, "event_count")
  const batchCount = metadataNumber(raw.artifact?.metadataJson, "batch_count")
  const stage = running && job.state.status === "running" ? job.state.stage : null
  const stageRecords =
    running && job.state.status === "running" ? job.state.records : null
  return (
    <div className="rounded-md border border-border/80 bg-background/70 p-2">
      <div className="flex items-start justify-between gap-2">
        <div className="min-w-0">
          <div className="flex flex-wrap items-center gap-1.5">
            <span className="rounded-md border border-border px-1.5 py-0.5 font-mono text-[0.65rem] uppercase text-muted-foreground">
              {raw.state}
            </span>
            <span className="font-mono text-xs">{source}</span>
            <span className="text-[0.7rem] text-muted-foreground">
              {formatBytes(raw.raw.sizeBytes)}
            </span>
          </div>
          <div className="mt-1 truncate font-mono text-[0.68rem] text-muted-foreground">
            {shortId(raw.raw.id)}
          </div>
          {eventCount !== null && batchCount !== null ? (
            <div className="mt-1 text-[0.7rem] text-muted-foreground">
              {eventCount.toLocaleString()} events /{" "}
              {batchCount.toLocaleString()} batches
            </div>
          ) : null}
          {stage ? (
            <div className="mt-1 flex items-center gap-1 text-[0.7rem] text-muted-foreground">
              <Loader2 className="size-3 animate-spin" />
              {stage}
              {stageRecords ? ` ${stageRecords.toLocaleString()}` : ""}
            </div>
          ) : null}
          {error ? (
            <div className="mt-1 text-[0.7rem] text-destructive">{error}</div>
          ) : null}
        </div>
        <Button
          className="h-7 shrink-0 text-xs"
          disabled={running}
          onClick={() => onPrepare(raw, raw.state === "prepared")}
          size="sm"
          type="button"
          variant="outline"
        >
          {running ? (
            <Loader2 className="size-3 animate-spin" />
          ) : raw.state === "prepared" ? (
            <RotateCw className="size-3" />
          ) : (
            <Play className="size-3" />
          )}
          {raw.state === "prepared" ? "Re-prepare" : "Prepare"}
        </Button>
      </div>
    </div>
  )
}

function metadataString(metadata: unknown, key: string) {
  if (!metadata || typeof metadata !== "object" || Array.isArray(metadata)) {
    return null
  }
  const value = (metadata as Record<string, unknown>)[key]
  return typeof value === "string" ? value : null
}

function metadataNumber(metadata: unknown, key: string) {
  if (!metadata || typeof metadata !== "object" || Array.isArray(metadata)) {
    return null
  }
  const value = (metadata as Record<string, unknown>)[key]
  return typeof value === "number" ? value : null
}

function shortId(id: string) {
  return id.length > 19 ? `${id.slice(0, 16)}...` : id
}
