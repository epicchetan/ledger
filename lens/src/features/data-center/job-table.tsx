import type { ColumnDef, Row } from "@tanstack/react-table"
import { RefreshCw, X } from "lucide-react"
import { useMemo, useState } from "react"

import { Button } from "@/components/ui/button"
import { TableCell, TableRow } from "@/components/ui/table"
import { DataTable } from "@/features/data-center/data-table"
import type { JobRecord } from "@/features/data-center/types"

export function ActiveJobTable({ jobs }: { jobs: JobRecord[] }) {
  const columns = useMemo<ColumnDef<JobRecord>[]>(
    () => [
      {
        id: "kind",
        accessorFn: (job) => job.kind,
        header: "Job",
        cell: ({ row }) => (
          <div className="min-w-0">
            <div className="truncate font-medium">{jobKindLabel(row.original.kind)}</div>
            <div className="mt-1 text-[0.68rem] text-muted-foreground">{shortJobId(row.original.id)}</div>
          </div>
        ),
      },
      {
        id: "latest",
        accessorFn: (job) => latestJobLine(job),
        header: "Progress",
        cell: ({ row }) => (
          <div className="truncate text-muted-foreground" title={latestJobLine(row.original)}>
            {latestJobLine(row.original)}
          </div>
        ),
      },
      {
        id: "status",
        accessorFn: (job) => job.status,
        header: "Status",
        cell: ({ row }) => <JobStatusBadge status={row.original.status} />,
      },
    ],
    [],
  )

  return (
    <section className="border border-border bg-card/40">
      <DataTable
        columns={columns}
        data={jobs}
        emptyMessage="No active jobs."
        getRowId={(job) => job.id}
        maxHeightClassName="max-h-72"
        minWidthClassName="min-w-[720px]"
      />
    </section>
  )
}

export function JobHistoryTable({
  title,
  jobs,
  loading,
  onClear,
}: {
  title: string
  jobs: JobRecord[]
  loading: boolean
  onClear?: () => void
}) {
  const [expandedJobId, setExpandedJobId] = useState<string | null>(null)
  const columns = useMemo<ColumnDef<JobRecord>[]>(
    () => [
      {
        id: "kind",
        accessorFn: (job) => job.kind,
        header: "Job",
        cell: ({ row }) => (
          <div className="min-w-0">
            <div className="truncate font-medium">{jobKindLabel(row.original.kind)}</div>
            <div className="mt-1 text-[0.68rem] text-muted-foreground">{shortJobId(row.original.id)}</div>
          </div>
        ),
      },
      {
        id: "time",
        accessorFn: (job) => job.finished_at ?? job.created_at,
        header: "Target",
        cell: ({ row }) => (
          <div className="min-w-0 text-muted-foreground">
            <div className="truncate">{jobTargetLabel(row.original)}</div>
            <div className="mt-1 text-[0.68rem]">{formatJobTime(row.original.finished_at ?? row.original.created_at)}</div>
          </div>
        ),
      },
      {
        id: "latest",
        accessorFn: (job) => latestJobLine(job),
        header: "Result",
        cell: ({ row }) => (
          <div className="truncate text-muted-foreground" title={latestJobLine(row.original)}>
            {latestJobLine(row.original)}
          </div>
        ),
      },
      {
        id: "status",
        accessorFn: (job) => job.status,
        header: "Status",
        cell: ({ row }) => <JobStatusBadge status={row.original.status} />,
      },
    ],
    [],
  )

  function toggleRow(row: Row<JobRecord>) {
    setExpandedJobId((current) => (current === row.original.id ? null : row.original.id))
  }

  return (
    <section className="border border-border bg-card/40">
      {loading || onClear ? (
        <div className="flex justify-end gap-2 border-b border-border px-3 py-1.5">
          {loading ? <RefreshCw className="size-3.5 animate-spin text-muted-foreground" /> : null}
          {onClear ? (
            <Button type="button" variant="ghost" size="icon" onClick={onClear} aria-label="Clear selected job history">
              <X className="size-4" />
            </Button>
          ) : null}
        </div>
      ) : null}
      <DataTable
        columns={columns}
        data={jobs}
        emptyMessage={loading ? `Loading ${title}.` : "No jobs found."}
        getRowId={(job) => job.id}
        initialSorting={[{ id: "time", desc: true }]}
        maxHeightClassName="max-h-[calc(100svh-9rem)]"
        minWidthClassName="min-w-[920px]"
        onRowClick={toggleRow}
        renderExpandedRow={(row, colSpan) =>
          expandedJobId === row.original.id ? <ExpandedJobRow key={`${row.id}-expanded`} job={row.original} colSpan={colSpan} /> : null
        }
      />
    </section>
  )
}

function ExpandedJobRow({ job, colSpan }: { job: JobRecord; colSpan: number }) {
  return (
    <TableRow className="border-border/70 hover:bg-transparent">
      <TableCell colSpan={colSpan} className="bg-background/40 px-3 py-2 text-xs">
        <div className="mb-2 grid grid-cols-[6rem_minmax(0,1fr)] gap-2 text-muted-foreground">
          <div>Started</div>
          <div>{formatJobTime(job.started_at)}</div>
          <div>Finished</div>
          <div>{formatJobTime(job.finished_at)}</div>
          {job.error ? (
            <>
              <div>Error</div>
              <div className="break-words text-red-300">{job.error}</div>
            </>
          ) : null}
        </div>
        <div className="space-y-1">
          {(job.progress.length > 0 ? job.progress : ["No progress events recorded."]).map((line, index) => (
            <div key={`${job.id}-${index}`} className="break-words text-muted-foreground">
              {line}
            </div>
          ))}
        </div>
      </TableCell>
    </TableRow>
  )
}

function JobStatusBadge({ status }: { status: JobRecord["status"] }) {
  return <div className={`inline-flex border px-1.5 py-0.5 text-[0.68rem] ${jobStatusTone(status)}`}>{status}</div>
}

function latestJobLine(job: JobRecord) {
  return job.error ?? job.progress.at(-1) ?? "No progress recorded."
}

function shortJobId(jobId: string) {
  return jobId.slice(0, 8)
}

function jobKindLabel(kind: string) {
  return kind.replaceAll("_", " ")
}

function jobTargetLabel(job: JobRecord) {
  if (job.target) return `${job.target.symbol} ${job.target.market_date}`
  return job.market_day_id ?? "No MarketDay"
}

function jobStatusTone(status: JobRecord["status"]) {
  switch (status) {
    case "succeeded":
      return "border-emerald-500/30 text-emerald-300"
    case "failed":
      return "border-red-400/35 text-red-300"
    case "running":
    case "queued":
      return "border-border text-muted-foreground"
  }
}

function formatJobTime(value: string | null) {
  if (!value) return "-"
  const date = new Date(value)
  if (Number.isNaN(date.getTime())) return "-"
  return new Intl.DateTimeFormat("en-US", {
    timeZone: "America/New_York",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hour12: false,
  }).format(date)
}
