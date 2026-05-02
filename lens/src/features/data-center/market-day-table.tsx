import type { Column, ColumnDef } from "@tanstack/react-table"
import { ArrowUpDown, Database, MoreVertical, RefreshCw, ScrollText, ShieldCheck, Trash2 } from "lucide-react"
import { useMemo } from "react"

import { Button } from "@/components/ui/button"
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuSeparator,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu"
import { HoverCard, HoverCardContent, HoverCardTrigger } from "@/components/ui/hover-card"
import { DataTable } from "@/features/data-center/data-table"
import { DatasetStatusBadge } from "@/features/data-center/dataset-status-badge"
import { statusLabel } from "@/features/data-center/status-labels"
import type { ArtifactKey, DatasetAction, MarketDay, RawDataSummary, ReplayArtifact } from "@/features/data-center/types"

interface MarketDayTableProps {
  days: MarketDay[]
  activeJobDayIds: Set<string>
  onAction: (day: MarketDay, action: DatasetAction) => void
}

const artifactColumns: Array<{ key: ArtifactKey; label: string }> = [
  { key: "events", label: "Events" },
  { key: "batches", label: "Batches" },
  { key: "trades", label: "Trades" },
  { key: "bookCheck", label: "Book" },
]

function formatCount(value: number | null) {
  return value == null ? "-" : new Intl.NumberFormat("en-US").format(value)
}

export function MarketDayTable({ days, activeJobDayIds, onAction }: MarketDayTableProps) {
  const columns = useMemo<ColumnDef<MarketDay>[]>(
    () => [
      {
        id: "marketDate",
        accessorFn: (day) => day.marketDate,
        header: ({ column }) => <SortableHeader column={column} label="Date" />,
        cell: ({ row }) => (
          <div>
            <div className="font-medium text-foreground">{row.original.marketDate}</div>
            <div className="text-[0.68rem] text-muted-foreground">{row.original.sessionStart}</div>
          </div>
        ),
      },
      {
        id: "contract",
        accessorFn: (day) => day.contract,
        header: ({ column }) => <SortableHeader column={column} label="Contract" />,
        cell: ({ row }) => <div className="text-muted-foreground">{row.original.contract}</div>,
      },
      {
        id: "rawData",
        accessorFn: (day) => day.rawData.status,
        header: ({ column }) => <SortableHeader column={column} label="Raw Data" />,
        cell: ({ row }) => <RawHover raw={row.original.rawData} />,
      },
      {
        id: "replayDataset",
        accessorFn: (day) => day.replayDataset.status,
        header: ({ column }) => <SortableHeader column={column} label="Replay Dataset" />,
        cell: ({ row }) => <ReplayHover day={row.original} />,
      },
      ...artifactColumns.map<ColumnDef<MarketDay>>((artifactColumn) => ({
        id: artifactColumn.key,
        header: artifactColumn.label,
        enableSorting: false,
        cell: ({ row }) => (
          <ArtifactHover label={artifactColumn.label} artifact={row.original.replayDataset.artifacts[artifactColumn.key]} />
        ),
      })),
      {
        id: "events",
        accessorFn: (day) => day.replayDataset.eventCount ?? -1,
        header: ({ column }) => <SortableHeader column={column} label="Events" />,
        cell: ({ row }) => (
          <div className="text-muted-foreground">{formatCount(row.original.replayDataset.eventCount)}</div>
        ),
      },
      {
        id: "validation",
        accessorFn: (day) => day.replayDataset.lastValidatedAt ?? "",
        header: ({ column }) => <SortableHeader column={column} label="Validation" />,
        cell: ({ row }) => <ValidationCell day={row.original} />,
      },
      {
        id: "actions",
        header: "",
        enableSorting: false,
        cell: ({ row }) => (
          <div className="text-right">
            <RowActions day={row.original} active={activeJobDayIds.has(row.original.id)} onAction={onAction} />
          </div>
        ),
      },
    ],
    [activeJobDayIds, onAction],
  )

  return (
    <div className="min-h-0 overflow-hidden border border-border bg-card/40">
      <DataTable
        columns={columns}
        data={days}
        emptyMessage="No market days found."
        getRowId={(day) => day.id}
        initialSorting={[{ id: "marketDate", desc: true }]}
      />
    </div>
  )
}

function SortableHeader<TData, TValue>({ column, label }: { column: Column<TData, TValue>; label: string }) {
  return (
    <button
      type="button"
      className="-ml-1 inline-flex h-6 items-center gap-1 px-1 text-[0.68rem] uppercase text-muted-foreground transition-colors hover:text-foreground"
      onClick={() => column.toggleSorting(column.getIsSorted() === "asc")}
    >
      {label}
      <ArrowUpDown className="size-3" />
    </button>
  )
}

function RawHover({ raw }: { raw: RawDataSummary }) {
  return (
    <HoverCard>
      <HoverCardTrigger asChild>
        <button type="button" className="text-left">
          <DatasetStatusBadge status={raw.status === "available" ? "raw" : raw.status === "error" ? "invalid" : "missing"} />
        </button>
      </HoverCardTrigger>
      <HoverCardContent align="start" className="w-80 border border-border bg-popover p-3 text-xs">
        <DetailTitle title="Layer 1 Raw Data" />
        <DetailRow label="Provider" value={raw.provider ?? "-"} />
        <DetailRow label="Dataset" value={raw.dataset ?? "-"} />
        <DetailRow label="Schema" value={raw.schema ?? "-"} />
        <DetailRow label="Symbol" value={raw.sourceSymbol ?? "-"} />
        <DetailRow label="Size" value={raw.size ?? "-"} />
        <DetailRow label="Updated" value={raw.updatedAt ?? "-"} />
        <DetailRow label="R2 Key" value={raw.remoteKey ?? "-"} wrap />
      </HoverCardContent>
    </HoverCard>
  )
}

function ReplayHover({ day }: { day: MarketDay }) {
  const dataset = day.replayDataset
  const status = dataset.status === "available" ? (day.status === "ready" ? "ready" : "replay") : dataset.status === "invalid" ? "invalid" : "missing"

  return (
    <HoverCard>
      <HoverCardTrigger asChild>
        <button type="button" className="text-left">
          <DatasetStatusBadge status={status} />
        </button>
      </HoverCardTrigger>
      <HoverCardContent align="start" className="w-80 border border-border bg-popover p-3 text-xs">
        <DetailTitle title="Layer 2 ReplayDataset" />
        <p className="mb-2 leading-5 text-muted-foreground">{dataset.trustSummary}</p>
        <DetailRow label="Dataset ID" value={dataset.id ?? "-"} wrap />
        <DetailRow label="Raw Input" value={dataset.rawObjectKey ?? "-"} wrap />
        <DetailRow label="Events" value={formatCount(dataset.eventCount)} />
        <DetailRow label="Batches" value={formatCount(dataset.batchCount)} />
        <DetailRow label="Trades" value={formatCount(dataset.tradeCount)} />
      </HoverCardContent>
    </HoverCard>
  )
}

function ArtifactHover({ label, artifact }: { label: string; artifact: ReplayArtifact }) {
  return (
    <HoverCard>
      <HoverCardTrigger asChild>
        <button type="button">
          <span className="sr-only">{statusLabel(artifact.status)}</span>
          <DatasetStatusBadge status={artifact.status} compact />
        </button>
      </HoverCardTrigger>
      <HoverCardContent align="start" className="w-80 border border-border bg-popover p-3 text-xs">
        <DetailTitle title={label} />
        <DetailRow label="Status" value={statusLabel(artifact.status)} />
        <DetailRow label="Size" value={artifact.size ?? "-"} />
        <DetailRow label="SHA" value={artifact.sha256 ?? "-"} wrap />
        <DetailRow label="Local" value={artifact.path ?? "-"} wrap />
        <DetailRow label="R2 Key" value={artifact.remoteKey ?? "-"} wrap />
      </HoverCardContent>
    </HoverCard>
  )
}

function ValidationCell({ day }: { day: MarketDay }) {
  const status = day.status === "ready" ? "valid" : day.status === "warning" ? "warning" : day.status === "invalid" ? "invalid" : "missing"
  return (
    <HoverCard>
      <HoverCardTrigger asChild>
        <button type="button" className="text-left">
          <DatasetStatusBadge status={status} compact />
          <span className="ml-2 text-muted-foreground">{day.replayDataset.lastValidatedAt ?? "-"}</span>
        </button>
      </HoverCardTrigger>
      <HoverCardContent align="end" className="w-72 border border-border bg-popover p-3 text-xs">
        <DetailTitle title="Validation" />
        <p className="mb-2 leading-5 text-muted-foreground">{day.replayDataset.trustSummary}</p>
        {day.replayDataset.warnings.length > 0 ? (
          <ul className="space-y-1 text-amber-300">
            {day.replayDataset.warnings.map((warning) => (
              <li key={warning}>{warning}</li>
            ))}
          </ul>
        ) : (
          <DetailRow label="Warnings" value="-" />
        )}
      </HoverCardContent>
    </HoverCard>
  )
}

function RowActions({
  day,
  active,
  onAction,
}: {
  day: MarketDay
  active: boolean
  onAction: (day: MarketDay, action: DatasetAction) => void
}) {
  const hasRaw = day.rawData.status === "available"
  const hasReplay = day.replayDataset.status === "available"

  return (
    <DropdownMenu>
      <DropdownMenuTrigger asChild>
        <Button type="button" variant="ghost" size="icon" disabled={active} aria-label={`Actions for ${day.contract} ${day.marketDate}`}>
          <MoreVertical className="size-4" />
        </Button>
      </DropdownMenuTrigger>
      <DropdownMenuContent align="end">
        <DropdownMenuItem onSelect={() => onAction(day, "prepare")}>
          <Database className="size-3.5" />
          Prepare
        </DropdownMenuItem>
        <DropdownMenuItem disabled={!hasRaw} onSelect={() => onAction(day, "rebuild")}>
          <RefreshCw className="size-3.5" />
          Rebuild ReplayDataset
        </DropdownMenuItem>
        <DropdownMenuItem disabled={!hasReplay} onSelect={() => onAction(day, "validate")}>
          <ShieldCheck className="size-3.5" />
          Validate Full
        </DropdownMenuItem>
        <DropdownMenuItem onSelect={() => onAction(day, "history")}>
          <ScrollText className="size-3.5" />
          Job History
        </DropdownMenuItem>
        <DropdownMenuSeparator />
        <DropdownMenuItem disabled={!hasReplay} className="text-red-300 focus:text-red-200" onSelect={() => onAction(day, "deleteReplay")}>
          <Trash2 className="size-3.5" />
          Delete ReplayDataset
        </DropdownMenuItem>
        <DropdownMenuItem disabled={!hasRaw} className="text-red-300 focus:text-red-200" onSelect={() => onAction(day, "deleteRaw")}>
          <Trash2 className="size-3.5" />
          Delete Raw Data
        </DropdownMenuItem>
      </DropdownMenuContent>
    </DropdownMenu>
  )
}

function DetailTitle({ title }: { title: string }) {
  return <div className="mb-2 font-semibold text-foreground">{title}</div>
}

function DetailRow({ label, value, wrap = false }: { label: string; value: string; wrap?: boolean }) {
  return (
    <div className="grid grid-cols-[5.5rem_minmax(0,1fr)] gap-2 py-0.5">
      <div className="text-muted-foreground">{label}</div>
      <div className={wrap ? "break-all text-foreground" : "truncate text-foreground"}>{value}</div>
    </div>
  )
}
