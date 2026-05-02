import { Database, MoreVertical, RefreshCw, ShieldCheck, Trash2 } from "lucide-react"

import { Button } from "@/components/ui/button"
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuSeparator,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu"
import { HoverCard, HoverCardContent, HoverCardTrigger } from "@/components/ui/hover-card"
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
  return (
    <div className="min-h-0 overflow-hidden border border-border bg-card/40">
      <div className="grid grid-cols-[1fr_auto] items-center border-b border-border px-3 py-2">
        <div>
          <h2 className="text-sm font-semibold">Market Days</h2>
          <p className="text-xs text-muted-foreground">Layer 1 raw data and Layer 2 replay datasets in durable R2 storage.</p>
        </div>
        <div className="text-xs text-muted-foreground">{days.length} days</div>
      </div>

      <div className="thin-scrollbar max-h-[calc(100svh-12.5rem)] overflow-auto">
        <table className="w-full min-w-[1120px] border-collapse text-left text-xs">
          <thead className="sticky top-0 z-10 bg-card text-[0.68rem] uppercase text-muted-foreground">
            <tr className="[&_th]:border-b [&_th]:border-border [&_th]:px-3 [&_th]:py-2 [&_th]:font-medium">
              <th>Date</th>
              <th>Contract</th>
              <th>Raw Data</th>
              <th>Replay Dataset</th>
              {artifactColumns.map((column) => (
                <th key={column.key}>{column.label}</th>
              ))}
              <th>Events</th>
              <th>Validation</th>
              <th className="w-10" />
            </tr>
          </thead>
          <tbody>
            {days.map((day) => (
              <tr key={day.id} className="border-b border-border/70 transition-colors hover:bg-muted/35">
                <td className="px-3 py-2.5 align-middle">
                  <div className="font-medium text-foreground">{day.marketDate}</div>
                  <div className="text-[0.68rem] text-muted-foreground">{day.sessionStart}</div>
                </td>
                <td className="px-3 py-2.5 align-middle text-muted-foreground">{day.contract}</td>
                <td className="px-3 py-2.5 align-middle">
                  <RawHover raw={day.rawData} />
                </td>
                <td className="px-3 py-2.5 align-middle">
                  <ReplayHover day={day} />
                </td>
                {artifactColumns.map((column) => {
                  const artifact = day.replayDataset.artifacts[column.key]
                  return (
                    <td key={column.key} className="px-3 py-2.5 align-middle">
                      <ArtifactHover label={column.label} artifact={artifact} />
                    </td>
                  )
                })}
                <td className="px-3 py-2.5 align-middle text-muted-foreground">
                  {formatCount(day.replayDataset.eventCount)}
                </td>
                <td className="px-3 py-2.5 align-middle">
                  <ValidationCell day={day} />
                </td>
                <td className="px-3 py-2.5 align-middle text-right">
                  <RowActions day={day} active={activeJobDayIds.has(day.id)} onAction={onAction} />
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
    </div>
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
