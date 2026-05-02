import { AlertTriangle, CheckCircle2, CircleDashed, Database, Download, Loader2, XCircle } from "lucide-react"

import { statusLabel } from "@/features/data-center/status-labels"
import { cn } from "@/lib/utils"
import type { ArtifactStatus, MarketDayStatus } from "@/features/data-center/types"

interface StatusBadgeProps {
  status: MarketDayStatus | ArtifactStatus
  compact?: boolean
}

function statusTone(status: MarketDayStatus | ArtifactStatus) {
  switch (status) {
    case "ready":
    case "valid":
      return "border-emerald-500/30 bg-emerald-500/10 text-emerald-300"
    case "warning":
      return "border-amber-400/30 bg-amber-400/10 text-amber-300"
    case "invalid":
      return "border-red-400/35 bg-red-400/10 text-red-300"
    case "replay":
      return "border-cyan-400/25 bg-cyan-400/10 text-cyan-200"
    case "loading":
    case "deleting":
      return "border-border bg-muted/30 text-foreground"
    case "raw":
    case "remote":
      return "border-sky-400/25 bg-sky-400/10 text-sky-200"
    case "missing":
      return "border-border bg-muted/30 text-muted-foreground"
  }
}

function StatusIcon({ status }: { status: MarketDayStatus | ArtifactStatus }) {
  switch (status) {
    case "ready":
    case "valid":
      return <CheckCircle2 className="size-3" />
    case "warning":
      return <AlertTriangle className="size-3" />
    case "invalid":
      return <XCircle className="size-3" />
    case "replay":
      return <Database className="size-3" />
    case "loading":
    case "deleting":
      return <Loader2 className="size-3 animate-spin" />
    case "raw":
    case "remote":
      return <Download className="size-3" />
    case "missing":
      return <CircleDashed className="size-3" />
  }
}

export function DatasetStatusBadge({ status, compact = false }: StatusBadgeProps) {
  return (
    <span
      className={cn(
        "inline-flex items-center gap-1 border px-1.5 py-0.5 text-[0.68rem] font-medium leading-none",
        statusTone(status),
      )}
    >
      <StatusIcon status={status} />
      {compact ? null : statusLabel(status)}
    </span>
  )
}
