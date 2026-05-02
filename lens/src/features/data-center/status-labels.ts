import type { ArtifactStatus, MarketDayStatus } from "@/features/data-center/types"

const labels: Record<MarketDayStatus | ArtifactStatus, string> = {
  missing: "Missing",
  raw: "Raw",
  replay: "Replay",
  ready: "Ready",
  warning: "Warning",
  invalid: "Invalid",
  loading: "Loading",
  deleting: "Deleting",
  remote: "Remote",
  valid: "Valid",
}

export function statusLabel(status: MarketDayStatus | ArtifactStatus) {
  return labels[status]
}
