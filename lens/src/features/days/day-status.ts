import type { ReadinessState } from "@/features/days/readiness"

// Dot color + one-word status shown on the terse card and echoed in the tray
// header. Shared so the card and tray read from the same source of truth.
export const STATE_META: Record<
  ReadinessState,
  { label: string; dot: string; pulse?: boolean }
> = {
  installing: { label: "Installing", dot: "bg-link", pulse: true },
  ready: { label: "Ready to replay", dot: "bg-success" },
  offloaded: { label: "Offloaded", dot: "bg-amber-500" },
  failed: { label: "Failed", dot: "bg-destructive" },
}

// While a job runs, the label is its live stage ("downloading", "decoding",
// ...) so the pipeline is visible right on the card; otherwise the state's
// static label.
export function stateLabel(
  state: ReadinessState,
  stage: string | null
): string {
  if (state === "installing" && stage) {
    return stage.charAt(0).toUpperCase() + stage.slice(1)
  }
  return STATE_META[state].label
}
