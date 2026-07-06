import type { ReadinessState } from "@/features/days/readiness"

// Dot color + one-word status shown on the terse card and echoed in the tray
// header. Shared so the card and tray read from the same source of truth.
export const STATE_META: Record<
  ReadinessState,
  { label: string; dot: string; pulse?: boolean }
> = {
  "ready-local": { label: "Ready to replay", dot: "bg-success" },
  "ready-remote": { label: "Needs hydrate", dot: "bg-amber-500" },
  preparing: { label: "Preparing", dot: "bg-link", pulse: true },
  failed: { label: "Prepare failed", dot: "bg-destructive" },
  unprepared: { label: "Not prepared", dot: "bg-muted-foreground" },
}
