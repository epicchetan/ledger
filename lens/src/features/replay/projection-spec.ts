export const TIME_BAR_SPECS = [
  "bars:1m",
  "bars:5m",
  "bars:15m",
  "bars:1h",
] as const

export const TICK_BAR_SPECS = [
  "bars:100t",
  "bars:500t",
  "bars:1000t",
  "bars:2000t",
] as const

export function projectionLabel(spec: string): string {
  const separator = spec.indexOf(":")
  return separator === -1 ? spec : spec.slice(separator + 1)
}

export function projectionMenuLabel(spec: string): string {
  const label = projectionLabel(spec)
  return isTickBarsSpec(spec) ? label.slice(0, -1) : label
}

export function isTickBarsSpec(spec: string): boolean {
  return /^bars:\d+t$/.test(spec)
}
