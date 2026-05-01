const TZ = "America/New_York"

/**
 * UTC-to-ET offset in seconds for a given instant.
 * Negative: e.g. -14400 (EDT) or -18000 (EST).
 */
export function getETOffsetSeconds(utcMs: number): number {
  const d = new Date(utcMs)
  const et = new Date(d.toLocaleString("en-US", { timeZone: TZ }))
  const utc = new Date(d.toLocaleString("en-US", { timeZone: "UTC" }))
  return (et.getTime() - utc.getTime()) / 1000
}

/**
 * Shift a UTC epoch-seconds timestamp so lightweight-charts
 * (which labels the axis in UTC) displays US Eastern time.
 */
export function utcToChartTime(utcSeconds: number): number {
  return utcSeconds + getETOffsetSeconds(utcSeconds * 1000)
}
