// Time mapping for the chart. lightweight-charts renders timestamps as UTC, so
// the bars layer feeds it ET-shifted seconds (chartTime = utcSeconds +
// etOffsetSeconds) and axis/crosshair format those with a UTC clock — the
// standard lightweight-charts timezone approach — so labels read as ET and
// agree with the action bar's clock readout.

// Session/time-bucket boundaries use whole seconds here; BigInt division is
// exact and avoids the 2^53 hazard of the raw ns value. Event-anchored tick
// bars use nextBarChartTime below so sub-second ordering is retained.
export function nsToSeconds(ns: string): number {
  return Number(BigInt(ns) / 1_000_000_000n)
}

// Candlestick time keys must be strictly increasing. Tick bars can share an
// anchor second (or even an exact event timestamp), so retain microsecond
// precision and deterministically advance collisions. The epsilon is display
// only; exact nanosecond timestamps remain on the Bar payload.
const CHART_TIME_EPSILON_SECONDS = 0.000001

export function nextBarChartTime(
  anchorNs: string,
  previous: number | null,
  offsetSeconds: number
): number {
  const ns = BigInt(anchorNs)
  const wholeSeconds = ns / 1_000_000_000n
  const microseconds = (ns % 1_000_000_000n) / 1_000n
  const anchored =
    Number(wholeSeconds) + Number(microseconds) / 1_000_000 + offsetSeconds
  if (previous === null || anchored > previous) return anchored
  return previous + CHART_TIME_EPSILON_SECONDS
}

// ET's UTC offset at a given instant, in seconds (−18000 EST / −14400 EDT).
// longOffset renders the zone as "GMT-04:00"; a bare "GMT" (zero offset) parses
// to 0.
const ET_OFFSET = new Intl.DateTimeFormat("en-US", {
  timeZone: "America/New_York",
  timeZoneName: "longOffset",
})

export function etOffsetSeconds(atMs: number): number {
  const name = ET_OFFSET.formatToParts(new Date(atMs)).find(
    (part) => part.type === "timeZoneName"
  )?.value
  const match = name ? /GMT([+-])(\d{2}):(\d{2})/.exec(name) : null
  if (!match) return 0
  const sign = match[1] === "-" ? -1 : 1
  return sign * (Number(match[2]) * 3600 + Number(match[3]) * 60)
}

// "bars:1s" → 1, "bars:1m" → 60; null for unrecognized specs.
export function intervalSecondsOf(spec: string): number | null {
  const match = /:(\d+)([sm])$/.exec(spec)
  if (!match) return null
  const value = Number(match[1])
  return match[2] === "m" ? value * 60 : value
}

// The chart is fed ET-shifted seconds; formatting them with a UTC clock reads
// them back as ET (the timezone approach above). AM/PM included to match the
// action bar's readout.
const UTC_CLOCK = new Intl.DateTimeFormat("en-US", {
  timeZone: "UTC",
  hour: "numeric",
  minute: "2-digit",
  second: "2-digit",
  hour12: true,
})

export function formatChartTime(seconds: number): string {
  return UTC_CLOCK.format(new Date(seconds * 1000))
}
