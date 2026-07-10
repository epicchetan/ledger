// A tiny external store for the chart's overlays (legend, jump-to-live, clock
// scrub preview, price-scale mode). Keeping this state here rather than in
// Replay means the frame stream re-renders only the overlay components, and only
// when their snapshot actually changes — the same "React sees booleans, not
// frames" discipline as the page.

export interface LegendData {
  open: number // price units (ticks already × TICK_SIZE)
  high: number
  low: number
  close: number
  volume: number
  up: boolean // close >= open, drives value color
}

// Field-wise equality: a crosshair moving inside one bar re-carries a
// value-identical legend, which must not notify (per-pixel churn otherwise).
function legendEqual(a: LegendData | null, b: LegendData | null): boolean {
  if (a === b) return true
  if (!a || !b) return false
  return (
    a.open === b.open &&
    a.high === b.high &&
    a.low === b.low &&
    a.close === b.close &&
    a.volume === b.volume &&
    a.up === b.up
  )
}

export class ChartUi {
  // getSnapshot-stable: the same reference is returned until a value-unequal
  // set replaces it, so useSyncExternalStore neither tears nor churns.
  legend: LegendData | null = null
  atEdge = true
  // The slider's mid-drag scrub preview in session-ms; null when not scrubbing.
  scrubMs: number | null = null
  // Whether the right price scale is still auto-fitting (false once the user
  // drags the axis into manual mode).
  autoScale = true
  // Assigned by the attached bars layer; null when detached.
  jumpToLive: (() => void) | null = null
  // Assigned by the attached bars layer (like jumpToLive); re-engages autoScale.
  resetPriceScale: (() => void) | null = null
  private readonly listeners = new Set<() => void>()

  subscribe = (onChange: () => void): (() => void) => {
    this.listeners.add(onChange)
    return () => {
      this.listeners.delete(onChange)
    }
  }

  setLegend(legend: LegendData | null): void {
    if (legendEqual(this.legend, legend)) return
    this.legend = legend
    this.emit()
  }

  setAtEdge(atEdge: boolean): void {
    if (this.atEdge === atEdge) return
    this.atEdge = atEdge
    this.emit()
  }

  setScrubMs(scrubMs: number | null): void {
    if (this.scrubMs === scrubMs) return
    this.scrubMs = scrubMs
    this.emit()
  }

  setAutoScale(autoScale: boolean): void {
    if (this.autoScale === autoScale) return
    this.autoScale = autoScale
    this.emit()
  }

  private emit(): void {
    for (const listener of this.listeners) listener()
  }
}
