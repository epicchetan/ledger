import type { CanvasRenderingTarget2D } from "fancy-canvas"
import type {
  AutoscaleInfo,
  Coordinate,
  IChartApi,
  IPrimitivePaneRenderer,
  IPrimitivePaneView,
  ISeriesApi,
  Logical,
  PrimitivePaneViewZOrder,
  SeriesAttachedParameter,
  SeriesType,
  Time,
} from "lightweight-charts"

import type { VpBar } from "@/lib/ws-types"

// --- Renderer ---

class VolumeProfileRenderer implements IPrimitivePaneRenderer {
  private _source: VolumeProfile

  constructor(source: VolumeProfile) {
    this._source = source
  }

  draw(target: CanvasRenderingTarget2D): void {
    const series = this._source.getSeries()
    const chart = this._source.getChart()
    const bars = this._source.getBars()
    if (!series || !chart || bars.length === 0) return

    target.useMediaCoordinateSpace(({ context: ctx, mediaSize }) => {
      const timeScale = chart.timeScale()
      const barSpacing = timeScale.options().barSpacing

      for (const bar of bars) {
        const x = timeScale.timeToCoordinate(bar.time as unknown as Time)
        if (x === null) continue
        if (x + barSpacing < 0 || x > mediaSize.width) continue
        if (bar.levels.length === 0) continue

        let maxVol = 0
        for (const [, vol] of bar.levels) {
          if (vol > maxVol) maxVol = vol
        }
        if (maxVol === 0) continue

        // Derive bar height from price scale using adjacent tick prices.
        const refPrice = bar.levels[0][0]
        const y1 = series.priceToCoordinate(refPrice) as Coordinate | null
        const y2 = series.priceToCoordinate(refPrice + 0.25) as Coordinate | null
        if (y1 === null || y2 === null) continue
        const tickHeight = Math.abs(y2 - y1)
        const barHeight = Math.max(1, tickHeight - 1)

        const colWidth = barSpacing * 0.9

        for (const [price, volume] of bar.levels) {
          const y = series.priceToCoordinate(price) as Coordinate | null
          if (y === null) continue
          if (y < -barHeight || y > mediaSize.height + barHeight) continue

          const w = (volume / maxVol) * colWidth
          const isPoc = price === bar.poc

          ctx.fillStyle = isPoc
            ? "rgba(100, 160, 220, 0.50)"
            : "rgba(100, 130, 160, 0.25)"
          ctx.fillRect(x, y - barHeight / 2, w, barHeight)
        }
      }
    })
  }
}

// --- Pane View ---

class VolumeProfilePaneView implements IPrimitivePaneView {
  private _source: VolumeProfile
  private _renderer: VolumeProfileRenderer

  constructor(source: VolumeProfile) {
    this._source = source
    this._renderer = new VolumeProfileRenderer(source)
  }

  zOrder(): PrimitivePaneViewZOrder {
    return "bottom"
  }

  renderer(): IPrimitivePaneRenderer | null {
    return this._source.getBars().length > 0 ? this._renderer : null
  }
}

// --- Primitive ---

export class VolumeProfile {
  private _bars: VpBar[] = []
  private _series: ISeriesApi<SeriesType, Time> | null = null
  private _chart: IChartApi | null = null
  private _requestUpdate: (() => void) | null = null
  private _paneViews: VolumeProfilePaneView[]

  constructor() {
    this._paneViews = [new VolumeProfilePaneView(this)]
  }

  // --- Public API ---

  /** Replace the full dataset (on subscribe). */
  setData(bars: VpBar[]): void {
    this._bars = bars
    this._requestUpdate?.()
  }

  /** Process an array of VpBars from on_batch_end: same time → replace, new time → append. */
  updateBars(updates: VpBar[]): void {
    for (const bar of updates) {
      const last = this._bars.length > 0 ? this._bars[this._bars.length - 1] : null
      if (last && last.time === bar.time) {
        this._bars[this._bars.length - 1] = bar
      } else {
        this._bars.push(bar)
      }
    }
    this._requestUpdate?.()
  }

  getBars(): VpBar[] {
    return this._bars
  }

  getSeries(): ISeriesApi<SeriesType, Time> | null {
    return this._series
  }

  getChart(): IChartApi | null {
    return this._chart
  }

  // --- ISeriesPrimitive methods ---

  updateAllViews(): void {
    // No-op; pane view reads fresh data on each render.
  }

  paneViews(): readonly IPrimitivePaneView[] {
    return this._paneViews
  }

  attached(param: SeriesAttachedParameter<Time, SeriesType>): void {
    this._series = param.series
    this._chart = param.chart
    this._requestUpdate = param.requestUpdate
  }

  detached(): void {
    this._series = null
    this._chart = null
    this._requestUpdate = null
    this._bars = []
  }

  autoscaleInfo(_startTimePoint: Logical, _endTimePoint: Logical): AutoscaleInfo | null {
    void _startTimePoint
    void _endTimePoint

    if (this._bars.length === 0) return null

    let minPrice = Infinity
    let maxPrice = -Infinity

    for (const bar of this._bars) {
      for (const [price] of bar.levels) {
        if (price < minPrice) minPrice = price
        if (price > maxPrice) maxPrice = price
      }
    }

    if (minPrice === Infinity) return null

    return {
      priceRange: {
        minValue: minPrice,
        maxValue: maxPrice,
      },
    }
  }
}
