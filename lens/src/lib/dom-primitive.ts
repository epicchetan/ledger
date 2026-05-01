import type {
  ISeriesPrimitive,
  SeriesAttachedParameter,
  Time,
  SeriesType,
  IPrimitivePaneView,
  IPrimitivePaneRenderer,
} from "lightweight-charts"
import type { CanvasRenderingTarget2D } from "fancy-canvas"
import type { DomSnapshot } from "./ws-types"

const MIN_ROW_PX = 18
const BAR_HEIGHT = 14
const MAX_BAR_FRACTION = 0.06
const FONT = "10px Geist Mono, monospace"
const BUCKET_STEPS = [0.25, 0.5, 1, 2, 5, 10, 25, 50]

export class DomPrimitive implements ISeriesPrimitive<Time> {
  private _series: SeriesAttachedParameter<Time, SeriesType>["series"] | null = null
  private _requestUpdate: (() => void) | null = null
  private _snapshot: DomSnapshot | null = null
  private _paneView: DomPaneView

  constructor() {
    this._paneView = new DomPaneView(this)
  }

  attached(param: SeriesAttachedParameter<Time, SeriesType>): void {
    this._series = param.series
    this._requestUpdate = param.requestUpdate
  }

  detached(): void {
    this._series = null
    this._requestUpdate = null
  }

  update(snapshot: DomSnapshot | null): void {
    this._snapshot = snapshot
    this._requestUpdate?.()
  }

  paneViews(): readonly IPrimitivePaneView[] {
    return [this._paneView]
  }

  get series() { return this._series }
  get snapshot() { return this._snapshot }
}

class DomPaneView implements IPrimitivePaneView {
  private _primitive: DomPrimitive

  constructor(primitive: DomPrimitive) {
    this._primitive = primitive
  }

  zOrder(): "bottom" {
    return "bottom"
  }

  renderer(): IPrimitivePaneRenderer | null {
    return new DomRenderer(this._primitive)
  }
}

/** Round price down to nearest bucket boundary. */
function bucketFloor(price: number, bucket: number): number {
  return Math.floor(price / bucket) * bucket
}

/** Aggregate levels into buckets, summing sizes. */
function aggregateLevels(
  levels: [number, number, number][],
  bucket: number,
): [number, number][] {
  const map = new Map<number, number>()
  for (const [price, size] of levels) {
    const key = bucketFloor(price, bucket)
    map.set(key, (map.get(key) ?? 0) + size)
  }
  // Sort: for display we'll sort later per-side
  return Array.from(map.entries())
}

class DomRenderer implements IPrimitivePaneRenderer {
  private _primitive: DomPrimitive

  constructor(primitive: DomPrimitive) {
    this._primitive = primitive
  }

  draw(target: CanvasRenderingTarget2D): void {
    const series = this._primitive.series
    const snapshot = this._primitive.snapshot
    if (!series || !snapshot) return

    target.useMediaCoordinateSpace((scope) => {
      const ctx = scope.context
      const { width, height } = scope.mediaSize
      const { bids, asks } = snapshot

      if (bids.length === 0 && asks.length === 0) return

      // Determine bucket size from current zoom
      // Measure pixel distance for one tick (0.25)
      const refPrice = bids[0]?.[0] ?? asks[0]?.[0] ?? 0
      const y1 = series.priceToCoordinate(refPrice)
      const y2 = series.priceToCoordinate(refPrice + 0.25)

      let bucket = BUCKET_STEPS[BUCKET_STEPS.length - 1]
      if (y1 !== null && y2 !== null) {
        const pxPerTick = Math.abs(y1 - y2)
        // Find smallest bucket where pixel spacing >= MIN_ROW_PX
        for (const step of BUCKET_STEPS) {
          const ticks = step / 0.25
          if (pxPerTick * ticks >= MIN_ROW_PX) {
            bucket = step
            break
          }
        }
      }

      // Aggregate
      const aggBids = aggregateLevels(bids, bucket)
      const aggAsks = aggregateLevels(asks, bucket)

      // Sort bids descending, asks ascending
      aggBids.sort((a, b) => b[0] - a[0])
      aggAsks.sort((a, b) => a[0] - b[0])

      const maxSize = Math.max(
        ...aggBids.map((l) => l[1]),
        ...aggAsks.map((l) => l[1]),
        1,
      )

      const maxBarWidth = width * MAX_BAR_FRACTION
      ctx.font = FONT
      ctx.textBaseline = "middle"

      // Asks
      for (let i = 0; i < aggAsks.length; i++) {
        const [price, size] = aggAsks[i]
        const y = series.priceToCoordinate(price)
        if (y === null || y < -BAR_HEIGHT || y > height + BAR_HEIGHT) continue

        const barW = (size / maxSize) * maxBarWidth
        const isBest = i === 0

        ctx.fillStyle = isBest ? "rgba(239,83,80,0.16)" : "rgba(239,83,80,0.10)"
        ctx.fillRect(width - barW, y - BAR_HEIGHT / 2, barW, BAR_HEIGHT)

        // Size at the tip
        ctx.fillStyle = isBest ? "rgba(239,83,80,0.8)" : "rgba(239,83,80,0.55)"
        ctx.textAlign = "right"
        ctx.fillText(String(size), width - barW - 3, y)

      }

      // Bids
      for (let i = 0; i < aggBids.length; i++) {
        const [price, size] = aggBids[i]
        const y = series.priceToCoordinate(price)
        if (y === null || y < -BAR_HEIGHT || y > height + BAR_HEIGHT) continue

        const barW = (size / maxSize) * maxBarWidth
        const isBest = i === 0

        ctx.fillStyle = isBest ? "rgba(38,166,154,0.16)" : "rgba(38,166,154,0.10)"
        ctx.fillRect(width - barW, y - BAR_HEIGHT / 2, barW, BAR_HEIGHT)

        // Size at the tip
        ctx.fillStyle = isBest ? "rgba(38,166,154,0.8)" : "rgba(38,166,154,0.55)"
        ctx.textAlign = "right"
        ctx.fillText(String(size), width - barW - 3, y)

      }
    })
  }
}
