import {
  CandlestickSeries,
  HistogramSeries,
  LineStyle,
  type CandlestickData,
  type HistogramData,
  type IChartApi,
  type ISeriesApi,
  type LogicalRange,
  type MouseEventParams,
  type Time,
  type UTCTimestamp,
} from "lightweight-charts"

import type { BarsSnapshot } from "@/features/replay/accumulator"
import type {
  ChartLayer,
  LayerContext,
  LayerFactory,
} from "@/features/replay/chart/layers"
import { intervalSecondsOf, nsToSeconds } from "@/features/replay/chart/time"
import type { LegendData } from "@/features/replay/chart/ui-state"
import { TICK_SIZE, type Bar } from "@/features/replay/types"

// Above this many appended bars in one publish, a single setData of the caches
// beats a per-bar update() loop (a big catch-up frame).
const APPEND_UPDATE_MAX = 4

// Epoch-reset viewport: up to this many bars visible, plus right-hand
// whitespace matching the time scale's rightOffset so the epoch-reset recenter
// and the live-edge resting position agree. The window clamps down toward the
// actual bar count (never below MIN_VISIBLE_BARS) so a sparse epoch — a seek
// landing minutes after the open on a coarse interval — draws readable candles
// instead of stretching a 120-bar window around five bars.
const INITIAL_VISIBLE_BARS = 120
const MIN_VISIBLE_BARS = 30
const RIGHT_PAD = 8

// The bars base layer: candlestick + volume histogram. Owns two series and a
// mapped-data cache parallel to the accumulator's bars array, classifies each
// publish as append-or-reset by reference identity (see apply), and feeds the
// overlay store (legend, at-edge, jump-to-live) since it holds the chart, both
// series, and the data — the surface stays layer-agnostic.
class BarsLayer implements ChartLayer {
  private readonly ctx: LayerContext
  private chart: IChartApi | null = null
  private candles: ISeriesApi<"Candlestick"> | null = null
  private volume: ISeriesApi<"Histogram"> | null = null
  private unsubscribe: (() => void) | null = null
  private paintFrame: number | null = null

  // Parallel to the accumulator's bars. sourceBars holds the accumulator array
  // by reference so the append check can compare element identity; the live bar
  // is never written into these (each frame re-carries it).
  private sourceBars: Bar[] = []
  private candleData: CandlestickData[] = []
  private volumeData: HistogramData[] = []
  private epoch: number | null = null

  // Overlay bookkeeping: the current live bar, whether the crosshair maps to a
  // bar, and the latest bar's legend (live if forming, else last completed) —
  // the fallback shown when the crosshair leaves and the value tracked per
  // frame while not hovering.
  private live: Bar | null = null
  private hovering = false
  private latest: LegendData | null = null
  // The chart's DOM element, kept so the pointerup probe can be unbound.
  private chartEl: HTMLElement | null = null

  constructor(ctx: LayerContext) {
    this.ctx = ctx
  }

  attach(chart: IChartApi): void {
    const { colors, spec } = this.ctx
    this.chart = chart
    this.candles = chart.addSeries(CandlestickSeries, {
      upColor: colors.up,
      downColor: colors.down,
      wickUpColor: colors.up,
      wickDownColor: colors.down,
      borderVisible: false,
      priceFormat: { type: "price", precision: 2, minMove: TICK_SIZE },
      lastValueVisible: true,
      priceLineVisible: true,
      priceLineWidth: 1,
      priceLineStyle: LineStyle.Dashed,
      priceLineColor: colors.priceLine,
    })
    // Classic under-candle overlay on its own scale, no extra pane.
    this.volume = chart.addSeries(HistogramSeries, {
      priceScaleId: "volume",
      priceFormat: { type: "volume" },
      color: colors.volume,
      lastValueVisible: false,
      priceLineVisible: false,
    })
    chart.priceScale("volume").applyOptions({
      scaleMargins: { top: 0.82, bottom: 0 },
    })
    // The 1s tab shows seconds on the axis; 1m and coarser don't.
    chart.timeScale().applyOptions({
      timeVisible: true,
      secondsVisible: (intervalSecondsOf(spec) ?? 60) < 60,
    })

    chart.subscribeCrosshairMove(this.onCrosshairMove)
    chart.timeScale().subscribeVisibleLogicalRangeChange(this.onRangeChange)
    this.ctx.ui.jumpToLive = () => chart.timeScale().scrollToRealTime()
    this.ctx.ui.resetPriceScale = () => {
      chart.priceScale("right").applyOptions({ autoScale: true })
      this.ctx.ui.setAutoScale(true)
    }
    // lightweight-charts fires no price-scale mode event, so autoScale is read
    // from two probes: each publish (covers playback) and this pointerup (covers
    // a paused user dragging the axis; the double-tap reset lands here too).
    this.chartEl = chart.chartElement()
    this.chartEl.addEventListener("pointerup", this.onPointerUp)

    this.unsubscribe = this.ctx.accumulator.subscribe(this.onPublish)
    // Render existing data without waiting for the next frame.
    this.onPublish()
  }

  detach(): void {
    this.unsubscribe?.()
    this.unsubscribe = null
    if (this.paintFrame !== null) {
      cancelAnimationFrame(this.paintFrame)
      this.paintFrame = null
    }
    if (this.chart) {
      this.chart.unsubscribeCrosshairMove(this.onCrosshairMove)
      this.chart
        .timeScale()
        .unsubscribeVisibleLogicalRangeChange(this.onRangeChange)
      if (this.candles) this.chart.removeSeries(this.candles)
      if (this.volume) this.chart.removeSeries(this.volume)
    }
    if (this.chartEl) {
      this.chartEl.removeEventListener("pointerup", this.onPointerUp)
      this.chartEl = null
    }
    this.ctx.ui.jumpToLive = null
    this.ctx.ui.resetPriceScale = null
    this.ctx.ui.setLegend(null)
    this.ctx.ui.setAtEdge(true)
    this.ctx.ui.setAutoScale(true)
    this.chart = null
    this.candles = null
    this.volume = null
    this.sourceBars = []
    this.candleData = []
    this.volumeData = []
    this.epoch = null
    this.live = null
    this.hovering = false
    this.latest = null
  }

  // A rendering bug must degrade to a stale chart, never a white screen.
  private onPublish = (): void => {
    if (this.paintFrame !== null) return
    this.paintFrame = requestAnimationFrame(() => {
      this.paintFrame = null
      try {
        this.apply(this.ctx.accumulator.getSnapshot())
      } catch (error) {
        console.warn(`[replay] ${this.ctx.spec}: chart apply failed`, error)
      }
    })
  }

  private apply(snap: BarsSnapshot): void {
    if (!this.candles || !this.volume) return
    this.live = snap.live

    // Append iff same epoch, no shrink, and the old tail element is the same
    // object in the new array (accumulator appends share references; the
    // overlap/backfill branch rebuilds objects, breaking identity — exactly the
    // case that must force a full reset).
    const prev = this.sourceBars
    const appendable =
      snap.epoch === this.epoch &&
      snap.bars.length >= prev.length &&
      (prev.length === 0 ||
        snap.bars[prev.length - 1] === prev[prev.length - 1])

    if (!appendable) {
      this.reset(snap, snap.epoch !== this.epoch)
    } else if (snap.bars.length > prev.length) {
      this.append(snap)
    }

    // Live bar last, always: same interval start ⇒ same time key ⇒ the forming
    // candle is replaced in place. Never cached — a setData that dropped it is
    // repaired within this same publish.
    if (snap.live) {
      this.candles.update(this.toCandle(snap.live))
      this.volume.update(this.toVolume(snap.live))
    }

    // Track the forming candle in the legend (the one behavior upgrade over the
    // old chart); store equality keeps the per-frame write cheap.
    this.latest = this.computeLatest(snap)
    if (!this.hovering) this.ctx.ui.setLegend(this.latest)

    // Probe the price scale each publish (covers a running chart re-fitting);
    // the store's equality gate makes the repeated write free.
    this.syncAutoScale()
  }

  // Full reset: epoch change, first data, or an overlap rewrite. Recenters the
  // viewport only when the epoch changed (seek, first fill).
  private reset(snap: BarsSnapshot, recenter: boolean): void {
    if (!this.candles || !this.volume || !this.chart) return
    this.epoch = snap.epoch
    this.sourceBars = snap.bars
    this.candleData = snap.bars.map((bar) => this.toCandle(bar))
    this.volumeData = snap.bars.map((bar) => this.toVolume(bar))
    this.candles.setData(this.candleData)
    this.volume.setData(this.volumeData)
    if (recenter) {
      const n = snap.bars.length + (snap.live ? 1 : 0)
      const window = Math.min(
        INITIAL_VISIBLE_BARS,
        Math.max(n, MIN_VISIBLE_BARS)
      )
      this.chart.timeScale().setVisibleLogicalRange({
        from: n - window,
        to: n + RIGHT_PAD,
      })
    }
    this.writeAtEdge(this.chart.timeScale().getVisibleLogicalRange())
  }

  private append(snap: BarsSnapshot): void {
    if (!this.candles || !this.volume || !this.chart) return
    const prevCount = this.sourceBars.length
    const k = snap.bars.length - prevCount

    // Follow the right edge only if the user was already there; a catch-up
    // flood must not yank a user who panned back to inspect.
    const range = this.chart.timeScale().getVisibleLogicalRange()
    const atEdge = range === null || range.to >= prevCount - 1

    const perBar = k <= APPEND_UPDATE_MAX
    for (let i = prevCount; i < snap.bars.length; i++) {
      const candle = this.toCandle(snap.bars[i])
      const vol = this.toVolume(snap.bars[i])
      this.candleData.push(candle)
      this.volumeData.push(vol)
      if (perBar) {
        this.candles.update(candle)
        this.volume.update(vol)
      }
    }
    this.sourceBars = snap.bars
    // One setData beats thousands of updates on a big catch-up frame.
    if (!perBar) {
      this.candles.setData(this.candleData)
      this.volume.setData(this.volumeData)
    }

    if (atEdge) this.chart.timeScale().scrollToRealTime()
    this.writeAtEdge(this.chart.timeScale().getVisibleLogicalRange())
  }

  // Legend and jump-to-live wiring. The crosshair handler narrows the union
  // series data with an `"open" in` guard (as the reference chart did); the
  // range handler drives the at-edge display state (a lagging mirror of the
  // append follow decision — display-only, may trail it by one event).
  private onCrosshairMove = (param: MouseEventParams<Time>): void => {
    if (!this.candles || !this.volume) return
    const candle = param.seriesData.get(this.candles) as
      | CandlestickData
      | undefined
    if (candle && "open" in candle) {
      const vol = param.seriesData.get(this.volume) as HistogramData | undefined
      this.hovering = true
      this.ctx.ui.setLegend({
        open: candle.open,
        high: candle.high,
        low: candle.low,
        close: candle.close,
        volume: vol?.value ?? 0,
        up: candle.close >= candle.open,
      })
    } else {
      // Crosshair left a bar: fall back to the latest bar rather than blanking.
      this.hovering = false
      this.ctx.ui.setLegend(this.latest)
    }
  }

  private onRangeChange = (range: LogicalRange | null): void => {
    this.writeAtEdge(range)
  }

  // Reflects the right price scale's auto-fit mode into the store so the "Auto"
  // pill appears once the user drags the axis into manual mode.
  private onPointerUp = (): void => {
    this.syncAutoScale()
  }

  private syncAutoScale(): void {
    if (!this.chart) return
    this.ctx.ui.setAutoScale(this.chart.priceScale("right").options().autoScale)
  }

  private writeAtEdge(range: LogicalRange | null): void {
    const barCount = this.candleData.length + (this.live ? 1 : 0)
    this.ctx.ui.setAtEdge(range === null || range.to >= barCount - 1)
  }

  // The latest bar for the legend: the forming live bar if present, else the
  // last completed bar; null on an empty epoch.
  private computeLatest(snap: BarsSnapshot): LegendData | null {
    if (snap.live) return this.legendOf(snap.live)
    if (snap.bars.length === 0) return null
    return this.legendOf(snap.bars[snap.bars.length - 1])
  }

  private legendOf(bar: Bar): LegendData {
    return {
      open: bar.open * TICK_SIZE,
      high: bar.high * TICK_SIZE,
      low: bar.low * TICK_SIZE,
      close: bar.close * TICK_SIZE,
      volume: bar.volume,
      up: bar.close >= bar.open,
    }
  }

  private timeOf(bar: Bar): UTCTimestamp {
    return (nsToSeconds(bar.intervalStartNs) +
      this.ctx.offsetSeconds) as UTCTimestamp
  }

  private toCandle(bar: Bar): CandlestickData {
    return {
      time: this.timeOf(bar),
      open: bar.open * TICK_SIZE,
      high: bar.high * TICK_SIZE,
      low: bar.low * TICK_SIZE,
      close: bar.close * TICK_SIZE,
    }
  }

  private toVolume(bar: Bar): HistogramData {
    return {
      time: this.timeOf(bar),
      value: bar.volume,
      color: this.ctx.colors.volume,
    }
  }
}

export const barsLayer: LayerFactory = (ctx) => new BarsLayer(ctx)
