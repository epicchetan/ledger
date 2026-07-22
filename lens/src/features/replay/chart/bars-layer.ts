import {
  CandlestickSeries,
  LineStyle,
  type CandlestickData,
  type IChartApi,
  type ISeriesApi,
  type LogicalRange,
  type UTCTimestamp,
} from "lightweight-charts"

import type { BarsSnapshot } from "@/features/replay/accumulator"
import type {
  ChartLayer,
  LayerContext,
  LayerFactory,
} from "@/features/replay/chart/layers"
import {
  intervalSecondsOf,
  nextBarChartTime,
} from "@/features/replay/chart/time"
import {
  centeredTimeViewport,
  defaultTimeViewport,
  INITIAL_VISIBLE_BARS,
  logicalRangeEqual,
  timeRangeForViewport,
} from "@/features/replay/chart/viewport-policy"
import type { TimeViewport } from "@/features/replay/chart/viewport-store"
import { isTickBarsSpec } from "@/features/replay/projection-spec"
import { TICK_SIZE, type Bar } from "@/features/replay/types"

// Above this many appended bars in one publish, a single setData of the caches
// beats a per-bar update() loop (a big catch-up frame).
const APPEND_UPDATE_MAX = 4

// The bars base layer: one candlestick series. Owns a mapped-data cache
// parallel to the accumulator's bars array, classifies each publish as
// append-or-reset by reference identity (see apply), and feeds the
// viewport/follow stores since it holds the chart and the data — the surface
// stays layer-agnostic.
class BarsLayer implements ChartLayer {
  private readonly ctx: LayerContext
  private chart: IChartApi | null = null
  private candles: ISeriesApi<"Candlestick"> | null = null
  private unsubscribe: (() => void) | null = null
  private paintFrame: number | null = null

  // Parallel to the accumulator's bars. sourceBars holds the accumulator array
  // by reference so the append check can compare element identity; the live bar
  // is never written into these (each frame re-carries it).
  private sourceBars: Bar[] = []
  private candleData: CandlestickData[] = []
  private epoch: number | null = null
  private live: Bar | null = null
  private chartEl: HTMLElement | null = null
  private readonly activePointers = new Set<number>()
  private gestureStartRange: LogicalRange | null = null
  private interactionSettleTimer: ReturnType<typeof setTimeout> | null = null
  private timeInteractionPending = false

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
    // Tick bars are event-anchored and can begin at any second; the 1s time
    // interval also needs seconds, while 1m and coarser time bars do not.
    chart.timeScale().applyOptions({
      timeVisible: true,
      secondsVisible:
        isTickBarsSpec(spec) || (intervalSecondsOf(spec) ?? 60) < 60,
    })
    chart.timeScale().subscribeVisibleLogicalRangeChange(this.onRangeChange)

    this.ctx.ui.getState().setJumpToLatest(this.centerLatest)
    this.chartEl = chart.chartElement()
    this.chartEl.addEventListener("pointerdown", this.onPointerDown)
    this.chartEl.addEventListener("pointerup", this.onPointerEnd)
    this.chartEl.addEventListener("pointercancel", this.onPointerEnd)
    this.chartEl.addEventListener("wheel", this.onWheel, { passive: true })

    this.unsubscribe = this.ctx.accumulator.subscribe(this.onPublish)
    // Render existing data without waiting for the next frame.
    this.onPublish()
  }

  detach(): void {
    this.capturePriceViewport()
    this.unsubscribe?.()
    this.unsubscribe = null
    if (this.paintFrame !== null) {
      cancelAnimationFrame(this.paintFrame)
      this.paintFrame = null
    }
    if (this.chart) {
      this.chart
        .timeScale()
        .unsubscribeVisibleLogicalRangeChange(this.onRangeChange)
      if (this.candles) this.chart.removeSeries(this.candles)
    }
    if (this.chartEl) {
      this.chartEl.removeEventListener("pointerdown", this.onPointerDown)
      this.chartEl.removeEventListener("pointerup", this.onPointerEnd)
      this.chartEl.removeEventListener("pointercancel", this.onPointerEnd)
      this.chartEl.removeEventListener("wheel", this.onWheel)
      this.chartEl = null
    }
    if (this.interactionSettleTimer) clearTimeout(this.interactionSettleTimer)
    this.interactionSettleTimer = null
    this.activePointers.clear()
    this.gestureStartRange = null
    this.timeInteractionPending = false
    this.ctx.ui.getState().setJumpToLatest(null)
    this.chart = null
    this.candles = null
    this.sourceBars = []
    this.candleData = []
    this.epoch = null
    this.live = null
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
    if (!this.candles) return
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

    // Live bar last, always. Its display key is derived against the same
    // completed tail on every update, so the forming candle is replaced in
    // place even when its event anchor collides with the previous tick bar.
    // Never cached — a setData that dropped it is repaired in this publish.
    if (snap.live) {
      this.candles.update(
        this.toCandle(snap.live, this.lastCompletedChartTime())
      )
    }
  }

  // Full reset: epoch change, first data, or an overlap rewrite. Recenters the
  // viewport only when the epoch changed (seek, first fill).
  private reset(snap: BarsSnapshot, recenter: boolean): void {
    if (!this.candles || !this.chart) return
    const previousEpoch = this.epoch
    this.epoch = snap.epoch
    this.sourceBars = snap.bars
    this.candleData = []
    let previousTime: number | null = null
    for (const bar of snap.bars) {
      const candle = this.toCandle(bar, previousTime)
      this.candleData.push(candle)
      previousTime = candle.time as number
    }
    this.candles.setData(this.candleData)
    if (recenter) {
      const n = snap.bars.length + (snap.live ? 1 : 0)
      const storedTime = this.ctx.viewport.getState().viewport.time
      const viewport =
        previousEpoch === null
          ? (storedTime ?? defaultTimeViewport(n))
          : centeredTimeViewport(storedTime, n)
      this.ctx.viewport.getState().setTime(viewport)
      this.applyTimeViewport(viewport, n)
    }
    this.applyPriceViewport()
  }

  private append(snap: BarsSnapshot): void {
    if (!this.candles || !this.chart) return
    const prevCount = this.sourceBars.length
    const k = snap.bars.length - prevCount

    const perBar = k <= APPEND_UPDATE_MAX
    let previousTime = this.lastCompletedChartTime()
    for (let i = prevCount; i < snap.bars.length; i++) {
      const candle = this.toCandle(snap.bars[i], previousTime)
      this.candleData.push(candle)
      previousTime = candle.time as number
      if (perBar) {
        this.candles.update(candle)
      }
    }
    this.sourceBars = snap.bars
    // One setData beats thousands of updates on a big catch-up frame.
    if (!perBar) {
      this.candles.setData(this.candleData)
    }

    const timeViewport = this.ctx.viewport.getState().viewport.time
    if (
      timeViewport?.mode === "follow" &&
      this.activePointers.size === 0 &&
      !this.timeInteractionPending
    ) {
      this.applyTimeViewport(
        timeViewport,
        snap.bars.length + (snap.live ? 1 : 0)
      )
    }
  }

  private onPointerDown = (event: PointerEvent): void => {
    if (this.activePointers.size === 0) {
      if (this.interactionSettleTimer) {
        clearTimeout(this.interactionSettleTimer)
        this.interactionSettleTimer = null
      }
      this.gestureStartRange =
        this.chart?.timeScale().getVisibleLogicalRange() ?? null
      this.timeInteractionPending = true
    }
    this.activePointers.add(event.pointerId)
  }

  private onPointerEnd = (event: PointerEvent): void => {
    this.activePointers.delete(event.pointerId)
    if (this.activePointers.size > 0) return
    this.scheduleInteractionCapture()
  }

  private onWheel = (): void => {
    if (!this.timeInteractionPending) {
      this.gestureStartRange =
        this.chart?.timeScale().getVisibleLogicalRange() ?? null
      this.timeInteractionPending = true
    }
    this.scheduleInteractionCapture()
  }

  private onRangeChange = (): void => {
    if (this.timeInteractionPending && this.activePointers.size === 0) {
      this.scheduleInteractionCapture()
    }
  }

  private scheduleInteractionCapture(): void {
    if (this.interactionSettleTimer) clearTimeout(this.interactionSettleTimer)
    this.interactionSettleTimer = setTimeout(() => {
      this.interactionSettleTimer = null
      this.captureTimeViewport(this.gestureStartRange)
      this.capturePriceViewport()
      this.gestureStartRange = null
      this.timeInteractionPending = false
      const timeViewport = this.ctx.viewport.getState().viewport.time
      if (timeViewport?.mode === "follow") {
        this.applyTimeViewport(
          timeViewport,
          this.candleData.length + (this.live ? 1 : 0)
        )
      }
    }, 160)
  }

  // Keep the user's current zoom while placing the newest candle at the
  // viewport midpoint. Follow mode is explicit: later appends move the range
  // only while this policy remains active.
  private centerLatest = (): void => {
    if (!this.chart) return
    const range = this.chart.timeScale().getVisibleLogicalRange()
    const visibleBars = range ? range.to - range.from : INITIAL_VISIBLE_BARS
    const viewport: TimeViewport = {
      mode: "follow",
      visibleBars,
      latestFraction: 0.5,
    }
    this.ctx.viewport.getState().setTime(viewport)
    this.applyTimeViewport(
      viewport,
      this.candleData.length + (this.live ? 1 : 0)
    )
  }

  private applyTimeViewport(viewport: TimeViewport, barCount: number): void {
    if (!this.chart) return
    this.chart
      .timeScale()
      .setVisibleLogicalRange(timeRangeForViewport(viewport, barCount))
  }

  private applyPriceViewport(): void {
    if (!this.chart) return
    const priceScale = this.chart.priceScale("right")
    const viewport = this.ctx.viewport.getState().viewport.price
    if (viewport.mode === "auto") {
      priceScale.setAutoScale(true)
    } else {
      priceScale.setVisibleRange(viewport.range)
    }
  }

  private captureTimeViewport(start: LogicalRange | null): void {
    if (!this.chart) return
    const range = this.chart.timeScale().getVisibleLogicalRange()
    if (!range || (start && logicalRangeEqual(start, range))) return
    this.ctx.viewport.getState().setTime({
      mode: "fixed",
      range: { from: range.from, to: range.to },
    })
  }

  private capturePriceViewport(): void {
    if (!this.chart) return
    const priceScale = this.chart.priceScale("right")
    if (priceScale.options().autoScale) {
      this.ctx.viewport.getState().setPrice({ mode: "auto" })
      return
    }
    const range = priceScale.getVisibleRange()
    if (!range || range.to <= range.from) return
    this.ctx.viewport.getState().setPrice({
      mode: "manual",
      range: { from: range.from, to: range.to },
    })
  }

  private lastCompletedChartTime(): number | null {
    return (this.candleData.at(-1)?.time as number | undefined) ?? null
  }

  private toCandle(bar: Bar, previousTime: number | null): CandlestickData {
    return {
      time: nextBarChartTime(
        bar.intervalStartNs,
        previousTime,
        this.ctx.offsetSeconds
      ) as UTCTimestamp,
      open: bar.open * TICK_SIZE,
      high: bar.high * TICK_SIZE,
      low: bar.low * TICK_SIZE,
      close: bar.close * TICK_SIZE,
    }
  }
}

export const barsLayer: LayerFactory = (ctx) => new BarsLayer(ctx)
