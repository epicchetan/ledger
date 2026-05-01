import { useEffect, useRef, useState, type MutableRefObject } from "react"
import {
  createChart,
  CrosshairMode,
  type IChartApi,
  type ISeriesApi,
  type CandlestickData,
  type Time,
  type LogicalRange,
  CandlestickSeries,
} from "lightweight-charts"
import { ChevronRight } from "lucide-react"

import { cn } from "@/lib/utils"
import { DomPrimitive } from "@/lib/dom-primitive"
import type { DomSnapshot } from "@/lib/ws-types"

const RIGHT_OFFSET = 8

interface OhlcValues {
  open: number
  high: number
  low: number
  close: number
}

interface ChartProps {
  className?: string
  seriesRef: MutableRefObject<ISeriesApi<"Candlestick", Time, CandlestickData<Time>> | null>
  chartRef: MutableRefObject<IChartApi | null>
  domSnapshot?: DomSnapshot | null
  mode?: "candles" | "vp"
}

export function Chart({ className, seriesRef, chartRef, domSnapshot, mode = "candles" }: ChartProps) {
  const containerRef = useRef<HTMLDivElement>(null)
  const domPrimitiveRef = useRef<DomPrimitive | null>(null)
  const [atEdge, setAtEdge] = useState(true)
  const [legend, setLegend] = useState<OhlcValues | null>(null)

  function scrollToRealTime() {
    chartRef.current?.timeScale().scrollToRealTime()
  }

  useEffect(() => {
    const container = containerRef.current
    if (!container) return

    const chart = createChart(container, {
      autoSize: true,
      layout: {
        background: { color: "transparent" },
        textColor: "#787b86",
        fontFamily: "Geist Mono, monospace",
        fontSize: 11,
        attributionLogo: false,
      },
      grid: {
        vertLines: { color: "rgba(255,255,255,0.04)" },
        horzLines: { color: "rgba(255,255,255,0.04)" },
      },
      crosshair: {
        mode: CrosshairMode.Magnet,
        vertLine: {
          color: "rgba(255,255,255,0.2)",
          style: 2,
          labelBackgroundColor: "#363a45",
        },
        horzLine: {
          color: "rgba(255,255,255,0.2)",
          style: 2,
          labelBackgroundColor: "#363a45",
        },
      },
      rightPriceScale: {
        borderVisible: false,
        scaleMargins: { top: 0.25, bottom: 0.25 },
      },
      timeScale: {
        borderVisible: false,
        timeVisible: true,
        secondsVisible: false,
        rightOffset: RIGHT_OFFSET,
      },
    })

    const series = chart.addSeries(CandlestickSeries, mode === "vp"
      ? {
          upColor: "transparent",
          downColor: "transparent",
          borderUpColor: "transparent",
          borderDownColor: "transparent",
          wickUpColor: "transparent",
          wickDownColor: "transparent",
          lastValueVisible: false,
          priceLineVisible: false,
        }
      : {
          upColor: "#26a69a",
          downColor: "#ef5350",
          borderUpColor: "#26a69a",
          borderDownColor: "#ef5350",
          wickUpColor: "#26a69a",
          wickDownColor: "#ef5350",
          lastValueVisible: true,
          priceLineVisible: true,
          priceLineWidth: 1,
          priceLineColor: "rgba(255,255,255,0.25)",
          priceLineStyle: 2,
        },
    )

    // Track whether user is at the live edge
    chart.timeScale().subscribeVisibleLogicalRangeChange((range: LogicalRange | null) => {
      if (!range) return
      const series_ = seriesRef.current
      if (!series_) return
      const data = series_.data()
      if (!data || data.length === 0) return
      const lastIndex = data.length - 1
      const isAtEdge = range.to >= lastIndex - 1
      setAtEdge(isAtEdge)
    })

    // OHLC legend on crosshair move (candles mode only)
    if (mode === "candles") {
      chart.subscribeCrosshairMove((param) => {
        const candleData = param.seriesData.get(series) as CandlestickData<Time> | undefined
        if (candleData && "open" in candleData) {
          setLegend({
            open: candleData.open,
            high: candleData.high,
            low: candleData.low,
            close: candleData.close,
          })
        } else {
          // Show latest bar when crosshair leaves
          const data = series.data() as CandlestickData<Time>[]
          if (data.length > 0) {
            const last = data[data.length - 1]
            setLegend({
              open: last.open,
              high: last.high,
              low: last.low,
              close: last.close,
            })
          } else {
            setLegend(null)
          }
        }
      })
    }

    // Keyboard navigation
    function handleKeyDown(e: KeyboardEvent) {
      const ts = chart.timeScale()
      const range = ts.getVisibleLogicalRange()
      if (!range) return

      const span = range.to - range.from
      const step = Math.max(1, Math.floor(span * 0.1))

      switch (e.key) {
        case "ArrowLeft":
          e.preventDefault()
          ts.setVisibleLogicalRange({ from: range.from - step, to: range.to - step })
          break
        case "ArrowRight":
          e.preventDefault()
          ts.setVisibleLogicalRange({ from: range.from + step, to: range.to + step })
          break
        case "ArrowUp":
        case "+":
        case "=":
          e.preventDefault()
          ts.setVisibleLogicalRange({
            from: range.from + step,
            to: range.to - step,
          })
          break
        case "ArrowDown":
        case "-":
          e.preventDefault()
          ts.setVisibleLogicalRange({
            from: range.from - step,
            to: range.to + step,
          })
          break
        case "Home":
          e.preventDefault()
          ts.setVisibleLogicalRange({ from: 0, to: span })
          break
        case "End":
          e.preventDefault()
          ts.scrollToRealTime()
          break
      }
    }

    container.tabIndex = 0
    container.style.outline = "none"
    container.addEventListener("keydown", handleKeyDown)

    if (mode === "candles") {
      const domPrimitive = new DomPrimitive()
      series.attachPrimitive(domPrimitive)
      domPrimitiveRef.current = domPrimitive
    }

    chartRef.current = chart
    seriesRef.current = series as typeof seriesRef.current

    return () => {
      container.removeEventListener("keydown", handleKeyDown)
      chart.remove()
      chartRef.current = null
      seriesRef.current = null
      domPrimitiveRef.current = null
    }
  }, [chartRef, seriesRef, mode])

  // Update DOM primitive when snapshot changes
  useEffect(() => {
    domPrimitiveRef.current?.update(domSnapshot ?? null)
  }, [domSnapshot])

  return (
    <div ref={containerRef} className={cn("relative bg-background", className)}>
      {/* OHLCV Legend (candles mode only) */}
      {mode === "candles" && legend && (
        <div className="pointer-events-none absolute top-2 left-2 z-10 flex gap-3 font-mono text-[11px] leading-none">
          <span className="text-muted-foreground">
            O <span className={legend.close >= legend.open ? "text-[#26a69a]" : "text-[#ef5350]"}>{legend.open.toFixed(2)}</span>
          </span>
          <span className="text-muted-foreground">
            H <span className={legend.close >= legend.open ? "text-[#26a69a]" : "text-[#ef5350]"}>{legend.high.toFixed(2)}</span>
          </span>
          <span className="text-muted-foreground">
            L <span className={legend.close >= legend.open ? "text-[#26a69a]" : "text-[#ef5350]"}>{legend.low.toFixed(2)}</span>
          </span>
          <span className="text-muted-foreground">
            C <span className={legend.close >= legend.open ? "text-[#26a69a]" : "text-[#ef5350]"}>{legend.close.toFixed(2)}</span>
          </span>
        </div>
      )}

      {/* Scroll to realtime button */}
      {!atEdge && (
        <button
          onClick={scrollToRealTime}
          className="absolute right-16 bottom-8 z-10 flex size-7 items-center justify-center rounded border border-border/50 bg-card/90 text-muted-foreground transition-colors hover:bg-accent hover:text-foreground"
        >
          <ChevronRight className="size-4" />
        </button>
      )}
    </div>
  )
}
