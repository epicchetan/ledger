/* eslint-disable react-refresh/only-export-components */
import {
  ColorType,
  createChart,
  CrosshairMode,
  LineStyle,
  TrackingModeExitMode,
  type IChartApi,
  type Time,
} from "lightweight-charts"
import { useEffect, useRef, useSyncExternalStore } from "react"

import type { ChartLayer } from "@/features/replay/chart/layers"
import type { ChartColors } from "@/features/replay/chart/theme"
import { formatChartTime } from "@/features/replay/chart/time"
import { cn } from "@/lib/utils"

interface ChartSurfaceProps {
  layers: ChartLayer[]
  colors: ChartColors
  className?: string
}

// The only React↔chart bridge. The frame stream flows accumulator → layer →
// series entirely outside React; this component owns only the IChartApi
// lifecycle and the layer diff, so no state changes and nothing re-renders per
// frame.
export function ChartSurface({ layers, colors, className }: ChartSurfaceProps) {
  const containerRef = useRef<HTMLDivElement | null>(null)
  const chartRef = useRef<IChartApi | null>(null)
  const attachedRef = useRef<ChartLayer[]>([])

  // Chart lifecycle: created once per instance. The surface never restyles in
  // place — replay keys it by theme, so a host theme flip remounts it with a
  // freshly resolved `colors`; here that colors set is read exactly once.
  useEffect(() => {
    const container = containerRef.current
    if (!container) return
    const chart = createChart(container, {
      autoSize: true,
      layout: {
        background: { type: ColorType.Solid, color: colors.background },
        textColor: colors.text,
        fontFamily: colors.fontFamily,
        fontSize: 11,
        attributionLogo: false,
      },
      grid: {
        vertLines: { color: colors.grid },
        horzLines: { color: colors.grid },
      },
      crosshair: {
        mode: CrosshairMode.Magnet,
        vertLine: {
          color: colors.crosshair,
          style: LineStyle.Dashed,
          labelBackgroundColor: colors.crosshairLabel,
        },
        horzLine: {
          color: colors.crosshair,
          style: LineStyle.Dashed,
          labelBackgroundColor: colors.crosshairLabel,
        },
      },
      rightPriceScale: {
        borderVisible: false,
        scaleMargins: { top: 0.08, bottom: 0.24 },
      },
      timeScale: {
        borderVisible: false,
        rightOffset: 8,
      },
      // Long-press enters the crosshair; lifting the finger drops it (the
      // TradingView-app feel) instead of persisting until the next tap.
      trackingMode: { exitMode: TrackingModeExitMode.OnTouchEnd },
      localization: {
        timeFormatter: (time: Time) => formatChartTime(time as number),
      },
    })
    chartRef.current = chart
    return () => {
      for (const layer of attachedRef.current) layer.detach()
      attachedRef.current = []
      chart.remove()
      chartRef.current = null
    }
  }, [colors])

  // Layer diff: detach layers no longer present, attach new ones. attach()
  // performs the full initial sync from the layer's warm accumulator, so a tab
  // switch is old-detached (series removed) + new-attached (setData) — instant,
  // no refetch.
  useEffect(() => {
    const chart = chartRef.current
    if (!chart) return
    const attached = attachedRef.current
    for (const layer of attached) {
      if (!layers.includes(layer)) layer.detach()
    }
    for (const layer of layers) {
      if (!attached.includes(layer)) layer.attach(chart)
    }
    attachedRef.current = layers
  }, [layers])

  // touch-none keeps the browser from initiating native pan/zoom over the
  // canvas (the library's listeners are partly passive, so CSS is the only
  // reliable gate); select-none + no-callout stop the long-press-for-crosshair
  // gesture from triggering the OS text-selection magnifier/menu.
  return (
    <div
      ref={containerRef}
      className={cn(
        "touch-none select-none [-webkit-touch-callout:none]",
        className
      )}
    />
  )
}

// data-remux-theme is stamped on <html> by the host (see the @custom-variant in
// index.css); observe it so lens re-skins with the host.
export function useRemuxTheme(): string {
  return useSyncExternalStore(
    (onChange) => {
      const observer = new MutationObserver(onChange)
      observer.observe(document.documentElement, {
        attributeFilter: ["data-remux-theme"],
      })
      return () => observer.disconnect()
    },
    () => document.documentElement.getAttribute("data-remux-theme") ?? "dark"
  )
}
