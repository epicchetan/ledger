import type { IChartApi } from "lightweight-charts"

import type { BarsAccumulator } from "@/features/replay/accumulator"
import { barsLayer } from "@/features/replay/chart/bars-layer"
import type { ChartColors } from "@/features/replay/chart/theme"
import type { ReplayChartUiStore } from "@/features/replay/chart/ui-store"
import type { ReplayViewportStore } from "@/features/replay/chart/viewport-store"

// A projection kind's presence on the chart. attach() creates series /
// primitives and subscribes to the data source; detach() must remove every
// series it added and unsubscribe. Layers never touch the transport.
export interface ChartLayer {
  attach(chart: IChartApi): void
  detach(): void
}

export interface LayerContext {
  spec: string
  // Today every drawable kind is bars-frame-shaped; when a second frame kind
  // exists this widens to a union alongside the frame-protocol generalization
  // (see the spec's non-goals).
  accumulator: BarsAccumulator
  offsetSeconds: number
  colors: ChartColors
  // Ephemeral chart-control state. Bars and transport never enter this store.
  ui: ReplayChartUiStore
  // Durable, viewer-local time/price viewport policy.
  viewport: ReplayViewportStore
}

export type LayerFactory = (ctx: LayerContext) => ChartLayer

export const kindOf = (spec: string): string => spec.split(":", 1)[0]

// A spec whose kind has no entry is headless — the page never builds a layer
// for it. Adding a drawable kind is: write the factory, add one line.
export const CHART_LAYERS: Record<string, LayerFactory> = {
  bars: barsLayer,
}
