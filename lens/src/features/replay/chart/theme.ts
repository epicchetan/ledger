// Canvas fillStyle can't evaluate var() or color-mix(), so chart colors are
// resolved from the live theme through a probe element at chart creation. A
// host theme flip remounts the surface (replay keys it by theme), which calls
// chartColors() again against the freshly stamped tokens.

// lightweight-charts parses color strings with its own parser, which accepts
// only hex/rgb/hsl forms — but engines serialize computed color-mix() results
// as oklab(…) (Chromium) or color(srgb …) (WebKit), which that parser rejects
// from inside its render loop, aborting the whole paint (a blank chart, not a
// bad color). Normalize every resolved color to plain rgba() via a 1×1 canvas
// readback, which handles any color the engine itself understands.
let normalizer: CanvasRenderingContext2D | null = null

function toRgba(color: string): string {
  if (!normalizer) {
    const canvas = document.createElement("canvas")
    canvas.width = 1
    canvas.height = 1
    normalizer = canvas.getContext("2d", { willReadFrequently: true })
    if (!normalizer) return color
  }
  normalizer.clearRect(0, 0, 1, 1)
  normalizer.fillStyle = color
  normalizer.fillRect(0, 0, 1, 1)
  const [r, g, b, a] = normalizer.getImageData(0, 0, 1, 1).data
  return `rgba(${r}, ${g}, ${b}, ${Math.round((a / 255) * 1000) / 1000})`
}

// Resolves any CSS color expression (var(), color-mix(), …) to an rgba()
// string lightweight-charts' parser accepts. The probe must be in the document
// to compute; it inherits the :root tokens for the active theme.
export function resolveCssColor(expression: string): string {
  const probe = document.createElement("span")
  probe.style.color = expression
  document.body.appendChild(probe)
  const color = getComputedStyle(probe).color
  probe.remove()
  return toRgba(color)
}

// Canvas font strings can't evaluate var() either, so the mono stack is
// resolved from computed style like the colors. `--font-mono` is a Tailwind
// `@theme inline` token (not emitted as a :root custom property), so probing it
// bare resolves to the inherited sans stack; the nested fallback reads the
// underlying `--rmx-font-mono` (Menlo/Consolas) it aliases. Empty ⇒ monospace.
export function chartFontFamily(): string {
  const probe = document.createElement("span")
  probe.style.fontFamily = "var(--font-mono, var(--rmx-font-mono))"
  document.body.appendChild(probe)
  const family = getComputedStyle(probe).fontFamily
  probe.remove()
  return family || "monospace"
}

export interface ChartColors {
  background: string // var(--background) — solid, so labels/logo pick the right variant
  text: string // var(--muted-foreground)
  fontFamily: string // computed --font-mono stack (resolved once, beside the colors)
  grid: string // border, faded
  up: string // var(--success)
  down: string // var(--destructive)
  volume: string // foreground, faded — neutral (buy/sell coloring is a non-goal)
  crosshair: string // foreground, faded — the dashed crosshair lines
  crosshairLabel: string // var(--muted) — solid chip behind axis labels
  priceLine: string // foreground, faded — the dashed last-price line
}

export function chartColors(): ChartColors {
  return {
    background: resolveCssColor("var(--background)"),
    text: resolveCssColor("var(--muted-foreground)"),
    fontFamily: chartFontFamily(),
    grid: resolveCssColor(
      "color-mix(in oklab, var(--border) 55%, transparent)"
    ),
    up: resolveCssColor("var(--success)"),
    down: resolveCssColor("var(--destructive)"),
    volume: resolveCssColor(
      "color-mix(in oklab, var(--foreground) 22%, transparent)"
    ),
    crosshair: resolveCssColor(
      "color-mix(in oklab, var(--foreground) 20%, transparent)"
    ),
    crosshairLabel: resolveCssColor("var(--muted)"),
    priceLine: resolveCssColor(
      "color-mix(in oklab, var(--foreground) 25%, transparent)"
    ),
  }
}
