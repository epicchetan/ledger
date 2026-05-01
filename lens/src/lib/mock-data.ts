import type { CandlestickData, Time } from "lightweight-charts"

export function generateMockCandles(count = 200): CandlestickData<Time>[] {
  const candles: CandlestickData<Time>[] = []
  // 2024-01-15 09:30:00 UTC
  const startTime = Date.UTC(2024, 0, 15, 9, 30, 0) / 1000
  let price = 100

  for (let i = 0; i < count; i++) {
    const open = price
    const move1 = (Math.random() - 0.48) * 1.5
    const move2 = (Math.random() - 0.48) * 1.5
    const close = open + move1 + move2

    const high = Math.max(open, close) + Math.random() * 0.8
    const low = Math.min(open, close) - Math.random() * 0.8

    candles.push({
      time: (startTime + i * 60) as unknown as Time,
      open: Math.round(open * 100) / 100,
      high: Math.round(high * 100) / 100,
      low: Math.round(low * 100) / 100,
      close: Math.round(close * 100) / 100,
    })

    price = close
  }

  return candles
}
