import { BarsAccumulator } from "@/features/replay/accumulator"
import type {
  Bar,
  BarsPosition,
  BarsProjectionFrame,
  BarsStatus,
} from "@/features/replay/types"

export interface ProjectionClient<TFrame, TAccumulator> {
  kind: string
  schemaVersion: number
  parseFrame: (value: unknown) => TFrame | null
  createAccumulator: (spec: string) => TAccumulator
}

const barsProjectionClient: ProjectionClient<
  BarsProjectionFrame,
  BarsAccumulator
> = {
  kind: "bars",
  schemaVersion: 1,
  parseFrame: parseBarsProjectionFrame,
  createAccumulator: (spec) => new BarsAccumulator(spec),
}

const projectionClients = new Map<string, ProjectionClient<unknown, unknown>>([
  [
    clientKey(barsProjectionClient.kind, barsProjectionClient.schemaVersion),
    barsProjectionClient,
  ],
])

export function getProjectionClient(
  kind: string,
  schemaVersion: number
): ProjectionClient<unknown, unknown> | null {
  return projectionClients.get(clientKey(kind, schemaVersion)) ?? null
}

export function getBarsProjectionClient(): typeof barsProjectionClient {
  return barsProjectionClient
}

export function parseProjectionFrame(
  value: unknown
): BarsProjectionFrame | null {
  if (!isRecord(value)) return null
  const { kind, schemaVersion } = value
  if (typeof kind !== "string" || !isInteger(schemaVersion)) return null
  const client = getProjectionClient(kind, schemaVersion)
  if (!client) return null
  return client.parseFrame(value) as BarsProjectionFrame | null
}

function parseBarsProjectionFrame(value: unknown): BarsProjectionFrame | null {
  if (!isRecord(value)) return null
  const {
    subscriptionId,
    sessionGeneration,
    spec,
    kind,
    schemaVersion,
    frameSequence,
    base,
    head,
    operation,
    reason,
    payload,
  } = value
  if (
    typeof subscriptionId !== "string" ||
    !isInteger(sessionGeneration) ||
    typeof spec !== "string" ||
    kind !== "bars" ||
    schemaVersion !== 1 ||
    !isInteger(frameSequence)
  ) {
    return null
  }
  if (
    operation !== "snapshot" &&
    operation !== "append" &&
    operation !== "replace" &&
    operation !== "patch"
  ) {
    return null
  }
  if (reason !== "initial" && reason !== "cadence" && reason !== "resync") {
    return null
  }
  const parsedBase = base === null ? null : parsePosition(base)
  if (base !== null && parsedBase === null) return null
  const parsedHead = parsePosition(head)
  const parsedPayload = parseBarsPayload(payload)
  if (!parsedHead || !parsedPayload) return null
  return {
    subscriptionId,
    sessionGeneration,
    spec,
    kind,
    schemaVersion,
    frameSequence,
    base: parsedBase,
    head: parsedHead,
    operation,
    reason,
    payload: parsedPayload,
  }
}

function parsePosition(value: unknown): BarsPosition | null {
  if (!isRecord(value)) return null
  const { epoch, projectionRevision, processedBatches, completedBars } = value
  if (
    !isInteger(epoch) ||
    !isInteger(projectionRevision) ||
    !isInteger(processedBatches) ||
    !isInteger(completedBars)
  ) {
    return null
  }
  return { epoch, projectionRevision, processedBatches, completedBars }
}

function parseBarsPayload(
  value: unknown
): BarsProjectionFrame["payload"] | null {
  if (!isRecord(value) || !Array.isArray(value.bars)) return null
  const bars: Bar[] = []
  for (const valueBar of value.bars) {
    const bar = parseBar(valueBar)
    if (!bar) return null
    bars.push(bar)
  }
  const live = value.live === null ? null : parseBar(value.live)
  if (value.live !== null && !live) return null
  const status = parseStatus(value.status)
  if (!status) return null
  return { bars, live, status }
}

function parseStatus(value: unknown): BarsStatus | null {
  if (!isRecord(value)) return null
  const {
    spec,
    epoch,
    processedBatches,
    completedBars,
    revision,
    lastTsEventNs,
  } = value
  if (
    typeof spec !== "string" ||
    !isInteger(epoch) ||
    !isInteger(processedBatches) ||
    !isInteger(completedBars) ||
    !isInteger(revision) ||
    !isNullableString(lastTsEventNs)
  ) {
    return null
  }
  return {
    spec,
    epoch,
    processedBatches,
    completedBars,
    revision,
    lastTsEventNs,
  }
}

function parseBar(value: unknown): Bar | null {
  if (!isRecord(value)) return null
  const { intervalStartNs, firstTsEventNs, lastTsEventNs } = value
  const { open, high, low, close } = value
  const { volume, buyVolume, sellVolume, tradeCount } = value
  if (
    typeof intervalStartNs !== "string" ||
    typeof firstTsEventNs !== "string" ||
    typeof lastTsEventNs !== "string" ||
    !isNumber(open) ||
    !isNumber(high) ||
    !isNumber(low) ||
    !isNumber(close) ||
    !isNumber(volume) ||
    !isNumber(buyVolume) ||
    !isNumber(sellVolume) ||
    !isNumber(tradeCount)
  ) {
    return null
  }
  return {
    intervalStartNs,
    open,
    high,
    low,
    close,
    volume,
    buyVolume,
    sellVolume,
    tradeCount,
    firstTsEventNs,
    lastTsEventNs,
  }
}

function clientKey(kind: string, schemaVersion: number): string {
  return `${kind}:${schemaVersion}`
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value)
}

function isInteger(value: unknown): value is number {
  return Number.isSafeInteger(value) && (value as number) >= 0
}

function isNumber(value: unknown): value is number {
  return typeof value === "number" && Number.isFinite(value)
}

function isNullableString(value: unknown): value is string | null {
  return value === null || typeof value === "string"
}
