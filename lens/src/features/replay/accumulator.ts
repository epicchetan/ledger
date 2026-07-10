import type {
  Bar,
  BarsPosition,
  BarsProjectionFrame,
  BarsStatus,
} from "@/features/replay/types"

export interface BarsSnapshot {
  spec: string
  epoch: number | null
  bars: Bar[]
  live: Bar | null
  status: BarsStatus | null
  position: BarsPosition | null
}

export type ProjectionApplyResult =
  | { kind: "applied"; head: BarsPosition; immediateAck: boolean }
  | { kind: "duplicate"; head: BarsPosition; immediateAck: boolean }
  | { kind: "base_mismatch" }
  | { kind: "rejected" }

// The bars projection accumulator is the client-side half of the server's
// base/head contract. Validation and next-state construction finish before any
// field mutates; a malformed delta or wrong base therefore leaves the chart's
// last known-good state intact and requests an authoritative resync upstream.
export class BarsAccumulator {
  readonly spec: string
  private subscriptionId: string | null = null
  private sessionGeneration: number | null = null
  private lastFrameSequence = 0
  private position: BarsPosition | null = null
  private bars: Bar[] = []
  private live: Bar | null = null
  private status: BarsStatus | null = null
  private snapshot: BarsSnapshot
  private readonly listeners = new Set<() => void>()

  constructor(spec: string) {
    this.spec = spec
    this.snapshot = {
      spec,
      epoch: null,
      bars: [],
      live: null,
      status: null,
      position: null,
    }
  }

  getAppliedPosition(): BarsPosition | null {
    return this.position ? { ...this.position } : null
  }

  beginSubscription(
    subscriptionId: string,
    sessionGeneration: number,
    retainAppliedState: boolean
  ): void {
    const sameGeneration = this.sessionGeneration === sessionGeneration
    this.subscriptionId = subscriptionId
    this.sessionGeneration = sessionGeneration
    this.lastFrameSequence = 0
    if (!retainAppliedState || !sameGeneration) this.reset()
  }

  apply(frame: BarsProjectionFrame): ProjectionApplyResult {
    if (
      frame.spec !== this.spec ||
      frame.subscriptionId !== this.subscriptionId ||
      frame.sessionGeneration !== this.sessionGeneration ||
      frame.kind !== "bars" ||
      frame.schemaVersion !== 1
    ) {
      return { kind: "rejected" }
    }
    if (frame.frameSequence <= this.lastFrameSequence) {
      return positionsEqual(frame.head, this.position)
        ? {
            kind: "duplicate",
            head: frame.head,
            immediateAck: frame.reason === "seekFinal",
          }
        : { kind: "rejected" }
    }
    if (!validEnvelope(frame, this.spec)) return { kind: "rejected" }

    const snapshot = frame.base === null
    if (snapshot !== (frame.operation === "snapshot")) {
      return { kind: "rejected" }
    }
    if (!snapshot && !positionsEqual(frame.base, this.position)) {
      return { kind: "base_mismatch" }
    }

    let nextBars: Bar[]
    if (snapshot) {
      if (frame.payload.bars.length !== frame.head.completedBars) {
        return { kind: "rejected" }
      }
      nextBars = frame.payload.bars.slice()
    } else {
      const base = frame.base
      if (!base || frame.operation !== "append") {
        return { kind: "rejected" }
      }
      if (
        frame.head.completedBars < base.completedBars ||
        frame.payload.bars.length !==
          frame.head.completedBars - base.completedBars
      ) {
        return { kind: "rejected" }
      }
      nextBars = this.bars.concat(frame.payload.bars)
    }

    const duplicate = positionsEqual(frame.head, this.position)
    this.bars = nextBars
    this.live = frame.payload.live
    this.status = frame.payload.status
    this.position = { ...frame.head }
    this.lastFrameSequence = frame.frameSequence
    this.publish()
    return {
      kind: duplicate ? "duplicate" : "applied",
      head: frame.head,
      immediateAck:
        snapshot || frame.reason === "seekFinal" || frame.reason === "resync",
    }
  }

  subscribe = (listener: () => void): (() => void) => {
    this.listeners.add(listener)
    return () => {
      this.listeners.delete(listener)
    }
  }

  getSnapshot = (): BarsSnapshot => this.snapshot

  private reset(): void {
    this.position = null
    this.bars = []
    this.live = null
    this.status = null
    this.publish()
  }

  private publish(): void {
    this.snapshot = {
      spec: this.spec,
      epoch: this.position?.epoch ?? null,
      bars: this.bars,
      live: this.live,
      status: this.status,
      position: this.position,
    }
    for (const listener of this.listeners) listener()
  }
}

function validEnvelope(frame: BarsProjectionFrame, spec: string): boolean {
  const { status } = frame.payload
  return (
    status.spec === spec &&
    status.epoch === frame.head.epoch &&
    status.revision === frame.head.projectionRevision &&
    status.processedBatches === frame.head.processedBatches &&
    status.completedBars === frame.head.completedBars
  )
}

export function positionsEqual(
  left: BarsPosition | null,
  right: BarsPosition | null
): boolean {
  if (left === null || right === null) return left === right
  return (
    left.epoch === right.epoch &&
    left.projectionRevision === right.projectionRevision &&
    left.processedBatches === right.processedBatches &&
    left.completedBars === right.completedBars
  )
}
