import type { Bar, BarsFrame, BarsStatus } from "@/features/replay/types"

// A pull to re-sync a spec's bars after a dropped notification leaves a gap.
// Resolves to the authoritative slice from `from` onward (a session/bars call).
export type BarsBackfill = (spec: string, from: number) => Promise<BarsFrame>

export interface BarsSnapshot {
  spec: string
  epoch: number | null
  bars: Bar[]
  live: Bar | null
  status: BarsStatus | null
}

// One accumulator per projection stream, implementing the barsFrame contract:
// an epoch change resets the bar list; within an epoch, frames append
// contiguously by index. A non-contiguous `from` means a notification was
// dropped — rather than carry corrupt state it pulls a backfill and re-syncs.
// The view layer adapts it to React through subscribe/getSnapshot
// (useSyncExternalStore); getSnapshot returns a cached object that only changes
// identity when the accumulator does.
export class BarsAccumulator {
  readonly spec: string
  // Wired after construction (from an effect, once the session id is known) so
  // no session-scoped closure is created during render.
  private backfill: BarsBackfill | null = null
  private epoch: number | null = null
  private bars: Bar[] = []
  private live: Bar | null = null
  private status: BarsStatus | null = null
  private backfilling = false
  private snapshot: BarsSnapshot
  private readonly listeners = new Set<() => void>()

  constructor(spec: string) {
    this.spec = spec
    this.snapshot = { spec, epoch: null, bars: [], live: null, status: null }
  }

  setBackfill(backfill: BarsBackfill): void {
    this.backfill = backfill
  }

  ingest(frame: BarsFrame): void {
    if (frame.spec !== this.spec) return

    // New epoch (or first frame): reset, then accept from index 0. A non-zero
    // start means the opening frame was dropped — rebuild from the pull.
    if (this.epoch === null || frame.epoch !== this.epoch) {
      this.epoch = frame.epoch
      // A seek advanced the epoch: retire any in-flight backfill guard so the
      // new epoch can pull its own gaps.
      this.backfilling = false
      if (frame.from === 0) {
        this.bars = frame.bars.slice()
      } else {
        this.bars = []
        this.requestBackfill(0)
      }
      this.live = frame.live
      this.status = frame.status
      this.publish()
      return
    }

    // Contiguous append.
    if (frame.from === this.bars.length) {
      if (frame.bars.length > 0) this.bars = this.bars.concat(frame.bars)
      this.live = frame.live
      this.status = frame.status
      this.publish()
      return
    }

    // Overlap (a backfill pull, or a re-sent frame): trust it from `from` on.
    if (frame.from < this.bars.length) {
      this.bars = this.bars.slice(0, frame.from).concat(frame.bars)
      this.live = frame.live
      this.status = frame.status
      this.publish()
      return
    }

    // Gap: a notification was dropped, leaving a hole before this frame.
    // Re-sync by pull instead of appending past it. live/status still reflect
    // the newest frame.
    console.warn(
      `[replay] ${this.spec}: gap at ${this.bars.length}, frame from ${frame.from}; backfilling`
    )
    this.live = frame.live
    this.status = frame.status
    this.requestBackfill(this.bars.length)
    this.publish()
  }

  subscribe = (listener: () => void): (() => void) => {
    this.listeners.add(listener)
    return () => {
      this.listeners.delete(listener)
    }
  }

  getSnapshot = (): BarsSnapshot => this.snapshot

  private requestBackfill(from: number): void {
    if (this.backfilling || !this.backfill) return
    this.backfilling = true
    void this.backfill(this.spec, from)
      .then((frame) => {
        this.backfilling = false
        // A seek may have advanced the epoch while the pull was in flight; drop
        // the stale slice and let the new epoch sync itself.
        if (frame.epoch === this.epoch) this.ingest(frame)
      })
      .catch((error) => {
        this.backfilling = false
        console.warn(`[replay] ${this.spec}: backfill failed`, error)
      })
  }

  private publish(): void {
    this.snapshot = {
      spec: this.spec,
      epoch: this.epoch,
      bars: this.bars,
      live: this.live,
      status: this.status,
    }
    for (const listener of this.listeners) listener()
  }
}
