import { formatStoreObject } from "@/features/data-center/api"
import type { StoreObject } from "@/features/data-center/types"
import type {
  EsDayCatalog,
  EsRawStatus,
  JobRecord,
} from "@/features/days/types"

// The day lifecycle, folded from the catalog (which carries full descriptors,
// locality included) plus the day-keyed job stream. Everything is always in
// R2; "ready" means the artifact is also local — the only state replay can
// hit play from. "offloaded" covers both artifact-remote and not-built-yet:
// either way it isn't here, and Install fixes it.
export type ReadinessState = "installing" | "ready" | "offloaded" | "failed"

export interface RawReadiness {
  raw: EsRawStatus
  state: ReadinessState
  symbol: string
  // Live job stage ("downloading", "decoding", ...) while installing.
  stage: string | null
  eventCount: number | null
  batchCount: number | null
  job?: JobRecord
  error?: string
  // Shaped store objects backing this raw (the raw itself, plus its artifact
  // when built). Feeds the per-day storage drawer without a second fetch.
  storageObjects: StoreObject[]
}

export interface DayReadiness {
  marketDay: string
  raws: RawReadiness[]
  // The raw the day card's headline reflects and whose primary action runs.
  primary: RawReadiness
  state: ReadinessState
  stage: string | null
  error?: string
  extraRawCount: number
  storageObjects: StoreObject[]
}

// Raws with no market day yet file under this pseudo-day key everywhere:
// job tagging, error tagging, and the home-list card.
export const UNASSIGNED_KEY = "Unassigned"

export function metadataString(metadata: unknown, key: string): string | null {
  if (!metadata || typeof metadata !== "object" || Array.isArray(metadata)) {
    return null
  }
  const value = (metadata as Record<string, unknown>)[key]
  return typeof value === "string" ? value : null
}

function metadataNumber(metadata: unknown, key: string): number | null {
  if (!metadata || typeof metadata !== "object" || Array.isArray(metadata)) {
    return null
  }
  const value = (metadata as Record<string, unknown>)[key]
  return typeof value === "number" ? value : null
}

export function deriveRawReadiness(
  raw: EsRawStatus,
  dayJobs: JobRecord[],
  error: string | undefined
): RawReadiness {
  // Jobs are keyed to the day; an install's subject names its raw directly.
  const job = dayJobs.find((entry) => entry.subject === raw.raw.id)

  let state: ReadinessState
  let stage: string | null = null
  if (job?.state.status === "running") {
    state = "installing"
    stage = job.state.stage
  } else if (raw.artifact?.local) {
    state = "ready"
  } else if (error) {
    state = "failed"
  } else {
    state = "offloaded"
  }

  const storageObjects = [raw.raw, raw.artifact]
    .filter((descriptor) => descriptor !== null)
    .map(formatStoreObject)

  return {
    raw,
    state,
    symbol: metadataString(raw.raw.metadataJson, "source_symbol") ?? "-",
    stage,
    eventCount: metadataNumber(raw.artifact?.metadataJson, "event_count"),
    batchCount: metadataNumber(raw.artifact?.metadataJson, "batch_count"),
    job,
    error,
    storageObjects,
  }
}

// Higher rank = closer to replayable, so the day card headlines the most
// advanced raw. Failures outrank plain offloaded raws so a broken install is
// never hidden behind an untouched sibling.
const STATE_RANK: Record<ReadinessState, number> = {
  ready: 4,
  installing: 3,
  failed: 2,
  offloaded: 1,
}

function pickPrimary(raws: RawReadiness[]): RawReadiness {
  return raws.reduce((best, candidate) =>
    STATE_RANK[candidate.state] > STATE_RANK[best.state] ? candidate : best
  )
}

function deriveDay(
  marketDay: string,
  rawStatuses: EsRawStatus[],
  dayJobs: JobRecord[],
  error: string | undefined
): DayReadiness {
  const raws = rawStatuses.map((raw) =>
    deriveRawReadiness(raw, dayJobs, error)
  )
  const primary = pickPrimary(raws)
  return {
    marketDay,
    raws,
    primary,
    state: primary.state,
    stage: primary.stage,
    error,
    extraRawCount: raws.length - 1,
    storageObjects: raws.flatMap((entry) => entry.storageObjects),
  }
}

export interface ReadyCatalog {
  days: DayReadiness[]
  unassigned: DayReadiness | null
}

// The whole catalog resolved into day cards. Inputs are exactly the two
// feed-level sources: the catalog and the day-keyed job/error maps — no
// store-list join. `unassigned` collapses raws with no market day into a
// single pseudo-day so the home stays a flat list.
export function deriveCatalog(
  catalog: EsDayCatalog,
  jobsByDay: Map<string, JobRecord[]>,
  dayErrors: Map<string, string>
): ReadyCatalog {
  const days = catalog.days.map((day) =>
    deriveDay(
      day.marketDay,
      day.raws,
      jobsByDay.get(day.marketDay) ?? [],
      dayErrors.get(day.marketDay)
    )
  )
  const unassigned =
    catalog.unassigned.length > 0
      ? deriveDay(
          UNASSIGNED_KEY,
          catalog.unassigned,
          jobsByDay.get(UNASSIGNED_KEY) ?? [],
          dayErrors.get(UNASSIGNED_KEY)
        )
      : null
  return { days, unassigned }
}
