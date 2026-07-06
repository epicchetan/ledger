import type { StoreObject } from "@/features/data-center/types"
import type {
  EsDayCatalog,
  EsRawStatus,
  JobRecord,
  StoreDescriptor,
} from "@/features/days/types"

// Where an object currently lives. Replay reads the local artifact, so
// "ready-local" is the only state that can hit play immediately; "ready-remote"
// needs a hydrate first.
export type Locality = "local" | "remote" | "absent" | "unknown"

// The single most useful next step for a raw, derived by folding prep state
// (from the day catalog) together with locality (from the store object list)
// and any in-flight job or last error.
export type ReadinessState =
  | "preparing"
  | "failed"
  | "ready-local"
  | "ready-remote"
  | "unprepared"

export interface RawReadiness {
  raw: EsRawStatus
  state: ReadinessState
  symbol: string
  rawLocality: Locality
  artifactLocality: Locality
  eventCount: number | null
  batchCount: number | null
  job?: JobRecord
  error?: string
  // Resolved store objects (null when the catalog descriptor has no matching
  // store entry). artifactObject is the hydrate target for a ready-remote day.
  rawObject: StoreObject | null
  artifactObject: StoreObject | null
  // Store objects backing this raw (the raw itself, plus its artifact when
  // prepared). Feeds the per-day storage drawer without a second fetch.
  storageObjects: StoreObject[]
}

export interface DayReadiness {
  marketDay: string
  raws: RawReadiness[]
  // The raw the day card's headline reflects and whose primary action runs.
  primary: RawReadiness
  state: ReadinessState
  extraRawCount: number
  storageObjects: StoreObject[]
}

export type ObjectIndex = Map<string, StoreObject>

export function indexObjects(objects: StoreObject[]): ObjectIndex {
  const map: ObjectIndex = new Map()
  for (const object of objects) map.set(object.id, object)
  return map
}

function locality(object: StoreObject | undefined): Locality {
  if (!object) return "unknown"
  if (object.local) return "local"
  if (object.remote) return "remote"
  return "absent"
}

function metadataString(metadata: unknown, key: string): string | null {
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

function collectStorage(
  descriptors: (StoreDescriptor | null | undefined)[],
  objects: ObjectIndex
): StoreObject[] {
  const out: StoreObject[] = []
  for (const descriptor of descriptors) {
    if (!descriptor) continue
    const object = objects.get(descriptor.id)
    if (object) out.push(object)
  }
  return out
}

export function deriveRawReadiness(
  raw: EsRawStatus,
  objects: ObjectIndex,
  job: JobRecord | undefined,
  error: string | undefined
): RawReadiness {
  const rawObject = objects.get(raw.raw.id)
  const artifactObject = raw.artifact
    ? objects.get(raw.artifact.id)
    : undefined
  const artifactLocality = raw.artifact ? locality(artifactObject) : "absent"

  let state: ReadinessState
  if (job?.state.status === "running") {
    state = "preparing"
  } else if (error) {
    state = "failed"
  } else if (raw.artifact) {
    state = artifactLocality === "local" ? "ready-local" : "ready-remote"
  } else {
    state = "unprepared"
  }

  return {
    raw,
    state,
    symbol: metadataString(raw.raw.metadataJson, "source_symbol") ?? "-",
    rawLocality: locality(rawObject),
    artifactLocality,
    eventCount: metadataNumber(raw.artifact?.metadataJson, "event_count"),
    batchCount: metadataNumber(raw.artifact?.metadataJson, "batch_count"),
    job,
    error,
    rawObject: rawObject ?? null,
    artifactObject: artifactObject ?? null,
    storageObjects: collectStorage([raw.raw, raw.artifact], objects),
  }
}

// Higher rank = further along the pipeline, so the day card headlines the most
// advanced raw. Failures outrank plain unprepared raws so a broken prepare is
// never hidden behind an untouched sibling.
const STATE_RANK: Record<ReadinessState, number> = {
  "ready-local": 5,
  "ready-remote": 4,
  preparing: 3,
  failed: 2,
  unprepared: 1,
}

function pickPrimary(raws: RawReadiness[]): RawReadiness {
  return raws.reduce((best, candidate) =>
    STATE_RANK[candidate.state] > STATE_RANK[best.state] ? candidate : best
  )
}

function deriveDay(
  marketDay: string,
  rawStatuses: EsRawStatus[],
  objects: ObjectIndex,
  jobsBySubject: Map<string, JobRecord>,
  rowErrors: Map<string, string>
): DayReadiness {
  const raws = rawStatuses.map((raw) =>
    deriveRawReadiness(
      raw,
      objects,
      jobsBySubject.get(raw.raw.id),
      rowErrors.get(raw.raw.id)
    )
  )
  const primary = pickPrimary(raws)
  return {
    marketDay,
    raws,
    primary,
    state: primary.state,
    extraRawCount: raws.length - 1,
    storageObjects: raws.flatMap((entry) => entry.storageObjects),
  }
}

export interface ReadyCatalog {
  days: DayReadiness[]
  unassigned: DayReadiness | null
}

// The whole catalog resolved into day cards. `unassigned` collapses raws with
// no market day into a single pseudo-day so the home stays a flat list.
export function deriveCatalog(
  catalog: EsDayCatalog,
  objects: ObjectIndex,
  jobsBySubject: Map<string, JobRecord>,
  rowErrors: Map<string, string>
): ReadyCatalog {
  const days = catalog.days.map((day) =>
    deriveDay(day.marketDay, day.raws, objects, jobsBySubject, rowErrors)
  )
  const unassigned =
    catalog.unassigned.length > 0
      ? deriveDay(
          "Unassigned",
          catalog.unassigned,
          objects,
          jobsBySubject,
          rowErrors
        )
      : null
  return { days, unassigned }
}
