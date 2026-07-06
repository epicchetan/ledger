import type { RemuxStoreObject } from "@/features/data-center/api"

// The catalog embeds full store descriptors — including remote/local
// locality — so the days feature needs no second store fetch.
export type StoreDescriptor = RemuxStoreObject

export type EsRawState = "unprepared" | "prepared"

export interface EsRawStatus {
  raw: StoreDescriptor
  artifact: StoreDescriptor | null
  state: EsRawState
}

export interface EsDayEntry {
  marketDay: string
  raws: EsRawStatus[]
}

export interface EsDayCatalog {
  days: EsDayEntry[]
  unassigned: EsRawStatus[]
}

export interface JobRecord {
  id: string
  kind: string
  subject: string
  // The day a job settles. Subject is the dedup key (raw id for installs,
  // "day symbol" for fetches); this is the display key the UI files jobs by.
  // Absent (not null) on the wire when the raw has no market day yet.
  marketDay?: string | null
  state: JobState
  startedAtNs: string | number
  finishedAtNs: string | number | null
}

export type JobState =
  | { status: "running"; stage: string; records?: number | null }
  | { status: "completed"; summary: unknown }
  | { status: "failed"; error: string }

export interface JobsListResult {
  jobs: JobRecord[]
}

export interface JobStartResult {
  jobId: string
  alreadyRunning?: boolean
}

export interface JobProgressEvent {
  jobId: string
  kind: string
  subject: string
  marketDay: string | null
  stage: string
  records?: number | null
}

export interface JobFinishedEvent {
  jobId: string
  kind: string
  subject: string
  marketDay: string | null
  ok: boolean
  summary?: unknown
  error?: string | null
}

export interface EsOffloadReport {
  marketDay: string
  offloaded: { id: string; bytesRemoved: number }[]
  bytesRemoved: number
}

export interface DaysLoadState {
  kind: "loading" | "ready" | "empty" | "error"
  message: string
}
