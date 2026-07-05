export interface StoreDescriptor {
  id: string
  role: string
  kind: string
  fileName: string
  contentSha256: string
  sizeBytes: number
  format: string | null
  mediaType: string | null
  lineage: string[]
  metadataJson: unknown
}

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
  stage: string
  records?: number | null
}

export interface JobFinishedEvent {
  jobId: string
  ok: boolean
  summary?: unknown
  error?: string | null
}

export interface DaysLoadState {
  kind: "loading" | "ready" | "empty" | "error"
  message: string
}
