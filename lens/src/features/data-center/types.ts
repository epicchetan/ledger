export type MarketDayStatus =
  | "missing"
  | "raw"
  | "replay"
  | "ready"
  | "warning"
  | "invalid"
  | "loading"
  | "deleting"

export type ArtifactStatus = "missing" | "remote" | "valid" | "invalid"

export type ArtifactKey = "events" | "batches" | "trades" | "bookCheck"

export type DatasetAction = "prepare" | "rebuild" | "validate" | "deleteReplay" | "deleteRaw"

export type JobStatus = "queued" | "running" | "succeeded" | "failed"

export interface JobRecord {
  id: string
  kind: string
  status: JobStatus
  market_day_id: string | null
  target: JobTarget | null
  created_at: string
  started_at: string | null
  finished_at: string | null
  progress: string[]
  result: unknown
  error: string | null
}

export interface JobTarget {
  market_day_id: string
  symbol: string
  market_date: string
}

export interface ReplayArtifact {
  status: ArtifactStatus
  remoteKey: string | null
  path: string | null
  size: string | null
  sha256: string | null
  updatedAt: string | null
}

export interface RawDataSummary {
  status: "missing" | "available" | "error"
  provider: string | null
  dataset: string | null
  schema: string | null
  sourceSymbol: string | null
  remoteKey: string | null
  size: string | null
  sha256: string | null
  updatedAt: string | null
}

export interface ReplayDatasetSummary {
  status: "missing" | "building" | "available" | "invalid"
  id: string | null
  rawObjectKey: string | null
  eventCount: number | null
  batchCount: number | null
  tradeCount: number | null
  firstEventTime: string | null
  lastEventTime: string | null
  lastValidatedAt: string | null
  trustSummary: string
  warnings: string[]
  artifacts: Record<ArtifactKey, ReplayArtifact>
}

export interface MarketDay {
  id: string
  symbol: "ES"
  contract: string
  marketDate: string
  sessionStart: string
  sessionEnd: string
  source: "Databento"
  status: MarketDayStatus
  rawData: RawDataSummary
  replayDataset: ReplayDatasetSummary
}

export interface DataCenterLoadState {
  kind: "loading" | "ready" | "empty" | "error"
  message: string
}
