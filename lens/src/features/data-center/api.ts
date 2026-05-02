import type {
  ArtifactKey,
  ArtifactStatus,
  JobRecord,
  MarketDay,
  MarketDayStatus,
  RawDataSummary,
  ReplayArtifact,
  ReplayDatasetSummary,
} from "@/features/data-center/types"

const API_BASE = import.meta.env.VITE_LEDGER_API_URL ?? "http://127.0.0.1:3001"
const REQUEST_TIMEOUT_MS = 20_000

type StorageKind = "raw_dbn" | "event_store" | "batch_index" | "trade_index" | "book_check" | string

interface ApiMarketDay {
  id: string
  root: string
  contract_symbol: string
  market_date: string
  data_start_ns: number
  data_end_ns: number
  status: string
}

interface ApiStoredObject {
  kind: StorageKind
  logical_key: string
  format: string
  schema_version: number
  content_sha256: string
  size_bytes: number
  remote_bucket: string
  remote_key: string
  producer: string | null
  producer_version: string | null
  source_provider: string | null
  source_dataset: string | null
  source_schema: string | null
  source_symbol: string | null
  metadata_json: Record<string, unknown>
}

interface ApiRawMarketDataRecord {
  object: ApiStoredObject
  provider: string
  dataset: string
  schema: string
  source_symbol: string
  status: "missing" | "available" | "error"
  updated_at_ns: number
}

interface ApiReplayDatasetRecord {
  id: string
  raw_object_remote_key: string
  status: "missing" | "building" | "available" | "invalid"
  schema_version: number
  producer: string | null
  producer_version: string | null
  artifact_set_hash: string
  updated_at_ns: number
}

interface ApiValidationReportRecord {
  mode: "light" | "full"
  status: "valid" | "warning" | "invalid"
  created_at_ns: number
  report_json: {
    counts?: {
      event_count?: number
      batch_count?: number
      trade_count?: number
    }
    warnings?: string[]
  }
}

interface ApiMarketDayRecord {
  market_day: ApiMarketDay
  raw: ApiRawMarketDataRecord | null
  replay_dataset: ApiReplayDatasetRecord | null
  last_validation: ApiValidationReportRecord | null
}

interface ApiReplayDatasetObjectStatus {
  kind: StorageKind
  remote_key: string | null
  size_bytes: number | null
  content_sha256: string | null
  object_valid: boolean
}

interface ApiReplayDatasetStatus extends ApiMarketDayRecord {
  catalog_found: boolean
  replay_artifacts_available: boolean
  replay_objects_valid: boolean
  objects: ApiReplayDatasetObjectStatus[]
}

interface ApiCreateJobResponse {
  job: JobRecord
}

interface ApiJobResponse {
  job: JobRecord
}

interface RequestOptions extends RequestInit {
  emptyBody?: boolean
}

export async function fetchMarketDays(): Promise<MarketDay[]> {
  const records = await request<ApiMarketDayRecord[]>("/market-days")
  return records.map((record) => mapMarketDay(record, null))
}

export async function fetchMarketDayStatus(day: MarketDay): Promise<MarketDay> {
  const status = await request<ApiReplayDatasetStatus>(marketDayPath(day, ""))
  return mapMarketDay(status, status)
}

export async function prepareReplayDataset(contract: string, marketDate: string): Promise<JobRecord> {
  const response = await request<ApiCreateJobResponse>(marketDayActionPath(contract, marketDate, "prepare"), {
    method: "POST",
  })
  return response.job
}

export async function rebuildReplayDataset(day: MarketDay): Promise<JobRecord> {
  const response = await request<ApiCreateJobResponse>(marketDayPath(day, "replay/build"), {
    method: "POST",
  })
  return response.job
}

export async function validateReplayDataset(day: MarketDay): Promise<JobRecord> {
  const response = await request<ApiCreateJobResponse>(marketDayPath(day, "replay/validate"), {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ skip_book_check: false, replay_batches: 1, replay_all: false }),
  })
  return response.job
}

export async function deleteReplayDataset(day: MarketDay): Promise<JobRecord> {
  const response = await request<ApiCreateJobResponse>(marketDayPath(day, "replay"), {
    method: "DELETE",
  })
  return response.job
}

export async function deleteRawMarketData(day: MarketDay): Promise<JobRecord> {
  const response = await request<ApiCreateJobResponse>(marketDayPath(day, "raw"), {
    method: "DELETE",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ cascade: false }),
  })
  return response.job
}

export async function fetchJob(jobId: string): Promise<JobRecord> {
  const response = await request<ApiJobResponse>(`/jobs/${encodeURIComponent(jobId)}`)
  return response.job
}

export async function fetchActiveJobs(): Promise<JobRecord[]> {
  return request<JobRecord[]>("/jobs")
}

async function request<T>(path: string, options: RequestOptions = {}): Promise<T> {
  const controller = new AbortController()
  const timeout = window.setTimeout(() => controller.abort(), REQUEST_TIMEOUT_MS)
  let response: Response

  try {
    response = await fetch(`${API_BASE}${path}`, {
      ...options,
      signal: options.signal ?? controller.signal,
    })
  } catch (error) {
    if (error instanceof DOMException && error.name === "AbortError") {
      throw new Error(`Request timed out after ${REQUEST_TIMEOUT_MS / 1000}s: ${path}`)
    }
    throw error
  } finally {
    window.clearTimeout(timeout)
  }

  if (!response.ok) {
    const message = await errorMessage(response)
    throw new Error(message)
  }

  if (options.emptyBody) {
    return undefined as T
  }

  return (await response.json()) as T
}

async function errorMessage(response: Response) {
  try {
    const body = (await response.json()) as { error?: string }
    return body.error ?? `${response.status} ${response.statusText}`
  } catch {
    return `${response.status} ${response.statusText}`
  }
}

function mapMarketDay(record: ApiMarketDayRecord, status: ApiReplayDatasetStatus | null): MarketDay {
  const marketDay = record.market_day
  const rawData = rawSummary(record.raw)
  const replayDataset = datasetSummary(record, status)

  return {
    id: marketDay.id,
    symbol: marketDay.root === "ES" ? "ES" : "ES",
    contract: marketDay.contract_symbol,
    marketDate: marketDay.market_date,
    sessionStart: formatNs(marketDay.data_start_ns),
    sessionEnd: formatNs(marketDay.data_end_ns),
    source: "Databento",
    status: statusToViewStatus(record, status),
    rawData,
    replayDataset,
  }
}

function statusToViewStatus(record: ApiMarketDayRecord, status: ApiReplayDatasetStatus | null): MarketDayStatus {
  if (record.market_day.status === "downloading" || record.market_day.status === "preprocessing") return "loading"
  if (record.last_validation?.status === "invalid" || record.replay_dataset?.status === "invalid") return "invalid"
  if (record.last_validation?.status === "warning") return "warning"
  if (record.last_validation?.status === "valid") return "ready"
  if (status?.replay_objects_valid) return "ready"
  if (record.replay_dataset?.status === "available") return "replay"
  if (record.raw?.status === "available") return "raw"
  return "missing"
}

function rawSummary(raw: ApiRawMarketDataRecord | null): RawDataSummary {
  if (!raw) {
    return {
      status: "missing",
      provider: null,
      dataset: null,
      schema: null,
      sourceSymbol: null,
      remoteKey: null,
      size: null,
      sha256: null,
      updatedAt: null,
    }
  }

  return {
    status: raw.status,
    provider: raw.provider,
    dataset: raw.dataset,
    schema: raw.schema,
    sourceSymbol: raw.source_symbol,
    remoteKey: raw.object.remote_key,
    size: formatBytes(raw.object.size_bytes),
    sha256: raw.object.content_sha256,
    updatedAt: formatNs(raw.updated_at_ns),
  }
}

function datasetSummary(record: ApiMarketDayRecord, status: ApiReplayDatasetStatus | null): ReplayDatasetSummary {
  const artifacts = artifactMap(status)
  const counts = record.last_validation?.report_json.counts
  const warnings = record.last_validation?.report_json.warnings ?? []
  const replayDataset = record.replay_dataset

  return {
    status: replayDataset?.status ?? "missing",
    id: replayDataset?.id ?? null,
    rawObjectKey: replayDataset?.raw_object_remote_key ?? null,
    eventCount: counts?.event_count ?? null,
    batchCount: counts?.batch_count ?? null,
    tradeCount: counts?.trade_count ?? null,
    firstEventTime: null,
    lastEventTime: null,
    lastValidatedAt: record.last_validation ? formatNs(record.last_validation.created_at_ns) : null,
    trustSummary: trustSummary(record, status),
    warnings,
    artifacts,
  }
}

function trustSummary(record: ApiMarketDayRecord, status: ApiReplayDatasetStatus | null) {
  if (record.last_validation?.status === "valid") return "ReplayDataset has a persisted validation report."
  if (record.last_validation?.status === "warning") return "ReplayDataset is usable but has validation warnings."
  if (record.replay_dataset?.status === "available") {
    return status?.replay_objects_valid
      ? "ReplayDataset artifacts are durable in R2 and available for validation or replay-session staging."
      : "ReplayDataset artifact metadata exists, but one or more R2 objects failed verification."
  }
  if (record.raw?.status === "available") return "Raw market data is durable in R2. ReplayDataset can be built without redownloading."
  return "No raw market data or replay dataset is available for this MarketDay."
}

function artifactMap(status: ApiReplayDatasetStatus | null): Record<ArtifactKey, ReplayArtifact> {
  return {
    events: artifactFromStatus(status, "event_store"),
    batches: artifactFromStatus(status, "batch_index"),
    trades: artifactFromStatus(status, "trade_index"),
    bookCheck: artifactFromStatus(status, "book_check"),
  }
}

function artifactFromStatus(status: ApiReplayDatasetStatus | null, kind: StorageKind): ReplayArtifact {
  const object = status?.objects.find((item) => item.kind === kind)
  if (!object) return artifact("missing", null, null, null, null)
  if (object.object_valid) {
    return artifact("valid", object.remote_key, null, object.size_bytes, object.content_sha256)
  }
  if (object.remote_key) {
    return artifact("remote", object.remote_key, null, object.size_bytes, object.content_sha256)
  }
  return artifact("missing", object.remote_key, null, object.size_bytes, object.content_sha256)
}

function artifact(
  status: ArtifactStatus,
  remoteKey: string | null,
  path: string | null,
  sizeBytes: number | null,
  sha256: string | null,
): ReplayArtifact {
  return {
    status,
    remoteKey,
    path,
    size: sizeBytes == null ? null : formatBytes(sizeBytes),
    sha256,
    updatedAt: null,
  }
}

function marketDayPath(day: MarketDay, action: string) {
  return marketDayActionPath(day.contract, day.marketDate, action)
}

function marketDayActionPath(contract: string, marketDate: string, action: string) {
  const suffix = action ? `/${action}` : ""
  return `/market-days/${encodeURIComponent(contract)}/${marketDate}${suffix}`
}

function formatBytes(bytes: number | null) {
  if (bytes == null || !Number.isFinite(bytes)) return null
  return new Intl.NumberFormat("en-US", {
    maximumFractionDigits: bytes >= 1_000_000_000 ? 2 : 1,
    notation: "compact",
  }).format(bytes)
}

function formatNs(ns: number) {
  if (!Number.isFinite(ns) || ns <= 0) return "-"
  const date = new Date(Math.floor(ns / 1_000_000))
  return new Intl.DateTimeFormat("en-US", {
    timeZone: "America/New_York",
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hour12: false,
    timeZoneName: "short",
  }).format(date)
}
