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

type ApiMarketDayStatus = "missing" | "downloading" | "downloaded" | "preprocessing" | "ready" | "error"

interface ApiDataCenterMarketDay {
  id: string
  root: string
  contract: string
  market_date: string
  timezone: string
  data_start_ns: string
  data_start_iso: string
  data_end_ns: string
  data_end_iso: string
  rth_start_ns: string
  rth_start_iso: string
  rth_end_ns: string
  rth_end_iso: string
  market_day_status: ApiMarketDayStatus
  catalog_found: boolean
  raw: ApiRawDataLayer
  replay_dataset: ApiReplayDatasetLayer
}

interface ApiObjectSummary {
  kind: StorageKind
  logical_key: string
  format: string
  schema_version: number
  content_sha256: string
  size_bytes: number
  remote_key: string
}

interface ApiRawDataLayer {
  status: "missing" | "available" | "error"
  provider: string | null
  dataset: string | null
  schema: string | null
  source_symbol: string | null
  object: ApiObjectSummary | null
  updated_at_ns: string | null
  updated_at_iso: string | null
}

interface ApiReplayDatasetLayer {
  status: "missing" | "building" | "available" | "invalid"
  id: string | null
  raw_object_key: string | null
  schema_version: number | null
  producer: string | null
  producer_version: string | null
  artifact_set_hash: string | null
  updated_at_ns: string | null
  updated_at_iso: string | null
  artifacts_available: boolean
  objects_valid: boolean
  artifacts: ApiReplayArtifact[]
  validation: ApiValidationSummary | null
}

interface ApiReplayArtifact {
  kind: StorageKind
  remote_key: string | null
  size_bytes: number | null
  content_sha256: string | null
  object_valid: boolean
}

interface ApiValidationSummary {
  mode: "light" | "full"
  status: "valid" | "warning" | "invalid"
  created_at_ns: string
  created_at_iso: string
  event_count: number | null
  batch_count: number | null
  trade_count: number | null
  warnings: string[]
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
  const records = await request<ApiDataCenterMarketDay[]>("/market-days")
  return records.map(mapMarketDay)
}

export async function fetchMarketDayStatus(day: MarketDay): Promise<MarketDay> {
  const status = await request<ApiDataCenterMarketDay>(marketDayPath(day, ""))
  return mapMarketDay(status)
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
  return request<JobRecord[]>("/jobs?active=true")
}

export async function fetchRecentJobs(limit = 25): Promise<JobRecord[]> {
  return request<JobRecord[]>(`/jobs?active=false&limit=${limit}`)
}

export async function fetchMarketDayJobs(day: MarketDay, limit = 25): Promise<JobRecord[]> {
  return request<JobRecord[]>(`${marketDayPath(day, "jobs")}?active=false&limit=${limit}`)
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

function mapMarketDay(record: ApiDataCenterMarketDay): MarketDay {
  const rawData = rawSummary(record.raw)
  const replayDataset = datasetSummary(record)

  return {
    id: record.id,
    symbol: record.root === "ES" ? "ES" : "ES",
    contract: record.contract,
    marketDate: record.market_date,
    sessionStart: formatIso(record.data_start_iso),
    sessionEnd: formatIso(record.data_end_iso),
    source: "Databento",
    status: statusToViewStatus(record),
    rawData,
    replayDataset,
  }
}

function statusToViewStatus(record: ApiDataCenterMarketDay): MarketDayStatus {
  const validation = record.replay_dataset.validation
  if (record.market_day_status === "downloading" || record.market_day_status === "preprocessing") return "loading"
  if (validation?.status === "invalid" || record.replay_dataset.status === "invalid") return "invalid"
  if (validation?.status === "warning") return "warning"
  if (validation?.status === "valid") return "ready"
  if (record.replay_dataset.objects_valid) return "ready"
  if (record.replay_dataset.status === "available") return "replay"
  if (record.raw.status === "available") return "raw"
  return "missing"
}

function rawSummary(raw: ApiRawDataLayer): RawDataSummary {
  return {
    status: raw.status,
    provider: raw.provider,
    dataset: raw.dataset,
    schema: raw.schema,
    sourceSymbol: raw.source_symbol,
    remoteKey: raw.object?.remote_key ?? null,
    size: formatBytes(raw.object?.size_bytes ?? null),
    sha256: raw.object?.content_sha256 ?? null,
    updatedAt: formatIso(raw.updated_at_iso),
  }
}

function datasetSummary(record: ApiDataCenterMarketDay): ReplayDatasetSummary {
  const replayDataset = record.replay_dataset
  const validation = replayDataset.validation

  return {
    status: replayDataset.status,
    id: replayDataset.id,
    rawObjectKey: replayDataset.raw_object_key,
    eventCount: validation?.event_count ?? null,
    batchCount: validation?.batch_count ?? null,
    tradeCount: validation?.trade_count ?? null,
    firstEventTime: null,
    lastEventTime: null,
    lastValidatedAt: formatIso(validation?.created_at_iso ?? null),
    trustSummary: trustSummary(record),
    warnings: validation?.warnings ?? [],
    artifacts: artifactMap(replayDataset.artifacts),
  }
}

function trustSummary(record: ApiDataCenterMarketDay) {
  const validation = record.replay_dataset.validation
  if (validation?.status === "valid") return "ReplayDataset has a persisted validation report."
  if (validation?.status === "warning") return "ReplayDataset is usable but has validation warnings."
  if (record.replay_dataset.status === "available") {
    return record.replay_dataset.objects_valid
      ? "ReplayDataset artifacts are durable in R2 and available for validation or replay-session staging."
      : "ReplayDataset artifact metadata exists, but one or more R2 objects failed verification."
  }
  if (record.raw.status === "available") return "Raw market data is durable in R2. ReplayDataset can be built without redownloading."
  return "No raw market data or replay dataset is available for this MarketDay."
}

function artifactMap(artifacts: ApiReplayArtifact[]): Record<ArtifactKey, ReplayArtifact> {
  return {
    events: artifactFromStatus(artifacts, "event_store"),
    batches: artifactFromStatus(artifacts, "batch_index"),
    trades: artifactFromStatus(artifacts, "trade_index"),
    bookCheck: artifactFromStatus(artifacts, "book_check"),
  }
}

function artifactFromStatus(artifacts: ApiReplayArtifact[], kind: StorageKind): ReplayArtifact {
  const object = artifacts.find((item) => item.kind === kind)
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

function formatIso(iso: string | null) {
  if (!iso) return "-"
  const date = new Date(iso)
  if (Number.isNaN(date.getTime())) return "-"
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
