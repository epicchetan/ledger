import type {
  DeleteStoreObjectReport,
  LocalStoreObject,
  StoreObject,
  StoreObjectFilters,
  StoreRemoteObject,
} from "@/features/data-center/types"

const API_BASE = import.meta.env.VITE_LEDGER_API_URL ?? "http://127.0.0.1:3001"
const REQUEST_TIMEOUT_MS = 20_000

interface ApiStoreObject {
  id: string
  role: string
  kind: string
  file_name: string
  content_sha256: string
  size_bytes: number
  format: string | null
  media_type: string | null
  remote: ApiStoreRemoteObject | null
  local: ApiLocalStoreObject | null
  lineage: string[]
  metadata_json: unknown
  created_at_ns: string
  created_at_iso: string
  updated_at_ns: string
  updated_at_iso: string
  last_accessed_at_ns: string | null
  last_accessed_at_iso: string | null
}

interface ApiStoreRemoteObject {
  bucket: string
  key: string
  size_bytes: number
  sha256: string | null
  etag: string | null
}

interface ApiLocalStoreObject {
  relative_path: string
  size_bytes: number
  last_accessed_at_ns: string
  last_accessed_at_iso: string
}

interface ApiDeleteStoreObjectReport {
  id: string | null
  descriptor_removed: boolean
  remote_object_deleted: boolean
  remote_descriptor_deleted: boolean
  local_deleted: boolean
  remote_key: string | null
  remote_descriptor_key: string | null
  local_path: string | null
  bytes_deleted: number
}

interface RequestOptions extends RequestInit {
  emptyBody?: boolean
}

export async function fetchStoreObjects(filters: StoreObjectFilters): Promise<StoreObject[]> {
  const params = new URLSearchParams()
  if (filters.role) params.set("role", filters.role)
  if (filters.kind) params.set("kind", filters.kind)
  if (filters.idPrefix) params.set("id_prefix", filters.idPrefix)
  const suffix = params.size ? `?${params.toString()}` : ""
  const records = await request<ApiStoreObject[]>(`/store/objects${suffix}`)
  return records.map(mapStoreObject)
}

export async function deleteStoreObject(id: string): Promise<DeleteStoreObjectReport> {
  const report = await request<ApiDeleteStoreObjectReport>(`/store/objects/${encodeURIComponent(id)}`, {
    method: "DELETE",
  })
  return {
    id: report.id,
    descriptorRemoved: report.descriptor_removed,
    remoteObjectDeleted: report.remote_object_deleted,
    remoteDescriptorDeleted: report.remote_descriptor_deleted,
    localDeleted: report.local_deleted,
    remoteKey: report.remote_key,
    remoteDescriptorKey: report.remote_descriptor_key,
    localPath: report.local_path,
    bytesDeleted: report.bytes_deleted,
  }
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

function mapStoreObject(record: ApiStoreObject): StoreObject {
  return {
    id: record.id,
    role: record.role,
    kind: record.kind,
    fileName: record.file_name,
    contentSha256: record.content_sha256,
    sizeBytes: record.size_bytes,
    size: formatBytes(record.size_bytes),
    format: record.format,
    mediaType: record.media_type,
    remote: record.remote ? mapRemote(record.remote) : null,
    local: record.local ? mapLocal(record.local) : null,
    lineage: record.lineage,
    metadataJson: record.metadata_json,
    createdAt: formatIso(record.created_at_iso) ?? "-",
    updatedAt: formatIso(record.updated_at_iso) ?? "-",
    lastAccessedAt: formatIso(record.last_accessed_at_iso),
  }
}

function mapRemote(remote: ApiStoreRemoteObject): StoreRemoteObject {
  return {
    bucket: remote.bucket,
    key: remote.key,
    sizeBytes: remote.size_bytes,
    sha256: remote.sha256,
    etag: remote.etag,
  }
}

function mapLocal(local: ApiLocalStoreObject): LocalStoreObject {
  return {
    relativePath: local.relative_path,
    sizeBytes: local.size_bytes,
    lastAccessedAt: formatIso(local.last_accessed_at_iso) ?? "-",
  }
}

export function formatBytes(bytes: number) {
  const units = ["B", "KB", "MB", "GB", "TB"]
  let value = bytes
  let unit = 0
  while (value >= 1024 && unit < units.length - 1) {
    value /= 1024
    unit += 1
  }
  const maximumFractionDigits = unit === 0 ? 0 : value >= 100 ? 1 : 2
  return `${new Intl.NumberFormat("en-US", { maximumFractionDigits }).format(value)} ${units[unit]}`
}

function formatIso(iso: string | null) {
  if (!iso) return null
  const date = new Date(iso)
  if (Number.isNaN(date.getTime())) return "-"
  return new Intl.DateTimeFormat("en-US", {
    timeZone: "America/New_York",
    month: "short",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hour12: false,
  }).format(date)
}
