import { requestIpc } from "@remux/viewer-kit/ipc"

import type {
  DeleteStoreObjectReport,
  LocalStoreObject,
  StoreObject,
  StoreRemoteObject,
} from "@/features/data-center/types"

// The ES day catalog embeds these wire descriptors; the days feature shapes
// them through formatStoreObject instead of fetching a separate store list.
export interface RemuxStoreObject {
  id: string
  role: string
  kind: string
  fileName: string
  contentSha256: string
  sizeBytes: number
  format: string | null
  mediaType: string | null
  remote: RemuxStoreRemoteObject | null
  local: RemuxLocalStoreObject | null
  lineage: string[]
  metadataJson: unknown
  createdAtNs: string
  createdAtIso: string
  updatedAtNs: string
  updatedAtIso: string
  lastAccessedAtNs: string | null
  lastAccessedAtIso: string | null
}

interface RemuxStoreRemoteObject {
  bucket: string
  key: string
  sizeBytes: number
  sha256: string | null
  etag: string | null
}

interface RemuxLocalStoreObject {
  relativePath: string
  sizeBytes: number
  lastAccessedAtNs: string
  lastAccessedAtIso: string
}

export async function deleteStoreObject(
  id: string
): Promise<DeleteStoreObjectReport> {
  return requestIpc<DeleteStoreObjectReport>("remux/ledger/store/delete", {
    id,
  })
}

export function formatStoreObject(record: RemuxStoreObject): StoreObject {
  return {
    id: record.id,
    role: record.role,
    kind: record.kind,
    fileName: record.fileName,
    contentSha256: record.contentSha256,
    sizeBytes: record.sizeBytes,
    size: formatBytes(record.sizeBytes),
    format: record.format,
    mediaType: record.mediaType,
    remote: record.remote ? formatRemote(record.remote) : null,
    local: record.local ? formatLocal(record.local) : null,
    lineage: record.lineage,
    metadataJson: record.metadataJson,
    createdAt: formatIso(record.createdAtIso) ?? "-",
    updatedAt: formatIso(record.updatedAtIso) ?? "-",
    updatedAtMs: parseIsoMs(record.updatedAtIso),
    lastAccessedAt: formatIso(record.lastAccessedAtIso),
  }
}

function formatRemote(remote: RemuxStoreRemoteObject): StoreRemoteObject {
  return {
    bucket: remote.bucket,
    key: remote.key,
    sizeBytes: remote.sizeBytes,
    sha256: remote.sha256,
    etag: remote.etag,
  }
}

function formatLocal(local: RemuxLocalStoreObject): LocalStoreObject {
  return {
    relativePath: local.relativePath,
    sizeBytes: local.sizeBytes,
    lastAccessedAt: formatIso(local.lastAccessedAtIso) ?? "-",
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

function parseIsoMs(iso: string | null) {
  if (!iso) return 0
  const ms = new Date(iso).getTime()
  return Number.isNaN(ms) ? 0 : ms
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
