export interface StoreRemoteObject {
  bucket: string
  key: string
  sizeBytes: number
  sha256: string | null
  etag: string | null
}

export interface LocalStoreObject {
  relativePath: string
  sizeBytes: number
  lastAccessedAt: string
}

export interface StoreObject {
  id: string
  role: string
  kind: string
  fileName: string
  contentSha256: string
  sizeBytes: number
  size: string
  format: string | null
  mediaType: string | null
  remote: StoreRemoteObject | null
  local: LocalStoreObject | null
  lineage: string[]
  metadataJson: unknown
  createdAt: string
  updatedAt: string
  lastAccessedAt: string | null
}

export interface StoreObjectFilters {
  role: string
  kind: string
  idPrefix: string
}

export interface DeleteStoreObjectReport {
  id: string | null
  descriptorRemoved: boolean
  remoteObjectDeleted: boolean
  remoteDescriptorDeleted: boolean
  localDeleted: boolean
  remoteKey: string | null
  remoteDescriptorKey: string | null
  localPath: string | null
  bytesDeleted: number
}

export interface DataCenterLoadState {
  kind: "loading" | "ready" | "empty" | "error"
  message: string
}
