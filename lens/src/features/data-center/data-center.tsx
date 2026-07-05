import {
  closeHostTab,
  openHostOverview,
  updateHostTab,
} from "@remux/viewer-kit/host"
import { ActionBar, ActionButton } from "@remux/viewer-kit/ui"
import {
  AlertTriangle,
  Loader2,
  PanelRightOpen,
  RefreshCw,
  Search,
  Trash2,
  X,
} from "lucide-react"
import type { FormEvent, ReactNode } from "react"
import { useCallback, useEffect, useMemo, useState } from "react"
import { toast } from "sonner"

import { Button } from "@/components/ui/button"
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog"
import { Input } from "@/components/ui/input"
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select"
import {
  deleteStoreObject,
  fetchLocalStatus,
  fetchStoreObjects,
  formatBytes,
  hydrateStoreObject,
  subscribeHydratedStoreObjects,
} from "@/features/data-center/api"
import {
  DetailRow,
  ObjectDetailDialog,
  ObjectList,
} from "@/features/data-center/object-list"
import type {
  DataCenterLoadState,
  HydratedStoreObjectEvent,
  LocalStoreStatus,
  ObjectSortKey,
  StoreObject,
  StoreObjectFilters,
} from "@/features/data-center/types"

const EMPTY_FILTERS: StoreObjectFilters = {
  role: "",
  kind: "",
  idPrefix: "",
}

function errorMessage(error: unknown) {
  return error instanceof Error ? error.message : "Unknown error"
}

function normalizeFilters(filters: StoreObjectFilters): StoreObjectFilters {
  return {
    role: filters.role,
    kind: filters.kind.trim(),
    idPrefix: filters.idPrefix.trim(),
  }
}

function sameFilters(left: StoreObjectFilters, right: StoreObjectFilters) {
  return (
    left.role === right.role &&
    left.kind === right.kind &&
    left.idPrefix === right.idPrefix
  )
}

function sortObjects(objects: StoreObject[], sort: ObjectSortKey) {
  const sorted = [...objects]
  switch (sort) {
    case "size":
      sorted.sort((a, b) => b.sizeBytes - a.sizeBytes)
      break
    case "name":
      sorted.sort((a, b) => a.fileName.localeCompare(b.fileName))
      break
    case "kind":
      sorted.sort(
        (a, b) =>
          a.kind.localeCompare(b.kind) || a.fileName.localeCompare(b.fileName)
      )
      break
    default:
      sorted.sort((a, b) => b.updatedAtMs - a.updatedAtMs)
  }
  return sorted
}

export function DataCenter({ viewerMenu }: { viewerMenu?: ReactNode }) {
  const [objects, setObjects] = useState<StoreObject[]>([])
  const [filters, setFilters] = useState<StoreObjectFilters>(EMPTY_FILTERS)
  const [appliedFilters, setAppliedFilters] =
    useState<StoreObjectFilters>(EMPTY_FILTERS)
  const [sort, setSort] = useState<ObjectSortKey>("updated")
  const [detailTarget, setDetailTarget] = useState<StoreObject | null>(null)
  const [deleteTarget, setDeleteTarget] = useState<StoreObject | null>(null)
  const [deleting, setDeleting] = useState(false)
  const [hydratingIds, setHydratingIds] = useState<Set<string>>(new Set())
  const [localStatus, setLocalStatus] = useState<LocalStoreStatus | null>(null)
  const [loadState, setLoadState] = useState<DataCenterLoadState>({
    kind: "loading",
    message: "Connecting to Ledger extension.",
  })

  const refresh = useCallback(async () => {
    setLoadState({
      kind: "loading",
      message: "Loading store objects from Ledger.",
    })
    try {
      const [nextObjects, nextLocalStatus] = await Promise.all([
        fetchStoreObjects(appliedFilters),
        fetchLocalStatus(),
      ])
      setObjects(nextObjects)
      setLocalStatus(nextLocalStatus)
      setLoadState(
        nextObjects.length === 0
          ? { kind: "empty", message: "No store objects are registered yet." }
          : {
              kind: "ready",
              message: `Loaded ${nextObjects.length} store object${nextObjects.length === 1 ? "" : "s"}.`,
            }
      )
    } catch (error) {
      setObjects([])
      setLocalStatus(null)
      setLoadState({
        kind: "error",
        message: `Ledger remux request failed: ${errorMessage(error)}`,
      })
      toast.error("Ledger extension unavailable", {
        description: "Check the remux runtime and ledger-remux server logs.",
      })
    }
  }, [appliedFilters])

  useEffect(() => {
    void refresh()
  }, [refresh])

  useEffect(() => {
    const timeout = window.setTimeout(() => {
      const nextFilters = normalizeFilters(filters)
      setAppliedFilters((current) =>
        sameFilters(current, nextFilters) ? current : nextFilters
      )
    }, 180)

    return () => window.clearTimeout(timeout)
  }, [filters])

  const handleHydratedEvent = useCallback(
    (event: HydratedStoreObjectEvent) => {
      setHydratingIds((current) => {
        const next = new Set(current)
        next.delete(event.id)
        return next
      })
      if (event.ok) {
        toast.success("Hydrate complete", {
          description: shortId(event.id),
        })
      } else {
        toast.error("Hydrate failed", {
          description: event.error ?? shortId(event.id),
        })
      }
      void refresh()
    },
    [refresh]
  )

  useEffect(() => {
    return subscribeHydratedStoreObjects((event) => {
      handleHydratedEvent(event)
    })
  }, [handleHydratedEvent])

  const totalBytes = useMemo(
    () => objects.reduce((sum, object) => sum + object.sizeBytes, 0),
    [objects]
  )

  const tabStatus =
    loadState.kind === "loading"
      ? "Loading"
      : loadState.kind === "error"
        ? "Offline"
        : objects.length === 0
          ? "0 objects"
          : `${objects.length} objects / ${formatBytes(totalBytes)}`

  useEffect(() => {
    // The catch is load-bearing: tab chrome must never affect the data flow,
    // and a bare void still leaves an unhandled rejection.
    void updateHostTab({ title: "Ledger", status: tabStatus }).catch(
      () => undefined
    )
  }, [tabStatus])

  const barStatus =
    loadState.kind === "loading" && objects.length === 0
      ? "Loading"
      : loadState.kind === "error"
        ? "Offline"
        : `${objects.length} objects · ${formatBytes(totalBytes)} · ${
            localStatus
              ? `local ${localStatus.size}/${localStatus.max}`
              : "local -"
          }`

  const sortedObjects = useMemo(
    () => sortObjects(objects, sort),
    [objects, sort]
  )

  // Re-derive from the latest refresh so the open dialog tracks hydrate and
  // touch updates instead of showing the row as it was when tapped.
  const detailObject = detailTarget
    ? (objects.find((object) => object.id === detailTarget.id) ?? detailTarget)
    : null

  function applyFilters(event: FormEvent<HTMLFormElement>) {
    event.preventDefault()
    setAppliedFilters(normalizeFilters(filters))
  }

  async function hydrateObject(object: StoreObject) {
    setHydratingIds((current) => new Set(current).add(object.id))
    try {
      const result = await hydrateStoreObject(object.id)
      if (result.alreadyLocal) {
        setHydratingIds((current) => {
          const next = new Set(current)
          next.delete(object.id)
          return next
        })
        toast.info("Object already local", {
          description: object.fileName,
        })
        await refresh()
        return
      }
      toast.success("Hydrate started", {
        description: object.fileName,
      })
    } catch (error) {
      setHydratingIds((current) => {
        const next = new Set(current)
        next.delete(object.id)
        return next
      })
      toast.error("Hydrate failed", {
        description: errorMessage(error),
      })
    }
  }

  async function confirmDelete() {
    if (!deleteTarget) return
    setDeleting(true)
    try {
      const report = await deleteStoreObject(deleteTarget.id)
      toast.success("Object deleted", {
        description: `${deleteTarget.fileName} removed ${formatBytes(report.bytesDeleted)} across store locations.`,
      })
      setDeleteTarget(null)
      await refresh()
    } catch (error) {
      toast.error("Delete failed", {
        description: errorMessage(error),
      })
    } finally {
      setDeleting(false)
    }
  }

  return (
    <div className="flex h-full flex-col bg-background text-foreground">
      <form
        className="lens-safe-top flex shrink-0 flex-col gap-2 border-b border-border px-3 pb-2"
        onSubmit={applyFilters}
      >
        <div className="relative">
          <Search className="pointer-events-none absolute top-1/2 left-2 size-3.5 -translate-y-1/2 text-muted-foreground" />
          <Input
            className="h-8 w-full pl-7 text-xs"
            value={filters.idPrefix}
            onChange={(event) =>
              setFilters((current) => ({
                ...current,
                idPrefix: event.target.value,
              }))
            }
            placeholder="Search object id"
          />
        </div>
        <div className="flex items-center gap-2">
          <Input
            className="h-8 min-w-0 flex-1 text-xs"
            value={filters.kind}
            onChange={(event) =>
              setFilters((current) => ({
                ...current,
                kind: event.target.value,
              }))
            }
            placeholder="Kind"
          />
          <Select
            value={filters.role || "all"}
            onValueChange={(value) =>
              setFilters((current) => ({
                ...current,
                role: value === "all" ? "" : value,
              }))
            }
          >
            <SelectTrigger aria-label="Object role" className="text-xs">
              <SelectValue placeholder="Role" />
            </SelectTrigger>
            <SelectContent className="text-xs">
              <SelectItem className="text-xs" value="all">
                All roles
              </SelectItem>
              <SelectItem className="text-xs" value="raw">
                Raw
              </SelectItem>
              <SelectItem className="text-xs" value="artifact">
                Artifact
              </SelectItem>
            </SelectContent>
          </Select>
          <Select
            value={sort}
            onValueChange={(value) => setSort(value as ObjectSortKey)}
          >
            <SelectTrigger aria-label="Sort objects" className="text-xs">
              <SelectValue placeholder="Sort" />
            </SelectTrigger>
            <SelectContent className="text-xs">
              <SelectItem className="text-xs" value="updated">
                Updated
              </SelectItem>
              <SelectItem className="text-xs" value="size">
                Size
              </SelectItem>
              <SelectItem className="text-xs" value="name">
                Name
              </SelectItem>
              <SelectItem className="text-xs" value="kind">
                Kind
              </SelectItem>
            </SelectContent>
          </Select>
        </div>
      </form>

      <main className="lens-scroll min-h-0 flex-1 overflow-y-auto px-3 py-2">
        {loadState.kind === "error" || loadState.kind === "empty" ? (
          <DataCenterStatePanel
            title={loadState.kind === "error" ? "Remux Error" : "No Objects"}
            message={loadState.message}
            onRetry={loadState.kind === "error" ? refresh : undefined}
          />
        ) : loadState.kind === "loading" && objects.length === 0 ? (
          <DataCenterStatePanel
            title="Loading"
            message={loadState.message}
            loading
          />
        ) : (
          <ObjectList
            objects={sortedObjects}
            hydratingIds={hydratingIds}
            onSelect={setDetailTarget}
            onHydrate={(object) => void hydrateObject(object)}
          />
        )}
      </main>

      <ActionBar
        left={
          <>
            {viewerMenu}
            <ActionButton
              icon={<RefreshCw aria-hidden="true" />}
              label="Refresh data"
              busy={loadState.kind === "loading"}
              disabled={loadState.kind === "loading"}
              onClick={() => {
                void refresh()
              }}
            />
          </>
        }
        right={
          <>
            <ActionButton
              icon={<PanelRightOpen aria-hidden="true" />}
              label="Open tabs"
              onClick={() => {
                void openHostOverview({ section: "tabs" })
              }}
            />
            <ActionButton
              icon={<X aria-hidden="true" />}
              label="Close tab"
              onClick={() => {
                void closeHostTab()
              }}
            />
          </>
        }
        status={barStatus}
      />

      <ObjectDetailDialog
        object={detailObject}
        hydrating={detailObject ? hydratingIds.has(detailObject.id) : false}
        onOpenChange={(open) => {
          if (!open) setDetailTarget(null)
        }}
        onHydrate={(object) => void hydrateObject(object)}
        onDelete={(object) => {
          // One dialog at a time: close the detail dialog before the confirm.
          setDetailTarget(null)
          setDeleteTarget(object)
        }}
      />

      <DeleteObjectDialog
        object={deleteTarget}
        deleting={deleting}
        onOpenChange={(open) => {
          if (!open && !deleting) setDeleteTarget(null)
        }}
        onConfirm={() => void confirmDelete()}
      />
    </div>
  )
}

function shortId(id: string) {
  return id.length > 19 ? `${id.slice(0, 16)}...` : id
}

function DeleteObjectDialog({
  object,
  deleting,
  onOpenChange,
  onConfirm,
}: {
  object: StoreObject | null
  deleting: boolean
  onOpenChange: (open: boolean) => void
  onConfirm: () => void
}) {
  return (
    <Dialog open={object !== null} onOpenChange={onOpenChange}>
      <DialogContent className="sm:max-w-lg">
        <DialogHeader>
          <DialogTitle>Delete store object</DialogTitle>
          <DialogDescription>
            This removes the exact object descriptor, local object, R2 object,
            and R2 descriptor mirror when present.
          </DialogDescription>
        </DialogHeader>
        {object ? (
          <div className="space-y-1 text-xs">
            <DetailRow label="File" value={object.fileName} />
            <DetailRow label="Role" value={object.role} />
            <DetailRow label="Kind" value={object.kind} wrap />
            <DetailRow label="ID" value={object.id} mono wrap />
            <DetailRow
              label="Remote"
              value={object.remote?.key ?? "-"}
              mono
              wrap
            />
          </div>
        ) : null}
        <DialogFooter>
          <Button
            type="button"
            variant="outline"
            disabled={deleting}
            onClick={() => onOpenChange(false)}
          >
            Cancel
          </Button>
          <Button
            type="button"
            variant="destructive"
            disabled={deleting}
            onClick={onConfirm}
          >
            <Trash2
              className={deleting ? "size-3.5 animate-pulse" : "size-3.5"}
            />
            Delete
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  )
}

function DataCenterStatePanel({
  title,
  message,
  onRetry,
  loading = false,
}: {
  title: string
  message: string
  onRetry?: () => Promise<void>
  loading?: boolean
}) {
  return (
    <div className="flex min-h-[18rem] flex-col items-center justify-center rounded-lg border border-border bg-card/40 p-6 text-center">
      <div className="mb-2 flex items-center gap-2 text-sm font-semibold">
        {loading ? (
          <Loader2 className="size-4 animate-spin text-muted-foreground" />
        ) : (
          <AlertTriangle className="size-4 text-muted-foreground" />
        )}
        {title}
      </div>
      <p className="max-w-xl text-xs leading-5 text-muted-foreground">
        {message}
      </p>
      {onRetry ? (
        <Button
          type="button"
          variant="outline"
          size="sm"
          className="mt-4"
          onClick={() => void onRetry()}
        >
          Retry
        </Button>
      ) : null}
    </div>
  )
}
