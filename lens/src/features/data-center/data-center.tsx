import { AlertTriangle, Search, Trash2 } from "lucide-react"
import type { FormEvent } from "react"
import { useCallback, useEffect, useState } from "react"
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
  fetchStoreObjects,
  formatBytes,
} from "@/features/data-center/api"
import { ObjectTable } from "@/features/data-center/object-table"
import type {
  DataCenterLoadState,
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

export function DataCenter() {
  const [objects, setObjects] = useState<StoreObject[]>([])
  const [filters, setFilters] = useState<StoreObjectFilters>(EMPTY_FILTERS)
  const [appliedFilters, setAppliedFilters] =
    useState<StoreObjectFilters>(EMPTY_FILTERS)
  const [deleteTarget, setDeleteTarget] = useState<StoreObject | null>(null)
  const [deleting, setDeleting] = useState(false)
  const [loadState, setLoadState] = useState<DataCenterLoadState>({
    kind: "loading",
    message: "Connecting to Ledger API.",
  })

  const refresh = useCallback(async () => {
    setLoadState({
      kind: "loading",
      message: "Loading store objects from Ledger API.",
    })
    try {
      const nextObjects = await fetchStoreObjects(appliedFilters)
      setObjects(nextObjects)
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
      setLoadState({
        kind: "error",
        message: `Ledger API request failed: ${errorMessage(error)}`,
      })
      toast.error("Ledger API unavailable", {
        description:
          "Start ledger-api on 127.0.0.1:3001 or set VITE_LEDGER_API_URL.",
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

  function applyFilters(event: FormEvent<HTMLFormElement>) {
    event.preventDefault()
    setAppliedFilters(normalizeFilters(filters))
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
    <div className="flex h-svh flex-col bg-background text-foreground">
      <header className="flex min-h-10 shrink-0 items-center justify-between gap-2 border-b-2 border-border px-3 py-1">
        <div className="flex min-w-0 items-center gap-3">
          <div className="text-sm font-semibold">Ledger</div>
          <div className="h-4 w-px bg-border" />
          <div className="text-xs font-medium text-muted-foreground">
            Data Center
          </div>
        </div>
        <div className="text-xs text-muted-foreground">
          {objectSummary(objects, loadState.kind)}
        </div>
      </header>

      <main className="flex min-h-0 flex-1 flex-col gap-2 p-3">
        <form
          className="flex shrink-0 flex-wrap items-center justify-between gap-2"
          onSubmit={applyFilters}
        >
          <div className="relative min-w-[18rem] flex-1 md:max-w-xl">
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
          <div className="flex min-w-0 flex-wrap items-center justify-end gap-2">
            <Input
              className="h-8 w-52 text-xs"
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
              <SelectTrigger
                aria-label="Object role"
                className="h-8 w-32 rounded-none border-border bg-background px-2 text-xs text-foreground hover:bg-muted/30"
              >
                <SelectValue placeholder="Role" />
              </SelectTrigger>
              <SelectContent className="rounded-none border border-border bg-popover text-xs shadow-none ring-0">
                <SelectItem className="rounded-none text-xs" value="all">
                  All roles
                </SelectItem>
                <SelectItem className="rounded-none text-xs" value="raw">
                  Raw
                </SelectItem>
                <SelectItem className="rounded-none text-xs" value="artifact">
                  Artifact
                </SelectItem>
              </SelectContent>
            </Select>
          </div>
        </form>

        {loadState.kind === "error" || loadState.kind === "empty" ? (
          <DataCenterStatePanel
            title={loadState.kind === "error" ? "API Error" : "No Objects"}
            message={loadState.message}
            onRetry={loadState.kind === "error" ? refresh : undefined}
          />
        ) : (
          <ObjectTable objects={objects} onDelete={setDeleteTarget} />
        )}
      </main>

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

function objectSummary(
  objects: StoreObject[],
  state: DataCenterLoadState["kind"]
) {
  if (state === "loading") return "Loading"
  if (state === "error") return "Offline"
  if (objects.length === 0) return "0 objects"
  const totalBytes = objects.reduce((sum, object) => sum + object.sizeBytes, 0)
  return `${objects.length} objects / ${formatBytes(totalBytes)}`
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
            <DetailRow label="ID" value={object.id} wrap />
            <DetailRow label="Remote" value={object.remote?.key ?? "-"} wrap />
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
}: {
  title: string
  message: string
  onRetry?: () => Promise<void>
}) {
  return (
    <div className="flex min-h-[18rem] flex-1 flex-col items-center justify-center border border-border bg-card/40 p-6 text-center">
      <div className="mb-2 flex items-center gap-2 text-sm font-semibold">
        <AlertTriangle className="size-4 text-muted-foreground" />
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

function DetailRow({
  label,
  value,
  wrap = false,
}: {
  label: string
  value: string
  wrap?: boolean
}) {
  return (
    <div className="grid grid-cols-[5rem_minmax(0,1fr)] gap-2 py-0.5">
      <div className="text-muted-foreground">{label}</div>
      <div
        className={
          wrap ? "break-all text-foreground" : "truncate text-foreground"
        }
      >
        {value}
      </div>
    </div>
  )
}
