import { Cloud, Download, HardDrive, Loader2, Trash2 } from "lucide-react"
import type { MouseEvent } from "react"

import { Button } from "@/components/ui/button"
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from "@/components/ui/dialog"
import type { StoreObject } from "@/features/data-center/types"
import { cn } from "@/lib/utils"

interface ObjectListProps {
  objects: StoreObject[]
  hydratingIds: Set<string>
  onSelect: (object: StoreObject) => void
  onHydrate: (object: StoreObject) => void
}

export function ObjectList({
  objects,
  hydratingIds,
  onSelect,
  onHydrate,
}: ObjectListProps) {
  return (
    <div className="flex flex-col gap-1.5">
      {objects.map((object) => (
        <ObjectRow
          key={object.id}
          object={object}
          hydrating={hydratingIds.has(object.id)}
          onSelect={onSelect}
          onHydrate={onHydrate}
        />
      ))}
    </div>
  )
}

// The row is a tap container, not a <button>: the quick action inside it is
// a real button and interactive elements must not nest.
function ObjectRow({
  object,
  hydrating,
  onSelect,
  onHydrate,
}: {
  object: StoreObject
  hydrating: boolean
  onSelect: (object: StoreObject) => void
  onHydrate: (object: StoreObject) => void
}) {
  return (
    <div
      className="flex min-h-11 cursor-pointer items-center gap-2 rounded-lg border border-border bg-card/40 py-2 pr-1.5 pl-3 select-none active:bg-muted/40"
      onClick={() => onSelect(object)}
    >
      <div className="min-w-0 flex-1">
        <div className="flex items-baseline justify-between gap-2">
          <div className="min-w-0 truncate text-sm text-foreground">
            {object.fileName}
          </div>
          <div className="shrink-0 font-mono text-xs text-muted-foreground tabular-nums">
            {object.size}
          </div>
        </div>
        <div className="mt-0.5 flex items-center gap-2">
          <RoleBadge role={object.role} />
          <div className="min-w-0 flex-1 truncate text-xs text-muted-foreground">
            {object.kind}
          </div>
          <div className="flex shrink-0 items-center gap-1.5">
            <Cloud
              aria-hidden="true"
              className={
                object.remote
                  ? "size-3 text-muted-foreground"
                  : "size-3 text-muted-foreground/40"
              }
            />
            <HardDrive
              aria-hidden="true"
              className={
                object.local
                  ? "size-3 text-success"
                  : "size-3 text-muted-foreground/40"
              }
            />
          </div>
        </div>
      </div>
      <QuickAction
        object={object}
        hydrating={hydrating}
        onHydrate={onHydrate}
      />
    </div>
  )
}

function QuickAction({
  object,
  hydrating,
  onHydrate,
}: {
  object: StoreObject
  hydrating: boolean
  onHydrate: (object: StoreObject) => void
}) {
  if (hydrating) {
    return (
      <div className="flex size-7 shrink-0 items-center justify-center">
        <Loader2 className="size-4 animate-spin text-muted-foreground" />
      </div>
    )
  }
  if (object.local) {
    return (
      <div className="flex size-7 shrink-0 items-center justify-center">
        <HardDrive className="size-4 text-success" />
      </div>
    )
  }
  return (
    <Button
      type="button"
      variant="ghost"
      size="icon-sm"
      className="shrink-0"
      aria-label={`Hydrate ${object.fileName}`}
      onClick={(event: MouseEvent) => {
        // A hydrate tap must never also open the detail dialog.
        event.stopPropagation()
        onHydrate(object)
      }}
    >
      <Download className="size-4" />
    </Button>
  )
}

export function RoleBadge({ role }: { role: string }) {
  const tone =
    role === "raw"
      ? "border-link/25 bg-link/10 text-link"
      : "border-success/25 bg-success/10 text-success"

  return (
    <span
      className={`inline-flex shrink-0 rounded-md border px-1.5 py-0.5 text-[0.68rem] font-medium ${tone}`}
    >
      {role}
    </span>
  )
}

export function ObjectDetailDialog({
  object,
  hydrating,
  onOpenChange,
  onHydrate,
  onDelete,
}: {
  object: StoreObject | null
  hydrating: boolean
  onOpenChange: (open: boolean) => void
  onHydrate: (object: StoreObject) => void
  onDelete: (object: StoreObject) => void
}) {
  return (
    <Dialog open={object !== null} onOpenChange={onOpenChange}>
      <DialogContent className="max-h-[80svh] overflow-y-auto sm:max-w-lg">
        <DialogHeader>
          <DialogTitle className="break-all">
            {object?.fileName ?? ""}
          </DialogTitle>
          <DialogDescription>Store object details.</DialogDescription>
        </DialogHeader>
        {object ? (
          <div className="space-y-1 text-xs">
            <DetailRow label="ID" value={object.id} mono wrap />
            <DetailRow label="SHA-256" value={object.contentSha256} mono wrap />
            <DetailRow label="Role" value={object.role} />
            <DetailRow label="Kind" value={object.kind} wrap />
            <DetailRow label="Format" value={object.format ?? "-"} />
            <DetailRow label="Media" value={object.mediaType ?? "-"} wrap />
            <DetailRow label="Size" value={object.size} mono />
            <DetailRow
              label="Bucket"
              value={object.remote?.bucket ?? "-"}
              mono
            />
            <DetailRow
              label="Key"
              value={object.remote?.key ?? "-"}
              mono
              wrap
            />
            <DetailRow
              label="Local"
              value={object.local?.relativePath ?? "-"}
              mono
              wrap
            />
            <DetailRow label="Used" value={object.lastAccessedAt ?? "-"} mono />
            <DetailRow label="Created" value={object.createdAt} mono />
            <DetailRow label="Updated" value={object.updatedAt} mono />
          </div>
        ) : null}
        <DialogFooter>
          <Button
            type="button"
            variant="destructive"
            onClick={() => {
              if (object) onDelete(object)
            }}
          >
            <Trash2 className="size-3.5" />
            Delete
          </Button>
          <Button
            type="button"
            disabled={hydrating}
            onClick={() => {
              if (object) onHydrate(object)
            }}
          >
            {hydrating ? (
              <Loader2 className="size-3.5 animate-spin" />
            ) : (
              <Download className="size-3.5" />
            )}
            {object?.local ? "Touch local" : "Hydrate"}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  )
}

export function DeleteObjectDialog({
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

export function DetailRow({
  label,
  value,
  mono = false,
  wrap = false,
}: {
  label: string
  value: string
  mono?: boolean
  wrap?: boolean
}) {
  return (
    <div className="grid grid-cols-[5.5rem_minmax(0,1fr)] gap-2 py-0.5">
      <div className="text-muted-foreground">{label}</div>
      <div
        className={cn(
          wrap ? "break-all" : "truncate",
          mono && "font-mono",
          "text-foreground"
        )}
      >
        {value}
      </div>
    </div>
  )
}
