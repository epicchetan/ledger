import { useCallback, useEffect, useState } from "react"
import { toast } from "sonner"

import {
  deleteStoreObject,
  formatBytes,
  hydrateStoreObject,
  subscribeHydratedStoreObjects,
} from "@/features/data-center/api"
import type { StoreObject } from "@/features/data-center/types"

function shortId(id: string) {
  return id.length > 19 ? `${id.slice(0, 16)}...` : id
}

function errorMessage(error: unknown) {
  return error instanceof Error ? error.message : "Unknown error"
}

// Shared store-object operations (hydrate, delete, detail selection). Both the
// All storage screen and the per-day storage drawer drive the same object
// lifecycle, so the pending state, hydrated-event subscription, and refresh
// trigger live here rather than being duplicated per surface.
//
// `onChanged` runs after any mutation settles (delete, hydrate started/already
// local) and on each hydrated event so the caller can re-fetch and let the
// derived view (day readiness or the object list) pick up the new locality.
export function useStoreObjectOps(onChanged: () => void | Promise<void>) {
  const [hydratingIds, setHydratingIds] = useState<Set<string>>(new Set())
  const [detailTarget, setDetailTarget] = useState<StoreObject | null>(null)
  const [deleteTarget, setDeleteTarget] = useState<StoreObject | null>(null)
  const [deleting, setDeleting] = useState(false)

  const clearHydrating = useCallback((id: string) => {
    setHydratingIds((current) => {
      const next = new Set(current)
      next.delete(id)
      return next
    })
  }, [])

  useEffect(() => {
    return subscribeHydratedStoreObjects((event) => {
      clearHydrating(event.id)
      if (event.ok) {
        toast.success("Hydrate complete", { description: shortId(event.id) })
      } else {
        toast.error("Hydrate failed", {
          description: event.error ?? shortId(event.id),
        })
      }
      void onChanged()
    })
  }, [clearHydrating, onChanged])

  const hydrateObject = useCallback(
    async (object: StoreObject) => {
      setHydratingIds((current) => new Set(current).add(object.id))
      try {
        const result = await hydrateStoreObject(object.id)
        if (result.alreadyLocal) {
          clearHydrating(object.id)
          toast.info("Object already local", { description: object.fileName })
          void onChanged()
          return
        }
        toast.success("Hydrate started", { description: object.fileName })
      } catch (error) {
        clearHydrating(object.id)
        toast.error("Hydrate failed", { description: errorMessage(error) })
      }
    },
    [clearHydrating, onChanged]
  )

  const confirmDelete = useCallback(async () => {
    if (!deleteTarget) return
    setDeleting(true)
    try {
      const report = await deleteStoreObject(deleteTarget.id)
      toast.success("Object deleted", {
        description: `${deleteTarget.fileName} removed ${formatBytes(report.bytesDeleted)} across store locations.`,
      })
      setDeleteTarget(null)
      void onChanged()
    } catch (error) {
      toast.error("Delete failed", { description: errorMessage(error) })
    } finally {
      setDeleting(false)
    }
  }, [deleteTarget, onChanged])

  return {
    hydratingIds,
    hydrateObject,
    detailTarget,
    setDetailTarget,
    deleteTarget,
    setDeleteTarget,
    deleting,
    confirmDelete,
  }
}
