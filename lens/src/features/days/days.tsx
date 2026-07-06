import {
  subscribeHostResume,
  updateHostTab,
} from "@remux/viewer-kit/host"
import { Loader2 } from "lucide-react"
import { useCallback, useEffect, useMemo, useState } from "react"
import { toast } from "sonner"

import { fetchStoreObjects } from "@/features/data-center/api"
import { DeleteObjectDialog } from "@/features/data-center/object-list"
import type {
  StoreObject,
  StoreObjectFilters,
} from "@/features/data-center/types"
import { useStoreObjectOps } from "@/features/data-center/use-store-object-ops"
import {
  fetchEsDays,
  fetchJobs,
  startPrepareJob,
  subscribeLedgerJobs,
} from "@/features/days/api"
import { DayActionBar } from "@/features/days/day-action-bar"
import { DayList } from "@/features/days/day-list"
import { deriveCatalog, indexObjects } from "@/features/days/readiness"
import type {
  DaysLoadState,
  EsDayCatalog,
  EsRawStatus,
  JobFinishedEvent,
  JobProgressEvent,
  JobRecord,
} from "@/features/days/types"

const EMPTY_CATALOG: EsDayCatalog = {
  days: [],
  unassigned: [],
}

// The readiness rollup needs every object's locality, so we pull the full
// store list unfiltered and join it onto the catalog by id on the client.
const ALL_OBJECTS: StoreObjectFilters = {
  role: "",
  kind: "",
  idPrefix: "",
}

function errorMessage(error: unknown) {
  return error instanceof Error ? error.message : "Unknown error"
}

export function Days() {
  const [catalog, setCatalog] = useState<EsDayCatalog>(EMPTY_CATALOG)
  const [objects, setObjects] = useState<StoreObject[]>([])
  const [jobs, setJobs] = useState<JobRecord[]>([])
  const [rowErrors, setRowErrors] = useState<Map<string, string>>(new Map())
  const [loadState, setLoadState] = useState<DaysLoadState>({
    kind: "loading",
    message: "Loading ES days.",
  })
  const [selectedKey, setSelectedKey] = useState<string | null>(null)

  const load = useCallback(async () => {
    setLoadState({ kind: "loading", message: "Loading ES days." })
    try {
      const [nextCatalog, nextJobs, nextObjects] = await Promise.all([
        fetchEsDays(),
        fetchJobs(),
        fetchStoreObjects(ALL_OBJECTS),
      ])
      setCatalog(nextCatalog)
      setJobs(nextJobs.jobs)
      setObjects(nextObjects)
      const totalRows =
        nextCatalog.days.reduce((sum, day) => sum + day.raws.length, 0) +
        nextCatalog.unassigned.length
      setLoadState(
        totalRows === 0
          ? { kind: "empty", message: "No ES days are registered yet." }
          : { kind: "ready", message: `Loaded ${nextCatalog.days.length} days.` }
      )
    } catch (error) {
      setCatalog(EMPTY_CATALOG)
      setObjects([])
      setJobs([])
      setLoadState({
        kind: "error",
        message: `Ledger remux request failed: ${errorMessage(error)}`,
      })
      toast.error("Ledger extension unavailable", {
        description: "Check the remux runtime and ledger-remux server logs.",
      })
    }
  }, [])

  useEffect(() => {
    void load()
  }, [load])

  useEffect(() => {
    return subscribeHostResume(() => {
      void load()
    })
  }, [load])

  const ops = useStoreObjectOps(load)

  const handleProgress = useCallback((event: JobProgressEvent) => {
    setJobs((current) => upsertRunningJob(current, event))
  }, [])

  const handleFinished = useCallback(
    (event: JobFinishedEvent) => {
      let failedSubject: string | null = null
      setJobs((current) =>
        current.map((job) => {
          if (job.id !== event.jobId) return job
          if (!event.ok) failedSubject = job.subject
          return {
            ...job,
            finishedAtNs: Date.now().toString(),
            state: event.ok
              ? ({ status: "completed", summary: event.summary } as const)
              : ({
                  status: "failed",
                  error: event.error ?? "Job failed",
                } as const),
          }
        })
      )
      if (event.ok) {
        toast.success("Ledger job complete")
      } else {
        toast.error("Ledger job failed", {
          description: event.error ?? "Job failed",
        })
        const subject = failedSubject
        if (subject) {
          setRowErrors((current) => {
            const next = new Map(current)
            next.set(subject, event.error ?? "Job failed")
            return next
          })
        }
      }
      void load()
    },
    [load]
  )

  useEffect(() => {
    return subscribeLedgerJobs({
      progress: handleProgress,
      finished: handleFinished,
    })
  }, [handleFinished, handleProgress])

  const jobsBySubject = useMemo(() => {
    const map = new Map<string, JobRecord>()
    for (const job of jobs) {
      if (job.state.status === "running") map.set(job.subject, job)
    }
    return map
  }, [jobs])

  const objectIndex = useMemo(() => indexObjects(objects), [objects])
  const derived = useMemo(
    () => deriveCatalog(catalog, objectIndex, jobsBySubject, rowErrors),
    [catalog, objectIndex, jobsBySubject, rowErrors]
  )

  const findDay = useCallback(
    (key: string | null) => {
      if (!key) return null
      const match = derived.days.find((day) => day.marketDay === key)
      if (match) return match
      return derived.unassigned?.marketDay === key ? derived.unassigned : null
    },
    [derived]
  )

  const selectedDay = useMemo(
    () => findDay(selectedKey),
    [findDay, selectedKey]
  )

  const runningCount = jobs.filter((job) => job.state.status === "running").length
  const dayCount = catalog.days.length
  const barStatus =
    loadState.kind === "loading" && dayCount === 0
      ? "Loading"
      : loadState.kind === "error"
        ? "Offline"
        : `${dayCount} days - ${runningCount} job${runningCount === 1 ? "" : "s"} running`

  useEffect(() => {
    void updateHostTab({ title: "Ledger", status: barStatus }).catch(
      () => undefined
    )
  }, [barStatus])

  async function prepare(raw: EsRawStatus, force: boolean) {
    setRowErrors((current) => {
      const next = new Map(current)
      next.delete(raw.raw.id)
      return next
    })
    try {
      const result = await startPrepareJob(raw.raw.id, force)
      toast.success(result.alreadyRunning ? "Prepare already running" : "Prepare started", {
        description: shortId(raw.raw.id),
      })
      await load()
    } catch (error) {
      toast.error("Prepare failed", { description: errorMessage(error) })
      setRowErrors((current) => {
        const next = new Map(current)
        next.set(raw.raw.id, errorMessage(error))
        return next
      })
    }
  }

  return (
    <div className="flex h-full flex-col bg-background text-foreground">
      <main className="lens-safe-top lens-scroll min-h-0 flex-1 overflow-y-auto px-3 py-2">
        {loadState.kind === "error" || loadState.kind === "empty" ? (
          <DaysStatePanel
            loading={false}
            message={loadState.message}
            title={loadState.kind === "error" ? "Remux Error" : "No Days"}
          />
        ) : loadState.kind === "loading" && dayCount === 0 ? (
          <DaysStatePanel loading message={loadState.message} title="Loading" />
        ) : (
          <DayList
            days={derived.days}
            unassigned={derived.unassigned}
            selectedKey={selectedKey}
            onSelect={(day) => setSelectedKey(day.marketDay)}
          />
        )}
      </main>

      <DayActionBar
        day={selectedDay}
        status={barStatus}
        hydratingIds={ops.hydratingIds}
        onClose={() => setSelectedKey(null)}
        onPrepare={(raw, force) => void prepare(raw, force)}
        onHydrate={(object) => void ops.hydrateObject(object)}
        onDeleteObject={ops.setDeleteTarget}
      />

      <DeleteObjectDialog
        object={ops.deleteTarget}
        deleting={ops.deleting}
        onOpenChange={(open) => {
          if (!open && !ops.deleting) ops.setDeleteTarget(null)
        }}
        onConfirm={() => void ops.confirmDelete()}
      />
    </div>
  )
}

function DaysStatePanel({
  loading,
  message,
  title,
}: {
  loading: boolean
  message: string
  title: string
}) {
  return (
    <div className="flex min-h-[18rem] flex-col items-center justify-center rounded-lg border border-border bg-card/40 p-6 text-center">
      <div className="mb-2 flex items-center gap-2 text-sm font-semibold">
        {loading ? <Loader2 className="size-4 animate-spin" /> : null}
        {title}
      </div>
      <p className="max-w-xl text-xs leading-5 text-muted-foreground">
        {message}
      </p>
    </div>
  )
}

function upsertRunningJob(jobs: JobRecord[], event: JobProgressEvent) {
  const nextState = {
    status: "running" as const,
    stage: event.stage,
    records: event.records,
  }
  if (jobs.some((job) => job.id === event.jobId)) {
    return jobs.map((job) =>
      job.id === event.jobId
        ? { ...job, state: nextState, subject: event.subject, kind: event.kind }
        : job
    )
  }
  return [
    ...jobs,
    {
      id: event.jobId,
      kind: event.kind,
      subject: event.subject,
      state: nextState,
      startedAtNs: Date.now().toString(),
      finishedAtNs: null,
    },
  ]
}

function shortId(id: string) {
  return id.length > 19 ? `${id.slice(0, 16)}...` : id
}
