import {
  subscribeHostResume,
  updateHostTab,
} from "@remux/viewer-kit/host"
import { Loader2 } from "lucide-react"
import { useCallback, useEffect, useMemo, useState } from "react"
import { toast } from "sonner"

import { deleteStoreObject, formatBytes } from "@/features/data-center/api"
import type { StoreObject } from "@/features/data-center/types"
import {
  fetchEsDays,
  fetchJobs,
  offloadDay,
  startInstallJob,
  subscribeLedgerJobs,
} from "@/features/days/api"
import { DayActionBar } from "@/features/days/day-action-bar"
import { DayList } from "@/features/days/day-list"
import {
  deriveCatalog,
  metadataString,
  UNASSIGNED_KEY,
  type DayReadiness,
} from "@/features/days/readiness"
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

function errorMessage(error: unknown) {
  return error instanceof Error ? error.message : "Unknown error"
}

export function Days({ onReplay }: { onReplay: (day: DayReadiness) => void }) {
  const [catalog, setCatalog] = useState<EsDayCatalog>(EMPTY_CATALOG)
  const [jobs, setJobs] = useState<JobRecord[]>([])
  // Last failure per day (keyed by market day, or the Unassigned pseudo-day).
  // Cleared the moment a new job makes progress for that day.
  const [dayErrors, setDayErrors] = useState<Map<string, string>>(new Map())
  const [loadState, setLoadState] = useState<DaysLoadState>({
    kind: "loading",
    message: "Loading ES days.",
  })
  const [selectedKey, setSelectedKey] = useState<string | null>(null)
  const [offloadingDay, setOffloadingDay] = useState<string | null>(null)

  // The catalog carries full descriptors (locality included), so days needs
  // exactly two feed-level sources: the catalog and the job stream.
  const load = useCallback(async () => {
    setLoadState({ kind: "loading", message: "Loading ES days." })
    try {
      const [nextCatalog, nextJobs] = await Promise.all([
        fetchEsDays(),
        fetchJobs(),
      ])
      setCatalog(nextCatalog)
      setJobs(nextJobs.jobs)
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

  const clearDayError = useCallback((dayKey: string) => {
    setDayErrors((current) => {
      if (!current.has(dayKey)) return current
      const next = new Map(current)
      next.delete(dayKey)
      return next
    })
  }, [])

  const setDayError = useCallback((dayKey: string, error: string) => {
    setDayErrors((current) => {
      const next = new Map(current)
      next.set(dayKey, error)
      return next
    })
  }, [])

  const handleProgress = useCallback(
    (event: JobProgressEvent) => {
      setJobs((current) => upsertRunningJob(current, event))
      // A job making progress is a fresh attempt: retire the day's old error.
      clearDayError(event.marketDay ?? UNASSIGNED_KEY)
    },
    [clearDayError]
  )

  const handleFinished = useCallback(
    (event: JobFinishedEvent) => {
      setJobs((current) =>
        current.map((job) => {
          if (job.id !== event.jobId) return job
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
        setDayError(
          event.marketDay ?? UNASSIGNED_KEY,
          event.error ?? "Job failed"
        )
      }
      void load()
    },
    [load, setDayError]
  )

  useEffect(() => {
    return subscribeLedgerJobs({
      progress: handleProgress,
      finished: handleFinished,
    })
  }, [handleFinished, handleProgress])

  const jobsByDay = useMemo(() => {
    const map = new Map<string, JobRecord[]>()
    for (const job of jobs) {
      if (job.state.status !== "running") continue
      const dayKey = job.marketDay ?? UNASSIGNED_KEY
      const entries = map.get(dayKey)
      if (entries) entries.push(job)
      else map.set(dayKey, [job])
    }
    return map
  }, [jobs])

  const derived = useMemo(
    () => deriveCatalog(catalog, jobsByDay, dayErrors),
    [catalog, jobsByDay, dayErrors]
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
  const dayCount = derived.days.length
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

  async function install(raw: EsRawStatus) {
    const dayKey =
      metadataString(raw.raw.metadataJson, "market_day") ?? UNASSIGNED_KEY
    clearDayError(dayKey)
    try {
      const result = await startInstallJob(raw.raw.id)
      toast.success(
        result.alreadyRunning ? "Install already running" : "Install started",
        { description: shortId(raw.raw.id) }
      )
      await load()
    } catch (error) {
      toast.error("Install failed", { description: errorMessage(error) })
      setDayError(dayKey, errorMessage(error))
    }
  }

  // The one destructive act on this screen rides the platform's own confirm
  // instead of a modal of ours. Raws refuse deletion server-side regardless.
  async function deleteObject(object: StoreObject) {
    const confirmed = window.confirm(
      `Delete ${object.fileName} (${object.size})?\n\nRemoves the descriptor, local copy, R2 object, and R2 descriptor mirror.`
    )
    if (!confirmed) return
    try {
      const report = await deleteStoreObject(object.id)
      toast.success("Object deleted", {
        description: `${object.fileName} removed ${formatBytes(report.bytesDeleted)} across store locations.`,
      })
      await load()
    } catch (error) {
      toast.error("Delete failed", { description: errorMessage(error) })
    }
  }

  async function offload(day: DayReadiness) {
    setOffloadingDay(day.marketDay)
    try {
      const report = await offloadDay(day.marketDay)
      toast.success("Day offloaded", {
        description: `Freed ${formatBytes(report.bytesRemoved)} locally; R2 keeps every byte.`,
      })
      await load()
    } catch (error) {
      toast.error("Offload failed", { description: errorMessage(error) })
    } finally {
      setOffloadingDay(null)
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
        offloadingDay={offloadingDay}
        onClose={() => setSelectedKey(null)}
        onInstall={(raw) => void install(raw)}
        onReplay={onReplay}
        onOffload={(day) => void offload(day)}
        onDeleteObject={(object) => void deleteObject(object)}
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
        ? {
            ...job,
            state: nextState,
            subject: event.subject,
            kind: event.kind,
            marketDay: event.marketDay,
          }
        : job
    )
  }
  return [
    ...jobs,
    {
      id: event.jobId,
      kind: event.kind,
      subject: event.subject,
      marketDay: event.marketDay,
      state: nextState,
      startedAtNs: Date.now().toString(),
      finishedAtNs: null,
    },
  ]
}

function shortId(id: string) {
  return id.length > 19 ? `${id.slice(0, 16)}...` : id
}
