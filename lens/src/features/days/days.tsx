import {
  closeHostTab,
  openHostOverview,
  subscribeHostResume,
  updateHostTab,
} from "@remux/viewer-kit/host"
import { ActionBar, ActionButton } from "@remux/viewer-kit/ui"
import {
  CloudDownload,
  Loader2,
  PanelRightOpen,
  RefreshCw,
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
  fetchEsDays,
  fetchJobs,
  startFetchJob,
  startPrepareJob,
  subscribeLedgerJobs,
} from "@/features/days/api"
import { DayList } from "@/features/days/day-list"
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

export function Days({ viewerMenu }: { viewerMenu?: ReactNode }) {
  const [catalog, setCatalog] = useState<EsDayCatalog>(EMPTY_CATALOG)
  const [jobs, setJobs] = useState<JobRecord[]>([])
  const [rowErrors, setRowErrors] = useState<Map<string, string>>(new Map())
  const [loadState, setLoadState] = useState<DaysLoadState>({
    kind: "loading",
    message: "Loading ES days.",
  })
  const [fetchOpen, setFetchOpen] = useState(false)
  const [fetching, setFetching] = useState(false)

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

  async function fetchDay(marketDay: string, symbol: string) {
    setFetching(true)
    try {
      const result = await startFetchJob(marketDay, symbol)
      toast.success(result.alreadyRunning ? "Fetch already running" : "Fetch started", {
        description: `${marketDay} ${symbol}`,
      })
      setFetchOpen(false)
      await load()
    } catch (error) {
      toast.error("Fetch failed", { description: errorMessage(error) })
    } finally {
      setFetching(false)
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
            days={catalog.days}
            jobsBySubject={jobsBySubject}
            onPrepare={(raw, force) => void prepare(raw, force)}
            rowErrors={rowErrors}
            unassigned={catalog.unassigned}
          />
        )}
      </main>

      <ActionBar
        left={
          <>
            {viewerMenu}
            <ActionButton
              busy={loadState.kind === "loading"}
              disabled={loadState.kind === "loading"}
              icon={<RefreshCw aria-hidden="true" />}
              label="Refresh"
              onClick={() => {
                void load()
              }}
            />
            <ActionButton
              icon={<CloudDownload aria-hidden="true" />}
              label="Fetch day"
              onClick={() => setFetchOpen(true)}
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

      <FetchDayDialog
        fetching={fetching}
        onConfirm={(marketDay, symbol) => void fetchDay(marketDay, symbol)}
        onOpenChange={setFetchOpen}
        open={fetchOpen}
      />
    </div>
  )
}

function FetchDayDialog({
  fetching,
  onConfirm,
  onOpenChange,
  open,
}: {
  fetching: boolean
  onConfirm: (marketDay: string, symbol: string) => void
  onOpenChange: (open: boolean) => void
  open: boolean
}) {
  const [marketDay, setMarketDay] = useState("")
  const [symbol, setSymbol] = useState("")

  function submit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault()
    const day = marketDay.trim()
    const nextSymbol = symbol.trim().toUpperCase()
    if (!day || !nextSymbol) return
    onConfirm(day, nextSymbol)
  }

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="sm:max-w-md">
        <form onSubmit={submit}>
          <DialogHeader>
            <DialogTitle>Fetch day</DialogTitle>
            <DialogDescription>
              Databento ES MBO raw download.
            </DialogDescription>
          </DialogHeader>
          <div className="my-4 grid gap-2">
            <Input
              className="text-xs"
              disabled={fetching}
              onChange={(event) => setMarketDay(event.target.value)}
              placeholder="YYYY-MM-DD"
              value={marketDay}
            />
            <Input
              className="text-xs uppercase"
              disabled={fetching}
              onChange={(event) => setSymbol(event.target.value)}
              placeholder="ESH6"
              value={symbol}
            />
          </div>
          <DialogFooter>
            <Button
              disabled={fetching}
              onClick={() => onOpenChange(false)}
              type="button"
              variant="outline"
            >
              Cancel
            </Button>
            <Button disabled={fetching} type="submit">
              {fetching ? <Loader2 className="size-3.5 animate-spin" /> : null}
              Fetch
            </Button>
          </DialogFooter>
        </form>
      </DialogContent>
    </Dialog>
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
