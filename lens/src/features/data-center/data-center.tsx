import { AlertTriangle, RefreshCw, Send } from "lucide-react"
import { useCallback, useEffect, useMemo, useRef, useState } from "react"
import { toast } from "sonner"

import { Button } from "@/components/ui/button"
import { Input } from "@/components/ui/input"
import {
  deleteRawMarketData,
  deleteReplayDataset,
  fetchActiveJobs,
  fetchMarketDays,
  fetchMarketDayStatus,
  fetchJob,
  prepareReplayDataset,
  rebuildReplayDataset,
  validateReplayDataset,
} from "@/features/data-center/api"
import { MarketDayTable } from "@/features/data-center/market-day-table"
import type { DataCenterLoadState, DatasetAction, JobRecord, MarketDay, MarketDayStatus } from "@/features/data-center/types"

const JOB_POLL_MS = 1_500
const JOB_IDLE_POLL_MS = 5_000

function updateDay(days: MarketDay[], nextDay: MarketDay) {
  return days.map((day) => (day.id === nextDay.id ? nextDay : day))
}

function errorMessage(error: unknown) {
  return error instanceof Error ? error.message : "Unknown error"
}

export function DataCenter() {
  const [days, setDays] = useState<MarketDay[]>([])
  const [pendingPrepare, setPendingPrepare] = useState(false)
  const [prepareContract, setPrepareContract] = useState("ESH6")
  const [prepareDate, setPrepareDate] = useState("")
  const [jobs, setJobs] = useState<JobRecord[]>([])
  const refreshGeneration = useRef(0)
  const hadActiveJobs = useRef(false)
  const [loadState, setLoadState] = useState<DataCenterLoadState>({
    kind: "loading",
    message: "Connecting to Ledger API.",
  })

  const replaceActiveJobs = useCallback((nextJobs: JobRecord[]) => {
    const activeJobs = nextJobs.filter(isActiveJob).slice(0, 5)
    setJobs(activeJobs)
    return activeJobs
  }, [])

  const refresh = useCallback(async () => {
    const generation = refreshGeneration.current + 1
    refreshGeneration.current = generation
    setLoadState({ kind: "loading", message: "Loading cataloged market days from Ledger API." })

    try {
      const nextDays = await fetchMarketDays()
      const activeJobs = await fetchActiveJobs()
      if (refreshGeneration.current !== generation) return

      setDays(nextDays)
      replaceActiveJobs(activeJobs)

      if (nextDays.length === 0) {
        setLoadState({
          kind: "empty",
          message: "No market days are cataloged yet. Ingest a day from the CLI or API first.",
        })
      } else {
        setLoadState({
          kind: "ready",
          message: `Loaded ${nextDays.length} cataloged market day${nextDays.length === 1 ? "" : "s"}.`,
        })
        hydrateStatuses(nextDays, generation)
      }
    } catch (error) {
      setDays([])
      setLoadState({
        kind: "error",
        message: `Ledger API request failed: ${errorMessage(error)}`,
      })
      toast.error("Ledger API unavailable", {
        description: "Start ledger-api on 127.0.0.1:3001 or set VITE_LEDGER_API_URL.",
      })
    }
  }, [replaceActiveJobs])

  function hydrateStatuses(nextDays: MarketDay[], generation: number) {
    for (const day of nextDays) {
      void fetchMarketDayStatus(day)
        .then((nextDay) => {
          if (refreshGeneration.current !== generation) return
          setDays((current) => updateDay(current, nextDay))
        })
        .catch(() => {
          // Keep the catalog-derived row visible. Status hydration can be retried
          // without blocking the whole Data Center surface.
        })
    }
  }

  useEffect(() => {
    void refresh()
  }, [refresh])

  useEffect(() => {
    let cancelled = false
    let timeout: number | null = null

    async function pollActiveJobs() {
      let nextDelay = JOB_IDLE_POLL_MS
      try {
        const activeJobs = replaceActiveJobs(await fetchActiveJobs())
        if (cancelled) return

        const hasActiveJobs = activeJobs.length > 0
        if (!hasActiveJobs && hadActiveJobs.current) {
          void refresh()
        }
        hadActiveJobs.current = hasActiveJobs
        nextDelay = hasActiveJobs ? JOB_POLL_MS : JOB_IDLE_POLL_MS
      } catch {
        // Keep the current catalog visible if the API is briefly unavailable.
        // The main refresh path owns user-facing connection errors.
      }

      if (!cancelled) {
        timeout = window.setTimeout(() => void pollActiveJobs(), nextDelay)
      }
    }

    void pollActiveJobs()
    return () => {
      cancelled = true
      if (timeout !== null) {
        window.clearTimeout(timeout)
      }
    }
  }, [refresh, replaceActiveJobs])

  function upsertJob(job: JobRecord) {
    setJobs((current) => [job, ...current.filter((item) => item.id !== job.id)].filter(isActiveJob).slice(0, 5))
  }

  async function waitForJob(job: JobRecord): Promise<JobRecord> {
    let current = job
    upsertJob(current)
    while (current.status === "queued" || current.status === "running") {
      await sleep(JOB_POLL_MS)
      current = await fetchJob(current.id)
      upsertJob(current)
    }
    if (current.status === "failed") {
      throw new Error(current.error ?? "Job failed")
    }
    return current
  }

  async function prepareMarketDay(contract: string, marketDate: string) {
    const nextContract = contract.trim().toUpperCase()
    if (!nextContract || !marketDate) {
      toast.error("Prepare requires a contract and date")
      return
    }

    setPendingPrepare(true)
    try {
      const job = await prepareReplayDataset(nextContract, marketDate)
      toast.success("Prepare job started", {
        description: `${nextContract} ${marketDate} job ${shortJobId(job.id)}`,
      })
      await waitForJob(job)
      toast.success("ReplayDataset prepared", {
        description: `${nextContract} ${marketDate} has durable raw and replay layers.`,
      })
      await refresh()
    } catch (error) {
      toast.error("Prepare failed", {
        description: errorMessage(error),
      })
    } finally {
      setPendingPrepare(false)
    }
  }

  async function runAction(day: MarketDay, action: DatasetAction) {
    try {
      if (action === "prepare") {
        await prepareMarketDay(day.contract, day.marketDate)
      } else {
        const job = await startDatasetAction(day, action)
        toast.success(`${actionLabel(action)} job started`, {
          description: `${day.contract} ${day.marketDate} job ${shortJobId(job.id)}`,
        })
        await waitForJob(job)
        await refresh()
      }
    } catch (error) {
      toast.error(`${actionLabel(action)} failed`, {
        description: errorMessage(error),
      })
    }
  }

  const activeJobs = jobs.filter(isActiveJob)
  const activeJobsByDayId = useMemo(() => jobsByDayId(activeJobs), [activeJobs])
  const activeJobDayIds = useMemo(() => new Set(activeJobsByDayId.keys()), [activeJobsByDayId])
  const displayDays = useMemo(
    () =>
      days.map((day) => {
        const activeJob = activeJobsByDayId.get(day.id)
        return activeJob ? { ...day, status: statusForActiveJob(activeJob) } : day
      }),
    [days, activeJobsByDayId],
  )

  return (
    <div className="flex h-svh flex-col bg-background text-foreground">
      <header className="flex h-10 shrink-0 items-center justify-between border-b-2 border-[rgba(255,255,255,0.15)] px-3">
        <div className="flex min-w-0 items-center gap-3">
          <div className="text-sm font-semibold">Ledger</div>
          <div className="h-4 w-px bg-border" />
          <div className="truncate text-xs text-muted-foreground">Data Center</div>
          <ConnectionBadge state={loadState} />
        </div>
        <Button type="button" variant="ghost" size="sm" onClick={() => void refresh()} disabled={loadState.kind === "loading"}>
          <RefreshCw className={loadState.kind === "loading" ? "size-3.5 animate-spin" : "size-3.5"} />
          Refresh
        </Button>
      </header>

      <PrepareBar
        contract={prepareContract}
        marketDate={prepareDate}
        pending={pendingPrepare}
        onContractChange={setPrepareContract}
        onDateChange={setPrepareDate}
        onPrepare={() => void prepareMarketDay(prepareContract, prepareDate)}
      />

      <main className="flex min-h-0 flex-1 flex-col gap-3 p-3">
        {activeJobs.length > 0 ? <JobActivity jobs={activeJobs} /> : null}
        {loadState.kind === "error" || loadState.kind === "empty" ? (
          <DataCenterStatePanel title={loadState.kind === "error" ? "API Error" : "No Market Days"} message={loadState.message} onRetry={loadState.kind === "error" ? refresh : undefined} />
        ) : (
          <MarketDayTable days={displayDays} activeJobDayIds={activeJobDayIds} onAction={runAction} />
        )}
      </main>
    </div>
  )
}

function isActiveJob(job: JobRecord) {
  return job.status === "queued" || job.status === "running"
}

function jobsByDayId(jobs: JobRecord[]) {
  const map = new Map<string, JobRecord>()
  for (const job of jobs) {
    const dayId = job.target?.market_day_id ?? job.market_day_id
    if (dayId && !map.has(dayId)) {
      map.set(dayId, job)
    }
  }
  return map
}

function statusForActiveJob(job: JobRecord): MarketDayStatus {
  switch (job.kind) {
    case "delete_replay_dataset":
    case "delete_raw_market_data":
      return "deleting"
    default:
      return "loading"
  }
}

function actionLabel(action: DatasetAction) {
  switch (action) {
    case "prepare":
      return "Prepare"
    case "rebuild":
      return "Rebuild"
    case "validate":
      return "Validate"
    case "deleteReplay":
      return "Delete ReplayDataset"
    case "deleteRaw":
      return "Delete Raw Data"
  }
}

function startDatasetAction(day: MarketDay, action: DatasetAction) {
  switch (action) {
    case "prepare":
      return prepareReplayDataset(day.contract, day.marketDate)
    case "rebuild":
      return rebuildReplayDataset(day)
    case "validate":
      return validateReplayDataset(day)
    case "deleteReplay":
      return deleteReplayDataset(day)
    case "deleteRaw":
      return deleteRawMarketData(day)
  }
}

function sleep(ms: number) {
  return new Promise((resolve) => window.setTimeout(resolve, ms))
}

function shortJobId(jobId: string) {
  return jobId.slice(0, 8)
}

function PrepareBar({
  contract,
  marketDate,
  pending,
  onContractChange,
  onDateChange,
  onPrepare,
}: {
  contract: string
  marketDate: string
  pending: boolean
  onContractChange: (value: string) => void
  onDateChange: (value: string) => void
  onPrepare: () => void
}) {
  return (
    <section className="flex shrink-0 flex-wrap items-center gap-2 border-b border-border px-3 py-2">
      <div className="mr-1 text-xs font-medium">Prepare MarketDay</div>
      <Input
        className="h-7 w-24 text-xs uppercase"
        value={contract}
        onChange={(event) => onContractChange(event.target.value)}
        placeholder="ESH6"
        disabled={pending}
      />
      <Input
        className="h-7 w-36 text-xs"
        type="date"
        value={marketDate}
        onChange={(event) => onDateChange(event.target.value)}
        disabled={pending}
      />
      <Button type="button" variant="outline" size="sm" onClick={onPrepare} disabled={pending}>
        <Send className={pending ? "size-3.5 animate-pulse" : "size-3.5"} />
        Prepare
      </Button>
    </section>
  )
}

function JobActivity({ jobs }: { jobs: JobRecord[] }) {
  return (
    <section className="border border-border bg-card/40">
      <div className="border-b border-border px-3 py-2">
        <h2 className="text-sm font-semibold">Jobs</h2>
      </div>
      <div className="divide-y divide-border">
        {jobs.map((job) => {
          const latest = job.error ?? job.progress.at(-1) ?? "queued"
          return (
            <div key={job.id} className="grid grid-cols-[minmax(0,1fr)_auto] gap-3 px-3 py-2 text-xs">
              <div className="min-w-0">
                <div className="truncate font-medium">
                  {job.kind} <span className="text-muted-foreground">{shortJobId(job.id)}</span>
                </div>
                <div className="mt-1 truncate text-muted-foreground" title={latest}>
                  {latest}
                </div>
              </div>
              <div className="self-start border border-border px-1.5 py-0.5 text-[0.68rem] text-muted-foreground">
                {job.status}
              </div>
            </div>
          )
        })}
      </div>
    </section>
  )
}

function ConnectionBadge({ state }: { state: DataCenterLoadState }) {
  const tone =
    state.kind === "ready"
      ? "border-emerald-500/30 bg-emerald-500/10 text-emerald-300"
      : state.kind === "error"
        ? "border-red-400/35 bg-red-400/10 text-red-300"
        : "border-border bg-muted/20 text-muted-foreground"

  return <div className={`hidden border px-1.5 py-0.5 text-[0.68rem] sm:block ${tone}`}>{state.kind}</div>
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
      <p className="max-w-xl text-xs leading-5 text-muted-foreground">{message}</p>
      {onRetry ? (
        <Button type="button" variant="outline" size="sm" className="mt-4" onClick={() => void onRetry()}>
          <RefreshCw className="size-3.5" />
          Retry
        </Button>
      ) : null}
    </div>
  )
}
