import {
  requestIpc,
  subscribeIpcEvents,
  type JsonRpcMessage,
} from "@remux/viewer-kit/ipc"

import type {
  EsDayCatalog,
  EsOffloadReport,
  JobFinishedEvent,
  JobProgressEvent,
  JobsListResult,
  JobStartResult,
} from "@/features/days/types"

export async function fetchEsDays(): Promise<EsDayCatalog> {
  return requestIpc<EsDayCatalog>("remux/ledger/es/days", {})
}

export async function fetchJobs(): Promise<JobsListResult> {
  return requestIpc<JobsListResult>("remux/ledger/jobs/list", {})
}

// Install: make the feed's artifact local, whatever that takes. The backend
// hydrates an existing artifact or rebuilds a missing/invalid one from the
// R2 raw — one verb, no prepare/hydrate split on this side.
export async function startInstallJob(rawId: string): Promise<JobStartResult> {
  return requestIpc<JobStartResult>("remux/ledger/es/install", { rawId })
}

// Offload: drop the day's local copies while R2 keeps every byte. Synchronous.
export async function offloadDay(marketDay: string): Promise<EsOffloadReport> {
  return requestIpc<EsOffloadReport>("remux/ledger/es/offload", { marketDay })
}

export function subscribeLedgerJobs(subscriber: {
  progress: (event: JobProgressEvent) => void
  finished: (event: JobFinishedEvent) => void
}) {
  return subscribeIpcEvents((events) => {
    for (const message of events) {
      const progress = parseProgressEvent(message)
      if (progress) {
        subscriber.progress(progress)
        continue
      }
      const finished = parseFinishedEvent(message)
      if (finished) subscriber.finished(finished)
    }
  })
}

function parseProgressEvent(message: JsonRpcMessage): JobProgressEvent | null {
  if (message.method !== "remux/ledger/jobs/progress") return null
  if (!message.params || typeof message.params !== "object") return null
  const params = message.params as Partial<JobProgressEvent>
  if (
    typeof params.jobId !== "string" ||
    typeof params.kind !== "string" ||
    typeof params.subject !== "string" ||
    typeof params.stage !== "string"
  ) {
    return null
  }
  return {
    jobId: params.jobId,
    kind: params.kind,
    subject: params.subject,
    marketDay: typeof params.marketDay === "string" ? params.marketDay : null,
    stage: params.stage,
    records: typeof params.records === "number" ? params.records : null,
  }
}

function parseFinishedEvent(message: JsonRpcMessage): JobFinishedEvent | null {
  if (message.method !== "remux/ledger/jobs/finished") return null
  if (!message.params || typeof message.params !== "object") return null
  const params = message.params as Partial<JobFinishedEvent>
  if (typeof params.jobId !== "string" || typeof params.ok !== "boolean") {
    return null
  }
  return {
    jobId: params.jobId,
    kind: typeof params.kind === "string" ? params.kind : "",
    subject: typeof params.subject === "string" ? params.subject : "",
    marketDay: typeof params.marketDay === "string" ? params.marketDay : null,
    ok: params.ok,
    summary: params.summary,
    error: typeof params.error === "string" ? params.error : null,
  }
}
