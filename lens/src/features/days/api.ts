import {
  requestIpc,
  subscribeIpcEvents,
  type JsonRpcMessage,
} from "@remux/viewer-kit/ipc"

import type {
  EsDayCatalog,
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

export async function startPrepareJob(
  rawId: string,
  force = false
): Promise<JobStartResult> {
  return requestIpc<JobStartResult>("remux/ledger/es/prepare", {
    rawId,
    force,
  })
}

export async function startFetchJob(
  marketDay: string,
  symbol: string
): Promise<JobStartResult> {
  return requestIpc<JobStartResult>("remux/ledger/es/fetch", {
    marketDay,
    symbol,
  })
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
    ok: params.ok,
    summary: params.summary,
    error: typeof params.error === "string" ? params.error : null,
  }
}
