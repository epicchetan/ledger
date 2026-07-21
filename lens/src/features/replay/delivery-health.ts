import type { RemuxViewHostStatus } from "@remux/viewer-kit/host"

// Initial viewer startup has its own loading state. Once a replay is live,
// however, connecting means the established host transport was lost. Keep
// this distinction separate from projection resync/recovery so the action bar
// only says "reconnecting" when the host actually reports a connection loss.
export function isHostTransportUnavailable(
  status: RemuxViewHostStatus,
  sessionEstablished: boolean
): boolean {
  switch (status.type) {
    case "reconnecting":
    case "closed":
    case "error":
      return true
    case "connecting":
      return sessionEstablished
    case "idle":
    case "connected":
      return false
  }
}
