import {
  openHostOverview,
  reloadHostView,
} from "@remux/viewer-kit/host"
import { ActionBar, ActionButton } from "@remux/viewer-kit/ui"
import {
  ArrowLeft,
  ChevronDown,
  Download,
  Loader2,
  PanelRightOpen,
  RefreshCw,
  Trash2,
} from "lucide-react"
import { useState } from "react"

import type { StoreObject } from "@/features/data-center/types"
import { DayFeeds } from "@/features/days/day-feeds"
import type { DayReadiness } from "@/features/days/readiness"
import type { EsRawStatus } from "@/features/days/types"
import { cn } from "@/lib/utils"

interface DayActionBarProps {
  day: DayReadiness | null
  status: string
  hydratingIds: Set<string>
  onClose: () => void
  onPrepare: (raw: EsRawStatus, force: boolean) => void
  onHydrate: (object: StoreObject) => void
  onDeleteObject: (object: StoreObject) => void
}

// One action bar that morphs with selection. The core buttons (tab out, reload)
// are pinned in every state. Picking a day expands a panel upward on the same
// bar surface — the feed pipeline, its next-step action, and store objects — so
// the day-scoped control the bar itself carries is minimize. Drilling into an
// object doesn't take the bar over: it just adds that object's controls (back,
// delete, hydrate) alongside the pinned ones for as long as it is open.
export function DayActionBar({
  day,
  status,
  hydratingIds,
  onClose,
  onPrepare,
  onHydrate,
  onDeleteObject,
}: DayActionBarProps) {
  const open = day !== null

  // Retain the last day through the collapse so the panel animates closed with
  // its content intact rather than snapping to empty. Deriving from a prop
  // during render (not in an effect) is React's sanctioned pattern for this.
  const [shown, setShown] = useState<DayReadiness | null>(day)
  if (day && day !== shown) setShown(day)

  // The drilled-in object lives here, not in the panel body, so the bar can
  // carry its controls. Reset on the market-day string, not the derived day's
  // identity: a background refresh re-derives the day object every poll, and
  // keying off that would kick the user out of an open object mid-view.
  const [objectId, setObjectId] = useState<string | null>(null)
  const [dayKey, setDayKey] = useState<string | null>(day?.marketDay ?? null)
  if (day && day.marketDay !== dayKey) {
    setDayKey(day.marketDay)
    setObjectId(null)
  }

  const openObject =
    objectId && shown
      ? (shown.storageObjects.find((object) => object.id === objectId) ?? null)
      : null
  const hydrating = openObject ? hydratingIds.has(openObject.id) : false

  return (
    <ActionBar
      className="lens-action-bar"
      status={status}
      left={
        <div className="flex w-full min-w-0 flex-col">
          <div
            className={cn(
              "grid -mx-[18px] transition-[grid-template-rows] duration-200 ease-out",
              open ? "grid-rows-[1fr]" : "grid-rows-[0fr]"
            )}
          >
            <div className="overflow-hidden">
              {shown ? (
                <div className="lens-scroll max-h-[52svh] overflow-y-auto px-[18px] pb-3">
                  <DayFeeds
                    day={shown}
                    openObject={openObject}
                    hydratingIds={hydratingIds}
                    onOpenObject={(object) => setObjectId(object.id)}
                    onPrepare={onPrepare}
                    onHydrate={onHydrate}
                  />
                </div>
              ) : null}
            </div>
          </div>

          <div className="flex w-full items-center gap-1.5">
            <ActionButton
              icon={<PanelRightOpen aria-hidden="true" />}
              label="Open tabs"
              onClick={() => {
                void openHostOverview({ section: "tabs" })
              }}
            />
            <ActionButton
              icon={<RefreshCw aria-hidden="true" />}
              label="Reload viewer"
              onClick={() => {
                void reloadHostView()
              }}
            />

            {open ? (
              <div className="ml-auto flex items-center gap-1.5">
                {openObject ? (
                  <>
                    <ActionButton
                      icon={
                        hydrating ? (
                          <Loader2
                            className="animate-spin"
                            aria-hidden="true"
                          />
                        ) : (
                          <Download aria-hidden="true" />
                        )
                      }
                      label={
                        openObject.local
                          ? "Touch local copy"
                          : "Hydrate object"
                      }
                      tone="primary"
                      busy={hydrating}
                      disabled={hydrating}
                      onClick={() => onHydrate(openObject)}
                    />
                    <ActionButton
                      icon={<Trash2 aria-hidden="true" />}
                      label="Delete object"
                      onClick={() => onDeleteObject(openObject)}
                    />
                    <ActionButton
                      icon={<ArrowLeft aria-hidden="true" />}
                      label="Back to feed"
                      onClick={() => setObjectId(null)}
                    />
                    <span
                      className="mx-0.5 h-5 w-px shrink-0 bg-border"
                      aria-hidden="true"
                    />
                  </>
                ) : null}
                <ActionButton
                  icon={<ChevronDown aria-hidden="true" />}
                  label="Minimize"
                  onClick={onClose}
                />
              </div>
            ) : null}
          </div>
        </div>
      }
    />
  )
}
