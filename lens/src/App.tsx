import { Toaster } from "@/components/ui/sonner"
import { DataCenter } from "@/features/data-center/data-center"
import { Days } from "@/features/days/days"
import { reloadHostView } from "@remux/viewer-kit/host"
import { ActionMenu, ActionMenuItem } from "@remux/viewer-kit/ui"
import { Boxes, CalendarDays, PanelsTopLeft, RotateCw } from "lucide-react"
import { useMemo, useState } from "react"

type Screen = "days" | "objects"

export function App() {
  const [screen, setScreen] = useState<Screen>("days")
  const viewerMenu = useMemo(
    () => (
      <ActionMenu
        align="start"
        icon={<PanelsTopLeft aria-hidden="true" />}
        label="Viewer"
        preserveFocus
      >
        <ActionMenuItem
          disabled={screen === "days"}
          icon={<CalendarDays aria-hidden="true" />}
          label="Days"
          onSelect={() => setScreen("days")}
        />
        <ActionMenuItem
          disabled={screen === "objects"}
          icon={<Boxes aria-hidden="true" />}
          label="Objects"
          onSelect={() => setScreen("objects")}
        />
        <ActionMenuItem
          icon={<RotateCw aria-hidden="true" />}
          label="Reload viewer"
          onSelect={() => {
            void reloadHostView()
          }}
        />
      </ActionMenu>
    ),
    [screen]
  )

  return (
    <>
      {screen === "days" ? (
        <Days viewerMenu={viewerMenu} />
      ) : (
        <DataCenter viewerMenu={viewerMenu} />
      )}
      <Toaster />
    </>
  )
}

export default App
