import { useState } from "react"
import { ChartNoAxesCombined } from "lucide-react"

import { Button } from "@/components/ui/button"
import {
  Dialog,
  DialogContent,
  DialogTitle,
  DialogTrigger,
} from "@/components/ui/dialog"
import {
  Command,
  CommandGroup,
  CommandInput,
  CommandItem,
  CommandList,
} from "@/components/ui/command"

interface IndicatorPickerProps {
  showDom: boolean
  onToggleDom: () => void
  disabled?: boolean
}

export function IndicatorPicker({ showDom, onToggleDom, disabled }: IndicatorPickerProps) {
  const [open, setOpen] = useState(false)

  return (
    <Dialog open={open} onOpenChange={setOpen}>
      <DialogTrigger asChild>
        <Button variant="ghost" size="sm" className="font-mono text-xs gap-1.5" disabled={disabled}>
          <ChartNoAxesCombined className="size-3.5" />
          Indicators
        </Button>
      </DialogTrigger>
      <DialogContent showCloseButton={false} className="p-0 gap-0 sm:max-w-sm">
        <DialogTitle className="sr-only">Indicators</DialogTitle>
        <Command>
          <CommandInput placeholder="Search indicators..." />
          <CommandList>
            <CommandGroup>
              <CommandItem data-checked={showDom} onSelect={onToggleDom}>
                DOM
              </CommandItem>
            </CommandGroup>
          </CommandList>
        </Command>
      </DialogContent>
    </Dialog>
  )
}
