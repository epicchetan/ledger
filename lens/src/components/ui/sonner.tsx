import type { CSSProperties } from "react"
import { Toaster as Sonner, type ToasterProps } from "sonner"

type ToastStyle = CSSProperties & Record<"--toast-close-button-start" | "--toast-close-button-end" | "--toast-close-button-transform", string>

export function Toaster(props: ToasterProps) {
  const { style, ...rest } = props
  const toastStyle: ToastStyle = {
    "--toast-close-button-start": "unset",
    "--toast-close-button-end": "0",
    "--toast-close-button-transform": "translate(35%, -35%)",
    ...style,
  }

  return (
    <Sonner
      theme="dark"
      position="top-right"
      offset={{ top: 52, right: 12 }}
      closeButton
      duration={3200}
      visibleToasts={3}
      className="toaster group"
      toastOptions={{
        classNames: {
          toast:
            "group toast !rounded-none !border-border !bg-card !p-3 !font-mono !text-xs !text-foreground !shadow-none",
          title: "!text-xs !font-medium !leading-5 !text-foreground",
          description: "!text-xs !leading-5 !text-muted-foreground",
          closeButton: "!rounded-none !border-border !bg-card !text-muted-foreground hover:!bg-muted hover:!text-foreground",
          actionButton:
            "!h-7 !rounded-none !border !border-border !bg-primary !px-2 !text-xs !text-primary-foreground",
          cancelButton: "!h-7 !rounded-none !bg-muted !px-2 !text-xs !text-muted-foreground",
          success: "!border-emerald-500/30",
          error: "!border-red-400/35",
          info: "!border-border",
        },
      }}
      style={toastStyle}
      {...rest}
    />
  )
}
