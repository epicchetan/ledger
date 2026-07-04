import { useEffect, useState, type CSSProperties } from "react"
import { Toaster as Sonner, type ToasterProps } from "sonner"

type ToastStyle = CSSProperties &
  Record<
    | "--toast-close-button-start"
    | "--toast-close-button-end"
    | "--toast-close-button-transform",
    string
  >
type RemuxTheme = "light" | "dark"

export function Toaster(props: ToasterProps) {
  const { style, ...rest } = props
  const theme = useRemuxTheme()
  const toastStyle: ToastStyle = {
    "--toast-close-button-start": "unset",
    "--toast-close-button-end": "0",
    "--toast-close-button-transform": "translate(35%, -35%)",
    ...style,
  }

  return (
    <Sonner
      theme={theme}
      position="top-right"
      offset={{ top: "max(12px, env(safe-area-inset-top))", right: 12 }}
      closeButton
      duration={3200}
      visibleToasts={3}
      className="toaster group"
      toastOptions={{
        classNames: {
          toast:
            "group toast !rounded-lg !border-border !bg-card !p-3 !font-mono !text-xs !text-foreground !shadow-[var(--rmx-shadow-menu)]",
          title: "!text-xs !font-medium !leading-5 !text-foreground",
          description: "!text-xs !leading-5 !text-muted-foreground",
          closeButton:
            "!rounded-md !border-border !bg-card !text-muted-foreground hover:!bg-muted hover:!text-foreground",
          actionButton:
            "!h-7 !rounded-md !border !border-border !bg-primary !px-2 !text-xs !text-primary-foreground",
          cancelButton:
            "!h-7 !rounded-md !bg-muted !px-2 !text-xs !text-muted-foreground",
          success: "!border-success/30",
          error: "!border-destructive/40",
          info: "!border-border",
        },
      }}
      style={toastStyle}
      {...rest}
    />
  )
}

function useRemuxTheme(): RemuxTheme {
  const [theme, setTheme] = useState(readRemuxTheme)

  useEffect(() => {
    const observer = new MutationObserver(() => {
      setTheme(readRemuxTheme())
    })
    observer.observe(document.documentElement, {
      attributeFilter: ["data-remux-theme"],
      attributes: true,
    })
    return () => observer.disconnect()
  }, [])

  return theme
}

function readRemuxTheme(): RemuxTheme {
  return document.documentElement.dataset.remuxTheme === "light"
    ? "light"
    : "dark"
}
