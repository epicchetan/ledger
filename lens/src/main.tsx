import { initializeIpc } from "@remux/viewer-kit/ipc"
import { mountViewer } from "@remux/viewer-kit/react"
import "@remux/viewer-kit/ui/styles.css"

import App from "./App"
import "./index.css"

mountViewer(<App />, { name: "ledger", initialize: initializeIpc })
