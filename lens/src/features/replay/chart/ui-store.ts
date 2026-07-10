import { createStore } from "zustand/vanilla"

interface ReplayChartUiState {
  scrubMs: number | null
  jumpToLatest: (() => void) | null
  setScrubMs: (scrubMs: number | null) => void
  setJumpToLatest: (jumpToLatest: (() => void) | null) => void
}

export function createReplayChartUiStore() {
  return createStore<ReplayChartUiState>()((set) => ({
    scrubMs: null,
    jumpToLatest: null,
    setScrubMs: (scrubMs) =>
      set((state) => (state.scrubMs === scrubMs ? state : { scrubMs })),
    setJumpToLatest: (jumpToLatest) =>
      set((state) =>
        state.jumpToLatest === jumpToLatest ? state : { jumpToLatest }
      ),
  }))
}

export type ReplayChartUiStore = ReturnType<typeof createReplayChartUiStore>
