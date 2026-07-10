# Ledger Lens

Lens is the Ledger Remux viewer. It builds to static assets under `dist/` and
talks to `ledger-remux` through Remux IPC. Its permanent operating surface is
the Remux phone app, so the layout is phone-first.

The default screen is the market-day catalog:

```text
market days -> feed readiness/store objects -> install/offload/replay
```

Replay attaches to Ledger's server-owned session and projection delivery. The
viewer draws a candles-only `bars:1m` chart; clock/transport truth stays on the
server while viewer-local time and price viewport state lives in scoped
Zustand stores and the durable Remux tab route.

There is no `VITE_LEDGER_API_URL` path in this viewer. Data requests use
`requestIpc('remux/ledger/...')`, and hydrate completion arrives through
`subscribeIpcEvents`.

## Styling

Viewer-kit tokens are the design system; Lens carries no palette of its own.
`src/index.css` follows the codex import order:

```css
@import "tailwindcss";
@import "tw-animate-css";
@import "@remux/viewer-kit/tokens.css";
@import "@remux/viewer-kit/theme.css";
```

The rules:

```text
never redeclare semantic vars
  --background/--foreground/--border/--radius/etc. come from tokens.css;
  theme.css promotes them into Tailwind utilities. index.css contains no
  color values of its own.
theme flipping
  the host stamps data-remux-theme; light/dark overrides live inside
  tokens.css. index.css keeps a @custom-variant dark line for the few
  dark:-conditional utilities.
typography
  no web fonts. UI text uses var(--rmx-font-sans) via the body reset;
  data values (ids, hashes, sizes, timestamps, paths, keys) use font-mono,
  mapped locally to var(--rmx-font-mono).
status colors
  badges and toasts use semantic tokens (success/link/destructive), never
  hand-picked palette classes.
```

## Layout and Shell

The shell follows the first-party mobile conventions:

```text
viewport meta   width=device-width ... viewport-fit=cover (required for
                env(safe-area-inset-*) to resolve on iOS)
base reset      html/body/#root height:100%, overflow:hidden; the app shell
                is h-full flex flex-col
top safe area   page content pads with max(12px, env(safe-area-inset-top));
                there is no header bar
bottom row      viewer-kit ActionBar owns the bottom safe area and morphs
                between day/feed actions and replay transport
host tab        updateHostTab persists title/status plus the exact replay
                session identity and versioned viewport route
day list        single-column month-grouped market days; selection expands a
                bounded feed/object tray above the action row
replay chart    full-bleed candles between safe top and action bar; chart
                gestures own the time/price viewport without page scrolling
```

## Development

Lens depends on Remux viewer-kit through a local file link:

```json
"@remux/viewer-kit": "file:../../remux/packages/viewer-kit"
```

That path assumes `ledger` and `remux` are sibling checkouts under the same
parent directory.

Run frontend commands from this directory:

```bash
npm install
npm test
npm run build
npm run lint
```

Run through Remux by adding the ledger parent directory to
`<remux>/.remux/config.toml`:

```toml
extension_roots = ["extensions", "<parent-of-ledger>"]
```

```bash
cd <remux>
npm run dev
```

The ledger repository root is the extension directory; `extension_roots`
entries are parent directories scanned for children containing
`remux-extension.json`. Configured roots replace the default, so
`"extensions"` must stay listed. `REMUX_EXTENSION_ROOTS` remains available
as an environment override.

The chart diagnostic harness is available at `/harness.html` during `vite dev`.
Its `window.harness` object can seed/append/reseek synthetic bars and inspect or
reset the scoped Zustand viewport store.
