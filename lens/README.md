# Ledger Lens

Lens is the Ledger Remux viewer. It builds to static assets under `dist/` and
talks to `ledger-remux` through Remux IPC. Its permanent operating surface is
the Remux phone app, so the layout is phone-first.

The default screen is the Data Center object explorer:

```text
store objects -> metadata -> hydrate/delete -> local status
```

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
top safe area   the filter block pads with max(12px, env(safe-area-inset-top));
                there is no header bar
bottom row      viewer-kit ActionBar owns the bottom safe area: Open tabs
                (explicit { section: "tabs" }), Reload viewer, Refresh data,
                Close tab, plus a live status line
host tab        updateHostTab({ title, status }) mirrors the load state into
                the tab overview; the .catch(() => undefined) is load-bearing
object list     single-column rows (file/size, role/kind/presence glyphs,
                quick hydrate action); tapping a row opens the detail dialog
                with full metadata and Hydrate/Delete actions
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

Data Center does not prepare data, open sessions, manage charts, or run replay
workflows in this phase. Those surfaces will return when the Ledger session
layer exists.
