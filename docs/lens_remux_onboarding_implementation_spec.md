# Lens Remux Onboarding Implementation Spec

## Purpose

This spec makes Lens a first-class Remux viewer.

The extension conversion (`docs/remux_extension_implementation_spec.md`) made
Lens *work* inside Remux. This pass makes it *native*: Ledger is a personal
extension whose permanent operating surface is the Remux phone app, so Lens
stops carrying its own parallel design system and adopts the Remux one
wholesale.

```text
1. theming: delete the hand-maintained oklch palettes; sit directly on
   viewer-kit tokens (tokens.css + theme.css), exactly like codex
2. shell: adopt the first-party mobile shell (viewport meta, html/body
   reset, safe areas, overscroll, tap-highlight)
3. bottom row: ActionBar with exit-to-tabs, reload, refresh, close;
   host tab metadata via updateHostTab
4. layout: replace the 980px desktop table with a phone-first object
   list + detail dialog
5. delete everything the new surface no longer uses
```

This is deliberately not backwards compatible. Lens has no other consumer;
git history preserves the old surface.

## Motivation

Lens today is a desktop shadcn app that happens to render inside a Remux
tab: its own two-theme oklch palette duplicating what viewer-kit tokens
already provide, a flat rounded-none look the rest of Remux does not share,
a table that forces horizontal scrolling at phone width, hover-card
interactions that cannot fire on touch, no safe-area handling (the viewport
meta lacks `viewport-fit=cover`, so insets resolve to zero on iOS), and a
font that was never actually loaded.

The viewer-kit token architecture removes the reason for any of it to be
local. `tokens.css` ships three tiers: `--rmx-*` primitives, `--rmx-*`
semantic roles with light/dark flipping keyed off `data-remux-theme`, and —
critically — a full set of un-prefixed shadcn bindings (`--background`,
`--foreground`, `--card`, `--popover`, `--primary`, `--muted`, `--border`,
`--input`, `--ring`, `--destructive`, `--radius`, plus `--success`,
`--warning`, `--link`). `theme.css` promotes those into Tailwind v4
utilities via `@theme inline`. A shadcn viewer that imports both files gets
the entire Remux look — colors, radius, host-driven theme flipping — with
zero local palette code. Codex is the in-tree proof of this pattern.

## Scope

In scope:

```text
index.css rewrite onto viewer-kit tokens.css + theme.css (codex pattern)
first-party shell reset, viewport meta, safe-area handling
ActionBar bottom row with exit to tabs, reload, refresh, close
updateHostTab wiring for tab overview metadata
object list + detail dialog replacing the desktop table stack
semantic status colors (success/warning/destructive) for badges
typography onto token font stacks (system fonts, no font packages)
Remux radius/elevation language (drop rounded-none/shadow-none overrides)
deletion of orphaned components and dependencies
lens/README.md rewrite of the styling/layout sections
```

Out of scope:

```text
any ledger-remux / crates change (method surface is untouched)
viewer-kit /shadcn component adoption (only cn/Separator/Sidebar/Sheet
  exist there; none fit this surface yet)
new Data Center capability (no new methods, no new data)
subscribeHostNavigate (single-surface viewer; nothing to navigate yet)
keyboard lift (no composer; the filter inputs live in the scroll flow)
web/laptop layout work (remux web host is a later phase)
```

## Style Contract

Verified against `remux/packages/viewer-kit/src/tokens/` (generated from
`primitives.ts`) and `remux/extensions/codex/viewer/app.css`, the only
in-tree Tailwind viewer and the pattern Lens follows.

The rules:

```text
import order (in index.css, mirroring codex app.css):
  @import "tailwindcss";
  @import "tw-animate-css";
  @import "@remux/viewer-kit/tokens.css";
  @import "@remux/viewer-kit/theme.css";
  @source  "../node_modules/@remux/viewer-kit/src";

never redeclare --background/--foreground/--border/--radius/etc.
  tokens.css owns them; lens deletes both of its :root palette blocks and
  every palette/radius/shadow mapping in its @theme inline block
  (theme.css provides those). What survives locally is one minimal
  @theme inline for fonts (see Typography): theme.css maps colors and
  radii, not font stacks

theme flipping comes from tokens.css
  the host stamps data-remux-theme before first paint; light overrides and
  the prefers-color-scheme fallback live inside tokens.css; lens keeps its
  @custom-variant dark line for the few dark:-conditional utility cases

viewer-local roles only
  if lens ever needs a color tokens.css lacks, it defines a viewer-local
  var and promotes it with a small local @theme inline (the codex --chrome
  pattern); it never forks a semantic token
```

What this changes visually, for free:

```text
--primary becomes the Remux accent orange: primary buttons in lens adopt
  the same accent as codex send / terminal active keys
--destructive becomes --rmx-danger, flipping correctly per theme
--radius becomes 10px: every stock shadcn component (button, dialog,
  input, select) rounds to the Remux radius language without edits
success/warning utilities (text-success, etc.) become available for
  status badges
```

`main.tsx` drops its `@remux/viewer-kit/tokens.css` import (index.css now
owns the CSS graph so Tailwind processes `theme.css`) and keeps
`@remux/viewer-kit/ui/styles.css` for the ActionBar shell.

`tw-animate-css` turns out to be like the font: declared but never
imported, so the dialog's `animate-in/out` classes have been silent no-ops.
It stays as a dependency and gets actually imported, restoring dialog and
future Sheet animations.

## Shell and Safe Area

`index.html` viewport meta matches every first-party viewer (without
`viewport-fit=cover`, `env(safe-area-inset-*)` is zero on iOS):

```html
<meta name="viewport" content="width=device-width, initial-scale=1, maximum-scale=1, user-scalable=no, viewport-fit=cover" />
```

Base reset in `index.css` (shared verbatim across first-party viewers):

```css
@layer base {
  * { box-sizing: border-box; }
  html, body, #root {
    height: 100%;
    min-height: 100%;
    margin: 0;
    overflow: hidden;
    text-size-adjust: 100%;
    -webkit-text-size-adjust: 100%;
  }
  body {
    background: var(--background);
    color: var(--foreground);
    -webkit-font-smoothing: antialiased;
    text-rendering: optimizeLegibility;
  }
}
```

Conventions applied to the lens surface:

```text
app shell        h-full flex flex-col (no dvh; the WebView sizes #root)
top safe area    the first-party pattern is content padding, not a header
                 bar: the filter block gets
                 padding-top: max(12px, env(safe-area-inset-top))
bottom safe area owned by ActionBar (max(12px, env(safe-area-inset-bottom)))
scroll region    flex-1 min-h-0 overflow-y-auto,
                 overscroll-behavior: contain,
                 -webkit-overflow-scrolling: touch
interactives     -webkit-tap-highlight-color: transparent
```

The current `header` element is removed entirely: its brand text is
redundant with the tab chrome, and its status figures move to the ActionBar
status slot and the tab metadata.

## Bottom Row and Host Tab

Imports:

```tsx
import { ActionBar, ActionButton } from "@remux/viewer-kit/ui"
import {
  closeHostTab,
  openHostOverview,
  reloadHostView,
  updateHostTab,
} from "@remux/viewer-kit/host"
```

The ActionBar is the last child of the shell; the kit owns its safe-area
inset, border, gradient, and button elevation:

```text
left    Open tabs      PanelRightOpen  () => void openHostOverview({ section: "tabs" })
        Reload viewer  RotateCw        () => void reloadHostView()
        Refresh data   RefreshCw       () => void refresh()
                       busy: loadState.kind === "loading"
right   Close tab      X               () => void closeHostTab()
status  "12 objects · 3.4 GB · local 1.2/50 GB"   ("Local -" while unknown)
```

The `section` param on `openHostOverview` is required for correctness, not
style: the host preserves the previously open overview section when it is
omitted (`browserStore.openOverview`), so a button labeled "Open tabs"
could land on the files section. Codex passes `{ section: 'tabs' }` for
the same reason.

Tab metadata effect (the overview currently shows manifest defaults only):

```tsx
useEffect(() => {
  void updateHostTab({ title: "Ledger", status: tabStatus }).catch(
    () => undefined
  )
}, [tabStatus])
```

`tabStatus` mirrors the load state: `Loading`, `Offline`, `0 objects`, or
`12 objects / 3.4 GB`. The `.catch(() => undefined)` is load-bearing: a
bare `void` still leaves an unhandled rejection when the host is briefly
unavailable, and tab chrome must never affect the data flow. This is the
first-party pattern (the editor viewer chains the same catch).

The toaster moves its offset off the removed header:
`offset={{ top: "max(12px, env(safe-area-inset-top))", right: 12 }}`, and
its `!rounded-none`/`!shadow-none` overrides are dropped in favor of the
token radius and `--rmx-shadow-menu` for the floating surface.

## Object List

The table stack is desktop furniture: `min-w-[980px]` forces horizontal
scrolling at phone width, the max-height is hardcoded against a header that
no longer exists, and all three metadata surfaces (file, local, remote) are
HoverCards, which never fire on touch. Replace the whole stack with a
phone-first list; TanStack Table, the generic `DataTable`, and the hover
cards all go.

```text
object-table.tsx -> object-list.tsx

row (tap target, min height ~44px):
  line 1  fileName (truncate)                    size (tabular, right)
  line 2  RoleBadge  kind (truncate, muted)      local/remote glyphs (right)
  right   quick action: Download icon button when remote-only,
          spinner while hydrating, HardDrive in success tone when local

tap row -> ObjectDetailDialog (the existing Dialog component):
  DetailRows: id, sha256, role, kind, format, media type, size,
  remote bucket/key, local path, last used, created, updated
  footer: Hydrate (disabled while hydrating) and Delete
          (Delete opens the existing confirm dialog)

interaction rules:
  the row is NOT a <button>; it is a container with an onClick handler,
    so the quick-action <Button> inside it never nests interactive
    elements (invalid HTML with unpredictable tap behavior on mobile)
  the quick action calls event.stopPropagation(); a hydrate tap must
    never also open the detail dialog
  one dialog at a time: Delete inside the detail dialog closes the
    detail dialog first, then opens DeleteObjectDialog
    (setDetailTarget(null) before setDeleteTarget(object))

sorting: a compact Select in the filter block
  Updated (default, desc) | Size | Name | Kind
  replaces column-header sorting; the list is a sorted array map

filter block (top of the surface, safe-area padded):
  row 1  id-prefix search input (full width)
  row 2  kind input (flex-1) · role Select · sort Select
```

Badges drop the hand-picked sky/cyan/emerald classes for semantic tokens:

```text
role badge     raw -> text-link tone, artifact -> text-success tone
               (border/background at reduced alpha of the same token)
local glyph    text-success when local, muted when remote-only
hydrating      spinner, muted
error states   text-destructive
```

Empty, loading, and error panels survive with token styling (`rounded-lg`,
`border-border`, `bg-card/40`); the in-panel Retry button stays alongside
the always-available bar refresh.

Interaction contract carried over unchanged: hydrate optimistic in-flight
set, `remux/ledger/store/hydrated` subscription refreshing the list and
toasting, delete confirm flow, 180ms debounced filters.

## Typography

Remux ships no web fonts anywhere; type is token stacks
(`--rmx-font-sans`: Arial/Helvetica, `--rmx-font-mono`: Menlo/Consolas).
Lens follows:

```text
delete @fontsource-variable/geist (it was never imported; the "Geist Mono"
  stacks in index.css never loaded and have been silently falling back)
delete lens's --font-sans/--font-serif/--font-mono declarations
UI text     var(--rmx-font-sans) via the token default
data values font-mono (ids, hashes, sizes, timestamps, paths, R2 keys)
  mapped to var(--rmx-font-mono) with a one-line local @theme entry:
  --font-mono: var(--rmx-font-mono);
```

The mono-for-data convention keeps Ledger's data-dense identity while
matching how the editor, terminal, and code surfaces use the mono stack.

## Dead Code Removal

Files (verified: nothing outside each file imports them, or their only
consumer is removed by the list redesign):

```text
src/components/ui/calendar.tsx        only consumer of react-day-picker
src/components/ui/command.tsx         only consumer of cmdk + input-group
src/components/ui/input-group.tsx     imported only by command.tsx
src/components/ui/popover.tsx         no importers
src/components/ui/separator.tsx       no importers
src/components/ui/hover-card.tsx      replaced by the detail dialog
src/components/ui/dropdown-menu.tsx   replaced by row tap + quick action
src/components/ui/table.tsx           table stack removed
src/features/data-center/data-table.tsx
```

Surviving `components/ui`: button, dialog, input, select, sonner.

Dependencies removed from `lens/package.json`:

```text
@fontsource-variable/geist
@tanstack/react-table
cmdk
react-day-picker
```

Kept: `radix-ui` (dialog, select), `class-variance-authority` (button),
`clsx` + `tailwind-merge` (cn), `lucide-react`, `sonner`, `tw-animate-css`
(now actually imported), and the `shadcn` devDependency as the component
generator.

## Documentation Updates

```text
lens/README.md
  rewrite the styling section: viewer-kit tokens are the design system
  (never redeclare semantic vars), codex import order, the shell reset,
  safe-area conventions, ActionBar/host-tab wiring, and the list + detail
  layout. Keep the file: link section and the .remux/config.toml
  extension_roots run snippet (already applied in the working tree; the
  env var is documented as an override only)

README.md (repo root)
  the Lens section's config.toml run snippet is likewise already applied;
  this pass only touches the section if the layout rewrite invalidates
  its wording
```

## Validation

```bash
cd lens
npm install            # prunes removed deps from the lockfile
npm run build
npm run lint
```

Extension discovery now goes through Remux config, not the environment.
In `<remux>/.remux/config.toml`:

```toml
extension_roots = ["extensions", "<parent-of-ledger>"]
```

Entries are parent directories scanned for children containing
`remux-extension.json`; configured roots replace the default, so
`"extensions"` must be listed explicitly. `REMUX_EXTENSION_ROOTS` still
exists but only as an environment override of this config.

```bash
cd <remux>
npm run dev
```

Phone checklist:

```text
dark and light: flip the OS theme; lens follows without reload artifacts;
  surfaces/borders match the codex/editor look (no oklch remnants)
filter block clears the notch; ActionBar clears the home indicator
no horizontal scrolling anywhere at phone width
row tap opens the detail dialog; hydrate and delete work from it
quick hydrate from a row shows spinner, then hydrated toast + row flip
Open tabs, Reload viewer, Refresh data (busy state), Close tab all work
tab overview shows "Ledger" with the live status line
dialog animates (tw-animate-css actually loaded)
primary buttons render the Remux accent orange
```

## Tests

Lens has no unit test harness and this pass does not add one. Gates:

```text
tsc -b passes with the nine files deleted (proves nothing imported them)
eslint passes
vite build succeeds; bundle shrinks (tanstack + react-day-picker + cmdk
  are out)
manual phone validation per the checklist
```

## Success Criteria

This pass is complete when:

```text
index.css contains no color values of its own — only imports, the base
  reset, the dark custom-variant, and viewer-local structure styles
theme flipping is driven entirely by tokens.css via data-remux-theme
the viewport meta includes viewport-fit=cover and safe areas hold on iOS
the object list is single-column at phone width with tap-to-detail
hover-card, dropdown-menu, table, data-table, calendar, command,
  input-group, popover, separator are deleted
@fontsource-variable/geist, @tanstack/react-table, cmdk, react-day-picker
  are out of package.json and the lockfile
the standard bottom row and tab metadata work on the phone
Open tabs lands on the tabs overview section explicitly
both READMEs describe the .remux/config.toml discovery workflow
npm run build and npm run lint pass
no ledger-remux or crates/* changes in the diff
```

## Future Extensions

Left for later passes:

```text
viewer-kit Sheet for a bottom-drawer detail surface if the dialog feels
  heavy on the phone
subscribeHostNavigate + launcher route metadata when sessions/charts add
  a second surface
ActionMenu in the bottom row once surface-level actions outgrow four
  buttons
per-object push notifications via remux/notifications/* for long hydrates
remux web host layout (the list already degrades gracefully to wide
  viewports, but a desktop density pass is deferred)
```
