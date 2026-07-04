# Remux Extension Implementation Spec

## Purpose

This spec converts the Ledger repository into an out-of-tree Remux extension.

Ledger stays a standalone repository and Rust workspace. It gains a Remux
extension surface:

```text
remux-extension.json   manifest at the repo root
lens                   the extension viewer (static web app)
crates/remux           stdio JSON-RPC server over ledger crates
```

Nothing moves into the Remux repository. Remux discovers the ledger repo
through the `extension_roots` list in `<remux>/.remux/config.toml`
(`REMUX_EXTENSION_ROOTS` remains an environment override).

The important boundary:

```text
remux runtime
  owns process supervision
  owns viewer asset serving
  owns the websocket fan-out to clients
  owns the phone app

ledger-remux (crates/remux)
  stdio JSON-RPC transport adapter
  no store orchestration logic
  no session/replay logic

lens
  viewer UI
  talks to ledger only through remux IPC

store
  remains the shared operation layer
  gains one narrow hydrate fast-path helper (see Hydrate Jobs)
```

`ledger-remux` is the third thin transport over `store`, next to `ledger-cli`
and `ledger-api`. When the future `ledger` session crate exists, this same
binary grows `remux/ledger/session/*` methods over it. The transport pattern
established here is the one every later feature reuses.

## Motivation

Two goals:

```text
1. Give Ledger a permanent operating surface on the phone through Remux.
2. Settle the transport architecture before the session/app layer is designed.
```

The read/stream surface of the future session layer should be designed for the
transport it will actually serve. That transport is Remux stdio JSON-RPC with
notification broadcast, not the axum WebSocket the old README describes.

This phase deliberately ships only the Data Center capability set. Charts,
sessions, and playback require the `ledger` crate
(`docs/ledger_feed_system_implementation_spec.md`) and are not part of this
conversion.

## Scope

In scope:

```text
remux-extension.json manifest at the repo root
raster icon asset
crates/remux: ledger-remux stdio JSON-RPC binary over store
store: public Store::touch_valid_local_copy helper
async hydrate with completion broadcast notification
lens conversion to a Remux viewer:
  @remux/viewer-kit adoption (file: link)
  IPC data layer replacing HTTP fetch
  vite base path, react dedupe, tailwind @source
  host-driven theming
  removal of the dead replay prototype code
headless stdio smoke validation
dev workflow documentation
README updates for the surfaces this change invalidates
  (lens/README.md, root README Lens section)
```

Out of scope:

```text
sessions, feeds, playback, charts (ledger crate spec)
retiring crates/api (later phase; it stays untouched and working)
publishing @remux/viewer-kit (file: link this phase)
push notifications through remux/notifications/*
remux web/laptop host support
authentication (Tailscale remains the boundary)
multi-view manifests
streaming any cache/runtime state
```

## Remux Protocol Contract

Verified against the Remux runtime source
(`cli/extensionProcess.cjs`, `cli/extensionRegistry.cjs`,
`cli/extensionManifest.cjs`, `docs/guides/extension-authoring.md`).
`ledger-remux` must honor all of it.

Discovery:

```text
extension_roots in <remux>/.remux/config.toml is a list of PARENT
  directories; relative entries resolve from the remux checkout root.
Each parent is scanned for subdirectories containing remux-extension.json.
Configured roots REPLACE the default <remux>/extensions root, so
  "extensions" must stay listed.
REMUX_EXTENSION_ROOTS (delimiter-separated) overrides the config when set.
Manifest paths (entry, icon, server cwd) resolve relative to the manifest dir.
```

Transport:

```text
newline-delimited JSON-RPC 2.0 over stdin/stdout
one JSON object per line, no framing headers

runtime -> server (stdin):
  requests      {"jsonrpc":"2.0","id":<n>,"method":"remux/ledger/...","params":{...}}
  notifications {"jsonrpc":"2.0","method":"...","params":{...}}   (no id)

server -> runtime (stdout):
  responses     {"jsonrpc":"2.0","id":<n>,"result":<value>}
                {"jsonrpc":"2.0","id":<n>,"error":{"code":<n>,"message":"...","data":...}}
  notifications {"jsonrpc":"2.0","method":"...","params":{...}}   (no id)
```

Rules:

```text
method names arrive UNSTRIPPED (full remux/ledger/... prefix)
stdout is protocol-only; invalid lines are dropped with a runtime warning
stderr is the log channel and is surfaced in remux runtime logs
runtime request timeout is 300 seconds
server-initiated id-less lines are broadcast to all connected clients;
  viewers receive them through subscribeIpcEvents
methods starting with remux/notifications/ are reserved for the push
  notification pipeline; do not use that prefix for data streaming
inbound id-less notifications (viewer notifyIpc) may arrive; unknown ones
  are ignored without a response
```

Lifecycle:

```text
stop is SIGTERM; the server should exit 0 promptly
a non-zero exit or signal exit while not stopping is RUNTIME-FATAL:
  remux kills the entire runtime, every extension, every tab
a spontaneous clean exit(0) is not fatal but leaves the extension stopped
therefore: the server never exits non-zero, never panics past a request
  boundary, and reports all failures as JSON-RPC error responses
```

Manifest constraints:

```text
version must be 1
views.main with entry is required
display icons must be raster (png/jpg/webp); svg is rejected
views.<id>.dev is rejected; viewers are always built assets
server.transport must be "stdio"
```

## Extension Layout

New files in the ledger repo:

```text
remux-extension.json
assets/remux/ledger.png          raster icon (dark variant optional:
assets/remux/ledger-dark.png)
crates/remux/                    ledger-remux crate
```

Manifest:

```json
{
  "version": 1,
  "id": "ledger",
  "name": "Ledger",
  "display": {
    "title": "Ledger",
    "icon": "assets/remux/ledger.png"
  },
  "server": {
    "transport": "stdio",
    "command": "cargo",
    "args": ["run", "--quiet", "-p", "ledger-remux", "--"],
    "cwd": "."
  },
  "views": {
    "main": {
      "route": "/viewers/ledger",
      "entry": "lens/dist/index.html"
    }
  },
  "launchers": [
    {
      "id": "data-center",
      "view": "main",
      "label": "Ledger"
    }
  ],
  "fileHandlers": []
}
```

Notes:

```text
cwd "." is the ledger repo root, so dotenvy finds .env and LEDGER_DATA_DIR
  defaults resolve exactly as they do for ledger-cli.
cargo run compiles on first launch after a change; cargo writes build
  diagnostics to stderr, so the protocol channel stays clean. --quiet keeps
  stderr noise down. A dedicated --target-dir and --offline (the codex
  pattern) are optional hardening, not required.
lens/dist is the built viewer output; remux serves the directory containing
  the entry file with SPA fallback under /viewers/ledger.
```

Runtime launch:

```text
# <remux>/.remux/config.toml
extension_roots = ["extensions", "<parent-of-ledger>"]

npm run dev
```

The ledger repo root is itself the extension directory; its parent is the
scan root. The remux extensions dir must be listed explicitly because
configured roots replace the default.

## Crate: crates/remux

Package and binary:

```text
package: ledger-remux
binary:  ledger-remux
```

Workspace update:

```toml
[workspace]
members = [
    "crates/cache",
    "crates/runtime",
    "crates/store",
    "crates/api",
    "crates/cli",
    "crates/remux",
]
```

Dependencies:

```toml
[dependencies]
anyhow.workspace = true
chrono.workspace = true
dotenvy.workspace = true
serde.workspace = true
serde_json.workspace = true
store = { path = "../store" }
thiserror.workspace = true
tokio.workspace = true

[dev-dependencies]
tempfile.workspace = true
```

Module layout:

```text
crates/remux/src/
  main.rs        env init, store construction, loop wiring
  rpc.rs         line framing, request/response/notification types, dispatch
  methods.rs     store method handlers and DTO mapping
  hydrate.rs     async hydrate job tracking
  error.rs       RpcError codes and conversions
```

### Runtime shape

```text
stdin reader task
  reads lines, parses JSON-RPC messages
  requests are dispatched as spawned tokio tasks
  id-less inbound notifications are ignored in this phase

writer task
  single owner of stdout
  receives outbound messages over an mpsc channel
  writes one serialized JSON object per line
  this is the only code that writes to stdout

request handling
  each request runs in its own tokio task
  a JoinError (handler panic) becomes a JSON-RPC error response
  the process survives every handler failure

shutdown
  SIGTERM handler triggers graceful exit(0)
  stdin EOF also triggers exit(0)
```

The single-writer channel guarantees line atomicity. The per-request task
spawn keeps slow requests (hydrate lookups, R2 head calls) from blocking the
read loop, and converts panics into error responses instead of process death.

### Method surface

All methods take and return camelCase JSON. This is a deliberate divergence
from `ledger-api`, whose DTOs are snake_case and force Lens to hand-map field
names (`lens/src/features/data-center/api.ts`). Requirements:

```text
every Remux DTO derives #[serde(rename_all = "camelCase")]
every result is a wrapped object ({ "objects": [...] }, never a bare array)
lens deletes only its snake_case -> camelCase field-name mapping; it keeps
  a small DTO -> view-model formatter for derived UI fields like size and
  display timestamps after unwrapping result.objects / result.object
```

Params mirror the existing CLI flags; results mirror the store report types.

```text
remux/ledger/ping
  params: none
  result: { "ok": true, "service": "ledger-remux", "version": "0.1.0" }

remux/ledger/store/list
  params: { "role"?: "raw"|"artifact", "kind"?: string, "idPrefix"?: string }
  result: { "objects": [StoreObjectDto] }

remux/ledger/store/get
  params: { "id": string }
  result: { "object": StoreObjectDto }
  error:  objectNotFound

remux/ledger/store/delete
  params: { "id": string }
  result: DeleteReportDto
  error:  objectNotFound when nothing was removed

remux/ledger/store/hydrate
  params: { "id": string }
  result: { "started": true } | { "started": false, "alreadyLocal": true }
  behavior: async; see Hydrate Jobs

remux/ledger/store/localStatus
  params: none
  result: LocalStatusDto (object count, bytes used, limit)
```

Error mapping:

```text
-32601  method not found
-32602  invalid params (missing/expected fields, bad object id)
-32000  domain errors (store failures, not found, hydrate in flight)
```

Domain error responses carry `message` and, where useful, a `data` object
with the object id.

DTO policy: `methods.rs` owns its DTO structs, adapted from
`crates/api/src/dto.rs` (explicit fields, ISO timestamps alongside ns; the
ISO formatting follows `crates/api/src/time.rs`, which is why `chrono` is a
dependency). Do not share DTO code with `ledger-api` in this phase; the api
crate is scheduled for retirement, and a premature shared DTO crate is the
wrong app layer. Orchestration stays in `store`, exactly like api and cli.

### Hydrate Jobs

Hydration of a raw day can exceed the 300 second remux request timeout, so
`store/hydrate` must not answer with the finished result.

The already-local fast path needs behavior that `Store` performs internally
during `hydrate` but does not expose. Add one narrow helper to `crates/store`:

```rust
impl<S: RemoteStore + 'static> Store<S> {
    pub fn touch_valid_local_copy(&self, id: &StoreObjectId) -> Result<bool>;
}
```

Behavior mirrors the local fast path inside `Store::hydrate`:

```text
unknown id                      -> Err (same not-found error as hydrate)
descriptor has no local entry   -> Ok(false)
local entry present             -> local.absolute_from_root + local_path_valid
                                   (exists + size match + full sha256 match)
valid local copy                -> refresh last_accessed_at_ns, refresh the
                                   LocalObjectEntry with local_entry, persist
                                   the descriptor, return Ok(true)
invalid local copy              -> Ok(false), leaving remote hydrate to the
                                   caller
```

The sha256 pass takes seconds on multi-GB files; that is acceptable because
Remux request handlers run in spawned tasks and never block the read loop.
No other `store` changes are in scope. This helper is intentionally not a
read-only predicate: skipping the access update would make phone-triggered
hydrate behave differently from `Store::hydrate` and could distort local LRU
pruning.

```text
1. validate the object id and descriptor exists
2. if store.touch_valid_local_copy(&id)? reply { started: false, alreadyLocal: true }
3. if a hydrate for this id is already in flight, reply -32000 "hydrate in flight"
4. otherwise record the id in an in-memory job set, reply { started: true }
5. run store.hydrate(id) in a spawned task
6. on completion (success or failure) remove the job entry and broadcast:

   {"jsonrpc":"2.0","method":"remux/ledger/store/hydrated",
    "params":{"id":"sha256-...","ok":true}}

   {"jsonrpc":"2.0","method":"remux/ledger/store/hydrated",
    "params":{"id":"sha256-...","ok":false,"error":"..."}}
```

The job set is in-memory only. If the server restarts mid-hydrate, the
partial download is re-validated or redone by the next hydrate call; `store`
already verifies sha256 on hydrate.

This notification is deliberately the first use of the broadcast path. It is
the same mechanism session frames will use later, proven on a trivial payload.

## Lens Conversion

Lens stays in `lens/` and becomes the extension viewer.

### Dependencies and config

```text
package.json
  add: "@remux/viewer-kit": "file:../../remux/packages/viewer-kit"
  the path assumes ledger and remux are sibling checkouts; document this in
  the README. npm installs file: directory deps as symlinks, which matches
  how in-tree remux viewers consume viewer-kit (workspace symlink, compiled
  from TypeScript source by vite).

vite.config.ts
  base: '/viewers/ledger/'
  resolve.dedupe: ['react', 'react-dom']
    (the symlink resolves into the remux repo, which has its own react copy;
     without dedupe the viewer loads two reacts and hooks fail)

tailwind (v4)
  add @source pointing at the viewer-kit package so its component classes
  are scanned; automatic content detection skips node_modules

tsconfig
  tsc -b will typecheck viewer-kit source under lens flags; keep
  moduleResolution "bundler" and jsx "react-jsx" aligned with remux viewers
```

### Entry

`src/main.tsx` adopts the standard remux viewer mount (the terminal viewer is
the reference):

```tsx
import { initializeIpc } from '@remux/viewer-kit/ipc';
import { mountViewer } from '@remux/viewer-kit/react';
import '@remux/viewer-kit/tokens.css';
import '@remux/viewer-kit/ui/styles.css';

import App from './App';

mountViewer(<App />, { name: 'ledger', initialize: initializeIpc });
```

Passing `initialize: initializeIpc` is not optional plumbing: `initializeIpc`
is what posts the `remux/ready` handshake, and the mobile host reloads a
viewer that does not report ready within 8 seconds. `mountViewer` without it
renders in a browser tab but fails in the app.

### Theming

The host stamps `data-remux-theme` on `<html>` before first paint.

```text
dark values live in :root
light overrides live under :root[data-remux-theme="light"]
```

Lens's existing theme handling is replaced by this convention.

### Data layer

`src/features/data-center/api.ts` switches from fetch to IPC:

```text
fetchStoreObjects  -> requestIpc('remux/ledger/store/list', filters)
deleteStoreObject  -> requestIpc('remux/ledger/store/delete', { id })
new: hydrate       -> requestIpc('remux/ledger/store/hydrate', { id })
new: localStatus   -> requestIpc('remux/ledger/store/localStatus')
new: subscribeIpcEvents for remux/ledger/store/hydrated
     refresh the affected row and surface success/failure in the toaster
```

`VITE_LEDGER_API_URL` and the HTTP client timeout logic are removed with the
fetch layer, and so is the snake_case-to-camelCase field mapping. Keep a small
view-model formatter, though: the existing UI types include derived display
fields (`size`, formatted timestamps) that are not wire DTO fields.

Data Center UI additions stay minimal: a hydrate action per object row with
an in-flight indicator driven by the hydrated event, and a small local-store
status line. No new pages.

### Dead code removal

Delete the orphaned pre-refactor prototype in the same change:

```text
src/features/replay-prototype/
src/hooks/use-replay.ts
src/lib/ws-types.ts
src/lib/api.ts            (stale hardcoded base URL, unused by live code)
src/components/top-bar.tsx (calls the stale api; unreachable from App)
chart/volume-profile/dom primitives that exist only for the prototype
```

Git history preserves them. The future chart surface will be written against
real session frames, not the dead `bars:*` websocket protocol.

## Documentation Updates

This conversion invalidates existing docs; updating them is in scope.

```text
lens/README.md
  currently describes an API-backed dev flow with VITE_LEDGER_API_URL
  rewrite for the viewer model: viewer-kit file: link, build output,
  IPC data layer, remux dev workflow

README.md (repo root)
  the Lens section currently says Lens talks to ledger-api over HTTP
  update the Lens/CLI sections to the remux extension surface and point at
  this spec
```

The root README's broader staleness (pre-refactor Session/WebSocket/projection
claims) is a separate cleanup and stays out of scope; only the sections this
change invalidates are updated here.

## Validation

Headless first, in keeping with CLI-before-UI:

```bash
# liveness + list over real R2 config
printf '%s\n' \
  '{"jsonrpc":"2.0","id":1,"method":"remux/ledger/ping"}' \
  '{"jsonrpc":"2.0","id":2,"method":"remux/ledger/store/list","params":{"role":"raw"}}' \
  | cargo run --quiet -p ledger-remux
```

Then through remux:

```bash
cd lens && npm install && npm run build
# add <parent-of-ledger> to extension_roots in <remux>/.remux/config.toml
cd <remux> && npm run dev
curl http://127.0.0.1:48123/remux/extensions     # catalog lists "ledger"
# open the Ledger tile in the phone app over Tailscale
```

## Tests

`ledger-remux` unit tests:

```text
rpc parses requests, notifications, and rejects invalid lines
responses and errors serialize to single protocol lines
unknown method returns -32601
missing/invalid params return -32602
invalid object id returns -32602 with the id in data
```

`store` unit tests for the new helper:

```text
touch_valid_local_copy returns true for a valid local copy
touch_valid_local_copy refreshes last_accessed_at_ns for a valid local copy
touch_valid_local_copy returns false when the local file is missing
touch_valid_local_copy returns false on size or sha256 mismatch
touch_valid_local_copy returns false when the descriptor has no local entry
touch_valid_local_copy errors on an unknown object id
```

Handler tests (using the `TestRemote` pattern from `crates/store/tests`):

```text
store/list maps filters and returns DTOs
store/get returns objectNotFound for unknown id
store/delete reports removal
store/hydrate on an already-local object returns alreadyLocal
store/hydrate broadcasts hydrated { ok: true } on completion
store/hydrate broadcasts hydrated { ok: false, error } on failure
concurrent hydrate for the same id is rejected while in flight
a panicking handler yields an error response and the loop survives
```

Process-level test:

```text
spawn the binary, write request lines to stdin, assert response lines,
send SIGTERM (or close stdin), assert exit code 0
```

Workspace validation:

```text
cargo fmt --all
cargo test --workspace
cd lens && npm run build && npm run lint
```

## Success Criteria

This phase is complete when:

```text
remux catalog lists the ledger extension from an out-of-tree root
the Ledger tile opens on the phone and Data Center renders
list, filter, and delete work over remux IPC
hydrate starts from the phone and completes with a broadcast notification
local store status is visible in the viewer
ledger-remux never exits non-zero on request failures
SIGTERM and stdin EOF produce clean exit(0)
stdout carries protocol lines only; logs go to stderr
Remux DTOs serialize camelCase; lens keeps formatting but no snake_case mapping
the dead lens replay prototype code is removed
lens/README.md and the root README Lens section describe the remux path
crates/api and crates/cli are untouched and still work
cargo test --workspace passes
lens builds clean
```

## Future Extensions

Left for later specs:

```text
remux/ledger/session/* methods over the future ledger session crate
frame streaming as broadcast notifications (bars first)
push notifications via remux/notifications/* for long job completion
retiring crates/api once the remux path is the only Lens transport
publishing @remux/viewer-kit as a built package (tsup, d.ts, dist exports)
remux web host for laptop use
file handlers (e.g. opening a .dbn.zst from remux file browsing)
import/upload of new raw days from the viewer
```
