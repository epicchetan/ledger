# Ledger Lens

Vite frontend for Ledger.

The default screen is the API-backed Data Center surface:

```text
MarketDay -> Layer 1 Raw Data -> Layer 2 ReplayDataset -> ReplaySession
```

Set `VITE_LEDGER_API_URL` to point Lens at a non-default API address. The
default is `http://127.0.0.1:3001`.

Data Center uses the job-backed API lifecycle:

```text
Prepare MarketDay -> poll job -> durable raw + replay layers
Build/Rebuild ReplayDataset -> poll job -> validation status updates
Validate ReplayDataset -> poll job
Delete ReplayDataset or Raw Data -> poll job
```

Normal status reads are intentionally cheap; expensive verification runs inside
explicit validation jobs. The table is full-width by design: rows show durable
Layer 1 and Layer 2 state, and hover cards expose object keys, sizes, hashes,
and validation details without keeping a separate detail panel open.

The previous replay/chart prototype is preserved under
`src/features/replay-prototype/` while the Data Center becomes the entry point.

## Development

Run frontend commands from this directory:

```bash
npm run dev
```

Useful checks:

```bash
npm run typecheck
npm run lint
npm run build
```

The app stays self-contained under `lens/`; Rust crates and API work live under
the repository root and `crates/`.
