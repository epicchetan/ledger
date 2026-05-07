# Ledger Lens

Vite frontend for Ledger.

The default screen is the API-backed Data Center object explorer:

```text
store objects -> metadata -> exact object delete
```

Set `VITE_LEDGER_API_URL` to point Lens at a non-default API address. The
default is `http://127.0.0.1:3001`.

Data Center does not prepare data, run validation jobs, open sessions, or
manage chart sessions. Those surfaces were removed while `store`, feeds, and
the new runtime are being built.

Reusable charting code is still kept under `src/components/` and the prototype
area for future reference.

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
