# ledger-api

`ledger-api` is the HTTP transport adapter for Ledger and Lens.

It should stay thin: route parsing, response serialization, job tracking, and
local development CORS live here. Market-day lifecycle behavior, replay dataset
staging, validation composition, ingest orchestration, and later active
replay-session ownership belong in `ledger`.

## Routes

```text
GET  /health
GET  /market-days
GET  /market-days/:symbol/:date
GET  /market-days/:symbol/:date?verify=true
GET  /market-days/:symbol/:date/jobs
POST /market-days/:symbol/:date/prepare
POST /market-days/:symbol/:date/replay/build
POST /market-days/:symbol/:date/replay/validate
DELETE /market-days/:symbol/:date/replay
DELETE /market-days/:symbol/:date/replay/cache
DELETE /market-days/:symbol/:date/raw
GET  /jobs?active=true
GET  /jobs?active=false&limit=50
GET  /jobs/:id
```

Market-day status is cheap by default. It reports durable Layer 1 raw data,
durable Layer 2 replay dataset state, artifact metadata, and latest validation
summary from SQLite. Use `verify=true` when a caller explicitly wants R2 object
metadata checked.

Long-running lifecycle operations return a job immediately. Lens polls
`GET /jobs?active=true` and `GET /jobs/:id` for active progress and terminal
status. `GET /jobs?active=false&limit=50` returns recent job history, and
`GET /market-days/:symbol/:date/jobs` returns history for one MarketDay. Jobs
are persisted in SQLite so API restarts do not leave Lens with invisible
in-memory state. Market-day jobs include `market_day_id` and a `target` object
so Lens can overlay active job state onto the exact table row without guessing
from display text. `prepare` runs the normal one-click path: ensure raw DBN is
durable in R2, build replay artifacts when needed, run light readiness checks,
and leave a ReplayDataset with current validation status.
`replay/build` forces the Layer 2 rebuild path while preserving Layer 1 raw
data. `replay/validate` is the heavier audit path.
`replay/cache` removes only local cached replay artifacts; it does not touch R2
or durable catalog records.

## Development

```bash
cargo run -p ledger-api
```

The default bind address is `127.0.0.1:3001` and can be overridden with
`LEDGER_API_ADDR`.
