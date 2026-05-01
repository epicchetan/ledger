# ledger-replay

`ledger-replay` owns headless replay and simulated execution over a prepared
`ledger-domain::EventStore`.

The crate separates exchange truth from trader visibility and simulated order
arrival. It consumes already-prepared artifacts; it does not find, download, or
cache sessions.

## Owns

- Stepping exchange batches through `ledger-book::OrderBook`.
- Simulated order submission, arrival, cancellation, and fills.
- Conservative same-timestamp ordering between exchange events and simulated
  order arrivals.
- Visibility frames delayed/coalesced from exchange truth.
- Replay reports containing fills, emitted frames, cursor position, and final
  book checksum.

## Must Not Own

- Databento download or DBN preprocessing.
- R2, SQLite, filesystem staging, or session loading.
- CLI command surfaces.
- Durable artifact format definitions beyond consuming `EventStore`.

## Main Public Concepts

- `ReplaySimulator` coordinates exchange batches, execution, and visibility.
- `ExecutionSimulator` models order-entry latency, cancel latency, marketable
  fills, passive queue-ahead, and cancel/fill races.
- `VisibilityModel` emits delayed/coalesced BBO, depth, and trade frames.
- `ReplaySimReport` snapshots replay progress and emitted results after a run.

## Testing Expectations

Tests should use synthetic `EventStore` values and avoid external files. Cover
market-order fills at arrival time, passive queue-ahead behavior, cancel/fill
races, visibility delay/coalescing, seek behavior, and same-timestamp policy.
