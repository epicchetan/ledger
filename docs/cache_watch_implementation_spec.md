# Cache Watch Implementation Spec

## Purpose

Add one generic capability to `crates/cache`: per-cell change watches. A
task can park until a cell changes; applying any mutating operation to that
cell wakes it. This is the wakeup primitive that lets runtime processes
(feeds), drivers, and future transports react to cell changes without
polling and without bespoke side channels.

This is a base-crate capability, not domain machinery:

```text
cache owns   how a parked task wakes when a cell changes
runtime      UNCHANGED — the worker already applies writes through cache
             mutators, so notification happens as a side effect of every
             write no matter who submitted it
ledger       decides which cells to watch and what to do on wake
```

## API

```rust
impl Cache {
    /// Watch a registered cell for changes. Errors on an unregistered key.
    /// Follows the same visibility rules as reads.
    pub fn watch_key(&self, key: &Key) -> Result<CellWatch, CacheError>;
}

pub struct CellWatch {
    rx: tokio::sync::watch::Receiver<u64>,
}

impl CellWatch {
    /// Parks the task until the cell's generation advances past the last
    /// value this watch observed. Returns immediately if it already has.
    /// Cancel-safe inside tokio::select!.
    pub async fn changed(&mut self) -> Result<(), CacheError>;

    /// The generation this watch last observed.
    pub fn generation(&self) -> u64;
}
```

`CellWatch` is cheap to clone-from-source (call `watch_key` again) and
carries no payload. Watches say "something changed"; the watcher reads the
cell to learn what. That keeps the primitive generic — it works identically
for a clock cell, a cursor cell, or any future cell.

## Semantics

```text
generation      each registered cell carries a u64 generation, starting at 0

bump-on-write   every mutating operation bumps the generation after the
                mutation is applied:
                  set_value, clear_value, update_value,
                  replace_array, push_array, insert_array,
                  replace_array_range, remove_array_range, clear_array,
                  update_array

wake-on-write   writers never know who is listening; the cache wakes parked
                watchers as part of applying the write

never-miss      if the generation advanced while a watcher was busy, its
                next changed() returns immediately instead of parking

coalescing      N rapid writes collapse into one wakeup; the watcher
                re-reads the latest state (correct for clock-style cells:
                only the latest value matters; correct for arrays:
                consumers re-read from their own progress cursor)

no payload      watches carry the generation only, never cell data

cost            one tokio::sync::watch::Sender<u64> per registered cell;
                a write to an unwatched cell costs an atomic store
```

Rejected alternative: a single cache-global generation ("any write
happened"). One channel, but in a busy session a feed would wake on its own
emissions and every other writer's activity. Per-key watches are barely
more code and wake exactly the right tasks.

## Implementation Notes

- The generation lives inside the `watch::Sender<u64>` itself — the channel
  is the counter. Bumping is `send_modify(|g| *g += 1)` after the mutation.
- Every mutating path in `cache.rs` already funnels through the cell
  record; add the bump there so no operation can forget it. This includes
  writes applied by the runtime worker and prepare-context writes — they
  all use the same mutators.
- `changed()` maps the underlying channel-closed error (cache dropped) into
  a `CacheError` variant.
- Ownership is unchanged: watching requires no ownership; writing still
  does. Visibility of watches follows the same policy as reads.

## Tests

```text
watch_key on an unregistered key errors
changed() parks until a set_value on the watched cell
every mutating op bumps the generation (value ops and each array op)
a write that lands while the watcher is busy makes the next changed()
  return immediately (never-miss)
multiple rapid writes coalesce into one wakeup with the latest state
  readable afterward
watches on different keys wake independently
watcher outlives a burst of writes without missing that a change happened
```

## Success Criteria

```text
Cache::watch_key and CellWatch exist and are exercised by tests
every mutating operation bumps the watched cell's generation
no polling primitive, no payload on the channel, no domain knowledge
runtime crate is untouched
cargo test --workspace passes
```
