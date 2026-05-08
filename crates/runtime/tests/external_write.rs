use cache::{ArrayKey, Cache, CacheError, CellDescriptor, CellKind, CellOwner, Key, ValueKey};
use runtime::{ExternalWriteBatch, Runtime, RuntimeError};

#[derive(Clone, Debug, PartialEq, Eq)]
struct Status {
    playing: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct Batch {
    seq: u64,
}

fn key(value: &str) -> Key {
    Key::new(value).unwrap()
}

fn feed_owner() -> CellOwner {
    CellOwner::new("feed.databento.es_mbo").unwrap()
}

fn projection_owner() -> CellOwner {
    CellOwner::new("projection.candles_1m").unwrap()
}

fn other_owner() -> CellOwner {
    CellOwner::new("test.other").unwrap()
}

fn descriptor(key_path: &str, owner: CellOwner, kind: CellKind) -> CellDescriptor {
    CellDescriptor {
        key: key(key_path),
        owner,
        kind,
        public_read: false,
    }
}

fn status_cell(cache: &Cache, owner: CellOwner) -> ValueKey<Status> {
    cache
        .register_value(
            descriptor("feed.databento.es_mbo.status", owner, CellKind::Value),
            None,
        )
        .unwrap()
}

fn batches_cell(cache: &Cache, owner: CellOwner) -> ArrayKey<Batch> {
    cache
        .register_array(
            descriptor("feed.databento.es_mbo.batches", owner, CellKind::Array),
            Vec::new(),
        )
        .unwrap()
}

fn runtime_with_cache(cache: &Cache) -> Runtime {
    Runtime::new(cache.clone())
}

#[test]
fn external_batch_set_value_writes_through_cache() {
    let cache = Cache::new();
    let owner = feed_owner();
    let status = status_cell(&cache, owner.clone());
    let mut runtime = runtime_with_cache(&cache);
    let mut batch = ExternalWriteBatch::new(owner);
    batch.set_value(&status, Status { playing: true });

    runtime.submit_external_writes(batch);
    let step = runtime.run_once().unwrap();

    assert_eq!(
        cache.read_value(&status).unwrap(),
        Some(Status { playing: true })
    );
    assert_eq!(step.changed_keys, vec![key("feed.databento.es_mbo.status")]);
    assert!(step.idle_after);
}

#[test]
fn external_batch_clear_value_clears_through_cache() {
    let cache = Cache::new();
    let owner = feed_owner();
    let status = status_cell(&cache, owner.clone());
    cache
        .set_value(&owner, &status, Status { playing: true })
        .unwrap();
    let mut runtime = runtime_with_cache(&cache);
    let mut batch = ExternalWriteBatch::new(owner);
    batch.clear_value(&status);

    runtime.submit_external_writes(batch);
    runtime.run_once().unwrap();

    assert_eq!(cache.read_value(&status).unwrap(), None);
}

#[test]
fn external_batch_push_array_preserves_item_order() {
    let cache = Cache::new();
    let owner = feed_owner();
    let batches = batches_cell(&cache, owner.clone());
    let mut runtime = runtime_with_cache(&cache);
    let mut batch = ExternalWriteBatch::new(owner);
    batch.push_array(&batches, vec![Batch { seq: 1 }, Batch { seq: 2 }]);

    runtime.submit_external_writes(batch);
    runtime.run_once().unwrap();

    assert_eq!(
        cache.read_array(&batches).unwrap(),
        vec![Batch { seq: 1 }, Batch { seq: 2 }]
    );
}

#[test]
fn external_batch_replace_array_replaces_existing_items() {
    let cache = Cache::new();
    let owner = feed_owner();
    let batches = batches_cell(&cache, owner.clone());
    cache
        .push_array(&owner, &batches, vec![Batch { seq: 1 }])
        .unwrap();
    let mut runtime = runtime_with_cache(&cache);
    let mut batch = ExternalWriteBatch::new(owner);
    batch.replace_array(&batches, vec![Batch { seq: 10 }]);

    runtime.submit_external_writes(batch);
    runtime.run_once().unwrap();

    assert_eq!(cache.read_array(&batches).unwrap(), vec![Batch { seq: 10 }]);
}

#[test]
fn external_batch_insert_array_validates_bounds_through_cache() {
    let cache = Cache::new();
    let owner = feed_owner();
    let batches = batches_cell(&cache, owner.clone());
    let mut runtime = runtime_with_cache(&cache);
    let mut batch = ExternalWriteBatch::new(owner);
    batch.insert_array(&batches, 1, vec![Batch { seq: 1 }]);

    runtime.submit_external_writes(batch);
    let err = runtime.run_once().unwrap_err();

    assert_eq!(
        err,
        RuntimeError::Cache(CacheError::ArrayRangeOutOfBounds {
            key: key("feed.databento.es_mbo.batches"),
        })
    );
    assert!(cache.read_array(&batches).unwrap().is_empty());
}

#[test]
fn external_batch_replace_array_range_validates_bounds_through_cache() {
    let cache = Cache::new();
    let owner = feed_owner();
    let batches = batches_cell(&cache, owner.clone());
    let mut runtime = runtime_with_cache(&cache);
    let mut batch = ExternalWriteBatch::new(owner);
    batch.replace_array_range(&batches, 0..1, vec![Batch { seq: 1 }]);

    runtime.submit_external_writes(batch);
    let err = runtime.run_once().unwrap_err();

    assert_eq!(
        err,
        RuntimeError::Cache(CacheError::ArrayRangeOutOfBounds {
            key: key("feed.databento.es_mbo.batches"),
        })
    );
}

#[test]
fn external_batch_remove_array_range_validates_bounds_through_cache() {
    let cache = Cache::new();
    let owner = feed_owner();
    let batches = batches_cell(&cache, owner.clone());
    let mut runtime = runtime_with_cache(&cache);
    let mut batch = ExternalWriteBatch::new(owner);
    batch.remove_array_range(&batches, 0..1);

    runtime.submit_external_writes(batch);
    let err = runtime.run_once().unwrap_err();

    assert_eq!(
        err,
        RuntimeError::Cache(CacheError::ArrayRangeOutOfBounds {
            key: key("feed.databento.es_mbo.batches"),
        })
    );
}

#[test]
fn external_batch_clear_array_clears_through_cache() {
    let cache = Cache::new();
    let owner = feed_owner();
    let batches = batches_cell(&cache, owner.clone());
    cache
        .push_array(&owner, &batches, vec![Batch { seq: 1 }])
        .unwrap();
    let mut runtime = runtime_with_cache(&cache);
    let mut batch = ExternalWriteBatch::new(owner);
    batch.clear_array(&batches);

    runtime.submit_external_writes(batch);
    runtime.run_once().unwrap();

    assert!(cache.read_array(&batches).unwrap().is_empty());
}

#[test]
fn external_batch_records_each_changed_key_once_and_preserves_operation_order() {
    let cache = Cache::new();
    let owner = feed_owner();
    let status = status_cell(&cache, owner.clone());
    let batches = batches_cell(&cache, owner.clone());
    let mut runtime = runtime_with_cache(&cache);
    let mut batch = ExternalWriteBatch::new(owner);
    batch
        .set_value(&status, Status { playing: false })
        .set_value(&status, Status { playing: true })
        .push_array(&batches, vec![Batch { seq: 1 }])
        .push_array(&batches, vec![Batch { seq: 2 }]);

    runtime.submit_external_writes(batch);
    let step = runtime.run_once().unwrap();

    assert_eq!(
        cache.read_value(&status).unwrap(),
        Some(Status { playing: true })
    );
    assert_eq!(
        cache.read_array(&batches).unwrap(),
        vec![Batch { seq: 1 }, Batch { seq: 2 }]
    );
    assert_eq!(
        step.changed_keys,
        vec![
            key("feed.databento.es_mbo.status"),
            key("feed.databento.es_mbo.batches")
        ]
    );
}

#[test]
fn external_batch_fails_on_owner_mismatch() {
    let cache = Cache::new();
    let owner = feed_owner();
    let status = status_cell(&cache, owner.clone());
    let mut runtime = runtime_with_cache(&cache);
    let wrong_owner = other_owner();
    let mut batch = ExternalWriteBatch::new(wrong_owner.clone());
    batch.set_value(&status, Status { playing: true });

    runtime.submit_external_writes(batch);
    let err = runtime.run_once().unwrap_err();

    assert_eq!(
        err,
        RuntimeError::Cache(CacheError::OwnerMismatch {
            key: key("feed.databento.es_mbo.status"),
            writer: wrong_owner,
            owner,
        })
    );
    assert_eq!(cache.read_value(&status).unwrap(), None);
}

#[test]
fn failed_external_batch_consumes_committed_writes_and_skips_later_operations() {
    let cache = Cache::new();
    let owner = feed_owner();
    let status = status_cell(&cache, owner.clone());
    let projection_batches = cache
        .register_array(
            descriptor(
                "projection.candles_1m.batches",
                projection_owner(),
                CellKind::Array,
            ),
            Vec::<Batch>::new(),
        )
        .unwrap();
    let mut runtime = runtime_with_cache(&cache);
    let mut batch = ExternalWriteBatch::new(owner);
    batch
        .set_value(&status, Status { playing: true })
        .push_array(&projection_batches, vec![Batch { seq: 1 }])
        .clear_value(&status);

    runtime.submit_external_writes(batch);
    let err = runtime.run_once().unwrap_err();

    assert!(matches!(
        err,
        RuntimeError::Cache(CacheError::OwnerMismatch { .. })
    ));
    assert_eq!(
        cache.read_value(&status).unwrap(),
        Some(Status { playing: true })
    );
    assert!(cache.read_array(&projection_batches).unwrap().is_empty());
    assert!(runtime.is_idle());
}

#[test]
fn empty_external_batch_is_ignored_by_runtime() {
    let cache = Cache::new();
    let owner = feed_owner();
    let mut runtime = runtime_with_cache(&cache);
    runtime.submit_external_writes(ExternalWriteBatch::new(owner));

    let step = runtime.run_once().unwrap();

    assert_eq!(step.external_write_batches_applied, 0);
    assert!(step.idle_after);
}
