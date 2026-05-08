use std::{
    sync::{Arc, Barrier},
    thread,
};

use cache::{ArrayKey, Cache, CacheError, CellDescriptor, CellKind, CellOwner, Key, ValueKey};

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

fn descriptor(key: &str, owner: CellOwner, kind: CellKind, public_read: bool) -> CellDescriptor {
    CellDescriptor {
        key: Key::new(key).unwrap(),
        owner,
        kind,
        public_read,
    }
}

fn status_descriptor(owner: CellOwner) -> CellDescriptor {
    descriptor("feed.databento.es_mbo.status", owner, CellKind::Value, true)
}

fn batches_descriptor(owner: CellOwner) -> CellDescriptor {
    descriptor(
        "feed.databento.es_mbo.batches",
        owner,
        CellKind::Array,
        false,
    )
}

fn status_cache() -> (Cache, CellOwner, ValueKey<Status>) {
    let cache = Cache::new();
    let owner = feed_owner();
    let key = cache
        .register_value::<Status>(status_descriptor(owner.clone()), None)
        .unwrap();
    (cache, owner, key)
}

fn batches_cache() -> (Cache, CellOwner, ArrayKey<Batch>) {
    let cache = Cache::new();
    let owner = feed_owner();
    let key = cache
        .register_array::<Batch>(batches_descriptor(owner.clone()), Vec::new())
        .unwrap();
    (cache, owner, key)
}

#[test]
fn register_value_returns_typed_handle() {
    let (cache, _owner, status_key) = status_cache();

    assert_eq!(status_key.key().as_str(), "feed.databento.es_mbo.status");
    assert_eq!(cache.read_value(&status_key).unwrap(), None);
}

#[test]
fn register_array_returns_typed_handle() {
    let (cache, _owner, batches_key) = batches_cache();

    assert_eq!(batches_key.key().as_str(), "feed.databento.es_mbo.batches");
    assert!(cache.read_array(&batches_key).unwrap().is_empty());
}

#[test]
fn register_rejects_duplicate_key() {
    let cache = Cache::new();
    let owner = feed_owner();
    cache
        .register_array::<Batch>(batches_descriptor(owner.clone()), Vec::new())
        .unwrap();

    let err = cache
        .register_array::<Batch>(batches_descriptor(owner), Vec::new())
        .unwrap_err();

    assert_eq!(
        err,
        CacheError::DuplicateCell(key("feed.databento.es_mbo.batches"))
    );
}

#[test]
fn register_value_rejects_array_descriptor_kind() {
    let cache = Cache::new();
    let descriptor = descriptor(
        "feed.databento.es_mbo.status",
        feed_owner(),
        CellKind::Array,
        true,
    );

    let err = cache
        .register_value::<Status>(descriptor, None)
        .unwrap_err();

    assert_eq!(
        err,
        CacheError::WrongCellKind {
            key: key("feed.databento.es_mbo.status"),
            expected: CellKind::Value,
            found: CellKind::Array,
        }
    );
}

#[test]
fn register_array_rejects_value_descriptor_kind() {
    let cache = Cache::new();
    let descriptor = descriptor(
        "feed.databento.es_mbo.batches",
        feed_owner(),
        CellKind::Value,
        false,
    );

    let err = cache
        .register_array::<Batch>(descriptor, Vec::new())
        .unwrap_err();

    assert_eq!(
        err,
        CacheError::WrongCellKind {
            key: key("feed.databento.es_mbo.batches"),
            expected: CellKind::Array,
            found: CellKind::Value,
        }
    );
}

#[test]
fn describe_returns_registered_descriptor() {
    let (cache, owner, status_key) = status_cache();

    assert_eq!(
        cache.describe(status_key.key()).unwrap(),
        status_descriptor(owner)
    );
}

#[test]
fn descriptor_public_read_round_trips() {
    let (cache, _owner, batches_key) = batches_cache();

    assert!(!cache.describe(batches_key.key()).unwrap().public_read);
}

#[test]
fn describe_missing_key_errors() {
    let cache = Cache::new();

    let err = cache.describe(&key("missing.key")).unwrap_err();

    assert_eq!(err, CacheError::MissingCell(key("missing.key")));
}

#[test]
fn owner_can_set_owned_value() {
    let (cache, owner, status_key) = status_cache();

    let effects = cache
        .set_value(&owner, &status_key, Status { playing: true })
        .unwrap();

    assert_eq!(
        effects.changed_keys,
        vec![key("feed.databento.es_mbo.status")]
    );
    assert_eq!(
        cache.read_value(&status_key).unwrap(),
        Some(Status { playing: true })
    );
}

#[test]
fn owner_can_mutate_owned_array() {
    let (cache, owner, batches_key) = batches_cache();

    let effects = cache
        .push_array(&owner, &batches_key, vec![Batch { seq: 1 }])
        .unwrap();

    assert_eq!(
        effects.changed_keys,
        vec![key("feed.databento.es_mbo.batches")]
    );
    assert_eq!(
        cache.read_array(&batches_key).unwrap(),
        vec![Batch { seq: 1 }]
    );
}

#[test]
fn other_owner_cannot_mutate_feed_cell() {
    let (cache, owner, status_key) = status_cache();

    let err = cache
        .set_value(&other_owner(), &status_key, Status { playing: true })
        .unwrap_err();

    assert_eq!(
        err,
        CacheError::OwnerMismatch {
            key: key("feed.databento.es_mbo.status"),
            writer: other_owner(),
            owner,
        }
    );
    assert_eq!(cache.read_value(&status_key).unwrap(), None);
}

#[test]
fn feed_owner_cannot_mutate_projection_cell() {
    let cache = Cache::new();
    let projection = projection_owner();
    let state_key = cache
        .register_value::<Status>(
            descriptor(
                "projection.candles_1m.state",
                projection.clone(),
                CellKind::Value,
                false,
            ),
            None,
        )
        .unwrap();

    let err = cache
        .set_value(&feed_owner(), &state_key, Status { playing: true })
        .unwrap_err();

    assert_eq!(
        err,
        CacheError::OwnerMismatch {
            key: key("projection.candles_1m.state"),
            writer: feed_owner(),
            owner: projection,
        }
    );
    assert_eq!(cache.read_value(&state_key).unwrap(), None);
}

#[test]
fn projection_owner_cannot_mutate_feed_cell() {
    let (cache, owner, status_key) = status_cache();

    let err = cache
        .set_value(&projection_owner(), &status_key, Status { playing: true })
        .unwrap_err();

    assert_eq!(
        err,
        CacheError::OwnerMismatch {
            key: key("feed.databento.es_mbo.status"),
            writer: projection_owner(),
            owner,
        }
    );
    assert_eq!(cache.read_value(&status_key).unwrap(), None);
}

#[test]
fn read_value_starts_as_initial_value() {
    let cache = Cache::new();
    let owner = feed_owner();
    let status_key = cache
        .register_value(status_descriptor(owner), Some(Status { playing: false }))
        .unwrap();

    assert_eq!(
        cache.read_value(&status_key).unwrap(),
        Some(Status { playing: false })
    );
}

#[test]
fn set_value_replaces_current_value() {
    let (cache, owner, status_key) = status_cache();

    cache
        .set_value(&owner, &status_key, Status { playing: false })
        .unwrap();
    cache
        .set_value(&owner, &status_key, Status { playing: true })
        .unwrap();

    assert_eq!(
        cache.read_value(&status_key).unwrap(),
        Some(Status { playing: true })
    );
}

#[test]
fn clear_value_sets_none() {
    let (cache, owner, status_key) = status_cache();
    cache
        .set_value(&owner, &status_key, Status { playing: true })
        .unwrap();

    let effects = cache.clear_value(&owner, &status_key).unwrap();

    assert_eq!(
        effects.changed_keys,
        vec![key("feed.databento.es_mbo.status")]
    );
    assert_eq!(cache.read_value(&status_key).unwrap(), None);
}

#[test]
fn update_value_can_modify_existing_value() {
    let (cache, owner, status_key) = status_cache();
    cache
        .set_value(&owner, &status_key, Status { playing: false })
        .unwrap();

    let (was_present, effects) = cache
        .update_value(&owner, &status_key, |status| {
            let was_present = status.is_some();
            status.as_mut().unwrap().playing = true;
            was_present
        })
        .unwrap();

    assert!(was_present);
    assert_eq!(
        effects.changed_keys,
        vec![key("feed.databento.es_mbo.status")]
    );
    assert_eq!(
        cache.read_value(&status_key).unwrap(),
        Some(Status { playing: true })
    );
}

#[test]
fn update_value_can_initialize_none() {
    let (cache, owner, status_key) = status_cache();

    let (result, _effects) = cache
        .update_value(&owner, &status_key, |status| {
            *status = Some(Status { playing: true });
            "initialized"
        })
        .unwrap();

    assert_eq!(result, "initialized");
    assert_eq!(
        cache.read_value(&status_key).unwrap(),
        Some(Status { playing: true })
    );
}

#[test]
fn read_array_starts_as_initial_items() {
    let cache = Cache::new();
    let owner = feed_owner();
    let batches_key = cache
        .register_array(
            batches_descriptor(owner),
            vec![Batch { seq: 1 }, Batch { seq: 2 }],
        )
        .unwrap();

    assert_eq!(
        cache.read_array(&batches_key).unwrap(),
        vec![Batch { seq: 1 }, Batch { seq: 2 }]
    );
}

#[test]
fn read_array_range_returns_slice() {
    let cache = Cache::new();
    let owner = feed_owner();
    let batches_key = cache
        .register_array(
            batches_descriptor(owner),
            vec![Batch { seq: 1 }, Batch { seq: 2 }, Batch { seq: 3 }],
        )
        .unwrap();

    assert_eq!(
        cache.read_array_range(&batches_key, 1..3).unwrap(),
        vec![Batch { seq: 2 }, Batch { seq: 3 }]
    );
}

#[test]
fn read_array_range_rejects_out_of_bounds_range() {
    let (cache, _owner, batches_key) = batches_cache();

    let err = cache.read_array_range(&batches_key, 1..0).unwrap_err();

    assert_eq!(
        err,
        CacheError::ArrayRangeOutOfBounds {
            key: key("feed.databento.es_mbo.batches")
        }
    );
}

#[test]
fn replace_array_replaces_all_items() {
    let (cache, owner, batches_key) = batches_cache();

    cache
        .replace_array(&owner, &batches_key, vec![Batch { seq: 2 }])
        .unwrap();

    assert_eq!(
        cache.read_array(&batches_key).unwrap(),
        vec![Batch { seq: 2 }]
    );
}

#[test]
fn push_array_appends_items() {
    let (cache, owner, batches_key) = batches_cache();

    cache
        .push_array(&owner, &batches_key, vec![Batch { seq: 1 }])
        .unwrap();
    cache
        .push_array(&owner, &batches_key, vec![Batch { seq: 2 }])
        .unwrap();

    assert_eq!(
        cache.read_array(&batches_key).unwrap(),
        vec![Batch { seq: 1 }, Batch { seq: 2 }]
    );
}

#[test]
fn insert_array_inserts_at_index() {
    let (cache, owner, batches_key) = batches_cache();
    cache
        .replace_array(
            &owner,
            &batches_key,
            vec![Batch { seq: 1 }, Batch { seq: 3 }],
        )
        .unwrap();

    cache
        .insert_array(&owner, &batches_key, 1, vec![Batch { seq: 2 }])
        .unwrap();

    assert_eq!(
        cache.read_array(&batches_key).unwrap(),
        vec![Batch { seq: 1 }, Batch { seq: 2 }, Batch { seq: 3 }]
    );
}

#[test]
fn insert_array_rejects_out_of_bounds_index() {
    let (cache, owner, batches_key) = batches_cache();

    let err = cache
        .insert_array(&owner, &batches_key, 1, vec![Batch { seq: 1 }])
        .unwrap_err();

    assert_eq!(
        err,
        CacheError::ArrayRangeOutOfBounds {
            key: key("feed.databento.es_mbo.batches")
        }
    );
    assert!(cache.read_array(&batches_key).unwrap().is_empty());
}

#[test]
fn replace_array_range_replaces_range() {
    let (cache, owner, batches_key) = batches_cache();
    cache
        .replace_array(
            &owner,
            &batches_key,
            vec![Batch { seq: 1 }, Batch { seq: 2 }, Batch { seq: 4 }],
        )
        .unwrap();

    cache
        .replace_array_range(&owner, &batches_key, 1..3, vec![Batch { seq: 3 }])
        .unwrap();

    assert_eq!(
        cache.read_array(&batches_key).unwrap(),
        vec![Batch { seq: 1 }, Batch { seq: 3 }]
    );
}

#[test]
fn replace_array_range_rejects_out_of_bounds_range() {
    let (cache, owner, batches_key) = batches_cache();

    let err = cache
        .replace_array_range(&owner, &batches_key, 0..1, vec![Batch { seq: 1 }])
        .unwrap_err();

    assert_eq!(
        err,
        CacheError::ArrayRangeOutOfBounds {
            key: key("feed.databento.es_mbo.batches")
        }
    );
}

#[test]
fn remove_array_range_removes_range() {
    let (cache, owner, batches_key) = batches_cache();
    cache
        .replace_array(
            &owner,
            &batches_key,
            vec![Batch { seq: 1 }, Batch { seq: 2 }, Batch { seq: 3 }],
        )
        .unwrap();

    cache
        .remove_array_range(&owner, &batches_key, 1..2)
        .unwrap();

    assert_eq!(
        cache.read_array(&batches_key).unwrap(),
        vec![Batch { seq: 1 }, Batch { seq: 3 }]
    );
}

#[test]
fn remove_array_range_rejects_out_of_bounds_range() {
    let (cache, owner, batches_key) = batches_cache();

    let err = cache
        .remove_array_range(&owner, &batches_key, 0..1)
        .unwrap_err();

    assert_eq!(
        err,
        CacheError::ArrayRangeOutOfBounds {
            key: key("feed.databento.es_mbo.batches")
        }
    );
}

#[test]
fn clear_array_removes_all_items() {
    let (cache, owner, batches_key) = batches_cache();
    cache
        .replace_array(&owner, &batches_key, vec![Batch { seq: 1 }])
        .unwrap();

    cache.clear_array(&owner, &batches_key).unwrap();

    assert!(cache.read_array(&batches_key).unwrap().is_empty());
}

#[test]
fn update_array_can_perform_custom_mutation() {
    let (cache, owner, batches_key) = batches_cache();

    let (len, effects) = cache
        .update_array(&owner, &batches_key, |items| {
            items.push(Batch { seq: 1 });
            items.push(Batch { seq: 2 });
            items.len()
        })
        .unwrap();

    assert_eq!(len, 2);
    assert_eq!(
        effects.changed_keys,
        vec![key("feed.databento.es_mbo.batches")]
    );
    assert_eq!(
        cache.read_array(&batches_key).unwrap(),
        vec![Batch { seq: 1 }, Batch { seq: 2 }]
    );
}

#[test]
fn empty_array_mutations_still_return_changed_key() {
    let (cache, owner, batches_key) = batches_cache();

    let effects = cache.push_array(&owner, &batches_key, Vec::new()).unwrap();

    assert_eq!(
        effects.changed_keys,
        vec![key("feed.databento.es_mbo.batches")]
    );
}

#[test]
fn typed_handles_expose_registered_key() {
    let (_cache, _owner, status_key) = status_cache();

    assert_eq!(status_key.key().as_str(), "feed.databento.es_mbo.status");
}

#[test]
fn clone_cache_shares_same_cells() {
    let (cache, owner, status_key) = status_cache();
    let cloned = cache.clone();

    cloned
        .set_value(&owner, &status_key, Status { playing: true })
        .unwrap();

    assert_eq!(
        cache.read_value(&status_key).unwrap(),
        Some(Status { playing: true })
    );
}

#[test]
fn parallel_writes_to_different_cells_do_not_deadlock() {
    let cache = Cache::new();
    let owner = feed_owner();
    let status_key = cache
        .register_value::<Status>(status_descriptor(owner.clone()), None)
        .unwrap();
    let batches_key = cache
        .register_array::<Batch>(batches_descriptor(owner.clone()), Vec::new())
        .unwrap();
    let barrier = Arc::new(Barrier::new(2));

    let cache_a = cache.clone();
    let owner_a = owner.clone();
    let status_key_a = status_key.clone();
    let barrier_a = barrier.clone();
    let status_thread = thread::spawn(move || {
        barrier_a.wait();
        cache_a
            .set_value(&owner_a, &status_key_a, Status { playing: true })
            .unwrap();
    });

    let cache_b = cache.clone();
    let owner_b = owner.clone();
    let batches_key_b = batches_key.clone();
    let barrier_b = barrier.clone();
    let batches_thread = thread::spawn(move || {
        barrier_b.wait();
        cache_b
            .push_array(&owner_b, &batches_key_b, vec![Batch { seq: 1 }])
            .unwrap();
    });

    status_thread.join().unwrap();
    batches_thread.join().unwrap();

    assert_eq!(
        cache.read_value(&status_key).unwrap(),
        Some(Status { playing: true })
    );
    assert_eq!(
        cache.read_array(&batches_key).unwrap(),
        vec![Batch { seq: 1 }]
    );
}

#[test]
fn parallel_reads_can_observe_registered_cells() {
    let (cache, owner, status_key) = status_cache();
    cache
        .set_value(&owner, &status_key, Status { playing: true })
        .unwrap();

    let mut handles = Vec::new();
    for _ in 0..4 {
        let cache = cache.clone();
        let status_key = status_key.clone();
        handles.push(thread::spawn(move || {
            cache.read_value(&status_key).unwrap()
        }));
    }

    for handle in handles {
        assert_eq!(handle.join().unwrap(), Some(Status { playing: true }));
    }
}

#[test]
fn write_releases_lock_before_next_read() {
    let (cache, owner, status_key) = status_cache();

    cache
        .set_value(&owner, &status_key, Status { playing: true })
        .unwrap();

    assert_eq!(
        cache.read_value(&status_key).unwrap(),
        Some(Status { playing: true })
    );
}
