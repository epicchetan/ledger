use std::{
    sync::{Arc, Barrier},
    thread,
};

use ledger_runtime::{
    ArrayKey, CellDescriptor, CellKind, CellOwner, DataPlane, DataPlaneError, Key, ValueKey,
};

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

fn origin_owner() -> CellOwner {
    CellOwner::Origin("origin.es_mbo".to_string())
}

fn projection_owner() -> CellOwner {
    CellOwner::Projection("projection.candles_1m".to_string())
}

fn runtime_owner() -> CellOwner {
    CellOwner::Runtime
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
    descriptor("origin.es_mbo.status", owner, CellKind::Value, true)
}

fn batches_descriptor(owner: CellOwner) -> CellDescriptor {
    descriptor("origin.es_mbo.batches", owner, CellKind::Array, false)
}

fn status_plane() -> (DataPlane, CellOwner, ValueKey<Status>) {
    let data_plane = DataPlane::new();
    let owner = origin_owner();
    let key = data_plane
        .register_value::<Status>(status_descriptor(owner.clone()), None)
        .unwrap();
    (data_plane, owner, key)
}

fn batches_plane() -> (DataPlane, CellOwner, ArrayKey<Batch>) {
    let data_plane = DataPlane::new();
    let owner = origin_owner();
    let key = data_plane
        .register_array::<Batch>(batches_descriptor(owner.clone()), Vec::new())
        .unwrap();
    (data_plane, owner, key)
}

#[test]
fn register_value_returns_typed_handle() {
    let (data_plane, _owner, status_key) = status_plane();

    assert_eq!(status_key.key().as_str(), "origin.es_mbo.status");
    assert_eq!(data_plane.read_value(&status_key).unwrap(), None);
}

#[test]
fn register_array_returns_typed_handle() {
    let (data_plane, _owner, batches_key) = batches_plane();

    assert_eq!(batches_key.key().as_str(), "origin.es_mbo.batches");
    assert!(data_plane.read_array(&batches_key).unwrap().is_empty());
}

#[test]
fn register_rejects_duplicate_key() {
    let data_plane = DataPlane::new();
    let owner = origin_owner();
    data_plane
        .register_array::<Batch>(batches_descriptor(owner.clone()), Vec::new())
        .unwrap();

    let err = data_plane
        .register_array::<Batch>(batches_descriptor(owner), Vec::new())
        .unwrap_err();

    assert_eq!(
        err,
        DataPlaneError::DuplicateCell(key("origin.es_mbo.batches"))
    );
}

#[test]
fn register_value_rejects_array_descriptor_kind() {
    let data_plane = DataPlane::new();
    let descriptor = descriptor(
        "origin.es_mbo.status",
        origin_owner(),
        CellKind::Array,
        true,
    );

    let err = data_plane
        .register_value::<Status>(descriptor, None)
        .unwrap_err();

    assert_eq!(
        err,
        DataPlaneError::WrongCellKind {
            key: key("origin.es_mbo.status"),
            expected: CellKind::Value,
            found: CellKind::Array,
        }
    );
}

#[test]
fn register_array_rejects_value_descriptor_kind() {
    let data_plane = DataPlane::new();
    let descriptor = descriptor(
        "origin.es_mbo.batches",
        origin_owner(),
        CellKind::Value,
        false,
    );

    let err = data_plane
        .register_array::<Batch>(descriptor, Vec::new())
        .unwrap_err();

    assert_eq!(
        err,
        DataPlaneError::WrongCellKind {
            key: key("origin.es_mbo.batches"),
            expected: CellKind::Array,
            found: CellKind::Value,
        }
    );
}

#[test]
fn describe_returns_registered_descriptor() {
    let (data_plane, owner, status_key) = status_plane();

    assert_eq!(
        data_plane.describe(status_key.key()).unwrap(),
        status_descriptor(owner)
    );
}

#[test]
fn descriptor_public_read_round_trips() {
    let (data_plane, _owner, batches_key) = batches_plane();

    assert!(!data_plane.describe(batches_key.key()).unwrap().public_read);
}

#[test]
fn describe_missing_key_errors() {
    let data_plane = DataPlane::new();

    let err = data_plane.describe(&key("missing.key")).unwrap_err();

    assert_eq!(err, DataPlaneError::MissingCell(key("missing.key")));
}

#[test]
fn owner_can_set_owned_value() {
    let (data_plane, owner, status_key) = status_plane();

    let effects = data_plane
        .set_value(&owner, &status_key, Status { playing: true })
        .unwrap();

    assert_eq!(effects.changed_keys, vec![key("origin.es_mbo.status")]);
    assert_eq!(
        data_plane.read_value(&status_key).unwrap(),
        Some(Status { playing: true })
    );
}

#[test]
fn owner_can_mutate_owned_array() {
    let (data_plane, owner, batches_key) = batches_plane();

    let effects = data_plane
        .push_array(&owner, &batches_key, vec![Batch { seq: 1 }])
        .unwrap();

    assert_eq!(effects.changed_keys, vec![key("origin.es_mbo.batches")]);
    assert_eq!(
        data_plane.read_array(&batches_key).unwrap(),
        vec![Batch { seq: 1 }]
    );
}

#[test]
fn runtime_owner_cannot_mutate_origin_cell() {
    let (data_plane, owner, status_key) = status_plane();

    let err = data_plane
        .set_value(&runtime_owner(), &status_key, Status { playing: true })
        .unwrap_err();

    assert_eq!(
        err,
        DataPlaneError::OwnerMismatch {
            key: key("origin.es_mbo.status"),
            writer: runtime_owner(),
            owner,
        }
    );
    assert_eq!(data_plane.read_value(&status_key).unwrap(), None);
}

#[test]
fn origin_owner_cannot_mutate_projection_cell() {
    let data_plane = DataPlane::new();
    let projection = projection_owner();
    let state_key = data_plane
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

    let err = data_plane
        .set_value(&origin_owner(), &state_key, Status { playing: true })
        .unwrap_err();

    assert_eq!(
        err,
        DataPlaneError::OwnerMismatch {
            key: key("projection.candles_1m.state"),
            writer: origin_owner(),
            owner: projection,
        }
    );
    assert_eq!(data_plane.read_value(&state_key).unwrap(), None);
}

#[test]
fn projection_owner_cannot_mutate_origin_cell() {
    let (data_plane, owner, status_key) = status_plane();

    let err = data_plane
        .set_value(&projection_owner(), &status_key, Status { playing: true })
        .unwrap_err();

    assert_eq!(
        err,
        DataPlaneError::OwnerMismatch {
            key: key("origin.es_mbo.status"),
            writer: projection_owner(),
            owner,
        }
    );
    assert_eq!(data_plane.read_value(&status_key).unwrap(), None);
}

#[test]
fn read_value_starts_as_initial_value() {
    let data_plane = DataPlane::new();
    let owner = origin_owner();
    let status_key = data_plane
        .register_value(status_descriptor(owner), Some(Status { playing: false }))
        .unwrap();

    assert_eq!(
        data_plane.read_value(&status_key).unwrap(),
        Some(Status { playing: false })
    );
}

#[test]
fn set_value_replaces_current_value() {
    let (data_plane, owner, status_key) = status_plane();

    data_plane
        .set_value(&owner, &status_key, Status { playing: false })
        .unwrap();
    data_plane
        .set_value(&owner, &status_key, Status { playing: true })
        .unwrap();

    assert_eq!(
        data_plane.read_value(&status_key).unwrap(),
        Some(Status { playing: true })
    );
}

#[test]
fn clear_value_sets_none() {
    let (data_plane, owner, status_key) = status_plane();
    data_plane
        .set_value(&owner, &status_key, Status { playing: true })
        .unwrap();

    let effects = data_plane.clear_value(&owner, &status_key).unwrap();

    assert_eq!(effects.changed_keys, vec![key("origin.es_mbo.status")]);
    assert_eq!(data_plane.read_value(&status_key).unwrap(), None);
}

#[test]
fn update_value_can_modify_existing_value() {
    let (data_plane, owner, status_key) = status_plane();
    data_plane
        .set_value(&owner, &status_key, Status { playing: false })
        .unwrap();

    let (was_present, effects) = data_plane
        .update_value(&owner, &status_key, |status| {
            let was_present = status.is_some();
            status.as_mut().unwrap().playing = true;
            was_present
        })
        .unwrap();

    assert!(was_present);
    assert_eq!(effects.changed_keys, vec![key("origin.es_mbo.status")]);
    assert_eq!(
        data_plane.read_value(&status_key).unwrap(),
        Some(Status { playing: true })
    );
}

#[test]
fn update_value_can_initialize_none() {
    let (data_plane, owner, status_key) = status_plane();

    let (result, _effects) = data_plane
        .update_value(&owner, &status_key, |status| {
            *status = Some(Status { playing: true });
            "initialized"
        })
        .unwrap();

    assert_eq!(result, "initialized");
    assert_eq!(
        data_plane.read_value(&status_key).unwrap(),
        Some(Status { playing: true })
    );
}

#[test]
fn read_array_starts_as_initial_items() {
    let data_plane = DataPlane::new();
    let owner = origin_owner();
    let batches_key = data_plane
        .register_array(
            batches_descriptor(owner),
            vec![Batch { seq: 1 }, Batch { seq: 2 }],
        )
        .unwrap();

    assert_eq!(
        data_plane.read_array(&batches_key).unwrap(),
        vec![Batch { seq: 1 }, Batch { seq: 2 }]
    );
}

#[test]
fn read_array_range_returns_slice() {
    let data_plane = DataPlane::new();
    let owner = origin_owner();
    let batches_key = data_plane
        .register_array(
            batches_descriptor(owner),
            vec![Batch { seq: 1 }, Batch { seq: 2 }, Batch { seq: 3 }],
        )
        .unwrap();

    assert_eq!(
        data_plane.read_array_range(&batches_key, 1..3).unwrap(),
        vec![Batch { seq: 2 }, Batch { seq: 3 }]
    );
}

#[test]
fn read_array_range_rejects_out_of_bounds_range() {
    let (data_plane, _owner, batches_key) = batches_plane();

    let err = data_plane.read_array_range(&batches_key, 1..0).unwrap_err();

    assert_eq!(
        err,
        DataPlaneError::ArrayRangeOutOfBounds {
            key: key("origin.es_mbo.batches")
        }
    );
}

#[test]
fn replace_array_replaces_all_items() {
    let (data_plane, owner, batches_key) = batches_plane();

    data_plane
        .replace_array(&owner, &batches_key, vec![Batch { seq: 2 }])
        .unwrap();

    assert_eq!(
        data_plane.read_array(&batches_key).unwrap(),
        vec![Batch { seq: 2 }]
    );
}

#[test]
fn push_array_appends_items() {
    let (data_plane, owner, batches_key) = batches_plane();

    data_plane
        .push_array(&owner, &batches_key, vec![Batch { seq: 1 }])
        .unwrap();
    data_plane
        .push_array(&owner, &batches_key, vec![Batch { seq: 2 }])
        .unwrap();

    assert_eq!(
        data_plane.read_array(&batches_key).unwrap(),
        vec![Batch { seq: 1 }, Batch { seq: 2 }]
    );
}

#[test]
fn insert_array_inserts_at_index() {
    let (data_plane, owner, batches_key) = batches_plane();
    data_plane
        .replace_array(
            &owner,
            &batches_key,
            vec![Batch { seq: 1 }, Batch { seq: 3 }],
        )
        .unwrap();

    data_plane
        .insert_array(&owner, &batches_key, 1, vec![Batch { seq: 2 }])
        .unwrap();

    assert_eq!(
        data_plane.read_array(&batches_key).unwrap(),
        vec![Batch { seq: 1 }, Batch { seq: 2 }, Batch { seq: 3 }]
    );
}

#[test]
fn insert_array_rejects_out_of_bounds_index() {
    let (data_plane, owner, batches_key) = batches_plane();

    let err = data_plane
        .insert_array(&owner, &batches_key, 1, vec![Batch { seq: 1 }])
        .unwrap_err();

    assert_eq!(
        err,
        DataPlaneError::ArrayRangeOutOfBounds {
            key: key("origin.es_mbo.batches")
        }
    );
    assert!(data_plane.read_array(&batches_key).unwrap().is_empty());
}

#[test]
fn replace_array_range_replaces_range() {
    let (data_plane, owner, batches_key) = batches_plane();
    data_plane
        .replace_array(
            &owner,
            &batches_key,
            vec![Batch { seq: 1 }, Batch { seq: 2 }, Batch { seq: 4 }],
        )
        .unwrap();

    data_plane
        .replace_array_range(&owner, &batches_key, 1..3, vec![Batch { seq: 3 }])
        .unwrap();

    assert_eq!(
        data_plane.read_array(&batches_key).unwrap(),
        vec![Batch { seq: 1 }, Batch { seq: 3 }]
    );
}

#[test]
fn replace_array_range_rejects_out_of_bounds_range() {
    let (data_plane, owner, batches_key) = batches_plane();

    let err = data_plane
        .replace_array_range(&owner, &batches_key, 0..1, vec![Batch { seq: 1 }])
        .unwrap_err();

    assert_eq!(
        err,
        DataPlaneError::ArrayRangeOutOfBounds {
            key: key("origin.es_mbo.batches")
        }
    );
}

#[test]
fn remove_array_range_removes_range() {
    let (data_plane, owner, batches_key) = batches_plane();
    data_plane
        .replace_array(
            &owner,
            &batches_key,
            vec![Batch { seq: 1 }, Batch { seq: 2 }, Batch { seq: 3 }],
        )
        .unwrap();

    data_plane
        .remove_array_range(&owner, &batches_key, 1..2)
        .unwrap();

    assert_eq!(
        data_plane.read_array(&batches_key).unwrap(),
        vec![Batch { seq: 1 }, Batch { seq: 3 }]
    );
}

#[test]
fn remove_array_range_rejects_out_of_bounds_range() {
    let (data_plane, owner, batches_key) = batches_plane();

    let err = data_plane
        .remove_array_range(&owner, &batches_key, 0..1)
        .unwrap_err();

    assert_eq!(
        err,
        DataPlaneError::ArrayRangeOutOfBounds {
            key: key("origin.es_mbo.batches")
        }
    );
}

#[test]
fn clear_array_removes_all_items() {
    let (data_plane, owner, batches_key) = batches_plane();
    data_plane
        .replace_array(&owner, &batches_key, vec![Batch { seq: 1 }])
        .unwrap();

    data_plane.clear_array(&owner, &batches_key).unwrap();

    assert!(data_plane.read_array(&batches_key).unwrap().is_empty());
}

#[test]
fn update_array_can_perform_custom_mutation() {
    let (data_plane, owner, batches_key) = batches_plane();

    let (len, effects) = data_plane
        .update_array(&owner, &batches_key, |items| {
            items.push(Batch { seq: 1 });
            items.push(Batch { seq: 2 });
            items.len()
        })
        .unwrap();

    assert_eq!(len, 2);
    assert_eq!(effects.changed_keys, vec![key("origin.es_mbo.batches")]);
    assert_eq!(
        data_plane.read_array(&batches_key).unwrap(),
        vec![Batch { seq: 1 }, Batch { seq: 2 }]
    );
}

#[test]
fn empty_array_mutations_still_return_changed_key() {
    let (data_plane, owner, batches_key) = batches_plane();

    let effects = data_plane
        .push_array(&owner, &batches_key, Vec::new())
        .unwrap();

    assert_eq!(effects.changed_keys, vec![key("origin.es_mbo.batches")]);
}

#[test]
fn typed_handles_expose_registered_key() {
    let (_data_plane, _owner, status_key) = status_plane();

    assert_eq!(status_key.key().as_str(), "origin.es_mbo.status");
}

#[test]
fn clone_data_plane_shares_same_cells() {
    let (data_plane, owner, status_key) = status_plane();
    let cloned = data_plane.clone();

    cloned
        .set_value(&owner, &status_key, Status { playing: true })
        .unwrap();

    assert_eq!(
        data_plane.read_value(&status_key).unwrap(),
        Some(Status { playing: true })
    );
}

#[test]
fn parallel_writes_to_different_cells_do_not_deadlock() {
    let data_plane = DataPlane::new();
    let owner = origin_owner();
    let status_key = data_plane
        .register_value::<Status>(status_descriptor(owner.clone()), None)
        .unwrap();
    let batches_key = data_plane
        .register_array::<Batch>(batches_descriptor(owner.clone()), Vec::new())
        .unwrap();
    let barrier = Arc::new(Barrier::new(2));

    let data_plane_a = data_plane.clone();
    let owner_a = owner.clone();
    let status_key_a = status_key.clone();
    let barrier_a = barrier.clone();
    let status_thread = thread::spawn(move || {
        barrier_a.wait();
        data_plane_a
            .set_value(&owner_a, &status_key_a, Status { playing: true })
            .unwrap();
    });

    let data_plane_b = data_plane.clone();
    let owner_b = owner.clone();
    let batches_key_b = batches_key.clone();
    let barrier_b = barrier.clone();
    let batches_thread = thread::spawn(move || {
        barrier_b.wait();
        data_plane_b
            .push_array(&owner_b, &batches_key_b, vec![Batch { seq: 1 }])
            .unwrap();
    });

    status_thread.join().unwrap();
    batches_thread.join().unwrap();

    assert_eq!(
        data_plane.read_value(&status_key).unwrap(),
        Some(Status { playing: true })
    );
    assert_eq!(
        data_plane.read_array(&batches_key).unwrap(),
        vec![Batch { seq: 1 }]
    );
}

#[test]
fn parallel_reads_can_observe_registered_cells() {
    let (data_plane, owner, status_key) = status_plane();
    data_plane
        .set_value(&owner, &status_key, Status { playing: true })
        .unwrap();

    let mut handles = Vec::new();
    for _ in 0..4 {
        let data_plane = data_plane.clone();
        let status_key = status_key.clone();
        handles.push(thread::spawn(move || {
            data_plane.read_value(&status_key).unwrap()
        }));
    }

    for handle in handles {
        assert_eq!(handle.join().unwrap(), Some(Status { playing: true }));
    }
}

#[test]
fn write_releases_lock_before_next_read() {
    let (data_plane, owner, status_key) = status_plane();

    data_plane
        .set_value(&owner, &status_key, Status { playing: true })
        .unwrap();

    assert_eq!(
        data_plane.read_value(&status_key).unwrap(),
        Some(Status { playing: true })
    );
}
