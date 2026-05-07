use ledger_runtime::{
    ArrayKey, CellDescriptor, CellKind, CellOwner, DataPlane, DataPlaneError, ExternalWriteBatch,
    Key, Runtime, RuntimeError, ValueKey,
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

fn descriptor(key_path: &str, owner: CellOwner, kind: CellKind) -> CellDescriptor {
    CellDescriptor {
        key: key(key_path),
        owner,
        kind,
        public_read: false,
    }
}

fn status_cell(data_plane: &DataPlane, owner: CellOwner) -> ValueKey<Status> {
    data_plane
        .register_value(
            descriptor("origin.es_mbo.status", owner, CellKind::Value),
            None,
        )
        .unwrap()
}

fn batches_cell(data_plane: &DataPlane, owner: CellOwner) -> ArrayKey<Batch> {
    data_plane
        .register_array(
            descriptor("origin.es_mbo.batches", owner, CellKind::Array),
            Vec::new(),
        )
        .unwrap()
}

fn runtime_with_plane(data_plane: &DataPlane) -> Runtime {
    Runtime::new(data_plane.clone())
}

#[test]
fn external_batch_set_value_writes_through_data_plane() {
    let data_plane = DataPlane::new();
    let owner = origin_owner();
    let status = status_cell(&data_plane, owner.clone());
    let mut runtime = runtime_with_plane(&data_plane);
    let mut batch = ExternalWriteBatch::new(owner);
    batch.set_value(&status, Status { playing: true });

    runtime.submit_external_writes(batch);
    let step = runtime.run_once().unwrap();

    assert_eq!(
        data_plane.read_value(&status).unwrap(),
        Some(Status { playing: true })
    );
    assert_eq!(step.changed_keys, vec![key("origin.es_mbo.status")]);
    assert!(step.idle_after);
}

#[test]
fn external_batch_clear_value_clears_through_data_plane() {
    let data_plane = DataPlane::new();
    let owner = origin_owner();
    let status = status_cell(&data_plane, owner.clone());
    data_plane
        .set_value(&owner, &status, Status { playing: true })
        .unwrap();
    let mut runtime = runtime_with_plane(&data_plane);
    let mut batch = ExternalWriteBatch::new(owner);
    batch.clear_value(&status);

    runtime.submit_external_writes(batch);
    runtime.run_once().unwrap();

    assert_eq!(data_plane.read_value(&status).unwrap(), None);
}

#[test]
fn external_batch_push_array_preserves_item_order() {
    let data_plane = DataPlane::new();
    let owner = origin_owner();
    let batches = batches_cell(&data_plane, owner.clone());
    let mut runtime = runtime_with_plane(&data_plane);
    let mut batch = ExternalWriteBatch::new(owner);
    batch.push_array(&batches, vec![Batch { seq: 1 }, Batch { seq: 2 }]);

    runtime.submit_external_writes(batch);
    runtime.run_once().unwrap();

    assert_eq!(
        data_plane.read_array(&batches).unwrap(),
        vec![Batch { seq: 1 }, Batch { seq: 2 }]
    );
}

#[test]
fn external_batch_replace_array_replaces_existing_items() {
    let data_plane = DataPlane::new();
    let owner = origin_owner();
    let batches = batches_cell(&data_plane, owner.clone());
    data_plane
        .push_array(&owner, &batches, vec![Batch { seq: 1 }])
        .unwrap();
    let mut runtime = runtime_with_plane(&data_plane);
    let mut batch = ExternalWriteBatch::new(owner);
    batch.replace_array(&batches, vec![Batch { seq: 10 }]);

    runtime.submit_external_writes(batch);
    runtime.run_once().unwrap();

    assert_eq!(
        data_plane.read_array(&batches).unwrap(),
        vec![Batch { seq: 10 }]
    );
}

#[test]
fn external_batch_insert_array_validates_bounds_through_data_plane() {
    let data_plane = DataPlane::new();
    let owner = origin_owner();
    let batches = batches_cell(&data_plane, owner.clone());
    let mut runtime = runtime_with_plane(&data_plane);
    let mut batch = ExternalWriteBatch::new(owner);
    batch.insert_array(&batches, 1, vec![Batch { seq: 1 }]);

    runtime.submit_external_writes(batch);
    let err = runtime.run_once().unwrap_err();

    assert_eq!(
        err,
        RuntimeError::DataPlane(DataPlaneError::ArrayRangeOutOfBounds {
            key: key("origin.es_mbo.batches"),
        })
    );
    assert!(data_plane.read_array(&batches).unwrap().is_empty());
}

#[test]
fn external_batch_replace_array_range_validates_bounds_through_data_plane() {
    let data_plane = DataPlane::new();
    let owner = origin_owner();
    let batches = batches_cell(&data_plane, owner.clone());
    let mut runtime = runtime_with_plane(&data_plane);
    let mut batch = ExternalWriteBatch::new(owner);
    batch.replace_array_range(&batches, 0..1, vec![Batch { seq: 1 }]);

    runtime.submit_external_writes(batch);
    let err = runtime.run_once().unwrap_err();

    assert_eq!(
        err,
        RuntimeError::DataPlane(DataPlaneError::ArrayRangeOutOfBounds {
            key: key("origin.es_mbo.batches"),
        })
    );
}

#[test]
fn external_batch_remove_array_range_validates_bounds_through_data_plane() {
    let data_plane = DataPlane::new();
    let owner = origin_owner();
    let batches = batches_cell(&data_plane, owner.clone());
    let mut runtime = runtime_with_plane(&data_plane);
    let mut batch = ExternalWriteBatch::new(owner);
    batch.remove_array_range(&batches, 0..1);

    runtime.submit_external_writes(batch);
    let err = runtime.run_once().unwrap_err();

    assert_eq!(
        err,
        RuntimeError::DataPlane(DataPlaneError::ArrayRangeOutOfBounds {
            key: key("origin.es_mbo.batches"),
        })
    );
}

#[test]
fn external_batch_clear_array_clears_through_data_plane() {
    let data_plane = DataPlane::new();
    let owner = origin_owner();
    let batches = batches_cell(&data_plane, owner.clone());
    data_plane
        .push_array(&owner, &batches, vec![Batch { seq: 1 }])
        .unwrap();
    let mut runtime = runtime_with_plane(&data_plane);
    let mut batch = ExternalWriteBatch::new(owner);
    batch.clear_array(&batches);

    runtime.submit_external_writes(batch);
    runtime.run_once().unwrap();

    assert!(data_plane.read_array(&batches).unwrap().is_empty());
}

#[test]
fn external_batch_records_each_changed_key_once_and_preserves_operation_order() {
    let data_plane = DataPlane::new();
    let owner = origin_owner();
    let status = status_cell(&data_plane, owner.clone());
    let batches = batches_cell(&data_plane, owner.clone());
    let mut runtime = runtime_with_plane(&data_plane);
    let mut batch = ExternalWriteBatch::new(owner);
    batch
        .set_value(&status, Status { playing: false })
        .set_value(&status, Status { playing: true })
        .push_array(&batches, vec![Batch { seq: 1 }])
        .push_array(&batches, vec![Batch { seq: 2 }]);

    runtime.submit_external_writes(batch);
    let step = runtime.run_once().unwrap();

    assert_eq!(
        data_plane.read_value(&status).unwrap(),
        Some(Status { playing: true })
    );
    assert_eq!(
        data_plane.read_array(&batches).unwrap(),
        vec![Batch { seq: 1 }, Batch { seq: 2 }]
    );
    assert_eq!(
        step.changed_keys,
        vec![key("origin.es_mbo.status"), key("origin.es_mbo.batches")]
    );
}

#[test]
fn external_batch_fails_on_owner_mismatch() {
    let data_plane = DataPlane::new();
    let owner = origin_owner();
    let status = status_cell(&data_plane, owner.clone());
    let mut runtime = runtime_with_plane(&data_plane);
    let mut batch = ExternalWriteBatch::new(CellOwner::Runtime);
    batch.set_value(&status, Status { playing: true });

    runtime.submit_external_writes(batch);
    let err = runtime.run_once().unwrap_err();

    assert_eq!(
        err,
        RuntimeError::DataPlane(DataPlaneError::OwnerMismatch {
            key: key("origin.es_mbo.status"),
            writer: CellOwner::Runtime,
            owner,
        })
    );
    assert_eq!(data_plane.read_value(&status).unwrap(), None);
}

#[test]
fn failed_external_batch_consumes_committed_writes_and_skips_later_operations() {
    let data_plane = DataPlane::new();
    let owner = origin_owner();
    let status = status_cell(&data_plane, owner.clone());
    let projection_batches = data_plane
        .register_array(
            descriptor(
                "projection.candles_1m.batches",
                projection_owner(),
                CellKind::Array,
            ),
            Vec::<Batch>::new(),
        )
        .unwrap();
    let mut runtime = runtime_with_plane(&data_plane);
    let mut batch = ExternalWriteBatch::new(owner);
    batch
        .set_value(&status, Status { playing: true })
        .push_array(&projection_batches, vec![Batch { seq: 1 }])
        .clear_value(&status);

    runtime.submit_external_writes(batch);
    let err = runtime.run_once().unwrap_err();

    assert!(matches!(
        err,
        RuntimeError::DataPlane(DataPlaneError::OwnerMismatch { .. })
    ));
    assert_eq!(
        data_plane.read_value(&status).unwrap(),
        Some(Status { playing: true })
    );
    assert!(data_plane
        .read_array(&projection_batches)
        .unwrap()
        .is_empty());
    assert!(runtime.is_idle());
}

#[test]
fn empty_external_batch_is_ignored_by_runtime() {
    let data_plane = DataPlane::new();
    let owner = origin_owner();
    let mut runtime = runtime_with_plane(&data_plane);
    runtime.submit_external_writes(ExternalWriteBatch::new(owner));

    let step = runtime.run_once().unwrap();

    assert_eq!(step.external_write_batches_applied, 0);
    assert!(step.idle_after);
}
