use ledger_runtime::{
    ArrayKey, CellDescriptor, CellKind, CellOwner, DataPlane, DataPlaneError, Key, Projection,
    ProjectionContext, ProjectionDescriptor, ProjectionError, ProjectionId, ValueKey,
};

#[derive(Clone, Debug, PartialEq, Eq)]
struct SourceBatch {
    seq: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct ProjectionState {
    processed: usize,
}

fn key(value: &str) -> Key {
    Key::new(value).unwrap()
}

fn projection_id() -> ProjectionId {
    ProjectionId::new("projection.candles_1m").unwrap()
}

#[test]
fn projection_id_accepts_key_like_identifier() {
    let id = projection_id();

    assert_eq!(id.as_str(), "projection.candles_1m");
    assert_eq!(
        id.owner(),
        CellOwner::Projection("projection.candles_1m".to_string())
    );
}

#[test]
fn projection_id_rejects_invalid_identifier() {
    assert!(matches!(
        ProjectionId::new("Projection.Candles"),
        Err(ProjectionError::InvalidProjectionId(value)) if value == "Projection.Candles"
    ));
}

#[test]
fn projection_descriptor_holds_id_and_dependencies() {
    let descriptor = ProjectionDescriptor::new(projection_id(), vec![key("origin.es_mbo.batches")]);

    assert_eq!(descriptor.id.as_str(), "projection.candles_1m");
    assert_eq!(descriptor.dependencies, vec![key("origin.es_mbo.batches")]);
}

#[test]
fn projection_context_reads_inputs_and_writes_as_projection_owner() {
    let data_plane = DataPlane::new();
    let origin = CellOwner::Origin("origin.es_mbo".to_string());
    let projection_id = projection_id();
    let projection_owner = projection_id.owner();
    let batches = data_plane
        .register_array(
            CellDescriptor {
                key: key("origin.es_mbo.batches"),
                owner: origin.clone(),
                kind: CellKind::Array,
                public_read: false,
            },
            vec![SourceBatch { seq: 1 }],
        )
        .unwrap();
    let state = data_plane
        .register_value::<ProjectionState>(
            CellDescriptor {
                key: key("projection.candles_1m.state"),
                owner: projection_owner,
                kind: CellKind::Value,
                public_read: false,
            },
            None,
        )
        .unwrap();
    let mut ctx = ProjectionContext::new(&data_plane, projection_id);

    assert_eq!(
        ctx.read_array(&batches).unwrap(),
        vec![SourceBatch { seq: 1 }]
    );
    ctx.set_value(&state, ProjectionState { processed: 1 })
        .unwrap();

    assert_eq!(
        data_plane.read_value(&state).unwrap(),
        Some(ProjectionState { processed: 1 })
    );
    assert_eq!(ctx.changed_keys(), &[key("projection.candles_1m.state")]);
}

#[test]
fn projection_context_rejects_writes_to_cells_owned_by_another_owner() {
    let data_plane = DataPlane::new();
    let origin = CellOwner::Origin("origin.es_mbo".to_string());
    let status = data_plane
        .register_value::<ProjectionState>(
            CellDescriptor {
                key: key("origin.es_mbo.status"),
                owner: origin.clone(),
                kind: CellKind::Value,
                public_read: false,
            },
            None,
        )
        .unwrap();
    let mut ctx = ProjectionContext::new(&data_plane, projection_id());

    let err = ctx
        .set_value(&status, ProjectionState { processed: 1 })
        .unwrap_err();

    assert_eq!(
        err,
        ProjectionError::DataPlane(DataPlaneError::OwnerMismatch {
            key: key("origin.es_mbo.status"),
            writer: CellOwner::Projection("projection.candles_1m".to_string()),
            owner: origin,
        })
    );
}

struct CountingProjection {
    descriptor: ProjectionDescriptor,
    batches: ArrayKey<SourceBatch>,
    state: ValueKey<ProjectionState>,
}

impl Projection for CountingProjection {
    fn descriptor(&self) -> &ProjectionDescriptor {
        &self.descriptor
    }

    fn run(&mut self, ctx: &mut ProjectionContext<'_>) -> Result<(), ProjectionError> {
        let batches = ctx.read_array(&self.batches)?;
        ctx.set_value(
            &self.state,
            ProjectionState {
                processed: batches.len(),
            },
        )?;
        Ok(())
    }
}

#[test]
fn projection_trait_object_runs_against_context() {
    let data_plane = DataPlane::new();
    let origin = CellOwner::Origin("origin.es_mbo".to_string());
    let projection_id = projection_id();
    let batches = data_plane
        .register_array(
            CellDescriptor {
                key: key("origin.es_mbo.batches"),
                owner: origin,
                kind: CellKind::Array,
                public_read: false,
            },
            vec![SourceBatch { seq: 1 }, SourceBatch { seq: 2 }],
        )
        .unwrap();
    let state = data_plane
        .register_value::<ProjectionState>(
            CellDescriptor {
                key: key("projection.candles_1m.state"),
                owner: projection_id.owner(),
                kind: CellKind::Value,
                public_read: false,
            },
            None,
        )
        .unwrap();
    let descriptor =
        ProjectionDescriptor::new(projection_id.clone(), vec![key("origin.es_mbo.batches")]);
    let mut projection: Box<dyn Projection> = Box::new(CountingProjection {
        descriptor,
        batches,
        state: state.clone(),
    });
    let mut ctx = ProjectionContext::new(&data_plane, projection_id);

    projection.run(&mut ctx).unwrap();

    assert_eq!(
        data_plane.read_value(&state).unwrap(),
        Some(ProjectionState { processed: 2 })
    );
    assert_eq!(
        ctx.into_effects().changed_keys,
        vec![key("projection.candles_1m.state")]
    );
}
