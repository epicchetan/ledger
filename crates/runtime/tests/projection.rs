use cache::{ArrayKey, Cache, CacheError, CellDescriptor, CellKind, CellOwner, Key, ValueKey};
use runtime::{Projection, ProjectionContext, ProjectionDescriptor, ProjectionError, ProjectionId};

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
    assert_eq!(id.owner(), CellOwner::new("projection.candles_1m").unwrap());
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
    let descriptor =
        ProjectionDescriptor::new(projection_id(), vec![key("feed.databento.es_mbo.batches")]);

    assert_eq!(descriptor.id.as_str(), "projection.candles_1m");
    assert_eq!(
        descriptor.dependencies,
        vec![key("feed.databento.es_mbo.batches")]
    );
}

#[test]
fn projection_context_reads_inputs_and_writes_as_projection_owner() {
    let cache = Cache::new();
    let feed = CellOwner::new("feed.databento.es_mbo").unwrap();
    let projection_id = projection_id();
    let projection_owner = projection_id.owner();
    let batches = cache
        .register_array(
            CellDescriptor {
                key: key("feed.databento.es_mbo.batches"),
                owner: feed.clone(),
                kind: CellKind::Array,
                public_read: false,
            },
            vec![SourceBatch { seq: 1 }],
        )
        .unwrap();
    let state = cache
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
    let mut ctx = ProjectionContext::new(&cache, projection_id);

    assert_eq!(
        ctx.read_array(&batches).unwrap(),
        vec![SourceBatch { seq: 1 }]
    );
    ctx.set_value(&state, ProjectionState { processed: 1 })
        .unwrap();

    assert_eq!(
        cache.read_value(&state).unwrap(),
        Some(ProjectionState { processed: 1 })
    );
    assert_eq!(ctx.changed_keys(), &[key("projection.candles_1m.state")]);
}

#[test]
fn projection_context_rejects_writes_to_cells_owned_by_another_owner() {
    let cache = Cache::new();
    let feed = CellOwner::new("feed.databento.es_mbo").unwrap();
    let status = cache
        .register_value::<ProjectionState>(
            CellDescriptor {
                key: key("feed.databento.es_mbo.status"),
                owner: feed.clone(),
                kind: CellKind::Value,
                public_read: false,
            },
            None,
        )
        .unwrap();
    let mut ctx = ProjectionContext::new(&cache, projection_id());

    let err = ctx
        .set_value(&status, ProjectionState { processed: 1 })
        .unwrap_err();

    assert_eq!(
        err,
        ProjectionError::Cache(CacheError::OwnerMismatch {
            key: key("feed.databento.es_mbo.status"),
            writer: CellOwner::new("projection.candles_1m").unwrap(),
            owner: feed,
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
    let cache = Cache::new();
    let feed = CellOwner::new("feed.databento.es_mbo").unwrap();
    let projection_id = projection_id();
    let batches = cache
        .register_array(
            CellDescriptor {
                key: key("feed.databento.es_mbo.batches"),
                owner: feed,
                kind: CellKind::Array,
                public_read: false,
            },
            vec![SourceBatch { seq: 1 }, SourceBatch { seq: 2 }],
        )
        .unwrap();
    let state = cache
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
    let descriptor = ProjectionDescriptor::new(
        projection_id.clone(),
        vec![key("feed.databento.es_mbo.batches")],
    );
    let mut projection: Box<dyn Projection> = Box::new(CountingProjection {
        descriptor,
        batches,
        state: state.clone(),
    });
    let mut ctx = ProjectionContext::new(&cache, projection_id);

    projection.run(&mut ctx).unwrap();

    assert_eq!(
        cache.read_value(&state).unwrap(),
        Some(ProjectionState { processed: 2 })
    );
    assert_eq!(
        ctx.into_effects().changed_keys,
        vec![key("projection.candles_1m.state")]
    );
}
