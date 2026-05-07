use std::{fmt, ops::Range};

use thiserror::Error;

use crate::{ArrayKey, CellOwner, DataPlane, DataPlaneError, Key, ValueKey, WriteEffects};

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct ProjectionId(Key);

impl ProjectionId {
    pub fn new(value: impl Into<String>) -> Result<Self, ProjectionError> {
        let value = value.into();
        let key =
            Key::new(value.clone()).map_err(|_| ProjectionError::InvalidProjectionId(value))?;
        Ok(Self(key))
    }

    pub fn as_str(&self) -> &str {
        self.0.as_str()
    }

    pub fn owner(&self) -> CellOwner {
        CellOwner::Projection(self.as_str().to_string())
    }
}

impl fmt::Display for ProjectionId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectionDescriptor {
    pub id: ProjectionId,
    pub dependencies: Vec<Key>,
}

impl ProjectionDescriptor {
    pub fn new(id: ProjectionId, dependencies: Vec<Key>) -> Self {
        Self { id, dependencies }
    }
}

pub trait Projection: Send {
    fn descriptor(&self) -> &ProjectionDescriptor;

    fn run(&mut self, ctx: &mut ProjectionContext<'_>) -> Result<(), ProjectionError>;
}

pub struct ProjectionContext<'a> {
    data_plane: &'a DataPlane,
    projection_id: ProjectionId,
    changed_keys: Vec<Key>,
}

impl<'a> ProjectionContext<'a> {
    pub fn new(data_plane: &'a DataPlane, projection_id: ProjectionId) -> Self {
        Self {
            data_plane,
            projection_id,
            changed_keys: Vec::new(),
        }
    }

    pub fn projection_id(&self) -> &ProjectionId {
        &self.projection_id
    }

    pub fn changed_keys(&self) -> &[Key] {
        &self.changed_keys
    }

    pub fn into_effects(self) -> WriteEffects {
        WriteEffects {
            changed_keys: self.changed_keys,
        }
    }

    pub fn read_value<T>(&self, key: &ValueKey<T>) -> Result<Option<T>, ProjectionError>
    where
        T: Clone + Send + Sync + 'static,
    {
        Ok(self.data_plane.read_value(key)?)
    }

    pub fn read_array<T>(&self, key: &ArrayKey<T>) -> Result<Vec<T>, ProjectionError>
    where
        T: Clone + Send + Sync + 'static,
    {
        Ok(self.data_plane.read_array(key)?)
    }

    pub fn read_array_range<T>(
        &self,
        key: &ArrayKey<T>,
        range: Range<usize>,
    ) -> Result<Vec<T>, ProjectionError>
    where
        T: Clone + Send + Sync + 'static,
    {
        Ok(self.data_plane.read_array_range(key, range)?)
    }

    pub fn set_value<T>(&mut self, key: &ValueKey<T>, value: T) -> Result<(), ProjectionError>
    where
        T: Clone + Send + Sync + 'static,
    {
        let effects = self
            .data_plane
            .set_value(&self.projection_id.owner(), key, value)?;
        self.record_effects(effects);
        Ok(())
    }

    pub fn clear_value<T>(&mut self, key: &ValueKey<T>) -> Result<(), ProjectionError>
    where
        T: Clone + Send + Sync + 'static,
    {
        let effects = self
            .data_plane
            .clear_value(&self.projection_id.owner(), key)?;
        self.record_effects(effects);
        Ok(())
    }

    pub fn update_value<T, R>(
        &mut self,
        key: &ValueKey<T>,
        update: impl FnOnce(&mut Option<T>) -> R,
    ) -> Result<R, ProjectionError>
    where
        T: Clone + Send + Sync + 'static,
    {
        let (result, effects) =
            self.data_plane
                .update_value(&self.projection_id.owner(), key, update)?;
        self.record_effects(effects);
        Ok(result)
    }

    pub fn replace_array<T>(
        &mut self,
        key: &ArrayKey<T>,
        items: Vec<T>,
    ) -> Result<(), ProjectionError>
    where
        T: Clone + Send + Sync + 'static,
    {
        let effects = self
            .data_plane
            .replace_array(&self.projection_id.owner(), key, items)?;
        self.record_effects(effects);
        Ok(())
    }

    pub fn push_array<T>(&mut self, key: &ArrayKey<T>, items: Vec<T>) -> Result<(), ProjectionError>
    where
        T: Clone + Send + Sync + 'static,
    {
        let effects = self
            .data_plane
            .push_array(&self.projection_id.owner(), key, items)?;
        self.record_effects(effects);
        Ok(())
    }

    pub fn insert_array<T>(
        &mut self,
        key: &ArrayKey<T>,
        index: usize,
        items: Vec<T>,
    ) -> Result<(), ProjectionError>
    where
        T: Clone + Send + Sync + 'static,
    {
        let effects =
            self.data_plane
                .insert_array(&self.projection_id.owner(), key, index, items)?;
        self.record_effects(effects);
        Ok(())
    }

    pub fn replace_array_range<T>(
        &mut self,
        key: &ArrayKey<T>,
        range: Range<usize>,
        items: Vec<T>,
    ) -> Result<(), ProjectionError>
    where
        T: Clone + Send + Sync + 'static,
    {
        let effects =
            self.data_plane
                .replace_array_range(&self.projection_id.owner(), key, range, items)?;
        self.record_effects(effects);
        Ok(())
    }

    pub fn remove_array_range<T>(
        &mut self,
        key: &ArrayKey<T>,
        range: Range<usize>,
    ) -> Result<(), ProjectionError>
    where
        T: Clone + Send + Sync + 'static,
    {
        let effects =
            self.data_plane
                .remove_array_range(&self.projection_id.owner(), key, range)?;
        self.record_effects(effects);
        Ok(())
    }

    pub fn clear_array<T>(&mut self, key: &ArrayKey<T>) -> Result<(), ProjectionError>
    where
        T: Clone + Send + Sync + 'static,
    {
        let effects = self
            .data_plane
            .clear_array(&self.projection_id.owner(), key)?;
        self.record_effects(effects);
        Ok(())
    }

    pub fn update_array<T, R>(
        &mut self,
        key: &ArrayKey<T>,
        update: impl FnOnce(&mut Vec<T>) -> R,
    ) -> Result<R, ProjectionError>
    where
        T: Clone + Send + Sync + 'static,
    {
        let (result, effects) =
            self.data_plane
                .update_array(&self.projection_id.owner(), key, update)?;
        self.record_effects(effects);
        Ok(result)
    }

    fn record_effects(&mut self, effects: WriteEffects) {
        for key in effects.changed_keys {
            if !self.changed_keys.contains(&key) {
                self.changed_keys.push(key);
            }
        }
    }
}

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum ProjectionError {
    #[error("invalid projection id `{0}`")]
    InvalidProjectionId(String),

    #[error(transparent)]
    DataPlane(#[from] DataPlaneError),
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{CellDescriptor, CellKind};

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
        let descriptor =
            ProjectionDescriptor::new(projection_id(), vec![key("origin.es_mbo.batches")]);

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
}
