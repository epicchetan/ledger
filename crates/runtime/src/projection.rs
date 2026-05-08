use std::{fmt, ops::Range};

use cache::{ArrayKey, Cache, CacheError, CellOwner, Key, ValueKey, WriteEffects};
use thiserror::Error;

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
        CellOwner::new(self.as_str()).expect("projection id already validates as a cache owner")
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
    cache: &'a Cache,
    projection_id: ProjectionId,
    changed_keys: Vec<Key>,
}

impl<'a> ProjectionContext<'a> {
    pub fn new(cache: &'a Cache, projection_id: ProjectionId) -> Self {
        Self {
            cache,
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
        Ok(self.cache.read_value(key)?)
    }

    pub fn read_array<T>(&self, key: &ArrayKey<T>) -> Result<Vec<T>, ProjectionError>
    where
        T: Clone + Send + Sync + 'static,
    {
        Ok(self.cache.read_array(key)?)
    }

    pub fn read_array_range<T>(
        &self,
        key: &ArrayKey<T>,
        range: Range<usize>,
    ) -> Result<Vec<T>, ProjectionError>
    where
        T: Clone + Send + Sync + 'static,
    {
        Ok(self.cache.read_array_range(key, range)?)
    }

    pub fn set_value<T>(&mut self, key: &ValueKey<T>, value: T) -> Result<(), ProjectionError>
    where
        T: Clone + Send + Sync + 'static,
    {
        let effects = self
            .cache
            .set_value(&self.projection_id.owner(), key, value)?;
        self.record_effects(effects);
        Ok(())
    }

    pub fn clear_value<T>(&mut self, key: &ValueKey<T>) -> Result<(), ProjectionError>
    where
        T: Clone + Send + Sync + 'static,
    {
        let effects = self.cache.clear_value(&self.projection_id.owner(), key)?;
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
            self.cache
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
            .cache
            .replace_array(&self.projection_id.owner(), key, items)?;
        self.record_effects(effects);
        Ok(())
    }

    pub fn push_array<T>(&mut self, key: &ArrayKey<T>, items: Vec<T>) -> Result<(), ProjectionError>
    where
        T: Clone + Send + Sync + 'static,
    {
        let effects = self
            .cache
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
        let effects = self
            .cache
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
            self.cache
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
        let effects = self
            .cache
            .remove_array_range(&self.projection_id.owner(), key, range)?;
        self.record_effects(effects);
        Ok(())
    }

    pub fn clear_array<T>(&mut self, key: &ArrayKey<T>) -> Result<(), ProjectionError>
    where
        T: Clone + Send + Sync + 'static,
    {
        let effects = self.cache.clear_array(&self.projection_id.owner(), key)?;
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
            self.cache
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
    Cache(#[from] CacheError),
}
