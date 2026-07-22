use std::cell::RefCell;

use cache::{ArrayKey, Cache, CacheSchemaBatch, CellDescriptor, CellOwner, Key, ValueKey};

use crate::RuntimeError;

/// Narrow cache-schema capability available only inside a Runtime owner
/// command. It cannot read or mutate cell payloads.
pub struct CacheSchema<'a> {
    cache: &'a Cache,
    batch: RefCell<CacheSchemaBatch>,
}

impl<'a> CacheSchema<'a> {
    pub(crate) fn new(cache: &'a Cache) -> Self {
        Self {
            cache,
            batch: RefCell::new(cache.schema_batch()),
        }
    }

    pub fn register_value<T>(
        &self,
        descriptor: CellDescriptor,
        initial: Option<T>,
    ) -> Result<ValueKey<T>, RuntimeError>
    where
        T: Clone + Send + Sync + 'static,
    {
        Ok(self
            .batch
            .borrow_mut()
            .register_value(descriptor, initial)?)
    }

    pub fn register_array<T>(
        &self,
        descriptor: CellDescriptor,
        initial: Vec<T>,
    ) -> Result<ArrayKey<T>, RuntimeError>
    where
        T: Clone + Send + Sync + 'static,
    {
        Ok(self
            .batch
            .borrow_mut()
            .register_array(descriptor, initial)?)
    }

    pub fn unregister_owned(
        &self,
        owner: &CellOwner,
        keys: &[Key],
    ) -> Result<Vec<CellDescriptor>, RuntimeError> {
        Ok(self
            .batch
            .borrow_mut()
            .unregister_owned(self.cache, owner, keys)?)
    }

    pub(crate) fn commit(self) -> Result<Vec<CellDescriptor>, RuntimeError> {
        Ok(self.cache.commit_schema_batch(self.batch.into_inner())?)
    }
}
