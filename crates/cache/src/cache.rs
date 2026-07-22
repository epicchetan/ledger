use std::{
    any::Any,
    collections::HashMap,
    ops::Range,
    sync::{Arc, RwLock},
};

use tokio::sync::watch;

use crate::{
    cell::{ArrayStorage, ValueStorage},
    ArrayKey, CacheError, CellDescriptor, CellKind, CellOwner, CellWatch, Key, ValueKey,
    WriteEffects,
};

#[derive(Clone)]
pub struct Cache {
    inner: Arc<CacheInner>,
}

/// A cloneable, read-only capability for a cache owned by a runtime.
#[derive(Clone)]
pub struct CacheReader {
    inner: Arc<CacheInner>,
}

/// An immutable view used by owner-executed runtime snapshots.
///
/// Atomicity comes from the runtime invoking the reader between committed
/// steps; the view deliberately exposes no mutation or watch registration.
pub struct CacheReadView<'a> {
    cache: &'a Cache,
}

/// Staged cache-registry changes committed atomically by the Runtime owner.
/// Payload mutation remains outside this schema-only transaction.
pub struct CacheSchemaBatch {
    removals: Vec<(CellOwner, Key)>,
    additions: HashMap<Key, Arc<Cell>>,
}

struct CacheInner {
    cells: RwLock<HashMap<Key, Arc<Cell>>>,
}

struct Cell {
    descriptor: CellDescriptor,
    /// The channel is the generation counter; subscribing yields a watch.
    generation: watch::Sender<u64>,
    storage: RwLock<Box<dyn Any + Send + Sync>>,
}

impl Cache {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(CacheInner {
                cells: RwLock::new(HashMap::new()),
            }),
        }
    }

    pub fn reader(&self) -> CacheReader {
        CacheReader {
            inner: self.inner.clone(),
        }
    }

    pub fn read_view(&self) -> CacheReadView<'_> {
        CacheReadView { cache: self }
    }

    pub fn schema_batch(&self) -> CacheSchemaBatch {
        CacheSchemaBatch {
            removals: Vec::new(),
            additions: HashMap::new(),
        }
    }

    /// Validate and publish a complete schema replacement under one registry
    /// write lock. No reader or watcher can observe a partially changed key
    /// set, and validation failure leaves the registry untouched.
    pub fn commit_schema_batch(
        &self,
        batch: CacheSchemaBatch,
    ) -> Result<Vec<CellDescriptor>, CacheError> {
        let mut cells = self
            .inner
            .cells
            .write()
            .map_err(|_| CacheError::RegistryLockPoisoned)?;
        let mut removed_keys = Vec::with_capacity(batch.removals.len());
        let mut removed_descriptors = Vec::with_capacity(batch.removals.len());
        for (owner, key) in &batch.removals {
            let cell = cells
                .get(key)
                .ok_or_else(|| CacheError::MissingCell(key.clone()))?;
            self.ensure_owner(cell, owner, key)?;
            if !removed_keys.contains(key) {
                removed_keys.push(key.clone());
                removed_descriptors.push(cell.descriptor.clone());
            }
        }
        for key in batch.additions.keys() {
            if cells.contains_key(key) && !removed_keys.contains(key) {
                return Err(CacheError::DuplicateCell(key.clone()));
            }
        }

        for key in removed_keys {
            cells.remove(&key);
        }
        for (key, cell) in batch.additions {
            cells.insert(key, cell);
        }
        Ok(removed_descriptors)
    }

    pub fn register_value<T>(
        &self,
        descriptor: CellDescriptor,
        initial: Option<T>,
    ) -> Result<ValueKey<T>, CacheError>
    where
        T: Clone + Send + Sync + 'static,
    {
        self.ensure_descriptor_kind(&descriptor, CellKind::Value)?;
        let key = descriptor.key.clone();
        let cell = Arc::new(Cell {
            descriptor,
            generation: watch::Sender::new(0),
            storage: RwLock::new(Box::new(ValueStorage { value: initial })),
        });

        self.insert_cell(key.clone(), cell)?;
        Ok(ValueKey::new(key))
    }

    pub fn register_array<T>(
        &self,
        descriptor: CellDescriptor,
        initial: Vec<T>,
    ) -> Result<ArrayKey<T>, CacheError>
    where
        T: Clone + Send + Sync + 'static,
    {
        self.ensure_descriptor_kind(&descriptor, CellKind::Array)?;
        let key = descriptor.key.clone();
        let cell = Arc::new(Cell {
            descriptor,
            generation: watch::Sender::new(0),
            storage: RwLock::new(Box::new(ArrayStorage { items: initial })),
        });

        self.insert_cell(key.clone(), cell)?;
        Ok(ArrayKey::new(key))
    }

    pub fn describe(&self, key: &Key) -> Result<CellDescriptor, CacheError> {
        Ok(self.lookup_cell(key)?.descriptor.clone())
    }

    /// Watch a registered cell for changes. Errors on an unregistered key.
    /// Follows the same visibility rules as reads.
    pub fn watch_key(&self, key: &Key) -> Result<CellWatch, CacheError> {
        let cell = self.lookup_cell(key)?;
        Ok(CellWatch::new(key.clone(), cell.generation.subscribe()))
    }

    /// Remove registered cells owned by `owner` as one registry mutation.
    ///
    /// Every key and owner is validated before anything is removed. Existing
    /// watches close when the removed cells' senders are dropped, and the keys
    /// may be registered again afterward.
    pub fn unregister_owned(
        &self,
        owner: &CellOwner,
        keys: &[Key],
    ) -> Result<Vec<CellDescriptor>, CacheError> {
        let mut cells = self
            .inner
            .cells
            .write()
            .map_err(|_| CacheError::RegistryLockPoisoned)?;
        let mut unique = Vec::new();
        for key in keys {
            if unique.contains(key) {
                continue;
            }
            let cell = cells
                .get(key)
                .ok_or_else(|| CacheError::MissingCell(key.clone()))?;
            self.ensure_owner(cell, owner, key)?;
            unique.push(key.clone());
        }

        let mut removed = Vec::with_capacity(unique.len());
        for key in unique {
            let cell = cells
                .remove(&key)
                .expect("unregister keys were validated before mutation");
            removed.push(cell.descriptor.clone());
        }
        Ok(removed)
    }

    pub fn read_value<T>(&self, key: &ValueKey<T>) -> Result<Option<T>, CacheError>
    where
        T: Clone + Send + Sync + 'static,
    {
        let cell = self.lookup_cell(key.key())?;
        self.ensure_cell_kind(&cell, key.key(), CellKind::Value)?;
        let storage = cell
            .storage
            .read()
            .map_err(|_| CacheError::CellLockPoisoned {
                key: key.key().clone(),
            })?;
        let storage = storage
            .as_ref()
            .downcast_ref::<ValueStorage<T>>()
            .ok_or_else(|| CacheError::TypeMismatch(key.key().clone()))?;
        Ok(storage.value.clone())
    }

    pub fn read_array<T>(&self, key: &ArrayKey<T>) -> Result<Vec<T>, CacheError>
    where
        T: Clone + Send + Sync + 'static,
    {
        let cell = self.lookup_cell(key.key())?;
        self.ensure_cell_kind(&cell, key.key(), CellKind::Array)?;
        let storage = cell
            .storage
            .read()
            .map_err(|_| CacheError::CellLockPoisoned {
                key: key.key().clone(),
            })?;
        let storage = storage
            .as_ref()
            .downcast_ref::<ArrayStorage<T>>()
            .ok_or_else(|| CacheError::TypeMismatch(key.key().clone()))?;
        Ok(storage.items.clone())
    }

    pub fn read_array_range<T>(
        &self,
        key: &ArrayKey<T>,
        range: Range<usize>,
    ) -> Result<Vec<T>, CacheError>
    where
        T: Clone + Send + Sync + 'static,
    {
        let cell = self.lookup_cell(key.key())?;
        self.ensure_cell_kind(&cell, key.key(), CellKind::Array)?;
        let storage = cell
            .storage
            .read()
            .map_err(|_| CacheError::CellLockPoisoned {
                key: key.key().clone(),
            })?;
        let storage = storage
            .as_ref()
            .downcast_ref::<ArrayStorage<T>>()
            .ok_or_else(|| CacheError::TypeMismatch(key.key().clone()))?;
        validate_range(key.key(), &range, storage.items.len())?;
        Ok(storage.items[range].to_vec())
    }

    pub fn set_value<T>(
        &self,
        writer: &CellOwner,
        key: &ValueKey<T>,
        value: T,
    ) -> Result<WriteEffects, CacheError>
    where
        T: Clone + Send + Sync + 'static,
    {
        self.with_value_storage(writer, key, |storage| {
            storage.value = Some(value);
            Ok(())
        })?;
        Ok(WriteEffects::changed(key.key()))
    }

    pub fn clear_value<T>(
        &self,
        writer: &CellOwner,
        key: &ValueKey<T>,
    ) -> Result<WriteEffects, CacheError>
    where
        T: Clone + Send + Sync + 'static,
    {
        self.with_value_storage(writer, key, |storage| {
            storage.value = None;
            Ok(())
        })?;
        Ok(WriteEffects::changed(key.key()))
    }

    pub fn update_value<T, R>(
        &self,
        writer: &CellOwner,
        key: &ValueKey<T>,
        update: impl FnOnce(&mut Option<T>) -> R,
    ) -> Result<(R, WriteEffects), CacheError>
    where
        T: Clone + Send + Sync + 'static,
    {
        let result =
            self.with_value_storage(writer, key, |storage| Ok(update(&mut storage.value)))?;
        Ok((result, WriteEffects::changed(key.key())))
    }

    pub fn replace_array<T>(
        &self,
        writer: &CellOwner,
        key: &ArrayKey<T>,
        items: Vec<T>,
    ) -> Result<WriteEffects, CacheError>
    where
        T: Clone + Send + Sync + 'static,
    {
        self.with_array_storage(writer, key, |storage| {
            storage.items = items;
            Ok(())
        })?;
        Ok(WriteEffects::changed(key.key()))
    }

    pub fn push_array<T>(
        &self,
        writer: &CellOwner,
        key: &ArrayKey<T>,
        items: Vec<T>,
    ) -> Result<WriteEffects, CacheError>
    where
        T: Clone + Send + Sync + 'static,
    {
        self.with_array_storage(writer, key, |storage| {
            storage.items.extend(items);
            Ok(())
        })?;
        Ok(WriteEffects::changed(key.key()))
    }

    pub fn insert_array<T>(
        &self,
        writer: &CellOwner,
        key: &ArrayKey<T>,
        index: usize,
        items: Vec<T>,
    ) -> Result<WriteEffects, CacheError>
    where
        T: Clone + Send + Sync + 'static,
    {
        self.with_array_storage(writer, key, |storage| {
            validate_insert_index(key.key(), index, storage.items.len())?;
            storage.items.splice(index..index, items);
            Ok(())
        })?;
        Ok(WriteEffects::changed(key.key()))
    }

    pub fn replace_array_range<T>(
        &self,
        writer: &CellOwner,
        key: &ArrayKey<T>,
        range: Range<usize>,
        items: Vec<T>,
    ) -> Result<WriteEffects, CacheError>
    where
        T: Clone + Send + Sync + 'static,
    {
        self.with_array_storage(writer, key, |storage| {
            validate_range(key.key(), &range, storage.items.len())?;
            storage.items.splice(range, items);
            Ok(())
        })?;
        Ok(WriteEffects::changed(key.key()))
    }

    pub fn remove_array_range<T>(
        &self,
        writer: &CellOwner,
        key: &ArrayKey<T>,
        range: Range<usize>,
    ) -> Result<WriteEffects, CacheError>
    where
        T: Clone + Send + Sync + 'static,
    {
        self.with_array_storage(writer, key, |storage| {
            validate_range(key.key(), &range, storage.items.len())?;
            storage.items.drain(range);
            Ok(())
        })?;
        Ok(WriteEffects::changed(key.key()))
    }

    pub fn clear_array<T>(
        &self,
        writer: &CellOwner,
        key: &ArrayKey<T>,
    ) -> Result<WriteEffects, CacheError>
    where
        T: Clone + Send + Sync + 'static,
    {
        self.with_array_storage(writer, key, |storage| {
            storage.items.clear();
            Ok(())
        })?;
        Ok(WriteEffects::changed(key.key()))
    }

    pub fn update_array<T, R>(
        &self,
        writer: &CellOwner,
        key: &ArrayKey<T>,
        update: impl FnOnce(&mut Vec<T>) -> R,
    ) -> Result<(R, WriteEffects), CacheError>
    where
        T: Clone + Send + Sync + 'static,
    {
        let result =
            self.with_array_storage(writer, key, |storage| Ok(update(&mut storage.items)))?;
        Ok((result, WriteEffects::changed(key.key())))
    }

    fn insert_cell(&self, key: Key, cell: Arc<Cell>) -> Result<(), CacheError> {
        let mut cells = self
            .inner
            .cells
            .write()
            .map_err(|_| CacheError::RegistryLockPoisoned)?;
        if cells.contains_key(&key) {
            return Err(CacheError::DuplicateCell(key));
        }
        cells.insert(key, cell);
        Ok(())
    }

    fn lookup_cell(&self, key: &Key) -> Result<Arc<Cell>, CacheError> {
        let cells = self
            .inner
            .cells
            .read()
            .map_err(|_| CacheError::RegistryLockPoisoned)?;
        cells
            .get(key)
            .cloned()
            .ok_or_else(|| CacheError::MissingCell(key.clone()))
    }

    fn ensure_descriptor_kind(
        &self,
        descriptor: &CellDescriptor,
        expected: CellKind,
    ) -> Result<(), CacheError> {
        if descriptor.kind == expected {
            Ok(())
        } else {
            Err(CacheError::WrongCellKind {
                key: descriptor.key.clone(),
                expected,
                found: descriptor.kind,
            })
        }
    }

    fn ensure_cell_kind(
        &self,
        cell: &Cell,
        key: &Key,
        expected: CellKind,
    ) -> Result<(), CacheError> {
        if cell.descriptor.kind == expected {
            Ok(())
        } else {
            Err(CacheError::WrongCellKind {
                key: key.clone(),
                expected,
                found: cell.descriptor.kind,
            })
        }
    }

    fn ensure_owner(&self, cell: &Cell, writer: &CellOwner, key: &Key) -> Result<(), CacheError> {
        if &cell.descriptor.owner == writer {
            Ok(())
        } else {
            Err(CacheError::OwnerMismatch {
                key: key.clone(),
                writer: writer.clone(),
                owner: cell.descriptor.owner.clone(),
            })
        }
    }

    fn with_value_storage<T, R>(
        &self,
        writer: &CellOwner,
        key: &ValueKey<T>,
        update: impl FnOnce(&mut ValueStorage<T>) -> Result<R, CacheError>,
    ) -> Result<R, CacheError>
    where
        T: Clone + Send + Sync + 'static,
    {
        let cell = self.lookup_cell(key.key())?;
        self.ensure_cell_kind(&cell, key.key(), CellKind::Value)?;
        self.ensure_owner(&cell, writer, key.key())?;
        let mut storage = cell
            .storage
            .write()
            .map_err(|_| CacheError::CellLockPoisoned {
                key: key.key().clone(),
            })?;
        let storage = storage
            .as_mut()
            .downcast_mut::<ValueStorage<T>>()
            .ok_or_else(|| CacheError::TypeMismatch(key.key().clone()))?;
        let result = update(storage)?;
        // Bumped only on success, before the storage lock releases, so a
        // woken watcher can never read pre-write state.
        cell.generation.send_modify(|generation| *generation += 1);
        Ok(result)
    }

    fn with_array_storage<T, R>(
        &self,
        writer: &CellOwner,
        key: &ArrayKey<T>,
        update: impl FnOnce(&mut ArrayStorage<T>) -> Result<R, CacheError>,
    ) -> Result<R, CacheError>
    where
        T: Clone + Send + Sync + 'static,
    {
        let cell = self.lookup_cell(key.key())?;
        self.ensure_cell_kind(&cell, key.key(), CellKind::Array)?;
        self.ensure_owner(&cell, writer, key.key())?;
        let mut storage = cell
            .storage
            .write()
            .map_err(|_| CacheError::CellLockPoisoned {
                key: key.key().clone(),
            })?;
        let storage = storage
            .as_mut()
            .downcast_mut::<ArrayStorage<T>>()
            .ok_or_else(|| CacheError::TypeMismatch(key.key().clone()))?;
        let result = update(storage)?;
        // Bumped only on success, before the storage lock releases, so a
        // woken watcher can never read pre-write state.
        cell.generation.send_modify(|generation| *generation += 1);
        Ok(result)
    }
}

impl CacheSchemaBatch {
    pub fn register_value<T>(
        &mut self,
        descriptor: CellDescriptor,
        initial: Option<T>,
    ) -> Result<ValueKey<T>, CacheError>
    where
        T: Clone + Send + Sync + 'static,
    {
        ensure_descriptor_kind(&descriptor, CellKind::Value)?;
        let key = descriptor.key.clone();
        let cell = Arc::new(Cell {
            descriptor,
            generation: watch::Sender::new(0),
            storage: RwLock::new(Box::new(ValueStorage { value: initial })),
        });
        self.insert_addition(key.clone(), cell)?;
        Ok(ValueKey::new(key))
    }

    pub fn register_array<T>(
        &mut self,
        descriptor: CellDescriptor,
        initial: Vec<T>,
    ) -> Result<ArrayKey<T>, CacheError>
    where
        T: Clone + Send + Sync + 'static,
    {
        ensure_descriptor_kind(&descriptor, CellKind::Array)?;
        let key = descriptor.key.clone();
        let cell = Arc::new(Cell {
            descriptor,
            generation: watch::Sender::new(0),
            storage: RwLock::new(Box::new(ArrayStorage { items: initial })),
        });
        self.insert_addition(key.clone(), cell)?;
        Ok(ArrayKey::new(key))
    }

    pub fn unregister_owned(
        &mut self,
        cache: &Cache,
        owner: &CellOwner,
        keys: &[Key],
    ) -> Result<Vec<CellDescriptor>, CacheError> {
        let mut descriptors = Vec::new();
        for key in keys {
            let descriptor = cache.describe(key)?;
            if &descriptor.owner != owner {
                return Err(CacheError::OwnerMismatch {
                    key: key.clone(),
                    writer: owner.clone(),
                    owner: descriptor.owner,
                });
            }
            if !self.removals.iter().any(|(_, existing)| existing == key) {
                self.removals.push((owner.clone(), key.clone()));
                descriptors.push(descriptor);
            }
        }
        Ok(descriptors)
    }

    fn insert_addition(&mut self, key: Key, cell: Arc<Cell>) -> Result<(), CacheError> {
        if self.additions.contains_key(&key) {
            return Err(CacheError::DuplicateCell(key));
        }
        self.additions.insert(key, cell);
        Ok(())
    }
}

fn ensure_descriptor_kind(
    descriptor: &CellDescriptor,
    expected: CellKind,
) -> Result<(), CacheError> {
    if descriptor.kind == expected {
        Ok(())
    } else {
        Err(CacheError::WrongCellKind {
            key: descriptor.key.clone(),
            expected,
            found: descriptor.kind,
        })
    }
}

impl CacheReader {
    pub fn describe(&self, key: &Key) -> Result<CellDescriptor, CacheError> {
        self.as_cache().describe(key)
    }

    pub fn watch_key(&self, key: &Key) -> Result<CellWatch, CacheError> {
        self.as_cache().watch_key(key)
    }

    pub fn read_value<T>(&self, key: &ValueKey<T>) -> Result<Option<T>, CacheError>
    where
        T: Clone + Send + Sync + 'static,
    {
        self.as_cache().read_value(key)
    }

    pub fn read_array<T>(&self, key: &ArrayKey<T>) -> Result<Vec<T>, CacheError>
    where
        T: Clone + Send + Sync + 'static,
    {
        self.as_cache().read_array(key)
    }

    pub fn read_array_range<T>(
        &self,
        key: &ArrayKey<T>,
        range: Range<usize>,
    ) -> Result<Vec<T>, CacheError>
    where
        T: Clone + Send + Sync + 'static,
    {
        self.as_cache().read_array_range(key, range)
    }

    fn as_cache(&self) -> Cache {
        Cache {
            inner: self.inner.clone(),
        }
    }
}

impl CacheReadView<'_> {
    pub fn describe(&self, key: &Key) -> Result<CellDescriptor, CacheError> {
        self.cache.describe(key)
    }

    pub fn read_value<T>(&self, key: &ValueKey<T>) -> Result<Option<T>, CacheError>
    where
        T: Clone + Send + Sync + 'static,
    {
        self.cache.read_value(key)
    }

    pub fn read_array<T>(&self, key: &ArrayKey<T>) -> Result<Vec<T>, CacheError>
    where
        T: Clone + Send + Sync + 'static,
    {
        self.cache.read_array(key)
    }

    pub fn read_array_range<T>(
        &self,
        key: &ArrayKey<T>,
        range: Range<usize>,
    ) -> Result<Vec<T>, CacheError>
    where
        T: Clone + Send + Sync + 'static,
    {
        self.cache.read_array_range(key, range)
    }
}

impl Default for Cache {
    fn default() -> Self {
        Self::new()
    }
}

fn validate_range(key: &Key, range: &Range<usize>, len: usize) -> Result<(), CacheError> {
    if range.start <= range.end && range.end <= len {
        Ok(())
    } else {
        Err(CacheError::ArrayRangeOutOfBounds { key: key.clone() })
    }
}

fn validate_insert_index(key: &Key, index: usize, len: usize) -> Result<(), CacheError> {
    if index <= len {
        Ok(())
    } else {
        Err(CacheError::ArrayRangeOutOfBounds { key: key.clone() })
    }
}
