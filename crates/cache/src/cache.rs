use std::{
    any::Any,
    collections::HashMap,
    ops::Range,
    sync::{Arc, RwLock},
};

use crate::{
    cell::{ArrayStorage, ValueStorage},
    ArrayKey, CacheError, CellDescriptor, CellKind, CellOwner, Key, ValueKey, WriteEffects,
};

#[derive(Clone)]
pub struct Cache {
    inner: Arc<CacheInner>,
}

struct CacheInner {
    cells: RwLock<HashMap<Key, Arc<Cell>>>,
}

struct Cell {
    descriptor: CellDescriptor,
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
            storage: RwLock::new(Box::new(ArrayStorage { items: initial })),
        });

        self.insert_cell(key.clone(), cell)?;
        Ok(ArrayKey::new(key))
    }

    pub fn describe(&self, key: &Key) -> Result<CellDescriptor, CacheError> {
        Ok(self.lookup_cell(key)?.descriptor.clone())
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
        let result = self.with_value_storage(writer, key, |storage| update(&mut storage.value))?;
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
        })??;
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
        })??;
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
        })??;
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
        let result = self.with_array_storage(writer, key, |storage| update(&mut storage.items))?;
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
        update: impl FnOnce(&mut ValueStorage<T>) -> R,
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
        Ok(update(storage))
    }

    fn with_array_storage<T, R>(
        &self,
        writer: &CellOwner,
        key: &ArrayKey<T>,
        update: impl FnOnce(&mut ArrayStorage<T>) -> R,
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
        Ok(update(storage))
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
