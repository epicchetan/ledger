use std::marker::PhantomData;

use crate::Key;

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum CellOwner {
    Runtime,
    Origin(String),
    Projection(String),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CellKind {
    Value,
    Array,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct CellDescriptor {
    pub key: Key,
    pub owner: CellOwner,
    pub kind: CellKind,
    pub public_read: bool,
}

#[derive(Debug, Clone)]
pub struct ValueKey<T> {
    pub(crate) key: Key,
    _marker: PhantomData<fn() -> T>,
}

impl<T> ValueKey<T> {
    pub fn key(&self) -> &Key {
        &self.key
    }

    pub(crate) fn new(key: Key) -> Self {
        Self {
            key,
            _marker: PhantomData,
        }
    }
}

#[derive(Debug, Clone)]
pub struct ArrayKey<T> {
    pub(crate) key: Key,
    _marker: PhantomData<fn() -> T>,
}

impl<T> ArrayKey<T> {
    pub fn key(&self) -> &Key {
        &self.key
    }

    pub(crate) fn new(key: Key) -> Self {
        Self {
            key,
            _marker: PhantomData,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct WriteEffects {
    pub changed_keys: Vec<Key>,
}

impl WriteEffects {
    pub(crate) fn changed(key: &Key) -> Self {
        Self {
            changed_keys: vec![key.clone()],
        }
    }
}

pub(crate) struct ValueStorage<T> {
    pub(crate) value: Option<T>,
}

pub(crate) struct ArrayStorage<T> {
    pub(crate) items: Vec<T>,
}
