use std::ops::Range;

use cache::{ArrayKey, Cache, CacheError, CellOwner, Key, ValueKey, WriteEffects};
use tokio::sync::mpsc;

use crate::RuntimeError;

pub type ExternalWriteReceiver = mpsc::Receiver<ExternalWriteBatch>;

#[derive(Clone)]
pub struct ExternalWriteSink {
    tx: mpsc::Sender<ExternalWriteBatch>,
}

impl ExternalWriteSink {
    pub fn channel(capacity: usize) -> (Self, ExternalWriteReceiver) {
        let (tx, rx) = mpsc::channel(capacity);
        (Self { tx }, rx)
    }

    pub async fn submit(&self, batch: ExternalWriteBatch) -> Result<(), RuntimeError> {
        if batch.is_empty() {
            return Ok(());
        }

        self.tx
            .send(batch)
            .await
            .map_err(|_| RuntimeError::RuntimeIngressClosed)
    }

    pub fn is_closed(&self) -> bool {
        self.tx.is_closed()
    }
}

pub struct ExternalWriteBatch {
    writer: CellOwner,
    operations: Vec<Box<dyn ExternalWriteOperation>>,
}

impl ExternalWriteBatch {
    pub fn new(writer: CellOwner) -> Self {
        Self {
            writer,
            operations: Vec::new(),
        }
    }

    pub fn writer(&self) -> &CellOwner {
        &self.writer
    }

    pub fn len(&self) -> usize {
        self.operations.len()
    }

    pub fn is_empty(&self) -> bool {
        self.operations.is_empty()
    }

    pub fn set_value<T>(&mut self, key: &ValueKey<T>, value: T) -> &mut Self
    where
        T: Clone + Send + Sync + 'static,
    {
        self.operations.push(Box::new(SetValue {
            key: key.clone(),
            value,
        }));
        self
    }

    pub fn clear_value<T>(&mut self, key: &ValueKey<T>) -> &mut Self
    where
        T: Clone + Send + Sync + 'static,
    {
        self.operations
            .push(Box::new(ClearValue { key: key.clone() }));
        self
    }

    pub fn replace_array<T>(&mut self, key: &ArrayKey<T>, items: Vec<T>) -> &mut Self
    where
        T: Clone + Send + Sync + 'static,
    {
        self.operations.push(Box::new(ReplaceArray {
            key: key.clone(),
            items,
        }));
        self
    }

    pub fn push_array<T>(&mut self, key: &ArrayKey<T>, items: Vec<T>) -> &mut Self
    where
        T: Clone + Send + Sync + 'static,
    {
        self.operations.push(Box::new(PushArray {
            key: key.clone(),
            items,
        }));
        self
    }

    pub fn insert_array<T>(&mut self, key: &ArrayKey<T>, index: usize, items: Vec<T>) -> &mut Self
    where
        T: Clone + Send + Sync + 'static,
    {
        self.operations.push(Box::new(InsertArray {
            key: key.clone(),
            index,
            items,
        }));
        self
    }

    pub fn replace_array_range<T>(
        &mut self,
        key: &ArrayKey<T>,
        range: Range<usize>,
        items: Vec<T>,
    ) -> &mut Self
    where
        T: Clone + Send + Sync + 'static,
    {
        self.operations.push(Box::new(ReplaceArrayRange {
            key: key.clone(),
            range,
            items,
        }));
        self
    }

    pub fn remove_array_range<T>(&mut self, key: &ArrayKey<T>, range: Range<usize>) -> &mut Self
    where
        T: Clone + Send + Sync + 'static,
    {
        self.operations.push(Box::new(RemoveArrayRange {
            key: key.clone(),
            range,
        }));
        self
    }

    pub fn clear_array<T>(&mut self, key: &ArrayKey<T>) -> &mut Self
    where
        T: Clone + Send + Sync + 'static,
    {
        self.operations
            .push(Box::new(ClearArray { key: key.clone() }));
        self
    }

    pub(crate) fn apply(self, cache: &Cache) -> (Result<(), CacheError>, WriteEffects) {
        let Self { writer, operations } = self;
        let mut ctx = ExternalWriteContext::new(cache, writer);
        let result = Self::apply_operations(operations, &mut ctx);
        let effects = ctx.into_effects();
        (result, effects)
    }

    fn apply_operations(
        operations: Vec<Box<dyn ExternalWriteOperation>>,
        ctx: &mut ExternalWriteContext<'_>,
    ) -> Result<(), CacheError> {
        for operation in operations {
            operation.apply(ctx)?;
        }
        Ok(())
    }
}

trait ExternalWriteOperation: Send {
    fn apply(self: Box<Self>, ctx: &mut ExternalWriteContext<'_>) -> Result<(), CacheError>;
}

pub(crate) struct ExternalWriteContext<'a> {
    cache: &'a Cache,
    writer: CellOwner,
    changed_keys: Vec<Key>,
}

impl<'a> ExternalWriteContext<'a> {
    pub(crate) fn new(cache: &'a Cache, writer: CellOwner) -> Self {
        Self {
            cache,
            writer,
            changed_keys: Vec::new(),
        }
    }

    pub(crate) fn into_effects(self) -> WriteEffects {
        WriteEffects {
            changed_keys: self.changed_keys,
        }
    }

    pub(crate) fn set_value<T>(&mut self, key: &ValueKey<T>, value: T) -> Result<(), CacheError>
    where
        T: Clone + Send + Sync + 'static,
    {
        let effects = self.cache.set_value(&self.writer, key, value)?;
        self.record_effects(effects);
        Ok(())
    }

    pub(crate) fn clear_value<T>(&mut self, key: &ValueKey<T>) -> Result<(), CacheError>
    where
        T: Clone + Send + Sync + 'static,
    {
        let effects = self.cache.clear_value(&self.writer, key)?;
        self.record_effects(effects);
        Ok(())
    }

    pub(crate) fn replace_array<T>(
        &mut self,
        key: &ArrayKey<T>,
        items: Vec<T>,
    ) -> Result<(), CacheError>
    where
        T: Clone + Send + Sync + 'static,
    {
        let effects = self.cache.replace_array(&self.writer, key, items)?;
        self.record_effects(effects);
        Ok(())
    }

    pub(crate) fn push_array<T>(
        &mut self,
        key: &ArrayKey<T>,
        items: Vec<T>,
    ) -> Result<(), CacheError>
    where
        T: Clone + Send + Sync + 'static,
    {
        let effects = self.cache.push_array(&self.writer, key, items)?;
        self.record_effects(effects);
        Ok(())
    }

    pub(crate) fn insert_array<T>(
        &mut self,
        key: &ArrayKey<T>,
        index: usize,
        items: Vec<T>,
    ) -> Result<(), CacheError>
    where
        T: Clone + Send + Sync + 'static,
    {
        let effects = self.cache.insert_array(&self.writer, key, index, items)?;
        self.record_effects(effects);
        Ok(())
    }

    pub(crate) fn replace_array_range<T>(
        &mut self,
        key: &ArrayKey<T>,
        range: Range<usize>,
        items: Vec<T>,
    ) -> Result<(), CacheError>
    where
        T: Clone + Send + Sync + 'static,
    {
        let effects = self
            .cache
            .replace_array_range(&self.writer, key, range, items)?;
        self.record_effects(effects);
        Ok(())
    }

    pub(crate) fn remove_array_range<T>(
        &mut self,
        key: &ArrayKey<T>,
        range: Range<usize>,
    ) -> Result<(), CacheError>
    where
        T: Clone + Send + Sync + 'static,
    {
        let effects = self.cache.remove_array_range(&self.writer, key, range)?;
        self.record_effects(effects);
        Ok(())
    }

    pub(crate) fn clear_array<T>(&mut self, key: &ArrayKey<T>) -> Result<(), CacheError>
    where
        T: Clone + Send + Sync + 'static,
    {
        let effects = self.cache.clear_array(&self.writer, key)?;
        self.record_effects(effects);
        Ok(())
    }

    fn record_effects(&mut self, effects: WriteEffects) {
        for key in effects.changed_keys {
            if !self.changed_keys.contains(&key) {
                self.changed_keys.push(key);
            }
        }
    }
}

struct SetValue<T> {
    key: ValueKey<T>,
    value: T,
}

impl<T> ExternalWriteOperation for SetValue<T>
where
    T: Clone + Send + Sync + 'static,
{
    fn apply(self: Box<Self>, ctx: &mut ExternalWriteContext<'_>) -> Result<(), CacheError> {
        ctx.set_value(&self.key, self.value)
    }
}

struct ClearValue<T> {
    key: ValueKey<T>,
}

impl<T> ExternalWriteOperation for ClearValue<T>
where
    T: Clone + Send + Sync + 'static,
{
    fn apply(self: Box<Self>, ctx: &mut ExternalWriteContext<'_>) -> Result<(), CacheError> {
        ctx.clear_value(&self.key)
    }
}

struct ReplaceArray<T> {
    key: ArrayKey<T>,
    items: Vec<T>,
}

impl<T> ExternalWriteOperation for ReplaceArray<T>
where
    T: Clone + Send + Sync + 'static,
{
    fn apply(self: Box<Self>, ctx: &mut ExternalWriteContext<'_>) -> Result<(), CacheError> {
        ctx.replace_array(&self.key, self.items)
    }
}

struct PushArray<T> {
    key: ArrayKey<T>,
    items: Vec<T>,
}

impl<T> ExternalWriteOperation for PushArray<T>
where
    T: Clone + Send + Sync + 'static,
{
    fn apply(self: Box<Self>, ctx: &mut ExternalWriteContext<'_>) -> Result<(), CacheError> {
        ctx.push_array(&self.key, self.items)
    }
}

struct InsertArray<T> {
    key: ArrayKey<T>,
    index: usize,
    items: Vec<T>,
}

impl<T> ExternalWriteOperation for InsertArray<T>
where
    T: Clone + Send + Sync + 'static,
{
    fn apply(self: Box<Self>, ctx: &mut ExternalWriteContext<'_>) -> Result<(), CacheError> {
        ctx.insert_array(&self.key, self.index, self.items)
    }
}

struct ReplaceArrayRange<T> {
    key: ArrayKey<T>,
    range: Range<usize>,
    items: Vec<T>,
}

impl<T> ExternalWriteOperation for ReplaceArrayRange<T>
where
    T: Clone + Send + Sync + 'static,
{
    fn apply(self: Box<Self>, ctx: &mut ExternalWriteContext<'_>) -> Result<(), CacheError> {
        ctx.replace_array_range(&self.key, self.range, self.items)
    }
}

struct RemoveArrayRange<T> {
    key: ArrayKey<T>,
    range: Range<usize>,
}

impl<T> ExternalWriteOperation for RemoveArrayRange<T>
where
    T: Clone + Send + Sync + 'static,
{
    fn apply(self: Box<Self>, ctx: &mut ExternalWriteContext<'_>) -> Result<(), CacheError> {
        ctx.remove_array_range(&self.key, self.range)
    }
}

struct ClearArray<T> {
    key: ArrayKey<T>,
}

impl<T> ExternalWriteOperation for ClearArray<T>
where
    T: Clone + Send + Sync + 'static,
{
    fn apply(self: Box<Self>, ctx: &mut ExternalWriteContext<'_>) -> Result<(), CacheError> {
        ctx.clear_array(&self.key)
    }
}
