use std::{
    any::Any,
    sync::{
        atomic::{AtomicU64, Ordering},
        Arc,
    },
    time::Instant,
};

use cache::{CacheError, CacheReadView};
use tokio::sync::oneshot;

use crate::RuntimeError;

pub(crate) type SnapshotValue = Box<dyn Any + Send>;
pub(crate) type BoxedSnapshotRead = Box<
    dyn for<'a> FnOnce(&CacheReadView<'a>) -> Result<SnapshotValue, CacheError> + Send + 'static,
>;

pub(crate) struct SnapshotRequest {
    pub(crate) queued_at: Instant,
    pub(crate) read: BoxedSnapshotRead,
    pub(crate) reply: oneshot::Sender<Result<SnapshotValue, RuntimeError>>,
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct SnapshotMetricsSnapshot {
    pub requests: u64,
    pub completed: u64,
    pub cancelled: u64,
    pub rejected: u64,
    pub panicked: u64,
    pub total_queue_wait_ns: u64,
    pub max_queue_wait_ns: u64,
    pub total_execution_ns: u64,
    pub max_execution_ns: u64,
}

#[derive(Clone, Default)]
pub(crate) struct SnapshotMetrics {
    inner: Arc<SnapshotMetricsInner>,
}

#[derive(Default)]
struct SnapshotMetricsInner {
    requests: AtomicU64,
    completed: AtomicU64,
    cancelled: AtomicU64,
    rejected: AtomicU64,
    panicked: AtomicU64,
    total_queue_wait_ns: AtomicU64,
    max_queue_wait_ns: AtomicU64,
    total_execution_ns: AtomicU64,
    max_execution_ns: AtomicU64,
}

impl SnapshotMetrics {
    pub(crate) fn requested(&self) {
        self.inner.requests.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn completed(&self) {
        self.inner.completed.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn cancelled(&self) {
        self.inner.cancelled.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn rejected(&self) {
        self.inner.rejected.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn panicked(&self) {
        self.inner.panicked.fetch_add(1, Ordering::Relaxed);
    }

    pub(crate) fn record_queue_wait(&self, started: Instant, queued_at: Instant) {
        record_duration(
            &self.inner.total_queue_wait_ns,
            &self.inner.max_queue_wait_ns,
            started.saturating_duration_since(queued_at).as_nanos(),
        );
    }

    pub(crate) fn record_execution(&self, finished: Instant, started: Instant) {
        record_duration(
            &self.inner.total_execution_ns,
            &self.inner.max_execution_ns,
            finished.saturating_duration_since(started).as_nanos(),
        );
    }

    pub(crate) fn snapshot(&self) -> SnapshotMetricsSnapshot {
        SnapshotMetricsSnapshot {
            requests: self.inner.requests.load(Ordering::Relaxed),
            completed: self.inner.completed.load(Ordering::Relaxed),
            cancelled: self.inner.cancelled.load(Ordering::Relaxed),
            rejected: self.inner.rejected.load(Ordering::Relaxed),
            panicked: self.inner.panicked.load(Ordering::Relaxed),
            total_queue_wait_ns: self.inner.total_queue_wait_ns.load(Ordering::Relaxed),
            max_queue_wait_ns: self.inner.max_queue_wait_ns.load(Ordering::Relaxed),
            total_execution_ns: self.inner.total_execution_ns.load(Ordering::Relaxed),
            max_execution_ns: self.inner.max_execution_ns.load(Ordering::Relaxed),
        }
    }
}

fn record_duration(total: &AtomicU64, max: &AtomicU64, duration_ns: u128) {
    let duration_ns = duration_ns.min(u64::MAX as u128) as u64;
    total.fetch_add(duration_ns, Ordering::Relaxed);
    max.fetch_max(duration_ns, Ordering::Relaxed);
}
