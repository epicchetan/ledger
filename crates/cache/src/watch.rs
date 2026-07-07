use tokio::sync::watch;

use crate::{CacheError, Key};

/// Change watch on one registered cell. Carries the cell's generation only,
/// never payload: wake, then read the cell to learn what changed.
#[derive(Debug)]
pub struct CellWatch {
    key: Key,
    rx: watch::Receiver<u64>,
}

impl CellWatch {
    pub(crate) fn new(key: Key, rx: watch::Receiver<u64>) -> Self {
        Self { key, rx }
    }

    /// Parks the task until the cell's generation advances past the last
    /// value this watch observed. Returns immediately if it already has.
    /// Cancel-safe inside `tokio::select!`.
    pub async fn changed(&mut self) -> Result<(), CacheError> {
        self.rx
            .changed()
            .await
            .map_err(|_| CacheError::WatchClosed {
                key: self.key.clone(),
            })
    }

    /// The latest generation visible to this watch.
    pub fn generation(&self) -> u64 {
        *self.rx.borrow()
    }
}
