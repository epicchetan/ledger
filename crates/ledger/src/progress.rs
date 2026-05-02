use serde::{Deserialize, Serialize};
use std::sync::Arc;
use std::time::Instant;

pub type LedgerProgressSink = Arc<dyn Fn(LedgerProgressEvent) + Send + Sync>;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(tag = "type", rename_all = "snake_case")]
pub enum LedgerProgressEvent {
    Step { message: String },
    Done { message: String, elapsed_ms: u128 },
}

#[derive(Clone, Default)]
pub struct LedgerProgress {
    sink: Option<LedgerProgressSink>,
}

impl LedgerProgress {
    pub fn new(sink: Option<LedgerProgressSink>) -> Self {
        Self { sink }
    }

    pub fn quiet() -> Self {
        Self { sink: None }
    }

    pub fn step(&self, message: impl Into<String>) {
        if let Some(sink) = &self.sink {
            sink(LedgerProgressEvent::Step {
                message: message.into(),
            });
        }
    }

    pub fn done(&self, message: impl Into<String>, started_at: Instant) {
        if let Some(sink) = &self.sink {
            sink(LedgerProgressEvent::Done {
                message: message.into(),
                elapsed_ms: started_at.elapsed().as_millis(),
            });
        }
    }
}
