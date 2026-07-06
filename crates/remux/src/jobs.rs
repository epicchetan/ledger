use crate::error::RpcError;
use serde::{Serialize, Serializer};
use serde_json::Value;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};

#[derive(Clone, Default)]
pub struct JobRegistry {
    inner: Arc<Mutex<JobTable>>,
}

#[derive(Default)]
struct JobTable {
    next_id: u64,
    records: Vec<JobRecord>,
    running_by_key: HashMap<JobKey, String>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct JobRecord {
    pub id: String,
    pub kind: String,
    pub subject: String,
    /// The market day this job settles, when known. Subject stays the dedup
    /// key; this is the display key — the UI files every job under its day.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub market_day: Option<String>,
    pub state: JobState,
    #[serde(serialize_with = "serialize_ns")]
    pub started_at_ns: u64,
    #[serde(serialize_with = "serialize_optional_ns")]
    pub finished_at_ns: Option<u64>,
}

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "status", rename_all = "camelCase")]
pub enum JobState {
    Running {
        stage: String,
        #[serde(skip_serializing_if = "Option::is_none")]
        records: Option<u64>,
    },
    Completed {
        summary: Value,
    },
    Failed {
        error: String,
    },
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct JobStartDto {
    pub job_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub already_running: Option<bool>,
}

#[derive(Debug, Clone)]
pub struct JobStart {
    pub id: String,
    pub already_running: bool,
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct JobKey {
    kind: String,
    subject: String,
}

impl JobRegistry {
    pub fn start(
        &self,
        kind: &str,
        subject: &str,
        market_day: Option<String>,
    ) -> Result<JobStart, RpcError> {
        let key = JobKey {
            kind: kind.to_string(),
            subject: subject.to_string(),
        };
        let mut table = self.lock()?;
        if let Some(id) = table.running_by_key.get(&key) {
            return Ok(JobStart {
                id: id.clone(),
                already_running: true,
            });
        }
        table.next_id += 1;
        let id = format!("job-{}", table.next_id);
        let record = JobRecord {
            id: id.clone(),
            kind: kind.to_string(),
            subject: subject.to_string(),
            market_day,
            state: JobState::Running {
                stage: "queued".to_string(),
                records: None,
            },
            started_at_ns: store::now_ns(),
            finished_at_ns: None,
        };
        table.records.push(record);
        table.running_by_key.insert(key, id.clone());
        Ok(JobStart {
            id,
            already_running: false,
        })
    }

    pub fn progress(&self, id: &str, stage: impl Into<String>, records: Option<u64>) {
        if let Ok(mut table) = self.inner.lock() {
            if let Some(record) = table.records.iter_mut().find(|record| record.id == id) {
                if !matches!(record.state, JobState::Running { .. }) {
                    return;
                }
                record.state = JobState::Running {
                    stage: stage.into(),
                    records,
                };
            }
        }
    }

    pub fn complete(&self, id: &str, summary: Value) {
        self.finish(id, JobState::Completed { summary });
    }

    pub fn fail(&self, id: &str, error: String) {
        self.finish(id, JobState::Failed { error });
    }

    pub fn list(&self) -> Result<Vec<JobRecord>, RpcError> {
        let table = self.lock()?;
        let mut running = table
            .records
            .iter()
            .filter(|record| matches!(record.state, JobState::Running { .. }))
            .cloned()
            .collect::<Vec<_>>();
        running.sort_by(|left, right| left.started_at_ns.cmp(&right.started_at_ns));
        let mut terminal = table
            .records
            .iter()
            .filter(|record| !matches!(record.state, JobState::Running { .. }))
            .cloned()
            .collect::<Vec<_>>();
        terminal.sort_by(|left, right| right.finished_at_ns.cmp(&left.finished_at_ns));
        running.extend(terminal);
        Ok(running)
    }

    fn finish(&self, id: &str, state: JobState) {
        if let Ok(mut table) = self.inner.lock() {
            let mut finished_key = None;
            if let Some(record) = table.records.iter_mut().find(|record| record.id == id) {
                record.finished_at_ns = Some(store::now_ns());
                record.state = state;
                finished_key = Some(JobKey {
                    kind: record.kind.clone(),
                    subject: record.subject.clone(),
                });
            }
            if let Some(key) = finished_key {
                table.running_by_key.remove(&key);
            }
            prune_terminal(&mut table);
        }
    }

    fn lock(&self) -> Result<std::sync::MutexGuard<'_, JobTable>, RpcError> {
        self.inner
            .lock()
            .map_err(|_| RpcError::domain("job registry poisoned"))
    }
}

impl From<JobStart> for JobStartDto {
    fn from(start: JobStart) -> Self {
        Self {
            job_id: start.id,
            already_running: start.already_running.then_some(true),
        }
    }
}

fn prune_terminal(table: &mut JobTable) {
    let mut terminal = table
        .records
        .iter()
        .filter(|record| !matches!(record.state, JobState::Running { .. }))
        .map(|record| {
            (
                record.id.clone(),
                record.finished_at_ns.unwrap_or(record.started_at_ns),
            )
        })
        .collect::<Vec<_>>();
    if terminal.len() <= 50 {
        return;
    }
    terminal.sort_by(|left, right| right.1.cmp(&left.1));
    let keep = terminal
        .into_iter()
        .take(50)
        .map(|(id, _)| id)
        .collect::<std::collections::HashSet<_>>();
    table.records.retain(|record| {
        matches!(record.state, JobState::Running { .. }) || keep.contains(&record.id)
    });
}

fn serialize_ns<S>(value: &u64, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    serializer.serialize_str(&value.to_string())
}

fn serialize_optional_ns<S>(value: &Option<u64>, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    match value {
        Some(value) => serializer.serialize_some(&value.to_string()),
        None => serializer.serialize_none(),
    }
}
