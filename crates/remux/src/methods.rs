use crate::error::RpcError;
use crate::jobs::{JobRecord, JobRegistry, JobStartDto};
use crate::rpc::{send_notification, OutboundSender, Request};
use chrono::{DateTime, SecondsFormat, Utc};
use ledger::feed::es_replay::{
    es_day_catalog, prepare_es_replay_artifact, EsDayCatalog, EsDayEntry, EsRawState, EsRawStatus,
    PrepareProgress,
};
use ledger::market::MarketDay;
use serde::{de::DeserializeOwned, Deserialize, Serialize};
use serde_json::{json, Value};
use store::{
    DeleteObjectReport, LocalObjectEntry, LocalStoreStatus, ObjectFilter, RemoteObjectLocation,
    RemoteStore, RemoveLocalObjectReport, Store, StoreObjectDescriptor, StoreObjectId,
    StoreObjectRole,
};

const PING_METHOD: &str = "remux/ledger/ping";
const STORE_LIST_METHOD: &str = "remux/ledger/store/list";
const STORE_GET_METHOD: &str = "remux/ledger/store/get";
const STORE_DELETE_METHOD: &str = "remux/ledger/store/delete";
const STORE_HYDRATE_METHOD: &str = "remux/ledger/store/hydrate";
const STORE_OFFLOAD_METHOD: &str = "remux/ledger/store/offload";
const STORE_LOCAL_STATUS_METHOD: &str = "remux/ledger/store/localStatus";
const ES_DAYS_METHOD: &str = "remux/ledger/es/days";
const ES_INSTALL_METHOD: &str = "remux/ledger/es/install";
const ES_OFFLOAD_METHOD: &str = "remux/ledger/es/offload";
const JOBS_LIST_METHOD: &str = "remux/ledger/jobs/list";
const JOBS_PROGRESS_NOTIFICATION: &str = "remux/ledger/jobs/progress";
const JOBS_FINISHED_NOTIFICATION: &str = "remux/ledger/jobs/finished";

#[derive(Clone)]
pub struct LedgerRemux<S: RemoteStore + 'static> {
    store: Store<S>,
    jobs: JobRegistry,
    output_tx: OutboundSender,
}

impl<S: RemoteStore + 'static> LedgerRemux<S> {
    pub fn new(store: Store<S>, output_tx: OutboundSender) -> Self {
        Self {
            store,
            jobs: JobRegistry::default(),
            output_tx,
        }
    }

    pub async fn handle(&self, request: Request) -> Result<Value, RpcError> {
        match request.method.as_str() {
            PING_METHOD => to_value(PingDto {
                ok: true,
                service: "ledger-remux".to_string(),
                version: env!("CARGO_PKG_VERSION").to_string(),
            }),
            STORE_LIST_METHOD => self.list_store_objects(request.params),
            STORE_GET_METHOD => self.get_store_object(request.params),
            STORE_DELETE_METHOD => self.delete_store_object(request.params).await,
            STORE_HYDRATE_METHOD => self.hydrate_store_object(request.params),
            STORE_OFFLOAD_METHOD => self.offload_store_object(request.params),
            STORE_LOCAL_STATUS_METHOD => self.local_status(),
            ES_DAYS_METHOD => self.es_days(),
            ES_INSTALL_METHOD => self.install_es(request.params),
            ES_OFFLOAD_METHOD => self.offload_es(request.params),
            JOBS_LIST_METHOD => self.jobs_list(),
            method => Err(RpcError::method_not_found(method)),
        }
    }

    fn list_store_objects(&self, params: Value) -> Result<Value, RpcError> {
        let params = parse_params::<StoreListParams>(params)?;
        let objects = self
            .store
            .list_objects(object_filter(params)?)?
            .into_iter()
            .map(StoreObjectDto::from)
            .collect::<Vec<_>>();
        to_value(StoreListResultDto { objects })
    }

    fn get_store_object(&self, params: Value) -> Result<Value, RpcError> {
        let params = parse_params::<IdParams>(params)?;
        let id = parse_object_id(&params.id)?;
        let object = self
            .store
            .get_object(&id)?
            .map(StoreObjectDto::from)
            .ok_or_else(|| RpcError::object_not_found(&params.id))?;
        to_value(StoreGetResultDto { object })
    }

    async fn delete_store_object(&self, params: Value) -> Result<Value, RpcError> {
        let params = parse_params::<IdParams>(params)?;
        let id = parse_object_id(&params.id)?;
        // No raw override over RPC: deleting paid source data is a deliberate
        // CLI act, never something the viewer can do.
        let report = self.store.delete_object(&id, false).await?;
        if !report.descriptor_removed {
            return Err(RpcError::object_not_found(&params.id));
        }
        to_value(DeleteReportDto::from(report))
    }

    fn offload_store_object(&self, params: Value) -> Result<Value, RpcError> {
        let params = parse_params::<IdParams>(params)?;
        let id = parse_object_id(&params.id)?;
        if self.store.get_object(&id)?.is_none() {
            return Err(RpcError::object_not_found(&params.id));
        }
        let report = self.store.offload_object(&id)?;
        to_value(OffloadReportDto::from(report))
    }

    // Hydrate rides the same job registry as install: one job model, one pair
    // of notifications, dedup by (kind, subject). An already-local object just
    // completes the job immediately — the store's hydrate fast-path is free.
    fn hydrate_store_object(&self, params: Value) -> Result<Value, RpcError> {
        let params = parse_params::<IdParams>(params)?;
        let id = parse_object_id(&params.id)?;
        let descriptor = self
            .store
            .get_object(&id)?
            .ok_or_else(|| RpcError::object_not_found(&params.id))?;
        let market_day = descriptor
            .metadata_json
            .get("market_day")
            .and_then(Value::as_str)
            .map(str::to_string);
        let subject = id.to_string();
        let start = self.jobs.start("store.hydrate", &subject, market_day.clone())?;
        if !start.already_running {
            self.spawn_hydrate_job(start.id.clone(), id, market_day);
        }
        to_value(JobStartDto::from(start))
    }

    fn local_status(&self) -> Result<Value, RpcError> {
        to_value(LocalStatusDto::from(self.store.local_status()?))
    }

    fn es_days(&self) -> Result<Value, RpcError> {
        let catalog =
            es_day_catalog(&self.store).map_err(|err| RpcError::domain(err.to_string()))?;
        to_value(EsDayCatalogDto::from(catalog))
    }

    // Install: make the feed's artifact local, whatever that takes — an
    // existing artifact is hydrated and validated, a missing or invalid one is
    // rebuilt from the R2 raw. One verb for the UI; forced rebuilds stay a CLI
    // maintenance op (`ledger es prepare`).
    fn install_es(&self, params: Value) -> Result<Value, RpcError> {
        let params = parse_params::<EsInstallParams>(params)?;
        let raw_id = parse_object_id(&params.raw_id)?;
        let raw = self
            .store
            .get_object(&raw_id)?
            .ok_or_else(|| RpcError::object_not_found(&params.raw_id))?;
        let market_day = raw
            .metadata_json
            .get("market_day")
            .and_then(Value::as_str)
            .map(str::to_string);
        let subject = raw_id.to_string();
        let start = self.jobs.start("es.install", &subject, market_day.clone())?;
        if !start.already_running {
            self.spawn_install_job(start.id.clone(), raw_id, market_day);
        }
        to_value(JobStartDto::from(start))
    }

    // Offload a day: drop local copies of everything the day owns while R2
    // keeps every byte. Synchronous — file deletes don't need a job.
    fn offload_es(&self, params: Value) -> Result<Value, RpcError> {
        let params = parse_params::<EsOffloadParams>(params)?;
        let market_day = MarketDay::parse(&params.market_day)
            .map_err(|err| RpcError::invalid_params(err.to_string()))?;
        let catalog =
            es_day_catalog(&self.store).map_err(|err| RpcError::domain(err.to_string()))?;
        let entry = catalog
            .days
            .into_iter()
            .find(|entry| entry.market_day == market_day)
            .ok_or_else(|| RpcError::domain(format!("unknown ES market day {market_day}")))?;

        let mut offloaded = Vec::new();
        let mut bytes_removed = 0u64;
        for status in entry.raws {
            let mut descriptors = vec![status.raw];
            descriptors.extend(status.artifact);
            for descriptor in descriptors {
                if descriptor.local.is_none() {
                    continue;
                }
                let report = self.store.offload_object(&descriptor.id)?;
                bytes_removed += report.bytes_removed;
                offloaded.push(OffloadedObjectDto {
                    id: descriptor.id.to_string(),
                    bytes_removed: report.bytes_removed,
                });
            }
        }
        to_value(EsOffloadReportDto {
            market_day: market_day.to_string(),
            offloaded,
            bytes_removed,
        })
    }

    fn jobs_list(&self) -> Result<Value, RpcError> {
        to_value(JobsListDto {
            jobs: self.jobs.list()?,
        })
    }

    fn spawn_install_job(
        &self,
        job_id: String,
        raw_id: StoreObjectId,
        market_day: Option<String>,
    ) {
        let store = self.store.clone();
        let jobs = self.jobs.clone();
        let output_tx = self.output_tx.clone();
        let subject = raw_id.to_string();
        let (progress_tx, mut progress_rx) = tokio::sync::mpsc::unbounded_channel();
        let progress_jobs = jobs.clone();
        let progress_output = output_tx.clone();
        let progress_job_id = job_id.clone();
        let progress_subject = subject.clone();
        let progress_market_day = market_day.clone();
        tokio::spawn(async move {
            while let Some(progress) = progress_rx.recv().await {
                let (stage, records) = prepare_progress_stage(progress);
                progress_jobs.progress(&progress_job_id, stage.clone(), records);
                let params = json!({
                    "jobId": progress_job_id,
                    "kind": "es.install",
                    "subject": progress_subject,
                    "marketDay": progress_market_day,
                    "stage": stage,
                    "records": records,
                });
                if let Err(error) =
                    send_notification(&progress_output, JOBS_PROGRESS_NOTIFICATION, params).await
                {
                    eprintln!("[ledger-remux] failed to broadcast job progress: {error}");
                }
            }
        });
        tokio::spawn(async move {
            let result =
                prepare_es_replay_artifact(&store, &raw_id, false, Some(progress_tx)).await;
            let finished = JobFinishedContext {
                job_id: &job_id,
                kind: "es.install",
                subject: &subject,
                market_day: market_day.as_deref(),
            };
            match result {
                Ok(artifact) => {
                    let summary = summary_value(&artifact.summary(&raw_id));
                    jobs.complete(&job_id, summary.clone());
                    send_job_finished(&output_tx, finished, true, Some(summary), None).await;
                }
                Err(error) => {
                    let error = error.to_string();
                    jobs.fail(&job_id, error.clone());
                    send_job_finished(&output_tx, finished, false, None, Some(error)).await;
                }
            }
        });
    }

    fn spawn_hydrate_job(&self, job_id: String, id: StoreObjectId, market_day: Option<String>) {
        let store = self.store.clone();
        let jobs = self.jobs.clone();
        let output_tx = self.output_tx.clone();
        let subject = id.to_string();
        tokio::spawn(async move {
            jobs.progress(&job_id, "hydrating", None);
            let params = json!({
                "jobId": job_id,
                "kind": "store.hydrate",
                "subject": subject,
                "marketDay": market_day,
                "stage": "hydrating",
                "records": Value::Null,
            });
            if let Err(error) =
                send_notification(&output_tx, JOBS_PROGRESS_NOTIFICATION, params).await
            {
                eprintln!("[ledger-remux] failed to broadcast job progress: {error}");
            }
            let finished = JobFinishedContext {
                job_id: &job_id,
                kind: "store.hydrate",
                subject: &subject,
                market_day: market_day.as_deref(),
            };
            match store.hydrate(&id).await {
                Ok(hydrated) => {
                    let summary = json!({
                        "id": subject,
                        "path": hydrated.path.display().to_string(),
                        "sizeBytes": hydrated.descriptor.size_bytes,
                    });
                    jobs.complete(&job_id, summary.clone());
                    send_job_finished(&output_tx, finished, true, Some(summary), None).await;
                }
                Err(error) => {
                    let error = error.to_string();
                    jobs.fail(&job_id, error.clone());
                    send_job_finished(&output_tx, finished, false, None, Some(error)).await;
                }
            }
        });
    }
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct PingDto {
    ok: bool,
    service: String,
    version: String,
}

#[derive(Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
struct StoreListParams {
    role: Option<String>,
    kind: Option<String>,
    id_prefix: Option<String>,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct IdParams {
    id: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct EsInstallParams {
    raw_id: String,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
struct EsOffloadParams {
    market_day: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct EsOffloadReportDto {
    market_day: String,
    offloaded: Vec<OffloadedObjectDto>,
    bytes_removed: u64,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct OffloadedObjectDto {
    id: String,
    bytes_removed: u64,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct StoreListResultDto {
    objects: Vec<StoreObjectDto>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct JobsListDto {
    jobs: Vec<JobRecord>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct StoreGetResultDto {
    object: StoreObjectDto,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct EsDayCatalogDto {
    days: Vec<EsDayEntryDto>,
    unassigned: Vec<EsRawStatusDto>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct EsDayEntryDto {
    market_day: String,
    raws: Vec<EsRawStatusDto>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct EsRawStatusDto {
    raw: StoreObjectDto,
    artifact: Option<StoreObjectDto>,
    state: EsRawState,
}

impl From<EsDayCatalog> for EsDayCatalogDto {
    fn from(catalog: EsDayCatalog) -> Self {
        Self {
            days: catalog.days.into_iter().map(EsDayEntryDto::from).collect(),
            unassigned: catalog
                .unassigned
                .into_iter()
                .map(EsRawStatusDto::from)
                .collect(),
        }
    }
}

impl From<EsDayEntry> for EsDayEntryDto {
    fn from(entry: EsDayEntry) -> Self {
        Self {
            market_day: entry.market_day.to_string(),
            raws: entry.raws.into_iter().map(EsRawStatusDto::from).collect(),
        }
    }
}

impl From<EsRawStatus> for EsRawStatusDto {
    fn from(status: EsRawStatus) -> Self {
        Self {
            raw: StoreObjectDto::from(status.raw),
            artifact: status.artifact.map(StoreObjectDto::from),
            state: status.state,
        }
    }
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct StoreObjectDto {
    id: String,
    role: String,
    kind: String,
    file_name: String,
    content_sha256: String,
    size_bytes: u64,
    format: Option<String>,
    media_type: Option<String>,
    remote: Option<StoreRemoteObjectDto>,
    local: Option<LocalStoreObjectDto>,
    lineage: Vec<String>,
    metadata_json: Value,
    created_at_ns: String,
    created_at_iso: String,
    updated_at_ns: String,
    updated_at_iso: String,
    last_accessed_at_ns: Option<String>,
    last_accessed_at_iso: Option<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct StoreRemoteObjectDto {
    bucket: String,
    key: String,
    size_bytes: u64,
    sha256: Option<String>,
    etag: Option<String>,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct LocalStoreObjectDto {
    relative_path: String,
    size_bytes: u64,
    last_accessed_at_ns: String,
    last_accessed_at_iso: String,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct DeleteReportDto {
    id: Option<String>,
    descriptor_removed: bool,
    remote_object_deleted: bool,
    remote_descriptor_deleted: bool,
    local_deleted: bool,
    remote_key: Option<String>,
    remote_descriptor_key: Option<String>,
    local_path: Option<String>,
    bytes_deleted: u64,
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct OffloadReportDto {
    id: Option<String>,
    removed: bool,
    path: Option<String>,
    bytes_removed: u64,
}

impl From<RemoveLocalObjectReport> for OffloadReportDto {
    fn from(report: RemoveLocalObjectReport) -> Self {
        Self {
            id: report.id.map(|id| id.to_string()),
            removed: report.removed,
            path: report.path.map(|path| path.display().to_string()),
            bytes_removed: report.bytes_removed,
        }
    }
}

#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
struct LocalStatusDto {
    root: String,
    local_objects: usize,
    size_bytes: u64,
    max_bytes: u64,
}

impl From<StoreObjectDescriptor> for StoreObjectDto {
    fn from(descriptor: StoreObjectDescriptor) -> Self {
        Self {
            id: descriptor.id.to_string(),
            role: descriptor.role.as_str().to_string(),
            kind: descriptor.kind,
            file_name: descriptor.file_name,
            content_sha256: descriptor.content_sha256,
            size_bytes: descriptor.size_bytes,
            format: descriptor.format,
            media_type: descriptor.media_type,
            remote: descriptor.remote.map(StoreRemoteObjectDto::from),
            local: descriptor.local.map(LocalStoreObjectDto::from),
            lineage: descriptor
                .lineage
                .into_iter()
                .map(|id| id.to_string())
                .collect(),
            metadata_json: descriptor.metadata_json,
            created_at_ns: ns_string(descriptor.created_at_ns),
            created_at_iso: ns_iso(descriptor.created_at_ns),
            updated_at_ns: ns_string(descriptor.updated_at_ns),
            updated_at_iso: ns_iso(descriptor.updated_at_ns),
            last_accessed_at_ns: descriptor.last_accessed_at_ns.map(ns_string),
            last_accessed_at_iso: descriptor.last_accessed_at_ns.map(ns_iso),
        }
    }
}

impl From<RemoteObjectLocation> for StoreRemoteObjectDto {
    fn from(remote: RemoteObjectLocation) -> Self {
        Self {
            bucket: remote.bucket,
            key: remote.key,
            size_bytes: remote.size_bytes,
            sha256: remote.sha256,
            etag: remote.etag,
        }
    }
}

impl From<LocalObjectEntry> for LocalStoreObjectDto {
    fn from(local: LocalObjectEntry) -> Self {
        Self {
            relative_path: local.relative_path.display().to_string(),
            size_bytes: local.size_bytes,
            last_accessed_at_ns: ns_string(local.last_accessed_at_ns),
            last_accessed_at_iso: ns_iso(local.last_accessed_at_ns),
        }
    }
}

impl From<DeleteObjectReport> for DeleteReportDto {
    fn from(report: DeleteObjectReport) -> Self {
        Self {
            id: report.id.map(|id| id.to_string()),
            descriptor_removed: report.descriptor_removed,
            remote_object_deleted: report.remote_object_deleted,
            remote_descriptor_deleted: report.remote_descriptor_deleted,
            local_deleted: report.local_deleted,
            remote_key: report.remote_key,
            remote_descriptor_key: report.remote_descriptor_key,
            local_path: report.local_path.map(|path| path.display().to_string()),
            bytes_deleted: report.bytes_deleted,
        }
    }
}

impl From<LocalStoreStatus> for LocalStatusDto {
    fn from(status: LocalStoreStatus) -> Self {
        Self {
            root: status.root.display().to_string(),
            local_objects: status.local_objects,
            size_bytes: status.size_bytes,
            max_bytes: status.max_bytes,
        }
    }
}

fn parse_params<T: DeserializeOwned>(params: Value) -> Result<T, RpcError> {
    let params = if params.is_null() {
        serde_json::json!({})
    } else {
        params
    };
    serde_json::from_value(params).map_err(|err| RpcError::invalid_params(err.to_string()))
}

fn parse_object_id(value: &str) -> Result<StoreObjectId, RpcError> {
    StoreObjectId::new(value.to_string())
        .map_err(|err| RpcError::invalid_object_id(value, err.to_string()))
}

fn object_filter(params: StoreListParams) -> Result<ObjectFilter, RpcError> {
    Ok(ObjectFilter {
        role: params
            .role
            .as_deref()
            .map(StoreObjectRole::parse)
            .transpose()
            .map_err(|err| RpcError::invalid_params(err.to_string()))?,
        kind: params.kind,
        id_prefix: params.id_prefix,
    })
}

fn to_value<T: Serialize>(value: T) -> Result<Value, RpcError> {
    serde_json::to_value(value).map_err(|err| RpcError::domain(err.to_string()))
}

// Job summaries ride a notification, so a serialization failure degrades to an
// inline error value instead of losing the completion event.
fn summary_value<T: Serialize>(value: &T) -> Value {
    serde_json::to_value(value).unwrap_or_else(|error| json!({ "error": error.to_string() }))
}

fn prepare_progress_stage(progress: PrepareProgress) -> (String, Option<u64>) {
    match progress {
        PrepareProgress::Hydrating => ("hydrating".to_string(), None),
        PrepareProgress::Decoding { records } => ("decoding".to_string(), Some(records)),
        PrepareProgress::Encoding { events, .. } => ("encoding".to_string(), Some(events)),
        PrepareProgress::Registering => ("registering".to_string(), None),
    }
}

// Identity fields repeated on the finished notification so the client can key
// the outcome by day without joining against its local job table.
#[derive(Clone, Copy)]
struct JobFinishedContext<'a> {
    job_id: &'a str,
    kind: &'a str,
    subject: &'a str,
    market_day: Option<&'a str>,
}

async fn send_job_finished(
    output_tx: &OutboundSender,
    context: JobFinishedContext<'_>,
    ok: bool,
    summary: Option<Value>,
    error: Option<String>,
) {
    let params = json!({
        "jobId": context.job_id,
        "kind": context.kind,
        "subject": context.subject,
        "marketDay": context.market_day,
        "ok": ok,
        "summary": summary,
        "error": error,
    });
    if let Err(error) = send_notification(output_tx, JOBS_FINISHED_NOTIFICATION, params).await {
        eprintln!("[ledger-remux] failed to broadcast job completion: {error}");
    }
}

fn ns_string(ns: u64) -> String {
    ns.to_string()
}

fn ns_iso(ns: u64) -> String {
    ns_to_datetime(ns).to_rfc3339_opts(SecondsFormat::Nanos, true)
}

fn ns_to_datetime(ns: u64) -> DateTime<Utc> {
    let secs = (ns / 1_000_000_000) as i64;
    let nanos = (ns % 1_000_000_000) as u32;
    DateTime::<Utc>::from_timestamp(secs, nanos).unwrap_or(DateTime::<Utc>::UNIX_EPOCH)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::error::{DOMAIN_ERROR, INVALID_PARAMS, METHOD_NOT_FOUND};
    use async_trait::async_trait;
    use serde_json::json;
    use std::collections::HashMap;
    use std::path::Path;
    use std::sync::{Arc, Mutex};
    use store::{ObjectMetadata, RegisterFileRequest, RemoteObject, StoreConfig};
    use tempfile::{tempdir, TempDir};
    use tokio::io::AsyncWriteExt;
    use tokio::sync::mpsc;

    #[derive(Clone, Default)]
    struct TestRemote {
        bucket: String,
        objects: Arc<Mutex<HashMap<String, (Vec<u8>, ObjectMetadata)>>>,
    }

    impl TestRemote {
        fn new() -> Self {
            Self {
                bucket: "test-bucket".to_string(),
                objects: Arc::new(Mutex::new(HashMap::new())),
            }
        }
    }

    #[async_trait]
    impl RemoteStore for TestRemote {
        async fn put_path(
            &self,
            key: &str,
            path: &Path,
            metadata: &ObjectMetadata,
        ) -> anyhow::Result<RemoteObject> {
            let bytes = tokio::fs::read(path).await?;
            self.put_bytes(key, &bytes, metadata).await
        }

        async fn get_to_path(&self, key: &str, dest: &Path) -> anyhow::Result<RemoteObject> {
            let (bytes, metadata) = self
                .objects
                .lock()
                .unwrap()
                .get(key)
                .cloned()
                .ok_or_else(|| anyhow::anyhow!("missing object {key}"))?;
            if let Some(parent) = dest.parent() {
                tokio::fs::create_dir_all(parent).await?;
            }
            let mut file = tokio::fs::File::create(dest).await?;
            file.write_all(&bytes).await?;
            file.sync_all().await?;
            Ok(remote_object(
                &self.bucket,
                key,
                bytes.len() as u64,
                &metadata,
            ))
        }

        async fn head(&self, key: &str) -> anyhow::Result<Option<RemoteObject>> {
            Ok(self
                .objects
                .lock()
                .unwrap()
                .get(key)
                .map(|(bytes, metadata)| {
                    remote_object(&self.bucket, key, bytes.len() as u64, metadata)
                }))
        }

        async fn delete(&self, key: &str) -> anyhow::Result<()> {
            self.objects.lock().unwrap().remove(key);
            Ok(())
        }

        async fn put_bytes(
            &self,
            key: &str,
            bytes: &[u8],
            metadata: &ObjectMetadata,
        ) -> anyhow::Result<RemoteObject> {
            self.objects
                .lock()
                .unwrap()
                .insert(key.to_string(), (bytes.to_vec(), metadata.clone()));
            Ok(remote_object(
                &self.bucket,
                key,
                bytes.len() as u64,
                metadata,
            ))
        }

        async fn get_bytes(&self, key: &str) -> anyhow::Result<Vec<u8>> {
            self.objects
                .lock()
                .unwrap()
                .get(key)
                .map(|(bytes, _)| bytes.clone())
                .ok_or_else(|| anyhow::anyhow!("missing object {key}"))
        }

        async fn list_keys(&self, prefix: &str) -> anyhow::Result<Vec<String>> {
            Ok(self
                .objects
                .lock()
                .unwrap()
                .keys()
                .filter(|key| key.starts_with(prefix))
                .cloned()
                .collect())
        }

        fn bucket(&self) -> &str {
            &self.bucket
        }
    }

    fn remote_object(
        bucket: &str,
        key: &str,
        size_bytes: u64,
        metadata: &ObjectMetadata,
    ) -> RemoteObject {
        RemoteObject {
            bucket: bucket.to_string(),
            key: key.to_string(),
            size_bytes,
            sha256: Some(metadata.sha256.clone()),
            etag: None,
            metadata: metadata.user_metadata.clone(),
        }
    }

    fn open_methods(data: &TempDir) -> LedgerRemux<TestRemote> {
        let store = Store::open(
            data.path(),
            StoreConfig {
                local_max_bytes: 1024,
            },
            Arc::new(TestRemote::new()),
        )
        .unwrap();
        let (output_tx, _output_rx) = mpsc::channel(8);
        LedgerRemux::new(store, output_tx)
    }

    async fn register_object(
        methods: &LedgerRemux<TestRemote>,
        data: &TempDir,
        role: StoreObjectRole,
        file_name: &str,
        bytes: &[u8],
    ) -> StoreObjectDescriptor {
        let source = data.path().join(file_name);
        tokio::fs::write(&source, bytes).await.unwrap();
        methods
            .store
            .register_file(RegisterFileRequest {
                path: &source,
                role,
                kind: "runtime.artifact".to_string(),
                file_name: None,
                format: None,
                media_type: None,
                lineage: Vec::new(),
                metadata_json: json!({}),
            })
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn unknown_method_returns_method_not_found() {
        let data = tempdir().unwrap();
        let methods = open_methods(&data);

        let err = methods
            .handle(Request {
                method: "remux/ledger/missing".to_string(),
                params: Value::Null,
            })
            .await
            .unwrap_err();

        assert_eq!(err.code, METHOD_NOT_FOUND);
    }

    #[tokio::test]
    async fn invalid_params_return_invalid_params() {
        let data = tempdir().unwrap();
        let methods = open_methods(&data);

        let err = methods
            .handle(Request {
                method: STORE_LIST_METHOD.to_string(),
                params: json!({ "role": "bogus" }),
            })
            .await
            .unwrap_err();

        assert_eq!(err.code, INVALID_PARAMS);
    }

    #[tokio::test]
    async fn invalid_object_id_returns_invalid_params_with_id() {
        let data = tempdir().unwrap();
        let methods = open_methods(&data);

        let err = methods
            .handle(Request {
                method: STORE_GET_METHOD.to_string(),
                params: json!({ "id": "bad" }),
            })
            .await
            .unwrap_err();

        assert_eq!(err.code, INVALID_PARAMS);
        assert_eq!(err.data, Some(json!({ "id": "bad" })));
    }

    #[tokio::test]
    async fn list_maps_filters_and_returns_wrapped_objects() {
        let data = tempdir().unwrap();
        let methods = open_methods(&data);
        register_object(
            &methods,
            &data,
            StoreObjectRole::Raw,
            "raw.bin",
            b"raw bytes",
        )
        .await;
        register_object(
            &methods,
            &data,
            StoreObjectRole::Artifact,
            "artifact.bin",
            b"artifact bytes",
        )
        .await;

        let result = methods
            .handle(Request {
                method: STORE_LIST_METHOD.to_string(),
                params: json!({ "role": "raw" }),
            })
            .await
            .unwrap();

        assert_eq!(result["objects"].as_array().unwrap().len(), 1);
        assert_eq!(result["objects"][0]["role"], "raw");
        assert!(result["objects"][0].get("fileName").is_some());
        assert!(result["objects"][0].get("file_name").is_none());
    }

    #[tokio::test]
    async fn es_days_returns_camel_case_dtos() {
        let data = tempdir().unwrap();
        let methods = open_methods(&data);
        let source = data.path().join("es-raw.dbn.zst");
        tokio::fs::write(&source, b"es raw bytes").await.unwrap();
        methods
            .store
            .register_file(RegisterFileRequest {
                path: &source,
                role: StoreObjectRole::Raw,
                kind: "databento.dbn.zst".to_string(),
                file_name: None,
                format: None,
                media_type: None,
                lineage: Vec::new(),
                metadata_json: json!({
                    "market_day": "2026-03-10",
                    "source_symbol": "ESH6",
                }),
            })
            .await
            .unwrap();
        let unassigned_source = data.path().join("es-raw-unassigned.dbn.zst");
        tokio::fs::write(&unassigned_source, b"unassigned raw bytes")
            .await
            .unwrap();
        methods
            .store
            .register_file(RegisterFileRequest {
                path: &unassigned_source,
                role: StoreObjectRole::Raw,
                kind: "databento.dbn.zst".to_string(),
                file_name: None,
                format: None,
                media_type: None,
                lineage: Vec::new(),
                metadata_json: json!({ "source_symbol": "ESH6" }),
            })
            .await
            .unwrap();

        let result = methods
            .handle(Request {
                method: ES_DAYS_METHOD.to_string(),
                params: Value::Null,
            })
            .await
            .unwrap();

        assert_eq!(result["days"].as_array().unwrap().len(), 1);
        assert_eq!(result["days"][0]["marketDay"], "2026-03-10");
        assert_eq!(result["days"][0]["raws"][0]["state"], "unprepared");
        let raw = &result["days"][0]["raws"][0]["raw"];
        assert!(raw.get("fileName").is_some());
        assert!(raw.get("file_name").is_none());
        assert!(raw.get("sizeBytes").is_some());
        assert!(raw["createdAtNs"].is_string());
        assert_eq!(result["unassigned"].as_array().unwrap().len(), 1);
    }

    #[tokio::test]
    async fn get_returns_object_not_found_for_unknown_id() {
        let data = tempdir().unwrap();
        let methods = open_methods(&data);
        let id = format!("sha256-{}", "0".repeat(64));

        let err = methods
            .handle(Request {
                method: STORE_GET_METHOD.to_string(),
                params: json!({ "id": id }),
            })
            .await
            .unwrap_err();

        assert_eq!(err.message, "objectNotFound");
    }

    #[tokio::test]
    async fn delete_reports_removal() {
        let data = tempdir().unwrap();
        let methods = open_methods(&data);
        let descriptor = register_object(
            &methods,
            &data,
            StoreObjectRole::Artifact,
            "delete.bin",
            b"delete bytes",
        )
        .await;

        let result = methods
            .handle(Request {
                method: STORE_DELETE_METHOD.to_string(),
                params: json!({ "id": descriptor.id.to_string() }),
            })
            .await
            .unwrap();

        assert_eq!(result["descriptorRemoved"], true);
        assert_eq!(result["remoteObjectDeleted"], true);
    }

    #[tokio::test]
    async fn delete_refuses_raw_objects() {
        let data = tempdir().unwrap();
        let methods = open_methods(&data);
        let descriptor = register_object(
            &methods,
            &data,
            StoreObjectRole::Raw,
            "paid.dbn.zst",
            b"paid raw bytes",
        )
        .await;

        let err = methods
            .handle(Request {
                method: STORE_DELETE_METHOD.to_string(),
                params: json!({ "id": descriptor.id.to_string() }),
            })
            .await
            .unwrap_err();

        assert_eq!(err.code, DOMAIN_ERROR);
        assert!(err.message.contains("refusing to delete raw object"));
        assert!(methods.store.get_object(&descriptor.id).unwrap().is_some());
    }

    #[tokio::test]
    async fn offload_drops_local_and_keeps_descriptor() {
        let data = tempdir().unwrap();
        let methods = open_methods(&data);
        let descriptor = register_object(
            &methods,
            &data,
            StoreObjectRole::Artifact,
            "offload.bin",
            b"offload bytes",
        )
        .await;

        let result = methods
            .handle(Request {
                method: STORE_OFFLOAD_METHOD.to_string(),
                params: json!({ "id": descriptor.id.to_string() }),
            })
            .await
            .unwrap();

        assert_eq!(result["removed"], true);
        assert_eq!(result["bytesRemoved"], b"offload bytes".len() as u64);
        let after = methods
            .store
            .get_object(&descriptor.id)
            .unwrap()
            .unwrap();
        assert!(after.local.is_none());
        assert!(after.remote.is_some());
    }

    async fn register_es_raw(
        methods: &LedgerRemux<TestRemote>,
        data: &TempDir,
        file_name: &str,
        market_day: &str,
    ) -> StoreObjectDescriptor {
        let source = data.path().join(file_name);
        tokio::fs::write(&source, b"es raw bytes").await.unwrap();
        methods
            .store
            .register_file(RegisterFileRequest {
                path: &source,
                role: StoreObjectRole::Raw,
                kind: "databento.dbn.zst".to_string(),
                file_name: None,
                format: None,
                media_type: None,
                lineage: Vec::new(),
                metadata_json: json!({
                    "market_day": market_day,
                    "source_symbol": "ESH6",
                }),
            })
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn install_starts_day_tagged_job() {
        let data = tempdir().unwrap();
        let methods = open_methods(&data);
        let descriptor = register_es_raw(&methods, &data, "install.dbn.zst", "2026-03-10").await;

        let result = methods
            .handle(Request {
                method: ES_INSTALL_METHOD.to_string(),
                params: json!({ "rawId": descriptor.id.to_string() }),
            })
            .await
            .unwrap();

        assert_eq!(result["jobId"], "job-1");

        let jobs = methods
            .handle(Request {
                method: JOBS_LIST_METHOD.to_string(),
                params: Value::Null,
            })
            .await
            .unwrap();
        assert_eq!(jobs["jobs"][0]["kind"], "es.install");
        assert_eq!(jobs["jobs"][0]["subject"], descriptor.id.to_string());
        assert_eq!(jobs["jobs"][0]["marketDay"], "2026-03-10");
    }

    #[tokio::test]
    async fn install_returns_object_not_found_for_unknown_raw() {
        let data = tempdir().unwrap();
        let methods = open_methods(&data);
        let id = format!("sha256-{}", "0".repeat(64));

        let err = methods
            .handle(Request {
                method: ES_INSTALL_METHOD.to_string(),
                params: json!({ "rawId": id }),
            })
            .await
            .unwrap_err();

        assert_eq!(err.message, "objectNotFound");
    }

    #[tokio::test]
    async fn offload_day_drops_local_copies() {
        let data = tempdir().unwrap();
        let methods = open_methods(&data);
        let descriptor = register_es_raw(&methods, &data, "offload.dbn.zst", "2026-03-10").await;

        let result = methods
            .handle(Request {
                method: ES_OFFLOAD_METHOD.to_string(),
                params: json!({ "marketDay": "2026-03-10" }),
            })
            .await
            .unwrap();

        assert_eq!(result["marketDay"], "2026-03-10");
        assert_eq!(result["bytesRemoved"], b"es raw bytes".len() as u64);
        assert_eq!(result["offloaded"].as_array().unwrap().len(), 1);
        let after = methods
            .store
            .get_object(&descriptor.id)
            .unwrap()
            .unwrap();
        assert!(after.local.is_none());
        assert!(after.remote.is_some());
    }

    #[tokio::test]
    async fn offload_unknown_day_errors() {
        let data = tempdir().unwrap();
        let methods = open_methods(&data);

        let err = methods
            .handle(Request {
                method: ES_OFFLOAD_METHOD.to_string(),
                params: json!({ "marketDay": "2026-03-10" }),
            })
            .await
            .unwrap_err();

        assert_eq!(err.code, DOMAIN_ERROR);
        assert!(err.message.contains("unknown ES market day"));
    }

    #[tokio::test]
    async fn offload_returns_object_not_found_for_unknown_id() {
        let data = tempdir().unwrap();
        let methods = open_methods(&data);
        let id = format!("sha256-{}", "0".repeat(64));

        let err = methods
            .handle(Request {
                method: STORE_OFFLOAD_METHOD.to_string(),
                params: json!({ "id": id }),
            })
            .await
            .unwrap_err();

        assert_eq!(err.message, "objectNotFound");
    }

    #[tokio::test]
    async fn hydrate_starts_day_tagged_registry_job() {
        let data = tempdir().unwrap();
        let methods = open_methods(&data);
        let descriptor = register_es_raw(&methods, &data, "hydrate.dbn.zst", "2026-03-10").await;

        let result = methods
            .handle(Request {
                method: STORE_HYDRATE_METHOD.to_string(),
                params: json!({ "id": descriptor.id.to_string() }),
            })
            .await
            .unwrap();

        assert_eq!(result["jobId"], "job-1");

        let jobs = methods
            .handle(Request {
                method: JOBS_LIST_METHOD.to_string(),
                params: Value::Null,
            })
            .await
            .unwrap();
        assert_eq!(jobs["jobs"][0]["kind"], "store.hydrate");
        assert_eq!(jobs["jobs"][0]["subject"], descriptor.id.to_string());
        assert_eq!(jobs["jobs"][0]["marketDay"], "2026-03-10");
    }

    #[tokio::test]
    async fn hydrate_returns_object_not_found_for_unknown_id() {
        let data = tempdir().unwrap();
        let methods = open_methods(&data);
        let id = format!("sha256-{}", "0".repeat(64));

        let err = methods
            .handle(Request {
                method: STORE_HYDRATE_METHOD.to_string(),
                params: json!({ "id": id }),
            })
            .await
            .unwrap_err();

        assert_eq!(err.message, "objectNotFound");
    }
}
