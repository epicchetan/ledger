use crate::dto::{
    DeleteStoreObjectResponse, HealthResponse, LocalStoreObject, StoreObject, StoreObjectListQuery,
    StoreRemoteObject,
};
use crate::error::ApiError;
use crate::state::ApiState;
use crate::time::{ns_iso, ns_string};
use axum::extract::{Path, Query, State};
use axum::Json;
use std::time::Instant;
use store::{
    DeleteObjectReport, ObjectFilter, RemoteObjectLocation, StoreObjectDescriptor, StoreObjectId,
    StoreObjectRole,
};

pub(crate) async fn health() -> Json<HealthResponse> {
    Json(HealthResponse {
        ok: true,
        service: "ledger-api".to_string(),
    })
}

pub(crate) async fn list_store_objects(
    State(state): State<ApiState>,
    Query(query): Query<StoreObjectListQuery>,
) -> Result<Json<Vec<StoreObject>>, ApiError> {
    let started_at = log_start("GET /store/objects");
    let filter = object_filter(query)?;
    let objects = state
        .store
        .list_objects(filter)
        .map_err(ApiError::internal)?
        .into_iter()
        .map(store_object)
        .collect();
    log_done("GET /store/objects", started_at);
    Ok(Json(objects))
}

pub(crate) async fn get_store_object(
    State(state): State<ApiState>,
    Path(id): Path<String>,
) -> Result<Json<StoreObject>, ApiError> {
    let label = format!("GET /store/objects/{id}");
    let started_at = log_start(&label);
    let id = parse_object_id(&id)?;
    let object = state
        .store
        .get_object(&id)
        .map_err(ApiError::internal)?
        .map(store_object)
        .ok_or_else(|| ApiError::not_found(format!("store object {id} not found")))?;
    log_done(&label, started_at);
    Ok(Json(object))
}

pub(crate) async fn delete_store_object(
    State(state): State<ApiState>,
    Path(id): Path<String>,
) -> Result<Json<DeleteStoreObjectResponse>, ApiError> {
    let label = format!("DELETE /store/objects/{id}");
    let started_at = log_start(&label);
    let id = parse_object_id(&id)?;
    let report = state
        .store
        .delete_object(&id)
        .await
        .map_err(ApiError::internal)?;
    if !report.descriptor_removed {
        return Err(ApiError::not_found(format!("store object {id} not found")));
    }
    log_done(&label, started_at);
    Ok(Json(delete_report(report)))
}

fn object_filter(query: StoreObjectListQuery) -> Result<ObjectFilter, ApiError> {
    Ok(ObjectFilter {
        role: query
            .role
            .as_deref()
            .map(StoreObjectRole::parse)
            .transpose()
            .map_err(|err| ApiError::bad_request(err.to_string()))?,
        kind: query.kind,
        id_prefix: query.id_prefix,
    })
}

fn parse_object_id(id: &str) -> Result<StoreObjectId, ApiError> {
    StoreObjectId::new(id).map_err(|err| ApiError::bad_request(err.to_string()))
}

fn store_object(descriptor: StoreObjectDescriptor) -> StoreObject {
    StoreObject {
        id: descriptor.id.to_string(),
        role: descriptor.role.as_str().to_string(),
        kind: descriptor.kind,
        file_name: descriptor.file_name,
        content_sha256: descriptor.content_sha256,
        size_bytes: descriptor.size_bytes,
        format: descriptor.format,
        media_type: descriptor.media_type,
        remote: descriptor.remote.map(store_remote),
        local: descriptor.local.map(|local| LocalStoreObject {
            relative_path: local.relative_path.display().to_string(),
            size_bytes: local.size_bytes,
            last_accessed_at_ns: ns_string(local.last_accessed_at_ns),
            last_accessed_at_iso: ns_iso(local.last_accessed_at_ns),
        }),
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

fn store_remote(remote: RemoteObjectLocation) -> StoreRemoteObject {
    StoreRemoteObject {
        bucket: remote.bucket,
        key: remote.key,
        size_bytes: remote.size_bytes,
        sha256: remote.sha256,
        etag: remote.etag,
    }
}

fn delete_report(report: DeleteObjectReport) -> DeleteStoreObjectResponse {
    DeleteStoreObjectResponse {
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

fn log_start(label: &str) -> Instant {
    eprintln!("[ledger-api] {label}");
    Instant::now()
}

fn log_done(label: &str, started_at: Instant) {
    eprintln!(
        "[ledger-api] {label} completed ({:.2}s)",
        started_at.elapsed().as_secs_f64()
    );
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn health_reports_ok() {
        let Json(response) = health().await;
        assert!(response.ok);
        assert_eq!(response.service, "ledger-api");
    }

    #[test]
    fn object_filter_rejects_unknown_role() {
        let err = object_filter(StoreObjectListQuery {
            role: Some("unknown".to_string()),
            kind: None,
            id_prefix: None,
        })
        .unwrap_err();

        assert!(err.message.contains("unknown store object role"));
    }
}
