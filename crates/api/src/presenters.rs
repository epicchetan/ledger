use crate::dto::{
    DataCenterMarketDay, DataCenterObjectSummary, DataCenterRawDataLayer, DataCenterRawDataStatus,
    DataCenterReplayArtifact, DataCenterReplayDatasetLayer, DataCenterReplayDatasetStatus,
    DataCenterTrustStatus, DataCenterValidationCheck, DataCenterValidationCheckStatus,
    DataCenterValidationIssue, DataCenterValidationIssueSeverity, DataCenterValidationMode,
    DataCenterValidationStatus, DataCenterValidationSummary, DataCenterValidationTrigger,
};
use crate::time::{ns_iso, ns_string};
use ledger_domain::MarketDay;
use ledger_store::{
    MarketDayRecord, RawMarketDataRecord, RawMarketDataStatus, ReplayDatasetObjectStatus,
    ReplayDatasetRecord, ReplayDatasetRecordStatus, ReplayDatasetStatus, StoredObject,
    ValidationMode, ValidationReportRecord, ValidationReportStatus,
};
use serde_json::Value;

pub(crate) fn data_center_market_day(record: MarketDayRecord) -> DataCenterMarketDay {
    data_center_market_day_from_parts(
        record.market_day,
        true,
        record.raw,
        record.replay_dataset,
        false,
        false,
        Vec::new(),
        record.last_validation,
    )
}

pub(crate) fn data_center_market_day_status(status: ReplayDatasetStatus) -> DataCenterMarketDay {
    data_center_market_day_from_parts(
        status.market_day,
        status.catalog_found,
        status.raw,
        status.replay_dataset,
        status.replay_artifacts_available,
        status.replay_objects_valid,
        status.objects,
        status.last_validation,
    )
}

fn data_center_market_day_from_parts(
    market_day: MarketDay,
    catalog_found: bool,
    raw: Option<RawMarketDataRecord>,
    replay_dataset: Option<ReplayDatasetRecord>,
    artifacts_available: bool,
    objects_valid: bool,
    artifacts: Vec<ReplayDatasetObjectStatus>,
    validation: Option<ValidationReportRecord>,
) -> DataCenterMarketDay {
    DataCenterMarketDay {
        id: market_day.id,
        root: market_day.root,
        contract: market_day.contract_symbol,
        market_date: market_day.market_date,
        timezone: market_day.timezone,
        data_start_ns: ns_string(market_day.data_start_ns),
        data_start_iso: ns_iso(market_day.data_start_ns),
        data_end_ns: ns_string(market_day.data_end_ns),
        data_end_iso: ns_iso(market_day.data_end_ns),
        rth_start_ns: ns_string(market_day.rth_start_ns),
        rth_start_iso: ns_iso(market_day.rth_start_ns),
        rth_end_ns: ns_string(market_day.rth_end_ns),
        rth_end_iso: ns_iso(market_day.rth_end_ns),
        market_day_status: market_day.status,
        catalog_found,
        raw: data_center_raw_data(raw),
        replay_dataset: data_center_replay_dataset(
            replay_dataset,
            artifacts_available,
            objects_valid,
            artifacts,
            validation,
        ),
    }
}

fn data_center_raw_data(raw: Option<RawMarketDataRecord>) -> DataCenterRawDataLayer {
    let Some(raw) = raw else {
        return DataCenterRawDataLayer {
            status: DataCenterRawDataStatus::Missing,
            provider: None,
            dataset: None,
            schema: None,
            source_symbol: None,
            object: None,
            updated_at_ns: None,
            updated_at_iso: None,
        };
    };
    let updated_at_ns = raw.updated_at_ns;

    DataCenterRawDataLayer {
        status: match raw.status {
            RawMarketDataStatus::Missing => DataCenterRawDataStatus::Missing,
            RawMarketDataStatus::Available => DataCenterRawDataStatus::Available,
            RawMarketDataStatus::Error => DataCenterRawDataStatus::Error,
        },
        provider: Some(raw.provider),
        dataset: Some(raw.dataset),
        schema: Some(raw.schema),
        source_symbol: Some(raw.source_symbol),
        object: Some(data_center_object(raw.object)),
        updated_at_ns: Some(ns_string(updated_at_ns)),
        updated_at_iso: Some(ns_iso(updated_at_ns)),
    }
}

fn data_center_replay_dataset(
    replay_dataset: Option<ReplayDatasetRecord>,
    artifacts_available: bool,
    objects_valid: bool,
    artifacts: Vec<ReplayDatasetObjectStatus>,
    validation: Option<ValidationReportRecord>,
) -> DataCenterReplayDatasetLayer {
    let validation = validation.map(data_center_validation);
    let artifacts = artifacts
        .into_iter()
        .map(data_center_replay_artifact)
        .collect();

    let Some(replay_dataset) = replay_dataset else {
        return DataCenterReplayDatasetLayer {
            status: DataCenterReplayDatasetStatus::Missing,
            id: None,
            raw_object_key: None,
            schema_version: None,
            producer: None,
            producer_version: None,
            artifact_set_hash: None,
            updated_at_ns: None,
            updated_at_iso: None,
            artifacts_available: false,
            objects_valid: false,
            artifacts,
            validation,
        };
    };
    let updated_at_ns = replay_dataset.updated_at_ns;

    DataCenterReplayDatasetLayer {
        status: match replay_dataset.status {
            ReplayDatasetRecordStatus::Missing => DataCenterReplayDatasetStatus::Missing,
            ReplayDatasetRecordStatus::Building => DataCenterReplayDatasetStatus::Building,
            ReplayDatasetRecordStatus::Available => DataCenterReplayDatasetStatus::Available,
            ReplayDatasetRecordStatus::Invalid => DataCenterReplayDatasetStatus::Invalid,
        },
        id: Some(replay_dataset.id),
        raw_object_key: Some(replay_dataset.raw_object_remote_key),
        schema_version: Some(replay_dataset.schema_version),
        producer: replay_dataset.producer,
        producer_version: replay_dataset.producer_version,
        artifact_set_hash: Some(replay_dataset.artifact_set_hash),
        updated_at_ns: Some(ns_string(updated_at_ns)),
        updated_at_iso: Some(ns_iso(updated_at_ns)),
        artifacts_available,
        objects_valid,
        artifacts,
        validation,
    }
}

fn data_center_object(object: StoredObject) -> DataCenterObjectSummary {
    DataCenterObjectSummary {
        kind: object.kind,
        logical_key: object.logical_key,
        format: object.format,
        schema_version: object.schema_version,
        content_sha256: object.content_sha256,
        size_bytes: object.size_bytes,
        remote_key: object.remote_key,
    }
}

fn data_center_replay_artifact(artifact: ReplayDatasetObjectStatus) -> DataCenterReplayArtifact {
    DataCenterReplayArtifact {
        kind: artifact.kind,
        remote_key: artifact.remote_key,
        size_bytes: artifact.size_bytes,
        content_sha256: artifact.content_sha256,
        object_valid: artifact.object_valid,
    }
}

fn data_center_validation(report: ValidationReportRecord) -> DataCenterValidationSummary {
    let counts = report.report_json.get("counts");
    let checks = validation_checks(&report.report_json);
    let issues = validation_issues(&report.report_json);
    let warnings = report_warnings(&report.report_json, &issues);
    let warning_count = report.warning_count.max(warnings.len() as i64) as u64;
    let error_count = report.error_count.max(
        issues
            .iter()
            .filter(|issue| issue.severity == DataCenterValidationIssueSeverity::Error)
            .count() as i64,
    ) as u64;
    let summary = report_summary(&report);

    DataCenterValidationSummary {
        mode: match report.mode {
            ValidationMode::Light => DataCenterValidationMode::Light,
            ValidationMode::Full => DataCenterValidationMode::Full,
        },
        status: match report.status {
            ValidationReportStatus::Valid => DataCenterValidationStatus::Valid,
            ValidationReportStatus::Warning => DataCenterValidationStatus::Warning,
            ValidationReportStatus::Invalid => DataCenterValidationStatus::Invalid,
        },
        trigger: validation_trigger(&report),
        trust_status: trust_status(&report),
        summary,
        recommended_action: report
            .report_json
            .get("recommended_action")
            .and_then(Value::as_str)
            .map(str::to_string),
        created_at_ns: ns_string(report.created_at_ns),
        created_at_iso: ns_iso(report.created_at_ns),
        event_count: counts
            .and_then(|counts| counts.get("event_count"))
            .and_then(Value::as_u64),
        batch_count: counts
            .and_then(|counts| counts.get("batch_count"))
            .and_then(Value::as_u64),
        trade_count: counts
            .and_then(|counts| counts.get("trade_count"))
            .and_then(Value::as_u64),
        check_count: checks.len() as u64,
        passed_check_count: checks
            .iter()
            .filter(|check| check.status == DataCenterValidationCheckStatus::Pass)
            .count() as u64,
        warning_count,
        error_count,
        checks,
        issues,
        warnings,
    }
}

fn report_summary(report: &ValidationReportRecord) -> String {
    if !report.summary.is_empty() {
        return report.summary.clone();
    }
    report
        .report_json
        .get("summary")
        .and_then(Value::as_str)
        .map(str::to_string)
        .unwrap_or_else(|| {
            match report.status {
                ValidationReportStatus::Valid => "ReplayDataset has a persisted validation report.",
                ValidationReportStatus::Warning => {
                    "ReplayDataset is usable but has validation warnings."
                }
                ValidationReportStatus::Invalid => "ReplayDataset is invalid.",
            }
            .to_string()
        })
}

fn validation_trigger(report: &ValidationReportRecord) -> DataCenterValidationTrigger {
    let value = report
        .report_json
        .get("trigger")
        .and_then(Value::as_str)
        .unwrap_or(&report.trigger);
    match value {
        "prepare" => DataCenterValidationTrigger::Prepare,
        "rebuild" => DataCenterValidationTrigger::Rebuild,
        _ => DataCenterValidationTrigger::Manual,
    }
}

fn trust_status(report: &ValidationReportRecord) -> DataCenterTrustStatus {
    let value = report
        .report_json
        .get("trust_status")
        .or_else(|| report.report_json.get("status"))
        .and_then(Value::as_str)
        .unwrap_or(&report.trust_status);
    match value {
        "missing" => DataCenterTrustStatus::Missing,
        "raw_available" => DataCenterTrustStatus::RawAvailable,
        "ready_to_train" => DataCenterTrustStatus::ReadyToTrain,
        "ready_with_warnings" => DataCenterTrustStatus::ReadyWithWarnings,
        "invalid" => DataCenterTrustStatus::Invalid,
        _ => DataCenterTrustStatus::ReplayDatasetAvailable,
    }
}

fn validation_checks(report_json: &Value) -> Vec<DataCenterValidationCheck> {
    report_json
        .get("checks")
        .and_then(Value::as_array)
        .map(|checks| {
            checks
                .iter()
                .map(|check| DataCenterValidationCheck {
                    id: string_field(check, "id", "unknown"),
                    label: string_field(check, "label", "Check"),
                    status: check_status(
                        check
                            .get("status")
                            .and_then(Value::as_str)
                            .unwrap_or("skipped"),
                    ),
                    summary: string_field(check, "summary", ""),
                })
                .collect()
        })
        .unwrap_or_default()
}

fn validation_issues(report_json: &Value) -> Vec<DataCenterValidationIssue> {
    report_json
        .get("issues")
        .and_then(Value::as_array)
        .map(|issues| {
            issues
                .iter()
                .map(|issue| DataCenterValidationIssue {
                    severity: match issue
                        .get("severity")
                        .and_then(Value::as_str)
                        .unwrap_or("warning")
                    {
                        "error" => DataCenterValidationIssueSeverity::Error,
                        _ => DataCenterValidationIssueSeverity::Warning,
                    },
                    code: string_field(issue, "code", "validation.issue"),
                    message: string_field(issue, "message", ""),
                    check_id: string_field(issue, "check_id", "validation"),
                    recommended_action: issue
                        .get("recommended_action")
                        .and_then(Value::as_str)
                        .map(str::to_string),
                })
                .collect()
        })
        .unwrap_or_default()
}

fn report_warnings(report_json: &Value, issues: &[DataCenterValidationIssue]) -> Vec<String> {
    let warnings = report_json
        .get("warnings")
        .and_then(Value::as_array)
        .map(|warnings| {
            warnings
                .iter()
                .filter_map(Value::as_str)
                .map(str::to_string)
                .collect::<Vec<_>>()
        })
        .unwrap_or_default();
    if !warnings.is_empty() {
        return warnings;
    }
    issues
        .iter()
        .filter(|issue| issue.severity == DataCenterValidationIssueSeverity::Warning)
        .map(|issue| issue.message.clone())
        .collect()
}

fn check_status(value: &str) -> DataCenterValidationCheckStatus {
    match value {
        "pass" => DataCenterValidationCheckStatus::Pass,
        "warning" => DataCenterValidationCheckStatus::Warning,
        "fail" => DataCenterValidationCheckStatus::Fail,
        _ => DataCenterValidationCheckStatus::Skipped,
    }
}

fn string_field(value: &Value, key: &str, default: &str) -> String {
    value
        .get(key)
        .and_then(Value::as_str)
        .unwrap_or(default)
        .to_string()
}
