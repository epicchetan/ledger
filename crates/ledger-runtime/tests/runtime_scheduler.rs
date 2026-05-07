use std::sync::{Arc, Mutex};

use ledger_runtime::{
    CellDescriptor, CellKind, CellOwner, DataPlane, DataPlaneError, ExternalWriteBatch, Key,
    Projection, ProjectionContext, ProjectionDescriptor, ProjectionError, ProjectionId, Runtime,
    RuntimeError, ValueKey,
};

fn key(value: &str) -> Key {
    Key::new(value).unwrap()
}

fn projection_id(value: &str) -> ProjectionId {
    ProjectionId::new(value).unwrap()
}

fn descriptor(key_path: &str, owner: CellOwner, kind: CellKind) -> CellDescriptor {
    CellDescriptor {
        key: key(key_path),
        owner,
        kind,
        public_read: false,
    }
}

fn origin_owner() -> CellOwner {
    CellOwner::Origin("origin.es_mbo".to_string())
}

fn source_cell(data_plane: &DataPlane) -> ValueKey<i32> {
    data_plane
        .register_value(
            descriptor("origin.es_mbo.value", origin_owner(), CellKind::Value),
            None,
        )
        .unwrap()
}

fn projection_cell(data_plane: &DataPlane, key_path: &str, id: &ProjectionId) -> ValueKey<i32> {
    data_plane
        .register_value(descriptor(key_path, id.owner(), CellKind::Value), None)
        .unwrap()
}

fn external_set(owner: CellOwner, key: &ValueKey<i32>, value: i32) -> ExternalWriteBatch {
    let mut batch = ExternalWriteBatch::new(owner);
    batch.set_value(key, value);
    batch
}

fn log_snapshot(log: &Arc<Mutex<Vec<String>>>) -> Vec<String> {
    log.lock().unwrap().clone()
}

struct CopyProjection {
    descriptor: ProjectionDescriptor,
    input: ValueKey<i32>,
    output: ValueKey<i32>,
    output_offset: i32,
    log_name: &'static str,
    log: Arc<Mutex<Vec<String>>>,
    fail_after_write: bool,
}

impl CopyProjection {
    fn new(
        id: ProjectionId,
        dependency: Key,
        input: ValueKey<i32>,
        output: ValueKey<i32>,
        log_name: &'static str,
        log: Arc<Mutex<Vec<String>>>,
    ) -> Self {
        Self {
            descriptor: ProjectionDescriptor::new(id, vec![dependency]),
            input,
            output,
            output_offset: 0,
            log_name,
            log,
            fail_after_write: false,
        }
    }

    fn with_offset(mut self, output_offset: i32) -> Self {
        self.output_offset = output_offset;
        self
    }

    fn fail_after_write(mut self) -> Self {
        self.fail_after_write = true;
        self
    }
}

impl Projection for CopyProjection {
    fn descriptor(&self) -> &ProjectionDescriptor {
        &self.descriptor
    }

    fn run(&mut self, ctx: &mut ProjectionContext<'_>) -> Result<(), ProjectionError> {
        self.log.lock().unwrap().push(self.log_name.to_string());
        if let Some(value) = ctx.read_value(&self.input)? {
            ctx.set_value(&self.output, value + self.output_offset)?;
        }
        if self.fail_after_write {
            return Err(ProjectionError::InvalidProjectionId(
                "forced_failure".to_string(),
            ));
        }
        Ok(())
    }
}

struct FailingProjection {
    descriptor: ProjectionDescriptor,
    log: Arc<Mutex<Vec<String>>>,
}

impl Projection for FailingProjection {
    fn descriptor(&self) -> &ProjectionDescriptor {
        &self.descriptor
    }

    fn run(&mut self, _ctx: &mut ProjectionContext<'_>) -> Result<(), ProjectionError> {
        self.log.lock().unwrap().push("failing".to_string());
        Err(ProjectionError::InvalidProjectionId(
            "forced_failure".to_string(),
        ))
    }
}

struct IncrementSelfProjection {
    descriptor: ProjectionDescriptor,
    cell: ValueKey<i32>,
}

impl Projection for IncrementSelfProjection {
    fn descriptor(&self) -> &ProjectionDescriptor {
        &self.descriptor
    }

    fn run(&mut self, ctx: &mut ProjectionContext<'_>) -> Result<(), ProjectionError> {
        let next = ctx.read_value(&self.cell)?.unwrap_or(0) + 1;
        ctx.set_value(&self.cell, next)?;
        Ok(())
    }
}

#[test]
fn external_write_changed_key_queues_and_runs_dependent_projection() {
    let data_plane = DataPlane::new();
    let source = source_cell(&data_plane);
    let projection_id = projection_id("projection.copy");
    let output = projection_cell(&data_plane, "projection.copy.output", &projection_id);
    let log = Arc::new(Mutex::new(Vec::new()));
    let mut runtime = Runtime::new(data_plane.clone());
    runtime
        .register_projection(CopyProjection::new(
            projection_id.clone(),
            source.key().clone(),
            source.clone(),
            output.clone(),
            "copy",
            log.clone(),
        ))
        .unwrap();

    runtime.submit_external_writes(external_set(origin_owner(), &source, 7));
    let step = runtime.run_once().unwrap();

    assert_eq!(step.projection_run, Some(projection_id.clone()));
    assert_eq!(step.scheduled_projections, vec![projection_id]);
    assert_eq!(data_plane.read_value(&output).unwrap(), Some(7));
    assert_eq!(log_snapshot(&log), vec!["copy"]);
}

#[test]
fn duplicate_dependency_changes_run_projection_once() {
    let data_plane = DataPlane::new();
    let source = source_cell(&data_plane);
    let projection_id = projection_id("projection.copy");
    let output = projection_cell(&data_plane, "projection.copy.output", &projection_id);
    let log = Arc::new(Mutex::new(Vec::new()));
    let mut runtime = Runtime::new(data_plane.clone());
    runtime
        .register_projection(CopyProjection::new(
            projection_id,
            source.key().clone(),
            source.clone(),
            output,
            "copy",
            log.clone(),
        ))
        .unwrap();
    let mut batch = ExternalWriteBatch::new(origin_owner());
    batch.set_value(&source, 1).set_value(&source, 2);

    runtime.submit_external_writes(batch);
    runtime.run_until_idle(10).unwrap();

    assert_eq!(log_snapshot(&log), vec!["copy"]);
}

#[test]
fn projection_write_queues_downstream_projection() {
    let data_plane = DataPlane::new();
    let source = source_cell(&data_plane);
    let first_id = projection_id("projection.first");
    let second_id = projection_id("projection.second");
    let first_output = projection_cell(&data_plane, "projection.first.output", &first_id);
    let second_output = projection_cell(&data_plane, "projection.second.output", &second_id);
    let log = Arc::new(Mutex::new(Vec::new()));
    let mut runtime = Runtime::new(data_plane.clone());
    runtime
        .register_projection(
            CopyProjection::new(
                first_id.clone(),
                source.key().clone(),
                source.clone(),
                first_output.clone(),
                "first",
                log.clone(),
            )
            .with_offset(10),
        )
        .unwrap();
    runtime
        .register_projection(
            CopyProjection::new(
                second_id.clone(),
                first_output.key().clone(),
                first_output.clone(),
                second_output.clone(),
                "second",
                log.clone(),
            )
            .with_offset(100),
        )
        .unwrap();

    runtime.submit_external_writes(external_set(origin_owner(), &source, 1));
    let first_step = runtime.run_once().unwrap();
    let second_step = runtime.run_once().unwrap();

    assert_eq!(first_step.projection_run, Some(first_id));
    assert_eq!(
        first_step.scheduled_projections,
        vec![projection_id("projection.first"), second_id.clone()]
    );
    assert_eq!(second_step.projection_run, Some(second_id));
    assert_eq!(data_plane.read_value(&second_output).unwrap(), Some(111));
    assert_eq!(log_snapshot(&log), vec!["first", "second"]);
}

#[test]
fn downstream_projections_run_in_registration_order() {
    let data_plane = DataPlane::new();
    let source = source_cell(&data_plane);
    let first_id = projection_id("projection.first");
    let second_id = projection_id("projection.second");
    let first_output = projection_cell(&data_plane, "projection.first.output", &first_id);
    let second_output = projection_cell(&data_plane, "projection.second.output", &second_id);
    let log = Arc::new(Mutex::new(Vec::new()));
    let mut runtime = Runtime::new(data_plane.clone());
    runtime
        .register_projection(CopyProjection::new(
            first_id,
            source.key().clone(),
            source.clone(),
            first_output,
            "first",
            log.clone(),
        ))
        .unwrap();
    runtime
        .register_projection(CopyProjection::new(
            second_id,
            source.key().clone(),
            source.clone(),
            second_output,
            "second",
            log.clone(),
        ))
        .unwrap();

    runtime.submit_external_writes(external_set(origin_owner(), &source, 1));
    runtime.run_until_idle(10).unwrap();

    assert_eq!(log_snapshot(&log), vec!["first", "second"]);
}

#[test]
fn external_writes_are_applied_before_projection_runs() {
    let data_plane = DataPlane::new();
    let source = source_cell(&data_plane);
    let projection_id = projection_id("projection.copy");
    let output = projection_cell(&data_plane, "projection.copy.output", &projection_id);
    let log = Arc::new(Mutex::new(Vec::new()));
    let mut runtime = Runtime::new(data_plane.clone());
    runtime
        .register_projection(CopyProjection::new(
            projection_id.clone(),
            source.key().clone(),
            source.clone(),
            output.clone(),
            "copy",
            log,
        ))
        .unwrap();
    runtime.queue_projection(&projection_id).unwrap();
    runtime.submit_external_writes(external_set(origin_owner(), &source, 42));

    runtime.run_once().unwrap();

    assert_eq!(data_plane.read_value(&output).unwrap(), Some(42));
}

#[test]
fn external_write_batches_are_fifo() {
    let data_plane = DataPlane::new();
    let source = source_cell(&data_plane);
    let mut runtime = Runtime::new(data_plane.clone());

    runtime.submit_external_writes(external_set(origin_owner(), &source, 1));
    runtime.submit_external_writes(external_set(origin_owner(), &source, 2));
    let step = runtime.run_once().unwrap();

    assert_eq!(step.external_write_batches_applied, 2);
    assert_eq!(data_plane.read_value(&source).unwrap(), Some(2));
}

#[test]
fn one_run_once_runs_at_most_one_projection() {
    let data_plane = DataPlane::new();
    let source = source_cell(&data_plane);
    let first_id = projection_id("projection.first");
    let second_id = projection_id("projection.second");
    let first_output = projection_cell(&data_plane, "projection.first.output", &first_id);
    let second_output = projection_cell(&data_plane, "projection.second.output", &second_id);
    let log = Arc::new(Mutex::new(Vec::new()));
    let mut runtime = Runtime::new(data_plane);
    runtime
        .register_projection(CopyProjection::new(
            first_id.clone(),
            source.key().clone(),
            source.clone(),
            first_output,
            "first",
            log.clone(),
        ))
        .unwrap();
    runtime
        .register_projection(CopyProjection::new(
            second_id.clone(),
            source.key().clone(),
            source,
            second_output,
            "second",
            log.clone(),
        ))
        .unwrap();
    runtime.queue_projection(&first_id).unwrap();
    runtime.queue_projection(&second_id).unwrap();

    let step = runtime.run_once().unwrap();

    assert_eq!(step.projection_run, Some(first_id));
    assert!(!step.idle_after);
    assert_eq!(log_snapshot(&log), vec!["first"]);
}

#[test]
fn run_once_returns_idle_when_no_work_exists() {
    let data_plane = DataPlane::new();
    let mut runtime = Runtime::new(data_plane);

    let step = runtime.run_once().unwrap();

    assert_eq!(step.external_write_batches_applied, 0);
    assert_eq!(step.projection_run, None);
    assert!(step.changed_keys.is_empty());
    assert!(step.scheduled_projections.is_empty());
    assert!(step.idle_after);
}

#[test]
fn run_until_idle_drains_external_writes_and_projections() {
    let data_plane = DataPlane::new();
    let source = source_cell(&data_plane);
    let projection_id = projection_id("projection.copy");
    let output = projection_cell(&data_plane, "projection.copy.output", &projection_id);
    let log = Arc::new(Mutex::new(Vec::new()));
    let mut runtime = Runtime::new(data_plane.clone());
    runtime
        .register_projection(CopyProjection::new(
            projection_id,
            source.key().clone(),
            source.clone(),
            output.clone(),
            "copy",
            log,
        ))
        .unwrap();

    runtime.submit_external_writes(external_set(origin_owner(), &source, 9));
    let stats = runtime.run_until_idle(10).unwrap();

    assert_eq!(stats.steps, 1);
    assert_eq!(stats.external_write_batches_applied, 1);
    assert_eq!(stats.projections_run, 1);
    assert_eq!(
        stats.changed_keys,
        vec![key("origin.es_mbo.value"), key("projection.copy.output")]
    );
    assert_eq!(data_plane.read_value(&output).unwrap(), Some(9));
}

#[test]
fn run_until_idle_returns_limit_exceeded_for_cycle() {
    let data_plane = DataPlane::new();
    let projection_id = projection_id("projection.loop");
    let output = projection_cell(&data_plane, "projection.loop.output", &projection_id);
    let descriptor = ProjectionDescriptor::new(projection_id.clone(), vec![output.key().clone()]);
    let mut runtime = Runtime::new(data_plane);
    runtime
        .register_projection(IncrementSelfProjection {
            descriptor,
            cell: output,
        })
        .unwrap();
    runtime.queue_projection(&projection_id).unwrap();

    let err = runtime.run_until_idle(1).unwrap_err();

    assert_eq!(err, RuntimeError::RunLimitExceeded { max_steps: 1 });
}

#[test]
fn projection_errors_are_wrapped_with_projection_id() {
    let data_plane = DataPlane::new();
    let projection_id = projection_id("projection.failing");
    let log = Arc::new(Mutex::new(Vec::new()));
    let mut runtime = Runtime::new(data_plane);
    runtime
        .register_projection(FailingProjection {
            descriptor: ProjectionDescriptor::new(projection_id.clone(), Vec::new()),
            log,
        })
        .unwrap();
    runtime.queue_projection(&projection_id).unwrap();

    let err = runtime.run_once().unwrap_err();

    assert_eq!(
        err,
        RuntimeError::Projection {
            id: projection_id,
            source: ProjectionError::InvalidProjectionId("forced_failure".to_string()),
        }
    );
}

#[test]
fn projection_committed_writes_schedule_dependents_before_error_returns() {
    let data_plane = DataPlane::new();
    let source = source_cell(&data_plane);
    let failing_id = projection_id("projection.failing");
    let downstream_id = projection_id("projection.downstream");
    let failing_output = projection_cell(&data_plane, "projection.failing.output", &failing_id);
    let downstream_output =
        projection_cell(&data_plane, "projection.downstream.output", &downstream_id);
    let log = Arc::new(Mutex::new(Vec::new()));
    let mut runtime = Runtime::new(data_plane.clone());
    runtime
        .register_projection(
            CopyProjection::new(
                failing_id.clone(),
                source.key().clone(),
                source.clone(),
                failing_output.clone(),
                "failing",
                log.clone(),
            )
            .fail_after_write(),
        )
        .unwrap();
    runtime
        .register_projection(CopyProjection::new(
            downstream_id.clone(),
            failing_output.key().clone(),
            failing_output,
            downstream_output.clone(),
            "downstream",
            log.clone(),
        ))
        .unwrap();
    runtime.submit_external_writes(external_set(origin_owner(), &source, 3));

    let err = runtime.run_once().unwrap_err();
    assert!(matches!(err, RuntimeError::Projection { .. }));
    let step = runtime.run_once().unwrap();

    assert_eq!(step.projection_run, Some(downstream_id));
    assert_eq!(data_plane.read_value(&downstream_output).unwrap(), Some(3));
    assert_eq!(log_snapshot(&log), vec!["failing", "downstream"]);
}

#[test]
fn external_write_committed_writes_schedule_dependents_before_error_returns() {
    let data_plane = DataPlane::new();
    let source = source_cell(&data_plane);
    let wrong_owner_cell = data_plane
        .register_value(
            descriptor(
                "projection.wrong.output",
                CellOwner::Projection("projection.wrong".to_string()),
                CellKind::Value,
            ),
            None::<i32>,
        )
        .unwrap();
    let projection_id = projection_id("projection.copy");
    let output = projection_cell(&data_plane, "projection.copy.output", &projection_id);
    let log = Arc::new(Mutex::new(Vec::new()));
    let mut runtime = Runtime::new(data_plane.clone());
    runtime
        .register_projection(CopyProjection::new(
            projection_id.clone(),
            source.key().clone(),
            source.clone(),
            output.clone(),
            "copy",
            log,
        ))
        .unwrap();
    let mut failing_batch = ExternalWriteBatch::new(origin_owner());
    failing_batch
        .set_value(&source, 1)
        .set_value(&wrong_owner_cell, 100);
    runtime.submit_external_writes(failing_batch);
    runtime.submit_external_writes(external_set(origin_owner(), &source, 2));

    let err = runtime.run_once().unwrap_err();
    assert!(matches!(
        err,
        RuntimeError::DataPlane(DataPlaneError::OwnerMismatch { .. })
    ));
    let step = runtime.run_once().unwrap();

    assert_eq!(step.external_write_batches_applied, 1);
    assert_eq!(step.projection_run, Some(projection_id));
    assert_eq!(data_plane.read_value(&output).unwrap(), Some(2));
}

#[test]
fn phase_local_scheduling_allows_popped_projection_to_requeue_itself() {
    let data_plane = DataPlane::new();
    let projection_id = projection_id("projection.loop");
    let output = projection_cell(&data_plane, "projection.loop.output", &projection_id);
    let descriptor = ProjectionDescriptor::new(projection_id.clone(), vec![output.key().clone()]);
    let mut runtime = Runtime::new(data_plane.clone());
    runtime
        .register_projection(IncrementSelfProjection {
            descriptor,
            cell: output.clone(),
        })
        .unwrap();
    runtime.queue_projection(&projection_id).unwrap();

    let step = runtime.run_once().unwrap();

    assert_eq!(step.projection_run, Some(projection_id.clone()));
    assert_eq!(step.scheduled_projections, vec![projection_id]);
    assert!(!step.idle_after);
    assert_eq!(data_plane.read_value(&output).unwrap(), Some(1));
}

#[test]
fn direct_data_plane_writes_do_not_schedule_projections() {
    let data_plane = DataPlane::new();
    let source = source_cell(&data_plane);
    let projection_id = projection_id("projection.copy");
    let output = projection_cell(&data_plane, "projection.copy.output", &projection_id);
    let log = Arc::new(Mutex::new(Vec::new()));
    let mut runtime = Runtime::new(data_plane.clone());
    runtime
        .register_projection(CopyProjection::new(
            projection_id,
            source.key().clone(),
            source.clone(),
            output.clone(),
            "copy",
            log.clone(),
        ))
        .unwrap();

    data_plane
        .set_value(&origin_owner(), &source, 5)
        .expect("direct setup write should succeed");
    let stats = runtime.run_until_idle(10).unwrap();

    assert_eq!(stats.steps, 0);
    assert_eq!(data_plane.read_value(&output).unwrap(), None);
    assert!(log_snapshot(&log).is_empty());
}

#[test]
fn duplicate_projection_registration_is_rejected() {
    let data_plane = DataPlane::new();
    let source = source_cell(&data_plane);
    let projection_id = projection_id("projection.copy");
    let first_output = projection_cell(&data_plane, "projection.copy.first", &projection_id);
    let log = Arc::new(Mutex::new(Vec::new()));
    let mut runtime = Runtime::new(data_plane.clone());
    runtime
        .register_projection(CopyProjection::new(
            projection_id.clone(),
            source.key().clone(),
            source.clone(),
            first_output,
            "copy",
            log.clone(),
        ))
        .unwrap();

    let duplicate = CopyProjection::new(
        projection_id.clone(),
        source.key().clone(),
        source,
        projection_cell(&data_plane, "projection.copy.second", &projection_id),
        "copy2",
        log,
    );
    let err = runtime.register_projection(duplicate).unwrap_err();

    assert_eq!(err, RuntimeError::DuplicateProjection(projection_id));
}

#[test]
fn queue_projection_rejects_unknown_projection_id() {
    let data_plane = DataPlane::new();
    let mut runtime = Runtime::new(data_plane);
    let unknown = projection_id("projection.unknown");

    let err = runtime.queue_projection(&unknown).unwrap_err();

    assert_eq!(err, RuntimeError::MissingProjection(unknown));
}

#[test]
fn duplicate_dependencies_in_one_descriptor_are_indexed_once() {
    let data_plane = DataPlane::new();
    let source = source_cell(&data_plane);
    let projection_id = projection_id("projection.copy");
    let output = projection_cell(&data_plane, "projection.copy.output", &projection_id);
    let log = Arc::new(Mutex::new(Vec::new()));
    let descriptor = ProjectionDescriptor::new(
        projection_id,
        vec![source.key().clone(), source.key().clone()],
    );
    let mut projection = CopyProjection::new(
        descriptor.id.clone(),
        source.key().clone(),
        source.clone(),
        output,
        "copy",
        log.clone(),
    );
    projection.descriptor = descriptor;
    let mut runtime = Runtime::new(data_plane);
    runtime.register_projection(projection).unwrap();

    runtime.submit_external_writes(external_set(origin_owner(), &source, 1));
    runtime.run_until_idle(10).unwrap();

    assert_eq!(log_snapshot(&log), vec!["copy"]);
}

#[test]
fn empty_dependencies_are_allowed() {
    let data_plane = DataPlane::new();
    let projection_id = projection_id("projection.failing");
    let log = Arc::new(Mutex::new(Vec::new()));
    let mut runtime = Runtime::new(data_plane);
    runtime
        .register_projection(FailingProjection {
            descriptor: ProjectionDescriptor::new(projection_id.clone(), Vec::new()),
            log: log.clone(),
        })
        .unwrap();

    runtime.queue_projection(&projection_id).unwrap();
    let err = runtime.run_once().unwrap_err();

    assert!(matches!(err, RuntimeError::Projection { .. }));
    assert_eq!(log_snapshot(&log), vec!["failing"]);
}
