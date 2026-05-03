use super::{
    ProjectionAdvance, ProjectionContext, ProjectionFrameDraft, ProjectionMetrics, ProjectionNode,
    ProjectionRegistry, TruthTick,
};
use anyhow::{bail, ensure, Context, Result};
use indexmap::{IndexMap, IndexSet};
use ledger_domain::{
    now_ns, ProjectionExecutionType, ProjectionFrame, ProjectionFrameOp, ProjectionFrameStamp,
    ProjectionKey, ProjectionLagPolicy, ProjectionManifest, ProjectionSpec,
    ProjectionWakeEventMask, ProjectionWakePolicy, UnixNanos,
};
use serde_json::Value;
use std::time::Instant;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProjectionRuntimeConfig {
    pub session_id: String,
    pub replay_dataset_id: String,
    pub initial_cursor: ProjectionRuntimeCursor,
}

impl Default for ProjectionRuntimeConfig {
    fn default() -> Self {
        Self {
            session_id: "synthetic-session".to_string(),
            replay_dataset_id: "synthetic-replay-dataset".to_string(),
            initial_cursor: ProjectionRuntimeCursor::default(),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ProjectionRuntimeCursor {
    pub batch_idx: u64,
    pub cursor_ts_ns: UnixNanos,
}

impl ProjectionRuntimeCursor {
    pub fn new(batch_idx: u64, cursor_ts_ns: UnixNanos) -> Self {
        Self {
            batch_idx,
            cursor_ts_ns,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct ProjectionSubscriptionId(u64);

impl ProjectionSubscriptionId {
    pub fn get(self) -> u64 {
        self.0
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct ProjectionSubscription {
    pub id: ProjectionSubscriptionId,
    pub key: ProjectionKey,
    pub initial_frames: Vec<ProjectionFrame>,
}

pub struct ProjectionRuntime {
    registry: ProjectionRegistry,
    config: ProjectionRuntimeConfig,
    cursor: ProjectionRuntimeCursor,
    nodes: IndexMap<ProjectionKey, NodeEntry>,
    deps: IndexMap<ProjectionKey, Vec<ProjectionKey>>,
    reverse_deps: IndexMap<ProjectionKey, Vec<ProjectionKey>>,
    active_topological_order: Vec<ProjectionKey>,
    subscriptions: IndexMap<ProjectionSubscriptionId, SubscriptionEntry>,
    last_payloads: IndexMap<ProjectionKey, Value>,
    last_due_ts_ns: IndexMap<ProjectionKey, UnixNanos>,
    generation: u64,
    sequence: u64,
    next_subscription_id: u64,
    metrics: ProjectionMetrics,
}

impl ProjectionRuntime {
    pub fn new(registry: ProjectionRegistry, config: ProjectionRuntimeConfig) -> Self {
        let cursor = config.initial_cursor;
        Self {
            registry,
            config,
            cursor,
            nodes: IndexMap::new(),
            deps: IndexMap::new(),
            reverse_deps: IndexMap::new(),
            active_topological_order: Vec::new(),
            subscriptions: IndexMap::new(),
            last_payloads: IndexMap::new(),
            last_due_ts_ns: IndexMap::new(),
            generation: 0,
            sequence: 0,
            next_subscription_id: 1,
            metrics: ProjectionMetrics::default(),
        }
    }

    pub fn registry(&self) -> &ProjectionRegistry {
        &self.registry
    }

    pub fn generation(&self) -> u64 {
        self.generation
    }

    pub fn metrics(&self) -> &ProjectionMetrics {
        &self.metrics
    }

    pub fn cursor(&self) -> ProjectionRuntimeCursor {
        self.cursor
    }

    pub fn active_topological_order(&self) -> &[ProjectionKey] {
        &self.active_topological_order
    }

    pub fn active_node_count(&self) -> usize {
        self.nodes.len()
    }

    pub fn ref_count(&self, key: &ProjectionKey) -> Option<usize> {
        self.nodes.get(key).map(|entry| entry.ref_count)
    }

    pub fn last_payload(&self, key: &ProjectionKey) -> Option<&Value> {
        self.last_payloads.get(key)
    }

    pub fn subscribe(&mut self, spec: ProjectionSpec) -> Result<ProjectionSubscription> {
        let requested_key = self.ensure_node(spec, &mut Vec::new())?;
        let closure = self.collect_closure(&requested_key)?;

        for key in &closure {
            let entry = self
                .nodes
                .get_mut(key)
                .with_context(|| format!("projection node {key} missing for subscription"))?;
            entry.ref_count += 1;
        }
        self.recompute_topological_order()?;

        let id = ProjectionSubscriptionId(self.next_subscription_id);
        self.next_subscription_id += 1;
        self.subscriptions.insert(
            id,
            SubscriptionEntry {
                key: requested_key.clone(),
                closure,
            },
        );

        let initial_frames = vec![self.snapshot_frame(
            &requested_key,
            ProjectionFrameOp::Snapshot,
            self.cursor.batch_idx,
            self.cursor.cursor_ts_ns,
        )?];

        Ok(ProjectionSubscription {
            id,
            key: requested_key,
            initial_frames,
        })
    }

    pub fn unsubscribe(&mut self, id: ProjectionSubscriptionId) -> Result<()> {
        let subscription = self
            .subscriptions
            .swap_remove(&id)
            .with_context(|| format!("projection subscription {} does not exist", id.get()))?;

        for key in &subscription.closure {
            let entry = self
                .nodes
                .get_mut(key)
                .with_context(|| format!("projection node {key} missing during unsubscribe"))?;
            ensure!(
                entry.ref_count > 0,
                "projection node {key} has invalid zero ref count"
            );
            entry.ref_count -= 1;
        }

        let removable = self
            .nodes
            .iter()
            .filter_map(|(key, entry)| (entry.ref_count == 0).then_some(key.clone()))
            .collect::<Vec<_>>();
        for key in removable {
            self.remove_node(&key);
        }

        self.recompute_topological_order()?;
        Ok(())
    }

    pub fn advance(&mut self, tick: TruthTick) -> Result<Vec<ProjectionFrame>> {
        self.cursor = ProjectionRuntimeCursor::new(tick.batch_idx, tick.cursor_ts_ns);
        let mut changed = IndexSet::<ProjectionKey>::new();
        let mut frames = Vec::new();

        for key in self.active_topological_order.clone() {
            let dep_changed = self
                .deps
                .get(&key)
                .into_iter()
                .flatten()
                .any(|dep| changed.contains(dep));
            let due = dep_changed || self.wake_due(&key, &tick)?;

            if !due {
                self.metrics.record_skip(&key);
                continue;
            }

            let dependency_payloads = self.dependency_payloads(&key)?;
            let ctx = ProjectionContext::new(&tick, &key, dependency_payloads);
            let started_at = Instant::now();
            let advance = {
                let entry = self
                    .nodes
                    .get_mut(&key)
                    .with_context(|| format!("projection node {key} missing during advance"))?;
                entry.node.advance(&ctx)?
            };

            match advance {
                ProjectionAdvance::NoChange => {}
                ProjectionAdvance::StateChanged => {
                    changed.insert(key.clone());
                    let snapshot = self.node_snapshot(&key)?;
                    self.last_payloads.insert(key.clone(), snapshot);
                }
                ProjectionAdvance::NeedsAsyncWork(reason) => {
                    bail!("projection node {key} requested async work: {reason}");
                }
            }

            self.last_due_ts_ns.insert(key.clone(), tick.cursor_ts_ns);
            let drafts = self.drain_node_frames(&key)?;
            let frame_count = drafts.len();
            for draft in drafts {
                frames.push(self.stamp_frame(&key, draft, tick.batch_idx, tick.cursor_ts_ns)?);
            }
            self.metrics
                .record_advance(&key, started_at.elapsed(), frame_count);
        }

        Ok(frames)
    }

    pub fn reset(&mut self) -> Result<Vec<ProjectionFrame>> {
        self.reset_at(self.cursor)
    }

    pub fn reset_at(&mut self, cursor: ProjectionRuntimeCursor) -> Result<Vec<ProjectionFrame>> {
        self.cursor = cursor;
        self.generation += 1;
        self.sequence = 0;
        self.metrics.clear();
        self.last_due_ts_ns.clear();
        self.last_payloads.clear();

        for (key, entry) in &mut self.nodes {
            entry.node.reset()?;
            self.last_payloads
                .insert(key.clone(), entry.node.snapshot());
        }

        let requested_keys = self
            .subscriptions
            .values()
            .map(|subscription| subscription.key.clone())
            .collect::<IndexSet<_>>();
        requested_keys
            .iter()
            .map(|key| {
                self.snapshot_frame(
                    key,
                    ProjectionFrameOp::Snapshot,
                    self.cursor.batch_idx,
                    self.cursor.cursor_ts_ns,
                )
            })
            .collect()
    }

    fn ensure_node(
        &mut self,
        spec: ProjectionSpec,
        stack: &mut Vec<ProjectionKey>,
    ) -> Result<ProjectionKey> {
        let spec = self.registry.normalize_spec(&spec)?;
        let key = spec.key()?;

        if stack.contains(&key) {
            let cycle = stack
                .iter()
                .chain(std::iter::once(&key))
                .map(ToString::to_string)
                .collect::<Vec<_>>()
                .join(" -> ");
            bail!("projection dependency cycle detected: {cycle}");
        }

        if self.nodes.contains_key(&key) {
            return Ok(key);
        }

        stack.push(key.clone());
        let factory = self.registry.factory(&spec.id, spec.version)?;
        let dependency_specs = factory
            .resolve_dependencies(&spec.params)
            .with_context(|| format!("resolving dependencies for projection {key}"))?;
        let mut dep_keys = Vec::with_capacity(dependency_specs.len());
        for dependency_spec in dependency_specs {
            dep_keys.push(self.ensure_node(dependency_spec, stack)?);
        }
        stack.pop();

        let manifest = factory.manifest().clone();
        ensure_supported_execution(&key, &manifest)?;
        let node = factory
            .build(spec.clone(), key.clone())
            .with_context(|| format!("building projection node {key}"))?;
        let snapshot = node.snapshot();

        self.nodes.insert(
            key.clone(),
            NodeEntry {
                manifest,
                node,
                ref_count: 0,
            },
        );
        self.deps.insert(key.clone(), dep_keys.clone());
        self.last_payloads.insert(key.clone(), snapshot);
        for dep_key in dep_keys {
            self.reverse_deps
                .entry(dep_key)
                .or_default()
                .push(key.clone());
        }

        Ok(key)
    }

    fn collect_closure(&self, key: &ProjectionKey) -> Result<Vec<ProjectionKey>> {
        let mut out = IndexSet::new();
        self.collect_closure_inner(key, &mut out)?;
        Ok(out.into_iter().collect())
    }

    fn collect_closure_inner(
        &self,
        key: &ProjectionKey,
        out: &mut IndexSet<ProjectionKey>,
    ) -> Result<()> {
        if !self.nodes.contains_key(key) {
            bail!("projection node {key} missing while collecting dependency closure");
        }
        if out.insert(key.clone()) {
            if let Some(deps) = self.deps.get(key) {
                for dep in deps {
                    self.collect_closure_inner(dep, out)?;
                }
            }
        }
        Ok(())
    }

    fn remove_node(&mut self, key: &ProjectionKey) {
        self.nodes.swap_remove(key);
        self.last_payloads.swap_remove(key);
        self.last_due_ts_ns.swap_remove(key);

        if let Some(deps) = self.deps.swap_remove(key) {
            for dep in deps {
                if let Some(reverse) = self.reverse_deps.get_mut(&dep) {
                    reverse.retain(|candidate| candidate != key);
                    if reverse.is_empty() {
                        self.reverse_deps.swap_remove(&dep);
                    }
                }
            }
        }
        self.reverse_deps.swap_remove(key);
    }

    fn recompute_topological_order(&mut self) -> Result<()> {
        let active = self
            .nodes
            .iter()
            .filter_map(|(key, entry)| (entry.ref_count > 0).then_some(key.clone()))
            .collect::<IndexSet<_>>();
        let mut ordered = Vec::with_capacity(active.len());
        let mut visiting = IndexSet::new();
        let mut visited = IndexSet::new();

        for key in &active {
            self.visit_active(key, &active, &mut visiting, &mut visited, &mut ordered)?;
        }

        self.active_topological_order = ordered;
        Ok(())
    }

    fn visit_active(
        &self,
        key: &ProjectionKey,
        active: &IndexSet<ProjectionKey>,
        visiting: &mut IndexSet<ProjectionKey>,
        visited: &mut IndexSet<ProjectionKey>,
        ordered: &mut Vec<ProjectionKey>,
    ) -> Result<()> {
        if visited.contains(key) {
            return Ok(());
        }
        if !visiting.insert(key.clone()) {
            bail!("projection dependency cycle detected while ordering at {key}");
        }
        if let Some(deps) = self.deps.get(key) {
            for dep in deps {
                if active.contains(dep) {
                    self.visit_active(dep, active, visiting, visited, ordered)?;
                }
            }
        }
        visiting.swap_remove(key);
        visited.insert(key.clone());
        ordered.push(key.clone());
        Ok(())
    }

    fn wake_due(&self, key: &ProjectionKey, tick: &TruthTick) -> Result<bool> {
        let entry = self
            .nodes
            .get(key)
            .with_context(|| format!("projection node {key} missing during wake check"))?;
        Ok(match &entry.manifest.wake_policy {
            ProjectionWakePolicy::EveryTick => true,
            ProjectionWakePolicy::OnEventMask(mask) => wake_masks_intersect(*mask, tick.flags),
            ProjectionWakePolicy::OnIntervalNs { interval_ns } => {
                let Some(last_due) = self.last_due_ts_ns.get(key).copied() else {
                    return Ok(true);
                };
                tick.cursor_ts_ns.saturating_sub(last_due) >= *interval_ns
            }
            ProjectionWakePolicy::Manual => false,
        })
    }

    fn dependency_payloads(&self, key: &ProjectionKey) -> Result<IndexMap<ProjectionKey, Value>> {
        let mut out = IndexMap::new();
        for dep in self.deps.get(key).into_iter().flatten() {
            let payload = self
                .last_payloads
                .get(dep)
                .with_context(|| format!("projection dependency {dep} has no payload"))?;
            out.insert(dep.clone(), payload.clone());
        }
        Ok(out)
    }

    fn node_snapshot(&self, key: &ProjectionKey) -> Result<Value> {
        self.nodes
            .get(key)
            .map(|entry| entry.node.snapshot())
            .with_context(|| format!("projection node {key} missing during snapshot"))
    }

    fn drain_node_frames(&mut self, key: &ProjectionKey) -> Result<Vec<ProjectionFrameDraft>> {
        self.nodes
            .get_mut(key)
            .with_context(|| format!("projection node {key} missing while draining frames"))?
            .node
            .drain_frames()
    }

    fn snapshot_frame(
        &mut self,
        key: &ProjectionKey,
        op: ProjectionFrameOp,
        batch_idx: u64,
        cursor_ts_ns: UnixNanos,
    ) -> Result<ProjectionFrame> {
        let payload = self
            .last_payloads
            .get(key)
            .cloned()
            .with_context(|| format!("projection node {key} has no snapshot payload"))?;
        self.stamp_frame(
            key,
            ProjectionFrameDraft { op, payload },
            batch_idx,
            cursor_ts_ns,
        )
    }

    fn stamp_frame(
        &mut self,
        key: &ProjectionKey,
        draft: ProjectionFrameDraft,
        batch_idx: u64,
        cursor_ts_ns: UnixNanos,
    ) -> Result<ProjectionFrame> {
        let entry = self
            .nodes
            .get(key)
            .with_context(|| format!("projection node {key} missing while stamping frame"))?;
        self.sequence += 1;
        Ok(ProjectionFrame {
            stamp: ProjectionFrameStamp {
                session_id: self.config.session_id.clone(),
                replay_dataset_id: self.config.replay_dataset_id.clone(),
                generation: self.generation,
                projection_key: key.clone(),
                output_schema: entry.manifest.output_schema.clone(),
                batch_idx,
                cursor_ts_ns: cursor_ts_ns.to_string(),
                source_view: entry.manifest.source_view,
                temporal_policy: entry.manifest.temporal_policy,
                execution_type: entry.manifest.execution_type,
                produced_at_ns: now_ns().to_string(),
                sequence: self.sequence,
            },
            op: draft.op,
            payload: draft.payload,
        })
    }
}

#[derive(Debug)]
struct SubscriptionEntry {
    key: ProjectionKey,
    closure: Vec<ProjectionKey>,
}

struct NodeEntry {
    manifest: ProjectionManifest,
    node: Box<dyn ProjectionNode>,
    ref_count: usize,
}

fn ensure_supported_execution(key: &ProjectionKey, manifest: &ProjectionManifest) -> Result<()> {
    match manifest.execution_type {
        ProjectionExecutionType::CoreSync
        | ProjectionExecutionType::InlineSync
        | ProjectionExecutionType::CoalescedSync => {}
        ProjectionExecutionType::AsyncLatest
        | ProjectionExecutionType::AsyncOrdered
        | ProjectionExecutionType::OfflineArtifact
        | ProjectionExecutionType::ReviewOnly => {
            bail!(
                "projection {key} uses {:?}, which is not supported by the sync projection runtime",
                manifest.execution_type
            );
        }
    }
    if matches!(manifest.lag_policy, ProjectionLagPolicy::ReviewOnly) {
        bail!("projection {key} uses review-only lag policy in the sync projection runtime");
    }
    Ok(())
}

fn wake_masks_intersect(left: ProjectionWakeEventMask, right: ProjectionWakeEventMask) -> bool {
    (left.exchange_events && right.exchange_events)
        || (left.trades && right.trades)
        || (left.bbo_changed && right.bbo_changed)
        || (left.depth_changed && right.depth_changed)
        || (left.visibility_frame && right.visibility_frame)
        || (left.fill_event && right.fill_event)
        || (left.order_event && right.order_event)
        || (left.external_snapshot && right.external_snapshot)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::projection::{resolve_dependency_specs, ProjectionFactory};
    use ledger_domain::{
        DependencyDecl, DependencyParams, ExpectedCost, MemoryClass, ProjectionCachePolicy,
        ProjectionCostHint, ProjectionDeliverySemantics, ProjectionFramePolicy, ProjectionId,
        ProjectionKind, ProjectionLagPolicy, ProjectionOutputSchema, ProjectionUpdateMode,
        ProjectionVersion, SourceView, TemporalPolicy, TrustTier,
    };
    use serde_json::{json, Value};

    fn manifest(
        id: &str,
        dependencies: Vec<DependencyDecl>,
        wake_policy: ProjectionWakePolicy,
    ) -> ProjectionManifest {
        ProjectionManifest {
            id: ProjectionId::new(id).unwrap(),
            version: ProjectionVersion::new(1).unwrap(),
            name: id.to_string(),
            description: format!("Synthetic {id} test projection."),
            kind: ProjectionKind::Base,
            params_schema: json!({ "type": "object" }),
            default_params: json!({}),
            dependencies,
            output_schema: ProjectionOutputSchema::new("counter_v1").unwrap(),
            update_mode: ProjectionUpdateMode::Online,
            source_view: Some(SourceView::ExchangeTruth),
            temporal_policy: TemporalPolicy::Causal,
            wake_policy,
            execution_type: ProjectionExecutionType::InlineSync,
            delivery_semantics: ProjectionDeliverySemantics::ReplaceLatest,
            frame_policy: ProjectionFramePolicy::EmitEveryUpdate,
            lag_policy: ProjectionLagPolicy::ProhibitedWhenDue,
            cache_policy: ProjectionCachePolicy::SessionMemory,
            trust_tier: TrustTier::Core,
            cost_hint: ProjectionCostHint {
                expected_cost: ExpectedCost::Tiny,
                max_inline_us: Some(10),
                max_output_hz: Some(1000),
                max_lag_ms: None,
                memory_class: MemoryClass::SmallState,
            },
            visualization: vec![],
            validation: vec![],
        }
    }

    fn dep(id: &str) -> DependencyDecl {
        DependencyDecl {
            id: ProjectionId::new(id).unwrap(),
            version: ProjectionVersion::new(1).unwrap(),
            params: DependencyParams::InheritAll,
            required: true,
        }
    }

    #[derive(Clone)]
    struct CounterFactory {
        manifest: ProjectionManifest,
        kind: CounterKind,
    }

    impl CounterFactory {
        fn source() -> Self {
            Self {
                manifest: manifest("source_counter", vec![], ProjectionWakePolicy::EveryTick),
                kind: CounterKind::Source,
            }
        }

        fn double() -> Self {
            Self {
                manifest: manifest(
                    "double_counter",
                    vec![dep("source_counter")],
                    ProjectionWakePolicy::Manual,
                ),
                kind: CounterKind::Double,
            }
        }

        fn sum() -> Self {
            Self {
                manifest: manifest(
                    "sum_counter",
                    vec![dep("source_counter"), dep("double_counter")],
                    ProjectionWakePolicy::Manual,
                ),
                kind: CounterKind::Sum,
            }
        }

        fn cycle_a() -> Self {
            Self {
                manifest: manifest(
                    "cycle_a",
                    vec![dep("cycle_b")],
                    ProjectionWakePolicy::Manual,
                ),
                kind: CounterKind::Source,
            }
        }

        fn cycle_b() -> Self {
            Self {
                manifest: manifest(
                    "cycle_b",
                    vec![dep("cycle_a")],
                    ProjectionWakePolicy::Manual,
                ),
                kind: CounterKind::Source,
            }
        }
    }

    impl ProjectionFactory for CounterFactory {
        fn manifest(&self) -> &ProjectionManifest {
            &self.manifest
        }

        fn resolve_dependencies(&self, params: &Value) -> Result<Vec<ProjectionSpec>> {
            resolve_dependency_specs(params, &self.manifest.dependencies)
        }

        fn build(
            &self,
            _spec: ProjectionSpec,
            key: ProjectionKey,
        ) -> Result<Box<dyn ProjectionNode>> {
            Ok(Box::new(CounterNode {
                key,
                kind: self.kind,
                value: 0,
                pending: Vec::new(),
            }))
        }
    }

    #[derive(Debug, Clone, Copy)]
    enum CounterKind {
        Source,
        Double,
        Sum,
    }

    struct CounterNode {
        key: ProjectionKey,
        kind: CounterKind,
        value: i64,
        pending: Vec<ProjectionFrameDraft>,
    }

    impl ProjectionNode for CounterNode {
        fn key(&self) -> &ProjectionKey {
            &self.key
        }

        fn advance(&mut self, ctx: &ProjectionContext<'_>) -> Result<ProjectionAdvance> {
            let next = match self.kind {
                CounterKind::Source => self.value + 1,
                CounterKind::Double => {
                    ctx.dependencies()
                        .values()
                        .next()
                        .and_then(|value| value["value"].as_i64())
                        .unwrap_or_default()
                        * 2
                }
                CounterKind::Sum => ctx
                    .dependencies()
                    .values()
                    .filter_map(|value| value["value"].as_i64())
                    .sum(),
            };
            if next == self.value {
                return Ok(ProjectionAdvance::NoChange);
            }
            self.value = next;
            self.pending
                .push(ProjectionFrameDraft::replace(self.snapshot()));
            Ok(ProjectionAdvance::StateChanged)
        }

        fn snapshot(&self) -> Value {
            json!({
                "key": self.key.display_name(),
                "value": self.value
            })
        }

        fn drain_frames(&mut self) -> Result<Vec<ProjectionFrameDraft>> {
            Ok(std::mem::take(&mut self.pending))
        }

        fn reset(&mut self) -> Result<()> {
            self.value = 0;
            self.pending.clear();
            Ok(())
        }
    }

    fn registry() -> ProjectionRegistry {
        let mut registry = ProjectionRegistry::new();
        registry.register(CounterFactory::source()).unwrap();
        registry.register(CounterFactory::double()).unwrap();
        registry.register(CounterFactory::sum()).unwrap();
        registry
    }

    fn runtime() -> ProjectionRuntime {
        ProjectionRuntime::new(registry(), ProjectionRuntimeConfig::default())
    }

    fn spec(id: &str) -> ProjectionSpec {
        ProjectionSpec::new(id, 1, json!({})).unwrap()
    }

    #[test]
    fn runtime_registers_and_lists_manifests() {
        let registry = registry();
        let manifests = registry.list_manifests();

        assert_eq!(manifests.len(), 3);
        assert_eq!(manifests[0].id.as_str(), "source_counter");
        assert_eq!(
            registry
                .manifest(
                    &ProjectionId::new("sum_counter").unwrap(),
                    ProjectionVersion::new(1).unwrap()
                )
                .unwrap()
                .id
                .as_str(),
            "sum_counter"
        );
    }

    #[test]
    fn runtime_rejects_duplicate_factory() {
        let mut registry = ProjectionRegistry::new();
        registry.register(CounterFactory::source()).unwrap();

        assert!(registry.register(CounterFactory::source()).is_err());
    }

    #[test]
    fn runtime_rejects_invalid_manifest() {
        let mut registry = ProjectionRegistry::new();
        let mut factory = CounterFactory::source();
        factory.manifest.description.clear();

        assert!(registry.register(factory).is_err());
    }

    #[test]
    fn runtime_subscribe_instantiates_node() {
        let mut runtime = runtime();
        let subscription = runtime.subscribe(spec("source_counter")).unwrap();

        assert_eq!(runtime.active_node_count(), 1);
        assert_eq!(subscription.initial_frames.len(), 1);
        assert_eq!(
            subscription.initial_frames[0].stamp.projection_key,
            subscription.key
        );
    }

    #[test]
    fn runtime_reuses_same_key_for_two_subscriptions() {
        let mut runtime = runtime();
        let first = runtime.subscribe(spec("source_counter")).unwrap();
        let second = runtime.subscribe(spec("source_counter")).unwrap();

        assert_eq!(runtime.active_node_count(), 1);
        assert_eq!(first.key, second.key);
        assert_eq!(runtime.ref_count(&first.key), Some(2));
    }

    #[test]
    fn runtime_resolves_dependency_tree() {
        let mut runtime = runtime();
        let subscription = runtime.subscribe(spec("sum_counter")).unwrap();

        assert_eq!(runtime.active_node_count(), 3);
        let order = runtime
            .active_topological_order()
            .iter()
            .map(|key| key.id.as_str().to_string())
            .collect::<Vec<_>>();
        assert_eq!(
            order,
            vec!["source_counter", "double_counter", "sum_counter"]
        );
        assert_eq!(runtime.ref_count(&subscription.key), Some(1));
    }

    #[test]
    fn runtime_detects_dependency_cycle() {
        let mut registry = ProjectionRegistry::new();
        registry.register(CounterFactory::cycle_a()).unwrap();
        registry.register(CounterFactory::cycle_b()).unwrap();
        let mut runtime = ProjectionRuntime::new(registry, ProjectionRuntimeConfig::default());

        let err = runtime.subscribe(spec("cycle_a")).unwrap_err().to_string();
        assert!(err.contains("cycle"));
    }

    #[test]
    fn runtime_advances_in_topological_order() {
        let mut runtime = runtime();
        let subscription = runtime.subscribe(spec("sum_counter")).unwrap();
        let frames = runtime.advance(TruthTick::synthetic(1, 100)).unwrap();

        let values = frames
            .iter()
            .map(|frame| {
                (
                    frame.stamp.projection_key.id.as_str().to_string(),
                    frame.payload["value"].as_i64().unwrap(),
                )
            })
            .collect::<Vec<_>>();
        assert_eq!(
            values,
            vec![
                ("source_counter".to_string(), 1),
                ("double_counter".to_string(), 2),
                ("sum_counter".to_string(), 3)
            ]
        );
        assert_eq!(
            runtime.last_payload(&subscription.key).unwrap()["value"],
            json!(3)
        );
    }

    #[test]
    fn runtime_skips_unsubscribed_nodes() {
        let mut runtime = runtime();
        let subscription = runtime.subscribe(spec("source_counter")).unwrap();
        runtime.unsubscribe(subscription.id).unwrap();

        let frames = runtime.advance(TruthTick::synthetic(1, 100)).unwrap();

        assert!(frames.is_empty());
        assert_eq!(runtime.active_node_count(), 0);
    }

    #[test]
    fn runtime_reference_counts_unsubscribe() {
        let mut runtime = runtime();
        let first = runtime.subscribe(spec("sum_counter")).unwrap();
        let second = runtime.subscribe(spec("double_counter")).unwrap();

        let source_key = spec("source_counter").key().unwrap();
        let double_key = spec("double_counter").key().unwrap();
        assert_eq!(runtime.ref_count(&source_key), Some(2));
        assert_eq!(runtime.ref_count(&double_key), Some(2));

        runtime.unsubscribe(first.id).unwrap();
        assert_eq!(runtime.ref_count(&source_key), Some(1));
        assert_eq!(runtime.ref_count(&double_key), Some(1));
        assert_eq!(runtime.ref_count(&first.key), None);

        runtime.unsubscribe(second.id).unwrap();
        assert_eq!(runtime.active_node_count(), 0);
    }

    #[test]
    fn runtime_generation_increments_on_reset() {
        let mut runtime = runtime();
        runtime.subscribe(spec("source_counter")).unwrap();
        runtime.advance(TruthTick::synthetic(1, 100)).unwrap();

        let frames = runtime.reset().unwrap();

        assert_eq!(runtime.generation(), 1);
        assert_eq!(frames.len(), 1);
        assert_eq!(frames[0].stamp.generation, 1);
        assert_eq!(frames[0].payload["value"], json!(0));
    }

    #[test]
    fn runtime_initial_snapshot_contains_generation_and_projection_key() {
        let mut runtime = runtime();
        let subscription = runtime.subscribe(spec("source_counter")).unwrap();
        let frame = &subscription.initial_frames[0];

        assert_eq!(frame.op, ProjectionFrameOp::Snapshot);
        assert_eq!(frame.stamp.generation, 0);
        assert_eq!(frame.stamp.projection_key, subscription.key);
        assert_eq!(frame.stamp.sequence, 1);
    }

    #[test]
    fn runtime_initial_snapshot_uses_configured_cursor() {
        let mut runtime = ProjectionRuntime::new(
            registry(),
            ProjectionRuntimeConfig {
                session_id: "session-a".to_string(),
                replay_dataset_id: "dataset-a".to_string(),
                initial_cursor: ProjectionRuntimeCursor::new(7, 700),
            },
        );
        let subscription = runtime.subscribe(spec("source_counter")).unwrap();
        let frame = &subscription.initial_frames[0];

        assert_eq!(frame.stamp.session_id, "session-a");
        assert_eq!(frame.stamp.replay_dataset_id, "dataset-a");
        assert_eq!(frame.stamp.batch_idx, 7);
        assert_eq!(frame.stamp.cursor_ts_ns, "700");
    }

    #[test]
    fn runtime_reset_at_uses_requested_cursor() {
        let mut runtime = runtime();
        runtime.subscribe(spec("source_counter")).unwrap();
        runtime.advance(TruthTick::synthetic(1, 100)).unwrap();

        let frames = runtime
            .reset_at(ProjectionRuntimeCursor::new(3, 300))
            .unwrap();

        assert_eq!(runtime.generation(), 1);
        assert_eq!(runtime.cursor(), ProjectionRuntimeCursor::new(3, 300));
        assert_eq!(frames[0].stamp.generation, 1);
        assert_eq!(frames[0].stamp.batch_idx, 3);
        assert_eq!(frames[0].stamp.cursor_ts_ns, "300");
    }

    #[test]
    fn runtime_frame_stamp_uses_string_nanoseconds() {
        let mut runtime = runtime();
        runtime.subscribe(spec("source_counter")).unwrap();
        let frame = runtime
            .advance(TruthTick::synthetic(7, 1_773_266_400_000_000_000))
            .unwrap()
            .remove(0);
        let encoded = serde_json::to_value(frame.stamp).unwrap();

        assert_eq!(encoded["cursor_ts_ns"], json!("1773266400000000000"));
        assert!(encoded["produced_at_ns"].is_string());
    }
}
