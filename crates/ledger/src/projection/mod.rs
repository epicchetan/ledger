pub mod bars;
mod delivery;
mod trade_prints;

use std::collections::{HashMap, HashSet};

use cache::{ArrayKey, CellDescriptor, Key, ValueKey};
use runtime::{CacheSchema, RuntimeTask};

use crate::feed::es_replay::EsReplayCells;
use crate::LedgerError;

pub use bars::*;
pub use delivery::*;

pub(crate) trait ProjectionCellRegistrar {
    fn register_value<T>(
        &self,
        descriptor: CellDescriptor,
        initial: Option<T>,
    ) -> Result<ValueKey<T>, LedgerError>
    where
        T: Clone + Send + Sync + 'static;

    fn register_array<T>(
        &self,
        descriptor: CellDescriptor,
        initial: Vec<T>,
    ) -> Result<ArrayKey<T>, LedgerError>
    where
        T: Clone + Send + Sync + 'static;
}

impl ProjectionCellRegistrar for cache::Cache {
    fn register_value<T>(
        &self,
        descriptor: CellDescriptor,
        initial: Option<T>,
    ) -> Result<ValueKey<T>, LedgerError>
    where
        T: Clone + Send + Sync + 'static,
    {
        Ok(cache::Cache::register_value(self, descriptor, initial)?)
    }

    fn register_array<T>(
        &self,
        descriptor: CellDescriptor,
        initial: Vec<T>,
    ) -> Result<ArrayKey<T>, LedgerError>
    where
        T: Clone + Send + Sync + 'static,
    {
        Ok(cache::Cache::register_array(self, descriptor, initial)?)
    }
}

impl ProjectionCellRegistrar for CacheSchema<'_> {
    fn register_value<T>(
        &self,
        descriptor: CellDescriptor,
        initial: Option<T>,
    ) -> Result<ValueKey<T>, LedgerError>
    where
        T: Clone + Send + Sync + 'static,
    {
        Ok(CacheSchema::register_value(self, descriptor, initial)?)
    }

    fn register_array<T>(
        &self,
        descriptor: CellDescriptor,
        initial: Vec<T>,
    ) -> Result<ArrayKey<T>, LedgerError>
    where
        T: Clone + Send + Sync + 'static,
    {
        Ok(CacheSchema::register_array(self, descriptor, initial)?)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum ProjectionSpec {
    Bars(BarsParams),
}

impl ProjectionSpec {
    /// Parse a projection spec string, e.g. "bars:1m".
    pub fn parse(spec: &str) -> Result<Self, LedgerError> {
        let Some(interval) = spec.strip_prefix("bars:") else {
            return Err(invalid_spec(spec, "expected `bars:<interval>`"));
        };
        if interval.is_empty() {
            return Err(invalid_spec(spec, "missing interval"));
        }

        let mut digit_end = 0;
        for byte in interval.bytes() {
            if byte.is_ascii_digit() {
                digit_end += 1;
            } else {
                break;
            }
        }
        if digit_end == 0 {
            return Err(invalid_spec(spec, "interval value must be digits"));
        }

        let digits = &interval[..digit_end];
        let unit = &interval[digit_end..];
        if unit.len() != 1 {
            return Err(invalid_spec(spec, "interval unit must be s, m, or h"));
        }

        let value = digits
            .parse::<u64>()
            .map_err(|_| invalid_spec(spec, "interval value is too large"))?;
        if value == 0 {
            return Err(invalid_spec(
                spec,
                "interval value must be greater than zero",
            ));
        }

        let unit_ns = match unit {
            "s" => bars::SECOND_NS,
            "m" => bars::MINUTE_NS,
            "h" => bars::HOUR_NS,
            _ => return Err(invalid_spec(spec, "interval unit must be s, m, or h")),
        };
        let interval_ns = value
            .checked_mul(unit_ns)
            .ok_or_else(|| invalid_spec(spec, "interval is too large"))?;

        Ok(Self::Bars(BarsParams { interval_ns }))
    }

    /// Canonical form, e.g. "bars:1m". Parsing the canonical form yields
    /// an equal spec.
    pub fn canonical(&self) -> String {
        match self {
            Self::Bars(params) => bars::canonical_spec(*params),
        }
    }
}

fn invalid_spec(spec: &str, reason: impl Into<String>) -> LedgerError {
    LedgerError::InvalidProjectionSpec {
        spec: spec.to_string(),
        reason: reason.into(),
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) enum ProjectionNodeSpec {
    CanonicalTradePrints,
    Bars(BarsParams),
}

impl ProjectionNodeSpec {
    fn from_public(spec: &ProjectionSpec) -> Self {
        match spec {
            ProjectionSpec::Bars(params) => Self::Bars(*params),
        }
    }

    pub(crate) fn canonical_key(&self) -> String {
        match self {
            Self::CanonicalTradePrints => trade_prints::COMPONENT_ID.to_string(),
            Self::Bars(params) => format!(
                "projection.{}",
                bars::canonical_spec(*params).replace(':', ".")
            ),
        }
    }

    fn dependencies(&self) -> Vec<Self> {
        match self {
            Self::CanonicalTradePrints => Vec::new(),
            Self::Bars(_) => vec![Self::CanonicalTradePrints],
        }
    }
}

#[derive(Debug, Clone)]
pub(crate) struct ProjectionPlan {
    requested: Vec<ProjectionSpec>,
    nodes: Vec<ProjectionNodeSpec>,
}

impl ProjectionPlan {
    pub(crate) fn resolve(requested: &[ProjectionSpec]) -> Result<Self, LedgerError> {
        let mut public_specs = HashSet::new();
        for spec in requested {
            let canonical = spec.canonical();
            if !public_specs.insert(canonical.clone()) {
                return Err(invalid_spec(
                    &canonical,
                    format!("duplicate canonical projection spec `{canonical}`"),
                ));
            }
        }

        let mut nodes = Vec::new();
        let mut visiting = HashSet::new();
        let mut installed = HashSet::new();
        for spec in requested {
            visit_node(
                ProjectionNodeSpec::from_public(spec),
                &mut visiting,
                &mut installed,
                &mut nodes,
            )?;
        }

        Ok(Self {
            requested: requested.to_vec(),
            nodes,
        })
    }

    pub(crate) fn requested(&self) -> &[ProjectionSpec] {
        &self.requested
    }

    pub(crate) fn nodes(&self) -> &[ProjectionNodeSpec] {
        &self.nodes
    }
}

fn visit_node(
    node: ProjectionNodeSpec,
    visiting: &mut HashSet<ProjectionNodeSpec>,
    installed: &mut HashSet<ProjectionNodeSpec>,
    nodes: &mut Vec<ProjectionNodeSpec>,
) -> Result<(), LedgerError> {
    if installed.contains(&node) {
        return Ok(());
    }
    if !visiting.insert(node.clone()) {
        return Err(LedgerError::ProjectionPlan(format!(
            "dependency cycle at `{}`",
            node.canonical_key()
        )));
    }
    for dependency in node.dependencies() {
        visit_node(dependency, visiting, installed, nodes)?;
    }
    visiting.remove(&node);
    installed.insert(node.clone());
    nodes.push(node);
    Ok(())
}

#[derive(Debug, Clone)]
pub(crate) enum ProjectionOutput {
    TradePrints(trade_prints::TradePrintCells),
    Bars(BarsCells),
}

impl ProjectionOutput {
    fn trade_prints(
        &self,
        node: &ProjectionNodeSpec,
    ) -> Result<&trade_prints::TradePrintCells, LedgerError> {
        match self {
            Self::TradePrints(cells) => Ok(cells),
            Self::Bars(_) => Err(LedgerError::ProjectionDependency {
                node: node.canonical_key(),
                expected: "canonical trade-print",
            }),
        }
    }

    fn bars(&self, node: &ProjectionNodeSpec) -> Result<&BarsCells, LedgerError> {
        match self {
            Self::Bars(cells) => Ok(cells),
            Self::TradePrints(_) => Err(LedgerError::ProjectionDependency {
                node: node.canonical_key(),
                expected: "bars",
            }),
        }
    }

    pub(crate) fn owned_keys(&self) -> Vec<Key> {
        match self {
            Self::TradePrints(cells) => {
                vec![cells.prints.key().clone(), cells.status.key().clone()]
            }
            Self::Bars(cells) => vec![
                cells.bars.key().clone(),
                cells.live.key().clone(),
                cells.status.key().clone(),
            ],
        }
    }
}

pub(crate) struct InstalledProjectionNode {
    pub(crate) node: ProjectionNodeSpec,
    pub(crate) output: ProjectionOutput,
    pub(crate) task: Box<dyn RuntimeTask>,
    pub(crate) delivery: Option<Box<dyn ProjectionDeliverySource>>,
}

#[derive(Debug, Clone)]
pub enum SessionProjectionOutput {
    Bars {
        canonical_spec: String,
        cells: BarsCells,
    },
}

impl SessionProjectionOutput {
    pub fn canonical_spec(&self) -> &str {
        match self {
            Self::Bars { canonical_spec, .. } => canonical_spec,
        }
    }

    pub fn bars_cells(&self) -> &BarsCells {
        match self {
            Self::Bars { cells, .. } => cells,
        }
    }

    pub fn into_bars(self) -> (String, BarsCells) {
        match self {
            Self::Bars {
                canonical_spec,
                cells,
            } => (canonical_spec, cells),
        }
    }
}

pub(crate) fn install_projection_node(
    registrar: &impl ProjectionCellRegistrar,
    feed: &EsReplayCells,
    node: &ProjectionNodeSpec,
    outputs: &HashMap<ProjectionNodeSpec, ProjectionOutput>,
    epoch: u64,
) -> Result<InstalledProjectionNode, LedgerError> {
    match node {
        ProjectionNodeSpec::CanonicalTradePrints => {
            trade_prints::install_trade_prints_projection(registrar, feed.clone(), epoch)
        }
        ProjectionNodeSpec::Bars(params) => {
            let dependency_node = ProjectionNodeSpec::CanonicalTradePrints;
            let dependency =
                outputs
                    .get(&dependency_node)
                    .ok_or_else(|| LedgerError::ProjectionDependency {
                        node: node.canonical_key(),
                        expected: "canonical trade-print",
                    })?;
            bars::install_bars_projection(
                registrar,
                dependency.trade_prints(node)?.clone(),
                *params,
                epoch,
            )
        }
    }
}

pub(crate) fn delivery_sources_for_outputs(
    outputs: &[SessionProjectionOutput],
) -> Vec<Box<dyn ProjectionDeliverySource>> {
    outputs
        .iter()
        .map(|output| match output {
            SessionProjectionOutput::Bars {
                canonical_spec,
                cells,
            } => Box::new(BarsDeliverySource::new(
                canonical_spec.clone(),
                cells.clone(),
            )) as Box<dyn ProjectionDeliverySource>,
        })
        .collect()
}

pub(crate) fn public_projection_outputs(
    plan: &ProjectionPlan,
    outputs: &HashMap<ProjectionNodeSpec, ProjectionOutput>,
) -> Result<Vec<SessionProjectionOutput>, LedgerError> {
    plan.requested()
        .iter()
        .map(|spec| {
            let node = ProjectionNodeSpec::from_public(spec);
            let output = outputs.get(&node).ok_or_else(|| {
                LedgerError::ProjectionPlan(format!(
                    "requested projection `{}` was not installed",
                    spec.canonical()
                ))
            })?;
            match spec {
                ProjectionSpec::Bars(_) => Ok(SessionProjectionOutput::Bars {
                    canonical_spec: spec.canonical(),
                    cells: output.bars(&node)?.clone(),
                }),
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn bars(interval_ns: u64) -> ProjectionSpec {
        ProjectionSpec::Bars(BarsParams { interval_ns })
    }

    #[test]
    fn empty_request_resolves_to_empty_plan() {
        let plan = ProjectionPlan::resolve(&[]).unwrap();
        assert!(plan.nodes().is_empty());
    }

    #[test]
    fn bars_resolve_after_one_shared_trade_print_dependency() {
        let plan = ProjectionPlan::resolve(&[bars(MINUTE_NS), bars(5 * MINUTE_NS)]).unwrap();
        assert_eq!(
            plan.nodes(),
            &[
                ProjectionNodeSpec::CanonicalTradePrints,
                ProjectionNodeSpec::Bars(BarsParams {
                    interval_ns: MINUTE_NS,
                }),
                ProjectionNodeSpec::Bars(BarsParams {
                    interval_ns: 5 * MINUTE_NS,
                }),
            ]
        );
    }

    #[test]
    fn duplicate_canonical_public_specs_are_rejected_before_installation() {
        let error = ProjectionPlan::resolve(&[bars(MINUTE_NS), bars(60 * SECOND_NS)]).unwrap_err();
        assert!(matches!(error, LedgerError::InvalidProjectionSpec { .. }));
    }
}
