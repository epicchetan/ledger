use super::ProjectionRegistry;
use anyhow::{bail, Context, Result};
use indexmap::{IndexMap, IndexSet};
use ledger_domain::{ProjectionKey, ProjectionSpec};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProjectionGraph {
    pub root: ProjectionKey,
    pub nodes: Vec<ProjectionGraphNode>,
    pub edges: Vec<ProjectionGraphEdge>,
    pub cycle: bool,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProjectionGraphNode {
    pub spec: ProjectionSpec,
    pub key: ProjectionKey,
    pub dependencies: Vec<ProjectionKey>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProjectionGraphEdge {
    pub from: ProjectionKey,
    pub to: ProjectionKey,
}

pub fn resolve_projection_graph(
    registry: &ProjectionRegistry,
    spec: ProjectionSpec,
) -> Result<ProjectionGraph> {
    let root_spec = registry.normalize_spec(&spec)?;
    let root = root_spec.key()?;
    let mut nodes = IndexMap::<ProjectionKey, ProjectionGraphNode>::new();
    let mut edges = Vec::new();
    let mut stack = Vec::new();

    visit_projection(registry, root_spec, &mut nodes, &mut edges, &mut stack)?;

    Ok(ProjectionGraph {
        root,
        nodes: nodes.into_values().collect(),
        edges,
        cycle: false,
    })
}

fn visit_projection(
    registry: &ProjectionRegistry,
    spec: ProjectionSpec,
    nodes: &mut IndexMap<ProjectionKey, ProjectionGraphNode>,
    edges: &mut Vec<ProjectionGraphEdge>,
    stack: &mut Vec<ProjectionKey>,
) -> Result<()> {
    let spec = registry.normalize_spec(&spec)?;
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

    if nodes.contains_key(&key) {
        return Ok(());
    }

    stack.push(key.clone());
    let factory = registry.factory(&spec.id, spec.version)?;
    let dependency_specs = factory
        .resolve_dependencies(&spec.params)
        .with_context(|| format!("resolving dependencies for projection {key}"))?;
    let mut dependencies = IndexSet::new();

    for dependency_spec in dependency_specs {
        let dependency_spec = registry.normalize_spec(&dependency_spec)?;
        let dependency_key = dependency_spec.key()?;
        visit_projection(registry, dependency_spec, nodes, edges, stack)?;
        dependencies.insert(dependency_key.clone());
        edges.push(ProjectionGraphEdge {
            from: dependency_key,
            to: key.clone(),
        });
    }
    stack.pop();

    nodes.insert(
        key.clone(),
        ProjectionGraphNode {
            spec,
            key,
            dependencies: dependencies.into_iter().collect(),
        },
    );

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::projection::{base_projection_registry, BARS_ID, CANONICAL_TRADES_ID};
    use ledger_domain::ProjectionSpec;
    use serde_json::json;

    #[test]
    fn projection_graph_resolves_bars_dependency() {
        let registry = base_projection_registry().unwrap();
        let graph = resolve_projection_graph(
            &registry,
            ProjectionSpec::new(BARS_ID, 1, json!({ "seconds": 60 })).unwrap(),
        )
        .unwrap();

        assert!(!graph.cycle);
        assert_eq!(graph.nodes.len(), 2);
        assert_eq!(graph.edges.len(), 1);
        assert_eq!(graph.root.id.as_str(), BARS_ID);
        assert_eq!(graph.edges[0].from.id.as_str(), CANONICAL_TRADES_ID);
        assert_eq!(graph.edges[0].to.id.as_str(), BARS_ID);
    }
}
