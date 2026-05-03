use super::ProjectionFactory;
use anyhow::{bail, ensure, Context, Result};
use indexmap::IndexMap;
use ledger_domain::{
    DependencyDecl, DependencyParams, ProjectionId, ProjectionManifest, ProjectionSpec,
    ProjectionVersion,
};
use serde_json::{Map, Value};
use std::sync::Arc;

type FactoryKey = (ProjectionId, ProjectionVersion);

#[derive(Clone, Default)]
pub struct ProjectionRegistry {
    factories: IndexMap<FactoryKey, Arc<dyn ProjectionFactory>>,
}

impl ProjectionRegistry {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn register<F>(&mut self, factory: F) -> Result<()>
    where
        F: ProjectionFactory + 'static,
    {
        self.register_arc(Arc::new(factory))
    }

    pub fn register_arc(&mut self, factory: Arc<dyn ProjectionFactory>) -> Result<()> {
        let manifest = factory.manifest();
        manifest.validate()?;
        let key = (manifest.id.clone(), manifest.version);
        ensure!(
            !self.factories.contains_key(&key),
            "projection factory {}:{} is already registered",
            manifest.id,
            manifest.version
        );
        self.factories.insert(key, factory);
        Ok(())
    }

    pub fn list_manifests(&self) -> Vec<ProjectionManifest> {
        self.factories
            .values()
            .map(|factory| factory.manifest().clone())
            .collect()
    }

    pub fn manifest(
        &self,
        id: &ProjectionId,
        version: ProjectionVersion,
    ) -> Result<&ProjectionManifest> {
        self.factories
            .get(&(id.clone(), version))
            .map(|factory| factory.manifest())
            .with_context(|| format!("projection factory {id}:{version} is not registered"))
    }

    pub fn factory(
        &self,
        id: &ProjectionId,
        version: ProjectionVersion,
    ) -> Result<Arc<dyn ProjectionFactory>> {
        self.factories
            .get(&(id.clone(), version))
            .cloned()
            .with_context(|| format!("projection factory {id}:{version} is not registered"))
    }

    pub fn normalize_spec(&self, spec: &ProjectionSpec) -> Result<ProjectionSpec> {
        let manifest = self.manifest(&spec.id, spec.version)?;
        let params = merge_params(&manifest.default_params, &spec.params);
        ProjectionSpec::new(spec.id.as_str().to_string(), spec.version.get(), params)
    }
}

pub fn resolve_dependency_specs(
    parent_params: &Value,
    dependencies: &[DependencyDecl],
) -> Result<Vec<ProjectionSpec>> {
    dependencies
        .iter()
        .map(|dependency| {
            let params = resolve_dependency_params(parent_params, dependency)?;
            ProjectionSpec::new(
                dependency.id.as_str().to_string(),
                dependency.version.get(),
                params,
            )
        })
        .collect()
}

fn resolve_dependency_params(parent_params: &Value, dependency: &DependencyDecl) -> Result<Value> {
    match &dependency.params {
        DependencyParams::Static(params) => Ok(params.clone()),
        DependencyParams::InheritAll => Ok(parent_params.clone()),
        DependencyParams::Inherit(paths) => {
            let mut out = Value::Object(Map::new());
            for path in paths {
                match get_path(parent_params, path) {
                    Some(value) => set_path(&mut out, path, value.clone())?,
                    None if dependency.required => {
                        bail!(
                            "dependency {}:{} requires missing inherited param `{path}`",
                            dependency.id,
                            dependency.version
                        );
                    }
                    None => {}
                }
            }
            Ok(out)
        }
    }
}

fn merge_params(defaults: &Value, overrides: &Value) -> Value {
    match (defaults, overrides) {
        (_, Value::Null) => defaults.clone(),
        (Value::Object(defaults), Value::Object(overrides)) => {
            let mut out = defaults.clone();
            for (key, value) in overrides {
                let merged = match out.get(key) {
                    Some(default_value) => merge_params(default_value, value),
                    None => value.clone(),
                };
                out.insert(key.clone(), merged);
            }
            Value::Object(out)
        }
        (_, override_value) => override_value.clone(),
    }
}

fn get_path<'a>(value: &'a Value, path: &str) -> Option<&'a Value> {
    let mut current = value;
    for segment in path.split('.') {
        current = current.get(segment)?;
    }
    Some(current)
}

fn set_path(target: &mut Value, path: &str, value: Value) -> Result<()> {
    let segments = path.split('.').collect::<Vec<_>>();
    ensure!(
        !segments.is_empty(),
        "cannot set empty dependency param path"
    );

    let mut current = target;
    for segment in &segments[..segments.len() - 1] {
        let object = current
            .as_object_mut()
            .context("dependency params target is not an object")?;
        current = object
            .entry((*segment).to_string())
            .or_insert_with(|| Value::Object(Map::new()));
    }

    let leaf = segments
        .last()
        .context("cannot set empty dependency param path")?;
    current
        .as_object_mut()
        .context("dependency params target is not an object")?
        .insert((*leaf).to_string(), value);
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use ledger_domain::{DependencyParams, ProjectionVersion};
    use serde_json::json;

    #[test]
    fn dependency_inherit_copies_nested_paths() {
        let params = json!({
            "source_view": "exchange_truth",
            "window": {
                "seconds": 60,
                "kind": "time"
            }
        });
        let dependency = DependencyDecl {
            id: ProjectionId::new("bars").unwrap(),
            version: ProjectionVersion::new(1).unwrap(),
            params: DependencyParams::Inherit(vec![
                "source_view".to_string(),
                "window.seconds".to_string(),
            ]),
            required: true,
        };

        let resolved = resolve_dependency_specs(&params, &[dependency]).unwrap();

        assert_eq!(
            resolved[0].params,
            json!({
                "source_view": "exchange_truth",
                "window": {
                    "seconds": 60
                }
            })
        );
    }
}
