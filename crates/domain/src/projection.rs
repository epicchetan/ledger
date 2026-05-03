use anyhow::{bail, ensure, Result};
use serde::de::Error as DeError;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use serde_json::{Map, Value};
use sha2::{Digest, Sha256};
use std::fmt;

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ProjectionId(String);

impl ProjectionId {
    pub fn new(value: impl Into<String>) -> Result<Self> {
        let value = value.into();
        ensure!(
            is_valid_projection_id(&value),
            "invalid projection id `{value}`; expected lowercase snake_case tokens separated by dots"
        );
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn into_inner(self) -> String {
        self.0
    }
}

impl fmt::Display for ProjectionId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl Serialize for ProjectionId {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.0)
    }
}

impl<'de> Deserialize<'de> for ProjectionId {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::new(value).map_err(D::Error::custom)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ProjectionVersion(u16);

impl ProjectionVersion {
    pub fn new(value: u16) -> Result<Self> {
        ensure!(value > 0, "projection version must be greater than zero");
        Ok(Self(value))
    }

    pub fn get(self) -> u16 {
        self.0
    }
}

impl fmt::Display for ProjectionVersion {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "v{}", self.0)
    }
}

impl Serialize for ProjectionVersion {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_u16(self.0)
    }
}

impl<'de> Deserialize<'de> for ProjectionVersion {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = u16::deserialize(deserializer)?;
        Self::new(value).map_err(D::Error::custom)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ProjectionParamsHash(String);

impl ProjectionParamsHash {
    pub fn new(value: impl Into<String>) -> Result<Self> {
        let value = value.into();
        ensure!(
            is_valid_sha256_hash(&value),
            "invalid projection params hash `{value}`; expected sha256:<64 hex chars>"
        );
        Ok(Self(value))
    }

    pub fn from_params(params: &Value) -> Result<Self> {
        let canonical = canonicalize_json(params);
        let bytes = serde_json::to_vec(&canonical)?;
        let digest = Sha256::digest(bytes);
        Self::new(format!("sha256:{}", hex::encode(digest)))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for ProjectionParamsHash {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl Serialize for ProjectionParamsHash {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.0)
    }
}

impl<'de> Deserialize<'de> for ProjectionParamsHash {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::new(value).map_err(D::Error::custom)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct ProjectionSchemaName(String);

pub type ProjectionPayloadSchemaName = ProjectionSchemaName;

impl ProjectionSchemaName {
    pub fn new(value: impl Into<String>) -> Result<Self> {
        let value = value.into();
        ensure!(
            is_valid_schema_name(&value),
            "invalid projection schema name `{value}`; expected lowercase snake_case"
        );
        Ok(Self(value))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for ProjectionSchemaName {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl Serialize for ProjectionSchemaName {
    fn serialize<S>(&self, serializer: S) -> std::result::Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&self.0)
    }
}

impl<'de> Deserialize<'de> for ProjectionSchemaName {
    fn deserialize<D>(deserializer: D) -> std::result::Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let value = String::deserialize(deserializer)?;
        Self::new(value).map_err(D::Error::custom)
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(transparent)]
pub struct ProjectionOutputSchema {
    pub name: ProjectionSchemaName,
}

impl ProjectionOutputSchema {
    pub fn new(name: impl Into<String>) -> Result<Self> {
        Ok(Self {
            name: ProjectionSchemaName::new(name)?,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProjectionSpec {
    pub id: ProjectionId,
    pub version: ProjectionVersion,
    #[serde(default)]
    pub params: Value,
}

impl ProjectionSpec {
    pub fn new(id: impl Into<String>, version: u16, params: Value) -> Result<Self> {
        Ok(Self {
            id: ProjectionId::new(id)?,
            version: ProjectionVersion::new(version)?,
            params,
        })
    }

    pub fn key(&self) -> Result<ProjectionKey> {
        Ok(ProjectionKey {
            id: self.id.clone(),
            version: self.version,
            params_hash: ProjectionParamsHash::from_params(&self.params)?,
        })
    }
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct ProjectionKey {
    pub id: ProjectionId,
    pub version: ProjectionVersion,
    pub params_hash: ProjectionParamsHash,
}

impl ProjectionKey {
    pub fn display_name(&self) -> String {
        format!("{}:{}:{}", self.id, self.version, self.params_hash)
    }
}

impl fmt::Display for ProjectionKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.display_name())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProjectionManifest {
    pub id: ProjectionId,
    pub version: ProjectionVersion,
    pub name: String,
    pub description: String,
    pub kind: ProjectionKind,
    #[serde(default)]
    pub params_schema: Value,
    #[serde(default)]
    pub default_params: Value,
    #[serde(default)]
    pub dependencies: Vec<DependencyDecl>,
    pub output_schema: ProjectionOutputSchema,
    pub update_mode: ProjectionUpdateMode,
    pub source_view: Option<SourceView>,
    pub temporal_policy: TemporalPolicy,
    pub wake_policy: ProjectionWakePolicy,
    pub execution_type: ProjectionExecutionType,
    pub delivery_semantics: ProjectionDeliverySemantics,
    pub frame_policy: ProjectionFramePolicy,
    pub lag_policy: ProjectionLagPolicy,
    pub cache_policy: ProjectionCachePolicy,
    pub trust_tier: TrustTier,
    pub cost_hint: ProjectionCostHint,
    #[serde(default)]
    pub visualization: Vec<VisualizationHint>,
    #[serde(default)]
    pub validation: Vec<ValidationDecl>,
}

impl ProjectionManifest {
    pub fn validate(&self) -> Result<()> {
        ensure!(
            !self.name.trim().is_empty(),
            "projection manifest {}:{} has an empty name",
            self.id,
            self.version
        );
        ensure!(
            !self.description.trim().is_empty(),
            "projection manifest {}:{} has an empty description",
            self.id,
            self.version
        );
        let _ = ProjectionParamsHash::from_params(&self.default_params)?;
        for dependency in &self.dependencies {
            dependency.validate()?;
        }
        if matches!(self.execution_type, ProjectionExecutionType::CoreSync) {
            ensure!(
                matches!(
                    self.lag_policy,
                    ProjectionLagPolicy::Prohibited | ProjectionLagPolicy::ProhibitedWhenDue
                ),
                "core sync projection {}:{} cannot allow lag",
                self.id,
                self.version
            );
        }
        if matches!(self.execution_type, ProjectionExecutionType::ReviewOnly) {
            ensure!(
                matches!(self.trust_tier, TrustTier::ResearchOnly),
                "review-only projection {}:{} must use research_only trust tier",
                self.id,
                self.version
            );
        }
        Ok(())
    }

    pub fn default_spec(&self) -> Result<ProjectionSpec> {
        self.validate()?;
        ProjectionSpec::new(
            self.id.as_str().to_string(),
            self.version.get(),
            self.default_params.clone(),
        )
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProjectionKind {
    Base,
    DerivedStudy,
    CompositeStudy,
    ModelStudy,
    Visual,
    Journal,
    Diagnostic,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProjectionUpdateMode {
    Online,
    Offline,
    OnlineAndOffline,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum SourceView {
    ExchangeTruth,
    TraderVisibility,
    ExecutionSimulation,
    ExternalAsOf,
    JournalContext,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TemporalPolicy {
    Causal,
    AsOf,
    Hindsight,
    ReviewOnly,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProjectionWakePolicy {
    EveryTick,
    OnEventMask(ProjectionWakeEventMask),
    OnIntervalNs { interval_ns: u64 },
    Manual,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, Default)]
pub struct ProjectionWakeEventMask {
    pub exchange_events: bool,
    pub trades: bool,
    pub bbo_changed: bool,
    pub depth_changed: bool,
    pub visibility_frame: bool,
    pub fill_event: bool,
    pub order_event: bool,
    pub external_snapshot: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProjectionExecutionType {
    CoreSync,
    InlineSync,
    CoalescedSync,
    AsyncLatest,
    AsyncOrdered,
    OfflineArtifact,
    ReviewOnly,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProjectionDeliverySemantics {
    ReplaceLatest,
    PatchByKey,
    Append,
    Snapshot,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProjectionFramePolicy {
    EmitEveryUpdate,
    EmitOnChange,
    CoalesceNs { interval_ns: u64 },
    EmitOnWindowClose,
    Manual,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProjectionLagPolicy {
    Prohibited,
    ProhibitedWhenDue,
    StateCurrentFramesCoalesced,
    LatestWins { max_lag_ms: Option<u64> },
    CatchUpOrdered { max_lag_ms: Option<u64> },
    ArtifactAsOf,
    ReviewOnly,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProjectionCachePolicy {
    None,
    SessionMemory,
    LocalReadThrough,
    DatasetArtifact,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum TrustTier {
    Core,
    Experimental,
    ResearchOnly,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProjectionCostHint {
    pub expected_cost: ExpectedCost,
    pub max_inline_us: Option<u64>,
    pub max_output_hz: Option<u32>,
    pub max_lag_ms: Option<u64>,
    pub memory_class: MemoryClass,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ExpectedCost {
    Tiny,
    Cheap,
    Medium,
    Heavy,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum MemoryClass {
    SmallState,
    RollingWindow,
    LargeSnapshot,
    ArtifactBacked,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct DependencyDecl {
    pub id: ProjectionId,
    pub version: ProjectionVersion,
    pub params: DependencyParams,
    pub required: bool,
}

impl DependencyDecl {
    pub fn validate(&self) -> Result<()> {
        match &self.params {
            DependencyParams::Static(params) => {
                let _ = ProjectionParamsHash::from_params(params)?;
            }
            DependencyParams::Inherit(paths) => {
                ensure!(
                    !paths.is_empty(),
                    "dependency {}:{} inherit params cannot be empty",
                    self.id,
                    self.version
                );
                for path in paths {
                    ensure!(
                        is_valid_param_path(path),
                        "dependency {}:{} has invalid inherited param path `{path}`",
                        self.id,
                        self.version
                    );
                }
            }
            DependencyParams::InheritAll => {}
        }
        Ok(())
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case", tag = "mode", content = "value")]
pub enum DependencyParams {
    Static(Value),
    Inherit(Vec<String>),
    InheritAll,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ValidationDecl {
    pub kind: ValidationKind,
    pub name: Option<String>,
    pub required: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ValidationKind {
    SyntheticFixture,
    DeterministicDigest,
    ProfileBudget,
    GoldenFrames,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct VisualizationHint {
    pub surface: VisualizationSurface,
    pub role: String,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum VisualizationSurface {
    Chart,
    Dom,
    Tape,
    Heatmap,
    Panel,
    Journal,
    Diagnostic,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ProjectionFrame {
    pub stamp: ProjectionFrameStamp,
    pub op: ProjectionFrameOp,
    pub payload: Value,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProjectionFrameStamp {
    pub session_id: String,
    pub replay_dataset_id: String,
    pub generation: u64,
    pub projection_key: ProjectionKey,
    pub output_schema: ProjectionOutputSchema,
    pub batch_idx: u64,
    pub cursor_ts_ns: String,
    pub source_view: Option<SourceView>,
    pub temporal_policy: TemporalPolicy,
    pub execution_type: ProjectionExecutionType,
    pub produced_at_ns: String,
    pub sequence: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ProjectionFrameOp {
    Replace,
    Patch,
    Append,
    Delete,
    Snapshot,
    Heartbeat,
}

fn canonicalize_json(value: &Value) -> Value {
    match value {
        Value::Array(values) => Value::Array(values.iter().map(canonicalize_json).collect()),
        Value::Object(map) => {
            let mut keys = map.keys().collect::<Vec<_>>();
            keys.sort();
            let mut out = Map::new();
            for key in keys {
                out.insert(key.clone(), canonicalize_json(&map[key]));
            }
            Value::Object(out)
        }
        _ => value.clone(),
    }
}

fn is_valid_projection_id(value: &str) -> bool {
    !value.is_empty() && value.split('.').all(is_valid_name_token)
}

fn is_valid_schema_name(value: &str) -> bool {
    is_valid_name_token(value)
}

fn is_valid_name_token(value: &str) -> bool {
    let mut chars = value.chars();
    match chars.next() {
        Some(first) if first.is_ascii_lowercase() => {}
        _ => return false,
    }
    chars.all(|ch| ch.is_ascii_lowercase() || ch.is_ascii_digit() || ch == '_')
}

fn is_valid_sha256_hash(value: &str) -> bool {
    let Some(hex) = value.strip_prefix("sha256:") else {
        return false;
    };
    hex.len() == 64 && hex.chars().all(|ch| ch.is_ascii_hexdigit())
}

fn is_valid_param_path(value: &str) -> bool {
    !value.is_empty() && value.split('.').all(is_valid_name_token)
}

pub fn ensure_projection_id(value: &str) -> Result<()> {
    if is_valid_projection_id(value) {
        Ok(())
    } else {
        bail!("invalid projection id `{value}`")
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn test_manifest() -> ProjectionManifest {
        ProjectionManifest {
            id: ProjectionId::new("bars").unwrap(),
            version: ProjectionVersion::new(1).unwrap(),
            name: "Bars".to_string(),
            description: "Builds canonical bars from trade prints.".to_string(),
            kind: ProjectionKind::Base,
            params_schema: json!({
                "type": "object",
                "required": ["source_view"],
                "properties": {
                    "source_view": { "enum": ["exchange_truth", "trader_visibility"] }
                }
            }),
            default_params: json!({
                "source_view": "trader_visibility",
                "seconds": 60
            }),
            dependencies: vec![DependencyDecl {
                id: ProjectionId::new("canonical_trades").unwrap(),
                version: ProjectionVersion::new(1).unwrap(),
                params: DependencyParams::Inherit(vec!["source_view".to_string()]),
                required: true,
            }],
            output_schema: ProjectionOutputSchema::new("candles_v1").unwrap(),
            update_mode: ProjectionUpdateMode::Online,
            source_view: Some(SourceView::TraderVisibility),
            temporal_policy: TemporalPolicy::Causal,
            wake_policy: ProjectionWakePolicy::OnEventMask(ProjectionWakeEventMask {
                trades: true,
                ..Default::default()
            }),
            execution_type: ProjectionExecutionType::InlineSync,
            delivery_semantics: ProjectionDeliverySemantics::PatchByKey,
            frame_policy: ProjectionFramePolicy::EmitOnWindowClose,
            lag_policy: ProjectionLagPolicy::ProhibitedWhenDue,
            cache_policy: ProjectionCachePolicy::SessionMemory,
            trust_tier: TrustTier::Core,
            cost_hint: ProjectionCostHint {
                expected_cost: ExpectedCost::Cheap,
                max_inline_us: Some(50),
                max_output_hz: Some(10),
                max_lag_ms: None,
                memory_class: MemoryClass::SmallState,
            },
            visualization: vec![VisualizationHint {
                surface: VisualizationSurface::Chart,
                role: "primary_series".to_string(),
            }],
            validation: vec![ValidationDecl {
                kind: ValidationKind::DeterministicDigest,
                name: None,
                required: true,
            }],
        }
    }

    #[test]
    fn projection_key_same_for_reordered_json_params() {
        let left = ProjectionSpec::new(
            "bars",
            1,
            json!({
                "source_view": "trader_visibility",
                "window": {
                    "seconds": 60,
                    "kind": "time"
                }
            }),
        )
        .unwrap();
        let right = ProjectionSpec::new(
            "bars",
            1,
            json!({
                "window": {
                    "kind": "time",
                    "seconds": 60
                },
                "source_view": "trader_visibility"
            }),
        )
        .unwrap();

        assert_eq!(left.key().unwrap(), right.key().unwrap());
    }

    #[test]
    fn projection_key_differs_when_params_differ() {
        let left = ProjectionSpec::new("bars", 1, json!({ "seconds": 60 })).unwrap();
        let right = ProjectionSpec::new("bars", 1, json!({ "seconds": 30 })).unwrap();

        assert_ne!(left.key().unwrap(), right.key().unwrap());
    }

    #[test]
    fn projection_id_validation_accepts_expected_ids() {
        for id in [
            "bars",
            "bbo",
            "batch_features",
            "level_sets.gamma",
            "model.level_acceptance",
        ] {
            ProjectionId::new(id).unwrap();
            ensure_projection_id(id).unwrap();
        }
    }

    #[test]
    fn projection_id_validation_rejects_bad_ids() {
        for id in [
            "",
            "Bars",
            "bars-v1",
            "bars:v1",
            " bars",
            "bars.",
            ".bars",
            "bars..fast",
            "bars/fast",
            "1bars",
        ] {
            assert!(ProjectionId::new(id).is_err(), "{id} should be invalid");
        }
    }

    #[test]
    fn projection_version_rejects_zero() {
        assert!(ProjectionVersion::new(0).is_err());
        assert!(serde_json::from_value::<ProjectionVersion>(json!(0)).is_err());
    }

    #[test]
    fn manifest_roundtrips_json() {
        let manifest = test_manifest();
        manifest.validate().unwrap();

        let encoded = serde_json::to_string_pretty(&manifest).unwrap();
        let decoded: ProjectionManifest = serde_json::from_str(&encoded).unwrap();

        assert_eq!(decoded, manifest);
        assert_eq!(decoded.default_spec().unwrap().id.as_str(), "bars");
    }

    #[test]
    fn frame_stamp_roundtrips_json() {
        let spec =
            ProjectionSpec::new("bbo", 1, json!({ "source_view": "exchange_truth" })).unwrap();
        let stamp = ProjectionFrameStamp {
            session_id: "session-1".to_string(),
            replay_dataset_id: "dataset-1".to_string(),
            generation: 2,
            projection_key: spec.key().unwrap(),
            output_schema: ProjectionOutputSchema::new("bbo_v1").unwrap(),
            batch_idx: 42,
            cursor_ts_ns: "1773266400000000000".to_string(),
            source_view: Some(SourceView::ExchangeTruth),
            temporal_policy: TemporalPolicy::Causal,
            execution_type: ProjectionExecutionType::CoreSync,
            produced_at_ns: "1773266400000000100".to_string(),
            sequence: 7,
        };

        let encoded = serde_json::to_value(&stamp).unwrap();
        assert_eq!(encoded["cursor_ts_ns"], json!("1773266400000000000"));
        assert_eq!(encoded["produced_at_ns"], json!("1773266400000000100"));

        let decoded: ProjectionFrameStamp = serde_json::from_value(encoded).unwrap();
        assert_eq!(decoded, stamp);
    }

    #[test]
    fn lag_policy_roundtrips_json() {
        let policy = ProjectionLagPolicy::LatestWins {
            max_lag_ms: Some(250),
        };

        let encoded = serde_json::to_value(&policy).unwrap();
        let decoded: ProjectionLagPolicy = serde_json::from_value(encoded).unwrap();

        assert_eq!(decoded, policy);
    }

    #[test]
    fn wake_policy_roundtrips_json() {
        let policy = ProjectionWakePolicy::OnEventMask(ProjectionWakeEventMask {
            bbo_changed: true,
            visibility_frame: true,
            ..Default::default()
        });

        let encoded = serde_json::to_value(&policy).unwrap();
        let decoded: ProjectionWakePolicy = serde_json::from_value(encoded).unwrap();

        assert_eq!(decoded, policy);
    }

    #[test]
    fn manifest_rejects_core_sync_with_lag() {
        let mut manifest = test_manifest();
        manifest.execution_type = ProjectionExecutionType::CoreSync;
        manifest.lag_policy = ProjectionLagPolicy::LatestWins {
            max_lag_ms: Some(10),
        };

        let err = manifest.validate().unwrap_err();

        assert!(format!("{err:#}").contains("cannot allow lag"));
    }

    #[test]
    fn params_hash_deserialization_rejects_malformed_hashes() {
        assert!(serde_json::from_value::<ProjectionParamsHash>(json!("sha256:abc")).is_err());
        assert!(serde_json::from_value::<ProjectionParamsHash>(json!("md5:abc")).is_err());
    }
}
