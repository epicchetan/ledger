#[derive(Debug, thiserror::Error)]
pub enum LedgerError {
    #[error(transparent)]
    Store(#[from] anyhow::Error),

    #[error("not an ES raw object: {0}")]
    InvalidRaw(String),

    #[error("invalid DBN record: {0}")]
    InvalidDbnRecord(String),

    #[error("invalid artifact: {0}")]
    InvalidArtifact(String),

    #[error("market day derivation failed: {0}")]
    MarketDay(String),

    #[error("databento fetch failed: {0}")]
    Fetch(String),

    #[error("invalid projection spec `{spec}`: {reason}")]
    InvalidProjectionSpec { spec: String, reason: String },

    #[error("projection plan failed: {0}")]
    ProjectionPlan(String),

    #[error("projection `{node}` requires {expected} output")]
    ProjectionDependency {
        node: String,
        expected: &'static str,
    },

    #[error(transparent)]
    Cache(#[from] cache::CacheError),

    #[error(transparent)]
    Runtime(#[from] runtime::RuntimeError),

    #[error(transparent)]
    ProjectionDelivery(#[from] crate::projection::ProjectionDeliveryError),

    #[error("invalid clock speed `{0}`")]
    InvalidClockSpeed(f64),

    #[error("ES replay feed control failed: {0}")]
    FeedControl(String),
}
