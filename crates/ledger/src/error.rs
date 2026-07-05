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
}
