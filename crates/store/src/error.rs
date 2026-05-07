use thiserror::Error;

#[derive(Debug, Error)]
pub enum StoreError {
    #[error("invalid object id `{value}`: {reason}")]
    InvalidObjectId { value: String, reason: String },
}
