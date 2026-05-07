use crate::StoreError;
use serde::{Deserialize, Serialize};
use std::fmt;

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
#[serde(try_from = "String", into = "String")]
pub struct StoreObjectId(String);

impl StoreObjectId {
    pub fn new(value: impl Into<String>) -> Result<Self, StoreError> {
        let value = value.into();
        let Some(hash) = value.strip_prefix("sha256-") else {
            return Err(StoreError::InvalidObjectId {
                value,
                reason: "id must start with `sha256-`".to_string(),
            });
        };
        if hash.len() != 64 {
            return Err(StoreError::InvalidObjectId {
                value,
                reason: "sha256 suffix must contain 64 hex characters".to_string(),
            });
        }
        if !hash
            .bytes()
            .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
        {
            return Err(StoreError::InvalidObjectId {
                value,
                reason: "sha256 suffix must be lowercase hex".to_string(),
            });
        }
        Ok(Self(format!("sha256-{hash}")))
    }

    pub fn from_sha256(hash: impl AsRef<str>) -> Result<Self, StoreError> {
        Self::new(format!("sha256-{}", hash.as_ref()))
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }

    pub fn sha256(&self) -> &str {
        &self.0["sha256-".len()..]
    }

    pub fn shard(&self) -> &str {
        &self.sha256()[..2]
    }
}

impl fmt::Display for StoreObjectId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

impl TryFrom<String> for StoreObjectId {
    type Error = StoreError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        Self::new(value)
    }
}

impl From<StoreObjectId> for String {
    fn from(value: StoreObjectId) -> Self {
        value.0
    }
}
