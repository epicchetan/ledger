use std::fmt;

use crate::error::DataPlaneError;

#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct Key(String);

impl Key {
    pub fn new(value: impl Into<String>) -> Result<Self, DataPlaneError> {
        let value = value.into();
        if is_valid_key(&value) {
            Ok(Self(value))
        } else {
            Err(DataPlaneError::InvalidKey(value))
        }
    }

    pub fn as_str(&self) -> &str {
        &self.0
    }
}

impl fmt::Display for Key {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.0)
    }
}

fn is_valid_key(value: &str) -> bool {
    if value.is_empty() {
        return false;
    }

    value.split('.').all(|segment| {
        !segment.is_empty()
            && segment
                .bytes()
                .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'_')
    })
}
