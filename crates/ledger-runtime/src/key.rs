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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn key_accepts_lowercase_dotted_paths() {
        assert_eq!(
            Key::new("origin.es_mbo.batches").unwrap().as_str(),
            "origin.es_mbo.batches"
        );
        assert_eq!(
            Key::new("projection.candles_1m.bars").unwrap().as_str(),
            "projection.candles_1m.bars"
        );
    }

    #[test]
    fn key_rejects_empty_path() {
        assert!(matches!(
            Key::new(""),
            Err(DataPlaneError::InvalidKey(value)) if value.is_empty()
        ));
    }

    #[test]
    fn key_rejects_uppercase() {
        assert!(matches!(
            Key::new("origin.ES.batches"),
            Err(DataPlaneError::InvalidKey(value)) if value == "origin.ES.batches"
        ));
    }

    #[test]
    fn key_rejects_empty_segments() {
        assert!(Key::new("origin..batches").is_err());
        assert!(Key::new(".origin.batches").is_err());
        assert!(Key::new("origin.batches.").is_err());
    }
}
