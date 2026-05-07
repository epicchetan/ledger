use thiserror::Error;

use crate::{CellKind, CellOwner, Key};

#[derive(Debug, Error, Clone, PartialEq, Eq)]
pub enum DataPlaneError {
    #[error("invalid key `{0}`")]
    InvalidKey(String),

    #[error("cell `{0}` already exists")]
    DuplicateCell(Key),

    #[error("cell `{0}` does not exist")]
    MissingCell(Key),

    #[error("cell `{key}` expected kind {expected:?}, found {found:?}")]
    WrongCellKind {
        key: Key,
        expected: CellKind,
        found: CellKind,
    },

    #[error("writer {writer:?} cannot mutate cell `{key}` owned by {owner:?}")]
    OwnerMismatch {
        key: Key,
        writer: CellOwner,
        owner: CellOwner,
    },

    #[error("cell `{0}` payload type mismatch")]
    TypeMismatch(Key),

    #[error("array range is out of bounds for cell `{key}`")]
    ArrayRangeOutOfBounds { key: Key },

    #[error("data-plane registry lock poisoned")]
    RegistryLockPoisoned,

    #[error("lock poisoned while accessing cell `{key}`")]
    CellLockPoisoned { key: Key },
}
