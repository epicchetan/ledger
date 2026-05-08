use cache::{CacheError, CellOwner, Key};

#[test]
fn key_accepts_lowercase_dotted_paths() {
    assert_eq!(
        Key::new("feed.databento.es_mbo.batches").unwrap().as_str(),
        "feed.databento.es_mbo.batches"
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
        Err(CacheError::InvalidKey(value)) if value.is_empty()
    ));
}

#[test]
fn key_rejects_uppercase() {
    assert!(matches!(
        Key::new("feed.ES.batches"),
        Err(CacheError::InvalidKey(value)) if value == "feed.ES.batches"
    ));
}

#[test]
fn key_rejects_empty_segments() {
    assert!(Key::new("feed..batches").is_err());
    assert!(Key::new(".feed.batches").is_err());
    assert!(Key::new("feed.batches.").is_err());
}

#[test]
fn cell_owner_uses_same_validation_as_key() {
    assert_eq!(
        CellOwner::new("projection.candles_1m").unwrap().as_str(),
        "projection.candles_1m"
    );
    assert!(matches!(
        CellOwner::new("Projection.Candles"),
        Err(CacheError::InvalidKey(value)) if value == "Projection.Candles"
    ));
}
