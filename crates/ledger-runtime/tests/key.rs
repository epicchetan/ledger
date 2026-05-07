use ledger_runtime::{DataPlaneError, Key};

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
