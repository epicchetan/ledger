use store::{StoreObjectId, StoreObjectRole};

#[test]
fn object_id_validates_sha256_ids() {
    let id = StoreObjectId::from_sha256(
        "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
    )
    .unwrap();

    assert_eq!(
        id.as_str(),
        "sha256-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    );
    assert_eq!(id.shard(), "aa");
    assert!(StoreObjectId::new("sha1-aaaaaaaa").is_err());
    assert!(StoreObjectId::new(
        "sha256-AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"
    )
    .is_err());
}

#[test]
fn object_role_parses_cli_values() {
    assert_eq!(StoreObjectRole::parse("raw").unwrap(), StoreObjectRole::Raw);
    assert_eq!(
        StoreObjectRole::parse("artifact").unwrap(),
        StoreObjectRole::Artifact
    );
    assert!(StoreObjectRole::parse("source").is_err());
}
