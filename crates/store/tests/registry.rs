use store::{
    JsonObjectRegistry, LocalStore, ObjectFilter, StoreObjectDescriptor, StoreObjectId,
    StoreObjectRole,
};
use tempfile::tempdir;

#[test]
fn registry_put_get_list_remove_round_trips_descriptors() {
    let tmp = tempdir().unwrap();
    let local = LocalStore::new(tmp.path());
    let registry = JsonObjectRegistry::new(&local);
    let id = StoreObjectId::from_sha256(
        "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
    )
    .unwrap();
    let descriptor = StoreObjectDescriptor {
        id: id.clone(),
        role: StoreObjectRole::Raw,
        kind: "databento.dbn.zst".to_string(),
        file_name: "raw.dbn.zst".to_string(),
        content_sha256: id.sha256().to_string(),
        size_bytes: 10,
        format: Some("dbn.zst".to_string()),
        media_type: None,
        remote: None,
        local: None,
        lineage: Vec::new(),
        metadata_json: serde_json::json!({"provider":"databento"}),
        created_at_ns: 1,
        updated_at_ns: 1,
        last_accessed_at_ns: None,
    };

    registry.put(&descriptor).unwrap();

    assert_eq!(registry.get(&id).unwrap(), Some(descriptor.clone()));
    assert_eq!(
        registry
            .list(ObjectFilter {
                role: Some(StoreObjectRole::Raw),
                ..Default::default()
            })
            .unwrap(),
        vec![descriptor.clone()]
    );
    assert_eq!(
        registry
            .list(ObjectFilter {
                role: Some(StoreObjectRole::Artifact),
                ..Default::default()
            })
            .unwrap(),
        Vec::<StoreObjectDescriptor>::new()
    );
    assert_eq!(registry.remove(&id).unwrap(), Some(descriptor));
    assert_eq!(registry.get(&id).unwrap(), None);
}
