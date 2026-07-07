use std::time::Duration;

use cache::{Cache, CacheError, CellDescriptor, CellKind, CellOwner, Key};
use tokio::time::timeout;

#[derive(Clone, Debug, PartialEq, Eq)]
struct Status {
    playing: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct Batch {
    seq: u64,
}

const PARK: Duration = Duration::from_millis(50);
const WAKE: Duration = Duration::from_secs(1);

fn key(value: &str) -> Key {
    Key::new(value).unwrap()
}

fn feed_owner() -> CellOwner {
    CellOwner::new("feed.databento.es_mbo").unwrap()
}

fn descriptor(key: &str, kind: CellKind) -> CellDescriptor {
    CellDescriptor {
        key: Key::new(key).unwrap(),
        owner: feed_owner(),
        kind,
        public_read: true,
    }
}

fn status_cache() -> (Cache, CellOwner, cache::ValueKey<Status>) {
    let cache = Cache::new();
    let key = cache
        .register_value::<Status>(
            descriptor("feed.databento.es_mbo.status", CellKind::Value),
            None,
        )
        .unwrap();
    (cache, feed_owner(), key)
}

fn batches_cache() -> (Cache, CellOwner, cache::ArrayKey<Batch>) {
    let cache = Cache::new();
    let key = cache
        .register_array::<Batch>(
            descriptor("feed.databento.es_mbo.batches", CellKind::Array),
            Vec::new(),
        )
        .unwrap();
    (cache, feed_owner(), key)
}

#[test]
fn watch_key_on_unregistered_key_errors() {
    let cache = Cache::new();

    let err = cache.watch_key(&key("missing.key")).unwrap_err();

    assert_eq!(err, CacheError::MissingCell(key("missing.key")));
}

#[tokio::test]
async fn changed_parks_until_value_write() {
    let (cache, owner, status_key) = status_cache();
    let mut watch = cache.watch_key(status_key.key()).unwrap();

    assert!(timeout(PARK, watch.changed()).await.is_err());

    cache
        .set_value(&owner, &status_key, Status { playing: true })
        .unwrap();

    timeout(WAKE, watch.changed()).await.unwrap().unwrap();
    assert_eq!(watch.generation(), 1);
}

#[test]
fn every_value_op_bumps_generation() {
    let (cache, owner, status_key) = status_cache();
    let watch = cache.watch_key(status_key.key()).unwrap();

    cache
        .set_value(&owner, &status_key, Status { playing: true })
        .unwrap();
    assert_eq!(watch.generation(), 1);

    cache.clear_value(&owner, &status_key).unwrap();
    assert_eq!(watch.generation(), 2);

    cache
        .update_value(&owner, &status_key, |status| {
            *status = Some(Status { playing: false });
        })
        .unwrap();
    assert_eq!(watch.generation(), 3);
}

#[test]
fn every_array_op_bumps_generation() {
    let (cache, owner, batches_key) = batches_cache();
    let watch = cache.watch_key(batches_key.key()).unwrap();

    cache
        .replace_array(&owner, &batches_key, vec![Batch { seq: 1 }])
        .unwrap();
    assert_eq!(watch.generation(), 1);

    cache
        .push_array(&owner, &batches_key, vec![Batch { seq: 2 }])
        .unwrap();
    assert_eq!(watch.generation(), 2);

    cache
        .insert_array(&owner, &batches_key, 1, vec![Batch { seq: 3 }])
        .unwrap();
    assert_eq!(watch.generation(), 3);

    cache
        .replace_array_range(&owner, &batches_key, 0..1, vec![Batch { seq: 4 }])
        .unwrap();
    assert_eq!(watch.generation(), 4);

    cache
        .remove_array_range(&owner, &batches_key, 0..1)
        .unwrap();
    assert_eq!(watch.generation(), 5);

    cache.clear_array(&owner, &batches_key).unwrap();
    assert_eq!(watch.generation(), 6);

    cache
        .update_array(&owner, &batches_key, |items| items.push(Batch { seq: 5 }))
        .unwrap();
    assert_eq!(watch.generation(), 7);
}

#[test]
fn failed_mutation_does_not_bump() {
    let (cache, owner, batches_key) = batches_cache();
    let watch = cache.watch_key(batches_key.key()).unwrap();

    cache
        .insert_array(&owner, &batches_key, 1, vec![Batch { seq: 1 }])
        .unwrap_err();
    cache
        .remove_array_range(&owner, &batches_key, 0..1)
        .unwrap_err();
    cache
        .set_value(
            &CellOwner::new("test.other").unwrap(),
            &cache
                .register_value::<Status>(descriptor("test.owned", CellKind::Value), None)
                .unwrap(),
            Status { playing: true },
        )
        .unwrap_err();

    assert_eq!(watch.generation(), 0);
}

#[tokio::test]
async fn write_while_busy_makes_next_changed_immediate() {
    let (cache, owner, status_key) = status_cache();
    let mut watch = cache.watch_key(status_key.key()).unwrap();

    // The watcher is "busy": the write lands before anyone awaits changed().
    cache
        .set_value(&owner, &status_key, Status { playing: true })
        .unwrap();

    timeout(WAKE, watch.changed()).await.unwrap().unwrap();
}

#[tokio::test]
async fn rapid_writes_coalesce_into_one_wakeup() {
    let (cache, owner, batches_key) = batches_cache();
    let mut watch = cache.watch_key(batches_key.key()).unwrap();

    for seq in 1..=5 {
        cache
            .push_array(&owner, &batches_key, vec![Batch { seq }])
            .unwrap();
    }

    timeout(WAKE, watch.changed()).await.unwrap().unwrap();
    assert_eq!(watch.generation(), 5);
    assert_eq!(cache.read_array(&batches_key).unwrap().len(), 5);

    // The burst was one wakeup: nothing further is pending.
    assert!(timeout(PARK, watch.changed()).await.is_err());
}

#[tokio::test]
async fn watches_on_different_keys_wake_independently() {
    let cache = Cache::new();
    let owner = feed_owner();
    let status_key = cache
        .register_value::<Status>(
            descriptor("feed.databento.es_mbo.status", CellKind::Value),
            None,
        )
        .unwrap();
    let batches_key = cache
        .register_array::<Batch>(
            descriptor("feed.databento.es_mbo.batches", CellKind::Array),
            Vec::new(),
        )
        .unwrap();
    let mut status_watch = cache.watch_key(status_key.key()).unwrap();
    let mut batches_watch = cache.watch_key(batches_key.key()).unwrap();

    cache
        .set_value(&owner, &status_key, Status { playing: true })
        .unwrap();

    timeout(WAKE, status_watch.changed())
        .await
        .unwrap()
        .unwrap();
    assert!(timeout(PARK, batches_watch.changed()).await.is_err());
}

#[tokio::test]
async fn parked_watcher_wakes_from_another_task() {
    let (cache, owner, status_key) = status_cache();
    let mut watch = cache.watch_key(status_key.key()).unwrap();

    let writer = tokio::spawn({
        let cache = cache.clone();
        let status_key = status_key.clone();
        async move {
            tokio::time::sleep(PARK).await;
            cache
                .set_value(&owner, &status_key, Status { playing: true })
                .unwrap();
        }
    });

    timeout(WAKE, watch.changed()).await.unwrap().unwrap();
    writer.await.unwrap();
    assert_eq!(
        cache.read_value(&status_key).unwrap(),
        Some(Status { playing: true })
    );
}

#[tokio::test]
async fn changed_after_cache_dropped_errors() {
    let (cache, _owner, status_key) = status_cache();
    let mut watch = cache.watch_key(status_key.key()).unwrap();
    drop(cache);

    let err = timeout(WAKE, watch.changed()).await.unwrap().unwrap_err();

    assert_eq!(
        err,
        CacheError::WatchClosed {
            key: key("feed.databento.es_mbo.status"),
        }
    );
}
