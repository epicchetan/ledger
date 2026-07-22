mod support;

use std::collections::HashMap;
use std::time::Duration;

use ledger::projection::{
    AppliedProjectionPosition, BarsDeliveryPosition, BarsParams, ProjectionDeliveryEvent,
    ProjectionDeliveryFrame, ProjectionDemand, ProjectionFrameOperation, ProjectionFramePayload,
    ProjectionFrameReason, ProjectionPosition, ProjectionSpec,
    ProjectionSubscriptionProjectionRequest, ProjectionSubscriptionRequest,
};
use ledger::session::{LedgerSessionBuilder, LedgerSessionHandle};
use tokio::sync::mpsc;

use support::{fabricate_prepared_day, store_fixture, trade, StoreFixture};

const WAKE: Duration = Duration::from_secs(2);

#[tokio::test]
async fn bars_delivery_invalidates_old_epoch_and_resumes_from_validated_head() {
    let mut running = start_session().await;
    let session = &running.session;
    let events = &mut running.events;
    let delivery = session.projection_delivery().unwrap().clone();

    let first = delivery
        .subscribe(subscription("consumer-a", None, vec![1]))
        .await
        .unwrap();
    let initial = next_frame(events, &first.subscription_id).await;
    assert_eq!(initial.operation, ProjectionFrameOperation::Snapshot);
    assert_eq!(initial.reason, ProjectionFrameReason::Initial);
    assert!(initial.base.is_none());
    assert_eq!(bars_len(&initial.payload), 0);
    let initial_head = initial.head.clone();
    delivery
        .acknowledge(
            first.subscription_id.clone(),
            vec![AppliedProjectionPosition {
                spec: "bars:100ns".to_string(),
                head: initial_head.clone(),
            }],
        )
        .await
        .unwrap();

    session.seek_to(350).await.unwrap();
    let rebuilt = next_frame(events, &first.subscription_id).await;
    assert_eq!(rebuilt.reason, ProjectionFrameReason::Cadence);
    assert_eq!(rebuilt.operation, ProjectionFrameOperation::Snapshot);
    assert_eq!(bars_len(&rebuilt.payload), 3);
    let head = rebuilt.head.clone();
    delivery
        .acknowledge(
            first.subscription_id.clone(),
            vec![AppliedProjectionPosition {
                spec: "bars:100ns".to_string(),
                head: head.clone(),
            }],
        )
        .await
        .unwrap();
    delivery
        .acknowledge(
            first.subscription_id.clone(),
            vec![AppliedProjectionPosition {
                spec: "bars:100ns".to_string(),
                head: head.clone(),
            }],
        )
        .await
        .expect("duplicate acknowledgment is idempotent");
    let regressed = delivery
        .acknowledge(
            first.subscription_id.clone(),
            vec![AppliedProjectionPosition {
                spec: "bars:100ns".to_string(),
                head: initial_head,
            }],
        )
        .await
        .unwrap_err();
    assert!(regressed.to_string().contains("invalid acknowledgment"));
    let metrics = delivery.metrics();
    assert!(metrics.atomic_collects >= 2);
    assert!(metrics.frames_admitted >= 2);

    let resumed = delivery
        .subscribe(subscription("consumer-b", Some(head.clone()), vec![1]))
        .await
        .unwrap();
    let suffix = next_frame(events, &resumed.subscription_id).await;
    assert_eq!(suffix.base, Some(head.clone()));
    assert_eq!(suffix.head, head);
    assert_eq!(suffix.operation, ProjectionFrameOperation::Append);
    assert_eq!(bars_len(&suffix.payload), 0);

    running.session.shutdown().await.unwrap();
}

#[tokio::test]
async fn same_position_seek_starts_another_zero_origin_epoch() {
    let mut running = start_session().await;
    let delivery = running.session.projection_delivery().unwrap().clone();
    let subscribed = delivery
        .subscribe(subscription("consumer-same-position", None, vec![1]))
        .await
        .unwrap();
    let _ = next_frame(&mut running.events, &subscribed.subscription_id).await;

    running.session.seek_to(350).await.unwrap();
    let first = next_frame(&mut running.events, &subscribed.subscription_id).await;
    running.session.seek_to(350).await.unwrap();
    let second = next_frame(&mut running.events, &subscribed.subscription_id).await;

    assert_eq!(
        position_epoch(&second.head),
        position_epoch(&first.head) + 1
    );
    assert_eq!(second.operation, ProjectionFrameOperation::Snapshot);
    assert!(second.base.is_none());
    assert_eq!(bars_len(&second.payload), 3);

    running.session.shutdown().await.unwrap();
}

#[tokio::test]
async fn one_and_five_minute_sources_deliver_independent_frames_from_one_session() {
    let fixture = store_fixture();
    let events = vec![
        trade(100, 1, 100, 1, None),
        trade(61_000_000_000, 2, 101, 2, None),
        trade(301_000_000_000, 3, 102, 3, None),
    ];
    let (raw_id, _) = fabricate_prepared_day(&fixture.store, events).await;
    let mut builder = LedgerSessionBuilder::new(fixture.store.clone()).unwrap();
    let feed = builder.es_replay(raw_id).unwrap();
    let outputs = builder
        .projections(
            &feed,
            &[
                ProjectionSpec::Bars(BarsParams::time(60_000_000_000)),
                ProjectionSpec::Bars(BarsParams::time(5 * 60_000_000_000)),
            ],
        )
        .unwrap();
    assert_eq!(outputs[0].canonical_spec(), "bars:1m");
    assert_eq!(outputs[1].canonical_spec(), "bars:5m");

    let session = builder.start().await.unwrap();
    let mut events = session.take_projection_events().unwrap();
    let delivery = session.projection_delivery().unwrap().clone();
    let subscribed = delivery
        .subscribe(ProjectionSubscriptionRequest {
            consumer_instance_id: "consumer-multi".to_string(),
            projections: vec![
                ProjectionSubscriptionProjectionRequest {
                    spec: "bars:1m".to_string(),
                    schema_versions: vec![1],
                    requested_max_fps: Some(20),
                    have: None,
                },
                ProjectionSubscriptionProjectionRequest {
                    spec: "bars:5m".to_string(),
                    schema_versions: vec![1],
                    requested_max_fps: Some(20),
                    have: None,
                },
            ],
        })
        .await
        .unwrap();
    assert_eq!(
        subscribed
            .projections
            .iter()
            .map(|projection| projection.spec.as_str())
            .collect::<Vec<_>>(),
        vec!["bars:1m", "bars:5m"]
    );
    let initial = next_frames(&mut events, &subscribed.subscription_id, 2).await;
    assert_eq!(initial["bars:1m"].reason, ProjectionFrameReason::Initial);
    assert_eq!(initial["bars:5m"].reason, ProjectionFrameReason::Initial);

    session.seek_to(301_000_000_000).await.unwrap();
    let final_frames = next_frames(&mut events, &subscribed.subscription_id, 2).await;
    assert_eq!(
        final_frames["bars:1m"].reason,
        ProjectionFrameReason::Cadence
    );
    assert_eq!(
        final_frames["bars:5m"].reason,
        ProjectionFrameReason::Cadence
    );
    assert_eq!(
        final_frames["bars:1m"].operation,
        ProjectionFrameOperation::Snapshot
    );
    assert_eq!(
        final_frames["bars:5m"].operation,
        ProjectionFrameOperation::Snapshot
    );
    assert_eq!(bars_len(&final_frames["bars:1m"].payload), 2);
    assert_eq!(bars_len(&final_frames["bars:5m"].payload), 1);

    session.shutdown().await.unwrap();
}

#[tokio::test]
async fn time_tick_time_rebuilds_in_place_and_reuses_removed_source_keys() {
    let mut running = start_session().await;
    let session = &running.session;
    let events = &mut running.events;
    let delivery = session.projection_delivery().unwrap().clone();
    session.seek_to(350).await.unwrap();

    let changed = session
        .set_projections(vec![ProjectionSpec::Bars(BarsParams::ticks(2))])
        .await
        .unwrap();
    assert!(changed.changed);
    assert_eq!(changed.projections[0].canonical_spec(), "bars:2t");
    assert_eq!(session.projection_specs(), vec!["bars:2t"]);

    let subscribed = delivery
        .subscribe(ProjectionSubscriptionRequest {
            consumer_instance_id: "consumer-replaced".to_string(),
            projections: vec![ProjectionSubscriptionProjectionRequest {
                spec: "bars:2t".to_string(),
                schema_versions: vec![1],
                requested_max_fps: Some(20),
                have: None,
            }],
        })
        .await
        .unwrap();
    let frame = next_frame(events, &subscribed.subscription_id).await;
    assert_eq!(frame.operation, ProjectionFrameOperation::Snapshot);
    assert_eq!(bars_len(&frame.payload), 2);
    assert_eq!(position_epoch(&frame.head), changed.epoch);

    let unchanged = session
        .set_projections(vec![ProjectionSpec::Bars(BarsParams::ticks(2))])
        .await
        .unwrap();
    assert!(!unchanged.changed);
    assert_eq!(unchanged.epoch, changed.epoch);

    let restored = session
        .set_projections(vec![ProjectionSpec::Bars(BarsParams::time(100))])
        .await
        .unwrap();
    assert!(restored.changed);
    assert!(restored.epoch > changed.epoch);
    assert_eq!(session.projection_specs(), vec!["bars:100ns"]);
    assert!(delivery
        .unsubscribe(subscribed.subscription_id)
        .await
        .unwrap());

    let restored_subscription = delivery
        .subscribe(subscription("consumer-restored", None, vec![1]))
        .await
        .unwrap();
    let restored_frame = next_frame(events, &restored_subscription.subscription_id).await;
    assert_eq!(restored_frame.operation, ProjectionFrameOperation::Snapshot);
    assert_eq!(bars_len(&restored_frame.payload), 3);
    assert_eq!(position_epoch(&restored_frame.head), restored.epoch);

    running.session.shutdown().await.unwrap();
}

#[tokio::test]
async fn session_with_an_empty_initial_graph_can_install_its_first_projection() {
    let fixture = store_fixture();
    let (raw_id, _) =
        fabricate_prepared_day(&fixture.store, vec![trade(10, 1, 100, 1, None)]).await;
    let mut builder = LedgerSessionBuilder::new(fixture.store.clone()).unwrap();
    let feed = builder.es_replay(raw_id).unwrap();
    builder.projections(&feed, &[]).unwrap();
    let session = builder.start().await.unwrap();
    let mut events = session.take_projection_events().unwrap();

    let changed = session
        .set_projections(vec![ProjectionSpec::Bars(BarsParams::time(100))])
        .await
        .unwrap();
    assert!(changed.changed);
    assert_eq!(session.projection_specs(), vec!["bars:100ns"]);
    let delivery = session.projection_delivery().unwrap();
    let subscribed = delivery
        .subscribe(subscription("consumer-first", None, vec![1]))
        .await
        .unwrap();
    let frame = next_frame(&mut events, &subscribed.subscription_id).await;
    assert_eq!(frame.operation, ProjectionFrameOperation::Snapshot);
    assert_eq!(position_epoch(&frame.head), changed.epoch);

    session.shutdown().await.unwrap();
}

#[tokio::test]
async fn invalid_positions_and_schemas_cannot_corrupt_delivery_state() {
    let mut running = start_session().await;
    let session = &running.session;
    let events = &mut running.events;
    let delivery = session.projection_delivery().unwrap().clone();
    session.seek_to(350).await.unwrap();

    let generation = delivery.session_generation();
    let invalid = ProjectionPosition::Bars(BarsDeliveryPosition {
        session_generation: generation,
        epoch: 0,
        projection_revision: u64::MAX,
        processed_batches: usize::MAX,
        completed_bars: usize::MAX,
    });
    let subscribed = delivery
        .subscribe(subscription(
            "consumer-invalid",
            Some(invalid.clone()),
            vec![1],
        ))
        .await
        .unwrap();
    let frame = next_frame(events, &subscribed.subscription_id).await;
    assert_eq!(frame.operation, ProjectionFrameOperation::Snapshot);
    assert!(frame.base.is_none());
    assert_eq!(bars_len(&frame.payload), 3);

    let beyond = ProjectionPosition::Bars(BarsDeliveryPosition {
        session_generation: generation,
        epoch: 0,
        projection_revision: u64::MAX,
        processed_batches: usize::MAX,
        completed_bars: usize::MAX,
    });
    let error = delivery
        .acknowledge(
            subscribed.subscription_id.clone(),
            vec![AppliedProjectionPosition {
                spec: "bars:100ns".to_string(),
                head: beyond,
            }],
        )
        .await
        .unwrap_err();
    assert!(error.to_string().contains("invalid acknowledgment"));

    delivery
        .resync(
            subscribed.subscription_id.clone(),
            vec![AppliedProjectionPosition {
                spec: "bars:100ns".to_string(),
                head: invalid,
            }],
            "base_mismatch".to_string(),
        )
        .await
        .unwrap();
    let resynced = next_frame(events, &subscribed.subscription_id).await;
    assert_eq!(resynced.reason, ProjectionFrameReason::Resync);
    assert_eq!(resynced.operation, ProjectionFrameOperation::Snapshot);
    assert!(resynced.base.is_none());
    assert_eq!(bars_len(&resynced.payload), 3);

    let schema_error = delivery
        .subscribe(subscription("consumer-schema", None, vec![99]))
        .await
        .unwrap_err();
    assert!(schema_error.to_string().contains("unsupported schema"));
    assert_eq!(delivery.metrics().schema_rejections, 1);

    running.session.shutdown().await.unwrap();
}

#[tokio::test]
async fn inactive_consumer_skips_rebuild_frames_and_resumes_with_a_snapshot() {
    let mut running = start_session().await;
    let session = &running.session;
    let events = &mut running.events;
    let delivery = session.projection_delivery().unwrap().clone();

    let subscribed = delivery
        .subscribe(subscription("consumer-inactive", None, vec![1]))
        .await
        .unwrap();
    let initial = next_frame(events, &subscribed.subscription_id).await;
    let initial_head = initial.head;
    delivery
        .demand(
            subscribed.subscription_id.clone(),
            ProjectionDemand {
                active: false,
                requested_max_fps: Some(20),
            },
        )
        .await
        .unwrap();

    session.seek_to(350).await.unwrap();
    assert_no_frame(events, &subscribed.subscription_id).await;

    let feed_status = session
        .cache()
        .read_value(&running.feed.status)
        .unwrap()
        .expect("feed status after seek");
    assert_eq!(
        feed_status.clock.revision,
        session.clock_snapshot().unwrap().revision
    );
    assert_eq!(
        feed_status.cursor,
        session
            .cache()
            .read_value(&running.feed.cursor)
            .unwrap()
            .unwrap()
    );

    delivery
        .demand(
            subscribed.subscription_id.clone(),
            ProjectionDemand {
                active: true,
                requested_max_fps: Some(20),
            },
        )
        .await
        .unwrap();
    let resumed = next_frame(events, &subscribed.subscription_id).await;
    assert_eq!(resumed.reason, ProjectionFrameReason::Cadence);
    assert_ne!(resumed.head, initial_head);
    assert_eq!(resumed.base, None);
    assert_eq!(resumed.operation, ProjectionFrameOperation::Snapshot);
    assert_eq!(bars_len(&resumed.payload), 3);

    running.session.shutdown().await.unwrap();
}

#[tokio::test(start_paused = true)]
async fn abandoned_subscription_expires_without_closing_session_truth() {
    let mut running = start_session().await;
    let session = &running.session;
    let events = &mut running.events;
    let delivery = session.projection_delivery().unwrap().clone();

    let subscribed = delivery
        .subscribe(subscription("consumer-abandoned", None, vec![1]))
        .await
        .unwrap();
    let _ = next_frame(events, &subscribed.subscription_id).await;

    tokio::time::advance(Duration::from_secs(31)).await;
    let expired = tokio::time::timeout(WAKE, async {
        loop {
            match events.recv().await.expect("delivery stream open") {
                ProjectionDeliveryEvent::SubscriptionExpired { subscription_id }
                    if subscription_id == subscribed.subscription_id =>
                {
                    return;
                }
                ProjectionDeliveryEvent::Frame(_)
                | ProjectionDeliveryEvent::Watermark(_)
                | ProjectionDeliveryEvent::SubscriptionExpired { .. } => {}
            }
        }
    })
    .await;
    assert!(expired.is_ok(), "subscription lease did not expire");
    assert_eq!(delivery.metrics().lease_expirations, 1);
    assert!(delivery
        .demand(
            subscribed.subscription_id,
            ProjectionDemand {
                active: true,
                requested_max_fps: Some(20),
            },
        )
        .await
        .unwrap_err()
        .to_string()
        .contains("unknown projection subscription"));

    assert!(session.clock_snapshot().is_ok());
    running.session.shutdown().await.unwrap();
}

struct RunningDeliverySession {
    _fixture: StoreFixture,
    session: LedgerSessionHandle,
    feed: ledger::feed::es_replay::EsReplayCells,
    events: mpsc::Receiver<ProjectionDeliveryEvent>,
}

async fn start_session() -> RunningDeliverySession {
    let fixture = store_fixture();
    let events = vec![
        trade(10, 1, 100, 1, None),
        trade(150, 2, 101, 1, None),
        trade(250, 3, 102, 1, None),
        trade(350, 4, 103, 1, None),
    ];
    let (raw_id, _) = fabricate_prepared_day(&fixture.store, events).await;
    let mut builder = LedgerSessionBuilder::new(fixture.store.clone()).unwrap();
    let feed = builder.es_replay(raw_id).unwrap();
    builder
        .projections(&feed, &[ProjectionSpec::Bars(BarsParams::time(100))])
        .unwrap();
    let session = builder.start().await.unwrap();
    let events = session
        .take_projection_events()
        .expect("delivery events available once");
    RunningDeliverySession {
        _fixture: fixture,
        session,
        feed,
        events,
    }
}

fn subscription(
    consumer: &str,
    have: Option<ProjectionPosition>,
    schemas: Vec<u16>,
) -> ProjectionSubscriptionRequest {
    ProjectionSubscriptionRequest {
        consumer_instance_id: consumer.to_string(),
        projections: vec![ProjectionSubscriptionProjectionRequest {
            spec: "bars:100ns".to_string(),
            schema_versions: schemas,
            requested_max_fps: Some(20),
            have,
        }],
    }
}

async fn next_frame(
    events: &mut mpsc::Receiver<ProjectionDeliveryEvent>,
    subscription_id: &str,
) -> ledger::projection::ProjectionDeliveryFrame {
    tokio::time::timeout(WAKE, async {
        loop {
            match events.recv().await.expect("delivery stream open") {
                ProjectionDeliveryEvent::Frame(frame)
                    if frame.subscription_id == subscription_id =>
                {
                    return *frame;
                }
                ProjectionDeliveryEvent::Frame(_)
                | ProjectionDeliveryEvent::Watermark(_)
                | ProjectionDeliveryEvent::SubscriptionExpired { .. } => {}
            }
        }
    })
    .await
    .expect("projection frame should arrive")
}

async fn next_frames(
    events: &mut mpsc::Receiver<ProjectionDeliveryEvent>,
    subscription_id: &str,
    count: usize,
) -> HashMap<String, ProjectionDeliveryFrame> {
    tokio::time::timeout(WAKE, async {
        let mut frames = HashMap::new();
        while frames.len() < count {
            match events.recv().await.expect("delivery stream open") {
                ProjectionDeliveryEvent::Frame(frame)
                    if frame.subscription_id == subscription_id =>
                {
                    frames.insert(frame.spec.clone(), *frame);
                }
                ProjectionDeliveryEvent::Frame(_)
                | ProjectionDeliveryEvent::Watermark(_)
                | ProjectionDeliveryEvent::SubscriptionExpired { .. } => {}
            }
        }
        frames
    })
    .await
    .expect("projection frames should arrive")
}

async fn assert_no_frame(
    events: &mut mpsc::Receiver<ProjectionDeliveryEvent>,
    subscription_id: &str,
) {
    let result = tokio::time::timeout(Duration::from_millis(125), async {
        loop {
            match events.recv().await.expect("delivery stream open") {
                ProjectionDeliveryEvent::Frame(frame)
                    if frame.subscription_id == subscription_id =>
                {
                    return;
                }
                ProjectionDeliveryEvent::Frame(_)
                | ProjectionDeliveryEvent::Watermark(_)
                | ProjectionDeliveryEvent::SubscriptionExpired { .. } => {}
            }
        }
    })
    .await;
    assert!(
        result.is_err(),
        "inactive consumer received a projection frame"
    );
}

fn bars_len(payload: &ProjectionFramePayload) -> usize {
    match payload {
        ProjectionFramePayload::Bars(payload) => payload.bars.len(),
    }
}

fn position_epoch(position: &ProjectionPosition) -> u64 {
    match position {
        ProjectionPosition::Bars(position) => position.epoch,
    }
}
