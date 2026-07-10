mod support;

use std::time::Duration;

use ledger::projection::{
    AppliedProjectionPosition, BarsDeliveryPosition, BarsParams, ProjectionDeliveryEvent,
    ProjectionDemand, ProjectionFrameOperation, ProjectionFramePayload, ProjectionFrameReason,
    ProjectionPosition, ProjectionSubscriptionProjectionRequest, ProjectionSubscriptionRequest,
};
use ledger::session::{LedgerSessionBuilder, LedgerSessionHandle};
use tokio::sync::mpsc;

use support::{fabricate_prepared_day, store_fixture, trade, StoreFixture};

const WAKE: Duration = Duration::from_secs(2);

#[tokio::test]
async fn bars_delivery_snapshots_seeks_once_and_resumes_from_validated_head() {
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
    let seek_final = next_frame(events, &first.subscription_id).await;
    assert_eq!(seek_final.reason, ProjectionFrameReason::SeekFinal);
    assert_eq!(seek_final.operation, ProjectionFrameOperation::Append);
    assert_eq!(bars_len(&seek_final.payload), 3);
    let head = seek_final.head.clone();
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
    assert_eq!(metrics.atomic_collects, 2);
    assert_eq!(metrics.frames_admitted, 2);
    assert_eq!(metrics.seek_barriers_completed, 1);

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
async fn inactive_consumer_skips_seek_final_and_resumes_from_its_sent_head() {
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
    wait_for_seek_barrier(&delivery, 1).await;
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
    assert_eq!(resumed.base, Some(initial_head));
    assert_eq!(resumed.operation, ProjectionFrameOperation::Append);
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
        .bars(&feed, BarsParams { interval_ns: 100 })
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

async fn wait_for_seek_barrier(
    delivery: &ledger::projection::ProjectionDeliveryHandle,
    completed: u64,
) {
    tokio::time::timeout(WAKE, async {
        while delivery.metrics().seek_barriers_completed < completed {
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("seek barrier should complete");
}

fn bars_len(payload: &ProjectionFramePayload) -> usize {
    match payload {
        ProjectionFramePayload::Bars(payload) => payload.bars.len(),
    }
}
