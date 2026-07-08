mod support;

use std::time::Duration;

use cache::{CacheError, CellOwner};
use ledger::feed::es_replay::{EsReplayCells, EsReplayCursor};
use ledger::market::{BookAction, BookSide, EsMboEvent, PriceTicks};
use ledger::projection::{Bar, BarsCells, BarsParams, BarsStatus};
use ledger::session::{LedgerSessionBuilder, LedgerSessionHandle};
use ledger::LedgerError;
use runtime::ExternalWriteBatch;
use tokio::time::timeout;

use support::{event, fabricate_prepared_day, store_fixture, trade, StoreFixture};

const WAKE: Duration = Duration::from_secs(2);

#[tokio::test]
async fn prepare_publishes_initial_status_before_any_seek() {
    let running = RunningBarsSession::start(vec![trade(100, 1, 100, 1, Some(BookSide::Bid))]).await;

    let status = running
        .session
        .cache()
        .read_value(&running.bars.status)
        .unwrap()
        .unwrap();
    assert_eq!(
        status,
        BarsStatus {
            spec: "bars:100ns".to_string(),
            epoch: 0,
            processed_batches: 0,
            completed_bars: 0,
            last_ts_event_ns: None,
        }
    );
    assert!(running
        .session
        .cache()
        .read_array(&running.bars.bars)
        .unwrap()
        .is_empty());
    assert!(running
        .session
        .cache()
        .read_value(&running.bars.live)
        .unwrap()
        .is_none());

    running.shutdown().await;
}

#[tokio::test]
async fn bars_aggregate_ohlc_volume_delta_and_counts_across_bucket_boundaries() {
    let mut running = RunningBarsSession::start(vec![
        trade(10, 1, 100, 2, Some(BookSide::Bid)),
        trade(20, 2, 105, 3, Some(BookSide::Ask)),
        trade(150, 3, 99, 4, Some(BookSide::Bid)),
        trade(350, 4, 110, 5, Some(BookSide::Ask)),
    ])
    .await;

    let cursor = running.seek_and_wait(350, |cursor| cursor.ended).await;
    running.wait_caught_up(&cursor).await;
    let (bars, live) = running.snapshot();

    assert_eq!(bars.len(), 2);
    assert_eq!(bar_starts(&bars), vec![0, 100]);
    assert_eq!(
        bars[0],
        Bar {
            interval_start_ns: 0,
            open: PriceTicks(100),
            high: PriceTicks(105),
            low: PriceTicks(100),
            close: PriceTicks(105),
            volume: 5,
            buy_volume: 2,
            sell_volume: 3,
            trade_count: 2,
            first_ts_event_ns: 10,
            last_ts_event_ns: 20,
        }
    );
    assert_eq!(
        bars[1],
        Bar {
            interval_start_ns: 100,
            open: PriceTicks(99),
            high: PriceTicks(99),
            low: PriceTicks(99),
            close: PriceTicks(99),
            volume: 4,
            buy_volume: 4,
            sell_volume: 0,
            trade_count: 1,
            first_ts_event_ns: 150,
            last_ts_event_ns: 150,
        }
    );
    assert_eq!(
        live.unwrap(),
        Bar {
            interval_start_ns: 300,
            open: PriceTicks(110),
            high: PriceTicks(110),
            low: PriceTicks(110),
            close: PriceTicks(110),
            volume: 5,
            buy_volume: 0,
            sell_volume: 5,
            trade_count: 1,
            first_ts_event_ns: 350,
            last_ts_event_ns: 350,
        }
    );

    running.shutdown().await;
}

#[tokio::test]
async fn non_print_events_and_fill_contribute_nothing() {
    let mut running = RunningBarsSession::start(vec![
        action_event(10, 1, BookAction::Add),
        action_event(20, 2, BookAction::Cancel),
        action_event(30, 3, BookAction::Fill),
        trade(40, 4, 100, 7, Some(BookSide::Bid)),
        trade(140, 5, 101, 1, Some(BookSide::Ask)),
    ])
    .await;

    let cursor = running.seek_and_wait(140, |cursor| cursor.ended).await;
    running.wait_caught_up(&cursor).await;
    let (bars, live) = running.snapshot();

    assert_eq!(bars.len(), 1);
    assert_eq!(bars[0].volume, 7);
    assert_eq!(bars[0].trade_count, 1);
    assert_eq!(bars[0].buy_volume, 7);
    assert_eq!(bars[0].sell_volume, 0);
    assert_eq!(live.unwrap().volume, 1);

    running.shutdown().await;
}

#[tokio::test]
async fn unattributed_prints_count_volume_without_buy_or_sell_volume() {
    let mut running = RunningBarsSession::start(vec![
        trade(10, 1, 100, 4, None),
        trade(150, 2, 101, 1, Some(BookSide::Bid)),
    ])
    .await;

    let cursor = running.seek_and_wait(150, |cursor| cursor.ended).await;
    running.wait_caught_up(&cursor).await;
    let (bars, _live) = running.snapshot();

    assert_eq!(bars.len(), 1);
    assert_eq!(bars[0].volume, 4);
    assert_eq!(bars[0].trade_count, 1);
    assert_eq!(bars[0].buy_volume, 0);
    assert_eq!(bars[0].sell_volume, 0);

    running.shutdown().await;
}

#[tokio::test]
async fn live_bar_stays_live_at_feed_end_and_is_not_promoted() {
    let mut running = RunningBarsSession::start(vec![
        trade(10, 1, 100, 1, Some(BookSide::Bid)),
        trade(150, 2, 101, 1, Some(BookSide::Ask)),
    ])
    .await;

    let cursor = running.seek_and_wait(150, |cursor| cursor.ended).await;
    let status = running.wait_caught_up(&cursor).await;
    let (bars, live) = running.snapshot();

    assert_eq!(status.completed_bars, 1);
    assert_eq!(bars.len(), 1);
    assert_eq!(bars[0].interval_start_ns, 0);
    assert_eq!(live.unwrap().interval_start_ns, 100);

    running.shutdown().await;
}

#[tokio::test]
async fn completed_bars_are_immutable_under_sequential_forward_seeks() {
    let mut running = RunningBarsSession::start(vec![
        trade(10, 1, 100, 1, Some(BookSide::Bid)),
        trade(150, 2, 101, 1, Some(BookSide::Ask)),
        trade(250, 3, 102, 1, Some(BookSide::Bid)),
    ])
    .await;

    let cursor = running
        .seek_and_wait(150, |cursor| cursor.batch_idx == 2)
        .await;
    running.wait_caught_up(&cursor).await;
    let first_bars = running.snapshot().0;

    let cursor = running.seek_and_wait(250, |cursor| cursor.ended).await;
    running.wait_caught_up(&cursor).await;
    let second_bars = running.snapshot().0;

    assert_eq!(first_bars.len(), 1);
    assert_eq!(&second_bars[..first_bars.len()], first_bars.as_slice());

    running.shutdown().await;
}

#[tokio::test]
async fn incremental_seeks_match_one_single_seek_over_the_same_events() {
    let events = vec![
        trade(10, 1, 100, 2, Some(BookSide::Bid)),
        trade(20, 2, 101, 3, Some(BookSide::Ask)),
        trade(150, 3, 99, 4, None),
        trade(250, 4, 105, 5, Some(BookSide::Bid)),
        trade(350, 5, 103, 6, Some(BookSide::Ask)),
    ];

    let mut incremental = RunningBarsSession::start(events.clone()).await;
    for (target, expected_batches) in [(10, 1), (20, 2), (150, 3), (250, 4), (350, 5)] {
        let cursor = incremental
            .seek_and_wait(target, |cursor| cursor.batch_idx == expected_batches)
            .await;
        incremental.wait_caught_up(&cursor).await;
    }
    let incremental_snapshot = incremental.snapshot();

    let mut single = RunningBarsSession::start(events).await;
    let cursor = single.seek_and_wait(350, |cursor| cursor.ended).await;
    single.wait_caught_up(&cursor).await;
    let single_snapshot = single.snapshot();

    assert_eq!(incremental_snapshot, single_snapshot);

    incremental.shutdown().await;
    single.shutdown().await;
}

#[tokio::test]
async fn backward_seek_rebuilds_and_following_forward_seek_reproduces_bars() {
    let mut running = RunningBarsSession::start(vec![
        trade(10, 1, 100, 1, Some(BookSide::Bid)),
        trade(150, 2, 101, 2, Some(BookSide::Ask)),
        trade(250, 3, 102, 3, Some(BookSide::Bid)),
        trade(350, 4, 103, 4, Some(BookSide::Ask)),
    ])
    .await;

    let cursor = running.seek_and_wait(350, |cursor| cursor.ended).await;
    running.wait_caught_up(&cursor).await;
    let pre_regression = running.snapshot();

    let cursor = running
        .seek_and_wait(175, |cursor| cursor.epoch == 1 && cursor.batch_idx == 2)
        .await;
    let status = running.wait_caught_up(&cursor).await;
    let (truncated_bars, truncated_live) = running.snapshot();

    assert_eq!(status.epoch, cursor.epoch);
    assert_eq!(status.processed_batches, cursor.batch_idx);
    assert_eq!(bar_starts(&truncated_bars), vec![0]);
    assert_eq!(truncated_live.unwrap().interval_start_ns, 100);

    let cursor = running.seek_and_wait(350, |cursor| cursor.ended).await;
    running.wait_caught_up(&cursor).await;
    assert_eq!(running.snapshot(), pre_regression);

    running.shutdown().await;
}

#[tokio::test]
async fn two_bar_projections_coexist_on_one_feed_and_both_catch_up() {
    let events = vec![
        trade(100, 1, 100, 1, Some(BookSide::Bid)),
        trade(1_500_000_000, 2, 101, 2, Some(BookSide::Ask)),
        trade(61_000_000_000, 3, 102, 3, Some(BookSide::Bid)),
    ];
    let fixture = store_fixture();
    let (raw_id, _artifact) = fabricate_prepared_day(&fixture.store, events).await;
    let mut builder = LedgerSessionBuilder::new(fixture.store.clone()).unwrap();
    let feed = builder.es_replay(raw_id).unwrap();
    let one_second = builder
        .bars(
            &feed,
            BarsParams {
                interval_ns: 1_000_000_000,
            },
        )
        .unwrap();
    let one_minute = builder
        .bars(
            &feed,
            BarsParams {
                interval_ns: 60_000_000_000,
            },
        )
        .unwrap();
    let session = timeout(WAKE, builder.start()).await.unwrap().unwrap();
    let mut cursor_watch = session.cache().watch_key(feed.cursor.key()).unwrap();
    let mut one_second_watch = session.cache().watch_key(one_second.status.key()).unwrap();
    let mut one_minute_watch = session.cache().watch_key(one_minute.status.key()).unwrap();
    wait_for_cursor(session.cache(), &mut cursor_watch, &feed, |cursor| {
        cursor.feed_seq == 0
    })
    .await;

    seek_to(&session, 61_000_000_000).await;
    let cursor = wait_for_cursor(session.cache(), &mut cursor_watch, &feed, |cursor| {
        cursor.ended
    })
    .await;
    let one_second_status =
        wait_for_catch_up(session.cache(), &mut one_second_watch, &one_second, &cursor).await;
    let one_minute_status =
        wait_for_catch_up(session.cache(), &mut one_minute_watch, &one_minute, &cursor).await;

    assert_eq!(one_second_status.processed_batches, cursor.batch_idx);
    assert_eq!(one_minute_status.processed_batches, cursor.batch_idx);
    assert_eq!(
        session.cache().read_array(&one_second.bars).unwrap().len(),
        2
    );
    assert_eq!(
        session.cache().read_array(&one_minute.bars).unwrap().len(),
        1
    );
    assert!(session
        .cache()
        .read_value(&one_second.live)
        .unwrap()
        .is_some());
    assert!(session
        .cache()
        .read_value(&one_minute.live)
        .unwrap()
        .is_some());

    shutdown_session(session).await;
    let _ = fixture;
}

#[tokio::test]
async fn registering_same_canonical_spec_twice_errors() {
    let fixture = store_fixture();
    let (raw_id, _artifact) =
        fabricate_prepared_day(&fixture.store, vec![trade(100, 1, 100, 1, None)]).await;
    let mut builder = LedgerSessionBuilder::new(fixture.store.clone()).unwrap();
    let feed = builder.es_replay(raw_id).unwrap();
    builder
        .bars(
            &feed,
            BarsParams {
                interval_ns: 60_000_000_000,
            },
        )
        .unwrap();
    let err = builder
        .bars(
            &feed,
            BarsParams {
                interval_ns: 60 * 1_000_000_000,
            },
        )
        .unwrap_err();

    assert!(matches!(
        err,
        LedgerError::Cache(CacheError::DuplicateCell(_))
    ));
}

#[tokio::test]
async fn projection_cells_are_owned_by_projection_component() {
    let running = RunningBarsSession::start(vec![trade(100, 1, 100, 1, None)]).await;
    let descriptor = running
        .session
        .cache()
        .describe(running.bars.bars.key())
        .unwrap();
    assert_eq!(
        descriptor.owner,
        CellOwner::new("projection.bars.100ns").unwrap()
    );

    let mut batch = ExternalWriteBatch::new(CellOwner::new("session").unwrap());
    batch.replace_array(&running.bars.bars, Vec::<Bar>::new());
    timeout(
        WAKE,
        running.session.runtime().submit_external_writes(batch),
    )
    .await
    .unwrap()
    .unwrap();
    let err = timeout(WAKE, running.session.shutdown())
        .await
        .unwrap()
        .unwrap_err();

    assert!(err.to_string().contains("cannot mutate cell"));
}

struct RunningBarsSession {
    _fixture: StoreFixture,
    session: LedgerSessionHandle,
    feed: EsReplayCells,
    bars: BarsCells,
    cursor_watch: cache::CellWatch,
    bars_watch: cache::CellWatch,
}

impl RunningBarsSession {
    async fn start(events: Vec<EsMboEvent>) -> Self {
        Self::start_with_params(events, BarsParams { interval_ns: 100 }).await
    }

    async fn start_with_params(events: Vec<EsMboEvent>, params: BarsParams) -> Self {
        let fixture = store_fixture();
        let (raw_id, _artifact) = fabricate_prepared_day(&fixture.store, events).await;
        let mut builder = LedgerSessionBuilder::new(fixture.store.clone()).unwrap();
        let feed = builder.es_replay(raw_id).unwrap();
        let bars = builder.bars(&feed, params).unwrap();
        let session = timeout(WAKE, builder.start()).await.unwrap().unwrap();
        let mut cursor_watch = session.cache().watch_key(feed.cursor.key()).unwrap();
        let mut bars_watch = session.cache().watch_key(bars.status.key()).unwrap();
        wait_for_cursor(session.cache(), &mut cursor_watch, &feed, |cursor| {
            cursor.feed_seq == 0
        })
        .await;
        wait_for_bars_status(session.cache(), &mut bars_watch, &bars, |status| {
            status.epoch == 0 && status.processed_batches == 0
        })
        .await;

        Self {
            _fixture: fixture,
            session,
            feed,
            bars,
            cursor_watch,
            bars_watch,
        }
    }

    async fn seek_and_wait(
        &mut self,
        session_ns: u64,
        predicate: impl FnMut(&EsReplayCursor) -> bool,
    ) -> EsReplayCursor {
        seek_to(&self.session, session_ns).await;
        wait_for_cursor(
            self.session.cache(),
            &mut self.cursor_watch,
            &self.feed,
            predicate,
        )
        .await
    }

    async fn wait_caught_up(&mut self, cursor: &EsReplayCursor) -> BarsStatus {
        wait_for_catch_up(
            self.session.cache(),
            &mut self.bars_watch,
            &self.bars,
            cursor,
        )
        .await
    }

    fn snapshot(&self) -> (Vec<Bar>, Option<Bar>) {
        (
            self.session.cache().read_array(&self.bars.bars).unwrap(),
            self.session.cache().read_value(&self.bars.live).unwrap(),
        )
    }

    async fn shutdown(self) {
        shutdown_session(self.session).await;
    }
}

async fn seek_to(session: &LedgerSessionHandle, session_ns: u64) {
    timeout(WAKE, session.seek_to(session_ns))
        .await
        .unwrap()
        .unwrap();
}

async fn wait_for_cursor(
    cache: &cache::Cache,
    watch: &mut cache::CellWatch,
    cells: &EsReplayCells,
    mut predicate: impl FnMut(&EsReplayCursor) -> bool,
) -> EsReplayCursor {
    timeout(WAKE, async {
        loop {
            if let Some(cursor) = cache.read_value(&cells.cursor).unwrap() {
                if predicate(&cursor) {
                    return cursor;
                }
            }
            watch.changed().await.unwrap();
        }
    })
    .await
    .unwrap()
}

async fn wait_for_bars_status(
    cache: &cache::Cache,
    watch: &mut cache::CellWatch,
    cells: &BarsCells,
    mut predicate: impl FnMut(&BarsStatus) -> bool,
) -> BarsStatus {
    timeout(WAKE, async {
        loop {
            if let Some(status) = cache.read_value(&cells.status).unwrap() {
                if predicate(&status) {
                    return status;
                }
            }
            watch.changed().await.unwrap();
        }
    })
    .await
    .unwrap()
}

async fn wait_for_catch_up(
    cache: &cache::Cache,
    watch: &mut cache::CellWatch,
    cells: &BarsCells,
    cursor: &EsReplayCursor,
) -> BarsStatus {
    wait_for_bars_status(cache, watch, cells, |status| {
        status.epoch == cursor.epoch && status.processed_batches == cursor.batch_idx
    })
    .await
}

async fn shutdown_session(session: LedgerSessionHandle) {
    timeout(WAKE, session.shutdown()).await.unwrap().unwrap();
}

fn action_event(ts_event_ns: u64, sequence: u64, action: BookAction) -> EsMboEvent {
    let mut event = event(ts_event_ns, sequence);
    event.action = action;
    event.side = Some(BookSide::Ask);
    event.price_ticks = Some(PriceTicks(18_000 + sequence as i64));
    event.size = 10;
    event
}

fn bar_starts(bars: &[Bar]) -> Vec<u64> {
    bars.iter().map(|bar| bar.interval_start_ns).collect()
}
