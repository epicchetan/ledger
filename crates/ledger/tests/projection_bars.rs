mod support;

use std::time::Duration;

use cache::{CellOwner, Key};
use ledger::feed::es_replay::{EsReplayCells, EsReplayCursor};
use ledger::market::{
    canonical_trade_print, BookAction, BookSide, EsMboEvent, PriceTicks, TradePrint,
};
use ledger::projection::{Bar, BarsCells, BarsParams, BarsStatus, ProjectionSpec};
use ledger::session::{LedgerSessionBuilder, LedgerSessionHandle};
use ledger::LedgerError;
use runtime::ExternalWriteBatch;
use tokio::time::timeout;

use support::{event, fabricate_prepared_day, store_fixture, trade, StoreFixture};

const WAKE: Duration = Duration::from_secs(2);
const TEST_FEED_CATCHUP_CHUNK_BATCHES: usize = 1024;
const TEST_REBUILD_CHUNK_BATCHES: usize = 4096;

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
            revision: 0,
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
async fn only_non_print_batches_advance_bars_lineage_without_creating_bars() {
    let mut running = RunningBarsSession::start(vec![
        action_event(10, 1, BookAction::Add),
        action_event(20, 2, BookAction::Fill),
        action_event(30, 3, BookAction::Cancel),
    ])
    .await;

    let cursor = running.seek_and_wait(30, |cursor| cursor.ended).await;
    let status = running.wait_caught_up(&cursor).await;
    let (bars, live) = running.snapshot();

    assert_eq!(status.processed_batches, cursor.batch_idx);
    assert!(status.revision > 0);
    assert!(bars.is_empty());
    assert!(live.is_none());

    running.shutdown().await;
}

#[tokio::test]
async fn one_minute_graph_matches_direct_raw_event_reference_before_and_after_regression() {
    let events = vec![
        trade(10_000_000_000, 1, 18_000, 2, Some(BookSide::Bid)),
        action_event(20_000_000_000, 2, BookAction::Add),
        trade(59_000_000_000, 3, 18_004, 3, Some(BookSide::Ask)),
        trade(61_000_000_000, 4, 17_999, 5, None),
        action_event(70_000_000_000, 5, BookAction::Fill),
        trade(301_000_000_000, 6, 18_010, 7, Some(BookSide::Bid)),
    ];
    let params = BarsParams {
        interval_ns: 60_000_000_000,
    };
    let mut running = RunningBarsSession::start_with_params(events.clone(), params).await;

    let end_cursor = running
        .seek_and_wait(301_000_000_000, |cursor| cursor.ended)
        .await;
    running.wait_caught_up(&end_cursor).await;
    assert_eq!(
        running.snapshot(),
        reference_bars(&events, params.interval_ns)
    );

    let backward_cursor = running
        .seek_and_wait(65_000_000_000, |cursor| {
            cursor.epoch == 1 && cursor.batch_idx == 4
        })
        .await;
    running.wait_caught_up(&backward_cursor).await;
    assert_eq!(
        running.snapshot(),
        reference_bars(&events[..4], params.interval_ns)
    );

    let end_cursor = running
        .seek_and_wait(301_000_000_000, |cursor| cursor.ended)
        .await;
    running.wait_caught_up(&end_cursor).await;
    assert_eq!(
        running.snapshot(),
        reference_bars(&events, params.interval_ns)
    );

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
async fn chunked_feed_and_one_batch_steps_converge_to_same_bars_and_cursor() {
    let events = generated_trades(TEST_FEED_CATCHUP_CHUNK_BATCHES + 9);
    let mut chunked = RunningBarsSession::start(events.clone()).await;
    let chunked_cursor = chunked
        .seek_and_wait(events.len() as u64, |cursor| cursor.ended)
        .await;
    chunked.wait_caught_up(&chunked_cursor).await;
    let chunked_snapshot = chunked.snapshot();

    let mut stepped = RunningBarsSession::start(events).await;
    let stepped_cursor = step_one_batch_at_a_time(&mut stepped).await;
    stepped.wait_caught_up(&stepped_cursor).await;
    let stepped_snapshot = stepped.snapshot();

    assert_eq!(stepped_cursor, chunked_cursor);
    assert!(!stepped_cursor.catching_up);
    assert_eq!(stepped_snapshot, chunked_snapshot);

    chunked.shutdown().await;
    stepped.shutdown().await;
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
async fn rebuild_over_multiple_chunks_matches_fresh_fold_to_same_target() {
    let target = TEST_REBUILD_CHUNK_BATCHES * 2 + 25;
    let events = generated_trades(target + 100);
    let mut rebuilt = RunningBarsSession::start(events.clone()).await;

    let end_cursor = rebuilt
        .seek_and_wait(events.len() as u64, |cursor| cursor.ended)
        .await;
    rebuilt.wait_caught_up(&end_cursor).await;
    let cursor = rebuilt
        .seek_and_wait(target as u64, |cursor| {
            cursor.epoch == 1 && cursor.batch_idx == target
        })
        .await;
    let status = rebuilt.wait_caught_up(&cursor).await;
    let rebuilt_snapshot = rebuilt.snapshot();

    let mut fresh = RunningBarsSession::start(events).await;
    let fresh_cursor = fresh
        .seek_and_wait(target as u64, |cursor| cursor.batch_idx == target)
        .await;
    fresh.wait_caught_up(&fresh_cursor).await;

    assert_eq!(status.processed_batches, target);
    assert_eq!(rebuilt_snapshot, fresh.snapshot());

    rebuilt.shutdown().await;
    fresh.shutdown().await;
}

#[tokio::test]
async fn epoch_change_mid_rebuild_restarts_and_keeps_newest_epoch() {
    let target = TEST_REBUILD_CHUNK_BATCHES * 8 + 25;
    let final_target = 50usize;
    let events = generated_trades(target + 100);
    let mut running = RunningBarsSession::start(events.clone()).await;

    let end_cursor = running
        .seek_and_wait(events.len() as u64, |cursor| cursor.ended)
        .await;
    running.wait_caught_up(&end_cursor).await;
    running
        .seek_and_wait(target as u64, |cursor| {
            cursor.epoch == 1 && cursor.batch_idx == target
        })
        .await;
    wait_for_bars_status(
        running.session.cache(),
        &mut running.bars_watch,
        &running.bars,
        |status| {
            status.epoch == 1 && status.processed_batches > 0 && status.processed_batches < target
        },
    )
    .await;

    let cursor = running
        .seek_and_wait(final_target as u64, |cursor| {
            cursor.epoch == 2 && cursor.batch_idx == final_target
        })
        .await;
    let status = running.wait_caught_up(&cursor).await;
    let newest_snapshot = running.snapshot();

    let mut fresh = RunningBarsSession::start(events).await;
    let fresh_cursor = fresh
        .seek_and_wait(final_target as u64, |cursor| {
            cursor.batch_idx == final_target
        })
        .await;
    fresh.wait_caught_up(&fresh_cursor).await;

    assert_eq!(status.epoch, 2);
    assert_eq!(status.processed_batches, final_target);
    assert_eq!(newest_snapshot, fresh.snapshot());

    running.shutdown().await;
    fresh.shutdown().await;
}

#[tokio::test]
async fn one_and_five_minute_bars_share_one_canonical_stream_and_both_catch_up() {
    let events = vec![
        trade(100, 1, 100, 1, Some(BookSide::Bid)),
        trade(61_000_000_000, 3, 102, 3, Some(BookSide::Bid)),
        trade(301_000_000_000, 4, 103, 4, Some(BookSide::Ask)),
    ];
    let fixture = store_fixture();
    let (raw_id, _artifact) = fabricate_prepared_day(&fixture.store, events).await;
    let mut builder = LedgerSessionBuilder::new(fixture.store.clone()).unwrap();
    let feed = builder.es_replay(raw_id).unwrap();
    let mut projections = builder
        .projections(
            &feed,
            &[
                ProjectionSpec::Bars(BarsParams {
                    interval_ns: 60_000_000_000,
                }),
                ProjectionSpec::Bars(BarsParams {
                    interval_ns: 5 * 60_000_000_000,
                }),
            ],
        )
        .unwrap()
        .into_iter();
    let one_minute = projections.next().unwrap().into_bars().1;
    let five_minute = projections.next().unwrap().into_bars().1;
    let session = timeout(WAKE, builder.start()).await.unwrap().unwrap();
    let mut cursor_watch = session.cache().watch_key(feed.cursor.key()).unwrap();
    let mut one_minute_watch = session.cache().watch_key(one_minute.status.key()).unwrap();
    let mut five_minute_watch = session.cache().watch_key(five_minute.status.key()).unwrap();
    wait_for_cursor(session.cache(), &mut cursor_watch, &feed, |cursor| {
        cursor.feed_seq == 0
    })
    .await;

    seek_to(&session, 301_000_000_000).await;
    let cursor = wait_for_cursor(session.cache(), &mut cursor_watch, &feed, |cursor| {
        cursor.ended
    })
    .await;
    let one_minute_status =
        wait_for_catch_up(session.cache(), &mut one_minute_watch, &one_minute, &cursor).await;
    let five_minute_status = wait_for_catch_up(
        session.cache(),
        &mut five_minute_watch,
        &five_minute,
        &cursor,
    )
    .await;

    assert_eq!(one_minute_status.processed_batches, cursor.batch_idx);
    assert_eq!(five_minute_status.processed_batches, cursor.batch_idx);
    assert_eq!(
        session.cache().read_array(&one_minute.bars).unwrap().len(),
        2
    );
    assert_eq!(
        session.cache().read_array(&five_minute.bars).unwrap().len(),
        1
    );
    assert!(session
        .cache()
        .read_value(&one_minute.live)
        .unwrap()
        .is_some());
    assert!(session
        .cache()
        .read_value(&five_minute.live)
        .unwrap()
        .is_some());
    let trade_prints = session
        .cache()
        .describe(&Key::new("projection.trade_prints.prints").unwrap())
        .unwrap();
    assert_eq!(
        trade_prints.owner,
        CellOwner::new("projection.trade_prints").unwrap()
    );
    assert!(!trade_prints.public_read);
    assert!(session
        .runtime()
        .component_status(&runtime::ComponentId::new("projection.trade_prints").unwrap())
        .await
        .is_ok());

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
    let err = builder
        .projections(
            &feed,
            &[
                ProjectionSpec::Bars(BarsParams {
                    interval_ns: 60_000_000_000,
                }),
                ProjectionSpec::Bars(BarsParams {
                    interval_ns: 60 * 1_000_000_000,
                }),
            ],
        )
        .unwrap_err();

    assert!(matches!(err, LedgerError::InvalidProjectionSpec { .. }));
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
        let bars = builder
            .projections(&feed, &[ProjectionSpec::Bars(params)])
            .unwrap()
            .pop()
            .unwrap()
            .into_bars()
            .1;
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
    cache: &cache::CacheReader,
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
    cache: &cache::CacheReader,
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
    cache: &cache::CacheReader,
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

fn generated_trades(count: usize) -> Vec<EsMboEvent> {
    (0..count)
        .map(|idx| {
            let sequence = (idx + 1) as u64;
            let side = if idx % 2 == 0 {
                Some(BookSide::Bid)
            } else {
                Some(BookSide::Ask)
            };
            trade(
                sequence,
                sequence,
                100 + (idx % 17) as i64,
                1 + (idx % 5) as u32,
                side,
            )
        })
        .collect()
}

fn reference_bars(events: &[EsMboEvent], interval_ns: u64) -> (Vec<Bar>, Option<Bar>) {
    let mut completed = Vec::new();
    let mut live: Option<Bar> = None;
    for print in events.iter().filter_map(canonical_trade_print) {
        let bucket = print.ts_event_ns - (print.ts_event_ns % interval_ns);
        match live.take() {
            None => live = Some(reference_new_bar(print, bucket)),
            Some(mut bar) if bucket == bar.interval_start_ns => {
                reference_fold_print(&mut bar, print);
                live = Some(bar);
            }
            Some(bar) if bucket > bar.interval_start_ns => {
                completed.push(bar);
                live = Some(reference_new_bar(print, bucket));
            }
            Some(mut bar) => {
                reference_fold_print(&mut bar, print);
                live = Some(bar);
            }
        }
    }
    (completed, live)
}

fn reference_new_bar(print: TradePrint, interval_start_ns: u64) -> Bar {
    let mut bar = Bar {
        interval_start_ns,
        open: print.price_ticks,
        high: print.price_ticks,
        low: print.price_ticks,
        close: print.price_ticks,
        volume: 0,
        buy_volume: 0,
        sell_volume: 0,
        trade_count: 0,
        first_ts_event_ns: print.ts_event_ns,
        last_ts_event_ns: print.ts_event_ns,
    };
    reference_fold_volume(&mut bar, print);
    bar
}

fn reference_fold_print(bar: &mut Bar, print: TradePrint) {
    bar.high = bar.high.max(print.price_ticks);
    bar.low = bar.low.min(print.price_ticks);
    bar.first_ts_event_ns = bar.first_ts_event_ns.min(print.ts_event_ns);
    reference_fold_volume(bar, print);
    if print.ts_event_ns >= bar.last_ts_event_ns {
        bar.close = print.price_ticks;
        bar.last_ts_event_ns = print.ts_event_ns;
    }
}

fn reference_fold_volume(bar: &mut Bar, print: TradePrint) {
    let size = u64::from(print.size);
    bar.volume += size;
    match print.aggressor {
        Some(BookSide::Bid) => bar.buy_volume += size,
        Some(BookSide::Ask) => bar.sell_volume += size,
        None => {}
    }
    bar.trade_count += 1;
}

async fn step_one_batch_at_a_time(running: &mut RunningBarsSession) -> EsReplayCursor {
    loop {
        let cursor = running
            .session
            .cache()
            .read_value(&running.feed.cursor)
            .unwrap()
            .unwrap();
        if cursor.ended {
            return cursor;
        }
        let start_batch_idx = cursor.batch_idx;
        running
            .seek_and_wait(cursor.next_ts_event_ns.unwrap(), |cursor| {
                cursor.batch_idx > start_batch_idx || cursor.ended
            })
            .await;
    }
}
