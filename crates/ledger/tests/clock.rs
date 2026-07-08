use std::time::{Duration, Instant};

use ledger::clock::{ClockMode, ClockState};

#[test]
fn with_speed_rejects_zero_negative_nan_and_infinite_speeds() {
    let clock = ClockState::initial();

    assert!(clock.with_speed(0.0).is_err());
    assert!(clock.with_speed(-1.0).is_err());
    assert!(clock.with_speed(f64::NAN).is_err());
    assert!(clock.with_speed(f64::INFINITY).is_err());
}

#[test]
fn play_pause_with_speed_and_seek_reanchor_and_increment_revision() {
    let initial = ClockState::initial();
    let before_play = Instant::now();

    let playing = initial.play();
    assert_eq!(playing.mode, ClockMode::Running);
    assert_eq!(playing.revision, initial.revision + 1);
    assert!(playing.anchor_wall >= before_play);

    let paused = playing.pause();
    assert_eq!(paused.mode, ClockMode::Paused);
    assert_eq!(paused.revision, playing.revision + 1);

    let faster = paused.with_speed(2.5).unwrap();
    assert_eq!(faster.speed, 2.5);
    assert_eq!(faster.revision, paused.revision + 1);

    let sought = faster.seek_to(42);
    assert_eq!(sought.anchor_session_ns, 42);
    assert_eq!(sought.revision, faster.revision + 1);
}

#[test]
fn seek_to_accepts_backward_targets() {
    let clock = ClockState::initial().seek_to(1_000);

    let sought = clock.seek_to(100);

    assert_eq!(sought.anchor_session_ns, 100);
    assert_eq!(sought.revision, clock.revision + 1);
}

#[test]
fn now_ns_is_anchor_math() {
    let paused = ClockState {
        mode: ClockMode::Paused,
        speed: 10.0,
        anchor_session_ns: 123,
        anchor_wall: Instant::now() - Duration::from_millis(50),
        revision: 0,
    };
    assert_eq!(paused.now_ns(), 123);

    let running = ClockState {
        mode: ClockMode::Running,
        speed: 2.0,
        anchor_session_ns: 1_000,
        anchor_wall: Instant::now() - Duration::from_millis(20),
        revision: 0,
    };
    let now = running.now_ns();

    assert!(now >= 40_000_000);
    assert!(now < 200_000_000);
}

#[test]
fn wall_deadline_returns_none_when_paused_and_exact_instant_when_running() {
    let anchor_wall = Instant::now();
    let paused = ClockState {
        mode: ClockMode::Paused,
        speed: 1.0,
        anchor_session_ns: 100,
        anchor_wall,
        revision: 0,
    };
    assert_eq!(paused.wall_deadline(200), None);

    let running = ClockState {
        mode: ClockMode::Running,
        speed: 2.0,
        anchor_session_ns: 100,
        anchor_wall,
        revision: 0,
    };
    let deadline = running.wall_deadline(50_000_100).unwrap();
    let delta = deadline.duration_since(anchor_wall);

    assert!(delta >= Duration::from_millis(24));
    assert!(delta <= Duration::from_millis(26));
}
