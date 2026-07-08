use crate::LedgerError;
use serde::{Deserialize, Serialize};
use std::time::{Duration, Instant};

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum ClockMode {
    Paused,
    Running,
}

#[derive(Debug, Clone, PartialEq)]
pub struct ClockState {
    pub mode: ClockMode,
    pub speed: f64,
    pub anchor_session_ns: u64,
    pub anchor_wall: Instant,
    pub revision: u64,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ClockSnapshot {
    pub mode: ClockMode,
    pub speed: f64,
    pub session_now_ns: u64,
    pub revision: u64,
}

impl ClockState {
    pub fn initial() -> Self {
        Self {
            mode: ClockMode::Paused,
            speed: 1.0,
            anchor_session_ns: 0,
            anchor_wall: Instant::now(),
            revision: 0,
        }
    }

    pub fn now_ns(&self) -> u64 {
        match self.mode {
            ClockMode::Paused => self.anchor_session_ns,
            ClockMode::Running => {
                let elapsed = Instant::now()
                    .checked_duration_since(self.anchor_wall)
                    .unwrap_or_default();
                let advanced = duration_nanos_at_speed(elapsed, self.speed);
                self.anchor_session_ns.saturating_add(advanced)
            }
        }
    }

    pub fn snapshot(&self) -> ClockSnapshot {
        ClockSnapshot {
            mode: self.mode,
            speed: self.speed,
            session_now_ns: self.now_ns(),
            revision: self.revision,
        }
    }

    pub fn wall_deadline(&self, target_ns: u64) -> Option<Instant> {
        if self.mode != ClockMode::Running {
            return None;
        }

        if target_ns <= self.anchor_session_ns {
            return Some(self.anchor_wall);
        }

        let delta_ns = (target_ns - self.anchor_session_ns) as f64;
        let seconds = delta_ns / self.speed / 1_000_000_000.0;
        self.anchor_wall
            .checked_add(Duration::from_secs_f64(seconds))
    }

    pub fn play(&self) -> ClockState {
        self.transition(ClockMode::Running, self.speed, self.now_ns())
    }

    pub fn pause(&self) -> ClockState {
        self.transition(ClockMode::Paused, self.speed, self.now_ns())
    }

    pub fn with_speed(&self, speed: f64) -> Result<ClockState, LedgerError> {
        if !speed.is_finite() || speed <= 0.0 {
            return Err(LedgerError::InvalidClockSpeed(speed));
        }
        Ok(self.transition(self.mode, speed, self.now_ns()))
    }

    pub fn seek_to(&self, session_ns: u64) -> ClockState {
        self.transition(self.mode, self.speed, session_ns)
    }

    fn transition(&self, mode: ClockMode, speed: f64, anchor_session_ns: u64) -> ClockState {
        ClockState {
            mode,
            speed,
            anchor_session_ns,
            anchor_wall: Instant::now(),
            revision: self.revision.saturating_add(1),
        }
    }
}

fn duration_nanos_at_speed(duration: Duration, speed: f64) -> u64 {
    let nanos = duration.as_secs_f64() * 1_000_000_000.0 * speed;
    if nanos >= u64::MAX as f64 {
        u64::MAX
    } else {
        nanos as u64
    }
}
