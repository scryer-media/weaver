// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use std::time::Duration;

use tokio::sync::watch;

// Where a run's waits take their time from: the system's, or a clock a
// test moves by hand.
#[derive(Clone, Default)]
pub struct RunClock {
    // How far a hand-moved clock has been moved; none on the system's.
    moved: Option<watch::Receiver<Duration>>,
}

impl RunClock {
    // The moment `after` from now.
    pub(crate) fn deadline(&self, after: Duration) -> Deadline {
        match &self.moved {
            Some(moved) => Deadline::Moved {
                at: *moved.borrow() + after,
                moved: moved.clone(),
            },
            None if crate::e2e_clock::override_enabled() => Deadline::E2e(
                crate::e2e_clock::utc_now()
                    + chrono::TimeDelta::from_std(after).expect("bounded speed-test wait"),
            ),
            None => Deadline::Monotonic(tokio::time::Instant::now() + after),
        }
    }
}

// A clock that moves only when it is told to, so no wait on it ends on its
// own.
#[cfg(any(test, feature = "test-support"))]
pub struct HandClock(watch::Sender<Duration>);

#[cfg(any(test, feature = "test-support"))]
impl HandClock {
    pub fn new() -> Self {
        Self(watch::channel(Duration::ZERO).0)
    }

    pub fn clock(&self) -> RunClock {
        RunClock {
            moved: Some(self.0.subscribe()),
        }
    }

    pub fn advance(&self, by: Duration) {
        self.0.send_modify(|moved| *moved += by);
    }
}

#[cfg(any(test, feature = "test-support"))]
impl Default for HandClock {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Clone)]
pub(crate) enum Deadline {
    Monotonic(tokio::time::Instant),
    E2e(chrono::DateTime<chrono::Utc>),
    Moved {
        at: Duration,
        moved: watch::Receiver<Duration>,
    },
}

impl Deadline {
    pub(crate) fn expired(&self) -> bool {
        match self {
            Self::Monotonic(at) => *at <= tokio::time::Instant::now(),
            Self::E2e(at) => *at <= crate::e2e_clock::utc_now(),
            Self::Moved { at, moved } => *moved.borrow() >= *at,
        }
    }

    pub(crate) async fn wait(&self) {
        match self {
            Self::Monotonic(at) => tokio::time::sleep_until(*at).await,
            Self::E2e(_) => {
                while !self.expired() {
                    tokio::time::sleep(crate::e2e_clock::schedule_poll_interval()).await;
                }
            }
            Self::Moved { at, moved } => {
                let mut moved = moved.clone();
                // A clock no one can move again never gets there.
                if moved.wait_for(|moved| moved >= at).await.is_err() {
                    std::future::pending::<()>().await;
                }
            }
        }
    }
}

// When a run's work must end: the operator cancelled, the run decided it has
// measured enough, or the measurement reached its maximum duration.
#[derive(Clone)]
pub(crate) struct Stop {
    cancel: watch::Receiver<bool>,
    halt: Option<watch::Receiver<bool>>,
    deadline: Option<Deadline>,
}

impl Stop {
    pub(crate) fn new(cancel: watch::Receiver<bool>) -> Self {
        Self {
            cancel,
            halt: None,
            deadline: None,
        }
    }

    // Also stop at `deadline`.
    pub(crate) fn with_deadline(&self, deadline: Deadline) -> Self {
        Self {
            deadline: Some(deadline),
            ..self.clone()
        }
    }

    // Also stop once `halt` is set.
    pub(crate) fn with_halt(&self, halt: watch::Receiver<bool>) -> Self {
        Self {
            halt: Some(halt),
            ..self.clone()
        }
    }

    pub(crate) fn cancelled(&self) -> bool {
        *self.cancel.borrow()
    }

    pub(crate) fn is_stopped(&self) -> bool {
        self.cancelled()
            || self.halt.as_ref().is_some_and(|halt| *halt.borrow())
            || self.deadline.as_ref().is_some_and(Deadline::expired)
    }

    pub(crate) async fn stopped(&mut self) {
        let Self {
            cancel,
            halt,
            deadline,
        } = self;
        let deadline = &*deadline;
        let expired = async move {
            match deadline {
                Some(deadline) => deadline.wait().await,
                None => std::future::pending().await,
            }
        };
        let halted = async move {
            match halt {
                Some(halt) => {
                    let _ = halt.wait_for(|halted| *halted).await;
                }
                None => std::future::pending().await,
            }
        };
        tokio::select! {
            // A dropped sender means the service is gone; stop as well.
            _ = cancel.wait_for(|cancelled| *cancelled) => {}
            _ = halted => {}
            _ = expired => {}
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test(start_paused = true)]
    async fn duration_deadline_uses_the_monotonic_clock() {
        let (_cancel, receiver) = watch::channel(false);
        let mut stop =
            Stop::new(receiver).with_deadline(RunClock::default().deadline(Duration::from_secs(10)));
        assert!(!stop.is_stopped());
        tokio::time::advance(Duration::from_secs(9)).await;
        assert!(!stop.is_stopped());
        tokio::time::advance(Duration::from_secs(1)).await;
        stop.stopped().await;
        assert!(stop.is_stopped());
    }

    // A deadline on a hand-moved clock passes only as the clock is moved,
    // however long the run takes.
    #[tokio::test]
    async fn a_hand_moved_deadline_passes_only_as_the_clock_moves() {
        let hand = HandClock::new();
        let (_cancel, receiver) = watch::channel(false);
        let mut stop = Stop::new(receiver).with_deadline(hand.clock().deadline(Duration::from_secs(10)));
        hand.advance(Duration::from_secs(9));
        assert!(!stop.is_stopped());
        let waiting = tokio::spawn(async move {
            stop.stopped().await;
            stop
        });
        hand.advance(Duration::from_secs(1));
        assert!(waiting.await.unwrap().is_stopped());
    }

    #[tokio::test]
    async fn an_article_boundary_hold_can_be_cancelled_before_its_deadline() {
        let (cancel, receiver) = watch::channel(false);
        let mut stop =
            Stop::new(receiver).with_deadline(RunClock::default().deadline(Duration::from_secs(600)));
        cancel.send_replace(true);
        stop.stopped().await;
        assert!(stop.cancelled());
    }
}
