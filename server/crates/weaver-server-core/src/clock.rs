// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

//! The one clock the server's owners ask the time of, so a test can set it.
//!
//! Two readings, because they answer different questions: [`Clock::now`]
//! is monotonic, for spans and deadlines, and never steps back;
//! [`Clock::unix_ms`] is the wall clock, for times that are stored or
//! shown, and steps whenever the machine's clock is set.
//!
//! Code that only waits on `tokio::time` needs none of this: a paused
//! runtime already drives it. Code that reads the time per packet, per
//! article or per write call keeps its plain `Instant::now()`, since a
//! call through this trait costs more than the read.

use std::time::Instant;

/// Where an owner takes the time from.
pub trait Clock: Send + Sync {
    /// Monotonic time, for spans and deadlines.
    fn now(&self) -> Instant;

    /// Wall-clock milliseconds since the Unix epoch, for stored times.
    fn unix_ms(&self) -> i64;

    /// Whether time passes by itself. A clock a test moves by hand does
    /// not: whoever waits on it waits to be woken after the test moves it,
    /// not for a span to pass.
    fn runs(&self) -> bool {
        true
    }
}

/// The machine's own clocks.
pub struct SystemClock;

impl Clock for SystemClock {
    fn now(&self) -> Instant {
        Instant::now()
    }

    fn unix_ms(&self) -> i64 {
        chrono::Utc::now().timestamp_millis()
    }
}

#[cfg(test)]
pub(crate) use test_clocks::{ManualClock, RuntimeClock};

#[cfg(test)]
mod test_clocks {
    use std::sync::{Condvar, Mutex};
    use std::time::{Duration, Instant};

    use super::Clock;

    /// A clock a test moves by hand, which counts the looks taken at it.
    /// Both readings move together unless the test sets the wall clock on
    /// its own, as a machine's clock may be set.
    pub(crate) struct ManualClock {
        origin: Instant,
        state: Mutex<State>,
        looked: Condvar,
    }

    struct State {
        at: Duration,
        unix_ms: i64,
        // How far each look moves the clock on, after it is read.
        step: Duration,
        looks: u32,
    }

    impl ManualClock {
        /// Standing at `unix_ms` on the wall clock.
        pub(crate) fn at_unix_ms(unix_ms: i64) -> Self {
            Self::with(unix_ms, Duration::ZERO)
        }

        /// A clock `step` later at each look.
        pub(crate) fn stepping(step: Duration) -> Self {
            Self::with(0, step)
        }

        fn with(unix_ms: i64, step: Duration) -> Self {
            Self {
                origin: Instant::now(),
                state: Mutex::new(State {
                    at: Duration::ZERO,
                    unix_ms,
                    step,
                    looks: 0,
                }),
                looked: Condvar::new(),
            }
        }

        /// What [`Clock::now`] reads once the clock stands `at` from where
        /// it began. Not a look.
        pub(crate) fn instant(&self, at: Duration) -> Instant {
            self.origin + at
        }

        /// Move both readings on by `by`.
        pub(crate) fn advance(&self, by: Duration) {
            let mut state = self.state.lock().unwrap();
            state.at += by;
            state.unix_ms += by.as_millis() as i64;
        }

        /// Move both readings to `at` from where the clock began, forward
        /// or back.
        pub(crate) fn move_to(&self, at: Duration) {
            let mut state = self.state.lock().unwrap();
            let delta = at.as_millis() as i64 - state.at.as_millis() as i64;
            state.at = at;
            state.unix_ms += delta;
        }

        /// Set the wall clock alone, as a machine's clock is set.
        pub(crate) fn set_unix_ms(&self, unix_ms: i64) {
            self.state.lock().unwrap().unix_ms = unix_ms;
        }

        /// Step the wall clock alone by `ms`, forward or back.
        pub(crate) fn shift_unix_ms(&self, ms: i64) {
            self.state.lock().unwrap().unix_ms += ms;
        }

        /// Wait until the clock has been looked at `looks` times in all.
        pub(crate) fn wait_looks(&self, looks: u32) {
            let mut state = self.state.lock().unwrap();
            while state.looks < looks {
                state = self.looked.wait(state).unwrap();
            }
        }

        // Count a look and step on past it.
        fn look<T>(&self, read: impl FnOnce(&State) -> T) -> T {
            let mut state = self.state.lock().unwrap();
            let value = read(&state);
            state.looks += 1;
            let step = state.step;
            state.at += step;
            state.unix_ms += step.as_millis() as i64;
            self.looked.notify_all();
            value
        }
    }

    impl Default for ManualClock {
        fn default() -> Self {
            Self::with(0, Duration::ZERO)
        }
    }

    impl Clock for ManualClock {
        fn now(&self) -> Instant {
            self.look(|state| self.origin + state.at)
        }

        fn unix_ms(&self) -> i64 {
            self.look(|state| state.unix_ms)
        }

        fn runs(&self) -> bool {
            false
        }
    }

    /// The runtime's clock, which a paused runtime moves only as timers
    /// fire. The wall clock reads `unix_ms` at the moment it was made.
    pub(crate) struct RuntimeClock {
        start: tokio::time::Instant,
        unix_ms: i64,
    }

    impl RuntimeClock {
        pub(crate) fn at_unix_ms(unix_ms: i64) -> Self {
            Self {
                start: tokio::time::Instant::now(),
                unix_ms,
            }
        }
    }

    impl Clock for RuntimeClock {
        fn now(&self) -> Instant {
            tokio::time::Instant::now().into_std()
        }

        fn unix_ms(&self) -> i64 {
            self.unix_ms + self.start.elapsed().as_millis() as i64
        }
    }
}
