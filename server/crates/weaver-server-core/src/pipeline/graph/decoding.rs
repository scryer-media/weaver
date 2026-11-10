// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The time a job's decoders spend decoding: the wall clock while at least
// one of its engine runs or work loans is executing. A decoder waiting is
// not decoding, whether it waits for bytes still to come, for the job to
// hear what it said, for a slot, for memory or for a thread to run on; nor
// is a write the job makes straight from the posts. Two decoders running at
// once spend one second for each second of the clock, not two.

use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use tokio::time::Instant;

#[derive(Clone, Debug, Default)]
pub(crate) struct DecodeClock(Arc<Mutex<Spent>>);

#[derive(Debug, Default)]
struct Spent {
    // How many decoders are executing now, and since when one has been.
    executing: usize,
    since: Option<Instant>,
    // The time spent before `since`.
    before: Duration,
    // How many times a decoder began executing, or went on after a wait.
    entered: u64,
}

impl DecodeClock {
    // The time spent decoding so far.
    pub(crate) fn spent(&self) -> Duration {
        let spent = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        spent.before + spent.since.map_or(Duration::ZERO, |since| since.elapsed())
    }

    // Whether a decoder has executed, `after` times or more, and none is
    // executing now: each waits, or is done.
    #[cfg(test)]
    pub(crate) fn idle_after(&self, after: u64) -> bool {
        let spent = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        spent.entered >= after && spent.executing == 0
    }

    // A decoder starts executing: it is counted until the handle drops,
    // but while it waits.
    pub(crate) fn decoding(&self) -> Decoding {
        self.enter();
        Decoding {
            clock: self.clone(),
            waits: Mutex::new(0),
        }
    }

    fn enter(&self) {
        let mut spent = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        spent.executing += 1;
        spent.entered += 1;
        if spent.executing == 1 {
            spent.since = Some(Instant::now());
        }
    }

    fn leave(&self) {
        let mut spent = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        spent.executing = spent.executing.saturating_sub(1);
        if spent.executing == 0
            && let Some(since) = spent.since.take()
        {
            spent.before += since.elapsed();
        }
    }
}

// One decoder executing. Its own threads may each wait at once: it is
// waiting while any of them is, and executing again once none is.
#[derive(Debug)]
pub(crate) struct Decoding {
    clock: DecodeClock,
    waits: Mutex<usize>,
}

impl Decoding {
    // The decoder waits, and is not decoding, until the handle drops.
    pub(crate) fn wait(&self) -> Waiting<'_> {
        let mut waits = self.waits.lock().unwrap_or_else(PoisonError::into_inner);
        *waits += 1;
        if *waits == 1 {
            self.clock.leave();
        }
        Waiting(self)
    }
}

impl Drop for Decoding {
    fn drop(&mut self) {
        let waits = *self.waits.get_mut().unwrap_or_else(PoisonError::into_inner);
        if waits == 0 {
            self.clock.leave();
        }
    }
}

pub(crate) struct Waiting<'a>(&'a Decoding);

impl Drop for Waiting<'_> {
    fn drop(&mut self) {
        let mut waits = self.0.waits.lock().unwrap_or_else(PoisonError::into_inner);
        *waits -= 1;
        if *waits == 0 {
            self.0.clock.enter();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::DecodeClock;
    use std::time::Duration;

    const SECOND: Duration = Duration::from_secs(1);

    // Nothing is spent before a decoder executes, while it waits, or after
    // it is done.
    #[tokio::test(start_paused = true)]
    async fn only_the_time_a_decoder_executes_is_spent() {
        let clock = DecodeClock::default();
        tokio::time::advance(SECOND * 10).await;
        assert_eq!(clock.spent(), Duration::ZERO);

        let decoding = clock.decoding();
        tokio::time::advance(SECOND * 2).await;
        {
            let _waiting = decoding.wait();
            let _again = decoding.wait();
            tokio::time::advance(SECOND * 100).await;
        }
        assert_eq!(clock.spent(), SECOND * 2);
        tokio::time::advance(SECOND * 3).await;
        assert_eq!(clock.spent(), SECOND * 5);
        drop(decoding);
        tokio::time::advance(SECOND * 7).await;
        assert_eq!(clock.spent(), SECOND * 5);
    }

    // Two decoders at once spend the clock once, and a wait of one leaves
    // the other's time running.
    #[tokio::test(start_paused = true)]
    async fn decoders_running_together_spend_the_clock_once() {
        let clock = DecodeClock::default();
        let first = clock.decoding();
        let second = clock.decoding();
        tokio::time::advance(SECOND * 3).await;
        assert_eq!(clock.spent(), SECOND * 3);
        {
            let _waiting = first.wait();
            tokio::time::advance(SECOND * 2).await;
        }
        assert_eq!(clock.spent(), SECOND * 5);
        drop(first);
        drop(second);
        tokio::time::advance(SECOND * 2).await;
        assert_eq!(clock.spent(), SECOND * 5);
    }
}
