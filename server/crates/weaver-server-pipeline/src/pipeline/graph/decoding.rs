// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The time a job's decoders spend decoding: the wall clock while at least
// one of its engine runs or work loans is executing. A decoder waiting is
// not decoding, whether it waits for bytes still to come, for the job to
// hear what it said, for a slot, for memory or for a thread to run on; nor
// is a write the job makes straight from the posts. Two decoders running at
// once spend one second for each second of the clock, not two.
//
// Each second of it is shared out among the decoders running in it, one
// part each, so that the parts of every decoder add up to the clock: two
// decoders running at once each take half a second of every second. A
// decoder runs from its start to its end, whether it executes or waits in
// between, so the shares change only as decoders start and end: a wait and
// what follows it cost what they did before any share was kept. A run
// thrown away, to run again from the start, gives back its parts, and no
// second is given back twice. While the job is paused no decoder takes a
// part: that time is the pause's to give back.

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
    // How many decoders are running: started and not yet ended.
    running: usize,
    // The parts shared out so far: what one decoder running all along
    // would have taken. Counted up to the time spent at `marked`.
    parts: Duration,
    marked: Duration,
    // While the job is paused, no part is shared out.
    paused: bool,
    // The parts of runs thrown away.
    discarded: Duration,
}

impl Spent {
    // The time spent up to `now`.
    fn spent_at(&self, now: Instant) -> Duration {
        self.before
            + self
                .since
                .map_or(Duration::ZERO, |since| now.saturating_duration_since(since))
    }

    // The parts shared out up to `now`.
    fn parts_at(&self, now: Instant) -> Duration {
        if self.running == 0 || self.paused {
            return self.parts;
        }
        let spent = self.spent_at(now).saturating_sub(self.marked);
        self.parts
            + match self.running {
                1 => spent,
                running => spent / u32::try_from(running).unwrap_or(u32::MAX),
            }
    }

    // Counts the parts up to `now`, before who runs changes.
    fn mark(&mut self, now: Instant) -> Duration {
        self.parts = self.parts_at(now);
        self.marked = self.spent_at(now);
        self.parts
    }

    fn enter(&mut self, now: impl FnOnce() -> Instant) {
        self.executing += 1;
        self.entered += 1;
        if self.executing == 1 {
            self.since = Some(now());
        }
    }

    fn leave(&mut self, now: impl FnOnce() -> Instant) {
        self.executing = self.executing.saturating_sub(1);
        if self.executing == 0
            && let Some(since) = self.since.take()
        {
            self.before += now().saturating_duration_since(since);
        }
    }
}

impl DecodeClock {
    fn lock(&self) -> std::sync::MutexGuard<'_, Spent> {
        self.0.lock().unwrap_or_else(PoisonError::into_inner)
    }

    // The time spent decoding so far.
    pub(crate) fn spent(&self) -> Duration {
        let spent = self.lock();
        spent.before + spent.since.map_or(Duration::ZERO, |since| since.elapsed())
    }

    // Of the time spent, what runs thrown away took: never more than was
    // spent.
    pub(crate) fn discarded(&self) -> Duration {
        self.lock().discarded
    }

    // A run is thrown away, to run again from the start: the parts it took
    // are given back.
    pub(crate) fn discard(&self, parts: Duration) {
        let mut spent = self.lock();
        spent.discarded = spent.discarded.saturating_add(parts);
    }

    // The job paused, or went on: no part is shared out while it is paused.
    pub(crate) fn hold(&self, paused: bool) {
        let mut spent = self.lock();
        spent.mark(Instant::now());
        spent.paused = paused;
    }

    // Whether a decoder has executed, `after` times or more, and none is
    // executing now: each waits, or is done.
    #[cfg(test)]
    pub(crate) fn idle_after(&self, after: u64) -> bool {
        let spent = self.lock();
        spent.entered >= after && spent.executing == 0
    }

    // A decoder starts: it runs, and is counted executing until the handle
    // drops, but while it waits. One look at the clock serves both.
    pub(crate) fn decoding(&self) -> Decoding {
        let at = {
            let mut spent = self.lock();
            let now = Instant::now();
            let at = spent.mark(now);
            spent.running += 1;
            spent.enter(|| now);
            at
        };
        Decoding {
            clock: self.clone(),
            waits: Mutex::new(0),
            at,
        }
    }

    // A decoder executes again after a wait, as before any share was kept.
    fn enter(&self) {
        self.lock().enter(Instant::now);
    }

    // A decoder waits, as before any share was kept.
    fn leave(&self) {
        self.lock().leave(Instant::now);
    }

    // A decoder ends: its parts up to now, the last it takes. One look at
    // the clock serves both.
    fn end(&self, at: Duration, executing: bool) -> Duration {
        let mut spent = self.lock();
        let now = Instant::now();
        let parts = spent.mark(now);
        spent.running = spent.running.saturating_sub(1);
        if executing {
            spent.leave(|| now);
        }
        parts.saturating_sub(at)
    }

    fn parts(&self) -> Duration {
        self.lock().parts_at(Instant::now())
    }
}

// One decoder running. Its own threads may each wait at once: it is
// waiting while any of them is, and executing again once none is.
#[derive(Debug)]
pub(crate) struct Decoding {
    clock: DecodeClock,
    waits: Mutex<usize>,
    // The parts the clock had shared out when the decoder started.
    at: Duration,
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

    // The parts of the clock this decoder took so far.
    pub(crate) fn spent(&self) -> Duration {
        self.clock.parts().saturating_sub(self.at)
    }
}

impl Drop for Decoding {
    fn drop(&mut self) {
        let waits = *self.waits.get_mut().unwrap_or_else(PoisonError::into_inner);
        self.clock.end(self.at, waits == 0);
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

    // Each decoder takes its part of every second spent while it runs,
    // executing or waiting: the whole second alone, half of it beside
    // another. The parts add up to the clock.
    #[tokio::test(start_paused = true)]
    async fn each_decoder_takes_its_part_of_the_seconds_it_runs_in() {
        let clock = DecodeClock::default();
        let first = clock.decoding();
        tokio::time::advance(SECOND * 2).await;
        let second = clock.decoding();
        {
            let _waiting = first.wait();
            tokio::time::advance(SECOND * 4).await;
        }
        tokio::time::advance(SECOND).await;
        assert_eq!(first.spent(), SECOND * 4 + SECOND / 2);
        assert_eq!(second.spent(), SECOND * 2 + SECOND / 2);
        assert_eq!(clock.spent(), SECOND * 7);
        assert_eq!(first.spent() + second.spent(), clock.spent());
    }

    // Eight decoders at once for ten seconds take a part of an eighth each:
    // the parts of all of them are the ten seconds, not eighty.
    #[tokio::test(start_paused = true)]
    async fn decoders_together_take_no_more_than_the_clock() {
        let clock = DecodeClock::default();
        let decoders: Vec<_> = (0..8).map(|_| clock.decoding()).collect();
        tokio::time::advance(SECOND * 10).await;
        for decoding in &decoders {
            assert_eq!(decoding.spent(), SECOND * 10 / 8);
        }
        let parts: Duration = decoders.iter().map(super::Decoding::spent).sum();
        assert_eq!(parts, clock.spent());
    }

    // While the job is paused the clock runs, but no decoder takes a part:
    // that time is the pause's.
    #[tokio::test(start_paused = true)]
    async fn a_decoder_takes_no_part_while_the_job_is_paused() {
        let clock = DecodeClock::default();
        let decoding = clock.decoding();
        tokio::time::advance(SECOND * 2).await;
        clock.hold(true);
        tokio::time::advance(SECOND * 5).await;
        clock.hold(false);
        tokio::time::advance(SECOND).await;
        assert_eq!(clock.spent(), SECOND * 8);
        assert_eq!(decoding.spent(), SECOND * 3);
    }
}
