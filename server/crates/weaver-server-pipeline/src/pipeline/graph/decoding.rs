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
pub struct DecodeClock(Arc<Mutex<Spent>>);

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
    pub fn spent(&self) -> Duration {
        let spent = self.lock();
        spent.before + spent.since.map_or(Duration::ZERO, |since| since.elapsed())
    }

    // Of the time spent, what runs thrown away took: never more than was
    // spent.
    pub fn discarded(&self) -> Duration {
        self.lock().discarded
    }

    // A run is thrown away, to run again from the start: the parts it took
    // are given back.
    pub(crate) fn discard(&self, parts: Duration) {
        let mut spent = self.lock();
        spent.discarded = spent.discarded.saturating_add(parts);
    }

    // The job paused, or went on: no part is shared out while it is paused.
    pub fn hold(&self, paused: bool) {
        let mut spent = self.lock();
        spent.mark(Instant::now());
        spent.paused = paused;
    }

    // Whether a decoder has executed, `after` times or more, and none is
    // executing now: each waits, or is done.
    #[cfg(any(test, feature = "pipeline-test-fixtures"))]
    pub fn idle_after(&self, after: u64) -> bool {
        let spent = self.lock();
        spent.entered >= after && spent.executing == 0
    }

    // A decoder starts: it runs, and is counted executing until the handle
    // drops, but while it waits. One look at the clock serves both.
    pub fn decoding(&self) -> Decoding {
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
pub struct Decoding {
    clock: DecodeClock,
    waits: Mutex<usize>,
    // The parts the clock had shared out when the decoder started.
    at: Duration,
}

impl Decoding {
    // The decoder waits, and is not decoding, until the handle drops.
    pub fn wait(&self) -> Waiting<'_> {
        let mut waits = self.waits.lock().unwrap_or_else(PoisonError::into_inner);
        *waits += 1;
        if *waits == 1 {
            self.clock.leave();
        }
        Waiting(self)
    }

    // The parts of the clock this decoder took so far.
    pub fn spent(&self) -> Duration {
        self.clock.parts().saturating_sub(self.at)
    }
}

impl Drop for Decoding {
    fn drop(&mut self) {
        let waits = *self.waits.get_mut().unwrap_or_else(PoisonError::into_inner);
        self.clock.end(self.at, waits == 0);
    }
}

pub struct Waiting<'a>(&'a Decoding);

impl Drop for Waiting<'_> {
    fn drop(&mut self) {
        let mut waits = self.0.waits.lock().unwrap_or_else(PoisonError::into_inner);
        *waits -= 1;
        if *waits == 0 {
            self.0.clock.enter();
        }
    }
}
