// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The live write rate of each volume: what the volume takes while a write
// call is inside it, in bytes per second.
//
// A volume's writer adds to two counters, the bytes it wrote and the
// nanoseconds it spent inside the write calls. Every
// [`SAMPLE_INTERVAL`] one thread reads both for each volume and keeps the
// readings of the last [`WINDOW`]; the rate is the bytes over the window
// divided by the seconds spent writing them. Time the writer sat idle is not
// in it, so the figure is what the disk takes, not what the download brought.
//
// Zero means not measured. A volume's first reading is only where its
// counters stood when it was first seen; no rate comes from it. A window
// with no writes leaves the last rate in place. Nothing here writes, flushes
// or looks at a disk: a pass reads atomics, and one that finds no new bytes
// stores nothing.
//
// The thread does not tick while nothing is written. After a pass in which
// no volume's counters moved it waits to be rung ([`RateSignal::wake`]) and
// reads nothing until then. It ends when the disk owner stops.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Condvar, Mutex, MutexGuard};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

use tracing::warn;

use super::volume::Volume;
use super::{StorageCapacity, StorageKind, lock_paths};
use crate::operations::instrumentation::VolumeSnapshot;

// How often the counters are read while they move.
pub(crate) const SAMPLE_INTERVAL: Duration = Duration::from_secs(2);
// How far back the rate looks.
pub(crate) const WINDOW: Duration = Duration::from_secs(30);

const SAMPLER_THREAD: &str = "weaver-volume-rate";

// A volume's two write counters, read together.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct WriteReading {
    pub(crate) written_bytes: u64,
    pub(crate) write_nanos: u64,
}

impl WriteReading {
    // The nanoseconds are read first, and the bytes after them. The writer
    // adds a call's bytes first and stores its nanoseconds last, with
    // `Release` (see [`Volume`]), so the bytes read here are at least the
    // ones the nanoseconds were spent on: a reading may hold the bytes of a
    // call still in flight, never the time of a call without its bytes.
    fn of(volume: &Volume) -> Self {
        let write_nanos = volume.write_nanos.load(Ordering::Acquire);
        let written_bytes = volume.written_bytes.load(Ordering::Relaxed);
        Self {
            written_bytes,
            write_nanos,
        }
    }
}

// The readings of one volume over the last [`WINDOW`], oldest first. Only a
// reading that differs from the one before it is kept, so an idle volume
// holds its last reading and nothing more; the oldest kept is the latest one
// from at or before the window's start, which is what the counters stood at
// when the window began.
#[derive(Debug, Default)]
pub(crate) struct RateWindow {
    readings: VecDeque<(Duration, WriteReading)>,
}

impl RateWindow {
    // Take the reading made at `at`, a time since the sampler began, and
    // answer the rate over the window ending there. `None` when the reading
    // is the window's first, brought no new bytes, or no time inside a write
    // call to divide by.
    pub(crate) fn observe(&mut self, at: Duration, reading: WriteReading) -> Option<u64> {
        match self.readings.back() {
            Some((_, last)) if *last == reading => return None,
            // The counters only rise, so there is a stretch to measure.
            Some((_, last))
                if reading.written_bytes >= last.written_bytes
                    && reading.write_nanos >= last.write_nanos => {}
            // The first reading says where the counters stand, not how fast
            // they got there: what a volume wrote before it was seen, or
            // before it was dropped and listed again, was not written in this
            // window. Counters that fell are another volume's and start over
            // the same way.
            _ => {
                self.readings.clear();
                self.readings.push_back((at, reading));
                return None;
            }
        }
        let start = at.saturating_sub(WINDOW);
        while self.readings.len() >= 2 && self.readings[1].0 <= start {
            self.readings.pop_front();
        }
        self.readings.push_back((at, reading));
        let (_, base) = self.readings.front()?;
        let bytes = reading.written_bytes - base.written_bytes;
        let nanos = reading.write_nanos - base.write_nanos;
        if bytes == 0 || nanos == 0 {
            return None;
        }
        let rate = u128::from(bytes) * 1_000_000_000 / u128::from(nanos);
        // Zero is kept for "not measured": the slowest rate told is one.
        Some(u64::try_from(rate).unwrap_or(u64::MAX).max(1))
    }

    // The latest reading taken.
    fn latest(&self) -> Option<WriteReading> {
        self.readings.back().map(|(_, reading)| *reading)
    }
}

// The window of every volume the sampler has seen.
#[derive(Default)]
pub(crate) struct VolumeRates {
    // By the volume's address; holding the volume keeps the address its own.
    windows: HashMap<usize, (Arc<Volume>, RateWindow)>,
}

impl VolumeRates {
    // One pass at `at`, a time since the sampler began: read each volume's
    // counters and store the rate of each one that wrote since the last
    // pass. A volume no longer listed is forgotten. Says whether any
    // volume's counters moved: whether there is anything to come back for.
    pub(crate) fn pass(&mut self, volumes: &[Arc<Volume>], at: Duration) -> bool {
        let mut moved = false;
        for volume in volumes {
            let (_, window) = self
                .windows
                .entry(Arc::as_ptr(volume) as usize)
                .or_insert_with(|| (Arc::clone(volume), RateWindow::default()));
            let reading = WriteReading::of(volume);
            // A volume first seen with nothing written has not moved.
            moved |= window.latest().unwrap_or_default() != reading;
            if let Some(rate) = window.observe(at, reading) {
                volume.write_rate.store(rate, Ordering::Relaxed);
            }
        }
        if self.windows.len() > volumes.len() {
            let listed: HashSet<usize> = volumes
                .iter()
                .map(|volume| Arc::as_ptr(volume) as usize)
                .collect();
            self.windows.retain(|address, _| listed.contains(address));
        }
        moved
    }

    // Whether any volume seen has counters past its latest reading: a write
    // landed since the pass.
    fn behind(&self) -> bool {
        self.windows
            .values()
            .any(|(volume, window)| window.latest() != Some(WriteReading::of(volume)))
    }
}

#[derive(Debug, Default)]
struct SignalState {
    // The disk owner stopped: the sampler ends.
    stopped: bool,
    // The sampler waits for a write, and one rang.
    idle: bool,
    rung: bool,
    // Passes made, so a wait can tell one pass from the next.
    passes: u64,
}

// What the sampler waits on between passes, and what wakes it.
//
// While counters move it waits one interval. After a pass in which none
// moved it waits here until a writer rings [`Self::wake`] or the owner
// stops, so an idle host runs no timer and reads nothing.
#[derive(Debug, Default)]
pub(crate) struct RateSignal {
    // Set only while the sampler waits for a write: what `wake` reads.
    idle: AtomicBool,
    state: Mutex<SignalState>,
    wake: Condvar,
}

impl RateSignal {
    fn lock(&self) -> MutexGuard<'_, SignalState> {
        self.state
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    // Tell the sampler a volume was written to. Rung by a volume's writer
    // after a write: while the sampler is sampling this is one relaxed load
    // and nothing else, and only the first ring after an idle stretch takes
    // the lock.
    //
    // The load is relaxed, so a write racing the sampler's last look before
    // it waits can go unrung. The sampler looks at the counters once more
    // after raising the flag, and the writer's next write rings; what is
    // lost at worst is the rate of a volume's very last write.
    pub(crate) fn wake(&self) {
        if !self.idle.load(Ordering::Relaxed) {
            return;
        }
        let mut state = self.lock();
        if state.idle {
            state.rung = true;
            self.idle.store(false, Ordering::Relaxed);
            drop(state);
            self.wake.notify_all();
        }
    }

    // End the sampler, wherever it waits.
    pub(super) fn stop(&self) {
        self.lock().stopped = true;
        self.idle.store(false, Ordering::Relaxed);
        self.wake.notify_all();
    }

    // Wait one interval between passes. False when the owner stopped.
    fn pause(&self, interval: Duration) -> bool {
        let state = self.lock();
        let state = self
            .wake
            .wait_timeout_while(state, interval, |state| !state.stopped)
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .0;
        !state.stopped
    }

    // A pass is over. When no counters `moved` in it, and none is `behind`
    // once writers can see the sampler is idle, wait for a ring. False when
    // the owner stopped.
    fn passed(&self, moved: bool, behind: impl FnOnce() -> bool) -> bool {
        let mut state = self.lock();
        state.passes += 1;
        if !moved && !state.stopped {
            state.idle = true;
            state.rung = false;
            self.idle.store(true, Ordering::SeqCst);
            if !behind() {
                // Whoever waits for the sampler to go idle sees it now.
                self.wake.notify_all();
                state = self
                    .wake
                    .wait_while(state, |state| !state.stopped && !state.rung)
                    .unwrap_or_else(|poisoned| poisoned.into_inner());
            }
            state.idle = false;
            state.rung = false;
            self.idle.store(false, Ordering::Relaxed);
        }
        let stopped = state.stopped;
        drop(state);
        self.wake.notify_all();
        !stopped
    }

    // Block until the sampler has made `passes` passes and waits for a
    // write.
    #[cfg(test)]
    fn wait_until_idle_after(&self, passes: u64) -> u64 {
        let state = self.lock();
        self.wake
            .wait_while(state, |state| !(state.idle && state.passes >= passes))
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .passes
    }
}

// The sampler's loop: wait an interval, read every volume `volumes` lists,
// and go idle when nothing moved. Ends when `signal` is stopped or `volumes`
// has no owner left to list them.
fn run_sampler(
    signal: &RateSignal,
    interval: Duration,
    mut volumes: impl FnMut() -> Option<Vec<Arc<Volume>>>,
) {
    let began = Instant::now();
    let mut rates = VolumeRates::default();
    loop {
        if !signal.pause(interval) {
            return;
        }
        let Some(listed) = volumes() else {
            return;
        };
        let moved = rates.pass(&listed, began.elapsed());
        drop(listed);
        if !signal.passed(moved, || rates.behind()) {
            return;
        }
    }
}

impl StorageCapacity {
    // Start the thread that keeps every volume's write rate. It ends when
    // the owner stops, as the root samplers do, and holds the owner only
    // weakly so it never keeps it from stopping.
    pub(crate) fn watch_volume_rates(self: &Arc<Self>) {
        drop(self.spawn_rate_sampler(SAMPLE_INTERVAL));
    }

    fn spawn_rate_sampler(self: &Arc<Self>, interval: Duration) -> Option<JoinHandle<()>> {
        let owner = Arc::downgrade(self);
        let signal = Arc::clone(&self.volume_rates);
        let spawned = std::thread::Builder::new()
            .name(SAMPLER_THREAD.to_string())
            .spawn(move || {
                run_sampler(&signal, interval, || {
                    owner.upgrade().map(|owner| owner.volumes())
                })
            });
        match spawned {
            Ok(handle) => Some(handle),
            Err(error) => {
                warn!(
                    error = %error,
                    "could not start the volume write rate sampler; rates stay unmeasured"
                );
                None
            }
        }
    }

    // What a volume's writer rings after a write, so the sampler leaves its
    // idle wait: `signal.wake()`, one relaxed load while the sampler runs.
    // SEAM: writer rings wake(). The disk writer takes this once, when it
    // is made, and calls `wake()` after each write call returns (after its
    // counters are added); nothing rings it yet, so until the writer lands
    // an idle sampler is woken by nothing.
    pub(crate) fn volume_rate_signal(&self) -> Arc<RateSignal> {
        Arc::clone(&self.volume_rates)
    }

    // The label of every volume held, and nothing else of it.
    pub fn volume_labels(&self) -> Vec<String> {
        self.volumes()
            .iter()
            .map(|volume| volume.label.clone())
            .collect()
    }

    // One row per volume, from what is held: the counters are read where
    // they stand and no disk is looked at.
    pub fn volume_snapshots(&self) -> Vec<VolumeSnapshot> {
        self.snapshots_of(&self.volumes())
    }

    fn snapshots_of(&self, volumes: &[Arc<Volume>]) -> Vec<VolumeSnapshot> {
        volumes
            .iter()
            .map(|volume| VolumeSnapshot {
                label: volume.label.clone(),
                kind: self.kind_on_device(volume.device).as_str(),
                device: volume.device,
                written_bytes: volume.written_bytes.load(Ordering::Relaxed),
                write_nanos: volume.write_nanos.load(Ordering::Relaxed),
                write_calls: volume.write_calls.load(Ordering::Relaxed),
                write_seeks: volume.write_seeks.load(Ordering::Relaxed),
                write_rate: volume.write_rate.load(Ordering::Relaxed),
                buffered_bytes: volume.buffered_bytes.load(Ordering::Relaxed),
            })
            .collect()
    }

    // The kind of storage behind `device`, as the owner holds it for a root
    // or a folder found on that device. Unknown when none is held. Never
    // looks.
    fn kind_on_device(&self, device: Option<u64>) -> StorageKind {
        let Some(device) = device else {
            return StorageKind::Unknown;
        };
        for slot in &self.slots {
            let state = slot.lock();
            if state.device == Some(Some(device))
                && let Some(kind) = state.kind
                && kind != StorageKind::Unknown
            {
                return kind;
            }
        }
        lock_paths(&self.paths)
            .entries
            .values()
            .filter(|entry| entry.device.is_some_and(|look| look.device == Some(device)))
            .filter_map(|entry| entry.space.as_ref().map(|look| look.kind))
            .find(|kind| *kind != StorageKind::Unknown)
            .unwrap_or(StorageKind::Unknown)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const MIB: u64 = 1024 * 1024;

    fn secs(seconds: u64) -> Duration {
        Duration::from_secs(seconds)
    }

    fn reading(written_bytes: u64, write_millis: u64) -> WriteReading {
        WriteReading {
            written_bytes,
            write_nanos: write_millis * 1_000_000,
        }
    }

    // A window that has taken its first reading, of a volume with nothing
    // written.
    fn begun() -> RateWindow {
        let mut window = RateWindow::default();
        assert_eq!(window.observe(secs(0), reading(0, 0)), None);
        window
    }

    fn wrote(volume: &Volume, bytes: u64, millis: u64) {
        volume.written_bytes.fetch_add(bytes, Ordering::Relaxed);
        volume
            .write_nanos
            .fetch_add(millis * 1_000_000, Ordering::Release);
    }

    fn rate(volume: &Volume) -> u64 {
        volume.write_rate.load(Ordering::Relaxed)
    }

    // The rate is bytes over the time spent inside write calls, not over
    // the time that passed.
    #[test]
    fn the_rate_is_bytes_over_time_spent_writing() {
        let mut window = begun();
        // 100 MiB in half a second of writing, over two seconds of clock.
        assert_eq!(
            window.observe(secs(2), reading(100 * MIB, 500)),
            Some(200 * MIB)
        );
    }

    // A first reading only says where the counters stand. Whatever it
    // holds, no rate comes from it; the next one is measured from it.
    #[test]
    fn a_first_reading_is_a_baseline_and_gives_no_rate() {
        let mut window = RateWindow::default();
        // Ten hours of slow writing before the window began.
        assert_eq!(
            window.observe(secs(2), reading(36_000 * MIB, 36_000_000)),
            None
        );
        // Then 50 MiB in a quarter of a second: 200 MiB a second, not the
        // one MiB a second of everything before.
        assert_eq!(
            window.observe(secs(4), reading(36_050 * MIB, 36_000_250)),
            Some(200 * MIB)
        );
    }

    // Counters that fell belong to another volume: they are a new baseline.
    #[test]
    fn counters_that_fell_start_over_with_no_rate() {
        let mut window = begun();
        assert!(window.observe(secs(2), reading(100 * MIB, 1000)).is_some());
        assert_eq!(window.observe(secs(4), reading(40 * MIB, 4000)), None);
        assert_eq!(
            window.observe(secs(6), reading(60 * MIB, 5000)),
            Some(20 * MIB)
        );
    }

    // Nothing written reads as not measured, however long it lasts.
    #[test]
    fn a_volume_never_written_to_stays_unmeasured() {
        let mut window = RateWindow::default();
        for tick in 1..=40 {
            assert_eq!(window.observe(secs(tick * 2), reading(0, 0)), None);
        }
    }

    // A pass with no new bytes answers nothing, so the last rate stands.
    #[test]
    fn a_window_with_no_writes_answers_nothing() {
        let mut window = begun();
        assert!(window.observe(secs(2), reading(10 * MIB, 100)).is_some());
        for tick in 2..=60 {
            assert_eq!(window.observe(secs(tick * 2), reading(10 * MIB, 100)), None);
        }
    }

    // Writes older than the window no longer count: the rate follows the
    // disk as it is now.
    #[test]
    fn writes_older_than_the_window_fall_out() {
        let mut window = begun();
        // A fast first stretch: 100 MiB a second.
        let mut at = 0;
        let mut total = reading(0, 0);
        for _ in 0..5 {
            at += 2;
            total.written_bytes += 100 * MIB;
            total.write_nanos += 1_000_000_000;
            assert_eq!(window.observe(secs(at), total), Some(100 * MIB));
        }
        // Then 10 MiB a second. While both stretches are in the window the
        // rate lies between them.
        at += 2;
        total.written_bytes += 10 * MIB;
        total.write_nanos += 1_000_000_000;
        let mixed = window.observe(secs(at), total).unwrap();
        assert!(mixed < 100 * MIB && mixed > 10 * MIB, "{mixed}");
        let mut latest = mixed;
        for _ in 0..15 {
            at += 2;
            total.written_bytes += 10 * MIB;
            total.write_nanos += 1_000_000_000;
            latest = window.observe(secs(at), total).unwrap();
        }
        assert_eq!(latest, 10 * MIB);
    }

    // After a long idle stretch the next writes are measured on their own:
    // the idle time adds nothing, and the old writes are out of the window.
    #[test]
    fn writes_after_an_idle_stretch_are_measured_alone() {
        let mut window = begun();
        assert_eq!(
            window.observe(secs(2), reading(100 * MIB, 1000)),
            Some(100 * MIB)
        );
        assert_eq!(
            window.observe(secs(600), reading(120 * MIB, 3000)),
            Some(10 * MIB)
        );
    }

    // The window holds one reading per pass that wrote, and no more than
    // the passes of one window.
    #[test]
    fn the_window_holds_a_bounded_number_of_readings() {
        let mut window = RateWindow::default();
        for tick in 1..=500u64 {
            window.observe(secs(tick * 2), reading(tick * MIB, tick * 10));
        }
        let most = (WINDOW.as_secs() / SAMPLE_INTERVAL.as_secs()) as usize + 1;
        assert!(window.readings.len() <= most, "{}", window.readings.len());
    }

    // Bytes with no time inside a write call yet cannot be divided: nothing
    // is answered, and the next reading covers them.
    #[test]
    fn bytes_with_no_write_time_answer_nothing() {
        let mut window = begun();
        assert_eq!(window.observe(secs(2), reading(MIB, 0)), None);
        assert_eq!(window.observe(secs(4), reading(2 * MIB, 1000)), Some(2 * MIB));
    }

    // A rate under one byte a second is told as one, never as unmeasured.
    #[test]
    fn the_slowest_rate_told_is_one() {
        let mut window = begun();
        assert_eq!(window.observe(secs(2), reading(1, 5000)), Some(1));
    }

    // A pass stores each volume's own rate and leaves alone the ones that
    // did not write, and says whether anything moved.
    #[test]
    fn a_pass_stores_the_rate_of_each_volume_that_wrote() {
        let fast = Arc::new(Volume::new(Some(1), "/mnt/fast".into()));
        let slow = Arc::new(Volume::new(Some(2), "/mnt/slow".into()));
        let idle = Arc::new(Volume::new(Some(3), "/mnt/idle".into()));
        let volumes = [Arc::clone(&fast), Arc::clone(&slow), Arc::clone(&idle)];
        let mut rates = VolumeRates::default();

        // Nothing written anywhere: nothing moved.
        assert!(!rates.pass(&volumes, secs(2)));
        for volume in &volumes {
            assert_eq!(rate(volume), 0);
        }

        wrote(&fast, 400 * MIB, 1000);
        wrote(&slow, 40 * MIB, 2000);
        assert!(rates.behind());
        assert!(rates.pass(&volumes, secs(4)));
        assert!(!rates.behind());
        assert_eq!(rate(&fast), 400 * MIB);
        assert_eq!(rate(&slow), 20 * MIB);
        assert_eq!(rate(&idle), 0);

        // Passes with nothing new, well past the window: nothing moves and
        // the rates stand.
        for tick in 3..=40 {
            assert!(!rates.pass(&volumes, secs(tick * 2)));
        }
        assert_eq!(rate(&fast), 400 * MIB);
        assert_eq!(rate(&slow), 20 * MIB);
        assert_eq!(rate(&idle), 0);
    }

    // A volume first seen with bytes already written gets no rate from
    // them; it has moved, so the sampler comes back, and what it writes
    // next is measured on its own.
    #[test]
    fn a_volume_first_seen_with_bytes_gets_no_rate_from_them() {
        let volume = Arc::new(Volume::new(Some(1), "/mnt/pool".into()));
        wrote(&volume, 50 * MIB, 500);
        let mut rates = VolumeRates::default();
        assert!(rates.pass(&[Arc::clone(&volume)], secs(2)));
        assert_eq!(rate(&volume), 0);
        wrote(&volume, 10 * MIB, 1000);
        assert!(rates.pass(&[Arc::clone(&volume)], secs(4)));
        assert_eq!(rate(&volume), 10 * MIB);
    }

    // A volume dropped from the list and listed again keeps the rate it
    // had: its whole-life average is never stored over it.
    #[test]
    fn a_volume_listed_again_does_not_take_its_whole_life_average() {
        let volume = Arc::new(Volume::new(Some(1), "/mnt/pool".into()));
        let listed = [Arc::clone(&volume)];
        let mut rates = VolumeRates::default();
        rates.pass(&listed, secs(2));
        // A slow start, 1 MiB a second for 100 seconds of writing, then a
        // fast stretch that the window holds alone.
        wrote(&volume, 100 * MIB, 100_000);
        rates.pass(&listed, secs(4));
        assert_eq!(rate(&volume), MIB);
        wrote(&volume, 300 * MIB, 1000);
        rates.pass(&listed, secs(40));
        assert_eq!(rate(&volume), 300 * MIB);

        // Dropped, then listed again with nothing new written. Its life's
        // average is under 4 MiB a second.
        rates.pass(&[], secs(42));
        assert!(rates.windows.is_empty());
        rates.pass(&listed, secs(44));
        assert_eq!(rate(&volume), 300 * MIB);

        // Its next write is measured from where it was listed again.
        wrote(&volume, 50 * MIB, 1000);
        rates.pass(&listed, secs(46));
        assert_eq!(rate(&volume), 50 * MIB);
    }

    // A volume that is no longer listed is dropped, not held for ever.
    #[test]
    fn a_volume_no_longer_listed_is_forgotten() {
        let kept = Arc::new(Volume::new(Some(1), "/mnt/kept".into()));
        let gone = Arc::new(Volume::new(Some(2), "/mnt/gone".into()));
        let mut rates = VolumeRates::default();
        rates.pass(&[Arc::clone(&kept), Arc::clone(&gone)], secs(2));
        assert_eq!(Arc::strong_count(&gone), 2);
        rates.pass(&[Arc::clone(&kept)], secs(4));
        assert_eq!(Arc::strong_count(&gone), 1);
        assert_eq!(rates.windows.len(), 1);
    }

    // The nanoseconds are read before the bytes, so a reading never holds
    // the time of a call without its bytes.
    #[test]
    fn a_reading_holds_both_counters() {
        let volume = Volume::new(Some(1), "/mnt/pool".into());
        wrote(&volume, 5 * MIB, 20);
        assert_eq!(WriteReading::of(&volume), reading(5 * MIB, 20));
    }

    fn sampler(
        signal: &Arc<RateSignal>,
        interval: Duration,
        volumes: &[Arc<Volume>],
    ) -> JoinHandle<()> {
        let signal = Arc::clone(signal);
        let volumes = volumes.to_vec();
        std::thread::spawn(move || run_sampler(&signal, interval, || Some(volumes.clone())))
    }

    // With nothing written the sampler makes one pass and then waits to be
    // rung. A ring after a write brings it back: it measures the write and,
    // once nothing moves again, waits once more.
    #[test]
    fn an_idle_sampler_waits_for_a_ring_and_a_ring_brings_it_back() {
        let volume = Arc::new(Volume::new(Some(1), "/mnt/pool".into()));
        let signal = Arc::new(RateSignal::default());
        let thread = sampler(&signal, Duration::ZERO, &[Arc::clone(&volume)]);

        let passes = signal.wait_until_idle_after(1);
        assert_eq!(passes, 1, "an idle sampler makes no pass");
        assert!(signal.idle.load(Ordering::Relaxed));
        assert_eq!(rate(&volume), 0);

        wrote(&volume, 80 * MIB, 500);
        signal.wake();
        // One pass sees the write, the next sees nothing new and waits.
        let passes = signal.wait_until_idle_after(passes + 2);
        assert_eq!(passes, 3);
        assert_eq!(rate(&volume), 160 * MIB);

        signal.stop();
        thread.join().expect("the sampler ended");
    }

    // A ring while the sampler is not idle does nothing and is not kept: it
    // is the busy writer's whole cost.
    #[test]
    fn a_ring_while_sampling_is_not_kept() {
        let signal = RateSignal::default();
        signal.wake();
        signal.wake();
        let state = signal.lock();
        assert!(!state.rung && !state.idle);
        assert!(!signal.idle.load(Ordering::Relaxed));
    }

    // A write that lands between the pass and the wait is seen before the
    // sampler waits: it does not go idle on counters that moved.
    #[test]
    fn a_write_behind_the_pass_keeps_the_sampler_from_waiting() {
        let signal = RateSignal::default();
        // Nothing moved in the pass, but a volume is behind: no wait, and
        // the sampler goes on.
        assert!(signal.passed(false, || true));
        let state = signal.lock();
        assert_eq!(state.passes, 1);
        assert!(!state.idle);
        assert!(!signal.idle.load(Ordering::Relaxed));
    }

    // Stopping ends the sampler where it waits for a ring.
    #[test]
    fn stopping_ends_a_sampler_that_waits_for_a_ring() {
        let signal = Arc::new(RateSignal::default());
        let thread = sampler(&signal, Duration::ZERO, &[]);
        signal.wait_until_idle_after(1);
        signal.stop();
        thread.join().expect("the sampler ended");
    }

    // Stopping ends the sampler inside its interval too, without the
    // interval running out.
    #[test]
    fn stopping_ends_a_sampler_inside_its_interval() {
        let signal = Arc::new(RateSignal::default());
        let thread = sampler(&signal, Duration::from_secs(24 * 3600), &[]);
        signal.stop();
        thread.join().expect("the sampler ended");
    }

    fn owner(dir: &std::path::Path) -> StorageCapacity {
        StorageCapacity::with_probe(
            [
                dir.join("data"),
                dir.join("intermediate"),
                dir.join("complete"),
            ],
            Duration::from_secs(3600),
            Arc::new(|_| {
                Ok(super::super::DiskSpace {
                    total_bytes: 1,
                    available_bytes: 1,
                })
            }),
        )
    }

    // The owner's sampler ends when the owner is dropped, as its root
    // samplers do, whether it waits for a ring or inside its interval.
    #[test]
    fn the_sampler_ends_when_its_owner_stops() {
        let dir = tempfile::tempdir().unwrap();

        let storage = Arc::new(owner(dir.path()));
        let signal = storage.volume_rate_signal();
        let thread = storage
            .spawn_rate_sampler(Duration::ZERO)
            .expect("a sampler");
        signal.wait_until_idle_after(1);
        drop(storage);
        thread.join().expect("the idle sampler ended");

        let storage = Arc::new(owner(dir.path()));
        let thread = storage
            .spawn_rate_sampler(Duration::from_secs(24 * 3600))
            .expect("a sampler");
        drop(storage);
        thread.join().expect("the waiting sampler ended");
    }

    // The kind of storage is answered for a device from what the owner
    // holds of its roots, with no look of its own.
    #[test]
    fn a_volumes_kind_is_what_the_owner_holds_for_its_device() {
        let dir = tempfile::tempdir().unwrap();
        let storage = owner(dir.path());
        // Device ids no real root here can hold: the real one with a high
        // bit turned over.
        let real = super::super::nearest_device(dir.path());
        let held = real.unwrap_or(0) ^ (1 << 63);
        let other = real.unwrap_or(0) ^ (1 << 62);
        assert_ne!(Some(held), real);
        assert_ne!(Some(other), real);
        assert_ne!(held, other);
        // The root's own look is over before its reading is replaced, and
        // the next is an hour off.
        storage.wait_for_probes(super::super::StorageRoot::ALL[0], 1);
        {
            let mut state = storage.slots[0].lock();
            state.device = Some(Some(held));
            state.kind = Some(StorageKind::Solid);
        }
        assert_eq!(storage.kind_on_device(Some(held)), StorageKind::Solid);
        assert_eq!(storage.kind_on_device(Some(other)), StorageKind::Unknown);
        assert_eq!(storage.kind_on_device(None), StorageKind::Unknown);

        // A row carries each counter where it stands, and the kind held for
        // the volume's device.
        let written = Volume::new(Some(held), "/mnt/pool".into());
        written.written_bytes.store(8_000_000, Ordering::Relaxed);
        written.write_nanos.store(2_500_000_000, Ordering::Relaxed);
        written.write_calls.store(120, Ordering::Relaxed);
        written.write_seeks.store(9, Ordering::Relaxed);
        written.write_rate.store(3_200_000, Ordering::Relaxed);
        written.buffered_bytes.store(65_536, Ordering::Relaxed);
        let unseen = Volume::new(None, "/mnt/scratch".into());
        let rows = storage.snapshots_of(&[Arc::new(written), Arc::new(unseen)]);
        assert_eq!(
            rows,
            vec![
                VolumeSnapshot {
                    label: "/mnt/pool".into(),
                    kind: "ssd",
                    device: Some(held),
                    written_bytes: 8_000_000,
                    write_nanos: 2_500_000_000,
                    write_calls: 120,
                    write_seeks: 9,
                    write_rate: 3_200_000,
                    buffered_bytes: 65_536,
                },
                VolumeSnapshot {
                    label: "/mnt/scratch".into(),
                    kind: "unknown",
                    device: None,
                    written_bytes: 0,
                    write_nanos: 0,
                    write_calls: 0,
                    write_seeks: 0,
                    write_rate: 0,
                    buffered_bytes: 0,
                },
            ]
        );
    }
}
