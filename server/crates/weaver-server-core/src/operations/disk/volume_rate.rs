// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Two figures for each volume, both in bytes per second.
//
// `write_rate` is what the volume takes. Time spent inside buffered write
// calls cannot tell it: until the OS cache fills, a write call returns at
// memory speed. What can is a stretch in which the volume's writer never ran
// out of work: it wrote as fast as the volume let it the whole time, so the
// bytes it wrote over the wall time that passed are the volume's rate. A
// span between two readings is such a stretch, a busy span, when the
// writer's count of idle turns did not move across it and bytes were waiting
// for the writer at both of its ends. Only busy spans count; any other
// leaves the figure in force alone, whether that is a start-up probe's, an
// earlier busy window's, or zero for "not measured".
//
// `throughput` is what the writer is writing now: all bytes over the wall
// time of the same window, whoever was the limit. It returns to zero when
// nothing was written in the window.
//
// Every [`SAMPLE_INTERVAL`] one thread reads each volume's counters and
// keeps the readings of the last [`WINDOW`]. Nothing here writes, flushes or
// looks at a disk: a pass reads atomics.
//
// The thread does not tick while nothing is written. Once no counters move
// and every throughput is back at zero it waits to be rung
// ([`RateSignal::wake`]) and reads nothing until then. It ends when the disk
// owner stops.

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

// A volume's counters, read together.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct WriteReading {
    pub(crate) written_bytes: u64,
    pub(crate) write_nanos: u64,
    // Times the writer had found nothing waiting.
    pub(crate) idle_turns: u64,
    // Whether bytes were waiting in memory for the writer. The bytes of a
    // call in flight still count as waiting.
    pub(crate) waiting: bool,
}

impl WriteReading {
    // The nanoseconds are read first, and the bytes after them. The writer
    // adds a call's bytes first and stores its nanoseconds last, with
    // `Release` (see [`Volume`]), so the bytes read here are at least the
    // ones the nanoseconds were spent on: a reading may hold the bytes of a
    // call still in flight, never the time of a call without its bytes. The
    // idle turns are read after both: a turn the writer took once those
    // bytes were written is then seen with them.
    fn of(volume: &Volume) -> Self {
        let write_nanos = volume.write_nanos.load(Ordering::Acquire);
        let written_bytes = volume.written_bytes.load(Ordering::Relaxed);
        let idle_turns = volume.idle_turns.load(Ordering::Relaxed);
        let waiting = volume.buffered_bytes.load(Ordering::Relaxed) > 0;
        Self {
            written_bytes,
            write_nanos,
            idle_turns,
            waiting,
        }
    }

    // Whether the write counters are where `other` has them.
    fn wrote_as(&self, other: &Self) -> bool {
        self.written_bytes == other.written_bytes && self.write_nanos == other.write_nanos
    }

    // Whether the writer never ran out of work from `earlier` to here: it
    // had bytes waiting at both ends and took no idle turn between them.
    //
    // The waiting bytes are the guard against a span that only looks busy.
    // A writer that sits waiting takes no idle turn while it waits, so a
    // span that began with nothing to write and ended on a burst has idle
    // turns that did not move and bytes written, over wall time the writer
    // mostly spent with nothing to do. Bytes waiting at the span's start
    // say the writer had work from its first moment; a floor on the span's
    // bytes could not, as a late burst passes any floor, and it would shut
    // out a slow volume that never writes that much in one span.
    fn busy_since(&self, earlier: &Self) -> bool {
        self.idle_turns == earlier.idle_turns && self.waiting && earlier.waiting
    }
}

// What one reading made of a volume's figures.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct Observed {
    // The volume's rate over the busy spans of the window, when the span
    // this reading ends is one and the window's busy spans wrote anything.
    // `None` leaves the figure in force as it is.
    pub(crate) write_rate: Option<u64>,
    // All bytes written in the window over its wall time; zero when none
    // were.
    pub(crate) throughput: u64,
}

// The readings of one volume over the last [`WINDOW`], oldest first, one
// for each pass. The oldest kept is the latest from at or before the
// window's start, which is what the counters stood at when the window began.
#[derive(Debug, Default)]
pub(crate) struct RateWindow {
    readings: VecDeque<(Duration, WriteReading)>,
}

// Bytes over a stretch of time as bytes per second. Zero is kept for "not
// measured" and "nothing written": the slowest rate told is one.
fn per_second(bytes: u64, over: Duration) -> u64 {
    if bytes == 0 || over.is_zero() {
        return 0;
    }
    let rate = u128::from(bytes) * 1_000_000_000 / over.as_nanos();
    u64::try_from(rate).unwrap_or(u64::MAX).max(1)
}

impl RateWindow {
    // Take the reading made at `at`, a time since the sampler began, and
    // answer the figures over the window ending there. A window's first
    // reading only says where the counters stand: no figure comes from it.
    pub(crate) fn observe(&mut self, at: Duration, reading: WriteReading) -> Observed {
        let Some((_, last)) = self.readings.back().copied() else {
            self.restart(at, reading);
            return Observed::default();
        };
        // The counters only rise. Ones that fell are another volume's:
        // start over from them.
        if reading.written_bytes < last.written_bytes || reading.write_nanos < last.write_nanos {
            self.restart(at, reading);
            return Observed::default();
        }
        let start = at.saturating_sub(WINDOW);
        while self.readings.len() >= 2 && self.readings[1].0 <= start {
            self.readings.pop_front();
        }
        self.readings.push_back((at, reading));

        let (began, base) = self.readings[0];
        let throughput = per_second(
            reading.written_bytes - base.written_bytes,
            at.saturating_sub(began),
        );
        let write_rate = reading
            .busy_since(&last)
            .then(|| self.busy_rate())
            .filter(|rate| *rate > 0);
        Observed {
            write_rate,
            throughput,
        }
    }

    // Forget every reading and begin again from this one. What was written
    // before it is in no span.
    pub(crate) fn restart(&mut self, at: Duration, reading: WriteReading) {
        self.readings.clear();
        self.readings.push_back((at, reading));
    }

    // Bytes over wall time across the busy spans held. A busy span in which
    // no call finished adds its time and no bytes: the writer was inside a
    // call the whole of it.
    fn busy_rate(&self) -> u64 {
        let mut bytes = 0u64;
        let mut wall = Duration::ZERO;
        for ((began, earlier), (ended, later)) in self.readings.iter().zip(self.readings.iter().skip(1)) {
            if later.busy_since(earlier) {
                bytes += later.written_bytes - earlier.written_bytes;
                wall += ended.saturating_sub(*began);
            }
        }
        per_second(bytes, wall)
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
    // counters, store its throughput where that changed, and store its rate
    // when the span just ended was a busy one. A volume no longer listed is
    // forgotten. Says whether there is anything to come back for: a
    // volume's write counters moved, or one still shows a throughput that
    // has to fall back to zero.
    pub(crate) fn pass(&mut self, volumes: &[Arc<Volume>], at: Duration) -> bool {
        let mut again = false;
        for volume in volumes {
            let (_, window) = self
                .windows
                .entry(Arc::as_ptr(volume) as usize)
                .or_insert_with(|| (Arc::clone(volume), RateWindow::default()));
            let reading = WriteReading::of(volume);
            // A volume first seen with nothing written has not moved.
            let moved = !reading.wrote_as(&window.latest().unwrap_or_default());
            let observed = window.observe(at, reading);
            if let Some(rate) = observed.write_rate {
                volume.write_rate.store(rate, Ordering::Relaxed);
            }
            if volume.throughput.load(Ordering::Relaxed) != observed.throughput {
                volume.throughput.store(observed.throughput, Ordering::Relaxed);
            }
            again |= moved || observed.throughput > 0;
        }
        self.forget_unlisted(volumes);
        again
    }

    // Begin every listed volume's window again at `at`. Made when the
    // sampler is rung out of its idle wait: it was not reading while it
    // waited, so the time it waited is no part of any span.
    pub(crate) fn restart(&mut self, volumes: &[Arc<Volume>], at: Duration) {
        for volume in volumes {
            let (_, window) = self
                .windows
                .entry(Arc::as_ptr(volume) as usize)
                .or_insert_with(|| (Arc::clone(volume), RateWindow::default()));
            window.restart(at, WriteReading::of(volume));
        }
        self.forget_unlisted(volumes);
    }

    fn forget_unlisted(&mut self, volumes: &[Arc<Volume>]) {
        if self.windows.len() > volumes.len() {
            let listed: HashSet<usize> = volumes
                .iter()
                .map(|volume| Arc::as_ptr(volume) as usize)
                .collect();
            self.windows.retain(|address, _| listed.contains(address));
        }
    }

    // Whether any volume seen has write counters past its latest reading: a
    // write landed since the pass.
    fn behind(&self) -> bool {
        self.windows.values().any(|(volume, window)| {
            window
                .latest()
                .is_none_or(|latest| !latest.wrote_as(&WriteReading::of(volume)))
        })
    }
}

// What the sampler does after a pass.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Next {
    // The owner stopped.
    Stop,
    // Wait an interval and pass again.
    Tick,
    // It waited idle and was rung: begin every window again first.
    Rung,
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
    // The kind of storage found for each volume's device, kept so a scrape
    // does not look it up again. Only a kind that was found is kept.
    kinds: Mutex<HashMap<u64, StorageKind>>,
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

    fn kinds(&self) -> MutexGuard<'_, HashMap<u64, StorageKind>> {
        self.kinds
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
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

    // A pass is over. When there is nothing to come back for (`again` is
    // false), and no volume is `behind` once writers can see the sampler is
    // idle, wait for a ring.
    fn passed(&self, again: bool, behind: impl FnOnce() -> bool) -> Next {
        let mut state = self.lock();
        state.passes += 1;
        let mut waited = false;
        if !again && !state.stopped {
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
                waited = true;
            }
            state.idle = false;
            state.rung = false;
            self.idle.store(false, Ordering::Relaxed);
        }
        let stopped = state.stopped;
        drop(state);
        self.wake.notify_all();
        match (stopped, waited) {
            (true, _) => Next::Stop,
            (false, true) => Next::Rung,
            (false, false) => Next::Tick,
        }
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

// The sampler's loop: wait an interval, read every volume `volumes` lists
// at the time `clock` tells, and go idle when there is nothing to come back
// for. Rung out of idle, it begins every window again at once, so the next
// pass measures one interval and not the time it waited. Ends when `signal`
// is stopped or `volumes` has no owner left to list them.
fn run_sampler(
    signal: &RateSignal,
    interval: Duration,
    mut clock: impl FnMut() -> Duration,
    mut volumes: impl FnMut() -> Option<Vec<Arc<Volume>>>,
) {
    let mut rates = VolumeRates::default();
    loop {
        if !signal.pause(interval) {
            return;
        }
        let Some(listed) = volumes() else {
            return;
        };
        let again = rates.pass(&listed, clock());
        drop(listed);
        match signal.passed(again, || rates.behind()) {
            Next::Stop => return,
            Next::Tick => {}
            Next::Rung => {
                let Some(listed) = volumes() else {
                    return;
                };
                rates.restart(&listed, clock());
            }
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
                let began = Instant::now();
                run_sampler(
                    &signal,
                    interval,
                    || began.elapsed(),
                    || owner.upgrade().map(|owner| owner.volumes()),
                )
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

    // The kind of storage behind `device`. Found once for each device and
    // kept beside the sampler's state; while it is still unknown each ask
    // looks through what the owner holds again. Never looks at a disk.
    pub(super) fn kind_on_device(&self, device: Option<u64>) -> StorageKind {
        let Some(device) = device else {
            return StorageKind::Unknown;
        };
        if let Some(kind) = self.volume_rates.kinds().get(&device) {
            return *kind;
        }
        let kind = self.held_kind_on_device(device);
        if kind != StorageKind::Unknown {
            self.volume_rates.kinds().insert(device, kind);
        }
        kind
    }

    // The kind the owner holds for a root or a folder found on `device`;
    // unknown when none is held. Three root locks, the shared paths lock and
    // a walk of the cached folders: asked only until a kind is found.
    fn held_kind_on_device(&self, device: u64) -> StorageKind {
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

    // A reading with bytes waiting for the writer.
    fn busy(written_mib: u64, idle_turns: u64) -> WriteReading {
        WriteReading {
            written_bytes: written_mib * MIB,
            write_nanos: written_mib * 1_000_000,
            idle_turns,
            waiting: true,
        }
    }

    // The same with nothing waiting.
    fn drained(written_mib: u64, idle_turns: u64) -> WriteReading {
        WriteReading {
            waiting: false,
            ..busy(written_mib, idle_turns)
        }
    }

    // A window whose first reading, at zero, is `first`.
    fn begun(first: WriteReading) -> RateWindow {
        let mut window = RateWindow::default();
        assert_eq!(window.observe(secs(0), first), Observed::default());
        window
    }

    fn volume(label: &str) -> Arc<Volume> {
        Arc::new(Volume::new(Some(1), label.into()))
    }

    fn wrote(volume: &Volume, mib: u64) {
        volume.written_bytes.fetch_add(mib * MIB, Ordering::Relaxed);
        volume.write_nanos.fetch_add(mib * 1_000_000, Ordering::Release);
    }

    fn waiting(volume: &Volume, bytes: u64) {
        volume.buffered_bytes.store(bytes, Ordering::Relaxed);
    }

    fn rate(volume: &Volume) -> u64 {
        volume.write_rate.load(Ordering::Relaxed)
    }

    fn throughput(volume: &Volume) -> u64 {
        volume.throughput.load(Ordering::Relaxed)
    }

    // A span across which the writer took no idle turn, with bytes waiting
    // at both ends, sets the rate: its bytes over the wall time that passed,
    // whatever time the write calls say they took.
    #[test]
    fn a_busy_span_sets_bytes_over_wall_time() {
        let mut window = begun(busy(0, 4));
        // 100 MiB in two seconds of clock; the calls took a tenth of one.
        let observed = window.observe(secs(2), busy(100, 4));
        assert_eq!(observed.write_rate, Some(50 * MIB));
    }

    // A span in which the writer ran out of work even once changes nothing,
    // however much was written in it.
    #[test]
    fn a_span_in_which_the_writer_idled_gives_no_rate() {
        let mut window = begun(busy(0, 4));
        assert_eq!(window.observe(secs(2), busy(100, 5)).write_rate, None);
        assert_eq!(window.observe(secs(4), busy(200, 9)).write_rate, None);
    }

    // The guard: idle turns that did not move are not enough. A writer that
    // sat waiting takes no idle turn while it waits, so a span that began
    // with nothing waiting, or ended so, is not a busy one.
    #[test]
    fn a_span_with_nothing_waiting_at_either_end_gives_no_rate() {
        // Nothing waiting at the start: one article landed late in the span.
        let mut window = begun(drained(0, 4));
        assert_eq!(window.observe(secs(2), busy(1, 4)).write_rate, None);
        // Nothing waiting at the end: the writer caught up.
        let mut window = begun(busy(0, 4));
        assert_eq!(window.observe(secs(2), drained(1, 4)).write_rate, None);
        // Bytes waiting at both ends of the same span: it counts.
        let mut window = begun(busy(0, 4));
        assert_eq!(
            window.observe(secs(2), busy(1, 4)).write_rate,
            Some(MIB / 2)
        );
    }

    // A burst written at memory speed (far more bytes than the volume takes,
    // next to no time inside the calls) while the writer kept running dry
    // does not raise the figure.
    #[test]
    fn a_memory_speed_burst_does_not_raise_the_figure() {
        let disk = volume("/mnt/pool");
        let listed = [Arc::clone(&disk)];
        let mut rates = VolumeRates::default();
        waiting(&disk, MIB);
        rates.pass(&listed, secs(0));
        wrote(&disk, 600);
        rates.pass(&listed, secs(2));
        assert_eq!(rate(&disk), 300 * MIB);

        // 26 GiB in two seconds, a millisecond inside the calls, the writer
        // out of work thirty times on the way.
        disk.written_bytes
            .fetch_add(26 * 1024 * MIB, Ordering::Relaxed);
        disk.write_nanos.fetch_add(1_000_000, Ordering::Release);
        disk.idle_turns.fetch_add(30, Ordering::Relaxed);
        rates.pass(&listed, secs(4));
        assert_eq!(rate(&disk), 300 * MIB);
        // What is being written now does show it.
        assert!(throughput(&disk) > 6 * 1024 * MIB, "{}", throughput(&disk));
    }

    // A figure a start-up probe stored stays through any number of spans
    // that are not busy, is never set back to zero, and is replaced by the
    // first busy span.
    #[test]
    fn a_probe_figure_stands_until_the_first_busy_span() {
        const PROBED: u64 = 180 * MIB;
        let disk = volume("/mnt/pool");
        let listed = [Arc::clone(&disk)];
        disk.write_rate.store(PROBED, Ordering::Relaxed);
        let mut rates = VolumeRates::default();
        rates.pass(&listed, secs(0));

        // Sixty spans: the writer idles in some, nothing waits in others,
        // nothing is written in the rest.
        let mut at = 0;
        for span in 0..60 {
            at += 2;
            match span % 3 {
                0 => {
                    waiting(&disk, MIB);
                    wrote(&disk, 900);
                    disk.idle_turns.fetch_add(1, Ordering::Relaxed);
                }
                1 => {
                    waiting(&disk, 0);
                    wrote(&disk, 900);
                }
                _ => {}
            }
            rates.pass(&listed, secs(at));
            assert_eq!(rate(&disk), PROBED, "span {span}");
        }

        // Two spans with work waiting throughout: the second is busy from
        // end to end.
        waiting(&disk, MIB);
        rates.pass(&listed, secs(at + 2));
        assert_eq!(rate(&disk), PROBED);
        wrote(&disk, 80);
        rates.pass(&listed, secs(at + 4));
        assert_eq!(rate(&disk), 40 * MIB);
    }

    // The rate is over every busy span the window holds, and over those
    // alone: a span that is not busy between two that are adds neither its
    // bytes nor its time.
    #[test]
    fn the_rate_is_over_the_busy_spans_of_the_window_alone() {
        let mut window = begun(busy(0, 0));
        assert_eq!(
            window.observe(secs(2), busy(100, 0)).write_rate,
            Some(50 * MIB)
        );
        // The writer idled: 4,000 MiB that do not count.
        assert_eq!(window.observe(secs(4), busy(4100, 1)).write_rate, None);
        // Busy again at 20 MiB a second: (100 + 40) MiB over four seconds.
        assert_eq!(
            window.observe(secs(6), busy(4140, 1)).write_rate,
            Some(35 * MIB)
        );
    }

    // A busy span in which no call finished adds its time: the writer was
    // inside a slow call for all of it. With no bytes in any busy span
    // nothing is stored; zero is never a rate.
    #[test]
    fn a_busy_span_with_no_bytes_adds_time_and_never_a_zero() {
        let mut window = begun(busy(0, 0));
        assert_eq!(window.observe(secs(2), busy(0, 0)).write_rate, None);
        assert_eq!(window.observe(secs(4), busy(0, 0)).write_rate, None);
        // One call of 6 MiB returns after six seconds inside it.
        assert_eq!(window.observe(secs(6), busy(6, 0)).write_rate, Some(MIB));
        // The next has not returned yet.
        assert_eq!(
            window.observe(secs(8), busy(6, 0)).write_rate,
            Some(6 * MIB / 8)
        );
    }

    // Busy spans older than the window no longer count: the rate follows
    // the volume as it is now.
    #[test]
    fn busy_spans_older_than_the_window_fall_out() {
        let mut window = begun(busy(0, 0));
        let mut at = 0;
        let mut written = 0;
        for _ in 0..5 {
            at += 2;
            written += 200;
            assert_eq!(
                window.observe(secs(at), busy(written, 0)).write_rate,
                Some(100 * MIB)
            );
        }
        at += 2;
        written += 20;
        let mixed = window.observe(secs(at), busy(written, 0)).write_rate.unwrap();
        assert!(mixed < 100 * MIB && mixed > 10 * MIB, "{mixed}");
        let mut latest = mixed;
        for _ in 0..15 {
            at += 2;
            written += 20;
            latest = window.observe(secs(at), busy(written, 0)).write_rate.unwrap();
        }
        assert_eq!(latest, 10 * MIB);
    }

    // A rate under one byte a second is told as one, never as unmeasured.
    #[test]
    fn the_slowest_rate_told_is_one() {
        let mut window = begun(busy(0, 0));
        let mut one_byte = busy(0, 0);
        one_byte.written_bytes = 1;
        assert_eq!(window.observe(secs(5), one_byte).write_rate, Some(1));
        assert_eq!(per_second(0, secs(5)), 0);
        assert_eq!(per_second(5, Duration::ZERO), 0);
    }

    // A first reading only says where the counters stand. Whatever it
    // holds, no figure comes from it; the next one is measured from it.
    #[test]
    fn a_first_reading_is_a_baseline_and_gives_no_figure() {
        let mut window = RateWindow::default();
        assert_eq!(window.observe(secs(2), busy(36_000, 0)), Observed::default());
        assert_eq!(
            window.observe(secs(4), busy(36_050, 0)),
            Observed {
                write_rate: Some(25 * MIB),
                throughput: 25 * MIB,
            }
        );
    }

    // Counters that fell belong to another volume: they are a new baseline.
    #[test]
    fn counters_that_fell_start_over_with_no_figure() {
        let mut window = begun(busy(0, 0));
        assert!(window.observe(secs(2), busy(100, 0)).write_rate.is_some());
        assert_eq!(window.observe(secs(4), busy(40, 0)), Observed::default());
        assert_eq!(
            window.observe(secs(6), busy(60, 0)).write_rate,
            Some(10 * MIB)
        );
    }

    // Throughput is every byte over the wall time of the window, busy or
    // not. It rises with the writes and is zero again once the window holds
    // none.
    #[test]
    fn throughput_rises_with_writes_and_returns_to_zero() {
        let mut window = begun(drained(0, 0));
        // Nothing written yet.
        assert_eq!(window.observe(secs(2), drained(0, 0)).throughput, 0);
        // 40 MiB in the last two of four seconds, the writer idling: it
        // counts here and gives no rate.
        let observed = window.observe(secs(4), drained(40, 3));
        assert_eq!(observed.write_rate, None);
        assert_eq!(observed.throughput, 10 * MIB);
        let observed = window.observe(secs(6), drained(100, 5));
        assert!(observed.throughput > 10 * MIB, "{}", observed.throughput);

        // The writes stop. While any are in the window it is above zero.
        let mut at = 6;
        let mut latest = observed.throughput;
        while at < 34 {
            at += 2;
            let now = window.observe(secs(at), drained(100, 5)).throughput;
            assert!(now > 0 && now <= latest, "{now} at {at}");
            latest = now;
        }
        // Thirty seconds after the last write it is zero, and stays there.
        for _ in 0..5 {
            at += 2;
            assert_eq!(window.observe(secs(at), drained(100, 5)).throughput, 0);
        }
    }

    // The window holds one reading per pass, and no more than the passes of
    // one window.
    #[test]
    fn the_window_holds_a_bounded_number_of_readings() {
        let mut window = RateWindow::default();
        let most = (WINDOW.as_secs() / SAMPLE_INTERVAL.as_secs()) as usize + 2;
        for tick in 1..=500u64 {
            // Every third pass brings nothing new.
            window.observe(secs(tick * 2), busy(tick - tick % 3, 0));
            assert!(window.readings.len() <= most, "{}", window.readings.len());
        }
    }

    // A pass stores each volume's own figures, and says whether there is
    // anything to come back for: counters that moved, or a throughput that
    // has yet to fall back to zero.
    #[test]
    fn a_pass_stores_each_volumes_figures_and_says_when_to_come_back() {
        let fast = volume("/mnt/fast");
        let slow = volume("/mnt/slow");
        let idle = volume("/mnt/idle");
        let volumes = [Arc::clone(&fast), Arc::clone(&slow), Arc::clone(&idle)];
        let mut rates = VolumeRates::default();
        waiting(&fast, MIB);
        waiting(&slow, MIB);

        // Nothing written anywhere: nothing to come back for.
        assert!(!rates.pass(&volumes, secs(0)));

        wrote(&fast, 800);
        wrote(&slow, 40);
        assert!(rates.behind());
        assert!(rates.pass(&volumes, secs(2)));
        assert!(!rates.behind());
        assert_eq!(rate(&fast), 400 * MIB);
        assert_eq!(rate(&slow), 20 * MIB);
        assert_eq!(rate(&idle), 0);
        assert_eq!(throughput(&fast), 400 * MIB);
        assert_eq!(throughput(&idle), 0);

        // The writes stop and the writers drain. The pass comes back while
        // a throughput is above zero, and no longer once all are zero. The
        // rates stand throughout.
        waiting(&fast, 0);
        waiting(&slow, 0);
        let mut at = 2;
        let mut came_back = 0;
        loop {
            at += 2;
            if !rates.pass(&volumes, secs(at)) {
                break;
            }
            came_back += 1;
            assert!(throughput(&fast) > 0);
        }
        // The write was read at two seconds; thirty seconds on it is out.
        assert_eq!(came_back, 14);
        assert_eq!(throughput(&fast), 0);
        assert_eq!(throughput(&slow), 0);
        assert_eq!(rate(&fast), 400 * MIB);
        assert_eq!(rate(&slow), 20 * MIB);
        assert_eq!(rate(&idle), 0);
    }

    // A volume first seen with bytes already written gets no figure from
    // them; it has moved, so the sampler comes back.
    #[test]
    fn a_volume_first_seen_with_bytes_gets_no_figure_from_them() {
        let disk = volume("/mnt/pool");
        waiting(&disk, MIB);
        wrote(&disk, 50);
        let mut rates = VolumeRates::default();
        assert!(rates.pass(&[Arc::clone(&disk)], secs(2)));
        assert_eq!((rate(&disk), throughput(&disk)), (0, 0));
        wrote(&disk, 20);
        assert!(rates.pass(&[Arc::clone(&disk)], secs(4)));
        assert_eq!((rate(&disk), throughput(&disk)), (10 * MIB, 10 * MIB));
    }

    // A volume dropped from the list and listed again keeps the rate it
    // had: nothing it wrote before it was listed again is in any span.
    #[test]
    fn a_volume_listed_again_keeps_its_rate() {
        let disk = volume("/mnt/pool");
        let listed = [Arc::clone(&disk)];
        let mut rates = VolumeRates::default();
        waiting(&disk, MIB);
        rates.pass(&listed, secs(0));
        wrote(&disk, 600);
        rates.pass(&listed, secs(2));
        assert_eq!(rate(&disk), 300 * MIB);

        rates.pass(&[], secs(4));
        assert!(rates.windows.is_empty());
        wrote(&disk, 10);
        rates.pass(&listed, secs(1000));
        assert_eq!(rate(&disk), 300 * MIB);

        wrote(&disk, 100);
        rates.pass(&listed, secs(1002));
        assert_eq!(rate(&disk), 50 * MIB);
    }

    // A volume that is no longer listed is dropped, not held for ever.
    #[test]
    fn a_volume_no_longer_listed_is_forgotten() {
        let kept = volume("/mnt/kept");
        let gone = volume("/mnt/gone");
        let mut rates = VolumeRates::default();
        rates.pass(&[Arc::clone(&kept), Arc::clone(&gone)], secs(2));
        assert_eq!(Arc::strong_count(&gone), 2);
        rates.pass(&[Arc::clone(&kept)], secs(4));
        assert_eq!(Arc::strong_count(&gone), 1);
        assert_eq!(rates.windows.len(), 1);
    }

    // Beginning again leaves the time before it out of every span: bytes
    // written while the sampler was not reading are in no figure.
    #[test]
    fn windows_begun_again_measure_from_there() {
        let disk = volume("/mnt/pool");
        let listed = [Arc::clone(&disk)];
        let mut rates = VolumeRates::default();
        waiting(&disk, MIB);
        rates.pass(&listed, secs(0));
        // An hour unread, then a ring.
        wrote(&disk, 7);
        rates.restart(&listed, secs(3600));
        assert_eq!((rate(&disk), throughput(&disk)), (0, 0));
        wrote(&disk, 60);
        rates.pass(&listed, secs(3602));
        assert_eq!((rate(&disk), throughput(&disk)), (30 * MIB, 30 * MIB));
    }

    // A reading holds every counter where it stands.
    #[test]
    fn a_reading_holds_every_counter() {
        let disk = volume("/mnt/pool");
        wrote(&disk, 5);
        disk.idle_turns.store(3, Ordering::Relaxed);
        assert_eq!(WriteReading::of(&disk), drained(5, 3));
        waiting(&disk, 1);
        assert_eq!(WriteReading::of(&disk), busy(5, 3));
    }

    // A sampler over `volumes` whose clock tells two more seconds each time
    // it is read, after `before_reading` has run with the count of readings
    // so far: what a test writes there lands just before that pass reads.
    fn sampler(
        signal: &Arc<RateSignal>,
        interval: Duration,
        volumes: &[Arc<Volume>],
        mut before_reading: impl FnMut(u64) + Send + 'static,
    ) -> JoinHandle<()> {
        let signal = Arc::clone(signal);
        let volumes = volumes.to_vec();
        let mut readings = 0;
        std::thread::spawn(move || {
            run_sampler(
                &signal,
                interval,
                move || {
                    readings += 1;
                    before_reading(readings);
                    secs(readings * 2)
                },
                || Some(volumes.clone()),
            )
        })
    }

    // With nothing written the sampler makes one pass and then waits to be
    // rung. A ring brings it back: it begins its windows again, measures
    // what is written next over one interval, stays until the throughput is
    // back at zero, and waits once more.
    #[test]
    fn an_idle_sampler_waits_for_a_ring_and_a_ring_brings_it_back() {
        let disk = volume("/mnt/pool");
        let signal = Arc::new(RateSignal::default());
        let written = Arc::clone(&disk);
        // Reading 1 is the first pass and reading 2 begins the windows
        // again after the ring; reading 3 is the pass after that.
        let thread = sampler(&signal, Duration::ZERO, &[Arc::clone(&disk)], move |reading| {
            match reading {
                3 => wrote(&written, 80),
                // The writer has caught up: nothing waits any more.
                4 => waiting(&written, 0),
                _ => {}
            }
        });

        let passes = signal.wait_until_idle_after(1);
        assert_eq!(passes, 1, "an idle sampler makes no pass");
        assert!(signal.idle.load(Ordering::Relaxed));
        assert_eq!((rate(&disk), throughput(&disk)), (0, 0));

        waiting(&disk, MIB);
        signal.wake();
        // The pass after the ring measures 80 MiB over its two seconds.
        // Thirty seconds of passes later the throughput is back at zero,
        // and then it waits.
        let passes = signal.wait_until_idle_after(passes + 1);
        assert_eq!(passes, 17);
        assert_eq!(rate(&disk), 40 * MIB);
        assert_eq!(throughput(&disk), 0);

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
        // Nothing to come back for in the pass, but a volume is behind: no
        // wait, and the sampler goes on as on any tick.
        assert_eq!(signal.passed(false, || true), Next::Tick);
        assert_eq!(signal.passed(true, || false), Next::Tick);
        let state = signal.lock();
        assert_eq!(state.passes, 2);
        assert!(!state.idle);
        assert!(!signal.idle.load(Ordering::Relaxed));
    }

    // Stopping ends the sampler where it waits for a ring.
    #[test]
    fn stopping_ends_a_sampler_that_waits_for_a_ring() {
        let signal = Arc::new(RateSignal::default());
        let thread = sampler(&signal, Duration::ZERO, &[], |_| {});
        signal.wait_until_idle_after(1);
        signal.stop();
        thread.join().expect("the sampler ended");
    }

    // Stopping ends the sampler inside its interval too, without the
    // interval running out.
    #[test]
    fn stopping_ends_a_sampler_inside_its_interval() {
        let signal = Arc::new(RateSignal::default());
        let thread = sampler(&signal, Duration::from_secs(24 * 3600), &[], |_| {});
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
        // A kind not found yet is not kept: it is looked for again.
        assert_eq!(storage.kind_on_device(Some(other)), StorageKind::Unknown);
        assert_eq!(storage.kind_on_device(None), StorageKind::Unknown);
        assert!(storage.volume_rates.kinds().is_empty());
        assert_eq!(storage.kind_on_device(Some(held)), StorageKind::Solid);
        // A kind found is kept and not looked for again: what the owner
        // holds for the device may change, the answer does not.
        storage.slots[0].lock().kind = Some(StorageKind::Rotational);
        assert_eq!(storage.kind_on_device(Some(held)), StorageKind::Solid);
        assert_eq!(storage.held_kind_on_device(held), StorageKind::Rotational);
        storage.slots[0].lock().kind = Some(StorageKind::Solid);
        // The one not found before is found once the owner holds it.
        {
            let mut state = storage.slots[1].lock();
            state.device = Some(Some(other));
            state.kind = Some(StorageKind::Network);
        }
        assert_eq!(storage.kind_on_device(Some(other)), StorageKind::Network);
        assert_eq!(storage.volume_rates.kinds().len(), 2);

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
