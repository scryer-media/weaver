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
// Zero means not measured. A volume stays at zero until its first window with
// writes in it, and a window with no writes leaves the last rate in place.
// Nothing here writes, flushes or looks at a disk: a pass reads atomics, and
// one that finds no new bytes stores nothing.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::{Duration, Instant};

use tracing::warn;

use super::volume::Volume;
use super::{StorageCapacity, StorageKind, lock_paths};
use crate::operations::instrumentation::VolumeSnapshot;

// How often the counters are read.
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
    fn of(volume: &Volume) -> Self {
        Self {
            written_bytes: volume.written_bytes.load(Ordering::Relaxed),
            write_nanos: volume.write_nanos.load(Ordering::Relaxed),
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
    // brought no new bytes, or no time inside a write call to divide by.
    pub(crate) fn observe(&mut self, at: Duration, reading: WriteReading) -> Option<u64> {
        match self.readings.back() {
            Some((_, last)) if *last == reading => return None,
            // The counters only rise. Ones that fell are another volume's:
            // start over from them.
            Some((_, last))
                if reading.written_bytes < last.written_bytes
                    || reading.write_nanos < last.write_nanos =>
            {
                self.readings.clear();
                self.readings.push_back((at, reading));
                return None;
            }
            Some(_) => {}
            // A volume starts from nothing written, so everything its first
            // reading holds was written in the window.
            None => {
                self.readings.push_back((at, WriteReading::default()));
                if reading == WriteReading::default() {
                    return None;
                }
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
    // pass. A volume no longer listed is forgotten.
    pub(crate) fn pass(&mut self, volumes: &[Arc<Volume>], at: Duration) {
        for volume in volumes {
            let (_, window) = self
                .windows
                .entry(Arc::as_ptr(volume) as usize)
                .or_insert_with(|| (Arc::clone(volume), RateWindow::default()));
            if let Some(rate) = window.observe(at, WriteReading::of(volume)) {
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
    }
}

impl StorageCapacity {
    // Start the thread that keeps every volume's write rate. It holds the
    // owner weakly and ends at its first pass after the owner is dropped.
    pub(crate) fn watch_volume_rates(self: &Arc<Self>) {
        let owner = Arc::downgrade(self);
        let spawned = std::thread::Builder::new()
            .name(SAMPLER_THREAD.to_string())
            .spawn(move || {
                let began = Instant::now();
                let mut rates = VolumeRates::default();
                loop {
                    std::thread::sleep(SAMPLE_INTERVAL);
                    let Some(owner) = owner.upgrade() else {
                        return;
                    };
                    rates.pass(&owner.volumes(), began.elapsed());
                }
            });
        if let Err(error) = spawned {
            warn!(
                error = %error,
                "could not start the volume write rate sampler; rates stay unmeasured"
            );
        }
    }

    // One row per volume, from what is held: the counters are read where
    // they stand and no disk is looked at.
    pub fn volume_snapshots(&self) -> Vec<VolumeSnapshot> {
        self.volumes()
            .iter()
            .map(|volume| VolumeSnapshot {
                label: volume.label.clone(),
                kind: self.kind_on_device(volume.device).as_str(),
                device: volume.device,
                written_bytes: volume.written_bytes.load(Ordering::Relaxed),
                write_nanos: volume.write_nanos.load(Ordering::Relaxed),
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

    // The rate is bytes over the time spent inside write calls, not over
    // the time that passed.
    #[test]
    fn the_rate_is_bytes_over_time_spent_writing() {
        let mut window = RateWindow::default();
        // 100 MiB in half a second of writing, over two seconds of clock.
        assert_eq!(
            window.observe(secs(2), reading(100 * MIB, 500)),
            Some(200 * MIB)
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
        let mut window = RateWindow::default();
        assert!(window.observe(secs(2), reading(10 * MIB, 100)).is_some());
        for tick in 2..=60 {
            assert_eq!(window.observe(secs(tick * 2), reading(10 * MIB, 100)), None);
        }
    }

    // Writes older than the window no longer count: the rate follows the
    // disk as it is now.
    #[test]
    fn writes_older_than_the_window_fall_out() {
        let mut window = RateWindow::default();
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
        let mut window = RateWindow::default();
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
        let mut window = RateWindow::default();
        assert_eq!(window.observe(secs(2), reading(MIB, 0)), None);
        assert_eq!(window.observe(secs(4), reading(2 * MIB, 1000)), Some(2 * MIB));
    }

    // A rate under one byte a second is told as one, never as unmeasured.
    #[test]
    fn the_slowest_rate_told_is_one() {
        let mut window = RateWindow::default();
        assert_eq!(window.observe(secs(2), reading(1, 5000)), Some(1));
    }

    // A pass stores each volume's own rate and leaves alone the ones that
    // did not write.
    #[test]
    fn a_pass_stores_the_rate_of_each_volume_that_wrote() {
        let fast = Arc::new(Volume::new(Some(1), "/mnt/fast".into()));
        let slow = Arc::new(Volume::new(Some(2), "/mnt/slow".into()));
        let idle = Arc::new(Volume::new(Some(3), "/mnt/idle".into()));
        let volumes = [Arc::clone(&fast), Arc::clone(&slow), Arc::clone(&idle)];
        let mut rates = VolumeRates::default();

        rates.pass(&volumes, secs(2));
        for volume in &volumes {
            assert_eq!(volume.write_rate.load(Ordering::Relaxed), 0);
        }

        fast.written_bytes.store(400 * MIB, Ordering::Relaxed);
        fast.write_nanos.store(1_000_000_000, Ordering::Relaxed);
        slow.written_bytes.store(40 * MIB, Ordering::Relaxed);
        slow.write_nanos.store(2_000_000_000, Ordering::Relaxed);
        rates.pass(&volumes, secs(4));
        assert_eq!(fast.write_rate.load(Ordering::Relaxed), 400 * MIB);
        assert_eq!(slow.write_rate.load(Ordering::Relaxed), 20 * MIB);
        assert_eq!(idle.write_rate.load(Ordering::Relaxed), 0);

        // Passes with nothing new, well past the window: the rates stand.
        for tick in 3..=40 {
            rates.pass(&volumes, secs(tick * 2));
        }
        assert_eq!(fast.write_rate.load(Ordering::Relaxed), 400 * MIB);
        assert_eq!(slow.write_rate.load(Ordering::Relaxed), 20 * MIB);
        assert_eq!(idle.write_rate.load(Ordering::Relaxed), 0);
    }

    // A volume first seen with bytes already written is measured from
    // nothing: all of them were written since it was made.
    #[test]
    fn a_volume_first_seen_with_bytes_is_measured_at_once() {
        let volume = Arc::new(Volume::new(Some(1), "/mnt/pool".into()));
        volume.written_bytes.store(50 * MIB, Ordering::Relaxed);
        volume.write_nanos.store(500_000_000, Ordering::Relaxed);
        let mut rates = VolumeRates::default();
        rates.pass(&[Arc::clone(&volume)], secs(2));
        assert_eq!(volume.write_rate.load(Ordering::Relaxed), 100 * MIB);
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

    // The kind of storage is answered for a device from what the owner
    // holds of its roots, with no look of its own.
    #[test]
    fn a_volumes_kind_is_what_the_owner_holds_for_its_device() {
        let dir = tempfile::tempdir().unwrap();
        let roots = [
            dir.path().join("data"),
            dir.path().join("intermediate"),
            dir.path().join("complete"),
        ];
        let storage = StorageCapacity::with_probe(
            roots,
            Duration::from_secs(3600),
            Arc::new(|_| {
                Ok(super::super::DiskSpace {
                    total_bytes: 1,
                    available_bytes: 1,
                })
            }),
        );
        // The root's own look is over before its reading is replaced, and
        // the next is an hour off.
        storage.wait_for_probes(super::super::StorageRoot::ALL[0], 1);
        {
            let mut state = storage.slots[0].lock();
            state.device = Some(Some(7));
            state.kind = Some(StorageKind::Solid);
        }
        assert_eq!(storage.kind_on_device(Some(7)), StorageKind::Solid);
        assert_eq!(storage.kind_on_device(Some(8)), StorageKind::Unknown);
        assert_eq!(storage.kind_on_device(None), StorageKind::Unknown);
        // The placeholder registry lists no volume.
        assert!(storage.volume_snapshots().is_empty());
    }
}
