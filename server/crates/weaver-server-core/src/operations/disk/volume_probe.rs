// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The first figure a volume's write rate takes: a synced write of a scratch
// file, made once per volume and kept between starts. Samples of real writes
// replace it afterwards; the probe only fills a rate that is still zero.
//
// The sync here measures a scratch file. Nothing on the download path syncs.
//
// Intended call site: where a volume's writer is first made, the caller
// leaves the probe with that writer for when it has nothing queued, so it
// runs on the writer's thread, never on the pipeline loop or the async
// runtime, and no write stands behind it that was queued before it began:
//
//     submitter.when_idle(move || { let _ = probe_once(volume, dir, store)(); });
//
// and may call `ProbeStore::apply_stored` wherever a volume is first seen,
// which reads memory only.

use std::collections::BTreeMap;
use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use tracing::{debug, info};

use super::volume::Volume;
use crate::runtime::system_probe::{ProbeFileGuard, ProbeWrite, synced_probe_write};

// The probe's scratch file, in the directory probed. A plain name with a
// text extension rather than a dotfile, as the start-up probe's is: security
// software treats a hidden file written with write-through as suspicious.
const SCRATCH_FILE: &str = "weaver-volume-probe.txt";
// Where the figures are kept, in the data directory.
const PROBE_RECORD: &str = "volume-probes.json";
// A stored figure older than this is taken again, and its row is dropped
// the next time the record is written.
const PROBE_MAX_AGE: Duration = Duration::from_secs(7 * 24 * 60 * 60);

// One volume's stored figure.
#[derive(Debug, Clone, Copy, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
struct StoredRate {
    // Seconds since the Unix epoch.
    measured_at: u64,
    // Bytes per second.
    write_rate: u64,
}

#[derive(Debug, Default, serde::Serialize, serde::Deserialize)]
struct Record {
    // By `volume_key`.
    volumes: BTreeMap<String, StoredRate>,
}

type Clock = Box<dyn Fn() -> SystemTime + Send + Sync>;
type Measure = Box<dyn Fn(&Path) -> Option<u64> + Send + Sync>;

// The probed figures of every volume, held in memory and in one small file.
// The file is read once, when the store is opened; asking after a volume
// afterwards reads memory. Rows older than [`PROBE_MAX_AGE`] are dropped
// whenever the file is written, so it holds only volumes probed in the last
// seven days.
pub(crate) struct ProbeStore {
    record: PathBuf,
    rows: Mutex<BTreeMap<String, StoredRate>>,
    now: Clock,
    measure: Measure,
}

impl ProbeStore {
    // Open the store kept in `data_dir`, reading what an earlier run left.
    // A missing or unreadable record is an empty one. This reads a file:
    // call it at start-up, off the pipeline loop and the async runtime.
    pub(crate) fn open(data_dir: &Path) -> Arc<Self> {
        Self::open_with(
            data_dir,
            Box::new(SystemTime::now),
            Box::new(measure_write_rate),
        )
    }

    // The store with its clock and probe passed in, so a test can age a row
    // and count probes without waiting or syncing.
    fn open_with(data_dir: &Path, now: Clock, measure: Measure) -> Arc<Self> {
        let record = data_dir.join(PROBE_RECORD);
        let rows = std::fs::read(&record)
            .ok()
            .and_then(|bytes| serde_json::from_slice::<Record>(&bytes).ok())
            .map(|record| record.volumes)
            .unwrap_or_default();
        Arc::new(Self {
            record,
            rows: Mutex::new(rows),
            now,
            measure,
        })
    }

    // Put the stored figure of `volume` on it, when one is held that is not
    // too old and the volume has no rate yet. Answers whether a stored
    // figure is held. Reads memory only.
    pub(crate) fn apply_stored(&self, volume: &Volume) -> bool {
        let now = unix_seconds((self.now)());
        let stored = self.lock().get(&volume_key(volume)).copied();
        let Some(stored) = stored.filter(|stored| fresh(stored, now)) else {
            return false;
        };
        set_if_unmeasured(volume, stored.write_rate);
        true
    }

    // Keep `write_rate` for `volume`, dropping rows that have aged out. A
    // record that cannot be written is tried again at the next probe.
    fn keep(&self, volume: &Volume, write_rate: u64) {
        let now = unix_seconds((self.now)());
        let mut rows = self.lock();
        rows.retain(|_, stored| fresh(stored, now));
        rows.insert(
            volume_key(volume),
            StoredRate {
                measured_at: now,
                write_rate,
            },
        );
        let written = serde_json::to_vec(&Record {
            volumes: rows.clone(),
        })
        .map_err(io::Error::other)
        .and_then(|json| std::fs::write(&self.record, json));
        if let Err(error) = written {
            debug!(
                path = %self.record.display(),
                error = %error,
                "could not store the volume write probe; it is taken again next start"
            );
        }
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, BTreeMap<String, StoredRate>> {
        self.rows.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

// The operation that gives `volume` its first write rate, to queue on the
// volume's writer. `dir` is a directory on the volume that can be written.
//
// When run it removes a scratch file a killed run left in `dir`, then: a
// stored figure, when one is held, is put on the volume and nothing is
// probed; a volume that already has a rate is left alone; otherwise the
// volume is probed once, the figure is stored, and it becomes the volume's
// rate unless a sample of real writes set one meanwhile.
//
// It always answers `Ok`: a volume that cannot be probed stays unmeasured,
// and that must not fail the writes queued behind it.
pub(crate) fn probe_once(
    volume: Arc<Volume>,
    dir: PathBuf,
    store: Arc<ProbeStore>,
) -> impl FnOnce() -> io::Result<()> + Send + 'static {
    move || {
        let _ = std::fs::remove_file(dir.join(SCRATCH_FILE));
        if store.apply_stored(&volume) || volume.write_rate.load(Ordering::Relaxed) > 0 {
            return Ok(());
        }
        let Some(write_rate) = (store.measure)(&dir) else {
            debug!(
                volume = %volume.label,
                dir = %dir.display(),
                "volume write probe could not run; the volume stays unmeasured"
            );
            return Ok(());
        };
        info!(
            volume = %volume.label,
            mb_per_second = write_rate / 1_000_000,
            "probed volume write rate"
        );
        set_if_unmeasured(&volume, write_rate);
        store.keep(&volume, write_rate);
        Ok(())
    }
}

// What the volume under `dir` takes, in bytes per second: a scratch file
// written there in growing chunks, each synced, for about a second of write
// time, then removed. `None` when the file could not be written.
fn measure_write_rate(dir: &Path) -> Option<u64> {
    measure_with(dir, synced_probe_write)
}

// The probe with its file writer passed in. The scratch file is removed
// however the writer ends.
fn measure_with(dir: &Path, write: impl FnOnce(&Path) -> Option<ProbeWrite>) -> Option<u64> {
    let path = dir.join(SCRATCH_FILE);
    let _guard = ProbeFileGuard(path.clone());
    probed_rate(write(&path)?)
}

// Bytes per second over the time spent writing and syncing. `None` when no
// time was measured. Zero is kept for "not measured": the slowest is one.
fn probed_rate(write: ProbeWrite) -> Option<u64> {
    let nanos = write.elapsed.as_nanos();
    if write.bytes == 0 || nanos == 0 {
        return None;
    }
    let rate = u128::from(write.bytes) * 1_000_000_000 / nanos;
    Some(u64::try_from(rate).unwrap_or(u64::MAX).max(1))
}

// Only a rate that is still zero is set: a sample of real writes that got
// there first stays.
fn set_if_unmeasured(volume: &Volume, write_rate: u64) {
    let _ = volume
        .write_rate
        .compare_exchange(0, write_rate, Ordering::Relaxed, Ordering::Relaxed);
}

// A volume's row: its device id and its label together. The registry holds
// one volume per device id, and one per label for paths with no device id,
// so no two volumes share a row; a label that turns up on another device, or
// a device under another label, has no row and is probed again.
fn volume_key(volume: &Volume) -> String {
    match volume.device {
        Some(device) => format!("{device}|{}", volume.label),
        None => format!("none|{}", volume.label),
    }
}

fn fresh(stored: &StoredRate, now: u64) -> bool {
    stored.write_rate > 0
        && now
            .checked_sub(stored.measured_at)
            .is_some_and(|age| age < PROBE_MAX_AGE.as_secs())
}

fn unix_seconds(at: SystemTime) -> u64 {
    at.duration_since(UNIX_EPOCH)
        .map(|since| since.as_secs())
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::AtomicU64;

    use super::*;
    use crate::runtime::system_probe::{PROBE_CHUNK_BYTES, probe_file_bytes};

    const MIB: u64 = 1024 * 1024;
    const DAY: u64 = 24 * 60 * 60;

    fn volume(device: Option<u64>, label: &str) -> Arc<Volume> {
        Arc::new(Volume::new(device, label.to_string()))
    }

    // A store whose clock stands at `now` and whose probe answers `rate`,
    // counting each time it is asked.
    fn store(data_dir: &Path, now: u64, rate: Option<u64>) -> (Arc<ProbeStore>, Arc<AtomicU64>) {
        let probes = Arc::new(AtomicU64::new(0));
        let counted = Arc::clone(&probes);
        let store = ProbeStore::open_with(
            data_dir,
            Box::new(move || UNIX_EPOCH + Duration::from_secs(now)),
            Box::new(move |_| {
                counted.fetch_add(1, Ordering::Relaxed);
                rate
            }),
        );
        (store, probes)
    }

    fn rate_of(volume: &Volume) -> u64 {
        volume.write_rate.load(Ordering::Relaxed)
    }

    // The figure is the bytes written over the time the synced chunks took.
    // A slow volume stops after its first chunk; a fast one writes them all.
    #[test]
    fn the_probed_rate_is_bytes_over_synced_write_time() {
        let probe_at = |bytes_per_second: u64| {
            probe_file_bytes(|repeats| {
                Some(Duration::from_nanos(
                    (repeats * PROBE_CHUNK_BYTES) as u64 * 1_000_000_000 / bytes_per_second,
                ))
            })
            .unwrap()
        };

        let slow = probe_at(8 * MIB);
        assert_eq!(slow.bytes, 16 * MIB);
        assert_eq!(slow.elapsed, Duration::from_secs(2));
        assert_eq!(probed_rate(slow), Some(8 * MIB));

        let fast = probe_at(1024 * MIB);
        assert_eq!(fast.bytes, 480 * MIB);
        assert_eq!(fast.elapsed, Duration::from_micros(468_750));
        assert_eq!(probed_rate(fast), Some(1024 * MIB));

        // No time measured is no figure, and the slowest figure is one.
        assert_eq!(
            probed_rate(ProbeWrite {
                bytes: 16 * MIB,
                elapsed: Duration::ZERO,
            }),
            None
        );
        assert_eq!(
            probed_rate(ProbeWrite {
                bytes: 1,
                elapsed: Duration::from_secs(5),
            }),
            Some(1)
        );
    }

    // The scratch file is gone whether the writer finished or gave up.
    #[test]
    fn the_scratch_file_is_removed_however_the_probe_ends() {
        let dir = tempfile::tempdir().unwrap();
        let scratch = dir.path().join(SCRATCH_FILE);

        let measured = measure_with(dir.path(), |path| {
            assert_eq!(path, scratch);
            std::fs::write(path, b"probe").unwrap();
            Some(ProbeWrite {
                bytes: 100,
                elapsed: Duration::from_secs(1),
            })
        });
        assert_eq!(measured, Some(100));
        assert!(!scratch.exists());

        let failed = measure_with(dir.path(), |path| {
            std::fs::write(path, b"probe").unwrap();
            None
        });
        assert_eq!(failed, None);
        assert!(!scratch.exists());
    }

    // A scratch file a killed run left is removed by the next probe of that
    // directory, even one that finds a stored figure and writes nothing.
    #[test]
    fn a_leftover_scratch_file_is_removed_at_the_next_probe() {
        let data = tempfile::tempdir().unwrap();
        let dir = tempfile::tempdir().unwrap();
        let scratch = dir.path().join(SCRATCH_FILE);
        let (store, probes) = store(data.path(), 1000, Some(50));

        std::fs::write(&scratch, b"left behind").unwrap();
        probe_once(volume(Some(1), "/"), dir.path().to_path_buf(), store.clone())().unwrap();
        assert!(!scratch.exists());

        std::fs::write(&scratch, b"left behind").unwrap();
        probe_once(volume(Some(1), "/"), dir.path().to_path_buf(), store)().unwrap();
        assert!(!scratch.exists());
        assert_eq!(probes.load(Ordering::Relaxed), 1);
    }

    #[test]
    fn a_probe_sets_the_rate_of_an_unmeasured_volume() {
        let data = tempfile::tempdir().unwrap();
        let (store, probes) = store(data.path(), 1000, Some(180_000_000));
        let volume = volume(Some(1), "/");

        probe_once(Arc::clone(&volume), data.path().to_path_buf(), store)().unwrap();

        assert_eq!(rate_of(&volume), 180_000_000);
        assert_eq!(probes.load(Ordering::Relaxed), 1);
    }

    // A volume real writes have measured is not probed at all.
    #[test]
    fn a_volume_with_a_live_rate_is_not_probed() {
        let data = tempfile::tempdir().unwrap();
        let (store, probes) = store(data.path(), 1000, Some(180_000_000));
        let volume = volume(Some(1), "/");
        volume.write_rate.store(90_000_000, Ordering::Relaxed);

        probe_once(Arc::clone(&volume), data.path().to_path_buf(), store)().unwrap();

        assert_eq!(rate_of(&volume), 90_000_000);
        assert_eq!(probes.load(Ordering::Relaxed), 0);
    }

    // A sample of real writes lands while the probe is writing: the sample
    // stays, and the probed figure is still stored for the next start.
    #[test]
    fn a_live_rate_set_during_the_probe_is_not_overwritten() {
        let data = tempfile::tempdir().unwrap();
        let volume = volume(Some(1), "/");
        let sampled = Arc::clone(&volume);
        let store = ProbeStore::open_with(
            data.path(),
            Box::new(|| UNIX_EPOCH + Duration::from_secs(1000)),
            Box::new(move |_| {
                sampled.write_rate.store(90_000_000, Ordering::Relaxed);
                Some(180_000_000)
            }),
        );

        probe_once(Arc::clone(&volume), data.path().to_path_buf(), store)().unwrap();
        assert_eq!(rate_of(&volume), 90_000_000);

        let (reopened, probes) = self::store(data.path(), 1000, Some(1));
        let after_restart = self::volume(Some(1), "/");
        assert!(reopened.apply_stored(&after_restart));
        assert_eq!(rate_of(&after_restart), 180_000_000);
        assert_eq!(probes.load(Ordering::Relaxed), 0);
    }

    // A stored figure does not replace a live one either.
    #[test]
    fn a_stored_figure_does_not_replace_a_live_rate() {
        let data = tempfile::tempdir().unwrap();
        let (first, _) = store(data.path(), 1000, Some(180_000_000));
        probe_once(volume(Some(1), "/"), data.path().to_path_buf(), first)().unwrap();

        let (reopened, _) = store(data.path(), 1000, Some(1));
        let volume = volume(Some(1), "/");
        volume.write_rate.store(90_000_000, Ordering::Relaxed);

        assert!(reopened.apply_stored(&volume));
        assert_eq!(rate_of(&volume), 90_000_000);
    }

    // A new store over the same directory is the next start: the volume
    // takes the stored figure and is not probed again.
    #[test]
    fn a_stored_figure_survives_a_restart_without_a_second_probe() {
        let data = tempfile::tempdir().unwrap();
        let (first, first_probes) = store(data.path(), 1000, Some(180_000_000));
        probe_once(volume(Some(1), "/"), data.path().to_path_buf(), first)().unwrap();
        assert_eq!(first_probes.load(Ordering::Relaxed), 1);

        let (second, second_probes) = store(data.path(), 1000 + DAY, Some(5));
        let volume = volume(Some(1), "/");
        probe_once(Arc::clone(&volume), data.path().to_path_buf(), second)().unwrap();

        assert_eq!(rate_of(&volume), 180_000_000);
        assert_eq!(second_probes.load(Ordering::Relaxed), 0);
    }

    // Two volumes each keep their own figure, including two with no device
    // id and one whose label another device also carries.
    #[test]
    fn two_volumes_never_share_a_stored_row() {
        let data = tempfile::tempdir().unwrap();
        let volumes = [
            (volume(Some(1), "/"), 100),
            (volume(Some(2), "/mnt/pool"), 200),
            (volume(None, "/mnt/share"), 300),
            (volume(None, "/mnt/other"), 400),
            (volume(Some(3), "/mnt/pool"), 500),
        ];
        for (volume, rate) in &volumes {
            let (store, _) = store(data.path(), 1000, Some(*rate));
            probe_once(Arc::clone(volume), data.path().to_path_buf(), store)().unwrap();
        }

        let (reopened, probes) = store(data.path(), 1000, None);
        assert_eq!(reopened.lock().len(), volumes.len());
        for (probed, rate) in &volumes {
            let seen_again = Volume::new(probed.device, probed.label.clone());
            assert!(reopened.apply_stored(&seen_again));
            assert_eq!(rate_of(&seen_again), *rate);
        }
        assert_eq!(probes.load(Ordering::Relaxed), 0);
    }

    // The same label on another device is another volume: it has no stored
    // figure and is probed.
    #[test]
    fn a_volume_whose_identity_changed_is_probed_again() {
        let data = tempfile::tempdir().unwrap();
        let (first, _) = store(data.path(), 1000, Some(180_000_000));
        probe_once(volume(Some(1), "/mnt/pool"), data.path().to_path_buf(), first)().unwrap();

        let (second, probes) = store(data.path(), 1000, Some(60_000_000));
        let replaced = volume(Some(9), "/mnt/pool");
        assert!(!second.apply_stored(&replaced));
        probe_once(Arc::clone(&replaced), data.path().to_path_buf(), second)().unwrap();

        assert_eq!(rate_of(&replaced), 60_000_000);
        assert_eq!(probes.load(Ordering::Relaxed), 1);
    }

    // A figure older than seven days is taken again, and writing the new
    // one drops every row that has aged out.
    #[test]
    fn an_old_figure_is_probed_again_and_old_rows_are_dropped() {
        let data = tempfile::tempdir().unwrap();
        let (first, _) = store(data.path(), 1000, Some(180_000_000));
        probe_once(volume(Some(1), "/"), data.path().to_path_buf(), Arc::clone(&first))()
            .unwrap();
        probe_once(volume(Some(2), "/mnt/gone"), data.path().to_path_buf(), first)().unwrap();

        let (later, probes) = store(data.path(), 1000 + 7 * DAY, Some(120_000_000));
        let volume = volume(Some(1), "/");
        probe_once(Arc::clone(&volume), data.path().to_path_buf(), later)().unwrap();
        assert_eq!(rate_of(&volume), 120_000_000);
        assert_eq!(probes.load(Ordering::Relaxed), 1);

        let (reopened, _) = store(data.path(), 1000 + 7 * DAY, None);
        assert_eq!(
            reopened.lock().keys().cloned().collect::<Vec<_>>(),
            vec![volume_key(&volume)]
        );
    }

    // A volume that cannot be probed stays unmeasured, stores nothing, and
    // does not fail the operations queued behind it.
    #[test]
    fn a_probe_that_cannot_write_leaves_the_volume_unmeasured() {
        let data = tempfile::tempdir().unwrap();
        let (store, probes) = store(data.path(), 1000, None);
        let volume = volume(Some(1), "/");

        probe_once(Arc::clone(&volume), data.path().to_path_buf(), store)().unwrap();

        assert_eq!(rate_of(&volume), 0);
        assert_eq!(probes.load(Ordering::Relaxed), 1);
        assert!(!data.path().join(PROBE_RECORD).exists());
    }
}
