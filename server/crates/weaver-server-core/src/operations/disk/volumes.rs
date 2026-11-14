// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The volumes the disk owner knows: one `Volume` per device id, found from
// a path.
//
// Asking after a path's volume never looks at a disk. A path asked after
// before answers from memory, and so does one under a root or a path whose
// device the owner already read: every thread of the owner's that reads a
// device makes its volume known before it hands the device out. A path no
// reading covers is read by a thread of this file's own, started for it and
// ended when nothing is left to read; until that returns the asker is given
// the volume of an unknown device at the path's root.

use std::collections::{HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

use tracing::warn;

use super::volume::Volume;
use super::{StorageCapacity, lock_paths, nearest_device};

// Paths remembered at once. A job asks after its own directories, so the
// count follows the jobs that ran. One more path than this drops the path
// asked after longest ago, and no other: a path still asked after keeps
// its volume.
const REMEMBERED_PATHS: usize = 1024;

// Paths that may wait to be read at once. One past that is not queued: it
// is asked after again, and queued then.
const UNREAD_PATHS: usize = 256;

// How long a path whose device could not be read keeps that answer before
// an ask has it read again; also how long the slowest write rate keeps the
// volumes of paths not read yet (`StorageCapacity::slowest_write_mbps`).
pub(super) const UNREAD_RETRY: Duration = Duration::from_secs(5);

// The name of the thread that reads the paths no reading covers.
const READER_THREAD: &str = "weaver-volume-reader";

#[derive(Default)]
pub(super) struct Volumes {
    known: Mutex<Known>,
    unread: Mutex<Unread>,
    // The volumes the slowest write rate is read from, found once for each
    // `Known::generation` (`StorageCapacity::slowest_write_mbps`).
    pub(super) rate_volumes: Mutex<Option<super::volume_view::RateVolumes>>,
    // Rung when the reader thread ends.
    #[cfg(test)]
    read: std::sync::Condvar,
}

#[derive(Default)]
struct Known {
    by_path: HashMap<PathBuf, Remembered>,
    // Counts asks, so the path asked after longest ago is told apart.
    asks: u64,
    // The configured folders a delivery can land under besides the roots,
    // each category's, as last told, and the device each was read on:
    // `None` until its read returns, and when it could not be read. A
    // folder may be a mount of its own inside a root, so nothing under it
    // is answered from a directory above it.
    folders: HashMap<PathBuf, Option<u64>>,
    told: Vec<PathBuf>,
    by_device: HashMap<u64, Arc<Volume>>,
    // Volumes of paths the OS gave no device for, by label.
    unnumbered: HashMap<String, Arc<Volume>>,
    // Moves whenever a path may be answered another volume than before: a
    // path remembered anew or for another volume, or the folders told
    // changed. What is held on a path's answer is good while it stands.
    generation: u64,
}

struct Remembered {
    volume: Arc<Volume>,
    // The ask count at the latest ask.
    asked: u64,
    // For a path whose device could not be read: when an ask has it read
    // again. `None` for a path whose device is known, which no ask reads.
    retry_at: Option<Instant>,
}

// The paths waiting for the reader thread, and whether it runs.
#[derive(Default)]
struct Unread {
    paths: HashSet<PathBuf>,
    // The path the reader thread is reading now.
    reading: Option<PathBuf>,
    running: bool,
}

impl StorageCapacity {
    // The volume `path` lies on, from memory alone: no disk call is ever
    // made here, so the pipeline loop may ask. A path memory does not cover
    // is queued to be read off this thread, and is known from then on;
    // until it is, the answer is the volume of an unknown device at the
    // path's root, which takes writes and has no measured rate.
    pub(crate) fn volume(&self, path: &Path) -> Arc<Volume> {
        if let Some(volume) = self.known_volume(path) {
            return volume;
        }
        Volumes::read_later(&self.volumes, path);
        self.volumes.unread_volume(path)
    }

    // The volume `path` lies on, read on this thread when memory does not
    // cover it. For a thread that may wait on a disk; never for the
    // pipeline loop or the async runtime.
    pub(crate) fn read_volume(&self, path: &Path) -> Arc<Volume> {
        self.known_volume(path)
            .unwrap_or_else(|| self.volumes.read(path))
    }

    // The volume `path` lies on when memory alone can say: the path was
    // asked after before, or a root or a reading above it gives its device
    // and that device's volume is known. Never a disk call.
    // A path whose device could not be read is read again, off this thread,
    // by the first ask `UNREAD_RETRY` after the last read.
    pub(crate) fn known_volume(&self, path: &Path) -> Option<Arc<Volume>> {
        let recalled = self.volumes.lock().recall(path);
        if let Some((volume, retry)) = recalled {
            if retry {
                Volumes::read_later(&self.volumes, path);
            }
            return Some(volume);
        }
        let device = self.held_device(path)??;
        let mut known = self.volumes.lock();
        let volume = Arc::clone(known.by_device.get(&device)?);
        known.remember(path, &volume);
        Some(volume)
    }

    // Every volume asked after so far.
    pub(crate) fn volumes(&self) -> Vec<Arc<Volume>> {
        let known = self.volumes.lock();
        known
            .by_device
            .values()
            .chain(known.unnumbered.values())
            .map(Arc::clone)
            .collect()
    }

    // The configured folders besides the roots, each category's, as they
    // stand now: told at start-up and whenever they may have changed. Never
    // a disk call: a folder not told before has its device read off this
    // thread, and until that returns nothing under it is answered from a
    // directory above it. Telling the same folders again does nothing.
    pub(crate) fn set_folders(&self, folders: &[PathBuf]) {
        let unread: Vec<PathBuf> = {
            let mut known = self.volumes.lock();
            if known.told == folders {
                return;
            }
            let mut next: HashMap<PathBuf, Option<u64>> = HashMap::with_capacity(folders.len());
            let mut unread = Vec::new();
            for folder in folders {
                match known.folders.get(folder) {
                    Some(device) => {
                        next.insert(folder.clone(), *device);
                    }
                    None if !next.contains_key(folder) => {
                        next.insert(folder.clone(), None);
                        unread.push(folder.clone());
                    }
                    None => {}
                }
            }
            // What was remembered under a folder that came or went was
            // answered by another rule: it is asked anew.
            let changed: Vec<PathBuf> = known
                .folders
                .keys()
                .filter(|folder| !next.contains_key(*folder))
                .chain(unread.iter())
                .cloned()
                .collect();
            known
                .by_path
                .retain(|path, _| !changed.iter().any(|folder| path.starts_with(folder)));
            known.folders = next;
            known.told = folders.to_vec();
            known.generation += 1;
            unread
        };
        for folder in unread {
            Volumes::read_later(&self.volumes, &folder);
        }
    }

    // The device the owner already holds for `path` or the nearest
    // directory above it that it has read, without a disk call. A root or
    // a configured folder on the way up answers for everything under it,
    // and answers nothing while its own device is not read: it may be a
    // mount of its own, so the directory above it does not speak for it.
    fn held_device(&self, path: &Path) -> Option<Option<u64>> {
        let folder = {
            let known = self.volumes.lock();
            match known.folders.is_empty() {
                true => None,
                false => path
                    .ancestors()
                    .find_map(|at| known.folders.get(at).map(|device| (at, *device))),
            }
        };
        let cache = lock_paths(&self.paths);
        for at in path.ancestors() {
            if let Some((_, device)) = folder.filter(|(folder, _)| *folder == at) {
                return Some(device);
            }
            if let Some(device) = self.root_device_at(at) {
                return Some(device);
            }
            let looked = cache
                .entries
                .get(at)
                .and_then(|entry| entry.device.as_ref())
                .map(|look| look.device)
                .filter(Option::is_some);
            if looked.is_some() {
                return looked;
            }
        }
        None
    }
}

impl Known {
    // The volume remembered for `path`, which is asked after now, and
    // whether its device, which could not be read, is due to be read again.
    // A path whose device is known reads no clock.
    fn recall(&mut self, path: &Path) -> Option<(Arc<Volume>, bool)> {
        self.asks += 1;
        let asked = self.asks;
        let remembered = self.by_path.get_mut(path)?;
        remembered.asked = asked;
        let due = remembered
            .retry_at
            .map(|at| (at, Instant::now()))
            .filter(|(at, now)| now >= at);
        if let Some((_, now)) = due {
            remembered.retry_at = Some(now + UNREAD_RETRY);
        }
        let retry = due.is_some();
        Some((Arc::clone(&remembered.volume), retry))
    }

    // Remember `path`'s volume. Past the cap the one path asked after
    // longest ago is dropped for it, and no other.
    fn remember(&mut self, path: &Path, volume: &Arc<Volume>) {
        self.asks += 1;
        let asked = self.asks;
        if let Some(device) = self.folders.get_mut(path) {
            *device = volume.device;
        }
        if self
            .by_path
            .get(path)
            .is_none_or(|remembered| !Arc::ptr_eq(&remembered.volume, volume))
        {
            self.generation += 1;
        }
        if !self.by_path.contains_key(path)
            && self.by_path.len() >= REMEMBERED_PATHS
            && let Some(oldest) = self
                .by_path
                .iter()
                .min_by_key(|(_, remembered)| remembered.asked)
                .map(|(path, _)| path.clone())
        {
            self.by_path.remove(&oldest);
        }
        self.by_path.insert(
            path.to_path_buf(),
            Remembered {
                volume: Arc::clone(volume),
                asked,
                retry_at: volume
                    .device
                    .is_none()
                    .then(|| Instant::now() + UNREAD_RETRY),
            },
        );
    }
}

impl Volumes {
    // See `Known::generation`.
    pub(super) fn generation(&self) -> u64 {
        self.lock().generation
    }

    fn lock(&self) -> std::sync::MutexGuard<'_, Known> {
        self.known.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn unread(&self) -> std::sync::MutexGuard<'_, Unread> {
        self.unread.lock().unwrap_or_else(PoisonError::into_inner)
    }

    // Make the volume of `device` known, which a thread of the owner's just
    // read at `path`. A device that could not be read makes nothing known.
    // May look at the disk for the volume's name.
    pub(super) fn seen(&self, device: Option<u64>, path: &Path) {
        if device.is_some() {
            self.named(device, path);
        }
    }

    // Read the volume `path` lies on, on this thread.
    fn read(&self, path: &Path) -> Arc<Volume> {
        self.named(nearest_device(path), path)
    }

    // The volume of `device`, which was read at `path`, named the first
    // time it is seen. The name is read from the disk, so it is read before
    // the lock is taken; another caller that named the volume first keeps
    // its own.
    fn named(&self, device: Option<u64>, path: &Path) -> Arc<Volume> {
        if let Some(device) = device {
            let mut known = self.lock();
            if let Some(volume) = known.by_device.get(&device).map(Arc::clone) {
                known.remember(path, &volume);
                return volume;
            }
        }
        let label = label(path);
        let mut known = self.lock();
        let volume = match device {
            Some(device) => Arc::clone(
                known
                    .by_device
                    .entry(device)
                    .or_insert_with(|| Arc::new(Volume::new(Some(device), label))),
            ),
            None => Arc::clone(
                known
                    .unnumbered
                    .entry(label.clone())
                    .or_insert_with(|| Arc::new(Volume::new(None, label))),
            ),
        };
        known.remember(path, &volume);
        volume
    }

    // The volume answered for a path not read yet: the one of an unknown
    // device at the path's root. Not remembered for the path, so the path
    // answers its own volume once it is read.
    fn unread_volume(&self, path: &Path) -> Arc<Volume> {
        let label = super::volume_label::unread_label(path);
        Arc::clone(
            self.lock()
                .unnumbered
                .entry(label.clone())
                .or_insert_with(|| Arc::new(Volume::new(None, label))),
        )
    }

    // Have `path` read off this thread. The reader thread is started when
    // it does not run, and ends when nothing is left to read; a path
    // already waiting or being read is not queued again. When the thread cannot be
    // started the path stays queued, and the next ask tries again.
    fn read_later(volumes: &Arc<Self>, path: &Path) {
        {
            let mut unread = volumes.unread();
            if unread.reading.as_deref() == Some(path) {
                return;
            }
            if !unread.paths.contains(path) {
                if unread.paths.len() >= UNREAD_PATHS {
                    return;
                }
                unread.paths.insert(path.to_path_buf());
            }
            if unread.running {
                return;
            }
            unread.running = true;
        }
        let reader = Arc::clone(volumes);
        let spawned = std::thread::Builder::new()
            .name(READER_THREAD.to_string())
            .spawn(move || reader.read_unread());
        if let Err(error) = spawned {
            volumes.unread().running = false;
            warn!(error = %error, "could not start the thread that reads a path's volume; its device stays unknown until the next ask");
        }
    }

    // The reader thread: read each waiting path, and end when none waits.
    fn read_unread(&self) {
        // A read that panics still marks the thread ended, so a later ask
        // starts another.
        struct Ended<'a>(&'a Volumes, bool);
        impl Drop for Ended<'_> {
            fn drop(&mut self) {
                if self.1 {
                    return;
                }
                let mut unread = self.0.unread();
                unread.reading = None;
                unread.running = false;
                #[cfg(test)]
                self.0.read.notify_all();
            }
        }
        let mut ended = Ended(self, false);
        loop {
            let path = {
                let mut unread = self.unread();
                unread.reading = None;
                let Some(path) = unread.paths.iter().next().cloned() else {
                    // Decided with the queue locked, where an asker queues
                    // a path: none is left behind as the thread goes.
                    unread.running = false;
                    ended.1 = true;
                    #[cfg(test)]
                    self.read.notify_all();
                    return;
                };
                unread.paths.remove(&path);
                unread.reading = Some(path.clone());
                path
            };
            self.read(&path);
        }
    }

    // Have every path whose device could not be read, and the volumes held
    // for the slowest write rate, read again on the next ask, as
    // `UNREAD_RETRY` after the last read would.
    #[cfg(test)]
    fn age_retries(&self) {
        let now = Instant::now();
        for remembered in self.lock().by_path.values_mut() {
            if remembered.retry_at.is_some() {
                remembered.retry_at = Some(now);
            }
        }
        if let Some(rates) = self
            .rate_volumes
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .as_mut()
        {
            rates.age();
        }
    }

    // Wait until nothing waits to be read and the reader thread has ended.
    #[cfg(test)]
    pub(super) fn wait_read(&self) {
        let mut unread = self.unread();
        while unread.running || !unread.paths.is_empty() {
            unread = self
                .read
                .wait(unread)
                .unwrap_or_else(PoisonError::into_inner);
        }
    }
}

fn label(path: &Path) -> String {
    super::volume_label::volume_label(path)
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::Ordering;
    use std::time::Duration;

    use super::super::{DISK_LOOKS, DiskSpace, StorageRoot};
    use super::*;
    use crate::runtime::fs::test_devices;

    const NEVER: Duration = Duration::from_secs(365 * 24 * 60 * 60);

    // An owner whose roots lie under `dir`, each read once and not again.
    fn owner(dir: &Path) -> StorageCapacity {
        let storage = StorageCapacity::with_probe(
            [dir.join("data"), dir.join("intermediate"), dir.join("complete")],
            NEVER,
            Arc::new(|_| {
                Ok(DiskSpace {
                    total_bytes: 1,
                    available_bytes: 1,
                })
            }),
        );
        for root in StorageRoot::ALL {
            storage.wait_for_probes(root, 1);
        }
        storage
    }

    fn looks() -> u64 {
        DISK_LOOKS.load(Ordering::Relaxed)
    }

    // Whether a path waits to be read or the reader thread runs.
    fn reading(storage: &StorageCapacity) -> bool {
        let unread = storage.volumes.unread();
        unread.running || !unread.paths.is_empty()
    }

    // A path no reading covers is answered at once, with no look at a disk
    // by the asker: the look is the reader thread's, made once, and from
    // its return the path answers its own volume from memory.
    #[test]
    fn a_path_no_reading_covers_is_read_off_the_asking_thread() {
        let dir = tempfile::tempdir().unwrap();
        let elsewhere = tempfile::tempdir().unwrap();
        let asked = elsewhere.path().join("job");
        test_devices::assign(&asked, 77);
        // The device read of the path stands until it is let go.
        let (release, mut entered) = test_devices::hold(&asked);
        let storage = owner(dir.path());
        let before = looks();

        let meanwhile = storage.volume(&asked);

        assert_eq!(meanwhile.device, None);
        assert!(meanwhile.label.ends_with("(device unknown)"), "{}", meanwhile.label);
        // The read is out, on another thread, while the asker has its answer.
        assert_eq!(entered.blocking_recv().as_deref(), Some(asked.as_path()));
        // Asked again meanwhile: the same answer, and the path is not
        // queued a second time.
        assert!(Arc::ptr_eq(&storage.volume(&asked), &meanwhile));
        assert!(storage.volumes.unread().paths.is_empty());
        assert!(storage.known_volume(&asked).is_none());
        drop(release);
        storage.volumes.wait_read();

        assert_eq!(looks(), before + 1, "one look, the reader thread's");
        let read = storage.volume(&asked);
        assert_eq!(read.device, Some(77));
        assert!(Arc::ptr_eq(&storage.volume(&asked), &read));
        assert!(Arc::ptr_eq(&storage.read_volume(&asked), &read));
        assert_eq!(looks(), before + 1, "known from memory from then on");
        assert!(entered.try_recv().is_err(), "read once");
        assert!(!reading(&storage));
    }

    // A job's directories lie under the roots, whose own threads read their
    // devices and make their volumes known: such a path is answered from
    // memory the first time it is asked after. The data root, which no job
    // writes under, makes no volume known.
    #[test]
    fn a_directory_under_a_root_is_answered_from_what_the_roots_thread_read() {
        let dir = tempfile::tempdir().unwrap();
        test_devices::assign(&dir.path().join("intermediate"), 5);
        test_devices::assign(&dir.path().join("complete"), 6);
        let storage = owner(dir.path());
        let before = looks();

        let working = storage.volume(&dir.path().join("intermediate").join("job-1"));
        let output = storage.volume(&dir.path().join("complete").join("movies").join("job-1"));

        assert_eq!((working.device, output.device), (Some(5), Some(6)));
        assert_eq!(looks(), before, "no look");
        assert!(!reading(&storage), "nothing was left to read");
        assert_eq!(storage.volumes().len(), 2);
    }

    // A destination outside the roots has its device looked at before its
    // job is admitted. That look makes the volume known, so the job's
    // directory there is answered from memory.
    #[tokio::test(start_paused = true)]
    async fn a_look_at_a_destinations_device_makes_its_volume_known() {
        let dir = tempfile::tempdir().unwrap();
        let intermediate = dir.path().join("intermediate");
        let shows = PathBuf::from("/volumes-known-by-a-look/shows");
        test_devices::assign(&intermediate, 5);
        test_devices::assign(&shows, 9);
        let storage = owner(dir.path());
        assert!(!storage.same_filesystem(&intermediate, &shows).await);
        // The look reads on after it tells the device: it is waited for
        // before the looks are counted.
        storage.looks_returned().await;
        let before = looks();

        let output = storage.volume(&shows.join(".weaver-staging").join("7"));

        assert_eq!(output.device, Some(9));
        assert_eq!(looks(), before, "no look");
        assert!(!reading(&storage), "nothing was left to read");
    }

    // A thread that may wait on a disk reads the path itself, once, and no
    // other thread is started for it.
    #[test]
    fn a_thread_that_may_wait_reads_the_volume_itself() {
        let dir = tempfile::tempdir().unwrap();
        let elsewhere = tempfile::tempdir().unwrap();
        let asked = elsewhere.path().join("job");
        test_devices::assign(&asked, 12);
        let storage = owner(dir.path());
        let before = looks();

        let read = storage.read_volume(&asked);

        assert_eq!(read.device, Some(12));
        assert_eq!(looks(), before + 1);
        assert!(!reading(&storage));
        assert!(Arc::ptr_eq(&storage.volume(&asked), &read));
        assert!(Arc::ptr_eq(&storage.read_volume(&asked), &read));
        assert_eq!(looks(), before + 1, "known from memory from then on");
    }

    // A category's folder that is a mount of its own inside the complete
    // directory: once the owner is told of it, nothing under it is answered
    // from the complete directory's device, before its own device is read
    // or after.
    #[test]
    fn a_configured_folder_inside_a_root_is_its_own_volume() {
        let dir = tempfile::tempdir().unwrap();
        let complete = dir.path().join("complete");
        let shows = complete.join("shows");
        test_devices::assign(&complete, 6);
        test_devices::assign(&shows, 8);
        let storage = owner(dir.path());
        let job = shows.join("job-1");
        // Not told of yet, the folder is a directory of the root's.
        assert_eq!(storage.volume(&job).device, Some(6));
        let (release, mut entered) = test_devices::hold(&shows);
        let before = looks();

        storage.set_folders(std::slice::from_ref(&shows));

        // The folder's read is out on the reader thread. Until it returns
        // the job's directory under it has no known device, and the root's
        // is not handed out for it.
        assert_eq!(entered.blocking_recv().as_deref(), Some(shows.as_path()));
        assert!(storage.known_volume(&job).is_none());
        assert!(storage.known_volume(&shows).is_none());
        // A directory of the root's beside the folder is still the root's.
        assert_eq!(storage.volume(&complete.join("films").join("job-2")).device, Some(6));
        drop(release);
        storage.volumes.wait_read();
        let after = looks();
        assert!(after > before);

        let read = storage.volume(&job);
        assert_eq!(read.device, Some(8));
        assert!(Arc::ptr_eq(&storage.volume(&shows.join("job-3")), &read));
        assert!(Arc::ptr_eq(&storage.read_volume(&job), &read));
        // Told the same folders again: nothing is read again.
        storage.set_folders(std::slice::from_ref(&shows));
        assert!(!reading(&storage));
        assert_eq!(looks(), after, "from memory from then on");

        // The folder is a category's no longer: it is the root's again.
        storage.set_folders(&[]);
        assert_eq!(storage.volume(&job).device, Some(6));
    }

    // A root whose own device is not read answers nothing for what lies
    // under it from a root above it.
    #[test]
    fn a_root_inside_another_does_not_take_the_outer_roots_device() {
        let dir = tempfile::tempdir().unwrap();
        let data = dir.path().join("data");
        test_devices::assign(&data, 3);
        let storage = owner(dir.path());
        let inner = data.join("intermediate");
        test_devices::assign(&inner, 4);
        let (release, mut entered) = test_devices::hold(&inner);
        storage.retarget(StorageRoot::Intermediate, inner.clone());
        assert_eq!(entered.blocking_recv().as_deref(), Some(inner.as_path()));
        assert!(storage.known_volume(&inner.join("job-1")).is_none());
        drop(release);
        storage.reread_roots();
        assert_eq!(storage.volume(&inner.join("job-1")).device, Some(4));
    }

    // Past the cap one path is dropped for each new one, the one asked
    // after longest ago: a path still asked after keeps the volume that
    // was read for it, and is not read again.
    #[test]
    fn a_path_still_asked_after_outlives_the_cap_on_remembered_paths() {
        let dir = tempfile::tempdir().unwrap();
        let elsewhere = tempfile::tempdir().unwrap();
        let live = elsewhere.path().join("job");
        test_devices::assign(&live, 12);
        test_devices::assign(&dir.path().join("intermediate"), 5);
        let storage = owner(dir.path());
        let read = storage.read_volume(&live);
        let before = looks();
        let under = |index: usize| dir.path().join("intermediate").join(format!("job-{index}"));

        for index in 0..REMEMBERED_PATHS - 1 {
            storage.volume(&under(index));
        }
        assert_eq!(storage.volumes.lock().by_path.len(), REMEMBERED_PATHS);
        // Asked after again, the path is no longer the oldest.
        assert!(Arc::ptr_eq(&storage.volume(&live), &read));
        for index in REMEMBERED_PATHS..REMEMBERED_PATHS + 8 {
            storage.volume(&under(index));
        }

        let known = storage.volumes.lock();
        assert_eq!(known.by_path.len(), REMEMBERED_PATHS, "one out for each one in");
        assert!(known.by_path.contains_key(&live));
        assert!(!known.by_path.contains_key(&under(0)), "the oldest went");
        assert!(known.by_path.contains_key(&under(REMEMBERED_PATHS - 2)));
        drop(known);
        assert!(Arc::ptr_eq(&storage.volume(&live), &read));
        assert_eq!(looks(), before, "nothing was read again");
        assert!(!reading(&storage));
    }

    // The slowest write rate is read from the volumes found the turn before
    // while no path's answer changed: a rate published since is in it at
    // once, and a folder told, or read, is found anew.
    #[test]
    fn the_slowest_write_rate_follows_rates_and_folders_without_a_path_lookup() {
        let dir = tempfile::tempdir().unwrap();
        let complete = dir.path().join("complete");
        let intermediate = dir.path().join("intermediate");
        let shows = complete.join("shows");
        test_devices::assign(&complete, 6);
        test_devices::assign(&intermediate, 6);
        test_devices::assign(&shows, 8);
        let storage = owner(dir.path());
        let roots = [intermediate.as_path(), complete.as_path()];
        let root_volume = storage.volume(&complete);
        root_volume.write_rate.store(400_000_000, Ordering::Relaxed);
        assert_eq!(storage.slowest_write_mbps(&roots, &[]), 400.0);
        let generation = storage.volumes.generation();

        root_volume.write_rate.store(300_000_000, Ordering::Relaxed);
        assert_eq!(storage.slowest_write_mbps(&roots, &[]), 300.0, "a new sample is read");
        assert_eq!(storage.volumes.generation(), generation, "no path was looked up again");

        let folders = [shows.clone()];
        storage.set_folders(&folders);
        storage.volumes.wait_read();
        let shows_volume = storage.volume(&shows);
        assert_eq!(shows_volume.device, Some(8));
        shows_volume.write_rate.store(90_000_000, Ordering::Relaxed);
        assert_eq!(storage.slowest_write_mbps(&roots, &folders), 90.0, "the folder's volume is found");

        storage.set_folders(&[]);
        assert_eq!(storage.slowest_write_mbps(&roots, &[]), 300.0, "the folder left");
    }

    // A path whose device could not be read keeps that answer for a while,
    // read no more meanwhile; the first ask after it has the path read
    // again, and the device read then is known from then on.
    #[test]
    fn a_path_whose_device_could_not_be_read_is_read_again_later() {
        let dir = tempfile::tempdir().unwrap();
        let storage = owner(dir.path());
        // No ancestor of a relative path that does not exist is a
        // directory: its device cannot be read.
        let asked = PathBuf::from("weaver-volumes-test-unreadable").join("job");
        storage.volume(&asked);
        storage.volumes.wait_read();
        let failed = storage.volume(&asked);
        assert_eq!(failed.device, None);
        let before = looks();
        assert!(Arc::ptr_eq(&storage.volume(&asked), &failed));
        assert!(!reading(&storage), "not read again at once");
        assert_eq!(looks(), before);

        test_devices::assign(&asked, 61);
        let generation = storage.volumes.generation();
        storage.volumes.age_retries();
        assert!(Arc::ptr_eq(&storage.volume(&asked), &failed), "answered meanwhile");
        storage.volumes.wait_read();

        assert!(storage.volumes.generation() > generation, "the read that landed moved it");
        let read = storage.volume(&asked);
        assert_eq!(read.device, Some(61));
        let after = looks();
        storage.volumes.age_retries();
        assert!(Arc::ptr_eq(&storage.volume(&asked), &read));
        assert!(!reading(&storage), "a known device is not read again");
        assert_eq!(looks(), after);
    }

    // A path the slowest write rate asks after whose read could not be
    // queued is asked after again on a later turn, and its volume's rate is
    // in the answer once the read lands. Settled, the turn holds no clock.
    #[test]
    fn the_slowest_write_rate_asks_again_after_a_path_it_could_not_have_read() {
        let dir = tempfile::tempdir().unwrap();
        let elsewhere = tempfile::tempdir().unwrap();
        let intermediate = dir.path().join("intermediate");
        let asked = elsewhere.path().join("downloads");
        test_devices::assign(&intermediate, 5);
        test_devices::assign(&asked, 62);
        let storage = owner(dir.path());
        storage
            .volume(&intermediate)
            .write_rate
            .store(400_000_000, Ordering::Relaxed);
        let roots = [intermediate.as_path(), asked.as_path()];
        // The queue of paths to read is full.
        {
            let mut unread = storage.volumes.unread();
            unread.running = true;
            for index in 0..UNREAD_PATHS {
                unread.paths.insert(dir.path().join(format!("queued-{index}")));
            }
        }
        assert_eq!(storage.slowest_write_mbps(&roots, &[]), 400.0);
        assert!(!storage.volumes.unread().paths.contains(&asked));
        {
            let mut unread = storage.volumes.unread();
            unread.paths.clear();
            unread.running = false;
        }
        // Before the retry is due, the turn asks after no path.
        assert_eq!(storage.slowest_write_mbps(&roots, &[]), 400.0);
        assert!(!reading(&storage));

        storage.volumes.age_retries();
        assert_eq!(storage.slowest_write_mbps(&roots, &[]), 400.0);
        storage.volumes.wait_read();
        let read = storage.volume(&asked);
        assert_eq!(read.device, Some(62));
        read.write_rate.store(50_000_000, Ordering::Relaxed);

        assert_eq!(storage.slowest_write_mbps(&roots, &[]), 50.0, "the read volume is found");
        let rates = storage.volumes.rate_volumes.lock().unwrap();
        assert!(rates.as_ref().unwrap().settled());
    }
}
