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

use tracing::warn;

use super::volume::Volume;
use super::{StorageCapacity, lock_paths, nearest_device};

// Paths remembered before the memory of them starts over. A job asks after
// its own directories, so the count follows the jobs that ran.
const REMEMBERED_PATHS: usize = 1024;

// Paths that may wait to be read at once. One past that is not queued: it
// is asked after again, and queued then.
const UNREAD_PATHS: usize = 256;

// The name of the thread that reads the paths no reading covers.
const READER_THREAD: &str = "weaver-volume-reader";

#[derive(Default)]
pub(super) struct Volumes {
    known: Mutex<Known>,
    unread: Mutex<Unread>,
    // Rung when the reader thread ends.
    #[cfg(test)]
    read: std::sync::Condvar,
}

#[derive(Default)]
struct Known {
    by_path: HashMap<PathBuf, Arc<Volume>>,
    by_device: HashMap<u64, Arc<Volume>>,
    // Volumes of paths the OS gave no device for, by label.
    unnumbered: HashMap<String, Arc<Volume>>,
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
    pub(crate) fn known_volume(&self, path: &Path) -> Option<Arc<Volume>> {
        if let Some(volume) = self.volumes.lock().by_path.get(path) {
            return Some(Arc::clone(volume));
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

    // The device the owner already holds for `path` or the nearest
    // directory above it that it has read, without a disk call.
    fn held_device(&self, path: &Path) -> Option<Option<u64>> {
        let cache = lock_paths(&self.paths);
        path.ancestors().find_map(|at| {
            self.root_device(at).or_else(|| {
                cache
                    .entries
                    .get(at)
                    .and_then(|entry| entry.device.as_ref())
                    .map(|look| look.device)
                    .filter(Option::is_some)
            })
        })
    }
}

impl Known {
    fn remember(&mut self, path: &Path, volume: &Arc<Volume>) {
        if self.by_path.len() >= REMEMBERED_PATHS {
            self.by_path.clear();
        }
        self.by_path.insert(path.to_path_buf(), Arc::clone(volume));
    }
}

impl Volumes {
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
}
