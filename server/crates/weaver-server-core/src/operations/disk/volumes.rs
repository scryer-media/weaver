// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The volumes the disk owner knows: one `Volume` per device id, found from
// a path. A path asked after before answers from memory, and so does one
// under a root or a path whose device the owner already read; only a path
// no reading covers is looked at on disk, once.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, PoisonError};

use super::volume::Volume;
use super::{StorageCapacity, lock_paths, nearest_device};

// Paths remembered before the memory of them starts over. A job asks after
// its own directories, so the count follows the jobs that ran.
const REMEMBERED_PATHS: usize = 1024;

#[derive(Default)]
pub(super) struct Volumes {
    known: Mutex<Known>,
}

#[derive(Default)]
struct Known {
    by_path: HashMap<PathBuf, Arc<Volume>>,
    by_device: HashMap<u64, Arc<Volume>>,
    // Volumes of paths the OS gave no device for, by label.
    unnumbered: HashMap<String, Arc<Volume>>,
}

impl StorageCapacity {
    // The volume `path` lies on. No disk call is made when the device is
    // already known: from an earlier ask, from a root the path lies under,
    // or from a reading the owner holds of the path or a directory above it.
    pub(crate) fn volume(&self, path: &Path) -> Arc<Volume> {
        if let Some(volume) = self.volumes.lock().by_path.get(path) {
            return Arc::clone(volume);
        }
        let device = self
            .held_device(path)
            .unwrap_or_else(|| nearest_device(path));
        let mut known = self.volumes.lock();
        let volume = match device {
            Some(device) => Arc::clone(
                known
                    .by_device
                    .entry(device)
                    .or_insert_with(|| Arc::new(Volume::new(Some(device), label(path)))),
            ),
            None => {
                let label = label(path);
                Arc::clone(
                    known
                        .unnumbered
                        .entry(label.clone())
                        .or_insert_with(|| Arc::new(Volume::new(None, label))),
                )
            }
        };
        if known.by_path.len() >= REMEMBERED_PATHS {
            known.by_path.clear();
        }
        known.by_path.insert(path.to_path_buf(), Arc::clone(&volume));
        volume
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

impl Volumes {
    fn lock(&self) -> std::sync::MutexGuard<'_, Known> {
        self.known.lock().unwrap_or_else(PoisonError::into_inner)
    }
}

fn label(path: &Path) -> String {
    super::volume_label::volume_label(path)
}
