// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A run's volumes: the node's envelope files, read while they grow. A run
// reads verified bytes only: a read waits on the shelf until every byte of
// its range is in, fresh or repaired, and damaged bytes wait for their repair
// as missing ones wait to arrive, so a decoder never builds on bytes known to
// be bad. A read ends short only where a closed volume ends, and fails once
// its bytes are lost, damaged ones nothing will repair included, or the run
// is stopped. A lost read is remembered: the run cannot tell it from its
// own failure, so its owner says why it ended. Nothing here polls: every
// change to the shelf wakes the reads waiting on it.

use std::collections::BTreeMap;
use std::fs::File;
use std::io;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, PoisonError};

use weaver_graph::{ByteRange, VolumeIndex, Volumes};

use crate::pipeline::graph::envelope::Shelf;
use crate::pipeline::graph::leaf::read_exact_at;

#[derive(Debug, thiserror::Error)]
pub(crate) enum Unreadable {
    #[error("the run was stopped")]
    Stopped,
    #[error("{volume} lost {range}: nothing will bring those bytes")]
    Lost {
        volume: VolumeIndex,
        range: ByteRange,
    },
}

impl From<Unreadable> for io::Error {
    fn from(error: Unreadable) -> Self {
        // A stopped run's reads fail for good. `Interrupted` is the one kind
        // every reader retries, a decoder's header parse and `read_exact`
        // included, so a run stopped under one would spin and never return.
        let kind = match error {
            Unreadable::Stopped => io::ErrorKind::BrokenPipe,
            Unreadable::Lost { .. } => io::ErrorKind::NotFound,
        };
        io::Error::new(kind, error)
    }
}

pub(crate) struct RunVolumes {
    shelf: Arc<Shelf>,
    generation: u64,
    // The run's own handles, so a read never holds the shelf's lock while
    // it touches the disk.
    files: Mutex<BTreeMap<u32, File>>,
    lost: AtomicBool,
    stopped: AtomicBool,
}

impl std::fmt::Debug for RunVolumes {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("RunVolumes")
            .field("generation", &self.generation)
            .field("lost", &self.lost())
            .field("stopped", &self.stopped())
            .finish_non_exhaustive()
    }
}

impl RunVolumes {
    pub(crate) fn new(shelf: Arc<Shelf>, generation: u64) -> Self {
        Self {
            shelf,
            generation,
            files: Mutex::default(),
            lost: AtomicBool::new(false),
            stopped: AtomicBool::new(false),
        }
    }

    // The run's owner stops it where it stands: its next read fails, and
    // nothing it says from here on is heard. Set under the shelf's lock, so
    // a run between a look at the shelf and its wait on it is woken too.
    pub(crate) fn stop(&self) {
        self.shelf
            .update(|_| self.stopped.store(true, Ordering::Release));
    }

    fn stopped(&self) -> bool {
        self.stopped.load(Ordering::Acquire)
    }

    // Whether a read failed because nothing will bring its bytes.
    pub(crate) fn lost(&self) -> bool {
        self.lost.load(Ordering::Acquire)
    }

    // Whether the run may still speak: nothing has stopped it.
    pub(crate) fn current(&self) -> bool {
        !self.stopped() && self.shelf.generation() == self.generation
    }

    // Wait until `range` of `volume` can be read, as far as a closed volume
    // lets it go, and mark it read. Returns how many bytes to read, and the
    // file they are in.
    fn wait_for(
        &self,
        volume: VolumeIndex,
        range: ByteRange,
    ) -> Result<(u64, Option<std::path::PathBuf>), Unreadable> {
        let mut state = self.shelf.lock();
        loop {
            if state.generation != self.generation || self.stopped() {
                return Err(Unreadable::Stopped);
            }
            let complete = state.complete;
            let known = state.volumes.contains(&volume.0);
            let kept = state.kept.get_mut(&volume.0);
            let end = kept
                .as_ref()
                .and_then(|kept| kept.closed)
                .map_or(range.end, |closed| range.end.min(closed));
            let want = ByteRange::new(range.start.min(end), end);
            if want.is_empty() {
                return Ok((0, None));
            }
            match kept {
                Some(kept) if kept.coverage.verified(want) => {
                    kept.reached = kept.reached.max(want.end);
                    return Ok((want.len(), Some(kept.path().to_path_buf())));
                }
                Some(kept) if kept.settled => {
                    return Err(Unreadable::Lost {
                        volume,
                        range: want,
                    });
                }
                None if complete && !known => {
                    return Err(Unreadable::Lost {
                        volume,
                        range: want,
                    });
                }
                _ => {}
            }
            state = self.shelf.wait(state);
        }
    }
}

impl Volumes for RunVolumes {
    fn next(&self, after: Option<VolumeIndex>) -> io::Result<Option<VolumeIndex>> {
        let Some(from) = after.map_or(Some(0), |after| after.0.checked_add(1)) else {
            return Ok(None);
        };
        let mut state = self.shelf.lock();
        loop {
            if state.generation != self.generation || self.stopped() {
                return Err(Unreadable::Stopped.into());
            }
            // No index lies between `after` and `from`, so only a gap past
            // `from` can still fill, and only while a volume can still join
            // there. Past the last volume bound, one joins only from a member
            // beside them still deciding its owner.
            match state.volumes.range(from..).next() {
                Some(&next) if next == from || state.complete => {
                    return Ok(Some(VolumeIndex(next)));
                }
                None if !state.deciding => return Ok(None),
                _ => {}
            }
            state = self.shelf.wait(state);
        }
    }

    fn len(&self, volume: VolumeIndex) -> Option<u64> {
        self.shelf.lock().kept.get(&volume.0)?.closed
    }

    fn read_at(&self, volume: VolumeIndex, offset: u64, into: &mut [u8]) -> io::Result<usize> {
        let range = ByteRange::at(offset, into.len() as u64);
        let (len, path) = self.wait_for(volume, range).inspect_err(|error| {
            if matches!(error, Unreadable::Lost { .. }) {
                self.lost.store(true, Ordering::Release);
            }
        })?;
        let Some(path) = path else {
            return Ok(0);
        };
        let len = usize::try_from(len).expect("a read fits the buffer it fills");
        let mut files = self.files.lock().unwrap_or_else(PoisonError::into_inner);
        let file = match files.entry(volume.0) {
            std::collections::btree_map::Entry::Occupied(found) => found.into_mut(),
            std::collections::btree_map::Entry::Vacant(slot) => slot.insert(File::open(path)?),
        };
        read_exact_at(file, offset, &mut into[..len])?;
        Ok(len)
    }
}
