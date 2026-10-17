// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A run's volumes: the node's envelope files, read while they grow. A read
// waits on the shelf until its range is in, damaged bytes counting as in, since
// the engine's own checks catch them. It ends short only where a closed volume
// ends, and fails once its bytes are lost or the run is stopped. Nothing here
// polls: every change to the shelf wakes the reads waiting on it.

use std::collections::BTreeMap;
use std::fs::File;
use std::io;
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
        let kind = match error {
            Unreadable::Stopped => io::ErrorKind::Interrupted,
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
}

impl RunVolumes {
    pub(crate) fn new(shelf: Arc<Shelf>, generation: u64) -> Self {
        Self {
            shelf,
            generation,
            files: Mutex::default(),
        }
    }

    // Whether the run may still speak: nothing has stopped it.
    pub(crate) fn current(&self) -> bool {
        self.shelf.generation() == self.generation
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
            if state.generation != self.generation {
                return Err(Unreadable::Stopped);
            }
            let complete = state.complete;
            let known = volume.0 < state.volumes;
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
                Some(kept) if kept.coverage.covered(want) => {
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
    fn count(&self) -> usize {
        self.shelf.lock().volumes as usize
    }

    fn len(&self, volume: VolumeIndex) -> Option<u64> {
        self.shelf.lock().kept.get(&volume.0)?.closed
    }

    fn read_at(&self, volume: VolumeIndex, offset: u64, into: &mut [u8]) -> io::Result<usize> {
        let range = ByteRange::at(offset, into.len() as u64);
        let (len, path) = self.wait_for(volume, range)?;
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
