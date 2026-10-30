// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A run's volumes: the node's envelope files, read while they grow. A run
// reads verified bytes only: a read waits on the shelf until every byte of
// its range is in, fresh or repaired, and damaged bytes wait for their repair
// as missing ones wait to arrive, so a decoder never builds on bytes known to
// be bad. A read ends short where a closed volume ends, or where bytes that
// will never come begin once its volume settles; one that starts on those
// fails with the span that is lost, damaged bytes nothing will repair
// included, as one fails once the run is stopped. A lost read is remembered
// until the run says it read on past the span: a run that cannot tell it
// from its own failure leaves it to its owner to say why it ended. Nothing
// here polls: every change to the shelf wakes the reads waiting on it.

use std::collections::{BTreeMap, BTreeSet};
use std::fs::File;
use std::io;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, PoisonError};

use weaver_graph::{ByteRange, Coverage, Lost, VolumeIndex, Volumes};

use crate::pipeline::graph::envelope::Shelf;
use crate::pipeline::graph::leaf::read_exact_at;

#[derive(Debug, thiserror::Error)]
pub(crate) enum Unreadable {
    #[error("the run was stopped")]
    Stopped,
    #[error(transparent)]
    Lost(Lost),
}

impl From<Unreadable> for io::Error {
    fn from(error: Unreadable) -> Self {
        match error {
            // A stopped run's reads fail for good. `Interrupted` is the one
            // kind every reader retries, a decoder's header parse and
            // `read_exact` included, so a run stopped under one would spin
            // and never return.
            Unreadable::Stopped => io::Error::new(io::ErrorKind::BrokenPipe, error),
            // The span itself is the payload, which a run reads back.
            Unreadable::Lost(lost) => io::Error::new(io::ErrorKind::NotFound, lost),
        }
    }
}

pub(crate) struct RunVolumes {
    shelf: Arc<Shelf>,
    generation: u64,
    // The run's own handles, so a read never holds the shelf's lock while
    // it touches the disk.
    files: Mutex<BTreeMap<u32, File>>,
    // The lost spans reads failed on that the run has not read on past.
    lost: Mutex<BTreeSet<(u32, u64, u64)>>,
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
            lost: Mutex::default(),
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

    // Whether a read failed because nothing will bring its bytes, and the
    // run did not read on past them.
    pub(crate) fn lost(&self) -> bool {
        !self.spans().is_empty()
    }

    fn spans(&self) -> std::sync::MutexGuard<'_, BTreeSet<(u32, u64, u64)>> {
        self.lost.lock().unwrap_or_else(PoisonError::into_inner)
    }

    // Whether every volume the run reads is in: no volume can join, and
    // each bound one has closed.
    pub(crate) fn all_in(&self) -> bool {
        let state = self.shelf.lock();
        state.complete
            && !state.deciding
            && state.volumes.iter().all(|volume| {
                state
                    .kept
                    .get(volume)
                    .is_some_and(|kept| kept.closed.is_some())
            })
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
                    return match settled(&kept.coverage, want) {
                        Ok(len) => {
                            kept.reached = kept.reached.max(want.start + len);
                            Ok((len, Some(kept.path().to_path_buf())))
                        }
                        Err(range) => Err(Unreadable::Lost(Lost { volume, range })),
                    };
                }
                None if complete && !known => {
                    return Err(Unreadable::Lost(Lost {
                        volume,
                        range: want,
                    }));
                }
                _ => {}
            }
            state = self.shelf.wait(state);
        }
    }
}

impl Volumes for RunVolumes {
    fn uncertain_lengths(&self) -> bool {
        self.shelf.lock().uncertain_lengths
    }

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
        let mut state = self.shelf.lock();
        if state.generation != self.generation || self.stopped() {
            return None;
        }
        let kept = state.kept.get_mut(&volume.0)?;
        let length = kept.closed?;
        kept.length_read = Some(length);
        Some(length)
    }

    fn settled(&self, volume: VolumeIndex) -> bool {
        let state = self.shelf.lock();
        state.kept.get(&volume.0).is_some_and(|kept| {
            kept.closed.is_some_and(|length| {
                kept.settled || kept.coverage.verified(ByteRange::new(0, length))
            })
        })
    }

    fn read_at(&self, volume: VolumeIndex, offset: u64, into: &mut [u8]) -> io::Result<usize> {
        let range = ByteRange::at(offset, into.len() as u64);
        let (len, path) = self.wait_for(volume, range).inspect_err(|error| {
            if let Unreadable::Lost(lost) = error {
                self.spans()
                    .insert((lost.volume.0, lost.range.start, lost.range.end));
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

    fn bridged(&self, lost: Lost) {
        self.spans()
            .remove(&(lost.volume.0, lost.range.start, lost.range.end));
    }
}

// What a read of `want` gets from a volume nothing more will come to: the
// verified bytes from its start, as many as run unbroken, or, when its
// first byte is not one of them, the span from there to the next verified
// byte, which is lost.
fn settled(coverage: &Coverage, want: ByteRange) -> Result<u64, ByteRange> {
    let mut reached = want.start;
    let mut next = want.end;
    for run in coverage.runs() {
        if run.range.end <= reached || !run.provenance.is_verified() {
            continue;
        }
        if run.range.start > reached {
            next = next.min(run.range.start);
            break;
        }
        reached = run.range.end;
    }
    match reached.min(want.end) - want.start {
        0 => Err(ByteRange::new(want.start, next.max(want.start))),
        len => Ok(len),
    }
}
