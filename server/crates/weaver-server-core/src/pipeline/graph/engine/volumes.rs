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

use std::collections::BTreeSet;
use std::fs::File;
use std::io;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError, Weak};

use weaver_graph::{ByteRange, Coverage, Lost, VolumeIndex, Volumes};

use crate::operations::disk::reader::VolumeReads;
use crate::operations::disk::volume::Volume;
use crate::operations::disk::writer::Barrier;
use crate::pipeline::graph::decoding::Decoding;
use crate::pipeline::graph::envelope::{ReadFiles, Shelf, Shelved};
use crate::pipeline::graph::leaf::read_exact_at;

// The file a read is in, and the barrier its bytes wait on, if any.
type ReadAt = (std::path::PathBuf, Option<Barrier>);

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
    // The run's handles are registered with the storage owner for draining.
    // A read never holds the shelf's state lock while it touches the disk.
    files: Arc<ReadFiles>,
    // The volume the envelopes are written to: the run reads them in its
    // turns, holding one across its reads on a volume with more than one.
    volume: Option<Arc<Volume>>,
    // The lost spans reads failed on that the run has not read on past.
    lost: Mutex<BTreeSet<(u32, u64, u64)>>,
    stopped: AtomicBool,
    // Stopped for the job to wait for room on a disk.
    parked: AtomicBool,
    // The time the run's decoder spent executing, in nanoseconds, set as
    // it returns.
    decoded: AtomicU64,
    // The run's decoder while it executes: a read waiting on the shelf is
    // the decoder waiting, not decoding.
    decoding: Mutex<Weak<Decoding>>,
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
            files: shelf.reader_files(),
            volume: shelf.volume().cloned(),
            shelf,
            generation,
            lost: Mutex::default(),
            stopped: AtomicBool::new(false),
            parked: AtomicBool::new(false),
            decoded: AtomicU64::new(0),
            decoding: Mutex::default(),
        }
    }

    // The run's decoder began executing: it is done once the handle goes.
    pub(crate) fn decoding(&self, decoding: Weak<Decoding>) {
        *self.decoding.lock().unwrap_or_else(PoisonError::into_inner) = decoding;
    }

    // The run gives back the read turn it holds: it waits on something
    // other than the disk, or it ended.
    pub(crate) fn let_go(&self) {
        self.files
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .reading
            .let_go();
    }

    fn wait<'a>(&self, state: MutexGuard<'a, Shelved>) -> MutexGuard<'a, Shelved> {
        // A run waiting for bytes to come reads nothing: its turn goes back.
        // The shelf's lock is held here, and a read takes the files' lock
        // before the shelf's, so it is only tried: when it is held, a drain
        // holds it and lets the turn go itself, or the run's other thread
        // reads in it.
        match self.files.try_lock() {
            Ok(mut files) => files.reading.let_go(),
            Err(std::sync::TryLockError::Poisoned(files)) => files.into_inner().reading.let_go(),
            Err(std::sync::TryLockError::WouldBlock) => {}
        }
        let decoding = self
            .decoding
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .upgrade();
        let _waiting = decoding.as_deref().map(Decoding::wait);
        self.shelf.wait(state)
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

    // The run stops where it stands because its job waits for room on a
    // disk: it ends, and once it has, it is queued to run again from the
    // start of its volumes.
    pub(crate) fn park(&self) {
        self.parked.store(true, Ordering::Release);
        self.stop();
    }

    // Whether the run stopped for its job to wait for room.
    pub(crate) fn parked(&self) -> bool {
        self.parked.load(Ordering::Acquire)
    }

    // The run's decoder returned, having taken `spent` of the job's
    // decoding clock. Set before the run says it ended, which its owner
    // hears after this.
    pub(crate) fn decoded(&self, spent: std::time::Duration) {
        self.decoded.store(
            u64::try_from(spent.as_nanos()).unwrap_or(u64::MAX),
            Ordering::Release,
        );
    }

    // The part of the job's decoding clock the run's decoder took, once it
    // returned.
    pub(crate) fn decode_time(&self) -> std::time::Duration {
        std::time::Duration::from_nanos(self.decoded.load(Ordering::Acquire))
    }

    #[cfg(test)]
    pub(super) fn open_files(&self) -> usize {
        self.files
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .open
            .len()
    }

    // Whether the run holds a read turn.
    #[cfg(test)]
    pub(super) fn holds_turn(&self) -> bool {
        self.files
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .reading
            .held()
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
    ) -> Result<(u64, Option<ReadAt>), Unreadable> {
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
                    return Ok((
                        want.len(),
                        Some((kept.path().to_path_buf(), kept.settled())),
                    ));
                }
                Some(kept) if kept.settled => {
                    return match settled(&kept.coverage, want) {
                        Ok(len) => {
                            kept.reached = kept.reached.max(want.start + len);
                            Ok((len, Some((kept.path().to_path_buf(), kept.settled()))))
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
            state = self.wait(state);
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
            state = self.wait(state);
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

    fn bound(&self) -> io::Result<Vec<VolumeIndex>> {
        let mut state = self.shelf.lock();
        loop {
            if state.generation != self.generation || self.stopped() {
                return Err(Unreadable::Stopped.into());
            }
            if !state.volumes.is_empty() || state.complete {
                return Ok(state.volumes.iter().copied().map(VolumeIndex).collect());
            }
            state = self.wait(state);
        }
    }

    // The length read is one the run built on, as a closed one is: the
    // volume closing at another starts the run over.
    fn sized(&self, volume: VolumeIndex) -> io::Result<Option<u64>> {
        let mut state = self.shelf.lock();
        loop {
            if state.generation != self.generation || self.stopped() {
                return Err(Unreadable::Stopped.into());
            }
            let known = state.volumes.contains(&volume.0);
            let complete = state.complete;
            if let Some(kept) = state.kept.get_mut(&volume.0)
                && let Some(length) = kept.closed.or(kept.sized)
            {
                kept.length_read = Some(length);
                return Ok(Some(length));
            }
            if complete && !known {
                return Ok(None);
            }
            state = self.wait(state);
        }
    }

    // A volume with bytes nothing will bring is not settled: a read of them
    // fails, and work on the pool reads only what is all in.
    fn settled(&self, volume: VolumeIndex) -> bool {
        let state = self.shelf.lock();
        state.kept.get(&volume.0).is_some_and(|kept| {
            kept.closed
                .is_some_and(|length| kept.coverage.verified(ByteRange::new(0, length)))
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
        let Some((path, settled)) = path else {
            return Ok(0);
        };
        // The coverage says the bytes were handed to the file; they are
        // read once the volume's writer has put them there. The run reads
        // on a thread of its own, which the loop never waits for.
        if let Some(settled) = settled {
            settled.wait_aside();
            // A write of the volume failed: its file does not hold what
            // the coverage says, and the failure fails the job.
            if settled.unwritten() {
                return Err(Unreadable::Stopped.into());
            }
        }
        let len = usize::try_from(len).expect("a read fits the buffer it fills");
        // The read is made in a turn of the envelopes' volume. A spinning
        // one's single turn is taken for this read alone, before the
        // files' lock; any other's is the one the run holds.
        let reads = self.volume.as_deref().map(Volume::read_turns);
        let _one = reads
            .filter(|reads| reads.whole_pieces())
            .map(VolumeReads::one_here);
        let mut files = self.files.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(reads) = reads.filter(|reads| !reads.whole_pieces())
            && !files.reading.holds(reads)
        {
            // Waited for holding nothing: a drain takes the files' lock,
            // and the turn may be another reader's for a while.
            files.reading.let_go();
            drop(files);
            let turn = reads.owned_here();
            files = self.files.lock().unwrap_or_else(PoisonError::into_inner);
            files.reading.keep(turn);
        }
        // The owner can revoke access after wait_for returns. Check again
        // while holding the cache lock, so draining it waits for this read
        // and a late reader cannot reopen a file after the drain.
        if !self.current() {
            files.reading.let_go();
            return Err(Unreadable::Stopped.into());
        }
        let file = match files.open.entry(volume.0) {
            std::collections::btree_map::Entry::Occupied(found) => found.into_mut(),
            std::collections::btree_map::Entry::Vacant(slot) => slot.insert(File::open(path)?),
        };
        read_exact_at(file, offset, &mut into[..len])?;
        Ok(len)
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
