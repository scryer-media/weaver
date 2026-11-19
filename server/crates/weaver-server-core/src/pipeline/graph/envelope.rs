// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Envelopes: the bytes of a bound member that belong to no member of the
// object that owns it, such as headers and the padding between members.
// They are the inverse map's small remainder. The archive itself is never
// written, so these are kept, per node and envelope, as the bytes pass, in
// a file of their own under the directory finalize never delivers, and a
// read of the posted volume puts them back between the members' bytes.
//
// An engine places every byte of its volumes in the envelope numbered as the
// volume, so for an engine the envelopes are its volumes, whole or still
// growing. That is why a node's envelopes sit on a shelf behind one lock and
// one condition: the pipeline writes, closes and cancels, and the engine's
// run, on a thread of its own, waits on the same shelf for what it reads.

pub(crate) mod durable;

use std::collections::btree_map::Entry;
use std::collections::{BTreeMap, BTreeSet, HashMap, HashSet};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, PoisonError, Weak};

use weaver_graph::{ByteRange, Coverage, Marks, NodeId, Placement, Provenance, VolumeIndex};

use super::leaf::{Leaf, LeafError};

#[derive(Debug)]
pub(crate) struct Envelopes {
    working_dir: PathBuf,
    // The writer of the working directory's volume, when the job's files
    // are written through one.
    to: Option<Arc<crate::operations::disk::writer::Submitter>>,
    given: Given,
    shelves: BTreeMap<NodeId, Arc<Shelf>>,
    // The nodes whose kept envelopes changed, under the generation of the
    // latest change: every way to change what a checkpoint says of them
    // marks the node.
    marks: Marks<NodeId>,
    // Rung as a run begins to wait on any of the job's shelves.
    #[cfg(test)]
    blocked: Arc<tokio::sync::Notify>,
}

// One node's envelopes, shared with the run that reads them.
#[derive(Debug)]
pub(crate) struct Shelf {
    dir: PathBuf,
    to: Option<Arc<crate::operations::disk::writer::Submitter>>,
    given: Given,
    node: NodeId,
    state: Mutex<Shelved>,
    changed: Condvar,
    readers: Mutex<Vec<Weak<ReadFiles>>>,
    #[cfg(test)]
    blocked: Arc<tokio::sync::Notify>,
}

// What a run reads its envelopes with, which a drain reaches.
//
// Lock order: `disk`, then `files`, then the shelf's state lock. `files` is
// never held across a disk call, so a drain, which takes `files` alone, is
// never held behind a read that hangs; `disk` keeps the run's reads one at a
// time, as they were when they were made under `files`, and nothing that
// drains takes it.
#[derive(Debug, Default)]
pub(crate) struct ReadFiles {
    files: Mutex<RunFiles>,
    pub(crate) disk: Mutex<()>,
    // Raised when the run is stopped or drained: a wait for a read turn
    // hears it and ends.
    pub(crate) stop: crate::operations::disk::reader::TurnStop,
    // A test's hold on the next read inside its disk call: the read says it
    // entered, and returns once the test lets it.
    #[cfg(test)]
    hang: Mutex<Option<(std::sync::mpsc::Sender<()>, std::sync::mpsc::Receiver<()>)>>,
}

impl ReadFiles {
    #[cfg(test)]
    pub(crate) fn hang_next_read(
        &self,
    ) -> (std::sync::mpsc::Receiver<()>, std::sync::mpsc::Sender<()>) {
        let (entered_tx, entered) = std::sync::mpsc::channel();
        let (go, go_rx) = std::sync::mpsc::channel();
        *self.hang.lock().unwrap_or_else(PoisonError::into_inner) = Some((entered_tx, go_rx));
        (entered, go)
    }

    #[cfg(test)]
    pub(crate) fn in_read(&self) {
        let hang = self
            .hang
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take();
        if let Some((entered, go)) = hang {
            entered.send(()).unwrap();
            go.recv().unwrap();
        }
    }

    pub(crate) fn lock(&self) -> std::sync::LockResult<MutexGuard<'_, RunFiles>> {
        self.files.lock()
    }

    pub(crate) fn try_lock(&self) -> std::sync::TryLockResult<MutexGuard<'_, RunFiles>> {
        self.files.try_lock()
    }
}

// A run's open envelope files, and the read turn it holds on their volume
// (`Reading`): under one lock, which a read takes already, so a read in a
// turn held costs nothing more. A drain lets both go. A read in flight holds
// its own handle to the file and its turn, so the drain does not wait for it
// and the turn is not handed on before the read ends.
#[derive(Debug, Default)]
pub(crate) struct RunFiles {
    pub(crate) open: BTreeMap<u32, Arc<std::fs::File>>,
    pub(crate) reading: crate::operations::disk::reader::Reading,
}

#[derive(Debug, Default)]
pub(crate) struct Shelved {
    pub(crate) kept: BTreeMap<u32, Kept>,
    // The volumes bound to the object, by index, and whether any more can
    // join. An index orders them; a gap is not a volume.
    pub(crate) volumes: BTreeSet<u32>,
    pub(crate) complete: bool,
    // A member beside the volumes has yet to decide its owner, so a volume
    // may still join past the last one bound.
    pub(crate) deciding: bool,
    // Which run may read. Moving it on stops the one reading: a repair
    // behind what it read, or the node going away.
    pub(crate) generation: u64,
    // How many times the shelf woke its waiters, and the count a run last
    // began to wait at: a run waits for good while the two agree.
    #[cfg(test)]
    pub(crate) woken: u64,
    #[cfg(test)]
    pub(crate) waits_since: Option<u64>,
}

#[derive(Debug)]
pub(crate) struct Kept {
    leaf: Leaf,
    pub(crate) coverage: Coverage,
    // The length, once its member closed, and whether nothing more will
    // come at all, not even a repair: what is missing then is lost.
    pub(crate) closed: Option<u64>,
    // The length its member declared, known before it closes: where a run
    // that reads a set from its tail finds the tail while the volume grows.
    pub(crate) sized: Option<u64>,
    // The length the volume last closed at, kept across a reopen: a run
    // that read past the volume built on it until it closes at another.
    pub(crate) closed_at: Option<u64>,
    pub(crate) settled: bool,
    // Its object holds it at a place a byte still owed may move it from
    // (`Engine::placed`): the run reads none of it until the place settles.
    pub(crate) open: bool,
    // How far the run has read, which a repair behind it invalidates.
    pub(crate) reached: u64,
    // The closed length the run read, a dependency even before any payload
    // byte of this volume was read. Only another closed length breaks it.
    pub(crate) length_read: Option<u64>,
}

// The file an envelope is kept in: its name, how many bytes it holds, and
// whether it is on disk.
#[derive(Debug)]
pub(crate) struct KeptFile {
    pub(crate) name: String,
    pub(crate) covered: u64,
    pub(crate) on_disk: bool,
}

impl Kept {
    // An envelope with nothing kept yet, in `leaf`'s file.
    fn in_file(leaf: Leaf) -> Self {
        Self {
            leaf,
            coverage: Coverage::new(),
            closed: None,
            sized: None,
            closed_at: None,
            settled: false,
            open: false,
            reached: 0,
            length_read: None,
        }
    }

    pub(crate) fn path(&self) -> &Path {
        self.leaf.path()
    }

    // Everything submitted for the envelope's file so far, when it is
    // written through a volume's writer.
    pub(crate) fn settled(&self) -> Option<crate::operations::disk::writer::Barrier> {
        self.leaf.settled()
    }

    pub(crate) fn into_leaf(self) -> Leaf {
        self.leaf
    }
}

impl Envelopes {
    pub(crate) fn new(working_dir: PathBuf) -> Self {
        Self {
            working_dir,
            to: None,
            given: Given::default(),
            shelves: BTreeMap::new(),
            marks: Marks::default(),
            #[cfg(test)]
            blocked: Arc::default(),
        }
    }

    // What rings as a run begins to wait on one of the shelves.
    #[cfg(test)]
    pub(crate) fn blocked(&self) -> Arc<tokio::sync::Notify> {
        Arc::clone(&self.blocked)
    }

    // Envelopes made from here on are written through `to`. Set before
    // anything is kept.
    pub(crate) fn write_through(
        &mut self,
        to: Option<Arc<crate::operations::disk::writer::Submitter>>,
    ) {
        debug_assert!(self.shelves.is_empty());
        self.to = to;
    }

    // The names the job's new files were given while their making may
    // still be queued.
    pub(crate) fn given(&self) -> &Given {
        &self.given
    }

    // The node's shelf, made empty the first time it is asked for.
    pub(crate) fn shelf(&mut self, node: NodeId) -> Arc<Shelf> {
        let dir = &self.working_dir;
        let to = &self.to;
        let given = &self.given;
        #[cfg(test)]
        let blocked = &self.blocked;
        Arc::clone(self.shelves.entry(node).or_insert_with(|| {
            Arc::new(Shelf {
                dir: dir.clone(),
                to: to.clone(),
                given: given.clone(),
                node,
                state: Mutex::default(),
                changed: Condvar::new(),
                readers: Mutex::default(),
                #[cfg(test)]
                blocked: Arc::clone(blocked),
            })
        }))
    }

    // Keep the envelope pieces among `placements`, which the object at
    // `node` gave for the volume range that `bytes` is: pieces in volume
    // order, so each one's bytes are the next run of `bytes`.
    pub(crate) fn capture(
        &mut self,
        node: NodeId,
        placements: &[Placement],
        bytes: &[u8],
        provenance: Provenance,
    ) -> Result<(), LeafError> {
        let mut cursor = 0usize;
        for placement in placements {
            let len = usize::try_from(placement.len()).expect("a piece fits the address space");
            let Some(end) = cursor.checked_add(len).filter(|&end| end <= bytes.len()) else {
                return Ok(());
            };
            if let Placement::Envelope { envelope, range } = placement {
                self.keep(
                    node,
                    *envelope,
                    range.start,
                    &bytes[cursor..end],
                    provenance,
                )?;
            }
            cursor = end;
        }
        Ok(())
    }

    // Keep `bytes` at `offset` of an envelope, replacing what they overlap:
    // a repaired header replaces the damaged one. A repair, or a clean copy
    // mending damage, behind what a run already read stops that run, since
    // its output used superseded input. A further clean copy of clean bytes
    // never reaches here: the first copy stands.
    pub(crate) fn keep(
        &mut self,
        node: NodeId,
        envelope: u32,
        offset: u64,
        bytes: &[u8],
        provenance: Provenance,
    ) -> Result<(), LeafError> {
        if bytes.is_empty() {
            return Ok(());
        }
        let shelf = self.shelf(node);
        let range = ByteRange::at(offset, bytes.len() as u64);
        self.marks.mark(node);
        shelf.update(|state| {
            let kept = shelf.kept(state, envelope)?;
            let mends = provenance.is_verified() && kept.coverage.damaged(range);
            kept.leaf.write(offset, bytes, None)?;
            kept.coverage.write(range, provenance);
            if (provenance == Provenance::Repaired || mends) && range.start < kept.reached {
                state.generation += 1;
            }
            Ok(())
        })
    }

    // Append the bytes of `range` of an envelope to `into`, or nothing and
    // `false` when any of them were never kept.
    pub(crate) fn read(
        &self,
        node: NodeId,
        envelope: u32,
        range: ByteRange,
        into: &mut Vec<u8>,
    ) -> bool {
        match self.shelves.get(&node) {
            Some(shelf) => shelf.read(envelope, range, into),
            None => range.is_empty(),
        }
    }

    // Whether every byte of `range` of an envelope was kept, from the
    // record alone.
    pub(crate) fn keeps(&self, node: NodeId, envelope: u32, range: ByteRange) -> bool {
        match self.shelves.get(&node) {
            Some(shelf) => shelf.keeps(envelope, range),
            None => range.is_empty(),
        }
    }

    // Every node with a shelf, in node order.
    pub(crate) fn nodes(&self) -> impl Iterator<Item = NodeId> + '_ {
        self.shelves.keys().copied()
    }

    // The node's shelf, if anything was ever kept on it.
    pub(crate) fn shelf_of(&self, node: NodeId) -> Option<Arc<Shelf>> {
        self.shelves.get(&node).map(Arc::clone)
    }

    // The file an envelope sits in, relative to the working directory, and
    // how many bytes it keeps there.
    pub(crate) fn kept_file(&self, node: NodeId, envelope: u32) -> Option<KeptFile> {
        let shelf = self.shelves.get(&node)?;
        let state = shelf.lock();
        state.kept.get(&envelope).map(|kept| KeptFile {
            name: kept.leaf.name().to_owned(),
            covered: kept.coverage.bytes_covered(),
            on_disk: kept.leaf.on_disk(),
        })
    }

    // The envelope starts in `name`, a file that already holds its bytes at
    // their own offsets, or none when `on_disk` says it was never made:
    // nothing is cleared, and what is kept in it is written over the same
    // bytes. False when the envelope started already.
    pub(crate) fn start_in(
        &mut self,
        node: NodeId,
        envelope: u32,
        name: &str,
        on_disk: bool,
    ) -> bool {
        self.start_after(node, envelope, name, on_disk, 0)
    }

    // As `start_in`, for a file that the operation numbered `made`, already
    // submitted to the working directory's writer, puts at `name`: whoever
    // reads the envelope's file waits through that one.
    pub(crate) fn start_after(
        &mut self,
        node: NodeId,
        envelope: u32,
        name: &str,
        on_disk: bool,
        made: u64,
    ) -> bool {
        let shelf = self.shelf(node);
        let started = shelf.update(|state| match state.kept.entry(envelope) {
            Entry::Occupied(_) => false,
            Entry::Vacant(free) => {
                free.insert(Kept::in_file(Leaf::handed(
                    &shelf.dir,
                    name,
                    on_disk,
                    shelf.to.as_ref(),
                    made,
                )));
                true
            }
        });
        if started {
            self.marks.mark(node);
        }
        started
    }

    // An envelope the object at `node` gave back with its volume leaves the
    // shelf, file and all: the file, at the volume's own offsets, is what
    // the object kept of the volume. Nothing is read or copied.
    pub(crate) fn take(&mut self, node: NodeId, envelope: u32) -> Option<Kept> {
        let shelf = self.shelves.get(&node)?;
        let kept = shelf.update(|state| {
            // Removing a volume changes the order already-read input used.
            // The run barrier also retracts its acknowledged derived output.
            let read_after = state.kept.range(envelope..).any(|(index, kept)| {
                kept.reached > 0 || (*index == envelope && kept.length_read.is_some())
            });
            let kept = state.kept.remove(&envelope)?;
            if read_after {
                state.generation += 1;
            }
            Some(kept)
        })?;
        self.marks.mark(node);
        Some(kept)
    }

    // Change the node's shelf as `change` says it did, which marks the node
    // when it did and wakes whoever waits on it.
    pub(crate) fn update_if<E>(
        &mut self,
        node: NodeId,
        change: impl FnOnce(&Shelf, &mut Shelved) -> Result<bool, E>,
    ) -> Result<(), E> {
        let shelf = self.shelf(node);
        let mut changed = false;
        let result = shelf.update_if(|state| {
            let result = change(&shelf, state);
            changed = !matches!(result, Ok(false));
            result
        });
        if changed {
            self.marks.mark(node);
        }
        result
    }

    // End storage access before completion unlinks consumed volumes or
    // moves outputs. Keep the facts for reporting, but invalidate readers
    // and drain their handles before returning storage to completion.
    pub(crate) fn release_handles(&mut self) {
        for shelf in self.shelves.values() {
            shelf.update(|state| {
                state.generation += 1;
                for kept in state.kept.values_mut() {
                    kept.leaf.release_handle();
                }
            });
            shelf.release_readers();
        }
    }

    #[cfg(test)]
    pub(crate) fn open_files(&self) -> Vec<PathBuf> {
        self.shelves
            .values()
            .flat_map(|shelf| {
                shelf
                    .lock()
                    .kept
                    .values()
                    .filter(|kept| kept.leaf.is_open())
                    .map(|kept| kept.path().to_path_buf())
                    .collect::<Vec<_>>()
            })
            .collect()
    }

    // The node is gone, and its envelopes with it. A run still reading them
    // is stopped first. Their files are returned, not removed: the last
    // checkpoint the database holds may still name them.
    pub(crate) fn forget(&mut self, node: NodeId) -> Vec<Leaf> {
        let Some(shelf) = self.shelves.remove(&node) else {
            return Vec::new();
        };
        self.marks.mark(node);
        let kept = shelf.update(|state| {
            state.generation += 1;
            std::mem::take(&mut state.kept)
        });
        shelf.release_readers();
        kept.into_values().map(|kept| kept.leaf).collect()
    }
}

// A job that goes away stops every run reading its envelopes, so no thread
// waits on a shelf nothing will write again.
impl Drop for Envelopes {
    fn drop(&mut self) {
        self.release_handles();
    }
}

// The shelves this thread locked, by node, for a test to read back.
#[cfg(test)]
thread_local! {
    pub(crate) static LOCKED: std::cell::RefCell<Vec<NodeId>> =
        const { std::cell::RefCell::new(Vec::new()) };
    // The most bytes one read of a kept envelope asked for on this thread.
    pub(crate) static READ_MOST: std::cell::Cell<u64> = const { std::cell::Cell::new(0) };
}

impl Shelf {
    // The volume the shelf's envelopes are written to, whose read turns a
    // run reading them takes. `None` when the job writes through no writer.
    pub(crate) fn volume(&self) -> Option<&Arc<crate::operations::disk::volume::Volume>> {
        self.to.as_ref().map(|to| to.volume())
    }

    pub(crate) fn reader_files(&self) -> Arc<ReadFiles> {
        let files = Arc::new(ReadFiles::default());
        let mut readers = self.readers.lock().unwrap_or_else(PoisonError::into_inner);
        readers.retain(|reader| reader.strong_count() != 0);
        readers.push(Arc::downgrade(&files));
        files
    }

    // Invalidate the generation first, then drain every run, including
    // superseded ones still held by a worker. Never hold the state lock
    // here: readers check their generation under their file-cache lock.
    // A run waiting for a read turn is woken and fails; one inside a read
    // is not waited for, since no read holds the file-cache lock.
    fn release_readers(&self) {
        let mut readers = self.readers.lock().unwrap_or_else(PoisonError::into_inner);
        readers.retain(|reader| {
            let Some(files) = reader.upgrade() else {
                return false;
            };
            files.stop.raise();
            let mut files = files.lock().unwrap_or_else(PoisonError::into_inner);
            files.open.clear();
            files.reading.let_go();
            true
        });
    }

    pub(crate) fn lock(&self) -> MutexGuard<'_, Shelved> {
        #[cfg(test)]
        LOCKED.with_borrow_mut(|locked| locked.push(self.node));
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    // Change the shelf and wake whoever waits on it.
    pub(crate) fn update<R>(&self, change: impl FnOnce(&mut Shelved) -> R) -> R {
        let mut state = self.lock();
        let result = change(&mut state);
        #[cfg(test)]
        {
            state.woken += 1;
        }
        drop(state);
        self.changed.notify_all();
        result
    }

    // Change the shelf as `change` says it did, and wake whoever waits on
    // it only when it did: a waiter woken to an unchanged shelf only waits
    // again. One that failed part way may have changed it, and wakes them.
    pub(crate) fn update_if<E>(
        &self,
        change: impl FnOnce(&mut Shelved) -> Result<bool, E>,
    ) -> Result<(), E> {
        let mut state = self.lock();
        let result = change(&mut state);
        let wakes = !matches!(result, Ok(false));
        #[cfg(test)]
        if wakes {
            state.woken += 1;
        }
        drop(state);
        if wakes {
            self.changed.notify_all();
        }
        result.map(|_| ())
    }

    // Wait for the shelf to change; the guard is given back once it did.
    pub(crate) fn wait<'a>(
        &self,
        #[cfg_attr(not(test), expect(unused_mut))] mut state: MutexGuard<'a, Shelved>,
    ) -> MutexGuard<'a, Shelved> {
        #[cfg(test)]
        {
            state.waits_since = Some(state.woken);
            self.blocked.notify_one();
        }
        #[cfg_attr(not(test), expect(unused_mut))]
        let mut state = self
            .changed
            .wait(state)
            .unwrap_or_else(PoisonError::into_inner);
        #[cfg(test)]
        {
            state.waits_since = None;
        }
        state
    }

    // Whether a run waits on the shelf for a change that has not come.
    #[cfg(test)]
    pub(crate) fn blocked(&self) -> bool {
        let state = self.lock();
        state.waits_since == Some(state.woken)
    }

    // A volume's entry, made the first time anything is said of it.
    pub(crate) fn kept<'a>(
        &self,
        state: &'a mut Shelved,
        envelope: u32,
    ) -> Result<&'a mut Kept, LeafError> {
        match state.kept.entry(envelope) {
            Entry::Occupied(kept) => Ok(kept.into_mut()),
            Entry::Vacant(free) => {
                let name = self.given.free(
                    self.to.as_ref(),
                    &self.dir,
                    &envelope_name(self.node, envelope),
                );
                let mut leaf = Leaf::at(&self.dir, &name).through(self.to.as_ref());
                leaf.truncate()?;
                Ok(free.insert(Kept::in_file(leaf)))
            }
        }
    }

    // Append the bytes of `range` of an envelope to `into`, or nothing and
    // `false` when any of them were never kept.
    pub(crate) fn read(&self, envelope: u32, range: ByteRange, into: &mut Vec<u8>) -> bool {
        self.read_on(envelope, range, into, false)
    }

    // The same read for a thread the loop does not wait for.
    pub(crate) fn read_aside(&self, envelope: u32, range: ByteRange, into: &mut Vec<u8>) -> bool {
        self.read_on(envelope, range, into, true)
    }

    fn read_on(&self, envelope: u32, range: ByteRange, into: &mut Vec<u8>, aside: bool) -> bool {
        let state = self.lock();
        let Some(kept) = state.kept.get(&envelope) else {
            return range.is_empty();
        };
        let len = usize::try_from(range.len()).expect("an envelope range fits the address space");
        #[cfg(test)]
        READ_MOST.set(READ_MOST.get().max(range.len()));
        kept.coverage.covered(range)
            && match aside {
                true => kept.leaf.read_aside(range.start, len, into),
                false => kept.leaf.read(range.start, len, into),
            }
            .is_ok()
    }

    // Whether every byte of `range` of an envelope was kept, read from the
    // record alone: a restore cut the record to its file's length.
    pub(crate) fn keeps(&self, envelope: u32, range: ByteRange) -> bool {
        self.lock()
            .kept
            .get(&envelope)
            .map_or(range.is_empty(), |kept| kept.coverage.covered(range))
    }

    // Whether any byte of any volume is in.
    pub(crate) fn has_bytes(&self) -> bool {
        self.lock()
            .kept
            .values()
            .any(|kept| kept.coverage.bytes_covered() > 0)
    }

    // The furthest volume a run reached, and how far into it.
    pub(crate) fn reached(&self) -> Option<(VolumeIndex, u64)> {
        self.lock()
            .kept
            .iter()
            .rev()
            .find(|(_, kept)| kept.reached > 0)
            .map(|(volume, kept)| (VolumeIndex(*volume), kept.reached))
    }

    pub(crate) fn generation(&self) -> u64 {
        self.lock().generation
    }

    // The bytes the run read are good, but the object now knows something
    // the run was started without, a password: the run stops, and its owner
    // starts one over the same bytes.
    pub(crate) fn restart(&self) {
        self.update(|state| state.generation += 1);
    }

    // A fresh run reads from the start: what an earlier one read is
    // forgotten, and the generation it reads under is the shelf's.
    pub(crate) fn begin(&self) -> u64 {
        self.update(|state| {
            for kept in state.kept.values_mut() {
                kept.reached = 0;
                kept.length_read = None;
            }
            state.generation
        })
    }
}

// The file an envelope of `node` numbered `envelope` sits in, unless a
// checkpoint says another.
pub(crate) fn envelope_name(node: NodeId, envelope: u32) -> String {
    format!(
        "{}/graph-envelope-{}-{envelope}",
        weaver_graph::names::INTERNAL_DIR,
        node.0
    )
}

// The names given to new files of a job that writes through a volume's
// writer, by the directory each lies under. A file made through a writer is
// not on disk until the writer has run, so the disk alone does not say
// which names are taken: a name given here stays taken until everything
// submitted for the directory's volume has returned, when the disk says it
// all again. A file removed through the writer may still sit on disk: its
// name is passed over until it is gone, which costs only a longer name.
// Nothing waits here, and a name is asked for once for each new file kept
// aside, never for each article.
#[derive(Clone, Debug, Default)]
pub(crate) struct Given(Arc<Mutex<HashMap<PathBuf, HashSet<String>>>>);

impl Given {
    // `name` under `dir`, or the first free one after it: one no file sits
    // at and none was given since the directory's writer was last idle.
    // With no writer the disk says it all, and nothing is kept.
    pub(crate) fn free(
        &self,
        to: Option<&Arc<crate::operations::disk::writer::Submitter>>,
        dir: &Path,
        name: &str,
    ) -> String {
        let Some(to) = to else {
            return free_name(dir, name);
        };
        let mut given = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        if to.barrier().returned() {
            given.remove(dir);
        }
        let taken = given.entry(dir.to_path_buf()).or_default();
        let free = |name: &str| !taken.contains(name) && !dir.join(name).exists();
        let found = if free(name) {
            name.to_owned()
        } else {
            (1u32..)
                .map(|again| format!("{name}-{again}"))
                .find(|again| free(again))
                .expect("an unbounded counter finds a free name")
        };
        taken.insert(found.clone());
        found
    }
}

// `name`, or, while a file still sits there, the first free one after it:
// one a member given back sits at waiting to move, or one that goes once a
// checkpoint commits. Nothing is written over a file that sits anywhere.
pub(crate) fn free_name(dir: &Path, name: &str) -> String {
    if !dir.join(name).exists() {
        return name.to_owned();
    }
    (1u32..)
        .map(|again| format!("{name}-{again}"))
        .find(|again| !dir.join(again).exists())
        .expect("an unbounded counter finds a free name")
}
