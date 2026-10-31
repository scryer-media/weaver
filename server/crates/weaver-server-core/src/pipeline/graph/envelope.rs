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
use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, PoisonError};

use weaver_graph::{ByteRange, Coverage, Marks, NodeId, Placement, Provenance, VolumeIndex};

use super::leaf::{Leaf, LeafError};

#[derive(Debug)]
pub(crate) struct Envelopes {
    working_dir: PathBuf,
    shelves: BTreeMap<NodeId, Arc<Shelf>>,
    // The nodes whose kept envelopes changed, under the generation of the
    // latest change: every way to change what a checkpoint says of them
    // marks the node.
    marks: Marks<NodeId>,
}

// One node's envelopes, shared with the run that reads them.
#[derive(Debug)]
pub(crate) struct Shelf {
    dir: PathBuf,
    node: NodeId,
    state: Mutex<Shelved>,
    changed: Condvar,
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
}

#[derive(Debug)]
pub(crate) struct Kept {
    leaf: Leaf,
    pub(crate) coverage: Coverage,
    // The length, once its member closed, and whether nothing more will
    // come at all, not even a repair: what is missing then is lost.
    pub(crate) closed: Option<u64>,
    // The length the volume last closed at, kept across a reopen: a run
    // that read past the volume built on it until it closes at another.
    pub(crate) closed_at: Option<u64>,
    pub(crate) settled: bool,
    // How far the run has read, which a repair behind it invalidates.
    pub(crate) reached: u64,
    // The closed length the run read, a dependency even before any payload
    // byte of this volume was read. Only another closed length breaks it.
    pub(crate) length_read: Option<u64>,
}

impl Kept {
    pub(crate) fn path(&self) -> &Path {
        self.leaf.path()
    }

    pub(crate) fn into_leaf(self) -> Leaf {
        self.leaf
    }
}

impl Envelopes {
    pub(crate) fn new(working_dir: PathBuf) -> Self {
        Self {
            working_dir,
            shelves: BTreeMap::new(),
            marks: Marks::default(),
        }
    }

    // The node's shelf, made empty the first time it is asked for.
    pub(crate) fn shelf(&mut self, node: NodeId) -> Arc<Shelf> {
        let dir = &self.working_dir;
        Arc::clone(self.shelves.entry(node).or_insert_with(|| {
            Arc::new(Shelf {
                dir: dir.clone(),
                node,
                state: Mutex::default(),
                changed: Condvar::new(),
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

    // Every node with a shelf, in node order.
    pub(crate) fn nodes(&self) -> impl Iterator<Item = NodeId> + '_ {
        self.shelves.keys().copied()
    }

    // The node's shelf, if anything was ever kept on it.
    pub(crate) fn shelf_of(&self, node: NodeId) -> Option<Arc<Shelf>> {
        self.shelves.get(&node).map(Arc::clone)
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

    // The node is gone, and its envelopes with it. A run still reading them
    // is stopped first.
    pub(crate) fn forget(&mut self, node: NodeId) -> Result<(), LeafError> {
        let Some(shelf) = self.shelves.remove(&node) else {
            return Ok(());
        };
        self.marks.mark(node);
        let kept = shelf.update(|state| {
            state.generation += 1;
            std::mem::take(&mut state.kept)
        });
        for (_, kept) in kept {
            kept.leaf.remove()?;
        }
        Ok(())
    }
}

// A job that goes away stops every run reading its envelopes, so no thread
// waits on a shelf nothing will write again.
impl Drop for Envelopes {
    fn drop(&mut self) {
        for shelf in self.shelves.values() {
            shelf.update(|state| state.generation += 1);
        }
    }
}

// The shelves this thread locked, by node, for a test to read back.
#[cfg(test)]
thread_local! {
    pub(crate) static LOCKED: std::cell::RefCell<Vec<NodeId>> =
        const { std::cell::RefCell::new(Vec::new()) };
}

impl Shelf {
    pub(crate) fn lock(&self) -> MutexGuard<'_, Shelved> {
        #[cfg(test)]
        LOCKED.with_borrow_mut(|locked| locked.push(self.node));
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    // Change the shelf and wake whoever waits on it.
    pub(crate) fn update<R>(&self, change: impl FnOnce(&mut Shelved) -> R) -> R {
        let result = change(&mut self.lock());
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
        let result = change(&mut self.lock());
        if !matches!(result, Ok(false)) {
            self.changed.notify_all();
        }
        result.map(|_| ())
    }

    // Wait for the shelf to change; the guard is given back once it did.
    pub(crate) fn wait<'a>(&self, state: MutexGuard<'a, Shelved>) -> MutexGuard<'a, Shelved> {
        self.changed
            .wait(state)
            .unwrap_or_else(PoisonError::into_inner)
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
                let name = free_envelope_name(&self.dir, self.node, envelope);
                let mut leaf = Leaf::at(&self.dir, &name);
                leaf.truncate()?;
                Ok(free.insert(Kept {
                    leaf,
                    coverage: Coverage::new(),
                    closed: None,
                    closed_at: None,
                    settled: false,
                    reached: 0,
                    length_read: None,
                }))
            }
        }
    }

    // Append the bytes of `range` of an envelope to `into`, or nothing and
    // `false` when any of them were never kept.
    pub(crate) fn read(&self, envelope: u32, range: ByteRange, into: &mut Vec<u8>) -> bool {
        let state = self.lock();
        let Some(kept) = state.kept.get(&envelope) else {
            return range.is_empty();
        };
        let len = usize::try_from(range.len()).expect("an envelope range fits the address space");
        kept.coverage.covered(range) && kept.leaf.read(range.start, len, into).is_ok()
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

// The file a new envelope starts in: the one its node and number name, or,
// while a member given back from an envelope of that number still sits
// there waiting to move, the first free one after it. Nothing is written
// over a file a member sits at.
fn free_envelope_name(dir: &Path, node: NodeId, envelope: u32) -> String {
    let name = envelope_name(node, envelope);
    if !dir.join(&name).exists() {
        return name;
    }
    (1u32..)
        .map(|again| format!("{name}-{again}"))
        .find(|again| !dir.join(again).exists())
        .expect("an unbounded counter finds a free name")
}
