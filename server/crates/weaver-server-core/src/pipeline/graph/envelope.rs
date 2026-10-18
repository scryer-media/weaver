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

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Condvar, Mutex, MutexGuard, PoisonError};

use weaver_graph::{ByteRange, Coverage, NodeId, Placement, Provenance, VolumeIndex};

use super::leaf::{Leaf, LeafError};

#[derive(Debug)]
pub(crate) struct Envelopes {
    working_dir: PathBuf,
    shelves: BTreeMap<NodeId, Arc<Shelf>>,
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
    // How many volumes the object has, a gap for one not claimed yet
    // included, and whether any more can join.
    pub(crate) volumes: u32,
    pub(crate) complete: bool,
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
    pub(crate) settled: bool,
    // How far the run has read, which a repair behind it invalidates.
    pub(crate) reached: u64,
}

impl Kept {
    pub(crate) fn path(&self) -> &Path {
        self.leaf.path()
    }
}

impl Envelopes {
    pub(crate) fn new(working_dir: PathBuf) -> Self {
        Self {
            working_dir,
            shelves: BTreeMap::new(),
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
    // a repaired header replaces the damaged one. Repaired bytes behind what
    // a run already read stop that run, since what it built on was wrong.
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
        shelf.update(|state| {
            let kept = shelf.kept(state, envelope)?;
            kept.leaf.write(offset, bytes, None)?;
            kept.coverage.write(range, provenance);
            if provenance == Provenance::Repaired && range.start < kept.reached {
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
        let Some(shelf) = self.shelves.get(&node) else {
            return range.is_empty();
        };
        let state = shelf.lock();
        let Some(kept) = state.kept.get(&envelope) else {
            return range.is_empty();
        };
        let len = usize::try_from(range.len()).expect("an envelope range fits the address space");
        kept.coverage.covered(range) && kept.leaf.read(range.start, len, into).is_ok()
    }

    // The node is gone, and its envelopes with it. A run still reading them
    // is stopped first.
    pub(crate) fn forget(&mut self, node: NodeId) -> Result<(), LeafError> {
        let Some(shelf) = self.shelves.remove(&node) else {
            return Ok(());
        };
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

impl Shelf {
    pub(crate) fn lock(&self) -> MutexGuard<'_, Shelved> {
        self.state.lock().unwrap_or_else(PoisonError::into_inner)
    }

    // Change the shelf and wake whoever waits on it.
    pub(crate) fn update<R>(&self, change: impl FnOnce(&mut Shelved) -> R) -> R {
        let result = change(&mut self.lock());
        self.changed.notify_all();
        result
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
        if !state.kept.contains_key(&envelope) {
            let name = format!(".weaver-chunks/graph-envelope-{}-{envelope}", self.node.0);
            let mut leaf = Leaf::at(&self.dir, &name)?;
            leaf.truncate()?;
            state.kept.insert(
                envelope,
                Kept {
                    leaf,
                    coverage: Coverage::new(),
                    closed: None,
                    settled: false,
                    reached: 0,
                },
            );
        }
        Ok(state.kept.get_mut(&envelope).expect("inserted above"))
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
            }
            state.generation
        })
    }
}
