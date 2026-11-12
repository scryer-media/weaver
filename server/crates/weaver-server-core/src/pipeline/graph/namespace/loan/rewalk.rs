// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A restored recovery object's rewalk, ready to run anywhere: the object,
// what of its volumes arrived, and the access that reads them where they
// lie, in their members' own files or held for them. Nothing is staged and
// nothing is written; the object hears its own volumes again as they first
// came, and what it said goes back to the loop with it.

use std::cell::RefCell;
use std::collections::{BTreeMap, BTreeSet};

use weaver_graph::{ByteRange, NodeId, Placement, Posted, Rewalk, Tree, VolumeIndex};

use super::super::super::hardware::Shutdown;
use super::super::super::leaf::LeafError;
use super::super::{NamespaceWriter, read_placed};
use super::{Lent, Repaired, ScratchRead};

pub(crate) struct RewalkLoan {
    rewalk: Rewalk,
    access: Lent,
}

// One piece read again, or the range of one that did not read back
// (`Err`), or the end of them (`None`): `whole` when every piece was
// taken, not when a shutdown or the working directory going cut the
// reading short. `scratch` is what the piece read of holds on scratch,
// which the loop checks before it hears the piece.
pub(crate) struct Rewalked {
    pub(crate) piece: Option<(VolumeIndex, Result<Posted, ByteRange>)>,
    pub(crate) whole: bool,
    pub(crate) scratch: ScratchRead,
}

impl std::fmt::Debug for Rewalked {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Rewalked")
            .field(
                "piece",
                &self.piece.as_ref().map(|(volume, piece)| match piece {
                    Ok(posted) => (volume, posted.offset, Some(posted.bytes.len())),
                    Err(range) => (volume, range.start, None),
                }),
            )
            .field("whole", &self.whole)
            .finish()
    }
}

impl RewalkLoan {
    pub(crate) fn node(&self) -> NodeId {
        self.rewalk.node
    }

    // Read each piece and hand it to `said`, or the range of one that did
    // not read back, which answers whether to go on, and whether every
    // piece was taken. A shutdown, or the job's working directory going,
    // ends it short: what was not read is asked again by the next restore.
    pub(crate) fn run(
        mut self,
        shutdown: &Shutdown,
        said: &mut dyn FnMut(VolumeIndex, Result<Posted, ByteRange>, ScratchRead) -> bool,
    ) -> bool {
        self.access.shutdown = shutdown.clone();
        let access = &self.access;
        let node = self.rewalk.node;
        let mut placed = |volume: VolumeIndex, placement: &Placement, into: &mut Vec<u8>| {
            access
                .volumes
                .get(&volume)
                .is_some_and(|&at| read_placed(access, at, node, placement, into))
        };
        let mut whole = true;
        for index in 0..self.rewalk.len() {
            if access.stopped() {
                whole = false;
                break;
            }
            if let Some((volume, piece)) = self.rewalk.read(index, &mut placed)
                && !said(volume, piece, access.scratch_read())
            {
                whole = false;
                break;
            }
        }
        let _ = self.access.repaired.discard();
        whole
    }
}

impl NamespaceWriter {
    // The error a rewalk cut short with no shutdown stands for: the job's
    // working directory, where its files lie, went.
    pub(crate) fn gone(&self) -> LeafError {
        let path = self.working_dir.join(weaver_graph::names::INTERNAL_DIR);
        LeafError::Io {
            path,
            source: std::io::Error::from(std::io::ErrorKind::NotFound),
        }
    }

    // What the restored recovery object at `node` asked again, with the
    // access its volumes read through. `None` when it has nothing to read.
    pub(crate) fn lend_rewalk(
        &self,
        tree: &mut Tree,
        node: NodeId,
    ) -> Result<Option<RewalkLoan>, LeafError> {
        let Some((parent, volumes)) = tree.volumes(node) else {
            let _ = tree.take_rewalk(node);
            return Ok(None);
        };
        let volumes: Vec<(VolumeIndex, weaver_graph::MemberId)> = volumes
            .iter()
            .map(|(volume, member)| (*volume, *member))
            .collect();
        let mut access = Lent {
            namespace: parent,
            set: node,
            recovery: Some(node),
            defer_holds: false,
            scratch_read: Some(RefCell::default()),
            held_scratch: BTreeMap::new(),
            internal: self.working_dir.join(weaver_graph::names::INTERNAL_DIR),
            members: BTreeMap::new(),
            usage: RefCell::default(),
            postings: BTreeMap::new(),
            shelves: BTreeMap::new(),
            volumes: BTreeMap::new(),
            window: tree.window(node),
            open: RefCell::new(None),
            reads: 0,
            repaired: Repaired::new(&self.working_dir, parent, node)?,
            found: Vec::new(),
            diagnostics: Vec::new(),
            scratch: None,
            shutdown: Shutdown::default(),
        };
        let mut seen = BTreeSet::new();
        for (volume, member) in volumes {
            access.volumes.insert(volume, (parent, member));
            self.take_member(tree, parent, member, &mut access, &mut seen);
        }
        match std::fs::create_dir(&access.internal) {
            Err(error) if error.kind() != std::io::ErrorKind::AlreadyExists => {
                return Err(LeafError::Io {
                    path: access.internal,
                    source: error,
                });
            }
            _ => {}
        }
        Ok(tree
            .take_rewalk(node)
            .map(|rewalk| RewalkLoan { rewalk, access }))
    }
}
