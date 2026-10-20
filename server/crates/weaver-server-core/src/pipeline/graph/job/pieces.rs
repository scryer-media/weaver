// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A graph job over a piece root: a torrent's files as the root's volumes,
// each announced with the length the metainfo gives it, fed by the pieces
// that verified.

use std::sync::Arc;

use bitvec::prelude::{BitVec, Msb0};
use weaver_graph::{Builder, ByteRange, MemberId, VolumeIndex};

use super::restore::{RestoreFailure, Restored};
use super::{GraphError, GraphJob, Storage};
use crate::pipeline::graph::checkpoint::Snapshot;
use crate::pipeline::graph::engine::Runs;
use crate::pipeline::graph::namespace::NamespaceWriter;
use crate::pipeline::graph::root::pieces::{PieceLayout, PieceRoot, Verified};
use crate::pipeline::graph::status::RepairSlots;

impl GraphJob<PieceRoot> {
    // The runtime of a torrent's job. `scope` is the job's process-wide id.
    pub(crate) fn for_torrent(scope: u64, layout: Arc<PieceLayout>, storage: Storage) -> Self {
        let Storage {
            working_dir,
            accountant,
            free_disk,
            slots,
            mail,
            passwords,
            caps,
            nesting,
        } = storage;
        let runs = Runs::new(scope, slots, mail, Arc::clone(&accountant));
        let writer = NamespaceWriter::new(scope, working_dir, accountant, free_disk).capped(caps);
        Self::torrent(
            layout,
            Box::new(weaver_objects::Objects),
            writer,
            runs,
            nesting,
        )
        .offering(passwords)
    }

    pub(crate) fn torrent(
        layout: Arc<PieceLayout>,
        builder: Box<dyn Builder + Send + Sync>,
        writer: NamespaceWriter,
        runs: Runs,
        nesting: u32,
    ) -> Self {
        let members: Vec<(String, u64)> = (0..layout.volume_count())
            .filter_map(|volume| layout.volume(VolumeIndex(volume as u32)))
            .map(|file| (file.name.clone(), file.length))
            .collect();
        Self::with_root(
            PieceRoot::new(layout),
            members
                .iter()
                .map(|(name, length)| (name.as_str(), Some(*length))),
            builder,
            writer,
            runs,
            nesting,
        )
    }

    // A torrent's job back from its checkpoint, over the torrent's layout.
    pub(crate) fn restore_torrent(
        snapshot: &Snapshot,
        layout: Arc<PieceLayout>,
        scope: u64,
        storage: Storage,
        limit: usize,
    ) -> Result<Restored<PieceRoot>, RestoreFailure> {
        let root = PieceRoot::restore(layout, &snapshot.root.read::<PieceRoot>()?)?;
        Self::restore_with(snapshot, root, scope, storage, limit)
    }

    // A piece that verified, as its files' shares. A piece is fetched
    // whole, so one fetched again for a share that did not read back also
    // carries shares already in hand: only the ones still to come go down.
    // A repair a share makes due is lent under `slots`.
    pub(crate) fn verified(
        &mut self,
        shares: Vec<Verified>,
        slots: &mut RepairSlots,
        limit: usize,
    ) -> Result<(), GraphError> {
        for share in shares {
            if self.root.waiting(share.volume, share.piece) {
                self.intake(share, slots, limit)?;
            }
        }
        Ok(())
    }

    // The pieces in hand, for the fetch to start from.
    pub(crate) fn have(&self) -> BitVec<u8, Msb0> {
        self.root.have()
    }

    // Each file's bytes in hand, by torrent index.
    pub(crate) fn file_have(&self) -> Vec<u64> {
        self.root.file_have()
    }

    // The files the fetch should fetch now, by torrent index.
    pub(crate) fn selection(&self) -> Vec<usize> {
        self.root.selection()
    }

    // A volume's posted bytes as they are in hand now, for a peer.
    pub(crate) fn posted(&self, volume: VolumeIndex, range: ByteRange, into: &mut Vec<u8>) -> bool {
        self.writer
            .read_in_hand(&self.tree, MemberId(volume.0), range, into)
    }
}
