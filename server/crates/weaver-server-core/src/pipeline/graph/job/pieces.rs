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
            kept_dir,
            accountant,
            free_disk,
            slots,
            mail,
            passwords,
            caps,
            nesting,
            unpack,
        } = storage;
        let runs = Runs::new(scope, slots, mail, Arc::clone(&accountant));
        let writer = NamespaceWriter::new(scope, working_dir, accountant, free_disk)
            .capped(caps)
            .keeping_in(kept_dir);
        Self::torrent(layout, super::builder(unpack), writer, runs, nesting).offering(passwords)
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

    // A piece a check found where the files were placed, as its files'
    // shares: in hand as a verified piece is, and nothing is written, since
    // the files hold it already (libtorrent `torrent::on_piece_hashed` has
    // the piece and writes nothing).
    pub(crate) fn adopted(
        &mut self,
        shares: Vec<Verified>,
        slots: &mut RepairSlots,
        limit: usize,
    ) -> Result<(), GraphError> {
        for share in shares {
            if self.root.waiting(share.volume, share.piece) {
                self.intake_found(share, slots, limit)?;
            }
        }
        Ok(())
    }

    // A piece that failed its hash, fetched or checked on disk: every
    // file's share of it that was in hand is not, and waits to be fetched
    // again, so the share it brings next is delivered and rewrites the
    // file. Nothing is lost: a peer may yet send it.
    // True when the job held any of it.
    pub(crate) fn failed(
        &mut self,
        piece: u32,
        slots: &mut RepairSlots,
        limit: usize,
    ) -> Result<bool, GraphError> {
        let in_hand = self.root.in_hand(piece);
        for &(volume, range) in &in_hand {
            self.unarrive(volume, piece, Some(range));
        }
        if in_hand.is_empty() {
            return Ok(false);
        }
        self.after_units(in_hand.into_iter().map(|(volume, _)| volume), slots, limit)?;
        Ok(true)
    }

    // A check of every piece began (libtorrent `torrent::force_recheck`
    // forgets every piece): no piece in hand is had until the check finds
    // it, and a checkpoint taken meanwhile says so. True when any was had.
    pub(crate) fn checking(
        &mut self,
        slots: &mut RepairSlots,
        limit: usize,
    ) -> Result<bool, GraphError> {
        if !self.root.forget() {
            return Ok(false);
        }
        self.after_units([], slots, limit)?;
        Ok(true)
    }

    // The check found the piece (libtorrent `torrent::on_piece_hashed`,
    // `we_have`): it is had again. True when it waited for the check.
    pub(crate) fn found(
        &mut self,
        piece: u32,
        slots: &mut RepairSlots,
        limit: usize,
    ) -> Result<bool, GraphError> {
        if !self.root.found(piece) {
            return Ok(false);
        }
        self.after_units([], slots, limit)?;
        Ok(true)
    }

    // Whether a piece in hand waits for a check to find it.
    pub(crate) fn unchecked(&self) -> bool {
        self.root.unchecked()
    }

    // The pieces in hand that no check waits to find, for the fetch to
    // start from.
    pub(crate) fn have(&self) -> BitVec<u8, Msb0> {
        self.root.have()
    }

    // Whether the piece is in hand and no check waits to find it.
    pub(crate) fn has_piece(&self, piece: u32) -> bool {
        self.root.had(piece)
    }

    // Where the volume's file is kept, written or not.
    pub(crate) fn kept_path(&self, volume: VolumeIndex) -> Option<std::path::PathBuf> {
        self.writer.kept_path(MemberId(volume.0))
    }

    // Each file's bytes in hand, by torrent index.
    pub(crate) fn file_have(&self) -> Vec<u64> {
        self.root.file_have()
    }

    // The files the job has raised, by torrent index; none when it raised none.
    pub(crate) fn selection(&self) -> Vec<usize> {
        self.root.selection()
    }

    // Raise every waiting piece of file `file` to critical, as an object's
    // seek of all of it does.
    #[cfg(test)]
    pub(crate) fn raise(&mut self, file: u32) {
        use crate::pipeline::graph::root::{Ask, Root};
        use weaver_graph::{NodeId, Purpose, Seek, Weight, When};
        let ask = Ask {
            seek: Seek {
                volume: VolumeIndex(file),
                range: ByteRange::new(0, u64::MAX),
                purpose: Purpose::Data,
                weight: Weight::FIRST,
                when: When::Now,
            },
            bidder: NodeId(1),
            in_order: false,
        };
        self.root.answer(&[ask], |_| true);
    }

    // A volume's posted bytes as they are in hand now, for a peer or a
    // check. A share the root does not hold is not read, though an object
    // over it could still give its bytes back: the session is not told it
    // has what the job waits for.
    pub(crate) fn posted(&self, volume: VolumeIndex, range: ByteRange, into: &mut Vec<u8>) -> bool {
        self.root.holds(volume, range)
            && self
                .writer
                .read_in_hand(&self.tree, MemberId(volume.0), range, into)
    }
}
