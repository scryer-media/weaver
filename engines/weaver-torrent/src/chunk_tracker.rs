// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver
//
// Derived from rqbit (https://github.com/ikatson/rqbit), Copyright 2021 Igor Katson,
// licensed under the Apache License 2.0 (see LICENSE-APACHE). Modified by the
// Weaver authors from 2026-10-20 onward.

use std::collections::BTreeSet;

use crate::buffers::ByteBuf;
use crate::core::lengths::{ChunkInfo, Lengths, ValidPieceIndex};
use crate::peer_protocol::Piece;
use anyhow::Context;
use rand::{RngExt, rngs::StdRng, seq::SliceRandom};
use tracing::{debug, trace};

use crate::{
    bitv::{BitV, BoxBitV},
    file_info::FileInfo,
    piece_priority::{FilePriority, PiecePriority},
    storage::TorrentStorage,
    torrent_state::initializing::Checkpoint,
    type_aliases::{BF, BS, FileInfos},
};

struct PieceSelection {
    // How many bytes of each piece lie in the selected files, padding left
    // out, whatever the piece's priority: what of the piece is wanted while
    // the piece is (libtorrent counts a wanted piece less its pad bytes,
    // `calc_bytes` over `piece_picker::want`; the part of a boundary piece an
    // unselected file holds is left out here too).
    file_bytes: Box<[u32]>,

    // The fetch priority of each piece, and how many pieces are at each level.
    priorities: Vec<PiecePriority>,
    priority_counts: [u32; PiecePriority::COUNT],
}

/// The pieces whose count of peers moved since the picker last read the
/// counts: each one, or `all` of them. libtorrent `piece_picker` moves a
/// piece at each `inc_refcount`/`dec_refcount`, and a change to many pieces
/// at once only marks it `m_dirty`, rebuilt at the next pick.
#[derive(Debug, Default, PartialEq, Eq)]
pub(crate) struct Moved {
    pub pieces: Vec<u32>,
    pub all: bool,
}

/// Where a piece sits in the picker's order: its level, by rank from the
/// highest, and its bucket in that level.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct Key {
    level: u8,
    bucket: u32,
}

/// Whether some piece is set in `pieces` and `wanted` and clear in `have`,
/// the raw bytes of three bitfields over the same pieces, eight bytes at a
/// time. A byte past the end of `have` holds no piece.
fn any_wanted_lacking(pieces: &[u8], wanted: &[u8], have: &[u8]) -> bool {
    let n = pieces.len().min(wanted.len());
    let byte = |at: usize| pieces[at] & wanted[at] & !have.get(at).copied().unwrap_or(0) != 0;
    let Some(have) = have.get(..n) else {
        return (0..n).any(byte);
    };
    let whole = n - n % 8;
    let word = |bytes: &[u8], at: usize| {
        u64::from_ne_bytes(bytes[at..at + 8].try_into().expect("eight bytes"))
    };
    (0..whole)
        .step_by(8)
        .any(|at| word(pieces, at) & word(wanted, at) & !word(have, at) != 0)
        || (whole..n).any(byte)
}

/// The rank of `level` among the wanted levels, the highest first.
fn level_rank(level: PiecePriority) -> Option<usize> {
    PiecePriority::WANTED_DESCENDING
        .iter()
        .position(|wanted| *wanted == level)
}

/// The pieces of one level in pick order, bucket by bucket, in random order
/// within a bucket: bucket `b` is `pieces[ends[b - 1]..ends[b]]`, the first
/// from 0 (libtorrent `m_pieces` and `m_priority_boundaries`). Every move
/// costs a step a bucket boundary crossed, never a step a piece.
#[derive(Default)]
struct Ladder {
    pieces: Vec<ValidPieceIndex>,
    ends: Vec<u32>,
}

impl Ladder {
    fn begin(&self, bucket: usize) -> u32 {
        if bucket == 0 {
            0
        } else {
            self.ends[bucket - 1]
        }
    }

    fn put(&mut self, slot: u32, piece: ValidPieceIndex, at: &mut [u32]) {
        self.pieces[slot as usize] = piece;
        at[piece.get_usize()] = slot;
    }

    fn swap(&mut self, a: u32, b: u32, at: &mut [u32]) {
        self.pieces.swap(a as usize, b as usize);
        for slot in [a, b] {
            at[self.pieces[slot as usize].get_usize()] = slot;
        }
    }

    // Buckets up to `bucket` exist; the new ones are empty, at the end.
    fn reach(&mut self, bucket: usize) {
        if self.ends.len() <= bucket {
            self.ends.resize(
                bucket + 1,
                u32::try_from(self.pieces.len()).expect("piece indexes fit in u32"),
            );
        }
    }

    /// libtorrent `piece_picker::add`: `piece` joins `bucket` at a random
    /// place in it. Each later bucket hands its first piece on to its end,
    /// which moves the free slot down to the end of `bucket`.
    fn add(&mut self, piece: ValidPieceIndex, bucket: usize, rng: &mut StdRng, at: &mut [u32]) {
        self.reach(bucket);
        let mut free = u32::try_from(self.pieces.len()).expect("piece indexes fit in u32");
        self.pieces.push(piece);
        for b in (bucket + 1..self.ends.len()).rev() {
            let first = self.begin(b);
            if first != free {
                self.put(free, self.pieces[first as usize], at);
            }
            free = first;
            self.ends[b] += 1;
        }
        self.ends[bucket] += 1;
        let slot = rng.random_range(self.begin(bucket)..=free);
        if slot != free {
            self.put(free, self.pieces[slot as usize], at);
        }
        self.put(slot, piece, at);
    }

    /// libtorrent `piece_picker::remove`: the piece at `slot` of `bucket`
    /// leaves. The last of its bucket takes its place, and the last of each
    /// later bucket the place so freed.
    fn remove(&mut self, slot: u32, bucket: usize, at: &mut [u32]) {
        let mut free = slot;
        for b in bucket..self.ends.len() {
            self.ends[b] -= 1;
            let last = self.ends[b];
            if last != free {
                self.put(free, self.pieces[last as usize], at);
            }
            free = last;
        }
        self.pieces.pop();
    }

    /// libtorrent `piece_picker::update` and `shuffle`: the piece at `slot`
    /// moves from bucket `from` to `to` a boundary at a time, trading places
    /// with the first piece of each bucket it rises out of or the last of
    /// each it sinks out of, then with a random piece of `to`.
    fn update(&mut self, mut slot: u32, from: usize, to: usize, rng: &mut StdRng, at: &mut [u32]) {
        let piece = self.pieces[slot as usize];
        self.reach(to);
        if to < from {
            for b in (to + 1..=from).rev() {
                let first = self.begin(b);
                if first != slot {
                    self.put(slot, self.pieces[first as usize], at);
                }
                slot = first;
                self.ends[b - 1] += 1;
            }
        } else {
            for b in from..to {
                let last = self.ends[b] - 1;
                if last != slot {
                    self.put(slot, self.pieces[last as usize], at);
                }
                slot = last;
                self.ends[b] -= 1;
            }
        }
        self.put(slot, piece, at);
        let other = rng.random_range(self.begin(to)..self.ends[to]);
        self.swap(slot, other, at);
    }
}

/// The queued pieces some peer has, in the order the picker hands them out
/// (libtorrent `piece_picker`'s `m_pieces`, `m_priority_boundaries` and
/// `m_piece_map`). It moves with each change to a piece or its count of
/// peers, and a pick reads it from the front only as far as the piece it
/// hands out.
struct PickIndex {
    // One ladder a wanted level, the highest first.
    levels: [Ladder; PiecePriority::WANTED_DESCENDING.len()],
    // Each piece's key, or None while it is out of the order: had, not
    // wanted, in flight, or had by no peer (libtorrent `piece_pos::priority`
    // -1).
    keys: Vec<Option<Key>>,
    // Each piece's slot in its level's ladder (libtorrent `piece_pos::index`).
    at: Vec<u32>,
    // How many peers have each piece, as the picker last read the counts
    // (libtorrent `piece_pos::peer_count`).
    peers: Vec<u32>,
    // The ladders are void until the next pick rebuilds them (libtorrent
    // `m_dirty`, `update_pieces`).
    dirty: bool,
    // The place a piece takes among its equals.
    rng: StdRng,
}

impl PickIndex {
    fn new(total_pieces: u32) -> Self {
        let n = total_pieces as usize;
        Self {
            levels: Default::default(),
            keys: vec![None; n],
            at: vec![0; n],
            peers: vec![0; n],
            dirty: true,
            rng: rand::make_rng(),
        }
    }

    /// `piece` moves to `key`: it joins, leaves, or moves in the order
    /// (libtorrent `add`, `remove`, `update`).
    fn place(&mut self, piece: ValidPieceIndex, key: Option<Key>) {
        let id = piece.get_usize();
        let old = std::mem::replace(&mut self.keys[id], key);
        if self.dirty || old == key {
            return;
        }
        let Self {
            levels, at, rng, ..
        } = self;
        let slot = at[id];
        match (old, key) {
            (Some(old), Some(new)) if old.level == new.level => levels[new.level as usize].update(
                slot,
                old.bucket as usize,
                new.bucket as usize,
                rng,
                at,
            ),
            (old, new) => {
                if let Some(old) = old {
                    levels[old.level as usize].remove(slot, old.bucket as usize, at);
                }
                if let Some(new) = new {
                    levels[new.level as usize].add(piece, new.bucket as usize, rng, at);
                }
            }
        }
    }

    /// libtorrent `update_pieces`: every piece is laid out anew by its key,
    /// each bucket in random order.
    fn rebuild(&mut self, keys: Vec<Option<Key>>, lengths: &Lengths) {
        let mut buckets: [Vec<Vec<ValidPieceIndex>>; PiecePriority::WANTED_DESCENDING.len()] =
            Default::default();
        for (id, key) in keys.iter().enumerate() {
            let (Some(key), Some(piece)) = (
                key,
                u32::try_from(id)
                    .ok()
                    .and_then(|id| lengths.validate_piece_index(id)),
            ) else {
                continue;
            };
            let level = &mut buckets[key.level as usize];
            let bucket = key.bucket as usize;
            if level.len() <= bucket {
                level.resize_with(bucket + 1, Vec::new);
            }
            level[bucket].push(piece);
        }
        for (ladder, level) in self.levels.iter_mut().zip(buckets) {
            ladder.pieces.clear();
            ladder.ends.clear();
            for mut bucket in level {
                bucket.shuffle(&mut self.rng);
                ladder.pieces.extend(bucket);
                ladder
                    .ends
                    .push(u32::try_from(ladder.pieces.len()).expect("piece indexes fit in u32"));
            }
            for (slot, piece) in ladder.pieces.iter().enumerate() {
                self.at[piece.get_usize()] = u32::try_from(slot).expect("piece indexes fit in u32");
            }
        }
        self.keys = keys;
        self.dirty = false;
    }
}

pub struct ChunkTracker {
    // This forms the basis of a "queue" to pull from.
    // It's set to 1 if we need a piece, but the moment we start requesting a peer,
    // it's set to 0.
    //
    // Initially this is the opposite of "have", until we start making requests.
    // An in-flight request is not in in the queue, and not in "have".
    //
    // needed initial value = selected & !have
    queue_pieces: BF,

    // The pieces out of the queue for a peer or their hash check: taken
    // when reserved, given back when requeued or broken, done when had. A
    // taken piece is never queued, whatever is wanted again meanwhile
    // (libtorrent `piece_picker::is_piece_free` takes no downloading piece).
    taken: BF,

    // How many pieces are queued, and the queued pieces a block of which was
    // received, by index. Kept, not recounted: moved wherever a piece joins
    // or leaves the queue or receives or drops its blocks (libtorrent keeps
    // its started pieces as `m_downloads[piece_downloading]`).
    queued: u32,
    started: BTreeSet<u32>,

    // The queued pieces in pick order, moved at each change to a piece's
    // queue bit, priority, blocks or count of peers.
    index: PickIndex,

    // This has a bit set per each chunk (block) that we have written to the output file.
    // It doesn't mean it's valid yet. Used to track how much is left in each piece.
    chunk_status: BF,

    // These are the pieces that we actually have, fully checked and downloaded,
    // and how many (libtorrent `m_num_have`).
    have: BoxBitV,
    have_count: u32,

    // The pieces that are wanted: those with bytes in the wanted files, less the
    // ones whose priority is Skip. This doesn't change unless prioritize or
    // set_piece_priorities was called.
    selected: BF,

    // What the wanted pieces are derived from. Boxed: it is read only when the
    // queue is walked or the selection changes.
    selection: Box<PieceSelection>,

    // How many bytes do we have per each file.
    per_file_bytes: Vec<u64>,

    lengths: Lengths,

    // Quick to retrieve stats, that MUST be in sync with the BFs
    // above (have/selected).
    hns: HaveNeededSelected,
}

/// What the torrent has and wants, kept in step with the pieces had and
/// wanted. Every reader of finished, progress and what is left reads it here.
#[derive(Default, Debug, PartialEq, Eq, Clone, Copy)]
pub struct HaveNeededSelected {
    // How many bytes of pieces we have downloaded and verified.
    pub have_bytes: u64,
    // How many bytes of wanted pieces we still lack, whole pieces.
    pub needed_bytes: u64,
    // The bytes of the selected files in the wanted pieces, without padding:
    // the progress total (libtorrent `torrent_status::total_wanted`).
    pub wanted_bytes: u64,
    // Of those, the ones in pieces we have (libtorrent
    // `torrent_status::total_wanted_done`).
    pub wanted_done_bytes: u64,
}

impl HaveNeededSelected {
    pub const fn progress(&self) -> u64 {
        self.wanted_done_bytes
    }

    pub const fn total(&self) -> u64 {
        self.wanted_bytes
    }

    /// The wanted bytes still missing.
    pub const fn remaining(&self) -> u64 {
        self.wanted_bytes - self.wanted_done_bytes
    }

    /// Every wanted piece is had (libtorrent `piece_picker::is_finished`).
    pub const fn finished(&self) -> bool {
        self.needed_bytes == 0
    }
}

// Compute the have-status of chunks.
//
// Save as "have_pieces", but there's one bit per chunk (not per piece).
fn compute_chunk_have_status(lengths: &Lengths, have_pieces: &BS) -> anyhow::Result<BF> {
    if have_pieces.len() < lengths.total_pieces() as usize {
        anyhow::bail!(
            "bug: have_pieces.len() < lengths.total_pieces(); {} < {}",
            have_pieces.len(),
            lengths.total_pieces()
        );
    }

    let required_size = lengths.chunk_bitfield_bytes();
    let vec = vec![0u8; required_size];
    let mut chunk_bf = BF::from_boxed_slice(vec.into_boxed_slice());

    for piece in lengths.iter_piece_infos() {
        let chunks = lengths.chunks_per_piece(piece.piece_index) as usize;
        let offset = (lengths.default_chunks_per_piece() * piece.piece_index.get()) as usize;
        let range = offset..(offset + chunks);
        if have_pieces[piece.piece_index.get() as usize] {
            chunk_bf
                .get_mut(range.clone())
                .with_context(|| {
                    format!("bug in bitvec: error getting range {range:?} from chunk_bf")
                })?
                .fill(true);
        }
    }
    Ok(chunk_bf)
}

fn compute_queued_pieces_unchecked(have_pieces: &BS, selected_pieces: &BS) -> BF {
    // it's needed ONLY if it's selected and we don't have it.
    use core::ops::BitAnd;
    use core::ops::Not;

    have_pieces
        .to_bitvec()
        .not()
        .bitand(selected_pieces)
        .into_boxed_bitslice()
}

fn compute_queued_pieces(have_pieces: &BS, selected_pieces: &BS) -> anyhow::Result<BF> {
    if have_pieces.len() != selected_pieces.len() {
        anyhow::bail!(
            "have_pieces.len() != selected_pieces.len(), {} != {}",
            have_pieces.len(),
            selected_pieces.len()
        );
    }

    Ok(compute_queued_pieces_unchecked(
        have_pieces,
        selected_pieces,
    ))
}

/// How many bytes of each piece lie in the selected files, padding left out.
/// A piece with none is not wanted.
pub(crate) fn compute_selected_bytes(
    lengths: &Lengths,
    only_files_is_empty_or_contains: impl Fn(usize) -> bool,
    file_infos: &FileInfos,
) -> Box<[u32]> {
    let mut bytes = vec![0u32; lengths.total_pieces() as usize].into_boxed_slice();
    for (_, fi) in file_infos
        .iter()
        .enumerate()
        .filter(|(_, fi)| !fi.attrs.padding)
        .filter(|(id, _)| only_files_is_empty_or_contains(*id))
    {
        for piece in fi.piece_range.clone() {
            let Some(slot) = bytes.get_mut(piece as usize) else {
                continue;
            };
            #[allow(clippy::cast_possible_truncation)]
            {
                *slot += lengths.size_of_piece_in_file(piece, fi.offset_in_torrent, fi.len) as u32;
            }
        }
    }
    bytes
}

// The pieces with bytes in the selected files.
fn pieces_with_bytes(lengths: &Lengths, file_bytes: &[u32]) -> BF {
    let mut bf = BF::from_boxed_slice(vec![0u8; lengths.piece_bitfield_bytes()].into_boxed_slice());
    for (piece, bytes) in file_bytes.iter().enumerate() {
        if *bytes > 0 {
            bf.set(piece, true);
        }
    }
    bf
}

#[derive(Debug)]
pub enum ChunkMarkingResult {
    PreviouslyCompleted,
    NotCompleted,
    Completed,
}

impl ChunkTracker {
    pub fn new(
        // Have pieces are the ones we have already downloaded and verified.
        have_pieces: BoxBitV,
        // Each piece's bytes in the files the user has selected
        // (`compute_selected_bytes`).
        file_bytes: Box<[u32]>,
        lengths: Lengths,
        file_infos: &FileInfos,
    ) -> anyhow::Result<Self> {
        let selected_pieces = pieces_with_bytes(&lengths, &file_bytes);
        let needed_pieces = compute_queued_pieces(have_pieces.as_slice(), &selected_pieces)
            .context("error computing needed pieces")?;

        // TODO: ideally this needs to be a list based on needed files, e.g.
        // last needed piece for each file. But let's keep simple for now.

        let total_pieces = lengths.total_pieces();
        let mut priority_counts = [0; PiecePriority::COUNT];
        priority_counts[PiecePriority::Normal as usize] = total_pieces;
        let mut ct = Self {
            chunk_status: compute_chunk_have_status(&lengths, have_pieces.as_slice())
                .context("error computing chunk status")?,
            queue_pieces: needed_pieces,
            taken: BF::from_boxed_slice(
                vec![0u8; lengths.piece_bitfield_bytes()].into_boxed_slice(),
            ),
            queued: 0,
            started: BTreeSet::new(),
            index: PickIndex::new(total_pieces),
            have_count: u32::try_from(have_pieces.as_slice().count_ones())
                .expect("piece indexes fit in u32"),
            selection: Box::new(PieceSelection {
                file_bytes,
                priorities: vec![PiecePriority::Normal; total_pieces as usize],
                priority_counts,
            }),
            selected: selected_pieces,
            lengths,
            have: have_pieces,
            hns: HaveNeededSelected::default(),
            per_file_bytes: vec![0; file_infos.len()],
        };
        ct.recalculate_per_file_bytes(file_infos);
        ct.hns = ct.calc_hns();
        ct.recount_queue();
        Ok(ct)
    }

    /// Takes back the blocks a checkpoint recorded as written: each is
    /// received, so no peer is asked for it, and a piece not had keeps
    /// waiting for the rest before its hash is checked (libtorrent
    /// `torrent::read_resume_data` and `piece_picker::mark_as_finished` for
    /// each block of an unfinished piece). `blocks` is in the tracker's own
    /// layout, as [`Self::checkpoint`] gives it.
    pub(crate) fn restore_blocks(&mut self, blocks: &BS) {
        for chunk in blocks.iter_ones() {
            self.chunk_status.set(chunk, true);
        }
        self.recount_queue();
    }

    /// What a checkpoint records of the torrent: the pieces had, and every
    /// block written, those of the pieces not had among them (libtorrent
    /// `torrent::write_resume_data`: `have_pieces`, and `unfinished_pieces`
    /// with the blocks of each downloading piece that are finished), then
    /// the stamp of each of the first `file_count` files of `files`. The
    /// stamps are read after the blocks: a block written in between is
    /// fetched again, and none is recorded that the files do not hold.
    pub(crate) fn checkpoint(&self, files: &dyn TorrentStorage, file_count: usize) -> Checkpoint {
        let have = self.have.as_slice().to_bitvec().into_boxed_bitslice();
        let blocks = self.chunk_status.clone();
        Checkpoint {
            have,
            blocks: Some(blocks),
            files: Some(
                (0..file_count)
                    .map(|file| files.stamp(file).ok().flatten())
                    .collect(),
            ),
        }
    }

    fn recalculate_per_file_bytes(&mut self, file_infos: &FileInfos) {
        for (slot, fi) in self.per_file_bytes.iter_mut().zip(file_infos.iter()) {
            *slot = fi
                .piece_range
                .clone()
                .filter(|p| self.have.as_slice()[*p as usize])
                .map(|id| {
                    self.lengths
                        .size_of_piece_in_file(id, fi.offset_in_torrent, fi.len)
                })
                .sum();
        }
    }

    pub fn get_lengths(&self) -> &Lengths {
        &self.lengths
    }

    pub fn get_have_pieces(&self) -> &dyn BitV {
        &*self.have
    }

    pub fn get_have_pieces_mut(&mut self) -> &mut dyn BitV {
        &mut *self.have
    }

    pub fn reserve_needed_piece(&mut self, index: ValidPieceIndex) {
        self.taken.set(index.get_usize(), true);
        self.set_queued(index, false);
    }

    // Every change to the queue passes here, piece by piece, but the
    // selection's wholesale one, which recounts. The queue holds only
    // wanted pieces with a block still to come that no peer or hash check
    // has taken: one with every block is being checked, and a piece not
    // wanted keeps its blocks out of it (libtorrent
    // `piece_picker::is_piece_free` takes neither a filtered piece, nor a
    // downloading one, nor one whose blocks are all finished).
    fn set_queued(&mut self, piece: ValidPieceIndex, queued: bool) {
        let queued = queued
            && !self.taken[piece.get_usize()]
            && self.selected[piece.get_usize()]
            && self.received_chunks(piece) < self.lengths.chunks_per_piece(piece) as usize;
        if self.queue_pieces.replace(piece.get_usize(), queued) != queued {
            if queued {
                self.queued += 1;
            } else {
                self.queued -= 1;
            }
        }
        self.reposition(piece);
    }

    // `piece` joined or left the queue, changed priority, or received its
    // first block or dropped its blocks: it joins or leaves the started
    // pieces, and moves in the pick order.
    fn reposition(&mut self, piece: ValidPieceIndex) {
        if self.queue_pieces[piece.get_usize()] && self.received_chunks(piece) > 0 {
            self.started.insert(piece.get());
        } else {
            self.started.remove(&piece.get());
        }
        let key = self.key(piece);
        self.index.place(piece, key);
    }

    // Where `piece` belongs in the pick order (libtorrent
    // `piece_pos::priority`): its level first, then the peers that have it,
    // the fewest first, a started piece before an open one as rare. Out of
    // the order unless queued, wanted and had by some peer.
    fn key(&self, piece: ValidPieceIndex) -> Option<Key> {
        let id = piece.get_usize();
        let peers = self.index.peers[id];
        if !self.queue_pieces[id] || peers == 0 {
            return None;
        }
        let level = level_rank(self.selection.priorities[id])?;
        let open = self.received_chunks(piece) == 0;
        Some(Key {
            level: u8::try_from(level).ok()?,
            bucket: 2 * (peers - 1) + open as u32,
        })
    }

    /// The picker starts afresh, its random choices drawn from `rng`: the
    /// counts of peers it last read are void, and the next pick reads them
    /// all.
    pub(crate) fn start_picking(&mut self, rng: StdRng) {
        self.index.rng = rng;
        self.index.dirty = true;
    }

    /// The pick order catches up with the counts of peers that `moved`
    /// names, reading each through `peers`: a piece whose count changed
    /// moves. Every piece is read and laid out anew when `moved` names them
    /// all or the order was voided (libtorrent `update_pieces` when
    /// `m_dirty`).
    pub(crate) fn catch_up(&mut self, moved: Moved, peers: impl Fn(ValidPieceIndex) -> u32) {
        self.index.dirty |= moved.all;
        if !self.index.dirty {
            for id in moved.pieces {
                let Some(piece) = self.lengths.validate_piece_index(id) else {
                    continue;
                };
                let count = peers(piece);
                if std::mem::replace(&mut self.index.peers[piece.get_usize()], count) != count {
                    let key = self.key(piece);
                    self.index.place(piece, key);
                }
            }
            return;
        }
        let total = self.lengths.total_pieces();
        let mut keys = vec![None; total as usize];
        for piece in (0..total).filter_map(|id| self.lengths.validate_piece_index(id)) {
            self.index.peers[piece.get_usize()] = peers(piece);
            keys[piece.get_usize()] = self.key(piece);
        }
        self.index.rebuild(keys, &self.lengths);
    }

    /// The queued pieces of `level` some peer has, in the order a peer is
    /// handed them: the rarest first, a started piece before an open one as
    /// rare, in random order among equals (libtorrent `pick_pieces` walking
    /// `m_pieces` from `priority_begin`). Read after [`Self::catch_up`].
    pub(crate) fn pick_order(&self, level: PiecePriority) -> &[ValidPieceIndex] {
        debug_assert!(
            !self.index.dirty,
            "the pick order is read before it is rebuilt"
        );
        level_rank(level).map_or(&[], |rank| &self.index.levels[rank].pieces)
    }

    /// Whether `piece` is in the pick order at `level`.
    pub(crate) fn picks_at(&self, piece: ValidPieceIndex, level: PiecePriority) -> bool {
        self.index.keys[piece.get_usize()]
            .is_some_and(|key| Some(key.level as usize) == level_rank(level))
    }

    /// How many peers had `piece` when the picker last read the counts.
    pub(crate) fn peers_with(&self, piece: ValidPieceIndex) -> u32 {
        self.index.peers[piece.get_usize()]
    }

    /// The pick order holds exactly the pieces whose key puts them in it,
    /// each at its slot, in its level's ladder and its bucket's range, the
    /// buckets in order.
    #[cfg(test)]
    pub(crate) fn check_index(&self) {
        let index = &self.index;
        assert!(!index.dirty, "the pick order is void");
        for (rank, ladder) in index.levels.iter().enumerate() {
            assert!(ladder.ends.is_sorted(), "{:?}", ladder.ends);
            assert_eq!(
                ladder.ends.last().copied().unwrap_or(0),
                u32::try_from(ladder.pieces.len()).unwrap()
            );
            for (slot, piece) in ladder.pieces.iter().enumerate() {
                let key = index.keys[piece.get_usize()].expect("a piece in the order has a key");
                let slot = u32::try_from(slot).unwrap();
                assert_eq!(key.level as usize, rank, "piece {piece}");
                assert_eq!(index.at[piece.get_usize()], slot, "piece {piece}");
                let bucket = key.bucket as usize;
                assert!(
                    (ladder.begin(bucket)..ladder.ends[bucket]).contains(&slot),
                    "piece {piece} at {slot} out of bucket {bucket}: {:?}",
                    ladder.ends
                );
            }
        }
        let total = self.lengths.total_pieces();
        for piece in (0..total).filter_map(|id| self.lengths.validate_piece_index(id)) {
            assert_eq!(
                index.keys[piece.get_usize()],
                self.key(piece),
                "piece {piece}"
            );
        }
        assert_eq!(
            index.keys.iter().flatten().count(),
            index.levels.iter().map(|l| l.pieces.len()).sum::<usize>()
        );
    }

    // The counts of the queue, after a change to all of it.
    fn recount_queue(&mut self) {
        self.index.dirty = true;
        self.queued =
            u32::try_from(self.queue_pieces.count_ones()).expect("piece indexes fit in u32");
        let lengths = self.lengths;
        let chunk_status = &self.chunk_status;
        self.started = self
            .queue_pieces
            .iter_ones()
            .filter(|id| {
                u32::try_from(*id)
                    .ok()
                    .and_then(|id| lengths.validate_piece_index(id))
                    .and_then(|piece| chunk_status.get(lengths.chunk_range(piece)))
                    .is_some_and(|chunks| chunks.any())
            })
            .map(|id| u32::try_from(id).expect("piece indexes fit in u32"))
            .collect();
    }

    pub fn get_hns(&self) -> &HaveNeededSelected {
        &self.hns
    }

    fn calc_hns(&self) -> HaveNeededSelected {
        let mut hns = HaveNeededSelected::default();
        for piece in self.lengths.iter_piece_infos() {
            let id = piece.piece_index.get() as usize;
            let len = piece.len as u64;
            let wanted = u64::from(self.selection.file_bytes[id]);
            let is_have = self.have.as_slice()[id];
            let is_selected = self.selected[id];
            let is_needed = is_selected && !is_have;
            hns.have_bytes += len * (is_have as u64);
            hns.needed_bytes += len * (is_needed as u64);
            hns.wanted_bytes += wanted * (is_selected as u64);
            hns.wanted_done_bytes += wanted * ((is_selected && is_have) as u64);
        }
        hns
    }

    /// What the whole torrent still lacks, whatever is wanted, held or
    /// paused (libtorrent `torrent::bytes_left`).
    pub fn bytes_left(&self) -> u64 {
        self.lengths.total_length() - self.hns.have_bytes
    }

    /// The pieces a peer may be handed, in index order: wanted, not had, and
    /// not in flight. Which one goes first is the piece picker's call; in
    /// sequential mode it walks these.
    pub(crate) fn iter_queued_pieces(&self) -> impl Iterator<Item = ValidPieceIndex> + '_ {
        self.queue_pieces
            .iter_ones()
            .filter_map(|id| self.lengths.validate_piece_index(id.try_into().ok()?))
    }

    /// The fetch priority of `piece`.
    pub(crate) fn priority(&self, piece: ValidPieceIndex) -> PiecePriority {
        self.selection.priorities[piece.get_usize()]
    }

    /// How many chunks of `piece` were received.
    pub(crate) fn received_chunks(&self, piece: ValidPieceIndex) -> usize {
        self.chunk_status
            .get(self.lengths.chunk_range(piece))
            .map_or(0, |chunks| chunks.count_ones())
    }

    /// How many pieces we have.
    pub(crate) fn have_count(&self) -> usize {
        self.have_count as usize
    }

    /// The queued pieces a block of which was received, in index order.
    pub(crate) fn started_pieces(&self) -> impl Iterator<Item = ValidPieceIndex> + '_ {
        self.started
            .iter()
            .filter_map(|id| self.lengths.validate_piece_index(*id))
    }

    /// How many queued pieces received a block.
    pub(crate) fn started_count(&self) -> usize {
        self.started.len()
    }

    /// Whether every queued piece received a block.
    pub(crate) fn every_queued_piece_started(&self) -> bool {
        self.queued as usize == self.started.len()
    }

    pub(crate) fn is_piece_have(&self, id: ValidPieceIndex) -> bool {
        self.have.as_slice()[id.get() as usize]
    }

    /// Queues `index` again unless we have it. The chunks it already received
    /// stay received, so whoever takes it next asks only for the rest. Returns
    /// whether it was queued.
    pub fn requeue_piece_if_not_have(&mut self, index: ValidPieceIndex) -> bool {
        if self.is_piece_have(index) {
            return false;
        }
        self.taken.set(index.get_usize(), false);
        self.set_queued(index, true);
        true
    }

    pub fn mark_piece_broken_if_not_have(&mut self, index: ValidPieceIndex) {
        if self.is_piece_have(index) {
            return;
        }
        debug!("marking piece={} as broken", index);
        if let Some(s) = self.chunk_status.get_mut(self.lengths.chunk_range(index)) {
            s.fill(false);
        }
        self.taken.set(index.get_usize(), false);
        self.set_queued(index, true);
    }

    pub fn mark_piece_downloaded(&mut self, idx: ValidPieceIndex) {
        let id = idx.get() as usize;
        self.taken.set(id, false);
        if !self.have.as_slice()[id] {
            self.have.as_slice_mut().set(id, true);
            self.have_count += 1;
            let len = self.lengths.piece_length(idx) as u64;
            self.hns.have_bytes += len;
            if self.selected[id] {
                self.hns.needed_bytes -= len;
                self.hns.wanted_done_bytes += u64::from(self.selection.file_bytes[id]);
            }
        }
    }

    /// Whether `chunk` was received.
    pub(crate) fn is_chunk_received(&self, chunk: &ChunkInfo) -> bool {
        self.chunk_status
            .get(chunk.absolute_index as usize)
            .is_some_and(|received| *received)
    }

    /// The chunks of `piece` not yet received.
    pub fn iter_missing_chunks(
        &self,
        piece: ValidPieceIndex,
    ) -> impl Iterator<Item = ChunkInfo> + '_ {
        self.lengths
            .iter_chunk_infos(piece)
            .filter(|chunk| !self.is_chunk_received(chunk))
    }

    pub fn is_chunk_ready_to_upload(&self, chunk: &ChunkInfo) -> bool {
        self.have
            .as_slice()
            .get(chunk.piece_index.get() as usize)
            .map(|b| *b)
            .unwrap_or(false)
    }

    pub fn get_remaining_bytes(&self) -> u64 {
        self.hns.remaining()
    }

    // return true if the whole piece is marked downloaded
    pub fn mark_chunk_downloaded(
        &mut self,
        piece: &Piece<ByteBuf<'_>>,
    ) -> Option<ChunkMarkingResult> {
        let chunk_info = self.lengths.chunk_info_from_received_data(
            self.lengths.validate_piece_index(piece.index)?,
            piece.begin,
            piece.len().try_into().unwrap(),
        )?;
        let chunk_range = self.lengths.chunk_range(chunk_info.piece_index);
        let chunk_range = self.chunk_status.get_mut(chunk_range).unwrap();
        if chunk_range.all() {
            return Some(ChunkMarkingResult::PreviouslyCompleted);
        }
        chunk_range.set(chunk_info.chunk_index as usize, true);
        trace!(
            "piece={}, chunk_info={:?}, bits={:?}",
            piece.index, chunk_info, chunk_range,
        );

        let completed = chunk_range.all();
        self.reposition(chunk_info.piece_index);
        if completed {
            return Some(ChunkMarkingResult::Completed);
        }
        Some(ChunkMarkingResult::NotCompleted)
    }

    /// The files' levels changed: each piece's bytes in the wanted files,
    /// and every piece's priority, are set anew from them (libtorrent
    /// `torrent::update_piece_priorities`, then `prioritize_pieces`).
    /// `pieces` is [`crate::piece_priority::file_to_piece_priorities`] of
    /// `files`, with any piece set since over it.
    pub fn prioritize(
        &mut self,
        file_infos: &FileInfos,
        files: &[FilePriority],
        pieces: &[PiecePriority],
    ) -> HaveNeededSelected {
        self.selection.file_bytes = compute_selected_bytes(
            &self.lengths,
            |idx| files.get(idx).is_none_or(|level| level.wanted()),
            file_infos,
        );
        let sel = &mut *self.selection;
        for (slot, priority) in sel.priorities.iter_mut().zip(pieces) {
            sel.priority_counts[*slot as usize] -= 1;
            sel.priority_counts[*priority as usize] += 1;
            *slot = *priority;
        }
        self.apply_selection()
    }

    /// Set the fetch priority of the given pieces. Moving a piece to or from Skip
    /// changes what is wanted, as deselecting or selecting a file does.
    pub fn set_piece_priorities(
        &mut self,
        priorities: impl IntoIterator<Item = (ValidPieceIndex, PiecePriority)>,
    ) -> HaveNeededSelected {
        let mut wanted_changed = false;
        for (index, priority) in priorities {
            let sel = &mut *self.selection;
            let prev = std::mem::replace(&mut sel.priorities[index.get_usize()], priority);
            sel.priority_counts[prev as usize] -= 1;
            sel.priority_counts[priority as usize] += 1;
            wanted_changed |= (prev == PiecePriority::Skip) != (priority == PiecePriority::Skip);
            self.reposition(index);
        }
        if wanted_changed {
            self.apply_selection();
        }
        self.hns
    }

    // Recompute what is wanted (the wanted files' pieces less the skipped ones),
    // and bring the queue in line with it. A piece no longer wanted leaves
    // the queue and keeps its blocks; one wanted again comes back with them
    // (libtorrent `piece_picker::set_piece_priority` filters a piece, it
    // does not forget it).
    fn apply_selection(&mut self) -> HaveNeededSelected {
        let mut selected = pieces_with_bytes(&self.lengths, &self.selection.file_bytes);
        if self.selection.priority_counts[PiecePriority::Skip as usize] > 0 {
            for (idx, p) in self.selection.priorities.iter().enumerate() {
                if *p == PiecePriority::Skip {
                    selected.set(idx, false);
                }
            }
        }
        let prev_selected = std::mem::replace(&mut self.selected, selected);

        // prev_selected=false and selected=true and have=false: queue the piece
        // again, with the blocks it had.
        {
            let mut b = BF::from_boxed_slice(
                vec![0u8; self.lengths.piece_bitfield_bytes()].into_boxed_slice(),
            );
            for idx in self
                .selected
                .iter_ones()
                .filter(|idx| !prev_selected[*idx] && !self.have.as_slice()[*idx])
            {
                b.set(idx, true);
            }

            for idx in b.iter_ones() {
                #[allow(clippy::cast_possible_truncation)]
                if let Some(idx) = self.lengths.validate_piece_index(idx as u32) {
                    self.set_queued(idx, true);
                }
            }
        }

        // selected=false, have=false: don't need the piece, and don't have it - cancel downloading it
        {
            // TODO: is there a better way to write this?
            // self.queue_pieces &= self.have | self.selected;
            let mut have_or_selected: BF = self.selected.clone();
            have_or_selected |= self.have.as_slice();
            self.queue_pieces &= have_or_selected;
        }
        self.recount_queue();

        self.hns = self.calc_hns();
        self.hns
    }

    pub(crate) fn get_selected_pieces(&self) -> &BF {
        &self.selected
    }

    pub fn is_file_finished(&self, file_info: &FileInfo) -> bool {
        self.have
            .as_slice()
            .get(file_info.piece_range_usize())
            .map(|r| r.all())
            .unwrap_or(true)
    }

    pub(crate) fn is_finished(&self) -> bool {
        self.get_hns().finished()
    }

    /// Whether `pieces` names a piece we want and lack: a selected one we do
    /// not have (libtorrent `peer_connection::do_update_interest`).
    pub(crate) fn wants_any_of(&self, pieces: &BF) -> bool {
        !self.is_finished()
            && any_wanted_lacking(
                pieces.as_raw_slice(),
                self.selected.as_raw_slice(),
                self.have.as_bytes(),
            )
    }

    pub fn per_file_have_bytes(&self) -> &[u64] {
        &self.per_file_bytes
    }

    /// The piece's bytes in the file are counted had. True when they made
    /// the file whole: it lacked bytes before and lacks none now. An empty
    /// file is never made whole by a piece, so never true for it.
    pub fn update_file_have_on_piece_completed(
        &mut self,
        piece_id: ValidPieceIndex,
        file_id: usize,
        file_info: &FileInfo,
    ) -> bool {
        let diff_have = self.lengths.size_of_piece_in_file(
            piece_id.get(),
            file_info.offset_in_torrent,
            file_info.len,
        );
        let had = &mut self.per_file_bytes[file_id];
        let lacked = *had < file_info.len;
        *had += diff_have;
        lacked && *had >= file_info.len
    }
}

#[cfg(test)]
mod tests {
    use crate::core::{constants::CHUNK_SIZE, lengths::Lengths};

    use crate::{
        bitv::BitV,
        chunk_tracker::HaveNeededSelected,
        file_info::FileInfo,
        piece_priority::{FilePriority, file_to_piece_priorities},
        type_aliases::{BF, FileInfos},
    };

    use super::{ChunkTracker, compute_chunk_have_status, compute_selected_bytes};

    /// The files `wanted` names at Normal, every other one Ignored.
    fn select(ct: &mut ChunkTracker, files: &FileInfos, wanted: &[usize]) -> HaveNeededSelected {
        let levels: Vec<FilePriority> = (0..files.len())
            .map(|file| {
                if wanted.contains(&file) {
                    FilePriority::Normal
                } else {
                    FilePriority::Ignored
                }
            })
            .collect();
        let pieces = file_to_piece_priorities(ct.get_lengths(), files, &levels, false);
        ct.prioritize(files, &levels, &pieces)
    }

    #[test]
    fn wanted_lacking_matches_a_walk_bit_by_bit() {
        let mut seed = 0x9e37_79b9_u32;
        let mut byte = || {
            seed = seed.wrapping_mul(1_664_525).wrapping_add(1_013_904_223);
            (seed >> 24) as u8
        };
        for n in 0..40usize {
            for have_len in [n, n.saturating_sub(3), n + 2] {
                for _ in 0..20 {
                    let pieces: Vec<u8> = (0..n).map(|_| byte() & byte()).collect();
                    let wanted: Vec<u8> = (0..n).map(|_| byte() | byte()).collect();
                    let have: Vec<u8> = (0..have_len).map(|_| byte() | byte()).collect();
                    let bit = |b: &[u8], i: usize| {
                        b.get(i / 8).is_some_and(|v| v & (0x80 >> (i % 8)) != 0)
                    };
                    let walk =
                        (0..n * 8).any(|i| bit(&pieces, i) && bit(&wanted, i) && !bit(&have, i));
                    assert_eq!(
                        super::any_wanted_lacking(&pieces, &wanted, &have),
                        walk,
                        "{n} bytes, have {have_len}"
                    );
                }
            }
        }
    }

    #[test]
    fn test_compute_chunk_status() {
        // Create the most obnoxious lengths, and ensure it doesn't break in that case.
        let piece_length = CHUNK_SIZE * 2 + 1;
        let l = Lengths::new(piece_length as u64 * 2 + 1, piece_length).unwrap();

        assert_eq!(l.total_pieces(), 3);
        assert_eq!(l.default_chunks_per_piece(), 3);
        assert_eq!(l.total_chunks(), 7);

        {
            let mut have_pieces =
                BF::from_boxed_slice(vec![u8::MAX; l.piece_bitfield_bytes()].into_boxed_slice());
            have_pieces.set(0, false);

            let chunks = compute_chunk_have_status(&l, &have_pieces).unwrap();
            assert!(!chunks[0]);
            assert!(!chunks[1]);
            assert!(!chunks[2]);
            assert!(chunks[3]);
            assert!(chunks[4]);
            assert!(chunks[5]);
            assert!(chunks[6]);
        }

        {
            let mut have_pieces =
                BF::from_boxed_slice(vec![u8::MAX; l.piece_bitfield_bytes()].into_boxed_slice());
            have_pieces.set(1, false);

            let chunks = compute_chunk_have_status(&l, &have_pieces).unwrap();
            dbg!(&chunks);
            assert!(chunks[0]);
            assert!(chunks[1]);
            assert!(chunks[2]);
            assert!(!chunks[3]);
            assert!(!chunks[4]);
            assert!(!chunks[5]);
            assert!(chunks[6]);
        }

        {
            let mut have_pieces =
                BF::from_boxed_slice(vec![u8::MAX; l.piece_bitfield_bytes()].into_boxed_slice());
            have_pieces.set(2, false);

            let chunks = compute_chunk_have_status(&l, &have_pieces).unwrap();
            dbg!(&chunks);
            assert!(chunks[0]);
            assert!(chunks[1]);
            assert!(chunks[2]);
            assert!(chunks[3]);
            assert!(chunks[4]);
            assert!(chunks[5]);
            assert!(!chunks[6]);
        }

        {
            // A more reasonable case.
            let piece_length = CHUNK_SIZE * 2;
            let l = Lengths::new(piece_length as u64 * 2 + 1, piece_length).unwrap();

            assert_eq!(l.total_pieces(), 3);
            assert_eq!(l.default_chunks_per_piece(), 2);
            assert_eq!(l.total_chunks(), 5);

            {
                let mut have_pieces = BF::from_boxed_slice(
                    vec![u8::MAX; l.piece_bitfield_bytes()].into_boxed_slice(),
                );
                have_pieces.set(1, false);

                let chunks = compute_chunk_have_status(&l, &have_pieces).unwrap();
                dbg!(&chunks);
                assert!(chunks[0]);
                assert!(chunks[1]);
                assert!(!chunks[2]);
                assert!(!chunks[3]);
                assert!(chunks[4]);
            }

            {
                let mut have_pieces = BF::from_boxed_slice(
                    vec![u8::MAX; l.piece_bitfield_bytes()].into_boxed_slice(),
                );
                have_pieces.set(2, false);

                let chunks = compute_chunk_have_status(&l, &have_pieces).unwrap();
                dbg!(&chunks);
                assert!(chunks[0]);
                assert!(chunks[1]);
                assert!(chunks[2]);
                assert!(chunks[3]);
                assert!(!chunks[4]);
            }
        }
    }

    #[test]
    fn test_selecting_files_by_level() {
        let piece_len = CHUNK_SIZE * 2 + 1;
        let total_len = piece_len as u64 * 2 + 1;
        let l = Lengths::new(total_len, piece_len).unwrap();
        assert_eq!(l.total_pieces(), 3);
        assert_eq!(l.total_chunks(), 7);

        let all_files = vec![
            FileInfo {
                relative_filename: "0".into(),
                offset_in_torrent: 0,
                piece_range: 0..1,
                len: piece_len as u64,
                attrs: Default::default(),
            },
            FileInfo {
                relative_filename: "1".into(),
                offset_in_torrent: piece_len as u64,
                piece_range: 1..2,
                len: 1,
                attrs: Default::default(),
            },
            FileInfo {
                relative_filename: "2".into(),
                offset_in_torrent: piece_len as u64 + 1,
                piece_range: 1..1,
                len: 0,
                attrs: Default::default(),
            },
            FileInfo {
                relative_filename: "3".into(),
                offset_in_torrent: piece_len as u64 + 1,
                piece_range: 1..3,
                len: piece_len as u64,
                attrs: Default::default(),
            },
        ];

        let bf_len = l.piece_bitfield_bytes();
        let initial_have = BF::from_boxed_slice(vec![0u8; bf_len].into_boxed_slice());
        let initial_selected = {
            let mut bf = BF::from_boxed_slice(vec![0u8; bf_len].into_boxed_slice());
            bf.get_mut(0..3).unwrap().fill(true);
            bf
        };

        // Initially, we need all files and all pieces.
        let mut ct = ChunkTracker::new(
            initial_have.clone().into_dyn(),
            compute_selected_bytes(&l, |_| true, &all_files),
            l,
            &all_files,
        )
        .unwrap();

        // Select all file, no changes.
        assert_eq!(
            select(&mut ct, &all_files, &[0, 1, 2, 3]),
            HaveNeededSelected {
                have_bytes: 0,
                wanted_bytes: total_len,
                wanted_done_bytes: 0,
                needed_bytes: total_len,
            }
        );
        assert_eq!(ct.have.as_slice(), initial_have.as_bitslice());
        assert_eq!(ct.queue_pieces, initial_selected);

        // Select only the first file.
        println!("Select only the first file.");
        assert_eq!(
            select(&mut ct, &all_files, &[0]),
            HaveNeededSelected {
                have_bytes: 0,
                wanted_bytes: all_files[0].len,
                wanted_done_bytes: 0,
                needed_bytes: all_files[0].len,
            }
        );
        assert!(ct.queue_pieces[0]);
        assert!(!ct.queue_pieces[1]);
        assert!(!ct.queue_pieces[2]);

        // Select only the second file.
        assert_eq!(
            select(&mut ct, &all_files, &[1]),
            HaveNeededSelected {
                have_bytes: 0,
                wanted_bytes: all_files[1].len,
                wanted_done_bytes: 0,
                needed_bytes: piece_len as u64,
            }
        );
        assert!(!ct.queue_pieces[0]);
        assert!(ct.queue_pieces[1]);
        assert!(!ct.queue_pieces[2]);

        // Select only the third file (zero sized one!).
        assert_eq!(
            select(&mut ct, &all_files, &[2]),
            HaveNeededSelected {
                have_bytes: 0,
                wanted_bytes: 0,
                wanted_done_bytes: 0,
                needed_bytes: 0,
            }
        );
        assert!(!ct.queue_pieces[0]);
        assert!(!ct.queue_pieces[1]);
        assert!(!ct.queue_pieces[2]);

        // Select only the fourth file.
        assert_eq!(
            select(&mut ct, &all_files, &[3]),
            HaveNeededSelected {
                have_bytes: 0,
                wanted_bytes: all_files[3].len,
                wanted_done_bytes: 0,
                needed_bytes: (piece_len + 1) as u64,
            }
        );
        assert!(!ct.queue_pieces[0]);
        assert!(ct.queue_pieces[1]);
        assert!(ct.queue_pieces[2]);

        // Select first and last file
        assert_eq!(
            select(&mut ct, &all_files, &[0, 3]),
            HaveNeededSelected {
                have_bytes: 0,
                wanted_bytes: all_files[0].len + all_files[3].len,
                wanted_done_bytes: 0,
                needed_bytes: all_files[0].len + all_files[3].len + 1,
            }
        );
        assert!(ct.queue_pieces[0]);
        assert!(ct.queue_pieces[1]);
        assert!(ct.queue_pieces[2]);

        // Select all files
        assert_eq!(
            select(&mut ct, &all_files, &[0, 1, 2, 3]),
            HaveNeededSelected {
                have_bytes: 0,
                wanted_bytes: total_len,
                wanted_done_bytes: 0,
                needed_bytes: total_len
            }
        );
        assert!(ct.queue_pieces[0]);
        assert!(ct.queue_pieces[1]);
        assert!(ct.queue_pieces[2]);
    }

    // A piece with every block is never queued: between its last block and
    // its hash check passing it is taken, and a piece whose last block came
    // with no one holding it is not free either (libtorrent
    // `piece_picker::is_piece_free`). Wanting its file again meanwhile
    // queues neither, so no second peer is handed a piece in hand.
    #[test]
    fn test_a_piece_with_every_block_is_not_queued_when_its_file_is_wanted_again() {
        let piece_len = CHUNK_SIZE * 2;
        let l = Lengths::new(piece_len as u64 * 3, piece_len).unwrap();
        let files = vec![FileInfo {
            relative_filename: "0".into(),
            offset_in_torrent: 0,
            piece_range: 0..3,
            len: piece_len as u64 * 3,
            attrs: Default::default(),
        }];
        let bf_len = l.piece_bitfield_bytes();
        let have = BF::from_boxed_slice(vec![0u8; bf_len].into_boxed_slice());
        let selected = compute_selected_bytes(&l, |_| true, &files);
        let mut ct = ChunkTracker::new(have.into_dyn(), selected, l, &files).unwrap();
        let p = |i| l.validate_piece_index(i).unwrap();
        let block = vec![0u8; CHUNK_SIZE as usize];
        for (i, chunk) in [(0, 0), (0, 1), (1, 0), (1, 1)] {
            ct.mark_chunk_downloaded(&crate::peer_protocol::Piece::from_data(
                i,
                chunk * CHUNK_SIZE,
                &block[..],
            ))
            .unwrap();
        }
        // Piece 0 goes to its hash check; piece 1's blocks all came while
        // no one held it, and it waits in the queue for its check.
        ct.reserve_needed_piece(p(0));
        assert!(!ct.queue_pieces[0]);

        // Meanwhile the user ignores the file and wants it again.
        select(&mut ct, &files, &[]);
        select(&mut ct, &files, &[0]);
        assert!(!ct.queue_pieces[0], "a piece in its hash check is queued");
        assert!(!ct.queue_pieces[1], "a piece with every block is queued");
        assert!(ct.queue_pieces[2]);

        // The hash passes.
        ct.mark_piece_downloaded(p(0));
        assert!(ct.is_piece_have(p(0)));
        assert!(!ct.queue_pieces[0]);
    }

    // The files' levels give every piece its priority, and a piece set on its
    // own is set over them until the levels change again, which rewrites
    // every piece (libtorrent `torrent::update_piece_priorities` calls
    // `prioritize_pieces` with the whole vector).
    #[test]
    fn test_file_levels_and_piece_priorities_compose() {
        use crate::piece_priority::PiecePriority;

        let piece_len = CHUNK_SIZE;
        let l = Lengths::new(piece_len as u64 * 4, piece_len).unwrap();
        // File 0 is pieces 0..=1 and half of 2; file 1 is the other half of 2, and 3.
        let half = piece_len as u64 / 2;
        let files = vec![
            FileInfo {
                relative_filename: "0".into(),
                offset_in_torrent: 0,
                piece_range: 0..3,
                len: piece_len as u64 * 2 + half,
                attrs: Default::default(),
            },
            FileInfo {
                relative_filename: "1".into(),
                offset_in_torrent: piece_len as u64 * 2 + half,
                piece_range: 2..4,
                len: piece_len as u64 + half,
                attrs: Default::default(),
            },
        ];
        let bf_len = l.piece_bitfield_bytes();
        let have = BF::from_boxed_slice(vec![0u8; bf_len].into_boxed_slice());
        let selected = compute_selected_bytes(&l, |_| true, &files);
        let mut ct = ChunkTracker::new(have.into_dyn(), selected, l, &files).unwrap();
        let p = |i| l.validate_piece_index(i).unwrap();
        let wanted = |ct: &ChunkTracker| -> Vec<usize> { ct.selected.iter_ones().collect() };

        // Ignoring file 1 keeps piece 2, which file 0 still needs.
        select(&mut ct, &files, &[0]);
        assert_eq!(wanted(&ct), vec![0, 1, 2]);

        // A priority does not bring back a piece only an ignored file needs.
        ct.set_piece_priorities([(p(3), PiecePriority::High)]);
        assert_eq!(wanted(&ct), vec![0, 1, 2]);
        assert!(!ct.queue_pieces[3]);

        // Skip removes a piece a wanted file needs.
        ct.set_piece_priorities([(p(1), PiecePriority::Skip)]);
        assert_eq!(wanted(&ct), vec![0, 2]);
        assert!(!ct.queue_pieces[1]);

        // A change to the levels gives every piece its files' level again.
        select(&mut ct, &files, &[0, 1]);
        assert_eq!(wanted(&ct), vec![0, 1, 2, 3]);
        assert!(ct.queue_pieces[1]);
        assert_eq!(ct.priority(p(3)), PiecePriority::Normal);
        assert_eq!(ct.get_hns().needed_bytes, piece_len as u64 * 4);
    }

    // What is left is the whole torrent less what is had, whatever is
    // wanted (libtorrent `torrent::bytes_left`); the progress total is the
    // selected files' bytes, without padding or the part of a boundary piece
    // an unselected file holds; finished is every wanted piece had.
    #[test]
    fn left_progress_and_finished_have_one_definition_each() {
        use crate::core::torrent_metainfo::FileDetailsAttrs;

        let piece_len = CHUNK_SIZE;
        let p64 = piece_len as u64;
        let half = p64 / 2;
        let l = Lengths::new(p64 * 4, piece_len).unwrap();
        let file = |name: &str, offset: u64, len: u64, pieces, padding| FileInfo {
            relative_filename: name.into(),
            offset_in_torrent: offset,
            piece_range: pieces,
            len,
            attrs: FileDetailsAttrs {
                padding,
                ..Default::default()
            },
        };
        // a: piece 0 and half of 1; padding: the rest of 1; b: piece 2 and
        // half of 3; c, not selected: the rest of 3.
        let files = vec![
            file("a", 0, p64 + half, 0..2, false),
            file("pad", p64 + half, half, 1..2, true),
            file("b", p64 * 2, p64 + half, 2..4, false),
            file("c", p64 * 3 + half, half, 3..4, false),
        ];
        let bf_len = l.piece_bitfield_bytes();
        let have = BF::from_boxed_slice(vec![0u8; bf_len].into_boxed_slice());
        let selected = compute_selected_bytes(&l, |id| id != 3, &files);
        let mut ct = ChunkTracker::new(have.into_dyn(), selected, l, &files).unwrap();
        let p = |i| l.validate_piece_index(i).unwrap();

        assert_eq!(ct.bytes_left(), p64 * 4);
        assert_eq!(ct.get_hns().total(), files[0].len + files[2].len);
        assert_eq!(ct.get_hns().progress(), 0);
        assert!(!ct.is_finished());

        // The boundary piece counts only b's half toward progress, and the
        // whole of it toward what is left.
        ct.mark_piece_downloaded(p(3));
        assert_eq!(ct.bytes_left(), p64 * 3);
        assert_eq!(ct.get_hns().progress(), half);
        assert_eq!(ct.get_hns().remaining(), p64 * 2 + half);

        // Deselecting b leaves what is left as it was.
        select(&mut ct, &files, &[0]);
        assert_eq!(ct.bytes_left(), p64 * 3);
        assert_eq!(ct.get_hns().total(), files[0].len);
        assert_eq!(ct.get_hns().progress(), 0);
        select(&mut ct, &files, &[0, 2]);
        assert_eq!(ct.get_hns().progress(), half);

        // Finished flips with the last wanted piece and not before.
        ct.mark_piece_downloaded(p(0));
        ct.mark_piece_downloaded(p(2));
        assert!(!ct.is_finished());
        ct.mark_piece_downloaded(p(1));
        assert!(ct.is_finished());
        assert_eq!(ct.get_hns().progress(), ct.get_hns().total());
        assert_eq!(ct.bytes_left(), 0);
        assert_eq!(ct.get_hns(), &ct.calc_hns());
    }

    // The counts a pick reads are kept, and agree with a recount of the
    // bitfields after every kind of move.
    #[test]
    fn the_kept_counts_follow_every_move() {
        use crate::piece_priority::PiecePriority;

        let piece_len = CHUNK_SIZE * 2;
        let l = Lengths::new(piece_len as u64 * 4, piece_len).unwrap();
        let files = vec![FileInfo {
            relative_filename: "0".into(),
            offset_in_torrent: 0,
            piece_range: 0..4,
            len: piece_len as u64 * 4,
            attrs: Default::default(),
        }];
        let bf_len = l.piece_bitfield_bytes();
        let mut have = BF::from_boxed_slice(vec![0u8; bf_len].into_boxed_slice());
        have.set(3, true);
        let selected = compute_selected_bytes(&l, |_| true, &files);
        let mut ct = ChunkTracker::new(have.into_dyn(), selected, l, &files).unwrap();
        let p = |i| l.validate_piece_index(i).unwrap();
        let block = vec![0u8; CHUNK_SIZE as usize];
        let deliver = |ct: &mut ChunkTracker, i: u32, chunk: u32| {
            ct.mark_chunk_downloaded(&crate::peer_protocol::Piece::from_data(
                i,
                chunk * CHUNK_SIZE,
                &block[..],
            ))
            .unwrap();
        };
        // (queued, started, had), kept and recounted.
        let counts = |ct: &ChunkTracker| {
            let recounted = (
                ct.queue_pieces.count_ones(),
                ct.iter_queued_pieces()
                    .filter(|p| ct.received_chunks(*p) > 0)
                    .map(|p| p.get())
                    .collect::<Vec<_>>(),
                ct.have.as_slice().count_ones(),
            );
            let kept = (
                ct.queued as usize,
                ct.started_pieces().map(|p| p.get()).collect::<Vec<_>>(),
                ct.have_count(),
            );
            assert_eq!(kept, recounted);
            assert_eq!(
                ct.every_queued_piece_started(),
                kept.0 == kept.1.len(),
                "{kept:?}"
            );
            kept
        };
        assert_eq!(counts(&ct), (3, vec![], 1));

        // A block of a queued piece starts it; a piece taken leaves the queue.
        deliver(&mut ct, 1, 0);
        ct.reserve_needed_piece(p(0));
        assert_eq!(counts(&ct), (2, vec![1], 1));

        // The taken piece comes back with its block; another is taken.
        deliver(&mut ct, 0, 1);
        ct.requeue_piece_if_not_have(p(0));
        ct.reserve_needed_piece(p(2));
        assert_eq!(counts(&ct), (2, vec![0, 1], 1));
        assert!(ct.every_queued_piece_started());

        // A failed piece comes back without its blocks.
        ct.mark_piece_broken_if_not_have(p(1));
        assert_eq!(counts(&ct), (2, vec![0], 1));
        assert!(!ct.every_queued_piece_started());

        // A passed piece is had; one requeued that we have stays out.
        ct.mark_piece_downloaded(p(2));
        assert!(!ct.requeue_piece_if_not_have(p(2)));
        assert_eq!(counts(&ct), (2, vec![0], 2));

        // Skipped, a piece leaves the queue; wanted again, it comes back with
        // its blocks.
        ct.set_piece_priorities([(p(0), PiecePriority::Skip)]);
        assert_eq!(counts(&ct), (1, vec![], 2));
        ct.set_piece_priorities([(p(0), PiecePriority::Normal)]);
        assert_eq!(counts(&ct), (2, vec![0], 2));
    }
}
