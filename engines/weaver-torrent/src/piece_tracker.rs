// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver
//
// Derived from rqbit (https://github.com/ikatson/rqbit), Copyright 2021 Igor Katson,
// licensed under the Apache License 2.0 (see LICENSE-APACHE). Modified by the
// Weaver authors from 2026-10-20 onward.

//! Coordinates piece download state by wrapping ChunkTracker with inflight piece tracking.
//!
//! This module provides the [`PieceTracker`] type which encapsulates the relationship between
//! queued pieces (in ChunkTracker) and in-flight pieces (being downloaded by a peer).
//!
//! The key invariant maintained is: a piece is in exactly one state at any time:
//! - HAVE (completed)
//! - QUEUED (available to download)
//! - IN_FLIGHT (currently being downloaded)
//! - NOT_NEEDED (not selected for download)

use std::{
    cmp::Reverse,
    collections::{BTreeSet, HashMap, HashSet},
    time::Duration,
};

use rand::{RngExt, SeedableRng, rngs::StdRng};
use tokio::time::Instant;

use crate::buffers::ByteBuf;
use crate::core::lengths::{ChunkInfo, ValidPieceIndex};
use crate::peer_protocol::Piece;

use crate::{
    chunk_tracker::{ChunkMarkingResult, ChunkTracker, Moved},
    file_info::FileInfo,
    piece_priority::{FilePriority, PiecePriority},
    type_aliases::{FileInfos, PeerHandle},
};

/// libtorrent `initial_picker_threshold`: until we have this many pieces a
/// peer is handed a random piece rather than the rarest, so we have whole
/// pieces to trade sooner.
const INITIAL_PICKER_THRESHOLD: usize = 4;

/// libtorrent `pick_pieces`: past this many chunks in started pieces, the
/// started ones are finished first.
const MAX_PARTIAL_CHUNKS: usize = 2048;

/// Tracks a piece currently being downloaded.
#[derive(Debug, Clone)]
pub struct InflightPiece {
    pub peer: PeerHandle,
    pub started: Instant,
    // The blocks of the piece asked of another peer as well, in the end
    // game, one such peer a block: the block index and the peer.
    duplicates: Vec<(u32, PeerHandle)>,
}

/// What becomes of a block that arrives (libtorrent
/// `peer_connection::incoming_piece`).
#[derive(Debug, PartialEq, Eq)]
pub enum Arrival {
    /// The peer's copy is the one written. The other peers the block is
    /// asked of, its piece's owner and the peer doubling it in the end
    /// game, are to have their requests cancelled (libtorrent
    /// `torrent::cancel_block`).
    Write { cancel: Vec<PeerHandle> },
    /// We have the block, or another copy of it arrived first (libtorrent
    /// counts it as redundant bytes).
    Discard,
}

/// The sender of every block of one piece, by block index; None for a
/// block whose sender is not known.
#[derive(Debug, Default, Clone, PartialEq, Eq)]
pub(crate) struct Senders(Vec<Option<PeerHandle>>);

impl Senders {
    /// The sender of each block, by block index.
    pub fn blocks(&self) -> &[Option<PeerHandle>] {
        &self.0
    }

    /// The peers that sent a block of the piece, each once.
    pub fn participants(&self) -> BTreeSet<PeerHandle> {
        self.0.iter().flatten().copied().collect()
    }

    /// Whether one peer sent the whole piece. A block of unknown sender
    /// counts as sent by another (libtorrent `torrent::piece_failed`).
    pub fn sole_sender(&self) -> bool {
        self.0.iter().all(Option::is_some) && self.participants().len() == 1
    }
}

impl FromIterator<Option<PeerHandle>> for Senders {
    fn from_iter<T: IntoIterator<Item = Option<PeerHandle>>>(iter: T) -> Self {
        Self(iter.into_iter().collect())
    }
}

/// In the end game, a piece goes over to a peer once its owner has held it
/// this many times the peer's average piece time.
const TAKE_OVER_AFTER: u32 = 3;

/// Result of attempting to acquire a piece.
#[derive(Debug)]
pub enum AcquireResult {
    /// A new piece was reserved from the queue.
    Reserved(ValidPieceIndex),
    /// A piece was taken over from the peer that held it, in the end game.
    Stolen {
        piece: ValidPieceIndex,
        from_peer: PeerHandle,
    },
    /// The end game: a block of a piece in flight with another peer, which
    /// this peer is to ask for too.
    EndGame {
        piece: ValidPieceIndex,
        chunk: ChunkInfo,
    },
    /// No pieces are available for this peer.
    NoneAvailable,
    /// The piece the peer would be handed waits on room the storage
    /// refused: it is handed out once the storage has room again.
    NoRoom,
}

/// How a peer is picked for, besides the pieces it has.
#[derive(Debug, Default, Clone, Copy)]
pub struct PickOptions<'a> {
    /// The peer is snubbed: it is handed the commonest piece rather than the
    /// rarest, so a rare one does not wait on it (libtorrent `reverse`).
    pub reverse: bool,
    /// The peer is on parole: a piece it helped send failed its hash check.
    /// It is handed only a whole piece no one has started, so a piece that
    /// fails again has one sender, and it takes no piece another peer is
    /// downloading (libtorrent `on_parole`).
    pub on_parole: bool,
    /// The pieces the peer suggested, the latest first. After the started
    /// pieces, the first of them it may take goes before the picker's own
    /// order (libtorrent `suggested_pieces`).
    pub suggested: &'a [ValidPieceIndex],
    /// The torrent downloads in order: the top-level pieces first, rarest
    /// first, then every other wanted piece by index, whatever its level
    /// (libtorrent `sequential`, from `torrent::is_sequential_download`).
    pub sequential: bool,
}

/// Parameters for acquiring a piece.
pub struct AcquireRequest<'a, I, P, A, S>
where
    I: Iterator<Item = ValidPieceIndex>,
    P: Fn(ValidPieceIndex) -> bool,
    A: Fn(ValidPieceIndex) -> u32,
    S: Fn(ValidPieceIndex) -> bool,
{
    /// The peer requesting a piece.
    pub peer: PeerHandle,
    /// The peer's average piece download time (for steal calculations). None
    /// for a peer that steals from no one: one with no piece downloaded yet,
    /// or one that is snubbed.
    pub peer_avg_time: Option<Duration>,
    /// Pieces a stream needs, in the order it needs them: handed out before
    /// any other.
    pub priority_pieces: I,
    /// Returns true if the peer has the given piece.
    pub peer_has_piece: P,
    /// How many connected peers have the given piece.
    pub availability: A,
    /// The pieces whose count of peers moved since the last pick: the
    /// picker reads their counts through `availability` and moves them in
    /// its order before it picks.
    pub moved: Moved,
    /// How many peers are connected.
    pub num_peers: usize,
    /// How many chunk requests the peer has outstanding.
    pub outstanding_requests: usize,
    pub options: PickOptions<'a>,
    /// Returns true if the piece can be stolen (e.g., not locked for writing).
    pub can_steal: S,
}

/// Coordinates piece download state.
///
/// Wraps a [`ChunkTracker`] with tracking of which pieces are currently being downloaded
/// (in-flight) and by which peer. This ensures that:
///
/// - A piece is only assigned to one peer at a time (unless stolen)
/// - Pieces are properly requeued when a peer dies
/// - State transitions maintain invariants
pub struct PieceTracker {
    chunks: ChunkTracker,
    inflight: HashMap<ValidPieceIndex, InflightPiece>,
    // Who sent each received block of the pieces not yet handed to their
    // hash check. A record names only blocks its piece keeps: it outlives a
    // sender that goes away, as the blocks do, and leaves with them
    // (libtorrent keeps a block's downloader in the piece picker,
    // `block_info::peer`, read by `piece_picker::get_downloaders`).
    senders: HashMap<ValidPieceIndex, Senders>,
    // The blocks a copy of which arrived and is being written, by absolute
    // index: any other copy is redundant (libtorrent
    // `piece_picker::mark_as_writing`).
    writing: HashSet<u32>,
    // Breaks ties between equally rare pieces, and starts the initial random
    // pick.
    rng: StdRng,
}

impl PieceTracker {
    // === CONSTRUCTION ===

    /// Create a new PieceTracker wrapping the given ChunkTracker.
    pub fn new(chunks: ChunkTracker) -> Self {
        Self::with_rng(chunks, rand::make_rng())
    }

    fn with_rng(mut chunks: ChunkTracker, mut rng: StdRng) -> Self {
        chunks.start_picking(StdRng::from_rng(&mut rng));
        Self {
            chunks,
            inflight: HashMap::new(),
            senders: HashMap::new(),
            writing: HashSet::new(),
            rng,
        }
    }

    /// Read-only access to the underlying ChunkTracker.
    pub fn chunks(&self) -> &ChunkTracker {
        &self.chunks
    }

    /// Consume the PieceTracker, requeuing any in-flight pieces.
    ///
    /// This is used when pausing a torrent - any pieces that were being downloaded
    /// need to be put back in the queue so they can be re-downloaded on resume.
    pub fn into_chunks(mut self) -> ChunkTracker {
        // Requeue all in-flight pieces so they'll be re-downloaded on resume
        for piece in self.inflight.into_keys() {
            self.chunks.mark_piece_broken_if_not_have(piece);
        }
        self.chunks
    }

    // === PIECE ACQUISITION ===

    /// Attempt to acquire a piece for the requesting peer.
    ///
    /// The acquisition strategy is:
    /// 1. Reserve a piece: a stream's next piece first, then the one
    ///    [`Self::pick`] chooses
    /// 2. In the end game, take over a piece its owner has held at least
    ///    [`TAKE_OVER_AFTER`] times the requesting peer's average piece
    ///    time, or else ask for a block already asked of another peer
    ///
    /// A block in flight with one peer is asked of another only in the end
    /// game, once every queued piece has a block received: no piece is left
    /// that no peer has started. Before it, a copy the first peer already
    /// sent would arrive for a block we have.
    ///
    /// If `Stolen` is returned, the caller MUST call `peers.on_steal()` to notify
    /// the old peer and update counters.
    #[cfg(test)]
    pub fn acquire_piece<I, P, A, S>(
        &mut self,
        req: AcquireRequest<'_, I, P, A, S>,
    ) -> AcquireResult
    where
        I: Iterator<Item = ValidPieceIndex>,
        P: Fn(ValidPieceIndex) -> bool,
        A: Fn(ValidPieceIndex) -> u32,
        S: Fn(ValidPieceIndex) -> bool,
    {
        self.acquire_admitted_piece(req, |_| true)
    }

    /// [`Self::acquire_piece`], with `admit` asked of the one piece step 1 would
    /// reserve. A piece it refuses stays queued and no other is reserved in its
    /// place: the refusal is a lack of room, not of that piece. A steal takes a
    /// piece already in flight, so it is never asked.
    pub fn acquire_admitted_piece<I, P, A, S, F>(
        &mut self,
        mut req: AcquireRequest<'_, I, P, A, S>,
        admit: F,
    ) -> AcquireResult
    where
        I: Iterator<Item = ValidPieceIndex>,
        P: Fn(ValidPieceIndex) -> bool,
        A: Fn(ValidPieceIndex) -> u32,
        S: Fn(ValidPieceIndex) -> bool,
        F: FnOnce(ValidPieceIndex) -> bool,
    {
        self.chunks
            .catch_up(std::mem::take(&mut req.moved), &req.availability);

        // A peer on parole takes no piece another peer is downloading.
        let may_steal = !req.options.on_parole;

        // 1. A stream's next pieces first, in the order it needs them, then
        // the picker's choice.
        let needed = req.priority_pieces.by_ref().find(|&piece| {
            !self.chunks.is_piece_have(piece)
                && !self.inflight.contains_key(&piece)
                && may_hand(&self.chunks, &req.peer_has_piece, req.options, piece)
        });
        let needed = needed.or_else(|| self.pick(&req));

        if let Some(piece) = needed
            && admit(piece)
        {
            return self.reserve_piece(piece, req.peer);
        }
        // Past here, a piece to hand out is one the storage refused.
        let refused = needed.is_some();

        // 2. The end game: every piece not in flight has a block received,
        // and the peer has no request outstanding (libtorrent
        // `request_a_block` with `strict_end_game_mode`: a busy block only
        // once the download queue holds every piece wanted, and only by a
        // peer whose queues are empty). A piece held three times as long as
        // the peer takes to download one goes over whole; otherwise the
        // peer doubles a block.
        let end_game = req.outstanding_requests == 0 && self.chunks.every_queued_piece_started();
        if may_steal && end_game {
            if let Some(result) = self.try_steal(&req) {
                return result;
            }
            if let Some(result) = self.double_a_block(&req) {
                return result;
            }
        }

        if refused {
            AcquireResult::NoRoom
        } else {
            AcquireResult::NoneAvailable
        }
    }

    /// The end game's pick for the peer `req` asks for, after libtorrent
    /// `piece_picker::pick_pieces` past its "end-game mode" mark: a random
    /// piece the peer has that another peer is downloading, then a random
    /// block of it neither received, nor being written, nor asked of a
    /// third peer already. Each block is asked of one peer besides its
    /// owner at most.
    fn double_a_block<I, P, A, S>(
        &mut self,
        req: &AcquireRequest<'_, I, P, A, S>,
    ) -> Option<AcquireResult>
    where
        I: Iterator<Item = ValidPieceIndex>,
        P: Fn(ValidPieceIndex) -> bool,
        A: Fn(ValidPieceIndex) -> u32,
        S: Fn(ValidPieceIndex) -> bool,
    {
        let Self {
            chunks,
            inflight,
            writing,
            rng,
            ..
        } = self;
        let mut pieces: Vec<(ValidPieceIndex, Vec<ChunkInfo>)> = inflight
            .iter()
            .filter(|(piece, info)| info.peer != req.peer && (req.peer_has_piece)(**piece))
            .map(|(piece, info)| {
                let free: Vec<_> = chunks
                    .iter_missing_chunks(*piece)
                    .filter(|c| {
                        !writing.contains(&c.absolute_index)
                            && !info.duplicates.iter().any(|(b, _)| *b == c.chunk_index)
                    })
                    .collect();
                (*piece, free)
            })
            .filter(|(_, free)| !free.is_empty())
            .collect();
        // In index order, so the pick follows the rng alone.
        pieces.sort_unstable_by_key(|(piece, _)| piece.get());
        if pieces.is_empty() {
            return None;
        }
        let (piece, free) = pieces.swap_remove(rng.random_range(0..pieces.len()));
        let chunk = free[rng.random_range(0..free.len())];
        inflight
            .get_mut(&piece)?
            .duplicates
            .push((chunk.chunk_index, req.peer));
        Some(AcquireResult::EndGame { piece, chunk })
    }

    /// The queued piece to hand the peer `req` asks for, after libtorrent
    /// `piece_picker::pick_pieces`. It walks the pick order the chunk tracker
    /// keeps, and the started pieces, only as far as the piece it hands out:
    ///
    /// - Only the highest priority level with a piece the peer may take.
    /// - Started pieces first while they are many: more than one and a half
    ///   per connected peer, or more than [`MAX_PARTIAL_CHUNKS`] chunks, or
    ///   while we have fewer than [`INITIAL_PICKER_THRESHOLD`] pieces. The
    ///   rarest goes first, then the one most received.
    /// - Then the peer's latest suggestion it may take.
    /// - Then, once we have [`INITIAL_PICKER_THRESHOLD`] pieces, the first
    ///   piece of the order it may take: the rarest, a started one before an
    ///   unstarted one as rare, a random one among those equal. A snubbed
    ///   peer walks the order from its end, and takes the commonest.
    /// - Before that, the first piece it may take from a random place in
    ///   the order.
    /// - In sequential mode, neither: the first top-level piece of the order
    ///   it may take, then the first other piece it may take by index from
    ///   the first wanted piece not had, whatever its level; started pieces
    ///   go first only while they are many (libtorrent `pick_pieces` with
    ///   `sequential`, walking `m_pieces` while at `top_priority`, then
    ///   from `m_cursor`).
    fn pick<I, P, A, S>(&mut self, req: &AcquireRequest<'_, I, P, A, S>) -> Option<ValidPieceIndex>
    where
        I: Iterator<Item = ValidPieceIndex>,
        P: Fn(ValidPieceIndex) -> bool,
        A: Fn(ValidPieceIndex) -> u32,
        S: Fn(ValidPieceIndex) -> bool,
    {
        let Self {
            chunks,
            inflight,
            rng,
            ..
        } = self;
        let chunks = &*chunks;
        let may = |p: &ValidPieceIndex| may_hand(chunks, &req.peer_has_piece, req.options, *p);
        let sequential = req.options.sequential;
        // Sequential replaces both the random start and rarest first
        // (libtorrent `peer_connection::picker_options`).
        let rarest_first = !sequential && chunks.have_count() >= INITIAL_PICKER_THRESHOLD;
        let random = !sequential && !rarest_first;
        // The first piece of a level's order the peer may take, the order
        // read the way this pick reads it: from a random place before we
        // have INITIAL_PICKER_THRESHOLD pieces, from its end for a snubbed
        // peer, from its front otherwise.
        let mut walk = |order: &[ValidPieceIndex]| {
            let start = if !random || order.is_empty() {
                0
            } else {
                rng.random_range(0..order.len())
            };
            let (head, tail) = order.split_at(start);
            let mut walked = tail.iter().chain(head).copied();
            if rarest_first && req.options.reverse {
                walked.rfind(|p| may(p))
            } else {
                walked.find(|p| may(p))
            }
        };
        // The tier a piece is picked in: its level, but in sequential mode
        // every level below the top is one tier, walked by index.
        let top = PiecePriority::WANTED_DESCENDING[0];
        let tier = |p: ValidPieceIndex| {
            let level = chunks.priority(p);
            (!sequential || level == top).then_some(level)
        };
        // The highest tier with a piece the peer may take, and the piece the
        // walk of its order stops at.
        let walked = if sequential {
            walk(chunks.pick_order(top)).or_else(|| chunks.iter_queued_pieces().find(|p| may(p)))
        } else {
            PiecePriority::WANTED_DESCENDING
                .into_iter()
                .find_map(|level| walk(chunks.pick_order(level)))
        }?;
        let tier_of_walked = tier(walked);

        let partials = inflight.len() + chunks.started_count();
        let chunks_per_piece = chunks.get_lengths().default_chunks_per_piece() as usize;
        let prioritize_partials = random
            || partials > req.num_peers * 3 / 2
            || partials * chunks_per_piece > MAX_PARTIAL_CHUNKS;

        if prioritize_partials {
            let mut started_pieces = chunks
                .started_pieces()
                .filter(|p| tier(*p) == tier_of_walked && may(p));
            let partial = if rarest_first {
                started_pieces
                    .min_by_key(|p| (chunks.peers_with(*p), Reverse(chunks.received_chunks(*p))))
            } else {
                started_pieces.next()
            };
            if partial.is_some() {
                return partial;
            }
        }

        let suggested = req.options.suggested.iter().copied();
        suggested
            .filter(|p| chunks.picks_at(*p, chunks.priority(*p)) && tier(*p) == tier_of_walked)
            .find(|p| may(p))
            .or(Some(walked))
    }

    /// Reserve a piece: remove from queue, add to inflight.
    fn reserve_piece(&mut self, piece: ValidPieceIndex, peer: PeerHandle) -> AcquireResult {
        self.chunks.reserve_needed_piece(piece);
        self.inflight.insert(
            piece,
            InflightPiece {
                peer,
                started: Instant::now(),
                duplicates: Vec::new(),
            },
        );
        AcquireResult::Reserved(piece)
    }

    /// Try to take over a piece another peer has held at least
    /// [`TAKE_OVER_AFTER`] times the requesting peer's average piece time.
    fn try_steal<I, P, A, S>(
        &mut self,
        req: &AcquireRequest<'_, I, P, A, S>,
    ) -> Option<AcquireResult>
    where
        I: Iterator<Item = ValidPieceIndex>,
        P: Fn(ValidPieceIndex) -> bool,
        A: Fn(ValidPieceIndex) -> u32,
        S: Fn(ValidPieceIndex) -> bool,
    {
        let my_avg = req.peer_avg_time?;
        let min_elapsed = my_avg * TAKE_OVER_AFTER;

        // The piece held longest by another peer, held at least
        // TAKE_OVER_AFTER times the requesting peer's piece time, and that
        // the requesting peer has (can download)
        let (piece, old_peer, _) = self
            .inflight
            .iter()
            .filter(|(_, info)| info.peer != req.peer)
            .filter(|(p, _)| (req.peer_has_piece)(**p))
            .map(|(p, info)| (*p, info.peer, info.started.elapsed()))
            .filter(|(_, _, elapsed)| *elapsed >= min_elapsed)
            .max_by_key(|(_, _, elapsed)| *elapsed)?;

        // Check can_steal (e.g., per_piece_lock)
        if !(req.can_steal)(piece) {
            return None;
        }

        // Update ownership (piece stays in inflight, just changes owner)
        let info = self.inflight.get_mut(&piece)?;
        info.peer = req.peer;
        info.started = Instant::now();
        info.duplicates.retain(|(_, peer)| *peer != req.peer);

        Some(AcquireResult::Stolen {
            piece,
            from_peer: old_peer,
        })
    }

    // === PIECE COMPLETION ===

    /// The piece has every block and goes to its hash check: it leaves
    /// inflight tracking, or the queue when its last block came after it was
    /// given back, and the record of who sent its blocks leaves with it.
    ///
    /// Returns how long it was in flight, if it was, and who sent its
    /// blocks. Does NOT mark the piece as downloaded: the caller checks its
    /// hash and calls `mark_piece_hash_ok` or `mark_piece_hash_failed`.
    pub fn take_inflight(&mut self, piece: ValidPieceIndex) -> (Option<Duration>, Senders) {
        self.chunks.reserve_needed_piece(piece);
        let senders = self.senders.remove(&piece).unwrap_or_default();
        let started = self.inflight.remove(&piece).map(|i| i.started.elapsed());
        (started, senders)
    }

    /// Mark piece as downloaded after successful hash verification.
    pub fn mark_piece_hash_ok(&mut self, piece: ValidPieceIndex) {
        self.chunks.mark_piece_downloaded(piece);
    }

    /// Mark piece as failed after hash verification failure - requeues the piece.
    pub fn mark_piece_hash_failed(&mut self, piece: ValidPieceIndex) {
        self.chunks.mark_piece_broken_if_not_have(piece);
    }

    /// Release all pieces owned by a peer: it died, choked us, or rejected
    /// a request.
    ///
    /// Moves all pieces owned by the peer from IN_FLIGHT back to QUEUED, with
    /// the chunks they already received, so the next owner asks only for the
    /// rest. Returns the number of pieces released.
    pub fn release_pieces_owned_by(&mut self, peer: PeerHandle) -> usize {
        let owned: Vec<_> = self
            .inflight
            .iter()
            .filter(|(_, info)| info.peer == peer)
            .map(|(p, _)| *p)
            .collect();
        // The blocks it doubled in the end game are asked of their owners
        // alone again.
        for info in self.inflight.values_mut() {
            info.duplicates.retain(|(_, holder)| *holder != peer);
        }
        self.requeue(owned)
    }

    /// The newest piece `peer` owns that waits on it alone goes back to the
    /// queue, with the chunks it already received: one none of whose
    /// missing chunks is free, each one asked of the peer (`asked`), doubled
    /// by another or being written. A piece with a free chunk stays the
    /// peer's: it is not held up while any peer may still fetch that chunk.
    /// The peer's requests stay outstanding, and a block it still sends is
    /// kept (libtorrent `peer_connection::snub_peer` times out one request
    /// a snub, the last of the download queue, only when its piece has no
    /// free block, and leaves it in the download queue). Returns the piece
    /// released, if any.
    pub fn release_piece_blocked_by(
        &mut self,
        peer: PeerHandle,
        asked: impl Fn(&ChunkInfo) -> bool,
    ) -> Option<ValidPieceIndex> {
        let blocked = self
            .inflight
            .iter()
            .filter(|(_, info)| info.peer == peer)
            .filter(|(piece, info)| {
                self.chunks.iter_missing_chunks(**piece).all(|chunk| {
                    asked(&chunk)
                        || self.writing.contains(&chunk.absolute_index)
                        || info.duplicates.iter().any(|(b, _)| *b == chunk.chunk_index)
                })
            })
            .max_by_key(|(piece, info)| (info.started, piece.get()))
            .map(|(p, _)| *p)?;
        self.requeue(vec![blocked]);
        Some(blocked)
    }

    // `pieces` leave flight and are queued again, with the chunks they
    // already received, so the next owner asks only for the rest.
    fn requeue(&mut self, pieces: Vec<ValidPieceIndex>) -> usize {
        for piece in &pieces {
            self.inflight.remove(piece);
            self.chunks.requeue_piece_if_not_have(*piece);
        }
        pieces.len()
    }

    /// `peer`'s copy of `chunk` arrived: whether it is the one written. A
    /// block we lack is data we want, whoever owns its piece now, if anyone:
    /// the first copy is written, and every other peer it is asked of has
    /// its request cancelled (libtorrent `peer_connection::incoming_piece`:
    /// `piece_picker::mark_as_writing` takes a block of any piece we do not
    /// have, then `torrent::cancel_block` cancels it with the other peers).
    /// A later copy, or a block of a piece we have, is not.
    #[cfg(test)]
    pub fn block_arrived(&mut self, chunk: &ChunkInfo, peer: PeerHandle) -> Arrival {
        self.admitted_block_arrived(chunk, peer, |_| true)
    }

    /// [`Self::block_arrived`], with the block's piece asked of `admit`, the
    /// gate a piece passes to be handed out, when no peer owns it: a piece
    /// in flight passed it when it was handed out, but one queued may hold
    /// no room, as one that failed its hash while its block was owed. A
    /// block of a piece `admit` refuses is discarded.
    pub fn admitted_block_arrived(
        &mut self,
        chunk: &ChunkInfo,
        peer: PeerHandle,
        admit: impl FnOnce(ValidPieceIndex) -> bool,
    ) -> Arrival {
        let piece = chunk.piece_index;
        if self.chunks.is_piece_have(piece)
            || self.chunks.is_chunk_received(chunk)
            || self.writing.contains(&chunk.absolute_index)
            || !(self.inflight.contains_key(&piece) || admit(piece))
        {
            return Arrival::Discard;
        }
        self.writing.insert(chunk.absolute_index);
        let mut cancel = Vec::new();
        if let Some(info) = self.inflight.get_mut(&chunk.piece_index) {
            if let Some(at) = info
                .duplicates
                .iter()
                .position(|(block, _)| *block == chunk.chunk_index)
            {
                cancel.push(info.duplicates.swap_remove(at).1);
            }
            cancel.push(info.peer);
            cancel.retain(|other| *other != peer);
        }
        Arrival::Write { cancel }
    }

    /// The write of `chunk` failed: it is no longer being written, and a
    /// later copy is taken (libtorrent `piece_picker::write_failed`).
    pub fn write_failed(&mut self, chunk: &ChunkInfo) {
        self.writing.remove(&chunk.absolute_index);
    }

    // === QUERIES ===

    /// Get the inflight info for a piece, if it's currently being downloaded.
    #[cfg(test)]
    pub fn get_inflight(&self, piece: ValidPieceIndex) -> Option<&InflightPiece> {
        self.inflight.get(&piece)
    }

    /// Whether `peer` owns the in-flight `piece`. Only the owner requests a
    /// piece's chunks, so a peer whose piece was stolen stops requesting it.
    pub fn is_owned_by(&self, piece: ValidPieceIndex, peer: PeerHandle) -> bool {
        self.inflight
            .get(&piece)
            .is_some_and(|info| info.peer == peer)
    }

    /// Whether `peer` requests chunks of `piece`: it owns it, or asks for a
    /// block of it in the end game that has not arrived. A peer whose piece
    /// was stolen stops requesting it.
    pub fn may_request(&self, piece: ValidPieceIndex, peer: PeerHandle) -> bool {
        self.is_owned_by(piece, peer)
            || self
                .inflight
                .get(&piece)
                .is_some_and(|info| info.duplicates.iter().any(|(_, p)| *p == peer))
    }

    /// The chunks of `piece` not yet received. A peer that steals a
    /// half-received piece requests only these.
    pub fn missing_chunks(&self, piece: ValidPieceIndex) -> impl Iterator<Item = ChunkInfo> + '_ {
        self.chunks.iter_missing_chunks(piece)
    }

    /// The chunks of `piece` `peer` is to request: its owner, those not yet
    /// received; a peer that doubles a block of it in the end game, that
    /// block.
    pub fn chunks_to_request(
        &self,
        piece: ValidPieceIndex,
        peer: PeerHandle,
    ) -> impl Iterator<Item = ChunkInfo> + '_ {
        let info = self.inflight.get(&piece);
        let owner = info.is_some_and(|info| info.peer == peer);
        self.missing_chunks(piece).filter(move |chunk| {
            owner || info.is_some_and(|info| info.duplicates.contains(&(chunk.chunk_index, peer)))
        })
    }

    /// Check if a piece is currently in-flight.
    #[allow(dead_code)]
    pub fn is_inflight(&self, piece: ValidPieceIndex) -> bool {
        self.inflight.contains_key(&piece)
    }

    /// Get the number of pieces currently in-flight.
    #[allow(dead_code)]
    pub fn inflight_count(&self) -> usize {
        self.inflight.len()
    }

    // === PASS-THROUGH METHODS ===

    /// `sender` delivered the chunk `piece` carries, and the piece keeps it,
    /// unless it already had every chunk. Returns the result indicating if
    /// the piece is complete.
    pub fn mark_chunk_downloaded(
        &mut self,
        piece: &Piece<ByteBuf<'_>>,
        sender: PeerHandle,
    ) -> Option<ChunkMarkingResult> {
        let lengths = self.chunks.get_lengths();
        let index = lengths.validate_piece_index(piece.index)?;
        let chunk = lengths.chunk_info_from_received_data(
            index,
            piece.begin,
            piece.len().try_into().ok()?,
        )?;
        self.writing.remove(&chunk.absolute_index);
        let result = self.chunks.mark_chunk_downloaded(piece)?;
        if !matches!(result, ChunkMarkingResult::PreviouslyCompleted) {
            let lengths = self.chunks.get_lengths();
            let blocks = lengths.chunks_per_piece(index) as usize;
            let record = self
                .senders
                .entry(index)
                .or_insert_with(|| Senders(vec![None; blocks]));
            if let Some(slot) = record.0.get_mut(chunk.chunk_index as usize) {
                *slot = Some(sender);
            }
        }
        Some(result)
    }

    /// The files' levels changed; see [`ChunkTracker::prioritize`].
    pub fn prioritize(
        &mut self,
        file_infos: &FileInfos,
        files: &[FilePriority],
        pieces: &[PiecePriority],
    ) -> crate::chunk_tracker::HaveNeededSelected {
        self.chunks.prioritize(file_infos, files, pieces)
    }

    /// Set the fetch priority of the given pieces.
    pub fn set_piece_priorities(
        &mut self,
        priorities: impl IntoIterator<Item = (ValidPieceIndex, PiecePriority)>,
    ) -> crate::chunk_tracker::HaveNeededSelected {
        self.chunks.set_piece_priorities(priorities)
    }

    /// Update per-file have bytes when a piece completes. Returns remaining bytes for the file.
    pub fn update_file_have_on_piece_completed(
        &mut self,
        piece_id: ValidPieceIndex,
        file_id: usize,
        file_info: &FileInfo,
    ) -> u64 {
        self.chunks
            .update_file_have_on_piece_completed(piece_id, file_id, file_info)
    }

    /// Flush the have pieces bitfield to disk.
    pub fn flush_have_pieces(&mut self, flush_async: bool) -> anyhow::Result<()> {
        self.chunks.get_have_pieces_mut().flush(flush_async)
    }
}

/// Whether `piece` may go to a peer that has the pieces `peer_has_piece`
/// names: it has the piece, and when on parole, no one has delivered any of it.
fn may_hand(
    chunks: &ChunkTracker,
    peer_has_piece: &impl Fn(ValidPieceIndex) -> bool,
    options: PickOptions<'_>,
    piece: ValidPieceIndex,
) -> bool {
    peer_has_piece(piece) && !(options.on_parole && chunks.received_chunks(piece) > 0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::core::constants::CHUNK_SIZE;
    use crate::core::lengths::Lengths;
    use crate::piece_priority::{FilePriority, PiecePriority};
    use crate::{bitv::BitV as BitVTrait, type_aliases::BF};
    use rand::SeedableRng;
    use std::cell::Cell;
    use std::collections::BTreeSet;
    use std::net::{IpAddr, Ipv4Addr, SocketAddr};

    const PIECE_LENGTH: u32 = CHUNK_SIZE * 4;

    fn peer(id: u8) -> PeerHandle {
        SocketAddr::new(IpAddr::V4(Ipv4Addr::new(127, 0, 0, id)), 6881)
    }

    /// A ChunkTracker for one file of `num_pieces` four-chunk pieces, all
    /// selected, none had.
    fn make_test_chunk_tracker(num_pieces: u32) -> ChunkTracker {
        let total_length = PIECE_LENGTH as u64 * num_pieces as u64;
        let lengths = Lengths::new(total_length, PIECE_LENGTH).unwrap();
        let bf_len = lengths.piece_bitfield_bytes();
        let have = BF::from_boxed_slice(vec![0u8; bf_len].into_boxed_slice());
        let file_infos = vec![crate::file_info::FileInfo {
            relative_filename: "test.dat".into(),
            offset_in_torrent: 0,
            len: total_length,
            piece_range: 0..num_pieces,
            attrs: Default::default(),
        }];
        let selected =
            crate::chunk_tracker::compute_selected_bytes(&lengths, |_| true, &file_infos);
        ChunkTracker::new(have.into_dyn(), selected, lengths, &file_infos).unwrap()
    }

    /// A tracker of `num_pieces` pieces, none had, whose random picks follow
    /// `seed`.
    fn seeded(num_pieces: u32, seed: u64) -> PieceTracker {
        PieceTracker::with_rng(
            make_test_chunk_tracker(num_pieces),
            StdRng::seed_from_u64(seed),
        )
    }

    fn tracker(num_pieces: u32) -> PieceTracker {
        seeded(num_pieces, 0)
    }

    /// A tracker of `wanted` pieces we lack, followed by the
    /// INITIAL_PICKER_THRESHOLD pieces we have: it picks rarest first.
    fn rarest_first_tracker(wanted: u32, seed: u64) -> PieceTracker {
        let mut tracker = seeded(
            wanted + u32::try_from(INITIAL_PICKER_THRESHOLD).unwrap(),
            seed,
        );
        for p in wanted..wanted + u32::try_from(INITIAL_PICKER_THRESHOLD).unwrap() {
            let p = index(&tracker, p);
            tracker.reserve_piece(p, peer(99));
            tracker.take_inflight(p);
            tracker.mark_piece_hash_ok(p);
        }
        tracker
    }

    fn index(tracker: &PieceTracker, i: u32) -> ValidPieceIndex {
        tracker
            .chunks()
            .get_lengths()
            .validate_piece_index(i)
            .unwrap()
    }

    /// Chunk `chunk` of piece `index` arrives.
    fn deliver(tracker: &mut PieceTracker, index: u32, chunk: u32) {
        deliver_from(tracker, peer(1), index, chunk);
    }

    /// Chunk `chunk` of piece `index` arrives from `sender`.
    fn deliver_from(tracker: &mut PieceTracker, sender: PeerHandle, index: u32, chunk: u32) {
        let block = vec![0u8; CHUNK_SIZE as usize];
        tracker
            .mark_chunk_downloaded(&Piece::from_data(index, chunk * CHUNK_SIZE, &block), sender)
            .unwrap();
    }

    /// Who the record of piece `i` names, block by block.
    fn record(tracker: &PieceTracker, i: u32) -> Option<Vec<Option<PeerHandle>>> {
        tracker
            .senders
            .get(&index(tracker, i))
            .map(|senders| senders.blocks().to_vec())
    }

    type NoStream = std::iter::Empty<ValidPieceIndex>;
    type Ask<'a, P, A> = AcquireRequest<'a, NoStream, P, A, fn(ValidPieceIndex) -> bool>;

    /// A request from `who`, which has the pieces `has` names, when the
    /// pieces have the availability `availability` gives, any count of
    /// which may have moved since the last ask. Many peers are connected,
    /// the peer has no request outstanding, and it has no average piece
    /// time, so it steals nothing.
    fn ask<'a, P, A>(who: PeerHandle, has: P, availability: A) -> Ask<'a, P, A>
    where
        P: Fn(ValidPieceIndex) -> bool,
        A: Fn(ValidPieceIndex) -> u32,
    {
        AcquireRequest {
            peer: who,
            peer_avg_time: None,
            priority_pieces: std::iter::empty(),
            peer_has_piece: has,
            availability,
            moved: Moved {
                pieces: Vec::new(),
                all: true,
            },
            num_peers: 100,
            outstanding_requests: 0,
            options: PickOptions::default(),
            can_steal: |_| true,
        }
    }

    fn all(_: ValidPieceIndex) -> bool {
        true
    }

    fn one(_: ValidPieceIndex) -> u32 {
        1
    }

    /// Availability from a table, one count per piece; no peer has a piece
    /// past its end (the had pieces of [`rarest_first_tracker`]).
    fn table(counts: &[u32]) -> impl Fn(ValidPieceIndex) -> u32 + '_ {
        |p| counts.get(p.get_usize()).copied().unwrap_or(0)
    }

    fn reserved(result: AcquireResult) -> u32 {
        match result {
            AcquireResult::Reserved(p) => p.get(),
            other => panic!("expected Reserved, got {other:?}"),
        }
    }

    // Every piece one peer would be handed in turn, until none is left for
    // it, a lower index being rarer.
    fn drain_for_peer(
        tracker: &mut PieceTracker,
        peer_has_piece: impl Fn(ValidPieceIndex) -> bool,
    ) -> Vec<u32> {
        let mut order = Vec::new();
        loop {
            match tracker.acquire_piece(ask(peer(1), &peer_has_piece, |p| p.get() + 1)) {
                AcquireResult::Reserved(p) => order.push(p.get()),
                AcquireResult::NoneAvailable => return order,
                other => panic!("unexpected {other:?}"),
            }
        }
    }

    fn pri(tracker: &mut PieceTracker, set: &[(u32, PiecePriority)]) {
        let lengths = *tracker.chunks().get_lengths();
        tracker.set_piece_priorities(
            set.iter()
                .map(|(i, p)| (lengths.validate_piece_index(*i).unwrap(), *p)),
        );
    }

    #[test]
    fn a_piece_has_a_sole_sender_only_when_one_known_peer_sent_every_block() {
        let (a, b) = (peer(1), peer(2));
        let sole: Senders = [a, a, a, a].map(Some).into_iter().collect();
        assert!(sole.sole_sender());
        assert_eq!(sole.participants(), BTreeSet::from([a]));

        let shared: Senders = [a, b, a, a].map(Some).into_iter().collect();
        assert!(!shared.sole_sender());
        assert_eq!(shared.participants(), BTreeSet::from([a, b]));

        // A block of unknown sender may have come from another peer.
        let partly_unknown: Senders = [None, Some(a), Some(a), Some(a)].into_iter().collect();
        assert!(!partly_unknown.sole_sender());
        assert_eq!(partly_unknown.participants(), BTreeSet::from([a]));
    }

    #[test]
    fn a_peer_that_goes_away_mid_piece_leaves_a_record_only_of_the_blocks_the_piece_keeps() {
        let mut tracker = tracker(1);
        let (gone, next) = (peer(1), peer(2));
        let piece = reserved(tracker.acquire_piece(ask(gone, all, one)));
        let piece = index(&tracker, piece);

        // The peer sends block 0 and goes away with blocks 1 to 3 asked for:
        // the piece goes back with block 0, and the record names only it.
        deliver_from(&mut tracker, gone, 0, 0);
        assert_eq!(tracker.release_pieces_owned_by(gone), 1);
        assert_eq!(
            record(&tracker, 0),
            Some(vec![Some(gone), None, None, None])
        );

        // Another peer finishes it: the hash check is handed both senders,
        // and the record leaves with the piece.
        assert_eq!(reserved(tracker.acquire_piece(ask(next, all, one))), 0);
        for chunk in 1..4 {
            deliver_from(&mut tracker, next, 0, chunk);
        }
        let (_, senders) = tracker.take_inflight(piece);
        assert_eq!(
            senders.blocks(),
            [Some(gone), Some(next), Some(next), Some(next)]
        );
        assert!(tracker.senders.is_empty());

        // The piece fails and starts over with no record.
        tracker.mark_piece_hash_failed(piece);
        assert_eq!(record(&tracker, 0), None);
    }

    #[test]
    fn a_skipped_piece_keeps_its_blocks_and_their_record() {
        let mut tracker = tracker(2);
        deliver_from(&mut tracker, peer(1), 1, 0);

        // Skipped, the piece keeps its block, and the record its sender.
        pri(&mut tracker, &[(1, PiecePriority::Skip)]);
        assert_eq!(
            record(&tracker, 1),
            Some(vec![Some(peer(1)), None, None, None])
        );

        // Wanted again, it asks only for the blocks it lacks (libtorrent
        // `piece_picker::set_piece_priority` filters a piece, it does not
        // forget it).
        pri(&mut tracker, &[(1, PiecePriority::Normal)]);
        assert_eq!(tracker.missing_chunks(index(&tracker, 1)).count(), 3);
        assert_eq!(
            record(&tracker, 1),
            Some(vec![Some(peer(1)), None, None, None])
        );
    }

    #[test]
    fn test_new_piece_tracker() {
        let tracker = tracker(10);
        assert_eq!(tracker.inflight_count(), 0);
    }

    #[test]
    fn test_reserve_piece_from_queue() {
        let mut tracker = tracker(5);
        let piece = reserved(tracker.acquire_piece(ask(peer(1), all, one)));
        assert!(tracker.is_inflight(index(&tracker, piece)));
        assert_eq!(tracker.inflight_count(), 1);
    }

    #[test]
    fn test_reserve_filters_by_peer_has_piece() {
        let mut tracker = tracker(5);
        let piece = reserved(tracker.acquire_piece(ask(peer(1), |p| p.get() >= 2, one)));
        assert!(piece >= 2, "Should have gotten a piece >= 2");
    }

    #[test]
    fn test_a_refused_piece_stays_queued_and_nothing_takes_its_place() {
        let mut tracker = rarest_first_tracker(5, 0);
        let counts = [3, 3, 1, 3, 3];

        let mut asked = Vec::new();
        let result = tracker.acquire_admitted_piece(ask(peer(1), all, table(&counts)), |piece| {
            asked.push(piece.get());
            false
        });
        assert!(matches!(result, AcquireResult::NoRoom), "{result:?}");
        assert_eq!(asked, vec![2]);
        assert_eq!(tracker.inflight_count(), 0);

        // With room again, the same piece is the one handed out.
        let result = tracker.acquire_admitted_piece(ask(peer(1), all, table(&counts)), |_| true);
        assert_eq!(reserved(result), 2);
    }

    #[test]
    fn test_complete_piece() {
        let mut tracker = tracker(5);
        let piece = reserved(tracker.acquire_piece(ask(peer(1), all, one)));
        let piece = index(&tracker, piece);

        // Complete the piece (take_inflight + hash check + mark_piece_hash_ok)
        assert!(tracker.take_inflight(piece).0.is_some());
        assert!(!tracker.is_inflight(piece));
        tracker.mark_piece_hash_ok(piece);
        assert!(tracker.chunks().is_piece_have(piece));
    }

    #[test]
    fn test_fail_piece_requeues() {
        let mut tracker = tracker(5);
        let piece = reserved(tracker.acquire_piece(ask(peer(1), all, one)));
        let piece = index(&tracker, piece);
        assert!(tracker.is_inflight(piece));

        // Fail the piece (take_inflight + hash check fails + mark_piece_hash_failed)
        assert!(tracker.take_inflight(piece).0.is_some());
        tracker.mark_piece_hash_failed(piece);
        assert!(!tracker.is_inflight(piece));
        assert!(!tracker.chunks().is_piece_have(piece));

        // Back in the queue: a peer with only that piece is handed it.
        let again = tracker.acquire_piece(ask(peer(2), |p| p == piece, one));
        assert_eq!(reserved(again), piece.get());
    }

    #[test]
    fn test_release_pieces_owned_by_peer() {
        let mut tracker = tracker(5);
        let (peer_a, peer_b) = (peer(1), peer(2));

        let a1 = reserved(tracker.acquire_piece(ask(peer_a, all, one)));
        let a2 = reserved(tracker.acquire_piece(ask(peer_a, all, one)));
        let b = reserved(tracker.acquire_piece(ask(peer_b, all, one)));
        let [a1, a2, b] = [a1, a2, b].map(|p| index(&tracker, p));
        assert_eq!(tracker.inflight_count(), 3);

        // Peer A dies
        assert_eq!(tracker.release_pieces_owned_by(peer_a), 2);
        assert_eq!(tracker.inflight_count(), 1);
        assert!(tracker.is_inflight(b));
        assert!(!tracker.is_inflight(a1));
        assert!(!tracker.is_inflight(a2));
    }

    #[test]
    fn test_into_chunks_requeues_inflight() {
        let mut tracker = tracker(5);
        let a = reserved(tracker.acquire_piece(ask(peer(1), all, one)));
        let b = reserved(tracker.acquire_piece(ask(peer(1), all, one)));
        assert_eq!(tracker.inflight_count(), 2);

        // Convert back to chunks (simulates pause): both are queued again.
        let mut tracker = PieceTracker::new(tracker.into_chunks());
        let only_those = |p: ValidPieceIndex| p.get() == a || p.get() == b;
        let mut again = BTreeSet::new();
        for _ in 0..2 {
            again.insert(reserved(tracker.acquire_piece(ask(
                peer(2),
                only_those,
                one,
            ))));
        }
        assert_eq!(again, BTreeSet::from([a, b]));
    }

    #[test]
    fn test_priority_pieces_checked_first() {
        let mut tracker = rarest_first_tracker(5, 0);
        let stream = vec![index(&tracker, 3), index(&tracker, 2)];
        let result = tracker.acquire_piece(AcquireRequest {
            peer: peer(1),
            peer_avg_time: None,
            priority_pieces: stream.into_iter(),
            peer_has_piece: all,
            availability: |p| 10 - p.get(),
            moved: Moved::default(),
            num_peers: 100,
            outstanding_requests: 0,
            options: PickOptions::default(),
            can_steal: |_| true,
        });
        // The stream's next piece, though piece 4 is rarer.
        assert_eq!(reserved(result), 3);
    }

    #[test]
    fn test_none_available_when_no_pieces() {
        let mut tracker = tracker(5);
        let result = tracker.acquire_piece(ask(peer(1), |_| false, one));
        assert!(matches!(result, AcquireResult::NoneAvailable), "{result:?}");
    }

    #[test]
    fn a_late_block_completes_a_released_piece_which_leaves_the_queue_for_its_check() {
        // libtorrent `piece_picker::mark_as_writing` takes a block of a piece
        // no one is downloading, unless we have the piece.
        let mut tracker = tracker(1);
        let slow = peer(1);
        let piece = index(&tracker, 0);
        assert_eq!(reserved(tracker.acquire_piece(ask(slow, all, one))), 0);
        for chunk in 0..3 {
            deliver_from(&mut tracker, slow, 0, chunk);
        }
        assert_eq!(
            tracker.release_piece_blocked_by(slow, |c| c.chunk_index == 3),
            Some(piece)
        );
        assert!(tracker.chunks().iter_queued_pieces().any(|p| p == piece));

        // The last block arrives anyway: it is written, with no one to
        // cancel, and the piece goes to its check, out of the queue.
        let last = chunk_of(&tracker, 0, 3);
        assert_eq!(
            tracker.block_arrived(&last, slow),
            Arrival::Write { cancel: vec![] }
        );
        deliver_from(&mut tracker, slow, 0, 3);
        let (in_flight, senders) = tracker.take_inflight(piece);
        assert_eq!(in_flight, None);
        assert_eq!(senders.blocks(), [Some(slow); 4]);
        assert_eq!(tracker.chunks().iter_queued_pieces().count(), 0);
        let result = tracker.acquire_piece(ask(peer(2), all, one));
        assert!(matches!(result, AcquireResult::NoneAvailable), "{result:?}");

        // A copy of a block of a piece we have is not written.
        tracker.mark_piece_hash_ok(piece);
        assert_eq!(tracker.block_arrived(&last, slow), Arrival::Discard);
    }

    #[test]
    fn a_snub_releases_one_piece_with_no_free_block() {
        // libtorrent `peer_connection::snub_peer`: a request times out only
        // when its piece has no free block left for another peer, and one
        // a snub.
        let mut tracker = tracker(3);
        let (slow, other) = (peer(1), peer(2));
        let mut owned = Vec::new();
        for _ in 0..3 {
            owned.push(reserved(tracker.acquire_piece(ask(slow, all, one))));
        }
        let (whole, part, doubled_rest) = (owned[0], owned[1], owned[2]);
        // Every missing block of `whole` is asked of the slow peer; two of
        // `part`'s are; `doubled_rest` has its first three blocks asked of
        // it and the fourth doubled by another peer.
        let asked = move |c: &ChunkInfo| {
            let p = c.piece_index.get();
            p == whole
                || (p == part && c.chunk_index < 2)
                || (p == doubled_rest && c.chunk_index < 3)
        };
        tracker
            .inflight
            .get_mut(&index(&tracker, doubled_rest))
            .unwrap()
            .duplicates
            .push((3, other));

        let mut released: Vec<u32> = (0..2)
            .map(|_| tracker.release_piece_blocked_by(slow, asked).unwrap().get())
            .collect();
        assert_eq!(tracker.release_piece_blocked_by(slow, asked), None);
        assert!(!tracker.is_inflight(index(&tracker, whole)));
        assert!(!tracker.is_inflight(index(&tracker, doubled_rest)));
        assert!(tracker.is_owned_by(index(&tracker, part), slow));
        let mut queued: Vec<u32> = tracker
            .chunks()
            .iter_queued_pieces()
            .map(|p| p.get())
            .collect();
        queued.sort_unstable();
        released.sort_unstable();
        assert_eq!(queued, released);
        let mut expected = vec![whole, doubled_rest];
        expected.sort_unstable();
        assert_eq!(released, expected);
    }

    #[tokio::test(start_paused = true)]
    async fn test_steal_only_pieces_peer_has() {
        // try_steal must check the stealing peer has the piece: a peer that
        // "steals" a piece it cannot download leaves it stuck in flight.
        let mut tracker = tracker(5);
        let (peer_a, peer_b) = (peer(1), peer(2));
        let a_has = |p: ValidPieceIndex| p.get() == 0 || p.get() == 4;
        let first = reserved(tracker.acquire_piece(ask(peer_a, a_has, one)));
        let second = reserved(tracker.acquire_piece(ask(peer_a, a_has, one)));
        assert_eq!(BTreeSet::from([first, second]), BTreeSet::from([0, 4]));

        // Peer B is twenty times as fast as A has been on both: 1 ms a
        // piece against 20 ms held. It has piece 4 only.
        tokio::time::advance(Duration::from_millis(20)).await;
        let request = || {
            let mut request = ask(peer_b, |p: ValidPieceIndex| p.get() == 4, one);
            request.peer_avg_time = Some(Duration::from_millis(1));
            request
        };

        // Pieces 1 to 3 are queued, unstarted: no end game, and no piece
        // changes hands, however slow its owner.
        let result = tracker.acquire_piece(request());
        assert!(matches!(result, AcquireResult::NoneAvailable), "{result:?}");

        // Another peer takes them; now every wanted piece is in flight.
        for _ in 1..=3 {
            reserved(tracker.acquire_piece(ask(peer(3), all, one)));
        }
        match tracker.acquire_piece(request()) {
            AcquireResult::Stolen { piece, from_peer } => {
                assert_eq!(piece.get(), 4);
                assert_eq!(from_peer, peer_a);
                assert_eq!(
                    tracker.get_inflight(index(&tracker, 0)).unwrap().peer,
                    peer_a
                );
            }
            other => panic!("Expected Stolen, got {other:?}"),
        }
    }

    #[test]
    fn the_rarest_piece_goes_first() {
        let mut tracker = rarest_first_tracker(6, 0);
        let counts = [4, 3, 5, 1, 2, 6];
        let order: Vec<u32> = (0..6)
            .map(|_| reserved(tracker.acquire_piece(ask(peer(1), all, table(&counts)))))
            .collect();
        assert_eq!(order, [3, 4, 1, 0, 2, 5]);
    }

    #[test]
    fn equally_rare_pieces_are_picked_at_random() {
        let counts = [2, 1, 2, 2, 1, 2, 1, 2];
        let picked: BTreeSet<u32> = (0..32)
            .map(|seed| {
                let mut tracker = rarest_first_tracker(8, seed);
                reserved(tracker.acquire_piece(ask(peer(1), all, table(&counts))))
            })
            .collect();
        assert_eq!(picked, BTreeSet::from([1, 4, 6]));
    }

    #[test]
    fn a_started_piece_goes_before_an_open_one_as_rare_but_not_a_rarer_one() {
        let mut tracker = rarest_first_tracker(6, 0);
        deliver(&mut tracker, 2, 0);
        let counts = [2, 2, 2, 2, 1, 2];
        let order: Vec<u32> = (0..2)
            .map(|_| reserved(tracker.acquire_piece(ask(peer(1), all, table(&counts)))))
            .collect();
        assert_eq!(order, [4, 2]);
    }

    #[test]
    fn many_started_pieces_are_finished_first_rarest_then_most_received() {
        let mut tracker = rarest_first_tracker(6, 0);
        // Three started pieces with one peer connected: more than 3/2 a peer.
        deliver(&mut tracker, 0, 0);
        deliver(&mut tracker, 3, 0);
        deliver(&mut tracker, 3, 1);
        deliver(&mut tracker, 5, 0);
        let counts = [2, 1, 2, 2, 1, 3];
        let order: Vec<u32> = (0..4)
            .map(|_| {
                let mut request = ask(peer(1), all, table(&counts));
                request.num_peers = 1;
                reserved(tracker.acquire_piece(request))
            })
            .collect();
        // Started: 3 before 0 (as rare, more received), then 5; then the
        // rarest open piece.
        assert_eq!(order, [3, 0, 5, 1]);
    }

    #[test]
    fn before_four_pieces_we_finish_started_ones_then_pick_at_random() {
        let counts = [5, 5, 1, 5, 5, 5, 5, 5];
        let mut picked = BTreeSet::new();
        for seed in 0..32 {
            let mut tracker = seeded(8, seed);
            deliver(&mut tracker, 6, 0);
            let first = reserved(tracker.acquire_piece(ask(peer(1), all, table(&counts))));
            assert_eq!(first, 6);
            picked.insert(reserved(tracker.acquire_piece(ask(
                peer(1),
                all,
                table(&counts),
            ))));
        }
        // Random, not the rarest: piece 2 is no likelier than the rest.
        assert!(picked.len() > 3, "{picked:?}");
        assert!(!picked.contains(&6));
    }

    #[test]
    fn a_snubbed_peer_takes_the_commonest_piece() {
        let mut tracker = rarest_first_tracker(5, 0);
        let counts = [4, 3, 9, 1, 2];
        let mut request = ask(peer(1), all, table(&counts));
        request.options.reverse = true;
        assert_eq!(reserved(tracker.acquire_piece(request)), 2);
    }

    #[test]
    fn a_higher_priority_level_goes_before_a_rarer_piece() {
        let mut tracker = rarest_first_tracker(4, 0);
        pri(&mut tracker, &[(2, PiecePriority::High)]);
        let counts = [1, 1, 9, 1];
        let request = ask(peer(1), all, table(&counts));
        assert_eq!(reserved(tracker.acquire_piece(request)), 2);
    }

    #[test]
    fn the_latest_suggestion_the_peer_may_take_goes_before_the_rarest() {
        let mut tracker = rarest_first_tracker(5, 0);
        let counts = [1, 9, 9, 9, 9];
        let suggested = [1, 2, 3].map(|i| index(&tracker, i));
        let suggesting = |has: fn(ValidPieceIndex) -> bool| {
            let mut request = ask(peer(1), has, table(&counts));
            request.options.suggested = &suggested;
            request
        };

        // Piece 0 is rarest. Piece 1, the latest suggestion, the peer lacks.
        let first = tracker.acquire_piece(suggesting(|p| p.get() != 1));
        assert_eq!(reserved(first), 2);
        // Without suggestions the rarest goes first.
        let second = tracker.acquire_piece(ask(peer(1), all, table(&counts)));
        assert_eq!(reserved(second), 0);
        // A higher priority level goes before a suggestion.
        pri(&mut tracker, &[(4, PiecePriority::High)]);
        assert_eq!(reserved(tracker.acquire_piece(suggesting(all))), 4);
        assert_eq!(reserved(tracker.acquire_piece(suggesting(all))), 1);
    }

    #[test]
    fn a_peer_on_parole_is_handed_only_unstarted_pieces_and_steals_nothing() {
        let mut tracker = rarest_first_tracker(3, 0);
        deliver(&mut tracker, 1, 0);
        let counts = [3, 1, 3];
        let parole = |has: fn(ValidPieceIndex) -> bool| {
            let mut request = ask(peer(1), has, table(&counts));
            request.options.on_parole = true;
            request
        };

        // Piece 1 is rarest, but started.
        let first = reserved(tracker.acquire_piece(parole(all)));
        assert_ne!(first, 1);
        // Only the started piece is left that this peer has: none for it.
        let result = tracker.acquire_piece(parole(|p| p.get() == 1));
        assert!(matches!(result, AcquireResult::NoneAvailable), "{result:?}");

        // Another peer holds the rest, and the queue is empty: a peer off
        // parole would take one over in the end game; one on parole does not.
        tracker.acquire_piece(ask(peer(2), all, table(&counts)));
        tracker.acquire_piece(ask(peer(2), all, table(&counts)));
        let mut request = parole(all);
        request.peer_avg_time = Some(Duration::ZERO);
        let result = tracker.acquire_piece(request);
        assert!(matches!(result, AcquireResult::NoneAvailable), "{result:?}");
        let mut request = ask(peer(3), all, table(&counts));
        request.peer_avg_time = Some(Duration::ZERO);
        assert!(matches!(
            tracker.acquire_piece(request),
            AcquireResult::Stolen { .. }
        ));
    }

    #[tokio::test(start_paused = true)]
    async fn the_end_game_takes_over_a_slow_piece_only_when_every_piece_is_started_and_the_peer_is_idle()
     {
        let (slow, fast) = (peer(1), peer(2));
        let mut tracker = tracker(3);
        let slow_has = |p: ValidPieceIndex| p.get() < 2;
        reserved(tracker.acquire_piece(ask(slow, slow_has, one)));
        reserved(tracker.acquire_piece(ask(slow, slow_has, one)));

        // The fast peer takes a piece in 1 s; the slow one has held both
        // for 5 s, past three times that.
        tokio::time::advance(Duration::from_secs(5)).await;
        let request = |outstanding_requests| {
            let mut request = ask(fast, slow_has, one);
            request.peer_avg_time = Some(Duration::from_secs(1));
            request.outstanding_requests = outstanding_requests;
            request
        };

        // Piece 2 is still queued, though the fast peer lacks it: no end game.
        let result = tracker.acquire_piece(request(0));
        assert!(matches!(result, AcquireResult::NoneAvailable), "{result:?}");

        // Piece 2 goes to someone; now every wanted piece is in flight.
        assert_eq!(reserved(tracker.acquire_piece(ask(peer(3), all, one))), 2);
        let result = tracker.acquire_piece(request(1));
        assert!(matches!(result, AcquireResult::NoneAvailable), "{result:?}");
        match tracker.acquire_piece(request(0)) {
            AcquireResult::Stolen { from_peer, .. } => assert_eq!(from_peer, slow),
            other => panic!("expected Stolen, got {other:?}"),
        }
    }

    #[test]
    fn test_higher_priority_levels_are_handed_out_first() {
        let mut tracker = rarest_first_tracker(6, 0);
        pri(
            &mut tracker,
            &[
                (0, PiecePriority::Low),
                (2, PiecePriority::High),
                (3, PiecePriority::Critical),
                (4, PiecePriority::High),
            ],
        );
        // Critical, then High, then Normal, then Low; the rarer first within
        // a level.
        assert_eq!(drain_for_peer(&mut tracker, all), vec![3, 2, 4, 1, 5, 0]);
    }

    #[test]
    fn test_a_higher_piece_the_peer_lacks_does_not_hold_up_lower_ones() {
        let mut tracker = rarest_first_tracker(4, 0);
        pri(&mut tracker, &[(2, PiecePriority::Critical)]);
        // This peer has everything but the critical piece: it gets the rest.
        assert_eq!(
            drain_for_peer(&mut tracker, |p| p.get() != 2),
            vec![0, 1, 3]
        );
        // The critical piece is still waiting for a peer that has it.
        assert_eq!(drain_for_peer(&mut tracker, all), vec![2]);
    }

    #[test]
    fn test_raising_a_queued_piece_reorders_the_queue() {
        let mut tracker = rarest_first_tracker(5, 0);
        pri(&mut tracker, &[(3, PiecePriority::High)]);
        assert_eq!(
            drain_for_peer(&mut tracker, |p| p.get() == 3 || p.get() == 1),
            vec![3, 1]
        );
        pri(
            &mut tracker,
            &[(3, PiecePriority::Normal), (2, PiecePriority::High)],
        );
        assert_eq!(drain_for_peer(&mut tracker, all), vec![2, 0, 4]);
    }

    #[test]
    fn test_skip_drops_a_piece_from_what_is_needed_and_back() {
        let mut tracker = tracker(4);
        let piece_len = PIECE_LENGTH as u64;
        let full = tracker.chunks().get_hns().needed_bytes;

        pri(&mut tracker, &[(1, PiecePriority::Skip)]);
        assert_eq!(tracker.chunks().get_hns().needed_bytes, full - piece_len);
        assert_eq!(tracker.chunks().get_hns().wanted_bytes, full - piece_len);
        let mut drained = drain_for_peer(&mut tracker, all);
        drained.sort();
        assert_eq!(drained, vec![0, 2, 3]);

        // Wanted again: it is queued again, at the priority it is given.
        pri(&mut tracker, &[(1, PiecePriority::Low)]);
        assert_eq!(tracker.chunks().get_hns().needed_bytes, full);
        assert_eq!(drain_for_peer(&mut tracker, all), vec![1]);
    }

    /// The one file of a [`make_test_chunk_tracker`] tracker.
    fn one_file(tracker: &PieceTracker) -> crate::type_aliases::FileInfos {
        let lengths = tracker.chunks().get_lengths();
        vec![crate::file_info::FileInfo {
            relative_filename: "test.dat".into(),
            offset_in_torrent: 0,
            len: lengths.total_length(),
            piece_range: 0..lengths.total_pieces(),
            attrs: Default::default(),
        }]
    }

    /// Put the one file at `level`, its head and tail at the top with
    /// `first_last`.
    fn file_level(tracker: &mut PieceTracker, level: FilePriority, first_last: bool) {
        let files = one_file(tracker);
        let lengths = *tracker.chunks().get_lengths();
        let pieces =
            crate::piece_priority::file_to_piece_priorities(&lengths, &files, &[level], first_last);
        tracker.prioritize(&files, &[level], &pieces);
    }

    #[test]
    fn an_ignored_file_keeps_the_blocks_its_pieces_had() {
        let mut tracker = tracker(3);
        let full = tracker.chunks().get_hns().needed_bytes;
        deliver_from(&mut tracker, peer(1), 1, 0);
        deliver_from(&mut tracker, peer(1), 1, 1);

        file_level(&mut tracker, FilePriority::Ignored, false);
        assert_eq!(tracker.chunks().get_hns().needed_bytes, 0);
        assert_eq!(drain_for_peer(&mut tracker, all), Vec::<u32>::new());
        assert_eq!(tracker.missing_chunks(index(&tracker, 1)).count(), 2);

        // Wanted again, the piece asks only for the two blocks it lacks.
        file_level(&mut tracker, FilePriority::Normal, false);
        assert_eq!(tracker.chunks().get_hns().needed_bytes, full);
        assert_eq!(tracker.missing_chunks(index(&tracker, 1)).count(), 2);
        let mut drained = drain_for_peer(&mut tracker, all);
        drained.sort();
        assert_eq!(drained, vec![0, 1, 2]);
        let chunks: Vec<u32> = tracker
            .chunks_to_request(index(&tracker, 1), peer(1))
            .map(|c| c.chunk_index)
            .collect();
        assert_eq!(chunks, vec![2, 3]);
    }

    #[test]
    fn a_piece_in_flight_is_handed_to_no_other_peer_when_its_file_is_wanted_again() {
        let mut tracker = tracker(2);
        let held = reserved(tracker.acquire_piece(ask(peer(1), all, one)));
        file_level(&mut tracker, FilePriority::Ignored, false);
        file_level(&mut tracker, FilePriority::Normal, false);
        assert!(tracker.is_owned_by(index(&tracker, held), peer(1)));
        // The other peer gets the other piece, and then no piece of its own
        // (the end game may double a block, which reserves nothing).
        let other = reserved(tracker.acquire_piece(ask(peer(2), all, one)));
        assert_ne!(other, held);
        assert!(!matches!(
            tracker.acquire_piece(ask(peer(3), all, one)),
            AcquireResult::Reserved(_)
        ));
    }

    // Every piece a peer that has them all would be handed in turn with
    // sequential on.
    fn drain_sequential(tracker: &mut PieceTracker) -> Vec<u32> {
        drain_sequential_with(tracker, 100, false)
    }

    // [`drain_sequential`] with `num_peers` connected, by a peer that is
    // snubbed when `reverse`.
    fn drain_sequential_with(
        tracker: &mut PieceTracker,
        num_peers: usize,
        reverse: bool,
    ) -> Vec<u32> {
        let mut order = Vec::new();
        loop {
            let mut req = ask(peer(1), all, |p: ValidPieceIndex| 100 - p.get());
            req.options.sequential = true;
            req.options.reverse = reverse;
            req.num_peers = num_peers;
            match tracker.acquire_piece(req) {
                AcquireResult::Reserved(p) => order.push(p.get()),
                AcquireResult::NoneAvailable => return order,
                other => panic!("unexpected {other:?}"),
            }
        }
    }

    #[test]
    fn sequential_hands_out_the_top_level_then_every_wanted_piece_by_index() {
        // Rarest first would go from the end; sequential goes from the
        // front, the Maximum piece first, the High piece in its place, the
        // skipped piece never (libtorrent `piece_picker::pick_pieces`).
        let mut tracker = rarest_first_tracker(7, 0);
        deliver(&mut tracker, 0, 0);
        pri(
            &mut tracker,
            &[
                (5, PiecePriority::Critical),
                (3, PiecePriority::High),
                (4, PiecePriority::Skip),
            ],
        );
        assert_eq!(drain_sequential(&mut tracker), vec![5, 0, 1, 2, 3, 6]);
    }

    #[test]
    fn sequential_takes_started_pieces_first_only_while_they_are_many() {
        // Two started pieces: few for a hundred peers, so the index order
        // stands; more than one and a half per peer for one, so they go
        // first (libtorrent `pick_pieces`, `prioritize_partials`).
        let started = |tracker: &mut PieceTracker| {
            deliver(tracker, 4, 0);
            deliver(tracker, 6, 0);
        };
        let mut tracker = rarest_first_tracker(7, 0);
        started(&mut tracker);
        assert_eq!(
            drain_sequential_with(&mut tracker, 100, false),
            vec![0, 1, 2, 3, 4, 5, 6]
        );
        let mut tracker = rarest_first_tracker(7, 0);
        started(&mut tracker);
        assert_eq!(
            drain_sequential_with(&mut tracker, 1, false),
            vec![4, 6, 0, 1, 2, 3, 5]
        );
    }

    #[test]
    fn a_snubbed_peer_takes_pieces_by_index_in_sequential_mode() {
        // Sequential turns rarest first off, and with it the snubbed peer's
        // commonest-first walk (libtorrent `peer_connection::picker_options`
        // sets `reverse` only alongside `rarest_first`).
        let mut tracker = rarest_first_tracker(7, 0);
        assert_eq!(
            drain_sequential_with(&mut tracker, 100, true),
            vec![0, 1, 2, 3, 4, 5, 6]
        );
    }

    #[test]
    fn the_first_and_last_pieces_of_a_file_go_first_and_off_restores_the_file_level() {
        // 300 pieces: 1% is three at each end.
        let mut tracker = tracker(300);
        let edges = [0, 1, 2, 297, 298, 299];
        file_level(&mut tracker, FilePriority::Normal, true);
        let level = |tracker: &PieceTracker, p: u32| tracker.chunks().priority(index(tracker, p));
        for p in 0..300 {
            let expected = if edges.contains(&p) {
                PiecePriority::Critical
            } else {
                PiecePriority::Normal
            };
            assert_eq!(level(&tracker, p), expected, "piece {p}");
        }
        // The head and tail go first.
        let order = drain_for_peer(&mut tracker, all);
        let first: BTreeSet<u32> = order[..6].iter().copied().collect();
        assert_eq!(first, BTreeSet::from(edges));

        file_level(&mut tracker, FilePriority::Normal, false);
        for p in 0..300 {
            assert_eq!(level(&tracker, p), PiecePriority::Normal, "piece {p}");
        }
    }

    #[test]
    fn a_stolen_half_received_piece_asks_only_for_its_missing_chunks() {
        let mut tracker = tracker(1);
        let (slow, fast) = (peer(1), peer(2));

        let piece = reserved(tracker.acquire_piece(ask(slow, all, one)));
        let piece = index(&tracker, piece);
        assert_eq!(
            tracker
                .missing_chunks(piece)
                .map(|c| c.chunk_index)
                .collect::<Vec<_>>(),
            vec![0, 1, 2, 3]
        );

        // The slow peer delivers chunks 0 and 2.
        deliver(&mut tracker, 0, 0);
        deliver(&mut tracker, 0, 2);

        // A peer with a zero average piece time steals at once.
        let mut request = ask(fast, all, one);
        request.peer_avg_time = Some(Duration::ZERO);
        match tracker.acquire_piece(request) {
            AcquireResult::Stolen {
                piece: stolen,
                from_peer,
            } => {
                assert_eq!(stolen, piece);
                assert_eq!(from_peer, slow);
            }
            other => panic!("expected Stolen, got {other:?}"),
        }

        // The thief asks only for what has not arrived; the victim no longer
        // owns the piece, so it requests none of it.
        assert_eq!(
            tracker
                .missing_chunks(piece)
                .map(|c| c.chunk_index)
                .collect::<Vec<_>>(),
            vec![1, 3]
        );
        assert!(tracker.is_owned_by(piece, fast));
        assert!(!tracker.is_owned_by(piece, slow));
    }

    /// The block the end game hands `who`, which has every piece, has no
    /// request outstanding and no average piece time.
    fn doubled(tracker: &mut PieceTracker, who: PeerHandle) -> Option<ChunkInfo> {
        match tracker.acquire_piece(ask(who, all, one)) {
            AcquireResult::EndGame { chunk, .. } => Some(chunk),
            AcquireResult::NoneAvailable => None,
            other => panic!("expected EndGame, got {other:?}"),
        }
    }

    fn chunk_of(tracker: &PieceTracker, piece: u32, chunk: u32) -> ChunkInfo {
        let piece = index(tracker, piece);
        tracker
            .chunks()
            .get_lengths()
            .iter_chunk_infos(piece)
            .nth(chunk as usize)
            .unwrap()
    }

    #[test]
    fn in_the_end_game_a_peer_with_no_piece_time_asks_for_a_block_asked_of_another() {
        let mut tracker = tracker(1);
        let (owner, idle) = (peer(1), peer(2));
        assert_eq!(reserved(tracker.acquire_piece(ask(owner, all, one))), 0);
        deliver_from(&mut tracker, owner, 0, 0);

        // Every wanted piece is in flight and the idle peer has no request
        // outstanding: it asks for one block the owner has not delivered,
        // and requests that block alone.
        let chunk = doubled(&mut tracker, idle).unwrap();
        assert_ne!(chunk.chunk_index, 0);
        let piece = index(&tracker, 0);
        assert!(tracker.may_request(piece, idle));
        assert_eq!(
            tracker.chunks_to_request(piece, idle).collect::<Vec<_>>(),
            [chunk]
        );
        assert_eq!(tracker.chunks_to_request(piece, owner).count(), 3);
    }

    #[test]
    fn each_block_is_asked_of_one_peer_besides_its_owner_at_most() {
        let mut tracker = tracker(1);
        assert_eq!(reserved(tracker.acquire_piece(ask(peer(1), all, one))), 0);
        deliver_from(&mut tracker, peer(1), 0, 0);

        let picked: BTreeSet<u32> = (2..5)
            .map(|p| doubled(&mut tracker, peer(p)).unwrap().chunk_index)
            .collect();
        assert_eq!(picked, BTreeSet::from([1, 2, 3]));
        assert_eq!(doubled(&mut tracker, peer(5)), None);
    }

    #[test]
    fn no_block_is_doubled_while_a_wanted_piece_is_unstarted_or_by_a_busy_or_paroled_peer() {
        let mut tracker = tracker(2);
        let (owner, idle) = (peer(1), peer(2));
        let has_0 = |p: ValidPieceIndex| p.get() == 0;
        assert_eq!(reserved(tracker.acquire_piece(ask(owner, has_0, one))), 0);

        // Piece 1 is wanted and no one has started it: no end game, though
        // the idle peer lacks it.
        let result = tracker.acquire_piece(ask(idle, has_0, one));
        assert!(matches!(result, AcquireResult::NoneAvailable), "{result:?}");

        // Once a block of it is received, every wanted piece is started.
        deliver_from(&mut tracker, peer(3), 1, 0);
        let mut busy = ask(idle, has_0, one);
        busy.outstanding_requests = 1;
        let result = tracker.acquire_piece(busy);
        assert!(matches!(result, AcquireResult::NoneAvailable), "{result:?}");
        let mut paroled = ask(idle, has_0, one);
        paroled.options.on_parole = true;
        let result = tracker.acquire_piece(paroled);
        assert!(matches!(result, AcquireResult::NoneAvailable), "{result:?}");
        assert!(matches!(
            tracker.acquire_piece(ask(idle, has_0, one)),
            AcquireResult::EndGame { .. }
        ));
    }

    #[test]
    fn the_first_copy_of_a_doubled_block_is_written_and_the_other_request_cancelled() {
        let mut tracker = tracker(1);
        let (owner, idle, stranger) = (peer(1), peer(2), peer(3));
        assert_eq!(reserved(tracker.acquire_piece(ask(owner, all, one))), 0);
        let first = doubled(&mut tracker, idle).unwrap();

        // The doubling peer's copy comes first: the owner's request goes,
        // and so does its copy, while being written and once written.
        assert_eq!(
            tracker.block_arrived(&first, idle),
            Arrival::Write {
                cancel: vec![owner]
            }
        );
        assert!(!tracker.may_request(index(&tracker, 0), idle));
        assert_eq!(tracker.block_arrived(&first, owner), Arrival::Discard);
        deliver_from(&mut tracker, idle, 0, first.chunk_index);
        assert_eq!(tracker.block_arrived(&first, owner), Arrival::Discard);

        // The owner's copy comes first: the doubling peer's request goes.
        let second = doubled(&mut tracker, idle).unwrap();
        assert_eq!(
            tracker.block_arrived(&second, owner),
            Arrival::Write { cancel: vec![idle] }
        );
        assert_eq!(tracker.block_arrived(&second, idle), Arrival::Discard);

        // A block asked of no one else is the owner's alone.
        let mut rest = (0..4)
            .map(|c| chunk_of(&tracker, 0, c))
            .filter(|c| *c != first && *c != second);
        let (third, fourth) = (rest.next().unwrap(), rest.next().unwrap());
        assert_eq!(
            tracker.block_arrived(&third, owner),
            Arrival::Write { cancel: vec![] }
        );

        // A copy from a peer it was not asked of is data all the same, when
        // it comes first: it is written, and both requests for it go
        // (libtorrent `peer_connection::incoming_piece` takes any block it
        // lacks).
        let doubled = doubled(&mut tracker, idle).unwrap();
        assert_eq!(doubled, fourth);
        let Arrival::Write { mut cancel } = tracker.block_arrived(&fourth, stranger) else {
            panic!("the stranger's copy was not written");
        };
        cancel.sort_unstable();
        assert_eq!(cancel, vec![owner, idle]);
        assert!(!tracker.may_request(index(&tracker, 0), idle));
        assert_eq!(tracker.block_arrived(&fourth, owner), Arrival::Discard);
    }

    #[test]
    fn a_peer_that_goes_away_leaves_the_block_it_doubled_to_be_doubled_again() {
        let mut tracker = tracker(1);
        let (owner, gone, next) = (peer(1), peer(2), peer(3));
        assert_eq!(reserved(tracker.acquire_piece(ask(owner, all, one))), 0);
        for chunk in 0..3 {
            deliver_from(&mut tracker, owner, 0, chunk);
        }
        let last = chunk_of(&tracker, 0, 3);
        assert_eq!(doubled(&mut tracker, gone), Some(last));
        assert_eq!(doubled(&mut tracker, next), None);

        assert_eq!(tracker.release_pieces_owned_by(gone), 0);
        assert!(!tracker.may_request(index(&tracker, 0), gone));
        assert_eq!(doubled(&mut tracker, next), Some(last));

        // Its copy, sent before it went, is written all the same, and the
        // other two requests for the block go.
        let Arrival::Write { mut cancel } = tracker.block_arrived(&last, gone) else {
            panic!("the departed peer's copy was not written");
        };
        cancel.sort_unstable();
        assert_eq!(cancel, vec![owner, next]);
    }

    /// The pick order catches up with the counts `counts` gives, `moved`
    /// naming those that moved since the last catch-up, and is checked
    /// whole: every piece where its key puts it, and every count read.
    fn catch_up(tracker: &mut PieceTracker, counts: &[Cell<u32>], moved: Moved) {
        tracker
            .chunks
            .catch_up(moved, |p| counts[p.get_usize()].get());
        tracker.chunks.check_index();
        for (id, count) in counts.iter().enumerate() {
            let piece = index(tracker, u32::try_from(id).unwrap());
            assert_eq!(tracker.chunks.peers_with(piece), count.get(), "piece {id}");
        }
    }

    fn moved(pieces: &[u32]) -> Moved {
        Moved {
            pieces: pieces.to_vec(),
            all: false,
        }
    }

    /// The pick order of `level`, its buckets as sets: pieces in one
    /// bucket are equals, in random order.
    fn order(tracker: &PieceTracker, level: PiecePriority) -> Vec<BTreeSet<u32>> {
        let chunks = &tracker.chunks;
        let mut buckets: Vec<(u32, bool, BTreeSet<u32>)> = Vec::new();
        for piece in chunks.pick_order(level) {
            let key = (
                chunks.peers_with(*piece),
                chunks.received_chunks(*piece) == 0,
            );
            match buckets.last_mut() {
                Some((peers, open, set)) if (*peers, *open) == key => {
                    set.insert(piece.get());
                }
                _ => buckets.push((key.0, key.1, BTreeSet::from([piece.get()]))),
            }
        }
        buckets.into_iter().map(|(_, _, set)| set).collect()
    }

    fn sets(buckets: &[&[u32]]) -> Vec<BTreeSet<u32>> {
        buckets
            .iter()
            .map(|b| b.iter().copied().collect())
            .collect()
    }

    #[test]
    fn the_pick_order_follows_every_move_of_a_piece_or_its_count() {
        use PiecePriority::{High, Normal, Skip};
        let mut tracker = rarest_first_tracker(8, 0);
        // Piece i of the eight wanted is had by i + 1 peers; no peer has the
        // four we have.
        let counts: Vec<Cell<u32>> = (0..12)
            .map(|i| Cell::new(if i < 8 { i + 1 } else { 0 }))
            .collect();
        let set = |piece: usize, count: u32| counts[piece].set(count);
        let all = Moved {
            pieces: Vec::new(),
            all: true,
        };
        catch_up(&mut tracker, &counts, all);
        assert_eq!(
            order(&tracker, Normal),
            sets(&[&[0], &[1], &[2], &[3], &[4], &[5], &[6], &[7]])
        );

        // A count falls: the piece rises to its equals.
        set(7, 1);
        catch_up(&mut tracker, &counts, moved(&[7]));
        assert_eq!(
            order(&tracker, Normal),
            sets(&[&[0, 7], &[1], &[2], &[3], &[4], &[5], &[6]])
        );
        // A count rises past every other: the piece sinks to the end.
        set(0, 10);
        catch_up(&mut tracker, &counts, moved(&[0]));
        assert_eq!(
            order(&tracker, Normal),
            sets(&[&[7], &[1], &[2], &[3], &[4], &[5], &[6], &[0]])
        );
        // The last peer with a piece goes: it leaves; a peer comes with it:
        // it joins its equals.
        set(3, 0);
        catch_up(&mut tracker, &counts, moved(&[3]));
        assert_eq!(
            order(&tracker, Normal),
            sets(&[&[7], &[1], &[2], &[4], &[5], &[6], &[0]])
        );
        set(3, 2);
        set(4, 3);
        catch_up(&mut tracker, &counts, moved(&[3, 4]));
        assert_eq!(
            order(&tracker, Normal),
            sets(&[&[7], &[1, 3], &[2, 4], &[5], &[6], &[0]])
        );
        // A logged piece whose count is back where it was, and a had piece's
        // count, move nothing.
        set(9, 3);
        catch_up(&mut tracker, &counts, moved(&[5, 9]));
        assert_eq!(
            order(&tracker, Normal),
            sets(&[&[7], &[1, 3], &[2, 4], &[5], &[6], &[0]])
        );

        // A first block: the piece goes before the open ones as rare.
        deliver(&mut tracker, 4, 0);
        catch_up(&mut tracker, &counts, moved(&[]));
        assert_eq!(
            order(&tracker, Normal),
            sets(&[&[7], &[1, 3], &[4], &[2], &[5], &[6], &[0]])
        );

        // A priority: the piece moves to its level's order, and back.
        pri(&mut tracker, &[(6, High)]);
        catch_up(&mut tracker, &counts, moved(&[]));
        assert_eq!(order(&tracker, High), sets(&[&[6]]));
        assert_eq!(
            order(&tracker, Normal),
            sets(&[&[7], &[1, 3], &[4], &[2], &[5], &[0]])
        );
        pri(&mut tracker, &[(6, Normal)]);
        catch_up(&mut tracker, &counts, moved(&[]));
        assert_eq!(order(&tracker, High), sets(&[]));
        assert_eq!(
            order(&tracker, Normal),
            sets(&[&[7], &[1, 3], &[4], &[2], &[5], &[6], &[0]])
        );

        // Reserved: the piece leaves; its peer goes: it is requeued.
        tracker.reserve_piece(index(&tracker, 7), peer(2));
        catch_up(&mut tracker, &counts, moved(&[]));
        assert_eq!(
            order(&tracker, Normal),
            sets(&[&[1, 3], &[4], &[2], &[5], &[6], &[0]])
        );
        tracker.release_pieces_owned_by(peer(2));
        catch_up(&mut tracker, &counts, moved(&[]));
        assert_eq!(
            order(&tracker, Normal),
            sets(&[&[7], &[1, 3], &[4], &[2], &[5], &[6], &[0]])
        );
        // A piece that fails its check is back, open again.
        tracker.reserve_piece(index(&tracker, 4), peer(2));
        tracker.take_inflight(index(&tracker, 4));
        tracker.mark_piece_hash_failed(index(&tracker, 4));
        catch_up(&mut tracker, &counts, moved(&[]));
        assert_eq!(
            order(&tracker, Normal),
            sets(&[&[7], &[1, 3], &[2, 4], &[5], &[6], &[0]])
        );
        // A piece had is gone.
        tracker.reserve_piece(index(&tracker, 2), peer(2));
        tracker.take_inflight(index(&tracker, 2));
        tracker.mark_piece_hash_ok(index(&tracker, 2));
        catch_up(&mut tracker, &counts, moved(&[]));
        assert_eq!(
            order(&tracker, Normal),
            sets(&[&[7], &[1, 3], &[4], &[5], &[6], &[0]])
        );

        // Skipped: the order is laid out anew without it, and with it again.
        pri(&mut tracker, &[(1, Skip)]);
        catch_up(&mut tracker, &counts, moved(&[]));
        assert_eq!(
            order(&tracker, Normal),
            sets(&[&[7], &[3], &[4], &[5], &[6], &[0]])
        );
        pri(&mut tracker, &[(1, Normal)]);
        catch_up(&mut tracker, &counts, moved(&[]));
        assert_eq!(
            order(&tracker, Normal),
            sets(&[&[7], &[1, 3], &[4], &[5], &[6], &[0]])
        );

        // Every count moved: every piece is read again.
        for count in &counts {
            count.set(1);
        }
        let all = Moved {
            pieces: Vec::new(),
            all: true,
        };
        catch_up(&mut tracker, &counts, all);
        assert_eq!(order(&tracker, Normal), sets(&[&[0, 1, 3, 4, 5, 6, 7]]));
    }

    #[test]
    fn the_pick_order_holds_through_any_run_of_moves() {
        use PiecePriority::{Critical, High, Low, Normal, Skip};
        const WANTED: u32 = 48;
        let mut tracker = rarest_first_tracker(WANTED, 1);
        let mut rng = StdRng::seed_from_u64(7);
        let counts: Vec<Cell<u32>> = (0..WANTED + u32::try_from(INITIAL_PICKER_THRESHOLD).unwrap())
            .map(|_| Cell::new(0))
            .collect();
        let all = Moved {
            pieces: Vec::new(),
            all: true,
        };
        catch_up(&mut tracker, &counts, all);
        for _ in 0..4000 {
            let piece = rng.random_range(0..WANTED);
            let who = peer(rng.random_range(1..4));
            let mut logged = Vec::new();
            match rng.random_range(0..16) {
                0..6 => {
                    counts[piece as usize].set(rng.random_range(0..8));
                    logged.push(piece);
                }
                6 if !tracker.chunks.is_piece_have(index(&tracker, piece)) => {
                    deliver(&mut tracker, piece, rng.random_range(0..3));
                }
                7..9 => {
                    let level = [Low, Normal, High, Critical][rng.random_range(0..4)];
                    pri(&mut tracker, &[(piece, level)]);
                }
                9 => pri(&mut tracker, &[(piece, Skip)]),
                10..12 => {
                    let queued: Vec<_> = tracker.chunks.iter_queued_pieces().collect();
                    if !queued.is_empty() {
                        let piece = queued[rng.random_range(0..queued.len())];
                        tracker.reserve_piece(piece, who);
                    }
                }
                12 => {
                    tracker.release_pieces_owned_by(who);
                }
                13..16 => {
                    let mut inflight: Vec<_> = tracker.inflight.keys().copied().collect();
                    inflight.sort_unstable_by_key(|p| p.get());
                    if !inflight.is_empty() {
                        let piece = inflight[rng.random_range(0..inflight.len())];
                        tracker.take_inflight(piece);
                        if rng.random_bool(0.5) {
                            tracker.mark_piece_hash_ok(piece);
                        } else {
                            tracker.mark_piece_hash_failed(piece);
                        }
                    }
                }
                _ => {}
            }
            catch_up(&mut tracker, &counts, moved(&logged));
        }
    }

    #[test]
    fn a_pick_walks_the_order_only_to_the_piece_it_hands_out() {
        let mut tracker = rarest_first_tracker(1000, 0);
        // The rarer the lower: the order is the pieces in index order.
        let (asked, read) = (Cell::new(0), Cell::new(0));
        let rarer_first = |p: ValidPieceIndex| {
            read.set(read.get() + 1);
            p.get() + 1
        };
        // The piece handed to a peer with the pieces `has` names, how many
        // pieces the pick asked whether the peer has, and how many counts it
        // read, after the counts of `logged` moved.
        let mut pick = |has: &dyn Fn(u32) -> bool, reverse: bool, logged: &[u32]| {
            asked.set(0);
            read.set(0);
            let mut req = ask(
                peer(1),
                |p: ValidPieceIndex| {
                    asked.set(asked.get() + 1);
                    has(p.get())
                },
                &rarer_first,
            );
            req.moved = moved(logged);
            req.options.reverse = reverse;
            let piece = reserved(tracker.acquire_piece(req));
            (piece, asked.get(), read.get())
        };

        // The first pick reads every count once, to lay the order out.
        assert_eq!(pick(&|_| true, false, &[]), (0, 1, 1004));
        // Then a peer with every piece: the first piece of the order.
        assert_eq!(pick(&|_| true, false, &[]), (1, 1, 0));
        // A peer with one piece: the walk passes the 698 pieces before it.
        assert_eq!(pick(&|p| p == 700, false, &[]), (700, 699, 0));
        // A snubbed peer walks from the end: the 99 pieces after its one.
        assert_eq!(pick(&|p| p == 900, true, &[]), (900, 100, 0));
        // Counts that moved are read, and only they.
        assert_eq!(pick(&|_| true, false, &[5, 6, 2]), (2, 1, 3));
    }
}
