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
    collections::{HashMap, HashSet},
    time::Duration,
};

use rand::{RngExt, rngs::StdRng};
use tokio::time::Instant;

use crate::buffers::ByteBuf;
use crate::core::lengths::{ChunkInfo, ValidPieceIndex};
use crate::peer_protocol::Piece;

use crate::{
    chunk_tracker::{ChunkMarkingResult, ChunkTracker},
    file_info::FileInfo,
    piece_priority::PiecePriority,
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
}

/// Result of attempting to acquire a piece.
#[derive(Debug)]
pub enum AcquireResult {
    /// A new piece was reserved from the queue.
    Reserved(ValidPieceIndex),
    /// A piece was stolen from a slower peer.
    Stolen {
        piece: ValidPieceIndex,
        from_peer: PeerHandle,
    },
    /// No pieces are available for this peer.
    NoneAvailable,
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

    fn with_rng(chunks: ChunkTracker, rng: StdRng) -> Self {
        Self {
            chunks,
            inflight: HashMap::new(),
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
    /// 1. Take over a piece from a peer that's 10x slower
    /// 2. Reserve a piece: a stream's next piece first, then the one
    ///    [`Self::pick`] chooses
    /// 3. In the end game, take over a piece from a peer that's 3x slower
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

    /// [`Self::acquire_piece`], with `admit` asked of the one piece step 2 would
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
        // A peer on parole takes no piece another peer is downloading.
        let may_steal = !req.options.on_parole;

        // 1. Try steal with 10x threshold (very slow peer). Weaver's own:
        // libtorrent lets any peer request the free blocks of a piece another
        // is downloading; with one owner a piece, a far faster peer takes the
        // piece over instead.
        if may_steal && let Some(result) = self.try_steal(&req, 10.0) {
            return result;
        }

        // 2. A stream's next pieces first, in the order it needs them, then
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

        // 3. The end game: every wanted piece is in flight, and the peer has
        // nothing left to download. libtorrent request_a_block with
        // strict_end_game_mode.
        let end_game =
            req.outstanding_requests == 0 && self.chunks.iter_queued_pieces().next().is_none();
        if may_steal
            && end_game
            && let Some(result) = self.try_steal(&req, 3.0)
        {
            return result;
        }

        AcquireResult::NoneAvailable
    }

    /// The queued piece to hand the peer `req` asks for, after libtorrent
    /// `piece_picker::pick_pieces`:
    ///
    /// - Only the highest priority level with a piece the peer may take.
    /// - Started pieces first while they are many: more than one and a half
    ///   per connected peer, or more than [`MAX_PARTIAL_CHUNKS`] chunks, or
    ///   while we have fewer than [`INITIAL_PICKER_THRESHOLD`] pieces. The
    ///   rarest goes first, then the one most received.
    /// - Then the peer's latest suggestion it may take.
    /// - Then, once we have [`INITIAL_PICKER_THRESHOLD`] pieces, the rarest
    ///   piece, a started one before an unstarted one as rare, and a random
    ///   one among those equal. A snubbed peer takes the commonest instead.
    /// - Before that, the first free piece from a random place.
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
        } = self;
        let chunks = &*chunks;
        let started = |p: ValidPieceIndex| chunks.received_chunks(p) > 0;
        let level = PiecePriority::WANTED_DESCENDING
            .into_iter()
            .filter(|level| chunks.any_at(*level))
            .find(|level| {
                chunks.iter_queued_pieces().any(|p| {
                    chunks.priority(p) == *level
                        && may_hand(chunks, &req.peer_has_piece, req.options, p)
                })
            })?;
        let candidates = || {
            chunks.iter_queued_pieces().filter(move |p| {
                chunks.priority(*p) == level
                    && may_hand(chunks, &req.peer_has_piece, req.options, *p)
            })
        };

        let rarest_first = chunks.have_count() >= INITIAL_PICKER_THRESHOLD;
        let partials = inflight.len() + chunks.iter_queued_pieces().filter(|p| started(*p)).count();
        let chunks_per_piece = chunks.get_lengths().default_chunks_per_piece() as usize;
        let prioritize_partials = !rarest_first
            || partials > req.num_peers * 3 / 2
            || partials * chunks_per_piece > MAX_PARTIAL_CHUNKS;

        if prioritize_partials {
            let mut started_pieces = candidates().filter(|p| started(*p));
            let partial = if rarest_first {
                started_pieces
                    .min_by_key(|p| ((req.availability)(*p), Reverse(chunks.received_chunks(*p))))
            } else {
                started_pieces.next()
            };
            if partial.is_some() {
                return partial;
            }
        }

        let suggested = req.options.suggested;
        if !suggested.is_empty() {
            let latest = candidates()
                .filter_map(|p| Some((suggested.iter().position(|s| *s == p)?, p)))
                .min_by_key(|(at, _)| *at);
            if let Some((_, piece)) = latest {
                return Some(piece);
            }
        }

        if !rarest_first {
            let total = chunks.get_lengths().total_pieces();
            let start = rng.random_range(0..total);
            return candidates().min_by_key(|p| (p.get() + total - start) % total);
        }

        // One pass, keeping a random one of the pieces with the best key:
        // the k-th equal replaces the kept one with chance 1/k.
        let key = |p: ValidPieceIndex| {
            let (availability, open) = ((req.availability)(p), !started(p));
            if req.options.reverse {
                (u32::MAX - availability, !open)
            } else {
                (availability, open)
            }
        };
        let mut best = None;
        let mut ties = 0u32;
        for piece in candidates() {
            let k = key(piece);
            match best {
                Some((best_key, _)) if k > best_key => continue,
                Some((best_key, _)) if k == best_key => {
                    ties += 1;
                    if rng.random_range(0..ties) != 0 {
                        continue;
                    }
                }
                _ => ties = 1,
            }
            best = Some((k, piece));
        }
        best.map(|(_, piece)| piece)
    }

    /// Reserve a piece: remove from queue, add to inflight.
    fn reserve_piece(&mut self, piece: ValidPieceIndex, peer: PeerHandle) -> AcquireResult {
        self.chunks.reserve_needed_piece(piece);
        self.inflight.insert(
            piece,
            InflightPiece {
                peer,
                started: Instant::now(),
            },
        );
        AcquireResult::Reserved(piece)
    }

    /// Try to steal a piece from a slower peer.
    fn try_steal<I, P, A, S>(
        &mut self,
        req: &AcquireRequest<'_, I, P, A, S>,
        threshold: f64,
    ) -> Option<AcquireResult>
    where
        I: Iterator<Item = ValidPieceIndex>,
        P: Fn(ValidPieceIndex) -> bool,
        A: Fn(ValidPieceIndex) -> u32,
        S: Fn(ValidPieceIndex) -> bool,
    {
        let my_avg = req.peer_avg_time?;
        let min_elapsed = Duration::from_secs_f64(my_avg.as_secs_f64() * threshold);

        // Find the slowest piece from another peer that exceeds threshold
        // and that the stealing peer actually has (can download)
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

        Some(AcquireResult::Stolen {
            piece,
            from_peer: old_peer,
        })
    }

    // === PIECE COMPLETION ===

    /// Remove piece from inflight tracking (e.g., after all chunks received).
    ///
    /// Returns download duration if piece was in-flight.
    /// Note: Does NOT mark the piece as downloaded - caller should do hash check
    /// and then call `mark_piece_hash_ok` or `mark_piece_hash_failed`.
    pub fn take_inflight(&mut self, piece: ValidPieceIndex) -> Option<Duration> {
        let inflight = self.inflight.remove(&piece)?;
        Some(inflight.started.elapsed())
    }

    /// Mark piece as downloaded after successful hash verification.
    pub fn mark_piece_hash_ok(&mut self, piece: ValidPieceIndex) {
        self.chunks.mark_piece_downloaded(piece);
    }

    /// Mark piece as failed after hash verification failure - requeues the piece.
    pub fn mark_piece_hash_failed(&mut self, piece: ValidPieceIndex) {
        self.chunks.mark_piece_broken_if_not_have(piece);
    }

    /// Release all pieces owned by a peer: it died, choked us, or let its
    /// requests time out.
    ///
    /// Moves all pieces owned by the peer from IN_FLIGHT back to QUEUED, with
    /// the chunks they already received, so the next owner asks only for the
    /// rest. Returns the number of pieces released.
    pub fn release_pieces_owned_by(&mut self, peer: PeerHandle) -> usize {
        // Collect pieces to release (can't modify while iterating)
        let pieces_to_release: Vec<_> = self
            .inflight
            .iter()
            .filter(|(_, info)| info.peer == peer)
            .map(|(p, _)| *p)
            .collect();

        let count = pieces_to_release.len();
        for piece in pieces_to_release {
            self.inflight.remove(&piece);
            self.chunks.requeue_piece_if_not_have(piece);
        }
        count
    }

    // === QUERIES ===

    /// Get the inflight info for a piece, if it's currently being downloaded.
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

    /// The chunks of `piece` its owner still has to request: those not yet
    /// received. A peer that steals a half-received piece requests only these.
    pub fn missing_chunks(&self, piece: ValidPieceIndex) -> impl Iterator<Item = ChunkInfo> + '_ {
        self.chunks.iter_missing_chunks(piece)
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

    /// Mark a chunk as downloaded. Returns the result indicating if the piece is complete.
    pub fn mark_chunk_downloaded(
        &mut self,
        piece: &Piece<ByteBuf<'_>>,
    ) -> Option<ChunkMarkingResult> {
        self.chunks.mark_chunk_downloaded(piece)
    }

    /// Update which files are selected for download.
    pub fn update_only_files(
        &mut self,
        file_infos: &FileInfos,
        new_only_files: &HashSet<usize>,
    ) -> anyhow::Result<crate::chunk_tracker::HaveNeededSelected> {
        self.chunks.update_only_files(file_infos, new_only_files)
    }

    /// Set the fetch priority of the given pieces.
    pub fn set_piece_priorities(
        &mut self,
        priorities: impl IntoIterator<Item = (ValidPieceIndex, crate::piece_priority::PiecePriority)>,
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
    use crate::piece_priority::PiecePriority;
    use crate::{bitv::BitV as BitVTrait, type_aliases::BF};
    use rand::SeedableRng;
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
        let mut selected = BF::from_boxed_slice(vec![0u8; bf_len].into_boxed_slice());
        selected[..num_pieces as usize].fill(true);
        let file_infos = vec![crate::file_info::FileInfo {
            relative_filename: "test.dat".into(),
            offset_in_torrent: 0,
            len: total_length,
            piece_range: 0..num_pieces,
            attrs: Default::default(),
        }];
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
        let mut tracker = seeded(wanted + INITIAL_PICKER_THRESHOLD as u32, seed);
        for p in wanted..wanted + INITIAL_PICKER_THRESHOLD as u32 {
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
        let block = vec![0u8; CHUNK_SIZE as usize];
        tracker
            .mark_chunk_downloaded(&Piece::from_data(index, chunk * CHUNK_SIZE, &block))
            .unwrap();
    }

    type NoStream = std::iter::Empty<ValidPieceIndex>;
    type Ask<'a, P, A> = AcquireRequest<'a, NoStream, P, A, fn(ValidPieceIndex) -> bool>;

    /// A request from `who`, which has the pieces `has` names, when the
    /// pieces have the availability `availability` gives. Many peers are
    /// connected, the peer has no request outstanding, and it has no average
    /// piece time, so it steals nothing.
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

    /// Availability from a table, one count per piece.
    fn table(counts: &[u32]) -> impl Fn(ValidPieceIndex) -> u32 + '_ {
        |p| counts[p.get_usize()]
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
        assert!(matches!(result, AcquireResult::NoneAvailable), "{result:?}");
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
        assert!(tracker.take_inflight(piece).is_some());
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
        assert!(tracker.take_inflight(piece).is_some());
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
    fn test_take_inflight_nonexistent_piece_returns_none() {
        let mut tracker = tracker(5);
        let piece = index(&tracker, 0);
        assert!(tracker.take_inflight(piece).is_none());
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

        // Peer B is ten times as fast as A has been on both: 1 ms a piece
        // against 20 ms held. It has piece 4 only.
        tokio::time::advance(Duration::from_millis(20)).await;
        let mut request = ask(peer_b, |p: ValidPieceIndex| p.get() == 4, one);
        request.peer_avg_time = Some(Duration::from_millis(1));
        match tracker.acquire_piece(request) {
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
    async fn the_end_game_takes_over_a_slow_piece_only_when_nothing_is_queued_and_the_peer_is_idle()
    {
        let (slow, fast) = (peer(1), peer(2));
        let mut tracker = tracker(3);
        let slow_has = |p: ValidPieceIndex| p.get() < 2;
        reserved(tracker.acquire_piece(ask(slow, slow_has, one)));
        reserved(tracker.acquire_piece(ask(slow, slow_has, one)));

        // The fast peer takes a piece in 1 s; the slow one has held both
        // for 5 s, past three times that and short of ten.
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
        assert_eq!(tracker.chunks().get_hns().selected_bytes, full - piece_len);
        let mut drained = drain_for_peer(&mut tracker, all);
        drained.sort();
        assert_eq!(drained, vec![0, 2, 3]);

        // Wanted again: it is queued again, at the priority it is given.
        pri(&mut tracker, &[(1, PiecePriority::Low)]);
        assert_eq!(tracker.chunks().get_hns().needed_bytes, full);
        assert_eq!(drain_for_peer(&mut tracker, all), vec![1]);
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
}
