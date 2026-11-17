// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver
//
// Derived from rqbit (https://github.com/ikatson/rqbit), Copyright 2021 Igor Katson,
// licensed under the Apache License 2.0 (see LICENSE-APACHE). Modified by the
// Weaver authors from 2026-10-20 onward.

use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU8, AtomicU64, Ordering},
    },
    time::Instant,
};

use crate::core::lengths::Lengths;
use anyhow::Context;

use super::utils::BinarySize;
use rand::{RngExt, seq::IteratorRandom};
use tracing::{info, trace, warn};

use crate::{
    api::TorrentIdOrHash,
    bitv::BitV,
    bitv_factory::BitVFactory,
    chunk_tracker::{ChunkTracker, compute_selected_bytes},
    file_ops::{CheckOutcome, CheckedSoFar, FileOps, Verdict, inconsistent_hashes},
    piece_priority::PiecePriority,
    storage::FileStamp,
    type_aliases::{BF, FileStorage},
};

const MAX_FASTRESUME_CHECKS: usize = 64;

use super::{
    ManagedTorrentShared, TorrentMetadata,
    live::peers::{PeerList, ResumePeers},
    paused::TorrentStatePaused,
};

// Where a torrent's check stands: none is under way; one began, and reads
// what was saved or vouched for; one is past that and waits its turn to
// read every piece back, with no progress yet; or one reads every piece
// back, its progress in `checked_bytes`.
const CHECK_IDLE: u8 = 0;
const CHECK_STARTED: u8 = 1;
const CHECK_WAITING: u8 = 2;
const CHECK_READING: u8 = 3;

/// What a torrent's check trusts of what was saved.
pub(crate) enum InitialCheck {
    /// Nothing: every piece is read back.
    Full,
    /// The checkpoint, when there is one and the files are as it recorded.
    From(Option<Checkpoint>),
}

pub struct TorrentStateInitializing {
    pub(crate) files: FileStorage,
    pub(crate) shared: Arc<ManagedTorrentShared>,
    pub(crate) metadata: Arc<TorrentMetadata>,
    pub(crate) only_files: Option<Vec<usize>>,
    pub(crate) checked_bytes: AtomicU64,
    pause_requested: AtomicBool,
    // Rung by a pause request, for a check that waits its turn.
    pause_wake: tokio::sync::Notify,
    // Where the last check that read every piece back was paused, and the
    // bytes it had read: the next check goes on from there. Kept for this
    // run only.
    paused_at: parking_lot::Mutex<Option<(CheckedSoFar, u64)>>,
    check: AtomicU8,
    // Every piece is read back and nothing saved is trusted: after an
    // error, and on a forced recheck.
    full_check: bool,
    // What the embedder recorded of the data: when set, and the files are
    // as it recorded them, it is taken as what the storage holds and no
    // piece is read back to check it.
    checkpoint: Option<Checkpoint>,
    // The fetch priority of every piece; None while all are Normal.
    piece_priorities: Option<Vec<PiecePriority>>,
    // The peer list the first run is founded on: the peers and bans it was
    // added with.
    peers: parking_lot::Mutex<PeerList>,
}

/// What a torrent had written of its data when it was last saved, for the
/// next run to start from instead of reading every piece back (libtorrent
/// `add_torrent_params::have_pieces` and `unfinished_pieces`, which
/// `torrent::write_resume_data` writes and `torrent::read_resume_data`
/// takes). [`crate::ManagedTorrent::checkpoint`] makes one.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Checkpoint {
    /// The pieces had and verified, as a BEP 3 bitfield: bit 0 (the high bit
    /// of the first byte) is piece 0. Its length is the piece count, or the
    /// piece count rounded up to whole bytes with the spare bits clear.
    pub have: BF,
    /// Every block written to the storage, one bit per block, piece `i`'s
    /// blocks from bit `i` times the blocks of a whole piece on; the blocks
    /// of a had piece may be set or not. Its length is the block count, or
    /// the block count rounded up to whole bytes with the spare bits clear.
    /// None records no block of a piece not had.
    pub blocks: Option<BF>,
    /// The stamp of every file of the torrent by its index, None for one the
    /// storage keeps no file for; a file past the end has none. The next run
    /// trusts the checkpoint only while each file's stamp is the same, and
    /// checks every piece otherwise. None records no files: the embedder
    /// vouches for them.
    pub files: Option<Vec<Option<FileStamp>>>,
}

impl Checkpoint {
    /// A checkpoint of the pieces in `have`, no block of any other written,
    /// the files vouched for.
    pub fn pieces(have: BF) -> Self {
        Self {
            have,
            blocks: None,
            files: None,
        }
    }
}

/// Bring a bitfield of `units` bits to whole bytes with the spare bits
/// clear. It accepts exactly `units` bits, or the byte-rounded length with
/// its spare bits clear, as a peer's BITFIELD message must be (BEP 3).
fn normalize_bits(bits: BF, units: usize, what: &str) -> anyhow::Result<BF> {
    let bytes = units.div_ceil(8);
    let len = bits.len();
    if len != units && len != bytes * 8 {
        anyhow::bail!(
            "the checkpoint's {what} bitfield has {len} bits, but the torrent has {units} {what}: expected {units} bits, or {bytes} bytes with the spare bits clear"
        );
    }
    if bits[units..].any() {
        anyhow::bail!(
            "the checkpoint's {what} bitfield marks {what} past the last one ({units} {what}): the spare bits must be clear"
        );
    }
    let mut normalized = BF::from_boxed_slice(vec![0u8; bytes].into_boxed_slice());
    normalized[..units].copy_from_bitslice(&bits[..units]);
    Ok(normalized)
}

/// Bring a caller-supplied checkpoint to the torrent's layouts: one bit per
/// piece and one per block, each rounded up to whole bytes, spare bits clear.
pub(crate) fn normalize_checkpoint(
    checkpoint: Checkpoint,
    lengths: &Lengths,
) -> anyhow::Result<Checkpoint> {
    Ok(Checkpoint {
        have: normalize_bits(checkpoint.have, lengths.total_pieces() as usize, "pieces")?,
        blocks: checkpoint
            .blocks
            .map(|blocks| normalize_bits(blocks, lengths.total_chunks() as usize, "blocks"))
            .transpose()?,
        files: checkpoint.files,
    })
}

/// Whether the spot check of a piece the resume data names says the files
/// are not what the resume data says: the piece does not read, or its
/// bytes do not hash to the torrent's. A piece that cannot be told, a v2
/// piece whose file's hashes are not held yet, leaves the resume data
/// trusted: those hashes are kept only once every file's are held, so a
/// v2 torrent stopped before that has none to check against.
fn says_files_differ(checked: &anyhow::Result<Verdict>) -> bool {
    matches!(
        checked,
        Ok(Verdict::Failed | Verdict::Inconsistent) | Err(_)
    )
}

impl TorrentStateInitializing {
    /// Whether the check reads every piece back and trusts nothing saved.
    pub(crate) fn is_full_check(&self) -> bool {
        self.full_check
    }

    pub(crate) fn new(
        shared: Arc<ManagedTorrentShared>,
        metadata: Arc<TorrentMetadata>,
        only_files: Option<Vec<usize>>,
        files: FileStorage,
        check: InitialCheck,
        piece_priorities: Option<Vec<PiecePriority>>,
        peers: PeerList,
    ) -> Self {
        let (full_check, checkpoint) = match check {
            InitialCheck::Full => (true, None),
            InitialCheck::From(checkpoint) => (false, checkpoint),
        };
        Self {
            shared,
            metadata,
            only_files,
            files,
            checked_bytes: AtomicU64::new(0),
            pause_requested: AtomicBool::new(false),
            pause_wake: tokio::sync::Notify::new(),
            paused_at: parking_lot::Mutex::new(None),
            check: AtomicU8::new(CHECK_IDLE),
            full_check,
            checkpoint,
            piece_priorities,
            peers: parking_lot::Mutex::new(peers),
        }
    }

    /// The peers and bans a restart is to begin with; see [`ResumePeers`].
    pub(crate) fn resume_peers(&self) -> ResumePeers {
        self.peers.lock().resume_peers()
    }

    /// Bans `ip` on the list the first run is founded on; see
    /// [`PeerList::ban`].
    pub(crate) fn ban_peer(&self, ip: std::net::IpAddr) {
        self.peers.lock().ban(ip);
    }

    // The peer list, out of the torrent being checked and into the state
    // that takes its place. Only that change of state takes it, under the
    // torrent's lock, so nothing reads the torrent between with no list.
    pub(crate) fn take_peers(&self) -> PeerList {
        std::mem::take(&mut *self.peers.lock())
    }

    pub fn get_checked_bytes(&self) -> u64 {
        self.checked_bytes
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    pub(crate) fn request_pause(&self) {
        self.pause_requested.store(true, Ordering::Relaxed);
        self.pause_wake.notify_waiters();
    }

    pub(crate) fn clear_pause_request(&self) {
        self.pause_requested.store(false, Ordering::Relaxed);
    }

    pub(crate) fn is_pause_requested(&self) -> bool {
        self.pause_requested.load(Ordering::Relaxed)
    }

    pub(crate) fn try_start_check(&self) -> bool {
        self.check
            .compare_exchange(
                CHECK_IDLE,
                CHECK_STARTED,
                Ordering::AcqRel,
                Ordering::Acquire,
            )
            .is_ok()
    }

    pub(crate) fn finish_check(&self) {
        self.check.store(CHECK_IDLE, Ordering::Release);
    }

    /// Whether a check is under way, in any of its steps.
    pub(crate) fn is_checking(&self) -> bool {
        self.check.load(Ordering::Acquire) != CHECK_IDLE
    }

    /// Whether the check reads every piece back now, or is past the saved
    /// state and waits its turn to (libtorrent's torrent waiting for an
    /// `active_checking` slot is in `checking_files` already); one that
    /// starts from a checkpoint or from saved pieces that hold up does not.
    pub(crate) fn is_reading_pieces(&self) -> bool {
        matches!(
            self.check.load(Ordering::Acquire),
            CHECK_WAITING | CHECK_READING
        )
    }

    async fn validate_fastresume(
        &self,
        bitv_factory: &dyn BitVFactory,
        have_pieces: Option<Box<dyn BitV>>,
    ) -> Option<Box<dyn BitV>> {
        let hp = have_pieces?;
        let actual = hp.as_bytes().len();
        let expected = self.metadata.lengths().piece_bitfield_bytes();
        if actual != expected {
            warn!(
                actual,
                expected,
                "the bitfield loaded isn't of correct length, ignoring it, will do full check"
            );
            return None;
        }

        let is_broken = self
            .shared
            .spawner
            .block_in_place_in_turn(&self.files.read_turns(), || {
                let fo = crate::file_ops::FileOps::new(
                    &self.metadata,
                    &self.files,
                    self.shared.options.disable_v1_hashes,
                );

                let mut to_validate = BF::from_boxed_slice(
                    vec![0u8; self.metadata.lengths().piece_bitfield_bytes()].into_boxed_slice(),
                );
                let mut queue = hp.as_slice().to_owned();

                // Validate at least one piece from each file, if we claim we have it.
                for fi in self.metadata.file_infos.iter() {
                    let prange = fi.piece_range_usize();
                    let offset = prange.start;
                    for piece_id in hp
                        .as_slice()
                        .get(fi.piece_range_usize())
                        .into_iter()
                        .flat_map(|s| s.iter_ones())
                        .map(|pid| pid + offset)
                        .take(1)
                    {
                        to_validate.set(piece_id, true);
                        queue.set(piece_id, false);
                    }
                }

                // For all the remaining pieces we claim we have, validate them with decreasing probability.
                let queue = queue
                    .iter_ones()
                    .sample(&mut rand::rng(), MAX_FASTRESUME_CHECKS);

                for (tmp_id, piece_id) in queue.into_iter().enumerate() {
                    let denom: u32 = (tmp_id + 1).min(50).try_into().unwrap();
                    if rand::rng().random_ratio(1, denom) {
                        to_validate.set(piece_id, true);
                    }
                }

                let to_validate_count = to_validate.count_ones();
                for (id, piece_id) in to_validate
                    .iter_ones()
                    .filter_map(|id| {
                        self.metadata
                            .lengths()
                            .validate_piece_index(id.try_into().ok()?)
                    })
                    .enumerate()
                {
                    if says_files_differ(&fo.check_piece_whole(piece_id)) {
                        return true;
                    }

                    #[allow(clippy::cast_possible_truncation)]
                    let progress = (self.metadata.lengths().total_length() as f64
                        / to_validate_count as f64
                        * (id + 1) as f64) as u64;
                    let progress = progress.min(self.metadata.lengths().total_length());
                    self.checked_bytes.store(progress, Ordering::Relaxed);
                }

                false
            })
            .await;

        if is_broken {
            warn!(
                id = ?self.shared.id,
                info_hash = ?self.shared.info_hash,
                "data corrupted, ignoring fastresume data"
            );
            if let Err(e) = bitv_factory.clear(self.shared.id.into()).await {
                warn!(id=?self.shared.id, info_hash = ?self.shared.info_hash, "error clearing bitfield: {e:#}");
            }
            self.checked_bytes.store(0, Ordering::Relaxed);
            return None;
        }

        Some(hp)
    }

    /// The pieces of a checkpoint every block of which was written but that
    /// are not had, their hash not checked before the checkpoint was made,
    /// have it checked now: one that passes is had, one that does not has
    /// its blocks dropped, to be fetched again (libtorrent
    /// `torrent::on_resume_data_checked`: `verify_piece` on each unfinished
    /// piece `is_piece_finished`, then `piece_passed` or `piece_failed`).
    /// The storage hears each verdict as a check of the files tells it:
    /// [`crate::storage::TorrentStorage::on_piece_checked`] for one that passed, and
    /// [`crate::storage::TorrentStorage::on_piece_hash_failed`] for one that did not, so what
    /// it holds is what the check found. Returns the blocks left.
    async fn verify_finished(&self, have: &mut BF, mut blocks: BF) -> anyhow::Result<BF> {
        let lengths = *self.metadata.lengths();
        let finished: Vec<_> = lengths
            .iter_piece_infos()
            .map(|piece| piece.piece_index)
            .filter(|piece| !have[piece.get_usize()] && blocks[lengths.chunk_range(*piece)].all())
            .collect();
        if finished.is_empty() {
            return Ok(blocks);
        }
        let verdicts = self
            .shared
            .spawner
            .block_in_place_in_turn(&self.files.read_turns(), || {
                let fo = FileOps::new(
                    &self.metadata,
                    &self.files,
                    self.shared.options.disable_v1_hashes,
                );
                // The storage hears each verdict here, on the check's
                // thread, as a check of every piece tells it.
                finished
                    .iter()
                    .map(|piece| {
                        let piece = *piece;
                        let passed = match fo.check_piece_whole(piece) {
                            Ok(Verdict::Passed) => true,
                            Ok(Verdict::Inconsistent) => return Err(inconsistent_hashes(piece)),
                            Ok(Verdict::Failed | Verdict::Unknown) | Err(_) => false,
                        };
                        if passed {
                            self.files
                                .on_piece_checked(piece)
                                .with_context(|| format!("the storage did not take piece {piece}"))?;
                        } else {
                            self.files.on_piece_hash_failed(piece).with_context(|| {
                                format!("the storage did not let go of piece {piece}")
                            })?;
                        }
                        Ok((piece, passed))
                    })
                    .collect::<anyhow::Result<Vec<_>>>()
            })
            .await?;
        for (piece, passed) in verdicts {
            if passed {
                have.set(piece.get_usize(), true);
            } else {
                blocks[lengths.chunk_range(piece)].fill(false);
            }
        }
        Ok(blocks)
    }

    /// Whether every file is as `checkpoint` recorded it (libtorrent
    /// `torrent::start_checking`, which has `verify_resume_data` vouch for
    /// the resume data before trusting it). A stamp that cannot be read is
    /// no stamp.
    fn files_match(&self, checkpoint: &Checkpoint) -> bool {
        let Some(recorded) = checkpoint.files.as_ref() else {
            return true;
        };
        (0..self.metadata.file_infos.len()).all(|file| {
            let now = self.files.stamp(file).ok().flatten();
            let then = recorded.get(file).copied().flatten();
            if now != then {
                info!(
                    torrent = ?self.shared.id,
                    file, ?now, ?then,
                    "a file changed since the checkpoint, checking every piece"
                );
            }
            now == then
        })
    }

    // The have set from fastresume data when it holds up, else from hashing
    // every piece; every piece when `full`.
    // `waiting` is called once the check is past the saved state and waits
    // its turn to hash.
    async fn resume_or_check(
        &self,
        bitv_factory: &dyn BitVFactory,
        id: TorrentIdOrHash,
        full: bool,
        waiting: &(dyn Fn() + Sync),
    ) -> anyhow::Result<Box<dyn BitV>> {
        let have_pieces = if full {
            if let Err(e) = bitv_factory.clear(id).await {
                warn!(id=?self.shared.id, info_hash = ?self.shared.info_hash, error=?e, "error clearing bitfield");
            }
            None
        } else {
            bitv_factory
                .load(id)
                .await
                .context("error loading have_pieces")?
        };

        if let Some(h) = self.validate_fastresume(bitv_factory, have_pieces).await {
            return Ok(h);
        }

        // Torrents hash their files one at a time by default, each waiting
        // its turn here (libtorrent `active_checking`), as checking its
        // files, with nothing hashed yet, or what a paused check had.
        let from = self.paused_at.lock().take();
        let read = from.as_ref().map_or(0, |(_, read)| *read);
        self.checked_bytes.store(read, Ordering::Relaxed);
        self.check.store(CHECK_WAITING, Ordering::Release);
        waiting();
        let session = self.shared.session.upgrade();
        // A check paused while it waits its turn gives the turn up at once,
        // with what a paused check before it had read: it has read nothing,
        // and one asked to pause does not wait out another torrent's check.
        // A pause taken back before the wait is woken (the torrent resumed)
        // is no pause: the wake finds the request gone, and the check goes
        // on waiting its turn.
        let _turn = match &session {
            Some(session) => {
                let turn = loop {
                    let paused = self.pause_wake.notified();
                    tokio::pin!(paused);
                    paused.as_mut().enable();
                    if self.is_pause_requested() {
                        break None;
                    }
                    tokio::select! {
                        biased;
                        _ = &mut paused => {}
                        turn = session.concurrent_initialize_semaphore.acquire() => break Some(
                            turn.context("bug: concurrent check semaphore was closed")?,
                        ),
                    }
                };
                let Some(turn) = turn else {
                    *self.paused_at.lock() = from;
                    anyhow::bail!("initial check paused");
                };
                Some(turn)
            }
            None => None,
        };
        info!("Doing initial checksum validation, this might take a while...");
        // The check runs in one read turn of the torrent's volume, taken
        // once, as its blocking permits were; on a spinning volume its
        // reader takes the one turn a piece at a time instead, so uploads
        // read between two pieces. Its hashers read nothing.
        let threads = (self.metadata.lengths().total_pieces() as usize)
            .min(self.shared.spawner.check_threads());
        let turns = self.files.read_turns();
        let check = || {
            let stats = self.shared.spawner.stats();
            let call = stats.call(crate::session_stats::disk::DiskOp::InitialCheck);
            let before = self.checked_bytes.load(Ordering::Relaxed);
            self.check.store(CHECK_READING, Ordering::Release);
            let outcome = FileOps::new(
                &self.metadata,
                &self.files,
                self.shared.options.disable_v1_hashes,
            )
            .initial_check_from(
                threads,
                &self.checked_bytes,
                &self.pause_requested,
                from.map(|(at, _)| at),
            );
            call.bytes(
                self.checked_bytes
                    .load(Ordering::Relaxed)
                    .saturating_sub(before),
            );
            outcome
        };
        let outcome = if turns.whole_pieces() {
            self.shared.spawner.block_in_place(check)
        } else {
            self.shared
                .spawner
                .block_in_place_in_turn(&turns, check)
                .await
        }?;
        let have_pieces = match outcome {
            CheckOutcome::Done(have) => have,
            CheckOutcome::Paused(at) => {
                let read = self.checked_bytes.load(Ordering::Relaxed);
                *self.paused_at.lock() = Some((at, read));
                anyhow::bail!("initial check paused");
            }
        };
        bitv_factory
            .store_initial_check(id, have_pieces)
            .await
            .context("error storing initial check bitfield")
    }

    /// Checks the torrent's data. `waiting` is called once the check is
    /// past the saved state and waits its turn to hash the files.
    pub async fn check(&self, waiting: &(dyn Fn() + Sync)) -> anyhow::Result<TorrentStatePaused> {
        let id: TorrentIdOrHash = self.shared.info_hash.into();
        let bitv_factory = self
            .shared
            .session
            .upgrade()
            .context("session is dead")?
            .bitv_factory
            .clone();

        let checkpoint = self
            .checkpoint
            .as_ref()
            .filter(|checkpoint| !self.full_check && self.files_match(checkpoint));
        // A checkpoint the files no longer match vouches for nothing.
        let full_check = self.full_check || self.checkpoint.is_some();
        let (have_pieces, blocks) = match checkpoint {
            Some(checkpoint) => {
                // The embedder vouches for this data, so none is read back
                // but the pieces it holds every block of. Storing the pieces
                // makes them the fastresume state too, replacing whatever was
                // there.
                info!(
                    torrent=?self.shared.id,
                    "starting from the supplied checkpoint, skipping the initial check"
                );
                let mut have = checkpoint.have.clone();
                let blocks = match checkpoint.blocks.clone() {
                    Some(blocks) => Some(self.verify_finished(&mut have, blocks).await?),
                    None => None,
                };
                let have = bitv_factory
                    .store_initial_check(id, have)
                    .await
                    .context("error storing the supplied have-bitfield")?;
                (have, blocks)
            }
            None => (
                self.resume_or_check(&*bitv_factory, id, full_check, waiting)
                    .await?,
                None,
            ),
        };

        let selected_bytes = compute_selected_bytes(
            self.metadata.lengths(),
            |idx| {
                self.only_files
                    .as_ref()
                    .map(|o| o.contains(&idx))
                    .unwrap_or(true)
            },
            &self.metadata.file_infos,
        );

        let mut chunk_tracker = ChunkTracker::new(
            have_pieces.into_dyn(),
            selected_bytes,
            *self.metadata.lengths(),
            &self.metadata.file_infos,
        )
        .context("error creating chunk tracker")?;
        if let Some(blocks) = blocks.as_deref() {
            chunk_tracker.restore_blocks(blocks);
        }
        if let Some(priorities) = self.piece_priorities.as_ref() {
            let lengths = self.metadata.lengths();
            chunk_tracker.set_piece_priorities(
                priorities
                    .iter()
                    .enumerate()
                    .filter(|(_, p)| **p != PiecePriority::Normal)
                    .filter_map(|(idx, p)| {
                        Some((lengths.validate_piece_index(idx.try_into().ok()?)?, *p))
                    }),
            );
        }

        let hns = chunk_tracker.get_hns();

        info!(
            torrent=?self.shared.id,
            "Initial check results: have {}, needed {}, total wanted {}",
            BinarySize(hns.have_bytes),
            BinarySize(hns.needed_bytes),
            BinarySize(hns.wanted_bytes)
        );

        // Ensure file lengths are correct, and reopen read-only.
        self.shared
            .spawner
            .block_in_place_with_semaphore(|| {
                for (idx, fi) in self.metadata.file_infos.iter().enumerate() {
                    if self
                        .only_files
                        .as_ref()
                        .map(|v| v.contains(&idx))
                        .unwrap_or(true)
                    {
                        let now = Instant::now();
                        if fi.attrs.padding {
                            continue;
                        }
                        if let Err(err) = self.files.ensure_file_length(idx, fi.len) {
                            warn!(
                                id=?self.shared.id, info_hash = ?self.shared.info_hash,
                                "Error setting length for file {:?} to {}: {:#?}",
                                fi.relative_filename, fi.len, err
                            );
                        } else {
                            trace!(
                                "Set length for file {:?} to {} in {:?}",
                                fi.relative_filename,
                                BinarySize(fi.len),
                                now.elapsed()
                            );
                        }
                    }
                }
                Ok::<_, anyhow::Error>(())
            })
            .await?;

        let paused = TorrentStatePaused {
            shared: self.shared.clone(),
            metadata: self.metadata.clone(),
            files: self.files.take()?,
            chunk_tracker,
            streams: Arc::new(Default::default()),
            // The list stays here until the state changes; see
            // [`Self::take_peers`].
            peers: PeerList::default(),
        };
        Ok(paused)
    }
}

#[cfg(test)]
mod tests {
    use crate::core::lengths::Lengths;

    use super::{Checkpoint, normalize_checkpoint};
    use crate::type_aliases::BF;

    fn normalize_initial_have(have: BF, lengths: &Lengths) -> anyhow::Result<BF> {
        normalize_checkpoint(Checkpoint::pieces(have), lengths).map(|c| c.have)
    }

    fn bf(bits: &[bool]) -> BF {
        bits.iter()
            .copied()
            .collect::<bitvec::vec::BitVec<u8, bitvec::order::Msb0>>()
            .into_boxed_bitslice()
    }

    #[test]
    fn test_normalize_checkpoint_have() {
        // 10 pieces: two bytes on the wire, six spare bits.
        let l = Lengths::new(10 * 1024, 1024).unwrap();
        let mut ten = [false; 10];
        ten[0] = true;
        ten[9] = true;

        // Exactly the piece count, padded to whole bytes.
        let n = normalize_initial_have(bf(&ten), &l).unwrap();
        assert_eq!(n.len(), 16);
        assert_eq!(n.as_raw_slice(), &[0b1000_0000, 0b0100_0000]);

        // Whole bytes with the spare bits clear, as a BITFIELD message carries it.
        let wire = BF::from_boxed_slice(vec![0b1000_0000, 0b0100_0000].into_boxed_slice());
        assert_eq!(
            normalize_initial_have(wire, &l).unwrap().as_raw_slice(),
            &[0b1000_0000, 0b0100_0000]
        );

        // A spare bit set names a piece that does not exist.
        let spare = BF::from_boxed_slice(vec![0, 0b0010_0000].into_boxed_slice());
        assert!(normalize_initial_have(spare, &l).is_err());

        // Any other length.
        for bits in [0usize, 9, 11, 15, 24] {
            assert!(
                normalize_initial_have(bf(&vec![false; bits]), &l).is_err(),
                "{bits} bits"
            );
        }
    }

    // The blocks are held to their layout as the pieces are to theirs.
    #[test]
    fn test_normalize_checkpoint_blocks() {
        // 10 pieces of two blocks, the last of one: 19 blocks, three bytes.
        let l = Lengths::new(9 * 2 * 16384 + 16384, 2 * 16384).unwrap();
        assert_eq!(l.total_chunks(), 19);
        let have = BF::from_boxed_slice(vec![0, 0].into_boxed_slice());
        let blocks = |bits: usize, set: &[usize]| {
            let mut b = bf(&vec![false; bits]);
            for i in set {
                b.set(*i, true);
            }
            b
        };
        let n = normalize_checkpoint(
            Checkpoint {
                have: have.clone(),
                blocks: Some(blocks(19, &[0, 18])),
                files: None,
            },
            &l,
        )
        .unwrap();
        assert_eq!(
            n.blocks.unwrap().as_raw_slice(),
            &[0b1000_0000, 0, 0b0010_0000]
        );
        // A spare bit set names a block that does not exist.
        assert!(
            normalize_checkpoint(
                Checkpoint {
                    have: have.clone(),
                    blocks: Some(blocks(24, &[19])),
                    files: None,
                },
                &l,
            )
            .is_err()
        );
        for bits in [0usize, 18, 20, 16, 32] {
            assert!(
                normalize_checkpoint(
                    Checkpoint {
                        have: have.clone(),
                        blocks: Some(blocks(bits, &[])),
                        files: None,
                    },
                    &l,
                )
                .is_err(),
                "{bits} bits"
            );
        }
    }

    mod files_against_the_checkpoint {
        use std::sync::Arc;

        use anyhow::Context;
        use tempfile::TempDir;

        use crate::{
            AddTorrent, AddTorrentOptions, BF, Checkpoint, ManagedTorrent, ManagedTorrentState,
            Session,
            api::TorrentIdOrHash,
            tests::{
                embedder_util::{TestTorrent, isolated_session_opts},
                test_util::setup_test_logging,
            },
        };

        const PIECE_LENGTH: u32 = 1024;
        const PIECES: usize = 8;

        // A session and a torrent whose one file lies in the torrent's own
        // directory, every piece of it written.
        async fn torrent(name: &str) -> anyhow::Result<(TestTorrent, TempDir, Arc<Session>)> {
            setup_test_logging();
            let t = TestTorrent::new(name, PIECES * PIECE_LENGTH as usize, PIECE_LENGTH).await?;
            let out = TempDir::with_prefix(name)?;
            let session =
                Session::new_with_opts(out.path().into(), isolated_session_opts(false)).await?;
            Ok((t, out, session))
        }

        // The torrent added on its files, paused unless `run`, from
        // `checkpoint`, once checked.
        async fn added(
            session: &Arc<Session>,
            t: &TestTorrent,
            checkpoint: Option<Checkpoint>,
            run: bool,
        ) -> anyhow::Result<Arc<ManagedTorrent>> {
            let handle = session
                .add_torrent(
                    AddTorrent::from_bytes(t.torrent.clone()),
                    Some(AddTorrentOptions {
                        output_folder: Some(t.dir.path().to_str().unwrap().to_owned()),
                        overwrite: true,
                        paused: !run,
                        checkpoint,
                        ..Default::default()
                    }),
                )
                .await?
                .into_handle()
                .context("expected a handle")?;
            handle.wait_until_initialized().await?;
            Ok(handle)
        }

        fn have(handle: &ManagedTorrent) -> Vec<usize> {
            handle
                .with_chunk_tracker(|ct| ct.get_have_pieces().as_slice().iter_ones().collect())
                .unwrap()
        }

        // The torrent checked once from its files, then its checkpoint, and
        // the torrent taken out of the session.
        async fn checkpointed(
            session: &Arc<Session>,
            t: &TestTorrent,
        ) -> anyhow::Result<Checkpoint> {
            let handle = added(session, t, None, false).await?;
            assert_eq!(have(&handle), (0..PIECES).collect::<Vec<_>>());
            let checkpoint = handle.checkpoint().context("no checkpoint")?;
            let files = checkpoint.files.as_ref().context("no file stamps")?;
            assert_eq!(files.len(), 1);
            assert_eq!(
                files[0].map(|stamp| stamp.len),
                Some((PIECES * PIECE_LENGTH as usize) as u64)
            );
            session
                .delete(TorrentIdOrHash::Id(handle.id()), false)
                .await?;
            Ok(checkpoint)
        }

        fn without_piece(mut have: BF, piece: usize) -> BF {
            have.set(piece, false);
            have
        }

        // libtorrent `torrent::start_checking`: resume data the files still
        // match is taken, and no piece is read back. Here the checkpoint
        // drops a piece the files hold: trusted, the piece stays not had.
        #[tokio::test(flavor = "multi_thread")]
        async fn files_the_checkpoint_matches_start_from_it_with_no_check() -> anyhow::Result<()> {
            let (t, _out, session) = torrent("files_match_the_checkpoint").await?;
            let checkpoint = checkpointed(&session, &t).await?;

            let trusted = Checkpoint {
                have: without_piece(checkpoint.have.clone(), 3),
                blocks: None,
                files: checkpoint.files.clone(),
            };
            let handle = added(&session, &t, Some(trusted), false).await?;
            assert_eq!(have(&handle), [0, 1, 2, 4, 5, 6, 7]);
            Ok(())
        }

        // libtorrent `active_checking` limits only torrents that hash their
        // files: one whose resume data holds up starts while every check
        // turn is taken.
        #[tokio::test(flavor = "multi_thread")]
        async fn a_vouched_torrent_starts_while_another_holds_the_check_turn() -> anyhow::Result<()>
        {
            let (t, _out, session) = torrent("vouched_beside_a_check").await?;
            let checkpoint = checkpointed(&session, &t).await?;
            let turns = session.concurrent_initialize_semaphore.clone();
            assert_eq!(turns.available_permits(), 1);
            let _held = turns.clone().acquire_many_owned(1).await?;

            let handle = added(&session, &t, Some(checkpoint), false).await?;
            assert_eq!(have(&handle), (0..PIECES).collect::<Vec<_>>());
            Ok(())
        }

        // libtorrent 1.x `verify_resume_data`: a file whose size is not the
        // one recorded voids the resume data, and every piece is checked.
        #[tokio::test(flavor = "multi_thread")]
        async fn a_truncated_file_has_every_piece_checked() -> anyhow::Result<()> {
            let (t, _out, session) = torrent("a_truncated_file").await?;
            let checkpoint = checkpointed(&session, &t).await?;

            std::fs::OpenOptions::new()
                .write(true)
                .open(t.dir.path().join("0.data"))?
                .set_len(3 * PIECE_LENGTH as u64 + 100)?;
            let handle = added(&session, &t, Some(checkpoint), false).await?;
            assert_eq!(have(&handle), [0, 1, 2]);
            Ok(())
        }

        // libtorrent `torrent::force_recheck`: every piece is forgotten and
        // checked again from the files, whatever was vouched for, and a
        // running torrent runs again.
        #[tokio::test(flavor = "multi_thread")]
        async fn a_forced_recheck_checks_every_piece_of_a_complete_torrent() -> anyhow::Result<()> {
            let (t, _out, session) = torrent("a_forced_recheck").await?;
            let all = BF::from_boxed_slice(vec![0xff; PIECES.div_ceil(8)].into_boxed_slice());
            let handle = added(&session, &t, Some(Checkpoint::pieces(all)), true).await?;
            assert_eq!(have(&handle), (0..PIECES).collect::<Vec<_>>());

            {
                use std::io::{Seek, SeekFrom, Write};
                let mut file = std::fs::OpenOptions::new()
                    .write(true)
                    .open(t.dir.path().join("0.data"))?;
                file.seek(SeekFrom::Start(5 * PIECE_LENGTH as u64))?;
                file.write_all(&[0x5a; 16])?;
            }
            session.force_recheck(&handle).await?;
            handle.wait_until_initialized().await?;
            assert!(handle.with_state(|s| matches!(s, ManagedTorrentState::Live(_))));
            assert_eq!(have(&handle), [0, 1, 2, 3, 4, 6, 7]);
            assert!(!handle.is_paused());
            Ok(())
        }
    }

    mod checking_in_the_stats {
        use std::path::Path;
        use std::sync::Arc;
        use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

        use anyhow::Context;
        use parking_lot::Mutex;
        use tempfile::TempDir;
        use tokio::sync::mpsc::UnboundedSender;

        use crate::{
            AddTorrent, AddTorrentOptions, Session, TorrentStatsState,
            core::lengths::ValidPieceIndex,
            storage::{BoxStorageFactory, StorageFactory, StorageFactoryExt, TorrentStorage},
            tests::embedder_util::{MemoryStorageFactory, TestTorrent, isolated_session_opts},
            torrent_state::{ManagedTorrentShared, TorrentMetadata},
        };

        /// A memory storage whose first read says so on `reading` and
        /// waits for a word on `gate`.
        #[derive(Clone)]
        struct GatedStorageFactory {
            inner: MemoryStorageFactory,
            reading: UnboundedSender<()>,
            gate: Arc<Mutex<Option<std::sync::mpsc::Receiver<()>>>>,
        }

        struct GatedStorage {
            inner: Box<dyn TorrentStorage>,
            reading: UnboundedSender<()>,
            gate: Arc<Mutex<Option<std::sync::mpsc::Receiver<()>>>>,
        }

        impl StorageFactory for GatedStorageFactory {
            type Storage = GatedStorage;

            fn create(
                &self,
                shared: &ManagedTorrentShared,
                metadata: &TorrentMetadata,
            ) -> anyhow::Result<GatedStorage> {
                Ok(GatedStorage {
                    inner: Box::new(self.inner.create(shared, metadata)?),
                    reading: self.reading.clone(),
                    gate: self.gate.clone(),
                })
            }

            fn clone_box(&self) -> BoxStorageFactory {
                self.clone().boxed()
            }
        }

        impl TorrentStorage for GatedStorage {
            fn init(
                &mut self,
                shared: &ManagedTorrentShared,
                metadata: &TorrentMetadata,
            ) -> anyhow::Result<()> {
                self.inner.init(shared, metadata)
            }

            fn pread_exact(
                &self,
                file_id: usize,
                offset: u64,
                buf: &mut [u8],
            ) -> anyhow::Result<()> {
                let gate = self.gate.lock().take();
                if let Some(gate) = gate {
                    let _ = self.reading.send(());
                    gate.recv()?;
                }
                self.inner.pread_exact(file_id, offset, buf)
            }

            fn pwrite_all(&self, file_id: usize, offset: u64, buf: &[u8]) -> anyhow::Result<()> {
                self.inner.pwrite_all(file_id, offset, buf)
            }

            fn remove_file(&self, file_id: usize, filename: &Path) -> anyhow::Result<()> {
                self.inner.remove_file(file_id, filename)
            }

            fn remove_directory_if_empty(&self, path: &Path) -> anyhow::Result<()> {
                self.inner.remove_directory_if_empty(path)
            }

            fn ensure_file_length(&self, file_id: usize, length: u64) -> anyhow::Result<()> {
                self.inner.ensure_file_length(file_id, length)
            }

            fn take(&self) -> anyhow::Result<Box<dyn TorrentStorage>> {
                Ok(Box::new(GatedStorage {
                    inner: self.inner.take()?,
                    reading: self.reading.clone(),
                    gate: self.gate.clone(),
                }))
            }

            fn on_piece_completed(&self, piece: ValidPieceIndex) -> anyhow::Result<()> {
                self.inner.on_piece_completed(piece)
            }
        }

        // A torrent initializing reads as checking once it is past its
        // saved state: while it waits its turn to hash, with nothing hashed
        // yet, as libtorrent's torrent waiting for an `active_checking` slot
        // is in `checking_files`, and until its check is done once it reads.
        #[tokio::test(flavor = "multi_thread")]
        async fn a_torrent_reads_as_checking_from_the_wait_for_its_turn() -> anyhow::Result<()> {
            let name = "reads_as_checking";
            let t = TestTorrent::new(name, 8 * 1024, 1024).await?;
            let out = TempDir::with_prefix(name)?;
            let session =
                Session::new_with_opts(out.path().into(), isolated_session_opts(false)).await?;
            let checks = session.concurrent_initialize_semaphore.clone();
            let held = checks
                .clone()
                .acquire_many_owned(u32::try_from(checks.available_permits()).unwrap())
                .await?;
            let (reading_tx, mut reading) = tokio::sync::mpsc::unbounded_channel();
            let (go, gate) = std::sync::mpsc::channel();
            let storage = GatedStorageFactory {
                inner: MemoryStorageFactory::default(),
                reading: reading_tx,
                gate: Arc::new(Mutex::new(Some(gate))),
            };
            let handle = session
                .add_torrent(
                    AddTorrent::from_bytes(t.torrent.clone()),
                    Some(AddTorrentOptions {
                        storage_factory: Some(storage.clone_box()),
                        paused: true,
                        ..Default::default()
                    }),
                )
                .await?
                .into_handle()
                .context("expected a handle")?;
            let checking = |state: &TorrentStatsState| {
                matches!(
                    state,
                    TorrentStatsState::Initializing { checking: true, .. }
                )
            };
            let stats = loop {
                let changed = handle.next_state_change();
                let stats = handle.stats();
                if checking(&stats.state) {
                    break stats;
                }
                changed.await;
            };
            assert_eq!(stats.progress_bytes, 0, "nothing is hashed while it waits");
            assert!(
                reading.try_recv().is_err(),
                "it read pieces before its turn"
            );

            drop(held);
            reading.recv().await.context("the check read nothing")?;
            assert!(matches!(
                handle.stats().state,
                TorrentStatsState::Initializing { checking: true, .. }
            ));
            go.send(())?;
            handle.wait_until_initialized().await?;
            assert!(matches!(handle.stats().state, TorrentStatsState::Paused));
            session.stop().await;
            Ok(())
        }

        /// A memory storage whose reads all wait while it is shut, the
        /// first of them saying so on `reading`.
        #[derive(Clone)]
        struct ShutStorageFactory {
            inner: MemoryStorageFactory,
            door: Arc<Door>,
        }

        struct Door {
            open: Mutex<bool>,
            opened: parking_lot::Condvar,
            reading: Mutex<Option<UnboundedSender<()>>>,
            // The bytes read through the door.
            read: AtomicU64,
            // Every read is refused as of files shut (`FilesShut`), until
            // every piece is forgotten.
            refusing: AtomicBool,
            // The storage cannot forget its pieces.
            forgets_fail: AtomicBool,
            // How many times every piece was forgotten.
            forgotten: AtomicU64,
        }

        impl Door {
            fn shut(reading: UnboundedSender<()>) -> Arc<Self> {
                Arc::new(Self {
                    open: Mutex::new(false),
                    opened: parking_lot::Condvar::new(),
                    reading: Mutex::new(Some(reading)),
                    read: AtomicU64::new(0),
                    refusing: AtomicBool::new(false),
                    forgets_fail: AtomicBool::new(false),
                    forgotten: AtomicU64::new(0),
                })
            }

            fn open(&self) {
                *self.open.lock() = true;
                self.opened.notify_all();
            }

            fn pass(&self) {
                if let Some(reading) = self.reading.lock().take() {
                    let _ = reading.send(());
                }
                let mut open = self.open.lock();
                while !*open {
                    self.opened.wait(&mut open);
                }
            }
        }

        struct ShutStorage {
            inner: Box<dyn TorrentStorage>,
            door: Arc<Door>,
        }

        impl StorageFactory for ShutStorageFactory {
            type Storage = ShutStorage;

            fn create(
                &self,
                shared: &ManagedTorrentShared,
                metadata: &TorrentMetadata,
            ) -> anyhow::Result<ShutStorage> {
                Ok(ShutStorage {
                    inner: Box::new(self.inner.create(shared, metadata)?),
                    door: self.door.clone(),
                })
            }

            fn clone_box(&self) -> BoxStorageFactory {
                self.clone().boxed()
            }
        }

        impl TorrentStorage for ShutStorage {
            fn init(
                &mut self,
                shared: &ManagedTorrentShared,
                metadata: &TorrentMetadata,
            ) -> anyhow::Result<()> {
                self.inner.init(shared, metadata)
            }

            fn pread_exact(
                &self,
                file_id: usize,
                offset: u64,
                buf: &mut [u8],
            ) -> anyhow::Result<()> {
                self.door.pass();
                if self.door.refusing.load(Ordering::Acquire) {
                    return Err(crate::storage::FilesShut::refused().into());
                }
                self.door
                    .read
                    .fetch_add(buf.len() as u64, Ordering::Relaxed);
                self.inner.pread_exact(file_id, offset, buf)
            }

            fn on_check_everything(&self) -> anyhow::Result<()> {
                anyhow::ensure!(
                    !self.door.forgets_fail.load(Ordering::Acquire),
                    "the pieces are not forgotten"
                );
                self.inner.on_check_everything()?;
                self.door.refusing.store(false, Ordering::Release);
                self.door.forgotten.fetch_add(1, Ordering::AcqRel);
                Ok(())
            }

            fn pwrite_all(&self, file_id: usize, offset: u64, buf: &[u8]) -> anyhow::Result<()> {
                self.inner.pwrite_all(file_id, offset, buf)
            }

            fn remove_file(&self, file_id: usize, filename: &Path) -> anyhow::Result<()> {
                self.inner.remove_file(file_id, filename)
            }

            fn remove_directory_if_empty(&self, path: &Path) -> anyhow::Result<()> {
                self.inner.remove_directory_if_empty(path)
            }

            fn ensure_file_length(&self, file_id: usize, length: u64) -> anyhow::Result<()> {
                self.inner.ensure_file_length(file_id, length)
            }

            fn take(&self) -> anyhow::Result<Box<dyn TorrentStorage>> {
                Ok(Box::new(ShutStorage {
                    inner: self.inner.take()?,
                    door: self.door.clone(),
                }))
            }

            fn on_piece_completed(&self, piece: ValidPieceIndex) -> anyhow::Result<()> {
                self.inner.on_piece_completed(piece)
            }
        }

        // More pieces than a check has threads, so a pause stops it short.
        const PIECES: usize = 512;

        // A pause that stops a check under way leaves the torrent paused,
        // checking nothing, not reading as a check that never ends; resumed,
        // it is checked whole and runs. `recheck` pauses a check of every
        // piece asked for while the torrent runs, else the first check of
        // a torrent added running.
        async fn a_check_stopped_by_a_pause_reads_as_paused(
            name: &str,
            recheck: bool,
        ) -> anyhow::Result<()> {
            let t = TestTorrent::new(name, PIECES * 1024, 1024).await?;
            let out = TempDir::with_prefix(name)?;
            let session =
                Session::new_with_opts(out.path().into(), isolated_session_opts(false)).await?;
            let (reading_tx, mut reading) = tokio::sync::mpsc::unbounded_channel();
            let door = Door::shut(reading_tx);
            if recheck {
                door.open();
            }
            let storage = ShutStorageFactory {
                inner: MemoryStorageFactory::holding(vec![t.content.clone()]),
                door: door.clone(),
            };
            let handle = session
                .add_torrent(
                    AddTorrent::from_bytes(t.torrent.clone()),
                    Some(AddTorrentOptions {
                        storage_factory: Some(storage.clone_box()),
                        ..Default::default()
                    }),
                )
                .await?
                .into_handle()
                .context("expected a handle")?;
            if recheck {
                handle.wait_until_completed().await?;
                let (reading_tx, again) = tokio::sync::mpsc::unbounded_channel();
                reading = again;
                *door.reading.lock() = Some(reading_tx);
                *door.open.lock() = false;
                door.read.store(0, Ordering::Relaxed);
                session.force_recheck(&handle).await?;
            }
            reading.recv().await.context("the check read nothing")?;
            session.pause(&handle).await?;
            door.open();

            // The check stops at the next piece of each of its threads.
            loop {
                let changed = handle.next_state_change();
                let checking = handle.with_state(|state| {
                    matches!(state, crate::ManagedTorrentState::Initializing(init) if init.is_checking())
                });
                if !checking {
                    break;
                }
                changed.await;
            }
            assert!(
                handle.with_state(|state| matches!(
                    state,
                    crate::ManagedTorrentState::Initializing(_)
                )),
                "the check ran to its end"
            );
            assert!(handle.is_paused());
            let stats = handle.stats();
            assert!(
                matches!(stats.state, TorrentStatsState::Paused),
                "{:?}",
                stats.state
            );
            // The pause keeps the check's place: every piece before it was
            // read once, and the progress counts them.
            let (next, progress) = handle
                .with_state(|state| match state {
                    crate::ManagedTorrentState::Initializing(init) => init
                        .paused_at
                        .lock()
                        .as_ref()
                        .map(|(at, read)| (at.next, *read)),
                    _ => None,
                })
                .context("the paused check kept no place")?;
            assert!(0 < next && next < PIECES, "{next}");
            assert_eq!(progress, next as u64 * 1024);
            assert_eq!(door.read.load(Ordering::Relaxed), progress);

            // Resumed, it goes on from there: no piece is read twice.
            session.unpause(&handle).await?;
            handle.wait_until_completed().await?;
            assert_eq!(door.read.load(Ordering::Relaxed), PIECES as u64 * 1024);
            session.stop().await;
            Ok(())
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn a_first_check_stopped_by_a_pause_reads_as_paused() -> anyhow::Result<()> {
            a_check_stopped_by_a_pause_reads_as_paused("first_check_paused", false).await
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn a_recheck_stopped_by_a_pause_reads_as_paused() -> anyhow::Result<()> {
            a_check_stopped_by_a_pause_reads_as_paused("recheck_paused", true).await
        }

        // Files that change under a torrent while a check of them is under
        // way are checked from the first piece again: nothing the stopped
        // check read from the files as they were stays had. `paused` leaves
        // the check paused part way, with pieces it passed, before the
        // files change; else the check is reading when it is told.
        async fn a_check_of_files_that_changed_starts_over(
            name: &str,
            paused: bool,
        ) -> anyhow::Result<()> {
            let t = TestTorrent::new(name, PIECES * 1024, 1024).await?;
            let out = TempDir::with_prefix(name)?;
            let session =
                Session::new_with_opts(out.path().into(), isolated_session_opts(false)).await?;
            let (reading_tx, mut reading) = tokio::sync::mpsc::unbounded_channel();
            let door = Door::shut(reading_tx);
            let storage = ShutStorageFactory {
                inner: MemoryStorageFactory::holding(vec![t.content.clone()]),
                door: door.clone(),
            };
            let handle = session
                .add_torrent(
                    AddTorrent::from_bytes(t.torrent.clone()),
                    Some(AddTorrentOptions {
                        storage_factory: Some(storage.clone_box()),
                        ..Default::default()
                    }),
                )
                .await?
                .into_handle()
                .context("expected a handle")?;
            reading.recv().await.context("the check read nothing")?;
            let checking = |handle: &crate::ManagedTorrent| {
                handle.with_state(|state| {
                    matches!(state, crate::ManagedTorrentState::Initializing(init) if init.is_checking())
                })
            };
            if paused {
                session.pause(&handle).await?;
                door.open();
                loop {
                    let changed = handle.next_state_change();
                    if !checking(&handle) {
                        break;
                    }
                    changed.await;
                }
                let passed = handle
                    .with_state(|state| match state {
                        crate::ManagedTorrentState::Initializing(init) => {
                            init.paused_at.lock().as_ref().map(|(at, _)| at.next)
                        }
                        _ => None,
                    })
                    .context("the paused check kept no place")?;
                assert!(0 < passed && passed < PIECES, "{passed}");
                session.forget_for_recheck(&handle).await?;
            } else {
                // The check is told while its reads wait at the door; it
                // stops at its next piece once they are let through.
                let forgetting = {
                    let session = session.clone();
                    let handle = handle.clone();
                    tokio::spawn(async move { session.forget_for_recheck(&handle).await })
                };
                while !handle.with_state(|state| {
                    matches!(state, crate::ManagedTorrentState::Initializing(init) if init.is_pause_requested())
                }) {
                    tokio::task::yield_now().await;
                }
                door.open();
                forgetting.await??;
            }
            assert!(!checking(&handle), "the old check still runs");
            assert!(
                handle.with_state(|state| match state {
                    crate::ManagedTorrentState::Initializing(init) =>
                        init.is_full_check() && init.paused_at.lock().is_none(),
                    _ => false,
                }),
                "the torrent kept the old check's place"
            );
            assert_eq!(handle.is_paused(), paused, "forgetting changed the pause");

            // Another file's bytes are under the torrent now.
            storage.inner.log.overwrite_file(0, 0xa5);
            door.read.store(0, Ordering::Relaxed);
            session.restart_check(&handle).await?;
            handle.wait_until_initialized().await?;
            assert_eq!(
                door.read.load(Ordering::Relaxed),
                PIECES as u64 * 1024,
                "the check did not read every piece again"
            );
            let have =
                handle.with_chunk_tracker(|ct| ct.get_have_pieces().as_slice().count_ones())?;
            assert_eq!(have, 0, "a piece of the old files stayed had");
            assert_eq!(handle.is_paused(), paused);
            session.stop().await;
            Ok(())
        }

        // A session that stops while a torrent waits for its check to stop
        // drops the check with no word: the wait ends with an error, and
        // does not hang on a change of state that never comes. The check's
        // reads are still held at the door when it does.
        #[tokio::test(flavor = "multi_thread")]
        async fn a_wait_for_a_check_to_stop_ends_when_the_session_stops() -> anyhow::Result<()> {
            let name = "stopped_under_a_forget";
            let t = TestTorrent::new(name, PIECES * 1024, 1024).await?;
            let out = TempDir::with_prefix(name)?;
            let session =
                Session::new_with_opts(out.path().into(), isolated_session_opts(false)).await?;
            let (reading_tx, mut reading) = tokio::sync::mpsc::unbounded_channel();
            let door = Door::shut(reading_tx);
            let storage = ShutStorageFactory {
                inner: MemoryStorageFactory::holding(vec![t.content.clone()]),
                door: door.clone(),
            };
            let handle = session
                .add_torrent(
                    AddTorrent::from_bytes(t.torrent.clone()),
                    Some(AddTorrentOptions {
                        storage_factory: Some(storage.clone_box()),
                        ..Default::default()
                    }),
                )
                .await?
                .into_handle()
                .context("expected a handle")?;
            reading.recv().await.context("the check read nothing")?;
            let forgetting = {
                let session = session.clone();
                let handle = handle.clone();
                tokio::spawn(async move { session.forget_for_recheck(&handle).await })
            };
            while !handle.with_state(|state| {
                matches!(state, crate::ManagedTorrentState::Initializing(init) if init.is_pause_requested())
            }) {
                tokio::task::yield_now().await;
            }
            session.cancellation_token().cancel();
            let ended = forgetting.await?;
            assert!(
                ended.is_err(),
                "the pieces were forgotten under a stopped session"
            );
            door.open();
            session.stop().await;
            Ok(())
        }

        // A pause taken back before the check that waits its turn has woken
        // to it (the torrent paused and resumed at once) is no pause: the
        // check goes on waiting, takes its turn when one is free, and the
        // torrent runs. It does not end in an error.
        #[tokio::test]
        async fn a_pause_taken_back_while_a_check_waits_its_turn_is_no_pause() -> anyhow::Result<()>
        {
            let name = "pause_taken_back";
            let t = TestTorrent::new(name, 8 * 1024, 1024).await?;
            let out = TempDir::with_prefix(name)?;
            let session =
                Session::new_with_opts(out.path().into(), isolated_session_opts(false)).await?;
            let checks = session.concurrent_initialize_semaphore.clone();
            let held = checks
                .clone()
                .acquire_many_owned(u32::try_from(checks.available_permits()).unwrap())
                .await?;
            let handle = session
                .add_torrent(
                    AddTorrent::from_bytes(t.torrent.clone()),
                    Some(AddTorrentOptions {
                        storage_factory: Some(
                            MemoryStorageFactory::holding(vec![t.content.clone()]).clone_box(),
                        ),
                        ..Default::default()
                    }),
                )
                .await?
                .into_handle()
                .context("expected a handle")?;
            loop {
                let changed = handle.next_state_change();
                if matches!(
                    handle.stats().state,
                    TorrentStatsState::Initializing { checking: true, .. }
                ) {
                    break;
                }
                changed.await;
            }
            // Asked and taken back with the check's task not run between.
            handle.with_state(|state| match state {
                crate::ManagedTorrentState::Initializing(init) => {
                    init.request_pause();
                    init.clear_pause_request();
                }
                _ => panic!("the torrent is not being checked"),
            });
            drop(held);
            handle.wait_until_initialized().await?;
            assert!(handle.live().is_some(), "the torrent does not run");
            session.stop().await;
            Ok(())
        }

        // A check told to stop once it is past its last place to stop (it
        // reads the one piece its checkpoint left to verify, and hashes no
        // other) ends as it would have. The torrent it leaves does not run
        // for the moment before its pieces are forgotten: its tracker hears
        // no `started` followed by a `stopped`, only the `started` of the
        // run after the check that the forgetting was for.
        #[tokio::test(flavor = "multi_thread")]
        async fn a_check_told_to_stop_past_its_last_stop_says_nothing_to_a_tracker()
        -> anyhow::Result<()> {
            use crate::tests::e2e_pause::{Tracker, announce, answered, next};
            let name = "stopped_past_the_stop";
            let t = TestTorrent::new(name, 8 * 1024, 1024).await?;
            let out = TempDir::with_prefix(name)?;
            let (tracker, mut heard) = Tracker::start(None);
            let session = Session::new_with_opts(
                out.path().into(),
                crate::tests::e2e_tracker_announces::tracker_session_opts()?,
            )
            .await?;
            let (reading_tx, mut reading) = tokio::sync::mpsc::unbounded_channel();
            let door = Door::shut(reading_tx);
            let storage = ShutStorageFactory {
                inner: MemoryStorageFactory::holding(vec![t.content.clone()]),
                door: door.clone(),
            };
            // Every piece had but the fourth, every block of which is
            // written: the check reads that one back, and no other.
            let mut have = crate::BF::from_boxed_slice(vec![0xff].into_boxed_slice());
            have.set(3, false);
            let checkpoint = crate::Checkpoint {
                have,
                blocks: Some(crate::BF::from_boxed_slice(vec![0xff].into_boxed_slice())),
                files: None,
            };
            let handle = session
                .add_torrent(
                    AddTorrent::from_bytes(t.torrent.clone()),
                    Some(AddTorrentOptions {
                        storage_factory: Some(storage.clone_box()),
                        trackers: Some(vec![tracker.url()]),
                        checkpoint: Some(checkpoint),
                        ..Default::default()
                    }),
                )
                .await?
                .into_handle()
                .context("expected a handle")?;
            reading.recv().await.context("the check read nothing")?;
            let forgetting = {
                let session = session.clone();
                let handle = handle.clone();
                tokio::spawn(async move { session.forget_for_recheck(&handle).await })
            };
            while !handle.with_state(|state| {
                matches!(state, crate::ManagedTorrentState::Initializing(init) if init.is_pause_requested())
            }) {
                tokio::task::yield_now().await;
            }
            door.open();
            forgetting.await??;
            assert!(
                handle.tracker_status().is_none(),
                "the torrent ran before its pieces were forgotten"
            );

            session.start_check(&handle)?;
            handle.wait_until_initialized().await?;
            assert_eq!(next(&mut heard).await?, announce(Some("started")));
            answered(&handle).await?;
            let queries = tracker.queries.lock().clone();
            assert_eq!(queries.len(), 1, "{queries:?}");
            drop(tracker);
            session.stop().await;
            Ok(())
        }

        // The torrent of `a_check_told_to_stop_past_its_last_stop_says_nothing_to_a_tracker`
        // added to a session with a tracker, its check waiting on its
        // first read: the session, the torrent, the tracker with what it
        // heard, and the door.
        struct Tracked {
            session: Arc<Session>,
            handle: Arc<crate::ManagedTorrent>,
            tracker: crate::tests::e2e_pause::Tracker,
            heard: tokio::sync::mpsc::UnboundedReceiver<crate::tests::e2e_pause::Heard>,
            door: Arc<Door>,
            total: u64,
            _out: TempDir,
        }

        async fn checking_with_a_tracker(
            name: &str,
            checkpoint: Option<crate::Checkpoint>,
        ) -> anyhow::Result<Tracked> {
            let t = TestTorrent::new(name, 8 * 1024, 1024).await?;
            let out = TempDir::with_prefix(name)?;
            let (tracker, heard) = crate::tests::e2e_pause::Tracker::start(None);
            let session = Session::new_with_opts(
                out.path().into(),
                crate::tests::e2e_tracker_announces::tracker_session_opts()?,
            )
            .await?;
            let (reading_tx, mut reading) = tokio::sync::mpsc::unbounded_channel();
            let door = Door::shut(reading_tx);
            let storage = ShutStorageFactory {
                inner: MemoryStorageFactory::holding(vec![t.content.clone()]),
                door: door.clone(),
            };
            let handle = session
                .add_torrent(
                    AddTorrent::from_bytes(t.torrent.clone()),
                    Some(AddTorrentOptions {
                        storage_factory: Some(storage.clone_box()),
                        trackers: Some(vec![tracker.url()]),
                        checkpoint,
                        ..Default::default()
                    }),
                )
                .await?
                .into_handle()
                .context("expected a handle")?;
            reading.recv().await.context("the check read nothing")?;
            Ok(Tracked {
                session,
                handle,
                tracker,
                heard,
                door,
                total: 8 * 1024,
                _out: out,
            })
        }

        // A check under way when the torrent's files are shut to reads
        // (`FilesShut`, as a move that left a file among them that is not
        // the torrent's own shuts them) does not take the reads it is
        // refused for pieces that are missing: it ends, every piece is
        // forgotten, and the torrent does not run with none of them. Its
        // tracker hears nothing of it. The check that whoever changed the
        // files then begins reads them, finds every piece, and the tracker
        // hears the one `started` of the run after it.
        #[tokio::test(flavor = "multi_thread")]
        async fn a_check_that_meets_shut_files_ends_and_says_nothing_to_a_tracker()
        -> anyhow::Result<()> {
            use crate::tests::e2e_pause::{announce, answered, next};
            let Tracked {
                session,
                handle,
                tracker,
                mut heard,
                door,
                total,
                _out,
            } = checking_with_a_tracker("shut_under_check", None).await?;
            door.refusing.store(true, Ordering::Release);
            door.open();
            loop {
                let changed = handle.next_state_change();
                if door.forgotten.load(Ordering::Acquire) > 0
                    && handle.with_state(|state| {
                        matches!(state, crate::ManagedTorrentState::Initializing(init) if !init.is_checking())
                    })
                {
                    break;
                }
                changed.await;
            }
            assert!(
                handle.tracker_status().is_none(),
                "the torrent ran on a check that read nothing"
            );
            assert!(tracker.queries.lock().is_empty());
            assert_eq!(door.read.load(Ordering::Relaxed), 0);

            session.forget_for_recheck(&handle).await?;
            session.start_check(&handle)?;
            handle.wait_until_initialized().await?;
            assert_eq!(
                handle.stats().progress_bytes,
                total,
                "a piece was not found"
            );
            assert_eq!(next(&mut heard).await?, announce(Some("started")));
            answered(&handle).await?;
            let queries = tracker.queries.lock().clone();
            assert_eq!(queries.len(), 1, "{queries:?}");
            drop(tracker);
            session.stop().await;
            Ok(())
        }

        // A check ended paused on a stop asked of it past its last place
        // to stop, for pieces that are then forgotten, and the storage
        // cannot forget them: the torrent is not left paused with nothing
        // saying so and nobody to start it. It is in error, with the
        // reason, and the one who asked is told. Its next start checks
        // every piece and runs; its tracker hears that run's `started` and
        // nothing before it.
        #[tokio::test(flavor = "multi_thread")]
        async fn pieces_that_cannot_be_forgotten_leave_the_torrent_in_error_not_paused()
        -> anyhow::Result<()> {
            use crate::tests::e2e_pause::{announce, answered, next};
            let mut have = crate::BF::from_boxed_slice(vec![0xff].into_boxed_slice());
            have.set(3, false);
            let checkpoint = crate::Checkpoint {
                have,
                blocks: Some(crate::BF::from_boxed_slice(vec![0xff].into_boxed_slice())),
                files: None,
            };
            let Tracked {
                session,
                handle,
                tracker,
                mut heard,
                door,
                total,
                _out,
            } = checking_with_a_tracker("forget_fails", Some(checkpoint)).await?;
            let forgetting = {
                let session = session.clone();
                let handle = handle.clone();
                tokio::spawn(async move { session.forget_for_recheck(&handle).await })
            };
            while !handle.with_state(|state| {
                matches!(state, crate::ManagedTorrentState::Initializing(init) if init.is_pause_requested())
            }) {
                tokio::task::yield_now().await;
            }
            door.forgets_fail.store(true, Ordering::Release);
            door.open();
            let refused = forgetting.await?.unwrap_err();
            assert!(
                format!("{refused:#}").contains("the pieces are not forgotten"),
                "{refused:#}"
            );
            assert!(
                handle.with_state(|state| matches!(state, crate::ManagedTorrentState::Error(_))),
                "the torrent is not in error"
            );
            assert!(matches!(handle.stats().state, TorrentStatsState::Error));
            assert!(handle.tracker_status().is_none());

            door.forgets_fail.store(false, Ordering::Release);
            session.start_check(&handle)?;
            handle.wait_until_initialized().await?;
            assert_eq!(handle.stats().progress_bytes, total);
            assert_eq!(next(&mut heard).await?, announce(Some("started")));
            answered(&handle).await?;
            let queries = tracker.queries.lock().clone();
            assert_eq!(queries.len(), 1, "{queries:?}");
            drop(tracker);
            session.stop().await;
            Ok(())
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn a_running_check_of_files_that_changed_starts_over() -> anyhow::Result<()> {
            a_check_of_files_that_changed_starts_over("changed_under_check", false).await
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn a_paused_check_of_files_that_changed_starts_over() -> anyhow::Result<()> {
            a_check_of_files_that_changed_starts_over("changed_under_paused_check", true).await
        }
    }

    // Resume data that says a torrent has every piece is not trusted when
    // a piece it names does not hash: the files are checked, and the piece
    // is found missing. A torrent is added and checked with its session
    // keeping resume data, the session stops, a byte of the first piece
    // changes on disk, and a new session over the same resume data picks
    // the torrent up.
    #[test]
    fn a_spot_checked_piece_that_cannot_be_told_leaves_resume_data_trusted() {
        use super::says_files_differ;
        use crate::file_ops::Verdict;
        assert!(!says_files_differ(&Ok(Verdict::Passed)));
        assert!(!says_files_differ(&Ok(Verdict::Unknown)));
        assert!(says_files_differ(&Ok(Verdict::Failed)));
        assert!(says_files_differ(&Ok(Verdict::Inconsistent)));
        assert!(says_files_differ(&Err(anyhow::anyhow!("no file"))));
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn resume_data_naming_a_piece_that_does_not_hash_is_not_trusted() -> anyhow::Result<()> {
        use crate::tests::embedder_util::{TestTorrent, isolated_session_opts};
        use crate::{AddTorrent, AddTorrentOptions, Session, SessionPersistenceConfig};
        use anyhow::Context;

        const PIECE: u32 = 1024;
        let t = TestTorrent::new("resume_not_trusted", 8 * PIECE as usize, PIECE).await?;
        let state = tempfile::TempDir::with_prefix("resume_not_trusted_state")?;
        let opts = || crate::SessionOptions {
            persistence: Some(SessionPersistenceConfig::Json {
                folder: state.path().to_owned(),
            }),
            ..isolated_session_opts(false)
        };
        let total = {
            let session = Session::new_with_opts(t.dir.path().to_owned(), opts()).await?;
            let handle = session
                .add_torrent(
                    AddTorrent::from_bytes(t.torrent.clone()),
                    Some(AddTorrentOptions {
                        overwrite: true,
                        ..Default::default()
                    }),
                )
                .await?
                .into_handle()
                .context("expected a handle")?;
            handle.wait_until_completed().await?;
            let total = handle.stats().total_bytes;
            session.stop().await;
            total
        };

        let file = t.dir.path().join("0.data");
        let mut bytes = std::fs::read(&file)?;
        bytes[5] ^= 1;
        std::fs::write(&file, bytes)?;

        let session = Session::new_with_opts(t.dir.path().to_owned(), opts()).await?;
        let handle = session
            .with_torrents(|torrents| torrents.map(|(_, handle)| handle.clone()).next())
            .context("the torrent was not picked up")?;
        handle.wait_until_initialized().await?;
        let stats = handle.stats();
        assert_eq!(stats.total_bytes, total);
        assert_eq!(stats.progress_bytes, total - u64::from(PIECE));
        Ok(())
    }
}
