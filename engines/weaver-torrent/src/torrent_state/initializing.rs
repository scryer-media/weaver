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
    file_ops::{FileOps, Verdict, inconsistent_hashes},
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
// what was saved or vouched for; or one reads every piece back, its progress
// in `checked_bytes`.
const CHECK_IDLE: u8 = 0;
const CHECK_STARTED: u8 = 1;
const CHECK_READING: u8 = 2;

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

impl TorrentStateInitializing {
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

    /// Whether the check reads every piece back now; one that starts from
    /// a checkpoint or from saved pieces that hold up does not.
    pub(crate) fn is_reading_pieces(&self) -> bool {
        self.check.load(Ordering::Acquire) == CHECK_READING
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
            .block_in_place_with_semaphore(|| {
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
                    if fo.check_piece(piece_id).is_err() {
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
    /// Returns the blocks left.
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
            .block_in_place_with_semaphore(|| {
                let fo = FileOps::new(
                    &self.metadata,
                    &self.files,
                    self.shared.options.disable_v1_hashes,
                );
                finished
                    .iter()
                    .map(|piece| (*piece, fo.check_piece(*piece)))
                    .collect::<Vec<_>>()
            })
            .await;
        for (piece, verdict) in verdicts {
            match verdict {
                Ok(Verdict::Passed) => have.set(piece.get_usize(), true),
                Ok(Verdict::Inconsistent) => return Err(inconsistent_hashes(piece)),
                Ok(Verdict::Failed | Verdict::Unknown) | Err(_) => {
                    blocks[lengths.chunk_range(piece)].fill(false)
                }
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
    async fn resume_or_check(
        &self,
        bitv_factory: &dyn BitVFactory,
        id: TorrentIdOrHash,
        full: bool,
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
        // its turn here (libtorrent `active_checking`).
        let session = self.shared.session.upgrade();
        let _turn = match &session {
            Some(session) => Some(
                session
                    .concurrent_initialize_semaphore
                    .acquire()
                    .await
                    .context("bug: concurrent check semaphore was closed")?,
            ),
            None => None,
        };
        info!("Doing initial checksum validation, this might take a while...");
        let have_pieces = self
            .shared
            .spawner
            .block_in_place_with_permits(
                (self.metadata.lengths().total_pieces() as usize)
                    .min(self.shared.spawner.check_threads()),
                |threads| {
                    self.check.store(CHECK_READING, Ordering::Release);
                    FileOps::new(
                        &self.metadata,
                        &self.files,
                        self.shared.options.disable_v1_hashes,
                    )
                    .initial_check(
                        threads,
                        &self.checked_bytes,
                        &self.pause_requested,
                    )
                },
            )
            .await?;
        bitv_factory
            .store_initial_check(id, have_pieces)
            .await
            .context("error storing initial check bitfield")
    }

    pub async fn check(&self) -> anyhow::Result<TorrentStatePaused> {
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
                self.resume_or_check(&*bitv_factory, id, full_check).await?,
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

        // A torrent initializing reads as checking only while its check
        // reads pieces back: not while it waits its turn to check, and
        // until its check is done once it reads.
        #[tokio::test(flavor = "multi_thread")]
        async fn a_torrent_reads_as_checking_only_while_it_reads_pieces_back() -> anyhow::Result<()>
        {
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
            assert!(matches!(
                handle.stats().state,
                TorrentStatsState::Initializing {
                    checking: false,
                    ..
                }
            ));

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
    }
}
