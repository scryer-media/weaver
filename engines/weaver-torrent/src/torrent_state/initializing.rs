// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver
//
// Derived from rqbit (https://github.com/ikatson/rqbit), Copyright 2021 Igor Katson,
// licensed under the Apache License 2.0 (see LICENSE-APACHE). Modified by the
// Weaver authors from 2026-10-20 onward.

use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
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
    chunk_tracker::{ChunkTracker, compute_selected_pieces},
    file_ops::FileOps,
    piece_priority::PiecePriority,
    type_aliases::{BF, FileStorage},
};

const MAX_FASTRESUME_CHECKS: usize = 64;

use super::{ManagedTorrentShared, TorrentMetadata, paused::TorrentStatePaused};

pub struct TorrentStateInitializing {
    pub(crate) files: FileStorage,
    pub(crate) shared: Arc<ManagedTorrentShared>,
    pub(crate) metadata: Arc<TorrentMetadata>,
    pub(crate) only_files: Option<Vec<usize>>,
    pub(crate) checked_bytes: AtomicU64,
    pause_requested: AtomicBool,
    check_running: AtomicBool,
    previously_errored: bool,
    // Pieces the embedder says are present and verified: when set, they are
    // the have set and no piece is read back to check it.
    initial_have: Option<BF>,
    // The fetch priority of every piece; None while all are Normal.
    piece_priorities: Option<Vec<PiecePriority>>,
}

/// Bring a caller-supplied have-bitfield to the torrent's BEP 3 bitfield
/// layout: one bit per piece, rounded up to whole bytes, spare bits clear.
///
/// It accepts exactly the piece count in bits, or the byte-rounded length with
/// its spare bits clear, as a peer's BITFIELD message must be (BEP 3).
pub(crate) fn normalize_initial_have(have: BF, lengths: &Lengths) -> anyhow::Result<BF> {
    let pieces = lengths.total_pieces() as usize;
    let bytes = lengths.piece_bitfield_bytes();
    let bits = have.len();
    if bits != pieces && bits != bytes * 8 {
        anyhow::bail!(
            "initial_have has {bits} bits, but the torrent has {pieces} pieces: expected {pieces} bits, or {bytes} bytes with the spare bits clear"
        );
    }
    if have[pieces..].any() {
        anyhow::bail!(
            "initial_have marks pieces past the last one ({pieces} pieces): the spare bits must be clear"
        );
    }
    let mut normalized = BF::from_boxed_slice(vec![0u8; bytes].into_boxed_slice());
    normalized[..pieces].copy_from_bitslice(&have[..pieces]);
    Ok(normalized)
}

impl TorrentStateInitializing {
    pub fn new(
        shared: Arc<ManagedTorrentShared>,
        metadata: Arc<TorrentMetadata>,
        only_files: Option<Vec<usize>>,
        files: FileStorage,
        previously_errored: bool,
        initial_have: Option<BF>,
        piece_priorities: Option<Vec<PiecePriority>>,
    ) -> Self {
        Self {
            shared,
            metadata,
            only_files,
            files,
            checked_bytes: AtomicU64::new(0),
            pause_requested: AtomicBool::new(false),
            check_running: AtomicBool::new(false),
            previously_errored,
            initial_have,
            piece_priorities,
        }
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
        self.check_running
            .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
            .is_ok()
    }

    pub(crate) fn finish_check(&self) {
        self.check_running.store(false, Ordering::Release);
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
                let fo = crate::file_ops::FileOps::new(&self.metadata, &self.files);

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

    // The have set from fastresume data when it holds up, else from hashing every piece.
    async fn resume_or_check(
        &self,
        bitv_factory: &dyn BitVFactory,
        id: TorrentIdOrHash,
    ) -> anyhow::Result<Box<dyn BitV>> {
        let have_pieces = if self.previously_errored {
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

        info!("Doing initial checksum validation, this might take a while...");
        let have_pieces = self
            .shared
            .spawner
            .block_in_place_with_semaphore(|| {
                FileOps::new(&self.metadata, &self.files)
                    .initial_check(&self.checked_bytes, &self.pause_requested)
            })
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

        let have_pieces = match self.initial_have.as_ref() {
            Some(have) => {
                // The embedder vouches for these pieces, so none is read back. Storing
                // them makes them the fastresume state too, replacing whatever was there.
                info!(
                    torrent=?self.shared.id,
                    "using the supplied have-bitfield, skipping the initial check"
                );
                bitv_factory
                    .store_initial_check(id, have.clone())
                    .await
                    .context("error storing the supplied have-bitfield")?
            }
            None => self.resume_or_check(&*bitv_factory, id).await?,
        };

        let selected_pieces = compute_selected_pieces(
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
            selected_pieces,
            *self.metadata.lengths(),
            &self.metadata.file_infos,
        )
        .context("error creating chunk tracker")?;
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
            "Initial check results: have {}, needed {}, total selected {}",
            BinarySize(hns.have_bytes),
            BinarySize(hns.needed_bytes),
            BinarySize(hns.selected_bytes)
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
        };
        Ok(paused)
    }
}

#[cfg(test)]
mod tests {
    use crate::core::lengths::Lengths;

    use super::normalize_initial_have;
    use crate::type_aliases::BF;

    fn bf(bits: &[bool]) -> BF {
        bits.iter()
            .copied()
            .collect::<bitvec::vec::BitVec<u8, bitvec::order::Msb0>>()
            .into_boxed_bitslice()
    }

    #[test]
    fn test_normalize_initial_have() {
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
}
