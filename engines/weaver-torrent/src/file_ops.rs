// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver
//
// Derived from rqbit (https://github.com/ikatson/rqbit), Copyright 2021 Igor Katson,
// licensed under the Apache License 2.0 (see LICENSE-APACHE). Modified by the
// Weaver authors from 2026-10-20 onward.

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};

use crate::buffers::{ByteBuf, ByteBufOwned};
use crate::core::{
    Id32,
    lengths::{ChunkInfo, ValidPieceIndex},
    merkle::BLOCK_SIZE,
    torrent_metainfo::ValidatedTorrentMetaV1Info,
};
use crate::peer_protocol::{DoubleBufHelper, Piece};
use crate::sha1w::{ISha1, ISha256, Sha1, Sha256};
use crate::torrent_state::TorrentMetadata;
use anyhow::{Context, bail};
use tracing::{debug, trace, warn};

use crate::{
    file_info::FileInfo,
    storage::TorrentStorage,
    type_aliases::{BF, FileInfos, PeerHandle},
};

/// What a piece's hashes say of its data. The v1 and the v2 hash each
/// pass, fail or are not known, and the two together decide (libtorrent
/// `torrent::on_piece_hashed` and `torrent::on_piece_verified`).
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Verdict {
    Passed,
    Failed,
    /// No hash to check the piece against is known yet.
    Unknown,
    /// The v1 and v2 hashes disagree: the torrent describes two payloads
    /// (libtorrent `torrent::handle_inconsistent_hashes`).
    Inconsistent,
}

impl Verdict {
    fn of(v1: Option<bool>, v2: Option<bool>) -> Self {
        match (v1, v2) {
            (Some(v1), Some(v2)) if v1 != v2 => Self::Inconsistent,
            (Some(true), _) | (_, Some(true)) => Self::Passed,
            (None, None) => Self::Unknown,
            _ => Self::Failed,
        }
    }
}

/// The error a torrent stops on when its hashes disagree (libtorrent
/// `errors::torrent_inconsistent_hashes`).
pub(crate) fn inconsistent_hashes(piece: ValidPieceIndex) -> anyhow::Error {
    anyhow::anyhow!("piece {piece}: v1 and v2 hashes do not describe the same data")
}

/// A piece's hashes as its bytes stream in: SHA-1 over the whole piece for
/// its v1 hash, and SHA-256 over each 16 KiB block of its file's share,
/// the padding after the file left out, for its v2 hash (libtorrent
/// `disk_io_thread` `hash` job).
struct PieceDigest {
    v1: Option<Sha1>,
    v2: Option<BlockDigest>,
}

struct BlockDigest {
    /// The bytes of the file's share still to come.
    share: u64,
    block: Sha256,
    in_block: u64,
    blocks: Vec<Id32>,
}

impl PieceDigest {
    fn update(&mut self, buf: &[u8]) {
        if let Some(v1) = &mut self.v1 {
            v1.update(buf);
        }
        let Some(v2) = &mut self.v2 else {
            return;
        };
        let take = buf
            .len()
            .min(usize::try_from(v2.share).unwrap_or(usize::MAX));
        let mut buf = &buf[..take];
        v2.share -= take as u64;
        while !buf.is_empty() {
            let take = buf.len().min((BLOCK_SIZE - v2.in_block) as usize);
            v2.block.update(&buf[..take]);
            v2.in_block += take as u64;
            buf = &buf[take..];
            if v2.in_block == BLOCK_SIZE {
                let block = std::mem::replace(&mut v2.block, Sha256::new());
                v2.blocks.push(Id32::new(block.finish()));
                v2.in_block = 0;
            }
        }
    }

    fn finish(self) -> (Option<[u8; 20]>, Option<Vec<Id32>>) {
        let v2 = self.v2.map(|mut v2| {
            if v2.in_block > 0 {
                v2.blocks.push(Id32::new(v2.block.finish()));
            }
            v2.blocks
        });
        (self.v1.map(ISha1::finish), v2)
    }
}

fn update_hash_from_file(
    file_id: usize,
    file_info: &FileInfo,
    mut pos: u64,
    files: &dyn TorrentStorage,
    hash: &mut PieceDigest,
    buf: &mut [u8],
    mut bytes_to_read: usize,
) -> anyhow::Result<()> {
    let mut read = 0;
    while bytes_to_read > 0 {
        let chunk = std::cmp::min(buf.len(), bytes_to_read);
        if file_info.attrs.padding {
            buf[..chunk].fill(0);
        } else {
            files
                .pread_exact(file_id, pos, &mut buf[..chunk])
                .with_context(|| {
                    format!("failed reading chunk of size {chunk}, read so far {read}")
                })?;
        }
        bytes_to_read -= chunk;
        read += chunk;
        pos += chunk as u64;
        hash.update(&buf[..chunk]);
    }
    Ok(())
}

pub(crate) struct FileOps<'a> {
    metadata: &'a TorrentMetadata,
    torrent: &'a ValidatedTorrentMetaV1Info<ByteBufOwned>,
    files: &'a dyn TorrentStorage,
    file_infos: &'a FileInfos,
}

impl<'a> FileOps<'a> {
    pub fn new(metadata: &'a TorrentMetadata, files: &'a dyn TorrentStorage) -> Self {
        Self {
            metadata,
            torrent: &metadata.info,
            files,
            file_infos: &metadata.file_infos,
        }
    }

    /// A digest for `piece`: v1 when the torrent has v1 hashes, v2 when
    /// what its blocks must hash to is known.
    fn digest(&self, piece: ValidPieceIndex) -> PieceDigest {
        let v2 = self.metadata.v2.as_ref().and_then(|v2| {
            v2.expected(piece.get())?;
            Some(BlockDigest {
                share: v2.share(piece.get())?,
                block: Sha256::new(),
                in_block: 0,
                blocks: Vec::new(),
            })
        });
        PieceDigest {
            v1: self.torrent.info().has_v1_listing().then(Sha1::new),
            v2,
        }
    }

    /// Whether `piece`'s v1 and v2 hashes match its digest, each `None`
    /// when not known.
    fn hashes_match(
        &self,
        piece: ValidPieceIndex,
        digest: PieceDigest,
    ) -> anyhow::Result<(Option<bool>, Option<bool>)> {
        let (sha1, blocks) = digest.finish();
        let v1 = sha1
            .map(|sha1| {
                self.torrent.info().compare_hash(piece.get(), sha1).context(
                    "bug: either torrent info broken or we have a bug - piece index invalid",
                )
            })
            .transpose()?;
        let v2 = blocks.and_then(|blocks| {
            self.metadata
                .v2
                .as_ref()?
                .compare_hash(piece.get(), &blocks)
        });
        Ok((v1, v2))
    }

    // Returns the bitvector with pieces we have.
    pub fn initial_check(
        &self,
        progress: &AtomicU64,
        pause_requested: &AtomicBool,
    ) -> anyhow::Result<BF> {
        let mut have_pieces =
            BF::from_boxed_slice(vec![0u8; self.torrent.lengths().piece_bitfield_bytes()].into());
        let mut piece_files = Vec::<usize>::new();

        #[derive(Debug)]
        struct CurrentFile<'a> {
            index: usize,
            fi: &'a FileInfo,
            processed_bytes: u64,
            is_broken: bool,
        }
        impl CurrentFile<'_> {
            fn remaining(&self) -> u64 {
                self.fi.len - self.processed_bytes
            }
            fn mark_processed_bytes(&mut self, bytes: u64) {
                self.processed_bytes += bytes
            }
        }
        let mut file_iterator = self
            .file_infos
            .iter()
            .enumerate()
            .map(|(idx, fi)| CurrentFile {
                index: idx,
                fi,
                processed_bytes: 0,
                is_broken: false,
            });

        let mut current_file = file_iterator.next().context("empty input file list")?;

        let mut read_buffer = vec![0u8; 65536];

        for piece_info in self.torrent.lengths().iter_piece_infos() {
            if pause_requested.load(Ordering::Relaxed) {
                bail!("initial check paused");
            }

            piece_files.clear();
            let mut computed_hash = self.digest(piece_info.piece_index);
            let mut piece_remaining = piece_info.len as usize;
            let mut some_files_broken = false;
            progress.fetch_add(piece_info.len as u64, Ordering::Relaxed);

            while piece_remaining > 0 {
                let mut to_read_in_file: usize =
                    std::cmp::min(current_file.remaining(), piece_remaining as u64).try_into()?;

                // Keep changing the current file to next until we find a file that has greater than 0 length.
                while to_read_in_file == 0 {
                    current_file = file_iterator.next().context("broken torrent metadata")?;

                    to_read_in_file =
                        std::cmp::min(current_file.remaining(), piece_remaining as u64)
                            .try_into()?;
                }

                piece_files.push(current_file.index);

                let pos = current_file.processed_bytes;
                piece_remaining -= to_read_in_file;
                current_file.mark_processed_bytes(to_read_in_file as u64);

                if current_file.is_broken {
                    // no need to read.
                    continue;
                }

                if let Err(err) = update_hash_from_file(
                    current_file.index,
                    current_file.fi,
                    pos,
                    self.files,
                    &mut computed_hash,
                    &mut read_buffer,
                    to_read_in_file,
                ) {
                    debug!(
                        "error reading from file {} ({:?}) at {}: {:#}",
                        current_file.index, current_file.fi.relative_filename, pos, &err
                    );
                    current_file.is_broken = true;
                    some_files_broken = true;
                }
            }

            if some_files_broken {
                trace!(
                    "piece {} had errors, marking as needed",
                    piece_info.piece_index
                );
                continue;
            }

            // A piece whose v1 hash fails is not checked against v2: its data
            // is most likely missing (libtorrent `torrent::on_piece_hashed`).
            let (v1, v2) = self.hashes_match(piece_info.piece_index, computed_hash)?;
            match Verdict::of(v1, v2.filter(|_| v1 != Some(false))) {
                Verdict::Passed => have_pieces.set(piece_info.piece_index.get() as usize, true),
                Verdict::Failed | Verdict::Unknown => {}
                Verdict::Inconsistent => return Err(inconsistent_hashes(piece_info.piece_index)),
            }
        }

        Ok(have_pieces)
    }

    /// A downloaded piece checked against both its hashes, the v2 one even
    /// when v1 fails, so that a torrent whose hashes disagree stops
    /// (libtorrent `torrent::on_piece_verified`).
    pub fn check_piece(&self, piece_index: ValidPieceIndex) -> anyhow::Result<Verdict> {
        let mut h = self.digest(piece_index);
        let piece_length = self.torrent.lengths().piece_length(piece_index);
        let mut absolute_offset = self.torrent.lengths().piece_offset(piece_index);
        let mut buf = vec![0u8; std::cmp::min(65536, piece_length as usize)];

        let mut piece_remaining_bytes = piece_length as usize;

        for (file_idx, fi) in self.file_infos.iter().enumerate() {
            let file_len = fi.len;
            if absolute_offset > file_len {
                absolute_offset -= file_len;
                continue;
            }
            let file_remaining_len = file_len - absolute_offset;

            let to_read_in_file: usize =
                std::cmp::min(file_remaining_len, piece_remaining_bytes as u64).try_into()?;
            trace!(
                "piece={}, file_idx={}, seeking to {}",
                piece_index, file_idx, absolute_offset,
            );
            update_hash_from_file(
                file_idx,
                fi,
                absolute_offset,
                self.files,
                &mut h,
                &mut buf,
                to_read_in_file,
            )
            .with_context(|| {
                format!(
                    "error reading {to_read_in_file} bytes, file_id: {file_idx} (\"{:?}\")",
                    fi.relative_filename
                )
            })?;

            piece_remaining_bytes -= to_read_in_file;

            if piece_remaining_bytes == 0 {
                break;
            }

            absolute_offset = 0;
        }

        let (v1, v2) = self.hashes_match(piece_index, h)?;
        let verdict = Verdict::of(v1, v2);
        match verdict {
            Verdict::Passed => trace!("piece={} hash matches", piece_index),
            _ => {
                let piece_length = self.torrent.lengths().piece_length(piece_index);
                let absolute_offset = self.torrent.lengths().piece_offset(piece_index);
                warn!(
                    piece_length,
                    absolute_offset,
                    ?verdict,
                    "the piece={} hash does not match",
                    piece_index
                );
            }
        }
        Ok(verdict)
    }

    pub fn read_chunk(
        &self,
        who_sent: PeerHandle,
        chunk_info: &ChunkInfo,
        result_buf: &mut [u8],
    ) -> anyhow::Result<()> {
        if result_buf.len() < chunk_info.size as usize {
            anyhow::bail!("read_chunk(): not enough capacity in the provided buffer")
        }
        let mut absolute_offset = self.torrent.lengths().chunk_absolute_offset(chunk_info);
        let mut buf = result_buf;

        for (file_idx, file_info) in self.file_infos.iter().enumerate() {
            let file_len = file_info.len;
            if absolute_offset > file_len {
                absolute_offset -= file_len;
                continue;
            }
            let file_remaining_len = file_len - absolute_offset;
            let to_read_in_file = std::cmp::min(file_remaining_len, buf.len() as u64).try_into()?;

            trace!(
                "piece={}, handle={}, file_idx={}, seeking to {}. To read chunk: {:?}",
                chunk_info.piece_index, who_sent, file_idx, absolute_offset, &chunk_info
            );
            if file_info.attrs.padding {
                buf[..to_read_in_file].fill(0);
            } else {
                self.files
                    .pread_exact(file_idx, absolute_offset, &mut buf[..to_read_in_file])
                    .with_context(|| {
                        format!("error reading {file_idx} bytes, file_id: {to_read_in_file}")
                    })?;
            }

            buf = &mut buf[to_read_in_file..];

            if buf.is_empty() {
                break;
            }

            absolute_offset = 0;
        }

        Ok(())
    }

    pub fn write_chunk(
        &self,
        who_sent: PeerHandle,
        data: &Piece<ByteBuf<'a>>,
        chunk_info: &ChunkInfo,
    ) -> anyhow::Result<()> {
        let mut absolute_offset = self.torrent.lengths().chunk_absolute_offset(chunk_info);
        let mut data = DoubleBufHelper::new(data.data().0, data.data().1);

        for (file_idx, file_info) in self.file_infos.iter().enumerate() {
            let file_len = file_info.len;
            if absolute_offset > file_len {
                absolute_offset -= file_len;
                continue;
            }

            let remaining_len = file_len - absolute_offset;
            let to_write = std::cmp::min(data.len() as u64, remaining_len).try_into()?;

            trace!(
                "piece={}, chunk={:?}, handle={}, begin={}, file={}, writing {} bytes at {}",
                chunk_info.piece_index,
                chunk_info,
                who_sent,
                chunk_info.offset,
                file_idx,
                to_write,
                absolute_offset
            );
            let slices = data.as_ioslices(to_write);
            debug_assert_eq!(slices[0].len() + slices[1].len(), to_write);
            if !file_info.attrs.padding {
                let written = self
                    .files
                    .pwrite_all_vectored(file_idx, absolute_offset, slices)
                    .with_context(|| {
                        format!(
                            "error writing to file {file_idx} (\"{:?}\")",
                            file_info.relative_filename
                        )
                    })?;
                debug_assert_eq!(written, to_write);
            }
            data.advance(to_write);
            if data.is_empty() {
                break;
            }

            absolute_offset = 0;
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;
    use std::sync::atomic::{AtomicBool, AtomicU64};

    use super::*;
    use crate::core::v2_test_torrent::{TestTorrent, metadata, payload};
    use crate::torrent_state::ManagedTorrentShared;

    /// Each file's bytes, read only.
    struct Files(Vec<Vec<u8>>);

    impl Files {
        /// `payload` laid out over the torrent's files.
        fn new(metadata: &TorrentMetadata, payload: &[u8]) -> Self {
            Files(
                metadata
                    .file_infos
                    .iter()
                    .map(|fi| {
                        let start = fi.offset_in_torrent as usize;
                        payload[start..start + fi.len as usize].to_vec()
                    })
                    .collect(),
            )
        }
    }

    impl TorrentStorage for Files {
        fn init(
            &mut self,
            _shared: &ManagedTorrentShared,
            _metadata: &TorrentMetadata,
        ) -> anyhow::Result<()> {
            Ok(())
        }

        fn pread_exact(&self, file_id: usize, offset: u64, buf: &mut [u8]) -> anyhow::Result<()> {
            let start = offset as usize;
            buf.copy_from_slice(
                self.0[file_id]
                    .get(start..start + buf.len())
                    .context("read past the end")?,
            );
            Ok(())
        }

        fn pwrite_all(&self, _file_id: usize, _offset: u64, _buf: &[u8]) -> anyhow::Result<()> {
            bail!("read only")
        }

        fn remove_file(&self, _file_id: usize, _filename: &Path) -> anyhow::Result<()> {
            bail!("read only")
        }

        fn remove_directory_if_empty(&self, _path: &Path) -> anyhow::Result<()> {
            bail!("read only")
        }

        fn ensure_file_length(&self, _file_id: usize, _length: u64) -> anyhow::Result<()> {
            bail!("read only")
        }

        fn take(&self) -> anyhow::Result<Box<dyn TorrentStorage>> {
            Ok(Box::new(Files(self.0.clone())))
        }
    }

    fn check_all(metadata: &TorrentMetadata, payload: &[u8]) -> Vec<Verdict> {
        let files = Files::new(metadata, payload);
        let ops = FileOps::new(metadata, &files);
        metadata
            .lengths()
            .iter_piece_infos()
            .map(|p| ops.check_piece(p.piece_index).unwrap())
            .collect()
    }

    fn initial_check(metadata: &TorrentMetadata, payload: &[u8]) -> anyhow::Result<Vec<bool>> {
        let files = Files::new(metadata, payload);
        let have = FileOps::new(metadata, &files)
            .initial_check(&AtomicU64::new(0), &AtomicBool::new(false))?;
        Ok(have
            .iter()
            .by_vals()
            .take(metadata.lengths().total_pieces() as usize)
            .collect())
    }

    /// A file under a block and one of three pieces and a tail, at 32 KiB
    /// pieces: pieces 0 (a, padded), 1..=4 (b).
    fn two_files() -> TestTorrent {
        TestTorrent::new(
            vec![("a", payload(100, 1)), ("b", payload(3 * 32768 + 5, 2))],
            32768,
        )
    }

    #[test]
    fn the_verdict_is_libtorrents_tribool() {
        // libtorrent torrent::on_piece_hashed / on_piece_verified: a hash
        // that is not known defers to the other, a disagreement is an
        // error, and neither known leaves the piece unverified.
        use Verdict::*;
        for (v1, v2, verdict) in [
            (Some(true), None, Passed),
            (None, Some(true), Passed),
            (Some(true), Some(true), Passed),
            (Some(false), None, Failed),
            (None, Some(false), Failed),
            (Some(false), Some(false), Failed),
            (Some(true), Some(false), Inconsistent),
            (Some(false), Some(true), Inconsistent),
            (None, None, Unknown),
        ] {
            assert_eq!(Verdict::of(v1, v2), verdict, "{v1:?} {v2:?}");
        }
    }

    #[test]
    fn a_v2_piece_is_checked_on_the_blocks_of_its_files_share() {
        let t = two_files();
        let metadata = metadata(&t.torrent_bytes());
        let mut payload = t.v1_payload();
        // Piece 0 passes only if a's padding is left out of its one block.
        assert_eq!(check_all(&metadata, &payload), vec![Verdict::Passed; 5]);
        assert_eq!(initial_check(&metadata, &payload).unwrap(), vec![true; 5]);

        // A byte flipped in b's third piece fails that piece alone.
        payload[3 * 32768 + 7] ^= 1;
        let verdicts = check_all(&metadata, &payload);
        assert_eq!(verdicts[3], Verdict::Failed);
        assert_eq!(
            verdicts.iter().filter(|v| **v == Verdict::Passed).count(),
            4
        );
        assert_eq!(
            initial_check(&metadata, &payload).unwrap(),
            vec![true, true, true, false, true]
        );
    }

    #[test]
    fn a_v2_piece_is_unknown_until_its_files_layer_is_held() {
        let mut t = two_files();
        t.layers = false;
        let metadata = metadata(&t.torrent_bytes());
        let payload = t.v1_payload();
        let verdicts = check_all(&metadata, &payload);
        assert_eq!(verdicts[0], Verdict::Passed);
        assert_eq!(verdicts[1..], [Verdict::Unknown; 4]);
        // Not held, so fetched again; libtorrent would hold it unverified.
        assert_eq!(
            initial_check(&metadata, &payload).unwrap(),
            vec![true, false, false, false, false]
        );
        // And not fetched until the layer is held.
        let lengths = metadata.lengths();
        let can = |p| metadata.can_verify(lengths.validate_piece_index(p).unwrap());
        assert_eq!(
            (0..5).map(can).collect::<Vec<_>>(),
            [true, false, false, false, false]
        );
    }

    /// A hybrid whose v1 hashes are `v1`'s and v2 hashes `v2`'s.
    fn hybrid(v1: &TestTorrent, v2: &TestTorrent) -> TorrentMetadata {
        metadata(&v2.torrent_bytes_with_info(&v2.info_bytes_with_pieces(&v1.v1_pieces())))
    }

    fn two_hybrids() -> (TestTorrent, TestTorrent) {
        let mut real = two_files();
        real.hybrid = true;
        let mut other = TestTorrent::new(
            vec![("a", payload(100, 9)), ("b", payload(3 * 32768 + 5, 8))],
            32768,
        );
        other.hybrid = true;
        (real, other)
    }

    #[test]
    fn a_hybrid_piece_passes_on_v1_while_v2_is_unknown() {
        let (mut real, _) = two_hybrids();
        real.layers = false;
        let metadata = metadata(&real.torrent_bytes());
        let payload = real.v1_payload();
        assert_eq!(check_all(&metadata, &payload), vec![Verdict::Passed; 5]);
        assert_eq!(initial_check(&metadata, &payload).unwrap(), vec![true; 5]);
        let lengths = metadata.lengths();
        assert!((0..5).all(|p| metadata.can_verify(lengths.validate_piece_index(p).unwrap())));

        let mut payload = payload;
        payload[2 * 32768] ^= 1;
        assert_eq!(check_all(&metadata, &payload)[2], Verdict::Failed);
    }

    #[test]
    fn a_hybrid_with_bad_v1_hashes_is_inconsistent_once_downloaded() {
        // libtorrent test_transfer.cpp corrupt_v1_hashes: a hybrid with
        // wrong v1 hashes and right v2 hashes ends in
        // torrent_inconsistent_hashes. The check skips v2 for a piece
        // whose v1 hash fails, so there it is only missing.
        let (real, other) = two_hybrids();
        let metadata = hybrid(&other, &real);
        let payload = real.v1_payload();
        assert_eq!(
            check_all(&metadata, &payload),
            vec![Verdict::Inconsistent; 5]
        );
        assert_eq!(initial_check(&metadata, &payload).unwrap(), vec![false; 5]);
    }

    #[test]
    fn a_hybrid_with_bad_v2_hashes_is_inconsistent_at_the_check() {
        // libtorrent torrent::on_piece_hashed: v1 passes, v2 fails.
        let (real, other) = two_hybrids();
        let metadata = hybrid(&real, &other);
        let payload = real.v1_payload();
        assert_eq!(
            check_all(&metadata, &payload),
            vec![Verdict::Inconsistent; 5]
        );
        let err = initial_check(&metadata, &payload).unwrap_err();
        assert!(
            format!("{err}").contains("v1 and v2 hashes do not describe the same data"),
            "{err}"
        );
    }
}
