// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver
//
// Derived from rqbit (https://github.com/ikatson/rqbit), Copyright 2021 Igor Katson,
// licensed under the Apache License 2.0 (see LICENSE-APACHE). Modified by the
// Weaver authors from 2026-10-20 onward.

use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};

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
            let take = buf
                .len()
                .min(usize::try_from(BLOCK_SIZE - v2.in_block).unwrap_or(usize::MAX));
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

/// Whether a read failed because the file ends before the range, or is not
/// there at all.
fn ends_the_file(err: &anyhow::Error) -> bool {
    err.chain()
        .filter_map(|cause| cause.downcast_ref::<std::io::Error>())
        .any(|io| {
            matches!(
                io.kind(),
                std::io::ErrorKind::UnexpectedEof | std::io::ErrorKind::NotFound
            )
        })
}

pub(crate) struct FileOps<'a> {
    metadata: &'a TorrentMetadata,
    torrent: &'a ValidatedTorrentMetaV1Info<ByteBufOwned>,
    files: &'a dyn TorrentStorage,
    file_infos: &'a FileInfos,
    disable_v1_hashes: bool,
}

impl<'a> FileOps<'a> {
    /// `disable_v1_hashes` is [`AddTorrentOptions::disable_v1_hashes`](crate::AddTorrentOptions::disable_v1_hashes).
    pub fn new(
        metadata: &'a TorrentMetadata,
        files: &'a dyn TorrentStorage,
        disable_v1_hashes: bool,
    ) -> Self {
        Self {
            metadata,
            torrent: &metadata.info,
            files,
            file_infos: &metadata.file_infos,
            disable_v1_hashes,
        }
    }

    /// A digest for `piece`: v2 when what its blocks must hash to is
    /// known, v1 when the torrent has v1 hashes, unless v1 hashes are
    /// disabled and v2 is known (libtorrent `torrent::construct_storage`
    /// and `torrent::start_checking`, `disable_v1_hashes`).
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
        let v1 = self.torrent.info().has_v1_listing() && !(self.disable_v1_hashes && v2.is_some());
        PieceDigest {
            v1: v1.then(Sha1::new),
            v2,
        }
    }

    /// Whether `piece`'s v1 and v2 hashes match its digest, each `None`
    /// when not known.
    fn hashes_match(
        &self,
        piece: ValidPieceIndex,
        digest: PieceDigest,
    ) -> (Option<bool>, Option<bool>) {
        let (sha1, blocks) = digest.finish();
        let v1 = sha1.and_then(|sha1| self.torrent.v1_hash_matches(piece, sha1));
        let v2 = blocks.and_then(|blocks| {
            self.metadata
                .v2
                .as_ref()?
                .compare_hash(piece.get(), &blocks)
        });
        (v1, v2)
    }

    /// The pieces whose data is already there, hashed on `threads` threads
    /// at once. Each takes the next piece no thread has taken, reads it
    /// once with its own buffer and hashes it as it reads (libtorrent
    /// `torrent::start_checking` keeps a hash job per piece out to its
    /// `hashing_threads`). The verdicts land in the bitfield in piece
    /// order, the first error by piece ending the check; pausing stops it
    /// at the next piece.
    pub fn initial_check(
        &self,
        threads: usize,
        progress: &AtomicU64,
        pause_requested: &AtomicBool,
    ) -> anyhow::Result<BF> {
        let lengths = self.torrent.lengths();
        let total = lengths.total_pieces() as usize;
        let next = AtomicUsize::new(0);
        let broken: Vec<AtomicU64> = self
            .file_infos
            .iter()
            .map(|_| AtomicU64::new(u64::MAX))
            .collect();
        let worker = || {
            let mut buf = vec![0u8; 65536];
            let mut checked = Vec::new();
            while !pause_requested.load(Ordering::Relaxed) {
                let taken = next.fetch_add(1, Ordering::Relaxed);
                let Some(piece) = u32::try_from(taken)
                    .ok()
                    .and_then(|p| lengths.validate_piece_index(p))
                else {
                    break;
                };
                progress.fetch_add(lengths.piece_length(piece).into(), Ordering::Relaxed);
                // A piece the storage wants as hashed is read whole, once,
                // and goes to it from that buffer if it passes.
                let mut whole = self
                    .files
                    .wants_hashed_piece(piece)
                    .then(|| vec![0u8; lengths.piece_length(piece) as usize]);
                let had = self.has_piece(piece, &broken, &mut buf, whole.as_deref_mut());
                if let (Ok(true), Some(whole)) = (&had, whole) {
                    self.files.on_piece_hashed(piece, whole.into());
                }
                if had.is_err() {
                    next.fetch_max(total, Ordering::Relaxed);
                }
                checked.push((piece, had));
            }
            checked
        };
        let mut checked = std::thread::scope(|s| {
            let others: Vec<_> = (1..threads.clamp(1, total))
                .map(|_| s.spawn(worker))
                .collect();
            let mut checked = worker();
            for other in others {
                checked.extend(
                    other
                        .join()
                        .unwrap_or_else(|e| std::panic::resume_unwind(e)),
                );
            }
            checked
        });

        checked.sort_unstable_by_key(|(piece, _)| piece.get());
        let done = checked.len();
        let mut have_pieces =
            BF::from_boxed_slice(vec![0u8; lengths.piece_bitfield_bytes()].into());
        // The storage hears each verdict: a piece the check found it has, and
        // one the check did not find failed its hash as a downloaded one
        // does, so the storage lets go of whatever it held of it. What the
        // storage holds is what the check found (libtorrent
        // `torrent::on_piece_hashed`: `we_have` on a pass, nothing on a miss).
        for (piece, had) in checked {
            let had = had?;
            if had {
                self.files
                    .on_piece_checked(piece)
                    .with_context(|| format!("the storage did not take piece {piece}"))?;
            } else {
                self.files
                    .on_piece_hash_failed(piece)
                    .with_context(|| format!("the storage did not let go of piece {piece}"))?;
            }
            have_pieces.set(piece.get() as usize, had);
        }
        if done < total {
            bail!("initial check paused");
        }
        Ok(have_pieces)
    }

    /// Whether `piece`'s data is there, read once from the files it spans.
    /// A file that ends at some offset, or is not there at all, is `broken`
    /// from there and not read there again: every piece of it from there on
    /// is needed, whichever piece found it. Any other miss fails that piece
    /// alone (libtorrent `torrent::on_piece_hashed` skips the rest of a file
    /// only on eof, a missing file or a file too short).
    /// When `whole` is given, the piece is read into it rather than through `buf`.
    fn has_piece(
        &self,
        piece: ValidPieceIndex,
        broken: &[AtomicU64],
        buf: &mut [u8],
        mut whole: Option<&mut [u8]>,
    ) -> anyhow::Result<bool> {
        let lengths = self.torrent.lengths();
        let start = lengths.piece_offset(piece);
        let mut pos = start;
        let mut remaining = u64::from(lengths.piece_length(piece));
        let mut digest = self.digest(piece);
        let first = self
            .file_infos
            .partition_point(|fi| fi.offset_in_torrent + fi.len <= pos);
        for (file, fi) in self.file_infos.iter().enumerate().skip(first) {
            if remaining == 0 {
                break;
            }
            if fi.len == 0 {
                continue;
            }
            let at = pos - fi.offset_in_torrent;
            let take = remaining.min(fi.len - at);
            if at >= broken[file].load(Ordering::Relaxed) {
                return Ok(false);
            }
            let into = match whole.as_deref_mut() {
                Some(whole) => {
                    let from = usize::try_from(pos - start)?;
                    &mut whole[from..from + usize::try_from(take)?]
                }
                None => &mut *buf,
            };
            if let Err(err) = update_hash_from_file(
                file,
                fi,
                at,
                self.files,
                &mut digest,
                into,
                take.try_into()?,
            ) {
                debug!(
                    "error reading from file {file} ({:?}) at {at}: {err:#}",
                    fi.relative_filename
                );
                if ends_the_file(&err) {
                    broken[file].fetch_min(at, Ordering::Relaxed);
                }
                trace!("piece {piece} had errors, marking as needed");
                return Ok(false);
            }
            pos += take;
            remaining -= take;
        }
        anyhow::ensure!(remaining == 0, "broken torrent metadata");

        // A piece whose v1 hash fails is not checked against v2: its data
        // is most likely missing (libtorrent `torrent::on_piece_hashed`).
        let (v1, v2) = self.hashes_match(piece, digest);
        match Verdict::of(v1, v2.filter(|_| v1 != Some(false))) {
            Verdict::Passed => Ok(true),
            Verdict::Failed | Verdict::Unknown => Ok(false),
            Verdict::Inconsistent => Err(inconsistent_hashes(piece)),
        }
    }

    /// A downloaded piece checked against both its hashes, the v2 one even
    /// when v1 fails, so that a torrent whose hashes disagree stops
    /// (libtorrent `torrent::on_piece_verified`).
    pub fn check_piece(&self, piece_index: ValidPieceIndex) -> anyhow::Result<Verdict> {
        let mut h = self.digest(piece_index);
        if !self
            .files
            .hash_held_piece(piece_index, &mut |bytes| h.update(bytes))
        {
            self.hash_from_files(piece_index, &mut h)?;
        }

        let (v1, v2) = self.hashes_match(piece_index, h);
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

    // The piece's bytes, read from the storage's files into `h`.
    fn hash_from_files(
        &self,
        piece_index: ValidPieceIndex,
        h: &mut PieceDigest,
    ) -> anyhow::Result<()> {
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
                h,
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
        Ok(())
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
    use crate::AddTorrentOptions;
    use crate::core::v2_test_torrent::{TestTorrent, metadata, payload};
    use crate::torrent_state::ManagedTorrentShared;

    /// Each file's bytes, read only, how many bytes were read, the pieces
    /// it was told failed, ranges of files it does not hand out though the
    /// file goes on past them, as a storage that holds only some pieces, and
    /// the pieces it was told a check found.
    struct Files(
        Vec<Vec<u8>>,
        AtomicU64,
        std::sync::Mutex<Vec<u32>>,
        Vec<(usize, std::ops::Range<u64>)>,
        std::sync::Mutex<Vec<u32>>,
    );

    impl Files {
        /// `payload` laid out over the torrent's files.
        fn new(metadata: &TorrentMetadata, payload: &[u8]) -> Self {
            Files(
                metadata
                    .file_infos
                    .iter()
                    .map(|fi| {
                        let start = usize::try_from(fi.offset_in_torrent).unwrap();
                        payload[start..start + usize::try_from(fi.len).unwrap()].to_vec()
                    })
                    .collect(),
                AtomicU64::new(0),
                Default::default(),
                Vec::new(),
                Default::default(),
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
            let end = offset + buf.len() as u64;
            if self
                .3
                .iter()
                .any(|(file, hole)| *file == file_id && hole.start < end && offset < hole.end)
            {
                bail!("not in hand");
            }
            let start = usize::try_from(offset).unwrap();
            buf.copy_from_slice(
                self.0[file_id]
                    .get(start..start + buf.len())
                    .ok_or_else(|| std::io::Error::from(std::io::ErrorKind::UnexpectedEof))?,
            );
            self.1.fetch_add(buf.len() as u64, Ordering::Relaxed);
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
            Ok(Box::new(Files(
                self.0.clone(),
                AtomicU64::new(0),
                Default::default(),
                self.3.clone(),
                Default::default(),
            )))
        }

        fn on_piece_hash_failed(&self, piece: ValidPieceIndex) -> anyhow::Result<()> {
            self.2.lock().unwrap().push(piece.get());
            Ok(())
        }

        fn on_piece_checked(&self, piece: ValidPieceIndex) -> anyhow::Result<()> {
            self.4.lock().unwrap().push(piece.get());
            Ok(())
        }
    }

    /// Every piece checked as on completion, SHA-1 skipped where v2 is
    /// known when `disable_v1_hashes`.
    fn check_all(
        metadata: &TorrentMetadata,
        payload: &[u8],
        disable_v1_hashes: bool,
    ) -> Vec<Verdict> {
        let files = Files::new(metadata, payload);
        let ops = FileOps::new(metadata, &files, disable_v1_hashes);
        metadata
            .lengths()
            .iter_piece_infos()
            .map(|p| ops.check_piece(p.piece_index).unwrap())
            .collect()
    }

    fn initial_check(
        metadata: &TorrentMetadata,
        payload: &[u8],
        disable_v1_hashes: bool,
    ) -> anyhow::Result<Vec<bool>> {
        let files = Files::new(metadata, payload);
        let have = FileOps::new(metadata, &files, disable_v1_hashes).initial_check(
            4,
            &AtomicU64::new(0),
            &AtomicBool::new(false),
        )?;
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

    /// A storage over `Files` that wants the pieces it names as a check
    /// hashes them, and keeps what it was handed.
    struct Wanting {
        files: Files,
        wanted: Vec<u32>,
        hashed: std::sync::Mutex<Vec<(u32, bytes::Bytes)>>,
    }

    impl TorrentStorage for Wanting {
        fn init(
            &mut self,
            shared: &ManagedTorrentShared,
            metadata: &TorrentMetadata,
        ) -> anyhow::Result<()> {
            self.files.init(shared, metadata)
        }

        fn pread_exact(&self, file_id: usize, offset: u64, buf: &mut [u8]) -> anyhow::Result<()> {
            self.files.pread_exact(file_id, offset, buf)
        }

        fn pwrite_all(&self, file_id: usize, offset: u64, buf: &[u8]) -> anyhow::Result<()> {
            self.files.pwrite_all(file_id, offset, buf)
        }

        fn remove_file(&self, file_id: usize, filename: &Path) -> anyhow::Result<()> {
            self.files.remove_file(file_id, filename)
        }

        fn remove_directory_if_empty(&self, path: &Path) -> anyhow::Result<()> {
            self.files.remove_directory_if_empty(path)
        }

        fn ensure_file_length(&self, file_id: usize, length: u64) -> anyhow::Result<()> {
            self.files.ensure_file_length(file_id, length)
        }

        fn take(&self) -> anyhow::Result<Box<dyn TorrentStorage>> {
            self.files.take()
        }

        fn wants_hashed_piece(&self, piece: ValidPieceIndex) -> bool {
            self.wanted.contains(&piece.get())
        }

        fn on_piece_hashed(&self, piece: ValidPieceIndex, bytes: bytes::Bytes) {
            self.hashed.lock().unwrap().push((piece.get(), bytes));
        }
    }

    /// A check hands the storage each piece it wants as it hashed it, read
    /// once into one buffer: the files are read no more than for a check
    /// that wants none, and a piece the check does not find is not handed.
    #[test]
    fn a_check_hands_a_wanted_piece_over_as_it_hashed_it() {
        let t = two_files();
        let metadata = metadata(&t.torrent_bytes());
        let mut payload = t.v1_payload();
        payload[3 * 32768 + 7] ^= 1;
        let check = |wanted: Vec<u32>| {
            let storage = Wanting {
                files: Files::new(&metadata, &payload),
                wanted,
                hashed: Default::default(),
            };
            FileOps::new(&metadata, &storage, true)
                .initial_check(4, &AtomicU64::new(0), &AtomicBool::new(false))
                .unwrap();
            let read = storage.files.1.load(Ordering::Relaxed);
            let mut hashed = storage.hashed.into_inner().unwrap();
            hashed.sort_by_key(|(piece, _)| *piece);
            (read, hashed)
        };
        let (read_none, none) = check(Vec::new());
        assert!(none.is_empty());
        let (read, hashed) = check(vec![1, 3, 4]);
        assert_eq!(read, read_none);
        let lengths = metadata.lengths();
        let pieces: Vec<u32> = hashed.iter().map(|(piece, _)| *piece).collect();
        assert_eq!(pieces, [1, 4]);
        for (piece, bytes) in hashed {
            let piece = lengths.validate_piece_index(piece).unwrap();
            let start = usize::try_from(lengths.piece_offset(piece)).unwrap();
            let end = start + lengths.piece_length(piece) as usize;
            assert_eq!(&bytes[..], &payload[start..end], "piece {piece}");
        }
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
        assert_eq!(
            check_all(&metadata, &payload, true),
            vec![Verdict::Passed; 5]
        );
        assert_eq!(
            initial_check(&metadata, &payload, true).unwrap(),
            vec![true; 5]
        );

        // A byte flipped in b's third piece fails that piece alone.
        payload[3 * 32768 + 7] ^= 1;
        let verdicts = check_all(&metadata, &payload, true);
        assert_eq!(verdicts[3], Verdict::Failed);
        assert_eq!(
            verdicts.iter().filter(|v| **v == Verdict::Passed).count(),
            4
        );
        assert_eq!(
            initial_check(&metadata, &payload, true).unwrap(),
            vec![true, true, true, false, true]
        );
    }

    #[test]
    fn a_v2_piece_is_unknown_until_its_files_layer_is_held() {
        let mut t = two_files();
        t.layers = false;
        let metadata = metadata(&t.torrent_bytes());
        let payload = t.v1_payload();
        let verdicts = check_all(&metadata, &payload, true);
        assert_eq!(verdicts[0], Verdict::Passed);
        assert_eq!(verdicts[1..], [Verdict::Unknown; 4]);
        // Not held, so fetched again; libtorrent would hold it unverified.
        assert_eq!(
            initial_check(&metadata, &payload, true).unwrap(),
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
        assert_eq!(
            check_all(&metadata, &payload, true),
            vec![Verdict::Passed; 5]
        );
        assert_eq!(
            initial_check(&metadata, &payload, true).unwrap(),
            vec![true; 5]
        );
        let lengths = metadata.lengths();
        assert!((0..5).all(|p| metadata.can_verify(lengths.validate_piece_index(p).unwrap())));

        let mut payload = payload;
        payload[2 * 32768] ^= 1;
        assert_eq!(check_all(&metadata, &payload, true)[2], Verdict::Failed);
    }

    #[test]
    fn a_hybrid_with_bad_v1_hashes_is_inconsistent_once_downloaded() {
        // libtorrent test_transfer.cpp corrupt_v1_hashes: a hybrid with
        // wrong v1 hashes and right v2 hashes ends in
        // torrent_inconsistent_hashes. The check skips v2 for a piece
        // whose v1 hash fails, so there it is only missing. Both digests
        // are taken only with v1 hashes kept, libtorrent's default.
        let (real, other) = two_hybrids();
        let metadata = hybrid(&other, &real);
        let payload = real.v1_payload();
        assert_eq!(
            check_all(&metadata, &payload, false),
            vec![Verdict::Inconsistent; 5]
        );
        assert_eq!(
            initial_check(&metadata, &payload, false).unwrap(),
            vec![false; 5]
        );
    }

    #[test]
    fn a_hybrid_with_bad_v2_hashes_is_inconsistent_at_the_check() {
        // libtorrent torrent::on_piece_hashed: v1 passes, v2 fails. With
        // v1 hashes kept, libtorrent's default.
        let (real, other) = two_hybrids();
        let metadata = hybrid(&real, &other);
        let payload = real.v1_payload();
        assert_eq!(
            check_all(&metadata, &payload, false),
            vec![Verdict::Inconsistent; 5]
        );
        let err = initial_check(&metadata, &payload, false).unwrap_err();
        assert!(
            format!("{err}").contains("v1 and v2 hashes do not describe the same data"),
            "{err}"
        );
    }

    /// libtorrent test_checking.cpp's files, less the three of hundreds of
    /// pieces, in a hybrid of 16 KiB pieces.
    fn checking_torrent() -> TestTorrent {
        let sizes = [
            0, 5, 11, 16000, 17, 10, 8000, 8000, 1, 1, 1, 1, 1, 100, 1, 1, 1, 1, 100, 1, 1, 1, 1,
            1, 1, 1, 1, 1, 1, 1, 1, 13, 65000, 34, 75, 2, 30, 400, 50000, 73000, 900, 43000, 400,
            4300, 6, 4,
        ];
        let names: Vec<String> = (0..sizes.len()).map(|i| format!("f{i:02}")).collect();
        let mut t = TestTorrent::new(
            names
                .iter()
                .zip(sizes)
                .enumerate()
                .map(|(i, (name, len))| {
                    (name.as_str(), payload(len, u8::try_from(i & 0xff).unwrap()))
                })
                .collect(),
            16384,
        );
        t.hybrid = true;
        t
    }

    #[test]
    fn every_piece_is_read_and_hashed_once_on_any_number_of_threads() {
        // libtorrent test_checking.cpp `checking`, on 1 to 64 hashing
        // threads (settings_pack `hashing_threads`).
        let t = checking_torrent();
        let metadata = metadata(&t.torrent_bytes());
        let payload = t.v1_payload();
        let lengths = metadata.lengths();
        let data: u64 = t.files.iter().map(|(_, d)| d.len() as u64).sum();
        for threads in [1, 2, 4, 64] {
            let files = Files::new(&metadata, &payload);
            let progress = AtomicU64::new(0);
            let have = FileOps::new(&metadata, &files, true)
                .initial_check(threads, &progress, &AtomicBool::new(false))
                .unwrap();
            assert!(
                have.iter()
                    .by_vals()
                    .take(lengths.total_pieces() as usize)
                    .all(|b| b),
                "{threads}"
            );
            assert_eq!(files.1.load(Ordering::Relaxed), data, "{threads}");
            assert_eq!(progress.load(Ordering::Relaxed), lengths.total_length());
        }
    }

    #[test]
    fn a_short_or_missing_file_leaves_its_pieces_needed_on_any_number_of_threads() {
        // libtorrent test_checking.cpp `incomplete`: every file but each
        // third cut to half its length, or to nothing. The storage is told
        // each piece the check did not find, so it holds none of them, and
        // each it found.
        let t = checking_torrent();
        let metadata = metadata(&t.torrent_bytes());
        let payload = t.v1_payload();
        let cut = |i: usize, len: usize| match i % 3 {
            2 => len,
            n => len * n / 2,
        };
        let mut files = Files::new(&metadata, &payload);
        for (fi, data) in metadata.file_infos.iter().zip(files.0.iter_mut()) {
            if let Some(i) = t
                .files
                .iter()
                .position(|(n, _)| fi.relative_filename == Path::new(n))
            {
                data.truncate(cut(i, data.len()));
            }
        }
        // A piece is there when every byte of it in a file is.
        let lengths = metadata.lengths();
        let expected: Vec<bool> = lengths
            .iter_piece_infos()
            .map(|p| {
                let start = lengths.piece_offset(p.piece_index);
                let end = start + u64::from(p.len);
                metadata
                    .file_infos
                    .iter()
                    .zip(&files.0)
                    .filter(|(fi, _)| !fi.attrs.padding && fi.len > 0)
                    .filter(|(fi, _)| {
                        fi.offset_in_torrent < end && fi.offset_in_torrent + fi.len > start
                    })
                    .all(|(fi, data)| {
                        end.min(fi.offset_in_torrent + fi.len) - fi.offset_in_torrent
                            <= data.len() as u64
                    })
            })
            .collect();
        assert!(expected.contains(&true) && expected.contains(&false));
        for threads in [1, 4, 64] {
            let have = FileOps::new(&metadata, &files, true)
                .initial_check(threads, &AtomicU64::new(0), &AtomicBool::new(false))
                .unwrap();
            let have: Vec<bool> = have.iter().by_vals().take(expected.len()).collect();
            assert_eq!(have, expected, "{threads}");
            let mut failed = std::mem::take(&mut *files.2.lock().unwrap());
            failed.sort_unstable();
            let missing: Vec<u32> = (0..u32::try_from(expected.len()).unwrap())
                .filter(|piece| !expected[*piece as usize])
                .collect();
            assert_eq!(failed, missing, "{threads}");
            let mut found = std::mem::take(&mut *files.4.lock().unwrap());
            found.sort_unstable();
            let had: Vec<u32> = (0..u32::try_from(expected.len()).unwrap())
                .filter(|piece| expected[*piece as usize])
                .collect();
            assert_eq!(found, had, "{threads}");
        }
    }

    #[test]
    fn a_range_the_storage_does_not_hand_out_fails_only_its_pieces() {
        // libtorrent `torrent::on_piece_hashed` skips the rest of a file
        // only when the file ends; a storage that holds some pieces of a
        // file and not others has every piece it holds checked.
        let t = checking_torrent();
        let metadata = metadata(&t.torrent_bytes());
        let mut files = Files::new(&metadata, &t.v1_payload());
        let (file, fi) = metadata
            .file_infos
            .iter()
            .enumerate()
            .find(|(_, fi)| fi.relative_filename == Path::new("f39"))
            .unwrap();
        let hole = 20000..20100;
        files.3.push((file, hole.clone()));
        let lengths = metadata.lengths();
        let expected: Vec<bool> = lengths
            .iter_piece_infos()
            .map(|p| {
                let start = lengths.piece_offset(p.piece_index);
                let end = start + u64::from(p.len);
                !(fi.offset_in_torrent + hole.start < end
                    && start < fi.offset_in_torrent + hole.end)
            })
            .collect();
        assert_eq!(expected.iter().filter(|had| !**had).count(), 1);
        // The file goes on past the hole for whole pieces, which the check
        // still finds.
        assert!(lengths.iter_piece_infos().any(|p| {
            let start = lengths.piece_offset(p.piece_index);
            start >= fi.offset_in_torrent + hole.end
                && start + u64::from(p.len) <= fi.offset_in_torrent + fi.len
        }));
        for threads in [1, 4, 64] {
            let have = FileOps::new(&metadata, &files, true)
                .initial_check(threads, &AtomicU64::new(0), &AtomicBool::new(false))
                .unwrap();
            let have: Vec<bool> = have.iter().by_vals().take(expected.len()).collect();
            assert_eq!(have, expected, "{threads}");
            let failed = std::mem::take(&mut *files.2.lock().unwrap());
            assert_eq!(failed.len(), 1, "{threads}");
        }
    }

    #[test]
    fn a_paused_check_ends_without_a_bitfield() {
        let t = checking_torrent();
        let metadata = metadata(&t.torrent_bytes());
        let files = Files::new(&metadata, &t.v1_payload());
        let err = FileOps::new(&metadata, &files, true)
            .initial_check(4, &AtomicU64::new(0), &AtomicBool::new(true))
            .unwrap_err();
        assert!(format!("{err}").contains("initial check paused"), "{err}");
        assert_eq!(files.1.load(Ordering::Relaxed), 0);
    }

    /// Which pieces of `metadata` get a SHA-1 digest.
    fn sha1_pieces(metadata: &TorrentMetadata, disable_v1_hashes: bool) -> Vec<bool> {
        let files = Files(
            Vec::new(),
            AtomicU64::new(0),
            Default::default(),
            Vec::new(),
            Default::default(),
        );
        let ops = FileOps::new(metadata, &files, disable_v1_hashes);
        metadata
            .lengths()
            .iter_piece_infos()
            .map(|p| ops.digest(p.piece_index).v1.is_some())
            .collect()
    }

    #[test]
    fn a_hybrid_piece_whose_v2_hashes_are_known_gets_no_sha1() {
        // libtorrent `torrent_flags::disable_v1_hashes`: a hybrid verified
        // by v2 alone, so wrong v1 hashes go unread (test_transfer.cpp
        // corrupt_v1_hashes passes with the flag set).
        let (real, other) = two_hybrids();
        let metadata = metadata(&real.torrent_bytes());
        assert_eq!(sha1_pieces(&metadata, true), vec![false; 5]);
        assert_eq!(sha1_pieces(&metadata, false), vec![true; 5]);
        assert!(AddTorrentOptions::default().disable_v1_hashes);

        let bad_v1 = hybrid(&other, &real);
        let payload = real.v1_payload();
        assert_eq!(check_all(&bad_v1, &payload, true), vec![Verdict::Passed; 5]);
        assert_eq!(
            initial_check(&bad_v1, &payload, true).unwrap(),
            vec![true; 5]
        );
    }

    #[test]
    fn a_hybrid_piece_whose_v2_hashes_are_not_known_still_gets_sha1() {
        // Without its layer, file b's pieces are verified by v1; file a,
        // one piece, is known by its root.
        let (mut real, _) = two_hybrids();
        real.layers = false;
        let metadata = metadata(&real.torrent_bytes());
        assert_eq!(
            sha1_pieces(&metadata, true),
            vec![false, true, true, true, true]
        );
        let payload = real.v1_payload();
        assert_eq!(
            check_all(&metadata, &payload, true),
            vec![Verdict::Passed; 5]
        );
    }

    #[test]
    fn a_v1_piece_always_gets_sha1() {
        let data = payload(40000, 7);
        let pieces: Vec<u8> = data
            .chunks(16384)
            .flat_map(|piece| {
                let mut h = Sha1::new();
                h.update(piece);
                h.finish()
            })
            .collect();
        let mut torrent =
            b"d4:infod6:lengthi40000e4:name1:x12:piece lengthi16384e6:pieces60:".to_vec();
        torrent.extend(pieces);
        torrent.extend(b"ee");
        let metadata = metadata(&torrent);
        assert_eq!(sha1_pieces(&metadata, true), vec![true; 3]);
        assert_eq!(check_all(&metadata, &data, true), vec![Verdict::Passed; 3]);
    }
}
