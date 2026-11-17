// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver
//
// Derived from rqbit (https://github.com/ikatson/rqbit), Copyright 2021 Igor Katson,
// licensed under the Apache License 2.0 (see LICENSE-APACHE). Modified by the
// Weaver authors from 2026-10-20 onward.

use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use crate::buffers::{ByteBuf, ByteBufOwned};
use crate::core::{
    Id32,
    lengths::{ChunkInfo, ValidPieceIndex},
    merkle::BLOCK_SIZE,
    torrent_metainfo::ValidatedTorrentMetaV1Info,
};
use crate::peer_protocol::{DoubleBufHelper, Piece};
use crate::session_stats::check::CHECK_STATS;
use crate::sha1w::{ISha1, ISha256, Sha1, Sha256};
use crate::torrent_state::TorrentMetadata;
use anyhow::Context;
use parking_lot::{Condvar, Mutex};
use tracing::{debug, trace, warn};

use crate::{
    file_info::FileInfo,
    storage::{FilesShut, ReadTurns, TorrentStorage},
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

/// Where a paused initial check stopped: every piece before `next` was
/// checked and the storage heard its verdict; `have` holds those found.
#[derive(Debug)]
pub(crate) struct CheckedSoFar {
    pub(crate) next: usize,
    pub(crate) have: BF,
}

/// How an initial check ended: every piece checked, or paused.
#[derive(Debug)]
pub(crate) enum CheckOutcome {
    Done(BF),
    Paused(CheckedSoFar),
}

/// The share of a piece one file holds.
struct Span<'a> {
    file: usize,
    info: &'a FileInfo,
    /// Where the share starts in the file.
    at: u64,
    /// Where the share starts in the piece.
    from: usize,
    len: usize,
}

/// The read side of a check: the pieces of one torrent's files, read a
/// slice at a read. Asked for the pieces in piece order, and the slices of
/// a piece in order, it reads every file front to back, each read starting
/// where the one before it ended, so a spinning disk is read without a
/// seek (libtorrent issues a check's hash jobs in piece order to its one
/// hashing thread, torrent.cpp:2524-2546 and settings_pack.cpp:353,
/// v2.0.15; it maps the file and leaves the size of a read to the kernel's
/// read-ahead, where the read here is the slice).
pub(crate) struct PieceReader<'o, 'a> {
    ops: &'o FileOps<'a>,
    // Where each file was found to end: nothing at or past it is read.
    broken: Vec<u64>,
    // Where the last read of each file ended.
    ends: Vec<u64>,
    // The offset in the torrent up to which the storage was told what is
    // about to be read, and how far past a read it is told.
    told: u64,
    told_ahead: u64,
    // The read turns of the volume the files are on: each read takes one
    // for as long as it reads, never while it waits for a hasher.
    turns: &'o ReadTurns,
}

/// How far past the byte it reads a check tells the storage what it reads
/// next ([`TorrentStorage::will_read`]): the system fetches that much of
/// the files ahead of the one reader, which then waits on the disk for
/// little of it. Far more than a read, so a device that serves several
/// requests at once has several; the same bytes in the same order, so one
/// that serves one still reads front to back.
pub(crate) const CHECK_TOLD_AHEAD: u64 = 8 << 20;

impl<'o, 'a> PieceReader<'o, 'a> {
    /// A reader that tells the storage what it reads next `told_ahead`
    /// bytes ahead, none when that is nothing, and reads in a turn of
    /// `turns`.
    pub(crate) fn new(ops: &'o FileOps<'a>, told_ahead: u64, turns: &'o ReadTurns) -> Self {
        Self {
            ops,
            broken: vec![u64::MAX; ops.file_infos.len()],
            ends: vec![0; ops.file_infos.len()],
            told: 0,
            told_ahead,
            turns,
        }
    }

    /// Whether the data of `piece` from `from` on is there, read into
    /// `into`: the whole piece, or a slice of one too large to hold. A
    /// file that ends at some offset, or is not there at all, is broken
    /// from there and not read there again: every piece of it from there
    /// on is needed. Any other miss fails that piece alone (libtorrent
    /// `torrent::on_piece_hashed` skips the rest of a file only on eof, a
    /// missing file or a file too short), but for a read refused because
    /// the files are shut ([`FilesShut`]), which is an error: it ends the
    /// check.
    pub(crate) fn read(
        &mut self,
        piece: ValidPieceIndex,
        from: usize,
        into: &mut [u8],
    ) -> anyhow::Result<bool> {
        self.tell(piece, from);
        let turns = self.turns;
        let _turn = turns.wait_here();
        let mut filled = 0;
        for span in self.ops.piece_spans(piece, from, into.len()) {
            if span.at >= self.broken[span.file] {
                return Ok(false);
            }
            let share = into
                .get_mut(span.from..span.from + span.len)
                .context("broken torrent metadata")?;
            if let Err(err) = self.ops.read_span(&span, share, self.ends[span.file]) {
                debug!(
                    "error reading from file {} ({:?}) at {}: {err:#}",
                    span.file, span.info.relative_filename, span.at
                );
                // Files shut to reads say nothing of the piece: the check
                // ends here, and finds no piece missing on a read it was
                // refused.
                if FilesShut::refused_in(&err) {
                    return Err(err);
                }
                if ends_the_file(&err) {
                    self.broken[span.file] = self.broken[span.file].min(span.at);
                }
                trace!("piece {piece} had errors, marking as needed");
                return Ok(false);
            }
            self.ends[span.file] = span.at + span.len as u64;
            filled += span.len;
        }
        anyhow::ensure!(filled == into.len(), "broken torrent metadata");
        Ok(true)
    }
}

impl PieceReader<'_, '_> {
    // The storage is told what the reads from `from` of `piece` on are
    // about to read, `told_ahead` bytes of the torrent's files in order,
    // each time the reader comes within half of that of what it told.
    fn tell(&mut self, piece: ValidPieceIndex, from: usize) {
        if self.told_ahead == 0 {
            return;
        }
        let at = self.ops.torrent.lengths().piece_offset(piece) + from as u64;
        if self.told >= at + self.told_ahead / 2 {
            return;
        }
        let start = self.told.max(at);
        let end = at + self.told_ahead;
        let infos = self.ops.file_infos;
        let first = infos.partition_point(|fi| fi.offset_in_torrent + fi.len <= start);
        for (file, info) in infos.iter().enumerate().skip(first) {
            if info.offset_in_torrent >= end {
                break;
            }
            if info.len == 0 || info.attrs.padding {
                continue;
            }
            let from = start.max(info.offset_in_torrent) - info.offset_in_torrent;
            let to = end.min(info.offset_in_torrent + info.len) - info.offset_in_torrent;
            if from < self.broken[file] {
                self.ops.files.will_read(file, from, to - from);
            }
        }
        self.told = end;
    }
}

/// A piece on its way from a check's reader to the one hasher that takes
/// it: its slices in the order they were read, and how the read ended.
struct Stream {
    piece: ValidPieceIndex,
    // The storage wants the bytes if the piece passes: the piece is one
    // slice, in a buffer of its own.
    wanted: bool,
    slices: Mutex<Slices>,
    // Rung when a slice joins, and when the read ended.
    more: Condvar,
}

#[derive(Default)]
struct Slices {
    // What the reader hashed of the piece before it handed it on, and the
    // time that took.
    begun: Option<(PieceDigest, Duration)>,
    read: VecDeque<Vec<u8>>,
    // Set once nothing more joins: whether the whole piece was there.
    end: Option<anyhow::Result<bool>>,
}

impl Stream {
    fn end(&self, end: anyhow::Result<bool>) {
        self.slices.lock().end = Some(end);
        self.more.notify_all();
    }
}

/// The whole piece a connection uploading from a volume that reads whole
/// pieces (a spinning disk) read last: the blocks the peer asks of it are
/// copied out of it, the piece read once, in one go, not a block at a time
/// between other readers' reads. One piece of memory per such connection,
/// kept for as long as it lasts and sized before each read.
#[derive(Default)]
pub struct HeldPiece {
    piece: Option<ValidPieceIndex>,
    bytes: Vec<u8>,
}

/// The memory a check reads into.
///
/// `slice` is the most one read buffer holds, and the unit the reader
/// hands a hasher: a piece up to it is read in one read, a larger one in
/// slices of it, in order, each hashed as it comes, by the one hasher
/// that took the piece.
///
/// `ahead` is the most the buffers of a torrent's check hold together:
/// the slices read and waiting for a hasher, those being hashed, and the
/// buffers kept to read into again. The reader waits for a hasher to be
/// done with a buffer before it takes one past this, so a check holds at
/// most `ahead` and one slice. 64 MiB lets four threads each hash a
/// 16 MiB piece at once; at 1 MiB pieces a check on four threads holds
/// 7 MiB, a piece waiting for each hasher. libtorrent holds 4 MiB of
/// blocks for its one hashing thread (`checking_mem_usage`, 256 blocks of
/// 16 KiB, settings_pack.cpp:354, v2.0.15); the rest here is what hashing
/// on several threads holds.
///
/// 256 KiB was the fastest slice measured on a hot cache (64 KiB, 256 KiB,
/// 1 MiB and 16 MiB tried at 1 MiB and 16 MiB pieces): it is hashed from
/// the processor's cache, and a hasher is handed 4 of them per MiB.
///
/// One piece is held past this: a piece the storage wants as hashed is
/// handed to it whole, so it is read whole, one at a time, into a buffer
/// of its own, however large.
///
/// The check of a single piece (a seed-mode piece a peer asks for, a
/// resumed torrent's pieces) holds one buffer of at most `slice`.
#[derive(Clone, Copy)]
pub(crate) struct CheckMemory {
    pub(crate) slice: usize,
    pub(crate) ahead: usize,
}

pub(crate) const CHECK_MEMORY: CheckMemory = CheckMemory {
    slice: 256 << 10,
    ahead: 64 << 20,
};

/// What a check's reader and its hashers share: the pieces read, or being
/// read, that no hasher took yet, and the buffers the hashers are done
/// with. The reader runs at most `most` pieces ahead of the hashers, one
/// for each hasher, and no further than [`CheckMemory`] allows.
struct ReadAhead {
    line: Mutex<Line>,
    // Rung when a piece joins the line, and when the reader ends.
    read: Condvar,
    // Rung when a hasher is done with a buffer.
    room: Condvar,
    // The hashers waiting for a piece.
    idle: AtomicUsize,
    most: usize,
    memory: CheckMemory,
    // A hasher met an error: the reader reads no further.
    failed: AtomicBool,
}

#[derive(Default)]
struct Line {
    pieces: VecDeque<Arc<Stream>>,
    spare: Vec<Vec<u8>>,
    ended: bool,
    // The bytes of every buffer the check holds: on their way to a
    // hasher, with a hasher, in the reader's hand, and spare.
    held: usize,
    // The most `held` has been.
    most_held: usize,
}

impl Line {
    fn hold(&mut self, bytes: usize) {
        self.held += bytes;
        self.most_held = self.most_held.max(self.held);
    }

    // The check is done with a buffer: kept to read into again while the
    // check holds no more than it may, and let go otherwise.
    fn spare(&mut self, bytes: Vec<u8>, memory: CheckMemory) {
        if self.held <= memory.ahead {
            self.spare.push(bytes);
        } else {
            self.held -= bytes.capacity();
        }
    }
}

impl ReadAhead {
    // Whether the reader may hold `len` bytes more now. It may not while
    // that takes the check past what it may hold and a hasher has a
    // buffer to be done with; the reader holds none when it asks.
    fn has_room(&self, line: &Line, len: usize) -> bool {
        self.most == 0 || line.held == 0 || line.held + len <= self.memory.ahead
    }

    // A buffer of `len` bytes to read into: one the check is done with,
    // or a new one. A spare buffer too short for `len` is let go, not
    // grown, so a new buffer is always asked room for. A buffer for a
    // hasher is waited for while the check holds all it may; the one the
    // reader hashes from is not.
    fn buffer(&self, len: usize, for_hasher: bool) -> Vec<u8> {
        let mut line = self.line.lock();
        let mut bytes = loop {
            if let Some(bytes) = line.spare.pop() {
                if bytes.capacity() >= len {
                    break bytes;
                }
                line.held -= bytes.capacity();
                continue;
            }
            if !for_hasher || self.has_room(&line, len) {
                let bytes = Vec::with_capacity(len);
                line.hold(bytes.capacity());
                break bytes;
            }
            self.room.wait(&mut line);
        };
        drop(line);
        bytes.resize(len, 0);
        bytes
    }

    // A buffer of `len` bytes that goes to the storage: a new one, in the
    // place of a spare buffer, which is let go, and of as many more as the
    // check would otherwise hold more than it may. So the check holds no
    // more buffers for the pieces the storage wants than for the rest.
    fn own_buffer(&self, len: usize) -> Vec<u8> {
        let mut line = self.line.lock();
        loop {
            if let Some(spare) = line.spare.pop() {
                line.held -= spare.capacity();
            }
            while line.held + len > self.memory.ahead {
                let Some(spare) = line.spare.pop() else {
                    break;
                };
                line.held -= spare.capacity();
            }
            if self.has_room(&line, len) {
                break;
            }
            self.room.wait(&mut line);
        }
        line.hold(len);
        drop(line);
        vec![0u8; len]
    }

    // A buffer the check is done with.
    fn done_with(&self, bytes: Vec<u8>) {
        self.line.lock().spare(bytes, self.memory);
        self.room.notify_one();
    }

    // A buffer of `len` bytes went to the storage, or was dropped.
    fn gone(&self, len: usize) {
        self.line.lock().held -= len;
        self.room.notify_one();
    }

    // Whether a hasher waits with no piece to take.
    fn hungry(&self) -> bool {
        self.idle.load(Ordering::Relaxed) > 0 && self.line.lock().pieces.is_empty()
    }

    // `piece`, of `len` bytes, joins the line for a hasher to take while it
    // is read, unless as many wait as may or the check has no room for it.
    // With `begun`, what the reader hashed of it, it joins whatever waits.
    fn open(
        &self,
        piece: ValidPieceIndex,
        len: usize,
        begun: Option<(PieceDigest, Duration)>,
    ) -> Option<Arc<Stream>> {
        let mut line = self.line.lock();
        if begun.is_none() && (line.pieces.len() >= self.most || !self.has_room(&line, len)) {
            return None;
        }
        let stream = Arc::new(Stream {
            piece,
            wanted: false,
            slices: Mutex::new(Slices {
                begun,
                ..Default::default()
            }),
            more: Condvar::new(),
        });
        line.pieces.push_back(stream.clone());
        drop(line);
        self.read.notify_one();
        Some(stream)
    }

    // The piece the storage wants, read whole into `bytes`, joins the line
    // for a hasher, unless as many wait as may: true when it went, `bytes`
    // left empty.
    fn hand_on(&self, piece: ValidPieceIndex, bytes: &mut Vec<u8>) -> bool {
        let mut line = self.line.lock();
        if line.pieces.len() >= self.most {
            return false;
        }
        line.pieces.push_back(Arc::new(Stream {
            piece,
            wanted: true,
            slices: Mutex::new(Slices {
                begun: None,
                read: VecDeque::from([std::mem::take(bytes)]),
                end: Some(Ok(true)),
            }),
            more: Condvar::new(),
        }));
        drop(line);
        self.read.notify_one();
        true
    }
}

/// How the reader's own hashing of a piece ended: with its verdict, or
/// with the piece handed to a hasher from an offset on, and what was
/// hashed of it.
enum Hashed {
    Here(bool),
    HandedOn(usize, Box<(PieceDigest, Duration)>),
}

type Checked = Vec<(ValidPieceIndex, anyhow::Result<bool>)>;

pub(crate) struct FileOps<'a> {
    metadata: &'a TorrentMetadata,
    torrent: &'a ValidatedTorrentMetaV1Info<ByteBufOwned>,
    files: &'a dyn TorrentStorage,
    file_infos: &'a FileInfos,
    disable_v1_hashes: bool,
}

impl<'a> FileOps<'a> {
    // Checks, in debug builds and tests, that a storage that reaches the
    // disk is not reached from a uTP thread.
    #[track_caller]
    fn on_disk(&self) {
        if !self.files.writes_to_memory() {
            crate::spawn_utils::no_block_on_utp_thread();
        }
    }

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

    /// [`Self::initial_check_from`] from the first piece, a pause an error.
    #[cfg(test)]
    pub fn initial_check(
        &self,
        threads: usize,
        progress: &AtomicU64,
        pause_requested: &AtomicBool,
    ) -> anyhow::Result<BF> {
        match self.initial_check_from(threads, progress, pause_requested, None)? {
            CheckOutcome::Done(have) => Ok(have),
            CheckOutcome::Paused(_) => anyhow::bail!("initial check paused"),
        }
    }

    /// The pieces whose data is already there. One thread, the caller's,
    /// reads the pieces in piece order, a slice at a read; the other
    /// `threads - 1` hash what it read, a piece on one of them, slice by
    /// slice as it is read (libtorrent `torrent::start_checking` issues a
    /// hash job per piece in piece order, torrent.cpp:2524-2546, v2.0.15,
    /// and keeps a few outstanding, :2497-2504). The reader runs at most a
    /// piece per hasher ahead, within the memory [`CHECK_MEMORY`] allows;
    /// past that, and with no thread but its own, it hashes what it reads
    /// itself, until a hasher has nothing to take. The verdicts
    /// land in the bitfield in piece order, the first error by piece ending
    /// the check; pausing stops the reader at the next piece, every piece
    /// read is still hashed, and the check comes back as where it stopped.
    /// When `from` is given the check goes on from where a paused one
    /// stopped: the pieces before it are not read again and keep their
    /// verdicts (libtorrent `torrent::start_checking` goes on from
    /// `m_checking_piece`, which a pause leaves, torrent.cpp:2697-2712,
    /// v2.0.15).
    pub fn initial_check_from(
        &self,
        threads: usize,
        progress: &AtomicU64,
        pause_requested: &AtomicBool,
        from: Option<CheckedSoFar>,
    ) -> anyhow::Result<CheckOutcome> {
        self.check_within(
            self.check_memory(),
            threads,
            progress,
            pause_requested,
            from,
        )
        .0
    }

    /// The memory a check of these files reads into: [`CHECK_MEMORY`], a
    /// whole piece at a read on a volume that reads whole pieces (a
    /// spinning disk), so its one reader reads each piece in one go.
    fn check_memory(&self) -> CheckMemory {
        if self.files.read_turns().whole_pieces() {
            let piece = self.torrent.lengths().default_piece_length() as usize;
            CheckMemory {
                slice: piece.max(CHECK_MEMORY.slice),
                ..CHECK_MEMORY
            }
        } else {
            CHECK_MEMORY
        }
    }

    /// [`Self::initial_check_from`] within `memory`, and the most bytes its
    /// buffers held at once.
    fn check_within(
        &self,
        memory: CheckMemory,
        threads: usize,
        progress: &AtomicU64,
        pause_requested: &AtomicBool,
        from: Option<CheckedSoFar>,
    ) -> (anyhow::Result<CheckOutcome>, usize) {
        let mut ahead = ReadAhead {
            line: Mutex::default(),
            read: Condvar::new(),
            room: Condvar::new(),
            idle: AtomicUsize::new(0),
            most: 0,
            memory,
            failed: AtomicBool::new(false),
        };
        let outcome = self.check_through(&mut ahead, threads, progress, pause_requested, from);
        (outcome, ahead.line.into_inner().most_held)
    }

    fn check_through(
        &self,
        ahead: &mut ReadAhead,
        threads: usize,
        progress: &AtomicU64,
        pause_requested: &AtomicBool,
        from: Option<CheckedSoFar>,
    ) -> anyhow::Result<CheckOutcome> {
        let lengths = self.torrent.lengths();
        let total = lengths.total_pieces() as usize;
        let (start, mut have_pieces) = match from {
            Some(CheckedSoFar { next, have }) => (next.min(total), have),
            None => (
                0,
                BF::from_boxed_slice(vec![0u8; lengths.piece_bitfield_bytes()].into()),
            ),
        };
        let hashers = threads.clamp(1, (total - start).max(1)) - 1;
        ahead.most = hashers;
        let ahead = &*ahead;
        let mut checked = std::thread::scope(|s| {
            let others: Vec<_> = (0..hashers)
                .map(|_| s.spawn(|| self.hash_as_read(ahead)))
                .collect();
            let mut checked = self.read_in_order(start..total, ahead, progress, pause_requested);
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
        // Every piece read is checked before a thread stops, so the pieces
        // checked are those from `start` up to `done`.
        let done = start + checked.len();
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
            return Ok(CheckOutcome::Paused(CheckedSoFar {
                next: done,
                have: have_pieces,
            }));
        }
        Ok(CheckOutcome::Done(have_pieces))
    }

    /// The reader of a check: `pieces` read in order on the calling thread,
    /// a slice at a read. A piece goes to the hashers through `ahead`,
    /// slice by slice as it is read, or is hashed here when as many wait
    /// as may. Returns the verdicts reached here: those of the pieces
    /// hashed here.
    fn read_in_order(
        &self,
        pieces: std::ops::Range<usize>,
        ahead: &ReadAhead,
        progress: &AtomicU64,
        pause_requested: &AtomicBool,
    ) -> Checked {
        // The hashers leave once the line is empty and the reader ended,
        // however it ended.
        struct Ended<'r>(&'r ReadAhead);
        impl Drop for Ended<'_> {
            fn drop(&mut self) {
                self.0.line.lock().ended = true;
                self.0.read.notify_all();
            }
        }
        let _ended = Ended(ahead);
        let memory = ahead.memory;
        let lengths = self.torrent.lengths();
        let turns = self.files.read_turns();
        let mut reader = PieceReader::new(self, CHECK_TOLD_AHEAD, &turns);
        let mut checked = Vec::new();
        for index in pieces {
            if pause_requested.load(Ordering::Relaxed) || ahead.failed.load(Ordering::Relaxed) {
                break;
            }
            let Some(piece) = u32::try_from(index)
                .ok()
                .and_then(|p| lengths.validate_piece_index(p))
            else {
                break;
            };
            let len = lengths.piece_length(piece) as usize;
            progress.fetch_add(len as u64, Ordering::Relaxed);
            // A piece the storage wants as hashed is handed to it in the
            // buffer it was read into, so that buffer is its own, and
            // whole.
            let had = if self.files.wants_hashed_piece(piece) {
                let mut bytes = ahead.own_buffer(len);
                let had = match reader.read(piece, 0, &mut bytes) {
                    Ok(true) if ahead.hand_on(piece, &mut bytes) => continue,
                    Ok(true) => {
                        let started = Instant::now();
                        let mut digest = self.digest(piece);
                        digest.update(&bytes);
                        let had = self.had(piece, digest);
                        CHECK_STATS.hashed(started.elapsed());
                        if matches!(had, Ok(true)) {
                            self.files.on_piece_hashed(piece, bytes.into());
                        }
                        had
                    }
                    read => read,
                };
                ahead.gone(len);
                had
            } else if let Some(stream) = ahead.open(piece, len, None) {
                self.read_to(&mut reader, &stream, 0, len, ahead);
                continue;
            } else {
                let mut bytes = ahead.buffer(len.min(memory.slice), false);
                let had = self.hash_in_slices(&mut reader, piece, &mut bytes, ahead);
                ahead.done_with(bytes);
                match had {
                    Ok(Hashed::Here(had)) => Ok(had),
                    Ok(Hashed::HandedOn(from, begun)) => {
                        if let Some(stream) = ahead.open(piece, len, Some(*begun)) {
                            self.read_to(&mut reader, &stream, from, len, ahead);
                        }
                        continue;
                    }
                    Err(err) => Err(err),
                }
            };
            let stop = had.is_err();
            checked.push((piece, had));
            if stop {
                break;
            }
        }
        checked
    }

    /// The bytes of the piece of `stream` from `from` up to `len` read
    /// through `reader`, a slice at a read, each handed to the piece's
    /// hasher as it is read.
    fn read_to(
        &self,
        reader: &mut PieceReader<'_, 'a>,
        stream: &Stream,
        mut from: usize,
        len: usize,
        ahead: &ReadAhead,
    ) {
        // The piece's hasher is not left waiting, however the read ends.
        struct Ended<'s>(&'s Stream);
        impl Drop for Ended<'_> {
            fn drop(&mut self) {
                let mut slices = self.0.slices.lock();
                if slices.end.is_none() {
                    slices.end = Some(Ok(false));
                }
                drop(slices);
                self.0.more.notify_all();
            }
        }
        let _ended = Ended(stream);
        while from < len {
            let mut bytes = ahead.buffer((len - from).min(ahead.memory.slice), true);
            match reader.read(stream.piece, from, &mut bytes) {
                Ok(true) => {
                    from += bytes.len();
                    stream.slices.lock().read.push_back(bytes);
                    stream.more.notify_one();
                }
                read => {
                    ahead.done_with(bytes);
                    stream.end(read);
                    return;
                }
            }
        }
        stream.end(Ok(true));
    }

    /// A hasher of a check: the pieces the reader read, taken from `ahead`
    /// as they come, until the reader ended and none waits.
    fn hash_as_read(&self, ahead: &ReadAhead) -> Checked {
        let mut checked = Vec::new();
        loop {
            let stream = {
                let mut line = ahead.line.lock();
                loop {
                    if let Some(stream) = line.pieces.pop_front() {
                        break Some(stream);
                    }
                    if line.ended {
                        break None;
                    }
                    ahead.idle.fetch_add(1, Ordering::Relaxed);
                    ahead.read.wait(&mut line);
                    ahead.idle.fetch_sub(1, Ordering::Relaxed);
                }
            };
            let Some(stream) = stream else {
                break;
            };
            let had = self.hash_read(&stream, ahead);
            if had.is_err() {
                ahead.failed.store(true, Ordering::Relaxed);
            }
            checked.push((stream.piece, had));
        }
        checked
    }

    /// Whether the piece of `stream` is the torrent's: its slices hashed
    /// in order as the reader hands them on. One the storage wanted goes
    /// to it in its buffer if it passes; any other buffer goes back to be
    /// read into again.
    fn hash_read(&self, stream: &Stream, ahead: &ReadAhead) -> anyhow::Result<bool> {
        let begun = stream.slices.lock().begun.take();
        let (mut digest, mut hashing) =
            begun.unwrap_or_else(|| (self.digest(stream.piece), Duration::ZERO));
        let mut kept = None;
        let end = loop {
            let mut read = {
                let mut slices = stream.slices.lock();
                loop {
                    if !slices.read.is_empty() {
                        break std::mem::take(&mut slices.read);
                    }
                    if slices.end.is_some() {
                        break VecDeque::new();
                    }
                    stream.more.wait(&mut slices);
                }
            };
            if read.is_empty() {
                break stream.slices.lock().end.take().unwrap_or(Ok(false));
            }
            let started = Instant::now();
            for slice in &read {
                digest.update(slice);
            }
            hashing += started.elapsed();
            if stream.wanted {
                kept = read.pop_front();
                continue;
            }
            let mut line = ahead.line.lock();
            for slice in read {
                line.spare(slice, ahead.memory);
            }
            drop(line);
            ahead.room.notify_one();
        };
        let had = match end {
            Ok(true) => {
                let started = Instant::now();
                let had = self.had(stream.piece, digest);
                CHECK_STATS.hashed(hashing + started.elapsed());
                had
            }
            end => end,
        };
        if let Some(bytes) = kept {
            let len = bytes.len();
            if matches!(had, Ok(true)) {
                self.files.on_piece_hashed(stream.piece, bytes.into());
            }
            ahead.gone(len);
        }
        had
    }

    /// Whether `piece` is the torrent's: read through `reader` a slice of
    /// `into` at a time, in order, and hashed here as it is read, until a
    /// hasher waits with nothing to take: the piece is then that hasher's
    /// from the slice reached, with what was hashed of it here.
    fn hash_in_slices(
        &self,
        reader: &mut PieceReader<'_, 'a>,
        piece: ValidPieceIndex,
        into: &mut [u8],
        ahead: &ReadAhead,
    ) -> anyhow::Result<Hashed> {
        let len = self.torrent.lengths().piece_length(piece) as usize;
        let mut digest = self.digest(piece);
        let mut hashing = Duration::ZERO;
        let mut from = 0;
        while from < len {
            if ahead.hungry() {
                return Ok(Hashed::HandedOn(from, Box::new((digest, hashing))));
            }
            let most = into.len();
            let slice = &mut into[..(len - from).min(most)];
            if !reader.read(piece, from, slice)? {
                return Ok(Hashed::Here(false));
            }
            let started = Instant::now();
            digest.update(slice);
            hashing += started.elapsed();
            from += slice.len();
        }
        let started = Instant::now();
        let had = self.had(piece, digest);
        CHECK_STATS.hashed(hashing + started.elapsed());
        had.map(Hashed::Here)
    }

    /// Whether the piece `digest` hashed is the torrent's, for a check of
    /// what is on disk.
    fn had(&self, piece: ValidPieceIndex, digest: PieceDigest) -> anyhow::Result<bool> {
        // A piece whose v1 hash fails is not checked against v2: its data
        // is most likely missing (libtorrent `torrent::on_piece_hashed`).
        let (v1, v2) = self.hashes_match(piece, digest);
        match Verdict::of(v1, v2.filter(|_| v1 != Some(false))) {
            Verdict::Passed => Ok(true),
            Verdict::Failed | Verdict::Unknown => Ok(false),
            Verdict::Inconsistent => Err(inconsistent_hashes(piece)),
        }
    }

    /// The shares its files hold of the `len` bytes of `piece` from `from`
    /// on, in file order.
    fn piece_spans(
        &self,
        piece: ValidPieceIndex,
        from: usize,
        len: usize,
    ) -> impl Iterator<Item = Span<'a>> + use<'a> {
        let lengths = self.torrent.lengths();
        let start = lengths.piece_offset(piece) + from as u64;
        let len = len as u64;
        let file_infos = self.file_infos;
        let first = file_infos.partition_point(|fi| fi.offset_in_torrent + fi.len <= start);
        let mut done = 0u64;
        file_infos
            .iter()
            .enumerate()
            .skip(first)
            .filter(|(_, info)| info.len > 0)
            .map_while(move |(file, info)| {
                if done == len {
                    return None;
                }
                let at = start + done - info.offset_in_torrent;
                let take = (len - done).min(info.len - at);
                let span = Span {
                    file,
                    info,
                    at,
                    from: usize::try_from(done).ok()?,
                    len: usize::try_from(take).ok()?,
                };
                done += take;
                Some(span)
            })
    }

    /// A piece's share of one file read whole into `into`, in one read
    /// call. `after` is where the read before it on that file ended.
    fn read_span(&self, span: &Span<'_>, into: &mut [u8], after: u64) -> anyhow::Result<()> {
        if span.info.attrs.padding {
            into.fill(0);
            return Ok(());
        }
        let started = Instant::now();
        let read = self.files.pread_exact(span.file, span.at, into);
        let bytes = if read.is_ok() { into.len() } else { 0 };
        CHECK_STATS.read(bytes, started, span.at != after);
        read
    }

    /// The bytes of `piece` from `from` on read into `into`: a piece's
    /// share of a file in one read call.
    fn read_whole(
        &self,
        piece: ValidPieceIndex,
        from: usize,
        into: &mut [u8],
    ) -> anyhow::Result<()> {
        let mut filled = 0;
        for span in self.piece_spans(piece, from, into.len()) {
            let share = into
                .get_mut(span.from..span.from + span.len)
                .context("broken torrent metadata")?;
            // A slice that goes on in the file the one before it ended in
            // starts where that one ended.
            let after = if from > 0 && filled == 0 { span.at } else { 0 };
            self.read_span(&span, share, after).with_context(|| {
                format!(
                    "error reading {} bytes, file_id: {} (\"{:?}\")",
                    span.len, span.file, span.info.relative_filename
                )
            })?;
            filled += span.len;
        }
        anyhow::ensure!(filled == into.len(), "broken torrent metadata");
        Ok(())
    }

    /// A downloaded piece checked against both its hashes, the v2 one even
    /// when v1 fails, so that a torrent whose hashes disagree stops
    /// (libtorrent `torrent::on_piece_verified`).
    pub fn check_piece(&self, piece_index: ValidPieceIndex) -> anyhow::Result<Verdict> {
        self.check_piece_read(piece_index, |h| self.hash_from_files(piece_index, h))
    }

    /// [`Self::check_piece`] of a piece that was not just written, as a
    /// seed-mode piece a peer first asks for: its share of each file is
    /// read in one read call, not a part at a time, into one buffer of at
    /// most [`CheckMemory::slice`], a larger piece a slice at a time.
    pub fn check_piece_whole(&self, piece_index: ValidPieceIndex) -> anyhow::Result<Verdict> {
        self.check_piece_sliced(piece_index, CHECK_MEMORY.slice)
    }

    fn check_piece_sliced(
        &self,
        piece_index: ValidPieceIndex,
        slice: usize,
    ) -> anyhow::Result<Verdict> {
        self.check_piece_read(piece_index, |h| {
            let len = self.torrent.lengths().piece_length(piece_index) as usize;
            let mut bytes = vec![0u8; len.min(slice)];
            let mut hashing = Duration::ZERO;
            let mut from = 0;
            while from < len {
                let slice = &mut bytes[..(len - from).min(slice)];
                self.read_whole(piece_index, from, slice)?;
                let started = Instant::now();
                h.update(slice);
                hashing += started.elapsed();
                from += slice.len();
            }
            CHECK_STATS.hashed(hashing);
            Ok(())
        })
    }

    // The piece checked against both its hashes, its bytes hashed where
    // the storage holds them, or else by `read`.
    fn check_piece_read(
        &self,
        piece_index: ValidPieceIndex,
        read: impl FnOnce(&mut PieceDigest) -> anyhow::Result<()>,
    ) -> anyhow::Result<Verdict> {
        self.on_disk();
        let mut h = self.digest(piece_index);
        if !self
            .files
            .hash_held_piece(piece_index, &mut |bytes| h.update(bytes))
        {
            read(&mut h)?;
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
        // The piece's first file found by halving, not by a walk from the
        // torrent's first file, as the reads and writes of blocks find it:
        // a file before it, or one that ends where the piece starts, gives
        // the piece no bytes.
        let (first, mut absolute_offset) =
            self.file_at(self.torrent.lengths().piece_offset(piece_index));
        let mut buf = vec![0u8; std::cmp::min(65536, piece_length as usize)];

        let mut piece_remaining_bytes = piece_length as usize;

        for (file_idx, fi) in self.file_infos.iter().enumerate().skip(first) {
            let file_len = fi.len;
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
        self.on_disk();
        if result_buf.len() < chunk_info.size as usize {
            anyhow::bail!("read_chunk(): not enough capacity in the provided buffer")
        }
        let (first, mut absolute_offset) =
            self.file_at(self.torrent.lengths().chunk_absolute_offset(chunk_info));
        let mut buf = result_buf;

        for (file_idx, file_info) in self.file_infos.iter().enumerate().skip(first) {
            let file_len = file_info.len;
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

    /// The file that holds the torrent's byte at `offset`, and where in
    /// that file the byte is: found by halving, as a torrent may have many
    /// thousands of files and this runs for every block read or written.
    /// Past the torrent's end, the count of files.
    fn file_at(&self, offset: u64) -> (usize, u64) {
        let file = self
            .file_infos
            .partition_point(|fi| fi.offset_in_torrent + fi.len <= offset);
        let start = self
            .file_infos
            .get(file)
            .map_or(offset, |fi| fi.offset_in_torrent);
        (file, offset - start)
    }

    /// Each of `chunks` read into its buffer of `bufs`, sized to the block.
    /// A run of blocks one after another in one file on disk is read in one
    /// go (libtorrent `disk_io_thread::do_read` reads a cache line of blocks
    /// with one readv); a block in padding, or across a file boundary, is
    /// read on its own. On an error, the first block not read and why: the
    /// blocks before it are read whole. With `held`, from a volume that
    /// reads whole pieces, each block is copied out of the whole piece it is
    /// in, read once into `held` when the connection first asks for it.
    pub fn read_chunks(
        &self,
        who_sent: PeerHandle,
        chunks: &[ChunkInfo],
        bufs: &mut [&mut [u8]],
        held: Option<&mut HeldPiece>,
    ) -> Result<(), (usize, anyhow::Error)> {
        self.on_disk();
        if let Some(held) = held {
            return self.read_chunks_held(chunks, bufs, held);
        }
        let lengths = self.torrent.lengths();
        let mut i = 0;
        while i < chunks.len() {
            let start = lengths.chunk_absolute_offset(&chunks[i]);
            let (file_idx, in_file) = self.file_at(start);
            let file_start = start - in_file;
            let file = self
                .file_infos
                .get(file_idx)
                .filter(|file_info| !file_info.attrs.padding)
                .map(|file_info| (file_idx, file_start + file_info.len));
            let mut end = start;
            let mut j = i;
            if let Some((_, file_end)) = file {
                while j < chunks.len()
                    && bufs[j].len() == chunks[j].size as usize
                    && lengths.chunk_absolute_offset(&chunks[j]) == end
                    && end + u64::from(chunks[j].size) <= file_end
                {
                    end += u64::from(chunks[j].size);
                    j += 1;
                }
            }
            match file {
                Some((file_idx, _)) if j - i > 1 => {
                    trace!(
                        handle = %who_sent,
                        file_idx,
                        blocks = j - i,
                        "reading a run of blocks from {}",
                        start - file_start
                    );
                    self.files
                        .preadv_exact(file_idx, start - file_start, &mut bufs[i..j])
                        .with_context(|| {
                            format!("error reading {} bytes, file_id: {file_idx}", end - start)
                        })
                        .map_err(|e| (i, e))?;
                    i = j;
                }
                _ => {
                    self.read_chunk(who_sent, &chunks[i], bufs[i])
                        .map_err(|e| (i, e))?;
                    i += 1;
                }
            }
        }
        Ok(())
    }

    /// [`Self::read_chunks`] out of whole pieces: a block of a piece other
    /// than the one `held` holds reads that piece whole first, its share of
    /// each file in one read, into the buffer `held` keeps, sized before
    /// the read. A piece that fails to read is held no more.
    fn read_chunks_held(
        &self,
        chunks: &[ChunkInfo],
        bufs: &mut [&mut [u8]],
        held: &mut HeldPiece,
    ) -> Result<(), (usize, anyhow::Error)> {
        let lengths = self.torrent.lengths();
        for (i, (chunk, buf)) in chunks.iter().zip(bufs.iter_mut()).enumerate() {
            if held.piece != Some(chunk.piece_index) {
                held.piece = None;
                let len = lengths.piece_length(chunk.piece_index) as usize;
                held.bytes.resize(len, 0);
                self.read_piece(chunk.piece_index, &mut held.bytes)
                    .map_err(|e| (i, e))?;
                held.piece = Some(chunk.piece_index);
            }
            let from = chunk.offset as usize;
            let block = held
                .bytes
                .get(from..from + chunk.size as usize)
                .filter(|block| block.len() == buf.len())
                .context("read_chunks(): the block is not in its piece")
                .map_err(|e| (i, e))?;
            buf.copy_from_slice(block);
        }
        Ok(())
    }

    /// The whole of `piece` read into `into`, its share of each file in one
    /// read call, padding as zeros.
    fn read_piece(&self, piece: ValidPieceIndex, into: &mut [u8]) -> anyhow::Result<()> {
        let mut filled = 0;
        for span in self.piece_spans(piece, 0, into.len()) {
            let share = into
                .get_mut(span.from..span.from + span.len)
                .context("broken torrent metadata")?;
            if span.info.attrs.padding {
                share.fill(0);
            } else {
                self.files
                    .pread_exact(span.file, span.at, share)
                    .with_context(|| {
                        format!(
                            "error reading {} bytes, file_id: {} (\"{:?}\")",
                            span.len, span.file, span.info.relative_filename
                        )
                    })?;
            }
            filled += span.len;
        }
        anyhow::ensure!(filled == into.len(), "broken torrent metadata");
        Ok(())
    }

    pub fn write_chunk(
        &self,
        who_sent: PeerHandle,
        data: &Piece<ByteBuf<'a>>,
        chunk_info: &ChunkInfo,
    ) -> anyhow::Result<()> {
        self.on_disk();
        let (first, mut absolute_offset) =
            self.file_at(self.torrent.lengths().chunk_absolute_offset(chunk_info));
        let mut data = DoubleBufHelper::new(data.data().0, data.data().1);

        for (file_idx, file_info) in self.file_infos.iter().enumerate().skip(first) {
            let file_len = file_info.len;

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
    use anyhow::bail;

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

    /// A storage over `Files` that keeps each run of buffers it was asked to
    /// read in one go: the file, the offset and how many.
    struct Runs {
        files: Files,
        runs: std::sync::Mutex<Vec<(usize, u64, usize)>>,
    }

    impl TorrentStorage for Runs {
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

        fn preadv_exact(
            &self,
            file_id: usize,
            mut offset: u64,
            bufs: &mut [&mut [u8]],
        ) -> anyhow::Result<()> {
            self.runs
                .lock()
                .unwrap()
                .push((file_id, offset, bufs.len()));
            for buf in bufs.iter_mut() {
                self.files.pread_exact(file_id, offset, buf)?;
                offset += buf.len() as u64;
            }
            Ok(())
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
    }

    /// A storage over `Files` on a spinning volume: one read turn, whole
    /// pieces at a read. It keeps each read it was asked for, in order, and
    /// whether a read ran without the volume's one turn held.
    struct Spinning {
        files: Files,
        turns: ReadTurns,
        reads: std::sync::Mutex<Vec<(usize, u64, usize)>>,
        unturned: AtomicBool,
    }

    impl Spinning {
        fn new(files: Files) -> Self {
            Self {
                files,
                turns: ReadTurns::new(Arc::new(tokio::sync::Semaphore::new(1)), true),
                reads: Default::default(),
                unturned: AtomicBool::new(false),
            }
        }
    }

    impl TorrentStorage for Spinning {
        fn init(
            &mut self,
            shared: &ManagedTorrentShared,
            metadata: &TorrentMetadata,
        ) -> anyhow::Result<()> {
            self.files.init(shared, metadata)
        }

        fn pread_exact(&self, file_id: usize, offset: u64, buf: &mut [u8]) -> anyhow::Result<()> {
            if self.turns.semaphore().available_permits() != 0 {
                self.unturned.store(true, Ordering::Relaxed);
            }
            self.reads.lock().unwrap().push((file_id, offset, buf.len()));
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

        fn on_piece_hash_failed(&self, piece: ValidPieceIndex) -> anyhow::Result<()> {
            self.files.on_piece_hash_failed(piece)
        }

        fn on_piece_checked(&self, piece: ValidPieceIndex) -> anyhow::Result<()> {
            self.files.on_piece_checked(piece)
        }

        fn read_turns(&self) -> ReadTurns {
            self.turns.clone()
        }
    }

    /// The check of a torrent on a spinning volume finds what the check on
    /// a solid-state one finds, over the same files with the same pieces
    /// missing and damaged: the same pieces had, the same failed and found
    /// told to the storage, on one hashing thread and on four. Each read
    /// holds the volume's one turn, reads a piece's whole share of a file,
    /// and starts where the read before it on that file ended: the files
    /// are read front to back, a piece a read, and the turn comes back.
    #[test]
    fn a_check_on_a_spinning_volume_finds_what_one_on_a_solid_one_finds() {
        let t = checking_torrent();
        let metadata = metadata(&t.torrent_bytes());
        let mut payload = t.v1_payload();
        let lengths = metadata.lengths();
        // A damaged byte in the third piece, and a file cut short.
        let third = usize::try_from(lengths.default_piece_length()).unwrap() * 2 + 7;
        payload[third] ^= 0xff;
        let seeded = || {
            let mut files = Files::new(&metadata, &payload);
            let cut = files
                .0
                .iter()
                .enumerate()
                .filter(|(_, bytes)| bytes.len() > 4)
                .map(|(file, _)| file)
                .nth(5)
                .unwrap();
            let len = files.0[cut].len();
            files.0[cut].truncate(len / 2);
            files
        };
        for threads in [1, 4] {
            let solid = seeded();
            let had_solid = FileOps::new(&metadata, &solid, false)
                .initial_check(threads, &AtomicU64::new(0), &AtomicBool::new(false))
                .unwrap();
            let spinning = Spinning::new(seeded());
            let had_spinning = FileOps::new(&metadata, &spinning, false)
                .initial_check(threads, &AtomicU64::new(0), &AtomicBool::new(false))
                .unwrap();
            assert_eq!(had_spinning, had_solid, "{threads}");
            assert!(!had_solid.iter().by_vals().all(|had| had), "{threads}");
            assert_eq!(
                *spinning.files.2.lock().unwrap(),
                *solid.2.lock().unwrap(),
                "{threads}"
            );
            assert_eq!(
                *spinning.files.4.lock().unwrap(),
                *solid.4.lock().unwrap(),
                "{threads}"
            );

            assert!(!spinning.unturned.load(Ordering::Relaxed), "{threads}");
            assert_eq!(spinning.turns.semaphore().available_permits(), 1);
            let reads = spinning.reads.lock().unwrap();
            let shares: usize = lengths
                .iter_piece_infos()
                .map(|piece| {
                    FileOps::new(&metadata, &spinning.files, false)
                        .piece_spans(piece.piece_index, 0, piece.len as usize)
                        .filter(|span| !span.info.attrs.padding)
                        .count()
                })
                .sum();
            assert!(reads.len() <= shares, "{threads}: {} > {shares}", reads.len());
            let mut ends = vec![0u64; metadata.file_infos.len()];
            for &(file, at, len) in reads.iter() {
                assert_eq!(at, ends[file], "{threads}: file {file}");
                ends[file] = at + len as u64;
            }
        }
    }

    fn peer() -> PeerHandle {
        "127.0.0.1:6881"
            .parse::<std::net::SocketAddr>()
            .unwrap()
            .into()
    }

    // Every block of the torrent, in order.
    fn every_block(metadata: &TorrentMetadata) -> Vec<ChunkInfo> {
        let lengths = metadata.lengths();
        lengths
            .iter_piece_infos()
            .flat_map(|piece| lengths.iter_chunk_infos(piece.piece_index))
            .collect()
    }

    // A downloaded piece read back from its files hashes the same bytes in
    // the same order as a walk from the torrent's first file did: over
    // empty files at the start, in the middle, at a piece's start and at
    // the end, a piece across three files and more, and padding, in a v1
    // torrent and a hybrid.
    #[test]
    fn a_piece_hashed_from_its_files_hashes_what_a_walk_of_the_files_did() {
        // The walk hash_from_files made before it found the first file by
        // halving.
        fn walked(ops: &FileOps<'_>, piece: ValidPieceIndex) -> PieceDigest {
            let mut h = ops.digest(piece);
            let piece_length = ops.torrent.lengths().piece_length(piece);
            let mut absolute_offset = ops.torrent.lengths().piece_offset(piece);
            let mut buf = vec![0u8; std::cmp::min(65536, piece_length as usize)];
            let mut remaining = piece_length as usize;
            for (file_idx, fi) in ops.file_infos.iter().enumerate() {
                if absolute_offset > fi.len {
                    absolute_offset -= fi.len;
                    continue;
                }
                let to_read: usize = std::cmp::min(fi.len - absolute_offset, remaining as u64)
                    .try_into()
                    .unwrap();
                update_hash_from_file(
                    file_idx,
                    fi,
                    absolute_offset,
                    ops.files,
                    &mut h,
                    &mut buf,
                    to_read,
                )
                .unwrap();
                remaining -= to_read;
                if remaining == 0 {
                    break;
                }
                absolute_offset = 0;
            }
            h
        }

        let v1 = v1_torrent(
            &[
                ("a", 0),
                ("b", 3),
                ("c", 0),
                ("d", 16380),
                ("e", 0),
                ("f", 0),
                ("g", 5),
                ("h", 20000),
                ("i", 0),
                ("j", 1),
                ("k", 1),
                ("l", 12000),
                ("m", 0),
                ("n", 30000),
                ("o", 0),
            ],
            16384,
        );
        let hybrid = checking_torrent();
        let torrents = [v1, (metadata(&hybrid.torrent_bytes()), hybrid.v1_payload())];
        let mut across_three = false;
        for (metadata, payload) in torrents {
            let files = Files::new(&metadata, &payload);
            let ops = FileOps::new(&metadata, &files, false);
            let lengths = metadata.lengths();
            for info in lengths.iter_piece_infos() {
                let piece = info.piece_index;
                let start = lengths.piece_offset(piece);
                let end = start + u64::from(lengths.piece_length(piece));
                let holding = metadata
                    .file_infos
                    .iter()
                    .filter(|fi| {
                        fi.offset_in_torrent < end && fi.offset_in_torrent + fi.len > start
                    })
                    .count();
                across_three |= holding >= 3;
                let mut found = ops.digest(piece);
                ops.hash_from_files(piece, &mut found).unwrap();
                let (found, expected) = (found.finish(), walked(&ops, piece).finish());
                assert_eq!(found, expected, "piece {piece}");
                assert!(found.0.is_some() || found.1.is_some(), "piece {piece}");
            }
        }
        assert!(across_three, "no piece spans three files");
    }

    // Over many files, empty ones and padding among them, the file found
    // for an offset is the one a walk from the first file finds, and every
    // block reads the torrent's bytes at its place, alone or in a run.
    #[test]
    fn the_file_of_an_offset_is_the_one_a_walk_of_the_files_finds() {
        let t = checking_torrent();
        let metadata = metadata(&t.torrent_bytes());
        let payload = t.v1_payload();
        let files = Files::new(&metadata, &payload);
        let ops = FileOps::new(&metadata, &files, false);
        let walked = |offset: u64| {
            let mut file_start = 0;
            for (file, fi) in metadata.file_infos.iter().enumerate() {
                if offset - file_start < fi.len {
                    return (file, offset - file_start);
                }
                file_start += fi.len;
            }
            (metadata.file_infos.len(), 0)
        };
        assert!(metadata.file_infos.len() > 40);
        let total = payload.len() as u64;
        for fi in metadata.file_infos.iter() {
            for offset in [fi.offset_in_torrent, fi.offset_in_torrent + fi.len] {
                for offset in offset.saturating_sub(1)..=(offset + 1).min(total) {
                    assert_eq!(ops.file_at(offset), walked(offset), "{offset}");
                }
            }
        }

        let lengths = metadata.lengths();
        let chunks = every_block(&metadata);
        let expected: Vec<&[u8]> = chunks
            .iter()
            .map(|chunk| {
                let start = usize::try_from(lengths.chunk_absolute_offset(chunk)).unwrap();
                &payload[start..start + chunk.size as usize]
            })
            .collect();
        let mut read: Vec<Vec<u8>> = chunks
            .iter()
            .map(|chunk| vec![0xee; chunk.size as usize])
            .collect();
        for (chunk, buf) in chunks.iter().zip(&mut read) {
            ops.read_chunk(peer(), chunk, buf).unwrap();
        }
        assert_eq!(read, expected);
        read.iter_mut().for_each(|buf| buf.fill(0xee));
        let mut bufs: Vec<&mut [u8]> = read.iter_mut().map(Vec::as_mut_slice).collect();
        ops.read_chunks(peer(), &chunks, &mut bufs, None).unwrap();
        assert_eq!(read, expected);
    }

    /// Blocks read out of whole pieces, as a connection uploading from a
    /// spinning disk reads them, are the bytes the blocks read on their own
    /// are, in every order a peer asks for them, across files and padding.
    #[test]
    fn blocks_read_out_of_whole_pieces_are_the_blocks_read_alone() {
        for t in [two_files(), checking_torrent()] {
            let metadata = metadata(&t.torrent_bytes());
            let payload = t.v1_payload();
            let storage = Runs {
                files: Files::new(&metadata, &payload),
                runs: Default::default(),
            };
            let ops = FileOps::new(&metadata, &storage, false);
            let chunks = every_block(&metadata);
            let mut orders = vec![chunks.clone()];
            orders.push(chunks.iter().rev().copied().collect());
            // Two blocks of a piece, then a block of another, then the
            // first piece again.
            orders.push(chunks.chunks(2).flat_map(|two| two.iter().rev()).copied().collect());
            for order in orders {
                let mut old: Vec<Vec<u8>> =
                    order.iter().map(|c| vec![0xee; c.size as usize]).collect();
                let mut bufs: Vec<&mut [u8]> = old.iter_mut().map(Vec::as_mut_slice).collect();
                ops.read_chunks(peer(), &order, &mut bufs, None).unwrap();

                let mut held = HeldPiece::default();
                let mut new: Vec<Vec<u8>> =
                    order.iter().map(|c| vec![0xee; c.size as usize]).collect();
                // A batch at a time, the held piece kept between batches.
                for (batch, bufs) in order.chunks(3).zip(new.chunks_mut(3)) {
                    let mut bufs: Vec<&mut [u8]> = bufs.iter_mut().map(Vec::as_mut_slice).collect();
                    ops.read_chunks(peer(), batch, &mut bufs, Some(&mut held))
                        .unwrap();
                }
                assert_eq!(new, old);
            }
        }
    }

    /// A block read out of a whole piece that fails to read names the first
    /// block of that piece in the batch, the blocks before it read whole, as
    /// a run that fails names its first block; the failed piece is not held,
    /// and is read again when next asked for.
    #[test]
    fn a_whole_piece_that_fails_names_its_first_block_and_is_not_held() {
        let t = two_files();
        let metadata = metadata(&t.torrent_bytes());
        let payload = t.v1_payload();
        let mut files = Files::new(&metadata, &payload);
        let b = metadata
            .file_infos
            .iter()
            .position(|fi| !fi.attrs.padding && fi.offset_in_torrent > 0)
            .unwrap();
        files.3.push((b, 40_000..40_001));
        let lengths = metadata.lengths();
        let bad = metadata.file_infos[b].offset_in_torrent + 40_000;
        let storage = Runs {
            files,
            runs: Default::default(),
        };
        let ops = FileOps::new(&metadata, &storage, false);
        let chunks = every_block(&metadata);
        let first_bad = chunks
            .iter()
            .position(|c| {
                let start = lengths.piece_offset(c.piece_index);
                (start..start + u64::from(lengths.piece_length(c.piece_index))).contains(&bad)
            })
            .unwrap();
        let mut read: Vec<Vec<u8>> = chunks.iter().map(|c| vec![0xee; c.size as usize]).collect();
        let mut bufs: Vec<&mut [u8]> = read.iter_mut().map(Vec::as_mut_slice).collect();
        let mut held = HeldPiece::default();
        let (first, _) = ops
            .read_chunks(peer(), &chunks, &mut bufs, Some(&mut held))
            .unwrap_err();
        assert_eq!(first, first_bad);
        assert_eq!(held.piece, None);
        for (chunk, got) in chunks[..first].iter().zip(&read) {
            let mut block = vec![0; chunk.size as usize];
            ops.read_chunk(peer(), chunk, &mut block).unwrap();
            assert_eq!(*got, block);
        }
        // The old way fails no later.
        let mut bufs: Vec<&mut [u8]> = read.iter_mut().map(Vec::as_mut_slice).collect();
        let (old_first, _) = ops.read_chunks(peer(), &chunks, &mut bufs, None).unwrap_err();
        assert!(old_first <= first, "{old_first} {first}");
    }

    /// The blocks one after another in one file are read in one go, a block
    /// across a file boundary or in padding on its own, and each block
    /// reads as it does alone.
    #[test]
    fn a_run_of_blocks_in_one_file_is_read_in_one_go() {
        let t = two_files();
        let metadata = metadata(&t.torrent_bytes());
        let payload = t.v1_payload();
        let storage = Runs {
            files: Files::new(&metadata, &payload),
            runs: Default::default(),
        };
        let ops = FileOps::new(&metadata, &storage, false);
        let chunks = every_block(&metadata);
        let alone: Vec<Vec<u8>> = chunks
            .iter()
            .map(|chunk| {
                let mut buf = vec![0; chunk.size as usize];
                ops.read_chunk(peer(), chunk, &mut buf).unwrap();
                buf
            })
            .collect();
        let mut read: Vec<Vec<u8>> = chunks
            .iter()
            .map(|chunk| vec![0xee; chunk.size as usize])
            .collect();
        let mut bufs: Vec<&mut [u8]> = read.iter_mut().map(Vec::as_mut_slice).collect();
        ops.read_chunks(peer(), &chunks, &mut bufs, None).unwrap();
        assert_eq!(read, alone);
        // Piece 0's blocks hold a, then padding, and the last piece's b's
        // tail, then padding: the rest are b's, from its start.
        let b = metadata
            .file_infos
            .iter()
            .position(|fi| !fi.attrs.padding && fi.offset_in_torrent > 0)
            .unwrap();
        assert_eq!(*storage.runs.lock().unwrap(), [(b, 0, chunks.len() - 4)]);
    }

    /// A run that fails names its first block: the blocks before the run
    /// are read whole.
    #[test]
    fn a_run_that_fails_names_its_first_block() {
        let t = two_files();
        let metadata = metadata(&t.torrent_bytes());
        let payload = t.v1_payload();
        let mut files = Files::new(&metadata, &payload);
        let b = metadata
            .file_infos
            .iter()
            .position(|fi| !fi.attrs.padding && fi.offset_in_torrent > 0)
            .unwrap();
        files.3.push((b, 40_000..40_001));
        let storage = Runs {
            files,
            runs: Default::default(),
        };
        let ops = FileOps::new(&metadata, &storage, false);
        let chunks = every_block(&metadata);
        let mut read: Vec<Vec<u8>> = chunks
            .iter()
            .map(|chunk| vec![0xee; chunk.size as usize])
            .collect();
        let mut bufs: Vec<&mut [u8]> = read.iter_mut().map(Vec::as_mut_slice).collect();
        let (first, _) = ops.read_chunks(peer(), &chunks, &mut bufs, None).unwrap_err();
        assert_eq!(first, 2);
        let mut block = vec![0; chunks[0].size as usize];
        ops.read_chunk(peer(), &chunks[0], &mut block).unwrap();
        assert_eq!(read[0], block);
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

    /// The check as it ran with every thread reading: each takes the next
    /// piece no thread has taken, reads it 64 KiB at a time and hashes it
    /// as it reads. Kept to hold the check to its results.
    fn check_reading_on_every_thread(
        ops: &FileOps<'_>,
        threads: usize,
        progress: &AtomicU64,
        pause_requested: &AtomicBool,
    ) -> anyhow::Result<CheckOutcome> {
        use std::sync::atomic::AtomicUsize;
        let lengths = ops.torrent.lengths();
        let total = lengths.total_pieces() as usize;
        let mut have_pieces =
            BF::from_boxed_slice(vec![0u8; lengths.piece_bitfield_bytes()].into());
        let next = AtomicUsize::new(0);
        let broken: Vec<AtomicU64> = ops
            .file_infos
            .iter()
            .map(|_| AtomicU64::new(u64::MAX))
            .collect();
        let has_piece = |piece: ValidPieceIndex,
                         buf: &mut [u8],
                         mut whole: Option<&mut [u8]>|
         -> anyhow::Result<bool> {
            let start = lengths.piece_offset(piece);
            let mut pos = start;
            let mut remaining = u64::from(lengths.piece_length(piece));
            let mut digest = ops.digest(piece);
            let first = ops
                .file_infos
                .partition_point(|fi| fi.offset_in_torrent + fi.len <= pos);
            for (file, fi) in ops.file_infos.iter().enumerate().skip(first) {
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
                    ops.files,
                    &mut digest,
                    into,
                    take.try_into()?,
                ) {
                    if ends_the_file(&err) {
                        broken[file].fetch_min(at, Ordering::Relaxed);
                    }
                    return Ok(false);
                }
                pos += take;
                remaining -= take;
            }
            anyhow::ensure!(remaining == 0, "broken torrent metadata");
            let (v1, v2) = ops.hashes_match(piece, digest);
            match Verdict::of(v1, v2.filter(|_| v1 != Some(false))) {
                Verdict::Passed => Ok(true),
                Verdict::Failed | Verdict::Unknown => Ok(false),
                Verdict::Inconsistent => Err(inconsistent_hashes(piece)),
            }
        };
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
                let mut whole = ops
                    .files
                    .wants_hashed_piece(piece)
                    .then(|| vec![0u8; lengths.piece_length(piece) as usize]);
                let had = has_piece(piece, &mut buf, whole.as_deref_mut());
                if let (Ok(true), Some(whole)) = (&had, whole) {
                    ops.files.on_piece_hashed(piece, whole.into());
                }
                if had.is_err() {
                    next.fetch_max(total, Ordering::Relaxed);
                }
                checked.push((piece, had));
            }
            checked
        };
        let mut checked = std::thread::scope(|s| {
            let others: Vec<_> = (1..threads.clamp(1, total.max(1)))
                .map(|_| s.spawn(worker))
                .collect();
            let mut checked = worker();
            for other in others {
                checked.extend(other.join().unwrap());
            }
            checked
        });
        checked.sort_unstable_by_key(|(piece, _)| piece.get());
        let done = checked.len();
        for (piece, had) in checked {
            let had = had?;
            if had {
                ops.files.on_piece_checked(piece)?;
            } else {
                ops.files.on_piece_hash_failed(piece)?;
            }
            have_pieces.set(piece.get() as usize, had);
        }
        if done < total {
            return Ok(CheckOutcome::Paused(CheckedSoFar {
                next: done,
                have: have_pieces,
            }));
        }
        Ok(CheckOutcome::Done(have_pieces))
    }

    /// A v1 torrent of `files` laid end to end, so a piece can span two.
    fn v1_torrent(files: &[(&str, usize)], piece_length: usize) -> (TorrentMetadata, Vec<u8>) {
        let total: usize = files.iter().map(|(_, len)| len).sum();
        let data = payload(total, 9);
        let pieces: Vec<u8> = data
            .chunks(piece_length)
            .flat_map(|piece| {
                let mut h = Sha1::new();
                h.update(piece);
                h.finish()
            })
            .collect();
        let mut torrent = b"d4:infod5:filesl".to_vec();
        for (name, len) in files {
            torrent.extend(format!("d6:lengthi{len}e4:pathl{}:{name}ee", name.len()).bytes());
        }
        torrent.extend(
            format!(
                "e4:name1:x12:piece lengthi{piece_length}e6:pieces{}:",
                pieces.len()
            )
            .bytes(),
        );
        torrent.extend(pieces);
        torrent.extend(b"ee");
        (metadata(&torrent), data)
    }

    /// What a check left behind: the pieces it has, those the storage was
    /// told failed and those it was told were found, in the order told,
    /// and the progress counted.
    type Left = (Vec<bool>, Vec<u32>, Vec<u32>, u64);

    fn left(metadata: &TorrentMetadata, files: &Files, have: BF, progress: &AtomicU64) -> Left {
        (
            have.iter()
                .by_vals()
                .take(metadata.lengths().total_pieces() as usize)
                .collect(),
            std::mem::take(&mut *files.2.lock().unwrap()),
            std::mem::take(&mut *files.4.lock().unwrap()),
            progress.load(Ordering::Relaxed),
        )
    }

    /// The check with one reader finds what the check with every thread
    /// reading found, and tells the storage the same, in the same order:
    /// over one file, files a piece spans, a short last piece, a damaged
    /// piece, a file cut short, a missing file, a range the storage does
    /// not hand out, a hybrid and a v1 torrent.
    #[test]
    fn one_reader_finds_what_every_thread_reading_found() {
        let hybrid_t = checking_torrent();
        let hybrid_m = metadata(&hybrid_t.torrent_bytes());
        let hybrid_p = hybrid_t.v1_payload();
        let (one_m, one_p) = v1_torrent(&[("a", 5 * 16384 + 77)], 16384);
        let (many_m, many_p) = v1_torrent(
            &[
                ("a", 100),
                ("b", 40_000),
                ("c", 0),
                ("d", 16384),
                ("e", 70_001),
            ],
            16384,
        );
        let v2_t = two_files();
        let v2_m = metadata(&v2_t.torrent_bytes());
        let v2_p = v2_t.v1_payload();

        // Each case: a torrent, its payload, and what is wrong with the
        // files the storage holds.
        type Damage = Box<dyn Fn(&mut Files)>;
        let whole: Damage = Box::new(|_| {});
        let flipped: Damage = Box::new(|files| {
            let file = files.0.iter_mut().max_by_key(|f| f.len()).unwrap();
            let middle = file.len() / 2;
            file[middle] ^= 1;
        });
        let short: Damage = Box::new(|files| {
            let file = files.0.iter_mut().max_by_key(|f| f.len()).unwrap();
            file.truncate(file.len() / 2);
        });
        let missing: Damage = Box::new(|files| {
            files.0.iter_mut().max_by_key(|f| f.len()).unwrap().clear();
        });
        let hole: Damage = Box::new(|files| {
            let (file, _) = files
                .0
                .iter()
                .enumerate()
                .max_by_key(|(_, f)| f.len())
                .unwrap();
            files.3.push((file, 20_000..20_100));
        });
        let torrents = [
            (&one_m, &one_p),
            (&many_m, &many_p),
            (&v2_m, &v2_p),
            (&hybrid_m, &hybrid_p),
        ];
        let damages = [whole, flipped, short, missing, hole];
        for (t, (metadata, payload)) in torrents.iter().enumerate() {
            for (d, damage) in damages.iter().enumerate() {
                for disable_v1_hashes in [true, false] {
                    let reference = {
                        let mut files = Files::new(metadata, payload);
                        damage(&mut files);
                        let progress = AtomicU64::new(0);
                        let outcome = check_reading_on_every_thread(
                            &FileOps::new(metadata, &files, disable_v1_hashes),
                            4,
                            &progress,
                            &AtomicBool::new(false),
                        )
                        .unwrap();
                        let CheckOutcome::Done(have) = outcome else {
                            panic!("paused");
                        };
                        left(metadata, &files, have, &progress)
                    };
                    if d > 0 {
                        assert!(reference.0.contains(&false), "torrent {t} damage {d}");
                    }
                    for threads in [1, 2, 4, 9] {
                        let mut files = Files::new(metadata, payload);
                        damage(&mut files);
                        let progress = AtomicU64::new(0);
                        let have = FileOps::new(metadata, &files, disable_v1_hashes)
                            .initial_check(threads, &progress, &AtomicBool::new(false))
                            .unwrap();
                        assert_eq!(
                            left(metadata, &files, have, &progress),
                            reference,
                            "torrent {t} damage {d} threads {threads} v1 off {disable_v1_hashes}"
                        );
                    }
                }
            }
        }
    }

    /// A hybrid whose hashes disagree ends the check with one reader as it
    /// ended it with every thread reading: in the same error, the storage
    /// told of the same pieces before it.
    #[test]
    fn one_reader_ends_on_the_error_every_thread_reading_ended_on() {
        let (real, other) = two_hybrids();
        let metadata = hybrid(&real, &other);
        let payload = real.v1_payload();
        let told = |files: &Files| {
            (
                std::mem::take(&mut *files.2.lock().unwrap()),
                std::mem::take(&mut *files.4.lock().unwrap()),
            )
        };
        let files = Files::new(&metadata, &payload);
        let reference = check_reading_on_every_thread(
            &FileOps::new(&metadata, &files, false),
            1,
            &AtomicU64::new(0),
            &AtomicBool::new(false),
        )
        .unwrap_err();
        let reference = (format!("{reference:#}"), told(&files));
        for threads in [1, 2, 4] {
            let files = Files::new(&metadata, &payload);
            let err = FileOps::new(&metadata, &files, false)
                .initial_check(threads, &AtomicU64::new(0), &AtomicBool::new(false))
                .unwrap_err();
            assert_eq!((format!("{err:#}"), told(&files)), reference, "{threads}");
        }
    }

    /// A storage over `Files` that keeps every read asked of it: the file,
    /// where it starts, how long it is, and the thread that asked. It can
    /// ask the check to pause as a byte of a file is read, wants every
    /// piece when told to, and counts the pieces read whole that no hasher
    /// has finished.
    struct Recording {
        files: Files,
        reads: std::sync::Mutex<Vec<(usize, u64, usize, std::thread::ThreadId)>>,
        pause_at: Option<(usize, u64, std::sync::Arc<AtomicBool>)>,
        wants: bool,
        // The pieces of a one-file torrent the storage wants as hashed,
        // and the torrent's piece length.
        wants_in: Option<(std::ops::Range<u32>, u32)>,
        held: std::sync::Mutex<(usize, usize)>,
        // What it was told is read next: the file, where, and how much.
        told: std::sync::Mutex<Vec<(usize, u64, u64)>>,
    }

    impl Recording {
        fn new(files: Files) -> Self {
            Self {
                files,
                reads: Default::default(),
                pause_at: None,
                wants: false,
                wants_in: None,
                held: Default::default(),
                told: Default::default(),
            }
        }
    }

    impl TorrentStorage for Recording {
        fn init(
            &mut self,
            shared: &ManagedTorrentShared,
            metadata: &TorrentMetadata,
        ) -> anyhow::Result<()> {
            self.files.init(shared, metadata)
        }

        fn will_read(&self, file_id: usize, offset: u64, len: u64) {
            self.told.lock().unwrap().push((file_id, offset, len));
        }

        fn pread_exact(&self, file_id: usize, offset: u64, buf: &mut [u8]) -> anyhow::Result<()> {
            self.reads.lock().unwrap().push((
                file_id,
                offset,
                buf.len(),
                std::thread::current().id(),
            ));
            if let Some((file, byte, pause)) = &self.pause_at
                && *file == file_id
                && (offset..offset + buf.len() as u64).contains(byte)
            {
                pause.store(true, Ordering::Relaxed);
            }
            let wanted = self.wants
                || self.wants_in.as_ref().is_some_and(|(pieces, len)| {
                    pieces.contains(&u32::try_from(offset / u64::from(*len)).unwrap())
                });
            if wanted {
                let mut held = self.held.lock().unwrap();
                held.0 += 1;
                held.1 = held.1.max(held.0);
            }
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
            bail!("not taken")
        }

        fn on_piece_hash_failed(&self, piece: ValidPieceIndex) -> anyhow::Result<()> {
            self.files.on_piece_hash_failed(piece)
        }

        fn on_piece_checked(&self, piece: ValidPieceIndex) -> anyhow::Result<()> {
            self.files.on_piece_checked(piece)
        }

        fn wants_hashed_piece(&self, piece: ValidPieceIndex) -> bool {
            self.wants
                || self
                    .wants_in
                    .as_ref()
                    .is_some_and(|(pieces, _)| pieces.contains(&piece.get()))
        }

        fn on_piece_hashed(&self, _piece: ValidPieceIndex, _bytes: bytes::Bytes) {
            self.held.lock().unwrap().0 -= 1;
        }
    }

    /// The read calls a check makes: one for each share of a piece a file
    /// holds, as long as the share, all from the thread that called the
    /// check, and on each file every read starting where the one before it
    /// ended. A disk head reading them never moves back, nor skips ahead.
    #[test]
    fn a_check_reads_each_file_front_to_back_a_piece_at_a_read_on_one_thread() {
        let (v1_m, v1_p) = v1_torrent(
            &[
                ("a", 100),
                ("b", 40_000),
                ("c", 0),
                ("d", 16384),
                ("e", 70_001),
            ],
            16384,
        );
        let hybrid_t = checking_torrent();
        let hybrid_m = metadata(&hybrid_t.torrent_bytes());
        let hybrid_p = hybrid_t.v1_payload();
        for (metadata, payload) in [(&v1_m, &v1_p), (&hybrid_m, &hybrid_p)] {
            let lengths = metadata.lengths();
            // The shares the files hold of the pieces, in piece order.
            let mut shares = Vec::new();
            for piece in lengths.iter_piece_infos() {
                let start = lengths.piece_offset(piece.piece_index);
                let end = start + u64::from(piece.len);
                for (file, fi) in metadata.file_infos.iter().enumerate() {
                    let from = start.max(fi.offset_in_torrent);
                    let to = end.min(fi.offset_in_torrent + fi.len);
                    if from < to && !fi.attrs.padding {
                        shares.push((
                            file,
                            from - fi.offset_in_torrent,
                            usize::try_from(to - from).unwrap(),
                        ));
                    }
                }
            }
            for threads in [1, 2, 4, 9] {
                let storage = Recording::new(Files::new(metadata, payload));
                let have = FileOps::new(metadata, &storage, true)
                    .initial_check(threads, &AtomicU64::new(0), &AtomicBool::new(false))
                    .unwrap();
                assert!(
                    have.iter()
                        .by_vals()
                        .take(lengths.total_pieces() as usize)
                        .all(|had| had)
                );
                let reads = storage.reads.into_inner().unwrap();
                let here = std::thread::current().id();
                assert!(reads.iter().all(|read| read.3 == here), "{threads}");
                let asked: Vec<_> = reads.iter().map(|r| (r.0, r.1, r.2)).collect();
                assert_eq!(asked, shares, "{threads}");
                let mut ends = vec![0u64; metadata.file_infos.len()];
                for (file, at, len) in asked {
                    assert_eq!(at, ends[file], "file {file} on {threads} threads");
                    ends[file] = at + len as u64;
                }
            }
        }
    }

    /// A reader tells the storage what it reads next before it reads it:
    /// each file's bytes once, front to back with no gap, never past the
    /// file's end, nothing for a file with no bytes, and never further
    /// past the piece being read than it is set to tell. A check tells the
    /// storage of every byte it reads the same way.
    #[test]
    fn a_check_tells_the_storage_what_it_reads_next() {
        const PIECE: usize = 16384;
        const AHEAD: u64 = 40_000;
        let (metadata, payload) = v1_torrent(
            &[
                ("a", 100),
                ("b", 40_000),
                ("c", 0),
                ("d", 16384),
                ("e", 70_001),
            ],
            PIECE,
        );
        let lengths = metadata.lengths();
        let total = lengths.total_length();
        // Each file's bytes told so far, which must go on where they ended.
        let told_whole = |storage: &Recording| {
            let mut ends = vec![0u64; metadata.file_infos.len()];
            for (file, at, len) in storage.told.lock().unwrap().iter().copied() {
                assert!(len > 0, "file {file}");
                assert_eq!(at, ends[file], "file {file}");
                ends[file] = at + len;
                assert!(ends[file] <= metadata.file_infos[file].len, "file {file}");
            }
            ends
        };

        let storage = Recording::new(Files::new(&metadata, &payload));
        let ops = FileOps::new(&metadata, &storage, true);
        let turns = ReadTurns::private();
        let mut reader = PieceReader::new(&ops, AHEAD, &turns);
        for piece in lengths.iter_piece_infos() {
            let start = lengths.piece_offset(piece.piece_index);
            let mut bytes = vec![0u8; piece.len as usize];
            assert!(reader.read(piece.piece_index, 0, &mut bytes).unwrap());
            let ends = told_whole(&storage);
            let told: u64 = ends.iter().sum();
            assert!(told >= total.min(start + u64::from(piece.len)), "{start}");
            assert!(told <= start + AHEAD, "{start}");
            // Nothing was read that the storage was not told of first.
            for (file, at, len, _) in storage.reads.lock().unwrap().iter() {
                assert!(at + *len as u64 <= ends[*file], "file {file}");
            }
        }
        let whole: Vec<u64> = metadata.file_infos.iter().map(|fi| fi.len).collect();
        assert_eq!(told_whole(&storage), whole);

        for threads in [1, 4] {
            let storage = Recording::new(Files::new(&metadata, &payload));
            FileOps::new(&metadata, &storage, true)
                .initial_check(threads, &AtomicU64::new(0), &AtomicBool::new(false))
                .unwrap();
            assert_eq!(told_whole(&storage), whole, "{threads}");
        }

        // A reader set to tell nothing tells nothing.
        let storage = Recording::new(Files::new(&metadata, &payload));
        let ops = FileOps::new(&metadata, &storage, true);
        let mut reader = PieceReader::new(&ops, 0, &turns);
        let first = lengths.iter_piece_infos().next().unwrap();
        let mut bytes = vec![0u8; first.len as usize];
        assert!(reader.read(first.piece_index, 0, &mut bytes).unwrap());
        assert!(storage.told.lock().unwrap().is_empty());
    }

    /// The buffers of a check hold no more than its memory allows and one
    /// buffer more, on any number of threads, whether the storage wants
    /// every piece as hashed, none, or a stretch of them between pieces it
    /// does not want; and never more than a piece waiting for each hasher,
    /// one with each hasher and one in the reader's hand.
    #[test]
    fn a_check_holds_no_more_bytes_than_its_memory_allows() {
        const PIECE: usize = 1024;
        let (metadata, payload) = v1_torrent(&[("a", 300 * PIECE)], PIECE);
        let memory = CheckMemory {
            slice: PIECE,
            ahead: 3 * PIECE,
        };
        let roomy = CheckMemory {
            slice: PIECE,
            ahead: 1000 * PIECE,
        };
        let stats = CHECK_STATS.snapshot();
        let mut checks = 0;
        for threads in [1, 2, 4, 9] {
            for wants in [None, Some(0..300), Some(100..200)] {
                let all = wants == Some(0..300);
                let mut storage = Recording::new(Files::new(&metadata, &payload));
                storage.wants_in = wants.map(|pieces| (pieces, u32::try_from(PIECE).unwrap()));
                let ops = FileOps::new(&metadata, &storage, true);
                let check = |memory| {
                    let (outcome, most) = ops.check_within(
                        memory,
                        threads,
                        &AtomicU64::new(0),
                        &AtomicBool::new(false),
                        None,
                    );
                    let CheckOutcome::Done(have) = outcome.unwrap() else {
                        panic!("paused");
                    };
                    assert_eq!(have.count_ones(), 300);
                    most
                };
                let most = check(memory);
                assert!(
                    (PIECE..=memory.ahead + PIECE).contains(&most),
                    "{most} bytes on {threads}"
                );
                let most = check(roomy);
                assert!(
                    (PIECE..=(2 * threads - 1) * PIECE).contains(&most),
                    "{most} bytes on {threads} with room"
                );
                checks += 2;
                // The pieces the storage wanted and was not yet handed.
                let (held, most) = storage.held.into_inner().unwrap();
                assert_eq!(held, 0);
                if all {
                    assert!((1..2 * threads).contains(&most), "{most} on {threads}");
                }
            }
        }
        // The process's counters saw these checks, whatever else ran.
        let counted = CHECK_STATS.snapshot();
        assert!(counted.read_calls - stats.read_calls >= checks * 300);
        assert!(counted.read_bytes - stats.read_bytes >= checks * 300 * PIECE as u64);
        assert!(counted.pieces_hashed - stats.pieces_hashed >= checks * 300);
    }

    /// A piece larger than one buffer is read a slice at a time, in order,
    /// on the one reader, into one buffer, to the verdict the piece read
    /// whole gets; one the storage wants as hashed still reaches it whole.
    #[test]
    fn a_piece_larger_than_a_buffer_is_read_in_slices_to_the_same_verdict() {
        const SLICE: usize = 5000;
        let memory = CheckMemory {
            slice: SLICE,
            ahead: 4 * SLICE,
        };
        let hybrid_t = checking_torrent();
        let hybrid_m = metadata(&hybrid_t.torrent_bytes());
        let hybrid_p = hybrid_t.v1_payload();
        let (v1_m, v1_p) = v1_torrent(&[("a", 100), ("b", 9 * 16384 + 5), ("c", 30_000)], 16384);
        for (metadata, payload) in [(&v1_m, &v1_p), (&hybrid_m, &hybrid_p)] {
            let mut damaged = payload.clone();
            let middle = damaged.len() / 2;
            damaged[middle] ^= 1;
            for payload in [payload, &damaged] {
                for threads in [1, 4] {
                    let whole = {
                        let files = Files::new(metadata, payload);
                        let progress = AtomicU64::new(0);
                        let have = FileOps::new(metadata, &files, false)
                            .initial_check(threads, &progress, &AtomicBool::new(false))
                            .unwrap();
                        left(metadata, &files, have, &progress)
                    };
                    let storage = Recording::new(Files::new(metadata, payload));
                    let progress = AtomicU64::new(0);
                    let (outcome, most) = FileOps::new(metadata, &storage, false).check_within(
                        memory,
                        threads,
                        &progress,
                        &AtomicBool::new(false),
                        None,
                    );
                    let CheckOutcome::Done(have) = outcome.unwrap() else {
                        panic!("paused");
                    };
                    assert_eq!(left(metadata, &storage.files, have, &progress), whole);
                    if threads == 1 {
                        assert_eq!(most, SLICE, "one buffer of a slice");
                    } else {
                        assert!(
                            (SLICE..=memory.ahead + SLICE).contains(&most),
                            "{most} bytes on {threads}"
                        );
                    }
                    // Every read is a slice at most, from one thread, and
                    // goes on in its file where the one before it ended.
                    let reads = storage.reads.into_inner().unwrap();
                    let mut ends = std::collections::HashMap::new();
                    for (file, offset, len, thread) in &reads {
                        assert!(*len <= SLICE);
                        assert_eq!(*thread, reads[0].3);
                        let end = ends.entry(*file).or_insert(0u64);
                        assert_eq!(*offset, *end, "file {file}");
                        *end = offset + *len as u64;
                    }
                }
            }
        }

        // Wanted, a piece is read whole whatever its size, and handed over.
        let mut storage = Recording::new(Files::new(&v1_m, &v1_p));
        storage.wants = true;
        let (outcome, most) = FileOps::new(&v1_m, &storage, true).check_within(
            memory,
            1,
            &AtomicU64::new(0),
            &AtomicBool::new(false),
            None,
        );
        assert!(matches!(outcome.unwrap(), CheckOutcome::Done(_)));
        assert_eq!(most, 16384, "one piece at a time on one thread");
        assert!(
            storage
                .reads
                .into_inner()
                .unwrap()
                .iter()
                .any(|(_, _, len, _)| *len == 16384)
        );
    }

    /// The check of one piece reads a piece larger than its buffer a slice
    /// at a time, to the verdict the piece read a part at a time gets.
    #[test]
    fn one_piece_larger_than_a_buffer_is_checked_in_slices_to_the_same_verdict() {
        const SLICE: usize = 5000;
        let (metadata, mut payload) =
            v1_torrent(&[("a", 100), ("b", 5 * 16384), ("c", 20_000)], 16384);
        payload[3 * 16384 + 7] ^= 1;
        let storage = Recording::new(Files::new(&metadata, &payload));
        let ops = FileOps::new(&metadata, &storage, true);
        let mut verdicts = Vec::new();
        for piece in metadata.lengths().iter_piece_infos() {
            let piece = piece.piece_index;
            let parts = ops.check_piece(piece).unwrap();
            storage.reads.lock().unwrap().clear();
            assert_eq!(ops.check_piece_sliced(piece, SLICE).unwrap(), parts);
            let reads = storage.reads.lock().unwrap();
            assert!(reads.iter().all(|(_, _, len, _)| *len <= SLICE), "{piece}");
            let read: usize = reads.iter().map(|(_, _, len, _)| len).sum();
            assert_eq!(read, metadata.lengths().piece_length(piece) as usize);
            verdicts.push(parts);
        }
        assert!(verdicts.contains(&Verdict::Failed));
        assert!(verdicts.contains(&Verdict::Passed));
    }

    /// A pause asked for while a piece is read stops the check at the next
    /// piece: the pieces read are hashed and told to the storage, no later
    /// piece is read, every thread of the check has ended when it returns,
    /// and the check goes on from there to the same end as one never
    /// paused.
    #[test]
    fn a_pause_mid_check_stops_at_the_next_piece_and_the_check_goes_on_from_there() {
        let (metadata, payload) = v1_torrent(&[("a", 100), ("b", 40 * 16384 + 5)], 16384);
        let lengths = metadata.lengths();
        let pieces = lengths.total_pieces() as usize;
        const PAUSED_IN: usize = 17;
        for threads in [1, 2, 4] {
            let pause = std::sync::Arc::new(AtomicBool::new(false));
            let mut storage = Recording::new(Files::new(&metadata, &payload));
            // The byte of file b that piece PAUSED_IN starts with.
            storage.pause_at = Some((1, (PAUSED_IN * 16384 - 100) as u64, pause.clone()));
            let ops = FileOps::new(&metadata, &storage, true);
            let progress = AtomicU64::new(0);
            let outcome = ops
                .initial_check_from(threads, &progress, &pause, None)
                .unwrap();
            let CheckOutcome::Paused(at) = outcome else {
                panic!("not paused on {threads}");
            };
            assert_eq!(at.next, PAUSED_IN + 1, "{threads}");
            assert_eq!(at.have.count_ones(), PAUSED_IN + 1);
            assert_eq!(
                *storage.files.4.lock().unwrap(),
                (0..=u32::try_from(PAUSED_IN).unwrap()).collect::<Vec<_>>()
            );
            assert_eq!(
                progress.load(Ordering::Relaxed),
                (PAUSED_IN as u64 + 1) * 16384
            );
            let read_before = storage.reads.lock().unwrap().len();

            pause.store(false, Ordering::Relaxed);
            let outcome = ops
                .initial_check_from(threads, &progress, &pause, Some(at))
                .unwrap();
            let CheckOutcome::Done(have) = outcome else {
                panic!("paused again on {threads}");
            };
            assert_eq!(have.count_ones(), pieces);
            assert_eq!(progress.load(Ordering::Relaxed), lengths.total_length());
            // No piece was read twice: one read for each piece, and one
            // more for the piece the two files share.
            assert_eq!(storage.reads.lock().unwrap().len(), pieces + 1);
            assert!(read_before < pieces);
        }
    }

    /// A seed-mode piece is read a share of a file at a read, and passes
    /// or fails as the piece read a part at a time does.
    #[test]
    fn a_piece_checked_whole_is_read_a_share_at_a_read_to_the_same_verdict() {
        let (metadata, mut payload) = v1_torrent(&[("a", 100), ("b", 5 * 16384)], 16384);
        payload[3 * 16384 + 7] ^= 1;
        let storage = Recording::new(Files::new(&metadata, &payload));
        let ops = FileOps::new(&metadata, &storage, true);
        for piece in metadata.lengths().iter_piece_infos() {
            let piece = piece.piece_index;
            let parts = ops.check_piece(piece).unwrap();
            storage.reads.lock().unwrap().clear();
            assert_eq!(ops.check_piece_whole(piece).unwrap(), parts);
            let shares = if piece.get() == 0 { 2 } else { 1 };
            assert_eq!(storage.reads.lock().unwrap().len(), shares, "{piece}");
        }
        assert_eq!(
            ops.check_piece_whole(metadata.lengths().validate_piece_index(3).unwrap())
                .unwrap(),
            Verdict::Failed
        );
        // A read that fails is an error, as it is a part at a time.
        let mut cut = Files::new(&metadata, &payload);
        cut.0[1].truncate(16384);
        let ops = FileOps::new(&metadata, &cut, true);
        let piece = metadata.lengths().validate_piece_index(2).unwrap();
        assert!(ops.check_piece(piece).is_err());
        assert!(ops.check_piece_whole(piece).is_err());
    }

    /// A piece of a v2 or a hybrid torrent read whole gets the verdict it
    /// gets read a part at a time, whole or damaged, with and without its
    /// v1 hashes: the checks of a resumed torrent's pieces read them whole.
    #[test]
    fn a_v2_or_hybrid_piece_checked_whole_gets_the_verdict_it_got_in_parts() {
        for torrent in [checking_torrent(), two_files()] {
            let metadata = metadata(&torrent.torrent_bytes());
            let whole = torrent.v1_payload();
            let mut damaged = whole.clone();
            let middle = damaged.len() / 2;
            damaged[middle] ^= 1;
            // A read that fails is an error either way; its callers do
            // not tell one error from another.
            let verdict = |v: anyhow::Result<Verdict>| v.ok();
            for payload in [&whole, &damaged] {
                for disable_v1_hashes in [true, false] {
                    let mut files = Files::new(&metadata, payload);
                    let longest = files.0.iter_mut().max_by_key(|f| f.len()).unwrap();
                    longest.truncate(longest.len() - 1);
                    let ops = FileOps::new(&metadata, &files, disable_v1_hashes);
                    let mut verdicts = Vec::new();
                    for piece in metadata.lengths().iter_piece_infos() {
                        let piece = piece.piece_index;
                        let parts = verdict(ops.check_piece(piece));
                        assert_eq!(verdict(ops.check_piece_whole(piece)), parts, "{piece}");
                        verdicts.push(parts);
                    }
                    assert!(verdicts.contains(&None), "no piece failed to read");
                    assert!(verdicts.contains(&Some(Verdict::Passed)));
                }
            }
        }
    }

    /// Adversary proofs for the check reader (review of ruling A1).
    mod adversary {
        use super::*;

        /// A v1 torrent of `files` laid end to end, a file marked `true`
        /// padding (BEP 47 `attr` p), its bytes zero in the payload.
        fn padded_v1(
            files: &[(&str, usize, bool)],
            piece_length: usize,
        ) -> (TorrentMetadata, Vec<u8>) {
            let total: usize = files.iter().map(|(_, len, _)| len).sum();
            let mut data = payload(total, 5);
            let mut at = 0;
            for (_, len, pad) in files {
                if *pad {
                    data[at..at + len].fill(0);
                }
                at += len;
            }
            let pieces: Vec<u8> = data
                .chunks(piece_length)
                .flat_map(|piece| {
                    let mut h = Sha1::new();
                    h.update(piece);
                    h.finish()
                })
                .collect();
            let mut torrent = b"d4:infod5:filesl".to_vec();
            for (name, len, pad) in files {
                let attr = if *pad { "4:attr1:p" } else { "" };
                torrent.extend(
                    format!("d{attr}6:lengthi{len}e4:pathl{}:{name}ee", name.len()).bytes(),
                );
            }
            torrent.extend(
                format!(
                    "e4:name1:x12:piece lengthi{piece_length}e6:pieces{}:",
                    pieces.len()
                )
                .bytes(),
            );
            torrent.extend(pieces);
            torrent.extend(b"ee");
            (metadata(&torrent), data)
        }

        type Damage = (String, Box<dyn Fn(&mut Files)>);

        /// Every damage of one file: a flipped byte, cut short by one byte,
        /// gone, and a range not in hand.
        fn damages_of(file: usize, len: usize) -> Vec<Damage> {
            let mut out: Vec<Damage> = vec![
                (
                    format!("flip {file}"),
                    Box::new(move |f: &mut Files| f.0[file][len / 2] ^= 1),
                ),
                (
                    format!("short {file}"),
                    Box::new(move |f: &mut Files| f.0[file].truncate(len - 1)),
                ),
                (
                    format!("missing {file}"),
                    Box::new(move |f: &mut Files| f.0[file].clear()),
                ),
                (
                    format!("hole {file}"),
                    Box::new(move |f: &mut Files| f.3.push((file, 0..len as u64))),
                ),
            ];
            if len > 2 {
                out.push((
                    format!("cut mid {file}"),
                    Box::new(move |f: &mut Files| f.0[file].truncate(len / 2)),
                ));
            }
            out
        }

        fn reference(
            metadata: &TorrentMetadata,
            payload: &[u8],
            damage: &dyn Fn(&mut Files),
            threads: usize,
        ) -> Left {
            let mut files = Files::new(metadata, payload);
            damage(&mut files);
            let progress = AtomicU64::new(0);
            let outcome = check_reading_on_every_thread(
                &FileOps::new(metadata, &files, true),
                threads,
                &progress,
                &AtomicBool::new(false),
            )
            .unwrap();
            let CheckOutcome::Done(have) = outcome else {
                panic!("paused");
            };
            left(metadata, &files, have, &progress)
        }

        fn same_as_reference(
            label: &str,
            metadata: &TorrentMetadata,
            payload: &[u8],
            damages: &[Damage],
        ) {
            let whole: Damage = ("whole".into(), Box::new(|_: &mut Files| {}));
            for (name, damage) in std::iter::once(&whole).chain(damages) {
                let want = reference(metadata, payload, damage, 1);
                assert_eq!(
                    reference(metadata, payload, damage, 4),
                    want,
                    "{label} {name}: reference not stable"
                );
                if name != "whole" && !name.starts_with("pad") {
                    assert!(want.0.contains(&false), "{label} {name}: damage not seen");
                }
                for threads in [1, 2, 3, 4, 9] {
                    let mut files = Files::new(metadata, payload);
                    damage(&mut files);
                    let progress = AtomicU64::new(0);
                    let have = FileOps::new(metadata, &files, true)
                        .initial_check(threads, &progress, &AtomicBool::new(false))
                        .unwrap();
                    assert_eq!(
                        left(metadata, &files, have, &progress),
                        want,
                        "{label} {name} threads {threads}"
                    );
                }
            }
        }

        fn every_file_damage(
            metadata: &TorrentMetadata,
            pick: impl Fn(usize) -> bool,
        ) -> Vec<Damage> {
            metadata
                .file_infos
                .iter()
                .enumerate()
                .filter(|(i, fi)| fi.len > 0 && !fi.attrs.padding && pick(*i))
                .flat_map(|(i, fi)| damages_of(i, usize::try_from(fi.len).unwrap()))
                .collect()
        }

        #[test]
        fn adversary_padding_files_get_the_old_verdicts() {
            let (m, p) = padded_v1(
                &[
                    ("a", 100, false),
                    ("p1", 16284, true),
                    ("b", 40000, false),
                    ("p2", 9152, true),
                    ("c", 5, false),
                ],
                16384,
            );
            assert_eq!(m.file_infos.iter().filter(|fi| fi.attrs.padding).count(), 2);
            let mut damages = every_file_damage(&m, |_| true);
            // The bytes a storage holds for a padding file are never read.
            for pad in [1usize, 3] {
                damages.push((
                    format!("pad junk {pad}"),
                    Box::new(move |f: &mut Files| f.0[pad].iter_mut().for_each(|b| *b = 0xa5)),
                ));
                damages.push((
                    format!("pad gone {pad}"),
                    Box::new(move |f: &mut Files| f.0[pad].clear()),
                ));
            }
            same_as_reference("padding", &m, &p, &damages);
        }

        #[test]
        fn adversary_file_boundaries_on_piece_boundaries_get_the_old_verdicts() {
            let (m, p) = v1_torrent(&[("a", 16384), ("b", 32768), ("c", 16389)], 16384);
            same_as_reference("boundary", &m, &p, &every_file_damage(&m, |_| true));
        }

        #[test]
        fn adversary_zero_length_files_get_the_old_verdicts() {
            let (m, p) = v1_torrent(
                &[
                    ("z0", 0),
                    ("a", 16384),
                    ("z1", 0),
                    ("z2", 0),
                    ("b", 100),
                    ("z3", 0),
                ],
                16384,
            );
            same_as_reference("zero", &m, &p, &every_file_damage(&m, |_| true));
        }

        #[test]
        fn adversary_thousands_of_tiny_files_in_one_piece_get_the_old_verdicts() {
            let names: Vec<String> = (0..2000).map(|i| format!("t{i:04}")).collect();
            let mut files: Vec<(&str, usize)> = names.iter().map(|n| (n.as_str(), 8)).collect();
            files.push(("big", 30000));
            let (m, p) = v1_torrent(&files, 16384);
            assert_eq!(m.lengths().total_pieces(), 3);
            let pick = |i: usize| [0, 1, 999, 1998, 1999, 2000].contains(&i);
            let mut damages = every_file_damage(&m, pick);
            damages.push((
                "two tiny gone".into(),
                Box::new(|f: &mut Files| {
                    f.0[10].clear();
                    f.0[1500].clear();
                }),
            ));
            same_as_reference("tiny", &m, &p, &damages);
            // And tiny files spread over several pieces.
            let spread: Vec<(&str, usize)> = names.iter().map(|n| (n.as_str(), 7)).collect();
            let (m, p) = v1_torrent(&spread, 4096);
            assert!(m.lengths().total_pieces() > 3);
            same_as_reference("spread", &m, &p, &every_file_damage(&m, |i| i % 97 == 0));
        }

        /// The pieces a storage wants as hashed reach it with the bytes the
        /// old check handed it, whatever else is wrong.
        #[test]
        fn adversary_wanted_pieces_reach_the_storage_as_before() {
            let (m, p) = padded_v1(
                &[
                    ("a", 100, false),
                    ("p1", 16284, true),
                    ("b", 70000, false),
                    ("c", 9, false),
                ],
                16384,
            );
            let wanted: Vec<u32> = (0..m.lengths().total_pieces())
                .filter(|i| i % 2 == 0)
                .collect();
            let run = |new: bool, threads: usize, damage: &dyn Fn(&mut Files)| {
                let mut files = Files::new(&m, &p);
                damage(&mut files);
                let storage = Wanting {
                    files,
                    wanted: wanted.clone(),
                    hashed: Default::default(),
                };
                let ops = FileOps::new(&m, &storage, true);
                if new {
                    ops.initial_check(threads, &AtomicU64::new(0), &AtomicBool::new(false))
                        .unwrap();
                } else {
                    check_reading_on_every_thread(
                        &ops,
                        threads,
                        &AtomicU64::new(0),
                        &AtomicBool::new(false),
                    )
                    .unwrap();
                }
                let mut hashed = storage.hashed.into_inner().unwrap();
                hashed.sort_by_key(|(piece, _)| *piece);
                hashed
            };
            let none = |_: &mut Files| {};
            let flip = |f: &mut Files| f.0[2][20000] ^= 1;
            let cut = |f: &mut Files| f.0[2].truncate(30000);
            for damage in [&none as &dyn Fn(&mut Files), &flip, &cut] {
                let want = run(false, 1, damage);
                for threads in [1, 2, 4, 9] {
                    assert_eq!(run(true, threads, damage), want, "threads {threads}");
                }
            }
        }

        /// A storage over `Files` that panics on a named piece, in a read of
        /// it or as it is handed over hashed.
        struct Panics {
            files: Files,
            piece_length: u64,
            read_panic: Option<u64>,
            hashed_panic: Option<u32>,
        }

        impl TorrentStorage for Panics {
            fn init(
                &mut self,
                shared: &ManagedTorrentShared,
                metadata: &TorrentMetadata,
            ) -> anyhow::Result<()> {
                self.files.init(shared, metadata)
            }
            fn pread_exact(
                &self,
                file_id: usize,
                offset: u64,
                buf: &mut [u8],
            ) -> anyhow::Result<()> {
                if self.read_panic == Some(offset / self.piece_length) {
                    panic!("adversary read panic");
                }
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
            fn wants_hashed_piece(&self, _piece: ValidPieceIndex) -> bool {
                self.hashed_panic.is_some()
            }
            fn on_piece_hashed(&self, piece: ValidPieceIndex, _bytes: bytes::Bytes) {
                if self.hashed_panic == Some(piece.get()) {
                    panic!("adversary hasher panic");
                }
            }
        }

        /// A panic on the reader's thread or a hasher's ends the check in
        /// that panic: nothing waits for ever (the runner bounds a hang).
        #[test]
        fn adversary_a_panic_in_the_reader_or_a_hasher_ends_the_check() {
            let (m, p) = v1_torrent(&[("a", 64 * 1024)], 1024);
            for threads in [1, 2, 4, 9] {
                for (read_panic, hashed_panic) in [
                    (Some(5), None),
                    (None, Some(5)),
                    (Some(0), None),
                    (None, Some(63)),
                ] {
                    let storage = Panics {
                        files: Files::new(&m, &p),
                        piece_length: 1024,
                        read_panic,
                        hashed_panic,
                    };
                    let ended = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                        FileOps::new(&m, &storage, true)
                            .initial_check(threads, &AtomicU64::new(0), &AtomicBool::new(false))
                            .map(|_| ())
                    }));
                    let err = ended.expect_err("the panic was swallowed");
                    let msg = err
                        .downcast_ref::<&str>()
                        .map(|s| s.to_string())
                        .or_else(|| err.downcast_ref::<String>().cloned())
                        .unwrap_or_default();
                    assert!(msg.starts_with("adversary"), "{msg} threads {threads}");
                    // Nothing was told to the storage: a panic is not a verdict.
                    assert!(storage.files.2.lock().unwrap().is_empty());
                    assert!(storage.files.4.lock().unwrap().is_empty());
                }
            }
        }

        /// A pause at every point of a check leaves pieces from 0 to `next`
        /// checked and told in order, and the check resumed from there ends
        /// where a check that never paused ended.
        #[test]
        fn adversary_a_pause_anywhere_resumes_to_the_same_end() {
            let (m, p) = v1_torrent(&[("a", 40 * 1024 + 3), ("b", 7000), ("c", 20 * 1024)], 1024);
            let mut damaged = Files::new(&m, &p);
            damaged.0[0][5000] ^= 1;
            damaged.0[1].truncate(3000);
            let want = {
                let progress = AtomicU64::new(0);
                let have = FileOps::new(&m, &damaged, true)
                    .initial_check(1, &progress, &AtomicBool::new(false))
                    .unwrap();
                left(&m, &damaged, have, &progress).0
            };
            let total = m.lengths().total_pieces() as u64;
            for threads in [1, 2, 4] {
                for at_byte in (0..m.file_infos[0].len).step_by(4096) {
                    let mut files = Files::new(&m, &p);
                    files.0[0][5000] ^= 1;
                    files.0[1].truncate(3000);
                    let pause = std::sync::Arc::new(AtomicBool::new(false));
                    let mut storage = Recording::new(files);
                    storage.pause_at = Some((0, at_byte, pause.clone()));
                    let ops = FileOps::new(&m, &storage, true);
                    let progress = AtomicU64::new(0);
                    let first = ops
                        .initial_check_from(threads, &progress, &pause, None)
                        .unwrap();
                    let CheckOutcome::Paused(so_far) = first else {
                        panic!("did not pause at {at_byte}");
                    };
                    let next = so_far.next as u64;
                    assert!(next < total);
                    let failed = storage.files.2.lock().unwrap().clone();
                    let found = storage.files.4.lock().unwrap().clone();
                    let mut told: Vec<u32> = failed.iter().chain(&found).copied().collect();
                    told.sort_unstable();
                    assert_eq!(
                        told,
                        (0..u32::try_from(next).unwrap()).collect::<Vec<_>>(),
                        "at {at_byte} threads {threads}"
                    );
                    pause.store(false, Ordering::Relaxed);
                    storage.pause_at = None;
                    let ops = FileOps::new(&m, &storage, true);
                    let CheckOutcome::Done(have) = ops
                        .initial_check_from(
                            threads,
                            &progress,
                            &AtomicBool::new(false),
                            Some(so_far),
                        )
                        .unwrap()
                    else {
                        panic!("paused again");
                    };
                    let had: Vec<bool> = have
                        .iter()
                        .by_vals()
                        .take(usize::try_from(total).unwrap())
                        .collect();
                    assert_eq!(had, want, "at {at_byte} threads {threads}");
                }
            }
        }

        /// Adversary proofs, review 2 of the check reader (commits
        /// 0b5dd1957 and 67a459e30).
        mod review2 {
            use super::*;

            fn sliced(
                metadata: &TorrentMetadata,
                payload: &[u8],
                damage: &dyn Fn(&mut Files),
                memory: CheckMemory,
                threads: usize,
                wants: bool,
            ) -> (Left, usize) {
                let mut files = Files::new(metadata, payload);
                damage(&mut files);
                let mut storage = Recording::new(files);
                storage.wants = wants;
                let progress = AtomicU64::new(0);
                let (outcome, most) = FileOps::new(metadata, &storage, true).check_within(
                    memory,
                    threads,
                    &progress,
                    &AtomicBool::new(false),
                    None,
                );
                let CheckOutcome::Done(have) = outcome.unwrap() else {
                    panic!("paused");
                };
                (left(metadata, &storage.files, have, &progress), most)
            }

            /// Pieces a little longer than a slice: each read is a full
            /// slice and a short one. A spare buffer of a short slice is
            /// grown to a full one without asking for room.
            #[test]
            fn adversary2_pieces_just_past_a_slice_stay_within_the_stated_memory() {
                const PIECE: usize = 1024;
                let (metadata, payload) = v1_torrent(&[("a", 300 * PIECE)], PIECE);
                let mut seen = Vec::new();
                for slice in [600, 768, 1000] {
                    let memory = CheckMemory {
                        slice,
                        ahead: 3 * PIECE,
                    };
                    for threads in [2, 3, 4, 9] {
                        let (l, most) =
                            sliced(&metadata, &payload, &|_| {}, memory, threads, false);
                        assert!(l.0.iter().all(|h| *h));
                        seen.push((slice, threads, most, memory.ahead + slice));
                    }
                }
                println!("ADV2 most held (slice, threads, most, stated bound): {seen:?}");
                assert!(
                    seen.iter().all(|(_, _, most, bound)| most <= bound),
                    "{seen:?}"
                );
            }

            /// Wanted pieces larger than the whole read-ahead, mixed with
            /// unwanted ones of the same size: no hang, the verdicts of the
            /// old check, and the bytes held.
            #[test]
            fn adversary2_wanted_pieces_larger_than_the_memory_mixed_with_others() {
                const PIECE: usize = 16384;
                let (metadata, payload) = v1_torrent(&[("a", 20 * PIECE)], PIECE);
                let memory = CheckMemory {
                    slice: 1024,
                    ahead: 4096,
                };
                let mut damaged = payload.clone();
                damaged[4 * PIECE + 3] ^= 1;
                damaged[10 * PIECE + 3] ^= 1;
                let mut seen = Vec::new();
                for payload in [&payload, &damaged] {
                    let want = reference(&metadata, payload, &|_: &mut Files| {}, 1);
                    for threads in [1, 2, 4, 9] {
                        let mut storage = Recording::new(Files::new(&metadata, payload));
                        storage.wants_in = Some((3..6, u32::try_from(PIECE).unwrap()));
                        let progress = AtomicU64::new(0);
                        let (outcome, most) = FileOps::new(&metadata, &storage, true).check_within(
                            memory,
                            threads,
                            &progress,
                            &AtomicBool::new(false),
                            None,
                        );
                        let CheckOutcome::Done(have) = outcome.unwrap() else {
                            panic!("paused");
                        };
                        assert_eq!(
                            left(&metadata, &storage.files, have, &progress),
                            want,
                            "threads {threads}"
                        );
                        seen.push((threads, most));
                        assert!(most <= PIECE + memory.ahead + memory.slice, "{most}");
                    }
                }
                println!("ADV2 wanted 16 KiB pieces, 4 KiB ahead: (threads, most) {seen:?}");
            }

            /// Every damage of every file, slices and read-aheads smaller
            /// than, equal to and larger than a piece, read-ahead below one
            /// slice, the storage wanting none or all: the old check's
            /// verdicts, and no hang.
            #[test]
            fn adversary2_every_damage_over_odd_memories_gets_the_old_verdicts() {
                let (metadata, payload) =
                    v1_torrent(&[("a", 100), ("b", 9 * 16384 + 5), ("c", 30_000)], 16384);
                let whole: Damage = ("whole".into(), Box::new(|_: &mut Files| {}));
                let damages = every_file_damage(&metadata, |_| true);
                let memories = [
                    CheckMemory {
                        slice: 1000,
                        ahead: 500,
                    },
                    CheckMemory {
                        slice: 1000,
                        ahead: 1000,
                    },
                    CheckMemory {
                        slice: 5000,
                        ahead: 15_000,
                    },
                    CheckMemory {
                        slice: 4096,
                        ahead: 16384,
                    },
                    CheckMemory {
                        slice: 32768,
                        ahead: 16384,
                    },
                    CHECK_MEMORY,
                ];
                let mut runs = 0;
                for (name, damage) in std::iter::once(&whole).chain(&damages) {
                    let want = reference(&metadata, &payload, damage, 1);
                    for memory in memories {
                        for threads in [1, 2, 3, 5, 9] {
                            for wants in [false, true] {
                                let (got, most) =
                                    sliced(&metadata, &payload, damage, memory, threads, wants);
                                assert_eq!(
                                    got, want,
                                    "{name} slice {} ahead {} threads {threads} wants {wants}",
                                    memory.slice, memory.ahead
                                );
                                if !wants {
                                    assert!(
                                        most <= memory.ahead.max(memory.slice) + memory.slice,
                                        "{name} {most} slice {} ahead {}",
                                        memory.slice,
                                        memory.ahead
                                    );
                                }
                                runs += 1;
                            }
                        }
                    }
                }
                println!("ADV2 odd memories: {runs} checks");
            }

            /// A pause raised while the second slice of a piece is read
            /// stops the check after that piece, on any number of threads,
            /// and the check goes on from there to the end an unpaused one
            /// reaches, with a damaged piece before and after the pause.
            #[test]
            fn adversary2_a_pause_mid_slice_resumes_to_the_same_end() {
                const PIECE: usize = 16384;
                let (metadata, mut payload) = v1_torrent(&[("a", 20 * PIECE)], PIECE);
                payload[2 * PIECE + 1] ^= 1;
                payload[15 * PIECE + 1] ^= 1;
                let memory = CheckMemory {
                    slice: 5000,
                    ahead: 15_000,
                };
                let want = reference(&metadata, &payload, &|_: &mut Files| {}, 1).0;
                for threads in [1, 2, 4] {
                    let pause = std::sync::Arc::new(AtomicBool::new(false));
                    let mut storage = Recording::new(Files::new(&metadata, &payload));
                    storage.pause_at = Some((0, (7 * PIECE + 6000) as u64, pause.clone()));
                    let ops = FileOps::new(&metadata, &storage, true);
                    let progress = AtomicU64::new(0);
                    let (outcome, _) = ops.check_within(memory, threads, &progress, &pause, None);
                    let CheckOutcome::Paused(at) = outcome.unwrap() else {
                        panic!("not paused on {threads}");
                    };
                    assert_eq!(at.next, 8, "threads {threads}");
                    let (outcome, _) = ops.check_within(
                        memory,
                        threads,
                        &progress,
                        &AtomicBool::new(false),
                        Some(at),
                    );
                    let CheckOutcome::Done(have) = outcome.unwrap() else {
                        panic!("paused again");
                    };
                    let had: Vec<bool> = have.iter().by_vals().take(20).collect();
                    assert_eq!(had, want, "threads {threads}");
                }
            }

            /// A pure v2 piece whose file's piece layer is not held reads
            /// whole and good, and the single-piece check calls it Unknown:
            /// validate_fastresume (initializing.rs) now takes only Passed,
            /// so resume data naming it sends the torrent to a full check,
            /// which then marks the piece needed (`had` maps Unknown to
            /// false). Before 67a459e30 the same resume data was kept.
            #[test]
            fn adversary2_a_good_v2_piece_without_its_layer_is_not_passed_by_the_spot_check() {
                let mut t = two_files();
                t.layers = false;
                let metadata = metadata(&t.torrent_bytes());
                let payload = t.v1_payload();
                let files = Files::new(&metadata, &payload);
                let ops = FileOps::new(&metadata, &files, false);
                let piece = metadata.lengths().validate_piece_index(1).unwrap();
                let verdict = ops.check_piece_whole(piece).unwrap();
                assert_eq!(verdict, Verdict::Unknown);
                assert!(
                    ops.check_piece_whole(piece).is_ok(),
                    "the old test passed it"
                );
                assert!(!matches!(ops.check_piece_whole(piece), Ok(Verdict::Passed)));
            }

            /// The same growth, step by step and with no thread: two spare
            /// buffers of a short slice are grown to full slices while the
            /// check already holds all it may.
            #[test]
            fn adversary2_a_short_spare_is_grown_past_the_memory() {
                let ahead = ReadAhead {
                    line: Mutex::default(),
                    read: Condvar::new(),
                    room: Condvar::new(),
                    idle: AtomicUsize::new(0),
                    most: 2,
                    memory: CheckMemory {
                        slice: 1000,
                        ahead: 3000,
                    },
                    failed: AtomicBool::new(false),
                };
                let short_a = ahead.buffer(24, true);
                let short_b = ahead.buffer(24, true);
                let _full_a = ahead.buffer(1000, true);
                let _full_b = ahead.buffer(1000, true);
                let _rest = ahead.buffer(952, true);
                assert_eq!(ahead.line.lock().held, 3000);
                ahead.done_with(short_a);
                ahead.done_with(short_b);
                // The short spares are let go, not grown: the reader's
                // own buffer is the one slice past `ahead`. A buffer for
                // a hasher would wait here for room.
                let _own = ahead.buffer(1000, false);
                let line = ahead.line.lock();
                assert!(line.spare.is_empty());
                assert_eq!(line.held, 2952 + 1000);
            }

            /// The default memory scaled down 256 times (a slice of 1 KiB,
            /// 64 slices ahead), over pieces of one and a half slices, as a
            /// v1 torrent of 384 KiB pieces reads at the default: how far
            /// past `ahead` plus one slice the check goes.
            #[test]
            fn adversary2_pieces_of_a_slice_and_a_half_at_the_default_ratio() {
                const PIECE: usize = 1536;
                let (metadata, payload) = v1_torrent(&[("a", 2000 * PIECE)], PIECE);
                let memory = CheckMemory {
                    slice: 1024,
                    ahead: 64 * 1024,
                };
                let mut seen = Vec::new();
                for threads in [4, 9, 16] {
                    for _ in 0..3 {
                        let (l, most) =
                            sliced(&metadata, &payload, &|_| {}, memory, threads, false);
                        assert!(l.0.iter().all(|h| *h));
                        seen.push((threads, most));
                    }
                }
                println!(
                    "ADV2 slice and a half (threads, most), stated bound {}: {seen:?}",
                    memory.ahead + memory.slice
                );
                assert!(
                    seen.iter()
                        .all(|(_, most)| *most <= memory.ahead + memory.slice)
                );
            }
        }
    }
}
