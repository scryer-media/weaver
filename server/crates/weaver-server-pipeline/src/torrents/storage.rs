// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The torrent session's storage. A piece's chunks are kept in memory while
// it is in flight, read back from there while the session checks its hash,
// and once it verified the whole buffer goes to the torrent's store, which
// hands it to the writer of the volume its files are on and, once written,
// hands the tee its archive shares. Until then the piece is read from
// memory; after that a read of the piece is a read of those files.
//
// A piece's buffer is held against the resident budget every job shares from
// the moment the first block of it arrives from the peer that owns it until
// it verifies, fails its hash, or the storage goes. Its bytes are also
// counted on the volume it is written to, from the same moment until its
// writer has written them. A piece the budget or the volume has no room for
// is refused, and the connection that sent the block stops reading until
// the room this storage, or the volume's writer, rings when it lets bytes
// go. The room is held from the first block; the memory is not: a piece's
// pages are backed only as its blocks arrive (`PieceBuffer`).
//
// A piece no peer owns, given back by one that left or stopped for room, or
// of which a block arrives from a peer that does not own it, is written
// block by block, as below: the blocks it holds in memory are written out,
// and its whole-piece room goes back once they are (`Inner::spill`). Such a
// piece holds no room of its own: each block of it holds one block's room
// for its write alone, taken before it goes to the disk threads and given
// back as the write returns (`hold_block`). So every room held for good is a
// piece in memory that its owner, reading, fills; all else is a write under
// way, which gives its room back as it ends.
//
// A torrent whose piece is larger than the whole resident budget could
// never hold one, and is written block by block instead, as libtorrent
// writes every piece: each block goes to the volume's writer as it
// arrives, the piece is hashed back from its files on the volume's read
// turns, and a piece that fails is fetched again. The choice is made once,
// as the storage's maker is made, from the piece's length and the budget
// then; nothing records it. Such a piece holds no room of its own while it
// arrives: each block holds the room of its own write, taken where its
// connection can wait for it, its blocks are written one at a time, and its
// bytes are counted on its volume as each block's write fits.

use std::collections::HashMap;
use std::io::IoSlice;
use std::ops::Range;
use std::path::Path;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, PoisonError};

use anyhow::{Context, bail};
use bytes::Bytes;
use tokio::sync::Notify;
use weaver_graph::holds::{Reserve, ResidentWake};
use weaver_torrent::core::lengths::ValidPieceIndex;
use weaver_torrent::storage::{
    BlockRoom, BoxStorageFactory, GivesRoomBack, HeldRoom, ReadTurns, StorageFactory,
    StorageFactoryExt, StoredPiece, TorrentStorage,
};
use weaver_torrent::{ManagedTorrentShared, TorrentMetadata};

use super::intake::Holds;
use super::layout::{PieceLayout, layout_of};
use super::writes::{Done, Paid, VolumeRoom};

#[cfg(test)]
mod tests;

// Where verified pieces go and the files are read from.
pub(crate) trait Pieces: Send + Sync {
    // A piece verified: `held` is the whole piece, as it was hashed. Stored
    // before this returns.
    fn verified(&self, piece: u32, held: &Bytes) -> anyhow::Result<()>;
    // The same, without waiting for the disk: `done` hears what storing it
    // came to, on whatever thread it ended on, or is dropped untold when
    // the writes were given up. `paid` is what the volume counted for the
    // piece at its start, handed to the writes that pay with it.
    fn store(self: Arc<Self>, piece: u32, held: Bytes, paid: Option<Paid>, done: Done) {
        drop(paid);
        done(self.verified(piece, &held));
    }
    // Count a piece of `bytes` against the volume it is written to, before
    // it starts. Refused when the volume has no room: `room` is rung once
    // it has.
    fn reserve(&self, _bytes: u64, _room: &Arc<Notify>) -> VolumeRoom {
        VolumeRoom::Unmetered
    }
    // The session's run ended: what is queued to be written is given up.
    fn stop_writes(&self) {}
    // The torrent went away: nothing of it is written again.
    fn retired(&self) -> bool {
        false
    }
    // A block of a piece too large to hold, written as it arrives: `bytes`
    // at `offset` of `file`, `at` bytes into piece `piece`. Written before
    // this returns, on a thread that may wait on a disk.
    fn write_block(
        &self,
        piece: u32,
        _file: usize,
        _offset: u64,
        _at: u64,
        _bytes: &[u8],
    ) -> anyhow::Result<()> {
        bail!("piece {piece} is written block by block where only whole pieces are stored")
    }
    // A piece written block by block verified, every block of it on disk:
    // it is had once this returns. `room` is what it held while it
    // arrived, given back once nothing reads the piece back for it.
    fn sealed(self: Arc<Self>, _piece: u32, room: HeldRoom) -> anyhow::Result<()> {
        drop(room);
        Ok(())
    }
    // Whether its pieces are written block by block, when it decided so
    // itself as it was made: none leaves it to the storage.
    fn writes_blocks(&self) -> Option<bool> {
        None
    }
    // A piece failed its hash, fetched or checked on disk: it is not had.
    // True when it was.
    fn failed(&self, piece: u32) -> anyhow::Result<bool>;
    // A check of every piece began: no piece is had until the check finds
    // it. True when any was had.
    fn checking(&self) -> anyhow::Result<bool>;
    // The check found the piece: it is had. True when it was not.
    fn found(&self, piece: u32) -> anyhow::Result<bool>;
    // Whether the piece's bytes, as a check of the files hashes them, are
    // wanted: they then come to `hashed`, before the check says `found`.
    fn wants_hashed(&self, _piece: u32) -> bool {
        false
    }
    // The piece as a check of the files read and hashed it, whole.
    fn hashed(&self, _piece: u32, _held: &Bytes) {}
    // The file's bytes from `offset` as it holds them on disk.
    fn read(&self, file: usize, offset: u64, out: &mut [u8]) -> std::io::Result<()>;
    // The file's `len` bytes from `offset` are about to be read in order.
    fn will_read(&self, _file: usize, _offset: u64, _len: u64) {}
    // The read turns of the volume the files are on, asked when a run of
    // the torrent starts and when its files move.
    fn read_turns(&self) -> ReadTurns {
        ReadTurns::private()
    }
    // Resident room the storage held was given back.
    fn room_returned(&self) {}
    // `run` on a thread that may wait on a disk, not this one.
    fn aside(&self, run: Box<dyn FnOnce() + Send>) {
        run();
    }
    // `outs` filled one after the other from the file's bytes at `offset`
    // as it holds them on disk.
    fn readv(&self, file: usize, mut offset: u64, outs: &mut [&mut [u8]]) -> std::io::Result<()> {
        for out in outs.iter_mut() {
            self.read(file, offset, out)?;
            offset += out.len() as u64;
        }
        Ok(())
    }
    // The file made at least `length` long.
    fn grow(&self, file: usize, length: u64) -> std::io::Result<()>;
    // A piece the session counted made the file whole, told once for it.
    fn file_whole(&self, _file: usize) {}
}

// A piece in flight, the length of its piece: its chunks as they arrive,
// then the whole piece while it goes to the store. Only a piece still being
// written holds resident room: a verified one's bytes are on their way to
// disk. The room its volume counted for it is handed on with it.
enum Flight {
    Writing(Arc<Writing>, Option<Paid>),
    Verified(Bytes),
    // A piece written block by block: its blocks are on disk as they land,
    // one at a time under its lock, each holding its own room while it is
    // written; the piece holds none. The lock holds what became of the
    // blocks it held in memory when it was given back, written aside.
    Blocks(Arc<Mutex<Lost>>),
}

// A write of blocks a piece given back held that failed: the kind of its
// error, when the disk named one, and the error. The piece is not on disk
// whole, whatever else of it arrives.
type Lost = Option<(Option<std::io::ErrorKind>, String)>;

fn lost(error: &anyhow::Error) -> Lost {
    let kind = error.chain().find_map(|cause| {
        cause
            .downcast_ref::<std::io::Error>()
            .map(std::io::Error::kind)
    });
    Some((kind, format!("{error:#}")))
}

// A piece being written: its blocks land here, and it is hashed here once
// whole, under its own lock, so hashing one piece holds up no other's
// blocks.
struct Writing {
    // The room it holds.
    len: usize,
    bytes: std::sync::RwLock<PieceBuffer>,
    // The ranges of the piece its blocks filled, in order and apart, each
    // added under the write lock of `bytes` once its bytes are there.
    landed: Mutex<Vec<Range<u32>>>,
    // Its bytes went to its files: the piece is written block by block
    // from here, and a block that finds this goes there too.
    spilled: AtomicBool,
}

impl Writing {
    fn landed(&self) -> std::sync::MutexGuard<'_, Vec<Range<u32>>> {
        self.landed.lock().unwrap_or_else(PoisonError::into_inner)
    }

    // `range` of the piece filled: merged with the ranges it touches. A
    // piece's blocks mostly arrive in order, so this mostly grows the last.
    fn land(&self, range: Range<u32>) {
        let mut landed = self.landed();
        let at = landed.partition_point(|r| r.end < range.start);
        let upto = landed[at..].partition_point(|r| r.start <= range.end) + at;
        if at == upto {
            landed.insert(at, range);
            return;
        }
        let start = range.start.min(landed[at].start);
        let end = range.end.max(landed[upto - 1].end);
        landed.drain(at + 1..upto);
        landed[at] = start..end;
    }

    // Whether every byte of the piece landed.
    fn whole(&self) -> bool {
        matches!(self.landed().as_slice(), [only] if only.start == 0 && only.end as usize == self.len)
    }

    fn read(&self) -> std::sync::RwLockReadGuard<'_, PieceBuffer> {
        self.bytes.read().unwrap_or_else(PoisonError::into_inner)
    }

    fn write(&self) -> std::sync::RwLockWriteGuard<'_, PieceBuffer> {
        self.bytes.write().unwrap_or_else(PoisonError::into_inner)
    }
}

// The memory of a piece in flight, the piece's length, one run of bytes.
//
// A piece longer than one block is an anonymous mapping: the OS gives it
// address space at the grant and a page only once a block is written to
// it, and takes the pages back when the last of its bytes is dropped,
// whether it was written, failed or given up. A piece granted and not yet
// arriving takes no memory: its room is held, its pages are not. A
// buffer from the allocator, zeroed whole, is taken whole at the grant: a
// recycled one is written over end to end before its first block arrives
// (mimalloc `_mi_page_malloc_zero`, alloc.c:88-89 in v3).
//
// A piece of one block arrives whole at once, and the rare mapping the OS
// refuses falls back to the allocator: that buffer is held whole from the
// grant, as every piece's was.
enum PieceBuffer {
    Mapped(memmap2::MmapMut),
    Heap(Vec<u8>),
}

impl Default for PieceBuffer {
    fn default() -> Self {
        Self::Heap(Vec::new())
    }
}

impl PieceBuffer {
    fn new(len: usize) -> Self {
        if len > weaver_torrent::core::constants::CHUNK_SIZE as usize
            && let Ok(mapped) = memmap2::MmapMut::map_anon(len)
        {
            return Self::Mapped(mapped);
        }
        Self::Heap(vec![0; len])
    }

    // The whole piece, handed on without a copy: the mapping goes with it.
    fn into_bytes(self) -> Bytes {
        match self {
            Self::Mapped(mapped) => Bytes::from_owner(mapped),
            Self::Heap(bytes) => Bytes::from(bytes),
        }
    }

    // The bytes of it the OS backs with memory now.
    #[cfg(all(test, unix))]
    fn resident(&self) -> u64 {
        match self {
            Self::Mapped(mapped) => resident_of(mapped),
            Self::Heap(bytes) => bytes.len() as u64,
        }
    }
}

impl std::ops::Deref for PieceBuffer {
    type Target = [u8];

    fn deref(&self) -> &[u8] {
        match self {
            Self::Mapped(mapped) => mapped,
            Self::Heap(bytes) => bytes,
        }
    }
}

impl std::ops::DerefMut for PieceBuffer {
    fn deref_mut(&mut self) -> &mut [u8] {
        match self {
            Self::Mapped(mapped) => mapped,
            Self::Heap(bytes) => bytes,
        }
    }
}

// The bytes of `region` the OS backs with memory now, a page at a time.
#[cfg(all(test, unix))]
pub(crate) fn resident_of(region: &[u8]) -> u64 {
    // SAFETY: sysconf reads a constant of the system.
    let page = usize::try_from(unsafe { libc::sysconf(libc::_SC_PAGESIZE) }).unwrap_or(4096);
    let start = region.as_ptr() as usize / page * page;
    let end = (region.as_ptr() as usize + region.len()).div_ceil(page) * page;
    let mut pages = vec![0u8; (end - start) / page];
    // SAFETY: the range covers `region`, which is mapped while it is
    // borrowed, and `pages` has one entry for each of its pages.
    let read = unsafe { libc::mincore(start as _, end - start, pages.as_mut_ptr().cast()) };
    assert_eq!(read, 0, "mincore failed");
    pages.iter().filter(|&&state: &&u8| state & 1 != 0).count() as u64 * page as u64
}

impl Flight {
    fn writing(len: usize, paid: Option<Paid>) -> Self {
        Self::Writing(
            Arc::new(Writing {
                len,
                bytes: std::sync::RwLock::new(PieceBuffer::new(len)),
                landed: Mutex::new(Vec::new()),
                spilled: AtomicBool::new(false),
            }),
            paid,
        )
    }

    // `out` filled from `at` of the piece; false when the piece has left.
    fn read(&self, at: usize, out: &mut [u8]) -> bool {
        let mut copy = |bytes: &[u8]| match bytes.get(at..at + out.len()) {
            Some(from) => {
                out.copy_from_slice(from);
                true
            }
            None => false,
        };
        match self {
            Self::Writing(writing, _) => copy(&writing.read()),
            Self::Verified(bytes) => copy(bytes),
            Self::Blocks(..) => false,
        }
    }

    fn held(&self) -> u64 {
        match self {
            Self::Writing(writing, _) => writing.len as u64,
            Self::Verified(_) => 0,
            Self::Blocks(_) => 0,
        }
    }

    // Whether the piece's bytes are read from memory: a piece written block
    // by block is read from its files.
    fn in_memory(&self) -> bool {
        !matches!(self, Self::Blocks(..))
    }
}

// The resident room storages hold now, and the most they held at once.
#[cfg(test)]
#[derive(Default, Debug)]
pub(crate) struct ResidentPeak {
    pub(crate) now: std::sync::atomic::AtomicU64,
    pub(crate) peak: std::sync::atomic::AtomicU64,
    // Run once, between a storage's first refusal and its last ask.
    pub(crate) refused: Mutex<Option<Refused>>,
}

#[cfg(test)]
pub(crate) struct Refused(pub(crate) Box<dyn FnOnce() + Send>);

#[cfg(test)]
impl std::fmt::Debug for Refused {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("Refused")
    }
}

struct Inner {
    layout: Arc<PieceLayout>,
    pieces: Arc<dyn Pieces>,
    // The pieces in flight, by index. Read for each block that lands, by
    // every connection of the torrent at once; changed once a piece.
    flight: std::sync::RwLock<HashMap<u32, Flight>>,
    holds: Holds,
    // What the session waits on for room, as it last asked.
    room: Mutex<Option<Arc<Notify>>>,
    // Whether its pieces are written block by block as they arrive.
    blocks: bool,
    // The resident room it holds now, and the most it held at once,
    // shared by every storage one factory made.
    #[cfg(test)]
    resident: Arc<ResidentPeak>,
}

// What the budget calls once room comes back anywhere, for the session
// waiting on `room`: one wake a session, however often it asks.
fn wake(room: &Arc<Notify>) -> (usize, ResidentWake) {
    let key = Arc::as_ptr(room) as usize;
    let room = Arc::clone(room);
    (key, Arc::new(move || room.notify_waiters()))
}

impl Inner {
    // `bytes` of resident room asked for: held when granted. Refused, the
    // session waiting on its room is rung once room comes back, whoever
    // gives it back: another torrent, or a job of any other kind.
    //
    // The last ask and the keeping of the wake are one step of the budget,
    // so room another holder gives back after it is always rung.
    fn reserve(&self, bytes: u64) -> bool {
        if self.try_reserve(bytes) {
            return true;
        }
        #[cfg(test)]
        if let Some(Refused(between)) = self
            .resident
            .refused
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take()
        {
            between();
        }
        let room = self
            .room
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone();
        let Some(room) = room else {
            return false;
        };
        let (key, wake) = wake(&room);
        let reserve =
            self.holds
                .accountant
                .reserve_resident_or_wake(self.holds.namespace, bytes, key, wake);
        self.granted(bytes, reserve)
    }

    // `bytes` of resident room asked for once: held when granted.
    fn try_reserve(&self, bytes: u64) -> bool {
        let reserve = self
            .holds
            .accountant
            .reserve_resident(self.holds.namespace, bytes);
        self.granted(bytes, reserve)
    }

    fn granted(&self, bytes: u64, reserve: Reserve) -> bool {
        let granted = matches!(reserve, Reserve::Granted);
        #[cfg(not(test))]
        let _ = bytes;
        #[cfg(test)]
        if granted {
            use std::sync::atomic::Ordering;
            let now = self.resident.now.fetch_add(bytes, Ordering::AcqRel) + bytes;
            self.resident.peak.fetch_max(now, Ordering::AcqRel);
        }
        granted
    }

    // Room given back unrung: the room was the asker's own.
    fn unreserve(&self, bytes: u64) {
        #[cfg(test)]
        self.resident
            .now
            .fetch_sub(bytes, std::sync::atomic::Ordering::AcqRel);
        self.holds
            .accountant
            .release_resident(self.holds.namespace, bytes);
    }

    // Room given back: the session waiting on it asks again, and so do the
    // torrent's read-backs waiting on it.
    fn release(&self, bytes: u64) {
        self.unreserve(bytes);
        if let Some(room) = &*self.room.lock().unwrap_or_else(PoisonError::into_inner) {
            room.notify_waiters();
        }
        if bytes > 0 {
            self.pieces.room_returned();
        }
    }

    fn flight(&self) -> std::sync::RwLockWriteGuard<'_, HashMap<u32, Flight>> {
        self.flight.write().unwrap_or_else(PoisonError::into_inner)
    }

    fn flight_read(&self) -> std::sync::RwLockReadGuard<'_, HashMap<u32, Flight>> {
        self.flight.read().unwrap_or_else(PoisonError::into_inner)
    }

    // A piece no peer owns, `writing` in memory, is written block by block
    // from here: the blocks it holds go to its files, as each block of such
    // a piece does, and its whole-piece room goes back once they are
    // written; it keeps none. Its other blocks, and a block that landed in
    // `writing` as this ran, go to its files after these, under the same
    // lock, each holding its own block's room for its write. A piece whole by now is left as it is: it is
    // checked from memory. Its writes wait on the disk, so this runs aside.
    //
    // A write that fails is kept on the piece, which the session then does
    // not check: it goes the way of a piece its store refused, fetched
    // again, and no sender is blamed for the disk's error.
    fn spill(&self, index: u32, writing: &Arc<Writing>) {
        // A torrent gone writes nothing: its pieces go with it, and their
        // room with its storage.
        if self.pieces.retired() {
            return;
        }
        let one = Arc::new(Mutex::new(None));
        let mut written = one.lock().unwrap_or_else(PoisonError::into_inner);
        // The map's lock is never held while the piece's own lock is waited
        // on: a whole piece is hashed under that lock, a hash long enough to
        // hold up every other piece's blocks.
        let (held, landed, paid) = loop {
            let mut flight = self.flight();
            let Some(entry) = flight.get_mut(&index) else {
                return;
            };
            if !matches!(entry, Flight::Writing(now, _) if Arc::ptr_eq(now, writing)) {
                return;
            }
            if writing.whole() {
                return;
            }
            let mut bytes = match writing.bytes.try_write() {
                Ok(bytes) => bytes,
                Err(std::sync::TryLockError::Poisoned(poisoned)) => poisoned.into_inner(),
                // A block landing, or the hash of the piece it made whole:
                // waited out with the map free, and the piece looked at again.
                Err(std::sync::TryLockError::WouldBlock) => {
                    drop(flight);
                    drop(writing.write());
                    continue;
                }
            };
            if writing.whole() {
                return;
            }
            let held = std::mem::take(&mut *bytes);
            let landed = std::mem::take(&mut *writing.landed());
            writing.spilled.store(true, Ordering::Release);
            let paid = match std::mem::replace(entry, Flight::Blocks(Arc::clone(&one))) {
                Flight::Writing(_, paid) => paid,
                _ => None,
            };
            break (held, landed, paid);
        };
        // What its volume counted for the whole piece goes back first: its
        // blocks are counted as each one's write fits.
        drop(paid);
        'spans: for span in self.layout.spans(index) {
            let share = span.at..span.at + span.range.len();
            for run in &landed {
                let from = share.start.max(u64::from(run.start));
                let to = share.end.min(u64::from(run.end));
                if from >= to {
                    continue;
                }
                let offset = span.range.start + (from - span.at);
                let bytes = &held[from as usize..to as usize];
                if let Err(error) = self
                    .pieces
                    .write_block(index, span.file, offset, from, bytes)
                {
                    tracing::warn!(
                        piece = index,
                        "a block of a piece given back was not written: {error:#}"
                    );
                    *written = lost(&error);
                    break 'spans;
                }
            }
        }
        drop(held);
        drop(written);
        self.release(writing.len as u64);
    }
}

impl GivesRoomBack for Inner {
    fn give_back(&self, bytes: u64) {
        self.release(bytes);
    }
}

impl Drop for Inner {
    fn drop(&mut self) {
        let flight = self
            .flight
            .get_mut()
            .unwrap_or_else(PoisonError::into_inner);
        let held = flight.values().map(Flight::held).sum();
        self.holds
            .accountant
            .release_resident(self.holds.namespace, held);
    }
}

#[derive(Clone)]
pub(crate) struct PieceStorage {
    inner: Arc<Inner>,
}

// Whether a torrent whose pieces are `piece_length` long writes them block
// by block: one whole piece would not fit in the resident budget at all.
pub(crate) fn writes_blocks(piece_length: u64, holds: &Holds) -> bool {
    piece_length > holds.accountant.budgets().resident_limit
}

impl PieceStorage {
    // The storage of `pieces`, choosing its write path as the factory does.
    #[cfg(test)]
    pub(crate) fn new(layout: Arc<PieceLayout>, pieces: Arc<dyn Pieces>, holds: Holds) -> Self {
        let blocks = pieces
            .writes_blocks()
            .unwrap_or_else(|| writes_blocks(layout.piece_length(), &holds));
        Self::made(
            layout,
            pieces,
            holds,
            blocks,
            #[cfg(test)]
            Arc::default(),
        )
    }

    fn made(
        layout: Arc<PieceLayout>,
        pieces: Arc<dyn Pieces>,
        holds: Holds,
        blocks: bool,
        #[cfg(test)] resident: Arc<ResidentPeak>,
    ) -> Self {
        Self {
            inner: Arc::new(Inner {
                layout,
                pieces,
                flight: std::sync::RwLock::new(HashMap::new()),
                holds,
                room: Mutex::new(None),
                blocks,
                #[cfg(test)]
                resident,
            }),
        }
    }

    // `between` runs once, the next time a reservation of its resident
    // room is first refused, before it asks the last time.
    #[cfg(test)]
    pub(crate) fn on_refusal(&self, between: impl FnOnce() + Send + 'static) {
        *self
            .inner
            .resident
            .refused
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = Some(Refused(Box::new(between)));
    }

    // The most resident room it held at once.
    #[cfg(test)]
    pub(crate) fn resident_peak(&self) -> u64 {
        self.inner
            .resident
            .peak
            .load(std::sync::atomic::Ordering::Acquire)
    }

    // The memory the OS backs the pieces still arriving with now, and how
    // many such pieces there are.
    #[cfg(all(test, unix))]
    pub(crate) fn arriving_memory(&self) -> (u64, usize) {
        let flight = self.flight();
        let arriving = flight.values().filter_map(|entry| match entry {
            Flight::Writing(writing, _) => Some(writing.read().resident()),
            _ => None,
        });
        arriving.fold((0, 0), |(bytes, pieces), held| (bytes + held, pieces + 1))
    }

    fn flight(&self) -> std::sync::RwLockWriteGuard<'_, HashMap<u32, Flight>> {
        self.inner.flight()
    }

    fn flight_read(&self) -> std::sync::RwLockReadGuard<'_, HashMap<u32, Flight>> {
        self.inner.flight_read()
    }

    // A file's range in the torrent's coordinates, and the one piece it
    // lies in, when it lies in one.
    fn place(&self, file_id: usize, offset: u64, len: usize) -> anyhow::Result<(u64, Option<u32>)> {
        let layout = &self.inner.layout;
        let file = layout
            .files()
            .nth(file_id)
            .with_context(|| format!("no file {file_id} in the torrent"))?;
        if offset + len as u64 > file.length {
            bail!("{len} bytes at {offset} run past the end of file {file_id}");
        }
        let start = layout
            .torrent_offset(file_id, offset)
            .with_context(|| format!("no file {file_id} in the torrent"))?;
        let piece = start / layout.piece_length();
        let last = (start + len as u64).saturating_sub(1) / layout.piece_length();
        Ok((start, (piece == last).then_some(piece as u32)))
    }

    // `buf` written at `offset` of the file: copied into the piece held for
    // it, or for a piece written block by block, to its file when `may_wait`
    // says the write may wait on a disk. False when it would have and may
    // not, having written nothing.
    fn write(
        &self,
        file_id: usize,
        offset: u64,
        buf: &[u8],
        may_wait: bool,
    ) -> anyhow::Result<bool> {
        if buf.is_empty() {
            return Ok(true);
        }
        let (start, piece) = self.place(file_id, offset, buf.len())?;
        let Some(piece) = piece else {
            bail!("a write of file {file_id} crosses a piece boundary");
        };
        let range = self.inner.layout.piece_range(piece);
        let at = (start - range.start) as usize;
        // Twice at most: a piece given back meanwhile is written block by
        // block from then on.
        loop {
            let landing = match self.flight_read().get(&piece) {
                Some(Flight::Writing(writing, _)) => Ok(Arc::clone(writing)),
                Some(Flight::Blocks(one)) => Err(Arc::clone(one)),
                _ => bail!("piece {piece} is written while the session holds no room for it"),
            };
            let held = match landing {
                Ok(writing) => writing,
                Err(_) if !may_wait => return Ok(false),
                // Outside the map's lock, one block of the piece at a time,
                // in the room the block holds for its write.
                Err(one) => {
                    let _one = one.lock().unwrap_or_else(PoisonError::into_inner);
                    self.inner
                        .pieces
                        .write_block(piece, file_id, offset, at as u64, buf)?;
                    return Ok(true);
                }
            };
            // Outside the map's lock: another piece's blocks land meanwhile.
            let mut bytes = held.write();
            if held.spilled.load(Ordering::Acquire) {
                continue;
            }
            let Some(to) = bytes.get_mut(at..at + buf.len()) else {
                bail!("piece {piece} is written after it completed");
            };
            to.copy_from_slice(buf);
            held.land(at as u32..(at + buf.len()) as u32);
            return Ok(true);
        }
    }

    // Room for a piece not in memory: its resident room, then its volume's.
    // A piece written block by block, as every piece of a storage that
    // writes blocks and a piece `blocks` names are, holds none, its blocks
    // holding theirs as they are written: its blocks are counted on the
    // volume as each one's write fits.
    fn start_piece(&self, index: u32, length: u64, room: &Arc<Notify>, blocks: bool) -> bool {
        if self.inner.blocks || blocks {
            // It holds no room of its own: each of its blocks holds the room
            // of its own write, taken where its connection can wait, so it
            // is never refused on a disk thread, where its block cannot.
            // What reads it back for the tee asks the budget itself.
            self.flight()
                .entry(index)
                .or_insert_with(|| Flight::Blocks(Arc::default()));
            return true;
        }
        // Refused, it may have started meanwhile, from a block on a disk
        // thread: it holds its room, and nothing waits for a ring.
        if !self.inner.reserve(length) {
            return self.flight_read().contains_key(&index);
        }
        // Then its volume, outside the map's lock: the volume's room is
        // rung by its writer, which takes no lock of the session's.
        let paid = match self.inner.pieces.reserve(length, room) {
            VolumeRoom::Unmetered => None,
            VolumeRoom::Held(paid) => Some(paid),
            VolumeRoom::Refused => {
                // Not rung: the room given back is the session's own, and
                // the volume's writer rings once it has written enough.
                self.inner.unreserve(length);
                return false;
            }
        };
        let mut flight = self.flight();
        if flight.contains_key(&index) {
            // Granted meanwhile: the room asked for here goes back.
            drop(flight);
            drop(paid);
            self.inner.release(length);
            return true;
        }
        flight.insert(index, Flight::writing(length as usize, paid));
        true
    }

    // A piece written block by block verified: had once the store says so,
    // with the room it held going to what reads it back for the tee.
    fn seal(&self, index: u32) -> anyhow::Result<()> {
        let room = match self.flight().remove(&index) {
            Some(entry) => entry.held(),
            None => bail!("piece {index} completed with nothing written"),
        };
        let room = HeldRoom::new(room, Arc::clone(&self.inner) as _);
        Arc::clone(&self.inner.pieces).sealed(index, room)
    }

    // Room for a piece of which a block arrived, written block by block
    // when `blocks` says so. A piece given back by a peer that left keeps
    // what was written and the room it holds.
    fn reserve(&self, piece: ValidPieceIndex, room: &Arc<Notify>, blocks: bool) -> bool {
        // Kept before the budget is asked, so a release after a refusal rings
        // the session that was refused.
        *self
            .inner
            .room
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = Some(Arc::clone(room));
        let index = piece.get();
        let length = self.inner.layout.piece_range(index).len();
        // Looked at under the map's lock, released before the piece starts.
        let kept = self
            .flight()
            .get(&index)
            .map(|entry| matches!(entry, Flight::Writing(_, None)));
        match kept {
            // Kept from a run that ended, which gave back what its volume
            // counted: counted again, or not taken up.
            Some(true) => {}
            Some(false) => return true,
            None => return self.start_piece(index, length, room, blocks),
        }
        let paid = match self.inner.pieces.reserve(length, room) {
            VolumeRoom::Unmetered => return true,
            VolumeRoom::Held(paid) => paid,
            VolumeRoom::Refused => return false,
        };
        if let Some(Flight::Writing(_, held @ None)) = self.flight().get_mut(&index) {
            *held = Some(paid);
        }
        true
    }
}

impl TorrentStorage for PieceStorage {
    fn init(&mut self, _: &ManagedTorrentShared, metadata: &TorrentMetadata) -> anyhow::Result<()> {
        let theirs = layout_of(&metadata.info)?;
        if theirs != *self.inner.layout {
            bail!("the torrent is not the one its store was made for");
        }
        Ok(())
    }

    fn pread_exact(&self, file_id: usize, offset: u64, buf: &mut [u8]) -> anyhow::Result<()> {
        if buf.is_empty() {
            return Ok(());
        }
        let (start, piece) = self.place(file_id, offset, buf.len())?;
        if let Some(piece) = piece
            && let Some(held) = self.flight().get(&piece)
            && held.in_memory()
        {
            let at = (start - self.inner.layout.piece_range(piece).start) as usize;
            if !held.read(at, buf) {
                bail!("piece {piece} left memory as it was read");
            }
            return Ok(());
        }
        if self
            .inner
            .layout
            .file(file_id)
            .is_some_and(|file| file.padding)
        {
            // Padding is zeros by definition.
            buf.fill(0);
            return Ok(());
        }
        self.inner
            .pieces
            .read(file_id, offset, buf)
            .with_context(|| format!("{} bytes at {offset} of file {file_id}", buf.len()))
    }

    fn will_read(&self, file_id: usize, offset: u64, len: u64) {
        self.inner.pieces.will_read(file_id, offset, len);
    }

    fn read_turns(&self) -> ReadTurns {
        self.inner.pieces.read_turns()
    }

    // A piece an upload holds whole is held against the same resident
    // budget as a piece in flight, before it is read; the session waiting
    // for room is rung when it goes back.
    fn hold_whole_piece(&self, bytes: u64) -> Option<HeldRoom> {
        self.inner
            .try_reserve(bytes)
            .then(|| HeldRoom::new(bytes, Arc::clone(&self.inner) as _))
    }

    // A block of a piece given back, or started by a peer that does not own
    // it, or of a storage that writes every piece block by block, holds its
    // own room while it is written: taken here, where its connection can
    // wait for it, so no disk thread drops a block for want of room.
    fn hold_block(&self, bytes: u64, room: &Arc<Notify>) -> BlockRoom {
        *self
            .inner
            .room
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = Some(Arc::clone(room));
        if self.inner.reserve(bytes) {
            BlockRoom::Held(HeldRoom::new(bytes, Arc::clone(&self.inner) as _))
        } else {
            BlockRoom::Refused
        }
    }

    // A look at the budget every job shares, the session rung once room
    // comes back when there is none.
    fn room_for(&self, bytes: u64, room: &Arc<Notify>) -> bool {
        let (key, wake) = wake(room);
        self.inner
            .holds
            .accountant
            .resident_fits_or_wake(bytes, key, wake)
    }

    // A run of a file's bytes on disk is read in one go; one with any of
    // its pieces held in memory, or of padding, a buffer at a time.
    fn preadv_exact(
        &self,
        file_id: usize,
        offset: u64,
        bufs: &mut [&mut [u8]],
    ) -> anyhow::Result<()> {
        let len: usize = bufs.iter().map(|buf| buf.len()).sum();
        let layout = &self.inner.layout;
        let on_disk = len > 0
            && bufs.len() > 1
            && layout.file(file_id).is_some_and(|file| !file.padding)
            && {
                let (start, _) = self.place(file_id, offset, len)?;
                let first = start / layout.piece_length();
                let last = (start + len as u64 - 1) / layout.piece_length();
                let flight = self.flight();
                !(first..=last)
                    .any(|piece| flight.get(&(piece as u32)).is_some_and(Flight::in_memory))
            };
        if !on_disk {
            let mut at = offset;
            for buf in bufs.iter_mut() {
                self.pread_exact(file_id, at, buf)?;
                at += buf.len() as u64;
            }
            return Ok(());
        }
        self.inner
            .pieces
            .readv(file_id, offset, bufs)
            .with_context(|| format!("{len} bytes at {offset} of file {file_id}"))
    }

    fn pwrite_all(&self, file_id: usize, offset: u64, buf: &[u8]) -> anyhow::Result<()> {
        self.write(file_id, offset, buf, true)?;
        Ok(())
    }

    // A block is copied into the piece held for it on the runtime; one of
    // a piece written block by block, as a piece given back or started by
    // a peer that does not own it is, is left for its writer to land where
    // it may wait.
    fn pwrite_held(
        &self,
        file_id: usize,
        offset: u64,
        bufs: [IoSlice<'_>; 2],
    ) -> anyhow::Result<bool> {
        let mut at = offset;
        for buf in &bufs {
            if !self.write(file_id, at, buf, false)? {
                return Ok(false);
            }
            at += buf.len() as u64;
        }
        Ok(true)
    }

    // A piece given back whose blocks failed to reach its files is not
    // checked: it is refused, for the session to fetch it again. Waits for
    // the writes of the blocks it held, under way aside.
    fn piece_written(&self, piece: ValidPieceIndex) -> anyhow::Result<()> {
        let one = match self.flight().get(&piece.get()) {
            Some(Flight::Blocks(one)) => Arc::clone(one),
            _ => return Ok(()),
        };
        let lost = one.lock().unwrap_or_else(PoisonError::into_inner).clone();
        match lost {
            None => Ok(()),
            Some((Some(kind), error)) => Err(anyhow::Error::from(std::io::Error::new(kind, error))
                .context(format!("piece {} lost blocks it held", piece.get()))),
            Some((None, error)) => bail!("piece {} lost blocks it held: {error}", piece.get()),
        }
    }

    // A block is only ever copied into the piece held for it, but for a
    // piece too large to hold, whose blocks go to its files.
    fn writes_to_memory(&self) -> bool {
        !self.inner.blocks
    }

    // A piece given back, or started by a peer that did not own it, is
    // written block by block too.
    fn writes_piece_to_memory(&self, piece: ValidPieceIndex) -> bool {
        !self.inner.blocks
            && !matches!(
                self.flight_read().get(&piece.get()),
                Some(Flight::Blocks(..))
            )
    }

    fn pwrite_all_vectored(
        &self,
        file_id: usize,
        offset: u64,
        bufs: [IoSlice<'_>; 2],
    ) -> anyhow::Result<usize> {
        let mut at = offset;
        for buf in &bufs {
            self.pwrite_all(file_id, at, buf)?;
            at += buf.len() as u64;
        }
        Ok((at - offset) as usize)
    }

    // The service removes a torrent's files, with what unpacking made of
    // them: the session removes nothing.
    fn remove_file(&self, _: usize, _: &Path) -> anyhow::Result<()> {
        Ok(())
    }

    fn remove_directory_if_empty(&self, _: &Path) -> anyhow::Result<()> {
        Ok(())
    }

    // A wanted file whole on disk at its length once checked, as
    // libtorrent's storage makes it.
    fn ensure_file_length(&self, file_id: usize, length: u64) -> anyhow::Result<()> {
        Ok(self.inner.pieces.grow(file_id, length)?)
    }

    // The run ends: the writes of the pieces it handed over and has not
    // heard back on are given up, as libtorrent aborts the jobs of a
    // storage it closes. Those pieces are fetched again in a later run. A
    // piece handed over stays here, verified, until it is written, so a run
    // with none such has nothing queued to give up.
    // What its volume counted for the pieces it keeps in memory goes back
    // too: a stopped torrent holds no room its volume's other writers wait
    // for. A piece is counted again as the next run takes it up.
    fn take(&self) -> anyhow::Result<Box<dyn TorrentStorage>> {
        let mut handed = false;
        let paid: Vec<Paid> = self
            .flight()
            .values_mut()
            .filter_map(|entry| match entry {
                Flight::Writing(_, paid) => paid.take(),
                Flight::Verified(_) => {
                    handed = true;
                    None
                }
                Flight::Blocks(..) => None,
            })
            .collect();
        // Outside the map's lock: a piece given up leaves the map as it goes.
        if handed {
            self.inner.pieces.stop_writes();
        }
        drop(paid);
        Ok(Box::new(self.clone()))
    }

    // Room for a piece the first block of which arrived from the peer that
    // owns it: the whole piece's. A piece in flight here already holds its
    // room.
    fn reserve_piece(&self, piece: ValidPieceIndex, room: &Arc<Notify>) -> bool {
        self.reserve(piece, room, false)
    }

    // Room for a piece the first block of which arrived from a peer that
    // does not own it: one block's, for it is written block by block.
    fn reserve_unowned_piece(&self, piece: ValidPieceIndex, room: &Arc<Notify>) -> bool {
        self.reserve(piece, room, true)
    }

    // A piece given back holds no whole-piece room past the writes of the
    // blocks it took, which go to its files aside.
    fn on_piece_requeued(&self, piece: ValidPieceIndex) {
        let index = piece.get();
        let writing = match self.flight().get(&index) {
            Some(Flight::Writing(writing, _)) => Arc::clone(writing),
            _ => return,
        };
        let inner = Arc::clone(&self.inner);
        self.inner
            .pieces
            .aside(Box::new(move || inner.spill(index, &writing)));
    }

    // Paused by the operator: each piece held in memory unfinished is
    // written to its files aside, as a piece given back is, and gives back
    // its room; the session keeps its blocks, and the next run takes the
    // rest of it block by block. One held whole for a check its run ended
    // before is fetched again, so it goes now with its room. A paused
    // torrent holds no resident room another job waits for.
    fn on_paused(&self) {
        let mut spills = Vec::new();
        let mut whole = 0;
        self.flight().retain(|index, entry| match entry {
            Flight::Writing(writing, _) if writing.whole() => {
                whole += writing.len as u64;
                false
            }
            Flight::Writing(writing, _) => {
                spills.push((*index, Arc::clone(writing)));
                true
            }
            Flight::Verified(_) | Flight::Blocks(..) => true,
        });
        // Outside the map's lock: room let go rings whoever waits on it.
        self.inner.release(whole);
        for (index, writing) in spills {
            let inner = Arc::clone(&self.inner);
            self.inner
                .pieces
                .aside(Box::new(move || inner.spill(index, &writing)));
        }
    }

    // The piece goes to the store, and only then does it stop being read
    // from memory, so no read finds it in neither place. Its room is given
    // back first: it is on its way to disk.
    //
    // A piece written block by block is on disk already: it is had once the
    // store says so, and the room it held goes with it to what reads it
    // back for the tee.
    fn on_piece_completed(&self, piece: ValidPieceIndex) -> anyhow::Result<()> {
        let index = piece.get();
        if self.inner.blocks || !self.writes_piece_to_memory(piece) {
            return self.seal(index);
        }
        let (held, released) = {
            let mut flight = self.flight();
            let Some(entry) = flight.get_mut(&index) else {
                bail!("piece {index} completed with nothing written");
            };
            let released = entry.held();
            let held = match std::mem::replace(entry, Flight::Verified(Bytes::new())) {
                Flight::Writing(writing, _) => std::mem::take(&mut *writing.write()).into_bytes(),
                Flight::Verified(bytes) => bytes,
                Flight::Blocks(..) => Bytes::new(),
            };
            *entry = Flight::Verified(held.clone());
            (held, released)
        };
        self.inner.release(released);
        let verified = self.inner.pieces.verified(index, &held);
        self.flight().remove(&index);
        verified
    }

    // The piece goes to the store, which hands it to its volume's writer
    // and returns: the session's hashing thread does not wait for the disk.
    // Its resident room is given back now, before the write, as it is on its
    // way to disk; the room its volume counted goes with it, and its writer
    // gives that back as it writes it. Until the store says it is written,
    // the piece stays here and every read of it is a read of memory, so no
    // read finds it in neither place.
    fn store_verified_piece(&self, piece: ValidPieceIndex, stored: StoredPiece) {
        let index = piece.get();
        // One written block by block is on disk already.
        if !self.writes_piece_to_memory(piece) {
            stored.stored(self.seal(index));
            return;
        }
        let taken = {
            let mut flight = self.flight();
            flight.get_mut(&index).map(|entry| {
                let released = entry.held();
                match std::mem::replace(entry, Flight::Verified(Bytes::new())) {
                    Flight::Writing(writing, paid) => {
                        let held = std::mem::take(&mut *writing.write()).into_bytes();
                        *entry = Flight::Verified(held.clone());
                        (held, paid, released)
                    }
                    Flight::Verified(held) => {
                        *entry = Flight::Verified(held.clone());
                        (held, None, released)
                    }
                    // Sealed above: it writes to its files.
                    Flight::Blocks(one) => {
                        *entry = Flight::Blocks(one);
                        (Bytes::new(), None, 0)
                    }
                }
            })
        };
        let Some((held, paid, released)) = taken else {
            stored.stored(Err(anyhow::anyhow!(
                "piece {index} completed with nothing written"
            )));
            return;
        };
        self.inner.release(released);
        let landed = Landed {
            inner: Arc::clone(&self.inner),
            index,
            at: held.as_ptr() as usize,
            stored: Some(stored),
        };
        Arc::clone(&self.inner.pieces).store(
            index,
            held,
            paid,
            Box::new(move |written| landed.finish(written)),
        );
    }

    // A whole piece is hashed where its blocks landed, under its own lock
    // alone; one already verified, from its bytes; one written block by
    // block, from its files.
    fn hash_held_piece(&self, piece: ValidPieceIndex, update: &mut dyn FnMut(&[u8])) -> bool {
        // Taken out of the map first, so the hash runs with the map free.
        let held = match self.flight().get(&piece.get()) {
            Some(Flight::Writing(writing, _)) => Ok(Arc::clone(writing)),
            Some(Flight::Verified(bytes)) => Err(Ok(bytes.clone())),
            Some(Flight::Blocks(one)) => Err(Err(Arc::clone(one))),
            None => return false,
        };
        let writing = match held {
            Ok(writing) => writing,
            Err(Ok(bytes)) => {
                update(&bytes);
                return true;
            }
            // Read from its files once the blocks it held in memory, when
            // it was given back, are there.
            Err(Err(one)) => {
                drop(one.lock().unwrap_or_else(PoisonError::into_inner));
                return false;
            }
        };
        let bytes = writing.read();
        // Emptied: it completed meanwhile, and is read from where it went,
        // or it was given back, and is read from its files once the blocks
        // it held are there.
        if bytes.len() != writing.len {
            drop(bytes);
            let spilled = match self.flight().get(&piece.get()) {
                Some(Flight::Blocks(one)) => Some(Arc::clone(one)),
                _ => None,
            };
            if let Some(one) = spilled {
                drop(one.lock().unwrap_or_else(PoisonError::into_inner));
            }
            return false;
        }
        update(&bytes);
        true
    }

    // What failed is dropped, from memory and from the store, and the
    // piece is fetched again into room granted afresh. A piece the
    // session's check of the files did not find is one the store has no
    // longer: the session's check is the one word on which pieces are had.
    fn on_piece_hash_failed(&self, piece: ValidPieceIndex) -> anyhow::Result<()> {
        let failed = self.flight().remove(&piece.get());
        self.inner.release(failed.as_ref().map_or(0, Flight::held));
        self.inner.pieces.failed(piece.get())?;
        Ok(())
    }

    fn on_check_everything(&self) -> anyhow::Result<()> {
        #[cfg(test)]
        if super::service::CHECK_EVERYTHING_FAILS.swap(false, std::sync::atomic::Ordering::AcqRel) {
            bail!("refused by its test");
        }
        self.inner.pieces.checking()?;
        Ok(())
    }

    fn on_piece_checked(&self, piece: ValidPieceIndex) -> anyhow::Result<()> {
        self.inner.pieces.found(piece.get())?;
        Ok(())
    }

    fn wants_hashed_piece(&self, piece: ValidPieceIndex) -> bool {
        self.inner.pieces.wants_hashed(piece.get())
    }

    fn on_piece_hashed(&self, piece: ValidPieceIndex, bytes: Bytes) {
        self.inner.pieces.hashed(piece.get(), &bytes);
    }

    fn on_file_completed(&self, file_id: usize) {
        self.inner.pieces.file_whole(file_id);
    }
}

// A verified piece on its way to disk. Once the store says what came of it,
// it leaves memory and the session hears; dropped untold, it leaves memory
// and the session hears it was not written. Only the piece it handed over
// leaves: one fetched again meanwhile is another's.
struct Landed {
    inner: Arc<Inner>,
    index: u32,
    // Where its bytes are: the piece it handed over, among the pieces of
    // that index.
    at: usize,
    stored: Option<StoredPiece>,
}

impl Landed {
    fn finish(mut self, written: anyhow::Result<()>) {
        self.leave();
        if let Some(stored) = self.stored.take() {
            stored.stored(written);
        }
    }

    fn leave(&self) {
        let mut flight = self.inner.flight();
        if let Some(Flight::Verified(held)) = flight.get(&self.index)
            && held.as_ptr() as usize == self.at
        {
            flight.remove(&self.index);
        }
    }
}

impl Drop for Landed {
    fn drop(&mut self) {
        if self.stored.is_some() {
            self.leave();
        }
    }
}

// Makes the storage of one torrent.
#[derive(Clone)]
pub(crate) struct PieceStorageFactory {
    layout: Arc<PieceLayout>,
    pieces: Arc<dyn Pieces>,
    holds: Holds,
    // Whether its pieces are written block by block: chosen here, once,
    // for every run it makes a storage for.
    blocks: bool,
    #[cfg(test)]
    resident: Arc<ResidentPeak>,
}

impl PieceStorageFactory {
    pub(crate) fn new(layout: Arc<PieceLayout>, pieces: Arc<dyn Pieces>, holds: Holds) -> Self {
        let blocks = pieces
            .writes_blocks()
            .unwrap_or_else(|| writes_blocks(layout.piece_length(), &holds));
        Self {
            layout,
            pieces,
            holds,
            blocks,
            #[cfg(test)]
            resident: Arc::default(),
        }
    }

    pub(crate) fn boxed_factory(self) -> BoxStorageFactory {
        self.boxed()
    }

    // Whether its pieces are written block by block.
    #[cfg(test)]
    pub(crate) fn writes_blocks(&self) -> bool {
        self.blocks
    }

    // The most resident room its storages held at once.
    #[cfg(test)]
    pub(crate) fn resident_peak(&self) -> Arc<ResidentPeak> {
        Arc::clone(&self.resident)
    }
}

impl StorageFactory for PieceStorageFactory {
    type Storage = PieceStorage;

    fn create(
        &self,
        _: &ManagedTorrentShared,
        _: &TorrentMetadata,
    ) -> anyhow::Result<PieceStorage> {
        Ok(PieceStorage::made(
            Arc::clone(&self.layout),
            Arc::clone(&self.pieces),
            self.holds.clone(),
            self.blocks,
            #[cfg(test)]
            Arc::clone(&self.resident),
        ))
    }

    fn clone_box(&self) -> BoxStorageFactory {
        self.clone().boxed()
    }
}
