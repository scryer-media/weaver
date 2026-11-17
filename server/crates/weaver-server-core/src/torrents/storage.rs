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
// the moment the session is granted the piece until it verifies, fails its
// hash, or the storage goes. Its bytes are also counted on the volume it is
// written to, from the same moment until its writer has written them. A
// piece the budget or the volume has no room for is refused, and the session
// waits for the room this storage, or the volume's writer, rings when it
// lets bytes go.
//
// A torrent whose piece is larger than the whole resident budget could
// never hold one, and is written block by block instead, as libtorrent
// writes every piece: each block goes to the volume's writer as it
// arrives, the piece is hashed back from its files on the volume's read
// turns, and a piece that fails is fetched again. The choice is made once,
// as the storage's maker is made, from the piece's length and the budget
// then; nothing records it. Such a piece holds a small, fixed room while it
// arrives (`Pieces::block_room`), its blocks are written one at a time, and
// its bytes are counted on its volume as each block's write fits.

use std::collections::HashMap;
use std::io::IoSlice;
use std::path::Path;
use std::sync::{Arc, Mutex, PoisonError};

use anyhow::{Context, bail};
use bytes::Bytes;
use tokio::sync::Notify;
use weaver_graph::holds::Reserve;
use weaver_torrent::core::lengths::ValidPieceIndex;
use weaver_torrent::storage::{
    BoxStorageFactory, GivesRoomBack, HeldRoom, ReadTurns, StorageFactory, StorageFactoryExt,
    StoredPiece, TorrentStorage,
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
    // The resident room a piece written block by block holds while it
    // arrives.
    fn block_room(&self, piece_length: u64) -> u64 {
        u64::from(weaver_torrent::core::constants::CHUNK_SIZE).min(piece_length)
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
    // one at a time under its lock, and it holds the room it was started
    // with until it verified or failed.
    Blocks(Arc<Mutex<()>>, u64),
}

// A piece being written: its blocks land here, and it is hashed here once
// whole, under its own lock, so hashing one piece holds up no other's
// blocks.
struct Writing {
    // The room it holds.
    len: usize,
    bytes: std::sync::RwLock<Vec<u8>>,
}

impl Writing {
    fn read(&self) -> std::sync::RwLockReadGuard<'_, Vec<u8>> {
        self.bytes.read().unwrap_or_else(PoisonError::into_inner)
    }

    fn write(&self) -> std::sync::RwLockWriteGuard<'_, Vec<u8>> {
        self.bytes.write().unwrap_or_else(PoisonError::into_inner)
    }
}

impl Flight {
    fn writing(len: usize, paid: Option<Paid>) -> Self {
        Self::Writing(
            Arc::new(Writing {
                len,
                bytes: std::sync::RwLock::new(vec![0; len]),
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
            Self::Blocks(_, room) => *room,
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
}

struct Inner {
    layout: Arc<PieceLayout>,
    pieces: Arc<dyn Pieces>,
    // The pieces in flight, by index.
    flight: Mutex<HashMap<u32, Flight>>,
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

impl Inner {
    // `bytes` of resident room asked for: held when granted.
    fn reserve(&self, bytes: u64) -> bool {
        let granted = matches!(
            self.holds
                .accountant
                .reserve_resident(self.holds.namespace, bytes),
            Reserve::Granted
        );
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

    // Room given back: the session waiting on it asks again.
    fn release(&self, bytes: u64) {
        self.unreserve(bytes);
        if let Some(room) = &*self.room.lock().unwrap_or_else(PoisonError::into_inner) {
            room.notify_waiters();
        }
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
                flight: Mutex::new(HashMap::new()),
                holds,
                room: Mutex::new(None),
                blocks,
                #[cfg(test)]
                resident,
            }),
        }
    }

    // The most resident room it held at once.
    #[cfg(test)]
    pub(crate) fn resident_peak(&self) -> u64 {
        self.inner
            .resident
            .peak
            .load(std::sync::atomic::Ordering::Acquire)
    }

    fn flight(&self) -> std::sync::MutexGuard<'_, HashMap<u32, Flight>> {
        self.inner
            .flight
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
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

    // Room for a piece not in memory: its resident room, then its volume's.
    // A piece written block by block holds its small room alone: its
    // blocks are counted on the volume as each one's write fits.
    fn start_piece(&self, index: u32, length: u64, room: &Arc<Notify>) -> bool {
        if self.inner.blocks {
            let held = self.inner.pieces.block_room(length);
            if !self.inner.reserve(held) {
                return false;
            }
            let mut flight = self.flight();
            if flight.contains_key(&index) {
                drop(flight);
                self.inner.release(held);
                return true;
            }
            flight.insert(index, Flight::Blocks(Arc::default(), held));
            return true;
        }
        if !self.inner.reserve(length) {
            return false;
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
            .reserve(bytes)
            .then(|| HeldRoom::new(bytes, Arc::clone(&self.inner) as _))
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
                !(first..=last).any(|piece| {
                    flight
                        .get(&(piece as u32))
                        .is_some_and(Flight::in_memory)
                })
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
        if buf.is_empty() {
            return Ok(());
        }
        let (start, piece) = self.place(file_id, offset, buf.len())?;
        let Some(piece) = piece else {
            bail!("a write of file {file_id} crosses a piece boundary");
        };
        let range = self.inner.layout.piece_range(piece);
        let at = (start - range.start) as usize;
        let landing = match self.flight().get(&piece) {
            Some(Flight::Writing(writing, _)) => Ok(Arc::clone(writing)),
            Some(Flight::Blocks(one, _)) => Err(Arc::clone(one)),
            _ => bail!("piece {piece} is written while the session holds no room for it"),
        };
        let held = match landing {
            Ok(writing) => writing,
            // Outside the map's lock, one block of the piece at a time: the
            // room it holds is one block's.
            Err(one) => {
                let _one = one.lock().unwrap_or_else(PoisonError::into_inner);
                return self
                    .inner
                    .pieces
                    .write_block(piece, file_id, offset, at as u64, buf);
            }
        };
        // Outside the map's lock: another piece's blocks land meanwhile.
        let mut bytes = held.write();
        let Some(to) = bytes.get_mut(at..at + buf.len()) else {
            bail!("piece {piece} is written after it completed");
        };
        to.copy_from_slice(buf);
        Ok(())
    }

    // A block is only ever copied into the piece held for it, but for a
    // piece too large to hold, whose blocks go to its files.
    fn writes_to_memory(&self) -> bool {
        !self.inner.blocks
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

    // Room for a piece the session is about to be handed, or of which a
    // block arrived while no peer owns it. A piece given back by a peer that
    // left keeps what was written and the room it holds.
    fn reserve_piece(&self, piece: ValidPieceIndex, room: &Arc<Notify>) -> bool {
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
            None => return self.start_piece(index, length, room),
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

    // The piece goes to the store, and only then does it stop being read
    // from memory, so no read finds it in neither place. Its room is given
    // back first: it is on its way to disk.
    //
    // A piece written block by block is on disk already: it is had once the
    // store says so, and the room it held goes with it to what reads it
    // back for the tee.
    fn on_piece_completed(&self, piece: ValidPieceIndex) -> anyhow::Result<()> {
        let index = piece.get();
        if self.inner.blocks {
            let room = match self.flight().remove(&index) {
                Some(entry) => entry.held(),
                None => bail!("piece {index} completed with nothing written"),
            };
            let room = HeldRoom::new(room, Arc::clone(&self.inner) as _);
            return Arc::clone(&self.inner.pieces).sealed(index, room);
        }
        let (held, released) = {
            let mut flight = self.flight();
            let Some(entry) = flight.get_mut(&index) else {
                bail!("piece {index} completed with nothing written");
            };
            let released = entry.held();
            let held = match std::mem::replace(entry, Flight::Verified(Bytes::new())) {
                Flight::Writing(writing, _) => Bytes::from(std::mem::take(&mut *writing.write())),
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
        let taken = {
            let mut flight = self.flight();
            flight.get_mut(&index).map(|entry| {
                let released = entry.held();
                match std::mem::replace(entry, Flight::Verified(Bytes::new())) {
                    Flight::Writing(writing, paid) => {
                        let held = Bytes::from(std::mem::take(&mut *writing.write()));
                        *entry = Flight::Verified(held.clone());
                        (held, paid, released)
                    }
                    Flight::Verified(held) => {
                        *entry = Flight::Verified(held.clone());
                        (held, None, released)
                    }
                    // Never handed here: it writes to its files.
                    Flight::Blocks(one, room) => {
                        *entry = Flight::Blocks(one, room);
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
            Some(Flight::Verified(bytes)) => Err(bytes.clone()),
            Some(Flight::Blocks(..)) | None => return false,
        };
        let writing = match held {
            Ok(writing) => writing,
            Err(bytes) => {
                update(&bytes);
                return true;
            }
        };
        let bytes = writing.read();
        // Emptied: it completed meanwhile, and is read from where it went.
        if bytes.len() != writing.len {
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
        let mut flight = self
            .inner
            .flight
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
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
