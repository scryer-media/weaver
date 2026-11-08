// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The torrent session's storage. A piece's chunks are kept in memory while
// it is in flight, read back from there while the session checks its hash,
// and once it verified the whole buffer goes to the torrent's store, which
// writes it to the torrent's files and hands the tee its archive shares.
// After that a read of the piece is a read of those files.
//
// A piece's buffer is held against the resident budget every job shares from
// the moment the session is granted the piece until it verifies, fails its
// hash, or the storage goes. A piece the budget has no room for is refused,
// and the session waits for the room this storage rings when it lets bytes go.

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
    BoxStorageFactory, StorageFactory, StorageFactoryExt, TorrentStorage,
};
use weaver_torrent::{ManagedTorrentShared, TorrentMetadata};

use super::intake::Holds;
use super::layout::{PieceLayout, layout_of};

#[cfg(test)]
mod tests;

// Where verified pieces go and the files are read from.
pub(crate) trait Pieces: Send + Sync {
    // A piece verified: `held` is the whole piece, as it was hashed.
    fn verified(&self, piece: u32, held: &Bytes) -> anyhow::Result<()>;
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
    // The file made at least `length` long.
    fn grow(&self, file: usize, length: u64) -> std::io::Result<()>;
}

// A piece in flight, the length of its piece: its chunks as they arrive,
// then the whole piece while it goes to the store. Only a piece still being
// written holds room: a verified one's bytes are on their way to disk.
enum Flight {
    Writing(Arc<Writing>),
    Verified(Bytes),
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
    fn writing(len: usize) -> Self {
        Self::Writing(Arc::new(Writing {
            len,
            bytes: std::sync::RwLock::new(vec![0; len]),
        }))
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
            Self::Writing(writing) => copy(&writing.read()),
            Self::Verified(bytes) => copy(bytes),
        }
    }

    fn held(&self) -> u64 {
        match self {
            Self::Writing(writing) => writing.len as u64,
            Self::Verified(_) => 0,
        }
    }
}

struct Inner {
    layout: Arc<PieceLayout>,
    pieces: Arc<dyn Pieces>,
    // The pieces in flight, by index.
    flight: Mutex<HashMap<u32, Flight>>,
    holds: Holds,
    // What the session waits on for room, as it last asked.
    room: Mutex<Option<Arc<Notify>>>,
}

impl Inner {
    // Room given back: the session waiting on it asks again.
    fn release(&self, bytes: u64) {
        self.holds
            .accountant
            .release_resident(self.holds.namespace, bytes);
        if let Some(room) = &*self.room.lock().unwrap_or_else(PoisonError::into_inner) {
            room.notify_waiters();
        }
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

impl PieceStorage {
    pub(crate) fn new(layout: Arc<PieceLayout>, pieces: Arc<dyn Pieces>, holds: Holds) -> Self {
        Self {
            inner: Arc::new(Inner {
                layout,
                pieces,
                flight: Mutex::new(HashMap::new()),
                holds,
                room: Mutex::new(None),
            }),
        }
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
        let held = match self.flight().get(&piece) {
            Some(Flight::Writing(writing)) => Arc::clone(writing),
            _ => bail!("piece {piece} is written while the session holds no room for it"),
        };
        // Outside the map's lock: another piece's blocks land meanwhile.
        let mut bytes = held.write();
        let Some(to) = bytes.get_mut(at..at + buf.len()) else {
            bail!("piece {piece} is written after it completed");
        };
        to.copy_from_slice(buf);
        Ok(())
    }

    // A block is only ever copied into the piece held for it.
    fn writes_to_memory(&self) -> bool {
        true
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

    fn take(&self) -> anyhow::Result<Box<dyn TorrentStorage>> {
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
        let mut flight = self.flight();
        if flight.contains_key(&index) {
            return true;
        }
        let length = self.inner.layout.piece_range(index).len();
        let holds = &self.inner.holds;
        if let Reserve::Breached(_) = holds.accountant.reserve_resident(holds.namespace, length) {
            return false;
        }
        flight.insert(index, Flight::writing(length as usize));
        true
    }

    // The piece goes to the store, and only then does it stop being read
    // from memory, so no read finds it in neither place. Its room is given
    // back first: it is on its way to disk.
    fn on_piece_completed(&self, piece: ValidPieceIndex) -> anyhow::Result<()> {
        let index = piece.get();
        let (held, released) = {
            let mut flight = self.flight();
            let Some(entry) = flight.get_mut(&index) else {
                bail!("piece {index} completed with nothing written");
            };
            let released = entry.held();
            let held = match std::mem::replace(entry, Flight::Verified(Bytes::new())) {
                Flight::Writing(writing) => Bytes::from(std::mem::take(&mut *writing.write())),
                Flight::Verified(bytes) => bytes,
            };
            *entry = Flight::Verified(held.clone());
            (held, released)
        };
        self.inner.release(released);
        let verified = self.inner.pieces.verified(index, &held);
        self.flight().remove(&index);
        verified
    }

    // A whole piece is hashed where its blocks landed, under its own lock
    // alone; one already verified, from its bytes.
    fn hash_held_piece(&self, piece: ValidPieceIndex, update: &mut dyn FnMut(&[u8])) -> bool {
        // Taken out of the map first, so the hash runs with the map free.
        let held = match self.flight().get(&piece.get()) {
            Some(Flight::Writing(writing)) => Flight::Writing(Arc::clone(writing)),
            Some(Flight::Verified(bytes)) => Flight::Verified(bytes.clone()),
            None => return false,
        };
        let writing = match held {
            Flight::Writing(writing) => writing,
            Flight::Verified(bytes) => {
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
}

// Makes the storage of one torrent.
#[derive(Clone)]
pub(crate) struct PieceStorageFactory {
    layout: Arc<PieceLayout>,
    pieces: Arc<dyn Pieces>,
    holds: Holds,
}

impl PieceStorageFactory {
    pub(crate) fn new(layout: Arc<PieceLayout>, pieces: Arc<dyn Pieces>, holds: Holds) -> Self {
        Self {
            layout,
            pieces,
            holds,
        }
    }

    pub(crate) fn boxed_factory(self) -> BoxStorageFactory {
        self.boxed()
    }
}

impl StorageFactory for PieceStorageFactory {
    type Storage = PieceStorage;

    fn create(
        &self,
        _: &ManagedTorrentShared,
        _: &TorrentMetadata,
    ) -> anyhow::Result<PieceStorage> {
        Ok(PieceStorage::new(
            Arc::clone(&self.layout),
            Arc::clone(&self.pieces),
            self.holds.clone(),
        ))
    }

    fn clone_box(&self) -> BoxStorageFactory {
        self.clone().boxed()
    }
}
