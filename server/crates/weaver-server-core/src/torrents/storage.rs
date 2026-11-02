// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The torrent session's storage, which is the piece root's intake. Nothing
// the session writes goes to a file of its own: a piece's chunks are kept in
// memory while it is in flight, read back from there while the session
// checks its hash, and once it verified each file's share of it goes to the
// job's root, which writes it to the torrent's file of its name and places
// it where the job's tree says. After that a read of the piece is a read of
// those files, as the metainfo lists them, whatever unpacking made of them.
//
// A piece's buffer is held against the resident budget every job shares from
// the moment the session is granted the piece until it verifies, fails its
// hash, or the storage goes. A piece the budget has no room for is refused,
// and the session waits for the room this storage rings when it lets bytes go.

use std::collections::HashMap;
use std::io::IoSlice;
use std::path::Path;
use std::sync::{Arc, Mutex, PoisonError, RwLock};

use anyhow::{Context, bail};
use bitvec::boxed::BitBox;
use bitvec::order::Msb0;
use bytes::Bytes;
use tokio::sync::Notify;
use weaver_graph::holds::Reserve;
use weaver_graph::slots::AskerId;
use weaver_graph::{ByteRange, VolumeIndex};
use weaver_torrent::core::lengths::ValidPieceIndex;
use weaver_torrent::storage::{
    BoxStorageFactory, StorageFactory, StorageFactoryExt, TorrentStorage,
};
use weaver_torrent::{ManagedTorrentShared, TorrentMetadata, ValidatedTorrentMetaV1Info};

use super::intake::Holds;
use super::service::Signal;
use crate::jobs::ids::JobId;
use crate::pipeline::graph::job::{GraphError, GraphJob};
use crate::pipeline::graph::root::pieces::{
    LayoutError, PieceFile, PieceLayout, PieceRoot, Verified,
};
use crate::pipeline::graph::status::RepairSlots;

#[cfg(test)]
mod tests;

// Where verified pieces go and posted bytes come from.
pub(crate) trait Pieces: Send + Sync {
    // A piece verified: its files' shares, in torrent order.
    fn verified(&self, shares: Vec<Verified>) -> anyhow::Result<()>;
    // A piece failed its hash, fetched or checked on disk: none of it is in
    // hand. True when any of it was.
    fn failed(&self, piece: u32) -> anyhow::Result<bool>;
    // A check of every piece began: no piece in hand is had until the check
    // finds it. True when any was had.
    fn checking(&self) -> anyhow::Result<bool>;
    // The check found the piece: it is had again. True when it waited for
    // the check.
    fn found(&self, piece: u32) -> anyhow::Result<bool>;
    // The volume's posted bytes of the range, appended to `into`; false
    // when they are not all in hand.
    fn posted(&self, volume: VolumeIndex, range: ByteRange, into: &mut Vec<u8>) -> bool;
    // `posted` straight into `out`, which is the range's length; false, and
    // `out` left as it may be, when they are not all in hand.
    fn posted_exact(&self, volume: VolumeIndex, range: ByteRange, out: &mut [u8]) -> bool {
        let mut into = Vec::with_capacity(out.len());
        let read = self.posted(volume, range, &mut into) && into.len() == out.len();
        if read {
            out.copy_from_slice(&into);
        }
        read
    }
    // The volume's bytes from `offset` as its file on disk holds them,
    // whether or not they are in hand: files placed before the torrent was
    // added are read so, for a check to find them or a peer to be served.
    fn placed(&self, _: VolumeIndex, _: u64, _: &mut [u8]) -> std::io::Result<()> {
        Err(std::io::ErrorKind::NotFound.into())
    }
    // Whether every share of the piece is in hand.
    fn had(&self, _: u32) -> bool {
        false
    }
    // A piece found on disk, where its shares already are: the job takes
    // them in hand without writing them, and the bytes need no room.
    fn adopted(&self, shares: Vec<Verified>) -> anyhow::Result<()> {
        self.verified(shares)
    }
}

// A graph job over a piece root as the storage's other side. `limit` is the
// slot limit every step the job takes follows; `repairs` the repair slots
// every torrent job shares, under which a step lends the repairs it made
// due; and `signal` where a job those slots woke is marked, so the service
// lends for it.
pub(crate) struct TorrentJob {
    job: RwLock<GraphJob<PieceRoot>>,
    repairs: Arc<Mutex<RepairSlots>>,
    signal: Arc<Signal>,
    limit: usize,
}

impl TorrentJob {
    pub(super) fn new(
        job: GraphJob<PieceRoot>,
        repairs: Arc<Mutex<RepairSlots>>,
        signal: Arc<Signal>,
        limit: usize,
    ) -> Self {
        Self {
            job: RwLock::new(job),
            repairs,
            signal,
            limit,
        }
    }

    pub(crate) fn with<T>(&self, read: impl FnOnce(&mut GraphJob<PieceRoot>) -> T) -> T {
        read(&mut self.job.write().unwrap_or_else(PoisonError::into_inner))
    }

    // A look at the job that changes nothing: reads of what it has run
    // side by side, and wait only for a step.
    fn look<T>(&self, look: impl FnOnce(&GraphJob<PieceRoot>) -> T) -> T {
        look(&self.job.read().unwrap_or_else(PoisonError::into_inner))
    }

    // One step of the job under the shared repair slots, at `limit`. A slot
    // the step gave back may go to a job that waited for it: that job is
    // marked, and the service lends for it.
    pub(crate) fn step(
        &self,
        limit: usize,
        step: impl FnOnce(&mut GraphJob<PieceRoot>, &mut RepairSlots, usize) -> Result<(), GraphError>,
    ) -> Result<(), GraphError> {
        let mut repairs = self.repairs.lock().unwrap_or_else(PoisonError::into_inner);
        let result = self.with(|job| {
            let result = step(job, &mut repairs, limit);
            // A move asked for while something was out happens at the first
            // step that finds nothing out.
            job.try_relocate();
            result
        });
        for woken in repairs.take_woken() {
            self.signal.mark(JobId(woken.0));
        }
        result
    }

    // A step that says whether it changed the job.
    fn stepped(
        &self,
        step: impl FnOnce(&mut GraphJob<PieceRoot>, &mut RepairSlots, usize) -> Result<bool, GraphError>,
    ) -> anyhow::Result<bool> {
        let mut changed = false;
        self.step(self.limit, |job, repairs, limit| {
            changed = step(job, repairs, limit)?;
            Ok(())
        })?;
        Ok(changed)
    }

    // A slot handed to the job while it waited goes to the repair it could
    // not lend for want of one. Every step lends what it made due, so a
    // job no slot was handed to has nothing to lend here.
    pub(crate) fn lend_handed(&self) -> Result<(), GraphError> {
        let handed = self
            .repairs
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .handed(self.asker());
        if !handed {
            return Ok(());
        }
        self.step(self.limit, |job, repairs, limit| job.lend(repairs, limit))
    }

    // The job's repair slot, or its place in the queue, for a torrent
    // going away.
    pub(crate) fn forget(&self) {
        let asker = self.asker();
        let ticket = self.with(GraphJob::forget_slot);
        let mut repairs = self.repairs.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(ticket) = ticket {
            repairs.release(ticket);
        }
        repairs.forget(asker);
        for woken in repairs.take_woken() {
            self.signal.mark(JobId(woken.0));
        }
    }

    fn asker(&self) -> AskerId {
        AskerId(self.with(|job| job.scope()))
    }
}

impl TorrentJob {
    // A piece's shares taken in. The repair slots every torrent job shares
    // are held only for the lend the shares made due: the file writes and
    // the runs' step take this job's lock alone, so one torrent's commit
    // never holds up another's. A step another thread takes between the
    // two sees the shares in hand and lends for them itself, as this lend
    // would.
    fn commit(&self, shares: Vec<Verified>, write: bool) -> anyhow::Result<()> {
        let taken = self.with(|job| job.verified_unlent(shares, write, self.limit));
        Ok(self.step(self.limit, |job, repairs, limit| {
            if taken? {
                job.lend(repairs, limit)?;
            }
            Ok(())
        })?)
    }
}

impl Pieces for TorrentJob {
    fn verified(&self, shares: Vec<Verified>) -> anyhow::Result<()> {
        self.commit(shares, true)
    }

    fn failed(&self, piece: u32) -> anyhow::Result<bool> {
        self.stepped(|job, repairs, limit| job.failed(piece, repairs, limit))
    }

    fn checking(&self) -> anyhow::Result<bool> {
        self.stepped(|job, repairs, limit| job.checking(repairs, limit))
    }

    fn found(&self, piece: u32) -> anyhow::Result<bool> {
        self.stepped(|job, repairs, limit| job.found(piece, repairs, limit))
    }

    fn posted(&self, volume: VolumeIndex, range: ByteRange, into: &mut Vec<u8>) -> bool {
        self.look(|job| job.posted(volume, range, into))
    }

    fn posted_exact(&self, volume: VolumeIndex, range: ByteRange, out: &mut [u8]) -> bool {
        self.look(|job| job.posted_exact(volume, range, out))
    }

    // Read under the job, as a read of what it has is: a move of its files
    // happens in one of its steps, so the read finds the file where it
    // was or where it went, never between.
    fn placed(&self, volume: VolumeIndex, offset: u64, buf: &mut [u8]) -> std::io::Result<()> {
        self.look(|job| job.read_kept(volume, offset, buf))
    }

    fn had(&self, piece: u32) -> bool {
        self.look(|job| job.has_piece(piece))
    }

    fn adopted(&self, shares: Vec<Verified>) -> anyhow::Result<()> {
        self.commit(shares, false)
    }
}

// The pieces the job has, as the session takes them to skip checking the
// files: one bit per piece, most significant first, the spare bits of the
// last byte clear, as BEP 3 sends them. None while a check of every piece
// has not found every piece in hand: the job vouches for nothing then, and
// the session checks every piece again (libtorrent
// `torrent::write_resume_data` saves no piece past those checked). None
// too while the job has no piece of files that were `placed` where its
// client asked: they may be there already, and the check finds them
// (libtorrent checks the files of a torrent added with no resume data).
pub(crate) fn initial_have(job: &GraphJob<PieceRoot>, placed: bool) -> Option<BitBox<u8, Msb0>> {
    if job.unchecked() {
        return None;
    }
    let mut have = job.have();
    if placed && !have.any() {
        return None;
    }
    have.set_uninitialized(false);
    Some(have.into_boxed_bitslice())
}

// The layout of a torrent's files over its pieces, from its metainfo.
pub(crate) fn layout_of<B: AsRef<[u8]>>(
    info: &ValidatedTorrentMetaV1Info<B>,
) -> Result<PieceLayout, LayoutError> {
    let files = info
        .iter_file_details()
        .map(|details| PieceFile {
            name: details.filename.to_vec().join("/"),
            length: details.len,
            padding: details.attrs().padding,
        })
        .collect();
    PieceLayout::new(u64::from(info.lengths().default_piece_length()), files)
}

// A piece in flight, the length of its piece: its chunks as they arrive,
// then the whole piece while its shares go to the job. Only a piece still
// being written holds room: a verified one's bytes are the job's.
enum Flight {
    Writing(Vec<u8>),
    Verified(Bytes),
}

impl Flight {
    fn bytes(&self) -> &[u8] {
        match self {
            Self::Writing(bytes) => bytes,
            Self::Verified(bytes) => bytes,
        }
    }

    fn held(&self) -> u64 {
        match self {
            Self::Writing(bytes) => bytes.len() as u64,
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

    // A piece found on disk that the job did not have goes to it from
    // there.
    pub(crate) fn adopt(&self, piece: u32) -> anyhow::Result<()> {
        adopt(&self.inner.layout, &*self.inner.pieces, piece)
    }
}

// A piece the job did not have, found where the files were placed: the job
// takes its shares as the files on disk hold them.
pub(crate) fn adopt(layout: &PieceLayout, pieces: &dyn Pieces, piece: u32) -> anyhow::Result<()> {
    adopt_under(layout, pieces, piece, |take| Some(take())).unwrap_or(Ok(()))
}

// `adopt`, the job taking the shares in under `keep` (a seed-mode session's
// `while_seeding`), which is given only that: the files are read before it,
// so nothing waits on the disk under it. None when `keep` did not take them.
pub(crate) fn adopt_under(
    layout: &PieceLayout,
    pieces: &dyn Pieces,
    piece: u32,
    keep: impl FnOnce(&mut dyn FnMut() -> anyhow::Result<()>) -> Option<anyhow::Result<()>>,
) -> Option<anyhow::Result<()>> {
    if pieces.had(piece) {
        return Some(Ok(()));
    }
    let mut bytes = vec![0; layout.piece_range(piece).len() as usize];
    let mut spans = Vec::new();
    for span in layout.spans(piece) {
        let Some(volume) = span.volume else { continue };
        let at = span.at as usize..span.at as usize + span.range.len() as usize;
        if let Err(error) = pieces.placed(volume, span.range.start, &mut bytes[at.clone()]) {
            return Some(Err(error.into()));
        }
        spans.push((volume, span.range.start, at));
    }
    let bytes = Bytes::from(bytes);
    let mut shares = Some(
        spans
            .into_iter()
            .map(|(volume, offset, at)| Verified {
                volume,
                piece,
                offset,
                bytes: bytes.slice(at),
            })
            .collect(),
    );
    keep(&mut || pieces.adopted(shares.take().unwrap_or_default()))
}

impl TorrentStorage for PieceStorage {
    fn init(&mut self, _: &ManagedTorrentShared, metadata: &TorrentMetadata) -> anyhow::Result<()> {
        let theirs = layout_of(&metadata.info)?;
        if theirs != *self.inner.layout {
            bail!("the torrent is not the one the job was made for");
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
            buf.copy_from_slice(&held.bytes()[at..at + buf.len()]);
            return Ok(());
        }
        let Some(volume) = self.inner.layout.file_volume(file_id) else {
            // Padding is zeros by definition.
            buf.fill(0);
            return Ok(());
        };
        let range = ByteRange::at(offset, buf.len() as u64);
        if self.inner.pieces.posted_exact(volume, range, buf) {
            return Ok(());
        }
        // Not in hand: what the file on disk holds, which the session's
        // check hashes and which a seed added over placed files serves. A
        // file not there, or too short, says so as the session reads it.
        self.inner
            .pieces
            .placed(volume, offset, buf)
            .with_context(|| format!("{range} of file {file_id} is not in hand"))
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
        let mut flight = self.flight();
        let Some(Flight::Writing(held)) = flight.get_mut(&piece) else {
            bail!("piece {piece} is written while the session holds no room for it");
        };
        held[at..at + buf.len()].copy_from_slice(buf);
        Ok(())
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

    // The job's files are the job's: the session removes nothing.
    fn remove_file(&self, _: usize, _: &Path) -> anyhow::Result<()> {
        Ok(())
    }

    fn remove_directory_if_empty(&self, _: &Path) -> anyhow::Result<()> {
        Ok(())
    }

    // Lengths are the job's announcements, made from the metainfo.
    fn ensure_file_length(&self, _: usize, _: u64) -> anyhow::Result<()> {
        Ok(())
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
        flight.insert(index, Flight::Writing(vec![0; length as usize]));
        true
    }

    // The piece's shares go to the job, and only then does the piece stop
    // being read from memory, so no read finds it in neither place. Its room
    // is given back first: the job counts the bytes from here.
    fn on_piece_completed(&self, piece: ValidPieceIndex) -> anyhow::Result<()> {
        let index = piece.get();
        let (held, released) = {
            let mut flight = self.flight();
            let Some(entry) = flight.get_mut(&index) else {
                bail!("piece {index} completed with nothing written");
            };
            let released = entry.held();
            let held = match std::mem::replace(entry, Flight::Verified(Bytes::new())) {
                Flight::Writing(bytes) => Bytes::from(bytes),
                Flight::Verified(bytes) => bytes,
            };
            *entry = Flight::Verified(held.clone());
            (held, released)
        };
        self.inner.release(released);
        let shares = self
            .inner
            .layout
            .spans(index)
            .into_iter()
            .filter_map(|span| {
                let volume = span.volume?;
                let at = span.at as usize;
                Some(Verified {
                    volume,
                    piece: index,
                    offset: span.range.start,
                    bytes: held.slice(at..at + span.range.len() as usize),
                })
            })
            .collect();
        self.inner.pieces.verified(shares)?;
        self.flight().remove(&index);
        Ok(())
    }

    // What failed is dropped, from memory and from the job, and the piece
    // is fetched again into room granted afresh. A piece the session's
    // check of the files did not find is one the job has no longer: the
    // session's check is the one word on which pieces are had.
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

    // The check found the piece: had again if the job had it before, and
    // otherwise found where the files were placed, and taken from there.
    fn on_piece_checked(&self, piece: ValidPieceIndex) -> anyhow::Result<()> {
        if !self.inner.pieces.found(piece.get())? {
            self.adopt(piece.get())?;
        }
        Ok(())
    }
}

// Makes the storage of one torrent's job.
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
