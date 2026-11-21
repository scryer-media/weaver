// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Verified pieces on their way to the store, under the disk budget. A piece
// the budget refuses, or whose write finds no space, is refused to the
// session as a write that found no room: the session puts the torrent in
// upload mode, where it fetches nothing and keeps seeding, and the owner
// hears what the write wanted, to wait for room as any job does. Any other
// write the store could not make fails the torrent, as it fails a usenet
// job: the one classifier both use tells the two apart.

use std::io;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError};

use bytes::Bytes;
use weaver_graph::holds::{HoldsAccountant, HoldsNamespace};

use crate::torrents::storage::Pieces;
use crate::torrents::writes::{Done, Paid, VolumeRoom};
use tokio::sync::Notify;
use weaver_server_disk::operations::disk::{
    CapacityReader, DiskLedger, StorageRoot, is_out_of_space, is_quota_exceeded,
};
use weaver_server_disk::operations::room_wait::{Disk, Wants};
use weaver_server_disk::operations::spend::Spend;
use weaver_server_disk::pipeline::extraction::safety::ExtractionLimits;
use weaver_torrent::storage::HeldRoom;

// Where a torrent's pieces land: the disk the folder they are written to
// lies on, by the choice a usenet job held for a full disk is placed by,
// and the reading and ledger writes there book on. A disk off the roots'
// has none: its writes are not booked, and wait only when the disk itself
// refuses one.
#[derive(Clone)]
pub struct Place {
    pub disk: Disk,
    // The folder the disk was chosen for.
    pub dir: PathBuf,
    reader: Option<CapacityReader>,
}

impl Place {
    pub fn new(disk: Disk, dir: PathBuf, reader: Option<CapacityReader>) -> Self {
        Self { disk, dir, reader }
    }

    // The reading writes here book on, when they book.
    pub fn reader(&self) -> Option<&CapacityReader> {
        self.reader.as_ref()
    }
}

// The disk budget the pieces are written under: the reserve the job's
// limits keep free on the disk the torrent's folder lies on, booked on the
// ledger every writer on that disk shares, usenet's included, so two
// writers never spend the same free space. Until the disk of the folder
// the pieces go to is known, no piece is let through: each is refused as a
// booking the ledger refused, and the torrent waits for its disk to be
// known, then on its ledger.
pub struct Room {
    limits: Arc<ExtractionLimits>,
    spot: Mutex<Spot>,
    // Room a wake promised on the place's ledger for the write it let go:
    // the next pieces spend it before they book, and what is left goes
    // back when the place changes, the run ends or the room goes.
    in_hand: AtomicU64,
    // A wake judged by readings of the disk its write found full promised
    // it room beside the other waiters': the next piece that lands ends
    // that promise.
    promised: AtomicBool,
    // Its disk is being looked up.
    placing: AtomicBool,
}

// The folder the pieces go to, when it is set, and the disk they land on
// once known: a place for another folder is no place.
struct Spot {
    aim: Option<PathBuf>,
    place: Option<Place>,
    // What the torrent's start was promised on the place's disk, drawn as
    // its pieces land, by the account every job class keeps its room by.
    start: Spend,
    // Its start was judged and promised in this run.
    started: bool,
}

impl Room {
    // A budget booked on `free_disk`, which reads the complete root: with
    // no disks to look a folder up on.
    pub fn new(limits: Arc<ExtractionLimits>, free_disk: CapacityReader) -> Self {
        Self::with(
            limits,
            None,
            Some(Place::new(
                Disk::Root(StorageRoot::Complete),
                PathBuf::new(),
                Some(free_disk),
            )),
        )
    }

    // A budget for pieces that go to `dir`, whose disk is not known yet.
    pub fn aimed(limits: Arc<ExtractionLimits>, dir: PathBuf) -> Self {
        Self::with(limits, Some(dir), None)
    }

    // A budget for pieces that land at `place`, known already.
    pub fn placed(limits: Arc<ExtractionLimits>, place: Place) -> Self {
        Self::with(limits, Some(place.dir.clone()), Some(place))
    }

    fn with(limits: Arc<ExtractionLimits>, aim: Option<PathBuf>, place: Option<Place>) -> Self {
        Self {
            limits,
            spot: Mutex::new(Spot {
                aim,
                place,
                start: Spend::default(),
                started: false,
            }),
            in_hand: AtomicU64::new(0),
            promised: AtomicBool::new(false),
            placing: AtomicBool::new(false),
        }
    }

    // A look for its disk begins: whether none was out.
    pub fn begin_placing(&self) -> bool {
        !self.placing.swap(true, Ordering::AcqRel)
    }

    // The look for its disk ended without a place.
    pub fn end_placing(&self) {
        self.placing.store(false, Ordering::Release);
    }

    // The look for its disk ends once the place known is the one for the
    // folder the pieces go to. While it is not, the look goes on, for the
    // folder named: one look at a time, and the latest folder wins. A move
    // that names a folder after this ends begins a look of its own.
    pub fn finish_placing(&self) -> Result<(), PathBuf> {
        let spot = self.locked();
        match &spot.aim {
            Some(aim) if spot.place.as_ref().is_none_or(|place| place.dir != *aim) => {
                Err(aim.clone())
            }
            _ => {
                self.placing.store(false, Ordering::Release);
                Ok(())
            }
        }
    }

    // Whether a look for its disk is out.
    #[cfg(any(test, feature = "test-support"))]
    pub fn placing(&self) -> bool {
        self.placing.load(Ordering::Acquire)
    }

    fn locked(&self) -> std::sync::MutexGuard<'_, Spot> {
        self.spot.lock().unwrap_or_else(PoisonError::into_inner)
    }

    // Where the pieces land, once known.
    pub fn place(&self) -> Option<Place> {
        self.locked().place.clone()
    }

    // The pieces go to `dir` from now: a place known for another folder is
    // dropped, with the room in hand on it. Whether the disk of `dir` is to
    // be looked up.
    pub fn aim(&self, dir: &Path) -> bool {
        let mut spot = self.locked();
        if spot.place.as_ref().is_some_and(|place| place.dir != dir) {
            let left = spot.place.take();
            self.give_in_hand(left.as_ref());
            Self::give_start(&mut spot);
        }
        spot.aim = Some(dir.to_path_buf());
        spot.place.is_none()
    }

    // The disk the pieces land on was found: `place`, for the folder they
    // go to, from now. A place found for a folder they no longer go to is
    // not taken. Room in hand on the disk they landed on goes back. Whether
    // it was taken.
    pub fn set_place(&self, place: Place) -> bool {
        let mut spot = self.locked();
        if spot.aim.as_ref().is_some_and(|aim| *aim != place.dir) {
            return false;
        }
        let moved = spot.place.as_ref().is_some_and(|left| !same_disk(left, &place));
        let left = spot.place.replace(place);
        self.give_in_hand(left.as_ref());
        if moved {
            Self::give_start(&mut spot);
        }
        true
    }

    // What the start promised on the disk the pieces no longer land on goes
    // back there, and the start is judged again on the new disk, as the
    // look that finds it asks: the promise follows the torrent. Until then
    // its pieces book as they are written.
    fn give_start(spot: &mut Spot) {
        std::mem::take(&mut spot.start).release_all();
        spot.started = false;
    }

    // Whether the torrent's start was promised its room in this run.
    pub fn started(&self) -> bool {
        self.locked().started
    }

    // The torrent starts with `bytes` still to fetch promised on its
    // place's ledger, by the start rule: its pieces draw on that promise
    // as they land.
    pub fn take_start(&self, bytes: u64) {
        let mut spot = self.locked();
        let spot = &mut *spot;
        if let Some(reader) = spot.place.as_ref().and_then(Place::reader) {
            spot.start.take_posted(bytes, reader);
        }
        spot.started = true;
    }

    // What the start promised and did not land goes back, and the next
    // start is judged again: the torrent paused, stopped, ended or its
    // wanted files changed.
    pub fn release_start(&self) {
        let mut spot = self.locked();
        Self::give_start(&mut spot);
        spot.started = false;
    }

    // What the start promised and has not landed.
    pub fn start_left(&self) -> u64 {
        self.locked().start.posted()
    }

    // What a wake handed this torrent on `place` goes back.
    fn give_in_hand(&self, place: Option<&Place>) {
        let given = self.in_hand.swap(0, Ordering::AcqRel);
        if let Some(reader) = place.and_then(Place::reader) {
            reader.ledger().release(given);
        }
    }

    // A wake promised `bytes` on the place's ledger to this torrent.
    pub fn hand(&self, bytes: u64) {
        self.in_hand.fetch_add(bytes, Ordering::AcqRel);
    }

    // A wake promised this torrent room beside the other waiters': the next
    // piece that lands ends it.
    pub fn promise_room(&self) {
        self.promised.store(true, Ordering::Release);
    }

    // What a wake handed this torrent and it did not spend goes back, and
    // a promise beside the other waiters' is no longer pending: its run
    // ended.
    pub fn give_back(&self) {
        self.promised.store(false, Ordering::Release);
        let spot = self.locked();
        self.give_in_hand(spot.place.as_ref());
    }

    #[cfg(any(test, feature = "test-support"))]
    pub fn in_hand(&self) -> u64 {
        self.in_hand.load(Ordering::Acquire)
    }

    // `bytes` more, booked when they fit over the reserve: from what a
    // wake handed it first, else checked and promised on the ledger in one
    // step. A disk off the roots', or one never read, is never refused; a
    // disk not known yet refuses every piece. The booking goes back unless
    // it is landed.
    fn admits(&self, bytes: u64) -> Option<Booked> {
        let mut spot = self.locked();
        let spot = &mut *spot;
        let place = spot.place.as_ref()?;
        let Some(reader) = place.reader() else {
            return Some(Booked::none());
        };
        let ledger = Arc::clone(reader.ledger());
        let spent = self
            .in_hand
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |have| {
                have.checked_sub(bytes)
            })
            .is_ok();
        // Drawn from what the start promised, then booked on the ledger a
        // slab at a time, as every job class's writes are.
        if !spent && !spot.start.draw(bytes, reader, self.limits.min_free_bytes) {
            return None;
        }
        Some(Booked {
            ledger: Some(ledger),
            bytes,
            landed: false,
        })
    }
}

impl Drop for Room {
    fn drop(&mut self) {
        self.give_back();
        self.release_start();
    }
}

// Whether two places lie on one disk: on one ledger, or both off the
// roots'.
fn same_disk(left: &Place, place: &Place) -> bool {
    match (left.reader(), place.reader()) {
        (Some(left), Some(place)) => left.ledger().same_disk(place.ledger()),
        (None, None) => true,
        _ => false,
    }
}

// Bytes booked for one piece: landed when its write returns, else given
// back, whether the write failed or panicked.
struct Booked {
    ledger: Option<Arc<DiskLedger>>,
    bytes: u64,
    landed: bool,
}

impl Booked {
    fn none() -> Self {
        Self {
            ledger: None,
            bytes: 0,
            landed: true,
        }
    }

    fn landed(mut self) {
        if let Some(ledger) = &self.ledger {
            ledger.landed(self.bytes, 0);
        }
        self.landed = true;
    }
}

impl Drop for Booked {
    fn drop(&mut self) {
        if !self.landed
            && let Some(ledger) = &self.ledger
        {
            ledger.release(self.bytes);
        }
    }
}

// Where a torrent's bytes in memory are counted: the accountant every job
// shares, under the namespace of the job's root.
#[derive(Clone)]
pub struct Holds {
    pub accountant: Arc<HoldsAccountant>,
    pub namespace: HoldsNamespace,
}

// What the intake tells its owner.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Heard {
    // The pieces had changed: one verified, one failed its hash, a check of
    // every piece began, or the check found one.
    Changed,
    // The store could not take a piece, and why: the torrent's failure. A
    // write that found no room is not one.
    Refused(String),
    // A piece's write found the disk full, and what it wanted: the
    // torrent waits in upload mode for a reading that shows room made.
    NoRoom(Wants),
    // The ledger had no room over the reserve for a piece of these bytes:
    // the torrent waits in upload mode on the ledger, as a usenet job whose
    // booking is refused does.
    NotBooked(u64),
    // A piece landed after a wake promised the torrent room beside the
    // other waiters': that promise ends.
    Landed,
    // A piece the session counted made one of the torrent's files whole.
    FileWhole,
}

pub type Hear = Box<dyn Fn(Heard) + Send + Sync>;

pub struct Intake {
    inner: Arc<dyn Pieces>,
    room: Room,
    hear: Hear,
}

impl Intake {
    pub fn new(inner: Arc<dyn Pieces>, room: Room, hear: Hear) -> Self {
        Self { inner, room, hear }
    }

    // The disk budget its pieces are written under.
    pub fn room(&self) -> &Room {
        &self.room
    }

    // What the store said of a piece it was told of: a change is heard, and
    // one it could not make fails the torrent.
    fn heard(&self, changed: anyhow::Result<bool>) -> anyhow::Result<bool> {
        match changed {
            Ok(changed) => {
                if changed {
                    (self.hear)(Heard::Changed);
                }
                Ok(changed)
            }
            Err(error) => {
                (self.hear)(Heard::Refused(format!("{error:#}")));
                Err(error)
            }
        }
    }
}

impl Intake {
    // Book a piece of `bytes` on the disk's ledger: refused when it would eat
    // into the reserve, which the torrent hears.
    fn book(&self, bytes: u64) -> anyhow::Result<Booked> {
        self.room.admits(bytes).ok_or_else(|| {
            (self.hear)(Heard::NotBooked(bytes));
            anyhow::Error::from(io::Error::from(io::ErrorKind::StorageFull)).context(format!(
                "{bytes} bytes more would eat into the disk reserve"
            ))
        })
    }

    // What came of a booked piece's write, heard: a write that failed gives
    // its booking back, as one that was given up or panicked does as it
    // drops.
    fn settled(
        &self,
        booked: Booked,
        bytes: u64,
        written: anyhow::Result<()>,
    ) -> anyhow::Result<()> {
        let error = match written {
            Ok(()) => {
                booked.landed();
                if self.room.promised.swap(false, Ordering::AcqRel) {
                    (self.hear)(Heard::Landed);
                }
                (self.hear)(Heard::Changed);
                return Ok(());
            }
            Err(error) => error,
        };
        drop(booked);
        let causes = || {
            error
                .chain()
                .filter_map(|cause| cause.downcast_ref::<io::Error>())
        };
        if causes().any(is_out_of_space) {
            (self.hear)(Heard::NoRoom(Wants {
                left: Some(bytes),
                quota: causes().any(is_quota_exceeded),
                needs: None,
            }));
        } else {
            (self.hear)(Heard::Refused(format!("{error:#}")));
        }
        Err(error)
    }
}

impl Pieces for Intake {
    fn verified(&self, piece: u32, held: &Bytes) -> anyhow::Result<()> {
        let bytes = held.len() as u64;
        let booked = self.book(bytes)?;
        let written = self.inner.verified(piece, held);
        self.settled(booked, bytes, written)
    }

    fn store(self: Arc<Self>, piece: u32, held: Bytes, paid: Option<Paid>, done: Done) {
        let bytes = held.len() as u64;
        let booked = match self.book(bytes) {
            Ok(booked) => booked,
            Err(error) => {
                drop(paid);
                return done(Err(error));
            }
        };
        let inner = Arc::clone(&self.inner);
        inner.store(
            piece,
            held,
            paid,
            Box::new(move |written| done(self.settled(booked, bytes, written))),
        );
    }

    fn reserve(&self, bytes: u64, room: &Arc<Notify>) -> VolumeRoom {
        self.inner.reserve(bytes, room)
    }

    fn stop_writes(&self) {
        self.inner.stop_writes();
    }

    fn retired(&self) -> bool {
        self.inner.retired()
    }

    // A block is booked on the disk's ledger as a piece is, and its write
    // heard the same way when it fails. One that landed changes nothing
    // the torrent has: the piece is had, and heard, once it is sealed.
    fn write_block(
        &self,
        piece: u32,
        file: usize,
        offset: u64,
        at: u64,
        bytes: &[u8],
    ) -> anyhow::Result<()> {
        let len = bytes.len() as u64;
        let booked = self.book(len)?;
        match self.inner.write_block(piece, file, offset, at, bytes) {
            Ok(()) => {
                booked.landed();
                if self.room.promised.swap(false, Ordering::AcqRel) {
                    (self.hear)(Heard::Landed);
                }
                Ok(())
            }
            Err(error) => self.settled(booked, len, Err(error)),
        }
    }

    fn sealed(self: Arc<Self>, piece: u32, room: HeldRoom) -> anyhow::Result<()> {
        let sealed = Arc::clone(&self.inner).sealed(piece, room);
        self.heard(sealed.map(|()| true)).map(|_| ())
    }

    fn writes_blocks(&self) -> Option<bool> {
        self.inner.writes_blocks()
    }

    fn read_turns(&self) -> weaver_torrent::storage::ReadTurns {
        self.inner.read_turns()
    }

    fn room_returned(&self) {
        self.inner.room_returned();
    }

    fn aside(&self, run: Box<dyn FnOnce() + Send>) {
        self.inner.aside(run);
    }

    fn failed(&self, piece: u32) -> anyhow::Result<bool> {
        self.heard(self.inner.failed(piece))
    }

    fn checking(&self) -> anyhow::Result<bool> {
        self.heard(self.inner.checking())
    }

    fn found(&self, piece: u32) -> anyhow::Result<bool> {
        self.heard(self.inner.found(piece))
    }

    fn wants_hashed(&self, piece: u32) -> bool {
        self.inner.wants_hashed(piece)
    }

    fn hashed(&self, piece: u32, held: &Bytes) {
        self.inner.hashed(piece, held);
    }

    fn read(&self, file: usize, offset: u64, out: &mut [u8]) -> io::Result<()> {
        self.inner.read(file, offset, out)
    }

    fn will_read(&self, file: usize, offset: u64, len: u64) {
        self.inner.will_read(file, offset, len);
    }

    fn readv(&self, file: usize, offset: u64, outs: &mut [&mut [u8]]) -> io::Result<()> {
        self.inner.readv(file, offset, outs)
    }

    fn grow(&self, file: usize, length: u64) -> io::Result<()> {
        self.inner.grow(file, length)
    }

    fn file_whole(&self, _file: usize) {
        (self.hear)(Heard::FileWhole);
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Mutex;
    use std::sync::atomic::{AtomicBool, Ordering};

    use weaver_torrent::storage::write_refused;

    use super::*;

    // A store that refuses every write while the disk is full, and
    // any write or verdict at all while it is broken; the pieces it was told
    // failed, in order, and those a check has yet to find.
    #[derive(Default)]
    struct Disk {
        full: AtomicBool,
        broken: AtomicBool,
        // Refuses every write as a read-only file system would, or panics.
        read_only: AtomicBool,
        panics: AtomicBool,
        took: Mutex<Vec<(u32, u64)>>,
        failed: Mutex<Vec<u32>>,
        unchecked: Mutex<Vec<u32>>,
    }

    impl Pieces for Disk {
        fn verified(&self, piece: u32, held: &Bytes) -> anyhow::Result<()> {
            if self.full.load(Ordering::SeqCst) {
                return Err(
                    anyhow::Error::from(io::Error::from(io::ErrorKind::StorageFull))
                        .context("writing a share"),
                );
            }
            if self.read_only.load(Ordering::SeqCst) {
                return Err(anyhow::Error::from(io::Error::from(
                    io::ErrorKind::ReadOnlyFilesystem,
                ))
                .context("writing a share"));
            }
            assert!(!self.panics.load(Ordering::SeqCst), "the write panicked");
            anyhow::ensure!(!self.broken.load(Ordering::SeqCst), "the tree is broken");
            let mut took = self.took.lock().unwrap();
            took.push((piece, held.len() as u64));
            Ok(())
        }

        fn failed(&self, piece: u32) -> anyhow::Result<bool> {
            anyhow::ensure!(!self.broken.load(Ordering::SeqCst), "the tree is broken");
            self.failed.lock().unwrap().push(piece);
            let mut took = self.took.lock().unwrap();
            let before = took.len();
            took.retain(|(held, _)| *held != piece);
            Ok(took.len() != before)
        }

        fn checking(&self) -> anyhow::Result<bool> {
            anyhow::ensure!(!self.broken.load(Ordering::SeqCst), "the tree is broken");
            let took = self.took.lock().unwrap();
            let mut unchecked = self.unchecked.lock().unwrap();
            *unchecked = took.iter().map(|(piece, _)| *piece).collect();
            Ok(!unchecked.is_empty())
        }

        fn found(&self, piece: u32) -> anyhow::Result<bool> {
            anyhow::ensure!(!self.broken.load(Ordering::SeqCst), "the tree is broken");
            let mut unchecked = self.unchecked.lock().unwrap();
            let before = unchecked.len();
            unchecked.retain(|waiting| *waiting != piece);
            Ok(unchecked.len() != before)
        }

        fn read(&self, _: usize, _: u64, _: &mut [u8]) -> io::Result<()> {
            Err(io::ErrorKind::NotFound.into())
        }

        fn grow(&self, _: usize, _: u64) -> io::Result<()> {
            Ok(())
        }
    }

    fn room(free_disk: CapacityReader) -> Room {
        let mut limits = ExtractionLimits::unbounded();
        limits.min_free_bytes = 100;
        Room::new(Arc::new(limits), free_disk)
    }

    // What the intake said, in order.
    fn heard() -> (Arc<Mutex<Vec<Heard>>>, Hear) {
        let said = Arc::new(Mutex::new(Vec::new()));
        let kept = Arc::clone(&said);
        (
            said,
            Box::new(move |heard| kept.lock().unwrap().push(heard)),
        )
    }

    fn piece(bytes: &[u8]) -> Bytes {
        Bytes::copy_from_slice(bytes)
    }

    // A write that finds no space goes back to the session as one, which
    // keeps the torrent seeding; any other write the store could not make
    // fails the torrent, and is said to.
    #[test]
    fn a_full_disk_is_refused_as_no_room_and_nothing_is_kept() {
        let disk = Arc::new(Disk::default());
        let (said, hear) = heard();
        let intake = Intake::new(
            Arc::clone(&disk) as Arc<dyn Pieces>,
            room(CapacityReader::unknown()),
            hear,
        );
        intake.verified(0, &piece(b"aaaa")).unwrap();
        disk.full.store(true, Ordering::SeqCst);
        assert!(write_refused(
            &intake.verified(1, &piece(b"bbbb")).unwrap_err()
        ));
        disk.full.store(false, Ordering::SeqCst);
        intake.verified(1, &piece(b"bbbb")).unwrap();
        disk.broken.store(true, Ordering::SeqCst);
        assert!(!write_refused(
            &intake.verified(2, &piece(b"cccc")).unwrap_err()
        ));
        assert_eq!(*disk.took.lock().unwrap(), [(0, 4), (1, 4)]);
        let said = said.lock().unwrap().clone();
        let no_room = Heard::NoRoom(Wants {
            left: Some(4),
            ..Wants::default()
        });
        assert_eq!(said[..3], [Heard::Changed, no_room, Heard::Changed]);
        assert!(matches!(&said[3..], [Heard::Refused(why)] if why.contains("broken")));
    }

    // A piece that failed its hash goes to the store, and its pieces
    // changed only when it held some of it; a store that could not let it go
    // fails the torrent, and is said to.
    #[test]
    fn a_failed_piece_reaches_the_store_and_is_heard_when_it_held_some() {
        let disk = Arc::new(Disk::default());
        let (said, hear) = heard();
        let intake = Intake::new(
            Arc::clone(&disk) as Arc<dyn Pieces>,
            room(CapacityReader::unknown()),
            hear,
        );
        intake.verified(0, &piece(b"aaaa")).unwrap();
        assert!(intake.failed(0).unwrap());
        assert!(!intake.failed(1).unwrap());
        disk.broken.store(true, Ordering::SeqCst);
        assert!(intake.failed(2).is_err());
        assert_eq!(*disk.failed.lock().unwrap(), [0, 1]);
        assert!(disk.took.lock().unwrap().is_empty());
        let said = said.lock().unwrap().clone();
        assert_eq!(said[..2], [Heard::Changed, Heard::Changed]);
        assert!(matches!(&said[2..], [Heard::Refused(why)] if why.contains("broken")));
    }

    // A check of every piece beginning and each piece it finds change the
    // store's pieces, and are heard when they did; a store that could not take
    // them fails the torrent, and is said to.
    #[test]
    fn a_check_and_what_it_finds_reach_the_store_and_are_heard() {
        let disk = Arc::new(Disk::default());
        let (said, hear) = heard();
        let intake = Intake::new(
            Arc::clone(&disk) as Arc<dyn Pieces>,
            room(CapacityReader::unknown()),
            hear,
        );
        assert!(!intake.checking().unwrap());
        intake.verified(0, &piece(b"aaaa")).unwrap();
        assert!(intake.checking().unwrap());
        assert_eq!(*disk.unchecked.lock().unwrap(), [0]);
        assert!(intake.found(0).unwrap());
        assert!(!intake.found(0).unwrap());
        assert!(disk.unchecked.lock().unwrap().is_empty());
        disk.broken.store(true, Ordering::SeqCst);
        assert!(intake.checking().is_err());
        assert!(intake.found(0).is_err());
        let said = said.lock().unwrap().clone();
        assert_eq!(said[..3], [Heard::Changed, Heard::Changed, Heard::Changed]);
        assert!(
            matches!(&said[3..], [Heard::Refused(a), Heard::Refused(b)] if a.contains("broken") && b.contains("broken"))
        );
    }

    // The budget refuses as no room a piece that would eat into the
    // reserve, and admits pieces once a reading shows room, counting each
    // against it.
    #[test]
    fn the_disk_budget_refuses_what_would_eat_into_the_reserve() {
        use weaver_server_disk::operations::disk::{Capacity, CapacityReading};

        let sampled_at = std::time::Instant::now();
        let reading = Arc::new(Mutex::new(CapacityReading {
            available_bytes: 102,
            total_bytes: 1000,
            sampled_at,
            stale: false,
        }));
        let read = Arc::clone(&reading);
        let disk = Arc::new(Disk::default());
        let (said, hear) = heard();
        let intake = Intake::new(
            Arc::clone(&disk) as Arc<dyn Pieces>,
            room(CapacityReader::from_fn(move || {
                Capacity::Known(*read.lock().unwrap())
            })),
            hear,
        );
        // Two bytes fit over the reserve; the next four do not.
        intake.verified(0, &piece(b"aa")).unwrap();
        assert!(write_refused(
            &intake.verified(1, &piece(b"bbbb")).unwrap_err()
        ));
        assert_eq!(*disk.took.lock().unwrap(), [(0, 2)]);
        // A new reading with room admits them.
        {
            let mut now = reading.lock().unwrap();
            now.available_bytes = 1000;
            now.sampled_at = sampled_at + std::time::Duration::from_secs(1);
        }
        intake.verified(1, &piece(b"bbbb")).unwrap();
        assert_eq!(*disk.took.lock().unwrap(), [(0, 2), (1, 4)]);
        assert_eq!(
            *said.lock().unwrap(),
            [Heard::Changed, Heard::NotBooked(4), Heard::Changed]
        );
    }

    // Two writers on one volume book on the one ledger: what the first
    // promised, its slab of what was left included, the second cannot
    // spend; the first's next piece draws on its slab; a write that failed
    // gives its promise back, and one that landed counts against the
    // reading.
    #[test]
    fn writers_on_one_volume_share_the_ledger() {
        use weaver_server_disk::operations::disk::{Capacity, CapacityReading};

        let reading = CapacityReading {
            available_bytes: 110,
            total_bytes: 1000,
            sampled_at: std::time::Instant::now(),
            stale: false,
        };
        let reader = CapacityReader::from_fn(move || Capacity::Known(reading));
        let first = room(reader.clone());
        let second = room(reader.clone());
        // Ten bytes over the reserve: six promised to one, and the four
        // left are its slab.
        let six = first.admits(6).unwrap();
        assert_eq!(reader.ledger().promised(), 10);
        assert!(second.admits(4).is_none());
        let four = first.admits(4).unwrap();
        assert_eq!(reader.ledger().promised(), 10);
        // A write that failed gives its room back for the other.
        drop(four);
        assert_eq!(reader.ledger().promised(), 6);
        let theirs = second.admits(4).unwrap();
        assert_eq!(reader.ledger().promised(), 10);
        drop(theirs);
        // One that landed is no longer promised but still spent against
        // the reading it followed.
        six.landed();
        assert_eq!(reader.ledger().promised(), 0);
        assert!(second.admits(5).is_none());
        assert!(second.admits(4).is_some());
    }

    // A piece the store could not write gives back what it was promised;
    // the slab booked with it stays the torrent's for its next piece, which
    // draws on it, and goes back with the torrent's room.
    #[test]
    fn a_refused_write_gives_its_promise_back() {
        use weaver_server_disk::operations::disk::{Capacity, CapacityReading};

        let reading = CapacityReading {
            available_bytes: 1000,
            total_bytes: 1000,
            sampled_at: std::time::Instant::now(),
            stale: false,
        };
        let reader = CapacityReader::from_fn(move || Capacity::Known(reading));
        let disk = Arc::new(Disk::default());
        let (_, hear) = heard();
        let intake = Intake::new(
            Arc::clone(&disk) as Arc<dyn Pieces>,
            room(reader.clone()),
            hear,
        );
        disk.full.store(true, Ordering::SeqCst);
        assert!(intake.verified(0, &piece(b"aaaa")).is_err());
        assert_eq!(reader.ledger().promised(), SLAB_LEFT);
        disk.full.store(false, Ordering::SeqCst);
        intake.verified(0, &piece(b"aaaa")).unwrap();
        assert_eq!(reader.ledger().promised(), SLAB_LEFT - 4);
        drop(intake);
        assert_eq!(reader.ledger().promised(), 0);
    }

    // The slab a four-byte piece books with it on a disk 900 bytes over the
    // reserve: the rest.
    const SLAB_LEFT: u64 = 900 - 4;

    fn known(available_bytes: u64) -> CapacityReader {
        use weaver_server_disk::operations::disk::{Capacity, CapacityReading};

        let reading = CapacityReading {
            available_bytes,
            total_bytes: 1 << 40,
            sampled_at: std::time::Instant::now(),
            stale: false,
        };
        CapacityReader::from_fn(move || Capacity::Known(reading))
    }

    // A write the disk refuses for a reason other than room, read-only
    // here, fails the torrent as it fails a usenet job: the one classifier
    // both use says it is not one that room made would let go.
    #[test]
    fn a_write_refused_for_no_want_of_room_fails_the_torrent() {
        let disk = Arc::new(Disk::default());
        let (said, hear) = heard();
        let reader = known(1000);
        let intake = Intake::new(
            Arc::clone(&disk) as Arc<dyn Pieces>,
            room(reader.clone()),
            hear,
        );
        disk.read_only.store(true, Ordering::SeqCst);
        assert!(intake.verified(0, &piece(b"aaaa")).is_err());
        assert!(
            matches!(&said.lock().unwrap()[..], [Heard::Refused(why)] if why.contains("writing a share"))
        );
        assert_eq!(reader.ledger().promised(), SLAB_LEFT);
        drop(intake);
        assert_eq!(reader.ledger().promised(), 0);
    }

    // A write that panics gives its booking back as it unwinds: nothing
    // stays promised for a piece that never landed.
    #[test]
    fn a_write_that_panics_gives_its_booking_back() {
        let disk = Arc::new(Disk::default());
        let (_, hear) = heard();
        let reader = known(1000);
        let intake = Intake::new(
            Arc::clone(&disk) as Arc<dyn Pieces>,
            room(reader.clone()),
            hear,
        );
        disk.panics.store(true, Ordering::SeqCst);
        let unwound = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            intake.verified(0, &piece(b"aaaa"))
        }));
        assert!(unwound.is_err());
        assert_eq!(reader.ledger().promised(), SLAB_LEFT);
        drop(intake);
        assert_eq!(reader.ledger().promised(), 0);
    }

    // What a wake handed the torrent is spent by its next pieces before
    // they book, and what is left goes back when its run ends; a landed
    // piece after a wake that promised it room says so, once.
    #[test]
    fn room_a_wake_hands_is_spent_first_and_given_back_after() {
        let disk = Arc::new(Disk::default());
        let (said, hear) = heard();
        // Nothing over the reserve: only what was handed lets a piece go.
        let reader = known(100);
        let intake = Intake::new(
            Arc::clone(&disk) as Arc<dyn Pieces>,
            room(reader.clone()),
            hear,
        );
        reader.ledger().promise(6);
        intake.room().hand(6);
        intake.room().promise_room();
        intake.verified(0, &piece(b"aaaa")).unwrap();
        assert_eq!(intake.room().in_hand(), 2);
        assert_eq!(reader.ledger().promised(), 2);
        assert!(write_refused(
            &intake.verified(1, &piece(b"bbbb")).unwrap_err()
        ));
        intake.room().give_back();
        assert_eq!(reader.ledger().promised(), 0);
        assert_eq!(
            *said.lock().unwrap(),
            [Heard::Landed, Heard::Changed, Heard::NotBooked(4)]
        );
    }

    // A piece of a torrent whose disk is not known yet is let through by
    // nothing: it is refused as a booking, and nothing is written, so
    // nothing lands unchecked. A place found for a folder the pieces no
    // longer go to is not taken. Once its disk is known to lie off the
    // roots', pieces book on no ledger and land.
    #[test]
    fn a_piece_waits_for_its_disk_to_be_known_and_books_nothing_off_the_roots() {
        let disk = Arc::new(Disk::default());
        let (said, hear) = heard();
        let intake = Intake::new(
            Arc::clone(&disk) as Arc<dyn Pieces>,
            Room::aimed(
                Arc::new(ExtractionLimits::unbounded()),
                PathBuf::from("/elsewhere"),
            ),
            hear,
        );
        assert!(write_refused(
            &intake.verified(0, &piece(b"aaaa")).unwrap_err()
        ));
        assert!(disk.took.lock().unwrap().is_empty());
        assert_eq!(*said.lock().unwrap(), [Heard::NotBooked(4)]);
        let off = |dir: &str| {
            Place::new(
                weaver_server_disk::operations::room_wait::Disk::Path(PathBuf::from(dir)),
                PathBuf::from(dir),
                None,
            )
        };
        assert!(!intake.room().set_place(off("/before")));
        assert!(intake.room().place().is_none());
        let reader = known(100);
        assert!(intake.room().set_place(off("/elsewhere")));
        intake.verified(1, &piece(b"bbbb")).unwrap();
        assert_eq!(reader.ledger().promised(), 0);
        assert_eq!(*disk.took.lock().unwrap(), [(1, 4)]);
        // Aimed at another folder, its place goes until that one is found.
        assert!(intake.room().aim(Path::new("/moved")));
        assert!(intake.room().place().is_none());
    }
}
