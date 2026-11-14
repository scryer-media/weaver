// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Verified pieces on their way to the store, under the disk budget. A piece
// the budget refuses, or whose write finds no space, is refused to the
// session as a write that found no room: the session puts the torrent in
// upload mode, where it fetches nothing and keeps seeding, and the owner
// hears what the write wanted, to wait for room as any job does. Any other
// write the store could not make fails the torrent.

use std::io;
use std::sync::Arc;

use bytes::Bytes;
use weaver_graph::holds::{HoldsAccountant, HoldsNamespace};
use weaver_torrent::storage::{wants_room, write_refused};

use crate::operations::disk::{CapacityReader, checked_room, is_quota_exceeded};
use crate::operations::room_wait::Wants;
use crate::pipeline::ExtractionLimits;
use crate::torrents::storage::Pieces;

// The disk budget the pieces are written under: the reserve the job's
// limits keep free on the volume the reader samples, booked on the ledger
// every writer on that volume shares, usenet's included, so two writers
// never spend the same free space.
pub(crate) struct Room {
    limits: Arc<ExtractionLimits>,
    free_disk: CapacityReader,
}

impl Room {
    pub(crate) fn new(limits: Arc<ExtractionLimits>, free_disk: CapacityReader) -> Self {
        Self { limits, free_disk }
    }

    // Whether `bytes` more fit over the reserve, promised on the ledger when
    // they do. A volume never read is never refused.
    fn admits(&self, bytes: u64) -> bool {
        let ledger = self.free_disk.ledger();
        if bytes > checked_room(ledger, &self.free_disk, self.limits.min_free_bytes) {
            return false;
        }
        ledger.promise(bytes);
        true
    }

    // The promised `bytes` were written.
    fn landed(&self, bytes: u64) {
        self.free_disk.ledger().landed(bytes, 0);
    }

    // The promised `bytes` were not written.
    fn release(&self, bytes: u64) {
        self.free_disk.ledger().release(bytes);
    }
}

// Where a torrent's bytes in memory are counted: the accountant every job
// shares, under the namespace of the job's root.
#[derive(Clone)]
pub(crate) struct Holds {
    pub(crate) accountant: Arc<HoldsAccountant>,
    pub(crate) namespace: HoldsNamespace,
}

// What the intake tells its owner.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Heard {
    // The pieces had changed: one verified, one failed its hash, a check of
    // every piece began, or the check found one.
    Changed,
    // The store could not take a piece, and why: the torrent's failure. A
    // write that found no room is not one.
    Refused(String),
    // A piece found no room on the disk, over the reserve or at all, and
    // what its write wanted: the torrent waits in upload mode for room.
    NoRoom(Wants),
    // A piece the session counted made one of the torrent's files whole.
    FileWhole,
}

pub(crate) type Hear = Box<dyn Fn(Heard) + Send + Sync>;

pub(crate) struct Intake {
    inner: Arc<dyn Pieces>,
    room: Room,
    hear: Hear,
}

impl Intake {
    pub(crate) fn new(inner: Arc<dyn Pieces>, room: Room, hear: Hear) -> Self {
        Self { inner, room, hear }
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

impl Pieces for Intake {
    fn verified(&self, piece: u32, held: &Bytes) -> anyhow::Result<()> {
        let bytes = held.len() as u64;
        if !self.room.admits(bytes) {
            (self.hear)(Heard::NoRoom(Wants {
                left: Some(bytes),
                ..Wants::default()
            }));
            return Err(
                anyhow::Error::from(io::Error::from(io::ErrorKind::StorageFull)).context(format!(
                    "{bytes} bytes more would eat into the disk reserve"
                )),
            );
        }
        let wrote = self.inner.verified(piece, held);
        match &wrote {
            Ok(()) => self.room.landed(bytes),
            Err(_) => self.room.release(bytes),
        }
        match wrote {
            Ok(()) => {
                (self.hear)(Heard::Changed);
                Ok(())
            }
            Err(error) if wants_room(&error) => {
                let quota = error
                    .chain()
                    .filter_map(|cause| cause.downcast_ref::<io::Error>())
                    .any(is_quota_exceeded);
                (self.hear)(Heard::NoRoom(Wants {
                    left: Some(bytes),
                    quota,
                    needs: None,
                }));
                Err(error)
            }
            Err(error) if write_refused(&error) => Err(error),
            Err(error) => {
                (self.hear)(Heard::Refused(format!("{error:#}")));
                Err(error)
            }
        }
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

    use super::*;

    // A store that refuses every write while the disk is full, and
    // any write or verdict at all while it is broken; the pieces it was told
    // failed, in order, and those a check has yet to find.
    #[derive(Default)]
    struct Disk {
        full: AtomicBool,
        broken: AtomicBool,
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
        use crate::operations::disk::{Capacity, CapacityReading};

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
        let no_room = Heard::NoRoom(Wants {
            left: Some(4),
            ..Wants::default()
        });
        assert_eq!(
            *said.lock().unwrap(),
            [Heard::Changed, no_room, Heard::Changed]
        );
    }

    // Two writers on one volume book on the one ledger: what the first
    // promised the second cannot spend, a write that failed gives its
    // promise back, and one that landed counts against the reading.
    #[test]
    fn writers_on_one_volume_share_the_ledger() {
        use crate::operations::disk::{Capacity, CapacityReading};

        let reading = CapacityReading {
            available_bytes: 110,
            total_bytes: 1000,
            sampled_at: std::time::Instant::now(),
            stale: false,
        };
        let reader = CapacityReader::from_fn(move || Capacity::Known(reading));
        let first = room(reader.clone());
        let second = room(reader.clone());
        // Ten bytes over the reserve: six promised to one leave four.
        assert!(first.admits(6));
        assert!(!second.admits(5));
        assert!(second.admits(4));
        assert_eq!(reader.ledger().promised(), 10);
        // A write that failed gives its room back for the other.
        second.release(4);
        assert_eq!(reader.ledger().promised(), 6);
        // One that landed is no longer promised but still spent against
        // the reading it followed.
        first.landed(6);
        assert_eq!(reader.ledger().promised(), 0);
        assert!(!second.admits(5));
        assert!(second.admits(4));
    }

    // A piece the store could not write gives back what it was promised.
    #[test]
    fn a_refused_write_gives_its_promise_back() {
        use crate::operations::disk::{Capacity, CapacityReading};

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
        assert_eq!(reader.ledger().promised(), 0);
        disk.full.store(false, Ordering::SeqCst);
        intake.verified(0, &piece(b"aaaa")).unwrap();
        assert_eq!(reader.ledger().promised(), 0);
    }
}
