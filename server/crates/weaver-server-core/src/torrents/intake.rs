// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Verified pieces on their way to the store, under the disk budget. A piece
// the budget refuses, or whose write finds no space, is refused to the
// session as a write that found no room: the session puts the torrent in
// upload mode, where it fetches nothing, keeps seeding, and tries again
// later. Any other write the store could not make fails the torrent.

use std::io;
use std::sync::{Arc, Mutex, PoisonError};

use bytes::Bytes;
use weaver_graph::holds::{HoldsAccountant, HoldsNamespace};
use weaver_torrent::storage::write_refused;

use crate::operations::disk::{CapacityDebits, CapacityReader};
use crate::pipeline::ExtractionLimits;
use crate::torrents::storage::Pieces;

// The disk budget the pieces are written under: the reserve the job's
// limits keep free on the volume the reader samples, against what was
// written since its last reading.
pub(crate) struct Room {
    limits: Arc<ExtractionLimits>,
    free_disk: CapacityReader,
    debits: Mutex<CapacityDebits>,
}

impl Room {
    pub(crate) fn new(limits: Arc<ExtractionLimits>, free_disk: CapacityReader) -> Self {
        Self {
            limits,
            free_disk,
            debits: Mutex::new(CapacityDebits::default()),
        }
    }

    // Whether `bytes` more fit, counted against the reading when they do. A
    // volume never read is never refused.
    fn admits(&self, bytes: u64) -> bool {
        let mut debits = self.debits.lock().unwrap_or_else(PoisonError::into_inner);
        let Some(reading) = debits.apply(self.free_disk.current()).reading() else {
            return true;
        };
        if self.limits.short_of_reserve(&reading, bytes).is_some() {
            return false;
        }
        debits.debit(bytes);
        true
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
            return Err(
                anyhow::Error::from(io::Error::from(io::ErrorKind::StorageFull)).context(format!(
                    "{bytes} bytes more would eat into the disk reserve"
                )),
            );
        }
        match self.inner.verified(piece, held) {
            Ok(()) => {
                (self.hear)(Heard::Changed);
                Ok(())
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

    fn readv(&self, file: usize, offset: u64, outs: &mut [&mut [u8]]) -> io::Result<()> {
        self.inner.readv(file, offset, outs)
    }

    fn grow(&self, file: usize, length: u64) -> io::Result<()> {
        self.inner.grow(file, length)
    }
}

#[cfg(test)]
mod tests {
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
        assert_eq!(said[..2], [Heard::Changed, Heard::Changed]);
        assert!(matches!(&said[2..], [Heard::Refused(why)] if why.contains("broken")));
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
        assert_eq!(*said.lock().unwrap(), [Heard::Changed, Heard::Changed]);
    }
}
