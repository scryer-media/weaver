// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Verified pieces on their way to the job. When the disk budget refuses
// them, or a write finds no space, the pieces wait here, in hand, instead of
// being lost: a peer reading one is served from here, and once the budget
// admits them again they go down in the order they verified. What waits is
// held against the resident budget every job shares; a piece past it is
// refused like any write the job could not take.

use std::io;
use std::sync::{Arc, Mutex, PoisonError};

use weaver_graph::holds::{HoldsAccountant, HoldsNamespace, Reserve};
use weaver_graph::{ByteRange, VolumeIndex};

use crate::operations::disk::{CapacityDebits, CapacityReader};
use crate::pipeline::graph::limits::ExtractionLimits;
use crate::pipeline::graph::root::pieces::Verified;
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

// Where the held pieces are counted: the accountant every job shares, under
// the namespace of the job's root.
pub(crate) struct Holds {
    pub(crate) accountant: Arc<HoldsAccountant>,
    pub(crate) namespace: HoldsNamespace,
}

fn size(shares: &[Verified]) -> u64 {
    shares.iter().map(|share| share.bytes.len() as u64).sum()
}

// Whether an error is the disk being out of space.
pub(crate) fn disk_full(error: &anyhow::Error) -> bool {
    error.chain().any(|cause| {
        cause.downcast_ref::<io::Error>().is_some_and(|error| {
            matches!(
                error.kind(),
                io::ErrorKind::StorageFull | io::ErrorKind::QuotaExceeded
            )
        })
    })
}

// What the intake tells its owner.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum Heard {
    // Pieces reached the job.
    Took,
    // The first piece is held: the torrent waits for room.
    Held,
    // The job could not take a piece, or none could be held, and why. The
    // session counts a piece it handed over as had whatever was said, so
    // this is the torrent's failure.
    Refused(String),
}

pub(crate) type Hear = Box<dyn Fn(Heard) + Send + Sync>;

pub(crate) struct Intake {
    inner: Arc<dyn Pieces>,
    room: Room,
    holds: Holds,
    // Pieces the disk had no room for, in the order they verified.
    held: Mutex<Vec<Vec<Verified>>>,
    hear: Hear,
}

impl Intake {
    pub(crate) fn new(inner: Arc<dyn Pieces>, room: Room, holds: Holds, hear: Hear) -> Self {
        Self {
            inner,
            room,
            holds,
            held: Mutex::new(Vec::new()),
            hear,
        }
    }

    // Hand shares to the job, as the result says.
    fn hand(&self, shares: Vec<Verified>) -> Result<(), (Vec<Verified>, anyhow::Error)> {
        let result = self.inner.verified(shares.clone());
        if result.is_ok() {
            (self.hear)(Heard::Took);
        }
        result.map_err(|error| (shares, error))
    }

    fn refuse(&self, error: anyhow::Error) -> anyhow::Error {
        (self.hear)(Heard::Refused(format!("{error:#}")));
        error
    }

    fn held(&self) -> std::sync::MutexGuard<'_, Vec<Vec<Verified>>> {
        self.held.lock().unwrap_or_else(PoisonError::into_inner)
    }

    // Keep shares for later, when the resident budget has room for them;
    // the first one kept is news, as the torrent now waits.
    fn hold(&self, held: &mut Vec<Vec<Verified>>, shares: Vec<Verified>) -> anyhow::Result<()> {
        let bytes = size(&shares);
        if let Reserve::Breached(budget) = self
            .holds
            .accountant
            .reserve_resident(self.holds.namespace, bytes)
        {
            return Err(self.refuse(anyhow::anyhow!(
                "no room on disk, and {bytes} bytes more would pass the {budget:?} budget"
            )));
        }
        held.push(shares);
        if held.len() == 1 {
            (self.hear)(Heard::Held);
        }
        Ok(())
    }

    // Whether pieces are waiting for room.
    pub(crate) fn holding(&self) -> bool {
        !self.held().is_empty()
    }

    // Send what waits down again, as far as the budget admits; true when
    // some still waits.
    pub(crate) fn retry(&self) -> bool {
        let mut held = self.held();
        while let Some(first) = held.first() {
            let bytes = size(first);
            if !self.room.admits(bytes) {
                return true;
            }
            let shares = held.remove(0);
            match self.hand(shares) {
                Err((shares, error)) if disk_full(&error) => {
                    held.insert(0, shares);
                    return true;
                }
                Err((_, error)) => {
                    self.refuse(error);
                }
                Ok(()) => {}
            }
            self.holds
                .accountant
                .release_resident(self.holds.namespace, bytes);
        }
        false
    }
}

impl Drop for Intake {
    fn drop(&mut self) {
        let held: u64 = self.held().iter().map(|shares| size(shares)).sum();
        if held > 0 {
            self.holds
                .accountant
                .release_resident(self.holds.namespace, held);
        }
    }
}

impl Pieces for Intake {
    fn verified(&self, shares: Vec<Verified>) -> anyhow::Result<()> {
        let mut held = self.held();
        if !held.is_empty() || !self.room.admits(size(&shares)) {
            return self.hold(&mut held, shares);
        }
        match self.hand(shares) {
            Ok(()) => Ok(()),
            Err((shares, error)) if disk_full(&error) => self.hold(&mut held, shares),
            Err((_, error)) => Err(self.refuse(error)),
        }
    }

    fn posted(&self, volume: VolumeIndex, range: ByteRange, into: &mut Vec<u8>) -> bool {
        if self.inner.posted(volume, range, into) {
            return true;
        }
        let held = self.held();
        let Some(share) = held.iter().flatten().find(|share| {
            share.volume == volume
                && share.offset <= range.start
                && range.end <= share.offset + share.bytes.len() as u64
        }) else {
            return false;
        };
        let at = (range.start - share.offset) as usize;
        into.extend_from_slice(&share.bytes[at..at + range.len() as usize]);
        true
    }
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicBool, Ordering};

    use super::*;

    // A job's side that refuses every write while the disk is full.
    #[derive(Default)]
    struct Disk {
        full: AtomicBool,
        took: Mutex<Vec<(u32, u64)>>,
    }

    impl Pieces for Disk {
        fn verified(&self, shares: Vec<Verified>) -> anyhow::Result<()> {
            if self.full.load(Ordering::SeqCst) {
                return Err(
                    anyhow::Error::from(io::Error::from(io::ErrorKind::StorageFull))
                        .context("writing a share"),
                );
            }
            let mut took = self.took.lock().unwrap();
            took.extend(shares.iter().map(|share| (share.piece, share.offset)));
            Ok(())
        }

        fn posted(&self, _: VolumeIndex, _: ByteRange, _: &mut Vec<u8>) -> bool {
            false
        }
    }

    fn room(free_disk: CapacityReader) -> Room {
        let mut limits = ExtractionLimits::unbounded();
        limits.min_free_bytes = 100;
        Room::new(Arc::new(limits), free_disk)
    }

    const NAMESPACE: HoldsNamespace = HoldsNamespace {
        scope: 7,
        node: weaver_graph::NodeId::ROOT,
    };

    fn accountant(resident_limit: u64) -> Arc<HoldsAccountant> {
        Arc::new(HoldsAccountant::new(weaver_graph::holds::HoldBudgets {
            resident_limit,
            scratch_total: u64::MAX,
            scratch_ceiling: u64::MAX,
            disk_reserve: 0,
        }))
    }

    fn holds(accountant: &Arc<HoldsAccountant>) -> Holds {
        Holds {
            accountant: Arc::clone(accountant),
            namespace: NAMESPACE,
        }
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

    fn share(piece: u32, offset: u64, bytes: &[u8]) -> Vec<Verified> {
        vec![Verified {
            volume: VolumeIndex(0),
            piece,
            offset,
            bytes: bytes::Bytes::copy_from_slice(bytes),
        }]
    }

    #[test]
    fn a_full_disk_holds_pieces_in_hand_until_there_is_room() {
        let disk = Arc::new(Disk::default());
        let accountant = accountant(u64::MAX);
        let (said, hear) = heard();
        let intake = Intake::new(
            Arc::clone(&disk) as Arc<dyn Pieces>,
            room(CapacityReader::unknown()),
            holds(&accountant),
            hear,
        );
        intake.verified(share(0, 0, b"aaaa")).unwrap();
        disk.full.store(true, Ordering::SeqCst);
        intake.verified(share(1, 4, b"bbbb")).unwrap();
        assert!(intake.holding());
        // A later piece waits behind the held one even once there is room.
        disk.full.store(false, Ordering::SeqCst);
        intake.verified(share(2, 8, b"cccc")).unwrap();
        assert_eq!(*disk.took.lock().unwrap(), [(0, 0)]);
        // A peer reading a held piece is served from it.
        let mut read = Vec::new();
        assert!(intake.posted(VolumeIndex(0), ByteRange::at(5, 2), &mut read));
        assert_eq!(read, b"bb");
        assert!(!intake.posted(VolumeIndex(0), ByteRange::at(2, 4), &mut Vec::new()));
        // What waits is held against the resident budget.
        assert_eq!(accountant.usage().resident, 8);
        // Still full: nothing moves.
        disk.full.store(true, Ordering::SeqCst);
        assert!(intake.retry());
        disk.full.store(false, Ordering::SeqCst);
        assert!(!intake.retry());
        assert!(!intake.holding());
        assert_eq!(accountant.usage().resident, 0);
        assert_eq!(*disk.took.lock().unwrap(), [(0, 0), (1, 4), (2, 8)]);
        assert_eq!(
            *said.lock().unwrap(),
            [Heard::Took, Heard::Held, Heard::Took, Heard::Took]
        );
    }

    // While the disk is full the pieces in hand never pass the resident
    // budget: the one that would is refused, and said to be.
    #[test]
    fn held_pieces_stay_within_the_resident_budget() {
        let disk = Arc::new(Disk::default());
        disk.full.store(true, Ordering::SeqCst);
        let accountant = accountant(10);
        let (said, hear) = heard();
        let intake = Intake::new(
            Arc::clone(&disk) as Arc<dyn Pieces>,
            room(CapacityReader::unknown()),
            holds(&accountant),
            hear,
        );
        intake.verified(share(0, 0, b"aaaa")).unwrap();
        intake.verified(share(1, 4, b"bbbb")).unwrap();
        intake.verified(share(2, 8, b"cccc")).unwrap_err();
        assert_eq!(accountant.usage().resident, 8);
        assert!(accountant.usage().resident <= accountant.budgets().resident_limit);
        let said = said.lock().unwrap().clone();
        assert_eq!(said[0], Heard::Held);
        assert!(matches!(&said[1], Heard::Refused(why) if why.contains("budget")));
        // A peer is never served the refused piece.
        assert!(!intake.posted(VolumeIndex(0), ByteRange::at(8, 4), &mut Vec::new()));
        // Dropping the intake gives back what it held.
        drop(intake);
        assert_eq!(accountant.usage().resident, 0);
    }

    // The budget holds pieces that would eat into the reserve, and lets
    // them down once a reading shows room, counting each against it.
    #[test]
    fn the_disk_budget_holds_pieces_until_it_admits_them() {
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
        let accountant = accountant(u64::MAX);
        let intake = Intake::new(
            Arc::clone(&disk) as Arc<dyn Pieces>,
            room(CapacityReader::from_fn(move || {
                Capacity::Known(*read.lock().unwrap())
            })),
            holds(&accountant),
            Box::new(|_| {}),
        );
        // Two bytes fit over the reserve; the next four do not.
        intake.verified(share(0, 0, b"aa")).unwrap();
        intake.verified(share(1, 2, b"bbbb")).unwrap();
        assert!(intake.holding());
        assert_eq!(*disk.took.lock().unwrap(), [(0, 0)]);
        assert!(intake.retry());
        // A new reading with room lets them down.
        {
            let mut now = reading.lock().unwrap();
            now.available_bytes = 1000;
            now.sampled_at = sampled_at + std::time::Duration::from_secs(1);
        }
        assert!(!intake.retry());
        assert!(!intake.holding());
        assert_eq!(*disk.took.lock().unwrap(), [(0, 0), (1, 2)]);
    }

    #[test]
    fn only_a_full_disk_is_held() {
        let full = anyhow::Error::from(io::Error::from(io::ErrorKind::StorageFull)).context("x");
        assert!(disk_full(&full));
        let other = anyhow::Error::from(io::Error::from(io::ErrorKind::PermissionDenied));
        assert!(!disk_full(&other));
    }
}
