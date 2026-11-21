// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// One job's account of the room it takes on one disk: what its start
// promised, what it owes, what landed, and whether it waits for room. Every
// job class keeps its room through this, so the disk space rules live once.

use std::sync::Arc;

use crate::operations::disk::{CapacityReader, DiskLedger};

// How much is promised from a device beyond what a write needs when the
// ledger is asked, so that most writes touch nothing shared, and how much
// that was promised may land before the ledger hears of it.
pub const SLAB_BYTES: u64 = 64 * 1024 * 1024;

// What the job owes one device: the bytes its members took and have not
// written yet, and how much of that the device's ledger holds promised for
// it. While the job waits for room on the device, nothing it owes there is
// promised, so a job that waits holds no room another could use.
#[derive(Debug, Default)]
pub struct Spend {
    ledger: Option<Arc<DiskLedger>>,
    // Taken for members, not landed yet.
    pub owed: u64,
    // Of `owed`, what is not promised: all of it while the job waits.
    unbooked: u64,
    // What a repair the job holds back until there is room would write:
    // promised in hand when the job wakes, for the repair to spend.
    wanted: u64,
    // What members removed while the job waits held: the runs that built
    // them write them again once it wakes, so it is promised then, to spend
    // as they write.
    again: u64,
    // Promised beyond what is owed, spent before the ledger is asked again.
    slab: u64,
    // Of `slab`, what was promised for a held repair when the job woke and
    // no repair has taken yet.
    in_hand: u64,
    // What landed since the ledger last heard: of what was promised, and of
    // what was not.
    landed: u64,
    landed_unbooked: u64,
    // The job's place among those waiting for room on the device.
    waiting: Option<u64>,
    // What its start promised here for the posts it still had to fetch, and
    // has not landed yet: drawn as posts land, and on a disk of its own by
    // what the job delivers there. A wait for room leaves it promised.
    posted: u64,
}

impl Spend {
    // The ledger of the device `reader` reads, kept from the first use.
    pub fn bind(&mut self, reader: &CapacityReader) -> Arc<DiskLedger> {
        Arc::clone(
            self.ledger
                .get_or_insert_with(|| Arc::clone(reader.ledger())),
        )
    }

    // The ledger this account keeps its room on, once bound.
    pub fn ledger(&self) -> Option<&Arc<DiskLedger>> {
        self.ledger.as_ref()
    }

    // The ledger hears what landed.
    pub fn settle(&mut self) {
        if let Some(ledger) = &self.ledger {
            ledger.landed(self.landed, self.landed_unbooked);
        }
        self.landed = 0;
        self.landed_unbooked = 0;
    }

    // `bytes` of what is owed landed: what was not promised first. The
    // ledger hears of it once a slab's worth has landed, or sooner when it
    // is next asked.
    pub fn land(&mut self, bytes: u64) {
        let unbooked = bytes.min(self.unbooked);
        self.unbooked -= unbooked;
        self.owed = self.owed.saturating_sub(bytes);
        self.landed += bytes - unbooked;
        self.landed_unbooked += unbooked;
        if self.landed >= SLAB_BYTES {
            self.settle();
        }
    }

    // `bytes` that were owed will not land.
    pub fn forgo(&mut self, bytes: u64) {
        let unbooked = bytes.min(self.unbooked);
        self.unbooked -= unbooked;
        self.owed = self.owed.saturating_sub(bytes);
        if let Some(ledger) = &self.ledger {
            ledger.release(bytes - unbooked);
        }
    }

    // `bytes` more are owed to the device `reader` reads: promised from
    // what is in hand, or from the ledger while the device has room for
    // them above `reserve`, with up to a slab more. When it has not, the
    // job begins to wait for room and gives back all it promised here;
    // what it owes is promised again when it wakes. A device with no
    // reading, or only the last good one held across a failed probe, or a
    // job with no reserve, is not checked: only a good reading begins a
    // wait.
    pub fn book(&mut self, bytes: u64, reader: &CapacityReader, reserve: u64) {
        if bytes == 0 {
            return;
        }
        self.owed += bytes;
        if self.waiting.is_some() {
            self.unbooked += bytes;
            return;
        }
        if let Err((ledger, drawn)) = self.take(bytes, reader, reserve) {
            self.hold(&ledger, self.owed - (bytes - drawn));
        }
    }

    // `bytes` are about to be written on the device `reader` reads, by a
    // job that turns a write back rather than wait with it written: drawn
    // as `book` draws them, and promised on the ledger until they land
    // there or are given back. Whether they may be written. When the device
    // has no room for them above `reserve`, nothing changes here and the
    // job does not begin to wait.
    pub fn draw(&mut self, bytes: u64, reader: &CapacityReader, reserve: u64) -> bool {
        if bytes == 0 {
            return true;
        }
        match self.take(bytes, reader, reserve) {
            Ok(()) => true,
            Err((_, drawn)) => {
                self.posted += drawn;
                false
            }
        }
    }

    // What is in hand is spent first, as a repair's promise is, then what
    // the start promised here for the job's bytes, then the ledger is asked
    // for the rest with up to a slab more. Refused, the ledger and what was
    // drawn from the start's promise.
    fn take(
        &mut self,
        bytes: u64,
        reader: &CapacityReader,
        reserve: u64,
    ) -> Result<(), (Arc<DiskLedger>, u64)> {
        let drawn = bytes.saturating_sub(self.slab).min(self.posted);
        self.posted -= drawn;
        let bytes = bytes - drawn;
        if bytes <= self.slab {
            self.slab -= bytes;
            self.in_hand = self.in_hand.min(self.slab);
            return Ok(());
        }
        self.in_hand = 0;
        let ledger = self.bind(reader);
        self.settle();
        let need = bytes - self.slab;
        if let Some(extra) = ledger.book(reader, reserve, need, SLAB_BYTES) {
            self.slab = extra;
            return Ok(());
        }
        Err((ledger, drawn))
    }

    // The job begins to wait: all it promised here is given back, and all
    // it owes is unbooked until it wakes. `promised` is what of `owed` was
    // promised.
    fn hold(&mut self, ledger: &DiskLedger, promised: u64) {
        ledger.release(self.slab + promised.saturating_sub(self.unbooked));
        self.slab = 0;
        self.in_hand = 0;
        self.unbooked = self.owed;
        self.waiting = Some(ledger.wait());
    }

    // A repair would write `bytes` on the device `reader` reads before it
    // can start. Whether it may: the bytes are then promised in hand for
    // it. When the device has no room for them above `reserve`, the job
    // begins to wait, as it does for a write, and the repair waits with it;
    // what it would write is promised in hand when the job wakes. Each
    // repair that goes out is promised its own bytes: from what was put in
    // hand for a held one, else from the ledger, so two cannot go out on
    // one promise.
    pub fn want(&mut self, bytes: u64, reader: &CapacityReader, reserve: u64) -> bool {
        if self.waiting.is_some() {
            self.wanted = self.wanted.max(bytes);
            return false;
        }
        if bytes == 0 {
            return true;
        }
        if bytes <= self.in_hand {
            self.in_hand -= bytes;
            return true;
        }
        let ledger = self.bind(reader);
        self.settle();
        let need = bytes - self.in_hand;
        if ledger.book(reader, reserve, need, 0).is_some() {
            self.slab = self.slab.saturating_add(need);
            self.in_hand = 0;
            return true;
        }
        self.hold(&ledger, self.owed);
        self.wanted = bytes;
        false
    }

    // What `want` may take from what this holds: given back with
    // `give_back` should the repair not go out after all.
    pub fn taking(&self) -> (u64, u64) {
        (self.slab, self.in_hand)
    }

    // A `want` since `taken` is undone: what it promised is given back.
    pub fn give_back(&mut self, (slab, in_hand): (u64, u64)) {
        let promised = self.slab.saturating_sub(slab);
        if promised > 0
            && let Some(ledger) = &self.ledger
        {
            ledger.release(promised);
        }
        self.slab = slab;
        self.in_hand = in_hand;
    }

    // A repair that would write `bytes` here waits with the job: if the job
    // waits here already, for those bytes too; else it begins to, for them.
    pub fn wait_for(&mut self, bytes: u64, reader: &CapacityReader) {
        if self.waiting.is_some() {
            self.wanted = self.wanted.max(bytes);
            return;
        }
        if bytes == 0 {
            return;
        }
        let ledger = self.bind(reader);
        self.settle();
        self.hold(&ledger, self.owed);
        self.wanted = bytes;
    }

    // What a held repair would write here.
    pub fn wanted(&self) -> u64 {
        self.wanted
    }

    // What held repairs wanted is forgotten: each that would still write
    // wants its bytes anew.
    pub fn rewant(&mut self) {
        self.wanted = 0;
    }

    // The job's place among those waiting for room here, while it waits.
    pub fn waiting(&self) -> Option<u64> {
        self.waiting
    }

    // A member of `bytes` removed while the job waits is written again once
    // it wakes: it is owed then. Nothing while the job does not wait.
    pub fn write_again(&mut self, bytes: u64) {
        if self.waiting.is_some() {
            self.again = self.again.saturating_add(bytes);
        }
    }

    // What the device must have room for before the job wakes.
    pub fn needed(&self) -> u64 {
        self.unbooked
            .saturating_add(self.wanted)
            .saturating_add(self.again)
    }

    // The job has room again: all it owes is promised, what a held repair
    // would write is promised in hand for it, and it no longer waits.
    pub fn rebook(&mut self) {
        let Some(ledger) = &self.ledger else {
            return;
        };
        if self.waiting.is_some() {
            ledger.promise(self.needed());
            self.rebooked();
        }
    }

    // The job has room again, and all it owes was promised by the wake that
    // judged it: what it holds says so, and it no longer waits.
    pub fn rebooked(&mut self) {
        let Some(ledger) = &self.ledger else {
            return;
        };
        if self.waiting.take().is_some() {
            self.slab = self
                .slab
                .saturating_add(self.wanted)
                .saturating_add(self.again);
            self.in_hand = self.wanted;
            self.unbooked = 0;
            self.wanted = 0;
            self.again = 0;
            ledger.unwait();
        }
    }

    // The job started with `bytes` still to fetch promised on the ledger
    // `reader` reads: what lands draws on that promise. The ledger is not
    // touched; the promise exists.
    pub fn take_posted(&mut self, bytes: u64, reader: &CapacityReader) {
        self.bind(reader);
        self.posted = self.posted.saturating_add(bytes);
    }

    // What the start promised here and has not landed.
    pub fn posted(&self) -> u64 {
        self.posted
    }

    // `bytes` of the posts landed here: drawn from what the start promised,
    // which moves to landed on the ledger once a slab of it has, and what
    // lands past that promise is booked as it lands, as a repair's write
    // is. Constant work per write; the booking lock is taken once per slab
    // of the overflow at most.
    pub fn land_posted(&mut self, bytes: u64, reader: &CapacityReader, reserve: u64) {
        let drawn = bytes.min(self.posted);
        self.posted -= drawn;
        self.landed += drawn;
        if self.landed >= SLAB_BYTES {
            self.settle();
        }
        let over = bytes - drawn;
        if over > 0 {
            self.book(over, reader, reserve);
            self.land(over);
        }
    }

    // What the start promised for the posts and did not land is given back.
    pub fn release_posted(&mut self) {
        let posted = std::mem::take(&mut self.posted);
        self.settle();
        if let Some(ledger) = &self.ledger {
            ledger.release(posted);
        }
    }

    // All that was promised is given back, and the job no longer waits.
    pub fn release_all(&mut self) {
        self.release_posted();
        if let Some(ledger) = &self.ledger {
            ledger.release(self.slab + (self.owed - self.unbooked));
            if self.waiting.take().is_some() {
                ledger.unwait();
            }
        }
        self.owed = 0;
        self.unbooked = 0;
        self.wanted = 0;
        self.again = 0;
        self.slab = 0;
        self.in_hand = 0;
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;
    use std::time::Duration;

    use super::*;
    use crate::operations::disk::{DiskSpace, SharedProbeFn, StorageCapacity, StorageRoot};

    // A start promised 65,536 bytes; four 16,384 byte pieces draw on it,
    // each landing as a torrent's booking lands: the ledger's promised
    // figure falls by each piece's bytes and ends at zero, so no byte was
    // promised twice, and nothing is left to give back.
    #[test]
    fn pieces_draw_on_the_start_promise_and_land_once() {
        let dir = tempfile::tempdir().unwrap();
        let probe: SharedProbeFn = Arc::new(|_: &Path| {
            Ok(DiskSpace {
                total_bytes: 1 << 40,
                available_bytes: 1 << 30,
            })
        });
        let capacity = StorageCapacity::with_probe(
            [0, 1, 2].map(|_| dir.path().to_path_buf()),
            Duration::from_secs(365 * 24 * 60 * 60),
            probe,
        );
        capacity.wait_for_probes(StorageRoot::Complete, 1);
        let reader = capacity.reader(StorageRoot::Complete);
        let ledger = capacity.ledger(StorageRoot::Complete);
        ledger.promise(65_536);
        let mut spend = Spend::default();
        spend.take_posted(65_536, &reader);
        for left in [49_152, 32_768, 16_384, 0] {
            assert!(spend.draw(16_384, &reader, 100));
            assert_eq!(ledger.promised_total(), left + 16_384, "a draw books nothing");
            ledger.landed(16_384, 0);
            assert_eq!(ledger.promised_total(), left);
        }
        assert_eq!(spend.posted(), 0);
        spend.release_all();
        assert_eq!(ledger.promised_total(), 0);
    }
}
