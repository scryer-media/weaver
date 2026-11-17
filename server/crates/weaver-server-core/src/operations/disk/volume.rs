// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, OnceLock};

use super::StorageKind;
use super::reader::VolumeReads;

// The most of a volume's hard limit one write counts at once
// (`Volume::write_piece`).
const WRITE_PIECE: u64 = 4 << 20;

// The most owners one set of writers tells apart: one bit each in a
// volume's mark of who asked for a wake, and one prepaid slot each.
pub(crate) const OWNERS: usize = 64;

// One volume as the OS presents it: every path with the same device id.
pub(crate) struct Volume {
    // The device id the disk owner holds for the volume's paths; `None`
    // when the OS gave none.
    pub(crate) device: Option<u64>,
    // The mount path or drive root shown to the user.
    pub(crate) label: String,
    // Bytes its writer has written, and the nanoseconds spent inside those
    // write calls. After each call the writer adds the nanoseconds first
    // and the bytes last, with `Release`; the rate sampler loads the bytes
    // first, with `Acquire`, and the nanoseconds after, so it never reads a
    // call's bytes without its time.
    pub(crate) written_bytes: AtomicU64,
    pub(crate) write_nanos: AtomicU64,
    // Write calls its writer has made, and how many of them did not start
    // where the previous write to the same file ended.
    pub(crate) write_calls: AtomicU64,
    pub(crate) write_seeks: AtomicU64,
    // Bytes waiting in memory for its writer.
    pub(crate) buffered_bytes: AtomicU64,
    // Bytes a caller holds in memory for this volume and has not handed to
    // its writer yet, as that caller reports them: a gauge, not counted
    // against any limit. Only the one caller that reports it changes it.
    pub(crate) arrived_bytes: AtomicU64,
    // What counts against the hard limit (`unwritten_bytes`): what callers
    // reserved ahead of the bytes they will write (`Reservation`), and what
    // waits for the writer. Reservations and writes raise it; releases and
    // the writer, as it writes, lower it.
    counted_bytes: AtomicU64,
    // The most `counted_bytes` has been.
    peak_bytes: AtomicU64,
    // Bytes of `counted_bytes` each owner moved ahead of the writes it is
    // about to queue (`Owner::prepay`), by the owner's slot: its writes to
    // the volume take from here first and are counted only past it. An
    // owner prepays and writes from one thread; only that thread reads or
    // changes its slot.
    pub(super) prepaid: [AtomicU64; OWNERS],
    // The volume's buffer limits. Nothing is reserved or counted past the
    // hard one (`Reservation::reserve`, `fits`); the soft limit is a
    // reported figure.
    // One pair for the volume, shared by every stream writing to it.
    // `u64::MAX` until the volume is sized: no limit.
    buffer_soft_bytes: AtomicU64,
    buffer_hard_bytes: AtomicU64,
    // One more than the count of unwritten bytes someone asked to be woken
    // at; zero when no one asked. Whoever wakes them clears it.
    pub(crate) wake_line: AtomicU64,
    // Who asked for the wake: one bit for each owner of the volume's
    // writers that asked (`VolumeWriters::owner`). Taken with the line.
    wake_askers: AtomicU64,
    // Times its writer ran out of work. A sample across which this did not
    // move is one in which the writer never did.
    pub(crate) idle_turns: AtomicU64,
    // What the volume takes, in bytes per second; zero until measured. A
    // synced probe sets it when the volume is first seen, and samples taken
    // while its writer never ran out of work replace it.
    pub(crate) write_rate: AtomicU64,
    // Bytes per second of wall time its writer wrote over the last window,
    // whether or not the volume was the limit; zero when nothing was written.
    pub(crate) throughput: AtomicU64,
    // What its writer did and what waiting on it cost, for the metrics.
    pub(crate) writer: WriterCounts,
    // Its read turns, made with its kind when first asked for
    // (`Volume::reads`).
    reads: OnceLock<VolumeReads>,
}

// A volume writer's own counts. Each is one relaxed add per event.
#[derive(Default)]
pub(crate) struct WriterCounts {
    // Batches the writer ran, and the operations in them it ran.
    pub(crate) batches: AtomicU64,
    pub(crate) operations: AtomicU64,
    // Times the queuing thread stood still for the writer, and for how long:
    // in all, and by what it waited for (`LoopWait`).
    pub(crate) loop_waits: AtomicU64,
    pub(crate) loop_wait_nanos: AtomicU64,
    pub(crate) loop_waits_by_kind: [AtomicU64; LoopWait::COUNT],
    pub(crate) loop_wait_nanos_by_kind: [AtomicU64; LoopWait::COUNT],
    // Reads the queuing thread made itself of a file the writer writes,
    // after its wait, and how long they took, the open included.
    pub(crate) loop_reads: AtomicU64,
    pub(crate) loop_read_nanos: AtomicU64,
    // Times a thread other than the queuing thread (a check, a repair, a replay)
    // stood for the writer in `Barrier::wait_aside`, and for how long.
    pub(crate) aside_waits: AtomicU64,
    pub(crate) aside_wait_nanos: AtomicU64,
    // Files the writer opened, and the time inside those opens: the making
    // of the file, its sparse mark and its length when it is sized.
    pub(crate) opens: AtomicU64,
    pub(crate) open_nanos: AtomicU64,
    // The steps of those opens, each counted when the opener ran and timed
    // it: the create (with the folders made for it), the sparse mark, and
    // the length set at creation (with the look at the file before it).
    pub(crate) open_creates: AtomicU64,
    pub(crate) open_create_nanos: AtomicU64,
    pub(crate) open_marks: AtomicU64,
    pub(crate) open_mark_nanos: AtomicU64,
    pub(crate) open_sizes: AtomicU64,
    pub(crate) open_size_nanos: AtomicU64,
    // Lengths the writer set, at a close or a truncate, and the time inside
    // those calls; and closes it found already at their length, which set
    // nothing.
    pub(crate) length_sets: AtomicU64,
    pub(crate) length_set_nanos: AtomicU64,
    pub(crate) length_sets_skipped: AtomicU64,
    // Handles the writer dropped, and the time inside those drops.
    pub(crate) closes: AtomicU64,
    pub(crate) close_nanos: AtomicU64,
    // Operations that came back with an error.
    pub(crate) failed_operations: AtomicU64,
    // Operations not run: their caller gave its files up, or an earlier
    // operation of the same caller failed.
    pub(crate) abandoned_operations: AtomicU64,
    // Writer threads started for the volume, one after another.
    pub(crate) threads: AtomicU64,
    // Times the writer woke to mail after waiting for it: a send that found
    // it parked.
    pub(crate) woken: AtomicU64,
}

// What the queuing thread stood still for.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum LoopWait {
    // A read of a file's bytes, after the file's writes before it.
    Read,
    // A file's operations to return: its length, or a delivery of it.
    Settle,
    // The volume's buffered bytes to fall below its limit, while the queuing thread
    // itself produces what is written (a repair's or a replay's output).
    Budget,
}

impl LoopWait {
    pub(crate) const COUNT: usize = 3;
    // In the order of `instrumentation::LOOP_WAIT_KINDS`, which labels them.
    pub(crate) const ALL: [LoopWait; Self::COUNT] = [Self::Read, Self::Settle, Self::Budget];
}

impl WriterCounts {
    // The queuing thread stood still for `nanos` waiting for `kind`. Relaxed adds.
    pub(crate) fn loop_waited(&self, kind: LoopWait, nanos: u64) {
        self.loop_waits.fetch_add(1, Ordering::Relaxed);
        self.loop_wait_nanos.fetch_add(nanos, Ordering::Relaxed);
        self.loop_waits_by_kind[kind as usize].fetch_add(1, Ordering::Relaxed);
        self.loop_wait_nanos_by_kind[kind as usize].fetch_add(nanos, Ordering::Relaxed);
    }

    // The queuing thread read a writer's file itself for `nanos`. Relaxed adds.
    pub(crate) fn loop_read(&self, nanos: u64) {
        self.loop_reads.fetch_add(1, Ordering::Relaxed);
        self.loop_read_nanos.fetch_add(nanos, Ordering::Relaxed);
    }

    // A thread aside stood `nanos` for the writer. Relaxed adds.
    pub(crate) fn aside_waited(&self, nanos: u64) {
        self.aside_waits.fetch_add(1, Ordering::Relaxed);
        self.aside_wait_nanos.fetch_add(nanos, Ordering::Relaxed);
    }

    // The writer opened a file in `nanos`. Relaxed adds.
    pub(crate) fn opened(&self, nanos: u64) {
        self.opens.fetch_add(1, Ordering::Relaxed);
        self.open_nanos.fetch_add(nanos, Ordering::Relaxed);
    }

    // The steps of an open, in nanoseconds, each when it ran. Relaxed adds.
    pub(crate) fn open_steps(&self, create: Option<u64>, mark: Option<u64>, size: Option<u64>) {
        for (step, count, total) in [
            (create, &self.open_creates, &self.open_create_nanos),
            (mark, &self.open_marks, &self.open_mark_nanos),
            (size, &self.open_sizes, &self.open_size_nanos),
        ] {
            if let Some(nanos) = step {
                count.fetch_add(1, Ordering::Relaxed);
                total.fetch_add(nanos, Ordering::Relaxed);
            }
        }
    }

    // The writer set a file's length in `nanos`. Relaxed adds.
    pub(crate) fn length_set(&self, nanos: u64) {
        self.length_sets.fetch_add(1, Ordering::Relaxed);
        self.length_set_nanos.fetch_add(nanos, Ordering::Relaxed);
    }

    // The writer dropped `handles` in `nanos`. Relaxed adds.
    pub(crate) fn closed(&self, handles: u64, nanos: u64) {
        self.closes.fetch_add(handles, Ordering::Relaxed);
        self.close_nanos.fetch_add(nanos, Ordering::Relaxed);
    }
}

impl Volume {
    pub(crate) fn new(device: Option<u64>, label: String) -> Self {
        Self {
            device,
            label,
            written_bytes: AtomicU64::new(0),
            write_nanos: AtomicU64::new(0),
            write_calls: AtomicU64::new(0),
            write_seeks: AtomicU64::new(0),
            buffered_bytes: AtomicU64::new(0),
            arrived_bytes: AtomicU64::new(0),
            counted_bytes: AtomicU64::new(0),
            peak_bytes: AtomicU64::new(0),
            prepaid: std::array::from_fn(|_| AtomicU64::new(0)),
            buffer_soft_bytes: AtomicU64::new(u64::MAX),
            buffer_hard_bytes: AtomicU64::new(u64::MAX),
            wake_line: AtomicU64::new(0),
            wake_askers: AtomicU64::new(0),
            idle_turns: AtomicU64::new(0),
            write_rate: AtomicU64::new(0),
            throughput: AtomicU64::new(0),
            writer: WriterCounts::default(),
            reads: OnceLock::new(),
        }
    }

    // The volume's read turns, shared by everything that reads files on it.
    // Made once, the first time they are asked for, for the volume's kind
    // as `kind` says then (`StorageCapacity::volume_reads`); the turns stay
    // what they were made, as callers hold them.
    pub(crate) fn reads(&self, kind: impl FnOnce() -> StorageKind) -> &VolumeReads {
        self.reads.get_or_init(|| VolumeReads::of(kind()))
    }

    // The bytes counted against the volume's hard limit: reserved ahead of
    // their writes, and waiting for its writer.
    pub(crate) fn unwritten_bytes(&self) -> u64 {
        self.counted_bytes.load(Ordering::SeqCst)
    }

    // The most bytes ever counted against the volume at once.
    pub(crate) fn peak_bytes(&self) -> u64 {
        self.peak_bytes.load(Ordering::Relaxed)
    }

    // The most one write counts at once: a longer write is queued in pieces
    // of this size, each counted once it fits (`Submitter::write`). A caller
    // that reserves ahead and also writes keeps this much of the hard limit
    // out of its reservations (`Reservation::reserve`), so its writes
    // always find room once the writer has written what it holds.
    pub(crate) fn write_piece(&self) -> u64 {
        let (_, hard) = self.buffer_limits();
        (hard / 16).clamp(1, WRITE_PIECE)
    }

    // Count `bytes` if they fit below the hard limit less `keep`. A volume
    // that counts nothing grants one request whatever its size, so a
    // request larger than the limit is still granted, alone. A volume not
    // sized grants everything.
    fn grant(&self, bytes: u64, keep: u64) -> bool {
        let (_, hard) = self.buffer_limits();
        let limit = if hard == u64::MAX {
            u64::MAX
        } else {
            hard.saturating_sub(keep)
        };
        let granted =
            self.counted_bytes
                .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |counted| {
                    (counted == 0 || counted.saturating_add(bytes) <= limit)
                        .then(|| counted.saturating_add(bytes))
                });
        match granted {
            Ok(counted) => {
                self.peak_bytes
                    .fetch_max(counted.saturating_add(bytes), Ordering::Relaxed);
                true
            }
            Err(_) => false,
        }
    }

    // Whether `bytes` more may be queued: they fit below the hard limit, or
    // the volume counts nothing, or its writer holds nothing it could write
    // to make room.
    pub(crate) fn fits(&self, bytes: u64) -> bool {
        let (_, hard) = self.buffer_limits();
        let counted = self.unwritten_bytes();
        counted == 0
            || counted.saturating_add(bytes) <= hard
            || self.buffered_bytes.load(Ordering::SeqCst) == 0
    }

    // Count `bytes` whatever the room: what a caller already holds, or a
    // write once it `fits`.
    pub(super) fn charge(&self, bytes: u64) {
        let counted = self.counted_bytes.fetch_add(bytes, Ordering::SeqCst);
        self.peak_bytes
            .fetch_max(counted.saturating_add(bytes), Ordering::Relaxed);
    }

    // Bytes counted at `reserved` are counted at `actual` from here, in one
    // step, so the count never dips below what is held while the
    // difference is given back.
    fn settle(&self, reserved: u64, actual: u64) {
        if actual <= reserved {
            self.release(reserved - actual);
        } else {
            self.charge(actual - reserved);
        }
    }

    // `bytes` counted no longer count: given back, or written by the
    // writer. The count never goes below zero: a release of more than it
    // holds is a fault, and wrapping would stop the volume's grants for
    // good.
    pub(super) fn release(&self, bytes: u64) {
        let before = self
            .counted_bytes
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |counted| {
                Some(counted.saturating_sub(bytes))
            })
            .unwrap_or_else(|counted| counted);
        debug_assert!(before >= bytes, "a volume released more than it counted");
    }

    // Give the volume its soft and hard buffer limits. A limit lowered
    // below what the volume holds refuses reservations until it has
    // drained.
    pub(crate) fn set_buffer_limits(&self, soft_bytes: u64, hard_bytes: u64) {
        self.buffer_soft_bytes.store(soft_bytes, Ordering::Relaxed);
        self.buffer_hard_bytes.store(hard_bytes, Ordering::Relaxed);
    }

    // The volume's soft and hard buffer limits, in that order.
    pub(crate) fn buffer_limits(&self) -> (u64, u64) {
        (
            self.buffer_soft_bytes.load(Ordering::Relaxed),
            self.buffer_hard_bytes.load(Ordering::Relaxed),
        )
    }

    // Count exactly `bytes`, as a test sets a volume's fill.
    #[cfg(test)]
    pub(crate) fn count_for_test(&self, bytes: u64) {
        self.counted_bytes.store(bytes, Ordering::SeqCst);
        self.peak_bytes.fetch_max(bytes, Ordering::Relaxed);
    }

    // The unwritten bytes at which whoever was stopped at the hard limit is
    // woken: an eighth below it, so no one is woken per write.
    pub(crate) fn resume_line(&self) -> u64 {
        let (_, hard) = self.buffer_limits();
        hard.saturating_sub((hard / 8).max(1))
    }

    // Take the wake someone asked for, when the unwritten bytes have fallen
    // to their line: the bits of whoever asked, once for each ask, to
    // whoever lowered the bytes there; zero when there is no wake to take.
    // The caller lowers its count first and asks after. A line raised while
    // it takes is looked at again, so a wake the count has reached is never
    // missed. One load when no one asked.
    pub(crate) fn take_wake(&self) -> u64 {
        let taken = self
            .wake_line
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |ask| {
                (ask != 0 && self.unwritten_bytes() < ask).then_some(0)
            })
            .is_ok();
        if taken {
            self.wake_askers.swap(0, Ordering::SeqCst)
        } else {
            0
        }
    }

    // Ask to be woken, once, when the unwritten bytes are `bytes` or
    // fewer. False when they already are: nothing was asked and no wake
    // comes. One line is kept, the highest asked, which wakes soonest:
    // whoever a wake finds still short of room asks again for its own
    // line, so a lower ask never holds back a higher one. `asker` is the asking owner's bit; an owner whose ask
    // finds the count already there may still be woken by a line another
    // owner asked for, which only says the volume may have room.
    pub(crate) fn ask_wake(&self, bytes: u64, asker: u64) -> bool {
        // Marked before the line, so whoever takes the line finds the bit.
        self.wake_askers.fetch_or(asker, Ordering::SeqCst);
        let ask = bytes.saturating_add(1);
        let before = self.wake_line.fetch_max(ask, Ordering::SeqCst);
        if before >= ask || self.unwritten_bytes() > bytes {
            // A line this high or higher is out: whoever brings the count
            // below it takes the wake.
            return true;
        }
        // Already there: the line asked before goes back, unless a wake
        // was taken meanwhile, which is then on its way.
        self.wake_line
            .compare_exchange(ask, before, Ordering::SeqCst, Ordering::SeqCst)
            .is_err()
    }
}

// Bytes counted against a volume's hard limit ahead of what they are for,
// held by whoever counted them. It is asked for before the bytes exist and
// granted or refused (`reserve`), settled to what they came to (`settle`),
// and handed to the writer with the write they pay for, which gives them
// back once written. Whatever it still holds is given back when it is
// dropped. A byte count and its volume; nothing else.
pub(crate) struct Reservation {
    volume: Arc<Volume>,
    bytes: u64,
}

impl Reservation {
    // Nothing reserved on `volume` yet.
    pub(crate) fn none(volume: Arc<Volume>) -> Self {
        Self { volume, bytes: 0 }
    }

    pub(crate) fn volume(&self) -> &Arc<Volume> {
        &self.volume
    }

    pub(crate) fn bytes(&self) -> u64 {
        self.bytes
    }

    // Reserve `bytes` more if they fit below the volume's hard limit less
    // `keep` (`Volume::write_piece` for a caller that also writes). False
    // when refused: nothing was counted, and the caller may ask to be woken
    // when the volume has room (`Submitter::wake_below`). A volume that
    // counts nothing grants one request of any size.
    pub(crate) fn reserve(&mut self, bytes: u64, keep: u64) -> bool {
        let granted = self.volume.grant(bytes, keep);
        if granted {
            self.bytes += bytes;
        }
        granted
    }

    // Count `bytes` more whatever the room: bytes the caller already holds,
    // which no refusal could stop.
    pub(crate) fn count(&mut self, bytes: u64) {
        self.volume.charge(bytes);
        self.bytes += bytes;
    }

    // `of` of the reserved bytes came to `actual`: they count as that from
    // here, in one step.
    pub(crate) fn settle(&mut self, of: u64, actual: u64) {
        debug_assert!(of <= self.bytes, "settled more than was reserved");
        self.volume.settle(of, actual);
        self.bytes = self.bytes - of + actual;
    }

    // Give back `bytes` of the reservation.
    pub(crate) fn give_back(&mut self, bytes: u64) {
        debug_assert!(bytes <= self.bytes, "gave back more than was reserved");
        self.volume.release(bytes);
        self.bytes -= bytes;
    }

    // `bytes` of the reservation, as a reservation of their own on the same
    // volume. Nothing is counted again.
    pub(crate) fn split(&mut self, bytes: u64) -> Reservation {
        debug_assert!(bytes <= self.bytes, "split more than was reserved");
        self.bytes -= bytes;
        Reservation {
            volume: Arc::clone(&self.volume),
            bytes,
        }
    }

    // Hold what `other` holds as well. Nothing is counted again when both
    // are on one volume; otherwise its bytes are given back on its volume
    // and counted here.
    pub(crate) fn join(&mut self, mut other: Reservation) {
        let bytes = std::mem::take(&mut other.bytes);
        if !Arc::ptr_eq(&self.volume, &other.volume) {
            other.volume.release(bytes);
            self.volume.charge(bytes);
        }
        self.bytes += bytes;
    }

    // `bytes` of the reservation leave it, still counted: handed to the
    // writer, which gives them back once written.
    pub(super) fn hand_over(&mut self, bytes: u64) {
        debug_assert!(bytes <= self.bytes, "handed over more than was reserved");
        self.bytes -= bytes;
    }
}

impl Drop for Reservation {
    fn drop(&mut self) {
        if self.bytes != 0 {
            self.volume.release(self.bytes);
        }
    }
}

impl std::fmt::Debug for Reservation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Reservation")
            .field("volume", &self.volume.label)
            .field("bytes", &self.bytes)
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn sized(hard: u64) -> Volume {
        let volume = Volume::new(Some(1), "/v".into());
        volume.set_buffer_limits(hard, hard);
        volume
    }

    fn reservation(volume: &Arc<Volume>) -> Reservation {
        Reservation::none(Arc::clone(volume))
    }

    // Reservations that keep a write piece stop below the hard limit less
    // that piece; writes take the piece, and nothing goes past the limit.
    #[test]
    fn reservations_keep_a_write_piece_and_writes_take_it() {
        let volume = Arc::new(sized(1600));
        let keep = volume.write_piece();
        assert_eq!(keep, 100);
        let mut held = reservation(&volume);
        assert!(held.reserve(1000, keep));
        assert!(held.reserve(500, keep));
        assert!(
            !held.reserve(1, keep),
            "the hard limit less the piece is reached"
        );
        assert_eq!(held.bytes(), 1500);
        assert_eq!(volume.unwritten_bytes(), 1500);
        // The piece is left for writes.
        volume.buffered_bytes.store(1, Ordering::SeqCst);
        assert!(volume.fits(100));
        assert!(!volume.fits(101));
        let mut written = reservation(&volume);
        written.count(100);
        assert_eq!(volume.unwritten_bytes(), 1600);
        assert!(!volume.fits(1));
        held.give_back(600);
        assert!(held.reserve(500, keep));
        assert_eq!(volume.peak_bytes(), 1600);
        drop((held, written));
        assert_eq!(volume.unwritten_bytes(), 0, "dropped, they give back");
    }

    // A volume that counts nothing grants one request larger than its
    // whole limit, and only one.
    #[test]
    fn an_empty_volume_grants_one_request_larger_than_its_limit() {
        let volume = Arc::new(sized(1600));
        let keep = volume.write_piece();
        let mut held = reservation(&volume);
        assert!(held.reserve(5000, keep));
        assert!(!held.reserve(1, keep));
        assert_eq!(volume.peak_bytes(), 5000);
        held.give_back(5000);
        assert_eq!(volume.unwritten_bytes(), 0);
        assert!(held.reserve(1500, keep));
    }

    // A volume not sized grants everything.
    #[test]
    fn a_volume_not_sized_grants_everything() {
        let volume = Arc::new(Volume::new(None, "/v".into()));
        let keep = volume.write_piece();
        let mut held = reservation(&volume);
        assert!(held.reserve(u64::MAX / 2, keep));
        assert!(held.reserve(1 << 40, keep));
    }

    // A settle counts the difference in one step; a split and a join on one
    // volume count nothing again; a join across volumes moves the bytes.
    #[test]
    fn a_reservation_settles_splits_and_joins() {
        let volume = Arc::new(sized(1600));
        let other = Arc::new(sized(1600));
        let mut held = reservation(&volume);
        held.count(400);
        held.settle(100, 160);
        assert_eq!((held.bytes(), volume.unwritten_bytes()), (460, 460));
        held.settle(160, 10);
        assert_eq!((held.bytes(), volume.unwritten_bytes()), (310, 310));
        let part = held.split(110);
        assert_eq!(
            (held.bytes(), part.bytes(), volume.unwritten_bytes()),
            (200, 110, 310)
        );
        held.join(part);
        assert_eq!((held.bytes(), volume.unwritten_bytes()), (310, 310));
        let mut there = reservation(&other);
        there.join(held.split(300));
        assert_eq!(
            (volume.unwritten_bytes(), other.unwritten_bytes()),
            (10, 300)
        );
        held.hand_over(10);
        drop(held);
        assert_eq!(
            volume.unwritten_bytes(),
            10,
            "handed over, it stays counted"
        );
        volume.release(10);
    }

    // Two asks at different lines keep the higher, whichever came last: the
    // count reaching it wakes at once, not at the lower line.
    #[test]
    fn a_lower_ask_never_holds_back_a_higher_one() {
        let volume = sized(1600);
        volume.charge(1000);
        assert!(volume.ask_wake(800, 1));
        assert!(volume.ask_wake(400, 1), "a lower line asked after");
        volume.release(150);
        assert_eq!(volume.take_wake(), 0, "850 is above the line at 800");
        volume.release(100);
        assert_eq!(volume.take_wake(), 1, "750 has reached the higher line");
        assert_eq!(volume.take_wake(), 0, "one wake for the line");
        // Asked the other way about, the same.
        assert!(volume.ask_wake(400, 1));
        assert!(volume.ask_wake(700, 1));
        volume.release(100);
        assert_eq!(volume.take_wake(), 1, "650 has reached the higher line");
        // An ask the count is already under asks nothing and leaves the
        // line asked before it as it was.
        assert!(volume.ask_wake(300, 1));
        assert!(!volume.ask_wake(900, 1), "already under 900");
        volume.release(400);
        assert_eq!(volume.take_wake(), 1, "250 has reached the line at 300");
    }

    // A wake goes to every owner that asked for it, once, and to no other.
    #[test]
    fn a_wake_goes_to_whoever_asked() {
        let volume = sized(1600);
        volume.charge(1000);
        assert!(volume.ask_wake(800, 0b01));
        assert!(volume.ask_wake(600, 0b10));
        volume.release(100);
        assert_eq!(volume.take_wake(), 0, "900 is above both lines");
        volume.release(150);
        assert_eq!(volume.take_wake(), 0b11, "the line taken wakes both");
        assert_eq!(volume.take_wake(), 0, "one wake for the asks");
        assert!(volume.ask_wake(700, 0b10));
        volume.release(100);
        assert_eq!(volume.take_wake(), 0b10, "only the owner that asked");
    }

    // A release past the count is a fault: in a test it is caught, and
    // outside one the count stops at zero rather than wrapping.
    #[test]
    fn a_release_never_takes_the_count_below_zero() {
        let volume = sized(1600);
        volume.charge(10);
        let released = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            volume.release(11);
        }));
        assert_eq!(released.is_err(), cfg!(debug_assertions));
        assert_eq!(volume.unwritten_bytes(), 0);
        assert!(
            volume.grant(1500, volume.write_piece()),
            "the volume still grants reservations"
        );
    }

    // The old arithmetic of a volume's count, before reservations: what a
    // lease reserved below the lease limit (the hard limit less the loop's
    // share), what was charged, settled and released, and the one wake
    // line. Plain numbers, for comparison only.
    struct Old {
        hard: u64,
        counted: u64,
        peak: u64,
        line: u64,
    }

    impl Old {
        fn share(&self) -> u64 {
            (self.hard / 16).clamp(1, WRITE_PIECE)
        }

        fn lease_limit(&self) -> u64 {
            if self.hard == u64::MAX {
                return u64::MAX;
            }
            self.hard.saturating_sub(self.share())
        }

        fn reserve(&mut self, bytes: u64) -> bool {
            let limit = self.lease_limit();
            if self.counted == 0 || self.counted.saturating_add(bytes) <= limit {
                self.counted = self.counted.saturating_add(bytes);
                self.peak = self.peak.max(self.counted);
                return true;
            }
            false
        }

        fn charge(&mut self, bytes: u64) {
            self.counted += bytes;
            self.peak = self.peak.max(self.counted);
        }

        fn release(&mut self, bytes: u64) {
            self.counted = self.counted.saturating_sub(bytes);
        }

        fn settle(&mut self, reserved: u64, arrived: u64) {
            if arrived <= reserved {
                self.release(reserved - arrived);
            } else {
                self.charge(arrived - reserved);
            }
        }

        fn take_wake(&mut self) -> bool {
            if self.line != 0 && self.counted < self.line {
                self.line = 0;
                return true;
            }
            false
        }

        fn ask_wake(&mut self, bytes: u64) -> bool {
            let ask = bytes + 1;
            let before = self.line;
            self.line = self.line.max(ask);
            if before >= ask || self.counted > bytes {
                return true;
            }
            self.line = before;
            false
        }
    }

    // A seeded run of reservations, settles, writes, give-backs and wakes,
    // as callers make them, counts exactly what the old arithmetic counted:
    // the same bytes, peak, grants and wakes after every step.
    #[test]
    fn reservations_count_what_the_old_arithmetic_counted() {
        let mut seed = 0x9e37_79b9_7f4a_7c15u64;
        let mut next = move |below: u64| {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            seed % below
        };
        for hard in [1600, 64 << 20, u64::MAX] {
            let volume = Arc::new(sized(hard));
            let mut old = Old {
                hard,
                counted: 0,
                peak: 0,
                line: 0,
            };
            // Reservations held by four callers, and the bytes the old
            // arithmetic held for each.
            let mut held: Vec<Reservation> = (0..4).map(|_| reservation(&volume)).collect();
            let mut old_held = [0u64; 4];
            // Bytes handed to the writer and not written yet.
            let mut writing: u64 = 0;
            let size = |at: u64| 1 + at % 3000;
            for step in 0..20_000 {
                let who = next(4) as usize;
                match next(7) {
                    0 | 1 => {
                        let bytes = size(next(1 << 20));
                        let keep = volume.write_piece();
                        let granted = held[who].reserve(bytes, keep);
                        assert_eq!(granted, old.reserve(bytes), "grant at step {step}");
                        if granted {
                            old_held[who] += bytes;
                        }
                    }
                    2 => {
                        let of = next(old_held[who] + 1);
                        let actual = size(next(1 << 20)) - 1;
                        held[who].settle(of, actual);
                        old.settle(of, actual);
                        old_held[who] = old_held[who] - of + actual;
                    }
                    3 => {
                        // A write paid from the reservation: handed to the
                        // writer still counted.
                        let bytes = next(old_held[who] + 1);
                        held[who].hand_over(bytes);
                        old_held[who] -= bytes;
                        writing += bytes;
                    }
                    4 => {
                        // The writer wrote some of what it holds.
                        let bytes = next(writing + 1);
                        volume.release(bytes);
                        old.release(bytes);
                        writing -= bytes;
                    }
                    5 => {
                        let bytes = next(old_held[who] + 1);
                        held[who].give_back(bytes);
                        old.release(bytes);
                        old_held[who] -= bytes;
                    }
                    _ => {
                        let line = next(old.counted.saturating_add(1).min(1 << 24));
                        assert_eq!(
                            volume.ask_wake(line, 1),
                            old.ask_wake(line),
                            "ask at step {step}"
                        );
                    }
                }
                assert_eq!(
                    volume.take_wake() != 0,
                    old.take_wake(),
                    "wake at step {step}"
                );
                assert_eq!(held[who].bytes(), old_held[who], "held at step {step}");
                assert_eq!(
                    volume.unwritten_bytes(),
                    old.counted,
                    "count at step {step}"
                );
                assert_eq!(volume.peak_bytes(), old.peak, "peak at step {step}");
            }
            // What the callers still hold is given back as they drop.
            let still: u64 = old_held.iter().sum();
            drop(held);
            old.release(still);
            assert_eq!(volume.unwritten_bytes(), old.counted);
            assert_eq!(volume.unwritten_bytes(), writing);
        }
    }
}
