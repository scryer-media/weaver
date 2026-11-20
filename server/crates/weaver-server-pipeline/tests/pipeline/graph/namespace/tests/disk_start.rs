// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A started job's posts land on what its start promised for them. What
// lands past that promise, from a post bigger than it said, is booked as it
// lands, a slab at a time; with no room for it the job waits for room, its
// write landed and nothing refused.

use super::*;
use weaver_server_disk::operations::disk::{Capacity, CapacityReading};

const SLAB: u64 = 64 * MIB;

// A disk read once, its reading never renewed: what lands on it counts
// against that reading until the test ends.
fn read_once(free: u64) -> CapacityReader {
    let at = std::time::Instant::now();
    CapacityReader::from_fn(move || {
        Capacity::Known(CapacityReading {
            available_bytes: free,
            total_bytes: u64::MAX,
            sampled_at: at,
            stale: false,
        })
    })
}

// How many times anything took a ledger's booking lock: the disk crate
// counts each take, so this is the lock itself, not a mirror of the code
// that takes it.
fn bookings() -> u64 {
    weaver_server_disk::operations::disk::BOOKING_LOCKS.load(std::sync::atomic::Ordering::Relaxed)
}

// What the disk has free by its ledger: the reading, less what landed since
// and everything promised.
fn free(reader: &CapacityReader) -> u64 {
    reader
        .ledger()
        .free_with(|| reader.current())
        .expect("a reading")
        .bytes
}

// A job whose start promised `declared` bytes of one post, which is
// `actual` bytes long.
fn misstated(reader: &CapacityReader, declared: u64, actual: u64) -> (Job, MemberId) {
    let mut job = on_disk(reader, RESERVE, 5);
    let video = job.member("video.mkv", Some(actual));
    posting(&mut job, declared);
    // The ledger takes the reading as it stood before anything landed, as
    // a sampler's ledger does from its first probe.
    free(reader);
    (job, video)
}

// 9a. A post a fifth bigger than its start promised lands whole: the
// promise is spent, and the rest is booked once, as it lands.
#[test]
fn a_post_bigger_than_its_start_promised_books_the_rest_as_it_lands() {
    let reader = read_once(RESERVE + 1024 * MIB);
    let (mut job, video) = misstated(&reader, 10, 12);
    let before = bookings();
    job.send(video, 0, b"BIGGERTHANSO", Provenance::Fresh);

    assert!(job.writer.refusal().is_none());
    assert!(!job.writer.disk_held());
    assert_eq!(job.file("video.mkv"), b"BIGGERTHANSO");
    assert_eq!(job.writer.posted(), [0, 0], "the promise is spent");
    assert_eq!(bookings() - before, 1, "the rest is booked once");
    assert!(free(&reader) <= RESERVE + 1024 * MIB - 12);
}

// 9b. With no room past the promise, the bytes past it still land and the
// job waits for room: nothing is refused, no write fails.
#[test]
fn a_post_bigger_than_its_start_promised_waits_for_room_when_there_is_none() {
    let reader = read_once(RESERVE + 10);
    let (mut job, video) = misstated(&reader, 10, 12);
    job.send(video, 0, b"BIGGERTHANSO", Provenance::Fresh);

    assert!(job.writer.refusal().is_none(), "nothing is refused");
    assert_eq!(job.file("video.mkv"), b"BIGGERTHANSO", "the write landed");
    assert!(job.writer.disk_held(), "the job waits for room");
}

// 10 and 13. A post of 128 MiB whose start promised 96 MiB lands a MiB at a
// time. After every write the ledger leaves no more free than the reading
// less every byte written, and it has heard of all but a slab of them at
// most. The 32 MiB past the promise take the booking lock once.
#[test]
fn landings_settle_a_slab_at_a_time_and_book_the_overflow_a_slab_at_a_time() {
    const DECLARED: u64 = 96 * MIB;
    const ACTUAL: u64 = 128 * MIB;
    let reading = RESERVE + 1024 * MIB;
    let reader = read_once(reading);
    let (mut job, video) = misstated(&reader, DECLARED, ACTUAL);
    let before = bookings();
    let chunk = vec![b'v'; MIB as usize];
    let mut written = 0;
    while written < ACTUAL {
        job.send(video, written, &chunk, Provenance::Fresh);
        written += MIB;
        let free = free(&reader);
        assert!(
            free <= reading - written,
            "{free} free after {written} written"
        );
        let heard = reading - free - reader.ledger().promised();
        assert!(written - heard <= SLAB, "{written} written, {heard} heard");
    }
    assert!(job.writer.refusal().is_none());
    assert!(!job.writer.disk_held());
    assert_eq!(job.writer.posted(), [0, 0]);
    assert_eq!(
        bookings() - before,
        (ACTUAL - DECLARED).div_ceil(SLAB),
        "the booking lock once per slab of overflow"
    );
}

// 5 (review). Two jobs let go on one reading, each promised at its start
// only the few bytes it has left to fetch, run steps that each need 600 MiB
// of the 1 GiB above the reserve. The first books its room; the second
// waits for room at its booking, its write landed and nothing refused, and
// goes on once the first ends and gives its room back.
#[test]
fn two_steps_let_go_on_one_room_book_it_in_turn_and_the_second_waits() {
    let free = Arc::new(std::sync::atomic::AtomicU64::new(1536 * MIB));
    let reader = disk(&free, 100 * 1024 * MIB, &Arc::default());
    let mut first = on_disk(&reader, 512 * MIB, 600 * MIB);
    let mut second = on_disk(&reader, 512 * MIB, 600 * MIB);
    posting(&mut first, 10);
    posting(&mut second, 10);
    assert_eq!(reader.ledger().promised(), 20, "both started");

    hello(&mut first, "first.rar");
    assert!(first.writer.refusal().is_none());
    assert!(!first.writer.disk_held(), "the first step's room is booked");
    hello(&mut second, "second.rar");
    assert!(
        second.writer.refusal().is_none(),
        "the second is not refused"
    );
    assert!(second.writer.disk_held(), "the second waits at its booking");

    drop(first);
    assert!(second.writer.wake_disk(), "the first's room is free again");
    assert!(!second.writer.disk_held());
}
