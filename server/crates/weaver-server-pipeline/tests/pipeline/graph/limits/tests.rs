// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Instant;

use super::*;
use weaver_server_disk::operations::disk::{Capacity, CapacityReading};

const MIB: u64 = 1024 * 1024;

fn free(available_bytes: u64) -> CapacityReader {
    CapacityReader::from_fn(move || {
        Capacity::Known(CapacityReading {
            available_bytes,
            total_bytes: u64::MAX,
            sampled_at: Instant::now(),
            stale: false,
        })
    })
}

// Two repairs due in one walk are each promised their own bytes: the
// second does not go out on what the first was promised.
#[test]
fn each_repair_that_goes_out_is_promised_its_own_bytes() {
    let reader = free(10 * 1024 * MIB);
    let mut spend = Spend::default();
    assert!(spend.want(100 * MIB, &reader, MIB));
    assert!(spend.want(50 * MIB, &reader, MIB));
    assert_eq!(reader.ledger().promised(), 150 * MIB);
}

// A repair held while the job waited is promised in hand when it wakes,
// and takes that promise when it goes out; another is promised afresh.
#[test]
fn a_held_repair_takes_what_was_put_in_hand_for_it_and_no_more() {
    let short = Arc::new(std::sync::atomic::AtomicU64::new(10 * MIB));
    let reader = {
        let short = Arc::clone(&short);
        CapacityReader::from_fn(move || {
            Capacity::Known(CapacityReading {
                available_bytes: short.load(Ordering::SeqCst),
                total_bytes: u64::MAX,
                sampled_at: Instant::now(),
                stale: false,
            })
        })
    };
    let mut spend = Spend::default();
    assert!(
        !spend.want(100 * MIB, &reader, MIB),
        "no room: the job waits"
    );
    assert_eq!(reader.ledger().promised(), 0);
    short.store(10 * 1024 * MIB, Ordering::SeqCst);
    spend.rebook();
    assert_eq!(reader.ledger().promised(), 100 * MIB, "promised in hand");
    assert!(spend.want(100 * MIB, &reader, MIB));
    assert_eq!(reader.ledger().promised(), 100 * MIB, "taken from hand");
    assert!(spend.want(30 * MIB, &reader, MIB));
    assert_eq!(reader.ledger().promised(), 130 * MIB, "the next is its own");
}

// A member that moves to another device brings what it still owes: it
// is promised on the new device, where it lands from now on.
#[test]
fn a_member_that_moves_device_is_owed_on_the_new_one() {
    let (output, working) = (free(10 * 1024 * MIB), free(10 * 1024 * MIB));
    let mut caps = Caps::unbounded();
    caps.book(Device::Output, 10 * MIB, &output);
    let owed = caps.carry(Device::Output, 10 * MIB, (&working, Device::Working));
    assert_eq!(owed, 10 * MIB);
    assert_eq!(caps.spend[Device::Working as usize].owed, 10 * MIB);
    assert_eq!(caps.spend[Device::Output as usize].owed, 0);
    assert!(working.ledger().promised() >= 10 * MIB);
}

// A member taken back while the job waits, by a run stopped for room,
// is written again once the run goes on: the job wakes only when the
// disk has room for it as well as for what it owes, and what it is
// promised then covers both, so the run that writes it again does not
// fill the disk and stop once more.
#[test]
fn a_member_retracted_while_the_job_waits_is_room_the_job_needs_to_wake() {
    let available = Arc::new(std::sync::atomic::AtomicU64::new(10 * MIB));
    let working = {
        let available = Arc::clone(&available);
        CapacityReader::from_fn(move || {
            Capacity::Known(CapacityReading {
                available_bytes: available.load(Ordering::SeqCst),
                total_bytes: u64::MAX,
                sampled_at: Instant::now(),
                stale: false,
            })
        })
    };
    let output = free(10 * 1024 * MIB);
    let mut caps = Caps::unbounded();
    Arc::make_mut(&mut caps.limits).min_free_bytes = MIB;
    let member = (NodeId(1), MemberId(0));
    caps.extents.insert(
        member,
        Extent {
            end: 40 * MIB,
            level: 0,
            owed: 0,
            device: Device::Working,
        },
    );
    caps.written[0] = 40 * MIB;
    caps.book(Device::Working, 100 * MIB, &working);
    assert!(caps.disk_held(), "no room for what it owes: the job waits");

    caps.rewriting(true);
    caps.forget(&Tree::new(), member.0, member.1);
    caps.rewriting(false);
    available.store(MIB + 120 * MIB, Ordering::SeqCst);
    assert!(
        !caps.wake_disk(&output, &working),
        "room for what it owes, not for the member it writes again"
    );
    available.store(MIB + 140 * MIB, Ordering::SeqCst);
    assert!(caps.wake_disk(&output, &working));
    assert_eq!(working.ledger().promised(), 140 * MIB);
    caps.book(Device::Working, 40 * MIB, &working);
    assert!(!caps.disk_held());
    assert_eq!(
        working.ledger().promised(),
        140 * MIB,
        "the member written again spends what was promised for it"
    );
}

fn reading(available: &Arc<AtomicU64>, stale: bool) -> CapacityReader {
    let available = Arc::clone(available);
    CapacityReader::from_fn(move || {
        Capacity::Known(CapacityReading {
            available_bytes: available.load(Ordering::SeqCst),
            total_bytes: u64::MAX,
            sampled_at: Instant::now(),
            stale,
        })
    })
}

fn member(caps: &mut Caps, member: (NodeId, MemberId), end: u64, device: Device) {
    caps.extents.insert(
        member,
        Extent {
            end,
            level: 0,
            owed: 0,
            device,
        },
    );
    caps.written[0] += end;
}

// Every member that goes while a run stopped for room takes back what
// it built is written again, the run's own and those buried under them
// alike; one that goes for any other reason while the job waits is not,
// and asks for no room.
#[test]
fn only_what_a_stopped_run_takes_back_is_room_the_job_needs_again() {
    let available = Arc::new(AtomicU64::new(10 * MIB));
    let working = reading(&available, false);
    let output = free(10 * 1024 * MIB);
    let mut caps = Caps::unbounded();
    Arc::make_mut(&mut caps.limits).min_free_bytes = MIB;
    let (own, buried, refused) = (
        (NodeId(1), MemberId(0)),
        (NodeId(2), MemberId(0)),
        (NodeId(3), MemberId(0)),
    );
    member(&mut caps, own, 30 * MIB, Device::Working);
    member(&mut caps, buried, 20 * MIB, Device::Working);
    member(&mut caps, refused, 70 * MIB, Device::Working);
    caps.book(Device::Working, 100 * MIB, &working);
    assert!(caps.disk_held());

    caps.forget(&Tree::new(), refused.0, refused.1);
    caps.rewriting(true);
    caps.forget(&Tree::new(), own.0, own.1);
    caps.forget(&Tree::new(), buried.0, buried.1);
    caps.rewriting(false);
    available.store(MIB + 149 * MIB, Ordering::SeqCst);
    assert!(
        !caps.wake_disk(&output, &working),
        "a byte short of what it owes and both members it writes again"
    );
    available.store(MIB + 150 * MIB, Ordering::SeqCst);
    assert!(caps.wake_disk(&output, &working));
    assert_eq!(working.ledger().promised(), 150 * MIB);
}

// A job that waits, owes nothing and has nothing to write again wakes
// idle whatever the reading: holding it would hold back nothing that
// takes room. A reading under the reserve still wakes no one.
#[test]
fn a_job_that_owes_nothing_wakes_idle_whatever_the_reading() {
    let available = Arc::new(AtomicU64::new(0));
    let working = reading(&available, false);
    let output = free(10 * 1024 * MIB);
    let mut caps = Caps::unbounded();
    Arc::make_mut(&mut caps.limits).min_free_bytes = 5 * 1024 * MIB;
    let file = (NodeId(1), MemberId(0));
    member(&mut caps, file, 0, Device::Working);
    caps.extents.get_mut(&file).unwrap().owed = 40 * MIB;
    caps.book(Device::Working, 40 * MIB, &working);
    assert!(caps.disk_held(), "the disk is under its reserve");
    assert!(
        !caps.wake_disk(&output, &working),
        "it still owes what it took"
    );
    assert!(!caps.wake_idle(), "nor does it wake idle");
    caps.closed(file.0, file.1);
    assert!(caps.owes_nothing());
    assert!(
        !caps.wake_disk(&output, &working),
        "the disk is still under its reserve"
    );
    assert!(caps.wake_idle(), "it owes nothing now");
    assert!(!caps.disk_held());
    assert_eq!(working.ledger().promised(), 0);
}

// A stale reading is the last good one held across a failed probe: it
// begins no wait, as no reading begins none.
#[test]
fn a_stale_reading_begins_no_wait() {
    let available = Arc::new(AtomicU64::new(0));
    let stale = reading(&available, true);
    let mut caps = Caps::unbounded();
    Arc::make_mut(&mut caps.limits).min_free_bytes = MIB;
    caps.book(Device::Output, 100 * MIB, &stale);
    assert!(!caps.disk_held());
    assert!(caps.want_both((10 * MIB, &stale), (10 * MIB, &stale)));
    assert!(!caps.disk_held());
}

// A repair that writes on both devices goes out on both or on neither:
// when the second has no room, nothing stays promised on the first, and
// the job waits for its part on each.
#[test]
fn a_repair_held_on_one_device_holds_no_room_on_the_other() {
    let roomy = free(10 * 1024 * MIB);
    let available = Arc::new(AtomicU64::new(10 * MIB));
    let short = reading(&available, false);
    let mut caps = Caps::unbounded();
    Arc::make_mut(&mut caps.limits).min_free_bytes = MIB;
    assert!(!caps.want_both((100 * MIB, &roomy), (100 * MIB, &short)));
    assert_eq!(roomy.ledger().promised(), 0, "nothing kept on the first");
    assert_eq!(short.ledger().promised(), 0);
    assert!(caps.disk_held());
    available.store(MIB + 100 * MIB, Ordering::SeqCst);
    assert!(caps.wake_disk(&roomy, &short));
    assert_eq!(roomy.ledger().promised(), 100 * MIB);
    assert_eq!(short.ledger().promised(), 100 * MIB);
    assert!(caps.want_both((100 * MIB, &roomy), (100 * MIB, &short)));
    assert_eq!(
        roomy.ledger().promised(),
        100 * MIB,
        "the repair takes what was put in hand on the first"
    );
    assert_eq!(short.ledger().promised(), 100 * MIB, "and on the second");
}

const GIB: u64 = 1024 * MIB;

// A disk of `total` bytes showing what `available` holds free.
fn disk(available: &Arc<AtomicU64>, total: u64) -> CapacityReader {
    let available = Arc::clone(available);
    CapacityReader::from_fn(move || {
        Capacity::Known(CapacityReading {
            available_bytes: available.load(Ordering::SeqCst),
            total_bytes: total,
            sampled_at: Instant::now(),
            stale: false,
        })
    })
}

fn reserved(reserve: u64) -> Caps {
    let mut caps = Caps::unbounded();
    Arc::make_mut(&mut caps.limits).min_free_bytes = reserve;
    caps
}

// A member's first write takes its whole length, as `admit_write` does
// for a file: owed to the device until it lands.
fn takes(caps: &mut Caps, key: (NodeId, MemberId), end: u64, at: (&CapacityReader, Device)) {
    let (reader, device) = at;
    member(caps, key, end, device);
    caps.extents.get_mut(&key).unwrap().owed = end;
    caps.book(device, end, reader);
}

// On a disk of 100 GiB that keeps 5 GiB free, a job that owes 120 GiB,
// more than the whole disk, is never refused for it: it waits, as any
// job short of room does, and the wait reads what it owes, what is free
// and the reserve.
#[test]
fn a_job_larger_than_the_whole_disk_waits_and_is_never_refused() {
    let available = Arc::new(AtomicU64::new(20 * GIB));
    let output = disk(&available, 100 * GIB);
    let working = free(10 * 1024 * GIB);
    let mut caps = reserved(5 * GIB);
    takes(
        &mut caps,
        (NodeId(1), MemberId(0)),
        120 * GIB,
        (&output, Device::Output),
    );
    assert!(caps.disk_held(), "no room for it: the job waits");
    assert_eq!(
        caps.disk_wait((&output, &working)),
        Some(DiskWait {
            device: Device::Output,
            owes: 120 * GIB,
            free: Some(20 * GIB),
            reserve: 5 * GIB,
        })
    );
    assert!(caps.check().is_ok(), "nothing refuses it");
    assert!(caps.refusal().is_none());
    assert!(!caps.wake_disk(&output, &working));
    assert!(caps.disk_held(), "it goes on waiting");
}

// A job that owes 60 GiB with 20 GiB free waits for what it owes, and
// wakes once a reading shows that much above the reserve: what the wait
// reads is what its wake asks for.
#[test]
fn a_wait_reads_what_its_wake_asks_for() {
    let available = Arc::new(AtomicU64::new(20 * GIB));
    let output = disk(&available, 100 * GIB);
    let working = free(10 * 1024 * GIB);
    let mut caps = reserved(5 * GIB);
    takes(
        &mut caps,
        (NodeId(1), MemberId(0)),
        60 * GIB,
        (&output, Device::Output),
    );
    let wait = caps.disk_wait((&output, &working)).expect("the job waits");
    assert_eq!((wait.owes, wait.free), (60 * GIB, Some(20 * GIB)));

    available.store(wait.reserve + wait.owes - 1, Ordering::SeqCst);
    assert!(!caps.wake_disk(&output, &working), "a byte short");
    available.store(wait.reserve + wait.owes, Ordering::SeqCst);
    assert!(caps.wake_disk(&output, &working));
    assert_eq!(
        caps.disk_wait((&output, &working)),
        None,
        "it no longer waits"
    );
}

// Two folders on one disk wait together: the wait reads what both owe,
// as their wake asks room for both at once.
#[test]
fn two_folders_on_one_disk_wait_for_both() {
    let available = Arc::new(AtomicU64::new(20 * GIB));
    let one = disk(&available, 100 * GIB);
    let mut caps = reserved(5 * GIB);
    takes(
        &mut caps,
        (NodeId(1), MemberId(0)),
        30 * GIB,
        (&one, Device::Output),
    );
    takes(
        &mut caps,
        (NodeId(1), MemberId(1)),
        30 * GIB,
        (&one, Device::Working),
    );
    let wait = caps.disk_wait((&one, &one)).expect("the job waits");
    assert_eq!(wait.owes, 60 * GIB);

    available.store(5 * GIB + 60 * GIB - 1, Ordering::SeqCst);
    assert!(!caps.wake_disk(&one, &one));
    available.store(5 * GIB + 60 * GIB, Ordering::SeqCst);
    assert!(caps.wake_disk(&one, &one));
}
