// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

mod facts;

use weaver_graph::recovery::RepairOutcome;
use weaver_graph::{ByteRange, Delivery, MemberId, Object, ObjectId, Provenance, Recovery};

use super::Par3;
use crate::testing::{Job, par3_set, payload};

const OWN: ObjectId = ObjectId(11);
const BLOCK: u64 = 1024;

fn job(files: &[(String, Vec<u8>)]) -> Job<Par3> {
    let mut job = Job::new(Par3::new(OWN), OWN);
    for (name, bytes) in files {
        job.volume(name, bytes, 700, false);
    }
    job
}

fn bind(job: &mut Job<Par3>) {
    let mut renamed = Vec::new();
    job.object.bind(&mut job.namespace, &mut renamed);
}

// Recovery blocks the rulings so far found reachable.
fn reachable(object: &Par3) -> u64 {
    let mut sets = Vec::new();
    object.sets(&mut sets);
    sets.iter().map(|set| set.blocks_reachable).sum()
}

#[test]
fn par3_verifies_a_clean_member() {
    let one = payload(1, 9000);
    let two = payload(2, 4000);
    let files = par3_set("set", &[("one.bin", &one), ("two.bin", &two)], BLOCK, 3);
    assert!(files.len() >= 2);
    let mut job = job(&files);
    let first = job.member("one.bin", &one);
    let second = job.member("two.bin", &two);
    bind(&mut job);
    assert!(job.object.covers(first) && job.object.covers(second));
    assert_eq!(job.object.repair(&mut job.store), RepairOutcome::Clean);
    assert!(reachable(&job.object) >= 3);
    assert_eq!(job.store.writes, 0);
}

#[test]
fn par3_repairs_one_damaged_member() {
    let one = payload(3, 9000);
    let two = payload(4, 4000);
    let files = par3_set("set", &[("one.bin", &one), ("two.bin", &two)], BLOCK, 3);
    let mut job = job(&files);
    let mut wrong = one.clone();
    wrong[5000] ^= 0x42;
    let first = job.member("one.bin", &wrong);
    job.arrive(first, 4900, &wrong[4900..5100], Provenance::Damaged);
    let second = job.member("two.bin", &two);
    bind(&mut job);
    let outcome = job.object.repair(&mut job.store);
    assert!(
        matches!(outcome, RepairOutcome::Repaired { slices } if slices >= 1),
        "{outcome:?}"
    );
    assert!(job.store.members[&first] == one);
    assert!(job.store.members[&second] == two);
}

// A set short of recovery waits while its volume's bytes arrive, and is due
// again in the step the volume closes, its blocks in hand: no repair between.
#[test]
fn par3_short_of_blocks_waits_until_a_volume_is_in_hand() {
    let one = payload(31, 9000);
    let files = par3_set("set", &[("one.bin", &one)], BLOCK, 3);
    let mut job = job(&files[..1]);
    let (name, bytes) = &files[1];
    let volume = job.name(name, bytes);
    let mut wrong = one.clone();
    wrong[5000] ^= 0x42;
    let member = job.member("one.bin", &wrong);
    job.arrive(member, 4900, &wrong[4900..5100], Provenance::Damaged);
    bind(&mut job);
    assert_eq!(job.object.repair(&mut job.store), RepairOutcome::Waiting);
    let mut out = Vec::new();
    for (index, piece) in bytes.chunks(700).enumerate() {
        job.object.deliver(
            Delivery {
                volume,
                offset: index as u64 * 700,
                bytes: piece,
                provenance: Provenance::Fresh,
            },
            &mut |_, _| false,
            &mut out,
        );
        assert_eq!(job.object.outcome(), Some(RepairOutcome::Waiting));
    }
    job.object.volume_closed(volume, &mut out);
    assert_eq!(job.object.outcome(), None);
    let outcome = job.object.repair(&mut job.store);
    assert!(
        matches!(outcome, RepairOutcome::Repaired { slices } if slices >= 1),
        "{outcome:?}"
    );
    assert_eq!(job.object.outcome(), Some(outcome));
    assert!(job.store.members[&member] == one);
}

#[test]
fn par3_short_of_blocks_is_impossible() {
    let one = payload(5, 9000);
    let files = par3_set("set", &[("one.bin", &one)], BLOCK, 1);
    let mut job = job(&files);
    let mut wrong = one.clone();
    for at in [100, 3000, 7000] {
        wrong[at] ^= 0x42;
    }
    job.member("one.bin", &wrong);
    bind(&mut job);
    assert!(matches!(
        job.object.repair(&mut job.store),
        RepairOutcome::Impossible {
            damaged: 3,
            reachable: 1
        }
    ));
}

// Where the first recovery block's bytes lie in a volume: a recovery data
// packet ends in its block, and its length sits 24 bytes into its 48-byte
// header, whose type is the last eight.
fn first_block(volume: &[u8]) -> ByteRange {
    let found = volume
        .windows(8)
        .position(|window| window == b"PAR REC\x00")
        .expect("the volume carries a recovery block");
    let start = found - 40;
    let length = u64::from_le_bytes(volume[start + 24..start + 32].try_into().unwrap());
    let end = start as u64 + length;
    ByteRange::new(end - BLOCK, end)
}

#[test]
fn a_hole_in_a_volume_costs_only_the_blocks_it_cuts() {
    let one = payload(6, 9000);
    let files = par3_set("set", &[("one.bin", &one)], BLOCK, 3);
    assert!(files.len() >= 2);
    let (volume, before) = files.split_last().expect("an index and a volume");
    let mut job = job(before);
    let block = first_block(&volume.1);
    let hole = ByteRange::new(block.start + 100, block.start + 200);
    let at = job.name(&volume.0, &volume.1);
    let mut out = Vec::new();
    for range in [
        ByteRange::new(0, hole.start),
        ByteRange::new(hole.end, volume.1.len() as u64),
    ] {
        job.object.deliver(
            Delivery {
                volume: at,
                offset: range.start,
                bytes: &volume.1[range.start as usize..range.end as usize],
                provenance: Provenance::Fresh,
            },
            &mut |_, _| false,
            &mut out,
        );
    }
    job.object.volume_lost(at, hole, &mut out);
    job.object.volume_closed(at, &mut out);
    let mut wrong = one.clone();
    wrong[5000] ^= 0x42;
    let first = job.member("one.bin", &wrong);
    bind(&mut job);
    assert_eq!(
        job.object.repair(&mut job.store),
        RepairOutcome::Repaired { slices: 1 }
    );
    assert_eq!(reachable(&job.object), 2);
    assert!(job.store.members[&first] == one);
}

// Two lost blocks need two syndromes and two rebuilt blocks, a block of
// scratch and a 2x2 matrix: over 5 KiB at full width. Under a 1 KiB limit
// the decoder takes a stripe of each block at a time, and under a limit
// with no room for even one symbol it is refused rather than run.
#[test]
fn a_repair_larger_than_the_memory_limit_rebuilds_a_stripe_at_a_time() {
    let one = payload(16, 9000);
    let files = par3_set("set", &[("one.bin", &one)], BLOCK, 3);
    let mut wrong = one.clone();
    for at in [1500, 6500] {
        wrong[at] ^= 0x42;
    }
    let mut job = job(&files);
    job.object.memory_limit = 1024;
    let member = job.member("one.bin", &wrong);
    bind(&mut job);
    assert_eq!(
        job.object.repair(&mut job.store),
        RepairOutcome::Repaired { slices: 2 }
    );
    assert!(job.store.members[&member] == one);
    assert!(job.store.writes > 2, "{} writes", job.store.writes);

    let mut job = self::job(&files);
    job.object.memory_limit = 4;
    job.member("one.bin", &wrong);
    bind(&mut job);
    assert!(matches!(
        job.object.repair(&mut job.store),
        RepairOutcome::Impossible { damaged: 2, .. }
    ));
}

// Bind, repair, then bind again, as the namespace does once a repair has
// read its members back; what the second bind renamed.
fn identify(job: &mut Job<Par3>) -> (RepairOutcome, Vec<MemberId>) {
    bind(job);
    let outcome = job.object.repair(&mut job.store);
    let mut renamed = Vec::new();
    job.object.bind(&mut job.namespace, &mut renamed);
    (outcome, renamed)
}

fn name_of(job: &Job<Par3>, member: MemberId) -> &str {
    &job.namespace.get(member).expect("a member").name
}

#[test]
fn a_member_under_a_hex_name_is_bound_by_content_and_renamed() {
    let one = payload(7, 9000);
    let two = payload(8, 4000);
    let files = par3_set("set", &[("one.bin", &one), ("two.bin", &two)], BLOCK, 3);
    let mut job = job(&files);
    let hidden = job.member("3f9a0c71e25b84d6", &one);
    let named = job.member("two.bin", &two);
    bind(&mut job);
    assert!(!job.object.covers(hidden) && job.object.covers(named));
    let (outcome, renamed) = identify(&mut job);
    assert_eq!(outcome, RepairOutcome::Clean);
    assert_eq!(renamed, vec![hidden]);
    assert_eq!(name_of(&job, hidden), "one.bin");
    assert!(job.object.covers(hidden));
    assert_eq!(job.object.outcome(), Some(RepairOutcome::Clean));
    // Bound by its name now, to the same description.
    let mut again = Vec::new();
    job.object.bind(&mut job.namespace, &mut again);
    assert!(again.is_empty() && job.object.covers(hidden));
    assert_eq!(job.object.outcome(), Some(RepairOutcome::Clean));
}

#[test]
fn two_hidden_members_of_one_length_are_told_apart_by_their_bytes() {
    let one = payload(9, 6000);
    let two = payload(10, 6000);
    let files = par3_set("set", &[("one.bin", &one), ("two.bin", &two)], BLOCK, 3);
    let mut job = job(&files);
    let first = job.member("a07d5e19c4b26f83", &two);
    let second = job.member("e81b6d3f0a4c9725", &one);
    let (outcome, mut renamed) = identify(&mut job);
    assert_eq!(outcome, RepairOutcome::Clean);
    renamed.sort();
    assert_eq!(renamed, vec![first, second]);
    assert_eq!(name_of(&job, first), "two.bin");
    assert_eq!(name_of(&job, second), "one.bin");
}

#[test]
fn a_damaged_hidden_member_is_repaired_and_renamed() {
    let one = payload(11, 9000);
    let files = par3_set("set", &[("one.bin", &one)], BLOCK, 3);
    let mut job = job(&files);
    let mut wrong = one.clone();
    wrong[5000] ^= 0x42;
    let hidden = job.member("5b84d63f9a0c71e2", &wrong);
    job.arrive(hidden, 4900, &wrong[4900..5100], Provenance::Damaged);
    let (outcome, renamed) = identify(&mut job);
    assert_eq!(outcome, RepairOutcome::Repaired { slices: 1 });
    assert_eq!(renamed, vec![hidden]);
    assert_eq!(name_of(&job, hidden), "one.bin");
    assert!(job.store.members[&hidden] == one);
}

#[test]
fn a_member_whose_bytes_match_nothing_stays_unbound() {
    let one = payload(12, 9000);
    let two = payload(13, 4000);
    let files = par3_set("set", &[("one.bin", &one), ("two.bin", &two)], BLOCK, 3);
    let mut job = job(&files);
    let named = job.member("two.bin", &two);
    let stranger = job.member("c4b26f83a07d5e19", &payload(14, 9000));
    let (_, renamed) = identify(&mut job);
    assert!(renamed.is_empty());
    assert!(!job.object.covers(stranger) && job.object.covers(named));
    assert_eq!(name_of(&job, stranger), "c4b26f83a07d5e19");
}

#[test]
fn a_hidden_member_still_arriving_holds_the_set_without_asking_for_recovery() {
    let one = payload(15, 9000);
    let files = par3_set("set", &[("one.bin", &one)], BLOCK, 3);
    let mut job = job(&files);
    let hidden = job
        .namespace
        .announce("9725e81b6d3f0a4c", Some(one.len() as u64));
    job.arrive(hidden, 0, &one[..4000], Provenance::Fresh);
    bind(&mut job);
    assert_eq!(job.object.repair(&mut job.store), RepairOutcome::Waiting);
    let mut wants = Vec::new();
    Recovery::wants(&mut job.object, &mut wants);
    assert!(wants.is_empty(), "{wants:?}");
    job.arrive(hidden, 4000, &one[4000..], Provenance::Fresh);
    job.store.members.insert(hidden, one.clone());
    bind(&mut job);
    assert_eq!(
        job.object.outcome(),
        None,
        "a member that completed may be bound"
    );
    let (outcome, renamed) = identify(&mut job);
    assert_eq!(outcome, RepairOutcome::Clean);
    assert_eq!(renamed, vec![hidden]);
}
