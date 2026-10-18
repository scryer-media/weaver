// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use weaver_graph::recovery::RepairOutcome;
use weaver_graph::{ByteRange, Delivery, Object, ObjectId, Provenance, Recovery};

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
    assert!(job.object.blocks_reachable() >= 3);
    assert_eq!(job.object.repair(&mut job.store), RepairOutcome::Clean);
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
            &mut out,
        );
    }
    job.object.volume_lost(at, hole, &mut out);
    job.object.volume_closed(at, &mut out);
    let mut wrong = one.clone();
    wrong[5000] ^= 0x42;
    let first = job.member("one.bin", &wrong);
    bind(&mut job);
    assert_eq!(job.object.blocks_reachable(), 2);
    assert_eq!(
        job.object.repair(&mut job.store),
        RepairOutcome::Repaired { slices: 1 }
    );
    assert!(job.store.members[&first] == one);
}
