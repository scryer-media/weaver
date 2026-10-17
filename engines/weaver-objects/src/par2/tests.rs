// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use weaver_graph::recovery::RepairOutcome;
use weaver_graph::{ByteRange, MemberId, Object, ObjectId, Provenance, Recovery, SliceVerdict};

use super::Par2;
use crate::testing::{Job, par2_set, payload, segments};

const OWN: ObjectId = ObjectId(7);
const BLOCK: u64 = 512;
const ARTICLE: u64 = 700;

// A namespace holding the recovery files of `sets`, the first one building
// the object, every file delivered in article-sized pieces.
fn job(files: &[(String, Vec<u8>)]) -> Job<Par2> {
    let mut job = Job::new(Par2::new(OWN), OWN);
    for (name, bytes) in files {
        job.volume(name, bytes, ARTICLE as usize, false);
    }
    job
}

fn bind(job: &mut Job<Par2>) -> Vec<MemberId> {
    let mut renamed = Vec::new();
    job.object.bind(&mut job.namespace, &mut renamed);
    renamed
}

// What the namespace feeds: every piece verified unless it touches `bad`.
fn feed(job: &mut Job<Par2>, member: MemberId, bytes: &[u8], bad: Option<ByteRange>) {
    let mut pieces = segments(0, bytes, ARTICLE, BLOCK, true);
    for piece in &mut pieces {
        piece.verified = bad.is_none_or(|bad| piece.range.intersect(bad).is_none());
    }
    job.object.feed(member, &pieces);
}

fn verdicts(job: &Job<Par2>, member: MemberId) -> Vec<SliceVerdict> {
    let mut out = Vec::new();
    job.object.verdicts(member, &mut out);
    out
}

// A member whose bytes at `at` arrived wrong and failed their checksum.
fn damaged_member(
    job: &mut Job<Par2>,
    name: &str,
    bytes: &[u8],
    at: usize,
) -> (MemberId, ByteRange) {
    let mut wrong = bytes.to_vec();
    wrong[at] ^= 0x5A;
    let member = job.member(name, &wrong);
    let article = ByteRange::at(at as u64 / ARTICLE * ARTICLE, ARTICLE)
        .intersect(ByteRange::new(0, bytes.len() as u64));
    let article = article.expect("inside");
    job.arrive(
        member,
        article.start,
        &wrong[article.as_range().start as usize..article.end as usize],
        Provenance::Damaged,
    );
    (member, article)
}

#[test]
fn binds_by_id_without_renaming() {
    let data = payload(1, 5000);
    let mut job = job(&par2_set("set", &[("payload.bin", &data)], BLOCK, 2, 1));
    let member = job.member("payload.bin", &data);
    assert!(bind(&mut job).is_empty());
    assert!(job.object.covers(member));
}

#[test]
fn binds_by_name_when_the_id_does_not_match() {
    let data = payload(2, 5000);
    let mut job = job(&par2_set("set", &[("payload.bin", &data)], BLOCK, 2, 1));
    // Case changes the id; the name still finds it, and it keeps its name.
    let member = job.member("PAYLOAD.BIN", &data);
    assert!(bind(&mut job).is_empty());
    assert!(job.object.covers(member));
    assert_eq!(job.namespace.get(member).unwrap().name, "PAYLOAD.BIN");
}

#[test]
fn binds_by_prefix_and_renames_through_the_namespace() {
    let data = payload(3, 40_000);
    let mut job = job(&par2_set("set", &[("payload.bin", &data)], BLOCK, 2, 1));
    let member = job.member("a8f3c1e09b", &data);
    assert_eq!(bind(&mut job), vec![member]);
    assert!(job.object.covers(member));
    assert_eq!(job.namespace.get(member).unwrap().name, "payload.bin");
    // Bound now by name, it is not renamed again.
    assert!(bind(&mut job).is_empty());
}

#[test]
fn two_members_claiming_one_description_bind_neither() {
    let data = payload(4, 5000);
    let mut job = job(&par2_set("set", &[("payload.bin", &data)], BLOCK, 2, 1));
    let first = job.member("x1", &data);
    let second = job.member("x2", &data);
    assert!(bind(&mut job).is_empty());
    assert!(!job.object.covers(first) && !job.object.covers(second));
}

#[test]
fn a_name_matching_two_members_binds_neither() {
    let data = payload(5, 5000);
    let other = payload(6, 5000);
    let mut job = job(&par2_set("set", &[("payload.bin", &data)], BLOCK, 2, 1));
    // Neither carries the described content, and both fold to the name.
    let first = job.member("PAYLOAD.BIN", &other);
    let second = job.member("Payload.Bin", &other);
    bind(&mut job);
    assert!(!job.object.covers(first) && !job.object.covers(second));
}

#[test]
fn a_clean_set_repairs_with_zero_reads() {
    let data = payload(7, 5000);
    let mut job = job(&par2_set("set", &[("payload.bin", &data)], BLOCK, 2, 1));
    let member = job.member("payload.bin", &data);
    bind(&mut job);
    feed(&mut job, member, &data, None);
    assert!(
        verdicts(&job, member)
            .iter()
            .all(|verdict| *verdict == SliceVerdict::Intact)
    );
    assert_eq!(job.object.repair(&mut job.store), RepairOutcome::Clean);
    assert_eq!((job.store.reads, job.store.volume_reads), (0, 0));
}

#[test]
fn an_unfed_clean_member_is_read_back_and_found_clean() {
    let data = payload(8, 5000);
    let mut job = job(&par2_set("set", &[("payload.bin", &data)], BLOCK, 2, 1));
    job.member("payload.bin", &data);
    bind(&mut job);
    assert_eq!(job.object.repair(&mut job.store), RepairOutcome::Clean);
    assert!(job.store.reads > 0);
    assert_eq!(job.store.writes, 0);
}

#[test]
fn a_damaged_slice_vetoes_every_arm() {
    let data = payload(9, 5000);
    let mut job = job(&par2_set("set", &[("payload.bin", &data)], BLOCK, 2, 1));
    let mut wrong = data.clone();
    wrong[1300] ^= 0x01;
    let member = job.member("payload.bin", &wrong);
    bind(&mut job);
    // Every piece claims to be verified, but one slice folds wrong.
    feed(&mut job, member, &wrong, None);
    let seen = verdicts(&job, member);
    assert_eq!(seen[2], SliceVerdict::Damaged);
    assert_eq!(
        seen.iter()
            .filter(|verdict| **verdict == SliceVerdict::Damaged)
            .count(),
        1
    );
    assert!(!seen.contains(&SliceVerdict::Intact));
}

#[test]
fn a_damaged_member_is_repaired_through_member_access() {
    let data = payload(10, 5000);
    let mut job = job(&par2_set("set", &[("payload.bin", &data)], BLOCK, 2, 1));
    let (member, bad) = damaged_member(&mut job, "payload.bin", &data, 2100);
    bind(&mut job);
    let wrong = job.store.members[&member].clone();
    feed(&mut job, member, &wrong, Some(bad));
    let outcome = job.object.repair(&mut job.store);
    assert!(
        matches!(outcome, RepairOutcome::Repaired { slices } if slices >= 1),
        "{outcome:?}"
    );
    assert!(job.store.members[&member] == data);
    assert!(job.store.writes > 0 && job.store.volume_reads > 0);
}

#[test]
fn two_sets_in_one_namespace_bind_and_repair_apart() {
    let one = payload(11, 5000);
    let two = payload(12, 6000);
    let mut files = par2_set("first", &[("one.bin", &one)], BLOCK, 2, 1);
    files.extend(par2_set("second", &[("two.bin", &two)], 1024, 2, 1));
    let mut job = job(&files);
    let first = job.member("one.bin", &one);
    let (second, bad) = damaged_member(&mut job, "two.bin", &two, 4500);
    bind(&mut job);
    assert!(job.object.covers(first) && job.object.covers(second));
    feed(&mut job, first, &one, None);
    let wrong = job.store.members[&second].clone();
    feed(&mut job, second, &wrong, Some(bad));
    assert_eq!(verdicts(&job, first).len(), 10);
    assert_eq!(verdicts(&job, second).len(), 6);
    assert!(matches!(
        job.object.repair(&mut job.store),
        RepairOutcome::Repaired { .. }
    ));
    assert!(job.store.members[&second] == two);
    assert!(job.store.members[&first] == one);
}

#[test]
fn a_shared_recovery_file_feeds_both_sets() {
    let one = payload(13, 5000);
    let two = payload(14, 5000);
    let first = par2_set("first", &[("one.bin", &one)], BLOCK, 2, 1);
    let second = par2_set("second", &[("two.bin", &two)], BLOCK, 2, 1);
    // Each set's index alone, and one file carrying both sets' blocks.
    let mut shared = first[1].1.clone();
    shared.extend_from_slice(&second[1].1);
    let files = vec![
        first[0].clone(),
        second[0].clone(),
        ("shared.vol00+04.par2".to_owned(), shared),
    ];
    let mut job = job(&files);
    let (left, left_bad) = damaged_member(&mut job, "one.bin", &one, 600);
    let (right, right_bad) = damaged_member(&mut job, "two.bin", &two, 3000);
    bind(&mut job);
    assert!(job.object.covers(left) && job.object.covers(right));
    let wrong = job.store.members[&left].clone();
    feed(&mut job, left, &wrong, Some(left_bad));
    let wrong = job.store.members[&right].clone();
    feed(&mut job, right, &wrong, Some(right_bad));
    assert_eq!(job.object.blocks_reachable(), 4);
    assert!(matches!(
        job.object.repair(&mut job.store),
        RepairOutcome::Repaired { .. }
    ));
    assert!(job.store.members[&left] == one);
    assert!(job.store.members[&right] == two);
}

#[test]
fn blocks_reachable_drops_when_a_volume_is_lost() {
    let data = payload(15, 5000);
    let files = par2_set("set", &[("payload.bin", &data)], BLOCK, 4, 2);
    assert_eq!(files.len(), 3);
    let mut job = job(&files);
    job.member("payload.bin", &data);
    bind(&mut job);
    let before = job.object.blocks_reachable();
    assert_eq!(before, 4);
    let lost = files[2].1.len() as u64;
    let mut out = Vec::new();
    job.object.volume_lost(
        weaver_graph::VolumeIndex(2),
        ByteRange::new(0, lost),
        &mut out,
    );
    assert!(job.object.blocks_reachable() < before);
}

#[test]
fn a_repair_short_of_blocks_reports_counts() {
    let data = payload(16, 5000);
    let mut job = job(&par2_set("set", &[("payload.bin", &data)], BLOCK, 2, 1));
    let mut wrong = data.clone();
    for at in [100, 1100, 2100] {
        wrong[at] ^= 0x33;
    }
    let member = job.member("payload.bin", &wrong);
    job.arrive(member, 0, &wrong[..2800], Provenance::Damaged);
    bind(&mut job);
    feed(&mut job, member, &wrong, Some(ByteRange::new(0, 2800)));
    assert_eq!(
        job.object.repair(&mut job.store),
        RepairOutcome::Impossible {
            damaged: 3,
            reachable: 2
        }
    );
    assert_eq!(job.store.writes, 0);
}

#[test]
fn wants_asks_for_recovery_while_short() {
    let data = payload(17, 5000);
    let files = par2_set("set", &[("payload.bin", &data)], BLOCK, 4, 2);
    let mut job = Job::new(Par2::new(OWN), OWN);
    job.volume(&files[0].0, &files[0].1, ARTICLE as usize, false);
    // A volume named but not yet arrived.
    let volume = job
        .object
        .claim(&files[1].0, &files[1].1[..64])
        .expect("claimed");
    let (member, bad) = damaged_member(&mut job, "payload.bin", &data, 300);
    bind(&mut job);
    let wrong = job.store.members[&member].clone();
    feed(&mut job, member, &wrong, Some(bad));
    let mut seeks = Vec::new();
    Recovery::wants(&mut job.object, &mut seeks);
    assert!(!seeks.is_empty());
    assert!(
        seeks
            .iter()
            .all(|seek| seek.volume == volume && seek.purpose == weaver_graph::Purpose::Recovery)
    );
    assert_eq!(job.object.repair(&mut job.store), RepairOutcome::Waiting);
}
