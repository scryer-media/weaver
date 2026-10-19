// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

mod facts;

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
fn binds_by_its_described_name_without_renaming() {
    let data = payload(1, 5000);
    let mut job = job(&par2_set("set", &[("payload.bin", &data)], BLOCK, 2, 1));
    let member = job.member("payload.bin", &data);
    assert!(bind(&mut job).is_empty());
    assert!(job.object.covers(member));
}

#[test]
fn binds_by_a_folded_name_and_keeps_its_own() {
    let data = payload(2, 5000);
    let mut job = job(&par2_set("set", &[("payload.bin", &data)], BLOCK, 2, 1));
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

// Evidence folded piece by piece, as articles arrive after the member is
// bound and last first, says what one fold of the whole grid says, clean
// or with one slice wrong.
#[test]
fn evidence_fed_piece_by_piece_says_what_one_fold_of_the_whole_does() {
    let data = payload(13, 5000);
    let mut wrong = data.clone();
    wrong[2300] ^= 0x01;
    let files = par2_set("set", &[("payload.bin", &data)], BLOCK, 2, 1);
    for (bytes, damaged) in [(&data, false), (&wrong, true)] {
        let mut piecewise = job(&files);
        let member = piecewise.member("payload.bin", bytes);
        bind(&mut piecewise);
        for piece in segments(0, bytes, ARTICLE, BLOCK, true).iter().rev() {
            piecewise.object.feed(member, std::slice::from_ref(piece));
        }
        let mut whole = job(&files);
        let other = whole.member("payload.bin", bytes);
        feed(&mut whole, other, bytes, None);
        bind(&mut whole);
        let seen = verdicts(&piecewise, member);
        assert_eq!(seen, verdicts(&whole, other));
        assert_eq!(seen.contains(&SliceVerdict::Damaged), damaged);
        assert_eq!(
            seen.iter().all(|verdict| *verdict == SliceVerdict::Intact),
            !damaged
        );
    }
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
    let volume = job.name(&files[1].0, &files[1].1);
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

// The front of a volume walk steps over a lost article and resyncs on the
// next packet, so every recovery packet clear of the loss is still found.
#[test]
fn a_lost_article_inside_a_volume_keeps_the_packets_past_it() {
    let data = payload(17, 5000);
    let files = par2_set("set", &[("payload.bin", &data)], BLOCK, 4, 1);
    assert_eq!(files.len(), 2);
    let mut job = job(&files[..1]);
    job.member("payload.bin", &data);
    let (name, bytes) = &files[1];
    let volume = job.object.claim(name, &bytes[..64]).expect("a PAR2 volume");
    let lost = ByteRange::at(ARTICLE, ARTICLE);
    let mut out = Vec::new();
    for (index, piece) in bytes.chunks(ARTICLE as usize).enumerate() {
        let offset = index as u64 * ARTICLE;
        if offset == lost.start {
            continue;
        }
        job.object.deliver(
            weaver_graph::Delivery {
                volume,
                offset,
                bytes: piece,
                provenance: Provenance::Fresh,
            },
            &mut |_, _| false,
            &mut out,
        );
    }
    job.object.volume_lost(volume, lost, &mut out);
    job.object.volume_closed(volume, &mut out);
    bind(&mut job);
    // Recovery packets wholly outside the lost article.
    let mut clear = 0;
    let mut at = 0usize;
    while at + 64 <= bytes.len() {
        let length = u64::from_le_bytes(bytes[at + 8..at + 16].try_into().expect("8 bytes"));
        let packet = ByteRange::at(at as u64, length);
        if &bytes[at + 48..at + 64] == b"PAR 2.0\0RecvSlic" && packet.intersect(lost).is_none() {
            clear += 1;
        }
        at += length as usize;
    }
    assert!(clear >= 2, "the fixture keeps packets past the loss");
    assert_eq!(job.object.blocks_reachable(), clear);
}

// A set's volume named but not yet arrived: claimed from its front, so
// nothing of it is walked and it may be any set's.
fn named_volume(job: &mut Job<Par2>, file: &(String, Vec<u8>)) -> weaver_graph::VolumeIndex {
    job.name(&file.0, &file.1)
}

fn wanted(job: &mut Job<Par2>) -> Vec<weaver_graph::VolumeIndex> {
    let mut seeks = Vec::new();
    Recovery::wants(&mut job.object, &mut seeks);
    let mut volumes: Vec<_> = seeks.iter().map(|seek| seek.volume).collect();
    volumes.dedup();
    volumes
}

// The first set's blocks in hand do not cover the second set's damage.
#[test]
fn two_sets_each_ask_for_their_own_shortfall() {
    let one = payload(18, 5000);
    let two = payload(19, 5000);
    let first = par2_set("first", &[("one.bin", &one)], BLOCK, 2, 1);
    let second = par2_set("second", &[("two.bin", &two)], BLOCK, 2, 1);
    let mut job = job(&[first[0].clone(), first[1].clone(), second[0].clone()]);
    let later = named_volume(&mut job, &second[1]);
    let (left, left_bad) = damaged_member(&mut job, "one.bin", &one, 600);
    let (right, right_bad) = damaged_member(&mut job, "two.bin", &two, 3000);
    bind(&mut job);
    let wrong = job.store.members[&left].clone();
    feed(&mut job, left, &wrong, Some(left_bad));
    let wrong = job.store.members[&right].clone();
    feed(&mut job, right, &wrong, Some(right_bad));
    assert_eq!(wanted(&mut job), vec![later]);
}

// One damaged slice of a member both sets describe is one slice short, the
// first set's, and the first set's block covers it.
#[test]
fn a_shared_members_damage_counts_once() {
    let shared = payload(20, 5000);
    let first = par2_set("first", &[("shared.bin", &shared)], BLOCK, 1, 1);
    let second = par2_set("second", &[("shared.bin", &shared)], BLOCK, 2, 1);
    let mut job = job(&[first[0].clone(), first[1].clone(), second[0].clone()]);
    named_volume(&mut job, &second[1]);
    let (member, bad) = damaged_member(&mut job, "shared.bin", &shared, 1300);
    bind(&mut job);
    assert!(
        job.object
            .sets
            .values()
            .all(|state| state.bound(member).is_some()),
        "described alike, the member is bound in both sets"
    );
    let wrong = job.store.members[&member].clone();
    feed(&mut job, member, &wrong, Some(bad));
    assert!(wanted(&mut job).is_empty());
    assert_eq!(
        job.object.repair(&mut job.store),
        RepairOutcome::Repaired { slices: 1 }
    );
    assert!(job.store.members[&member] == shared);
}

// The member is the first set's file to the byte, but the second set
// describes other bytes under its name: it answers in both, and is the
// first set's alone.
#[test]
fn a_name_two_sets_describe_is_the_first_sets() {
    let quill = payload(21, 5000);
    let other = payload(22, 5000);
    let mut files = par2_set("first", &[("quill.bin", &quill)], BLOCK, 2, 1);
    files.extend(par2_set("second", &[("quill.bin", &other)], BLOCK, 2, 1));
    let mut job = job(&files);
    let member = job.member("quill.bin", &quill);
    bind(&mut job);
    let bound: Vec<bool> = job
        .object
        .ordered()
        .iter()
        .map(|key| job.object.sets[key].bound(member).is_some())
        .collect();
    assert_eq!(bound, [true, false]);
    feed(&mut job, member, &quill, None);
    assert_eq!(job.object.repair(&mut job.store), RepairOutcome::Clean);
    assert_eq!(job.store.writes, 0);
}

// An index whose files no member answers to yet: nothing to repair, and no
// blocks to ask for.
#[test]
fn a_set_covering_nothing_asks_for_nothing() {
    let data = payload(23, 5000);
    let files = par2_set("set", &[("payload.bin", &data)], BLOCK, 2, 1);
    let mut job = job(&files[..1]);
    named_volume(&mut job, &files[1]);
    bind(&mut job);
    assert_eq!(job.object.repair(&mut job.store), RepairOutcome::Clean);
    assert!(wanted(&mut job).is_empty());
    assert_eq!(job.store.volume_reads, 0);
}

// A described file no member carries is named once, by name and length,
// until the namespace announces it; bound then with every slice missing,
// the repair writes it whole.
#[test]
fn a_described_file_no_member_carries_is_named_and_rebuilt_whole() {
    let carried = payload(31, 5000);
    let absent = payload(32, 3 * BLOCK as usize - 12);
    let files = par2_set(
        "set",
        &[("carried.bin", &carried), ("absent.bin", &absent)],
        BLOCK,
        4,
        1,
    );
    let mut job = job(&files);
    let member = job.member("carried.bin", &carried);
    bind(&mut job);
    feed(&mut job, member, &carried, None);
    let mut named = Vec::new();
    job.object.absent(&mut named);
    assert_eq!(named, vec![("absent.bin".to_string(), absent.len() as u64)]);

    // Announced and closed with nothing written, as the namespace does.
    let rebuilt = job
        .namespace
        .announce("absent.bin", Some(absent.len() as u64));
    job.store.members.insert(rebuilt, vec![0; absent.len()]);
    bind(&mut job);
    assert!(job.object.covers(rebuilt));
    named.clear();
    job.object.absent(&mut named);
    assert!(named.is_empty(), "{named:?}");

    assert_eq!(
        job.object.repair(&mut job.store),
        RepairOutcome::Repaired { slices: 3 }
    );
    assert!(job.store.members[&rebuilt] == absent);
    assert!(job.store.members[&member] == carried);
}

// A recovery packet whose payload fails its hash is dropped at the scan, so
// it is never counted as a block in hand.
#[test]
fn a_recovery_packet_failing_its_hash_is_dropped_at_the_scan() {
    let data = payload(33, 5000);
    let mut files = par2_set("set", &[("payload.bin", &data)], BLOCK, 4, 1);
    let volume = &mut files[1].1;
    let at = volume
        .windows(16)
        .position(|window| window == b"PAR 2.0\0RecvSlic")
        .expect("a recovery packet");
    // Past the type and the exponent, inside the payload.
    volume[at + 16 + 4 + 10] ^= 0x01;
    let mut job = job(&files);
    job.member("payload.bin", &data);
    bind(&mut job);
    assert_eq!(job.object.blocks_reachable(), 3);
}

// A set describing more than a job may hold is refused: it binds nothing
// and names nothing to rebuild.
#[test]
fn a_set_past_the_output_limit_is_refused() {
    let data = payload(34, 5000);
    let files = par2_set("set", &[("payload.bin", &data)], BLOCK, 2, 1);
    for (limit, admitted) in [(data.len() as u64 - 1, false), (data.len() as u64, true)] {
        let mut object = Par2::new(OWN);
        object.output_limit = limit;
        let mut job = Job::new(object, OWN);
        for (name, bytes) in &files {
            job.volume(name, bytes, ARTICLE as usize, false);
        }
        let member = job.member("payload.bin", &data);
        bind(&mut job);
        assert_eq!(job.object.covers(member), admitted);
        let mut named = Vec::new();
        job.object.absent(&mut named);
        assert!(named.is_empty());
    }
}

// The namespace face with the largest single read it was asked for.
struct Widest<'a> {
    store: &'a mut crate::testing::Store,
    widest: u64,
}

impl weaver_graph::recovery::MemberAccess for Widest<'_> {
    fn read(&mut self, member: MemberId, range: ByteRange, into: &mut Vec<u8>) -> bool {
        self.widest = self.widest.max(range.len());
        self.store.read(member, range, into)
    }

    fn write_repaired(&mut self, member: MemberId, offset: u64, bytes: &[u8]) {
        self.store.write_repaired(member, offset, bytes);
    }

    fn length(&self, member: MemberId) -> Option<u64> {
        self.store.length(member)
    }

    fn read_volume(
        &mut self,
        volume: weaver_graph::VolumeIndex,
        range: ByteRange,
        into: &mut Vec<u8>,
    ) -> bool {
        self.store.read_volume(volume, range, into)
    }
}

// A member several times the repair memory limit is repaired reading one
// slice at a time.
#[test]
fn a_repair_larger_than_the_memory_limit_reads_one_slice_at_a_time() {
    const SLICE: u64 = 4096;
    const LIMIT: usize = 64 * 1024;
    let data = payload(35, 64 * SLICE as usize);
    let mut job = job(&par2_set("set", &[("payload.bin", &data)], SLICE, 2, 1));
    job.object.memory_limit = LIMIT;
    let (member, bad) = damaged_member(&mut job, "payload.bin", &data, 100_000);
    bind(&mut job);
    let wrong = job.store.members[&member].clone();
    feed(&mut job, member, &wrong, Some(bad));
    let mut access = Widest {
        store: &mut job.store,
        widest: 0,
    };
    let outcome = job.object.repair(&mut access);
    assert!(
        matches!(outcome, RepairOutcome::Repaired { slices } if slices >= 1),
        "{outcome:?}"
    );
    assert!(access.widest <= SLICE, "{}", access.widest);
    assert!(job.store.members[&member] == data);
}
