// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

mod facts;

use weaver_graph::recovery::RepairOutcome;
use weaver_graph::{
    ByteRange, MemberId, Object, ObjectId, Provenance, Recovery, SliceVerdict, VolumeIndex,
};

use super::Par2;
use crate::testing::{Job, par2_set, par2_set_from, payload, segments};

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

// Recovery blocks reachable across the sets, as their rulings say.
fn reachable(object: &Par2) -> u64 {
    let mut sets = Vec::new();
    object.sets(&mut sets);
    sets.iter().map(|set| set.blocks_reachable).sum()
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
fn unique_content_outranks_another_files_exact_name() {
    let a = payload(31, 5000);
    let b = payload(32, 5000);
    let mut job = job(&par2_set(
        "set",
        &[("a.bin", &a), ("b.bin", &b)],
        BLOCK,
        2,
        1,
    ));
    let member = job.member("b.bin", &a);
    assert_eq!(bind(&mut job), vec![member]);
    assert!(job.object.covers(member));
    assert_eq!(job.namespace.get(member).unwrap().name, "a.bin");
}

#[test]
fn identical_files_bind_to_their_exact_described_names_without_repair() {
    let data = payload(33, 5000);
    let mut job = job(&par2_set(
        "set",
        &[("a.bin", &data), ("b.bin", &data)],
        BLOCK,
        2,
        1,
    ));
    let a = job.member("a.bin", &data);
    let b = job.member("b.bin", &data);
    assert!(bind(&mut job).is_empty());
    for member in [a, b] {
        assert!(job.object.covers(member));
        feed(&mut job, member, &data, None);
        assert!(
            verdicts(&job, member)
                .iter()
                .all(|v| *v == SliceVerdict::Intact)
        );
    }
    assert_eq!(
        job.object
            .repair(&mut job.store, weaver_graph::Budget::default()),
        RepairOutcome::Clean
    );
    assert_eq!(job.store.writes, 0);
}

#[test]
fn a_content_tie_requires_an_exact_name_among_the_matching_descriptions() {
    let data = payload(33, 5000);
    let other = payload(34, 5000);
    let mut job = job(&par2_set(
        "set",
        &[("a.bin", &data), ("b.bin", &data), ("c.bin", &other)],
        BLOCK,
        2,
        1,
    ));
    for name in ["obfuscated", "A.BIN", "c.bin"] {
        let member = job.member(name, &data);
        assert!(bind(&mut job).is_empty());
        assert!(
            !job.object.covers(member),
            "{name} must not resolve the tie"
        );
    }
}

#[test]
fn an_exact_name_resolves_a_prefix_tie_without_hiding_later_damage() {
    let a = payload(35, 40_000);
    let mut b = a.clone();
    b[20_000] ^= 0x5A;
    let mut job = job(&par2_set(
        "set",
        &[("a.bin", &a), ("b.bin", &b)],
        BLOCK,
        2,
        1,
    ));
    let member = job.member("b.bin", &a);
    assert!(bind(&mut job).is_empty());
    assert!(job.object.covers(member));
    feed(&mut job, member, &a, None);
    let seen = verdicts(&job, member);
    assert!(seen.contains(&SliceVerdict::Damaged));
    assert!(!seen.contains(&SliceVerdict::Intact));
}

// A member its producer started over and announced again under the name it
// was posted with has lost the front it was known by, so nothing binds it by
// that name; once its front arrives again it is known by its content and
// renamed a second time.
#[test]
fn a_member_announced_again_under_its_posted_name_is_renamed_again_by_its_front() {
    let data = payload(3, 40_000);
    let mut job = job(&par2_set("set", &[("payload.bin", &data)], BLOCK, 2, 1));
    let member = job.member("a8f3c1e09b", &data);
    assert_eq!(bind(&mut job), vec![member]);
    job.namespace.retract(member);
    job.namespace.rename(member, "a8f3c1e09b");
    job.namespace.get_mut(member).unwrap().length = Some(data.len() as u64);
    assert!(bind(&mut job).is_empty());
    assert!(!job.object.covers(member));
    job.arrive(member, 0, &data, Provenance::Fresh);
    assert_eq!(bind(&mut job), vec![member]);
    assert!(job.object.covers(member));
    assert_eq!(job.namespace.get(member).unwrap().name, "payload.bin");
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
    assert_eq!(
        job.object
            .repair(&mut job.store, weaver_graph::Budget::default()),
        RepairOutcome::Clean
    );
    assert_eq!((job.store.reads, job.store.volume_reads), (0, 0));
}

#[test]
fn an_unfed_clean_member_is_read_back_and_found_clean() {
    let data = payload(8, 5000);
    let mut job = job(&par2_set("set", &[("payload.bin", &data)], BLOCK, 2, 1));
    job.member("payload.bin", &data);
    bind(&mut job);
    assert_eq!(
        job.object
            .repair(&mut job.store, weaver_graph::Budget::default()),
        RepairOutcome::Clean
    );
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
    let outcome = job
        .object
        .repair(&mut job.store, weaver_graph::Budget::default());
    assert!(
        matches!(outcome, RepairOutcome::Repaired { slices } if slices >= 1),
        "{outcome:?}"
    );
    assert!(job.store.members[&member] == data);
    assert!(job.store.writes > 0 && job.store.volume_reads > 0);
}

// A member that arrived with its bytes shifted along by three, so three
// longer than described, is rebuilt, and the bind after recognises it at the
// described length: what is left is the file, its whole-file hash the
// description's.
#[test]
fn a_repaired_member_longer_than_described_is_recognised_at_its_length() {
    let data = payload(12, 5000);
    let mut job = job(&par2_set("set", &[("payload.bin", &data)], BLOCK, 12, 1));
    let mut shifted = vec![0x9b, 0x7d, 0x1f];
    shifted.extend_from_slice(&data);
    let member = job.member("payload.bin", &shifted);
    assert!(bind(&mut job).is_empty());
    feed(&mut job, member, &shifted, None);
    let outcome = job
        .object
        .repair(&mut job.store, weaver_graph::Budget::default());
    assert!(
        matches!(outcome, RepairOutcome::Repaired { .. }),
        "{outcome:?}"
    );
    assert_eq!(bind(&mut job), vec![member]);
    let record = job.namespace.get(member).unwrap();
    assert_eq!(record.name, "payload.bin");
    assert_eq!(record.length, Some(data.len() as u64));
    assert!(record.is_complete());
    let described = job
        .object
        .sets
        .values()
        .find_map(|state| {
            let file = state
                .members()
                .iter()
                .find_map(|(file, bound)| (*bound == member).then_some(*file))?;
            state.usable()?.file_description(&file).cloned()
        })
        .expect("the set describes the member");
    assert_eq!(described.length, data.len() as u64);
    let repaired = &job.store.members[&member][..data.len()];
    assert_eq!(par2_rs::checksum::md5(repaired), described.hash_full);
    // Recognised once, it is not again.
    assert!(bind(&mut job).is_empty());
}

// A member none of whose bytes came, booked at more than the described
// length for the articles it lost, is rebuilt at the described length and
// takes it: what was booked never arrived, and keeps its name.
#[test]
fn a_repaired_member_booked_longer_than_described_takes_the_described_length() {
    let data = payload(13, 5000);
    let mut job = job(&par2_set("set", &[("payload.bin", &data)], BLOCK, 12, 1));
    let booked = data.len() as u64 + 300;
    let member = job.namespace.announce("payload.bin", Some(booked));
    job.store.members.insert(member, Vec::new());
    assert!(bind(&mut job).is_empty());
    let outcome = job
        .object
        .repair(&mut job.store, weaver_graph::Budget::default());
    assert!(
        matches!(outcome, RepairOutcome::Repaired { .. }),
        "{outcome:?}"
    );
    assert!(bind(&mut job).is_empty());
    let record = job.namespace.get(member).unwrap();
    assert_eq!(record.name, "payload.bin");
    assert_eq!(record.length, Some(data.len() as u64));
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
        job.object
            .repair(&mut job.store, weaver_graph::Budget::default()),
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
    assert_eq!(reachable(&job.object), 4);
    assert!(matches!(
        job.object
            .repair(&mut job.store, weaver_graph::Budget::default()),
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
    let before = reachable(&job.object);
    assert_eq!(before, 4);
    let lost = files[2].1.len() as u64;
    let mut out = Vec::new();
    job.object.volume_lost(
        weaver_graph::VolumeIndex(2),
        ByteRange::new(0, lost),
        &mut out,
    );
    assert!(reachable(&job.object) < before);
}

// Two losses told apart, then told again as the one run they make, as a
// restore tells them: the blocks they take from what the open volume is
// asked to bring are counted once.
#[test]
fn a_loss_told_again_as_one_run_costs_its_blocks_once() {
    let data = payload(15, 5000);
    let files = par2_set("set", &[("payload.bin", &data)], BLOCK, 16, 2);
    let mut job = job(&files[..2]);
    job.member("payload.bin", &data);
    let later = named_volume(&mut job, &files[files.len() - 1]);
    bind(&mut job);
    let coming = |object: &Par2| object.volumes[later.0 as usize].coming(object.smallest_packet());
    let whole = coming(&job.object);
    let mut out = Vec::new();
    job.object
        .volume_lost(later, ByteRange::new(0, 1), &mut out);
    job.object
        .volume_lost(later, ByteRange::new(1, 2), &mut out);
    let lost = coming(&job.object);
    assert!(lost < whole, "the losses cost blocks");
    job.object
        .volume_lost(later, ByteRange::new(0, 2), &mut out);
    job.object
        .volume_lost(later, ByteRange::new(1, 2), &mut out);
    assert_eq!(coming(&job.object), lost);
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
        job.object
            .repair(&mut job.store, weaver_graph::Budget::default()),
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
    Recovery::wants(&job.object, &mut seeks);
    assert!(!seeks.is_empty());
    assert!(
        seeks
            .iter()
            .all(|seek| seek.volume == volume && seek.purpose == weaver_graph::Purpose::Recovery)
    );
    assert_eq!(
        job.object
            .repair(&mut job.store, weaver_graph::Budget::default()),
        RepairOutcome::Waiting
    );
}

// The PAR2 file with every packet of the given types left out: an index
// posted without the descriptions, which ride in the volumes instead.
fn without_packets(bytes: &[u8], kinds: &[&[u8; 16]]) -> Vec<u8> {
    let mut out = Vec::new();
    let mut at = 0;
    while at + 64 <= bytes.len() {
        let length = u64::from_le_bytes(bytes[at + 8..at + 16].try_into().unwrap()) as usize;
        if !kinds.iter().any(|kind| bytes[at + 48..at + 64] == kind[..]) {
            out.extend_from_slice(&bytes[at..at + length]);
        }
        at += length;
    }
    out
}

const FILE_DESC: &[u8; 16] = b"PAR 2.0\0FileDesc";
const IFSC: &[u8; 16] = b"PAR 2.0\0IFSC\0\0\0\0";

// An index read through with no description in it, one recovery volume
// named and not yet in, and the member the set protects.
fn undescribed(seed: u64, damaged: bool) -> (Job<Par2>, VolumeIndex, MemberId) {
    let data = payload(seed, 5000);
    let files = par2_set("set", &[("payload.bin", &data)], BLOCK, 4, 2);
    let mut job = Job::new(Par2::new(OWN), OWN);
    let index = without_packets(&files[0].1, &[FILE_DESC, IFSC]);
    job.volume(&files[0].0, &index, ARTICLE as usize, false);
    let volume = job.name(&files[1].0, &files[1].1);
    let member = if damaged {
        damaged_member(&mut job, "payload.bin", &data, 300).0
    } else {
        job.member("payload.bin", &data)
    };
    bind(&mut job);
    assert!(!job.object.sets.is_empty());
    assert!(
        job.object
            .sets
            .values()
            .all(|state| state.usable().is_none())
    );
    (job, volume, member)
}

fn asked(job: &Job<Par2>) -> Vec<VolumeIndex> {
    let mut seeks = Vec::new();
    Recovery::wants(&job.object, &mut seeks);
    seeks
        .iter()
        .filter(|seek| seek.purpose == weaver_graph::Purpose::Recovery)
        .map(|seek| seek.volume)
        .collect()
}

// A set whose index carries no description reads a volume for one only
// once the namespace shows damage it may cover. On a whole post it checks
// nothing and asks for nothing: no recovery byte is spent on it. Damage
// that shows after it ruled so, a range refused or bytes held damaged,
// makes it due again.
#[test]
fn an_undescribed_set_asks_for_a_volume_only_once_damage_shows() {
    let (mut job, volume, member) = undescribed(23, false);
    assert_eq!(
        job.object
            .repair(&mut job.store, weaver_graph::Budget::default()),
        RepairOutcome::Unread
    );
    assert!(asked(&job).is_empty());
    assert_eq!(job.object.outcome(), Some(RepairOutcome::Unread));
    Recovery::refused(&mut job.object, member, ByteRange::new(0, 700));
    assert_eq!(job.object.outcome(), None);
    assert_eq!(
        job.object
            .repair(&mut job.store, weaver_graph::Budget::default()),
        RepairOutcome::Waiting
    );
    assert_eq!(asked(&job), vec![volume]);

    let (mut job, volume, _) = undescribed(23, true);
    assert_eq!(
        job.object
            .repair(&mut job.store, weaver_graph::Budget::default()),
        RepairOutcome::Waiting
    );
    let wanted = asked(&job);
    assert!(!wanted.is_empty());
    assert!(wanted.iter().all(|asked| *asked == volume));
}

// A set that ruled it checked nothing, with no volume left to describe
// it, is tried again when a volume binds: the descriptions may be in it.
// A file lost whole is refused, never fed; the refusal is the damage.
#[test]
fn a_volume_bound_after_an_unread_ruling_reopens_the_set() {
    let data = payload(24, 5000);
    let files = par2_set("set", &[("payload.bin", &data)], BLOCK, 4, 2);
    let mut job = Job::new(Par2::new(OWN), OWN);
    let index = without_packets(&files[0].1, &[FILE_DESC, IFSC]);
    job.volume(&files[0].0, &index, ARTICLE as usize, false);
    let member = job
        .namespace
        .announce("payload.bin", Some(data.len() as u64));
    Recovery::refused(
        &mut job.object,
        member,
        ByteRange::new(0, data.len() as u64),
    );
    bind(&mut job);
    assert_eq!(
        job.object
            .repair(&mut job.store, weaver_graph::Budget::default()),
        RepairOutcome::Unread
    );
    assert_eq!(job.object.outcome(), Some(RepairOutcome::Unread));
    let (name, bytes) = &files[1];
    let volume = job.name(name, bytes);
    assert_eq!(job.object.outcome(), None);
    assert_eq!(
        job.object
            .repair(&mut job.store, weaver_graph::Budget::default()),
        RepairOutcome::Waiting
    );
    assert_eq!(asked(&job), vec![volume]);
    let mut out = Vec::new();
    for (at, piece) in bytes.chunks(ARTICLE as usize).enumerate() {
        job.object.deliver(
            weaver_graph::Delivery {
                volume,
                offset: at as u64 * ARTICLE,
                bytes: piece,
                provenance: Provenance::Fresh,
            },
            &mut |_, _| false,
            &mut out,
        );
    }
    job.object.volume_closed(volume, &mut out);
    assert!(
        job.object
            .sets
            .values()
            .all(|state| state.usable().is_some())
    );
    assert_eq!(job.object.outcome(), None);
}

// A set short of recovery blocks waits while the pieces that bring none
// arrive, and is due again in the step the block it needs is in hand: no
// repair and no close between.
#[test]
fn a_short_set_waits_until_the_blocks_it_needs_are_in_hand() {
    let data = payload(18, 5000);
    let files = par2_set("set", &[("payload.bin", &data)], BLOCK, 4, 2);
    let mut job = Job::new(Par2::new(OWN), OWN);
    job.volume(&files[0].0, &files[0].1, ARTICLE as usize, false);
    let (name, bytes) = &files[1];
    let volume = job.name(name, bytes);
    let (member, bad) = damaged_member(&mut job, "payload.bin", &data, 300);
    bind(&mut job);
    let wrong = job.store.members[&member].clone();
    feed(&mut job, member, &wrong, Some(bad));
    assert_eq!(
        job.object
            .repair(&mut job.store, weaver_graph::Budget::default()),
        RepairOutcome::Waiting
    );
    let held = |job: &Job<Par2>| -> usize {
        job.object
            .sets
            .values()
            .map(|state| state.recovery.len())
            .sum()
    };
    let mut out = Vec::new();
    for (index, piece) in bytes.chunks(ARTICLE as usize).enumerate() {
        assert_eq!(job.object.outcome(), Some(RepairOutcome::Waiting));
        job.object.deliver(
            weaver_graph::Delivery {
                volume,
                offset: index as u64 * ARTICLE,
                bytes: piece,
                provenance: Provenance::Fresh,
            },
            &mut |_, _| false,
            &mut out,
        );
        assert_eq!(job.object.outcome().is_none(), held(&job) > 0);
        if held(&job) > 0 {
            break;
        }
    }
    assert_eq!(held(&job), 1);
    assert_eq!(
        job.object
            .repair(&mut job.store, weaver_graph::Budget::default()),
        RepairOutcome::Repaired { slices: 1 }
    );
    assert_eq!(
        job.object.outcome(),
        Some(RepairOutcome::Repaired { slices: 1 })
    );
    assert!(job.store.members[&member] == data);
}

// An object that has found no set has not ruled while its volume's bytes
// may still hold one: the front of an index is only a packet header. Once
// the volume is read through with no set in it, nothing was checked: it is
// Unread, no verdict on any member, not due again every step, and a set
// found later makes it due once.
#[test]
fn an_object_with_no_sets_rules_unread_once_a_volume_is_read_through() {
    let data = payload(18, 5000);
    let files = par2_set("set", &[("payload.bin", &data)], BLOCK, 4, 2);
    let mut job = Job::new(Par2::new(OWN), OWN);
    assert_eq!(job.object.outcome(), None);
    let (name, bytes) = &files[0];
    let header = &bytes[..par2_rs::packet::HEADER_SIZE];
    let volume = job.name(name, header);
    let mut out = Vec::new();
    job.object.deliver(
        weaver_graph::Delivery {
            volume,
            offset: 0,
            bytes: header,
            provenance: Provenance::Fresh,
        },
        &mut |_, _| false,
        &mut out,
    );
    assert!(job.object.sets.is_empty());
    assert_eq!(job.object.outcome(), None);
    job.object.volume_closed(volume, &mut out);
    assert_eq!(job.object.outcome(), Some(RepairOutcome::Unread));
    assert_eq!(
        job.object
            .repair(&mut job.store, weaver_graph::Budget::default()),
        RepairOutcome::Unread
    );
    assert_eq!(job.object.outcome(), Some(RepairOutcome::Unread));
    job.volume(&files[1].0, &files[1].1, ARTICLE as usize, false);
    assert_eq!(job.object.outcome(), None);
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
    let volume = job
        .object
        .claim(name, &bytes[..64], None)
        .expect("a PAR2 volume");
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
    assert_eq!(reachable(&job.object), clear);
}

// A set's volume named but not yet arrived: claimed from its front, so
// nothing of it is walked and it may be any set's.
fn named_volume(job: &mut Job<Par2>, file: &(String, Vec<u8>)) -> weaver_graph::VolumeIndex {
    job.name(&file.0, &file.1)
}

fn wanted(job: &mut Job<Par2>) -> Vec<weaver_graph::VolumeIndex> {
    let mut seeks = Vec::new();
    Recovery::wants(&job.object, &mut seeks);
    let mut volumes: Vec<_> = seeks.iter().map(|seek| seek.volume).collect();
    volumes.dedup();
    volumes
}

// No index and no set found yet: the volume read for the set's
// descriptions is lost whole. Closed over a loss, it was never read
// through, so another volume is read in its place.
#[test]
fn a_volume_lost_whole_is_not_read_through_and_another_is_read_for_the_set() {
    let data = payload(24, 5000);
    let files = par2_set("set", &[("payload.bin", &data)], BLOCK, 8, 3);
    let mut job = Job::new(Par2::new(OWN), OWN);
    for file in &files[1..] {
        named_volume(&mut job, file);
    }
    assert_eq!(
        job.object
            .repair(&mut job.store, weaver_graph::Budget::default()),
        RepairOutcome::Waiting
    );
    let first = wanted(&mut job);
    assert_eq!(first.len(), 1, "one volume is read for the descriptions");
    let mut out = Vec::new();
    let length = job.store.volumes[&first[0]].len() as u64;
    job.object
        .volume_lost(first[0], ByteRange::new(0, length), &mut out);
    job.object.volume_closed(first[0], &mut out);
    let next = wanted(&mut job);
    assert_eq!(next.len(), 1, "another volume is read in its place");
    assert_ne!(next, first);
    assert_eq!(job.object.outcome(), Some(RepairOutcome::Waiting));
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
        job.object
            .repair(&mut job.store, weaver_graph::Budget::default()),
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
    assert_eq!(
        job.object
            .repair(&mut job.store, weaver_graph::Budget::default()),
        RepairOutcome::Clean
    );
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
    assert_eq!(
        job.object
            .repair(&mut job.store, weaver_graph::Budget::default()),
        RepairOutcome::Clean
    );
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
        job.object
            .repair(&mut job.store, weaver_graph::Budget::default()),
        RepairOutcome::Repaired { slices: 3 }
    );
    assert!(job.store.members[&rebuilt] == absent);
    assert!(job.store.members[&member] == carried);
}

// A posted file under a name no description answers to, its first article
// lost: its front names nothing, so its slices do. It is not announced
// again beside itself; the repair knows it by a slice it reads back and
// mends it in place, and the bind after names it what the set describes.
#[test]
fn a_member_whose_front_was_lost_is_known_by_its_slices_and_mended_in_place() {
    let file = payload(33, 6 * BLOCK as usize - 40);
    let files = par2_set("set", &[("described.bin", &file)], BLOCK, 4, 1);
    let mut job = job(&files);
    let posted = job.member("5f0c9e2ab1d74c6e", &file);
    let lost = vec![0; ARTICLE as usize];
    job.arrive(posted, 0, &lost, Provenance::Damaged);
    job.store.members.get_mut(&posted).expect("posted")[..lost.len()].copy_from_slice(&lost);
    assert!(bind(&mut job).is_empty());
    assert!(!job.object.covers(posted));
    let mut named = Vec::new();
    job.object.absent(&mut named);
    assert!(named.is_empty(), "{named:?}");

    assert_eq!(
        job.object
            .repair(&mut job.store, weaver_graph::Budget::default()),
        RepairOutcome::Repaired { slices: 2 }
    );
    assert!(job.store.members[&posted] == file);
    assert!(job.object.covers(posted));

    // The namespace delivers the mended front as repaired bytes.
    job.arrive(posted, 0, &file[..lost.len()], Provenance::Repaired);
    assert_eq!(bind(&mut job), vec![posted]);
    assert!(job.object.covers(posted));
    assert_eq!(
        job.namespace.get(posted).expect("posted").name,
        "described.bin"
    );
}

// A posted file under a name no description answers to, every byte of it
// lost: no front and no slice can say what it is, and the post's encoded
// size only bounds its length, so it is what is left, the one free file it
// could be. The repair mends it in place and the bind after recognises it,
// name and length.
#[test]
fn a_member_with_no_intact_byte_binds_by_elimination_and_is_mended_in_place() {
    let file = payload(34, 3 * BLOCK as usize - 24);
    let files = par2_set("set", &[("described.bin", &file)], BLOCK, 4, 1);
    let mut job = job(&files);
    let lost = vec![0; file.len() + 100];
    let posted = job.member("5f0c9e2ab1d74c6e", &lost);
    job.arrive(posted, 0, &lost, Provenance::Damaged);
    assert!(bind(&mut job).is_empty());
    let mut named = Vec::new();
    job.object.absent(&mut named);
    assert!(named.is_empty(), "{named:?}");

    assert_eq!(
        job.object
            .repair(&mut job.store, weaver_graph::Budget::default()),
        RepairOutcome::Repaired { slices: 3 }
    );
    assert!(job.store.members[&posted][..file.len()] == file);
    assert!(job.object.covers(posted));

    job.arrive(posted, 0, &file, Provenance::Repaired);
    assert_eq!(bind(&mut job), vec![posted]);
    let record = job.namespace.get(posted).expect("posted");
    assert_eq!(record.name, "described.bin");
    assert_eq!(record.length, Some(file.len() as u64));
    assert!(record.is_complete());
}

// An all-lost member that could be either of two free files is neither
// while the other file's member is still to come: elimination binds it only
// once that member has named itself, and then to the one file left, which
// the repair mends in place.
#[test]
fn elimination_waits_for_the_members_still_to_come() {
    let first = payload(35, 3 * BLOCK as usize - 24);
    let second = payload(36, 4 * BLOCK as usize - 8);
    let files = par2_set(
        "set",
        &[
            ("first.bin", first.as_slice()),
            ("second.bin", second.as_slice()),
        ],
        BLOCK,
        4,
        1,
    );
    let mut job = job(&files);
    let lost = vec![0; second.len() + 100];
    let posted = job.member("5f0c9e2ab1d74c6e", &lost);
    job.arrive(posted, 0, &lost, Provenance::Damaged);
    let coming = job
        .namespace
        .announce("a91d07e4c3b25f86", Some(second.len() as u64));
    assert!(bind(&mut job).is_empty());

    assert_eq!(
        job.object
            .repair(&mut job.store, weaver_graph::Budget::default()),
        RepairOutcome::Clean
    );
    assert!(!job.object.covers(posted));
    assert!(job.store.members[&posted] == lost);

    job.arrive(coming, 0, &second, Provenance::Fresh);
    job.store.members.insert(coming, second.clone());
    assert_eq!(bind(&mut job), vec![coming]);
    assert_eq!(
        job.object
            .repair(&mut job.store, weaver_graph::Budget::default()),
        RepairOutcome::Repaired { slices: 3 }
    );
    assert!(job.object.covers(posted));
    assert!(job.store.members[&posted][..first.len()] == first);
    assert!(job.store.members[&coming] == second);
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
    assert_eq!(reachable(&job.object), 3);
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
    let outcome = job
        .object
        .repair(&mut access, weaver_graph::Budget::default());
    assert!(
        matches!(outcome, RepairOutcome::Repaired { slices } if slices >= 1),
        "{outcome:?}"
    );
    assert!(access.widest <= SLICE, "{}", access.widest);
    assert!(job.store.members[&member] == data);
}

// The memory a repair is granted is its one limit, above the object's own
// allowance as well as below it; the allowance is what it holds to only
// when it is granted none.
#[test]
fn a_repair_holds_to_the_memory_it_was_granted_not_the_objects_own() {
    let mut object = Par2::new(OWN);
    object.memory_limit = 64 * 1024;
    let granted = |memory_bytes| weaver_graph::Budget {
        memory_bytes,
        threads: 1,
        ..Default::default()
    };
    assert_eq!(object.workspace(granted(Some(1 << 30))), 1 << 30);
    assert_eq!(object.workspace(granted(Some(4096))), 4096);
    assert_eq!(object.workspace(granted(None)), 64 * 1024);
}

// A recovery volume whose first article arrived, the rest still to come.
fn head(job: &mut Job<Par2>, file: &(String, Vec<u8>)) -> weaver_graph::VolumeIndex {
    let volume = job.name(&file.0, &file.1);
    let end = (ARTICLE as usize).min(file.1.len());
    job.object.deliver(
        weaver_graph::Delivery {
            volume,
            offset: 0,
            bytes: &file.1[..end],
            provenance: Provenance::Fresh,
        },
        &mut |_, _| false,
        &mut Vec::new(),
    );
    volume
}

fn seeks(object: &Par2) -> Vec<weaver_graph::Seek> {
    let mut out = Vec::new();
    Recovery::wants(object, &mut out);
    out
}

// The head of a recovery volume arrives before any damage: what it showed
// says the volume carries blocks, and the rest of it is deferred.
#[test]
fn a_recovery_volume_whose_head_arrived_before_damage_is_deferred() {
    let data = payload(40, 5000);
    let files = par2_set("set", &[("payload.bin", &data)], BLOCK, 4, 2);
    let mut job = job(&files[..1]);
    let volume = head(&mut job, &files[1]);
    job.member("payload.bin", &data);
    bind(&mut job);
    assert_eq!(
        seeks(&job.object),
        vec![weaver_graph::Seek::deferred(
            volume,
            ByteRange::new(ARTICLE, u64::MAX)
        )]
    );
}

const DAMAGED: [usize; 4] = [100, 1100, 2100, 3100];

// Damage appears: four slices bad, three blocks in hand from the heads. The
// one block short is wanted from the volume named for the fewest blocks,
// whole, and the others stay deferred.
#[test]
fn damage_raises_the_smallest_volumes_its_shortfall_needs_and_defers_the_rest() {
    let data = payload(41, 5000);
    let files = par2_set("set", &[("payload.bin", &data)], BLOCK, 8, 3);
    let mut volumes: Vec<_> = files[1..].to_vec();
    let blocks = |name: &str| crate::recovery_volume::named_blocks(name, ".par2").unwrap_or(0);
    volumes.sort_by_key(|(name, _)| std::cmp::Reverse(blocks(name)));
    let mut job = job(&files[..1]);
    let sampled: Vec<_> = volumes.iter().map(|file| head(&mut job, file)).collect();
    let mut wrong = data.clone();
    for at in DAMAGED {
        wrong[at] ^= 0x33;
    }
    let bad = ByteRange::new(0, 3500);
    let member = job.member("payload.bin", &wrong);
    job.arrive(member, 0, &wrong[..3500], Provenance::Damaged);
    bind(&mut job);
    feed(&mut job, member, &wrong, Some(bad));
    let wanted = seeks(&job.object);
    let names: Vec<_> = volumes.iter().map(|(name, _)| name.as_str()).collect();
    let now: Vec<_> = wanted
        .iter()
        .filter(|seek| seek.when == weaver_graph::When::Now)
        .map(|seek| seek.volume)
        .collect();
    let later: Vec<_> = wanted
        .iter()
        .filter(|seek| seek.when == weaver_graph::When::Deferred)
        .map(|seek| seek.volume)
        .collect();
    let smallest = *sampled.last().unwrap();
    assert_eq!(now, vec![smallest], "{names:?} {later:?}");
    assert!(later.iter().all(|volume| *volume != smallest));
    assert_eq!(later.len(), sampled.len() - 1);
    assert!(
        wanted
            .iter()
            .filter(|seek| seek.when == weaver_graph::When::Now)
            .all(|seek| seek.purpose == weaver_graph::Purpose::Recovery
                && seek.range == ByteRange::new(ARTICLE, u64::MAX))
    );
}

// One recovery packet past the hold cap, every piece but its first article
// arriving first: the hold fills to just under the cap, and the first
// article, which would take it over, lands at the front, so it is walked
// and the held bytes drain into the packet.
#[test]
fn the_article_that_closes_the_gap_is_walked_with_the_hold_full() {
    use par2_rs::FileHashState;
    use par2_rs::packet::{HEADER_SIZE, MAGIC};

    use super::walker::{Found, Walker};

    let data = payload(9, 8 * 1024 * 1024);
    let length = (HEADER_SIZE + 4 + data.len()) as u64;
    let mut packet = Vec::with_capacity(length as usize);
    packet.extend_from_slice(MAGIC);
    packet.extend_from_slice(&length.to_le_bytes());
    packet.extend_from_slice(&[0; 16]);
    packet.extend_from_slice(&[7; 16]);
    packet.extend_from_slice(b"PAR 2.0\0RecvSlic");
    packet.extend_from_slice(&3u32.to_le_bytes());
    packet.extend_from_slice(&data);
    let mut hasher = FileHashState::new();
    hasher.update(&packet[32..]);
    let hash = hasher.finalize();
    packet[16..32].copy_from_slice(&hash);

    let first = 4096;
    let mut walker = Walker::default();
    let mut found = Vec::new();
    for (index, piece) in packet[first..].chunks(64 * 1024).enumerate() {
        walker.deliver((first + index * 64 * 1024) as u64, piece, &mut found);
    }
    assert!(found.is_empty(), "nothing walks before the front arrives");
    walker.deliver(0, &packet[..first], &mut found);
    assert!(
        matches!(
            found.as_slice(),
            [Found::Recovery { exponent: 3, range, .. }] if *range == ByteRange::new(0, length)
        ),
        "{found:?}"
    );
    assert_eq!(walker.front(), length);
}

// Every packet of a PAR2 file, by where it lies and whether it carries a
// recovery block.
fn packets_of(bytes: &[u8]) -> Vec<(ByteRange, bool)> {
    let mut out = Vec::new();
    let mut at = 0usize;
    while at + 64 <= bytes.len() {
        let length = u64::from_le_bytes(bytes[at + 8..at + 16].try_into().expect("8 bytes"));
        let recovery = &bytes[at + 48..at + 64] == b"PAR 2.0\0RecvSlic";
        out.push((ByteRange::at(at as u64, length), recovery));
        at += length as usize;
    }
    out
}

// The articles of a volume, delivered in order, those inside `damaged`
// with every byte wrong.
fn deliver_articles(
    object: &mut Par2,
    volume: weaver_graph::VolumeIndex,
    bytes: &[u8],
    damaged: Option<ByteRange>,
) {
    for (index, piece) in bytes.chunks(ARTICLE as usize).enumerate() {
        let offset = index as u64 * ARTICLE;
        let mut piece = piece.to_vec();
        let range = ByteRange::at(offset, piece.len() as u64);
        let provenance = match damaged.and_then(|damaged| damaged.intersect(range)) {
            Some(_) => {
                piece.iter_mut().for_each(|byte| *byte ^= 0x5A);
                Provenance::Damaged
            }
            None => Provenance::Fresh,
        };
        object.deliver(
            weaver_graph::Delivery {
                volume,
                offset,
                bytes: &piece,
                provenance,
            },
            &mut |_, _| false,
            &mut Vec::new(),
        );
    }
}

// The articles covering the first recovery packet whose articles end before
// the volume does, so the walk goes on past them.
fn passed_articles(bytes: &[u8]) -> ByteRange {
    packets_of(bytes)
        .into_iter()
        .filter(|(_, recovery)| *recovery)
        .map(|(packet, _)| {
            ByteRange::new(
                packet.start / ARTICLE * ARTICLE,
                packet.end.div_ceil(ARTICLE) * ARTICLE,
            )
        })
        .find(|articles| articles.end < bytes.len() as u64)
        .expect("a recovery packet clear of the last article")
}

// Recovery packets the walk can find once `range` came again: every one
// that does not straddle its edges, whose bytes before or after it do not
// come again.
fn findable(bytes: &[u8], range: ByteRange) -> u64 {
    packets_of(bytes)
        .into_iter()
        .filter(|(packet, recovery)| {
            *recovery && (range.covers(*packet) || packet.intersect(range).is_none())
        })
        .count() as u64
}

fn redeliver(object: &mut Par2, volume: weaver_graph::VolumeIndex, bytes: &[u8], range: ByteRange) {
    object.deliver(
        weaver_graph::Delivery {
            volume,
            offset: range.start,
            bytes: &bytes[range.start as usize..range.end as usize],
            provenance: Provenance::Fresh,
        },
        &mut |_, _| false,
        &mut Vec::new(),
    );
}

// A recovery volume's articles arrive damaged and the walk goes on past
// them; their bytes come again correct, and every packet wholly inside them
// is found from what came, each counted once.
#[test]
fn articles_the_walk_read_damaged_are_walked_again_when_they_come_again() {
    let data = payload(50, 5000);
    let files = par2_set("set", &[("payload.bin", &data)], BLOCK, 4, 1);
    assert_eq!(files.len(), 2);
    let mut job = job(&files[..1]);
    job.member("payload.bin", &data);
    let (name, bytes) = &files[1];
    let volume = job.name(name, bytes);
    let range = passed_articles(bytes);
    deliver_articles(&mut job.object, volume, bytes, Some(range));
    bind(&mut job);
    let in_hand = |object: &Par2| {
        object
            .sets
            .values()
            .map(|state| state.recovery.len() as u64)
            .sum::<u64>()
    };
    let missed = in_hand(&job.object);
    job.object.unposted(volume, range);
    redeliver(&mut job.object, volume, bytes, range);
    let named = job.object.volumes[volume.0 as usize].named_blocks;
    let walked = job.object.volumes[volume.0 as usize].walked_blocks;
    job.object.volume_closed(volume, &mut Vec::new());
    bind(&mut job);
    let expected = findable(bytes, range);
    assert_eq!(reachable(&job.object), expected);
    assert!(expected > missed, "the damaged articles held a packet");
    assert_eq!(walked, expected, "every block found is counted once");
    assert_eq!(named, Some(4));
}

// The same, the bytes coming again before and after a checkpoint taken
// between: the checkpoint resumes at the start of the range, steps over
// what the walk read past it, and the replay of every byte finds each
// packet once.
#[test]
fn a_walk_checkpointed_after_its_range_is_unposted_resumes_at_the_range() {
    let data = payload(51, 5000);
    let files = par2_set("set", &[("payload.bin", &data)], BLOCK, 4, 1);
    let mut job = job(&files[..1]);
    job.member("payload.bin", &data);
    let (name, bytes) = &files[1];
    let volume = job.name(name, bytes);
    let range = passed_articles(bytes);
    deliver_articles(&mut job.object, volume, bytes, Some(range));
    job.object.unposted(volume, range);
    let walk = &job.object.volumes[volume.0 as usize].walker;
    assert_eq!(walk.front(), range.start, "the checkpoint's front");
    assert!(
        walk.lost()
            .contains(&ByteRange::new(range.end, bytes.len() as u64)),
        "{:?}",
        walk.lost()
    );
    let mut restored = super::facts::par2(&job.object.facts()).expect("restores");
    deliver_articles(&mut restored, volume, bytes, None);
    let walked = restored.volumes[volume.0 as usize].walked_blocks;
    restored.volume_closed(volume, &mut Vec::new());
    job.object = restored;
    bind(&mut job);
    assert_eq!(reachable(&job.object), findable(bytes, range));
    assert_eq!(walked, findable(bytes, range));
}

// A recovery packet built by hand: header, exponent and payload, its hash
// over everything past the hash.
fn recovery_packet(exponent: u32, payload: &[u8]) -> Vec<u8> {
    use par2_rs::FileHashState;
    use par2_rs::packet::{HEADER_SIZE, MAGIC};

    let length = (HEADER_SIZE + 4 + payload.len()) as u64;
    let mut packet = Vec::with_capacity(length as usize);
    packet.extend_from_slice(MAGIC);
    packet.extend_from_slice(&length.to_le_bytes());
    packet.extend_from_slice(&[0; 16]);
    packet.extend_from_slice(&[7; 16]);
    packet.extend_from_slice(b"PAR 2.0\0RecvSlic");
    packet.extend_from_slice(&exponent.to_le_bytes());
    packet.extend_from_slice(payload);
    let mut hasher = FileHashState::new();
    hasher.update(&packet[32..]);
    let hash = hasher.finalize();
    packet[16..32].copy_from_slice(&hash);
    packet
}

fn recovered(found: &[super::walker::Found]) -> Vec<(u32, ByteRange)> {
    found
        .iter()
        .filter_map(|found| match found {
            super::walker::Found::Recovery {
                exponent, range, ..
            } => Some((*exponent, *range)),
            _ => None,
        })
        .collect()
}

// A recovery packet streamed when the range it is read from is unposted is
// dropped, its hash having read the bytes that were there, and found again
// from the bytes that come. One streamed past a range behind the front is
// not touched: the walk past the range goes on.
#[test]
fn a_streamed_packet_in_an_unposted_range_is_dropped_and_found_again() {
    use super::walker::Walker;

    let first = recovery_packet(1, &payload(52, 4000));
    let second = recovery_packet(2, &payload(53, 4000));
    let mut volume = first.clone();
    volume.extend_from_slice(&second);
    let one = ByteRange::at(0, first.len() as u64);
    let two = ByteRange::at(one.end, second.len() as u64);

    // Inside the range: the front stands in it, and starts again from it.
    let mut walker = Walker::default();
    let mut found = Vec::new();
    let mut wrong = volume[..2000].to_vec();
    wrong[1000] ^= 0x01;
    walker.deliver(0, &wrong, &mut found);
    assert!(walker.reading_recovery());
    walker.unposted(ByteRange::new(0, 2000), &mut found);
    assert_eq!(walker.front(), 0);
    walker.deliver(0, &volume[..2000], &mut found);
    walker.deliver(2000, &volume[2000..], &mut found);
    assert_eq!(recovered(&found), vec![(1, one), (2, two)]);

    // Behind the front: the first packet is walked again on its own while
    // the second streams on.
    let mut walker = Walker::default();
    let mut found = Vec::new();
    let mut wrong = volume[..one.end as usize].to_vec();
    wrong[1000] ^= 0x01;
    walker.deliver(0, &wrong, &mut found);
    walker.deliver(
        one.end,
        &volume[one.end as usize..two.start as usize + 2000],
        &mut found,
    );
    assert!(recovered(&found).is_empty(), "the first failed its hash");
    walker.unposted(one, &mut found);
    assert_eq!(walker.front(), 0, "a checkpoint resumes at the range");
    assert!(walker.lost().is_empty(), "the second starts where it ends");
    walker.deliver(0, &volume[..one.end as usize], &mut found);
    assert_eq!(recovered(&found), vec![(1, one)]);
    assert_eq!(walker.front(), two.start, "the second still streams");
    walker.deliver(
        two.start + 2000,
        &volume[two.start as usize + 2000..],
        &mut found,
    );
    assert_eq!(recovered(&found), vec![(1, one), (2, two)]);
    assert_eq!(walker.front(), two.end);
}

// A packet whose bytes fail its hash, or whose header claims past the
// volume's end, was none. The walk reads what it took for the packet again
// from the volume, from the first magic past its own, so a valid copy that
// lies within those bytes is found, whether the volume's length was known
// before the bytes came or only at its close.
#[test]
fn a_packet_that_was_none_is_read_again_from_the_next_magic() {
    use super::walker::{Found, Walker};

    let valid = recovery_packet(1, &payload(54, 4000));
    // Its length field claims eight bytes more: into the valid copy.
    let mut longer = valid.clone();
    longer[8] ^= 0x08;
    // Cut to its magic: the header read with it is the valid copy's front.
    let magic = valid[..8].to_vec();
    for bad in [longer, magic] {
        for sized_first in [true, false] {
            let mut volume = bad.clone();
            volume.extend_from_slice(&valid);
            let length = volume.len() as u64;
            let copy = ByteRange::at(bad.len() as u64, valid.len() as u64);
            let context = format!("bad of {} bytes, sized_first={sized_first}", bad.len());
            let mut walker = Walker::default();
            let mut found = Vec::new();
            if sized_first {
                walker.sized(length, &mut found);
            }
            walker.deliver(0, &volume, &mut found);
            if !sized_first {
                walker.sized(length, &mut found);
            }
            let again: Vec<ByteRange> = found
                .iter()
                .filter_map(|found| match found {
                    Found::Again(range) => Some(*range),
                    _ => None,
                })
                .collect();
            for range in again {
                assert_eq!(range.start, copy.start, "{context}: at the copy's magic");
                assert_eq!(
                    walker.front(),
                    copy.start,
                    "{context}: the walk starts there"
                );
                walker.deliver(
                    range.start,
                    &volume[range.start as usize..range.end as usize],
                    &mut found,
                );
            }
            assert_eq!(recovered(&found), vec![(1, copy)], "{context}");
            assert_eq!(walker.front(), length, "{context}");
        }
    }
}

// A packet that was none, in whose bytes no magic begins, is read again
// only where a magic could still begin: its last bytes, short of a whole
// magic. Its payload is not read twice, however long it is.
#[test]
fn a_packet_that_was_none_reads_again_only_where_a_packet_can_begin() {
    use super::walker::{Found, Walker};
    use par2_rs::packet::MAGIC;

    let valid = recovery_packet(1, &payload(55, 64 * 1024));
    let mut bad = valid.clone();
    let last = bad.len() - 1;
    bad[last] ^= 0x01;
    let mut volume = bad.clone();
    volume.extend_from_slice(&valid);
    let copy = ByteRange::at(bad.len() as u64, valid.len() as u64);
    let mut walker = Walker::default();
    let mut found = Vec::new();
    walker.deliver(0, &volume, &mut found);
    let again: Vec<ByteRange> = found
        .iter()
        .filter_map(|found| match found {
            Found::Again(range) => Some(*range),
            _ => None,
        })
        .collect();
    assert_eq!(
        again,
        vec![ByteRange::new(
            copy.start + 1 - MAGIC.len() as u64,
            copy.start
        )]
    );
    for range in again {
        walker.deliver(
            range.start,
            &volume[range.start as usize..range.end as usize],
            &mut found,
        );
    }
    assert_eq!(recovered(&found), vec![(1, copy)]);
    assert_eq!(walker.front(), volume.len() as u64);
}

// A refused slice is held only by a twin that is neither refused nor came
// damaged: the first two slices of one file came damaged and the other
// file was refused whole, so four slices lack their bytes and the one
// recovery block cannot rebuild them.
#[test]
fn a_refused_slice_whose_twin_came_damaged_is_lost() {
    let data = payload(41, 5000);
    let files = par2_set(
        "set",
        &[("first.bin", &data), ("again.bin", &data)],
        BLOCK,
        1,
        1,
    );
    let mut job = job(&files);
    let mut wrong = data.clone();
    wrong[0] ^= 0x5A;
    wrong[600] ^= 0x5A;
    let first = job.member("first.bin", &wrong);
    let again = job.namespace.announce("again.bin", Some(data.len() as u64));
    job.store.members.insert(again, vec![0; data.len()]);
    Recovery::refused(&mut job.object, again, ByteRange::new(0, data.len() as u64));
    bind(&mut job);
    feed(&mut job, first, &wrong, Some(ByteRange::new(0, ARTICLE)));
    assert_eq!(
        verdicts(&job, first)[..2],
        [SliceVerdict::Damaged, SliceVerdict::Damaged]
    );
    assert_eq!(
        job.object.outcome(),
        Some(RepairOutcome::Impossible {
            damaged: 2,
            reachable: 1
        })
    );
}

// A loss inside a recovery volume still open costs at most the packets it
// touches: the set is not ruled short on the most the loss could cost, and
// once the volume closes its walk says what it held.
#[test]
fn a_loss_in_an_open_volume_rules_nothing_short_before_it_closes() {
    let data = payload(43, 5000);
    let files = par2_set("set", &[("payload.bin", &data)], BLOCK, 8, 1);
    assert_eq!(files.len(), 2);
    let bytes = &files[1].1;
    let lost = ByteRange::at(2 * ARTICLE, ARTICLE);
    let hit = packets_of(bytes)
        .into_iter()
        .filter(|(packet, recovery)| *recovery && packet.intersect(lost).is_some())
        .count() as u64;
    assert!(hit > 0, "the loss lies among the recovery packets");
    let damaged = 8 - hit;
    let mut job = job(&files[..1]);
    let mut short = data.clone();
    short[..(damaged * BLOCK) as usize].fill(0);
    let member = job
        .namespace
        .announce("payload.bin", Some(data.len() as u64));
    job.store.members.insert(member, short);
    Recovery::refused(&mut job.object, member, ByteRange::new(0, damaged * BLOCK));
    let volume = named_volume(&mut job, &files[1]);
    bind(&mut job);
    let mut out = Vec::new();
    redeliver(&mut job.object, volume, bytes, ByteRange::new(0, ARTICLE));
    job.object.volume_lost(volume, lost, &mut out);
    assert_eq!(job.object.outcome(), None, "the loss costs {hit} blocks");
    assert_eq!(
        job.object
            .repair(&mut job.store, weaver_graph::Budget::default()),
        RepairOutcome::Waiting
    );
    redeliver(
        &mut job.object,
        volume,
        bytes,
        ByteRange::new(ARTICLE, lost.start),
    );
    redeliver(
        &mut job.object,
        volume,
        bytes,
        ByteRange::new(lost.end, bytes.len() as u64),
    );
    job.object.volume_closed(volume, &mut out);
    assert_eq!(
        job.object
            .repair(&mut job.store, weaver_graph::Budget::default()),
        RepairOutcome::Repaired { slices: damaged }
    );
    assert!(job.store.members[&member] == data);
}

// Not every choice of as many blocks as slices are damaged rebuilds them.
// The constants of slices 0 and 8 have equal 4369th powers, so blocks 0
// and 4369 cannot tell those two slices apart. The repair reads another
// block, and rebuilds both.
#[test]
fn a_repair_reads_another_block_when_those_read_do_not_rebuild() {
    let constants = par2_rs::input_slice_constants(9);
    assert_eq!(
        par2_rs::gf_pow(constants[0], 4369),
        par2_rs::gf_pow(constants[8], 4369)
    );
    let data = payload(44, 9 * BLOCK as usize);
    let described = [("payload.bin", data.as_slice())];
    let low = par2_set_from("set", &described, BLOCK, 0, 1, 1);
    let high = par2_set_from("set", &described, BLOCK, 4369, 2, 1);
    let files = [low[0].clone(), low[1].clone(), high[1].clone()];
    let mut wrong = data.clone();
    for slice in [0, 8] {
        wrong[(slice * BLOCK) as usize] ^= 0x5A;
    }
    let mut job = job(&files);
    let member = job.member("payload.bin", &wrong);
    for slice in [0, 8] {
        let at = slice * BLOCK / ARTICLE * ARTICLE;
        let article = ByteRange::new(at, (at + ARTICLE).min(data.len() as u64));
        job.arrive(
            member,
            article.start,
            &wrong[article.start as usize..article.end as usize],
            Provenance::Damaged,
        );
    }
    bind(&mut job);
    assert_eq!(
        job.object
            .repair(&mut job.store, weaver_graph::Budget::default()),
        RepairOutcome::Repaired { slices: 2 }
    );
    assert!(job.store.members[&member] == data);
}

// A file refused whole beside another the set describes with the same
// bytes: every slice of it is held, so it needs no recovery block, and the
// repair copies them from the file that holds them.
#[test]
fn a_file_refused_whole_is_copied_from_the_file_holding_its_slices() {
    let data = payload(41, 5000);
    let files = par2_set(
        "set",
        &[("first.bin", &data), ("again.bin", &data)],
        BLOCK,
        1,
        1,
    );
    let mut job = job(&files);
    let first = job.member("first.bin", &data);
    let again = job.namespace.announce("again.bin", Some(data.len() as u64));
    job.store.members.insert(again, vec![0; data.len()]);
    Recovery::refused(&mut job.object, again, ByteRange::new(0, data.len() as u64));
    bind(&mut job);
    assert!(job.object.covers(again));
    feed(&mut job, first, &data, None);
    assert_eq!(job.object.outcome(), None, "nothing it lacks is lost");
    assert_eq!(
        job.object
            .repair(&mut job.store, weaver_graph::Budget::default()),
        RepairOutcome::Repaired { slices: 10 }
    );
    assert!(job.store.members[&again] == data);
    assert!(job.store.members[&first] == data);
}
