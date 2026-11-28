// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A binding weighs only what changed since the last one: the postings
// marked since, the files whose claimants changed, the unnamed members that
// came or went, and the members those touch. What it comes to is what
// binding everything again does, step by step, and what it costs grows
// with what changed, not with the job.

use std::collections::{BTreeMap, BTreeSet, HashMap};

use par2_rs::{FileDescription, FileId, Par2FileSet, RecoverySetId};

use super::super::bind::Claimed;
use super::cross_sets::{Twins, decided, kept};
use super::*;

// What a set binds at each file, and how many postings, set-member entries
// and whole passes binding has cost.
fn cost(job: &Job<Par2>) -> (usize, usize, usize) {
    let claims = &job.object.claims;
    (claims.visits, job.object.entries(), claims.whole_passes)
}

// A bind after one member changed matches only that member against the
// sets describing a file like it, and binds again only a set it changes.
#[test]
fn a_bind_after_one_member_changes_checks_only_its_candidate_sets() {
    let posted: Vec<(String, Vec<u8>)> = (0..3)
        .map(|at| (format!("part{at}.bin"), payload(300 + at, 1200)))
        .collect();
    let described: Vec<(&str, &[u8])> = posted
        .iter()
        .map(|(name, bytes)| (name.as_str(), bytes.as_slice()))
        .collect();
    let mut files = par2_set("real", &described, BLOCK, 1, 1);
    let others: Vec<Vec<u8>> = (0..7).map(|other| payload(400 + other, 700)).collect();
    for (at, bytes) in others.iter().enumerate() {
        let name = format!("other{at}.bin");
        files.extend(par2_set(
            &format!("other{at}"),
            &[(&name, bytes)],
            BLOCK,
            1,
            1,
        ));
    }
    let mut job = job(&files);
    let parts: Vec<MemberId> = posted
        .iter()
        .map(|(name, bytes)| job.member(name, bytes))
        .collect();
    bind(&mut job);
    bind(&mut job);
    let counted = |job: &Job<Par2>| (job.object.set_checks, job.object.set_binds);

    // A part marked as changed, its bytes the same: matched against its set
    // alone, which binds what it did and so is not bound again.
    let before = counted(&job);
    job.namespace.get_mut(parts[0]).unwrap();
    bind(&mut job);
    let after = counted(&job);
    assert_eq!((after.0 - before.0, after.1 - before.1), (1, 0));

    // A member of another set's file: matched against that set alone, which
    // comes to bind it.
    let before = counted(&job);
    let other = job.member("other3.bin", &others[3]);
    bind(&mut job);
    let after = counted(&job);
    assert_eq!((after.0 - before.0, after.1 - before.1), (1, 1));
    assert!(job.object.covers(other));
}

// `count` members of one set, each with a front, then more bytes, then the
// rest, a bind after each: the postings and set-member entries binding
// visits past its whole passes. Two sets describing overlapping files, a
// second posting of every member, or an unnamed member fitting a free file
// change it by a constant per member.
fn linear(count: usize, overlapping: bool, copies: bool, unnamed: bool) -> usize {
    let bytes: Vec<Vec<u8>> = (0..count)
        .map(|at| payload(1000 + at as u64, 1500))
        .collect();
    let names: Vec<String> = (0..count).map(|at| format!("f{at}.bin")).collect();
    let spare = payload(999, 1500);
    let mut described: Vec<(&str, &[u8])> = names
        .iter()
        .zip(&bytes)
        .map(|(name, bytes)| (name.as_str(), bytes.as_slice()))
        .collect();
    if unnamed {
        described.push(("spare.bin", &spare));
    }
    let mut files = par2_set("one", &described, BLOCK, 2, 1);
    if overlapping {
        files.extend(par2_set(
            "two",
            &described[count / 4..count * 3 / 4],
            BLOCK,
            2,
            1,
        ));
    }
    let mut job = job(&files);
    bind(&mut job);
    if unnamed {
        job.member("junk.dat", &payload(998, 1500));
        bind(&mut job);
        assert!(
            job.object
                .sets
                .values()
                .any(|state| !state.unnamed().is_empty())
        );
    }
    let (visits, entries, wholes) = cost(&job);
    for (name, bytes) in names.iter().zip(&bytes) {
        let member = job.namespace.announce(name, Some(bytes.len() as u64));
        job.arrive(member, 0, &bytes[..700], Provenance::Fresh);
        if copies {
            let copy = job.namespace.announce(name, Some(bytes.len() as u64));
            job.arrive(copy, 0, &bytes[..700], Provenance::Fresh);
            assert!(job.namespace.fold(copy, member).is_some());
        }
        bind(&mut job);
        job.arrive(member, 700, &bytes[700..1400], Provenance::Fresh);
        bind(&mut job);
        job.arrive(member, 1400, &bytes[1400..], Provenance::Fresh);
        bind(&mut job);
        assert!(job.object.covers(member), "{name}");
    }
    let (now_visits, now_entries, now_wholes) = cost(&job);
    (now_visits - visits) + (now_entries - entries) - (now_wholes - wholes) * count
}

// What one member costs a binding, at most, over its three binds: 11 for
// one posting, 15 with a second folded in.
const PER_MEMBER: usize = 16;

fn grows_linearly(overlapping: bool, copies: bool, unnamed: bool) {
    let small = linear(200, overlapping, copies, unnamed);
    let large = linear(400, overlapping, copies, unnamed);
    assert!(small <= PER_MEMBER * 200, "200 members: {small}");
    assert!(large <= PER_MEMBER * 400, "400 members: {large}");
    assert!(large * 10 <= small * 22, "{small} then {large}");
}

#[test]
fn binding_costs_grow_with_the_members_that_change() {
    grows_linearly(false, false, false);
}

#[test]
fn binding_costs_grow_linearly_over_two_overlapping_sets() {
    grows_linearly(true, false, false);
}

#[test]
fn binding_costs_grow_linearly_with_further_postings() {
    grows_linearly(false, true, false);
}

#[test]
fn binding_costs_grow_linearly_beside_an_unnamed_member() {
    grows_linearly(false, false, true);
}

// `count` described files, all but the last posted, and a bind: the bind
// reports each posted member once, in member order, by its name, and keeps
// the start of `count - 1` members, one per member it came to bind. A bind
// that moves nothing reports nothing, so no report outlives its bind. The
// last file posted under an obfuscated name: reported alone, by the name it
// had before the bind named it, at the cost of one member kept.
fn reports(count: usize) {
    let bytes: Vec<Vec<u8>> = (0..count)
        .map(|at| payload(1100 + at as u64, 1500))
        .collect();
    let names: Vec<String> = (0..count).map(|at| format!("f{at}.bin")).collect();
    let described: Vec<(&str, &[u8])> = names
        .iter()
        .zip(&bytes)
        .map(|(name, bytes)| (name.as_str(), bytes.as_slice()))
        .collect();
    let mut job = job(&par2_set("one", &described, BLOCK, 2, 1));
    bind(&mut job);
    let mut taken = Vec::new();
    job.object.take_newly(&mut taken);
    assert_eq!(taken, []);
    let posted: Vec<(MemberId, String)> = names[..count - 1]
        .iter()
        .zip(&bytes)
        .map(|(name, bytes)| (job.member(name, bytes), name.clone()))
        .collect();
    bind(&mut job);
    job.object.take_newly(&mut taken);
    assert_eq!(taken, posted);
    assert_eq!(job.object.bind_moves, count - 1);
    taken.clear();
    bind(&mut job);
    job.object.take_newly(&mut taken);
    assert_eq!(taken, []);
    assert_eq!(job.object.bind_moves, 0);
    let last = job.member("obf.dat", &bytes[count - 1]);
    bind(&mut job);
    job.object.take_newly(&mut taken);
    assert_eq!(taken, [(last, "obf.dat".to_owned())]);
    assert_eq!(job.namespace.get(last).unwrap().name, names[count - 1]);
    assert_eq!(job.object.bind_moves, 1);
}

#[test]
fn a_bind_reports_what_it_came_to_cover_at_a_cost_of_the_members_it_moved() {
    reports(41);
    reports(401);
}

// `count` described files, `posted` of them posted one by one with too
// little of their bytes for a front, a bind after each: each binds by its
// name, and the descriptions those binds compared a name with.
fn name_probes(count: usize, posted: usize) -> usize {
    let bytes: Vec<Vec<u8>> = (0..count)
        .map(|at| payload(1200 + at as u64, 1500))
        .collect();
    let names: Vec<String> = (0..count).map(|at| format!("f{at}.bin")).collect();
    let described: Vec<(&str, &[u8])> = names
        .iter()
        .zip(&bytes)
        .map(|(name, bytes)| (name.as_str(), bytes.as_slice()))
        .collect();
    let mut job = job(&par2_set("one", &described, BLOCK, 2, 1));
    bind(&mut job);
    super::super::NAME_PROBES.with(|probes| probes.set(0));
    for (name, bytes) in names[..posted].iter().zip(&bytes) {
        let member = job.namespace.announce(name, Some(bytes.len() as u64));
        job.arrive(member, 0, &bytes[..700], Provenance::Fresh);
        bind(&mut job);
        assert!(job.object.covers(member), "{name}");
    }
    super::super::NAME_PROBES.with(std::cell::Cell::get)
}

// Binding a member by its name compares it with the one description of
// that name, whatever else the set describes: forty members posted cost
// forty comparisons beside forty described files or four hundred, where
// comparing with every description cost forty times the described files.
#[test]
fn binding_by_name_compares_only_the_descriptions_of_that_name() {
    assert_eq!(name_probes(40, 40), 40);
    assert_eq!(name_probes(400, 40), 40);
}

// Two members posted under one described name, neither with a front: both
// claim the file by name and neither binds. One's front comes and proves it
// by content: it binds. It is retracted and posted again under another
// name, claiming nothing: the other binds by its name. Retracted alone, it
// still claims the file by the name it keeps.
#[test]
fn a_second_claimant_by_name_binds_neither_until_one_proves_or_leaves() {
    let a = payload(701, 2100);
    let other = payload(702, 2100);
    let mut twins = Twins::new(&par2_set("alpha", &[("a.bin", &a)], BLOCK, 4, 1));
    twins.bind("no members");
    twins.each(|job| {
        let member = job.namespace.announce("a.bin", Some(a.len() as u64));
        job.arrive(member, 700, &a[700..1400], Provenance::Fresh);
        job.store.members.insert(member, a.clone());
        Some(member)
    });
    twins.each(|job| {
        let member = job.namespace.announce("a.bin", Some(other.len() as u64));
        job.arrive(member, 700, &other[700..1400], Provenance::Fresh);
        job.store.members.insert(member, other.clone());
        Some(member)
    });
    twins.bind("two by name");
    let [real, second] = twins.members[..] else {
        panic!("members");
    };
    assert!(!twins.jobs[0].object.covers(real));
    assert!(!twins.jobs[0].object.covers(second));
    let claimed = twins.jobs[0].object.claims.claimants();
    assert!(
        claimed
            .values()
            .any(|claimants| claimants == &[(real, false), (second, false)])
    );
    twins.each(|job| {
        job.arrive(real, 0, &a[..700], Provenance::Fresh);
        job.arrive(real, 1400, &a[1400..], Provenance::Fresh);
        None
    });
    twins.bind("one proves by content");
    assert!(twins.jobs[0].object.covers(real));
    assert!(!twins.jobs[0].object.covers(second));
    twins.each(|job| {
        job.namespace.retract(real);
        None
    });
    twins.bind("the prover retracted");
    assert!(!twins.jobs[0].object.covers(real));
    assert!(!twins.jobs[0].object.covers(second));
    twins.each(|job| {
        job.namespace.rename(real, "gone.dat");
        None
    });
    twins.bind("the prover leaves");
    assert!(!twins.jobs[0].object.covers(real));
    assert!(twins.jobs[0].object.covers(second));
}

// Two sets describing one file alike, the first with too few blocks for
// the member's damage: its repair rules it short, it gives the member up,
// and the member falls to the second, which repairs it. Binding keeps it
// there.
#[test]
fn a_member_given_up_falls_to_the_next_set_and_stays() {
    let bytes = payload(711, 20_000);
    let mut files = par2_set("alpha", &[("f.bin", &bytes)], BLOCK, 1, 1);
    files.extend(par2_set("beta", &[("f.bin", &bytes)], BLOCK * 2, 6, 1));
    let mut twins = Twins::new(&files);
    // Its front, the first 16 KiB, clean: it claims both sets by content.
    // Two articles past the front damaged. A member claiming by name alone
    // stays with the first set that names it, given up or not.
    twins.each(|job| {
        let member = job.member("f.bin", &bytes);
        let mut store = bytes.clone();
        for article in [24, 25] {
            let range = ByteRange::at(article * ARTICLE, ARTICLE);
            store[range.start as usize] ^= 0x5A;
            job.arrive(
                member,
                range.start,
                &store[range.start as usize..range.end as usize],
                Provenance::Damaged,
            );
        }
        job.store.members.insert(member, store);
        Some(member)
    });
    twins.bind("bound to the first");
    let member = twins.members[0];
    let [alpha, beta] = twins.jobs[0].object.ordered()[..] else {
        panic!("sets");
    };
    assert_eq!(twins.jobs[0].object.covered.sets(member), [alpha]);
    twins.repair("the first gives it up");
    assert!(twins.jobs[0].object.given_up.contains(&(alpha, member)));
    assert_eq!(twins.jobs[0].object.covered.sets(member), [beta]);
    twins.bind("bound again");
    assert_eq!(twins.jobs[0].object.covered.sets(member), [beta]);
}

// A member whose front came damaged, under an obfuscated name: once whole
// it is unnamed, held by the set with a file it may be, until the repair
// knows it by its slices and the set takes it.
#[test]
fn an_unnamed_member_taken_by_its_holding_set_stays_bound() {
    let bytes = payload(721, 2200);
    let mut twins = Twins::new(&par2_set("alpha", &[("c.bin", &bytes)], BLOCK, 6, 1));
    twins.each(|job| Some(damaged_member(job, "q9.dat", &bytes, 100).0));
    twins.bind("unnamed");
    let member = twins.members[0];
    let key = twins.jobs[0].object.ordered()[0];
    let state = &twins.jobs[0].object.sets[&key];
    assert!(state.unnamed().iter().any(|(held, _)| *held == member));
    assert!(state.members().is_empty());
    twins.repair("known by its slices");
    assert!(twins.jobs[0].object.covers(member));
    twins.bind("bound again");
    assert!(twins.jobs[0].object.covers(member));
    twins.restore();
    twins.bind("restored");
    assert!(twins.jobs[0].object.covers(member));
}

// A set describing two files under one name, of the lengths given. A PAR2
// set made by par2-rs refuses two files of one name, and two names apart
// only in case answer a posting under either name exactly; one written by
// another tool can describe them so, as the index here does.
fn same_named(job: &mut Job<Par2>, name: &str, lengths: [u64; 2]) {
    let files: HashMap<FileId, FileDescription> = lengths
        .iter()
        .enumerate()
        .map(|(at, length)| {
            let id = 0xE1 + at as u8;
            let file_id = FileId::from_bytes([id; 16]);
            let description = FileDescription {
                file_id,
                hash_full: [id; 16],
                hash_16k: [id; 16],
                length: *length,
                par2_name: name.to_owned(),
                filename: name.to_owned(),
            };
            (file_id, description)
        })
        .collect();
    let index = Par2FileSet {
        recovery_set_id: RecoverySetId::from_bytes([0xE0; 16]),
        slice_size: BLOCK,
        recovery_file_ids: files.keys().copied().collect(),
        non_recovery_file_ids: Vec::new(),
        files,
        slice_checksums: HashMap::new(),
        recovery_slices: BTreeMap::new(),
        creator: None,
        unicode_names: Default::default(),
    };
    job.object
        .file_set([0xE0; 16], super::super::SetState::indexed(index));
}

// Two files a set describes under one name, apart in length, and a member
// posted under that name for each: the length each has reached rules out
// the other file, and each binds the one it leaves.
#[test]
fn a_pair_under_one_name_is_bound_by_length() {
    let short = payload(731, 1000);
    let long = payload(732, 1600);
    let mut twins = Twins::new(&[]);
    for job in &mut twins.jobs {
        same_named(job, "pair.bin", [1000, 1600]);
    }
    twins.bind("the set");
    // The long file, past the short one's end.
    twins.each(|job| {
        let member = job.namespace.announce("pair.bin", Some(long.len() as u64));
        job.arrive(member, 1400, &long[1400..], Provenance::Fresh);
        Some(member)
    });
    twins.bind("one past the other's length");
    // The short file, all of it: it cannot be the long one.
    twins.each(|job| {
        let member = job.namespace.announce("pair.bin", Some(short.len() as u64));
        job.arrive(member, 0, &short, Provenance::Fresh);
        Some(member)
    });
    twins.bind("both");
    let [long_member, short_member] = twins.members[..] else {
        panic!("members");
    };
    let object = &twins.jobs[0].object;
    let length_of = |member| {
        let [key] = object.covered.sets(member) else {
            panic!("{member:?} bound once");
        };
        let state = &object.sets[key];
        let file = state.bound(member).unwrap();
        state
            .usable()
            .unwrap()
            .file_description(&file)
            .unwrap()
            .length
    };
    assert_eq!(length_of(long_member), 1600);
    assert_eq!(length_of(short_member), 1000);
}

// A member posted under a name of its own, with a further posting under
// the name its set describes folded in: it answers to the description
// through that posting, and keeps its name. The further posting is removed
// alone: the member answers to it no more, and takes the described name.
#[test]
fn a_member_whose_answering_posting_is_removed_takes_the_described_name() {
    let a = payload(751, 2100);
    let mut twins = Twins::new(&par2_set("alpha", &[("a.bin", &a)], BLOCK, 4, 1));
    twins.each(|job| Some(job.member("obf.dat", &a)));
    let member = twins.members[0];
    twins.each(|job| {
        let copy = job.namespace.announce("a.bin", Some(a.len() as u64));
        job.arrive(copy, 0, &a[..ARTICLE as usize], Provenance::Fresh);
        assert!(job.namespace.fold(copy, member).is_some());
        Some(copy)
    });
    twins.bind("a posting answers");
    assert_eq!(twins.jobs[0].namespace.get(member).unwrap().name, "obf.dat");
    let copy = twins.members[1];
    twins.each(|job| {
        job.namespace.remove(copy);
        None
    });
    twins.bind("the answering posting removed");
    assert_eq!(twins.jobs[0].namespace.get(member).unwrap().name, "a.bin");
}

// The same member, its answering posting taken over by another member: the
// member it left answers to it no more and takes the described name, and
// the member it joined is named as a whole binding names it.
#[test]
fn a_member_whose_answering_posting_moves_to_another_takes_the_described_name() {
    let a = payload(761, 2100);
    let b = payload(762, 1700);
    let files = par2_set("alpha", &[("a.bin", &a), ("b.bin", &b)], BLOCK, 4, 1);
    let mut twins = Twins::new(&files);
    twins.each(|job| Some(job.member("obf.dat", &a)));
    twins.each(|job| Some(job.member("obn.dat", &b)));
    let (left, joined) = (twins.members[0], twins.members[1]);
    twins.each(|job| {
        let copy = job.namespace.announce("a.bin", Some(a.len() as u64));
        job.arrive(copy, 0, &a[..ARTICLE as usize], Provenance::Fresh);
        assert!(job.namespace.fold(copy, left).is_some());
        Some(copy)
    });
    twins.bind("a posting answers");
    assert_eq!(twins.jobs[0].namespace.get(left).unwrap().name, "obf.dat");
    let copy = twins.members[2];
    twins.each(|job| {
        assert!(job.namespace.restore_copy(copy, joined));
        None
    });
    twins.bind("the answering posting moved");
    assert_eq!(twins.jobs[0].namespace.get(left).unwrap().name, "a.bin");
    assert_eq!(twins.jobs[0].namespace.resolve(copy), joined);
}

// A job bound live and the same job restored from its checkpoint and bound
// once decide and keep the same.
#[test]
fn a_restored_job_binds_what_the_live_one_holds() {
    let a = payload(741, 3000);
    let b = payload(742, 1800);
    let unmatched = payload(743, 1800);
    let mut files = par2_set("alpha", &[("a.bin", &a), ("b.bin", &b)], BLOCK, 4, 1);
    files.extend(par2_set("beta", &[("b.bin", &b)], BLOCK, 2, 1));
    let mut jobs = [job(&files), job(&files)];
    let mut members = Vec::new();
    for job in &mut jobs {
        let found = vec![
            job.member("posted-a.dat", &a),
            job.member("b.bin", &b),
            job.member("sample.dat", &unmatched),
            damaged_member(job, "x.dat", &a, 900).0,
        ];
        bind(job);
        members = found;
    }
    let facts = jobs[1].object.facts();
    jobs[1].object = crate::par2::facts::par2(&facts).unwrap();
    for job in &mut jobs {
        bind(job);
    }
    let restorable = |job: &Job<Par2>| {
        // Not kept: the members still to be said again, which a restore
        // says again whole.
        let lines: Vec<String> = kept(job).lines().map(str::to_owned).collect();
        let at = job.object.ordered().len();
        lines
            .iter()
            .enumerate()
            .filter(|(line, _)| *line != at + 1)
            .map(|(_, text)| text.clone())
            .collect::<Vec<_>>()
    };
    // The rulings name each set by its volume, whose name a restore learns
    // again from the namespace, not from the checkpoint.
    let bound = |job: &Job<Par2>| {
        let decided = decided(job, &members);
        let mut lines: Vec<&str> = decided.lines().collect();
        lines.pop();
        lines.join("\n")
    };
    assert_eq!(bound(&jobs[0]), bound(&jobs[1]));
    assert_eq!(restorable(&jobs[0]), restorable(&jobs[1]));
    let mut renamed = Vec::new();
    jobs[1].object.bind(&mut jobs[1].namespace, &mut renamed);
    assert!(renamed.is_empty());
}

// xorshift64*: the seeded steps below.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 >> 12;
        self.0 ^= self.0 << 25;
        self.0 ^= self.0 >> 27;
        self.0.wrapping_mul(0x2545_F491_4F6C_DD1D)
    }

    fn below(&mut self, count: usize) -> usize {
        (self.next() % count as u64) as usize
    }
}

// One posting of a described file, under one name, with what is left of it
// to arrive, by article, and the articles that arrive damaged.
struct Posting {
    member: MemberId,
    file: usize,
    pending: Vec<usize>,
    damaged: BTreeSet<usize>,
    copy: bool,
}

// What the seeds reached of the cases one member's binding turns on
// another's.
#[derive(Debug, Default)]
struct Reached {
    second_by_name: usize,
    content_over_name: usize,
    claimant_left: usize,
    given_up_fallback: usize,
    unnamed_taken: usize,
    paired_by_length: usize,
    verified_changed: usize,
    name_freed: usize,
    copy_removed: usize,
}

fn verified_sets(object: &Par2) -> BTreeMap<[u8; 16], bool> {
    object
        .binding
        .iter()
        .map(|key| (*key, super::super::verifies(&object.sets[key])))
        .collect()
}

fn reach(
    job: &Job<Par2>,
    before: &Claimed,
    verified: &BTreeMap<[u8; 16], bool>,
    reached: &mut Reached,
) {
    let object = &job.object;
    let claimed = object.claims.claimants();
    for claimants in claimed.values() {
        let named = claimants.iter().filter(|(_, content)| !content).count();
        if named >= 2 {
            reached.second_by_name += 1;
        }
        if named >= 1 && claimants.iter().any(|(_, content)| *content) {
            reached.content_over_name += 1;
        }
    }
    for (place, was) in before {
        let now = claimed.get(place);
        if was.len() >= 2
            && was
                .iter()
                .any(|(member, _)| now.is_none_or(|now| !now.iter().any(|(m, _)| m == member)))
        {
            reached.claimant_left += 1;
        }
    }
    for (key, member) in &object.given_up {
        if object
            .covered
            .sets(*member)
            .iter()
            .any(|other| other != key)
        {
            reached.given_up_fallback += 1;
        }
    }
    for (key, state) in &object.sets {
        let Some(index) = state.usable() else {
            continue;
        };
        for (file, member) in state.members() {
            let Some(record) = job.namespace.get(*member) else {
                continue;
            };
            let by_name = claimed.get(&(*key, *file)).is_some_and(|claimants| {
                claimants.iter().any(|(m, content)| m == member && !content)
            });
            let own = index.file_description(file).unwrap();
            // Another file of the set is described under the same name, and
            // the member's length ruled it out.
            let paired = index
                .files
                .values()
                .any(|other| other.file_id != own.file_id && other.filename == own.filename);
            if by_name && paired && own.filename.eq_ignore_ascii_case(&record.name) {
                reached.paired_by_length += 1;
            }
        }
    }
    for (key, now) in verified_sets(object) {
        if verified.get(&key).is_some_and(|was| *was != now) {
            reached.verified_changed += 1;
        }
    }
}

// Random steps over two sets describing overlapping files, and on odd
// seeds a third describing two files under one name, the twins bound after
// each: members announced under described, swapped, case-varied and
// obfuscated names; their articles arriving in any order, some damaged;
// further postings folded in; the sets' volumes arriving in any order,
// forwards or backwards; members retracted and renamed; volumes lost and
// ranges refused; repairs; and checkpoint round trips. The incremental
// binding and the whole one keep the same after every bind. What the seeds
// reached of the cases one member's binding turns on another's.
fn seeded(whole: bool) -> Reached {
    const SEEDS: u64 = 48;
    const STEPS: usize = 64;
    let contents: Vec<(&str, Vec<u8>)> = vec![
        ("a.bin", payload(801, 3000)),
        ("b.bin", payload(802, 1800)),
        ("shared.bin", payload(803, 2500)),
        ("c.bin", payload(804, 2200)),
        ("pair.bin", payload(805, 1000)),
        ("PAIR.BIN", payload(806, 1600)),
        ("twin.bin", payload(807, 1000)),
        ("twin.bin", payload(808, 1600)),
    ];
    let of = |at: usize| (contents[at].0, contents[at].1.as_slice());
    let mut volumes = par2_set("alpha", &[of(0), of(1), of(2), of(4)], BLOCK, 2, 1);
    volumes.extend(par2_set("beta", &[of(2), of(1), of(3), of(5)], BLOCK, 8, 2));
    let mut reached = Reached::default();
    for seed in 0..SEEDS {
        let mut rng = Rng(0x9E37_79B9_7F4A_7C15 ^ (seed + 1).wrapping_mul(0xD1B5_4A32_D192_ED03));
        let mut twins = Twins::of(&[], whole);
        // A set with no volume is not restored: the odd seeds bind with it
        // and do not restore, the even ones restore without it.
        let paired = seed % 2 == 1;
        if paired {
            for job in &mut twins.jobs {
                same_named(job, "twin.bin", [1000, 1600]);
            }
        }
        let mut undelivered: Vec<usize> = (0..volumes.len()).collect();
        let mut delivered: Vec<(VolumeIndex, u64)> = Vec::new();
        let mut postings: Vec<Posting> = Vec::new();
        for step in 0..STEPS {
            let label = format!("seed {seed} step {step}");
            let before = twins.jobs[0].object.claims.claimants();
            let verified = verified_sets(&twins.jobs[0].object);
            match rng.below(14) {
                0 | 1 if postings.len() < 10 => {
                    let file = rng.below(contents.len());
                    let (described, bytes) = &contents[file];
                    let name = match rng.below(5) {
                        0 | 1 => (*described).to_owned(),
                        2 => contents[rng.below(contents.len())].0.to_owned(),
                        3 => described.to_ascii_uppercase(),
                        _ => format!("x{step}.dat"),
                    };
                    let articles = bytes.len().div_ceil(ARTICLE as usize);
                    let damaged: BTreeSet<usize> =
                        (0..articles).filter(|_| rng.below(7) == 0).collect();
                    let mut store = bytes.clone();
                    for article in &damaged {
                        store[article * ARTICLE as usize] ^= 0x5A;
                    }
                    let length = bytes.len() as u64;
                    twins.each(|job| {
                        let member = job.namespace.announce(&name, Some(length));
                        job.store.members.insert(member, store.clone());
                        Some(member)
                    });
                    postings.push(Posting {
                        member: *twins.members.last().unwrap(),
                        file,
                        pending: (0..articles).collect(),
                        damaged,
                        copy: false,
                    });
                }
                0..=4 => {
                    let waiting: Vec<usize> = (0..postings.len())
                        .filter(|at| !postings[*at].pending.is_empty())
                        .collect();
                    if waiting.is_empty() {
                        continue;
                    }
                    let posting = &mut postings[waiting[rng.below(waiting.len())]];
                    let article = posting.pending.remove(rng.below(posting.pending.len()));
                    let bytes = &contents[posting.file].1;
                    let start = article * ARTICLE as usize;
                    let end = (start + ARTICLE as usize).min(bytes.len());
                    let mut piece = bytes[start..end].to_vec();
                    let provenance = if posting.damaged.contains(&article) {
                        piece[0] ^= 0x5A;
                        Provenance::Damaged
                    } else {
                        Provenance::Fresh
                    };
                    let member = posting.member;
                    twins.each(|job| {
                        job.arrive(member, start as u64, &piece, provenance);
                        None
                    });
                }
                5 => {
                    // A further posting of a member whose front came clean.
                    let fronted: Vec<usize> = (0..postings.len())
                        .filter(|at| {
                            let posting = &postings[*at];
                            !posting.copy
                                && !posting.pending.contains(&0)
                                && !posting.damaged.contains(&0)
                        })
                        .collect();
                    if fronted.is_empty() {
                        continue;
                    }
                    let into = &postings[fronted[rng.below(fronted.len())]];
                    let (member, file) = (into.member, into.file);
                    let bytes = &contents[file].1;
                    let name = twins.jobs[0].namespace.get(member).unwrap().name.clone();
                    twins.each(|job| {
                        let copy = job.namespace.announce(&name, Some(bytes.len() as u64));
                        job.arrive(copy, 0, &bytes[..ARTICLE as usize], Provenance::Fresh);
                        job.namespace.fold(copy, member);
                        Some(copy)
                    });
                    postings.push(Posting {
                        member: *twins.members.last().unwrap(),
                        file,
                        pending: Vec::new(),
                        damaged: BTreeSet::new(),
                        copy: true,
                    });
                }
                6 | 7 => {
                    if undelivered.is_empty() {
                        continue;
                    }
                    let at = undelivered.remove(rng.below(undelivered.len()));
                    let (name, bytes) = &volumes[at];
                    let chunk = [ARTICLE as usize, 333][rng.below(2)];
                    let reverse = rng.below(2) == 0;
                    let mut index = None;
                    twins.each(|job| {
                        index = Some(job.volume(name, bytes, chunk, reverse));
                        None
                    });
                    delivered.push((index.unwrap(), bytes.len() as u64));
                }
                8 => {
                    let Some(posting) = postings.get(rng.below(postings.len().max(1))) else {
                        continue;
                    };
                    let member = posting.member;
                    // The member holds a name a set describes another member
                    // as, and gives it up: the next binding sees it freed.
                    let held = &twins.jobs[0].namespace.get(member).unwrap().name;
                    let object = &twins.jobs[0].object;
                    let freed = object.sets.values().any(|state| {
                        state.usable().is_some_and(|index| {
                            state.members().iter().any(|(file, other)| {
                                *other != member
                                    && index.file_description(file).is_some_and(|description| {
                                        description.filename.eq_ignore_ascii_case(held)
                                    })
                            })
                        })
                    });
                    reached.name_freed += usize::from(freed);
                    if rng.below(2) == 0 {
                        twins.each(|job| {
                            job.namespace.retract(member);
                            None
                        });
                        let at = postings.iter().position(|p| p.member == member).unwrap();
                        let articles = contents[postings[at].file]
                            .1
                            .len()
                            .div_ceil(ARTICLE as usize);
                        postings[at].pending = (0..articles).collect();
                    } else {
                        let name = contents[rng.below(contents.len())].0;
                        twins.each(|job| {
                            job.namespace.rename(member, name);
                            None
                        });
                    }
                }
                9 => {
                    if delivered.is_empty() {
                        continue;
                    }
                    let (volume, length) = delivered[rng.below(delivered.len())];
                    let start = rng.below(length as usize) as u64;
                    twins.each(|job| {
                        job.object.volume_lost(
                            volume,
                            ByteRange::new(start, length),
                            &mut Vec::new(),
                        );
                        None
                    });
                }
                10 => {
                    let Some(posting) = postings.get(rng.below(postings.len().max(1))) else {
                        continue;
                    };
                    let member = posting.member;
                    let length = contents[posting.file].1.len() as u64;
                    let start = rng.below(length as usize) as u64;
                    let range = ByteRange::new(start, (start + ARTICLE).min(length));
                    twins.each(|job| {
                        Recovery::refused(&mut job.object, member, range);
                        None
                    });
                }
                11 => {
                    let unnamed: Vec<([u8; 16], MemberId)> = twins.jobs[0]
                        .object
                        .sets
                        .iter()
                        .flat_map(|(key, state)| {
                            state
                                .unnamed()
                                .iter()
                                .map(move |(member, _)| (*key, *member))
                        })
                        .collect();
                    twins.repair(&label);
                    let object = &twins.jobs[0].object;
                    reached.unnamed_taken += unnamed
                        .iter()
                        .filter(|(key, member)| object.sets[key].bound(*member).is_some())
                        .count();
                }
                12 => {
                    // A further posting removed alone: the member it was
                    // folded into answers to it no more.
                    let copies: Vec<usize> = (0..postings.len())
                        .filter(|at| postings[*at].copy)
                        .collect();
                    if copies.is_empty() {
                        continue;
                    }
                    let at = copies[rng.below(copies.len())];
                    let copy = postings.remove(at).member;
                    twins.each(|job| {
                        assert!(job.namespace.remove(copy).is_some());
                        None
                    });
                    reached.copy_removed += 1;
                }
                _ if paired => {}
                _ => twins.restore(),
            }
            twins.bind(&label);
            reach(&twins.jobs[0], &before, &verified, &mut reached);
        }
    }
    reached
}

// The incremental binding against the scan and the whole, over the seeded
// steps; and the seeds reach each case one member's binding turns on
// another's.
#[test]
fn seeded_steps_bind_what_binding_everything_does() {
    let reached = seeded(false);
    assert!(reached.second_by_name > 0, "{reached:?}");
    assert!(reached.content_over_name > 0, "{reached:?}");
    assert!(reached.claimant_left > 0, "{reached:?}");
    assert!(reached.given_up_fallback > 0, "{reached:?}");
    assert!(reached.unnamed_taken > 0, "{reached:?}");
    assert!(reached.paired_by_length > 0, "{reached:?}");
    assert!(reached.verified_changed > 0, "{reached:?}");
    assert!(reached.name_freed > 0, "{reached:?}");
    assert!(reached.copy_removed > 0, "{reached:?}");
}

// The incremental binding against a whole pass of today's binding, its
// claims made stale before each bind, over the same seeded steps: the same
// in all, and coverage changed as often after every step.
#[test]
fn seeded_steps_count_coverage_changes_as_a_whole_pass_does() {
    seeded(true);
}
