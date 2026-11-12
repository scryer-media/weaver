// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Binding, listing what is absent, telling which members a set covers and
// identifying members by content each look at every file and member a
// bounded number of times. Each is held to a copy of the scan it replaces:
// the same answers, in the same order, over many shapes, and work that no
// longer grows with the files times the members.

use std::cell::Cell;
use std::collections::{BTreeMap, BTreeSet};

use par3_rs::Fingerprint;
use par3_rs::layout::{BlockLayout, FileLayout};
use par3_rs::runtime::ExecutionOptions;
use weaver_graph::recovery::MemberAccess;
use weaver_graph::{ByteRange, Member, MemberId, Namespace, ObjectId, Provenance};

use super::super::identity::{self, Eligible, READ_LIMIT, eligible, fingerprint, holds_block_of};
use super::super::{ByName, Described, Fitting, Par3, Unnamed, absent, by_name, scan, sets};
use crate::testing::{Store, par3_set, payload};

// A small deterministic generator, so every shape is the same on every run.
struct Seeded(u64);

impl Seeded {
    fn below(&mut self, bound: u64) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        (self.0 >> 33) % bound.max(1)
    }
}

// What binding by name came to, every described path's length included.
type Bound = (
    BTreeMap<String, MemberId>,
    BTreeMap<String, bool>,
    BTreeMap<MemberId, Unnamed>,
    Vec<Option<u64>>,
);

// Binding by name as it was: each file scans every member by name, twice,
// each member scans every file, and each length every file; every name
// compared and every length fitted counted in `asked`.
fn by_name_rescanning<'m, I>(
    files: &[Described<'_>],
    others: impl Fn() -> I,
    asked: &Cell<u64>,
) -> Bound
where
    I: Iterator<Item = &'m Member>,
{
    let files = || files.iter();
    let answers = |name: &str, file: &Described<'_>| {
        asked.set(asked.get() + 1);
        name == file.path || name == file.name
    };
    let mut bound = BTreeMap::new();
    for file in files() {
        let mut matches = others().filter(|member| answers(&member.name, file));
        if let (Some(member), None) = (matches.next(), matches.next()) {
            bound.insert(file.path.to_owned(), member.id);
        }
    }
    let described: BTreeMap<String, bool> = files()
        .map(|file| {
            let carried = others().any(|member| answers(&member.name, file));
            (file.path.to_owned(), carried)
        })
        .collect();
    let fits_free = |unnamed: Unnamed| {
        files().any(|file| {
            asked.set(asked.get() + 1);
            unnamed.bound().fits(file.size) && described.get(file.path) == Some(&false)
        })
    };
    let unnamed: BTreeMap<MemberId, Unnamed> = others()
        .filter(|member| !files().any(|file| answers(&member.name, file)))
        .filter_map(|member| Some((member.id, Unnamed::of(member).filter(|u| fits_free(*u))?)))
        .collect();
    let lengths = files()
        .map(|described| {
            files()
                .find(|file| {
                    asked.set(asked.get() + 1);
                    file.path == described.path
                })
                .map(|file| file.size)
        })
        .collect();
    (bound, described, unnamed, lengths)
}

fn by_name_indexed<'m, I>(files: &[Described<'_>], others: impl Fn() -> I, work: &mut u64) -> Bound
where
    I: Iterator<Item = &'m Member>,
{
    let ByName {
        bound,
        described,
        unnamed,
        lengths,
    } = by_name(files, others, work);
    let lengths = files
        .iter()
        .map(|file| lengths.get(file.path).copied())
        .collect();
    (bound, described, unnamed, lengths)
}

// Described files: flat names, names under a directory whose bare name is
// shared, a path described by more than one set, lengths shared and not.
fn described_files(random: &mut Seeded, count: u64) -> Vec<(String, String, u64)> {
    let mut files = Vec::new();
    for at in 0..count {
        let name = format!("f{}.bin", random.below(count.max(2)));
        let path = match random.below(3) {
            0 => name.clone(),
            _ => format!("d{}/{name}", random.below(3)),
        };
        files.push((path, name, 1 + random.below(6) * 100 + at % 2));
        if random.below(5) == 0 {
            let again = files[random.below(files.len() as u64) as usize].clone();
            files.push(again);
        }
    }
    files
}

// Members named by a file's path or bare name, by a name no file answers
// to, or several by one name, of the files' lengths, near them or none,
// each whole and checked, partly arrived, or with no byte checked.
fn members(random: &mut Seeded, files: &[(String, String, u64)], count: u64) -> Namespace {
    let mut namespace = Namespace::new();
    for at in 0..count {
        let name = match (random.below(4), files.is_empty()) {
            (0, false) => files[random.below(files.len() as u64) as usize].0.clone(),
            (1, false) => files[random.below(files.len() as u64) as usize].1.clone(),
            _ => format!("{:016x}", random.below(count.max(2)) * 7919 + at % 3),
        };
        let length = match random.below(6) {
            0 => None,
            _ => Some(1 + random.below(7) * 100 + random.below(2)),
        };
        let member = namespace.announce(&name, length);
        let Some(length) = length else {
            continue;
        };
        let record = namespace.get_mut(member).expect("announced");
        match random.below(3) {
            0 => record
                .coverage
                .write(ByteRange::new(0, length), Provenance::Fresh),
            1 => record
                .coverage
                .write(ByteRange::new(0, length / 2), Provenance::Fresh),
            _ => record
                .coverage
                .write(ByteRange::new(0, length), Provenance::Damaged),
        }
    }
    namespace
}

fn as_described(files: &[(String, String, u64)]) -> Vec<Described<'_>> {
    files
        .iter()
        .map(|(path, name, size)| Described {
            path,
            name,
            size: *size,
        })
        .collect()
}

const OWN: ObjectId = ObjectId(11);

fn others<'n>(namespace: &'n Namespace) -> impl Fn() -> std::vec::IntoIter<&'n Member> + 'n {
    move || {
        namespace
            .members()
            .filter(|member| member.bound_to != Some(OWN))
            .collect::<Vec<_>>()
            .into_iter()
    }
}

// Binding by name binds the same paths to the same members, describes the
// same paths the same way, leaves the same members unnamed and finds the
// same lengths as the scans it replaces, across many shapes.
#[test]
fn par3_binding_by_name_answers_as_scanning_does() {
    let mut bound = 0;
    let mut unnamed = 0;
    for seed in 0..4_000 {
        let mut random = Seeded(seed);
        let count = random.below(12);
        let files = described_files(&mut random, count);
        let count = random.below(14);
        let namespace = members(&mut random, &files, count);
        let described = as_described(&files);
        let asked = Cell::new(0);
        let scanned = by_name_rescanning(&described, others(&namespace), &asked);
        let indexed = by_name_indexed(&described, others(&namespace), &mut 0);
        assert_eq!(indexed, scanned, "seed {seed} files {files:?}");
        bound += scanned.0.len();
        unnamed += scanned.2.len();
    }
    // The shapes bind members and leave some unnamed, so the comparison
    // covers both.
    assert!(
        bound > 1_000 && unnamed > 1_000,
        "{bound} bound, {unnamed} unnamed"
    );
}

// With as many members as files, the scans compare a number of names that
// grows with their product; the index looks at each file and member a
// bounded number of times.
#[test]
fn par3_binding_by_name_grows_with_the_files_and_members_not_their_product() {
    let case = |n: u64| {
        let files: Vec<(String, String, u64)> = (0..n)
            .map(|at| (format!("d/{at}.bin"), format!("{at}.bin"), 100 + at % 5))
            .collect();
        let mut namespace = Namespace::new();
        for at in 0..n {
            let name = match at % 3 {
                0 => format!("d/{at}.bin"),
                1 => format!("{at}.bin"),
                _ => format!("{at:016x}"),
            };
            let member = namespace.announce(&name, Some(100 + at % 7));
            let record = namespace.get_mut(member).expect("announced");
            record
                .coverage
                .write(ByteRange::new(0, 100 + at % 7), Provenance::Fresh);
        }
        let described = as_described(&files);
        let asked = Cell::new(0);
        let mut work = 0;
        let scanned = by_name_rescanning(&described, others(&namespace), &asked);
        let indexed = by_name_indexed(&described, others(&namespace), &mut work);
        assert_eq!(indexed, scanned);
        (asked.get(), work)
    };
    let (small_asked, small_work) = case(64);
    let (large_asked, large_work) = case(256);
    // Four times the files and members: the scans ask more than twelve
    // times as much, the index takes four times the looks.
    assert!(
        large_asked > 12 * small_asked,
        "{small_asked} -> {large_asked}"
    );
    assert!(large_work <= 4 * small_work, "{small_work} -> {large_work}");
    // Thousands of files and members none of whose names answer: each is
    // looked at no more than twice.
    let n = 4_096;
    let files: Vec<(String, String, u64)> = (0..n)
        .map(|at| (format!("{at}.bin"), format!("{at}.bin"), 100))
        .collect();
    let mut namespace = Namespace::new();
    for at in 0..n {
        namespace.announce(&format!("{at:016x}"), Some(100));
    }
    let mut work = 0;
    by_name_indexed(&as_described(&files), others(&namespace), &mut work);
    assert_eq!(work, 3 * n, "{work} looks for {n} files and members");
}

// Listing what is absent as it was: each file scans every unnamed member,
// and the list it adds to.
fn absent_rescanning<'f>(
    files: impl Iterator<Item = (&'f str, u64)>,
    (unnamed, identified): (&BTreeMap<MemberId, Unnamed>, &BTreeMap<MemberId, String>),
    missing: impl Fn(&str) -> bool,
    out: &mut Vec<(String, u64)>,
    asked: &mut u64,
) {
    for (path, size) in files {
        let maybe = unnamed.iter().any(|(member, unnamed)| {
            *asked += 1;
            unnamed.bound().fits(size) && identified.get(member).is_none_or(|was| was == path)
        });
        let named = (path.to_owned(), size);
        *asked += out.len() as u64;
        if missing(path) && !maybe && !out.contains(&named) {
            out.push(named);
        }
    }
}

type AbsentShape = (
    Vec<(String, u64)>,
    BTreeMap<MemberId, Unnamed>,
    BTreeMap<MemberId, String>,
    BTreeSet<String>,
    Vec<(String, u64)>,
);

// Files repeated across sets, members unnamed with exact lengths and
// bounds, some identified as a described file and some as none, files
// carried and not, and a list another object already added to.
fn absent_shape(seed: u64) -> AbsentShape {
    let mut random = Seeded(seed);
    let mut files = Vec::new();
    for at in 0..random.below(14) {
        files.push((
            format!("{}.bin", random.below(10)),
            100 + random.below(5) * 100 + at % 2,
        ));
    }
    let mut unnamed = BTreeMap::new();
    let mut identified = BTreeMap::new();
    for at in 0..random.below(8) {
        let member = MemberId(at as u32);
        unnamed.insert(
            member,
            Unnamed {
                length: 100 + random.below(6) * 100,
                intact: random.below(2) == 0,
                complete: random.below(3) != 0,
            },
        );
        match random.below(3) {
            0 if !files.is_empty() => {
                let (path, _) = &files[random.below(files.len() as u64) as usize];
                identified.insert(member, path.clone());
            }
            1 => {
                identified.insert(member, "elsewhere.bin".to_owned());
            }
            _ => {}
        }
    }
    let missing = (0..10)
        .filter(|_| random.below(4) != 0)
        .map(|at| format!("{at}.bin"))
        .collect();
    let out = match (random.below(3), files.is_empty()) {
        (0, false) => vec![files[0].clone(), ("other.bin".to_owned(), 7)],
        _ => Vec::new(),
    };
    (files, unnamed, identified, missing, out)
}

// The absent files are the same, in the same order, after the same list
// another object made, as the scans they replace list, across many shapes.
#[test]
fn par3_absent_files_are_listed_as_scanning_lists_them() {
    let mut listed = 0;
    for seed in 0..20_000 {
        let (files, unnamed, identified, missing, out) = absent_shape(seed);
        let files = || files.iter().map(|(path, size)| (path.as_str(), *size));
        let missing = |path: &str| missing.contains(path);
        let mut scanned = out.clone();
        absent_rescanning(
            files(),
            (&unnamed, &identified),
            missing,
            &mut scanned,
            &mut 0,
        );
        let mut indexed = out.clone();
        absent(
            files(),
            (&unnamed, &identified),
            missing,
            &mut indexed,
            &mut 0,
        );
        assert_eq!(indexed, scanned, "seed {seed}");
        listed += scanned.len() - out.len();
    }
    assert!(listed > 10_000, "{listed} listed");
}

// With as many unnamed members as files, none of them fitting, the scans
// ask a number of fits that grows with the product; the index looks at
// each once.
#[test]
fn par3_absent_files_grow_with_the_files_and_members_not_their_product() {
    let case = |n: u64| {
        let files: Vec<(String, u64)> =
            (0..n).map(|at| (format!("{at}.bin"), 1_000 + at)).collect();
        let unnamed: BTreeMap<MemberId, Unnamed> = (0..n)
            .map(|at| {
                let unnamed = Unnamed {
                    length: 10 + at,
                    intact: at % 2 == 0,
                    complete: true,
                };
                (MemberId(at as u32), unnamed)
            })
            .collect();
        let identified = BTreeMap::new();
        let files = || files.iter().map(|(path, size)| (path.as_str(), *size));
        let (mut asked, mut work) = (0, 0);
        let mut scanned = Vec::new();
        absent_rescanning(
            files(),
            (&unnamed, &identified),
            |_| true,
            &mut scanned,
            &mut asked,
        );
        let mut indexed = Vec::new();
        absent(
            files(),
            (&unnamed, &identified),
            |_| true,
            &mut indexed,
            &mut work,
        );
        assert_eq!(indexed, scanned);
        assert_eq!(indexed.len() as u64, n);
        (asked, work)
    };
    let (small_asked, small_work) = case(64);
    let (large_asked, large_work) = case(256);
    assert!(
        large_asked > 12 * small_asked,
        "{small_asked} -> {large_asked}"
    );
    assert_eq!(large_work, 4 * small_work);
    assert_eq!(small_work, 2 * 64);
}

// Whether a member is covered is what a scan of the bound paths says, for
// every member, bound or not, and across a restore of the facts.
#[test]
fn par3_covered_members_are_those_a_scan_of_the_bound_paths_finds() {
    for seed in 0..2_000 {
        let mut random = Seeded(seed);
        let mut bound = BTreeMap::new();
        for at in 0..random.below(12) {
            bound.insert(format!("{at}.bin"), MemberId(random.below(10) as u32));
        }
        let mut par3 = Par3::new(OWN);
        par3.described = bound.keys().map(|path| (path.clone(), true)).collect();
        par3.set_bound(bound.clone());
        let restored =
            super::super::facts::par3(&super::super::facts::of(&par3)).expect("restored");
        for id in 0..12 {
            let member = MemberId(id);
            let scanned = bound.values().any(|bound| *bound == member);
            assert_eq!(par3.covered.contains(&member), scanned, "seed {seed}");
            assert_eq!(restored.covered.contains(&member), scanned, "seed {seed}");
        }
    }
}

// Whether an unnamed member still arriving may be a file of some length is
// what a scan of those members says, for every length.
#[test]
fn par3_members_still_arriving_fit_as_a_scan_finds() {
    for seed in 0..5_000 {
        let mut random = Seeded(seed);
        let members: Vec<Unnamed> = (0..random.below(10))
            .map(|_| Unnamed {
                length: random.below(8) * 100,
                intact: random.below(2) == 0,
                complete: random.below(2) == 0,
            })
            .collect();
        let mut arriving = Fitting::default();
        for member in members.iter().filter(|member| !member.complete) {
            arriving.add(*member);
        }
        for long in 0..900 {
            let scanned = members
                .iter()
                .any(|member| !member.complete && member.bound().fits(long));
            assert_eq!(arriving.fits(long), scanned, "seed {seed} length {long}");
        }
    }
}

// Identifying a member as it was: every free file of its length compared
// by whole fingerprint, or every one tried for a block, each counted.
fn identify_rescanning<'a>(
    eligible: &[&'a FileLayout],
    fingerprints: &BTreeSet<Fingerprint>,
    access: &mut dyn MemberAccess,
    member: MemberId,
    len: u64,
    read: &mut u64,
    asked: &mut u64,
) -> Option<&'a str> {
    if eligible.is_empty() {
        return None;
    }
    let whole = fingerprint(access, member, ByteRange::new(0, len), read);
    let matched: Vec<&FileLayout> = match whole {
        Some(whole) if fingerprints.contains(&whole) => eligible
            .iter()
            .copied()
            .filter(|file| {
                *asked += 1;
                file.fingerprint == whole
            })
            .collect(),
        _ => {
            if eligible.iter().map(|file| file.len).sum::<u64>() > READ_LIMIT.saturating_sub(*read)
            {
                return None;
            }
            eligible
                .iter()
                .copied()
                .filter(|file| {
                    *asked += 1;
                    holds_block_of(access, member, file, read)
                })
                .collect()
        }
    };
    let file = matched.first()?;
    if matched.iter().any(|other| other.path != file.path) {
        return None;
    }
    Some(&file.path)
}

// The member store, each read it answers recorded.
struct Recording {
    store: Store,
    reads: Vec<(MemberId, ByteRange)>,
}

impl MemberAccess for Recording {
    fn read(&mut self, member: MemberId, range: ByteRange, into: &mut Vec<u8>) -> bool {
        self.reads.push((member, range));
        self.store.read(member, range, into)
    }
    fn write_repaired(&mut self, member: MemberId, offset: u64, bytes: &[u8]) {
        self.store.write_repaired(member, offset, bytes);
    }
    fn length(&self, member: MemberId) -> Option<u64> {
        self.store.length(member)
    }
}

// The files of a set's layouts, as a repair gathers them.
fn layouts(volumes: &[(String, Vec<u8>)]) -> Vec<BlockLayout> {
    let mut packets = Vec::new();
    for (_, bytes) in volumes {
        scan(bytes, &mut packets);
    }
    sets(packets)
        .iter()
        .filter_map(|set| BlockLayout::new(set, &ExecutionOptions::default()).ok())
        .collect()
}

// Many free files of one length, two of one content under different paths,
// and members that are whole copies of them, copies with a block changed,
// strangers of that length, and two copies of one file.
#[test]
fn par3_identifying_by_content_answers_and_reads_as_scanning_does() {
    const LEN: usize = 2_048;
    const FILES: u64 = 24;
    let contents: Vec<(String, Vec<u8>)> = (0..FILES)
        .map(|at| (format!("file-{at}.bin"), payload(100 + at, LEN)))
        .chain([
            ("dup-a.bin".to_owned(), payload(900, LEN)),
            ("dup-b.bin".to_owned(), payload(900, LEN)),
        ])
        .collect();
    let inputs: Vec<(&str, &[u8])> = contents
        .iter()
        .map(|(name, bytes)| (name.as_str(), bytes.as_slice()))
        .collect();
    let volumes = par3_set("set", &inputs, 1024, 2);
    let layouts = layouts(&volumes);
    let files: Vec<&FileLayout> = layouts.iter().flat_map(BlockLayout::files).collect();
    let fingerprints: BTreeSet<Fingerprint> = files.iter().map(|file| file.fingerprint).collect();
    let len = LEN as u64;
    let free: Vec<&FileLayout> = files
        .iter()
        .copied()
        .filter(|file| eligible(file, len))
        .collect();
    assert_eq!(free.len() as u64, FILES + 2);
    let indexed_files = Eligible::new(free.clone());
    let mut store = Store::default();
    let mut random = Seeded(7);
    let mut members = Vec::new();
    for at in 0..200u32 {
        let member = MemberId(at);
        let bytes = match random.below(5) {
            0 => payload(5_000 + u64::from(at), LEN),
            1 => {
                let mut changed = contents[random.below(FILES) as usize].1.clone();
                changed[random.below(len) as usize] ^= 0x5A;
                changed
            }
            2 => contents[FILES as usize + random.below(2) as usize]
                .1
                .clone(),
            _ => contents[random.below(FILES) as usize].1.clone(),
        };
        store.members.insert(member, bytes);
        members.push(member);
    }
    let run = |indexed: bool, members: &[MemberId]| {
        let mut access = Recording {
            store: Store {
                members: store.members.clone(),
                ..Store::default()
            },
            reads: Vec::new(),
        };
        let (mut read, mut work) = (0, 0);
        let found: Vec<Option<String>> = members
            .iter()
            .map(|&member| {
                let found = match indexed {
                    true => identity::identify(
                        &indexed_files,
                        &fingerprints,
                        &mut access,
                        member,
                        len,
                        &mut read,
                        &mut work,
                    ),
                    false => identify_rescanning(
                        &free,
                        &fingerprints,
                        &mut access,
                        member,
                        len,
                        &mut read,
                        &mut work,
                    ),
                };
                found.map(str::to_owned)
            })
            .collect();
        (found, access.reads, read, work)
    };
    let (scanned, scanned_reads, scanned_read, _) = run(false, &members);
    let (indexed, indexed_reads, indexed_read, _) = run(true, &members);
    assert_eq!(indexed, scanned);
    assert_eq!(indexed_reads, scanned_reads);
    assert_eq!(indexed_read, scanned_read);
    // Whole copies and changed copies are named, copies of the two alike
    // are refused, strangers are not named.
    assert!(scanned.iter().filter(|found| found.is_some()).count() > 100);
    assert!(scanned.iter().any(Option::is_none));
    // Members that are whole copies: the scan compares every file of the
    // length for each, the index looks once.
    let whole: Vec<MemberId> = members
        .iter()
        .copied()
        .filter(|member| {
            let bytes = &store.members[member];
            contents[..FILES as usize]
                .iter()
                .any(|(_, content)| content == bytes)
        })
        .collect();
    assert!(whole.len() > 50);
    let (scanned, scanned_reads, _, asked) = run(false, &whole);
    let (indexed, indexed_reads, _, work) = run(true, &whole);
    assert_eq!(indexed, scanned);
    assert_eq!(indexed_reads, scanned_reads);
    assert_eq!(asked, whole.len() as u64 * (FILES + 2));
    assert_eq!(work, whole.len() as u64);
}
