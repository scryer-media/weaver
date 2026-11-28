// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Which member each described file is. Within one set a member is the file
// its first 16 KiB and its length say it is. An exact described name can
// distinguish matching fronts; otherwise a content tie stays unbound.
// With no content match, its name may identify it. A name that answers to two
// descriptions binds nothing, unless the member's length rules out all but
// one. Content evidence outranks a name-only claim; equal claims to one
// description stay unbound and remain available for slice verification. A
// member whose content answers in two sets is the first set's that has not
// given it up, and the first set's once every one of them has: the others
// count it missing in their arithmetic and never write it. A set gives a
// member up when it rules it cannot rebuild what it binds (`Par2::give_up`),
// and the member then moves to the next set whose content it answers in
// (`next_holder`), which verifies it and may rebuild it. A member bound by its
// content under another name takes the described name. Each posting of a
// member posted more than once answers for it: the member is every file
// one of its postings is.

#[cfg(test)]
use std::cell::Cell;
use std::cell::OnceCell;
use std::collections::{BTreeMap, BTreeSet};

#[cfg(test)]
use par2_rs::FileDescription;
use par2_rs::{FileId, Par2FileSet};
use weaver_graph::{Member, MemberId, Namespace, ObjectId};

use super::catalogue::Catalogue;
use super::sets::{Front, Length, SetState};

// One member as the rules see it.
struct Candidate<'a> {
    name: &'a str,
    // The length, once every byte of it has arrived: exact when any byte of
    // it decoded, else only what the post's encoded size bounds it to.
    complete: Option<Length>,
    // The furthest byte seen so far.
    seen: u64,
    first_bytes: &'a [u8],
    // Its front as PAR2 names files: the one kept for the same front, or
    // hashed once, when a set first asks.
    front: OnceCell<Option<Front>>,
    #[cfg(test)]
    hashed: Cell<usize>,
}

impl<'a> Candidate<'a> {
    fn of(member: &'a Member, first_bytes: &'a [u8], kept: Option<Option<Front>>) -> Self {
        Self {
            name: &member.name,
            complete: member
                .length
                .filter(|_| member.is_complete())
                .map(|length| {
                    if member
                        .coverage
                        .runs()
                        .any(|run| run.provenance.is_verified())
                    {
                        Length::Exact(length)
                    } else {
                        Length::AtMost(length)
                    }
                }),
            seen: member
                .coverage
                .runs()
                .map(|run| run.range.end)
                .max()
                .unwrap_or(0),
            first_bytes,
            front: kept.map(OnceCell::from).unwrap_or_default(),
            #[cfg(test)]
            hashed: Cell::new(0),
        }
    }

    // A description whose length this member already contradicts is not it.
    fn contradicts(&self, length: u64) -> bool {
        match self.complete {
            Some(complete) => !complete.fits(length),
            None => self.seen > length,
        }
    }

    fn front(&self) -> Option<Front> {
        *self.front.get_or_init(|| {
            #[cfg(test)]
            self.hashed
                .set(self.hashed.get() + usize::from(!self.first_bytes.is_empty()));
            Front::of(self.first_bytes)
        })
    }
}

// The members a set's index named, by file.
pub(super) type Bindings = BTreeMap<FileId, MemberId>;

// Each posting's front as PAR2 names files, under the stamp of the front
// it was hashed from.
pub(super) type Fronts = BTreeMap<MemberId, (u64, Option<Front>)>;

// What binding the sets over the namespace came to.
pub(super) struct Bound<S> {
    // Each set's bindings.
    pub(super) sets: S,
    // Each complete member no set matched, with the length it has of its
    // own: one whose front could not name it, which a repair may still
    // know by its slices or by elimination.
    pub(super) unnamed: Vec<(MemberId, Length)>,
    // How many fronts the binding hashed.
    #[cfg(test)]
    pub(super) digests: usize,
    // How many times a member was matched against a set.
    #[cfg(test)]
    pub(super) checks: usize,
}

// Bind the usable sets the catalogue files over the namespace. A member is
// matched only against the sets the catalogue finds for its front or its
// name: no other set can match it. Only a set some member claimed has
// bindings, by its key. `fronts` is what the last binding hashed: a front
// whose stamp is unchanged is not hashed again, and what this binding
// hashed replaces it.
pub(super) fn bind_catalogued(
    sets: &BTreeMap<[u8; 16], SetState>,
    catalogue: &Catalogue,
    namespace: &Namespace,
    own: ObjectId,
    fronts: &mut Fronts,
    given_up: &BTreeSet<([u8; 16], MemberId)>,
) -> Bound<BTreeMap<[u8; 16], Bindings>> {
    bind_over(
        namespace,
        own,
        fronts,
        catalogue.any(),
        |front, name| {
            catalogue
                .candidates(front, name)
                .map(|(_, key)| (key, &sets[&key]))
        },
        |key, member| given_up.contains(&(key, member)),
    )
}

// The set a member bound to `file` of the set `key` moves to once that set
// gave it up: the first, in the catalogue's order, whose content the member
// answers in that has not given it up, or else the first it answers in at
// all. The member's content is the front `key` describes for the file, and
// a set answers when exactly one of its files has that front at a length
// that fits it. Asked only of a set that gave its members up, through the
// catalogue's entries for the one front: never of every set or member.
pub(super) fn next_holder(
    sets: &BTreeMap<[u8; 16], SetState>,
    catalogue: &Catalogue,
    (key, file): ([u8; 16], FileId),
    member: MemberId,
    given_up: &BTreeSet<([u8; 16], MemberId)>,
) -> Option<([u8; 16], FileId)> {
    let description = sets.get(&key)?.usable()?.file_description(&file)?;
    let front = Front::described(description.length, description.hash_16k);
    let length = description.length;
    let mut first = None;
    for (_, other) in catalogue.fronted(front) {
        let Some(state) = sets.get(&other) else {
            continue;
        };
        let Some(index) = state.usable() else {
            continue;
        };
        let [found] = state.fronted(&front) else {
            continue;
        };
        if index
            .file_description(found)
            .is_none_or(|described| described.length != length)
        {
            continue;
        }
        if !given_up.contains(&(other, member)) {
            return Some((other, *found));
        }
        first.get_or_insert((other, *found));
    }
    first
}

// Bind each set over the namespace, matching every member against every
// set given, in the order given: a set whose index is not usable binds
// nothing. Each set's bindings, in that order.
#[cfg(test)]
pub(super) fn bind_sets(
    sets: &[&SetState],
    namespace: &Namespace,
    own: ObjectId,
    fronts: &mut Fronts,
    passed: impl Fn(usize, MemberId) -> bool,
) -> Bound<Vec<Bindings>> {
    let any = sets.iter().any(|state| state.usable().is_some());
    let found = bind_over(
        namespace,
        own,
        fronts,
        any,
        |_, _| sets.iter().copied().enumerate(),
        passed,
    );
    let mut bound = found.sets;
    Bound {
        sets: (0..sets.len())
            .map(|at| bound.remove(&at).unwrap_or_default())
            .collect(),
        unnamed: found.unnamed,
        digests: found.digests,
        checks: found.checks,
    }
}

// Bind the sets `candidates` lists for each member's front and name, in
// their order, over the namespace. `any` says whether a usable set is
// listed at all: only then is a member's front hashed. `passed` says
// whether a set gave the member up.
fn bind_over<'s, K, I>(
    namespace: &Namespace,
    own: ObjectId,
    fronts: &mut Fronts,
    any: bool,
    candidates: impl Fn(Option<Front>, &str) -> I,
    passed: impl Fn(K, MemberId) -> bool,
) -> Bound<BTreeMap<K, Bindings>>
where
    K: Copy + Ord,
    I: Iterator<Item = (K, &'s SetState)>,
{
    let mut claims: BTreeMap<K, BTreeMap<FileId, Vec<(MemberId, bool)>>> = BTreeMap::new();
    let mut complete = BTreeMap::new();
    let last = std::mem::take(fronts);
    #[cfg(test)]
    let mut digests = 0;
    #[cfg(test)]
    let mut checks = 0;
    for posting in namespace.records() {
        let member = namespace.resolve(posting.id);
        if namespace
            .get(member)
            .is_some_and(|record| record.bound_to == Some(own))
        {
            continue;
        }
        let (stamp, first_bytes) = namespace.stamped_front(posting.id);
        let kept = last
            .get(&posting.id)
            .filter(|(hashed, _)| *hashed == stamp)
            .map(|(_, front)| *front);
        let candidate = Candidate::of(posting, first_bytes, kept);
        if member == posting.id
            && let Some(length) = candidate.complete
        {
            complete.insert(member, length);
        }
        let mut first = None;
        // The first set the content answers in, kept while each such set
        // gave the member up: it is the member's once all of them have.
        let mut given_up = None;
        let listed = any.then(|| candidates(candidate.front(), candidate.name));
        for (at, state) in listed.into_iter().flatten() {
            #[cfg(test)]
            {
                checks += 1;
            }
            if let Some((file, content)) = match_member(state, &candidate) {
                if content {
                    if passed(at, member) {
                        given_up.get_or_insert((at, (file, content)));
                        continue;
                    }
                    first = Some((at, (file, content)));
                    break;
                }
                first.get_or_insert((at, (file, content)));
            }
        }
        if let Some(fallback) = given_up
            && first.is_none_or(|(_, (_, content))| !content)
        {
            first = Some(fallback);
        }
        if let Some((at, (file, content))) = first {
            let claimants = claims.entry(at).or_default().entry(file).or_default();
            if let Some((_, proven)) = claimants.iter_mut().find(|(id, _)| *id == member) {
                *proven |= content;
            } else {
                claimants.push((member, content));
            }
        }
        if let Some(front) = candidate.front.get() {
            fronts.insert(posting.id, (stamp, *front));
        }
        #[cfg(test)]
        {
            digests += candidate.hashed.get();
        }
    }
    let bound: BTreeMap<K, Bindings> = claims
        .into_iter()
        .map(|(at, claims)| {
            let bindings = claims
                .into_iter()
                .filter_map(|(file, mut claimants)| {
                    // Verified content outranks a posted name. Equal evidence
                    // still needs a unique member; ordering resolves nothing.
                    if claimants.iter().any(|(_, content)| *content) {
                        claimants.retain(|(_, content)| *content);
                    }
                    match claimants.as_slice() {
                        [(member, _)] => Some((file, *member)),
                        _ => None,
                    }
                })
                .collect();
            (at, bindings)
        })
        .collect();
    for members in bound.values() {
        for member in members.values() {
            complete.remove(member);
        }
    }
    // A conflicting name did not identify the member. Keep it available
    // for slice verification and elimination alongside unmatched members.
    Bound {
        sets: bound,
        unnamed: complete.into_iter().collect(),
        #[cfg(test)]
        digests,
        #[cfg(test)]
        checks,
    }
}

// The file the member is, by its content first and its name after.
fn match_member(state: &SetState, candidate: &Candidate<'_>) -> Option<(FileId, bool)> {
    let index = state.usable()?;
    match by_content(state, index, candidate) {
        ContentMatch::Unique(file) => return Some((file, true)),
        ContentMatch::Ambiguous(files) => {
            let mut named = files.into_iter().filter(|file| {
                index
                    .file_description(file)
                    .is_some_and(|description| description.filename == candidate.name)
            });
            let file = named.next();
            return file
                .filter(|_| named.next().is_none())
                .map(|file| (file, true));
        }
        ContentMatch::Inconclusive => {}
    }
    let mut matches = named(state, index, candidate.name)?;
    // Two names, two descriptions: the posted name of a swapped pair answers
    // to the partner's description. Length breaks that tie whenever the pair
    // differ in it. Only a tie is broken this way: a lone name match keeps
    // its binding whatever the lengths say, because a short file at its own
    // name is damage.
    if matches.len() > 1 {
        let possible: Vec<_> = matches
            .iter()
            .copied()
            .filter(|(_, length)| !candidate.contradicts(*length))
            .collect();
        if possible.len() == 1 {
            matches = possible;
        }
    }
    match matches.as_slice() {
        [(file, _)] => Some((*file, false)),
        _ => None,
    }
}

// A zero-length description has no content to be identified by, and a
// window the front does not reach cannot be tested. A split fragment begins
// with the whole described file, so its front matches though it is not it.
enum ContentMatch {
    Unique(FileId),
    Inconclusive,
    Ambiguous(Vec<FileId>),
}

fn by_content(state: &SetState, index: &Par2FileSet, candidate: &Candidate<'_>) -> ContentMatch {
    let Some(front) = candidate.front() else {
        return ContentMatch::Inconclusive;
    };
    let matches: Vec<FileId> = state
        .fronted(&front)
        .iter()
        .copied()
        .filter(|file| {
            index.file_description(file).is_some_and(|description| {
                !candidate.contradicts(description.length)
                    && !is_split_fragment_of(candidate.name, &description.filename)
            })
        })
        .collect();
    match matches.as_slice() {
        [file] => ContentMatch::Unique(*file),
        [] => ContentMatch::Inconclusive,
        _ => ContentMatch::Ambiguous(matches),
    }
}

// The files the set describes under `name`, each with its length, in file
// order: those named exactly, or else those named so in another ASCII case.
// None when more than one is named so in another case.
fn named(state: &SetState, index: &Par2FileSet, name: &str) -> Option<Vec<(FileId, u64)>> {
    let folded = state.named(name);
    let exact: Vec<(FileId, u64)> = folded
        .iter()
        .copied()
        .filter(|(file, _)| {
            index
                .file_description(file)
                .is_some_and(|description| description.filename == name)
        })
        .collect();
    if !exact.is_empty() {
        return Some(exact);
    }
    // Posted subjects can change case without changing file identity.
    // Only a unique folded match is usable; length must not resolve a
    // collision between distinct case-sensitive recovery names.
    (folded.len() <= 1).then(|| folded.to_vec())
}

// What `named` answered before the name index: every description scanned.
#[cfg(test)]
fn named_by_scan(index: &Par2FileSet, name: &str) -> Option<Vec<(FileId, u64)>> {
    let mut matches = described(index, |description| description.filename == name);
    if matches.is_empty() {
        matches = described(index, |description| {
            description.filename.eq_ignore_ascii_case(name)
        });
        if matches.len() > 1 {
            return None;
        }
    }
    Some(matches)
}

#[cfg(test)]
fn described(index: &Par2FileSet, test: impl Fn(&FileDescription) -> bool) -> Vec<(FileId, u64)> {
    let mut found: Vec<_> = index
        .files
        .values()
        .filter(|description| test(description))
        .map(|description| (description.file_id, description.length))
        .collect();
    found.sort();
    found
}

// Whether a name is a numeric split fragment of the described name.
fn is_split_fragment_of(candidate_name: &str, described_name: &str) -> bool {
    candidate_name
        .strip_prefix(described_name)
        .and_then(|suffix| suffix.strip_prefix('.'))
        .is_some_and(|extension| {
            !extension.is_empty() && extension.as_bytes().iter().all(u8::is_ascii_digit)
        })
}

#[cfg(test)]
mod tests {
    use std::collections::{BTreeMap, BTreeSet, HashMap};

    use par2_rs::{FileDescription, FileId, Par2FileSet, RecoverySetId};
    use weaver_graph::{MemberId, ObjectId, Provenance, Recovery};

    use super::super::Par2;
    use super::super::sets::SetState;
    use super::{named, named_by_scan};
    use crate::testing::{Job, par2_set, payload};

    const OWN: ObjectId = ObjectId(7);
    const FILES: usize = 50;

    // The described name of the file a set bound `member` to.
    fn bound_name(job: &Job<Par2>, member: MemberId) -> Option<String> {
        job.object.sets.values().find_map(|state| {
            let file = state.bound(member)?;
            Some(state.usable()?.file_description(&file)?.filename.clone())
        })
    }

    // Fifty files of one length, posted under names that are not theirs:
    // only their fronts can bind them, and no length rules any out. Each
    // front is hashed once, not once per description, and not again by a
    // later bind while it stands.
    #[test]
    fn a_bind_hashes_each_front_once() {
        let files: Vec<(String, Vec<u8>)> = (0..FILES)
            .map(|at| (format!("file{at:02}.bin"), payload(at as u64 + 1, 2048)))
            .collect();
        let described: Vec<(&str, &[u8])> = files
            .iter()
            .map(|(name, bytes)| (name.as_str(), bytes.as_slice()))
            .collect();
        let mut job = Job::new(Par2::new(OWN), OWN);
        for (name, bytes) in par2_set("set", &described, 512, 1, 1) {
            job.volume(&name, &bytes, 700, false);
        }
        let members: Vec<_> = files
            .iter()
            .enumerate()
            .map(|(at, (_, bytes))| job.member(&format!("posted{at:02}.dat"), bytes))
            .collect();
        for call in 1..=2 {
            job.object.bind(&mut job.namespace, &mut Vec::new());
            assert_eq!(job.object.digests, FILES, "bind call {call}");
        }
        for (member, (name, _)) in members.iter().zip(&files) {
            assert!(job.object.covers(*member));
            assert_eq!(&job.namespace.get(*member).unwrap().name, name);
        }
    }

    // A repair that writes a front again is a new front, even over an
    // earlier repair of the same range: it is hashed again, once, and the
    // member is the file its new front says.
    #[test]
    fn a_repaired_front_is_hashed_again_once() {
        let (a, b) = (payload(1, 2048), payload(2, 2048));
        let mut job = Job::new(Par2::new(OWN), OWN);
        for (name, bytes) in par2_set("set", &[("a.bin", &a), ("b.bin", &b)], 512, 1, 1) {
            job.volume(&name, &bytes, 700, false);
        }
        let member = job.member("posted.dat", &a);
        let bind = |job: &mut Job<Par2>| {
            job.object.bind(&mut job.namespace, &mut Vec::new());
            (job.object.digests, bound_name(job, member))
        };
        assert_eq!(bind(&mut job), (1, Some("a.bin".to_owned())));
        job.arrive(member, 0, &b, Provenance::Repaired);
        assert_eq!(bind(&mut job), (2, Some("b.bin".to_owned())));
        job.arrive(member, 0, &a, Provenance::Repaired);
        assert_eq!(bind(&mut job), (3, Some("a.bin".to_owned())));
        assert_eq!(bind(&mut job), (3, Some("a.bin".to_owned())));
    }

    // An index describing `files`, each its id byte, name and length.
    fn index(files: &[(u8, &str, u64)]) -> Par2FileSet {
        let files: HashMap<FileId, FileDescription> = files
            .iter()
            .map(|&(id, name, length)| {
                let file_id = FileId::from_bytes([id; 16]);
                let description = FileDescription {
                    file_id,
                    hash_full: [id; 16],
                    hash_16k: [id; 16],
                    length,
                    par2_name: name.to_owned(),
                    filename: name.to_owned(),
                };
                (file_id, description)
            })
            .collect();
        Par2FileSet {
            recovery_set_id: RecoverySetId::from_bytes([9; 16]),
            slice_size: 512,
            recovery_file_ids: files.keys().copied().collect(),
            non_recovery_file_ids: Vec::new(),
            files,
            slice_checksums: HashMap::new(),
            recovery_slices: BTreeMap::new(),
            creator: None,
            unicode_names: Default::default(),
        }
    }

    // What `named` found, each file by its id byte; none for a refusal.
    type Found = Option<Vec<(u8, u64)>>;

    fn ids(found: Option<Vec<(FileId, u64)>>) -> Found {
        found.map(|found| {
            found
                .into_iter()
                .map(|(file, length)| (file.as_bytes()[0], length))
                .collect()
        })
    }

    // The name index answers every posted name as scanning every
    // description did: the same files, in the same order, and the same
    // refusals.
    #[test]
    fn the_name_index_answers_as_the_scan_did() {
        let index = index(&[
            (1, "movie.mkv", 100),
            (2, "dup.bin", 60),
            (3, "dup.bin", 50),
            (4, "same.bin", 70),
            (5, "same.bin", 70),
            (6, "Case.TXT", 10),
            (7, "Fold.nfo", 5),
            (8, "fold.NFO", 6),
            (9, "Exact.srt", 3),
            (10, "exact.SRT", 4),
            (11, "\u{c9}t\u{e9}.bin", 8),
        ]);
        let state = SetState::indexed(index.clone());
        assert!(state.usable().is_some());
        let cases: [(&str, Found); 9] = [
            // Exact, unique.
            ("movie.mkv", Some(vec![(1, 100)])),
            // Exact, two descriptions of one name, lengths apart.
            ("dup.bin", Some(vec![(2, 60), (3, 50)])),
            // Exact, two descriptions of one name and length.
            ("same.bin", Some(vec![(4, 70), (5, 70)])),
            // Another case, unique.
            ("case.txt", Some(vec![(6, 10)])),
            // Two descriptions apart only in case, posted in a third.
            ("FOLD.Nfo", None),
            // Exact beside another apart only in case: exact wins.
            ("Exact.srt", Some(vec![(9, 3)])),
            // Folding is ASCII only: 'É' is not 'é'.
            ("\u{e9}t\u{e9}.bin", Some(vec![])),
            // ASCII letters of a non-ASCII name still fold.
            ("\u{c9}T\u{e9}.BIN", Some(vec![(11, 8)])),
            // Nothing of the name.
            ("nothing.here", Some(vec![])),
        ];
        for (name, expected) in cases {
            let scanned = named_by_scan(&index, name);
            assert_eq!(ids(scanned.clone()), expected, "scan, {name}");
            assert_eq!(named(&state, &index, name), scanned, "index, {name}");
        }
    }

    // Seeded names over a small alphabet in random case, so exact, folded,
    // colliding and missing names all occur: the index answers each as the
    // scan did.
    #[test]
    fn the_name_index_answers_seeded_names_as_the_scan_did() {
        let mut state = 0x2545_f491_4f6c_dd1d_u64;
        let mut next = move || {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state
        };
        // One to three of 'a', 'b' and 'é', each in either case.
        fn name(next: &mut dyn FnMut() -> u64) -> String {
            let length = 1 + next() % 3;
            (0..length)
                .map(|_| {
                    let [lower, upper] =
                        [['a', 'A'], ['b', 'B'], ['\u{e9}', '\u{c9}']][(next() % 3) as usize];
                    if next().is_multiple_of(2) {
                        lower
                    } else {
                        upper
                    }
                })
                .collect()
        }
        let described: Vec<(u8, String, u64)> = (1..=60u8)
            .map(|id| (id, name(&mut next), next() % 4))
            .collect();
        let files: Vec<(u8, &str, u64)> = described
            .iter()
            .map(|(id, name, length)| (*id, name.as_str(), *length))
            .collect();
        let index = index(&files);
        let set = SetState::indexed(index.clone());
        let mut outcomes = BTreeSet::new();
        for _ in 0..400 {
            let posted = name(&mut next);
            let scanned = named_by_scan(&index, &posted);
            outcomes.insert(scanned.as_ref().map(Vec::len));
            assert_eq!(named(&set, &index, &posted), scanned, "{posted}");
        }
        assert!(outcomes.contains(&None), "{outcomes:?}");
        assert!(outcomes.contains(&Some(0)), "{outcomes:?}");
        assert!(outcomes.contains(&Some(1)), "{outcomes:?}");
        assert!(
            outcomes
                .iter()
                .any(|count| count.is_some_and(|count| count > 1)),
            "{outcomes:?}"
        );
    }
}
