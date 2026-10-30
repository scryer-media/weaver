// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Which member each described file is. Within one set a member is the file
// its first 16 KiB and its length say it is. An exact described name can
// distinguish matching fronts; otherwise a content tie stays unbound.
// With no content match, its name may identify it. A name that answers to two
// descriptions binds nothing, unless the member's length rules out all but
// one. Content evidence outranks a name-only claim; equal claims to one
// description stay unbound and remain available for slice verification. A
// member whose content answers in two sets is the first set's: the others count it
// missing in their arithmetic and never write it. A member bound by its
// content under another name takes the described name. Each posting of a
// member posted more than once answers for it: the member is every file
// one of its postings is.

#[cfg(test)]
use std::cell::Cell;
use std::cell::OnceCell;
use std::collections::BTreeMap;

use par2_rs::{FileDescription, FileId, Par2FileSet};
use weaver_graph::{Member, MemberId, Namespace, ObjectId};

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
type Bindings = BTreeMap<FileId, MemberId>;

// Each posting's front as PAR2 names files, under the stamp of the front
// it was hashed from.
pub(super) type Fronts = BTreeMap<MemberId, (u64, Option<Front>)>;

// What binding the sets over the namespace came to.
pub(super) struct Bound {
    // Each set's bindings, in the order the sets were given.
    pub(super) sets: Vec<Bindings>,
    // Each complete member no set matched, with the length it has of its
    // own: one whose front could not name it, which a repair may still
    // know by its slices or by elimination.
    pub(super) unnamed: Vec<(MemberId, Length)>,
    // How many fronts the binding hashed.
    #[cfg(test)]
    pub(super) digests: usize,
}

// Bind each set over the namespace: a set whose index is not usable binds
// nothing. `fronts` is what the last binding hashed: a front whose stamp
// is unchanged is not hashed again, and what this binding hashed replaces
// it.
pub(super) fn bind_sets(
    sets: &[&SetState],
    namespace: &Namespace,
    own: ObjectId,
    fronts: &mut Fronts,
) -> Bound {
    let mut claims: Vec<BTreeMap<FileId, Vec<(MemberId, bool)>>> =
        vec![BTreeMap::new(); sets.len()];
    let mut complete = BTreeMap::new();
    let last = std::mem::take(fronts);
    #[cfg(test)]
    let mut digests = 0;
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
        for (at, state) in sets.iter().enumerate() {
            if let Some((file, content)) = match_member(state, &candidate) {
                if content {
                    first = Some((at, (file, content)));
                    break;
                }
                first.get_or_insert((at, (file, content)));
            }
        }
        if let Some((at, (file, content))) = first {
            let claimants = claims[at].entry(file).or_default();
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
    let bound: Vec<Bindings> = claims
        .into_iter()
        .map(|claims| {
            claims
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
                .collect()
        })
        .collect();
    for members in &bound {
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
    let mut matches = described(index, |description| description.filename == candidate.name);
    if matches.is_empty() {
        // Posted subjects can change case without changing file identity.
        // Only a unique folded match is usable; length must not resolve a
        // collision between distinct case-sensitive recovery names.
        matches = described(index, |description| {
            description.filename.eq_ignore_ascii_case(candidate.name)
        });
        if matches.len() > 1 {
            return None;
        }
    }
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
    use weaver_graph::{MemberId, ObjectId, Provenance, Recovery};

    use super::super::Par2;
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
}
