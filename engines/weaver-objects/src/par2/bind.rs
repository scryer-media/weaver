// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Which member each described file is. Within one set a member is the file
// its first 16 KiB and its length say it is; only while its front cannot
// tell is it the file its name says, since a posted name can lie, swapped
// with another file's, where the bytes cannot. A name that answers to two
// descriptions binds nothing, unless the member's length rules out all but
// one, and two members that answer to one description both go unbound. A
// member that answers in two sets is the first set's: the others count it
// missing in their arithmetic and never write it. A member bound by its
// content under another name takes the described name.

use std::collections::BTreeMap;

use par2_rs::checksum::md5;
use par2_rs::{FileDescription, FileId, Par2FileSet};
use weaver_graph::{Member, MemberId, Namespace, ObjectId};

// PAR2 identifies a file by the MD5 of at most this much of its front.
const HASH_WINDOW: u64 = 16 * 1024;

// One member as the rules see it.
struct Candidate<'a> {
    name: &'a str,
    // The length, once every byte of it has arrived.
    complete: Option<u64>,
    // The furthest byte seen so far.
    seen: u64,
    first_bytes: &'a [u8],
}

impl<'a> Candidate<'a> {
    fn of(member: &'a Member, first_bytes: &'a [u8]) -> Self {
        Self {
            name: &member.name,
            complete: member.length.filter(|_| member.is_complete()),
            seen: member
                .coverage
                .runs()
                .map(|run| run.range.end)
                .max()
                .unwrap_or(0),
            first_bytes,
        }
    }

    // A description whose length this member already contradicts is not it.
    fn contradicts(&self, length: u64) -> bool {
        match self.complete {
            Some(complete) => complete != length,
            None => self.seen > length,
        }
    }

    // The MD5 of the description's hash window, if the front reaches it.
    fn window_hash(&self, length: u64) -> Option<[u8; 16]> {
        let window = usize::try_from(length.min(HASH_WINDOW)).ok()?;
        (window > 0 && self.first_bytes.len() >= window).then(|| md5(&self.first_bytes[..window]))
    }
}

// The bindings of each set over the namespace, and the described name of
// each member bound by its content alone.
pub(super) fn bind_sets(
    indexes: &[&Par2FileSet],
    namespace: &Namespace,
    own: ObjectId,
) -> (Vec<BTreeMap<FileId, MemberId>>, Vec<(MemberId, String)>) {
    let mut claims: Vec<BTreeMap<FileId, Vec<(MemberId, bool)>>> =
        vec![BTreeMap::new(); indexes.len()];
    for member in namespace.members() {
        if member.bound_to == Some(own) {
            continue;
        }
        let candidate = Candidate::of(member, namespace.first_bytes(member.id));
        let first = indexes
            .iter()
            .enumerate()
            .find_map(|(at, index)| Some((at, match_member(index, &candidate)?)));
        if let Some((at, (file, by_content))) = first {
            claims[at]
                .entry(file)
                .or_default()
                .push((member.id, by_content));
        }
    }
    let mut names = Vec::new();
    let bound = claims
        .into_iter()
        .zip(indexes)
        .map(|(claims, index)| {
            claims
                .into_iter()
                .filter_map(|(file, claimants)| {
                    let [(member, by_content)] = claimants.as_slice() else {
                        return None;
                    };
                    if *by_content && let Some(description) = index.file_description(&file) {
                        names.push((*member, description.filename.clone()));
                    }
                    Some((file, *member))
                })
                .collect()
        })
        .collect();
    (bound, names)
}

// A file and whether the member matched it by content alone, under a name
// that does not answer to it even folded.
fn match_member(index: &Par2FileSet, candidate: &Candidate<'_>) -> Option<(FileId, bool)> {
    if let Some(file) = by_content(index, candidate) {
        let named = index
            .file_description(&file)
            .is_some_and(|description| description.filename.eq_ignore_ascii_case(candidate.name));
        return Some((file, !named));
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
fn by_content(index: &Par2FileSet, candidate: &Candidate<'_>) -> Option<FileId> {
    let matches = described(index, |description| {
        !candidate.contradicts(description.length)
            && !is_split_fragment_of(candidate.name, &description.filename)
            && candidate.window_hash(description.length) == Some(description.hash_16k)
    });
    match matches.as_slice() {
        [(file, _)] => Some(*file),
        _ => None,
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
