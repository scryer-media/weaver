// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Which member each described file is. Within one set a member is matched by
// file id, then by name, then by its first 16 KiB and its length, and the
// first rule that answers decides. Ambiguity refuses: a name that answers to
// two descriptions binds nothing, and two members that answer to one
// description both go unbound. A member bound by its content alone takes the
// described name. Each set binds on its own, so a member two sets describe
// alike is bound in both; a name is the weakest rule, and a bind by name
// gives way when another set describes that name as a different file.

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

// How a member matched a description.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) enum Matched {
    Id,
    Name,
    Content,
}

// The bindings of one set over the namespace, and which were by content.
// `others` are the other sets in the namespace.
pub(super) fn bind_set(
    index: &Par2FileSet,
    others: &[&Par2FileSet],
    namespace: &Namespace,
    own: ObjectId,
) -> BTreeMap<FileId, (MemberId, Matched)> {
    let mut claims: BTreeMap<FileId, Vec<(MemberId, Matched)>> = BTreeMap::new();
    for member in namespace.members() {
        if member.bound_to == Some(own) {
            continue;
        }
        let candidate = Candidate::of(member, namespace.first_bytes(member.id));
        if let Some((file, matched)) = match_member(index, others, &candidate) {
            claims.entry(file).or_default().push((member.id, matched));
        }
    }
    claims
        .into_iter()
        .filter_map(|(file, claimants)| match claimants.as_slice() {
            [only] => Some((file, *only)),
            _ => None,
        })
        .collect()
}

fn match_member(
    index: &Par2FileSet,
    others: &[&Par2FileSet],
    candidate: &Candidate<'_>,
) -> Option<(FileId, Matched)> {
    if let Some(file) = by_id(index, candidate) {
        return Some((file, Matched::Id));
    }
    match by_name(index, candidate) {
        Named::One(file) if !disputed(others, candidate, file) => Some((file, Matched::Name)),
        Named::Nothing => by_content(index, candidate).map(|file| (file, Matched::Content)),
        _ => None,
    }
}

// Another set answers the member's name with some other file. A file id is
// the same in every set that describes the file, so a different one is a
// different file under the same name.
fn disputed(others: &[&Par2FileSet], candidate: &Candidate<'_>, file: FileId) -> bool {
    others.iter().any(|other| match by_name(other, candidate) {
        Named::One(theirs) => theirs != file,
        Named::Ambiguous => true,
        Named::Nothing => false,
    })
}

// The file id is the MD5 of the 16 KiB hash, the length and the name, so a
// member that computes to a described id is that file beyond doubt.
fn by_id(index: &Par2FileSet, candidate: &Candidate<'_>) -> Option<FileId> {
    let length = candidate.complete?;
    let mut input = Vec::with_capacity(24 + candidate.name.len());
    input.extend_from_slice(
        &candidate
            .window_hash(length)
            .or_else(|| (length == 0).then(|| md5(&[])))?,
    );
    input.extend_from_slice(&length.to_le_bytes());
    input.extend_from_slice(candidate.name.as_bytes());
    let id = FileId::from_bytes(md5(&input));
    index.files.contains_key(&id).then_some(id)
}

enum Named {
    One(FileId),
    Ambiguous,
    Nothing,
}

fn by_name(index: &Par2FileSet, candidate: &Candidate<'_>) -> Named {
    let exact = described(index, |description| {
        names(description).any(|name| name == candidate.name)
    });
    let matches = if exact.is_empty() {
        // Posted names change case without changing identity, but only a
        // unique folded match is one; length never settles a folded tie.
        let folded = described(index, |description| {
            names(description).any(|name| name.eq_ignore_ascii_case(candidate.name))
        });
        if folded.len() > 1 {
            return Named::Ambiguous;
        }
        folded
    } else {
        exact
    };
    // Two descriptions under one name: a swapped pair. The one the member's
    // length has not contradicted is it, if exactly one is left.
    let matches = if matches.len() > 1 {
        let possible: Vec<_> = matches
            .iter()
            .copied()
            .filter(|(_, length)| !candidate.contradicts(*length))
            .collect();
        if possible.len() == 1 {
            possible
        } else {
            matches
        }
    } else {
        matches
    };
    match matches.as_slice() {
        [] => Named::Nothing,
        [(file, _)] => Named::One(*file),
        _ => Named::Ambiguous,
    }
}

// An obfuscated name lies; the front does not.
fn by_content(index: &Par2FileSet, candidate: &Candidate<'_>) -> Option<FileId> {
    let matches = described(index, |description| {
        !candidate.contradicts(description.length)
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

fn names(description: &FileDescription) -> impl Iterator<Item = &str> {
    [
        description.filename.as_str(),
        description.par2_name.as_str(),
    ]
    .into_iter()
}
