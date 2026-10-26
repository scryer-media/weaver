// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Which described file a member is, by its bytes, when no described name
// answers to it. The member's whole fingerprint names the one free
// description that carries it; failing that, as a PAR2 member is known by a
// slice it holds, the one free description of its length one of whose blocks
// it holds intact at that block's place. A hole or damage elsewhere in the
// member does not hide what it is. A match proposes a name only; the set
// still verifies and repairs the member as any other it binds.

use par3_rs::Fingerprint;
use par3_rs::hash::FingerprintHasher;
use par3_rs::layout::{ExtentKind, FileLayout};
use weaver_graph::recovery::MemberAccess;
use weaver_graph::{ByteRange, MemberId};

// What one pass may read beyond the whole-file fingerprints.
const READ_LIMIT: u64 = 1 << 30;
const CHUNK: u64 = 64 << 10;

// The described path the member of `len` bytes is, among `files` (every
// file the sets describe), where `free` says which paths no member carries.
pub(super) fn identify<'a>(
    files: &[&'a FileLayout],
    free: impl Fn(&str) -> bool,
    access: &mut dyn MemberAccess,
    member: MemberId,
    len: u64,
    read: &mut u64,
) -> Option<&'a str> {
    let eligible: Vec<&FileLayout> = files
        .iter()
        .copied()
        .filter(|file| free(&file.path) && eligible(file, len))
        .collect();
    if eligible.is_empty() {
        return None;
    }
    let whole = fingerprint(access, member, ByteRange::new(0, len), read);
    let matched: Vec<&FileLayout> = match whole {
        Some(whole) if files.iter().any(|file| file.fingerprint == whole) => eligible
            .iter()
            .copied()
            .filter(|file| file.fingerprint == whole)
            .collect(),
        _ => {
            if eligible.iter().map(|file| file.len).sum::<u64>() > READ_LIMIT.saturating_sub(*read)
            {
                // An unexamined candidate is not a negative match.
                return None;
            }
            eligible
                .iter()
                .copied()
                .filter(|file| holds_block_of(access, member, file, read))
                .collect()
        }
    };
    let file = matched.first()?;
    // Compatible aliases need independent output identities.
    if matched.iter().any(|other| other.path != file.path) {
        return None;
    }
    Some(&file.path)
}

// A description a member can be identified as: its length, content that
// can be fingerprinted, a flat path, and every byte of it protected.
fn eligible(file: &FileLayout, len: u64) -> bool {
    file.len == len
        && file.len != 0
        && file.fingerprint != [0; 16]
        && !file.path.contains('/')
        && !file
            .extents
            .iter()
            .any(|extent| matches!(extent.kind, ExtentKind::Unprotected))
}

// Whether the member holds one of the file's fingerprinted blocks intact at
// its place: read back in order until one checks. A block that does not
// read back proves nothing and the reading carries on past it.
fn holds_block_of(
    access: &mut dyn MemberAccess,
    member: MemberId,
    file: &FileLayout,
    read: &mut u64,
) -> bool {
    file.extents.iter().any(|extent| {
        let ExtentKind::Block {
            fingerprint: Some(expected),
            ..
        } = extent.kind
        else {
            return false;
        };
        let range = ByteRange::new(extent.range.start, extent.range.end);
        !range.is_empty() && fingerprint(access, member, range, read) == Some(expected)
    })
}

// The fingerprint of a range of the member, read back a chunk at a time.
fn fingerprint(
    access: &mut dyn MemberAccess,
    member: MemberId,
    range: ByteRange,
    read: &mut u64,
) -> Option<Fingerprint> {
    let mut hash = FingerprintHasher::new();
    let mut bytes = Vec::new();
    let mut offset = range.start;
    while offset < range.end {
        let end = range.end.min(offset.saturating_add(CHUNK));
        bytes.clear();
        if !access.read(member, ByteRange::new(offset, end), &mut bytes)
            || bytes.len() as u64 != end - offset
        {
            return None;
        }
        *read = read.saturating_add(bytes.len() as u64);
        hash.update(&bytes);
        offset = end;
    }
    Some(hash.finalize())
}
