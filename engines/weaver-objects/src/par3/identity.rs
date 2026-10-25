// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Which described file a member is, by its bytes, when no described name
// answers to it. The rules are content discovery's: the
// member's whole fingerprint names the one free description that carries
// it, and failing that, a description whose block extents all match but one
// interior one, over most of its bytes. A match proposes a name only; the
// set still verifies and repairs the member as any other it binds.

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
    let fingerprint = fingerprint(access, member, ByteRange::new(0, len), read)?;
    let damaged_name = if files.iter().any(|file| file.fingerprint == fingerprint) {
        None
    } else {
        unique_damaged_name(eligible.iter().copied(), access, member, read)
    };
    let mut matches = eligible.iter().filter(|file| {
        file.fingerprint == fingerprint || damaged_name.as_deref() == Some(file.path.as_str())
    });
    let file = matches.next()?;
    // Compatible aliases need independent output identities.
    if matches.any(|other| other.path != file.path) {
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

fn unique_damaged_name<'a>(
    files: impl Iterator<Item = &'a FileLayout> + Clone,
    access: &mut dyn MemberAccess,
    member: MemberId,
    read: &mut u64,
) -> Option<String> {
    let required = files
        .clone()
        .try_fold(0u64, |total, file| total.checked_add(file.len));
    if required.is_none_or(|bytes| bytes > READ_LIMIT.saturating_sub(*read))
        || files.clone().any(|file| {
            file.extents.iter().any(|extent| {
                matches!(
                    extent.kind,
                    ExtentKind::Block {
                        fingerprint: None,
                        ..
                    }
                )
            })
        })
    {
        // An unexamined candidate is not a negative match. Never pick the
        // first name merely because checking another exhausted the budget.
        return None;
    }
    let mut matched: Option<&str> = None;
    for file in files {
        if matches_single_damaged_extent(access, member, file, read)? {
            if matched.is_some_and(|name| name != file.path) {
                return None;
            }
            matched = Some(&file.path);
        }
    }
    matched.map(str::to_owned)
}

// Deliberately conservative: an exact-length image with authenticated
// extents at both ends, every other extent matching but one interior
// block, and most of the bytes authenticated. `None` when a read fails.
fn matches_single_damaged_extent(
    access: &mut dyn MemberAccess,
    member: MemberId,
    file: &FileLayout,
    read: &mut u64,
) -> Option<bool> {
    let count = file.extents.len();
    if count < 3
        || file.len > READ_LIMIT.saturating_sub(*read)
        || file.extents.range(0).is_none_or(|range| range.start != 0)
        || file
            .extents
            .range(count - 1)
            .is_none_or(|range| range.end != file.len)
        || (1..count).any(|index| {
            file.extents.range(index - 1).map(|range| range.end)
                != file.extents.range(index).map(|range| range.start)
        })
    {
        return Some(false);
    }
    let mut damaged = 0;
    let mut matched_bytes = 0;
    for (index, extent) in file.extents.iter().enumerate() {
        let ExtentKind::Block {
            fingerprint: Some(expected),
            ..
        } = extent.kind
        else {
            return Some(false);
        };
        let range = ByteRange::new(extent.range.start, extent.range.end);
        if fingerprint(access, member, range, read)? != expected {
            damaged += 1;
            if damaged > 1 || index == 0 || index + 1 == count {
                return Some(false);
            }
        } else {
            matched_bytes += range.len();
        }
    }
    Some(damaged == 1 && matched_bytes > file.len / 2)
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
