// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// One volume's headers, walked by unrar-rs over the bytes in hand. The walk
// reads every header and seeks over every data area, so the store keeps only
// header bytes: the signature, then whatever the walk ran short at, as far as
// the point it says the next header reaches. Bytes that arrived damaged are
// kept out of the walk until the volume closes, because a header's own CRC
// only warns and a damaged name would be read as a real one. Where the set
// ends is never read from them at all: the store gives volumes back on it.

use std::sync::Arc;

use unrar_rs::{
    ArchiveFormat, KdfCache, RarArchive, RarError, RarVolumeFacts, RarVolumeFactsWalk,
    RarVolumeHeaderEncryption,
};
use weaver_graph::{ByteRange, Emit, Provenance, Why};

use super::image::SparseImage;
use crate::kept::Kept;

// What read_signature reads before it scans.
const SIGNATURE: u64 = 8;

// How much clean prefix a volume may show without one header reading
// before it is not a RAR volume: real headers live in the first few hundred
// bytes.
const MAX_HEADER_PREFIX_BYTES: u64 = 4 * 1024 * 1024;

// A member's data area in this volume, in the order the headers list them.
#[derive(Clone, Debug)]
pub(super) struct Block {
    pub(super) data: ByteRange,
}

#[derive(Debug)]
pub(super) enum Step {
    Walked {
        moved: bool,
        damaged: Option<ByteRange>,
    },
    // The headers are enciphered and no proved password opens them.
    Locked(RarVolumeHeaderEncryption),
    // The bytes in this range make no RAR headers.
    Broken(ByteRange),
    Refuse(Emit),
}

#[derive(Debug)]
pub(super) struct Walk {
    pub(super) facts: Option<RarVolumeFacts>,
    // What the end record says of a volume after this one, once the walk
    // read an end record that arrived whole.
    more: Option<bool>,
    pub(super) ended: bool,
    pub(super) blocks: Vec<Block>,
    pub(super) locked: Option<RarVolumeHeaderEncryption>,
    dirty: bool,
}

impl Default for Walk {
    fn default() -> Self {
        Self {
            facts: None,
            more: None,
            ended: false,
            blocks: Vec::new(),
            locked: None,
            dirty: true,
        }
    }
}

fn signature_len(facts: &RarVolumeFacts) -> u64 {
    match facts.archive_format() {
        ArchiveFormat::Rar5 => 8,
        ArchiveFormat::Rar4 | ArchiveFormat::Rar14 => 7,
    }
}

// Where the last data area the headers name ends: every byte before it is
// a header or data the headers place.
fn data_end(facts: &RarVolumeFacts) -> u64 {
    let members = facts
        .members
        .iter()
        .map(|member| member.data_offset.saturating_add(member.data_size));
    let services = facts
        .services
        .iter()
        .map(|service| service.data_offset.saturating_add(service.data_size));
    members.chain(services).fold(signature_len(facts), u64::max)
}

// The first byte at or after `from` the walk may not read.
fn hole(kept: &Kept, from: u64, closed: bool) -> u64 {
    let mut at = from;
    for (range, _, provenance) in kept.runs() {
        if (!closed && provenance == Provenance::Damaged) || range.end <= at {
            continue;
        }
        if range.start > at {
            break;
        }
        at = range.end;
    }
    at
}

// Whether a byte past `from` arrived damaged.
fn damaged_past(kept: &Kept, from: u64) -> bool {
    kept.runs()
        .into_iter()
        .any(|(range, _, provenance)| provenance == Provenance::Damaged && range.end > from)
}

// The damaged run a byte sits in.
fn damaged_at(kept: &Kept, at: u64) -> Option<ByteRange> {
    kept.runs()
        .into_iter()
        .find(|(range, _, provenance)| *provenance == Provenance::Damaged && range.contains(at))
        .map(|(range, _, _)| range)
}

// A file header as a physical walk reads it, to hold a Quick Open cache to.
#[derive(PartialEq, Eq)]
struct MemberIdentity {
    name: String,
    data_offset: u64,
    data_size: u64,
    split_before: bool,
    split_after: bool,
}

enum QuickOpenCrossCheck {
    Agreed,
    Inconclusive,
    Mismatch,
}

// The file headers a physical walk finds, with the Quick Open cache
// suppressed; `None` when the walk does not run cleanly.
fn physical_member_identities(
    kept: &Kept,
    closed: bool,
    password: Option<&str>,
    kdf: &KdfCache,
) -> Option<Vec<MemberIdentity>> {
    let mut image = SparseImage::of(kept, closed);
    if unrar_rs::signature::read_signature(&mut image).ok()? != ArchiveFormat::Rar5 {
        return None;
    }
    let parsed = unrar_rs::header::parse_all_headers_with_kdf_cache_and_options(
        &mut image,
        password,
        kdf,
        unrar_rs::header::HeaderParseOptions {
            allow_quick_open: false,
        },
    )
    .ok()?;
    Some(
        parsed
            .files
            .iter()
            .map(|file| MemberIdentity {
                name: file.header.name.clone(),
                data_offset: file.header.data_offset,
                data_size: file.header.data_size,
                split_before: file.header.split_before,
                split_after: file.header.split_after,
            })
            .collect(),
    )
}

fn quick_open_cross_check(
    facts: &RarVolumeFacts,
    kept: &Kept,
    closed: bool,
    password: Option<&str>,
    kdf: &KdfCache,
) -> QuickOpenCrossCheck {
    let claimed: Vec<MemberIdentity> = facts
        .members
        .iter()
        .map(|member| MemberIdentity {
            name: member.name.clone(),
            data_offset: member.data_offset,
            data_size: member.data_size,
            split_before: member.split_before,
            split_after: member.split_after,
        })
        .collect();
    let physical = physical_member_identities(kept, closed, password, kdf);
    if physical.as_deref() == Some(claimed.as_slice()) {
        return QuickOpenCrossCheck::Agreed;
    }
    let walk_stopped_short = physical
        .as_deref()
        .is_none_or(|walked| walked.len() < claimed.len() && claimed.starts_with(walked));
    if !closed && walk_stopped_short {
        return QuickOpenCrossCheck::Inconclusive;
    }
    QuickOpenCrossCheck::Mismatch
}

impl Walk {
    // Something the walk reads changed: walk again.
    pub(super) fn touch(&mut self) {
        self.dirty = true;
    }

    // How far the headers place bytes: nothing before any are read, every
    // byte once the volume has ended.
    pub(super) fn reach(&self) -> Option<u64> {
        let facts = self.facts.as_ref()?;
        Some(match self.ended {
            true => u64::MAX,
            false => data_end(facts),
        })
    }

    // The first byte the next walk needs and may not read yet.
    pub(super) fn hole(&self, kept: &Kept, closed: bool) -> u64 {
        hole(kept, self.facts.as_ref().map_or(0, data_end), closed)
    }

    // Whether a hole at `at` is the main header, the first header after
    // the signature, or the signature itself.
    pub(super) fn at_main_header(&self, at: u64) -> bool {
        at <= self.facts.as_ref().map_or(SIGNATURE, signature_len)
    }

    // Whether the volume is one of a set. Only the main header sets the
    // volume flag, so a set flag is its word at once; a clear one is its
    // word only once the walk has ended, since a walk short of the main
    // header reports the flag clear. Enciphered headers make that walk
    // common.
    pub(super) fn in_set(&self) -> Option<bool> {
        let facts = self.facts.as_ref()?;
        match (facts.is_volume, self.ended) {
            (true, _) => Some(true),
            (false, true) => Some(false),
            (false, false) => None,
        }
    }

    // Whether a volume follows this one: never, for an archive that is no
    // set; otherwise what the end record says, once the walk has read it
    // whole.
    pub(super) fn successor(&self) -> Option<bool> {
        match self.in_set()? {
            false => Some(false),
            true => self.more,
        }
    }

    // Whether no volume follows this one.
    pub(super) fn is_last(&self) -> bool {
        self.successor() == Some(false)
    }

    pub(super) fn step(
        &mut self,
        kept: &mut Kept,
        length: Option<u64>,
        password: Option<&str>,
        kdf: &Arc<KdfCache>,
        closed: bool,
    ) -> Step {
        if !std::mem::take(&mut self.dirty) {
            return self.waiting(kept, closed, false);
        }
        let signature = ByteRange::new(0, length.map_or(SIGNATURE, |length| length.min(SIGNATURE)));
        kept.want(signature);
        if hole(kept, 0, closed) < signature.end {
            return self.waiting(kept, closed, false);
        }
        let image = SparseImage::of(kept, closed);
        let walk = match RarArchive::parse_volume_facts_walk_with_shared_kdf_cache(
            image,
            password,
            Arc::clone(kdf),
        ) {
            Ok(walk) => walk,
            Err(RarError::EncryptedArchive) => {
                let image = SparseImage::of(kept, closed);
                return match RarArchive::parse_volume_header_encryption(image) {
                    Ok(encryption) => {
                        self.locked = Some(encryption);
                        Step::Locked(encryption)
                    }
                    Err(_) => Step::Broken(self.broken(kept, closed)),
                };
            }
            Err(
                RarError::UnsupportedEncryption { .. }
                | RarError::UnsupportedEncryptionKdf { .. }
                | RarError::InvalidPassword
                | RarError::WrongPassword { .. },
            ) => return Step::Refuse(Emit::NeedsEngine(Why::EncryptedHeader)),
            Err(_) => return self.unparsed(kept, length, closed),
        };
        self.locked = None;
        if walk.facts.headers_from_quick_open {
            match quick_open_cross_check(&walk.facts, kept, closed, password, kdf) {
                QuickOpenCrossCheck::Agreed => {}
                QuickOpenCrossCheck::Inconclusive => return self.waiting(kept, closed, false),
                QuickOpenCrossCheck::Mismatch => {
                    return Step::Refuse(Emit::NeedsEngine(Why::Unsupported));
                }
            }
        }
        self.accept(walk, kept, length, password, kdf, closed)
    }

    fn accept(
        &mut self,
        walk: RarVolumeFactsWalk,
        kept: &mut Kept,
        length: Option<u64>,
        password: Option<&str>,
        kdf: &Arc<KdfCache>,
        closed: bool,
    ) -> Step {
        let facts = walk.facts;
        let reach = data_end(&facts);
        let ended = match (walk.short_at, length) {
            (None, _) => true,
            (Some(short_at), Some(length)) => short_at > length,
            (Some(_), None) => false,
        };
        let mut damaged = None;
        if let (true, Some(short_at), Some(length)) = (ended, walk.short_at, length) {
            if reach > length {
                damaged = Some(ByteRange::new(length, reach));
            } else if RarArchive::parse_volume_facts_with_shared_kdf_cache(
                SparseImage::of(kept, closed),
                password,
                Arc::clone(kdf),
            )
            .is_err()
            {
                damaged = Some(ByteRange::new(length, short_at.max(length)));
            }
        }
        if let (false, Some(short_at)) = (ended, walk.short_at) {
            let end = length.map_or(short_at, |length| short_at.min(length));
            kept.want(ByteRange::new(reach, end.max(reach)));
        }
        // The end record lies past the last data area: a damaged byte there
        // may be its own.
        let more =
            (walk.short_at.is_none() && !damaged_past(kept, reach)).then_some(facts.more_volumes);
        let moved = self.facts.as_ref() != Some(&facts) || self.ended != ended || self.more != more;
        self.blocks = facts
            .members
            .iter()
            .map(|member| Block {
                data: ByteRange::at(member.data_offset, member.data_size),
            })
            .collect();
        self.facts = Some(facts);
        self.more = more;
        self.ended = ended;
        match damaged {
            Some(range) => Step::Walked {
                moved,
                damaged: Some(range),
            },
            None => self.waiting(kept, closed, moved),
        }
    }

    // The walk waits on its next bytes; when those arrived damaged, say so.
    fn waiting(&self, kept: &Kept, closed: bool, moved: bool) -> Step {
        let damaged = match self.ended || closed {
            true => None,
            false => damaged_at(kept, self.hole(kept, closed)),
        };
        Step::Walked { moved, damaged }
    }

    // A walk that read no header: before any is read, a prefix too short to
    // hold the main header, which more bytes answer, until the prefix shown
    // is past the ceiling or nothing more will arrive.
    fn unparsed(&mut self, kept: &mut Kept, length: Option<u64>, closed: bool) -> Step {
        let prefix = hole(kept, 0, closed);
        let whole = length.is_some_and(|length| prefix >= length);
        if self.facts.is_some() || closed || whole || prefix > MAX_HEADER_PREFIX_BYTES {
            return Step::Broken(self.broken(kept, closed));
        }
        let end = length.map_or(prefix * 2, |length| length.min(prefix * 2));
        kept.want(ByteRange::new(0, end));
        self.waiting(kept, closed, false)
    }

    // The bytes a walk that failed read: from the last data area to the
    // first byte it could not see, or from the front.
    fn broken(&self, kept: &Kept, closed: bool) -> ByteRange {
        let from = self.facts.as_ref().map_or(0, data_end);
        let end = hole(kept, from, closed);
        match end > from {
            true => ByteRange::new(from, end),
            false => ByteRange::new(0, hole(kept, 0, closed)),
        }
    }
}
