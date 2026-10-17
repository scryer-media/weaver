// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Receiving down. The parent hands bytes to a child in the child's own
// coordinates, and the child says what they turned out to be.

use bytes::Bytes;

use crate::member::MemberId;
use crate::volume::{ByteRange, VolumeIndex};

// Where the bytes came from and whether they can be trusted.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Provenance {
    // Arrived from the source and passed its checksum.
    Fresh,
    // Arrived but failed its checksum. Kept, because a mostly right media
    // file is worth delivering when nothing can repair it. A store object
    // places it and marks the range; a decoder refuses to consume it.
    Damaged,
    // Produced by a repair. Replaces whatever was there before.
    Repaired,
}

impl Provenance {
    // Whether a recovery object may count these bytes as evidence: only bytes
    // that passed an independent checksum upstream.
    pub fn is_verified(self) -> bool {
        matches!(self, Provenance::Fresh | Provenance::Repaired)
    }
}

pub struct Delivery<'a> {
    pub volume: VolumeIndex,
    pub offset: u64,
    pub bytes: &'a [u8],
    pub provenance: Provenance,
}

impl Delivery<'_> {
    pub fn range(&self) -> ByteRange {
        ByteRange::at(self.offset, self.bytes.len() as u64)
    }
}

// The recovery data an archive carries inside itself.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum RecoveryKind {
    Par2,
    Par3,
}

// Why an object cannot stream a member. The reason travels with the refusal
// so status and metrics say why, in one vocabulary for every format.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Why {
    // The member's bytes are not its output bytes: a compressed coder, a
    // solid archive, a filter chain.
    Compressed,
    // The object needs a range it cannot have: a tail header under a parent
    // that cannot seek.
    TailUnreachable,
    // Nothing names the members: the header itself is encrypted and no
    // password opened it.
    EncryptedHeader,
    // The member's bytes are enciphered; an engine with the password reads
    // them.
    Encrypted,
    // An entry whose extent the local header does not declare, under a parent
    // that cannot seek to the directory that would.
    UnboundedEntry,
    // A redirection, an anti-item, a device node: a "member" that is not a
    // file body.
    NotAFile,
    // Damage that no recovery object covers, or that none can repair.
    Unrepairable,
    // The first bytes were lost and never repaired, so nothing could say
    // what the member is.
    FirstBytesLost,
    // Holds breached their budget before the headers that place them came.
    HoldsBreached,
    // The format is recognised but the object does not implement this shape.
    Unsupported,
}

// What a child says back when bytes arrive.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Emit {
    // The bytes are not something this object can act on yet; nothing to say.
    Pending,
    // "I found a member", with its length when the header says it.
    Member {
        member: MemberId,
        name: String,
        length: Option<u64>,
    },
    // "These bytes belong to member M at this offset."
    Bytes {
        member: MemberId,
        offset: u64,
        bytes: Bytes,
        provenance: Provenance,
    },
    // "Member M is complete."
    MemberClosed(MemberId),
    // "What I said about member M was built on bytes that were wrong; forget
    // it, I am starting M over." Everything under M is dropped.
    Retract(MemberId),
    // "My own recovery data lies in my volume at this range."
    RecoveryAt {
        volume: VolumeIndex,
        range: ByteRange,
        kind: RecoveryKind,
    },
    // "My volume is bad at this range": a header checksum failed.
    Damaged {
        volume: VolumeIndex,
        range: ByteRange,
    },
    // "I cannot read further without a password."
    NeedsPassword,
    // "This is readable, but not by streaming; hand it to an engine."
    NeedsEngine(Why),
    // "Nothing here can read this; make it a plain file."
    Refuse(Why),
}
