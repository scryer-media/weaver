// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Receiving down. The parent hands bytes to a child in the child's own
// coordinates, and the child says what they turned out to be.

use std::any::Any;
use std::fmt;
use std::sync::Arc;

use crate::evidence::Evidenced;
use crate::member::MemberId;
use crate::object::Kind;
use crate::volume::{ByteRange, VolumeIndex};

// Bytes posted for a member: where they start, what they are, with what
// was proved of them, and how they came.
pub struct Posted {
    pub offset: u64,
    pub bytes: Evidenced,
    pub provenance: Provenance,
}

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
    // Arrived, and then a checksum over them failed: an object's over its
    // own volume or member, or a recovery set's over its slices. Damaged
    // as `Damaged` is, and a verdict on bytes in hand: only proof displaces
    // it, never the same bytes, or another copy of them, arriving again.
    Refuted,
}

impl Provenance {
    // Whether a recovery object may count these bytes as evidence: only bytes
    // that passed an independent checksum upstream.
    pub fn is_verified(self) -> bool {
        matches!(self, Provenance::Fresh | Provenance::Repaired)
    }

    // Whether the bytes are held as damage: what clean bytes over them
    // would mend, and what a decoder refuses to consume.
    pub fn is_damaged(self) -> bool {
        matches!(self, Provenance::Damaged | Provenance::Refuted)
    }

    // Whether a record of bytes with this provenance already answers the
    // same bytes arriving again with `later`, and stands: a repair stands
    // against everything but a repair. A posting's own bytes coming again
    // are the same bytes, read back or released from a hold, and are taken
    // again; a further posting's copy is answered by what the member holds
    // first, clean or refuted, not by this.
    pub(crate) fn answers(self, later: Provenance) -> bool {
        self == Provenance::Repaired && later != Provenance::Repaired
    }

    // What bytes arriving with `later` are once they land where the record
    // says `self`. A checksum's verdict stands over the bytes it judged,
    // whether they came before it, come after it, or come again: only a
    // repair displaces it. Anything else lands as it came.
    pub(crate) fn landing(self, later: Provenance) -> Provenance {
        match self {
            Provenance::Refuted if later != Provenance::Repaired => Provenance::Refuted,
            _ => later,
        }
    }
}

// Bytes of one of an object's volumes. The object emits its members' bytes
// as slices of these, which copy nothing and cut the evidence with them.
pub struct Delivery<'a> {
    pub volume: VolumeIndex,
    pub offset: u64,
    pub bytes: &'a Evidenced,
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
    // A RAR recovery record: parity over the volume's own bytes before it.
    Rar,
}

impl RecoveryKind {
    // The recovery object built over the range.
    pub fn object(self) -> Kind {
        match self {
            RecoveryKind::Par2 => Kind::Par2,
            RecoveryKind::Par3 => Kind::Par3,
            RecoveryKind::Rar => Kind::RarRecord,
        }
    }
}

// What the object that found its own recovery data hands the recovery
// object built over it: what it learned opening its headers that the
// recovery object reads its range by. Opaque to the tree, which only
// carries it; only the kind built over the range reads it. Most hand
// nothing. Two are the same when they are one value.
#[derive(Clone, Default)]
pub struct Handed(Option<Arc<dyn Any + Send + Sync>>);

impl Handed {
    pub fn new(value: impl Any + Send + Sync) -> Self {
        Self(Some(Arc::new(value)))
    }

    // What was handed, when it is a `T`.
    pub fn read<T: Any>(&self) -> Option<&T> {
        self.0.as_deref()?.downcast_ref()
    }
}

impl PartialEq for Handed {
    fn eq(&self, other: &Self) -> bool {
        match (&self.0, &other.0) {
            (Some(one), Some(other)) => Arc::ptr_eq(one, other),
            (one, other) => one.is_none() && other.is_none(),
        }
    }
}

impl Eq for Handed {}

// What was handed may be a secret: Debug says only whether there is one.
impl fmt::Debug for Handed {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let said = if self.0.is_some() {
            "Handed(..)"
        } else {
            "Handed"
        };
        formatter.write_str(said)
    }
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
    // Every volume is in and nothing more will reach the object, yet it
    // never placed every member it named: its map never came, or never
    // closed what it announced.
    Unmapped,
    // A volume the set needs is in, and nothing states where in the set it
    // goes: more than one free place fits it and no name or header chooses.
    OrderUnknown,
    // The object would sit deeper than a job's namespaces may nest. What
    // the member holds is the container, delivered as it is.
    TooDeep,
    // Recognised container rejected by structural or security validation.
    // Its posted bytes are not a usable substitute for its members.
    InvalidArchive,
}

impl Why {
    // Whether a member given back for this reason is withheld rather than
    // delivered: its bytes are a container nothing opened, not the output.
    // A member given back for any other reason is a file, its bytes the
    // output as they stand.
    pub fn withholds(self) -> bool {
        match self {
            Why::Encrypted
            | Why::EncryptedHeader
            | Why::Compressed
            | Why::Unmapped
            | Why::OrderUnknown
            | Why::InvalidArchive => true,
            Why::TailUnreachable
            | Why::UnboundedEntry
            | Why::NotAFile
            | Why::Unrepairable
            | Why::FirstBytesLost
            | Why::HoldsBreached
            | Why::Unsupported
            | Why::TooDeep => false,
        }
    }

    // Whether an object that refused a member for this reason disclaims it:
    // not its to open, so the member is the posted file and is whole when
    // its bytes are. For any other reason the object's set failed with the
    // member, which is delivered as it stands, never whole.
    pub fn disclaims(self) -> bool {
        match self {
            Why::Unsupported | Why::TooDeep => true,
            Why::Compressed
            | Why::TailUnreachable
            | Why::EncryptedHeader
            | Why::Encrypted
            | Why::UnboundedEntry
            | Why::NotAFile
            | Why::Unrepairable
            | Why::FirstBytesLost
            | Why::HoldsBreached
            | Why::Unmapped
            | Why::OrderUnknown
            | Why::InvalidArchive => false,
        }
    }
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
    // "These bytes belong to member M at this offset", with the evidence of
    // them that came with them or that the object computed.
    Bytes {
        member: MemberId,
        offset: u64,
        bytes: Evidenced,
        provenance: Provenance,
    },
    // "My headers say member M's bytes have this CRC32": said once the
    // object knows it, for a member it announced. Only a checksum of the
    // member's own bytes, never a keyed one.
    Checksum {
        member: MemberId,
        crc32: u32,
    },
    // "Member M is complete."
    MemberClosed(MemberId),
    // "What I said about member M was built on bytes that were wrong; forget
    // it, I am starting M over." Everything under M is dropped.
    Retract(MemberId),
    // "My own recovery data lies in my volume at this range, and this is
    // what reading it takes." Said again with what was handed anew, the
    // recovery object built over the range takes it in place of what it
    // held.
    RecoveryAt {
        volume: VolumeIndex,
        range: ByteRange,
        kind: RecoveryKind,
        handed: Handed,
    },
    // "The bytes of my volume at `range`, which I placed in my envelope at
    // `at`, are no longer mine": a header that came since says whose they
    // are. They come to me again, and go where my map puts them now.
    Unkept {
        volume: VolumeIndex,
        range: ByteRange,
        envelope: u32,
        at: u64,
    },
    // "Give me the bytes of my volume at `range` again, as they were
    // posted": what I said of them was read under a key I now know is
    // wrong. Every member they reached is open again, and what of the range
    // was posted comes again with the provenance it came with.
    Again {
        volume: VolumeIndex,
        range: ByteRange,
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
    // "My own headers say this volume is not mine where I hold it." The
    // claim that bound it was provisional: the member goes back to its
    // namespace, with what I parsed of it, to be offered again.
    GiveBack(VolumeIndex),
}

#[cfg(test)]
mod tests {
    use super::Why;

    #[test]
    fn a_refusal_withholds_its_member_only_when_the_bytes_are_not_the_output() {
        let withheld = [
            Why::Encrypted,
            Why::EncryptedHeader,
            Why::Compressed,
            Why::Unmapped,
            Why::OrderUnknown,
            Why::InvalidArchive,
        ];
        let delivered = [
            Why::TailUnreachable,
            Why::UnboundedEntry,
            Why::NotAFile,
            Why::Unrepairable,
            Why::FirstBytesLost,
            Why::HoldsBreached,
            Why::Unsupported,
            Why::TooDeep,
        ];
        for why in withheld {
            assert!(why.withholds(), "{why:?}");
        }
        for why in delivered {
            assert!(!why.withholds(), "{why:?}");
        }
        // Only a shape the object does not implement, or an object the job
        // never opens for its depth, leaves the member the posted file;
        // every other refusal is its set failing.
        for why in withheld.into_iter().chain(delivered) {
            assert_eq!(
                why.disclaims(),
                matches!(why, Why::Unsupported | Why::TooDeep),
                "{why:?}"
            );
        }
    }
}
