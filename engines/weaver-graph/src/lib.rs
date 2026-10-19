// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Weaver's engine: one tree per job.
//
// At the top is a root source that fetches bytes from outside. It feeds a
// namespace, which is a list of members. A member is a leaf, whose bytes go
// to a file, or is owned by an object that understands one format and
// produces a namespace of its own. The tree goes as deep as the archives do.
//
// Every object has the same two faces and knows nothing about any format but
// its own:
//
//   ask up      `Seek`: "I want this range of this volume ahead of the normal
//               order", answered `Moved`, `Lost` or `Unseekable`.
//   receive     `Delivery`: bytes of a volume at an offset, with a provenance,
//               answered with what they turned out to be (`Emit`).
//
// Recovery objects (PAR2, PAR3, an archive's own recovery record) live in a
// namespace and repair the members beside them through the same two faces.
//
// This crate is the format-blind part: the interfaces, the tree that wires
// them, the namespace, the name ledger, the holds accountant, the slots and
// the sniffer. It must never learn a format's layout. Nothing here names a
// header field, an article or a piece. The one exception is the sniffer,
// which knows magic bytes and file types, and only those, so that it can
// say which object a member belongs to.

pub mod deliver;
pub mod engine;
pub mod facts;
pub mod holds;
pub mod member;
pub mod names;
pub mod object;
pub mod records;
pub mod recovery;
pub mod seek;
pub mod slots;
pub mod sniff;
pub mod tree;
pub mod volume;
mod wire;

pub use deliver::{Delivery, Emit, Provenance, RecoveryKind, Why};
pub use engine::{Engine, Extraction, Sink, Volumes};
pub use facts::{Durable, Facts, Restore, RestoreError};
pub use member::{Coverage, Member, MemberId, Namespace, Run};
pub use names::NameLedger;
pub use object::{Carried, Extent, Kind, Object, ObjectId, Placement, Standing};
pub use records::{
    CoverageRecord, CryptRecord, EnvelopeRecord, MemberRecord, NodeRecord, NodeState, TreeRecord,
};
pub use recovery::{Recovery, Segment, SliceVerdict};
pub use seek::{Pull, Purpose, Seek, SeekUp, Weight};
pub use sniff::{Sniff, VERDICT_BYTES, sniff};
pub use tree::{Builder, Effect, NodeId, Tree};
pub use volume::{ByteRange, VolumeIndex};
