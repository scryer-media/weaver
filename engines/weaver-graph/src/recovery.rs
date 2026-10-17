// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Recovery objects. A PAR2 set, a PAR3 set, an SFV, or the recovery data an
// archive carries inside itself. One lives in a namespace and covers members
// beside it; it verifies them from evidence the namespace hands it as bytes
// pass, and repairs them by asking up for its recovery data.

use crate::member::MemberId;
use crate::seek::Seek;
use crate::volume::ByteRange;

// One block-aligned piece of a delivered range with its checksum, computed by
// the namespace from bytes it had in hand. The grid folds these into slice
// verdicts without reading the member back.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Segment {
    pub range: ByteRange,
    pub crc32: u32,
    // Whether the bytes passed an independent checksum upstream. Only
    // verified segments are evidence; a damaged one marks its slices.
    pub verified: bool,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum SliceVerdict {
    Unknown,
    Intact,
    Damaged,
}

// What a repair attempt came to.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RepairOutcome {
    // Nothing was damaged.
    Clean,
    // The damaged slices were rewritten; the namespace delivers them down
    // as repaired.
    Repaired { slices: u64 },
    // More slices are damaged than recovery blocks are reachable.
    Impossible { damaged: u64, reachable: u64 },
    // Recovery blocks are still on their way.
    Waiting,
}

// The namespace's face toward a recovery object: read a member's bytes and
// write repaired ones. Positional, in member coordinates. The namespace
// resolves where the bytes actually live through the owner's inverse map.
pub trait MemberAccess {
    fn read(&mut self, member: MemberId, range: ByteRange, into: &mut Vec<u8>) -> bool;
    fn write_repaired(&mut self, member: MemberId, offset: u64, bytes: &[u8]);
    fn length(&self, member: MemberId) -> Option<u64>;
}

pub trait Recovery {
    // Whether this object describes the member, by id, name or content.
    fn covers(&self, member: MemberId) -> bool;

    // Evidence as bytes pass: the namespace calls this for every covered
    // member on every delivery and every repair.
    fn feed(&mut self, member: MemberId, segments: &[Segment]);

    // The verdict per slice of a covered member, as far as the evidence goes.
    fn verdicts(&self, member: MemberId, out: &mut Vec<SliceVerdict>);

    // Recovery blocks this object wants ahead of the normal order, given the
    // damage it knows about.
    fn wants(&mut self, out: &mut Vec<Seek>);

    // Repair what is damaged among the members this object covers.
    fn repair(&mut self, access: &mut dyn MemberAccess) -> RepairOutcome;

    // Recovery blocks reachable right now, for health and for the
    // impossible verdict. A lost recovery article lowers it.
    fn blocks_reachable(&self) -> u64;
}
