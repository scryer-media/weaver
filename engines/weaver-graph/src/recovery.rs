// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Recovery objects. A PAR2 set, a PAR3 set, an SFV, or the recovery data an
// archive carries inside itself. One lives in a namespace and covers members
// beside it; it verifies them from evidence the namespace hands it as bytes
// pass, and repairs them by asking up for its recovery data.

use crate::member::{MemberId, Namespace};
use crate::seek::Seek;
use crate::volume::{ByteRange, VolumeIndex};

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

// One recovery set an object holds, as its last repair left it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct SetRuling {
    // The set's own identifier as its format spells it, in lowercase hex.
    pub id: String,
    pub slice_size: u64,
    pub blocks_reachable: u64,
    pub damaged_slices: u64,
    // `None` until the set has ruled on the members bound now.
    pub outcome: Option<RepairOutcome>,
    // The ruling read member bytes back; a clean one that did not was
    // vouched for by the evidence fed as bytes passed.
    pub read: bool,
}

// The namespace's face toward a recovery object: read a member's bytes and
// write repaired ones. Positional, in member coordinates. The namespace
// resolves where the bytes actually live through the owner's inverse map.
pub trait MemberAccess {
    fn read(&mut self, member: MemberId, range: ByteRange, into: &mut Vec<u8>) -> bool;
    fn write_repaired(&mut self, member: MemberId, offset: u64, bytes: &[u8]);
    // A range the object proved bad and could not rebuild. The namespace
    // marks it damaged, as bytes that arrived failing their own check are,
    // so the member's coverage says what the object found.
    fn found_damaged(&mut self, _member: MemberId, _range: ByteRange) {}
    fn length(&self, member: MemberId) -> Option<u64>;
    // Bytes of the recovery object's own volume, which the namespace kept
    // where the object's inverse map placed them. This is where the
    // recovery data a repair spends is read from. A namespace that keeps
    // nothing for its recovery objects answers no, and they cannot repair.
    fn read_volume(
        &mut self,
        _volume: VolumeIndex,
        _range: ByteRange,
        _into: &mut Vec<u8>,
    ) -> bool {
        false
    }
}

pub trait Recovery {
    // Match the namespace's members to what this object describes, by id,
    // name or content. A member recognised by content alone is renamed
    // through the namespace and listed in `renamed`. Called whenever the
    // namespace's members, their lengths or their first bytes change, and
    // after the object learned more of what it describes. An object handed
    // its members when it was made has nothing to match.
    fn bind(&mut self, _namespace: &mut Namespace, _renamed: &mut Vec<MemberId>) {}

    // The files this object describes and can rebuild that no member of
    // the namespace carries, by name and length. The owner announces each
    // once the namespace has every member it will get, so the repair has
    // a member to write it into; the next `bind` matches it by name.
    fn absent(&self, _out: &mut Vec<(String, u64)>) {}

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

    // What the last repair came to, or `None` before one ran, and again
    // after a bind changed which members this object covers: the verdict
    // was on other members.
    fn outcome(&self) -> Option<RepairOutcome>;

    // Each set the object holds, in the order it repairs them. An object
    // with no sets of its own to name says nothing.
    fn sets(&self, _out: &mut Vec<SetRuling>) {}

    // Recovery blocks reachable right now, for health and for the
    // impossible verdict. A lost recovery article lowers it.
    fn blocks_reachable(&self) -> u64;
}
