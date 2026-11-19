// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Recovery objects. A PAR2 set, a PAR3 set, an SFV, or the recovery data an
// archive carries inside itself. One lives in a namespace and covers members
// beside it; it verifies them from evidence the namespace hands it as bytes
// pass, and repairs them by asking up for its recovery data.

use std::collections::BTreeSet;

use crate::engine::Budget;
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
    // The object read no recovery data it could check anything against, so
    // it rules on nothing: no member is clean or short by it. It has nothing
    // more to wait for either.
    Unread,
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
    // What the set is called where it was posted, when its object can say:
    // the file that carries it.
    pub name: Option<String>,
}

// The sets of one object that cannot rebuild what they bind, set by set.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ShortSets {
    pub sets: Vec<ShortSet>,
    // Another set of the object bound members and found them whole or
    // made them so.
    pub vouched: bool,
}

// One set that ruled it cannot rebuild what it binds.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ShortSet {
    // What the set is called where it was posted, when its object can say.
    pub name: Option<String>,
    pub damaged: u64,
    pub reachable: u64,
    // The members it binds, which its ruling leaves short.
    pub members: BTreeSet<MemberId>,
}

// Why a recovery set bound no member at all, as the object that holds it
// says.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum UnboundReason {
    // No set was found in the object's volumes.
    NoSet,
    // What the set describes never arrived whole: its index, or the
    // description of a file it covers, is missing.
    Undescribed,
    // Its index arrived and could not be used: it does not read as one, or
    // it describes more than a job may hold.
    Unreadable,
    // The object found more packets or sets than it reads, and keeps none.
    TooLarge,
    // Its descriptions are whole and no member fits one.
    Unmatched,
}

impl UnboundReason {
    // The reason's stable label.
    pub fn label(self) -> &'static str {
        match self {
            Self::NoSet => "no_set",
            Self::Undescribed => "undescribed",
            Self::Unreadable => "unreadable",
            Self::TooLarge => "too_large",
            Self::Unmatched => "unmatched",
        }
    }
}

// One set an object holds that bound no member, by its own identifier when
// it has one: an object that holds no set names none.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Unbound {
    pub id: Option<String>,
    pub reason: UnboundReason,
}

// A worker's ephemeral operation failure, separate from its durable ruling.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RepairDiagnostic {
    pub operation: String,
    pub context: String,
    pub error: String,
    pub causes: Vec<String>,
}

impl RepairDiagnostic {
    pub fn capture(operation: &str, context: &str, error: &dyn std::error::Error) -> Self {
        let mut causes = Vec::new();
        let mut source = error.source();
        while let Some(error) = source {
            causes.push(error.to_string());
            source = error.source();
        }
        Self {
            operation: operation.to_owned(),
            context: context.to_owned(),
            error: error.to_string(),
            causes,
        }
    }
}

// The namespace's face toward a recovery object: positional member reads
// and staged repair writes, resolved through the owner's inverse map.
pub trait MemberAccess {
    // Ephemeral worker diagnostics. The owner publishes them only when it
    // accepts the current attempt; they never change the serialized ruling.
    fn repair_error(&mut self, _operation: &str, _context: &str, _error: &dyn std::error::Error) {}

    fn read(&mut self, member: MemberId, range: ByteRange, into: &mut Vec<u8>) -> bool;
    fn write_repaired(&mut self, member: MemberId, offset: u64, bytes: &[u8]);
    // A range the object proved bad and could not rebuild. The namespace
    // marks it damaged, as bytes that arrived failing their own check are,
    // so the member's coverage says what the object found.
    fn found_damaged(&mut self, _member: MemberId, _range: ByteRange) {}
    fn length(&self, member: MemberId) -> Option<u64>;
    // Whether the namespace has no room for this member to be rebuilt: the
    // object writes nothing into it, and a repair that would have rebuilt
    // it does not say it repaired what it covers.
    fn withheld(&self, _member: MemberId) -> bool {
        false
    }
    // The runs of `range` of the member that arrived, in offset order, each
    // of which reads; the bytes between them never came.
    fn arrived(&self, _member: MemberId, range: ByteRange, out: &mut Vec<ByteRange>) {
        out.push(range);
    }
    // An empty directory of the namespace's own, for a recovery object
    // whose format repairs files on disk rather than ranges: it stages its
    // volumes and the members it covers there for this one repair. The
    // namespace removes it once the repair returns. A namespace that keeps
    // no files answers none, and such an object cannot repair.
    fn scratch(&mut self) -> Option<std::path::PathBuf> {
        None
    }
    // A write the object made in that directory failed. The namespace hears
    // why, so a disk with no room is not taken for a set that cannot be
    // repaired.
    fn scratch_failed(&mut self, _error: std::io::Error) {}
    // How many bytes this repair writes in all when it runs: every rebuilt
    // byte it hands to `write_repaired`, and whatever it stages and rebuilds
    // in its scratch directory. Said before the first write, and said again
    // it adds. A repair that finds the disk full is run again from its
    // start, so this is the room it waits for.
    fn will_write(&mut self, _bytes: u64) {}

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
    // through the namespace, and one the object settles at another length
    // takes it there (`Namespace::take_described`): each is listed in
    // `renamed`, and its file and the object it is a volume of follow it.
    // Called whenever the
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

    // Whether the object has yet to learn what some set of it describes:
    // its own volumes may still say, once something asks for them. The
    // owner asks it to rule once the namespace holds every other member,
    // and the ruling says whether it waits on a volume or checked nothing.
    fn undescribed(&self) -> bool {
        false
    }

    // Whether this object's own volumes are part of the release. Recovery
    // data is spent by the job and left out; a listing is kept beside what
    // it lists.
    fn released(&self) -> bool {
        false
    }

    // Whether this object rebuilds what it covers, or only checks it. A set
    // that only checks finds damage and repairs none of it, so its volumes
    // are no data to repair with.
    fn rebuilds(&self) -> bool;

    // Whether this object's recovery is carried inside the one member it
    // covers, as a RAR volume's own record is: it repairs that member and
    // nothing beside it, so a listing over other members still rules them.
    fn carried_inside(&self) -> bool;

    // Whether a rebuild can write more than the recovery data it received:
    // a format that may name one block in many places of a file can fill a
    // missing file far larger than its recovery. The owner holds such a set
    // to the disk reserve before it goes out.
    fn outgrows_recovery(&self) -> bool;

    // Evidence as bytes pass: the namespace calls this for every covered
    // member on every delivery and every repair. It is where a ruling is
    // taken back for its input: bytes said again keep it when they are what
    // it read. Forgetting input says nothing to a set.
    fn feed(&mut self, member: MemberId, segments: &[Segment]);

    // Evidence published while acknowledging this object's own repair.
    // It establishes the ruling's new input rather than invalidating it.
    fn feed_proven(&mut self, member: MemberId, segments: &[Segment]) {
        self.feed(member, segments);
    }

    // The block sizes at whose boundaries `feed`'s checksums have to be cut
    // for this object to vouch for a block without reading it back. A
    // source that cuts its checksums there, as the article decoder can,
    // gives the object its proof as the bytes pass. An object that folds
    // checksums of any alignment, or takes none, names none.
    fn block_sizes(&self, _out: &mut Vec<u64>) {}

    // The parent's other answer to a seek: this range of a covered member
    // will never come. The object learns that and nothing else, no bytes
    // and no verdict on them, and counts it against what it can still
    // reach, so it rules short the moment the refusals outrun its blocks.
    fn refused(&mut self, _member: MemberId, _range: ByteRange) {}

    // The verdict per slice of a covered member, as far as the evidence goes.
    fn verdicts(&self, member: MemberId, out: &mut Vec<SliceVerdict>);

    // Recovery blocks this object wants ahead of the normal order, given the
    // damage it knows about.
    fn wants(&self, out: &mut Vec<Seek>);

    // Repair what is damaged among the members this object covers, within
    // what `budget` grants the repair.
    fn repair(&mut self, access: &mut dyn MemberAccess, budget: Budget) -> RepairOutcome;

    // What the last repair came to, or `None` before one ran, and again
    // after a bind changed which members this object covers: the verdict
    // was on other members. One that came up short of recovery data is
    // `Waiting` while what it needs may still come, and `None` in the step
    // that is in hand or can no longer come: the owner rules again only on
    // `None`.
    fn outcome(&self) -> Option<RepairOutcome>;

    // The bytes of a covered member, `length` long, that a clean or repaired
    // ruling proves: what the object's checks reach. Most objects check the
    // whole member; one whose parity covers only part of it proves that
    // part, and damage outside it stays as it was.
    fn proves(&self, _member: MemberId, length: u64) -> ByteRange {
        ByteRange::new(0, length)
    }

    // Each set the object holds, in the order it repairs them. An object
    // with no sets of its own to name says nothing.
    fn sets(&self, _out: &mut Vec<SetRuling>) {}

    // What each set of this object that stands ruled it cannot rebuild what
    // it binds leaves short, when the object can say it set by set. `None`
    // when it cannot: then its own outcome rules every member it covers.
    fn short_sets(&self) -> Option<ShortSets> {
        None
    }

    // Each set the object holds that bound no member, with why, and one
    // with no identifier when it holds no set at all. Asked once, when the
    // job is done. An object that binds what it is handed says nothing.
    fn unbound(&self, _out: &mut Vec<Unbound>) {}

    // How this object's repair ranks against another's over the same
    // bytes. A returned repair rewrites bytes an earlier repair wrote only
    // when its object ranks above every other set of its namespace; between
    // equals the first repair stands. Most objects rank alike.
    fn precedence(&self) -> u8 {
        0
    }
}
