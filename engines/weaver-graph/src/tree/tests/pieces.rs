// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A store refused though the set beside it proves every byte of it, and one
// other member the set covers. A member whose name carries a type people
// open is a file of its own, never the store's last piece; a member whose
// name says nothing of what it is still is.

use super::*;
use crate::deliver::Posted;
use crate::recovery::{MemberAccess, RepairOutcome};
use crate::{Segment, SliceVerdict};

// A pretend recovery set that covers every member and has ruled clean.
struct Proven;

impl Object for Proven {
    fn unposted(&mut self, _: VolumeIndex, _: ByteRange) {}

    fn kind(&self) -> Kind {
        Kind::Par2
    }
    fn deliver(
        &mut self,
        _: Delivery<'_>,
        _: &mut dyn FnMut(&Placement, &mut Vec<u8>) -> bool,
        _: &mut Vec<Emit>,
    ) {
    }
    fn volume_closed(&mut self, _: VolumeIndex, _: &mut Vec<Emit>) {}
    fn volume_lost(&mut self, _: VolumeIndex, _: ByteRange, _: &mut Vec<Emit>) {}
    fn wants(&self, _: &mut Vec<Seek>) {}
    fn pulled(&mut self, _: Seek, _: Pull, _: &mut Vec<Emit>) {}
    fn locate(&self, volume: VolumeIndex, range: ByteRange, out: &mut Vec<Placement>) {
        out.push(Placement::Envelope {
            envelope: volume.0,
            range,
        });
    }
    fn extent(&self, _: MemberId, _: ByteRange, _: &mut Vec<Extent>) -> Located {
        Located::Never
    }
    fn envelope(&self, _: u32, _: ByteRange) -> Option<Bytes> {
        None
    }
    fn recovery(&self) -> Option<&dyn Recovery> {
        Some(self)
    }
    fn recovery_mut(&mut self) -> Option<&mut dyn Recovery> {
        Some(self)
    }
}

impl Recovery for Proven {
    // Its bind is the default, which changes nothing it covers.
    fn take_newly(&mut self, _: &mut Vec<(MemberId, String)>) {}

    fn rebuilds(&self) -> bool {
        true
    }

    fn carried_inside(&self) -> bool {
        false
    }

    fn outgrows_recovery(&self) -> bool {
        false
    }

    fn covers(&self, _: MemberId) -> bool {
        true
    }
    fn covers_some(&self) -> bool {
        true
    }
    fn feed(&mut self, _: MemberId, _: &[Segment]) {}
    fn verdicts(&self, _: MemberId, _: &mut Vec<SliceVerdict>) {}
    fn wants(&self, _: &mut Vec<Seek>) {}
    fn repair(&mut self, _: &mut dyn MemberAccess, _: crate::Budget) -> RepairOutcome {
        RepairOutcome::Clean
    }
    fn outcome(&self) -> Option<RepairOutcome> {
        Some(RepairOutcome::Clean)
    }
}

// The store doubles of the parent module, and the proven set for PAR2.
struct Joining;

impl Builder for Joining {
    fn build(&mut self, kind: Kind, id: ObjectId) -> Option<Box<dyn Object>> {
        match kind {
            Kind::Par2 => Some(Box::new(Proven)),
            _ => Build.build(kind, id),
        }
    }

    fn read(&self, _: &Tree, _: NodeId, _: VolumeIndex, _: &Placement, _: &mut Vec<u8>) -> bool {
        false
    }
}

const LENGTH: u64 = 40;

fn deliver(tree: &mut Tree, member: MemberId, bytes: Vec<u8>) {
    tree.deliver(
        NodeId::ROOT,
        member,
        Posted {
            offset: 0,
            bytes: Bytes::from(bytes).into(),
            provenance: Provenance::Fresh,
        },
        &mut Joining,
        &mut Vec::new(),
    );
}

// `lantern.rar`, whose front signs the store, `other` beside it, shorter
// and signing nothing, and a set over both. The store is refused whole:
// whether the split store is founded over the two.
fn joins(other: &str) -> bool {
    joins_refused(other, None)
}

// `joins`, with the store's member read back as refused for `why` when one
// is given, as the owner reads every refused member back.
fn joins_refused(other: &str, why: Option<Why>) -> bool {
    let mut tree = Tree::new();
    let first = tree.announce_root("lantern.rar", Some(LENGTH), &mut Vec::new());
    let beside = tree.announce_root(other, Some(LENGTH / 2), &mut Vec::new());
    let set = tree.announce_root("lantern.par2", None, &mut Vec::new());
    let mut front = SIGNATURE.to_vec();
    front.resize(LENGTH as usize, b'r');
    deliver(&mut tree, first, front);
    deliver(&mut tree, beside, vec![b'n'; (LENGTH / 2) as usize]);
    let mut header = b"PAR2\0PKT".to_vec();
    header.extend_from_slice(&64u64.to_le_bytes());
    header.resize(64, 0);
    deliver(&mut tree, set, header);
    assert!(
        tree.child_of(NodeId::ROOT, set).is_some(),
        "the set is bound"
    );
    let mut effects = Vec::new();
    match why {
        Some(why) => tree.refused_written(NodeId::ROOT, first, why, &mut Joining, &mut effects),
        None => tree.refused_whole(NodeId::ROOT, first, &mut Joining, &mut effects),
    }
    tree.bound_below(NodeId::ROOT)
        .into_iter()
        .any(|node| tree.kind(node) == Some(Kind::Split))
}

// A name that says the member is a file people open keeps it a file: the
// `.nfo` beside a refused store is not its last piece, nor is an `.mkv`.
#[test]
fn a_member_whose_name_carries_a_known_type_is_never_a_piece() {
    assert!(!joins("lantern.nfo"));
    assert!(!joins("lantern.mkv"));
}

// A name that says nothing of what the member is leaves its bytes and the
// set to say it: the hash-named member is the store's last piece.
#[test]
fn a_hash_named_member_beside_a_refused_store_is_its_last_piece() {
    assert!(joins("5f0c9a1e7b2d4c38a6e1f9b0d2c4a7e3"));
}

// A member refused for a reason that disclaims it (no layer of the release,
// too deep, a shape nothing opens) is the posted file, whole: it is never
// the first piece of a split set, though the set proves it and a member
// beside it names nothing. One its object refused for what lies past it
// still is.
#[test]
fn a_member_refused_as_the_posted_file_is_never_the_first_piece() {
    let other = "5f0c9a1e7b2d4c38a6e1f9b0d2c4a7e3";
    for why in [Why::Packed, Why::TooDeep, Why::Unsupported] {
        assert!(!joins_refused(other, Some(why)), "{why:?}");
    }
    assert!(joins_refused(other, Some(Why::Unmapped)));
}

// A recovery set that refuses whatever it is handed.
struct RefusingSet;

impl Object for RefusingSet {
    fn unposted(&mut self, _: VolumeIndex, _: ByteRange) {}

    fn kind(&self) -> Kind {
        Kind::Par2
    }
    fn deliver(
        &mut self,
        _: Delivery<'_>,
        _: &mut dyn FnMut(&Placement, &mut Vec<u8>) -> bool,
        out: &mut Vec<Emit>,
    ) {
        out.push(Emit::Refuse(Why::Unrepairable));
    }
    fn volume_closed(&mut self, _: VolumeIndex, _: &mut Vec<Emit>) {}
    fn volume_lost(&mut self, _: VolumeIndex, _: ByteRange, _: &mut Vec<Emit>) {}
    fn wants(&self, _: &mut Vec<Seek>) {}
    fn pulled(&mut self, _: Seek, _: Pull, _: &mut Vec<Emit>) {}
    fn locate(&self, volume: VolumeIndex, range: ByteRange, out: &mut Vec<Placement>) {
        out.push(Placement::Envelope {
            envelope: volume.0,
            range,
        });
    }
    fn extent(&self, _: MemberId, _: ByteRange, _: &mut Vec<Extent>) -> Located {
        Located::Never
    }
    fn envelope(&self, _: u32, _: ByteRange) -> Option<Bytes> {
        None
    }
    fn recovery(&self) -> Option<&dyn Recovery> {
        Some(self)
    }
    fn recovery_mut(&mut self) -> Option<&mut dyn Recovery> {
        Some(self)
    }
}

impl Recovery for RefusingSet {
    // Its bind is the default, which changes nothing it covers.
    fn take_newly(&mut self, _: &mut Vec<(MemberId, String)>) {}

    fn rebuilds(&self) -> bool {
        true
    }
    fn carried_inside(&self) -> bool {
        false
    }
    fn outgrows_recovery(&self) -> bool {
        false
    }
    fn covers(&self, _: MemberId) -> bool {
        true
    }
    fn covers_some(&self) -> bool {
        true
    }
    fn feed(&mut self, _: MemberId, _: &[Segment]) {}
    fn verdicts(&self, _: MemberId, _: &mut Vec<SliceVerdict>) {}
    fn wants(&self, _: &mut Vec<Seek>) {}
    fn repair(&mut self, _: &mut dyn MemberAccess, _: crate::Budget) -> RepairOutcome {
        RepairOutcome::Clean
    }
    fn outcome(&self) -> Option<RepairOutcome> {
        None
    }
}

struct RefusingSets;

impl Builder for RefusingSets {
    fn build(&mut self, kind: Kind, id: ObjectId) -> Option<Box<dyn Object>> {
        match kind {
            Kind::Par2 => Some(Box::new(RefusingSet)),
            _ => Build.build(kind, id),
        }
    }

    fn read(&self, _: &Tree, _: NodeId, _: VolumeIndex, _: &Placement, _: &mut Vec<u8>) -> bool {
        false
    }
}

// A member posted under `name`, its front `front`, then handed `then` once
// bound, which the store doubles refuse on: whether the job spends it.
fn spent_after(name: &str, front: Vec<u8>, then: Option<&'static [u8]>) -> (bool, Option<bool>) {
    let mut tree = Tree::new();
    let member = tree.announce_root(name, None, &mut Vec::new());
    let posted = |offset, bytes: Vec<u8>| Posted {
        offset,
        bytes: Bytes::from(bytes).into(),
        provenance: Provenance::Fresh,
    };
    if !front.is_empty() {
        let at = front.len() as u64;
        tree.deliver(
            NodeId::ROOT,
            member,
            posted(0, front),
            &mut RefusingSets,
            &mut Vec::new(),
        );
        if let Some(then) = then {
            tree.deliver(
                NodeId::ROOT,
                member,
                posted(at, then.to_vec()),
                &mut RefusingSets,
                &mut Vec::new(),
            );
        }
        tree.bury(NodeId::ROOT, member, &mut Vec::new());
    }
    assert_eq!(
        tree.child_of(NodeId::ROOT, member),
        None,
        "{name} is bound to nothing"
    );
    let refuser = tree
        .refused_by(NodeId::ROOT, member)
        .map(|owner| tree.recovers(owner));
    (tree.spends(NodeId::ROOT, member), refuser)
}

// The job spends a member bound to nothing whose name says recovery data
// unless an object that is not a recovery object refused it: that object
// read it as its own, so it is a file of the delivery, as the status says.
#[test]
fn a_recovery_named_member_an_archive_refused_is_not_spent() {
    let mut rar = SIGNATURE.to_vec();
    rar.extend_from_slice(b"hello");
    // Refused by the archive its bytes founded: kept.
    assert_eq!(
        spent_after("show.p01", rar, Some(b"REFUSE")),
        (false, Some(false))
    );
    // Bound to nothing and refused by nothing: spent by its name.
    assert_eq!(
        spent_after("show.vol00+01.par2", Vec::new(), None),
        (true, None)
    );
    // Refused by the recovery set its bytes founded: spent.
    let mut par2 = b"PAR2\0PKT".to_vec();
    par2.extend_from_slice(&64u64.to_le_bytes());
    par2.resize(64, 0);
    assert_eq!(
        spent_after("show.vol00+01.par2", par2, None),
        (true, Some(true))
    );
}
