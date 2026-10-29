// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A repair that rewrites a member reopens the objects whose refusal it
// answers: the one holding the member, by the object's own claim on it, and
// no other, whatever it refused for.

use super::*;
use crate::deliver::Posted;

// A pretend store that holds the volumes of its own base name: `.rar` is
// its first, `.r00` and `.r01` the next.
#[derive(Default)]
struct Based {
    base: Option<String>,
}

impl Object for Based {
    // It keeps no record of what it said of its posted bytes.
    fn unposted(&mut self, _: crate::VolumeIndex, _: crate::ByteRange) {}

    fn kind(&self) -> Kind {
        Kind::RarStore
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
    fn bound(&mut self, volume: VolumeIndex, name: &str, _: &[u8], _: Option<u64>) {
        if volume == VolumeIndex(0) {
            self.base = name.strip_suffix(".rar").map(str::to_owned);
        }
    }
    fn claim(&mut self, name: &str, _: &[u8], _: Option<u64>) -> Option<VolumeIndex> {
        let (base, extension) = name.rsplit_once('.')?;
        if Some(base) != self.base.as_deref() {
            return None;
        }
        match extension {
            "r00" => Some(VolumeIndex(1)),
            "r01" => Some(VolumeIndex(2)),
            _ => None,
        }
    }
}

struct Stores;

impl Builder for Stores {
    fn build(&mut self, kind: Kind, _: ObjectId) -> Option<Box<dyn Object>> {
        (kind == Kind::RarStore).then(|| Box::new(Based::default()) as Box<dyn Object>)
    }

    fn read(&self, _: &Tree, _: NodeId, _: VolumeIndex, _: &Placement, _: &mut Vec<u8>) -> bool {
        false
    }
}

fn post(tree: &mut Tree, name: &str, bytes: &'static [u8]) -> MemberId {
    let member = tree.announce_root(name, None, &mut Vec::new());
    tree.deliver(
        NodeId::ROOT,
        member,
        Posted {
            offset: 0,
            bytes: Bytes::from_static(bytes),
            provenance: Provenance::Fresh,
        },
        &mut Stores,
        &mut Vec::new(),
    );
    member
}

// Two sets refused, each for a reason that may mean a volume it never had.
// The member the repair rewrote is a volume of the first alone, so only the
// first is opened again; the second keeps its refusal and its volume.
#[test]
fn a_repair_reopens_only_the_refused_set_that_takes_the_rewritten_member() {
    let mut tree = Tree::new();
    let lantern = post(&mut tree, "lantern.rar", SIGNATURE);
    let harbour = post(&mut tree, "harbour.rar", SIGNATURE);
    let (first, _) = tree.child_of(NodeId::ROOT, lantern).unwrap();
    let (second, _) = tree.child_of(NodeId::ROOT, harbour).unwrap();
    let mut effects = Vec::new();
    tree.emitted(
        first,
        vec![Emit::Refuse(Why::Unmapped)],
        &mut Stores,
        &mut effects,
    );
    tree.emitted(
        second,
        vec![Emit::Refuse(Why::FirstBytesLost)],
        &mut Stores,
        &mut effects,
    );
    // A volume of the first set, heard after it refused, is refused with it.
    let rewritten = post(&mut tree, "lantern.r01", b"w");
    assert_eq!(
        tree.volumes(first)
            .map(|(_, volumes)| volumes.get(&VolumeIndex(2)).copied()),
        Some(Some(rewritten))
    );
    let reopened = tree.reopen(NodeId::ROOT, rewritten, &mut Stores, &mut Vec::new());
    assert_eq!(reopened, vec![rewritten, lantern]);
    assert_eq!(
        tree.kind(second),
        Some(Kind::RarStore),
        "the second set stays"
    );
    assert_eq!(
        tree.volumes(second)
            .map(|(_, volumes)| volumes.get(&VolumeIndex(0)).copied()),
        Some(Some(harbour))
    );
}
