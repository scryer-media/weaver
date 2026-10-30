// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The sets a namespace lists are read off the tree's index of them. Whatever
// binds, moves, lends, retires or removes a set, or removes the namespace a
// set moved into, the index lists what a walk over the namespaces finds, in
// the walk's order; and a rebuilt tree lists them again.

use serde::{Deserialize, Serialize};

use super::*;
use crate::deliver::Posted;
use crate::facts::{Durable, Facts, Restore, RestoreError};
use crate::recovery::{MemberAccess, RepairOutcome};
use crate::{Segment, SliceVerdict};

// A pretend recovery object that covers nothing and keeps its facts.
#[derive(Serialize, Deserialize)]
struct Set;

impl Durable for Set {
    const VERSION: u16 = 1;
}

impl Object for Set {
    // It keeps no record of what it said of its posted bytes.
    fn unposted(&mut self, _: crate::VolumeIndex, _: crate::ByteRange) {}

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
    fn facts(&self) -> Facts {
        Facts::of(Kind::Par2, self)
    }
}

impl Recovery for Set {
    fn covers(&self, _: MemberId) -> bool {
        false
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

// The store doubles of the parent module, and a set for PAR2.
struct Sets;

impl Builder for Sets {
    fn build(&mut self, kind: Kind, id: ObjectId) -> Option<Box<dyn Object>> {
        match kind {
            Kind::Par2 => Some(Box::new(Set)),
            _ => Build.build(kind, id),
        }
    }

    fn read(&self, _: &Tree, _: NodeId, _: VolumeIndex, _: &Placement, _: &mut Vec<u8>) -> bool {
        false
    }
}

impl Restore for Sets {
    fn restore(&self, facts: &Facts) -> Result<Box<dyn Object>, RestoreError> {
        Ok(Box::new(facts.read::<Set>(Kind::Par2)?))
    }
}

// The walk the index answers in place of: every live recovery object bound
// in the namespace or in the one above it whose home is the namespace.
fn walked(tree: &Tree, node: NodeId) -> Vec<NodeId> {
    let mut found = tree.bound_below(node);
    if let Some((parent, _)) = tree.volumes(node) {
        found.extend(tree.bound_below(parent));
    }
    found.retain(|set| tree.is_live(*set) && tree.recovers(*set) && tree.home(*set) == Some(node));
    found
}

#[track_caller]
fn listed_as_walked(tree: &Tree) {
    for node in tree.nodes.keys() {
        assert_eq!(
            tree.recovery_nodes(*node),
            walked(tree, *node),
            "the sets of {node:?}"
        );
    }
}

fn deliver(tree: &mut Tree, member: MemberId, offset: u64, bytes: &[u8]) {
    tree.deliver(
        NodeId::ROOT,
        member,
        Posted {
            offset,
            bytes: Bytes::copy_from_slice(bytes),
            provenance: Provenance::Fresh,
        },
        &mut Sets,
        &mut Vec::new(),
    );
}

// A store on `show`, which has had its first twelve bytes.
fn store_on(tree: &mut Tree, show: MemberId) -> NodeId {
    let mut front = SIGNATURE.to_vec();
    front.extend_from_slice(b"hello");
    deliver(tree, show, 0, &front);
    tree.child_of(NodeId::ROOT, show)
        .expect("a store is bound")
        .0
}

// A set posted at the root under `name`.
pub(super) fn posted(tree: &mut Tree, name: &str) -> NodeId {
    let member = tree.announce_root(name, None, &mut Vec::new());
    // A PAR2 packet header stating its own length, followed by nothing.
    let mut header = b"PAR2\0PKT".to_vec();
    header.extend_from_slice(&64u64.to_le_bytes());
    header.resize(64, 0);
    deliver(tree, member, 0, &header);
    tree.child_of(NodeId::ROOT, member)
        .expect("a set is bound")
        .0
}

fn window(tree: &mut Tree, node: NodeId, member: MemberId, range: ByteRange) -> NodeId {
    tree.attach_window(
        node,
        (member, range),
        (Kind::Par2, &Handed::default()),
        Vec::new(),
        &mut Sets,
        &mut Vec::new(),
    )
    .expect("a window is built")
}

#[test]
fn every_namespace_lists_the_sets_a_walk_finds_after_each_change() {
    let mut tree = Tree::new();
    let show = tree.announce_root("show.rar", None, &mut Vec::new());
    let store = store_on(&mut tree, show);
    listed_as_walked(&tree);
    let set = posted(&mut tree, "show.par2");
    listed_as_walked(&tree);
    let above = window(&mut tree, NodeId::ROOT, show, ByteRange::new(10, 30));
    let below = window(&mut tree, store, MemberId(0), ByteRange::new(0, 4));
    listed_as_walked(&tree);
    assert_eq!(tree.recovery_nodes(NodeId::ROOT), vec![set, above]);
    assert_eq!(tree.recovery_nodes(store), vec![below]);

    // The posted set moves into the store's namespace, after its own.
    tree.move_recovery(set, store);
    listed_as_walked(&tree);
    assert_eq!(tree.recovery_nodes(NodeId::ROOT), vec![above]);
    assert_eq!(tree.recovery_nodes(store), vec![below, set]);

    // Lent out and taken back, it stays where it is.
    let object = tree.lend(set).expect("the set is lent");
    listed_as_walked(&tree);
    assert_eq!(tree.recovery_nodes(store), vec![below, set]);
    tree.take_back(set, object, &mut Sets, &mut Vec::new());
    listed_as_walked(&tree);

    // The store's member starts over: the store and both windows go, and
    // the set is homed where it was posted again.
    tree.retract(NodeId::ROOT, show, &mut Sets, &mut Vec::new());
    listed_as_walked(&tree);
    assert_eq!(tree.recovery_nodes(NodeId::ROOT), vec![set]);

    // A store bound again, a window in it, the set moved in once more; then
    // the store refuses, and it and its window retire.
    let again = store_on(&mut tree, show);
    let inside = window(&mut tree, again, MemberId(0), ByteRange::new(0, 4));
    tree.move_recovery(set, again);
    listed_as_walked(&tree);
    assert_eq!(tree.recovery_nodes(again), vec![inside, set]);
    deliver(&mut tree, show, 12, b"REFUSE");
    assert!(!tree.is_live(again));
    listed_as_walked(&tree);
    assert_eq!(tree.recovery_nodes(again), vec![set]);
}

#[test]
fn a_rebuilt_tree_lists_its_sets_again() {
    let mut tree = Tree::new();
    let leaf = tree.announce_root("show.bin", None, &mut Vec::new());
    deliver(&mut tree, leaf, 0, b"plain bytes, no signature");
    let set = posted(&mut tree, "show.par2");
    let inside = window(&mut tree, NodeId::ROOT, leaf, ByteRange::new(2, 8));
    assert_eq!(tree.recovery_nodes(NodeId::ROOT), vec![set, inside]);
    let rebuilt = Tree::rebuild(&tree.records(), &Sets).expect("the tree rebuilds");
    listed_as_walked(&rebuilt);
    assert_eq!(rebuilt.recovery_nodes(NodeId::ROOT), vec![set, inside]);
}
