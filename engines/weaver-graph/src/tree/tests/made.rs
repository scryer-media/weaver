// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// An object that places some members itself and runs for the rest. A
// repair that rewrote its volume replays the damaged members it placed,
// through its map, and leaves those its run makes to the run's restart. A
// store, which places every member, and an engine, whose run makes every
// member, are as they were.

use std::sync::Arc;

use super::*;
use crate::deliver::Posted;
use crate::engine::{Budget, Sink, Volumes};

// The run of the test's engine face: it makes nothing.
struct Idle;

impl Extraction for Idle {
    fn run(self: Box<Self>, _: Arc<dyn Volumes>, _: &mut Sink<'_>, _: Budget) {}
}

// Each member lies at ten bytes a member in the volume. With `made`, the
// object has an engine face, and its run makes the members it marks.
struct Hybrid {
    made: Option<Vec<bool>>,
}

impl Object for Hybrid {
    fn unposted(&mut self, _: VolumeIndex, _: ByteRange) {}

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
    fn locate(&self, _: VolumeIndex, _: ByteRange, _: &mut Vec<Placement>) {}
    fn extent(&self, member: MemberId, range: ByteRange, out: &mut Vec<Extent>) -> Located {
        out.push(Extent {
            volume: VolumeIndex(0),
            range: range.shift(i64::from(member.0) * 10),
        });
        Located::Whole
    }
    fn envelope(&self, _: u32, _: ByteRange) -> Option<Bytes> {
        None
    }
    fn engine(&self) -> Option<&dyn Engine> {
        self.made.is_some().then_some(self as &dyn Engine)
    }
    fn engine_mut(&mut self) -> Option<&mut dyn Engine> {
        self.made.is_some().then_some(self as &mut dyn Engine)
    }
}

impl Engine for Hybrid {
    fn start(&mut self) -> Box<dyn Extraction> {
        Box::new(Idle)
    }
    fn makes(&self, member: MemberId) -> bool {
        self.made
            .as_ref()
            .and_then(|made| made.get(member.0 as usize).copied())
            .unwrap_or(true)
    }
}

struct Builds(Option<Vec<bool>>);

impl Builder for Builds {
    fn build(&mut self, kind: Kind, _: ObjectId) -> Option<Box<dyn Object>> {
        (kind == Kind::RarStore).then(|| {
            Box::new(Hybrid {
                made: self.0.clone(),
            }) as Box<dyn Object>
        })
    }
    fn read(&self, _: &Tree, _: NodeId, _: VolumeIndex, _: &Placement, _: &mut Vec<u8>) -> bool {
        false
    }
}

// The root `show.rar` bound to the object, which holds two members of six
// bytes, each with a checksum and every byte damaged.
fn damaged(made: Option<Vec<bool>>) -> (Tree, MemberId, NodeId) {
    let mut tree = Tree::new();
    let mut effects = Vec::new();
    let mut builds = Builds(made);
    let root = tree.announce_root("show.rar", None, &mut effects);
    let mut bytes = SIGNATURE.to_vec();
    bytes.extend_from_slice(b"hello");
    tree.deliver(
        NodeId::ROOT,
        root,
        Posted {
            offset: 0,
            bytes: bytes.into(),
            provenance: Provenance::Fresh,
        },
        &mut builds,
        &mut effects,
    );
    let child = tree.child_of(NodeId::ROOT, root).expect("bound").0;
    for (member, name) in [(MemberId(0), "stored.mkv"), (MemberId(1), "coded.mkv")] {
        tree.emitted(
            child,
            vec![
                Emit::Member {
                    member,
                    name: name.into(),
                    length: Some(6),
                },
                Emit::Checksum { member, crc32: 0 },
            ],
            &mut builds,
            &mut effects,
        );
        tree.namespace_mut(child)
            .unwrap()
            .get_mut(member)
            .unwrap()
            .coverage
            .write(ByteRange::new(0, 6), Provenance::Damaged);
    }
    (tree, root, child)
}

#[test]
fn a_damaged_member_the_object_placed_replays_and_one_its_run_makes_does_not() {
    let (tree, root, child) = damaged(Some(vec![false, true]));
    assert_eq!(
        tree.rejected_inputs(NodeId::ROOT, [root]),
        vec![(child, vec![(VolumeIndex(0), ByteRange::new(0, 6))])]
    );
}

#[test]
fn a_store_replays_every_damaged_member_and_an_engine_none() {
    let (tree, root, child) = damaged(None);
    assert_eq!(
        tree.rejected_inputs(NodeId::ROOT, [root]),
        vec![(
            child,
            vec![
                (VolumeIndex(0), ByteRange::new(0, 6)),
                (VolumeIndex(0), ByteRange::new(10, 16)),
            ]
        )]
    );
    let (tree, root, _) = damaged(Some(vec![true, true]));
    assert!(tree.rejected_inputs(NodeId::ROOT, [root]).is_empty());
}

// The members the object's run makes, by the tree's record.
fn run_makes(tree: &Tree, node: NodeId) -> Vec<MemberId> {
    tree.namespace(node)
        .expect("the namespace")
        .members()
        .filter(|member| !member.own)
        .map(|member| member.id)
        .collect()
}

// The object lent out: the tree's record answers as it did while the object
// was in its node, and a member its run names meanwhile is the run's.
#[test]
fn a_lent_hybrid_places_its_own_and_its_run_makes_the_rest_as_before() {
    let (mut tree, _, child) = damaged(Some(vec![false, true]));
    assert!(tree.places_own(child));
    assert_eq!(run_makes(&tree, child), vec![MemberId(1)]);
    let _object = tree.lend_for_test(child).expect("lent");
    assert!(tree.lent(child));
    assert!(tree.places_own(child), "still a hybrid while out");
    assert_eq!(run_makes(&tree, child), vec![MemberId(1)]);
    let mut effects = Vec::new();
    tree.emitted(
        child,
        vec![Emit::Member {
            member: MemberId(2),
            name: "late.mkv".into(),
            length: Some(6),
        }],
        &mut Builds(None),
        &mut effects,
    );
    assert_eq!(
        run_makes(&tree, child),
        vec![MemberId(1), MemberId(2)],
        "a member named while out is the run's"
    );
}
