// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use std::sync::Arc;

use bytes::Bytes;
use serde::{Deserialize, Serialize};

use super::super::{Builder, Tree};
use crate::deliver::{Delivery, Emit, Posted, Provenance, Why};
use crate::engine::{Engine, Extraction, Sink, Volumes};
use crate::facts::{Durable, Facts, Restore, RestoreError};
use crate::member::MemberId;
use crate::object::{Extent, Kind, Located, Object, ObjectId, Placement, Standing};
use crate::records::{NodeState, TreeRecord};
use crate::seek::{Pull, Seek, Weight};
use crate::tree::NodeId;
use crate::volume::{ByteRange, VolumeIndex};

const XZ: &[u8] = b"\xfd7zXZ\x00";
const GZIP: &[u8] = b"\x1f\x8b\x08\x00\x00\x00\x00\x00\x00\x03";
const PAYLOAD: &[u8] = b"payload";
// Where the archive's front ends: its damage lands here, its replay after.
const FRONT_END: u64 = (XZ.len() + GZIP.len() + PAYLOAD.len()) as u64;

// A pretend wrapper: its own magic, then one member that is everything
// after it, under a fixed name. As an engine it bids for the volume after
// the one its run reached, and refuses itself, quiet, while its run asks.
#[derive(Serialize, Deserialize)]
struct Wrap {
    kind: Kind,
    inner: String,
    announced: bool,
}

impl Durable for Wrap {
    const VERSION: u16 = 1;
}

impl Wrap {
    fn header(&self) -> u64 {
        match self.kind {
            Kind::Xz => XZ.len() as u64,
            _ => GZIP.len() as u64,
        }
    }
}

impl Object for Wrap {
    // It keeps no record of what it said of its posted bytes.
    fn unposted(&mut self, _: crate::VolumeIndex, _: crate::ByteRange) {}

    fn kind(&self) -> Kind {
        self.kind
    }
    fn deliver(
        &mut self,
        delivery: Delivery<'_>,
        _: &mut dyn FnMut(&Placement, &mut Vec<u8>) -> bool,
        out: &mut Vec<Emit>,
    ) {
        if !self.announced {
            self.announced = true;
            out.push(Emit::Member {
                member: MemberId(0),
                name: self.inner.clone(),
                length: None,
            });
        }
        let skip = self.header().saturating_sub(delivery.offset) as usize;
        if skip < delivery.bytes.len() {
            out.push(Emit::Bytes {
                member: MemberId(0),
                offset: delivery.offset + skip as u64 - self.header(),
                bytes: Bytes::copy_from_slice(&delivery.bytes[skip..]).into(),
                provenance: delivery.provenance,
            });
        }
    }
    fn volume_closed(&mut self, _: VolumeIndex, _: &mut Vec<Emit>) {}
    fn volume_lost(&mut self, _: VolumeIndex, _: ByteRange, _: &mut Vec<Emit>) {}
    fn quiet(&mut self, standing: &dyn Standing, out: &mut Vec<Emit>) {
        if standing.asking() {
            out.push(Emit::Refuse(Why::EncryptedHeader));
        }
    }
    fn wants(&self, _: &mut Vec<Seek>) {}
    fn pulled(&mut self, _: Seek, _: Pull, _: &mut Vec<Emit>) {}
    fn locate(&self, _: VolumeIndex, range: ByteRange, out: &mut Vec<Placement>) {
        let start = range.start.max(self.header());
        out.push(Placement::Member {
            member: MemberId(0),
            range: ByteRange::new(start, range.end.max(start)).shift(-(self.header() as i64)),
        });
    }
    fn extent(&self, _: MemberId, range: ByteRange, out: &mut Vec<Extent>) -> Located {
        out.push(Extent {
            volume: VolumeIndex(0),
            range: range.shift(self.header() as i64),
        });
        Located::Whole
    }
    fn envelope(&self, _: u32, _: ByteRange) -> Option<Bytes> {
        None
    }
    fn engine(&self) -> Option<&dyn Engine> {
        Some(self)
    }
    fn engine_mut(&mut self) -> Option<&mut dyn Engine> {
        Some(self)
    }
    fn facts(&self) -> Facts {
        Facts::of(self.kind, self)
    }
}

impl Engine for Wrap {
    fn start(&mut self) -> Box<dyn Extraction> {
        Box::new(Idle)
    }
    fn bid(&self, reached: VolumeIndex, out: &mut Vec<Seek>) {
        let next = VolumeIndex(reached.0 + 1);
        out.push(Seek::data(next, ByteRange::new(0, u64::MAX), Weight(1)));
    }
}

struct Idle;

impl Extraction for Idle {
    fn run(self: Box<Self>, _: Arc<dyn Volumes>, _: &mut Sink<'_>, _: crate::Budget) {}
}

pub(crate) struct Objects;

impl Builder for Objects {
    fn build(&mut self, kind: Kind, _: ObjectId) -> Option<Box<dyn Object>> {
        let inner = match kind {
            Kind::Xz => "inner.gz",
            Kind::Gzip => "leaf.bin",
            _ => return None,
        };
        Some(Box::new(Wrap {
            kind,
            inner: inner.to_owned(),
            announced: false,
        }))
    }

    fn read(&self, _: &Tree, _: NodeId, _: VolumeIndex, _: &Placement, _: &mut Vec<u8>) -> bool {
        false
    }
}

impl Restore for Objects {
    fn restore(&self, facts: &Facts) -> Result<Box<dyn Object>, RestoreError> {
        Ok(Box::new(facts.read::<Wrap>(facts.kind)?))
    }
}

// Root, an xz wrapper below it, a gzip wrapper below that: three levels,
// with a root leaf that takes the name the deepest member wants, so the
// ledger's suffix has to come back too, and damage in the coverage.
pub(crate) fn three_levels() -> (Tree, MemberId) {
    let mut tree = Tree::new();
    let mut effects = Vec::new();
    let leaf = tree.announce_root("leaf.bin", Some(4), &mut effects);
    let archive = tree.announce_root("a.xz", None, &mut effects);
    let deliver = |tree: &mut Tree, member, offset, bytes: &[u8], provenance| {
        tree.deliver(
            NodeId::ROOT,
            member,
            Posted {
                offset,
                bytes: Bytes::copy_from_slice(bytes).into(),
                provenance,
            },
            &mut Objects,
            &mut Vec::new(),
        );
    };
    deliver(&mut tree, leaf, 0, b"leaf", Provenance::Fresh);
    let front = [XZ, GZIP, PAYLOAD].concat();
    deliver(&mut tree, archive, 0, &front, Provenance::Fresh);
    deliver(&mut tree, archive, FRONT_END, b"bad", Provenance::Damaged);
    (tree, archive)
}

#[test]
fn a_rebuilt_tree_has_the_records_it_was_exported_from() {
    let (tree, _) = three_levels();
    let record = tree.records();
    let kinds: Vec<_> = record.nodes.iter().map(|node| node.kind).collect();
    assert_eq!(kinds, vec![None, Some(Kind::Xz), Some(Kind::Gzip)]);

    let rebuilt = Tree::rebuild(&record, &Objects).expect("rebuilds");
    let again = rebuilt.records();
    for (before, after) in record.nodes.iter().zip(&again.nodes) {
        assert_eq!(before, after, "node {:?}", before.node);
    }
    assert_eq!(record, again);

    let deepest = NodeId(2);
    let name = |tree: &Tree| {
        tree.namespace(deepest)
            .and_then(|namespace| namespace.get(MemberId(0)))
            .map(|member| member.output_name.clone())
    };
    assert_eq!(name(&rebuilt), name(&tree));
    assert_ne!(name(&rebuilt).as_deref(), Some("leaf.bin"));
    assert_eq!(
        rebuilt.child_of(NodeId::ROOT, MemberId(1)),
        tree.child_of(NodeId::ROOT, MemberId(1))
    );
    assert!(
        record.nodes[0]
            .coverage
            .iter()
            .any(|run| run.provenance == Provenance::Damaged)
    );
}

#[test]
fn a_rebuilt_tree_takes_the_replay_as_the_live_one_does() {
    let (mut tree, archive) = three_levels();
    let mut rebuilt = Tree::rebuild(&tree.records(), &Objects).expect("rebuilds");
    let replay = |tree: &mut Tree| {
        let mut effects = Vec::new();
        tree.deliver(
            NodeId::ROOT,
            archive,
            Posted {
                offset: FRONT_END + 3,
                bytes: Bytes::from_static(b" more").into(),
                provenance: Provenance::Fresh,
            },
            &mut Objects,
            &mut effects,
        );
        let later = tree.announce_root("b.xz", None, &mut effects);
        (effects, later)
    };
    assert_eq!(replay(&mut rebuilt), replay(&mut tree));
    assert_eq!(rebuilt.records(), tree.records());
}

#[test]
fn a_record_that_no_tree_could_have_written_is_refused() {
    let (tree, _) = three_levels();
    let record = tree.records();

    let mut backwards = record.clone();
    backwards.nodes.swap(1, 2);
    assert!(matches!(
        Tree::rebuild(&backwards, &Objects),
        Err(RestoreError::Tree {
            node: NodeId(2),
            ..
        })
    ));

    let mut reused = record.clone();
    reused.next_node = 2;
    assert!(Tree::rebuild(&reused, &Objects).is_err());

    let mut unsupported = record.clone();
    unsupported.nodes[2].facts = Some(Facts::unsupported(Kind::Gzip));
    assert_eq!(
        Tree::rebuild(&unsupported, &Objects).err(),
        Some(RestoreError::Unsupported { kind: Kind::Gzip })
    );

    let mut overlapping: TreeRecord = record.clone();
    let run = overlapping.nodes[0].coverage[0];
    overlapping.nodes[0].coverage.push(run);
    assert!(Tree::rebuild(&overlapping, &Objects).is_err());
}

#[test]
fn a_retired_node_comes_back_retired_with_what_it_gave_up() {
    let (tree, archive) = three_levels();
    let mut record = tree.records();
    record.nodes[1].state = NodeState::Retired;
    record.nodes[0].members[1].given_up = Some((NodeId(1), VolumeIndex(0)));
    let rebuilt = Tree::rebuild(&record, &Objects).expect("rebuilds");
    assert_eq!(
        rebuilt.retired(NodeId::ROOT, archive),
        Some((NodeId(1), VolumeIndex(0)))
    );
    assert_eq!(rebuilt.records(), record);
}

// A refusal the owner buried leaves its member settled; the record says so,
// so a rebuilt tree offers it to no later object either.
#[test]
fn a_buried_refusal_comes_back_settled() {
    let (tree, archive) = three_levels();
    let mut record = tree.records();
    record.nodes[0].members[1].owner = None;
    record.nodes[0].members[1].buried = true;
    let rebuilt = Tree::rebuild(&record, &Objects).expect("rebuilds");
    assert_eq!(rebuilt.retired(NodeId::ROOT, archive), None);
    assert_eq!(
        rebuilt.records().nodes[0].members[1],
        record.nodes[0].members[1]
    );

    let mut both = record.clone();
    both.nodes[0].members[1].given_up = Some((NodeId(1), VolumeIndex(0)));
    assert!(
        Tree::rebuild(&both, &Objects).is_err(),
        "given up and buried at once"
    );
}

// A refused object that announced a member and never closed it, buried: the
// member stays in its namespace, settled, and the record carries it back,
// so a rebuilt tree still reads that the object left a member open, and the
// member holds no name.
#[test]
fn a_member_a_buried_refusal_left_open_comes_back_buried() {
    let (mut tree, archive) = three_levels();
    let mut effects = Vec::new();
    tree.emitted(
        NodeId(1),
        vec![Emit::Refuse(Why::Unsupported)],
        &mut Objects,
        &mut effects,
    );
    tree.bury(NodeId::ROOT, archive, &mut Vec::new());
    assert!(tree.left_open(NodeId::ROOT, archive));
    let open: Vec<(NodeId, MemberId)> = [NodeId(1), NodeId(2)]
        .into_iter()
        .flat_map(|node| {
            tree.namespace(node)
                .into_iter()
                .flat_map(|namespace| namespace.members().map(|member| member.id))
                .map(move |member| (node, member))
                .collect::<Vec<_>>()
        })
        .filter(|(node, member)| tree.buried_member(*node, *member))
        .collect();
    assert!(!open.is_empty(), "a member left open stays, buried");

    let record = tree.records();
    let rebuilt = Tree::rebuild(&record, &Objects).expect("rebuilds");
    assert!(rebuilt.left_open(NodeId::ROOT, archive));
    for (node, member) in &open {
        assert!(rebuilt.buried_member(*node, *member), "{node:?} {member:?}");
    }
    assert_eq!(rebuilt.records(), record);
}

// A run's progress is the tree's, never its node's: reporting how far a
// run read and that it asked marks no node and changes no record, the
// object's facts among them. Rebuilt from those records, the tree bids and
// rules from progress of its own, which starts empty, and a run it starts
// answers what an earlier one asked.
#[test]
fn a_run_reports_progress_without_marking_its_node_or_its_facts() {
    let bids = |tree: &Tree| {
        let mut out = Vec::new();
        tree.collect_wants(&mut out);
        out.into_iter()
            .map(|(node, seek)| (node, seek.volume))
            .collect::<Vec<_>>()
    };
    let refused = |tree: &mut Tree, node| {
        tree.quiet(node, &mut Objects, &mut Vec::new());
        tree.record(node).and_then(|record| record.reason)
    };
    let (mut tree, _) = three_levels();
    let engine = NodeId(1);
    let record = tree.records();
    let at = tree.generation();
    tree.run_reached(engine, VolumeIndex(0));
    tree.run_asked(engine);
    assert_eq!(tree.generation(), at);
    assert_eq!(tree.changed_since(at).count(), 0, "progress marks no node");
    assert_eq!(tree.records(), record, "no record carries progress");
    assert_eq!(bids(&tree), vec![(engine, VolumeIndex(1))]);

    let mut rebuilt = Tree::rebuild(&record, &Objects).expect("rebuilds");
    assert!(bids(&rebuilt).is_empty(), "a rebuilt tree knows no run yet");
    rebuilt.run_reached(engine, VolumeIndex(0));
    assert_eq!(bids(&rebuilt), bids(&tree));
    rebuilt.run_asked(engine);
    assert!(rebuilt.start(engine).is_some());
    assert_eq!(refused(&mut rebuilt, engine), None, "the new run asks anew");

    assert_eq!(refused(&mut tree, engine), Some(Why::EncryptedHeader));
}
