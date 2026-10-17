// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use bytes::Bytes;
use serde::{Deserialize, Serialize};

use super::super::{Builder, Tree};
use crate::deliver::{Delivery, Emit, Provenance};
use crate::facts::{Durable, Facts, Restore, RestoreError};
use crate::member::MemberId;
use crate::object::{Extent, Kind, Object, ObjectId, Placement};
use crate::records::{NodeState, TreeRecord};
use crate::seek::{Pull, Seek};
use crate::tree::NodeId;
use crate::volume::{ByteRange, VolumeIndex};

const XZ: &[u8] = b"\xfd7zXZ\x00";
const GZIP: &[u8] = b"\x1f\x8b";

// A pretend wrapper: its own magic, then one member that is everything
// after it, under a fixed name.
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
    fn kind(&self) -> Kind {
        self.kind
    }
    fn deliver(&mut self, delivery: Delivery<'_>, out: &mut Vec<Emit>) {
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
                bytes: Bytes::copy_from_slice(&delivery.bytes[skip..]),
                provenance: delivery.provenance,
            });
        }
    }
    fn volume_closed(&mut self, _: VolumeIndex, _: &mut Vec<Emit>) {}
    fn volume_lost(&mut self, _: VolumeIndex, _: ByteRange, _: &mut Vec<Emit>) {}
    fn wants(&mut self, _: &mut Vec<Seek>) {}
    fn pulled(&mut self, _: Seek, _: Pull, _: &mut Vec<Emit>) {}
    fn locate(&self, _: VolumeIndex, range: ByteRange, out: &mut Vec<Placement>) {
        let start = range.start.max(self.header());
        out.push(Placement::Member {
            member: MemberId(0),
            range: ByteRange::new(start, range.end.max(start)).shift(-(self.header() as i64)),
        });
    }
    fn extent(&self, _: MemberId, range: ByteRange, out: &mut Vec<Extent>) {
        out.push(Extent {
            volume: VolumeIndex(0),
            range: range.shift(self.header() as i64),
        });
    }
    fn envelope(&self, _: u32, _: ByteRange) -> Option<Bytes> {
        None
    }
    fn facts(&self) -> Facts {
        Facts::of(self.kind, self)
    }
}

struct Objects;

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
}

impl Restore for Objects {
    fn restore(&self, facts: &Facts) -> Result<Box<dyn Object>, RestoreError> {
        Ok(Box::new(facts.read::<Wrap>(facts.kind)?))
    }
}

// Root, an xz wrapper below it, a gzip wrapper below that: three levels,
// with a root leaf that takes the name the deepest member wants, so the
// ledger's suffix has to come back too, and damage in the coverage.
fn three_levels() -> (Tree, MemberId) {
    let mut tree = Tree::new();
    let mut effects = Vec::new();
    let leaf = tree.announce_root("leaf.bin", Some(4), &mut effects);
    let archive = tree.announce_root("a.xz", None, &mut effects);
    let deliver = |tree: &mut Tree, member, offset, bytes: &[u8], provenance| {
        tree.deliver(
            NodeId::ROOT,
            member,
            offset,
            Bytes::copy_from_slice(bytes),
            provenance,
            &mut Objects,
            &mut Vec::new(),
        );
    };
    deliver(&mut tree, leaf, 0, b"leaf", Provenance::Fresh);
    let front = [XZ, GZIP, b"payload"].concat();
    deliver(&mut tree, archive, 0, &front, Provenance::Fresh);
    deliver(&mut tree, archive, 15, b"bad", Provenance::Damaged);
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
            18,
            Bytes::from_static(b" more"),
            Provenance::Fresh,
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
