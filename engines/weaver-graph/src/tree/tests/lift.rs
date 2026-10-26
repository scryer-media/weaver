// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A seek lifted through a store that has placed only part of the range: the
// placed part is asked of the store's own volumes, and the rest waits for
// the store to read the header that places it.

use super::*;
use crate::deliver::Posted;

// How long the store's first volume is.
const FIRST_LEN: u64 = 100;

// A pretend store: a seven-byte header on its first volume, then one member,
// the rest of that volume and on into a second volume whose header it has
// not read. It places the member's bytes in its first volume and says the
// rest is not located yet.
struct Front {
    announced: bool,
}

impl Object for Front {
    fn unposted(&mut self, _: VolumeIndex, _: ByteRange) {}

    fn kind(&self) -> Kind {
        Kind::RarStore
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
                name: "inner.bin".into(),
                length: None,
            });
        }
        let skip = HEADER
            .saturating_sub(delivery.offset)
            .min(delivery.bytes.len() as u64);
        if skip == delivery.bytes.len() as u64 {
            return;
        }
        out.push(Emit::Bytes {
            member: MemberId(0),
            offset: delivery.offset + skip - HEADER,
            bytes: delivery.bytes[skip as usize..].to_vec().into(),
            provenance: delivery.provenance,
        });
    }
    fn volume_closed(&mut self, _: VolumeIndex, _: &mut Vec<Emit>) {}
    fn volume_lost(&mut self, _: VolumeIndex, _: ByteRange, _: &mut Vec<Emit>) {}
    fn wants(&self, _: &mut Vec<Seek>) {}
    fn pulled(&mut self, _: Seek, _: Pull, _: &mut Vec<Emit>) {}
    fn locate(&self, _: VolumeIndex, range: ByteRange, out: &mut Vec<Placement>) {
        let start = range.start.max(HEADER);
        out.push(Placement::Member {
            member: MemberId(0),
            range: ByteRange::new(start, range.end.max(start)).shift(-(HEADER as i64)),
        });
    }
    fn extent(&self, _: MemberId, range: ByteRange, out: &mut Vec<Extent>) -> Located {
        let placed = ByteRange::new(0, FIRST_LEN - HEADER);
        if let Some(part) = range.intersect(placed) {
            out.push(Extent {
                volume: VolumeIndex(0),
                range: part.shift(HEADER as i64),
            });
        }
        match range.end <= placed.end {
            true => Located::Whole,
            false => Located::NotYet,
        }
    }
    fn envelope(&self, _: u32, _: ByteRange) -> Option<Bytes> {
        None
    }
}

struct Fronts;

impl Builder for Fronts {
    fn build(&mut self, kind: Kind, _: ObjectId) -> Option<Box<dyn Object>> {
        (kind == Kind::RarStore).then(|| Box::new(Front { announced: false }) as Box<dyn Object>)
    }
    fn read(&self, _: &Tree, _: NodeId, _: VolumeIndex, _: &Placement, _: &mut Vec<u8>) -> bool {
        false
    }
}

// A store bound at the root whose member is a store of its own.
fn nested() -> (Tree, MemberId, NodeId) {
    let mut tree = Tree::new();
    let first = tree.announce_root("outer.rar", None, &mut Vec::new());
    let mut bytes = SIGNATURE.to_vec();
    bytes.extend_from_slice(SIGNATURE);
    bytes.extend_from_slice(b"hello");
    tree.deliver(
        NodeId::ROOT,
        first,
        Posted {
            offset: 0,
            bytes: bytes.into(),
            provenance: Provenance::Fresh,
        },
        &mut Fronts,
        &mut Vec::new(),
    );
    let outer = tree.child_of(NodeId::ROOT, first).expect("outer bound").0;
    let inner = tree.child_of(outer, MemberId(0)).expect("inner bound").0;
    (tree, first, inner)
}

#[test]
fn a_seek_the_parent_has_partly_placed_asks_the_placed_part_and_is_not_located_yet() {
    let (mut tree, first, inner) = nested();
    let mut root = Root(Vec::new());
    let seek = Seek::data(
        VolumeIndex(0),
        ByteRange::new(80, 120),
        crate::seek::Weight(1),
    );
    assert_eq!(tree.lift(inner, seek, &mut root), Pull::Unlocated);
    assert_eq!(
        root.0,
        vec![Seek {
            volume: VolumeIndex(first.0),
            range: ByteRange::new(87, FIRST_LEN),
            ..seek
        }]
    );
}

#[test]
fn a_seek_the_parent_has_placed_whole_is_asked_whole() {
    let (mut tree, first, inner) = nested();
    let mut root = Root(Vec::new());
    let seek = Seek::data(
        VolumeIndex(0),
        ByteRange::new(10, 20),
        crate::seek::Weight(1),
    );
    assert_eq!(tree.lift(inner, seek, &mut root), Pull::Moved(1));
    assert_eq!(
        root.0,
        vec![Seek {
            volume: VolumeIndex(first.0),
            range: ByteRange::new(17, 27),
            ..seek
        }]
    );
}

#[test]
fn a_deferral_the_parent_has_not_located_parks_nothing() {
    let (mut tree, _, inner) = nested();
    let mut root = Root(Vec::new());
    let early = Seek::deferred(VolumeIndex(0), ByteRange::new(FIRST_LEN, FIRST_LEN + 10));
    assert_eq!(tree.lift(inner, early, &mut root), Pull::Moved(0));
    assert!(root.0.is_empty());
}
