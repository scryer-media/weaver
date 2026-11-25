// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// What an object's delivery reached it as is how it placed those bytes when
// it took them. A later delivery of the same step, before the owner drains
// the effects, can give the object its map; the earlier delivery's effect
// still says the bytes were held, so the owner holds them and hands them
// again once the map places them, as it would had each delivery been a step
// of its own.

use super::*;
use crate::deliver::Posted;

const LENGTH: u64 = 40;
// Where the one member lies, once the map at the volume's end is read.
const MEMBER: ByteRange = ByteRange { start: 7, end: 30 };

// A pretend store that places nothing until the last byte of its volume
// arrives: the owner holds what it took before, and it says a new
// placement number once its map is read.
#[derive(Default)]
struct Late {
    mapped: bool,
}

impl Object for Late {
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
        if delivery.offset + delivery.bytes.len() as u64 == LENGTH {
            self.mapped = true;
            out.push(Emit::Member {
                member: MemberId(0),
                name: "a.txt".into(),
                length: Some(MEMBER.len()),
            });
        }
    }
    fn volume_closed(&mut self, _: VolumeIndex, _: &mut Vec<Emit>) {}
    fn volume_lost(&mut self, _: VolumeIndex, _: ByteRange, _: &mut Vec<Emit>) {}
    fn wants(&self, _: &mut Vec<Seek>) {}
    fn pulled(&mut self, _: Seek, _: Pull, _: &mut Vec<Emit>) {}
    fn locate(&self, _: VolumeIndex, range: ByteRange, out: &mut Vec<Placement>) {
        let Some(part) = range.intersect(MEMBER).filter(|_| self.mapped) else {
            out.push(Placement::Held { range });
            return;
        };
        if part.start > range.start {
            out.push(Placement::Held {
                range: ByteRange::new(range.start, part.start),
            });
        }
        out.push(Placement::Member {
            member: MemberId(0),
            range: part.shift(-(MEMBER.start as i64)),
        });
        if part.end < range.end {
            out.push(Placement::Held {
                range: ByteRange::new(part.end, range.end),
            });
        }
    }
    fn placement(&self) -> Option<u64> {
        Some(u64::from(self.mapped))
    }
    fn extent(&self, _: MemberId, _: ByteRange, _: &mut Vec<Extent>) -> Located {
        Located::NotYet
    }
    fn envelope(&self, _: u32, _: ByteRange) -> Option<Bytes> {
        None
    }
}

struct Building;

impl Builder for Building {
    fn build(&mut self, kind: Kind, _: ObjectId) -> Option<Box<dyn Object>> {
        (kind == Kind::RarStore).then(|| Box::new(Late::default()) as Box<dyn Object>)
    }
    fn read(&self, _: &Tree, _: NodeId, _: VolumeIndex, _: &Placement, _: &mut Vec<u8>) -> bool {
        false
    }
}

fn arrive(tree: &mut Tree, member: MemberId, range: ByteRange, effects: &mut Vec<Effect>) {
    let mut bytes = SIGNATURE.to_vec();
    bytes.resize(LENGTH as usize, b'x');
    tree.deliver(
        NodeId::ROOT,
        member,
        Posted {
            offset: range.start,
            bytes: bytes[range.start as usize..range.end as usize]
                .to_vec()
                .into(),
            provenance: Provenance::Fresh,
        },
        &mut Building,
        effects,
    );
}

fn took(effects: &[Effect], offset: u64) -> Took {
    effects
        .iter()
        .find_map(|effect| match effect {
            Effect::Reached {
                offset: at, took, ..
            } if *at == offset => Some(took.clone()),
            _ => None,
        })
        .expect("the delivery reached the store")
}

// Two deliveries in one step: the first, before the map, says its bytes
// were held under the number the store said then, though the store places
// them on its member by the time the step's effects drain; the second, which
// brought the map, says how the map places it, its member's bytes by their
// length alone.
#[test]
fn a_delivery_says_how_the_object_placed_it_when_it_took_it() {
    let mut tree = Tree::new();
    let volume = tree.announce_root("box.rar", Some(LENGTH), &mut Vec::new());
    let mut effects = Vec::new();
    arrive(&mut tree, volume, ByteRange::new(0, 20), &mut effects);
    let store = tree
        .child_of(NodeId::ROOT, volume)
        .expect("the store bound")
        .0;
    arrive(&mut tree, volume, ByteRange::new(20, LENGTH), &mut effects);

    assert_eq!(
        took(&effects, 0),
        Took {
            covered: 20,
            pieces: vec![Placement::Held {
                range: ByteRange::new(0, 20)
            }],
            number: Some(0),
        }
    );
    let mut now = Vec::new();
    tree.locate(store, VolumeIndex(0), ByteRange::new(0, 20), &mut now);
    assert!(
        now.iter()
            .any(|piece| matches!(piece, Placement::Member { .. })),
        "the map places the first delivery's bytes by now: {now:?}"
    );
    assert_eq!(tree.placement(store), Some(1));
    assert_eq!(
        took(&effects, 20),
        Took {
            covered: 20,
            pieces: vec![
                Placement::Member {
                    member: MemberId(0),
                    range: ByteRange::new(13, MEMBER.len()),
                },
                Placement::Held {
                    range: ByteRange::new(MEMBER.end, LENGTH),
                },
            ],
            number: Some(1),
        }
    );
}

// A delivery the map places on its member alone carries no pieces.
#[test]
fn a_delivery_placed_on_members_alone_carries_its_length_only() {
    let mut tree = Tree::new();
    let volume = tree.announce_root("box.rar", Some(LENGTH), &mut Vec::new());
    let mut effects = Vec::new();
    arrive(&mut tree, volume, ByteRange::new(0, 7), &mut effects);
    arrive(&mut tree, volume, ByteRange::new(30, LENGTH), &mut effects);
    effects.clear();
    arrive(&mut tree, volume, ByteRange::new(10, 20), &mut effects);
    assert_eq!(
        took(&effects, 10),
        Took {
            covered: 10,
            pieces: Vec::new(),
            number: Some(1),
        }
    );
}
