// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A loss told on bytes a store holds until its map is read waits with them,
// and goes down once the map places them. Bytes of that range that arrive
// before the map is read are no loss: the member they make is delivered as
// they came, and only what never arrived is marked lost.

use super::*;
use crate::deliver::Posted;

// The volume: the store's signature, a member `a.txt`, a member `b.nfo`,
// and the map that says where both lie.
fn a() -> ByteRange {
    ByteRange::new(7, 20)
}
fn b() -> ByteRange {
    ByteRange::new(20, 30)
}
const LENGTH: u64 = 40;

// A pretend store that holds every byte until the bytes of its map, at its
// volume's end, arrive; then it places what it holds on its members.
#[derive(Default)]
struct Mapped {
    held: Vec<(u64, Vec<u8>, Provenance)>,
    mapped: bool,
    placed: [u64; 2],
}

impl Mapped {
    fn spans() -> [(MemberId, ByteRange); 2] {
        [(MemberId(0), a()), (MemberId(1), b())]
    }

    fn place(&mut self, offset: u64, bytes: &[u8], provenance: Provenance, out: &mut Vec<Emit>) {
        let arrived = ByteRange::at(offset, bytes.len() as u64);
        for (member, span) in Self::spans() {
            let Some(part) = arrived.intersect(span) else {
                continue;
            };
            let slice = &bytes[(part.start - offset) as usize..(part.end - offset) as usize];
            out.push(Emit::Bytes {
                member,
                offset: part.start - span.start,
                bytes: slice.to_vec().into(),
                provenance,
            });
            let placed = &mut self.placed[member.0 as usize];
            *placed += part.len();
            if *placed == span.len() {
                out.push(Emit::MemberClosed(member));
            }
        }
    }
}

impl Object for Mapped {
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
        let (offset, provenance) = (delivery.offset, delivery.provenance);
        if self.mapped {
            self.place(offset, delivery.bytes, provenance, out);
            return;
        }
        self.held
            .push((offset, delivery.bytes.to_vec(), provenance));
        if offset + delivery.bytes.len() as u64 != LENGTH {
            return;
        }
        self.mapped = true;
        for (member, span) in Self::spans() {
            out.push(Emit::Member {
                member,
                name: ["a.txt", "b.nfo"][member.0 as usize].into(),
                length: Some(span.len()),
            });
        }
        for (offset, bytes, provenance) in std::mem::take(&mut self.held) {
            self.place(offset, &bytes, provenance, out);
        }
    }
    fn volume_closed(&mut self, _: VolumeIndex, _: &mut Vec<Emit>) {}
    fn volume_lost(&mut self, _: VolumeIndex, _: ByteRange, _: &mut Vec<Emit>) {}
    fn wants(&self, _: &mut Vec<Seek>) {}
    fn pulled(&mut self, _: Seek, _: Pull, _: &mut Vec<Emit>) {}
    fn locate(&self, _: VolumeIndex, range: ByteRange, out: &mut Vec<Placement>) {
        if !self.mapped {
            out.push(Placement::Held { range });
            return;
        }
        let mut at = range.start;
        for (member, span) in Self::spans() {
            let Some(part) = range.intersect(span) else {
                continue;
            };
            if part.start > at {
                out.push(Placement::Held {
                    range: ByteRange::new(at, part.start),
                });
            }
            out.push(Placement::Member {
                member,
                range: part.shift(-(span.start as i64)),
            });
            at = part.end;
        }
        if at < range.end {
            out.push(Placement::Held {
                range: ByteRange::new(at, range.end),
            });
        }
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
        (kind == Kind::RarStore).then(|| Box::new(Mapped::default()) as Box<dyn Object>)
    }
    fn read(&self, _: &Tree, _: NodeId, _: VolumeIndex, _: &Placement, _: &mut Vec<u8>) -> bool {
        false
    }
}

fn arrive(tree: &mut Tree, member: MemberId, range: ByteRange) {
    let mut bytes = SIGNATURE.to_vec();
    bytes.resize(LENGTH as usize, b'x');
    let bytes = bytes[range.start as usize..range.end as usize].to_vec();
    tree.deliver(
        NodeId::ROOT,
        member,
        Posted {
            offset: range.start,
            bytes: bytes.into(),
            provenance: Provenance::Fresh,
        },
        &mut Building,
        &mut Vec::new(),
    );
}

fn lost(record: &crate::Member, range: ByteRange) -> bool {
    record
        .coverage
        .overlapping(range)
        .any(|run| run.provenance == Provenance::Lost)
}

// The volume's front arrives and binds the store; the rest is told lost
// before the store has read its map; then `b.nfo` and the map arrive after
// all. `b.nfo` is whole and fresh; of `a.txt`, only the bytes that never
// came are lost.
#[test]
fn bytes_that_arrive_after_a_held_loss_are_delivered_as_they_came() {
    let mut tree = Tree::new();
    let volume = tree.announce_root("box.rar", Some(LENGTH), &mut Vec::new());
    arrive(&mut tree, volume, ByteRange::new(0, 12));
    let store = tree
        .child_of(NodeId::ROOT, volume)
        .expect("the store bound")
        .0;
    tree.damage(
        NodeId::ROOT,
        VolumeIndex(volume.0),
        ByteRange::new(12, LENGTH),
        &mut Vec::new(),
    );
    arrive(&mut tree, volume, ByteRange::new(b().start, LENGTH));

    let namespace = tree.namespace(store).expect("the store's namespace");
    let nfo = namespace.get(MemberId(1)).expect("b.nfo announced");
    assert!(nfo.is_whole(), "b.nfo arrived whole: {:?}", nfo.coverage);
    assert!(
        !lost(nfo, ByteRange::new(0, b().len())),
        "{:?}",
        nfo.coverage
    );
    let txt = namespace.get(MemberId(0)).expect("a.txt announced");
    assert!(
        !lost(txt, ByteRange::new(0, 12 - a().start)),
        "{:?}",
        txt.coverage
    );
    assert!(
        lost(txt, ByteRange::new(12 - a().start, a().len())),
        "{:?}",
        txt.coverage
    );
}

// The same loss with none of its bytes arriving: when the map is read
// another way, every byte of it is marked lost, `b.nfo`'s too.
#[test]
fn a_held_loss_whose_bytes_never_arrive_marks_every_member_it_reaches() {
    let mut tree = Tree::new();
    let volume = tree.announce_root("box.rar", Some(LENGTH), &mut Vec::new());
    arrive(&mut tree, volume, ByteRange::new(0, 12));
    let store = tree
        .child_of(NodeId::ROOT, volume)
        .expect("the store bound")
        .0;
    tree.damage(
        NodeId::ROOT,
        VolumeIndex(volume.0),
        ByteRange::new(12, b().end),
        &mut Vec::new(),
    );
    arrive(&mut tree, volume, ByteRange::new(b().end, LENGTH));

    let namespace = tree.namespace(store).expect("the store's namespace");
    let nfo = namespace.get(MemberId(1)).expect("b.nfo announced");
    assert!(
        lost(nfo, ByteRange::new(0, b().len())),
        "{:?}",
        nfo.coverage
    );
    let txt = namespace.get(MemberId(0)).expect("a.txt announced");
    assert!(
        lost(txt, ByteRange::new(12 - a().start, a().len())),
        "{:?}",
        txt.coverage
    );
}
