// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// An object asks for a range of its volume again: what of it was posted
// comes again, as it came, and every member it reached is open until then.

use std::sync::{Arc, Mutex};

use super::*;
use crate::deliver::Posted;

// What the object heard, in order.
type Log = Arc<Mutex<Vec<Heard>>>;

#[derive(Clone, Debug, PartialEq, Eq)]
enum Heard {
    Bytes(u64, Vec<u8>, Provenance),
    Unposted(ByteRange),
}

// A pretend store whose one member is its volume, each byte flipped so the
// member is no container of its own. The first time bytes saying `CHECK`
// reach it, it asks for its first twenty bytes again; it rebuilds its
// posted bytes from what it was given.
struct Trial {
    log: Log,
    given: Vec<Option<u8>>,
    asked: bool,
}

impl Trial {
    fn hear(&self, heard: Heard) {
        self.log.lock().expect("log").push(heard);
    }
}

impl Object for Trial {
    fn unposted(&mut self, _: VolumeIndex, range: ByteRange) {
        self.hear(Heard::Unposted(range));
    }

    fn kind(&self) -> Kind {
        Kind::RarStore
    }
    fn deliver(
        &mut self,
        delivery: Delivery<'_>,
        _: &mut dyn FnMut(&Placement, &mut Vec<u8>) -> bool,
        out: &mut Vec<Emit>,
    ) {
        self.hear(Heard::Bytes(
            delivery.offset,
            delivery.bytes.to_vec(),
            delivery.provenance,
        ));
        let end = delivery.offset as usize + delivery.bytes.len();
        if self.given.len() < end {
            self.given.resize(end, None);
        }
        for (at, byte) in delivery.bytes.iter().enumerate() {
            self.given[delivery.offset as usize + at] = Some(*byte);
        }
        if self.given.len() == end && delivery.offset == 0 {
            out.push(Emit::Member {
                member: MemberId(0),
                name: "inner.bin".into(),
                length: None,
            });
        }
        out.push(Emit::Bytes {
            member: MemberId(0),
            offset: delivery.offset,
            bytes: delivery.bytes.iter().map(|byte| byte ^ 0x20).collect(),
            provenance: delivery.provenance,
        });
        if !self.asked && delivery.bytes.windows(5).any(|window| window == b"CHECK") {
            self.asked = true;
            out.push(Emit::MemberClosed(MemberId(0)));
            out.push(Emit::Again {
                volume: delivery.volume,
                range: ByteRange::new(0, 20),
            });
        }
    }
    fn volume_closed(&mut self, _: VolumeIndex, _: &mut Vec<Emit>) {}
    fn volume_lost(&mut self, _: VolumeIndex, _: ByteRange, _: &mut Vec<Emit>) {}
    fn wants(&self, _: &mut Vec<Seek>) {}
    fn pulled(&mut self, _: Seek, _: Pull, _: &mut Vec<Emit>) {}
    fn locate(&self, _: VolumeIndex, range: ByteRange, out: &mut Vec<Placement>) {
        out.push(Placement::Member {
            member: MemberId(0),
            range,
        });
    }
    fn extent(&self, _: MemberId, _: ByteRange, _: &mut Vec<Extent>) {}
    fn envelope(&self, _: u32, _: ByteRange) -> Option<Bytes> {
        None
    }
    fn posted(
        &self,
        _: VolumeIndex,
        range: ByteRange,
        _: &mut dyn FnMut(&Placement, &mut Vec<u8>) -> bool,
        into: &mut Vec<u8>,
    ) -> bool {
        let Some(bytes) = self.given.get(range.start as usize..range.end as usize) else {
            return false;
        };
        let bytes: Option<Vec<u8>> = bytes.iter().copied().collect();
        bytes.is_some_and(|bytes| {
            into.extend_from_slice(&bytes);
            true
        })
    }
}

struct Trials(Log);

impl Builder for Trials {
    fn build(&mut self, kind: Kind, _: ObjectId) -> Option<Box<dyn Object>> {
        (kind == Kind::RarStore).then(|| {
            Box::new(Trial {
                log: Arc::clone(&self.0),
                given: Vec::new(),
                asked: false,
            }) as Box<dyn Object>
        })
    }
    fn read(&self, _: &Tree, _: NodeId, _: VolumeIndex, _: &Placement, _: &mut Vec<u8>) -> bool {
        false
    }
}

fn deliver(
    tree: &mut Tree,
    builder: &mut Trials,
    member: MemberId,
    (offset, bytes, provenance): (u64, &[u8], Provenance),
    effects: &mut Vec<Effect>,
) {
    tree.deliver(
        NodeId::ROOT,
        member,
        Posted {
            offset,
            bytes: Bytes::copy_from_slice(bytes),
            provenance,
        },
        builder,
        effects,
    );
}

// The store's volume posted with a hole at 12..15 and its tail short of the
// range asked for: each posted run comes again with its own provenance, and
// nothing that was never posted.
#[test]
fn a_range_asked_for_again_is_delivered_again_as_it_was_posted() {
    let log = Log::default();
    let mut builder = Trials(Arc::clone(&log));
    let mut tree = Tree::new();
    let mut effects = Vec::new();
    let show = tree.announce_root("show.rar", None, &mut effects);
    let mut front = SIGNATURE.to_vec();
    front.extend_from_slice(b"hello");
    deliver(
        &mut tree,
        &mut builder,
        show,
        (0, &front, Provenance::Fresh),
        &mut effects,
    );
    let child = tree.child_of(NodeId::ROOT, show).expect("bound").0;
    deliver(
        &mut tree,
        &mut builder,
        show,
        (15, b"CHECK", Provenance::Damaged),
        &mut effects,
    );
    let heard = log.lock().expect("log").clone();
    assert_eq!(
        heard,
        vec![
            Heard::Bytes(0, front.clone(), Provenance::Fresh),
            Heard::Bytes(15, b"CHECK".to_vec(), Provenance::Damaged),
            Heard::Unposted(ByteRange::new(0, 12)),
            Heard::Unposted(ByteRange::new(15, 20)),
            Heard::Bytes(0, front.clone(), Provenance::Fresh),
            Heard::Bytes(15, b"CHECK".to_vec(), Provenance::Damaged),
        ]
    );
    // The member the bytes reached was closed, opened again by the request,
    // and holds what came again.
    let record = tree
        .namespace(child)
        .and_then(|namespace| namespace.get(MemberId(0)))
        .expect("member");
    assert!(!record.closed);
    assert!(record.coverage.covered(ByteRange::new(0, 12)));
    assert!(record.coverage.covered(ByteRange::new(15, 20)));
    assert!(!record.coverage.covered(ByteRange::new(12, 15)));
    let leaves = effects
        .iter()
        .filter(|effect| {
            matches!(
                effect,
                Effect::WriteLeaf { node, offset: 0, .. } if *node == child
            )
        })
        .count();
    assert_eq!(leaves, 2, "the member's front is written again");
}
