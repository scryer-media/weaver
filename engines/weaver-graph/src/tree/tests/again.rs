// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// An object asks for a range of its volume again: what of it was posted
// comes again, as it came, and every member it reached is open until then.

use std::sync::{Arc, Mutex};

use super::*;
use crate::deliver::Posted;
use crate::member::Run;

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
    // Asks for its first twenty bytes once more when its front comes the
    // second time, inside the replay of the first request.
    nest: bool,
    fronts: u32,
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
        if self.nest && delivery.offset == 0 {
            self.fronts += 1;
            if self.fronts == 2 {
                out.push(Emit::Again {
                    volume: delivery.volume,
                    range: ByteRange::new(0, 20),
                });
            }
        }
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
    fn extent(&self, _: MemberId, _: ByteRange, _: &mut Vec<Extent>) -> Located {
        Located::Never
    }
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

struct Trials(Log, bool);

impl Builder for Trials {
    fn build(&mut self, kind: Kind, _: ObjectId) -> Option<Box<dyn Object>> {
        (kind == Kind::RarStore).then(|| {
            Box::new(Trial {
                log: Arc::clone(&self.0),
                given: Vec::new(),
                asked: false,
                nest: self.1,
                fronts: 0,
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
            bytes: Bytes::copy_from_slice(bytes).into(),
            provenance,
        },
        builder,
        effects,
    );
    answer(tree, builder, effects);
}

// The owner answers each request for bytes again once the effects before
// it are applied, and what the answer raises the same way.
fn answer(tree: &mut Tree, builder: &mut Trials, effects: &mut Vec<Effect>) {
    let mut at = 0;
    while at < effects.len() {
        if let Effect::Again { node, ranges } = &effects[at] {
            let (node, ranges) = (*node, ranges.clone());
            let mut raised = Vec::new();
            tree.again(node, &ranges, builder, &mut raised);
            effects.splice(at + 1..at + 1, raised);
        }
        at += 1;
    }
}

// The store's volume posted with a hole at 12..15 and its tail short of the
// range asked for: each posted run comes again with its own provenance, and
// nothing that was never posted.
#[test]
fn a_range_asked_for_again_is_delivered_again_as_it_was_posted() {
    let log = Log::default();
    let mut builder = Trials(Arc::clone(&log), false);
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

// A range that never came is recorded as the loss it is at every depth,
// apart from bytes beside it that came damaged: asked for again, the
// damaged bytes come again as they were posted, and nothing of the loss.
#[test]
fn a_loss_beside_damaged_bytes_is_never_said_again_as_bytes() {
    let log = Log::default();
    let mut builder = Trials(Arc::clone(&log), false);
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
    tree.damage(
        NodeId::ROOT,
        VolumeIndex(show.0),
        ByteRange::new(20, 24),
        &mut effects,
    );
    deliver(
        &mut tree,
        &mut builder,
        show,
        (15, b"CHECK", Provenance::Damaged),
        &mut effects,
    );
    let runs = |tree: &Tree, node: NodeId, member: MemberId| -> Vec<Run> {
        tree.namespace(node)
            .and_then(|namespace| namespace.get(member))
            .map(|record| record.coverage.runs().copied().collect())
            .unwrap_or_default()
    };
    for (node, member) in [(NodeId::ROOT, show), (child, MemberId(0))] {
        let held = runs(&tree, node, member);
        assert!(
            held.contains(&Run {
                range: ByteRange::new(15, 20),
                provenance: Provenance::Damaged,
            }),
            "{held:?}"
        );
        assert!(
            held.contains(&Run {
                range: ByteRange::new(20, 24),
                provenance: Provenance::Lost,
            }),
            "{held:?}"
        );
    }
    let mut said: Vec<(u64, Vec<u8>, Provenance)> = Vec::new();
    tree.again_read(
        child,
        &[(VolumeIndex(0), ByteRange::new(0, 24))],
        &builder,
        &mut |_, posted| said.push((posted.offset, posted.bytes.to_vec(), posted.provenance)),
    );
    assert_eq!(
        said,
        vec![
            (0, front, Provenance::Fresh),
            (15, b"CHECK".to_vec(), Provenance::Damaged),
        ]
    );
}

// A member asked for again whole is read back a piece at a time, none
// longer than `AGAIN_PIECE`, each piece handed on before the next is read,
// and all of it uncovered only once every piece is read.
#[test]
fn a_large_range_asked_for_again_is_read_back_in_bounded_pieces() {
    let log = Log::default();
    let mut builder = Trials(Arc::clone(&log), false);
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
    let body = vec![0x71; (2 * AGAIN_PIECE + 13) as usize];
    deliver(
        &mut tree,
        &mut builder,
        show,
        (front.len() as u64, &body, Provenance::Fresh),
        &mut effects,
    );
    let length = (front.len() + body.len()) as u64;
    let mut pieces = Vec::new();
    tree.again_read(
        child,
        &[(VolumeIndex(0), ByteRange::new(0, length))],
        &builder,
        &mut |_, posted| {
            let piece = ByteRange::at(posted.offset, posted.bytes.len() as u64);
            pieces.push(piece);
        },
    );
    assert!(
        pieces.iter().all(|piece| piece.len() <= AGAIN_PIECE),
        "{pieces:?}"
    );
    assert_eq!(pieces.first().map(|piece| piece.start), Some(0));
    assert!(pieces.windows(2).all(|pair| pair[0].end == pair[1].start));
    assert_eq!(pieces.last().map(|piece| piece.end), Some(length));
    let record = tree
        .namespace(child)
        .and_then(|namespace| namespace.get(MemberId(0)))
        .expect("member");
    assert!(record.coverage.runs().next().is_none());
}

// An owner that applies each replayed piece's effects before the next piece
// goes, a request raised inside a replay answered at once, as the
// namespace writer answers them.
fn answer_at_once(tree: &mut Tree, builder: &mut Trials, effects: Vec<Effect>) {
    for effect in effects {
        let Effect::Again { node, ranges } = effect else {
            continue;
        };
        let mut staged = Vec::new();
        let serial = tree.again_read(node, &ranges, builder, &mut |volume, posted| {
            staged.push((volume, posted));
        });
        for (volume, posted) in staged {
            let mut raised = Vec::new();
            tree.replay_again(serial, node, volume, posted, builder, &mut raised);
            answer_at_once(tree, builder, raised);
        }
        tree.again_done();
    }
}

// A request raised while an earlier one is being said again reads back and
// says again what the earlier one has yet to say: the earlier one does not
// say those bytes a second time.
#[test]
fn a_replay_raised_inside_a_replay_is_said_once() {
    let log = Log::default();
    let mut builder = Trials(Arc::clone(&log), true);
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
    effects.clear();
    tree.deliver(
        NodeId::ROOT,
        show,
        Posted {
            offset: 15,
            bytes: Bytes::from_static(b"CHECK").into(),
            provenance: Provenance::Fresh,
        },
        &mut builder,
        &mut effects,
    );
    answer_at_once(&mut tree, &mut builder, effects);
    let heard = log.lock().expect("log").clone();
    let checks = heard
        .iter()
        .filter(|heard| matches!(heard, Heard::Bytes(15, _, _)))
        .count();
    // Once as it came, and once by the request raised inside the first
    // one's replay, which read it back before the first one said it.
    assert_eq!(checks, 2, "{heard:?}");
    let fronts = heard
        .iter()
        .filter(|heard| matches!(heard, Heard::Bytes(0, _, _)))
        .count();
    // As it came, by the first request, and by the one raised inside it.
    assert_eq!(fronts, 3, "{heard:?}");
}
