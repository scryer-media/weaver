// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// What a window's owner handed it, kept by its node: an object that takes
// the node back after a loan, the one lent or one rebuilt from its facts in
// its place, is handed it before it hears anything.

use std::sync::{Arc, Mutex};

use super::*;
use crate::deliver::Posted;
use crate::facts::{Durable, Facts};
use crate::recovery::{MemberAccess, RepairOutcome};
use crate::{Segment, SliceVerdict};

// What the window's object heard, in order.
type Log = Arc<Mutex<Vec<Heard>>>;

#[derive(Clone, Debug, PartialEq, Eq)]
enum Heard {
    // What it was handed, when it is a candidate.
    Handed(Option<String>),
    Sized(u64),
    Bytes(u64, Vec<u8>, Provenance),
}

#[derive(Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
struct ReaderFacts {
    sized: usize,
    delivered: usize,
}

impl Durable for ReaderFacts {
    const VERSION: u16 = 1;
}

// A pretend recovery object that reads its range by what it was handed.
struct Reader(Log);

impl Reader {
    fn hear(&self, heard: Heard) {
        self.0.lock().expect("log").push(heard);
    }
}

impl Object for Reader {
    fn unposted(&mut self, _: VolumeIndex, _: ByteRange) {}

    fn kind(&self) -> Kind {
        Kind::Par2
    }
    fn facts(&self) -> Facts {
        let heard = self.0.lock().expect("log");
        Facts::of(
            Kind::Par2,
            &ReaderFacts {
                sized: heard
                    .iter()
                    .filter(|event| matches!(event, Heard::Sized(_)))
                    .count(),
                delivered: heard
                    .iter()
                    .filter(|event| matches!(event, Heard::Bytes(..)))
                    .count(),
            },
        )
    }
    fn deliver(
        &mut self,
        delivery: Delivery<'_>,
        _: &mut dyn FnMut(&Placement, &mut Vec<u8>) -> bool,
        _: &mut Vec<Emit>,
    ) {
        self.hear(Heard::Bytes(
            delivery.offset,
            delivery.bytes.to_vec(),
            delivery.provenance,
        ));
    }
    fn sized(&mut self, _: VolumeIndex, length: u64, _: &mut Vec<Emit>) {
        self.hear(Heard::Sized(length));
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
    fn handed(&mut self, handed: &Handed) {
        self.hear(Heard::Handed(handed.read::<String>().cloned()));
    }
    fn recovery(&self) -> Option<&dyn Recovery> {
        Some(self)
    }
    fn recovery_mut(&mut self) -> Option<&mut dyn Recovery> {
        Some(self)
    }
}

impl Recovery for Reader {
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

// The store doubles of the parent module, and a reader for recovery data.
struct Reading(Log);

impl Builder for Reading {
    fn build(&mut self, kind: Kind, id: ObjectId) -> Option<Box<dyn Object>> {
        match kind {
            Kind::Par2 => Some(Box::new(Reader(Arc::clone(&self.0)))),
            _ => Build.build(kind, id),
        }
    }

    fn read(&self, _: &Tree, _: NodeId, _: VolumeIndex, _: &Placement, _: &mut Vec<u8>) -> bool {
        false
    }
}

const WINDOW: ByteRange = ByteRange { start: 10, end: 30 };

fn heard(log: &Log) -> Vec<Heard> {
    std::mem::take(&mut *log.lock().expect("log"))
}

fn candidate(secret: &str) -> Handed {
    Handed::new(secret.to_string())
}

fn reader_window() -> (Tree, NodeId, Log) {
    let (mut tree, first, _, _) = bound_tree();
    let log = Log::default();
    let window = tree
        .attach_window(
            NodeId::ROOT,
            (first, WINDOW),
            (Kind::Par2, &candidate("candidate")),
            Vec::new(),
            &mut Reading(Arc::clone(&log)),
            &mut Vec::new(),
        )
        .expect("a window is built");
    heard(&log);
    (tree, window, log)
}

#[test]
fn a_delivery_marks_opaque_facts_once_without_marking_its_members() {
    for length in [None, Some(31)] {
        let (mut tree, window, log) = reader_window();
        let before = tree.own_record(window).unwrap().facts.unwrap();
        let at = tree.generation();
        let arrived = Arrived {
            volume: VolumeIndex(0),
            offset: 2,
            bytes: Bytes::from_static(b"body").into(),
            provenance: Provenance::Fresh,
        };
        let mut effects = Vec::new();
        tree.deliver_to_object(window, &arrived, length, &mut Reading(log), &mut effects);

        assert!(effects.is_empty(), "the object emits nothing");
        assert_eq!(tree.generation(), at + 1);
        assert_eq!(
            tree.marked_since(at).collect::<Vec<_>>(),
            vec![Mark::Node(window)]
        );
        let after = tree.own_record(window).unwrap().facts.unwrap();
        assert_ne!(before, after);
        assert_eq!(
            after.read::<ReaderFacts>(Kind::Par2).unwrap(),
            ReaderFacts {
                sized: usize::from(length.is_some()),
                delivered: 1,
            }
        );
    }
}

#[test]
fn a_delivery_with_no_live_object_marks_nothing() {
    for absent in 0..3 {
        let (mut tree, window, log) = reader_window();
        match absent {
            0 => {
                tree.nodes.remove(&window);
            }
            1 => {
                tree.nodes.object_out(window);
            }
            _ => tree.nodes.get_mut(&window).unwrap().retired = true,
        }
        let at = tree.generation();
        let arrived = Arrived {
            volume: VolumeIndex(0),
            offset: 2,
            bytes: Bytes::from_static(b"body").into(),
            provenance: Provenance::Fresh,
        };
        tree.deliver_to_object(
            window,
            &arrived,
            Some(31),
            &mut Reading(Arc::clone(&log)),
            &mut Vec::new(),
        );
        assert_eq!(tree.generation(), at, "case {absent}");
        assert!(heard(&log).is_empty(), "case {absent}");
    }
}

#[test]
fn a_retired_lent_object_keeps_deliveries_in_order_without_marking_facts() {
    let (mut tree, window, log) = reader_window();
    let object = tree.lend(window).expect("the window lends");
    tree.nodes.get_mut(&window).unwrap().retired = true;
    let before = tree.own_record(window).unwrap();
    let at = tree.generation();
    for (offset, bytes, provenance, length) in [
        (2, b"first".as_slice(), Provenance::Repaired, 21),
        (7, b"next".as_slice(), Provenance::Damaged, 22),
    ] {
        let arrived = Arrived {
            volume: VolumeIndex(0),
            offset,
            bytes: Bytes::copy_from_slice(bytes).into(),
            provenance,
        };
        tree.deliver_to_object(
            window,
            &arrived,
            Some(length),
            &mut Reading(Arc::clone(&log)),
            &mut Vec::new(),
        );
    }
    assert_eq!(tree.generation(), at);
    assert_eq!(tree.own_record(window).unwrap(), before);
    assert!(heard(&log).is_empty());

    tree.nodes.get_mut(&window).unwrap().retired = false;
    tree.take_back(
        window,
        object,
        &mut Reading(Arc::clone(&log)),
        &mut Vec::new(),
    );
    assert_eq!(
        heard(&log),
        vec![
            Heard::Handed(Some("candidate".to_string())),
            Heard::Sized(21),
            Heard::Bytes(2, b"first".to_vec(), Provenance::Repaired),
            Heard::Sized(22),
            Heard::Bytes(7, b"next".to_vec(), Provenance::Damaged),
        ]
    );
}

// The window is lent; meanwhile bytes reach it and its owner hands it a
// second candidate. What comes back is a fresh object, as one rebuilt from
// its facts is: it is handed the last candidate before the bytes that
// waited, and the lent one it replaces never heard the second.
#[test]
fn an_object_taken_back_in_a_lent_windows_place_is_handed_the_last_candidate_first() {
    let (mut tree, first, _, _) = bound_tree();
    let log = Log::default();
    let window = tree
        .attach_window(
            NodeId::ROOT,
            (first, WINDOW),
            (Kind::Par2, &candidate("first-candidate")),
            Vec::new(),
            &mut Reading(Arc::clone(&log)),
            &mut Vec::new(),
        )
        .expect("a window is built");
    assert_eq!(
        heard(&log),
        vec![Heard::Handed(Some("first-candidate".to_string()))]
    );

    let lent = tree.lend(window).expect("the window lends");
    tree.deliver(
        NodeId::ROOT,
        first,
        Posted {
            offset: 12,
            bytes: Bytes::from(vec![7u8; 4]).into(),
            provenance: Provenance::Fresh,
        },
        &mut Reading(Arc::clone(&log)),
        &mut Vec::new(),
    );
    let again = tree.attach_window(
        NodeId::ROOT,
        (first, WINDOW),
        (Kind::Par2, &candidate("second-candidate")),
        Vec::new(),
        &mut Reading(Arc::clone(&log)),
        &mut Vec::new(),
    );
    assert_eq!(again, None);
    assert!(heard(&log).is_empty(), "a lent window hears nothing");
    drop(lent);

    let rebuilt: Box<dyn Object> = Box::new(Reader(Arc::clone(&log)));
    tree.take_back(
        window,
        rebuilt,
        &mut Reading(Arc::clone(&log)),
        &mut Vec::new(),
    );
    assert_eq!(
        heard(&log),
        vec![
            Heard::Handed(Some("second-candidate".to_string())),
            Heard::Sized(20),
            Heard::Bytes(2, vec![7u8; 4], Provenance::Fresh),
        ]
    );
}

// A member whose front is all a member keeps comes while a window beside it
// is lent: it queues behind the loan, undecided. Damage past its front,
// settled as the server's namespace writer settles a member whose front can
// say no more, leaves it queued: the window, back, is asked with the rest,
// and the front, judged, founds the store it signs.
#[test]
fn a_full_front_queued_behind_a_lent_window_waits_through_damage() {
    let (mut tree, first, child, _) = bound_tree();
    let log = Log::default();
    let window = tree
        .attach_window(
            NodeId::ROOT,
            (first, WINDOW),
            (Kind::Par2, &candidate("candidate")),
            Vec::new(),
            &mut Reading(Arc::clone(&log)),
            &mut Vec::new(),
        )
        .expect("a window is built");
    let lent = tree.lend(window).expect("the window lends");
    let member = tree.announce_root("fresh.rar", None, &mut Vec::new());
    let mut front = SIGNATURE.to_vec();
    front.resize(crate::member::FIRST_BYTES, 0);
    tree.deliver(
        NodeId::ROOT,
        member,
        Posted {
            offset: 0,
            bytes: Bytes::from(front).into(),
            provenance: Provenance::Fresh,
        },
        &mut Reading(Arc::clone(&log)),
        &mut Vec::new(),
    );
    assert!(tree.is_undecided(NodeId::ROOT, member), "it queues");
    assert!(!tree.front_to_come(NodeId::ROOT, member), "full");
    let past = crate::member::FIRST_BYTES as u64;
    let mut effects = Vec::new();
    tree.damage(
        NodeId::ROOT,
        VolumeIndex(member.0),
        ByteRange::new(past, past + 10),
        &mut effects,
    );
    if tree.is_undecided(NodeId::ROOT, member) && !tree.front_to_come(NodeId::ROOT, member) {
        tree.settle_leaf(NodeId::ROOT, member, &mut effects);
    }
    assert!(
        tree.is_undecided(NodeId::ROOT, member),
        "it waits for the loan"
    );
    tree.take_back(window, lent, &mut Reading(Arc::clone(&log)), &mut effects);
    assert!(!tree.is_undecided(NodeId::ROOT, member));
    let (owner, volume) = tree.child_of(NodeId::ROOT, member).expect("a store");
    assert_ne!(owner, child, "a store of its own");
    assert_eq!(volume, VolumeIndex(0));
}

// A member shorter than any verdict's bytes, whole, queues behind a lent
// window and closes there, settled as the server's namespace writer settles
// a member that closed undecided. Its front is all of it, so it says all a
// sniff can: the window, back, is asked with the rest, and the front founds
// the store it signs.
#[test]
fn a_whole_member_shorter_than_a_verdict_founds_its_store_after_a_loan() {
    let (mut tree, first, child, _) = bound_tree();
    let log = Log::default();
    let window = tree
        .attach_window(
            NodeId::ROOT,
            (first, WINDOW),
            (Kind::Par2, &candidate("candidate")),
            Vec::new(),
            &mut Reading(Arc::clone(&log)),
            &mut Vec::new(),
        )
        .expect("a window is built");
    let lent = tree.lend(window).expect("the window lends");
    let member = tree.announce_root("tiny.rar", None, &mut Vec::new());
    let mut whole = SIGNATURE.to_vec();
    whole.extend_from_slice(b"tiny");
    assert!(whole.len() < crate::VERDICT_BYTES, "shorter than a verdict");
    tree.deliver(
        NodeId::ROOT,
        member,
        Posted {
            offset: 0,
            bytes: Bytes::from(whole).into(),
            provenance: Provenance::Fresh,
        },
        &mut Reading(Arc::clone(&log)),
        &mut Vec::new(),
    );
    assert!(tree.is_undecided(NodeId::ROOT, member), "it queues");
    let mut effects = Vec::new();
    tree.close(
        NodeId::ROOT,
        member,
        &mut Reading(Arc::clone(&log)),
        &mut effects,
    );
    assert!(!tree.front_to_come(NodeId::ROOT, member), "whole");
    if tree.is_undecided(NodeId::ROOT, member) {
        tree.settle_leaf(NodeId::ROOT, member, &mut effects);
    }
    assert!(
        tree.is_undecided(NodeId::ROOT, member),
        "it waits for the loan"
    );
    tree.take_back(window, lent, &mut Reading(Arc::clone(&log)), &mut effects);
    assert!(!tree.is_undecided(NodeId::ROOT, member));
    let (owner, volume) = tree.child_of(NodeId::ROOT, member).expect("a store");
    assert_ne!(owner, child, "a store of its own");
    assert_eq!(volume, VolumeIndex(0));
}
