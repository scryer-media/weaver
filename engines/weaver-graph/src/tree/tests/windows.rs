// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A window: a child bound to a range of a member its owner keeps.

use std::sync::{Arc, Mutex};

use super::*;
use crate::recovery::{MemberAccess, RepairOutcome};
use crate::seek::Weight;
use crate::{Segment, SliceVerdict};

// What the window's object heard, in order.
type Log = Arc<Mutex<Vec<Heard>>>;

#[derive(Clone, Debug, PartialEq, Eq)]
enum Heard {
    Bytes(u64, Vec<u8>),
    Sized(u64),
    Closed,
    Lost(ByteRange),
}

// A pretend recovery object that asks for its volume from byte 5 on.
struct Tape(Log);

impl Tape {
    fn hear(&self, heard: Heard) {
        self.0.lock().expect("log").push(heard);
    }
}

impl Object for Tape {
    fn kind(&self) -> Kind {
        Kind::Par2
    }
    fn deliver(&mut self, delivery: Delivery<'_>, _: &mut Vec<Emit>) {
        self.hear(Heard::Bytes(delivery.offset, delivery.bytes.to_vec()));
    }
    fn sized(&mut self, _: VolumeIndex, length: u64, _: &mut Vec<Emit>) {
        self.hear(Heard::Sized(length));
    }
    fn volume_closed(&mut self, _: VolumeIndex, _: &mut Vec<Emit>) {
        self.hear(Heard::Closed);
    }
    fn volume_lost(&mut self, _: VolumeIndex, range: ByteRange, _: &mut Vec<Emit>) {
        self.hear(Heard::Lost(range));
    }
    fn wants(&mut self, out: &mut Vec<Seek>) {
        out.push(Seek::recovery(
            VolumeIndex(0),
            ByteRange::new(5, u64::MAX),
            Weight::FIRST,
        ));
    }
    fn pulled(&mut self, _: Seek, _: Pull, _: &mut Vec<Emit>) {}
    fn locate(&self, volume: VolumeIndex, range: ByteRange, out: &mut Vec<Placement>) {
        out.push(Placement::Envelope {
            envelope: volume.0,
            range,
        });
    }
    fn extent(&self, _: MemberId, _: ByteRange, _: &mut Vec<Extent>) {}
    fn envelope(&self, _: u32, _: ByteRange) -> Option<Bytes> {
        None
    }
    fn recovery(&self) -> Option<&dyn Recovery> {
        Some(self)
    }
    fn recovery_mut(&mut self) -> Option<&mut dyn Recovery> {
        Some(self)
    }
}

impl Recovery for Tape {
    fn covers(&self, _: MemberId) -> bool {
        false
    }
    fn feed(&mut self, _: MemberId, _: &[Segment]) {}
    fn verdicts(&self, _: MemberId, _: &mut Vec<SliceVerdict>) {}
    fn wants(&mut self, _: &mut Vec<Seek>) {}
    fn repair(&mut self, _: &mut dyn MemberAccess) -> RepairOutcome {
        RepairOutcome::Clean
    }
    fn outcome(&self) -> Option<RepairOutcome> {
        None
    }
    fn blocks_reachable(&self) -> u64 {
        0
    }
}

// The store doubles of the parent module, and a tape for recovery data.
struct Taping(Log);

impl Builder for Taping {
    fn build(&mut self, kind: Kind, id: ObjectId) -> Option<Box<dyn Object>> {
        match kind {
            Kind::Par2 => Some(Box::new(Tape(Arc::clone(&self.0)))),
            _ => Build.build(kind, id),
        }
    }
}

const WINDOW: ByteRange = ByteRange { start: 10, end: 30 };

// The bound store with a window on bytes 10..30 of its member, which has
// had its first twelve bytes.
fn windowed() -> (Tree, MemberId, NodeId, NodeId, Log) {
    let (mut tree, first, child, _) = bound_tree();
    let log = Log::default();
    let mut front = SIGNATURE.to_vec();
    front.extend_from_slice(b"hello");
    let passed = vec![(0, Bytes::from(front), Provenance::Fresh)];
    let window = tree
        .attach_window(
            NodeId::ROOT,
            (first, WINDOW),
            Kind::Par2,
            passed,
            &mut Taping(Arc::clone(&log)),
            &mut Vec::new(),
        )
        .expect("a window is built");
    (tree, first, child, window, log)
}

fn heard(log: &Log) -> Vec<Heard> {
    std::mem::take(&mut *log.lock().expect("log"))
}

#[test]
fn a_window_takes_what_passed_and_what_passes_at_its_own_offsets() {
    let (mut tree, first, child, window, log) = windowed();
    assert_eq!(
        heard(&log),
        vec![Heard::Sized(20), Heard::Bytes(0, b"lo".to_vec())]
    );
    let mut effects = Vec::new();
    tree.deliver(
        NodeId::ROOT,
        first,
        12,
        Bytes::from(vec![7u8; 25]),
        Provenance::Damaged,
        &mut Taping(Arc::clone(&log)),
        &mut effects,
    );
    assert_eq!(
        heard(&log),
        vec![Heard::Sized(20), Heard::Bytes(2, vec![7u8; 18])]
    );
    // The member is still its owner's, and the store had every byte.
    assert_eq!(
        tree.child_of(NodeId::ROOT, first),
        Some((child, VolumeIndex(0)))
    );
    assert!(effects.iter().any(|effect| matches!(
        effect,
        Effect::WriteLeaf { node, offset: 5, .. } if *node == child
    )));
    assert_eq!(tree.recovery_nodes(NodeId::ROOT), vec![window]);
    assert_eq!(
        tree.volumes(window).map(|(parent, _)| parent),
        Some(NodeId::ROOT)
    );
}

#[test]
fn closing_the_member_closes_the_window() {
    let (mut tree, first, _, _, log) = windowed();
    heard(&log);
    tree.close(
        NodeId::ROOT,
        first,
        &mut Taping(Arc::clone(&log)),
        &mut Vec::new(),
    );
    assert_eq!(heard(&log), vec![Heard::Sized(20), Heard::Closed]);
}

#[test]
fn a_window_on_a_closed_member_is_closed_once_it_has_what_passed() {
    let (mut tree, first, _, _) = bound_tree();
    tree.close(NodeId::ROOT, first, &mut Build, &mut Vec::new());
    let log = Log::default();
    tree.attach_window(
        NodeId::ROOT,
        (first, ByteRange::new(8, 12)),
        Kind::Par2,
        vec![(8, Bytes::from_static(b"ello"), Provenance::Fresh)],
        &mut Taping(Arc::clone(&log)),
        &mut Vec::new(),
    );
    assert_eq!(
        heard(&log),
        vec![
            Heard::Sized(4),
            Heard::Bytes(0, b"ello".to_vec()),
            Heard::Sized(4),
            Heard::Closed,
        ]
    );
}

#[test]
fn a_seek_from_a_window_is_moved_to_its_start_and_clipped_at_its_end() {
    let (mut tree, first, _, window, _) = windowed();
    let mut wants = Vec::new();
    tree.collect_wants(&mut wants);
    let (_, seek) = *wants
        .iter()
        .find(|(node, _)| *node == window)
        .expect("the window wants");
    let mut root = Root(Vec::new());
    assert_eq!(tree.lift(window, seek, &mut root), Pull::Moved(1));
    assert_eq!(
        root.0,
        vec![Seek {
            volume: VolumeIndex(first.0),
            range: ByteRange::new(15, 30),
            ..seek
        }]
    );
    let past = Seek {
        range: ByteRange::new(21, 40),
        ..seek
    };
    let mut root = Root(Vec::new());
    tree.lift(window, past, &mut root);
    assert!(root.0.is_empty());
}

#[test]
fn damage_on_the_member_is_lost_to_the_window_where_they_overlap() {
    let (mut tree, first, _, _, log) = windowed();
    heard(&log);
    let mut effects = Vec::new();
    let mut taping = Taping(Arc::clone(&log));
    tree.damaged_under_windows(
        NodeId::ROOT,
        first,
        ByteRange::new(25, 40),
        &mut taping,
        &mut effects,
    );
    tree.damaged_under_windows(
        NodeId::ROOT,
        first,
        ByteRange::new(0, 10),
        &mut taping,
        &mut effects,
    );
    assert_eq!(heard(&log), vec![Heard::Lost(ByteRange::new(15, 20))]);
}

#[test]
fn a_range_has_one_window_and_a_retracted_member_takes_its_windows_with_it() {
    let (mut tree, first, _, window, log) = windowed();
    let again = tree.attach_window(
        NodeId::ROOT,
        (first, WINDOW),
        Kind::Par2,
        Vec::new(),
        &mut Taping(Arc::clone(&log)),
        &mut Vec::new(),
    );
    assert_eq!(again, None);
    tree.retract(NodeId::ROOT, first, &mut Vec::new());
    assert!(tree.namespace(window).is_none());
    assert!(tree.recovery_nodes(NodeId::ROOT).is_empty());
}

#[test]
fn a_whole_member_range_is_itself_and_a_window_range_is_moved() {
    let (tree, _, child, window, _) = windowed();
    let range = ByteRange::new(3, 9);
    assert_eq!(tree.in_member(child, range), Some(range));
    assert_eq!(tree.in_member(window, range), Some(ByteRange::new(13, 19)));
    assert_eq!(tree.in_member(window, ByteRange::new(25, 30)), None);
    assert_eq!(tree.in_member(NodeId::ROOT, range), None);
}
