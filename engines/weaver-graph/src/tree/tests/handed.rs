// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// What a window's owner handed it, kept by its node: an object that takes
// the node back after a loan, the one lent or one rebuilt from its facts in
// its place, is handed it before it hears anything.

use std::sync::{Arc, Mutex};

use super::*;
use crate::deliver::Posted;
use crate::recovery::{MemberAccess, RepairOutcome};
use crate::{Segment, SliceVerdict};

// What the window's object heard, in order.
type Log = Arc<Mutex<Vec<Heard>>>;

#[derive(Clone, Debug, PartialEq, Eq)]
enum Heard {
    // What it was handed, when it is a candidate.
    Handed(Option<String>),
    Sized(u64),
    Bytes(u64, Vec<u8>),
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
    fn deliver(
        &mut self,
        delivery: Delivery<'_>,
        _: &mut dyn FnMut(&Placement, &mut Vec<u8>) -> bool,
        _: &mut Vec<Emit>,
    ) {
        self.hear(Heard::Bytes(delivery.offset, delivery.bytes.to_vec()));
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
            Heard::Bytes(2, vec![7u8; 4]),
        ]
    );
}
