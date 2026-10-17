// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// An object that reports recovery data inside its own volume gets a
// recovery object over that range, beside it in the namespace above.

use super::*;

// The carrier's volume: the signature as envelope 0, five bytes of
// "inner.bin", then eight bytes of trailing recovery data as envelope 1.
const BODY: ByteRange = ByteRange { start: 7, end: 12 };
const TRAILER: ByteRange = ByteRange { start: 12, end: 20 };

fn volume() -> Vec<u8> {
    [SIGNATURE, b"hello", b"PAR3tail"].concat()
}

// A pretend store that says, with its first bytes, that the trailer is
// recovery data of `kind`.
struct Carrier {
    kind: RecoveryKind,
    announced: bool,
}

impl Object for Carrier {
    fn kind(&self) -> Kind {
        Kind::RarStore
    }

    fn deliver(&mut self, delivery: Delivery<'_>, out: &mut Vec<Emit>) {
        if !self.announced {
            self.announced = true;
            out.push(Emit::Member {
                member: MemberId(0),
                name: "inner.bin".into(),
                length: Some(BODY.len()),
            });
            out.push(Emit::RecoveryAt {
                volume: delivery.volume,
                range: TRAILER,
                kind: self.kind,
            });
        }
        let range = ByteRange::at(delivery.offset, delivery.bytes.len() as u64);
        if let Some(part) = range.intersect(BODY) {
            let start = (part.start - delivery.offset) as usize;
            out.push(Emit::Bytes {
                member: MemberId(0),
                offset: part.start - BODY.start,
                bytes: Bytes::copy_from_slice(&delivery.bytes[start..start + part.len() as usize]),
                provenance: delivery.provenance,
            });
        }
    }

    fn volume_closed(&mut self, _: VolumeIndex, _: &mut Vec<Emit>) {}
    fn volume_lost(&mut self, _: VolumeIndex, _: ByteRange, _: &mut Vec<Emit>) {}
    fn wants(&mut self, _: &mut Vec<Seek>) {}
    fn pulled(&mut self, _: Seek, _: Pull, _: &mut Vec<Emit>) {}

    fn locate(&self, _: VolumeIndex, range: ByteRange, out: &mut Vec<Placement>) {
        if let Some(header) = range.intersect(ByteRange::new(0, BODY.start)) {
            out.push(Placement::Envelope {
                envelope: 0,
                range: header,
            });
        }
        if let Some(body) = range.intersect(BODY) {
            out.push(Placement::Member {
                member: MemberId(0),
                range: body.shift(-(BODY.start as i64)),
            });
        }
        if let Some(trailer) = range.intersect(ByteRange::new(TRAILER.start, u64::MAX)) {
            out.push(Placement::Envelope {
                envelope: 1,
                range: trailer,
            });
        }
    }

    fn extent(&self, _: MemberId, _: ByteRange, _: &mut Vec<Extent>) {}

    fn envelope(&self, _: u32, _: ByteRange) -> Option<Bytes> {
        None
    }
}

// What the window's object heard and read.
#[derive(Default)]
struct Tape {
    bytes: Vec<(u64, Vec<u8>)>,
    closed: bool,
    lost: Vec<ByteRange>,
    volume: Option<Vec<u8>>,
}

// A recovery object that keeps a tape of its one volume and, asked to
// repair, reads it back whole.
struct Trail {
    tape: Arc<Mutex<Tape>>,
    length: u64,
}

impl Object for Trail {
    fn kind(&self) -> Kind {
        Kind::Par3
    }

    fn deliver(&mut self, delivery: Delivery<'_>, _: &mut Vec<Emit>) {
        self.tape
            .lock()
            .unwrap()
            .bytes
            .push((delivery.offset, delivery.bytes.to_vec()));
    }

    fn sized(&mut self, _: VolumeIndex, length: u64, _: &mut Vec<Emit>) {
        self.length = length;
    }

    fn volume_closed(&mut self, _: VolumeIndex, _: &mut Vec<Emit>) {
        self.tape.lock().unwrap().closed = true;
    }

    fn volume_lost(&mut self, _: VolumeIndex, range: ByteRange, _: &mut Vec<Emit>) {
        self.tape.lock().unwrap().lost.push(range);
    }

    fn wants(&mut self, _: &mut Vec<Seek>) {}
    fn pulled(&mut self, _: Seek, _: Pull, _: &mut Vec<Emit>) {}

    fn locate(&self, _: VolumeIndex, range: ByteRange, out: &mut Vec<Placement>) {
        out.push(Placement::Envelope { envelope: 0, range });
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

impl Recovery for Trail {
    fn covers(&self, _: MemberId) -> bool {
        false
    }

    fn feed(&mut self, _: MemberId, _: &[Segment]) {}
    fn verdicts(&self, _: MemberId, _: &mut Vec<SliceVerdict>) {}
    fn wants(&mut self, _: &mut Vec<Seek>) {}

    fn repair(&mut self, access: &mut dyn MemberAccess) -> RepairOutcome {
        let mut volume = Vec::new();
        if access.read_volume(VolumeIndex(0), ByteRange::new(0, self.length), &mut volume) {
            self.tape.lock().unwrap().volume = Some(volume);
        }
        let mut past = Vec::new();
        assert!(
            !access.read_volume(
                VolumeIndex(0),
                ByteRange::new(0, self.length + 1),
                &mut past
            ),
            "nothing past the window is the volume's"
        );
        RepairOutcome::Clean
    }

    fn outcome(&self) -> Option<RepairOutcome> {
        None
    }

    fn blocks_reachable(&self) -> u64 {
        0
    }
}

struct Carrying {
    kind: RecoveryKind,
    tape: Arc<Mutex<Tape>>,
}

impl Builder for Carrying {
    fn build(&mut self, kind: Kind, _: ObjectId) -> Option<Box<dyn Object>> {
        match kind {
            Kind::RarStore => Some(Box::new(Carrier {
                kind: self.kind,
                announced: false,
            })),
            Kind::Par3 => Some(Box::new(Trail {
                tape: Arc::clone(&self.tape),
                length: 0,
            })),
            _ => None,
        }
    }
}

struct Carried {
    job: Job,
    build: Carrying,
    member: MemberId,
}

impl Carried {
    // The carrier's volume, announced, with its first fifteen bytes in.
    fn new(kind: RecoveryKind) -> Self {
        let mut job = Job::new(PLENTY);
        let mut build = Carrying {
            kind,
            tape: Arc::default(),
        };
        let member = job
            .tree
            .announce_root("pack.bin", Some(20), &mut Vec::new());
        job.writer
            .announced(
                &mut job.tree,
                &mut build,
                &Source,
                NodeId::ROOT,
                member,
                &mut job.out,
            )
            .expect("announce");
        let mut carried = Self { job, build, member };
        carried.send(0, 15);
        carried
    }

    fn send(&mut self, start: usize, end: usize) {
        let arrival = Arrival {
            node: NodeId::ROOT,
            member: self.member,
            offset: start as u64,
            bytes: Bytes::copy_from_slice(&volume()[start..end]),
            provenance: Provenance::Fresh,
        };
        let job = &mut self.job;
        job.writer
            .deliver(
                &mut job.tree,
                &mut self.build,
                &Source,
                arrival,
                &mut job.out,
            )
            .expect("deliver");
    }

    fn close(&mut self) {
        let job = &mut self.job;
        job.writer
            .close(
                &mut job.tree,
                &mut self.build,
                &Source,
                NodeId::ROOT,
                self.member,
                &mut job.out,
            )
            .expect("close");
    }

    fn tape(&self) -> std::sync::MutexGuard<'_, Tape> {
        self.build.tape.lock().unwrap()
    }
}

#[test]
fn recovery_data_inside_a_volume_gets_a_window_fed_what_passed_and_what_passes() {
    let mut carried = Carried::new(RecoveryKind::Par3);
    let windows = carried.job.tree.recovery_nodes(NodeId::ROOT);
    assert_eq!(windows.len(), 1);
    // The volume is still the carrier's, and so is its member.
    let (owner, _) = carried
        .job
        .tree
        .child_of(NodeId::ROOT, carried.member)
        .expect("still bound");
    assert!(!carried.job.tree.recovers(owner));
    assert_eq!(carried.job.file("inner.bin"), b"hello");
    // What passed before the window was built was read back through the
    // carrier's envelope.
    assert_eq!(carried.tape().bytes, vec![(0, b"PAR".to_vec())]);
    carried.send(15, 20);
    assert_eq!(
        carried.tape().bytes,
        vec![(0, b"PAR".to_vec()), (3, b"3tail".to_vec())]
    );
    carried.close();
    assert!(carried.tape().closed);

    let job = &mut carried.job;
    let outcomes = job
        .writer
        .repair(
            &mut job.tree,
            &mut carried.build,
            &Source,
            NodeId::ROOT,
            &mut job.out,
        )
        .expect("repair");
    assert_eq!(outcomes, vec![(windows[0], RepairOutcome::Clean)]);
    assert_eq!(carried.tape().volume.as_deref(), Some(&b"PAR3tail"[..]));
}

#[test]
fn damage_on_the_volume_is_lost_to_the_window_where_it_falls() {
    let mut carried = Carried::new(RecoveryKind::Par3);
    let mut effects = Vec::new();
    let job = &mut carried.job;
    job.tree.damage(
        NodeId::ROOT,
        VolumeIndex(carried.member.0),
        ByteRange::new(10, 14),
        &mut effects,
    );
    job.writer
        .apply_all(
            &mut job.tree,
            &mut carried.build,
            &Source,
            effects,
            &mut job.out,
        )
        .expect("apply");
    assert_eq!(carried.tape().lost, vec![ByteRange::new(0, 2)]);
}

#[test]
fn recovery_data_no_object_reads_is_recorded_and_gets_no_window() {
    let carried = Carried::new(RecoveryKind::Rar);
    assert!(carried.job.tree.recovery_nodes(NodeId::ROOT).is_empty());
    assert_eq!(
        carried.job.writer.unusable_recovery(),
        &[(NodeId::ROOT, carried.member)]
    );
    assert!(carried.tape().bytes.is_empty());
}
