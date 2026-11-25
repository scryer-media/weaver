// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A store that places one member itself and has its run make the rest, and
// whose run makes members announced after it started: a later volume names
// one, or the run names one itself. What the run's record says it makes
// grows by each, so a refusal, a restart and a retraction reach them as they
// reach the members the run made from its start.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, mpsc};

use bytes::Bytes;
use weaver_graph::holds::{HoldBudgets, HoldsAccountant};
use weaver_graph::{
    Builder, ByteRange, Claim, Delivery, Emit, Engine, Extent, Extraction, Kind, Located, MemberId,
    NodeId, Object, ObjectId, Placement, Provenance, Pull, Seek, Sink, Standing, VolumeIndex,
    Volumes, Why,
};
use weaver_server_disk::operations::disk::volume::LoopSite;

use super::{GraphJob, RootFile};
use crate::pipeline::graph::engine::echo::{HEADER, SIGNATURE};
use crate::pipeline::graph::engine::{DecodePool, Heard, RunMail, Runs};
use crate::pipeline::graph::namespace::NamespaceWriter;
use crate::pipeline::graph::root::intake::Decoded;
use crate::pipeline::graph::status::RepairSlots;
use weaver_server_disk::operations::disk::CapacityReader;

// The store's own member: these bytes of the first volume's body.
const OWN: u64 = 20;
const BODY: usize = 40;
// Each member the run makes.
const MADE: u64 = 16;
const OWN_MEMBER: MemberId = MemberId(0);
const FIRST: MemberId = MemberId(1);
const LATE: MemberId = MemberId(2);

// What the test has the run say, one step at a time.
enum Step {
    Say(Emit),
    End,
}

#[derive(Clone)]
struct Shared {
    starts: Arc<AtomicUsize>,
    steps: Arc<Mutex<mpsc::Receiver<Step>>>,
}

#[derive(Clone)]
struct Building {
    volumes: u32,
    // The volume whose arrival names the late member.
    late_at: Option<u32>,
    // The volume whose arrival has the store take the late member back to
    // place it itself.
    taken_at: Option<u32>,
    shared: Shared,
}

impl Builder for Building {
    fn build(&mut self, kind: Kind, _: ObjectId) -> Option<Box<dyn Object>> {
        (kind == Kind::RarStore).then(|| {
            Box::new(Store {
                volumes: self.volumes,
                late_at: self.late_at,
                taken_at: self.taken_at,
                announced: false,
                late: false,
                places_late: false,
                shared: self.shared.clone(),
            }) as Box<dyn Object>
        })
    }

    fn read(
        &self,
        _: &weaver_graph::Tree,
        _: NodeId,
        _: VolumeIndex,
        _: &Placement,
        _: &mut Vec<u8>,
    ) -> bool {
        false
    }
}

struct Store {
    volumes: u32,
    late_at: Option<u32>,
    taken_at: Option<u32>,
    announced: bool,
    late: bool,
    // Whether the store places the late member itself.
    places_late: bool,
    shared: Shared,
}

fn member(member: MemberId, name: &str, length: u64) -> Emit {
    Emit::Member {
        member,
        name: name.into(),
        length: Some(length),
    }
}

impl Object for Store {
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
        let volume = delivery.volume.0;
        if Some(volume) == self.late_at && !std::mem::replace(&mut self.late, true) {
            out.push(member(LATE, "late.bin", MADE));
        }
        if Some(volume) == self.taken_at && !std::mem::replace(&mut self.places_late, true) {
            out.push(Emit::Retract(LATE));
            out.push(member(LATE, "late.bin", MADE));
        }
        if volume != 0 {
            return;
        }
        if !std::mem::replace(&mut self.announced, true) {
            out.push(member(OWN_MEMBER, "own.bin", OWN));
            out.push(member(FIRST, "first.bin", MADE));
        }
        let own = ByteRange::new(HEADER as u64, HEADER as u64 + OWN);
        let end = delivery.offset + delivery.bytes.len() as u64;
        let Some(part) = own.intersect(ByteRange::new(delivery.offset, end)) else {
            return;
        };
        out.push(Emit::Bytes {
            member: OWN_MEMBER,
            offset: part.start - own.start,
            bytes: Bytes::copy_from_slice(
                &delivery.bytes[(part.start - delivery.offset) as usize
                    ..(part.end - delivery.offset) as usize],
            )
            .into(),
            provenance: delivery.provenance,
        });
    }

    fn volume_closed(&mut self, volume: VolumeIndex, out: &mut Vec<Emit>) {
        if volume == VolumeIndex(0) && self.announced {
            out.push(Emit::MemberClosed(OWN_MEMBER));
        }
    }

    fn volume_lost(&mut self, _: VolumeIndex, _: ByteRange, _: &mut Vec<Emit>) {}

    fn quiet(&mut self, _: &dyn Standing, _: &mut Vec<Emit>) {}

    fn wants(&self, _: &mut Vec<Seek>) {}
    fn pulled(&mut self, _: Seek, _: Pull, _: &mut Vec<Emit>) {}

    fn locate(&self, volume: VolumeIndex, range: ByteRange, out: &mut Vec<Placement>) {
        let own = ByteRange::new(HEADER as u64, HEADER as u64 + OWN);
        let envelope = |range: ByteRange| Placement::Envelope {
            envelope: volume.0,
            range,
        };
        let Some(part) = range.intersect(own).filter(|_| volume == VolumeIndex(0)) else {
            out.push(envelope(range));
            return;
        };
        if part.start > range.start {
            out.push(envelope(ByteRange::new(range.start, part.start)));
        }
        out.push(Placement::Member {
            member: OWN_MEMBER,
            range: part.shift(-(own.start as i64)),
        });
        if part.end < range.end {
            out.push(envelope(ByteRange::new(part.end, range.end)));
        }
    }

    fn extent(&self, _: MemberId, _: ByteRange, _: &mut Vec<Extent>) -> Located {
        Located::Never
    }

    fn envelope(&self, _: u32, _: ByteRange) -> Option<Bytes> {
        None
    }

    fn claim(&mut self, name: &str, _: weaver_graph::Clean<'_>, _length: Option<u64>) -> Claim {
        name.rsplit_once(".part")
            .and_then(|(_, part)| part.strip_suffix(".rar")?.parse::<u32>().ok())
            .filter(|part| *part >= 2 && *part <= self.volumes)
            .map(|part| VolumeIndex(part - 1))
            .into_iter()
            .collect()
    }

    fn engine(&self) -> Option<&dyn Engine> {
        Some(self)
    }

    fn engine_mut(&mut self) -> Option<&mut dyn Engine> {
        Some(self)
    }
}

impl Engine for Store {
    fn start(&mut self) -> Box<dyn Extraction> {
        self.shared.starts.fetch_add(1, Ordering::SeqCst);
        Box::new(Run {
            steps: Arc::clone(&self.shared.steps),
        })
    }

    // The run makes every member but the store's own, and the late one
    // once the store places it itself.
    fn makes(&self, member: MemberId) -> bool {
        member != OWN_MEMBER && !(member == LATE && self.places_late)
    }
}

// Says what the test hands it, until told to end or the test is gone.
struct Run {
    steps: Arc<Mutex<mpsc::Receiver<Step>>>,
}

impl Extraction for Run {
    fn run(self: Box<Self>, _: Arc<dyn Volumes>, sink: &mut Sink<'_>, _: weaver_graph::Budget) {
        loop {
            let step = self.steps.lock().unwrap().recv();
            match step {
                Ok(Step::Say(emit)) => {
                    if !sink(emit) {
                        return;
                    }
                }
                Ok(Step::End) | Err(_) => return,
            }
        }
    }
}

struct Rig {
    job: GraphJob,
    volumes: Vec<Vec<u8>>,
    starts: Arc<AtomicUsize>,
    steps: mpsc::Sender<Step>,
    mail: RunMail,
    _dir: tempfile::TempDir,
}

fn rig(count: u32, late_at: Option<u32>, taken_at: Option<u32>) -> Rig {
    let volumes: Vec<Vec<u8>> = (0..count)
        .map(|index| {
            let mut volume = if index == 0 {
                SIGNATURE.to_vec()
            } else {
                vec![0x80; HEADER]
            };
            volume.extend(
                (0..BODY as u8).map(|byte| byte.wrapping_mul(5).wrapping_add(index as u8) | 0x80),
            );
            volume
        })
        .collect();
    let files: Vec<RootFile> = volumes
        .iter()
        .enumerate()
        .map(|(index, volume)| RootFile {
            name: format!("ledger.part{}.rar", index + 1),
            articles: vec![(0, volume.len() as u32)],
        })
        .collect();
    let (steps, heard) = mpsc::channel();
    let shared = Shared {
        starts: Arc::new(AtomicUsize::new(0)),
        steps: Arc::new(Mutex::new(heard)),
    };
    let starts = Arc::clone(&shared.starts);
    let building = Building {
        volumes: count,
        late_at,
        taken_at,
        shared,
    };
    let dir = tempfile::tempdir().unwrap();
    let accountant = Arc::new(HoldsAccountant::new(HoldBudgets {
        resident_limit: u64::MAX,
        scratch_total: u64::MAX,
        scratch_ceiling: u64::MAX,
        disk_reserve: 0,
    }));
    let writer = NamespaceWriter::new(
        1,
        dir.path().to_path_buf(),
        accountant,
        CapacityReader::unknown(),
    );
    let mail = RunMail::default();
    let runs = Runs::new(1, 1, mail.sender(), DecodePool::unbounded());
    let job = GraphJob::new(&files, Box::new(building), writer, runs, u32::MAX);
    Rig {
        job,
        volumes,
        starts,
        steps,
        mail,
        _dir: dir,
    }
}

impl Rig {
    fn fetch(&mut self, index: u32) {
        let bytes = &self.volumes[index as usize];
        self.job
            .intake(
                Decoded {
                    volume: VolumeIndex(index),
                    ordinal: 0,
                    offset: 0,
                    bytes: Bytes::copy_from_slice(bytes),
                    intact: true,
                    positioned: true,
                    length: Some(bytes.len() as u64),
                    declared_end: None,
                    segments: Vec::new(),
                },
                &mut RepairSlots::default(),
                1,
            )
            .unwrap();
    }

    fn store(&self) -> NodeId {
        self.job
            .tree()
            .child_of(NodeId::ROOT, MemberId(0))
            .expect("the store bound")
            .0
    }

    fn starts(&self) -> usize {
        self.starts.load(Ordering::SeqCst)
    }

    fn say(&self, emit: Emit) {
        self.steps.send(Step::Say(emit)).unwrap();
    }

    fn end(&self) {
        self.steps.send(Step::End).unwrap();
    }

    // The run makes the member whole and closes it.
    fn make(&self, member: MemberId, fill: u8) {
        self.say(Emit::Bytes {
            member,
            offset: 0,
            bytes: Bytes::from(vec![fill; MADE as usize]).into(),
            provenance: Provenance::Fresh,
        });
        self.say(Emit::MemberClosed(member));
    }

    // Hear one word of the runs: whether it was the current run's end.
    async fn hear_one(&mut self) -> bool {
        let said = self.mail.recv().await.unwrap();
        let ended = matches!(said.heard, Heard::Ended)
            && self.job.runs().current(said.node, said.generation);
        self.job
            .run_said(said, &mut RepairSlots::default(), 1)
            .unwrap();
        ended
    }

    // Hear what the runs say until the current one ended.
    async fn until_ended(&mut self) {
        while !self.hear_one().await {}
    }

    // Hear what the runs say until `count` runs started.
    async fn until_started(&mut self, count: usize) {
        while self.starts() < count {
            self.hear_one().await;
        }
    }

    // The members the store's run makes, by the tree's record, while a run
    // is on record and the store places members itself.
    fn made(&self) -> Option<Vec<MemberId>> {
        let (tree, store) = (self.job.tree(), self.store());
        if !self.job.runs().has(store) || !tree.places_own(store) {
            return None;
        }
        let namespace = tree.namespace(store)?;
        Some(
            namespace
                .members()
                .filter(|member| !member.own)
                .map(|member| member.id)
                .collect(),
        )
    }

    fn refused(&self, member: MemberId) -> Option<Why> {
        self.job.writer().refused(self.store(), member)
    }

    fn read(&self, member: MemberId) -> Option<Vec<u8>> {
        let mut into = Vec::new();
        self.job
            .writer()
            .access(self.job.tree(), LoopSite::ReadOn)
            .read_at(self.store(), member, ByteRange::new(0, MADE), &mut into)
            .then_some(into)
    }
}

// The run started on the first two volumes, making the first member; the
// third names the late one.
fn started_then_late(count: u32, taken_at: Option<u32>) -> Rig {
    let mut rig = rig(count, Some(2), taken_at);
    rig.fetch(0);
    rig.fetch(1);
    assert_eq!(rig.starts(), 1, "the run started on the first volumes");
    assert_eq!(
        rig.made(),
        Some(vec![FIRST]),
        "the run makes the first member"
    );
    rig.fetch(2);
    assert!(
        rig.job
            .tree()
            .namespace(rig.store())
            .is_some_and(|namespace| namespace.get(LATE).is_some()),
        "the late member is named"
    );
    rig
}

// A run's refusal fails the members announced since it started as it fails
// those it made from the start; what the store placed stands.
#[tokio::test]
async fn a_refusal_mid_run_refuses_the_members_announced_since_it_started() {
    let mut rig = started_then_late(3, None);
    assert_eq!(
        rig.made(),
        Some(vec![FIRST, LATE]),
        "the late member joined"
    );
    rig.say(Emit::Refuse(Why::Unsupported));
    rig.end();
    rig.until_ended().await;
    assert_eq!(rig.refused(FIRST), Some(Why::Unsupported));
    assert_eq!(rig.refused(LATE), Some(Why::Unsupported), "the late member");
    assert_eq!(
        rig.refused(OWN_MEMBER),
        None,
        "the store's own member stands"
    );
}

// A run widened starts over: it retracts the late member it made and
// closed as it retracts the first, and the next run makes both once, under
// what it says.
#[tokio::test]
async fn a_widened_run_retracts_the_late_member_it_closed_and_the_next_makes_it_once() {
    let mut rig = started_then_late(3, None);
    rig.make(FIRST, 0xA1);
    rig.make(LATE, 0xA2);
    rig.say(Emit::Refuse(Why::TooLarge {
        needed: Some(1 << 20),
        allowed: None,
    }));
    rig.end();
    rig.until_started(2).await;
    for member in [FIRST, LATE] {
        let record = rig
            .job
            .tree()
            .namespace(rig.store())
            .and_then(|namespace| namespace.get(member))
            .expect("named");
        assert!(
            !record.closed && record.coverage.bytes_covered() == 0,
            "{member:?} started over with the run"
        );
    }
    rig.make(FIRST, 0xB1);
    rig.make(LATE, 0xB2);
    rig.end();
    rig.until_ended().await;
    assert_eq!(rig.starts(), 2, "one restart");
    for (member, fill) in [(FIRST, 0xB1), (LATE, 0xB2)] {
        let record = rig
            .job
            .tree()
            .namespace(rig.store())
            .and_then(|namespace| namespace.get(member))
            .expect("named");
        assert!(record.closed, "{member:?} closed");
        assert_eq!(
            rig.read(member),
            Some(vec![fill; MADE as usize]),
            "{member:?} holds what the second run made"
        );
    }
}

// The late member the run made, which the store then takes back to place
// itself, restarts the run: the record says the run made it, though the
// store says now that no run makes it.
#[tokio::test]
async fn a_late_member_the_store_takes_back_to_place_itself_restarts_the_run() {
    let mut rig = started_then_late(4, Some(3));
    rig.make(FIRST, 0xC1);
    rig.make(LATE, 0xC2);
    rig.end();
    rig.until_ended().await;
    assert_eq!(rig.starts(), 1);
    rig.fetch(3);
    assert_eq!(rig.starts(), 2, "the run started again");
}

// A member the run names while the store is out on loan joins the record
// once the store is back, before anything starts.
#[tokio::test]
async fn a_member_named_while_the_store_is_on_loan_joins_the_record_once_it_is_back() {
    let mut rig = rig(3, None, None);
    rig.fetch(0);
    rig.fetch(1);
    assert_eq!(rig.made(), Some(vec![FIRST]));
    let store = rig.store();
    let object = rig.job.lend_object_for_test(store).expect("lent");
    rig.say(member(LATE, "late.bin", MADE));
    loop {
        let said = rig.mail.recv().await.unwrap();
        let named = matches!(
            &said.heard,
            Heard::Emit(Emit::Member { member, .. }) | Heard::Paid(Emit::Member { member, .. }, _)
                if *member == LATE
        );
        rig.job
            .run_said(said, &mut RepairSlots::default(), 1)
            .unwrap();
        if named {
            break;
        }
    }
    rig.job.take_back_for_test(store, object, 1).unwrap();
    assert_eq!(
        rig.made(),
        Some(vec![FIRST, LATE]),
        "the late member joined"
    );
    assert_eq!(rig.starts(), 1, "nothing started again");
}
