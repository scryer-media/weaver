// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A later volume a store held without its first reaches the engine its set
// was handed to only once that engine's run had ended on the volumes it had:
// the store, quiet with no first volume, gives the volume up, and the engine
// takes it back. The member the engine writes must still end whole: the
// run starts over on the volumes it now has. A volume that joins while more
// could still join is waited on by the run reading, which is not restarted.
//
// Every volume is an eight-byte header and a body. The first volume's store
// asks for an engine as its volume closes; the later volume, posted under a
// name of no set, founds a store of its own that holds it at its second
// place and never sees a first. The engine's run walks its volumes in order
// and writes every body into one member, `whole.bin`.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use bytes::Bytes;
use weaver_graph::holds::{HoldBudgets, HoldsAccountant};
use weaver_graph::{
    Builder, ByteRange, Carried, Claim, Delivery, Emit, Engine, Extent, Extraction, Kind, Located,
    MemberId, NodeId, Object, ObjectId, Placement, Pull, Seek, Sink, Standing, VolumeIndex,
    Volumes, Why,
};
use weaver_server_disk::operations::disk::CapacityReader;

use crate::pipeline::graph::engine::echo::{HEADER, SIGNATURE};
use crate::pipeline::graph::engine::{Heard, RunMail, Runs, Said};
use crate::pipeline::graph::job::{GraphJob, RootFile};
use crate::pipeline::graph::namespace::NamespaceWriter;
use crate::pipeline::graph::root::intake::Decoded;
use crate::pipeline::graph::status::RepairSlots;

const FIRST: &str = "film.part1.rar";
const LATER: &str = "5f0c9e2ab1d74c6e8a3f1b0d9c2e7a41.rar";
// The second volume named in the first's numbering: the engine claims it.
const SECOND: &str = "film.part2.rar";
const MEMBER: &str = "whole.bin";

fn body(index: u32) -> Vec<u8> {
    (0..40u8)
        .map(|byte| byte.wrapping_mul(7).wrapping_add(index as u8) | 0x80)
        .collect()
}

fn volume(index: u32) -> Vec<u8> {
    let mut volume = SIGNATURE.to_vec();
    volume.extend(body(index));
    volume
}

// The first store built is the first volume's; every later one is the store
// the later volume founds.
#[derive(Default)]
struct Family {
    stores: u32,
    starts: Arc<AtomicUsize>,
    quiets: Quiets,
}

// Each time the engine hears the job is quiet: how many volumes it holds,
// and how many runs it had started by then.
type Quiets = Arc<Mutex<Vec<(usize, usize)>>>;

impl Builder for Family {
    fn build(&mut self, kind: Kind, _: ObjectId) -> Option<Box<dyn Object>> {
        match kind {
            Kind::RarStore => {
                self.stores += 1;
                Some(if self.stores == 1 {
                    Box::new(Handing)
                } else {
                    Box::new(Headless::default())
                })
            }
            Kind::RarEngine => Some(Box::new(Walker {
                starts: Arc::clone(&self.starts),
                quiets: Arc::clone(&self.quiets),
                held: 0,
            })),
            _ => None,
        }
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

// Keeps its volume whole as its envelope, claims nothing else, and asks for
// an engine as its first volume closes.
struct Handing;

impl Object for Handing {
    fn unposted(&mut self, _: VolumeIndex, _: ByteRange) {}
    fn kind(&self) -> Kind {
        Kind::RarStore
    }
    fn deliver(
        &mut self,
        _: Delivery<'_>,
        _: &mut dyn FnMut(&Placement, &mut Vec<u8>) -> bool,
        _: &mut Vec<Emit>,
    ) {
    }
    fn volume_closed(&mut self, volume: VolumeIndex, out: &mut Vec<Emit>) {
        if volume.0 == 0 {
            out.push(Emit::NeedsEngine(Why::Compressed));
        }
    }
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
    fn claim(&mut self, name: &str, _: weaver_graph::Clean<'_>, _: Option<u64>) -> Claim {
        (name == FIRST)
            .then_some(VolumeIndex(0))
            .into_iter()
            .collect()
    }
}

// Holds the later volume at its second place, whole as its envelope, and
// never sees a first: quiet, it gives the volume up as a store does whose
// first bytes were lost, carrying it to whoever claims it.
#[derive(Default)]
struct Headless {
    held: Vec<VolumeIndex>,
}

impl Object for Headless {
    fn unposted(&mut self, _: VolumeIndex, _: ByteRange) {}
    fn kind(&self) -> Kind {
        Kind::RarStore
    }
    fn bound(&mut self, volume: VolumeIndex, _: &str, _: &[u8], _: Option<u64>) {
        if !self.held.contains(&volume) {
            self.held.push(volume);
        }
    }
    fn deliver(
        &mut self,
        _: Delivery<'_>,
        _: &mut dyn FnMut(&Placement, &mut Vec<u8>) -> bool,
        _: &mut Vec<Emit>,
    ) {
    }
    fn volume_closed(&mut self, _: VolumeIndex, _: &mut Vec<Emit>) {}
    fn volume_lost(&mut self, _: VolumeIndex, _: ByteRange, _: &mut Vec<Emit>) {}
    fn quiet(&mut self, _: &dyn Standing, out: &mut Vec<Emit>) {
        out.push(Emit::Refuse(Why::FirstBytesLost));
    }
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
    fn claim(&mut self, name: &str, _: weaver_graph::Clean<'_>, _: Option<u64>) -> Claim {
        (name == LATER)
            .then_some(VolumeIndex(1))
            .into_iter()
            .collect()
    }
    fn carried(&self, volume: VolumeIndex) -> Option<&(dyn std::any::Any + Send + Sync)> {
        self.held.contains(&volume).then_some(&() as _)
    }
    fn carriable(&self, out: &mut Vec<VolumeIndex>) {
        out.extend(self.held.iter().copied());
    }
    fn give_back(&mut self, volume: VolumeIndex) -> Option<Carried> {
        self.held.retain(|held| *held != volume);
        Some(Box::new(()))
    }
}

// The engine: by its name it claims its set's second volume and nothing
// else; a volume given back with its parse it takes at the place that parse
// says, the second. It counts the runs it starts and the volumes bound to
// it, and notes both each time it hears the job is quiet.
struct Walker {
    starts: Arc<AtomicUsize>,
    quiets: Quiets,
    held: usize,
}

impl Object for Walker {
    fn unposted(&mut self, _: VolumeIndex, _: ByteRange) {}
    fn kind(&self) -> Kind {
        Kind::RarEngine
    }
    fn bound(&mut self, _: VolumeIndex, _: &str, _: &[u8], _: Option<u64>) {
        self.held += 1;
    }
    fn quiet(&mut self, _: &dyn Standing, _: &mut Vec<Emit>) {
        let starts = self.starts.load(Ordering::SeqCst);
        self.quiets.lock().unwrap().push((self.held, starts));
    }
    fn deliver(
        &mut self,
        _: Delivery<'_>,
        _: &mut dyn FnMut(&Placement, &mut Vec<u8>) -> bool,
        _: &mut Vec<Emit>,
    ) {
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
    fn claim(&mut self, name: &str, _: weaver_graph::Clean<'_>, _: Option<u64>) -> Claim {
        (name == SECOND)
            .then_some(VolumeIndex(1))
            .into_iter()
            .collect()
    }
    fn reclaim(
        &self,
        name: &str,
        _: &[u8],
        _: Option<u64>,
        _: &(dyn std::any::Any + Send + Sync),
    ) -> Claim {
        (name == LATER)
            .then_some(VolumeIndex(1))
            .into_iter()
            .collect()
    }
    fn reads_in_order(&self) -> bool {
        true
    }
    fn engine(&self) -> Option<&dyn Engine> {
        Some(self)
    }
    fn engine_mut(&mut self) -> Option<&mut dyn Engine> {
        Some(self)
    }
}

impl Engine for Walker {
    fn start(&mut self) -> Box<dyn Extraction> {
        self.starts.fetch_add(1, Ordering::SeqCst);
        Box::new(Walk)
    }
}

// Walks the volumes in the set's order until `next` says none is left, and
// writes each body after the last into `whole.bin`.
struct Walk;

impl Extraction for Walk {
    fn run(
        self: Box<Self>,
        volumes: Arc<dyn Volumes>,
        sink: &mut Sink<'_>,
        _: weaver_graph::Budget,
    ) {
        let member = MemberId(0);
        let announced = Emit::Member {
            member,
            name: MEMBER.to_string(),
            length: None,
        };
        if !sink(announced) {
            return;
        }
        let mut written = 0u64;
        let mut at = None;
        loop {
            let volume = match volumes.next(at) {
                Ok(Some(volume)) => volume,
                Ok(None) => break,
                Err(_) => {
                    sink(Emit::Refuse(Why::Unrepairable));
                    return;
                }
            };
            let mut offset = HEADER as u64;
            let mut chunk = [0u8; 16];
            loop {
                let Ok(read) = volumes.read_at(volume, offset, &mut chunk) else {
                    sink(Emit::Refuse(Why::Unrepairable));
                    return;
                };
                if read == 0 {
                    break;
                }
                let bytes = Emit::Bytes {
                    member,
                    offset: written,
                    bytes: Bytes::copy_from_slice(&chunk[..read]).into(),
                    provenance: weaver_graph::Provenance::Fresh,
                };
                if !sink(bytes) {
                    return;
                }
                offset += read as u64;
                written += read as u64;
            }
            at = Some(volume);
        }
        sink(Emit::MemberClosed(member));
    }
}

struct Late {
    job: GraphJob,
    mail: RunMail,
    starts: Arc<AtomicUsize>,
    quiets: Quiets,
    dir: tempfile::TempDir,
}

// A job of two volumes, the second posted as `second`.
fn late(second: &str) -> Late {
    let files = [FIRST, second]
        .iter()
        .enumerate()
        .map(|(index, name)| RootFile {
            name: name.to_string(),
            articles: vec![(0, volume(index as u32).len() as u32)],
        })
        .collect::<Vec<_>>();
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
    let runs = Runs::new(
        1,
        1,
        mail.sender(),
        crate::pipeline::graph::engine::DecodePool::unbounded(),
    );
    let family = Family::default();
    let starts = Arc::clone(&family.starts);
    let quiets = Arc::clone(&family.quiets);
    let job = GraphJob::new(&files, Box::new(family), writer, runs, u32::MAX);
    Late {
        job,
        mail,
        starts,
        quiets,
        dir,
    }
}

impl Late {
    fn deliver(&mut self, index: u32) {
        let bytes = volume(index);
        self.job
            .intake(
                Decoded {
                    volume: VolumeIndex(index),
                    ordinal: 0,
                    offset: 0,
                    length: Some(bytes.len() as u64),
                    bytes: Bytes::from(bytes),
                    intact: true,
                    positioned: true,
                    declared_end: None,
                    segments: Vec::new(),
                },
                &mut RepairSlots::default(),
                1,
            )
            .unwrap();
        while self.job.giving_up() {
            self.give_up_turn();
        }
    }

    fn give_up_turn(&mut self) {
        self.job
            .run_said(
                Said {
                    scope: 1,
                    node: NodeId::ROOT,
                    generation: 0,
                    heard: Heard::GivingUp,
                },
                &mut RepairSlots::default(),
                1,
            )
            .unwrap();
    }

    // Hear what the runs say until the current one returned.
    async fn until_ended(&mut self) {
        loop {
            let said = self.mail.recv().await.unwrap();
            let ended = matches!(said.heard, Heard::Ended)
                && self.job.runs().current(said.node, said.generation);
            self.job
                .run_said(said, &mut RepairSlots::default(), 1)
                .unwrap();
            if ended {
                return;
            }
        }
    }

    // The engine bound in the root, and the volumes it holds.
    fn engine_volumes(&self) -> Vec<VolumeIndex> {
        let tree = self.job.tree();
        tree.engines()
            .into_iter()
            .filter_map(|engine| tree.volumes(engine))
            .flat_map(|(_, volumes)| volumes.keys().copied().collect::<Vec<_>>())
            .collect()
    }

    fn engine(&self) -> NodeId {
        let engines = self.job.tree().engines();
        assert_eq!(engines.len(), 1, "one engine over the set");
        engines[0]
    }

    // The generation the engine's run started under.
    fn run_generation(&self) -> Option<u64> {
        self.job.runs().generation(self.engine())
    }

    fn starts(&self) -> usize {
        self.starts.load(Ordering::SeqCst)
    }

    // The members the engine announced, by name, and the file each was
    // written to, read whole.
    fn written(&self) -> Vec<(String, Vec<u8>)> {
        let engine = self.engine();
        let names: Vec<String> = self
            .job
            .tree()
            .namespace(engine)
            .map(|namespace| {
                namespace
                    .members()
                    .map(|member| member.name.clone())
                    .collect()
            })
            .unwrap_or_default();
        let files: Vec<Vec<u8>> = self
            .job
            .writer()
            .files(engine)
            .map(|(_, path)| std::fs::read(path).unwrap())
            .collect();
        assert_eq!(names.len(), files.len(), "{names:?}");
        names.into_iter().zip(files).collect()
    }

    // Hear every run out, and any the job starts again after it.
    async fn until_settled(&mut self) {
        while self.job.runs().live() {
            self.until_ended().await;
        }
    }
}

fn whole() -> Vec<(String, Vec<u8>)> {
    let mut whole = body(0);
    whole.extend(body(1));
    vec![(MEMBER.to_string(), whole)]
}

#[tokio::test]
async fn a_later_volume_given_to_an_engine_after_its_run_ended_still_ends_its_member_whole() {
    let mut late = late(LATER);
    late.deliver(0);
    assert_eq!(
        late.engine_volumes(),
        vec![VolumeIndex(0)],
        "the set went to an engine"
    );
    let first = late.run_generation().expect("the run started");
    late.deliver(1);
    assert_eq!(
        late.engine_volumes(),
        vec![VolumeIndex(0)],
        "the later volume is a headless store's, not the engine's"
    );
    // The payload is in and nothing more can join the engine's volumes: its
    // run reads the first and ends there. Hearing it end, the job is quiet:
    // the headless store gives its volume up, and the engine takes it back.
    late.until_ended().await;
    while late.job.giving_up() {
        late.give_up_turn();
    }
    assert_eq!(
        late.engine_volumes(),
        vec![VolumeIndex(0), VolumeIndex(1)],
        "the engine holds the later volume once the store gave it up"
    );
    late.until_settled().await;
    // The run started over under a later generation, and read both volumes
    // into the one member, each body once.
    assert_eq!(late.starts(), 2, "the ended run started once more");
    assert!(late.run_generation().unwrap() > first);
    assert_eq!(late.written(), whole(), "dir {:?}", late.dir.path());
}

#[tokio::test]
async fn a_volume_joining_while_more_can_join_restarts_no_run() {
    let mut late = late(SECOND);
    late.deliver(0);
    assert_eq!(late.engine_volumes(), vec![VolumeIndex(0)]);
    let first = late
        .run_generation()
        .expect("the run started on the first volume");
    late.deliver(1);
    assert_eq!(
        late.engine_volumes(),
        vec![VolumeIndex(0), VolumeIndex(1)],
        "the engine claimed its second volume by name"
    );
    assert_eq!(late.run_generation(), Some(first), "no generation raised");
    late.until_settled().await;
    assert_eq!(late.starts(), 1, "the run reading waited for the volume");
    assert_eq!(late.run_generation(), Some(first));
    assert_eq!(late.written(), whole(), "dir {:?}", late.dir.path());
}

// The engine takes the later volume from the headless store's give-up on
// the quiet's own pass. It is not told the job is quiet on that pass: its
// run has not read the volume yet. It hears quiet again only once the run
// that starts over on both volumes has ended.
#[tokio::test]
async fn an_engine_that_takes_a_given_up_volume_hears_no_quiet_before_its_run_reads_it() {
    let mut late = late(LATER);
    late.deliver(0);
    late.deliver(1);
    late.until_ended().await;
    while late.job.giving_up() {
        late.give_up_turn();
    }
    assert_eq!(
        late.engine_volumes(),
        vec![VolumeIndex(0), VolumeIndex(1)],
        "the engine took the later volume"
    );
    let heard = late.quiets.lock().unwrap().clone();
    assert!(
        heard.iter().all(|&(held, _)| held < 2),
        "the engine heard quiet holding a volume its run had not read: {heard:?}"
    );
    late.until_settled().await;
    assert_eq!(late.starts(), 2, "the run started over on both volumes");
    assert_eq!(late.written(), whole(), "dir {:?}", late.dir.path());
    let heard = late.quiets.lock().unwrap().clone();
    assert!(
        heard.iter().all(|&(held, starts)| held < 2 || starts == 2),
        "{heard:?}"
    );
}
