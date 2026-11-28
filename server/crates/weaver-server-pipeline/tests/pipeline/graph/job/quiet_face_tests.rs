// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A store that shows its engine face only once it hears quiet, as a mixed set
// with no end record learns only then that nothing more can join. It places
// one member of its own and keeps the rest on its shelf for a run, which
// makes the other member. Once its face shows, its run starts.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use bytes::Bytes;
use weaver_graph::holds::{HoldBudgets, HoldsAccountant};
use weaver_graph::{
    Builder, ByteRange, Claim, Delivery, Emit, Engine, Extent, Extraction, Kind, Located, MemberId,
    NodeId, Object, ObjectId, Placement, Provenance, Pull, Seek, Sink, Standing, VolumeIndex,
    Volumes,
};

use super::{GraphJob, RootFile};
use crate::pipeline::graph::engine::echo::{HEADER, SIGNATURE};
use crate::pipeline::graph::engine::{DecodePool, RunMail, Runs};
use crate::pipeline::graph::namespace::NamespaceWriter;
use crate::pipeline::graph::root::intake::Decoded;
use crate::pipeline::graph::status::RepairSlots;
use weaver_server_disk::operations::disk::CapacityReader;

// The store's own member: these bytes of the first volume's body.
const OWN: u64 = 20;
const BODY: usize = 40;

// When the store shows its engine face.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
enum Shows {
    // On quiet, saying nothing and keeping its placement number.
    #[default]
    Quiet,
    // On quiet, saying nothing, with a new placement number.
    QuietPlacedAnew,
    // As its last volume closes, saying nothing.
    LastClose,
    // Never: its quiet changes nothing.
    Never,
}

#[derive(Clone, Default)]
struct Quietly {
    volumes: u32,
    shows: Shows,
    starts: Arc<AtomicUsize>,
    quiets: Arc<AtomicUsize>,
}

impl Builder for Quietly {
    fn build(&mut self, kind: Kind, _: ObjectId) -> Option<Box<dyn Object>> {
        (kind == Kind::RarStore).then(|| {
            Box::new(Store {
                volumes: self.volumes,
                shows: self.shows,
                announced: false,
                ended: false,
                starts: Arc::clone(&self.starts),
                quiets: Arc::clone(&self.quiets),
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
    shows: Shows,
    announced: bool,
    // Whether it heard quiet: only then does it know nothing more can join,
    // and only then does it show its engine face.
    ended: bool,
    starts: Arc<AtomicUsize>,
    quiets: Arc<AtomicUsize>,
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
        if delivery.volume != VolumeIndex(0) {
            return;
        }
        let own = ByteRange::new(HEADER as u64, HEADER as u64 + OWN);
        let end = delivery.offset + delivery.bytes.len() as u64;
        let Some(part) = own.intersect(ByteRange::new(delivery.offset, end)) else {
            return;
        };
        if !std::mem::replace(&mut self.announced, true) {
            out.push(Emit::Member {
                member: MemberId(0),
                name: "own.bin".into(),
                length: Some(OWN),
            });
        }
        out.push(Emit::Bytes {
            member: MemberId(0),
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
            out.push(Emit::MemberClosed(MemberId(0)));
        }
        if self.shows == Shows::LastClose && volume.0 + 1 == self.volumes {
            self.ended = true;
        }
    }

    fn volume_lost(&mut self, _: VolumeIndex, _: ByteRange, _: &mut Vec<Emit>) {}

    // It learns nothing more can join, and says nothing.
    fn quiet(&mut self, _: &dyn Standing, _: &mut Vec<Emit>) {
        self.quiets.fetch_add(1, Ordering::SeqCst);
        self.ended = self.shows != Shows::Never;
    }

    fn placement(&self) -> Option<u64> {
        (self.shows == Shows::QuietPlacedAnew).then_some(u64::from(self.ended))
    }

    fn wants(&self, _: &mut Vec<Seek>) {}
    fn pulled(&mut self, _: Seek, _: Pull, _: &mut Vec<Emit>) {}

    fn locate(&self, volume: VolumeIndex, range: ByteRange, out: &mut Vec<Placement>) {
        let own = ByteRange::new(HEADER as u64, HEADER as u64 + OWN);
        let Some(part) = range.intersect(own).filter(|_| volume == VolumeIndex(0)) else {
            out.push(Placement::Envelope {
                envelope: volume.0,
                range,
            });
            return;
        };
        if part.start > range.start {
            out.push(Placement::Envelope {
                envelope: volume.0,
                range: ByteRange::new(range.start, part.start),
            });
        }
        out.push(Placement::Member {
            member: MemberId(0),
            range: part.shift(-(own.start as i64)),
        });
        if part.end < range.end {
            out.push(Placement::Envelope {
                envelope: volume.0,
                range: ByteRange::new(part.end, range.end),
            });
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
        self.ended.then_some(self as &dyn Engine)
    }

    fn engine_mut(&mut self) -> Option<&mut dyn Engine> {
        if self.ended { Some(self) } else { None }
    }
}

impl Engine for Store {
    fn start(&mut self) -> Box<dyn Extraction> {
        self.starts.fetch_add(1, Ordering::SeqCst);
        Box::new(Run {
            last: VolumeIndex(self.volumes - 1),
        })
    }

    // The run makes the second member alone.
    fn makes(&self, member: MemberId) -> bool {
        member == MemberId(1)
    }
}

// Reads the second half of the last volume's body as the second member.
struct Run {
    last: VolumeIndex,
}

impl Extraction for Run {
    fn run(
        self: Box<Self>,
        volumes: Arc<dyn Volumes>,
        sink: &mut Sink<'_>,
        _: weaver_graph::Budget,
    ) {
        let member = MemberId(1);
        let length = (BODY as u64) - OWN;
        if !sink(Emit::Member {
            member,
            name: "made.bin".into(),
            length: Some(length),
        }) {
            return;
        }
        let mut bytes = vec![0; length as usize];
        let Ok(read) = volumes.read_at(self.last, HEADER as u64 + OWN, &mut bytes) else {
            return;
        };
        bytes.truncate(read);
        if !sink(Emit::Bytes {
            member,
            offset: 0,
            bytes: Bytes::from(bytes).into(),
            provenance: Provenance::Fresh,
        }) {
            return;
        }
        sink(Emit::MemberClosed(member));
    }
}

struct Rig {
    job: GraphJob,
    volumes: Vec<Vec<u8>>,
    starts: Arc<AtomicUsize>,
    quiets: Arc<AtomicUsize>,
    _mail: RunMail,
    _dir: tempfile::TempDir,
}

fn rig(count: u32, shows: Shows) -> Rig {
    let volumes: Vec<Vec<u8>> = (0..count)
        .map(|index| {
            let mut volume = if index == 0 {
                SIGNATURE.to_vec()
            } else {
                vec![0x80; HEADER]
            };
            volume.extend(
                (0..BODY as u8).map(|byte| byte.wrapping_mul(7).wrapping_add(index as u8) | 0x80),
            );
            volume
        })
        .collect();
    let files: Vec<RootFile> = volumes
        .iter()
        .enumerate()
        .map(|(index, volume)| RootFile {
            name: format!("lantern.part{}.rar", index + 1),
            articles: vec![(0, volume.len() as u32)],
        })
        .collect();
    let quietly = Quietly {
        volumes: count,
        shows,
        ..Quietly::default()
    };
    let (starts, quiets) = (Arc::clone(&quietly.starts), Arc::clone(&quietly.quiets));
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
    let job = GraphJob::new(&files, Box::new(quietly), writer, runs, u32::MAX);
    Rig {
        job,
        volumes,
        starts,
        quiets,
        _mail: mail,
        _dir: dir,
    }
}

impl Rig {
    fn deliver_all(&mut self) {
        for (index, bytes) in self.volumes.iter().enumerate() {
            self.job
                .intake(
                    Decoded {
                        volume: VolumeIndex(index as u32),
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
    }

    fn store(&self) -> NodeId {
        self.job
            .tree()
            .child_of(NodeId::ROOT, MemberId(0))
            .expect("the store bound")
            .0
    }
}

// Every volume in: the store's face shows, and its run is asked and
// started.
fn face_starts_its_run(count: u32, shows: Shows) {
    let mut rig = rig(count, shows);
    rig.deliver_all();
    let node = rig.store();
    if shows != Shows::LastClose {
        assert!(
            rig.quiets.load(Ordering::SeqCst) > 0,
            "the store heard quiet"
        );
    }
    assert!(rig.job.tree().engine(node).is_some(), "the face shows");
    assert!(rig.job.runs().has(node), "a run was asked for the store");
    assert_eq!(rig.starts.load(Ordering::SeqCst), 1, "the run started");
}

#[tokio::test]
async fn a_store_whose_face_shows_on_quiet_over_one_volume_starts_its_run() {
    face_starts_its_run(1, Shows::Quiet);
}

#[tokio::test]
async fn a_store_whose_face_shows_on_quiet_over_two_volumes_starts_its_run() {
    face_starts_its_run(2, Shows::Quiet);
}

// A new placement number from the same quiet counts as a change: the job
// steps again, and the run starts.
#[tokio::test]
async fn a_store_whose_face_shows_on_quiet_placed_anew_over_one_volume_starts_its_run() {
    face_starts_its_run(1, Shows::QuietPlacedAnew);
}

#[tokio::test]
async fn a_store_whose_face_shows_on_quiet_placed_anew_over_two_volumes_starts_its_run() {
    face_starts_its_run(2, Shows::QuietPlacedAnew);
}

// A face that shows as the last volume closes starts its run before the job
// is quiet, whatever the count.
#[tokio::test]
async fn a_store_whose_face_shows_at_its_last_close_over_one_volume_starts_its_run() {
    face_starts_its_run(1, Shows::LastClose);
}

#[tokio::test]
async fn a_store_whose_face_shows_at_its_last_close_over_two_volumes_starts_its_run() {
    face_starts_its_run(2, Shows::LastClose);
}

// A quiet that changes nothing, with no face before or after, is no change:
// the store hears it once, the job steps nothing more, and no run is asked.
#[tokio::test]
async fn a_store_whose_quiet_changes_nothing_steps_nothing_and_asks_no_run() {
    let mut rig = rig(2, Shows::Never);
    rig.deliver_all();
    let node = rig.store();
    assert_eq!(
        rig.quiets.load(Ordering::SeqCst),
        1,
        "one quiet, no pass after"
    );
    assert!(rig.job.tree().engine(node).is_none(), "no face");
    assert!(!rig.job.runs().has(node), "no run asked");
    assert_eq!(rig.starts.load(Ordering::SeqCst), 0, "nothing started");
}
