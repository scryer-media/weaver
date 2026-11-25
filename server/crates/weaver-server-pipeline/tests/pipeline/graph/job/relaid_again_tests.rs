// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A store that streams two members out of its first volume, then, as a
// repaired header comes in the second, lays them out afresh: it retracts
// both, announces them again under new names and, in the same say, asks for
// the first volume's data again so it streams into them. The posted bytes
// are read back from the files the retraction discarded, as they stood.

use std::path::PathBuf;

use bytes::Bytes;
use weaver_graph::holds::{HoldBudgets, HoldsAccountant};
use weaver_graph::{
    Builder, ByteRange, Claim, Delivery, Emit, Engine, Extent, Kind, Located, MemberId, NodeId,
    Object, ObjectId, Placement, Pull, Seek, Standing, VolumeIndex,
};
use weaver_server_disk::operations::disk::volume::LoopSite;

use super::{GraphJob, RootFile};
use crate::pipeline::graph::engine::echo::{HEADER, SIGNATURE};
use crate::pipeline::graph::engine::{DecodePool, RunMail, Runs};
use crate::pipeline::graph::namespace::NamespaceWriter;
use crate::pipeline::graph::root::intake::Decoded;
use crate::pipeline::graph::status::RepairSlots;
use weaver_server_disk::operations::disk::CapacityReader;

// Each member's length: the first volume's body holds both, end to end.
const HALF: u64 = 20;
const BODY: usize = 2 * HALF as usize;
const OLD: [&str; 2] = ["obfuscated-a.bin", "obfuscated-b.bin"];
const NEW: [&str; 2] = ["alpha.bin", "beta.bin"];

#[derive(Clone, Copy)]
struct Relaying {
    // Whether the re-lay asks for the data again.
    again: bool,
}

impl Builder for Relaying {
    fn build(&mut self, kind: Kind, _: ObjectId) -> Option<Box<dyn Object>> {
        (kind == Kind::RarStore).then(|| {
            Box::new(Store {
                again: self.again,
                names: OLD,
                announced: false,
                relaid: false,
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
    again: bool,
    names: [&'static str; 2],
    announced: bool,
    relaid: bool,
}

fn body() -> ByteRange {
    ByteRange::new(HEADER as u64, (HEADER + BODY) as u64)
}

fn member_range(member: usize) -> ByteRange {
    ByteRange::at(HEADER as u64 + member as u64 * HALF, HALF)
}

impl Store {
    fn announce(&mut self, out: &mut Vec<Emit>) {
        for (id, name) in self.names.iter().enumerate() {
            out.push(Emit::Member {
                member: MemberId(id as u32),
                name: (*name).into(),
                length: Some(HALF),
            });
        }
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
        let end = delivery.offset + delivery.bytes.len() as u64;
        if delivery.volume == VolumeIndex(1) {
            // The repaired header: both members start over under their real
            // names, and their data is asked for again.
            if std::mem::replace(&mut self.relaid, true) {
                return;
            }
            out.push(Emit::Retract(MemberId(0)));
            out.push(Emit::Retract(MemberId(1)));
            self.names = NEW;
            self.announce(out);
            if self.again {
                out.push(Emit::Again {
                    volume: VolumeIndex(0),
                    range: body(),
                });
            }
            return;
        }
        if !std::mem::replace(&mut self.announced, true) {
            self.announce(out);
        }
        for id in 0..2 {
            let own = member_range(id);
            let Some(part) = own.intersect(ByteRange::new(delivery.offset, end)) else {
                continue;
            };
            out.push(Emit::Bytes {
                member: MemberId(id as u32),
                offset: part.start - own.start,
                bytes: Bytes::copy_from_slice(
                    &delivery.bytes[(part.start - delivery.offset) as usize
                        ..(part.end - delivery.offset) as usize],
                )
                .into(),
                provenance: delivery.provenance,
            });
        }
    }

    fn volume_closed(&mut self, volume: VolumeIndex, out: &mut Vec<Emit>) {
        if volume == VolumeIndex(0) && self.announced {
            out.push(Emit::MemberClosed(MemberId(0)));
            out.push(Emit::MemberClosed(MemberId(1)));
        }
    }

    fn volume_lost(&mut self, _: VolumeIndex, _: ByteRange, _: &mut Vec<Emit>) {}

    fn quiet(&mut self, _: &dyn Standing, _: &mut Vec<Emit>) {}

    fn wants(&self, _: &mut Vec<Seek>) {}
    fn pulled(&mut self, _: Seek, _: Pull, _: &mut Vec<Emit>) {}

    fn locate(&self, volume: VolumeIndex, range: ByteRange, out: &mut Vec<Placement>) {
        let envelope = |range: ByteRange| Placement::Envelope {
            envelope: volume.0,
            range,
        };
        let Some(part) = range.intersect(body()).filter(|_| volume == VolumeIndex(0)) else {
            out.push(envelope(range));
            return;
        };
        if part.start > range.start {
            out.push(envelope(ByteRange::new(range.start, part.start)));
        }
        for id in 0..2 {
            let own = member_range(id);
            if let Some(piece) = part.intersect(own) {
                out.push(Placement::Member {
                    member: MemberId(id as u32),
                    range: piece.shift(-(own.start as i64)),
                });
            }
        }
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
        name.ends_with(".part2.rar")
            .then_some(VolumeIndex(1))
            .into_iter()
            .collect()
    }

    fn engine(&self) -> Option<&dyn Engine> {
        None
    }

    fn engine_mut(&mut self) -> Option<&mut dyn Engine> {
        None
    }
}

struct Rig {
    job: GraphJob,
    volumes: Vec<Vec<u8>>,
    _mail: RunMail,
    _dir: tempfile::TempDir,
}

fn rig(again: bool) -> Rig {
    let mut first = SIGNATURE.to_vec();
    first.extend((0..BODY as u8).map(|byte| byte.wrapping_mul(7) | 0x80));
    let mut second = vec![0x80; HEADER];
    second.extend(b"repaired header".iter().map(|byte| byte | 0x80));
    let volumes = vec![first, second];
    let files: Vec<RootFile> = volumes
        .iter()
        .enumerate()
        .map(|(index, volume)| RootFile {
            name: format!("set.part{}.rar", index + 1),
            articles: vec![(0, volume.len() as u32)],
        })
        .collect();
    let dir = tempfile::tempdir().unwrap();
    let accountant = std::sync::Arc::new(HoldsAccountant::new(HoldBudgets {
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
    let job = GraphJob::new(&files, Box::new(Relaying { again }), writer, runs, u32::MAX);
    Rig {
        job,
        volumes,
        _mail: mail,
        _dir: dir,
    }
}

impl Rig {
    // The one article of the volume, fetched once.
    fn fetch(&mut self, index: usize) {
        let bytes = &self.volumes[index];
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

    fn store(&self) -> NodeId {
        self.job
            .tree()
            .child_of(NodeId::ROOT, MemberId(0))
            .expect("the store bound")
            .0
    }

    fn files(&self, node: NodeId) -> Vec<(MemberId, PathBuf)> {
        self.job
            .writer()
            .files(node)
            .map(|(member, path)| (member, path.to_path_buf()))
            .collect()
    }

    fn read(&self, node: NodeId, member: MemberId, range: ByteRange) -> Option<Vec<u8>> {
        let mut into = Vec::new();
        self.job
            .writer()
            .access(self.job.tree(), LoopSite::ReadOn)
            .read_at(node, member, range, &mut into)
            .then_some(into)
    }

    // The checkpoint that stops naming the files that went commits.
    fn commit(&mut self) {
        self.job.recorded(1);
        self.job.committed(1).unwrap();
    }
}

// Both members stream whole under their first names, and their files hold
// them.
fn streamed(rig: &mut Rig) -> (NodeId, Vec<PathBuf>) {
    rig.fetch(0);
    let store = rig.store();
    let namespace = rig
        .job
        .tree()
        .namespace(store)
        .expect("the store's members");
    for (id, name) in OLD.iter().enumerate() {
        let record = namespace.get(MemberId(id as u32)).expect("announced");
        assert_eq!(record.name, *name);
        assert!(record.closed, "{name} streamed whole");
    }
    let old: Vec<PathBuf> = rig.files(store).into_iter().map(|(_, path)| path).collect();
    assert_eq!(old.len(), 2, "both members have a file: {old:?}");
    (store, old)
}

// The re-lay asks for the data again in the say that retracted it: both
// members come whole under their new names from the bytes already posted,
// nothing stays under the old ones, the first volume reads back whole for a
// verifier, and no article is asked for again.
#[tokio::test]
async fn a_relay_that_asks_its_data_again_streams_both_members_under_their_new_names() {
    let mut rig = rig(true);
    let (store, old) = streamed(&mut rig);
    rig.fetch(1);
    let namespace = rig
        .job
        .tree()
        .namespace(store)
        .expect("the store's members");
    for (id, name) in NEW.iter().enumerate() {
        let member = MemberId(id as u32);
        let record = namespace.get(member).expect("announced again");
        assert_eq!(record.name, *name);
        assert_eq!(record.output_name, *name);
        assert!(record.closed, "{name} closed again");
        assert!(
            record.coverage.covered(ByteRange::new(0, HALF)),
            "{name} came whole"
        );
        let range = member_range(id);
        let posted = &rig.volumes[0][range.start as usize..range.end as usize];
        assert_eq!(
            rig.read(store, member, ByteRange::new(0, HALF)).as_deref(),
            Some(posted),
            "{name} holds what was posted"
        );
    }
    assert!(
        namespace
            .records()
            .all(|record| !OLD.contains(&record.name.as_str())
                && !OLD.contains(&record.output_name.as_str())),
        "no member keeps an old name"
    );
    let new = rig.files(store);
    assert_eq!(new.len(), 2, "both members have a file again: {new:?}");
    for (member, path) in &new {
        let range = member_range(member.0 as usize);
        assert_eq!(
            std::fs::read(path).unwrap(),
            &rig.volumes[0][range.start as usize..range.end as usize],
            "{path:?}"
        );
    }
    let length = rig.volumes[0].len() as u64;
    assert_eq!(
        rig.read(NodeId::ROOT, MemberId(0), ByteRange::new(0, length))
            .as_deref(),
        Some(rig.volumes[0].as_slice()),
        "the first volume reads back whole"
    );
    for volume in 0..2 {
        assert!(
            rig.job.article_settled(VolumeIndex(volume), 0),
            "volume {volume}'s article fetched once"
        );
    }
    rig.commit();
    for path in &old {
        assert!(
            new.iter().any(|(_, at)| at == path) || !path.exists(),
            "{path:?} is gone"
        );
    }
}

// A retraction whose say asks for nothing again leaves no bytes behind: the
// members read nothing once the step ends, and their files go with the
// checkpoint.
#[tokio::test]
async fn a_relay_that_asks_nothing_again_leaves_no_bytes_of_the_retracted_members() {
    let mut rig = rig(false);
    let (store, old) = streamed(&mut rig);
    rig.fetch(1);
    for (id, name) in NEW.iter().enumerate() {
        let member = MemberId(id as u32);
        let record = rig
            .job
            .tree()
            .namespace(store)
            .and_then(|namespace| namespace.get(member))
            .expect("announced again");
        assert_eq!(record.name, *name);
        assert_eq!(record.coverage.bytes_covered(), 0, "{name} holds nothing");
        assert_eq!(
            rig.read(store, member, ByteRange::new(0, HALF)),
            None,
            "{name} reads nothing of what was retracted"
        );
    }
    assert!(rig.files(store).is_empty(), "no member has a file");
    rig.commit();
    for path in &old {
        assert!(!path.exists(), "{path:?} is gone");
    }
}
