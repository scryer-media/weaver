// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use std::sync::Mutex;

use weaver_graph::holds::{HoldBudgets, ScratchHold};

use crate::pipeline::graph::checkpoint::HoldRecord;
use weaver_graph::recovery::{MemberAccess, Recovery, RepairOutcome, SliceVerdict};
use weaver_graph::{
    Delivery, Emit, Extent, Kind, Located, Object, ObjectId, Pull, RecoveryKind, Seek,
};

use super::*;
use crate::pipeline::graph::lines::Lines;

mod recovery;
mod streams;
mod windows;

const SIGNATURE: &[u8] = b"Rar!\x1a\x07\x00";
const HEADER: u64 = SIGNATURE.len() as u64;

// A pretend store with one volume: a seven-byte header kept as envelope 0,
// then one member, "inner.bin", of the length it is built with, to the end.
// The word RETRACT anywhere in a delivery takes the member back.
struct Store {
    announced: bool,
    length: u64,
}

impl Object for Store {
    // It keeps no record of what it said of its posted bytes.
    fn unposted(&mut self, _: weaver_graph::VolumeIndex, _: weaver_graph::ByteRange) {}

    fn kind(&self) -> Kind {
        Kind::RarStore
    }

    fn deliver(
        &mut self,
        delivery: Delivery<'_>,
        _: &mut dyn FnMut(&weaver_graph::Placement, &mut Vec<u8>) -> bool,
        out: &mut Vec<Emit>,
    ) {
        if delivery.bytes.windows(7).any(|window| window == b"RETRACT") {
            out.push(Emit::Retract(MemberId(0)));
            return;
        }
        if !self.announced {
            self.announced = true;
            out.push(Emit::Member {
                member: MemberId(0),
                name: "inner.bin".into(),
                length: Some(self.length),
            });
        }
        let skip = HEADER
            .saturating_sub(delivery.offset)
            .min(delivery.bytes.len() as u64);
        if skip < delivery.bytes.len() as u64 {
            out.push(Emit::Bytes {
                member: MemberId(0),
                offset: delivery.offset + skip - HEADER,
                bytes: Bytes::copy_from_slice(&delivery.bytes[skip as usize..]),
                provenance: delivery.provenance,
            });
        }
    }

    fn volume_closed(&mut self, _: VolumeIndex, _: &mut Vec<Emit>) {}
    fn volume_lost(&mut self, _: VolumeIndex, _: ByteRange, _: &mut Vec<Emit>) {}
    fn wants(&self, _: &mut Vec<Seek>) {}
    fn pulled(&mut self, _: Seek, _: Pull, _: &mut Vec<Emit>) {}

    fn locate(&self, _: VolumeIndex, range: ByteRange, out: &mut Vec<Placement>) {
        if let Some(header) = range.intersect(ByteRange::new(0, HEADER)) {
            out.push(Placement::Envelope {
                envelope: 0,
                range: header,
            });
        }
        if let Some(body) = range.intersect(ByteRange::new(HEADER, u64::MAX)) {
            out.push(Placement::Member {
                member: MemberId(0),
                range: body.shift(-(HEADER as i64)),
            });
        }
    }

    fn extent(&self, _: MemberId, _: ByteRange, _: &mut Vec<Extent>) -> Located {
        Located::Never
    }

    fn envelope(&self, _: u32, _: ByteRange) -> Option<Bytes> {
        None
    }
}

// A 7z signature header: the magic, format version 0.4, the start header's
// CRC, and a start header of zeros the CRC covers. The sniffer reads the
// whole 32 bytes and checks that CRC before it names the kind.
fn seven_z() -> [u8; 32] {
    let mut header = [0u8; 32];
    header[..8].copy_from_slice(b"7z\xbc\xaf\x27\x1c\x00\x04");
    let crc = crc32(&header[12..]);
    header[8..12].copy_from_slice(&crc.to_le_bytes());
    header
}
const MAP: &[u8] = b"MAP";
// The pretend 7z lays out as signature, body, map: the map at the end says
// where the body's member lies.
fn body() -> ByteRange {
    ByteRange::at(seven_z().len() as u64, 5)
}

fn map_at() -> ByteRange {
    ByteRange::at(body().end, MAP.len() as u64)
}

// A pretend store whose map comes last: until the map arrives the body has
// no place and is held; once it has, the body is "inner.bin".
struct Late {
    mapped: bool,
}

impl Object for Late {
    // It keeps no record of what it said of its posted bytes.
    fn unposted(&mut self, _: weaver_graph::VolumeIndex, _: weaver_graph::ByteRange) {}

    fn kind(&self) -> Kind {
        Kind::SevenZipStore
    }

    fn deliver(
        &mut self,
        delivery: Delivery<'_>,
        _: &mut dyn FnMut(&weaver_graph::Placement, &mut Vec<u8>) -> bool,
        out: &mut Vec<Emit>,
    ) {
        let range = ByteRange::at(delivery.offset, delivery.bytes.len() as u64);
        if !self.mapped && range.intersect(map_at()) == Some(map_at()) {
            self.mapped = true;
            out.push(Emit::Member {
                member: MemberId(0),
                name: "inner.bin".into(),
                length: Some(body().len()),
            });
        }
        if let Some(part) = range.intersect(body()).filter(|_| self.mapped) {
            let start = (part.start - delivery.offset) as usize;
            out.push(Emit::Bytes {
                member: MemberId(0),
                offset: part.start - body().start,
                bytes: Bytes::copy_from_slice(&delivery.bytes[start..start + part.len() as usize]),
                provenance: delivery.provenance,
            });
        }
    }

    fn volume_closed(&mut self, _: VolumeIndex, _: &mut Vec<Emit>) {}
    fn volume_lost(&mut self, _: VolumeIndex, _: ByteRange, _: &mut Vec<Emit>) {}
    fn wants(&self, _: &mut Vec<Seek>) {}
    fn pulled(&mut self, _: Seek, _: Pull, _: &mut Vec<Emit>) {}

    fn locate(&self, _: VolumeIndex, range: ByteRange, out: &mut Vec<Placement>) {
        if let Some(header) = range.intersect(ByteRange::new(0, body().start)) {
            out.push(Placement::Envelope {
                envelope: 0,
                range: header,
            });
        }
        if let Some(part) = range.intersect(body()) {
            out.push(if self.mapped {
                Placement::Member {
                    member: MemberId(0),
                    range: part.shift(-(body().start as i64)),
                }
            } else {
                Placement::Held { range: part }
            });
        }
        if let Some(map) = range.intersect(ByteRange::new(body().end, u64::MAX)) {
            out.push(Placement::Envelope {
                envelope: 0,
                range: map,
            });
        }
    }

    fn extent(&self, _: MemberId, _: ByteRange, _: &mut Vec<Extent>) -> Located {
        Located::Never
    }

    fn envelope(&self, _: u32, _: ByteRange) -> Option<Bytes> {
        None
    }
}

// Builds the pretend store, and the pretend recovery set with the job's
// record of what it was fed and the fix it makes when asked to repair.
struct Build {
    par: Arc<Mutex<Seen>>,
    fix: Option<Fix>,
    inner: u64,
    heard: Arc<Mutex<Vec<u64>>>,
}

impl Builder for Build {
    fn build(&mut self, kind: Kind, _: ObjectId) -> Option<Box<dyn Object>> {
        match kind {
            Kind::RarStore => Some(Box::new(Store {
                announced: false,
                length: self.inner,
            })),
            Kind::SevenZipStore => Some(Box::new(Late { mapped: false })),
            Kind::Xz => Some(Box::new(streams::Decoded::new(Arc::clone(&self.heard)))),
            Kind::Par2 => Some(Box::new(Par {
                volume: None,
                others: Vec::new(),
                fixing: None,
                fix: self.fix,
                seen: Arc::clone(&self.par),
            })),
            _ => None,
        }
    }

    fn read(
        &self,
        _: &weaver_graph::Tree,
        _: weaver_graph::NodeId,
        _: weaver_graph::VolumeIndex,
        _: &weaver_graph::Placement,
        _: &mut Vec<u8>,
    ) -> bool {
        false
    }
}

// The source's segments: one per delivered range, with a checksum the
// source vouches for.
const SOURCE_CRC: u32 = 0x5eed_c0de;

struct Source;

impl Segments for Source {
    fn segments_for(&self, node: NodeId, _: VolumeIndex, range: ByteRange, out: &mut Vec<Segment>) {
        // As the article root: its checksums are of the root's bytes only.
        if node != NodeId::ROOT {
            return;
        }
        out.push(Segment {
            range,
            crc32: SOURCE_CRC,
            verified: true,
        });
    }
}

// What the pretend recovery set saw: its evidence, and what it read when
// asked to repair.
#[derive(Default)]
struct Seen {
    fed: Vec<Segment>,
    read: Option<Vec<u8>>,
    volume: Option<Vec<u8>>,
}

// The repair the pretend set makes: read the named member, write a fix.
#[derive(Clone, Copy)]
struct Fix {
    name: &'static str,
    read: ByteRange,
    fix: (u64, &'static [u8]),
}

// A recovery object the tree holds as a node: bound to its own member, it
// covers the members beside it except any called `other.*`, keeps its own
// bytes in its member's file, and recognises a member by what has arrived
// of it, renaming it.
//
// Its front is a whole PAR2 packet header, which the sniffer reads before
// it names the kind: magic, a packet length of 64, and the hash, set id and
// type fields, which nothing here reads.
const PAR2_SIGNATURE: &[u8] = &{
    let mut header = [0u8; 64];
    let magic = *b"PAR2\0PKT\x40";
    let mut at = 0;
    while at < magic.len() {
        header[at] = magic[at];
        at += 1;
    }
    header
};

struct Par {
    volume: Option<MemberId>,
    others: Vec<MemberId>,
    fixing: Option<MemberId>,
    fix: Option<Fix>,
    seen: Arc<Mutex<Seen>>,
}

impl Object for Par {
    // It keeps no record of what it said of its posted bytes.
    fn unposted(&mut self, _: weaver_graph::VolumeIndex, _: weaver_graph::ByteRange) {}

    fn kind(&self) -> Kind {
        Kind::Par2
    }

    fn deliver(
        &mut self,
        _: Delivery<'_>,
        _: &mut dyn FnMut(&weaver_graph::Placement, &mut Vec<u8>) -> bool,
        _: &mut Vec<Emit>,
    ) {
    }
    fn volume_closed(&mut self, _: VolumeIndex, _: &mut Vec<Emit>) {}
    fn volume_lost(&mut self, _: VolumeIndex, _: ByteRange, _: &mut Vec<Emit>) {}
    fn wants(&self, _: &mut Vec<Seek>) {}
    fn pulled(&mut self, _: Seek, _: Pull, _: &mut Vec<Emit>) {}

    fn locate(&self, _: VolumeIndex, range: ByteRange, out: &mut Vec<Placement>) {
        out.push(Placement::Envelope { envelope: 0, range });
    }

    fn extent(&self, _: MemberId, _: ByteRange, _: &mut Vec<Extent>) -> Located {
        Located::Never
    }

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

impl Recovery for Par {
    fn bind(&mut self, namespace: &mut weaver_graph::Namespace, renamed: &mut Vec<MemberId>) {
        self.volume = namespace.find_by_name("set.par2").map(|own| own.id);
        self.fixing = self
            .fix
            .and_then(|fix| namespace.find_by_name(fix.name))
            .map(|member| member.id);
        let recognised: Vec<MemberId> = namespace
            .members()
            .filter(|member| member.name == "3f9a.bin" && member.coverage.bytes_covered() > 0)
            .map(|member| member.id)
            .collect();
        // What it does not describe, and the obfuscated member until its
        // bytes say what it is.
        self.others = namespace
            .members()
            .filter(|member| member.name.starts_with("other.") || member.name == "3f9a.bin")
            .filter(|member| !recognised.contains(&member.id))
            .map(|member| member.id)
            .collect();
        for member in recognised {
            namespace.rename(member, "show.mkv");
            renamed.push(member);
        }
    }

    fn covers(&self, member: MemberId) -> bool {
        self.volume.is_some_and(|own| own != member) && !self.others.contains(&member)
    }

    fn feed(&mut self, _: MemberId, segments: &[Segment]) {
        self.seen.lock().unwrap().fed.extend_from_slice(segments);
    }

    fn verdicts(&self, _: MemberId, _: &mut Vec<SliceVerdict>) {}
    fn wants(&self, _: &mut Vec<Seek>) {}

    fn repair(&mut self, access: &mut dyn MemberAccess) -> RepairOutcome {
        let mut volume = Vec::new();
        let span = ByteRange::new(0, PAR2_SIGNATURE.len() as u64);
        if access.read_volume(VolumeIndex(0), span, &mut volume) {
            self.seen.lock().unwrap().volume = Some(volume);
        }
        let (Some(fix), Some(member)) = (self.fix, self.fixing) else {
            return RepairOutcome::Clean;
        };
        let mut bytes = Vec::new();
        if access.read(member, fix.read, &mut bytes) {
            self.seen.lock().unwrap().read = Some(bytes);
        }
        access.write_repaired(member, fix.fix.0, fix.fix.1);
        RepairOutcome::Repaired { slices: 1 }
    }

    fn outcome(&self) -> Option<RepairOutcome> {
        None
    }
}

const PLENTY: HoldBudgets = HoldBudgets {
    resident_limit: u64::MAX,
    scratch_total: u64::MAX,
    scratch_ceiling: u64::MAX,
    disk_reserve: 0,
};

struct Job {
    dir: tempfile::TempDir,
    tree: Tree,
    writer: NamespaceWriter,
    accountant: Arc<HoldsAccountant>,
    par: Arc<Mutex<Seen>>,
    fix: Option<Fix>,
    // The length of the member the pretend store holds.
    inner: u64,
    // What every pretend stream had been delivered each time it heard its
    // volume closed.
    heard: Arc<Mutex<Vec<u64>>>,
    out: Vec<Effect>,
}

impl Job {
    fn new(budgets: HoldBudgets) -> Self {
        let dir = tempfile::tempdir().expect("temp dir");
        let accountant = Arc::new(HoldsAccountant::new(budgets));
        let writer = NamespaceWriter::new(
            1,
            dir.path().to_path_buf(),
            Arc::clone(&accountant),
            CapacityReader::unknown(),
        );
        Self {
            dir,
            tree: Tree::new(),
            writer,
            accountant,
            par: Arc::new(Mutex::new(Seen::default())),
            fix: None,
            inner: 5,
            heard: Arc::default(),
            out: Vec::new(),
        }
    }

    // A recovery set at the root, bound before anything else arrives.
    fn with_set(mut self, fix: Option<Fix>) -> Self {
        self.fix = fix;
        let set = self.member("set.par2", None);
        self.send(set, 0, PAR2_SIGNATURE, Provenance::Fresh);
        self
    }

    fn member(&mut self, name: &str, length: Option<u64>) -> MemberId {
        let member = self.tree.announce_root(name, length, &mut Vec::new());
        let mut build = self.build();
        self.writer
            .announced(
                &mut self.tree,
                &mut build,
                &Source,
                NodeId::ROOT,
                member,
                &mut self.out,
            )
            .expect("announce");
        member
    }

    fn send(&mut self, member: MemberId, offset: u64, bytes: &[u8], provenance: Provenance) {
        self.send_to(NodeId::ROOT, member, offset, bytes, provenance);
    }

    fn send_to(
        &mut self,
        node: NodeId,
        member: MemberId,
        offset: u64,
        bytes: &[u8],
        provenance: Provenance,
    ) {
        let arrival = Arrival {
            node,
            member,
            offset,
            bytes: Bytes::copy_from_slice(bytes),
            provenance,
        };
        let mut build = self.build();
        self.writer
            .deliver(&mut self.tree, &mut build, &Source, arrival, &mut self.out)
            .expect("deliver");
    }

    fn close(&mut self, member: MemberId) {
        let mut build = self.build();
        self.writer
            .close(
                &mut self.tree,
                &mut build,
                &Source,
                NodeId::ROOT,
                member,
                &mut self.out,
            )
            .expect("close");
    }

    fn build(&self) -> Build {
        Build {
            par: Arc::clone(&self.par),
            fix: self.fix,
            inner: self.inner,
            heard: Arc::clone(&self.heard),
        }
    }

    fn file(&self, name: &str) -> Vec<u8> {
        std::fs::read(self.dir.path().join(name)).expect("leaf file")
    }

    fn root(&self) -> &weaver_graph::Namespace {
        self.tree.namespace(NodeId::ROOT).expect("root")
    }

    fn is_open(&self, node: NodeId, member: MemberId) -> bool {
        self.writer.files[&node].leaves[&member].is_open()
    }

    // Run each repair that went out, here, and hand it back as its return
    // through the mail would. What each set came to, in the order lent.
    fn run_lent(&mut self, build: &mut dyn Builder, lent: Inline) -> Vec<(NodeId, RepairOutcome)> {
        let mut ruled = Vec::new();
        for loan in lent.0 {
            let returned = loan.run();
            ruled.extend(returned.outcome.map(|outcome| (returned.set, outcome)));
            self.writer
                .returned(
                    &mut self.tree,
                    build,
                    &Source,
                    &weaver_objects::Objects,
                    returned,
                    &mut self.out,
                )
                .expect("returned");
        }
        ruled
    }

    // Repair the sets now, due or not.
    fn repair(
        &mut self,
        build: &mut dyn Builder,
        node: NodeId,
        due: &[NodeId],
    ) -> Vec<(NodeId, RepairOutcome)> {
        let mut lent = Inline::default();
        self.writer
            .repair(&mut self.tree, node, due, &mut lent)
            .expect("lent");
        self.run_lent(build, lent)
    }
}

// What goes out, kept for the test to run: every due set at once.
#[derive(Default)]
struct Inline(Vec<Loan>);

impl Lender for Inline {
    fn admit(&mut self) -> bool {
        true
    }

    fn lend(&mut self, loan: Loan) {
        self.0.push(loan);
    }
}

#[test]
fn bytes_before_the_member_is_decided_are_held_then_replayed_to_its_leaf() {
    let mut job = Job::new(PLENTY);
    let member = job.member("notes.bin", Some(11));
    // "Ra" could still be a RAR signature; the bytes after it say nothing
    // until the front does.
    job.send(member, 0, b"Ra", Provenance::Fresh);
    job.send(member, 6, b"world", Provenance::Fresh);
    assert!(job.tree.is_undecided(NodeId::ROOT, member));
    assert_eq!(job.writer.held_bytes(NodeId::ROOT), 7);
    assert!(!job.dir.path().join("notes.bin").exists());

    job.send(member, 2, b"ws, ", Provenance::Fresh);
    assert!(!job.tree.is_undecided(NodeId::ROOT, member));
    assert_eq!(job.writer.held_bytes(NodeId::ROOT), 0);
    assert_eq!(job.accountant.usage().resident, 0);
    assert_eq!(job.file("notes.bin"), b"Raws, world");
}

#[test]
fn a_breach_refuses_the_member_to_a_leaf_which_receives_every_byte() {
    let mut job = Job::new(HoldBudgets {
        resident_limit: 4,
        scratch_total: 0,
        scratch_ceiling: 0,
        disk_reserve: 0,
    });
    let member = job.member("notes.bin", None);
    job.send(member, 0, b"Ra", Provenance::Fresh);
    job.send(member, 10, b"tail..", Provenance::Fresh);
    assert_eq!(
        job.writer.refused(NodeId::ROOT, member),
        Some(Why::HoldsBreached)
    );
    assert!(job.out.contains(&Effect::Refused {
        node: NodeId::ROOT,
        member,
        why: Why::HoldsBreached,
    }));
    assert_eq!(job.accountant.usage().resident, 0);

    // Decided now: the rest goes straight to the file.
    job.send(member, 2, b"r! mid.", Provenance::Fresh);
    job.close(member);
    assert_eq!(job.file("notes.bin"), b"Rar! mid.\0tail..");
}

#[test]
fn held_bytes_paged_to_scratch_come_back_intact() {
    let mut job = Job::new(HoldBudgets {
        resident_limit: 2,
        ..PLENTY
    });
    let member = job.member("notes.bin", None);
    job.send(member, 0, b"Ra", Provenance::Fresh);
    job.send(member, 4, b"back", Provenance::Fresh);
    assert_eq!(job.accountant.usage().scratch, 4);
    job.send(member, 2, b"rx", Provenance::Fresh);
    job.close(member);
    assert_eq!(job.file("notes.bin"), b"Rarxback");
    assert_eq!(job.accountant.usage().scratch, 0);
}

// A hold paged to scratch under pressure is not a record: its span is
// reused once nothing lives on scratch, so only a stop's page-out, after
// which nothing is held again, is written down.
#[test]
fn holds_paged_out_under_pressure_are_not_recorded_before_a_stop() {
    let mut job = Job::new(HoldBudgets {
        resident_limit: 4,
        scratch_total: u64::MAX,
        scratch_ceiling: u64::MAX,
        disk_reserve: 0,
    });
    let member = job.member("notes.bin", Some(11));
    job.send(member, 0, b"Ra", Provenance::Fresh);
    job.send(member, 6, b"world", Provenance::Fresh);
    assert_eq!(job.accountant.usage().scratch, 5);
    assert!(job.writer.hold_records(NodeId::ROOT).is_empty());

    job.writer.page_out_holds().expect("page out");
    assert_eq!(job.writer.hold_records(NodeId::ROOT).len(), 2);
}

#[test]
fn holds_paged_out_at_a_stop_read_back_in_a_writer_rebuilt_from_their_records() {
    let mut job = Job::new(PLENTY);
    let member = job.member("notes.bin", Some(11));
    job.send(member, 0, b"Ra", Provenance::Fresh);
    job.send(member, 6, b"world", Provenance::Fresh);
    assert_eq!(job.accountant.usage().resident, 7);

    job.writer.page_out_holds().expect("page out");
    assert_eq!(job.accountant.usage().resident, 0);
    assert_eq!(job.accountant.usage().scratch, 7);
    let records: Vec<HoldRecord> = job
        .writer
        .hold_records(NodeId::ROOT)
        .into_iter()
        .map(|held| HoldRecord {
            node: NodeId::ROOT,
            held,
        })
        .collect();
    assert_eq!(records.len(), 2);

    // The process goes away; its scratch file stays.
    let writer = std::mem::replace(
        &mut job.writer,
        NamespaceWriter::new(
            1,
            job.dir.path().to_path_buf(),
            Arc::clone(&job.accountant),
            CapacityReader::unknown(),
        ),
    );
    drop(writer);
    assert_eq!(job.accountant.usage().scratch, 0);

    job.writer
        .restore_holds(&job.tree, &records)
        .expect("restore holds");
    assert_eq!(job.accountant.usage().scratch, 7);
    let mut bytes = Vec::new();
    assert!(job.writer.access(&job.tree).read_at(
        NodeId::ROOT,
        member,
        ByteRange::new(6, 11),
        &mut bytes
    ));
    assert_eq!(bytes, b"world");
    bytes.clear();
    assert!(job.writer.access(&job.tree).read_at(
        NodeId::ROOT,
        member,
        ByteRange::new(0, 2),
        &mut bytes
    ));
    assert_eq!(bytes, b"Ra");

    // And they go down as any hold does once the member is decided.
    job.send(member, 2, b"ws, ", Provenance::Fresh);
    assert_eq!(job.file("notes.bin"), b"Raws, world");
    assert_eq!(job.accountant.usage().scratch, 0);
}

#[test]
fn a_hold_record_past_its_scratch_file_is_not_adopted() {
    let mut job = Job::new(PLENTY);
    let member = job.member("notes.bin", Some(11));
    let records = [HoldRecord {
        node: NodeId::ROOT,
        held: ScratchHold {
            volume: VolumeIndex(member.0),
            range: ByteRange::new(6, 11),
            scratch_offset: 0,
        },
    }];
    job.writer
        .restore_holds(&job.tree, &records)
        .expect("restore holds");
    assert_eq!(job.accountant.usage().scratch, 0);
    let mut bytes = Vec::new();
    assert!(!job.writer.access(&job.tree).read_at(
        NodeId::ROOT,
        member,
        ByteRange::new(6, 11),
        &mut bytes
    ));
}

#[test]
fn two_members_that_want_one_name_are_both_written() {
    let mut job = Job::new(PLENTY);
    let first = job.member("subs.srt", Some(5));
    let second = job.member("subs.srt", Some(6));
    job.send(first, 0, b"first", Provenance::Fresh);
    job.send(second, 0, b"second", Provenance::Fresh);
    assert_eq!(job.file("subs.srt"), b"first");
    assert_eq!(job.file("subs (1).srt"), b"second");
    assert!(job.root().get(first).unwrap().is_complete());
    assert!(job.root().get(second).unwrap().is_complete());
}

#[test]
fn a_member_closes_exactly_when_covered_at_its_length() {
    let mut job = Job::new(PLENTY);
    let member = job.member("video.mkv", Some(9));
    job.send(member, 0, b"abc", Provenance::Fresh);
    assert!(job.is_open(NodeId::ROOT, member));
    assert_eq!(
        std::fs::metadata(job.dir.path().join("video.mkv"))
            .unwrap()
            .len(),
        9,
        "a known length is reserved on the first write"
    );
    job.send(member, 6, b"end", Provenance::Fresh);
    assert!(job.is_open(NodeId::ROOT, member));
    job.send(member, 3, b"mid", Provenance::Fresh);
    assert!(!job.is_open(NodeId::ROOT, member));
    assert_eq!(job.file("video.mkv"), b"abcmidend");
}

// The tree's coverage and the leaf on disk agree: a run the coverage records
// that the file no longer gives back ends the replay with the file's error.
#[test]
fn replaying_a_leaf_whose_file_is_gone_ends_in_the_leaf_error() {
    let mut job = Job::new(PLENTY);
    let member = job.member("video.mkv", Some(5));
    job.send(member, 0, b"bytes", Provenance::Fresh);
    assert!(!job.is_open(NodeId::ROOT, member));
    std::fs::remove_file(job.dir.path().join("video.mkv")).expect("remove the leaf");

    let mut build = job.build();
    let replayed = job.writer.replay_file(
        &mut job.tree,
        &mut build,
        &Source,
        NodeId::ROOT,
        member,
        &mut job.out,
    );
    assert!(
        matches!(&replayed, Err(WriteError::Leaf(LeafError::Io { source, .. })) if source.kind() == std::io::ErrorKind::NotFound),
        "{replayed:?}"
    );
}

// A range held as damage may never have arrived: a lost tail past what the
// file holds is no read, and the replay hands down what the file has.
#[test]
fn replaying_a_leaf_whose_lost_tail_was_never_written_replays_what_it_holds() {
    let mut job = Job::new(PLENTY);
    let member = job.member("video.mkv", None);
    job.send(member, 0, b"bytes", Provenance::Fresh);
    let mut effects = Vec::new();
    job.tree.damage(
        NodeId::ROOT,
        VolumeIndex(member.0),
        ByteRange::new(5, 15),
        &mut effects,
    );
    let mut build = job.build();
    job.writer
        .apply_all(&mut job.tree, &mut build, &Source, effects, &mut job.out)
        .expect("apply the loss");
    assert_eq!(
        std::fs::metadata(job.dir.path().join("video.mkv"))
            .unwrap()
            .len(),
        5,
        "a member of unknown length is only as long as what arrived"
    );

    let replayed = job.writer.replay_file(
        &mut job.tree,
        &mut build,
        &Source,
        NodeId::ROOT,
        member,
        &mut job.out,
    );
    assert!(replayed.is_ok(), "{replayed:?}");
    assert_eq!(job.file("video.mkv"), b"bytes");
}

#[test]
fn a_member_of_unknown_length_is_sized_when_it_closes() {
    let mut job = Job::new(PLENTY);
    let member = job.member("post.uu", None);
    job.send(member, 0, b"abc", Provenance::Fresh);
    job.send(member, 3, b"def", Provenance::Fresh);
    job.close(member);
    assert_eq!(job.root().get(member).unwrap().length, Some(6));
    assert_eq!(job.file("post.uu"), b"abcdef");
}

#[test]
fn directory_members_and_empty_members_exist_once_announced() {
    let mut job = Job::new(PLENTY);
    let extras = job.member("extras/", Some(0));
    job.member("empty.txt", Some(0));
    let nested = job.member("extras/inner.txt", Some(2));
    job.send(nested, 0, b"hi", Provenance::Fresh);
    assert!(job.dir.path().join("extras").is_dir());
    job.close(extras);
    assert!(job.dir.path().join("extras").is_dir());
    assert_eq!(job.file("empty.txt"), b"");
    assert_eq!(job.file("extras/inner.txt"), b"hi");
}

// A store that learns its map last: the body arrives first and has no
// place, so it is held for the volume; the map announces the member, and
// what was held goes down again to reach it.
#[test]
fn bytes_held_before_a_store_knows_its_map_reach_the_member_once_it_does() {
    let mut job = Job::new(PLENTY);
    let member = job.member("a.7z", Some(map_at().end));
    let mut front = seven_z().to_vec();
    front.extend_from_slice(b"hello");
    job.send(member, 0, &front, Provenance::Fresh);
    assert!(!job.tree.is_undecided(NodeId::ROOT, member));
    assert_eq!(job.writer.held_bytes(NodeId::ROOT), body().len());
    assert!(!job.dir.path().join("inner.bin").exists());

    job.send(member, map_at().start, MAP, Provenance::Fresh);
    assert_eq!(job.writer.held_bytes(NodeId::ROOT), 0);
    assert_eq!(job.accountant.usage().resident, 0);
    assert_eq!(job.file("inner.bin"), b"hello");

    // The volume reads back whole through the object's placements.
    let access = job.writer.access(&job.tree);
    let mut posted = Vec::new();
    assert!(access.read_at(
        NodeId::ROOT,
        member,
        ByteRange::new(0, map_at().end),
        &mut posted
    ));
    let mut whole = front;
    whole.extend_from_slice(MAP);
    assert_eq!(posted, whole);
}

// The ledger confines every name to the working dir: a name that would
// climb out is written under the part of it that stays inside.
#[test]
fn a_name_that_would_escape_the_working_dir_is_confined() {
    let mut job = Job::new(PLENTY);
    let member = job
        .tree
        .announce_root("../escape.txt", Some(2), &mut Vec::new());
    job.send(member, 0, b"no", Provenance::Fresh);
    assert_eq!(job.file("escape.txt"), b"no");
}

#[test]
fn a_rename_moves_the_file_and_the_name_allocation() {
    let mut job = Job::new(PLENTY);
    let member = job.member("3f9a.bin", Some(4));
    job.send(member, 0, b"data", Provenance::Fresh);
    let mut build = job.build();
    let renamed = job
        .writer
        .rename(
            &mut job.tree,
            &mut build,
            NodeId::ROOT,
            member,
            "show.part01.rar",
        )
        .expect("rename");
    assert_eq!(renamed, "show.part01.rar");
    assert_eq!(job.file("show.part01.rar"), b"data");
    assert!(!job.dir.path().join("3f9a.bin").exists());
    let next = job.member("3f9a.bin", None);
    assert_eq!(job.root().get(next).unwrap().output_name, "3f9a.bin");
}

#[test]
fn damaged_bytes_are_written_and_marked_and_a_repair_overwrites_them() {
    let mut job = Job::new(PLENTY).with_set(Some(Fix {
        name: "video.mkv",
        read: ByteRange::new(0, 8),
        fix: (2, b"OK"),
    }));
    let member = job.member("video.mkv", Some(8));
    job.send(member, 0, b"go", Provenance::Fresh);
    job.send(member, 2, b"XX", Provenance::Damaged);
    job.send(member, 4, b"good", Provenance::Fresh);
    assert_eq!(job.file("video.mkv"), b"goXXgood");
    assert!(!job.root().is_clean());
    assert_eq!(
        job.root().damaged_ranges().collect::<Vec<_>>(),
        vec![(member, ByteRange::new(2, 4))]
    );

    let mut build = job.build();
    let due = job.tree.recovery_nodes(NodeId::ROOT);
    let outcomes = job.repair(&mut build, NodeId::ROOT, &due);
    let outcomes: Vec<RepairOutcome> = outcomes.into_iter().map(|(_, outcome)| outcome).collect();
    assert_eq!(outcomes, vec![RepairOutcome::Repaired { slices: 1 }]);
    let seen = job.par.lock().unwrap();
    assert_eq!(seen.read.as_deref(), Some(&b"goXXgood"[..]));
    assert_eq!(
        seen.volume.as_deref(),
        Some(PAR2_SIGNATURE),
        "the set reads its own volume back from its member's file"
    );
    drop(seen);
    assert_eq!(job.writer.read_backs(), 1);
    assert_eq!(job.file("set.par2"), PAR2_SIGNATURE);
    assert_eq!(job.file("video.mkv"), b"goOKgood");
    let coverage = &job.root().get(member).unwrap().coverage;
    assert!(coverage.is_clean());
    assert!(
        coverage
            .overlapping(ByteRange::new(2, 4))
            .all(|run| run.provenance == Provenance::Repaired)
    );
}

#[test]
fn the_segment_feed_is_evidence_unless_the_bytes_are_damage() {
    let mut job = Job::new(PLENTY).with_set(None);
    let member = job.member("video.mkv", Some(12));
    let uncovered = job.member("other.mkv", Some(2));
    job.send(member, 0, b"fresh", Provenance::Fresh);
    job.send(member, 5, b"bad", Provenance::Damaged);
    job.send(member, 8, b"good", Provenance::Repaired);
    job.send(uncovered, 0, b"no", Provenance::Fresh);
    assert_eq!(
        job.par.lock().unwrap().fed,
        vec![
            Segment {
                range: ByteRange::new(0, 5),
                crc32: SOURCE_CRC,
                verified: true,
            },
            Segment {
                range: ByteRange::new(5, 8),
                crc32: crc32(b"bad"),
                verified: false,
            },
            Segment {
                range: ByteRange::new(8, 12),
                crc32: crc32(b"good"),
                verified: true,
            },
        ]
    );
}

// The first three bytes of the signature leave the member undecided; the
// rest binds it to the store, and the held front goes down after it.
fn bound_store(job: &mut Job) -> (MemberId, NodeId) {
    let member = job.member("pack.bin", None);
    job.send(member, 0, &SIGNATURE[..3], Provenance::Fresh);
    let mut rest = SIGNATURE[3..].to_vec();
    rest.extend_from_slice(b"hello");
    job.send(member, 3, &rest, Provenance::Fresh);
    let (child, _) = job
        .tree
        .child_of(NodeId::ROOT, member)
        .expect("bound to the store");
    (member, child)
}

#[test]
fn a_bound_member_reads_back_through_its_envelope_and_its_child() {
    let mut job = Job::new(PLENTY);
    let (member, child) = bound_store(&mut job);
    assert_eq!(job.writer.held_bytes(NodeId::ROOT), 0);
    assert_eq!(job.file("inner.bin"), b"hello");
    assert!(!job.dir.path().join("pack.bin").exists());

    let access = job.writer.access(&job.tree);
    let mut posted = Vec::new();
    assert!(access.read_at(NodeId::ROOT, member, ByteRange::new(0, 12), &mut posted));
    assert_eq!(posted, [SIGNATURE, b"hello"].concat());

    let mut inner = Vec::new();
    assert!(access.read_at(child, MemberId(0), ByteRange::new(1, 4), &mut inner));
    assert_eq!(inner, b"ell");

    let mut beyond = vec![1u8];
    assert!(!access.read_at(NodeId::ROOT, member, ByteRange::new(10, 20), &mut beyond));
    assert_eq!(
        beyond,
        vec![1u8],
        "a failed read leaves the buffer as it was"
    );
}

#[test]
fn a_retraction_truncates_the_leaf_and_keeps_its_name_for_what_comes_back() {
    let mut job = Job::new(PLENTY);
    let (member, child) = bound_store(&mut job);
    job.send(member, 12, b"RETRACT", Provenance::Fresh);
    assert!(job.out.contains(&Effect::Retracted {
        node: child,
        member: MemberId(0),
    }));
    assert_eq!(job.file("inner.bin"), b"");
    let inner = job.tree.namespace(child).unwrap().get(MemberId(0)).unwrap();
    assert_eq!(inner.output_name, "inner.bin");
    assert_eq!(inner.coverage.bytes_covered(), 0);
}

// The store closed "inner.bin" whole and had "open.bin" part written when it
// gave up. The volume is written out as posted, the closed member stays
// delivered beside it, and the open one's file is gone: the volume stands
// for it, and the member stays only buried.
#[test]
fn a_refusal_keeps_the_member_it_closed_and_replaces_the_open_one_with_the_volume() {
    let mut job = Job::new(PLENTY);
    let (member, child) = bound_store(&mut job);
    let mut build = job.build();
    for emit in [
        Emit::MemberClosed(MemberId(0)),
        Emit::Member {
            member: MemberId(1),
            name: "open.bin".into(),
            length: Some(4),
        },
        Emit::Bytes {
            member: MemberId(1),
            offset: 0,
            bytes: Bytes::from_static(b"ab"),
            provenance: Provenance::Fresh,
        },
    ] {
        job.writer
            .said(
                &mut job.tree,
                &mut build,
                &Source,
                child,
                emit,
                &mut job.out,
            )
            .expect("said");
    }
    assert!(job.file("open.bin").starts_with(b"ab"));
    job.writer
        .said(
            &mut job.tree,
            &mut build,
            &Source,
            child,
            Emit::Refuse(Why::Compressed),
            &mut job.out,
        )
        .expect("refuse");
    assert_eq!(job.file("pack.bin"), [SIGNATURE, b"hello"].concat());
    assert_eq!(job.file("inner.bin"), b"hello");
    assert!(!job.dir.path().join("open.bin").exists());
    assert!(job.tree.buried(child));
    let kept = job.tree.namespace(child).expect("what it closed stays");
    assert!(kept.get(MemberId(0)).is_some_and(|inner| inner.is_whole()));
    // The open member stays only buried, with no file: the tree's word
    // that the store left it open.
    assert!(job.tree.buried_member(child, MemberId(1)));
    assert!(job.tree.left_open(NodeId::ROOT, member));
    assert_eq!(
        job.writer
            .files(child)
            .map(|(id, _)| id)
            .collect::<Vec<_>>(),
        [MemberId(0)]
    );
    assert!(job.root().get(member).unwrap().is_leaf());
}

// A root member recognised as the name a store's member was written under
// takes it, since it sits higher; the store's file moves aside on disk and
// keeps its bytes.
#[test]
fn a_shallower_member_that_claims_a_written_name_moves_the_deeper_file_aside() {
    let mut job = Job::new(PLENTY);
    let (_, child) = bound_store(&mut job);
    assert_eq!(job.file("inner.bin"), b"hello");
    let member = job.member("3f9a.bin", Some(4));
    job.send(member, 0, b"data", Provenance::Fresh);
    let mut build = job.build();
    let renamed = job
        .writer
        .rename(&mut job.tree, &mut build, NodeId::ROOT, member, "INNER.bin")
        .expect("rename");
    assert_eq!(renamed, "INNER.bin");
    let inner = job.tree.namespace(child).unwrap().get(MemberId(0)).unwrap();
    assert_eq!(inner.output_name, "inner (1).bin");
    assert_eq!(job.file("INNER.bin"), b"data");
    assert_eq!(job.file("inner (1).bin"), b"hello");
    assert!(!job.dir.path().join("3f9a.bin").exists());
}

#[test]
fn a_recovery_object_in_the_tree_is_fed_and_its_renames_move_the_file() {
    let mut job = Job::new(PLENTY);
    let set = job.member("set.par2", None);
    job.send(set, 0, PAR2_SIGNATURE, Provenance::Fresh);
    assert!(job.tree.child_of(NodeId::ROOT, set).is_some());
    assert!(
        job.par.lock().unwrap().fed.is_empty(),
        "its own bytes are not evidence"
    );

    assert_eq!(job.file("set.par2"), PAR2_SIGNATURE);

    let member = job.member("3f9a.bin", Some(8));
    job.send(member, 0, b"data", Provenance::Fresh);
    assert_eq!(
        job.par.lock().unwrap().fed,
        vec![Segment {
            range: ByteRange::new(0, 4),
            crc32: SOURCE_CRC,
            verified: true,
        }]
    );

    // The member's front changed, so the object matched again and
    // recognised the member by what has arrived of it.
    assert_eq!(job.root().get(member).unwrap().output_name, "show.mkv");
    assert!(!job.dir.path().join("3f9a.bin").exists());
    job.send(member, 4, b"more", Provenance::Fresh);
    assert_eq!(job.file("show.mkv"), b"datamore");
}

#[test]
fn segments_that_arrived_before_the_set_are_fed_when_it_covers_the_member() {
    let mut job = Job::new(PLENTY);
    let member = job.member("video.mkv", Some(12));
    job.send(member, 0, b"fresh", Provenance::Fresh);
    job.send(member, 5, b"bad", Provenance::Damaged);
    job.send(member, 8, b"good", Provenance::Repaired);
    assert!(job.par.lock().unwrap().fed.is_empty());

    let set = job.member("set.par2", None);
    job.send(set, 0, PAR2_SIGNATURE, Provenance::Fresh);
    assert_eq!(
        job.par.lock().unwrap().fed,
        vec![
            Segment {
                range: ByteRange::new(0, 5),
                crc32: SOURCE_CRC,
                verified: true,
            },
            Segment {
                range: ByteRange::new(5, 8),
                crc32: SOURCE_CRC,
                verified: false,
            },
        ],
        "a repaired run is not the source's bytes, so it gives nothing"
    );
}

#[test]
fn dropping_the_writer_gives_its_holds_back() {
    let mut job = Job::new(PLENTY);
    let member = job.member("notes.bin", None);
    job.send(member, 0, b"Ra", Provenance::Fresh);
    assert_eq!(job.accountant.usage().resident, 2);
    let Job {
        writer, accountant, ..
    } = job;
    drop(writer);
    assert_eq!(accountant.usage().resident, 0);
    assert_eq!(accountant.usage().namespaces, 0);
}

// Below the root an object's bytes reach the object under it as its emits,
// never through the root's path, and what that object cannot place yet is
// held for its volume in the namespace above it all the same.
#[test]
fn bytes_held_below_the_root_reach_their_member_once_its_store_knows_its_map() {
    let mut job = Job::new(PLENTY);
    job.inner = map_at().end;
    let member = job.member("pack.bin", Some(HEADER + map_at().end));
    let mut front = SIGNATURE.to_vec();
    front.extend_from_slice(&seven_z());
    front.extend_from_slice(b"hello");
    job.send(member, 0, &front, Provenance::Fresh);
    let (store, _) = job.tree.child_of(NodeId::ROOT, member).expect("a store");
    let (late, _) = job.tree.child_of(store, MemberId(0)).expect("a 7z inside");
    assert_eq!(job.writer.held_bytes(store), body().len());

    job.send(member, HEADER + map_at().start, MAP, Provenance::Fresh);
    assert_eq!(job.writer.held_bytes(store), 0);
    assert_eq!(job.accountant.usage().resident, 0);
    let access = job.writer.access(&job.tree);
    let mut inner = Vec::new();
    assert!(access.read_at(
        late,
        MemberId(0),
        body().shift(-(body().start as i64)),
        &mut inner
    ));
    assert_eq!(inner, b"hello");
}

// A source with no checksums for what it delivers, as for every namespace
// below the root.
struct Unsourced;

impl Segments for Unsourced {
    fn segments_for(&self, _: NodeId, _: VolumeIndex, _: ByteRange, _: &mut Vec<Segment>) {}
}

#[test]
fn bytes_the_source_has_no_checksums_for_are_checked_as_they_are_in_hand() {
    let mut job = Job::new(PLENTY).with_set(None);
    let member = job.member("video.mkv", Some(8));
    for (offset, bytes, provenance) in [
        (0, &b"fresh"[..], Provenance::Fresh),
        (5, &b"bad"[..], Provenance::Damaged),
    ] {
        let arrival = Arrival {
            node: NodeId::ROOT,
            member,
            offset,
            bytes: Bytes::copy_from_slice(bytes),
            provenance,
        };
        let mut build = job.build();
        job.writer
            .deliver(&mut job.tree, &mut build, &Unsourced, arrival, &mut job.out)
            .expect("deliver");
    }
    assert_eq!(
        job.par.lock().unwrap().fed,
        vec![
            Segment {
                range: ByteRange::new(0, 5),
                crc32: crc32(b"fresh"),
                verified: true,
            },
            Segment {
                range: ByteRange::new(5, 8),
                crc32: crc32(b"bad"),
                verified: false,
            },
        ]
    );
}

// A binding is said once, by the object's node and kind, over the member's
// name, at the member's depth.
#[test]
fn a_store_bound_over_a_member_logs_object_bound() {
    let (lines, _guard) = Lines::capture();
    let mut job = Job::new(PLENTY);
    let (_, child) = bound_store(&mut job);
    assert_eq!(
        lines.of("object bound"),
        [format!(
            "object bound job_id=1 node={} kind=rar_store over=pack.bin depth=0",
            child.0
        )]
    );
}

// A member given back is said once, with the object that gave it back and
// why; one no object held names none.
#[test]
fn a_member_given_back_logs_object_refused() {
    let (lines, _guard) = Lines::capture();
    let mut job = Job::new(PLENTY);
    let (_, child) = bound_store(&mut job);
    let mut build = job.build();
    job.writer
        .said(
            &mut job.tree,
            &mut build,
            &Source,
            child,
            Emit::Refuse(Why::Compressed),
            &mut job.out,
        )
        .expect("refuse");
    assert_eq!(
        lines.of("object refused"),
        ["object refused job_id=1 node=0 kind=rar_store member=pack.bin why=compressed"]
    );

    let (lines, _guard) = Lines::capture();
    let mut job = Job::new(HoldBudgets {
        resident_limit: 4,
        scratch_total: 0,
        scratch_ceiling: 0,
        disk_reserve: 0,
    });
    let member = job.member("notes.bin", None);
    job.send(member, 0, b"Ra", Provenance::Fresh);
    job.send(member, 10, b"tail..", Provenance::Fresh);
    job.send(member, 2, b"r! mid.", Provenance::Fresh);
    assert_eq!(
        lines.of("object refused"),
        ["object refused job_id=1 node=0 kind= member=notes.bin why=holds_breached"]
    );
}

// A set binding a member by what its bytes told it is said once, as the
// name the set describes it by.
#[test]
fn a_set_binding_a_member_by_content_logs_recovery_bound() {
    let (lines, _guard) = Lines::capture();
    let mut job = Job::new(PLENTY);
    let set = job.member("set.par2", None);
    job.send(set, 0, PAR2_SIGNATURE, Provenance::Fresh);
    let (node, _) = job.tree.child_of(NodeId::ROOT, set).expect("the set");
    let member = job.member("3f9a.bin", Some(8));
    job.send(member, 0, b"data", Provenance::Fresh);
    job.send(member, 4, b"more", Provenance::Fresh);
    assert_eq!(
        lines.of("recovery bound"),
        [format!(
            "recovery bound job_id=1 node={} kind=par2 set=set.par2 member=3f9a.bin \
             as=show.mkv by=content",
            node.0
        )]
    );
}

// The deadline runs from the first member an object produced, never from
// when the job's budget was built: a job that sat queued past it extracts
// its first bytes, and the posts' own writes are never under it. Once the
// extraction itself overruns, its next write is refused.
#[tokio::test(start_paused = true)]
async fn the_deadline_runs_from_the_first_produced_member_not_from_admission() {
    use crate::pipeline::extraction::{ExtractionLimits, ExtractionRejectionReason};
    use crate::post_processing::model::PostProcessingSettings;

    const DEADLINE: u64 = 60;
    let limits = ExtractionLimits {
        max_job_bytes: u64::MAX,
        max_member_bytes: u64::MAX,
        max_entries: u64::MAX,
        max_ratio: u64::MAX,
        ratio_floor_bytes: u64::MAX,
        max_seconds: DEADLINE,
        min_free_bytes: 0,
        max_memory_bytes: u64::MAX,
    };
    let policy = PostProcessingSettings {
        unacceptable_extensions: Vec::new(),
        ..PostProcessingSettings::default()
    };
    let mut job = Job::new(PLENTY);
    job.inner = 10;
    job.writer = job.writer.capped(Caps::new(
        Arc::new(limits),
        Ok(Arc::new(policy)),
        Vec::new(),
    ));
    let past = std::time::Duration::from_secs(DEADLINE + 1);

    tokio::time::advance(past).await;
    let plain = job.member("notes.bin", Some(5));
    job.send(plain, 0, b"plain", Provenance::Fresh);
    assert_eq!(job.file("notes.bin"), b"plain");

    let store = job.member("show.rar", None);
    let mut front = SIGNATURE.to_vec();
    front.extend_from_slice(b"hello");
    job.send(store, 0, &front, Provenance::Fresh);
    assert!(job.tree.child_of(NodeId::ROOT, store).is_some());
    assert!(job.writer.refusal().is_none());
    assert_eq!(job.file("inner.bin")[..5], *b"hello");

    tokio::time::advance(past).await;
    let arrival = Arrival {
        node: NodeId::ROOT,
        member: store,
        offset: front.len() as u64,
        bytes: Bytes::from_static(b"world"),
        provenance: Provenance::Fresh,
    };
    let mut build = job.build();
    let refused = job
        .writer
        .deliver(&mut job.tree, &mut build, &Source, arrival, &mut job.out);
    assert!(refused.is_err());
    assert_eq!(
        job.writer.refusal().map(ExtractionFailure::reason),
        Some(ExtractionRejectionReason::Deadline)
    );
}
