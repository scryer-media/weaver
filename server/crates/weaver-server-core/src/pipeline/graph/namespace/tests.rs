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
mod recovery_contracts;
mod replay;
mod split;
mod store_proof;
mod streams;
mod verification;
mod windows;

thread_local! {
    // How many members a pass over the holds asked what is held of them.
    pub(super) static RELEASE_ASKED: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
    // How many held ranges a pass asked their object to place.
    pub(crate) static RELEASE_LOCATED: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
    static STORE_DELIVERIES: std::cell::RefCell<Option<Vec<(ByteRange, Provenance)>>> = const { std::cell::RefCell::new(None) };
    // Each delivery the pretend 7z store heard, with the CRC32 its bytes
    // came proven by, when every byte of them was.
    static LATE_HEARD: std::cell::RefCell<Vec<(ByteRange, Option<u64>)>> = const { std::cell::RefCell::new(Vec::new()) };
}

const SIGNATURE: &[u8] = b"Rar!\x1a\x07\x00";
const HEADER: u64 = SIGNATURE.len() as u64;

// A pretend store with one volume: a seven-byte header kept as envelope 0,
// then one member, "inner.bin", of the length it is built with, to the end.
// The word RETRACT anywhere in a delivery takes the member back.
struct Store {
    announced: bool,
    length: u64,
    checksum_on_repair: Option<u32>,
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
        STORE_DELIVERIES.with_borrow_mut(|seen| {
            if let Some(seen) = seen {
                seen.push((delivery.range(), delivery.provenance));
            }
        });
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
        if delivery.provenance == Provenance::Repaired
            && let Some(crc32) = self.checksum_on_repair.take()
        {
            out.push(Emit::Checksum {
                member: MemberId(0),
                crc32,
            });
        }
        let skip = HEADER
            .saturating_sub(delivery.offset)
            .min(delivery.bytes.len() as u64);
        if skip < delivery.bytes.len() as u64 {
            out.push(Emit::Bytes {
                member: MemberId(0),
                offset: delivery.offset + skip - HEADER,
                bytes: Bytes::copy_from_slice(&delivery.bytes[skip as usize..]).into(),
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

    fn extent(&self, member: MemberId, range: ByteRange, out: &mut Vec<Extent>) -> Located {
        if member != MemberId(0) {
            return Located::Never;
        }
        if let Some(range) = range.intersect(ByteRange::new(0, self.length)) {
            out.push(Extent {
                volume: VolumeIndex(0),
                range: range.shift(HEADER as i64),
            });
        }
        Located::Whole
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
    // Its placement numbers: this one while unmapped, the next once mapped.
    number: u64,
}

static LATE_NUMBERS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

impl Late {
    fn new() -> Self {
        let number = LATE_NUMBERS.fetch_add(2, std::sync::atomic::Ordering::Relaxed);
        Self {
            mapped: false,
            number,
        }
    }
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
        let crc32 = weaver_graph::Algorithm::Crc32;
        let proven = delivery
            .bytes
            .proven(crc32)
            .then(|| delivery.bytes.digest(crc32).value);
        LATE_HEARD.with_borrow_mut(|heard| heard.push((range, proven)));
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
                bytes: Bytes::copy_from_slice(&delivery.bytes[start..start + part.len() as usize])
                    .into(),
                provenance: delivery.provenance,
            });
        }
    }

    fn volume_closed(&mut self, _: VolumeIndex, _: &mut Vec<Emit>) {}
    fn volume_lost(&mut self, _: VolumeIndex, _: ByteRange, _: &mut Vec<Emit>) {}
    fn wants(&self, _: &mut Vec<Seek>) {}
    fn pulled(&mut self, _: Seek, _: Pull, _: &mut Vec<Emit>) {}

    fn placement(&self) -> Option<u64> {
        Some(self.number + u64::from(self.mapped))
    }

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
    checksum_on_repair: Option<u32>,
    heard: Arc<Mutex<Vec<u64>>>,
}

impl Builder for Build {
    fn build(&mut self, kind: Kind, _: ObjectId) -> Option<Box<dyn Object>> {
        match kind {
            Kind::RarStore => Some(Box::new(Store {
                announced: false,
                length: self.inner,
                checksum_on_repair: self.checksum_on_repair,
            })),
            Kind::SevenZipStore => Some(Box::new(Late::new())),
            Kind::Xz => Some(Box::new(streams::Decoded::new(Arc::clone(&self.heard)))),
            Kind::Par2 => Some(Box::new(Par {
                volume: None,
                others: Vec::new(),
                fixing: None,
                fix: self.fix,
                seen: Arc::clone(&self.par),
                last: None,
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
// asked to repair, and what happens between its two reads.
#[derive(Default)]
struct Seen {
    report_error: bool,
    proof: Option<ByteRange>,
    fed: Vec<Segment>,
    read: Option<Vec<u8>>,
    volume: Option<Vec<u8>>,
    between: Option<Box<dyn FnMut() + Send>>,
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
    // What its last repair came to, until a bind changes what it covers.
    last: Option<RepairOutcome>,
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
        let covered = (self.volume, self.others.clone());
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
        if covered != (self.volume, self.others.clone()) {
            self.last = None;
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

    fn repair(&mut self, access: &mut dyn MemberAccess, _: weaver_graph::Budget) -> RepairOutcome {
        let outcome = self.ruled(access);
        self.last = Some(outcome);
        outcome
    }

    fn outcome(&self) -> Option<RepairOutcome> {
        self.last
    }

    fn proves(&self, _: MemberId, length: u64) -> ByteRange {
        self.seen
            .lock()
            .unwrap()
            .proof
            .unwrap_or(ByteRange::new(0, length))
    }
}

impl Par {
    fn ruled(&mut self, access: &mut dyn MemberAccess) -> RepairOutcome {
        if self.seen.lock().unwrap().report_error {
            #[derive(Debug)]
            struct Injected(std::io::Error);
            impl std::fmt::Display for Injected {
                fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
                    formatter.write_str("injected reconstruction failure")
                }
            }
            impl std::error::Error for Injected {
                fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
                    Some(&self.0)
                }
            }
            let error = Injected(std::io::Error::other("injected storage cause"));
            access.repair_error("execute recovery", "set-a", &error);
        }
        let mut volume = Vec::new();
        let span = ByteRange::new(0, PAR2_SIGNATURE.len() as u64);
        if access.read_volume(VolumeIndex(0), span, &mut volume) {
            self.seen.lock().unwrap().volume = Some(volume);
        }
        if let Some(between) = self.seen.lock().unwrap().between.as_mut() {
            between();
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
    checksum_on_repair: Option<u32>,
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
            checksum_on_repair: None,
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
            bytes: Bytes::copy_from_slice(bytes).into(),
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
            checksum_on_repair: self.checksum_on_repair,
            heard: Arc::clone(&self.heard),
        }
    }

    // Where the member's bytes past the holds budgets lie.
    fn spill(&self, member: MemberId) -> std::path::PathBuf {
        self.dir
            .path()
            .join(super::spill_name(NodeId::ROOT, member))
    }

    fn file(&self, name: &str) -> Vec<u8> {
        std::fs::read(self.dir.path().join(name)).expect("leaf file")
    }

    // Commit checkpoints as the write lane would, until no move waits: each
    // records what is wanted, and its commit runs the moves it covers and
    // frees the names they left.
    fn commit(&mut self) {
        while self.writer.moves_waiting() {
            let stamp = self.writer.committed + 1;
            self.writer.recorded(stamp);
            self.writer
                .commit_moves(&mut self.tree, stamp)
                .expect("moves");
        }
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
            let returned = loan.run(weaver_graph::Budget::default(), &Default::default());
            ruled.extend(returned.outcome.map(|outcome| (returned.set, outcome)));
            self.writer
                .returned(
                    &mut self.tree,
                    build,
                    &Source,
                    &weaver_objects::Objects::default(),
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

// Every article routes through the writer, so the effects of each one land
// in the room the writer keeps: empty between deliveries, its buffer reused
// by the next, and the bytes still reach the file in order.
#[test]
fn routed_deliveries_reuse_the_writer_s_effect_room() {
    let mut job = Job::new(PLENTY);
    let member = job.member("notes.bin", Some(11));
    job.send(member, 0, b"Raws", Provenance::Fresh);
    assert!(!job.tree.is_undecided(NodeId::ROOT, member));
    job.send(member, 4, b", wor", Provenance::Fresh);
    assert!(job.writer.effects.is_empty());
    let room = (job.writer.effects.as_ptr(), job.writer.effects.capacity());
    assert!(room.1 > 0, "a routed delivery left its buffer in the room");

    job.send(member, 9, b"ld", Provenance::Fresh);
    assert!(job.writer.effects.is_empty());
    assert_eq!(
        (job.writer.effects.as_ptr(), job.writer.effects.capacity()),
        room,
        "the next delivery reuses the room rather than a list of its own"
    );
    job.close(member);
    assert_eq!(job.file("notes.bin"), b"Raws, world");
}

// Members whose holds run past every budget are given up by none of them:
// each stays undecided, its bytes held in its spill file, and nothing is
// refused or given back.
#[test]
fn members_past_the_holds_are_held_and_given_up_by_none() {
    let mut job = Job::new(HoldBudgets {
        resident_limit: 4,
        scratch_total: 0,
        scratch_ceiling: 0,
        disk_reserve: 0,
    });
    let first = job.member("pack.r00", None);
    let second = job.member("pack.r01", None);
    for member in [first, second] {
        job.send(member, 0, b"Ra", Provenance::Fresh);
        job.send(member, 10, b"tail..", Provenance::Fresh);
        assert_eq!(job.writer.refused(NodeId::ROOT, member), None);
        assert!(job.tree.is_undecided(NodeId::ROOT, member));
        assert!(job.spill(member).is_file());
    }
    assert!(job.writer.take_given_back().is_empty());
    assert!(
        !job.out
            .iter()
            .any(|effect| matches!(effect, Effect::Refused { .. }))
    );
}

// An undecided member whose bytes spill before its first article comes
// gets every byte back, at its offset, once it is decided: the file is
// byte-identical, the spill file is let go, and no budget counted it.
#[test]
fn an_undecided_member_spilled_before_its_first_article_is_byte_identical() {
    let mut job = Job::new(HoldBudgets {
        resident_limit: 0,
        scratch_total: 0,
        scratch_ceiling: 0,
        disk_reserve: 0,
    });
    let member = job.member("notes.bin", None);
    job.send(member, 10, b"tail..", Provenance::Fresh);
    assert!(job.spill(member).is_file());
    assert_eq!(job.accountant.usage().resident, 0);
    assert_eq!(job.accountant.usage().scratch, 0);
    job.send(member, 2, b"r! mid.", Provenance::Fresh);
    job.send(member, 0, b"Ra", Provenance::Fresh);
    assert_eq!(job.writer.refused(NodeId::ROOT, member), None);
    job.close(member);
    assert_eq!(job.file("notes.bin"), b"Rar! mid.\0tail..");
    assert_eq!(job.writer.refused(NodeId::ROOT, member), None);
    // Nothing is held, and the spill file goes as any discarded file does,
    // once a checkpoint stops naming it.
    let files = &job.writer.files[&NodeId::ROOT];
    assert_eq!(files.holds.total_held(), 0);
    assert!(!files.spills.contains_key(&member));
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
        .map(|(held, on_file)| HoldRecord {
            node: NodeId::ROOT,
            held,
            on_file,
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
        on_file: false,
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

// Each budget a hold can run past. Memory past its limit pages to scratch;
// scratch past any of its budgets spills to the member's file.
const PAST: [(&str, HoldBudgets); 4] = [
    (
        "resident_limit",
        HoldBudgets {
            resident_limit: 0,
            ..PLENTY
        },
    ),
    (
        "scratch_total",
        HoldBudgets {
            resident_limit: 0,
            scratch_total: 0,
            ..PLENTY
        },
    ),
    (
        "scratch_ceiling",
        HoldBudgets {
            resident_limit: 0,
            scratch_ceiling: 0,
            ..PLENTY
        },
    ),
    (
        "disk_reserve",
        HoldBudgets {
            resident_limit: 0,
            disk_reserve: u64::MAX,
            ..PLENTY
        },
    ),
];

// Nothing of the namespace is held, on scratch or on file, and no budget
// counts anything.
fn nothing_held(job: &Job, budget: &str) {
    let files = &job.writer.files[&NodeId::ROOT];
    assert_eq!(files.holds.total_held(), 0, "{budget}");
    assert!(files.spills.is_empty(), "{budget}");
    assert_eq!(job.accountant.usage().resident, 0, "{budget}");
    assert_eq!(job.accountant.usage().scratch, 0, "{budget}");
}

// A member not yet decided, past each budget: it stays undecided, and once
// its front decides it every byte reaches its file.
#[test]
fn an_undecided_member_past_each_budget_is_written_whole() {
    for (budget, budgets) in PAST {
        let mut job = Job::new(budgets);
        let member = job.member("notes.bin", None);
        job.send(member, 10, b"tail..", Provenance::Fresh);
        job.send(member, 2, b"r! mid.", Provenance::Fresh);
        assert!(job.tree.is_undecided(NodeId::ROOT, member), "{budget}");
        assert_eq!(
            job.spill(member).is_file(),
            budget != "resident_limit",
            "{budget}"
        );
        assert_eq!(job.accountant.usage().resident, 0, "{budget}");

        job.send(member, 0, b"Ra", Provenance::Fresh);
        job.close(member);
        assert_eq!(job.writer.refused(NodeId::ROOT, member), None, "{budget}");
        assert_eq!(job.file("notes.bin"), b"Rar! mid.\0tail..", "{budget}");
        nothing_held(&job, budget);
    }
}

// A member bound to a store that has not yet placed its bytes, past each
// budget: it stays bound, and once the store's map places them the member
// below gets every byte.
#[test]
fn a_bound_member_past_each_budget_reaches_the_member_below_whole() {
    for (budget, budgets) in PAST {
        let mut job = Job::new(budgets);
        let member = job.member("a.7z", Some(map_at().end));
        let mut front = seven_z().to_vec();
        front.extend_from_slice(b"hello");
        job.send(member, 0, &front, Provenance::Fresh);
        assert!(!job.tree.is_undecided(NodeId::ROOT, member), "{budget}");
        assert!(job.tree.child_of(NodeId::ROOT, member).is_some(), "{budget}");
        assert_eq!(
            job.writer.held_bytes(NodeId::ROOT),
            body().len(),
            "{budget}"
        );
        assert_eq!(
            job.spill(member).is_file(),
            budget != "resident_limit",
            "{budget}"
        );

        job.send(member, map_at().start, MAP, Provenance::Fresh);
        assert_eq!(job.writer.refused(NodeId::ROOT, member), None, "{budget}");
        assert_eq!(job.file("inner.bin"), b"hello", "{budget}");
        nothing_held(&job, budget);
    }
}

// Two jobs share the budgets. The first fills them, and the second's holds
// go to its member's file; neither job loses a byte, and what the first
// holds stays where it is.
#[test]
fn a_job_whose_holds_find_the_budgets_full_spills_and_the_first_keeps_its_own() {
    let budgets = HoldBudgets {
        resident_limit: 0,
        scratch_total: 6,
        scratch_ceiling: u64::MAX,
        disk_reserve: 0,
    };
    let mut first = Job::new(budgets);
    let mut second = Job::new(budgets);
    second.accountant = Arc::clone(&first.accountant);
    second.writer = NamespaceWriter::new(
        2,
        second.dir.path().to_path_buf(),
        Arc::clone(&first.accountant),
        CapacityReader::unknown(),
    );

    let early = first.member("notes.bin", None);
    first.send(early, 10, b"tail..", Provenance::Fresh);
    assert!(!first.spill(early).is_file());
    assert_eq!(first.accountant.usage().scratch, 6);

    let late = second.member("notes.bin", None);
    second.send(late, 10, b"other.", Provenance::Fresh);
    assert!(second.spill(late).is_file());
    assert_eq!(first.accountant.usage().scratch, 6);

    second.send(late, 0, b"Rar! two.\0", Provenance::Fresh);
    second.close(late);
    assert_eq!(second.file("notes.bin"), b"Rar! two.\0other.");
    assert_eq!(second.writer.refused(NodeId::ROOT, late), None);

    // The first job's hold never moved.
    assert!(!first.spill(early).is_file());
    assert_eq!(first.accountant.usage().scratch, 6);
    first.send(early, 0, b"Rar! one.\0", Provenance::Fresh);
    first.close(early);
    assert_eq!(first.file("notes.bin"), b"Rar! one.\0tail..");
    assert_eq!(first.writer.refused(NodeId::ROOT, early), None);
    nothing_held(&first, "first");
    nothing_held(&second, "second");
}

// Holds paged to scratch and holds on file at a stop come back in a process
// whose ceiling is now lower than what the scratch file holds: what scratch
// cannot take goes to the member's file, and the member is written whole
// with nothing fetched again.
#[test]
fn holds_restored_under_a_lower_ceiling_come_back_on_file() {
    let mut job = Job::new(HoldBudgets {
        resident_limit: 0,
        scratch_ceiling: 6,
        ..PLENTY
    });
    let member = job.member("notes.bin", None);
    job.send(member, 10, b"tail..", Provenance::Fresh);
    job.send(member, 2, b"r! mid.", Provenance::Fresh);
    assert_eq!(job.accountant.usage().scratch, 6);
    assert!(job.spill(member).is_file());

    job.writer.page_out_holds().expect("page out");
    let records: Vec<HoldRecord> = job
        .writer
        .hold_records(NodeId::ROOT)
        .into_iter()
        .map(|(held, on_file)| HoldRecord {
            node: NodeId::ROOT,
            held,
            on_file,
        })
        .collect();
    assert_eq!(
        records.iter().map(|record| record.on_file).collect::<Vec<_>>(),
        vec![false, true],
        "{records:?}"
    );

    // The process goes away; its scratch and spill files stay. The next
    // has no scratch room at all.
    let lower = Arc::new(HoldsAccountant::new(HoldBudgets {
        resident_limit: 0,
        scratch_ceiling: 0,
        ..PLENTY
    }));
    job.accountant = Arc::clone(&lower);
    drop(std::mem::replace(
        &mut job.writer,
        NamespaceWriter::new(
            1,
            job.dir.path().to_path_buf(),
            lower,
            CapacityReader::unknown(),
        ),
    ));
    job.writer
        .restore_holds(&job.tree, &records)
        .expect("restore holds");
    assert_eq!(job.accountant.usage().scratch, 0);
    assert_eq!(job.writer.held_bytes(NodeId::ROOT), 13);
    assert!(job.writer.files[&NodeId::ROOT].spills.contains_key(&member));
    for (range, held) in [(ByteRange::new(10, 16), &b"tail.."[..]), (ByteRange::new(2, 9), b"r! mid.")] {
        let mut bytes = Vec::new();
        assert!(job.writer.access(&job.tree).read_at(NodeId::ROOT, member, range, &mut bytes));
        assert_eq!(bytes, held);
    }

    job.send(member, 0, b"Ra", Provenance::Fresh);
    job.close(member);
    assert_eq!(job.file("notes.bin"), b"Rar! mid.\0tail..");
    nothing_held(&job, "restored");
}

// A member an older build refused because its holds ran past their budgets
// is taken back: the refusal was never a ruling on its bytes, so the store
// that owned it is asked again and the member below comes out whole.
#[test]
fn a_member_refused_for_its_holds_is_reopened_and_delivered() {
    let mut job = Job::new(PLENTY);
    let (member, child) = bound_store(&mut job);
    store_says(&mut job, child, vec![Emit::Refuse(Why::HoldsBreached)]);
    assert_eq!(
        job.writer.refused(NodeId::ROOT, member),
        Some(Why::HoldsBreached)
    );

    let mut build = job.build();
    job.writer
        .lift_hold_refusals(&mut job.tree, &mut build, &Source, &mut job.out)
        .expect("lift");
    assert_eq!(job.writer.refused(NodeId::ROOT, member), None);
    assert!(job.tree.child_of(NodeId::ROOT, member).is_some());
    assert_eq!(job.file("inner.bin"), b"hello");
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
        3,
        "unwritten declared bytes must not allocate space on a non-sparse mount"
    );
    job.send(member, 6, b"end", Provenance::Fresh);
    assert!(job.is_open(NodeId::ROOT, member));
    job.send(member, 3, b"mid", Provenance::Fresh);
    assert!(!job.is_open(NodeId::ROOT, member));
    assert_eq!(job.file("video.mkv"), b"abcmidend");
}

// A member whose object states a CRC32 for its bytes is checked when it
// closes: bytes that each arrived good but together fail the stated
// checksum are damage on the whole member, and bytes that match it stay
// clean.
#[test]
fn a_member_failing_its_stated_checksum_is_damaged_when_it_closes() {
    for (sent, damaged) in [(&b"abcmidend"[..], false), (&b"abcmixend"[..], true)] {
        let mut job = Job::new(PLENTY);
        let member = job.member("video.mkv", Some(9));
        job.tree
            .namespace_mut(NodeId::ROOT)
            .and_then(|namespace| namespace.get_mut(member))
            .expect("the announced member")
            .crc32 = Some(crc32(b"abcmidend"));
        job.send(member, 0, sent, Provenance::Fresh);
        job.close(member);
        assert!(!job.is_open(NodeId::ROOT, member));
        let expected = if damaged {
            vec![(member, ByteRange::new(0, 9))]
        } else {
            Vec::new()
        };
        assert_eq!(
            job.root().damaged_ranges().collect::<Vec<_>>(),
            expected,
            "sent {:?}",
            String::from_utf8_lossy(sent)
        );
    }
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
        None,
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
        None,
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

// Held bytes go down again with the checksum the source took of them when
// they came, whether they stayed in memory or were paged to scratch and
// read back: the object below is handed them proven, as a first delivery
// is, and hashes nothing of them again.
#[test]
fn held_bytes_go_down_again_with_the_source_s_checksums() {
    for resident_limit in [u64::MAX, 2] {
        let mut job = Job::new(HoldBudgets {
            resident_limit,
            ..PLENTY
        });
        let member = job.member("a.7z", Some(map_at().end));
        let mut front = seven_z().to_vec();
        front.extend_from_slice(b"hello");
        job.send(member, 0, &front, Provenance::Fresh);
        assert_eq!(job.writer.held_bytes(NodeId::ROOT), body().len());
        let paged = job.accountant.usage().scratch;
        assert_eq!(paged > 0, resident_limit == 2, "limit {resident_limit}");
        LATE_HEARD.with_borrow_mut(Vec::clear);

        job.send(member, map_at().start, MAP, Provenance::Fresh);

        assert_eq!(job.writer.held_bytes(NodeId::ROOT), 0);
        assert_eq!(job.file("inner.bin"), b"hello");
        let replayed: Vec<_> = LATE_HEARD
            .with_borrow(Clone::clone)
            .into_iter()
            .filter(|(range, _)| *range == body())
            .collect();
        assert_eq!(
            replayed,
            vec![(body(), Some(u64::from(SOURCE_CRC)))],
            "limit {resident_limit}: the held body comes back proven by the source"
        );
    }
}

// A pass over the holds asks only the volumes that hold bytes: of six
// hundred members and two stores waiting on their maps, a delivery to a
// plain member asks the two stores, and each map then releases its own.
#[test]
fn a_pass_over_the_holds_asks_only_the_volumes_that_hold_bytes() {
    let mut job = Job::new(PLENTY);
    let plain: Vec<MemberId> = (0..600)
        .map(|index| job.member(&format!("f{index}.bin"), Some(4)))
        .collect();
    let stores = [
        job.member("a.7z", Some(map_at().end)),
        job.member("b.7z", Some(map_at().end)),
    ];
    let mut front = seven_z().to_vec();
    front.extend_from_slice(b"hello");
    for store in stores {
        job.send(store, 0, &front, Provenance::Fresh);
    }
    assert_eq!(job.writer.held_bytes(NodeId::ROOT), 2 * body().len());
    RELEASE_ASKED.set(0);

    job.send(plain[0], 0, b"data", Provenance::Fresh);

    assert_eq!(RELEASE_ASKED.get(), stores.len());
    assert_eq!(job.writer.held_bytes(NodeId::ROOT), 2 * body().len());
    job.send(stores[0], map_at().start, MAP, Provenance::Fresh);
    assert_eq!(job.writer.held_bytes(NodeId::ROOT), body().len());
    job.send(stores[1], map_at().start, MAP, Provenance::Fresh);
    assert_eq!(job.writer.held_bytes(NodeId::ROOT), 0);
}

// A pass asks a store where its held bytes go only once the store's
// placement number moved: deliveries that move nothing locate nothing, and
// the map that moves it releases the body, written as it came.
#[test]
fn a_pass_locates_held_bytes_only_once_their_object_s_map_moved() {
    let mut job = Job::new(PLENTY);
    let plain: Vec<MemberId> = (0..20)
        .map(|index| job.member(&format!("f{index}.bin"), Some(4)))
        .collect();
    let store = job.member("a.7z", Some(map_at().end));
    let mut front = seven_z().to_vec();
    front.extend_from_slice(b"hello");
    job.send(store, 0, &front, Provenance::Fresh);
    assert_eq!(job.writer.held_bytes(NodeId::ROOT), body().len());
    job.send(plain[0], 0, b"data", Provenance::Fresh);
    RELEASE_ASKED.set(0);
    RELEASE_LOCATED.set(0);

    for member in &plain[1..] {
        job.send(*member, 0, b"data", Provenance::Fresh);
    }

    assert_eq!(
        RELEASE_ASKED.get(),
        plain.len() - 1,
        "each pass asks the store"
    );
    assert_eq!(RELEASE_LOCATED.get(), 0, "and locates nothing");
    assert_eq!(job.writer.held_bytes(NodeId::ROOT), body().len());
    job.send(store, map_at().start, MAP, Provenance::Fresh);
    assert_eq!(
        RELEASE_LOCATED.get(),
        1,
        "the map moved it: the body is located"
    );
    assert_eq!(job.writer.held_bytes(NodeId::ROOT), 0);
    assert_eq!(job.file("inner.bin"), b"hello");
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
    // The file stays where it sat until a checkpoint records the name.
    assert_eq!(job.file("3f9a.bin"), b"data");
    job.commit();
    assert_eq!(job.file("show.part01.rar"), b"data");
    assert!(!job.dir.path().join("3f9a.bin").exists());
    let next = job.member("3f9a.bin", None);
    assert_eq!(job.root().get(next).unwrap().output_name, "3f9a.bin");
}

// A member renamed and then discarded before its file moved leaves its old
// name once the checkpoint that says the file is gone has committed: the
// member kept off the name meanwhile takes it. The file itself stays where
// the database names it until then, so a restore from the checkpoint before
// finds it.
#[test]
fn a_name_a_discarded_file_sat_at_is_free_once_its_checkpoint_commits() {
    let mut job = Job::new(PLENTY);
    let member = job.member("3f9a.bin", Some(4));
    job.send(member, 0, b"data", Provenance::Fresh);
    let mut build = job.build();
    job.writer
        .rename(&mut job.tree, &mut build, NodeId::ROOT, member, "show.mkv")
        .expect("rename");
    let leaf = job
        .writer
        .files_mut(NodeId::ROOT)
        .leaves
        .remove(&member)
        .expect("leaf");
    job.writer.discard(leaf).expect("discard");
    assert_eq!(job.file("3f9a.bin"), b"data");
    let next = job.member("3f9a.bin", None);
    assert_eq!(job.root().get(next).unwrap().output_name, "3f9a (1).bin");
    job.commit();
    assert!(!job.dir.path().join("3f9a.bin").exists());
    assert_eq!(job.root().get(next).unwrap().output_name, "3f9a.bin");
    assert!(!job.writer.moves_waiting());
}

// A file none of whose bytes were written was never made, so discarding it
// leaves nothing to go: a file made at its path before the checkpoint
// commits, such as the envelope of the volume's next holder, is a newer one
// and stays once it does.
#[test]
fn a_discarded_leaf_with_no_file_takes_no_newer_file_at_its_path() {
    let mut job = Job::new(PLENTY);
    let name = crate::pipeline::graph::envelope::envelope_name(NodeId(2), 1);
    let leaf = Leaf::at(job.dir.path(), &name);
    job.writer.discard(leaf).expect("discard");
    let path = job.dir.path().join(&name);
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(&path, b"next").unwrap();
    job.commit();
    assert_eq!(std::fs::read(&path).unwrap(), b"next");
}

// The other order: the newer file is at the path before the leaf that never
// made one is discarded. The path holding a file says nothing of the leaf.
#[test]
fn a_discarded_leaf_with_no_file_leaves_a_newer_file_already_at_its_path() {
    let mut job = Job::new(PLENTY);
    let name = crate::pipeline::graph::envelope::envelope_name(NodeId(2), 1);
    let leaf = Leaf::at(job.dir.path(), &name);
    let path = job.dir.path().join(&name);
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(&path, b"next").unwrap();
    job.writer.discard(leaf).expect("discard");
    job.commit();
    assert_eq!(std::fs::read(&path).unwrap(), b"next");
}

#[test]
fn a_new_repair_leaf_cannot_outlive_its_uncommitted_name_during_a_move() {
    let mut job = Job::new(PLENTY);
    let donor = job.member("b.bin", Some(5));
    job.send(donor, 0, b"alpha", Provenance::Fresh);
    let mut build = job.build();
    job.writer
        .rename(&mut job.tree, &mut build, NodeId::ROOT, donor, "a.bin")
        .unwrap();
    let repair = job.member("b.bin", Some(5));
    assert_eq!(job.root().get(repair).unwrap().output_name, "b (1).bin");
    let recorded_repair = job.root().get(repair).unwrap().clone();
    let donor_file = job.writer.file_state(NodeId::ROOT, donor);
    let repair_file = job.writer.file_state(NodeId::ROOT, repair);
    assert!(donor_file.has_file);
    assert_eq!(donor_file.on_disk.as_deref(), Some("b.bin"));
    assert!(!repair_file.has_file);
    job.writer.recorded(1);

    // Repair returns after this checkpoint was captured. Its first file
    // must stay unpublished until a checkpoint also records its ownership.
    job.send(repair, 0, b"bravo", Provenance::Repaired);
    assert!(job.writer.commit_moves(&mut job.tree, 1).unwrap());
    assert_eq!(job.file("a.bin"), b"alpha");
    assert!(
        !job.dir.path().join("b (1).bin").exists(),
        "a crash must not strand a repair at an uncommitted temporary name"
    );

    // Restore exactly the captured member and file facts. The newer repair
    // bytes are not adopted; the recorded donor is found after its move.
    *job.tree
        .namespace_mut(NodeId::ROOT)
        .unwrap()
        .get_mut(repair)
        .unwrap() = recorded_repair;
    job.tree.rebuild_names();
    job.writer = NamespaceWriter::new(
        1,
        job.dir.path().to_path_buf(),
        Arc::clone(&job.accountant),
        CapacityReader::unknown(),
    );
    job.writer
        .restore_file(&job.tree, NodeId::ROOT, donor, &donor_file, &Unsourced)
        .unwrap();
    job.writer
        .restore_file(&job.tree, NodeId::ROOT, repair, &repair_file, &Unsourced)
        .unwrap();
    for (node, member, name) in job.writer.waiting() {
        job.tree.leaving(node, member, &name);
    }
    let mut settled = Vec::new();
    job.tree.settle_names(&mut settled);
    job.writer.moved(&mut job.tree, &settled).unwrap();
    let mut uncommitted = Vec::new();
    assert!(
        !job.writer
            .read_posted(&job.tree, repair, ByteRange::new(0, 5), &mut uncommitted)
    );
    job.send(repair, 0, b"bravo", Provenance::Repaired);
    job.commit();
    assert_eq!(job.file("a.bin"), b"alpha");
    assert_eq!(job.file("b.bin"), b"bravo");
    assert!(!job.dir.path().join("b (1).bin").exists());
}

#[test]
fn a_new_repair_leaf_waits_while_only_a_vacated_name_is_uncommitted() {
    let mut job = Job::new(PLENTY);
    let donor = job.member("b.bin", Some(5));
    job.send(donor, 0, b"alpha", Provenance::Fresh);
    let mut build = job.build();
    job.writer
        .rename(&mut job.tree, &mut build, NodeId::ROOT, donor, "a.bin")
        .unwrap();
    job.writer.recorded(1);
    assert!(job.writer.commit_moves(&mut job.tree, 1).unwrap());
    assert!(job.writer.moves.is_empty());
    assert!(!job.writer.left.is_empty());

    let repair = job.member("b.bin", Some(5));
    assert_eq!(job.root().get(repair).unwrap().output_name, "b (1).bin");
    job.send(repair, 0, b"bravo", Provenance::Repaired);
    assert!(!job.dir.path().join("b (1).bin").exists());
    let pending = job.writer.file_state(NodeId::ROOT, repair);
    assert!(pending.has_file);
    let pending_name = format!(
        "{}/graph-waiting-0-{}",
        weaver_graph::names::INTERNAL_DIR,
        repair.0
    );
    assert_eq!(pending.on_disk.as_deref(), Some(pending_name.as_str()));
    job.commit();
    assert_eq!(job.file("a.bin"), b"alpha");
    assert_eq!(job.file("b.bin"), b"bravo");
    assert!(!job.dir.path().join("b (1).bin").exists());
}

#[test]
fn a_committed_leaf_move_rejects_an_unowned_destination() {
    let mut job = Job::new(PLENTY);
    let member = job.member("source.bin", Some(5));
    job.send(member, 0, b"owned", Provenance::Fresh);
    let mut build = job.build();
    job.writer
        .rename(
            &mut job.tree,
            &mut build,
            NodeId::ROOT,
            member,
            "target.bin",
        )
        .expect("rename intent");
    job.writer.recorded(2);
    std::fs::write(job.dir.path().join("target.bin"), b"unrelated").unwrap();
    assert!(!job.writer.commit_moves(&mut job.tree, 1).unwrap());
    let error = job.writer.commit_moves(&mut job.tree, 2).unwrap_err();
    assert!(
        matches!(error, WriteError::Leaf(LeafError::Io { source, .. }) if source.kind() == std::io::ErrorKind::AlreadyExists)
    );
    assert_eq!(job.file("source.bin"), b"owned");
    assert_eq!(job.file("target.bin"), b"unrelated");
    assert_eq!(
        job.writer.waiting(),
        vec![(NodeId::ROOT, member, "source.bin".into())]
    );
    assert_eq!(job.root().get(member).unwrap().output_name, "target.bin");
    // Removing the obstruction is an explicit storage repair. The failed
    // move kept the same recorded source and intent for a later retry.
    std::fs::remove_file(job.dir.path().join("target.bin")).unwrap();
    assert!(job.writer.commit_moves(&mut job.tree, 2).unwrap());
    assert_eq!(job.file("target.bin"), b"owned");
}

#[test]
fn an_exclusive_leaf_move_preserves_case_only_renaming() {
    let directory = tempfile::tempdir().unwrap();
    std::fs::write(directory.path().join("BEACON.BIN"), b"owned").unwrap();
    let mut leaf = Leaf::found(directory.path(), "BEACON.BIN").unwrap();
    // The name ledger intentionally keeps an already-equivalent spelling.
    // Exercise the filesystem operation with an actual leaf move intent.
    leaf.intend("beacon.bin");
    assert_eq!(leaf.move_on().unwrap(), Some("BEACON.BIN".into()));
    assert_eq!(
        std::fs::read(directory.path().join("beacon.bin")).unwrap(),
        b"owned"
    );
    let names: Vec<_> = std::fs::read_dir(directory.path())
        .unwrap()
        .map(|entry| entry.unwrap().file_name())
        .collect();
    assert!(names.iter().any(|name| name == "beacon.bin"));
    assert!(!names.iter().any(|name| name == "BEACON.BIN"));
}

// One file wants the name another still sits at, and the checkpoint that
// says the other leaves it comes later: the earlier commit moves nothing
// onto the name, the later one moves the other file off it, and the first
// moves on only once the checkpoint that records that move has committed.
// No file lands on bytes still written under the name, whatever the file
// system folds.
#[test]
fn a_file_waits_off_a_name_another_file_still_sits_at() {
    let mut job = Job::new(PLENTY);
    let (first, second) = (MemberId(1), MemberId(2));
    for (member, name, bytes) in [(first, "x.bin", b"alpha"), (second, "y.bin", b"bravo")] {
        std::fs::write(job.dir.path().join(name), bytes).expect("file");
        let leaf = Leaf::found(job.dir.path(), name).unwrap();
        job.writer
            .files_mut(NodeId::ROOT)
            .leaves
            .insert(member, leaf);
    }
    job.writer.intend(NodeId::ROOT, first, "y.bin");
    job.writer.recorded(1);
    job.writer.intend(NodeId::ROOT, second, "z.bin");
    job.writer.recorded(2);

    let moved = job.writer.commit_moves(&mut job.tree, 1).expect("commit");
    assert!(!moved);
    assert_eq!(job.file("x.bin"), b"alpha");
    assert_eq!(job.file("y.bin"), b"bravo");

    let moved = job.writer.commit_moves(&mut job.tree, 2).expect("commit");
    assert!(moved);
    assert_eq!(job.file("z.bin"), b"bravo");
    assert_eq!(job.file("x.bin"), b"alpha");
    assert!(!job.dir.path().join("y.bin").exists());

    job.commit();
    assert_eq!(job.file("y.bin"), b"alpha");
    assert_eq!(job.file("z.bin"), b"bravo");
    assert!(!job.dir.path().join("x.bin").exists());
}

#[test]
fn delivery_releases_partial_and_retired_files_without_changing_their_bytes() {
    let mut job = Job::new(PLENTY);
    let member = job.member("partial.bin", Some(10));
    job.send(member, 0, b"front", Provenance::Fresh);
    let mut retired = Leaf::at(job.dir.path(), "retired.bin");
    retired.write(0, b"old", None).unwrap();
    job.writer.discard(retired).unwrap();
    let mut scratch = Leaf::at(job.dir.path(), ".weaver-chunks/scratch");
    scratch.write(0, b"held", None).unwrap();
    job.writer.files_mut(NodeId::ROOT).scratch = Some(scratch);
    assert_eq!(job.writer.open_files().len(), 3);
    let partial = job.file("partial.bin");
    job.writer.release_handles();
    assert!(job.writer.open_files().is_empty());
    assert_eq!(job.file("partial.bin"), partial);
    assert_eq!(job.file("retired.bin"), b"old");
    assert_eq!(job.file(".weaver-chunks/scratch"), b"held");
    assert_eq!(job.root().get(member).unwrap().coverage.bytes_covered(), 5);
    // Releasing the handle does not bypass the checkpoint that owns removal.
    job.commit();
    assert!(!job.dir.path().join("retired.bin").exists());
    assert_eq!(job.file("partial.bin"), partial);
}

// Whether the member's file waits under the internal directory.
fn waits(job: &Job, member: MemberId) -> bool {
    job.writer.files[&NodeId::ROOT].leaves[&member]
        .name()
        .starts_with(weaver_graph::names::INTERNAL_DIR)
}

// A move waiting for one member holds the names it involves, never the
// rest: a member whose name nothing holds opens its file there at once.
#[test]
fn a_free_name_opens_at_once_while_another_members_move_waits() {
    let mut job = Job::new(PLENTY);
    let first = job.member("3f9a.bin", Some(5));
    job.send(first, 0, b"alpha", Provenance::Fresh);
    let mut build = job.build();
    job.writer
        .rename(&mut job.tree, &mut build, NodeId::ROOT, first, "show.mkv")
        .expect("rename");
    assert!(job.writer.moves_waiting());
    let second = job.member("extra.bin", Some(5));
    job.send(second, 0, b"bravo", Provenance::Fresh);
    assert!(!waits(&job, second));
    assert_eq!(job.file("extra.bin"), b"bravo");
    assert_eq!(job.writer.file_state(NodeId::ROOT, second).on_disk, None);
    job.commit();
    assert_eq!(job.file("show.mkv"), b"alpha");
    assert_eq!(job.file("extra.bin"), b"bravo");
    assert!(!job.dir.path().join("3f9a.bin").exists());
}

// The name a waiting move wants is that member's: a file for another member
// at that name waits off it, and once the move has run both files stand
// where they belong, neither written over.
#[test]
fn a_name_a_waiting_move_wants_is_held_for_it() {
    let mut job = Job::new(PLENTY);
    let (first, second) = (MemberId(1), MemberId(2));
    std::fs::write(job.dir.path().join("x.bin"), b"alpha").expect("file");
    job.writer
        .files_mut(NodeId::ROOT)
        .leaves
        .insert(first, Leaf::at(job.dir.path(), "x.bin"));
    job.writer.intend(NodeId::ROOT, first, "y.bin");
    let leaf = job
        .writer
        .new_leaf((NodeId::ROOT, second), "y.bin", false, job.dir.path());
    job.writer
        .files_mut(NodeId::ROOT)
        .leaves
        .insert(second, leaf);
    assert!(waits(&job, second));
    assert_eq!(
        job.writer.files[&NodeId::ROOT].leaves[&second].wants(),
        Some("y.bin")
    );
}

// A name a file left stays shut until the checkpoint recording it left has
// committed, and so does every name that runs through it as a directory.
#[test]
fn a_left_name_and_the_names_under_it_stay_shut_until_recorded() {
    let mut job = Job::new(PLENTY);
    let first = MemberId(1);
    std::fs::write(job.dir.path().join("box"), b"alpha").expect("file");
    job.writer
        .files_mut(NodeId::ROOT)
        .leaves
        .insert(first, Leaf::found(job.dir.path(), "box").unwrap());
    job.writer.intend(NodeId::ROOT, first, "crate.bin");
    job.writer.recorded(1);
    assert!(job.writer.commit_moves(&mut job.tree, 1).expect("commit"));
    assert_eq!(job.file("crate.bin"), b"alpha");
    assert!(job.writer.moves.is_empty());
    for (member, name) in [(MemberId(2), "box"), (MemberId(3), "box/inner.bin")] {
        let leaf = job
            .writer
            .new_leaf((NodeId::ROOT, member), name, false, job.dir.path());
        assert!(
            leaf.name().starts_with(weaver_graph::names::INTERNAL_DIR),
            "{name}"
        );
    }
    job.writer.recorded(2);
    assert!(job.writer.commit_moves(&mut job.tree, 2).expect("commit"));
    assert!(!job.writer.moves_waiting());
    for (member, name) in [(MemberId(4), "box"), (MemberId(5), "box/inner.bin")] {
        let leaf = job
            .writer
            .new_leaf((NodeId::ROOT, member), name, false, job.dir.path());
        assert_eq!(leaf.name(), name);
    }
}

// A member renamed twice before its file moved: the database still gives
// it the first new name, as the one its file waits for, until a checkpoint
// saying otherwise commits. A file for another member given that name
// meanwhile waits off it, so a restore from the earlier checkpoint finds
// nothing in the way of the move it holds to.
#[test]
fn a_name_the_database_still_gives_a_renamed_member_stays_shut() {
    let mut job = Job::new(PLENTY);
    let first = job.member("3f9a.bin", Some(5));
    job.send(first, 0, b"alpha", Provenance::Fresh);
    let mut build = job.build();
    job.writer
        .rename(&mut job.tree, &mut build, NodeId::ROOT, first, "x.bin")
        .expect("rename");
    job.writer.recorded(1);
    job.writer
        .rename(&mut job.tree, &mut build, NodeId::ROOT, first, "y.bin")
        .expect("rename");
    let second = job.member("x.bin", Some(5));
    assert_eq!(job.root().get(second).unwrap().output_name, "x.bin");
    job.send(second, 0, b"bravo", Provenance::Fresh);
    assert!(!job.dir.path().join("x.bin").exists());
    assert!(waits(&job, second));
    job.commit();
    assert_eq!(job.file("y.bin"), b"alpha");
    assert_eq!(job.file("x.bin"), b"bravo");
    assert!(!job.dir.path().join("3f9a.bin").exists());
}

// A file opened at its name while another member's move waits, then the
// job stops before the checkpoint covering that move commits. Restored from
// the checkpoint, each member's bytes are where the database gives them:
// the new file at its name, the waiting one where it sat, and the move then
// runs over nothing.
#[test]
fn a_restore_finds_a_file_opened_while_a_move_waited_at_its_name() {
    let mut job = Job::new(PLENTY);
    let first = job.member("3f9a.bin", Some(5));
    job.send(first, 0, b"alpha", Provenance::Fresh);
    let mut build = job.build();
    job.writer
        .rename(&mut job.tree, &mut build, NodeId::ROOT, first, "show.mkv")
        .expect("rename");
    let second = job.member("extra.bin", Some(5));
    job.send(second, 0, b"bravo", Provenance::Fresh);
    let first_file = job.writer.file_state(NodeId::ROOT, first);
    let second_file = job.writer.file_state(NodeId::ROOT, second);
    assert_eq!(first_file.on_disk.as_deref(), Some("3f9a.bin"));
    assert_eq!(second_file.on_disk, None);
    job.writer.recorded(1);

    job.writer = NamespaceWriter::new(
        1,
        job.dir.path().to_path_buf(),
        Arc::clone(&job.accountant),
        CapacityReader::unknown(),
    );
    job.writer
        .restore_file(&job.tree, NodeId::ROOT, first, &first_file, &Unsourced)
        .unwrap();
    job.writer
        .restore_file(&job.tree, NodeId::ROOT, second, &second_file, &Unsourced)
        .unwrap();
    for (member, name, bytes) in [
        (first, "3f9a.bin", b"alpha"),
        (second, "extra.bin", b"bravo"),
    ] {
        assert_eq!(job.writer.files[&NodeId::ROOT].leaves[&member].name(), name);
        let mut read = Vec::new();
        assert!(
            job.writer
                .read_posted(&job.tree, member, ByteRange::new(0, 5), &mut read)
        );
        assert_eq!(read, bytes);
    }
    assert_eq!(
        job.writer.files[&NodeId::ROOT].leaves[&first].wants(),
        Some("show.mkv")
    );
    job.commit();
    assert_eq!(job.file("show.mkv"), b"alpha");
    assert_eq!(job.file("extra.bin"), b"bravo");
    assert!(!job.dir.path().join("3f9a.bin").exists());
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
    bound_store_at(job, "pack.bin")
}

fn bound_store_at(job: &mut Job, name: &str) -> (MemberId, NodeId) {
    let member = job.member(name, None);
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
fn an_envelope_on_disk_reads_through_the_root_only_after_its_ownership_is_restored() {
    let mut job = Job::new(PLENTY);
    let (member, child) = bound_store(&mut job);
    let kept = job.writer.envelopes.records(child);
    assert_eq!(kept.len(), 1);
    let file = job.writer.file_state(child, MemberId(0));
    job.writer = NamespaceWriter::new(
        1,
        job.dir.path().to_path_buf(),
        Arc::clone(&job.accountant),
        CapacityReader::unknown(),
    );
    job.writer
        .restore_file(&job.tree, child, MemberId(0), &file, &Unsourced)
        .unwrap();

    let mut posted = Vec::new();
    assert!(
        !job.writer
            .read_posted(&job.tree, member, ByteRange::new(0, 7), &mut posted),
        "an existing envelope without committed ownership is not adopted"
    );
    job.writer.restore_kept(&kept[0]).unwrap();
    assert!(
        job.writer
            .read_posted(&job.tree, member, ByteRange::new(0, 12), &mut posted)
    );
    assert_eq!(posted, [SIGNATURE, b"hello"].concat());
}

#[test]
fn a_deleted_output_does_not_hide_the_surviving_committed_envelope() {
    let mut job = Job::new(PLENTY);
    let (member, child) = bound_store(&mut job);
    let kept = job.writer.envelopes.records(child);
    let file = job.writer.file_state(child, MemberId(0));
    job.writer = NamespaceWriter::new(
        1,
        job.dir.path().to_path_buf(),
        Arc::clone(&job.accountant),
        CapacityReader::unknown(),
    );
    std::fs::remove_file(job.dir.path().join("inner.bin")).unwrap();
    job.writer.restore_kept(&kept[0]).unwrap();
    job.writer
        .restore_file(&job.tree, child, MemberId(0), &file, &Unsourced)
        .unwrap();

    let mut posted = Vec::new();
    assert!(
        job.writer
            .read_posted(&job.tree, member, ByteRange::new(0, 7), &mut posted)
    );
    assert_eq!(posted, SIGNATURE);
    let mut missing = Vec::new();
    assert!(
        !job.writer
            .read_posted(&job.tree, member, ByteRange::new(7, 12), &mut missing)
    );
}

// A return can happen while the complete source still awaits handover.
// Its recorded coverage, not its allocated length, describes retained bytes.
#[test]
fn give_back_retains_the_unreplayed_leaf_without_adopting_sparse_holes() {
    for (sparse, restored) in [(false, false), (true, false), (false, true), (true, true)] {
        let mut job = Job::new(PLENTY);
        let member = job.member("pack.bin", Some(12));
        job.send(member, 0, SIGNATURE, Provenance::Fresh);
        let owner = job.tree.child_of(NodeId::ROOT, member).unwrap();
        let source = [SIGNATURE, b"hello"].concat();
        let (offset, bytes) = if sparse {
            (10, &source[10..])
        } else {
            (0, &source[..])
        };
        job.writer
            .write_file(
                &job.tree,
                NodeId::ROOT,
                member,
                offset,
                &bytes.to_vec().into(),
            )
            .unwrap();
        let original = job.writer.files[&NodeId::ROOT].leaves[&member]
            .path()
            .to_path_buf();
        let record = job
            .tree
            .namespace_mut(NodeId::ROOT)
            .unwrap()
            .get_mut(member)
            .unwrap();
        record
            .coverage
            .write(ByteRange::new(offset, 12), Provenance::Fresh);
        record.closed = !sparse;
        if restored {
            let state = job.writer.file_state(NodeId::ROOT, member);
            job.writer
                .restore_file(&job.tree, NodeId::ROOT, member, &state, &Unsourced)
                .unwrap();
            assert_eq!(
                job.writer.files[&NodeId::ROOT].leaves[&member]
                    .folded(12, weaver_graph::Algorithm::Crc32),
                None
            );
        }

        job.writer
            .given_back(&mut job.tree, (NodeId::ROOT, member), owner)
            .unwrap();

        let record = job
            .tree
            .namespace(NodeId::ROOT)
            .unwrap()
            .get(member)
            .unwrap();
        assert!(record.coverage.covered(ByteRange::new(0, HEADER)));
        assert!(record.coverage.covered(ByteRange::new(10, 12)));
        assert_eq!(record.coverage.covered(ByteRange::new(HEADER, 10)), !sparse);
        assert_eq!(record.closed, !sparse);
        let leaf = &job.writer.files[&NodeId::ROOT].leaves[&member];
        assert_eq!(leaf.path(), original, "handover keeps the recorded source");
        let mut actual = Vec::new();
        leaf.read(0, 12, &mut actual).unwrap();
        assert_eq!(&actual[..HEADER as usize], SIGNATURE);
        assert_eq!(&actual[10..], &source[10..]);
        if !sparse {
            assert_eq!(actual, source);
        }
    }
}

#[test]
fn give_back_does_not_retain_a_truncated_leaf_tail() {
    let mut job = Job::new(PLENTY);
    let member = job.member("pack.bin", Some(12));
    job.send(member, 0, SIGNATURE, Provenance::Fresh);
    let owner = job.tree.child_of(NodeId::ROOT, member).unwrap();
    job.writer
        .write_file(
            &job.tree,
            NodeId::ROOT,
            member,
            0,
            &[SIGNATURE, b"hello"].concat().into(),
        )
        .unwrap();
    let leaf = job
        .writer
        .files_mut(NodeId::ROOT)
        .leaves
        .get_mut(&member)
        .unwrap();
    leaf.close(12).unwrap();
    std::fs::OpenOptions::new()
        .write(true)
        .open(leaf.path())
        .unwrap()
        .set_len(9)
        .unwrap();
    let record = job
        .tree
        .namespace_mut(NodeId::ROOT)
        .unwrap()
        .get_mut(member)
        .unwrap();
    record
        .coverage
        .write(ByteRange::new(0, 12), Provenance::Fresh);
    record.closed = true;

    job.writer
        .given_back(&mut job.tree, (NodeId::ROOT, member), owner)
        .unwrap();

    let record = job
        .tree
        .namespace(NodeId::ROOT)
        .unwrap()
        .get(member)
        .unwrap();
    assert!(record.coverage.covered(ByteRange::new(0, 9)));
    assert!(!record.coverage.covered(ByteRange::new(9, 12)));
    assert!(!record.closed);
}

#[test]
fn give_back_reports_a_missing_recorded_leaf_without_discarding_the_source_record() {
    let mut job = Job::new(PLENTY);
    let member = job.member("pack.bin", Some(12));
    job.send(member, 0, SIGNATURE, Provenance::Fresh);
    let owner = job.tree.child_of(NodeId::ROOT, member).unwrap();
    job.writer
        .write_file(
            &job.tree,
            NodeId::ROOT,
            member,
            0,
            &[SIGNATURE, b"hello"].concat().into(),
        )
        .unwrap();
    let leaf = job
        .writer
        .files_mut(NodeId::ROOT)
        .leaves
        .get_mut(&member)
        .unwrap();
    leaf.close(12).unwrap();
    std::fs::remove_file(leaf.path()).unwrap();

    assert!(
        job.writer
            .given_back(&mut job.tree, (NodeId::ROOT, member), owner)
            .is_err()
    );
    assert!(job.writer.files[&NodeId::ROOT].leaves.contains_key(&member));
}

// A restore brings an envelope back as far as its file reaches, and the
// member's coverage as far as what arrived. Given back after that, the
// envelope is the member's file, and what it lacks is not in hand: the
// coverage gives it up to come again, so replaying the file reads only
// what the file holds.
#[test]
fn a_member_given_back_after_a_restore_holds_only_what_its_envelope_kept() {
    let mut job = Job::new(PLENTY);
    let member = job.member("pack.bin", None);
    job.send(member, 0, SIGNATURE, Provenance::Fresh);
    let (child, volume) = job
        .tree
        .child_of(NodeId::ROOT, member)
        .expect("bound to the store");
    let kept = job.writer.envelopes.records(child);
    assert_eq!(kept.len(), 1);
    job.writer = NamespaceWriter::new(
        1,
        job.dir.path().to_path_buf(),
        Arc::clone(&job.accountant),
        CapacityReader::unknown(),
    );
    let envelope = job
        .dir
        .path()
        .join(crate::pipeline::graph::envelope::envelope_name(
            child,
            kept[0].envelope,
        ));
    std::fs::OpenOptions::new()
        .write(true)
        .open(&envelope)
        .and_then(|file| file.set_len(3))
        .expect("the crash left three bytes");
    job.writer.restore_kept(&kept[0]).unwrap();

    let mut build = job.build();
    job.writer
        .said(
            &mut job.tree,
            &mut build,
            &Source,
            child,
            Emit::GiveBack(volume),
            &mut job.out,
        )
        .expect("give back");
    let replayed = job.writer.replay_file(
        &mut job.tree,
        &mut build,
        &Source,
        NodeId::ROOT,
        member,
        None,
        &mut job.out,
    );
    assert!(replayed.is_ok(), "{replayed:?}");
    let coverage = &job
        .tree
        .namespace(NodeId::ROOT)
        .and_then(|namespace| namespace.get(member))
        .expect("the member")
        .coverage;
    assert!(coverage.covered(ByteRange::new(0, 3)));
    assert!(!coverage.covered(ByteRange::new(3, 4)), "{coverage:?}");
    let access = job.writer.access(&job.tree);
    let mut posted = Vec::new();
    assert!(access.read_at(NodeId::ROOT, member, ByteRange::new(0, 3), &mut posted));
    assert_eq!(posted, &SIGNATURE[..3]);
    assert!(!access.read_at(NodeId::ROOT, member, ByteRange::new(3, 4), &mut posted));
}

// A retracted member has no file: what comes back is written anew, under
// the name it keeps. The file it had stays where the last checkpoint names
// it until the one that says it is gone commits.
#[test]
fn a_retraction_discards_the_leaf_and_keeps_its_name_for_what_comes_back() {
    let mut job = Job::new(PLENTY);
    let (member, child) = bound_store(&mut job);
    job.send(member, 12, b"RETRACT", Provenance::Fresh);
    assert!(job.out.contains(&Effect::Retracted {
        node: child,
        member: MemberId(0),
    }));
    assert!(job.dir.path().join("inner.bin").exists());
    job.commit();
    assert!(!job.dir.path().join("inner.bin").exists());
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
            bytes: Bytes::from_static(b"ab").into(),
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
    // The open member's file goes once the checkpoint that says so commits.
    assert!(job.dir.path().join("open.bin").exists());
    job.commit();
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

// A store that gave up is gone, and its envelopes with it, but their files
// stay until the checkpoint that records it gone has committed. Until then
// the checkpoint the database holds names them, and a crash comes back to
// it with the bytes they hold still on disk.
#[test]
fn a_gone_stores_envelope_file_stays_until_the_checkpoint_saying_so_commits() {
    let mut job = Job::new(PLENTY);
    let (_, child) = bound_store(&mut job);
    let kept = job.writer.envelopes.records(child);
    assert_eq!(kept.len(), 1);
    let envelope = job.dir.path().join(kept[0].file.clone().unwrap_or_else(|| {
        crate::pipeline::graph::envelope::envelope_name(child, kept[0].envelope)
    }));
    assert!(envelope.exists());
    // The checkpoint that names the envelope has committed.
    job.writer.recorded(1);
    job.writer.commit_moves(&mut job.tree, 1).expect("commit");
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
    assert!(job.tree.buried(child));
    assert!(job.writer.envelopes.records(child).is_empty());
    assert!(
        envelope.exists(),
        "no checkpoint has said the store is gone"
    );
    assert!(job.writer.awaits_commit());
    job.writer.recorded(2);
    job.writer.commit_moves(&mut job.tree, 1).expect("commit");
    assert!(
        envelope.exists(),
        "the committed checkpoint still names the envelope"
    );
    job.writer.commit_moves(&mut job.tree, 2).expect("commit");
    assert!(!envelope.exists());
    assert!(!job.writer.awaits_commit());
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
    job.commit();
    assert_eq!(job.file("INNER.bin"), b"data");
    assert_eq!(job.file("inner (1).bin"), b"hello");
    assert!(!job.dir.path().join("3f9a.bin").exists());
}

// The store says each of `emits` of its namespace, in order.
fn store_says(job: &mut Job, child: NodeId, emits: Vec<Emit>) {
    let mut build = job.build();
    for emit in emits {
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
}

// A whole member of the store: announced, written and closed.
fn store_file(member: u32, name: &str, bytes: &'static [u8]) -> Vec<Emit> {
    vec![
        Emit::Member {
            member: MemberId(member),
            name: name.into(),
            length: Some(bytes.len() as u64),
        },
        Emit::Bytes {
            member: MemberId(member),
            offset: 0,
            bytes: Bytes::from_static(bytes).into(),
            provenance: Provenance::Fresh,
        },
        Emit::MemberClosed(MemberId(member)),
    ]
}

// Two stores side by side: the first ranks higher, so its names win.
fn two_stores(job: &mut Job) -> (NodeId, NodeId) {
    let (_, higher) = bound_store(job);
    let (_, lower) = bound_store_at(job, "pack2.bin");
    (higher, lower)
}

fn directory(member: u32, name: &str) -> Vec<Emit> {
    vec![
        Emit::Member {
            member: MemberId(member),
            name: name.into(),
            length: Some(0),
        },
        Emit::MemberClosed(MemberId(member)),
    ]
}

// The lower store wrote its file "folder"; the higher one then names the
// directory "folder/" and a file in it. The file moves aside, and the
// directory and what goes in it wait for that move instead of being made
// over the file.
#[test]
fn a_directory_named_over_a_written_file_waits_for_the_file_to_move() {
    let mut job = Job::new(PLENTY);
    let (higher, lower) = two_stores(&mut job);
    store_says(&mut job, lower, store_file(1, "folder", b"data"));
    assert_eq!(job.file("folder"), b"data");
    let mut emits = directory(1, "folder/");
    emits.extend(store_file(2, "folder/b.bin", b"hi"));
    store_says(&mut job, higher, emits);
    job.commit();
    let file = job.tree.namespace(lower).unwrap().get(MemberId(1)).unwrap();
    assert_eq!(file.output_name, "folder (1)");
    assert_eq!(job.file("folder (1)"), b"data");
    assert!(job.dir.path().join("folder").is_dir());
    assert_eq!(job.file("folder/b.bin"), b"hi");
}

// Two archives hold one directory, and the lower one wrote it and a file
// in it first. The directory is shared: neither moves, so the file stays
// where its name says.
#[test]
fn a_directory_two_archives_hold_is_shared_and_never_moves() {
    let mut job = Job::new(PLENTY);
    let (higher, lower) = two_stores(&mut job);
    let mut emits = directory(1, "folder/");
    emits.extend(store_file(2, "folder/b.bin", b"data"));
    store_says(&mut job, lower, emits);
    store_says(&mut job, higher, directory(1, "folder/"));
    job.commit();
    for node in [higher, lower] {
        let namespace = job.tree.namespace(node).unwrap();
        assert_eq!(namespace.get(MemberId(1)).unwrap().output_name, "folder/");
    }
    assert_eq!(job.file("folder/b.bin"), b"data");
    assert!(!job.dir.path().join("folder (1)").exists());
}

// One archive holds a file "folder", another "folder/b.bin" and no entry
// for the directory. Both cannot be on disk; the file moves aside, and
// both are written.
#[test]
fn a_file_and_a_path_through_its_name_are_both_written() {
    let mut job = Job::new(PLENTY);
    let (higher, lower) = two_stores(&mut job);
    store_says(&mut job, higher, store_file(1, "folder", b"data"));
    store_says(&mut job, lower, store_file(1, "folder/b.bin", b"hi"));
    job.commit();
    let file = job
        .tree
        .namespace(higher)
        .unwrap()
        .get(MemberId(1))
        .unwrap();
    assert_eq!(file.output_name, "folder (1)");
    assert_eq!(job.file("folder (1)"), b"data");
    assert_eq!(job.file("folder/b.bin"), b"hi");
}

// A repaired header takes back a directory the store had announced. The
// directory goes once the checkpoint that stops naming it commits, unless
// another archive still holds it or something is still in it.
#[test]
fn a_retracted_directory_goes_unless_something_still_holds_it() {
    let mut job = Job::new(PLENTY);
    let (higher, lower) = two_stores(&mut job);
    let mut emits = directory(1, "alone/");
    emits.extend(directory(2, "shared/"));
    emits.extend(directory(3, "full/"));
    emits.extend(store_file(4, "full/kept.bin", b"kept"));
    store_says(&mut job, lower, emits);
    store_says(&mut job, higher, directory(1, "shared/"));
    job.commit();
    store_says(
        &mut job,
        lower,
        [1, 2, 3]
            .map(|member| Emit::Retract(MemberId(member)))
            .to_vec(),
    );
    job.commit();
    assert!(!job.dir.path().join("alone").exists());
    assert!(job.dir.path().join("shared").is_dir());
    assert_eq!(job.file("full/kept.bin"), b"kept");
}

// Commit until no move waits, returning the first commit error instead of
// panicking, so the test can say what the job would see.
fn commit_or_error(job: &mut Job) -> Result<(), WriteError> {
    while job.writer.moves_waiting() {
        let stamp = job.writer.committed + 1;
        job.writer.recorded(stamp);
        job.writer.commit_moves(&mut job.tree, stamp)?;
    }
    Ok(())
}

// A file "folder" in the higher archive moved aside for the lower archive's
// "folder/b.bin" (no directory entry). The lower archive then gives up while
// b.bin is still open: its name is released, so the file takes "folder"
// back. b.bin goes at the commit, but the implicit directory "folder" it
// made stays, so the take-back rename meets an empty directory.
#[test]
fn a_file_takes_back_a_name_an_implicit_directory_held() {
    let mut job = Job::new(PLENTY);
    let (higher, lower) = two_stores(&mut job);
    store_says(&mut job, higher, store_file(1, "folder", b"data"));
    store_says(
        &mut job,
        lower,
        vec![
            Emit::Member {
                member: MemberId(1),
                name: "folder/b.bin".into(),
                length: Some(4),
            },
            Emit::Bytes {
                member: MemberId(1),
                offset: 0,
                bytes: Bytes::from_static(b"hi").into(),
                provenance: Provenance::Fresh,
            },
        ],
    );
    commit_or_error(&mut job).expect("first commit");
    assert_eq!(job.file("folder (1)"), b"data");
    assert!(job.dir.path().join("folder/b.bin").exists());
    store_says(&mut job, lower, vec![Emit::Refuse(Why::Compressed)]);
    let committed = commit_or_error(&mut job);
    let file = job
        .tree
        .namespace(higher)
        .unwrap()
        .get(MemberId(1))
        .unwrap()
        .output_name
        .clone();
    assert!(
        committed.is_ok(),
        "take-back commit failed: {committed:?}; ledger name {file:?}; folder is_dir={}",
        job.dir.path().join("folder").is_dir()
    );
    assert_eq!(job.file(&file), b"data");
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
    job.send(member, 4, b"more", Provenance::Fresh);
    job.commit();
    assert!(!job.dir.path().join("3f9a.bin").exists());
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
            bytes: Bytes::copy_from_slice(bytes).into(),
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
// why. Holds past a budget give nothing back: the spill is said once per
// budget, with how far that budget stands.
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
    job.send(member, 20, b"later", Provenance::Fresh);
    assert!(lines.of("object refused").is_empty());
    assert_eq!(
        lines.of("holds spilled"),
        ["holds spilled job_id=1 node=0 member=0 budget=scratch_ceiling held=0 limit=0"]
    );
}

// Every reason an object can give a member back for reaches the job's
// events, with the member and the archive that gave it back; the same
// reason again says nothing new.
#[test]
fn every_reason_a_member_is_given_back_for_is_noted_once() {
    for why in crate::pipeline::graph::checkpoint::REASONS {
        let mut job = Job::new(PLENTY);
        let (_, child) = bound_store(&mut job);
        for _ in 0..2 {
            let mut build = job.build();
            job.writer
                .said(
                    &mut job.tree,
                    &mut build,
                    &Source,
                    child,
                    Emit::Refuse(why),
                    &mut job.out,
                )
                .expect("refuse");
        }
        assert_eq!(
            job.writer.take_given_back(),
            [super::GivenBack {
                member: "pack.bin".to_string(),
                container: "rar_store".to_string(),
                why,
            }],
            "{why:?}"
        );
        assert!(job.writer.take_given_back().is_empty(), "{why:?}");
    }
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

// The deadline counts the time a decoder executes, never the wall clock: a
// job that sat queued past it extracts its first bytes, and a stored
// member whose bytes come straight from the posts writes on however long
// they take to come. Once a decoder has executed past it, the next write
// of what objects produced is refused, worded as the deadline.
#[tokio::test(start_paused = true)]
async fn the_deadline_counts_only_the_time_a_decoder_executes() {
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
        crate::pipeline::graph::limits::known_policy(policy),
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

    // The stored member's bytes keep coming, past the deadline twice over.
    tokio::time::advance(past).await;
    let arrival = |offset: u64, bytes: &'static [u8]| Arrival {
        node: NodeId::ROOT,
        member: store,
        offset,
        bytes: Bytes::from_static(bytes).into(),
        provenance: Provenance::Fresh,
    };
    let mut build = job.build();
    let written = job.writer.deliver(
        &mut job.tree,
        &mut build,
        &Source,
        arrival(front.len() as u64, b"wor"),
        &mut job.out,
    );
    assert!(written.is_ok());
    assert!(job.writer.refusal().is_none());

    // A decoder that waits is not decoding; one that executes past the
    // deadline is.
    let decoding = job.writer.decode_clock().decoding();
    {
        let _waiting = decoding.wait();
        tokio::time::advance(past).await;
    }
    tokio::time::advance(past).await;
    drop(decoding);
    let mut build = job.build();
    let refused = job.writer.deliver(
        &mut job.tree,
        &mut build,
        &Source,
        arrival(front.len() as u64 + 3, b"ld"),
        &mut job.out,
    );
    assert!(refused.is_err());
    let refusal = job.writer.refusal().expect("refused");
    assert_eq!(refusal.reason(), ExtractionRejectionReason::Deadline);
    assert_eq!(
        refusal.job_failure_reason(),
        format!("WEAVER_EXTRACTION_REJECTED[deadline]: extraction exceeded the {DEADLINE} second deadline")
    );
}

// Time the job spends paused is not extraction: a decoder that executes
// past the deadline while the job is paused leaves it writing on once it
// resumes.
#[tokio::test(start_paused = true)]
async fn the_deadline_does_not_run_while_the_job_is_paused() {
    use crate::pipeline::extraction::ExtractionLimits;
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
        crate::pipeline::graph::limits::known_policy(policy),
        Vec::new(),
    ));

    let store = job.member("show.rar", None);
    let mut front = SIGNATURE.to_vec();
    front.extend_from_slice(b"hello");
    job.send(store, 0, &front, Provenance::Fresh);
    assert!(job.writer.refusal().is_none());

    let decoding = job.writer.decode_clock().decoding();
    job.writer.hold_clock(true);
    tokio::time::advance(std::time::Duration::from_secs(DEADLINE + 1)).await;
    job.writer.hold_clock(false);
    drop(decoding);
    let arrival = Arrival {
        node: NodeId::ROOT,
        member: store,
        offset: front.len() as u64,
        bytes: Bytes::from_static(b"world").into(),
        provenance: Provenance::Fresh,
    };
    let mut build = job.build();
    let written = job
        .writer
        .deliver(&mut job.tree, &mut build, &Source, arrival, &mut job.out);
    assert!(written.is_ok());
    assert!(job.writer.refusal().is_none());
}

// Limits nothing here comes near, but for the disk reserve.
fn reserve_limits(min_free_bytes: u64) -> crate::pipeline::extraction::ExtractionLimits {
    crate::pipeline::extraction::ExtractionLimits {
        max_job_bytes: u64::MAX,
        max_member_bytes: u64::MAX,
        max_entries: u64::MAX,
        max_ratio: u64::MAX,
        ratio_floor_bytes: u64::MAX,
        max_seconds: u64::MAX,
        min_free_bytes,
        max_memory_bytes: u64::MAX,
    }
}

// A read of the policy that fails is no breach: the names placed while it
// fails wait, and are judged when a read succeeds, refused or not.
#[test]
fn names_placed_while_the_policy_is_unreadable_are_judged_once_it_reads() {
    use crate::pipeline::extraction::ExtractionRejectionReason;
    use crate::post_processing::model::PostProcessingSettings;
    use std::sync::atomic::{AtomicBool, Ordering};

    let readable = Arc::new(AtomicBool::new(false));
    let read: crate::pipeline::graph::limits::PolicyRead = {
        let readable = Arc::clone(&readable);
        Arc::new(move || {
            if readable.load(Ordering::SeqCst) {
                Ok(Arc::new(PostProcessingSettings {
                    unacceptable_extensions: vec!["exe".into()],
                    ..PostProcessingSettings::default()
                }))
            } else {
                Err("database is locked".into())
            }
        })
    };
    let mut job = Job::new(PLENTY);
    job.writer = job.writer.capped(Caps::new(
        Arc::new(reserve_limits(0)),
        read,
        Vec::new(),
    ));
    let allowed = job.member("notes.bin", Some(5));
    job.member("beacon.exe", Some(5));
    assert!(job.writer.refusal().is_none());
    job.send(allowed, 0, b"notes", Provenance::Fresh);
    assert_eq!(job.file("notes.bin"), b"notes");

    let unread = job.writer.learn_policy();
    assert_eq!(
        unread,
        Err("The Unacceptable extensions setting could not be read: database is locked".into())
    );
    assert!(job.writer.refusal().is_none());

    readable.store(true, Ordering::SeqCst);
    assert_eq!(job.writer.learn_policy(), Ok(()));
    let refusal = job.writer.refusal().expect("the waiting name is refused");
    assert_eq!(refusal.reason(), ExtractionRejectionReason::ContentPolicy);
    assert_eq!(
        refusal.job_failure_reason(),
        "unwanted extension '.exe' in 'beacon.exe'"
    );
}

// A write the disk reserve refuses says the disk is out of space: where,
// what is free and what must stay free, in units a reader reads, under the
// marker anything that parses it reads.
#[test]
fn a_disk_reserve_refusal_says_the_disk_is_out_of_space() {
    use crate::operations::disk::{Capacity, CapacityReading};
    use crate::pipeline::extraction::ExtractionRejectionReason;

    const MIB: u64 = 1024 * 1024;
    let mut job = Job::new(PLENTY);
    job.inner = 10;
    let output = job.dir.path().join("complete");
    std::fs::create_dir_all(&output).unwrap();
    let reading = Capacity::Known(CapacityReading {
        available_bytes: 1536 * MIB,
        total_bytes: 100 * 1024 * MIB,
        sampled_at: std::time::Instant::now(),
        stale: false,
    });
    job.writer = job
        .writer
        .capped(Caps::new(
            Arc::new(reserve_limits(2048 * MIB)),
            crate::pipeline::graph::limits::known_policy(Default::default()),
            Vec::new(),
        ))
        .delivering_to(output.clone(), CapacityReader::from_fn(move || reading));

    let store = job.member("show.rar", None);
    let mut front = SIGNATURE.to_vec();
    front.extend_from_slice(b"hello");
    let arrival = Arrival {
        node: NodeId::ROOT,
        member: store,
        offset: 0,
        bytes: Bytes::from(front).into(),
        provenance: Provenance::Fresh,
    };
    let mut build = job.build();
    let refused = job
        .writer
        .deliver(&mut job.tree, &mut build, &Source, arrival, &mut job.out);
    assert!(refused.is_err());
    let refusal = job.writer.refusal().expect("the reserve refused the write");
    assert_eq!(refusal.reason(), ExtractionRejectionReason::DiskReserve);
    assert_eq!(
        refusal.job_failure_reason(),
        format!(
            "WEAVER_EXTRACTION_REJECTED[disk_reserve]: out of disk space: the disk holding {} \
             has 1.5 GiB free, writing 10 B more would leave 1.5 GiB, and 2.0 GiB must stay free",
            output.display()
        )
    );
}

// The stated checksum is checked whatever shape the writes left the fold
// in: a write over bytes already written forgets the run it overlapped, so
// the fold no longer spans the member, and the file is read back instead.
// Bytes rewritten wrong are damage; bytes rewritten the same stay clean.
#[test]
fn a_member_failing_its_stated_checksum_is_damaged_when_a_rewrite_broke_the_fold() {
    for (again, damaged) in [(&b"midend"[..], false), (&b"mixend"[..], true)] {
        let mut job = Job::new(PLENTY);
        let member = job.member("video.mkv", Some(9));
        job.tree
            .namespace_mut(NodeId::ROOT)
            .and_then(|namespace| namespace.get_mut(member))
            .expect("the announced member")
            .crc32 = Some(crc32(b"abcmidend"));
        job.send(member, 0, b"abcmi", Provenance::Fresh);
        job.send(member, 3, again, Provenance::Fresh);
        job.close(member);
        assert!(!job.is_open(NodeId::ROOT, member));
        let expected = if damaged {
            vec![(member, ByteRange::new(0, 9))]
        } else {
            Vec::new()
        };
        assert_eq!(
            job.root().damaged_ranges().collect::<Vec<_>>(),
            expected,
            "rewrote {:?}",
            String::from_utf8_lossy(again)
        );
    }
}
#[test]
fn same_path_rewrite_invalidates_repair_loan() {
    let mut job = Job::new(PLENTY).with_set(Some(Fix {
        name: "payload.bin",
        read: ByteRange::new(0, 32768),
        fix: (20000, b"OK"),
    }));
    let member = job.member("payload.bin", Some(32768));
    job.send(member, 0, &vec![0x6d; 32768], Provenance::Fresh);
    let mut lent = Inline::default();
    let due = job.tree.recovery_nodes(NodeId::ROOT);
    job.writer
        .repair(&mut job.tree, NodeId::ROOT, &due, &mut lent)
        .unwrap();
    assert_eq!(lent.0.len(), 1);
    let returned = lent
        .0
        .pop()
        .unwrap()
        .run(weaver_graph::Budget::default(), &Default::default());
    assert!(
        !job.writer.moved_since(&job.tree, &returned),
        "unchanged input retains the loan"
    );
    let before = job.writer.files[&NodeId::ROOT].leaves[&member].revision();
    job.send(member, 20000, b"new", Provenance::Fresh);
    assert_ne!(
        job.writer.files[&NodeId::ROOT].leaves[&member].revision(),
        before
    );
    assert_eq!(&job.file("payload.bin")[20000..20003], b"new");
    assert!(job.writer.moved_since(&job.tree, &returned));
    let mut build = job.build();
    let error = job
        .writer
        .returned(
            &mut job.tree,
            &mut build,
            &Source,
            &weaver_objects::Objects::default(),
            returned,
            &mut job.out,
        )
        .unwrap_err();
    assert!(matches!(error, WriteError::StaleRecovery { .. }));
    assert_eq!(
        &job.file("payload.bin")[20000..20003],
        b"new",
        "stale writes cannot publish without reconstruction"
    );
}

#[test]
fn rejected_damage_does_not_change_recovery_evidence() {
    let mut job = Job::new(PLENTY).with_set(None);
    let member = job.member("payload.bin", Some(8));
    job.send(member, 0, b"repaired", Provenance::Repaired);
    let fed = job.par.lock().unwrap().fed.len();
    job.send(member, 2, b"BAD", Provenance::Damaged);
    assert_eq!(job.file("payload.bin"), b"repaired");
    assert_eq!(job.par.lock().unwrap().fed.len(), fed);
}

#[test]
fn clipping_source_evidence_does_not_upgrade_verification() {
    struct Unverified;
    impl Segments for Unverified {
        fn segments_for(&self, _: NodeId, _: VolumeIndex, _: ByteRange, out: &mut Vec<Segment>) {
            out.push(Segment {
                range: ByteRange::new(0, 8),
                crc32: 0,
                verified: false,
            });
        }
    }
    let mut job = Job::new(PLENTY).with_set(None);
    let member = job.member("payload.bin", Some(8));
    let mut build = job.build();
    let arrival = Arrival {
        node: NodeId::ROOT,
        member,
        offset: 2,
        bytes: Bytes::from_static(b"data").into(),
        provenance: Provenance::Fresh,
    };
    job.writer
        .deliver(
            &mut job.tree,
            &mut build,
            &Unverified,
            arrival,
            &mut job.out,
        )
        .unwrap();
    let seen = job.par.lock().unwrap();
    assert_eq!(
        seen.fed.last(),
        Some(&Segment {
            range: ByteRange::new(2, 6),
            crc32: crc32(b"data"),
            verified: false,
        })
    );
}

#[test]
fn unrelated_candidate_and_shared_scratch_activity_do_not_invalidate_repair_loan() {
    for resident_limit in [u64::MAX, 0] {
        let mut job = Job::new(HoldBudgets {
            resident_limit,
            ..PLENTY
        })
        .with_set(Some(Fix {
            name: "payload.bin",
            read: ByteRange::new(0, 32768),
            fix: (20000, b"OK"),
        }));
        let member = job.member("payload.bin", Some(32768));
        let other = job.member("other.pending", Some(32768));
        job.send(member, 0, &vec![0x6d; 32768], Provenance::Fresh);
        job.send(other, 20000, b"old", Provenance::Fresh);
        let before = job.writer.files[&NodeId::ROOT].leaves[&member].revision();
        let mut lent = Inline::default();
        let due = job.tree.recovery_nodes(NodeId::ROOT);
        job.writer
            .repair(&mut job.tree, NodeId::ROOT, &due, &mut lent)
            .unwrap();
        let returned = lent
            .0
            .pop()
            .unwrap()
            .run(weaver_graph::Budget::default(), &Default::default());
        job.writer
            .keep_held(NodeId::ROOT, other, 25000, Bytes::from_static(b"held").into())
            .unwrap();
        job.send(other, 20000, b"new", Provenance::Fresh);
        assert_eq!(
            job.writer.files[&NodeId::ROOT].leaves[&member].revision(),
            before
        );
        assert!(
            !job.writer.moved_since(&job.tree, &returned),
            "unconsumed candidate activity cannot stale the repair"
        );
    }
}

#[test]
fn replacement_of_a_borrowed_hold_invalidates_repair_loan() {
    for resident_limit in [u64::MAX, 0] {
        let mut job = Job::new(HoldBudgets {
            resident_limit,
            ..PLENTY
        })
        .with_set(Some(Fix {
            name: "payload.bin",
            read: ByteRange::new(0, 8),
            fix: (2, b"OK"),
        }));
        let member = job.member("payload.bin", Some(8));
        job.writer
            .keep_held(NodeId::ROOT, member, 0, Bytes::from_static(b"original").into())
            .unwrap();
        job.tree
            .namespace_mut(NodeId::ROOT)
            .unwrap()
            .get_mut(member)
            .unwrap()
            .coverage
            .write(ByteRange::new(0, 8), Provenance::Fresh);
        let mut lent = Inline::default();
        let due = job.tree.recovery_nodes(NodeId::ROOT);
        job.writer
            .repair(&mut job.tree, NodeId::ROOT, &due, &mut lent)
            .unwrap();
        let returned = lent
            .0
            .pop()
            .unwrap()
            .run(weaver_graph::Budget::default(), &Default::default());
        assert_eq!(
            job.par.lock().unwrap().read.as_deref(),
            Some(b"original".as_slice())
        );
        assert!(!job.writer.moved_since(&job.tree, &returned));
        job.writer
            .keep_held(NodeId::ROOT, member, 2, Bytes::from_static(b"new").into())
            .unwrap();
        assert!(
            job.writer.moved_since(&job.tree, &returned),
            "borrowed RAM and scratch sources keep their own identities"
        );
    }
}

#[test]
fn a_returned_ruling_stands_on_the_ranges_it_read_and_only_those() {
    for resident_limit in [u64::MAX, 0] {
        let mut job = Job::new(HoldBudgets {
            resident_limit,
            ..PLENTY
        })
        .with_set(Some(Fix {
            name: "payload.bin",
            read: ByteRange::new(0, 8),
            fix: (2, b"OK"),
        }));
        let member = job.member("payload.bin", Some(16));
        let arrive = |job: &mut Job, offset: u64, bytes: &'static [u8]| {
            job.writer
            .keep_held(NodeId::ROOT, member, offset, Bytes::from_static(bytes).into())
            .unwrap();
            job.tree
                .namespace_mut(NodeId::ROOT)
                .unwrap()
                .get_mut(member)
                .unwrap()
                .coverage
                .write(
                    ByteRange::new(offset, offset + bytes.len() as u64),
                    Provenance::Fresh,
                );
        };
        arrive(&mut job, 0, b"original");
        let mut lent = Inline::default();
        let due = job.tree.recovery_nodes(NodeId::ROOT);
        job.writer
            .repair(&mut job.tree, NodeId::ROOT, &due, &mut lent)
            .unwrap();
        let returned = lent
            .0
            .pop()
            .unwrap()
            .run(weaver_graph::Budget::default(), &Default::default());
        assert_eq!(
            job.par.lock().unwrap().read.as_deref(),
            Some(b"original".as_slice())
        );
        arrive(&mut job, 8, b"and more");
        assert!(
            !job.writer.moved_since(&job.tree, &returned),
            "bytes past what the repair read leave its ruling standing"
        );
        arrive(&mut job, 2, b"new");
        assert!(
            job.writer.moved_since(&job.tree, &returned),
            "a hold replaced inside what the repair read takes its ruling back"
        );
    }
}

// The file a repair read is written again past what it read while the
// repair is out: its ruling stands. A byte it read written again, or said
// again with another provenance, takes the ruling back.
#[test]
fn a_returned_ruling_stands_while_its_file_changes_past_what_it_read() {
    for rewritten in [true, false] {
        let mut job = Job::new(PLENTY).with_set(Some(Fix {
            name: "payload.bin",
            read: ByteRange::new(0, 8),
            fix: (2, b"OK"),
        }));
        let member = job.member("payload.bin", Some(16));
        job.send(member, 0, b"original and mor", Provenance::Fresh);
        let mut lent = Inline::default();
        let due = job.tree.recovery_nodes(NodeId::ROOT);
        job.writer
            .repair(&mut job.tree, NodeId::ROOT, &due, &mut lent)
            .unwrap();
        let returned = lent
            .0
            .pop()
            .unwrap()
            .run(weaver_graph::Budget::default(), &Default::default());
        let write = |job: &mut Job, offset: u64, bytes: &[u8]| {
            job.writer
                .files_mut(NodeId::ROOT)
                .leaves
                .get_mut(&member)
                .unwrap()
                .write(offset, bytes, None)
                .unwrap();
        };
        write(&mut job, 8, b"AND MORE");
        assert_eq!(job.file("payload.bin"), b"originalAND MORE");
        assert!(
            !job.writer.moved_since(&job.tree, &returned),
            "bytes past what the repair read leave its ruling standing"
        );
        if rewritten {
            write(&mut job, 4, b"new");
        } else {
            job.tree
                .namespace_mut(NodeId::ROOT)
                .unwrap()
                .get_mut(member)
                .unwrap()
                .coverage
                .write(ByteRange::new(4, 7), Provenance::Repaired);
        }
        assert!(
            job.writer.moved_since(&job.tree, &returned),
            "rewritten={rewritten}: a byte the repair read changed takes its ruling back"
        );
    }
}

#[test]
fn worker_diagnostics_are_published_only_for_current_acknowledgment() {
    use crate::pipeline::graph::hardware::Shutdown;
    use std::sync::atomic::{AtomicBool, Ordering};
    for mode in 0..3 {
        let mut job = Job::new(PLENTY).with_set(Some(Fix {
            name: "payload.bin",
            read: ByteRange::new(0, 32768),
            fix: (20000, b"OK"),
        }));
        let member = job.member("payload.bin", Some(32768));
        job.send(member, 0, &vec![0x6d; 32768], Provenance::Fresh);
        job.par.lock().unwrap().report_error = true;
        let stop = Arc::new(AtomicBool::new(false));
        if mode == 2 {
            let stop = Arc::clone(&stop);
            job.par.lock().unwrap().between =
                Some(Box::new(move || stop.store(true, Ordering::Release)));
        }
        let mut lent = Inline::default();
        let due = job.tree.recovery_nodes(NodeId::ROOT);
        job.writer
            .repair(&mut job.tree, NodeId::ROOT, &due, &mut lent)
            .unwrap();
        let returned = lent
            .0
            .pop()
            .unwrap()
            .run(weaver_graph::Budget::default(), &Shutdown::of(stop));
        assert_eq!(
            returned.diagnostics.len(),
            1,
            "the worker captured the injected error"
        );
        if mode == 1 {
            job.send(member, 20000, b"new", Provenance::Fresh);
        }
        let mut build = job.build();
        let result = job.writer.returned(
            &mut job.tree,
            &mut build,
            &Source,
            &weaver_objects::Objects::default(),
            returned,
            &mut job.out,
        );
        if mode == 1 {
            assert!(matches!(result, Err(WriteError::StaleRecovery { .. })));
        } else {
            result.unwrap();
        }
        if mode == 0 {
            let [diagnostic] = job.writer.diagnostics.as_slice() else {
                panic!("one accepted diagnostic");
            };
            assert_eq!(diagnostic.operation, "execute recovery");
            assert_eq!(diagnostic.context, "set-a");
            assert_eq!(diagnostic.error, "injected reconstruction failure");
            assert_eq!(diagnostic.causes, ["injected storage cause"]);
        } else {
            assert!(
                job.writer.diagnostics.is_empty(),
                "stale or stopped work cannot log a current failure"
            );
        }
    }
}

// The only file under the implicit directory "folder" is renamed out of it
// (a recovery object names the member): nothing goes, so remove_left never
// sees the directory. The file "folder (1)" takes "folder" back and meets
// the empty directory left behind.
#[test]
fn a_file_takes_back_a_name_a_renamed_file_left_as_an_empty_directory() {
    let mut job = Job::new(PLENTY);
    let (higher, lower) = two_stores(&mut job);
    store_says(&mut job, higher, store_file(1, "folder", b"data"));
    store_says(&mut job, lower, store_file(1, "folder/b.bin", b"hi"));
    commit_or_error(&mut job).expect("first commit");
    assert_eq!(job.file("folder (1)"), b"data");
    let mut build = job.build();
    let renamed = job
        .writer
        .rename(&mut job.tree, &mut build, lower, MemberId(1), "other.bin")
        .expect("rename");
    assert_eq!(renamed, "other.bin");
    let committed = commit_or_error(&mut job);
    let file = job
        .tree
        .namespace(higher)
        .unwrap()
        .get(MemberId(1))
        .unwrap()
        .output_name
        .clone();
    assert!(
        committed.is_ok(),
        "take-back commit failed: {committed:?}; ledger name {file:?}; folder is_dir={}",
        job.dir.path().join("folder").is_dir()
    );
    assert_eq!(job.file(&file), b"data");
    assert_eq!(job.file("other.bin"), b"hi");
}

// One archive gives up while the other still has an open file in the
// shared implicit directory: the directory and that file stay.
#[test]
fn a_give_up_keeps_a_directory_another_archives_open_file_sits_in() {
    let mut job = Job::new(PLENTY);
    let (higher, lower) = two_stores(&mut job);
    let open = |member: u32, name: &str, bytes: &'static [u8]| {
        vec![
            Emit::Member {
                member: MemberId(member),
                name: name.into(),
                length: Some(8),
            },
            Emit::Bytes {
                member: MemberId(member),
                offset: 0,
                bytes: Bytes::from_static(bytes).into(),
                provenance: Provenance::Fresh,
            },
        ]
    };
    store_says(&mut job, higher, open(1, "folder/keep.bin", b"keep"));
    store_says(&mut job, lower, open(1, "folder/sub/b.bin", b"gone"));
    commit_or_error(&mut job).expect("first commit");
    store_says(&mut job, lower, vec![Emit::Refuse(Why::Compressed)]);
    commit_or_error(&mut job).expect("give-up commit");
    assert!(!job.dir.path().join("folder/sub").exists());
    assert!(job.file("folder/keep.bin").starts_with(b"keep"));
    // The still-open file writes on into the kept directory.
    store_says(
        &mut job,
        higher,
        vec![Emit::Bytes {
            member: MemberId(1),
            offset: 4,
            bytes: Bytes::from_static(b"more").into(),
            provenance: Provenance::Fresh,
        }],
    );
    assert_eq!(job.file("folder/keep.bin"), b"keepmore");
}

// A give-up empties "Folder/x" while the other archive holds the directory
// member "folder/" (other case): on a case-folding disk that is one
// directory, and it stays.
#[test]
fn a_give_up_keeps_a_directory_held_under_another_case() {
    let mut job = Job::new(PLENTY);
    let (higher, lower) = two_stores(&mut job);
    store_says(&mut job, higher, directory(1, "folder/"));
    store_says(
        &mut job,
        lower,
        vec![
            Emit::Member {
                member: MemberId(1),
                name: "Folder/x.bin".into(),
                length: Some(4),
            },
            Emit::Bytes {
                member: MemberId(1),
                offset: 0,
                bytes: Bytes::from_static(b"hi").into(),
                provenance: Provenance::Fresh,
            },
        ],
    );
    commit_or_error(&mut job).expect("first commit");
    store_says(&mut job, lower, vec![Emit::Refuse(Why::Compressed)]);
    commit_or_error(&mut job).expect("give-up commit");
    assert!(job.dir.path().join("folder").is_dir());
}
