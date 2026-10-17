// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use std::sync::Mutex;

use weaver_graph::holds::HoldBudgets;
use weaver_graph::recovery::{Recovery, SliceVerdict};
use weaver_graph::{Delivery, Emit, Extent, Kind, Object, ObjectId, Pull, Seek};

use super::*;

const SIGNATURE: &[u8] = b"Rar!\x1a\x07\x00";
const HEADER: u64 = SIGNATURE.len() as u64;

// A pretend store with one volume: a seven-byte header kept as envelope 0,
// then one member, "inner.bin", to the end. The word RETRACT anywhere in a
// delivery takes the member back.
struct Store {
    announced: bool,
}

impl Object for Store {
    fn kind(&self) -> Kind {
        Kind::RarStore
    }

    fn deliver(&mut self, delivery: Delivery<'_>, out: &mut Vec<Emit>) {
        if delivery.bytes.windows(7).any(|window| window == b"RETRACT") {
            out.push(Emit::Retract(MemberId(0)));
            return;
        }
        if !self.announced {
            self.announced = true;
            out.push(Emit::Member {
                member: MemberId(0),
                name: "inner.bin".into(),
                length: Some(5),
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
    fn wants(&mut self, _: &mut Vec<Seek>) {}
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

    fn extent(&self, _: MemberId, _: ByteRange, _: &mut Vec<Extent>) {}

    fn envelope(&self, _: u32, _: ByteRange) -> Option<Bytes> {
        None
    }
}

// Builds the pretend store, and the pretend recovery set with the job's
// record of what it was fed and the fix it makes when asked to repair.
struct Build {
    par: Arc<Mutex<Seen>>,
    fix: Option<Fix>,
}

impl Builder for Build {
    fn build(&mut self, kind: Kind, _: ObjectId) -> Option<Box<dyn Object>> {
        match kind {
            Kind::RarStore => Some(Box::new(Store { announced: false })),
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
}

// The source's segments: one per delivered range, with a checksum the
// source vouches for.
const SOURCE_CRC: u32 = 0x5eed_c0de;

struct Source;

impl Segments for Source {
    fn segments_for(&self, _: VolumeIndex, range: ByteRange, out: &mut Vec<Segment>) {
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
const PAR2_SIGNATURE: &[u8] = b"PAR2\0PKT\x40\x00\x00\x00";

struct Par {
    volume: Option<MemberId>,
    others: Vec<MemberId>,
    fixing: Option<MemberId>,
    fix: Option<Fix>,
    seen: Arc<Mutex<Seen>>,
}

impl Object for Par {
    fn kind(&self) -> Kind {
        Kind::Par2
    }

    fn deliver(&mut self, _: Delivery<'_>, _: &mut Vec<Emit>) {}
    fn volume_closed(&mut self, _: VolumeIndex, _: &mut Vec<Emit>) {}
    fn volume_lost(&mut self, _: VolumeIndex, _: ByteRange, _: &mut Vec<Emit>) {}
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

impl Recovery for Par {
    fn bind(&mut self, namespace: &mut weaver_graph::Namespace, renamed: &mut Vec<MemberId>) {
        self.volume = namespace.find_by_name("set.par2").map(|own| own.id);
        self.fixing = self
            .fix
            .and_then(|fix| namespace.find_by_name(fix.name))
            .map(|member| member.id);
        self.others = namespace
            .members()
            .filter(|member| member.name.starts_with("other."))
            .map(|member| member.id)
            .collect();
        let recognised: Vec<MemberId> = namespace
            .members()
            .filter(|member| member.name == "3f9a.bin" && member.coverage.bytes_covered() > 0)
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
    fn wants(&mut self, _: &mut Vec<Seek>) {}

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

    fn blocks_reachable(&self) -> u64 {
        0
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
        let member = self.tree.announce_root(name, length);
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
    job.member("extras/", Some(0));
    job.member("empty.txt", Some(0));
    let nested = job.member("extras/inner.txt", Some(2));
    job.send(nested, 0, b"hi", Provenance::Fresh);
    assert!(job.dir.path().join("extras").is_dir());
    assert_eq!(job.file("empty.txt"), b"");
    assert_eq!(job.file("extras/inner.txt"), b"hi");
}

// The ledger confines every name to the working dir: a name that would
// climb out is written under the part of it that stays inside.
#[test]
fn a_name_that_would_escape_the_working_dir_is_confined() {
    let mut job = Job::new(PLENTY);
    let member = job.tree.announce_root("../escape.txt", Some(2));
    job.send(member, 0, b"no", Provenance::Fresh);
    assert_eq!(job.file("escape.txt"), b"no");
}

#[test]
fn a_rename_moves_the_file_and_the_name_allocation() {
    let mut job = Job::new(PLENTY);
    let member = job.member("3f9a.bin", Some(4));
    job.send(member, 0, b"data", Provenance::Fresh);
    let renamed = job
        .writer
        .rename(&mut job.tree, NodeId::ROOT, member, "show.part01.rar")
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
    let outcomes = job
        .writer
        .repair(
            &mut job.tree,
            &mut build,
            &Source,
            NodeId::ROOT,
            &mut job.out,
        )
        .expect("repair");
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

    let mut access = job.writer.access(&job.tree, NodeId::ROOT);
    let mut posted = Vec::new();
    assert!(access.read(member, ByteRange::new(0, 12), &mut posted));
    assert_eq!(posted, [SIGNATURE, b"hello"].concat());

    let mut inner = Vec::new();
    let mut child_access = job.writer.access(&job.tree, child);
    assert!(child_access.read(MemberId(0), ByteRange::new(1, 4), &mut inner));
    assert_eq!(inner, b"ell");

    let mut beyond = vec![1u8];
    assert!(!access.read(member, ByteRange::new(10, 20), &mut beyond));
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
