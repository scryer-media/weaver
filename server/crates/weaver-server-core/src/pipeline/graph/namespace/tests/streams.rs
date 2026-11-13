// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A namespace whose source is a stream's run, not the articles: what the run
// says lands where any delivery lands, so a store below it keeps its
// envelopes and a recovery set below it keeps its volume, and both read
// back.

use super::*;

const XZ: &[u8] = &[0xFD, 0x37, 0x7A, 0x58, 0x5A, 0x00, 0x00, 0x04];

// A pretend stream: its volume is its envelope, and its one member comes
// from its run, which the test plays by saying the run's emits. Each time it
// hears its volume closed it says how many bytes it had been delivered.
pub(super) struct Decoded {
    delivered: u64,
    heard: Arc<Mutex<Vec<u64>>>,
}

impl Decoded {
    pub(super) fn new(heard: Arc<Mutex<Vec<u64>>>) -> Self {
        Self {
            delivered: 0,
            heard,
        }
    }
}

impl Object for Decoded {
    // It keeps no record of what it said of its posted bytes.
    fn unposted(&mut self, _: weaver_graph::VolumeIndex, _: weaver_graph::ByteRange) {}

    fn kind(&self) -> Kind {
        Kind::Xz
    }

    fn deliver(
        &mut self,
        delivery: Delivery<'_>,
        _: &mut dyn FnMut(&weaver_graph::Placement, &mut Vec<u8>) -> bool,
        _: &mut Vec<Emit>,
    ) {
        self.delivered += delivery.bytes.len() as u64;
    }

    fn volume_closed(&mut self, _: VolumeIndex, _: &mut Vec<Emit>) {
        self.heard.lock().unwrap().push(self.delivered);
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

    fn reads_in_order(&self) -> bool {
        true
    }
}

// A root member bound to the pretend stream, and the stream's node.
fn stream(job: &mut Job) -> NodeId {
    let member = job.member("pack.xz", None);
    job.send(member, 0, XZ, Provenance::Fresh);
    let (node, _) = job
        .tree
        .child_of(NodeId::ROOT, member)
        .expect("bound to the stream");
    node
}

impl Job {
    fn say(&mut self, node: NodeId, emit: Emit) {
        let mut build = self.build();
        self.writer
            .said(
                &mut self.tree,
                &mut build,
                &Source,
                node,
                emit,
                &mut self.out,
            )
            .expect("said");
    }

    fn say_member(&mut self, node: NodeId, member: u32, name: &str, length: Option<u64>) {
        self.say(
            node,
            Emit::Member {
                member: MemberId(member),
                name: name.into(),
                length,
            },
        );
    }

    fn say_bytes(&mut self, node: NodeId, member: u32, offset: u64, bytes: &[u8]) {
        self.say(
            node,
            Emit::Bytes {
                member: MemberId(member),
                offset,
                bytes: Bytes::copy_from_slice(bytes).into(),
                provenance: Provenance::Fresh,
            },
        );
    }
}

#[test]
fn a_store_a_stream_feeds_reads_its_volume_back_through_its_envelope() {
    let mut job = Job::new(PLENTY);
    let node = stream(&mut job);
    job.say_member(node, 0, "pack.bin", None);
    job.say_bytes(node, 0, 0, &SIGNATURE[..3]);
    let mut rest = SIGNATURE[3..].to_vec();
    rest.extend_from_slice(b"hello");
    job.say_bytes(node, 0, 3, &rest);
    assert!(job.tree.child_of(node, MemberId(0)).is_some(), "a store");
    assert_eq!(job.file("inner.bin"), b"hello");

    let access = job.writer.access(&job.tree);
    let mut posted = Vec::new();
    assert!(access.read_at(node, MemberId(0), ByteRange::new(0, 12), &mut posted));
    assert_eq!(posted, [SIGNATURE, b"hello"].concat());
}

#[test]
fn a_set_a_stream_feeds_reads_its_volume_back_from_its_file() {
    let mut job = Job::new(PLENTY);
    let node = stream(&mut job);
    job.say_member(node, 0, "set.par2", Some(PAR2_SIGNATURE.len() as u64));
    job.say_bytes(node, 0, 0, PAR2_SIGNATURE);
    assert!(
        job.tree
            .recovers(job.tree.child_of(node, MemberId(0)).expect("a set").0)
    );
    assert_eq!(job.file("set.par2"), PAR2_SIGNATURE);

    let mut build = job.build();
    let due = job.tree.recovery_nodes(node);
    job.repair(&mut build, node, &due);
    assert_eq!(
        job.par.lock().unwrap().volume.as_deref(),
        Some(PAR2_SIGNATURE),
        "the set reads its own volume back"
    );
}

// A stream says a member's bytes in two pieces and closes it in one pass,
// and the first piece binds a stream below: the member's object hears its
// volume closed once, after it has every byte the member received.
#[test]
fn a_stream_bound_and_closed_in_one_pass_hears_its_volume_closed_once_after_its_bytes() {
    let mut job = Job::new(PLENTY);
    let node = stream(&mut job);
    job.say_member(node, 0, "inner.xz", None);
    let mut build = job.build();
    let mut effects = Vec::new();
    job.tree.emitted(
        node,
        vec![
            Emit::Bytes {
                member: MemberId(0),
                offset: 0,
                bytes: Bytes::from_static(&XZ[..6]).into(),
                provenance: Provenance::Fresh,
            },
            Emit::Bytes {
                member: MemberId(0),
                offset: 6,
                bytes: Bytes::from_static(&XZ[6..]).into(),
                provenance: Provenance::Fresh,
            },
            Emit::MemberClosed(MemberId(0)),
        ],
        &mut job.writer.placing(&mut build),
        &mut effects,
    );
    job.writer
        .apply_all(&mut job.tree, &mut build, &Source, effects, &mut job.out)
        .expect("applied");
    assert!(
        job.tree.child_of(node, MemberId(0)).is_some(),
        "a stream below"
    );
    assert_eq!(*job.heard.lock().unwrap(), [XZ.len() as u64]);
}

// The caps over a writer whose job declared nothing, with `edit` applied:
// the job's limit is `max_job_bytes`.
fn caps(edit: impl FnOnce(&mut crate::pipeline::extraction::ExtractionLimits)) -> Caps {
    let mut limits = super::reserve_limits(0);
    edit(&mut limits);
    Caps::new(
        Arc::new(limits),
        crate::pipeline::graph::limits::known_policy(Default::default()),
        Vec::new(),
    )
}

impl Job {
    // A run's bytes of a member, as the writer takes them, refused or not.
    fn try_bytes(
        &mut self,
        node: NodeId,
        member: u32,
        offset: u64,
        bytes: &[u8],
    ) -> Result<(), WriteError> {
        let mut build = self.build();
        self.writer.said(
            &mut self.tree,
            &mut build,
            &Source,
            node,
            Emit::Bytes {
                member: MemberId(member),
                offset,
                bytes: Bytes::copy_from_slice(bytes).into(),
                provenance: Provenance::Fresh,
            },
            &mut self.out,
        )
    }

    // A member of `node` that a stream below reads, its first `front`
    // bytes said: the stream's node.
    fn nested(&mut self, node: NodeId, front: &[u8]) -> NodeId {
        self.say_member(node, 0, "inner.xz", None);
        self.say_bytes(node, 0, 0, front);
        let (child, _) = self
            .tree
            .child_of(node, MemberId(0))
            .expect("a stream below");
        child
    }

    // The bytes the stream at `node` keeps of its volume.
    fn kept(&self, node: NodeId) -> u64 {
        self.writer
            .kept_file(node, VolumeIndex(0))
            .and_then(|path| std::fs::metadata(path).ok())
            .map_or(0, |metadata| metadata.len())
    }
}

fn reason(job: &Job) -> crate::pipeline::extraction::ExtractionRejectionReason {
    job.writer.refusal().expect("refused").reason()
}

// A member one stream produces and another reads is no delivered file: the
// member limit is not its to meet. Past the limit it is kept whole.
#[test]
fn a_member_another_stream_reads_is_not_held_to_the_member_limit() {
    const LIMIT: u64 = 64;
    let mut job = Job::new(PLENTY);
    job.writer = job
        .writer
        .capped(caps(|limits| limits.max_member_bytes = LIMIT));
    let node = stream(&mut job);
    let child = job.nested(node, XZ);
    let rest = vec![7u8; 2 * LIMIT as usize - XZ.len()];
    job.say_bytes(node, 0, XZ.len() as u64, &rest);
    assert!(job.writer.refusal().is_none());
    assert_eq!(job.kept(child), 2 * LIMIT);
}

// A file a stream below delivers past the member limit is refused, as a
// file anywhere is, naming the member limit.
#[test]
fn a_file_a_stream_below_delivers_past_the_member_limit_is_refused() {
    use crate::pipeline::extraction::ExtractionRejectionReason;

    const LIMIT: u64 = 64;
    let mut job = Job::new(PLENTY);
    job.writer = job
        .writer
        .capped(caps(|limits| limits.max_member_bytes = LIMIT));
    let node = stream(&mut job);
    let child = job.nested(node, XZ);
    // A stated length under the limit ends its front, so its owner is
    // decided at once: a file, which the bytes said past it grow.
    job.say_member(child, 0, "film.mkv", Some(4));
    job.say_bytes(child, 0, 0, &[9u8; LIMIT as usize]);
    assert!(job.writer.refusal().is_none());

    assert!(job.try_bytes(child, 0, LIMIT, &[9]).is_err());
    assert_eq!(reason(&job), ExtractionRejectionReason::MemberBytes);
    assert_eq!(
        job.writer.refusal().unwrap().job_failure_reason(),
        format!(
            "WEAVER_EXTRACTION_REJECTED[member_bytes]: member output would reach {} bytes, \
             limit is {LIMIT}",
            LIMIT + 1
        )
    );
}

// A member that waited past the member limit and then becomes a file is
// judged at everything it holds, as a file written to that length is.
#[test]
fn a_member_that_waited_past_the_member_limit_is_refused_once_it_is_a_file() {
    use crate::operations::disk::Capacity;
    use crate::pipeline::extraction::ExtractionRejectionReason;
    use crate::pipeline::graph::limits::{Device, Output};

    const LIMIT: u64 = 64;
    let mut job = Job::new(PLENTY);
    let node = stream(&mut job);
    let mut caps = caps(|limits| limits.max_member_bytes = LIMIT);
    let folder = job.dir.path().to_path_buf();
    let mut admit = |end: u64, output: Output| {
        caps.admit_write(
            &job.tree,
            (node, MemberId(5)),
            end,
            output,
            (&folder, Capacity::Unknown, Device::Output),
        )
    };
    assert!(admit(LIMIT + 1, Output::Kept).is_ok());
    let refused = admit(LIMIT + 1, Output::File).expect_err("refused");
    assert_eq!(refused.reason(), ExtractionRejectionReason::MemberBytes);
    assert!(
        refused
            .job_failure_reason()
            .contains(&format!("member output would reach {} bytes", LIMIT + 1)),
        "{}",
        refused.job_failure_reason()
    );
}

// A member another stream reads and the files that stream writes are one
// output at two levels: each counts against the job's limit alone, never
// added to the other. A file exactly at the limit is written, though the
// member before it took bytes too; one byte more is refused.
#[test]
fn a_member_another_stream_reads_counts_once_against_the_job_limit() {
    use crate::pipeline::extraction::ExtractionRejectionReason;

    const LIMIT: u64 = 64;
    for (inner, refused) in [(LIMIT, false), (LIMIT + 1, true)] {
        let mut job = Job::new(PLENTY);
        job.writer = job
            .writer
            .capped(caps(|limits| limits.max_job_bytes = LIMIT));
        let node = stream(&mut job);
        let child = job.nested(node, XZ);
        assert_eq!(job.kept(child), XZ.len() as u64);
        job.say_member(child, 0, "film.mkv", Some(inner));
        let written = job.try_bytes(child, 0, 0, &vec![9u8; inner as usize]);
        assert_eq!(written.is_err(), refused, "{inner}");
        if refused {
            assert_eq!(reason(&job), ExtractionRejectionReason::JobBytes);
            let file = job.dir.path().join("film.mkv");
            assert!(std::fs::metadata(file).map_or(0, |file| file.len()) == 0);
        } else {
            assert!(job.writer.refusal().is_none());
            assert_eq!(job.file("film.mkv").len() as u64, inner);
        }
    }
}

// The caps over a writer whose job declared nothing, whose output may reach
// `limit` under the ratio: the floor the ratio never goes under.
fn ratio_caps(limit: u64) -> Caps {
    caps(|limits| {
        limits.max_ratio = 100;
        limits.ratio_floor_bytes = limit;
    })
}

impl Job {
    // Three levels below the stream at `node`: a member it says that a
    // stream reads, `fill` bytes long; a member that stream says that a
    // third stream reads, `fill` bytes long; and `film.mkv`, a file the
    // third says, `fill` bytes long. The two streams' nodes.
    fn three_levels(&mut self, node: NodeId, fill: u64) -> (NodeId, NodeId) {
        let rest = vec![7u8; fill as usize - XZ.len()];
        let child = self.nested(node, XZ);
        self.say_bytes(node, 0, XZ.len() as u64, &rest);
        let grandchild = self.nested(child, XZ);
        self.say_bytes(child, 0, XZ.len() as u64, &rest);
        self.say_member(grandchild, 0, "film.mkv", Some(fill));
        self.say_bytes(grandchild, 0, 0, &vec![9u8; fill as usize]);
        (child, grandchild)
    }
}

// Output nested three levels deep, each level exactly at the ratio's limit,
// is written whole: no level is added to another, though the three
// together are three times the limit.
#[test]
fn three_levels_each_at_the_ratio_limit_are_never_added() {
    const LIMIT: u64 = 64;
    let mut job = Job::new(PLENTY);
    job.writer = job.writer.capped(ratio_caps(LIMIT));
    let node = stream(&mut job);
    let (child, grandchild) = job.three_levels(node, LIMIT);
    assert!(job.writer.refusal().is_none());
    assert_eq!(job.kept(child), LIMIT);
    assert_eq!(job.kept(grandchild), LIMIT);
    assert_eq!(job.file("film.mkv"), vec![9u8; LIMIT as usize]);
}

// A member another stream reads that alone expands past the ratio is
// refused as it passes it, though no file of it is ever written: nothing
// past the limit is kept.
#[test]
fn a_member_another_stream_reads_past_the_ratio_is_refused() {
    use crate::pipeline::extraction::ExtractionRejectionReason;

    const LIMIT: u64 = 64;
    let mut job = Job::new(PLENTY);
    job.writer = job.writer.capped(ratio_caps(LIMIT));
    let node = stream(&mut job);
    let child = job.nested(node, XZ);
    let rest = vec![7u8; LIMIT as usize - XZ.len()];
    job.say_bytes(node, 0, XZ.len() as u64, &rest);
    assert!(job.writer.refusal().is_none());

    assert!(job.try_bytes(node, 0, LIMIT, &[7]).is_err());
    assert_eq!(reason(&job), ExtractionRejectionReason::Ratio);
    assert_eq!(job.kept(child), LIMIT);
}

// Each level judged alone still stops a nest whose levels grow on one
// another: a small member read by a stream whose own member, read by a
// third, grows past the ratio is refused there.
#[test]
fn a_nest_whose_inner_level_grows_past_the_ratio_is_refused_there() {
    use crate::pipeline::extraction::ExtractionRejectionReason;

    const LIMIT: u64 = 64;
    let mut job = Job::new(PLENTY);
    job.writer = job.writer.capped(ratio_caps(LIMIT));
    let node = stream(&mut job);
    let child = job.nested(node, XZ);
    let grandchild = job.nested(child, XZ);
    let rest = vec![7u8; LIMIT as usize - XZ.len()];
    job.say_bytes(child, 0, XZ.len() as u64, &rest);
    assert!(job.writer.refusal().is_none());
    assert_eq!(job.kept(child), XZ.len() as u64);

    assert!(job.try_bytes(child, 0, LIMIT, &[7]).is_err());
    assert_eq!(reason(&job), ExtractionRejectionReason::Ratio);
    assert_eq!(job.kept(grandchild), LIMIT);
}

// A restored job counts each level as the run before it did: each level
// holds what it held, never the levels added, so each takes exactly one
// byte more under a limit one byte over what each holds, and the files a
// second byte is refused.
#[test]
fn a_restored_job_counts_each_level_once() {
    use crate::operations::disk::Capacity;
    use crate::pipeline::extraction::ExtractionRejectionReason;
    use crate::pipeline::graph::limits::{Device, Output};

    const LIMIT: u64 = 64;
    let mut job = Job::new(PLENTY);
    job.writer = job.writer.capped(ratio_caps(LIMIT));
    let node = stream(&mut job);
    let (child, grandchild) = job.three_levels(node, LIMIT);
    assert!(job.writer.refusal().is_none());

    let mut restored = ratio_caps(LIMIT + 1);
    restored.restore_members(&job.tree);
    let folder = job.dir.path().to_path_buf();
    let mut admit = |at: (NodeId, u32), end: u64, output: Output| {
        restored.admit_write(
            &job.tree,
            (at.0, MemberId(at.1)),
            end,
            output,
            (&folder, Capacity::Unknown, Device::Output),
        )
    };
    assert!(admit((node, 7), 1, Output::Kept).is_ok());
    assert!(admit((child, 7), 1, Output::Kept).is_ok());
    assert!(admit((grandchild, 7), 1, Output::File).is_ok());
    let refused = admit((grandchild, 8), 1, Output::File).expect_err("refused");
    assert_eq!(refused.reason(), ExtractionRejectionReason::Ratio);
}

// A member whose stated length is small counts what is said of it, not
// what it stated: a run that says more than it stated is stopped once the
// bytes said pass the limit, and its file holds no more than the limit.
#[test]
fn a_member_said_past_its_stated_length_is_stopped_at_the_member_limit() {
    use crate::pipeline::extraction::ExtractionRejectionReason;

    const LIMIT: u64 = 64;
    const PIECE: usize = 16;
    let mut job = Job::new(PLENTY);
    job.writer = job
        .writer
        .capped(caps(|limits| limits.max_member_bytes = LIMIT));
    let node = stream(&mut job);
    job.say_member(node, 0, "film.mkv", Some(4));
    let mut offset = 0;
    let refused = loop {
        if let Err(error) = job.try_bytes(node, 0, offset, &[3u8; PIECE]) {
            break error;
        }
        offset += PIECE as u64;
        assert!(offset <= LIMIT, "{offset}");
    };
    drop(refused);
    assert_eq!(offset, LIMIT);
    assert_eq!(reason(&job), ExtractionRejectionReason::MemberBytes);
    assert!(job.file("film.mkv").len() as u64 <= LIMIT);
}

// Bytes of a member whose front has not decided its owner are held, and
// count as they arrive at their level, but are no file yet: the member
// limit is not theirs to meet while they wait.
#[test]
fn a_member_held_undecided_is_not_held_to_the_member_limit() {
    const LIMIT: u64 = 64;
    let mut job = Job::new(PLENTY);
    job.writer = job
        .writer
        .capped(caps(|limits| limits.max_member_bytes = LIMIT));
    let node = stream(&mut job);
    job.say_member(node, 0, "notes.bin", Some(4 * LIMIT));
    // "Ra" could still be a RAR signature; the bytes after it wait on it.
    job.say_bytes(node, 0, 0, b"Ra");
    job.say_bytes(node, 0, 8, &[1u8; LIMIT as usize - 8]);
    assert!(job.tree.is_undecided(node, MemberId(0)));
    assert!(job.writer.refusal().is_none());

    assert!(job.try_bytes(node, 0, LIMIT, &[1]).is_ok());
    assert!(job.tree.is_undecided(node, MemberId(0)));
    assert!(job.writer.refusal().is_none());
}

// Where the job's working folder is on a device of its own, what waits
// there is held to the reserve against that device's reading, and what is
// delivered against the output's. Exactly the free space over the reserve
// is kept; a byte more is refused, worded for the working folder.
#[test]
fn what_waits_in_the_working_folder_is_held_to_the_reserve_of_its_device() {
    use crate::operations::disk::{Capacity, CapacityReading};
    use crate::pipeline::extraction::ExtractionRejectionReason;

    const RESERVE: u64 = 1024;
    const ROOM: u64 = 64;
    let sampled_at = std::time::Instant::now();
    let reading = move |available_bytes| {
        Capacity::Known(CapacityReading {
            available_bytes,
            total_bytes: 1 << 40,
            sampled_at,
            stale: false,
        })
    };
    let mut job = Job::new(PLENTY);
    let output = job.dir.path().join("complete");
    std::fs::create_dir_all(&output).unwrap();
    job.writer = NamespaceWriter::new(
        1,
        job.dir.path().to_path_buf(),
        Arc::clone(&job.accountant),
        CapacityReader::from_fn(move || reading(RESERVE + ROOM)),
    )
    .capped(caps(|limits| limits.min_free_bytes = RESERVE))
    .delivering_to(output, CapacityReader::from_fn(move || reading(u64::MAX)));
    let node = stream(&mut job);
    let child = job.nested(node, XZ);
    let rest = vec![7u8; ROOM as usize - XZ.len()];
    job.say_bytes(node, 0, XZ.len() as u64, &rest);
    assert!(job.writer.refusal().is_none());
    assert_eq!(job.kept(child), ROOM);

    assert!(job.try_bytes(node, 0, ROOM, &[7]).is_err());
    assert_eq!(reason(&job), ExtractionRejectionReason::DiskReserve);
    let failure = job.writer.refusal().unwrap().job_failure_reason();
    assert!(
        failure.contains(&format!("the disk holding {} ", job.dir.path().display())),
        "{failure}"
    );
    assert_eq!(job.kept(child), ROOM);
}
