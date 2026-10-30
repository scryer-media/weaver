// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::*;
use crate::pipeline::graph::status::Claim;
use std::sync::atomic::Ordering;

fn pending() -> (Job, MemberId, Vec<u8>) {
    let mut job = Job::new(PLENTY);
    let bytes = vec![0x71; (2 * READ_BACK + 13) as usize];
    let member = job.member("payload.bin", Some(bytes.len() as u64));
    job.send(member, 0, &bytes, Provenance::Fresh);
    job.close(member);
    job.tree
        .namespace_mut(NodeId::ROOT)
        .unwrap()
        .get_mut(member)
        .unwrap()
        .crc32 = Some(crc32(&bytes));
    // A recorded backing can be returned from a restored envelope without
    // a runtime fold. Keep its bytes and ownership, just as that transfer does.
    job.writer
        .files_mut(NodeId::ROOT)
        .leaves
        .insert(member, Leaf::at(job.dir.path(), "payload.bin"));
    assert!(
        !job.writer
            .fails_its_checksum(&job.tree, NodeId::ROOT, member)
            .unwrap()
    );
    assert_eq!(job.writer.pending_verifications(), vec![(0, member.0)]);
    (job, member, bytes)
}

fn apply(job: &mut Job, verified: Verified) -> Result<(), WriteError> {
    let mut build = job.build();
    job.writer
        .verified(&mut job.tree, &mut build, &Source, verified, &mut job.out)
}

#[test]
fn a_complete_fold_needs_neither_a_read_nor_a_pending_record() {
    let mut job = Job::new(PLENTY);
    let member = job.member("payload.bin", Some(7));
    // The leaf folds only for a member that states a check when the bytes land.
    job.tree
        .namespace_mut(NodeId::ROOT)
        .unwrap()
        .get_mut(member)
        .unwrap()
        .crc32 = Some(crc32(b"payload"));
    job.send(member, 0, b"payload", Provenance::Fresh);
    job.close(member);
    std::fs::remove_file(job.dir.path().join("payload.bin")).unwrap();
    assert!(
        !job.writer
            .fails_its_checksum(&job.tree, NodeId::ROOT, member)
            .unwrap()
    );
    assert!(job.writer.pending_verifications().is_empty());
}

// A member that states no check: no reader needs a fold of it, so its
// leaf hashes none of the bytes it writes.
#[test]
fn a_member_stating_no_check_is_written_without_a_fold() {
    let mut job = Job::new(PLENTY);
    let bytes = vec![0x5c; 4096];
    let member = job.member("payload.bin", Some(bytes.len() as u64));
    let before = weaver_graph::evidence::counting::hashed();
    job.send(member, 0, &bytes, Provenance::Fresh);
    assert_eq!(weaver_graph::evidence::counting::hashed() - before, 0);
    let leaf = &job.writer.files_mut(NodeId::ROOT).leaves[&member];
    assert_eq!(
        leaf.folded(bytes.len() as u64, weaver_graph::Algorithm::Crc32),
        None
    );
}

// Bytes that came proven land before their member states its check, as a
// split RAR member's do before its last header: the leaf folds what they
// came with, hashing none of them, and the check stated after finds the
// fold without reading the file back.
#[test]
fn a_check_stated_after_proven_bytes_landed_finds_their_fold() {
    use weaver_graph::{Digest, Evidence, Evidenced};
    let mut job = Job::new(PLENTY);
    let member = job.member("payload.bin", Some(7));
    let piece = |range: std::ops::Range<u64>| Evidence {
        range: ByteRange::new(range.start, range.end),
        digest: Digest::crc32(crc32(&b"payload"[range.start as usize..range.end as usize])),
    };
    let arrival = Arrival {
        node: NodeId::ROOT,
        member,
        offset: 0,
        bytes: Evidenced::with(Bytes::from_static(b"payload"), [piece(0..3), piece(3..7)]),
        provenance: Provenance::Fresh,
    };
    let before = weaver_graph::evidence::counting::hashed();
    let mut build = job.build();
    job.writer
        .deliver(&mut job.tree, &mut build, &Source, arrival, &mut job.out)
        .expect("deliver");
    assert_eq!(weaver_graph::evidence::counting::hashed() - before, 0);
    job.tree
        .namespace_mut(NodeId::ROOT)
        .unwrap()
        .get_mut(member)
        .unwrap()
        .crc32 = Some(crc32(b"payload"));
    job.close(member);
    std::fs::remove_file(job.dir.path().join("payload.bin")).unwrap();
    assert!(
        !job.writer
            .fails_its_checksum(&job.tree, NodeId::ROOT, member)
            .unwrap()
    );
    assert!(job.writer.pending_verifications().is_empty());
}

// A member whose file an object took over after its namespace ended: no
// check of it can be lent, so its obligation keeps the job at work no
// longer.
#[test]
fn an_obligation_whose_file_an_object_took_is_not_due_once_its_namespace_ended() {
    let (mut job, member, _) = pending();
    assert!(job.writer.verifications_due(&job.tree, false));
    job.writer.files_mut(NodeId::ROOT).leaves.remove(&member);
    assert!(job.writer.take_verification(&job.tree).is_none());
    assert!(!job.writer.verifications_due(&job.tree, false));
}

// A repair slice fills a gap and closes the member before a later slice of
// the same batch replaces other bytes in it: the member is judged once, on
// the bytes the batch ends with, with no obligation left behind.
#[test]
fn a_member_closed_during_acknowledgement_gets_its_verdict_once() {
    for correct_final_bytes in [false, true] {
        let mut job = Job::new(PLENTY);
        let member = job.member("payload.bin", Some(8));
        job.tree
            .namespace_mut(NodeId::ROOT)
            .unwrap()
            .get_mut(member)
            .unwrap()
            .crc32 = Some(crc32(b"abcdefgh"));
        job.send(member, 4, b"efgX", Provenance::Fresh);
        job.writer.acknowledging = Some(Acknowledging {
            set: NodeId(91),
            closed: Default::default(),
        });
        job.send(member, 0, b"abcd", Provenance::Repaired);
        job.close(member);
        assert!(job.root().get(member).unwrap().closed);
        assert!(
            !job.root()
                .get(member)
                .unwrap()
                .coverage
                .damaged(ByteRange::new(0, 8)),
            "a completed gap must not reject bytes that later repair slices will replace"
        );
        if correct_final_bytes {
            job.send(member, 7, b"h", Provenance::Repaired);
        }
        let batch = job.writer.acknowledging.take().unwrap();
        let mut build = job.build();
        job.writer
            .acknowledged(&mut job.tree, &mut build, &Source, batch, &mut job.out)
            .unwrap();
        let whole = ByteRange::new(0, 8);
        let verdicts = job
            .out
            .iter()
            .filter(|effect| {
                matches!(effect, Effect::Damaged { node, member: damaged, range }
                    if *node == NodeId::ROOT && *damaged == member && *range == whole)
            })
            .count();
        assert_eq!(
            verdicts,
            usize::from(!correct_final_bytes),
            "only the completed repair batch determines the checksum verdict"
        );
        assert_eq!(
            job.root().get(member).unwrap().coverage.damaged(whole),
            !correct_final_bytes
        );
        assert!(job.writer.pending_verifications().is_empty());
        assert!(job.writer.take_verification(&job.tree).is_none());
    }
}

// A member closes with a stated checksum while a run of it is damaged, so
// nothing can check it yet. A clean arrival over the damage makes it whole
// by coverage: the obligation it closed with still stands, and the check
// finds the bytes wrong before the census can count it.
#[test]
fn a_member_cleaned_after_close_is_checked_before_it_counts_whole() {
    let mut job = Job::new(PLENTY);
    let member = job.member("payload.bin", Some(8));
    job.tree
        .namespace_mut(NodeId::ROOT)
        .unwrap()
        .get_mut(member)
        .unwrap()
        .crc32 = Some(crc32(b"abcdefgh"));
    job.send(member, 0, b"abcd", Provenance::Fresh);
    job.send(member, 4, b"efgh", Provenance::Damaged);
    job.close(member);
    let whole = ByteRange::new(0, 8);
    assert!(job.root().get(member).unwrap().closed);
    assert_eq!(job.writer.pending_verifications(), vec![(0, member.0)]);
    assert!(job.writer.take_verification(&job.tree).is_none());

    job.send(member, 4, b"efgX", Provenance::Fresh);
    assert!(!job.root().get(member).unwrap().coverage.damaged(whole));
    let check = job
        .writer
        .take_verification(&job.tree)
        .expect("the cleaned member is checked");
    apply(&mut job, check.run(&Default::default())).unwrap();
    let verdicts = job
        .out
        .iter()
        .filter(|effect| {
            matches!(effect, Effect::Damaged { node, member: damaged, range }
                if *node == NodeId::ROOT && *damaged == member && *range == whole)
        })
        .count();
    assert_eq!(verdicts, 1);
    assert!(!job.writer.verifications_waiting());
    assert_eq!(
        Claim::of(job.root().get(member).unwrap()),
        Claim::Damaged { bytes: 8 }
    );
}

// One article of a member with a stated checksum never came, so nothing
// closed it; its set rebuilds the gap. That proof closes the member at its
// length, the batch judges it, and its one check settles what the census
// counts: whole on the right checksum, damaged on a wrong one.
#[test]
fn a_member_a_set_rebuilt_whole_is_closed_and_judged_once() {
    for right in [true, false] {
        let mut job = Job::new(PLENTY).with_set(Some(Fix {
            name: "video.mkv",
            read: ByteRange { start: 0, end: 8 },
            fix: (2, b"OK"),
        }));
        let member = job.member("video.mkv", Some(8));
        let stated = crc32(b"goOKgood") ^ u32::from(!right);
        job.tree
            .namespace_mut(NodeId::ROOT)
            .unwrap()
            .get_mut(member)
            .unwrap()
            .crc32 = Some(stated);
        job.send(member, 0, b"go", Provenance::Fresh);
        job.send(member, 4, b"good", Provenance::Fresh);
        assert!(!job.root().get(member).unwrap().closed);
        // A recorded backing with no runtime fold, as a restore returns it:
        // the verdict needs a check that reads the file.
        job.writer
            .files_mut(NodeId::ROOT)
            .leaves
            .insert(member, Leaf::at(job.dir.path(), "video.mkv"));

        let set = job.writer.sets(&job.tree)[0].1;
        let mut build = job.build();
        job.repair(&mut build, NodeId::ROOT, &[set]);
        assert!(job.root().get(member).unwrap().closed, "{right}");
        assert_eq!(job.writer.pending_verifications(), vec![(0, member.0)]);
        let check = job.writer.take_verification(&job.tree).unwrap();
        apply(&mut job, check.run(&Default::default())).unwrap();
        assert!(job.writer.take_verification(&job.tree).is_none());
        assert!(!job.writer.verifications_waiting());
        let claim = Claim::of(job.root().get(member).unwrap());
        if right {
            assert_eq!(claim, Claim::Complete);
        } else {
            assert_eq!(claim, Claim::Damaged { bytes: 8 });
        }
    }
}

#[test]
fn verification_reads_bounded_chunks_and_clears_only_on_acknowledgment() {
    let (mut job, member, bytes) = pending();
    let check = job.writer.take_verification(&job.tree).unwrap();
    let mut reads = Vec::new();
    let verified = check.run_with(&Default::default(), |len| reads.push(len));
    assert_eq!(reads, vec![READ_BACK as usize, READ_BACK as usize, 13]);
    assert_eq!(job.writer.pending_verifications(), vec![(0, member.0)]);
    apply(&mut job, verified).unwrap();
    assert!(!job.writer.verifications_waiting());
    assert_eq!(
        job.writer.files[&NodeId::ROOT].leaves[&member]
            .folded(bytes.len() as u64, weaver_graph::Algorithm::Crc32),
        Some(u64::from(crc32(&bytes)))
    );
}

#[test]
fn a_checksum_mismatch_marks_the_member_damaged_through_the_graph() {
    let (mut job, member, bytes) = pending();
    job.tree
        .namespace_mut(NodeId::ROOT)
        .unwrap()
        .get_mut(member)
        .unwrap()
        .crc32 = Some(crc32(&bytes) ^ 1);
    let check = job.writer.take_verification(&job.tree).unwrap();
    apply(&mut job, check.run(&Default::default())).unwrap();
    assert!(
        job.root()
            .get(member)
            .unwrap()
            .coverage
            .damaged(ByteRange::new(0, bytes.len() as u64))
    );
    assert!(!job.writer.verifications_waiting());
}

#[test]
fn a_rewrite_discards_an_old_answer_before_installing_a_fold() {
    let (mut job, member, _) = pending();
    let check = job.writer.take_verification(&job.tree).unwrap();
    let verified = check.run(&Default::default());
    job.writer
        .files_mut(NodeId::ROOT)
        .leaves
        .get_mut(&member)
        .unwrap()
        .write(9, &[0x42], None)
        .unwrap();
    apply(&mut job, verified).unwrap();
    assert!(job.writer.verifications_waiting());
    let check = job.writer.take_verification(&job.tree).unwrap();
    apply(&mut job, check.run(&Default::default())).unwrap();
    assert!(
        job.root()
            .get(member)
            .unwrap()
            .coverage
            .damaged(ByteRange::new(0, 10))
    );
}

#[test]
fn cancellation_between_chunks_keeps_the_durable_obligation() {
    let (mut job, member, _) = pending();
    let check = job.writer.take_verification(&job.tree).unwrap();
    let stop = Arc::clone(&check.stop);
    let mut reads = 0;
    let verified = check.run_with(&Default::default(), |_| {
        reads += 1;
        stop.store(true, Ordering::Relaxed);
    });
    assert_eq!(reads, 1);
    apply(&mut job, verified).unwrap();
    assert_eq!(job.writer.pending_verifications(), vec![(0, member.0)]);
    let check = job.writer.take_verification(&job.tree).unwrap();
    apply(&mut job, check.run(&Default::default())).unwrap();
    assert!(!job.writer.verifications_waiting());
}

#[test]
fn cancellation_before_start_reads_nothing() {
    let (mut job, _, _) = pending();
    let check = job.writer.take_verification(&job.tree).unwrap();
    check.stop.store(true, Ordering::Relaxed);
    let verified = check.run_with(&Default::default(), |_| {
        panic!("cancelled check read bytes")
    });
    apply(&mut job, verified).unwrap();
    assert!(job.writer.verifications_waiting());
}

#[test]
fn cancellation_after_read_before_acknowledgment_keeps_the_obligation() {
    let (mut job, member, bytes) = pending();
    let check = job.writer.take_verification(&job.tree).unwrap();
    let stop = Arc::clone(&check.stop);
    let verified = check.run(&Default::default());
    stop.store(true, Ordering::Relaxed);
    apply(&mut job, verified).unwrap();
    assert_eq!(job.writer.pending_verifications(), vec![(0, member.0)]);
    assert_eq!(
        job.writer.files[&NodeId::ROOT].leaves[&member]
            .folded(bytes.len() as u64, weaver_graph::Algorithm::Crc32),
        None
    );
    let check = job.writer.take_verification(&job.tree).unwrap();
    apply(&mut job, check.run(&Default::default())).unwrap();
    assert!(!job.writer.verifications_waiting());
}

#[test]
fn a_stale_read_error_cannot_fail_replacement_backing() {
    let (mut job, member, bytes) = pending();
    let check = job.writer.take_verification(&job.tree).unwrap();
    std::fs::remove_file(job.dir.path().join("payload.bin")).unwrap();
    let verified = check.run(&Default::default());
    let mut replacement = Leaf::at(job.dir.path(), "payload.bin");
    replacement.write(0, &bytes, None).unwrap();
    job.writer
        .files_mut(NodeId::ROOT)
        .leaves
        .insert(member, replacement);
    apply(&mut job, verified).unwrap();
    assert!(job.writer.verifications_waiting());
    let check = job.writer.take_verification(&job.tree).unwrap();
    apply(&mut job, check.run(&Default::default())).unwrap();
    assert!(!job.writer.verifications_waiting());
}

#[test]
fn a_current_read_error_never_counts_as_success() {
    let (mut job, _, _) = pending();
    let check = job.writer.take_verification(&job.tree).unwrap();
    std::fs::remove_file(job.dir.path().join("payload.bin")).unwrap();
    assert!(apply(&mut job, check.run(&Default::default())).is_err());
    assert!(job.writer.verifications_waiting());
}

#[test]
fn a_panicking_worker_returns_an_error_without_clearing_pending() {
    let (mut job, _, _) = pending();
    let check = job.writer.take_verification(&job.tree).unwrap();
    let verified = check.run_with(&Default::default(), |_| panic!("injected read failure"));
    assert!(apply(&mut job, verified).is_err());
    assert!(job.writer.verifications_waiting());
}

#[test]
fn restoring_a_pending_check_cannot_assume_the_stated_checksum_passed() {
    let (mut job, member, bytes) = pending();
    job.tree
        .namespace_mut(NodeId::ROOT)
        .unwrap()
        .get_mut(member)
        .unwrap()
        .crc32 = Some(crc32(&bytes) ^ 1);
    let state = job.writer.file_state(NodeId::ROOT, member);
    let mut restored = NamespaceWriter::new(
        1,
        job.dir.path().to_owned(),
        Arc::clone(&job.accountant),
        CapacityReader::unknown(),
    );
    restored.restore_verifications(&job.writer.pending_verifications());
    restored
        .restore_file(&job.tree, NodeId::ROOT, member, &state)
        .unwrap();
    assert_eq!(
        restored.files[&NodeId::ROOT].leaves[&member]
            .folded(bytes.len() as u64, weaver_graph::Algorithm::Crc32),
        None
    );
    job.writer = restored;
    let check = job.writer.take_verification(&job.tree).unwrap();
    apply(&mut job, check.run(&Default::default())).unwrap();
    assert!(
        job.root()
            .get(member)
            .unwrap()
            .coverage
            .damaged(ByteRange::new(0, bytes.len() as u64))
    );
}
