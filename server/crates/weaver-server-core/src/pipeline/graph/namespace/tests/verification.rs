// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::*;
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
    job.send(member, 0, b"payload", Provenance::Fresh);
    job.close(member);
    job.tree
        .namespace_mut(NodeId::ROOT)
        .unwrap()
        .get_mut(member)
        .unwrap()
        .crc32 = Some(crc32(b"payload"));
    std::fs::remove_file(job.dir.path().join("payload.bin")).unwrap();
    assert!(
        !job.writer
            .fails_its_checksum(&job.tree, NodeId::ROOT, member)
            .unwrap()
    );
    assert!(job.writer.pending_verifications().is_empty());
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
        job.writer.files[&NodeId::ROOT].leaves[&member].folded(bytes.len() as u64),
        Some(crc32(&bytes))
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
        job.writer.files[&NodeId::ROOT].leaves[&member].folded(bytes.len() as u64),
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
        restored.files[&NodeId::ROOT].leaves[&member].folded(bytes.len() as u64),
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
