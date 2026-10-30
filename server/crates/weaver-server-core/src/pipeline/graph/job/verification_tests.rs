// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::*;
use crate::jobs::graph_checkpoint::{Delta, GraphCheckpoint};
use crate::pipeline::graph::checkpoint;
use crate::pipeline::graph::engine::{Heard, RunMail};
use crate::pipeline::graph::namespace::Lender;
use crate::pipeline::graph::root::tests::{decoded, file, job_with};
use crate::pipeline::graph::status::{RepairSlots, Step};
use weaver_graph::ByteRange;
use weaver_graph::slots::AskerId;

const SIZE: usize = (1 << 20) + 13;

fn pending() -> (GraphJob, tempfile::TempDir, RunMail, Vec<RootFile>) {
    let files = vec![file("payload.bin", &[SIZE as u32])];
    let (mut job, dir, _) = job_with(&files, Vec::new());
    let bytes = vec![0x71; SIZE];
    job.intake(
        decoded(0, 0, 0, &bytes, Some(SIZE as u64)),
        &mut RepairSlots::default(),
        1,
    )
    .unwrap();
    job.tree
        .namespace_mut(NodeId::ROOT)
        .unwrap()
        .get_mut(MemberId(0))
        .unwrap()
        .crc32 = Some(crc_turbo::checksum(crc_turbo::CrcAlgorithm::Crc32IsoHdlc, &bytes) as u32);
    let state = job.writer.file_state(NodeId::ROOT, MemberId(0));
    job.writer.restore_verifications(&[(0, 0)]);
    job.writer
        .restore_file(&job.tree, NodeId::ROOT, MemberId(0), &state)
        .unwrap();
    let mail = RunMail::default();
    job.loans = Loans::new(1, mail.sender(), job.runs.hardware());
    (job, dir, mail, files)
}

fn rows(delta: Delta) -> GraphCheckpoint {
    assert!(delta.whole);
    GraphCheckpoint {
        feed_verdict: delta.feed_verdict,
        nodes: delta.nodes.into_values().flatten().collect(),
        members: delta.members.into_values().flatten().collect(),
        coverage: delta.coverage.into_values().flatten().collect(),
        envelopes: delta.envelopes.into_values().flatten().collect(),
        crypt: delta.crypt.into_values().flatten().collect(),
        parts: delta.parts.into_values().flatten().collect(),
        holds: delta.holds.into_values().flatten().collect(),
        held_parts: delta.held_parts.into_values().flatten().collect(),
    }
}

#[tokio::test]
async fn a_blocked_verification_keeps_ownership_and_allows_checkpoints_until_acknowledged() {
    let (mut job, _dir, mut mail, _) = pending();
    let mut slots = RepairSlots::default();
    assert!(
        !job.repair_out(),
        "a queued checksum is not a second repair"
    );
    assert!(job.work_pending());
    assert!(
        matches!(
            crate::pipeline::graph::status::drive(&mut job, &mut slots, AskerId(1), 0),
            Step::Wait
        ),
        "a queued check still blocks delivery without an available slot"
    );
    let mut check = job.writer.take_verification(&job.tree).unwrap();
    let (started, resume) = check.pause_after_read();
    let mut lending = job.loans.lending(Some((&mut slots, 1)));
    assert!(lending.admit());
    lending.verify(check);
    started.await.unwrap();
    assert!(job.awaits_mail());
    assert!(!job.repair_out(), "an in-flight checksum is not a repair");
    assert_eq!(job.phase(), RootPhase::Verifying);
    assert!(matches!(
        crate::pipeline::graph::status::drive(&mut job, &mut slots, AskerId(1), 1),
        Step::Wait
    ));

    let checkpoint = Pending::default();
    assert!(checkpoint.put(&job, Vec::new()));
    let (delta, _) = checkpoint.take();
    let snapshot = checkpoint::snapshot(&rows(delta.unwrap())).unwrap();
    assert_eq!(snapshot.root.pending_verifications, vec![(0, 0)]);
    assert!(
        !checkpoint.put(&job, Vec::new()),
        "unchanged pending state writes nothing"
    );

    let (mut other, _other_dir, _) = job_with(&[file("other.bin", &[5])], Vec::new());
    other
        .intake(decoded(0, 0, 0, b"other", Some(5)), &mut slots, 1)
        .unwrap();
    let other_checkpoint = Pending::default();
    assert!(other_checkpoint.put(&other, Vec::new()));
    assert!(other_checkpoint.take().0.is_some());
    assert_eq!(other.phase(), RootPhase::Done);

    job.stop_runs(0);
    assert!(
        job.forget_slot().is_none(),
        "cancellation cannot release the worker's slot"
    );
    assert!(job.awaits_mail());
    resume.send(()).unwrap();
    let said = mail.recv().await.unwrap();
    assert!(matches!(&said.heard, Heard::Verified(_)));
    // A result under another loan number cannot release current ownership.
    let stale = job
        .writer
        .take_verification(&job.tree)
        .unwrap()
        .run(&Default::default());
    job.run_said(
        Said {
            scope: 1,
            node: said.node,
            generation: said.generation + 1,
            heard: Heard::Verified(Box::new(stale)),
        },
        &mut slots,
        0,
    )
    .unwrap();
    assert!(job.awaits_mail());
    job.run_said(said, &mut slots, 0).unwrap();
    assert!(!job.awaits_mail());
    assert_eq!(job.writer.pending_verifications(), vec![(0, 0)]);

    job.lend(&mut slots, 1).unwrap();
    let said = mail.recv().await.unwrap();
    job.run_said(said, &mut slots, 1).unwrap();
    assert!(job.writer.pending_verifications().is_empty());
    assert_eq!(job.phase(), RootPhase::Done);
    assert!(checkpoint.put(&job, Vec::new()));
    let delta = checkpoint.take().0.unwrap();
    assert_eq!(delta.nodes.len(), 1, "only the sparse root state changes");
    assert!(delta.members.is_empty() && delta.coverage.is_empty() && delta.parts.is_empty());
}

#[tokio::test]
async fn a_checkpoint_before_verification_cannot_restore_an_unchecked_crc_as_passed() {
    let (mut job, dir, _mail, files) = pending();
    job.tree
        .namespace_mut(NodeId::ROOT)
        .unwrap()
        .get_mut(MemberId(0))
        .unwrap()
        .crc32 = Some(1);
    let checkpoint = Pending::default();
    assert!(checkpoint.put(&job, Vec::new()));
    let snapshot = checkpoint::snapshot(&rows(checkpoint.take().0.unwrap())).unwrap();
    drop(job);
    let mut mail = RunMail::default();
    let mut restored = GraphJob::restore(
        &snapshot,
        &files,
        2,
        Storage {
            working_dir: dir.path().to_owned(),
            accountant: Arc::new(HoldsAccountant::new(weaver_graph::holds::HoldBudgets {
                resident_limit: u64::MAX,
                scratch_total: u64::MAX,
                scratch_ceiling: u64::MAX,
                disk_reserve: 0,
            })),
            free_disk: CapacityReader::unknown(),
            slots: 1,
            mail: mail.sender(),
            passwords: Vec::new(),
            caps: Caps::unbounded(),
            nesting: u32::MAX,
            hardware: Default::default(),
            metrics: None,
        },
        0,
    )
    .unwrap();
    assert!(
        restored.refetched.is_empty(),
        "pending verification does not refetch recorded bytes"
    );
    assert_eq!(restored.job.writer.pending_verifications(), vec![(0, 0)]);
    assert_eq!(restored.job.phase(), RootPhase::Verifying);
    let mut slots = RepairSlots::default();
    restored.job.lend(&mut slots, 1).unwrap();
    let said = mail.recv().await.unwrap();
    restored.job.run_said(said, &mut slots, 1).unwrap();
    assert!(restored.job.writer.pending_verifications().is_empty());
    assert!(
        restored
            .job
            .tree
            .namespace(NodeId::ROOT)
            .unwrap()
            .get(MemberId(0))
            .unwrap()
            .coverage
            .damaged(ByteRange::new(0, SIZE as u64))
    );
}

// The member closed damaged with its checksum owed, the payload is in and
// no set will rule again: nothing can clean it, so the obligation is no
// work to wait on, the census reads it damaged, and the walk drops it.
#[test]
fn an_ineligible_obligation_in_an_ended_namespace_is_not_pending_work() {
    let files = vec![file("payload.bin", &[SIZE as u32])];
    let (mut job, _dir, _) = job_with(&files, Vec::new());
    let bytes = vec![0x71; SIZE];
    let mut damaged = decoded(0, 0, 0, &bytes, Some(SIZE as u64));
    damaged.intact = false;
    let mut slots = RepairSlots::default();
    job.intake(damaged, &mut slots, 1).unwrap();
    let member = job
        .tree
        .namespace_mut(NodeId::ROOT)
        .unwrap()
        .get_mut(MemberId(0))
        .unwrap();
    member.crc32 = Some(crc_turbo::checksum(crc_turbo::CrcAlgorithm::Crc32IsoHdlc, &bytes) as u32);
    assert!(member.closed);
    assert!(member.coverage.damaged(ByteRange::new(0, SIZE as u64)));
    job.writer.restore_verifications(&[(0, 0)]);
    let mail = RunMail::default();
    job.loans = Loans::new(1, mail.sender(), job.runs.hardware());
    assert!(job.writer.ruled(&job.tree, NodeId::ROOT));

    assert!(!job.work_pending());
    assert_ne!(job.phase(), RootPhase::Verifying);
    assert_eq!(
        crate::pipeline::graph::status::drive(&mut job, &mut slots, AskerId(1), 1),
        Step::Deliver
    );
    job.lend(&mut slots, 1).unwrap();
    assert!(!job.awaits_mail(), "nothing is lent to check it");
    assert!(job.writer.pending_verifications().is_empty());
}

#[test]
fn a_queued_checksum_is_verifying_whatever_was_reported_before() {
    for reported in [
        None,
        Some(RootPhase::Downloading),
        Some(RootPhase::Verifying),
        Some(RootPhase::Repairing),
        Some(RootPhase::Extracting),
        Some(RootPhase::Done),
    ] {
        let (mut job, _dir, _mail, _) = pending();
        job.reported = reported;
        assert_eq!(job.phase(), RootPhase::Verifying, "{reported:?}");
        assert_eq!(
            job.phase_change(),
            (reported != Some(RootPhase::Verifying)).then_some(RootPhase::Verifying)
        );
        assert!(!job.repair_out());
        assert!(job.work_pending());
        assert!(
            matches!(
                crate::pipeline::graph::status::drive(
                    &mut job,
                    &mut RepairSlots::default(),
                    AskerId(1),
                    0
                ),
                Step::Wait
            ),
            "{reported:?}: the checksum must be acknowledged before delivery"
        );
    }
}
