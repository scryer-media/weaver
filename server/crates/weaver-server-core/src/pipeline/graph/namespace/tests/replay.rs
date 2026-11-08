// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::*;
use std::sync::atomic::Ordering;

const REPAIRED_FRONT: &[u8] = &{
    let mut bytes = [0x71; FIRST_BYTES];
    let mut at = 0;
    while at < SIGNATURE.len() {
        bytes[at] = SIGNATURE[at];
        at += 1;
    }
    bytes
};
const REPLAY_BODY: &[u8] = b"abcdefghijklmnopqrst";

fn repaired_header_claim(provenance: Provenance) -> (Job, MemberId, Vec<(ByteRange, Provenance)>) {
    let repaired = REPAIRED_FRONT.len() as u64;
    let mut job = Job::new(PLENTY).with_set(Some(Fix {
        name: "archive.rar",
        read: ByteRange::new(0, REPAIRED_FRONT.len() as u64 + 25),
        fix: (0, REPAIRED_FRONT),
    }));
    job.par.lock().unwrap().proof = Some(ByteRange::new(0, repaired));
    job.inner = repaired - HEADER + 25;
    let member = job.member("archive.rar", Some(HEADER + job.inner));
    job.send(member, 0, b"broken!", Provenance::Damaged);
    job.send(member, repaired + 20, b"tail!", Provenance::Fresh);
    let mut effects = Vec::new();
    job.tree.damage(
        NodeId::ROOT,
        VolumeIndex(member.0),
        ByteRange::new(HEADER, repaired + 20),
        &mut effects,
    );
    let mut build = job.build();
    job.writer
        .apply_all(&mut job.tree, &mut build, &Source, effects, &mut job.out)
        .unwrap();
    assert_eq!(job.file("archive.rar").len() as u64, repaired + 25);
    if provenance.arrived() {
        job.send(member, repaired, REPLAY_BODY, provenance);
    }

    STORE_DELIVERIES.with_borrow_mut(|seen| *seen = Some(Vec::new()));
    let due = job.tree.recovery_nodes(NodeId::ROOT);
    assert_eq!(job.repair(&mut build, NodeId::ROOT, &due).len(), 1);
    let deliveries = STORE_DELIVERIES.with_borrow_mut(Option::take).unwrap();
    assert!(job.tree.child_of(NodeId::ROOT, member).is_some());
    (job, member, deliveries)
}

#[test]
fn a_repaired_header_claim_replays_arrived_ranges_around_sparse_loss() {
    let (mut job, member, deliveries) = repaired_header_claim(Provenance::Lost);
    let start = REPAIRED_FRONT.len() as u64;
    let lost = ByteRange::new(start, start + 20);
    assert!(
        deliveries
            .iter()
            .all(|(range, _)| range.intersect(lost).is_none()),
        "a sparse hole is not ciphertext: {deliveries:?}",
    );
    assert!(deliveries.contains(&(ByteRange::new(start + 20, start + 25), Provenance::Fresh)));
    assert_eq!(
        job.root()
            .get(member)
            .unwrap()
            .coverage
            .overlapping(lost)
            .next()
            .unwrap()
            .provenance,
        Provenance::Lost
    );

    job.send(member, start + 5, &REPLAY_BODY[5..10], Provenance::Repaired);
    let remaining: Vec<_> = job
        .root()
        .get(member)
        .unwrap()
        .coverage
        .overlapping(lost)
        .map(|run| (run.range, run.provenance))
        .collect();
    assert_eq!(
        remaining,
        [
            (ByteRange::new(start, start + 5), Provenance::Lost),
            (ByteRange::new(start + 5, start + 10), Provenance::Repaired),
            (ByteRange::new(start + 10, start + 20), Provenance::Lost),
        ]
    );
    job.send(member, start, REPLAY_BODY, Provenance::Repaired);
    let expected = [&REPAIRED_FRONT[HEADER as usize..], REPLAY_BODY, b"tail!"].concat();
    assert_eq!(
        blake3::hash(&job.file("inner.bin")),
        blake3::hash(&expected)
    );
}

#[test]
fn a_repaired_header_claim_preserves_arrived_corrupt_bytes_and_their_provenance() {
    for provenance in [Provenance::Damaged, Provenance::Refuted] {
        let (job, member, deliveries) = repaired_header_claim(provenance);
        let start = REPAIRED_FRONT.len() as u64;
        assert!(
            deliveries.contains(&(ByteRange::new(start, start + 20), provenance)),
            "{deliveries:?}"
        );
        let expected = [&REPAIRED_FRONT[HEADER as usize..], REPLAY_BODY, b"tail!"].concat();
        assert_eq!(job.file("inner.bin"), expected);
        let (node, _) = job.tree.child_of(NodeId::ROOT, member).unwrap();
        assert_eq!(
            job.tree
                .namespace(node)
                .unwrap()
                .get(MemberId(0))
                .unwrap()
                .coverage
                .overlapping(ByteRange::new(start - HEADER, start + 20 - HEADER))
                .next()
                .unwrap()
                .provenance,
            provenance
        );
    }
}

fn staged_job() -> (Job, MemberId, NodeId, Vec<u8>) {
    let mut job = Job::new(PLENTY);
    let payload = vec![0x71; (2 * READ_BACK + 13) as usize];
    job.inner = payload.len() as u64;
    let mut posted = SIGNATURE.to_vec();
    posted.extend_from_slice(&payload);
    let member = job.member("archive.rar", Some(posted.len() as u64));
    job.send(member, 0, &posted, Provenance::Fresh);
    job.commit();
    let (node, _) = job.tree.child_of(NodeId::ROOT, member).unwrap();
    job.writer.queue_rechecks(vec![(
        node,
        vec![(VolumeIndex(0), ByteRange::new(0, posted.len() as u64))],
    )]);
    (job, member, node, payload)
}

fn apply(job: &mut Job, replayed: Replayed) {
    let mut build = job.build();
    job.writer
        .replayed(&mut job.tree, &mut build, &Source, replayed, &mut job.out)
        .unwrap();
}

#[test]
fn a_checksum_revealed_by_repair_replays_preexisting_damage_after_the_map_is_complete() {
    late_checksum_replay(true);
}

#[test]
fn a_checksum_revealed_by_clean_proof_replays_preexisting_damage() {
    late_checksum_replay(false);
}

fn late_checksum_replay(physical: bool) {
    for revealed in [false, true] {
        let posted = [SIGNATURE, b"hello"].concat();
        let mut job = Job::new(PLENTY).with_set(physical.then_some(Fix {
            name: "archive.rar",
            read: ByteRange::new(0, posted.len() as u64),
            fix: (HEADER + 2, b"l"),
        }));
        job.checksum_on_repair = revealed.then(|| crc32(b"hello"));
        let member = job.member("archive.rar", Some(posted.len() as u64));
        job.send(member, 0, &posted, Provenance::Fresh);
        job.close(member);
        let (node, _) = job.tree.child_of(NodeId::ROOT, member).unwrap();
        let derived = job
            .tree
            .namespace_mut(node)
            .unwrap()
            .get_mut(MemberId(0))
            .unwrap();
        derived.closed = true;
        derived
            .coverage
            .write(ByteRange::new(0, 5), Provenance::Damaged);
        assert_eq!(
            derived.crc32, None,
            "the missing metadata has not stated a checksum"
        );
        assert!(job.tree.rejected_inputs(NodeId::ROOT, [member]).is_empty());
        if !physical {
            job.tree
                .namespace_mut(NodeId::ROOT)
                .unwrap()
                .get_mut(member)
                .unwrap()
                .coverage
                .write(ByteRange::new(HEADER + 2, HEADER + 3), Provenance::Damaged);
        }
        job.commit();
        let set = job.writer.sets(&job.tree)[0].1;
        let mut lent = Inline::default();
        job.writer
            .repair(&mut job.tree, NodeId::ROOT, &[set], &mut lent)
            .unwrap();
        let mut build = job.build();
        assert_eq!(
            job.run_lent(&mut build, lent),
            vec![(
                set,
                if physical {
                    RepairOutcome::Repaired { slices: 1 }
                } else {
                    RepairOutcome::Clean
                }
            )]
        );
        assert_eq!(job.writer.rechecks_waiting(), revealed);
        if revealed {
            let replay = job.writer.take_replay(&job.tree).unwrap().unwrap();
            apply(&mut job, replay.run(&Default::default()));
            assert!(
                job.tree
                    .namespace(node)
                    .unwrap()
                    .get(MemberId(0))
                    .unwrap()
                    .coverage
                    .verified(ByteRange::new(0, 5))
            );
            assert_eq!(job.file("inner.bin"), b"hello");
        }
    }
}

#[test]
fn clean_proof_preserves_full_replay_only_for_already_rejected_derived_output() {
    for rejected in [false, true] {
        let mut job = Job::new(PLENTY).with_set(None);
        job.inner = 5;
        let posted = [SIGNATURE, b"hello"].concat();
        let member = job.member("archive.rar", Some(posted.len() as u64));
        job.send(member, 0, &posted, Provenance::Fresh);
        job.close(member);
        let (node, _) = job.tree.child_of(NodeId::ROOT, member).unwrap();
        let derived = job
            .tree
            .namespace_mut(node)
            .unwrap()
            .get_mut(MemberId(0))
            .unwrap();
        derived.crc32 = Some(crc32(b"hello"));
        derived.closed = true;
        if rejected {
            derived
                .coverage
                .write(ByteRange::new(0, 5), Provenance::Damaged);
        }
        // Current bytes are intact. Only a subset of their prior damage
        // is promoted by this Clean proof, which must not erase the full
        // dependency of an already rejected derived checksum.
        job.tree
            .namespace_mut(NodeId::ROOT)
            .unwrap()
            .get_mut(member)
            .unwrap()
            .coverage
            .write(ByteRange::new(HEADER + 1, HEADER + 2), Provenance::Damaged);
        job.commit();
        let set = job.writer.sets(&job.tree)[0].1;
        let mut lent = Inline::default();
        job.writer
            .repair(&mut job.tree, NodeId::ROOT, &[set], &mut lent)
            .unwrap();
        let mut build = job.build();
        assert_eq!(
            job.run_lent(&mut build, lent),
            vec![(set, RepairOutcome::Clean)]
        );
        assert_eq!(job.writer.rechecks_waiting(), rejected);
        if rejected {
            let replay = job.writer.take_replay(&job.tree).unwrap().unwrap();
            apply(&mut job, replay.run(&Default::default()));
            assert!(
                job.tree
                    .namespace(node)
                    .unwrap()
                    .get(MemberId(0))
                    .unwrap()
                    .coverage
                    .verified(ByteRange::new(0, 5))
            );
            assert_eq!(job.file("inner.bin"), b"hello");
        }
    }
}

#[test]
fn replay_stages_bounded_chunks_before_invalidating_coverage() {
    let (mut job, _, node, expected) = staged_job();
    let before = job.tree.record(node).unwrap();
    let replay = job.writer.take_replay(&job.tree).unwrap().unwrap();
    let replayed = replay.run(&Default::default());
    assert_eq!(
        job.tree.record(node).unwrap(),
        before,
        "the worker does not invalidate the live graph"
    );
    let chunks = replayed.chunk_lengths();
    assert!(chunks.len() >= 3);
    assert!(chunks.iter().all(|length| *length <= READ_BACK));
    assert_eq!(chunks.iter().sum::<u64>(), HEADER + expected.len() as u64);
    apply(&mut job, replayed);
    assert_eq!(job.file("inner.bin"), expected);
    assert!(!job.writer.rechecks_waiting());
}

#[test]
fn changed_inputs_discard_a_staged_replay_before_uncovering() {
    let (mut job, member, node, mut expected) = staged_job();
    let replay = job.writer.take_replay(&job.tree).unwrap().unwrap();
    let replayed = replay.run(&Default::default());
    job.send(member, HEADER + 19, &[0x42], Provenance::Repaired);
    expected[19] = 0x42;
    let before = job.tree.record(node).unwrap();
    apply(&mut job, replayed);
    assert_eq!(job.tree.record(node).unwrap(), before);
    assert_eq!(job.file("inner.bin"), expected);
    assert!(job.writer.rechecks_waiting());
    let replay = job.writer.take_replay(&job.tree).unwrap().unwrap();
    apply(&mut job, replay.run(&Default::default()));
    assert_eq!(job.file("inner.bin"), expected);
}

#[test]
fn cancelled_replay_keeps_original_coverage_and_backing() {
    let (mut job, _, node, expected) = staged_job();
    let before = job.tree.record(node).unwrap();
    let replay = job.writer.take_replay(&job.tree).unwrap().unwrap();
    replay.stop.store(true, Ordering::Relaxed);
    let replayed = replay.run(&Default::default());
    assert!(replayed.chunk_lengths().is_empty());
    apply(&mut job, replayed);
    assert_eq!(job.tree.record(node).unwrap(), before);
    assert_eq!(job.file("inner.bin"), expected);
}

#[test]
fn cancellation_between_replay_chunks_discards_the_whole_staging() {
    let (mut job, _, node, expected) = staged_job();
    let before = job.tree.record(node).unwrap();
    let replay = job.writer.take_replay(&job.tree).unwrap().unwrap();
    let stop = Arc::clone(&replay.stop);
    let replayed = replay.run_with(&Default::default(), || stop.store(true, Ordering::Relaxed));
    assert_eq!(replayed.chunk_lengths(), vec![READ_BACK]);
    apply(&mut job, replayed);
    assert_eq!(job.tree.record(node).unwrap(), before);
    assert_eq!(job.file("inner.bin"), expected);
}

#[test]
fn replay_worker_panic_returns_failure_without_invalidating_input() {
    let (mut job, _, node, expected) = staged_job();
    let before = job.tree.record(node).unwrap();
    let replay = job.writer.take_replay(&job.tree).unwrap().unwrap();
    let replayed = replay.run_with(&Default::default(), || panic!("injected readback failure"));
    let mut build = job.build();
    assert!(
        job.writer
            .replayed(&mut job.tree, &mut build, &Source, replayed, &mut job.out)
            .is_err()
    );
    assert_eq!(job.tree.record(node).unwrap(), before);
    assert_eq!(job.file("inner.bin"), expected);
}

#[test]
fn same_path_backing_changes_invalidate_replay_even_without_tree_edits() {
    let (mut job, _, node, mut expected) = staged_job();
    let replay = job.writer.take_replay(&job.tree).unwrap().unwrap();
    let replayed = replay.run(&Default::default());
    let before = job.tree.generation();
    job.writer
        .files_mut(node)
        .leaves
        .get_mut(&MemberId(0))
        .unwrap()
        .write(11, &[0x29], None)
        .unwrap();
    expected[11] = 0x29;
    assert_eq!(job.tree.generation(), before);
    apply(&mut job, replayed);
    assert!(job.writer.rechecks_waiting());
    assert_eq!(job.file("inner.bin"), expected);
}

#[test]
fn unrelated_root_file_progress_does_not_repeat_staged_replay() {
    let (mut job, _, _, expected) = staged_job();
    let replay = job.writer.take_replay(&job.tree).unwrap().unwrap();
    let replayed = replay.run(&Default::default());
    let unrelated = job.member("other.bin", Some(5));
    job.send(unrelated, 0, b"other", Provenance::Fresh);
    apply(&mut job, replayed);
    assert!(!job.writer.rechecks_waiting());
    assert_eq!(job.file("inner.bin"), expected);
    assert_eq!(job.file("other.bin"), b"other");
}

#[test]
fn changed_input_verdict_discards_replay_without_a_backing_write() {
    let (mut job, member, node, expected) = staged_job();
    let replay = job.writer.take_replay(&job.tree).unwrap().unwrap();
    let replayed = replay.run(&Default::default());
    let before = job.tree.record(node).unwrap();
    job.tree.damage(
        NodeId::ROOT,
        VolumeIndex(member.0),
        ByteRange::new(0, HEADER),
        &mut Vec::new(),
    );
    assert_eq!(
        job.tree.record(node).unwrap(),
        before,
        "only the source envelope's verdict changed"
    );
    apply(&mut job, replayed);
    assert!(job.writer.rechecks_waiting());
    assert_eq!(job.tree.record(node).unwrap(), before);
    assert_eq!(job.file("inner.bin"), expected);
}
