// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::*;
use weaver_server_disk::operations::disk::volume::LoopSite;

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

// The copies a replay staged that are still on disk.
fn staged_copies(job: &Job) -> Vec<std::path::PathBuf> {
    std::fs::read_dir(job.dir.path().join(weaver_graph::names::INTERNAL_DIR))
        .into_iter()
        .flatten()
        .flatten()
        .map(|entry| entry.path())
        .filter(|path| path.extension().is_some_and(|ext| ext == "repaired"))
        .collect()
}

#[test]
fn a_refused_replay_write_stops_reading_and_removes_the_copy_at_once() {
    use crate::pipeline::graph::leaf::counting;
    // What a replay reads through its second chunk, stopped there.
    let (mut whole, _, _, _) = staged_job();
    let replay = whole.writer.take_replay(&whole.tree).unwrap().unwrap();
    let stop = Arc::clone(&replay.stop);
    let mut chunks = 0;
    let (from, _) = counting::so_far();
    let replayed = replay.run_with(&Default::default(), || {
        chunks += 1;
        if chunks == 2 {
            stop.raise();
        }
    });
    let two_chunks = counting::so_far().0 - from;
    assert_eq!(replayed.chunk_lengths(), vec![READ_BACK, READ_BACK]);
    replayed.remove().unwrap();

    // The disk fills after the first chunk: the second is read, its write
    // refused, and nothing after it is read.
    let (mut job, _, node, expected) = staged_job();
    let before = job.tree.record(node).unwrap();
    let replay = job.writer.take_replay(&job.tree).unwrap().unwrap();
    let mut staged = 0;
    let (from, _) = counting::so_far();
    counting::fill_after(Some(READ_BACK));
    let replayed = replay.run_with(&Default::default(), || staged += 1);
    counting::fill_after(None);
    assert_eq!(staged, 1);
    assert_eq!(counting::so_far().0 - from, two_chunks);
    assert!(
        staged_copies(&job).is_empty(),
        "the copy goes before the return is heard"
    );
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
    replay.stop.raise();
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
    let replayed = replay.run_with(&Default::default(), || stop.raise());
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
        .write(LoopSite::LeafWrite, 11, &[0x29], None)
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

// What a hand-over of a leaf's file to a whole-volume archive left.
#[derive(Debug, PartialEq)]
struct HandedOver {
    heard: Vec<(ByteRange, Provenance)>,
    member: Vec<(ByteRange, Provenance)>,
    kept: Vec<(ByteRange, Provenance)>,
    envelope: Vec<u8>,
    leaf_released: bool,
}

// How a hand-over runs: writing every byte over itself as before the
// envelope knew they were there, and on a file system that gives a file a
// second name or one that gives none.
#[derive(Clone, Copy)]
struct HandOver {
    writes_over: bool,
    second_names: bool,
}

// A 7z whose front arrived damaged is a leaf first. A repair rewrites the
// front, the front reads as the archive, and the archive, which keeps its
// volume whole at the volume's own offsets, takes the file as its envelope
// under a second name. What it left, the bytes the volume holds, the
// effects, and the bytes the repair wrote to disk. No real post named: the
// shape stands for a 7z volume whose first article came damaged and that a
// PAR2 set rebuilt.
fn whole_hand_over(how: HandOver) -> (HandedOver, Vec<u8>, String, u64) {
    use crate::pipeline::graph::envelope::in_place::WRITES_OVER;
    use crate::pipeline::graph::leaf::counting;
    use crate::pipeline::graph::namespace::claimed::refused::SECOND_NAMES;

    WRITES_OVER.set(how.writes_over);
    SECOND_NAMES.set(!how.second_names);
    WHOLE_HEARD.with_borrow_mut(Vec::clear);
    let mut front = seven_z().to_vec();
    front.resize(FIRST_BYTES, 0x71);
    let front: &'static [u8] = Box::leak(front.into_boxed_slice());
    let body: Vec<u8> = (0..2 * READ_BACK + 13).map(|at| (at % 251) as u8).collect();
    let volume = [front, body.as_slice()].concat();
    let mut job = Job::new(PLENTY);
    job.whole = true;
    let mut job = job.with_set(Some(Fix {
        name: "archive.7z",
        read: ByteRange::new(0, front.len() as u64),
        fix: (0, front),
    }));
    job.par.lock().unwrap().proof = Some(ByteRange::new(0, front.len() as u64));
    let member = job.member("archive.7z", Some(volume.len() as u64));
    job.send(member, 0, b"broken!", Provenance::Damaged);
    let mut effects = Vec::new();
    job.tree.damage(
        NodeId::ROOT,
        VolumeIndex(member.0),
        ByteRange::new(7, front.len() as u64),
        &mut effects,
    );
    let mut build = job.build();
    job.writer
        .apply_all(&mut job.tree, &mut build, &Source, effects, &mut job.out)
        .unwrap();
    job.send(member, front.len() as u64, &body, Provenance::Fresh);
    assert!(
        job.tree.child_of(NodeId::ROOT, member).is_none(),
        "a leaf first"
    );
    assert_eq!(job.file("archive.7z").len(), volume.len());

    let before = counting::so_far();
    let due = job.tree.recovery_nodes(NodeId::ROOT);
    assert_eq!(job.repair(&mut build, NodeId::ROOT, &due).len(), 1);
    let after = counting::so_far();
    WRITES_OVER.set(false);
    SECOND_NAMES.set(false);

    let (child, _) = job.tree.child_of(NodeId::ROOT, member).expect("claimed");
    let envelope = job.writer.kept_file(child, VolumeIndex(0)).expect("kept");
    let kept = job.writer.envelopes.shelf_of(child).unwrap().lock().kept[&0]
        .coverage
        .runs()
        .map(|run| (run.range, run.provenance))
        .collect();
    let handed = HandedOver {
        heard: WHOLE_HEARD.with_borrow(Clone::clone),
        member: job
            .root()
            .get(member)
            .unwrap()
            .coverage
            .runs()
            .map(|run| (run.range, run.provenance))
            .collect(),
        kept,
        envelope: std::fs::read(envelope).unwrap(),
        leaf_released: !job.writer.files[&NodeId::ROOT].leaves.contains_key(&member),
    };
    (handed, volume, format!("{:?}", job.out), after.1 - before.1)
}

// The archive hears every run of the file with the provenance it arrived
// with, as before, and keeps the same record; only the write of each byte
// over itself is gone.
#[test]
fn a_leaf_handed_to_a_whole_volume_archive_is_not_written_over_itself() {
    let (written_over, volume, effects_over, rewrote) = whole_hand_over(HandOver {
        writes_over: true,
        second_names: true,
    });
    let (in_place, _, effects, wrote) = whole_hand_over(HandOver {
        writes_over: false,
        second_names: true,
    });
    assert_eq!(in_place, written_over);
    assert_eq!(effects, effects_over);
    assert_eq!(in_place.envelope, volume);
    assert!(in_place.leaf_released);
    assert!(in_place.heard.len() >= 3, "{:?}", in_place.heard);
    assert!(
        in_place
            .heard
            .iter()
            .all(|(range, _)| range.len() <= READ_BACK)
    );
    assert_eq!(
        in_place
            .heard
            .iter()
            .map(|(range, _)| range.len())
            .sum::<u64>(),
        volume.len() as u64
    );
    assert!(
        in_place
            .heard
            .contains(&(ByteRange::new(0, FIRST_BYTES as u64), Provenance::Repaired)),
        "{:?}",
        in_place.heard
    );
    // The replay wrote every byte of the volume over itself before, and
    // now writes none of it: what is still written is the repair's front,
    // staged once and put in the file once.
    assert_eq!(rewrote - wrote, volume.len() as u64);
    assert_eq!(wrote, 2 * FIRST_BYTES as u64);
}

// A file system that gives a file no second name leaves the envelope a file
// of its own: every byte of the volume goes down to it and is written, as
// it was before, and the envelope holds the volume.
#[test]
fn a_hand_over_with_no_second_name_writes_every_byte() {
    let (written_over, volume, _, rewrote) = whole_hand_over(HandOver {
        writes_over: true,
        second_names: false,
    });
    let (unlinked, _, _, wrote) = whole_hand_over(HandOver {
        writes_over: false,
        second_names: false,
    });
    assert_eq!(unlinked, written_over);
    assert_eq!(unlinked.envelope, volume);
    assert!(unlinked.leaf_released);
    assert_eq!(wrote, rewrote);
    assert!(wrote >= volume.len() as u64, "{wrote} of {}", volume.len());
}

// The file a test's leaf over `name` in `dir` is, as the file system tells.
#[cfg(unix)]
fn file_of(dir: &Path, name: &str) -> crate::pipeline::graph::leaf::FileId {
    Leaf::over(dir, name, true)
        .file_id(LoopSite::LeafWrite)
        .expect("a unix file system")
}

// Only bytes read from the very file an envelope began in, landing at the
// offset they were read from, skip the write.
#[cfg(unix)]
#[test]
fn only_bytes_read_from_the_envelope_s_own_file_at_their_own_offset_skip_the_write() {
    use crate::pipeline::graph::envelope::{Envelopes, InPlace};
    use crate::pipeline::graph::leaf::counting;

    let dir = tempfile::tempdir().unwrap();
    std::fs::write(dir.path().join("volume"), b"abcdef").unwrap();
    std::fs::write(dir.path().join("other"), b"ABCDEF").unwrap();
    let mut envelopes = Envelopes::new(dir.path().to_path_buf());
    let node = NodeId(3);
    assert!(envelopes.start_in(node, 0, "volume", true));
    let whole = [Placement::Envelope {
        envelope: 0,
        range: ByteRange::new(0, 6),
    }];
    let written = |envelopes: &mut Envelopes,
                   placements: &[Placement],
                   bytes: &[u8],
                   in_place: Option<InPlace>| {
        let before = counting::so_far().1;
        envelopes
            .capture(node, placements, bytes, Provenance::Fresh, in_place)
            .unwrap();
        counting::so_far().1 - before
    };
    let own = Some(InPlace {
        file: file_of(dir.path(), "volume"),
        offset: 0,
    });
    assert_eq!(written(&mut envelopes, &whole, b"abcdef", own), 0);
    assert!(envelopes.keeps(node, 0, ByteRange::new(0, 6)));
    // Another file's bytes, bytes from elsewhere, and bytes read at another
    // offset are written.
    let other = Some(InPlace {
        file: file_of(dir.path(), "other"),
        offset: 0,
    });
    assert_eq!(written(&mut envelopes, &whole, b"ABCDEF", other), 6);
    assert_eq!(written(&mut envelopes, &whole, b"abcdef", None), 6);
    let moved = own.map(|read| InPlace { offset: 10, ..read });
    assert_eq!(written(&mut envelopes, &whole, b"abcdef", moved), 6);
    // An envelope begun in a file of its own is written whatever it is told.
    let fresh = [Placement::Envelope {
        envelope: 1,
        range: ByteRange::new(0, 6),
    }];
    assert_eq!(written(&mut envelopes, &fresh, b"abcdef", own), 6);
    assert_eq!(std::fs::read(dir.path().join("volume")).unwrap(), b"abcdef");
}

// Where the file system gives no second name, the envelope begins in the
// empty file made at the name instead: that is another file than the one
// the bytes were read from, so every byte is written, and the envelope
// holds the volume.
#[cfg(unix)]
#[test]
fn an_envelope_begun_in_the_empty_file_a_refused_second_name_left_is_written() {
    use crate::pipeline::graph::envelope::{Envelopes, InPlace};
    use crate::pipeline::graph::leaf::counting;
    use crate::pipeline::graph::namespace::claimed::{refused::SECOND_NAMES, second_name_or_empty};

    let dir = tempfile::tempdir().unwrap();
    let from = dir.path().join("archive.7z");
    std::fs::write(&from, b"abcdef").unwrap();
    SECOND_NAMES.set(true);
    second_name_or_empty(&from, &dir.path().join("envelope"));
    SECOND_NAMES.set(false);
    let mut envelopes = Envelopes::new(dir.path().to_path_buf());
    let node = NodeId(3);
    assert!(envelopes.start_in(node, 0, "envelope", true));
    let whole = [Placement::Envelope {
        envelope: 0,
        range: ByteRange::new(0, 6),
    }];
    let read = Some(InPlace {
        file: file_of(dir.path(), "archive.7z"),
        offset: 0,
    });
    let before = counting::so_far().1;
    envelopes
        .capture(node, &whole, b"abcdef", Provenance::Fresh, read)
        .unwrap();
    assert_eq!(counting::so_far().1 - before, 6);
    assert_eq!(
        std::fs::read(dir.path().join("envelope")).unwrap(),
        b"abcdef"
    );
    assert_eq!(std::fs::read(&from).unwrap(), b"abcdef");
}

// A member's file made anew after the envelope took a second name of it, as
// a move that copies and a repair that rewrites it would leave it, is
// another file: bytes read from it are written to the envelope, which then
// holds them, not the bytes the old file held.
#[cfg(unix)]
#[test]
fn bytes_read_from_a_file_made_anew_after_the_hand_over_are_written() {
    use crate::pipeline::graph::envelope::{Envelopes, InPlace};
    use crate::pipeline::graph::leaf::counting;

    let dir = tempfile::tempdir().unwrap();
    let from = dir.path().join("archive.7z");
    std::fs::write(&from, b"broken").unwrap();
    std::fs::hard_link(&from, dir.path().join("envelope")).unwrap();
    let mut envelopes = Envelopes::new(dir.path().to_path_buf());
    let node = NodeId(3);
    assert!(envelopes.start_in(node, 0, "envelope", true));
    let shared = file_of(dir.path(), "archive.7z");
    assert_eq!(file_of(dir.path(), "envelope"), shared);
    // The file goes and comes back at its name holding the repaired bytes.
    std::fs::remove_file(&from).unwrap();
    std::fs::write(&from, b"abcdef").unwrap();
    let mut bytes = Vec::new();
    let member = Leaf::over(dir.path(), "archive.7z", true);
    member.read(LoopSite::LeafWrite, 0, 6, &mut bytes).unwrap();
    let read = Some(InPlace {
        file: member.file_id(LoopSite::LeafWrite).unwrap(),
        offset: 0,
    });
    assert_ne!(read.map(|read| read.file), Some(shared));
    let whole = [Placement::Envelope {
        envelope: 0,
        range: ByteRange::new(0, 6),
    }];
    let before = counting::so_far().1;
    envelopes
        .capture(node, &whole, &bytes, Provenance::Fresh, read)
        .unwrap();
    assert_eq!(counting::so_far().1 - before, 6);
    assert_eq!(
        std::fs::read(dir.path().join("envelope")).unwrap(),
        b"abcdef"
    );
}
