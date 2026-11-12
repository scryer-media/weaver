// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The queue against the map-of-work-items queue it replaced, kept as the
// reference: the same seeded operations drive both, and every item either
// hands out, every read and every count must match.

use std::collections::HashSet;
use std::sync::Arc;

use crate::jobs::ids::{JobId, MessageId, NzbFileId, SegmentId};
use crate::jobs::model::{FileSpec, JobSpec, MessageIds, SegmentSpec};
use weaver_model::files::FileRole;

use super::super::reference::ReferenceQueue;
use super::super::*;

// A small deterministic generator, so a failing seed replays exactly.
struct Rng(u64);

impl Rng {
    fn next(&mut self) -> u64 {
        self.0 ^= self.0 << 13;
        self.0 ^= self.0 >> 7;
        self.0 ^= self.0 << 17;
        self.0
    }

    fn below(&mut self, bound: u64) -> u64 {
        self.next() % bound.max(1)
    }

    fn chance(&mut self, percent: u64) -> bool {
        self.below(100) < percent
    }
}

const JOB: JobId = JobId(7);

fn spec(rng: &mut Rng, files: u32) -> JobSpec {
    let files: Vec<FileSpec> = (0..files)
        .map(|file| FileSpec {
            filename: format!("file-{file}.bin"),
            role: FileRole::Standalone,
            groups: vec![format!("alt.binaries.{file}")],
            posted_at_epoch: None,
            segments: (0..1 + rng.below(40) as u32)
                .map(|ordinal| SegmentSpec {
                    ordinal,
                    article_number: ordinal + 1,
                    bytes: 1_000 + rng.below(800_000) as u32,
                    message_ids: MessageIds::new(
                        (0..1 + rng.below(3))
                            .map(|id| format!("f{file}-s{ordinal}-{id}@example.test"))
                            .collect(),
                    ),
                })
                .collect(),
        })
        .collect();
    JobSpec {
        name: "equivalence".to_string(),
        password: None,
        total_bytes: 0,
        category: None,
        metadata: vec![],
        files,
    }
}

fn segment(file_index: u32, segment_number: u32) -> SegmentId {
    SegmentId {
        file_id: NzbFileId {
            job_id: JOB,
            file_index,
        },
        segment_number,
    }
}

// Everything one handed-out item is, for comparison.
type Fields = (
    SegmentId,
    String,
    Vec<String>,
    u32,
    u32,
    u32,
    bool,
    bool,
    Vec<usize>,
    Option<usize>,
    bool,
);

fn fields(work: &DownloadWork) -> Fields {
    (
        work.segment_id,
        work.message_id.0.to_string(),
        work.groups.to_vec(),
        work.priority,
        work.byte_estimate,
        work.retry_count,
        work.is_recovery,
        work.completion_critical,
        work.exclude_servers.clone(),
        work.avoid_server,
        work.counters.is_some(),
    )
}

fn view_fields(
    work: &QueuedWork<'_>,
) -> (
    SegmentId,
    u32,
    u32,
    u32,
    bool,
    bool,
    Vec<usize>,
    Option<usize>,
) {
    (
        work.segment_id,
        work.priority,
        work.byte_estimate,
        work.retry_count,
        work.is_recovery,
        work.completion_critical,
        work.exclude_servers.to_vec(),
        work.avoid_server,
    )
}

fn materialize(item: Option<QueueItem>, spec: &JobSpec) -> Option<Fields> {
    item.map(|item| fields(&item.into_work(spec).expect("the job's own article")))
}

// A predicate both queues can be asked, chosen by the seed: by server,
// by size, by file, by class.
#[derive(Clone, Copy)]
struct Predicate {
    kind: u64,
    server: usize,
    modulus: u32,
}

impl Predicate {
    fn draw(rng: &mut Rng) -> Self {
        Self {
            kind: rng.below(6),
            server: rng.below(3) as usize,
            modulus: 2 + rng.below(4) as u32,
        }
    }

    fn admits(self, work: &QueuedWork<'_>) -> bool {
        match self.kind {
            0 => true,
            1 => {
                !work.exclude_servers.contains(&self.server)
                    && work.avoid_server != Some(self.server)
            }
            2 => !work.byte_estimate.is_multiple_of(self.modulus),
            3 => !work
                .segment_id
                .file_id
                .file_index
                .is_multiple_of(self.modulus),
            4 => !work.is_recovery && work.retry_count < self.modulus,
            _ => work.completion_critical == self.modulus.is_multiple_of(2),
        }
    }
}

struct Pair {
    new: DownloadQueue,
    reference: ReferenceQueue,
}

impl Pair {
    fn new() -> Self {
        Self {
            new: DownloadQueue::new(),
            reference: ReferenceQueue::new(),
        }
    }

    fn assert_same(&self, spec: &JobSpec, step: usize) {
        assert_eq!(
            self.new.snapshot(),
            self.reference.snapshot(),
            "step {step}"
        );
        assert_eq!(self.new.len(), self.reference.len(), "step {step}");
        assert_eq!(self.new.is_empty(), self.reference.is_empty());
        for class in [true, false] {
            assert_eq!(
                self.new.len_in_class(class),
                self.reference.len_in_class(class),
                "step {step}"
            );
            assert_eq!(
                self.new.peek_in_class(class).map(|work| view_fields(&work)),
                self.reference
                    .peek_in_class(class)
                    .map(|work| view_fields(&work.view())),
                "step {step}"
            );
        }
        assert_eq!(
            self.new.excluded_work_count(),
            self.reference.excluded_work_count()
        );
        assert_eq!(
            self.new.recovery_work_count(),
            self.reference.recovery_work_count()
        );
        for file in 0..spec.files.len() as u32 {
            let file_id = segment(file, 0).file_id;
            assert_eq!(
                self.new.queued_count_for_file(file_id),
                self.reference.queued_count_for_file(file_id),
                "step {step}"
            );
        }
        let (mut new_ids, mut reference_ids) = (HashSet::new(), HashSet::new());
        self.new.extend_segment_ids(&mut new_ids);
        self.reference.extend_segment_ids(&mut reference_ids);
        assert_eq!(new_ids, reference_ids);
        let mut first: Vec<_> = self.new.first_articles().collect();
        let mut other: Vec<_> = self.reference.first_articles().collect();
        first.sort_by_key(|id| (id.file_id.file_index, id.segment_number));
        other.sort_by_key(|id| (id.file_id.file_index, id.segment_number));
        assert_eq!(first, other);
        assert!(self.new.counts_match_a_walk(), "step {step}");
        assert!(self.reference.counts_match_a_walk(), "step {step}");
    }
}

// The root's moves, as `apply_moves` makes them: the flags and priority of
// a class.
fn classify(rng: &mut Rng) -> (bool, bool, u32) {
    match rng.below(4) {
        0 => (true, false, 10),
        1 => (true, true, 10),
        2 => (false, true, 10),
        _ => (false, false, 3),
    }
}

fn run(seed: u64, steps: usize) {
    let mut rng = Rng(seed.wrapping_mul(0x9E37_79B9_7F4A_7C15) | 1);
    let files = 1 + rng.below(12) as u32;
    let mut spec = spec(&mut rng, files);
    let groups: Vec<Arc<[String]>> = spec
        .files
        .iter()
        .map(|file| Arc::from(file.groups.as_slice()))
        .collect();
    let mut download = Pair::new();
    let mut recovery = Pair::new();

    // Built as a job's queue is: every article in posted order, some already
    // in hand.
    for (file_index, file) in spec.files.iter().enumerate() {
        let mut reserved = false;
        for seg in &file.segments {
            if rng.chance(10) {
                continue;
            }
            let id = segment(file_index as u32, seg.ordinal);
            if !reserved && rng.chance(50) {
                download
                    .new
                    .reserve_file(id.file_id, file.segments.len() as u32);
                reserved = true;
            }
            download
                .new
                .push_article(id, &groups[file_index], 10, seg.bytes);
            download.reference.push(DownloadWork {
                counters: None,
                segment_id: id,
                message_id: MessageId::new(seg.message_ids.in_use()),
                groups: Arc::clone(&groups[file_index]),
                priority: 10,
                byte_estimate: seg.bytes,
                retry_count: 0,
                is_recovery: false,
                completion_critical: false,
                exclude_servers: vec![],
                avoid_server: None,
            });
        }
    }
    download.assert_same(&spec, 0);

    let mut in_flight: Vec<DownloadWork> = Vec::new();
    let random_segment = |rng: &mut Rng, spec: &JobSpec| {
        let file = rng.below(spec.files.len() as u64) as u32;
        let count = spec.files[file as usize].segments.len() as u64;
        segment(file, rng.below(count) as u32)
    };

    for step in 1..=steps {
        let pair = if rng.chance(25) {
            &mut recovery
        } else {
            &mut download
        };
        let predicate = Predicate::draw(&mut rng);
        let class = rng.chance(50);
        match rng.below(19) {
            0 => {
                let new = materialize(pair.new.pop(), &spec);
                let reference = pair.reference.pop();
                assert_eq!(
                    new,
                    reference.as_ref().map(fields),
                    "seed {seed} step {step}"
                );
                in_flight.extend(reference);
            }
            1..=3 => {
                let new = materialize(
                    pair.new.pop_first_matching(|work| predicate.admits(work)),
                    &spec,
                );
                let reference = pair
                    .reference
                    .pop_first_matching(|work| predicate.admits(&work.view()));
                assert_eq!(
                    new,
                    reference.as_ref().map(fields),
                    "seed {seed} step {step}"
                );
                in_flight.extend(reference);
            }
            4 => {
                let new = materialize(
                    pair.new.pop_next_matching(|work| predicate.admits(work)),
                    &spec,
                );
                let reference = pair
                    .reference
                    .pop_next_matching(|work| predicate.admits(&work.view()));
                assert_eq!(
                    new,
                    reference.as_ref().map(fields),
                    "seed {seed} step {step}"
                );
                in_flight.extend(reference);
            }
            5 => {
                let new = materialize(
                    pair.new
                        .pop_next_matching_in_class(class, |work| predicate.admits(work)),
                    &spec,
                );
                let reference = pair
                    .reference
                    .pop_next_matching_in_class(class, |work| predicate.admits(&work.view()));
                assert_eq!(
                    new,
                    reference.as_ref().map(fields),
                    "seed {seed} step {step}"
                );
                in_flight.extend(reference);
            }
            6 => {
                let new = materialize(
                    pair.new
                        .pop_first_matching_in_class(class, |work| predicate.admits(work)),
                    &spec,
                );
                let reference = pair
                    .reference
                    .pop_first_matching_in_class(class, |work| predicate.admits(&work.view()));
                assert_eq!(
                    new,
                    reference.as_ref().map(fields),
                    "seed {seed} step {step}"
                );
                in_flight.extend(reference);
            }
            7 | 8 => {
                // Work back from dispatch: retried, steered, reclassed.
                if in_flight.is_empty() {
                    continue;
                }
                let index = rng.below(in_flight.len() as u64) as usize;
                let mut work = in_flight.swap_remove(index);
                if rng.chance(50) {
                    work.retry_count += 1;
                }
                if rng.chance(30) {
                    work.exclude_servers.push(rng.below(3) as usize);
                }
                if rng.chance(30) {
                    work.avoid_server = Some(rng.below(3) as usize);
                }
                if rng.chance(20) {
                    (work.completion_critical, work.is_recovery, work.priority) =
                        classify(&mut rng);
                }
                if rng.chance(20) {
                    work.groups = Arc::from(vec!["alt.binaries.other".to_string()]);
                }
                // Rarer shapes: another job's work, a segment number far past
                // its file's run, a second copy of the same article.
                if rng.chance(4) {
                    work.segment_id.file_id.job_id = JobId(8);
                }
                if rng.chance(4) {
                    work.segment_id.segment_number += 1_000_000;
                }
                if rng.chance(6) {
                    pair.new.push(work.clone());
                    pair.reference.push(work.clone());
                }
                pair.new.push(work.clone());
                pair.reference.push(work);
            }
            9 => {
                let id = random_segment(&mut rng, &spec);
                pair.new.note_first_article(id);
                pair.reference.note_first_article(id);
            }
            10 | 11 => {
                // A round of the root's moves: the moved articles leave the
                // queue, take their new class and go back on one.
                let ids: Vec<SegmentId> = (0..1 + rng.below(6))
                    .map(|_| random_segment(&mut rng, &spec))
                    .collect();
                let (critical, is_recovery, priority) = classify(&mut rng);
                let new = pair.new.extract_segments(ids.iter().copied());
                let reference = pair.reference.extract_segments(ids.iter().copied());
                assert_eq!(
                    new.iter()
                        .map(|item| fields(&item.clone().into_work(&spec).unwrap()))
                        .collect::<Vec<_>>(),
                    reference.iter().map(fields).collect::<Vec<_>>(),
                    "seed {seed} step {step}"
                );
                let to_recovery = rng.chance(40);
                let target = if to_recovery {
                    &mut recovery
                } else {
                    &mut download
                };
                for mut item in new {
                    item.completion_critical |= critical;
                    item.is_recovery |= is_recovery;
                    item.priority = priority;
                    target.new.push(item);
                }
                for mut work in reference {
                    work.completion_critical |= critical;
                    work.is_recovery |= is_recovery;
                    work.priority = priority;
                    target.reference.push(work);
                }
            }
            12 => {
                let moves: Vec<(SegmentId, u32, Option<u32>)> = (0..1 + rng.below(6))
                    .map(|_| {
                        let rank = rng.chance(70).then(|| rng.below(5) as u32);
                        (
                            random_segment(&mut rng, &spec),
                            3 + rng.below(2) as u32 * 7,
                            rank,
                        )
                    })
                    .collect();
                assert_eq!(
                    pair.new.reprioritize_segments(moves.iter().copied()),
                    pair.reference.reprioritize_segments(moves.iter().copied())
                );
            }
            13 => {
                let file = segment(rng.below(spec.files.len() as u64) as u32, 0).file_id;
                let rank = rng.below(8) as u32;
                let priority = if rng.chance(80) { 10 } else { 3 };
                assert_eq!(
                    pair.new.rank_ordinary_file(file, priority, rank),
                    pair.reference.rank_ordinary_file(file, priority, rank)
                );
            }
            14 => {
                let pick = move |work: &QueuedWork<'_>| {
                    predicate.admits(work).then_some((
                        3 + work.segment_id.file_id.file_index % 3,
                        (!work.segment_id.segment_number.is_multiple_of(4))
                            .then_some(work.segment_id.segment_number % 5),
                    ))
                };
                assert_eq!(
                    pair.new.reprioritize_matching_with_rank(pick),
                    pair.reference
                        .reprioritize_matching_with_rank(|work| pick(&work.view()))
                );
            }
            15 => {
                if !rng.chance(30) {
                    continue;
                }
                let new = pair.new.extract_matching(|work| predicate.admits(work));
                let reference = pair
                    .reference
                    .extract_matching(|work| predicate.admits(&work.view()));
                assert_eq!(
                    new.into_iter()
                        .map(|item| fields(&item.into_work(&spec).unwrap()))
                        .collect::<Vec<_>>(),
                    reference.iter().map(fields).collect::<Vec<_>>(),
                    "seed {seed} step {step}"
                );
            }
            16 => {
                if rng.chance(70) {
                    assert_eq!(
                        pair.new.clear_rotation_hints(),
                        pair.reference.clear_rotation_hints()
                    );
                } else if rng.chance(30) {
                    pair.new.clear_exclude_servers();
                    pair.reference.clear_exclude_servers();
                }
            }
            17 => {
                assert_eq!(
                    pair.new
                        .peek_first_matching(|work| predicate.admits(work))
                        .map(|work| view_fields(&work)),
                    pair.reference
                        .peek_first_matching(|work| predicate.admits(&work.view()))
                        .map(|work| view_fields(&work.view())),
                );
                assert_eq!(
                    pair.new
                        .peek_next_matching(|work| predicate.admits(work))
                        .map(|work| view_fields(&work)),
                    pair.reference
                        .peek_next_matching(|work| predicate.admits(&work.view()))
                        .map(|work| view_fields(&work.view())),
                );
                assert_eq!(
                    pair.new.count_matching(|work| predicate.admits(work)),
                    pair.reference
                        .count_matching(|work| predicate.admits(&work.view())),
                );
            }
            _ => {
                // The part's id in use is spent while copies of it may be
                // queued: they are still fetched by the id they were queued
                // under.
                let id = random_segment(&mut rng, &spec);
                let part = spec.files[id.file_id.file_index as usize]
                    .segment_mut(id.segment_number)
                    .unwrap();
                if !part.message_ids.has_next() {
                    continue;
                }
                let old = part.message_ids.in_use().to_string();
                for queue in [&mut download.new, &mut recovery.new] {
                    queue.pin_message_id(id, || MessageId::new(&old));
                }
                part.message_ids.spend();
            }
        }
        download.assert_same(&spec, step);
        recovery.assert_same(&spec, step);
    }

    // Whatever is left comes out in the same order.
    for pair in [&mut download, &mut recovery] {
        let new: Vec<Fields> = pair
            .new
            .drain_all()
            .into_iter()
            .map(|item| fields(&item.into_work(&spec).unwrap()))
            .collect();
        let reference: Vec<Fields> = pair.reference.drain_all().iter().map(fields).collect();
        assert_eq!(new, reference, "seed {seed}");
        pair.assert_same(&spec, steps + 1);
    }
}

#[test]
fn lean_queue_serves_the_same_items_in_the_same_order_as_the_reference() {
    for seed in 0..200 {
        run(seed, 600);
    }
}

#[test]
fn lean_queue_matches_the_reference_over_a_long_run() {
    for seed in 1_000..1_004 {
        run(seed, 20_000);
    }
}
