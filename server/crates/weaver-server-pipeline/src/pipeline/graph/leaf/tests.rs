// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

use super::Leaf;

// The record of changed bytes kept before its stretches were disjoint: by
// start and revision, to the end, with an entry dropped only when a later
// one held it whole, and every entry from the start scanned for a query.
#[derive(Default)]
struct Before {
    changed: BTreeMap<(u64, u64), u64>,
}

impl Before {
    fn change(&mut self, revision: u64, start: u64, end: u64) {
        let within: Vec<(u64, u64)> = self
            .changed
            .range((start, 0)..(end, 0))
            .filter(|(_, to)| **to <= end)
            .map(|(key, _)| *key)
            .collect();
        for key in within {
            self.changed.remove(&key);
        }
        self.changed.insert((start, revision), end);
    }

    fn changed_since(&self, seen: u64, start: u64, end: u64) -> bool {
        self.changed
            .range(..(end, 0))
            .any(|(&(from, revision), &to)| revision > seen && to > start && from < end)
    }
}

fn leaf() -> Leaf {
    Leaf::over(Path::new("/nonexistent"), "member.bin", false)
}

struct Seeded(u64);

impl Seeded {
    fn next(&mut self, bound: u64) -> u64 {
        self.0 = self
            .0
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        (self.0 >> 33) % bound
    }
}

// A seeded run of changes as a leaf makes them: each revision changes one
// stretch or several (what one write replaced in each run it overlapped),
// overlapping, nested, straddling and repeated, with a start over now and
// then, which changes everything.
fn changes(seed: u64, count: usize) -> Vec<Vec<(u64, u64)>> {
    const LENGTH: u64 = 2048;
    let mut seeded = Seeded(seed);
    (0..count)
        .map(|_| {
            if seeded.next(40) == 0 {
                return vec![(0, u64::MAX)];
            }
            let mut at = seeded.next(LENGTH);
            (0..1 + seeded.next(3))
                .map(|_| {
                    let start = at;
                    let end = start + 1 + seeded.next(200);
                    at = end + seeded.next(3);
                    (start, end)
                })
                .collect()
        })
        .collect()
}

// Points at, before and after every stretch's ends.
fn points(leaf: &Leaf) -> Vec<u64> {
    let mut points = BTreeSet::from([0, 1, u64::MAX - 1, u64::MAX]);
    for (&from, &(_, to)) in &leaf.changed {
        for point in [from, to] {
            points.extend([point.saturating_sub(1), point, point.saturating_add(1)]);
        }
    }
    points.into_iter().collect()
}

// After every change, every query of some bytes asked of every revision gets
// the answer the scan over the old record gave.
#[test]
fn disjoint_stretches_answer_as_the_old_scan_did() {
    for seed in [1, 2, 3] {
        let mut leaf = leaf();
        let mut before = Before::default();
        let mut seeded = Seeded(seed ^ 0x5eed);
        for (at, stretches) in changes(seed, 400).into_iter().enumerate() {
            leaf.revision += 1;
            for &(start, end) in &stretches {
                leaf.change(true, start, end);
                before.change(leaf.revision, start, end);
            }
            let points = points(&leaf);
            for seen in 0..=leaf.revision {
                for _ in 0..8 {
                    let start = points[seeded.next(points.len() as u64) as usize];
                    let end = points[seeded.next(points.len() as u64) as usize];
                    let (start, end) = (start.min(end), start.max(end));
                    // Only ranges of some bytes are compared.
                    if start == end {
                        continue;
                    }
                    assert_eq!(
                        leaf.changed_since((leaf.identity, seen), start, end, true),
                        before.changed_since(seen, start, end),
                        "seed {seed}, change {at}, seen {seen}, {start}..{end}"
                    );
                }
            }
            assert!(leaf.changed_since((leaf.identity + 1, leaf.revision), 0, 1, true));
        }
    }
}

// A write over written bytes, a write of bytes already there, a close that
// cuts and a start over, on a real file, with each told whether to keep what
// it changes. The stretches kept after each step.
fn written_over(record: bool) -> Vec<Vec<(u64, (u64, u64))>> {
    use weaver_server_disk::operations::disk::volume::LoopSite;
    let dir = tempfile::tempdir().unwrap();
    let mut leaf = Leaf::at(dir.path(), "member.bin");
    let mut kept = Vec::new();
    let mut step = |leaf: &Leaf| kept.push(leaf.changed.iter().map(|(a, b)| (*a, *b)).collect());
    leaf.write(LoopSite::LeafWrite, 0, &[1; 100], None, record)
        .unwrap();
    step(&leaf);
    leaf.write(LoopSite::LeafWrite, 10, &[2; 10], None, record)
        .unwrap();
    step(&leaf);
    leaf.wrote_in_place(30, 10, record);
    step(&leaf);
    leaf.close(LoopSite::LeafWrite, 50, record).unwrap();
    step(&leaf);
    leaf.truncate(record).unwrap();
    step(&leaf);
    kept
}

// Told no repair's read is left to check, writes that replace bytes keep
// nothing.
#[test]
fn writes_keep_no_change_while_no_read_is_checked() {
    assert!(written_over(false).iter().all(Vec::is_empty));
}

// Told a read may be checked, each replaces as it did before: what it
// replaced, at its revision.
#[test]
fn writes_keep_what_they_replace_while_a_read_is_checked() {
    assert_eq!(
        written_over(true),
        vec![
            vec![],
            vec![(10, (2, 20))],
            vec![(10, (2, 20)), (30, (3, 40))],
            vec![(10, (2, 20)), (30, (3, 40)), (50, (4, 100))],
            vec![(0, (5, u64::MAX))],
        ]
    );
}

// The stretches stay disjoint and in order, a start over leaves one, and a
// query reads only the stretches its range spans and the one before them.
#[test]
fn stretches_stay_disjoint_and_a_query_reads_only_what_it_spans() {
    let mut leaf = leaf();
    let mut seeded = Seeded(9);
    for stretches in changes(4, 600) {
        leaf.revision += 1;
        for &(start, end) in &stretches {
            leaf.change(true, start, end);
        }
        if stretches == [(0, u64::MAX)] {
            assert_eq!(leaf.changed.len(), 1);
            assert_eq!(leaf.changed.get(&0), Some(&(leaf.revision, u64::MAX)));
        }
        let mut reach = 0;
        for (&from, &(revision, to)) in &leaf.changed {
            assert!(from >= reach && from < to, "{from}..{to} after {reach}");
            assert!(revision <= leaf.revision);
            reach = to;
        }
        for _ in 0..16 {
            let start = seeded.next(2300);
            let end = start + seeded.next(300);
            let spanned: Vec<_> = leaf.spanned(start, end).collect();
            let overlapping = leaf
                .changed
                .iter()
                .filter(|(from, (_, to))| *to > start && **from < end)
                .count();
            let before = usize::from(spanned.first().is_some_and(|(_, (_, to))| *to <= start));
            assert_eq!(spanned.len(), overlapping + before, "{start}..{end}");
        }
    }
}
