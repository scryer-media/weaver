// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Which free described file an unnamed member is when nothing of its content
// says: the file it is by elimination, once nothing still to come can be it.

use std::collections::{BTreeMap, BTreeSet, VecDeque};

use weaver_graph::MemberId;

// What is known of an unnamed member's length. A member some byte of which
// decoded knows its length exactly; one none of whose did is only bounded
// by its post's encoded size.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Length {
    Exact(u64),
    AtMost(u64),
}

impl Length {
    // Whether the member could be a described file of `long`.
    pub(crate) fn fits(self, long: u64) -> bool {
        match self {
            Self::Exact(length) => length == long,
            Self::AtMost(bound) => long <= bound,
        }
    }

    pub(crate) fn bound(self) -> u64 {
        match self {
            Self::Exact(length) | Self::AtMost(length) => length,
        }
    }
}

// A file is a member's by elimination only when it is what is left: the
// free files the members that fit it could be are no more than they are. A
// file over is one a member still to come may be. The longest such file is
// taken, the first of its length in the order given, by the member that fits
// it most tightly, the earliest of those still unnamed; each file taken
// leaves less for the rest, so the files are tried again until none is.
//
// Whether a file is what is left depends only on its length. A member known
// exactly fits its own length; one only bounded fits every length up to its
// bound, so the members only bounded together reach every length up to the
// longest bound. A length within that reach could be any free file within
// it; a length past it could be only the free files of its own length. Each
// length's margin, the members fitting it less the files it could be, is
// kept in a tree over the lengths, and a file taken changes the margins by a
// few ranges: the longest length with a margin of none or more is the next
// file to take. `work` counts the steps through the tree.
pub(crate) fn by_elimination<F: Clone>(
    free: &[(F, u64)],
    mut unnamed: Vec<(MemberId, Length)>,
    work: &mut u64,
) -> Vec<(F, MemberId)> {
    let mut known = Vec::new();
    if free.is_empty() || unnamed.is_empty() {
        return known;
    }
    let mut by_length: BTreeMap<u64, VecDeque<F>> = BTreeMap::new();
    for (file, long) in free {
        by_length.entry(*long).or_default().push_back(file.clone());
    }
    let lengths: Vec<u64> = by_length.keys().copied().collect();
    let mut files: Vec<VecDeque<F>> = by_length.into_values().collect();
    let mut left: Vec<i64> = files.iter().map(|files| files.len() as i64).collect();
    // The lengths at or under `long`, as a count of the shortest.
    let upto = |long: u64| lengths.partition_point(|length| *length <= long);
    let mut exact: BTreeMap<u64, BTreeSet<usize>> = BTreeMap::new();
    let mut bounded: BTreeSet<(u64, usize)> = BTreeSet::new();
    for (at, (_, length)) in unnamed.iter().enumerate() {
        match *length {
            Length::Exact(long) => {
                exact.entry(long).or_default().insert(at);
            }
            Length::AtMost(bound) => {
                bounded.insert((bound, at));
            }
        }
    }
    let mut reach = bounded.last().map(|(bound, _)| *bound);
    let mut within = reach.map_or(0, upto);
    let mut could_be: i64 = left[..within].iter().sum();
    let bounds: Vec<u64> = bounded.iter().map(|(bound, _)| *bound).collect();
    let margins: Vec<i64> = lengths
        .iter()
        .enumerate()
        .map(|(at, long)| {
            let fitting = exact.get(long).map_or(0, BTreeSet::len)
                + (bounds.len() - bounds.partition_point(|bound| bound < long));
            fitting as i64 - if at < within { could_be } else { left[at] }
        })
        .collect();
    let mut tree = Margins::new(&margins);
    while let Some(at) = tree.last_ready() {
        let long = lengths[at];
        let Some(file) = files[at].pop_front() else {
            break;
        };
        let tightest = [
            exact
                .get(&long)
                .and_then(|members| members.first())
                .copied(),
            bounded.range((long, 0)..).next().map(|(_, member)| *member),
        ]
        .into_iter()
        .flatten()
        .min_by_key(|member| (unnamed[*member].1.bound(), *member));
        let Some(member) = tightest else {
            break;
        };
        // The file taken is one fewer for its length to be.
        left[at] -= 1;
        if at < within {
            could_be -= 1;
            tree.add(0, within, 1);
        } else {
            tree.add(at, at + 1, 1);
        }
        if left[at] == 0 {
            tree.add(at, at + 1, RETIRED);
        }
        // The member taken is one fewer to fit what it fitted.
        match unnamed[member].1 {
            Length::Exact(exactly) => {
                if let Some(members) = exact.get_mut(&exactly) {
                    members.remove(&member);
                }
                tree.add(at, at + 1, -1);
            }
            Length::AtMost(bound) => {
                bounded.remove(&(bound, member));
                tree.add(0, upto(bound), -1);
                let narrowed = bounded.last().map(|(bound, _)| *bound);
                if narrowed != reach {
                    let still = narrowed.map_or(0, upto);
                    let mut fell = 0;
                    for (out, count) in (still..within).zip(&left[still..within]) {
                        *work += 1;
                        if *count > 0 {
                            tree.add(out, out + 1, could_be - count);
                            fell += count;
                        }
                    }
                    tree.add(0, still, fell);
                    could_be -= fell;
                    reach = narrowed;
                    within = still;
                }
            }
        }
        let last = unnamed.len() - 1;
        if member != last {
            match unnamed[last].1 {
                Length::Exact(exactly) => {
                    if let Some(members) = exact.get_mut(&exactly) {
                        members.remove(&last);
                        members.insert(member);
                    }
                }
                Length::AtMost(bound) => {
                    bounded.remove(&(bound, last));
                    bounded.insert((bound, member));
                }
            }
        }
        known.push((file, unnamed.swap_remove(member).0));
    }
    *work += tree.steps;
    known
}

// A margin no later change can lift to none: the length has no file left.
const RETIRED: i64 = i64::MIN / 4;

// Margins over the lengths, shortest first, with a range made larger or
// smaller at once and the longest length whose margin is none or more found
// in a walk from the root. Each node holds what was added to its whole range
// and the largest margin under it, its own addition included.
struct Margins {
    width: usize,
    largest: Vec<i64>,
    added: Vec<i64>,
    steps: u64,
}

impl Margins {
    fn new(margins: &[i64]) -> Self {
        let width = margins.len();
        let mut tree = Self {
            width,
            largest: vec![0; 4 * width.max(1)],
            added: vec![0; 4 * width.max(1)],
            steps: 0,
        };
        tree.build(1, 0, width, margins);
        tree
    }

    fn build(&mut self, node: usize, low: usize, high: usize, margins: &[i64]) {
        self.steps += 1;
        if high - low == 1 {
            self.added[node] = margins[low];
            self.largest[node] = margins[low];
            return;
        }
        let middle = low + (high - low) / 2;
        self.build(2 * node, low, middle, margins);
        self.build(2 * node + 1, middle, high, margins);
        self.largest[node] = self.largest[2 * node].max(self.largest[2 * node + 1]);
    }

    fn add(&mut self, from: usize, to: usize, by: i64) {
        if from < to && by != 0 {
            self.add_under(1, 0, self.width, from, to, by);
        }
    }

    fn add_under(&mut self, node: usize, low: usize, high: usize, from: usize, to: usize, by: i64) {
        self.steps += 1;
        if to <= low || high <= from {
            return;
        }
        if from <= low && high <= to {
            self.added[node] += by;
            self.largest[node] += by;
            return;
        }
        let middle = low + (high - low) / 2;
        self.add_under(2 * node, low, middle, from, to, by);
        self.add_under(2 * node + 1, middle, high, from, to, by);
        self.largest[node] =
            self.added[node] + self.largest[2 * node].max(self.largest[2 * node + 1]);
    }

    fn last_ready(&mut self) -> Option<usize> {
        let (mut node, mut low, mut high, mut above) = (1, 0, self.width, 0);
        if self.largest[node] < 0 {
            return None;
        }
        while high - low > 1 {
            self.steps += 1;
            above += self.added[node];
            let middle = low + (high - low) / 2;
            if self.largest[2 * node + 1] + above >= 0 {
                (node, low) = (2 * node + 1, middle);
            } else {
                (node, high) = (2 * node, middle);
            }
        }
        Some(low)
    }
}
