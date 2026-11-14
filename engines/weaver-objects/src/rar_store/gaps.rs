// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The volume numbers a store has said missing, as runs: each from its
// first number up to, not including, its end, apart from every other and
// not touching it. A set that numbers its last volume far past the ones it
// holds is said missing in a handful of runs, never a number at a time.

use std::collections::BTreeMap;

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(super) struct Gaps(BTreeMap<u32, u32>);

impl Gaps {
    // The runs said, in order.
    pub(super) fn runs(&self) -> impl Iterator<Item = (u32, u32)> + '_ {
        self.0.iter().map(|(&first, &end)| (first, end))
    }

    // The parts of `first..end` not said yet, in order, each said now. The
    // cost is that of the runs it meets, never of the numbers it spans.
    pub(super) fn say(&mut self, first: u32, end: u32) -> Vec<(u32, u32)> {
        let mut fresh = Vec::new();
        if first >= end {
            return fresh;
        }
        let mut low = first;
        let mut high = end;
        let mut at = first;
        // A run begun before `first` that reaches it or touches it.
        if let Some((&start, &stop)) = self.0.range(..first).next_back()
            && stop >= first
        {
            low = start;
            high = high.max(stop);
            at = at.max(stop);
            self.0.remove(&start);
        }
        // Runs begun inside `first..=end`, which it meets or touches.
        let met: Vec<(u32, u32)> = self
            .0
            .range(first..=end)
            .map(|(&start, &stop)| (start, stop))
            .collect();
        for (start, stop) in met {
            if start > at {
                fresh.push((at, start.min(end)));
            }
            at = at.max(stop);
            high = high.max(stop);
            self.0.remove(&start);
        }
        if at < end {
            fresh.push((at, end));
        }
        self.0.insert(low, high);
        fresh
    }
}

#[cfg(test)]
mod tests {
    use super::Gaps;

    #[test]
    fn a_run_said_again_says_nothing() {
        let mut gaps = Gaps::default();
        assert_eq!(gaps.say(0, 70_000), vec![(0, 70_000)]);
        assert!(gaps.say(0, 70_000).is_empty());
        assert!(gaps.say(5, 6).is_empty());
        assert_eq!(gaps.runs().collect::<Vec<_>>(), vec![(0, 70_000)]);
    }

    #[test]
    fn only_the_parts_not_said_are_said() {
        let mut gaps = Gaps::default();
        assert_eq!(gaps.say(2, 3), vec![(2, 3)]);
        assert_eq!(gaps.say(5, 7), vec![(5, 7)]);
        assert_eq!(gaps.say(0, 10), vec![(0, 2), (3, 5), (7, 10)]);
        assert_eq!(gaps.runs().collect::<Vec<_>>(), vec![(0, 10)]);
    }

    #[test]
    fn touching_runs_join() {
        let mut gaps = Gaps::default();
        assert_eq!(gaps.say(0, 3), vec![(0, 3)]);
        assert_eq!(gaps.say(4, 6), vec![(4, 6)]);
        assert_eq!(gaps.say(3, 4), vec![(3, 4)]);
        assert_eq!(gaps.runs().collect::<Vec<_>>(), vec![(0, 6)]);
        assert_eq!(gaps.say(8, 9), vec![(8, 9)]);
        assert_eq!(gaps.say(6, 8), vec![(6, 8)]);
        assert_eq!(gaps.runs().collect::<Vec<_>>(), vec![(0, 9)]);
    }

    // A run said one number at a time ends as the run said whole, so a
    // store restored from facts that kept numbers one by one says nothing
    // it said before.
    #[test]
    fn numbers_said_one_by_one_are_one_run() {
        let mut gaps = Gaps::default();
        for index in 0..100 {
            assert_eq!(gaps.say(index, index + 1), vec![(index, index + 1)]);
        }
        assert!(gaps.say(0, 100).is_empty());
        assert_eq!(gaps.runs().collect::<Vec<_>>(), vec![(0, 100)]);
    }

    #[test]
    fn an_empty_run_is_nothing() {
        let mut gaps = Gaps::default();
        assert!(gaps.say(4, 4).is_empty());
        assert_eq!(gaps.runs().count(), 0);
    }
}
