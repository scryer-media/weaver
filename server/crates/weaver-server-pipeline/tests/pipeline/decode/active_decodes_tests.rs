// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::*;

// The kept sum is the sum of the per-job counts after every kind of change.
#[test]
fn the_kept_sum_follows_every_change_to_the_counts() {
    let mut active = ActiveDecodes::default();
    let check = |active: &ActiveDecodes| {
        assert_eq!(active.total(), active.values().sum::<usize>());
    };
    active.started(JobId(1));
    active.started(JobId(1));
    active.started(JobId(2));
    check(&active);
    assert_eq!(active.total(), 3);
    active.finished(JobId(1));
    active.finished(JobId(3));
    check(&active);
    assert_eq!(active.total(), 2);
    active.finished(JobId(2));
    assert!(!active.contains_key(&JobId(2)));
    assert_eq!(active.remove(&JobId(1)), Some(1));
    assert_eq!(active.remove(&JobId(1)), None);
    check(&active);
    assert!(active.is_empty());
    assert_eq!(active.insert(JobId(4), 2), None);
    assert_eq!(active.insert(JobId(4), 5), Some(2));
    active.insert(JobId(5), 0);
    active.finished(JobId(5));
    check(&active);
    assert_eq!(active.total(), 5);
    assert!(!active.contains_key(&JobId(5)));
}
