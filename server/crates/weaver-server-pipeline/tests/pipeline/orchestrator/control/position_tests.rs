// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::*;

fn ids(range: std::ops::Range<u64>) -> Vec<JobId> {
    range.map(JobId).collect()
}

// Applies `writes` and checks the stored positions ascend along `order`.
fn assert_sorted(order: &[JobId], persisted: &mut HashMap<JobId, i64>, writes: &[(JobId, i64)]) {
    persisted.extend(writes.iter().copied());
    let stored: Vec<i64> = order.iter().map(|id| persisted[id]).collect();
    assert!(
        stored.windows(2).all(|pair| pair[0] < pair[1]),
        "{stored:?}"
    );
}

#[test]
fn a_queue_written_once_is_not_written_again() {
    let order = ids(0..10_000);
    let mut persisted = HashMap::new();
    let writes = queue_position_writes(&order, &persisted);
    assert_eq!(writes.len(), order.len());
    assert_sorted(&order, &mut persisted, &writes);
    assert!(queue_position_writes(&order, &persisted).is_empty());
}

#[test]
fn moving_one_job_writes_one_row_once_the_positions_are_spaced() {
    let mut order = ids(0..10_000);
    let mut persisted = HashMap::new();
    let writes = queue_position_writes(&order, &persisted);
    assert_sorted(&order, &mut persisted, &writes);

    // To the top, to the bottom, and into the middle.
    for (from, to) in [(9_999, 0), (0, 9_999), (5_000, 1_234)] {
        let id = order.remove(from);
        order.insert(to, id);
        let writes = queue_position_writes(&order, &persisted);
        assert_eq!(writes, vec![(id, writes[0].1)]);
        assert_sorted(&order, &mut persisted, &writes);
    }

    // Removing rows and appending one writes only the new row.
    order.retain(|id| id.0 % 3 != 0);
    order.push(JobId(20_000));
    let writes = queue_position_writes(&order, &persisted);
    assert_eq!(writes.len(), 1);
    assert_sorted(&order, &mut persisted, &writes);
}

#[test]
fn dense_positions_with_no_room_are_respaced_once() {
    let mut order = ids(0..100);
    let mut persisted: HashMap<JobId, i64> = order.iter().map(|id| (*id, id.0 as i64)).collect();
    let id = order.remove(50);
    order.insert(10, id);
    let writes = queue_position_writes(&order, &persisted);
    assert_sorted(&order, &mut persisted, &writes);
    let id = order.remove(70);
    order.insert(20, id);
    let writes = queue_position_writes(&order, &persisted);
    assert_eq!(writes.len(), 1);
    assert_sorted(&order, &mut persisted, &writes);
}
