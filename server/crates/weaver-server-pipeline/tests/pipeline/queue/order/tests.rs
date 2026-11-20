// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::*;

#[test]
fn keys_order_by_priority_then_position() {
    let mut keys = [
        DispatchKey::new(1, 0, JobId(1)),
        DispatchKey::new(0, 5, JobId(2)),
        DispatchKey::new(2, 1, JobId(3)),
        DispatchKey::new(1, 3, JobId(4)),
    ];
    keys.sort_unstable();
    let ids: Vec<u64> = keys.iter().map(|key| key.job_id().0).collect();
    assert_eq!(ids, [2, 1, 4, 3]);
}

#[test]
fn keep_first_keeps_the_first_n_in_order() {
    let all: Vec<DispatchKey> = (0..20u64)
        .map(|i| DispatchKey::new((i % 3) as u8, (19 - i) as usize, JobId(i)))
        .collect();
    for n in [0, 1, 3, 5, 19, 20, 25] {
        let mut kept = all.clone();
        keep_first(&mut kept, n);
        kept.sort_unstable();
        let mut sorted = all.clone();
        sorted.sort_unstable();
        sorted.truncate(n);
        assert_eq!(kept, sorted, "n = {n}");
    }
}

#[test]
fn priority_reads_high_normal_low_in_any_case() {
    let meta = |value: &str| vec![("Priority".to_string(), value.to_string())];
    assert_eq!(metadata_priority(&meta("HIGH")), 0);
    assert_eq!(metadata_priority(&meta("normal")), 1);
    assert_eq!(metadata_priority(&meta("low")), 2);
    assert_eq!(metadata_priority(&meta("other")), 1);
    assert_eq!(metadata_priority(&[]), 1);
}
