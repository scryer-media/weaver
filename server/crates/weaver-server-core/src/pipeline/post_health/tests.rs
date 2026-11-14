// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::*;

#[test]
fn samples_rotate_across_rounds() {
    let first = sample_indices(120, 0);
    let second = sample_indices(120, 1);

    assert_eq!(first.len(), second.len());
    assert_ne!(first, second);
    assert_eq!(first.first().copied(), Some(0));
    assert_eq!(second.first().copied(), Some(1));
}

#[test]
fn a_small_post_is_sampled_whole_and_a_large_one_one_in_twelve() {
    assert!(sample_indices(0, 3).is_empty());
    assert_eq!(sample_indices(7, 2), (0..7).collect::<Vec<_>>());
    // Eighty asked for, at a stride of twelve, which a thousand fits
    // eighty-four times.
    let large = sample_indices(1000, 0);
    assert_eq!(large.len(), 84);
    assert_eq!(&large[..3], &[0, 12, 24]);
    assert_eq!(large.last().copied(), Some(996));
}

#[test]
fn one_unsettled_batch_costs_coverage_not_the_verdict() {
    let mut tally = ProbeTally::default();
    tally.record_answered(&[true; 50]);
    tally.record_unsettled(50);
    let mut third = vec![true; 45];
    third.extend_from_slice(&[false; 5]);
    tally.record_answered(&third);

    let update = tally.update(JobId(30040), 0);

    assert!(!update.inconclusive);
    // The verdict is read over what answered, never over the sample the
    // round set out to take.
    assert_eq!(update.total, 100);
    assert_eq!(update.missed, 5);
    assert_eq!(update.unverified, 50);
    assert_eq!(tally.unverified_batches, 1);
}

#[test]
fn a_probe_that_settled_nothing_is_inconclusive() {
    let mut tally = ProbeTally::default();
    tally.record_unsettled(50);
    tally.record_unsettled(50);
    tally.record_unsettled(16);

    let update = tally.update(JobId(30041), 0);

    assert!(update.inconclusive);
    assert_eq!(update.total, 0);
    assert_eq!(update.missed, 0);
    assert_eq!(update.unverified, 116);
    assert_eq!(tally.unverified_batches, 3);
}

#[test]
fn damage_in_one_file_while_the_post_lands_is_a_hole_not_a_dead_release() {
    assert!(!looks_like_a_dead_release(1_000, 1, 4_000));
    assert!(looks_like_a_dead_release(1_000, 1, 4_001));
    assert!(looks_like_a_dead_release(1_000_000, 2, 1));
}

#[test]
fn a_round_that_found_articles_missing_rearms_on_the_next_lost_byte() {
    let tracked = 10 * 1024 * 1024 * 1024;
    assert_eq!(next_failed_bytes(500, tracked, u64::MAX, 3, false), 501);
    assert_eq!(next_failed_bytes(500, tracked, u64::MAX, 0, true), 501);
}

#[test]
fn a_clean_round_waits_for_material_damage_but_never_past_critical() {
    let tracked = 100 * 1024 * 1024 * 1024;
    // A two-hundredth of the payload is more than the floor.
    assert_eq!(
        next_failed_bytes(0, tracked, u64::MAX, 0, false),
        tracked / REARM_PAYLOAD_DIVISOR
    );
    // A small payload waits at least the floor.
    assert_eq!(
        next_failed_bytes(0, 1024, u64::MAX, 0, false),
        REARM_MIN_BYTES
    );
    // The bytes that take the post below critical are never waited past.
    assert_eq!(next_failed_bytes(0, tracked, 4096, 0, false), 4097);
}

#[test]
fn a_projection_is_the_missed_share_of_the_payload() {
    assert_eq!(projected(1_000, 10, 9), Some(900));
    assert_eq!(projected(1_000, 10, 0), None);
    assert_eq!(projected(1_000, 0, 0), None);
}

#[test]
fn a_post_at_exactly_its_critical_line_is_still_healthy() {
    assert!(!below_critical(850, 850));
    assert!(below_critical(849, 850));
    assert!(!below_critical(851, 850));
}

// Ten files: the share is eight first articles lost.
fn ten_heads(lost: usize, outstanding: usize) -> Gate {
    let heads = std::iter::repeat_n(Head::Lost, lost)
        .chain(std::iter::repeat_n(Head::Outstanding, outstanding))
        .chain(std::iter::repeat(Head::Settled))
        .take(10);
    first_article_gate(heads)
}

#[test]
fn the_sample_decides_before_health_while_its_outstanding_first_articles_could_reach_its_share() {
    // Four lost, six to come: the six could take it past eight.
    assert_eq!(ten_heads(4, 6), Gate::Undecided);
    // Four lost, three to come, three in hand: seven at most.
    assert_eq!(ten_heads(4, 3), Gate::Open);
    // Eight lost rules with two still to come.
    assert_eq!(
        ten_heads(8, 2),
        Gate::Dead {
            missing: 8,
            total: 10
        }
    );
    // A sample of fewer than ten files never rules, so health speaks.
    assert_eq!(
        first_article_gate(std::iter::repeat_n(Head::Lost, 9)),
        Gate::Open
    );
    assert_eq!(
        first_article_gate(std::iter::repeat_n(Head::Outstanding, 9)),
        Gate::Open
    );
}

// A batch carried across a pool rebuild carries whom it asked by server
// identity, since the new pool may number its servers differently: a server
// still in the pool stays asked under its new number, one that left drops
// out. What the batch found is kept.
#[test]
fn a_batch_across_a_pool_rebuild_carries_whom_it_asked() {
    let identity = |n: u32| weaver_nntp::ServerIdentity {
        stable_id: weaver_nntp::transfer::StableServerId(n),
        host: format!("news-{n}.example.com"),
        port: 563,
        username: None,
    };
    let mut layouts = crate::pipeline::PoolLayouts::default();
    layouts.activate(
        (2, vec![identity(1), identity(2), identity(3)]),
        3,
        vec![identity(1), identity(2), identity(3)],
    );
    let mut batch = ProbeBatch::new(JobId(30050), 0, vec!["a@x".into(), "b@x".into()]).in_pool(3);
    assert!(batch.rebase(&layouts, 3));
    for server in [0, 2] {
        batch.fold(
            server,
            ProbeBatchResult {
                exists: vec![server == 0, false],
                inconclusive: false,
            },
        );
    }
    assert_eq!(batch.asked(), [0, 2]);
    // Server 3 moves to the front, server 1 leaves.
    layouts.activate((3, Vec::new()), 4, vec![identity(3), identity(2)]);
    assert!(!batch.rebase(&layouts, 4));
    assert_eq!(batch.asked(), [0]);
    assert_eq!(batch.unfound(), vec!["b@x".to_string()]);
    assert!(batch.rebase(&layouts, 4));
}
