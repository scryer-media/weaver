// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use std::time::{Duration, Instant};

use super::Progress;
use crate::speed_test::model::BandwidthResult;

fn bandwidth() -> Option<BandwidthResult> {
    Some(BandwidthResult {
        provider_id: None,
        server: None,
        url: "https://127.0.0.1/".to_string(),
        connections: 0,
    })
}

// A run of `seconds` that moved `bytes`, `moved` of them by a warm-up end at `at`.
fn run(seconds: u64, bytes: u64, warm_up: Option<(u64, u64)>) -> f64 {
    let progress = Progress::new(1);
    let started = Instant::now();
    let _ = progress.started.set(started);
    let _ = progress
        .finished
        .set(started + Duration::from_secs(seconds));
    progress.add_leg_bytes(0, bytes);
    if let Some((at, moved)) = warm_up {
        progress.mark_warm_up(Duration::from_secs(at), moved);
    }
    progress
        .result(false, bandwidth())
        .expect("measuring")
        .average_bps
}

#[test]
fn the_average_leaves_the_warm_up_out() {
    assert_eq!(run(10, 1_000, Some((5, 400))), 120.0);
}

#[test]
fn a_run_that_never_got_past_its_warm_up_averages_the_whole_run() {
    assert_eq!(run(10, 1_000, None), 100.0);
}

#[test]
fn a_run_that_moved_nothing_after_its_warm_up_averages_the_whole_run() {
    assert_eq!(run(10, 1_000, Some((5, 1_000))), 100.0);
}

#[test]
fn each_connection_is_rated_over_the_newest_bin() {
    let progress = Progress::new(1);
    let started = Instant::now();
    let _ = progress.started.set(started);
    let _ = progress.finished.set(started + Duration::from_secs(2));
    let path = |member| crate::speed_test::model::SpeedTestDialPath {
        egress_id: 1,
        rung: Some(0),
        pool_id: Some(4),
        member_id: Some(member),
        proxy_ids: vec![member],
    };
    let carrying = progress.record(0, Duration::ZERO, path(7), None, None);
    let idle = progress.record(0, Duration::ZERO, path(8), None, None);
    carrying.add_bytes(4 << 20);
    progress.add_leg_bytes(0, 4 << 20);
    progress.sample();
    drop((carrying, idle));
    let result = progress.result(false, bandwidth()).expect("measuring");
    let rates: Vec<_> = result
        .connections
        .iter()
        .map(|connection| (connection.path.member_id, connection.bytes_per_second))
        .collect();
    assert_eq!(rates, [(Some(7), 2 << 20), (Some(8), 0)]);
}

// A Usenet run over two legs that closes a bin at each of `ticks` and at
// its end, `ms` in; each bin moves the given bytes per leg.
fn binned(ms: u64, ticks: &[u64], bins: &[[u64; 2]]) -> crate::speed_test::model::SpeedTestResult {
    assert_eq!(ticks.len() + 1, bins.len());
    let progress = Progress::new(2);
    let started = Instant::now();
    let _ = progress.started.set(started);
    let ends = ticks.iter().copied().chain([ms]);
    for (at, moved) in ends.zip(bins) {
        progress.add_leg_bytes(0, moved[0]);
        progress.add_leg_bytes(1, moved[1]);
        if at == ms {
            let _ = progress.finished.set(started + Duration::from_millis(ms));
            progress.sample();
        } else {
            progress.sample_at(Duration::from_millis(at));
        }
    }
    progress.result(true, None).expect("measuring")
}

#[test]
fn a_run_inside_one_bin_peaks_at_its_average() {
    // 100 MB in 800 ms: the sampler never ticks, the end closes the one bin.
    let result = binned(800, &[], &[[60_000_000, 40_000_000]]);
    assert_eq!(result.average_bps, 125_000_000.0);
    assert_eq!(result.peak_bps, 125_000_000.0);
    let legs: Vec<_> = result.legs.iter().map(|leg| leg.peak_bps).collect();
    assert_eq!(legs, [75_000_000.0, 50_000_000.0]);
}

#[test]
fn the_last_partial_bin_counts_at_its_own_width() {
    // 10 MB, 20 MB, then 15 MB in a 250 ms tail: the tail is the fastest.
    let result = binned(
        2_250,
        &[1_000, 2_000],
        &[
            [5_000_000, 5_000_000],
            [10_000_000, 10_000_000],
            [15_000_000, 0],
        ],
    );
    assert_eq!(result.average_bps, 45_000_000.0 / 2.25);
    assert_eq!(result.peak_bps, 60_000_000.0);
    assert!(result.peak_bps >= result.average_bps);
    let legs: Vec<_> = result.legs.iter().map(|leg| leg.peak_bps).collect();
    assert_eq!(legs, [60_000_000.0, 10_000_000.0]);
}

#[test]
fn the_first_bin_counts_the_bytes_before_the_first_tick() {
    // 30 MB before the first tick, then a slow second and a slow tail.
    let result = binned(
        2_500,
        &[1_000, 2_000],
        &[[30_000_000, 0], [1_000_000, 1_000_000], [500_000, 0]],
    );
    assert_eq!(result.peak_bps, 30_000_000.0);
    assert!(result.peak_bps >= result.average_bps);
}
