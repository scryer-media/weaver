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
