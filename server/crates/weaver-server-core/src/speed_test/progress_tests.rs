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
