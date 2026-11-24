// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::*;

const WINDOW: Duration = Duration::from_secs(60);

// A job failing in a loop must not spend another job's first line, and
// what its own window swallowed must be readable from the next line.
#[test]
fn a_noisy_job_does_not_throttle_a_quiet_one() {
    let mut throttle = JobLogThrottle::default();
    let noisy = JobId(9001);
    let quiet = JobId(9002);
    let start = Instant::now();

    assert_eq!(
        throttle.admit_at(noisy, WINDOW, start),
        Some(0),
        "the first emission of a window always gets through"
    );
    for _ in 0..500 {
        assert_eq!(
            throttle.admit_at(noisy, WINDOW, start),
            None,
            "repeats inside the window are counted, not emitted"
        );
    }

    assert_eq!(
        throttle.admit_at(quiet, WINDOW, start),
        Some(0),
        "another job's first emission is its own window, not the noisy job's"
    );

    assert_eq!(
        throttle.admit_at(noisy, WINDOW, start + WINDOW),
        Some(500),
        "the next line that gets through reports what the window swallowed"
    );
    assert_eq!(
        throttle.admit_at(noisy, WINDOW, start + WINDOW * 2),
        Some(0),
        "and the count starts again from the line that reported it"
    );
}

// The map is bounded, so a long-lived worker cannot accumulate a window
// for every job it has ever seen.
#[test]
fn the_throttle_bounds_the_jobs_it_remembers() {
    let mut throttle = JobLogThrottle::default();
    let start = Instant::now();
    for job in 0..(JOB_LOG_THROTTLE_MAX_JOBS as u64 * 2) {
        throttle.admit_at(JobId(job), WINDOW, start);
    }
    assert!(throttle.windows.len() <= JOB_LOG_THROTTLE_MAX_JOBS);
}
