// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::*;

fn order(ids: &[u64]) -> Vec<JobId> {
    ids.iter().copied().map(JobId).collect()
}

#[test]
fn splices_top_bottom_and_clamped_offsets() {
    let mut jobs = order(&[1, 2, 3, 4]);
    assert!(splice_job_order(&mut jobs, JobId(3), QueueMoveTarget::Top).unwrap());
    assert_eq!(jobs, order(&[3, 1, 2, 4]));

    assert!(splice_job_order(&mut jobs, JobId(1), QueueMoveTarget::Bottom).unwrap());
    assert_eq!(jobs, order(&[3, 2, 4, 1]));

    assert!(splice_job_order(&mut jobs, JobId(4), QueueMoveTarget::Offset(-1)).unwrap());
    assert_eq!(jobs, order(&[3, 4, 2, 1]));

    // Offsets clamp at the edges instead of erroring.
    assert!(splice_job_order(&mut jobs, JobId(3), QueueMoveTarget::Offset(-5)).is_ok());
    assert_eq!(jobs, order(&[3, 4, 2, 1]));
    assert!(splice_job_order(&mut jobs, JobId(3), QueueMoveTarget::Offset(99)).unwrap());
    assert_eq!(jobs, order(&[4, 2, 1, 3]));

    // No-op moves report false; unknown ids error.
    assert!(!splice_job_order(&mut jobs, JobId(4), QueueMoveTarget::Top).unwrap());
    assert!(matches!(
        splice_job_order(&mut jobs, JobId(9), QueueMoveTarget::Top),
        Err(SchedulerError::JobNotFound(JobId(9)))
    ));
}
