// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The job control types moved to the jobs crate. The speed test service's
// seam stays here, beside the speed test run it returns.
pub use weaver_server_jobs::jobs::control::*;

/// The speed test service as the scheduler holds it.
pub trait HeldSpeedTest: std::any::Any + Send + Sync {
    // Bumped on every change of a run's status or phase, and whenever an
    // article or a GET is counted.
    fn subscribe(&self) -> tokio::sync::watch::Receiver<u64>;
    fn run(&self, id: u32) -> Option<crate::speed_test::SpeedTestRun>;
}
