// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Scripts: files in an operator-configured directory, wired up as saved
// instances that each run one script on one trigger.

pub mod callbacks;
pub use weaver_server_postproc::post_processing::directives;
pub mod effects;
pub mod events;
pub mod executor;
pub mod feed;
pub mod hooks;
pub mod instances;
pub use weaver_server_postproc::post_processing::listing;
pub use weaver_server_postproc::post_processing::manifest;
pub use weaver_server_postproc::post_processing::model;
pub mod output;
pub mod preset;
pub mod quota_events;
pub mod route_events;
pub mod run_metrics;
pub use weaver_server_postproc::post_processing::runner;
pub mod scan;
pub mod scheduler;
pub mod secrets;
pub mod settings;
pub mod test_run;

#[cfg(test)]
mod executor_tests;
#[cfg(test)]
mod settings_tests;
