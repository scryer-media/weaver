// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

pub mod directives;
pub mod listing;
#[cfg(test)]
mod listing_tests;
pub mod manifest;
#[cfg(test)]
mod manifest_tests;
pub mod model;
#[cfg(test)]
mod model_tests;

pub mod admission_persistence;
pub mod callbacks;
pub mod database;
pub mod effects;
pub mod events;
pub mod executor;
pub mod feed;
pub mod instance_model;
pub mod instances;
pub mod job_repository;
pub mod output;
pub mod preset;
pub mod run_metrics;
pub mod runner;
#[cfg(any(test, all(unix, feature = "test-support")))]
#[doc(hidden)]
pub mod runner_fixtures;
#[cfg(test)]
mod runner_tests;
pub mod scan;
pub mod scheduler;
pub mod secrets;
pub mod secrets_persistence;
pub mod settings;
pub mod test_run;
