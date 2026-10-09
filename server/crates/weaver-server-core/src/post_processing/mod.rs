//! Post-processing scripts: files in an operator-configured directory, an
//! ordered list per job, executed as a bounded step of job finalization.

pub mod directives;
pub mod effects;
pub mod events;
pub mod executor;
pub mod feed;
pub mod hooks;
pub mod listing;
pub mod manifest;
pub mod model;
pub mod output;
pub mod runner;
pub mod scan;
pub mod scheduler;
pub mod settings;
pub mod test_run;

#[cfg(test)]
mod executor_tests;
#[cfg(test)]
mod listing_tests;
#[cfg(test)]
mod manifest_tests;
#[cfg(test)]
mod model_tests;
#[cfg(test)]
mod runner_tests;
#[cfg(test)]
mod settings_tests;
