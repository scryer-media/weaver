//! Scripts: files in an operator-configured directory, wired up as saved
//! instances that each run one script on one trigger.

pub mod callbacks;
pub mod directives;
pub mod effects;
pub mod events;
pub mod executor;
pub mod feed;
pub mod hooks;
pub mod instances;
pub mod listing;
pub mod manifest;
pub mod model;
pub mod output;
pub mod preset;
pub mod runner;
pub mod scan;
pub mod scheduler;
pub mod secrets;
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
