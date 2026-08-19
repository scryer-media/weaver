//! Post-processing scripts: files in `data_dir/scripts`, an ordered list per
//! job, executed as a bounded step of job finalization.

pub mod executor;
pub mod listing;
pub mod manifest;
pub mod model;
pub mod runner;
pub mod settings;

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
