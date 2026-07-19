//! Typed contracts for post-processing extensions.

pub mod discovery;
pub mod manifest;
pub mod model;
pub mod persistence;
pub mod runner;
pub mod service;

#[cfg(test)]
mod discovery_tests;
#[cfg(test)]
mod manifest_tests;
#[cfg(test)]
mod model_tests;
#[cfg(test)]
mod persistence_tests;
#[cfg(test)]
mod runner_tests;
#[cfg(test)]
mod service_tests;
