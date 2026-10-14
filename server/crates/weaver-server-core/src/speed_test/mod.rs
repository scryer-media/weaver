// Operator-run speed tests over one consumer's route: a bandwidth pull from
// a public source, or a Usenet pull of a test NZB on the server's own
// connections with the queue paused.

mod adaptive;
mod bandwidth;
mod failure;
#[cfg(any(test, feature = "test-support"))]
pub mod fixture;
mod http;
mod model;
mod overrides;
mod progress;
pub mod providers;
mod runner;
mod service;
mod usenet;

#[cfg(test)]
mod tests;

pub use failure::SpeedTestFailure;
pub use model::*;
pub use overrides::{TestedLeg, TestedRoute};
pub use providers::{PROVIDERS, ProviderInfo, ProviderServer};
pub use service::{
    ServiceInner, SpeedTestError, SpeedTestMetricsSnapshot, SpeedTestService,
    speed_test_metrics_snapshot,
};
