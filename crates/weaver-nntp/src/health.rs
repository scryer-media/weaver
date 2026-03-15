//! Per-server health tracking with automatic degradation and disabling.
//!
//! Servers transition through three states based on connection outcomes:
//!
//! - **Healthy** — all good, use normally
//! - **Degraded** — experiencing transient failures, still usable but deprioritised
//! - **Disabled** — temporarily taken out of rotation (auth failure or too many consecutive errors)

use std::time::{Duration, Instant};

/// The current operational state of a server.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ServerState {
    /// Server is operating normally.
    Healthy,
    /// Server is experiencing transient failures but is still usable.
    Degraded { consecutive_failures: u32 },
    /// Server is temporarily disabled and should not be used.
    Disabled {
        until: Instant,
        reason: DisableReason,
    },
}

/// Why a server was disabled.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DisableReason {
    /// Authentication failed — credentials are wrong or expired.
    AuthFailure,
    /// Too many consecutive failures exceeded the disable threshold.
    ConsecutiveFailures,
}

/// Configuration thresholds for health state transitions.
#[derive(Debug, Clone)]
pub struct HealthConfig {
    /// Consecutive failures before entering the Degraded state.
    pub degraded_threshold: u32,
    /// Consecutive failures before entering the Disabled state.
    pub disable_threshold: u32,
    /// Initial backoff duration when disabled due to consecutive failures.
    pub base_backoff: Duration,
    /// Maximum backoff duration.
    pub max_backoff: Duration,
    /// How long to disable a server after an authentication failure.
    pub auth_disable_duration: Duration,
}

impl Default for HealthConfig {
    fn default() -> Self {
        Self {
            degraded_threshold: 5,
            disable_threshold: 10,
            base_backoff: Duration::from_secs(30),
            max_backoff: Duration::from_secs(3600),
            auth_disable_duration: Duration::from_secs(300),
        }
    }
}

/// Per-server health state tracker.
#[derive(Debug)]
pub struct ServerHealth {
    state: ServerState,
    /// Total successful operations since creation.
    pub success_count: u64,
    /// Total failed operations since creation.
    pub failure_count: u64,
    /// Current run of consecutive failures (reset on success).
    pub consecutive_failures: u32,
    /// Number of times this server has been disabled (used for exponential backoff).
    disable_count: u32,
    config: HealthConfig,
}

impl ServerHealth {
    /// Create a new `ServerHealth` starting in the `Healthy` state.
    pub fn new(config: HealthConfig) -> Self {
        Self {
            state: ServerState::Healthy,
            success_count: 0,
            failure_count: 0,
            consecutive_failures: 0,
            disable_count: 0,
            config,
        }
    }

    /// Record a successful operation — resets consecutive failures and returns to Healthy.
    pub fn record_success(&mut self) {
        self.success_count += 1;
        self.consecutive_failures = 0;
        self.state = ServerState::Healthy;
    }

    /// Record a failed operation.
    ///
    /// If `is_auth` is true the server is immediately disabled regardless of the
    /// consecutive failure count. Otherwise the state transitions through
    /// Degraded and eventually Disabled based on configured thresholds.
    pub fn record_failure(&mut self, is_auth: bool) {
        self.failure_count += 1;
        self.consecutive_failures += 1;

        if is_auth {
            self.disable_count += 1;
            self.state = ServerState::Disabled {
                until: Instant::now() + self.config.auth_disable_duration,
                reason: DisableReason::AuthFailure,
            };
            return;
        }

        if self.consecutive_failures >= self.config.disable_threshold {
            let backoff = self.compute_backoff();
            self.disable_count += 1;
            self.state = ServerState::Disabled {
                until: Instant::now() + backoff,
                reason: DisableReason::ConsecutiveFailures,
            };
        } else if self.consecutive_failures >= self.config.degraded_threshold {
            self.state = ServerState::Degraded {
                consecutive_failures: self.consecutive_failures,
            };
        }
    }

    /// Whether this server can currently accept work.
    pub fn is_available(&self) -> bool {
        !matches!(self.state, ServerState::Disabled { .. })
    }

    /// The current state of this server.
    pub fn state(&self) -> &ServerState {
        &self.state
    }

    /// If the server is disabled and the backoff period has elapsed, transition
    /// back to Healthy. Otherwise this is a no-op.
    pub fn check_reenable(&mut self) {
        if let ServerState::Disabled { until, .. } = self.state
            && Instant::now() >= until
        {
            self.consecutive_failures = 0;
            self.state = ServerState::Healthy;
        }
    }

    /// Compute the exponential backoff duration capped at `max_backoff`.
    fn compute_backoff(&self) -> Duration {
        let multiplier = 2u32.saturating_pow(self.disable_count);
        let backoff = self.config.base_backoff.saturating_mul(multiplier);
        backoff.min(self.config.max_backoff)
    }
}

/// Manages health state for multiple servers.
#[derive(Debug)]
pub struct HealthTracker {
    servers: Vec<ServerHealth>,
}

impl HealthTracker {
    /// Create a tracker for `server_count` servers, all starting Healthy.
    pub fn new(server_count: usize, config: HealthConfig) -> Self {
        let servers = (0..server_count)
            .map(|_| ServerHealth::new(config.clone()))
            .collect();
        Self { servers }
    }

    /// Record a successful operation for the given server.
    pub fn record_success(&mut self, server_idx: usize) {
        self.servers[server_idx].record_success();
    }

    /// Record a failed operation for the given server.
    pub fn record_failure(&mut self, server_idx: usize, is_auth: bool) {
        self.servers[server_idx].record_failure(is_auth);
    }

    /// Whether the given server is available for work.
    pub fn is_available(&mut self, server_idx: usize) -> bool {
        self.servers[server_idx].check_reenable();
        self.servers[server_idx].is_available()
    }

    /// Check all disabled servers and re-enable any whose backoff has expired.
    pub fn check_reenable_all(&mut self) {
        for server in &mut self.servers {
            server.check_reenable();
        }
    }

    /// Return server indices ordered by health: Healthy first, Degraded second,
    /// Disabled servers are excluded entirely.
    pub fn ordered_servers(&mut self) -> Vec<usize> {
        self.check_reenable_all();

        let mut healthy = Vec::new();
        let mut degraded = Vec::new();

        for (idx, server) in self.servers.iter().enumerate() {
            match server.state() {
                ServerState::Healthy => healthy.push(idx),
                ServerState::Degraded { .. } => degraded.push(idx),
                ServerState::Disabled { .. } => {}
            }
        }

        healthy.extend(degraded);
        healthy
    }

    /// Get a reference to the health state for a specific server.
    pub fn server(&self, server_idx: usize) -> &ServerHealth {
        &self.servers[server_idx]
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn test_config() -> HealthConfig {
        HealthConfig {
            degraded_threshold: 3,
            disable_threshold: 5,
            base_backoff: Duration::from_millis(100),
            max_backoff: Duration::from_secs(10),
            auth_disable_duration: Duration::from_millis(100),
        }
    }

    #[test]
    fn healthy_by_default() {
        let health = ServerHealth::new(test_config());
        assert_eq!(*health.state(), ServerState::Healthy);
        assert!(health.is_available());
        assert_eq!(health.success_count, 0);
        assert_eq!(health.failure_count, 0);
        assert_eq!(health.consecutive_failures, 0);
    }

    #[test]
    fn degradation_after_failures() {
        let mut health = ServerHealth::new(test_config());

        // Below threshold — still healthy.
        for _ in 0..2 {
            health.record_failure(false);
        }
        assert_eq!(*health.state(), ServerState::Healthy);

        // Hit the degraded threshold (3).
        health.record_failure(false);
        assert!(matches!(
            health.state(),
            ServerState::Degraded {
                consecutive_failures: 3
            }
        ));
        assert!(health.is_available());
    }

    #[test]
    fn disable_after_many_failures() {
        let mut health = ServerHealth::new(test_config());

        // Accumulate failures up to the disable threshold (5).
        for _ in 0..5 {
            health.record_failure(false);
        }

        assert!(matches!(
            health.state(),
            ServerState::Disabled {
                reason: DisableReason::ConsecutiveFailures,
                ..
            }
        ));
        assert!(!health.is_available());
        assert_eq!(health.failure_count, 5);
    }

    #[test]
    fn auth_failure_disables_immediately() {
        let mut health = ServerHealth::new(test_config());

        // A single auth failure should disable immediately.
        health.record_failure(true);

        assert!(matches!(
            health.state(),
            ServerState::Disabled {
                reason: DisableReason::AuthFailure,
                ..
            }
        ));
        assert!(!health.is_available());
        assert_eq!(health.failure_count, 1);
        assert_eq!(health.consecutive_failures, 1);
    }

    #[test]
    fn success_resets_consecutive() {
        let mut health = ServerHealth::new(test_config());

        // Push into degraded state.
        for _ in 0..4 {
            health.record_failure(false);
        }
        assert!(matches!(health.state(), ServerState::Degraded { .. }));

        // A success should reset everything back to healthy.
        health.record_success();
        assert_eq!(*health.state(), ServerState::Healthy);
        assert_eq!(health.consecutive_failures, 0);
        assert_eq!(health.success_count, 1);
        assert_eq!(health.failure_count, 4);
    }

    #[test]
    fn reenable_after_backoff() {
        let config = HealthConfig {
            auth_disable_duration: Duration::from_millis(1),
            ..test_config()
        };
        let mut health = ServerHealth::new(config);

        health.record_failure(true);
        assert!(!health.is_available());

        // Wait for the disable duration to expire.
        std::thread::sleep(Duration::from_millis(5));

        health.check_reenable();
        assert_eq!(*health.state(), ServerState::Healthy);
        assert!(health.is_available());
        assert_eq!(health.consecutive_failures, 0);
    }

    #[test]
    fn ordered_servers_healthy_first() {
        let config = test_config();
        let mut tracker = HealthTracker::new(3, config);

        // Server 1 is degraded.
        for _ in 0..3 {
            tracker.record_failure(1, false);
        }

        let order = tracker.ordered_servers();
        // Servers 0 and 2 are healthy, server 1 is degraded — healthy come first.
        assert_eq!(order, vec![0, 2, 1]);
    }

    #[test]
    fn ordered_servers_excludes_disabled() {
        let config = test_config();
        let mut tracker = HealthTracker::new(3, config);

        // Disable server 1 via auth failure.
        tracker.record_failure(1, true);

        let order = tracker.ordered_servers();
        // Server 1 should be excluded entirely.
        assert_eq!(order, vec![0, 2]);
        assert!(!tracker.is_available(1));
    }
}
