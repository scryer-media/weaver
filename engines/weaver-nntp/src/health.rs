//! Per-server health tracking with automatic degradation and disabling.
//!
//! Servers transition through four states based on connection outcomes:
//!
//! - **Healthy** — all good, use normally
//! - **Degraded** — experiencing transient failures, still usable but deprioritised
//! - **CoolingDown** — short-lived quarantine after transport/capacity problems
//! - **Disabled** — temporarily taken out of rotation (auth failure or too many consecutive errors)

use std::time::{Duration, Instant};

/// The current operational state of a server.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ServerState {
    /// Server is operating normally.
    Healthy,
    /// Server is experiencing transient failures but is still usable.
    Degraded { consecutive_failures: u32 },
    /// Server hit a short-lived transport/capacity issue and should be skipped
    /// briefly without affecting the longer-lived health state machine.
    CoolingDown {
        until: Instant,
        reason: CooldownReason,
        resume_degraded: Option<u32>,
    },
    /// Server is temporarily disabled and should not be used.
    Disabled {
        until: Instant,
        reason: DisableReason,
    },
}

/// Why a server entered a short-lived cooldown.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CooldownReason {
    /// Transport-level problems such as timeouts, disconnects, or 400 errors.
    Transport,
    /// Capacity-related problems such as too many connections or pool exhaustion.
    Capacity,
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
    /// How long to cool down a server after a transport-level failure.
    pub transient_cooldown: Duration,
    /// How long to cool down a server after a capacity-related failure.
    pub capacity_cooldown: Duration,
}

impl Default for HealthConfig {
    fn default() -> Self {
        Self {
            degraded_threshold: 5,
            disable_threshold: 10,
            base_backoff: Duration::from_secs(30),
            max_backoff: Duration::from_hours(1),
            auth_disable_duration: Duration::from_mins(5),
            transient_cooldown: Duration::from_secs(10),
            capacity_cooldown: Duration::from_secs(5),
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
    /// Exponentially weighted moving average of latency in microseconds.
    latency_ewma_us: f64,
    /// Number of latency samples recorded.
    latency_samples: u32,
    /// Recent premature connection deaths (connections that died before
    /// `MIN_CONNECTION_LIFETIME`). Stored as timestamps for time-windowed counting.
    premature_deaths: Vec<Instant>,
}

impl ServerHealth {
    /// Create a new `ServerHealth` starting in the `Healthy` state.
    /// Connections younger than this when they die are counted as premature deaths.
    pub const MIN_CONNECTION_LIFETIME: Duration = Duration::from_secs(60);
    /// Window for counting recent premature deaths.
    const PREMATURE_DEATH_WINDOW: Duration = Duration::from_secs(120);

    pub fn new(config: HealthConfig) -> Self {
        Self {
            state: ServerState::Healthy,
            success_count: 0,
            failure_count: 0,
            consecutive_failures: 0,
            disable_count: 0,
            config,
            latency_ewma_us: 0.0,
            latency_samples: 0,
            premature_deaths: Vec::new(),
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

    /// Record a short-lived transport or capacity failure.
    ///
    /// Capacity failures only trigger a brief cooldown. Transport failures also
    /// advance the longer-lived degraded/disabled thresholds so a flaky primary
    /// eventually yields to backup servers instead of re-entering immediately forever.
    pub fn record_cooldown(&mut self, reason: CooldownReason) {
        self.failure_count += 1;

        let (duration, resume_degraded) = match reason {
            // Transport problems should still participate in the longer-lived
            // degraded/disabled state machine so a server that repeatedly
            // times out does not keep hopping in and out of short cooldowns
            // forever while remaining the preferred primary.
            CooldownReason::Transport => {
                self.consecutive_failures += 1;

                if self.consecutive_failures >= self.config.disable_threshold {
                    let backoff = self.compute_backoff();
                    self.disable_count += 1;
                    self.state = ServerState::Disabled {
                        until: Instant::now() + backoff,
                        reason: DisableReason::ConsecutiveFailures,
                    };
                    return;
                }

                let resume_degraded = if self.consecutive_failures >= self.config.degraded_threshold
                {
                    Some(self.consecutive_failures)
                } else {
                    None
                };

                (self.config.transient_cooldown, resume_degraded)
            }
            CooldownReason::Capacity => {
                let resume_degraded = match self.state {
                    ServerState::Degraded {
                        consecutive_failures,
                    } => Some(consecutive_failures),
                    ServerState::CoolingDown {
                        resume_degraded, ..
                    } => resume_degraded,
                    _ => None,
                };

                (self.config.capacity_cooldown, resume_degraded)
            }
        };

        self.state = ServerState::CoolingDown {
            until: Instant::now() + duration,
            reason,
            resume_degraded,
        };
    }

    /// Whether this server can currently accept work.
    pub fn is_available(&self) -> bool {
        !matches!(
            self.state,
            ServerState::Disabled { .. } | ServerState::CoolingDown { .. }
        )
    }

    /// The current state of this server.
    pub fn state(&self) -> &ServerState {
        &self.state
    }

    /// If the server is disabled and the backoff period has elapsed, transition
    /// back to Degraded for a probationary period. The consecutive failure count
    /// is set to one below the disable threshold so that a single additional
    /// failure immediately re-disables the server (with increased backoff),
    /// while a success resets the server to Healthy.
    pub fn check_reenable(&mut self) {
        match self.state {
            ServerState::Disabled { until, .. } if Instant::now() >= until => {
                // Re-enter as Degraded just below the disable threshold so one
                // more failure trips the circuit breaker again immediately.
                let probe_failures = self.config.disable_threshold.saturating_sub(1);
                self.consecutive_failures = probe_failures;
                self.state = ServerState::Degraded {
                    consecutive_failures: probe_failures,
                };
            }
            ServerState::CoolingDown {
                until,
                resume_degraded,
                ..
            } if Instant::now() >= until => {
                self.state = match resume_degraded {
                    Some(consecutive_failures) => ServerState::Degraded {
                        consecutive_failures,
                    },
                    None => ServerState::Healthy,
                };
            }
            _ => {}
        }
    }

    /// Record a latency sample, updating the EWMA with α=0.2.
    ///
    /// The first sample seeds the EWMA directly; subsequent samples are
    /// blended using `new = α * sample + (1 - α) * old`.
    pub fn record_latency(&mut self, duration: Duration) {
        let sample_us = duration.as_secs_f64() * 1_000_000.0;
        if self.latency_samples == 0 {
            self.latency_ewma_us = sample_us;
        } else {
            const ALPHA: f64 = 0.2;
            self.latency_ewma_us = ALPHA * sample_us + (1.0 - ALPHA) * self.latency_ewma_us;
        }
        self.latency_samples += 1;
    }

    /// Returns the EWMA latency in milliseconds, or 50.0 if no samples have
    /// been recorded yet (cold start default).
    pub fn latency_ms(&self) -> f64 {
        if self.latency_samples == 0 {
            50.0
        } else {
            self.latency_ewma_us / 1_000.0
        }
    }

    /// Record a premature connection death — a connection that died before
    /// reaching `MIN_CONNECTION_LIFETIME`. Indicates infrastructure problems
    /// (firewalls, proxies, ISP throttling) rather than article-level issues.
    pub fn record_premature_death(&mut self) {
        let now = Instant::now();
        self.premature_deaths.push(now);
        // Prune entries outside the window.
        let cutoff = now - Self::PREMATURE_DEATH_WINDOW;
        self.premature_deaths.retain(|&t| t > cutoff);
    }

    /// Count of premature connection deaths within the recent time window.
    pub fn recent_premature_deaths(&self) -> usize {
        let cutoff = Instant::now() - Self::PREMATURE_DEATH_WINDOW;
        self.premature_deaths
            .iter()
            .filter(|&&t| t > cutoff)
            .count()
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

    /// Record a short-lived cooldown-worthy failure for the given server.
    pub fn record_cooldown(&mut self, server_idx: usize, reason: CooldownReason) {
        self.servers[server_idx].record_cooldown(reason);
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
                ServerState::CoolingDown { .. } | ServerState::Disabled { .. } => {}
            }
        }

        healthy.extend(degraded);
        healthy
    }

    /// Record a premature connection death for the given server.
    pub fn record_premature_death(&mut self, server_idx: usize) {
        self.servers[server_idx].record_premature_death();
    }

    /// Recent premature deaths for the given server.
    pub fn recent_premature_deaths(&self, server_idx: usize) -> usize {
        self.servers[server_idx].recent_premature_deaths()
    }

    /// Record a latency sample for the given server.
    pub fn record_latency(&mut self, server_idx: usize, duration: Duration) {
        self.servers[server_idx].record_latency(duration);
    }

    /// Returns the EWMA latency in milliseconds for the given server.
    pub fn latency_ms(&self, server_idx: usize) -> f64 {
        self.servers[server_idx].latency_ms()
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
            transient_cooldown: Duration::from_millis(50),
            capacity_cooldown: Duration::from_millis(25),
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
        // Re-enables as Degraded (probationary), not Healthy.
        assert!(matches!(health.state(), ServerState::Degraded { .. }));
        assert!(health.is_available());
        // consecutive_failures is set to disable_threshold - 1 so one more
        // failure immediately re-disables.
        assert_eq!(
            health.consecutive_failures,
            test_config().disable_threshold - 1
        );

        // A success should fully reset to Healthy.
        health.record_success();
        assert_eq!(*health.state(), ServerState::Healthy);
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

    #[test]
    fn cooldown_excludes_server_until_expiry() {
        let mut health = ServerHealth::new(test_config());
        health.record_cooldown(CooldownReason::Transport);

        assert!(matches!(
            health.state(),
            ServerState::CoolingDown {
                reason: CooldownReason::Transport,
                ..
            }
        ));
        assert!(!health.is_available());

        std::thread::sleep(Duration::from_millis(60));
        health.check_reenable();

        assert_eq!(*health.state(), ServerState::Healthy);
        assert!(health.is_available());
        assert_eq!(health.consecutive_failures, 1);
    }

    #[test]
    fn cooldown_from_degraded_restores_degraded_state() {
        let mut health = ServerHealth::new(test_config());
        for _ in 0..3 {
            health.record_failure(false);
        }
        assert!(matches!(
            health.state(),
            ServerState::Degraded {
                consecutive_failures: 3
            }
        ));

        health.record_cooldown(CooldownReason::Capacity);
        assert!(!health.is_available());

        std::thread::sleep(Duration::from_millis(30));
        health.check_reenable();

        assert!(matches!(
            health.state(),
            ServerState::Degraded {
                consecutive_failures: 3
            }
        ));
        assert_eq!(health.consecutive_failures, 3);
    }

    #[test]
    fn repeated_transport_cooldowns_eventually_disable_server() {
        let mut health = ServerHealth::new(test_config());

        for expected_failures in 1..test_config().disable_threshold {
            health.record_cooldown(CooldownReason::Transport);
            assert!(matches!(
                health.state(),
                ServerState::CoolingDown {
                    reason: CooldownReason::Transport,
                    ..
                }
            ));
            assert_eq!(health.consecutive_failures, expected_failures);

            std::thread::sleep(Duration::from_millis(60));
            health.check_reenable();
        }

        assert!(matches!(
            health.state(),
            ServerState::Degraded {
                consecutive_failures: 4
            }
        ));

        health.record_cooldown(CooldownReason::Transport);
        assert!(matches!(
            health.state(),
            ServerState::Disabled {
                reason: DisableReason::ConsecutiveFailures,
                ..
            }
        ));
        assert!(!health.is_available());
        assert_eq!(health.consecutive_failures, test_config().disable_threshold);
    }

    #[test]
    fn latency_cold_start_returns_default() {
        let health = ServerHealth::new(test_config());
        // No samples recorded — should return the 50ms cold start default.
        assert!((health.latency_ms() - 50.0).abs() < f64::EPSILON);
    }

    #[test]
    fn latency_first_sample_seeds_ewma() {
        let mut health = ServerHealth::new(test_config());
        health.record_latency(Duration::from_millis(100));
        // First sample seeds directly: 100ms.
        assert!((health.latency_ms() - 100.0).abs() < 0.01);
    }

    #[test]
    fn latency_ewma_converges() {
        let mut health = ServerHealth::new(test_config());

        // Seed with 100ms.
        health.record_latency(Duration::from_millis(100));
        assert!((health.latency_ms() - 100.0).abs() < 0.01);

        // Feed 10 samples of 200ms — EWMA should converge toward 200ms.
        for _ in 0..10 {
            health.record_latency(Duration::from_millis(200));
        }

        // After 10 samples with alpha=0.2: should be very close to 200ms.
        // Exact: 100 * 0.8^10 + 200 * (1 - 0.8^10) = 100*0.107 + 200*0.893 ≈ 189.3
        let latency = health.latency_ms();
        assert!(
            latency > 180.0 && latency < 200.0,
            "expected EWMA to converge near 200ms, got {latency}ms"
        );
    }

    #[test]
    fn latency_ewma_update_formula() {
        let mut health = ServerHealth::new(test_config());

        // Seed: 100ms
        health.record_latency(Duration::from_millis(100));

        // Second sample: 200ms
        // EWMA = 0.2 * 200 + 0.8 * 100 = 40 + 80 = 120ms
        health.record_latency(Duration::from_millis(200));
        assert!((health.latency_ms() - 120.0).abs() < 0.01);

        // Third sample: 200ms
        // EWMA = 0.2 * 200 + 0.8 * 120 = 40 + 96 = 136ms
        health.record_latency(Duration::from_millis(200));
        assert!((health.latency_ms() - 136.0).abs() < 0.01);
    }

    #[test]
    fn tracker_record_latency() {
        let config = test_config();
        let mut tracker = HealthTracker::new(2, config);

        // Cold start for both servers.
        assert!((tracker.latency_ms(0) - 50.0).abs() < f64::EPSILON);
        assert!((tracker.latency_ms(1) - 50.0).abs() < f64::EPSILON);

        // Record latency for server 0 only.
        tracker.record_latency(0, Duration::from_millis(80));
        assert!((tracker.latency_ms(0) - 80.0).abs() < 0.01);
        // Server 1 should still be at cold start.
        assert!((tracker.latency_ms(1) - 50.0).abs() < f64::EPSILON);
    }

    #[test]
    fn circuit_breaker_disables_after_consecutive_failures() {
        let config = test_config(); // disable_threshold = 5
        let mut tracker = HealthTracker::new(1, config);

        // Record 5 consecutive transient failures.
        for _ in 0..5 {
            tracker.record_failure(0, false);
        }

        // Server should be disabled.
        assert!(!tracker.is_available(0));
        assert!(matches!(
            tracker.server(0).state(),
            ServerState::Disabled {
                reason: DisableReason::ConsecutiveFailures,
                ..
            }
        ));
    }

    #[test]
    fn circuit_breaker_ten_failures_disables_with_default_config() {
        // Use default config (disable_threshold = 10).
        let config = HealthConfig::default();
        let mut tracker = HealthTracker::new(1, config);

        // 10 consecutive failures should disable the server.
        for i in 0..10 {
            tracker.record_failure(0, false);
            if i < 9 {
                // Should still be available (healthy or degraded).
                assert!(
                    tracker.server(0).is_available(),
                    "server should be available after {} failures",
                    i + 1
                );
            }
        }

        assert!(!tracker.server(0).is_available());
    }
}
