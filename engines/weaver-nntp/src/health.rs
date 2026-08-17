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
    /// The windowed transport-failure ratio exceeded the configured threshold.
    ///
    /// The consecutive-failure machine cannot catch a server that fails a
    /// steady fraction of attempts: any success resets the run, so a primary
    /// stalling 10% of BODY fetches stays "Healthy" forever while a clean
    /// backup idles. The ratio window is the cumulative complement (compare
    /// SABnzbd's `bad_cons / threads` block).
    FailureRatio,
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
    /// Length of the rolling window for failure-ratio accounting.
    pub failure_ratio_window: Duration,
    /// Minimum attempts inside one window before the ratio can trip; keeps
    /// isolated blips on quiet servers from disabling anything.
    pub failure_ratio_min_attempts: u32,
    /// Percentage of failed attempts within a window that disables the server.
    pub failure_ratio_threshold_pct: u32,
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
            // 10% sustained transport failure over 40+ attempts is pathological
            // for any real provider (normal transient rates are well under 1%),
            // while the sample floor keeps a single blip from ever tripping.
            failure_ratio_window: Duration::from_secs(30),
            failure_ratio_min_attempts: 40,
            failure_ratio_threshold_pct: 10,
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
    /// Start of the current failure-ratio window; `None` until the first attempt.
    ratio_window_started: Option<Instant>,
    /// Attempts recorded in the current failure-ratio window.
    ratio_attempts: u32,
    /// Failed attempts recorded in the current failure-ratio window.
    ratio_failures: u32,
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
            ratio_window_started: None,
            ratio_attempts: 0,
            ratio_failures: 0,
        }
    }

    /// Record a successful operation — resets consecutive failures and returns to Healthy.
    pub fn record_success(&mut self) {
        self.success_count += 1;
        self.consecutive_failures = 0;
        self.note_ratio_attempt(false, false);
        // In-flight fetches routinely land right after a failure-ratio trip —
        // at the trip threshold most attempts still succeed. Those stragglers
        // must not lift the quarantine whose whole purpose is shifting work
        // away from a server that keeps succeeding most of the time.
        if let ServerState::Disabled {
            until,
            reason: DisableReason::FailureRatio,
        } = self.state
            && Instant::now() < until
        {
            return;
        }
        self.state = ServerState::Healthy;
    }

    /// Record one attempt into the failure-ratio window; returns `true` when
    /// the window tripped and moved the server to [`ServerState::Disabled`].
    ///
    /// Unlike `consecutive_failures` (reset by any success), the window counts
    /// cumulatively, so a server failing a steady fraction of a busy workload
    /// trips even though successes vastly outnumber failures. Quiet servers
    /// never reach `failure_ratio_min_attempts` within one window and fall
    /// back to the consecutive machine.
    fn note_ratio_attempt(&mut self, failed: bool, allow_trip: bool) -> bool {
        let now = Instant::now();
        match self.ratio_window_started {
            Some(started) if now.duration_since(started) <= self.config.failure_ratio_window => {}
            _ => {
                self.ratio_window_started = Some(now);
                self.ratio_attempts = 0;
                self.ratio_failures = 0;
            }
        }
        self.ratio_attempts += 1;
        if failed {
            self.ratio_failures += 1;
        }
        if !allow_trip || matches!(self.state, ServerState::Disabled { .. }) {
            return false;
        }
        if self.ratio_attempts < self.config.failure_ratio_min_attempts
            || self.ratio_failures.saturating_mul(100)
                < self
                    .ratio_attempts
                    .saturating_mul(self.config.failure_ratio_threshold_pct)
        {
            return false;
        }
        let backoff = self.compute_backoff();
        self.disable_count += 1;
        self.state = ServerState::Disabled {
            until: now + backoff,
            reason: DisableReason::FailureRatio,
        };
        self.ratio_window_started = None;
        self.ratio_attempts = 0;
        self.ratio_failures = 0;
        true
    }

    /// Record a failed operation.
    ///
    /// If `is_auth` is true the server is immediately disabled regardless of the
    /// consecutive failure count. Otherwise the state transitions through
    /// Degraded and eventually Disabled based on configured thresholds.
    pub fn record_failure(&mut self, is_auth: bool) {
        self.record_failure_gated(is_auth, true);
    }

    /// [`Self::record_failure`] with an explicit failure-ratio trip gate.
    ///
    /// [`HealthTracker`] passes `allow_ratio_trip: false` when no other fill
    /// server could absorb the shifted load — disabling the only usable
    /// server would turn a 10%-flaky-but-90%-working connection into a full
    /// outage. The window still counts attempts either way.
    pub fn record_failure_gated(&mut self, is_auth: bool, allow_ratio_trip: bool) {
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

        // A ratio trip subsumes the consecutive-threshold transitions below.
        if self.note_ratio_attempt(true, allow_ratio_trip) {
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
        self.record_cooldown_gated(reason, true);
    }

    /// [`Self::record_cooldown`] with an explicit failure-ratio trip gate
    /// (see [`Self::record_failure_gated`]).
    pub fn record_cooldown_gated(&mut self, reason: CooldownReason, allow_ratio_trip: bool) {
        self.failure_count += 1;

        // Ratio accounting runs first: when sustained transport flake trips
        // the window, the resulting disable subsumes the short cooldown.
        // Capacity rejections are provider connection policy, not flakiness,
        // and stay out of the ratio window.
        if matches!(reason, CooldownReason::Transport)
            && self.note_ratio_attempt(true, allow_ratio_trip)
        {
            return;
        }

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

    /// How many times this server has been disabled since process start. Drives
    /// the exponential re-enable backoff and is exported as a monitoring
    /// counter — a server that keeps flapping shows a climbing value even when
    /// each individual outage is short enough to miss a scrape.
    pub fn disable_count(&self) -> u32 {
        self.disable_count
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
    /// Backfill flag per server, parallel to `servers`. Failure-ratio trips
    /// only fire when another FILL server can absorb the shifted load;
    /// backfill servers never count (health never unlocks backfill, so
    /// disabling the last fill server in their favor would stall fill work).
    backfill: Vec<bool>,
}

impl HealthTracker {
    /// Create a tracker for `server_count` servers, all starting Healthy and
    /// all treated as fill servers.
    pub fn new(server_count: usize, config: HealthConfig) -> Self {
        Self::new_with_backfill(server_count, config, vec![false; server_count])
    }

    /// Create a tracker with explicit per-server backfill flags.
    pub fn new_with_backfill(
        server_count: usize,
        config: HealthConfig,
        backfill: Vec<bool>,
    ) -> Self {
        debug_assert_eq!(backfill.len(), server_count);
        let servers = (0..server_count)
            .map(|_| ServerHealth::new(config.clone()))
            .collect();
        Self { servers, backfill }
    }

    /// Whether a failure-ratio trip on `server_idx` has somewhere to shift
    /// load: another fill server that is currently usable. A server sitting in
    /// an unexpired disable/cooldown does not count — conservative, since the
    /// next window re-evaluates after it re-enables.
    fn ratio_trip_allowed(&self, server_idx: usize) -> bool {
        self.servers.iter().enumerate().any(|(idx, server)| {
            idx != server_idx
                && !self.backfill.get(idx).copied().unwrap_or(false)
                && matches!(
                    server.state(),
                    ServerState::Healthy | ServerState::Degraded { .. }
                )
        })
    }

    /// Record a successful operation for the given server.
    pub fn record_success(&mut self, server_idx: usize) {
        self.servers[server_idx].record_success();
    }

    /// Record a failed operation for the given server.
    pub fn record_failure(&mut self, server_idx: usize, is_auth: bool) {
        let allow_ratio_trip = self.ratio_trip_allowed(server_idx);
        self.servers[server_idx].record_failure_gated(is_auth, allow_ratio_trip);
    }

    /// Record a short-lived cooldown-worthy failure for the given server.
    pub fn record_cooldown(&mut self, server_idx: usize, reason: CooldownReason) {
        let allow_ratio_trip = self.ratio_trip_allowed(server_idx);
        self.servers[server_idx].record_cooldown_gated(reason, allow_ratio_trip);
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
            // High sample floor so consecutive-machine tests above never
            // interact with the ratio window.
            failure_ratio_window: Duration::from_secs(3600),
            failure_ratio_min_attempts: 40,
            failure_ratio_threshold_pct: 10,
        }
    }

    fn ratio_config(min_attempts: u32, threshold_pct: u32, window: Duration) -> HealthConfig {
        HealthConfig {
            failure_ratio_window: window,
            failure_ratio_min_attempts: min_attempts,
            failure_ratio_threshold_pct: threshold_pct,
            ..test_config()
        }
    }

    #[test]
    fn failure_ratio_trips_despite_interleaved_successes() {
        let mut health = ServerHealth::new(ratio_config(10, 20, Duration::from_secs(3600)));

        // 8 successes and 2 transport failures, interleaved so the run of
        // consecutive failures never exceeds one — the consecutive machine is
        // structurally blind to this shape.
        for attempt in 0..10 {
            if attempt % 5 == 4 {
                health.record_cooldown(CooldownReason::Transport);
            } else {
                health.record_success();
            }
            assert!(health.consecutive_failures <= 1);
        }

        assert!(matches!(
            health.state(),
            ServerState::Disabled {
                reason: DisableReason::FailureRatio,
                ..
            }
        ));
        assert!(!health.is_available());
    }

    #[test]
    fn failure_ratio_needs_min_samples() {
        let mut health = ServerHealth::new(ratio_config(10, 20, Duration::from_secs(3600)));

        // 25% failure ratio, but below the sample floor: a blip, not a trend.
        health.record_cooldown(CooldownReason::Transport);
        for _ in 0..3 {
            health.record_success();
        }

        assert_eq!(*health.state(), ServerState::Healthy);
        assert!(health.is_available());
    }

    #[test]
    fn capacity_cooldowns_stay_out_of_the_ratio_window() {
        let mut health = ServerHealth::new(ratio_config(4, 25, Duration::from_secs(3600)));

        // Provider capacity rejections at any rate must not trip the ratio.
        for _ in 0..12 {
            health.record_cooldown(CooldownReason::Capacity);
            health.record_success();
        }

        assert!(!matches!(
            health.state(),
            ServerState::Disabled {
                reason: DisableReason::FailureRatio,
                ..
            }
        ));
    }

    #[test]
    fn success_stragglers_do_not_lift_ratio_disable() {
        let mut health = ServerHealth::new(ratio_config(4, 50, Duration::from_secs(3600)));

        for _ in 0..2 {
            health.record_success();
            health.record_cooldown(CooldownReason::Transport);
        }
        assert!(matches!(
            health.state(),
            ServerState::Disabled {
                reason: DisableReason::FailureRatio,
                ..
            }
        ));

        // At a 10% failure rate, ~9 in-flight successes land right after the
        // trip. They must not restore Healthy while the quarantine holds.
        for _ in 0..9 {
            health.record_success();
        }
        assert!(!health.is_available());
        assert!(matches!(
            health.state(),
            ServerState::Disabled {
                reason: DisableReason::FailureRatio,
                ..
            }
        ));
    }

    #[test]
    fn ratio_window_expiry_resets_counts() {
        let mut health = ServerHealth::new(ratio_config(4, 50, Duration::from_millis(30)));

        health.record_cooldown(CooldownReason::Transport);
        health.record_cooldown(CooldownReason::Transport);
        std::thread::sleep(Duration::from_millis(60));

        // Fresh window: 1 failure over 4 attempts stays under 50% — without
        // the reset the carried failures would have tripped at attempt four.
        for _ in 0..3 {
            health.record_success();
        }
        health.record_cooldown(CooldownReason::Transport);

        // The final failure still earns its short transport cooldown, but the
        // ratio must not have tripped: without the reset, six attempts with
        // three failures would have crossed the 50% threshold.
        assert!(matches!(
            health.state(),
            ServerState::CoolingDown {
                reason: CooldownReason::Transport,
                ..
            }
        ));
    }

    /// Drive one server in a tracker through a 50% failure pattern that
    /// crosses the ratio threshold (min 4 samples).
    fn drive_ratio_pattern(tracker: &mut HealthTracker, server_idx: usize) {
        for _ in 0..4 {
            tracker.record_success(server_idx);
            tracker.record_cooldown(server_idx, CooldownReason::Transport);
        }
    }

    #[test]
    fn tracker_ratio_trip_requires_another_usable_fill_server() {
        // Single server: never trip — disabling the only server would turn a
        // flaky-but-working connection into a full outage.
        let mut solo = HealthTracker::new(1, ratio_config(4, 50, Duration::from_secs(3600)));
        drive_ratio_pattern(&mut solo, 0);
        assert!(!matches!(
            solo.server(0).state(),
            ServerState::Disabled {
                reason: DisableReason::FailureRatio,
                ..
            }
        ));

        // A backfill server is not an alternative: health never unlocks
        // backfill, so the fill workload would stall.
        let mut with_backfill = HealthTracker::new_with_backfill(
            2,
            ratio_config(4, 50, Duration::from_secs(3600)),
            vec![false, true],
        );
        drive_ratio_pattern(&mut with_backfill, 0);
        assert!(!matches!(
            with_backfill.server(0).state(),
            ServerState::Disabled {
                reason: DisableReason::FailureRatio,
                ..
            }
        ));

        // A second fill server that is itself disabled does not count either.
        let mut peer_down = HealthTracker::new(2, ratio_config(4, 50, Duration::from_secs(3600)));
        peer_down.record_failure(1, true);
        assert!(!peer_down.is_available(1));
        drive_ratio_pattern(&mut peer_down, 0);
        assert!(!matches!(
            peer_down.server(0).state(),
            ServerState::Disabled {
                reason: DisableReason::FailureRatio,
                ..
            }
        ));

        // With a healthy second fill server the trip fires and shifts load.
        let mut pair = HealthTracker::new(2, ratio_config(4, 50, Duration::from_secs(3600)));
        drive_ratio_pattern(&mut pair, 0);
        assert!(matches!(
            pair.server(0).state(),
            ServerState::Disabled {
                reason: DisableReason::FailureRatio,
                ..
            }
        ));
        assert!(pair.is_available(1));
    }

    #[test]
    fn ratio_disable_reenables_as_degraded_probe() {
        let mut health = ServerHealth::new(ratio_config(4, 50, Duration::from_secs(3600)));

        for _ in 0..2 {
            health.record_success();
            health.record_cooldown(CooldownReason::Transport);
        }
        assert!(!health.is_available());

        std::thread::sleep(Duration::from_millis(150));
        health.check_reenable();

        // Same re-entry semantics as a consecutive-failure disable: probe as
        // Degraded one failure below the breaker.
        assert!(matches!(health.state(), ServerState::Degraded { .. }));
        assert!(health.is_available());
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
    fn disable_count_is_observable() {
        let mut health = ServerHealth::new(test_config());
        assert_eq!(health.disable_count(), 0);

        health.record_failure(true);
        assert_eq!(health.disable_count(), 1);
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
