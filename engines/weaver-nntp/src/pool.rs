// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use std::collections::VecDeque;
use std::net::{IpAddr, SocketAddr};
use std::ops::{Deref, DerefMut};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use tokio::sync::OwnedSemaphorePermit;
use tokio::sync::{Mutex, Semaphore};

// The idle list is guarded synchronously.
//
// Nothing awaits while holding it, and a dropped [`PooledConnection`]
// must be able to put its socket back on the idle list *before* it releases
// the permit it was holding. Deferring the return to a spawned task released
// the permit first, so an acquire that fired in between found the list empty
// and dialled a fresh connection past a perfectly warm one.
use std::sync::Mutex as SyncMutex;
use std::sync::MutexGuard as SyncMutexGuard;
use tokio::time::Instant as TokioInstant;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, trace, warn};

use crate::address_plan::AddressPlanSnapshot;
use crate::connection::{NntpConnection, ServerConfig};

async fn acquisition_budget<T>(
    deadline: Option<&tokio::time::Instant>,
    future: impl std::future::Future<Output = Result<T>>,
) -> Result<T> {
    if let Some(deadline) = deadline {
        // Report the wait we actually served, rounded up to a second: a zero
        // in this error is reserved for acquires that never waited at all.
        let started = tokio::time::Instant::now();
        tokio::time::timeout_at(*deadline, future)
            .await
            .map_err(|_| NntpError::AcquireTimeout(started.elapsed().as_secs().max(1)))?
    } else {
        future.await
    }
}
use crate::error::{NntpError, Result};
use crate::health::{HealthConfig, HealthTracker, ServerState};
use crate::transfer::{ServerTransferControl, StableServerId};

// Identifies a specific server in the configuration.
#[derive(Debug, Clone, Copy, Hash, Eq, PartialEq)]
pub struct ServerId(pub usize);

// Whether BODY work can use any server without constructing a ranked order.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BodyServerAvailability {
    // At least one server is usable, even if all of its permits are busy.
    Eligible,
    // No server is usable now, but one has a timed health recovery.
    WaitingUntil(Duration),
    // No admissible server has a timed recovery.
    Blocked,
}

// Lock a synchronous pool mutex, reading through a poisoning panic.
//
// A poisoned lock only means some thread panicked while holding it. The
// guarded values are plain collections of connections, so continuing is safe
// and strictly better than refusing to hand out or take back a socket.
fn lock_recovering<T>(mutex: &SyncMutex<T>) -> SyncMutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

// Connection pool for a single NNTP server.
#[allow(dead_code)]
struct ServerPool {
    config: ServerConfig,
    idle: VecDeque<NntpConnection>,
    active_count: usize,
    max_connections: usize,
}

// Multi-server NNTP connection pool.
pub struct NntpPool {
    route_probes_started: std::sync::atomic::AtomicBool,
    #[cfg(test)]
    probe_waits: Vec<tokio::sync::watch::Sender<(u64, Option<tokio::time::Instant>)>>,
    pools: Vec<Arc<SyncMutex<ServerPool>>>,
    configs: Vec<ServerConfig>,
    stable_ids: Vec<StableServerId>,
    transfer_controls: Vec<Option<Arc<ServerTransferControl>>>,
    semaphores: Vec<Arc<Semaphore>>,
    socket_budgets: Vec<Arc<crate::socket_budget::SocketBudget>>,
    shutdown: CancellationToken,
    max_idle_age: Duration,
    health: Arc<Mutex<HealthTracker>>,
    // The health tracker's availability, read without its lock.
    availability: Arc<crate::health::AvailabilityBoard>,
    recovery_gates: Vec<Arc<crate::recovery::RecoveryGate>>,
    // Each server's login identity, as a fingerprint, and configured
    // backfill flag, as this pool was built; see [`NntpPool::activated`].
    setups: Vec<(u64, bool)>,
    // `HealthConfig::auth_give_up_after`, for the line said at the verdict.
    login_refusal_give_up: Duration,
    // Bumped by every change to any of `recovery_gates`; see
    // [`NntpPool::server_health_changes`].
    health_changed: Arc<tokio::sync::watch::Sender<u64>>,
    // Per-server timestamp of the last failed connection attempt.
    last_connect_failure: Vec<Arc<Mutex<Option<Instant>>>>,
    reconnect_delay: Duration,
    // Priority group for each server (parallel to pools/configs).
    groups: Vec<u32>,
    // Backfill flag for each server (parallel to pools/configs).
    backfill: Vec<bool>,
    // The servers that are not backfill.
    fill: crate::ServerSet,
    // The servers by priority level, shared by every policy sampled.
    policy_levels: crate::provider_policy::PolicyLevels,
    // Retention window in days for each server (parallel to pools/configs).
    retention_days: Vec<u32>,
    // Maximum connections per server (parallel to pools/configs).
    max_connections: Vec<usize>,
    // Per-server deadline (unix epoch ms, `0` = none) before which fresh
    // connects are skipped because the provider refused the last one.
    over_limit_until: Vec<AtomicU64>,
    // Per-server state of the episode behind `over_limit_until`: how many
    // connects in a row the provider has refused, and whether one caller is
    // out probing it. Only episode transitions take this lock; the steady
    // state reads the atomic deadline alone.
    over_limit_episodes: Vec<SyncMutex<OverLimitEpisode>>,
    // Per-server epoch-ms floor for the next blocking-connect warning, and the
    // failures suppressed since the last one was emitted. A server that cannot
    // be connected to fails on every dispatch pass, so an unthrottled warning
    // would be a log flood; a silent one is what made the condition invisible.
    blocking_connect_warn_after: Vec<AtomicU64>,
    blocking_connect_failures_since_warning: Vec<AtomicU64>,
    // Cold connection admission only; established BODY lanes never touch it.
    auth_admission: Vec<AuthAdmission>,
}

struct AuthAdmission {
    // 0 = unverified, 1 = verified, >=2 = monotonic retry deadline plus 2.
    state: AtomicU64,
    cooldown: Duration,
    epoch: Instant,
}

impl AuthAdmission {
    fn elapsed_ms(&self) -> u64 {
        self.epoch
            .elapsed()
            .as_millis()
            .try_into()
            .unwrap_or(u64::MAX)
    }

    fn check(&self) -> Result<()> {
        let mut state = self.state.load(Ordering::Acquire);
        while state >= 2 {
            if self.elapsed_ms() < state - 2 {
                return Err(NntpError::AuthenticationRejected);
            }
            match self
                .state
                .compare_exchange(state, 0, Ordering::AcqRel, Ordering::Acquire)
            {
                Ok(_) => break,
                Err(current) => state = current,
            }
        }
        Ok(())
    }

    fn finish<T>(&self, result: &Result<T>) {
        match result {
            Ok(_) => {
                let _ = self
                    .state
                    .compare_exchange(0, 1, Ordering::AcqRel, Ordering::Acquire);
            }
            Err(
                NntpError::AuthenticationFailed
                | NntpError::AuthenticationRejected
                | NntpError::AuthenticationRequired
                | NntpError::AccessDenied,
            ) => {
                self.state.store(
                    self.elapsed_ms()
                        .saturating_add(self.cooldown.as_millis().try_into().unwrap_or(u64::MAX))
                        .saturating_add(2),
                    Ordering::Release,
                );
            }
            Err(_) => {}
        }
    }

    // Asked before every dial, blocking or async; `probe` says the dial is
    // its server's recovery probe. A refused login latches the admission so
    // cold dials stop fanning out, and quarantines the server; the
    // quarantine's probe is then the one dial due, and it must reach the
    // server. The latch is re-armed by every sibling dial the refusal turned
    // away, so it can outlast the quarantine by the spread of their answers;
    // were it asked for the probe, the probe would be turned away without
    // dialing, the quarantine re-armed for its whole length, and the server
    // waited for twice as long. So the probe passes the latch.
    fn before_dial(&self, probe: bool) -> Result<()> {
        if probe { Ok(()) } else { self.check() }
    }

    // Told every dial's outcome, after [`Self::before_dial`]. A probe that
    // logs in ends the latch: the login is taken again. Any other dial that
    // connects is turned away still if a sibling's refusal latched the
    // admission meanwhile.
    fn after_dial<T>(&self, probe: bool, result: &Result<T>) -> Result<()> {
        if probe {
            match result {
                Ok(_) => self.state.store(1, Ordering::Release),
                Err(_) => self.finish(result),
            }
            return Ok(());
        }
        self.finish(result);
        if result.is_ok() { self.check() } else { Ok(()) }
    }
}

pub struct BlockingConnectionPermit {
    _permit: OwnedSemaphorePermit,
    pub(crate) socket_slot: crate::socket_budget::SocketSlot,
    pub(crate) health_lease: Arc<crate::recovery::ConnectionHealthLease>,
    pub(crate) max_idle_age: Duration,
}

#[cfg(test)]
impl BlockingConnectionPermit {
    // A permit backed by its own semaphore, for lane tests that never
    // contend for a pool slot.
    pub(crate) fn for_tests() -> Self {
        let semaphore = Arc::new(Semaphore::new(1));
        Self {
            max_idle_age: Duration::from_mins(5),
            _permit: semaphore
                .try_acquire_owned()
                .expect("a fresh semaphore always has its one permit"),
            socket_slot: crate::socket_budget::SocketBudget::new(1)
                .try_acquire()
                .unwrap(),
            health_lease: Arc::new(
                Arc::new(crate::recovery::RecoveryGate::default())
                    .admit(true)
                    .unwrap(),
            ),
        }
    }
}

// How long fresh connects to a server pause after the provider first answers
// a connect with "too many connections". Existing sessions keep running; only
// new sockets wait, which is what the provider is actually asking for.
//
// When the pause ends, one connect goes out as a probe. A probe the provider
// accepts ends the episode; a refused one doubles the pause, up to
// [`OVER_LIMIT_HOLDOFF_MAX`], so a provider still holding a previous
// process's sessions is asked again in half a minute rather than ten, while
// one that keeps refusing is left alone for longer each time.
pub const OVER_LIMIT_HOLDOFF_INITIAL: Duration = Duration::from_secs(30);

// Ceiling the over-limit holdoff doubles toward while every probe is refused.
pub const OVER_LIMIT_HOLDOFF_MAX: Duration = Duration::from_secs(10 * 60);

// How long a probe connect owns the right to ask the provider before another
// caller may take it over. Longer than any connect timeout, so a probe that
// is still dialling is never doubled up; short enough that a probe whose
// caller vanished does not hold the server closed for long.
pub(crate) const OVER_LIMIT_PROBE_WINDOW: Duration = Duration::from_secs(30);

// Whether a failed fresh dial is the provider declining one more socket
// rather than the server failing. Every failed dial is one or the other: a
// decline arms the over-limit holdoff, anything else is the server's health
// and is paced by the ordinary reconnect delay.
//
// A decline is only the provider's explicit too-many-connections reply. A
// dial the server accepted and then dropped (closed, reset, a failed
// handshake) is the server's health, however many other sockets are busy,
// and so is a refused, unreachable or timed-out dial.
pub(crate) fn dial_declined_over_limit(error: &NntpError) -> bool {
    matches!(
        error,
        NntpError::TooManyConnections | NntpError::ServerOverLimit { .. }
    )
}

// The pause the `refusals`th consecutive refused connect of an episode earns,
// doubling from the plan timing's initial holdoff (production:
// [`OVER_LIMIT_HOLDOFF_INITIAL`]) up to [`OVER_LIMIT_HOLDOFF_MAX`].
pub fn over_limit_holdoff(refusals: u32) -> Duration {
    let doublings = refusals.saturating_sub(1).min(16);
    crate::plan_timing::timing()
        .over_limit_holdoff_initial
        .saturating_mul(1u32 << doublings)
        .min(OVER_LIMIT_HOLDOFF_MAX)
}

fn duration_to_epoch_ms(duration: Duration) -> u64 {
    duration.as_millis().try_into().unwrap_or(u64::MAX)
}

// What [`NntpPool::admit_fresh_connect`] let a caller do, handed back with
// the connect's outcome so the pool knows what that outcome proves.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FreshConnectAdmission {
    // No over-limit episode was running when the connect was admitted. Its
    // success says nothing about a refusal that may have arrived since: a
    // socket the provider accepted before it started refusing is not proof
    // that it accepts again.
    Open,
    // The one connect allowed out after a holdoff to ask whether the
    // provider accepts new sockets again. Its outcome settles the episode —
    // unless the slot has changed hands since it was admitted, in which
    // case the later word stands. The token names this probe among those
    // the server has issued.
    Probe { token: u64 },
}

// One server's over-limit episode. `over_limit_until` is the lock-free
// mirror of whether an episode exists at all; this holds the rest.
#[derive(Debug, Default)]
struct OverLimitEpisode {
    // Consecutive refused connects; each one doubles the next holdoff.
    refusals: u32,
    // Epoch ms at which one caller was let through to probe the provider
    // after the pause ended, `0` when nobody is probing.
    probe_started: u64,
    // Which probe holds the slot: the count of probes ever admitted for
    // this server, so a late report from an earlier one is told apart.
    probe_token: u64,
}

// How often one server's blocking-lane connect failures may be warned about.
// Every dispatch pass retries, so the failures arrive as fast as the scheduler
// runs; the warning stands for all of them and carries the count.
const BLOCKING_CONNECT_WARN_INTERVAL: Duration = Duration::from_secs(60);

fn unix_epoch_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}

// Configuration for creating an NNTP pool.
pub struct PoolConfig {
    pub servers: Vec<ServerPoolConfig>,
    pub max_idle_age: Duration,
    pub health_config: HealthConfig,
    pub reconnect_delay: Duration,
    pub stale_check_age: Duration,
}

impl Default for PoolConfig {
    fn default() -> Self {
        Self {
            servers: Vec::new(),
            max_idle_age: Duration::from_mins(5),
            health_config: HealthConfig::default(),
            reconnect_delay: Duration::from_secs(1),
            stale_check_age: Duration::from_secs(30),
        }
    }
}

// Per-server pool configuration.
pub struct ServerPoolConfig {
    pub server: ServerConfig,
    // Durable database identity. Unlike [`ServerId`], this survives reorder
    // and client rebuilds.
    pub stable_id: StableServerId,
    // Shared BODY policy obtained from a long-lived registry.
    pub transfer_control: Option<Arc<ServerTransferControl>>,
    pub max_connections: usize,
    // Priority group. Lower values tried first within a tier.
    pub group: u32,
    // Backfill servers are ordered after every fill server and are only
    // reachable once all fill servers are excluded for a request.
    pub backfill: bool,
    // Days of retention this server is expected to hold (0 = unlimited).
    // Inert metadata for the pool: callers translate it into per-request
    // exclusions; carrying it here keeps it aligned with server indices.
    pub retention_days: u32,
}

// What makes a pool's server the same server in the next pool: see
// [`NntpPool::server_identities`].
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct ServerIdentity {
    pub stable_id: StableServerId,
    pub host: String,
    pub port: u16,
    pub username: Option<String>,
}

#[cfg(any(test, feature = "test-support"))]
impl Default for ServerPoolConfig {
    fn default() -> Self {
        Self::new(ServerConfig::default())
    }
}

impl ServerPoolConfig {
    pub fn new(server: ServerConfig) -> Self {
        Self {
            server,
            stable_id: StableServerId::default(),
            transfer_control: None,
            max_connections: 1,
            group: 0,
            backfill: false,
            retention_days: 0,
        }
    }
}

// See [`NntpPool::server_health_changes`].
pub struct ServerHealthChanges {
    receiver: tokio::sync::watch::Receiver<u64>,
    pool: std::sync::Weak<NntpPool>,
}

impl ServerHealthChanges {
    // Completes on the next change to any server's recovery. Never completes
    // once the pool is gone.
    pub async fn changed(&mut self) {
        if self.receiver.changed().await.is_err() {
            std::future::pending::<()>().await;
        }
    }

    // Whether this watch is on `pool`, rather than on a pool it replaced.
    pub fn follows(&self, pool: &Arc<NntpPool>) -> bool {
        std::ptr::eq(self.pool.as_ptr(), Arc::as_ptr(pool))
    }
}

// Fill servers' verdicts of login refusal found untold; see
// [`NntpPool::login_refusal_verdicts_untold`].
#[derive(Debug)]
pub struct UntoldLoginVerdicts(Vec<(usize, crate::recovery::UntoldVerdict)>);

// A recovery probe held out on a server, for tests. Answering it lifts the
// quarantine; dropping it unanswered gives the probe back.
#[cfg(any(test, feature = "test-support"))]
pub struct RecoveryProbeForTest(crate::recovery::ConnectionHealthLease);

#[cfg(any(test, feature = "test-support"))]
impl RecoveryProbeForTest {
    pub fn answer(self) {
        assert!(self.0.0.complete_recovery());
    }
}

impl Drop for NntpPool {
    fn drop(&mut self) {
        self.shutdown.cancel();
    }
}

// A pool asked to hold more servers than Weaver takes. The stored server
// list is trimmed to `MAX_PROVIDERS` when it is loaded, so this is only
// reachable from a caller that skipped that, and it is answered in words,
// never by a panic.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("{servers} servers are enabled and Weaver takes at most {max}")]
pub struct PoolTooLarge {
    pub servers: usize,
    pub max: usize,
}

impl NntpPool {
    // A pool for `config`, or the reason it cannot be built: more servers
    // than `MAX_PROVIDERS`. Every pool built from stored or user settings
    // is built here.
    pub fn try_new(config: PoolConfig) -> std::result::Result<Self, PoolTooLarge> {
        let servers = config.servers.len();
        let max = crate::MAX_PROVIDERS;
        if servers > max {
            return Err(PoolTooLarge { servers, max });
        }
        Ok(Self::new(config))
    }

    // Create a new multi-server connection pool. Fixtures and benches build
    // through here; settings go through [`Self::try_new`].
    pub fn new(config: PoolConfig) -> Self {
        let server_count = config.servers.len();
        // Server sets hold one bit per pool index; a pool past them could
        // only be answered by aliasing servers, so it is never built.
        assert!(
            server_count <= crate::ServerSet::CAPACITY,
            "a pool holds at most {} servers, {server_count} configured",
            crate::ServerSet::CAPACITY
        );
        let mut pools = Vec::with_capacity(server_count);
        let mut configs = Vec::with_capacity(server_count);
        let mut stable_ids = Vec::with_capacity(server_count);
        let mut transfer_controls = Vec::with_capacity(server_count);
        let mut semaphores = Vec::with_capacity(server_count);
        let mut last_connect_failure = Vec::with_capacity(server_count);
        let mut groups = Vec::with_capacity(server_count);
        let mut backfill = Vec::with_capacity(server_count);
        let mut retention_days = Vec::with_capacity(server_count);
        let mut max_connections = Vec::with_capacity(server_count);
        let mut over_limit_until = Vec::with_capacity(server_count);
        let mut over_limit_episodes = Vec::with_capacity(server_count);
        let mut blocking_connect_warn_after = Vec::with_capacity(server_count);
        let mut blocking_connect_failures_since_warning = Vec::with_capacity(server_count);
        let mut socket_budgets = Vec::with_capacity(server_count);
        let shutdown = CancellationToken::new();

        // A config where every server is backfill has no fill tier to
        // exhaust; treat it as an all-fill config so downloads can proceed.
        let all_backfill = server_count > 0 && config.servers.iter().all(|spc| spc.backfill);
        if all_backfill {
            warn!("every configured server is marked backfill; treating all servers as fill");
        }

        for spc in &config.servers {
            let budget = spc.transfer_control.as_ref().map_or_else(
                || crate::socket_budget::SocketBudget::new(spc.max_connections),
                |control| Arc::clone(&control.socket_budget),
            );
            let dialer = &spc.server.dialer;
            if let Some(mut targets) = dialer.inner.leg_targets() {
                budget.configure_legs(&targets.borrow_and_update());
                let watched_budget = budget.clone();
                let mut sockets = budget.subscribe();
                let stopped = shutdown.clone();
                dialer.runtime.spawn(async move {
                    loop {
                        tokio::select! {
                            _ = stopped.cancelled() => break,
                            changed = targets.changed() => if changed.is_err() { break; },
                            changed = sockets.changed() => if changed.is_err() { break; },
                        }
                        let current = targets.borrow_and_update().clone();
                        watched_budget.configure_legs(&current);
                    }
                });
            } else {
                budget.configure(spc.max_connections);
            }
            socket_budgets.push(budget);
            if let Some(control) = &spc.transfer_control {
                assert_eq!(
                    spc.stable_id,
                    control.stable_server_id(),
                    "ServerPoolConfig stable_id must match its transfer control"
                );
            }
            stable_ids.push(spc.stable_id);
            transfer_controls.push(spc.transfer_control.clone());
            groups.push(spc.group);
            backfill.push(spc.backfill && !all_backfill);
            retention_days.push(spc.retention_days);
            max_connections.push(spc.max_connections);
            over_limit_until.push(AtomicU64::new(0));
            over_limit_episodes.push(SyncMutex::new(OverLimitEpisode::default()));
            blocking_connect_warn_after.push(AtomicU64::new(0));
            blocking_connect_failures_since_warning.push(AtomicU64::new(0));
            semaphores.push(Arc::new(Semaphore::new(spc.max_connections)));
            configs.push(spc.server.clone());
            pools.push(Arc::new(SyncMutex::new(ServerPool {
                config: spc.server.clone(),
                idle: VecDeque::new(),
                active_count: 0,
                max_connections: spc.max_connections,
            })));
            last_connect_failure.push(Arc::new(Mutex::new(None)));
        }

        let auth_cooldown = config.health_config.auth_disable_duration;
        let login_refusal_give_up = config.health_config.auth_give_up_after;
        let mut health =
            HealthTracker::new_with_backfill(server_count, config.health_config, backfill.clone());
        for (idx, control) in transfer_controls.iter().enumerate() {
            if let Some(control) = control {
                health.set_recovery_gate(idx, Arc::clone(&control.recovery));
            }
        }
        // Each server as this pool was built: its login identity, as a
        // fingerprint, and its configured role.
        let setups: Vec<(u64, bool)> = config
            .servers
            .iter()
            .map(|spc| {
                (
                    crate::transfer::ServerTransferControl::login_fingerprint(
                        &spc.server.host,
                        spc.server.port,
                        spc.server.username.as_deref(),
                        spc.server.password.as_deref(),
                    ),
                    spc.backfill,
                )
            })
            .collect();
        let recovery_gates: Vec<_> = (0..server_count)
            .map(|idx| Arc::clone(&health.server(idx).recovery))
            .collect();
        let health_changed = Arc::new(tokio::sync::watch::channel(0).0);
        for (gate, (identity, backfill)) in recovery_gates.iter().zip(&setups) {
            gate.add_listener(Arc::downgrade(&health_changed));
            gate.built(*identity, *backfill);
        }
        let availability = health.availability();
        let health = Arc::new(Mutex::new(health));

        NntpPool {
            route_probes_started: std::sync::atomic::AtomicBool::new(false),
            #[cfg(test)]
            probe_waits: (0..server_count)
                .map(|_| tokio::sync::watch::channel((0, None)).0)
                .collect(),
            pools,
            configs,
            stable_ids,
            transfer_controls,
            semaphores,
            socket_budgets,
            shutdown,
            max_idle_age: config.max_idle_age,
            health,
            availability,
            recovery_gates,
            setups,
            login_refusal_give_up,
            health_changed,
            last_connect_failure,
            reconnect_delay: config.reconnect_delay,
            policy_levels: crate::provider_policy::PolicyLevels::of(
                groups.iter().zip(&backfill).map(|(priority, backfill)| {
                    crate::provider_policy::ProviderGroup {
                        backfill: *backfill,
                        priority: *priority,
                    }
                }),
            ),
            groups,
            fill: (0..server_count).filter(|idx| !backfill[*idx]).collect(),
            backfill,
            retention_days,
            max_connections,
            over_limit_until,
            over_limit_episodes,
            blocking_connect_warn_after,
            blocking_connect_failures_since_warning,
            auth_admission: (0..server_count)
                .map(|_| AuthAdmission {
                    state: AtomicU64::new(0),
                    cooldown: auth_cooldown,
                    epoch: Instant::now(),
                })
                .collect(),
        }
    }

    // How a new connection to this server picks its address.
    pub(crate) fn start_route_probes(self: &Arc<Self>) {
        if self.route_probes_started.swap(true, Ordering::AcqRel) {
            return;
        }
        for (idx, config) in self.configs.iter().enumerate() {
            let dialer = &config.dialer;
            let Some(mut targets) = dialer.inner.leg_targets() else {
                continue;
            };
            let weak = Arc::downgrade(self);
            let stopped = self.shutdown.clone();
            let semaphore = self.semaphores[idx].clone();
            let mut sockets = self.socket_budgets[idx].subscribe();
            let mut recovery = self.recovery_gates[idx].subscribe();
            dialer.runtime.spawn(async move {
                // A leg that needs a probe changes the route's leg targets
                // when its cooldown lapses, so the loop wakes on that change
                // rather than on a clock. It probes at most once per change;
                // only a probe held back by this server's own gates (auth
                // admission, over-limit, health, permits, socket budget)
                // wakes on the gate's release or its actual retry deadline.
                let mut retry_at: Option<tokio::time::Instant> = None;
                let mut wait_permit = false;
                let mut wait_sockets = false;
                let mut wait_recovery = false;
                let mut first = true;
                loop {
                    if !first {
                        #[cfg(test)]
                        if let Some(pool) = weak.upgrade() {
                            pool.probe_waits[idx].send_modify(|state| {
                                state.0 += 1;
                                state.1 = retry_at;
                            });
                        }
                        tokio::select! {
                            _ = stopped.cancelled() => break,
                            result = targets.changed() => if result.is_err() { break; },
                            result = sockets.changed(), if wait_sockets => if result.is_err() { break; },
                            result = recovery.changed(), if wait_recovery => if result.is_err() { break; },
                            result = semaphore.clone().acquire_owned(), if wait_permit => {
                                if result.is_err() { break; }
                            },
                            () = async {
                                match retry_at {
                                    Some(at) => tokio::time::sleep_until(at).await,
                                    None => std::future::pending().await,
                                }
                            } => {},
                        }
                    }
                    first = false;
                    retry_at = None;
                    wait_permit = false;
                    wait_sockets = false;
                    wait_recovery = false;
                    sockets.borrow_and_update();
                    recovery.borrow_and_update();
                    let Some(pool) = weak.upgrade() else {
                        break;
                    };
                    let route = &pool.configs[idx].dialer;
                    if !route.inner.needs_probe() {
                        continue;
                    }
                    if pool.auth_admission[idx].check().is_err() {
                        let auth = &pool.auth_admission[idx];
                        let remaining = auth.state.load(Ordering::Acquire).saturating_sub(2).saturating_sub(auth.elapsed_ms());
                        retry_at = Some(tokio::time::Instant::now() + Duration::from_millis(remaining));
                        continue;
                    }
                    if let Some(until) = pool.over_limit_until_epoch_ms(ServerId(idx)) {
                        retry_at = Some(tokio::time::Instant::now() + Duration::from_millis(until.saturating_sub(unix_epoch_ms())));
                        continue;
                    }
                    {
                        let mut health = pool.health.lock().await;
                        if !health.is_available(idx) {
                            if let crate::health::ServerState::CoolingDown { until, .. } | crate::health::ServerState::Disabled { until, .. } = health.server(idx).state() {
                                retry_at = Some(tokio::time::Instant::from_std(*until));
                            }
                            wait_recovery = true;
                            continue;
                        }
                    }
                    let Ok(permit) = pool.semaphores[idx].clone().try_acquire_owned() else {
                        wait_permit = true;
                        continue;
                    };
                    let Some(slot) = pool.socket_budgets[idx].try_acquire() else {
                        pool.socket_budgets[idx].recall_idle();
                        wait_sockets = true;
                        continue;
                    };
                    let Some(health_lease) = pool.recovery_gates[idx].admit(false) else {
                        wait_recovery = true;
                        continue;
                    };
                    let result = tokio::select! {
                        _ = stopped.cancelled() => break,
                        result = pool.connect_server(idx, None, false) => result,
                    };
                    if let Err(error) = &result {
                        let provider_evidence = !matches!(error, NntpError::Route(route) if route.is_path_evidence() || !route.is_evidence());
                        if provider_evidence && !dial_declined_over_limit(error) && !matches!(error, NntpError::PoolExhausted | NntpError::PoolShutdown | NntpError::AcquireTimeout(_)) {
                            let auth = matches!(error, NntpError::AuthenticationFailed | NntpError::AuthenticationRejected | NntpError::AuthenticationRequired | NntpError::AccessDenied);
                            pool.health.lock().await.record_connection_outcome(idx, &health_lease.0, false, auth);
                            pool.retire_quarantined_idle(idx);
                        }
                        if !matches!(error, NntpError::Route(route) if !route.is_evidence()) {
                            pool.note_connect_failure(idx, error).await;
                        }
                    }
                    if let Ok(mut connection) = result {
                        *pool.last_connect_failure[idx].lock().await = None;
                        slot.set_path(connection.route_path.as_ref());
                        slot.observe_outcome(connection.route_outcome.as_ref());
                        slot.active();
                        connection.socket_slot = Some(slot);
                        connection.health_lease = Some(Arc::new(health_lease));
                        lock_recovering(&pool.pools[idx]).active_count += 1;
                        // The normal return path registers exact-socket recall and
                        // keeps this successfully authenticated probe warm.
                        drop(PooledConnection {
                            conn: Some(connection),
                            pool: pool.pools[idx].clone(),
                            server_idx: idx,
                            return_to_pool: true,
                            shutdown: stopped.clone(),
                            _permit: Some(permit),
                        });
                    }
                }
            });
        }
    }

    // The address plan of this server's direct leg as it stands: none for
    // a server reached only through proxies.
    pub fn address_plan_snapshot(&self, server: ServerId) -> Option<AddressPlanSnapshot> {
        self.configs
            .get(server.0)?
            .dialer
            .plan
            .as_ref()
            .map(|plan| plan.snapshot())
    }

    async fn connect_server(
        &self,
        idx: usize,
        initial_group: Option<&str>,
        probe: bool,
    ) -> Result<NntpConnection> {
        let auth = &self.auth_admission[idx];
        // Cold handshakes fan out. A rejection latches subsequent admission,
        // which the recovery probe passes: see [`AuthAdmission::before_dial`].
        // Connection-limit recovery has its own single-probe gate below.
        auth.before_dial(probe)?;
        // Asked only once this connect will actually dial, so a probe slot is
        // never taken by a caller the auth gate turns away.
        let admission = self.admit_fresh_connect(ServerId(idx))?;
        let result = self.connect_server_untracked(idx, initial_group).await;
        let latched = auth.after_dial(probe, &result);
        // A refusal here is the provider declining a new socket, not answering
        // for the sessions already held, so it arms the holdoff and stays out
        // of health entirely.
        self.note_fresh_connect_outcome(ServerId(idx), admission, &result);
        let connection = result?;
        latched?;
        Ok(connection)
    }

    async fn connect_server_untracked(
        &self,
        idx: usize,
        initial_group: Option<&str>,
    ) -> Result<NntpConnection> {
        let mut connection =
            NntpConnection::connect_for_group(&self.configs[idx], initial_group).await?;
        connection.set_transfer_control(self.transfer_controls[idx].clone());
        Ok(connection)
    }

    // Acquire a connection from a specific server.
    pub async fn acquire(&self, server: ServerId) -> Result<PooledConnection> {
        self.acquire_for_group(server, None).await
    }

    async fn acquire_dispatch_permit(&self, idx: usize) -> Result<OwnedSemaphorePermit> {
        let semaphore = self.semaphores.get(idx).ok_or(NntpError::PoolExhausted)?;
        loop {
            let mut changed = self.socket_budgets[idx].subscribe();
            if let Ok(permit) = semaphore.clone().try_acquire_owned() {
                return Ok(permit);
            }
            // Nothing idle to call back, and every permit belongs to a
            // download lane that is mid-transfer: the owned lanes serve every
            // server, so an async caller that finds the permits all taken is
            // queued behind whole article transfers, not behind a request
            // that finishes in milliseconds. Waiting here can only spend the
            // caller's whole budget and then fail anyway. Say so now instead.
            // This is the ordinary state of a saturated server, and the
            // caller's requeue answers it, so it is not worth a warning per
            // attempt.
            if !self.socket_budgets[idx].recall_idle() {
                debug!(
                    server = idx,
                    max_connections = self.max_connections[idx],
                    "every connection is held by a busy download lane; not waiting"
                );
                return Err(NntpError::AcquireTimeout(0));
            }
            tokio::select! {
                _ = self.shutdown.cancelled() => return Err(NntpError::PoolShutdown),
                permit = semaphore.clone().acquire_owned() => return permit.map_err(|_| NntpError::PoolShutdown),
                _ = changed.changed() => {},
            }
        }
    }

    pub(crate) async fn acquire_guarded_before_deadline(
        &self,
        server: ServerId,
        initial_group: Option<&str>,
        demanded: bool,
        deadline: &mut tokio::time::Instant,
        guard: Option<&crate::client::WorkGuard>,
    ) -> Result<PooledConnection> {
        if self.shutdown.is_cancelled() {
            return Err(NntpError::PoolShutdown);
        }
        let idx = server.0;
        let permit = acquisition_budget(Some(deadline), self.acquire_dispatch_permit(idx)).await?;
        self.acquire_with_permit_budget(
            idx,
            Some(permit),
            initial_group,
            Some(deadline),
            demanded,
            guard,
        )
        .await
    }

    // Acquire a connection; a fresh connection to a pipelining server
    // selects `initial_group` inside its session-setup write.
    pub async fn acquire_for_group(
        &self,
        server: ServerId,
        initial_group: Option<&str>,
    ) -> Result<PooledConnection> {
        if self.shutdown.is_cancelled() {
            return Err(NntpError::PoolShutdown);
        }

        let idx = server.0;
        if idx >= self.pools.len() {
            return Err(NntpError::PoolExhausted);
        }

        let permit = self.acquire_dispatch_permit(idx).await?;

        self.acquire_with_permit(idx, Some(permit), initial_group)
            .await
    }

    // Whether a normal (in-cap) lease could be taken right now without
    // waiting on the server's connection semaphore.
    pub fn has_available_permit(&self, server: ServerId) -> bool {
        let idx = server.0;
        idx < self.semaphores.len()
            && self.route_available(idx)
            && self.semaphores[idx].available_permits() > 0
    }

    pub fn route_available(&self, idx: usize) -> bool {
        self.configs.get(idx).is_some_and(|config| {
            config
                .dialer
                .inner
                .leg_targets()
                .is_none_or(|targets| targets.borrow().iter().any(|target| *target > 0))
        })
    }

    // Internal: acquire a connection using an already-obtained permit.
    async fn acquire_with_permit(
        &self,
        idx: usize,
        permit: Option<tokio::sync::OwnedSemaphorePermit>,
        initial_group: Option<&str>,
    ) -> Result<PooledConnection> {
        self.acquire_with_permit_budget(idx, permit, initial_group, None, false, None)
            .await
    }

    async fn acquire_with_permit_budget(
        &self,
        idx: usize,
        permit: Option<tokio::sync::OwnedSemaphorePermit>,
        initial_group: Option<&str>,
        mut deadline: Option<&mut tokio::time::Instant>,
        demanded: bool,
        guard: Option<&crate::client::WorkGuard>,
    ) -> Result<PooledConnection> {
        // The guard is asked before a connection is admitted, so it is handed
        // the pool's view of the policy: see [`crate::client::WorkGuard`].
        let still_wanted =
            || guard.is_none_or(|guard| guard(ServerId(idx), &self.provider_policy()));
        // Try to get a healthy idle connection, with stale-check loop.
        let conn = loop {
            if !still_wanted() {
                return Err(NntpError::WorkObsolete);
            }
            // Subscribe before inspecting the cache and physical capacity.
            let mut changed = self.socket_budgets[idx].subscribe();
            if deadline
                .as_deref()
                .is_some_and(|deadline| tokio::time::Instant::now() >= *deadline)
            {
                return Err(NntpError::AcquireTimeout(0));
            }
            let candidate = {
                let mut pool = lock_recovering(&self.pools[idx]);
                self.take_healthy_idle(&mut pool)
            };

            match candidate {
                Some(mut c) => {
                    if c.idle_terminal() {
                        trace!(server = idx, "discarding terminal idle transport");
                        continue;
                    }
                    break c;
                }
                None => {
                    let slot = match self.socket_budgets[idx].try_acquire() {
                        Some(slot) => slot,
                        None => {
                            self.socket_budgets[idx].recall_idle();
                            acquisition_budget(deadline.as_deref(), async {
                                tokio::select! {
                                    _ = self.shutdown.cancelled() => Err(NntpError::PoolShutdown),
                                    _ = changed.changed() => Ok(()),
                                }
                            })
                            .await?;
                            continue;
                        }
                    };
                    // No idle connections available — need to create a new one.
                    // If a recent connection attempt failed, wait until the
                    // reconnect delay has passed before trying again. This
                    // prevents a spin loop where hundreds of tasks
                    // simultaneously get ServiceUnavailable and immediately
                    // retry.
                    {
                        let last_failure = self.last_connect_failure[idx].lock().await;
                        if let Some(ts) = *last_failure {
                            let elapsed = ts.elapsed();
                            if elapsed < self.reconnect_delay {
                                let remaining = self.reconnect_delay - elapsed;
                                drop(last_failure); // release lock while sleeping
                                acquisition_budget(deadline.as_deref(), async {
                                    tokio::time::sleep(remaining).await;
                                    Ok(())
                                })
                                .await?;
                            }
                        }
                    }

                    if !still_wanted() {
                        return Err(NntpError::WorkObsolete);
                    }
                    debug!(server = idx, "creating new connection");
                    let health_lease = self.recovery_gates[idx]
                        .admit(demanded)
                        .ok_or(NntpError::PoolExhausted)?;
                    // The socket is already had: see `ConnectionHealthLease::announce`.
                    health_lease.announce();
                    let started = tokio::time::Instant::now();
                    let connected = self
                        .connect_server(idx, initial_group, health_lease.0.probing())
                        .await;
                    if let Some(deadline) = deadline.as_deref_mut() {
                        *deadline += started.elapsed();
                    }
                    match connected {
                        Ok(mut c) => {
                            slot.set_path(c.route_path.as_ref());
                            slot.observe_outcome(c.route_outcome.as_ref());
                            slot.active();
                            c.socket_slot = Some(slot);
                            c.health_lease = Some(Arc::new(health_lease));
                            // Clear the failure timestamp on success.
                            let mut last_failure = self.last_connect_failure[idx].lock().await;
                            *last_failure = None;
                            break c;
                        }
                        Err(e) => {
                            if let NntpError::Route(error) = &e
                                && let weaver_tunnel::pipe::DialError::AtCapacity(changed) =
                                    error.as_ref()
                            {
                                drop(slot);
                                drop(health_lease);
                                let mut notifications: Vec<_> = changed
                                    .iter()
                                    .map(|changed| Box::pin(changed.notified()))
                                    .collect();
                                for notification in &mut notifications {
                                    notification.as_mut().enable();
                                }
                                if self.configs[idx].dialer.inner.has_capacity() {
                                    continue;
                                }
                                acquisition_budget(deadline.as_deref(), async {
                                        tokio::select! {
                                            _ = self.shutdown.cancelled() => Err(NntpError::PoolShutdown),
                                            _ = std::future::poll_fn(|cx| {
                                                for notification in &mut notifications {
                                                    if std::future::Future::poll(notification.as_mut(), cx).is_ready() {
                                                        return std::task::Poll::Ready(());
                                                    }
                                                }
                                                std::task::Poll::Pending
                                            }) => Ok(()),
                                        }
                                    }).await?;
                                continue;
                            }
                            let provider_evidence = !matches!(&e, NntpError::Route(error) if error.is_path_evidence() || !error.is_evidence());
                            if provider_evidence
                                && !dial_declined_over_limit(&e)
                                && !matches!(
                                    e,
                                    NntpError::PoolExhausted
                                        | NntpError::PoolShutdown
                                        | NntpError::AcquireTimeout(_)
                                )
                            {
                                let auth = matches!(
                                    e,
                                    NntpError::AuthenticationFailed
                                        | NntpError::AuthenticationRejected
                                        | NntpError::AuthenticationRequired
                                        | NntpError::AccessDenied
                                );
                                self.health.lock().await.record_connection_outcome(
                                    idx,
                                    &health_lease.0,
                                    false,
                                    auth,
                                );
                                self.retire_quarantined_idle(idx);
                            }
                            self.note_connect_failure(idx, &e).await;
                            return Err(e);
                        }
                    }
                }
            }
        };

        lock_recovering(&self.pools[idx]).active_count += 1;

        Ok(PooledConnection {
            conn: Some(conn),
            pool: self.pools[idx].clone(),
            server_idx: idx,
            return_to_pool: permit.is_some(),
            shutdown: self.shutdown.clone(),
            _permit: permit,
        })
    }

    // Drain all idle connections across all servers.
    //
    // Called when a network change is suspected (e.g. I/O errors after an
    // interface switch). Connections are dropped without recording health
    // failures, since the servers themselves are fine.
    pub async fn drain_all_idle(&self) {
        let mut total = 0usize;
        for pool in &self.pools {
            let mut p = lock_recovering(pool);
            total += p.idle.len();
            p.idle.clear();
        }
        if total > 0 {
            warn!(
                count = total,
                "drained all idle connections (suspected network change)"
            );
        }
    }

    // Drop the idle connections of one server after one of its sockets
    // failed. A single dead socket says nothing about other providers, so
    // their warm idle connections are left alone.
    pub async fn drain_idle_for(&self, idx: usize) {
        let Some(pool) = self.pools.get(idx) else {
            return;
        };
        let count = {
            let mut p = lock_recovering(pool);
            let count = p.idle.len();
            p.idle.clear();
            count
        };
        if count > 0 {
            debug!(
                server = idx,
                count, "drained this server's idle connections after a socket failure"
            );
        }
    }

    // Shut down the pool and wait for active leases to drain.
    pub async fn shutdown(&self) {
        self.shutdown.cancel();

        for pool in &self.pools {
            lock_recovering(pool).idle.clear();
        }

        // Generation replacement must not let the new client race the old
        // client's still-live BODY sockets for the same provider allowance.
        // Async leases are tracked by `active_count`; owned/blocking lanes are
        // also fenced by their configured semaphore permits.
        let deadline = TokioInstant::now() + Duration::from_secs(15);
        loop {
            let mut async_leases = 0usize;
            for pool in &self.pools {
                async_leases = async_leases.saturating_add(lock_recovering(pool).active_count);
            }
            let configured_leases: usize = self
                .semaphores
                .iter()
                .zip(&self.max_connections)
                .map(|(semaphore, configured)| {
                    configured.saturating_sub(semaphore.available_permits())
                })
                .sum();
            if async_leases == 0 && configured_leases == 0 {
                break;
            }
            if TokioInstant::now() >= deadline {
                warn!(
                    async_leases,
                    configured_leases, "timed out draining active NNTP leases during shutdown"
                );
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }

        debug!("NNTP pool shut down");
    }

    // The number of configured servers.
    pub fn server_count(&self) -> usize {
        self.pools.len()
    }

    // Priority group for each server (indexed by pool position).
    pub fn server_groups(&self) -> &[u32] {
        &self.groups
    }

    // Backfill flag for each server (indexed by pool position). An
    // all-backfill config is normalized to all-fill at construction.
    pub fn server_backfill_flags(&self) -> &[bool] {
        &self.backfill
    }

    // Retention window in days for each server (0 = unlimited), indexed by
    // pool position.
    pub fn server_retention_days(&self) -> &[u32] {
        &self.retention_days
    }

    // Whether any configured server is a backfill server.
    pub fn has_backfill_servers(&self) -> bool {
        self.backfill.iter().any(|backfill| *backfill)
    }

    // Total configured connections across fill (non-backfill) servers.
    pub fn fill_connection_capacity(&self) -> usize {
        self.backfill
            .iter()
            .zip(&self.max_connections)
            .filter(|(backfill, _)| !**backfill)
            .map(|(_, configured)| *configured)
            .sum()
    }

    pub fn configured_connections(&self, server: ServerId) -> Option<usize> {
        self.max_connections.get(server.0).copied()
    }

    // Deadline of this server's active over-limit holdoff, if one is running.
    pub fn over_limit_until_epoch_ms(&self, server: ServerId) -> Option<u64> {
        let deadline = self.over_limit_until.get(server.0)?.load(Ordering::Acquire);
        // Zero is the steady state; skip the clock read on the dispatch path
        // unless a holdoff was ever armed.
        if deadline == 0 {
            return None;
        }
        let now = unix_epoch_ms();
        if deadline > now {
            return Some(deadline);
        }
        // The pause is over but the episode is not: while one caller's probe
        // is out, everyone else is still held, until that probe answers.
        let probe_started = self.over_limit_episode(server.0)?.probe_started;
        let probe_until = probe_started.saturating_add(duration_to_epoch_ms(
            crate::plan_timing::timing().over_limit_probe_window,
        ));
        (probe_started != 0 && probe_until > now).then_some(probe_until)
    }

    // This server's episode state, readable through a poisoned lock: the
    // state is plain data, and refusing to connect because some thread
    // panicked elsewhere would be the worse outcome.
    fn over_limit_episode(&self, idx: usize) -> Option<SyncMutexGuard<'_, OverLimitEpisode>> {
        let slot = self.over_limit_episodes.get(idx)?;
        Some(match slot.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        })
    }

    // Consecutive refused connects in this server's current over-limit
    // episode, `0` when none is running.
    pub fn over_limit_refusals(&self, server: ServerId) -> u32 {
        self.over_limit_episode(server.0)
            .map(|episode| episode.refusals)
            .unwrap_or(0)
    }

    // Whether a fresh socket to `server` may be opened right now.
    //
    // [`FreshConnectAdmission::Open`] when no holdoff is running. During a
    // holdoff every caller gets [`NntpError::ServerOverLimit`]. Once the
    // holdoff ends, exactly one caller is let through as the
    // [`FreshConnectAdmission::Probe`] and the rest keep getting the error
    // until that probe's outcome is reported through
    // [`Self::note_fresh_connect_outcome`] (or one of the outcome methods it
    // dispatches to). A probe must report, or its slot stays taken for
    // [`OVER_LIMIT_PROBE_WINDOW`].
    pub fn admit_fresh_connect(&self, server: ServerId) -> Result<FreshConnectAdmission> {
        let idx = server.0;
        let deadline_slot = self
            .over_limit_until
            .get(idx)
            .ok_or(NntpError::PoolExhausted)?;
        // One atomic load on the steady-state path.
        if deadline_slot.load(Ordering::Acquire) == 0 {
            return Ok(FreshConnectAdmission::Open);
        }
        let now = unix_epoch_ms();
        let mut episode = self
            .over_limit_episode(idx)
            .ok_or(NntpError::PoolExhausted)?;
        // Re-read under the lock: the episode may have moved on meanwhile.
        let deadline = deadline_slot.load(Ordering::Acquire);
        if deadline == 0 {
            return Ok(FreshConnectAdmission::Open);
        }
        if deadline > now {
            return Err(NntpError::ServerOverLimit {
                until_epoch_ms: deadline,
            });
        }
        if episode.probe_started != 0 {
            let probe_until = episode.probe_started.saturating_add(duration_to_epoch_ms(
                crate::plan_timing::timing().over_limit_probe_window,
            ));
            if probe_until > now {
                return Err(NntpError::ServerOverLimit {
                    until_epoch_ms: probe_until,
                });
            }
        }
        episode.probe_started = now;
        episode.probe_token = episode.probe_token.wrapping_add(1);
        self.health_changed
            .send_modify(|revision| *revision = revision.wrapping_add(1));
        debug!(
            server = idx,
            refusals = episode.refusals,
            "holdoff ended; probing the provider with one connect"
        );
        Ok(FreshConnectAdmission::Probe {
            token: episode.probe_token,
        })
    }

    // Book the result of a connect that passed [`Self::admit_fresh_connect`].
    //
    // A refusal always counts. Anything else only means something when the
    // connect was the probe: an ordinary connect that was already dialling
    // when the provider started refusing may well succeed, and must not
    // clear the holdoff the refusal just armed.
    pub fn note_fresh_connect_outcome<T>(
        &self,
        server: ServerId,
        admission: FreshConnectAdmission,
        result: &Result<T>,
    ) {
        match result {
            Err(error) if dial_declined_over_limit(error) => self.note_provider_over_limit(server),
            Ok(_) => self.note_provider_admitted(server, admission),
            Err(_) => self.release_over_limit_probe(server, admission),
        }
    }

    // The provider accepted the probe: whatever over-limit episode was
    // running is over, and the next refusal starts a new one from the
    // shortest pause. Nothing happens for an [`FreshConnectAdmission::Open`]
    // connect, or for a probe whose slot has since been reset by a newer
    // refusal or taken over by a later probe.
    pub fn note_provider_admitted(&self, server: ServerId, admission: FreshConnectAdmission) {
        let FreshConnectAdmission::Probe { token } = admission else {
            return;
        };
        let idx = server.0;
        let Some(deadline_slot) = self.over_limit_until.get(idx) else {
            return;
        };
        let Some(mut episode) = self.over_limit_episode(idx) else {
            return;
        };
        if episode.probe_started == 0 || episode.probe_token != token {
            return;
        }
        let refusals = episode.refusals;
        episode.refusals = 0;
        episode.probe_started = 0;
        deadline_slot.store(0, Ordering::Release);
        drop(episode);
        self.health_changed
            .send_modify(|revision| *revision = revision.wrapping_add(1));
        if let Some(server) = self.configs.get(idx) {
            server.dialer.inner.over_limit_cleared();
        }
        info!(
            server = idx,
            server_address = %self.server_address(server),
            refusals,
            "provider accepts new connections again; over-limit holdoff cleared"
        );
    }

    // A probe that failed for some reason other than a refusal (a timeout, a
    // TLS fault) has answered nothing about the provider's limit. Free the
    // probe slot so the next caller can ask instead of waiting out the
    // window — if it is still this probe's slot.
    pub fn release_over_limit_probe(&self, server: ServerId, admission: FreshConnectAdmission) {
        let FreshConnectAdmission::Probe { token } = admission else {
            return;
        };
        if let Some(mut episode) = self.over_limit_episode(server.0)
            && episode.probe_token == token
        {
            episode.probe_started = 0;
            drop(episode);
            self.health_changed
                .send_modify(|revision| *revision = revision.wrapping_add(1));
        }
    }

    // Records one blocking-lane connect failure and answers whether this one
    // should be warned about.
    //
    // `Some(n)` means "warn, and say that `n` failures have gone unreported
    // since the last warning" — `n` counts this one, so the first failure of a
    // window reports `1`. `None` means the window is still open and the
    // failure has only been counted.
    pub fn note_blocking_connect_warning(&self, server: ServerId) -> Option<u64> {
        let idx = server.0;
        let counter = self.blocking_connect_failures_since_warning.get(idx)?;
        let suppressed = counter.fetch_add(1, Ordering::AcqRel).saturating_add(1);
        let slot = self.blocking_connect_warn_after.get(idx)?;
        let now = unix_epoch_ms();
        let next = now.saturating_add(
            BLOCKING_CONNECT_WARN_INTERVAL
                .as_millis()
                .try_into()
                .unwrap_or(u64::MAX),
        );
        slot.fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
            (current <= now).then_some(next)
        })
        .ok()?;
        counter.store(0, Ordering::Release);
        Some(suppressed)
    }

    // `host:port` of one configured server, for a log line that has to say
    // which one it is talking about.
    pub fn server_address(&self, server: ServerId) -> String {
        self.configs
            .get(server.0)
            .map(|config| format!("{}:{}", config.host, config.port))
            .unwrap_or_default()
    }

    // Whether fresh connects to this server are currently held off. One
    // atomic load, so callers on the dispatch path can ask freely.
    pub fn is_over_limit(&self, server: ServerId) -> bool {
        self.over_limit_until_epoch_ms(server).is_some()
    }

    // Park fresh connects to `server` after the provider refused one.
    //
    // A client restart can collect one rejection per configured connection in
    // a couple of seconds while the provider still holds the previous
    // process's sessions open, so only the first rejection of a window arms
    // and reports it; the rest are silent until the deadline passes. Each
    // window that ends in another refusal (the probe's) doubles the next one,
    // from [`OVER_LIMIT_HOLDOFF_INITIAL`] up to [`OVER_LIMIT_HOLDOFF_MAX`].
    pub fn note_provider_over_limit(&self, server: ServerId) {
        let idx = server.0;
        if let Some(budget) = self.socket_budgets.get(idx) {
            budget.note_provider_refusal();
        }
        let Some(deadline_slot) = self.over_limit_until.get(idx) else {
            return;
        };
        let Some(mut episode) = self.over_limit_episode(idx) else {
            return;
        };
        let now = unix_epoch_ms();
        let current = deadline_slot.load(Ordering::Acquire);
        if current > now {
            // One of the burst the running pause already stands for.
            trace!(server = idx, "provider refused a connection while held off");
            return;
        }
        // A pause that ended longer ago than the longest holdoff, with nothing
        // asking since, is history: this refusal is a new episode and starts
        // again from the shortest pause.
        let stale = current != 0
            && now.saturating_sub(current) > duration_to_epoch_ms(OVER_LIMIT_HOLDOFF_MAX);
        let refusals = if stale {
            1
        } else {
            episode.refusals.saturating_add(1)
        };
        let holdoff = over_limit_holdoff(refusals);
        let deadline = now.saturating_add(duration_to_epoch_ms(holdoff));
        episode.refusals = refusals;
        episode.probe_started = 0;
        deadline_slot.store(deadline, Ordering::Release);
        drop(episode);
        self.health_changed
            .send_modify(|revision| *revision = revision.wrapping_add(1));
        warn!(
            server = idx,
            server_address = %self.server_address(server),
            configured_connections = self.max_connections.get(idx).copied().unwrap_or(0),
            refusals,
            holdoff_secs = holdoff.as_secs(),
            retry_at_epoch_ms = deadline,
            "provider refused a new connection as over its limit; existing \
             connections keep running, one probe connect follows the holdoff \
             and each refused probe doubles it — lower this server's \
             configured connection count if this repeats"
        );
    }

    // Every server's standing for article eligibility, as of now. Health
    // availability comes from the tracker's published board, and credentials
    // and routes are lock-free reads of their own. Only a server with a
    // quota takes its transfer state's lock, briefly, to read what is left;
    // one without a quota is known from a lock-free read to have no limit.
    pub fn provider_policy(&self) -> crate::provider_policy::ProviderPolicy {
        self.provider_policy_with(Self::quota_left_lock_free, None)
    }

    // The policy as a connection already open to `server` reads it before it
    // issues its next request: the one view every recheck of an admitted
    // connection, or of work already bound to one, is judged by (a work
    // guard, an owned lane's lease and pre-BODY checks). A quarantine holds
    // back new connections; the one it has admitted, its probe, is past that
    // hold and must still be able to carry the request it was admitted for,
    // so `holds_probe`, the connection being its server's unanswered probe,
    // reads the server without its own hold. Any other connection, and
    // every other server, stands as in [`Self::provider_policy`].
    pub fn provider_policy_on_connection(
        &self,
        server: usize,
        holds_probe: bool,
    ) -> crate::provider_policy::ProviderPolicy {
        self.provider_policy_with(Self::quota_left_lock_free, holds_probe.then_some(server))
    }

    fn quota_left_lock_free(control: &crate::transfer::ServerTransferControl) -> u64 {
        if control.quota_configured() {
            Self::quota_left(control)
        } else {
            u64::MAX
        }
    }

    // The policy with every server's quota read from a snapshot, as it once
    // was, for the tests that hold the lock-free read to it.
    #[cfg(test)]
    pub(crate) fn provider_policy_by_snapshot(&self) -> crate::provider_policy::ProviderPolicy {
        self.provider_policy_with(Self::quota_left, None)
    }

    // What the quota leaves before local reservations; no limit without one.
    fn quota_left(control: &crate::transfer::ServerTransferControl) -> u64 {
        let quota = control.snapshot();
        if quota.quota_enabled {
            quota
                .quota_limit_bytes
                .saturating_sub(quota.quota_used_bytes)
        } else {
            u64::MAX
        }
    }

    // `connected` is the server of the connection asking, if one is: see
    // [`Self::provider_policy_on_connection`].
    fn provider_policy_with(
        &self,
        quota_left: impl Fn(&crate::transfer::ServerTransferControl) -> u64,
        connected: Option<usize>,
    ) -> crate::provider_policy::ProviderPolicy {
        use crate::provider_policy::{ProviderGroup, ProviderPolicy, ProviderState};
        ProviderPolicy::sampled(
            (0..self.configs.len())
                .map(|idx| {
                    let quota_remaining = self.transfer_controls[idx]
                        .as_deref()
                        .map_or(u64::MAX, &quota_left);
                    ProviderState {
                        group: ProviderGroup {
                            backfill: self.backfill[idx],
                            priority: self.server_groups()[idx],
                        },
                        available: self.availability.is_available(idx)
                            && self.route_available(idx)
                            && (connected == Some(idx) || !self.recovery_holds_new_work(idx)),
                        refuses_credentials: self.refuses_credentials(idx),
                        quota_remaining,
                    }
                })
                .collect(),
            self.policy_levels.clone(),
        )
    }

    // Inspect BODY eligibility without allocating or ranking server candidates.
    pub async fn body_server_availability(
        &self,
        failure_excludes: crate::ServerSet,
        retention_excludes: crate::ServerSet,
        requested_body_bytes: u64,
    ) -> BodyServerAvailability {
        let excluded = failure_excludes.union(retention_excludes);
        let now = Instant::now();
        let mut health = self.health.lock().await;
        health.check_reenable_all();
        let backfill_unlocked = crate::provider_policy::backfill_unlocked(self.fill, excluded);
        let mut retry_after = None;
        for server_idx in 0..self.configs.len() {
            if excluded.contains(server_idx)
                || !self.route_available(server_idx)
                || (self.backfill[server_idx] && !backfill_unlocked)
                || self.transfer_controls[server_idx]
                    .as_ref()
                    .is_some_and(|control| {
                        control.quota_rejection_for(requested_body_bytes).is_some()
                    })
            {
                continue;
            }
            match health.server(server_idx).state() {
                ServerState::Healthy | ServerState::Degraded { .. } => {
                    return BodyServerAvailability::Eligible;
                }
                ServerState::CoolingDown { until, .. } | ServerState::Disabled { until, .. } => {
                    let delay = until.saturating_duration_since(now);
                    retry_after =
                        Some(retry_after.map_or(delay, |current: Duration| current.min(delay)));
                }
            }
        }
        retry_after.map_or(
            BodyServerAvailability::Blocked,
            BodyServerAvailability::WaitingUntil,
        )
    }

    // Whether every fill (non-backfill) server is in `exclude` — the gate
    // that makes backfill servers reachable for a request
    // ([`crate::provider_policy::backfill_unlocked`]).
    pub fn fill_servers_exhausted(&self, exclude: crate::ServerSet) -> bool {
        crate::provider_policy::backfill_unlocked(self.fill, exclude)
    }

    // The servers that are not backfill.
    pub fn fill_servers(&self) -> crate::ServerSet {
        self.fill
    }

    // The same gate for an article that carries its own failure exclusions
    // and its job's retention exclusions apart.
    pub fn backfill_open_for(
        &self,
        failure_excludes: crate::ServerSet,
        retention_excludes: crate::ServerSet,
    ) -> bool {
        crate::provider_policy::backfill_unlocked(
            self.fill,
            failure_excludes.union(retention_excludes),
        )
    }

    // Whether pool server `idx` is a backfill server.
    pub fn is_backfill(&self, idx: usize) -> bool {
        self.backfill.get(idx).copied().unwrap_or(false)
    }

    // Whether this server refuses this pool's credentials: health disabled it
    // for `DisableReason::AuthFailure`, that disable still runs, and the
    // server has refused the login for `HealthConfig::auth_give_up_after`
    // with no login taken and no change of its login identity or
    // configured role between ([`Self::activated`]). This
    // is the one answer to "may this server serve an article" beyond the
    // article's own exclusions and its job's retention: a refusing server
    // serves nothing, so the work it alone could serve is unservable, not
    // waiting. Before the give-up time the
    // refusal is a wait, as every other quarantine (transport, capacity, an
    // outage) is. A backfill server's refusal is the answer at once: it never
    // holds work back. Lock-free on a healthy server; reads the recovery
    // gate, which holds the same deadline the health state does.
    pub fn refuses_credentials(&self, idx: usize) -> bool {
        self.recovery_gates
            .get(idx)
            .is_some_and(|gate| gate.refuses_credentials(self.is_backfill(idx)))
    }

    // The fill servers whose run of login refusals is still a wait (see
    // [`Self::refuses_credentials`]): quarantined, of any kind, inside a run
    // not yet its give-up time old. Empty in the common case; reads one
    // atomic per server that is not quarantined.
    pub fn login_refusal_waits(&self) -> crate::ServerSet {
        self.recovery_gates
            .iter()
            .enumerate()
            .filter(|(idx, gate)| {
                !self.is_backfill(*idx) && gate.login_refusal_wait_ends().is_some()
            })
            .map(|(idx, _)| idx)
            .collect()
    }

    // This pool is now the one in use. A server whose login identity (host,
    // port, username, password) or configured role (backfill or fill) is
    // other than the last pool's ends its run of login refusals, and its
    // quarantine's probe is due at once. Called once, where the pool is
    // put in use: a pool built and then dropped, because the rebuild
    // failed, changes nothing, and the pool still in use while this one
    // was being built could not spend a probe meant for this one.
    pub fn activated(&self) {
        for (gate, (identity, backfill)) in self.recovery_gates.iter().zip(&self.setups) {
            gate.activated(*identity, *backfill);
        }
    }

    // The earliest instant a fill server's run of login refusals is to be
    // looked at: when it becomes a verdict, the one change to what a server
    // may serve that no connection outcome announces, or, already past,
    // when it became one nobody has been told of while the server refuses
    // now. A caller waits for it, then weighs the waiting work between
    // [`Self::login_refusal_verdicts_untold`] and
    // [`Self::mark_login_refusal_verdicts_told`]. None in the common case,
    // at one atomic read per server that is not quarantined.
    pub fn next_login_refusal_verdict(&self) -> Option<Instant> {
        self.recovery_gates
            .iter()
            .enumerate()
            .filter(|(idx, _)| !self.is_backfill(*idx))
            .filter_map(|(_, gate)| gate.login_refusal_verdict_due())
            .min()
    }

    // The fill servers whose run of login refusals has lasted its give-up
    // time, untold, and that refuse the login now. None in the common case.
    // The caller weighs the work that waited on them and then marks the
    // verdicts told.
    pub fn login_refusal_verdicts_untold(&self) -> Option<UntoldLoginVerdicts> {
        let untold: Vec<_> = self
            .recovery_gates
            .iter()
            .enumerate()
            .filter(|(idx, _)| !self.is_backfill(*idx))
            .filter_map(|(idx, gate)| Some((idx, gate.login_refusal_verdict_untold()?)))
            .collect();
        (!untold.is_empty()).then_some(UntoldLoginVerdicts(untold))
    }

    // Marks told, and says the give-up line once for, every verdict of
    // `untold` whose server has refused the login without a break since it
    // was found: the work weighed in between was weighed against a server
    // that refused. Any other stays untold and is found again. Every
    // watcher of the server's health hears the change. Says whether any
    // was marked.
    pub fn mark_login_refusal_verdicts_told(&self, untold: &UntoldLoginVerdicts) -> bool {
        let mut told = false;
        for (idx, verdict) in &untold.0 {
            if self.recovery_gates[*idx].mark_login_refusal_verdict_told(*verdict) {
                crate::health::warn_login_refusal_gave_up(*idx, self.login_refusal_give_up);
                told = true;
            }
        }
        told
    }

    // Server `idx` fails on transport and is quarantined for it, as an
    // outage quarantines it, whatever it was quarantined for before.
    #[cfg(any(test, feature = "test-support"))]
    pub fn quarantine_for_transport_for_test(&self, idx: usize) {
        self.recovery_gates[idx].quarantine(Duration::from_mins(5), None);
    }

    // Server `idx` refuses the login and is quarantined for it afresh,
    // whatever it was quarantined for before: the run of refusals goes on,
    // or begins and becomes the verdict after `give_up`.
    #[cfg(any(test, feature = "test-support"))]
    pub fn refuse_login_for_test(&self, idx: usize, give_up: Duration) {
        self.recovery_gates[idx].quarantine_for_credentials(Duration::from_mins(5), give_up, None);
    }

    // Server `idx`'s quarantine reaches its deadline now: its probe is due.
    #[cfg(any(test, feature = "test-support"))]
    pub fn expire_quarantine_for_test(&self, idx: usize) {
        self.recovery_gates[idx].expire_now();
    }

    // Read server `idx`'s run of login refusals against `now` instead of
    // the clock.
    #[cfg(any(test, feature = "test-support"))]
    pub fn set_login_refusal_clock(&self, idx: usize, now: Instant) {
        self.recovery_gates[idx].set_refusal_clock(now);
    }

    // Access the health tracker for observability.
    pub fn health(&self) -> &Arc<Mutex<HealthTracker>> {
        &self.health
    }

    // Server configurations (parallel to health tracker indices).
    pub fn server_configs(&self) -> &[ServerConfig] {
        &self.configs
    }

    // Each server's identity, in pool order. A server is the same one when
    // its stable id, host, port and account all match: a reorder, a
    // priority, connection-count or enable change keeps it, while a new
    // host, port or account may hold different articles and counts as a new
    // server, which nothing has asked.
    pub fn server_identities(&self) -> Vec<ServerIdentity> {
        self.stable_ids
            .iter()
            .zip(&self.configs)
            .map(|(stable, config)| ServerIdentity {
                stable_id: *stable,
                host: config.host.clone(),
                port: config.port,
                username: config.username.clone(),
            })
            .collect()
    }

    // The pool position of the server with this stable id.
    pub fn server_for_stable_id(&self, id: StableServerId) -> Option<ServerId> {
        self.stable_ids
            .iter()
            .position(|stable| *stable == id)
            .map(ServerId)
    }

    // Lend `count` of this server's connections out of the pool until the
    // loan is dropped. The budget is shared across client generations, so a
    // rebuilt pool keeps the loan.
    pub fn lend_connections(
        &self,
        server: ServerId,
        count: usize,
    ) -> Option<crate::socket_budget::ConnectionLoan> {
        let budget = self.socket_budgets.get(server.0)?.clone();
        budget.lend(count);
        Some(crate::socket_budget::ConnectionLoan::new(budget, count))
    }

    pub fn stable_server_id(&self, server: ServerId) -> Option<StableServerId> {
        self.stable_ids.get(server.0).copied()
    }

    pub fn server_transfer_control(&self, server: ServerId) -> Option<Arc<ServerTransferControl>> {
        self.transfer_controls.get(server.0).cloned().flatten()
    }

    // Whether `server`'s quota turns work away. No lock and no `Arc` clone:
    // the presentation asks it of every fill server on every article.
    pub fn server_quota_blocked(&self, server: ServerId) -> bool {
        self.transfer_controls
            .get(server.0)
            .and_then(Option::as_ref)
            .is_some_and(|control| control.quota_blocked())
    }

    pub fn try_acquire_blocking_permit(
        &self,
        server: ServerId,
    ) -> Result<BlockingConnectionPermit> {
        self.try_acquire_blocking_permit_for_work(server, true)
    }

    pub(crate) fn try_acquire_blocking_permit_for_work(
        &self,
        server: ServerId,
        demanded: bool,
    ) -> Result<BlockingConnectionPermit> {
        let idx = server.0;
        if idx >= self.semaphores.len() {
            return Err(NntpError::PoolExhausted);
        }
        let recent_failure = self.last_connect_failure[idx]
            .try_lock()
            .map_err(|_| NntpError::PoolExhausted)?;
        if self.reconnect_hold(*recent_failure).is_some() {
            return Err(NntpError::ServerRecovering);
        }
        drop(recent_failure);
        let permit = self.semaphores[idx]
            .clone()
            .try_acquire_owned()
            .map_err(|_| NntpError::PoolExhausted)?;
        let budget = &self.socket_budgets[idx];
        // A distinct error, not `PoolExhausted`: this refusal is the recovery
        // gate holding the tier open for one probe, and reading it as
        // saturation sends the operator looking for a permit leak on a server
        // that has every permit free.
        // Except for a server refusing this pool's credentials: no probe
        // is due there, and its refusal is the answer the work gets.
        let health_lease = self.recovery_gates[idx].admit(demanded).ok_or_else(|| {
            if self.refuses_credentials(idx) {
                NntpError::AuthenticationRejected
            } else {
                NntpError::ServerRecovering
            }
        })?;
        let socket_slot = budget
            .try_acquire()
            .or_else(|| {
                budget.recall_idle();
                budget.try_acquire()
            })
            .ok_or(NntpError::PoolExhausted)?;
        // Told only now that the probe has its socket: a probe given back
        // for want of one goes unannounced, and its giving back is the one
        // change its watchers hear.
        health_lease.announce();
        Ok(BlockingConnectionPermit {
            max_idle_age: self.max_idle_age,
            _permit: permit,
            socket_slot,
            health_lease: Arc::new(health_lease),
        })
    }

    // Start the reconnect delay after a fresh dial failed for the server's
    // own reasons. A dial the provider declined, or one the holdoff turned
    // away before it dialled, is paced by the over-limit holdoff alone:
    // stamping the delay for it as well would answer the next caller with
    // `ServerRecovering` instead of the holdoff, as if the server were down.
    async fn note_connect_failure(&self, idx: usize, error: &NntpError) {
        if !dial_declined_over_limit(error) {
            *self.last_connect_failure[idx].lock().await = Some(Instant::now());
        }
    }

    pub(crate) fn note_blocking_connect_failure(&self, idx: usize) {
        *self.last_connect_failure[idx].blocking_lock() = Some(Instant::now());
    }

    // `probe` says the dial is its server's recovery probe: see
    // [`AuthAdmission::before_dial`], which the async dial asks too.
    pub(crate) fn with_blocking_connect_admission<T>(
        &self,
        server: ServerId,
        probe: bool,
        connect: impl FnOnce() -> Result<T>,
    ) -> Result<T> {
        let admission = self
            .auth_admission
            .get(server.0)
            .ok_or(NntpError::PoolExhausted)?;
        admission.before_dial(probe)?;
        let result = connect();
        admission.after_dial(probe, &result)?;
        result
    }

    // The config a blocking connection to `server` dials with.
    pub fn blocking_connect_config(&self, server: ServerId) -> Result<ServerConfig> {
        self.configs
            .get(server.0)
            .cloned()
            .ok_or(NntpError::PoolExhausted)
    }

    // Returns `(available_permits, configured_connections)` for the given server.
    //
    // This is lock-free — it reads semaphore permits and the pre-stored
    // max_connections value, so it can be called from synchronous contexts.
    pub fn server_load(&self, idx: usize) -> (usize, usize) {
        (
            self.semaphores[idx].available_permits(),
            self.max_connections[idx],
        )
    }

    pub fn socket_budget_snapshot(&self, idx: usize) -> crate::socket_budget::SocketBudgetSnapshot {
        self.socket_budgets[idx].snapshot()
    }

    pub fn recovery_snapshot(&self, idx: usize) -> crate::recovery::RecoverySnapshot {
        self.recovery_gates[idx].snapshot()
    }

    // When the reconnect delay after a dial that failed at `failed_at` ends,
    // while it still runs.
    fn reconnect_hold(&self, failed_at: Option<Instant>) -> Option<Instant> {
        failed_at
            .map(|at| at + self.reconnect_delay)
            .filter(|until| *until > Instant::now())
    }

    // What holds a new connection to server `idx` back right now, if
    // anything: the reconnect delay after a failed dial, or the recovery gate
    // of a quarantined server. The permit path refuses a dial on exactly
    // these terms, so a dispatcher that reads this sends no lease that would
    // only come back refused. A connection already open is not new and is
    // not held.
    pub fn connection_hold(
        &self,
        idx: usize,
        demanded: bool,
    ) -> Option<crate::recovery::ConnectionHold> {
        use crate::recovery::ConnectionHold;
        let gate = self.recovery_gates.get(idx)?.hold(demanded);
        if gate == Some(ConnectionHold::Recovery) {
            return gate;
        }
        // Provider refusal holds fresh sockets even when recovery considers
        // the server healthy. An expired episode admits one probe only.
        let provider = self.over_limit_until_epoch_ms(ServerId(idx)).map(|until| {
            Instant::now() + Duration::from_millis(until.saturating_sub(unix_epoch_ms()))
        });
        let reconnect = self.last_connect_failure[idx]
            .try_lock()
            .ok()
            .and_then(|failed_at| self.reconnect_hold(*failed_at));
        let deadline = provider
            .into_iter()
            .chain(reconnect)
            .chain(match gate {
                Some(ConnectionHold::Until(at)) => Some(at),
                _ => None,
            })
            .max();
        if let Some(until) = deadline {
            return Some(ConnectionHold::Until(until));
        }
        if self.over_limit_until[idx].load(Ordering::Acquire) != 0 {
            return Some(if demanded {
                ConnectionHold::Probe
            } else {
                ConnectionHold::Recovery
            });
        }
        gate
    }

    // How many new demanded connections server `idx` admits right now: its
    // free permits, at most one while its recovery probe is due, and none
    // while it is held.
    pub fn new_connection_seats(&self, idx: usize) -> usize {
        match self.connection_hold(idx, true) {
            None => self.server_load(idx).0,
            Some(crate::recovery::ConnectionHold::Probe) => self.server_load(idx).0.min(1),
            Some(_) => 0,
        }
    }

    // The earliest instant a held server admits demanded work again on its
    // own, with no recovery event to announce it.
    pub fn next_connection_hold_deadline(&self) -> Option<Instant> {
        (0..self.server_count())
            .filter_map(|idx| match self.connection_hold(idx, true) {
                Some(crate::recovery::ConnectionHold::Until(at)) => Some(at),
                _ => None,
            })
            .min()
    }

    // A watch on every server's recovery gate: its `changed` completes on
    // each quarantine, probe answer or probe given back, on any server, after
    // the last time it completed. Kept for the pool's life, it misses nothing
    // between waits.
    pub fn server_health_changes(self: &Arc<Self>) -> ServerHealthChanges {
        ServerHealthChanges {
            receiver: self.health_changed.subscribe(),
            pool: Arc::downgrade(self),
        }
    }

    // Quarantine server `idx` with its deadline already past and take its
    // one probe, as a dial would. Health leaves the server ranked, as it
    // does once a quarantine's deadline has lapsed.
    #[cfg(any(test, feature = "test-support"))]
    pub fn take_recovery_probe_for_test(&self, idx: usize) -> RecoveryProbeForTest {
        let gate = &self.recovery_gates[idx];
        gate.quarantine(Duration::ZERO, None)
            .expect("an unconditional quarantine always lands");
        let probe = gate
            .admit(true)
            .expect("a lapsed quarantine admits one demanded probe");
        probe.announce();
        RecoveryProbeForTest(probe)
    }

    // Whether server `idx`'s quarantine holds every new connection now:
    // before its deadline, or while its one probe is out. Such a server
    // takes no new article until its recovery changes, so the provider
    // policy reads it as unavailable and the next priority level takes the
    // work meanwhile, as it does for a server health has disabled. A
    // quarantine whose probe is due is available, so the probe has work;
    // taking the probe is a recovery change, which the next pass sees. The
    // probe's own connection is past the hold and reads the policy without
    // it: see [`Self::provider_policy_on_connection`]. The one owner of this
    // answer for the policy; lock-free on a server that is not quarantined.
    pub(crate) fn recovery_holds_new_work(&self, idx: usize) -> bool {
        matches!(
            self.recovery_gates
                .get(idx)
                .and_then(|gate| gate.hold(true)),
            Some(
                crate::recovery::ConnectionHold::Until(_)
                    | crate::recovery::ConnectionHold::Recovery
            )
        )
    }

    pub fn requires_recovery(&self, idx: usize) -> bool {
        self.recovery_gates[idx].quarantined()
    }

    pub(crate) fn retire_quarantined_idle(&self, idx: usize) {
        if self.requires_recovery(idx) {
            for _ in 0..self.socket_budgets[idx].snapshot().physical {
                if !self.socket_budgets[idx].recall_idle() {
                    break;
                }
            }
        }
    }

    // Currently leased connections for the given server.
    pub fn active_connections(&self, idx: usize) -> usize {
        self.max_connections[idx].saturating_sub(self.semaphores[idx].available_permits())
    }

    // Take a healthy idle connection, evicting stale/poisoned ones.
    fn take_healthy_idle(&self, pool: &mut ServerPool) -> Option<NntpConnection> {
        while let Some(conn) = pool.idle.pop_front() {
            if !conn.accepts_new_work() {
                continue;
            }
            if conn.is_poisoned() {
                trace!("evicting poisoned idle connection");
                continue;
            }
            if conn.last_used().elapsed() > self.max_idle_age {
                trace!("evicting stale idle connection");
                continue;
            }
            if let Some(slot) = &conn.socket_slot {
                slot.active();
            }
            return Some(conn);
        }
        None
    }
}

// RAII guard that returns a connection to the pool on drop.
pub struct PooledConnection {
    conn: Option<NntpConnection>,
    pool: Arc<SyncMutex<ServerPool>>,
    server_idx: usize,
    return_to_pool: bool,
    shutdown: CancellationToken,
    _permit: Option<tokio::sync::OwnedSemaphorePermit>,
}

impl PooledConnection {
    pub fn remote_addr(&self) -> Option<SocketAddr> {
        self.conn
            .as_ref()
            .expect("pooled connection is present")
            .remote_addr()
    }

    pub fn remote_ip(&self) -> Option<IpAddr> {
        self.remote_addr().map(|addr| addr.ip())
    }

    // Explicitly discard this connection instead of returning it to the pool.
    // Use when the connection is in a bad state.
    pub fn discard(mut self) {
        if self.conn.take().is_some() {
            let mut pool = lock_recovering(&self.pool);
            pool.active_count = pool.active_count.saturating_sub(1);
            drop(pool);
            trace!(server = self.server_idx, "discarded connection");
        }
    }
}

impl Deref for PooledConnection {
    type Target = NntpConnection;

    fn deref(&self) -> &NntpConnection {
        self.conn.as_ref().expect("connection taken after drop")
    }
}

impl DerefMut for PooledConnection {
    fn deref_mut(&mut self) -> &mut NntpConnection {
        self.conn.as_mut().expect("connection taken after drop")
    }
}

impl Drop for PooledConnection {
    // Return the socket to the idle list here, in the drop itself.
    //
    // `_permit` is a later field, so it is released only after this body has
    // run: by the time the next acquirer can take the permit, the connection
    // it should reuse is already on the list. Handing the return to a spawned
    // task inverted that — the permit went back first and the return landed
    // whenever the runtime got to it, so a caller that re-acquired
    // immediately reliably raced past a warm socket and dialled a new one.
    fn drop(&mut self) {
        if let Some(conn) = self.conn.take() {
            let server_idx = self.server_idx;
            let healthy = conn.is_healthy() && conn.accepts_new_work();
            let poisoned = conn.is_poisoned();
            let return_to_pool = self.return_to_pool;
            // An unanswered recovery probe is never kept idle; dropped, it
            // gives the probe back. See `ConnectionHealth::may_rest_idle`.
            let may_rest = conn.may_rest_idle();

            let mut pool = lock_recovering(&self.pool);
            pool.active_count = pool.active_count.saturating_sub(1);
            if healthy && return_to_pool && !may_rest {
                trace!(server = server_idx, "dropped unanswered recovery probe");
            } else if healthy && return_to_pool {
                if self.shutdown.is_cancelled() {
                    trace!(server = server_idx, "dropped connection from shutdown pool");
                } else {
                    // Register outside the owner lock: an already-retiring route
                    // can recall synchronously. Recheck before publishing idle.
                    drop(pool);
                    if let Some(slot) = &conn.socket_slot {
                        let owner = Arc::downgrade(&self.pool);
                        slot.idle(
                            crate::socket_budget::SocketPhase::AsyncIdle,
                            Arc::new(move |id| {
                                if let Some(owner) = owner.upgrade() {
                                    // Immediate local close is bounded and acknowledges
                                    // the recall only when the transport has dropped.
                                    lock_recovering(&owner).idle.retain(|conn| {
                                        conn.socket_slot.as_ref().is_none_or(|slot| slot.id() != id)
                                    });
                                }
                            }),
                        );
                    }
                    let mut pool = lock_recovering(&self.pool);
                    if conn.accepts_new_work() && !self.shutdown.is_cancelled() {
                        pool.idle.push_back(conn);
                        trace!(server = server_idx, "returned connection to pool");
                    }
                }
            } else if healthy {
                trace!(server = server_idx, "dropped non-poolable connection");
            } else if poisoned {
                trace!(server = server_idx, "dropped poisoned connection");
            } else {
                trace!(server = server_idx, "dropped unhealthy connection");
            }
        }
    }
}

#[cfg(test)]
#[path = "pool/recovery_tests.rs"]
mod recovery_tests;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::health::{DisableReason, HealthConfig, ServerState};

    #[tokio::test]
    async fn idle_route_probe_opens_one_socket_and_preserves_provider_health() {
        use std::sync::atomic::{AtomicBool, AtomicUsize};
        use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
        use weaver_tunnel::pipe::{DialError, DialPath, Dialed, DialedStream, Dialer, Target};
        struct Probe {
            needed: AtomicBool,
            attempts: AtomicUsize,
            targets: tokio::sync::watch::Sender<Vec<u16>>,
            attempted: tokio::sync::mpsc::UnboundedSender<usize>,
            checked: tokio::sync::mpsc::UnboundedSender<()>,
        }
        #[async_trait::async_trait]
        impl Dialer for Probe {
            fn leg_targets(&self) -> Option<tokio::sync::watch::Receiver<Vec<u16>>> {
                Some(self.targets.subscribe())
            }
            fn needs_probe(&self) -> bool {
                self.checked.send(()).ok();
                self.needed.load(Ordering::Acquire)
            }
            async fn dial(&self, _: &Target) -> std::result::Result<Dialed, DialError> {
                assert!(self.needed.swap(false, Ordering::AcqRel));
                let attempt = self.attempts.fetch_add(1, Ordering::AcqRel);
                self.attempted.send(attempt).unwrap();
                if attempt == 0 {
                    return Err(DialError::Egress(std::io::Error::other("fixture leg down")));
                }
                let (stream, peer) = tokio::io::duplex(1024);
                tokio::spawn(async move {
                    let (reader, mut writer) = tokio::io::split(peer);
                    writer.write_all(b"200 ready\r\n").await.unwrap();
                    let mut lines = BufReader::new(reader).lines();
                    while let Ok(Some(line)) = lines.next_line().await {
                        let reply = if line == "CAPABILITIES" {
                            &b"101 capabilities\r\nVERSION 2\r\nREADER\r\n.\r\n"[..]
                        } else {
                            &b"500 unsupported\r\n"[..]
                        };
                        if writer.write_all(reply).await.is_err() {
                            break;
                        }
                    }
                });
                Ok(Dialed {
                    stream: DialedStream::Tunnel(Box::new(stream)),
                    outcome: Arc::new(weaver_tunnel::bridge::ConnectionOutcome::default()),
                    path: DialPath {
                        leg: Some(0),
                        ..Default::default()
                    },
                    peer: Some("127.0.0.1:119".parse().unwrap()),
                    source: None,
                    setup: None,
                })
            }
            fn budget(&self) -> Duration {
                Duration::from_secs(30)
            }
            fn describe(&self) -> String {
                "probe fixture".into()
            }
        }
        let (attempted, mut attempts) = tokio::sync::mpsc::unbounded_channel();
        let (checked, mut checks) = tokio::sync::mpsc::unbounded_channel();
        let probe = Arc::new(Probe {
            needed: AtomicBool::new(true),
            attempts: AtomicUsize::new(0),
            targets: tokio::sync::watch::channel(vec![4]).0,
            attempted,
            checked,
        });
        let mut config = test_pool_config(4);
        config.health_config.degraded_threshold = 1;
        config.servers[0].server.tls = false;
        config.servers[0].server.dialer = Arc::new(crate::route_dialer::RouteDialer {
            inner: probe.clone(),
            plan: None,
            runtime: tokio::runtime::Handle::current(),
            server: 1,
            egress_controls: Arc::new(crate::transfer::ServerTransferRegistry::new()),
        });
        let pool = Arc::new(NntpPool::new(config));
        let mut changed = pool.socket_budgets[0].subscribe();
        pool.start_route_probes();
        assert_eq!(attempts.recv().await, Some(0));
        while pool.socket_budgets[0].snapshot().physical != 0 {
            changed.changed().await.unwrap();
        }
        assert!(matches!(
            pool.health.lock().await.server(0).state(),
            ServerState::Healthy
        ));
        probe.needed.store(true, Ordering::Release);
        probe.targets.send_modify(|_| {});
        assert_eq!(attempts.recv().await, Some(1));
        while pool.socket_budgets[0].snapshot().async_idle != 1 {
            changed.changed().await.unwrap();
        }
        while checks.try_recv().is_ok() {}
        pool.start_route_probes();
        probe.targets.send_modify(|_| {});
        checks.recv().await.unwrap();
        assert_eq!(probe.attempts.load(Ordering::Acquire), 2);
        assert_eq!(pool.socket_budgets[0].snapshot().physical, 1);
        assert!(matches!(
            pool.health.lock().await.server(0).state(),
            ServerState::Healthy
        ));
        pool.shutdown().await;
    }

    #[tokio::test(start_paused = true)]
    async fn route_probe_checks_on_target_changes_and_dials_once_per_change() {
        use std::sync::atomic::{AtomicBool, AtomicUsize};
        use weaver_tunnel::pipe::{DialError, Dialed, Dialer, Target};
        struct Counting {
            needed: AtomicBool,
            checks: AtomicUsize,
            dials: AtomicUsize,
            targets: tokio::sync::watch::Sender<Vec<u16>>,
            checked: tokio::sync::mpsc::UnboundedSender<()>,
            dialed: tokio::sync::mpsc::UnboundedSender<()>,
        }
        #[async_trait::async_trait]
        impl Dialer for Counting {
            fn leg_targets(&self) -> Option<tokio::sync::watch::Receiver<Vec<u16>>> {
                Some(self.targets.subscribe())
            }
            fn needs_probe(&self) -> bool {
                self.checks.fetch_add(1, Ordering::AcqRel);
                self.checked.send(()).ok();
                self.needed.load(Ordering::Acquire)
            }
            async fn dial(&self, _: &Target) -> std::result::Result<Dialed, DialError> {
                self.dials.fetch_add(1, Ordering::AcqRel);
                self.dialed.send(()).ok();
                Err(DialError::Egress(std::io::Error::other("fixture leg down")))
            }
            fn budget(&self) -> Duration {
                Duration::from_secs(30)
            }
            fn describe(&self) -> String {
                "counting fixture".into()
            }
        }
        let (checked, mut checks) = tokio::sync::mpsc::unbounded_channel();
        let (dialed, mut dials) = tokio::sync::mpsc::unbounded_channel();
        let route = Arc::new(Counting {
            needed: AtomicBool::new(false),
            checks: AtomicUsize::new(0),
            dials: AtomicUsize::new(0),
            targets: tokio::sync::watch::channel(vec![4]).0,
            checked,
            dialed,
        });
        let mut config = test_pool_config(4);
        config.servers[0].server.tls = false;
        config.servers[0].server.dialer = Arc::new(crate::route_dialer::RouteDialer {
            inner: route.clone(),
            plan: None,
            runtime: tokio::runtime::Handle::current(),
            server: 1,
            egress_controls: Arc::new(crate::transfer::ServerTransferRegistry::new()),
        });
        let pool = Arc::new(NntpPool::new(config));
        pool.start_route_probes();
        checks.recv().await.unwrap();

        // Observe the parked loop and verify that it armed no timer.
        let mut waits = pool.probe_waits[0].subscribe();
        waits.wait_for(|state| state.0 >= 1).await.unwrap();
        assert!(waits.borrow().1.is_none());
        assert_eq!(route.checks.load(Ordering::Acquire), 1);
        assert_eq!(route.dials.load(Ordering::Acquire), 0);

        // A lapse changes the targets: one probe dial for that change, and
        // none after it while the targets stay put, even though the leg
        // still reports that it needs one.
        route.needed.store(true, Ordering::Release);
        route.targets.send_modify(|_| {});
        dials.recv().await.unwrap();
        waits.wait_for(|state| state.0 >= 2).await.unwrap();
        assert!(waits.borrow().1.is_none());
        assert_eq!(route.dials.load(Ordering::Acquire), 1);

        // The next lapse gets its own probe.
        route.targets.send_modify(|_| {});
        dials.recv().await.unwrap();
        waits.wait_for(|state| state.0 >= 3).await.unwrap();
        assert!(waits.borrow().1.is_none());
        assert_eq!(route.dials.load(Ordering::Acquire), 2);
        pool.shutdown().await;
    }

    #[test]
    fn cold_auth_admission_fans_out_then_latches_rejection() {
        let pool = NntpPool::new(test_pool_config(4));
        let attempts = std::sync::atomic::AtomicUsize::new(0);
        let (started, starts) = std::sync::mpsc::channel();
        let release = (SyncMutex::new(false), std::sync::Condvar::new());
        std::thread::scope(|scope| {
            for _ in 0..4 {
                scope.spawn(|| {
                    let result: Result<()> =
                        pool.with_blocking_connect_admission(ServerId(0), false, || {
                            attempts.fetch_add(1, Ordering::Relaxed);
                            started.send(()).unwrap();
                            let (lock, ready) = &release;
                            let _guard = ready
                                .wait_while(lock.lock().unwrap(), |open| !*open)
                                .unwrap();
                            Err(NntpError::AuthenticationRejected)
                        });
                    assert!(matches!(result, Err(NntpError::AuthenticationRejected)));
                });
            }
            let concurrent = (0..4).take_while(|_| starts.recv().is_ok()).count();
            *release.0.lock().unwrap() = true;
            release.1.notify_all();
            assert_eq!(
                concurrent, 4,
                "all cold dials must start before any finishes"
            );
        });
        assert_eq!(attempts.load(Ordering::Relaxed), 4);
        assert!(matches!(
            pool.with_blocking_connect_admission(ServerId(0), false, || {
                panic!("latched authentication failure must prevent another dial")
            }),
            Err::<(), _>(NntpError::AuthenticationRejected)
        ));
        // A late successful sibling must not erase the rejection.
        pool.auth_admission[0].finish(&Ok(()));
        assert!(pool.auth_admission[0].check().is_err());
        pool.auth_admission[0].state.store(2, Ordering::Release);
        assert!(
            pool.with_blocking_connect_admission(ServerId(0), false, || Ok(()))
                .is_ok()
        );
        let rebuilt = NntpPool::new(test_pool_config(4));
        assert!(
            rebuilt
                .with_blocking_connect_admission(ServerId(0), false, || Ok(()))
                .is_ok()
        );
    }

    // A refused login latches the admission, and the siblings it turned
    // away re-arm the latch past the quarantine's deadline. The quarantine's
    // probe still dials: turned away by the latch, it would re-arm the
    // quarantine without asking the server. Logging in, it ends the latch.
    #[test]
    fn the_recovery_probe_dials_through_a_latched_login_refusal() {
        let pool = NntpPool::new(test_pool_config(4));
        let refused: Result<()> = Err(NntpError::AuthenticationFailed);
        pool.auth_admission[0].finish(&refused);
        assert!(matches!(
            pool.with_blocking_connect_admission(ServerId(0), false, || {
                panic!("a latched refusal turns an ordinary dial away")
            }),
            Err::<(), _>(NntpError::AuthenticationRejected)
        ));
        let dials = std::sync::atomic::AtomicUsize::new(0);
        let refused_again = pool.with_blocking_connect_admission(ServerId(0), true, || {
            dials.fetch_add(1, Ordering::Relaxed);
            Err::<(), _>(NntpError::AuthenticationFailed)
        });
        assert!(matches!(
            refused_again,
            Err(NntpError::AuthenticationFailed)
        ));
        assert_eq!(dials.load(Ordering::Relaxed), 1, "the probe dialed");
        assert!(pool.auth_admission[0].check().is_err(), "still latched");
        assert!(
            pool.with_blocking_connect_admission(ServerId(0), true, || {
                dials.fetch_add(1, Ordering::Relaxed);
                Ok(())
            })
            .is_ok()
        );
        assert_eq!(dials.load(Ordering::Relaxed), 2);
        assert!(
            pool.with_blocking_connect_admission(ServerId(0), false, || Ok(()))
                .is_ok(),
            "the probe's login ends the latch"
        );
    }

    #[tokio::test]
    async fn cold_async_dials_start_before_the_first_greeting() {
        use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut config = test_pool_config(4);
        config.servers[0].server.host = "127.0.0.1".into();
        config.servers[0].server.port = listener.local_addr().unwrap().port();
        config.servers[0].server.tls = false;
        let route = planned_route(&mut config);
        let pool = Arc::new(NntpPool::new(config));
        let server = tokio::spawn(async move {
            // A serialized first handshake cannot pass: the server deliberately
            // withholds every greeting until all four sockets arrive.
            let mut sockets = Vec::new();
            for _ in 0..4 {
                sockets.push(listener.accept().await.unwrap().0);
            }
            let mut sessions = tokio::task::JoinSet::new();
            for socket in sockets {
                sessions.spawn(async move {
                    let (reader, mut writer) = socket.into_split();
                    writer.write_all(b"200 ready\r\n").await.unwrap();
                    let mut lines = BufReader::new(reader).lines();
                    while let Some(line) = lines.next_line().await.unwrap() {
                        if line == "CAPABILITIES" {
                            writer
                                .write_all(b"101 capabilities\r\nVERSION 2\r\nREADER\r\n.\r\n")
                                .await
                                .unwrap();
                        } else {
                            writer.write_all(b"500 unsupported\r\n").await.unwrap();
                        }
                    }
                });
            }
            while let Some(result) = sessions.join_next().await {
                result.unwrap();
            }
        });
        let mut dials = tokio::task::JoinSet::new();
        for _ in 0..4 {
            let pool = Arc::clone(&pool);
            dials.spawn(async move { pool.connect_server(0, None, false).await });
        }
        async {
            while let Some(result) = dials.join_next().await {
                drop(result.unwrap().unwrap());
            }
            server.await.unwrap();
        }
        .await;
        // Four cold dials at once still race the addresses only once; the
        // rest dial the address that race pinned.
        let plan = route.snapshot();
        assert_eq!(plan.races_won, 1);
        assert_eq!(
            plan.pinned.map(|address| address.ip()),
            Some(std::net::IpAddr::from([127, 0, 0, 1]))
        );
    }

    #[tokio::test]
    async fn a_session_that_never_hears_a_greeting_is_booked_against_its_address() {
        use tokio::io::AsyncWriteExt;
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut config = test_pool_config(4);
        config.servers[0].server.host = "127.0.0.1".into();
        config.servers[0].server.port = listener.local_addr().unwrap().port();
        config.servers[0].server.tls = false;
        let route = planned_route(&mut config);
        let pool = Arc::new(NntpPool::new(config));
        let server = tokio::spawn(async move {
            // The first socket is closed unanswered; the second is greeted
            // with a refusal, which is still the server answering.
            drop(listener.accept().await.unwrap().0);
            let (mut socket, _) = listener.accept().await.unwrap();
            socket.write_all(b"502 not today\r\n").await.unwrap();
        });
        let failures = || route.snapshot().addresses[0].consecutive_failures;

        assert!(pool.connect_server(0, None, false).await.is_err());
        assert_eq!(failures(), 1);

        assert!(pool.connect_server(0, None, false).await.is_err());
        assert_eq!(failures(), 0);
        server.await.unwrap();
    }

    // Route the first server over a direct leg whose address plan the test
    // reads.
    fn planned_route(config: &mut PoolConfig) -> Arc<crate::route_dialer::AddressPlanned> {
        let route = Arc::new(crate::route_dialer::AddressPlanned::new(
            "pool test".into(),
            Arc::new(weaver_tunnel::pipe::Egress::new(
                0,
                crate::egress::SocketEgress::System,
                Duration::from_secs(30),
            )),
        ));
        config.servers[0].server.dialer = Arc::new(crate::route_dialer::RouteDialer {
            inner: route.clone(),
            plan: Some(route.plan.clone()),
            egress_controls: Arc::new(crate::transfer::ServerTransferRegistry::new()),
            runtime: tokio::runtime::Handle::current(),
            server: 1,
        });
        route
    }

    fn test_pool_config(max_per_server: usize) -> PoolConfig {
        PoolConfig {
            servers: vec![ServerPoolConfig {
                server: ServerConfig {
                    host: "news.example.com".into(),
                    port: 563,
                    tls: true,
                    ..Default::default()
                },
                max_connections: max_per_server,
                group: 0,
                ..ServerPoolConfig::default()
            }],
            max_idle_age: Duration::from_mins(5),
            health_config: HealthConfig::default(),
            reconnect_delay: Duration::from_secs(1),
            stale_check_age: Duration::from_secs(30),
        }
    }

    #[test]
    fn pool_creation() {
        let pool = NntpPool::new(test_pool_config(10));
        assert_eq!(pool.server_count(), 1);
    }

    fn lane_pool_config(max_per_server: usize) -> PoolConfig {
        let mut config = test_pool_config(max_per_server);
        config.servers[0].server.tls = false;
        config
    }

    // An async caller must not queue behind download lanes that are
    // mid-transfer: nothing can be recalled, and nothing frees up until a
    // whole article has moved.
    //
    // The clock is paused, so any timer the call waited on would show as
    // elapsed time; a call that never waited leaves it where it was.
    #[tokio::test(start_paused = true)]
    async fn dispatch_permit_fails_fast_when_lanes_hold_every_connection() {
        let pool = NntpPool::new(lane_pool_config(2));
        let held = (0..2)
            .map(|_| pool.semaphores[0].clone().try_acquire_owned().unwrap())
            .collect::<Vec<_>>();
        let started = TokioInstant::now();
        let error = pool.acquire_dispatch_permit(0).await.unwrap_err();
        let waited = started.elapsed();
        assert!(matches!(error, NntpError::AcquireTimeout(0)), "{error:?}");
        assert_eq!(waited, Duration::ZERO, "must not wait");
        drop(held);
    }

    // An idle lane that can hand its socket back is still recalled, and the
    // permit that recall releases is the one the acquire takes.
    #[tokio::test]
    async fn dispatch_permit_takes_the_permit_a_recall_releases() {
        let pool = NntpPool::new(lane_pool_config(1));
        let held = Arc::new(SyncMutex::new(Some(
            pool.semaphores[0].clone().try_acquire_owned().unwrap(),
        )));
        let slot = pool.socket_budgets[0]
            .try_acquire()
            .expect("a fresh budget has a free slot");
        let releasing = Arc::clone(&held);
        slot.idle(
            crate::socket_budget::SocketPhase::OwnedIdle,
            Arc::new(move |_| {
                lock_recovering(&releasing).take();
            }),
        );
        let permit = pool
            .acquire_dispatch_permit(0)
            .await
            .expect("acquire must succeed once the recall lands");
        drop(permit);
        drop(slot);
    }

    #[test]
    #[should_panic(expected = "ServerPoolConfig stable_id must match its transfer control")]
    fn pool_rejects_mismatched_transfer_control_identity() {
        let registry = crate::transfer::ServerTransferRegistry::new();
        let control = registry.configure(
            crate::transfer::StableServerId(2),
            crate::transfer::ServerTransferConfig::default(),
        );
        let mut config = test_pool_config(1);
        config.servers[0].stable_id = crate::transfer::StableServerId(1);
        config.servers[0].transfer_control = Some(control);
        let _ = NntpPool::new(config);
    }

    #[test]
    fn pool_multi_server() {
        let config = PoolConfig {
            servers: vec![
                ServerPoolConfig {
                    server: ServerConfig {
                        host: "primary.example.com".into(),
                        ..Default::default()
                    },
                    max_connections: 10,
                    group: 0,
                    ..ServerPoolConfig::default()
                },
                ServerPoolConfig {
                    server: ServerConfig {
                        host: "backup.example.com".into(),
                        ..Default::default()
                    },
                    max_connections: 5,
                    group: 0,
                    ..ServerPoolConfig::default()
                },
            ],
            max_idle_age: Duration::from_mins(5),
            health_config: HealthConfig::default(),
            reconnect_delay: Duration::from_secs(1),
            stale_check_age: Duration::from_secs(30),
        };
        let pool = NntpPool::new(config);
        assert_eq!(pool.server_count(), 2);
    }

    #[tokio::test]
    async fn fill_servers_exhausted_ignores_backfill_servers() {
        let config = PoolConfig {
            servers: vec![
                ServerPoolConfig {
                    server: ServerConfig {
                        host: "fill.example.com".into(),
                        ..Default::default()
                    },
                    max_connections: 2,
                    group: 0,
                    ..ServerPoolConfig::default()
                },
                ServerPoolConfig {
                    server: ServerConfig {
                        host: "backfill.example.com".into(),
                        ..Default::default()
                    },
                    max_connections: 2,
                    group: 1,
                    backfill: true,
                    ..ServerPoolConfig::default()
                },
            ],
            ..PoolConfig::default()
        };
        let pool = NntpPool::new(config);
        assert!(pool.fill_servers_exhausted(crate::ServerSet::from([0])));
        assert!(!pool.fill_servers_exhausted(crate::ServerSet::EMPTY));
    }

    #[tokio::test]
    async fn pool_shutdown_idempotent() {
        let pool = NntpPool::new(test_pool_config(5));
        pool.shutdown().await;
        pool.shutdown().await;
        // Acquiring after shutdown should fail.
        let result = pool.acquire(ServerId(0)).await;
        assert!(matches!(result, Err(NntpError::PoolShutdown)));
    }

    #[tokio::test]
    async fn pool_invalid_server_id() {
        let pool = NntpPool::new(test_pool_config(5));
        let result = pool.acquire(ServerId(99)).await;
        assert!(matches!(result, Err(NntpError::PoolExhausted)));
    }

    #[tokio::test]
    async fn health_tracker_is_created() {
        let config = PoolConfig {
            servers: vec![
                ServerPoolConfig {
                    server: ServerConfig {
                        host: "a.example.com".into(),
                        ..Default::default()
                    },
                    max_connections: 5,
                    group: 0,
                    ..ServerPoolConfig::default()
                },
                ServerPoolConfig {
                    server: ServerConfig {
                        host: "b.example.com".into(),
                        ..Default::default()
                    },
                    max_connections: 5,
                    group: 0,
                    ..ServerPoolConfig::default()
                },
            ],
            max_idle_age: Duration::from_mins(5),
            health_config: HealthConfig::default(),
            reconnect_delay: Duration::from_secs(1),
            stale_check_age: Duration::from_secs(30),
        };
        let pool = NntpPool::new(config);

        let health = pool.health().lock().await;
        // Both servers should start healthy.
        assert!(matches!(health.server(0).state(), ServerState::Healthy));
        assert!(matches!(health.server(1).state(), ServerState::Healthy));
    }

    #[tokio::test]
    async fn reconnect_throttle_delays() {
        // Set up a pool with a short reconnect delay.
        let config = PoolConfig {
            servers: vec![ServerPoolConfig {
                server: ServerConfig {
                    host: "nonexistent.invalid".into(),
                    port: 9999,
                    tls: false,
                    connect_timeout: Duration::from_millis(50),
                    ..Default::default()
                },
                max_connections: 2,
                group: 0,
                ..ServerPoolConfig::default()
            }],
            max_idle_age: Duration::from_mins(5),
            health_config: HealthConfig::default(),
            reconnect_delay: Duration::from_millis(200),
            stale_check_age: Duration::from_secs(30),
        };
        let pool = NntpPool::new(config);

        // First acquire will fail because the server doesn't exist,
        // but it will record the failure timestamp.
        let result1 = pool.acquire(ServerId(0)).await;
        assert!(result1.is_err());
        // However long that took, the next acquire must still find the
        // throttle active: date the failure where the clock cannot pass it,
        // so the acquire waits the whole delay.
        *pool.last_connect_failure[0].lock().await =
            Some(Instant::now() + Duration::from_secs(3600));

        // Second acquire should sleep through the throttle, then attempt
        // a real connection (which also fails — but NOT with ServiceUnavailable).
        let start = Instant::now();
        let result2 = pool.acquire(ServerId(0)).await;
        assert!(result2.is_err());
        assert!(
            !matches!(result2, Err(NntpError::ServiceUnavailable)),
            "pool should sleep through throttle, not return ServiceUnavailable"
        );
        assert!(
            start.elapsed() >= Duration::from_millis(200),
            "expected throttle to delay the acquire"
        );
    }

    #[tokio::test]
    async fn reconnect_throttle_clears_after_delay() {
        let config = PoolConfig {
            servers: vec![ServerPoolConfig {
                server: ServerConfig {
                    host: "nonexistent.invalid".into(),
                    port: 9999,
                    tls: false,
                    connect_timeout: Duration::from_millis(50),
                    ..Default::default()
                },
                max_connections: 2,
                group: 0,
                ..ServerPoolConfig::default()
            }],
            max_idle_age: Duration::from_mins(5),
            health_config: HealthConfig::default(),
            reconnect_delay: Duration::from_millis(50),
            stale_check_age: Duration::from_secs(30),
        };
        let pool = NntpPool::new(config);

        // First acquire fails and records timestamp.
        let _ = pool.acquire(ServerId(0)).await;

        // Date the failure a full delay back rather than waiting it out.
        let recorded = pool.last_connect_failure[0]
            .lock()
            .await
            .expect("the failed acquire records its failure");
        *pool.last_connect_failure[0].lock().await = Some(recorded - Duration::from_millis(50));

        // Should now attempt a real connection again (will fail, but NOT with ServiceUnavailable).
        let result = pool.acquire(ServerId(0)).await;
        assert!(
            !matches!(result, Err(NntpError::ServiceUnavailable)),
            "expected a real connection error after delay elapsed, not ServiceUnavailable"
        );
    }

    #[test]
    fn pool_config_default() {
        let config = PoolConfig::default();
        assert!(config.servers.is_empty());
        assert_eq!(config.max_idle_age, Duration::from_mins(5));
        assert_eq!(config.reconnect_delay, Duration::from_secs(1));
        assert_eq!(config.stale_check_age, Duration::from_secs(30));
    }

    #[tokio::test]
    async fn health_exposed_via_accessor() {
        let pool = NntpPool::new(test_pool_config(5));
        let health = pool.health();
        let h = health.lock().await;
        // Server 0 should be healthy.
        let ordered = {
            drop(h);
            let mut h2 = health.lock().await;
            h2.ordered_servers()
        };
        assert_eq!(ordered, vec![0]);
    }

    #[test]
    fn server_load_initial() {
        let pool = NntpPool::new(test_pool_config(10));
        let (available, max) = pool.server_load(0);
        // No connections acquired yet, so all permits should be available.
        assert_eq!(available, 10);
        assert_eq!(max, 10);
    }

    #[test]
    fn server_load_multi_server() {
        let config = PoolConfig {
            servers: vec![
                ServerPoolConfig {
                    server: ServerConfig {
                        host: "a.example.com".into(),
                        ..Default::default()
                    },
                    max_connections: 10,
                    group: 0,
                    ..ServerPoolConfig::default()
                },
                ServerPoolConfig {
                    server: ServerConfig {
                        host: "b.example.com".into(),
                        ..Default::default()
                    },
                    max_connections: 5,
                    group: 1,
                    ..ServerPoolConfig::default()
                },
            ],
            max_idle_age: Duration::from_mins(5),
            health_config: HealthConfig::default(),
            reconnect_delay: Duration::from_secs(1),
            stale_check_age: Duration::from_secs(30),
        };
        let pool = NntpPool::new(config);

        let (avail0, max0) = pool.server_load(0);
        assert_eq!(avail0, 10);
        assert_eq!(max0, 10);

        let (avail1, max1) = pool.server_load(1);
        assert_eq!(avail1, 5);
        assert_eq!(max1, 5);
    }

    #[tokio::test]
    async fn provider_rejection_parks_fresh_connects_for_that_server_only() {
        use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
        use tokio::net::TcpListener;

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let accepted = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let accepted_by_server = Arc::clone(&accepted);
        let server = tokio::spawn(async move {
            loop {
                let (socket, _) = listener.accept().await.unwrap();
                let slot = accepted_by_server.fetch_add(1, Ordering::SeqCst);
                tokio::spawn(async move {
                    let (reader, mut writer) = socket.into_split();
                    if slot >= 2 {
                        let _ = writer.write_all(b"502 Too Many Connections\r\n").await;
                        return;
                    }
                    writer
                        .write_all(b"200 test server ready\r\n")
                        .await
                        .unwrap();
                    let mut lines = BufReader::new(reader).lines();
                    while let Some(line) = lines.next_line().await.unwrap() {
                        if line == "CAPABILITIES" {
                            writer
                                .write_all(b"101 Capability list:\r\nVERSION 2\r\nREADER\r\n.\r\n")
                                .await
                                .unwrap();
                        } else if line.starts_with("BODY ") {
                            writer
                                .write_all(b"222 0 <test> body follows\r\npayload\r\n.\r\n")
                                .await
                                .unwrap();
                        } else if line == "QUIT" {
                            let _ = writer.write_all(b"205 closing\r\n").await;
                            return;
                        } else {
                            writer.write_all(b"500 unsupported\r\n").await.unwrap();
                        }
                    }
                });
            }
        });

        let mut config = test_pool_config(8);
        config.servers[0].server.host = "127.0.0.1".into();
        config.servers[0].server.port = port;
        config.servers[0].server.tls = false;
        config.servers.push(ServerPoolConfig {
            server: ServerConfig {
                host: "backup.example.com".into(),
                ..Default::default()
            },
            max_connections: 4,
            group: 0,
            ..ServerPoolConfig::default()
        });
        let pool = NntpPool::new(config);
        let attempts = tokio::join!(
            pool.acquire(ServerId(0)),
            pool.acquire(ServerId(0)),
            pool.acquire(ServerId(0)),
            pool.acquire(ServerId(0)),
            pool.acquire(ServerId(0)),
            pool.acquire(ServerId(0)),
            pool.acquire(ServerId(0)),
            pool.acquire(ServerId(0)),
        );
        let mut accepted_lanes: Vec<_> = [
            attempts.0, attempts.1, attempts.2, attempts.3, attempts.4, attempts.5, attempts.6,
            attempts.7,
        ]
        .into_iter()
        .filter_map(Result::ok)
        .collect();
        assert_eq!(accepted_lanes.len(), 2);
        // The provider refused connections, not the configuration.
        assert_eq!(pool.configured_connections(ServerId(0)), Some(8));
        assert_eq!(pool.server_load(0), (6, 8));
        assert!(pool.is_over_limit(ServerId(0)));

        let accepted_after_rejection = accepted.load(Ordering::SeqCst);
        assert!(matches!(
            pool.acquire(ServerId(0)).await,
            Err(NntpError::ServerOverLimit { .. })
        ));
        assert_eq!(
            accepted.load(Ordering::SeqCst),
            accepted_after_rejection,
            "a held-off server must not open another socket"
        );

        // Established lanes are untouched by the holdoff.
        for lane in &mut accepted_lanes {
            let body = lane.body_by_id("<test>").await.unwrap();
            assert_eq!(body.data.as_ref(), b"payload\r\n");
        }

        // Returning a lane leaves an idle connection, and idle leases never
        // consult the holdoff.
        drop(accepted_lanes.pop());
        for _ in 0..1000 {
            if !lock_recovering(&pool.pools[0]).idle.is_empty() {
                break;
            }
            tokio::task::yield_now().await;
        }
        let mut reused = pool.acquire(ServerId(0)).await.unwrap();
        let body = reused.body_by_id("<test>").await.unwrap();
        assert_eq!(body.data.as_ref(), b"payload\r\n");
        assert_eq!(accepted.load(Ordering::SeqCst), accepted_after_rejection);

        // A peer server that never rejected anything is unaffected.
        assert!(!pool.is_over_limit(ServerId(1)));
        assert_eq!(pool.server_load(1), (4, 4));

        server.abort();
    }

    // A dial the server accepted and then dropped is the server's health,
    // however many other sockets are busy: it never arms the over-limit
    // holdoff, and the next dial goes out once the ordinary reconnect delay
    // has passed. Only the explicit reply above parks fresh connects.
    #[tokio::test(start_paused = true)]
    async fn dropped_dial_beside_busy_sockets_waits_the_reconnect_delay_not_the_holdoff() {
        use std::sync::atomic::AtomicUsize;
        use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
        use weaver_tunnel::pipe::{DialError, DialPath, Dialed, DialedStream, Dialer, Target};
        struct DropsAfterTwo {
            dials: AtomicUsize,
        }
        #[async_trait::async_trait]
        impl Dialer for DropsAfterTwo {
            async fn dial(&self, _: &Target) -> std::result::Result<Dialed, DialError> {
                let dial = self.dials.fetch_add(1, Ordering::AcqRel);
                let (stream, peer) = tokio::io::duplex(1024);
                if dial < 2 {
                    tokio::spawn(async move {
                        let (reader, mut writer) = tokio::io::split(peer);
                        writer.write_all(b"200 ready\r\n").await.unwrap();
                        let mut lines = BufReader::new(reader).lines();
                        while let Ok(Some(line)) = lines.next_line().await {
                            let reply = if line == "CAPABILITIES" {
                                &b"101 capabilities\r\nVERSION 2\r\nREADER\r\n.\r\n"[..]
                            } else {
                                &b"500 unsupported\r\n"[..]
                            };
                            if writer.write_all(reply).await.is_err() {
                                break;
                            }
                        }
                    });
                } else {
                    // Accepted, then dropped before the greeting.
                    drop(peer);
                }
                Ok(Dialed {
                    stream: DialedStream::Tunnel(Box::new(stream)),
                    outcome: Arc::new(weaver_tunnel::bridge::ConnectionOutcome::default()),
                    path: DialPath {
                        leg: Some(0),
                        ..Default::default()
                    },
                    peer: Some("127.0.0.1:119".parse().unwrap()),
                    source: None,
                    setup: None,
                })
            }
            fn budget(&self) -> Duration {
                Duration::from_secs(30)
            }
            fn describe(&self) -> String {
                "dropping fixture".into()
            }
        }
        let route = Arc::new(DropsAfterTwo {
            dials: AtomicUsize::new(0),
        });
        let mut config = test_pool_config(4);
        // Far longer than any real time the test can take, so the delay is
        // always still running when the next acquire checks it.
        config.reconnect_delay = Duration::from_secs(3600);
        config.servers[0].server.tls = false;
        config.servers[0].server.dialer = Arc::new(crate::route_dialer::RouteDialer {
            inner: route.clone(),
            plan: None,
            runtime: tokio::runtime::Handle::current(),
            server: 1,
            egress_controls: Arc::new(crate::transfer::ServerTransferRegistry::new()),
        });
        let pool = NntpPool::new(config);

        // Two lanes are leased and held: busy sockets on this server.
        let busy = [
            pool.acquire(ServerId(0)).await.unwrap(),
            pool.acquire(ServerId(0)).await.unwrap(),
        ];
        assert_eq!(pool.socket_budgets[0].snapshot().physical, 2);

        let Err(dropped) = pool.acquire(ServerId(0)).await else {
            panic!("the dropped dial must fail");
        };
        assert!(
            matches!(
                &dropped,
                NntpError::ConnectionClosed | NntpError::Io(_) | NntpError::Tls(_)
            ),
            "{dropped:?}"
        );
        assert!(!pool.is_over_limit(ServerId(0)));
        assert_eq!(pool.over_limit_refusals(ServerId(0)), 0);
        assert!(pool.last_connect_failure[0].lock().await.is_some());

        // The next dial waits out the reconnect delay on the paused clock,
        // then goes out instead of meeting a holdoff.
        let started = TokioInstant::now();
        let Err(next) = pool.acquire(ServerId(0)).await else {
            panic!("the next dial is dropped too");
        };
        assert!(
            !matches!(next, NntpError::ServerOverLimit { .. }),
            "{next:?}"
        );
        assert!(
            !started.elapsed().is_zero(),
            "must wait the reconnect delay"
        );
        assert_eq!(route.dials.load(Ordering::Acquire), 4);
        assert!(!pool.is_over_limit(ServerId(0)));
        assert_eq!(pool.over_limit_refusals(ServerId(0)), 0);

        drop(busy);
        pool.shutdown().await;
    }

    #[tokio::test]
    async fn body_availability_treats_saturated_server_as_eligible() {
        let pool = NntpPool::new(test_pool_config(1));
        let _permit = pool
            .try_acquire_blocking_permit(ServerId(0))
            .expect("configured lane should be acquirable");

        assert_eq!(
            pool.body_server_availability(crate::ServerSet::EMPTY, crate::ServerSet::EMPTY, 0)
                .await,
            BodyServerAvailability::Eligible
        );
    }

    #[tokio::test]
    async fn body_availability_respects_fill_backfill_and_health_deadlines() {
        let mut config = test_pool_config(1);
        config.servers.push(ServerPoolConfig {
            server: ServerConfig {
                host: "backup.example.com".into(),
                ..Default::default()
            },
            stable_id: StableServerId(2),
            max_connections: 1,
            backfill: true,
            ..ServerPoolConfig::default()
        });
        let pool = NntpPool::new(config);
        pool.health()
            .lock()
            .await
            .record_cooldown(0, crate::health::CooldownReason::Transport);

        assert!(matches!(
            pool.body_server_availability(crate::ServerSet::EMPTY, crate::ServerSet::EMPTY, 0).await,
            BodyServerAvailability::WaitingUntil(delay) if !delay.is_zero()
        ));
        assert_eq!(
            pool.body_server_availability(crate::ServerSet::from([0]), crate::ServerSet::EMPTY, 0)
                .await,
            BodyServerAvailability::Eligible
        );
        assert_eq!(
            pool.body_server_availability(crate::ServerSet::EMPTY, crate::ServerSet::from([0]), 0)
                .await,
            BodyServerAvailability::Eligible
        );
    }

    #[tokio::test]
    async fn body_availability_keeps_degraded_servers_and_waits_for_disabled_servers() {
        let pool = NntpPool::new(test_pool_config(1));
        {
            let mut health = pool.health().lock().await;
            for _ in 0..3 {
                health.record_failure(0, false);
            }
        }
        assert_eq!(
            pool.body_server_availability(crate::ServerSet::EMPTY, crate::ServerSet::EMPTY, 0)
                .await,
            BodyServerAvailability::Eligible
        );

        pool.health().lock().await.record_failure(0, true);
        assert!(matches!(
            pool.body_server_availability(crate::ServerSet::EMPTY, crate::ServerSet::EMPTY, 0).await,
            BodyServerAvailability::WaitingUntil(delay) if !delay.is_zero()
        ));
    }

    // Backfill fills only articles the fill servers do not have. A fill
    // server refusing this pool's credentials, a short cooldown and a
    // consecutive-failure outage all leave the backfill tier closed.
    #[tokio::test]
    async fn body_availability_never_unlocks_backfill_for_an_unavailable_fill_server() {
        let fill_plus_backfill = || {
            let mut config = test_pool_config(1);
            config.servers.push(ServerPoolConfig {
                server: ServerConfig {
                    host: "backup.example.com".into(),
                    ..Default::default()
                },
                stable_id: StableServerId(2),
                max_connections: 1,
                backfill: true,
                ..ServerPoolConfig::default()
            });
            config
        };
        let disabled = NntpPool::new(fill_plus_backfill());
        disabled.health().lock().await.record_failure(0, true);
        assert!(
            matches!(
                disabled
                    .body_server_availability(crate::ServerSet::EMPTY, crate::ServerSet::EMPTY, 0)
                    .await,
                BodyServerAvailability::WaitingUntil(_)
            ),
            "a fill server refusing credentials must not open backfill"
        );
        assert!(!disabled.fill_servers_exhausted(crate::ServerSet::EMPTY));
        assert_eq!(
            disabled
                .body_server_availability(crate::ServerSet::from([0]), crate::ServerSet::EMPTY, 0)
                .await,
            BodyServerAvailability::Eligible,
            "an article the fill server does not have still reaches backfill"
        );

        let cooling = NntpPool::new(fill_plus_backfill());
        cooling
            .health()
            .lock()
            .await
            .record_cooldown(0, crate::health::CooldownReason::Transport);
        assert!(
            matches!(
                cooling.body_server_availability(crate::ServerSet::EMPTY, crate::ServerSet::EMPTY, 0).await,
                BodyServerAvailability::WaitingUntil(delay) if !delay.is_zero()
            ),
            "a 5-10s cooldown must wait, not spill onto backfill"
        );
        assert!(
            !cooling.fill_servers_exhausted(crate::ServerSet::EMPTY),
            "CoolingDown must not open the backfill gate"
        );

        let outage = NntpPool::new(fill_plus_backfill());
        {
            let mut health = outage.health().lock().await;
            for _ in 0..HealthConfig::default().disable_threshold {
                health.record_failure(0, false);
            }
            assert!(
                matches!(
                    health.server(0).state(),
                    ServerState::Disabled {
                        reason: DisableReason::ConsecutiveFailures,
                        ..
                    }
                ),
                "test setup: server 0 must be outage-disabled"
            );
        }
        assert!(
            matches!(
                outage.body_server_availability(crate::ServerSet::EMPTY, crate::ServerSet::EMPTY, 0).await,
                BodyServerAvailability::WaitingUntil(delay) if !delay.is_zero()
            ),
            "an outage disable heals on its own and must wait, not spill onto backfill"
        );
        assert!(
            !outage.fill_servers_exhausted(crate::ServerSet::EMPTY),
            "ConsecutiveFailures must not open the backfill gate"
        );
    }

    #[tokio::test]
    async fn body_availability_reports_quota_block_without_health_deadline() {
        let stable_id = StableServerId(1);
        let registry = crate::transfer::ServerTransferRegistry::new();
        let control = registry.configure(
            stable_id,
            crate::transfer::ServerTransferConfig {
                quota: Some(crate::transfer::QuotaRuntimeConfig {
                    limit_bytes: 0,
                    generation: 1,
                    retry_at: None,
                }),
                ..Default::default()
            },
        );
        let mut config = test_pool_config(1);
        config.servers[0].stable_id = stable_id;
        config.servers[0].transfer_control = Some(control);
        let pool = NntpPool::new(config);

        assert_eq!(
            pool.body_server_availability(crate::ServerSet::EMPTY, crate::ServerSet::EMPTY, 0)
                .await,
            BodyServerAvailability::Blocked
        );
    }

    #[tokio::test]
    async fn over_limit_holdoff_expires_and_fresh_connects_resume() {
        let pool = NntpPool::new(test_pool_config(4));
        pool.note_provider_over_limit(ServerId(0));
        assert!(pool.is_over_limit(ServerId(0)));

        // Rewind the deadline instead of sleeping ten minutes; the holdoff is
        // a wall-clock comparison, so this is the same state the pool reaches
        // on its own when the window ends.
        pool.over_limit_until[0].store(unix_epoch_ms() - 1, Ordering::Release);

        assert!(!pool.is_over_limit(ServerId(0)));
        assert_eq!(pool.over_limit_until_epoch_ms(ServerId(0)), None);
        // Connects are attempted again: the unreachable test host now fails on
        // the socket rather than on the holdoff.
        assert!(!matches!(
            pool.acquire(ServerId(0)).await,
            Err(NntpError::ServerOverLimit { .. })
        ));
        // A socket failure is not a refusal: the probe slot is released, so
        // the next caller may ask again instead of waiting out the window.
        assert_eq!(pool.over_limit_episode(0).unwrap().probe_started, 0);
        assert!(!matches!(
            pool.acquire(ServerId(0)).await,
            Err(NntpError::ServerOverLimit { .. })
        ));
        // The episode is still open (nothing was admitted), so a further
        // refusal continues the doubling rather than starting over.
        assert_eq!(pool.over_limit_refusals(ServerId(0)), 1);
    }

    #[test]
    fn over_limit_holdoff_doubles_from_thirty_seconds_to_ten_minutes() {
        assert_eq!(over_limit_holdoff(0), Duration::from_secs(30));
        assert_eq!(over_limit_holdoff(1), Duration::from_secs(30));
        assert_eq!(over_limit_holdoff(2), Duration::from_secs(60));
        assert_eq!(over_limit_holdoff(3), Duration::from_secs(120));
        assert_eq!(over_limit_holdoff(4), Duration::from_secs(240));
        assert_eq!(over_limit_holdoff(5), Duration::from_secs(480));
        assert_eq!(over_limit_holdoff(6), Duration::from_secs(600));
        assert_eq!(over_limit_holdoff(7), Duration::from_secs(600));
        assert_eq!(over_limit_holdoff(u32::MAX), Duration::from_secs(600));
    }

    // The first refusal buys a short pause; when it ends, one caller is let
    // through to ask the provider and everyone else keeps waiting on that
    // answer. A refused probe doubles the pause.
    #[test]
    fn one_probe_follows_the_holdoff_and_a_refused_probe_doubles_it() {
        let pool = NntpPool::new(test_pool_config(4));
        let before = unix_epoch_ms();
        pool.note_provider_over_limit(ServerId(0));
        let deadline = pool.over_limit_until_epoch_ms(ServerId(0)).unwrap();
        assert!(deadline >= before + 30_000 && deadline <= unix_epoch_ms() + 30_000);
        assert_eq!(pool.over_limit_refusals(ServerId(0)), 1);
        assert!(matches!(
            pool.admit_fresh_connect(ServerId(0)),
            Err(NntpError::ServerOverLimit { until_epoch_ms }) if until_epoch_ms == deadline
        ));

        // The pause ends.
        pool.over_limit_until[0].store(unix_epoch_ms() - 1, Ordering::Release);
        assert!(!pool.is_over_limit(ServerId(0)));
        // Exactly one caller is admitted as the probe...
        assert!(pool.admit_fresh_connect(ServerId(0)).is_ok());
        // ...and the rest are held for as long as that probe may take.
        let held = pool.admit_fresh_connect(ServerId(0));
        assert!(
            matches!(held, Err(NntpError::ServerOverLimit { .. })),
            "{held:?}"
        );
        assert!(
            pool.is_over_limit(ServerId(0)),
            "a server with a probe out still reads as held off"
        );

        // The probe is refused: a longer pause, counted against the episode.
        pool.note_provider_over_limit(ServerId(0));
        assert_eq!(pool.over_limit_refusals(ServerId(0)), 2);
        let second = pool.over_limit_until_epoch_ms(ServerId(0)).unwrap();
        assert!(second >= unix_epoch_ms() + 59_000 && second <= unix_epoch_ms() + 60_000);
        assert_eq!(pool.over_limit_episode(0).unwrap().probe_started, 0);

        // A third window is longer again.
        pool.over_limit_until[0].store(unix_epoch_ms() - 1, Ordering::Release);
        assert!(pool.admit_fresh_connect(ServerId(0)).is_ok());
        pool.note_provider_over_limit(ServerId(0));
        assert_eq!(pool.over_limit_refusals(ServerId(0)), 3);
        let third = pool.over_limit_until_epoch_ms(ServerId(0)).unwrap();
        assert!(third >= unix_epoch_ms() + 119_000 && third <= unix_epoch_ms() + 120_000);
    }

    // The provider accepting a socket ends the episode outright: no holdoff,
    // no probe slot, and the next refusal starts again from the short pause.
    #[test]
    fn an_admitted_connect_clears_the_holdoff_and_resets_the_backoff() {
        let pool = NntpPool::new(test_pool_config(4));
        pool.note_provider_over_limit(ServerId(0));
        pool.over_limit_until[0].store(unix_epoch_ms() - 1, Ordering::Release);
        assert!(pool.admit_fresh_connect(ServerId(0)).is_ok());
        pool.note_provider_over_limit(ServerId(0));
        assert_eq!(pool.over_limit_refusals(ServerId(0)), 2);

        pool.over_limit_until[0].store(unix_epoch_ms() - 1, Ordering::Release);
        let probe = pool.admit_fresh_connect(ServerId(0)).unwrap();
        assert!(matches!(probe, FreshConnectAdmission::Probe { .. }));
        pool.note_fresh_connect_outcome::<()>(ServerId(0), probe, &Ok(()));

        assert!(!pool.is_over_limit(ServerId(0)));
        assert_eq!(pool.over_limit_until_epoch_ms(ServerId(0)), None);
        assert_eq!(pool.over_limit_refusals(ServerId(0)), 0);
        assert_eq!(pool.over_limit_episode(0).unwrap().probe_started, 0);
        // Two callers in a row are admitted: nothing is probing any more.
        assert!(matches!(
            pool.admit_fresh_connect(ServerId(0)),
            Ok(FreshConnectAdmission::Open)
        ));
        assert!(matches!(
            pool.admit_fresh_connect(ServerId(0)),
            Ok(FreshConnectAdmission::Open)
        ));

        // A new refusal is a new episode, back at the shortest pause.
        pool.note_provider_over_limit(ServerId(0));
        assert_eq!(pool.over_limit_refusals(ServerId(0)), 1);
        let deadline = pool.over_limit_until_epoch_ms(ServerId(0)).unwrap();
        assert!(deadline <= unix_epoch_ms() + 30_000);
    }

    // A connect admitted while nothing was held off, that completes only
    // after a refusal has armed the holdoff, proves nothing: the provider
    // accepted it before it began refusing. The holdoff stands.
    #[test]
    fn a_connect_that_was_already_dialling_does_not_clear_a_new_holdoff() {
        let pool = NntpPool::new(test_pool_config(4));
        let admission = pool.admit_fresh_connect(ServerId(0)).unwrap();
        assert_eq!(admission, FreshConnectAdmission::Open);

        pool.note_provider_over_limit(ServerId(0));
        pool.note_fresh_connect_outcome::<()>(ServerId(0), admission, &Ok(()));

        assert!(pool.is_over_limit(ServerId(0)));
        assert_eq!(pool.over_limit_refusals(ServerId(0)), 1);
        assert!(matches!(
            pool.admit_fresh_connect(ServerId(0)),
            Err(NntpError::ServerOverLimit { .. })
        ));

        // Its failing for any other reason frees no probe slot either: after
        // the pause the probe is still exactly one caller.
        pool.note_fresh_connect_outcome::<()>(ServerId(0), admission, &Err(NntpError::Timeout));
        pool.over_limit_until[0].store(unix_epoch_ms() - 1, Ordering::Release);
        assert!(matches!(
            pool.admit_fresh_connect(ServerId(0)),
            Ok(FreshConnectAdmission::Probe { .. })
        ));
        pool.note_fresh_connect_outcome::<()>(
            ServerId(0),
            FreshConnectAdmission::Open,
            &Err(NntpError::Timeout),
        );
        assert!(pool.admit_fresh_connect(ServerId(0)).is_err());
    }

    // A probe only settles the episode while the slot is still its own. A
    // refusal booked after it went out resets the slot and starts a longer
    // pause; the probe's late success neither clears that pause nor does
    // its late failure free the slot a later probe holds.
    #[test]
    fn a_superseded_probe_neither_clears_nor_frees_anything() {
        let pool = NntpPool::new(test_pool_config(4));
        pool.note_provider_over_limit(ServerId(0));
        pool.over_limit_until[0].store(unix_epoch_ms() - 1, Ordering::Release);
        let first = pool.admit_fresh_connect(ServerId(0)).unwrap();
        assert!(matches!(first, FreshConnectAdmission::Probe { .. }));

        // A connect that was already dialling is refused meanwhile.
        pool.note_provider_over_limit(ServerId(0));
        assert_eq!(pool.over_limit_refusals(ServerId(0)), 2);
        let deadline = pool.over_limit_until_epoch_ms(ServerId(0)).unwrap();

        pool.note_provider_admitted(ServerId(0), first);
        assert_eq!(pool.over_limit_until_epoch_ms(ServerId(0)), Some(deadline));
        assert_eq!(pool.over_limit_refusals(ServerId(0)), 2);

        // The pause ends and a second probe goes out; the first probe's late
        // failure report leaves that slot alone.
        pool.over_limit_until[0].store(unix_epoch_ms() - 1, Ordering::Release);
        let second = pool.admit_fresh_connect(ServerId(0)).unwrap();
        assert!(matches!(second, FreshConnectAdmission::Probe { .. }));
        pool.release_over_limit_probe(ServerId(0), first);
        assert!(pool.admit_fresh_connect(ServerId(0)).is_err());
        pool.release_over_limit_probe(ServerId(0), second);
        assert!(pool.admit_fresh_connect(ServerId(0)).is_ok());
    }

    // An episode nobody probed — the queue drained during the pause — does
    // not carry its refusal count into the next one. A refusal long after
    // the last pause ended starts again from the shortest pause.
    #[test]
    fn a_refusal_long_after_the_last_pause_starts_a_new_episode() {
        let pool = NntpPool::new(test_pool_config(4));
        pool.note_provider_over_limit(ServerId(0));
        pool.over_limit_until[0].store(unix_epoch_ms() - 1, Ordering::Release);
        assert!(pool.admit_fresh_connect(ServerId(0)).is_ok());
        pool.note_provider_over_limit(ServerId(0));
        assert_eq!(pool.over_limit_refusals(ServerId(0)), 2);

        // The 60 s pause ended more than the longest holdoff ago.
        pool.over_limit_until[0].store(
            unix_epoch_ms() - duration_to_epoch_ms(OVER_LIMIT_HOLDOFF_MAX) - 1_000,
            Ordering::Release,
        );
        assert!(!pool.is_over_limit(ServerId(0)));
        assert!(matches!(
            pool.admit_fresh_connect(ServerId(0)),
            Ok(FreshConnectAdmission::Probe { .. })
        ));
        pool.note_provider_over_limit(ServerId(0));
        assert_eq!(pool.over_limit_refusals(ServerId(0)), 1);
        let deadline = pool.over_limit_until_epoch_ms(ServerId(0)).unwrap();
        assert!(deadline <= unix_epoch_ms() + 30_000);
    }

    // A probe whose caller never reported back does not hold the server
    // closed for good: after the probe window another caller may ask.
    #[test]
    fn a_stale_probe_slot_is_taken_over() {
        let pool = NntpPool::new(test_pool_config(4));
        pool.note_provider_over_limit(ServerId(0));
        pool.over_limit_until[0].store(unix_epoch_ms() - 1, Ordering::Release);
        assert!(pool.admit_fresh_connect(ServerId(0)).is_ok());
        assert!(pool.admit_fresh_connect(ServerId(0)).is_err());

        pool.over_limit_episode(0).unwrap().probe_started =
            unix_epoch_ms() - duration_to_epoch_ms(OVER_LIMIT_PROBE_WINDOW) - 1;
        assert!(!pool.is_over_limit(ServerId(0)));
        assert!(pool.admit_fresh_connect(ServerId(0)).is_ok());
        assert!(pool.admit_fresh_connect(ServerId(0)).is_err());
    }

    #[test]
    fn blocking_connect_warnings_report_once_a_window_and_carry_the_count() {
        let mut config = test_pool_config(4);
        config.servers.push(test_pool_config(4).servers.remove(0));
        let pool = NntpPool::new(config);

        // The first failure of a window reports itself.
        assert_eq!(pool.note_blocking_connect_warning(ServerId(0)), Some(1));
        // The rest are counted and stay quiet: a server that refuses one
        // connect refuses the next dispatch pass's too.
        assert_eq!(pool.note_blocking_connect_warning(ServerId(0)), None);
        assert_eq!(pool.note_blocking_connect_warning(ServerId(0)), None);
        // Each server has its own window.
        assert_eq!(pool.note_blocking_connect_warning(ServerId(1)), Some(1));

        // Rewind the window rather than sleeping a minute; the throttle is a
        // wall-clock comparison, so this is the state it reaches on its own.
        pool.blocking_connect_warn_after[0].store(unix_epoch_ms() - 1, Ordering::Release);
        assert_eq!(
            pool.note_blocking_connect_warning(ServerId(0)),
            Some(3),
            "the next warning stands for the two it suppressed and itself"
        );
        assert_eq!(pool.note_blocking_connect_warning(ServerId(0)), None);
    }

    #[test]
    fn repeated_rejections_never_reduce_the_configured_connection_count() {
        let pool = NntpPool::new(test_pool_config(8));

        pool.note_provider_over_limit(ServerId(0));
        let first_deadline = pool.over_limit_until_epoch_ms(ServerId(0)).unwrap();
        for _ in 0..32 {
            pool.note_provider_over_limit(ServerId(0));
        }

        assert_eq!(pool.configured_connections(ServerId(0)), Some(8));
        assert_eq!(pool.server_load(0), (8, 8));
        assert_eq!(pool.fill_connection_capacity(), 8);
        // Rejections inside an armed window neither extend it nor re-report it.
        assert_eq!(
            pool.over_limit_until_epoch_ms(ServerId(0)),
            Some(first_deadline)
        );
    }

    #[tokio::test]
    async fn provider_hold_seats_only_one_probe_and_announces_its_answer() {
        use crate::recovery::ConnectionHold;
        let pool = Arc::new(NntpPool::new(test_pool_config(4)));
        let mut changes = pool.server_health_changes();
        pool.note_provider_over_limit(ServerId(0));
        changes.changed().await;
        assert_eq!(pool.new_connection_seats(0), 0);
        assert!(pool.next_connection_hold_deadline().is_some());

        pool.over_limit_until[0].store(unix_epoch_ms() - 1, Ordering::Release);
        assert_eq!(pool.connection_hold(0, true), Some(ConnectionHold::Probe));
        assert_eq!(pool.new_connection_seats(0), 1);
        let probe = pool.admit_fresh_connect(ServerId(0)).unwrap();
        changes.changed().await;
        assert_eq!(pool.new_connection_seats(0), 0);
        assert!(pool.next_connection_hold_deadline().is_some());
        assert!(pool.admit_fresh_connect(ServerId(0)).is_err());

        pool.release_over_limit_probe(ServerId(0), probe);
        changes.changed().await;
        assert_eq!(pool.new_connection_seats(0), 1);
        let probe = pool.admit_fresh_connect(ServerId(0)).unwrap();
        pool.note_provider_admitted(ServerId(0), probe);
        changes.changed().await;
        assert_eq!(pool.new_connection_seats(0), 4);
        assert_eq!(pool.next_connection_hold_deadline(), None);
    }

    // A server whose one recovery probe is out holds every new connection,
    // for a dispatcher's choice of server and for the permit path alike, and
    // only the probe's answer lifts the hold. The answer is an event a watch
    // taken before it sees: nothing has to ask again to find out.
    #[tokio::test]
    async fn a_probe_out_holds_new_connections_until_its_answer_wakes_the_watch() {
        use crate::recovery::ConnectionHold;
        let pool = Arc::new(NntpPool::new(test_pool_config(2)));
        assert_eq!(pool.connection_hold(0, true), None);

        let probe = pool.take_recovery_probe_for_test(0);
        assert_eq!(
            pool.connection_hold(0, true),
            Some(ConnectionHold::Recovery)
        );
        assert!(matches!(
            pool.try_acquire_blocking_permit_for_work(ServerId(0), true),
            Err(NntpError::ServerRecovering)
        ));
        assert_eq!(
            pool.next_connection_hold_deadline(),
            None,
            "a probe's answer is an event, not a deadline"
        );

        let mut changes = pool.server_health_changes();
        probe.answer();
        changes.changed().await;
        assert_eq!(pool.connection_hold(0, true), None);
        assert!(
            pool.try_acquire_blocking_permit_for_work(ServerId(0), true)
                .is_ok()
        );
    }

    // A probe given back unanswered lets the next demanded dial through as
    // the probe; the watch wakes on that too.
    #[tokio::test]
    async fn a_probe_given_back_wakes_the_watch_and_admits_the_next_probe() {
        use crate::recovery::ConnectionHold;
        let pool = Arc::new(NntpPool::new(test_pool_config(2)));
        let probe = pool.take_recovery_probe_for_test(0);
        let mut changes = pool.server_health_changes();
        drop(probe);
        changes.changed().await;
        assert_eq!(pool.connection_hold(0, true), Some(ConnectionHold::Probe));
        assert_eq!(pool.new_connection_seats(0), 1);
        assert_eq!(
            pool.connection_hold(0, false),
            Some(ConnectionHold::Recovery),
            "a dial no work waits on is never the probe"
        );
    }

    // The delay after a failed dial is a hold with a deadline, and the
    // deadline is what the pool reports for a waiter to sleep until.
    #[tokio::test]
    async fn a_failed_dial_holds_new_connections_until_the_reconnect_delay_ends() {
        use crate::recovery::ConnectionHold;
        let mut config = test_pool_config(2);
        config.reconnect_delay = Duration::from_secs(3600);
        let pool = NntpPool::new(config);
        let failed_at = Instant::now();
        *pool.last_connect_failure[0].lock().await = Some(failed_at);
        let until = failed_at + Duration::from_secs(3600);
        assert_eq!(
            pool.connection_hold(0, true),
            Some(ConnectionHold::Until(until))
        );
        assert_eq!(pool.next_connection_hold_deadline(), Some(until));
        assert!(matches!(
            pool.try_acquire_blocking_permit_for_work(ServerId(0), true),
            Err(NntpError::ServerRecovering)
        ));

        *pool.last_connect_failure[0].lock().await =
            failed_at.checked_sub(Duration::from_secs(7200));
        assert_eq!(pool.connection_hold(0, true), None);
        assert_eq!(pool.next_connection_hold_deadline(), None);
    }

    // A local server that greets and answers CAPABILITIES on its first
    // `served` connections, and accepts then drops every later one.
    fn spawn_dropping_server(served: usize) -> (u16, Arc<std::sync::atomic::AtomicUsize>) {
        use std::io::{BufRead, BufReader, Write};
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let accepted = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let accepted_by_server = Arc::clone(&accepted);
        std::thread::spawn(move || {
            for socket in listener.incoming() {
                let Ok(mut socket) = socket else { return };
                let slot = accepted_by_server.fetch_add(1, Ordering::SeqCst);
                if slot >= served {
                    drop(socket);
                    continue;
                }
                std::thread::spawn(move || {
                    if socket.write_all(b"200 ready\r\n").is_err() {
                        return;
                    }
                    let reader = BufReader::new(socket.try_clone().unwrap());
                    for line in reader.lines() {
                        let Ok(line) = line else { return };
                        let reply: &[u8] = if line == "CAPABILITIES" {
                            b"101 capabilities\r\nVERSION 2\r\nREADER\r\n.\r\n"
                        } else {
                            b"500 unsupported\r\n"
                        };
                        if socket.write_all(reply).is_err() {
                            return;
                        }
                    }
                });
            }
        });
        (port, accepted)
    }

    fn blocking_lane_client(
        port: u16,
        max_connections: usize,
        reconnect_delay: Duration,
    ) -> (Arc<NntpPool>, crate::client::NntpClient) {
        let mut config = test_pool_config(max_connections);
        config.servers[0].server.host = "127.0.0.1".into();
        config.servers[0].server.port = port;
        config.servers[0].server.tls = false;
        config.reconnect_delay = reconnect_delay;
        let pool = Arc::new(NntpPool::new(config));
        let client = crate::client::NntpClient::from_pool(Arc::clone(&pool));
        (pool, client)
    }

    // The connection downloads use: a failed dial holds every new lane on its
    // server until the reconnect delay ends, without a socket, and once the
    // delay has passed the next lane dials again.
    #[test]
    fn a_failed_blocking_dial_holds_new_lanes_until_the_reconnect_delay_ends() {
        use crate::client::BlockingBodyLaneAcquireError;
        let (port, accepted) = spawn_dropping_server(0);
        let (pool, client) = blocking_lane_client(port, 2, Duration::from_secs(3600));

        let first = client
            .try_acquire_blocking_body_lane(&[], crate::ServerSet::EMPTY)
            .err()
            .expect("the dropped dial fails");
        assert!(
            matches!(first, BlockingBodyLaneAcquireError::Other(_)),
            "{first:?}"
        );
        assert_eq!(accepted.load(Ordering::SeqCst), 1);
        let recorded = pool.last_connect_failure[0]
            .blocking_lock()
            .expect("the failed dial records its failure");

        let held = client
            .try_acquire_blocking_body_lane(&[], crate::ServerSet::EMPTY)
            .err()
            .expect("the reconnect delay holds the next lane");
        assert!(
            matches!(held, BlockingBodyLaneAcquireError::ServerRecovering),
            "{held:?}"
        );
        assert_eq!(
            accepted.load(Ordering::SeqCst),
            1,
            "a held lane opens no socket"
        );

        // Date the failure a full delay back rather than waiting it out.
        *pool.last_connect_failure[0].blocking_lock() =
            recorded.checked_sub(Duration::from_secs(3600));
        let next = client
            .try_acquire_blocking_body_lane(&[], crate::ServerSet::EMPTY)
            .err()
            .expect("the next dial is dropped too");
        assert!(
            matches!(next, BlockingBodyLaneAcquireError::Other(_)),
            "{next:?}"
        );
        assert_eq!(accepted.load(Ordering::SeqCst), 2);
    }

    // A dial the server accepted and then dropped is the server's health,
    // however many lanes are busy on it: it never arms the over-limit
    // holdoff, and the next lane waits the ordinary reconnect delay.
    #[test]
    fn a_dropped_blocking_dial_beside_busy_lanes_waits_the_reconnect_delay_not_the_holdoff() {
        use crate::client::BlockingBodyLaneAcquireError;
        let (port, accepted) = spawn_dropping_server(2);
        let (pool, client) = blocking_lane_client(port, 4, Duration::from_secs(3600));

        let busy: Vec<_> = (0..2)
            .map(|_| {
                client
                    .try_acquire_blocking_body_lane(&[], crate::ServerSet::EMPTY)
                    .expect("the first two connections are served")
            })
            .collect();
        assert_eq!(pool.socket_budgets[0].snapshot().physical, 2);

        let dropped = client
            .try_acquire_blocking_body_lane(&[], crate::ServerSet::EMPTY)
            .err()
            .expect("the dropped dial must fail");
        assert!(
            matches!(
                &dropped,
                BlockingBodyLaneAcquireError::Other(NntpError::ConnectionClosed | NntpError::Io(_))
            ),
            "{dropped:?}"
        );
        assert!(!pool.is_over_limit(ServerId(0)));
        assert_eq!(pool.over_limit_refusals(ServerId(0)), 0);
        assert!(pool.last_connect_failure[0].blocking_lock().is_some());

        let next = client
            .try_acquire_blocking_body_lane(&[], crate::ServerSet::EMPTY)
            .err()
            .expect("the reconnect delay holds the next lane");
        assert!(
            matches!(next, BlockingBodyLaneAcquireError::ServerRecovering),
            "{next:?}"
        );
        assert_eq!(accepted.load(Ordering::SeqCst), 3);
        assert!(!pool.is_over_limit(ServerId(0)));
        assert_eq!(pool.over_limit_refusals(ServerId(0)), 0);
        drop(busy);
    }

    // When the over-limit holdoff ends, lanes dial again. A dial that fails
    // on the socket is not a refusal: the probe slot is given back, so the
    // next lane dials too, and the episode's refusal count stands.
    #[test]
    fn a_blocking_lane_dials_again_once_the_over_limit_holdoff_expires() {
        use crate::client::BlockingBodyLaneAcquireError;
        let (port, accepted) = spawn_dropping_server(0);
        let (pool, client) = blocking_lane_client(port, 4, Duration::ZERO);
        pool.note_provider_over_limit(ServerId(0));

        let held = client
            .try_acquire_blocking_body_lane(&[], crate::ServerSet::EMPTY)
            .err()
            .expect("a held-off server opens no lane");
        assert!(
            matches!(
                held,
                BlockingBodyLaneAcquireError::ProviderCapacity(NntpError::ServerOverLimit { .. })
            ),
            "{held:?}"
        );
        assert_eq!(accepted.load(Ordering::SeqCst), 0);

        // Rewind the deadline instead of waiting out the window.
        pool.over_limit_until[0].store(unix_epoch_ms() - 1, Ordering::Release);
        assert!(!pool.is_over_limit(ServerId(0)));

        let probe = client
            .try_acquire_blocking_body_lane(&[], crate::ServerSet::EMPTY)
            .err()
            .expect("the dropped dial fails");
        assert!(
            matches!(probe, BlockingBodyLaneAcquireError::Other(_)),
            "{probe:?}"
        );
        assert_eq!(accepted.load(Ordering::SeqCst), 1);
        assert_eq!(pool.over_limit_episode(0).unwrap().probe_started, 0);

        let next = client
            .try_acquire_blocking_body_lane(&[], crate::ServerSet::EMPTY)
            .err()
            .expect("the next dial is dropped too");
        assert!(
            matches!(next, BlockingBodyLaneAcquireError::Other(_)),
            "{next:?}"
        );
        assert_eq!(accepted.load(Ordering::SeqCst), 2);
        assert_eq!(pool.over_limit_refusals(ServerId(0)), 1);
    }

    // A quarantined server whose deadline has lapsed, with no probe out,
    // admits one new connection, the probe, however many permits are free:
    // the dispatcher seats exactly one lease on it, not one per permit.
    #[test]
    fn a_lapsed_quarantine_seats_one_probe_not_every_free_permit() {
        use crate::recovery::ConnectionHold;
        let pool = NntpPool::new(test_pool_config(30));
        pool.recovery_gates[0].quarantine(Duration::ZERO, None);
        assert_eq!(pool.server_load(0).0, 30);
        assert_eq!(pool.connection_hold(0, true), Some(ConnectionHold::Probe));
        assert_eq!(pool.new_connection_seats(0), 1);
        assert_eq!(
            pool.connection_hold(0, false),
            Some(ConnectionHold::Recovery),
            "a dial no work waits on is never the probe"
        );
    }

    // One watch kept across waits misses no change: a probe answered while
    // nobody waits is still seen by the next wait.
    #[tokio::test]
    async fn a_kept_watch_sees_a_change_made_between_waits() {
        let pool = Arc::new(NntpPool::new(test_pool_config(2)));
        let mut changes = pool.server_health_changes();
        let probe = pool.take_recovery_probe_for_test(0);
        changes.changed().await;
        probe.answer();
        changes.changed().await;
        assert_eq!(pool.connection_hold(0, true), None);
        assert!(changes.follows(&pool));
        let other = Arc::new(NntpPool::new(test_pool_config(2)));
        assert!(!changes.follows(&other));
    }

    // A primary whose quarantine has lapsed is ranked again while its one
    // probe is out, yet takes no new connection until that probe answers. A
    // probe whose BODY hangs held every article of a job behind it: the
    // backup, one priority level down, was refused each article as
    // `HigherPriority` and nothing downloaded. While the probe is out the
    // primary is unavailable to the policy, the backup takes the work, and
    // the probe's taking wakes the watch so the next pass sees it. Once the
    // probe answers, the primary is first again.
    #[tokio::test]
    async fn a_primary_with_its_probe_out_leaves_its_work_to_the_next_level() {
        use crate::provider_policy::{ArticleEligibility, ProviderGroup};
        let server = |host: &str, group: u32| ServerPoolConfig {
            server: ServerConfig {
                host: host.into(),
                ..Default::default()
            },
            max_connections: 4,
            group,
            ..ServerPoolConfig::default()
        };
        let pool = Arc::new(NntpPool::new(PoolConfig {
            servers: vec![
                server("primary.example.com", 0),
                server("backup.example.com", 1),
            ],
            ..PoolConfig::default()
        }));
        let none = crate::ServerSet::EMPTY;
        let primary = ProviderGroup {
            backfill: false,
            priority: 0,
        };
        let backup = ProviderGroup {
            backfill: false,
            priority: 1,
        };
        let backup_answer = |pool: &NntpPool| pool.provider_policy().eligibility(1, none, none, 1);

        // A lapsed quarantine with its probe due: the primary has work for
        // the probe, and the backup still waits on it.
        pool.recovery_gates[0].quarantine(Duration::ZERO, None);
        assert_eq!(
            pool.provider_policy().eligibility(0, none, none, 1),
            ArticleEligibility::Eligible(primary)
        );
        assert_eq!(
            backup_answer(&pool),
            ArticleEligibility::HigherPriority(primary)
        );

        // The probe goes out: the watch wakes, and the backup takes the work.
        // Read as a value, not awaited, so a missing publish fails at once.
        let mut changes = pool.server_health_changes();
        changes.receiver.borrow_and_update();
        let probe = pool.take_recovery_probe_for_test(0);
        assert!(
            changes.receiver.has_changed().unwrap(),
            "the probe going out wakes the watch"
        );
        changes.receiver.borrow_and_update();
        assert_eq!(
            pool.provider_policy().eligibility(0, none, none, 1),
            ArticleEligibility::Unavailable
        );
        assert_eq!(backup_answer(&pool), ArticleEligibility::Eligible(backup));
        // The probe's own connection is past the hold: it may still issue
        // the article it was admitted for, or the probe could never answer
        // and the primary would never come back.
        assert_eq!(
            pool.provider_policy_on_connection(0, true)
                .eligibility(0, none, none, 1),
            ArticleEligibility::Eligible(primary)
        );
        // Any other connection to the primary is held with it.
        assert_eq!(
            pool.provider_policy_on_connection(0, false)
                .eligibility(0, none, none, 1),
            ArticleEligibility::Unavailable
        );
        assert_eq!(
            pool.provider_policy_on_connection(1, false)
                .eligibility(1, none, none, 1),
            ArticleEligibility::Eligible(backup)
        );

        // The probe answers: the primary is first again.
        probe.answer();
        assert!(
            changes.receiver.has_changed().unwrap(),
            "the probe's answer wakes the watch"
        );
        assert_eq!(
            pool.provider_policy().eligibility(0, none, none, 1),
            ArticleEligibility::Eligible(primary)
        );
        assert_eq!(
            backup_answer(&pool),
            ArticleEligibility::HigherPriority(primary)
        );

        // A quarantine before its deadline holds every connection: the
        // backup takes the work then too.
        pool.recovery_gates[0].quarantine(Duration::from_hours(1), None);
        assert_eq!(backup_answer(&pool), ArticleEligibility::Eligible(backup));
    }

    // A probe admitted and given back at once because no socket was free
    // changes nothing its watchers could act on, and they hear of it once,
    // by its giving back, not once for the admission and again for that.
    #[tokio::test]
    async fn a_probe_given_back_for_want_of_a_socket_is_heard_of_once() {
        let pool = Arc::new(NntpPool::new(test_pool_config(1)));
        let held: Vec<_> = std::iter::from_fn(|| pool.socket_budgets[0].try_acquire()).collect();
        assert!(!held.is_empty(), "the server's every socket is held");
        pool.recovery_gates[0].quarantine(Duration::ZERO, None);
        let mut changes = pool.server_health_changes();
        let before = *changes.receiver.borrow_and_update();

        assert!(matches!(
            pool.try_acquire_blocking_permit_for_work(ServerId(0), true),
            Err(NntpError::PoolExhausted)
        ));

        assert_eq!(
            changes.receiver.borrow().wrapping_sub(before),
            1,
            "one change heard for the attempt"
        );
        assert_eq!(
            pool.recovery_gates[0].hold(true),
            Some(crate::recovery::ConnectionHold::Probe),
            "the probe is due again"
        );
        drop(held);
    }

    // The async dial asks the login latch as the blocking dial does: a
    // latched refusal turns an ordinary dial away without dialing, and the
    // quarantine's probe still reaches the server, whose login then ends
    // the latch.
    #[tokio::test]
    async fn the_async_recovery_probe_dials_through_a_latched_login_refusal() {
        use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut config = test_pool_config(4);
        config.servers[0].server.host = "127.0.0.1".into();
        config.servers[0].server.port = listener.local_addr().unwrap().port();
        config.servers[0].server.tls = false;
        let pool = Arc::new(NntpPool::new(config));
        let dials = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let accepted = Arc::clone(&dials);
        let server = tokio::spawn(async move {
            loop {
                let Ok((socket, _)) = listener.accept().await else {
                    return;
                };
                accepted.fetch_add(1, Ordering::SeqCst);
                tokio::spawn(async move {
                    let (reader, mut writer) = socket.into_split();
                    writer.write_all(b"200 ready\r\n").await.unwrap();
                    let mut lines = BufReader::new(reader).lines();
                    while let Ok(Some(line)) = lines.next_line().await {
                        let reply: &[u8] = if line == "CAPABILITIES" {
                            b"101 capabilities\r\nVERSION 2\r\nREADER\r\n.\r\n"
                        } else {
                            b"500 unsupported\r\n"
                        };
                        if writer.write_all(reply).await.is_err() {
                            return;
                        }
                    }
                });
            }
        });
        pool.auth_admission[0].finish(&Err::<(), _>(NntpError::AuthenticationFailed));

        assert!(matches!(
            pool.connect_server(0, None, false).await,
            Err(NntpError::AuthenticationRejected)
        ));
        assert_eq!(dials.load(Ordering::SeqCst), 0, "the latch turned it away");

        let probe = pool.connect_server(0, None, true).await;
        assert!(
            probe.is_ok(),
            "the probe dials through the latch: {:?}",
            probe.err()
        );
        assert_eq!(dials.load(Ordering::SeqCst), 1);

        let after = pool.connect_server(0, None, false).await;
        assert!(
            after.is_ok(),
            "the probe's login ends the latch: {:?}",
            after.err()
        );
        drop((probe, after));
        server.abort();
    }
}
