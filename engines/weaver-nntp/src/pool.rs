use std::collections::{HashSet, VecDeque};
use std::net::{IpAddr, SocketAddr};
use std::ops::{Deref, DerefMut};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use tokio::sync::OwnedSemaphorePermit;
use tokio::sync::{Mutex, Semaphore};

/// The idle list and the retired-IP set are guarded synchronously.
///
/// Nothing awaits while holding either, and a dropped [`PooledConnection`]
/// must be able to put its socket back on the idle list *before* it releases
/// the permit it was holding. Deferring the return to a spawned task released
/// the permit first, so an acquire that fired in between found the list empty
/// and dialled a fresh connection past a perfectly warm one.
use std::sync::Mutex as SyncMutex;
use std::sync::MutexGuard as SyncMutexGuard;
use tokio::time::Instant as TokioInstant;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, trace, warn};

use crate::connection::{NntpConnection, ServerConfig};
use crate::error::{NntpError, Result};
use crate::health::{DisableReason, HealthConfig, HealthTracker, ServerState};
use crate::transfer::{ServerTransferControl, StableServerId};

/// Identifies a specific server in the configuration.
#[derive(Debug, Clone, Copy, Hash, Eq, PartialEq)]
pub struct ServerId(pub usize);

/// Whether BODY work can use any server without constructing a ranked order.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BodyServerAvailability {
    /// At least one server is usable, even if all of its permits are busy.
    Eligible,
    /// No server is usable now, but one has a timed health recovery.
    WaitingUntil(Duration),
    /// No admissible server has a timed recovery.
    Blocked,
}

/// Lock a synchronous pool mutex, reading through a poisoning panic.
///
/// A poisoned lock only means some thread panicked while holding it. The
/// guarded values are plain collections of connections, so continuing is safe
/// and strictly better than refusing to hand out or take back a socket.
fn lock_recovering<T>(mutex: &SyncMutex<T>) -> SyncMutexGuard<'_, T> {
    mutex
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

/// Connection pool for a single NNTP server.
#[allow(dead_code)]
struct ServerPool {
    config: ServerConfig,
    idle: VecDeque<NntpConnection>,
    active_count: usize,
    max_connections: usize,
}

/// Multi-server NNTP connection pool.
pub struct NntpPool {
    pools: Vec<Arc<SyncMutex<ServerPool>>>,
    configs: Vec<ServerConfig>,
    stable_ids: Vec<StableServerId>,
    transfer_controls: Vec<Option<Arc<ServerTransferControl>>>,
    semaphores: Vec<Arc<Semaphore>>,
    shutdown: CancellationToken,
    max_idle_age: Duration,
    health: Arc<Mutex<HealthTracker>>,
    /// Per-server timestamp of the last failed connection attempt.
    last_connect_failure: Vec<Arc<Mutex<Option<Instant>>>>,
    reconnect_delay: Duration,
    stale_check_age: Duration,
    /// Priority group for each server (parallel to pools/configs).
    groups: Vec<u32>,
    /// Backfill flag for each server (parallel to pools/configs).
    backfill: Vec<bool>,
    /// Retention window in days for each server (parallel to pools/configs).
    retention_days: Vec<u32>,
    /// Maximum connections per server (parallel to pools/configs).
    max_connections: Vec<usize>,
    /// Per-server deadline (unix epoch ms, `0` = none) before which fresh
    /// connects are skipped because the provider refused the last one.
    over_limit_until: Vec<AtomicU64>,
    /// Per-server state of the episode behind `over_limit_until`: how many
    /// connects in a row the provider has refused, and whether one caller is
    /// out probing it. Only episode transitions take this lock; the steady
    /// state reads the atomic deadline alone.
    over_limit_episodes: Vec<SyncMutex<OverLimitEpisode>>,
    /// Per-server epoch-ms floor for the next blocking-connect warning, and the
    /// failures suppressed since the last one was emitted. A server that cannot
    /// be connected to fails on every dispatch pass, so an unthrottled warning
    /// would be a log flood; a silent one is what made the condition invisible.
    blocking_connect_warn_after: Vec<AtomicU64>,
    blocking_connect_failures_since_warning: Vec<AtomicU64>,
    retired_ips: Arc<SyncMutex<HashSet<(usize, IpAddr)>>>,
    connect_cursors: Vec<AtomicUsize>,
}

pub struct BlockingConnectionPermit {
    _permit: OwnedSemaphorePermit,
}

#[cfg(test)]
impl BlockingConnectionPermit {
    /// A permit backed by its own semaphore, for lane tests that never
    /// contend for a pool slot.
    pub(crate) fn for_tests() -> Self {
        let semaphore = Arc::new(Semaphore::new(1));
        Self {
            _permit: semaphore
                .try_acquire_owned()
                .expect("a fresh semaphore always has its one permit"),
        }
    }
}

/// How long fresh connects to a server pause after the provider first answers
/// a connect with "too many connections". Existing sessions keep running; only
/// new sockets wait, which is what the provider is actually asking for.
///
/// When the pause ends, one connect goes out as a probe. A probe the provider
/// accepts ends the episode; a refused one doubles the pause, up to
/// [`OVER_LIMIT_HOLDOFF_MAX`], so a provider still holding a previous
/// process's sessions is asked again in half a minute rather than ten, while
/// one that keeps refusing is left alone for longer each time.
pub const OVER_LIMIT_HOLDOFF_INITIAL: Duration = Duration::from_secs(30);

/// Ceiling the over-limit holdoff doubles toward while every probe is refused.
pub const OVER_LIMIT_HOLDOFF_MAX: Duration = Duration::from_secs(10 * 60);

/// How long a probe connect owns the right to ask the provider before another
/// caller may take it over. Longer than any connect timeout, so a probe that
/// is still dialling is never doubled up; short enough that a probe whose
/// caller vanished does not hold the server closed for long.
const OVER_LIMIT_PROBE_WINDOW: Duration = Duration::from_secs(30);

/// The pause the `refusals`th consecutive refused connect of an episode earns.
pub fn over_limit_holdoff(refusals: u32) -> Duration {
    let doublings = refusals.saturating_sub(1).min(16);
    OVER_LIMIT_HOLDOFF_INITIAL
        .saturating_mul(1u32 << doublings)
        .min(OVER_LIMIT_HOLDOFF_MAX)
}

fn duration_to_epoch_ms(duration: Duration) -> u64 {
    duration.as_millis().try_into().unwrap_or(u64::MAX)
}

/// What [`NntpPool::admit_fresh_connect`] let a caller do, handed back with
/// the connect's outcome so the pool knows what that outcome proves.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FreshConnectAdmission {
    /// No over-limit episode was running when the connect was admitted. Its
    /// success says nothing about a refusal that may have arrived since: a
    /// socket the provider accepted before it started refusing is not proof
    /// that it accepts again.
    Open,
    /// The one connect allowed out after a holdoff to ask whether the
    /// provider accepts new sockets again. Its outcome settles the episode —
    /// unless the slot has changed hands since it was admitted, in which
    /// case the later word stands. The token names this probe among those
    /// the server has issued.
    Probe { token: u64 },
}

/// One server's over-limit episode. `over_limit_until` is the lock-free
/// mirror of whether an episode exists at all; this holds the rest.
#[derive(Debug, Default)]
struct OverLimitEpisode {
    /// Consecutive refused connects; each one doubles the next holdoff.
    refusals: u32,
    /// Epoch ms at which one caller was let through to probe the provider
    /// after the pause ended, `0` when nobody is probing.
    probe_started: u64,
    /// Which probe holds the slot: the count of probes ever admitted for
    /// this server, so a late report from an earlier one is told apart.
    probe_token: u64,
}

/// How often one server's blocking-lane connect failures may be warned about.
/// Every dispatch pass retries, so the failures arrive as fast as the scheduler
/// runs; the warning stands for all of them and carries the count.
const BLOCKING_CONNECT_WARN_INTERVAL: Duration = Duration::from_secs(60);

fn unix_epoch_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis()
        .try_into()
        .unwrap_or(u64::MAX)
}

/// Configuration for creating an NNTP pool.
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

/// Per-server pool configuration.
pub struct ServerPoolConfig {
    pub server: ServerConfig,
    /// Durable database identity. Unlike [`ServerId`], this survives reorder
    /// and client rebuilds.
    pub stable_id: StableServerId,
    /// Shared BODY policy obtained from a long-lived registry.
    pub transfer_control: Option<Arc<ServerTransferControl>>,
    pub max_connections: usize,
    /// Priority group. Lower values tried first within a tier.
    pub group: u32,
    /// Backfill servers are ordered after every fill server and are only
    /// reachable once all fill servers are excluded for a request.
    pub backfill: bool,
    /// Days of retention this server is expected to hold (0 = unlimited).
    /// Inert metadata for the pool: callers translate it into per-request
    /// exclusions; carrying it here keeps it aligned with server indices.
    pub retention_days: u32,
}

impl Default for ServerPoolConfig {
    fn default() -> Self {
        Self {
            server: ServerConfig::default(),
            stable_id: StableServerId::default(),
            transfer_control: None,
            max_connections: 1,
            group: 0,
            backfill: false,
            retention_days: 0,
        }
    }
}

impl NntpPool {
    /// Create a new multi-server connection pool.
    pub fn new(config: PoolConfig) -> Self {
        let server_count = config.servers.len();
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
        let mut connect_cursors = Vec::with_capacity(server_count);

        // A config where every server is backfill has no fill tier to
        // exhaust; treat it as an all-fill config so downloads can proceed.
        let all_backfill = server_count > 0 && config.servers.iter().all(|spc| spc.backfill);
        if all_backfill {
            warn!("every configured server is marked backfill; treating all servers as fill");
        }

        for spc in &config.servers {
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
            connect_cursors.push(AtomicUsize::new(0));
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

        let health = Arc::new(Mutex::new(HealthTracker::new_with_backfill(
            server_count,
            config.health_config,
            backfill.clone(),
        )));

        NntpPool {
            pools,
            configs,
            stable_ids,
            transfer_controls,
            semaphores,
            shutdown: CancellationToken::new(),
            max_idle_age: config.max_idle_age,
            health,
            last_connect_failure,
            reconnect_delay: config.reconnect_delay,
            stale_check_age: config.stale_check_age,
            groups,
            backfill,
            retention_days,
            max_connections,
            over_limit_until,
            over_limit_episodes,
            blocking_connect_warn_after,
            blocking_connect_failures_since_warning,
            retired_ips: Arc::new(SyncMutex::new(HashSet::new())),
            connect_cursors,
        }
    }

    fn next_connect_offset(&self, idx: usize) -> usize {
        self.connect_cursors[idx].fetch_add(1, Ordering::Relaxed)
    }

    async fn retired_ips_for_server(&self, idx: usize) -> Vec<IpAddr> {
        lock_recovering(&self.retired_ips)
            .iter()
            .filter_map(|(server_idx, ip)| (*server_idx == idx).then_some(*ip))
            .collect()
    }

    async fn connect_server_excluding(
        &self,
        idx: usize,
        excluded_ips: &[IpAddr],
        initial_group: Option<&str>,
    ) -> Result<NntpConnection> {
        let admission = self.admit_fresh_connect(ServerId(idx))?;
        let result = self
            .connect_server_excluding_untracked(idx, excluded_ips, initial_group)
            .await;
        // A refusal here is the provider declining a new socket, not answering
        // for the sessions already held, so it arms the holdoff and stays out
        // of health entirely.
        self.note_fresh_connect_outcome(ServerId(idx), admission, &result);
        result
    }

    async fn connect_server_excluding_untracked(
        &self,
        idx: usize,
        excluded_ips: &[IpAddr],
        initial_group: Option<&str>,
    ) -> Result<NntpConnection> {
        let mut exclusions = self.retired_ips_for_server(idx).await;
        exclusions.extend(excluded_ips.iter().copied());
        exclusions.sort_unstable();
        exclusions.dedup();
        let offset = self.next_connect_offset(idx);
        let mut connection = NntpConnection::connect_with_ip_policy_for_group(
            &self.configs[idx],
            &exclusions,
            offset,
            initial_group,
        )
        .await?;
        connection.set_transfer_control(self.transfer_controls[idx].clone());
        Ok(connection)
    }

    /// Acquire a connection from a specific server.
    pub async fn acquire(&self, server: ServerId) -> Result<PooledConnection> {
        self.acquire_for_group(server, None).await
    }

    /// Acquire a connection; a fresh connection to a pipelining server
    /// selects `initial_group` inside its session-setup write.
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

        // Wait for a permit (limits total connections to this server).
        let permit = self.semaphores[idx]
            .clone()
            .acquire_owned()
            .await
            .map_err(|_| NntpError::PoolShutdown)?;

        self.acquire_with_permit(idx, Some(permit), initial_group)
            .await
    }

    /// Whether a normal (in-cap) lease could be taken right now without
    /// waiting on the server's connection semaphore.
    pub fn has_available_permit(&self, server: ServerId) -> bool {
        let idx = server.0;
        idx < self.semaphores.len() && self.semaphores[idx].available_permits() > 0
    }

    /// Acquire an explicit over-max connection from a specific server.
    pub async fn acquire_extra(&self, server: ServerId) -> Result<PooledConnection> {
        self.acquire_extra_excluding(server, &[]).await
    }

    /// Acquire an explicit over-max connection, excluding specific remote IPs.
    pub async fn acquire_extra_excluding(
        &self,
        server: ServerId,
        excluded_ips: &[IpAddr],
    ) -> Result<PooledConnection> {
        self.acquire_extra_excluding_for_group(server, excluded_ips, None)
            .await
    }

    /// Over-max acquire whose fresh connection selects `initial_group` in
    /// its session-setup write on a pipelining server.
    pub async fn acquire_extra_excluding_for_group(
        &self,
        server: ServerId,
        excluded_ips: &[IpAddr],
        initial_group: Option<&str>,
    ) -> Result<PooledConnection> {
        if self.shutdown.is_cancelled() {
            return Err(NntpError::PoolShutdown);
        }

        let idx = server.0;
        if idx >= self.pools.len() {
            return Err(NntpError::PoolExhausted);
        }
        self.acquire_fresh_with_permit(idx, None, excluded_ips, initial_group)
            .await
    }

    /// Internal: acquire a connection using an already-obtained permit.
    async fn acquire_with_permit(
        &self,
        idx: usize,
        permit: Option<tokio::sync::OwnedSemaphorePermit>,
        initial_group: Option<&str>,
    ) -> Result<PooledConnection> {
        // Try to get a healthy idle connection, with stale-check loop.
        let conn = loop {
            let candidate = {
                let mut pool = lock_recovering(&self.pools[idx]);
                self.take_healthy_idle(&mut pool)
            };

            match candidate {
                Some(mut c) => {
                    // If the connection is older than stale_check_age, probe it.
                    if c.last_used().elapsed() > self.stale_check_age {
                        trace!(server = idx, "pinging stale idle connection");
                        match c.ping().await {
                            Ok(()) => break c,
                            Err(e) => {
                                trace!(server = idx, error = %e, "stale ping failed, dropping that connection");
                                // Only this socket has proven itself dead. Its
                                // siblings are pinged on their own way out of
                                // the idle list if they are stale too, so the
                                // loop simply takes the next one rather than
                                // throwing away warm capacity on suspicion.
                                continue;
                            }
                        }
                    } else {
                        break c;
                    }
                }
                None => {
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
                                tokio::time::sleep(remaining).await;
                            }
                        }
                    }

                    debug!(server = idx, "creating new connection");
                    match self.connect_server_excluding(idx, &[], initial_group).await {
                        Ok(c) => {
                            // Clear the failure timestamp on success.
                            let mut last_failure = self.last_connect_failure[idx].lock().await;
                            *last_failure = None;
                            break c;
                        }
                        Err(e) => {
                            // Record the failure timestamp.
                            let mut last_failure = self.last_connect_failure[idx].lock().await;
                            *last_failure = Some(Instant::now());
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
            retired_ips: self.retired_ips.clone(),
            server_idx: idx,
            return_to_pool: permit.is_some(),
            shutdown: self.shutdown.clone(),
            _permit: permit,
        })
    }

    async fn acquire_fresh_with_permit(
        &self,
        idx: usize,
        permit: Option<tokio::sync::OwnedSemaphorePermit>,
        excluded_ips: &[IpAddr],
        initial_group: Option<&str>,
    ) -> Result<PooledConnection> {
        {
            let last_failure = self.last_connect_failure[idx].lock().await;
            if let Some(ts) = *last_failure {
                let elapsed = ts.elapsed();
                if elapsed < self.reconnect_delay {
                    let remaining = self.reconnect_delay - elapsed;
                    drop(last_failure);
                    tokio::time::sleep(remaining).await;
                }
            }
        }

        debug!(server = idx, "creating fresh over-max connection");
        let conn = match self
            .connect_server_excluding(idx, excluded_ips, initial_group)
            .await
        {
            Ok(conn) => {
                let mut last_failure = self.last_connect_failure[idx].lock().await;
                *last_failure = None;
                conn
            }
            Err(error) => {
                let mut last_failure = self.last_connect_failure[idx].lock().await;
                *last_failure = Some(Instant::now());
                return Err(error);
            }
        };

        lock_recovering(&self.pools[idx]).active_count += 1;

        Ok(PooledConnection {
            conn: Some(conn),
            pool: self.pools[idx].clone(),
            retired_ips: self.retired_ips.clone(),
            server_idx: idx,
            return_to_pool: permit.is_some(),
            shutdown: self.shutdown.clone(),
            _permit: permit,
        })
    }

    /// Drain all idle connections across all servers.
    ///
    /// Called when a network change is suspected (e.g. I/O errors after an
    /// interface switch). Connections are dropped without recording health
    /// failures, since the servers themselves are fine.
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

    /// Drop the idle connections of one server after one of its sockets
    /// failed. A single dead socket says nothing about other providers, so
    /// their warm idle connections are left alone.
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

    /// Shut down the pool and wait for active leases to drain.
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

    /// The number of configured servers.
    pub fn server_count(&self) -> usize {
        self.pools.len()
    }

    /// Priority group for each server (indexed by pool position).
    pub fn server_groups(&self) -> &[u32] {
        &self.groups
    }

    /// Backfill flag for each server (indexed by pool position). An
    /// all-backfill config is normalized to all-fill at construction.
    pub fn server_backfill_flags(&self) -> &[bool] {
        &self.backfill
    }

    /// Retention window in days for each server (0 = unlimited), indexed by
    /// pool position.
    pub fn server_retention_days(&self) -> &[u32] {
        &self.retention_days
    }

    /// Whether any configured server is a backfill server.
    pub fn has_backfill_servers(&self) -> bool {
        self.backfill.iter().any(|backfill| *backfill)
    }

    /// Total configured connections across fill (non-backfill) servers.
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

    /// Deadline of this server's active over-limit holdoff, if one is running.
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
        let probe_until =
            probe_started.saturating_add(duration_to_epoch_ms(OVER_LIMIT_PROBE_WINDOW));
        (probe_started != 0 && probe_until > now).then_some(probe_until)
    }

    /// This server's episode state, readable through a poisoned lock: the
    /// state is plain data, and refusing to connect because some thread
    /// panicked elsewhere would be the worse outcome.
    fn over_limit_episode(&self, idx: usize) -> Option<SyncMutexGuard<'_, OverLimitEpisode>> {
        let slot = self.over_limit_episodes.get(idx)?;
        Some(match slot.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        })
    }

    /// Consecutive refused connects in this server's current over-limit
    /// episode, `0` when none is running.
    pub fn over_limit_refusals(&self, server: ServerId) -> u32 {
        self.over_limit_episode(server.0)
            .map(|episode| episode.refusals)
            .unwrap_or(0)
    }

    /// Whether a fresh socket to `server` may be opened right now.
    ///
    /// [`FreshConnectAdmission::Open`] when no holdoff is running. During a
    /// holdoff every caller gets [`NntpError::ServerOverLimit`]. Once the
    /// holdoff ends, exactly one caller is let through as the
    /// [`FreshConnectAdmission::Probe`] and the rest keep getting the error
    /// until that probe's outcome is reported through
    /// [`Self::note_fresh_connect_outcome`] (or one of the outcome methods it
    /// dispatches to). A probe must report, or its slot stays taken for
    /// [`OVER_LIMIT_PROBE_WINDOW`].
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
            let probe_until = episode
                .probe_started
                .saturating_add(duration_to_epoch_ms(OVER_LIMIT_PROBE_WINDOW));
            if probe_until > now {
                return Err(NntpError::ServerOverLimit {
                    until_epoch_ms: probe_until,
                });
            }
        }
        episode.probe_started = now;
        episode.probe_token = episode.probe_token.wrapping_add(1);
        debug!(
            server = idx,
            refusals = episode.refusals,
            "holdoff ended; probing the provider with one connect"
        );
        Ok(FreshConnectAdmission::Probe {
            token: episode.probe_token,
        })
    }

    /// Book the result of a connect that passed [`Self::admit_fresh_connect`].
    ///
    /// A refusal always counts. Anything else only means something when the
    /// connect was the probe: an ordinary connect that was already dialling
    /// when the provider started refusing may well succeed, and must not
    /// clear the holdoff the refusal just armed.
    pub fn note_fresh_connect_outcome<T>(
        &self,
        server: ServerId,
        admission: FreshConnectAdmission,
        result: &Result<T>,
    ) {
        match result {
            Err(NntpError::TooManyConnections) => self.note_provider_over_limit(server),
            Ok(_) => self.note_provider_admitted(server, admission),
            Err(_) => self.release_over_limit_probe(server, admission),
        }
    }

    /// The provider accepted the probe: whatever over-limit episode was
    /// running is over, and the next refusal starts a new one from the
    /// shortest pause. Nothing happens for an [`FreshConnectAdmission::Open`]
    /// connect, or for a probe whose slot has since been reset by a newer
    /// refusal or taken over by a later probe.
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
        info!(
            server = idx,
            server_address = %self.server_address(server),
            refusals,
            "provider accepts new connections again; over-limit holdoff cleared"
        );
    }

    /// A probe that failed for some reason other than a refusal (a timeout, a
    /// TLS fault) has answered nothing about the provider's limit. Free the
    /// probe slot so the next caller can ask instead of waiting out the
    /// window — if it is still this probe's slot.
    pub fn release_over_limit_probe(&self, server: ServerId, admission: FreshConnectAdmission) {
        let FreshConnectAdmission::Probe { token } = admission else {
            return;
        };
        if let Some(mut episode) = self.over_limit_episode(server.0)
            && episode.probe_token == token
        {
            episode.probe_started = 0;
        }
    }

    /// Records one blocking-lane connect failure and answers whether this one
    /// should be warned about.
    ///
    /// `Some(n)` means "warn, and say that `n` failures have gone unreported
    /// since the last warning" — `n` counts this one, so the first failure of a
    /// window reports `1`. `None` means the window is still open and the
    /// failure has only been counted.
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

    /// `host:port` of one configured server, for a log line that has to say
    /// which one it is talking about.
    pub fn server_address(&self, server: ServerId) -> String {
        self.configs
            .get(server.0)
            .map(|config| format!("{}:{}", config.host, config.port))
            .unwrap_or_default()
    }

    /// Whether fresh connects to this server are currently held off. One
    /// atomic load, so callers on the dispatch path can ask freely.
    pub fn is_over_limit(&self, server: ServerId) -> bool {
        self.over_limit_until_epoch_ms(server).is_some()
    }

    /// Park fresh connects to `server` after the provider refused one.
    ///
    /// A client restart can collect one rejection per configured connection in
    /// a couple of seconds while the provider still holds the previous
    /// process's sessions open, so only the first rejection of a window arms
    /// and reports it; the rest are silent until the deadline passes. Each
    /// window that ends in another refusal (the probe's) doubles the next one,
    /// from [`OVER_LIMIT_HOLDOFF_INITIAL`] up to [`OVER_LIMIT_HOLDOFF_MAX`].
    pub fn note_provider_over_limit(&self, server: ServerId) {
        let idx = server.0;
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

    /// Inspect BODY eligibility without allocating or ranking server candidates.
    pub async fn body_server_availability(
        &self,
        failure_excludes: &[usize],
        retention_excludes: &[usize],
        requested_body_bytes: u64,
    ) -> BodyServerAvailability {
        let is_excluded = |server_idx: usize| {
            failure_excludes.contains(&server_idx) || retention_excludes.contains(&server_idx)
        };
        let now = Instant::now();
        let mut health = self.health.lock().await;
        health.check_reenable_all();
        // Same ordering-side gate the try-order uses: an auth-disabled fill
        // server counts as exhausted so the lane-unavailable arm sees
        // backfill as eligible instead of waiting on a deadline that only the
        // operator can resolve.
        let excludes = (0..self.configs.len())
            .filter(|idx| is_excluded(*idx))
            .collect::<Vec<_>>();
        let backfill_unlocked = self.fill_servers_exhausted_or_auth_disabled(&excludes, &health);
        let mut retry_after = None;
        for server_idx in 0..self.configs.len() {
            if is_excluded(server_idx)
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

    /// Whether every fill (non-backfill) server is in `exclude` — the gate
    /// that makes backfill servers reachable for a request.
    pub fn fill_servers_exhausted(&self, exclude: &[usize]) -> bool {
        self.backfill
            .iter()
            .enumerate()
            .filter(|(_, backfill)| !**backfill)
            .all(|(idx, _)| exclude.contains(&idx))
    }

    /// The ordering-side backfill gate: every fill server is either excluded
    /// for this request or disabled by health for a reason that will not
    /// heal on its own (`DisableReason::AuthFailure`).
    ///
    /// A fill server with bad credentials re-disables on every probe, so it
    /// never produces the 430 that would exclude it and otherwise pins an
    /// article forever: the try-order skips the disabled server, the
    /// remaining fill servers already 430'd, and backfill stays locked because
    /// the disabled server is not in `exclude`. Counting an auth-disabled
    /// server here lets the article spill to backfill instead of requeueing
    /// against a deadline that only the operator can resolve.
    ///
    /// Every other health state deliberately does *not* count. `CoolingDown`
    /// is a 5–10 s transport or capacity blip, and a `ConsecutiveFailures` /
    /// `FailureRatio` disable is an outage that heals by itself; in both cases
    /// waiting is far cheaper than spilling the whole queue onto a paid
    /// backfill account, which is also how SABnzbd and NZBGet behave. Note
    /// this is an *ordering* gate only — disabled servers must never enter a
    /// request's exclude set, or exhaustion booking would declare the segment
    /// missing before backfill was ever tried.
    pub fn fill_servers_exhausted_or_auth_disabled(
        &self,
        exclude: &[usize],
        health: &HealthTracker,
    ) -> bool {
        self.backfill
            .iter()
            .enumerate()
            .filter(|(_, backfill)| !**backfill)
            .all(|(idx, _)| {
                exclude.contains(&idx)
                    || matches!(
                        health.server(idx).state(),
                        ServerState::Disabled {
                            reason: DisableReason::AuthFailure,
                            ..
                        }
                    )
            })
    }

    /// Access the health tracker for observability.
    pub fn health(&self) -> &Arc<Mutex<HealthTracker>> {
        &self.health
    }

    /// Server configurations (parallel to health tracker indices).
    pub fn server_configs(&self) -> &[ServerConfig] {
        &self.configs
    }

    pub fn stable_server_id(&self, server: ServerId) -> Option<StableServerId> {
        self.stable_ids.get(server.0).copied()
    }

    pub fn server_transfer_control(&self, server: ServerId) -> Option<Arc<ServerTransferControl>> {
        self.transfer_controls.get(server.0).cloned().flatten()
    }

    pub fn try_acquire_blocking_permit(
        &self,
        server: ServerId,
    ) -> Result<BlockingConnectionPermit> {
        let idx = server.0;
        if idx >= self.semaphores.len() {
            return Err(NntpError::PoolExhausted);
        }
        let permit = self.semaphores[idx]
            .clone()
            .try_acquire_owned()
            .map_err(|_| NntpError::PoolExhausted)?;
        Ok(BlockingConnectionPermit { _permit: permit })
    }

    pub fn blocking_connect_plan(
        &self,
        server: ServerId,
        excluded_ips: &[IpAddr],
    ) -> Result<(ServerConfig, Vec<IpAddr>, usize)> {
        let idx = server.0;
        if idx >= self.configs.len() {
            return Err(NntpError::PoolExhausted);
        }
        let mut exclusions = Vec::new();
        if let Ok(retired) = self.retired_ips.try_lock() {
            exclusions.extend(
                retired
                    .iter()
                    .filter_map(|(server_idx, ip)| (*server_idx == idx).then_some(*ip)),
            );
        }
        exclusions.extend(excluded_ips.iter().copied());
        exclusions.sort_unstable();
        exclusions.dedup();
        let offset = self.next_connect_offset(idx);
        Ok((self.configs[idx].clone(), exclusions, offset))
    }

    /// Returns `(available_permits, configured_connections)` for the given server.
    ///
    /// This is lock-free — it reads semaphore permits and the pre-stored
    /// max_connections value, so it can be called from synchronous contexts.
    pub fn server_load(&self, idx: usize) -> (usize, usize) {
        (
            self.semaphores[idx].available_permits(),
            self.max_connections[idx],
        )
    }

    /// Currently leased connections for the given server.
    pub fn active_connections(&self, idx: usize) -> usize {
        self.max_connections[idx].saturating_sub(self.semaphores[idx].available_permits())
    }

    pub async fn retire_ip(&self, server: ServerId, ip: IpAddr) {
        let idx = server.0;
        if idx >= self.pools.len() {
            return;
        }
        lock_recovering(&self.retired_ips).insert((idx, ip));
        lock_recovering(&self.pools[idx])
            .idle
            .retain(|conn| conn.remote_ip() != ip);
    }

    /// Take a healthy idle connection, evicting stale/poisoned ones.
    fn take_healthy_idle(&self, pool: &mut ServerPool) -> Option<NntpConnection> {
        while let Some(conn) = pool.idle.pop_front() {
            if conn.is_poisoned() {
                trace!("evicting poisoned idle connection");
                continue;
            }
            if conn.last_used().elapsed() > self.max_idle_age {
                trace!("evicting stale idle connection");
                continue;
            }
            return Some(conn);
        }
        None
    }
}

/// RAII guard that returns a connection to the pool on drop.
pub struct PooledConnection {
    conn: Option<NntpConnection>,
    pool: Arc<SyncMutex<ServerPool>>,
    retired_ips: Arc<SyncMutex<HashSet<(usize, IpAddr)>>>,
    server_idx: usize,
    return_to_pool: bool,
    shutdown: CancellationToken,
    _permit: Option<tokio::sync::OwnedSemaphorePermit>,
}

impl PooledConnection {
    pub fn remote_addr(&self) -> SocketAddr {
        self.conn
            .as_ref()
            .expect("pooled connection is present")
            .remote_addr()
    }

    pub fn remote_ip(&self) -> IpAddr {
        self.remote_addr().ip()
    }

    /// Explicitly discard this connection instead of returning it to the pool.
    /// Use when the connection is in a bad state.
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
    /// Return the socket to the idle list here, in the drop itself.
    ///
    /// `_permit` is a later field, so it is released only after this body has
    /// run: by the time the next acquirer can take the permit, the connection
    /// it should reuse is already on the list. Handing the return to a spawned
    /// task inverted that — the permit went back first and the return landed
    /// whenever the runtime got to it, so a caller that re-acquired
    /// immediately (the probe's per-miss HEAD right after its STAT batch)
    /// reliably raced past a warm socket and dialled a new one.
    fn drop(&mut self) {
        if let Some(conn) = self.conn.take() {
            let server_idx = self.server_idx;
            let healthy = conn.is_healthy();
            let poisoned = conn.is_poisoned();
            let return_to_pool = self.return_to_pool;

            let retired = healthy
                && return_to_pool
                && lock_recovering(&self.retired_ips).contains(&(server_idx, conn.remote_ip()));
            let mut pool = lock_recovering(&self.pool);
            pool.active_count = pool.active_count.saturating_sub(1);
            if healthy && return_to_pool {
                if self.shutdown.is_cancelled() {
                    trace!(server = server_idx, "dropped connection from shutdown pool");
                } else if retired {
                    trace!(server = server_idx, "dropped retired-ip connection");
                } else {
                    pool.idle.push_back(conn);
                    trace!(server = server_idx, "returned connection to pool");
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
mod tests {
    use super::*;
    use crate::health::{HealthConfig, ServerState};

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
        assert!(pool.fill_servers_exhausted(&[0]));
        assert!(!pool.fill_servers_exhausted(&[]));
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

        // Second acquire should sleep through the throttle, then attempt
        // a real connection (which also fails — but NOT with ServiceUnavailable).
        let start = Instant::now();
        let result2 = pool.acquire(ServerId(0)).await;
        assert!(result2.is_err());
        assert!(
            !matches!(result2, Err(NntpError::ServiceUnavailable)),
            "pool should sleep through throttle, not return ServiceUnavailable"
        );
        // Should have waited at least most of the reconnect delay.
        assert!(
            start.elapsed() >= Duration::from_millis(100),
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

        // Wait for the reconnect delay to pass.
        tokio::time::sleep(Duration::from_millis(60)).await;

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
            let body = lane.body_by_id_raw("<test>").await.unwrap();
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
        let body = reused.body_by_id_raw("<test>").await.unwrap();
        assert_eq!(body.data.as_ref(), b"payload\r\n");
        assert_eq!(accepted.load(Ordering::SeqCst), accepted_after_rejection);

        // A peer server that never rejected anything is unaffected.
        assert!(!pool.is_over_limit(ServerId(1)));
        assert_eq!(pool.server_load(1), (4, 4));

        server.abort();
    }

    #[tokio::test]
    async fn body_availability_treats_saturated_server_as_eligible() {
        let pool = NntpPool::new(test_pool_config(1));
        let _permit = pool
            .try_acquire_blocking_permit(ServerId(0))
            .expect("configured lane should be acquirable");

        assert_eq!(
            pool.body_server_availability(&[], &[], 0).await,
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
            pool.body_server_availability(&[], &[], 0).await,
            BodyServerAvailability::WaitingUntil(delay) if !delay.is_zero()
        ));
        assert_eq!(
            pool.body_server_availability(&[0], &[], 0).await,
            BodyServerAvailability::Eligible
        );
        assert_eq!(
            pool.body_server_availability(&[], &[0], 0).await,
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
            pool.body_server_availability(&[], &[], 0).await,
            BodyServerAvailability::Eligible
        );

        pool.health().lock().await.record_failure(0, true);
        assert!(matches!(
            pool.body_server_availability(&[], &[], 0).await,
            BodyServerAvailability::WaitingUntil(delay) if !delay.is_zero()
        ));
    }

    /// An auth-disabled fill server must make backfill reachable, or an
    /// article the other fill servers do not have is pinned out of backfill
    /// for as long as the bad credentials last. A short cooldown and a
    /// consecutive-failure outage disable must not: both heal on their own.
    #[tokio::test]
    async fn body_availability_unlocks_backfill_only_for_auth_disabled_fill_servers() {
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
        assert_eq!(
            disabled.body_server_availability(&[], &[], 0).await,
            BodyServerAvailability::Eligible,
            "the healthy backfill server must be visible past a disabled fill tier"
        );
        assert!(
            disabled.fill_servers_exhausted_or_auth_disabled(&[], &*disabled.health().lock().await),
            "the ordering gate itself must agree"
        );
        assert!(
            !disabled.fill_servers_exhausted(&[]),
            "the exclude-only gate must stay untouched: disabled servers are \
             never put into a request exclude set"
        );

        let cooling = NntpPool::new(fill_plus_backfill());
        cooling
            .health()
            .lock()
            .await
            .record_cooldown(0, crate::health::CooldownReason::Transport);
        assert!(
            matches!(
                cooling.body_server_availability(&[], &[], 0).await,
                BodyServerAvailability::WaitingUntil(delay) if !delay.is_zero()
            ),
            "a 5-10s cooldown must wait, not spill onto backfill"
        );
        assert!(
            !cooling.fill_servers_exhausted_or_auth_disabled(&[], &*cooling.health().lock().await),
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
                outage.body_server_availability(&[], &[], 0).await,
                BodyServerAvailability::WaitingUntil(delay) if !delay.is_zero()
            ),
            "an outage disable heals on its own and must wait, not spill onto backfill"
        );
        assert!(
            !outage.fill_servers_exhausted_or_auth_disabled(&[], &*outage.health().lock().await),
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
            pool.body_server_availability(&[], &[], 0).await,
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

    /// The first refusal buys a short pause; when it ends, one caller is let
    /// through to ask the provider and everyone else keeps waiting on that
    /// answer. A refused probe doubles the pause.
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

    /// The provider accepting a socket ends the episode outright: no holdoff,
    /// no probe slot, and the next refusal starts again from the short pause.
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

    /// A connect admitted while nothing was held off, that completes only
    /// after a refusal has armed the holdoff, proves nothing: the provider
    /// accepted it before it began refusing. The holdoff stands.
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

    /// A probe only settles the episode while the slot is still its own. A
    /// refusal booked after it went out resets the slot and starts a longer
    /// pause; the probe's late success neither clears that pause nor does
    /// its late failure free the slot a later probe holds.
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

    /// An episode nobody probed — the queue drained during the pause — does
    /// not carry its refusal count into the next one. A refusal long after
    /// the last pause ended starts again from the shortest pause.
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

    /// A probe whose caller never reported back does not hold the server
    /// closed for good: after the probe window another caller may ask.
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
}
