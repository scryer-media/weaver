use std::collections::{HashSet, VecDeque};
use std::net::{IpAddr, SocketAddr};
use std::ops::{Deref, DerefMut};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use tokio::sync::OwnedSemaphorePermit;
use tokio::sync::{Mutex, Semaphore};
use tokio_util::sync::CancellationToken;
use tracing::{debug, trace, warn};

use crate::connection::{NntpConnection, ServerConfig};
use crate::error::{NntpError, Result};
use crate::health::{HealthConfig, HealthTracker};
use crate::transfer::{ServerTransferControl, StableServerId};

/// Identifies a specific server in the configuration.
#[derive(Debug, Clone, Copy, Hash, Eq, PartialEq)]
pub struct ServerId(pub usize);

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
    pools: Vec<Arc<Mutex<ServerPool>>>,
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
    retired_ips: Arc<Mutex<HashSet<(usize, IpAddr)>>>,
    connect_cursors: Vec<AtomicUsize>,
}

pub struct BlockingConnectionPermit {
    _permit: OwnedSemaphorePermit,
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
            connect_cursors.push(AtomicUsize::new(0));
            semaphores.push(Arc::new(Semaphore::new(spc.max_connections)));
            configs.push(spc.server.clone());
            pools.push(Arc::new(Mutex::new(ServerPool {
                config: spc.server.clone(),
                idle: VecDeque::new(),
                active_count: 0,
                max_connections: spc.max_connections,
            })));
            last_connect_failure.push(Arc::new(Mutex::new(None)));
        }

        let health = Arc::new(Mutex::new(HealthTracker::new(
            server_count,
            config.health_config,
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
            retired_ips: Arc::new(Mutex::new(HashSet::new())),
            connect_cursors,
        }
    }

    fn next_connect_offset(&self, idx: usize) -> usize {
        self.connect_cursors[idx].fetch_add(1, Ordering::Relaxed)
    }

    async fn retired_ips_for_server(&self, idx: usize) -> Vec<IpAddr> {
        self.retired_ips
            .lock()
            .await
            .iter()
            .filter_map(|(server_idx, ip)| (*server_idx == idx).then_some(*ip))
            .collect()
    }

    async fn connect_server_excluding(
        &self,
        idx: usize,
        excluded_ips: &[IpAddr],
    ) -> Result<NntpConnection> {
        let mut exclusions = self.retired_ips_for_server(idx).await;
        exclusions.extend(excluded_ips.iter().copied());
        exclusions.sort_unstable();
        exclusions.dedup();
        let offset = self.next_connect_offset(idx);
        let mut connection =
            NntpConnection::connect_with_ip_policy(&self.configs[idx], &exclusions, offset).await?;
        connection.set_transfer_control(self.transfer_controls[idx].clone());
        Ok(connection)
    }

    /// Acquire a connection from a specific server.
    pub async fn acquire(&self, server: ServerId) -> Result<PooledConnection> {
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

        self.acquire_with_permit(idx, Some(permit)).await
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
        if self.shutdown.is_cancelled() {
            return Err(NntpError::PoolShutdown);
        }

        let idx = server.0;
        if idx >= self.pools.len() {
            return Err(NntpError::PoolExhausted);
        }

        self.acquire_fresh_with_permit(idx, None, excluded_ips)
            .await
    }

    /// Internal: acquire a connection using an already-obtained permit.
    async fn acquire_with_permit(
        &self,
        idx: usize,
        permit: Option<tokio::sync::OwnedSemaphorePermit>,
    ) -> Result<PooledConnection> {
        // Try to get a healthy idle connection, with stale-check loop.
        let conn = loop {
            let candidate = {
                let mut pool = self.pools[idx].lock().await;
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
                                trace!(server = idx, error = %e, "stale ping failed, draining all idle");
                                // If one connection is dead, all are likely dead
                                // (network interface change). Drain everything so
                                // the loop falls through to create a fresh connection.
                                self.drain_all_idle().await;
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
                    match self.connect_server_excluding(idx, &[]).await {
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

        {
            let mut pool = self.pools[idx].lock().await;
            pool.active_count += 1;
        }

        Ok(PooledConnection {
            conn: Some(conn),
            pool: self.pools[idx].clone(),
            retired_ips: self.retired_ips.clone(),
            server_idx: idx,
            return_to_pool: permit.is_some(),
            _permit: permit,
        })
    }

    async fn acquire_fresh_with_permit(
        &self,
        idx: usize,
        permit: Option<tokio::sync::OwnedSemaphorePermit>,
        excluded_ips: &[IpAddr],
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
        let conn = match self.connect_server_excluding(idx, excluded_ips).await {
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

        {
            let mut pool = self.pools[idx].lock().await;
            pool.active_count += 1;
        }

        Ok(PooledConnection {
            conn: Some(conn),
            pool: self.pools[idx].clone(),
            retired_ips: self.retired_ips.clone(),
            server_idx: idx,
            return_to_pool: permit.is_some(),
            _permit: permit,
        })
    }

    /// Acquire a connection from any server that has capacity.
    ///
    /// Lower-numbered priority groups are exhausted before higher-numbered
    /// backfill groups are considered. Within a group, healthy servers are
    /// preferred over degraded ones, and immediately available servers are
    /// preferred over saturated ones.
    pub async fn acquire_any(&self) -> Result<PooledConnection> {
        if self.shutdown.is_cancelled() {
            return Err(NntpError::PoolShutdown);
        }

        let ordered = self.acquire_any_order().await;

        // Try non-blocking acquire on each server in group+health order.
        for idx in &ordered {
            match self.semaphores[*idx].clone().try_acquire_owned() {
                Ok(permit) => match self.acquire_with_permit(*idx, Some(permit)).await {
                    Ok(conn) => return Ok(conn),
                    Err(e) => {
                        warn!(server = idx, error = %e, "failed to acquire from server, trying next");
                        continue;
                    }
                },
                Err(_) => continue, // No permits available, try next server.
            }
        }

        // All available servers at capacity or no available servers;
        // fall back to blocking on the first available, or the first fill
        // server if all are disabled — generic callers must never be handed
        // a backfill connection.
        if let Some(&first) = ordered.first() {
            self.acquire(ServerId(first)).await
        } else {
            let fallback = self
                .backfill
                .iter()
                .position(|backfill| !*backfill)
                .unwrap_or(0);
            self.acquire(ServerId(fallback)).await
        }
    }

    async fn acquire_any_order(&self) -> Vec<usize> {
        #[derive(Default)]
        struct GroupCandidates {
            ready_healthy: Vec<usize>,
            ready_degraded: Vec<usize>,
            waiting_healthy: Vec<usize>,
            waiting_degraded: Vec<usize>,
        }

        let mut health = self.health.lock().await;
        health.check_reenable_all();

        let mut groups: std::collections::BTreeMap<u32, GroupCandidates> =
            std::collections::BTreeMap::new();
        for idx in 0..self.server_count() {
            if !health.is_available(idx) {
                continue;
            }
            // acquire_any serves ordinary callers with no failure history;
            // backfill servers are only reachable through the per-request
            // exclusion ladder (build_server_order).
            if self.backfill[idx] {
                continue;
            }
            let entry = groups.entry(self.groups[idx]).or_default();
            let ready = self.semaphores[idx].available_permits() > 0;
            match health.server(idx).state() {
                crate::health::ServerState::Healthy => {
                    if ready {
                        entry.ready_healthy.push(idx);
                    } else {
                        entry.waiting_healthy.push(idx);
                    }
                }
                crate::health::ServerState::Degraded { .. } => {
                    if ready {
                        entry.ready_degraded.push(idx);
                    } else {
                        entry.waiting_degraded.push(idx);
                    }
                }
                crate::health::ServerState::CoolingDown { .. }
                | crate::health::ServerState::Disabled { .. } => {}
            }
        }

        let mut ordered = Vec::with_capacity(self.server_count());
        for (_group, candidates) in groups {
            ordered.extend(candidates.ready_healthy);
            ordered.extend(candidates.ready_degraded);
            ordered.extend(candidates.waiting_healthy);
            ordered.extend(candidates.waiting_degraded);
        }
        ordered
    }

    /// Drain all idle connections across all servers.
    ///
    /// Called when a network change is suspected (e.g. I/O errors after an
    /// interface switch). Connections are dropped without recording health
    /// failures, since the servers themselves are fine.
    pub async fn drain_all_idle(&self) {
        let mut total = 0usize;
        for pool in &self.pools {
            let mut p = pool.lock().await;
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

    /// Gracefully shut down the pool, closing all idle connections.
    pub async fn shutdown(&self) {
        self.shutdown.cancel();

        for pool in &self.pools {
            let mut pool = pool.lock().await;
            for mut conn in pool.idle.drain(..) {
                let _ = conn.quit().await;
            }
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
            .map(|(_, max)| *max)
            .sum()
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
            .map_err(|_| NntpError::TooManyConnections)?;
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

    /// Returns `(available_permits, max_connections)` for the given server.
    ///
    /// This is lock-free — it reads semaphore permits and the pre-stored
    /// max_connections value, so it can be called from synchronous contexts.
    pub fn server_load(&self, idx: usize) -> (usize, usize) {
        let available = self.semaphores[idx].available_permits();
        let max = self.max_connections[idx];
        (available, max)
    }

    pub async fn retire_ip(&self, server: ServerId, ip: IpAddr) {
        let idx = server.0;
        if idx >= self.pools.len() {
            return;
        }
        {
            let mut retired = self.retired_ips.lock().await;
            retired.insert((idx, ip));
        }
        let mut pool = self.pools[idx].lock().await;
        pool.idle.retain(|conn| conn.remote_ip() != ip);
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
    pool: Arc<Mutex<ServerPool>>,
    retired_ips: Arc<Mutex<HashSet<(usize, IpAddr)>>>,
    server_idx: usize,
    return_to_pool: bool,
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
            let pool = self.pool.clone();
            let server_idx = self.server_idx;
            drop(tokio::spawn(async move {
                let mut pool = pool.lock().await;
                pool.active_count = pool.active_count.saturating_sub(1);
                trace!(server = server_idx, "discarded connection");
            }));
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
    fn drop(&mut self) {
        if let Some(conn) = self.conn.take() {
            let pool = self.pool.clone();
            let retired_ips = self.retired_ips.clone();
            let server_idx = self.server_idx;
            let poisoned = conn.is_poisoned();
            let return_to_pool = self.return_to_pool;

            if conn.is_healthy() && return_to_pool {
                // tokio::spawn can fail during runtime shutdown; if so, the
                // connection is simply dropped (permit released by _permit).
                drop(tokio::spawn(async move {
                    let mut pool = pool.lock().await;
                    pool.active_count = pool.active_count.saturating_sub(1);
                    let retired = retired_ips.lock().await;
                    if retired.contains(&(server_idx, conn.remote_ip())) {
                        trace!(server = server_idx, "dropped retired-ip connection");
                    } else {
                        drop(retired);
                        pool.idle.push_back(conn);
                        trace!(server = server_idx, "returned connection to pool");
                    }
                }));
            } else {
                drop(tokio::spawn(async move {
                    let mut pool = pool.lock().await;
                    pool.active_count = pool.active_count.saturating_sub(1);
                    if conn.is_healthy() {
                        trace!(server = server_idx, "dropped non-poolable connection");
                    } else if poisoned {
                        trace!(server = server_idx, "dropped poisoned connection");
                    } else {
                        trace!(server = server_idx, "dropped unhealthy connection");
                    }
                }));
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
    async fn acquire_any_order_skips_backfill_servers() {
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
        assert_eq!(
            pool.acquire_any_order().await,
            vec![0],
            "ordinary callers must never be handed a backfill connection"
        );
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

    #[tokio::test]
    async fn acquire_any_order_keeps_backfill_after_primary_group() {
        let config = PoolConfig {
            servers: vec![
                ServerPoolConfig {
                    server: ServerConfig {
                        host: "waiting-primary.example.com".into(),
                        ..Default::default()
                    },
                    max_connections: 0,
                    group: 0,
                    ..ServerPoolConfig::default()
                },
                ServerPoolConfig {
                    server: ServerConfig {
                        host: "ready-backfill.example.com".into(),
                        ..Default::default()
                    },
                    max_connections: 1,
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

        let ordered = pool.acquire_any_order().await;
        assert_eq!(ordered, vec![0, 1]);
    }

    #[tokio::test]
    async fn acquire_any_order_prefers_ready_servers_within_primary_group() {
        let config = PoolConfig {
            servers: vec![
                ServerPoolConfig {
                    server: ServerConfig {
                        host: "waiting-primary.example.com".into(),
                        ..Default::default()
                    },
                    max_connections: 0,
                    group: 0,
                    ..ServerPoolConfig::default()
                },
                ServerPoolConfig {
                    server: ServerConfig {
                        host: "ready-primary.example.com".into(),
                        ..Default::default()
                    },
                    max_connections: 1,
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

        let ordered = pool.acquire_any_order().await;
        assert_eq!(ordered, vec![1, 0]);
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
}
