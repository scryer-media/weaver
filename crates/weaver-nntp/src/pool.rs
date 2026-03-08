use std::collections::VecDeque;
use std::ops::{Deref, DerefMut};
use std::sync::Arc;
use std::time::{Duration, Instant};

use tokio::sync::{Mutex, Semaphore};
use tokio_util::sync::CancellationToken;
use tracing::{debug, trace, warn};

use crate::connection::{NntpConnection, ServerConfig};
use crate::error::{NntpError, Result};
use crate::health::{HealthConfig, HealthTracker};

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
    semaphores: Vec<Arc<Semaphore>>,
    shutdown: CancellationToken,
    max_idle_age: Duration,
    health: Arc<Mutex<HealthTracker>>,
    /// Per-server timestamp of the last failed connection attempt.
    last_connect_failure: Vec<Arc<Mutex<Option<Instant>>>>,
    reconnect_delay: Duration,
    stale_check_age: Duration,
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
            max_idle_age: Duration::from_secs(300),
            health_config: HealthConfig::default(),
            reconnect_delay: Duration::from_secs(1),
            stale_check_age: Duration::from_secs(30),
        }
    }
}

/// Per-server pool configuration.
pub struct ServerPoolConfig {
    pub server: ServerConfig,
    pub max_connections: usize,
}

impl NntpPool {
    /// Create a new multi-server connection pool.
    pub fn new(config: PoolConfig) -> Self {
        let server_count = config.servers.len();
        let mut pools = Vec::with_capacity(server_count);
        let mut configs = Vec::with_capacity(server_count);
        let mut semaphores = Vec::with_capacity(server_count);
        let mut last_connect_failure = Vec::with_capacity(server_count);

        for spc in &config.servers {
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
            semaphores,
            shutdown: CancellationToken::new(),
            max_idle_age: config.max_idle_age,
            health,
            last_connect_failure,
            reconnect_delay: config.reconnect_delay,
            stale_check_age: config.stale_check_age,
        }
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

        self.acquire_with_permit(idx, permit).await
    }

    /// Internal: acquire a connection using an already-obtained permit.
    async fn acquire_with_permit(
        &self,
        idx: usize,
        permit: tokio::sync::OwnedSemaphorePermit,
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
                                trace!(server = idx, error = %e, "stale ping failed, discarding");
                                // Discard and loop to try the next idle connection.
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
                    match NntpConnection::connect(&self.configs[idx]).await {
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
            server_idx: idx,
            health: self.health.clone(),
            _permit: permit,
        })
    }

    /// Acquire a connection from any server that has capacity.
    /// Tries servers in health-priority order (healthy first, degraded second,
    /// disabled servers are skipped).
    pub async fn acquire_any(&self) -> Result<PooledConnection> {
        if self.shutdown.is_cancelled() {
            return Err(NntpError::PoolShutdown);
        }

        let ordered = {
            let mut health = self.health.lock().await;
            health.ordered_servers()
        };

        // Try non-blocking acquire on each server in health order.
        for idx in &ordered {
            match self.semaphores[*idx].clone().try_acquire_owned() {
                Ok(permit) => {
                    match self.acquire_with_permit(*idx, permit).await {
                        Ok(conn) => return Ok(conn),
                        Err(e) => {
                            warn!(server = idx, error = %e, "failed to acquire from server, trying next");
                            continue;
                        }
                    }
                }
                Err(_) => continue, // No permits available, try next server.
            }
        }

        // All available servers at capacity or no available servers;
        // fall back to blocking on the first available, or server 0 if all disabled.
        if let Some(&first) = ordered.first() {
            self.acquire(ServerId(first)).await
        } else {
            // All servers disabled — fall back to server 0.
            self.acquire(ServerId(0)).await
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

    /// Access the health tracker for observability.
    pub fn health(&self) -> &Arc<Mutex<HealthTracker>> {
        &self.health
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
    server_idx: usize,
    health: Arc<Mutex<HealthTracker>>,
    _permit: tokio::sync::OwnedSemaphorePermit,
}

impl PooledConnection {
    /// Explicitly discard this connection instead of returning it to the pool.
    /// Use when the connection is in a bad state.
    pub fn discard(mut self) {
        self.conn = None;
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
            let health = self.health.clone();
            let server_idx = self.server_idx;
            let poisoned = conn.is_poisoned();

            if conn.is_healthy() {
                // tokio::spawn can fail during runtime shutdown; if so, the
                // connection is simply dropped (permit released by _permit).
                let _ = tokio::spawn(async move {
                    {
                        let mut h = health.lock().await;
                        h.record_success(server_idx);
                    }
                    let mut pool = pool.lock().await;
                    pool.active_count = pool.active_count.saturating_sub(1);
                    pool.idle.push_back(conn);
                    trace!("returned connection to pool");
                });
            } else {
                let _ = tokio::spawn(async move {
                    {
                        let mut h = health.lock().await;
                        h.record_failure(server_idx, false);
                    }
                    let mut pool = pool.lock().await;
                    pool.active_count = pool.active_count.saturating_sub(1);
                    if poisoned {
                        trace!("dropped poisoned connection, recorded failure");
                    } else {
                        trace!("dropped unhealthy connection, recorded failure");
                    }
                });
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
            }],
            max_idle_age: Duration::from_secs(300),
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
    fn pool_multi_server() {
        let config = PoolConfig {
            servers: vec![
                ServerPoolConfig {
                    server: ServerConfig {
                        host: "primary.example.com".into(),
                        ..Default::default()
                    },
                    max_connections: 10,
                },
                ServerPoolConfig {
                    server: ServerConfig {
                        host: "backup.example.com".into(),
                        ..Default::default()
                    },
                    max_connections: 5,
                },
            ],
            max_idle_age: Duration::from_secs(300),
            health_config: HealthConfig::default(),
            reconnect_delay: Duration::from_secs(1),
            stale_check_age: Duration::from_secs(30),
        };
        let pool = NntpPool::new(config);
        assert_eq!(pool.server_count(), 2);
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
                },
                ServerPoolConfig {
                    server: ServerConfig {
                        host: "b.example.com".into(),
                        ..Default::default()
                    },
                    max_connections: 5,
                },
            ],
            max_idle_age: Duration::from_secs(300),
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
            }],
            max_idle_age: Duration::from_secs(300),
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
            }],
            max_idle_age: Duration::from_secs(300),
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
        assert_eq!(config.max_idle_age, Duration::from_secs(300));
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
}
