use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use rand::distr::weighted::WeightedIndex;
use rand::prelude::*;
use tracing::{debug, warn};

use crate::connection::ServerConfig;
use crate::error::{NntpError, Result};
use crate::pool::{NntpPool, PoolConfig, PooledConnection, ServerId, ServerPoolConfig};

/// Configuration for the high-level NNTP client.
pub struct NntpClientConfig {
    /// Server configurations in priority order (primary first).
    pub servers: Vec<ServerPoolConfig>,
    /// Maximum age for idle connections before eviction.
    pub max_idle_age: Duration,
    /// Maximum number of retries on the same server for transient errors
    /// before failing over to the next server. A value of 1 means try once,
    /// then retry once (2 total attempts per server).
    pub max_retries_per_server: u32,
    /// Per-article soft timeout. If a fetch from a single server exceeds this
    /// duration, the client fails over to the next server. This is separate
    /// from the connection-level `command_timeout` (hard ceiling per connection).
    pub soft_timeout: Duration,
}

impl NntpClientConfig {
    /// Create a client config with a single server.
    pub fn single(server: ServerConfig, max_connections: usize) -> Self {
        NntpClientConfig {
            servers: vec![ServerPoolConfig {
                server,
                max_connections,
                group: 0,
            }],
            max_idle_age: Duration::from_secs(300),
            max_retries_per_server: 1,
            soft_timeout: Duration::from_secs(15),
        }
    }
}

/// High-level NNTP client with multi-server failover.
///
/// Provides a simple API for fetching articles, automatically managing
/// connection pooling and falling back to backup servers when an article
/// is not found on the primary server.
pub struct NntpClient {
    pool: Arc<NntpPool>,
    max_retries_per_server: u32,
    /// Per-article soft timeout — triggers failover to the next server.
    soft_timeout: Duration,
}

impl NntpClient {
    /// Create a new NNTP client from the given configuration.
    pub fn new(config: NntpClientConfig) -> Self {
        let max_retries_per_server = config.max_retries_per_server;
        let soft_timeout = config.soft_timeout;
        let pool = NntpPool::new(PoolConfig {
            servers: config.servers,
            max_idle_age: config.max_idle_age,
            ..PoolConfig::default()
        });

        NntpClient {
            pool: Arc::new(pool),
            max_retries_per_server,
            soft_timeout,
        }
    }

    /// Create a client wrapping an existing pool.
    pub fn from_pool(pool: Arc<NntpPool>) -> Self {
        NntpClient {
            pool,
            max_retries_per_server: 1,
            soft_timeout: Duration::from_secs(15),
        }
    }

    /// Fetch the body of an article by message-id, with multi-server failover.
    ///
    /// Tries each server in priority order. Falls back to the next server
    /// on `ArticleNotFound` (430). Retries on the same server for transient
    /// errors before escalating.
    pub async fn fetch_body(&self, message_id: &str) -> Result<Bytes> {
        self.fetch_with_failover(message_id, FetchKind::Body).await
    }

    /// Fetch the body of an article, selecting a newsgroup first if required.
    ///
    /// Some NNTP servers require a GROUP command before BODY will succeed.
    /// This method tries each group in `groups` until one succeeds, then
    /// issues the BODY command. If `groups` is empty it behaves identically
    /// to [`fetch_body`](Self::fetch_body).
    pub async fn fetch_body_with_groups(
        &self,
        message_id: &str,
        groups: &[String],
    ) -> Result<Bytes> {
        if groups.is_empty() {
            return self.fetch_body(message_id).await;
        }

        let order = self.build_server_order(&[]).await;
        let mut last_error: Option<NntpError> = None;

        for idx in order {
            let server = ServerId(idx);
            let start = Instant::now();

            match self
                .fetch_from_server_with_groups(server, message_id, groups)
                .await
            {
                Ok(data) => {
                    let elapsed = start.elapsed();
                    let mut health = self.pool.health().lock().await;
                    health.record_success(idx);
                    health.record_latency(idx, elapsed);
                    return Ok(data);
                }
                Err(NntpError::ArticleNotFound)
                | Err(NntpError::NoSuchArticle { .. })
                | Err(NntpError::NoArticleWithNumber) => {
                    debug!(
                        server = idx,
                        message_id, "article not found, trying next server"
                    );
                    last_error = Some(NntpError::NoSuchArticle {
                        message_id: message_id.to_string(),
                    });
                    continue;
                }
                Err(NntpError::AuthenticationFailed)
                | Err(NntpError::AuthenticationRejected)
                | Err(NntpError::AccessDenied) => {
                    warn!(
                        server = idx,
                        message_id, "authentication/access failure, trying next server"
                    );
                    {
                        let mut health = self.pool.health().lock().await;
                        health.record_failure(idx, true);
                    }
                    last_error = Some(NntpError::AuthenticationFailed);
                    continue;
                }
                Err(e) if is_transient(&e) => {
                    warn!(
                        server = idx,
                        error = %e,
                        message_id,
                        "transient error, trying next server"
                    );
                    {
                        let mut health = self.pool.health().lock().await;
                        health.record_failure(idx, false);
                    }
                    last_error = Some(e);
                    continue;
                }
                Err(e) => return Err(e),
            }
        }

        Err(last_error.unwrap_or(NntpError::PoolExhausted))
    }

    /// Like [`fetch_body_with_groups`](Self::fetch_body_with_groups) but skips
    /// the specified servers. Used after decode failures to avoid re-downloading
    /// from a server that returned corrupt data.
    pub async fn fetch_body_with_groups_excluding(
        &self,
        message_id: &str,
        groups: &[String],
        exclude: &[usize],
    ) -> Result<Bytes> {
        if groups.is_empty() {
            return self.fetch_body(message_id).await;
        }

        let order = self.build_server_order(exclude).await;

        if order.is_empty() {
            return Err(NntpError::PoolExhausted);
        }

        let mut last_error: Option<NntpError> = None;

        for idx in order {
            let server = ServerId(idx);
            let start = Instant::now();

            match self
                .fetch_from_server_with_groups(server, message_id, groups)
                .await
            {
                Ok(data) => {
                    let elapsed = start.elapsed();
                    let mut health = self.pool.health().lock().await;
                    health.record_success(idx);
                    health.record_latency(idx, elapsed);
                    return Ok(data);
                }
                Err(NntpError::ArticleNotFound)
                | Err(NntpError::NoSuchArticle { .. })
                | Err(NntpError::NoArticleWithNumber) => {
                    last_error = Some(NntpError::NoSuchArticle {
                        message_id: message_id.to_string(),
                    });
                    continue;
                }
                Err(e) if is_transient(&e) => {
                    {
                        let mut health = self.pool.health().lock().await;
                        health.record_failure(idx, false);
                    }
                    last_error = Some(e);
                    continue;
                }
                Err(e) => return Err(e),
            }
        }

        Err(last_error.unwrap_or(NntpError::PoolExhausted))
    }

    /// Fetch the headers of an article by message-id, with multi-server failover.
    pub async fn fetch_head(&self, message_id: &str) -> Result<Bytes> {
        self.fetch_with_failover(message_id, FetchKind::Head).await
    }

    /// Fetch a complete article (headers + body) by message-id, with multi-server failover.
    pub async fn fetch_article(&self, message_id: &str) -> Result<Bytes> {
        self.fetch_with_failover(message_id, FetchKind::Article)
            .await
    }

    /// Gracefully shut down the client and all pooled connections.
    pub async fn shutdown(&self) {
        self.pool.shutdown().await;
    }

    /// Access the underlying connection pool.
    pub fn pool(&self) -> &Arc<NntpPool> {
        &self.pool
    }

    /// Build the server try-order, grouping by priority and ranking within each
    /// group using latency-weighted random selection.
    ///
    /// Servers in `exclude` are skipped entirely. Disabled servers (circuit-
    /// breaker tripped) are also excluded so we don't waste time on servers
    /// that are known to be failing.
    async fn build_server_order(&self, exclude: &[usize]) -> Vec<usize> {
        let server_count = self.pool.server_count();
        let server_groups = self.pool.server_groups();

        // Check health to skip disabled servers.
        let mut health = self.pool.health().lock().await;
        health.check_reenable_all();

        // Collect available servers, grouped by priority.
        let mut groups: std::collections::BTreeMap<u32, Vec<usize>> =
            std::collections::BTreeMap::new();
        for idx in 0..server_count {
            if !exclude.contains(&idx) && health.is_available(idx) {
                groups.entry(server_groups[idx]).or_default().push(idx);
            }
        }
        drop(health);

        let mut result = Vec::with_capacity(server_count);
        for (_priority, servers) in groups {
            let ranked = self.rank_servers_in_group(&servers).await;
            result.extend(ranked);
        }
        result
    }

    /// Rank servers within a single priority group using weighted random selection
    /// based on latency EWMA and current load.
    ///
    /// The first server is chosen probabilistically (weight = 1/score), and the
    /// rest are appended in ascending score order. This ensures that faster,
    /// less-loaded servers are tried first while still providing load distribution.
    async fn rank_servers_in_group(&self, servers: &[usize]) -> Vec<usize> {
        if servers.len() <= 1 {
            return servers.to_vec();
        }

        // Compute scores: latency_ms * (1.0 + 2.0 * load_ratio) * premature_death_penalty.
        let scores: Vec<(usize, f64)> = {
            let health = self.pool.health().lock().await;
            servers
                .iter()
                .map(|&idx| {
                    let latency = health.latency_ms(idx);
                    let (available, max) = self.pool.server_load(idx);
                    let load_ratio = if max == 0 {
                        1.0
                    } else {
                        1.0 - (available as f64 / max as f64)
                    };
                    // Penalize servers whose connections keep dying prematurely.
                    let deaths = health.recent_premature_deaths(idx) as f64;
                    let death_penalty = 1.0 + 0.5 * deaths;
                    let score = latency * (1.0 + 2.0 * load_ratio) * death_penalty;
                    (idx, score.max(0.001)) // floor to avoid division by zero
                })
                .collect()
        };

        // Weighted random pick for the first server (weight = 1/score).
        let weights: Vec<f64> = scores.iter().map(|(_, s)| 1.0 / s).collect();
        let first_idx = match WeightedIndex::new(&weights) {
            Ok(dist) => {
                let mut rng = rand::rng();
                dist.sample(&mut rng)
            }
            Err(_) => 0, // fallback if all weights are zero/invalid
        };

        let mut result = Vec::with_capacity(scores.len());
        result.push(scores[first_idx].0);

        // Remaining servers sorted by ascending score.
        let mut rest: Vec<(usize, f64)> = scores
            .into_iter()
            .enumerate()
            .filter(|(i, _)| *i != first_idx)
            .map(|(_, pair)| pair)
            .collect();
        rest.sort_by(|a, b| a.1.partial_cmp(&b.1).unwrap_or(std::cmp::Ordering::Equal));
        result.extend(rest.iter().map(|(idx, _)| *idx));

        result
    }

    /// Internal: fetch with multi-server failover logic.
    async fn fetch_with_failover(&self, message_id: &str, kind: FetchKind) -> Result<Bytes> {
        let order = self.build_server_order(&[]).await;
        let mut last_error: Option<NntpError> = None;

        for idx in order {
            let server = ServerId(idx);
            let start = Instant::now();

            match self.fetch_from_server(server, message_id, kind).await {
                Ok(data) => {
                    let elapsed = start.elapsed();
                    let mut health = self.pool.health().lock().await;
                    health.record_success(idx);
                    health.record_latency(idx, elapsed);
                    return Ok(data);
                }
                Err(NntpError::ArticleNotFound)
                | Err(NntpError::NoSuchArticle { .. })
                | Err(NntpError::NoArticleWithNumber) => {
                    debug!(
                        server = idx,
                        message_id, "article not found, trying next server"
                    );
                    last_error = Some(NntpError::NoSuchArticle {
                        message_id: message_id.to_string(),
                    });
                    continue;
                }
                Err(NntpError::AuthenticationFailed)
                | Err(NntpError::AuthenticationRejected)
                | Err(NntpError::AccessDenied) => {
                    warn!(
                        server = idx,
                        message_id, "authentication/access failure, trying next server"
                    );
                    {
                        let mut health = self.pool.health().lock().await;
                        health.record_failure(idx, true);
                    }
                    last_error = Some(NntpError::AuthenticationFailed);
                    continue;
                }
                Err(e) if is_transient(&e) => {
                    warn!(
                        server = idx,
                        error = %e,
                        message_id,
                        "transient error, trying next server"
                    );
                    {
                        let mut health = self.pool.health().lock().await;
                        health.record_failure(idx, false);
                    }
                    last_error = Some(e);
                    continue;
                }
                Err(e) => {
                    // Non-recoverable error (pool shutdown, etc.)
                    return Err(e);
                }
            }
        }

        // All servers exhausted.
        Err(last_error.unwrap_or(NntpError::PoolExhausted))
    }

    /// Try to select one of the given groups on the connection.
    ///
    /// Returns `Ok(true)` if a group was selected, `Ok(false)` if none were
    /// accepted, or `Err` on a connection-level error.
    async fn try_select_group(conn: &mut PooledConnection, groups: &[String]) -> Result<bool> {
        for group in groups {
            match conn.select_group(group).await {
                Ok(()) => return Ok(true),
                Err(e) if is_connection_error(&e) => return Err(e),
                Err(_) => continue,
            }
        }
        Ok(false)
    }

    /// Fetch from a specific server, selecting a newsgroup first.
    ///
    /// Tries each group in order. If selecting a group fails, tries the next.
    /// Once a group is selected, issues the BODY command. Retries on transient
    /// errors up to `max_retries_per_server` times.
    ///
    /// The entire operation is bounded by the soft timeout — if it fires, the
    /// caller should fail over to the next server.
    async fn fetch_from_server_with_groups(
        &self,
        server: ServerId,
        message_id: &str,
        groups: &[String],
    ) -> Result<Bytes> {
        let soft_timeout = self.soft_timeout;
        match tokio::time::timeout(
            soft_timeout,
            self.fetch_from_server_with_groups_inner(server, message_id, groups),
        )
        .await
        {
            Ok(result) => result,
            Err(_) => Err(NntpError::SoftTimeout(soft_timeout.as_secs())),
        }
    }

    /// Inner implementation of fetch_from_server_with_groups without the soft timeout wrapper.
    async fn fetch_from_server_with_groups_inner(
        &self,
        server: ServerId,
        message_id: &str,
        groups: &[String],
    ) -> Result<Bytes> {
        let mut attempts = 0u32;

        loop {
            let mut conn = self.pool.acquire(server).await?;

            // Try to select a group — iterate through the list on failure.
            let group_result = Self::try_select_group(&mut conn, groups).await;

            match group_result {
                Err(e) if is_transient(&e) => {
                    if is_connection_error(&e) {
                        conn.discard();
                        self.pool.drain_all_idle().await;
                    }
                    if attempts < self.max_retries_per_server {
                        attempts += 1;
                        // Delay before retry to avoid hammering during reconnect throttle.
                        tokio::time::sleep(Duration::from_millis(200 * attempts as u64)).await;
                        debug!(
                            server = server.0,
                            attempt = attempts,
                            error = %e,
                            message_id,
                            "connection error during group selection, retrying"
                        );
                        continue;
                    }
                    return Err(e);
                }
                Err(e) => return Err(e),
                Ok(false) => {
                    // None of the groups were accepted; still attempt the BODY command
                    // in case the server doesn't require GROUP first.
                    debug!(
                        message_id,
                        "no group could be selected, attempting BODY anyway"
                    );
                }
                Ok(true) => {}
            }

            let result = conn.body_by_id_raw(message_id).await;
            match result {
                Ok(response) => return Ok(response.data),
                Err(NntpError::ArticleNotFound)
                | Err(NntpError::NoSuchArticle { .. })
                | Err(NntpError::NoArticleWithNumber) => {
                    return Err(result.unwrap_err());
                }
                Err(e) if is_transient(&e) => {
                    if is_connection_error(&e) {
                        conn.discard();
                        self.pool.drain_all_idle().await;
                    }
                    if attempts < self.max_retries_per_server {
                        attempts += 1;
                        tokio::time::sleep(Duration::from_millis(200 * attempts as u64)).await;
                        debug!(
                            server = server.0,
                            attempt = attempts,
                            error = %e,
                            message_id,
                            "transient error, retrying on same server"
                        );
                        continue;
                    }
                    return Err(e);
                }
                Err(e) => {
                    if is_connection_error(&e) {
                        conn.discard();
                        self.pool.drain_all_idle().await;
                    }
                    return Err(e);
                }
            }
        }
    }

    /// Fetch from a specific server, retrying on transient errors.
    ///
    /// Bounded by the soft timeout — if exceeded, returns `SoftTimeout` to
    /// trigger failover at the caller.
    async fn fetch_from_server(
        &self,
        server: ServerId,
        message_id: &str,
        kind: FetchKind,
    ) -> Result<Bytes> {
        let soft_timeout = self.soft_timeout;
        match tokio::time::timeout(
            soft_timeout,
            self.fetch_from_server_inner(server, message_id, kind),
        )
        .await
        {
            Ok(result) => result,
            Err(_) => Err(NntpError::SoftTimeout(soft_timeout.as_secs())),
        }
    }

    /// Inner implementation of fetch_from_server without the soft timeout wrapper.
    async fn fetch_from_server_inner(
        &self,
        server: ServerId,
        message_id: &str,
        kind: FetchKind,
    ) -> Result<Bytes> {
        let mut attempts = 0u32;

        loop {
            let mut conn = self.pool.acquire(server).await?;

            let result = match kind {
                FetchKind::Body => conn.body_by_id_raw(message_id).await,
                FetchKind::Head => conn.head_by_id(message_id).await,
                FetchKind::Article => conn.article_by_id(message_id).await,
            };

            match result {
                Ok(response) => return Ok(response.data),
                Err(NntpError::ArticleNotFound) | Err(NntpError::NoSuchArticle { .. }) => {
                    // Article not found — do not retry, return immediately.
                    return Err(result.unwrap_err());
                }
                Err(e) if is_transient(&e) => {
                    if is_connection_error(&e) {
                        conn.discard();
                        self.pool.drain_all_idle().await;
                    }
                    if attempts < self.max_retries_per_server {
                        attempts += 1;
                        tokio::time::sleep(Duration::from_millis(200 * attempts as u64)).await;
                        debug!(
                            server = server.0,
                            attempt = attempts,
                            error = %e,
                            message_id,
                            "transient error, retrying on same server"
                        );
                        continue;
                    }
                    return Err(e);
                }
                Err(e) => {
                    if is_connection_error(&e) {
                        conn.discard();
                        self.pool.drain_all_idle().await;
                    }
                    return Err(e);
                }
            }
        }
    }
}

/// The type of article fetch to perform.
#[derive(Debug, Clone, Copy)]
enum FetchKind {
    Body,
    Head,
    Article,
}

/// Returns true if the error is transient and the request might succeed on retry.
fn is_transient(err: &NntpError) -> bool {
    matches!(
        err,
        NntpError::Io(_)
            | NntpError::Timeout
            | NntpError::ConnectionClosed
            | NntpError::ServiceUnavailable
            | NntpError::TooManyConnections
            | NntpError::PoolExhausted
            | NntpError::SoftTimeout(_)
    )
}

/// Returns true if the error indicates the connection itself is bad
/// and should be discarded rather than returned to the pool.
fn is_connection_error(err: &NntpError) -> bool {
    matches!(
        err,
        NntpError::Io(_)
            | NntpError::Timeout
            | NntpError::ConnectionClosed
            | NntpError::TooManyConnections
            | NntpError::AccessDenied
            | NntpError::SoftTimeout(_)
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fetch_kind_is_copy() {
        let kind = FetchKind::Body;
        let _copy = kind;
        let _another = kind;
    }

    #[test]
    fn transient_errors() {
        assert!(is_transient(&NntpError::Timeout));
        assert!(is_transient(&NntpError::ConnectionClosed));
        assert!(is_transient(&NntpError::ServiceUnavailable));
        assert!(is_transient(&NntpError::TooManyConnections));
        assert!(is_transient(&NntpError::PoolExhausted));
        assert!(is_transient(&NntpError::Io(std::io::Error::new(
            std::io::ErrorKind::ConnectionReset,
            "reset"
        ))));

        // Non-transient errors
        assert!(!is_transient(&NntpError::AuthenticationFailed));
        assert!(!is_transient(&NntpError::AccessDenied));
        assert!(!is_transient(&NntpError::PoolShutdown));
        assert!(!is_transient(&NntpError::ArticleNotFound));
    }

    #[test]
    fn connection_errors() {
        assert!(is_connection_error(&NntpError::Timeout));
        assert!(is_connection_error(&NntpError::ConnectionClosed));
        assert!(is_connection_error(&NntpError::TooManyConnections));
        assert!(is_connection_error(&NntpError::AccessDenied));
        assert!(!is_connection_error(&NntpError::ArticleNotFound));
        assert!(!is_connection_error(&NntpError::AuthenticationFailed));
    }

    #[test]
    fn client_config_default_retries() {
        let config = NntpClientConfig::single(
            ServerConfig {
                host: "news.example.com".into(),
                ..Default::default()
            },
            5,
        );
        assert_eq!(config.max_retries_per_server, 1);
    }

    #[test]
    fn client_config_custom_retries() {
        let mut config = NntpClientConfig::single(
            ServerConfig {
                host: "news.example.com".into(),
                ..Default::default()
            },
            5,
        );
        config.max_retries_per_server = 3;
        assert_eq!(config.max_retries_per_server, 3);

        let client = NntpClient::new(config);
        assert_eq!(client.max_retries_per_server, 3);
    }

    #[test]
    fn from_pool_default_retries() {
        let config = NntpClientConfig::single(
            ServerConfig {
                host: "news.example.com".into(),
                ..Default::default()
            },
            5,
        );
        let pool = Arc::new(NntpPool::new(PoolConfig {
            servers: config.servers,
            max_idle_age: config.max_idle_age,
            ..PoolConfig::default()
        }));
        let client = NntpClient::from_pool(pool);
        assert_eq!(client.max_retries_per_server, 1);
    }

    #[test]
    fn transient_errors_are_retriable() {
        // Verify that the errors we consider transient would trigger retries
        assert!(is_transient(&NntpError::Timeout));
        assert!(is_transient(&NntpError::ConnectionClosed));
        assert!(is_transient(&NntpError::ServiceUnavailable));
        assert!(is_transient(&NntpError::TooManyConnections));
        assert!(is_transient(&NntpError::PoolExhausted));

        // These should NOT trigger retries
        assert!(!is_transient(&NntpError::ArticleNotFound));
        assert!(!is_transient(&NntpError::AuthenticationFailed));
        assert!(!is_transient(&NntpError::AccessDenied));
        assert!(!is_transient(&NntpError::NoSuchArticle {
            message_id: "<test@example.com>".into(),
        }));
    }

    #[test]
    fn client_config_single() {
        let config = NntpClientConfig::single(
            ServerConfig {
                host: "news.example.com".into(),
                port: 563,
                tls: true,
                ..Default::default()
            },
            10,
        );
        assert_eq!(config.servers.len(), 1);
        assert_eq!(config.servers[0].max_connections, 10);
        assert_eq!(config.max_idle_age, Duration::from_secs(300));
    }

    #[test]
    fn client_creation() {
        let config = NntpClientConfig::single(
            ServerConfig {
                host: "news.example.com".into(),
                ..Default::default()
            },
            5,
        );
        let client = NntpClient::new(config);
        assert_eq!(client.pool().server_count(), 1);
    }

    #[tokio::test]
    async fn client_shutdown() {
        let config = NntpClientConfig::single(
            ServerConfig {
                host: "news.example.com".into(),
                ..Default::default()
            },
            5,
        );
        let client = NntpClient::new(config);
        client.shutdown().await;

        // After shutdown, fetches should fail
        let result = client.fetch_body("<test@example.com>").await;
        assert!(result.is_err());
    }

    #[test]
    fn soft_timeout_is_transient() {
        assert!(is_transient(&NntpError::SoftTimeout(15)));
    }

    #[test]
    fn soft_timeout_is_connection_error() {
        assert!(is_connection_error(&NntpError::SoftTimeout(15)));
    }

    #[test]
    fn soft_timeout_display() {
        let err = NntpError::SoftTimeout(15);
        assert_eq!(err.to_string(), "article fetch soft timeout (15s)");
    }

    #[test]
    fn client_config_default_soft_timeout() {
        let config = NntpClientConfig::single(
            ServerConfig {
                host: "news.example.com".into(),
                ..Default::default()
            },
            5,
        );
        assert_eq!(config.soft_timeout, Duration::from_secs(15));
    }

    #[test]
    fn client_soft_timeout_propagated() {
        let mut config = NntpClientConfig::single(
            ServerConfig {
                host: "news.example.com".into(),
                ..Default::default()
            },
            5,
        );
        config.soft_timeout = Duration::from_secs(30);
        let client = NntpClient::new(config);
        assert_eq!(client.soft_timeout, Duration::from_secs(30));
    }

    #[test]
    fn from_pool_default_soft_timeout() {
        let config = NntpClientConfig::single(
            ServerConfig {
                host: "news.example.com".into(),
                ..Default::default()
            },
            5,
        );
        let pool = Arc::new(NntpPool::new(PoolConfig {
            servers: config.servers,
            max_idle_age: config.max_idle_age,
            ..PoolConfig::default()
        }));
        let client = NntpClient::from_pool(pool);
        assert_eq!(client.soft_timeout, Duration::from_secs(15));
    }

    /// Build a multi-server client for testing server ordering.
    fn multi_server_client(server_count: usize) -> NntpClient {
        let servers: Vec<ServerPoolConfig> = (0..server_count)
            .map(|i| ServerPoolConfig {
                server: ServerConfig {
                    host: format!("server{i}.example.com"),
                    ..Default::default()
                },
                max_connections: 10,
                group: if i < 2 { 0 } else { 1 }, // first 2 in group 0, rest in group 1
            })
            .collect();

        let pool = NntpPool::new(PoolConfig {
            servers,
            max_idle_age: Duration::from_secs(300),
            ..PoolConfig::default()
        });

        NntpClient {
            pool: Arc::new(pool),
            max_retries_per_server: 1,
            soft_timeout: Duration::from_secs(15),
        }
    }

    #[tokio::test]
    async fn build_server_order_respects_priority_groups() {
        let client = multi_server_client(4);

        let order = client.build_server_order(&[]).await;
        // All 4 servers should be present.
        assert_eq!(order.len(), 4);
        // Group 0 servers (0, 1) should come before group 1 servers (2, 3).
        let group0_positions: Vec<usize> = order.iter().position(|&x| x == 0).into_iter()
            .chain(order.iter().position(|&x| x == 1))
            .collect();
        let group1_positions: Vec<usize> = order.iter().position(|&x| x == 2).into_iter()
            .chain(order.iter().position(|&x| x == 3))
            .collect();

        let max_group0 = *group0_positions.iter().max().unwrap();
        let min_group1 = *group1_positions.iter().min().unwrap();
        assert!(
            max_group0 < min_group1,
            "group 0 servers should all come before group 1 servers"
        );
    }

    #[tokio::test]
    async fn build_server_order_excludes_servers() {
        let client = multi_server_client(4);

        let order = client.build_server_order(&[1, 3]).await;
        assert_eq!(order.len(), 2);
        assert!(!order.contains(&1));
        assert!(!order.contains(&3));
        assert!(order.contains(&0));
        assert!(order.contains(&2));
    }

    #[tokio::test]
    async fn weighted_selection_favours_faster_server() {
        let client = multi_server_client(2);

        // Seed server 0 with 50ms latency and server 1 with 500ms latency.
        {
            let mut health = client.pool.health().lock().await;
            health.record_latency(0, Duration::from_millis(50));
            health.record_latency(1, Duration::from_millis(500));
        }

        // Run 1000 iterations and count how often server 0 is picked first.
        let mut server0_first = 0u32;
        for _ in 0..1000 {
            let ranked = client.rank_servers_in_group(&[0, 1]).await;
            if ranked[0] == 0 {
                server0_first += 1;
            }
        }

        // Server 0 is 10x faster, so it should be picked first significantly
        // more often. With weight=1/score, server 0 weight ≈ 10x server 1 weight,
        // so server 0 should be picked ~90% of the time. Allow some variance.
        assert!(
            server0_first > 700,
            "faster server should be picked first most of the time, but was only first {server0_first}/1000 times"
        );
    }

    #[tokio::test]
    async fn rank_single_server_returns_it() {
        let client = multi_server_client(2);
        let ranked = client.rank_servers_in_group(&[1]).await;
        assert_eq!(ranked, vec![1]);
    }

    #[tokio::test]
    async fn rank_empty_returns_empty() {
        let client = multi_server_client(2);
        let ranked = client.rank_servers_in_group(&[]).await;
        assert!(ranked.is_empty());
    }
}
