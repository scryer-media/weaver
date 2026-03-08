use std::sync::Arc;
use std::time::Duration;

use bytes::Bytes;
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
}

impl NntpClientConfig {
    /// Create a client config with a single server.
    pub fn single(server: ServerConfig, max_connections: usize) -> Self {
        NntpClientConfig {
            servers: vec![ServerPoolConfig {
                server,
                max_connections,
            }],
            max_idle_age: Duration::from_secs(300),
            max_retries_per_server: 1,
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
}

impl NntpClient {
    /// Create a new NNTP client from the given configuration.
    pub fn new(config: NntpClientConfig) -> Self {
        let max_retries_per_server = config.max_retries_per_server;
        let pool = NntpPool::new(PoolConfig {
            servers: config.servers,
            max_idle_age: config.max_idle_age,
            ..PoolConfig::default()
        });

        NntpClient {
            pool: Arc::new(pool),
            max_retries_per_server,
        }
    }

    /// Create a client wrapping an existing pool.
    pub fn from_pool(pool: Arc<NntpPool>) -> Self {
        NntpClient {
            pool,
            max_retries_per_server: 1,
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

        let server_count = self.pool.server_count();
        let mut last_error: Option<NntpError> = None;

        for idx in 0..server_count {
            let server = ServerId(idx);

            match self
                .fetch_from_server_with_groups(server, message_id, groups)
                .await
            {
                Ok(data) => return Ok(data),
                Err(NntpError::ArticleNotFound)
                | Err(NntpError::NoSuchArticle { .. })
                | Err(NntpError::NoArticleWithNumber) => {
                    debug!(
                        server = idx,
                        message_id,
                        "article not found, trying next server"
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
                        message_id,
                        "authentication/access failure, recording on health tracker"
                    );
                    {
                        let mut health = self.pool.health().lock().await;
                        health.record_failure(idx, true);
                    }
                    return Err(NntpError::AuthenticationFailed);
                }
                Err(e) if is_transient(&e) => {
                    warn!(
                        server = idx,
                        error = %e,
                        message_id,
                        "transient error, trying next server"
                    );
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

    /// Internal: fetch with multi-server failover logic.
    async fn fetch_with_failover(
        &self,
        message_id: &str,
        kind: FetchKind,
    ) -> Result<Bytes> {
        let server_count = self.pool.server_count();
        let mut last_error: Option<NntpError> = None;

        for idx in 0..server_count {
            let server = ServerId(idx);

            match self.fetch_from_server(server, message_id, kind).await {
                Ok(data) => return Ok(data),
                Err(NntpError::ArticleNotFound)
                | Err(NntpError::NoSuchArticle { .. })
                | Err(NntpError::NoArticleWithNumber) => {
                    debug!(
                        server = idx,
                        message_id,
                        "article not found, trying next server"
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
                        message_id,
                        "authentication/access failure, recording on health tracker"
                    );
                    {
                        let mut health = self.pool.health().lock().await;
                        health.record_failure(idx, true);
                    }
                    return Err(NntpError::AuthenticationFailed);
                }
                Err(e) if is_transient(&e) => {
                    warn!(
                        server = idx,
                        error = %e,
                        message_id,
                        "transient error, trying next server"
                    );
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
    async fn try_select_group(
        conn: &mut PooledConnection,
        groups: &[String],
    ) -> Result<bool> {
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
    async fn fetch_from_server_with_groups(
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

            let result = conn.body_by_id(message_id).await;
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
                    }
                    return Err(e);
                }
            }
        }
    }

    /// Fetch from a specific server, retrying on transient errors.
    async fn fetch_from_server(
        &self,
        server: ServerId,
        message_id: &str,
        kind: FetchKind,
    ) -> Result<Bytes> {
        let mut attempts = 0u32;

        loop {
            let mut conn = self.pool.acquire(server).await?;

            let result = match kind {
                FetchKind::Body => conn.body_by_id(message_id).await,
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
}
