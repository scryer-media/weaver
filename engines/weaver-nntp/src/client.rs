use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use rand::distr::weighted::WeightedIndex;
use rand::prelude::*;
use tokio::time::Instant as TokioInstant;
use tracing::{debug, warn};
use weaver_yenc::{
    DecodeResult as YencDecodeResult, DecodedArticle, StreamingArticleDecoder, YencError,
};

use crate::connection::ServerConfig;
use crate::error::{NntpError, Result};
use crate::health::{CooldownReason, ServerState};
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

/// A BODY fetch that was streamed and decoded inline.
#[derive(Debug)]
pub struct DecodedBody {
    pub raw_size: u32,
    pub decoded: Vec<u8>,
    pub result: YencDecodeResult,
}

/// Errors from the streamed BODY decode path.
#[derive(Debug)]
pub enum DecodedBodyError {
    Nntp(NntpError),
    Decode { raw_size: u32, error: YencError },
}

/// Existence results used by the health probe pipeline.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProbeBatchResult {
    pub exists: Vec<bool>,
    pub inconclusive: bool,
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

    /// Check whether articles exist anywhere in the configured server set.
    ///
    /// Returns one boolean per message-id: true if any usable server reports
    /// the article exists, false if every usable server definitively reports it
    /// missing. If a transient server failure leaves the batch inconclusive,
    /// this returns an error so callers do not treat missing samples as
    /// authoritative.
    pub async fn stat_many(&self, message_ids: &[&str]) -> Result<Vec<bool>> {
        if message_ids.is_empty() {
            return Ok(Vec::new());
        }

        let order = self.build_server_order(&[]).await;
        if order.is_empty() {
            return Err(NntpError::ServiceUnavailable);
        }

        let mut found = vec![false; message_ids.len()];
        let mut remaining: Vec<usize> = (0..message_ids.len()).collect();
        let mut had_success = false;
        let mut last_error: Option<NntpError> = None;

        for idx in order {
            if remaining.is_empty() {
                break;
            }

            let batch: Vec<&str> = remaining.iter().map(|&i| message_ids[i]).collect();
            let start = Instant::now();

            match self.stat_many_from_server(ServerId(idx), &batch).await {
                Ok(results) => {
                    had_success = true;
                    self.record_server_success(idx, start.elapsed()).await;

                    let mut next_remaining = Vec::with_capacity(remaining.len());
                    for (original_idx, exists) in remaining.iter().copied().zip(results)
                    {
                        if exists {
                            found[original_idx] = true;
                        } else {
                            next_remaining.push(original_idx);
                        }
                    }
                    remaining = next_remaining;
                }
                Err(NntpError::AuthenticationFailed)
                | Err(NntpError::AuthenticationRejected)
                | Err(NntpError::AccessDenied) => {
                    self.record_server_failure(idx, true).await;
                    last_error = Some(NntpError::AuthenticationFailed);
                }
                Err(e) if is_retryable_stat_error(&e) => {
                    if let Some(reason) = stat_cooldown_reason(&e) {
                        self.record_server_cooldown(idx, reason).await;
                    }
                    last_error = Some(e);
                }
                Err(e) => return Err(e),
            }
        }

        if remaining.is_empty() || had_success {
            Ok(found)
        } else {
            Err(last_error.unwrap_or(NntpError::ServiceUnavailable))
        }
    }

    /// Confirm article existence for health probes.
    ///
    /// The fast path uses batched pipelined STAT checks. Any article that STAT
    /// reports missing is re-checked with a non-pipelined HEAD before the
    /// result is treated as authoritative. Transport or probe errors during
    /// confirmation mark the entire batch inconclusive so callers can unwind
    /// without applying projected health damage.
    pub async fn confirm_exists_for_probe(&self, message_ids: &[&str]) -> ProbeBatchResult {
        if message_ids.is_empty() {
            return ProbeBatchResult {
                exists: Vec::new(),
                inconclusive: false,
            };
        }

        let mut exists = match self.stat_many(message_ids).await {
            Ok(results) => results,
            Err(_) => {
                return ProbeBatchResult {
                    exists: vec![false; message_ids.len()],
                    inconclusive: true,
                };
            }
        };

        for (idx, message_id) in message_ids.iter().enumerate() {
            if exists[idx] {
                continue;
            }

            match self.fetch_head(message_id).await {
                Ok(_) => exists[idx] = true,
                Err(
                    NntpError::ArticleNotFound
                    | NntpError::NoSuchArticle { .. }
                    | NntpError::NoArticleWithNumber,
                ) => {}
                Err(_) => {
                    return ProbeBatchResult {
                        exists,
                        inconclusive: true,
                    };
                }
            }
        }

        ProbeBatchResult {
            exists,
            inconclusive: false,
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
                    self.record_server_success(idx, elapsed).await;
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
                Err(e @ NntpError::SoftTimeout(_)) => {
                    warn!(
                        server = idx,
                        error = %e,
                        message_id,
                        "soft timeout, trying next server"
                    );
                    if let Some(reason) = cooldown_reason(&e) {
                        self.record_server_cooldown(idx, reason).await;
                    }
                    last_error = Some(e);
                    continue;
                }
                Err(NntpError::AuthenticationFailed)
                | Err(NntpError::AuthenticationRejected)
                | Err(NntpError::AccessDenied) => {
                    warn!(
                        server = idx,
                        message_id, "authentication/access failure, trying next server"
                    );
                    self.record_server_failure(idx, true).await;
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
                    if let Some(reason) = cooldown_reason(&e) {
                        self.record_server_cooldown(idx, reason).await;
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
            return self
                .fetch_with_failover_excluding(message_id, FetchKind::Body, exclude)
                .await;
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
                    self.record_server_success(idx, elapsed).await;
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
                Err(e @ NntpError::SoftTimeout(_)) => {
                    if let Some(reason) = cooldown_reason(&e) {
                        self.record_server_cooldown(idx, reason).await;
                    }
                    last_error = Some(e);
                    continue;
                }
                Err(NntpError::AuthenticationFailed)
                | Err(NntpError::AuthenticationRejected)
                | Err(NntpError::AccessDenied) => {
                    self.record_server_failure(idx, true).await;
                    last_error = Some(NntpError::AuthenticationFailed);
                    continue;
                }
                Err(e) if is_transient(&e) => {
                    if let Some(reason) = cooldown_reason(&e) {
                        self.record_server_cooldown(idx, reason).await;
                    }
                    last_error = Some(e);
                    continue;
                }
                Err(e) => return Err(e),
            }
        }

        Err(last_error.unwrap_or(NntpError::PoolExhausted))
    }

    /// Fetch and decode a yEnc BODY by message-id using streamed NNTP chunks.
    pub async fn fetch_body_decoded_with_groups(
        &self,
        message_id: &str,
        groups: &[String],
    ) -> std::result::Result<DecodedBody, DecodedBodyError> {
        self.fetch_body_decoded_with_groups_excluding(message_id, groups, &[])
            .await
    }

    /// Like [`fetch_body_decoded_with_groups`](Self::fetch_body_decoded_with_groups)
    /// but skips the specified servers.
    pub async fn fetch_body_decoded_with_groups_excluding(
        &self,
        message_id: &str,
        groups: &[String],
        exclude: &[usize],
    ) -> std::result::Result<DecodedBody, DecodedBodyError> {
        let order = self.build_server_order(exclude).await;

        if order.is_empty() {
            return Err(DecodedBodyError::Nntp(NntpError::PoolExhausted));
        }

        let mut last_error: Option<DecodedBodyError> = None;

        for idx in order {
            let server = ServerId(idx);
            let start = Instant::now();

            match self
                .fetch_decoded_from_server_with_groups(server, message_id, groups)
                .await
            {
                Ok(decoded) => {
                    let elapsed = start.elapsed();
                    self.record_server_success(idx, elapsed).await;
                    return Ok(decoded);
                }
                Err(DecodedBodyError::Decode { raw_size, error }) => {
                    return Err(DecodedBodyError::Decode { raw_size, error });
                }
                Err(DecodedBodyError::Nntp(
                    NntpError::ArticleNotFound
                    | NntpError::NoSuchArticle { .. }
                    | NntpError::NoArticleWithNumber,
                )) => {
                    last_error = Some(DecodedBodyError::Nntp(NntpError::NoSuchArticle {
                        message_id: message_id.to_string(),
                    }));
                    continue;
                }
                Err(DecodedBodyError::Nntp(e @ NntpError::SoftTimeout(_))) => {
                    if let Some(reason) = cooldown_reason(&e) {
                        self.record_server_cooldown(idx, reason).await;
                    }
                    last_error = Some(DecodedBodyError::Nntp(e));
                    continue;
                }
                Err(DecodedBodyError::Nntp(
                    NntpError::AuthenticationFailed
                    | NntpError::AuthenticationRejected
                    | NntpError::AccessDenied,
                )) => {
                    self.record_server_failure(idx, true).await;
                    last_error = Some(DecodedBodyError::Nntp(NntpError::AuthenticationFailed));
                    continue;
                }
                Err(DecodedBodyError::Nntp(e)) if is_transient(&e) => {
                    if let Some(reason) = cooldown_reason(&e) {
                        self.record_server_cooldown(idx, reason).await;
                    }
                    last_error = Some(DecodedBodyError::Nntp(e));
                    continue;
                }
                Err(other) => return Err(other),
            }
        }

        Err(last_error.unwrap_or(DecodedBodyError::Nntp(NntpError::PoolExhausted)))
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

    async fn record_server_success(&self, server_idx: usize, elapsed: Duration) {
        let mut health = self.pool.health().lock().await;
        health.record_success(server_idx);
        health.record_latency(server_idx, elapsed);
    }

    async fn record_server_failure(&self, server_idx: usize, is_auth: bool) {
        let mut health = self.pool.health().lock().await;
        health.record_failure(server_idx, is_auth);
    }

    async fn record_server_cooldown(&self, server_idx: usize, reason: CooldownReason) {
        let mut health = self.pool.health().lock().await;
        health.record_cooldown(server_idx, reason);
    }

    async fn record_premature_death_if_needed(&self, server_idx: usize, age: Duration) {
        if age < crate::health::ServerHealth::MIN_CONNECTION_LIFETIME {
            let mut health = self.pool.health().lock().await;
            health.record_premature_death(server_idx);
        }
    }

    async fn discard_connection_error(&self, server_idx: usize, conn: PooledConnection) {
        let age = conn.created_at().elapsed();
        conn.discard();
        self.pool.drain_all_idle().await;
        self.record_premature_death_if_needed(server_idx, age).await;
    }

    async fn acquire_before_deadline(
        &self,
        server: ServerId,
        deadline: TokioInstant,
    ) -> Result<PooledConnection> {
        match tokio::time::timeout_at(deadline, self.pool.acquire(server)).await {
            Ok(result) => result,
            Err(_) => Err(self.soft_timeout_error()),
        }
    }

    async fn sleep_before_deadline(&self, delay: Duration, deadline: TokioInstant) -> Result<()> {
        match tokio::time::timeout_at(deadline, tokio::time::sleep(delay)).await {
            Ok(()) => Ok(()),
            Err(_) => Err(self.soft_timeout_error()),
        }
    }

    fn soft_timeout_error(&self) -> NntpError {
        NntpError::SoftTimeout(self.soft_timeout.as_secs())
    }

    /// Build the server try-order, exhausting lower-priority groups before
    /// considering higher-priority backfill groups.
    ///
    /// Servers in `exclude` are skipped entirely. Disabled servers and
    /// short-lived cooldown servers are excluded so we don't waste time on
    /// servers that are known to be failing right now. Within a priority
    /// group, immediately acquirable servers are preferred over fully
    /// saturated peers.
    #[allow(clippy::needless_range_loop)]
    async fn build_server_order(&self, exclude: &[usize]) -> Vec<usize> {
        let server_count = self.pool.server_count();
        let server_groups = self.pool.server_groups();

        // Check health to skip disabled servers.
        let mut health = self.pool.health().lock().await;
        health.check_reenable_all();

        #[derive(Default)]
        struct GroupCandidates {
            ready_healthy: Vec<usize>,
            ready_degraded: Vec<usize>,
            waiting_healthy: Vec<usize>,
            waiting_degraded: Vec<usize>,
        }

        let mut groups: std::collections::BTreeMap<u32, GroupCandidates> =
            std::collections::BTreeMap::new();
        for idx in 0..server_count {
            if !exclude.contains(&idx) && health.is_available(idx) {
                let entry = groups.entry(server_groups[idx]).or_default();
                let ready = self.pool.server_load(idx).0 > 0;
                match health.server(idx).state() {
                    ServerState::Healthy => {
                        if ready {
                            entry.ready_healthy.push(idx);
                        } else {
                            entry.waiting_healthy.push(idx);
                        }
                    }
                    ServerState::Degraded { .. } => {
                        if ready {
                            entry.ready_degraded.push(idx);
                        } else {
                            entry.waiting_degraded.push(idx);
                        }
                    }
                    ServerState::CoolingDown { .. } | ServerState::Disabled { .. } => {}
                }
            }
        }
        drop(health);

        let mut result = Vec::with_capacity(server_count);
        for (_priority, candidates) in groups {
            if !candidates.ready_healthy.is_empty() {
                result.extend(self.rank_servers_in_group(&candidates.ready_healthy).await);
            }
            if !candidates.ready_degraded.is_empty() {
                result.extend(self.rank_servers_in_group(&candidates.ready_degraded).await);
            }
            if !candidates.waiting_healthy.is_empty() {
                result.extend(
                    self.rank_servers_in_group(&candidates.waiting_healthy)
                        .await,
                );
            }
            if !candidates.waiting_degraded.is_empty() {
                result.extend(
                    self.rank_servers_in_group(&candidates.waiting_degraded)
                        .await,
                );
            }
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
        self.fetch_with_failover_excluding(message_id, kind, &[])
            .await
    }

    async fn fetch_with_failover_excluding(
        &self,
        message_id: &str,
        kind: FetchKind,
        exclude: &[usize],
    ) -> Result<Bytes> {
        let order = self.build_server_order(exclude).await;
        let mut last_error: Option<NntpError> = None;

        for idx in order {
            let server = ServerId(idx);
            let start = Instant::now();

            match self.fetch_from_server(server, message_id, kind).await {
                Ok(data) => {
                    let elapsed = start.elapsed();
                    self.record_server_success(idx, elapsed).await;
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
                Err(e @ NntpError::SoftTimeout(_)) => {
                    warn!(
                        server = idx,
                        error = %e,
                        message_id,
                        "soft timeout, trying next server"
                    );
                    if let Some(reason) = cooldown_reason(&e) {
                        self.record_server_cooldown(idx, reason).await;
                    }
                    last_error = Some(e);
                    continue;
                }
                Err(NntpError::AuthenticationFailed)
                | Err(NntpError::AuthenticationRejected)
                | Err(NntpError::AccessDenied) => {
                    warn!(
                        server = idx,
                        message_id, "authentication/access failure, trying next server"
                    );
                    self.record_server_failure(idx, true).await;
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
                    if let Some(reason) = cooldown_reason(&e) {
                        self.record_server_cooldown(idx, reason).await;
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
    /// current connection is discarded and the caller should fail over to the
    /// next server.
    async fn fetch_from_server_with_groups(
        &self,
        server: ServerId,
        message_id: &str,
        groups: &[String],
    ) -> Result<Bytes> {
        let deadline = TokioInstant::now() + self.soft_timeout;
        let mut attempts = 0u32;

        loop {
            let mut conn = self.acquire_before_deadline(server, deadline).await?;

            // Try to select a group — iterate through the list on failure.
            let group_result =
                match tokio::time::timeout_at(deadline, Self::try_select_group(&mut conn, groups))
                    .await
                {
                    Ok(result) => result,
                    Err(_) => {
                        self.discard_connection_error(server.0, conn).await;
                        return Err(self.soft_timeout_error());
                    }
                };

            match group_result {
                Err(e) if is_retryable_stat_error(&e) => {
                    if should_discard_stat_connection(&e) {
                        self.discard_connection_error(server.0, conn).await;
                    }
                    if attempts < self.max_retries_per_server {
                        attempts += 1;
                        // Delay before retry to avoid hammering during reconnect throttle.
                        self.sleep_before_deadline(
                            Duration::from_millis(200 * attempts as u64),
                            deadline,
                        )
                        .await?;
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

            let result =
                match tokio::time::timeout_at(deadline, conn.body_by_id_raw(message_id)).await {
                    Ok(result) => result,
                    Err(_) => {
                        self.discard_connection_error(server.0, conn).await;
                        return Err(self.soft_timeout_error());
                    }
                };
            match result {
                Ok(response) => return Ok(response.data),
                Err(NntpError::ArticleNotFound)
                | Err(NntpError::NoSuchArticle { .. })
                | Err(NntpError::NoArticleWithNumber) => {
                    return Err(result.unwrap_err());
                }
                Err(e) if is_transient(&e) => {
                    if is_connection_error(&e) {
                        self.discard_connection_error(server.0, conn).await;
                    }
                    if attempts < self.max_retries_per_server {
                        attempts += 1;
                        self.sleep_before_deadline(
                            Duration::from_millis(200 * attempts as u64),
                            deadline,
                        )
                        .await?;
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
                        self.discard_connection_error(server.0, conn).await;
                    }
                    return Err(e);
                }
            }
        }
    }

    async fn fetch_decoded_from_server_with_groups(
        &self,
        server: ServerId,
        message_id: &str,
        groups: &[String],
    ) -> std::result::Result<DecodedBody, DecodedBodyError> {
        let deadline = TokioInstant::now() + self.soft_timeout;
        let mut attempts = 0u32;

        loop {
            let mut conn = self
                .acquire_before_deadline(server, deadline)
                .await
                .map_err(DecodedBodyError::Nntp)?;

            let group_result =
                match tokio::time::timeout_at(deadline, Self::try_select_group(&mut conn, groups))
                    .await
                {
                    Ok(result) => result,
                    Err(_) => {
                        self.discard_connection_error(server.0, conn).await;
                        return Err(DecodedBodyError::Nntp(self.soft_timeout_error()));
                    }
                };

            match group_result {
                Err(e) if is_transient(&e) => {
                    if is_connection_error(&e) {
                        self.discard_connection_error(server.0, conn).await;
                    }
                    if attempts < self.max_retries_per_server {
                        attempts += 1;
                        self.sleep_before_deadline(
                            Duration::from_millis(200 * attempts as u64),
                            deadline,
                        )
                        .await
                        .map_err(DecodedBodyError::Nntp)?;
                        continue;
                    }
                    return Err(DecodedBodyError::Nntp(e));
                }
                Err(e) => return Err(DecodedBodyError::Nntp(e)),
                Ok(false) => {}
                Ok(true) => {}
            }

            let mut decoder = StreamingArticleDecoder::new();
            let mut output = Vec::new();
            let mut raw_size = 0u32;
            let mut decode_error: Option<YencError> = None;

            let stream_result = match tokio::time::timeout_at(deadline, async {
                conn.stream_body_chunked_raw(message_id, |chunk| {
                    raw_size = raw_size.saturating_add(chunk.len() as u32);
                    if let Err(err) = decoder.feed_chunk(chunk, &mut output) {
                        decode_error = Some(err);
                        return Err(NntpError::MalformedResponse(
                            "streamed yEnc decode failed".into(),
                        ));
                    }
                    Ok(())
                })
                .await
            })
            .await
            {
                Ok(result) => result,
                Err(_) => {
                    self.discard_connection_error(server.0, conn).await;
                    return Err(DecodedBodyError::Nntp(self.soft_timeout_error()));
                }
            };

            match stream_result {
                Ok(_) => {
                    if let Some(err) = decode_error.take() {
                        return Err(DecodedBodyError::Decode {
                            raw_size,
                            error: err,
                        });
                    }

                    match decoder.finish(output) {
                        Ok(DecodedArticle { data, result }) => {
                            return Ok(DecodedBody {
                                raw_size,
                                decoded: data,
                                result,
                            });
                        }
                        Err(err) => {
                            return Err(DecodedBodyError::Decode {
                                raw_size,
                                error: err,
                            });
                        }
                    }
                }
                Err(_e) if decode_error.is_some() => {
                    return Err(DecodedBodyError::Decode {
                        raw_size,
                        error: decode_error.take().expect("decode error present"),
                    });
                }
                Err(e @ NntpError::ArticleNotFound)
                | Err(e @ NntpError::NoSuchArticle { .. })
                | Err(e @ NntpError::NoArticleWithNumber) => {
                    return Err(DecodedBodyError::Nntp(e));
                }
                Err(e) if is_transient(&e) => {
                    if is_connection_error(&e) {
                        self.discard_connection_error(server.0, conn).await;
                    }
                    if attempts < self.max_retries_per_server {
                        attempts += 1;
                        self.sleep_before_deadline(
                            Duration::from_millis(200 * attempts as u64),
                            deadline,
                        )
                        .await
                        .map_err(DecodedBodyError::Nntp)?;
                        continue;
                    }
                    return Err(DecodedBodyError::Nntp(e));
                }
                Err(e) => {
                    if is_connection_error(&e) {
                        self.discard_connection_error(server.0, conn).await;
                    }
                    return Err(DecodedBodyError::Nntp(e));
                }
            }
        }
    }

    /// Check a batch of articles on a specific server, retrying on transient
    /// errors and using pipelining when the server supports it.
    async fn stat_many_from_server(
        &self,
        server: ServerId,
        message_ids: &[&str],
    ) -> Result<Vec<bool>> {
        if message_ids.is_empty() {
            return Ok(Vec::new());
        }

        let deadline = TokioInstant::now() + self.soft_timeout;
        let mut attempts = 0u32;

        loop {
            let mut conn = self.acquire_before_deadline(server, deadline).await?;

            let result = match tokio::time::timeout_at(deadline, async {
                if conn.capabilities().supports_pipelining() {
                    conn.stat_pipeline(message_ids).await
                } else {
                    let mut results = Vec::with_capacity(message_ids.len());
                    for message_id in message_ids {
                        results.push(conn.stat_by_id(message_id).await?);
                    }
                    Ok(results)
                }
            })
            .await
            {
                Ok(result) => result,
                Err(_) => {
                    self.discard_connection_error(server.0, conn).await;
                    return Err(self.soft_timeout_error());
                }
            };

            match result {
                Ok(results) => return Ok(results),
                Err(e) if is_retryable_stat_error(&e) => {
                    if should_discard_stat_connection(&e) {
                        self.discard_connection_error(server.0, conn).await;
                    }
                    if attempts < self.max_retries_per_server {
                        attempts += 1;
                        self.sleep_before_deadline(
                            Duration::from_millis(200 * attempts as u64),
                            deadline,
                        )
                        .await?;
                        debug!(
                            server = server.0,
                            attempt = attempts,
                            error = %e,
                            batch_size = message_ids.len(),
                            "retryable error during STAT batch, retrying on same server"
                        );
                        continue;
                    }
                    return Err(e);
                }
                Err(e) => {
                    if should_discard_stat_connection(&e) {
                        self.discard_connection_error(server.0, conn).await;
                    }
                    return Err(e);
                }
            }
        }
    }

    /// Fetch from a specific server, retrying on transient errors.
    ///
    /// Bounded by the soft timeout — if exceeded, the current connection is
    /// discarded and `SoftTimeout` is returned to trigger failover at the caller.
    async fn fetch_from_server(
        &self,
        server: ServerId,
        message_id: &str,
        kind: FetchKind,
    ) -> Result<Bytes> {
        let deadline = TokioInstant::now() + self.soft_timeout;
        let mut attempts = 0u32;

        loop {
            let mut conn = self.acquire_before_deadline(server, deadline).await?;

            let result = match tokio::time::timeout_at(deadline, async {
                match kind {
                    FetchKind::Body => conn.body_by_id_raw(message_id).await,
                    FetchKind::Head => conn.head_by_id(message_id).await,
                    FetchKind::Article => conn.article_by_id(message_id).await,
                }
            })
            .await
            {
                Ok(result) => result,
                Err(_) => {
                    self.discard_connection_error(server.0, conn).await;
                    return Err(self.soft_timeout_error());
                }
            };

            match result {
                Ok(response) => return Ok(response.data),
                Err(NntpError::ArticleNotFound) | Err(NntpError::NoSuchArticle { .. }) => {
                    // Article not found — do not retry, return immediately.
                    return Err(result.unwrap_err());
                }
                Err(e) if is_transient(&e) => {
                    if is_connection_error(&e) {
                        self.discard_connection_error(server.0, conn).await;
                    }
                    if attempts < self.max_retries_per_server {
                        attempts += 1;
                        self.sleep_before_deadline(
                            Duration::from_millis(200 * attempts as u64),
                            deadline,
                        )
                        .await?;
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
                        self.discard_connection_error(server.0, conn).await;
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
            | NntpError::TruncatedMultilineBody
            | NntpError::ServerDisconnectedMidBody
            | NntpError::MalformedMultilineTerminator
            | NntpError::ServiceUnavailable
            | NntpError::TooManyConnections
            | NntpError::PoolExhausted
            | NntpError::SoftTimeout(_)
    )
}

fn is_retryable_stat_error(err: &NntpError) -> bool {
    is_transient(err) || matches!(err, NntpError::MalformedResponse(_))
}

fn cooldown_reason(err: &NntpError) -> Option<CooldownReason> {
    match err {
        NntpError::Io(_)
        | NntpError::Timeout
        | NntpError::ConnectionClosed
        | NntpError::TruncatedMultilineBody
        | NntpError::ServerDisconnectedMidBody
        | NntpError::MalformedMultilineTerminator
        | NntpError::ServiceUnavailable
        | NntpError::SoftTimeout(_) => Some(CooldownReason::Transport),
        NntpError::TooManyConnections => Some(CooldownReason::Capacity),
        NntpError::PoolExhausted => None,
        _ => None,
    }
}

fn stat_cooldown_reason(err: &NntpError) -> Option<CooldownReason> {
    cooldown_reason(err).or_else(|| {
        matches!(err, NntpError::MalformedResponse(_)).then_some(CooldownReason::Transport)
    })
}

/// Returns true if the error indicates the connection itself is bad
/// and should be discarded rather than returned to the pool.
fn is_connection_error(err: &NntpError) -> bool {
    matches!(
        err,
        NntpError::Io(_)
            | NntpError::Timeout
            | NntpError::ConnectionClosed
            | NntpError::TruncatedMultilineBody
            | NntpError::ServerDisconnectedMidBody
            | NntpError::MalformedMultilineTerminator
            | NntpError::TooManyConnections
            | NntpError::AccessDenied
            | NntpError::SoftTimeout(_)
    )
}

fn should_discard_stat_connection(err: &NntpError) -> bool {
    is_connection_error(err) || matches!(err, NntpError::MalformedResponse(_))
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::{TcpListener, TcpStream};

    struct ScriptStep {
        expect_prefix: Option<&'static str>,
        response: &'static [u8],
    }

    async fn read_command_line(socket: &mut TcpStream) -> String {
        let mut buf = Vec::new();
        loop {
            let mut byte = [0u8; 1];
            let n = socket.read(&mut byte).await.unwrap();
            assert!(n > 0, "client closed connection before command completed");
            buf.push(byte[0]);
            if byte[0] == b'\n' {
                break;
            }
        }
        String::from_utf8(buf).unwrap()
    }

    async fn spawn_scripted_server(steps: Vec<ScriptStep>) -> u16 {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();

        tokio::spawn(async move {
            let (mut socket, _) = listener.accept().await.unwrap();
            for step in steps {
                if let Some(prefix) = step.expect_prefix {
                    let line = read_command_line(&mut socket).await;
                    assert!(
                        line.starts_with(prefix),
                        "expected command starting with {prefix:?}, got {line:?}"
                    );
                }
                if !step.response.is_empty() {
                    socket.write_all(step.response).await.unwrap();
                    socket.flush().await.unwrap();
                }
            }
        });

        port
    }

    async fn try_read_command_line(socket: &mut TcpStream) -> Option<String> {
        let mut buf = Vec::new();
        loop {
            let mut byte = [0u8; 1];
            let n = socket.read(&mut byte).await.unwrap();
            if n == 0 {
                if buf.is_empty() {
                    return None;
                }
                panic!("client closed connection before command completed");
            }
            buf.push(byte[0]);
            if byte[0] == b'\n' {
                return Some(String::from_utf8(buf).unwrap());
            }
        }
    }

    async fn spawn_probe_confirmation_server(head_response: &'static [u8]) -> u16 {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();

        tokio::spawn(async move {
            loop {
                let accept = tokio::time::timeout(Duration::from_secs(1), listener.accept()).await;
                let Ok(Ok((mut socket, _))) = accept else {
                    return;
                };

                tokio::spawn(async move {
                    socket.write_all(b"200 ready\r\n").await.unwrap();
                    socket.flush().await.unwrap();

                    while let Some(line) = try_read_command_line(&mut socket).await {
                        if line.starts_with("CAPABILITIES") {
                            socket
                                .write_all(
                                    b"101 Capability list:\r\nVERSION 2\r\nREADER\r\nPIPELINING\r\n.\r\n",
                                )
                                .await
                                .unwrap();
                            socket.flush().await.unwrap();
                            continue;
                        }

                        if line.starts_with("STAT ") {
                            socket.write_all(b"430 No such article\r\n").await.unwrap();
                            socket.flush().await.unwrap();
                            continue;
                        }

                        if line.starts_with("HEAD ") {
                            if !head_response.is_empty() {
                                socket.write_all(head_response).await.unwrap();
                                socket.flush().await.unwrap();
                            }
                            break;
                        }

                        panic!("unexpected command line: {line:?}");
                    }
                });
            }
        });

        port
    }

    fn scripted_server(port: u16, group: usize) -> ServerPoolConfig {
        ServerPoolConfig {
            server: ServerConfig {
                host: "127.0.0.1".into(),
                port,
                tls: false,
                connect_timeout: Duration::from_secs(1),
                command_timeout: Duration::from_secs(1),
                ..Default::default()
            },
            max_connections: 2,
            group: group as u32,
        }
    }

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
        assert!(is_transient(&NntpError::TruncatedMultilineBody));
        assert!(is_transient(&NntpError::ServerDisconnectedMidBody));
        assert!(is_transient(&NntpError::MalformedMultilineTerminator));
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
        assert!(!is_transient(&NntpError::MalformedResponse("bad".into())));
    }

    #[test]
    fn connection_errors() {
        assert!(is_connection_error(&NntpError::Timeout));
        assert!(is_connection_error(&NntpError::ConnectionClosed));
        assert!(is_connection_error(&NntpError::TruncatedMultilineBody));
        assert!(is_connection_error(&NntpError::ServerDisconnectedMidBody));
        assert!(is_connection_error(
            &NntpError::MalformedMultilineTerminator
        ));
        assert!(is_connection_error(&NntpError::TooManyConnections));
        assert!(is_connection_error(&NntpError::AccessDenied));
        assert!(!is_connection_error(&NntpError::ArticleNotFound));
        assert!(!is_connection_error(&NntpError::AuthenticationFailed));
    }

    #[test]
    fn malformed_stat_response_is_retryable_transport_fault() {
        let err = NntpError::MalformedResponse("2\u{fffd}3".into());
        assert!(is_retryable_stat_error(&err));
        assert!(should_discard_stat_connection(&err));
        assert_eq!(stat_cooldown_reason(&err), Some(CooldownReason::Transport));
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
    fn transient_failure_recording_counts_soft_timeouts() {
        assert_eq!(
            cooldown_reason(&NntpError::SoftTimeout(15)),
            Some(CooldownReason::Transport)
        );
        assert_eq!(
            cooldown_reason(&NntpError::TooManyConnections),
            Some(CooldownReason::Capacity)
        );
        assert_eq!(cooldown_reason(&NntpError::PoolExhausted), None);
        assert_eq!(
            cooldown_reason(&NntpError::Timeout),
            Some(CooldownReason::Transport)
        );
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
        let group0_positions: Vec<usize> = order
            .iter()
            .position(|&x| x == 0)
            .into_iter()
            .chain(order.iter().position(|&x| x == 1))
            .collect();
        let group1_positions: Vec<usize> = order
            .iter()
            .position(|&x| x == 2)
            .into_iter()
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

    #[tokio::test]
    async fn build_server_order_prefers_healthy_over_degraded() {
        let client = multi_server_client(2);

        {
            let mut health = client.pool.health().lock().await;
            for _ in 0..5 {
                health.record_failure(1, false);
            }
        }

        let order = client.build_server_order(&[]).await;
        assert_eq!(order, vec![0, 1]);
    }

    #[tokio::test]
    async fn build_server_order_keeps_backfill_after_primary_group() {
        let pool = NntpPool::new(PoolConfig {
            servers: vec![
                ServerPoolConfig {
                    server: ServerConfig {
                        host: "saturated-primary.example.com".into(),
                        ..Default::default()
                    },
                    max_connections: 0,
                    group: 0,
                },
                ServerPoolConfig {
                    server: ServerConfig {
                        host: "available-backfill.example.com".into(),
                        ..Default::default()
                    },
                    max_connections: 1,
                    group: 1,
                },
            ],
            max_idle_age: Duration::from_secs(300),
            ..PoolConfig::default()
        });

        let client = NntpClient {
            pool: Arc::new(pool),
            max_retries_per_server: 1,
            soft_timeout: Duration::from_secs(15),
        };

        let order = client.build_server_order(&[]).await;
        assert_eq!(order, vec![0, 1]);
    }

    #[tokio::test]
    async fn build_server_order_prefers_ready_servers_within_same_group() {
        let pool = NntpPool::new(PoolConfig {
            servers: vec![
                ServerPoolConfig {
                    server: ServerConfig {
                        host: "waiting-primary.example.com".into(),
                        ..Default::default()
                    },
                    max_connections: 0,
                    group: 0,
                },
                ServerPoolConfig {
                    server: ServerConfig {
                        host: "ready-primary.example.com".into(),
                        ..Default::default()
                    },
                    max_connections: 1,
                    group: 0,
                },
            ],
            max_idle_age: Duration::from_secs(300),
            ..PoolConfig::default()
        });

        let client = NntpClient {
            pool: Arc::new(pool),
            max_retries_per_server: 1,
            soft_timeout: Duration::from_secs(15),
        };

        let order = client.build_server_order(&[]).await;
        assert_eq!(order, vec![1, 0]);
    }

    #[tokio::test]
    async fn stat_many_fails_over_after_malformed_pipelined_response() {
        let primary_port = spawn_scripted_server(vec![
            ScriptStep {
                expect_prefix: None,
                response: b"200 ready\r\n",
            },
            ScriptStep {
                expect_prefix: Some("CAPABILITIES"),
                response: b"101 Capability list:\r\nVERSION 2\r\nREADER\r\nPIPELINING\r\n.\r\n",
            },
            ScriptStep {
                expect_prefix: Some("STAT "),
                response: b"2\xff3 malformed\r\n",
            },
            ScriptStep {
                expect_prefix: Some("STAT "),
                response: b"430 No such article\r\n",
            },
        ])
        .await;

        let backup_port = spawn_scripted_server(vec![
            ScriptStep {
                expect_prefix: None,
                response: b"200 ready\r\n",
            },
            ScriptStep {
                expect_prefix: Some("CAPABILITIES"),
                response: b"101 Capability list:\r\nVERSION 2\r\nREADER\r\nPIPELINING\r\n.\r\n",
            },
            ScriptStep {
                expect_prefix: Some("STAT "),
                response: b"223 0 <exists@example.com>\r\n",
            },
            ScriptStep {
                expect_prefix: Some("STAT "),
                response: b"430 No such article\r\n",
            },
        ])
        .await;

        let client = NntpClient::new(NntpClientConfig {
            servers: vec![
                scripted_server(primary_port, 0),
                scripted_server(backup_port, 1),
            ],
            max_idle_age: Duration::from_secs(300),
            max_retries_per_server: 0,
            soft_timeout: Duration::from_secs(5),
        });

        let results = client
            .stat_many(&["<exists@example.com>", "<missing@example.com>"])
            .await
            .expect("stat_many should fail over");
        assert_eq!(results, vec![true, false]);
    }

    #[tokio::test]
    async fn confirm_exists_for_probe_recovers_false_stat_with_head_success() {
        let port = spawn_probe_confirmation_server(
            b"221 0 <exists@example.com> Headers follow\r\nSubject: still here\r\n.\r\n",
        )
        .await;

        let client = NntpClient::new(NntpClientConfig {
            servers: vec![scripted_server(port, 0)],
            max_idle_age: Duration::from_secs(300),
            max_retries_per_server: 0,
            soft_timeout: Duration::from_secs(5),
        });

        let result = client
            .confirm_exists_for_probe(&["<exists@example.com>"])
            .await;
        assert_eq!(
            result,
            ProbeBatchResult {
                exists: vec![true],
                inconclusive: false,
            }
        );
    }

    #[tokio::test]
    async fn confirm_exists_for_probe_marks_head_transport_failure_inconclusive() {
        let port = spawn_probe_confirmation_server(b"").await;

        let client = NntpClient::new(NntpClientConfig {
            servers: vec![scripted_server(port, 0)],
            max_idle_age: Duration::from_secs(300),
            max_retries_per_server: 0,
            soft_timeout: Duration::from_secs(5),
        });

        let result = client
            .confirm_exists_for_probe(&["<exists@example.com>"])
            .await;
        assert_eq!(
            result,
            ProbeBatchResult {
                exists: vec![false],
                inconclusive: true,
            }
        );
    }
}
