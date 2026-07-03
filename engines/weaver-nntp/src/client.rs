use std::collections::VecDeque;
use std::future::Future;
use std::net::IpAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use rand::distr::weighted::WeightedIndex;
use rand::prelude::*;
use tokio::time::Instant as TokioInstant;
use tracing::{debug, warn};
use weaver_yenc::{DecodeResult as YencDecodeResult, YencError};

use crate::connection::ServerConfig;
use crate::error::{NntpError, Result};
use crate::fused_yenc::{FusedYencArticleStats, FusedYencError};
use crate::health::{CooldownReason, ServerState};
use crate::pool::{NntpPool, PoolConfig, PooledConnection, ServerId, ServerPoolConfig};
use crate::tls::TransportReadStats;

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
#[derive(Clone)]
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
    pub decoded: Vec<Box<[u8]>>,
    pub result: YencDecodeResult,
    pub cpu: DecodedBodyCpu,
    pub io: DecodedBodyIo,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct DecodedBodyCpu {
    pub raw_decode: Duration,
    pub read_poll: Duration,
    pub response_line: Duration,
    pub yenc_header: Duration,
    pub body_decode: Duration,
    pub yend_line: Duration,
    pub nntp_terminator: Duration,
    pub feed: Duration,
    pub finish: Duration,
}

#[derive(Debug, Clone, Copy, Default)]
pub struct DecodedBodyIo {
    pub read_calls: u64,
    pub read_bytes: u64,
    pub input_chunks: u64,
    pub decode_calls: u64,
    pub crc_update_calls: u64,
    pub output_batches: u64,
    pub leftover_bytes_after_terminator: u64,
    pub buffer_compactions: u64,
    pub encoded_bytes_consumed: u64,
    pub decoded_bytes_written: u64,
    pub transport_read: TransportReadStats,
}

/// Errors from the streamed BODY decode path.
#[derive(Debug)]
pub enum DecodedBodyError {
    Nntp(NntpError),
    Decode { raw_size: u32, error: YencError },
}

fn saturating_u32(value: u64) -> u32 {
    value.min(u32::MAX as u64) as u32
}

fn decoded_cpu_from_fused_stats(stats: &FusedYencArticleStats) -> DecodedBodyCpu {
    DecodedBodyCpu {
        raw_decode: stats.fused_decode_cpu,
        read_poll: stats.read_poll_cpu,
        response_line: stats.response_line_cpu,
        yenc_header: stats.yenc_header_cpu,
        body_decode: stats.body_decode_cpu,
        yend_line: stats.yend_line_cpu,
        nntp_terminator: stats.nntp_terminator_cpu,
        feed: stats.output_callback_cpu,
        finish: stats.article_finish_cpu,
    }
}

fn decoded_io_from_fused_stats(stats: &FusedYencArticleStats) -> DecodedBodyIo {
    DecodedBodyIo {
        read_calls: stats.read_calls,
        read_bytes: stats.read_bytes,
        input_chunks: stats.input_chunks,
        decode_calls: stats.decode_calls,
        crc_update_calls: stats.crc_update_calls,
        output_batches: stats.output_batches,
        leftover_bytes_after_terminator: stats.leftover_bytes_after_terminator,
        buffer_compactions: stats.buffer_compactions,
        encoded_bytes_consumed: stats.encoded_bytes_consumed,
        decoded_bytes_written: stats.decoded_bytes_written,
        transport_read: stats.transport_read,
    }
}

fn decoded_raw_size_from_fused_stats(stats: &FusedYencArticleStats) -> u32 {
    saturating_u32(
        stats
            .encoded_bytes_consumed
            .saturating_sub(stats.nntp_terminator_bytes),
    )
}

/// Existence results used by the health probe pipeline.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ProbeBatchResult {
    pub exists: Vec<bool>,
    pub inconclusive: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FetchAttemptOutcome {
    Success,
    NotFound,
    AuthenticationFailure,
    TransientFailure,
    PermanentFailure,
}

#[derive(Debug, Clone)]
pub struct FetchAttemptTrace {
    pub server_idx: usize,
    pub remote_ip: Option<IpAddr>,
    pub elapsed: Duration,
    pub outcome: FetchAttemptOutcome,
    pub error: Option<String>,
}

#[derive(Debug)]
pub struct FetchBodyTrace {
    pub attempts: Vec<FetchAttemptTrace>,
    pub result: Result<Bytes>,
}

#[derive(Debug)]
pub struct DecodedBodyTrace {
    pub attempts: Vec<FetchAttemptTrace>,
    pub result: std::result::Result<DecodedBody, DecodedBodyError>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BodyLaneMode {
    Sequential,
    PipelineDepth2,
    PipelineDepth4,
}

#[derive(Debug, Default, Clone)]
pub struct BodyLaneBatchStats {
    pub requested: usize,
    pub completed: usize,
    pub unresolved: usize,
    pub connection_discarded: bool,
    pub response_order_mismatch: bool,
    pub elapsed: Duration,
}

#[derive(Debug, Clone, Copy)]
pub struct BodyLaneTraceMeta {
    pub batch_complete: bool,
    pub batch_clean: bool,
    pub batch_response_count: u64,
    pub unresolved_count: u64,
    pub connection_discarded: bool,
}

pub struct BodyLaneLease {
    client: NntpClient,
    server_id: ServerId,
    conn: Option<PooledConnection>,
    remote_ip: IpAddr,
    groups: Vec<String>,
    mode: BodyLaneMode,
    rtt_ewma: Option<Duration>,
    rtt_samples: VecDeque<Duration>,
}

struct DecodedBatchItem {
    elapsed: Duration,
    result: std::result::Result<DecodedBody, DecodedBodyError>,
}

enum DecodedBatchDisposition {
    Terminal(std::result::Result<DecodedBody, DecodedBodyError>),
    Retry,
}

impl BodyLaneLease {
    pub fn server_id(&self) -> ServerId {
        self.server_id
    }

    pub fn remote_ip(&self) -> IpAddr {
        self.remote_ip
    }

    pub fn mode(&self) -> BodyLaneMode {
        self.mode
    }

    pub fn groups(&self) -> &[String] {
        &self.groups
    }

    pub fn rtt_ewma(&self) -> Option<Duration> {
        self.rtt_ewma
    }

    pub fn supports_pipelining(&self) -> bool {
        self.conn
            .as_ref()
            .is_some_and(|conn| conn.capabilities().supports_pipelining())
    }

    pub fn park(self) {}

    pub async fn discard(mut self) {
        self.discard_current().await;
    }

    pub async fn fetch_decoded_sequential(&mut self, message_id: &str) -> DecodedBodyTrace {
        self.mode = BodyLaneMode::Sequential;
        let started = Instant::now();
        let result = self.read_decoded_body(message_id).await;
        let elapsed = started.elapsed();
        self.observe_rtt(elapsed);

        if result.as_ref().is_err_and(
            |error| matches!(error, DecodedBodyError::Nntp(e) if is_connection_error(e)),
        ) || self.conn.as_ref().is_some_and(|conn| conn.is_poisoned())
        {
            self.discard_current().await;
        }

        self.trace_item(message_id, DecodedBatchItem { elapsed, result })
            .await
    }

    pub async fn fetch_decoded_pipeline_depth2<F, Fut>(
        &mut self,
        message_ids: &[String],
        on_trace: F,
    ) -> BodyLaneBatchStats
    where
        F: FnMut(usize, DecodedBodyTrace, BodyLaneTraceMeta) -> Fut,
        Fut: Future<Output = ()>,
    {
        self.mode = BodyLaneMode::PipelineDepth2;
        self.fetch_decoded_pipeline(message_ids, 2, on_trace).await
    }

    pub async fn fetch_decoded_pipeline_depth4<F, Fut>(
        &mut self,
        message_ids: &[String],
        on_trace: F,
    ) -> BodyLaneBatchStats
    where
        F: FnMut(usize, DecodedBodyTrace, BodyLaneTraceMeta) -> Fut,
        Fut: Future<Output = ()>,
    {
        self.mode = BodyLaneMode::PipelineDepth4;
        self.fetch_decoded_pipeline(message_ids, 4, on_trace).await
    }

    async fn fetch_decoded_pipeline<F, Fut>(
        &mut self,
        message_ids: &[String],
        max_depth: usize,
        mut on_trace: F,
    ) -> BodyLaneBatchStats
    where
        F: FnMut(usize, DecodedBodyTrace, BodyLaneTraceMeta) -> Fut,
        Fut: Future<Output = ()>,
    {
        let requested = message_ids.len().min(max_depth);
        let batch_started = Instant::now();
        let mut stats = BodyLaneBatchStats {
            requested,
            ..BodyLaneBatchStats::default()
        };
        if requested == 0 {
            return stats;
        }

        let request_write_error = if let Some(conn) = self.conn.as_mut() {
            let mut error = None;
            for message_id in &message_ids[..requested] {
                if let Err(write_error) = conn.write_body_request(message_id).await {
                    error = Some(write_error);
                    break;
                }
            }
            if error.is_none()
                && let Err(flush_error) = conn.flush_commands().await
            {
                error = Some(flush_error);
            }
            error
        } else {
            Some(NntpError::ConnectionClosed)
        };

        if let Some(error) = request_write_error {
            let elapsed = batch_started.elapsed();
            if is_connection_error(&error) {
                stats.connection_discarded = true;
                self.discard_current().await;
            }
            let make_error = NntpClient::batch_setup_error_factory(&error);
            for (idx, message_id) in message_ids.iter().take(requested).enumerate() {
                let is_last = idx + 1 == requested;
                let trace = self
                    .trace_item(
                        message_id,
                        DecodedBatchItem {
                            elapsed,
                            result: Err(DecodedBodyError::Nntp(make_error())),
                        },
                    )
                    .await;
                stats.completed += 1;
                on_trace(
                    idx,
                    trace,
                    BodyLaneTraceMeta {
                        batch_complete: is_last,
                        batch_clean: false,
                        batch_response_count: if is_last { stats.completed as u64 } else { 0 },
                        unresolved_count: if is_last { 0 } else { 0 },
                        connection_discarded: stats.connection_discarded,
                    },
                )
                .await;
            }
            stats.elapsed = batch_started.elapsed();
            return stats;
        }

        let mut closed_early = false;
        let mut batch_clean_so_far = true;
        for (response_idx, message_id) in message_ids.iter().take(requested).enumerate() {
            let item_started = Instant::now();
            let item_deadline = TokioInstant::now() + self.client.soft_timeout;
            let result = match tokio::time::timeout_at(
                item_deadline,
                NntpClient::read_decoded_pipelined_body(
                    self.conn
                        .as_mut()
                        .expect("connection is available while reading pipeline body"),
                ),
            )
            .await
            {
                Ok(result) => result,
                Err(_) => {
                    stats.connection_discarded = true;
                    self.discard_current().await;
                    closed_early = true;
                    Err(DecodedBodyError::Nntp(self.client.soft_timeout_error()))
                }
            };
            let elapsed = item_started.elapsed();
            self.observe_rtt(elapsed);
            let poisoned = self.conn.as_ref().is_some_and(|conn| conn.is_poisoned());

            let trace = self
                .trace_item(message_id, DecodedBatchItem { elapsed, result })
                .await;
            stats.completed += 1;
            batch_clean_so_far &= trace.result.is_ok();

            if poisoned {
                stats.connection_discarded = true;
                self.discard_current().await;
                closed_early = true;
            }

            let terminal_unresolved = if closed_early {
                requested.saturating_sub(response_idx + 1)
            } else {
                0
            };
            let is_batch_complete = closed_early || response_idx + 1 == requested;
            let meta = BodyLaneTraceMeta {
                batch_complete: is_batch_complete,
                batch_clean: batch_clean_so_far && !closed_early,
                batch_response_count: if is_batch_complete {
                    stats.completed as u64
                } else {
                    0
                },
                unresolved_count: if is_batch_complete {
                    terminal_unresolved as u64
                } else {
                    0
                },
                connection_discarded: stats.connection_discarded,
            };
            on_trace(response_idx, trace, meta).await;

            if closed_early {
                for (unread_idx, unread_message_id) in message_ids
                    .iter()
                    .take(requested)
                    .enumerate()
                    .skip(response_idx + 1)
                {
                    let trace = self
                        .trace_item(
                            unread_message_id,
                            DecodedBatchItem {
                                elapsed,
                                result: Err(DecodedBodyError::Nntp(NntpError::ConnectionClosed)),
                            },
                        )
                        .await;
                    stats.completed += 1;
                    stats.unresolved += 1;
                    on_trace(
                        unread_idx,
                        trace,
                        BodyLaneTraceMeta {
                            batch_complete: false,
                            batch_clean: false,
                            batch_response_count: 0,
                            unresolved_count: 0,
                            connection_discarded: true,
                        },
                    )
                    .await;
                }
                break;
            }
        }

        stats.elapsed = batch_started.elapsed();
        stats
    }

    async fn read_decoded_body(
        &mut self,
        message_id: &str,
    ) -> std::result::Result<DecodedBody, DecodedBodyError> {
        let deadline = TokioInstant::now() + self.client.soft_timeout;

        let Some(conn) = self.conn.as_mut() else {
            return Err(DecodedBodyError::Nntp(NntpError::ConnectionClosed));
        };

        let stream_result = match tokio::time::timeout_at(deadline, async {
            conn.stream_yenc_article(message_id, |_| Ok(())).await
        })
        .await
        {
            Ok(result) => result,
            Err(_) => {
                self.discard_current().await;
                return Err(DecodedBodyError::Nntp(self.client.soft_timeout_error()));
            }
        };

        match stream_result {
            Ok(article) => Ok(DecodedBody {
                raw_size: decoded_raw_size_from_fused_stats(&article.stats),
                cpu: decoded_cpu_from_fused_stats(&article.stats),
                io: decoded_io_from_fused_stats(&article.stats),
                decoded: article.chunks,
                result: article.result,
            }),
            Err(FusedYencError::Yenc(error)) => {
                Err(DecodedBodyError::Decode { raw_size: 0, error })
            }
            Err(FusedYencError::Nntp(error)) => Err(DecodedBodyError::Nntp(error)),
        }
    }

    async fn trace_item(&self, message_id: &str, item: DecodedBatchItem) -> DecodedBodyTrace {
        let mut attempts = Vec::new();
        let mut last_error = None;
        match self
            .client
            .classify_decoded_batch_item(
                self.server_id.0,
                Some(self.remote_ip),
                message_id,
                item,
                &mut attempts,
                &mut last_error,
            )
            .await
        {
            DecodedBatchDisposition::Terminal(result) => DecodedBodyTrace { attempts, result },
            DecodedBatchDisposition::Retry => DecodedBodyTrace {
                attempts,
                result: Err(
                    last_error.unwrap_or(DecodedBodyError::Nntp(NntpError::ConnectionClosed))
                ),
            },
        }
    }

    fn observe_rtt(&mut self, sample: Duration) {
        let ewma = if let Some(current) = self.rtt_ewma {
            current.mul_f64(0.75) + sample.mul_f64(0.25)
        } else {
            sample
        };
        self.rtt_ewma = Some(ewma);
        if self.rtt_samples.len() == 16 {
            self.rtt_samples.pop_front();
        }
        self.rtt_samples.push_back(sample);
    }

    async fn discard_current(&mut self) {
        if let Some(conn) = self.conn.take() {
            self.client
                .discard_connection_error(self.server_id.0, conn)
                .await;
        }
    }
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

    pub async fn body_server_order(&self, exclude: &[usize]) -> Vec<ServerId> {
        self.build_server_order(exclude)
            .await
            .into_iter()
            .map(ServerId)
            .collect()
    }

    pub async fn acquire_body_lane(
        &self,
        server: ServerId,
        groups: &[String],
    ) -> Result<BodyLaneLease> {
        self.acquire_body_lane_inner(server, groups, false, &[])
            .await
    }

    pub async fn acquire_extra_body_lane(
        &self,
        server: ServerId,
        groups: &[String],
    ) -> Result<BodyLaneLease> {
        self.acquire_body_lane_inner(server, groups, true, &[])
            .await
    }

    pub async fn acquire_extra_body_lane_excluding(
        &self,
        server: ServerId,
        groups: &[String],
        excluded_ips: &[IpAddr],
    ) -> Result<BodyLaneLease> {
        self.acquire_body_lane_inner(server, groups, true, excluded_ips)
            .await
    }

    async fn acquire_body_lane_inner(
        &self,
        server: ServerId,
        groups: &[String],
        extra: bool,
        excluded_ips: &[IpAddr],
    ) -> Result<BodyLaneLease> {
        let deadline = TokioInstant::now() + self.soft_timeout;
        let mut conn = if extra {
            match tokio::time::timeout_at(
                deadline,
                self.pool.acquire_extra_excluding(server, excluded_ips),
            )
            .await
            {
                Ok(result) => result?,
                Err(_) => return Err(self.soft_timeout_error()),
            }
        } else {
            self.acquire_before_deadline(server, deadline).await?
        };

        match tokio::time::timeout_at(deadline, Self::try_select_group(&mut conn, groups)).await {
            Ok(Ok(_)) => Ok(BodyLaneLease {
                client: self.clone(),
                server_id: server,
                remote_ip: conn.remote_ip(),
                conn: Some(conn),
                groups: groups.to_vec(),
                mode: BodyLaneMode::Sequential,
                rtt_ewma: None,
                rtt_samples: VecDeque::with_capacity(16),
            }),
            Ok(Err(error)) => {
                if is_connection_error(&error) {
                    self.discard_connection_error(server.0, conn).await;
                }
                Err(error)
            }
            Err(_) => {
                self.discard_connection_error(server.0, conn).await;
                Err(self.soft_timeout_error())
            }
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
        let mut had_retryable_uncertainty = false;

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
                    for (original_idx, exists) in remaining.iter().copied().zip(results) {
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
                    had_retryable_uncertainty = true;
                    last_error = Some(e);
                }
                Err(e) => return Err(e),
            }
        }

        if remaining.is_empty() {
            Ok(found)
        } else if had_retryable_uncertainty {
            Err(last_error.unwrap_or(NntpError::ServiceUnavailable))
        } else if had_success {
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
        self.fetch_body_with_groups_traced(message_id, groups)
            .await
            .result
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
        self.fetch_body_with_groups_excluding_traced(message_id, groups, exclude)
            .await
            .result
    }

    pub async fn fetch_body_with_groups_traced(
        &self,
        message_id: &str,
        groups: &[String],
    ) -> FetchBodyTrace {
        self.fetch_body_with_groups_excluding_traced(message_id, groups, &[])
            .await
    }

    pub async fn fetch_body_with_groups_excluding_traced(
        &self,
        message_id: &str,
        groups: &[String],
        exclude: &[usize],
    ) -> FetchBodyTrace {
        if groups.is_empty() {
            return FetchBodyTrace {
                attempts: Vec::new(),
                result: self
                    .fetch_with_failover_excluding(message_id, FetchKind::Body, exclude)
                    .await,
            };
        }

        let order = self.build_server_order(exclude).await;

        if order.is_empty() {
            return FetchBodyTrace {
                attempts: Vec::new(),
                result: Err(NntpError::PoolExhausted),
            };
        }

        let mut attempts = Vec::new();
        let mut last_error: Option<NntpError> = None;
        let mut last_retryable_error: Option<NntpError> = None;

        for idx in order {
            let server = ServerId(idx);
            let start = Instant::now();

            match self
                .fetch_from_server_with_groups(server, message_id, groups)
                .await
            {
                Ok((data, remote_ip)) => {
                    let elapsed = start.elapsed();
                    self.record_server_success(idx, elapsed).await;
                    attempts.push(FetchAttemptTrace {
                        server_idx: idx,
                        remote_ip,
                        elapsed,
                        outcome: FetchAttemptOutcome::Success,
                        error: None,
                    });
                    return FetchBodyTrace {
                        attempts,
                        result: Ok(data),
                    };
                }
                Err(NntpError::ArticleNotFound)
                | Err(NntpError::NoSuchArticle { .. })
                | Err(NntpError::NoArticleWithNumber) => {
                    attempts.push(FetchAttemptTrace {
                        server_idx: idx,
                        remote_ip: None,
                        elapsed: start.elapsed(),
                        outcome: FetchAttemptOutcome::NotFound,
                        error: Some("article not found".to_string()),
                    });
                    last_error = Some(NntpError::NoSuchArticle {
                        message_id: message_id.to_string(),
                    });
                    continue;
                }
                Err(e @ NntpError::SoftTimeout(_)) => {
                    attempts.push(FetchAttemptTrace {
                        server_idx: idx,
                        remote_ip: None,
                        elapsed: start.elapsed(),
                        outcome: FetchAttemptOutcome::TransientFailure,
                        error: Some(e.to_string()),
                    });
                    if let Some(reason) = cooldown_reason(&e) {
                        self.record_server_cooldown(idx, reason).await;
                    }
                    last_retryable_error = Some(e);
                    continue;
                }
                Err(NntpError::AuthenticationFailed)
                | Err(NntpError::AuthenticationRejected)
                | Err(NntpError::AccessDenied) => {
                    attempts.push(FetchAttemptTrace {
                        server_idx: idx,
                        remote_ip: None,
                        elapsed: start.elapsed(),
                        outcome: FetchAttemptOutcome::AuthenticationFailure,
                        error: Some("authentication/access failure".to_string()),
                    });
                    self.record_server_failure(idx, true).await;
                    last_error = Some(NntpError::AuthenticationFailed);
                    continue;
                }
                Err(e) if is_transient(&e) => {
                    attempts.push(FetchAttemptTrace {
                        server_idx: idx,
                        remote_ip: None,
                        elapsed: start.elapsed(),
                        outcome: FetchAttemptOutcome::TransientFailure,
                        error: Some(e.to_string()),
                    });
                    if let Some(reason) = cooldown_reason(&e) {
                        self.record_server_cooldown(idx, reason).await;
                    }
                    last_retryable_error = Some(e);
                    continue;
                }
                Err(e) => {
                    attempts.push(FetchAttemptTrace {
                        server_idx: idx,
                        remote_ip: None,
                        elapsed: start.elapsed(),
                        outcome: FetchAttemptOutcome::PermanentFailure,
                        error: Some(e.to_string()),
                    });
                    return FetchBodyTrace {
                        attempts,
                        result: Err(e),
                    };
                }
            }
        }

        FetchBodyTrace {
            attempts,
            result: Err(last_retryable_error
                .or(last_error)
                .unwrap_or(NntpError::PoolExhausted)),
        }
    }

    pub async fn fetch_body_with_groups_prefer_excluding_traced(
        &self,
        message_id: &str,
        groups: &[String],
        exclude: &[usize],
    ) -> FetchBodyTrace {
        if exclude.is_empty() {
            return self.fetch_body_with_groups_traced(message_id, groups).await;
        }

        let preferred = self
            .fetch_body_with_groups_excluding_traced(message_id, groups, exclude)
            .await;

        if preferred.attempts.is_empty()
            && matches!(preferred.result, Err(NntpError::PoolExhausted))
        {
            self.fetch_body_with_groups_traced(message_id, groups).await
        } else {
            preferred
        }
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

    pub async fn fetch_body_decoded_with_groups_traced(
        &self,
        message_id: &str,
        groups: &[String],
    ) -> DecodedBodyTrace {
        self.fetch_body_decoded_with_groups_excluding_traced(message_id, groups, &[])
            .await
    }

    pub async fn fetch_body_decoded_with_groups_prefer_excluding_traced(
        &self,
        message_id: &str,
        groups: &[String],
        exclude: &[usize],
    ) -> DecodedBodyTrace {
        if exclude.is_empty() {
            return self
                .fetch_body_decoded_with_groups_traced(message_id, groups)
                .await;
        }

        let preferred = self
            .fetch_body_decoded_with_groups_excluding_traced(message_id, groups, exclude)
            .await;

        if preferred.attempts.is_empty()
            && matches!(
                preferred.result,
                Err(DecodedBodyError::Nntp(NntpError::PoolExhausted))
            )
        {
            self.fetch_body_decoded_with_groups_traced(message_id, groups)
                .await
        } else {
            preferred
        }
    }

    pub async fn fetch_bodies_decoded_with_groups_prefer_excluding_traced(
        &self,
        message_ids: &[String],
        groups: &[String],
        exclude: &[usize],
    ) -> Vec<DecodedBodyTrace> {
        let mut traces: Vec<Option<DecodedBodyTrace>> =
            (0..message_ids.len()).map(|_| None).collect();

        self.fetch_bodies_decoded_with_groups_prefer_excluding_traced_each(
            message_ids,
            groups,
            exclude,
            |message_idx, trace| {
                traces[message_idx] = Some(trace);
                std::future::ready(())
            },
        )
        .await;

        traces
            .into_iter()
            .map(|trace| {
                trace.unwrap_or(DecodedBodyTrace {
                    attempts: Vec::new(),
                    result: Err(DecodedBodyError::Nntp(NntpError::PoolExhausted)),
                })
            })
            .collect()
    }

    pub async fn fetch_bodies_decoded_with_groups_prefer_excluding_traced_each<F, Fut>(
        &self,
        message_ids: &[String],
        groups: &[String],
        exclude: &[usize],
        mut on_trace: F,
    ) where
        F: FnMut(usize, DecodedBodyTrace) -> Fut,
        Fut: Future<Output = ()>,
    {
        if message_ids.is_empty() {
            return;
        }

        if message_ids.len() == 1 {
            let trace = self
                .fetch_body_decoded_with_groups_prefer_excluding_traced(
                    &message_ids[0],
                    groups,
                    exclude,
                )
                .await;
            on_trace(0, trace).await;
            return;
        }

        let mut order = self.build_server_order(exclude).await;
        if order.is_empty() && !exclude.is_empty() {
            order = self.build_server_order(&[]).await;
        }

        if order.is_empty() {
            for message_idx in 0..message_ids.len() {
                on_trace(
                    message_idx,
                    DecodedBodyTrace {
                        attempts: Vec::new(),
                        result: Err(DecodedBodyError::Nntp(NntpError::PoolExhausted)),
                    },
                )
                .await;
            }
            return;
        }

        let mut attempts_by_index: Vec<Vec<FetchAttemptTrace>> =
            (0..message_ids.len()).map(|_| Vec::new()).collect();
        let mut last_errors: Vec<Option<DecodedBodyError>> =
            (0..message_ids.len()).map(|_| None).collect();
        let mut pending: Vec<usize> = (0..message_ids.len()).collect();

        for idx in order {
            if pending.is_empty() {
                break;
            }

            let pending_now = std::mem::take(&mut pending);
            let pending_ids: Vec<String> = pending_now
                .iter()
                .map(|message_idx| message_ids[*message_idx].clone())
                .collect();

            let setup_deadline = TokioInstant::now() + self.soft_timeout;
            let batch_started = Instant::now();
            let mut conn = match self
                .acquire_before_deadline(ServerId(idx), setup_deadline)
                .await
            {
                Ok(conn) => conn,
                Err(error) => {
                    let make_error = Self::batch_setup_error_factory(&error);
                    for message_idx in pending_now {
                        let item = DecodedBatchItem {
                            elapsed: batch_started.elapsed(),
                            result: Err(DecodedBodyError::Nntp(make_error())),
                        };
                        match self
                            .classify_decoded_batch_item(
                                idx,
                                None,
                                &message_ids[message_idx],
                                item,
                                &mut attempts_by_index[message_idx],
                                &mut last_errors[message_idx],
                            )
                            .await
                        {
                            DecodedBatchDisposition::Terminal(result) => {
                                on_trace(
                                    message_idx,
                                    DecodedBodyTrace {
                                        attempts: std::mem::take(
                                            &mut attempts_by_index[message_idx],
                                        ),
                                        result,
                                    },
                                )
                                .await;
                            }
                            DecodedBatchDisposition::Retry => pending.push(message_idx),
                        }
                    }
                    continue;
                }
            };
            let remote_ip = Some(conn.remote_ip());

            let group_result = match tokio::time::timeout_at(
                setup_deadline,
                Self::try_select_group(&mut conn, groups),
            )
            .await
            {
                Ok(result) => result,
                Err(_) => {
                    self.discard_connection_error(idx, conn).await;
                    let error = self.soft_timeout_error();
                    let make_error = Self::batch_setup_error_factory(&error);
                    for message_idx in pending_now {
                        let item = DecodedBatchItem {
                            elapsed: batch_started.elapsed(),
                            result: Err(DecodedBodyError::Nntp(make_error())),
                        };
                        match self
                            .classify_decoded_batch_item(
                                idx,
                                remote_ip,
                                &message_ids[message_idx],
                                item,
                                &mut attempts_by_index[message_idx],
                                &mut last_errors[message_idx],
                            )
                            .await
                        {
                            DecodedBatchDisposition::Terminal(result) => {
                                on_trace(
                                    message_idx,
                                    DecodedBodyTrace {
                                        attempts: std::mem::take(
                                            &mut attempts_by_index[message_idx],
                                        ),
                                        result,
                                    },
                                )
                                .await;
                            }
                            DecodedBatchDisposition::Retry => pending.push(message_idx),
                        }
                    }
                    continue;
                }
            };

            if let Err(error) = group_result {
                if is_connection_error(&error) {
                    self.discard_connection_error(idx, conn).await;
                }
                let make_error = Self::batch_setup_error_factory(&error);
                for message_idx in pending_now {
                    let item = DecodedBatchItem {
                        elapsed: batch_started.elapsed(),
                        result: Err(DecodedBodyError::Nntp(make_error())),
                    };
                    match self
                        .classify_decoded_batch_item(
                            idx,
                            remote_ip,
                            &message_ids[message_idx],
                            item,
                            &mut attempts_by_index[message_idx],
                            &mut last_errors[message_idx],
                        )
                        .await
                    {
                        DecodedBatchDisposition::Terminal(result) => {
                            on_trace(
                                message_idx,
                                DecodedBodyTrace {
                                    attempts: std::mem::take(&mut attempts_by_index[message_idx]),
                                    result,
                                },
                            )
                            .await;
                        }
                        DecodedBatchDisposition::Retry => pending.push(message_idx),
                    }
                }
                continue;
            }

            let mut request_write_error = None;
            for message_id in &pending_ids {
                if let Err(error) = conn.write_body_request(message_id).await {
                    request_write_error = Some(error);
                    break;
                }
            }

            if request_write_error.is_none()
                && let Err(error) = conn.flush_commands().await
            {
                request_write_error = Some(error);
            }

            if let Some(error) = request_write_error {
                if is_connection_error(&error) {
                    self.discard_connection_error(idx, conn).await;
                }
                let make_error = Self::batch_setup_error_factory(&error);
                for message_idx in pending_now {
                    let item = DecodedBatchItem {
                        elapsed: batch_started.elapsed(),
                        result: Err(DecodedBodyError::Nntp(make_error())),
                    };
                    match self
                        .classify_decoded_batch_item(
                            idx,
                            remote_ip,
                            &message_ids[message_idx],
                            item,
                            &mut attempts_by_index[message_idx],
                            &mut last_errors[message_idx],
                        )
                        .await
                    {
                        DecodedBatchDisposition::Terminal(result) => {
                            on_trace(
                                message_idx,
                                DecodedBodyTrace {
                                    attempts: std::mem::take(&mut attempts_by_index[message_idx]),
                                    result,
                                },
                            )
                            .await;
                        }
                        DecodedBatchDisposition::Retry => pending.push(message_idx),
                    }
                }
                continue;
            }

            let mut conn = Some(conn);
            let mut closed_early = false;
            for (response_idx, message_idx) in pending_now.iter().copied().enumerate() {
                let item_started = Instant::now();
                let item_deadline = TokioInstant::now() + self.soft_timeout;
                let item = match tokio::time::timeout_at(
                    item_deadline,
                    Self::read_decoded_pipelined_body(
                        conn.as_mut()
                            .expect("connection is available while reading"),
                    ),
                )
                .await
                {
                    Ok(result) => DecodedBatchItem {
                        elapsed: item_started.elapsed(),
                        result,
                    },
                    Err(_) => {
                        self.discard_connection_error(
                            idx,
                            conn.take()
                                .expect("timed out connection is still owned by batch reader"),
                        )
                        .await;
                        closed_early = true;
                        DecodedBatchItem {
                            elapsed: item_started.elapsed(),
                            result: Err(DecodedBodyError::Nntp(self.soft_timeout_error())),
                        }
                    }
                };

                let poisoned = conn.as_ref().is_some_and(|conn| conn.is_poisoned());
                match self
                    .classify_decoded_batch_item(
                        idx,
                        remote_ip,
                        &message_ids[message_idx],
                        item,
                        &mut attempts_by_index[message_idx],
                        &mut last_errors[message_idx],
                    )
                    .await
                {
                    DecodedBatchDisposition::Terminal(result) => {
                        on_trace(
                            message_idx,
                            DecodedBodyTrace {
                                attempts: std::mem::take(&mut attempts_by_index[message_idx]),
                                result,
                            },
                        )
                        .await;
                    }
                    DecodedBatchDisposition::Retry => pending.push(message_idx),
                }

                if poisoned {
                    conn.take()
                        .expect("poisoned connection is still owned by batch reader")
                        .discard();
                    closed_early = true;
                }

                if closed_early {
                    for unread_idx in pending_now.iter().copied().skip(response_idx + 1) {
                        let item = DecodedBatchItem {
                            elapsed: item_started.elapsed(),
                            result: Err(DecodedBodyError::Nntp(NntpError::ConnectionClosed)),
                        };
                        match self
                            .classify_decoded_batch_item(
                                idx,
                                remote_ip,
                                &message_ids[unread_idx],
                                item,
                                &mut attempts_by_index[unread_idx],
                                &mut last_errors[unread_idx],
                            )
                            .await
                        {
                            DecodedBatchDisposition::Terminal(result) => {
                                on_trace(
                                    unread_idx,
                                    DecodedBodyTrace {
                                        attempts: std::mem::take(
                                            &mut attempts_by_index[unread_idx],
                                        ),
                                        result,
                                    },
                                )
                                .await;
                            }
                            DecodedBatchDisposition::Retry => pending.push(unread_idx),
                        }
                    }
                    break;
                }
            }
        }

        for message_idx in pending {
            on_trace(
                message_idx,
                DecodedBodyTrace {
                    attempts: std::mem::take(&mut attempts_by_index[message_idx]),
                    result: Err(last_errors[message_idx]
                        .take()
                        .unwrap_or(DecodedBodyError::Nntp(NntpError::PoolExhausted))),
                },
            )
            .await;
        }
    }

    /// Like [`fetch_body_decoded_with_groups`](Self::fetch_body_decoded_with_groups)
    /// but skips the specified servers.
    pub async fn fetch_body_decoded_with_groups_excluding(
        &self,
        message_id: &str,
        groups: &[String],
        exclude: &[usize],
    ) -> std::result::Result<DecodedBody, DecodedBodyError> {
        self.fetch_body_decoded_with_groups_excluding_traced(message_id, groups, exclude)
            .await
            .result
    }

    async fn classify_decoded_batch_item(
        &self,
        server_idx: usize,
        remote_ip: Option<IpAddr>,
        message_id: &str,
        item: DecodedBatchItem,
        attempts: &mut Vec<FetchAttemptTrace>,
        last_error: &mut Option<DecodedBodyError>,
    ) -> DecodedBatchDisposition {
        let elapsed = item.elapsed;
        match item.result {
            Ok(decoded) => {
                self.record_server_success(server_idx, elapsed).await;
                attempts.push(FetchAttemptTrace {
                    server_idx,
                    remote_ip,
                    elapsed,
                    outcome: FetchAttemptOutcome::Success,
                    error: None,
                });
                DecodedBatchDisposition::Terminal(Ok(decoded))
            }
            Err(DecodedBodyError::Decode { raw_size, error }) => {
                self.record_server_success(server_idx, elapsed).await;
                attempts.push(FetchAttemptTrace {
                    server_idx,
                    remote_ip,
                    elapsed,
                    outcome: FetchAttemptOutcome::Success,
                    error: None,
                });
                DecodedBatchDisposition::Terminal(Err(DecodedBodyError::Decode { raw_size, error }))
            }
            Err(DecodedBodyError::Nntp(
                NntpError::ArticleNotFound
                | NntpError::NoSuchArticle { .. }
                | NntpError::NoArticleWithNumber,
            )) => {
                attempts.push(FetchAttemptTrace {
                    server_idx,
                    remote_ip,
                    elapsed,
                    outcome: FetchAttemptOutcome::NotFound,
                    error: Some("article not found".to_string()),
                });
                *last_error = Some(DecodedBodyError::Nntp(NntpError::NoSuchArticle {
                    message_id: message_id.to_string(),
                }));
                DecodedBatchDisposition::Retry
            }
            Err(DecodedBodyError::Nntp(e @ NntpError::SoftTimeout(_))) => {
                attempts.push(FetchAttemptTrace {
                    server_idx,
                    remote_ip,
                    elapsed,
                    outcome: FetchAttemptOutcome::TransientFailure,
                    error: Some(e.to_string()),
                });
                if let Some(reason) = cooldown_reason(&e) {
                    self.record_server_cooldown(server_idx, reason).await;
                }
                *last_error = Some(DecodedBodyError::Nntp(e));
                DecodedBatchDisposition::Retry
            }
            Err(DecodedBodyError::Nntp(
                NntpError::AuthenticationFailed
                | NntpError::AuthenticationRejected
                | NntpError::AccessDenied,
            )) => {
                attempts.push(FetchAttemptTrace {
                    server_idx,
                    remote_ip,
                    elapsed,
                    outcome: FetchAttemptOutcome::AuthenticationFailure,
                    error: Some("authentication/access failure".to_string()),
                });
                self.record_server_failure(server_idx, true).await;
                *last_error = Some(DecodedBodyError::Nntp(NntpError::AuthenticationFailed));
                DecodedBatchDisposition::Retry
            }
            Err(DecodedBodyError::Nntp(e)) if is_transient(&e) => {
                attempts.push(FetchAttemptTrace {
                    server_idx,
                    remote_ip,
                    elapsed,
                    outcome: FetchAttemptOutcome::TransientFailure,
                    error: Some(e.to_string()),
                });
                if let Some(reason) = cooldown_reason(&e) {
                    self.record_server_cooldown(server_idx, reason).await;
                }
                *last_error = Some(DecodedBodyError::Nntp(e));
                DecodedBatchDisposition::Retry
            }
            Err(other) => {
                attempts.push(FetchAttemptTrace {
                    server_idx,
                    remote_ip,
                    elapsed,
                    outcome: FetchAttemptOutcome::PermanentFailure,
                    error: Some(format!("{other:?}")),
                });
                DecodedBatchDisposition::Terminal(Err(other))
            }
        }
    }

    pub async fn fetch_body_decoded_with_groups_excluding_traced(
        &self,
        message_id: &str,
        groups: &[String],
        exclude: &[usize],
    ) -> DecodedBodyTrace {
        let order = self.build_server_order(exclude).await;

        if order.is_empty() {
            return DecodedBodyTrace {
                attempts: Vec::new(),
                result: Err(DecodedBodyError::Nntp(NntpError::PoolExhausted)),
            };
        }

        let mut last_error: Option<DecodedBodyError> = None;
        let mut last_retryable_error: Option<DecodedBodyError> = None;
        let mut attempts = Vec::new();

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
                    attempts.push(FetchAttemptTrace {
                        server_idx: idx,
                        remote_ip: None,
                        elapsed,
                        outcome: FetchAttemptOutcome::Success,
                        error: None,
                    });
                    return DecodedBodyTrace {
                        attempts,
                        result: Ok(decoded),
                    };
                }
                Err(DecodedBodyError::Decode { raw_size, error }) => {
                    attempts.push(FetchAttemptTrace {
                        server_idx: idx,
                        remote_ip: None,
                        elapsed: start.elapsed(),
                        outcome: FetchAttemptOutcome::Success,
                        error: None,
                    });
                    return DecodedBodyTrace {
                        attempts,
                        result: Err(DecodedBodyError::Decode { raw_size, error }),
                    };
                }
                Err(DecodedBodyError::Nntp(
                    NntpError::ArticleNotFound
                    | NntpError::NoSuchArticle { .. }
                    | NntpError::NoArticleWithNumber,
                )) => {
                    attempts.push(FetchAttemptTrace {
                        server_idx: idx,
                        remote_ip: None,
                        elapsed: start.elapsed(),
                        outcome: FetchAttemptOutcome::NotFound,
                        error: Some("article not found".to_string()),
                    });
                    last_error = Some(DecodedBodyError::Nntp(NntpError::NoSuchArticle {
                        message_id: message_id.to_string(),
                    }));
                    continue;
                }
                Err(DecodedBodyError::Nntp(e @ NntpError::SoftTimeout(_))) => {
                    attempts.push(FetchAttemptTrace {
                        server_idx: idx,
                        remote_ip: None,
                        elapsed: start.elapsed(),
                        outcome: FetchAttemptOutcome::TransientFailure,
                        error: Some(e.to_string()),
                    });
                    if let Some(reason) = cooldown_reason(&e) {
                        self.record_server_cooldown(idx, reason).await;
                    }
                    last_retryable_error = Some(DecodedBodyError::Nntp(e));
                    continue;
                }
                Err(DecodedBodyError::Nntp(
                    NntpError::AuthenticationFailed
                    | NntpError::AuthenticationRejected
                    | NntpError::AccessDenied,
                )) => {
                    attempts.push(FetchAttemptTrace {
                        server_idx: idx,
                        remote_ip: None,
                        elapsed: start.elapsed(),
                        outcome: FetchAttemptOutcome::AuthenticationFailure,
                        error: Some("authentication/access failure".to_string()),
                    });
                    self.record_server_failure(idx, true).await;
                    last_error = Some(DecodedBodyError::Nntp(NntpError::AuthenticationFailed));
                    continue;
                }
                Err(DecodedBodyError::Nntp(e)) if is_transient(&e) => {
                    attempts.push(FetchAttemptTrace {
                        server_idx: idx,
                        remote_ip: None,
                        elapsed: start.elapsed(),
                        outcome: FetchAttemptOutcome::TransientFailure,
                        error: Some(e.to_string()),
                    });
                    if let Some(reason) = cooldown_reason(&e) {
                        self.record_server_cooldown(idx, reason).await;
                    }
                    last_retryable_error = Some(DecodedBodyError::Nntp(e));
                    continue;
                }
                Err(other) => {
                    attempts.push(FetchAttemptTrace {
                        server_idx: idx,
                        remote_ip: None,
                        elapsed: start.elapsed(),
                        outcome: FetchAttemptOutcome::PermanentFailure,
                        error: Some(format!("{other:?}")),
                    });
                    return DecodedBodyTrace {
                        attempts,
                        result: Err(other),
                    };
                }
            }
        }

        DecodedBodyTrace {
            attempts,
            result: Err(last_retryable_error
                .or(last_error)
                .unwrap_or(DecodedBodyError::Nntp(NntpError::PoolExhausted))),
        }
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

    pub async fn retire_server_ip(&self, server: ServerId, ip: IpAddr) {
        self.pool.retire_ip(server, ip).await;
    }

    pub fn try_acquire_blocking_body_lane(
        &self,
        groups: &[String],
        exclude: &[usize],
    ) -> Result<crate::blocking::BlockingBodyLane> {
        for server in self.blocking_body_server_order(exclude) {
            let permit = match self.pool.try_acquire_blocking_permit(server) {
                Ok(permit) => permit,
                Err(_) => continue,
            };
            let (config, excluded_ips, address_offset) =
                self.pool.blocking_connect_plan(server, &[])?;
            if !config.tls || config.starttls || config.tls_ca_cert.is_none() {
                continue;
            }
            let started = Instant::now();
            match crate::blocking::BlockingBodyLane::connect(
                server,
                &config,
                &excluded_ips,
                address_offset,
                groups,
                permit,
            ) {
                Ok(lane) => return Ok(lane),
                Err(error) => {
                    self.record_blocking_connect_failure(server.0, &error);
                    debug!(
                        server = server.0,
                        error = %error,
                        elapsed_ms = started.elapsed().as_millis(),
                        "blocking BODY lane connect failed"
                    );
                    continue;
                }
            }
        }
        Err(NntpError::PoolExhausted)
    }

    pub fn record_blocking_attempts(&self, attempts: &[FetchAttemptTrace]) {
        for attempt in attempts {
            match attempt.outcome {
                FetchAttemptOutcome::Success => {
                    let mut health = self.pool.health().blocking_lock();
                    health.record_success(attempt.server_idx);
                    health.record_latency(attempt.server_idx, attempt.elapsed);
                }
                FetchAttemptOutcome::AuthenticationFailure => {
                    self.pool
                        .health()
                        .blocking_lock()
                        .record_failure(attempt.server_idx, true);
                }
                FetchAttemptOutcome::TransientFailure => {
                    self.pool
                        .health()
                        .blocking_lock()
                        .record_cooldown(attempt.server_idx, CooldownReason::Transport);
                }
                FetchAttemptOutcome::NotFound | FetchAttemptOutcome::PermanentFailure => {}
            }
        }
    }

    fn blocking_body_server_order(&self, exclude: &[usize]) -> Vec<ServerId> {
        let server_count = self.pool.server_count();
        let server_groups = self.pool.server_groups();
        let Ok(mut health) = self.pool.health().try_lock() else {
            return Vec::new();
        };
        health.check_reenable_all();

        let mut candidates = Vec::with_capacity(server_count);
        for idx in 0..server_count {
            if exclude.contains(&idx) || !health.is_available(idx) {
                continue;
            }
            let candidate = {
                let (available, max) = self.pool.server_load(idx);
                let load_ratio = if max == 0 {
                    1.0
                } else {
                    1.0 - (available as f64 / max as f64)
                };
                let health_rank = match health.server(idx).state() {
                    ServerState::Healthy => 0u8,
                    ServerState::Degraded { .. } => 1,
                    ServerState::CoolingDown { .. } | ServerState::Disabled { .. } => 2,
                };
                let score = health.latency_ms(idx) * (1.0 + 2.0 * load_ratio);
                (server_groups[idx], health_rank, score, idx)
            };
            if candidate.1 < 2 {
                candidates.push(candidate);
            }
        }
        candidates.sort_by(|a, b| {
            a.0.cmp(&b.0)
                .then(a.1.cmp(&b.1))
                .then_with(|| a.2.partial_cmp(&b.2).unwrap_or(std::cmp::Ordering::Equal))
        });
        candidates
            .into_iter()
            .map(|(_, _, _, idx)| ServerId(idx))
            .collect()
    }

    fn record_blocking_connect_failure(&self, server_idx: usize, error: &NntpError) {
        if matches!(
            error,
            NntpError::AuthenticationFailed
                | NntpError::AuthenticationRejected
                | NntpError::AccessDenied
        ) {
            self.pool
                .health()
                .blocking_lock()
                .record_failure(server_idx, true);
        } else if let Some(reason) = cooldown_reason(error) {
            self.pool
                .health()
                .blocking_lock()
                .record_cooldown(server_idx, reason);
        }
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
        let mut last_retryable_error: Option<NntpError> = None;

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
                    last_retryable_error = Some(e);
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
                    last_retryable_error = Some(e);
                    continue;
                }
                Err(e) => {
                    // Non-recoverable error (pool shutdown, etc.)
                    return Err(e);
                }
            }
        }

        // All servers exhausted.
        Err(last_retryable_error
            .or(last_error)
            .unwrap_or(NntpError::PoolExhausted))
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
    ) -> Result<(Bytes, Option<IpAddr>)> {
        let deadline = TokioInstant::now() + self.soft_timeout;
        let mut attempts = 0u32;

        loop {
            let mut conn = self.acquire_before_deadline(server, deadline).await?;
            let remote_ip = Some(conn.remote_ip());

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
                Ok(response) => return Ok((response.data, remote_ip)),
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

            let stream_result = match tokio::time::timeout_at(deadline, async {
                conn.stream_yenc_article(message_id, |_| Ok(())).await
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
                Ok(article) => {
                    return Ok(DecodedBody {
                        raw_size: decoded_raw_size_from_fused_stats(&article.stats),
                        cpu: decoded_cpu_from_fused_stats(&article.stats),
                        io: decoded_io_from_fused_stats(&article.stats),
                        decoded: article.chunks,
                        result: article.result,
                    });
                }
                Err(FusedYencError::Yenc(error)) => {
                    return Err(DecodedBodyError::Decode { raw_size: 0, error });
                }
                Err(FusedYencError::Nntp(
                    e @ (NntpError::ArticleNotFound
                    | NntpError::NoSuchArticle { .. }
                    | NntpError::NoArticleWithNumber),
                )) => {
                    return Err(DecodedBodyError::Nntp(e));
                }
                Err(FusedYencError::Nntp(e)) if is_transient(&e) => {
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
                Err(FusedYencError::Nntp(e)) => {
                    if is_connection_error(&e) {
                        self.discard_connection_error(server.0, conn).await;
                    }
                    return Err(DecodedBodyError::Nntp(e));
                }
            }
        }
    }

    async fn read_decoded_pipelined_body(
        conn: &mut PooledConnection,
    ) -> std::result::Result<DecodedBody, DecodedBodyError> {
        let stream_result = conn.stream_next_yenc_article(|_| Ok(())).await;

        match stream_result {
            Ok(article) => Ok(DecodedBody {
                raw_size: decoded_raw_size_from_fused_stats(&article.stats),
                cpu: decoded_cpu_from_fused_stats(&article.stats),
                io: decoded_io_from_fused_stats(&article.stats),
                decoded: article.chunks,
                result: article.result,
            }),
            Err(FusedYencError::Yenc(error)) => {
                Err(DecodedBodyError::Decode { raw_size: 0, error })
            }
            Err(FusedYencError::Nntp(error)) => Err(DecodedBodyError::Nntp(error)),
        }
    }

    fn batch_setup_error_factory(error: &NntpError) -> Box<dyn Fn() -> NntpError + Send> {
        match error {
            NntpError::PoolExhausted => Box::new(|| NntpError::PoolExhausted),
            NntpError::SoftTimeout(duration) => {
                let duration = *duration;
                Box::new(move || NntpError::SoftTimeout(duration))
            }
            _ if is_transient(error) => Box::new(|| NntpError::ConnectionClosed),
            _ => {
                let message = error.to_string();
                Box::new(move || NntpError::MalformedResponse(message.clone()))
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
    async fn fetch_body_with_groups_traced_reports_successful_server_idx() {
        let port = spawn_scripted_server(vec![
            ScriptStep {
                expect_prefix: None,
                response: b"200 ready\r\n",
            },
            ScriptStep {
                expect_prefix: Some("CAPABILITIES"),
                response: b"101 Capability list:\r\nVERSION 2\r\nREADER\r\n.\r\n",
            },
            ScriptStep {
                expect_prefix: Some("GROUP "),
                response: b"211 1 1 1 alt.binaries.test\r\n",
            },
            ScriptStep {
                expect_prefix: Some("BODY "),
                response: b"222 1 <exists@example.com>\r\npayload\r\n.\r\n",
            },
        ])
        .await;

        let client = NntpClient::new(NntpClientConfig {
            servers: vec![scripted_server(port, 0)],
            max_idle_age: Duration::from_secs(300),
            max_retries_per_server: 0,
            soft_timeout: Duration::from_secs(5),
        });

        let trace = client
            .fetch_body_with_groups_traced(
                "<exists@example.com>",
                &[String::from("alt.binaries.test")],
            )
            .await;

        assert!(trace.result.is_ok(), "traced fetch should succeed");
        assert_eq!(trace.attempts.len(), 1);
        assert_eq!(trace.attempts[0].server_idx, 0);
        assert_eq!(
            trace.attempts[0].remote_ip,
            Some("127.0.0.1".parse().unwrap())
        );
        assert_eq!(trace.attempts[0].outcome, FetchAttemptOutcome::Success);
    }

    #[tokio::test]
    async fn grouped_body_missing_plus_transport_uncertainty_is_not_authoritative_not_found() {
        let primary_port = spawn_scripted_server(vec![
            ScriptStep {
                expect_prefix: None,
                response: b"200 ready\r\n",
            },
            ScriptStep {
                expect_prefix: Some("CAPABILITIES"),
                response: b"101 Capability list:\r\nVERSION 2\r\nREADER\r\n.\r\n",
            },
            ScriptStep {
                expect_prefix: Some("GROUP "),
                response: b"211 1 1 1 alt.binaries.test\r\n",
            },
            ScriptStep {
                expect_prefix: Some("BODY "),
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
                response: b"101 Capability list:\r\nVERSION 2\r\nREADER\r\n.\r\n",
            },
            ScriptStep {
                expect_prefix: Some("GROUP "),
                response: b"211 1 1 1 alt.binaries.test\r\n",
            },
            ScriptStep {
                expect_prefix: Some("BODY "),
                response: b"",
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

        let trace = client
            .fetch_body_with_groups_traced(
                "<maybe@example.com>",
                &[String::from("alt.binaries.test")],
            )
            .await;

        assert_eq!(trace.attempts.len(), 2);
        assert!(matches!(
            trace.result,
            Err(NntpError::ConnectionClosed) | Err(NntpError::SoftTimeout(_))
        ));
    }

    #[tokio::test]
    async fn extra_body_lane_reports_remote_ip() {
        let port = spawn_scripted_server(vec![
            ScriptStep {
                expect_prefix: None,
                response: b"200 ready\r\n",
            },
            ScriptStep {
                expect_prefix: Some("CAPABILITIES"),
                response: b"101 Capability list:\r\nVERSION 2\r\nREADER\r\n.\r\n",
            },
            ScriptStep {
                expect_prefix: Some("GROUP "),
                response: b"211 1 1 1 alt.binaries.test\r\n",
            },
        ])
        .await;

        let client = NntpClient::new(NntpClientConfig {
            servers: vec![scripted_server(port, 0)],
            max_idle_age: Duration::from_secs(300),
            max_retries_per_server: 0,
            soft_timeout: Duration::from_secs(5),
        });

        let lane = client
            .acquire_extra_body_lane(ServerId(0), &[String::from("alt.binaries.test")])
            .await
            .expect("extra BODY lane should acquire");

        assert_eq!(lane.remote_ip(), "127.0.0.1".parse::<IpAddr>().unwrap());
        lane.park();
    }

    #[tokio::test]
    async fn extra_body_lane_excluding_only_ip_fails_before_group_selection() {
        let port = spawn_scripted_server(vec![]).await;

        let client = NntpClient::new(NntpClientConfig {
            servers: vec![scripted_server(port, 0)],
            max_idle_age: Duration::from_secs(300),
            max_retries_per_server: 0,
            soft_timeout: Duration::from_secs(5),
        });

        let result = client
            .acquire_extra_body_lane_excluding(
                ServerId(0),
                &[String::from("alt.binaries.test")],
                &["127.0.0.1".parse().unwrap()],
            )
            .await;

        assert!(result.is_err(), "excluded only IP should not connect");
    }

    #[tokio::test]
    async fn extra_body_lane_uses_fresh_connection_instead_of_idle_pool() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let accepted = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let accepted_task = accepted.clone();

        tokio::spawn(async move {
            for _ in 0..2 {
                let (mut socket, _) = listener.accept().await.unwrap();
                accepted_task.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                socket.write_all(b"200 ready\r\n").await.unwrap();
                socket.flush().await.unwrap();
                let line = read_command_line(&mut socket).await;
                assert!(line.starts_with("CAPABILITIES"));
                socket
                    .write_all(b"101 Capability list:\r\nVERSION 2\r\nREADER\r\n.\r\n")
                    .await
                    .unwrap();
                socket.flush().await.unwrap();
                let line = read_command_line(&mut socket).await;
                assert!(line.starts_with("GROUP "));
                socket
                    .write_all(b"211 1 1 1 alt.binaries.test\r\n")
                    .await
                    .unwrap();
                socket.flush().await.unwrap();
            }
        });

        let client = NntpClient::new(NntpClientConfig {
            servers: vec![scripted_server(port, 0)],
            max_idle_age: Duration::from_secs(300),
            max_retries_per_server: 0,
            soft_timeout: Duration::from_secs(5),
        });

        let first = client
            .acquire_body_lane(ServerId(0), &[String::from("alt.binaries.test")])
            .await
            .expect("normal BODY lane should acquire");
        first.park();

        let second = client
            .acquire_extra_body_lane(ServerId(0), &[String::from("alt.binaries.test")])
            .await
            .expect("extra BODY lane should acquire fresh connection");
        second.discard().await;

        assert_eq!(
            accepted.load(std::sync::atomic::Ordering::SeqCst),
            2,
            "extra BODY lane must create a fresh connection instead of reusing idle"
        );
    }

    #[tokio::test]
    async fn parked_extra_body_lane_is_not_returned_to_normal_idle_pool() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let accepted = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let accepted_task = accepted.clone();

        tokio::spawn(async move {
            for _ in 0..2 {
                let (mut socket, _) = listener.accept().await.unwrap();
                accepted_task.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                socket.write_all(b"200 ready\r\n").await.unwrap();
                socket.flush().await.unwrap();
                let line = read_command_line(&mut socket).await;
                assert!(line.starts_with("CAPABILITIES"));
                socket
                    .write_all(b"101 Capability list:\r\nVERSION 2\r\nREADER\r\n.\r\n")
                    .await
                    .unwrap();
                socket.flush().await.unwrap();
                let line = read_command_line(&mut socket).await;
                assert!(line.starts_with("GROUP "));
                socket
                    .write_all(b"211 1 1 1 alt.binaries.test\r\n")
                    .await
                    .unwrap();
                socket.flush().await.unwrap();
            }
        });

        let client = NntpClient::new(NntpClientConfig {
            servers: vec![scripted_server(port, 0)],
            max_idle_age: Duration::from_secs(300),
            max_retries_per_server: 0,
            soft_timeout: Duration::from_secs(5),
        });

        let extra = client
            .acquire_extra_body_lane(ServerId(0), &[String::from("alt.binaries.test")])
            .await
            .expect("extra BODY lane should acquire");
        extra.park();
        tokio::time::sleep(Duration::from_millis(50)).await;

        let normal = client
            .acquire_body_lane(ServerId(0), &[String::from("alt.binaries.test")])
            .await
            .expect("normal BODY lane should not reuse parked extra connection");
        normal.discard().await;

        assert_eq!(
            accepted.load(std::sync::atomic::Ordering::SeqCst),
            2,
            "parked over-max BODY lane must close instead of entering normal idle pool"
        );
    }

    #[tokio::test]
    async fn fetch_body_prefer_excluding_falls_back_when_only_server_excluded() {
        let port = spawn_scripted_server(vec![
            ScriptStep {
                expect_prefix: None,
                response: b"200 ready\r\n",
            },
            ScriptStep {
                expect_prefix: Some("CAPABILITIES"),
                response: b"101 Capability list:\r\nVERSION 2\r\nREADER\r\n.\r\n",
            },
            ScriptStep {
                expect_prefix: Some("GROUP "),
                response: b"211 1 1 1 alt.binaries.test\r\n",
            },
            ScriptStep {
                expect_prefix: Some("BODY "),
                response: b"222 1 <exists@example.com>\r\npayload\r\n.\r\n",
            },
        ])
        .await;

        let client = NntpClient::new(NntpClientConfig {
            servers: vec![scripted_server(port, 0)],
            max_idle_age: Duration::from_secs(300),
            max_retries_per_server: 0,
            soft_timeout: Duration::from_secs(5),
        });

        let trace = client
            .fetch_body_with_groups_prefer_excluding_traced(
                "<exists@example.com>",
                &[String::from("alt.binaries.test")],
                &[0],
            )
            .await;

        assert!(trace.result.is_ok(), "fallback fetch should succeed");
        assert_eq!(trace.attempts.len(), 1);
        assert_eq!(trace.attempts[0].server_idx, 0);
        assert_eq!(trace.attempts[0].outcome, FetchAttemptOutcome::Success);
    }

    #[tokio::test]
    async fn fetch_body_prefer_excluding_uses_alternate_server_first() {
        let backup_port = spawn_scripted_server(vec![
            ScriptStep {
                expect_prefix: None,
                response: b"200 ready\r\n",
            },
            ScriptStep {
                expect_prefix: Some("CAPABILITIES"),
                response: b"101 Capability list:\r\nVERSION 2\r\nREADER\r\n.\r\n",
            },
            ScriptStep {
                expect_prefix: Some("GROUP "),
                response: b"211 1 1 1 alt.binaries.test\r\n",
            },
            ScriptStep {
                expect_prefix: Some("BODY "),
                response: b"222 1 <exists@example.com>\r\nbackup\r\n.\r\n",
            },
        ])
        .await;

        let client = NntpClient::new(NntpClientConfig {
            servers: vec![scripted_server(1, 0), scripted_server(backup_port, 0)],
            max_idle_age: Duration::from_secs(300),
            max_retries_per_server: 0,
            soft_timeout: Duration::from_secs(5),
        });

        let trace = client
            .fetch_body_with_groups_prefer_excluding_traced(
                "<exists@example.com>",
                &[String::from("alt.binaries.test")],
                &[0],
            )
            .await;

        assert!(trace.result.is_ok(), "alternate fetch should succeed");
        assert_eq!(trace.attempts.len(), 1);
        assert_eq!(trace.attempts[0].server_idx, 1);
        assert_eq!(trace.attempts[0].outcome, FetchAttemptOutcome::Success);
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
    async fn stat_many_fails_over_after_malformed_pipelined_response_when_article_is_confirmed() {
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
            .stat_many(&["<exists@example.com>"])
            .await
            .expect("stat_many should fail over");
        assert_eq!(results, vec![true]);
    }

    #[tokio::test]
    async fn stat_many_is_inconclusive_when_malformed_response_precedes_clean_missing() {
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

        let err = client
            .stat_many(&["<missing@example.com>"])
            .await
            .expect_err("malformed STAT plus clean missing must be inconclusive");
        assert!(matches!(err, NntpError::MalformedResponse(_)));
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

    #[tokio::test]
    async fn confirm_exists_for_probe_marks_malformed_stat_with_clean_missing_inconclusive() {
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

        let result = client
            .confirm_exists_for_probe(&["<missing@example.com>"])
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
