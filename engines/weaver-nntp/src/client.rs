use std::future::Future;
use std::net::IpAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use rand::distr::weighted::WeightedIndex;
use rand::prelude::*;
use tokio::time::Instant as TokioInstant;
use tracing::{debug, trace, warn};
use weaver_yenc::{CheckpointPlan, YencError};

use crate::connection::ServerConfig;
use crate::error::{NntpError, Result};
use crate::fused_yenc::{FusedArticleBody, FusedYencArticleStats, FusedYencError};
use crate::health::{CooldownReason, ServerState};
use crate::pool::{
    BodyServerAvailability, FreshConnectAdmission, NntpPool, PoolConfig, PooledConnection,
    ServerId, ServerPoolConfig,
};
use crate::tls::TransportReadStats;
use crate::transfer::{
    ActiveTransferBudget, QuotaRejection, ServerTransferSnapshot, StableServerId,
};

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
    /// Per-article active-time budget. Deliberate local rate-limit waits are
    /// excluded; exceeding the budget triggers failover to the next server.
    /// This is separate from the connection-level `command_timeout`.
    pub soft_timeout: Duration,
}

impl NntpClientConfig {
    /// Create a client config with a single server.
    pub fn single(server: ServerConfig, max_connections: usize) -> Self {
        NntpClientConfig {
            servers: vec![ServerPoolConfig {
                server,
                max_connections,
                ..ServerPoolConfig::default()
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
    /// Per-article active-time budget, excluding deliberate local rate waits.
    soft_timeout: Duration,
}

/// How many yielding `try_lock` attempts an owned-lane server selection makes
/// on the shared health mutex before reporting contention. The critical
/// sections behind that mutex are microseconds long, so a handful of retries
/// absorbs ordinary collisions without ever blocking a worker thread.
const BLOCKING_HEALTH_LOCK_SPINS: usize = 4;

/// Why a synchronous owned BODY lane could not be acquired.
///
/// Capacity outcomes are separated from server availability and transport
/// failures so callers can return leased work to the scheduler without
/// tearing down healthy cached lanes or consuming a download retry.
#[derive(Debug)]
pub enum BlockingBodyLaneAcquireError {
    ProviderCapacity(NntpError),
    LocalCapacity,
    NoEligibleServer,
    /// The health mutex was held elsewhere, so candidates could not be ranked.
    /// This says nothing about server availability — the caller should hand
    /// the work back to the scheduler and try again, not conclude that no
    /// server can serve it.
    SelectionContended,
    Other(NntpError),
}

impl BlockingBodyLaneAcquireError {
    fn from_connect_error(error: NntpError) -> Self {
        if matches!(
            error,
            NntpError::TooManyConnections | NntpError::ServerOverLimit { .. }
        ) {
            Self::ProviderCapacity(error)
        } else {
            Self::Other(error)
        }
    }

    pub fn is_capacity_admission(&self) -> bool {
        matches!(self, Self::ProviderCapacity(_) | Self::LocalCapacity)
    }

    /// Whether the leased work should be handed straight back to the scheduler
    /// and retried on the owned fast path, rather than falling back to an
    /// async lane. Capacity admission and selection contention are both
    /// "ask again shortly", not verdicts about the servers.
    pub fn should_requeue_owned_work(&self) -> bool {
        self.is_capacity_admission() || matches!(self, Self::SelectionContended)
    }

    /// A short, stable label for metrics and logs. Distinguishing the kinds is
    /// the whole point of the counters: local capacity and selection contention
    /// are self-clearing, provider capacity is the provider refusing sockets,
    /// and `other` is a transport failure that deserves attention.
    pub fn kind(&self) -> &'static str {
        match self {
            Self::ProviderCapacity(_) => "provider_capacity",
            Self::LocalCapacity => "local_capacity",
            Self::NoEligibleServer => "no_eligible_server",
            Self::SelectionContended => "selection_contended",
            Self::Other(_) => "other",
        }
    }
}

impl std::fmt::Display for BlockingBodyLaneAcquireError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ProviderCapacity(error) | Self::Other(error) => error.fmt(formatter),
            Self::LocalCapacity => formatter.write_str("blocking BODY lane capacity is saturated"),
            Self::NoEligibleServer => formatter.write_str("no eligible blocking BODY server"),
            Self::SelectionContended => {
                formatter.write_str("blocking BODY server selection contended on server health")
            }
        }
    }
}

impl std::error::Error for BlockingBodyLaneAcquireError {}

/// Whether the synchronous owned lanes can be given a batch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BlockingBodyLaneCandidacy {
    /// At least one eligible server can carry an owned blocking lane.
    Candidate,
    /// No eligible server can. This is the only answer that justifies the
    /// asynchronous path.
    None,
    /// The shared server health state was busy, so the candidates could not be
    /// ranked. Says nothing about the servers: ask again in a moment, on the
    /// owned lanes.
    Contended,
}

/// A BODY fetch that was streamed and decoded inline.
#[derive(Debug)]
pub struct DecodedBody {
    pub raw_size: u32,
    pub decoded: Vec<Box<[u8]>>,
    /// What the body decoded to. yEnc and uuencode carry different evidence, so
    /// this is a sum type rather than a yEnc result with absent fields.
    pub body: FusedArticleBody,
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
    pub throttle_wait: Duration,
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
        throttle_wait: stats.throttle_wait,
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
    QuotaBlocked,
    QuotaUnrequested,
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

/// BODY servers that can be leased now, plus the most useful quota block when
/// quota filtering removed an otherwise eligible server.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BodyServerSelection {
    pub eligible: Vec<ServerId>,
    /// Prefers the earliest scheduled retry; manual-only blocks are retained
    /// when no scheduled retry is available.
    pub quota_blocked: Option<QuotaRejection>,
}

fn retain_earliest_quota_rejection(
    selected: &mut Option<QuotaRejection>,
    mut candidate: QuotaRejection,
) {
    let first_blocked_registry_revision = selected
        .as_ref()
        .map_or(candidate.registry_capacity_revision, |current| {
            current.registry_capacity_revision
        });
    let replace = match selected.as_ref() {
        None => true,
        Some(current) => match (current.retry_at, candidate.retry_at) {
            (None, Some(_)) => true,
            (Some(current), Some(candidate)) => candidate < current,
            (None, None) | (Some(_), None) => false,
        },
    };
    if replace {
        candidate.registry_capacity_revision = first_blocked_registry_revision;
        *selected = Some(candidate);
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BodyLaneMode {
    Sequential,
    Pipelined { depth: u8 },
}

fn blend_ewma(current: Option<Duration>, sample: Duration) -> Duration {
    match current {
        Some(current) => current.mul_f64(0.75) + sample.mul_f64(0.25),
        None => sample,
    }
}

/// Whether an owned blocking BODY lane can serve this server.
///
/// Owned lanes are the download fast path and every server gets one, plaintext
/// included: a single lane pool per server is what lets one warm connection
/// serve BODY, PAR2 recovery and the existence probe alike, instead of the
/// probe having to reclaim a permit and cold-dial its own socket.
///
/// The one arrangement still left out is STARTTLS, whose in-band upgrade the
/// blocking transport does not implement.
fn supports_blocking_body_lane(config: &ServerConfig) -> bool {
    if config.starttls {
        return false;
    }
    if !config.tls {
        // Plain TCP needs no trust material and no backend selection.
        return true;
    }
    if config.tls_name_mismatch_certificate_der.is_some() {
        return blocking_lane_tls_eligible(config, crate::tls::NntpTlsBackend::ManualRustls);
    }
    match crate::tls::selected_blocking_tls_backend() {
        Ok(backend) => blocking_lane_tls_eligible(config, backend),
        Err(_) => false,
    }
}

fn blocking_lane_tls_eligible(config: &ServerConfig, backend: crate::tls::NntpTlsBackend) -> bool {
    if !config.tls || config.starttls {
        return false;
    }
    match backend {
        // The rustls lane trusts webpki roots, so a pinned CA is optional.
        crate::tls::NntpTlsBackend::ManualRustls => true,
        // The s2n lane builds trust exclusively from a pinned CA PEM.
        #[cfg(not(windows))]
        crate::tls::NntpTlsBackend::S2n => config.tls_ca_cert.is_some(),
    }
}

#[derive(Debug, Default, Clone)]
pub struct BodyLaneBatchStats {
    /// Work offered to this lane, capped by pipeline depth.
    pub offered: usize,
    /// BODY commands actually admitted and issued.
    pub requested: usize,
    /// Offered work left unrequested after quota admission stopped the prefix.
    pub unrequested: usize,
    pub quota_rejection: Option<QuotaRejection>,
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
    /// Command-to-status-line wait, sampled only when nothing else was
    /// outstanding on the connection.
    latency_ewma: Option<Duration>,
    /// Status-line-to-terminator wait: the article's own cost on the wire.
    transfer_ewma: Option<Duration>,
    checkpoint_plan: CheckpointPlan,
}

struct DecodedBatchItem {
    elapsed: Duration,
    result: std::result::Result<DecodedBody, DecodedBodyError>,
}

#[allow(clippy::large_enum_variant)] // Terminal is the hot path; boxing would allocate per decoded article.
enum DecodedBatchDisposition {
    Terminal(std::result::Result<DecodedBody, DecodedBodyError>),
    Retry,
}

impl BodyLaneLease {
    pub fn server_id(&self) -> ServerId {
        self.server_id
    }

    pub fn stable_server_id(&self) -> Option<StableServerId> {
        self.client.pool.stable_server_id(self.server_id)
    }

    pub fn transfer_snapshot(&self) -> Option<ServerTransferSnapshot> {
        self.client
            .pool
            .server_transfer_control(self.server_id)
            .map(|control| control.snapshot())
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

    pub fn latency_ewma(&self) -> Option<Duration> {
        self.latency_ewma
    }

    pub fn transfer_ewma(&self) -> Option<Duration> {
        self.transfer_ewma
    }

    pub fn supports_pipelining(&self) -> bool {
        self.conn
            .as_ref()
            .is_some_and(|conn| conn.capabilities().supports_pipelining())
    }

    /// Checkpoint every decoded article according to the immutable geometry
    /// snapshot captured by its batch.
    ///
    /// A download batch belongs to one job, so the caller sets this once per
    /// batch. It is re-applied before every response, including `None`, so a
    /// pooled connection cannot carry another job's geometry.
    pub fn set_checkpoint_plan(&mut self, checkpoint_plan: CheckpointPlan) {
        self.checkpoint_plan = checkpoint_plan;
    }

    pub fn park(self) {}

    pub async fn discard(mut self) {
        self.discard_current().await;
    }

    pub async fn fetch_decoded_sequential(&mut self, message_id: &str) -> DecodedBodyTrace {
        self.fetch_decoded_sequential_with_estimate(message_id, 0)
            .await
    }

    pub async fn fetch_decoded_sequential_with_estimate(
        &mut self,
        message_id: &str,
        estimated_body_bytes: u64,
    ) -> DecodedBodyTrace {
        self.mode = BodyLaneMode::Sequential;
        let started = Instant::now();
        let result = self
            .read_decoded_body(message_id, estimated_body_bytes)
            .await;
        let elapsed = started.elapsed();
        let policy_elapsed = result.as_ref().map_or(elapsed, |decoded| {
            elapsed.saturating_sub(decoded.io.throttle_wait)
        });
        if result.is_ok() {
            // Nothing else was outstanding, so the status-line wait is a clean
            // latency sample.
            let latency = self.take_response_line_wait().min(policy_elapsed);
            self.observe_latency(latency);
            self.observe_transfer(policy_elapsed.saturating_sub(latency));
        }

        if result.as_ref().is_err_and(
            |error| matches!(error, DecodedBodyError::Nntp(e) if is_connection_error(e)),
        ) || self.conn.as_ref().is_some_and(|conn| conn.is_poisoned())
        {
            self.discard_current().await;
        }

        self.trace_item(
            message_id,
            DecodedBatchItem {
                elapsed: policy_elapsed,
                result,
            },
        )
        .await
    }

    pub async fn fetch_decoded_pipeline<F, Fut>(
        &mut self,
        message_ids: &[&str],
        depth: usize,
        on_trace: F,
    ) -> BodyLaneBatchStats
    where
        F: FnMut(usize, DecodedBodyTrace, BodyLaneTraceMeta) -> Fut,
        Fut: Future<Output = ()>,
    {
        self.fetch_decoded_pipeline_with_estimates(message_ids, &[], depth, on_trace)
            .await
    }

    pub async fn fetch_decoded_pipeline_with_estimates<F, Fut>(
        &mut self,
        message_ids: &[&str],
        estimated_body_bytes: &[u64],
        depth: usize,
        on_trace: F,
    ) -> BodyLaneBatchStats
    where
        F: FnMut(usize, DecodedBodyTrace, BodyLaneTraceMeta) -> Fut,
        Fut: Future<Output = ()>,
    {
        self.mode = match depth {
            0 | 1 => BodyLaneMode::Sequential,
            depth => BodyLaneMode::Pipelined {
                depth: depth.min(u8::MAX as usize) as u8,
            },
        };
        self.fetch_decoded_pipeline_inner(message_ids, estimated_body_bytes, depth, on_trace)
            .await
    }

    async fn fetch_decoded_pipeline_inner<F, Fut>(
        &mut self,
        message_ids: &[&str],
        estimated_body_bytes: &[u64],
        max_depth: usize,
        mut on_trace: F,
    ) -> BodyLaneBatchStats
    where
        F: FnMut(usize, DecodedBodyTrace, BodyLaneTraceMeta) -> Fut,
        Fut: Future<Output = ()>,
    {
        let offered = message_ids.len().min(max_depth);
        let batch_started = Instant::now();
        let mut stats = BodyLaneBatchStats {
            offered,
            ..BodyLaneBatchStats::default()
        };
        if offered == 0 {
            return stats;
        }

        let request_write_error = if let Some(conn) = self.conn.as_mut() {
            let mut error = None;
            for (idx, message_id) in message_ids[..offered].iter().enumerate() {
                let estimate = estimated_body_bytes.get(idx).copied().unwrap_or(0);
                match conn
                    .write_body_request_with_estimate(message_id, estimate)
                    .await
                {
                    Ok(()) => stats.requested += 1,
                    Err(NntpError::QuotaBlocked(rejection)) => {
                        stats.quota_rejection = Some(*rejection);
                        stats.unrequested = offered.saturating_sub(stats.requested);
                        break;
                    }
                    Err(write_error) => {
                        error = Some(write_error);
                        break;
                    }
                }
            }
            if stats.requested > 0
                && error.is_none()
                && let Err(flush_error) = conn.flush_commands().await
            {
                error = Some(flush_error);
            }
            error
        } else {
            stats.requested = offered;
            Some(NntpError::ConnectionClosed)
        };

        let requested = stats.requested;
        if requested == 0 && stats.quota_rejection.is_none() {
            stats.elapsed = batch_started.elapsed();
            return stats;
        }

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
                        unresolved_count: 0,
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
            let mut budget = ActiveTransferBudget::new(self.client.soft_timeout);
            self.conn
                .as_mut()
                .expect("connection is available while reading pipeline body")
                .set_checkpoint_plan(self.checkpoint_plan.clone());
            let result = NntpClient::read_decoded_pipelined_body(
                self.conn
                    .as_mut()
                    .expect("connection is available while reading pipeline body"),
                &mut budget,
            )
            .await;
            let elapsed = item_started.elapsed();
            let policy_elapsed = result.as_ref().map_or(elapsed, |decoded| {
                elapsed.saturating_sub(decoded.io.throttle_wait)
            });
            if result.is_ok() {
                let response_line_wait = self.take_response_line_wait().min(policy_elapsed);
                // Only the head of the batch was issued with nothing else in
                // flight; later responses are already queued behind it.
                if response_idx == 0 {
                    self.observe_latency(response_line_wait);
                }
                self.observe_transfer(policy_elapsed.saturating_sub(response_line_wait));
            }
            let poisoned = self.conn.as_ref().is_some_and(|conn| conn.is_poisoned());

            let trace = self
                .trace_item(
                    message_id,
                    DecodedBatchItem {
                        elapsed: policy_elapsed,
                        result,
                    },
                )
                .await;
            stats.completed += 1;
            batch_clean_so_far &= decoded_result_keeps_connection(&trace.result);

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
            let is_batch_complete =
                closed_early || (response_idx + 1 == requested && stats.quota_rejection.is_none());
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

        if let Some(source) = stats.quota_rejection.clone() {
            let control = self.client.pool.server_transfer_control(self.server_id);
            for (tail_idx, message_id) in
                message_ids.iter().enumerate().take(offered).skip(requested)
            {
                let estimate = estimated_body_bytes.get(tail_idx).copied().unwrap_or(0);
                let error = if tail_idx == requested {
                    NntpError::quota_blocked(source.clone())
                } else if let Some(rejection) = control
                    .as_ref()
                    .and_then(|control| control.quota_rejection_for(estimate))
                {
                    NntpError::quota_blocked(rejection)
                } else {
                    NntpError::body_not_requested_due_to_quota(source.clone(), estimate)
                };
                let trace = self
                    .trace_item(
                        message_id,
                        DecodedBatchItem {
                            elapsed: Duration::ZERO,
                            result: Err(DecodedBodyError::Nntp(error)),
                        },
                    )
                    .await;
                stats.completed += 1;
                let batch_complete = !closed_early && tail_idx + 1 == offered;
                on_trace(
                    tail_idx,
                    trace,
                    BodyLaneTraceMeta {
                        batch_complete,
                        batch_clean: batch_clean_so_far,
                        batch_response_count: if batch_complete { requested as u64 } else { 0 },
                        unresolved_count: 0,
                        connection_discarded: stats.connection_discarded,
                    },
                )
                .await;
            }
        }

        stats.elapsed = batch_started.elapsed();
        stats
    }

    async fn read_decoded_body(
        &mut self,
        message_id: &str,
        estimated_body_bytes: u64,
    ) -> std::result::Result<DecodedBody, DecodedBodyError> {
        let mut budget = ActiveTransferBudget::new(self.client.soft_timeout);
        let checkpoint_plan = self.checkpoint_plan.clone();
        let Some(conn) = self.conn.as_mut() else {
            return Err(DecodedBodyError::Nntp(NntpError::ConnectionClosed));
        };
        conn.set_checkpoint_plan(checkpoint_plan);

        let stream_result = conn
            .stream_yenc_article_with_active_budget(
                message_id,
                estimated_body_bytes,
                &mut budget,
                |_| Ok(()),
            )
            .await;

        match stream_result {
            Ok(article) => Ok(DecodedBody {
                raw_size: decoded_raw_size_from_fused_stats(&article.stats),
                cpu: decoded_cpu_from_fused_stats(&article.stats),
                io: decoded_io_from_fused_stats(&article.stats),
                decoded: article.chunks,
                body: article.body,
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

    fn observe_latency(&mut self, sample: Duration) {
        self.latency_ewma = Some(blend_ewma(self.latency_ewma, sample));
    }

    fn observe_transfer(&mut self, sample: Duration) {
        self.transfer_ewma = Some(blend_ewma(self.transfer_ewma, sample));
    }

    /// Consume the last article's status-line wait so one response's latency
    /// cannot be credited to the next.
    fn take_response_line_wait(&mut self) -> Duration {
        self.conn
            .as_mut()
            .map_or(Duration::ZERO, |conn| conn.take_response_line_wait())
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
        self.body_server_selection(exclude).await.eligible
    }

    pub async fn body_server_availability(
        &self,
        failure_excludes: &[usize],
        retention_excludes: &[usize],
        requested_body_bytes: u64,
    ) -> BodyServerAvailability {
        self.pool
            .body_server_availability(failure_excludes, retention_excludes, requested_body_bytes)
            .await
    }

    pub async fn body_server_selection(&self, exclude: &[usize]) -> BodyServerSelection {
        self.body_server_selection_with_estimate(exclude, 0).await
    }

    pub async fn body_server_selection_with_estimate(
        &self,
        exclude: &[usize],
        requested_body_bytes: u64,
    ) -> BodyServerSelection {
        let mut eligible = Vec::new();
        let mut quota_blocked = None;
        for idx in self.build_server_order(exclude).await {
            let server = ServerId(idx);
            if let Some(rejection) = self
                .pool
                .server_transfer_control(server)
                .and_then(|control| control.quota_rejection_for_dispatch(requested_body_bytes))
            {
                retain_earliest_quota_rejection(&mut quota_blocked, rejection);
                continue;
            }
            eligible.push(server);
        }
        BodyServerSelection {
            eligible,
            quota_blocked,
        }
    }

    /// Return the current request-specific quota rejection for one server.
    /// Cached owned lanes use this to revalidate admission without touching
    /// connection health or acquiring a new lane. It is a dispatch decision,
    /// so a rejection latches the server's blocked signal.
    pub fn server_quota_rejection(
        &self,
        server: ServerId,
        requested_body_bytes: u64,
    ) -> Option<QuotaRejection> {
        self.pool
            .server_transfer_control(server)
            .and_then(|control| control.quota_rejection_for_dispatch(requested_body_bytes))
    }

    /// True only when the active pool contains at least one normal fill server
    /// and every such fill is at its absolute quota limit. Backfill servers do
    /// not participate in this global presentation predicate.
    pub fn all_normal_fill_servers_quota_blocked(&self) -> bool {
        let backfill = self.pool.server_backfill_flags();
        let mut saw_fill = false;
        for (idx, is_backfill) in backfill
            .iter()
            .copied()
            .enumerate()
            .take(self.pool.server_count())
        {
            if is_backfill {
                continue;
            }
            saw_fill = true;
            if !self
                .pool
                .server_transfer_control(ServerId(idx))
                .is_some_and(|control| control.snapshot().quota_blocked)
            {
                return false;
            }
        }
        saw_fill
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
        // A BODY lane fetches by message-id, which RFC 3977 answers with no
        // group selected, so the GROUP round trip is pure added latency on
        // every lane start. The candidate group is still offered to connect:
        // a server that has proven it insists on one (see `crate::server_caps`)
        // takes it inside the session-setup write, and only such a server
        // walks the candidate list below.
        let initial_group = groups.first().map(String::as_str);
        let mut conn = if extra {
            match tokio::time::timeout_at(
                deadline,
                self.pool
                    .acquire_extra_excluding_for_group(server, excluded_ips, initial_group),
            )
            .await
            {
                Ok(result) => result?,
                Err(_) => return Err(self.acquire_timeout_error()),
            }
        } else {
            match tokio::time::timeout_at(
                deadline,
                self.pool.acquire_for_group(server, initial_group),
            )
            .await
            {
                Ok(result) => result?,
                Err(_) => return Err(self.acquire_timeout_error()),
            }
        };

        if !conn.needs_group_prologue() {
            return Ok(BodyLaneLease {
                client: self.clone(),
                server_id: server,
                remote_ip: conn.remote_ip(),
                conn: Some(conn),
                groups: groups.to_vec(),
                mode: BodyLaneMode::Sequential,
                latency_ewma: None,
                transfer_ewma: None,
                checkpoint_plan: CheckpointPlan::None,
            });
        }

        match tokio::time::timeout_at(deadline, Self::try_select_group(&mut conn, groups)).await {
            Ok(Ok(_)) => Ok(BodyLaneLease {
                client: self.clone(),
                server_id: server,
                remote_ip: conn.remote_ip(),
                conn: Some(conn),
                groups: groups.to_vec(),
                mode: BodyLaneMode::Sequential,
                latency_ewma: None,
                transfer_ewma: None,
                checkpoint_plan: CheckpointPlan::None,
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
        self.stat_many_in_order(message_ids, order).await
    }

    /// [`Self::stat_many`] over an already chosen, non-empty server order.
    async fn stat_many_in_order(
        &self,
        message_ids: &[&str],
        order: Vec<usize>,
    ) -> Result<Vec<bool>> {
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
    /// reports missing is re-checked with a HEAD before the result is treated
    /// as authoritative — unless no usable server implements HEAD, in which
    /// case the STAT verdict is the final one rather than an excuse to call
    /// the whole batch inconclusive. Transport or probe errors during
    /// confirmation mark the entire batch inconclusive so callers can unwind
    /// without applying projected health damage.
    pub async fn confirm_exists_for_probe(&self, message_ids: &[&str]) -> ProbeBatchResult {
        self.confirm_exists_for_probe_excluding(message_ids, &[])
            .await
            .unwrap_or_else(|| ProbeBatchResult {
                exists: vec![false; message_ids.len()],
                inconclusive: true,
            })
    }

    /// [`Self::confirm_exists_for_probe`] that leaves the servers in
    /// `exclude` out entirely.
    ///
    /// The caller has usually had those servers answer already, on a warm
    /// connection it is holding — an owned download lane's — and asking them
    /// again here would queue behind the very permits those lanes are sitting
    /// on. Returns `None` when nothing usable is left once they are excluded,
    /// which is not a fault: it means the batch has been put to every server
    /// that could answer and the caller's verdict stands.
    pub async fn confirm_exists_for_probe_excluding(
        &self,
        message_ids: &[&str],
        exclude: &[usize],
    ) -> Option<ProbeBatchResult> {
        if message_ids.is_empty() {
            return Some(ProbeBatchResult {
                exists: Vec::new(),
                inconclusive: false,
            });
        }

        let order = self.build_server_order(exclude).await;
        if order.is_empty() {
            return None;
        }

        let mut exists = match self.stat_many_in_order(message_ids, order.clone()).await {
            Ok(results) => results,
            Err(_) => {
                return Some(ProbeBatchResult {
                    exists: vec![false; message_ids.len()],
                    inconclusive: true,
                });
            }
        };

        // What STAT reported missing is re-checked with HEAD before the
        // verdict stands — as one pipelined batch per server, narrowing to
        // what the servers before it could not find, the same walk STAT took.
        // Asking per article through the failover fetch made a verdict on N
        // misses cost N round trips per server, and a missing article is
        // exactly the case where every server has to be asked.
        let mut remaining: Vec<usize> = exists
            .iter()
            .enumerate()
            .filter_map(|(idx, found)| (!found).then_some(idx))
            .collect();
        for idx in order {
            if remaining.is_empty() {
                break;
            }
            let Some(config) = self.pool.server_configs().get(idx) else {
                continue;
            };
            // A server that has refused HEAD is an answer about the command,
            // not a fault: its STAT verdict stands for what it was asked.
            if !crate::server_caps::supports_head(&config.host, config.port) {
                continue;
            }
            let batch: Vec<&str> = remaining.iter().map(|&i| message_ids[i]).collect();
            match self.head_many_from_server(ServerId(idx), &batch).await {
                Ok(results) => {
                    let mut next_remaining = Vec::with_capacity(remaining.len());
                    for (original_idx, found) in remaining.iter().copied().zip(results) {
                        if found {
                            exists[original_idx] = true;
                        } else {
                            next_remaining.push(original_idx);
                        }
                    }
                    remaining = next_remaining;
                }
                // The refusal just happened on this batch; it has been recorded
                // against the server and the next one is asked instead.
                Err(NntpError::CommandNotRecognized) => {}
                Err(_) => {
                    return Some(ProbeBatchResult {
                        exists,
                        inconclusive: true,
                    });
                }
            }
        }

        Some(ProbeBatchResult {
            exists,
            inconclusive: false,
        })
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
                Err(e @ NntpError::QuotaBlocked(_)) => {
                    attempts.push(FetchAttemptTrace {
                        server_idx: idx,
                        remote_ip: None,
                        elapsed: Duration::ZERO,
                        outcome: FetchAttemptOutcome::QuotaBlocked,
                        error: Some(e.to_string()),
                    });
                    last_error = Some(e);
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
                    self.record_transient_server_failure(idx, &e).await;
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
        message_ids: &[&str],
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
        message_ids: &[&str],
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
                    message_ids[0],
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
                .map(|message_idx| message_ids[*message_idx].to_string())
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
                                message_ids[message_idx],
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
                                message_ids[message_idx],
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
                            message_ids[message_idx],
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
            let mut admitted = 0usize;
            let mut quota_rejection = None;
            for (pending_idx, message_id) in pending_ids.iter().enumerate() {
                match conn.write_body_request(message_id).await {
                    Ok(()) => admitted += 1,
                    Err(NntpError::QuotaBlocked(rejection)) => {
                        quota_rejection = Some((pending_idx, rejection));
                        break;
                    }
                    Err(error) => {
                        request_write_error = Some(error);
                        break;
                    }
                }
            }

            if admitted > 0
                && request_write_error.is_none()
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
                            message_ids[message_idx],
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

            if let Some((pending_idx, rejection)) = quota_rejection {
                let message_idx = pending_now[pending_idx];
                let item = DecodedBatchItem {
                    elapsed: Duration::ZERO,
                    result: Err(DecodedBodyError::Nntp(NntpError::QuotaBlocked(rejection))),
                };
                match self
                    .classify_decoded_batch_item(
                        idx,
                        remote_ip,
                        message_ids[message_idx],
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
                pending.extend(pending_now.iter().copied().skip(pending_idx + 1));
            }

            if admitted == 0 {
                continue;
            }

            let mut conn = Some(conn);
            let mut closed_early = false;
            for (response_idx, message_idx) in
                pending_now.iter().copied().take(admitted).enumerate()
            {
                let item_started = Instant::now();
                let mut budget = ActiveTransferBudget::new(self.soft_timeout);
                let result = Self::read_decoded_pipelined_body(
                    conn.as_mut()
                        .expect("connection is available while reading"),
                    &mut budget,
                )
                .await;
                let elapsed = item_started.elapsed();
                let policy_elapsed = result.as_ref().map_or(elapsed, |decoded| {
                    elapsed.saturating_sub(decoded.io.throttle_wait)
                });
                let item = DecodedBatchItem {
                    elapsed: policy_elapsed,
                    result,
                };

                let poisoned = conn.as_ref().is_some_and(|conn| conn.is_poisoned());
                match self
                    .classify_decoded_batch_item(
                        idx,
                        remote_ip,
                        message_ids[message_idx],
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
                    for unread_idx in pending_now
                        .iter()
                        .copied()
                        .take(admitted)
                        .skip(response_idx + 1)
                    {
                        let item = DecodedBatchItem {
                            elapsed: item_started.elapsed(),
                            result: Err(DecodedBodyError::Nntp(NntpError::ConnectionClosed)),
                        };
                        match self
                            .classify_decoded_batch_item(
                                idx,
                                remote_ip,
                                message_ids[unread_idx],
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
            Err(DecodedBodyError::Nntp(NntpError::QuotaBlocked(rejection))) => {
                attempts.push(FetchAttemptTrace {
                    server_idx,
                    remote_ip,
                    elapsed,
                    outcome: FetchAttemptOutcome::QuotaBlocked,
                    error: Some("server download quota blocked".to_string()),
                });
                *last_error = Some(DecodedBodyError::Nntp(NntpError::QuotaBlocked(rejection)));
                DecodedBatchDisposition::Retry
            }
            Err(DecodedBodyError::Nntp(error @ NntpError::BodyNotRequestedDueToQuota { .. })) => {
                attempts.push(FetchAttemptTrace {
                    server_idx,
                    remote_ip,
                    elapsed,
                    outcome: FetchAttemptOutcome::QuotaUnrequested,
                    error: Some("BODY was not issued after quota rejection".to_string()),
                });
                *last_error = Some(DecodedBodyError::Nntp(error));
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
                self.record_transient_server_failure(server_idx, &e).await;
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
                    let elapsed = start.elapsed().saturating_sub(decoded.io.throttle_wait);
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
                Err(DecodedBodyError::Nntp(e @ NntpError::QuotaBlocked(_))) => {
                    attempts.push(FetchAttemptTrace {
                        server_idx: idx,
                        remote_ip: None,
                        elapsed: Duration::ZERO,
                        outcome: FetchAttemptOutcome::QuotaBlocked,
                        error: Some(e.to_string()),
                    });
                    last_error = Some(DecodedBodyError::Nntp(e));
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
                    self.record_transient_server_failure(idx, &e).await;
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
    ) -> std::result::Result<crate::blocking::BlockingBodyLane, BlockingBodyLaneAcquireError> {
        self.try_acquire_blocking_body_lane_with_estimate(groups, exclude, 0)
    }

    /// Inspect the synchronous owned-lane candidates without acquiring a
    /// connection permit or opening a connection.
    ///
    /// `None` means the shared health state was busy, not that no server is
    /// eligible. The two must not be folded together: a caller that reads
    /// contention as "nothing can serve this" takes a permanent decision on a
    /// momentary lock collision.
    pub fn try_blocking_body_server_selection_with_estimate(
        &self,
        exclude: &[usize],
        requested_body_bytes: u64,
    ) -> Option<BodyServerSelection> {
        self.try_blocking_body_server_selection(exclude, requested_body_bytes)
    }

    pub fn try_acquire_blocking_body_lane_with_estimate(
        &self,
        groups: &[String],
        exclude: &[usize],
        requested_body_bytes: u64,
    ) -> std::result::Result<crate::blocking::BlockingBodyLane, BlockingBodyLaneAcquireError> {
        let Some(selection) =
            self.try_blocking_body_server_selection(exclude, requested_body_bytes)
        else {
            return Err(BlockingBodyLaneAcquireError::SelectionContended);
        };
        if selection.eligible.is_empty() {
            return Err(match selection.quota_blocked {
                Some(rejection) => {
                    BlockingBodyLaneAcquireError::Other(NntpError::quota_blocked(rejection))
                }
                None => BlockingBodyLaneAcquireError::NoEligibleServer,
            });
        }

        let mut saw_local_capacity = false;
        let mut provider_capacity_error = None;
        let mut other_error = None;
        for server in selection.eligible {
            let permit = match self.pool.try_acquire_blocking_permit(server) {
                Ok(permit) => permit,
                Err(_) => {
                    saw_local_capacity = true;
                    continue;
                }
            };
            let (config, excluded_ips, address_offset) = self
                .pool
                .blocking_connect_plan(server, &[])
                .map_err(BlockingBodyLaneAcquireError::Other)?;
            if !supports_blocking_body_lane(&config) {
                continue;
            }
            // Blocking lanes always open a fresh socket, so a held-off server
            // can only answer with the holdoff error. Skipping it here keeps
            // surplus lanes parked instead of re-attempting every dispatch.
            // Asked with the permit in hand so that the one caller admitted
            // as the post-holdoff probe is a caller that will actually dial.
            let admission = match self.pool.admit_fresh_connect(server) {
                Ok(admission) => admission,
                Err(error) => {
                    provider_capacity_error = Some(error);
                    continue;
                }
            };
            let started = Instant::now();
            match crate::blocking::BlockingBodyLane::connect(
                server,
                self.pool.stable_server_id(server).unwrap_or_default(),
                self.pool.server_transfer_control(server),
                &config,
                &excluded_ips,
                address_offset,
                groups,
                self.soft_timeout,
                permit,
            ) {
                Ok(lane) => {
                    if admission == FreshConnectAdmission::Probe {
                        self.pool.note_provider_admitted(server);
                    }
                    return Ok(lane);
                }
                Err(error) => {
                    self.record_blocking_connect_failure(server.0, admission, &error);
                    // Debug for every failure, and one WARN per server per
                    // window carrying how many it stands for. A lane that
                    // cannot connect is invisible otherwise: the work is
                    // requeued or handed to an async lane, so the download
                    // keeps running — slowly, on a fraction of its lanes, with
                    // nothing above debug to say why.
                    debug!(
                        server = server.0,
                        error = %error,
                        elapsed_ms = started.elapsed().as_millis(),
                        "blocking BODY lane connect failed"
                    );
                    if let Some(suppressed) = self.pool.note_blocking_connect_warning(server) {
                        warn!(
                            server = server.0,
                            address = %self.pool.server_address(server),
                            error = %error,
                            elapsed_ms = started.elapsed().as_millis(),
                            failures_since_last_warning = suppressed,
                            "blocking BODY lane connect failed"
                        );
                    }
                    match BlockingBodyLaneAcquireError::from_connect_error(error) {
                        BlockingBodyLaneAcquireError::ProviderCapacity(error) => {
                            provider_capacity_error = Some(error);
                        }
                        BlockingBodyLaneAcquireError::Other(error) => {
                            other_error = Some(error);
                        }
                        BlockingBodyLaneAcquireError::LocalCapacity
                        | BlockingBodyLaneAcquireError::NoEligibleServer
                        | BlockingBodyLaneAcquireError::SelectionContended => unreachable!(),
                    }
                }
            }
        }
        if let Some(error) = provider_capacity_error {
            Err(BlockingBodyLaneAcquireError::ProviderCapacity(error))
        } else if let Some(error) = other_error {
            Err(BlockingBodyLaneAcquireError::Other(error))
        } else if saw_local_capacity {
            Err(BlockingBodyLaneAcquireError::LocalCapacity)
        } else {
            Err(BlockingBodyLaneAcquireError::NoEligibleServer)
        }
    }

    /// Whether the owned blocking lanes could take this work, and whether the
    /// answer is one at all.
    ///
    /// [`BlockingBodyLaneCandidacy::Contended`] is the reason this is not a
    /// `bool`: the shared health state was busy for the length of the spin, so
    /// nothing is known about the servers. Treating that as "no candidate"
    /// sends the work to the asynchronous lanes, which then queue for the very
    /// connection permits the idle owned lanes are holding and wait out the
    /// whole acquire deadline for it.
    pub fn blocking_body_lane_candidacy(&self, exclude: &[usize]) -> BlockingBodyLaneCandidacy {
        let Some(selection) = self.try_blocking_body_server_selection(exclude, 0) else {
            return BlockingBodyLaneCandidacy::Contended;
        };
        let has_candidate = selection.eligible.into_iter().any(|server| {
            // Deliberately blind to the holdoff: an owned lane that is
            // already connected must keep receiving work while its server
            // refuses new sockets. A worker with no cached lane finds the
            // holdoff at acquire time and parks there instead.
            if self.pool.server_load(server.0).1 == 0 {
                return false;
            }
            let Ok((config, _, _)) = self.pool.blocking_connect_plan(server, &[]) else {
                return false;
            };
            supports_blocking_body_lane(&config)
        });
        if has_candidate {
            BlockingBodyLaneCandidacy::Candidate
        } else {
            BlockingBodyLaneCandidacy::None
        }
    }

    pub fn has_blocking_body_lane_candidate(&self, exclude: &[usize]) -> bool {
        matches!(
            self.blocking_body_lane_candidacy(exclude),
            BlockingBodyLaneCandidacy::Candidate
        )
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
                        .record_failure(attempt.server_idx, false);
                }
                FetchAttemptOutcome::NotFound
                | FetchAttemptOutcome::QuotaBlocked
                | FetchAttemptOutcome::QuotaUnrequested
                | FetchAttemptOutcome::PermanentFailure => {}
            }
        }
    }

    /// Ranked owned-lane candidates, or `None` when the health mutex was held
    /// by someone else for the whole (short) spin.
    ///
    /// The distinction matters: an empty selection means "no server can serve
    /// this request", while contention means "ask again in a moment". Folding
    /// the two together made every contended selection look like a permanent
    /// tiering verdict and silently pushed the work off the owned fast path.
    fn try_blocking_body_server_selection(
        &self,
        exclude: &[usize],
        requested_body_bytes: u64,
    ) -> Option<BodyServerSelection> {
        let server_count = self.pool.server_count();
        let server_groups = self.pool.server_groups();
        let backfill_flags = self.pool.server_backfill_flags();
        // This runs on a blocking worker thread, so the tokio mutex cannot be
        // awaited. The critical sections behind it are microseconds long, so a
        // few yielding retries turn nearly every collision into a hit.
        let mut health = None;
        for attempt in 0..BLOCKING_HEALTH_LOCK_SPINS {
            if let Ok(guard) = self.pool.health().try_lock() {
                health = Some(guard);
                break;
            }
            if attempt + 1 < BLOCKING_HEALTH_LOCK_SPINS {
                std::thread::yield_now();
            }
        }
        let mut health = health?;
        health.check_reenable_all();
        let backfill_unlocked = self
            .pool
            .fill_servers_exhausted_or_auth_disabled(exclude, &health);

        let mut candidates = Vec::with_capacity(server_count);
        let mut quota_blocked = None;
        for (idx, group) in server_groups.iter().copied().enumerate().take(server_count) {
            if exclude.contains(&idx) || !health.is_available(idx) {
                continue;
            }
            // Backfill servers only serve requests whose fill tier is
            // exhausted or auth-disabled (see build_server_order).
            if backfill_flags[idx] && !backfill_unlocked {
                continue;
            }
            if let Some(rejection) = self
                .pool
                .server_transfer_control(ServerId(idx))
                .and_then(|control| control.quota_rejection_for_dispatch(requested_body_bytes))
            {
                retain_earliest_quota_rejection(&mut quota_blocked, rejection);
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
                (backfill_flags[idx], group, health_rank, score, idx)
            };
            if candidate.2 < 2 {
                candidates.push(candidate);
            }
        }
        candidates.sort_by(|a, b| {
            a.0.cmp(&b.0)
                .then(a.1.cmp(&b.1))
                .then(a.2.cmp(&b.2))
                .then_with(|| a.3.partial_cmp(&b.3).unwrap_or(std::cmp::Ordering::Equal))
        });
        let eligible = candidates
            .into_iter()
            .map(|(_, _, _, _, idx)| ServerId(idx))
            .collect();
        Some(BodyServerSelection {
            eligible,
            quota_blocked,
        })
    }

    fn record_blocking_connect_failure(
        &self,
        server_idx: usize,
        admission: FreshConnectAdmission,
        error: &NntpError,
    ) {
        if matches!(error, NntpError::TooManyConnections) {
            // Provider admission pressure parks new sockets, it does not make
            // the server unhealthy. Cooling the whole server here would also
            // block already-established healthy lanes from refilling.
            self.pool.note_provider_over_limit(ServerId(server_idx));
            return;
        }
        // Any other failure says nothing about the provider's limit; if this
        // connect was the post-holdoff probe, let the next caller ask.
        if admission == FreshConnectAdmission::Probe {
            self.pool.release_over_limit_probe(ServerId(server_idx));
        }
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

    async fn record_transient_server_failure(&self, server_idx: usize, error: &NntpError) {
        if !matches!(
            error,
            NntpError::TooManyConnections
                | NntpError::ServerOverLimit { .. }
                | NntpError::PoolExhausted
                | NntpError::PoolShutdown
                // Learning session setup is not a server health failure.
                | NntpError::NoGroupSelected
                // Local capacity: we never reached the server, so this must
                // not walk it toward Degraded/Disabled.
                | NntpError::AcquireTimeout(_)
        ) {
            self.record_server_failure(server_idx, false).await;
        }
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

    /// Drop the connection that just failed, and only that one.
    ///
    /// One socket's fault says nothing about the server's other sockets: they
    /// were opened at different times, may run over different addresses, and
    /// are the warm capacity the next request is about to reuse. Throwing them
    /// away turned a single refused command into a fleet-wide cold dial.
    async fn discard_connection_error(&self, server_idx: usize, conn: PooledConnection) {
        let age = conn.created_at().elapsed();
        conn.discard();
        self.record_premature_death_if_needed(server_idx, age).await;
    }

    /// Whether `server` could hand out a normal lease right now without
    /// waiting on its connection semaphore.
    pub fn has_available_permit(&self, server: ServerId) -> bool {
        self.pool.has_available_permit(server)
    }

    async fn acquire_before_deadline(
        &self,
        server: ServerId,
        deadline: TokioInstant,
    ) -> Result<PooledConnection> {
        match tokio::time::timeout_at(deadline, self.pool.acquire(server)).await {
            Ok(result) => result,
            // Nothing was sent: we never got a socket. This is our own
            // capacity, not the server's health.
            Err(_) => Err(self.acquire_timeout_error()),
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

    /// The acquire-side twin of [`Self::soft_timeout_error`], for deadlines
    /// that expire while we are still queued behind our own connection
    /// semaphore.
    fn acquire_timeout_error(&self) -> NntpError {
        NntpError::AcquireTimeout(self.soft_timeout.as_secs())
    }

    /// Build the server try-order, exhausting lower-priority fill groups
    /// before higher-priority ones, with backfill servers reachable only
    /// once every fill server is excluded for this request.
    ///
    /// Servers in `exclude` are skipped entirely. Disabled servers and
    /// short-lived cooldown servers are excluded so we don't waste time on
    /// servers that are known to be failing right now.
    ///
    /// A `CoolingDown` fill tier does NOT unlock backfill: those are 5–10 s
    /// blips, so the order is temporarily empty and callers wait rather than
    /// spilling ordinary work onto backfill accounts. Neither does an outage
    /// disable (`ConsecutiveFailures` / `FailureRatio`): it heals on its own,
    /// and spilling the whole queue onto a paid backfill account for a 30 s
    /// primary blip is the wrong trade. An `AuthFailure` disable does unlock
    /// it — a fill server that keeps failing authentication never produces
    /// the 430 that would put it in `exclude`, so without this an article the
    /// other fill servers do not have would be pinned out of backfill until
    /// the operator fixes the credentials. Within a priority group, immediately acquirable
    /// servers are preferred over fully saturated peers.
    #[allow(clippy::needless_range_loop)]
    async fn build_server_order(&self, exclude: &[usize]) -> Vec<usize> {
        let server_count = self.pool.server_count();
        let server_groups = self.pool.server_groups();
        let backfill_flags = self.pool.server_backfill_flags();

        // Check health to skip disabled servers.
        let mut health = self.pool.health().lock().await;
        health.check_reenable_all();
        let backfill_unlocked = self
            .pool
            .fill_servers_exhausted_or_auth_disabled(exclude, &health);

        #[derive(Default)]
        struct GroupCandidates {
            ready_healthy: Vec<usize>,
            ready_degraded: Vec<usize>,
            waiting_healthy: Vec<usize>,
            waiting_degraded: Vec<usize>,
        }

        let mut fill_groups: std::collections::BTreeMap<u32, GroupCandidates> =
            std::collections::BTreeMap::new();
        let mut backfill_groups: std::collections::BTreeMap<u32, GroupCandidates> =
            std::collections::BTreeMap::new();
        for idx in 0..server_count {
            if exclude.contains(&idx) || !health.is_available(idx) {
                continue;
            }
            if backfill_flags[idx] && !backfill_unlocked {
                continue;
            }
            let tier = if backfill_flags[idx] {
                &mut backfill_groups
            } else {
                &mut fill_groups
            };
            let entry = tier.entry(server_groups[idx]).or_default();
            // A held-off server can still hand out idle connections, so it
            // stays eligible — but it ranks behind servers that can also open
            // new ones.
            let ready = self.pool.server_load(idx).0 > 0 && !self.pool.is_over_limit(ServerId(idx));
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
        drop(health);

        let mut result = Vec::with_capacity(server_count);
        for candidates in fill_groups
            .into_values()
            .chain(backfill_groups.into_values())
        {
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
                Err(e @ NntpError::QuotaBlocked(_)) => {
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
                    if matches!(
                        e,
                        NntpError::TooManyConnections | NntpError::ServerOverLimit { .. }
                    ) {
                        // The holdoff already reported itself once for this
                        // window; every later article that lands on the same
                        // server must stay quiet.
                        trace!(
                            server = idx,
                            message_id, "provider capacity rejected connection"
                        );
                    } else {
                        warn!(
                            server = idx,
                            error = %e,
                            message_id,
                            "transient error, trying next server"
                        );
                    }
                    self.record_transient_server_failure(idx, &e).await;
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
    /// Connection acquisition, group setup, and retry delay are bounded by a
    /// per-attempt soft timeout. BODY payload reads use an active-time budget
    /// that excludes deliberate transfer-policy waits.
    async fn fetch_from_server_with_groups(
        &self,
        server: ServerId,
        message_id: &str,
        groups: &[String],
    ) -> Result<(Bytes, Option<IpAddr>)> {
        let mut attempts = 0u32;

        loop {
            let deadline = TokioInstant::now() + self.soft_timeout;
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

            let mut budget = ActiveTransferBudget::new(self.soft_timeout);
            let result = conn
                .body_by_id_raw_with_active_budget(message_id, &mut budget)
                .await;
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
        let mut attempts = 0u32;

        loop {
            let deadline = TokioInstant::now() + self.soft_timeout;
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

            let mut budget = ActiveTransferBudget::new(self.soft_timeout);
            let stream_result = conn
                .stream_yenc_article_with_active_budget(message_id, 0, &mut budget, |_| Ok(()))
                .await;

            match stream_result {
                Ok(article) => {
                    return Ok(DecodedBody {
                        raw_size: decoded_raw_size_from_fused_stats(&article.stats),
                        cpu: decoded_cpu_from_fused_stats(&article.stats),
                        io: decoded_io_from_fused_stats(&article.stats),
                        decoded: article.chunks,
                        body: article.body,
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
        budget: &mut ActiveTransferBudget,
    ) -> std::result::Result<DecodedBody, DecodedBodyError> {
        let stream_result = conn
            .stream_next_yenc_article_with_active_budget(budget, |_| Ok(()))
            .await;

        match stream_result {
            Ok(article) => Ok(DecodedBody {
                raw_size: decoded_raw_size_from_fused_stats(&article.stats),
                cpu: decoded_cpu_from_fused_stats(&article.stats),
                io: decoded_io_from_fused_stats(&article.stats),
                decoded: article.chunks,
                body: article.body,
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
            NntpError::AcquireTimeout(duration) => {
                let duration = *duration;
                Box::new(move || NntpError::AcquireTimeout(duration))
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
    /// Whether `server` still answers STAT, as far as this process has been
    /// able to tell.
    fn server_supports_stat(&self, server: ServerId) -> bool {
        self.pool
            .server_configs()
            .get(server.0)
            .is_none_or(|config| crate::server_caps::supports_stat(&config.host, config.port))
    }

    /// Existence by HEAD, for a server that has refused STAT outright and for
    /// re-checking what STAT reported missing.
    ///
    /// One connection serves the whole batch. On a server that pipelines it is
    /// one write and one round trip, exactly as a STAT batch is; otherwise a
    /// round trip per article, but still no extra dials. Transient faults are
    /// retried on the same server the way a STAT batch is, so a re-check is
    /// not turned inconclusive by one dropped socket.
    async fn head_many_from_server(
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
                    conn.head_pipeline(message_ids).await
                } else {
                    let mut results = Vec::with_capacity(message_ids.len());
                    for message_id in message_ids {
                        match conn.head_by_id(message_id).await {
                            Ok(_) => results.push(true),
                            Err(
                                NntpError::ArticleNotFound
                                | NntpError::NoSuchArticle { .. }
                                | NntpError::NoArticleWithNumber,
                            ) => results.push(false),
                            Err(error) => return Err(error),
                        }
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
                Err(error) if is_retryable_stat_error(&error) => {
                    if should_discard_stat_connection(&error) {
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
                            error = %error,
                            batch_size = message_ids.len(),
                            "retryable error during HEAD batch, retrying on same server"
                        );
                        continue;
                    }
                    return Err(error);
                }
                Err(error) => {
                    if is_connection_error(&error) {
                        self.discard_connection_error(server.0, conn).await;
                    }
                    return Err(error);
                }
            }
        }
    }

    async fn stat_many_from_server(
        &self,
        server: ServerId,
        message_ids: &[&str],
    ) -> Result<Vec<bool>> {
        if message_ids.is_empty() {
            return Ok(Vec::new());
        }

        // A server that has already refused STAT is asked with HEAD directly,
        // instead of spending a refusal per batch to relearn it.
        if !self.server_supports_stat(server) {
            return self.head_many_from_server(server, message_ids).await;
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
                // The server has just told us it does not implement STAT. The
                // socket is fine — the refusal is an answer — so it goes back
                // to the pool and the batch is re-asked with HEAD.
                Err(NntpError::CommandNotRecognized) if !self.server_supports_stat(server) => {
                    drop(conn);
                    return self.head_many_from_server(server, message_ids).await;
                }
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
    /// Connection acquisition and retry delay are bounded by a per-attempt
    /// soft timeout. BODY payload reads use an active-time budget that excludes
    /// deliberate transfer-policy waits.
    async fn fetch_from_server(
        &self,
        server: ServerId,
        message_id: &str,
        kind: FetchKind,
    ) -> Result<Bytes> {
        let mut attempts = 0u32;

        loop {
            let deadline = TokioInstant::now() + self.soft_timeout;
            let mut conn = self.acquire_before_deadline(server, deadline).await?;

            let result = match kind {
                FetchKind::Body => {
                    let mut budget = ActiveTransferBudget::new(self.soft_timeout);
                    conn.body_by_id_raw_with_active_budget(message_id, &mut budget)
                        .await
                }
                FetchKind::Head | FetchKind::Article => {
                    match tokio::time::timeout_at(deadline, async {
                        match kind {
                            FetchKind::Head => conn.head_by_id(message_id).await,
                            FetchKind::Article => conn.article_by_id(message_id).await,
                            FetchKind::Body => unreachable!(),
                        }
                    })
                    .await
                    {
                        Ok(result) => result,
                        Err(_) => {
                            self.discard_connection_error(server.0, conn).await;
                            return Err(self.soft_timeout_error());
                        }
                    }
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

/// Whether a decoded BODY outcome left the connection fully consumed and
/// reusable for the rest of the pipelined batch.
///
/// See the blocking lane's twin of this helper: a 430 is a complete server
/// response with no body to drain, so it must not mark a batch dirty and
/// block the server's pipelining proof. Decode failures stay dirty because
/// the decoder can fail on a body the transport never finished delivering.
fn decoded_result_keeps_connection(
    result: &std::result::Result<DecodedBody, DecodedBodyError>,
) -> bool {
    match result {
        Ok(_) => true,
        Err(DecodedBodyError::Nntp(error)) => error.is_article_not_found(),
        Err(DecodedBodyError::Decode { .. }) => false,
    }
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
            // The next connection can select the group learned from this 412.
            | NntpError::NoGroupSelected
            | NntpError::ServiceUnavailable
            | NntpError::TooManyConnections
            | NntpError::ServerOverLimit { .. }
            | NntpError::PoolExhausted
            | NntpError::SoftTimeout(_)
            | NntpError::AcquireTimeout(_)
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
        NntpError::TooManyConnections => None,
        // The holdoff already answers this locally; nothing reached the server.
        NntpError::ServerOverLimit { .. } => None,
        NntpError::PoolExhausted => None,
        // Never obtaining a connection is our own capacity, not the server's
        // health: nothing was sent, so the server said nothing wrong. Cooling
        // it here strands every download behind a 10 s transient cooldown when
        // only one server is configured.
        NntpError::AcquireTimeout(_) => None,
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
mod tests;
