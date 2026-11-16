// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use std::net::IpAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use rand::distr::weighted::WeightedIndex;
use rand::prelude::*;
use tokio::time::Instant as TokioInstant;
use tracing::{debug, trace, warn};
use weaver_yenc::YencError;

use crate::ServerSet;

use crate::connection::ServerConfig;
use crate::error::{NntpError, Result};
use crate::fused_yenc::{FusedArticleBody, FusedYencArticleStats, FusedYencError};
use crate::health::{CooldownReason, ServerState};
use crate::pool::{
    BodyServerAvailability, FreshConnectAdmission, NntpPool, PoolConfig, PooledConnection,
    ServerId, ServerPoolConfig,
};
use crate::tls::TransportReadStats;
use crate::transfer::{ActiveTransferBudget, QuotaRejection};

// Configuration for the high-level NNTP client.
pub struct NntpClientConfig {
    // Server configurations in priority order (primary first).
    pub servers: Vec<ServerPoolConfig>,
    // Maximum age for idle connections before eviction.
    pub max_idle_age: Duration,
    // Maximum number of retries on the same server for transient errors
    // before failing over to the next server. A value of 1 means try once,
    // then retry once (2 total attempts per server).
    pub max_retries_per_server: u32,
    // Per-article inactivity budget: how long a BODY may go without bytes
    // arriving. Deliberate local rate-limit waits do not count; exhausting
    // the budget triggers failover to the next server.
    // This is separate from the connection-level `command_timeout`.
    pub soft_timeout: Duration,
}

impl NntpClientConfig {
    // Create a client config with a single server.
    pub fn single(server: ServerConfig, max_connections: usize) -> Self {
        NntpClientConfig {
            servers: vec![ServerPoolConfig {
                max_connections,
                ..ServerPoolConfig::new(server)
            }],
            max_idle_age: Duration::from_secs(300),
            max_retries_per_server: 1,
            soft_timeout: Duration::from_secs(15),
        }
    }
}

// High-level NNTP client with multi-server failover.
//
// Provides a simple API for fetching articles, automatically managing
// connection pooling and falling back to backup servers when an article
// is not found on the primary server.
#[derive(Clone)]
pub struct NntpClient {
    pool: Arc<NntpPool>,
    work_guard: Option<Arc<WorkGuard>>,
    max_retries_per_server: u32,
    // Per-article inactivity budget, excluding deliberate local rate waits.
    soft_timeout: Duration,
}

// Whether work bound to a server is still to be done there, judged by the
// view of the provider policy it is handed. The guard does not pick the view:
// before a connection is admitted it is handed the pool's
// ([`NntpPool::provider_policy`]); once the work has a connection, that
// connection's ([`NntpPool::provider_policy_on_connection`]), in which the
// recovery probe a quarantine admitted is past its own server's hold. Read
// otherwise, the probe would find its own work obsolete and be put down
// unanswered, holding the server it was admitted to recover.
pub type WorkGuard =
    dyn Fn(ServerId, &crate::provider_policy::ProviderPolicy) -> bool + Send + Sync;

// How many yielding `try_lock` attempts an owned-lane server selection makes
// on the shared health mutex before reporting contention. The critical
// sections behind that mutex are microseconds long, so a handful of retries
// absorbs ordinary collisions without ever blocking a worker thread.
const BLOCKING_HEALTH_LOCK_SPINS: usize = 4;

// Why a synchronous owned BODY lane could not be acquired.
//
// Capacity outcomes are separated from server availability and transport
// failures so callers can return leased work to the scheduler without
// tearing down healthy cached lanes or consuming a download retry.
#[derive(Debug)]
pub enum BlockingBodyLaneAcquireError {
    ProviderCapacity(NntpError),
    LocalCapacity,
    // Every candidate server is recovering and its one probe connection is
    // already out. Self-clearing like local capacity, but not the same
    // thing: the permits are free and the sockets are idle, and reporting it
    // as saturation points the reader at a leak that is not there.
    ServerRecovering,
    // The server refuses this pool's credentials, either on this dial or by
    // the disable an earlier refusal left (`NntpPool::refuses_credentials`).
    // A verdict, not a wait: the leased articles take the failure, so the
    // server leaves their candidates instead of holding them in the queue.
    CredentialsRefused { server: ServerId, error: NntpError },
    NoEligibleServer,
    // The health mutex was held elsewhere, so candidates could not be ranked.
    // This says nothing about server availability — the caller should hand
    // the work back to the scheduler and try again, not conclude that no
    // server can serve it.
    SelectionContended,
    Other(NntpError),
}

impl BlockingBodyLaneAcquireError {
    fn from_connect_error(server: ServerId, error: NntpError) -> Self {
        if matches!(
            error,
            NntpError::TooManyConnections | NntpError::ServerOverLimit { .. }
        ) {
            Self::ProviderCapacity(error)
        } else if error.is_credentials_refusal() {
            Self::CredentialsRefused { server, error }
        } else {
            Self::Other(error)
        }
    }

    // The server whose answer this is, where the refusal is one server's.
    pub fn server(&self) -> Option<ServerId> {
        match self {
            Self::CredentialsRefused { server, .. } => Some(*server),
            _ => None,
        }
    }

    pub fn is_capacity_admission(&self) -> bool {
        matches!(
            self,
            Self::ProviderCapacity(_) | Self::LocalCapacity | Self::ServerRecovering
        )
    }

    // The transport error behind this refusal, where there is one. Local
    // capacity, selection contention and an empty candidate list are the
    // lane's own bookkeeping and carry no server answer to report.
    pub fn nntp_error(&self) -> Option<&NntpError> {
        match self {
            Self::ProviderCapacity(error)
            | Self::Other(error)
            | Self::CredentialsRefused { error, .. } => Some(error),
            Self::LocalCapacity
            | Self::ServerRecovering
            | Self::NoEligibleServer
            | Self::SelectionContended => None,
        }
    }

    // Whether the leased work should be handed straight back to the scheduler
    // and retried on the owned fast path, rather than falling back to an
    // async lane. Capacity admission and selection contention are both
    // "ask again shortly", not verdicts about the servers.
    pub fn should_requeue_owned_work(&self) -> bool {
        self.is_capacity_admission() || matches!(self, Self::SelectionContended)
    }

    // A short, stable label for metrics and logs. Distinguishing the kinds is
    // the whole point of the counters: local capacity and selection contention
    // are self-clearing, provider capacity is the provider refusing sockets,
    // and `other` is a transport failure that deserves attention.
    pub fn kind(&self) -> &'static str {
        match self {
            Self::ProviderCapacity(_) => "provider_capacity",
            Self::LocalCapacity => "local_capacity",
            Self::ServerRecovering => "server_recovering",
            Self::CredentialsRefused { .. } => "credentials_refused",
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
            Self::ServerRecovering => formatter
                .write_str("server is recovering; the recovery probe connection is already out"),
            Self::CredentialsRefused { server, error } => {
                write!(
                    formatter,
                    "server {} refuses credentials: {error}",
                    server.0
                )
            }
            Self::NoEligibleServer => formatter.write_str("no eligible blocking BODY server"),
            Self::SelectionContended => {
                formatter.write_str("blocking BODY server selection contended on server health")
            }
        }
    }
}

impl std::error::Error for BlockingBodyLaneAcquireError {}

// How a server selection asks each server's quota.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum QuotaCheck {
    // A dispatch choosing its server: a server skipped for headroom has
    // turned the work away, and its blocked signal says so.
    Dispatch,
    // A question about the servers: reads the quota, changes nothing.
    ReadOnly,
}

// Whether the synchronous owned lanes can be given a batch.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum BlockingBodyLaneCandidacy {
    // At least one eligible server can carry an owned blocking lane.
    Candidate,
    // No eligible server can. This is the only answer that justifies the
    // asynchronous path.
    None,
    // The shared server health state was busy, so the candidates could not be
    // ranked. Says nothing about the servers: ask again in a moment, on the
    // owned lanes.
    Contended,
}

// A BODY fetch that was streamed and decoded inline.
#[derive(Debug)]
pub struct DecodedBody {
    pub raw_size: u32,
    pub decoded: Vec<Box<[u8]>>,
    // What the body decoded to. yEnc and uuencode carry different evidence, so
    // this is a sum type rather than a yEnc result with absent fields.
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

// Errors from the streamed BODY decode path.
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

// Existence results used by the health probe pipeline.
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
    GroupSelectionRequired,
    TransientFailure,
    PermanentFailure,
}

impl FetchAttemptOutcome {
    pub(crate) fn transient(error: &NntpError) -> Self {
        match error {
            NntpError::NoGroupSelected => Self::GroupSelectionRequired,
            _ => Self::TransientFailure,
        }
    }
}

pub type RouteFeedback = Option<Arc<weaver_tunnel::bridge::ConnectionOutcome>>;

#[derive(Debug, Clone)]
pub struct FetchAttemptTrace {
    pub route_feedback: RouteFeedback,
    pub connection_health: Option<Arc<crate::recovery::ConnectionHealth>>,
    pub server_idx: usize,
    pub remote_ip: Option<IpAddr>,
    pub elapsed: Duration,
    pub outcome: FetchAttemptOutcome,
    pub error: Option<String>,
}

#[derive(Debug)]
pub struct DecodedBodyTrace {
    pub attempts: Vec<FetchAttemptTrace>,
    pub result: std::result::Result<DecodedBody, DecodedBodyError>,
}

// BODY servers that can be leased now, plus the most useful quota block when
// quota filtering removed an otherwise eligible server.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BodyServerSelection {
    pub eligible: Vec<ServerId>,
    // Prefers the earliest scheduled retry; manual-only blocks are retained
    // when no scheduled retry is available.
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

// Whether an owned blocking BODY lane can serve this server.
//
// It always can. Owned lanes are the only download engine, and the blocking
// transport carries every arrangement a server config can describe:
// plaintext, implicit TLS, and the in-band STARTTLS upgrade. A TLS backend
// that cannot build trust for a given config — the s2n lane without a pinned
// CA PEM — yields to the rustls lane rather than leaving the server unserved.
//
// The predicate is kept because its callers read as a question about one
// server, and because a transport that genuinely could not be lane-served
// would have to answer here.
fn supports_blocking_body_lane(_config: &ServerConfig) -> bool {
    true
}

#[derive(Debug, Clone, Copy)]
pub struct BodyLaneTraceMeta {
    pub batch_complete: bool,
    pub batch_clean: bool,
    pub batch_response_count: u64,
    pub unresolved_count: u64,
    pub connection_discarded: bool,
    // The server answered a request on this connection while a later one
    // was already on the wire, since the connection last had nothing
    // outstanding. A fault without it hit a connection that never showed it
    // takes pipelined requests: an idle close, a restart, an outage.
    pub answered_pipelined: bool,
}

impl NntpClient {
    // Create a new NNTP client from the given configuration.
    pub fn new(config: NntpClientConfig) -> Self {
        let max_retries_per_server = config.max_retries_per_server;
        let soft_timeout = config.soft_timeout;
        let pool = NntpPool::new(PoolConfig {
            servers: config.servers,
            max_idle_age: config.max_idle_age,
            ..PoolConfig::default()
        });
        Self::around(pool, max_retries_per_server, soft_timeout)
    }

    // A client for settings: refuses, in words, a server list past
    // `MAX_PROVIDERS` instead of building a pool it cannot index.
    pub fn try_new(
        config: NntpClientConfig,
    ) -> std::result::Result<Self, crate::pool::PoolTooLarge> {
        let max_retries_per_server = config.max_retries_per_server;
        let soft_timeout = config.soft_timeout;
        let pool = NntpPool::try_new(PoolConfig {
            servers: config.servers,
            max_idle_age: config.max_idle_age,
            ..PoolConfig::default()
        })?;
        Ok(Self::around(pool, max_retries_per_server, soft_timeout))
    }

    fn around(pool: NntpPool, max_retries_per_server: u32, soft_timeout: Duration) -> Self {
        let pool = Arc::new(pool);
        pool.start_route_probes();
        NntpClient {
            pool,
            work_guard: None,
            max_retries_per_server,
            soft_timeout,
        }
    }

    // Create a client wrapping an existing pool.
    pub fn from_pool(pool: Arc<NntpPool>) -> Self {
        pool.start_route_probes();
        NntpClient {
            pool,
            work_guard: None,
            max_retries_per_server: 1,
            soft_timeout: Duration::from_secs(15),
        }
    }

    pub fn with_work_guard(
        &self,
        guard: impl Fn(ServerId, &crate::provider_policy::ProviderPolicy) -> bool
        + Send
        + Sync
        + 'static,
    ) -> Self {
        let mut client = self.clone();
        let previous = client.work_guard.take();
        client.work_guard = Some(Arc::new(move |server, policy| {
            previous
                .as_ref()
                .is_none_or(|previous| previous(server, policy))
                && guard(server, policy)
        }));
        client
    }

    pub async fn body_server_order(&self, exclude: ServerSet) -> Vec<ServerId> {
        self.body_server_selection(exclude).await.eligible
    }

    pub async fn body_server_availability(
        &self,
        failure_excludes: ServerSet,
        retention_excludes: ServerSet,
        requested_body_bytes: u64,
    ) -> BodyServerAvailability {
        self.pool
            .body_server_availability(failure_excludes, retention_excludes, requested_body_bytes)
            .await
    }

    pub async fn body_server_selection(&self, exclude: ServerSet) -> BodyServerSelection {
        self.body_server_selection_with_estimate(exclude, 0).await
    }

    pub async fn body_server_selection_with_estimate(
        &self,
        exclude: ServerSet,
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

    // Return the current request-specific quota rejection for one server.
    // Cached owned lanes use this to revalidate admission without touching
    // connection health or acquiring a new lane. It is a dispatch decision,
    // so a rejection latches the server's blocked signal.
    pub fn server_quota_rejection(
        &self,
        server: ServerId,
        requested_body_bytes: u64,
    ) -> Option<QuotaRejection> {
        self.pool
            .server_transfer_control(server)
            .and_then(|control| control.quota_rejection_for_dispatch(requested_body_bytes))
    }

    // True only when the active pool contains at least one normal fill server
    // and every such fill is at its absolute quota limit. Backfill servers do
    // not participate in this global presentation predicate.
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
            if !self.pool.server_quota_blocked(ServerId(idx)) {
                return false;
            }
        }
        saw_fill
    }

    // Fetch the body of an article by message-id, with multi-server failover.
    //
    // Tries each server in priority order. Falls back to the next server
    // on `ArticleNotFound` (430). Retries on the same server for transient
    // errors before escalating.
    pub async fn fetch_body(&self, message_id: &str) -> Result<Bytes> {
        self.fetch_with_failover(message_id).await
    }

    // Fetch and decode a yEnc BODY by message-id using streamed NNTP chunks.
    pub async fn fetch_body_decoded_with_groups(
        &self,
        message_id: &str,
        groups: &[String],
    ) -> std::result::Result<DecodedBody, DecodedBodyError> {
        self.fetch_body_decoded_with_groups_excluding(message_id, groups, ServerSet::EMPTY)
            .await
    }

    // Like [`fetch_body_decoded_with_groups`](Self::fetch_body_decoded_with_groups)
    // but skips the specified servers.
    pub async fn fetch_body_decoded_with_groups_excluding(
        &self,
        message_id: &str,
        groups: &[String],
        exclude: ServerSet,
    ) -> std::result::Result<DecodedBody, DecodedBodyError> {
        self.fetch_body_decoded_with_groups_excluding_traced(message_id, groups, exclude)
            .await
            .result
    }

    pub async fn fetch_body_decoded_with_groups_excluding_traced(
        &self,
        message_id: &str,
        groups: &[String],
        exclude: ServerSet,
    ) -> DecodedBodyTrace {
        self.fetch_body_decoded_with_groups_ordered_traced(
            message_id,
            groups,
            exclude,
            ServerSet::EMPTY,
        )
        .await
    }

    async fn fetch_body_decoded_with_groups_ordered_traced(
        &self,
        message_id: &str,
        groups: &[String],
        exclude: ServerSet,
        avoid: ServerSet,
    ) -> DecodedBodyTrace {
        let order = self.article_server_order(exclude, avoid).await;

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
            if !self.article_attempt_eligible(idx, exclude, &attempts).await {
                if let Some(rejection) = self
                    .pool
                    .server_transfer_control(ServerId(idx))
                    .and_then(|control| control.quota_rejection_for(1))
                {
                    attempts.push(FetchAttemptTrace {
                        route_feedback: None,
                        connection_health: None,
                        server_idx: idx,
                        remote_ip: None,
                        elapsed: Duration::ZERO,
                        outcome: FetchAttemptOutcome::QuotaBlocked,
                        error: Some("server download quota blocked".into()),
                    });
                    last_error = Some(DecodedBodyError::Nntp(NntpError::quota_blocked(rejection)));
                }
                continue;
            }
            let server = ServerId(idx);
            let start = Instant::now();

            match self
                .fetch_decoded_from_server_with_groups(server, message_id, groups)
                .await
            {
                Ok((decoded, remote_ip, route_feedback)) => {
                    let elapsed = start.elapsed().saturating_sub(decoded.io.throttle_wait);
                    self.record_server_success(idx, elapsed).await;
                    attempts.push(FetchAttemptTrace {
                        route_feedback,
                        connection_health: None,
                        server_idx: idx,
                        remote_ip,
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
                        route_feedback: None,
                        connection_health: None,
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
                        route_feedback: None,
                        connection_health: None,
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
                        route_feedback: None,
                        connection_health: None,
                        server_idx: idx,
                        remote_ip: None,
                        elapsed: Duration::ZERO,
                        outcome: FetchAttemptOutcome::QuotaBlocked,
                        error: Some(e.to_string()),
                    });
                    last_error = Some(DecodedBodyError::Nntp(e));
                    continue;
                }
                Err(DecodedBodyError::Nntp(error)) if error.is_credentials_refusal() => {
                    attempts.push(FetchAttemptTrace {
                        route_feedback: None,
                        connection_health: None,
                        server_idx: idx,
                        remote_ip: None,
                        elapsed: start.elapsed(),
                        outcome: FetchAttemptOutcome::AuthenticationFailure,
                        error: Some("authentication/access failure".to_string()),
                    });
                    last_error = Some(DecodedBodyError::Nntp(NntpError::AuthenticationFailed));
                    continue;
                }
                // Nothing was asked of the server: no attempt to trace.
                Err(DecodedBodyError::Nntp(NntpError::WorkObsolete)) => {
                    return DecodedBodyTrace {
                        attempts,
                        result: Err(DecodedBodyError::Nntp(NntpError::WorkObsolete)),
                    };
                }
                Err(DecodedBodyError::Nntp(e)) if is_transient(&e) => {
                    attempts.push(FetchAttemptTrace {
                        route_feedback: None,
                        connection_health: None,
                        server_idx: idx,
                        remote_ip: None,
                        elapsed: start.elapsed(),
                        outcome: FetchAttemptOutcome::transient(&e),
                        error: Some(e.to_string()),
                    });
                    last_retryable_error = Some(DecodedBodyError::Nntp(e));
                    continue;
                }
                Err(other) => {
                    attempts.push(FetchAttemptTrace {
                        route_feedback: None,
                        connection_health: None,
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

    // Gracefully shut down the client and all pooled connections.
    pub async fn shutdown(&self) {
        self.pool.shutdown().await;
    }

    // Access the underlying connection pool.
    pub fn pool(&self) -> &Arc<NntpPool> {
        &self.pool
    }

    // Feed the fetch times of successful attempts to the routes of the
    // connections that served them; a direct leg books them on its address
    // plan.
    pub fn record_fetch_attempts(&self, attempts: &[FetchAttemptTrace]) {
        for attempt in attempts {
            if attempt.outcome != FetchAttemptOutcome::Success {
                continue;
            }
            if let Some(feedback) = &attempt.route_feedback {
                feedback.body_latency(attempt.elapsed);
            }
        }
    }

    // Book one decoded article's bytes and wire time against the address
    // that served it, for the address plan's delivery comparison. `attempts`
    // is the fetch's trace; the successful attempt names the server and the
    // address. The caller leaves out a connection's first fetch and any fetch
    // made while the job could not take bytes as fast as the wire offered.
    pub fn record_address_delivery(
        &self,
        attempts: &[FetchAttemptTrace],
        payload_bytes: u64,
        wire: Duration,
    ) {
        let Some(served) = attempts
            .iter()
            .rev()
            .find(|attempt| attempt.outcome == FetchAttemptOutcome::Success)
        else {
            return;
        };
        if let Some(feedback) = &served.route_feedback {
            feedback.delivered(payload_bytes, wire);
        }
    }

    pub fn try_acquire_blocking_body_lane(
        &self,
        groups: &[String],
        exclude: ServerSet,
    ) -> std::result::Result<crate::blocking::BlockingBodyLane, BlockingBodyLaneAcquireError> {
        self.try_acquire_blocking_body_lane_with_estimate(groups, exclude, 0)
    }

    // Inspect the synchronous owned-lane candidates without acquiring a
    // connection permit or opening a connection.
    //
    // `None` means the shared health state was busy, not that no server is
    // eligible. The two must not be folded together: a caller that reads
    // contention as "nothing can serve this" takes a permanent decision on a
    // momentary lock collision.
    pub fn try_blocking_body_server_selection_with_estimate(
        &self,
        exclude: ServerSet,
        requested_body_bytes: u64,
    ) -> Option<BodyServerSelection> {
        self.try_blocking_body_server_selection(exclude, requested_body_bytes, QuotaCheck::Dispatch)
    }

    pub fn try_acquire_blocking_body_lane_with_estimate(
        &self,
        groups: &[String],
        exclude: ServerSet,
        requested_body_bytes: u64,
    ) -> std::result::Result<crate::blocking::BlockingBodyLane, BlockingBodyLaneAcquireError> {
        self.acquire_blocking_lane(groups, exclude, requested_body_bytes, None)
    }

    // Open a connection no work is waiting on yet, on `seat` only.
    //
    // The caller names the server whose free seat it may fill, so the warm
    // never takes a seat demanded work has claimed. The server choice is
    // still the one a lease would make: a seat on a server the article's
    // provider policy does not make eligible is refused.
    pub fn try_warm_blocking_body_lane(
        &self,
        groups: &[String],
        exclude: ServerSet,
        requested_body_bytes: u64,
        seat: ServerId,
    ) -> std::result::Result<crate::blocking::BlockingBodyLane, BlockingBodyLaneAcquireError> {
        self.acquire_blocking_lane(groups, exclude, requested_body_bytes, Some(seat))
    }

    fn acquire_blocking_lane(
        &self,
        groups: &[String],
        exclude: ServerSet,
        requested_body_bytes: u64,
        warm_seat: Option<ServerId>,
    ) -> std::result::Result<crate::blocking::BlockingBodyLane, BlockingBodyLaneAcquireError> {
        // Work is waiting on every dial except a warm.
        let demanded = warm_seat.is_none();
        let Some(selection) = self.try_blocking_body_server_selection(
            exclude,
            requested_body_bytes,
            QuotaCheck::Dispatch,
        ) else {
            return Err(BlockingBodyLaneAcquireError::SelectionContended);
        };
        if selection.eligible.is_empty() {
            return Err(match selection.quota_blocked {
                Some(rejection) => {
                    BlockingBodyLaneAcquireError::Other(NntpError::quota_blocked(rejection))
                }
                None => self
                    .credentials_refusal(exclude)
                    .unwrap_or(BlockingBodyLaneAcquireError::NoEligibleServer),
            });
        }

        let mut saw_local_capacity = false;
        let mut saw_contention = false;
        let mut saw_recovery_gate = false;
        let mut credentials_refused = None;
        let mut provider_capacity_error = None;
        let mut other_error = None;
        for server in selection.eligible {
            if warm_seat.is_some_and(|seat| seat != server) {
                continue;
            }
            let policy = self.pool.provider_policy();
            if !matches!(
                policy.eligibility(server.0, exclude, ServerSet::EMPTY, requested_body_bytes),
                crate::provider_policy::ArticleEligibility::Eligible(_)
            ) {
                // An occupied permit in a higher group cannot turn this
                // request into lower-priority demand.
                saw_local_capacity = true;
                continue;
            }
            let Some(permit) = self
                .pool
                .try_acquire_blocking_permit_uncontended(server, demanded)
            else {
                // Another caller held the server's reconnect stamp for the
                // moment: a lost race, retried at once like a contended
                // selection, never waited on as if capacity were short.
                saw_contention = true;
                continue;
            };
            let permit = match permit {
                Ok(permit) => permit,
                // Kept apart from a permit or socket shortage: one is a queue
                // to wait in, the other is a server holding itself open for a
                // single probe.
                Err(NntpError::ServerRecovering) => {
                    saw_recovery_gate = true;
                    continue;
                }
                Err(error) if error.is_credentials_refusal() => {
                    credentials_refused.get_or_insert(
                        BlockingBodyLaneAcquireError::CredentialsRefused { server, error },
                    );
                    continue;
                }
                Err(_) => {
                    saw_local_capacity = true;
                    continue;
                }
            };
            let config = self
                .pool
                .blocking_connect_config(server)
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
            let health_lease = Arc::clone(&permit.health_lease);
            let probe = health_lease.0.probing();
            match self
                .pool
                .with_blocking_connect_admission(server, probe, || {
                    crate::blocking::BlockingBodyLane::connect(
                        server,
                        self.pool.stable_server_id(server).unwrap_or_default(),
                        self.pool.server_transfer_control(server),
                        &config,
                        groups,
                        self.soft_timeout,
                        permit,
                    )
                }) {
                Ok(lane) => {
                    self.pool.note_provider_admitted(server, admission);
                    return Ok(lane);
                }
                Err(error) => {
                    self.record_blocking_connect_failure(
                        server.0,
                        admission,
                        &error,
                        &health_lease.0,
                    );
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
                    match BlockingBodyLaneAcquireError::from_connect_error(server, error) {
                        BlockingBodyLaneAcquireError::ProviderCapacity(error) => {
                            provider_capacity_error = Some(error);
                        }
                        BlockingBodyLaneAcquireError::Other(error) => {
                            other_error = Some(error);
                        }
                        refused @ BlockingBodyLaneAcquireError::CredentialsRefused { .. } => {
                            credentials_refused.get_or_insert(refused);
                        }
                        BlockingBodyLaneAcquireError::LocalCapacity
                        | BlockingBodyLaneAcquireError::ServerRecovering
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
        } else if saw_contention {
            Err(BlockingBodyLaneAcquireError::SelectionContended)
        } else if saw_local_capacity {
            Err(BlockingBodyLaneAcquireError::LocalCapacity)
        } else if saw_recovery_gate {
            Err(BlockingBodyLaneAcquireError::ServerRecovering)
        } else if let Some(refused) = credentials_refused {
            // Last of the refusals that name a server: any wait above means
            // another server may still serve the work.
            Err(refused)
        } else {
            Err(self
                .credentials_refusal(exclude)
                .unwrap_or(BlockingBodyLaneAcquireError::NoEligibleServer))
        }
    }

    // The refusal of the first server this request could otherwise be sent to
    // that refuses this pool's credentials, if one does. Health keeps such a
    // server out of the ranking, so without this its refusal would read as
    // "no eligible server", which waits: the work it alone may serve would
    // sit in the queue for as long as the credentials stay wrong.
    fn credentials_refusal(&self, exclude: ServerSet) -> Option<BlockingBodyLaneAcquireError> {
        let backfill_flags = self.pool.server_backfill_flags();
        let backfill_unlocked = self.pool.fill_servers_exhausted(exclude);
        (0..self.pool.server_count())
            .find(|idx| {
                !exclude.contains(*idx)
                    && self.pool.route_available(*idx)
                    && (!backfill_flags[*idx] || backfill_unlocked)
                    && self.pool.refuses_credentials(*idx)
            })
            .map(|idx| BlockingBodyLaneAcquireError::CredentialsRefused {
                server: ServerId(idx),
                error: NntpError::AuthenticationRejected,
            })
    }

    // Whether the owned blocking lanes could take this work, and whether the
    // answer is one at all.
    //
    // [`BlockingBodyLaneCandidacy::Contended`] is the reason this is not a
    // `bool`: the shared health state was busy for the length of the spin, so
    // nothing is known about the servers. Treating that as "no candidate"
    // sends the work to the asynchronous lanes, which then queue for the very
    // connection permits the idle owned lanes are holding and wait out the
    // whole acquire deadline for it.
    // The servers a new owned lane would try, in the order the pool ranks
    // them right now. A question, not a dispatch: it reserves nothing and
    // clears no quota latch. `None` while the health state is contended.
    pub fn blocking_body_server_order(&self, exclude: ServerSet) -> Option<Vec<ServerId>> {
        self.try_blocking_body_server_selection(exclude, 0, QuotaCheck::ReadOnly)
            .map(|selection| selection.eligible)
    }

    pub fn blocking_body_lane_candidacy(&self, exclude: ServerSet) -> BlockingBodyLaneCandidacy {
        // A question, not a dispatch: it asks for no bytes, so every server
        // with any headroom "fits", and letting that fit clear a server's
        // blocked latch erased the refusal the last real dispatch recorded —
        // a capped server read as open until something else asked it for
        // real bytes.
        let Some(selection) =
            self.try_blocking_body_server_selection(exclude, 0, QuotaCheck::ReadOnly)
        else {
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
            let Ok(config) = self.pool.blocking_connect_config(server) else {
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

    pub fn has_blocking_body_lane_candidate(&self, exclude: ServerSet) -> bool {
        matches!(
            self.blocking_body_lane_candidacy(exclude),
            BlockingBodyLaneCandidacy::Candidate
        )
    }

    pub fn record_blocking_attempts(&self, attempts: &[FetchAttemptTrace]) {
        for attempt in attempts {
            if let Some(ticket) = &attempt.connection_health {
                let success = matches!(
                    attempt.outcome,
                    FetchAttemptOutcome::Success | FetchAttemptOutcome::NotFound
                );
                let failure = matches!(
                    attempt.outcome,
                    FetchAttemptOutcome::TransientFailure
                        | FetchAttemptOutcome::AuthenticationFailure
                );
                if success || failure {
                    let mut health = self.pool.health().blocking_lock();
                    health.record_connection_outcome(
                        attempt.server_idx,
                        ticket,
                        success,
                        matches!(attempt.outcome, FetchAttemptOutcome::AuthenticationFailure),
                    );
                    if success {
                        health.record_latency(attempt.server_idx, attempt.elapsed);
                    }
                }
                self.pool.retire_quarantined_idle(attempt.server_idx);
                continue;
            }
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
                | FetchAttemptOutcome::GroupSelectionRequired
                | FetchAttemptOutcome::PermanentFailure => {}
            }
        }
    }

    // Ranked owned-lane candidates, or `None` when the health mutex was held
    // by someone else for the whole (short) spin.
    //
    // The distinction matters: an empty selection means "no server can serve
    // this request", while contention means "ask again in a moment". Folding
    // the two together made every contended selection look like a permanent
    // tiering verdict and silently pushed the work off the owned fast path.
    fn try_blocking_body_server_selection(
        &self,
        exclude: ServerSet,
        requested_body_bytes: u64,
        quota_check: QuotaCheck,
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
        let backfill_unlocked = self.pool.fill_servers_exhausted(exclude);

        let mut candidates = Vec::with_capacity(server_count);
        let mut quota_blocked = None;
        for (idx, group) in server_groups.iter().copied().enumerate().take(server_count) {
            if exclude.contains(idx) || !health.is_available(idx) || !self.pool.route_available(idx)
            {
                continue;
            }
            // Backfill servers only serve requests whose fill tier is
            // exhausted (see build_server_order).
            if backfill_flags[idx] && !backfill_unlocked {
                continue;
            }
            if let Some(rejection) =
                self.pool
                    .server_transfer_control(ServerId(idx))
                    .and_then(|control| match quota_check {
                        QuotaCheck::Dispatch => {
                            control.quota_rejection_for_dispatch(requested_body_bytes)
                        }
                        QuotaCheck::ReadOnly => control.quota_rejection_for(requested_body_bytes),
                    })
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
        ticket: &crate::recovery::ConnectionHealth,
    ) {
        if crate::pool::dial_declined_over_limit(error) {
            // Provider admission pressure parks new sockets, it does not make
            // the server unhealthy. Cooling the whole server here would also
            // block already-established healthy lanes from refilling.
            self.pool.note_provider_over_limit(ServerId(server_idx));
            return;
        }
        // Any other failure says nothing about the provider's limit; if this
        // connect was the post-holdoff probe, let the next caller ask.
        self.pool
            .release_over_limit_probe(ServerId(server_idx), admission);
        if matches!(
            error,
            NntpError::AuthenticationFailed
                | NntpError::AuthenticationRejected
                | NntpError::AuthenticationRequired
                | NntpError::AccessDenied
        ) {
            self.pool
                .health()
                .blocking_lock()
                .record_connection_outcome(server_idx, ticket, false, true);
        } else if cooldown_reason(error).is_some() {
            self.pool.note_blocking_connect_failure(server_idx);
            self.pool
                .health()
                .blocking_lock()
                .record_connection_outcome(server_idx, ticket, false, false);
        }
        self.pool.retire_quarantined_idle(server_idx);
    }

    async fn record_server_success(&self, server_idx: usize, elapsed: Duration) {
        let mut health = self.pool.health().lock().await;
        health.record_latency(server_idx, elapsed);
    }

    async fn record_connection_reply(
        &self,
        idx: usize,
        ticket: &crate::recovery::ConnectionHealth,
        error: Option<&NntpError>,
    ) {
        let success = error.is_none_or(|error| {
            matches!(
                error,
                NntpError::ArticleNotFound
                    | NntpError::NoSuchArticle { .. }
                    | NntpError::NoArticleWithNumber
            )
        });
        let auth = error.is_some_and(|error| {
            matches!(
                error,
                NntpError::AuthenticationFailed
                    | NntpError::AuthenticationRejected
                    | NntpError::AuthenticationRequired
                    | NntpError::AccessDenied
            )
        });
        let failure = error.is_some_and(|error| {
            stat_cooldown_reason(error).is_some()
                && !matches!(
                    error,
                    NntpError::PoolExhausted
                        | NntpError::PoolShutdown
                        | NntpError::TooManyConnections
                        | NntpError::ServerOverLimit { .. }
                        | NntpError::AcquireTimeout(_)
                )
        });
        if success || auth || failure {
            self.pool
                .health()
                .lock()
                .await
                .record_connection_outcome(idx, ticket, success, auth);
            self.pool.retire_quarantined_idle(idx);
        }
    }

    async fn record_premature_death_if_needed(&self, server_idx: usize, age: Duration) {
        if age < crate::health::ServerHealth::MIN_CONNECTION_LIFETIME {
            let mut health = self.pool.health().lock().await;
            health.record_premature_death(server_idx);
        }
    }

    // Drop the connection that just failed, and only that one.
    //
    // One socket's fault says nothing about the server's other sockets: they
    // were opened at different times, may run over different addresses, and
    // are the warm capacity the next request is about to reuse. Throwing them
    // away turned a single refused command into a fleet-wide cold dial.
    async fn discard_connection_error(&self, server_idx: usize, conn: PooledConnection) {
        let age = conn.created_at().elapsed();
        if let Some(lease) = &conn.health_lease {
            self.record_connection_reply(server_idx, &lease.0, Some(&NntpError::ConnectionClosed))
                .await;
        }
        conn.discard();
        self.record_premature_death_if_needed(server_idx, age).await;
    }

    // Whether `server` could hand out a normal lease right now without
    // waiting on its connection semaphore.
    pub fn has_available_permit(&self, server: ServerId) -> bool {
        self.pool.has_available_permit(server)
    }

    async fn acquire_before_deadline(
        &self,
        server: ServerId,
        deadline: &mut TokioInstant,
    ) -> Result<PooledConnection> {
        self.pool
            .acquire_guarded_before_deadline(
                server,
                None,
                true,
                deadline,
                self.work_guard.as_deref(),
            )
            .await
            .and_then(|conn| self.validate_acquired_work(server, conn))
            .map_err(|error| self.map_acquire_timeout(error))
    }

    fn validate_acquired_work(
        &self,
        server: ServerId,
        conn: PooledConnection,
    ) -> Result<PooledConnection> {
        if let Some(guard) = &self.work_guard {
            let policy = self
                .pool
                .provider_policy_on_connection(server.0, conn.holds_recovery_probe());
            if !guard(server, &policy) {
                return Err(NntpError::WorkObsolete);
            }
        }
        Ok(conn)
    }

    fn map_acquire_timeout(&self, error: NntpError) -> NntpError {
        match error {
            // Zero is the pool reporting that it never waited: the deadline was
            // already spent, or every connection belongs to a busy download
            // lane. Restamping it with the soft timeout would claim a wait that
            // never happened and hide the difference from whoever reads the log.
            NntpError::AcquireTimeout(0) => NntpError::AcquireTimeout(0),
            NntpError::AcquireTimeout(_) => self.acquire_timeout_error(),
            other => other,
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

    // The acquire-side twin of [`Self::soft_timeout_error`], for deadlines
    // that expire while we are still queued behind our own connection
    // semaphore.
    fn acquire_timeout_error(&self) -> NntpError {
        NntpError::AcquireTimeout(self.soft_timeout.as_secs())
    }

    // Rank candidates without granting permission to use them. Eligibility
    // is evaluated for each article immediately before its next attempt.
    async fn article_server_order(&self, exclude: ServerSet, avoid: ServerSet) -> Vec<usize> {
        let mut groups = std::collections::BTreeMap::new();
        for idx in 0..self.pool.server_count() {
            if !exclude.contains(idx) {
                groups
                    .entry((
                        self.pool.server_backfill_flags()[idx],
                        self.pool.server_groups()[idx],
                    ))
                    .or_insert_with(Vec::new)
                    .push(idx);
            }
        }
        let mut order = Vec::new();
        for group in groups.into_values() {
            let mut ranked = self.rank_servers_in_group(&group).await;
            ranked.sort_by_key(|idx| avoid.contains(*idx));
            order.extend(ranked);
        }
        order
    }

    async fn article_attempt_eligible(
        &self,
        server: usize,
        exclude: ServerSet,
        attempts: &[FetchAttemptTrace],
    ) -> bool {
        let missing: ServerSet = attempts
            .iter()
            .filter(|attempt| matches!(attempt.outcome, FetchAttemptOutcome::NotFound))
            .map(|attempt| attempt.server_idx)
            .collect();
        matches!(
            self.pool
                .provider_policy()
                .eligibility(server, missing, exclude, 1),
            crate::provider_policy::ArticleEligibility::Eligible(_)
        )
    }

    // Build the server try-order, exhausting lower-priority fill groups
    // before higher-priority ones, with backfill servers reachable only
    // once every fill server is excluded for this request.
    //
    // Servers in `exclude` are skipped entirely. Disabled servers and
    // short-lived cooldown servers are excluded so we don't waste time on
    // servers that are known to be failing right now.
    //
    // A `CoolingDown` fill tier does NOT unlock backfill: those are 5–10 s
    // blips, so the order is temporarily empty and callers wait rather than
    // spilling ordinary work onto backfill accounts. Neither does an outage
    // disable (`ConsecutiveFailures` / `FailureRatio`): it heals on its own,
    // and spilling the whole queue onto a paid backfill account for a 30 s
    // primary blip is the wrong trade. Nor does an `AuthFailure` disable:
    // backfill fills articles the fill servers do not have, it never stands
    // in for a fill server refusing this pool's credentials
    // ([`crate::provider_policy::backfill_unlocked`]). Within a priority group, immediately acquirable
    // servers are preferred over fully saturated peers.
    #[allow(clippy::needless_range_loop)]
    async fn build_server_order(&self, exclude: ServerSet) -> Vec<usize> {
        let server_count = self.pool.server_count();
        let server_groups = self.pool.server_groups();
        let backfill_flags = self.pool.server_backfill_flags();

        // Check health to skip disabled servers.
        let mut health = self.pool.health().lock().await;
        health.check_reenable_all();
        let backfill_unlocked = self.pool.fill_servers_exhausted(exclude);

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
            if exclude.contains(idx) || !health.is_available(idx) || !self.pool.route_available(idx)
            {
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

    // Rank servers within a single priority group using weighted random selection
    // based on latency EWMA and current load.
    //
    // The first server is chosen probabilistically (weight = 1/score), and the
    // rest are appended in ascending score order. This ensures that faster,
    // less-loaded servers are tried first while still providing load distribution.
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

    // Internal: fetch with multi-server failover logic.
    async fn fetch_with_failover(&self, message_id: &str) -> Result<Bytes> {
        let exclude = ServerSet::EMPTY;
        let order = self.article_server_order(exclude, ServerSet::EMPTY).await;
        let mut last_error: Option<NntpError> = None;
        let mut last_retryable_error: Option<NntpError> = None;

        let mut missing = ServerSet::EMPTY;
        for idx in order {
            if !matches!(
                self.pool
                    .provider_policy()
                    .eligibility(idx, missing, exclude, 1),
                crate::provider_policy::ArticleEligibility::Eligible(_)
            ) {
                continue;
            }
            let server = ServerId(idx);
            let start = Instant::now();

            match self.fetch_from_server(server, message_id).await {
                Ok(data) => {
                    let elapsed = start.elapsed();
                    self.record_server_success(idx, elapsed).await;
                    return Ok(data);
                }
                Err(NntpError::ArticleNotFound)
                | Err(NntpError::NoSuchArticle { .. })
                | Err(NntpError::NoArticleWithNumber) => {
                    missing.insert(idx);
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

    // Try to select one of the given groups on the connection.
    //
    // Returns `Ok(true)` if a group was selected or the server needs none,
    // `Ok(false)` if none were accepted, or `Err` on a connection-level
    // error.
    //
    // A server that has not proven it refuses message-id fetches without a
    // selected group is sent no GROUP, as on the download lanes: the round
    // trip is spent only where a 412 has asked for it.
    async fn try_select_group(conn: &mut PooledConnection, groups: &[String]) -> Result<bool> {
        if !conn.needs_group_prologue() {
            if !groups.is_empty() {
                conn.decline_group();
            }
            return Ok(true);
        }
        for group in groups {
            match conn.select_group(group).await {
                Ok(()) => return Ok(true),
                Err(e) if is_connection_error(&e) => return Err(e),
                Err(_) => continue,
            }
        }
        Ok(false)
    }

    async fn fetch_decoded_from_server_with_groups(
        &self,
        server: ServerId,
        message_id: &str,
        groups: &[String],
    ) -> std::result::Result<(DecodedBody, Option<IpAddr>, RouteFeedback), DecodedBodyError> {
        let mut attempts = 0u32;

        loop {
            let mut deadline = TokioInstant::now() + self.soft_timeout;
            let mut conn = self
                .acquire_before_deadline(server, &mut deadline)
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

            if let Some(lease) = &conn.health_lease {
                let error = match &stream_result {
                    Err(FusedYencError::Nntp(error)) => Some(error),
                    _ => None,
                };
                self.record_connection_reply(server.0, &lease.0, error)
                    .await;
            }
            match stream_result {
                Ok(article) => {
                    return Ok((
                        DecodedBody {
                            raw_size: decoded_raw_size_from_fused_stats(&article.stats),
                            cpu: decoded_cpu_from_fused_stats(&article.stats),
                            io: decoded_io_from_fused_stats(&article.stats),
                            decoded: article.chunks,
                            body: article.body,
                        },
                        conn.remote_ip(),
                        conn.route_outcome.clone(),
                    ));
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
                    } else if is_egress_quota_rejection(&e) {
                        conn.discard();
                    }
                    return Err(DecodedBodyError::Nntp(e));
                }
            }
        }
    }

    // Fetch from a specific server, retrying on transient errors.
    //
    // Connection acquisition and retry delay are bounded by a per-attempt
    // soft timeout. BODY payload reads use an inactivity budget that restarts
    // whenever bytes arrive or a transfer-policy wait ends.
    async fn fetch_from_server(&self, server: ServerId, message_id: &str) -> Result<Bytes> {
        let mut attempts = 0u32;

        loop {
            let mut deadline = TokioInstant::now() + self.soft_timeout;
            let mut conn = self.acquire_before_deadline(server, &mut deadline).await?;

            let mut budget = ActiveTransferBudget::new(self.soft_timeout);
            let result = conn
                .body_by_id_raw_with_active_budget(message_id, &mut budget)
                .await;

            if let Some(lease) = &conn.health_lease {
                self.record_connection_reply(server.0, &lease.0, result.as_ref().err())
                    .await;
            }
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
                    } else if is_egress_quota_rejection(&e) {
                        conn.discard();
                    }
                    return Err(e);
                }
            }
        }
    }
}

// Returns true if the error is transient and the request might succeed on retry.
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

// Whether the egress the connection runs over refused the body for quota.
//
// The socket is sound, but it is bound to a route leg with no quota left;
// pooling it would hand the next request the same refusal. It is dropped
// without a health penalty so the next dial takes whatever leg the route
// now offers.
fn is_egress_quota_rejection(err: &NntpError) -> bool {
    matches!(
        err,
        NntpError::QuotaBlocked(rejection) if rejection.scope == crate::transfer::TransferScope::Egress
    )
}

// Returns true if the error indicates the connection itself is bad
// and should be discarded rather than returned to the pool.
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
            // The batch's remaining replies are still queued on that socket,
            // so nothing else may be sent down it.
            | NntpError::SessionExpired
            | NntpError::SoftTimeout(_)
    )
}

#[cfg(test)]
mod tests;
