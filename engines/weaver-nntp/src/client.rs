// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use std::net::IpAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use rand::distr::weighted::WeightedIndex;
use rand::prelude::*;
use tracing::{debug, warn};
use weaver_yenc::YencError;

use crate::ServerSet;

use crate::connection::ServerConfig;
use crate::error::NntpError;
use crate::fused_yenc::FusedArticleBody;
use crate::health::{CooldownReason, ServerState};
use crate::pool::{
    BodyServerAvailability, FreshConnectAdmission, NntpPool, PoolConfig, ServerId, ServerPoolConfig,
};
use crate::tls::TransportReadStats;
use crate::transfer::QuotaRejection;

// Configuration for the high-level NNTP client.
pub struct NntpClientConfig {
    // Server configurations in priority order (primary first).
    pub servers: Vec<ServerPoolConfig>,
    // Maximum age for idle connections before eviction.
    pub max_idle_age: Duration,
    // Inert: the download lanes retry by handing an article back to the
    // scheduler, not on the same server inside the client.
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

// High-level NNTP client over a server pool.
//
// Ranks the servers for an article, hands out blocking BODY lanes on the
// chosen server, and records what each attempt taught the pool.
#[derive(Clone)]
pub struct NntpClient {
    pool: Arc<NntpPool>,
    // Per-article inactivity budget, excluding deliberate local rate waits.
    soft_timeout: Duration,
}

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
    // The server's network route had no room for the dial: a tunnel on it is
    // full, or its WireGuard session budget is spent
    // (`NntpError::is_route_capacity`). Self-clearing as a socket or session
    // closes, and no verdict about the server.
    RouteCapacity(NntpError),
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
        } else if error.is_route_capacity() {
            Self::RouteCapacity(error)
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
            Self::ProviderCapacity(_)
                | Self::RouteCapacity(_)
                | Self::LocalCapacity
                | Self::ServerRecovering
        )
    }

    // Whether the server's network route was full. Its work is asked again
    // after the lane-unavailable delay rather than at once: nothing tells the
    // scheduler when the tunnel frees, and handing the work straight back
    // would redial the full tunnel on every dispatch pass.
    pub fn is_route_capacity(&self) -> bool {
        matches!(self, Self::RouteCapacity(_))
    }

    // The transport error behind this refusal, where there is one. Local
    // capacity, selection contention and an empty candidate list are the
    // lane's own bookkeeping and carry no server answer to report.
    pub fn nntp_error(&self) -> Option<&NntpError> {
        match self {
            Self::ProviderCapacity(error)
            | Self::RouteCapacity(error)
            | Self::Other(error)
            | Self::CredentialsRefused { error, .. } => Some(error),
            Self::LocalCapacity
            | Self::ServerRecovering
            | Self::NoEligibleServer
            | Self::SelectionContended => None,
        }
    }

    // Whether the leased work should be handed straight back to the scheduler
    // to be retried on a lane. Capacity admission and selection contention
    // are both "ask again shortly", not verdicts about the servers.
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
            Self::RouteCapacity(_) => "route_capacity",
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
            Self::RouteCapacity(error) => {
                write!(formatter, "the server's network route is full ({error})")
            }
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
    // No eligible server can.
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

// Whether a failed dial says something about its server: everything but a
// route refusal that is no evidence (a full tunnel, a skipped leg).
fn is_server_evidence(error: &NntpError) -> bool {
    !matches!(error, NntpError::Route(route) if !route.is_evidence())
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
        let soft_timeout = config.soft_timeout;
        let pool = NntpPool::new(PoolConfig {
            servers: config.servers,
            max_idle_age: config.max_idle_age,
            ..PoolConfig::default()
        });
        Self::around(pool, soft_timeout)
    }

    // A client for settings: refuses, in words, a server list past
    // `MAX_PROVIDERS` instead of building a pool it cannot index.
    pub fn try_new(
        config: NntpClientConfig,
    ) -> std::result::Result<Self, crate::pool::PoolTooLarge> {
        let soft_timeout = config.soft_timeout;
        let pool = NntpPool::try_new(PoolConfig {
            servers: config.servers,
            max_idle_age: config.max_idle_age,
            ..PoolConfig::default()
        })?;
        Ok(Self::around(pool, soft_timeout))
    }

    fn around(pool: NntpPool, soft_timeout: Duration) -> Self {
        let pool = Arc::new(pool);
        pool.start_route_probes();
        NntpClient { pool, soft_timeout }
    }

    // Create a client wrapping an existing pool.
    pub fn from_pool(pool: Arc<NntpPool>) -> Self {
        pool.start_route_probes();
        NntpClient {
            pool,
            soft_timeout: Duration::from_secs(15),
        }
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
        let mut route_capacity_error = None;
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
                    // requeued for another lane, so the download
                    // keeps running — slowly, on a fraction of its lanes, with
                    // nothing above debug to say why. A full route is told as
                    // one: the dial never left this machine.
                    if error.is_route_capacity() {
                        debug!(
                            server = server.0,
                            error = %error,
                            "blocking BODY lane not opened: the server's network route is full"
                        );
                    } else {
                        debug!(
                            server = server.0,
                            error = %error,
                            elapsed_ms = started.elapsed().as_millis(),
                            "blocking BODY lane connect failed"
                        );
                    }
                    if let Some(suppressed) = self.pool.note_blocking_connect_warning(server) {
                        if error.is_route_capacity() {
                            warn!(
                                server = server.0,
                                address = %self.pool.server_address(server),
                                error = %error,
                                refusals_since_last_warning = suppressed,
                                "blocking BODY lane not opened: the server's network route is full; the work waits for room"
                            );
                        } else {
                            warn!(
                                server = server.0,
                                address = %self.pool.server_address(server),
                                error = %error,
                                elapsed_ms = started.elapsed().as_millis(),
                                failures_since_last_warning = suppressed,
                                "blocking BODY lane connect failed"
                            );
                        }
                    }
                    match BlockingBodyLaneAcquireError::from_connect_error(server, error) {
                        BlockingBodyLaneAcquireError::ProviderCapacity(error) => {
                            provider_capacity_error = Some(error);
                        }
                        BlockingBodyLaneAcquireError::RouteCapacity(error) => {
                            route_capacity_error = Some(error);
                        }
                        // A server's real fault is kept over a later
                        // refusal that says nothing about its server.
                        BlockingBodyLaneAcquireError::Other(error) => {
                            if is_server_evidence(&error)
                                || !other_error.as_ref().is_some_and(is_server_evidence)
                            {
                                other_error = Some(error);
                            }
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
        } else if let Some(error) = other_error.take_if(|error| is_server_evidence(error)) {
            // A server that failed for real is the answer, as a rung that
            // failed for real outranks a full one in the route's ladder.
            Err(BlockingBodyLaneAcquireError::Other(error))
        } else if let Some(error) = route_capacity_error {
            // A wait on a server that will take the work once its route has
            // room, ahead of a refusal that says nothing about any server.
            Err(BlockingBodyLaneAcquireError::RouteCapacity(error))
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
    // fails work the servers were never asked about.
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

    // Whether `server` could hand out a normal lease right now without
    // waiting on its connection semaphore.
    pub fn has_available_permit(&self, server: ServerId) -> bool {
        self.pool.has_available_permit(server)
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

#[cfg(test)]
mod tests;
