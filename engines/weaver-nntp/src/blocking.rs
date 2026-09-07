use std::collections::VecDeque;
#[cfg(not(windows))]
use std::ffi::{CStr, CString};
use std::io::{self, Read, Write};
use std::net::{IpAddr, SocketAddr, TcpStream, ToSocketAddrs};
#[cfg(not(windows))]
use std::os::fd::AsRawFd;
#[cfg(not(windows))]
use std::ptr::NonNull;
use std::sync::Arc;
use std::time::{Duration, Instant};

#[cfg(not(windows))]
use bytes::BufMut;
use bytes::BytesMut;
#[cfg(not(windows))]
use s2n_tls_sys as s2n;
use tokio_util::codec::Decoder;
use tracing::{debug, trace, warn};
use weaver_yenc::CheckpointPlan;

use crate::client::{
    BodyLaneMode, BodyLaneTraceMeta, DecodedBody, DecodedBodyCpu, DecodedBodyError, DecodedBodyIo,
    DecodedBodyTrace, FetchAttemptOutcome, FetchAttemptTrace, ProbeBatchResult,
};
use crate::codec::{NntpCodec, NntpFrame};
use crate::commands::Command;
use crate::connection::{NntpBufferProfile, ServerConfig};
use crate::error::{NntpError, Result};
use crate::fused_yenc::{
    FusedYencArticle, FusedYencArticleDecoder, FusedYencArticleStats, FusedYencError,
};
use crate::pool::{BlockingConnectionPermit, ServerId};
use crate::response::parse_response;
use crate::tls::{
    NntpTlsBackend, RustlsSession, TLS_READ_BUFFER, TlsCipherPreference, TransportReadStats,
    build_tls_config_with_name_mismatch_certificate, make_server_name,
    selected_blocking_tls_backend, tls_backend_for_preference,
};
use crate::transfer::{
    ActiveTransferBudget, BodyTransferAccounting, ServerTransferControl, StableServerId,
    active_transfer_read_timeout, active_transfer_timeout,
};
use crate::types::{ArticleId, Capabilities, Response};

const MIN_TIMEOUT: Duration = Duration::from_secs(1);
#[cfg(not(windows))]
const S2N_BLOCKING_IO_SLICE: Duration = Duration::from_millis(100);

fn classify_active_io_error(
    error: NntpError,
    active_timeout: bool,
    budget: Option<&ActiveTransferBudget>,
) -> NntpError {
    let timed_out = match &error {
        NntpError::Timeout => true,
        NntpError::Io(io_error) => matches!(
            io_error.kind(),
            io::ErrorKind::TimedOut | io::ErrorKind::WouldBlock
        ),
        _ => false,
    };
    if active_timeout && budget.is_some_and(|budget| budget.remaining().is_zero() || timed_out) {
        active_transfer_timeout(budget.expect("active BODY budget is present"))
    } else {
        error
    }
}

#[derive(Debug, Clone, Copy, Default)]
pub struct BlockingLaneStats {
    pub socket_reads: u64,
    pub socket_writes: u64,
    pub tls_recv_calls: u64,
    pub tls_send_calls: u64,
    pub body_responses: u64,
    pub decoded_articles: u64,
}

#[cfg(not(windows))]
struct BlockingS2nStream {
    conn: RawS2nConnection,
    _config: RawS2nConfig,
    tcp: TcpStream,
    stats: BlockingLaneStats,
}

/// Blocking rustls transport for the owned BODY lane. Mirrors the async
/// `ManualTls` read shape — large buffered ciphertext reads decrypted in bulk
/// through the shared `RustlsSession` engine — so throughput economics match
/// the s2n lane rather than a per-record `rustls::StreamOwned` loop.
struct BlockingManualTlsStream {
    tcp: TcpStream,
    session: RustlsSession,
    read_buffer: Vec<u8>,
    stats: BlockingLaneStats,
}

enum BlockingTransport {
    Plain(TcpStream),
    Rustls(Box<BlockingManualTlsStream>),
    #[cfg(not(windows))]
    S2n(BlockingS2nStream),
}

#[cfg(not(windows))]
struct RawS2nConfig {
    ptr: NonNull<s2n::s2n_config>,
}

#[cfg(not(windows))]
struct RawS2nConnection {
    ptr: NonNull<s2n::s2n_connection>,
}

/// One BODY command written to the wire whose response has not been read yet.
struct BodyRingRequest {
    message_id: String,
    /// The ring was empty when this went out, so the wait for its status line
    /// is a clean round-trip sample rather than time spent queued behind
    /// another article's payload.
    issued_alone: bool,
}

/// What [`BlockingBodyLane::ring_issue`] did with a request.
pub enum RingIssueOutcome {
    /// The command is on the ring; its response is owed.
    Issued,
    /// Nothing was written and the lane is still healthy — the caller owns the
    /// trace as this article's outcome and may keep reading the ring, but must
    /// not issue again.
    Rejected(Box<DecodedBodyTrace>),
    /// The write failed and the connection is poisoned. Everything still on the
    /// ring is unanswerable.
    Failed(Box<DecodedBodyTrace>),
}

/// A lane's outstanding BODY commands, driven one at a time by the caller.
///
/// [`BlockingBodyLane::fetch_decoded_pipeline_with_estimates`] writes a whole
/// batch and then reads it back, which empties the pipe at every batch edge:
/// the last response of one batch is read before the first command of the next
/// is written, so the server sits idle for a round trip. The ring lets the
/// caller top the pipe up while responses are still arriving, and keep topping
/// it up across a batch boundary.
#[derive(Default)]
struct BodyRing {
    outstanding: VecDeque<BodyRingRequest>,
    /// Requests written but not yet pushed to the wire. Batching the flush
    /// keeps a top-up of several commands in one segment, as the batch writer
    /// did.
    unflushed: usize,
    /// The connection faulted; nothing still on the ring will ever be answered.
    closed: bool,
    /// Depth the caller last issued at, and the size of the judging window that
    /// depth opens.
    current_depth: usize,
    window_depth: usize,
    window_responses: u64,
    window_clean: bool,
}

pub struct BlockingBodyLane {
    conn: BlockingNntpConnection,
    checkpoint_plan: CheckpointPlan,
    server_id: ServerId,
    stable_server_id: StableServerId,
    remote_ip: IpAddr,
    mode: BodyLaneMode,
    /// Command-to-status-line wait. Only sampled when no other request was
    /// outstanding, so pipelined batches cannot report it as near zero.
    latency_ewma: Option<Duration>,
    /// Status-line-to-terminator wait: what the article itself cost on the
    /// wire, independent of how far away the server is.
    transfer_ewma: Option<Duration>,
    soft_timeout: Duration,
    /// Outstanding pipelined BODY commands, when the caller drives the lane
    /// request-by-request instead of batch-by-batch.
    ring: BodyRing,
    _permit: BlockingConnectionPermit,
}

pub struct BlockingNntpConnection {
    transport: BlockingTransport,
    codec: NntpCodec,
    read_buf: BytesMut,
    read_scratch: Vec<u8>,
    buffer_profile: NntpBufferProfile,
    capabilities: Capabilities,
    /// The configured endpoint, kept so a requirement learned here is
    /// recorded against the server rather than the resolved address.
    host: String,
    port: u16,
    remote_addr: SocketAddr,
    command_timeout: Duration,
    current_group: Option<String>,
    credentials: Option<(String, String)>,
    poisoned: bool,
    transfer_control: Option<Arc<ServerTransferControl>>,
    body_accounting: VecDeque<BodyTransferAccounting>,
    /// Immutable geometry the next decoded article's CRC pass checkpoints at.
    /// Set per fetch by the lane; never inherited from a prior job.
    checkpoint_plan: CheckpointPlan,
    /// How long the last decoded article waited for its status line. The lane
    /// takes this to separate distance from transfer cost.
    last_response_line_wait: Duration,
    /// Armed when session setup declined to select a group this caller
    /// offered, because the server has never been shown to need one. See
    /// [`crate::server_caps`].
    group_probe_armed: bool,
}

impl BlockingBodyLane {
    /// Apply the batch's immutable geometry before every decoded response, so
    /// a lane reused by another job cannot carry previous checkpoint state.
    pub fn set_checkpoint_plan(&mut self, checkpoint_plan: CheckpointPlan) {
        self.checkpoint_plan = checkpoint_plan;
    }

    // These are independent lane identity, transfer policy, address-selection,
    // group, and ownership inputs at the engine boundary.
    #[allow(clippy::too_many_arguments)]
    pub fn connect(
        server_id: ServerId,
        stable_server_id: StableServerId,
        transfer_control: Option<Arc<ServerTransferControl>>,
        config: &ServerConfig,
        excluded_ips: &[IpAddr],
        address_offset: usize,
        groups: &[String],
        soft_timeout: Duration,
        permit: BlockingConnectionPermit,
    ) -> Result<Self> {
        if transfer_control
            .as_ref()
            .is_some_and(|control| control.stable_server_id() != stable_server_id)
        {
            return Err(NntpError::MalformedResponse(
                "stable server id does not match transfer control".to_string(),
            ));
        }
        // On a pipelining server the first candidate group rides in the
        // session-setup write; `select_group` then short-circuits on it.
        let mut conn = BlockingNntpConnection::connect_with_ip_policy_for_group(
            config,
            excluded_ips,
            address_offset,
            groups.first().map(String::as_str),
        )?;
        conn.set_transfer_control(transfer_control);
        // A body lane only ever fetches by message-id, which RFC 3977 serves
        // without a selected group. Walking the candidate groups costs a round
        // trip each before the first article can be asked for, so it runs only
        // for a server that has proven it refuses message-id fetches with 412.
        if !conn.needs_group_prologue() {
            let remote_ip = conn.remote_ip();
            return Ok(Self {
                conn,
                checkpoint_plan: CheckpointPlan::None,
                server_id,
                stable_server_id,
                remote_ip,
                mode: BodyLaneMode::Sequential,
                latency_ewma: None,
                transfer_ewma: None,
                soft_timeout,
                ring: BodyRing::default(),
                _permit: permit,
            });
        }
        for group in groups {
            match conn.select_group(group) {
                Ok(()) => {
                    let remote_ip = conn.remote_ip();
                    return Ok(Self {
                        conn,
                        checkpoint_plan: CheckpointPlan::None,
                        server_id,
                        stable_server_id,
                        remote_ip,
                        mode: BodyLaneMode::Sequential,
                        latency_ewma: None,
                        transfer_ewma: None,
                        soft_timeout,
                        ring: BodyRing::default(),
                        _permit: permit,
                    });
                }
                Err(NntpError::NoSuchGroup) => continue,
                Err(error) => return Err(error),
            }
        }

        if groups.is_empty() {
            let remote_ip = conn.remote_ip();
            Ok(Self {
                conn,
                checkpoint_plan: CheckpointPlan::None,
                server_id,
                stable_server_id,
                remote_ip,
                mode: BodyLaneMode::Sequential,
                latency_ewma: None,
                transfer_ewma: None,
                soft_timeout,
                ring: BodyRing::default(),
                _permit: permit,
            })
        } else {
            Err(NntpError::NoSuchGroup)
        }
    }

    pub fn server_id(&self) -> ServerId {
        self.server_id
    }

    pub fn stable_server_id(&self) -> StableServerId {
        self.stable_server_id
    }

    pub fn remote_ip(&self) -> IpAddr {
        self.remote_ip
    }

    pub fn mode(&self) -> BodyLaneMode {
        self.mode
    }

    pub fn latency_ewma(&self) -> Option<Duration> {
        self.latency_ewma
    }

    pub fn transfer_ewma(&self) -> Option<Duration> {
        self.transfer_ewma
    }

    pub fn supports_pipelining(&self) -> bool {
        self.conn.capabilities().supports_pipelining()
    }

    pub fn stats(&self) -> BlockingLaneStats {
        self.conn.stats()
    }

    /// Whether this lane's connection can still be used or parked.
    pub fn is_healthy(&self) -> bool {
        !self.conn.poisoned
    }

    /// Answer an existence probe on this lane's connection.
    ///
    /// The lane must be between leases: the ring is checked rather than
    /// assumed, because a probe written behind an unread BODY response would
    /// read that article's payload as its own status line.
    ///
    /// `inconclusive` carries the same meaning as it does for the async
    /// client — nothing here may be reported as a missing article unless the
    /// server actually said so.
    pub fn probe_exists(&mut self, message_ids: &[String]) -> ProbeBatchResult {
        if message_ids.is_empty() {
            return ProbeBatchResult {
                exists: Vec::new(),
                inconclusive: false,
            };
        }
        if !self.ring.outstanding.is_empty()
            || !self.conn.body_accounting.is_empty()
            || self.conn.poisoned
        {
            return ProbeBatchResult {
                exists: vec![false; message_ids.len()],
                inconclusive: true,
            };
        }
        match self.conn.probe_exists(message_ids) {
            Ok(exists) => ProbeBatchResult {
                exists,
                inconclusive: false,
            },
            Err(error) => {
                debug!(
                    server = self.server_id.0,
                    error = %error,
                    batch = message_ids.len(),
                    "owned lane could not settle an existence probe"
                );
                ProbeBatchResult {
                    exists: vec![false; message_ids.len()],
                    inconclusive: true,
                }
            }
        }
    }

    pub fn fetch_decoded_sequential(&mut self, message_id: &str) -> DecodedBodyTrace {
        self.fetch_decoded_sequential_with_estimate(message_id, 0)
    }

    pub fn fetch_decoded_sequential_with_estimate(
        &mut self,
        message_id: &str,
        estimated_body_bytes: u64,
    ) -> DecodedBodyTrace {
        self.mode = BodyLaneMode::Sequential;
        let started = Instant::now();
        let result = self.read_decoded_body(message_id, estimated_body_bytes);
        let elapsed = started.elapsed();
        let policy_elapsed = result.as_ref().map_or(elapsed, |decoded| {
            elapsed.saturating_sub(decoded.io.throttle_wait)
        });
        if result.is_ok() {
            // Nothing else was outstanding, so the status-line wait is a clean
            // latency sample.
            let latency = self.conn.take_response_line_wait().min(policy_elapsed);
            self.observe_latency(latency);
            self.observe_transfer(policy_elapsed.saturating_sub(latency));
        }
        self.trace_item(message_id, policy_elapsed, result)
    }

    pub fn fetch_decoded_pipeline(
        &mut self,
        message_ids: &[&str],
        max_depth: usize,
    ) -> Vec<(usize, DecodedBodyTrace, BodyLaneTraceMeta)> {
        self.fetch_decoded_pipeline_with_estimates(message_ids, &[], max_depth)
    }

    pub fn fetch_decoded_pipeline_with_estimates(
        &mut self,
        message_ids: &[&str],
        estimated_body_bytes: &[u64],
        max_depth: usize,
    ) -> Vec<(usize, DecodedBodyTrace, BodyLaneTraceMeta)> {
        self.mode = match max_depth {
            0 | 1 => BodyLaneMode::Sequential,
            depth => BodyLaneMode::Pipelined {
                depth: depth.min(u8::MAX as usize) as u8,
            },
        };

        let offered = message_ids.len().min(max_depth);
        if offered == 0 {
            return Vec::new();
        }

        let mut out = Vec::with_capacity(offered);
        let mut batch_clean = true;
        let mut requested = 0usize;
        let mut quota_rejection = None;
        let request_error = (|| {
            for (idx, message_id) in message_ids[..offered].iter().enumerate() {
                let estimate = estimated_body_bytes.get(idx).copied().unwrap_or(0);
                match self
                    .conn
                    .write_body_request_with_estimate(message_id, estimate)
                {
                    Ok(()) => requested += 1,
                    Err(NntpError::QuotaBlocked(rejection)) => {
                        quota_rejection = Some((idx, rejection));
                        break;
                    }
                    Err(error) => return Err(error),
                }
            }
            if requested > 0 {
                self.conn.flush_commands()
            } else {
                Ok(())
            }
        })();

        if let Err(error) = request_error {
            self.conn.fail_body_pipeline();
            let elapsed = Duration::ZERO;
            for (idx, message_id) in message_ids.iter().take(requested).enumerate() {
                let is_last = idx + 1 == requested;
                let trace = self.trace_item(
                    message_id,
                    elapsed,
                    Err(DecodedBodyError::Nntp(clone_nntp_error(&error))),
                );
                out.push((
                    idx,
                    trace,
                    BodyLaneTraceMeta {
                        batch_complete: is_last,
                        batch_clean: false,
                        batch_response_count: if is_last { requested as u64 } else { 0 },
                        unresolved_count: 0,
                        connection_discarded: true,
                    },
                ));
            }
            return out;
        }

        let mut closed_early = false;
        for (idx, message_id) in message_ids.iter().take(requested).enumerate() {
            let started = Instant::now();
            let result = if closed_early {
                Err(DecodedBodyError::Nntp(NntpError::ConnectionClosed))
            } else {
                self.read_next_decoded_body()
            };
            let elapsed = started.elapsed();
            let policy_elapsed = result.as_ref().map_or(elapsed, |decoded| {
                elapsed.saturating_sub(decoded.io.throttle_wait)
            });
            if result.is_ok() {
                let response_line_wait = self.conn.take_response_line_wait().min(policy_elapsed);
                // Only the head of the batch was issued with nothing else in
                // flight; later responses are already queued behind it, so
                // their status-line wait says nothing about distance.
                if idx == 0 {
                    self.observe_latency(response_line_wait);
                }
                self.observe_transfer(policy_elapsed.saturating_sub(response_line_wait));
            }
            if self.conn.poisoned
                || matches!(result, Err(DecodedBodyError::Nntp(ref e)) if is_connection_error(e))
            {
                closed_early = true;
            }
            let trace = self.trace_item(message_id, policy_elapsed, result);
            batch_clean &= decoded_result_keeps_connection(&trace.result);
            let is_complete = closed_early || (idx + 1 == requested && quota_rejection.is_none());
            out.push((
                idx,
                trace,
                BodyLaneTraceMeta {
                    batch_complete: is_complete,
                    batch_clean: batch_clean && !closed_early,
                    batch_response_count: if is_complete { (idx + 1) as u64 } else { 0 },
                    unresolved_count: if closed_early {
                        requested.saturating_sub(idx + 1) as u64
                    } else {
                        0
                    },
                    connection_discarded: closed_early,
                },
            ));
        }

        if let Some((quota_idx, source)) = quota_rejection {
            for (tail_idx, message_id) in
                message_ids.iter().enumerate().take(offered).skip(quota_idx)
            {
                let estimate = estimated_body_bytes.get(tail_idx).copied().unwrap_or(0);
                let error = if tail_idx == quota_idx {
                    NntpError::QuotaBlocked(source.clone())
                } else if let Some(rejection) = self
                    .conn
                    .transfer_control
                    .as_ref()
                    .and_then(|control| control.quota_rejection_for(estimate))
                {
                    NntpError::quota_blocked(rejection)
                } else {
                    NntpError::BodyNotRequestedDueToQuota {
                        preceding_rejection: source.clone(),
                        requested_body_bytes: estimate,
                    }
                };
                let trace = self.trace_item(
                    message_id,
                    Duration::ZERO,
                    Err(DecodedBodyError::Nntp(error)),
                );
                let batch_complete = !closed_early && tail_idx + 1 == offered;
                out.push((
                    tail_idx,
                    trace,
                    BodyLaneTraceMeta {
                        batch_complete,
                        batch_clean: batch_clean && !closed_early,
                        batch_response_count: if batch_complete { requested as u64 } else { 0 },
                        unresolved_count: 0,
                        connection_discarded: closed_early,
                    },
                ));
            }
        }

        out
    }

    /// BODY commands written to this lane whose responses have not been read.
    pub fn ring_outstanding(&self) -> usize {
        self.ring.outstanding.len()
    }

    /// The ring faulted: every request still on it is unanswerable and the
    /// connection must be discarded rather than parked.
    pub fn ring_is_closed(&self) -> bool {
        self.ring.closed
    }

    /// Write one more BODY onto the ring at `depth`.
    ///
    /// The command is buffered, not flushed; [`Self::ring_read_next`] pushes it
    /// before it waits, so a caller that tops the ring up by several requests
    /// still spends one write on them. `depth` is the pipeline depth in force,
    /// and sizes the window the lane judges its responses over.
    pub fn ring_issue(
        &mut self,
        message_id: &str,
        estimated_body_bytes: u64,
        depth: usize,
    ) -> RingIssueOutcome {
        if self.ring.closed || self.conn.poisoned {
            let trace = self.trace_item(
                message_id,
                Duration::ZERO,
                Err(DecodedBodyError::Nntp(NntpError::ConnectionClosed)),
            );
            return RingIssueOutcome::Failed(Box::new(trace));
        }
        let depth = depth.max(1);
        self.ring.current_depth = depth;
        self.mode = match depth {
            1 => BodyLaneMode::Sequential,
            depth => BodyLaneMode::Pipelined {
                depth: depth.min(u8::MAX as usize) as u8,
            },
        };
        let issued_alone = self.ring.outstanding.is_empty();
        match self
            .conn
            .write_body_request_with_estimate(message_id, estimated_body_bytes)
        {
            Ok(()) => {}
            // The transfer budget refused this article before anything reached
            // the wire, so the ring and the connection are exactly as they
            // were and the lane stays usable.
            Err(error @ NntpError::QuotaBlocked(_)) => {
                let trace = self.trace_item(
                    message_id,
                    Duration::ZERO,
                    Err(DecodedBodyError::Nntp(error)),
                );
                return RingIssueOutcome::Rejected(Box::new(trace));
            }
            Err(error) => {
                self.conn.fail_body_pipeline();
                self.ring.closed = true;
                let trace = self.trace_item(
                    message_id,
                    Duration::ZERO,
                    Err(DecodedBodyError::Nntp(error)),
                );
                return RingIssueOutcome::Failed(Box::new(trace));
            }
        }
        self.ring.unflushed += 1;
        self.ring.outstanding.push_back(BodyRingRequest {
            message_id: message_id.to_string(),
            issued_alone,
        });
        RingIssueOutcome::Issued
    }

    /// Read the response to the oldest request on the ring, or `None` when the
    /// ring is empty.
    ///
    /// The trace meta is judged over rolling windows of the issuing depth, so
    /// a caller that never lets the ring drain still gets the same
    /// `batch_complete` / `batch_clean` verdicts the batch API produced per
    /// batch. A fault closes the window immediately and reports everything
    /// still on the ring as unresolved.
    pub fn ring_read_next(&mut self) -> Option<(DecodedBodyTrace, BodyLaneTraceMeta)> {
        let request = self.ring.outstanding.pop_front()?;
        if self.ring.unflushed > 0 && !self.ring.closed {
            self.ring.unflushed = 0;
            if let Err(error) = self.conn.flush_commands() {
                self.conn.fail_body_pipeline();
                self.ring.closed = true;
                let trace = self.trace_item(
                    &request.message_id,
                    Duration::ZERO,
                    Err(DecodedBodyError::Nntp(error)),
                );
                let meta = self.close_ring_window(false);
                return Some((trace, meta));
            }
        }
        if self.ring.window_depth == 0 {
            self.ring.window_depth = self.ring.current_depth.max(1);
            self.ring.window_clean = true;
        }

        let started = Instant::now();
        let result = if self.ring.closed {
            Err(DecodedBodyError::Nntp(NntpError::ConnectionClosed))
        } else {
            self.read_next_decoded_body()
        };
        let elapsed = started.elapsed();
        let policy_elapsed = result.as_ref().map_or(elapsed, |decoded| {
            elapsed.saturating_sub(decoded.io.throttle_wait)
        });
        if result.is_ok() {
            let response_line_wait = self.conn.take_response_line_wait().min(policy_elapsed);
            // Only a request issued into an empty ring measures distance;
            // anything issued behind another article was queued behind its
            // payload and would read as near zero.
            if request.issued_alone {
                self.observe_latency(response_line_wait);
            }
            self.observe_transfer(policy_elapsed.saturating_sub(response_line_wait));
        }
        if self.conn.poisoned
            || matches!(result, Err(DecodedBodyError::Nntp(ref e)) if is_connection_error(e))
        {
            self.ring.closed = true;
        }
        let trace = self.trace_item(&request.message_id, policy_elapsed, result);
        let kept_connection = decoded_result_keeps_connection(&trace.result);
        self.ring.window_clean &= kept_connection;
        self.ring.window_responses += 1;

        let meta = if self.ring.closed {
            self.close_ring_window(false)
        } else if self.ring.window_responses >= self.ring.window_depth as u64 {
            self.close_ring_window(true)
        } else {
            BodyLaneTraceMeta {
                batch_complete: false,
                batch_clean: self.ring.window_clean,
                batch_response_count: 0,
                unresolved_count: 0,
                connection_discarded: false,
            }
        };
        Some((trace, meta))
    }

    /// Give up on everything still outstanding. The connection is poisoned:
    /// responses to those commands are still in the socket, so it can never be
    /// handed back to the pool.
    pub fn ring_abandon(&mut self) -> usize {
        let dropped = self.ring.outstanding.len();
        if dropped > 0 || self.ring.unflushed > 0 {
            self.conn.fail_body_pipeline();
        }
        self.ring.outstanding.clear();
        self.ring.unflushed = 0;
        self.ring.closed = true;
        self.ring.window_depth = 0;
        self.ring.window_responses = 0;
        dropped
    }

    fn close_ring_window(&mut self, healthy: bool) -> BodyLaneTraceMeta {
        let meta = BodyLaneTraceMeta {
            batch_complete: true,
            batch_clean: self.ring.window_clean && healthy,
            batch_response_count: self.ring.window_responses,
            unresolved_count: if healthy {
                0
            } else {
                self.ring.outstanding.len() as u64
            },
            connection_discarded: !healthy,
        };
        self.ring.window_depth = 0;
        self.ring.window_responses = 0;
        self.ring.window_clean = true;
        meta
    }

    pub fn park(mut self) {
        // An unread response still in the socket makes QUIT meaningless: the
        // reply read back would be the tail of an article, not the server's.
        if self.conn.body_accounting.is_empty()
            && self.ring.outstanding.is_empty()
            && !self.conn.poisoned
        {
            let _ = self.conn.quit();
        } else {
            self.conn.fail_body_pipeline();
        }
    }

    fn read_decoded_body(
        &mut self,
        message_id: &str,
        estimated_body_bytes: u64,
    ) -> std::result::Result<DecodedBody, DecodedBodyError> {
        let mut budget = ActiveTransferBudget::new(self.soft_timeout);
        self.conn.set_checkpoint_plan(self.checkpoint_plan.clone());
        match self.conn.stream_yenc_article_with_active_budget(
            message_id,
            estimated_body_bytes,
            &mut budget,
        ) {
            Ok(article) => Ok(decoded_body_from_article(article)),
            Err(FusedYencError::Yenc(error)) => {
                Err(DecodedBodyError::Decode { raw_size: 0, error })
            }
            Err(FusedYencError::Nntp(error)) => Err(DecodedBodyError::Nntp(error)),
        }
    }

    fn read_next_decoded_body(&mut self) -> std::result::Result<DecodedBody, DecodedBodyError> {
        let mut budget = ActiveTransferBudget::new(self.soft_timeout);
        self.conn.set_checkpoint_plan(self.checkpoint_plan.clone());
        match self
            .conn
            .stream_next_yenc_article_with_active_budget(&mut budget)
        {
            Ok(article) => Ok(decoded_body_from_article(article)),
            Err(FusedYencError::Yenc(error)) => {
                Err(DecodedBodyError::Decode { raw_size: 0, error })
            }
            Err(FusedYencError::Nntp(error)) => Err(DecodedBodyError::Nntp(error)),
        }
    }

    fn trace_item(
        &self,
        message_id: &str,
        elapsed: Duration,
        result: std::result::Result<DecodedBody, DecodedBodyError>,
    ) -> DecodedBodyTrace {
        let (outcome, error) = match &result {
            Ok(_) | Err(DecodedBodyError::Decode { .. }) => (FetchAttemptOutcome::Success, None),
            Err(DecodedBodyError::Nntp(
                NntpError::ArticleNotFound
                | NntpError::NoSuchArticle { .. }
                | NntpError::NoArticleWithNumber,
            )) => (
                FetchAttemptOutcome::NotFound,
                Some("article not found".to_string()),
            ),
            Err(DecodedBodyError::Nntp(NntpError::QuotaBlocked(_))) => (
                FetchAttemptOutcome::QuotaBlocked,
                Some("server download quota blocked".to_string()),
            ),
            Err(DecodedBodyError::Nntp(NntpError::BodyNotRequestedDueToQuota { .. })) => (
                FetchAttemptOutcome::QuotaUnrequested,
                Some("BODY was not issued after quota rejection".to_string()),
            ),
            Err(DecodedBodyError::Nntp(
                NntpError::AuthenticationFailed
                | NntpError::AuthenticationRejected
                | NntpError::AccessDenied,
            )) => (
                FetchAttemptOutcome::AuthenticationFailure,
                Some("authentication/access failure".to_string()),
            ),
            Err(DecodedBodyError::Nntp(error)) if is_transient(error) => (
                FetchAttemptOutcome::TransientFailure,
                Some(error.to_string()),
            ),
            Err(other) => (
                FetchAttemptOutcome::PermanentFailure,
                Some(format!("{other:?}")),
            ),
        };

        let result = match result {
            Err(DecodedBodyError::Nntp(
                NntpError::ArticleNotFound
                | NntpError::NoSuchArticle { .. }
                | NntpError::NoArticleWithNumber,
            )) => Err(DecodedBodyError::Nntp(NntpError::NoSuchArticle {
                message_id: message_id.to_string(),
            })),
            other => other,
        };

        DecodedBodyTrace {
            attempts: vec![FetchAttemptTrace {
                server_idx: self.server_id.0,
                remote_ip: Some(self.remote_ip),
                elapsed,
                outcome,
                error,
            }],
            result,
        }
    }

    fn observe_latency(&mut self, sample: Duration) {
        self.latency_ewma = Some(blend_ewma(self.latency_ewma, sample));
    }

    fn observe_transfer(&mut self, sample: Duration) {
        self.transfer_ewma = Some(blend_ewma(self.transfer_ewma, sample));
    }
}

fn blend_ewma(current: Option<Duration>, sample: Duration) -> Duration {
    match current {
        Some(current) => current.mul_f64(0.75) + sample.mul_f64(0.25),
        None => sample,
    }
}

impl BlockingNntpConnection {
    /// Declare immutable checkpoint geometry for subsequent decoded articles.
    pub fn set_checkpoint_plan(&mut self, checkpoint_plan: CheckpointPlan) {
        self.checkpoint_plan = checkpoint_plan;
    }

    /// Consume the last article's status-line wait, so a lane cannot credit
    /// one response's latency to the next.
    fn take_response_line_wait(&mut self) -> Duration {
        std::mem::replace(&mut self.last_response_line_wait, Duration::ZERO)
    }

    pub fn connect_with_ip_policy(
        config: &ServerConfig,
        excluded_ips: &[IpAddr],
        address_offset: usize,
    ) -> Result<Self> {
        Self::connect_with_ip_policy_for_group(config, excluded_ips, address_offset, None)
    }

    /// Connect and, on a server known to pipeline, select `initial_group`
    /// inside the session-setup write. An unselectable group is not an
    /// error here; the lane walks its candidate list afterwards.
    pub fn connect_with_ip_policy_for_group(
        config: &ServerConfig,
        excluded_ips: &[IpAddr],
        address_offset: usize,
        initial_group: Option<&str>,
    ) -> Result<Self> {
        Self::connect_with_ip_policy_with_backend(
            config,
            excluded_ips,
            address_offset,
            None,
            initial_group,
        )
    }

    /// `backend_override` bypasses env/platform backend selection; tests use
    /// it to exercise a specific TLS transport deterministically.
    fn connect_with_ip_policy_with_backend(
        config: &ServerConfig,
        excluded_ips: &[IpAddr],
        address_offset: usize,
        backend_override: Option<NntpTlsBackend>,
        initial_group: Option<&str>,
    ) -> Result<Self> {
        if config.starttls {
            return Err(NntpError::MalformedResponse(
                "blocking owned lane does not support STARTTLS".to_string(),
            ));
        }

        let connect_timeout = config.connect_timeout.max(MIN_TIMEOUT);
        let addrs = resolve_addrs(&config.host, config.port, excluded_ips, address_offset)?;
        let mut last_error = None;
        for addr in addrs {
            match TcpStream::connect_timeout(&addr, connect_timeout) {
                Ok(tcp) => {
                    tcp.set_nodelay(true).map_err(NntpError::Io)?;
                    tcp.set_read_timeout(Some(config.command_timeout.max(MIN_TIMEOUT)))
                        .map_err(NntpError::Io)?;
                    tcp.set_write_timeout(Some(config.command_timeout.max(MIN_TIMEOUT)))
                        .map_err(NntpError::Io)?;
                    let remote_addr = tcp.peer_addr().unwrap_or(addr);
                    return Self::from_tcp(
                        config,
                        tcp,
                        remote_addr,
                        backend_override,
                        initial_group,
                    );
                }
                Err(error) => last_error = Some(error),
            }
        }

        Err(NntpError::Io(last_error.unwrap_or_else(|| {
            io::Error::new(io::ErrorKind::NotFound, "no NNTP address resolved")
        })))
    }

    fn from_tcp(
        config: &ServerConfig,
        tcp: TcpStream,
        remote_addr: SocketAddr,
        backend_override: Option<NntpTlsBackend>,
        initial_group: Option<&str>,
    ) -> Result<Self> {
        let transport = if config.tls {
            let backend = if config.tls_name_mismatch_certificate_der.is_some() {
                NntpTlsBackend::ManualRustls
            } else {
                match backend_override {
                    Some(backend) => backend,
                    None => tls_backend_for_preference(
                        selected_blocking_tls_backend()?,
                        config.tls_cipher_preference,
                    ),
                }
            };
            match backend {
                NntpTlsBackend::ManualRustls => {
                    BlockingTransport::Rustls(Box::new(BlockingManualTlsStream::connect(
                        tcp,
                        &config.host,
                        config.tls_ca_cert.as_deref(),
                        config.tls_name_mismatch_certificate_der.as_deref(),
                        config.tls_cipher_preference,
                        config.command_timeout.max(MIN_TIMEOUT),
                    )?))
                }
                #[cfg(not(windows))]
                NntpTlsBackend::S2n => BlockingTransport::S2n(BlockingS2nStream::connect(
                    tcp,
                    &config.host,
                    config.tls_ca_cert.as_deref(),
                    config.command_timeout.max(MIN_TIMEOUT),
                )?),
            }
        } else {
            BlockingTransport::Plain(tcp)
        };

        let read_buf_capacity = config.buffer_profile.read_buf_capacity.max(64 * 1024);
        let mut conn = Self {
            transport,
            codec: NntpCodec::new(),
            read_buf: BytesMut::with_capacity(read_buf_capacity),
            read_scratch: vec![0; config.buffer_profile.socket_read_size.max(64 * 1024)],
            buffer_profile: config.buffer_profile,
            capabilities: match config.pipelining {
                crate::connection::PipeliningCapability::Probe => Capabilities::default(),
                crate::connection::PipeliningCapability::Known(supports) => {
                    Capabilities::from_pipelining(supports)
                }
            },
            host: config.host.clone(),
            port: config.port,
            remote_addr,
            command_timeout: config.command_timeout.max(MIN_TIMEOUT),
            current_group: None,
            credentials: None,
            poisoned: false,
            transfer_control: None,
            body_accounting: VecDeque::new(),
            checkpoint_plan: CheckpointPlan::None,
            last_response_line_wait: Duration::ZERO,
            group_probe_armed: false,
        };

        let greeting = conn.read_response()?;
        debug!(code = greeting.code.raw(), msg = %greeting.message, "received blocking NNTP greeting");
        match greeting.code.raw() {
            200 | 201 => {}
            400 => return Err(NntpError::ServiceUnavailable),
            502 => return Err(NntpError::from_status(greeting.code, &greeting.message)),
            _ => return Err(NntpError::unexpected(greeting.code, &greeting.message)),
        }

        // Session setup: authentication and nothing else, unless this server
        // has proven it needs a selected group. Every command here runs before
        // the lane's first BODY can be asked for, so each one costs a full
        // round trip of the article's time to first byte — see
        // [`crate::server_caps`] for why MODE READER is never sent and GROUP is
        // learned instead of assumed.
        let requires_group =
            crate::server_caps::requires_group_selection(&config.host, config.port);
        let requested_group = initial_group.filter(|_| requires_group);
        if matches!(
            config.pipelining,
            crate::connection::PipeliningCapability::Known(true)
        ) {
            conn.pipelined_session_setup(config, requested_group)?;
        } else {
            if let (Some(user), Some(pass)) = (&config.username, &config.password) {
                let user = user.clone();
                let pass = pass.clone();
                conn.authenticate(&user, &pass)?;
                conn.credentials = Some((user, pass));
            }
            if let Some(group) = requested_group {
                let resp = conn.send_command(&Command::Group(group.to_string()))?;
                if resp.code.is_error() {
                    debug!(
                        code = resp.code.raw(),
                        group, "initial blocking GROUP not selected"
                    );
                } else {
                    conn.current_group = Some(group.to_string());
                }
            }
        }
        if matches!(
            config.pipelining,
            crate::connection::PipeliningCapability::Probe
        ) {
            conn.fetch_capabilities()?;
        }

        // Setup is over: from here the next status line answers the caller's
        // own command, and is the one that can still be about the group this
        // connection chose not to select.
        conn.group_probe_armed = initial_group.is_some() && !requires_group;

        Ok(conn)
    }

    pub fn remote_ip(&self) -> IpAddr {
        self.remote_addr.ip()
    }

    pub fn capabilities(&self) -> &Capabilities {
        &self.capabilities
    }

    pub fn current_group(&self) -> Option<&str> {
        self.current_group.as_deref()
    }

    /// Session setup for a server known to pipeline. AUTHINFO goes first and
    /// on its own: RFC 4643 forbids pipelining it, and a provider that
    /// enforces that answers the whole batch with 480s or drops the
    /// connection. A GROUP this server has proven it needs then leaves in one
    /// flush and is answered in order (RFC 4644) — usually there is nothing at
    /// all to send, which is the point: the lane reaches its first BODY in
    /// four round trips.
    fn pipelined_session_setup(
        &mut self,
        config: &ServerConfig,
        initial_group: Option<&str>,
    ) -> Result<()> {
        if let (Some(user), Some(pass)) = (&config.username, &config.password) {
            let user = user.clone();
            let pass = pass.clone();
            self.authenticate(&user, &pass)?;
            self.credentials = Some((user, pass));
        }

        let Some(group) = initial_group else {
            return Ok(());
        };

        debug!(group, "selecting the group this server insists on");
        self.write_command_frame(&Command::Group(group.to_string()))?;
        self.flush_commands()?;

        let group_resp = self.read_response()?;
        if group_resp.code.is_error() {
            debug!(
                code = group_resp.code.raw(),
                group, "pipelined blocking GROUP not selected"
            );
        } else {
            self.current_group = Some(group.to_string());
        }
        Ok(())
    }

    pub fn stats(&self) -> BlockingLaneStats {
        match &self.transport {
            BlockingTransport::Plain(_) => BlockingLaneStats::default(),
            BlockingTransport::Rustls(inner) => inner.stats,
            #[cfg(not(windows))]
            BlockingTransport::S2n(inner) => inner.stats,
        }
    }

    fn fetch_capabilities(&mut self) -> Result<()> {
        let resp = self.send_command(&Command::Capabilities)?;
        if resp.code.raw() == 101 {
            let data = self.read_multiline_data()?;
            self.capabilities = Capabilities::parse(&data);
            trace!(caps = ?self.capabilities, "parsed blocking NNTP capabilities");
        }
        Ok(())
    }

    fn authenticate(&mut self, username: &str, password: &str) -> Result<()> {
        let user_resp = self.send_command(&Command::AuthInfoUser(username.to_string()))?;
        match user_resp.code.raw() {
            281 => return Ok(()),
            381 => {}
            _ => return Err(NntpError::from_status(user_resp.code, &user_resp.message)),
        }

        let pass_resp = self.send_command(&Command::AuthInfoPass(password.to_string()))?;
        match pass_resp.code.raw() {
            281 => Ok(()),
            481 => Err(NntpError::AuthenticationFailed),
            482 => Err(NntpError::AuthenticationRejected),
            _ => Err(NntpError::from_status(pass_resp.code, &pass_resp.message)),
        }
    }

    fn authenticate_with_active_budget(
        &mut self,
        username: &str,
        password: &str,
        budget: Option<&ActiveTransferBudget>,
    ) -> Result<()> {
        let user_resp = self.send_command_with_active_budget(
            &Command::AuthInfoUser(username.to_string()),
            budget,
        )?;
        match user_resp.code.raw() {
            281 => return Ok(()),
            381 => {}
            _ => return Err(NntpError::from_status(user_resp.code, &user_resp.message)),
        }

        let pass_resp = self.send_command_with_active_budget(
            &Command::AuthInfoPass(password.to_string()),
            budget,
        )?;
        match pass_resp.code.raw() {
            281 => Ok(()),
            481 => Err(NntpError::AuthenticationFailed),
            482 => Err(NntpError::AuthenticationRejected),
            _ => Err(NntpError::from_status(pass_resp.code, &pass_resp.message)),
        }
    }

    fn write_command_frame(&mut self, cmd: &Command) -> Result<()> {
        self.write_command_frame_with_timeout(cmd, self.command_timeout)
    }

    fn write_command_frame_with_timeout(&mut self, cmd: &Command, timeout: Duration) -> Result<()> {
        let encoded = cmd.encode();
        self.transport.write_all(&encoded, timeout)?;
        Ok(())
    }

    fn write_command_frame_with_active_budget(
        &mut self,
        cmd: &Command,
        budget: Option<&ActiveTransferBudget>,
    ) -> Result<()> {
        let (timeout, active_timeout) = active_transfer_read_timeout(self.command_timeout, budget)?;
        self.write_command_frame_with_timeout(cmd, timeout)
            .map_err(|error| classify_active_io_error(error, active_timeout, budget))
    }

    pub fn flush_commands(&mut self) -> Result<()> {
        self.flush_commands_with_timeout(self.command_timeout)
    }

    fn flush_commands_with_timeout(&mut self, timeout: Duration) -> Result<()> {
        self.transport.flush(timeout)
    }

    fn flush_commands_with_active_budget(
        &mut self,
        budget: Option<&ActiveTransferBudget>,
    ) -> Result<()> {
        let (timeout, active_timeout) = active_transfer_read_timeout(self.command_timeout, budget)?;
        self.flush_commands_with_timeout(timeout)
            .map_err(|error| classify_active_io_error(error, active_timeout, budget))
    }

    pub fn send_command(&mut self, cmd: &Command) -> Result<Response> {
        self.write_command_frame(cmd)?;
        self.flush_commands()?;
        self.read_response()
    }

    fn send_command_with_active_budget(
        &mut self,
        cmd: &Command,
        budget: Option<&ActiveTransferBudget>,
    ) -> Result<Response> {
        self.write_command_frame_with_active_budget(cmd, budget)?;
        self.flush_commands_with_active_budget(budget)?;
        self.read_response_with_active_budget(budget)
    }

    pub fn write_body_request(&mut self, message_id: &str) -> Result<()> {
        self.write_body_request_with_estimate(message_id, 0)
    }

    pub fn write_body_request_with_estimate(
        &mut self,
        message_id: &str,
        estimated_body_bytes: u64,
    ) -> Result<()> {
        self.reserve_body(estimated_body_bytes)?;
        let cmd = Command::Body(ArticleId::MessageId(message_id.to_string()));
        if let Err(error) = self.write_command_frame(&cmd) {
            self.body_accounting.pop_back();
            return Err(error);
        }
        Ok(())
    }

    fn set_transfer_control(&mut self, control: Option<Arc<ServerTransferControl>>) {
        debug_assert!(self.body_accounting.is_empty());
        self.transfer_control = control;
    }

    fn reserve_body(&mut self, estimated_body_bytes: u64) -> Result<()> {
        if let Some(control) = &self.transfer_control {
            self.body_accounting.push_back(
                control
                    .start_body(estimated_body_bytes)
                    .map_err(NntpError::quota_blocked)?,
            );
        }
        Ok(())
    }

    fn charge_active_body(&mut self, bytes: usize) -> Duration {
        match self.body_accounting.front_mut() {
            Some(BodyTransferAccounting::Unlimited) => {
                if let Some(control) = &self.transfer_control {
                    control.record_unlimited_body_bytes(bytes);
                }
                Duration::ZERO
            }
            Some(BodyTransferAccounting::Tracked(permit)) => permit.record_blocking(bytes),
            None => Duration::ZERO,
        }
    }

    fn charge_active_body_without_wait(&mut self, bytes: usize) {
        match self.body_accounting.front_mut() {
            Some(BodyTransferAccounting::Unlimited) => {
                if let Some(control) = &self.transfer_control {
                    control.record_unlimited_body_bytes(bytes);
                }
            }
            Some(BodyTransferAccounting::Tracked(permit)) => {
                permit.record_without_wait(bytes);
            }
            None => {}
        }
    }

    fn finish_active_body(&mut self) {
        if let Some(BodyTransferAccounting::Tracked(permit)) = self.body_accounting.pop_front() {
            permit.finish();
        }
    }

    fn abort_active_body(&mut self) {
        self.body_accounting.pop_front();
    }

    fn abort_all_bodies(&mut self) {
        self.body_accounting.clear();
    }

    fn fail_body_pipeline(&mut self) {
        self.poisoned = true;
        self.current_group = None;
        self.abort_all_bodies();
    }

    fn poison_on_soft_timeout(&mut self, error: &NntpError) {
        if matches!(error, NntpError::SoftTimeout(_)) {
            self.poisoned = true;
            self.current_group = None;
        }
    }

    fn read_response(&mut self) -> Result<Response> {
        match self.read_frame()? {
            NntpFrame::Line(line) => {
                let response = parse_response(&line)?;
                self.observe_group_requirement(&response);
                Ok(response)
            }
            NntpFrame::MultiLineData(_) => Err(NntpError::MalformedResponse(
                "expected single-line response, got multi-line data".into(),
            )),
        }
    }

    fn read_response_with_active_budget(
        &mut self,
        budget: Option<&ActiveTransferBudget>,
    ) -> Result<Response> {
        match self.read_frame_with_active_budget(budget)? {
            NntpFrame::Line(line) => {
                let response = parse_response(&line)?;
                self.observe_group_requirement(&response);
                Ok(response)
            }
            NntpFrame::MultiLineData(_) => Err(NntpError::MalformedResponse(
                "expected single-line response, got multi-line data".into(),
            )),
        }
    }

    /// Learn, from the first response after session setup, whether this server
    /// insists on a selected group the connection did not select.
    ///
    /// 412 is the only code that can mean this, and only on a connection that
    /// was offered a group and declined to spend the round trip on it. The
    /// connection is poisoned rather than repaired in place: the caller's
    /// command has already been refused, and a pipelined batch may have more
    /// refusals behind it. Discarding the socket lets the ordinary retry open
    /// a fresh one, which now selects the group — so only the first connection
    /// to such a server pays for the discovery.
    fn observe_group_requirement(&mut self, response: &Response) {
        if !std::mem::take(&mut self.group_probe_armed) {
            return;
        }
        if response.code.raw() != 412 {
            return;
        }
        if crate::server_caps::note_group_required(&self.host, self.port) {
            warn!(
                host = %self.host,
                port = self.port,
                "server refuses message-id fetches without a selected group; \
                 later connections will select one"
            );
        }
        self.poisoned = true;
    }

    /// Whether this server has proven it refuses message-id fetches without a
    /// selected group. Lanes skip the GROUP round trip unless it has.
    pub fn needs_group_prologue(&self) -> bool {
        crate::server_caps::requires_group_selection(&self.host, self.port)
    }

    fn read_frame(&mut self) -> Result<NntpFrame> {
        loop {
            if let Some(frame) = self.codec.decode(&mut self.read_buf)? {
                return Ok(frame);
            }
            self.read_into_buffer()?;
        }
    }

    fn read_frame_with_active_budget(
        &mut self,
        budget: Option<&ActiveTransferBudget>,
    ) -> Result<NntpFrame> {
        loop {
            if let Some(budget) = budget
                && budget.remaining().is_zero()
            {
                return Err(active_transfer_timeout(budget));
            }
            if let Some(frame) = self.codec.decode(&mut self.read_buf)? {
                return Ok(frame);
            }
            let (timeout, active_timeout) =
                active_transfer_read_timeout(self.command_timeout, budget)?;
            if let Err(error) = self.read_into_buffer_with_stats_timeout(timeout) {
                return Err(classify_active_io_error(error, active_timeout, budget));
            }
        }
    }

    fn read_multiline_data(&mut self) -> Result<bytes::Bytes> {
        self.codec.set_multiline(true);
        self.codec.set_raw_multiline(false);
        let frame = self.read_frame();
        self.codec.set_multiline(false);
        self.codec.set_raw_multiline(false);
        match frame {
            Ok(NntpFrame::MultiLineData(data)) => Ok(data.freeze()),
            Ok(NntpFrame::Line(line)) => Err(NntpError::MalformedResponse(format!(
                "expected multi-line data, got line: {line:?}"
            ))),
            Err(err) => Err(err),
        }
    }

    /// Existence for a batch of message-ids, on this already-open connection.
    ///
    /// This is what lets the health probe ride a warm owned lane instead of
    /// prising a connection permit away from one and dialling its own socket:
    /// that dial is TCP, TLS, greeting and AUTHINFO — about four and a half
    /// round trips — paid once per probe batch, which at a hundred milliseconds
    /// of distance dwarfs the batch itself.
    ///
    /// STAT leaves in a single pipelined write where the server supports it,
    /// so the batch costs one round trip rather than one per article. Every
    /// article STAT calls missing is then re-asked with HEAD, also in one
    /// write, because a provider's STAT index can lag its spool. A server that
    /// implements neither command cannot settle the batch, and says so with
    /// [`NntpError::CommandNotRecognized`] rather than a list of false
    /// missing verdicts.
    pub fn probe_exists(&mut self, message_ids: &[String]) -> Result<Vec<bool>> {
        if message_ids.is_empty() {
            return Ok(Vec::new());
        }

        let mut stat_verdict = None;
        if crate::server_caps::supports_stat(&self.host, self.port) {
            match self.stat_batch(message_ids) {
                Ok(results) => stat_verdict = Some(results),
                // The server has just retired STAT, which `stat_batch` has
                // recorded. HEAD alone has to settle the batch now.
                Err(error) if crate::connection::reports_unsupported_command(&error) => {}
                Err(error) => return Err(error),
            }
        }

        let head_supported = crate::server_caps::supports_head(&self.host, self.port);
        let have_stat = stat_verdict.is_some();
        let mut exists = match (stat_verdict, head_supported) {
            (Some(results), _) => results,
            (None, true) => vec![false; message_ids.len()],
            (None, false) => return Err(NntpError::CommandNotRecognized),
        };
        if !head_supported {
            return Ok(exists);
        }

        let misses: Vec<&str> = message_ids
            .iter()
            .zip(&exists)
            .filter_map(|(id, found)| (!found).then_some(id.as_str()))
            .collect();
        if misses.is_empty() {
            return Ok(exists);
        }

        let confirmed = match self.head_batch(&misses) {
            Ok(confirmed) => confirmed,
            // HEAD is gone too. If STAT answered, its verdict is the final
            // one; if it did not, nothing here can settle the batch.
            Err(error) if crate::connection::reports_unsupported_command(&error) => {
                return if have_stat {
                    Ok(exists)
                } else {
                    Err(NntpError::CommandNotRecognized)
                };
            }
            Err(error) => return Err(error),
        };

        let mut confirmed = confirmed.into_iter();
        for found in exists.iter_mut() {
            if !*found && confirmed.next() == Some(true) {
                *found = true;
            }
        }
        Ok(exists)
    }

    /// One pipelined STAT batch. Every response is read even after a refusal,
    /// so the socket is left exactly where the next command expects it.
    fn stat_batch(&mut self, message_ids: &[String]) -> Result<Vec<bool>> {
        let pipelined = self.capabilities.supports_pipelining();
        if pipelined {
            self.write_probe_batch(message_ids.iter().map(String::as_str), Command::Stat)?;
        }

        let mut results = Vec::with_capacity(message_ids.len());
        let mut refusal = None;
        for message_id in message_ids {
            if !pipelined {
                self.write_probe_batch(std::iter::once(message_id.as_str()), Command::Stat)?;
            }
            let response = self.read_response()?;
            match self.classify_stat_response(&response) {
                Ok(exists) => results.push(exists),
                Err(error) => {
                    results.push(false);
                    let _ = refusal.get_or_insert(error);
                }
            }
        }
        match refusal {
            Some(error) => Err(error),
            None => Ok(results),
        }
    }

    /// One pipelined HEAD batch, used to second-guess a STAT miss.
    ///
    /// A 221 carries headers that have to be drained before the next status
    /// line can be read, so the multi-line body is consumed and discarded in
    /// place; only its arrival matters.
    fn head_batch(&mut self, message_ids: &[&str]) -> Result<Vec<bool>> {
        let pipelined = self.capabilities.supports_pipelining();
        if pipelined {
            self.write_probe_batch(message_ids.iter().copied(), Command::Head)?;
        }

        let mut results = Vec::with_capacity(message_ids.len());
        let mut refusal = None;
        for message_id in message_ids {
            if !pipelined {
                self.write_probe_batch(std::iter::once(*message_id), Command::Head)?;
            }
            let response = self.read_response()?;
            match response.code.raw() {
                221 => {
                    self.read_multiline_data()?;
                    results.push(true);
                }
                430 | 423 => results.push(false),
                code if crate::server_caps::is_command_unsupported(code) => {
                    if crate::server_caps::note_head_unsupported(&self.host, self.port) {
                        debug!(host = %self.host, port = self.port, "server does not implement HEAD");
                    }
                    results.push(false);
                    let _ = refusal.get_or_insert(NntpError::CommandNotRecognized);
                }
                _ => {
                    results.push(false);
                    let _ = refusal
                        .get_or_insert(NntpError::from_status(response.code, &response.message));
                }
            }
        }
        match refusal {
            Some(error) => Err(error),
            None => Ok(results),
        }
    }

    /// Write one probe command per id and flush them together.
    ///
    /// A half-written batch leaves the peer expecting bytes that will never
    /// arrive, so any failure here poisons the connection rather than letting
    /// the lane read a reply to a command it did not finish sending.
    fn write_probe_batch<'a>(
        &mut self,
        message_ids: impl Iterator<Item = &'a str>,
        command: fn(ArticleId) -> Command,
    ) -> Result<()> {
        for message_id in message_ids {
            let cmd = command(ArticleId::MessageId(message_id.to_string()));
            if let Err(error) = self.write_command_frame(&cmd) {
                self.fail_body_pipeline();
                return Err(error);
            }
        }
        if let Err(error) = self.flush_commands() {
            self.fail_body_pipeline();
            return Err(error);
        }
        Ok(())
    }

    /// Turn one STAT status line into an existence verdict.
    ///
    /// A 500/501 is the server saying it does not implement STAT. That is an
    /// answer about the command, not a fault on the socket: it is recorded so
    /// the probe switches to HEAD, and the connection stays healthy.
    fn classify_stat_response(&mut self, response: &Response) -> Result<bool> {
        match response.code.raw() {
            223 => Ok(true),
            430 | 423 => Ok(false),
            code if crate::server_caps::is_command_unsupported(code) => {
                if crate::server_caps::note_stat_unsupported(&self.host, self.port) {
                    debug!(host = %self.host, port = self.port, "server does not implement STAT");
                }
                Err(NntpError::CommandNotRecognized)
            }
            _ => Err(NntpError::from_status(response.code, &response.message)),
        }
    }

    pub fn select_group(&mut self, group: &str) -> Result<()> {
        if self.current_group.as_deref() == Some(group) {
            return Ok(());
        }
        let response = self.send_command(&Command::Group(group.to_string()))?;
        if response.code.raw() == 480 {
            if let Some((user, pass)) = self.credentials.clone() {
                self.authenticate(&user, &pass)?;
                self.current_group = None;
                let retry = self.send_command(&Command::Group(group.to_string()))?;
                if retry.code.is_error() {
                    return Err(NntpError::from_status(retry.code, &retry.message));
                }
                self.current_group = Some(group.to_string());
                return Ok(());
            }
            return Err(NntpError::AuthenticationRequired);
        }
        if response.code.is_error() {
            return Err(NntpError::from_status(response.code, &response.message));
        }
        self.current_group = Some(group.to_string());
        Ok(())
    }

    pub fn stream_yenc_article(
        &mut self,
        message_id: &str,
    ) -> std::result::Result<FusedYencArticle, FusedYencError> {
        self.stream_yenc_article_with_estimate(message_id, 0)
    }

    /// [`Self::stream_yenc_article`], calling `on_chunk` with each decoded
    /// batch as it is produced rather than only once the article is complete.
    ///
    /// The batches handed to `on_chunk` are exactly the ones that end up in
    /// [`FusedYencArticle::chunks`], in the same order, so a caller can hash or
    /// write incrementally and still fall back to the buffered article.
    pub fn stream_yenc_article_with_chunks<F>(
        &mut self,
        message_id: &str,
        on_chunk: F,
    ) -> std::result::Result<FusedYencArticle, FusedYencError>
    where
        F: FnMut(&[u8]) -> Result<()>,
    {
        self.stream_yenc_article_with_estimate_inner(message_id, 0, None, on_chunk)
    }

    pub fn stream_yenc_article_with_estimate(
        &mut self,
        message_id: &str,
        estimated_body_bytes: u64,
    ) -> std::result::Result<FusedYencArticle, FusedYencError> {
        self.stream_yenc_article_with_estimate_inner(message_id, estimated_body_bytes, None, |_| {
            Ok(())
        })
    }

    fn stream_yenc_article_with_active_budget(
        &mut self,
        message_id: &str,
        estimated_body_bytes: u64,
        budget: &mut ActiveTransferBudget,
    ) -> std::result::Result<FusedYencArticle, FusedYencError> {
        self.stream_yenc_article_with_estimate_inner(
            message_id,
            estimated_body_bytes,
            Some(budget),
            |_| Ok(()),
        )
    }

    fn stream_yenc_article_with_estimate_inner<F>(
        &mut self,
        message_id: &str,
        estimated_body_bytes: u64,
        budget: Option<&mut ActiveTransferBudget>,
        on_chunk: F,
    ) -> std::result::Result<FusedYencArticle, FusedYencError>
    where
        F: FnMut(&[u8]) -> Result<()>,
    {
        self.reserve_body(estimated_body_bytes)?;
        let cmd = Command::Body(ArticleId::MessageId(message_id.to_string()));
        let request_started = Instant::now();
        let initial = match self.send_command_with_active_budget(&cmd, budget.as_deref()) {
            Ok(initial) => initial,
            Err(error) => {
                self.poison_on_soft_timeout(&error);
                self.abort_active_body();
                return Err(error.into());
            }
        };
        let initial = if initial.code.raw() == 480 {
            if let Some((user, pass)) = self.credentials.clone() {
                if let Err(error) =
                    self.authenticate_with_active_budget(&user, &pass, budget.as_deref())
                {
                    self.poison_on_soft_timeout(&error);
                    self.abort_active_body();
                    return Err(error.into());
                }
                self.current_group = None;
                match self.send_command_with_active_budget(&cmd, budget.as_deref()) {
                    Ok(initial) => initial,
                    Err(error) => {
                        self.poison_on_soft_timeout(&error);
                        self.abort_active_body();
                        return Err(error.into());
                    }
                }
            } else {
                self.abort_active_body();
                return Err(NntpError::AuthenticationRequired.into());
            }
        } else {
            initial
        };
        self.last_response_line_wait = request_started.elapsed();
        self.stream_yenc_article_response(initial, budget, on_chunk)
    }

    pub fn stream_next_yenc_article(
        &mut self,
    ) -> std::result::Result<FusedYencArticle, FusedYencError> {
        self.stream_next_yenc_article_inner(None, |_| Ok(()))
    }

    /// [`Self::stream_next_yenc_article`] with per-batch delivery; see
    /// [`Self::stream_yenc_article_with_chunks`].
    pub fn stream_next_yenc_article_with_chunks<F>(
        &mut self,
        on_chunk: F,
    ) -> std::result::Result<FusedYencArticle, FusedYencError>
    where
        F: FnMut(&[u8]) -> Result<()>,
    {
        self.stream_next_yenc_article_inner(None, on_chunk)
    }

    fn stream_next_yenc_article_with_active_budget(
        &mut self,
        budget: &mut ActiveTransferBudget,
    ) -> std::result::Result<FusedYencArticle, FusedYencError> {
        self.stream_next_yenc_article_inner(Some(budget), |_| Ok(()))
    }

    fn stream_next_yenc_article_inner<F>(
        &mut self,
        budget: Option<&mut ActiveTransferBudget>,
        on_chunk: F,
    ) -> std::result::Result<FusedYencArticle, FusedYencError>
    where
        F: FnMut(&[u8]) -> Result<()>,
    {
        let response_started = Instant::now();
        let initial = match self.read_response_with_active_budget(budget.as_deref()) {
            Ok(initial) => initial,
            Err(error) => {
                self.fail_body_pipeline();
                return Err(error.into());
            }
        };
        if initial.code.raw() == 480 {
            self.fail_body_pipeline();
            return Err(NntpError::AuthenticationRequired.into());
        }
        self.last_response_line_wait = response_started.elapsed();
        self.stream_yenc_article_response(initial, budget, on_chunk)
    }

    fn stream_yenc_article_response<F>(
        &mut self,
        initial: Response,
        mut budget: Option<&mut ActiveTransferBudget>,
        mut on_chunk: F,
    ) -> std::result::Result<FusedYencArticle, FusedYencError>
    where
        F: FnMut(&[u8]) -> Result<()>,
    {
        let mut decoder = match FusedYencArticleDecoder::from_body_response(initial) {
            Ok(decoder) => decoder,
            Err(error) => {
                if matches!(
                    &error,
                    FusedYencError::Nntp(
                        NntpError::ArticleNotFound
                            | NntpError::NoSuchArticle { .. }
                            | NntpError::NoArticleWithNumber
                    )
                ) {
                    self.abort_active_body();
                } else {
                    self.fail_body_pipeline();
                }
                return Err(error);
            }
        };
        let profile_cpu = profile_cpu_timings_enabled();
        decoder.set_profile_cpu(profile_cpu);
        decoder.set_checkpoint_plan(self.checkpoint_plan.clone());
        let mut read_calls = 0u64;
        let mut read_bytes = 0u64;
        let mut transport_read = TransportReadStats::default();
        let mut article_chunks = Vec::new();
        let mut throttle_wait = Duration::ZERO;
        let mut output_callback_cpu = Duration::ZERO;

        loop {
            let payload_before = decoder.body_payload_bytes_consumed();
            let decoded = decoder.decode_available(&mut self.read_buf);
            let payload_delta = decoder
                .body_payload_bytes_consumed()
                .saturating_sub(payload_before);
            if let Some(budget) = budget.as_deref()
                && budget.remaining().is_zero()
            {
                self.charge_active_body_without_wait(payload_delta as usize);
                let error = active_transfer_timeout(budget);
                self.fail_body_pipeline();
                return Err(error.into());
            }
            let waited = self.charge_active_body(payload_delta as usize);
            throttle_wait = throttle_wait.saturating_add(waited);
            if let Some(budget) = budget.as_deref_mut() {
                budget.exclude_wait(waited);
            }
            if let Some(budget) = budget.as_deref()
                && budget.remaining().is_zero()
            {
                let error = active_transfer_timeout(budget);
                self.fail_body_pipeline();
                return Err(error.into());
            }
            match decoded {
                Err(error) => {
                    self.fail_body_pipeline();
                    return Err(error);
                }
                Ok(Some(mut article)) => {
                    let chunks = std::mem::take(&mut article.chunks);
                    if let Err(error) = crate::connection::deliver_fused_output_chunks(
                        chunks,
                        &mut article_chunks,
                        &mut on_chunk,
                        &mut output_callback_cpu,
                        profile_cpu,
                    ) {
                        self.fail_body_pipeline();
                        return Err(error);
                    }
                    article.stats.read_calls = read_calls;
                    article.stats.read_bytes = read_bytes;
                    article.stats.transport_read = transport_read;
                    article.stats.throttle_wait = throttle_wait;
                    article.stats.leftover_bytes_after_terminator = self.read_buf.len() as u64;
                    article.stats.output_batches = article_chunks.len() as u64;
                    article.stats.output_callback_cpu = output_callback_cpu;
                    article.chunks = article_chunks;
                    if let Some(lane_stats) = self.transport.lane_stats_mut() {
                        lane_stats.body_responses += 1;
                        lane_stats.decoded_articles += 1;
                    }
                    self.finish_active_body();
                    return Ok(article);
                }
                Ok(None) => {
                    if let Err(error) = crate::connection::deliver_fused_output_chunks(
                        decoder.drain_output_chunks(),
                        &mut article_chunks,
                        &mut on_chunk,
                        &mut output_callback_cpu,
                        profile_cpu,
                    ) {
                        self.fail_body_pipeline();
                        return Err(error);
                    }
                }
            }

            let (read_timeout, active_timeout) =
                match active_transfer_read_timeout(self.command_timeout, budget.as_deref()) {
                    Ok(timeout) => timeout,
                    Err(error) => {
                        self.fail_body_pipeline();
                        return Err(error.into());
                    }
                };
            let (n, stats) = match self.read_into_buffer_with_stats_timeout(read_timeout) {
                Ok(read) => read,
                Err(error) => {
                    let error = classify_active_io_error(error, active_timeout, budget.as_deref());
                    self.fail_body_pipeline();
                    return Err(error.into());
                }
            };
            read_calls += 1;
            read_bytes += n as u64;
            transport_read.add(stats);
        }
    }

    pub fn quit(&mut self) -> Result<()> {
        let _ = self.send_command(&Command::Quit);
        Ok(())
    }

    fn read_into_buffer(&mut self) -> Result<usize> {
        let (bytes, _) = self.read_into_buffer_with_stats()?;
        Ok(bytes)
    }

    fn read_into_buffer_with_stats(&mut self) -> Result<(usize, TransportReadStats)> {
        self.read_into_buffer_with_stats_timeout(self.command_timeout)
    }

    fn read_into_buffer_with_stats_timeout(
        &mut self,
        timeout: Duration,
    ) -> Result<(usize, TransportReadStats)> {
        let socket_read_size = self.buffer_profile.socket_read_size.max(64 * 1024);
        if self.read_scratch.len() < socket_read_size {
            self.read_scratch.resize(socket_read_size, 0);
        }
        let (n, stats) = self
            .transport
            .read_into_buf(
                &mut self.read_buf,
                &mut self.read_scratch,
                socket_read_size,
                timeout,
            )
            .map_err(|error| {
                self.poisoned = true;
                self.current_group = None;
                NntpError::Io(error)
            })?;
        if n == 0 {
            self.poisoned = true;
            self.current_group = None;
            return Err(NntpError::ConnectionClosed);
        }
        Ok((n, stats))
    }
}

impl BlockingTransport {
    fn read_into_buf(
        &mut self,
        dst: &mut BytesMut,
        scratch: &mut [u8],
        target_read_size: usize,
        timeout: Duration,
    ) -> io::Result<(usize, TransportReadStats)> {
        match self {
            BlockingTransport::Plain(tcp) => {
                tcp.set_read_timeout(Some(timeout))?;
                let read_size = target_read_size.min(scratch.len()).max(1);
                let n = tcp.read(&mut scratch[..read_size])?;
                dst.extend_from_slice(&scratch[..n]);
                Ok((
                    n,
                    TransportReadStats {
                        try_read_calls: 1,
                        try_read_bytes: n as u64,
                        plaintext_bytes: n as u64,
                        ..TransportReadStats::default()
                    },
                ))
            }
            BlockingTransport::Rustls(inner) => inner.read_into_buf(dst, target_read_size, timeout),
            #[cfg(not(windows))]
            BlockingTransport::S2n(inner) => inner.read_into_buf(dst, target_read_size, timeout),
        }
    }

    fn write_all(&mut self, bytes: &[u8], timeout: Duration) -> Result<()> {
        match self {
            BlockingTransport::Plain(tcp) => {
                tcp.set_write_timeout(Some(timeout))
                    .map_err(NntpError::Io)?;
                tcp.write_all(bytes).map_err(NntpError::Io)
            }
            BlockingTransport::Rustls(inner) => inner.write_all(bytes, timeout),
            #[cfg(not(windows))]
            BlockingTransport::S2n(inner) => inner.write_all(bytes, timeout),
        }
    }

    fn flush(&mut self, timeout: Duration) -> Result<()> {
        match self {
            BlockingTransport::Plain(tcp) => {
                tcp.set_write_timeout(Some(timeout))
                    .map_err(NntpError::Io)?;
                tcp.flush().map_err(NntpError::Io)
            }
            BlockingTransport::Rustls(inner) => inner.flush(timeout),
            #[cfg(not(windows))]
            BlockingTransport::S2n(inner) => inner.flush(timeout),
        }
    }

    fn lane_stats_mut(&mut self) -> Option<&mut BlockingLaneStats> {
        match self {
            BlockingTransport::Plain(_) => None,
            BlockingTransport::Rustls(inner) => Some(&mut inner.stats),
            #[cfg(not(windows))]
            BlockingTransport::S2n(inner) => Some(&mut inner.stats),
        }
    }
}

impl BlockingManualTlsStream {
    fn connect(
        tcp: TcpStream,
        host: &str,
        ca_cert_path: Option<&std::path::Path>,
        adopted_name_mismatch_certificate_der: Option<&[u8]>,
        cipher_preference: TlsCipherPreference,
        timeout: Duration,
    ) -> Result<Self> {
        tcp.set_read_timeout(Some(timeout)).map_err(NntpError::Io)?;
        tcp.set_write_timeout(Some(timeout))
            .map_err(NntpError::Io)?;
        tcp.set_nonblocking(false).map_err(NntpError::Io)?;

        let config = build_tls_config_with_name_mismatch_certificate(
            ca_cert_path,
            adopted_name_mismatch_certificate_der,
            cipher_preference,
        )?;
        let server_name = make_server_name(host)?;
        let session = RustlsSession::new(config, server_name)?;
        let mut stream = Self {
            tcp,
            session,
            read_buffer: vec![0u8; TLS_READ_BUFFER],
            stats: BlockingLaneStats::default(),
        };
        stream.complete_handshake().map_err(NntpError::Io)?;
        Ok(stream)
    }

    fn complete_handshake(&mut self) -> io::Result<()> {
        let mut discarded = BytesMut::new();

        while self.session.is_handshaking() {
            self.flush_outbound()?;

            if self.session.is_handshaking() {
                let n = self.tcp.read(&mut self.read_buffer)?;
                if n == 0 {
                    return Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "server closed during TLS handshake",
                    ));
                }
                self.session
                    .feed_ciphertext_slice(&self.read_buffer[..n], &mut discarded, None)?;
            }
        }

        self.flush_outbound()
    }

    fn flush_outbound(&mut self) -> io::Result<()> {
        while let Some(outbound) = self.session.next_outbound()? {
            self.tcp.write_all(&outbound)?;
            self.stats.socket_writes += 1;
        }
        self.tcp.flush()
    }

    fn read_into_buf(
        &mut self,
        dst: &mut BytesMut,
        target_read_size: usize,
        timeout: Duration,
    ) -> io::Result<(usize, TransportReadStats)> {
        let mut stats = TransportReadStats::default();
        let drained = self.session.drain_plaintext(dst, Some(&mut stats))?;
        if drained > 0 {
            stats.cached_plaintext_returns += 1;
            return Ok((drained, stats));
        }

        self.tcp.set_read_timeout(Some(timeout))?;
        let read_size = target_read_size.max(TLS_READ_BUFFER);
        if self.read_buffer.len() < read_size {
            self.read_buffer.resize(read_size, 0);
        }

        let started = Instant::now();
        loop {
            let n = self.tcp.read(&mut self.read_buffer[..read_size])?;
            self.stats.socket_reads += 1;
            stats.try_read_calls += 1;
            if n == 0 {
                stats.backend_zero_returns += 1;
                return Ok((0, stats));
            }
            stats.try_read_bytes += n as u64;
            self.stats.tls_recv_calls += 1;
            let produced = self.session.feed_ciphertext_slice(
                &self.read_buffer[..n],
                dst,
                Some(&mut stats),
            )?;
            if produced > 0 {
                stats.backend_recv_calls += 1;
                stats.backend_recv_bytes += produced as u64;
                return Ok((produced, stats));
            }
            // Partial record buffered; keep reading. Each read is bounded by
            // the socket timeout, this guard bounds pathological trickle.
            stats.backend_pending_empty_returns += 1;
            if started.elapsed() >= timeout {
                return Err(io::Error::new(
                    io::ErrorKind::TimedOut,
                    "TLS record incomplete before read timeout",
                ));
            }
        }
    }

    fn write_all(&mut self, bytes: &[u8], timeout: Duration) -> Result<()> {
        self.tcp
            .set_write_timeout(Some(timeout))
            .map_err(NntpError::Io)?;
        self.session
            .buffer_plaintext(bytes)
            .map_err(NntpError::Io)?;
        self.stats.tls_send_calls += 1;
        self.flush_outbound().map_err(NntpError::Io)
    }

    fn flush(&mut self, timeout: Duration) -> Result<()> {
        self.tcp
            .set_write_timeout(Some(timeout))
            .map_err(NntpError::Io)?;
        self.flush_outbound().map_err(NntpError::Io)
    }
}

#[cfg(not(windows))]
impl BlockingS2nStream {
    fn connect(
        tcp: TcpStream,
        host: &str,
        ca_cert_path: Option<&std::path::Path>,
        timeout: Duration,
    ) -> Result<Self> {
        // Keep the direct S2N fd blocking for the throughput path, but wake a
        // stalled syscall often enough for the elapsed operation deadline to
        // be checked without reconfiguring socket options on every I/O call.
        let io_slice = timeout.min(S2N_BLOCKING_IO_SLICE);
        tcp.set_read_timeout(Some(io_slice))
            .map_err(NntpError::Io)?;
        tcp.set_write_timeout(Some(io_slice))
            .map_err(NntpError::Io)?;
        tcp.set_nonblocking(false).map_err(NntpError::Io)?;

        let config = RawS2nConfig::new(ca_cert_path)?;
        let conn = RawS2nConnection::new_client()?;

        let mut stream = Self {
            conn,
            _config: config,
            tcp,
            stats: BlockingLaneStats::default(),
        };
        stream.configure(host)?;
        stream.negotiate(timeout)?;
        Ok(stream)
    }

    fn configure(&mut self, host: &str) -> Result<()> {
        let server_name = CString::new(host).map_err(|_| {
            NntpError::MalformedResponse(format!("invalid s2n server name contains NUL: {host:?}"))
        })?;
        check_s2n_status("set config", unsafe {
            s2n::s2n_connection_set_config(self.conn.as_ptr(), self._config.as_ptr())
        })?;
        check_s2n_status("set server name", unsafe {
            s2n::s2n_set_server_name(self.conn.as_ptr(), server_name.as_ptr())
        })?;
        check_s2n_status("prefer throughput", unsafe {
            s2n::s2n_connection_prefer_throughput(self.conn.as_ptr())
        })?;
        check_s2n_status("configure receive buffering", unsafe {
            s2n::s2n_connection_set_recv_buffering(self.conn.as_ptr(), true)
        })?;
        check_s2n_status("set blinding", unsafe {
            s2n::s2n_connection_set_blinding(
                self.conn.as_ptr(),
                s2n::s2n_blinding::SELF_SERVICE_BLINDING,
            )
        })?;
        check_s2n_status("set fd", unsafe {
            s2n::s2n_connection_set_fd(self.conn.as_ptr(), self.tcp.as_raw_fd())
        })?;
        Ok(())
    }

    fn negotiate(&mut self, timeout: Duration) -> Result<()> {
        let started = Instant::now();
        loop {
            let mut blocked = s2n::s2n_blocked_status::NOT_BLOCKED;
            let rc = unsafe { s2n::s2n_negotiate(self.conn.as_ptr(), &mut blocked) };
            if rc >= 0 {
                let _ = unsafe { s2n::s2n_connection_free_handshake(self.conn.as_ptr()) };
                return Ok(());
            }
            if s2n_retryable_blocked(blocked) && started.elapsed() < timeout {
                continue;
            }
            return Err(s2n_last_error("handshake", blocked));
        }
    }

    fn read_into_buf(
        &mut self,
        dst: &mut BytesMut,
        target_read_size: usize,
        timeout: Duration,
    ) -> io::Result<(usize, TransportReadStats)> {
        self.read_buffered_pull_direct(dst, target_read_size, timeout)
    }

    fn read_buffered_pull_direct(
        &mut self,
        dst: &mut BytesMut,
        target_read_size: usize,
        timeout: Duration,
    ) -> io::Result<(usize, TransportReadStats)> {
        let target = target_read_size.max(1);
        dst.reserve(target);

        let started = Instant::now();
        let mut total = 0usize;
        let mut stats = TransportReadStats::default();
        loop {
            if total >= target {
                stats.backend_target_full_returns += 1;
                break;
            }

            let want = target - total;
            dst.reserve(want);
            let writable = dst.chunk_mut();
            let writable_len = writable.len().min(want);
            if writable_len == 0 {
                stats.backend_target_full_returns += 1;
                break;
            }

            let mut blocked = s2n::s2n_blocked_status::NOT_BLOCKED;
            let n = unsafe {
                s2n::s2n_recv(
                    self.conn.as_ptr(),
                    writable.as_mut_ptr().cast(),
                    writable_len as isize,
                    &mut blocked,
                )
            };
            if n > 0 {
                let n = n as usize;
                unsafe { dst.advance_mut(n) };
                total += n;
                self.stats.tls_recv_calls += 1;
                stats.backend_recv_calls += 1;
                stats.backend_recv_bytes += n as u64;
                stats.plaintext_bytes += n as u64;

                if self.peek_plaintext() > 0 || self.peek_buffered_ciphertext() > 0 {
                    continue;
                }
                break;
            }
            if n == 0 {
                stats.backend_zero_returns += 1;
                break;
            }
            if total == 0 {
                stats.backend_pending_empty_returns += 1;
            } else {
                stats.backend_pending_after_bytes_returns += 1;
            }
            if s2n_retryable_blocked(blocked) {
                if total > 0 {
                    break;
                }
                if self.peek_plaintext() > 0 || self.peek_buffered_ciphertext() > 0 {
                    continue;
                }
                if started.elapsed() < timeout {
                    continue;
                }
            }
            return Err(nntp_error_to_io(s2n_last_error("recv", blocked)));
        }

        Ok((total, stats))
    }

    fn peek_plaintext(&self) -> u32 {
        unsafe { s2n::s2n_peek(self.conn.as_ptr()) }
    }

    fn peek_buffered_ciphertext(&self) -> u32 {
        unsafe { s2n::s2n_peek_buffered(self.conn.as_ptr()) }
    }

    fn write_all(&mut self, mut bytes: &[u8], timeout: Duration) -> Result<()> {
        let started = Instant::now();
        while !bytes.is_empty() {
            let mut blocked = s2n::s2n_blocked_status::NOT_BLOCKED;
            let n = unsafe {
                s2n::s2n_send(
                    self.conn.as_ptr(),
                    bytes.as_ptr().cast(),
                    bytes.len() as isize,
                    &mut blocked,
                )
            };
            if n > 0 {
                self.stats.tls_send_calls += 1;
                bytes = &bytes[n as usize..];
                continue;
            }
            if n == 0 {
                return Err(NntpError::Io(io::Error::new(
                    io::ErrorKind::WriteZero,
                    "s2n write returned zero",
                )));
            }
            if s2n_retryable_blocked(blocked) && started.elapsed() < timeout {
                continue;
            }
            return Err(s2n_last_error("send", blocked));
        }
        self.flush(timeout)
    }

    fn flush(&mut self, timeout: Duration) -> Result<()> {
        let started = Instant::now();
        loop {
            let mut blocked = s2n::s2n_blocked_status::NOT_BLOCKED;
            let rc = unsafe { s2n::s2n_flush(self.conn.as_ptr(), &mut blocked) };
            if rc >= 0 {
                return Ok(());
            }
            if s2n_retryable_blocked(blocked) && started.elapsed() < timeout {
                continue;
            }
            return Err(s2n_last_error("flush", blocked));
        }
    }
}

#[cfg(not(windows))]
impl RawS2nConfig {
    fn new(ca_cert_path: Option<&std::path::Path>) -> Result<Self> {
        s2n_tls::init::init();
        let ca_cert_path = ca_cert_path.ok_or_else(|| {
            NntpError::MalformedResponse(
                "blocking s2n currently requires a CA PEM path for deterministic NNTP trust"
                    .to_string(),
            )
        })?;
        let pem_data = std::fs::read(ca_cert_path).map_err(|error| {
            NntpError::MalformedResponse(format!(
                "failed to read blocking s2n CA PEM {}: {error}",
                ca_cert_path.display()
            ))
        })?;
        let pem = CString::new(pem_data).map_err(|_| {
            NntpError::MalformedResponse(format!(
                "blocking s2n CA PEM {} contains NUL",
                ca_cert_path.display()
            ))
        })?;
        let policy = CString::new("default_tls13").expect("static s2n policy has no NUL");
        let ptr = NonNull::new(unsafe { s2n::s2n_config_new_minimal() }).ok_or_else(|| {
            NntpError::MalformedResponse("blocking s2n failed to allocate config".to_string())
        })?;
        let config = Self { ptr };
        check_s2n_status("set TLS 1.3 security policy", unsafe {
            s2n::s2n_config_set_cipher_preferences(config.as_ptr(), policy.as_ptr())
        })?;
        check_s2n_status("load CA PEM", unsafe {
            s2n::s2n_config_add_pem_to_trust_store(config.as_ptr(), pem.as_ptr())
        })?;
        // Pinned OFF, deliberately and permanently: the read loop
        // (`read_buffered_pull_direct`) already IS the multi-record
        // optimization — it drains one record per `s2n_recv` into advancing
        // offsets of the destination until the target fills or s2n is dry,
        // so every record is copied exactly once and the per-call cost being
        // amortized is a bare FFI call. s2n's internal multi-record mode
        // reaches the same call shape by assembling through its own buffered
        // plaintext first — extra copy work per byte to economize calls that
        // cost nothing here. It measured worse for exactly that reason. An
        // env switch used to offer this as a "try it" knob; it was a trap —
        // the name promises batching wins the loop above already delivers.
        // Pinned rather than left to the library default so an upstream
        // default change cannot silently reintroduce the copies.
        check_s2n_status("configure multi-record receive", unsafe {
            s2n::s2n_config_set_recv_multi_record(config.as_ptr(), false)
        })?;
        Ok(config)
    }

    fn as_ptr(&self) -> *mut s2n::s2n_config {
        self.ptr.as_ptr()
    }
}

#[cfg(not(windows))]
impl Drop for RawS2nConfig {
    fn drop(&mut self) {
        let _ = unsafe { s2n::s2n_config_free(self.as_ptr()) };
    }
}

#[cfg(not(windows))]
impl RawS2nConnection {
    fn new_client() -> Result<Self> {
        s2n_tls::init::init();
        let ptr = NonNull::new(unsafe { s2n::s2n_connection_new(s2n::s2n_mode::CLIENT) })
            .ok_or_else(|| {
                NntpError::MalformedResponse(
                    "blocking s2n failed to allocate connection".to_string(),
                )
            })?;
        Ok(Self { ptr })
    }

    fn as_ptr(&self) -> *mut s2n::s2n_connection {
        self.ptr.as_ptr()
    }
}

#[cfg(not(windows))]
impl Drop for RawS2nConnection {
    fn drop(&mut self) {
        let _ = unsafe { s2n::s2n_connection_free(self.as_ptr()) };
    }
}

#[cfg(not(windows))]
unsafe impl Send for RawS2nConfig {}
#[cfg(not(windows))]
unsafe impl Send for RawS2nConnection {}

#[cfg(not(windows))]
fn check_s2n_status(context: &str, rc: libc::c_int) -> Result<()> {
    if rc < 0 {
        return Err(s2n_last_error(
            context,
            s2n::s2n_blocked_status::NOT_BLOCKED,
        ));
    }
    Ok(())
}

#[cfg(not(windows))]
fn s2n_last_error(context: &str, blocked: s2n::s2n_blocked_status::Type) -> NntpError {
    let errno = unsafe { *s2n::s2n_errno_location() };
    let kind = unsafe { s2n::s2n_error_get_type(errno) as s2n::s2n_error_type::Type };
    if kind == s2n::s2n_error_type::CLOSED {
        return NntpError::ConnectionClosed;
    }
    if kind == s2n::s2n_error_type::IO {
        return NntpError::Io(io::Error::last_os_error());
    }
    if kind == s2n::s2n_error_type::BLOCKED || blocked != s2n::s2n_blocked_status::NOT_BLOCKED {
        return NntpError::Io(io::Error::new(
            io::ErrorKind::TimedOut,
            format!("blocking s2n timed out during {context}: blocked={blocked}"),
        ));
    }

    NntpError::MalformedResponse(format!(
        "blocking s2n failed to {context}: {}: {} (type={kind}, errno={errno})",
        s2n_error_name(errno),
        s2n_error_message(errno)
    ))
}

#[cfg(not(windows))]
fn s2n_retryable_blocked(blocked: s2n::s2n_blocked_status::Type) -> bool {
    if blocked != s2n::s2n_blocked_status::NOT_BLOCKED {
        return true;
    }
    let errno = unsafe { *s2n::s2n_errno_location() };
    let kind = unsafe { s2n::s2n_error_get_type(errno) as s2n::s2n_error_type::Type };
    kind == s2n::s2n_error_type::BLOCKED
}

#[cfg(not(windows))]
fn s2n_error_name(errno: libc::c_int) -> String {
    unsafe_cstr_to_string(unsafe { s2n::s2n_strerror_name(errno) })
}

#[cfg(not(windows))]
fn s2n_error_message(errno: libc::c_int) -> String {
    unsafe_cstr_to_string(unsafe { s2n::s2n_strerror(errno, std::ptr::null()) })
}

#[cfg(not(windows))]
fn unsafe_cstr_to_string(ptr: *const libc::c_char) -> String {
    if ptr.is_null() {
        return "<null>".to_string();
    }
    unsafe { CStr::from_ptr(ptr) }
        .to_string_lossy()
        .into_owned()
}

#[cfg(not(windows))]
fn nntp_error_to_io(error: NntpError) -> io::Error {
    match error {
        NntpError::Io(error) => error,
        other => io::Error::other(other),
    }
}

fn resolve_addrs(
    host: &str,
    port: u16,
    excluded_ips: &[IpAddr],
    address_offset: usize,
) -> Result<Vec<SocketAddr>> {
    let mut addrs = (host, port)
        .to_socket_addrs()
        .map_err(NntpError::Io)?
        .filter(|addr| !excluded_ips.contains(&addr.ip()))
        .collect::<Vec<_>>();
    if addrs.is_empty() {
        return Err(NntpError::PoolExhausted);
    }
    if !addrs.is_empty() {
        let offset = address_offset % addrs.len();
        addrs.rotate_left(offset);
    }
    Ok(addrs)
}

fn decoded_body_from_article(article: FusedYencArticle) -> DecodedBody {
    DecodedBody {
        raw_size: decoded_raw_size_from_fused_stats(&article.stats),
        cpu: decoded_cpu_from_fused_stats(&article.stats),
        io: decoded_io_from_fused_stats(&article.stats),
        decoded: article.chunks,
        body: article.body,
    }
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
    stats
        .encoded_bytes_consumed
        .saturating_sub(stats.nntp_terminator_bytes)
        .min(u32::MAX as u64) as u32
}

fn profile_cpu_timings_enabled() -> bool {
    matches!(
        std::env::var("WEAVER_PROFILE_NNTP_CPU")
            .or_else(|_| std::env::var("WEAVER_PROFILE_HOT_PATHS"))
            .ok()
            .as_deref()
            .map(str::trim)
            .map(str::to_ascii_lowercase)
            .as_deref(),
        Some("1" | "true" | "yes" | "on")
    )
}

fn clone_nntp_error(error: &NntpError) -> NntpError {
    match error {
        NntpError::Timeout => NntpError::Timeout,
        NntpError::ConnectionClosed => NntpError::ConnectionClosed,
        NntpError::TruncatedMultilineBody => NntpError::TruncatedMultilineBody,
        NntpError::ServerDisconnectedMidBody => NntpError::ServerDisconnectedMidBody,
        NntpError::MalformedMultilineTerminator => NntpError::MalformedMultilineTerminator,
        NntpError::AuthenticationRequired => NntpError::AuthenticationRequired,
        NntpError::AuthenticationFailed => NntpError::AuthenticationFailed,
        NntpError::AuthenticationRejected => NntpError::AuthenticationRejected,
        NntpError::NoSuchArticle { message_id } => NntpError::NoSuchArticle {
            message_id: message_id.clone(),
        },
        NntpError::ArticleNotFound => NntpError::ArticleNotFound,
        NntpError::NoSuchGroup => NntpError::NoSuchGroup,
        NntpError::NoGroupSelected => NntpError::NoGroupSelected,
        NntpError::NoArticleWithNumber => NntpError::NoArticleWithNumber,
        NntpError::ServiceUnavailable => NntpError::ServiceUnavailable,
        NntpError::CommandNotRecognized => NntpError::CommandNotRecognized,
        NntpError::TooManyConnections => NntpError::TooManyConnections,
        NntpError::ServerOverLimit { until_epoch_ms } => NntpError::ServerOverLimit {
            until_epoch_ms: *until_epoch_ms,
        },
        NntpError::AccessDenied => NntpError::AccessDenied,
        NntpError::TlsRequired => NntpError::TlsRequired,
        NntpError::PoolExhausted => NntpError::PoolExhausted,
        NntpError::PoolShutdown => NntpError::PoolShutdown,
        NntpError::SoftTimeout(seconds) => NntpError::SoftTimeout(*seconds),
        NntpError::AcquireTimeout(seconds) => NntpError::AcquireTimeout(*seconds),
        NntpError::QuotaBlocked(rejection) => NntpError::QuotaBlocked(rejection.clone()),
        NntpError::BodyNotRequestedDueToQuota {
            preceding_rejection,
            requested_body_bytes,
        } => NntpError::BodyNotRequestedDueToQuota {
            preceding_rejection: preceding_rejection.clone(),
            requested_body_bytes: *requested_body_bytes,
        },
        NntpError::UnexpectedResponse { code, message } => NntpError::UnexpectedResponse {
            code: *code,
            message: message.clone(),
        },
        NntpError::MalformedResponse(message) => NntpError::MalformedResponse(message.clone()),
        NntpError::Io(error) => NntpError::Io(io::Error::new(error.kind(), error.to_string())),
        NntpError::Tls(error) => NntpError::MalformedResponse(format!("TLS error: {error}")),
    }
}

/// Whether a decoded BODY outcome left the connection fully consumed and
/// reusable for the rest of the pipelined batch.
///
/// A 430 is a complete server response with no body to drain, so it neither
/// desynchronises the response stream nor says anything about the socket. It
/// must therefore not mark the batch dirty — doing so tore down the TLS
/// session and permanently blocked the server's pipelining proof over a
/// perfectly ordinary "this article lives on another provider".
///
/// Decode failures are deliberately *not* treated as clean here: the yEnc
/// decoder can fail on a body the transport never finished delivering, and
/// keeping a possibly mid-body socket is not worth the saved reconnect.
fn decoded_result_keeps_connection(
    result: &std::result::Result<DecodedBody, DecodedBodyError>,
) -> bool {
    match result {
        Ok(_) => true,
        Err(DecodedBodyError::Nntp(error)) => error.is_article_not_found(),
        Err(DecodedBodyError::Decode { .. }) => false,
    }
}

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
            | NntpError::AcquireTimeout(_)
    )
}

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

#[cfg(test)]
mod tests;
