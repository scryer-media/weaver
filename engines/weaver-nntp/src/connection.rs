use std::collections::VecDeque;
use std::future::Future;
use std::net::{IpAddr, SocketAddr};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

use bytes::{Bytes, BytesMut};
use tokio::io::AsyncWriteExt;
use tokio_util::codec::Decoder;
use tracing::{debug, trace, warn};
use weaver_yenc::CheckpointPlan;

use crate::codec::{NntpCodec, NntpFrame, StreamChunk};
use crate::commands::Command;
use crate::error::{NntpError, Result};
use crate::fused_yenc::{FusedYencArticle, FusedYencArticleDecoder, FusedYencError};
use crate::response::{is_multiline_status, parse_response};
use crate::tls::{NntpTransport, TransportReadStats};
use crate::transfer::{
    ActiveTransferBudget, BodyTransferAccounting, QuotaRejection, ServerTransferControl,
    active_transfer_read_timeout_at, active_transfer_timeout,
};
use crate::types::{ArticleId, Capabilities, MultiLineResponse, Response};

#[derive(Debug, Clone, Copy, Default)]
pub struct BodyStreamStats {
    pub bytes: u64,
    pub raw_decode_cpu: Duration,
    pub read_poll_cpu: Duration,
    pub throttle_wait: Duration,
}

/// The budget check against a clock reading the read turn already took.
fn ensure_active_transfer_budget_at(
    budget: Option<&ActiveTransferBudget>,
    now: Instant,
) -> Result<()> {
    if let Some(budget) = budget
        && budget.remaining_at(now).is_zero()
    {
        return Err(active_transfer_timeout(budget));
    }
    Ok(())
}

async fn await_active_transfer<F, T>(budget: Option<&ActiveTransferBudget>, future: F) -> Result<T>
where
    F: Future<Output = Result<T>>,
{
    let Some(budget) = budget else {
        return future.await;
    };
    let remaining = budget.remaining();
    if remaining.is_zero() {
        return Err(active_transfer_timeout(budget));
    }
    tokio::time::timeout(remaining, future)
        .await
        .map_err(|_| active_transfer_timeout(budget))?
}

#[cfg(unix)]
fn thread_cpu_time() -> Option<Duration> {
    let mut timespec = std::mem::MaybeUninit::<libc::timespec>::uninit();
    let rc = unsafe { libc::clock_gettime(libc::CLOCK_THREAD_CPUTIME_ID, timespec.as_mut_ptr()) };
    if rc != 0 {
        return None;
    }
    let timespec = unsafe { timespec.assume_init() };
    let seconds = u64::try_from(timespec.tv_sec).ok()?;
    let nanos = u32::try_from(timespec.tv_nsec).ok()?.min(999_999_999);
    Some(Duration::new(seconds, nanos))
}

#[cfg(windows)]
fn thread_cpu_time() -> Option<Duration> {
    use windows_sys::Win32::System::Threading::{GetCurrentThread, GetThreadTimes};

    const ZERO: windows_sys::Win32::Foundation::FILETIME =
        windows_sys::Win32::Foundation::FILETIME {
            dwLowDateTime: 0,
            dwHighDateTime: 0,
        };
    let mut times = [ZERO; 4];
    let [creation, exit, kernel, user] = &mut times;
    // SAFETY: the pseudo-handle is always valid for the current thread and
    // all four out-pointers reference live FILETIME slots.
    let rc = unsafe { GetThreadTimes(GetCurrentThread(), creation, exit, kernel, user) };
    if rc == 0 {
        return None;
    }
    // FILETIME counts 100 ns ticks; kernel+user matches the unix
    // CLOCK_THREAD_CPUTIME_ID semantics. Granularity is the scheduler tick
    // (~15.6 ms), which is fine for the aggregated deltas reported here.
    let ticks = |filetime: windows_sys::Win32::Foundation::FILETIME| {
        ((filetime.dwHighDateTime as u64) << 32) | filetime.dwLowDateTime as u64
    };
    let total = ticks(times[2]).saturating_add(ticks(times[3]));
    Some(Duration::from_nanos(total.saturating_mul(100)))
}

#[cfg(not(any(unix, windows)))]
fn thread_cpu_time() -> Option<Duration> {
    None
}

fn add_cpu_delta(total: &mut Duration, started: Option<Duration>) {
    let Some(started) = started else {
        return;
    };
    let Some(current) = thread_cpu_time() else {
        return;
    };
    if let Some(delta) = current.checked_sub(started) {
        *total += delta;
    }
}

fn profile_cpu_timings_enabled() -> bool {
    static ENABLED: OnceLock<bool> = OnceLock::new();
    *ENABLED.get_or_init(|| {
        env_truthy("WEAVER_PROFILE_NNTP_CPU") || env_truthy("WEAVER_PROFILE_HOT_PATHS")
    })
}

fn env_truthy(key: &str) -> bool {
    matches!(
        std::env::var(key)
            .ok()
            .as_deref()
            .map(str::trim)
            .map(str::to_ascii_lowercase)
            .as_deref(),
        Some("1" | "true" | "yes" | "on")
    )
}

async fn measure_poll_cpu<F>(future: F) -> (F::Output, Duration)
where
    F: std::future::Future,
{
    let mut future = std::pin::pin!(future);
    let mut cpu = Duration::ZERO;
    let output = std::future::poll_fn(|cx| {
        let started = thread_cpu_time();
        let polled = future.as_mut().poll(cx);
        add_cpu_delta(&mut cpu, started);
        polled
    })
    .await;
    (output, cpu)
}

/// Hand every decoded batch the fused decoder has produced to `on_chunk`, in
/// order, before appending it to the article's own chunk list.
///
/// This is what makes the streaming design observable to a caller: batches are
/// delivered as they are decoded, not once at article finish. Both the async
/// and the blocking article readers route through here so neither can quietly
/// stop firing the callback.
pub(crate) fn deliver_fused_output_chunks<F>(
    chunks: Vec<Box<[u8]>>,
    article_chunks: &mut Vec<Box<[u8]>>,
    on_chunk: &mut F,
    output_callback_cpu: &mut Duration,
    profile_cpu: bool,
) -> std::result::Result<(), FusedYencError>
where
    F: FnMut(&[u8]) -> Result<()>,
{
    if chunks.is_empty() {
        return Ok(());
    }

    let cpu_started = profile_cpu.then(thread_cpu_time).flatten();
    for chunk in chunks {
        if let Err(err) = on_chunk(&chunk) {
            add_cpu_delta(output_callback_cpu, cpu_started);
            return Err(err.into());
        }
        article_chunks.push(chunk);
    }
    add_cpu_delta(output_callback_cpu, cpu_started);
    Ok(())
}

/// State of a single NNTP connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ConnectionState {
    /// Not connected.
    Disconnected,
    /// Waiting for the server greeting.
    Greeting,
    /// Authenticated and ready for commands.
    Ready,
    /// A command is in progress.
    InUse,
    /// QUIT sent, connection closing.
    Closing,
}

/// Minimum allowed timeout value (1 second).
const MIN_TIMEOUT: Duration = Duration::from_secs(1);

/// Internal NNTP buffer sizing profile.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct NntpBufferProfile {
    pub read_buf_capacity: usize,
    pub socket_read_size: usize,
}

impl NntpBufferProfile {
    pub fn adaptive(available_bytes: u64, total_connections: usize) -> Self {
        let total_budget = (available_bytes / 32).min(256 * 1024 * 1024) as usize;
        let per_connection = total_budget
            .checked_div(total_connections)
            .unwrap_or(128 * 1024)
            .clamp(128 * 1024, 512 * 1024);
        Self {
            read_buf_capacity: per_connection,
            socket_read_size: per_connection.min(256 * 1024),
        }
    }
}

impl Default for NntpBufferProfile {
    fn default() -> Self {
        Self {
            read_buf_capacity: 64 * 1024,
            socket_read_size: 64 * 1024,
        }
    }
}

/// How an NNTP connection determines PIPELINING support.
///
/// Application runtime connections use the persisted server setting. Explicit
/// server validation opts into [`Self::Probe`] and performs one post-auth
/// CAPABILITIES exchange instead.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum PipeliningCapability {
    /// Discover the capability from the server.
    #[default]
    Probe,
    /// Use the persisted server setting and skip CAPABILITIES.
    Known(bool),
}

/// Configuration for connecting to a single NNTP server.
#[derive(Debug, Clone)]
pub struct ServerConfig {
    /// Hostname or IP address.
    pub host: String,
    /// Port number.
    pub port: u16,
    /// Use implicit TLS (true for port 563).
    pub tls: bool,
    /// Use STARTTLS after plain TCP connect.
    pub starttls: bool,
    /// Username for AUTHINFO USER.
    pub username: Option<String>,
    /// Password for AUTHINFO PASS.
    pub password: Option<String>,
    /// Timeout for the entire connection setup (connect + TLS + auth).
    /// Clamped to a minimum of 1 second.
    pub connect_timeout: Duration,
    /// Timeout for individual command responses.
    /// Clamped to a minimum of 1 second.
    pub command_timeout: Duration,
    /// Internal read-buffer sizing profile.
    pub buffer_profile: NntpBufferProfile,
    /// Optional path to a PEM-encoded CA certificate to trust in addition
    /// to the system/Mozilla roots (e.g. self-signed or internal CAs).
    pub tls_ca_cert: Option<std::path::PathBuf>,
    /// One explicitly adopted leaf certificate allowed only when normal TLS
    /// verification fails because this server's hostname does not match.
    pub tls_name_mismatch_certificate_der: Option<Vec<u8>>,
    /// Source of the PIPELINING capability for this connection.
    pub pipelining: PipeliningCapability,
    /// BODY pipelining depth a previous run proved for this server, if any.
    /// Inert for the connection itself; the download lanes read it to start
    /// where the last run left off instead of rediscovering the depth.
    pub pipelining_depth: Option<u8>,
    /// Which AEAD family the TLS ClientHello offers first.
    pub tls_cipher_preference: crate::tls::TlsCipherPreference,
}

impl Default for ServerConfig {
    fn default() -> Self {
        ServerConfig {
            host: String::new(),
            port: 563,
            tls: true,
            starttls: false,
            username: None,
            password: None,
            connect_timeout: Duration::from_secs(30),
            command_timeout: Duration::from_mins(1),
            buffer_profile: NntpBufferProfile::default(),
            tls_ca_cert: None,
            tls_name_mismatch_certificate_der: None,
            pipelining: PipeliningCapability::Probe,
            pipelining_depth: None,
            tls_cipher_preference: crate::tls::TlsCipherPreference::Auto,
        }
    }
}

/// A single NNTP connection to a server.
///
/// Manages the transport, codec, and read buffer manually (not using `Framed`)
/// to avoid borrow-checker issues when we need simultaneous access to the codec,
/// buffer, and transport.
pub struct NntpConnection {
    /// Wrapped in Option to allow taking ownership during STARTTLS upgrade.
    transport: Option<NntpTransport>,
    codec: NntpCodec,
    read_buf: BytesMut,
    buffer_profile: NntpBufferProfile,
    state: ConnectionState,
    capabilities: Capabilities,
    host: String,
    /// Kept alongside `host` so a requirement learned on this connection is
    /// recorded against the endpoint, not the resolved address.
    port: u16,
    remote_addr: SocketAddr,
    created_at: Instant,
    last_used: Instant,
    command_timeout: Duration,
    /// Set to true when an I/O error has occurred, preventing pool reuse.
    poisoned: bool,
    /// The currently selected newsgroup on this connection, if any.
    current_group: Option<String>,
    /// Stored credentials for transparent mid-session re-authentication.
    credentials: Option<(String, String)>,
    /// Optional custom CA certificate path, kept for STARTTLS upgrades.
    tls_ca_cert: Option<std::path::PathBuf>,
    /// Optional adopted leaf certificate, kept for STARTTLS upgrades.
    tls_name_mismatch_certificate_der: Option<Vec<u8>>,
    /// Cipher family order, kept for STARTTLS upgrades.
    tls_cipher_preference: crate::tls::TlsCipherPreference,
    transfer_control: Option<Arc<ServerTransferControl>>,
    body_accounting: VecDeque<BodyTransferAccounting>,
    /// Immutable geometry the next decoded article's CRC pass checkpoints at.
    ///
    /// Set per fetch by the lane rather than at connect time: connections are
    /// pooled across jobs, and checkpoint geometry belongs to a job snapshot,
    /// not to a socket. `None` is deliberately applied per response.
    checkpoint_plan: CheckpointPlan,
    /// How long the last decoded article waited for its status line. The lane
    /// takes this to separate distance from transfer cost.
    last_response_line_wait: Duration,
    /// Armed when session setup declined to select a group this caller
    /// offered, because the server has never been shown to need one. The first
    /// response afterwards either clears it or teaches the process that this
    /// server does insist. See [`crate::server_caps`].
    group_probe_armed: bool,
}

impl NntpConnection {
    /// Declare the checkpoint geometry for articles decoded on this connection
    /// from now on. See [`Self::checkpoint_plan`].
    pub fn set_checkpoint_plan(&mut self, checkpoint_plan: CheckpointPlan) {
        self.checkpoint_plan = checkpoint_plan;
    }

    /// Consume the last article's status-line wait, so a lane cannot credit
    /// one response's latency to the next.
    pub(crate) fn take_response_line_wait(&mut self) -> Duration {
        std::mem::replace(&mut self.last_response_line_wait, Duration::ZERO)
    }

    /// Connect to an NNTP server, perform TLS negotiation and authentication.
    pub async fn connect(config: &ServerConfig) -> Result<Self> {
        Self::connect_with_ip_policy_for_group(config, &[], 0, None).await
    }

    /// Connect and, on servers known to pipeline, select `initial_group` in
    /// the same write as the session setup so a BODY lane starts with no
    /// extra round trip. An unselectable group is not an error here: the
    /// lane walks its candidate list afterwards.
    pub(crate) async fn connect_with_ip_policy_for_group(
        config: &ServerConfig,
        excluded_ips: &[IpAddr],
        address_offset: usize,
        initial_group: Option<&str>,
    ) -> Result<Self> {
        let connect_timeout = config.connect_timeout.max(MIN_TIMEOUT);
        let result = tokio::time::timeout(connect_timeout, async {
            Self::connect_inner(config, excluded_ips, address_offset, initial_group).await
        })
        .await;

        match result {
            Ok(inner) => inner,
            Err(_) => Err(NntpError::Timeout),
        }
    }

    async fn connect_inner(
        config: &ServerConfig,
        excluded_ips: &[IpAddr],
        address_offset: usize,
        initial_group: Option<&str>,
    ) -> Result<Self> {
        debug!(host = %config.host, port = config.port, tls = config.tls, "connecting to NNTP server");

        // 1. Establish transport
        let transport = if config.tls {
            crate::tls::connect_tls_with_ip_policy(
                &config.host,
                config.port,
                config.tls_ca_cert.as_deref(),
                config.tls_name_mismatch_certificate_der.as_deref(),
                config.tls_cipher_preference,
                excluded_ips,
                address_offset,
            )
            .await?
        } else {
            crate::tls::connect_plain_with_ip_policy(
                &config.host,
                config.port,
                excluded_ips,
                address_offset,
            )
            .await?
        };

        let now = Instant::now();
        let remote_addr = transport.remote_addr();
        let read_buf_capacity = config.buffer_profile.read_buf_capacity.max(64 * 1024);
        let mut conn = NntpConnection {
            transport: Some(transport),
            codec: NntpCodec::new(),
            read_buf: BytesMut::with_capacity(read_buf_capacity),
            buffer_profile: config.buffer_profile,
            state: ConnectionState::Greeting,
            capabilities: match config.pipelining {
                PipeliningCapability::Probe => Capabilities::default(),
                PipeliningCapability::Known(supports) => Capabilities::from_pipelining(supports),
            },
            host: config.host.clone(),
            port: config.port,
            remote_addr,
            created_at: now,
            last_used: now,
            command_timeout: config.command_timeout.max(MIN_TIMEOUT),
            poisoned: false,
            current_group: None,
            credentials: None,
            tls_ca_cert: config.tls_ca_cert.clone(),
            tls_name_mismatch_certificate_der: config.tls_name_mismatch_certificate_der.clone(),
            tls_cipher_preference: config.tls_cipher_preference,
            transfer_control: None,
            body_accounting: VecDeque::new(),
            checkpoint_plan: CheckpointPlan::None,
            last_response_line_wait: Duration::ZERO,
            group_probe_armed: false,
        };

        // 2. Read greeting
        let greeting = conn.read_response().await?;
        debug!(code = greeting.code.raw(), msg = %greeting.message, "received greeting");

        match greeting.code.raw() {
            200 | 201 => {} // posting allowed / no posting — both fine for readers
            400 => return Err(NntpError::ServiceUnavailable),
            502 => return Err(NntpError::from_status(greeting.code, &greeting.message)),
            _ => return Err(NntpError::unexpected(greeting.code, &greeting.message)),
        }

        // 3. STARTTLS upgrade if configured and transport is plain
        if config.starttls && !conn.transport.as_ref().unwrap().is_tls() {
            conn.do_starttls().await?;
        }

        // 4-5. Session setup: authentication and nothing else, unless this
        // server has proven it needs a selected group. Every command here runs
        // before the lane's first BODY can be asked for, so each one costs a
        // full round trip of the article's time to first byte — see
        // [`crate::server_caps`] for why MODE READER is never sent and GROUP is
        // learned instead of assumed.
        //
        // A server known to pipeline authenticates serially (AUTHINFO must
        // not be pipelined, RFC 4643) and then takes the GROUP it asked for in
        // one write, answered in order. A server of unknown or negative
        // capability keeps the serial exchange throughout.
        let requires_group =
            crate::server_caps::requires_group_selection(&config.host, config.port);
        let requested_group = initial_group.filter(|_| requires_group);
        if matches!(config.pipelining, PipeliningCapability::Known(true)) {
            conn.pipelined_session_setup(config, requested_group)
                .await?;
        } else {
            if let (Some(user), Some(pass)) = (&config.username, &config.password) {
                conn.authenticate(user, pass).await?;
                conn.credentials = Some((user.clone(), pass.clone()));
            }

            if let Some(group) = requested_group {
                let resp = conn
                    .send_command(&Command::Group(group.to_string()))
                    .await?;
                if resp.code.is_error() {
                    debug!(code = resp.code.raw(), group, "initial GROUP not selected");
                } else {
                    conn.current_group = Some(group.to_string());
                }
            }
        }

        // 6. Validation uses exactly one post-auth capability exchange.
        if matches!(config.pipelining, PipeliningCapability::Probe) {
            conn.fetch_capabilities().await?;
        }

        // Setup is over: from here the next status line is an answer to the
        // caller's own command, and is the one that can still be about the
        // group this connection chose not to select.
        conn.group_probe_armed = initial_group.is_some() && !requires_group;

        conn.state = ConnectionState::Ready;
        debug!("NNTP connection ready");
        Ok(conn)
    }

    /// Perform STARTTLS upgrade.
    async fn do_starttls(&mut self) -> Result<()> {
        debug!("initiating STARTTLS");
        let resp = self.send_command(&Command::StartTls).await?;
        if resp.code.raw() != 382 {
            return Err(NntpError::unexpected(resp.code, &resp.message));
        }

        // Take ownership of the transport, upgrade it, and put it back.
        let old_transport = self.transport.take().expect("transport must be present");
        match crate::tls::upgrade_starttls(
            old_transport,
            &self.host,
            self.tls_ca_cert.as_deref(),
            self.tls_name_mismatch_certificate_der.as_deref(),
            self.tls_cipher_preference,
        )
        .await
        {
            Ok(upgraded) => {
                self.transport = Some(upgraded);
                self.codec = NntpCodec::new();
                self.read_buf.clear();
                debug!("STARTTLS upgrade complete");
                Ok(())
            }
            Err(e) => {
                // Transport is gone; poison the connection.
                self.poisoned = true;
                self.current_group = None;
                Err(e)
            }
        }
    }

    /// Fetch and parse server capabilities.
    async fn fetch_capabilities(&mut self) -> Result<()> {
        let resp = self.send_command(&Command::Capabilities).await?;
        if resp.code.raw() == 101 {
            let data = self.read_multiline_data().await?;
            self.capabilities = Capabilities::parse(&data);
            trace!(caps = ?self.capabilities, "parsed capabilities");
        } else {
            debug!(code = resp.code.raw(), "CAPABILITIES not supported");
        }
        Ok(())
    }

    /// Session setup for a server known to pipeline. AUTHINFO goes first and
    /// on its own: RFC 4643 forbids pipelining it, and a provider that
    /// enforces that answers the whole batch with 480s or drops the
    /// connection. A GROUP this server has proven it needs then leaves in one
    /// flush and is answered in order (RFC 4644) — usually there is nothing at
    /// all to send, which is the point: the lane reaches its first BODY in
    /// four round trips.
    async fn pipelined_session_setup(
        &mut self,
        config: &ServerConfig,
        initial_group: Option<&str>,
    ) -> Result<()> {
        if let (Some(user), Some(pass)) = (&config.username, &config.password) {
            self.authenticate(user, pass).await?;
            self.credentials = Some((user.clone(), pass.clone()));
        }

        let Some(group) = initial_group else {
            return Ok(());
        };

        debug!(group, "selecting the group this server insists on");
        self.write_command_frame(&Command::Group(group.to_string()))
            .await?;
        self.flush_commands().await?;

        let group_resp = self.read_response().await?;
        if group_resp.code.is_error() {
            debug!(
                code = group_resp.code.raw(),
                group, "pipelined GROUP not selected"
            );
        } else {
            self.current_group = Some(group.to_string());
        }
        Ok(())
    }

    /// Authenticate using AUTHINFO USER/PASS (RFC 4643).
    pub async fn authenticate(&mut self, username: &str, password: &str) -> Result<()> {
        debug!("authenticating");

        let user_resp = self
            .send_command(&Command::AuthInfoUser(username.to_string()))
            .await?;

        match user_resp.code.raw() {
            281 => {
                debug!("authenticated with username only");
                return Ok(());
            }
            381 => {
                // Password required — continue.
            }
            _ => {
                return Err(NntpError::from_status(user_resp.code, &user_resp.message));
            }
        }

        let pass_resp = self
            .send_command(&Command::AuthInfoPass(password.to_string()))
            .await?;

        match pass_resp.code.raw() {
            281 => {
                debug!("authentication successful");
                Ok(())
            }
            481 => Err(NntpError::AuthenticationFailed),
            482 => Err(NntpError::AuthenticationRejected),
            _ => Err(NntpError::from_status(pass_resp.code, &pass_resp.message)),
        }
    }

    /// Send a command and read the single-line response.
    async fn write_command_frame(&mut self, cmd: &Command) -> Result<()> {
        self.last_used = Instant::now();

        let encoded = cmd.encode();
        let verb = encoded
            .split(|byte| byte.is_ascii_whitespace())
            .next()
            .unwrap_or_default();
        trace!(cmd = %String::from_utf8_lossy(verb), "sending command");

        let transport = self.transport.as_mut().ok_or(NntpError::ConnectionClosed)?;
        transport.write_all(&encoded).await.map_err(|e| {
            self.poisoned = true;
            self.current_group = None;
            NntpError::Io(e)
        })?;
        Ok(())
    }

    pub async fn flush_commands(&mut self) -> Result<()> {
        let transport = self.transport.as_mut().ok_or(NntpError::ConnectionClosed)?;
        transport.flush().await.map_err(|e| {
            self.poisoned = true;
            self.current_group = None;
            NntpError::Io(e)
        })?;
        Ok(())
    }

    pub async fn send_command(&mut self, cmd: &Command) -> Result<Response> {
        self.write_command_frame(cmd).await?;
        self.flush_commands().await?;

        self.read_response().await
    }

    pub async fn write_body_request(&mut self, message_id: &str) -> Result<()> {
        self.write_body_request_with_estimate(message_id, 0).await
    }

    pub async fn write_body_request_with_estimate(
        &mut self,
        message_id: &str,
        estimated_body_bytes: u64,
    ) -> Result<()> {
        self.reserve_body(estimated_body_bytes)
            .map_err(NntpError::quota_blocked)?;
        let cmd = Command::Body(ArticleId::MessageId(message_id.to_string()));
        if let Err(error) = self.write_command_frame(&cmd).await {
            self.body_accounting.pop_back();
            return Err(error);
        }
        Ok(())
    }

    pub(crate) fn set_transfer_control(
        &mut self,
        transfer_control: Option<Arc<ServerTransferControl>>,
    ) {
        debug_assert!(self.body_accounting.is_empty());
        self.transfer_control = transfer_control;
    }

    fn reserve_body(
        &mut self,
        estimated_body_bytes: u64,
    ) -> std::result::Result<(), QuotaRejection> {
        if let Some(control) = &self.transfer_control {
            self.body_accounting
                .push_back(control.start_body(estimated_body_bytes)?);
        }
        Ok(())
    }

    async fn charge_active_body(&mut self, bytes: usize) -> Duration {
        match self.body_accounting.front_mut() {
            Some(BodyTransferAccounting::Unlimited) => {
                if let Some(control) = &self.transfer_control {
                    control.record_unlimited_body_bytes(bytes);
                }
                Duration::ZERO
            }
            Some(BodyTransferAccounting::Tracked(permit)) => permit.record_async(bytes).await,
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

    fn poison_on_soft_timeout(&mut self, error: &NntpError) {
        if matches!(error, NntpError::SoftTimeout(_)) {
            self.poisoned = true;
            self.current_group = None;
        }
    }

    async fn send_reserved_body_initial(&mut self, message_id: &str) -> Result<Response> {
        self.send_reserved_body_initial_with_budget(message_id, None)
            .await
    }

    async fn send_reserved_body_initial_with_budget(
        &mut self,
        message_id: &str,
        budget: Option<&ActiveTransferBudget>,
    ) -> Result<Response> {
        let cmd = Command::Body(ArticleId::MessageId(message_id.to_string()));
        let initial = match await_active_transfer(budget, self.send_command(&cmd)).await {
            Ok(initial) => initial,
            Err(error) => {
                self.poison_on_soft_timeout(&error);
                self.abort_active_body();
                return Err(error);
            }
        };
        if initial.code.raw() != 480 {
            return Ok(initial);
        }
        let Some((user, pass)) = self.credentials.clone() else {
            self.abort_active_body();
            return Err(NntpError::AuthenticationRequired);
        };
        debug!("server requested re-authentication (480), re-authenticating");
        if let Err(error) = await_active_transfer(budget, self.authenticate(&user, &pass)).await {
            self.poison_on_soft_timeout(&error);
            self.abort_active_body();
            return Err(error);
        }
        self.current_group = None;
        match await_active_transfer(budget, self.send_command(&cmd)).await {
            Ok(initial) => Ok(initial),
            Err(error) => {
                self.poison_on_soft_timeout(&error);
                self.abort_active_body();
                Err(error)
            }
        }
    }

    /// Read a single response line from the server.
    async fn read_response(&mut self) -> Result<Response> {
        let frame = self.read_frame().await?;
        self.trim_read_buffer();
        match frame {
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

    fn reset_multiline_decode_state(&mut self) {
        self.codec.set_multiline(false);
        self.codec.set_streaming_multiline(false);
        self.codec.set_raw_multiline(false);
    }

    fn classify_multiline_error(&self, err: NntpError) -> NntpError {
        if !self.codec.is_reading_multiline() {
            return err;
        }

        match err {
            NntpError::ConnectionClosed => {
                if pending_multiline_terminator(&self.read_buf) {
                    NntpError::MalformedMultilineTerminator
                } else {
                    NntpError::ServerDisconnectedMidBody
                }
            }
            NntpError::Timeout => NntpError::TruncatedMultilineBody,
            other => other,
        }
    }

    /// Read a raw frame from the codec, with timeout.
    async fn read_frame(&mut self) -> Result<NntpFrame> {
        let timeout = self.command_timeout;

        let result = tokio::time::timeout(timeout, async {
            loop {
                // Try to decode a frame from the read buffer.
                if let Some(frame) = self.codec.decode(&mut self.read_buf)? {
                    return Ok::<_, NntpError>(frame);
                }

                // Need more data from the transport.
                self.read_into_buffer().await?;
            }
        })
        .await;

        match result {
            Ok(inner) => inner,
            Err(_) => {
                self.poisoned = true;
                self.current_group = None;
                Err(NntpError::Timeout)
            }
        }
    }

    /// Read a multi-line data block from the server.
    ///
    /// Call after receiving a status code that indicates multi-line data follows.
    pub async fn read_multiline_data(&mut self) -> Result<Bytes> {
        self.read_multiline_data_inner(false).await
    }

    /// Read a multi-line data block without dot-unstuffing.
    ///
    /// The returned data retains NNTP dot-stuffing (lines starting with `..`
    /// keep both dots). The caller is responsible for inline unstuffing during
    /// content decoding. This avoids a separate scan+copy pass.
    pub async fn read_multiline_data_raw(&mut self) -> Result<Bytes> {
        self.read_multiline_data_inner(true).await
    }

    async fn read_multiline_data_inner(&mut self, raw: bool) -> Result<Bytes> {
        self.codec.set_multiline(true);
        self.codec.set_raw_multiline(raw);
        let frame = self.read_frame().await;
        self.codec.set_raw_multiline(false);
        match frame {
            Ok(NntpFrame::MultiLineData(data)) => {
                self.trim_read_buffer();
                let data = data.freeze();
                self.charge_active_body(self.codec.last_multiline_payload_bytes())
                    .await;
                self.finish_active_body();
                Ok(data)
            }
            Ok(NntpFrame::Line(line)) => {
                self.abort_active_body();
                self.reset_multiline_decode_state();
                Err(NntpError::MalformedResponse(format!(
                    "expected multi-line data, got line: {line:?}"
                )))
            }
            Err(err) => {
                let partial_bytes = partial_multiline_payload_len(&self.read_buf);
                self.charge_active_body(partial_bytes).await;
                let err = self.classify_multiline_error(err);
                self.abort_active_body();
                self.reset_multiline_decode_state();
                Err(err)
            }
        }
    }

    /// Send a command and read the complete multi-line response.
    ///
    /// Used for commands like BODY, HEAD, ARTICLE that return multi-line data.
    /// If the server responds with 480 (authentication required) and we have
    /// stored credentials, transparently re-authenticates and retries once.
    pub async fn send_multiline_command(&mut self, cmd: &Command) -> Result<MultiLineResponse> {
        self.send_multiline_command_inner(cmd, false).await
    }

    /// Like `send_multiline_command` but returns raw data without dot-unstuffing.
    pub async fn send_multiline_command_raw(&mut self, cmd: &Command) -> Result<MultiLineResponse> {
        self.send_multiline_command_inner(cmd, true).await
    }

    async fn send_multiline_command_inner(
        &mut self,
        cmd: &Command,
        raw: bool,
    ) -> Result<MultiLineResponse> {
        let initial = self.send_command(cmd).await?;

        // Handle mid-session re-auth (480) transparently.
        if initial.code.raw() == 480 {
            if let Some((user, pass)) = self.credentials.clone() {
                debug!("server requested re-authentication (480), re-authenticating");
                self.authenticate(&user, &pass).await?;
                self.current_group = None;
                let retry = self.send_command(cmd).await?;
                if retry.code.is_error() {
                    return Err(NntpError::from_status(retry.code, &retry.message));
                }
                if !is_multiline_status(retry.code.raw()) {
                    return Err(NntpError::MalformedResponse(format!(
                        "expected multi-line status, got {}",
                        retry.code.raw()
                    )));
                }
                let data = self.read_multiline_data_inner(raw).await?;
                return Ok(MultiLineResponse {
                    initial: retry,
                    data,
                });
            }
            return Err(NntpError::AuthenticationRequired);
        }

        if initial.code.is_error() {
            return Err(NntpError::from_status(initial.code, &initial.message));
        }

        if !is_multiline_status(initial.code.raw()) {
            return Err(NntpError::MalformedResponse(format!(
                "expected multi-line status, got {}",
                initial.code.raw()
            )));
        }

        let data = self.read_multiline_data_inner(raw).await?;
        Ok(MultiLineResponse { initial, data })
    }

    /// Ensure the given group is selected on this connection.
    ///
    /// Sends the GROUP command only if the current group differs from the
    /// requested one, avoiding unnecessary round-trips. Handles mid-session
    /// re-authentication (480) transparently.
    pub async fn select_group(&mut self, group: &str) -> Result<()> {
        if self.current_group.as_deref() == Some(group) {
            return Ok(());
        }
        let response = self
            .send_command(&Command::Group(group.to_string()))
            .await?;

        // Handle mid-session re-auth (480) transparently.
        if response.code.raw() == 480 {
            if let Some((user, pass)) = self.credentials.clone() {
                debug!("server requested re-authentication (480), re-authenticating");
                self.authenticate(&user, &pass).await?;
                self.current_group = None;
                let retry = self
                    .send_command(&Command::Group(group.to_string()))
                    .await?;
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

    /// The currently selected newsgroup on this connection, if any.
    pub fn current_group(&self) -> Option<&str> {
        self.current_group.as_deref()
    }

    /// Retrieve the body of an article by message-id.
    pub async fn body_by_id(&mut self, message_id: &str) -> Result<MultiLineResponse> {
        self.body_by_id_with_estimate(message_id, 0).await
    }

    pub async fn body_by_id_with_estimate(
        &mut self,
        message_id: &str,
        estimated_body_bytes: u64,
    ) -> Result<MultiLineResponse> {
        self.body_by_id_inner(message_id, estimated_body_bytes, false, None)
            .await
    }

    /// Retrieve the body of an article without dot-unstuffing.
    ///
    /// The returned data retains NNTP dot-stuffing. Use with `weaver_yenc::decode_nntp`
    /// which handles unstuffing inline during decode, avoiding a separate pass.
    /// Size its destination from `weaver_yenc::max_decoded_len` of the returned
    /// data, never from the article's declared `=ybegin size=` — a poster is
    /// free to omit that field, and an undersized destination is a typed error.
    pub async fn body_by_id_raw(&mut self, message_id: &str) -> Result<MultiLineResponse> {
        self.body_by_id_raw_with_estimate(message_id, 0).await
    }

    pub async fn body_by_id_raw_with_estimate(
        &mut self,
        message_id: &str,
        estimated_body_bytes: u64,
    ) -> Result<MultiLineResponse> {
        self.body_by_id_inner(message_id, estimated_body_bytes, true, None)
            .await
    }

    pub(crate) async fn body_by_id_raw_with_active_budget(
        &mut self,
        message_id: &str,
        budget: &mut ActiveTransferBudget,
    ) -> Result<MultiLineResponse> {
        self.body_by_id_inner(message_id, 0, true, Some(budget))
            .await
    }

    async fn body_by_id_inner(
        &mut self,
        message_id: &str,
        estimated_body_bytes: u64,
        raw: bool,
        budget: Option<&mut ActiveTransferBudget>,
    ) -> Result<MultiLineResponse> {
        self.reserve_body(estimated_body_bytes)
            .map_err(NntpError::quota_blocked)?;
        let initial = self
            .send_reserved_body_initial_with_budget(message_id, budget.as_deref())
            .await?;
        let response = initial.clone();
        let mut data = Vec::new();
        self.stream_body_response(initial, raw, budget, |chunk| {
            data.extend_from_slice(chunk);
            Ok(())
        })
        .await?;
        Ok(MultiLineResponse {
            initial: response,
            data: Bytes::from(data),
        })
    }

    /// Retrieve the headers of an article by message-id.
    ///
    /// A 500/501 here is the server saying it does not implement `HEAD` at
    /// all. That is an answer, not a fault: it is recorded against the server
    /// so later callers stop asking, and the connection stays healthy.
    pub async fn head_by_id(&mut self, message_id: &str) -> Result<MultiLineResponse> {
        let cmd = Command::Head(ArticleId::MessageId(message_id.to_string()));
        let result = self.send_multiline_command(&cmd).await;
        if result
            .as_ref()
            .err()
            .is_some_and(reports_unsupported_command)
            && crate::server_caps::note_head_unsupported(&self.host, self.port)
        {
            debug!(host = %self.host, port = self.port, "server does not implement HEAD");
        }
        result
    }

    /// Retrieve a complete article (headers + body) by message-id.
    pub async fn article_by_id(&mut self, message_id: &str) -> Result<MultiLineResponse> {
        let cmd = Command::Article(ArticleId::MessageId(message_id.to_string()));
        self.send_multiline_command(&cmd).await
    }

    /// Check whether an article exists on the server without downloading it.
    ///
    /// Returns `Ok(true)` if the article exists (223), `Ok(false)` if not found (430).
    pub async fn stat_by_id(&mut self, message_id: &str) -> Result<bool> {
        let cmd = Command::Stat(ArticleId::MessageId(message_id.to_string()));
        let resp = self.send_command(&cmd).await?;
        self.classify_stat_response(&resp)
    }

    /// Turn one STAT status line into an existence verdict.
    ///
    /// A 500/501 is the server saying it does not implement STAT. That is an
    /// answer about the command, not a fault on the socket: it is recorded so
    /// the existence probe switches to HEAD, and the connection stays healthy
    /// and pooled.
    fn classify_stat_response(&mut self, resp: &Response) -> Result<bool> {
        match resp.code.raw() {
            223 => Ok(true),
            430 => Ok(false),
            code if crate::server_caps::is_command_unsupported(code) => {
                if crate::server_caps::note_stat_unsupported(&self.host, self.port) {
                    debug!(host = %self.host, port = self.port, "server does not implement STAT");
                }
                Err(NntpError::CommandNotRecognized)
            }
            _ => Err(NntpError::from_status(resp.code, &resp.message)),
        }
    }

    /// Check multiple articles for existence using NNTP pipelining.
    ///
    /// Sends all STAT commands in a single write, then reads all responses.
    /// This amortizes network round-trip time across the batch — N articles
    /// checked in 1 RTT instead of N RTTs.
    ///
    /// Returns a `Vec<bool>` aligned with the input: true = exists, false = 430.
    pub async fn stat_pipeline(&mut self, message_ids: &[&str]) -> Result<Vec<bool>> {
        if message_ids.is_empty() {
            return Ok(Vec::new());
        }

        self.last_used = Instant::now();
        let transport = self.transport.as_mut().ok_or(NntpError::ConnectionClosed)?;

        // Write all STAT commands without reading responses.
        for msg_id in message_ids {
            let cmd = Command::Stat(ArticleId::MessageId(msg_id.to_string()));
            let encoded = cmd.encode();
            transport.write_all(&encoded).await.map_err(|e| {
                self.poisoned = true;
                NntpError::Io(e)
            })?;
        }
        // Single flush for the entire batch.
        transport.flush().await.map_err(|e| {
            self.poisoned = true;
            NntpError::Io(e)
        })?;

        // Read all responses.
        let mut results = Vec::with_capacity(message_ids.len());
        let mut refusal = None;
        for _ in message_ids {
            let resp = self.read_response().await?;
            // Every response of the batch is still owed, so a refusal is
            // remembered and returned only once the pipe is drained — leaving
            // it half-read would strand the socket mid-batch.
            match self.classify_stat_response(&resp) {
                Ok(exists) => results.push(exists),
                Err(error) => {
                    let _ = refusal.get_or_insert(error);
                }
            }
        }
        match refusal {
            Some(error) => Err(error),
            None => Ok(results),
        }
    }

    /// Stream the body of an article directly to a writer.
    ///
    /// Reads the multi-line data and writes it to the provided writer.
    /// Returns the total number of bytes written.
    pub async fn stream_body<W: tokio::io::AsyncWrite + Unpin>(
        &mut self,
        message_id: &str,
        writer: &mut W,
    ) -> Result<u64> {
        let data = self.body_by_id(message_id).await?.data;
        let len = data.len() as u64;
        writer.write_all(&data).await.map_err(NntpError::Io)?;
        writer.flush().await.map_err(NntpError::Io)?;

        Ok(len)
    }

    /// Send a lightweight `DATE` command to verify the connection is still alive.
    ///
    /// If the server responds successfully, the connection is still good.
    /// If the response is an error or the connection times out, it is marked
    /// as poisoned and an error is returned.
    pub async fn ping(&mut self) -> Result<()> {
        let resp = self.send_command(&Command::Date).await.inspect_err(|_e| {
            self.poisoned = true;
            self.current_group = None;
        })?;

        if resp.code.is_error() {
            self.poisoned = true;
            self.current_group = None;
            return Err(NntpError::unexpected(resp.code, &resp.message));
        }

        Ok(())
    }

    /// Stream the body of an article, calling the callback for each chunk.
    ///
    /// Returns the total number of bytes streamed (after dot-unstuffing).
    /// The callback receives each chunk of decoded data as it arrives,
    /// avoiding buffering the entire article in memory.
    pub async fn stream_body_chunked<F>(&mut self, message_id: &str, on_chunk: F) -> Result<u64>
    where
        F: FnMut(&[u8]) -> Result<()>,
    {
        self.stream_body_chunked_with_estimate(message_id, 0, on_chunk)
            .await
    }

    pub async fn stream_body_chunked_with_estimate<F>(
        &mut self,
        message_id: &str,
        estimated_body_bytes: u64,
        on_chunk: F,
    ) -> Result<u64>
    where
        F: FnMut(&[u8]) -> Result<()>,
    {
        self.reserve_body(estimated_body_bytes)
            .map_err(NntpError::quota_blocked)?;
        let initial = self.send_reserved_body_initial(message_id).await?;
        self.stream_body_response(initial, false, None, on_chunk)
            .await
            .map(|stats| stats.bytes)
    }

    /// Stream the raw body of an article, yielding chunks on line boundaries.
    ///
    /// Chunks retain NNTP dot-stuffing and exclude the final multiline
    /// terminator. This is the download hot path used by the streaming yEnc
    /// decoder.
    async fn stream_body_response<F>(
        &mut self,
        initial: Response,
        raw: bool,
        mut budget: Option<&mut ActiveTransferBudget>,
        mut on_chunk: F,
    ) -> Result<BodyStreamStats>
    where
        F: FnMut(&[u8]) -> Result<()>,
    {
        if initial.code.is_error() {
            self.abort_active_body();
            return Err(NntpError::from_status(initial.code, &initial.message));
        }

        if initial.code.raw() != 222 {
            self.abort_active_body();
            return Err(NntpError::unexpected(initial.code, &initial.message));
        }

        self.codec.set_streaming_multiline(true);
        self.codec.set_raw_multiline(raw);
        let mut stats = BodyStreamStats::default();
        let timeout = self.command_timeout;
        let profile_cpu = profile_cpu_timings_enabled();

        let result = async {
            loop {
                let cpu_started = profile_cpu.then(thread_cpu_time).flatten();
                let decoded = if raw {
                    self.codec.decode_streaming_raw_chunk(&mut self.read_buf)
                } else {
                    self.codec.decode_streaming_chunk(&mut self.read_buf)
                };
                let payload_bytes = self.codec.last_multiline_payload_bytes();
                add_cpu_delta(&mut stats.raw_decode_cpu, cpu_started);
                // One clock read per turn: the budget check and the read
                // timeout derived below both key off it; only an actual
                // throttle wait refreshes it.
                let mut now = Instant::now();
                match decoded? {
                    Some(StreamChunk::Data(data)) => {
                        stats.bytes += data.len() as u64;
                        if let Err(error) = ensure_active_transfer_budget_at(budget.as_deref(), now)
                        {
                            self.charge_active_body_without_wait(payload_bytes);
                            return Err(error);
                        }
                        let waited = self.charge_active_body(payload_bytes).await;
                        if !waited.is_zero() {
                            stats.throttle_wait = stats.throttle_wait.saturating_add(waited);
                            if let Some(budget) = budget.as_deref_mut() {
                                budget.exclude_wait(waited);
                            }
                            now = Instant::now();
                            ensure_active_transfer_budget_at(budget.as_deref(), now)?;
                        }
                        if let Err(err) = on_chunk(&data) {
                            self.poisoned = true;
                            self.current_group = None;
                            self.reset_multiline_decode_state();
                            return Err(err);
                        }
                        continue;
                    }
                    Some(StreamChunk::End) => {
                        ensure_active_transfer_budget_at(budget.as_deref(), now)?;
                        return Ok::<BodyStreamStats, NntpError>(stats);
                    }
                    None => {}
                }

                let (read_timeout, active_timeout) =
                    active_transfer_read_timeout_at(now, timeout, budget.as_deref())?;
                if profile_cpu {
                    let (read_result, read_cpu) = tokio::time::timeout(
                        read_timeout,
                        measure_poll_cpu(self.read_into_buffer()),
                    )
                    .await
                    .map_err(|_| {
                        if active_timeout {
                            active_transfer_timeout(
                                budget.as_deref().expect("active BODY budget is present"),
                            )
                        } else {
                            NntpError::TruncatedMultilineBody
                        }
                    })?;
                    stats.read_poll_cpu += read_cpu;
                    read_result?;
                } else {
                    tokio::time::timeout(read_timeout, self.read_into_buffer())
                        .await
                        .map_err(|_| {
                            if active_timeout {
                                active_transfer_timeout(
                                    budget.as_deref().expect("active BODY budget is present"),
                                )
                            } else {
                                NntpError::TruncatedMultilineBody
                            }
                        })??;
                }
            }
        }
        .await;

        self.trim_read_buffer();
        match result {
            Ok(stats) => {
                self.finish_active_body();
                Ok(stats)
            }
            Err(err) => {
                let partial_bytes = partial_multiline_payload_len(&self.read_buf);
                self.charge_active_body_without_wait(partial_bytes);
                let err = self.classify_multiline_error(err);
                self.poison_on_soft_timeout(&err);
                self.abort_active_body();
                self.reset_multiline_decode_state();
                Err(err)
            }
        }
    }

    pub async fn stream_body_chunked_raw<F>(
        &mut self,
        message_id: &str,
        on_chunk: F,
    ) -> Result<BodyStreamStats>
    where
        F: FnMut(&[u8]) -> Result<()>,
    {
        self.reserve_body(0).map_err(NntpError::quota_blocked)?;
        let initial = self.send_reserved_body_initial(message_id).await?;
        self.stream_body_response(initial, true, None, on_chunk)
            .await
    }

    pub async fn stream_yenc_article<F>(
        &mut self,
        message_id: &str,
        on_chunk: F,
    ) -> std::result::Result<FusedYencArticle, FusedYencError>
    where
        F: FnMut(&[u8]) -> Result<()>,
    {
        self.stream_yenc_article_with_estimate(message_id, 0, on_chunk)
            .await
    }

    pub async fn stream_yenc_article_with_estimate<F>(
        &mut self,
        message_id: &str,
        estimated_body_bytes: u64,
        on_chunk: F,
    ) -> std::result::Result<FusedYencArticle, FusedYencError>
    where
        F: FnMut(&[u8]) -> Result<()>,
    {
        self.stream_yenc_article_with_estimate_inner(
            message_id,
            estimated_body_bytes,
            None,
            on_chunk,
        )
        .await
    }

    pub(crate) async fn stream_yenc_article_with_active_budget<F>(
        &mut self,
        message_id: &str,
        estimated_body_bytes: u64,
        budget: &mut ActiveTransferBudget,
        on_chunk: F,
    ) -> std::result::Result<FusedYencArticle, FusedYencError>
    where
        F: FnMut(&[u8]) -> Result<()>,
    {
        self.stream_yenc_article_with_estimate_inner(
            message_id,
            estimated_body_bytes,
            Some(budget),
            on_chunk,
        )
        .await
    }

    async fn stream_yenc_article_with_estimate_inner<F>(
        &mut self,
        message_id: &str,
        estimated_body_bytes: u64,
        budget: Option<&mut ActiveTransferBudget>,
        on_chunk: F,
    ) -> std::result::Result<FusedYencArticle, FusedYencError>
    where
        F: FnMut(&[u8]) -> Result<()>,
    {
        self.reserve_body(estimated_body_bytes)
            .map_err(NntpError::quota_blocked)?;
        let cmd = Command::Body(ArticleId::MessageId(message_id.to_string()));
        let request_started = Instant::now();
        let initial = match await_active_transfer(budget.as_deref(), self.send_command(&cmd)).await
        {
            Ok(initial) => initial,
            Err(error) => {
                self.poison_on_soft_timeout(&error);
                self.abort_active_body();
                return Err(error.into());
            }
        };
        let initial = if initial.code.raw() == 480 {
            if let Some((user, pass)) = self.credentials.clone() {
                debug!("server requested re-authentication (480), re-authenticating");
                if let Err(error) =
                    await_active_transfer(budget.as_deref(), self.authenticate(&user, &pass)).await
                {
                    self.poison_on_soft_timeout(&error);
                    self.abort_active_body();
                    return Err(error.into());
                }
                self.current_group = None;
                match await_active_transfer(budget.as_deref(), self.send_command(&cmd)).await {
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
            .await
    }

    async fn stream_yenc_article_response<F>(
        &mut self,
        initial: Response,
        mut budget: Option<&mut ActiveTransferBudget>,
        mut on_chunk: F,
    ) -> std::result::Result<FusedYencArticle, FusedYencError>
    where
        F: FnMut(&[u8]) -> Result<()>,
    {
        let timeout = self.command_timeout;
        let profile_cpu = profile_cpu_timings_enabled();
        let mut decoder = match FusedYencArticleDecoder::from_body_response(initial) {
            Ok(decoder) => decoder,
            Err(error) => {
                self.abort_active_body();
                return Err(error);
            }
        };
        decoder.set_profile_cpu(profile_cpu);
        decoder.set_checkpoint_plan(self.checkpoint_plan.clone());
        let mut read_calls = 0u64;
        let mut read_bytes = 0u64;
        let mut transport_read = TransportReadStats::default();
        let mut read_poll_cpu = Duration::ZERO;
        let mut fused_decode_cpu = Duration::ZERO;
        let mut output_callback_cpu = Duration::ZERO;
        let mut throttle_wait = Duration::ZERO;
        let mut article_chunks = Vec::new();

        let result = async {
            loop {
                let payload_before = decoder.body_payload_bytes_consumed();
                let cpu_started = profile_cpu.then(thread_cpu_time).flatten();
                let decoded = decoder.decode_available(&mut self.read_buf);
                add_cpu_delta(&mut fused_decode_cpu, cpu_started);
                let payload_delta = decoder
                    .body_payload_bytes_consumed()
                    .saturating_sub(payload_before);
                // One clock read per turn: the budget check and the read
                // timeout derived below both key off it; only an actual
                // throttle wait refreshes it.
                let mut now = Instant::now();
                if let Err(error) = ensure_active_transfer_budget_at(budget.as_deref(), now) {
                    self.charge_active_body_without_wait(payload_delta as usize);
                    return Err(error.into());
                }
                let waited = self.charge_active_body(payload_delta as usize).await;
                if !waited.is_zero() {
                    throttle_wait = throttle_wait.saturating_add(waited);
                    if let Some(budget) = budget.as_deref_mut() {
                        budget.exclude_wait(waited);
                    }
                    now = Instant::now();
                    ensure_active_transfer_budget_at(budget.as_deref(), now)?;
                }
                match decoded? {
                    Some(mut article) => {
                        let chunks = std::mem::take(&mut article.chunks);
                        if let Err(err) = deliver_fused_output_chunks(
                            chunks,
                            &mut article_chunks,
                            &mut on_chunk,
                            &mut output_callback_cpu,
                            profile_cpu,
                        ) {
                            self.poisoned = true;
                            self.current_group = None;
                            return Err(err);
                        }
                        article.stats.read_calls = read_calls;
                        article.stats.read_bytes = read_bytes;
                        article.stats.transport_read = transport_read;
                        article.stats.read_poll_cpu = read_poll_cpu;
                        article.stats.fused_decode_cpu = fused_decode_cpu;
                        article.stats.leftover_bytes_after_terminator = self.read_buf.len() as u64;
                        article.stats.output_batches = article_chunks.len() as u64;
                        article.stats.output_callback_cpu = output_callback_cpu;
                        article.stats.throttle_wait = throttle_wait;
                        article.chunks = article_chunks;
                        return Ok::<FusedYencArticle, FusedYencError>(article);
                    }
                    None => {
                        if let Err(err) = deliver_fused_output_chunks(
                            decoder.drain_output_chunks(),
                            &mut article_chunks,
                            &mut on_chunk,
                            &mut output_callback_cpu,
                            profile_cpu,
                        ) {
                            self.poisoned = true;
                            self.current_group = None;
                            return Err(err);
                        }
                    }
                }

                let (read_timeout, active_timeout) =
                    active_transfer_read_timeout_at(now, timeout, budget.as_deref())?;
                if profile_cpu {
                    let (read_result, read_cpu) = tokio::time::timeout(
                        read_timeout,
                        measure_poll_cpu(self.read_into_buffer_with_stats()),
                    )
                    .await
                    .map_err(|_| {
                        if active_timeout {
                            active_transfer_timeout(
                                budget.as_deref().expect("active BODY budget is present"),
                            )
                        } else {
                            NntpError::TruncatedMultilineBody
                        }
                    })?;
                    read_poll_cpu += read_cpu;
                    let (n, read_stats) = read_result?;
                    transport_read.add(read_stats);
                    read_calls += 1;
                    read_bytes += n as u64;
                } else {
                    let n = tokio::time::timeout(read_timeout, self.read_into_buffer())
                        .await
                        .map_err(|_| {
                            if active_timeout {
                                active_transfer_timeout(
                                    budget.as_deref().expect("active BODY budget is present"),
                                )
                            } else {
                                NntpError::TruncatedMultilineBody
                            }
                        })??;
                    read_calls += 1;
                    read_bytes += n as u64;
                }
            }
        }
        .await;

        let read_buf_capacity_before_trim = self.read_buf.capacity();
        self.trim_read_buffer();
        match result {
            Ok(mut article) => {
                self.finish_active_body();
                if self.read_buf.capacity() < read_buf_capacity_before_trim {
                    article.stats.buffer_compactions += 1;
                }
                Ok(article)
            }
            Err(err) => {
                self.abort_active_body();
                self.poisoned = true;
                self.current_group = None;
                match err {
                    FusedYencError::Nntp(err) => Err(classify_fused_body_error(err).into()),
                    other => Err(other),
                }
            }
        }
    }

    pub async fn stream_next_body_chunked_raw<F>(&mut self, on_chunk: F) -> Result<BodyStreamStats>
    where
        F: FnMut(&[u8]) -> Result<()>,
    {
        let initial = match self.read_response().await {
            Ok(initial) => initial,
            Err(error) => {
                self.abort_active_body();
                return Err(error);
            }
        };
        if initial.code.raw() == 480 {
            self.abort_active_body();
            self.poisoned = true;
            self.current_group = None;
            return Err(NntpError::AuthenticationRequired);
        }

        self.stream_body_response(initial, true, None, on_chunk)
            .await
    }

    pub async fn stream_next_yenc_article<F>(
        &mut self,
        on_chunk: F,
    ) -> std::result::Result<FusedYencArticle, FusedYencError>
    where
        F: FnMut(&[u8]) -> Result<()>,
    {
        self.stream_next_yenc_article_inner(None, on_chunk).await
    }

    pub(crate) async fn stream_next_yenc_article_with_active_budget<F>(
        &mut self,
        budget: &mut ActiveTransferBudget,
        on_chunk: F,
    ) -> std::result::Result<FusedYencArticle, FusedYencError>
    where
        F: FnMut(&[u8]) -> Result<()>,
    {
        self.stream_next_yenc_article_inner(Some(budget), on_chunk)
            .await
    }

    async fn stream_next_yenc_article_inner<F>(
        &mut self,
        budget: Option<&mut ActiveTransferBudget>,
        on_chunk: F,
    ) -> std::result::Result<FusedYencArticle, FusedYencError>
    where
        F: FnMut(&[u8]) -> Result<()>,
    {
        let response_started = Instant::now();
        let initial = match await_active_transfer(budget.as_deref(), self.read_response()).await {
            Ok(initial) => initial,
            Err(error) => {
                self.poison_on_soft_timeout(&error);
                self.abort_active_body();
                return Err(error.into());
            }
        };
        if initial.code.raw() == 480 {
            self.abort_active_body();
            self.poisoned = true;
            self.current_group = None;
            return Err(NntpError::AuthenticationRequired.into());
        }
        self.last_response_line_wait = response_started.elapsed();

        self.stream_yenc_article_response(initial, budget, on_chunk)
            .await
    }

    fn trim_read_buffer(&mut self) {
        let target = self
            .buffer_profile
            .read_buf_capacity
            .max(self.read_buf.len())
            .max(64 * 1024);
        if self.read_buf.capacity() > target.saturating_mul(2) {
            let mut trimmed = BytesMut::with_capacity(target);
            trimmed.extend_from_slice(&self.read_buf);
            self.read_buf = trimmed;
        }
    }

    async fn read_into_buffer(&mut self) -> Result<usize> {
        let socket_read_size = self.buffer_profile.socket_read_size.max(64 * 1024);
        self.read_buf.reserve(socket_read_size);
        let transport = self.transport.as_mut().ok_or(NntpError::ConnectionClosed)?;
        let n = transport
            .read_into_buf(&mut self.read_buf, socket_read_size)
            .await
            .map_err(|e| {
                self.poisoned = true;
                self.current_group = None;
                NntpError::Io(e)
            })?;

        if n == 0 {
            self.poisoned = true;
            self.current_group = None;
            return Err(NntpError::ConnectionClosed);
        }

        Ok(n)
    }

    async fn read_into_buffer_with_stats(&mut self) -> Result<(usize, TransportReadStats)> {
        let socket_read_size = self.buffer_profile.socket_read_size.max(64 * 1024);
        self.read_buf.reserve(socket_read_size);
        let transport = self.transport.as_mut().ok_or(NntpError::ConnectionClosed)?;
        let read = transport
            .read_into_buf_with_stats(&mut self.read_buf, socket_read_size)
            .await
            .map_err(|e| {
                self.poisoned = true;
                self.current_group = None;
                NntpError::Io(e)
            })?;

        if read.bytes == 0 {
            self.poisoned = true;
            self.current_group = None;
            return Err(NntpError::ConnectionClosed);
        }

        Ok((read.bytes, read.stats))
    }

    /// Send QUIT and close the connection gracefully.
    pub async fn quit(&mut self) -> Result<()> {
        self.state = ConnectionState::Closing;
        let _ = self.send_command(&Command::Quit).await;
        Ok(())
    }

    /// Check whether this connection is still healthy and usable.
    pub fn is_healthy(&self) -> bool {
        !self.poisoned && self.state == ConnectionState::Ready
    }

    /// Whether the connection has been poisoned by an I/O error.
    pub fn is_poisoned(&self) -> bool {
        self.poisoned
    }

    /// The current connection state.
    pub fn state(&self) -> ConnectionState {
        self.state
    }

    /// When this connection was created.
    pub fn created_at(&self) -> Instant {
        self.created_at
    }

    /// When this connection was last used for a command.
    pub fn last_used(&self) -> Instant {
        self.last_used
    }

    /// The server's advertised capabilities.
    pub fn capabilities(&self) -> &Capabilities {
        &self.capabilities
    }

    /// IANA name of the negotiated TLS cipher suite, if the transport is TLS.
    pub fn negotiated_cipher_suite(&self) -> Option<String> {
        self.transport
            .as_ref()
            .and_then(NntpTransport::negotiated_cipher_suite)
    }

    pub fn remote_addr(&self) -> SocketAddr {
        self.remote_addr
    }

    pub fn remote_ip(&self) -> IpAddr {
        self.remote_addr.ip()
    }
}

/// Whether an error is the server refusing the command itself (500/501)
/// rather than answering it.
///
/// 500 is mapped to its own variant; 501 arrives as an unexpected response,
/// so both shapes have to be recognised here.
pub(crate) fn reports_unsupported_command(error: &NntpError) -> bool {
    match error {
        NntpError::CommandNotRecognized => true,
        NntpError::UnexpectedResponse { code, .. } => {
            crate::server_caps::is_command_unsupported(code.raw())
        }
        _ => false,
    }
}

fn classify_fused_body_error(err: NntpError) -> NntpError {
    match err {
        NntpError::ConnectionClosed => NntpError::ServerDisconnectedMidBody,
        NntpError::Timeout => NntpError::TruncatedMultilineBody,
        other => other,
    }
}

fn partial_multiline_payload_len(buf: &[u8]) -> usize {
    for suffix in [b".\r".as_slice(), b".".as_slice()] {
        if let Some(prefix) = buf.strip_suffix(suffix)
            && (prefix.is_empty() || prefix.ends_with(b"\n"))
        {
            return prefix.len();
        }
    }
    buf.len()
}

fn pending_multiline_terminator(buf: &[u8]) -> bool {
    buf == b"."
        || buf == b".\r"
        || buf.ends_with(b"\r.")
        || buf.ends_with(b"\n.")
        || buf.ends_with(b"\r.\r")
        || buf.ends_with(b"\n.\r")
}

#[cfg(test)]
mod tests;
