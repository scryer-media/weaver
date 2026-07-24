use std::collections::VecDeque;
use std::future::Future;
use std::net::{IpAddr, SocketAddr};
use std::sync::{Arc, OnceLock};
use std::time::{Duration, Instant};

use bytes::{Bytes, BytesMut};
use tokio::io::AsyncWriteExt;
use tokio_util::codec::Decoder;
use tracing::{debug, trace, warn};

use crate::codec::{NntpCodec, NntpFrame, StreamChunk};
use crate::commands::Command;
use crate::error::{NntpError, Result};
use crate::fused_yenc::{FusedYencArticle, FusedYencArticleDecoder, FusedYencError};
use crate::response::{is_multiline_status, parse_response};
use crate::tls::{NntpTransport, TransportReadStats};
use crate::transfer::{
    ActiveTransferBudget, BodyTransferAccounting, QuotaRejection, ServerTransferControl,
    active_transfer_read_timeout, active_transfer_timeout,
};
use crate::types::{ArticleId, Capabilities, MultiLineResponse, Response};

#[derive(Debug, Clone, Copy, Default)]
pub struct BodyStreamStats {
    pub bytes: u64,
    pub raw_decode_cpu: Duration,
    pub read_poll_cpu: Duration,
    pub throttle_wait: Duration,
}

fn ensure_active_transfer_budget(budget: Option<&ActiveTransferBudget>) -> Result<()> {
    if let Some(budget) = budget
        && budget.remaining().is_zero()
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

fn deliver_fused_output_chunks<F>(
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
    transfer_control: Option<Arc<ServerTransferControl>>,
    body_accounting: VecDeque<BodyTransferAccounting>,
}

impl NntpConnection {
    /// Connect to an NNTP server, perform TLS negotiation and authentication.
    pub async fn connect(config: &ServerConfig) -> Result<Self> {
        Self::connect_with_ip_policy(config, &[], 0).await
    }

    pub(crate) async fn connect_with_ip_policy(
        config: &ServerConfig,
        excluded_ips: &[IpAddr],
        address_offset: usize,
    ) -> Result<Self> {
        let connect_timeout = config.connect_timeout.max(MIN_TIMEOUT);
        let result = tokio::time::timeout(connect_timeout, async {
            Self::connect_inner(config, excluded_ips, address_offset).await
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
    ) -> Result<Self> {
        debug!(host = %config.host, port = config.port, tls = config.tls, "connecting to NNTP server");

        // 1. Establish transport
        let transport = if config.tls {
            crate::tls::connect_tls_with_ip_policy(
                &config.host,
                config.port,
                config.tls_ca_cert.as_deref(),
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
            capabilities: Capabilities::default(),
            host: config.host.clone(),
            remote_addr,
            created_at: now,
            last_used: now,
            command_timeout: config.command_timeout.max(MIN_TIMEOUT),
            poisoned: false,
            current_group: None,
            credentials: None,
            tls_ca_cert: config.tls_ca_cert.clone(),
            transfer_control: None,
            body_accounting: VecDeque::new(),
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

        // 4. Fetch capabilities
        conn.fetch_capabilities().await?;

        // 5. MODE READER if needed
        if conn.capabilities.mode_reader_required() {
            debug!("sending MODE READER");
            let resp = conn.send_command(&Command::ModeReader).await?;
            if resp.code.is_error() && resp.code.raw() != 500 {
                warn!(code = resp.code.raw(), "MODE READER failed");
            }
        }

        // 6. Authenticate if credentials provided
        if let (Some(user), Some(pass)) = (&config.username, &config.password) {
            conn.authenticate(user, pass).await?;
            conn.credentials = Some((user.clone(), pass.clone()));

            // 7. Re-fetch capabilities — some servers change them after auth
            conn.fetch_capabilities().await?;
        }

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
        match crate::tls::upgrade_starttls(old_transport, &self.host, self.tls_ca_cert.as_deref())
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
            NntpFrame::Line(line) => parse_response(&line),
            NntpFrame::MultiLineData(_) => Err(NntpError::MalformedResponse(
                "expected single-line response, got multi-line data".into(),
            )),
        }
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
    pub async fn head_by_id(&mut self, message_id: &str) -> Result<MultiLineResponse> {
        let cmd = Command::Head(ArticleId::MessageId(message_id.to_string()));
        self.send_multiline_command(&cmd).await
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
        match resp.code.raw() {
            223 => Ok(true),
            430 => Ok(false),
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
        for _ in message_ids {
            let resp = self.read_response().await?;
            match resp.code.raw() {
                223 => results.push(true),
                430 => results.push(false),
                _ => return Err(NntpError::from_status(resp.code, &resp.message)),
            }
        }
        Ok(results)
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
                match decoded? {
                    Some(StreamChunk::Data(data)) => {
                        stats.bytes += data.len() as u64;
                        if let Err(error) = ensure_active_transfer_budget(budget.as_deref()) {
                            self.charge_active_body_without_wait(payload_bytes);
                            return Err(error);
                        }
                        let waited = self.charge_active_body(payload_bytes).await;
                        stats.throttle_wait = stats.throttle_wait.saturating_add(waited);
                        if let Some(budget) = budget.as_deref_mut() {
                            budget.exclude_wait(waited);
                        }
                        ensure_active_transfer_budget(budget.as_deref())?;
                        if let Err(err) = on_chunk(&data) {
                            self.poisoned = true;
                            self.current_group = None;
                            self.reset_multiline_decode_state();
                            return Err(err);
                        }
                        continue;
                    }
                    Some(StreamChunk::End) => {
                        ensure_active_transfer_budget(budget.as_deref())?;
                        return Ok::<BodyStreamStats, NntpError>(stats);
                    }
                    None => {}
                }

                let (read_timeout, active_timeout) =
                    active_transfer_read_timeout(timeout, budget.as_deref())?;
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
                if let Err(error) = ensure_active_transfer_budget(budget.as_deref()) {
                    self.charge_active_body_without_wait(payload_delta as usize);
                    return Err(error.into());
                }
                let waited = self.charge_active_body(payload_delta as usize).await;
                throttle_wait = throttle_wait.saturating_add(waited);
                if let Some(budget) = budget.as_deref_mut() {
                    budget.exclude_wait(waited);
                }
                ensure_active_transfer_budget(budget.as_deref())?;
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
                    active_transfer_read_timeout(timeout, budget.as_deref())?;
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

    pub fn remote_addr(&self) -> SocketAddr {
        self.remote_addr
    }

    pub fn remote_ip(&self) -> IpAddr {
        self.remote_addr.ip()
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
mod tests {
    use std::io::{self, Cursor, Read};
    use std::sync::Arc;

    use super::*;
    use crate::test_support::{
        ScriptedStep, spawn_scripted_server as spawn_shared_scripted_server,
    };
    use crate::tls::ManualTlsStream;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::{TcpListener, TcpStream};
    use tokio::sync::oneshot;
    use tokio_rustls::rustls::client::UnbufferedClientConnection;
    use tokio_rustls::rustls::pki_types::{PrivateKeyDer, PrivatePkcs8KeyDer, ServerName};
    use tokio_rustls::rustls::unbuffered::ConnectionState as UnbufferedConnectionState;
    use tokio_rustls::rustls::{
        ClientConfig, ClientConnection, RootCertStore, ServerConfig as RustlsServerConfig,
    };
    use tokio_rustls::{TlsAcceptor, TlsConnector};
    use weaver_yenc::encode;

    const FUSED_YENC_OUTPUT_BATCH_TARGET: usize = 512 * 1024;
    const TLS_DRAIN_RECORD_BYTES: usize = 16 * 1024;
    const TLS_DRAIN_RECORDS: usize = 8;
    const TLS_DRAIN_PAYLOAD_BYTES: usize = TLS_DRAIN_RECORD_BYTES * TLS_DRAIN_RECORDS;
    const TLS_TEST_BUFFER_BYTES: usize = 256 * 1024;

    struct ScriptStep {
        expect_prefix: Option<&'static str>,
        response: &'static [u8],
        delay: Duration,
    }

    impl ScriptedStep for ScriptStep {
        fn expected_prefix(&self) -> Option<&str> {
            self.expect_prefix
        }

        fn response(&self) -> &[u8] {
            self.response
        }

        fn delay(&self) -> Duration {
            self.delay
        }
    }

    fn test_tls_configs() -> (Arc<ClientConfig>, Arc<RustlsServerConfig>) {
        let (client_config, server_config, _) = test_tls_configs_with_cert_der();
        (client_config, server_config)
    }

    fn test_tls_configs_with_cert_der() -> (Arc<ClientConfig>, Arc<RustlsServerConfig>, Vec<u8>) {
        let certified_key = rcgen::generate_simple_self_signed(vec!["localhost".to_string()])
            .expect("generate test cert");
        let cert_der = certified_key.cert.der().clone();
        let key_der = PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(
            certified_key.key_pair.serialize_der(),
        ));

        let server_provider = tokio_rustls::rustls::crypto::aws_lc_rs::default_provider();
        let server_config = RustlsServerConfig::builder_with_provider(Arc::new(server_provider))
            .with_safe_default_protocol_versions()
            .unwrap()
            .with_no_client_auth()
            .with_single_cert(vec![cert_der.clone()], key_der)
            .expect("server TLS config");

        let mut roots = RootCertStore::empty();
        roots.add(cert_der.clone()).expect("client root store");
        let client_provider = tokio_rustls::rustls::crypto::aws_lc_rs::default_provider();
        let client_config = ClientConfig::builder_with_provider(Arc::new(client_provider))
            .with_safe_default_protocol_versions()
            .unwrap()
            .with_root_certificates(roots)
            .with_no_client_auth();

        (
            Arc::new(client_config),
            Arc::new(server_config),
            cert_der.as_ref().to_vec(),
        )
    }

    async fn spawn_tls_drain_server(
        server_config: Arc<RustlsServerConfig>,
    ) -> (SocketAddr, oneshot::Receiver<()>) {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (flushed_tx, flushed_rx) = oneshot::channel();

        tokio::spawn(async move {
            let (socket, _) = listener.accept().await.unwrap();
            let acceptor = TlsAcceptor::from(server_config);
            let mut tls = acceptor.accept(socket).await.unwrap();
            let record = vec![0x5Au8; TLS_DRAIN_RECORD_BYTES];

            for _ in 0..TLS_DRAIN_RECORDS {
                tls.write_all(&record).await.unwrap();
                tls.flush().await.unwrap();
            }

            let _ = flushed_tx.send(());
            tokio::time::sleep(Duration::from_millis(250)).await;
        });

        (addr, flushed_rx)
    }

    async fn connect_tls_drain_client(
        addr: SocketAddr,
        client_config: Arc<ClientConfig>,
    ) -> NntpConnection {
        let tcp = TcpStream::connect(addr).await.unwrap();
        let remote_addr = tcp.peer_addr().unwrap();
        let connector = TlsConnector::from(client_config);
        let server_name = ServerName::try_from("localhost").unwrap();
        let tls = connector.connect(server_name, tcp).await.unwrap();
        let now = Instant::now();

        NntpConnection {
            transport: Some(NntpTransport::Tls {
                inner: tls,
                remote_addr,
            }),
            codec: NntpCodec::new(),
            read_buf: BytesMut::with_capacity(256 * 1024),
            buffer_profile: NntpBufferProfile {
                read_buf_capacity: 256 * 1024,
                socket_read_size: 256 * 1024,
            },
            state: ConnectionState::Ready,
            capabilities: Capabilities::default(),
            host: "localhost".to_string(),
            remote_addr,
            created_at: now,
            last_used: now,
            command_timeout: Duration::from_secs(5),
            poisoned: false,
            current_group: None,
            credentials: None,
            tls_ca_cert: None,
            transfer_control: None,
            body_accounting: VecDeque::new(),
        }
    }

    #[derive(Debug, Default, Clone, Copy)]
    struct BufferedRustlsFeedStats {
        tls_read_calls: usize,
        process_packets_calls: usize,
        tls_bytes: usize,
        plaintext_bytes: usize,
    }

    impl BufferedRustlsFeedStats {
        fn add(&mut self, other: Self) {
            self.tls_read_calls += other.tls_read_calls;
            self.process_packets_calls += other.process_packets_calls;
            self.tls_bytes += other.tls_bytes;
            self.plaintext_bytes += other.plaintext_bytes;
        }
    }

    #[derive(Debug, Default, Clone, Copy)]
    struct UnbufferedRustlsStats {
        process_calls: usize,
        encode_states: usize,
        transmit_states: usize,
        blocked_handshake_states: usize,
        write_ready_states: usize,
        read_traffic_states: usize,
        app_records: usize,
        app_bytes: usize,
        discarded_bytes: usize,
    }

    enum UnbufferedStep {
        NeedRead,
        Send(Vec<u8>),
        ReadyToWrite,
        PeerClosed,
        Continue,
    }

    fn feed_manual_tls(
        tls: &mut ClientConnection,
        ciphertext: &[u8],
        output: &mut Vec<u8>,
    ) -> io::Result<BufferedRustlsFeedStats> {
        let mut cursor = Cursor::new(ciphertext);
        let mut stats = BufferedRustlsFeedStats::default();

        while (cursor.position() as usize) < ciphertext.len() {
            stats.tls_read_calls += 1;
            match tls.read_tls(&mut cursor) {
                Ok(0) => break,
                Ok(n) => {
                    stats.tls_bytes += n;
                    stats.process_packets_calls += 1;
                    tls.process_new_packets()
                        .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err))?;
                    stats.plaintext_bytes += drain_manual_plaintext(tls, output)?;
                }
                Err(err) if err.kind() == io::ErrorKind::Other => {
                    let drained = drain_manual_plaintext(tls, output)?;
                    if drained == 0 {
                        return Err(err);
                    }
                    stats.plaintext_bytes += drained;
                }
                Err(err) => return Err(err),
            }
        }

        Ok(stats)
    }

    fn drain_manual_plaintext(
        tls: &mut ClientConnection,
        output: &mut Vec<u8>,
    ) -> io::Result<usize> {
        let mut total = 0usize;
        let mut chunk = [0u8; TLS_DRAIN_RECORD_BYTES];

        loop {
            match tls.reader().read(&mut chunk) {
                Ok(0) => break,
                Ok(n) => {
                    output.extend_from_slice(&chunk[..n]);
                    total += n;
                }
                Err(err) if err.kind() == io::ErrorKind::WouldBlock => break,
                Err(err) => return Err(err),
            }
        }

        Ok(total)
    }

    async fn connect_manual_rustls_client(
        addr: SocketAddr,
        client_config: Arc<ClientConfig>,
    ) -> (TcpStream, ClientConnection) {
        let mut tcp = TcpStream::connect(addr).await.unwrap();
        let server_name = ServerName::try_from("localhost").unwrap();
        let mut tls = ClientConnection::new(client_config, server_name).unwrap();
        let mut ciphertext = vec![0u8; 64 * 1024];
        let mut ignored_plaintext = Vec::new();

        while tls.is_handshaking() {
            while tls.wants_write() {
                let mut outbound = Vec::new();
                tls.write_tls(&mut outbound).unwrap();
                if outbound.is_empty() {
                    break;
                }
                tcp.write_all(&outbound).await.unwrap();
                tcp.flush().await.unwrap();
            }

            if tls.is_handshaking() {
                let n = tcp.read(&mut ciphertext).await.unwrap();
                assert!(n > 0, "server closed during manual TLS handshake");
                let _ =
                    feed_manual_tls(&mut tls, &ciphertext[..n], &mut ignored_plaintext).unwrap();
            }
        }

        while tls.wants_write() {
            let mut outbound = Vec::new();
            tls.write_tls(&mut outbound).unwrap();
            if outbound.is_empty() {
                break;
            }
            tcp.write_all(&outbound).await.unwrap();
            tcp.flush().await.unwrap();
        }

        (tcp, tls)
    }

    async fn manual_rustls_try_drain_ready_plaintext(
        tcp: &mut TcpStream,
        tls: &mut ClientConnection,
        output: &mut Vec<u8>,
    ) -> io::Result<(usize, usize, BufferedRustlsFeedStats)> {
        let mut ciphertext = vec![0u8; 256 * 1024];
        let mut socket_reads = 0usize;
        let mut stats = BufferedRustlsFeedStats::default();

        tcp.readable().await?;
        loop {
            match tcp.try_read(&mut ciphertext) {
                Ok(0) => break,
                Ok(n) => {
                    socket_reads += 1;
                    stats.add(feed_manual_tls(tls, &ciphertext[..n], output)?);
                }
                Err(err) if err.kind() == io::ErrorKind::WouldBlock => break,
                Err(err) => return Err(err),
            }
        }

        Ok((output.len(), socket_reads, stats))
    }

    fn discard_unbuffered_input(input: &mut [u8], input_len: &mut usize, discard: usize) {
        if discard == 0 {
            return;
        }
        assert!(
            discard <= *input_len,
            "rustls asked to discard {discard} bytes from {input_len} buffered bytes"
        );
        input.copy_within(discard..*input_len, 0);
        *input_len -= discard;
    }

    fn process_unbuffered_tls_once(
        tls: &mut UnbufferedClientConnection,
        input: &mut [u8],
        input_len: &mut usize,
        output: &mut Vec<u8>,
        stats: &mut UnbufferedRustlsStats,
    ) -> io::Result<UnbufferedStep> {
        stats.process_calls += 1;
        let mut outgoing = vec![0u8; TLS_TEST_BUFFER_BYTES];

        let (discard, extra_discard, step) = {
            let status = tls.process_tls_records(&mut input[..*input_len]);
            let discard = status.discard;
            match status
                .state
                .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err))?
            {
                UnbufferedConnectionState::EncodeTlsData(mut encoder) => {
                    stats.encode_states += 1;
                    let n = encoder.encode(&mut outgoing).map_err(|err| {
                        io::Error::new(
                            io::ErrorKind::InvalidData,
                            format!("failed to encode TLS handshake data: {err:?}"),
                        )
                    })?;
                    outgoing.truncate(n);
                    (discard, 0, UnbufferedStep::Send(outgoing))
                }
                UnbufferedConnectionState::TransmitTlsData(transmit) => {
                    stats.transmit_states += 1;
                    transmit.done();
                    (discard, 0, UnbufferedStep::Continue)
                }
                UnbufferedConnectionState::BlockedHandshake => {
                    stats.blocked_handshake_states += 1;
                    (discard, 0, UnbufferedStep::NeedRead)
                }
                UnbufferedConnectionState::WriteTraffic(_) => {
                    stats.write_ready_states += 1;
                    (discard, 0, UnbufferedStep::ReadyToWrite)
                }
                UnbufferedConnectionState::ReadTraffic(mut traffic) => {
                    stats.read_traffic_states += 1;
                    let mut record_discard = 0usize;
                    while let Some(record) = traffic.next_record() {
                        let record = record
                            .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err))?;
                        record_discard += record.discard;
                        stats.app_records += 1;
                        stats.app_bytes += record.payload.len();
                        output.extend_from_slice(record.payload);
                    }
                    (discard, record_discard, UnbufferedStep::Continue)
                }
                UnbufferedConnectionState::PeerClosed | UnbufferedConnectionState::Closed => {
                    (discard, 0, UnbufferedStep::PeerClosed)
                }
                _ => {
                    return Err(io::Error::other(
                        "unexpected rustls unbuffered state in client test probe",
                    ));
                }
            }
        };

        let total_discard = discard + extra_discard;
        discard_unbuffered_input(input, input_len, total_discard);
        stats.discarded_bytes += total_discard;
        Ok(step)
    }

    async fn connect_unbuffered_rustls_client(
        addr: SocketAddr,
        client_config: Arc<ClientConfig>,
    ) -> (TcpStream, UnbufferedClientConnection, UnbufferedRustlsStats) {
        let mut tcp = TcpStream::connect(addr).await.unwrap();
        let server_name = ServerName::try_from("localhost").unwrap();
        let mut tls = UnbufferedClientConnection::new(client_config, server_name).unwrap();
        let mut input = vec![0u8; TLS_TEST_BUFFER_BYTES];
        let mut input_len = 0usize;
        let mut ignored_plaintext = Vec::new();
        let mut stats = UnbufferedRustlsStats::default();

        loop {
            match process_unbuffered_tls_once(
                &mut tls,
                &mut input,
                &mut input_len,
                &mut ignored_plaintext,
                &mut stats,
            )
            .unwrap()
            {
                UnbufferedStep::NeedRead => {
                    if input_len == input.len() {
                        input.resize(input.len() * 2, 0);
                    }
                    let n = tcp.read(&mut input[input_len..]).await.unwrap();
                    assert!(n > 0, "server closed during unbuffered TLS handshake");
                    input_len += n;
                }
                UnbufferedStep::Send(bytes) => {
                    tcp.write_all(&bytes).await.unwrap();
                    tcp.flush().await.unwrap();
                }
                UnbufferedStep::ReadyToWrite => break,
                UnbufferedStep::PeerClosed => panic!("server closed during unbuffered handshake"),
                UnbufferedStep::Continue => {}
            }
        }

        (tcp, tls, stats)
    }

    async fn unbuffered_rustls_try_drain_ready_plaintext(
        tcp: &mut TcpStream,
        tls: &mut UnbufferedClientConnection,
        output: &mut Vec<u8>,
        stats: &mut UnbufferedRustlsStats,
    ) -> io::Result<(usize, usize, usize)> {
        let mut input = vec![0u8; TLS_TEST_BUFFER_BYTES];
        let mut input_len = 0usize;
        let mut socket_reads = 0usize;
        let mut tls_bytes = 0usize;

        tcp.readable().await?;
        loop {
            if input_len == input.len() {
                input.resize(input.len() * 2, 0);
            }
            match tcp.try_read(&mut input[input_len..]) {
                Ok(0) => break,
                Ok(n) => {
                    socket_reads += 1;
                    tls_bytes += n;
                    input_len += n;
                }
                Err(err) if err.kind() == io::ErrorKind::WouldBlock => break,
                Err(err) => return Err(err),
            }
        }

        loop {
            match process_unbuffered_tls_once(tls, &mut input, &mut input_len, output, stats)? {
                UnbufferedStep::NeedRead | UnbufferedStep::ReadyToWrite => break,
                UnbufferedStep::Send(bytes) => {
                    tcp.write_all(&bytes).await?;
                    tcp.flush().await?;
                }
                UnbufferedStep::PeerClosed => break,
                UnbufferedStep::Continue => {}
            }
        }

        Ok((output.len(), socket_reads, tls_bytes))
    }

    #[tokio::test]
    async fn tls_read_into_buffer_bulk_drain_probe() {
        let (client_config, server_config) = test_tls_configs();
        let (addr, flushed_rx) = spawn_tls_drain_server(server_config).await;
        let mut conn = connect_tls_drain_client(addr, client_config).await;

        flushed_rx.await.expect("test server flushed TLS payload");

        let first_read = conn.read_into_buffer().await.unwrap();
        let mut read_calls = 1usize;
        while conn.read_buf.len() < TLS_DRAIN_PAYLOAD_BYTES {
            let n = conn.read_into_buffer().await.unwrap();
            read_calls += 1;
            assert!(n > 0, "transport closed before reading diagnostic payload");
        }

        println!(
            "tls_drain_probe first_read_bytes={first_read} total_bytes={} read_calls={read_calls} target_socket_read_size={}",
            conn.read_buf.len(),
            conn.buffer_profile.socket_read_size
        );

        assert!(first_read > 0);
        assert!(first_read <= TLS_DRAIN_PAYLOAD_BYTES);
        assert_eq!(conn.read_buf.len(), TLS_DRAIN_PAYLOAD_BYTES);
    }

    #[tokio::test]
    async fn manual_rustls_bulk_drain_probe() {
        let (client_config, server_config) = test_tls_configs();
        let (addr, flushed_rx) = spawn_tls_drain_server(server_config).await;
        let (mut tcp, mut tls) = connect_manual_rustls_client(addr, client_config).await;

        flushed_rx.await.expect("test server flushed TLS payload");

        let mut output = Vec::with_capacity(TLS_DRAIN_PAYLOAD_BYTES);
        let mut first_drain_plaintext = 0usize;
        let mut total_socket_reads = 0usize;
        let mut stats = BufferedRustlsFeedStats::default();
        let mut drain_calls = 0usize;

        while output.len() < TLS_DRAIN_PAYLOAD_BYTES {
            let before = output.len();
            let (drain_plaintext, socket_reads, read_stats) =
                manual_rustls_try_drain_ready_plaintext(&mut tcp, &mut tls, &mut output)
                    .await
                    .unwrap();
            if drain_calls == 0 {
                first_drain_plaintext = drain_plaintext;
            }
            drain_calls += 1;
            total_socket_reads += socket_reads;
            stats.add(read_stats);
            assert!(
                output.len() > before || read_stats.tls_bytes > 0,
                "buffered rustls probe made no progress while draining TLS payload"
            );
        }

        println!(
            "manual_rustls_drain_probe first_drain_plaintext={first_drain_plaintext} drain_calls={drain_calls} socket_reads={total_socket_reads} tls_bytes={} tls_read_calls={} process_packets_calls={} payload_bytes={TLS_DRAIN_PAYLOAD_BYTES}",
            stats.tls_bytes, stats.tls_read_calls, stats.process_packets_calls
        );

        assert_eq!(output.len(), TLS_DRAIN_PAYLOAD_BYTES);
        assert!(total_socket_reads > 0);
        assert!(stats.tls_bytes >= TLS_DRAIN_PAYLOAD_BYTES);
        assert!(
            stats.tls_read_calls > TLS_DRAIN_RECORDS,
            "buffered rustls should need multiple 4 KiB reads per 16 KiB record"
        );
    }

    #[tokio::test]
    async fn unbuffered_rustls_bulk_drain_probe() {
        let (client_config, server_config) = test_tls_configs();
        let (addr, flushed_rx) = spawn_tls_drain_server(server_config).await;
        let (mut tcp, mut tls, mut stats) =
            connect_unbuffered_rustls_client(addr, client_config).await;
        let handshake_stats = stats;
        stats = UnbufferedRustlsStats::default();

        flushed_rx.await.expect("test server flushed TLS payload");

        let mut output = Vec::with_capacity(TLS_DRAIN_PAYLOAD_BYTES);
        let mut first_drain_plaintext = 0usize;
        let mut total_socket_reads = 0usize;
        let mut total_tls_bytes = 0usize;
        let mut drain_calls = 0usize;

        while output.len() < TLS_DRAIN_PAYLOAD_BYTES {
            let before = output.len();
            let (drain_plaintext, socket_reads, tls_bytes) =
                unbuffered_rustls_try_drain_ready_plaintext(
                    &mut tcp,
                    &mut tls,
                    &mut output,
                    &mut stats,
                )
                .await
                .unwrap();
            if drain_calls == 0 {
                first_drain_plaintext = drain_plaintext;
            }
            drain_calls += 1;
            total_socket_reads += socket_reads;
            total_tls_bytes += tls_bytes;
            // A readiness wake can deliver only part of a TLS record; that
            // consumes ciphertext without yielding plaintext yet.
            assert!(
                output.len() > before || tls_bytes > 0,
                "unbuffered probe made no progress while draining TLS payload"
            );
        }

        println!(
            "unbuffered_rustls_drain_probe first_drain_plaintext={first_drain_plaintext} drain_calls={drain_calls} socket_reads={total_socket_reads} tls_bytes={total_tls_bytes} handshake_process_calls={} payload_process_calls={} read_traffic_states={} app_records={} app_bytes={} discarded_bytes={} payload_bytes={TLS_DRAIN_PAYLOAD_BYTES}",
            handshake_stats.process_calls,
            stats.process_calls,
            stats.read_traffic_states,
            stats.app_records,
            stats.app_bytes,
            stats.discarded_bytes
        );

        assert_eq!(output.len(), TLS_DRAIN_PAYLOAD_BYTES);
        assert!(total_socket_reads > 0);
        assert!(total_tls_bytes >= TLS_DRAIN_PAYLOAD_BYTES);
        assert_eq!(stats.app_records, TLS_DRAIN_RECORDS);
    }

    #[tokio::test]
    async fn unbuffered_rustls_first_ready_pass_probe() {
        let (client_config, server_config) = test_tls_configs();
        let (addr, flushed_rx) = spawn_tls_drain_server(server_config).await;
        let (mut tcp, mut tls, mut stats) =
            connect_unbuffered_rustls_client(addr, client_config).await;
        let handshake_stats = stats;
        stats = UnbufferedRustlsStats::default();

        flushed_rx.await.expect("test server flushed TLS payload");

        let mut output = Vec::with_capacity(TLS_DRAIN_PAYLOAD_BYTES);
        let (first_drain_plaintext, socket_reads, tls_bytes) =
            unbuffered_rustls_try_drain_ready_plaintext(
                &mut tcp,
                &mut tls,
                &mut output,
                &mut stats,
            )
            .await
            .unwrap();

        println!(
            "unbuffered_rustls_first_ready_probe first_drain_plaintext={first_drain_plaintext} socket_reads={socket_reads} tls_bytes={tls_bytes} handshake_process_calls={} payload_process_calls={} read_traffic_states={} app_records={} app_bytes={} discarded_bytes={} payload_bytes={TLS_DRAIN_PAYLOAD_BYTES}",
            handshake_stats.process_calls,
            stats.process_calls,
            stats.read_traffic_states,
            stats.app_records,
            stats.app_bytes,
            stats.discarded_bytes
        );

        assert!(socket_reads > 0);
        assert!(tls_bytes > 0);
        assert_eq!(first_drain_plaintext, output.len());
    }

    #[tokio::test]
    async fn manual_tls_transport_bulk_drain_probe() {
        let (client_config, server_config) = test_tls_configs();
        let (addr, flushed_rx) = spawn_tls_drain_server(server_config).await;
        let tcp = TcpStream::connect(addr).await.unwrap();
        let remote_addr = tcp.peer_addr().unwrap();
        let server_name = ServerName::try_from("localhost").unwrap();
        let inner = ManualTlsStream::connect(tcp, client_config, server_name)
            .await
            .unwrap();
        let mut transport = NntpTransport::ManualTls { inner, remote_addr };

        flushed_rx.await.expect("test server flushed TLS payload");

        let mut read_buf = BytesMut::with_capacity(256 * 1024);
        let read = transport
            .read_into_buf_with_stats(&mut read_buf, 256 * 1024)
            .await
            .unwrap();

        println!(
            "manual_tls_transport_probe first_read_bytes={} total_bytes={} tls_read_calls={} process_packets_calls={} plaintext_reader_calls={} reader_would_block={}",
            read.bytes,
            read_buf.len(),
            read.stats.tls_read_calls,
            read.stats.tls_process_packets_calls,
            read.stats.plaintext_reader_calls,
            read.stats.plaintext_reader_would_block
        );

        assert_eq!(read.bytes, TLS_DRAIN_PAYLOAD_BYTES);
        assert_eq!(read_buf.len(), TLS_DRAIN_PAYLOAD_BYTES);
    }

    async fn spawn_scripted_server(steps: Vec<ScriptStep>, hold_open_after_last: Duration) -> u16 {
        spawn_shared_scripted_server(steps, hold_open_after_last).await
    }

    fn scripted_plain_config(port: u16) -> ServerConfig {
        ServerConfig {
            host: "127.0.0.1".into(),
            port,
            tls: false,
            connect_timeout: Duration::from_secs(1),
            command_timeout: Duration::from_millis(100),
            ..Default::default()
        }
    }

    fn leaked_response(bytes: Vec<u8>) -> &'static [u8] {
        Box::leak(bytes.into_boxed_slice())
    }

    fn yenc_body_response(decoded: &[u8], terminator: &[u8]) -> &'static [u8] {
        let mut article = Vec::new();
        encode(decoded, &mut article, 128, "test.bin").unwrap();

        let mut response = b"222 body follows\r\n".to_vec();
        response.extend_from_slice(&article);
        response.extend_from_slice(terminator);
        leaked_response(response)
    }

    #[test]
    fn group_tracking_initial_state() {
        // Verify that current_group starts as None and the accessor works.
        // We can't fully construct an NntpConnection without a server, so we
        // test the field semantics through the public accessor on a real
        // connection in the ignored integration test below. Here we just
        // verify the ServerConfig and ConnectionState basics hold.
        //
        // The actual group tracking behaviour (set after select_group, reset
        // on poison) is validated in the integration test.
        assert_eq!(ConnectionState::Disconnected, ConnectionState::Disconnected);
    }

    #[tokio::test]
    #[ignore] // Requires a real NNTP server
    async fn group_tracking() {
        let config = ServerConfig {
            host: "news.example.com".into(),
            port: 563,
            tls: true,
            username: Some("user".into()),
            password: Some("pass".into()),
            ..Default::default()
        };
        let mut conn = NntpConnection::connect(&config).await.unwrap();

        // current_group starts as None
        assert!(conn.current_group().is_none());

        // After selecting a group it should be set
        conn.select_group("alt.binaries.test").await.unwrap();
        assert_eq!(conn.current_group(), Some("alt.binaries.test"));

        // Selecting the same group again should be a no-op (no extra command)
        conn.select_group("alt.binaries.test").await.unwrap();
        assert_eq!(conn.current_group(), Some("alt.binaries.test"));

        // Selecting a different group should update
        conn.select_group("alt.binaries.other").await.unwrap();
        assert_eq!(conn.current_group(), Some("alt.binaries.other"));
    }

    #[test]
    fn server_config_defaults() {
        let cfg = ServerConfig::default();
        assert_eq!(cfg.port, 563);
        assert!(cfg.tls);
        assert!(!cfg.starttls);
        assert!(cfg.username.is_none());
        assert!(cfg.password.is_none());
    }

    #[tokio::test]
    async fn connect_accepts_201_greeting() {
        let port = spawn_scripted_server(
            vec![
                ScriptStep {
                    expect_prefix: None,
                    response: b"201 no posting\r\n",
                    delay: Duration::ZERO,
                },
                ScriptStep {
                    expect_prefix: Some("CAPABILITIES"),
                    response: b"500 unknown\r\n",
                    delay: Duration::ZERO,
                },
            ],
            Duration::ZERO,
        )
        .await;

        let conn = NntpConnection::connect(&scripted_plain_config(port)).await;
        assert!(conn.is_ok());
    }

    #[tokio::test]
    async fn connect_maps_400_greeting_to_service_unavailable() {
        let port = spawn_scripted_server(
            vec![ScriptStep {
                expect_prefix: None,
                response: b"400 service unavailable\r\n",
                delay: Duration::ZERO,
            }],
            Duration::ZERO,
        )
        .await;

        let err = match NntpConnection::connect(&scripted_plain_config(port)).await {
            Ok(_) => panic!("expected connect to fail"),
            Err(err) => err,
        };
        assert!(matches!(err, NntpError::ServiceUnavailable));
    }

    #[tokio::test]
    async fn body_by_id_reauthenticates_on_mid_session_480() {
        let port = spawn_scripted_server(
            vec![
                ScriptStep {
                    expect_prefix: None,
                    response: b"200 ready\r\n",
                    delay: Duration::ZERO,
                },
                ScriptStep {
                    expect_prefix: Some("CAPABILITIES"),
                    response: b"500 unknown\r\n",
                    delay: Duration::ZERO,
                },
                ScriptStep {
                    expect_prefix: Some("AUTHINFO USER"),
                    response: b"381 password required\r\n",
                    delay: Duration::ZERO,
                },
                ScriptStep {
                    expect_prefix: Some("AUTHINFO PASS"),
                    response: b"281 authentication accepted\r\n",
                    delay: Duration::ZERO,
                },
                ScriptStep {
                    expect_prefix: Some("CAPABILITIES"),
                    response: b"500 unknown\r\n",
                    delay: Duration::ZERO,
                },
                ScriptStep {
                    expect_prefix: Some("BODY "),
                    response: b"480 authentication required\r\n",
                    delay: Duration::ZERO,
                },
                ScriptStep {
                    expect_prefix: Some("AUTHINFO USER"),
                    response: b"381 password required\r\n",
                    delay: Duration::ZERO,
                },
                ScriptStep {
                    expect_prefix: Some("AUTHINFO PASS"),
                    response: b"281 authentication accepted\r\n",
                    delay: Duration::ZERO,
                },
                ScriptStep {
                    expect_prefix: Some("BODY "),
                    response: b"222 body follows\r\nhello\r\n.\r\n",
                    delay: Duration::ZERO,
                },
            ],
            Duration::ZERO,
        )
        .await;

        let mut config = scripted_plain_config(port);
        config.username = Some("user".into());
        config.password = Some("pass".into());

        let mut conn = NntpConnection::connect(&config).await.unwrap();
        let response = conn.body_by_id("<test@example.com>").await.unwrap();
        assert_eq!(&response.data[..], b"hello\r\n");
    }

    #[tokio::test]
    async fn stream_body_chunked_raw_reauthenticates_on_mid_session_480() {
        let port = spawn_scripted_server(
            vec![
                ScriptStep {
                    expect_prefix: None,
                    response: b"200 ready\r\n",
                    delay: Duration::ZERO,
                },
                ScriptStep {
                    expect_prefix: Some("CAPABILITIES"),
                    response: b"500 unknown\r\n",
                    delay: Duration::ZERO,
                },
                ScriptStep {
                    expect_prefix: Some("AUTHINFO USER"),
                    response: b"381 password required\r\n",
                    delay: Duration::ZERO,
                },
                ScriptStep {
                    expect_prefix: Some("AUTHINFO PASS"),
                    response: b"281 authentication accepted\r\n",
                    delay: Duration::ZERO,
                },
                ScriptStep {
                    expect_prefix: Some("CAPABILITIES"),
                    response: b"500 unknown\r\n",
                    delay: Duration::ZERO,
                },
                ScriptStep {
                    expect_prefix: Some("BODY "),
                    response: b"480 authentication required\r\n",
                    delay: Duration::ZERO,
                },
                ScriptStep {
                    expect_prefix: Some("AUTHINFO USER"),
                    response: b"381 password required\r\n",
                    delay: Duration::ZERO,
                },
                ScriptStep {
                    expect_prefix: Some("AUTHINFO PASS"),
                    response: b"281 authentication accepted\r\n",
                    delay: Duration::ZERO,
                },
                ScriptStep {
                    expect_prefix: Some("BODY "),
                    response: b"222 body follows\r\nhello\r\n.\r\n",
                    delay: Duration::ZERO,
                },
            ],
            Duration::ZERO,
        )
        .await;

        let mut config = scripted_plain_config(port);
        config.username = Some("user".into());
        config.password = Some("pass".into());

        let mut conn = NntpConnection::connect(&config).await.unwrap();
        let mut chunks = Vec::new();
        let total = conn
            .stream_body_chunked_raw("<test@example.com>", |chunk| {
                chunks.push(chunk.to_vec());
                Ok(())
            })
            .await
            .unwrap();

        assert_eq!(total.bytes, b"hello\r\n".len() as u64);
        assert_eq!(chunks, vec![b"hello\r\n".to_vec()]);
    }

    #[tokio::test]
    async fn body_by_id_handles_split_terminator() {
        let port = spawn_scripted_server(
            vec![
                ScriptStep {
                    expect_prefix: None,
                    response: b"200 ready\r\n",
                    delay: Duration::ZERO,
                },
                ScriptStep {
                    expect_prefix: Some("CAPABILITIES"),
                    response: b"500 unknown\r\n",
                    delay: Duration::ZERO,
                },
                ScriptStep {
                    expect_prefix: Some("BODY "),
                    response: b"222 body follows\r\nhello\r\n.",
                    delay: Duration::ZERO,
                },
                ScriptStep {
                    expect_prefix: None,
                    response: b"\r\n",
                    delay: Duration::from_millis(5),
                },
            ],
            Duration::ZERO,
        )
        .await;

        let mut conn = NntpConnection::connect(&scripted_plain_config(port))
            .await
            .unwrap();
        let response = conn.body_by_id("<test@example.com>").await.unwrap();
        assert_eq!(&response.data[..], b"hello\r\n");
    }

    #[tokio::test]
    async fn body_by_id_reports_disconnect_mid_body() {
        let port = spawn_scripted_server(
            vec![
                ScriptStep {
                    expect_prefix: None,
                    response: b"200 ready\r\n",
                    delay: Duration::ZERO,
                },
                ScriptStep {
                    expect_prefix: Some("CAPABILITIES"),
                    response: b"500 unknown\r\n",
                    delay: Duration::ZERO,
                },
                ScriptStep {
                    expect_prefix: Some("BODY "),
                    response: b"222 body follows\r\n..partial\r\n",
                    delay: Duration::ZERO,
                },
            ],
            Duration::ZERO,
        )
        .await;

        let mut conn = NntpConnection::connect(&scripted_plain_config(port))
            .await
            .unwrap();
        let registry = crate::transfer::ServerTransferRegistry::new();
        let control = registry.configure(
            crate::transfer::StableServerId(42),
            crate::transfer::ServerTransferConfig::default(),
        );
        conn.set_transfer_control(Some(control.clone()));
        let err = conn.body_by_id("<test@example.com>").await.unwrap_err();
        assert!(matches!(err, NntpError::ServerDisconnectedMidBody));
        assert_eq!(
            control.snapshot().lifetime_body_bytes,
            b"..partial\r\n".len() as u64
        );
    }

    #[tokio::test]
    async fn buffered_body_counts_wire_bytes_before_dot_unstuffing() {
        let port = spawn_scripted_server(
            vec![
                ScriptStep {
                    expect_prefix: None,
                    response: b"200 ready\r\n",
                    delay: Duration::ZERO,
                },
                ScriptStep {
                    expect_prefix: Some("CAPABILITIES"),
                    response: b"500 unknown\r\n",
                    delay: Duration::ZERO,
                },
                ScriptStep {
                    expect_prefix: Some("BODY "),
                    response: b"222 body follows\r\n..dot-stuffed\r\n.\r\n",
                    delay: Duration::ZERO,
                },
            ],
            Duration::ZERO,
        )
        .await;

        let mut conn = NntpConnection::connect(&scripted_plain_config(port))
            .await
            .unwrap();
        let registry = crate::transfer::ServerTransferRegistry::new();
        let control = registry.configure(
            crate::transfer::StableServerId(43),
            crate::transfer::ServerTransferConfig::default(),
        );
        conn.set_transfer_control(Some(control.clone()));
        conn.write_body_request("<buffered@example.com>")
            .await
            .unwrap();
        conn.flush_commands().await.unwrap();
        assert_eq!(conn.read_response().await.unwrap().code.raw(), 222);
        let data = conn.read_multiline_data().await.unwrap();
        assert_eq!(data.as_ref(), b".dot-stuffed\r\n");
        assert_eq!(
            control.snapshot().lifetime_body_bytes,
            b"..dot-stuffed\r\n".len() as u64
        );
    }

    #[tokio::test]
    async fn buffered_partial_failure_counts_wire_bytes_before_dot_unstuffing() {
        let port = spawn_scripted_server(
            vec![
                ScriptStep {
                    expect_prefix: None,
                    response: b"200 ready\r\n",
                    delay: Duration::ZERO,
                },
                ScriptStep {
                    expect_prefix: Some("CAPABILITIES"),
                    response: b"500 unknown\r\n",
                    delay: Duration::ZERO,
                },
                ScriptStep {
                    expect_prefix: Some("BODY "),
                    response: b"222 body follows\r\n..dot\r\nincomplete",
                    delay: Duration::ZERO,
                },
            ],
            Duration::ZERO,
        )
        .await;

        let mut conn = NntpConnection::connect(&scripted_plain_config(port))
            .await
            .unwrap();
        let registry = crate::transfer::ServerTransferRegistry::new();
        let control = registry.configure(
            crate::transfer::StableServerId(44),
            crate::transfer::ServerTransferConfig::default(),
        );
        conn.set_transfer_control(Some(control.clone()));
        conn.write_body_request("<buffered-partial@example.com>")
            .await
            .unwrap();
        conn.flush_commands().await.unwrap();
        assert_eq!(conn.read_response().await.unwrap().code.raw(), 222);
        let err = conn.read_multiline_data().await.unwrap_err();
        assert!(matches!(err, NntpError::ServerDisconnectedMidBody));
        assert_eq!(
            control.snapshot().lifetime_body_bytes,
            b"..dot\r\nincomplete".len() as u64
        );
    }

    #[tokio::test]
    async fn raw_body_counts_exact_wire_payload() {
        let port = spawn_scripted_server(
            vec![
                ScriptStep {
                    expect_prefix: None,
                    response: b"200 ready\r\n",
                    delay: Duration::ZERO,
                },
                ScriptStep {
                    expect_prefix: Some("CAPABILITIES"),
                    response: b"500 unknown\r\n",
                    delay: Duration::ZERO,
                },
                ScriptStep {
                    expect_prefix: Some("BODY "),
                    response: b"222 body follows\r\n..raw-dot\r\n.\r\n",
                    delay: Duration::ZERO,
                },
            ],
            Duration::ZERO,
        )
        .await;

        let mut conn = NntpConnection::connect(&scripted_plain_config(port))
            .await
            .unwrap();
        let registry = crate::transfer::ServerTransferRegistry::new();
        let control = registry.configure(
            crate::transfer::StableServerId(45),
            crate::transfer::ServerTransferConfig::default(),
        );
        conn.set_transfer_control(Some(control.clone()));
        let body = conn.body_by_id_raw("<raw@example.com>").await.unwrap();
        assert_eq!(body.data.as_ref(), b"..raw-dot\r\n");
        assert_eq!(
            control.snapshot().lifetime_body_bytes,
            b"..raw-dot\r\n".len() as u64
        );
    }

    #[tokio::test]
    async fn expired_active_budget_does_not_enter_async_rate_wait() {
        let port = spawn_scripted_server(
            vec![
                ScriptStep {
                    expect_prefix: None,
                    response: b"200 ready\r\n",
                    delay: Duration::ZERO,
                },
                ScriptStep {
                    expect_prefix: Some("CAPABILITIES"),
                    response: b"500 unknown\r\n",
                    delay: Duration::ZERO,
                },
                ScriptStep {
                    expect_prefix: Some("BODY "),
                    response: b"222 body follows\r\n",
                    delay: Duration::ZERO,
                },
            ],
            Duration::from_secs(1),
        )
        .await;

        let mut conn = NntpConnection::connect(&scripted_plain_config(port))
            .await
            .unwrap();
        let registry = crate::transfer::ServerTransferRegistry::new();
        let control = registry.configure(
            crate::transfer::StableServerId(46),
            crate::transfer::ServerTransferConfig {
                rate_bytes_per_sec: 100,
                quota: None,
            },
        );
        conn.set_transfer_control(Some(control.clone()));
        conn.write_body_request("<expired-budget@example.com>")
            .await
            .unwrap();
        conn.flush_commands().await.unwrap();
        let initial = conn.read_response().await.unwrap();

        let decoded = vec![b'A'; 1_200];
        let mut buffered_body = Vec::new();
        encode(&decoded, &mut buffered_body, 128, "expired.bin").unwrap();
        buffered_body.extend_from_slice(b".\r\n");
        conn.read_buf = BytesMut::from(buffered_body.as_slice());
        let mut budget = ActiveTransferBudget::new(Duration::ZERO);

        let error = tokio::time::timeout(
            Duration::from_secs(2),
            conn.stream_yenc_article_response(initial, Some(&mut budget), |_| Ok(())),
        )
        .await
        .expect("expired budget must not wait on the rate limiter")
        .unwrap_err();

        assert!(matches!(
            error,
            FusedYencError::Nntp(NntpError::SoftTimeout(_))
        ));
        assert!(conn.is_poisoned());
        let snapshot = control.snapshot();
        assert!(snapshot.lifetime_body_bytes >= decoded.len() as u64);
        assert_eq!(snapshot.throttle_wait, Duration::ZERO);
    }

    #[tokio::test]
    async fn quota_rejection_happens_before_body_command() {
        let port = spawn_scripted_server(
            vec![
                ScriptStep {
                    expect_prefix: None,
                    response: b"200 ready\r\n",
                    delay: Duration::ZERO,
                },
                ScriptStep {
                    expect_prefix: Some("CAPABILITIES"),
                    response: b"500 unknown\r\n",
                    delay: Duration::ZERO,
                },
            ],
            Duration::from_millis(50),
        )
        .await;

        let mut conn = NntpConnection::connect(&scripted_plain_config(port))
            .await
            .unwrap();
        let registry = crate::transfer::ServerTransferRegistry::new();
        let control = registry.configure(
            crate::transfer::StableServerId(43),
            crate::transfer::ServerTransferConfig {
                rate_bytes_per_sec: 0,
                quota: Some(crate::transfer::QuotaRuntimeConfig {
                    limit_bytes: 0,
                    generation: 1,
                    retry_at: None,
                }),
            },
        );
        conn.set_transfer_control(Some(control));

        let error = conn.body_by_id("<blocked@example.com>").await.unwrap_err();
        assert!(matches!(error, NntpError::QuotaBlocked(_)));
    }

    #[tokio::test]
    async fn body_by_id_reports_malformed_terminator() {
        let port = spawn_scripted_server(
            vec![
                ScriptStep {
                    expect_prefix: None,
                    response: b"200 ready\r\n",
                    delay: Duration::ZERO,
                },
                ScriptStep {
                    expect_prefix: Some("CAPABILITIES"),
                    response: b"500 unknown\r\n",
                    delay: Duration::ZERO,
                },
                ScriptStep {
                    expect_prefix: Some("BODY "),
                    response: b"222 body follows\r\npartial\r\n.\r",
                    delay: Duration::ZERO,
                },
            ],
            Duration::ZERO,
        )
        .await;

        let mut conn = NntpConnection::connect(&scripted_plain_config(port))
            .await
            .unwrap();
        let err = conn.body_by_id("<test@example.com>").await.unwrap_err();
        assert!(
            matches!(err, NntpError::MalformedMultilineTerminator),
            "unexpected error: {err:?}"
        );
    }

    #[tokio::test]
    async fn body_by_id_reports_truncated_multiline_body_on_timeout() {
        let port = spawn_scripted_server(
            vec![
                ScriptStep {
                    expect_prefix: None,
                    response: b"200 ready\r\n",
                    delay: Duration::ZERO,
                },
                ScriptStep {
                    expect_prefix: Some("CAPABILITIES"),
                    response: b"500 unknown\r\n",
                    delay: Duration::ZERO,
                },
                ScriptStep {
                    expect_prefix: Some("BODY "),
                    response: b"222 body follows\r\npartial\r\n",
                    delay: Duration::ZERO,
                },
            ],
            Duration::from_millis(1500),
        )
        .await;

        let mut config = scripted_plain_config(port);
        config.command_timeout = Duration::from_secs(1);

        let mut conn = NntpConnection::connect(&config).await.unwrap();
        let err = conn.body_by_id("<test@example.com>").await.unwrap_err();
        assert!(
            matches!(err, NntpError::TruncatedMultilineBody),
            "expected TruncatedMultilineBody, got {err:?}"
        );
    }

    #[tokio::test]
    async fn stream_body_chunked_raw_finishes_when_data_and_terminator_arrive_together() {
        let port = spawn_scripted_server(
            vec![
                ScriptStep {
                    expect_prefix: None,
                    response: b"200 ready\r\n",
                    delay: Duration::ZERO,
                },
                ScriptStep {
                    expect_prefix: Some("CAPABILITIES"),
                    response: b"500 unknown\r\n",
                    delay: Duration::ZERO,
                },
                ScriptStep {
                    expect_prefix: Some("BODY "),
                    response: b"222 body follows\r\nline one\r\nline two\r\n.\r\n",
                    delay: Duration::ZERO,
                },
            ],
            Duration::from_secs(1),
        )
        .await;

        let mut conn = NntpConnection::connect(&scripted_plain_config(port))
            .await
            .unwrap();
        let mut chunks = Vec::new();
        let total = conn
            .stream_body_chunked_raw("<test@example.com>", |chunk| {
                chunks.push(chunk.to_vec());
                Ok(())
            })
            .await
            .unwrap();

        assert_eq!(total.bytes, b"line one\r\nline two\r\n".len() as u64);
        assert_eq!(chunks, vec![b"line one\r\nline two\r\n".to_vec()]);
    }

    #[tokio::test]
    async fn stream_yenc_article_decodes_body_and_keeps_connection_usable() {
        let original = b"hello fused connection";
        let port = spawn_scripted_server(
            vec![
                ScriptStep {
                    expect_prefix: None,
                    response: b"200 ready\r\n",
                    delay: Duration::ZERO,
                },
                ScriptStep {
                    expect_prefix: Some("CAPABILITIES"),
                    response: b"500 unknown\r\n",
                    delay: Duration::ZERO,
                },
                ScriptStep {
                    expect_prefix: Some("BODY "),
                    response: yenc_body_response(original, b".\r\n"),
                    delay: Duration::ZERO,
                },
                ScriptStep {
                    expect_prefix: Some("BODY "),
                    response: b"222 body follows\r\nraw body\r\n.\r\n",
                    delay: Duration::ZERO,
                },
            ],
            Duration::ZERO,
        )
        .await;

        let mut conn = NntpConnection::connect(&scripted_plain_config(port))
            .await
            .unwrap();
        let mut chunks = Vec::new();
        let article = conn
            .stream_yenc_article("<test@example.com>", |chunk| {
                chunks.push(chunk.to_vec());
                Ok(())
            })
            .await
            .unwrap();

        assert_eq!(chunks, vec![original.to_vec()]);
        assert_eq!(article.stats.decoded_bytes_written, original.len() as u64);
        assert_eq!(article.stats.yenc_size_actual, original.len() as u64);
        assert_eq!(article.stats.yenc_control_hits, 1);
        assert_eq!(article.stats.nntp_terminator_hits, 1);
        assert_eq!(article.stats.nntp_terminator_bytes, b".\r\n".len() as u64);
        assert_eq!(article.stats.leftover_bytes_after_terminator, 0);

        let response = conn.body_by_id_raw("<next@example.com>").await.unwrap();
        assert_eq!(&response.data[..], b"raw body\r\n");
    }

    #[tokio::test]
    async fn stream_yenc_article_reauthenticates_on_mid_session_480() {
        let original = b"reauth fused";
        let port = spawn_scripted_server(
            vec![
                ScriptStep {
                    expect_prefix: None,
                    response: b"200 ready\r\n",
                    delay: Duration::ZERO,
                },
                ScriptStep {
                    expect_prefix: Some("CAPABILITIES"),
                    response: b"500 unknown\r\n",
                    delay: Duration::ZERO,
                },
                ScriptStep {
                    expect_prefix: Some("AUTHINFO USER"),
                    response: b"381 password required\r\n",
                    delay: Duration::ZERO,
                },
                ScriptStep {
                    expect_prefix: Some("AUTHINFO PASS"),
                    response: b"281 authentication accepted\r\n",
                    delay: Duration::ZERO,
                },
                ScriptStep {
                    expect_prefix: Some("CAPABILITIES"),
                    response: b"500 unknown\r\n",
                    delay: Duration::ZERO,
                },
                ScriptStep {
                    expect_prefix: Some("BODY "),
                    response: b"480 authentication required\r\n",
                    delay: Duration::ZERO,
                },
                ScriptStep {
                    expect_prefix: Some("AUTHINFO USER"),
                    response: b"381 password required\r\n",
                    delay: Duration::ZERO,
                },
                ScriptStep {
                    expect_prefix: Some("AUTHINFO PASS"),
                    response: b"281 authentication accepted\r\n",
                    delay: Duration::ZERO,
                },
                ScriptStep {
                    expect_prefix: Some("BODY "),
                    response: yenc_body_response(original, b".\r\n"),
                    delay: Duration::ZERO,
                },
            ],
            Duration::ZERO,
        )
        .await;

        let mut config = scripted_plain_config(port);
        config.username = Some("user".into());
        config.password = Some("pass".into());

        let mut conn = NntpConnection::connect(&config).await.unwrap();
        let mut chunks = Vec::new();
        conn.stream_yenc_article("<test@example.com>", |chunk| {
            chunks.push(chunk.to_vec());
            Ok(())
        })
        .await
        .unwrap();

        assert_eq!(chunks, vec![original.to_vec()]);
    }

    #[tokio::test]
    async fn stream_yenc_article_batches_large_decoded_output() {
        let mut original = Vec::with_capacity(FUSED_YENC_OUTPUT_BATCH_TARGET + 123);
        for idx in 0..(FUSED_YENC_OUTPUT_BATCH_TARGET + 123) {
            original.push((idx % 251) as u8);
        }

        let port = spawn_scripted_server(
            vec![
                ScriptStep {
                    expect_prefix: None,
                    response: b"200 ready\r\n",
                    delay: Duration::ZERO,
                },
                ScriptStep {
                    expect_prefix: Some("CAPABILITIES"),
                    response: b"500 unknown\r\n",
                    delay: Duration::ZERO,
                },
                ScriptStep {
                    expect_prefix: Some("BODY "),
                    response: yenc_body_response(&original, b".\r\n"),
                    delay: Duration::ZERO,
                },
            ],
            Duration::ZERO,
        )
        .await;

        let mut conn = NntpConnection::connect(&scripted_plain_config(port))
            .await
            .unwrap();
        let mut chunk_lens = Vec::new();
        let article = conn
            .stream_yenc_article("<test@example.com>", |chunk| {
                chunk_lens.push(chunk.len());
                Ok(())
            })
            .await
            .unwrap();

        assert_eq!(article.to_data(), original);
        assert_eq!(chunk_lens, vec![FUSED_YENC_OUTPUT_BATCH_TARGET, 123]);
        assert_eq!(article.stats.output_batches, 2);
    }

    #[tokio::test]
    async fn stream_yenc_article_reports_malformed_terminator_and_poisons_connection() {
        let port = spawn_scripted_server(
            vec![
                ScriptStep {
                    expect_prefix: None,
                    response: b"200 ready\r\n",
                    delay: Duration::ZERO,
                },
                ScriptStep {
                    expect_prefix: Some("CAPABILITIES"),
                    response: b"500 unknown\r\n",
                    delay: Duration::ZERO,
                },
                ScriptStep {
                    expect_prefix: Some("BODY "),
                    response: yenc_body_response(b"bad terminator", b"..\r\n"),
                    delay: Duration::ZERO,
                },
            ],
            Duration::ZERO,
        )
        .await;

        let mut conn = NntpConnection::connect(&scripted_plain_config(port))
            .await
            .unwrap();
        let err = conn
            .stream_yenc_article("<test@example.com>", |_| Ok(()))
            .await
            .unwrap_err();

        assert!(matches!(
            err,
            FusedYencError::Nntp(NntpError::MalformedMultilineTerminator)
        ));
        assert!(conn.is_poisoned());
    }

    #[tokio::test]
    async fn stream_next_yenc_article_decodes_queued_body_response() {
        let original = b"queued fused response";
        let port = spawn_scripted_server(
            vec![
                ScriptStep {
                    expect_prefix: None,
                    response: b"200 ready\r\n",
                    delay: Duration::ZERO,
                },
                ScriptStep {
                    expect_prefix: Some("CAPABILITIES"),
                    response: b"500 unknown\r\n",
                    delay: Duration::ZERO,
                },
                ScriptStep {
                    expect_prefix: Some("BODY "),
                    response: yenc_body_response(original, b".\r\n"),
                    delay: Duration::ZERO,
                },
            ],
            Duration::ZERO,
        )
        .await;

        let mut conn = NntpConnection::connect(&scripted_plain_config(port))
            .await
            .unwrap();
        conn.write_body_request("<test@example.com>").await.unwrap();
        conn.flush_commands().await.unwrap();

        let mut chunks = Vec::new();
        let article = conn
            .stream_next_yenc_article(|chunk| {
                chunks.push(chunk.to_vec());
                Ok(())
            })
            .await
            .unwrap();

        assert_eq!(chunks, vec![original.to_vec()]);
        assert_eq!(article.to_data(), original);
        assert_eq!(article.result.bytes_written, original.len());
    }

    #[tokio::test]
    #[ignore] // Requires a real NNTP server
    async fn connect_to_real_server() {
        let config = ServerConfig {
            host: "news.example.com".into(),
            port: 563,
            tls: true,
            username: Some("user".into()),
            password: Some("pass".into()),
            ..Default::default()
        };
        let conn = NntpConnection::connect(&config).await;
        assert!(conn.is_ok());
    }

    #[tokio::test]
    #[ignore] // Requires a real NNTP server
    async fn fetch_body_from_real_server() {
        let config = ServerConfig {
            host: "news.example.com".into(),
            port: 563,
            tls: true,
            username: Some("user".into()),
            password: Some("pass".into()),
            ..Default::default()
        };
        let mut conn = NntpConnection::connect(&config).await.unwrap();
        let result = conn.body_by_id("<test@example.com>").await;
        println!("{result:?}");
    }
}
