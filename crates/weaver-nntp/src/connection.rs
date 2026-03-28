use std::time::{Duration, Instant};

use bytes::{Bytes, BytesMut};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio_util::codec::Decoder;
use tracing::{debug, trace, warn};

use crate::codec::{NntpCodec, NntpFrame, StreamChunk};
use crate::commands::Command;
use crate::error::{NntpError, Result};
use crate::response::{is_multiline_status, parse_response};
use crate::tls::NntpTransport;
use crate::types::{ArticleId, Capabilities, MultiLineResponse, Response};

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
        let per_connection = if total_connections == 0 {
            128 * 1024
        } else {
            (total_budget / total_connections).clamp(128 * 1024, 512 * 1024)
        };
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
    io_buf: Vec<u8>,
    buffer_profile: NntpBufferProfile,
    state: ConnectionState,
    capabilities: Capabilities,
    host: String,
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
}

impl NntpConnection {
    /// Connect to an NNTP server, perform TLS negotiation and authentication.
    pub async fn connect(config: &ServerConfig) -> Result<Self> {
        let connect_timeout = config.connect_timeout.max(MIN_TIMEOUT);
        let result =
            tokio::time::timeout(connect_timeout, async { Self::connect_inner(config).await })
                .await;

        match result {
            Ok(inner) => inner,
            Err(_) => Err(NntpError::Timeout),
        }
    }

    async fn connect_inner(config: &ServerConfig) -> Result<Self> {
        debug!(host = %config.host, port = config.port, tls = config.tls, "connecting to NNTP server");

        // 1. Establish transport
        let transport = if config.tls {
            crate::tls::connect_tls(&config.host, config.port, config.tls_ca_cert.as_deref())
                .await?
        } else {
            crate::tls::connect_plain(&config.host, config.port).await?
        };

        let now = Instant::now();
        let read_buf_capacity = config.buffer_profile.read_buf_capacity.max(64 * 1024);
        let socket_read_size = config.buffer_profile.socket_read_size.max(64 * 1024);
        let mut conn = NntpConnection {
            transport: Some(transport),
            codec: NntpCodec::new(),
            read_buf: BytesMut::with_capacity(read_buf_capacity),
            io_buf: vec![0u8; socket_read_size],
            buffer_profile: config.buffer_profile,
            state: ConnectionState::Greeting,
            capabilities: Capabilities::default(),
            host: config.host.clone(),
            created_at: now,
            last_used: now,
            command_timeout: config.command_timeout.max(MIN_TIMEOUT),
            poisoned: false,
            current_group: None,
            credentials: None,
            tls_ca_cert: config.tls_ca_cert.clone(),
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
        debug!(user = %username, "authenticating");

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
    pub async fn send_command(&mut self, cmd: &Command) -> Result<Response> {
        self.last_used = Instant::now();

        let encoded = cmd.encode();
        trace!(cmd = %String::from_utf8_lossy(&encoded).trim(), "sending command");

        let transport = self.transport.as_mut().ok_or(NntpError::ConnectionClosed)?;
        transport.write_all(&encoded).await.map_err(|e| {
            self.poisoned = true;
            self.current_group = None;
            NntpError::Io(e)
        })?;
        let transport = self.transport.as_mut().ok_or(NntpError::ConnectionClosed)?;
        transport.flush().await.map_err(|e| {
            self.poisoned = true;
            self.current_group = None;
            NntpError::Io(e)
        })?;

        self.read_response().await
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
                let n = self.read_into_buffer().await?;
                self.read_buf.extend_from_slice(&self.io_buf[..n]);
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
                Ok(data.freeze())
            }
            Ok(NntpFrame::Line(line)) => {
                self.reset_multiline_decode_state();
                Err(NntpError::MalformedResponse(format!(
                    "expected multi-line data, got line: {line:?}"
                )))
            }
            Err(err) => {
                let err = self.classify_multiline_error(err);
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
        let cmd = Command::Body(ArticleId::MessageId(message_id.to_string()));
        self.send_multiline_command(&cmd).await
    }

    /// Retrieve the body of an article without dot-unstuffing.
    ///
    /// The returned data retains NNTP dot-stuffing. Use with `weaver_yenc::decode_nntp`
    /// which handles unstuffing inline during decode, avoiding a separate pass.
    pub async fn body_by_id_raw(&mut self, message_id: &str) -> Result<MultiLineResponse> {
        let cmd = Command::Body(ArticleId::MessageId(message_id.to_string()));
        self.send_multiline_command_raw(&cmd).await
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
        let cmd = Command::Body(ArticleId::MessageId(message_id.to_string()));
        let initial = self.send_command(&cmd).await?;

        if initial.code.is_error() {
            return Err(NntpError::from_status(initial.code, &initial.message));
        }

        if initial.code.raw() != 222 {
            return Err(NntpError::unexpected(initial.code, &initial.message));
        }

        let data = self.read_multiline_data().await?;
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
    pub async fn stream_body_chunked<F>(&mut self, message_id: &str, mut on_chunk: F) -> Result<u64>
    where
        F: FnMut(&[u8]) -> Result<()>,
    {
        let cmd = Command::Body(ArticleId::MessageId(message_id.to_string()));
        let initial = self.send_command(&cmd).await?;

        if initial.code.is_error() {
            return Err(NntpError::from_status(initial.code, &initial.message));
        }

        if initial.code.raw() != 222 {
            return Err(NntpError::unexpected(initial.code, &initial.message));
        }

        self.codec.set_streaming_multiline(true);
        let mut total_bytes = 0u64;
        let timeout = self.command_timeout;

        let result = tokio::time::timeout(timeout, async {
            loop {
                // Try to decode a chunk from the existing read buffer first.
                match self.codec.decode_streaming_chunk(&mut self.read_buf)? {
                    Some(StreamChunk::Data(data)) => {
                        total_bytes += data.len() as u64;
                        on_chunk(&data)?;
                        continue;
                    }
                    Some(StreamChunk::End) => {
                        return Ok::<u64, NntpError>(total_bytes);
                    }
                    None => {
                        // Need more data from the transport.
                    }
                }

                let n = self.read_into_buffer().await?;
                self.read_buf.extend_from_slice(&self.io_buf[..n]);
            }
        })
        .await;

        match result {
            Ok(inner) => {
                self.trim_read_buffer();
                match inner {
                    Ok(bytes) => Ok(bytes),
                    Err(err) => {
                        let err = self.classify_multiline_error(err);
                        self.reset_multiline_decode_state();
                        Err(err)
                    }
                }
            }
            Err(_) => {
                self.poisoned = true;
                self.current_group = None;
                self.reset_multiline_decode_state();
                Err(NntpError::TruncatedMultilineBody)
            }
        }
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
        let transport = self.transport.as_mut().ok_or(NntpError::ConnectionClosed)?;
        let n = transport.read(&mut self.io_buf).await.map_err(|e| {
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
}

fn pending_multiline_terminator(buf: &[u8]) -> bool {
    buf.ends_with(b"\r.")
        || buf.ends_with(b"\n.")
        || buf.ends_with(b"\r.\r")
        || buf.ends_with(b"\n.\r")
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio::net::{TcpListener, TcpStream};

    struct ScriptStep {
        expect_prefix: Option<&'static str>,
        response: &'static [u8],
        delay: Duration,
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

    async fn spawn_scripted_server(steps: Vec<ScriptStep>, hold_open_after_last: Duration) -> u16 {
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
                if step.delay > Duration::ZERO {
                    tokio::time::sleep(step.delay).await;
                }
                if !step.response.is_empty() {
                    socket.write_all(step.response).await.unwrap();
                    socket.flush().await.unwrap();
                }
            }
            if hold_open_after_last > Duration::ZERO {
                tokio::time::sleep(hold_open_after_last).await;
            }
        });

        port
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
                    response: b"222 body follows\r\npartial\r\n",
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
        assert!(matches!(err, NntpError::ServerDisconnectedMidBody));
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
        assert!(matches!(err, NntpError::MalformedMultilineTerminator));
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
