use std::collections::VecDeque;
#[cfg(not(windows))]
use std::ffi::{CStr, CString};
use std::io::{self, Read, Write};
use std::net::{IpAddr, SocketAddr, TcpStream, ToSocketAddrs};
#[cfg(not(windows))]
use std::os::fd::AsRawFd;
#[cfg(not(windows))]
use std::ptr::NonNull;
use std::time::{Duration, Instant};

#[cfg(not(windows))]
use bytes::BufMut;
use bytes::BytesMut;
#[cfg(not(windows))]
use s2n_tls_sys as s2n;
use tokio_util::codec::Decoder;
use tracing::{debug, trace, warn};

use crate::client::{
    BodyLaneMode, BodyLaneTraceMeta, DecodedBody, DecodedBodyCpu, DecodedBodyError, DecodedBodyIo,
    DecodedBodyTrace, FetchAttemptOutcome, FetchAttemptTrace,
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
    NntpTlsBackend, RustlsSession, TLS_READ_BUFFER, TransportReadStats, build_tls_config,
    make_server_name, selected_blocking_tls_backend,
};
use crate::types::{ArticleId, Capabilities, Response};

const MIN_TIMEOUT: Duration = Duration::from_secs(1);

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

pub struct BlockingBodyLane {
    conn: BlockingNntpConnection,
    server_id: ServerId,
    remote_ip: IpAddr,
    mode: BodyLaneMode,
    rtt_ewma: Option<Duration>,
    rtt_samples: VecDeque<Duration>,
    _permit: BlockingConnectionPermit,
}

pub struct BlockingNntpConnection {
    transport: BlockingTransport,
    codec: NntpCodec,
    read_buf: BytesMut,
    read_scratch: Vec<u8>,
    buffer_profile: NntpBufferProfile,
    capabilities: Capabilities,
    remote_addr: SocketAddr,
    command_timeout: Duration,
    current_group: Option<String>,
    credentials: Option<(String, String)>,
    poisoned: bool,
}

impl BlockingBodyLane {
    pub fn connect(
        server_id: ServerId,
        config: &ServerConfig,
        excluded_ips: &[IpAddr],
        address_offset: usize,
        groups: &[String],
        permit: BlockingConnectionPermit,
    ) -> Result<Self> {
        let mut conn =
            BlockingNntpConnection::connect_with_ip_policy(config, excluded_ips, address_offset)?;
        for group in groups {
            match conn.select_group(group) {
                Ok(()) => {
                    let remote_ip = conn.remote_ip();
                    return Ok(Self {
                        conn,
                        server_id,
                        remote_ip,
                        mode: BodyLaneMode::Sequential,
                        rtt_ewma: None,
                        rtt_samples: VecDeque::with_capacity(16),
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
                server_id,
                remote_ip,
                mode: BodyLaneMode::Sequential,
                rtt_ewma: None,
                rtt_samples: VecDeque::with_capacity(16),
                _permit: permit,
            })
        } else {
            Err(NntpError::NoSuchGroup)
        }
    }

    pub fn server_id(&self) -> ServerId {
        self.server_id
    }

    pub fn remote_ip(&self) -> IpAddr {
        self.remote_ip
    }

    pub fn mode(&self) -> BodyLaneMode {
        self.mode
    }

    pub fn rtt_ewma(&self) -> Option<Duration> {
        self.rtt_ewma
    }

    pub fn supports_pipelining(&self) -> bool {
        self.conn.capabilities().supports_pipelining()
    }

    pub fn stats(&self) -> BlockingLaneStats {
        self.conn.stats()
    }

    pub fn fetch_decoded_sequential(&mut self, message_id: &str) -> DecodedBodyTrace {
        self.mode = BodyLaneMode::Sequential;
        let started = Instant::now();
        let result = self.read_decoded_body(message_id);
        let elapsed = started.elapsed();
        self.observe_rtt(elapsed);
        self.trace_item(message_id, elapsed, result)
    }

    pub fn fetch_decoded_pipeline(
        &mut self,
        message_ids: &[String],
        max_depth: usize,
    ) -> Vec<(usize, DecodedBodyTrace, BodyLaneTraceMeta)> {
        self.mode = match max_depth {
            0 | 1 => BodyLaneMode::Sequential,
            2 => BodyLaneMode::PipelineDepth2,
            _ => BodyLaneMode::PipelineDepth4,
        };

        let requested = message_ids.len().min(max_depth);
        if requested == 0 {
            return Vec::new();
        }

        let mut out = Vec::with_capacity(requested);
        let mut batch_clean = true;
        let request_error = (|| {
            for message_id in &message_ids[..requested] {
                self.conn.write_body_request(message_id)?;
            }
            self.conn.flush_commands()
        })();

        if let Err(error) = request_error {
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
            self.observe_rtt(elapsed);
            if matches!(result, Err(DecodedBodyError::Nntp(ref e)) if is_connection_error(e)) {
                closed_early = true;
            }
            let trace = self.trace_item(message_id, elapsed, result);
            batch_clean &= trace.result.is_ok();
            let is_complete = closed_early || idx + 1 == requested;
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

        out
    }

    pub fn park(mut self) {
        let _ = self.conn.quit();
    }

    fn read_decoded_body(
        &mut self,
        message_id: &str,
    ) -> std::result::Result<DecodedBody, DecodedBodyError> {
        match self.conn.stream_yenc_article(message_id) {
            Ok(article) => Ok(decoded_body_from_article(article)),
            Err(FusedYencError::Yenc(error)) => {
                Err(DecodedBodyError::Decode { raw_size: 0, error })
            }
            Err(FusedYencError::Nntp(error)) => Err(DecodedBodyError::Nntp(error)),
        }
    }

    fn read_next_decoded_body(&mut self) -> std::result::Result<DecodedBody, DecodedBodyError> {
        match self.conn.stream_next_yenc_article() {
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
}

impl BlockingNntpConnection {
    pub fn connect_with_ip_policy(
        config: &ServerConfig,
        excluded_ips: &[IpAddr],
        address_offset: usize,
    ) -> Result<Self> {
        Self::connect_with_ip_policy_with_backend(config, excluded_ips, address_offset, None)
    }

    /// `backend_override` bypasses env/platform backend selection; tests use
    /// it to exercise a specific TLS transport deterministically.
    fn connect_with_ip_policy_with_backend(
        config: &ServerConfig,
        excluded_ips: &[IpAddr],
        address_offset: usize,
        backend_override: Option<NntpTlsBackend>,
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
                    return Self::from_tcp(config, tcp, remote_addr, backend_override);
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
    ) -> Result<Self> {
        let transport = if config.tls {
            let backend = match backend_override {
                Some(backend) => backend,
                None => selected_blocking_tls_backend()?,
            };
            match backend {
                NntpTlsBackend::ManualRustls => {
                    BlockingTransport::Rustls(Box::new(BlockingManualTlsStream::connect(
                        tcp,
                        &config.host,
                        config.tls_ca_cert.as_deref(),
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
            capabilities: Capabilities::default(),
            remote_addr,
            command_timeout: config.command_timeout.max(MIN_TIMEOUT),
            current_group: None,
            credentials: None,
            poisoned: false,
        };

        let greeting = conn.read_response()?;
        debug!(code = greeting.code.raw(), msg = %greeting.message, "received blocking NNTP greeting");
        match greeting.code.raw() {
            200 | 201 => {}
            400 => return Err(NntpError::ServiceUnavailable),
            502 => return Err(NntpError::from_status(greeting.code, &greeting.message)),
            _ => return Err(NntpError::unexpected(greeting.code, &greeting.message)),
        }

        conn.fetch_capabilities()?;
        if conn.capabilities.mode_reader_required() {
            let resp = conn.send_command(&Command::ModeReader)?;
            if resp.code.is_error() && resp.code.raw() != 500 {
                warn!(code = resp.code.raw(), "blocking MODE READER failed");
            }
        }
        if let (Some(user), Some(pass)) = (&config.username, &config.password) {
            let user = user.clone();
            let pass = pass.clone();
            conn.authenticate(&user, &pass)?;
            conn.credentials = Some((user, pass));
            conn.fetch_capabilities()?;
        }

        Ok(conn)
    }

    pub fn remote_ip(&self) -> IpAddr {
        self.remote_addr.ip()
    }

    pub fn capabilities(&self) -> &Capabilities {
        &self.capabilities
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

    fn write_command_frame(&mut self, cmd: &Command) -> Result<()> {
        let encoded = cmd.encode();
        self.transport.write_all(&encoded, self.command_timeout)?;
        Ok(())
    }

    pub fn flush_commands(&mut self) -> Result<()> {
        self.transport.flush(self.command_timeout)
    }

    pub fn send_command(&mut self, cmd: &Command) -> Result<Response> {
        self.write_command_frame(cmd)?;
        self.flush_commands()?;
        self.read_response()
    }

    pub fn write_body_request(&mut self, message_id: &str) -> Result<()> {
        let cmd = Command::Body(ArticleId::MessageId(message_id.to_string()));
        self.write_command_frame(&cmd)
    }

    fn read_response(&mut self) -> Result<Response> {
        match self.read_frame()? {
            NntpFrame::Line(line) => parse_response(&line),
            NntpFrame::MultiLineData(_) => Err(NntpError::MalformedResponse(
                "expected single-line response, got multi-line data".into(),
            )),
        }
    }

    fn read_frame(&mut self) -> Result<NntpFrame> {
        loop {
            if let Some(frame) = self.codec.decode(&mut self.read_buf)? {
                return Ok(frame);
            }
            self.read_into_buffer()?;
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
        let cmd = Command::Body(ArticleId::MessageId(message_id.to_string()));
        let initial = self.send_command(&cmd)?;
        let initial = if initial.code.raw() == 480 {
            if let Some((user, pass)) = self.credentials.clone() {
                self.authenticate(&user, &pass)?;
                self.current_group = None;
                self.send_command(&cmd)?
            } else {
                return Err(NntpError::AuthenticationRequired.into());
            }
        } else {
            initial
        };
        self.stream_yenc_article_response(initial)
    }

    pub fn stream_next_yenc_article(
        &mut self,
    ) -> std::result::Result<FusedYencArticle, FusedYencError> {
        let initial = self.read_response()?;
        if initial.code.raw() == 480 {
            self.poisoned = true;
            self.current_group = None;
            return Err(NntpError::AuthenticationRequired.into());
        }
        self.stream_yenc_article_response(initial)
    }

    fn stream_yenc_article_response(
        &mut self,
        initial: Response,
    ) -> std::result::Result<FusedYencArticle, FusedYencError> {
        let mut decoder = FusedYencArticleDecoder::from_body_response(initial)?;
        decoder.set_profile_cpu(profile_cpu_timings_enabled());
        let mut read_calls = 0u64;
        let mut read_bytes = 0u64;
        let mut transport_read = TransportReadStats::default();
        let mut article_chunks = Vec::new();

        loop {
            match decoder.decode_available(&mut self.read_buf)? {
                Some(mut article) => {
                    let chunks = std::mem::take(&mut article.chunks);
                    article_chunks.extend(chunks);
                    article.stats.read_calls = read_calls;
                    article.stats.read_bytes = read_bytes;
                    article.stats.transport_read = transport_read;
                    article.stats.leftover_bytes_after_terminator = self.read_buf.len() as u64;
                    article.stats.output_batches = article_chunks.len() as u64;
                    article.chunks = article_chunks;
                    if let Some(lane_stats) = self.transport.lane_stats_mut() {
                        lane_stats.body_responses += 1;
                        lane_stats.decoded_articles += 1;
                    }
                    return Ok(article);
                }
                None => {
                    article_chunks.extend(decoder.drain_output_chunks());
                }
            }

            let (n, stats) = self.read_into_buffer_with_stats()?;
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
                self.command_timeout,
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
        timeout: Duration,
    ) -> Result<Self> {
        tcp.set_read_timeout(Some(timeout)).map_err(NntpError::Io)?;
        tcp.set_write_timeout(Some(timeout))
            .map_err(NntpError::Io)?;
        tcp.set_nonblocking(false).map_err(NntpError::Io)?;

        let config = build_tls_config(ca_cert_path)?;
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
        tcp.set_read_timeout(Some(timeout)).map_err(NntpError::Io)?;
        tcp.set_write_timeout(Some(timeout))
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
        check_s2n_status("configure multi-record receive", unsafe {
            s2n::s2n_config_set_recv_multi_record(config.as_ptr(), multi_record_receive_enabled())
        })?;
        Ok(config)
    }

    fn as_ptr(&self) -> *mut s2n::s2n_config {
        self.ptr.as_ptr()
    }
}

/// Multi-record receive decrypts every full record already buffered before
/// returning, trading larger reads for extra buffered-copy work. Off by
/// default (single-record reads measured better under the previous allocator
/// economics); env-gated so the trade can be re-measured without a rebuild.
#[cfg(not(windows))]
fn multi_record_receive_enabled() -> bool {
    static ENABLED: std::sync::OnceLock<bool> = std::sync::OnceLock::new();
    *ENABLED.get_or_init(|| {
        std::env::var("WEAVER_NNTP_S2N_MULTI_RECORD")
            .map(|value| matches!(value.trim(), "1" | "true" | "on"))
            .unwrap_or(false)
    })
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
        result: article.result,
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
        NntpError::AccessDenied => NntpError::AccessDenied,
        NntpError::TlsRequired => NntpError::TlsRequired,
        NntpError::PoolExhausted => NntpError::PoolExhausted,
        NntpError::PoolShutdown => NntpError::PoolShutdown,
        NntpError::SoftTimeout(seconds) => NntpError::SoftTimeout(*seconds),
        NntpError::UnexpectedResponse { code, message } => NntpError::UnexpectedResponse {
            code: *code,
            message: message.clone(),
        },
        NntpError::MalformedResponse(message) => NntpError::MalformedResponse(message.clone()),
        NntpError::Io(error) => NntpError::Io(io::Error::new(error.kind(), error.to_string())),
        NntpError::Tls(error) => NntpError::MalformedResponse(format!("TLS error: {error}")),
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
mod tests {
    use super::*;
    use std::collections::HashMap;
    use std::sync::Arc;
    #[cfg(not(windows))]
    use std::sync::{Mutex, MutexGuard, OnceLock};
    use std::time::{SystemTime, UNIX_EPOCH};

    use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
    use tokio_rustls::TlsAcceptor;
    use tokio_rustls::rustls::ServerConfig as RustlsServerConfig;
    use tokio_rustls::rustls::pki_types::{PrivateKeyDer, PrivatePkcs8KeyDer};

    enum TestArticle {
        Body(Vec<u8>),
        Truncated(Vec<u8>),
    }

    #[cfg(not(windows))]
    fn s2n_test_guard() -> MutexGuard<'static, ()> {
        static LOCK: OnceLock<Mutex<()>> = OnceLock::new();
        LOCK.get_or_init(|| Mutex::new(())).lock().unwrap()
    }

    fn yenc_body(data: &[u8]) -> Vec<u8> {
        let mut body = Vec::new();
        weaver_yenc::encode(data, &mut body, 128, "owned.bin").unwrap();
        body
    }

    // Nanosecond timestamps alone collide when TLS tests start in parallel;
    // the serial keeps each fixture's CA file unique within the process.
    fn unique_ca_path(label: &str) -> std::path::PathBuf {
        static CA_SERIAL: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let serial = CA_SERIAL.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!(
            "weaver-{label}-{}-{serial}-{nonce}.pem",
            std::process::id()
        ))
    }

    fn spawn_tls_nntp_server(
        articles: Vec<(&'static str, TestArticle)>,
    ) -> (
        ServerConfig,
        std::thread::JoinHandle<()>,
        std::path::PathBuf,
    ) {
        let certified_key = rcgen::generate_simple_self_signed(vec!["localhost".to_string()])
            .expect("generate test cert");
        let cert_der = certified_key.cert.der().clone();
        let cert_pem = certified_key.cert.pem();
        let key_der = PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(
            certified_key.key_pair.serialize_der(),
        ));

        let server_provider = tokio_rustls::rustls::crypto::aws_lc_rs::default_provider();
        let server_config = RustlsServerConfig::builder_with_provider(Arc::new(server_provider))
            .with_safe_default_protocol_versions()
            .unwrap()
            .with_no_client_auth()
            .with_single_cert(vec![cert_der], key_der)
            .expect("server TLS config");

        let ca_path = unique_ca_path("blocking-tls-ca");
        std::fs::write(&ca_path, cert_pem).expect("write blocking TLS CA PEM");

        let listener = std::net::TcpListener::bind(("127.0.0.1", 0)).unwrap();
        let port = listener.local_addr().unwrap().port();
        listener.set_nonblocking(true).unwrap();

        let articles = articles
            .into_iter()
            .map(|(id, article)| (id.to_string(), article))
            .collect::<HashMap<_, _>>();
        let handle = std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            rt.block_on(async move {
                let listener = tokio::net::TcpListener::from_std(listener).unwrap();
                let (socket, _) = listener.accept().await.unwrap();
                let acceptor = TlsAcceptor::from(Arc::new(server_config));
                let tls = acceptor.accept(socket).await.unwrap();
                let mut stream = BufReader::new(tls);
                stream.get_mut().write_all(b"200 test server ready\r\n").await.unwrap();
                stream.get_mut().flush().await.unwrap();

                let mut line = String::new();
                loop {
                    line.clear();
                    if stream.read_line(&mut line).await.unwrap() == 0 {
                        break;
                    }
                    let command = line.trim_end_matches(['\r', '\n']);
                    if command.eq_ignore_ascii_case("CAPABILITIES") {
                        stream
                            .get_mut()
                            .write_all(b"101 Capability list:\r\nVERSION 2\r\nREADER\r\nPIPELINING\r\n.\r\n")
                            .await
                            .unwrap();
                    } else if command.to_ascii_uppercase().starts_with("GROUP ") {
                        stream
                            .get_mut()
                            .write_all(b"211 1 1 1 alt.test\r\n")
                            .await
                            .unwrap();
                    } else if let Some(id) = command.strip_prefix("BODY ") {
                        match articles.get(id.trim()) {
                            Some(TestArticle::Body(data)) => {
                                stream
                                    .get_mut()
                                    .write_all(format!("222 0 {} body follows\r\n", id.trim()).as_bytes())
                                    .await
                                    .unwrap();
                                stream.get_mut().write_all(&yenc_body(data)).await.unwrap();
                                stream.get_mut().write_all(b".\r\n").await.unwrap();
                            }
                            Some(TestArticle::Truncated(data)) => {
                                stream
                                    .get_mut()
                                    .write_all(format!("222 0 {} body follows\r\n", id.trim()).as_bytes())
                                    .await
                                    .unwrap();
                                stream.get_mut().write_all(&yenc_body(data)).await.unwrap();
                                stream.get_mut().flush().await.unwrap();
                                break;
                            }
                            None => {
                                stream
                                    .get_mut()
                                    .write_all(b"430 no such article\r\n")
                                    .await
                                    .unwrap();
                            }
                        }
                    } else if command.eq_ignore_ascii_case("QUIT") {
                        stream.get_mut().write_all(b"205 closing\r\n").await.unwrap();
                        stream.get_mut().flush().await.unwrap();
                        break;
                    } else {
                        stream
                            .get_mut()
                            .write_all(b"500 command not recognized\r\n")
                            .await
                            .unwrap();
                    }
                    stream.get_mut().flush().await.unwrap();
                }
            });
        });

        let config = ServerConfig {
            host: "localhost".to_string(),
            port,
            tls: true,
            starttls: false,
            username: None,
            password: None,
            connect_timeout: Duration::from_secs(5),
            command_timeout: Duration::from_secs(5),
            buffer_profile: NntpBufferProfile::default(),
            tls_ca_cert: Some(ca_path.clone()),
        };
        (config, handle, ca_path)
    }

    fn connect_with_backend(
        config: &ServerConfig,
        backend: NntpTlsBackend,
    ) -> BlockingNntpConnection {
        BlockingNntpConnection::connect_with_ip_policy_with_backend(config, &[], 0, Some(backend))
            .unwrap()
    }

    fn tls_lane_reads_single_body_response(backend: NntpTlsBackend) {
        let (config, handle, ca_path) = spawn_tls_nntp_server(vec![(
            "<solo@test>",
            TestArticle::Body(b"single article body".to_vec()),
        )]);
        let mut conn = connect_with_backend(&config, backend);
        conn.select_group("alt.test").unwrap();

        let article = conn.stream_yenc_article("<solo@test>").unwrap();

        assert_eq!(article.into_data(), b"single article body");
        conn.quit().unwrap();
        handle.join().unwrap();
        let _ = std::fs::remove_file(ca_path);
    }

    fn tls_lane_reads_pipelined_body_responses(backend: NntpTlsBackend) {
        let (config, handle, ca_path) = spawn_tls_nntp_server(vec![
            ("<one@test>", TestArticle::Body(b"first article".to_vec())),
            ("<two@test>", TestArticle::Body(b"second article".to_vec())),
        ]);
        let mut conn = connect_with_backend(&config, backend);
        conn.select_group("alt.test").unwrap();
        conn.write_body_request("<one@test>").unwrap();
        conn.write_body_request("<two@test>").unwrap();
        conn.flush_commands().unwrap();

        let first = conn.stream_next_yenc_article().unwrap();
        let second = conn.stream_next_yenc_article().unwrap();

        assert_eq!(first.into_data(), b"first article");
        assert_eq!(second.into_data(), b"second article");
        assert!(conn.stats().tls_recv_calls > 0);
        assert!(conn.stats().tls_send_calls > 0);
        conn.quit().unwrap();
        handle.join().unwrap();
        let _ = std::fs::remove_file(ca_path);
    }

    fn tls_lane_reads_large_pipelined_body_responses(backend: NntpTlsBackend) {
        let (config, handle, ca_path) = spawn_tls_nntp_server(vec![
            ("<one@test>", TestArticle::Body(vec![b'a'; 128 * 1024])),
            ("<two@test>", TestArticle::Body(vec![b'b'; 128 * 1024])),
        ]);
        let mut conn = connect_with_backend(&config, backend);
        conn.select_group("alt.test").unwrap();
        conn.write_body_request("<one@test>").unwrap();
        conn.write_body_request("<two@test>").unwrap();
        conn.flush_commands().unwrap();

        let first = conn.stream_next_yenc_article().unwrap();
        let second = conn.stream_next_yenc_article().unwrap();

        assert_eq!(first.to_data(), vec![b'a'; 128 * 1024]);
        assert_eq!(second.to_data(), vec![b'b'; 128 * 1024]);
        conn.quit().unwrap();
        handle.join().unwrap();
        let _ = std::fs::remove_file(ca_path);
    }

    fn tls_lane_reports_missing_article(backend: NntpTlsBackend) {
        let (config, handle, ca_path) = spawn_tls_nntp_server(Vec::new());
        let mut conn = connect_with_backend(&config, backend);
        conn.select_group("alt.test").unwrap();

        let error = conn.stream_yenc_article("<missing@test>").unwrap_err();

        assert!(matches!(
            error,
            FusedYencError::Nntp(NntpError::NoSuchArticle { .. })
                | FusedYencError::Nntp(NntpError::ArticleNotFound)
        ));
        conn.quit().unwrap();
        handle.join().unwrap();
        let _ = std::fs::remove_file(ca_path);
    }

    fn tls_lane_reports_truncated_response(backend: NntpTlsBackend) {
        let (config, handle, ca_path) = spawn_tls_nntp_server(vec![(
            "<truncated@test>",
            TestArticle::Truncated(b"incomplete article".to_vec()),
        )]);
        let mut conn = connect_with_backend(&config, backend);
        conn.select_group("alt.test").unwrap();

        let error = conn.stream_yenc_article("<truncated@test>").unwrap_err();

        assert!(matches!(
            error,
            FusedYencError::Nntp(NntpError::ConnectionClosed)
                | FusedYencError::Nntp(NntpError::Io(_))
                | FusedYencError::Nntp(NntpError::TruncatedMultilineBody)
                | FusedYencError::Yenc(_)
        ));
        handle.join().unwrap();
        let _ = std::fs::remove_file(ca_path);
    }

    #[test]
    fn blocking_rustls_reads_single_body_response() {
        tls_lane_reads_single_body_response(NntpTlsBackend::ManualRustls);
    }

    #[test]
    fn blocking_rustls_reads_pipelined_body_responses() {
        tls_lane_reads_pipelined_body_responses(NntpTlsBackend::ManualRustls);
    }

    #[test]
    fn blocking_rustls_reads_large_pipelined_body_responses() {
        tls_lane_reads_large_pipelined_body_responses(NntpTlsBackend::ManualRustls);
    }

    #[test]
    fn blocking_rustls_reports_missing_article() {
        tls_lane_reports_missing_article(NntpTlsBackend::ManualRustls);
    }

    #[test]
    fn blocking_rustls_reports_truncated_response() {
        tls_lane_reports_truncated_response(NntpTlsBackend::ManualRustls);
    }

    #[cfg(not(windows))]
    #[test]
    fn blocking_s2n_fd_reads_single_body_response() {
        let _guard = s2n_test_guard();
        tls_lane_reads_single_body_response(NntpTlsBackend::S2n);
    }

    #[cfg(not(windows))]
    #[test]
    fn blocking_s2n_fd_reads_pipelined_body_responses() {
        let _guard = s2n_test_guard();
        tls_lane_reads_pipelined_body_responses(NntpTlsBackend::S2n);
    }

    #[cfg(not(windows))]
    #[test]
    fn blocking_s2n_reads_large_pipelined_body_responses() {
        let _guard = s2n_test_guard();
        tls_lane_reads_large_pipelined_body_responses(NntpTlsBackend::S2n);
    }

    #[cfg(not(windows))]
    #[test]
    fn blocking_s2n_fd_reports_missing_article() {
        let _guard = s2n_test_guard();
        tls_lane_reports_missing_article(NntpTlsBackend::S2n);
    }

    #[cfg(not(windows))]
    #[test]
    fn blocking_s2n_fd_reports_truncated_response() {
        let _guard = s2n_test_guard();
        tls_lane_reports_truncated_response(NntpTlsBackend::S2n);
    }

    const TLS_DRAIN_RECORD_BYTES: usize = 16 * 1024;
    const TLS_DRAIN_RECORDS: usize = 8;

    fn spawn_raw_tls_drain_server() -> (u16, std::thread::JoinHandle<()>, std::path::PathBuf) {
        let certified_key = rcgen::generate_simple_self_signed(vec!["localhost".to_string()])
            .expect("generate test cert");
        let cert_der = certified_key.cert.der().clone();
        let cert_pem = certified_key.cert.pem();
        let key_der = PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(
            certified_key.key_pair.serialize_der(),
        ));

        let server_provider = tokio_rustls::rustls::crypto::aws_lc_rs::default_provider();
        let server_config = RustlsServerConfig::builder_with_provider(Arc::new(server_provider))
            .with_safe_default_protocol_versions()
            .unwrap()
            .with_no_client_auth()
            .with_single_cert(vec![cert_der], key_der)
            .expect("server TLS config");

        let ca_path = unique_ca_path("blocking-rustls-drain-ca");
        std::fs::write(&ca_path, cert_pem).expect("write drain CA PEM");

        let listener = std::net::TcpListener::bind(("127.0.0.1", 0)).unwrap();
        let port = listener.local_addr().unwrap().port();
        listener.set_nonblocking(true).unwrap();

        let handle = std::thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .unwrap();
            rt.block_on(async move {
                let listener = tokio::net::TcpListener::from_std(listener).unwrap();
                let (socket, _) = listener.accept().await.unwrap();
                let acceptor = TlsAcceptor::from(Arc::new(server_config));
                let mut tls = acceptor.accept(socket).await.unwrap();
                let record = vec![0x5Au8; TLS_DRAIN_RECORD_BYTES];
                for _ in 0..TLS_DRAIN_RECORDS {
                    tls.write_all(&record).await.unwrap();
                    tls.flush().await.unwrap();
                }
                tokio::time::sleep(Duration::from_millis(250)).await;
            });
        });

        (port, handle, ca_path)
    }

    #[test]
    #[ignore = "diagnostic read-shape probe; run explicitly with --ignored --nocapture"]
    fn rustls_blocking_drain_probe() {
        let (port, handle, ca_path) = spawn_raw_tls_drain_server();
        let tcp = TcpStream::connect(("127.0.0.1", port)).unwrap();
        let mut stream = BlockingManualTlsStream::connect(
            tcp,
            "localhost",
            Some(&ca_path),
            Duration::from_secs(5),
        )
        .expect("blocking rustls connect");

        let payload = TLS_DRAIN_RECORD_BYTES * TLS_DRAIN_RECORDS;
        let mut read_buf = BytesMut::with_capacity(payload);
        let mut total = 0usize;
        let mut aggregate = TransportReadStats::default();
        while total < payload {
            let (n, stats) = stream
                .read_into_buf(&mut read_buf, payload, Duration::from_secs(5))
                .expect("blocking rustls read");
            assert_ne!(n, 0, "TLS stream closed before probe payload");
            total += n;
            aggregate.add(stats);
        }

        handle.join().unwrap();
        let _ = std::fs::remove_file(&ca_path);
        let bytes_per_socket_read = (total as u64)
            .checked_div(aggregate.try_read_calls)
            .unwrap_or(0);
        println!(
            "rustls_blocking_drain_probe total_bytes={total} records={TLS_DRAIN_RECORDS} socket_reads={} tls_record_reads={} bytes_per_socket_read={bytes_per_socket_read}",
            aggregate.try_read_calls, aggregate.tls_read_calls
        );
        assert_eq!(total, payload);
        assert!(
            aggregate.try_read_calls < TLS_DRAIN_RECORDS as u64,
            "buffered manual rustls should consume multiple TLS records per socket read"
        );
    }

    #[test]
    fn blocking_decoder_reads_single_body_response() {
        let mut decoder = FusedYencArticleDecoder::from_body_response(
            parse_response("222 0 <one@test> body follows").unwrap(),
        )
        .unwrap();
        let mut input = BytesMut::new();
        input.extend_from_slice(&yenc_body(b"hello owned lane"));
        input.extend_from_slice(b".\r\n");
        let article = decoder
            .decode_available(&mut input)
            .unwrap()
            .expect("complete article");
        assert_eq!(article.into_data(), b"hello owned lane");
        assert!(input.is_empty());
    }

    #[test]
    fn blocking_decoder_preserves_pipelined_leftover() {
        let mut decoder = FusedYencArticleDecoder::from_body_response(
            parse_response("222 0 <one@test> body follows").unwrap(),
        )
        .unwrap();
        let mut input = BytesMut::new();
        input.extend_from_slice(&yenc_body(b"first"));
        input.extend_from_slice(b".\r\n222 0 <two@test> body follows\r\n");
        let article = decoder
            .decode_available(&mut input)
            .unwrap()
            .expect("complete article");
        assert_eq!(article.into_data(), b"first");
        assert!(std::str::from_utf8(&input).unwrap().starts_with("222 "));
    }
}
