use std::collections::VecDeque;
use std::ffi::{CStr, CString};
use std::io::{self, Read, Write};
use std::net::{IpAddr, SocketAddr, TcpStream, ToSocketAddrs};
use std::os::fd::AsRawFd;
use std::ptr::NonNull;
use std::time::{Duration, Instant};

use bytes::BytesMut;
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
use crate::tls::TransportReadStats;
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

struct BlockingS2nStream {
    conn: RawS2nConnection,
    _config: RawS2nConfig,
    tcp: TcpStream,
    stats: BlockingLaneStats,
    read_scratch: Vec<u8>,
}

enum BlockingTransport {
    Plain(TcpStream),
    S2n(BlockingS2nStream),
}

struct RawS2nConfig {
    ptr: NonNull<s2n::s2n_config>,
}

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
                    return Self::from_tcp(config, tcp, remote_addr);
                }
                Err(error) => last_error = Some(error),
            }
        }

        Err(NntpError::Io(last_error.unwrap_or_else(|| {
            io::Error::new(io::ErrorKind::NotFound, "no NNTP address resolved")
        })))
    }

    fn from_tcp(config: &ServerConfig, tcp: TcpStream, remote_addr: SocketAddr) -> Result<Self> {
        let transport = if config.tls {
            BlockingTransport::S2n(BlockingS2nStream::connect(
                tcp,
                &config.host,
                config.tls_ca_cert.as_deref(),
                config.command_timeout.max(MIN_TIMEOUT),
            )?)
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
                    if let BlockingTransport::S2n(inner) = &mut self.transport {
                        inner.stats.body_responses += 1;
                        inner.stats.decoded_articles += 1;
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
            BlockingTransport::S2n(inner) => inner.flush(timeout),
        }
    }
}

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
        tcp.set_nonblocking(true).map_err(NntpError::Io)?;

        let config = crate::tls::build_s2n_tls_config(ca_cert_path)?;
        let mut conn = S2nConnection::new_client();
        conn.set_config(config)
            .map_err(|error| s2n_error("set config", error))?;
        conn.set_server_name(host)
            .map_err(|error| s2n_error("set server name", error))?;
        conn.prefer_throughput()
            .map_err(|error| s2n_error("prefer throughput", error))?;
        // Receive buffering can block a large s2n_recv waiting for more wire
        // data, which deadlocks small NNTP control responses on blocking IO.
        conn.set_blinding(Blinding::SelfService)
            .map_err(|error| s2n_error("set blinding", error))?;

        let mut stream = Self {
            conn,
            io: Box::new(BlockingS2nIo {
                tcp,
                stats: BlockingLaneStats::default(),
                last_error: None,
                last_blocked: None,
            }),
            read_scratch: vec![0; 256 * 1024],
        };
        stream.install_callbacks()?;
        stream.negotiate(timeout)?;
        Ok(stream)
    }

    fn install_callbacks(&mut self) -> Result<()> {
        let ctx = (&mut *self.io) as *mut BlockingS2nIo as *mut c_void;
        self.conn
            .set_receive_callback(Some(blocking_s2n_recv_cb))
            .map_err(|error| s2n_error("set receive callback", error))?;
        self.conn
            .set_send_callback(Some(blocking_s2n_send_cb))
            .map_err(|error| s2n_error("set send callback", error))?;
        unsafe {
            self.conn
                .set_receive_context(ctx)
                .map_err(|error| s2n_error("set receive context", error))?;
            self.conn
                .set_send_context(ctx)
                .map_err(|error| s2n_error("set send context", error))?;
        }
        Ok(())
    }

    fn negotiate(&mut self, timeout: Duration) -> Result<()> {
        loop {
            self.io.last_blocked = None;
            match self.conn.poll_negotiate() {
                Poll::Ready(Ok(_)) => return Ok(()),
                Poll::Ready(Err(error)) => {
                    return Err(self
                        .take_io_error()
                        .unwrap_or_else(|| s2n_error("handshake", error)));
                }
                Poll::Pending => self.wait_for_blocked_io(timeout).map_err(NntpError::Io)?,
            }
        }
    }

    fn read_into_buf(
        &mut self,
        dst: &mut BytesMut,
        target_read_size: usize,
        timeout: Duration,
    ) -> io::Result<(usize, TransportReadStats)> {
        self.io.tcp.set_read_timeout(Some(timeout))?;
        let target = target_read_size.max(1);
        if self.read_scratch.len() < target {
            self.read_scratch.resize(target, 0);
        }

        let mut total = 0usize;
        let mut stats = TransportReadStats::default();
        loop {
            if total >= target {
                stats.s2n_target_full_returns += 1;
                break;
            }
            self.io.last_blocked = None;
            match self.conn.poll_recv(&mut self.read_scratch[total..target]) {
                Poll::Ready(Ok(0)) => {
                    stats.s2n_zero_returns += 1;
                    break;
                }
                Poll::Ready(Ok(n)) => {
                    total += n;
                    self.io.stats.tls_recv_calls += 1;
                    stats.s2n_read_calls += 1;
                    stats.s2n_read_bytes += n as u64;
                    stats.plaintext_bytes += n as u64;
                    if self.conn.peek_len() == 0 && self.conn.peek_buffered_len() == 0 {
                        break;
                    }
                }
                Poll::Ready(Err(error)) => {
                    if let Some(error) = self.take_io_error() {
                        return match error {
                            NntpError::Io(error) => Err(error),
                            other => Err(io::Error::other(other)),
                        };
                    }
                    return Err(io::Error::other(s2n_error("recv", error)));
                }
                Poll::Pending => {
                    if total == 0 {
                        stats.s2n_pending_empty_returns += 1;
                    } else {
                        stats.s2n_pending_after_bytes_returns += 1;
                    }
                    if total > 0 {
                        break;
                    }
                    self.wait_for_blocked_io(timeout)?;
                }
            }
        }

        dst.extend_from_slice(&self.read_scratch[..total]);
        Ok((total, stats))
    }

    fn write_all(&mut self, mut bytes: &[u8], timeout: Duration) -> Result<()> {
        self.io
            .tcp
            .set_write_timeout(Some(timeout))
            .map_err(NntpError::Io)?;
        while !bytes.is_empty() {
            self.io.last_blocked = None;
            match self.conn.poll_send(bytes) {
                Poll::Ready(Ok(0)) => {
                    return Err(NntpError::Io(io::Error::new(
                        io::ErrorKind::WriteZero,
                        "s2n write returned zero",
                    )));
                }
                Poll::Ready(Ok(n)) => {
                    self.io.stats.tls_send_calls += 1;
                    bytes = &bytes[n..];
                }
                Poll::Ready(Err(error)) => {
                    return Err(self
                        .take_io_error()
                        .unwrap_or_else(|| s2n_error("send", error)));
                }
                Poll::Pending => self.wait_for_blocked_io(timeout).map_err(NntpError::Io)?,
            }
        }
        self.flush(timeout)
    }

    fn flush(&mut self, timeout: Duration) -> Result<()> {
        self.io
            .tcp
            .set_write_timeout(Some(timeout))
            .map_err(NntpError::Io)?;
        loop {
            self.io.last_blocked = None;
            match self.conn.poll_flush() {
                Poll::Ready(Ok(_)) => return Ok(()),
                Poll::Ready(Err(error)) => {
                    return Err(self
                        .take_io_error()
                        .unwrap_or_else(|| s2n_error("flush", error)));
                }
                Poll::Pending => self.wait_for_blocked_io(timeout).map_err(NntpError::Io)?,
            }
        }
    }

    fn wait_for_blocked_io(&mut self, timeout: Duration) -> io::Result<()> {
        let event = match self.io.last_blocked.take() {
            Some(BlockedIo::Read) => libc::POLLIN,
            Some(BlockedIo::Write) => libc::POLLOUT,
            None => libc::POLLIN | libc::POLLOUT,
        };
        wait_fd(self.io.tcp.as_raw_fd(), event, timeout)
    }

    fn take_io_error(&mut self) -> Option<NntpError> {
        self.io.last_error.take().map(NntpError::Io)
    }
}

impl Drop for BlockingS2nStream {
    fn drop(&mut self) {
        let _ = self.conn.set_receive_callback(None);
        let _ = self.conn.set_send_callback(None);
        unsafe {
            let _ = self.conn.set_receive_context(std::ptr::null_mut());
            let _ = self.conn.set_send_context(std::ptr::null_mut());
        }
    }
}

unsafe extern "C" fn blocking_s2n_recv_cb(ctx: *mut c_void, buf: *mut u8, len: u32) -> c_int {
    if ctx.is_null() {
        return -1;
    }
    let io = unsafe { &mut *(ctx as *mut BlockingS2nIo) };
    let dest = unsafe { std::slice::from_raw_parts_mut(buf, len as usize) };
    match io.tcp.read(dest) {
        Ok(n) => {
            io.stats.socket_reads += 1;
            n as c_int
        }
        Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
            io.last_blocked = Some(BlockedIo::Read);
            set_errno_would_block();
            -1
        }
        Err(error) => {
            io.last_error = Some(error);
            -1
        }
    }
}

unsafe extern "C" fn blocking_s2n_send_cb(ctx: *mut c_void, buf: *const u8, len: u32) -> c_int {
    if ctx.is_null() {
        return -1;
    }
    let io = unsafe { &mut *(ctx as *mut BlockingS2nIo) };
    let src = unsafe { std::slice::from_raw_parts(buf, len as usize) };
    match io.tcp.write(src) {
        Ok(n) => {
            io.stats.socket_writes += 1;
            n as c_int
        }
        Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
            io.last_blocked = Some(BlockedIo::Write);
            set_errno_would_block();
            -1
        }
        Err(error) => {
            io.last_error = Some(error);
            -1
        }
    }
}

fn wait_fd(fd: RawFd, events: libc::c_short, timeout: Duration) -> io::Result<()> {
    let timeout_ms = timeout.as_millis().min(c_int::MAX as u128) as c_int;
    let timeout_ms = timeout_ms.max(1);
    let mut pollfd = libc::pollfd {
        fd,
        events,
        revents: 0,
    };
    loop {
        let rc = unsafe { libc::poll(&mut pollfd, 1, timeout_ms) };
        if rc > 0 {
            if pollfd.revents & events != 0 {
                return Ok(());
            }
            if pollfd.revents & (libc::POLLERR | libc::POLLHUP | libc::POLLNVAL) != 0 {
                return Err(io::Error::new(
                    io::ErrorKind::ConnectionAborted,
                    format!("s2n socket poll failed: revents=0x{:x}", pollfd.revents),
                ));
            }
            return Ok(());
        }
        if rc == 0 {
            return Err(io::Error::new(
                io::ErrorKind::TimedOut,
                "timed out waiting for s2n socket",
            ));
        }
        let error = io::Error::last_os_error();
        if error.kind() != io::ErrorKind::Interrupted {
            return Err(error);
        }
    }
}

fn set_errno_would_block() {
    #[cfg(any(target_os = "macos", target_os = "ios"))]
    unsafe {
        *libc::__error() = libc::EWOULDBLOCK;
    }
    #[cfg(target_os = "linux")]
    unsafe {
        *libc::__errno_location() = libc::EWOULDBLOCK;
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

fn s2n_error(context: &str, error: s2n_tls::error::Error) -> NntpError {
    NntpError::MalformedResponse(format!("blocking s2n failed to {context}: {error}"))
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

    fn yenc_body(data: &[u8]) -> Vec<u8> {
        let mut body = Vec::new();
        weaver_yenc::encode(data, &mut body, 128, "owned.bin").unwrap();
        body
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
