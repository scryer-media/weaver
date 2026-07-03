use std::io::{self, Cursor, Read, Write};
use std::net::{IpAddr, SocketAddr};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use boring::ssl::{ConnectConfiguration, SslConnector, SslMethod, SslVerifyMode, SslVersion};
use bytes::{BufMut, BytesMut};
use s2n_tls::{config::Config as S2nConfig, security};
use s2n_tls_tokio::{TlsConnector as S2nTlsConnector, TlsStream as S2nTlsStream};
use socket2::SockRef;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf};
use tokio::net::{TcpStream, lookup_host};
use tokio_boring::SslStream as BoringTlsStream;
use tokio_rustls::client::TlsStream as RustlsTlsStream;
use tokio_rustls::rustls::pki_types::ServerName;
use tokio_rustls::rustls::{ClientConfig, ClientConnection, RootCertStore};

use crate::error::NntpError;

pin_project_lite::pin_project! {
    /// A transport that is either a plain TCP connection or a TLS-wrapped one.
    ///
    /// `ManualTls` is the production TLS path. `Tls` keeps the tokio-rustls
    /// stream available for diagnostics that compare read batching behavior.
    #[project = NntpTransportProj]
    pub enum NntpTransport {
        /// Unencrypted TCP.
        Plain { #[pin] inner: TcpStream, remote_addr: SocketAddr },
        /// TLS-encrypted TCP through tokio-rustls.
        Tls { #[pin] inner: RustlsTlsStream<TcpStream>, remote_addr: SocketAddr },
        /// TLS-encrypted TCP driven directly through rustls.
        ManualTls { inner: ManualTlsStream, remote_addr: SocketAddr },
        /// TLS-encrypted TCP through s2n-tls.
        S2nTls { #[pin] inner: S2nTlsStream<TcpStream>, remote_addr: SocketAddr },
        /// TLS-encrypted TCP through Cloudflare's BoringSSL bindings.
        BoringTls { #[pin] inner: BoringTlsStream<TcpStream>, remote_addr: SocketAddr },
    }
}

const TLS_READ_BUFFER: usize = 256 * 1024;
const TLS_PLAINTEXT_DRAIN_CHUNK: usize = 16 * 1024;
const TLS_BACKEND_ENV: &str = "WEAVER_NNTP_TLS_BACKEND";
const BORING_READ_AHEAD_ENV: &str = "WEAVER_NNTP_BORING_READ_AHEAD";

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TransportReadStats {
    pub readiness_waits: u64,
    pub empty_readiness_wakes: u64,
    pub try_read_calls: u64,
    pub try_read_would_block: u64,
    pub try_read_bytes: u64,
    pub tls_read_calls: u64,
    pub tls_process_packets_calls: u64,
    pub plaintext_drain_calls: u64,
    pub plaintext_reader_calls: u64,
    pub plaintext_reader_would_block: u64,
    pub plaintext_bytes: u64,
    pub cached_plaintext_returns: u64,
    pub s2n_read_calls: u64,
    pub s2n_read_bytes: u64,
    pub s2n_target_full_returns: u64,
    pub s2n_pending_empty_returns: u64,
    pub s2n_pending_after_bytes_returns: u64,
    pub s2n_zero_returns: u64,
    pub boring_read_calls: u64,
    pub boring_read_bytes: u64,
    pub boring_target_full_returns: u64,
    pub boring_pending_empty_returns: u64,
    pub boring_pending_after_bytes_returns: u64,
    pub boring_zero_returns: u64,
}

impl TransportReadStats {
    pub fn add(&mut self, other: Self) {
        self.readiness_waits += other.readiness_waits;
        self.empty_readiness_wakes += other.empty_readiness_wakes;
        self.try_read_calls += other.try_read_calls;
        self.try_read_would_block += other.try_read_would_block;
        self.try_read_bytes += other.try_read_bytes;
        self.tls_read_calls += other.tls_read_calls;
        self.tls_process_packets_calls += other.tls_process_packets_calls;
        self.plaintext_drain_calls += other.plaintext_drain_calls;
        self.plaintext_reader_calls += other.plaintext_reader_calls;
        self.plaintext_reader_would_block += other.plaintext_reader_would_block;
        self.plaintext_bytes += other.plaintext_bytes;
        self.cached_plaintext_returns += other.cached_plaintext_returns;
        self.s2n_read_calls += other.s2n_read_calls;
        self.s2n_read_bytes += other.s2n_read_bytes;
        self.s2n_target_full_returns += other.s2n_target_full_returns;
        self.s2n_pending_empty_returns += other.s2n_pending_empty_returns;
        self.s2n_pending_after_bytes_returns += other.s2n_pending_after_bytes_returns;
        self.s2n_zero_returns += other.s2n_zero_returns;
        self.boring_read_calls += other.boring_read_calls;
        self.boring_read_bytes += other.boring_read_bytes;
        self.boring_target_full_returns += other.boring_target_full_returns;
        self.boring_pending_empty_returns += other.boring_pending_empty_returns;
        self.boring_pending_after_bytes_returns += other.boring_pending_after_bytes_returns;
        self.boring_zero_returns += other.boring_zero_returns;
    }
}

pub struct TransportRead {
    pub bytes: usize,
    pub stats: TransportReadStats,
}

async fn read_s2n_available_into(
    inner: &mut S2nTlsStream<TcpStream>,
    dst: &mut BytesMut,
    target_read_size: usize,
) -> io::Result<TransportRead> {
    let started_len = dst.len();
    let target_read_size = target_read_size.max(1);
    let mut stats = TransportReadStats::default();
    let bytes = std::future::poll_fn(|cx| {
        loop {
            let total = dst.len().saturating_sub(started_len);
            if total >= target_read_size {
                stats.s2n_target_full_returns += 1;
                return std::task::Poll::Ready(Ok(total));
            }

            dst.reserve(target_read_size - total);
            let mut read_buf = ReadBuf::uninit(dst.spare_capacity_mut());
            match std::pin::Pin::new(&mut *inner).poll_read(cx, &mut read_buf) {
                std::task::Poll::Ready(Ok(())) => {
                    let n = read_buf.filled().len();
                    if n == 0 {
                        stats.s2n_zero_returns += 1;
                        return std::task::Poll::Ready(Ok(total));
                    }
                    unsafe {
                        dst.advance_mut(n);
                    }
                    stats.s2n_read_calls += 1;
                    stats.s2n_read_bytes += n as u64;
                    stats.plaintext_bytes += n as u64;
                }
                std::task::Poll::Ready(Err(error)) => {
                    return std::task::Poll::Ready(Err(error));
                }
                std::task::Poll::Pending if total > 0 => {
                    stats.s2n_pending_after_bytes_returns += 1;
                    return std::task::Poll::Ready(Ok(total));
                }
                std::task::Poll::Pending => {
                    stats.s2n_pending_empty_returns += 1;
                    return std::task::Poll::Pending;
                }
            }
        }
    })
    .await?;

    Ok(TransportRead { bytes, stats })
}

async fn read_boring_available_into(
    inner: &mut BoringTlsStream<TcpStream>,
    dst: &mut BytesMut,
    target_read_size: usize,
) -> io::Result<TransportRead> {
    let started_len = dst.len();
    let target_read_size = target_read_size.max(1);
    let mut stats = TransportReadStats::default();
    let bytes = std::future::poll_fn(|cx| {
        loop {
            let total = dst.len().saturating_sub(started_len);
            if total >= target_read_size {
                stats.boring_target_full_returns += 1;
                return std::task::Poll::Ready(Ok(total));
            }

            dst.reserve(target_read_size - total);
            let mut read_buf = ReadBuf::uninit(dst.spare_capacity_mut());
            match std::pin::Pin::new(&mut *inner).poll_read(cx, &mut read_buf) {
                std::task::Poll::Ready(Ok(())) => {
                    let n = read_buf.filled().len();
                    if n == 0 {
                        stats.boring_zero_returns += 1;
                        return std::task::Poll::Ready(Ok(total));
                    }
                    unsafe {
                        dst.advance_mut(n);
                    }
                    stats.boring_read_calls += 1;
                    stats.boring_read_bytes += n as u64;
                    stats.plaintext_bytes += n as u64;
                }
                std::task::Poll::Ready(Err(error)) => {
                    return std::task::Poll::Ready(Err(error));
                }
                std::task::Poll::Pending if total > 0 => {
                    stats.boring_pending_after_bytes_returns += 1;
                    return std::task::Poll::Ready(Ok(total));
                }
                std::task::Poll::Pending => {
                    stats.boring_pending_empty_returns += 1;
                    return std::task::Poll::Pending;
                }
            }
        }
    })
    .await?;

    Ok(TransportRead { bytes, stats })
}

pub struct ManualTlsStream {
    tcp: TcpStream,
    tls: ClientConnection,
    read_buffer: Vec<u8>,
}

impl NntpTransport {
    /// Returns `true` if this transport is TLS-encrypted.
    pub fn is_tls(&self) -> bool {
        matches!(
            self,
            NntpTransport::Tls { .. }
                | NntpTransport::ManualTls { .. }
                | NntpTransport::S2nTls { .. }
                | NntpTransport::BoringTls { .. }
        )
    }

    pub fn remote_addr(&self) -> SocketAddr {
        match self {
            NntpTransport::Plain { remote_addr, .. }
            | NntpTransport::Tls { remote_addr, .. }
            | NntpTransport::ManualTls { remote_addr, .. }
            | NntpTransport::S2nTls { remote_addr, .. }
            | NntpTransport::BoringTls { remote_addr, .. } => *remote_addr,
        }
    }

    pub async fn read_into_buf(
        &mut self,
        dst: &mut BytesMut,
        target_read_size: usize,
    ) -> io::Result<usize> {
        match self {
            NntpTransport::Plain { inner, .. } => inner.read_buf(dst).await,
            NntpTransport::Tls { inner, .. } => inner.read_buf(dst).await,
            NntpTransport::S2nTls { inner, .. } => inner.read_buf(dst).await,
            NntpTransport::BoringTls { inner, .. } => inner.read_buf(dst).await,
            NntpTransport::ManualTls { inner, .. } => {
                inner.read_plaintext_into(dst, target_read_size).await
            }
        }
    }

    pub async fn read_into_buf_with_stats(
        &mut self,
        dst: &mut BytesMut,
        target_read_size: usize,
    ) -> io::Result<TransportRead> {
        match self {
            NntpTransport::Plain { inner, .. } => {
                let bytes = inner.read_buf(dst).await?;
                Ok(TransportRead {
                    bytes,
                    stats: TransportReadStats {
                        try_read_calls: 1,
                        try_read_bytes: bytes as u64,
                        plaintext_bytes: bytes as u64,
                        ..TransportReadStats::default()
                    },
                })
            }
            NntpTransport::Tls { inner, .. } => {
                let bytes = inner.read_buf(dst).await?;
                Ok(TransportRead {
                    bytes,
                    stats: TransportReadStats {
                        try_read_calls: 1,
                        try_read_bytes: bytes as u64,
                        plaintext_bytes: bytes as u64,
                        ..TransportReadStats::default()
                    },
                })
            }
            NntpTransport::S2nTls { inner, .. } => {
                read_s2n_available_into(inner, dst, target_read_size).await
            }
            NntpTransport::BoringTls { inner, .. } => {
                read_boring_available_into(inner, dst, target_read_size).await
            }
            NntpTransport::ManualTls { inner, .. } => {
                inner
                    .read_plaintext_into_with_stats(dst, target_read_size)
                    .await
            }
        }
    }

    pub async fn write_all(&mut self, bytes: &[u8]) -> io::Result<()> {
        match self {
            NntpTransport::Plain { inner, .. } => inner.write_all(bytes).await,
            NntpTransport::Tls { inner, .. } => inner.write_all(bytes).await,
            NntpTransport::ManualTls { inner, .. } => inner.write_all(bytes).await,
            NntpTransport::S2nTls { inner, .. } => inner.write_all(bytes).await,
            NntpTransport::BoringTls { inner, .. } => inner.write_all(bytes).await,
        }
    }

    pub async fn flush(&mut self) -> io::Result<()> {
        match self {
            NntpTransport::Plain { inner, .. } => inner.flush().await,
            NntpTransport::Tls { inner, .. } => inner.flush().await,
            NntpTransport::ManualTls { inner, .. } => inner.flush().await,
            NntpTransport::S2nTls { inner, .. } => inner.flush().await,
            NntpTransport::BoringTls { inner, .. } => inner.flush().await,
        }
    }
}

impl ManualTlsStream {
    pub(crate) async fn connect(
        tcp: TcpStream,
        config: Arc<ClientConfig>,
        server_name: ServerName<'static>,
    ) -> Result<Self, NntpError> {
        let tls = ClientConnection::new(config, server_name)?;
        let mut stream = Self {
            tcp,
            tls,
            read_buffer: vec![0u8; TLS_READ_BUFFER],
        };
        stream.complete_handshake().await.map_err(NntpError::Io)?;
        Ok(stream)
    }

    async fn complete_handshake(&mut self) -> io::Result<()> {
        let mut discarded = BytesMut::new();

        while self.tls.is_handshaking() {
            self.flush_tls().await?;

            if self.tls.is_handshaking() {
                let n = self.tcp.read(&mut self.read_buffer).await?;
                if n == 0 {
                    return Err(io::Error::new(
                        io::ErrorKind::UnexpectedEof,
                        "server closed during TLS handshake",
                    ));
                }
                self.feed_ciphertext(n, &mut discarded, None)?;
            }
        }

        self.flush_tls().await
    }

    async fn read_plaintext_into(
        &mut self,
        dst: &mut BytesMut,
        target_read_size: usize,
    ) -> io::Result<usize> {
        self.read_plaintext_into_inner(dst, target_read_size, None)
            .await
    }

    async fn read_plaintext_into_with_stats(
        &mut self,
        dst: &mut BytesMut,
        target_read_size: usize,
    ) -> io::Result<TransportRead> {
        let mut stats = TransportReadStats::default();
        let bytes = self
            .read_plaintext_into_inner(dst, target_read_size, Some(&mut stats))
            .await?;
        Ok(TransportRead { bytes, stats })
    }

    async fn read_plaintext_into_inner(
        &mut self,
        dst: &mut BytesMut,
        target_read_size: usize,
        mut stats: Option<&mut TransportReadStats>,
    ) -> io::Result<usize> {
        let started_len = dst.len();
        let drained = self.drain_plaintext(dst, stats.as_deref_mut())?;
        if drained > 0 {
            if let Some(stats) = stats.as_deref_mut() {
                stats.cached_plaintext_returns += 1;
            }
            return Ok(drained);
        }

        let read_size = target_read_size.max(TLS_READ_BUFFER);
        if self.read_buffer.len() < read_size {
            self.read_buffer.resize(read_size, 0);
        }

        loop {
            if let Some(stats) = stats.as_deref_mut() {
                stats.readiness_waits += 1;
            }
            self.tcp.readable().await?;
            let mut saw_eof = false;
            let len_before_ready = dst.len();

            loop {
                if let Some(stats) = stats.as_deref_mut() {
                    stats.try_read_calls += 1;
                }
                match self.tcp.try_read(&mut self.read_buffer[..read_size]) {
                    Ok(0) => {
                        saw_eof = true;
                        break;
                    }
                    Ok(n) => {
                        if let Some(stats) = stats.as_deref_mut() {
                            stats.try_read_bytes += n as u64;
                        }
                        self.feed_ciphertext(n, dst, stats.as_deref_mut())?;
                    }
                    Err(err) if err.kind() == io::ErrorKind::WouldBlock => {
                        if let Some(stats) = stats.as_deref_mut() {
                            stats.try_read_would_block += 1;
                        }
                        break;
                    }
                    Err(err) => return Err(err),
                }
            }

            if dst.len() > started_len {
                return Ok(dst.len() - started_len);
            }
            if saw_eof {
                return Ok(0);
            }
            if dst.len() == len_before_ready {
                if let Some(stats) = stats.as_deref_mut() {
                    stats.empty_readiness_wakes += 1;
                }
            }
        }
    }

    async fn write_all(&mut self, bytes: &[u8]) -> io::Result<()> {
        {
            let mut writer = self.tls.writer();
            writer.write_all(bytes)?;
        }
        self.flush_tls().await
    }

    async fn flush(&mut self) -> io::Result<()> {
        self.flush_tls().await
    }

    async fn flush_tls(&mut self) -> io::Result<()> {
        while self.tls.wants_write() {
            let mut outbound = Vec::new();
            self.tls.write_tls(&mut outbound)?;
            if outbound.is_empty() {
                break;
            }
            self.tcp.write_all(&outbound).await?;
        }
        self.tcp.flush().await
    }

    fn feed_ciphertext(
        &mut self,
        bytes: usize,
        dst: &mut BytesMut,
        stats: Option<&mut TransportReadStats>,
    ) -> io::Result<usize> {
        let buffer = std::mem::take(&mut self.read_buffer);
        let result = self.feed_ciphertext_slice(&buffer[..bytes], dst, stats);
        self.read_buffer = buffer;
        result
    }

    fn feed_ciphertext_slice(
        &mut self,
        ciphertext: &[u8],
        dst: &mut BytesMut,
        mut stats: Option<&mut TransportReadStats>,
    ) -> io::Result<usize> {
        let mut cursor = Cursor::new(ciphertext);
        let mut plaintext_bytes = 0usize;

        while (cursor.position() as usize) < ciphertext.len() {
            if let Some(stats) = stats.as_deref_mut() {
                stats.tls_read_calls += 1;
            }
            match self.tls.read_tls(&mut cursor) {
                Ok(0) => break,
                Ok(_) => {
                    if let Some(stats) = stats.as_deref_mut() {
                        stats.tls_process_packets_calls += 1;
                    }
                    self.tls
                        .process_new_packets()
                        .map_err(|err| io::Error::new(io::ErrorKind::InvalidData, err))?;
                    plaintext_bytes += self.drain_plaintext(dst, stats.as_deref_mut())?;
                }
                Err(err) if err.kind() == io::ErrorKind::Other => {
                    let drained = self.drain_plaintext(dst, stats.as_deref_mut())?;
                    if drained == 0 {
                        return Err(err);
                    }
                    plaintext_bytes += drained;
                }
                Err(err) => return Err(err),
            }
        }

        Ok(plaintext_bytes)
    }

    fn drain_plaintext(
        &mut self,
        dst: &mut BytesMut,
        mut stats: Option<&mut TransportReadStats>,
    ) -> io::Result<usize> {
        if let Some(stats) = stats.as_deref_mut() {
            stats.plaintext_drain_calls += 1;
        }
        let started_len = dst.len();

        loop {
            dst.reserve(TLS_PLAINTEXT_DRAIN_CHUNK);
            let old_len = dst.len();
            let spare = dst.spare_capacity_mut();
            let writable = spare.len().min(TLS_PLAINTEXT_DRAIN_CHUNK);
            if writable == 0 {
                break;
            }

            // SAFETY: rustls initializes at most `writable` bytes and reports
            // the exact byte count, after which we extend the BytesMut length.
            let target = unsafe {
                std::slice::from_raw_parts_mut(spare.as_mut_ptr().cast::<u8>(), writable)
            };

            if let Some(stats) = stats.as_deref_mut() {
                stats.plaintext_reader_calls += 1;
            }
            match self.tls.reader().read(target) {
                Ok(0) => break,
                Ok(n) => unsafe {
                    if let Some(stats) = stats.as_deref_mut() {
                        stats.plaintext_bytes += n as u64;
                    }
                    dst.set_len(old_len + n);
                },
                Err(err) if err.kind() == io::ErrorKind::WouldBlock => {
                    if let Some(stats) = stats.as_deref_mut() {
                        stats.plaintext_reader_would_block += 1;
                    }
                    break;
                }
                Err(err) => return Err(err),
            }
        }

        Ok(dst.len() - started_len)
    }
}

impl AsyncRead for NntpTransport {
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        match self.project() {
            NntpTransportProj::Plain { inner, .. } => inner.poll_read(cx, buf),
            NntpTransportProj::Tls { inner, .. } => inner.poll_read(cx, buf),
            NntpTransportProj::S2nTls { inner, .. } => inner.poll_read(cx, buf),
            NntpTransportProj::BoringTls { inner, .. } => inner.poll_read(cx, buf),
            NntpTransportProj::ManualTls { .. } => std::task::Poll::Ready(Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "manual TLS transport requires read_into_buf",
            ))),
        }
    }
}

impl AsyncWrite for NntpTransport {
    fn poll_write(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
    ) -> std::task::Poll<std::io::Result<usize>> {
        match self.project() {
            NntpTransportProj::Plain { inner, .. } => inner.poll_write(cx, buf),
            NntpTransportProj::Tls { inner, .. } => inner.poll_write(cx, buf),
            NntpTransportProj::S2nTls { inner, .. } => inner.poll_write(cx, buf),
            NntpTransportProj::BoringTls { inner, .. } => inner.poll_write(cx, buf),
            NntpTransportProj::ManualTls { .. } => std::task::Poll::Ready(Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "manual TLS transport requires NntpTransport::write_all",
            ))),
        }
    }

    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        match self.project() {
            NntpTransportProj::Plain { inner, .. } => inner.poll_flush(cx),
            NntpTransportProj::Tls { inner, .. } => inner.poll_flush(cx),
            NntpTransportProj::S2nTls { inner, .. } => inner.poll_flush(cx),
            NntpTransportProj::BoringTls { inner, .. } => inner.poll_flush(cx),
            NntpTransportProj::ManualTls { .. } => std::task::Poll::Ready(Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "manual TLS transport requires NntpTransport::flush",
            ))),
        }
    }

    fn poll_shutdown(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        match self.project() {
            NntpTransportProj::Plain { inner, .. } => inner.poll_shutdown(cx),
            NntpTransportProj::Tls { inner, .. } => inner.poll_shutdown(cx),
            NntpTransportProj::S2nTls { inner, .. } => inner.poll_shutdown(cx),
            NntpTransportProj::BoringTls { inner, .. } => inner.poll_shutdown(cx),
            NntpTransportProj::ManualTls { .. } => std::task::Poll::Ready(Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "manual TLS transport shutdown is handled by dropping the connection",
            ))),
        }
    }
}

/// Build a `rustls` `ClientConfig` using Mozilla root certificates,
/// optionally augmented with a custom CA certificate from a PEM file.
pub fn build_tls_config(ca_cert_path: Option<&Path>) -> Result<Arc<ClientConfig>, NntpError> {
    let mut root_store = RootCertStore::empty();
    root_store.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());

    // Load additional CA certificate if provided
    if let Some(path) = ca_cert_path {
        let pem_data = std::fs::read(path).map_err(|e| {
            NntpError::MalformedResponse(format!("failed to read CA cert {}: {e}", path.display()))
        })?;
        let mut cursor = std::io::Cursor::new(&pem_data);
        let certs: Vec<_> = rustls_pemfile::certs(&mut cursor)
            .filter_map(|r| r.ok())
            .collect();
        if certs.is_empty() {
            return Err(NntpError::MalformedResponse(format!(
                "no valid certificates found in {}",
                path.display()
            )));
        }
        for cert in certs {
            root_store
                .add(cert)
                .map_err(|e| NntpError::MalformedResponse(format!("invalid CA cert: {e}")))?;
        }
    }

    let provider = tokio_rustls::rustls::crypto::aws_lc_rs::default_provider();
    let config = ClientConfig::builder_with_provider(Arc::new(provider))
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_root_certificates(root_store)
        .with_no_client_auth();

    Ok(Arc::new(config))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum NntpTlsBackend {
    ManualRustls,
    S2n,
    Boring,
}

fn selected_tls_backend() -> Result<NntpTlsBackend, NntpError> {
    match std::env::var(TLS_BACKEND_ENV) {
        Ok(value) if value.eq_ignore_ascii_case("s2n") => Ok(NntpTlsBackend::S2n),
        Ok(value)
            if value.eq_ignore_ascii_case("boring")
                || value.eq_ignore_ascii_case("boringssl")
                || value.eq_ignore_ascii_case("tokio-boring")
                || value.eq_ignore_ascii_case("tokio_boring") =>
        {
            Ok(NntpTlsBackend::Boring)
        }
        Ok(value)
            if value.eq_ignore_ascii_case("rustls")
                || value.eq_ignore_ascii_case("manual-rustls")
                || value.eq_ignore_ascii_case("manual_rustls") =>
        {
            Ok(NntpTlsBackend::ManualRustls)
        }
        Ok(value) => Err(NntpError::MalformedResponse(format!(
            "unsupported {TLS_BACKEND_ENV} value {value:?}; expected boring, s2n, or manual-rustls"
        ))),
        Err(std::env::VarError::NotPresent) => Ok(NntpTlsBackend::ManualRustls),
        Err(error) => Err(NntpError::MalformedResponse(format!(
            "failed to read {TLS_BACKEND_ENV}: {error}"
        ))),
    }
}

fn env_flag_enabled(name: &str) -> Result<bool, NntpError> {
    match std::env::var(name) {
        Ok(value) => match value.trim() {
            "1" => Ok(true),
            "0" | "" => Ok(false),
            value
                if value.eq_ignore_ascii_case("true")
                    || value.eq_ignore_ascii_case("yes")
                    || value.eq_ignore_ascii_case("on") =>
            {
                Ok(true)
            }
            value
                if value.eq_ignore_ascii_case("false")
                    || value.eq_ignore_ascii_case("no")
                    || value.eq_ignore_ascii_case("off") =>
            {
                Ok(false)
            }
            value => Err(NntpError::MalformedResponse(format!(
                "unsupported {name} value {value:?}; expected boolean"
            ))),
        },
        Err(std::env::VarError::NotPresent) => Ok(false),
        Err(error) => Err(NntpError::MalformedResponse(format!(
            "failed to read {name}: {error}"
        ))),
    }
}

pub(crate) fn build_s2n_tls_config(ca_cert_path: Option<&Path>) -> Result<S2nConfig, NntpError> {
    let ca_cert_path = ca_cert_path.ok_or_else(|| {
        NntpError::MalformedResponse(format!(
            "{TLS_BACKEND_ENV}=s2n currently requires a CA PEM path for deterministic NNTP trust"
        ))
    })?;
    let pem_data = std::fs::read(ca_cert_path).map_err(|error| {
        NntpError::MalformedResponse(format!(
            "failed to read s2n CA PEM {}: {error}",
            ca_cert_path.display()
        ))
    })?;

    let mut builder = S2nConfig::builder();
    builder
        .with_system_certs(false)
        .map_err(|error| s2n_config_error("disable system certs", error))?;
    builder
        .set_security_policy(&security::DEFAULT_TLS13)
        .map_err(|error| s2n_config_error("set TLS 1.3 security policy", error))?;
    builder
        .trust_pem(&pem_data)
        .map_err(|error| s2n_config_error("load CA PEM", error))?;
    let mut config = builder
        .build()
        .map_err(|error| s2n_config_error("build config", error))?;
    enable_s2n_recv_multi_record(&mut config)?;
    Ok(config)
}

fn build_boring_tls_config(ca_cert_path: Option<&Path>) -> Result<ConnectConfiguration, NntpError> {
    let ca_cert_path = ca_cert_path.ok_or_else(|| {
        NntpError::MalformedResponse(format!(
            "{TLS_BACKEND_ENV}=boring currently requires a CA PEM path for deterministic NNTP trust"
        ))
    })?;
    let mut builder = SslConnector::builder(SslMethod::tls())
        .map_err(|error| boring_config_error("create connector", error))?;
    builder
        .set_min_proto_version(Some(SslVersion::TLS1_3))
        .map_err(|error| boring_config_error("set minimum TLS version", error))?;
    builder
        .set_max_proto_version(Some(SslVersion::TLS1_3))
        .map_err(|error| boring_config_error("set maximum TLS version", error))?;
    builder
        .set_ca_file(ca_cert_path)
        .map_err(|error| boring_config_error("load CA PEM", error))?;
    if env_flag_enabled(BORING_READ_AHEAD_ENV)? {
        builder.set_read_ahead(true);
    }
    builder.set_verify(SslVerifyMode::PEER);
    builder
        .build()
        .configure()
        .map_err(|error| boring_config_error("configure connector", error))
}

fn enable_s2n_recv_multi_record(config: &mut S2nConfig) -> Result<(), NntpError> {
    let config_ptr =
        unsafe { *(std::ptr::from_mut(config) as *mut std::ptr::NonNull<s2n_tls_sys::s2n_config>) };
    let result =
        unsafe { s2n_tls_sys::s2n_config_set_recv_multi_record(config_ptr.as_ptr(), true) };
    if result < 0 {
        return Err(NntpError::MalformedResponse(
            "s2n TLS config failed to enable multi-record receive".into(),
        ));
    }
    Ok(())
}

fn s2n_config_error(context: &str, error: s2n_tls::error::Error) -> NntpError {
    NntpError::MalformedResponse(format!("s2n TLS config failed to {context}: {error}"))
}

fn boring_config_error(context: &str, error: boring::error::ErrorStack) -> NntpError {
    NntpError::MalformedResponse(format!("BoringSSL TLS config failed to {context}: {error}"))
}

fn s2n_handshake_error(error: s2n_tls::error::Error) -> NntpError {
    NntpError::MalformedResponse(format!("s2n TLS handshake failed: {error}"))
}

fn boring_handshake_error(error: tokio_boring::HandshakeError<TcpStream>) -> NntpError {
    NntpError::MalformedResponse(format!("BoringSSL TLS handshake failed: {error}"))
}

async fn connect_s2n_tls(
    tcp: TcpStream,
    tls_config: S2nConfig,
    host: &str,
) -> Result<S2nTlsStream<TcpStream>, NntpError> {
    let builder = s2n_tls::connection::ModifiedBuilder::new(
        tls_config,
        |conn: &mut s2n_tls::connection::Connection| {
            conn.prefer_throughput()?;
            conn.set_receive_buffering(true)
        },
    );
    S2nTlsConnector::new(builder)
        .connect(host, tcp)
        .await
        .map_err(s2n_handshake_error)
}

async fn connect_boring_tls(
    tcp: TcpStream,
    tls_config: ConnectConfiguration,
    host: &str,
) -> Result<BoringTlsStream<TcpStream>, NntpError> {
    let stream = tokio_boring::connect(tls_config, host, tcp)
        .await
        .map_err(boring_handshake_error)?;
    let version = stream.ssl().version_str();
    let cipher = stream
        .ssl()
        .current_cipher()
        .and_then(|cipher| cipher.standard_name())
        .unwrap_or("<unknown>");
    if version != "TLSv1.3" || cipher != "TLS_AES_128_GCM_SHA256" {
        return Err(NntpError::MalformedResponse(format!(
            "BoringSSL negotiated unexpected TLS settings: version={version}, cipher={cipher}"
        )));
    }
    Ok(stream)
}

/// Create a `ServerName` from a hostname string.
fn make_server_name(host: &str) -> Result<ServerName<'static>, NntpError> {
    ServerName::try_from(host.to_string())
        .map_err(|_| NntpError::MalformedResponse(format!("invalid hostname for TLS: {host}")))
}

/// Configure TCP keepalive on a socket to prevent connections from silently
/// dying behind NATs/firewalls.
fn set_keepalive(tcp: &TcpStream) {
    let sock_ref = SockRef::from(tcp);
    let ka = socket2::TcpKeepalive::new()
        .with_time(Duration::from_mins(1))
        .with_interval(Duration::from_secs(15));
    let _ = sock_ref.set_tcp_keepalive(&ka);
}

async fn resolve_connect_addrs(
    host: &str,
    port: u16,
    excluded_ips: &[IpAddr],
    address_offset: usize,
) -> Result<Vec<SocketAddr>, NntpError> {
    let addrs: Vec<SocketAddr> = lookup_host((host, port)).await?.collect();
    filter_and_rotate_addrs(addrs, excluded_ips, address_offset).map_err(|_| {
        NntpError::Io(std::io::Error::new(
            std::io::ErrorKind::AddrNotAvailable,
            format!("no resolved addresses for {host}:{port} after IP exclusions"),
        ))
    })
}

fn filter_and_rotate_addrs(
    mut addrs: Vec<SocketAddr>,
    excluded_ips: &[IpAddr],
    address_offset: usize,
) -> std::result::Result<Vec<SocketAddr>, ()> {
    addrs.retain(|addr| !excluded_ips.contains(&addr.ip()));

    if addrs.is_empty() {
        return Err(());
    }

    let offset = address_offset % addrs.len();
    addrs.rotate_left(offset);
    Ok(addrs)
}

async fn connect_tcp_from_resolved(
    addrs: &[SocketAddr],
) -> Result<(TcpStream, SocketAddr), NntpError> {
    let mut last_error = None;
    for addr in addrs {
        match TcpStream::connect(addr).await {
            Ok(tcp) => {
                let remote_addr = tcp.peer_addr()?;
                tcp.set_nodelay(true)?;
                set_keepalive(&tcp);
                return Ok((tcp, remote_addr));
            }
            Err(error) => {
                last_error = Some(error);
            }
        }
    }

    Err(NntpError::Io(last_error.unwrap_or_else(|| {
        std::io::Error::new(
            std::io::ErrorKind::AddrNotAvailable,
            "no resolved addresses",
        )
    })))
}

/// Connect to a host with implicit TLS (e.g. port 563).
///
/// Performs TCP connect followed by an immediate TLS handshake.
/// If `ca_cert_path` is provided, the certificate is trusted in addition to
/// the Mozilla root store.
pub async fn connect_tls(
    host: &str,
    port: u16,
    ca_cert_path: Option<&Path>,
) -> Result<NntpTransport, NntpError> {
    connect_tls_with_ip_policy(host, port, ca_cert_path, &[], 0).await
}

pub async fn connect_tls_with_ip_policy(
    host: &str,
    port: u16,
    ca_cert_path: Option<&Path>,
    excluded_ips: &[IpAddr],
    address_offset: usize,
) -> Result<NntpTransport, NntpError> {
    let addrs = resolve_connect_addrs(host, port, excluded_ips, address_offset).await?;
    let (tcp, remote_addr) = connect_tcp_from_resolved(&addrs).await?;
    match selected_tls_backend()? {
        NntpTlsBackend::ManualRustls => {
            let tls_config = build_tls_config(ca_cert_path)?;
            let server_name = make_server_name(host)?;
            let manual_tls = ManualTlsStream::connect(tcp, tls_config, server_name).await?;
            Ok(NntpTransport::ManualTls {
                inner: manual_tls,
                remote_addr,
            })
        }
        NntpTlsBackend::S2n => {
            let tls_config = build_s2n_tls_config(ca_cert_path)?;
            let s2n_tls = connect_s2n_tls(tcp, tls_config, host).await?;
            Ok(NntpTransport::S2nTls {
                inner: s2n_tls,
                remote_addr,
            })
        }
        NntpTlsBackend::Boring => {
            let tls_config = build_boring_tls_config(ca_cert_path)?;
            let boring_tls = connect_boring_tls(tcp, tls_config, host).await?;
            Ok(NntpTransport::BoringTls {
                inner: boring_tls,
                remote_addr,
            })
        }
    }
}

/// Connect to a host with plain TCP (e.g. port 119).
pub async fn connect_plain(host: &str, port: u16) -> Result<NntpTransport, NntpError> {
    connect_plain_with_ip_policy(host, port, &[], 0).await
}

pub async fn connect_plain_with_ip_policy(
    host: &str,
    port: u16,
    excluded_ips: &[IpAddr],
    address_offset: usize,
) -> Result<NntpTransport, NntpError> {
    let addrs = resolve_connect_addrs(host, port, excluded_ips, address_offset).await?;
    let (tcp, remote_addr) = connect_tcp_from_resolved(&addrs).await?;
    Ok(NntpTransport::Plain {
        inner: tcp,
        remote_addr,
    })
}

/// Upgrade an existing plain TCP connection to TLS (STARTTLS).
///
/// The caller should already have sent the STARTTLS command and received
/// a 382 response before calling this function.
pub async fn upgrade_starttls(
    transport: NntpTransport,
    host: &str,
    ca_cert_path: Option<&Path>,
) -> Result<NntpTransport, NntpError> {
    let (tcp, remote_addr) = match transport {
        NntpTransport::Plain { inner, remote_addr } => (inner, remote_addr),
        NntpTransport::Tls { .. }
        | NntpTransport::ManualTls { .. }
        | NntpTransport::S2nTls { .. }
        | NntpTransport::BoringTls { .. } => {
            return Err(NntpError::MalformedResponse(
                "cannot STARTTLS on an already-TLS connection".into(),
            ));
        }
    };

    match selected_tls_backend()? {
        NntpTlsBackend::ManualRustls => {
            let tls_config = build_tls_config(ca_cert_path)?;
            let server_name = make_server_name(host)?;
            let manual_tls = ManualTlsStream::connect(tcp, tls_config, server_name).await?;
            Ok(NntpTransport::ManualTls {
                inner: manual_tls,
                remote_addr,
            })
        }
        NntpTlsBackend::S2n => {
            let tls_config = build_s2n_tls_config(ca_cert_path)?;
            let s2n_tls = connect_s2n_tls(tcp, tls_config, host).await?;
            Ok(NntpTransport::S2nTls {
                inner: s2n_tls,
                remote_addr,
            })
        }
        NntpTlsBackend::Boring => {
            let tls_config = build_boring_tls_config(ca_cert_path)?;
            let boring_tls = connect_boring_tls(tcp, tls_config, host).await?;
            Ok(NntpTransport::BoringTls {
                inner: boring_tls,
                remote_addr,
            })
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::time::{SystemTime, UNIX_EPOCH};

    use tokio::io::AsyncWriteExt;
    use tokio::net::TcpListener;
    use tokio::sync::oneshot;
    use tokio_rustls::TlsAcceptor;
    use tokio_rustls::rustls::ServerConfig as RustlsServerConfig;
    use tokio_rustls::rustls::pki_types::{PrivateKeyDer, PrivatePkcs8KeyDer};

    const TLS_DRAIN_RECORD_BYTES: usize = 16 * 1024;
    const TLS_DRAIN_RECORDS: usize = 8;
    const TLS_DRAIN_PAYLOAD_BYTES: usize = TLS_DRAIN_RECORD_BYTES * TLS_DRAIN_RECORDS;
    const TLS_TEST_BUFFER_BYTES: usize = 256 * 1024;

    #[test]
    fn filter_and_rotate_addrs_excludes_ips_before_rotation() {
        let a: SocketAddr = "127.0.0.1:443".parse().unwrap();
        let b: SocketAddr = "127.0.0.2:443".parse().unwrap();
        let c: SocketAddr = "127.0.0.3:443".parse().unwrap();

        let ordered =
            filter_and_rotate_addrs(vec![a, b, c], &[a.ip()], 1).expect("alternate addresses");

        assert_eq!(ordered, vec![c, b]);
    }

    #[test]
    fn filter_and_rotate_addrs_errors_when_all_ips_excluded() {
        let a: SocketAddr = "127.0.0.1:443".parse().unwrap();

        assert!(filter_and_rotate_addrs(vec![a], &[a.ip()], 0).is_err());
    }

    #[tokio::test]
    async fn tcp_keepalive_is_configured() {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        let tcp = TcpStream::connect(addr).await.unwrap();
        tcp.set_nodelay(true).unwrap();
        set_keepalive(&tcp);

        let nodelay = tcp.nodelay().unwrap();
        assert!(nodelay, "TCP_NODELAY should be enabled");

        let sock_ref = SockRef::from(&tcp);
        let keepalive = sock_ref.keepalive().unwrap();
        assert!(keepalive, "TCP keepalive should be enabled");

        let ka_time = sock_ref.tcp_keepalive_time().unwrap();
        assert_eq!(ka_time, Duration::from_mins(1));

        let ka_interval = sock_ref.tcp_keepalive_interval().unwrap();
        assert_eq!(ka_interval, Duration::from_secs(15));
    }

    fn s2n_probe_tls_config() -> (Arc<RustlsServerConfig>, std::path::PathBuf) {
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

        let nonce = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let ca_path = std::env::temp_dir().join(format!(
            "weaver-nntp-s2n-probe-ca-{}-{nonce}.pem",
            std::process::id()
        ));
        std::fs::write(&ca_path, cert_pem).expect("write s2n probe CA PEM");

        (Arc::new(server_config), ca_path)
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

    #[tokio::test]
    #[ignore = "diagnostic read-shape probe; run explicitly with --ignored --nocapture"]
    async fn s2n_tls_drain_probe() {
        let (server_config, ca_path) = s2n_probe_tls_config();
        let (addr, flushed_rx) = spawn_tls_drain_server(server_config).await;
        let tcp = TcpStream::connect(addr).await.unwrap();
        let tls_config = build_s2n_tls_config(Some(&ca_path)).expect("s2n TLS config");
        let s2n_tls = connect_s2n_tls(tcp, tls_config, "localhost")
            .await
            .expect("s2n TLS connect");
        let mut transport = NntpTransport::S2nTls {
            inner: s2n_tls,
            remote_addr: addr,
        };
        let mut read_buf = BytesMut::with_capacity(TLS_TEST_BUFFER_BYTES);
        let mut total = 0usize;
        let mut aggregate = TransportReadStats::default();
        let mut first_read_bytes = 0usize;

        while total < TLS_DRAIN_PAYLOAD_BYTES {
            let read = transport
                .read_into_buf_with_stats(&mut read_buf, TLS_TEST_BUFFER_BYTES)
                .await
                .expect("s2n read");
            assert_ne!(read.bytes, 0, "s2n TLS stream closed before probe payload");
            if first_read_bytes == 0 {
                first_read_bytes = read.bytes;
            }
            total += read.bytes;
            aggregate.add(read.stats);
        }

        flushed_rx.await.expect("server flushed probe records");
        let _ = std::fs::remove_file(&ca_path);
        let bytes_per_read_call = if aggregate.s2n_read_calls == 0 {
            0
        } else {
            aggregate.s2n_read_bytes / aggregate.s2n_read_calls
        };

        println!(
            "s2n_tls_drain_probe first_read_bytes={first_read_bytes} total_bytes={total} records={TLS_DRAIN_RECORDS} s2n_read_calls={} bytes_per_read_call={bytes_per_read_call}",
            aggregate.s2n_read_calls
        );
        assert_eq!(total, TLS_DRAIN_PAYLOAD_BYTES);
        assert_eq!(aggregate.s2n_read_bytes as usize, total);
        assert!(
            aggregate.s2n_read_calls < TLS_DRAIN_RECORDS as u64,
            "multi-record s2n recv should consume more than one TLS record per s2n_recv"
        );
    }
}
