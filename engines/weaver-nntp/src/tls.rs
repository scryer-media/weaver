use std::io::{self, Cursor, Read, Write};
use std::net::{IpAddr, SocketAddr};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use bytes::BytesMut;
use socket2::SockRef;
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, ReadBuf};
use tokio::net::{TcpStream, lookup_host};
use tokio_rustls::client::TlsStream;
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
        Tls { #[pin] inner: TlsStream<TcpStream>, remote_addr: SocketAddr },
        /// TLS-encrypted TCP driven directly through rustls.
        ManualTls { inner: ManualTlsStream, remote_addr: SocketAddr },
    }
}

const TLS_READ_BUFFER: usize = 256 * 1024;
const TLS_PLAINTEXT_DRAIN_CHUNK: usize = 16 * 1024;

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
    }
}

pub struct TransportRead {
    pub bytes: usize,
    pub stats: TransportReadStats,
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
            NntpTransport::Tls { .. } | NntpTransport::ManualTls { .. }
        )
    }

    pub fn remote_addr(&self) -> SocketAddr {
        match self {
            NntpTransport::Plain { remote_addr, .. }
            | NntpTransport::Tls { remote_addr, .. }
            | NntpTransport::ManualTls { remote_addr, .. } => *remote_addr,
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
        }
    }

    pub async fn flush(&mut self) -> io::Result<()> {
        match self {
            NntpTransport::Plain { inner, .. } => inner.flush().await,
            NntpTransport::Tls { inner, .. } => inner.flush().await,
            NntpTransport::ManualTls { inner, .. } => inner.flush().await,
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
    let tls_config = build_tls_config(ca_cert_path)?;
    let server_name = make_server_name(host)?;
    let manual_tls = ManualTlsStream::connect(tcp, tls_config, server_name).await?;
    Ok(NntpTransport::ManualTls {
        inner: manual_tls,
        remote_addr,
    })
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
        NntpTransport::Tls { .. } | NntpTransport::ManualTls { .. } => {
            return Err(NntpError::MalformedResponse(
                "cannot STARTTLS on an already-TLS connection".into(),
            ));
        }
    };

    let tls_config = build_tls_config(ca_cert_path)?;
    let server_name = make_server_name(host)?;
    let manual_tls = ManualTlsStream::connect(tcp, tls_config, server_name).await?;
    Ok(NntpTransport::ManualTls {
        inner: manual_tls,
        remote_addr,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::net::TcpListener;

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
}
