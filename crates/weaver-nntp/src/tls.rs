use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use socket2::SockRef;
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio::net::TcpStream;
use tokio_rustls::TlsConnector;
use tokio_rustls::client::TlsStream;
use tokio_rustls::rustls::pki_types::ServerName;
use tokio_rustls::rustls::{ClientConfig, RootCertStore};

use crate::error::NntpError;

pin_project_lite::pin_project! {
    /// A transport that is either a plain TCP connection or a TLS-wrapped one.
    ///
    /// Implements `AsyncRead` and `AsyncWrite` by delegating to the inner stream.
    #[project = NntpTransportProj]
    pub enum NntpTransport {
        /// Unencrypted TCP.
        Plain { #[pin] inner: TcpStream },
        /// TLS-encrypted TCP.
        Tls { #[pin] inner: TlsStream<TcpStream> },
    }
}

impl NntpTransport {
    /// Returns `true` if this transport is TLS-encrypted.
    pub fn is_tls(&self) -> bool {
        matches!(self, NntpTransport::Tls { .. })
    }
}

impl AsyncRead for NntpTransport {
    fn poll_read(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        match self.project() {
            NntpTransportProj::Plain { inner } => inner.poll_read(cx, buf),
            NntpTransportProj::Tls { inner } => inner.poll_read(cx, buf),
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
            NntpTransportProj::Plain { inner } => inner.poll_write(cx, buf),
            NntpTransportProj::Tls { inner } => inner.poll_write(cx, buf),
        }
    }

    fn poll_flush(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        match self.project() {
            NntpTransportProj::Plain { inner } => inner.poll_flush(cx),
            NntpTransportProj::Tls { inner } => inner.poll_flush(cx),
        }
    }

    fn poll_shutdown(
        self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> std::task::Poll<std::io::Result<()>> {
        match self.project() {
            NntpTransportProj::Plain { inner } => inner.poll_shutdown(cx),
            NntpTransportProj::Tls { inner } => inner.poll_shutdown(cx),
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

    let provider = tokio_rustls::rustls::crypto::ring::default_provider();
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
    let addr = format!("{host}:{port}");
    let tcp = TcpStream::connect(&addr).await?;
    tcp.set_nodelay(true)?;
    set_keepalive(&tcp);
    let tls_config = build_tls_config(ca_cert_path)?;
    let connector = TlsConnector::from(tls_config);
    let server_name = make_server_name(host)?;
    let tls_stream = connector.connect(server_name, tcp).await?;
    Ok(NntpTransport::Tls { inner: tls_stream })
}

/// Connect to a host with plain TCP (e.g. port 119).
pub async fn connect_plain(host: &str, port: u16) -> Result<NntpTransport, NntpError> {
    let addr = format!("{host}:{port}");
    let tcp = TcpStream::connect(&addr).await?;
    tcp.set_nodelay(true)?;
    set_keepalive(&tcp);
    Ok(NntpTransport::Plain { inner: tcp })
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
    let tcp = match transport {
        NntpTransport::Plain { inner } => inner,
        NntpTransport::Tls { .. } => {
            return Err(NntpError::MalformedResponse(
                "cannot STARTTLS on an already-TLS connection".into(),
            ));
        }
    };

    let tls_config = build_tls_config(ca_cert_path)?;
    let connector = TlsConnector::from(tls_config);
    let server_name = make_server_name(host)?;
    let tls_stream = connector.connect(server_name, tcp).await?;
    Ok(NntpTransport::Tls { inner: tls_stream })
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::net::TcpListener;

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

        let ka_time = sock_ref.keepalive_time().unwrap();
        assert_eq!(ka_time, Duration::from_mins(1));

        let ka_interval = sock_ref.keepalive_interval().unwrap();
        assert_eq!(ka_interval, Duration::from_secs(15));
    }
}
