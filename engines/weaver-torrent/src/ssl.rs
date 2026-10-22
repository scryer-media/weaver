// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

//! SSL torrents. A torrent whose info dict carries `ssl-cert` names the CA
//! every one of its peers is issued under. Each of its peer connections is
//! TLS over the stream that connected, both ends presenting a chain under
//! that CA and checking the other's against it alone; the client names the
//! torrent by its hex info hash in the SNI, which is how the session's SSL
//! listener finds the torrent whose CA and identity apply. The plain
//! BitTorrent handshake follows inside TLS, and message stream encryption
//! never runs on such a connection. The torrent's HTTPS announces present
//! the same identity and accept a tracker under the torrent's CA as well as
//! the system roots. Without an identity of our own the torrent connects to
//! no peer and announces nowhere.

use std::{
    io,
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};

use anyhow::Context as _;
use arc_swap::ArcSwapOption;
use base64::Engine as _;
use rustls_pki_types::{CertificateDer, PrivateKeyDer, ServerName, UnixTime, pem::PemObject};
use tokio::io::{AsyncRead, AsyncWrite, ReadBuf};
use tokio_rustls::rustls::{
    self, ClientConfig, DigitallySignedStruct, RootCertStore, ServerConfig, SignatureScheme,
    client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier},
    crypto::{CryptoProvider, WebPkiSupportedAlgorithms},
    server::{ParsedCertificate, WebPkiClientVerifier},
    sign::CertifiedKey,
};

use crate::{
    core::hash_id::{Id20, InfoHashes},
    type_aliases::{BoxAsyncReadVectored, BoxAsyncWrite},
    vectored_traits::AsyncReadVectoredIntoCompat,
};

/// Who we are to an SSL torrent's peers and trackers: a certificate chain
/// issued under the torrent's CA, our end's first, and its private key.
pub struct SslIdentity {
    pub certificate_chain: Vec<CertificateDer<'static>>,
    pub private_key: PrivateKeyDer<'static>,
}

impl SslIdentity {
    /// The identity in `certificate_chain` (one or more PEM certificates,
    /// our end's first) and `private_key` (one PEM private key).
    pub fn from_pem(certificate_chain: &[u8], private_key: &[u8]) -> anyhow::Result<Self> {
        let certificate_chain = CertificateDer::pem_slice_iter(certificate_chain)
            .collect::<Result<Vec<_>, _>>()
            .context("the certificate is not PEM")?;
        if certificate_chain.is_empty() {
            anyhow::bail!("no certificate is given");
        }
        let private_key =
            PrivateKeyDer::from_pem_slice(private_key).context("the private key is not PEM")?;
        Ok(Self {
            certificate_chain,
            private_key,
        })
    }

    /// The key is the certificate's: refused otherwise, as is a key the
    /// TLS provider cannot sign with.
    pub fn check(&self) -> anyhow::Result<()> {
        CertifiedKey::from_der(
            self.certificate_chain.clone(),
            self.private_key.clone_key(),
            &provider(),
        )
        .context("the private key does not match the certificate")?;
        Ok(())
    }

    // Both as one PEM document, the key first.
    fn to_pem(&self) -> Vec<u8> {
        let label = match &self.private_key {
            PrivateKeyDer::Pkcs1(_) => "RSA PRIVATE KEY",
            PrivateKeyDer::Sec1(_) => "EC PRIVATE KEY",
            _ => "PRIVATE KEY",
        };
        let mut out = pem(label, self.private_key.secret_der());
        for cert in &self.certificate_chain {
            out.extend(pem("CERTIFICATE", cert));
        }
        out
    }
}

impl Clone for SslIdentity {
    fn clone(&self) -> Self {
        Self {
            certificate_chain: self.certificate_chain.clone(),
            private_key: self.private_key.clone_key(),
        }
    }
}

impl std::fmt::Debug for SslIdentity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SslIdentity")
            .field("certificates", &self.certificate_chain.len())
            .finish_non_exhaustive()
    }
}

fn pem(label: &str, der: &[u8]) -> Vec<u8> {
    let body = base64::engine::general_purpose::STANDARD.encode(der);
    let mut out = format!("-----BEGIN {label}-----\n");
    for line in body.as_bytes().chunks(64) {
        out.push_str(std::str::from_utf8(line).unwrap_or_default());
        out.push('\n');
    }
    out.push_str(&format!("-----END {label}-----\n"));
    out.into_bytes()
}

fn provider() -> CryptoProvider {
    rustls::crypto::aws_lc_rs::default_provider()
}

/// The TLS an SSL torrent's connections are made under while it has an
/// identity.
struct Tls {
    client: Arc<ClientConfig>,
    server: Arc<ServerConfig>,
    identity: SslIdentity,
}

/// What makes a torrent an SSL torrent: its CA, the names it answers to in
/// the SNI, and our identity under that CA once one is given.
pub(crate) struct SslTorrent {
    ca: CertificateDer<'static>,
    roots: Arc<RootCertStore>,
    // The SNI a connection we make sends: the hex hash the torrent is known
    // by.
    sni: ServerName<'static>,
    // Every hex hash the torrent answers to.
    names: Vec<String>,
    tls: ArcSwapOption<Tls>,
}

impl std::fmt::Debug for SslTorrent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SslTorrent")
            .field("sni", &self.sni)
            .field("identity", &self.has_identity())
            .finish_non_exhaustive()
    }
}

impl SslTorrent {
    /// The SSL torrent whose `ssl-cert` is `ca_pem`, known by `info_hashes`.
    pub fn new(ca_pem: &[u8], info_hashes: InfoHashes) -> anyhow::Result<Self> {
        let ca = CertificateDer::from_pem_slice(ca_pem).context("the torrent's CA is not PEM")?;
        let mut roots = RootCertStore::empty();
        roots
            .add(ca.clone())
            .context("the torrent's CA is not a certificate")?;
        let name = |id: Id20| id.as_string();
        let sni = ServerName::try_from(name(info_hashes.best()))
            .context("the info hash is not a server name")?;
        Ok(Self {
            ca,
            roots: Arc::new(roots),
            sni,
            names: info_hashes.ids20().map(name).collect(),
            tls: ArcSwapOption::empty(),
        })
    }

    /// Our identity from now on: new connections and announces present
    /// it, those running keep theirs. `None` leaves the torrent without one.
    pub fn set_identity(&self, identity: Option<SslIdentity>) -> anyhow::Result<()> {
        let tls = identity.map(|identity| self.tls(identity)).transpose()?;
        self.tls.store(tls.map(Arc::new));
        Ok(())
    }

    pub fn has_identity(&self) -> bool {
        self.tls.load().is_some()
    }

    /// Whether a connection naming `sni` is for this torrent.
    pub fn answers_to(&self, sni: &str) -> bool {
        self.names.iter().any(|name| name.eq_ignore_ascii_case(sni))
    }

    /// What an incoming connection to the torrent is accepted under, while
    /// we have an identity.
    pub fn server_config(&self) -> Option<Arc<ServerConfig>> {
        self.tls.load().as_ref().map(|tls| tls.server.clone())
    }

    fn tls(&self, identity: SslIdentity) -> anyhow::Result<Tls> {
        identity.check()?;
        let provider = Arc::new(provider());
        let peers = Arc::new(UnderCa {
            roots: self.roots.clone(),
            algorithms: provider.signature_verification_algorithms,
        });
        let client = ClientConfig::builder_with_provider(provider.clone())
            .with_safe_default_protocol_versions()?
            .dangerous()
            .with_custom_certificate_verifier(peers)
            .with_client_auth_cert(
                identity.certificate_chain.clone(),
                identity.private_key.clone_key(),
            )
            .context("the identity cannot be presented")?;
        let clients =
            WebPkiClientVerifier::builder_with_provider(self.roots.clone(), provider.clone())
                .build()
                .context("the torrent's CA cannot verify peers")?;
        let server = ServerConfig::builder_with_provider(provider)
            .with_safe_default_protocol_versions()?
            .with_client_cert_verifier(clients)
            .with_single_cert(
                identity.certificate_chain.clone(),
                identity.private_key.clone_key(),
            )
            .context("the identity cannot be presented")?;
        Ok(Tls {
            client: Arc::new(client),
            server: Arc::new(server),
            identity,
        })
    }

    /// TLS over the stream that connected to a peer, as the client: the
    /// halves carry the BitTorrent handshake from here on.
    pub async fn connect(
        &self,
        reader: BoxAsyncReadVectored,
        writer: BoxAsyncWrite,
    ) -> io::Result<(BoxAsyncReadVectored, BoxAsyncWrite)> {
        let client = self
            .tls
            .load()
            .as_ref()
            .map(|tls| tls.client.clone())
            .ok_or_else(|| io::Error::other("the SSL torrent has no identity"))?;
        let stream = tokio_rustls::TlsConnector::from(client)
            .connect(self.sni.clone(), Joined { reader, writer })
            .await?;
        Ok(halves(stream))
    }

    /// `builder` for the torrent's HTTPS announces: it presents our
    /// identity, and takes a tracker under the torrent's CA as well as one
    /// under the system roots.
    pub fn http(&self, builder: reqwest::ClientBuilder) -> anyhow::Result<reqwest::ClientBuilder> {
        let tls = self.tls.load();
        let tls = tls.as_ref().context("the SSL torrent has no identity")?;
        Ok(builder
            .identity(reqwest::Identity::from_pem(&tls.identity.to_pem())?)
            .add_root_certificate(reqwest::Certificate::from_der(&self.ca)?))
    }
}

/// The peer, as the server of the TLS, presents a chain under the torrent's
/// CA; the name it is issued to is not ours to check.
#[derive(Debug)]
struct UnderCa {
    roots: Arc<RootCertStore>,
    algorithms: WebPkiSupportedAlgorithms,
}

impl ServerCertVerifier for UnderCa {
    fn verify_server_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        _server_name: &ServerName<'_>,
        _ocsp_response: &[u8],
        now: UnixTime,
    ) -> Result<ServerCertVerified, rustls::Error> {
        let cert = ParsedCertificate::try_from(end_entity)?;
        rustls::client::verify_server_cert_signed_by_trust_anchor(
            &cert,
            &self.roots,
            intermediates,
            now,
            self.algorithms.all,
        )?;
        Ok(ServerCertVerified::assertion())
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls12_signature(message, cert, dss, &self.algorithms)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        rustls::crypto::verify_tls13_signature(message, cert, dss, &self.algorithms)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.algorithms.supported_schemes()
    }
}

/// An incoming connection on the SSL listener, its TLS begun: the torrent
/// it names in the SNI says what it is accepted under.
pub(crate) struct Incoming(tokio_rustls::StartHandshake<Joined>);

impl Incoming {
    /// Reads the connection's ClientHello; a connection that does not open
    /// with one is refused here.
    pub async fn start(reader: BoxAsyncReadVectored, writer: BoxAsyncWrite) -> io::Result<Self> {
        tokio_rustls::LazyConfigAcceptor::new(
            rustls::server::Acceptor::default(),
            Joined { reader, writer },
        )
        .await
        .map(Self)
    }

    /// The torrent the connection names.
    pub fn server_name(&self) -> Option<String> {
        self.0.client_hello().server_name().map(str::to_owned)
    }

    /// Completes the TLS under `config`, the peer's chain checked against
    /// the torrent's CA.
    pub async fn accept(
        self,
        config: Arc<ServerConfig>,
    ) -> io::Result<(BoxAsyncReadVectored, BoxAsyncWrite)> {
        Ok(halves(self.0.into_stream(config).await?))
    }
}

fn halves<S>(stream: S) -> (BoxAsyncReadVectored, BoxAsyncWrite)
where
    S: AsyncRead + AsyncWrite + Unpin + Send + 'static,
{
    let (reader, writer) = tokio::io::split(stream);
    (
        Box::new(reader.into_vectored_compat()),
        Box::new(Sent {
            inner: writer,
            written: None,
        }),
    )
}

/// A TLS stream's writing half whose writes complete once their records
/// are on the stream under it, as a connection's writes are everywhere
/// else: TLS takes the bytes and may keep the records it made of them when
/// that stream is not ready, and nothing writes them out until asked.
struct Sent<W> {
    inner: W,
    // Bytes TLS took whose records are still being written out.
    written: Option<usize>,
}

impl<W: AsyncWrite + Unpin> AsyncWrite for Sent<W> {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        let this = self.get_mut();
        let written = match this.written {
            Some(written) => written,
            None => {
                let written = std::task::ready!(Pin::new(&mut this.inner).poll_write(cx, buf))?;
                this.written = Some(written);
                written
            }
        };
        let flushed = std::task::ready!(Pin::new(&mut this.inner).poll_flush(cx));
        this.written = None;
        flushed.map(|()| written).into()
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().inner).poll_shutdown(cx)
    }
}

/// The two halves of a connection as one stream, for TLS to run over.
pub(crate) struct Joined {
    reader: BoxAsyncReadVectored,
    writer: BoxAsyncWrite,
}

impl AsyncRead for Joined {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().reader).poll_read(cx, buf)
    }
}

impl AsyncWrite for Joined {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.get_mut().writer).poll_write(cx, buf)
    }

    fn poll_write_vectored(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        bufs: &[io::IoSlice<'_>],
    ) -> Poll<io::Result<usize>> {
        Pin::new(&mut self.get_mut().writer).poll_write_vectored(cx, bufs)
    }

    fn is_write_vectored(&self) -> bool {
        self.writer.is_write_vectored()
    }

    // A connection's stream sends what it is written without being flushed,
    // and a flush of some (uTP) waits until the peer has every byte: TLS
    // asks here only that its records were written.
    fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Poll::Ready(Ok(()))
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        Pin::new(&mut self.get_mut().writer).poll_shutdown(cx)
    }
}

/// The HTTP clients of an SSL torrent's announces: the session's, with the
/// torrent's TLS identity and CA added, read as each client is built.
pub(crate) struct SslHttpClients {
    pub inner: Arc<dyn crate::HttpClients>,
    pub ssl: Arc<SslTorrent>,
}

impl crate::HttpClients for SslHttpClients {
    fn builder(
        &self,
        target: Option<crate::HttpTarget<'_>>,
    ) -> anyhow::Result<reqwest::ClientBuilder> {
        self.ssl.http(self.inner.builder(target)?)
    }
}
