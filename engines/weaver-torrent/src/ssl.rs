// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

//! SSL torrents. A torrent whose info dict carries `ssl-cert` names the CA
//! every one of its peers is issued under. Each of its peer connections is
//! TLS over the stream that connected, both ends presenting a chain under
//! that CA and checking the other's against it alone, and taking only a
//! certificate issued to the torrent's name or to `*`; the client names the
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
    self, CertificateError, ClientConfig, DigitallySignedStruct, DistinguishedName, RootCertStore,
    ServerConfig, SignatureScheme,
    client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier},
    crypto::{CryptoProvider, WebPkiSupportedAlgorithms},
    server::{
        ParsedCertificate, WebPkiClientVerifier,
        danger::{ClientCertVerified, ClientCertVerifier},
    },
    sign::CertifiedKey,
};
use x509_parser::oid_registry::OID_X509_EXT_SUBJECT_ALT_NAME;

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
    // The torrent's name, which a peer's certificate is issued to.
    name: Arc<str>,
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
    /// The SSL torrent whose `ssl-cert` is `ca_pem`, known by `info_hashes`
    /// and named `name` (as libtorrent names it: see
    /// [`crate::core::v2_tree::torrent_name`]).
    pub fn new(ca_pem: &[u8], info_hashes: InfoHashes, name: &str) -> anyhow::Result<Self> {
        let ca = CertificateDer::from_pem_slice(ca_pem).context("the torrent's CA is not PEM")?;
        let mut roots = RootCertStore::empty();
        roots
            .add(ca.clone())
            .context("the torrent's CA is not a certificate")?;
        let hex = |id: Id20| id.as_string();
        let sni = ServerName::try_from(hex(info_hashes.best()))
            .context("the info hash is not a server name")?;
        Ok(Self {
            ca,
            roots: Arc::new(roots),
            name: name.into(),
            sni,
            names: info_hashes.ids20().map(hex).collect(),
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
            name: self.name.clone(),
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
            .with_client_cert_verifier(Arc::new(NamedClients {
                under_ca: clients,
                name: self.name.clone(),
            }))
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

    /// What the torrent's HTTPS announces present and trust: our identity,
    /// and the torrent's CA, so a tracker under it is taken as well as one
    /// under the system roots.
    pub fn client_tls(&self) -> anyhow::Result<crate::ClientTls> {
        let tls = self.tls.load();
        let tls = tls.as_ref().context("the SSL torrent has no identity")?;
        Ok(crate::ClientTls {
            identity: tls.identity.to_pem().into(),
            ca: self.ca.to_vec().into(),
        })
    }
}

/// Whether a peer's certificate is issued to the torrent named `name`, as
/// libtorrent checks one (`torrent::verify_peer_cert`): a DNS name in its
/// subjectAltName, or failing that the last common name of its subject,
/// that is the torrent's name or `*`, byte for byte.
fn names_torrent(cert: &CertificateDer<'_>, name: &str) -> bool {
    let Ok((_, cert)) = x509_parser::parse_x509_certificate(cert) else {
        return false;
    };
    let named = |given: &[u8]| !given.is_empty() && (given == b"*" || given == name.as_bytes());
    // Given twice, the extension is read as absent, as OpenSSL reads it.
    let alt_names = cert
        .tbs_certificate
        .get_extension_unique(&OID_X509_EXT_SUBJECT_ALT_NAME)
        .ok()
        .flatten()
        .map(|ext| dns_names(ext.value))
        .unwrap_or_default();
    alt_names.into_iter().any(named)
        || cert
            .subject()
            .iter_common_name()
            .last()
            .is_some_and(|cn| named(cn.as_slice()))
}

/// The dNSName entries of a subjectAltName's GeneralNames, as their bytes:
/// none when it does not decode, as OpenSSL reads none from it then.
fn dns_names(der: &[u8]) -> Vec<&[u8]> {
    // A DER element: its tag, its contents, and what follows it.
    fn element(der: &[u8]) -> Option<(u8, &[u8], &[u8])> {
        let (&tag, rest) = der.split_first()?;
        let (&first, rest) = rest.split_first()?;
        let (len, rest) = if first < 0x80 {
            (usize::from(first), rest)
        } else {
            let n = usize::from(first & 0x7f);
            if n == 0 || n > size_of::<usize>() || rest.len() < n {
                return None;
            }
            let (len, rest) = rest.split_at(n);
            let len = len.iter().fold(0, |len, &b| len << 8 | usize::from(b));
            (len, rest)
        };
        (rest.len() >= len).then(|| {
            let (contents, rest) = rest.split_at(len);
            (tag, contents, rest)
        })
    }
    const SEQUENCE: u8 = 0x30;
    // [2] IMPLICIT IA5String.
    const DNS_NAME: u8 = 0x82;
    // The tags of a GeneralName's choices, [0] to [8].
    const GENERAL_NAMES: [u8; 9] = [0xa0, 0x81, 0x82, 0xa3, 0xa4, 0xa5, 0x86, 0x87, 0x88];
    let Some((SEQUENCE, mut names, [])) = element(der) else {
        return Vec::new();
    };
    let mut dns = Vec::new();
    while !names.is_empty() {
        match element(names) {
            Some((tag, contents, rest)) if GENERAL_NAMES.contains(&tag) => {
                if tag == DNS_NAME {
                    dns.push(contents);
                }
                names = rest;
            }
            _ => return Vec::new(),
        }
    }
    dns
}

fn not_the_torrents() -> rustls::Error {
    rustls::Error::InvalidCertificate(CertificateError::NotValidForName)
}

/// The peer, as the server of the TLS, presents a chain under the torrent's
/// CA, issued to the torrent (see [`names_torrent`]).
#[derive(Debug)]
struct UnderCa {
    roots: Arc<RootCertStore>,
    algorithms: WebPkiSupportedAlgorithms,
    name: Arc<str>,
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
        if !names_torrent(end_entity, &self.name) {
            return Err(not_the_torrents());
        }
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

/// The peer, as the client of the TLS, presents a chain `under_ca` checks,
/// issued to the torrent (see [`names_torrent`]).
#[derive(Debug)]
struct NamedClients {
    under_ca: Arc<dyn ClientCertVerifier>,
    name: Arc<str>,
}

impl ClientCertVerifier for NamedClients {
    fn offer_client_auth(&self) -> bool {
        self.under_ca.offer_client_auth()
    }

    fn client_auth_mandatory(&self) -> bool {
        self.under_ca.client_auth_mandatory()
    }

    fn root_hint_subjects(&self) -> &[DistinguishedName] {
        self.under_ca.root_hint_subjects()
    }

    fn verify_client_cert(
        &self,
        end_entity: &CertificateDer<'_>,
        intermediates: &[CertificateDer<'_>],
        now: UnixTime,
    ) -> Result<ClientCertVerified, rustls::Error> {
        let verified = self
            .under_ca
            .verify_client_cert(end_entity, intermediates, now)?;
        if !names_torrent(end_entity, &self.name) {
            return Err(not_the_torrents());
        }
        Ok(verified)
    }

    fn verify_tls12_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        self.under_ca.verify_tls12_signature(message, cert, dss)
    }

    fn verify_tls13_signature(
        &self,
        message: &[u8],
        cert: &CertificateDer<'_>,
        dss: &DigitallySignedStruct,
    ) -> Result<HandshakeSignatureValid, rustls::Error> {
        self.under_ca.verify_tls13_signature(message, cert, dss)
    }

    fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
        self.under_ca.supported_verify_schemes()
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

/// The HTTP clients of an SSL torrent's announces: the session's, in the
/// shape that presents the torrent's TLS identity and trusts its CA, read
/// as each client is asked for.
pub(crate) struct SslHttpClients {
    pub inner: Arc<dyn crate::HttpClients>,
    pub ssl: Arc<SslTorrent>,
}

impl crate::HttpClients for SslHttpClients {
    fn client(
        &self,
        target: Option<crate::HttpTarget<'_>>,
        shape: &crate::ClientShape,
    ) -> anyhow::Result<reqwest::Client> {
        let shape = crate::ClientShape {
            tls: Some(self.ssl.client_tls()?),
            ..shape.clone()
        };
        self.inner.client(target, &shape)
    }

    fn egress(&self, to: std::net::SocketAddr) -> crate::Egress {
        self.inner.egress(to)
    }
}

#[cfg(test)]
mod tests {
    use rcgen::{CertificateParams, DistinguishedName, DnType, KeyPair, SanType};

    use super::*;

    const NAME: &str = "ptr-ssl-64MiB.bin";

    /// A certificate with `alt_names` as the DNS names of its
    /// subjectAltName and `common_names` (at most two) in its subject.
    fn cert(alt_names: &[&str], common_names: &[&str]) -> CertificateDer<'static> {
        let mut params = CertificateParams::default();
        params.subject_alt_names = alt_names
            .iter()
            .map(|name| SanType::DnsName((*name).try_into().unwrap()))
            .collect();
        params.distinguished_name = DistinguishedName::new();
        for (i, cn) in common_names.iter().enumerate() {
            // rcgen keeps one entry per type: the second common name goes
            // in under its OID.
            let ty = match i {
                0 => DnType::CommonName,
                _ => DnType::CustomDnType(vec![2, 5, 4, 3]),
            };
            params.distinguished_name.push(ty, *cn);
        }
        let key = KeyPair::generate().unwrap();
        params.self_signed(&key).unwrap().der().clone()
    }

    #[test]
    fn a_certificate_names_the_torrent_as_libtorrent_reads_it() {
        let named = |alt: &[&str], cn: &[&str]| names_torrent(&cert(alt, cn), NAME);
        // A DNS name in the subjectAltName: the torrent's, or `*`.
        assert!(named(&[NAME], &[]));
        assert!(named(&["another", NAME], &["peer"]));
        assert!(named(&["*"], &[]));
        // Byte for byte: no case folding, no wildcard matching.
        assert!(!named(&["PTR-SSL-64MIB.BIN"], &[]));
        assert!(!named(&["*.bin"], &[]));
        // None of those: the last common name decides, even beside a
        // subjectAltName naming another torrent.
        assert!(named(&[], &[NAME]));
        assert!(named(&["another"], &[NAME]));
        assert!(named(&[], &["*"]));
        assert!(named(&[], &["peer", NAME]));
        assert!(!named(&[], &[NAME, "peer"]));
        assert!(!named(&["another"], &["peer"]));
        assert!(!named(&[], &[]));
    }

    #[test]
    fn dns_names_read_as_openssl_decodes_them() {
        // [1] rfc822Name "a@b", then [2] dNSName "x".
        let names = [0x30, 0x08, 0x81, 0x03, b'a', b'@', b'b', 0x82, 0x01, b'x'];
        assert_eq!(dns_names(&names), [&b"x"[..]]);
        // A long-form length.
        assert_eq!(
            dns_names(&[0x30, 0x81, 0x03, 0x82, 0x01, b'x']),
            [&b"x"[..]]
        );
        // A directoryName, constructed, skipped whole.
        let names = [0x30, 0x07, 0xa4, 0x02, 0x30, 0x00, 0x82, 0x01, b'x'];
        assert_eq!(dns_names(&names), [&b"x"[..]]);
        // What does not decode gives none at all: a choice GeneralName has
        // not, a short element, bytes after the sequence, not a sequence.
        for bad in [
            &[0x30, 0x06, 0x89, 0x01, 0x00, 0x82, 0x01, b'x'][..],
            &[0x30, 0x05, 0x82, 0x01, b'x'],
            &[0x30, 0x03, 0x82, 0x01, b'x', 0x00],
            &[0x31, 0x03, 0x82, 0x01, b'x'],
            &[0x30, 0x80, 0x82, 0x01, b'x', 0x00, 0x00],
        ] {
            assert!(dns_names(bad).is_empty(), "{bad:x?}");
        }
    }
}
