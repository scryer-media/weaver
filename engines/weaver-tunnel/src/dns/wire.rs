// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The connection one server's queries ride during one lookup. Plain DNS
// opens a stream per query. DNS over TLS (RFC 7858) and over HTTPS (RFC
// 8484) wrap the stream the stage dials in TLS, and keep that one
// connection for the lookup's queries while the server keeps it open.
use super::{Carrier, DnsServer, Failed, Wire};
use crate::TunnelStream;
use std::sync::{Arc, LazyLock, OnceLock};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio_rustls::{
    TlsConnector,
    client::TlsStream,
    rustls::{
        CertificateError, ClientConfig, RootCertStore, client::Resumption, crypto::aws_lc_rs,
        pki_types::ServerName,
    },
};

// The largest reply taken on any wire.
pub(super) const REPLY_CAP: usize = 16384;

// TLS sessions one naming keeps for resumption.
const SESSIONS: usize = 32;

// The TLS settings of each encrypted wire.
pub(super) struct Wires {
    dot: Arc<ClientConfig>,
    doh: Arc<ClientConfig>,
}

impl Wires {
    pub(super) fn trusting(roots: RootCertStore) -> Self {
        let roots = Arc::new(roots);
        let config = |alpn: &[&[u8]]| {
            let mut config =
                ClientConfig::builder_with_provider(Arc::new(aws_lc_rs::default_provider()))
                    .with_safe_default_protocol_versions()
                    .expect("aws-lc-rs supports Rustls safe default protocol versions")
                    .with_root_certificates(roots.clone())
                    .with_no_client_auth();
            config.alpn_protocols = alpn.iter().map(|protocol| protocol.to_vec()).collect();
            Arc::new(config)
        };
        Self {
            dot: config(&[b"dot"]),
            // HTTP/2 when the server takes it, else HTTP/1.1.
            doh: config(&[b"h2", b"http/1.1"]),
        }
    }

    // The same settings with a session store of their own. The provider,
    // roots and verifier stay shared; only resumption state is apart.
    pub(super) fn apart(&self) -> Self {
        let fresh = |config: &Arc<ClientConfig>| {
            let mut config = ClientConfig::clone(config);
            config.resumption = Resumption::in_memory_sessions(SESSIONS);
            Arc::new(config)
        };
        Self {
            dot: fresh(&self.dot),
            doh: fresh(&self.doh),
        }
    }
}

// The web PKI roots: the only trust a product build has. Built once; every
// naming takes its own copy through `DnsSessions`.
pub(super) static WIRES: LazyLock<Wires> = LazyLock::new(|| {
    let mut roots = RootCertStore::empty();
    roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    Wires::trusting(roots)
});

// The TLS sessions one naming resumes. A ticket from a server asked on one
// route is never offered to it on another, which would tie the two routes
// together in the clear ClientHello. Built on first use, kept with the
// naming's answers, and dropped with them when the hop is rebuilt.
#[derive(Default)]
pub struct DnsSessions(OnceLock<Wires>);

impl DnsSessions {
    pub(super) fn wires(&self) -> &Wires {
        self.0.get_or_init(|| WIRES.apart())
    }
}

pub(super) type Tls = TlsStream<Box<dyn TunnelStream>>;

enum Link {
    Tls(Box<Tls>),
    Https(Box<super::https::Connection>),
}

// One server, asked for one lookup.
pub(super) struct Session<'a> {
    provider: &'a dyn Carrier,
    wires: &'a Wires,
    server: &'a DnsServer,
    link: Option<Link>,
}

impl<'a> Session<'a> {
    pub(super) fn new(provider: &'a dyn Carrier, wires: &'a Wires, server: &'a DnsServer) -> Self {
        Self {
            provider,
            wires,
            server,
            link: None,
        }
    }

    pub(super) fn server(&self) -> &DnsServer {
        self.server
    }

    // Sends `packet` and returns the reply. `reached` is set once the
    // server answers the dial, or a kept connection is used.
    //
    // A query rides the kept connection when there is one, else a new one.
    // A kept connection found dead before any byte of the reply came is
    // replaced once. So one query opens at most two connections, and a
    // server that closes after every reply costs one per query, as plain
    // DNS does.
    pub(super) async fn ask(
        &mut self,
        packet: &[u8],
        reached: &mut bool,
    ) -> Result<Vec<u8>, Failed> {
        if self.server.wire == Wire::Plain {
            let mut stream = self
                .provider
                .open(self.server)
                .await
                .map_err(Failed::Unreached)?;
            *reached = true;
            return framed(&mut stream, packet).await;
        }
        // The link is out of its slot while in use, so a query cut off part
        // way leaves no half-used connection behind.
        if let Some(mut link) = self.link.take() {
            *reached = true;
            match link.exchange(self.server, packet).await {
                Ok(reply) => return Ok(self.keep(link, reply)),
                Err(Failed::Closed) => {}
                Err(failed) => return Err(failed),
            }
        }
        let mut link = self.connect(reached).await?;
        let reply = link.exchange(self.server, packet).await?;
        Ok(self.keep(link, reply))
    }

    fn keep(&mut self, link: Link, (reply, open): (Vec<u8>, bool)) -> Vec<u8> {
        if open {
            self.link = Some(link);
        }
        reply
    }

    async fn connect(&mut self, reached: &mut bool) -> Result<Link, Failed> {
        let (config, name) = match &self.server.wire {
            Wire::Tls { name } => (&self.wires.dot, name.as_deref()),
            Wire::Https { .. } => (&self.wires.doh, None),
            Wire::Plain => unreachable!("plain DNS keeps no connection"),
        };
        // The certificate must name the server's address, or the name given.
        let verified = match name {
            Some(name) => {
                ServerName::try_from(name.to_owned()).map_err(|_| Failed::Tls(NOT_A_SERVER_NAME))?
            }
            None => ServerName::IpAddress(self.server.address.ip().into()),
        };
        let stream = self
            .provider
            .open(self.server)
            .await
            .map_err(Failed::Unreached)?;
        *reached = true;
        // A failed handshake or certificate fails this server. It is never
        // asked in plain instead.
        let tls = TlsConnector::from(config.clone())
            .connect(verified, stream)
            .await
            .map_err(|error| Failed::Tls(reason(&error)))?;
        if let Wire::Tls { .. } = self.server.wire {
            return Ok(Link::Tls(Box::new(tls)));
        }
        Ok(Link::Https(Box::new(
            super::https::Connection::new(tls).await?,
        )))
    }
}

// Why a TLS connection failed, in a few fixed words. Nothing the peer sent,
// such as the names on its certificate, reaches the detail.
const NOT_A_SERVER_NAME: &str = "the configured name is not a server name";
const WRONG_NAME: &str = "certificate not valid for this server";
const UNKNOWN_ISSUER: &str = "unknown issuer";
const OUT_OF_DATE: &str = "certificate expired or not yet valid";
const OTHER_CERTIFICATE: &str = "other certificate error";
const HANDSHAKE: &str = "handshake failed";
const CLOSED: &str = "connection closed";

fn reason(error: &std::io::Error) -> &'static str {
    use tokio_rustls::rustls::Error;
    let Some(error) = error
        .get_ref()
        .and_then(|inner| inner.downcast_ref::<Error>())
    else {
        return CLOSED;
    };
    match error {
        Error::InvalidCertificate(
            CertificateError::NotValidForName | CertificateError::NotValidForNameContext { .. },
        ) => WRONG_NAME,
        Error::InvalidCertificate(CertificateError::UnknownIssuer) => UNKNOWN_ISSUER,
        Error::InvalidCertificate(
            CertificateError::Expired
            | CertificateError::ExpiredContext { .. }
            | CertificateError::NotValidYet
            | CertificateError::NotValidYetContext { .. },
        ) => OUT_OF_DATE,
        Error::InvalidCertificate(_) => OTHER_CERTIFICATE,
        _ => HANDSHAKE,
    }
}

impl Link {
    // The reply, and whether the connection is still open for the next query.
    async fn exchange(
        &mut self,
        server: &DnsServer,
        packet: &[u8],
    ) -> Result<(Vec<u8>, bool), Failed> {
        match self {
            Self::Tls(stream) => framed(stream.as_mut(), packet)
                .await
                .map(|reply| (reply, true)),
            Self::Https(connection) => {
                let Wire::Https { path } = &server.wire else {
                    unreachable!("an HTTPS link is kept only for an HTTPS server")
                };
                connection.exchange(server, path, packet).await
            }
        }
    }
}

// One query over a stream, with the two-byte length prefix of DNS over TCP.
// A stream that ends before the first byte of the reply is closed; one that
// ends part way cut the reply short.
async fn framed<S>(stream: &mut S, packet: &[u8]) -> Result<Vec<u8>, Failed>
where
    S: AsyncRead + AsyncWrite + Unpin + ?Sized,
{
    let mut framed = Vec::with_capacity(packet.len() + 2);
    framed.extend_from_slice(&(packet.len() as u16).to_be_bytes());
    framed.extend_from_slice(packet);
    stream
        .write_all(&framed)
        .await
        .map_err(|_| Failed::Closed)?;
    stream.flush().await.map_err(|_| Failed::Closed)?;
    let mut prefix = [0; 2];
    match stream.read(&mut prefix[..1]).await {
        Ok(1) => {}
        _ => return Err(Failed::Closed),
    }
    stream
        .read_exact(&mut prefix[1..])
        .await
        .map_err(|_| Failed::Cut)?;
    let size = u16::from_be_bytes(prefix) as usize;
    if !(12..=REPLY_CAP).contains(&size) {
        return Err(Failed::Invalid);
    }
    let mut bytes = vec![0; size];
    stream
        .read_exact(&mut bytes)
        .await
        .map_err(|_| Failed::Cut)?;
    Ok(bytes)
}
