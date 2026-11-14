// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The connection one server's queries ride during one lookup. Plain DNS
// opens a stream per query. DNS over TLS (RFC 7858) and over HTTPS (RFC
// 8484) wrap the stream the stage dials in TLS, and keep that one
// connection for the lookup's queries.
use super::{Carrier, DnsServer, Failed, Wire};
use crate::TunnelStream;
use std::sync::{Arc, LazyLock};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio_rustls::{
    TlsConnector,
    client::TlsStream,
    rustls::{ClientConfig, RootCertStore, crypto::aws_lc_rs, pki_types::ServerName},
};

// The largest reply taken on any wire.
pub(super) const REPLY_CAP: usize = 16384;

// The TLS settings of each encrypted wire, built once and shared by every
// lookup. Sessions resume across lookups.
pub(super) struct Wires {
    dot: Arc<ClientConfig>,
    doh: Arc<ClientConfig>,
}

impl Wires {
    pub(super) fn trusting(roots: RootCertStore) -> Self {
        let roots = Arc::new(roots);
        let config = |alpn: &[u8]| {
            let mut config =
                ClientConfig::builder_with_provider(Arc::new(aws_lc_rs::default_provider()))
                    .with_safe_default_protocol_versions()
                    .expect("aws-lc-rs supports Rustls safe default protocol versions")
                    .with_root_certificates(roots.clone())
                    .with_no_client_auth();
            config.alpn_protocols = vec![alpn.to_vec()];
            Arc::new(config)
        };
        Self {
            dot: config(b"dot"),
            // Only HTTP/1.1 is spoken over HTTPS.
            doh: config(b"http/1.1"),
        }
    }
}

// The web PKI roots: the only trust a product build has.
pub(super) static WIRES: LazyLock<Wires> = LazyLock::new(|| {
    let mut roots = RootCertStore::empty();
    roots.extend(webpki_roots::TLS_SERVER_ROOTS.iter().cloned());
    Wires::trusting(roots)
});

type Tls = TlsStream<Box<dyn TunnelStream>>;

enum Link {
    Tls(Box<Tls>),
    Https(Box<super::https::Connection<Tls>>),
}

// Connections opened to one server in one lookup: the first, and one more
// when the server closes it between queries.
const OPENS: u8 = 2;

// One server, asked for one lookup.
pub(super) struct Session<'a> {
    provider: &'a dyn Carrier,
    wires: &'a Wires,
    server: &'a DnsServer,
    link: Option<Link>,
    opened: u8,
}

impl<'a> Session<'a> {
    pub(super) fn new(provider: &'a dyn Carrier, wires: &'a Wires, server: &'a DnsServer) -> Self {
        Self {
            provider,
            wires,
            server,
            link: None,
            opened: 0,
        }
    }

    pub(super) fn server(&self) -> &DnsServer {
        self.server
    }

    // Sends `packet` and returns the reply. `reached` is set once the
    // server answers the dial, or a kept connection is used.
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
        loop {
            // The link is out of its slot while in use, so a query cut off
            // part way leaves no half-used connection behind.
            let (mut link, kept) = match self.link.take() {
                Some(link) => {
                    *reached = true;
                    (link, true)
                }
                None => (self.connect(reached).await?, false),
            };
            match link.exchange(self.server, packet).await {
                Ok((reply, open)) => {
                    if open {
                        self.link = Some(link);
                    }
                    return Ok(reply);
                }
                Err(Failed::Closed) if kept && self.opened < OPENS => {}
                Err(failed) => return Err(failed),
            }
        }
    }

    async fn connect(&mut self, reached: &mut bool) -> Result<Link, Failed> {
        if self.opened >= OPENS {
            return Err(Failed::Closed);
        }
        let (config, name) = match &self.server.wire {
            Wire::Tls { name } => (&self.wires.dot, name.as_deref()),
            Wire::Https { .. } => (&self.wires.doh, None),
            Wire::Plain => unreachable!("plain DNS keeps no connection"),
        };
        // The certificate must name the server's address, or the name given.
        let verified = match name {
            Some(name) => ServerName::try_from(name.to_owned())
                .map_err(|error| Failed::Tls(format!("{name} is not a server name: {error}")))?,
            None => ServerName::IpAddress(self.server.address.ip().into()),
        };
        self.opened += 1;
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
            .map_err(|error| Failed::Tls(error.to_string()))?;
        if let Wire::Tls { .. } = self.server.wire {
            return Ok(Link::Tls(Box::new(tls)));
        }
        Ok(Link::Https(Box::new(super::https::Connection::new(tls))))
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
    let size = stream.read_u16().await.map_err(|_| Failed::Closed)? as usize;
    if !(12..=REPLY_CAP).contains(&size) {
        return Err(Failed::Invalid);
    }
    let mut bytes = vec![0; size];
    stream
        .read_exact(&mut bytes)
        .await
        .map_err(|_| Failed::Closed)?;
    Ok(bytes)
}
