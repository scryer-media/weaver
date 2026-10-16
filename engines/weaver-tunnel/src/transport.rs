// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// TCP proxy adapters. Only the proxy endpoint is resolved on the host.
use crate::{TunnelError, TunnelProvider, TunnelStream, fault::authority};
use base64::Engine;
use std::{
    fmt,
    net::{IpAddr, SocketAddr},
};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};

#[derive(Clone, Copy, Debug)]
pub enum TransportKind {
    HttpConnect,
    Socks5,
}

impl TransportKind {
    pub fn tunnel_kind(self) -> crate::metrics::TunnelKind {
        match self {
            Self::HttpConnect => crate::metrics::TunnelKind::HttpConnect,
            Self::Socks5 => crate::metrics::TunnelKind::Socks5,
        }
    }
    fn protocol(self) -> &'static str {
        match self {
            Self::Socks5 => "SOCKS",
            Self::HttpConnect => "HTTP",
        }
    }
}

#[derive(Clone)]
pub struct TransportProxy {
    pub kind: TransportKind,
    pub host: String,
    pub port: u16,
    pub username: Option<String>,
    pub password: Option<String>,
}

// What these adapters say when a proxy fails them. The tunnel error type
// carries a stream proxy's failure as engine text, so each sentence is
// written here only, and `fault` reads it back by the same words.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Said {
    SocksClosed,
    NotSocks,
    SocksAuthRequired,
    SocksMethodRejected,
    SocksCredentialLength,
    SocksAuthClosed,
    SocksCredentialsRejected,
    SocksConnectClosed,
    SocksReplyInvalid,
    InvalidDestination,
    InvalidConnectDestination,
    HttpConnectClosed,
    HttpHeadersTooLarge,
    HttpResponseInvalid,
    HttpAuthRequired,
    HttpCredentialsRejected,
    SocketSetup,
}

impl Said {
    const ALL: [Self; 17] = [
        Self::SocksClosed,
        Self::NotSocks,
        Self::SocksAuthRequired,
        Self::SocksMethodRejected,
        Self::SocksCredentialLength,
        Self::SocksAuthClosed,
        Self::SocksCredentialsRejected,
        Self::SocksConnectClosed,
        Self::SocksReplyInvalid,
        Self::InvalidDestination,
        Self::InvalidConnectDestination,
        Self::HttpConnectClosed,
        Self::HttpHeadersTooLarge,
        Self::HttpResponseInvalid,
        Self::HttpAuthRequired,
        Self::HttpCredentialsRejected,
        Self::SocketSetup,
    ];
    pub(crate) fn text(self) -> &'static str {
        match self {
            Self::SocksClosed => "SOCKS negotiation failed",
            Self::NotSocks => "the server did not answer as a SOCKS5 proxy",
            Self::SocksAuthRequired => "SOCKS proxy requires authentication",
            Self::SocksMethodRejected => "SOCKS authentication method rejected",
            Self::SocksCredentialLength => "invalid SOCKS credential length",
            Self::SocksAuthClosed => "SOCKS authentication failed",
            Self::SocksCredentialsRejected => "SOCKS credentials rejected",
            Self::SocksConnectClosed => "SOCKS CONNECT failed",
            Self::SocksReplyInvalid => "invalid SOCKS reply",
            Self::InvalidDestination => "invalid proxy destination",
            Self::InvalidConnectDestination => "invalid CONNECT destination",
            Self::HttpConnectClosed => "HTTP proxy CONNECT failed",
            Self::HttpHeadersTooLarge => "HTTP proxy response headers too large",
            Self::HttpResponseInvalid => "invalid HTTP proxy response",
            Self::HttpAuthRequired => "HTTP proxy requires authentication",
            Self::HttpCredentialsRejected => "HTTP proxy rejected the credentials",
            Self::SocketSetup => "proxy socket setup failed",
        }
    }
    // Which of these `text` says, wherever in it, however it was wrapped.
    pub(crate) fn find(text: &str) -> Option<Self> {
        Self::ALL
            .into_iter()
            .find(|said| text.contains(said.text()))
    }
}

fn failure(said: Said) -> TunnelError {
    TunnelError::Engine(said.text().into())
}

// What a proxy answered when it did not open a stream to a destination.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Reply {
    Socks(u8),
    Http(u16),
    // Nothing, before the hop's time ran out.
    Silent,
}

impl fmt::Display for Reply {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Socks(code) => write!(f, "{SOCKS_REPLY}{code}"),
            Self::Http(status) => write!(f, "{HTTP_REPLY}{status}"),
            Self::Silent => f.write_str(SILENT),
        }
    }
}

const SOCKS_REPLY: &str = "SOCKS reply ";
const HTTP_REPLY: &str = "HTTP ";
const SILENT: &str = "no answer";

impl Reply {
    pub(crate) fn read(text: &str) -> Option<Self> {
        if text == SILENT {
            return Some(Self::Silent);
        }
        if let Some(code) = text.strip_prefix(SOCKS_REPLY) {
            return code.parse().ok().map(Self::Socks);
        }
        text.strip_prefix(HTTP_REPLY)?.parse().ok().map(Self::Http)
    }
}

// Why a proxy that was asked for a destination did not open a stream to it.
#[derive(Debug)]
pub(crate) enum ConnectFailure {
    // The proxy failed, refused, or answered out of turn.
    Proxy(TunnelError),
    // The proxy answered that it could not reach the destination.
    Unreachable(Reply),
}
impl From<TunnelError> for ConnectFailure {
    fn from(error: TunnelError) -> Self {
        Self::Proxy(error)
    }
}

// The words every proxy's own refusal of a destination carries, and the
// prefix a hop's failure to reach the next hop's endpoint carries.
pub(crate) const UNREACHABLE: &str = " proxy could not connect to ";
pub(crate) const HOP_BEFORE: &str = "the hop before it could not reach ";
pub(crate) const REJECTED: &str = "HTTP proxy rejected CONNECT to ";
pub(crate) const REJECTED_STATUS: &str = " with HTTP ";

fn destination_unreachable(protocol: &str, host: &str, port: u16, reply: Reply) -> TunnelError {
    TunnelError::Engine(format!(
        "{protocol}{UNREACHABLE}{}: {reply}",
        authority(host, port)
    ))
}

// What the hop before a proxy said when it could not reach that proxy's
// endpoint, charged to that proxy.
pub(crate) fn hop_before(host: &str, port: u16, reply: Reply) -> TunnelError {
    TunnelError::Engine(format!("{HOP_BEFORE}{}: {reply}", authority(host, port)))
}

// Whether `error` is a proxy's answer that it could not reach the
// destination it was asked for.
pub(crate) fn is_destination_unreachable(error: &TunnelError) -> bool {
    matches!(error, TunnelError::Engine(detail) if detail.contains(UNREACHABLE))
}

// Establish a SOCKS5 CONNECT without resolving the destination locally.
pub async fn socks_connect<S: AsyncRead + AsyncWrite + Unpin + ?Sized>(
    stream: &mut S,
    host: &str,
    port: u16,
    credentials: Option<(&str, &str)>,
) -> Result<(), TunnelError> {
    socks_greet(stream, credentials).await?;
    socks_open(stream, host, port)
        .await
        .map_err(|failure| match failure {
            ConnectFailure::Proxy(error) => error,
            ConnectFailure::Unreachable(reply) => {
                destination_unreachable("SOCKS", host, port, reply)
            }
        })
}

// Agree a method with a SOCKS5 proxy and authenticate when it has credentials.
async fn socks_greet<S: AsyncRead + AsyncWrite + Unpin + ?Sized>(
    stream: &mut S,
    credentials: Option<(&str, &str)>,
) -> Result<(), TunnelError> {
    let method = if credentials.is_some() { 2 } else { 0 };
    stream
        .write_all(&[5, 1, method])
        .await
        .map_err(|_| failure(Said::SocksClosed))?;
    let mut reply = [0; 2];
    stream
        .read_exact(&mut reply)
        .await
        .map_err(|_| failure(Said::SocksClosed))?;
    if reply[0] != 5 {
        return Err(failure(Said::NotSocks));
    }
    if reply[1] != method {
        return Err(failure(if credentials.is_some() {
            Said::SocksMethodRejected
        } else {
            Said::SocksAuthRequired
        }));
    }
    if let Some((user, pass)) = credentials {
        if user.is_empty() || user.len() > 255 || pass.len() > 255 {
            return Err(failure(Said::SocksCredentialLength));
        }
        let mut auth = vec![1, user.len() as u8];
        auth.extend_from_slice(user.as_bytes());
        auth.push(pass.len() as u8);
        auth.extend_from_slice(pass.as_bytes());
        stream
            .write_all(&auth)
            .await
            .map_err(|_| failure(Said::SocksAuthClosed))?;
        stream
            .read_exact(&mut reply)
            .await
            .map_err(|_| failure(Said::SocksAuthClosed))?;
        if reply != [1, 0] {
            return Err(failure(Said::SocksCredentialsRejected));
        }
    }
    Ok(())
}

// Ask a greeted SOCKS5 proxy for the destination.
async fn socks_open<S: AsyncRead + AsyncWrite + Unpin + ?Sized>(
    stream: &mut S,
    host: &str,
    port: u16,
) -> Result<(), ConnectFailure> {
    let request = socks_request(host, port)?;
    stream
        .write_all(&request)
        .await
        .map_err(|_| failure(Said::SocksConnectClosed))?;
    let mut header = [0; 4];
    stream
        .read_exact(&mut header)
        .await
        .map_err(|_| failure(Said::SocksConnectClosed))?;
    match (header[0], header[1], header[2]) {
        (5, 0, 0) => {}
        // These replies report what became of the proxy's own connection
        // attempt. Every other one is the proxy declining or failing.
        (5, code @ 3..=6, _) => return Err(ConnectFailure::Unreachable(Reply::Socks(code))),
        (5, code @ 1.., _) => {
            return Err(destination_unreachable("SOCKS", host, port, Reply::Socks(code)).into());
        }
        _ => return Err(failure(Said::SocksReplyInvalid).into()),
    }
    let len = match header[3] {
        1 => 4,
        4 => 16,
        3 => stream
            .read_u8()
            .await
            .map_err(|_| failure(Said::SocksReplyInvalid))? as usize,
        _ => return Err(failure(Said::SocksReplyInvalid).into()),
    };
    let mut tail = vec![0; len + 2];
    stream
        .read_exact(&mut tail)
        .await
        .map_err(|_| failure(Said::SocksReplyInvalid))?;
    Ok(())
}

pub fn socks_request(host: &str, port: u16) -> Result<Vec<u8>, TunnelError> {
    let mut request = vec![5, 1, 0];
    match host.parse::<IpAddr>() {
        Ok(IpAddr::V4(ip)) => {
            request.push(1);
            request.extend_from_slice(&ip.octets());
        }
        Ok(IpAddr::V6(ip)) => {
            request.push(4);
            request.extend_from_slice(&ip.octets());
        }
        Err(_) => {
            if host.is_empty() || host.len() > 255 || host.bytes().any(|b| b.is_ascii_control()) {
                return Err(failure(Said::InvalidDestination));
            }
            request.extend_from_slice(&[3, host.len() as u8]);
            request.extend_from_slice(host.as_bytes());
        }
    }
    request.extend_from_slice(&port.to_be_bytes());
    Ok(request)
}

impl TransportProxy {
    // Negotiate over the inner stage's stream, preserving its egress binding.
    pub async fn negotiate<S: AsyncRead + AsyncWrite + Unpin + ?Sized>(
        &self,
        stream: &mut S,
        host: &str,
        port: u16,
    ) -> Result<(), TunnelError> {
        self.greet(stream).await?;
        self.connect(stream, host, port)
            .await
            .map_err(|failure| match failure {
                ConnectFailure::Proxy(error) => error,
                ConnectFailure::Unreachable(reply) => {
                    self.destination_unreachable(host, port, reply)
                }
            })
    }

    // The proxy's own failure to reach a destination it was asked for.
    pub(crate) fn destination_unreachable(
        &self,
        host: &str,
        port: u16,
        reply: Reply,
    ) -> TunnelError {
        destination_unreachable(self.kind.protocol(), host, port, reply)
    }

    // Everything the proxy asks of a client before it takes a destination.
    pub(crate) async fn greet<S: AsyncRead + AsyncWrite + Unpin + ?Sized>(
        &self,
        stream: &mut S,
    ) -> Result<(), TunnelError> {
        match self.kind {
            TransportKind::Socks5 => {
                socks_greet(
                    stream,
                    self.username
                        .as_deref()
                        .map(|u| (u, self.password.as_deref().unwrap_or(""))),
                )
                .await
            }
            TransportKind::HttpConnect => Ok(()),
        }
    }

    // Ask a greeted proxy for the destination, telling apart the proxy
    // failing from the proxy reporting that the destination did not answer.
    pub(crate) async fn connect<S: AsyncRead + AsyncWrite + Unpin + ?Sized>(
        &self,
        stream: &mut S,
        host: &str,
        port: u16,
    ) -> Result<(), ConnectFailure> {
        match self.kind {
            TransportKind::Socks5 => socks_open(stream, host, port).await,
            TransportKind::HttpConnect => {
                if host.is_empty()
                    || host
                        .bytes()
                        .any(|b| b.is_ascii_whitespace() || b.is_ascii_control())
                {
                    return Err(failure(Said::InvalidConnectDestination).into());
                }
                let authority = match host.parse::<IpAddr>() {
                    Ok(ip) => SocketAddr::new(ip, port).to_string(),
                    Err(_) => format!("{host}:{port}"),
                };
                let mut request = format!("CONNECT {authority} HTTP/1.1\r\nHost: {authority}\r\n");
                if let Some(user) = &self.username {
                    let encoded = base64::prelude::BASE64_STANDARD
                        .encode(format!("{user}:{}", self.password.as_deref().unwrap_or("")));
                    request.push_str(&format!("Proxy-Authorization: Basic {encoded}\r\n"));
                }
                request.push_str("\r\n");
                stream
                    .write_all(request.as_bytes())
                    .await
                    .map_err(|_| failure(Said::HttpConnectClosed))?;
                let mut header = Vec::new();
                while !header.ends_with(b"\r\n\r\n") {
                    if header.len() >= 16384 {
                        return Err(failure(Said::HttpHeadersTooLarge).into());
                    }
                    header.push(
                        stream
                            .read_u8()
                            .await
                            .map_err(|_| failure(Said::HttpConnectClosed))?,
                    );
                }
                let line = std::str::from_utf8(&header)
                    .map_err(|_| failure(Said::HttpResponseInvalid))?
                    .lines()
                    .next()
                    .unwrap_or("");
                let mut fields = line.split_whitespace();
                let status = matches!(fields.next(), Some("HTTP/1.0" | "HTTP/1.1"))
                    .then(|| fields.next().and_then(|v| v.parse::<u16>().ok()))
                    .flatten();
                match status {
                    Some(200..=299) => Ok(()),
                    // A gateway status reports what became of the proxy's own
                    // connection attempt. Any other is the proxy declining.
                    Some(status @ 502..=504) => {
                        Err(ConnectFailure::Unreachable(Reply::Http(status)))
                    }
                    Some(407) => Err(failure(if self.username.is_some() {
                        Said::HttpCredentialsRejected
                    } else {
                        Said::HttpAuthRequired
                    })
                    .into()),
                    Some(status) => Err(TunnelError::Engine(format!(
                        "{REJECTED}{}{REJECTED_STATUS}{status}",
                        crate::fault::authority(host, port)
                    ))
                    .into()),
                    None => Err(failure(Said::HttpResponseInvalid).into()),
                }
            }
        }
    }
}

#[async_trait::async_trait]
impl TunnelProvider for TransportProxy {
    fn kind(&self) -> Option<crate::metrics::TunnelKind> {
        Some(self.kind.tunnel_kind())
    }
    async fn dial(&self, host: &str, port: u16) -> Result<Box<dyn TunnelStream>, TunnelError> {
        #[expect(
            clippy::disallowed_methods,
            reason = "a proxy hop dials its proxy server"
        )]
        let mut stream = crate::resolution::connect_host(&self.host, self.port)
            .await
            // The system's own words name what went wrong, and `fault`
            // reads them.
            .map_err(|error| {
                TunnelError::Engine(format!("proxy endpoint is unreachable: {error}"))
            })?;
        stream
            .set_nodelay(true)
            .map_err(|_| failure(Said::SocketSetup))?;
        self.negotiate(&mut stream, host, port).await?;
        Ok(Box::new(stream))
    }
    fn describe(&self) -> String {
        format!("{:?} {}:{}", self.kind, self.host, self.port)
    }
}

#[cfg(test)]
#[path = "transport_tests.rs"]
mod tests;
