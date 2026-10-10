// TCP proxy adapters. Only the proxy endpoint is resolved on the host.
use crate::{TunnelError, TunnelProvider, TunnelStream};
use base64::Engine;
use std::net::{IpAddr, SocketAddr};
use tokio::io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt};
use tokio::net::TcpStream;

#[derive(Clone, Copy, Debug)]
pub enum TransportKind {
    HttpConnect,
    Socks5,
}

#[derive(Clone)]
pub struct TransportProxy {
    pub kind: TransportKind,
    pub host: String,
    pub port: u16,
    pub username: Option<String>,
    pub password: Option<String>,
}

fn failure(message: &'static str) -> TunnelError {
    TunnelError::Engine(message.into())
}

// Why a proxy that was asked for a destination did not open a stream to it.
#[derive(Debug)]
pub(crate) enum ConnectFailure {
    // The proxy failed, refused, or answered out of turn.
    Proxy(TunnelError),
    // The proxy answered that it could not reach the destination.
    Unreachable(&'static str),
}
impl From<TunnelError> for ConnectFailure {
    fn from(error: TunnelError) -> Self {
        Self::Proxy(error)
    }
}

fn destination_unreachable(proxy: &str, reason: &str) -> TunnelError {
    TunnelError::Engine(format!(
        "{proxy} proxy could not connect to destination: {reason}"
    ))
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
            ConnectFailure::Unreachable(reason) => destination_unreachable("SOCKS", reason),
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
        .map_err(|_| failure("SOCKS negotiation failed"))?;
    let mut reply = [0; 2];
    stream
        .read_exact(&mut reply)
        .await
        .map_err(|_| failure("SOCKS negotiation failed"))?;
    if reply != [5, method] {
        return Err(failure("SOCKS authentication method rejected"));
    }
    if let Some((user, pass)) = credentials {
        if user.is_empty() || user.len() > 255 || pass.len() > 255 {
            return Err(failure("invalid SOCKS credential length"));
        }
        let mut auth = vec![1, user.len() as u8];
        auth.extend_from_slice(user.as_bytes());
        auth.push(pass.len() as u8);
        auth.extend_from_slice(pass.as_bytes());
        stream
            .write_all(&auth)
            .await
            .map_err(|_| failure("SOCKS authentication failed"))?;
        stream
            .read_exact(&mut reply)
            .await
            .map_err(|_| failure("SOCKS authentication failed"))?;
        if reply != [1, 0] {
            return Err(failure("SOCKS credentials rejected"));
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
        .map_err(|_| failure("SOCKS CONNECT failed"))?;
    let mut header = [0; 4];
    stream
        .read_exact(&mut header)
        .await
        .map_err(|_| failure("SOCKS CONNECT failed"))?;
    if header[..3] != [5, 0, 0] {
        // These replies report what became of the proxy's own connection
        // attempt. Every other one is the proxy declining or failing.
        return Err(match (header[0], header[1]) {
            (5, 3) => ConnectFailure::Unreachable("network unreachable"),
            (5, 4) => ConnectFailure::Unreachable("host unreachable"),
            (5, 5) => ConnectFailure::Unreachable("connection refused"),
            (5, 6) => ConnectFailure::Unreachable("TTL expired"),
            _ => failure("SOCKS proxy could not connect to destination").into(),
        });
    }
    let len = match header[3] {
        1 => 4,
        4 => 16,
        3 => stream
            .read_u8()
            .await
            .map_err(|_| failure("invalid SOCKS reply"))? as usize,
        _ => return Err(failure("invalid SOCKS reply").into()),
    };
    let mut tail = vec![0; len + 2];
    stream
        .read_exact(&mut tail)
        .await
        .map_err(|_| failure("invalid SOCKS reply"))?;
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
                return Err(failure("invalid proxy destination"));
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
                ConnectFailure::Unreachable(reason) => self.destination_unreachable(reason),
            })
    }

    // The proxy's own failure to reach a destination it was asked for.
    pub(crate) fn destination_unreachable(&self, reason: &str) -> TunnelError {
        destination_unreachable(
            match self.kind {
                TransportKind::Socks5 => "SOCKS",
                TransportKind::HttpConnect => "HTTP",
            },
            reason,
        )
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
                    return Err(failure("invalid CONNECT destination").into());
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
                    .map_err(|_| failure("HTTP proxy CONNECT failed"))?;
                let mut header = Vec::new();
                while !header.ends_with(b"\r\n\r\n") {
                    if header.len() >= 16384 {
                        return Err(failure("HTTP proxy response headers too large").into());
                    }
                    header.push(
                        stream
                            .read_u8()
                            .await
                            .map_err(|_| failure("HTTP proxy CONNECT failed"))?,
                    );
                }
                let line = std::str::from_utf8(&header)
                    .map_err(|_| failure("invalid HTTP proxy response"))?
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
                    Some(502) => Err(ConnectFailure::Unreachable("bad gateway (502)")),
                    Some(503) => Err(ConnectFailure::Unreachable("service unavailable (503)")),
                    Some(504) => Err(ConnectFailure::Unreachable("gateway timeout (504)")),
                    _ => Err(failure("HTTP proxy rejected CONNECT").into()),
                }
            }
        }
    }
}

#[async_trait::async_trait]
impl TunnelProvider for TransportProxy {
    async fn dial(&self, host: &str, port: u16) -> Result<Box<dyn TunnelStream>, TunnelError> {
        let mut stream = TcpStream::connect((self.host.as_str(), self.port))
            .await
            .map_err(|_| failure("proxy endpoint is unreachable"))?;
        stream
            .set_nodelay(true)
            .map_err(|_| failure("proxy socket setup failed"))?;
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
