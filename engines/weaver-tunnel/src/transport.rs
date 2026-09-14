//! TCP proxy adapters. Only the proxy endpoint is resolved on the host.
use crate::{TunnelError, TunnelProvider, TunnelStream};
use base64::Engine;
use std::net::{IpAddr, SocketAddr};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
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

/// Establish a SOCKS5 CONNECT without resolving the destination locally.
pub async fn socks_connect(
    stream: &mut TcpStream,
    host: &str,
    port: u16,
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
        return Err(failure("SOCKS proxy could not connect to destination"));
    }
    let len = match header[3] {
        1 => 4,
        4 => 16,
        3 => stream
            .read_u8()
            .await
            .map_err(|_| failure("invalid SOCKS reply"))? as usize,
        _ => return Err(failure("invalid SOCKS reply")),
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

#[async_trait::async_trait]
impl TunnelProvider for TransportProxy {
    async fn dial(&self, host: &str, port: u16) -> Result<Box<dyn TunnelStream>, TunnelError> {
        let mut stream = TcpStream::connect((self.host.as_str(), self.port))
            .await
            .map_err(|_| failure("proxy endpoint is unreachable"))?;
        stream
            .set_nodelay(true)
            .map_err(|_| failure("proxy socket setup failed"))?;
        match self.kind {
            TransportKind::Socks5 => {
                socks_connect(
                    &mut stream,
                    host,
                    port,
                    self.username
                        .as_deref()
                        .map(|u| (u, self.password.as_deref().unwrap_or(""))),
                )
                .await?
            }
            TransportKind::HttpConnect => {
                if host.is_empty()
                    || host
                        .bytes()
                        .any(|b| b.is_ascii_whitespace() || b.is_ascii_control())
                {
                    return Err(failure("invalid CONNECT destination"));
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
                        return Err(failure("HTTP proxy response headers too large"));
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
                if !matches!(fields.next(), Some("HTTP/1.0" | "HTTP/1.1"))
                    || fields
                        .next()
                        .and_then(|v| v.parse::<u16>().ok())
                        .is_none_or(|code| !(200..300).contains(&code))
                {
                    return Err(failure("HTTP proxy rejected CONNECT"));
                }
            }
        }
        Ok(Box::new(stream))
    }
    fn describe(&self) -> String {
        format!("{:?} {}:{}", self.kind, self.host, self.port)
    }
}
