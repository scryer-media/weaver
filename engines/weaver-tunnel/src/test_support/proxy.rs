//! Strict local HTTP CONNECT/SOCKS5 fixture with an explicit destination map.
use crate::transport::TransportKind;
use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::{Arc, Mutex},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

pub struct ProxyServerDouble {
    pub addr: SocketAddr,
    pub targets: Arc<Mutex<Vec<(String, u16)>>>,
    task: tokio::task::JoinHandle<()>,
}
impl Drop for ProxyServerDouble {
    fn drop(&mut self) {
        self.task.abort();
    }
}
impl ProxyServerDouble {
    pub async fn start(
        kind: TransportKind,
        auth: Option<(String, String)>,
        mapping: HashMap<(String, u16), SocketAddr>,
    ) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let targets = Arc::new(Mutex::new(Vec::new()));
        let recorded = targets.clone();
        let mapping = Arc::new(mapping);
        let task = tokio::spawn(async move {
            let mut tasks = tokio::task::JoinSet::new();
            loop {
                tokio::select! {
                    accepted = listener.accept() => {
                        let Ok((stream, _)) = accepted else { break; };
                        let auth = auth.clone(); let mapping = mapping.clone(); let recorded = recorded.clone();
                        tasks.spawn(async move { let _ = tokio::time::timeout(std::time::Duration::from_secs(30), serve(stream, kind, auth, mapping, recorded)).await; });
                    }
                    _ = tasks.join_next(), if !tasks.is_empty() => {}
                }
            }
        });
        Self {
            addr,
            targets,
            task,
        }
    }
}
type Targets = Arc<Mutex<Vec<(String, u16)>>>;
async fn serve(
    mut stream: TcpStream,
    kind: TransportKind,
    auth: Option<(String, String)>,
    mapping: Arc<HashMap<(String, u16), SocketAddr>>,
    targets: Targets,
) -> std::io::Result<()> {
    let invalid = || std::io::Error::other("invalid fixture request");
    let target = if matches!(kind, TransportKind::HttpConnect) {
        let mut header = Vec::new();
        while !header.ends_with(b"\r\n\r\n") && header.len() < 16384 {
            header.push(stream.read_u8().await?);
        }
        let header = String::from_utf8(header).map_err(|_| invalid())?;
        if let Some((user, password)) = auth {
            use base64::Engine;
            let value =
                base64::engine::general_purpose::STANDARD.encode(format!("{user}:{password}"));
            if !header.lines().any(|line| {
                line.eq_ignore_ascii_case(&format!("Proxy-Authorization: Basic {value}"))
            }) {
                stream
                    .write_all(b"HTTP/1.1 407 Authentication Required\r\n\r\n")
                    .await?;
                return Ok(());
            }
        }
        let mut parts = header
            .lines()
            .next()
            .ok_or_else(invalid)?
            .split_whitespace();
        if parts.next() != Some("CONNECT") {
            return Err(invalid());
        }
        let authority = parts.next().ok_or_else(invalid)?;
        let (host, port) = authority.rsplit_once(':').ok_or_else(invalid)?;
        (
            host.trim_matches(['[', ']']).to_string(),
            port.parse().map_err(|_| invalid())?,
        )
    } else {
        if stream.read_u8().await? != 5 {
            return Err(invalid());
        }
        let count = stream.read_u8().await? as usize;
        let mut methods = vec![0; count];
        stream.read_exact(&mut methods).await?;
        let method = if auth.is_some() { 2 } else { 0 };
        if !methods.contains(&method) {
            stream.write_all(&[5, 255]).await?;
            return Ok(());
        }
        stream.write_all(&[5, method]).await?;
        if let Some((user, pass)) = auth {
            if stream.read_u8().await? != 1 {
                return Err(invalid());
            }
            let n = stream.read_u8().await? as usize;
            let mut u = vec![0; n];
            stream.read_exact(&mut u).await?;
            let n = stream.read_u8().await? as usize;
            let mut p = vec![0; n];
            stream.read_exact(&mut p).await?;
            let accepted = u == user.as_bytes() && p == pass.as_bytes();
            stream.write_all(&[1, u8::from(!accepted)]).await?;
            if !accepted {
                return Ok(());
            }
        }
        let mut prefix = [0; 4];
        stream.read_exact(&mut prefix).await?;
        if prefix[..3] != [5, 1, 0] {
            return Err(invalid());
        }
        let host = match prefix[3] {
            1 => {
                let mut ip = [0; 4];
                stream.read_exact(&mut ip).await?;
                std::net::Ipv4Addr::from(ip).to_string()
            }
            4 => {
                let mut ip = [0; 16];
                stream.read_exact(&mut ip).await?;
                std::net::Ipv6Addr::from(ip).to_string()
            }
            3 => {
                let n = stream.read_u8().await? as usize;
                let mut name = vec![0; n];
                stream.read_exact(&mut name).await?;
                String::from_utf8(name).map_err(|_| invalid())?
            }
            _ => return Err(invalid()),
        };
        (host, stream.read_u16().await?)
    };
    targets.lock().unwrap().push(target.clone());
    let Some(addr) = mapping.get(&target) else {
        if matches!(kind, TransportKind::HttpConnect) {
            stream
                .write_all(b"HTTP/1.1 502 Unreachable\r\n\r\n")
                .await?;
        } else {
            stream.write_all(&[5, 4, 0, 1, 0, 0, 0, 0, 0, 0]).await?;
        }
        return Ok(());
    };
    let mut upstream = TcpStream::connect(addr).await?;
    if matches!(kind, TransportKind::HttpConnect) {
        stream.write_all(b"HTTP/1.1 200 Connected\r\n\r\n").await?;
    } else {
        stream.write_all(&[5, 0, 0, 1, 0, 0, 0, 0, 0, 0]).await?;
    }
    tokio::io::copy_bidirectional(&mut stream, &mut upstream).await?;
    Ok(())
}
