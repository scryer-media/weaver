// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Strict local HTTP CONNECT/SOCKS5 fixture with an explicit destination map.
// A SOCKS5 double started with `start_udp` also answers UDP ASSOCIATE: it
// relays datagrams between its client and any loopback peer for as long as
// the association's control connection stays open.
use crate::transport::TransportKind;
use std::{
    collections::HashMap,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    sync::{Arc, Mutex},
};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream, UdpSocket},
    sync::watch,
};

pub struct ProxyServerDouble {
    pub addr: SocketAddr,
    pub targets: Arc<Mutex<Vec<(String, u16)>>>,
    udp: Arc<UdpRelays>,
    task: tokio::task::JoinHandle<()>,
}
impl Drop for ProxyServerDouble {
    fn drop(&mut self) {
        self.task.abort();
    }
}

// What a UDP-capable double holds of its associations.
struct UdpRelays {
    enabled: bool,
    // Name the relay as 0.0.0.0, the proxy's own address.
    unspecified: bool,
    // How many associations have been made.
    associations: watch::Sender<usize>,
    // Bumped to end every open association.
    hang_up: watch::Sender<u64>,
    current: Mutex<Option<Relay>>,
}

#[derive(Clone)]
struct Relay {
    // Faces the client.
    socket: Arc<UdpSocket>,
    // Faces the peers.
    outbound: SocketAddr,
    client: Arc<Mutex<Option<SocketAddr>>>,
}

impl ProxyServerDouble {
    pub async fn start(
        kind: TransportKind,
        auth: Option<(String, String)>,
        mapping: HashMap<(String, u16), SocketAddr>,
    ) -> Self {
        Self::serving(kind, auth, mapping, false, false).await
    }

    // A SOCKS5 double that also relays datagrams. With `unspecified` it
    // names its relay as 0.0.0.0 and the relay's port.
    pub async fn start_udp(auth: Option<(String, String)>, unspecified: bool) -> Self {
        Self::serving(
            TransportKind::Socks5,
            auth,
            HashMap::new(),
            true,
            unspecified,
        )
        .await
    }

    async fn serving(
        kind: TransportKind,
        auth: Option<(String, String)>,
        mapping: HashMap<(String, u16), SocketAddr>,
        udp: bool,
        unspecified: bool,
    ) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let targets = Arc::new(Mutex::new(Vec::new()));
        let recorded = targets.clone();
        let mapping = Arc::new(mapping);
        let udp = Arc::new(UdpRelays {
            enabled: udp,
            unspecified,
            associations: watch::channel(0).0,
            hang_up: watch::channel(0).0,
            current: Mutex::new(None),
        });
        let relays = udp.clone();
        let task = tokio::spawn(async move {
            let mut tasks = tokio::task::JoinSet::new();
            loop {
                tokio::select! {
                    accepted = listener.accept() => {
                        let Ok((stream, _)) = accepted else { break; };
                        let auth = auth.clone(); let mapping = mapping.clone(); let recorded = recorded.clone();
                        let relays = relays.clone();
                        tasks.spawn(async move { let _ = serve(stream, kind, auth, mapping, recorded, relays).await; });
                    }
                    _ = tasks.join_next(), if !tasks.is_empty() => {}
                }
            }
        });
        Self {
            addr,
            targets,
            udp,
            task,
        }
    }

    // How many associations the double has made, to wait on.
    pub fn associations(&self) -> watch::Receiver<usize> {
        self.udp.associations.subscribe()
    }

    // Close the control connection of every open association.
    pub fn hang_up(&self) {
        self.udp.hang_up.send_modify(|generation| *generation += 1);
    }

    // The address the open association's datagrams reach peers from.
    pub fn relay_outbound(&self) -> Option<SocketAddr> {
        self.current().map(|relay| relay.outbound)
    }

    // Send `raw` as it is from the open association's relay to its client,
    // which must have sent through the relay first.
    pub async fn inject(&self, raw: &[u8]) {
        let relay = self.current().expect("an open association");
        let client = relay
            .client
            .lock()
            .unwrap()
            .expect("the client has sent through the relay");
        relay.socket.send_to(raw, client).await.unwrap();
    }

    fn current(&self) -> Option<Relay> {
        self.udp.current.lock().unwrap().clone()
    }
}
type Targets = Arc<Mutex<Vec<(String, u16)>>>;
async fn serve(
    mut stream: TcpStream,
    kind: TransportKind,
    auth: Option<(String, String)>,
    mapping: Arc<HashMap<(String, u16), SocketAddr>>,
    targets: Targets,
    udp: Arc<UdpRelays>,
) -> std::io::Result<()> {
    let invalid = || std::io::Error::other("invalid fixture request");
    let mut command = 1;
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
        if prefix[0] != 5 || prefix[2] != 0 {
            return Err(invalid());
        }
        command = prefix[1];
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
    match command {
        1 => {}
        3 if udp.enabled => return associate(stream, udp).await,
        // Command not supported.
        _ => {
            stream.write_all(&[5, 7, 0, 1, 0, 0, 0, 0, 0, 0]).await?;
            return Ok(());
        }
    }
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

// One association: relay datagrams until the control connection ends or
// the test hangs it up. The relay's sockets close with it.
async fn associate(mut stream: TcpStream, udp: Arc<UdpRelays>) -> std::io::Result<()> {
    let socket = Arc::new(UdpSocket::bind("127.0.0.1:0").await?);
    let outbound = Arc::new(UdpSocket::bind("127.0.0.1:0").await?);
    let mut hang_up = udp.hang_up.subscribe();
    let named = if udp.unspecified {
        Ipv4Addr::UNSPECIFIED
    } else {
        Ipv4Addr::LOCALHOST
    };
    let mut reply = vec![5, 0, 0, 1];
    reply.extend_from_slice(&named.octets());
    reply.extend_from_slice(&socket.local_addr()?.port().to_be_bytes());
    let relay = Relay {
        socket: socket.clone(),
        outbound: outbound.local_addr()?,
        client: Arc::default(),
    };
    *udp.current.lock().unwrap() = Some(relay.clone());
    stream.write_all(&reply).await?;
    udp.associations.send_modify(|count| *count += 1);
    let mut byte = [0];
    let ended = tokio::select! {
        result = from_client(&relay, &outbound) => result,
        result = to_client(&relay, &outbound) => result,
        _ = stream.read(&mut byte) => Ok(()),
        _ = hang_up.changed() => Ok(()),
    };
    udp.current.lock().unwrap().take();
    ended
}

// Unwrap the client's datagrams and send them on to their peers.
async fn from_client(relay: &Relay, outbound: &UdpSocket) -> std::io::Result<()> {
    let mut buffer = vec![0; 65536];
    loop {
        let (len, from) = relay.socket.recv_from(&mut buffer).await?;
        *relay.client.lock().unwrap() = Some(from);
        // RSV RSV FRAG ATYP: fragments and names are not relayed.
        let (target, start) = match buffer[..len] {
            [0, 0, 0, 1, a, b, c, d, p, q, ..] => (
                SocketAddr::new(IpAddr::from([a, b, c, d]), u16::from_be_bytes([p, q])),
                10,
            ),
            [0, 0, 0, 4, ..] if len >= 22 => {
                let octets: [u8; 16] = buffer[4..20].try_into().unwrap();
                (
                    SocketAddr::new(
                        IpAddr::from(octets),
                        u16::from_be_bytes([buffer[20], buffer[21]]),
                    ),
                    22,
                )
            }
            _ => continue,
        };
        outbound.send_to(&buffer[start..len], target).await?;
    }
}

// Wrap the peers' datagrams and send them to the client.
async fn to_client(relay: &Relay, outbound: &UdpSocket) -> std::io::Result<()> {
    let mut buffer = vec![0; 65536];
    loop {
        let (len, from) = outbound.recv_from(&mut buffer).await?;
        let Some(client) = *relay.client.lock().unwrap() else {
            continue;
        };
        let IpAddr::V4(ip) = from.ip() else {
            continue;
        };
        let mut packet = vec![0, 0, 0, 1];
        packet.extend_from_slice(&ip.octets());
        packet.extend_from_slice(&from.port().to_be_bytes());
        packet.extend_from_slice(&buffer[..len]);
        relay.socket.send_to(&packet, client).await?;
    }
}
