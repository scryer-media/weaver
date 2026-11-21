// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A port forwarded to a WireGuard tunnel by the gateway at its far end, over
// NAT-PMP (RFC 6886) inside the tunnel.
//
// The client asks the gateway for its public address, then for a TCP and a
// UDP mapping of any port it chooses: internal port 0, suggested external
// port 1. The gateway answers with the port it forwards, to the same port
// inside the tunnel, and with how long the mapping lasts. Both mappings must
// name the same port. The client renews them at three quarters of the
// lifetime the gateway granted, asking for the port it holds.
use std::{
    net::{IpAddr, Ipv4Addr, SocketAddr},
    sync::Arc,
    time::Duration,
};

use tokio::sync::watch;

use crate::endpoint::{DatagramSocket, DatagramTransport};

pub const NATPMP_PORT: u16 = 5351;
// The lifetime asked for each mapping.
const LIFETIME: u32 = 60;
// RFC 6886 3.1: the first retransmission after 250 ms, each wait doubling,
// nine attempts in all.
const FIRST_WAIT: Duration = Duration::from_millis(250);
const ATTEMPTS: u32 = 9;

/// Where peers reach a tunnel from outside, as its gateway last said.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct PortMapping {
    /// The port the gateway forwards, `None` until it forwards one or once
    /// it stopped.
    pub external_port: Option<u16>,
    /// The gateway's public address, when it gave one.
    pub external_ip: Option<IpAddr>,
    /// Why the last request failed.
    pub error: Option<String>,
}

/// The gateway at the far end of a tunnel whose first IPv4 address is
/// `address`: the same network, host 1.
pub fn gateway_of(address: Ipv4Addr) -> Ipv4Addr {
    let [a, b, c, _] = address.octets();
    Ipv4Addr::new(a, b, c, 1)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(super) struct Lease {
    pub port: u16,
    pub lifetime: Duration,
}

// One request and the gateway's answer to it, the opcode answered named.
async fn transact(
    socket: &dyn DatagramSocket,
    gateway: SocketAddr,
    request: &[u8],
    answer: &mut [u8; 16],
) -> Result<usize, String> {
    let opcode = request[1] | 0x80;
    let mut wait = FIRST_WAIT;
    for _ in 0..ATTEMPTS {
        socket
            .send_to(request, gateway)
            .await
            .map_err(|error| format!("could not ask the gateway: {error}"))?;
        let heard = tokio::time::timeout(wait, async {
            loop {
                let (len, from) = socket.recv_from(answer).await?;
                if from == gateway && len >= 4 && answer[0] == 0 && answer[1] == opcode {
                    return Ok::<_, std::io::Error>(len);
                }
            }
        })
        .await;
        match heard {
            Ok(Ok(len)) => {
                let result = u16::from_be_bytes([answer[2], answer[3]]);
                if result != 0 {
                    return Err(format!("the gateway refused with result code {result}"));
                }
                return Ok(len);
            }
            Ok(Err(error)) => return Err(format!("could not hear the gateway: {error}")),
            Err(_) => wait *= 2,
        }
    }
    Err("the gateway did not answer NAT-PMP".into())
}

// The gateway's public address.
async fn public_address(
    socket: &dyn DatagramSocket,
    gateway: SocketAddr,
) -> Result<Ipv4Addr, String> {
    let mut answer = [0u8; 16];
    let len = transact(socket, gateway, &[0, 0], &mut answer).await?;
    if len < 12 {
        return Err("the gateway's address answer is short".into());
    }
    Ok(Ipv4Addr::new(answer[8], answer[9], answer[10], answer[11]))
}

// One mapping of `opcode` (1 UDP, 2 TCP).
async fn map_one(
    socket: &dyn DatagramSocket,
    gateway: SocketAddr,
    opcode: u8,
    internal: u16,
    external: u16,
) -> Result<Lease, String> {
    let mut request = [0u8; 12];
    request[1] = opcode;
    request[4..6].copy_from_slice(&internal.to_be_bytes());
    request[6..8].copy_from_slice(&external.to_be_bytes());
    request[8..12].copy_from_slice(&LIFETIME.to_be_bytes());
    let mut answer = [0u8; 16];
    let len = transact(socket, gateway, &request, &mut answer).await?;
    if len < 16 {
        return Err("the gateway's mapping answer is short".into());
    }
    let port = u16::from_be_bytes([answer[10], answer[11]]);
    let lifetime = u32::from_be_bytes([answer[12], answer[13], answer[14], answer[15]]);
    if port == 0 || lifetime == 0 {
        return Err("the gateway forwarded no port".into());
    }
    Ok(Lease {
        port,
        lifetime: Duration::from_secs(lifetime.into()),
    })
}

// A TCP and a UDP mapping of one port: any port when `held` is none, else
// the one held.
pub(super) async fn map(
    socket: &dyn DatagramSocket,
    gateway: SocketAddr,
    held: Option<u16>,
) -> Result<(Lease, Ipv4Addr), String> {
    let address = public_address(socket, gateway).await?;
    let (internal, external) = held.map_or((0, 1), |port| (port, port));
    let udp = map_one(socket, gateway, 1, internal, external).await?;
    let tcp = map_one(socket, gateway, 2, internal, external).await?;
    if udp.port != tcp.port {
        return Err(format!(
            "the gateway forwarded TCP port {} and UDP port {}",
            tcp.port, udp.port
        ));
    }
    Ok((
        Lease {
            port: tcp.port,
            lifetime: tcp.lifetime.min(udp.lifetime),
        },
        address,
    ))
}

// When to renew a lease granted for `lifetime`.
pub(super) fn renewal(lifetime: Duration) -> Duration {
    (lifetime * 3 / 4).max(Duration::from_secs(1))
}

/// Keeps a port forwarded through `transport` by `gateway`, publishing each
/// outcome to `published`, until every receiver is gone. It asks at once,
/// renews at three quarters of the lifetime granted, and asks again after
/// the lifetime it asked for when a request failed. A socket that closes,
/// as one does when the tunnel is rebuilt, is opened again inside the
/// tunnel's replacement and the port asked for again at once.
pub(super) async fn keep(
    transport: Arc<dyn DatagramTransport>,
    gateway: SocketAddr,
    published: watch::Sender<PortMapping>,
) {
    let mut socket: Option<Arc<dyn DatagramSocket>> = None;
    let mut held: Option<Lease> = None;
    let mut wait = Duration::ZERO;
    loop {
        let open = socket.clone();
        tokio::select! {
            () = tokio::time::sleep(wait) => {}
            () = async {
                match &open {
                    Some(socket) => socket.closed().await,
                    None => std::future::pending().await,
                }
            } => socket = None,
            () = published.closed() => return,
        }
        if socket.as_ref().is_none_or(|socket| socket.is_closed()) {
            socket = transport.bind(gateway).await.ok();
        }
        let outcome = match &socket {
            Some(socket) => map(&**socket, gateway, held.map(|lease| lease.port)).await,
            None => Err("could not open a socket inside the tunnel".into()),
        };
        match outcome {
            Ok((lease, address)) => {
                held = Some(lease);
                wait = renewal(lease.lifetime);
                published.send_replace(PortMapping {
                    external_port: Some(lease.port),
                    external_ip: Some(IpAddr::V4(address)),
                    error: None,
                });
            }
            Err(error) => {
                // A socket that failed is opened again for the next try.
                socket = None;
                held = None;
                wait = Duration::from_secs(LIFETIME.into());
                published.send_replace(PortMapping {
                    external_port: None,
                    external_ip: None,
                    error: Some(error),
                });
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::{
        Mutex,
        atomic::{AtomicUsize, Ordering},
    };

    use tokio::sync::mpsc;

    use super::*;
    use crate::TunnelError;

    const GATEWAY: SocketAddr =
        SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 63, 0, 1)), NATPMP_PORT);
    const PUBLIC: Ipv4Addr = Ipv4Addr::new(198, 51, 100, 7);

    // What the fake gateway answers.
    #[derive(Clone)]
    struct Script {
        udp_port: u16,
        tcp_port: u16,
        lifetime: u32,
        result: u16,
        silent: bool,
    }

    struct Gateway {
        script: Mutex<Script>,
        // Every request it heard, in order.
        heard: Mutex<Vec<Vec<u8>>>,
        binds: AtomicUsize,
        // The socket bound last, to close it under the keeper.
        open: Mutex<Option<Arc<FakeSocket>>>,
    }

    impl Gateway {
        fn new(port: u16, lifetime: u32) -> Arc<Self> {
            Arc::new(Self {
                script: Mutex::new(Script {
                    udp_port: port,
                    tcp_port: port,
                    lifetime,
                    result: 0,
                    silent: false,
                }),
                heard: Mutex::new(Vec::new()),
                binds: AtomicUsize::new(0),
                open: Mutex::new(None),
            })
        }

        fn script(&self, change: impl FnOnce(&mut Script)) {
            change(&mut self.script.lock().unwrap());
        }

        fn heard(&self) -> Vec<Vec<u8>> {
            self.heard.lock().unwrap().clone()
        }

        fn answer(&self, request: &[u8]) -> Option<Vec<u8>> {
            self.heard.lock().unwrap().push(request.to_vec());
            let script = self.script.lock().unwrap().clone();
            if script.silent {
                return None;
            }
            let mut answer = vec![0, request[1] | 0x80];
            answer.extend_from_slice(&script.result.to_be_bytes());
            answer.extend_from_slice(&1000u32.to_be_bytes());
            match request[1] {
                0 => answer.extend_from_slice(&PUBLIC.octets()),
                opcode => {
                    let port = if opcode == 1 {
                        script.udp_port
                    } else {
                        script.tcp_port
                    };
                    answer.extend_from_slice(&request[4..6]);
                    answer.extend_from_slice(&port.to_be_bytes());
                    answer.extend_from_slice(&script.lifetime.to_be_bytes());
                }
            }
            Some(answer)
        }
    }

    struct FakeSocket {
        gateway: Arc<Gateway>,
        answers: mpsc::UnboundedSender<Vec<u8>>,
        inbox: tokio::sync::Mutex<mpsc::UnboundedReceiver<Vec<u8>>>,
        closed: watch::Sender<bool>,
    }

    impl FakeSocket {
        fn close(&self) {
            self.closed.send_replace(true);
        }
    }

    #[async_trait::async_trait]
    impl DatagramSocket for FakeSocket {
        async fn send_to(&self, data: &[u8], target: SocketAddr) -> std::io::Result<()> {
            assert_eq!(target, GATEWAY);
            if let Some(answer) = self.gateway.answer(data) {
                let _ = self.answers.send(answer);
            }
            Ok(())
        }

        async fn recv_from(&self, buffer: &mut [u8]) -> std::io::Result<(usize, SocketAddr)> {
            let mut inbox = self.inbox.lock().await;
            let mut closed = self.closed.subscribe();
            tokio::select! {
                answer = inbox.recv() => {
                    let answer = answer.expect("the socket holds its sender");
                    buffer[..answer.len()].copy_from_slice(&answer);
                    Ok((answer.len(), GATEWAY))
                }
                _ = closed.wait_for(|closed| *closed) => {
                    Err(std::io::Error::other("closed"))
                }
            }
        }

        fn local_addr(&self) -> Option<SocketAddr> {
            None
        }

        fn link_mtu(&self) -> Option<u16> {
            None
        }

        fn is_closed(&self) -> bool {
            *self.closed.borrow()
        }

        async fn closed(&self) {
            let mut closed = self.closed.subscribe();
            let _ = closed.wait_for(|closed| *closed).await;
        }
    }

    struct FakeTransport(Arc<Gateway>);

    #[async_trait::async_trait]
    impl DatagramTransport for FakeTransport {
        async fn resolve(&self, _: &str) -> Result<Vec<IpAddr>, TunnelError> {
            unreachable!("NAT-PMP resolves nothing")
        }

        async fn bind(&self, peer: SocketAddr) -> Result<Arc<dyn DatagramSocket>, TunnelError> {
            assert_eq!(peer, GATEWAY);
            self.0.binds.fetch_add(1, Ordering::SeqCst);
            let (answers, inbox) = mpsc::unbounded_channel();
            let socket = Arc::new(FakeSocket {
                gateway: self.0.clone(),
                answers,
                inbox: tokio::sync::Mutex::new(inbox),
                closed: watch::channel(false).0,
            });
            *self.0.open.lock().unwrap() = Some(socket.clone());
            Ok(socket)
        }
    }

    // A keeper of the fake gateway's port, and the watch it publishes to.
    fn keeper(
        gateway: &Arc<Gateway>,
    ) -> (tokio::task::JoinHandle<()>, watch::Receiver<PortMapping>) {
        let (published, mapping) = watch::channel(PortMapping::default());
        let task = tokio::spawn(keep(
            Arc::new(FakeTransport(gateway.clone())),
            GATEWAY,
            published,
        ));
        (task, mapping)
    }

    // A mapping request's opcode, internal port and suggested external port.
    fn request(heard: &[u8]) -> (u8, u16, u16) {
        (
            heard[1],
            u16::from_be_bytes([heard[4], heard[5]]),
            u16::from_be_bytes([heard[6], heard[7]]),
        )
    }

    fn mapped(port: u16) -> PortMapping {
        PortMapping {
            external_port: Some(port),
            external_ip: Some(IpAddr::V4(PUBLIC)),
            error: None,
        }
    }

    #[test]
    fn the_gateway_is_host_one_of_the_tunnel_network() {
        assert_eq!(
            gateway_of(Ipv4Addr::new(10, 2, 0, 2)),
            Ipv4Addr::new(10, 2, 0, 1)
        );
        assert_eq!(
            gateway_of(Ipv4Addr::new(10, 63, 7, 214)),
            Ipv4Addr::new(10, 63, 7, 1)
        );
    }

    #[test]
    fn a_lease_is_renewed_at_three_quarters_of_its_lifetime() {
        assert_eq!(renewal(Duration::from_secs(60)), Duration::from_secs(45));
        assert_eq!(
            renewal(Duration::from_secs(7200)),
            Duration::from_secs(5400)
        );
        assert_eq!(renewal(Duration::from_secs(1)), Duration::from_secs(1));
    }

    // The keeper asks at once for any port, then renews at three quarters of
    // the lifetime granted, asking for the port it holds.
    #[tokio::test(start_paused = true)]
    async fn a_port_is_mapped_at_once_and_renewed_at_three_quarters_of_its_lifetime() {
        let gateway = Gateway::new(41234, 60);
        let started = tokio::time::Instant::now();
        let (_task, mut mapping) = keeper(&gateway);

        mapping.changed().await.unwrap();
        assert_eq!(*mapping.borrow_and_update(), mapped(41234));
        assert_eq!(started.elapsed(), Duration::ZERO);
        let heard = gateway.heard();
        assert_eq!(heard.len(), 3);
        assert_eq!(heard[0], vec![0, 0]);
        assert_eq!(request(&heard[1]), (1, 0, 1));
        assert_eq!(request(&heard[2]), (2, 0, 1));
        assert_eq!(&heard[1][8..12], &60u32.to_be_bytes());

        mapping.changed().await.unwrap();
        assert_eq!(*mapping.borrow_and_update(), mapped(41234));
        assert_eq!(started.elapsed(), Duration::from_secs(45));
        let heard = gateway.heard();
        assert_eq!(heard.len(), 6);
        assert_eq!(request(&heard[4]), (1, 41234, 41234));
        assert_eq!(request(&heard[5]), (2, 41234, 41234));
        assert_eq!(gateway.binds.load(Ordering::SeqCst), 1);
    }

    // The gateway forwards another port on renewal: the keeper publishes it.
    #[tokio::test(start_paused = true)]
    async fn a_renewal_that_forwards_another_port_publishes_it() {
        let gateway = Gateway::new(41234, 60);
        let (_task, mut mapping) = keeper(&gateway);
        mapping.changed().await.unwrap();
        assert_eq!(*mapping.borrow_and_update(), mapped(41234));

        gateway.script(|script| {
            script.udp_port = 52000;
            script.tcp_port = 52000;
        });
        mapping.changed().await.unwrap();
        assert_eq!(*mapping.borrow_and_update(), mapped(52000));
    }

    // A refusal is published, and the keeper asks afresh for any port after
    // the lifetime it asks for, on a socket opened again.
    #[tokio::test(start_paused = true)]
    async fn a_refusal_is_published_and_asked_again_after_a_minute() {
        let gateway = Gateway::new(41234, 60);
        gateway.script(|script| script.result = 2);
        let started = tokio::time::Instant::now();
        let (_task, mut mapping) = keeper(&gateway);

        mapping.changed().await.unwrap();
        let refused = mapping.borrow_and_update().clone();
        assert_eq!(refused.external_port, None);
        assert_eq!(
            refused.error.as_deref(),
            Some("the gateway refused with result code 2")
        );

        gateway.script(|script| script.result = 0);
        mapping.changed().await.unwrap();
        assert_eq!(*mapping.borrow_and_update(), mapped(41234));
        assert_eq!(started.elapsed(), Duration::from_secs(60));
        assert_eq!(gateway.binds.load(Ordering::SeqCst), 2);
        let heard = gateway.heard();
        assert_eq!(request(&heard[heard.len() - 1]), (2, 0, 1));
    }

    // TCP and UDP must be forwarded on the same port.
    #[tokio::test(start_paused = true)]
    async fn tcp_and_udp_forwarded_on_different_ports_is_an_error() {
        let gateway = Gateway::new(41234, 60);
        gateway.script(|script| script.udp_port = 41235);
        let (_task, mut mapping) = keeper(&gateway);
        mapping.changed().await.unwrap();
        let mismatch = mapping.borrow_and_update().clone();
        assert_eq!(mismatch.external_port, None);
        assert_eq!(
            mismatch.error.as_deref(),
            Some("the gateway forwarded TCP port 41234 and UDP port 41235")
        );
    }

    // A silent gateway is asked nine times, each wait doubling from 250 ms,
    // before the keeper gives up on that round.
    #[tokio::test(start_paused = true)]
    async fn a_silent_gateway_is_asked_nine_times_with_doubling_waits() {
        let gateway = Gateway::new(41234, 60);
        gateway.script(|script| script.silent = true);
        let started = tokio::time::Instant::now();
        let (_task, mut mapping) = keeper(&gateway);
        mapping.changed().await.unwrap();
        assert_eq!(
            mapping.borrow_and_update().error.as_deref(),
            Some("the gateway did not answer NAT-PMP")
        );
        assert_eq!(gateway.heard(), vec![vec![0, 0]; 9]);
        assert_eq!(started.elapsed(), Duration::from_millis(250 * 511));
    }

    // The socket closes, as it does when the tunnel is rebuilt: the keeper
    // opens another and asks again at once, for the port it held.
    #[tokio::test(start_paused = true)]
    async fn a_closed_socket_is_opened_again_and_the_port_asked_for_at_once() {
        let gateway = Gateway::new(41234, 60);
        let (_task, mut mapping) = keeper(&gateway);
        mapping.changed().await.unwrap();
        mapping.borrow_and_update();

        let closed_at = tokio::time::Instant::now();
        gateway.open.lock().unwrap().take().unwrap().close();
        mapping.changed().await.unwrap();
        assert_eq!(*mapping.borrow_and_update(), mapped(41234));
        assert_eq!(closed_at.elapsed(), Duration::ZERO);
        assert_eq!(gateway.binds.load(Ordering::SeqCst), 2);
        let heard = gateway.heard();
        assert_eq!(request(&heard[heard.len() - 1]), (2, 41234, 41234));
    }

    // The keeper ends once nobody watches the port.
    #[tokio::test(start_paused = true)]
    async fn the_keeper_ends_once_nobody_watches() {
        let gateway = Gateway::new(41234, 60);
        let (task, mut mapping) = keeper(&gateway);
        mapping.changed().await.unwrap();
        drop(mapping);
        task.await.unwrap();
    }
}
