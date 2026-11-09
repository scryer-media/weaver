// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::*;
use crate::{
    egress::SocketEgress,
    pipe::{Fallback, HopFailure, Naming},
    test_support::proxy::ProxyServerDouble,
    transport::{TransportKind, TransportProxy},
};
use tokio::{net::UdpSocket, sync::Semaphore};

fn bottom() -> Arc<Egress> {
    Arc::new(Egress::new(
        4,
        SocketEgress::SourceAddress("127.0.0.1".parse().unwrap()),
        Duration::from_secs(30),
    ))
}

fn hop(
    kind: TransportKind,
    endpoint: SocketAddr,
    credentials: Option<(&str, &str)>,
    inner: Arc<dyn Dialer>,
) -> Arc<TransportHop> {
    Arc::new(TransportHop {
        id: 7,
        spec: TransportProxy {
            kind,
            host: endpoint.ip().to_string(),
            port: endpoint.port(),
            username: credentials.map(|(user, _)| user.into()),
            password: credentials.map(|(_, password)| password.into()),
        },
        inner,
        timeout: Duration::from_secs(30),
        naming: Naming::Proxy,
        failure: HopFailure::default(),
    })
}

// A pause the test ends. It records each pause asked for, and lasts until
// the test lets one pass.
struct Held {
    asked: watch::Sender<Vec<Duration>>,
    release: Semaphore,
}

impl Held {
    fn new() -> Arc<Self> {
        Arc::new(Self {
            asked: watch::channel(Vec::new()).0,
            release: Semaphore::new(0),
        })
    }
}

#[async_trait::async_trait]
impl Pause for Held {
    async fn wait(&self, pause: Duration) {
        self.asked.send_modify(|asked| asked.push(pause));
        self.release.acquire().await.unwrap().forget();
    }
}

// A socket through a SOCKS5 hop on the loopback egress to `double`.
async fn open(
    double: &ProxyServerDouble,
    credentials: Option<(&str, &str)>,
    pause: Arc<Held>,
) -> Socks5Socket {
    let hop = hop(TransportKind::Socks5, double.addr, credentials, bottom());
    Socks5Datagrams::pausing(hop, Arc::new(EgressDatagrams(bottom())), pause)
        .open()
        .await
}

async fn peer() -> (UdpSocket, SocketAddr) {
    let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let address = socket.local_addr().unwrap();
    (socket, address)
}

fn is_up(state: &State) -> bool {
    matches!(state, State::Up(_))
}

// A datagram reaches the peer from the relay, and the peer's answer comes
// back unwrapped, from the peer.
async fn round_trip(socket: &Socks5Socket, double: &ProxyServerDouble, peer: &UdpSocket) {
    let address = peer.local_addr().unwrap();
    socket.send_to(b"ping", address).await.unwrap();
    let mut buffer = [0; 64];
    let (len, from) = peer.recv_from(&mut buffer).await.unwrap();
    assert_eq!(&buffer[..len], b"ping");
    assert_eq!(Some(from), double.relay_outbound());
    peer.send_to(b"pong", from).await.unwrap();
    let (len, from) = socket.recv_from(&mut buffer).await.unwrap();
    assert_eq!(&buffer[..len], b"pong");
    assert_eq!(from, address);
}

#[tokio::test]
async fn a_datagram_goes_through_the_relay_and_its_answer_comes_back_unwrapped() {
    let double = ProxyServerDouble::start_udp(None, false).await;
    let socket = open(&double, None, Held::new()).await;
    assert!(is_up(&socket.states().borrow()));
    let (peer, _) = peer().await;
    round_trip(&socket, &double, &peer).await;
}

#[tokio::test]
async fn an_association_signs_in_with_the_proxys_credentials() {
    let double = ProxyServerDouble::start_udp(Some(("user".into(), "secret".into())), false).await;
    let socket = open(&double, Some(("user", "secret")), Held::new()).await;
    let (peer, _) = peer().await;
    round_trip(&socket, &double, &peer).await;

    // Rejected credentials are final: no pause, no second try.
    let pause = Held::new();
    let refused = open(&double, Some(("user", "wrong")), pause.clone()).await;
    assert!(matches!(*refused.states().borrow(), State::Refused));
    let error = refused
        .send_to(b"x", peer.local_addr().unwrap())
        .await
        .unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::NetworkUnreachable);
    assert!(pause.asked.borrow().is_empty());
    assert_eq!(*double.associations().borrow(), 1);
}

#[tokio::test]
async fn a_relay_named_as_the_unspecified_address_is_the_proxys_own() {
    let double = ProxyServerDouble::start_udp(None, true).await;
    let socket = open(&double, None, Held::new()).await;
    let (peer, _) = peer().await;
    round_trip(&socket, &double, &peer).await;
}

#[tokio::test]
async fn a_proxy_that_refuses_udp_associate_is_not_asked_again() {
    let double = ProxyServerDouble::start(TransportKind::Socks5, None, Default::default()).await;
    let pause = Held::new();
    let socket = open(&double, None, pause.clone()).await;
    assert!(matches!(*socket.states().borrow(), State::Refused));
    assert!(pause.asked.borrow().is_empty());
}

// While the association is down a send fails, and nothing reaches the peer
// by any other path. The next association carries datagrams again.
#[tokio::test]
async fn a_dropped_control_connection_drops_datagrams_until_the_next_association() {
    let double = ProxyServerDouble::start_udp(None, false).await;
    let pause = Held::new();
    let socket = open(&double, None, pause.clone()).await;
    let (peer, address) = peer().await;
    round_trip(&socket, &double, &peer).await;
    let first_relay = double.relay_outbound();

    let mut states = socket.states();
    double.hang_up();
    states
        .wait_for(|state| matches!(state, State::Down))
        .await
        .unwrap();
    let error = socket.send_to(b"lost", address).await.unwrap_err();
    assert_eq!(error.kind(), io::ErrorKind::NetworkUnreachable);

    // After an association that held, the first pause is five seconds.
    pause
        .asked
        .subscribe()
        .wait_for(|asked| !asked.is_empty())
        .await
        .unwrap();
    assert_eq!(*pause.asked.borrow(), [Duration::from_secs(5)]);
    pause.release.add_permits(1);
    states.wait_for(is_up).await.unwrap();
    assert_eq!(*double.associations().borrow(), 2);

    // The first datagram the peer receives after the hang-up is the one
    // sent once the new association is up, from the new relay.
    socket.send_to(b"after", address).await.unwrap();
    let mut buffer = [0; 64];
    let (len, from) = peer.recv_from(&mut buffer).await.unwrap();
    assert_eq!(&buffer[..len], b"after");
    assert_eq!(Some(from), double.relay_outbound());
    assert_ne!(Some(from), first_relay);
}

#[tokio::test]
async fn a_fragmented_datagram_from_the_relay_is_dropped() {
    let double = ProxyServerDouble::start_udp(None, false).await;
    let socket = open(&double, None, Held::new()).await;
    let (peer, address) = peer().await;
    round_trip(&socket, &double, &peer).await;

    let mut fragment = vec![0, 0, 1, 1, 127, 0, 0, 1];
    fragment.extend_from_slice(&address.port().to_be_bytes());
    fragment.extend_from_slice(b"frag");
    double.inject(&fragment).await;
    let mut whole = vec![0, 0, 0, 1, 127, 0, 0, 1];
    whole.extend_from_slice(&address.port().to_be_bytes());
    whole.extend_from_slice(b"whole");
    double.inject(&whole).await;

    let mut buffer = [0; 64];
    let (len, from) = socket.recv_from(&mut buffer).await.unwrap();
    assert_eq!(&buffer[..len], b"whole");
    assert_eq!(from, address);
}

#[test]
fn the_pause_grows_with_failures_in_a_row() {
    assert_eq!(pause_after(0), Duration::from_secs(5));
    assert_eq!(pause_after(1), Duration::from_secs(5));
    assert_eq!(pause_after(4), Duration::from_secs(13));
    assert_eq!(pause_after(16), Duration::from_secs(125));
    assert_eq!(pause_after(u32::MAX), Duration::from_secs(125));
}

#[test]
fn the_udp_header_wraps_and_unwraps_both_families() {
    for target in ["192.0.2.1:6881", "[2001:db8::1]:6881"] {
        let target: SocketAddr = target.parse().unwrap();
        let packet = wrap(target, b"data");
        let (start, from) = unwrap(&packet).unwrap();
        assert_eq!((&packet[start..], from), (&b"data"[..], target));
    }
    assert_eq!(wrap("192.0.2.1:1".parse().unwrap(), b"").len(), 10);
    assert_eq!(wrap("[2001:db8::1]:1".parse().unwrap(), b"").len(), 22);

    // A sender named by its address in text.
    let mut named = vec![0, 0, 0, ATYP_DOMAIN, 9];
    named.extend_from_slice(b"192.0.2.1");
    named.extend_from_slice(&[0, 80]);
    named.extend_from_slice(b"x");
    assert_eq!(
        unwrap(&named),
        Some((named.len() - 1, "192.0.2.1:80".parse().unwrap()))
    );
    // Nothing to deliver: a bare header, a fragment, a host name, a cut
    // address.
    assert_eq!(unwrap(&wrap("192.0.2.1:1".parse().unwrap(), b"")), None);
    let mut fragment = wrap("192.0.2.1:1".parse().unwrap(), b"x");
    fragment[2] = 1;
    assert_eq!(unwrap(&fragment), None);
    let mut host = vec![0, 0, 0, ATYP_DOMAIN, 4];
    host.extend_from_slice(b"host\0\x50x");
    assert_eq!(unwrap(&host), None);
    assert_eq!(unwrap(&[0, 0, 0, ATYP_IPV6, 1, 2, 3]), None);
}

// A SOCKS5 hop carries datagrams only where they go through every stage
// beneath it; an HTTP CONNECT hop carries none.
#[test]
fn a_socks5_hop_carries_datagrams_only_over_a_stage_that_carries_them() {
    let endpoint: SocketAddr = "192.0.2.1:1080".parse().unwrap();
    let socks = |inner: Arc<dyn Dialer>| -> Arc<dyn Dialer> {
        hop(TransportKind::Socks5, endpoint, None, inner)
    };
    let connect = |inner: Arc<dyn Dialer>| -> Arc<dyn Dialer> {
        hop(TransportKind::HttpConnect, endpoint, None, inner)
    };

    // On the egress, and on another SOCKS5 hop.
    assert!(socks(bottom()).datagrams().is_some());
    assert!(socks(socks(bottom())).datagrams().is_some());
    // HTTP CONNECT, on the egress or beneath SOCKS5.
    assert!(connect(bottom()).datagrams().is_none());
    assert!(socks(connect(bottom())).datagrams().is_none());
    assert!(connect(socks(bottom())).datagrams().is_none());
    // The egress itself carries a session's datagrams no more than before.
    assert!(bottom().datagrams().is_none());

    // A ladder carries the datagrams of the rung a dial would try.
    let ladder = Arc::new(Fallback::new(vec![socks(bottom()), connect(bottom())]));
    assert!(Arc::clone(&ladder).datagrams().is_some());
    ladder.report_rung(
        0,
        Some(&crate::pipe::DialError::Egress(io::Error::other("down"))),
    );
    assert!(ladder.datagrams().is_none());
}
