// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

//! The sockets a torrent session uses, for the one leg of its route.
//!
//! A leg's path decides whether the session dials with its own kernel
//! sockets or through the leg's dialer. Whether it may use UDP at all (DHT,
//! UDP trackers) and whether it can listen are the dialer's to say: a ladder
//! leg listens inside its tunnel when it ends in a WireGuard hop.

use std::collections::{HashMap, VecDeque};
use std::future::Future;
use std::io::{self, IoSlice, IoSliceMut};
use std::net::{IpAddr, SocketAddr};
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::task::{Context, Poll, Waker};

use tokio::io::{AsyncRead, ReadBuf};
use weaver_torrent::network::{
    AsyncReadVectored, AsyncReadVectoredIntoCompat, BoxAsyncReadVectored, BoxAsyncWrite, BoxFuture,
    DatagramSend, DatagramSocket, Resolved, Resolver,
};
use weaver_torrent::upnp::{PortMapReceiver, PortMapped, PortmapProtocol, PortmapTransport};
use weaver_torrent::{
    ClientShape, ClientUse, Egress, HttpClients, HttpTarget, Session, SocketFactory, StreamListener,
};
use weaver_tunnel::bridge::ConnectionOutcome;
use weaver_tunnel::endpoint::{DatagramSocket as TunnelSocket, DatagramTransport};
use weaver_tunnel::pipe::{
    DialedStream, Dialer, InboundListener as Listening, Purpose, Resolution, Target,
};

use crate::proxies::{EgressBinding, LegPath, PathBridge, ResolvedBy};

/// How a leg reaches peers.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub enum LegKind {
    /// The host's own sockets.
    System,
    /// Kernel sockets bound to a device.
    Interface(String),
    /// Kernel sockets bound to a source address.
    SourceAddress(IpAddr),
    /// Through a proxy or tunnel rung: stream connections, and datagrams
    /// when the rung carries them.
    Ladder,
}

impl LegKind {
    pub fn of(binding: &EgressBinding, path: &LegPath) -> Self {
        match (path, binding) {
            (LegPath::Ladder { .. }, _) => Self::Ladder,
            (LegPath::Direct, EgressBinding::System) => Self::System,
            (LegPath::Direct, EgressBinding::Interface { name }) => Self::Interface(name.clone()),
            (LegPath::Direct, EgressBinding::SourceAddress { address }) => {
                Self::SourceAddress(*address)
            }
        }
    }

    /// Whether a session on this leg opens kernel sockets itself.
    pub fn own_sockets(&self) -> bool {
        matches!(self, Self::System | Self::Interface(_))
    }

    /// What a session on this leg may do over `dialer`, the leg's dialer
    /// when its sockets come from one. A ladder leg carries datagrams, and so
    /// DHT, exactly when its dialer does.
    pub fn shape(&self, dialer: Option<&Arc<dyn Dialer>>) -> Shape {
        match self {
            Self::System => Shape {
                own_sockets: true,
                dht: true,
                upnp: true,
                listen: true,
                utp: true,
                datagrams: true,
            },
            Self::Interface(_) => Shape {
                own_sockets: true,
                dht: true,
                upnp: false,
                listen: true,
                utp: true,
                datagrams: true,
            },
            Self::SourceAddress(_) => Shape {
                own_sockets: false,
                dht: true,
                upnp: false,
                listen: true,
                utp: true,
                datagrams: true,
            },
            Self::Ladder => {
                let datagrams = dialer.is_some_and(|dialer| carrier(dialer).is_some());
                Shape {
                    own_sockets: false,
                    dht: datagrams,
                    upnp: false,
                    listen: dialer.is_some_and(|dialer| Arc::clone(dialer).inbound().is_some()),
                    utp: datagrams,
                    datagrams,
                }
            }
        }
    }

    pub fn bind_device(&self) -> Option<&str> {
        match self {
            Self::Interface(name) => Some(name),
            _ => None,
        }
    }

    /// The address the leg's listener and datagram socket bind, when it
    /// pins one.
    pub fn listen_ip(&self) -> Option<IpAddr> {
        match self {
            Self::SourceAddress(address) => Some(*address),
            _ => None,
        }
    }
}

/// The capabilities of a session on one leg.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Shape {
    /// The session opens kernel sockets itself; otherwise every socket comes
    /// from the leg's factory.
    pub own_sockets: bool,
    pub dht: bool,
    pub upnp: bool,
    /// Incoming peer connections.
    pub listen: bool,
    /// uTP on the leg's datagram route.
    pub utp: bool,
    /// UDP of any kind: UDP trackers, uTP and DHT.
    pub datagrams: bool,
}

// The datagrams `dialer` carries for the session's UDP.
fn carrier(dialer: &Arc<dyn Dialer>) -> Option<Arc<dyn DatagramTransport>> {
    Arc::clone(dialer).datagrams()
}

/// Peer sockets through a leg's dialer; a leg that pins a source address
/// also listens and carries UDP on it, and any other leg carries UDP through
/// its dialer when the dialer carries datagrams.
pub struct LegSockets {
    dialer: Arc<dyn Dialer>,
    source: Option<IpAddr>,
}

impl LegSockets {
    pub fn new(dialer: Arc<dyn Dialer>, kind: &LegKind) -> Self {
        Self {
            dialer,
            source: kind.listen_ip(),
        }
    }

    // A listener on `addr`'s port: on the pinned source address, else inside
    // the leg's tunnel, where the `forwarded` one takes the port the tunnel's
    // provider forwards.
    fn listen<'a>(
        &'a self,
        addr: SocketAddr,
        forwarded: bool,
    ) -> BoxFuture<'a, io::Result<Box<dyn StreamListener>>> {
        if let Some(ip) = self.source {
            return Box::pin(async move {
                let listener =
                    tokio::net::TcpListener::bind(SocketAddr::new(ip, addr.port())).await?;
                Ok(Box::new(Listener(listener)) as Box<dyn StreamListener>)
            });
        }
        let Some(inbound) = Arc::clone(&self.dialer).inbound() else {
            return Box::pin(async move {
                Err(io::Error::new(
                    io::ErrorKind::Unsupported,
                    "this leg cannot listen",
                ))
            });
        };
        shared(async move {
            let listener = inbound
                .listen(addr.port(), forwarded)
                .await
                .map_err(io::Error::other)?;
            Ok(Box::new(Arriving(listener)) as Box<dyn StreamListener>)
        })
    }

    // The datagrams the session's UDP rides, when not the source address.
    fn carrier(&self) -> Option<Arc<dyn DatagramTransport>> {
        self.source
            .is_none()
            .then(|| carrier(&self.dialer))
            .flatten()
    }
}

// A future the session can share between threads. The session's futures
// must be Sync and a tunnel socket's are not; nothing reaches this one but
// its poll, which holds it exclusively.
fn shared<'a, T>(future: impl Future<Output = T> + Send + 'a) -> BoxFuture<'a, T> {
    let mut future = Mutex::new(Box::pin(future));
    Box::pin(std::future::poll_fn(move |cx| {
        future
            .get_mut()
            .unwrap_or_else(PoisonError::into_inner)
            .as_mut()
            .poll(cx)
    }))
}

// A peer connection's read half through a leg's dialer. The leg counts the
// bytes it reads, as it counts those of every connection it carries.
struct Counted {
    read: BoxAsyncReadVectored,
    outcome: Arc<ConnectionOutcome>,
}

impl AsyncRead for Counted {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<io::Result<()>> {
        let before = buf.filled().len();
        let polled = Pin::new(&mut self.read).poll_read(cx, buf);
        self.outcome.read(buf.filled().len() - before);
        polled
    }
}

impl AsyncReadVectored for Counted {
    fn poll_read_vectored(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        vec: &mut [IoSliceMut<'_>],
    ) -> Poll<io::Result<usize>> {
        let polled = Pin::new(&mut self.read).poll_read_vectored(cx, vec);
        if let Poll::Ready(Ok(read)) = polled {
            self.outcome.read(read);
        }
        polled
    }
}

fn halves(stream: DialedStream) -> (BoxAsyncReadVectored, BoxAsyncWrite) {
    match stream {
        DialedStream::Socket(socket) => {
            let (read, write) = socket.into_split();
            (Box::new(read), Box::new(write))
        }
        DialedStream::Tunnel(stream) => {
            let (read, write) = tokio::io::split(stream);
            (Box::new(read.into_vectored_compat()), Box::new(write))
        }
    }
}

impl LegSockets {
    // The leg's datagram socket for `addr`; one the leg binds at its source
    // address is set for uTP with `utp`'s receive buffer when it holds one.
    fn bind_datagrams<'a>(
        &'a self,
        addr: SocketAddr,
        utp: Option<usize>,
    ) -> BoxFuture<'a, io::Result<Arc<dyn DatagramSocket>>> {
        if let Some(ip) = self.source {
            // The leg's route binds it at the leg's source address, as it
            // binds every socket the leg dials from.
            let requested = SocketAddr::new(ip, addr.port());
            let bound = self.dialer.bind_local_udp(requested);
            return Box::pin(async move {
                let socket = bound.unwrap_or_else(|| {
                    Err(io::Error::new(
                        io::ErrorKind::Unsupported,
                        "the leg binds no socket of its own",
                    ))
                })?;
                let socket = tokio::net::UdpSocket::from_std(socket)?;
                if let Some(receive_buffer) = utp {
                    weaver_torrent::network::set_for_utp(&socket, requested, receive_buffer);
                }
                Ok(Arc::new(socket) as Arc<dyn DatagramSocket>)
            });
        }
        let Some(carrier) = self.carrier() else {
            return Box::pin(async move {
                Ok(Arc::new(NoDatagrams::new(addr)) as Arc<dyn DatagramSocket>)
            });
        };
        shared(async move {
            let socket = carrier.bind(addr).await.map_err(io::Error::other)?;
            Ok(Arc::new(Carried::new(socket, addr, utp.is_some())) as Arc<dyn DatagramSocket>)
        })
    }
}

impl SocketFactory for LegSockets {
    fn tcp_room(&self) -> bool {
        self.dialer.has_room(&Purpose::Torrent)
    }

    fn datagram_room(&self) -> bool {
        self.dialer.has_room(&Purpose::Datagram)
    }

    fn connect_tcp<'a>(
        &'a self,
        addr: SocketAddr,
    ) -> BoxFuture<'a, io::Result<(BoxAsyncReadVectored, BoxAsyncWrite)>> {
        Box::pin(async move {
            let target = Target {
                host: addr.ip().to_string(),
                port: addr.port(),
                purpose: Purpose::Torrent,
                addresses: vec![addr.ip()],
            };
            // The dial runs as a task of its own: its future is not Sync,
            // and the session's are.
            let dialer = Arc::clone(&self.dialer);
            let mut dial = Abort(tokio::spawn(async move { dialer.dial(&target).await }));
            let dialed = (&mut dial.0)
                .await
                .map_err(io::Error::other)?
                .map_err(|error| io::Error::other(error.to_string()))?;
            let (read, write) = halves(dialed.stream);
            let read = Counted {
                read,
                outcome: dialed.outcome,
            };
            Ok((Box::new(read) as BoxAsyncReadVectored, write))
        })
    }

    fn listen_tcp<'a>(
        &'a self,
        addr: SocketAddr,
    ) -> BoxFuture<'a, io::Result<Box<dyn StreamListener>>> {
        self.listen(addr, false)
    }

    fn listen_peers_tcp<'a>(
        &'a self,
        addr: SocketAddr,
    ) -> BoxFuture<'a, io::Result<Box<dyn StreamListener>>> {
        self.listen(addr, true)
    }

    fn bind_udp<'a>(
        &'a self,
        addr: SocketAddr,
    ) -> BoxFuture<'a, io::Result<Arc<dyn DatagramSocket>>> {
        self.bind_datagrams(addr, None)
    }

    fn bind_utp<'a>(
        &'a self,
        addr: SocketAddr,
        receive_buffer: usize,
    ) -> BoxFuture<'a, io::Result<Arc<dyn DatagramSocket>>> {
        self.bind_datagrams(addr, Some(receive_buffer))
    }

    // Where the leg's route sends traffic toward `to` from, as its requests
    // leave.
    fn source_toward(&self, to: SocketAddr) -> Option<io::Result<IpAddr>> {
        self.dialer.source_toward(to)
    }

    fn gateway_socket<'a>(
        &'a self,
        local: SocketAddr,
    ) -> BoxFuture<'a, io::Result<Arc<dyn DatagramSocket>>> {
        gateway_socket(&self.dialer, local)
    }
}

// A socket in the leg's tunnel, as the session's datagram socket.
struct Carried {
    socket: Mutex<Option<Arc<dyn TunnelSocket>>>,
    bind_addr: SocketAddr,
    link_mtu: Option<u16>,
    sends: Option<Arc<CarriedSends>>,
    worker: Mutex<CarriedWorker>,
}

struct CarriedWorker {
    task: Option<tokio::task::JoinHandle<()>>,
    completion: Option<tokio::sync::watch::Receiver<Option<Result<(), String>>>>,
}

const CARRIED_SEND_PACKETS: usize = 256;
const CARRIED_SEND_BYTES: usize = 256 * 1024;

struct CarriedPacket {
    bytes: Vec<u8>,
    target: SocketAddr,
}

struct CarriedWaiter {
    key: usize,
    len: usize,
    waker: Waker,
}

#[derive(Default)]
struct CarriedPending {
    packets: VecDeque<CarriedPacket>,
    // Includes the packet currently owned by the worker.
    count: usize,
    bytes: usize,
    // Oldest first.
    waiters: VecDeque<CarriedWaiter>,
    stopped: bool,
    fault: Option<String>,
}

impl CarriedPending {
    fn fits(&self, count: usize, bytes: usize) -> bool {
        count < CARRIED_SEND_PACKETS && bytes <= CARRIED_SEND_BYTES
    }

    // The oldest waiters the free room admits. A waiter never overtakes an
    // older one that does not fit yet; one that loses its room to another
    // sender registers again.
    fn admitted(&mut self) -> Vec<Waker> {
        let (mut count, mut bytes) = (self.count, self.bytes);
        let mut admitted = Vec::new();
        while let Some(waiter) = self.waiters.front()
            && self.fits(count, bytes + waiter.len)
        {
            count += 1;
            bytes += waiter.len;
            admitted.extend(self.waiters.pop_front().map(|waiter| waiter.waker));
        }
        admitted
    }
}

// A send the carrier refuses for its destination alone, such as one in a
// family the tunnel has no address in. That packet is lost; the socket and
// every other destination keep going.
fn fails_only_its_target(error: &io::Error) -> bool {
    matches!(
        error.kind(),
        io::ErrorKind::InvalidInput
            | io::ErrorKind::AddrNotAvailable
            | io::ErrorKind::HostUnreachable
            | io::ErrorKind::NetworkUnreachable
    )
}

struct CarriedSends {
    pending: Mutex<CarriedPending>,
    ready: tokio::sync::Notify,
    terminal: tokio::sync::watch::Sender<bool>,
}

impl CarriedSends {
    fn error(pending: &CarriedPending) -> io::Error {
        io::Error::new(
            io::ErrorKind::BrokenPipe,
            pending
                .fault
                .clone()
                .unwrap_or_else(|| "the tunnel datagram socket stopped".into()),
        )
    }

    fn stop(&self, fault: Option<String>) {
        let waiters = {
            let mut pending = self.pending.lock().unwrap_or_else(PoisonError::into_inner);
            pending.stopped = true;
            if pending.fault.is_none() {
                pending.fault = fault;
            }
            pending.packets.clear();
            pending.bytes = 0;
            pending.count = 0;
            std::mem::take(&mut pending.waiters)
        };
        self.terminal.send_replace(true);
        self.ready.notify_one();
        for waiter in waiters {
            waiter.waker.wake();
        }
    }

    async fn run(self: Arc<Self>, socket: Arc<dyn TunnelSocket>) {
        loop {
            let ready = self.ready.notified();
            tokio::pin!(ready);
            ready.as_mut().enable();
            let packet = {
                let mut pending = self.pending.lock().unwrap_or_else(PoisonError::into_inner);
                if pending.stopped {
                    return;
                }
                pending.packets.pop_front()
            };
            let Some(packet) = packet else {
                ready.await;
                continue;
            };
            if let Err(error) = socket.send_to(&packet.bytes, packet.target).await
                && !fails_only_its_target(&error)
            {
                // A deferred error belongs to the accepted packet, not to a
                // later MTU probe. End this socket instead of misattributing it.
                self.stop(Some(format!("the tunnel datagram send failed: {error}")));
                return;
            }
            let waiters = {
                let mut pending = self.pending.lock().unwrap_or_else(PoisonError::into_inner);
                if pending.stopped {
                    return;
                }
                pending.count -= 1;
                pending.bytes -= packet.bytes.len();
                pending.admitted()
            };
            for waker in waiters {
                waker.wake();
            }
        }
    }
}

impl Carried {
    fn new(socket: Arc<dyn TunnelSocket>, requested: SocketAddr, utp: bool) -> Self {
        let sends = utp.then(|| {
            Arc::new(CarriedSends {
                pending: Mutex::new(CarriedPending::default()),
                ready: tokio::sync::Notify::new(),
                terminal: tokio::sync::watch::channel(false).0,
            })
        });
        let worker = sends
            .as_ref()
            .map(|sends| tokio::spawn(Arc::clone(sends).run(Arc::clone(&socket))));
        Self {
            bind_addr: socket.local_addr().unwrap_or(requested),
            link_mtu: socket.link_mtu(),
            socket: Mutex::new(Some(socket)),
            sends,
            worker: Mutex::new(CarriedWorker {
                task: worker,
                completion: None,
            }),
        }
    }

    fn socket(&self) -> io::Result<Arc<dyn TunnelSocket>> {
        self.socket
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
            .ok_or_else(|| {
                io::Error::new(
                    io::ErrorKind::BrokenPipe,
                    "the tunnel datagram socket stopped",
                )
            })
    }

    fn stop(&self) {
        if let Some(sends) = &self.sends {
            sends.stop(None);
        }
        self.socket
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take();
        if let Some(worker) = self
            .worker
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .task
            .as_ref()
        {
            worker.abort();
        }
    }
}

impl Drop for Carried {
    fn drop(&mut self) {
        self.stop();
    }
}

impl DatagramSocket for Carried {
    fn bind_addr(&self) -> SocketAddr {
        self.bind_addr
    }

    fn recv_from<'a>(
        &'a self,
        buf: &'a mut [u8],
    ) -> BoxFuture<'a, io::Result<(usize, SocketAddr)>> {
        shared(async move {
            let socket = self.socket()?;
            let Some(sends) = &self.sends else {
                return socket.recv_from(buf).await;
            };
            let mut terminal = sends.terminal.subscribe();
            let stopped = *terminal.borrow_and_update();
            if !stopped {
                tokio::select! {
                    result = socket.recv_from(buf) => return result,
                    _ = terminal.changed() => {}
                }
            }
            Err(CarriedSends::error(
                &sends.pending.lock().unwrap_or_else(PoisonError::into_inner),
            ))
        })
    }

    fn send_to<'a>(
        &'a self,
        buf: &'a [u8],
        target: SocketAddr,
    ) -> BoxFuture<'a, io::Result<usize>> {
        shared(async move {
            if let Some(mut sender) = self.send_handle() {
                return std::future::poll_fn(|cx| sender.poll_send_to(cx, buf, target)).await;
            }
            self.socket()?.send_to(buf, target).await?;
            Ok(buf.len())
        })
    }

    // The tunnel's MTU: a larger packet is lost inside it.
    fn link_mtu(&self) -> Option<u16> {
        self.link_mtu
    }

    fn dont_fragment(&self) -> bool {
        true
    }

    fn sender(&self) -> Option<Box<dyn DatagramSend>> {
        self.send_handle()
            .map(|sender| Box::new(sender) as Box<dyn DatagramSend>)
    }

    // Backpressure requires a caller-owned registration, so raw polling
    // cannot safely wait for queue capacity.
    fn poll_send_to(
        &self,
        _cx: &mut Context<'_>,
        _buf: &[u8],
        _target: SocketAddr,
    ) -> std::task::Poll<io::Result<usize>> {
        Poll::Ready(Err(io::Error::new(
            io::ErrorKind::Unsupported,
            "tunnel datagram polling requires an owned sender",
        )))
    }

    fn poll_send_to_vectored(
        &self,
        cx: &mut Context<'_>,
        _bufs: &[IoSlice<'_>],
        target: SocketAddr,
    ) -> Poll<io::Result<usize>> {
        self.poll_send_to(cx, &[], target)
    }

    fn shutdown(&self) -> BoxFuture<'_, io::Result<()>> {
        self.shutdown_worker()
    }
}

impl Carried {
    fn send_handle(&self) -> Option<CarriedSender> {
        self.sends.as_ref().map(|sends| CarriedSender {
            sends: Arc::clone(sends),
            link_mtu: self.link_mtu,
            registration: Arc::new(()),
        })
    }

    fn shutdown_worker(&self) -> BoxFuture<'_, io::Result<()>> {
        self.stop();
        let mut completion = {
            let mut worker = self.worker.lock().unwrap_or_else(PoisonError::into_inner);
            if worker.completion.is_none() {
                let (completed, completion) = tokio::sync::watch::channel(None);
                if let Some(task) = worker.task.take() {
                    // The coordinator owns the join independently of callers.
                    // Canceling a caller drops only its watch subscription.
                    tokio::spawn(async move {
                        let result = match task.await {
                            Ok(()) => Ok(()),
                            Err(error) if error.is_cancelled() => Ok(()),
                            Err(error) => Err(error.to_string()),
                        };
                        completed.send_replace(Some(result));
                    });
                } else {
                    completed.send_replace(Some(Ok(())));
                }
                worker.completion = Some(completion);
            }
            worker
                .completion
                .as_ref()
                .expect("shutdown completion initialized")
                .clone()
        };
        shared(async move {
            loop {
                let result = completion.borrow_and_update().clone();
                if let Some(result) = result {
                    return result.map_err(io::Error::other);
                }
                completion.changed().await.map_err(|_| {
                    io::Error::other("the tunnel datagram shutdown coordinator stopped")
                })?;
            }
        })
    }
}

struct CarriedSender {
    sends: Arc<CarriedSends>,
    link_mtu: Option<u16>,
    registration: Arc<()>,
}

impl CarriedSender {
    fn key(&self) -> usize {
        Arc::as_ptr(&self.registration) as usize
    }

    fn poll_enqueue(
        &mut self,
        cx: &mut Context<'_>,
        len: usize,
        target: SocketAddr,
        copy: impl FnOnce() -> Vec<u8>,
    ) -> Poll<io::Result<usize>> {
        let mut pending = self
            .sends
            .pending
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        let queued = pending
            .waiters
            .iter()
            .position(|waiter| waiter.key == self.key());
        let previous = queued.and_then(|at| pending.waiters.remove(at));
        let headers = if target.is_ipv4() { 28 } else { 48 };
        let limit = self.link_mtu.map_or(65535 - headers, |mtu| {
            usize::from(mtu).saturating_sub(headers)
        });
        let result = if pending.stopped {
            Poll::Ready(Err(CarriedSends::error(&pending)))
        } else if len > limit {
            Poll::Ready(Err(weaver_torrent::network::message_size_error()))
        } else if !pending.fits(pending.count, pending.bytes + len) {
            // A waiter polled again keeps its place in line.
            let at = queued.unwrap_or(pending.waiters.len());
            pending.waiters.insert(
                at,
                CarriedWaiter {
                    key: self.key(),
                    len,
                    waker: cx.waker().clone(),
                },
            );
            Poll::Pending
        } else {
            pending.packets.push_back(CarriedPacket {
                bytes: copy(),
                target,
            });
            pending.count += 1;
            pending.bytes += len;
            Poll::Ready(Ok(len))
        };
        drop(pending);
        drop(previous);
        if result.is_ready() {
            self.sends.ready.notify_one();
        }
        result
    }
}

impl Drop for CarriedSender {
    fn drop(&mut self) {
        let waiter = {
            let mut pending = self
                .sends
                .pending
                .lock()
                .unwrap_or_else(PoisonError::into_inner);
            let queued = pending
                .waiters
                .iter()
                .position(|waiter| waiter.key == self.key());
            queued.and_then(|at| pending.waiters.remove(at))
        };
        drop(waiter);
    }
}

impl DatagramSend for CarriedSender {
    // Success means local acceptance. The worker alone sends the bounded
    // FIFO, so async handshakes and polled data retain their packet order.
    fn poll_send_to_vectored(
        &mut self,
        cx: &mut Context<'_>,
        bufs: &[IoSlice<'_>],
        target: SocketAddr,
    ) -> Poll<io::Result<usize>> {
        let len = bufs
            .iter()
            .fold(0usize, |len, buf| len.saturating_add(buf.len()));
        self.poll_enqueue(cx, len, target, || {
            let mut joined = Vec::with_capacity(len);
            for buf in bufs {
                joined.extend_from_slice(buf);
            }
            joined
        })
    }
}

/// The names a session resolves on its leg: who resolves each is the leg's
/// naming, as for every other dial on the leg. A name the leg hands to its
/// proxy resolves at the exit, and no lookup is made here. The naming that
/// answers holds its answer for as long as the answer's TTL allows, so a
/// name asked again, as each announce asks its tracker's, is looked up once
/// while that holds; the answers go with the leg's naming when it changes.
pub struct LegNames(pub Arc<dyn Dialer>);

impl Resolver for LegNames {
    fn resolve<'a>(&'a self, host: &'a str, port: u16) -> BoxFuture<'a, io::Result<Resolved>> {
        shared(async move {
            let resolved = self.0.resolve(host).await;
            Ok(
                match resolved.map_err(|error| io::Error::other(error.to_string()))? {
                    Resolution::Addresses(addresses) => Resolved::Addresses(
                        addresses
                            .into_iter()
                            .map(|ip| SocketAddr::new(ip, port))
                            .collect(),
                    ),
                    Resolution::Unresolved => Resolved::AtExit,
                },
            )
        })
    }
}

// A task that ends when its handle is dropped.
struct Abort<T>(tokio::task::JoinHandle<T>);

impl<T> Drop for Abort<T> {
    fn drop(&mut self) {
        self.0.abort();
    }
}

pub(crate) struct Listener(pub(crate) tokio::net::TcpListener);

impl StreamListener for Listener {
    fn local_addr(&self) -> SocketAddr {
        self.0
            .local_addr()
            .unwrap_or_else(|_| SocketAddr::from(([0, 0, 0, 0], 0)))
    }

    fn accept<'a>(
        &'a self,
    ) -> BoxFuture<'a, io::Result<(SocketAddr, BoxAsyncReadVectored, BoxAsyncWrite)>> {
        Box::pin(async move {
            let (stream, peer) = self.0.accept().await?;
            let (read, write) = stream.into_split();
            Ok((
                peer,
                Box::new(read) as BoxAsyncReadVectored,
                Box::new(write) as BoxAsyncWrite,
            ))
        })
    }
}

// A listener inside the leg's tunnel. The leg counts the bytes each
// connection it accepts reads, as it counts those of the ones it dials.
struct Arriving(Box<dyn Listening>);

impl StreamListener for Arriving {
    fn local_addr(&self) -> SocketAddr {
        self.0.local_addr()
    }

    // Each word of the leg's gateway on the port it forwards, as a NAT-PMP
    // mapper's TCP report, until the listener goes.
    fn port_mappings(&self) -> Option<PortMapReceiver> {
        let mut mapping = self.0.mapping()?;
        let (reports, received) = tokio::sync::mpsc::unbounded_channel();
        tokio::spawn(async move {
            while mapping.changed().await.is_ok() {
                let now = mapping.borrow_and_update().clone();
                let report = PortMapped {
                    transport: PortmapTransport::NatPmp,
                    protocol: PortmapProtocol::Tcp,
                    local_port: now.external_port.unwrap_or(0),
                    external_port: now.external_port,
                    external_ip: now.external_ip,
                    error: now.error,
                };
                if reports.send(report).is_err() {
                    return;
                }
            }
        });
        Some(received)
    }

    fn accept<'a>(
        &'a self,
    ) -> BoxFuture<'a, io::Result<(SocketAddr, BoxAsyncReadVectored, BoxAsyncWrite)>> {
        shared(async move {
            let accepted = self.0.accept().await.map_err(io::Error::other)?;
            let peer = accepted
                .peer
                .unwrap_or_else(|| SocketAddr::from(([0, 0, 0, 0], 0)));
            let (read, write) = halves(accepted.stream);
            let read = Counted {
                read,
                outcome: accepted.outcome,
            };
            Ok((peer, Box::new(read) as BoxAsyncReadVectored, write))
        })
    }
}

/// The datagram socket of a leg that carries no UDP: nothing arrives and
/// every send is refused, so UDP trackers fail on their own.
pub struct NoDatagrams(SocketAddr);

impl NoDatagrams {
    pub fn new(addr: SocketAddr) -> Self {
        Self(addr)
    }
}

fn no_udp() -> io::Error {
    io::Error::new(io::ErrorKind::Unsupported, "this leg carries no UDP")
}

impl DatagramSocket for NoDatagrams {
    fn bind_addr(&self) -> SocketAddr {
        self.0
    }

    fn recv_from<'a>(
        &'a self,
        _buf: &'a mut [u8],
    ) -> BoxFuture<'a, io::Result<(usize, SocketAddr)>> {
        Box::pin(std::future::pending())
    }

    fn send_to<'a>(
        &'a self,
        _buf: &'a [u8],
        _target: SocketAddr,
    ) -> BoxFuture<'a, io::Result<usize>> {
        Box::pin(async { Err(no_udp()) })
    }

    fn poll_send_to(
        &self,
        _cx: &mut std::task::Context<'_>,
        _buf: &[u8],
        _target: SocketAddr,
    ) -> std::task::Poll<io::Result<usize>> {
        std::task::Poll::Ready(Err(no_udp()))
    }
}

/// The HTTP clients of a session on a leg: the one for no target downloads
/// metainfo, and a tracker or web seed is asked through one held to the
/// addresses its host was checked at. A ladder leg's clients ride `bridge`,
/// the leg's own; a direct leg's route is `dialer`'s, which says where a
/// request leaves and binds the port mapper's socket. They name no client:
/// each request names the client of the torrent it is for.
pub fn tracker_client(
    kind: &LegKind,
    dialer: Arc<dyn Dialer>,
    bridge: Option<PathBridge>,
) -> Result<Arc<dyn HttpClients>, String> {
    let bridge = match (kind, bridge) {
        (LegKind::Ladder, None) => return Err("a ladder leg needs its bridge".into()),
        (LegKind::Ladder, bridge) => bridge,
        _ => None,
    };
    let base = {
        let kind = kind.clone();
        move || match &kind {
            LegKind::Interface(name) => bound_to(reqwest::Client::builder(), name),
            kind => Ok(plain_builder(kind)),
        }
    };
    let clients = LegClients::new(base, bridge, kind.clone(), dialer);
    // A leg whose clients cannot be built does not open.
    let _ = clients
        .client(None, &ClientShape::FETCH)
        .map_err(|error| format!("{error:#}"))?;
    Ok(Arc::new(clients))
}

// A builder leaving from the leg's source address when it pins one, and
// held to no device.
fn plain_builder(kind: &LegKind) -> reqwest::ClientBuilder {
    let builder = reqwest::Client::builder();
    match kind {
        LegKind::SourceAddress(address) => builder.local_address(*address),
        _ => builder,
    }
}

// The host's loopback interface, for a leg bound to it.
#[cfg(test)]
pub(crate) const LOOPBACK_DEVICE: &str = if cfg!(target_os = "linux") {
    "lo"
} else {
    "lo0"
};

// A leg on the host's own sockets.
#[cfg(test)]
pub(crate) fn system_dialer() -> Arc<dyn Dialer> {
    Arc::new(weaver_tunnel::pipe::Revocable::new(Arc::new(
        weaver_tunnel::pipe::Egress::new(
            4,
            weaver_tunnel::egress::SocketEgress::System,
            std::time::Duration::from_secs(30),
        ),
    )))
}

/// A session's HTTP clients: each from `base`, through `bridge` on a leg
/// that has one, leaving as the leg's `kind` does. One client is built for
/// each target and shape, and handed back each time it is asked for again,
/// so an announce or a web seed request to a host the session asked before
/// builds no client (its TLS roots, its connector) again. The clients live
/// with the leg: a session on a new route opens a new leg, and builds its
/// own.
pub(crate) struct LegClients<F> {
    base: F,
    bridge: Option<PathBridge>,
    kind: LegKind,
    dialer: Arc<dyn Dialer>,
    kept: Mutex<Kept>,
    built: AtomicUsize,
}

// A client's target, its host, port and checked addresses, and its shape.
type ClientKey = (String, u16, Vec<IpAddr>, ClientShape);

// The most clients a leg keeps of each kind; past it the one asked for
// longest ago is dropped, and built again when next asked for.
const CLIENTS_KEPT: usize = 1024;

// A leg's clients: those one torrent's web seeds use apart from the rest,
// so however many torrents a queue holds, their clients never push out the
// leg's tracker, metainfo and gateway clients.
#[derive(Default)]
struct Kept {
    shared: Recent,
    per_torrent: Recent,
}

impl Kept {
    fn of(&mut self, shape: &ClientShape) -> &mut Recent {
        if shape.torrent.is_some() {
            &mut self.per_torrent
        } else {
            &mut self.shared
        }
    }
}

// Clients by key, each stamped with when it was last asked for.
#[derive(Default)]
struct Recent {
    clients: HashMap<ClientKey, (reqwest::Client, u64)>,
    asked: u64,
}

impl Recent {
    fn get(&mut self, key: &ClientKey) -> Option<reqwest::Client> {
        self.asked += 1;
        let (client, asked) = self.clients.get_mut(key)?;
        *asked = self.asked;
        Some(client.clone())
    }

    fn insert(&mut self, key: ClientKey, client: reqwest::Client) {
        if self.clients.len() >= CLIENTS_KEPT {
            let oldest = self
                .clients
                .iter()
                .min_by_key(|(_, (_, asked))| *asked)
                .map(|(key, _)| key.clone());
            if let Some(oldest) = oldest {
                self.clients.remove(&oldest);
            }
        }
        self.clients.insert(key, (client, self.asked));
    }
}

impl<F> LegClients<F>
where
    F: Fn() -> Result<reqwest::ClientBuilder, String> + Send + Sync + 'static,
{
    pub(crate) fn new(
        base: F,
        bridge: Option<PathBridge>,
        kind: LegKind,
        dialer: Arc<dyn Dialer>,
    ) -> Self {
        Self {
            base,
            bridge,
            kind,
            dialer,
            kept: Mutex::default(),
            built: AtomicUsize::new(0),
        }
    }

    /// How many clients the leg has built.
    #[cfg(test)]
    pub(crate) fn built(&self) -> usize {
        self.built.load(Ordering::Relaxed)
    }

    // A client for a target dials the host at an address it was checked
    // at, and follows no redirect. Through the leg's bridge, the checked
    // addresses are pinned on the leg's path and the bridge is handed the
    // name, so whichever rung dials keeps to them; with none checked, the
    // name goes on to the leg's exit, which resolves it. The gateway a port mapper asks
    // is on the local network: its client, the leg's otherwise, never goes
    // through a proxy. A client with no target looks no name up itself.
    fn builder(
        &self,
        target: Option<HttpTarget<'_>>,
        shape: &ClientShape,
    ) -> anyhow::Result<reqwest::ClientBuilder> {
        if shape.used_for == ClientUse::Gateway {
            // The port mapper asks the gateway whatever the leg's own
            // settings make of its builder.
            let builder = (self.base)().unwrap_or_else(|error| {
                tracing::debug!(%error, "the leg's builder failed; the gateway's client is built plain");
                plain_builder(&self.kind)
            });
            return shape.apply(builder.no_proxy().dns_resolver(Arc::new(NoLookups)));
        }
        let builder = (self.base)().map_err(anyhow::Error::msg)?;
        let (host, port, addrs) = target.map_or(("", 0, &[][..]), |target| {
            (target.host, target.port, target.addrs)
        });
        let builder = match &self.bridge {
            Some(bridge) => {
                if !addrs.is_empty() && host.parse::<IpAddr>().is_err() {
                    bridge.path.pin(host, addrs.to_vec());
                }
                crate::proxies::through_bridge(builder, &bridge.bridge)
                    .map_err(anyhow::Error::msg)?
            }
            None => crate::proxies::pinned(builder, host, port, addrs),
        };
        shape.apply(match target {
            None => builder.dns_resolver(Arc::new(NoLookups)),
            Some(_) => builder.redirect(reqwest::redirect::Policy::none()),
        })
    }
}

// The resolver of a leg's client with no target: it answers no name, so a
// name such a client meets is refused, never asked of the host's own DNS.
// Through the leg's bridge the name is handed to the leg, and this is never
// asked.
struct NoLookups;

impl reqwest::dns::Resolve for NoLookups {
    fn resolve(&self, name: reqwest::dns::Name) -> reqwest::dns::Resolving {
        let said = format!(
            "{}: a client with no target looks no name up",
            name.as_str()
        );
        Box::pin(async move { Err(said.into()) })
    }
}

impl<F> HttpClients for LegClients<F>
where
    F: Fn() -> Result<reqwest::ClientBuilder, String> + Send + Sync + 'static,
{
    #[expect(
        clippy::disallowed_methods,
        reason = "a torrent session's client, built once for its leg, target and shape"
    )]
    fn client(
        &self,
        target: Option<HttpTarget<'_>>,
        shape: &ClientShape,
    ) -> anyhow::Result<reqwest::Client> {
        let key = target.map_or_else(
            || (String::new(), 0, Vec::new(), shape.clone()),
            |target| {
                (
                    target.host.to_owned(),
                    target.port,
                    target.addrs.to_vec(),
                    shape.clone(),
                )
            },
        );
        // Built under the lock, so callers racing for one key build it once.
        let mut kept = self.kept.lock().unwrap_or_else(PoisonError::into_inner);
        let kept = kept.of(shape);
        if let Some(client) = kept.get(&key) {
            return Ok(client);
        }
        let client = self.builder(target, shape)?.build()?;
        self.built.fetch_add(1, Ordering::Relaxed);
        kept.insert(key, client.clone());
        Ok(client)
    }

    // A request leaves as the leg's sockets do: through the bridge from
    // wherever the proxy exits, else from where the leg's route sends it.
    fn egress(&self, to: SocketAddr) -> Egress {
        if self.bridge.is_some() || self.kind == LegKind::Ladder {
            return Egress::Proxied;
        }
        match self.dialer.source_toward(to) {
            Some(Ok(from)) => Egress::From(from),
            Some(Err(error)) => {
                tracing::debug!(%to, %error, "the leg has no route toward the address");
                Egress::NoRoute
            }
            None => Egress::Proxied,
        }
    }

    // The port mapper's socket, bound by the leg's route as its own
    // sockets are.
    fn gateway_socket<'a>(
        &'a self,
        local: SocketAddr,
    ) -> BoxFuture<'a, io::Result<Arc<dyn DatagramSocket>>> {
        gateway_socket(&self.dialer, local)
    }
}

// A socket at `local` that `dialer`'s route binds as it binds its own, for
// a port mapper to ask the gateway from.
fn gateway_socket<'a>(
    dialer: &Arc<dyn Dialer>,
    local: SocketAddr,
) -> BoxFuture<'a, io::Result<Arc<dyn DatagramSocket>>> {
    let bound = dialer.bind_local_udp(local);
    Box::pin(async move {
        let socket = bound.unwrap_or_else(|| {
            Err(io::Error::new(
                io::ErrorKind::Unsupported,
                "the leg binds no socket of its own",
            ))
        })?;
        Ok(Arc::new(tokio::net::UdpSocket::from_std(socket)?) as Arc<dyn DatagramSocket>)
    })
}

#[cfg(any(
    target_os = "android",
    target_os = "fuchsia",
    target_os = "illumos",
    target_os = "ios",
    target_os = "linux",
    target_os = "macos",
    target_os = "solaris",
    target_os = "tvos",
    target_os = "visionos",
    target_os = "watchos",
))]
fn bound_to(builder: reqwest::ClientBuilder, name: &str) -> Result<reqwest::ClientBuilder, String> {
    Ok(builder.interface(name))
}

#[cfg(not(any(
    target_os = "android",
    target_os = "fuchsia",
    target_os = "illumos",
    target_os = "ios",
    target_os = "linux",
    target_os = "macos",
    target_os = "solaris",
    target_os = "tvos",
    target_os = "visionos",
    target_os = "watchos",
)))]
fn bound_to(_: reqwest::ClientBuilder, _: &str) -> Result<reqwest::ClientBuilder, String> {
    Err("binding to an interface is not supported on this platform".into())
}

#[cfg(test)]
#[path = "net/carried_tests.rs"]
mod carried_tests;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proxies::Rung;

    fn ladder() -> LegPath {
        LegPath::Ladder {
            rungs: vec![Rung::Proxy { id: 1 }],
            direct_fallback: false,
        }
    }

    #[test]
    fn a_direct_leg_follows_its_egress_binding() {
        assert_eq!(
            LegKind::of(&EgressBinding::System, &LegPath::Direct),
            LegKind::System
        );
        assert_eq!(
            LegKind::of(
                &EgressBinding::Interface { name: "en1".into() },
                &LegPath::Direct
            ),
            LegKind::Interface("en1".into())
        );
        let address: IpAddr = "192.0.2.4".parse().unwrap();
        assert_eq!(
            LegKind::of(&EgressBinding::SourceAddress { address }, &LegPath::Direct),
            LegKind::SourceAddress(address)
        );
    }

    #[test]
    fn a_ladder_leg_is_a_ladder_on_any_egress() {
        for binding in [
            EgressBinding::System,
            EgressBinding::Interface { name: "en1".into() },
            EgressBinding::SourceAddress {
                address: "192.0.2.4".parse().unwrap(),
            },
        ] {
            assert_eq!(LegKind::of(&binding, &ladder()), LegKind::Ladder);
        }
    }

    #[test]
    fn dht_runs_wherever_the_leg_carries_udp() {
        for (kind, own_sockets, datagrams) in [
            (LegKind::System, true, true),
            (LegKind::Interface("en1".into()), true, true),
            (
                LegKind::SourceAddress("192.0.2.4".parse().unwrap()),
                false,
                true,
            ),
            (LegKind::Ladder, false, false),
        ] {
            let shape = kind.shape(None);
            assert_eq!(shape.own_sockets, own_sockets, "{kind:?}");
            assert_eq!(shape.datagrams, datagrams, "{kind:?}");
            assert_eq!(shape.dht, shape.datagrams, "{kind:?}");
        }
    }

    #[test]
    fn upnp_runs_only_on_system_egress() {
        assert!(LegKind::System.shape(None).upnp);
        assert!(!LegKind::Interface("en1".into()).shape(None).upnp);
        assert!(
            !LegKind::SourceAddress("192.0.2.4".parse().unwrap())
                .shape(None)
                .upnp
        );
        assert!(!LegKind::Ladder.shape(Some(&carrying())).upnp);
    }

    #[test]
    fn a_ladder_leg_carries_udp_only_when_its_dialer_does() {
        let shape = LegKind::Ladder.shape(Some(&loopback_egress()));
        assert!(!shape.listen && !shape.datagrams && !shape.dht && !shape.own_sockets);
        assert_eq!(LegKind::Ladder.listen_ip(), None);

        let shape = LegKind::Ladder.shape(Some(&carrying()));
        assert!(shape.datagrams && shape.dht);
        assert!(!shape.listen && !shape.upnp && !shape.own_sockets);
    }

    #[test]
    fn a_source_address_leg_listens_on_its_address() {
        let address: IpAddr = "192.0.2.4".parse().unwrap();
        let kind = LegKind::SourceAddress(address);
        assert!(kind.shape(None).listen);
        assert_eq!(kind.listen_ip(), Some(address));
        assert_eq!(kind.bind_device(), None);
        assert_eq!(LegKind::Interface("en1".into()).bind_device(), Some("en1"));
    }

    #[tokio::test]
    async fn no_datagrams_refuses_every_send() {
        let socket = NoDatagrams(SocketAddr::from(([127, 0, 0, 1], 0)));
        let error = socket
            .send_to(b"x", SocketAddr::from(([127, 0, 0, 1], 9)))
            .await
            .unwrap_err();
        assert_eq!(error.kind(), io::ErrorKind::Unsupported);
    }

    fn loopback_egress() -> Arc<dyn Dialer> {
        Arc::new(weaver_tunnel::pipe::Revocable::new(Arc::new(
            weaver_tunnel::pipe::Egress::new(
                4,
                weaver_tunnel::egress::SocketEgress::SourceAddress("127.0.0.1".parse().unwrap()),
                std::time::Duration::from_secs(30),
            ),
        )))
    }

    // A source-address leg's sockets: peers dialed through the leg, a
    // listener and a UDP socket bound to the address.
    #[tokio::test]
    async fn a_source_address_leg_dials_listens_and_carries_udp_on_its_address() {
        let loopback: IpAddr = "127.0.0.1".parse().unwrap();
        let sockets = LegSockets::new(loopback_egress(), &LegKind::SourceAddress(loopback));
        let listener = sockets
            .listen_tcp(SocketAddr::from(([0, 0, 0, 0], 0)))
            .await
            .unwrap();
        assert_eq!(listener.local_addr().ip(), loopback);
        let (_read, mut write) = sockets.connect_tcp(listener.local_addr()).await.unwrap();
        let (_, mut read, _) = listener.accept().await.unwrap();
        tokio::io::AsyncWriteExt::write_all(&mut write, b"peer")
            .await
            .unwrap();
        let mut said = [0u8; 4];
        tokio::io::AsyncReadExt::read_exact(&mut read, &mut said)
            .await
            .unwrap();
        assert_eq!(&said, b"peer");

        let udp = sockets
            .bind_udp(SocketAddr::from(([0, 0, 0, 0], 0)))
            .await
            .unwrap();
        assert_eq!(udp.bind_addr().ip(), loopback);
        let other = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
        udp.send_to(b"dht", other.local_addr().unwrap())
            .await
            .unwrap();
        let mut got = [0u8; 3];
        let (len, from) = other.recv_from(&mut got).await.unwrap();
        assert_eq!((&got[..len], from), (&b"dht"[..], udp.bind_addr()));
    }

    // The engine's port mapper asks the leg's sockets for its route and its
    // gateway socket, as it asks the leg's clients.
    #[tokio::test]
    async fn a_source_address_legs_sockets_give_the_mapper_its_route() {
        let loopback: IpAddr = "127.0.0.1".parse().unwrap();
        let sockets = LegSockets::new(loopback_egress(), &LegKind::SourceAddress(loopback));
        let gateway = SocketAddr::from(([127, 0, 0, 1], 5351));
        assert_eq!(sockets.source_toward(gateway).unwrap().unwrap(), loopback);
        let socket = sockets
            .gateway_socket(SocketAddr::new(loopback, 0))
            .await
            .unwrap();
        assert_eq!(socket.bind_addr().ip(), loopback);
    }

    // A tunnel's datagrams, each socket a loopback socket that stands for
    // one inside the tunnel.
    #[derive(Default)]
    struct Tunnel {
        bound: Mutex<Vec<SocketAddr>>,
    }

    struct TunnelUdp(tokio::net::UdpSocket);

    #[async_trait::async_trait]
    impl TunnelSocket for TunnelUdp {
        async fn send_to(&self, data: &[u8], target: SocketAddr) -> io::Result<()> {
            self.0.send_to(data, target).await.map(drop)
        }
        async fn recv_from(&self, buffer: &mut [u8]) -> io::Result<(usize, SocketAddr)> {
            self.0.recv_from(buffer).await
        }
        fn local_addr(&self) -> Option<SocketAddr> {
            self.0.local_addr().ok()
        }
        fn link_mtu(&self) -> Option<u16> {
            Some(1280)
        }
        fn is_closed(&self) -> bool {
            false
        }
        async fn closed(&self) {
            std::future::pending().await
        }
    }

    #[async_trait::async_trait]
    impl DatagramTransport for Tunnel {
        async fn resolve(&self, host: &str) -> Result<Vec<IpAddr>, weaver_tunnel::TunnelError> {
            Ok(match host {
                "router.example" => vec!["192.0.2.7".parse().unwrap()],
                _ => Vec::new(),
            })
        }
        async fn bind(
            &self,
            peer: SocketAddr,
        ) -> Result<Arc<dyn TunnelSocket>, weaver_tunnel::TunnelError> {
            self.bound.lock().unwrap().push(peer);
            let socket = tokio::net::UdpSocket::bind("127.0.0.1:0")
                .await
                .map_err(|error| weaver_tunnel::TunnelError::Engine(error.to_string()))?;
            Ok(Arc::new(TunnelUdp(socket)))
        }
    }

    // A rung whose tunnel carries datagrams.
    struct Carrying(Arc<Tunnel>);

    #[async_trait::async_trait]
    impl Dialer for Carrying {
        async fn dial(
            &self,
            _: &Target,
        ) -> Result<weaver_tunnel::pipe::Dialed, weaver_tunnel::pipe::DialError> {
            Err(weaver_tunnel::pipe::DialError::Skipped(
                "datagrams only".into(),
            ))
        }
        async fn resolve(&self, host: &str) -> Result<Resolution, weaver_tunnel::pipe::DialError> {
            let addresses = self.0.resolve(host).await.unwrap();
            Ok(Resolution::Addresses(addresses))
        }
        fn datagrams(self: Arc<Self>) -> Option<Arc<dyn DatagramTransport>> {
            Some(self.0.clone())
        }
        fn budget(&self) -> std::time::Duration {
            std::time::Duration::from_secs(1)
        }
        fn describe(&self) -> String {
            "carrying".into()
        }
    }

    fn carrying() -> Arc<dyn Dialer> {
        Arc::new(Carrying(Arc::default()))
    }

    #[test]
    fn ladder_utp_requires_a_datagram_carrier_even_without_incoming_connections() {
        let carried = LegKind::Ladder.shape(Some(&carrying()));
        assert!(carried.utp && carried.datagrams && !carried.listen);
        let stream_only = LegKind::Ladder.shape(Some(&loopback_egress()));
        assert!(!stream_only.utp && !stream_only.datagrams);
        assert!(!LegKind::Ladder.shape(None).utp);
    }

    #[tokio::test]
    async fn bind_utp_on_a_non_listening_ladder_uses_only_its_carrier() {
        let tunnel = Arc::new(Tunnel::default());
        let sockets = LegSockets::new(Arc::new(Carrying(tunnel.clone())), &LegKind::Ladder);
        let asked = SocketAddr::from(([0, 0, 0, 0], 6881));
        let udp = sockets.bind_utp(asked, 1024).await.unwrap();
        assert_eq!(*tunnel.bound.lock().unwrap(), [asked]);
        let peer = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let target = peer.local_addr().unwrap();
        let mut sender = udp.sender().unwrap();
        assert_eq!(
            std::future::poll_fn(|cx| sender.poll_send_to(cx, b"utp", target))
                .await
                .unwrap(),
            3
        );
        let mut bytes = [0; 3];
        let (len, from) = peer.recv_from(&mut bytes).await.unwrap();
        assert_eq!((&bytes[..len], from), (&b"utp"[..], udp.bind_addr()));
        udp.shutdown().await.unwrap();

        let blocked = LegSockets::new(loopback_egress(), &LegKind::Ladder)
            .bind_utp(asked, 1024)
            .await
            .unwrap();
        assert_eq!(
            blocked.send_to(b"utp", target).await.unwrap_err().kind(),
            io::ErrorKind::Unsupported
        );
    }

    #[tokio::test]
    async fn a_tunnel_datagram_socket_reports_that_packets_cannot_fragment() {
        let sockets = LegSockets::new(carrying(), &LegKind::Ladder);
        let udp = sockets
            .bind_udp(SocketAddr::from(([0, 0, 0, 0], 0)))
            .await
            .unwrap();
        assert!(udp.dont_fragment());
    }

    // A ladder leg whose tunnel carries datagrams: the session's UDP binds
    // inside the tunnel, and its names resolve there.
    #[tokio::test]
    async fn a_ladder_leg_carries_udp_through_its_tunnel() {
        let tunnel = Arc::new(Tunnel::default());
        let sockets = LegSockets::new(Arc::new(Carrying(tunnel.clone())), &LegKind::Ladder);
        let asked = SocketAddr::from(([0, 0, 0, 0], 6881));
        let udp = sockets.bind_udp(asked).await.unwrap();
        assert_eq!(*tunnel.bound.lock().unwrap(), [asked]);
        assert_eq!(udp.bind_addr().ip(), IpAddr::from([127, 0, 0, 1]));
        assert_eq!(udp.link_mtu(), Some(1280), "the tunnel's MTU");
        assert!(udp.dont_fragment());

        let peer = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let sent = udp
            .send_to(b"dht", peer.local_addr().unwrap())
            .await
            .unwrap();
        assert_eq!(sent, 3);
        let mut got = [0u8; 3];
        let (len, from) = peer.recv_from(&mut got).await.unwrap();
        assert_eq!((&got[..len], from), (&b"dht"[..], udp.bind_addr()));

        peer.send_to(b"pong", udp.bind_addr()).await.unwrap();
        let mut back = [0u8; 4];
        let (len, from) = udp.recv_from(&mut back).await.unwrap();
        assert_eq!(
            (&back[..len], from),
            (&b"pong"[..], peer.local_addr().unwrap())
        );

        let resolver = LegNames(Arc::new(Carrying(tunnel.clone())));
        assert_eq!(
            resolver.resolve("router.example", 6881).await.unwrap(),
            Resolved::Addresses(vec![SocketAddr::from(([192, 0, 2, 7], 6881))])
        );
    }

    // A ladder leg's sockets: peers dialed through the leg, and neither a
    // listener nor UDP when its dialer carries no datagrams.
    #[tokio::test]
    async fn a_ladder_leg_dials_only() {
        let sockets = LegSockets::new(loopback_egress(), &LegKind::Ladder);
        let error = match sockets
            .listen_tcp(SocketAddr::from(([0, 0, 0, 0], 0)))
            .await
        {
            Ok(_) => panic!("a ladder leg listened"),
            Err(error) => error,
        };
        assert_eq!(error.kind(), io::ErrorKind::Unsupported);
        let udp = sockets
            .bind_udp(SocketAddr::from(([127, 0, 0, 1], 0)))
            .await
            .unwrap();
        let refused = udp
            .send_to(b"x", SocketAddr::from(([127, 0, 0, 1], 9)))
            .await
            .unwrap_err();
        assert_eq!(refused.kind(), io::ErrorKind::Unsupported);
        let peer = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let (_read, _write) = sockets
            .connect_tcp(peer.local_addr().unwrap())
            .await
            .unwrap();
        peer.accept().await.unwrap();
    }

    // A ladder whose last hop takes connections, here on a loopback
    // listener standing in for one inside a tunnel.
    #[derive(Default)]
    struct Accepting(Arc<std::sync::atomic::AtomicUsize>);
    struct LoopbackInbound(Arc<std::sync::atomic::AtomicUsize>);
    struct LoopbackListener(tokio::net::TcpListener, Arc<std::sync::atomic::AtomicUsize>);
    #[async_trait::async_trait]
    impl weaver_tunnel::pipe::Inbound for LoopbackInbound {
        async fn listen(
            &self,
            port: u16,
            _: bool,
        ) -> Result<Box<dyn Listening>, weaver_tunnel::TunnelError> {
            let listener = tokio::net::TcpListener::bind(SocketAddr::from(([127, 0, 0, 1], port)))
                .await
                .map_err(|error| weaver_tunnel::TunnelError::Engine(error.to_string()))?;
            Ok(Box::new(LoopbackListener(listener, self.0.clone())))
        }
    }
    #[async_trait::async_trait]
    impl Listening for LoopbackListener {
        fn local_addr(&self) -> SocketAddr {
            self.0.local_addr().unwrap()
        }
        async fn accept(&self) -> Result<weaver_tunnel::pipe::Dialed, weaver_tunnel::TunnelError> {
            let (stream, peer) = self
                .0
                .accept()
                .await
                .map_err(|error| weaver_tunnel::TunnelError::Engine(error.to_string()))?;
            let outcome = Arc::new(ConnectionOutcome::default());
            let reads = self.1.clone();
            outcome.on_read(move |bytes| {
                reads.fetch_add(bytes, std::sync::atomic::Ordering::SeqCst);
            });
            Ok(weaver_tunnel::pipe::Dialed {
                stream: DialedStream::Socket(stream),
                outcome,
                path: weaver_tunnel::pipe::DialPath::default(),
                peer: Some(peer),
                source: Some(self.local_addr()),
                setup: None,
            })
        }
    }
    #[async_trait::async_trait]
    impl Dialer for Accepting {
        async fn dial(
            &self,
            _: &Target,
        ) -> Result<weaver_tunnel::pipe::Dialed, weaver_tunnel::pipe::DialError> {
            Err(weaver_tunnel::pipe::DialError::Skipped(
                "takes connections only".into(),
            ))
        }
        fn inbound(self: Arc<Self>) -> Option<Arc<dyn weaver_tunnel::pipe::Inbound>> {
            Some(Arc::new(LoopbackInbound(self.0.clone())))
        }
        fn budget(&self) -> std::time::Duration {
            std::time::Duration::from_secs(1)
        }
        fn describe(&self) -> String {
            "accepting".into()
        }
    }

    // A ladder leg whose last hop takes connections listens through it, and
    // runs no uTP; one whose last hop does not, never listens.
    #[test]
    fn a_ladder_leg_listens_exactly_when_its_last_hop_takes_connections() {
        let accepting: Arc<dyn Dialer> = Arc::new(Accepting::default());
        let shape = LegKind::Ladder.shape(Some(&accepting));
        assert!(shape.listen && !shape.utp && !shape.upnp && !shape.own_sockets);
        for dialer in [loopback_egress(), carrying()] {
            assert!(!LegKind::Ladder.shape(Some(&dialer)).listen);
        }
        assert!(!LegKind::Ladder.shape(None).listen);
        for kind in [
            LegKind::System,
            LegKind::Interface("en1".into()),
            LegKind::SourceAddress("192.0.2.4".parse().unwrap()),
        ] {
            assert!(kind.shape(None).utp, "{kind:?}");
        }
    }

    // A peer that connects in through the leg reaches the session, which
    // sees its address, the port it listens on, and counts what it reads.
    #[tokio::test]
    async fn a_ladder_leg_takes_peers_through_its_last_hop_and_counts_their_bytes() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        let accepting = Arc::new(Accepting::default());
        let sockets = LegSockets::new(accepting.clone(), &LegKind::Ladder);
        let listener = sockets
            .listen_tcp(SocketAddr::from(([0, 0, 0, 0], 0)))
            .await
            .unwrap();
        let local = listener.local_addr();
        assert_eq!(local.ip(), IpAddr::from([127, 0, 0, 1]));
        let (accepted, peer) =
            tokio::join!(listener.accept(), tokio::net::TcpStream::connect(local));
        let (from, mut read, mut write) = accepted.unwrap();
        let mut peer = peer.unwrap();
        assert_eq!(from, peer.local_addr().unwrap());
        peer.write_all(b"interested").await.unwrap();
        let mut heard = [0u8; 10];
        read.read_exact(&mut heard).await.unwrap();
        assert_eq!(&heard, b"interested");
        assert_eq!(accepting.0.load(std::sync::atomic::Ordering::SeqCst), 10);
        write.write_all(b"unchoke").await.unwrap();
        let mut answer = [0u8; 7];
        peer.read_exact(&mut answer).await.unwrap();
        assert_eq!(&answer, b"unchoke");
    }

    // One WireGuard hop of a chain, as the runtime builds it: over the
    // loopback egress for the first hop, else inside the hop beneath.
    fn wireguard_hop(
        id: u32,
        spec: weaver_tunnel::WireGuardSpec,
        beneath: Option<&Arc<weaver_tunnel::pipe::SessionHop>>,
        forwarding: weaver_tunnel::pipe::PortForwarding,
    ) -> Arc<weaver_tunnel::pipe::SessionHop> {
        let egress = Arc::new(weaver_tunnel::pipe::Egress::new(
            4,
            weaver_tunnel::egress::SocketEgress::SourceAddress("127.0.0.1".parse().unwrap()),
            std::time::Duration::from_secs(30),
        ));
        let provider = weaver_tunnel::WireGuardTunnelProvider::new(
            spec,
            Arc::new(weaver_tunnel::NoopTunnelObserver),
        );
        let (provider, inner, proxies): (_, Arc<dyn Dialer>, Vec<u32>) = match beneath {
            None => (provider.with_udp_factory(egress.clone()), egress, vec![]),
            Some(beneath) => (
                provider.with_datagram_transport(
                    beneath
                        .clone()
                        .datagrams()
                        .expect("a WireGuard hop carries datagrams"),
                ),
                beneath.clone(),
                [beneath.path.proxies.clone(), vec![beneath.id]].concat(),
            ),
        };
        let provider = Arc::new(provider);
        Arc::new(weaver_tunnel::pipe::SessionHop {
            sockets: Default::default(),
            capacity: weaver_tunnel::pipe::SessionCapacity::new(Arc::new(
                tokio::sync::Semaphore::new(1),
            )),
            activity: Default::default(),
            id,
            provider: provider.clone(),
            inner,
            transport: None,
            path: weaver_tunnel::pipe::DialPath {
                egress: 4,
                proxies,
                ..Default::default()
            },
            endpoint: None,
            timeout: std::time::Duration::from_secs(30),
            naming: weaver_tunnel::pipe::Naming::proxy_servers(vec![
                weaver_tunnel::test_support::TEST_PEER_ADDRESS.into(),
            ]),
            resolver: Some(provider),
            forwarding,
            failure: Default::default(),
        })
    }

    // A ladder leg whose one rung is a chain of three WireGuard hops, each
    // carried inside the one before, listens on the chain's last hop and
    // announces the port that hop's gateway forwards over NAT-PMP; a peer
    // beyond the last hop connects in on that port.
    #[tokio::test]
    async fn a_ladder_leg_over_a_three_hop_wireguard_chain_listens_on_its_last_hop() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};
        use weaver_tunnel::pipe::PortForwarding;
        use weaver_tunnel::test_support::{
            TEST_CLIENT_ADDRESS, TEST_PEER_ADDRESS, WireGuardTestPeer, WireGuardTestPeerOptions,
            natpmp::{NatPmpGateway, PUBLIC_ADDRESS},
            test_key,
        };
        const CHAINED_PORT: u16 = 51820;
        const MAPPED: u16 = 47002;
        let gateway = NatPmpGateway::start(MAPPED).await;
        let exit = WireGuardTestPeer::start_with(WireGuardTestPeerOptions {
            private_key: test_key(53),
            udp_forward: Some((5351, gateway.address())),
            ..WireGuardTestPeerOptions::default()
        })
        .await;
        let middle = WireGuardTestPeer::start_with(WireGuardTestPeerOptions {
            private_key: test_key(52),
            udp_forward: Some((CHAINED_PORT, exit.endpoint())),
            ..WireGuardTestPeerOptions::default()
        })
        .await;
        let entry = WireGuardTestPeer::start_with(WireGuardTestPeerOptions {
            private_key: test_key(51),
            udp_forward: Some((CHAINED_PORT, middle.endpoint())),
            ..WireGuardTestPeerOptions::default()
        })
        .await;
        let chained = |peer: &WireGuardTestPeer, id: &str| weaver_tunnel::WireGuardSpec {
            endpoint_host: TEST_PEER_ADDRESS.to_string(),
            endpoint_port: CHAINED_PORT,
            ..peer.client_spec(id)
        };
        let first = wireguard_hop(1, entry.client_spec("1"), None, PortForwarding::None);
        let second = wireguard_hop(2, chained(&middle, "2"), Some(&first), PortForwarding::None);
        let last = wireguard_hop(
            3,
            chained(&exit, "3"),
            Some(&second),
            PortForwarding::NatPmp {
                gateway: weaver_tunnel::pipe::gateway_of(TEST_CLIENT_ADDRESS),
            },
        );
        let rung: Arc<dyn Dialer> = last;
        let leg: Arc<dyn Dialer> = Arc::new(weaver_tunnel::pipe::Revocable::new(Arc::new(
            weaver_tunnel::pipe::Fallback::new(vec![rung]),
        )));

        let shape = LegKind::Ladder.shape(Some(&leg));
        assert!(shape.listen && shape.utp);

        let sockets = LegSockets::new(leg, &LegKind::Ladder);
        let listener = sockets
            .listen_peers_tcp(SocketAddr::from(([0, 0, 0, 0], 0)))
            .await
            .unwrap();
        assert_eq!(listener.local_addr().ip(), IpAddr::V4(TEST_CLIENT_ADDRESS));
        let mut reports = listener
            .port_mappings()
            .expect("the last hop's gateway forwards a port");
        let report = reports.recv().await.unwrap();
        assert_eq!(report.transport, PortmapTransport::NatPmp);
        assert_eq!(report.protocol, PortmapProtocol::Tcp);
        assert_eq!(report.external_port, Some(MAPPED));
        assert_eq!(report.external_ip, Some(IpAddr::V4(PUBLIC_ADDRESS)));
        assert_eq!(report.error, None);

        // The listener moves to the forwarded port on its next accept; the
        // peer dials once it says it listens there.
        let (accepted, peer) = tokio::join!(listener.accept(), async {
            while listener.local_addr().port() != MAPPED {
                tokio::task::yield_now().await;
            }
            exit.dial_client(MAPPED).await
        });
        let (from, mut read, mut write) = accepted.unwrap();
        let mut peer = peer.unwrap();
        assert_eq!(from.ip(), IpAddr::V4(TEST_PEER_ADDRESS));
        peer.write_all(b"interested").await.unwrap();
        let mut heard = [0u8; 10];
        read.read_exact(&mut heard).await.unwrap();
        assert_eq!(&heard, b"interested");
        write.write_all(b"unchoke").await.unwrap();
        let mut answer = [0u8; 7];
        peer.read_exact(&mut answer).await.unwrap();
        assert_eq!(&answer, b"unchoke");

        // The outer hops' far sides find nothing on that port.
        assert!(entry.dial_client(MAPPED).await.is_err());
        assert!(middle.dial_client(MAPPED).await.is_err());

        // The SSL listener is not the one peers reach through the forwarded
        // port: it listens where it asked and reports no mapping.
        let other = sockets
            .listen_tcp(SocketAddr::from(([0, 0, 0, 0], 0)))
            .await
            .unwrap();
        assert_ne!(other.local_addr().port(), MAPPED);
        assert!(other.port_mappings().is_none());
    }

    // A dialer whose naming rule answers one address, or hands names on.
    struct Naming(Option<IpAddr>);

    #[async_trait::async_trait]
    impl Dialer for Naming {
        async fn dial(
            &self,
            _: &Target,
        ) -> Result<weaver_tunnel::pipe::Dialed, weaver_tunnel::pipe::DialError> {
            Err(weaver_tunnel::pipe::DialError::Skipped("names only".into()))
        }
        async fn resolve(&self, _: &str) -> Result<Resolution, weaver_tunnel::pipe::DialError> {
            Ok(self
                .0
                .map_or(Resolution::Unresolved, |ip| Resolution::Addresses(vec![ip])))
        }
        fn budget(&self) -> std::time::Duration {
            std::time::Duration::from_secs(1)
        }
        fn describe(&self) -> String {
            "naming".into()
        }
    }

    // A leg resolves every name the session looks up by its dialer's
    // naming, and a name its dialer hands on resolves at the exit: no
    // lookup is made here, of the host's resolver or any other.
    #[tokio::test]
    async fn a_leg_resolves_names_through_its_dialer_and_hands_the_rest_on() {
        let answer: IpAddr = "192.0.2.9".parse().unwrap();
        let resolver = LegNames(Arc::new(Naming(Some(answer))));
        assert_eq!(
            resolver.resolve("router.test", 6881).await.unwrap(),
            Resolved::Addresses(vec![SocketAddr::new(answer, 6881)])
        );

        let host_lookups = || {
            let metrics = weaver_tunnel::metrics::snapshot();
            let resolved: u64 = metrics
                .resolutions
                .iter()
                .filter(|(by, ..)| *by == weaver_tunnel::metrics::Resolver::Host)
                .map(|(_, ok, failed)| ok + failed)
                .sum();
            let held: u64 = metrics
                .cache
                .iter()
                .filter(|(by, ..)| *by == weaver_tunnel::metrics::Resolver::Host)
                .map(|(.., count)| count)
                .sum();
            resolved + held
        };
        let before = host_lookups();
        let resolver = LegNames(Arc::new(Naming(None)));
        assert_eq!(
            resolver.resolve("localhost", 6881).await.unwrap(),
            Resolved::AtExit
        );
        assert_eq!(host_lookups(), before);
    }

    #[test]
    fn a_ladder_client_needs_its_bridge() {
        assert!(tracker_client(&LegKind::Ladder, system_dialer(), None).is_err());
        assert!(tracker_client(&LegKind::System, system_dialer(), None).is_ok());
    }

    // A leg builds one client for each target and shape and hands it to
    // every request after: announces and web seed fetches to one host build
    // one each. Other checked addresses build another, and a session on a
    // new route, which opens a new leg, builds its own.
    #[test]
    fn a_leg_builds_each_client_once() {
        let leg = || {
            LegClients::new(
                || Ok(reqwest::Client::builder().no_proxy()),
                None,
                LegKind::System,
                system_dialer(),
            )
        };
        let clients = leg();
        let checked = [IpAddr::from([127, 0, 0, 1])];
        let target = || {
            Some(HttpTarget {
                host: "tracker.test",
                port: 6969,
                addrs: &checked,
            })
        };
        let announce = ClientShape::of(ClientUse::Tracker { title_case: false });
        for _ in 0..16 {
            clients.client(target(), &announce).unwrap();
            clients.client(target(), &ClientShape::FETCH).unwrap();
        }
        assert_eq!(clients.built(), 2);

        let moved = [IpAddr::from([127, 0, 0, 2])];
        let elsewhere = HttpTarget {
            host: "tracker.test",
            port: 6969,
            addrs: &moved,
        };
        clients.client(Some(elsewhere), &announce).unwrap();
        clients.client(Some(elsewhere), &announce).unwrap();
        assert_eq!(clients.built(), 3);

        let next = leg();
        next.client(target(), &announce).unwrap();
        assert_eq!(next.built(), 1);
        assert_eq!(clients.built(), 3);
    }

    // A web seed's client is its torrent's alone: a second ask for one
    // torrent's seed takes the client built for it, another torrent's seed
    // on the same host gets its own, so their requests never share a
    // connection.
    #[test]
    fn a_web_seed_client_is_its_torrents_alone() {
        let clients = LegClients::new(
            || Ok(reqwest::Client::builder().no_proxy()),
            None,
            LegKind::System,
            system_dialer(),
        );
        let checked = [IpAddr::from([127, 0, 0, 1])];
        let target = || {
            Some(HttpTarget {
                host: "seed.test",
                port: 80,
                addrs: &checked,
            })
        };
        let one = ClientShape::FETCH.for_torrent(weaver_torrent::core::Id20::new([1; 20]));
        let other = ClientShape::FETCH.for_torrent(weaver_torrent::core::Id20::new([2; 20]));
        clients.client(target(), &one).unwrap();
        clients.client(target(), &one).unwrap();
        assert_eq!(clients.built(), 1);
        clients.client(target(), &other).unwrap();
        assert_eq!(clients.built(), 2);
    }

    // Callers asking at once for one client get the one built for the
    // first of them.
    #[test]
    fn callers_asking_at_once_build_one_client() {
        let clients = LegClients::new(
            || Ok(reqwest::Client::builder().no_proxy()),
            None,
            LegKind::System,
            system_dialer(),
        );
        let checked = [IpAddr::from([127, 0, 0, 1])];
        let announce = ClientShape::of(ClientUse::Tracker { title_case: false });
        std::thread::scope(|scope| {
            for _ in 0..8 {
                scope.spawn(|| {
                    clients
                        .client(
                            Some(HttpTarget {
                                host: "tracker.test",
                                port: 6969,
                                addrs: &checked,
                            }),
                            &announce,
                        )
                        .unwrap();
                });
            }
        });
        assert_eq!(clients.built(), 1);
    }

    // A queue's web seeds, one client per torrent past the most a leg
    // keeps, drop the client asked for longest ago, keep every other, and
    // leave the tracker's alone.
    #[test]
    fn web_seed_clients_never_push_out_the_trackers() {
        let clients = LegClients::new(
            || Ok(reqwest::Client::builder().no_proxy()),
            None,
            LegKind::System,
            system_dialer(),
        );
        let checked = [IpAddr::from([127, 0, 0, 1])];
        let target = |host| {
            Some(HttpTarget {
                host,
                port: 80,
                addrs: &checked,
            })
        };
        let announce = ClientShape::of(ClientUse::Tracker { title_case: false });
        let seed = |n: usize| {
            let mut hash = [0u8; 20];
            hash[..8].copy_from_slice(&(n as u64).to_be_bytes());
            ClientShape::FETCH.for_torrent(weaver_torrent::core::Id20::new(hash))
        };
        clients.client(target("tracker.test"), &announce).unwrap();
        for n in 1..=CLIENTS_KEPT {
            clients.client(target("seed.test"), &seed(n)).unwrap();
        }
        // The first web seed is asked for again, so the second is now the
        // one asked for longest ago.
        clients.client(target("seed.test"), &seed(1)).unwrap();
        assert_eq!(clients.built(), CLIENTS_KEPT + 1);
        clients
            .client(target("seed.test"), &seed(CLIENTS_KEPT + 1))
            .unwrap();
        let built = clients.built();
        assert_eq!(built, CLIENTS_KEPT + 2);

        // The tracker's client is still kept, and so is every web seed's but
        // the second's, the first's included.
        clients.client(target("tracker.test"), &announce).unwrap();
        for n in std::iter::once(1).chain(3..=CLIENTS_KEPT + 1) {
            clients.client(target("seed.test"), &seed(n)).unwrap();
            assert_eq!(clients.built(), built, "web seed {n}");
        }
        // The second was dropped, and is built again.
        clients.client(target("seed.test"), &seed(2)).unwrap();
        assert_eq!(clients.built(), built + 1);
    }

    // Two legs asked for the same target and shape each build their own
    // client, from one builder that counts what it is asked for: a client
    // built for one leg is never handed to the other.
    #[test]
    fn legs_never_share_a_client() {
        let builds = Arc::new(AtomicUsize::new(0));
        let leg = || {
            let builds = Arc::clone(&builds);
            LegClients::new(
                move || {
                    builds.fetch_add(1, Ordering::Relaxed);
                    Ok(reqwest::Client::builder().no_proxy())
                },
                None,
                LegKind::System,
                system_dialer(),
            )
        };
        let checked = [IpAddr::from([127, 0, 0, 1])];
        let target = || {
            Some(HttpTarget {
                host: "seed.test",
                port: 80,
                addrs: &checked,
            })
        };
        let last = leg();
        last.client(target(), &ClientShape::FETCH).unwrap();
        last.client(target(), &ClientShape::FETCH).unwrap();
        assert_eq!(builds.load(Ordering::Relaxed), 1);
        let next = leg();
        next.client(target(), &ClientShape::FETCH).unwrap();
        assert_eq!(builds.load(Ordering::Relaxed), 2);
        assert_eq!((last.built(), next.built()), (1, 1));
    }

    // A leg's client for a web seed's target dials the host at the checked
    // address without looking the name up, and hands a redirect back
    // instead of following it.
    #[tokio::test(start_paused = true)]
    async fn a_target_client_dials_the_checked_address_and_follows_nothing() {
        use tokio::io::{AsyncReadExt, AsyncWriteExt};

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let served = tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            let mut head = Vec::new();
            let mut buf = [0u8; 1024];
            while !head.ends_with(b"\r\n\r\n") {
                let read = stream.read(&mut buf).await.unwrap();
                assert!(read > 0, "the request ended early");
                head.extend_from_slice(&buf[..read]);
            }
            stream
                .write_all(
                    b"HTTP/1.1 302 Found\r\nLocation: http://elsewhere.test/\r\n\
                      Content-Length: 0\r\nConnection: close\r\n\r\n",
                )
                .await
                .unwrap();
            String::from_utf8(head).unwrap()
        });

        // A direct leg's builder, kept off any proxy the host names.
        let clients = LegClients::new(
            || Ok(reqwest::Client::builder().no_proxy()),
            None,
            LegKind::System,
            system_dialer(),
        );
        let client = clients
            .client(
                Some(HttpTarget {
                    host: "seed.test",
                    port,
                    addrs: &[IpAddr::from([127, 0, 0, 1])],
                }),
                &ClientShape::FETCH,
            )
            .unwrap();
        let response = client
            .get(format!("http://seed.test:{port}/f"))
            .send()
            .await
            .unwrap();
        assert_eq!(response.status(), reqwest::StatusCode::FOUND);
        let head = served.await.unwrap();
        assert!(head.starts_with("GET /f HTTP/1.1\r\n"), "{head}");
        assert!(
            head.to_ascii_lowercase()
                .contains(&format!("host: seed.test:{port}\r\n")),
            "{head}"
        );
    }

    // A leg's client with no target, the gateway's among them, looks no
    // name up: a URL naming a host fails before any lookup, of reqwest's own
    // resolver or the host's DNS, with the refusal and not a failed connect.
    #[tokio::test]
    async fn a_client_with_no_target_looks_no_name_up() {
        let port = 9;
        let clients = LegClients::new(
            || Ok(reqwest::Client::builder().no_proxy()),
            None,
            LegKind::System,
            system_dialer(),
        );
        let host_lookups = || {
            weaver_tunnel::metrics::snapshot()
                .resolutions
                .iter()
                .filter(|(by, ..)| *by == weaver_tunnel::metrics::Resolver::Host)
                .map(|(_, ok, failed)| ok + failed)
                .sum::<u64>()
        };
        for shape in [ClientShape::FETCH, ClientShape::GATEWAY] {
            let before = host_lookups();
            let client = clients.client(None, &shape).unwrap();
            let error = client
                .get(format!("http://localhost:{port}/"))
                .send()
                .await
                .unwrap_err();
            let said = format!("{:?}", anyhow::Error::from(error));
            assert!(
                said.contains("a client with no target looks no name up"),
                "{said}"
            );
            assert_eq!(host_lookups(), before);
        }
    }

    // A leg whose WireGuard session has its TCP peer share full says it has
    // no TCP room, so peers connect over uTP; it has room again once a peer
    // closes. A leg on the kernel's sockets holds no share and always has
    // room.
    #[tokio::test]
    async fn a_leg_has_tcp_room_while_its_tcp_peer_share_does() {
        use weaver_tunnel::pipe::{PortForwarding, SessionSockets, SocketShares};
        use weaver_tunnel::test_support::{
            TEST_PEER_ADDRESS, TEST_PEER_HTTP_PORT, WireGuardTestPeer,
        };
        let peer = WireGuardTestPeer::start().await;
        let hop = wireguard_hop(1, peer.client_spec("1"), None, PortForwarding::None);
        let hop = Arc::new(weaver_tunnel::pipe::SessionHop {
            sockets: Arc::new(SessionSockets::new(SocketShares {
                nntp: 0,
                torrent: 1,
                core: 1,
                http: 1,
            })),
            ..Arc::into_inner(hop).unwrap()
        });
        let rung: Arc<dyn Dialer> = hop;
        let leg: Arc<dyn Dialer> = Arc::new(weaver_tunnel::pipe::Revocable::new(Arc::new(
            weaver_tunnel::pipe::Fallback::new(vec![rung]),
        )));
        let sockets = LegSockets::new(leg, &LegKind::Ladder);
        assert!(sockets.tcp_room());
        let address = SocketAddr::new(TEST_PEER_ADDRESS.into(), TEST_PEER_HTTP_PORT);
        let connected = sockets.connect_tcp(address).await.unwrap();
        assert!(!sockets.tcp_room());
        drop(connected);
        assert!(sockets.tcp_room());

        let kernel = LegSockets::new(loopback_egress(), &LegKind::Ladder);
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let mut held = Vec::new();
        for _ in 0..4 {
            held.push(
                kernel
                    .connect_tcp(listener.local_addr().unwrap())
                    .await
                    .unwrap(),
            );
            assert!(kernel.tcp_room());
        }
    }
}

/// What a session on one leg shows the trackers and peers it reaches.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Exposure {
    /// Who resolves the names the session looks up.
    pub resolved_by: Option<ResolvedBy>,
    /// The address a tracker sees: the session's vote of its external
    /// address, else a gateway's; none until one is known.
    pub address: Option<IpAddr>,
    /// The port the session announces, 0 when it announces none.
    pub announced_port: u16,
    /// Peers can connect in.
    pub inbound: bool,
    pub udp_trackers: bool,
    pub dht: bool,
    /// The session speaks uTP: it listens on uTP or connects out over it.
    pub utp: bool,
}

impl Exposure {
    /// The exposure of `session`, open on a leg of `shape` whose names
    /// `resolved_by` resolves, with the last report of each of its port
    /// mappings.
    pub fn of(
        session: &Session,
        shape: Shape,
        resolved_by: Option<ResolvedBy>,
        mappings: &[PortMapped],
    ) -> Self {
        let announced_port = session.announce_port().unwrap_or(0);
        Self {
            resolved_by,
            address: session
                .external_ips()
                .first()
                .copied()
                .or_else(|| mappings.iter().find_map(|mapping| mapping.external_ip)),
            announced_port,
            inbound: announced_port != 0,
            udp_trackers: shape.datagrams,
            dht: shape.dht,
            utp: session.uses_utp(),
        }
    }
}
