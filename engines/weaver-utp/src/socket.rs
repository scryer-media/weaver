// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver
// Lifted from librqbit-utp 0.7.0 (Apache-2.0), Copyright (c) the librqbit authors.
// Modified by the Weaver authors, 2026-11-03.

use std::{
    collections::{BTreeMap, VecDeque, btree_map::Entry},
    io::IoSlice,
    net::SocketAddr,
    num::NonZeroUsize,
    sync::{
        Arc, Weak,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
    task::Poll,
    time::Duration,
};

use crate::{
    Error,
    constants::{TX_BUF_SIZE_PER_VSOCK_INITIAL_DEFAULT, TX_BUF_SIZE_PER_VSOCK_MAX_DEFAULT},
    stats::{Counter, UtpStats},
};
use parking_lot::Mutex;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use crate::{UtpStream, runtime::Runtime, spawn_utils::task_with_cancel};

#[cfg(test)]
mod lifecycle_tests;
use crate::{
    congestion::ledbat::LedbatParams,
    constants::{
        DEFAULT_CONNECT_TIMEOUT, DEFAULT_FIN_RESENDS, DEFAULT_MIN_TIMEOUT, DEFAULT_NUM_RESENDS,
        DEFAULT_REMOTE_INACTIVITY_TIMEOUT, DEFAULT_SYN_RESENDS, IPV4_HEADER,
        RX_BUF_SIZE_PER_VSOCK_DEFAULT, UDP_HEADER, UTP_HEADER,
    },
    message::UtpMessage,
    mtu::{MtuCeiling, floor_mtu},
    raw::{Type, UtpHeader},
    seq_nr::SeqNr,
    stream_dispatch::{StreamArgs, UtpStreamStarter},
    traits::{Transport, UtpEnvironment},
    utils::{DropGuardSendBeforeDeath, FnDropGuard},
};
use tokio::sync::{
    mpsc::{self, UnboundedReceiver, UnboundedSender, unbounded_channel},
    oneshot,
};
use tracing::{debug, debug_span, trace, warn};

type ConnectionId = u16;

// When we get incoming packets this connection id is used to pick the stream.
type StreamRecvKey = (SocketAddr, ConnectionId);

#[derive(Debug, Default, Clone)]
pub struct SocketOpts {
    pub link_mtu: Option<NonZeroUsize>,

    /// Virtual socket read buffer. Serves several purposes:
    /// - temporary storage before the data is read back to the user's application
    /// - reassembling out of order packets
    /// - advertising recv window to the other side (sender). The sender will not send more data than this and might thus stall.
    ///   Ideal value for throughput should be BDP (bandwidth delay product).
    pub vsock_rx_bufsize_bytes: Option<NonZeroUsize>,

    /// Virtual socket write buffer. Serves several purposes:
    /// - how many bytes can store without blocking writer.
    /// - how many bytes can store without receiving an ACK.
    ///
    /// This should be governed by BDP (bandwidth delay product). If it's too low, the pipeline
    /// would be stalling.
    /// If it's too high, every VSock would take too much memory.
    pub vsock_tx_bufsize_bytes_initial: Option<NonZeroUsize>,
    pub vsock_tx_bufsize_bytes_max: Option<NonZeroUsize>,

    /// Disable Nagle's algorithm
    pub disable_nagle: bool,

    /// LEDBAT's tunables.
    pub ledbat: LedbatParams,

    /// The parent span for spawned tokio tasks. If not provided, will use
    /// root span.
    pub parent_span: Option<tracing::Id>,

    /// If provided, this can be used to abruptly terminate all spawned tasks.
    pub cancellation_token: CancellationToken,

    /// The least retransmission timeout once a round trip is known;
    /// 500 ms when unset.
    pub min_timeout: Option<Duration>,

    /// Sends of one data packet, and timeouts in a row, before the
    /// connection gives up; 3 when unset.
    pub num_resends: Option<NonZeroUsize>,

    /// Sends of a SYN-ACK before an incoming connection gives up; 2 when
    /// unset.
    pub syn_resends: Option<NonZeroUsize>,

    /// Sends of our FIN before the connection gives up on its ACK; 2 when
    /// unset.
    pub fin_resends: Option<NonZeroUsize>,

    /// How long a connect waits for the remote's answer; 3 s when unset.
    pub connect_timeout: Option<Duration>,

    /// How many seconds to wait for incoming data
    /// before terminating the connection.
    pub remote_inactivity_timeout: Option<Duration>,

    /// The connections the owner holds open at once. A SYN arriving while
    /// the socket holds more than twice as many streams is dropped
    /// unanswered; with none set, every SYN is taken up.
    pub connections_limit: Option<NonZeroUsize>,

    /// If true, will wait for ACK of FIN. By default we don't wait
    /// as it's not required for torrents.
    pub dont_wait_for_lastack: bool,

    /// How many times should the MTU probes be retransmitted. By default it's 1, so that
    /// MTU probing can handle a bit of packet loss. If it's 0, we would mark RTO'ed probes as failed
    /// sooner and thus perf would be better.
    pub mtu_probe_max_retransmissions: Option<usize>,

    #[cfg(test)]
    pub(crate) benchmark_delayed_acks: bool,
}

impl SocketOpts {
    pub(crate) fn validate(&self) -> crate::Result<ValidatedSocketOpts> {
        let max_user_rx_buffered_bytes = self
            .vsock_rx_bufsize_bytes
            .unwrap_or(RX_BUF_SIZE_PER_VSOCK_DEFAULT);

        // 1500 is ethernet MTU.
        let link_mtu = self.link_mtu.unwrap_or(non_zero_const!(1500));
        let link_mtu: u16 = link_mtu
            .get()
            .try_into()
            .ok()
            .ok_or(Error::ValidateLinkMtuExceedsU16)?;
        let min_mtu = IPV4_HEADER + UDP_HEADER + UTP_HEADER + 1;
        if link_mtu < min_mtu {
            return Err(Error::ValidateMtuTooLow { link_mtu, min_mtu });
        }

        Ok(ValidatedSocketOpts {
            link_mtu,
            vsock_rx_bufsize: max_user_rx_buffered_bytes,
            nagle: !self.disable_nagle,
            ledbat: self.ledbat,
            min_timeout: self.min_timeout.unwrap_or(DEFAULT_MIN_TIMEOUT),
            num_resends: self
                .num_resends
                .map_or(DEFAULT_NUM_RESENDS, NonZeroUsize::get),
            syn_resends: self
                .syn_resends
                .map_or(DEFAULT_SYN_RESENDS, NonZeroUsize::get),
            fin_resends: self
                .fin_resends
                .map_or(DEFAULT_FIN_RESENDS, NonZeroUsize::get),
            connect_timeout: self.connect_timeout.unwrap_or(DEFAULT_CONNECT_TIMEOUT),
            vsock_tx_bufsize_bytes_initial: self
                .vsock_tx_bufsize_bytes_initial
                .unwrap_or(TX_BUF_SIZE_PER_VSOCK_INITIAL_DEFAULT),
            vsock_tx_bufsize_bytes_max: self
                .vsock_tx_bufsize_bytes_max
                .unwrap_or(TX_BUF_SIZE_PER_VSOCK_MAX_DEFAULT),
            remote_inactivity_timeout: self
                .remote_inactivity_timeout
                .unwrap_or(DEFAULT_REMOTE_INACTIVITY_TIMEOUT),
            wait_for_last_ack: !self.dont_wait_for_lastack,
            mtu_probe_max_retransmissions: self.mtu_probe_max_retransmissions.unwrap_or(1),
            #[cfg(test)]
            benchmark_delayed_acks: self.benchmark_delayed_acks,
        })
    }
}

#[derive(Clone)]
pub(crate) struct ValidatedSocketOpts {
    pub link_mtu: u16,
    pub vsock_rx_bufsize: NonZeroUsize,
    pub vsock_tx_bufsize_bytes_initial: NonZeroUsize,
    pub vsock_tx_bufsize_bytes_max: NonZeroUsize,
    pub nagle: bool,
    pub ledbat: LedbatParams,
    pub min_timeout: Duration,
    pub num_resends: usize,
    pub syn_resends: usize,
    pub fin_resends: usize,
    pub connect_timeout: Duration,

    pub remote_inactivity_timeout: Duration,

    pub wait_for_last_ack: bool,

    pub mtu_probe_max_retransmissions: usize,

    #[cfg(test)]
    pub benchmark_delayed_acks: bool,
}

pub(crate) struct RequestWithSpan<V> {
    created_span: tracing::Span,
    tx: oneshot::Sender<V>,
}

type ConnectRequest = RequestWithSpan<crate::Result<UtpStream>>;
type Acceptor<T, E> = RequestWithSpan<UtpStreamStarter<T, E>>;

impl<V> RequestWithSpan<V> {
    fn new(tx: oneshot::Sender<V>) -> Self {
        Self {
            created_span: tracing::Span::current(),
            tx,
        }
    }
}

pub(crate) enum ControlRequest {
    ConnectRequest(SocketAddr, ConnectToken, ConnectRequest),
    ConnectDropped(SocketAddr, ConnectToken),

    Shutdown(StreamRecvKey),
}

impl std::fmt::Debug for ControlRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ControlRequest::ConnectRequest(socket_addr, token, _) => {
                write!(f, "ConnectRequest({socket_addr}, {token})")
            }
            ControlRequest::ConnectDropped(socket_addr, token) => {
                write!(f, "ConnectDropped({socket_addr}, {token})")
            }
            ControlRequest::Shutdown(key) => {
                write!(f, "Shutdown({key:?})")
            }
        }
    }
}

static NEXT_CONNECT_TOKEN: AtomicU64 = AtomicU64::new(0);
type ConnectToken = u64;

struct Connecting {
    token: ConnectToken,
    start: Instant,
    connection_id: SeqNr,
    seq_nr: SeqNr,
    requester: ConnectRequest,
}

const MAX_CONNECTING_PER_ADDR: usize = 4;

#[derive(Default)]
struct ConnectingPerAddr {
    slots: [Option<Connecting>; MAX_CONNECTING_PER_ADDR],
    len: usize,
}

impl ConnectingPerAddr {
    fn is_empty(&self) -> bool {
        self.len == 0
    }

    fn insert(&mut self, c: Connecting) -> bool {
        for slot in self.slots.iter_mut() {
            if slot.is_none() {
                *slot = Some(c);
                self.len += 1;
                return true;
            }
        }
        false
    }

    fn pop(&mut self, connection_id: SeqNr, seq_nr: SeqNr) -> Option<Connecting> {
        for slot in self.slots.iter_mut() {
            if let Some(c) = slot
                && c.connection_id == connection_id
                && c.seq_nr == seq_nr
            {
                self.len -= 1;
                return slot.take();
            }
        }
        None
    }

    fn pop_by_token(&mut self, token: ConnectToken) -> Option<Connecting> {
        for slot in self.slots.iter_mut() {
            if let Some(c) = slot
                && c.token == token
            {
                self.len -= 1;
                return slot.take();
            }
        }
        None
    }
}

const ACCEPT_QUEUE_MAX_ACCEPTORS: usize = 32;
const ACCEPT_QUEUE_MAX_SYNS: usize = 32;

struct Syn {
    remote: SocketAddr,
    header: UtpHeader,
}

enum MatchSynWithAccept<T, E> {
    Matched,
    SynInvalid(Acceptor<T, E>),
    ReceiverDead(Syn),
}

struct AcceptQueue<T, E> {
    syns: VecDeque<Syn>,
    next_available_acceptor: Option<Acceptor<T, E>>,
    rx: mpsc::Receiver<Acceptor<T, E>>,
}

impl<T, E> AcceptQueue<T, E> {
    fn try_next_acceptor(&mut self) -> Option<Acceptor<T, E>> {
        if let Some(next) = self.next_available_acceptor.take() {
            return Some(next);
        }
        self.rx.try_recv().ok()
    }

    fn try_cache_syn(&mut self, syn: Syn) -> Option<Syn> {
        if self.syns.iter().any(|pending| {
            pending.remote == syn.remote && pending.header.connection_id == syn.header.connection_id
        }) {
            return None;
        }
        if self.syns.len() < ACCEPT_QUEUE_MAX_SYNS {
            self.syns.push_back(syn);
            return None;
        }
        Some(syn)
    }
}

pub(crate) struct Dispatcher<T: Transport, E: UtpEnvironment> {
    env: E,
    socket: Arc<SocketState<T, E>>,
    transport: Option<Arc<T>>,

    accept_queue: AcceptQueue<T, E>,

    pub(crate) streams: BTreeMap<StreamRecvKey, crate::ingress::Sender>,
    connecting: BTreeMap<SocketAddr, ConnectingPerAddr>,
    control_rx: UnboundedReceiver<ControlRequest>,
    next_connection_id: SeqNr,
}

impl<T: Transport, E: UtpEnvironment> Dispatcher<T, E> {
    pub(crate) async fn run_forever(mut self) -> crate::Result<()> {
        let mut read_buf = [0u8; 16384];

        loop {
            if let Err(e) = self.run_once(&mut read_buf).await {
                warn!("error running dispatcher: {e:#}");
                return Err(e);
            }
        }
    }

    async fn run_once(&mut self, read_buf: &mut [u8]) -> crate::Result<()> {
        self.cleanup_accept_queue()?;

        tokio::select! {
            accept = self.accept_queue.rx.recv(), if self.accept_queue.next_available_acceptor.is_none() => {
                let accept = accept.unwrap();
                self.accept_queue.next_available_acceptor = Some(accept);
            }
            control_request = self.control_rx.recv() => {
                let control = control_request.unwrap();
                self.on_control(control).await;
            },
            recv = self.transport.as_deref().ok_or(Error::DispatcherDead)?.recv_from(read_buf) => {
                let received = self.env.now();
                let (len, addr) = recv.map_err(Error::Recv)?;
                let message = match UtpMessage::deserialize(&read_buf[..len], received) {
                    Some(msg) => msg,
                    None => {
                        self.socket.stats.invalid_packets.increment(1);
                        trace!(len, ?addr, "error deserializing and validating UTP message");
                        return Ok(())
                    }
                };
                self.on_recv(addr, message).await?;
            }
        }

        Ok(())
    }

    // Get into a state where we can't match acceptors with cached SYNs anymore.
    fn cleanup_accept_queue(&mut self) -> crate::Result<()> {
        while let Some(syn) = self.accept_queue.syns.pop_front() {
            let acceptor = match self.accept_queue.try_next_acceptor() {
                Some(acc) => acc,
                None => {
                    self.accept_queue.syns.push_front(syn);
                    return Ok(());
                }
            };
            match self.match_syn_with_accept(syn, acceptor) {
                MatchSynWithAccept::Matched => continue,
                MatchSynWithAccept::SynInvalid(sender) => {
                    self.accept_queue.next_available_acceptor = Some(sender);
                }
                MatchSynWithAccept::ReceiverDead(syn) => {
                    self.accept_queue.syns.push_front(syn);
                }
            }
        }

        Ok(())
    }

    fn get_next_free_conn_id(&mut self, addr: SocketAddr) -> SeqNr {
        while self
            .streams
            .contains_key(&(addr, self.next_connection_id.0))
        {
            self.next_connection_id += 2;
        }
        self.next_connection_id
    }

    // This would only block if the socket TX is full. It will block the dispatcher but it's a resonable tradeoff.
    async fn on_control(&mut self, msg: ControlRequest) {
        match msg {
            ControlRequest::ConnectRequest(addr, token, sender) => {
                let conn_id = self.get_next_free_conn_id(addr);
                let header = UtpHeader {
                    htype: Type::Syn,
                    connection_id: conn_id,
                    timestamp_microseconds: (self.env.now() - self.socket.created).as_micros()
                        as u32,
                    timestamp_difference_microseconds: 0,
                    wnd_size: 0,
                    seq_nr: self.env.random_u16().into(),
                    ack_nr: 0.into(),
                    extensions: Default::default(),
                };
                let mut buf = [0u8; UTP_HEADER as usize];
                header.serialize(&mut buf).unwrap();
                let Some(transport) = self.transport.as_deref() else {
                    let _ = sender.tx.send(Err(Error::DispatcherDead));
                    return;
                };
                match transport.send_to(&buf, addr).await {
                    Ok(len) if len == buf.len() => {}
                    Ok(len) => {
                        // Never seen this happen yet, so can be warn.
                        warn!(
                            len,
                            expected_len = buf.len(),
                            ?addr,
                            "did not send full length, dropping"
                        );
                        return;
                    }
                    Err(e) => {
                        let _ = sender.tx.send(Err(Error::ErrorSendingSyn(e)));
                        return;
                    }
                }
                let c = Connecting {
                    token,
                    connection_id: header.connection_id,
                    seq_nr: header.seq_nr,
                    requester: sender,
                    start: self.env.now(),
                };
                if self.connecting.entry(addr).or_default().insert(c) {
                    self.next_connection_id += 2;
                } else {
                    // This is super rare, can be warn.
                    warn!("too many concurrent connectins to {addr}");
                }
            }
            ControlRequest::ConnectDropped(addr, token) => {
                match self.connecting.entry(addr) {
                    Entry::Occupied(mut occ) => {
                        if occ.get_mut().pop_by_token(token).is_some() && occ.get().is_empty() {
                            occ.remove();
                        }
                    }
                    Entry::Vacant(_) => {}
                };
            }
            ControlRequest::Shutdown(key) => {
                trace!(?key, "removing stream");
                self.streams.remove(&key);
            }
        }
    }

    fn on_maybe_connect_ack(&mut self, addr: SocketAddr, msg: UtpMessage) -> crate::Result<()> {
        let mut occ = match self.connecting.entry(addr) {
            Entry::Occupied(occ) => occ,
            Entry::Vacant(_) => {
                trace!(
                    ?msg,
                    "dropping packet, noone is connecting, and no registered streams"
                );
                return Ok(());
            }
        };

        let conn = if let Some(conn) = occ
            .get_mut()
            .pop(msg.header.connection_id, msg.header.ack_nr)
        {
            if occ.get_mut().is_empty() {
                occ.remove();
            }
            conn
        } else {
            debug!(
                ?msg,
                "dropping packet, connection ID or SYN acknowledgement does not match"
            );
            self.socket.stats.invalid_packets.increment(1);
            return Ok(());
        };

        let now = self.env.now();
        let (tx, rx) = crate::ingress::channel(self.socket.opts.vsock_rx_bufsize);
        let args = StreamArgs::new_outgoing(&msg.header, conn.start, now)
            .with_parent_span(conn.requester.created_span.clone());

        let recv_key = (addr, msg.header.connection_id.0);
        if self.streams.insert(recv_key, tx).is_some() {
            warn!(key=?recv_key, "bug: a stream already existed with key. It should have been checked beforehand.");
        }

        let stream = UtpStreamStarter::new_shared(&self.socket, addr, rx, args).start();
        if conn.requester.tx.send(stream).is_ok() {
            trace!(?recv_key, "created stream and passed to connector");
        } else {
            debug!(?recv_key, "connecting receiver is dead. dropping");
            self.streams.remove(&recv_key);
        }

        Ok(())
    }

    fn match_syn_with_accept(
        &mut self,
        syn: Syn,
        accept: Acceptor<T, E>,
    ) -> MatchSynWithAccept<T, E> {
        let recv_key = (syn.remote, (syn.header.connection_id + 1).0);
        if self.streams.contains_key(&recv_key) {
            debug!(?recv_key, "SYN clashes with an existing stream, ignoring");
            self.socket.stats.invalid_packets.increment(1);
            return MatchSynWithAccept::SynInvalid(accept);
        }

        let args = StreamArgs::new_incoming(self.env.random_u16().into(), &syn.header)
            .with_parent_span(accept.created_span.clone());
        let (tx, rx) = crate::ingress::channel(self.socket.opts.vsock_rx_bufsize);

        let starter = UtpStreamStarter::new_shared(&self.socket, syn.remote, rx, args);

        self.streams.insert(recv_key, tx);
        match accept.tx.send(starter) {
            Ok(()) => {
                trace!("created stream and passed to acceptor");
                MatchSynWithAccept::Matched
            }
            Err(starter) => {
                starter.disarm();
                self.streams.remove(&recv_key);
                MatchSynWithAccept::ReceiverDead(syn)
            }
        }
    }

    async fn try_send_rst(&self, syn: Syn) {
        let mut buf: [u8; UTP_HEADER as usize] = [0; UTP_HEADER as usize];
        let packet = UtpHeader {
            htype: Type::Reset,
            connection_id: syn.header.connection_id,
            seq_nr: 0.into(),
            ack_nr: syn.header.seq_nr,
            ..Default::default()
        };
        if let Some(transport) = self.transport.as_deref()
            && let Ok(len) = packet.serialize(&mut buf[..])
            && let Err(e) = transport.send_to(&buf[..len], syn.remote).await
        {
            debug!(remote=?syn.remote, "error sending ST_RESET: {e:#}");
        }
    }

    async fn on_syn(&mut self, remote: SocketAddr, msg: UtpMessage) -> crate::Result<()> {
        let pending = self
            .accept_queue
            .syns
            .iter()
            .filter(|syn| {
                !self
                    .streams
                    .contains_key(&(syn.remote, (syn.header.connection_id + 1).0))
            })
            .count();
        let registered = self.streams.len().saturating_add(pending);
        let limit = self
            .socket
            .connections_limit
            .load(Ordering::Relaxed)
            .saturating_mul(2);
        if registered > limit {
            trace!(streams = registered, "dropping SYN, too many streams");
            self.socket.stats.syn_flood_dropped.increment(1);
            return Ok(());
        }
        let mut syn = Syn {
            remote,
            header: msg.header,
        };
        while let Some(acceptor) = self.accept_queue.try_next_acceptor() {
            match self.match_syn_with_accept(syn, acceptor) {
                MatchSynWithAccept::Matched => return Ok(()),
                MatchSynWithAccept::SynInvalid(sender) => {
                    self.accept_queue.next_available_acceptor = Some(sender);
                    return Ok(());
                }
                MatchSynWithAccept::ReceiverDead(s) => syn = s,
            }
        }
        match self.accept_queue.try_cache_syn(syn) {
            None => Ok(()),
            Some(syn) => {
                debug!("dropping SYN, no more space to cache them and no acceptors available");
                self.socket.stats.syn_rst.increment(1);
                self.try_send_rst(syn).await;
                Ok(())
            }
        }
    }

    async fn on_recv(&mut self, addr: SocketAddr, message: UtpMessage) -> crate::Result<()> {
        let key = (addr, message.header.connection_id.0);

        if let Some(tx) = self.streams.get(&key) {
            if tx.send(message).is_err() {
                trace!(
                    ?key,
                    "stream dead, but wasn't cleaned up yet, this is probably a race"
                );
                self.streams.remove(&key);
            }
            return Ok(());
        }

        trace!(?key, "no matching live streams");

        match message.header.get_type() {
            Type::State => {
                self.on_maybe_connect_ack(addr, message)?;
            }
            Type::Syn => {
                self.on_syn(addr, message).await?;
            }
            _ => {
                trace!(?message, ?addr, "dropping packet");
            }
        }
        Ok(())
    }
}

pub struct UtpSocket<T: Transport, E> {
    pub(crate) state: Arc<SocketState<T, E>>,
    runtime: Arc<Runtime<T>>,
}

impl<T: Transport, E> Drop for UtpSocket<T, E> {
    fn drop(&mut self) {
        self.runtime.request_shutdown();
    }
}

pub(crate) struct SocketState<T, E> {
    pub(crate) runtime: Weak<Runtime<T>>,
    // When was the socket created. All the uTP "timestamp_microsends" are relative to it.
    pub(crate) created: Instant,
    pub(crate) control_requests: UnboundedSender<ControlRequest>,
    accept_requests: mpsc::Sender<Acceptor<T, E>>,

    pub(crate) env: E,

    local_addr: SocketAddr,
    link_mtu: Option<u16>,
    dont_fragment: bool,
    opts: ValidatedSocketOpts,
    connections_limit: AtomicUsize,

    pub(crate) cancellation_token: CancellationToken,

    pub(crate) stats: UtpStats,

    mtu_ceiling: Mutex<MtuCeiling>,
}

impl<T: Transport, E: UtpEnvironment> std::fmt::Debug for UtpSocket<T, E> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("UtpSocket")
            .field("addr", &self.state.local_addr)
            .finish_non_exhaustive()
    }
}

impl<T: Transport, Env: UtpEnvironment> UtpSocket<T, Env> {
    pub fn new_with_opts(transport: T, env: Env, opts: SocketOpts) -> crate::Result<Arc<Self>> {
        Self::new_with_opts_and_init(transport, env, opts, |_| {})
    }

    /// Initialize the socket before its dispatcher can receive any packet.
    pub fn new_with_opts_and_init(
        transport: T,
        env: Env,
        opts: SocketOpts,
        initialize: impl FnOnce(&Arc<Self>),
    ) -> crate::Result<Arc<Self>> {
        let parent_span = opts.parent_span.clone();
        let (sock, mut dispatcher) = Self::build(transport, env, opts)?;
        initialize(&sock);
        let span = debug_span!(parent: parent_span, "utp_socket", addr=?sock.bind_addr());
        let cancellation = sock.state.cancellation_token.clone();
        let runtime = Arc::downgrade(&sock.runtime);
        if sock
            .runtime
            .register(move |transport| {
                dispatcher.transport = Some(transport);
                task_with_cancel(span, cancellation, async move {
                    let on_drop = runtime.clone();
                    let _shutdown = FnDropGuard::new(move || {
                        if let Some(runtime) = on_drop.upgrade() {
                            runtime.request_shutdown();
                        }
                    });
                    let result = dispatcher.run_forever().await;
                    if let Some(runtime) = runtime.upgrade() {
                        if let Err(error) = &result {
                            runtime.request_shutdown_with_error(std::io::Error::other(
                                error.to_string(),
                            ));
                        } else {
                            runtime.request_shutdown();
                        }
                    }
                    result
                })
            })
            .is_err()
        {
            sock.runtime.request_shutdown();
        }
        Ok(sock)
    }

    #[cfg(test)]
    pub(crate) fn new_with_opts_and_dispatcher(
        transport: T,
        env: Env,
        opts: SocketOpts,
    ) -> crate::Result<(Arc<Self>, Dispatcher<T, Env>)> {
        let (socket, mut dispatcher) = Self::build(transport, env, opts)?;
        dispatcher.transport = socket.runtime.transport();
        Ok((socket, dispatcher))
    }

    fn build(
        transport: T,
        env: Env,
        opts: SocketOpts,
    ) -> crate::Result<(Arc<Self>, Dispatcher<T, Env>)> {
        let validated_opts = opts.validate()?;
        let local_addr = transport.bind_addr();
        let link_mtu = transport.link_mtu();
        let dont_fragment = transport.dont_fragment();
        let transport = Arc::new(transport);
        let runtime = Runtime::new(transport, &opts.cancellation_token);

        let (accept_tx, accept_rx) = mpsc::channel(ACCEPT_QUEUE_MAX_ACCEPTORS);
        let (control_tx, control_rx) = unbounded_channel();

        let state = Arc::new(SocketState {
            runtime: Arc::downgrade(&runtime),
            created: env.now(),
            control_requests: control_tx,
            local_addr,
            link_mtu,
            dont_fragment,
            opts: validated_opts,
            connections_limit: AtomicUsize::new(
                opts.connections_limit.map_or(usize::MAX, NonZeroUsize::get),
            ),
            env: env.copy(),
            accept_requests: accept_tx,
            cancellation_token: runtime.cancellation().clone(),
            stats: UtpStats::default(),
            mtu_ceiling: Default::default(),
        });

        let sock = Arc::new(Self {
            state: state.clone(),
            runtime,
        });
        let dispatcher = Dispatcher {
            streams: BTreeMap::default(),
            connecting: BTreeMap::default(),
            next_connection_id: env.random_u16().into(),
            control_rx,
            socket: state,
            transport: None,
            env,
            accept_queue: AcceptQueue {
                syns: Default::default(),
                next_available_acceptor: None,
                rx: accept_rx,
            },
        };

        Ok((sock, dispatcher))
    }

    /// Stop every stream and dispatcher, join transport work, and release
    /// the owned transport. Repeated callers share the same result.
    pub async fn shutdown(&self) -> std::io::Result<()> {
        self.runtime.shutdown().await
    }

    pub fn bind_addr(&self) -> SocketAddr {
        self.state.local_addr
    }

    /// Whether this socket's transport sends with don't-fragment set.
    pub fn dont_fragment(&self) -> bool {
        self.state.dont_fragment
    }

    /// Change incoming SYN admission without evicting established streams.
    /// Zero stays zero; `usize::MAX` leaves no finite bound.
    pub fn set_connections_limit(&self, limit: usize) {
        self.state.connections_limit.store(limit, Ordering::Relaxed);
    }

    /// The effective connection limit; `None` means no finite bound.
    pub fn connections_limit(&self) -> Option<usize> {
        let limit = self.state.connections_limit.load(Ordering::Relaxed);
        (limit != usize::MAX).then_some(limit)
    }

    /// Counters remain readable after shutdown.
    pub fn stats(&self) -> &UtpStats {
        &self.state.stats
    }

    pub async fn accept(self: &Arc<Self>) -> crate::Result<UtpStream> {
        self.state.accept().await
    }

    pub async fn connect(self: &Arc<Self>, remote: SocketAddr) -> crate::Result<UtpStream> {
        self.state.connect(remote).await
    }

    #[cfg(test)]
    pub(crate) fn opts(&self) -> &ValidatedSocketOpts {
        self.state.opts()
    }

    #[cfg(test)]
    pub(crate) fn restrict_mtu(&self, mtu: u16) {
        self.state.restrict_mtu(mtu);
    }
}

impl<T: Transport, Env: UtpEnvironment> SocketState<T, Env> {
    // The link MTU a new stream probes up to: the link's, no larger than
    // the transport's path carries, and no higher than the streams that
    // died on large packets allow. Where datagrams may be fragmented, a
    // probe too large would arrive split rather than lost, so no stream
    // probes past the floor.
    pub(crate) fn probe_link_mtu(&self, is_ipv4: bool) -> u16 {
        let (link_mtu, dont_fragment) = self
            .runtime
            .upgrade()
            .and_then(|runtime| runtime.metadata())
            .unwrap_or((self.link_mtu, self.dont_fragment));
        let mtu = self
            .opts
            .link_mtu
            .min(link_mtu.unwrap_or(u16::MAX))
            .min(self.mtu_ceiling.lock().get());
        if dont_fragment {
            mtu
        } else {
            mtu.min(floor_mtu(is_ipv4))
        }
    }

    // A stream died on a packet of `mtu` bytes larger than it had seen
    // delivered.
    pub(crate) fn restrict_mtu(&self, mtu: u16) {
        self.mtu_ceiling.lock().restrict(mtu);
    }

    pub(crate) fn opts(&self) -> &ValidatedSocketOpts {
        &self.opts
    }

    pub async fn accept(self: &Arc<Self>) -> crate::Result<UtpStream> {
        let (tx, rx) = oneshot::channel();

        self.stats.accepting.increment(1);
        let _accepting_guard = FnDropGuard::new(|| self.stats.accepting.decrement(1));

        tokio::select! {
            biased;
            _ = self.cancellation_token.cancelled() => return Err(Error::DispatcherDead),
            sent = self.accept_requests.send(RequestWithSpan::new(tx)) => sent.map_err(|_| Error::DispatcherDead)?,
        }
        let stream = tokio::select! {
            biased;
            _ = self.cancellation_token.cancelled() => return Err(Error::DispatcherDead),
            received = rx => received.map_err(|_| Error::DispatcherDead)?,
        };
        let stream = stream.start()?;
        self.stats.accepts.increment(1);
        trace!("accepted");
        Ok(stream)
    }

    pub async fn connect(self: &Arc<Self>, remote: SocketAddr) -> crate::Result<UtpStream> {
        if self.cancellation_token.is_cancelled() {
            return Err(Error::DispatcherDead);
        }
        let (tx, rx) = oneshot::channel();
        let token = NEXT_CONNECT_TOKEN.fetch_add(1, Ordering::Relaxed);
        self.stats.connection_attempts.increment(1);
        self.stats.connecting.increment(1);

        let mut fail_guard = FnDropGuard::new(|| {
            self.stats.connection_failures.increment(1);
        });

        let _connecting_guard = FnDropGuard::new(|| {
            self.stats.connecting.decrement(1);
        });

        self.control_requests
            .send(ControlRequest::ConnectRequest(
                remote,
                token,
                RequestWithSpan::new(tx),
            ))
            .ok()
            .ok_or(Error::DispatcherDead)?;

        let mut send_drop_guard = DropGuardSendBeforeDeath::new(
            ControlRequest::ConnectDropped(remote, token),
            &self.control_requests,
        );

        let stream_or_err = tokio::select! {
            biased;
            _ = self.cancellation_token.cancelled() => return Err(Error::DispatcherDead),
            received = tokio::time::timeout(self.opts.connect_timeout, rx) => received
                .map_err(|_| Error::ConnectTimedOut)?
                .map_err(|_| Error::DispatcherDead)?,
        };
        send_drop_guard.disarm();
        if stream_or_err.is_ok() {
            fail_guard.disarm();
            self.stats.connection_successes.increment(1);
        }

        stream_or_err
    }

    /// Returns true if saw Poll::Pending
    pub(crate) fn try_poll_send_to(
        &self,
        transport: &T,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
        addr: SocketAddr,
    ) -> std::io::Result<bool> {
        match transport.poll_send_to(cx, buf, addr) {
            Poll::Ready(Ok(sz)) => {
                if sz != buf.len() {
                    warn!(
                        actual_len = sz,
                        expected_len = buf.len(),
                        "sent a broken packet"
                    );
                }
            }
            Poll::Ready(Err(e)) => {
                self.stats.send_errors.increment(1);
                debug!(
                    "error sending to UDP socket addr={}, len={}: {e:#}",
                    addr,
                    buf.len()
                );
                return Err(e);
            }
            Poll::Pending => {
                self.stats.send_poll_pending.increment(1);
                debug_every_ms!(5000, "UDP socket full, could not send packet");
                return Ok(true);
            }
        }
        Ok(false)
    }

    pub(crate) fn try_poll_send_to_vectored(
        &self,
        transport: &T,
        cx: &mut std::task::Context<'_>,
        bufs: &[IoSlice<'_>],
        addr: SocketAddr,
        total_len: usize,
    ) -> std::io::Result<bool> {
        match transport.poll_send_to_vectored(cx, bufs, addr) {
            Poll::Ready(Ok(sz)) => {
                if sz != total_len {
                    warn!(
                        actual_len = sz,
                        expected_len = total_len,
                        "sent a broken packet"
                    );
                }
            }
            Poll::Ready(Err(e)) => {
                self.stats.send_errors.increment(1);
                debug!(
                    "error sending to UDP socket addr={}, len={}: {e:#}",
                    addr, total_len
                );
                return Err(e);
            }
            Poll::Pending => {
                self.stats.send_poll_pending.increment(1);
                debug_every_ms!(5000, "UDP socket full, could not send packet");
                return Ok(true);
            }
        }
        Ok(false)
    }
}

#[cfg(test)]
mod tests {
    use std::net::{Ipv4Addr, SocketAddr};

    use anyhow::Context;
    use tokio::{
        io::{AsyncReadExt, AsyncWriteExt},
        try_join,
    };
    use tracing::{Instrument, debug_span, info};

    use crate::test_util::{MockUtpStream, setup_test_logging, transport::MockInterface};

    struct RepeatedSynSequence(crate::test_util::env::MockUtpEnvironment);

    impl crate::traits::UtpEnvironment for RepeatedSynSequence {
        fn now(&self) -> tokio::time::Instant {
            crate::traits::UtpEnvironment::now(&self.0)
        }

        fn copy(&self) -> Self {
            Self(self.0.clone())
        }

        fn random_u16(&self) -> u16 {
            100
        }
    }

    async fn unexpected_handshake_state_is_ignored(
        remote: SocketAddr,
        connection_id_offset: u16,
        syn_ack_offset: u16,
    ) {
        use super::{ControlRequest, RequestWithSpan};
        use crate::{
            UtpSocket,
            message::UtpMessage,
            raw::{Type, UtpHeader},
            test_util::{ADDR_1, ADDR_2, env::MockUtpEnvironment, transport::RememberingTransport},
        };

        let transport = RememberingTransport::new(ADDR_1);
        let (_socket, mut dispatcher) = UtpSocket::new_with_opts_and_dispatcher(
            transport.clone(),
            RepeatedSynSequence(MockUtpEnvironment::new()),
            Default::default(),
        )
        .unwrap();
        let (tx, mut rx) = tokio::sync::oneshot::channel();
        dispatcher
            .on_control(ControlRequest::ConnectRequest(
                ADDR_2,
                0,
                RequestWithSpan::new(tx),
            ))
            .await;
        let sent = transport.take_sent_utpmessages();
        assert_eq!(sent.len(), 1);
        let syn = &sent[0].header;
        assert_eq!(syn.htype, Type::Syn);
        let response = UtpHeader {
            htype: Type::State,
            connection_id: syn.connection_id,
            seq_nr: 1.into(),
            ack_nr: syn.seq_nr,
            wnd_size: 1024,
            ..Default::default()
        };
        dispatcher
            .on_recv(
                remote,
                UtpMessage::new_test(
                    UtpHeader {
                        connection_id: response.connection_id + connection_id_offset,
                        ack_nr: response.ack_nr + syn_ack_offset,
                        ..response
                    },
                    b"",
                ),
            )
            .await
            .unwrap();
        assert!(matches!(
            rx.try_recv(),
            Err(tokio::sync::oneshot::error::TryRecvError::Empty)
        ));
        assert!(dispatcher.streams.is_empty());

        dispatcher
            .on_recv(ADDR_2, UtpMessage::new_test(response, b""))
            .await
            .unwrap();
        let _stream = rx.try_recv().unwrap().unwrap();
        assert!(
            dispatcher
                .streams
                .contains_key(&(ADDR_2, syn.connection_id.0))
        );
        assert!(dispatcher.connecting.is_empty());
    }

    #[tokio::test]
    async fn a_state_handshake_requires_the_requested_endpoint() {
        unexpected_handshake_state_is_ignored(SocketAddr::from((Ipv4Addr::LOCALHOST, 3)), 0, 0)
            .await;
    }

    #[tokio::test]
    async fn a_state_handshake_requires_the_requested_connection_id() {
        unexpected_handshake_state_is_ignored(crate::test_util::ADDR_2, 1, 0).await;
    }

    #[tokio::test]
    async fn a_state_handshake_requires_the_sent_syn_sequence() {
        unexpected_handshake_state_is_ignored(crate::test_util::ADDR_2, 0, 1).await;
    }

    #[tokio::test]
    async fn equal_syn_sequences_complete_only_the_matching_connection_id() {
        use super::{ControlRequest, RequestWithSpan};
        use crate::{
            UtpSocket,
            message::UtpMessage,
            raw::{Type, UtpHeader},
            test_util::{ADDR_1, ADDR_2, env::MockUtpEnvironment, transport::RememberingTransport},
        };

        let transport = RememberingTransport::new(ADDR_1);
        let (_socket, mut dispatcher) = UtpSocket::new_with_opts_and_dispatcher(
            transport.clone(),
            RepeatedSynSequence(MockUtpEnvironment::new()),
            Default::default(),
        )
        .unwrap();
        let (first_tx, mut first_rx) = tokio::sync::oneshot::channel();
        let (second_tx, mut second_rx) = tokio::sync::oneshot::channel();
        for (token, tx) in [(0, first_tx), (1, second_tx)] {
            dispatcher
                .on_control(ControlRequest::ConnectRequest(
                    ADDR_2,
                    token,
                    RequestWithSpan::new(tx),
                ))
                .await;
        }
        let syns = transport.take_sent_utpmessages();
        assert_eq!(syns.len(), 2);
        assert!(syns.iter().all(|packet| packet.header.htype == Type::Syn));
        assert_eq!(syns[0].header.seq_nr, syns[1].header.seq_nr);
        assert_ne!(syns[0].header.connection_id, syns[1].header.connection_id);
        let response = |index: usize| {
            UtpMessage::new_test(
                UtpHeader {
                    htype: Type::State,
                    connection_id: syns[index].header.connection_id,
                    seq_nr: 1.into(),
                    ack_nr: syns[index].header.seq_nr,
                    wnd_size: 1024,
                    ..Default::default()
                },
                b"",
            )
        };

        dispatcher.on_recv(ADDR_2, response(1)).await.unwrap();
        let _second_stream = second_rx.try_recv().unwrap().unwrap();
        assert!(matches!(
            first_rx.try_recv(),
            Err(tokio::sync::oneshot::error::TryRecvError::Empty)
        ));
        assert!(
            dispatcher
                .streams
                .contains_key(&(ADDR_2, syns[1].header.connection_id.0))
        );
        assert!(
            !dispatcher
                .streams
                .contains_key(&(ADDR_2, syns[0].header.connection_id.0))
        );

        dispatcher.on_recv(ADDR_2, response(0)).await.unwrap();
        let _first_stream = first_rx.try_recv().unwrap().unwrap();
        assert_eq!(dispatcher.streams.len(), 2);
        assert!(dispatcher.connecting.is_empty());
    }

    #[tokio::test]
    async fn stream_routing_distinguishes_endpoints_and_all_raw_connection_ids() {
        use super::ControlRequest;
        use crate::{
            UtpSocket,
            message::UtpMessage,
            raw::{Type, UtpHeader},
            test_util::{ADDR_1, ADDR_2, env::MockUtpEnvironment, transport::RememberingTransport},
        };

        let (_socket, mut dispatcher) = UtpSocket::new_with_opts_and_dispatcher(
            RememberingTransport::new(ADDR_1),
            MockUtpEnvironment::new(),
            Default::default(),
        )
        .unwrap();
        let other = SocketAddr::from((Ipv4Addr::LOCALHOST, 3));
        let keys = [
            (ADDR_2, 0u16),
            (ADDR_2, 1),
            (ADDR_2, 32767),
            (ADDR_2, 32768),
            (ADDR_2, 32769),
            (ADDR_2, 65534),
            (ADDR_2, 65535),
            (other, 0),
            (other, 32768),
            (other, 65535),
        ];
        let mut receivers = Vec::new();
        for key in keys {
            let (tx, rx) = crate::ingress::channel(dispatcher.socket.opts.vsock_rx_bufsize);
            assert!(dispatcher.streams.insert(key, tx).is_none());
            receivers.push(rx);
        }
        let packet = |id: u16, payload: &[u8]| {
            UtpMessage::new_test(
                UtpHeader {
                    htype: Type::Data,
                    connection_id: id.into(),
                    ..Default::default()
                },
                payload,
            )
        };
        for (index, (remote, id)) in keys.into_iter().enumerate() {
            let payload = [index as u8];
            dispatcher
                .on_recv(remote, packet(id, &payload))
                .await
                .unwrap();
            assert_eq!(
                receivers[index].try_recv().unwrap().message.payload(),
                payload
            );
            assert!(receivers.iter_mut().all(|rx| rx.try_recv().is_err()));
        }

        dispatcher
            .on_control(ControlRequest::Shutdown((ADDR_2, 32768)))
            .await;
        assert_eq!(dispatcher.streams.len(), keys.len() - 1);
        dispatcher
            .on_recv(ADDR_2, packet(32768, b"removed"))
            .await
            .unwrap();
        assert!(receivers.iter_mut().all(|rx| rx.try_recv().is_err()));
        dispatcher
            .on_recv(other, packet(32768, b"still routed"))
            .await
            .unwrap();
        assert_eq!(
            receivers[8].try_recv().unwrap().message.payload(),
            b"still routed"
        );

        dispatcher.next_connection_id = 65534.into();
        assert_eq!(dispatcher.get_next_free_conn_id(ADDR_2), 2.into());
        dispatcher
            .on_control(ControlRequest::Shutdown((ADDR_2, 0)))
            .await;
        dispatcher.next_connection_id = 65534.into();
        assert_eq!(dispatcher.get_next_free_conn_id(ADDR_2), 0.into());
        assert!(dispatcher.streams.contains_key(&(other, 0)));
    }

    #[tokio::test]
    async fn a_wrapped_incoming_id_is_registered_and_removed_on_stream_drop() {
        use super::{MatchSynWithAccept, RequestWithSpan, Syn};
        use crate::{
            UtpSocket,
            raw::{Type, UtpHeader},
            test_util::{ADDR_1, ADDR_2, env::MockUtpEnvironment, transport::RememberingTransport},
        };
        let (_socket, mut dispatcher) = UtpSocket::new_with_opts_and_dispatcher(
            RememberingTransport::new(ADDR_1),
            MockUtpEnvironment::new(),
            Default::default(),
        )
        .unwrap();
        let (tx, rx) = tokio::sync::oneshot::channel();
        assert!(matches!(
            dispatcher.match_syn_with_accept(
                Syn {
                    remote: ADDR_2,
                    header: UtpHeader {
                        htype: Type::Syn,
                        connection_id: u16::MAX.into(),
                        ..Default::default()
                    },
                },
                RequestWithSpan::new(tx),
            ),
            MatchSynWithAccept::Matched
        ));
        let starter = rx.await.unwrap();
        assert!(dispatcher.streams.contains_key(&(ADDR_2, 0)));
        drop(starter);
        let shutdown = dispatcher.control_rx.recv().await.unwrap();
        assert!(
            matches!(shutdown, super::ControlRequest::Shutdown((remote, 0)) if remote == ADDR_2)
        );
        dispatcher.on_control(shutdown).await;
        assert!(dispatcher.streams.is_empty());
    }

    // A SYN while more than twice the connections limit of streams are
    // open is dropped unanswered; at twice the limit it is still taken up.
    #[tokio::test]
    async fn a_syn_past_twice_the_connections_limit_is_dropped() {
        use std::num::NonZeroUsize;

        use crate::{
            SocketOpts, UtpSocket,
            message::UtpMessage,
            raw::{Type, UtpHeader},
            test_util::{ADDR_1, ADDR_2, env::MockUtpEnvironment, transport::RememberingTransport},
        };

        setup_test_logging();
        let transport = RememberingTransport::new(ADDR_1);
        let (socket, mut dispatcher) = UtpSocket::new_with_opts_and_dispatcher(
            transport.clone(),
            MockUtpEnvironment::new(),
            SocketOpts {
                connections_limit: NonZeroUsize::new(1),
                ..Default::default()
            },
        )
        .unwrap();
        let syn = |id: u16| {
            UtpMessage::new_test(
                UtpHeader {
                    htype: Type::Syn,
                    connection_id: id.into(),
                    ..Default::default()
                },
                b"",
            )
        };
        let mut open = Vec::new();
        for id in 0..2u16 {
            let (tx, rx) = crate::ingress::channel(socket.opts().vsock_rx_bufsize);
            dispatcher.streams.insert((ADDR_2, 1000 + id), tx);
            open.push(rx);
        }

        dispatcher.on_recv(ADDR_2, syn(1)).await.unwrap();
        assert_eq!(dispatcher.accept_queue.syns.len(), 1, "taken up");

        let (tx, rx) = crate::ingress::channel(socket.opts().vsock_rx_bufsize);
        dispatcher.streams.insert((ADDR_2, 2000), tx);
        open.push(rx);
        dispatcher.on_recv(ADDR_2, syn(3)).await.unwrap();
        assert_eq!(dispatcher.accept_queue.syns.len(), 1, "dropped");
        assert_eq!(
            socket
                .stats()
                .syn_flood_dropped
                .load(std::sync::atomic::Ordering::Relaxed),
            1
        );
        assert!(transport.take_sent_utpmessages().is_empty(), "unanswered");
        assert_eq!(socket.stats().snapshot().syn_rst, 0);
    }

    #[tokio::test]
    async fn a_changed_connection_limit_bounds_pending_syns_immediately() {
        use crate::{
            SocketOpts, UtpSocket,
            message::UtpMessage,
            raw::{Type, UtpHeader},
            test_util::{ADDR_1, ADDR_2, env::MockUtpEnvironment, transport::RememberingTransport},
        };

        let (socket, mut dispatcher) = UtpSocket::new_with_opts_and_dispatcher(
            RememberingTransport::new(ADDR_1),
            MockUtpEnvironment::new(),
            SocketOpts {
                connections_limit: std::num::NonZeroUsize::new(3),
                ..Default::default()
            },
        )
        .unwrap();
        let syn = |id: u16| {
            UtpMessage::new_test(
                UtpHeader {
                    htype: Type::Syn,
                    connection_id: id.into(),
                    ..Default::default()
                },
                b"",
            )
        };
        for id in 0..3 {
            dispatcher.on_recv(ADDR_2, syn(id)).await.unwrap();
        }
        dispatcher.on_recv(ADDR_2, syn(0)).await.unwrap();
        assert_eq!(
            dispatcher.accept_queue.syns.len(),
            3,
            "a retransmission is one pending SYN"
        );

        socket.set_connections_limit(1);
        dispatcher.on_recv(ADDR_2, syn(3)).await.unwrap();
        assert_eq!(dispatcher.accept_queue.syns.len(), 3);
        assert_eq!(socket.stats().snapshot().syn_flood_dropped, 1);

        socket.set_connections_limit(2);
        dispatcher.on_recv(ADDR_2, syn(3)).await.unwrap();
        dispatcher.on_recv(ADDR_2, syn(4)).await.unwrap();
        assert_eq!(
            dispatcher.accept_queue.syns.len(),
            5,
            "at twice the limit a SYN is still accepted"
        );
        dispatcher.on_recv(ADDR_2, syn(5)).await.unwrap();
        assert_eq!(dispatcher.accept_queue.syns.len(), 5);
        assert_eq!(socket.stats().snapshot().syn_flood_dropped, 2);

        socket.set_connections_limit(usize::MAX);
        assert_eq!(socket.connections_limit(), None);
        dispatcher.on_recv(ADDR_2, syn(5)).await.unwrap();
        assert_eq!(dispatcher.accept_queue.syns.len(), 6);
        socket.set_connections_limit(0);
        assert_eq!(socket.connections_limit(), Some(0));
        dispatcher.on_recv(ADDR_2, syn(6)).await.unwrap();
        assert_eq!(dispatcher.accept_queue.syns.len(), 6);
        assert_eq!(socket.stats().snapshot().syn_flood_dropped, 3);
    }

    #[tokio::test]
    async fn a_full_syn_queue_counts_its_reset_separately() {
        use crate::{
            SocketOpts, UtpSocket,
            message::UtpMessage,
            raw::{Type, UtpHeader},
            test_util::{ADDR_1, ADDR_2, env::MockUtpEnvironment, transport::RememberingTransport},
        };

        let transport = RememberingTransport::new(ADDR_1);
        let (socket, mut dispatcher) = UtpSocket::new_with_opts_and_dispatcher(
            transport.clone(),
            MockUtpEnvironment::new(),
            SocketOpts::default(),
        )
        .unwrap();
        for id in 0..=super::ACCEPT_QUEUE_MAX_SYNS as u16 {
            dispatcher
                .on_recv(
                    ADDR_2,
                    UtpMessage::new_test(
                        UtpHeader {
                            htype: Type::Syn,
                            connection_id: id.into(),
                            seq_nr: id.into(),
                            ..Default::default()
                        },
                        b"",
                    ),
                )
                .await
                .unwrap();
        }
        let stats = socket.stats().snapshot();
        assert_eq!(stats.syn_rst, 1);
        assert_eq!(stats.syn_flood_dropped, 0);
        let sent = transport.take_sent_utpmessages();
        assert_eq!(sent.len(), 1);
        assert_eq!(sent[0].header.htype, Type::Reset);
        assert_eq!(
            sent[0].header.ack_nr,
            (super::ACCEPT_QUEUE_MAX_SYNS as u16).into()
        );
    }

    #[tokio::test]
    async fn test_echo() -> anyhow::Result<()> {
        setup_test_logging();
        let client_addr: SocketAddr = (Ipv4Addr::LOCALHOST, 1).into();
        let server_addr: SocketAddr = (Ipv4Addr::LOCALHOST, 2).into();

        let interface = MockInterface::new();

        let client = interface.create_socket(client_addr);
        let server = interface.create_socket(server_addr);

        async fn echo(s: MockUtpStream) -> anyhow::Result<()> {
            let (mut r, mut w) = s.split();
            w.write_u32(42).await.context("error writing 42")?;

            let read = r.read_u32().await.context("error reading u32")?;
            if read != 42 {
                anyhow::bail!("expected 42, got {}", read);
            }
            info!("received 42, closing echo");
            Ok(())
        }

        let connect = async {
            echo(
                client
                    .connect(server_addr)
                    .await
                    .context("error connecting")?,
            )
            .await
            .context("error running echo connect")
        }
        .instrument(debug_span!("connect"));
        let accept = async {
            echo(server.accept().await.context("error accepting")?)
                .await
                .context("error running echo accept")
        }
        .instrument(debug_span!("accept"));

        try_join!(connect, accept)?;
        Ok(())
    }
}
