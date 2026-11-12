// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver
// Lifted from librqbit-utp 0.7.0 (Apache-2.0), Copyright (c) the librqbit authors.
// Modified by the Weaver authors, 2026-11-12.

use std::{
    collections::{BTreeMap, VecDeque, btree_map::Entry},
    future::{Future, poll_fn},
    io::IoSlice,
    net::SocketAddr,
    num::NonZeroUsize,
    pin::Pin,
    sync::{
        Arc, Weak,
        atomic::{AtomicU64, AtomicUsize, Ordering},
    },
    task::{Poll, Wake, Waker},
    time::Duration,
};

use crate::{
    DatagramSend, Error, canonical_socket_addr,
    constants::{
        CONSECUTIVE_RECEIVE_ERRORS_MAX, TX_BUF_SIZE_PER_VSOCK_INITIAL_DEFAULT,
        TX_BUF_SIZE_PER_VSOCK_MAX_DEFAULT,
    },
    stats::{Counter, UtpStats},
};
use parking_lot::Mutex;
use tokio::time::Instant;
use tokio_util::sync::CancellationToken;

use crate::{UtpStream, runtime::Runtime, spawn_utils::task_with_cancel};

mod control;
pub(crate) use control::RetirementGuard;
use control::{ConnectAdmission, RetirementFlag, RetirementSignals, pending_connects};
#[cfg(test)]
mod control_integration_tests;

#[cfg(test)]
mod arrival_time_tests;
#[cfg(test)]
mod lifecycle_tests;
#[cfg(test)]
mod mapped_endpoint_tests;
#[cfg(test)]
mod send_handle_tests;
#[cfg(test)]
mod udp_error_tests;
use crate::{
    congestion::ledbat::LedbatParams,
    constants::{
        DEFAULT_CONNECT_TIMEOUT, DEFAULT_FIN_RESENDS, DEFAULT_MIN_TIMEOUT, DEFAULT_NUM_RESENDS,
        DEFAULT_REMOTE_INACTIVITY_TIMEOUT, DEFAULT_SYN_RESENDS, IPV4_HEADER,
        RX_BUF_SIZE_PER_VSOCK_DEFAULT, UDP_HEADER, UTP_HEADER,
    },
    message::UtpMessage,
    mtu::MtuCeiling,
    raw::{Type, UtpHeader},
    seq_nr::SeqNr,
    stream_dispatch::{StreamArgs, UtpStreamStarter},
    traits::{Transport, UtpEnvironment},
    utils::FnDropGuard,
};
use tokio::sync::{OwnedSemaphorePermit, mpsc, oneshot};
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

// An accept's answer channel. Dropped while still waiting, it closes the
// channel, so the dispatcher hands any later SYN to the next acceptor, and
// keeps a stream the dispatcher already sent for the next accept.
struct HandBackOnDrop<'a, T, E> {
    rx: Option<oneshot::Receiver<UtpStreamStarter<T, E>>>,
    handed_back: &'a Mutex<VecDeque<UtpStreamStarter<T, E>>>,
}

impl<T, E> Drop for HandBackOnDrop<'_, T, E> {
    fn drop(&mut self) {
        if let Some(mut rx) = self.rx.take() {
            rx.close();
            if let Ok(stream) = rx.try_recv() {
                self.handed_back.lock().push_back(stream);
            }
        }
    }
}

impl<V> RequestWithSpan<V> {
    fn new(tx: oneshot::Sender<V>) -> Self {
        Self {
            created_span: tracing::Span::current(),
            tx,
        }
    }
}

#[cfg(test)]
pub(crate) enum ControlRequest {
    ConnectRequest(SocketAddr, u64, ConnectRequest),
    Shutdown(StreamRecvKey),
}

#[cfg(test)]
impl std::fmt::Debug for ControlRequest {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ControlRequest::ConnectRequest(socket_addr, token, _) => {
                write!(f, "ConnectRequest({socket_addr}, {token})")
            }
            ControlRequest::Shutdown(key) => {
                write!(f, "Shutdown({key:?})")
            }
        }
    }
}

struct PendingConnectRequest {
    remote: SocketAddr,
    retired: Arc<RetirementFlag>,
    permit: OwnedSemaphorePermit,
    requester: ConnectRequest,
}

type ControlSend = Pin<Box<dyn Future<Output = std::io::Result<usize>> + Send + Sync>>;

struct PendingSend {
    future: Option<ControlSend>,
    result: Option<std::io::Result<usize>>,
}

impl PendingSend {
    fn new(future: ControlSend) -> Self {
        Self {
            future: Some(future),
            result: None,
        }
    }
    fn poll(&mut self, cx: &mut std::task::Context<'_>) -> bool {
        if let Some(future) = &mut self.future {
            match future.as_mut().poll(cx) {
                Poll::Pending => return false,
                Poll::Ready(result) => {
                    self.result = Some(result);
                    self.future.take();
                }
            }
        }
        true
    }
}

enum SendCompletion {
    Syn(SocketAddr, SeqNr, Arc<RetirementFlag>),
    Response(usize),
}

enum SynSend {
    Sending(PendingSend),
    Accepted(Instant),
}

struct Connecting {
    connection_id: SeqNr,
    seq_nr: SeqNr,
    retired: Arc<RetirementFlag>,
    permit: OwnedSemaphorePermit,
    send: SynSend,
    early_ack: Option<UtpMessage>,
    requester: ConnectRequest,
}

// The connects waiting on one remote address, by connection id. Nothing
// caps them per address: libtorrent opens as many uTP connections to one
// endpoint as it is asked to (v2.0.15 utp_socket_manager.cpp:323-342). Every
// entry holds a permit of the socket's connect admission, which bounds them
// all together.
#[derive(Default)]
struct ConnectingPerAddr {
    by_id: BTreeMap<u16, Connecting>,
}

impl ConnectingPerAddr {
    fn is_empty(&self) -> bool {
        self.by_id.is_empty()
    }

    fn insert(&mut self, c: Connecting) -> bool {
        match self.by_id.entry(c.connection_id.0) {
            Entry::Vacant(slot) => {
                slot.insert(c);
                true
            }
            Entry::Occupied(_) => false,
        }
    }

    fn contains(&self, connection_id: u16) -> bool {
        self.by_id.contains_key(&connection_id)
    }

    fn get_mut(&mut self, connection_id: SeqNr, seq_nr: SeqNr) -> Option<&mut Connecting> {
        self.by_id
            .get_mut(&connection_id.0)
            .filter(|c| c.seq_nr == seq_nr)
    }

    fn pop(&mut self, connection_id: SeqNr, seq_nr: SeqNr) -> Option<Connecting> {
        self.get_mut(connection_id, seq_nr)?;
        self.by_id.remove(&connection_id.0)
    }

    fn pop_by_flag(
        &mut self,
        connection_id: SeqNr,
        flag: &Arc<RetirementFlag>,
    ) -> Option<Connecting> {
        if !Arc::ptr_eq(&self.by_id.get(&connection_id.0)?.retired, flag) {
            return None;
        }
        self.by_id.remove(&connection_id.0)
    }
}

const ACCEPT_QUEUE_MAX_ACCEPTORS: usize = 32;
const ACCEPT_QUEUE_MAX_SYNS: usize = 32;
const CONTROL_RESPONSES_MAX: usize = 32;
// ST_RESETs the socket sends per interval, whatever prompts them.
const RESETS_PER_INTERVAL: usize = 16;
const RESET_INTERVAL: Duration = Duration::from_millis(100);
// Streams that ended whose send ids the socket remembers.
const TOMBSTONES_MAX: usize = 256;

pub(crate) struct RegisteredStream {
    tx: crate::ingress::Sender,
    retired: Arc<RetirementFlag>,
    // The remote's receive id for the stream, which its ST_RESET carries.
    send_id: SeqNr,
}

// The send ids of the streams that ended last, so a packet for one is
// answered with the id the remote knows it by. The oldest goes first.
#[derive(Default)]
struct Tombstones {
    order: VecDeque<StreamRecvKey>,
    send_ids: BTreeMap<StreamRecvKey, SeqNr>,
}

impl Tombstones {
    fn bury(&mut self, key: StreamRecvKey, send_id: SeqNr) {
        if self.send_ids.insert(key, send_id).is_some() {
            return;
        }
        self.order.push_back(key);
        if self.order.len() > TOMBSTONES_MAX
            && let Some(oldest) = self.order.pop_front()
        {
            self.send_ids.remove(&oldest);
        }
    }

    fn send_id(&self, key: &StreamRecvKey) -> Option<SeqNr> {
        self.send_ids.get(key).copied()
    }
}

// A fixed window of RESET_INTERVAL in which at most RESETS_PER_INTERVAL
// resets go out.
#[derive(Default)]
struct ResetBudget {
    window_start: Option<Instant>,
    sent: usize,
}

impl ResetBudget {
    fn take(&mut self, now: Instant) -> bool {
        if self
            .window_start
            .is_none_or(|start| now.saturating_duration_since(start) >= RESET_INTERVAL)
        {
            self.window_start = Some(now);
            self.sent = 0;
        }
        if self.sent >= RESETS_PER_INTERVAL {
            return false;
        }
        self.sent += 1;
        true
    }
}

struct Syn {
    remote: SocketAddr,
    header: UtpHeader,
    received: Instant,
}

enum MatchSynWithAccept<T, E> {
    Matched,
    SynInvalid(Acceptor<T, E>),
    // Boxed: a header carries a SACK of up to 255 bytes.
    ReceiverDead(Box<Syn>),
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

    pub(crate) streams: BTreeMap<StreamRecvKey, RegisteredStream>,
    connecting: BTreeMap<SocketAddr, ConnectingPerAddr>,
    connect_rx: mpsc::Receiver<PendingConnectRequest>,
    control_sends: Vec<PendingSend>,
    next_connection_id: SeqNr,
    tombstones: Tombstones,
    reset_budget: ResetBudget,
}

fn poll_control_sends(
    connecting: &mut BTreeMap<SocketAddr, ConnectingPerAddr>,
    responses: &mut [PendingSend],
    cx: &mut std::task::Context<'_>,
) -> Poll<SendCompletion> {
    for (remote, pending) in connecting {
        for connect in pending.by_id.values_mut() {
            let ready = if connect.retired.is_retired() || connect.requester.tx.is_closed() {
                true
            } else {
                match &mut connect.send {
                    SynSend::Sending(send) => send.poll(cx),
                    SynSend::Accepted(_) => false,
                }
            };
            if ready {
                return Poll::Ready(SendCompletion::Syn(
                    *remote,
                    connect.connection_id,
                    Arc::clone(&connect.retired),
                ));
            }
        }
    }
    for (index, response) in responses.iter_mut().enumerate() {
        if response.poll(cx) {
            return Poll::Ready(SendCompletion::Response(index));
        }
    }
    Poll::Pending
}

impl<T: Transport, E: UtpEnvironment> Dispatcher<T, E> {
    #[cfg(test)]
    fn register_stream(
        &mut self,
        key: StreamRecvKey,
        tx: crate::ingress::Sender,
    ) -> Option<RegisteredStream> {
        let (retired, mut guard) = self.socket.retirements.registration();
        guard.disarm();
        self.streams.insert(
            key,
            RegisteredStream {
                tx,
                retired,
                send_id: SeqNr(key.1) - 1,
            },
        )
    }
    fn finish_send(&mut self, completion: SendCompletion) -> crate::Result<()> {
        match completion {
            SendCompletion::Response(index) => {
                if let Some(Err(error)) = self.control_sends.swap_remove(index).result {
                    self.socket.stats.send_errors.increment(1);
                    debug!(%error, "error sending ST_RESET");
                }
            }
            SendCompletion::Syn(remote, connection_id, flag) => {
                let Some(mut entry) = self
                    .connecting
                    .get_mut(&remote)
                    .and_then(|pending| pending.pop_by_flag(connection_id, &flag))
                else {
                    return Ok(());
                };
                if self
                    .connecting
                    .get(&remote)
                    .is_some_and(ConnectingPerAddr::is_empty)
                {
                    self.connecting.remove(&remote);
                }
                if entry.retired.is_retired() || entry.requester.tx.is_closed() {
                    return Ok(());
                }
                let SynSend::Sending(mut send) = entry.send else {
                    return Err(Error::BugUnreachable);
                };
                match send.result.take() {
                    Some(Ok(len)) if len == UTP_HEADER as usize => {
                        entry.send = SynSend::Accepted(self.env.now());
                        if let Some(ack) = entry.early_ack.take() {
                            self.promote_connect(remote, entry, ack)?;
                        } else {
                            assert!(self.connecting.entry(remote).or_default().insert(entry));
                        }
                    }
                    result => {
                        let error = match result {
                            Some(Err(error)) => error,
                            _ => std::io::Error::new(
                                std::io::ErrorKind::WriteZero,
                                "the SYN datagram was not fully accepted",
                            ),
                        };
                        let _ = entry.requester.tx.send(Err(Error::ErrorSendingSyn(error)));
                    }
                }
            }
        }
        Ok(())
    }

    #[cfg(test)]
    async fn on_control(&mut self, control: ControlRequest) {
        match control {
            ControlRequest::Shutdown(key) => {
                self.streams.remove(&key);
            }
            ControlRequest::ConnectRequest(remote, _token, requester) => {
                let permit = self
                    .socket
                    .connect_admission
                    .acquire(&self.socket.cancellation_token)
                    .await
                    .unwrap();
                let (retired, mut guard) = self.socket.retirements.registration();
                guard.disarm();
                self.admit_connect(PendingConnectRequest {
                    remote: canonical_socket_addr(remote),
                    retired,
                    permit,
                    requester,
                });
                if self.connecting.values().any(|pending| {
                    pending
                        .by_id
                        .values()
                        .any(|connect| matches!(connect.send, SynSend::Sending(_)))
                }) {
                    let completion = poll_fn(|cx| {
                        poll_control_sends(&mut self.connecting, &mut self.control_sends, cx)
                    })
                    .await;
                    self.finish_send(completion).unwrap();
                }
            }
        }
    }
    pub(crate) async fn run_forever(mut self) -> crate::Result<()> {
        // Normal UDP datagrams carry at most u16::MAX - 8 payload bytes.
        let mut read_buf = [0u8; u16::MAX as usize];
        let mut completions = 0;
        let mut receive_errors = 0;

        loop {
            if self.socket.cancellation_token.is_cancelled() {
                return Ok(());
            }
            if let Err(e) = self.run_once(&mut read_buf, &mut receive_errors).await {
                warn!("error running dispatcher: {e:#}");
                return Err(e);
            }
            completions += 1;
            if completions == 64 {
                // A transport may repeatedly complete without yielding, for
                // errors or packets alike. Give cancellation and peers a turn.
                tokio::task::yield_now().await;
                completions = 0;
            }
        }
    }

    async fn run_once(
        &mut self,
        read_buf: &mut [u8],
        receive_errors: &mut usize,
    ) -> crate::Result<()> {
        let retirements = Arc::clone(&self.socket.retirements);
        let retired = retirements.notifications().notified();
        tokio::pin!(retired);
        retired.as_mut().enable();
        self.reap_retired();
        self.cleanup_accept_queue()?;

        tokio::select! {
            accept = self.accept_queue.rx.recv(), if self.accept_queue.next_available_acceptor.is_none() => {
                let accept = accept.unwrap();
                self.accept_queue.next_available_acceptor = Some(accept);
            }
            request = self.connect_rx.recv() => {
                if let Some(request) = request { self.admit_connect(request); }
            },
            _ = &mut retired => {},
            completion = poll_fn(|cx| poll_control_sends(&mut self.connecting, &mut self.control_sends, cx)) => {
                self.finish_send(completion)?;
            },
            recv = self.transport.as_deref().ok_or(Error::DispatcherDead)?.recv_from(read_buf) => {
                let received = self.env.now();
                let (len, addr) = match recv {
                    Ok(received) => {
                        *receive_errors = 0;
                        received
                    }
                    Err(error) if crate::udp_error::is_recoverable_receive_error(&error) => {
                        *receive_errors += 1;
                        if *receive_errors >= CONSECUTIVE_RECEIVE_ERRORS_MAX {
                            return Err(Error::Recv(error));
                        }
                        trace!(%error, "recoverable datagram receive error");
                        return Ok(());
                    }
                    Err(error) => return Err(Error::Recv(error)),
                };
                if len >= read_buf.len() {
                    self.socket.stats.invalid_packets.increment(1);
                    trace!(len, ?addr, "dropping datagram at receive buffer limit");
                    return Ok(());
                }
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
                    self.accept_queue.syns.push_front(*syn);
                }
            }
        }

        Ok(())
    }

    fn get_next_free_conn_id(&mut self, addr: SocketAddr) -> crate::Result<SeqNr> {
        for _ in 0..32768 {
            let candidate = self.next_connection_id;
            self.next_connection_id += 2;
            if !self.key_reserved((addr, candidate.0)) {
                return Ok(candidate);
            }
        }
        Err(Error::ConnectionIdsExhausted)
    }

    fn key_reserved(&self, key: StreamRecvKey) -> bool {
        self.streams.contains_key(&key)
            || self
                .connecting
                .get(&key.0)
                .is_some_and(|pending| pending.contains(key.1))
            || self
                .accept_queue
                .syns
                .iter()
                .any(|syn| syn.remote == key.0 && (syn.header.connection_id + 1).0 == key.1)
    }

    fn reap_retired(&mut self) {
        if self.socket.retirements.take_dirty() {
            let tombstones = &mut self.tombstones;
            self.streams.retain(|key, stream| {
                let live = !stream.retired.is_retired();
                if !live {
                    tombstones.bury(*key, stream.send_id);
                }
                live
            });
        }
        // A connect given up is remembered too: the remote may yet answer
        // the SYN, and its receive id is one above the one the SYN carried.
        let tombstones = &mut self.tombstones;
        self.connecting.retain(|remote, pending| {
            pending.by_id.retain(|_, connect| {
                let live = !connect.retired.is_retired() && !connect.requester.tx.is_closed();
                if !live {
                    tombstones.bury(
                        (*remote, connect.connection_id.0),
                        connect.connection_id + 1,
                    );
                }
                live
            });
            !pending.is_empty()
        });
    }

    fn admit_connect(&mut self, request: PendingConnectRequest) {
        self.reap_retired();
        let PendingConnectRequest {
            remote: addr,
            retired,
            permit,
            requester: sender,
        } = request;
        if retired.is_retired()
            || sender.tx.is_closed()
            || self.socket.cancellation_token.is_cancelled()
        {
            return;
        }
        let conn_id = match self.get_next_free_conn_id(addr) {
            Ok(id) => id,
            Err(error) => {
                let _ = sender.tx.send(Err(error));
                return;
            }
        };
        let header = UtpHeader {
            htype: Type::Syn,
            connection_id: conn_id,
            timestamp_microseconds: (self.env.now() - self.socket.created).as_micros() as u32,
            timestamp_difference_microseconds: 0,
            wnd_size: 0,
            seq_nr: self.env.random_u16().into(),
            ack_nr: 0.into(),
            extensions: Default::default(),
        };
        let mut buf = [0u8; UTP_HEADER as usize];
        header.serialize(&mut buf).unwrap();
        let Some(transport) = self.transport.clone() else {
            let _ = sender.tx.send(Err(Error::DispatcherDead));
            return;
        };
        let send = Box::pin(async move { transport.send_to(&buf, addr).await });
        let c = Connecting {
            connection_id: header.connection_id,
            seq_nr: header.seq_nr,
            requester: sender,
            retired,
            permit,
            send: SynSend::Sending(PendingSend::new(send)),
            early_ack: None,
        };
        assert!(self.connecting.entry(addr).or_default().insert(c));
    }

    fn on_maybe_connect_ack(&mut self, addr: SocketAddr, msg: UtpMessage) -> crate::Result<()> {
        let mut occ = match self.connecting.entry(addr) {
            Entry::Occupied(occ) => occ,
            Entry::Vacant(_) => {
                trace!(?msg, "no stream and no connect for the packet, resetting");
                self.reset_unknown_stream(addr, &msg.header);
                return Ok(());
            }
        };

        if let Some(connect) = occ
            .get_mut()
            .get_mut(msg.header.connection_id, msg.header.ack_nr)
            && matches!(connect.send, SynSend::Sending(_))
        {
            if connect.early_ack.is_none() {
                connect.early_ack = Some(msg);
            }
            return Ok(());
        }
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
            // A connect with this id may still be answered properly.
            if !occ.get().contains(msg.header.connection_id.0) {
                self.reset_unknown_stream(addr, &msg.header);
            }
            return Ok(());
        };

        self.promote_connect(addr, conn, msg)
    }

    /// A FIN or RESET that acknowledges a pending SYN ends that connect at
    /// once. libtorrent v2.0.15 answers a SYN it will not take with a FIN, and
    /// its own connecting socket treats a FIN or RESET that acknowledges the SYN
    /// as the end of the connection (utp_stream.cpp:2680-2693 for RESET,
    /// 2947-2990 for a FIN in syn_sent). Waiting for the connect timeout
    /// instead holds the attempt open for nothing.
    fn on_maybe_connect_refused(&mut self, addr: SocketAddr, msg: UtpMessage) {
        let Some(pending) = self.connecting.get_mut(&addr) else {
            trace!(?msg, ?addr, "no stream and no connect for the packet");
            self.reset_unknown_stream(addr, &msg.header);
            return;
        };
        let Some(conn) = pending.pop(msg.header.connection_id, msg.header.ack_nr) else {
            trace!(
                ?msg,
                ?addr,
                "dropping packet, it answers no pending connect"
            );
            if !pending.contains(msg.header.connection_id.0) {
                self.reset_unknown_stream(addr, &msg.header);
            }
            return;
        };
        if pending.is_empty() {
            self.connecting.remove(&addr);
        }
        let error = match msg.header.get_type() {
            Type::Reset => Error::StResetReceived,
            _ => Error::ConnectRefused,
        };
        debug!(?addr, %error, "connect refused by the remote");
        let _ = conn.requester.tx.send(Err(error));
    }

    fn promote_connect(
        &mut self,
        addr: SocketAddr,
        conn: Connecting,
        msg: UtpMessage,
    ) -> crate::Result<()> {
        let SynSend::Accepted(start) = conn.send else {
            return Err(Error::BugUnreachable);
        };
        if conn.retired.is_retired() || conn.requester.tx.is_closed() {
            return Ok(());
        }
        let recv_key = (addr, msg.header.connection_id.0);
        if self.streams.contains_key(&recv_key) {
            return Err(Error::BugUnreachable);
        }
        let (retired, guard) = self.socket.retirements.registration();
        let (tx, rx) = crate::ingress::channel(self.socket.opts.vsock_rx_bufsize);
        let args = StreamArgs::new_outgoing(&msg.header, start, msg.received)
            .with_parent_span(conn.requester.created_span.clone());
        let send_id = args.conn_id_send();
        self.streams.insert(
            recv_key,
            RegisteredStream {
                tx,
                retired,
                send_id,
            },
        );
        let stream = UtpStreamStarter::new_registered(&self.socket, addr, rx, args, guard).start();
        drop(conn.permit);
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
        if self.key_reserved(recv_key) {
            debug!(?recv_key, "SYN clashes with an existing stream, ignoring");
            self.socket.stats.invalid_packets.increment(1);
            return MatchSynWithAccept::SynInvalid(accept);
        }

        let args =
            StreamArgs::new_incoming(self.env.random_u16().into(), &syn.header, syn.received)
                .with_parent_span(accept.created_span.clone());
        let (tx, rx) = crate::ingress::channel(self.socket.opts.vsock_rx_bufsize);

        let (retired, guard) = self.socket.retirements.registration();
        let send_id = args.conn_id_send();
        let starter = UtpStreamStarter::new_registered(&self.socket, syn.remote, rx, args, guard);

        self.streams.insert(
            recv_key,
            RegisteredStream {
                tx,
                retired,
                send_id,
            },
        );
        match accept.tx.send(starter) {
            Ok(()) => {
                trace!("created stream and passed to acceptor");
                MatchSynWithAccept::Matched
            }
            Err(starter) => {
                starter.disarm();
                self.streams.remove(&recv_key);
                MatchSynWithAccept::ReceiverDead(Box::new(syn))
            }
        }
    }

    fn try_send_rst(&mut self, syn: Syn) {
        if !self.send_reset(syn.remote, syn.header.connection_id, syn.header.seq_nr) {
            self.socket.stats.syn_flood_dropped.increment(1);
        }
    }

    async fn on_syn(&mut self, remote: SocketAddr, msg: UtpMessage) -> crate::Result<()> {
        let future_key = (remote, (msg.header.connection_id + 1).0);
        if self.key_reserved(future_key) {
            self.socket.stats.invalid_packets.increment(1);
            return Ok(());
        }
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
        let limit = self.socket.connections_limit.load(Ordering::Relaxed);
        // A zero limit admits no stream at all.
        if limit == 0 || registered > limit.saturating_mul(2) {
            trace!(streams = registered, "dropping SYN, too many streams");
            self.socket.stats.syn_flood_dropped.increment(1);
            return Ok(());
        }
        let mut syn = Syn {
            remote,
            header: msg.header,
            received: msg.received,
        };
        while let Some(acceptor) = self.accept_queue.try_next_acceptor() {
            match self.match_syn_with_accept(syn, acceptor) {
                MatchSynWithAccept::Matched => return Ok(()),
                MatchSynWithAccept::SynInvalid(sender) => {
                    self.accept_queue.next_available_acceptor = Some(sender);
                    return Ok(());
                }
                MatchSynWithAccept::ReceiverDead(s) => syn = *s,
            }
        }
        match self.accept_queue.try_cache_syn(syn) {
            None => Ok(()),
            Some(syn) => {
                debug!("dropping SYN, no more space to cache them and no acceptors available");
                self.socket.stats.syn_rst.increment(1);
                self.try_send_rst(syn);
                Ok(())
            }
        }
    }

    async fn on_recv(&mut self, addr: SocketAddr, message: UtpMessage) -> crate::Result<()> {
        let addr = canonical_socket_addr(addr);
        let key = (addr, message.header.connection_id.0);

        if let Some(stream) = self.streams.get(&key) {
            if stream.tx.send(message).is_err() {
                trace!(
                    ?key,
                    "stream dead, but wasn't cleaned up yet, this is probably a race"
                );
                if let Some(stream) = self.streams.remove(&key) {
                    self.tombstones.bury(key, stream.send_id);
                }
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
            Type::Fin | Type::Reset => {
                self.on_maybe_connect_refused(addr, message);
            }
            Type::Data => {
                self.reset_unknown_stream(addr, &message.header);
            }
        }
        Ok(())
    }

    /// Answers a packet for a stream this socket does not hold with an
    /// ST_RESET, so the remote ends its side now rather than at its timeout.
    /// BEP 29 has a socket answer a packet for an unknown connection with a
    /// reset; libtorrent v2.0.15 answers every packet for a socket it is
    /// closing with one (utp_stream.cpp:3233-3239) and leaves the unknown
    /// case undone (utp_socket_manager.cpp:228-230). A reset is never
    /// answered.
    ///
    /// A stream that ended lately is answered with the id the remote knows
    /// it by. Any other is taken as one the remote opened, whose receive id
    /// is one below the id it sends with; that reset is left unsent when a
    /// live stream to the remote already sends with that id.
    ///
    /// The remote takes a reset only when it acknowledges a packet the
    /// remote sent (utp_stream.cpp:2624-2640, 2680-2693): a data packet or
    /// FIN takes its own sequence number, and a state packet, which takes
    /// none, the one before.
    fn reset_unknown_stream(&mut self, remote: SocketAddr, header: &UtpHeader) {
        let ack_nr = match header.get_type() {
            Type::Reset | Type::Syn => return,
            Type::State => header.seq_nr - 1,
            Type::Data | Type::Fin => header.seq_nr,
        };
        let key = (remote, header.connection_id.0);
        let send_id = match self.tombstones.send_id(&key) {
            Some(send_id) => send_id,
            None => {
                let send_id = header.connection_id - 1;
                let taken = self
                    .streams
                    .range((remote, ConnectionId::MIN)..=(remote, ConnectionId::MAX))
                    .any(|(_, stream)| stream.send_id == send_id);
                if taken {
                    return;
                }
                send_id
            }
        };
        if self.send_reset(remote, send_id, ack_nr) {
            self.socket.stats.unknown_stream_resets.increment(1);
        }
    }

    // Queues an ST_RESET within the socket's reset budget and the bound on
    // control sends; false when it is left unsent.
    fn send_reset(&mut self, remote: SocketAddr, connection_id: SeqNr, ack_nr: SeqNr) -> bool {
        if self.control_sends.len() == CONTROL_RESPONSES_MAX {
            trace!("dropping best-effort ST_RESET, response queue is full");
            return false;
        }
        if !self.reset_budget.take(self.env.now()) {
            self.socket.stats.resets_rate_limited.increment(1);
            trace!("dropping best-effort ST_RESET, reset budget spent");
            return false;
        }
        let mut buf: [u8; UTP_HEADER as usize] = [0; UTP_HEADER as usize];
        let packet = UtpHeader {
            htype: Type::Reset,
            connection_id,
            seq_nr: 0.into(),
            ack_nr,
            ..Default::default()
        };
        let Some(transport) = self.transport.clone() else {
            return false;
        };
        let Ok(len) = packet.serialize(&mut buf[..]) else {
            return false;
        };
        self.control_sends
            .push(PendingSend::new(Box::pin(async move {
                transport.send_to(&buf[..len], remote).await
            })));
        true
    }
}

pub struct UtpSocket<T: Transport, E> {
    pub(crate) state: Arc<SocketState<T, E>>,
    runtime: Arc<Runtime<T>>,
    // Incoming connections handed to an accept that was dropped before it
    // took them. The next accept takes them first, so dropping an accept
    // never leaves a remote's SYN unanswered. Held here rather than in the
    // shared state, which every stream refers to.
    handed_back: Mutex<VecDeque<UtpStreamStarter<T, E>>>,
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
    connect_requests: mpsc::Sender<PendingConnectRequest>,
    connect_admission: ConnectAdmission,
    pub(crate) retirements: Arc<RetirementSignals>,
    accept_requests: mpsc::Sender<Acceptor<T, E>>,

    pub(crate) env: E,

    local_addr: SocketAddr,
    link_mtu: Option<u16>,
    opts: ValidatedSocketOpts,
    connections_limit: AtomicUsize,

    pub(crate) cancellation_token: CancellationToken,

    pub(crate) stats: UtpStats,

    mtu_ceiling: Mutex<MtuCeiling>,

    // The one waker the socket's streams hand the transport, so its
    // writable wake reaches every stream it refused.
    send_wake: Arc<SendWake>,
    send_waker: Waker,

    // Why each retransmission timeout of the socket's streams came.
    #[cfg(test)]
    pub(crate) expiry_causes: Mutex<Vec<crate::stream_dispatch::ExpiryCause>>,
}

// A transport keeps one waker for its writable wake, the last it was handed
// (tokio 1.54.1 keeps one `writer` waker for poll sends, scheduled_io.rs:117-118;
// its async sends wait in a list apart). Streams sharing the transport all hand it
// this one, and it wakes each stream the transport refused, as libtorrent's
// socket manager does on writable (utp_socket_manager.cpp:235-250).
#[derive(Default)]
struct SendWake {
    // Moves on each writable wake.
    epoch: AtomicU64,
    waiters: Mutex<Vec<Waker>>,
}

impl Wake for SendWake {
    fn wake(self: Arc<Self>) {
        self.wake_by_ref();
    }

    fn wake_by_ref(self: &Arc<Self>) {
        self.epoch.fetch_add(1, Ordering::SeqCst);
        let waiters = std::mem::take(&mut *self.waiters.lock());
        for waker in waiters {
            waker.wake();
        }
    }
}

/// A stream's place in its socket's writable wakes: a stream the shared
/// transport refused tries it again only after the next one.
#[derive(Debug, Default)]
pub(crate) struct SendGate {
    refused_at: Option<u64>,
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
        let transport = Arc::new(transport);
        let runtime = Runtime::new(transport, &opts.cancellation_token);

        let (accept_tx, accept_rx) = mpsc::channel(ACCEPT_QUEUE_MAX_ACCEPTORS);
        let pending_connects = pending_connects(opts.connections_limit);
        let (connect_tx, connect_rx) = mpsc::channel(pending_connects);

        let send_wake = Arc::new(SendWake::default());
        let state = Arc::new(SocketState {
            runtime: Arc::downgrade(&runtime),
            created: env.now(),
            connect_requests: connect_tx,
            connect_admission: ConnectAdmission::new(pending_connects),
            retirements: Arc::default(),
            local_addr,
            link_mtu,
            opts: validated_opts,
            connections_limit: AtomicUsize::new(
                opts.connections_limit.map_or(usize::MAX, NonZeroUsize::get),
            ),
            env: env.copy(),
            accept_requests: accept_tx,
            cancellation_token: runtime.cancellation().clone(),
            stats: UtpStats::default(),
            mtu_ceiling: Default::default(),
            send_waker: Waker::from(send_wake.clone()),
            send_wake,
            #[cfg(test)]
            expiry_causes: Default::default(),
        });

        let sock = Arc::new(Self {
            state: state.clone(),
            runtime,
            handed_back: Mutex::default(),
        });
        let dispatcher = Dispatcher {
            streams: BTreeMap::default(),
            connecting: BTreeMap::default(),
            next_connection_id: env.random_u16().into(),
            connect_rx,
            control_sends: Vec::new(),
            tombstones: Tombstones::default(),
            reset_budget: ResetBudget::default(),
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

    /// Cancel-safe: dropping the future loses no incoming connection.
    pub async fn accept(self: &Arc<Self>) -> crate::Result<UtpStream> {
        self.state.accept(&self.handed_back).await
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
    // died on large packets allow. A stream probes whatever the transport
    // does with an oversized datagram, as libtorrent's does
    // (utp_stream.cpp:1503-1505 checks no socket flag).
    pub(crate) fn probe_link_mtu(&self) -> u16 {
        let link_mtu = self
            .runtime
            .upgrade()
            .and_then(|runtime| runtime.link_mtu())
            .unwrap_or(self.link_mtu);
        self.opts
            .link_mtu
            .min(link_mtu.unwrap_or(u16::MAX))
            .min(self.mtu_ceiling.lock().get())
    }

    // A stream died on a packet of `mtu` bytes larger than it had seen
    // delivered.
    pub(crate) fn restrict_mtu(&self, mtu: u16) {
        self.mtu_ceiling.lock().restrict(mtu);
    }

    pub(crate) fn opts(&self) -> &ValidatedSocketOpts {
        &self.opts
    }

    async fn accept(
        self: &Arc<Self>,
        handed_back: &Mutex<VecDeque<UtpStreamStarter<T, Env>>>,
    ) -> crate::Result<UtpStream> {
        if self.cancellation_token.is_cancelled() {
            return Err(Error::DispatcherDead);
        }
        let waiting = handed_back.lock().pop_front();
        if let Some(stream) = waiting {
            let stream = stream.start()?;
            self.stats.accepts.increment(1);
            trace!("accepted");
            return Ok(stream);
        }

        let (tx, rx) = oneshot::channel();
        let mut rx = HandBackOnDrop {
            rx: Some(rx),
            handed_back,
        };

        self.stats.accepting.increment(1);
        let _accepting_guard = FnDropGuard::new(|| self.stats.accepting.decrement(1));

        tokio::select! {
            biased;
            _ = self.cancellation_token.cancelled() => return Err(Error::DispatcherDead),
            sent = self.accept_requests.send(RequestWithSpan::new(tx)) => sent.map_err(|_| Error::DispatcherDead)?,
        }
        let receiver = rx.rx.as_mut().ok_or(Error::BugUnreachable)?;
        let stream = tokio::select! {
            biased;
            _ = self.cancellation_token.cancelled() => return Err(Error::DispatcherDead),
            received = receiver => received.map_err(|_| Error::DispatcherDead)?,
        };
        rx.rx = None;
        let stream = stream.start()?;
        self.stats.accepts.increment(1);
        trace!("accepted");
        Ok(stream)
    }

    pub async fn connect(self: &Arc<Self>, remote: SocketAddr) -> crate::Result<UtpStream> {
        if self.cancellation_token.is_cancelled() {
            return Err(Error::DispatcherDead);
        }
        let remote = canonical_socket_addr(remote);
        self.stats.connection_attempts.increment(1);
        self.stats.connecting.increment(1);

        let mut fail_guard = FnDropGuard::new(|| {
            self.stats.connection_failures.increment(1);
        });

        let _connecting_guard = FnDropGuard::new(|| {
            self.stats.connecting.decrement(1);
        });

        // The timeout runs from the SYN request, not from the wait for
        // admission.
        let permit = self
            .connect_admission
            .acquire(&self.cancellation_token)
            .await?;
        if self.cancellation_token.is_cancelled() {
            return Err(Error::DispatcherDead);
        }
        let (tx, rx) = oneshot::channel();
        let (retired, mut guard) = self.retirements.registration();
        self.connect_requests
            .try_send(PendingConnectRequest {
                remote,
                retired,
                permit,
                requester: RequestWithSpan::new(tx),
            })
            .map_err(|_| Error::DispatcherDead)?;
        let stream_or_err = tokio::time::timeout(self.opts.connect_timeout, async {
            let result = tokio::select! {
                biased;
                _ = self.cancellation_token.cancelled() => return Err(Error::DispatcherDead),
                result = rx => result.map_err(|_| Error::DispatcherDead)?,
            };
            guard.disarm();
            result
        })
        .await
        .map_err(|_| Error::ConnectTimedOut)?;
        if stream_or_err.is_ok() {
            fail_guard.disarm();
            self.stats.connection_successes.increment(1);
        }

        stream_or_err
    }

    // Polls one send. A stream with its own registration polls it with its
    // own waker. On the shared transport a stream the transport refused
    // waits, without polling it, for the next writable wake; a refusal
    // puts the stream among those that wake reaches. None: still waiting.
    fn poll_send_gated<R>(
        &self,
        gate: &mut SendGate,
        cx: &mut std::task::Context<'_>,
        shared: bool,
        send: impl FnOnce(&mut std::task::Context<'_>) -> Poll<R>,
    ) -> Option<Poll<R>> {
        if !shared {
            return Some(send(cx));
        }
        let epoch = self.send_wake.epoch.load(Ordering::SeqCst);
        if gate.refused_at == Some(epoch) {
            return None;
        }
        gate.refused_at = None;
        let sent = send(&mut std::task::Context::from_waker(&self.send_waker));
        if sent.is_pending() {
            gate.refused_at = Some(epoch);
            {
                let mut waiters = self.send_wake.waiters.lock();
                if !waiters.iter().any(|w| w.will_wake(cx.waker())) {
                    waiters.push(cx.waker().clone());
                }
            }
            // A wake between the refusal and the line above found no waiter.
            if self.send_wake.epoch.load(Ordering::SeqCst) != epoch {
                cx.waker().wake_by_ref();
            }
        }
        Some(sent)
    }

    /// Returns true if saw Poll::Pending
    pub(crate) fn try_poll_send_to(
        &self,
        transport: &T,
        sender: Option<&mut (dyn DatagramSend + 'static)>,
        gate: &mut SendGate,
        cx: &mut std::task::Context<'_>,
        buf: &[u8],
        addr: SocketAddr,
    ) -> std::io::Result<bool> {
        let shared = sender.is_none();
        let sent = self.poll_send_gated(gate, cx, shared, |cx| match sender {
            Some(sender) => sender.poll_send_to(cx, buf, addr),
            None => transport.poll_send_to(cx, buf, addr),
        });
        let Some(sent) = sent else {
            return Ok(true);
        };
        match sent {
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
        sender: Option<&mut (dyn DatagramSend + 'static)>,
        gate: &mut SendGate,
        cx: &mut std::task::Context<'_>,
        bufs: &[IoSlice<'_>],
        addr: SocketAddr,
    ) -> std::io::Result<bool> {
        let total_len: usize = bufs.iter().map(|buf| buf.len()).sum();
        let shared = sender.is_none();
        let sent = self.poll_send_gated(gate, cx, shared, |cx| match sender {
            Some(sender) => sender.poll_send_to_vectored(cx, bufs, addr),
            None => transport.poll_send_to_vectored(cx, bufs, addr),
        });
        let Some(sent) = sent else {
            return Ok(true);
        };
        match sent {
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
            assert!(dispatcher.register_stream(key, tx).is_none());
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
        assert_eq!(dispatcher.get_next_free_conn_id(ADDR_2).unwrap(), 2.into());
        dispatcher
            .on_control(ControlRequest::Shutdown((ADDR_2, 0)))
            .await;
        dispatcher.next_connection_id = 65534.into();
        assert_eq!(dispatcher.get_next_free_conn_id(ADDR_2).unwrap(), 0.into());
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
                    received: _socket.state.created,
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
        assert!(dispatcher.streams[&(ADDR_2, 0)].retired.is_retired());
        dispatcher.reap_retired();
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
            dispatcher.register_stream((ADDR_2, 1000 + id), tx);
            open.push(rx);
        }

        dispatcher.on_recv(ADDR_2, syn(1)).await.unwrap();
        assert_eq!(dispatcher.accept_queue.syns.len(), 1, "taken up");

        let (tx, rx) = crate::ingress::channel(socket.opts().vsock_rx_bufsize);
        dispatcher.register_stream((ADDR_2, 2000), tx);
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
    async fn a_zero_connection_limit_admits_no_syn() {
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
        socket.set_connections_limit(0);
        let syn = UtpMessage::new_test(
            UtpHeader {
                htype: Type::Syn,
                connection_id: 1.into(),
                ..Default::default()
            },
            b"",
        );
        dispatcher.on_recv(ADDR_2, syn).await.unwrap();
        assert!(dispatcher.accept_queue.syns.is_empty());
        assert!(dispatcher.streams.is_empty());
        assert_eq!(socket.stats().snapshot().syn_flood_dropped, 1);
        assert!(transport.take_sent_utpmessages().is_empty(), "unanswered");
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
        let completion = std::future::poll_fn(|cx| {
            super::poll_control_sends(
                &mut dispatcher.connecting,
                &mut dispatcher.control_sends,
                cx,
            )
        })
        .await;
        dispatcher.finish_send(completion).unwrap();
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
