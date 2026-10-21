// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver
//
// Derived from rqbit (https://github.com/ikatson/rqbit), Copyright 2021 Igor Katson,
// licensed under the Apache License 2.0 (see LICENSE-APACHE). Modified by the
// Weaver authors from 2026-10-20 onward.

use std::{
    net::{Ipv6Addr, SocketAddr, SocketAddrV4, SocketAddrV6},
    str::FromStr,
    sync::{
        Arc,
        atomic::{AtomicU16, Ordering},
    },
    task::Poll,
    time::{Duration, Instant},
};

use crate::bencode::ByteBufOwned;
use crate::dht::{
    Error, INACTIVITY_TIMEOUT, REQUERY_INTERVAL, RESPONSE_TIMEOUT,
    bprotocol::{
        self, AnnouncePeer, CompactNodeInfo, CompactNodeInfoOwned, ErrorDescription,
        FindNodeRequest, GetPeersRequest, Message, MessageKind, Node, PingRequest, Response, Want,
    },
    peer_store::PeerStore,
    routing_table::{InsertResult, NodeStatus, RoutingTable},
};
use backon::{ExponentialBuilder, Retryable};
use dashmap::DashMap;
use futures::{FutureExt, Stream, StreamExt, future::BoxFuture, stream::FuturesUnordered};

use crate::core::{
    compact_ip::{CompactSerialize, CompactSerializeFixedLen},
    hash_id::Id20,
    net::{DatagramSocket, Resolver},
    peer_id::generate_azereus_style,
    spawn_utils::{spawn, spawn_with_cancel},
};
use crate::crate_version;
use governor::{
    Quota, clock::Clock, middleware::NoOpMiddleware, nanos::Nanos, state::InMemoryState,
    state::NotKeyed,
};
use librqbit_dualstack_sockets::{BindDevice, UdpSocket};
use parking_lot::RwLock;
use std::num::NonZeroU32;

use serde::Serialize;
use tokio::sync::mpsc::{Sender, UnboundedReceiver, UnboundedSender, channel, unbounded_channel};

use tokio_util::{sync::CancellationToken, task::TaskTracker};
use tracing::{Instrument, debug, debug_span, info, trace, warn};

fn now() -> Instant {
    Instant::now()
}

#[derive(Debug, Serialize)]
pub struct DhtStats {
    #[serde(serialize_with = "crate::dht::utils::serialize_id20")]
    pub id: Id20,
    pub outstanding_requests: usize,
    pub routing_table_size: usize,
    pub routing_table_size_v6: usize,
}

struct OutstandingRequest {
    done: tokio::sync::oneshot::Sender<crate::dht::Result<ResponseOrError>>,
}

// A request's entry in `inflight_by_transaction_id`, from the moment the request
// is issued. Dropping it withdraws the entry, however the request ended.
struct Outstanding<'a> {
    dht: &'a DhtState,
    key: (u16, SocketAddr),
}

impl<'a> Outstanding<'a> {
    fn register(
        dht: &'a DhtState,
        key: (u16, SocketAddr),
    ) -> (
        Self,
        tokio::sync::oneshot::Receiver<crate::dht::Result<ResponseOrError>>,
    ) {
        let (done, answer) = tokio::sync::oneshot::channel();
        dht.inflight_by_transaction_id
            .insert(key, OutstandingRequest { done });
        (Self { dht, key }, answer)
    }
}

impl Drop for Outstanding<'_> {
    fn drop(&mut self) {
        self.dht.inflight_by_transaction_id.remove(&self.key);
    }
}

pub struct WorkerSendRequest {
    // If this is set, we are tracking the response in inflight_by_transaction_id
    our_tid: Option<u16>,
    message: Message<ByteBufOwned>,
    addr: SocketAddr,
}

// The requests one search keeps in flight: libtorrent's `dht_search_branching`.
const SEARCH_BRANCHING: usize = 5;
// The candidates one search keeps, closest first; libtorrent's traversal keeps
// 100. A candidate pushed past the end is dropped, and since the kept ones only
// get closer, it never comes back.
const SEARCH_CANDIDATES: usize = 100;
// k: a search is complete once the k closest candidates have answered.
const SEARCH_K: usize = 8;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CandidateState {
    Fresh,
    InFlight,
    Answered,
    Failed,
}

#[derive(Debug)]
struct Candidate {
    // None for a bootstrap address, whose id we do not know yet.
    id: Option<Id20>,
    addr: SocketAddr,
    state: CandidateState,
}

// The nodes one search knows of, closest to its target first, the ones without
// an id last. Each is asked at most once.
struct Candidates {
    target: Id20,
    list: Vec<Candidate>,
}

impl Candidates {
    fn new(target: Id20) -> Self {
        Self {
            target,
            list: Vec::new(),
        }
    }

    fn rank(&self, id: Option<Id20>) -> (bool, Option<Id20>) {
        (id.is_none(), id.map(|id| id.distance(&self.target)))
    }

    fn offer(&mut self, id: Option<Id20>, addr: SocketAddr) {
        if self
            .list
            .iter()
            .any(|c| c.addr == addr || (id.is_some() && c.id == id))
        {
            return;
        }
        let rank = self.rank(id);
        let pos = self.list.partition_point(|c| self.rank(c.id) <= rank);
        if pos >= SEARCH_CANDIDATES {
            return;
        }
        self.list.insert(
            pos,
            Candidate {
                id,
                addr,
                state: CandidateState::Fresh,
            },
        );
        self.list.truncate(SEARCH_CANDIDATES);
    }

    // The closest candidate not asked yet, unless the k closest that answered
    // are all closer than it.
    fn next_to_ask(&mut self) -> Option<&mut Candidate> {
        let mut answered = 0;
        for c in self.list.iter_mut() {
            if answered == SEARCH_K {
                return None;
            }
            match c.state {
                CandidateState::Answered => answered += 1,
                CandidateState::Fresh => return Some(c),
                CandidateState::InFlight | CandidateState::Failed => {}
            }
        }
        None
    }

    fn settle(&mut self, addr: SocketAddr, state: CandidateState) {
        if let Some(c) = self.list.iter_mut().find(|c| c.addr == addr) {
            c.state = state;
        }
    }
}

#[derive(Debug, Default, PartialEq, Eq)]
struct SearchOutcome {
    answered: usize,
    failed: usize,
}

// The outgoing rate limiter reads tokio's clock, the one every DHT deadline
// runs on, so a wait on the limiter and the deadline that bounds it are measured
// in the same time.
#[derive(Debug, Clone, Copy)]
struct TokioClock {
    start: tokio::time::Instant,
}

impl Clock for TokioClock {
    type Instant = Nanos;

    fn now(&self) -> Nanos {
        Nanos::from(tokio::time::Instant::now() - self.start)
    }
}

type RateLimiter =
    governor::RateLimiter<NotKeyed, InMemoryState, TokioClock, NoOpMiddleware<Nanos>>;

// Resolves once the limiter lets one more request out.
async fn until_ready(limiter: &RateLimiter) {
    while let Err(not_until) = limiter.check() {
        tokio::time::sleep(not_until.wait_time_from(limiter.clock().now())).await;
    }
}

fn make_rate_limiter() -> RateLimiter {
    // TODO: move to configuration, i'm lazy.
    let dht_queries_per_second = std::env::var("DHT_QUERIES_PER_SECOND")
        .map(|v| v.parse().expect("couldn't parse DHT_QUERIES_PER_SECOND"))
        .unwrap_or(250u32);

    // A bucket of `dht_queries_per_second` that refills at a tenth of it per
    // 100 ms and starts holding that tenth.
    let per_100_ms = NonZeroU32::new(dht_queries_per_second / 10)
        .expect("DHT_QUERIES_PER_SECOND must be at least 10");
    let max = NonZeroU32::new(dht_queries_per_second).expect("at least 10");
    let quota = Quota::with_period(Duration::from_millis(100) / per_100_ms.get())
        .expect("the refill period is not zero")
        .allow_burst(max);
    let limiter = RateLimiter::direct_with_clock(
        quota,
        TokioClock {
            start: tokio::time::Instant::now(),
        },
    );
    if let Some(spent) = NonZeroU32::new(max.get() - per_100_ms.get()) {
        let _ = limiter.check_n(spent);
    }
    limiter
}

trait RecursiveRequestCallbacks: Sized + Send + Sync + 'static {
    fn on_request_start(&self, req: &RecursiveRequest<Self>, target_node: Id20, addr: SocketAddr);
    fn on_request_end(
        &self,
        req: &RecursiveRequest<Self>,
        target_node: Id20,
        addr: SocketAddr,
        resp: &crate::dht::Result<ResponseOrError>,
    );
}

struct RecursiveRequestCallbacksGetPeers {
    // Id20::from_str("00000fffffffffffffffffffffffffffffffffff").unwrap()
    min_distance_to_announce: Id20,
    announce_port: Option<u16>,
}

impl RecursiveRequestCallbacks for RecursiveRequestCallbacksGetPeers {
    fn on_request_start(&self, _: &RecursiveRequest<Self>, _: Id20, _: SocketAddr) {}

    fn on_request_end(
        &self,
        req: &RecursiveRequest<Self>,
        target_node: Id20,
        addr: SocketAddr,
        resp: &crate::dht::Result<ResponseOrError>,
    ) {
        let announce_port = match self.announce_port {
            Some(a) => a,
            None => return,
        };
        let resp = match resp {
            Ok(ResponseOrError::Response(resp)) => resp,
            _ => return,
        };
        let token = match &resp.token {
            Some(token) => token,
            None => return,
        };
        if req.info_hash.distance(&target_node) > self.min_distance_to_announce {
            trace!(
                "not announcing, {:?} is too far from {:?}",
                target_node, req.info_hash
            );
            return;
        }
        let (tid, message) = req.dht.create_request(
            Request::Announce {
                info_hash: req.info_hash,
                token: token.clone(),
                port: announce_port,
            },
            addr,
        );

        let _ = req.dht.worker_sender.send(WorkerSendRequest {
            our_tid: Some(tid),
            message,
            addr,
        });
    }
}

struct RecursiveRequestCallbacksFindNodes {}
impl RecursiveRequestCallbacks for RecursiveRequestCallbacksFindNodes {
    fn on_request_start(&self, req: &RecursiveRequest<Self>, target_node: Id20, addr: SocketAddr) {
        let mut rt = req.dht.get_table_for_addr(addr).write();
        match rt.add_node(target_node, addr) {
            InsertResult::WasExisting | InsertResult::ReplacedBad(_) | InsertResult::Added => {
                rt.mark_outgoing_request(&target_node, now());
            }
            InsertResult::Ignored => {}
        }
    }

    fn on_request_end(
        &self,
        req: &RecursiveRequest<Self>,
        target_node: Id20,
        addr: SocketAddr,
        resp: &crate::dht::Result<ResponseOrError>,
    ) {
        let mut table = req.dht.get_table_for_addr(addr).write();
        if resp.is_ok() {
            table.mark_response(&target_node, now());
        } else {
            table.mark_error(&target_node);
        }
    }
}

struct RecursiveRequest<C: RecursiveRequestCallbacks> {
    info_hash: Id20,
    request: Request,
    dht: Arc<DhtState>,
    peer_tx: tokio::sync::mpsc::UnboundedSender<SocketAddr>,
    callbacks: C,
}

pub struct RequestPeersStream {
    rx: tokio::sync::mpsc::UnboundedReceiver<SocketAddr>,
    cancel_join_handle_v4: tokio::task::JoinHandle<()>,
    cancel_join_handle_v6: tokio::task::JoinHandle<()>,
}

impl RequestPeersStream {
    fn new(dht: Arc<DhtState>, info_hash: Id20, announce_port: Option<u16>) -> Self {
        let (peer_tx, peer_rx) = unbounded_channel();
        let make = |is_v4: bool, dht: Arc<DhtState>, peer_tx: UnboundedSender<SocketAddr>| {
            Arc::new(RecursiveRequest::get_peers(
                dht,
                info_hash,
                announce_port,
                peer_tx,
            ))
            .request_peers_forever(is_v4)
        };

        let v4 = make(true, dht.clone(), peer_tx.clone());
        let v6 = make(false, dht, peer_tx);

        Self {
            rx: peer_rx,
            cancel_join_handle_v4: v4,
            cancel_join_handle_v6: v6,
        }
    }
}

impl Drop for RequestPeersStream {
    fn drop(&mut self) {
        self.cancel_join_handle_v4.abort();
        self.cancel_join_handle_v6.abort();
    }
}

impl Stream for RequestPeersStream {
    type Item = SocketAddr;

    #[inline(never)]
    fn poll_next(
        mut self: std::pin::Pin<&mut Self>,
        cx: &mut std::task::Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        self.rx.poll_recv(cx)
    }
}

impl RecursiveRequest<RecursiveRequestCallbacksFindNodes> {
    async fn find_node_for_routing_table(
        dht: Arc<DhtState>,
        target: Id20,
        addrs: impl Iterator<Item = SocketAddr>,
    ) -> crate::dht::Result<()> {
        let req = RecursiveRequest {
            info_hash: target,
            request: Request::FindNode(target),
            dht,
            peer_tx: unbounded_channel().0,
            callbacks: RecursiveRequestCallbacksFindNodes {},
        };
        let outcome = req
            .search(addrs.map(|addr| (None, addr)))
            .instrument(debug_span!("find_node", target = format!("{target:?}")))
            .await?;
        if outcome.answered == 0 {
            return Err(Error::NoSuccessfulLookups {
                errors: outcome.failed,
            });
        }
        debug!(
            "finished, answered = {}, failed = {}",
            outcome.answered, outcome.failed
        );
        Ok(())
    }
}

impl RecursiveRequest<RecursiveRequestCallbacksGetPeers> {
    fn get_peers(
        dht: Arc<DhtState>,
        info_hash: Id20,
        announce_port: Option<u16>,
        peer_tx: UnboundedSender<SocketAddr>,
    ) -> Self {
        RecursiveRequest {
            info_hash,
            request: Request::GetPeers(info_hash),
            dht,
            peer_tx,
            callbacks: RecursiveRequestCallbacksGetPeers {
                min_distance_to_announce: Id20::from_str(
                    "0000ffffffffffffffffffffffffffffffffffff",
                )
                .unwrap(),
                announce_port,
            },
        }
    }

    // One search at a time, from the closest nodes of the routing table, again
    // every minute, or sooner while the table is short of nodes.
    fn request_peers_forever(self: Arc<Self>, is_v4: bool) -> tokio::task::JoinHandle<()> {
        spawn::<Error>(
            debug_span!(parent: None, "get_peers", is_v4, info_hash = format!("{:?}", self.info_hash)),
            "get_peers",
            async move {
                loop {
                    let seeds = self.closest_known(is_v4);
                    let known = seeds.len();
                    if known > 0 {
                        let outcome = self.search(seeds.into_iter()).await?;
                        trace!(?outcome, "search finished");
                    }
                    let pause = match known {
                        0 => Duration::from_secs(1),
                        n if n < SEARCH_K => REQUERY_INTERVAL / SEARCH_K as u32 * n as u32,
                        _ => REQUERY_INTERVAL,
                    };
                    tokio::time::sleep(pause).await;
                }
            },
        )
    }

    fn closest_known(&self, is_v4: bool) -> Vec<(Option<Id20>, SocketAddr)> {
        let table = if is_v4 {
            &self.dht.routing_table_v4
        } else {
            &self.dht.routing_table_v6
        };
        table
            .read()
            .sorted_by_distance_from(self.info_hash, now())
            .iter()
            .map(|n| (Some(n.id()), n.addr()))
            .take(SEARCH_K)
            .collect()
    }
}

impl<C: RecursiveRequestCallbacks> RecursiveRequest<C> {
    // Walk towards the target from `seeds`: ask the closest candidates not asked
    // yet, at most SEARCH_BRANCHING at a time, and offer the nodes each answer
    // names as new candidates. A node that fails or does not answer in time is
    // marked failed and skipped. The search ends when nothing is in flight and
    // no candidate is left to ask.
    async fn search(
        &self,
        seeds: impl Iterator<Item = (Option<Id20>, SocketAddr)>,
    ) -> crate::dht::Result<SearchOutcome> {
        let mut candidates = Candidates::new(self.info_hash);
        for (id, addr) in seeds {
            candidates.offer(id, addr);
        }
        let mut in_flight = FuturesUnordered::new();
        let mut outcome = SearchOutcome::default();
        loop {
            while in_flight.len() < SEARCH_BRANCHING {
                let Some(candidate) = candidates.next_to_ask() else {
                    break;
                };
                candidate.state = CandidateState::InFlight;
                let (id, addr) = (candidate.id, candidate.addr);
                in_flight.push(
                    self.ask(id, addr)
                        .map(move |r| (addr, r))
                        .instrument(debug_span!("ask", addr = addr.to_string())),
                );
            }
            let Some((addr, answer)) = in_flight.next().await else {
                return Ok(outcome);
            };
            let response = match answer {
                Ok(response) => response,
                Err(e) => {
                    debug!("{addr}: {e:#}");
                    outcome.failed += 1;
                    candidates.settle(addr, CandidateState::Failed);
                    continue;
                }
            };
            outcome.answered += 1;
            candidates.settle(addr, CandidateState::Answered);
            for peer in response.values.into_iter().flatten() {
                self.peer_tx.send(peer.0).ok().ok_or(Error::ReceiverDead)?;
            }
            let nodes = response
                .nodes
                .iter()
                .flat_map(|n| n.iter().map(|n| n.as_socketaddr()))
                .chain(
                    response
                        .nodes6
                        .iter()
                        .flat_map(|n| n.iter().map(|n| n.as_socketaddr())),
                )
                .filter(|node| addr.is_ipv4() == node.addr.is_ipv4());
            for node in nodes {
                candidates.offer(Some(node.id), node.addr);
            }
        }
    }

    async fn ask(
        &self,
        id: Option<Id20>,
        addr: SocketAddr,
    ) -> crate::dht::Result<Response<ByteBufOwned>> {
        if let Some(id) = id {
            self.callbacks.on_request_start(self, id, addr);
        }
        let response = self.dht.request(self.request.clone(), addr).await;
        if let Some(id) = id {
            self.callbacks.on_request_end(self, id, addr, &response);
        }
        match response? {
            ResponseOrError::Response(r) => Ok(r),
            ResponseOrError::Error(e) => {
                debug!("error response: {e:?}");
                Err(Error::ErrorResponse)
            }
        }
    }
}

pub struct DhtState {
    id: Id20,
    next_transaction_id: AtomicU16,

    // Created requests: (transaction_id, addr) => Requests.
    // If we get a response, it gets removed from here.
    inflight_by_transaction_id: DashMap<(u16, SocketAddr), OutstandingRequest>,

    routing_table_v4: RwLock<RoutingTable>,
    routing_table_v6: RwLock<RoutingTable>,
    listen_addr: SocketAddr,

    // Sending requests to the worker.
    rate_limiter: RateLimiter,
    // This is to send raw messages
    worker_sender: UnboundedSender<WorkerSendRequest>,

    cancellation_token: CancellationToken,

    pub(crate) peer_store: PeerStore,

    // BEP 5 "v": sent with every outgoing message when set.
    client_version: Option<ByteBufOwned>,
}

impl DhtState {
    #[allow(clippy::too_many_arguments)]
    fn new_internal(
        id: Id20,
        sender: UnboundedSender<WorkerSendRequest>,
        routing_table_v4: Option<RoutingTable>,
        routing_table_v6: Option<RoutingTable>,
        listen_addr: SocketAddr,
        peer_store: PeerStore,
        cancellation_token: CancellationToken,
        client_version: Option<ByteBufOwned>,
    ) -> Self {
        let routing_table_v4 = routing_table_v4.unwrap_or_else(|| RoutingTable::new(id, None));
        let routing_table_v6 = routing_table_v6.unwrap_or_else(|| RoutingTable::new(id, None));
        Self {
            id,
            next_transaction_id: AtomicU16::new(0),
            inflight_by_transaction_id: Default::default(),
            routing_table_v4: RwLock::new(routing_table_v4),
            routing_table_v6: RwLock::new(routing_table_v6),
            worker_sender: sender,
            listen_addr,
            rate_limiter: make_rate_limiter(),
            peer_store,
            cancellation_token,
            client_version,
        }
    }

    async fn request(
        &self,
        request: Request,
        addr: SocketAddr,
    ) -> crate::dht::Result<ResponseOrError> {
        // A request lives RESPONSE_TIMEOUT from the moment it is issued: first
        // waiting on the rate limiter, then on the answer. It is outstanding
        // until then, and withdrawn at its deadline or when whoever issued it
        // drops it, so a request never outlives the search that wanted it.
        let deadline = tokio::time::Instant::now() + RESPONSE_TIMEOUT;
        let (tid, message) = self.create_request(request, addr);
        let (_outstanding, answer) = Outstanding::register(self, (tid, addr));
        let send_and_wait = async {
            until_ready(&self.rate_limiter).await;
            trace!("sending {message:?}");
            self.worker_sender
                .send(WorkerSendRequest {
                    our_tid: Some(tid),
                    message,
                    addr,
                })
                .map_err(|_| Error::DhtDead)?;
            answer.await.map_err(|_| Error::DhtDead)?
        };
        let response = tokio::time::timeout_at(deadline, send_and_wait)
            .await
            .unwrap_or(Err(Error::ResponseTimeout(RESPONSE_TIMEOUT)))?;
        trace!("received {response:?}");
        Ok(response)
    }

    fn create_request(&self, request: Request, addr: SocketAddr) -> (u16, Message<ByteBufOwned>) {
        let transaction_id = self.next_transaction_id.fetch_add(1, Ordering::Relaxed);
        let transaction_id_buf = [(transaction_id >> 8) as u8, (transaction_id & 0xff) as u8];

        let want = if addr.is_ipv6() {
            Some(Want::V6)
        } else {
            Some(Want::V4)
        };

        let message = match request {
            Request::GetPeers(info_hash) => Message {
                transaction_id: ByteBufOwned::from(transaction_id_buf.as_ref()),
                version: None,
                ip: None,
                kind: MessageKind::GetPeersRequest(GetPeersRequest {
                    id: self.id,
                    info_hash,
                    want,
                }),
            },
            Request::FindNode(target) => Message {
                transaction_id: ByteBufOwned::from(transaction_id_buf.as_ref()),
                version: None,
                ip: None,
                kind: MessageKind::FindNodeRequest(FindNodeRequest {
                    id: self.id,
                    target,
                    want,
                }),
            },
            Request::Ping => Message {
                transaction_id: ByteBufOwned::from(transaction_id_buf.as_ref()),
                version: None,
                ip: None,
                kind: MessageKind::PingRequest(PingRequest { id: self.id }),
            },
            Request::Announce {
                info_hash,
                token,
                port,
            } => Message {
                kind: MessageKind::AnnouncePeer(AnnouncePeer {
                    id: self.id,
                    implied_port: 0,
                    info_hash,
                    port,
                    token,
                }),
                transaction_id: ByteBufOwned::from(transaction_id_buf.as_ref()),
                version: None,
                ip: None,
            },
        };
        (transaction_id, message)
    }

    fn generate_compact_nodes_both(
        &self,
        target: Id20,
        want: Want,
    ) -> (
        Option<CompactNodeInfoOwned<SocketAddrV4>>,
        Option<CompactNodeInfoOwned<SocketAddrV6>>,
    ) {
        let now = now();
        match want {
            Want::V4 => (
                Some(self.generate_compact_nodes(target, &self.routing_table_v4.read(), now)),
                None,
            ),
            Want::V6 => (
                None,
                Some(self.generate_compact_nodes(target, &self.routing_table_v6.read(), now)),
            ),
            Want::Both => (
                Some(self.generate_compact_nodes(target, &self.routing_table_v4.read(), now)),
                Some(self.generate_compact_nodes(target, &self.routing_table_v6.read(), now)),
            ),
            Want::None => (None, None),
        }
    }

    fn get_table_for_addr(&self, addr: SocketAddr) -> &RwLock<RoutingTable> {
        if addr.is_ipv4() {
            &self.routing_table_v4
        } else {
            &self.routing_table_v6
        }
    }

    fn generate_compact_nodes<A>(
        &self,
        target: Id20,
        table: &RoutingTable,
        now: Instant,
    ) -> CompactNodeInfo<ByteBufOwned, A>
    where
        A: CompactSerialize + CompactSerializeFixedLen + FromSocketAddr,
        Node<A>: CompactSerialize + CompactSerializeFixedLen,
    {
        let it = table
            .sorted_by_distance_from(target, now)
            .into_iter()
            .filter_map(|r| {
                Some(Node {
                    id: r.id(),
                    addr: A::from_socket_addr(r.addr())?,
                })
            })
            .take(8);
        CompactNodeInfo::new_from_iter(it)
    }

    fn on_received_message(
        self: &Arc<Self>,
        msg: Message<ByteBufOwned>,
        addr: SocketAddr,
    ) -> crate::dht::Result<()> {
        match &msg.kind {
            // If it's a response to a request we made, find the request task, notify it with the response,
            // and let it handle it.
            MessageKind::Error(_) | MessageKind::Response(_) => {
                let tid = msg
                    .get_our_transaction_id()
                    .ok_or(Error::BadTransactionId)?;
                let request = match self
                    .inflight_by_transaction_id
                    .remove(&(tid, addr))
                    .map(|(_, v)| v)
                {
                    Some(req) => req,
                    None => {
                        trace!(?msg, "outstanding request not found");
                        return Err(Error::RequestNotFound);
                    }
                };

                let response_or_error = match msg.kind {
                    MessageKind::Error(e) => ResponseOrError::Error(e),
                    MessageKind::Response(r) => ResponseOrError::Response(r),
                    _ => unreachable!(),
                };
                match request.done.send(Ok(response_or_error)) {
                    Ok(_) => {}
                    Err(e) => {
                        debug!(
                            "received response, but the receiver task is closed: {:?}",
                            e
                        );
                    }
                }
                return Ok(());
            }
            _ => {}
        };

        trace!("received query from {addr}: {msg:?}");

        match &msg.kind {
            // Otherwise, respond to a query.
            MessageKind::PingRequest(req) => {
                let message = Message {
                    transaction_id: msg.transaction_id,
                    version: None,
                    ip: Some(addr),
                    kind: MessageKind::Response(bprotocol::Response {
                        id: self.id,
                        ..Default::default()
                    }),
                };
                self.get_table_for_addr(addr)
                    .write()
                    .mark_last_query(&req.id, now());
                self.worker_sender
                    .send(WorkerSendRequest {
                        our_tid: None,
                        message,
                        addr,
                    })
                    .ok()
                    .ok_or(Error::DhtDead)?;
                Ok(())
            }
            MessageKind::AnnouncePeer(ann) => {
                self.get_table_for_addr(addr)
                    .write()
                    .mark_last_query(&ann.id, now());
                let added = self.peer_store.store_peer(ann, addr);
                trace!("{addr}: added_peer={added}, announce={ann:?}");
                let message = Message {
                    transaction_id: msg.transaction_id,
                    version: None,
                    ip: Some(addr),
                    kind: MessageKind::Response(bprotocol::Response {
                        id: self.id,
                        ..Default::default()
                    }),
                };
                self.worker_sender
                    .send(WorkerSendRequest {
                        our_tid: None,
                        message,
                        addr,
                    })
                    .ok()
                    .ok_or(Error::DhtDead)?;
                Ok(())
            }
            MessageKind::GetPeersRequest(req) => {
                let want = req
                    .want
                    .unwrap_or(if addr.is_ipv6() { Want::V6 } else { Want::V4 });
                let (nodes, nodes6) = self.generate_compact_nodes_both(req.info_hash, want);
                let compact_peer_info = self.peer_store.get_for_info_hash(req.info_hash, want);
                self.get_table_for_addr(addr)
                    .write()
                    .mark_last_query(&req.id, now());
                let message = Message {
                    transaction_id: msg.transaction_id,
                    version: None,
                    ip: Some(addr),
                    kind: MessageKind::Response(bprotocol::Response {
                        id: self.id,
                        nodes,
                        nodes6,
                        values: Some(compact_peer_info),
                        token: Some(ByteBufOwned::from(
                            &self.peer_store.gen_token_for(req.id, addr)[..],
                        )),
                    }),
                };
                self.worker_sender
                    .send(WorkerSendRequest {
                        our_tid: None,
                        message,
                        addr,
                    })
                    .ok()
                    .ok_or(Error::DhtDead)?;
                Ok(())
            }
            MessageKind::FindNodeRequest(req) => {
                let want = req
                    .want
                    .unwrap_or(if addr.is_ipv6() { Want::V6 } else { Want::V4 });
                let (nodes, nodes6) = self.generate_compact_nodes_both(req.target, want);
                self.get_table_for_addr(addr)
                    .write()
                    .mark_last_query(&req.id, now());
                let message = Message {
                    transaction_id: msg.transaction_id,
                    version: None,
                    ip: Some(addr),
                    kind: MessageKind::Response(bprotocol::Response {
                        id: self.id,
                        nodes,
                        nodes6,
                        ..Default::default()
                    }),
                };
                self.worker_sender
                    .send(WorkerSendRequest {
                        our_tid: None,
                        message,
                        addr,
                    })
                    .ok()
                    .ok_or(Error::DhtDead)?;
                Ok(())
            }
            _ => unreachable!(),
        }
    }

    pub fn get_stats(&self) -> DhtStats {
        DhtStats {
            id: self.id,
            outstanding_requests: self.inflight_by_transaction_id.len(),
            routing_table_size: self.routing_table_v4.read().len(),
            routing_table_size_v6: self.routing_table_v6.read().len(),
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
enum Request {
    GetPeers(Id20),
    FindNode(Id20),
    Announce {
        info_hash: Id20,
        token: ByteBufOwned,
        port: u16,
    },
    Ping,
}

enum ResponseOrError {
    Response(Response<ByteBufOwned>),
    Error(ErrorDescription<ByteBufOwned>),
}

impl core::fmt::Debug for ResponseOrError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Response(r) => write!(f, "{r:?}"),
            Self::Error(e) => write!(f, "{e:?}"),
        }
    }
}

/// The socket the DHT talks through.
///
/// The kernel socket is called directly; a custom transport goes through the
/// [`DatagramSocket`] trait object.
enum DhtSocket {
    Udp(UdpSocket),
    Custom(Arc<dyn DatagramSocket>),
}

impl DhtSocket {
    fn bind_addr(&self) -> SocketAddr {
        match self {
            Self::Udp(sock) => sock.bind_addr(),
            Self::Custom(sock) => sock.bind_addr(),
        }
    }

    async fn recv_from(&self, buf: &mut [u8]) -> std::io::Result<(usize, SocketAddr)> {
        match self {
            Self::Udp(sock) => sock.recv_from(buf).await,
            Self::Custom(sock) => sock.recv_from(buf).await,
        }
    }

    async fn send_to(&self, buf: &[u8], target: SocketAddr) -> std::io::Result<usize> {
        match self {
            Self::Udp(sock) => sock.send_to(buf, target).await,
            Self::Custom(sock) => sock.send_to(buf, target).await,
        }
    }
}

struct DhtWorker {
    socket: DhtSocket,
    resolver: Option<Arc<dyn Resolver>>,
    dht: Arc<DhtState>,
}

// Windows fails recv_from on a UDP socket with these for a single datagram, and the socket keeps
// working afterwards:
// - WSAEMSGSIZE: the datagram was longer than the buffer and got truncated.
// - WSAENETRESET: an earlier send_to got an ICMP time exceeded back.
// - WSAECONNRESET: an earlier send_to got an ICMP port unreachable back.
fn is_datagram_error(e: &std::io::Error) -> bool {
    const WSAEMSGSIZE: i32 = 10040;
    const WSAENETRESET: i32 = 10052;
    const WSAECONNRESET: i32 = 10054;
    cfg!(windows)
        && matches!(
            e.raw_os_error(),
            Some(WSAEMSGSIZE | WSAENETRESET | WSAECONNRESET)
        )
}

impl DhtWorker {
    fn on_send_error(&self, tid: u16, addr: SocketAddr, err: crate::dht::Error) {
        if let Some((_, OutstandingRequest { done })) =
            self.dht.inflight_by_transaction_id.remove(&(tid, addr))
        {
            let _ = done.send(Err(err)).is_err();
        };
    }

    /// Resolve a bootstrap `host:port` (or `ip:port`) string.
    async fn lookup_bootstrap(&self, hostname: &str) -> std::io::Result<Vec<SocketAddr>> {
        let Some(resolver) = self.resolver.as_ref() else {
            return Ok(tokio::net::lookup_host(hostname).await?.collect());
        };
        if let Ok(addr) = hostname.parse::<SocketAddr>() {
            return Ok(vec![addr]);
        }
        let (host, port) = hostname
            .rsplit_once(':')
            .and_then(|(host, port)| Some((host, port.parse::<u16>().ok()?)))
            .ok_or_else(|| {
                std::io::Error::new(std::io::ErrorKind::InvalidInput, "invalid socket address")
            })?;
        resolver.resolve(host, port).await
    }

    async fn bootstrap_hostname(&self, hostname: &str) -> crate::dht::Result<()> {
        let addrs = self
            .lookup_bootstrap(hostname)
            .await
            .map_err(|err| Error::lookup(hostname, err))?;
        let v4 = RecursiveRequest::find_node_for_routing_table(
            self.dht.clone(),
            self.dht.id,
            addrs.iter().copied().filter(|a| a.is_ipv4()),
        )
        .instrument(debug_span!("v4"));

        let v6 = RecursiveRequest::find_node_for_routing_table(
            self.dht.clone(),
            self.dht.id,
            addrs.iter().copied().filter(|a| a.is_ipv6()),
        )
        .instrument(debug_span!("v6"));

        let (v4, v6) = tokio::join!(v4, v6);
        v4.or(v6)
    }

    async fn bootstrap_hostname_with_backoff(&self, addr: &str) -> crate::dht::Result<()> {
        let backoff = ExponentialBuilder::new()
            .with_max_delay(Duration::from_secs(60))
            .with_jitter()
            .with_total_delay(Some(Duration::from_secs(86400)))
            .without_max_times();

        (|| self.bootstrap_hostname(addr))
            .retry(backoff)
            .notify(|error, retry_in| {
                warn!(?retry_in, ?addr, "error in bootstrap: {error:#}");
            })
            .instrument(debug_span!("bootstrap", hostname = addr))
            .await
    }

    async fn bootstrap(&self, bootstrap_addrs: &[String]) -> crate::dht::Result<()> {
        let mut futs = FuturesUnordered::new();

        for addr in bootstrap_addrs.iter() {
            futs.push(self.bootstrap_hostname_with_backoff(addr));
        }
        let mut successes = 0;
        while let Some(resp) = futs.next().await {
            if resp.is_ok() {
                successes += 1
            }
        }
        if successes == 0 {
            return Err(Error::BootstrapFailed);
        }
        Ok(())
    }

    async fn bucket_refresher(&self, is_v4: bool) -> crate::dht::Result<()> {
        let (tx, mut rx) = unbounded_channel();

        let table = if is_v4 {
            &self.dht.routing_table_v4
        } else {
            &self.dht.routing_table_v6
        };

        let mut futs = FuturesUnordered::new();
        let filler = async {
            let mut interval = tokio::time::interval(INACTIVITY_TIMEOUT);
            interval.tick().await;
            let mut iteration = 0;
            loop {
                interval.tick().await;
                let now = now();
                let mut found = 0;

                for bucket in table.read().iter_buckets() {
                    if now - bucket.leaf.last_refreshed < INACTIVITY_TIMEOUT {
                        continue;
                    }
                    found += 1;
                    let random_id = bucket.random_within();
                    tx.send(random_id).unwrap();
                }
                trace!("iteration {}, refreshing {} buckets", iteration, found);
                iteration += 1;
            }
        };

        tokio::pin!(filler);

        loop {
            tokio::select! {
                _ = &mut filler => {},
                random_id = rx.recv() => {
                    let random_id = random_id.unwrap();
                    let addrs = table
                        .read()
                        .sorted_by_distance_from(random_id, now())
                        .iter()
                        .map(|n| n.addr())
                        .take(8).collect::<Vec<_>>();
                    futs.push(
                        RecursiveRequest::find_node_for_routing_table(
                            self.dht.clone(), random_id, addrs.into_iter()
                        ).instrument(debug_span!("refresh_bucket"))
                    );
                },
                _ = futs.next(), if !futs.is_empty() => {},
            }
        }
    }

    async fn pinger(&self, is_v4: bool) -> crate::dht::Result<()> {
        let table = if is_v4 {
            &self.dht.routing_table_v4
        } else {
            &self.dht.routing_table_v6
        };
        let mut futs = FuturesUnordered::new();
        let mut interval = tokio::time::interval(INACTIVITY_TIMEOUT / 4);
        let (tx, mut rx) = unbounded_channel();
        let looper = async {
            let mut iteration = 0;
            loop {
                interval.tick().await;
                let mut found = 0;
                let now = now();
                for node in table.read().iter() {
                    if matches!(
                        node.status(now),
                        NodeStatus::Questionable | NodeStatus::Unknown
                    ) {
                        found += 1;
                        tx.send((node.id(), node.addr())).unwrap();
                    }
                }
                trace!("iteration {}, pinging {} nodes", iteration, found);
                iteration += 1;
            }
        };

        tokio::pin!(looper);

        loop {
            tokio::select! {
                _ = &mut looper => {},
                r = rx.recv() => {
                    let (id, addr) = r.unwrap();
                    futs.push(async move {
                        table.write().mark_outgoing_request(&id, now());
                        match self.dht.request(Request::Ping, addr).await {
                            Ok(_) => {
                                table.write().mark_response(&id, now());
                            },
                            Err(e) => {
                                table.write().mark_error(&id);
                                debug!("error: {e:#}");
                            }
                        }
                    }.instrument(debug_span!("ping", addr=addr.to_string())))
                },
                _ = futs.next(), if !futs.is_empty() => {},
            }
        }
    }

    async fn framer(
        &self,
        socket: &DhtSocket,
        mut input_rx: UnboundedReceiver<WorkerSendRequest>,
        output_tx: Sender<(Message<ByteBufOwned>, SocketAddr)>,
    ) -> crate::dht::Result<()> {
        let writer = async {
            let mut buf = Vec::new();
            while let Some(WorkerSendRequest {
                our_tid,
                message,
                addr,
            }) = input_rx.recv().await
            {
                if our_tid.is_none() {
                    trace!("{}: sending {:?}", addr, &message);
                }
                buf.clear();
                bprotocol::serialize_message(
                    &mut buf,
                    message.transaction_id,
                    message.version.or_else(|| self.dht.client_version.clone()),
                    message.ip,
                    message.kind,
                )
                .unwrap();
                if let Err(e) = socket.send_to(&buf, addr).await {
                    debug!("error sending to {addr}: {e:#}");
                    if let Some(tid) = our_tid {
                        self.on_send_error(tid, addr, Error::Send(e));
                    }
                }
            }
            Err(Error::DhtDead)
        };
        let reader = async {
            let mut buf = vec![0u8; 16384];
            loop {
                let (size, addr) = match socket.recv_from(&mut buf).await {
                    Ok(r) => r,
                    Err(e) if is_datagram_error(&e) => {
                        debug!("ignoring UDP receive error: {e:#}");
                        continue;
                    }
                    Err(e) => return Err(Error::Recv(e)),
                };
                match bprotocol::deserialize_message::<ByteBufOwned>(&buf[..size]) {
                    Ok(msg) => match output_tx.send((msg, addr)).await {
                        Ok(_) => {}
                        Err(_) => return Err(Error::DhtDead),
                    },
                    Err(e) => debug!("{}: error deserializing incoming message: {}", addr, e),
                }
            }
        };
        let result = tokio::select! {
            err = writer => err,
            err = reader => err,
        };
        result
    }

    async fn start(
        self,
        in_rx: UnboundedReceiver<WorkerSendRequest>,
        bootstrap_addrs: &[String],
    ) -> crate::dht::Result<()> {
        let (out_tx, mut out_rx) = channel(1);
        let framer = self
            .framer(&self.socket, in_rx, out_tx)
            .instrument(debug_span!("dht_framer"));

        let bootstrap = self.bootstrap(bootstrap_addrs);
        let mut bootstrap_done = false;

        let response_reader = {
            let this = &self;
            async move {
                while let Some((response, addr)) = out_rx.recv().await {
                    if let Err(e) = this.dht.on_received_message(response, addr) {
                        debug!("error in on_response, addr={:?}: {}", addr, e)
                    }
                }
                Err(Error::DhtDead)
            }
        }
        .instrument(debug_span!("dht_response_reader"));

        let pinger_v4 = self.pinger(true).instrument(debug_span!("pinger_v4"));
        let bucket_refresher_v4 = self
            .bucket_refresher(true)
            .instrument(debug_span!("bucket_refresher_v4"));

        let pinger_v6 = self.pinger(false).instrument(debug_span!("pinger_v6"));
        let bucket_refresher_v6 = self
            .bucket_refresher(false)
            .instrument(debug_span!("bucket_refresher_v6"));

        tokio::pin!(framer);
        tokio::pin!(bootstrap);
        tokio::pin!(response_reader);
        tokio::pin!(pinger_v4);
        tokio::pin!(bucket_refresher_v4);
        tokio::pin!(pinger_v6);
        tokio::pin!(bucket_refresher_v6);

        loop {
            tokio::select! {
                err = &mut framer => {
                    return Error::task_finished(&"framer", err);
                },
                result = &mut bootstrap, if !bootstrap_done => {
                    bootstrap_done = true;
                    result?;
                },
                err = &mut pinger_v4 => {
                    return Error::task_finished(&"pinger_v4", err);
                },
                err = &mut bucket_refresher_v4 => {
                    return Error::task_finished(&"bucket_refresher_v4", err);
                },
                err = &mut pinger_v6 => {
                    return Error::task_finished(&"pinger_v6", err);
                },
                err = &mut bucket_refresher_v6 => {
                    return Error::task_finished(&"bucket_refresher_v6", err);
                },
                err = &mut response_reader => {
                    return Error::task_finished(&"response_reader", err);
                }
            }
        }
    }
}

#[derive(Default)]
pub struct DhtConfig<'a> {
    pub peer_id: Option<Id20>,
    pub bootstrap_addrs: Option<Vec<String>>,
    pub routing_table: Option<RoutingTable>,
    pub routing_table_v6: Option<RoutingTable>,
    pub listen_addr: Option<SocketAddr>,
    pub peer_store: Option<PeerStore>,
    pub cancellation_token: Option<CancellationToken>,
    pub bind_device: Option<&'a BindDevice>,
    /// Send and receive DHT traffic through this socket instead of binding a
    /// kernel UDP socket. When set, `listen_addr` and `bind_device` are
    /// ignored and the DHT reports the socket's `bind_addr()` as its listen
    /// address.
    pub socket: Option<Arc<dyn DatagramSocket>>,
    /// Look up bootstrap hostnames through this resolver instead of the host's
    /// resolver.
    pub resolver: Option<Arc<dyn Resolver>>,
    /// The leading bytes of our node id, the rest being random (BEP 5 node ids
    /// are otherwise random). A client puts its BEP 20 peer id fingerprint here
    /// so that one identity covers the peer wire, trackers and the DHT. An
    /// explicit or restored `peer_id` is kept only if it starts with these bytes;
    /// otherwise a new id is made, and the routing tables and peer store, which
    /// are built around the old one, are dropped. At most 20 bytes are used.
    /// Defaults to rqbit's own fingerprint.
    pub node_id_prefix: Option<Vec<u8>>,
    /// The BEP 5 client version ("v"): by convention the two BEP 20 client
    /// letters followed by two version bytes. Sent with every outgoing message
    /// when set; when not, no "v" is sent.
    pub client_version: Option<Vec<u8>>,
    /// Track the DHT's worker in this tracker. The worker owns the socket, so once the
    /// tracker is empty after cancellation, the socket is gone.
    pub tasks: Option<TaskTracker>,
}

// A node id starting with `prefix` (at most 20 bytes of it), the rest random.
fn generate_node_id(prefix: &[u8]) -> Id20 {
    use rand::Rng as _;
    let mut id = [0u8; 20];
    let len = prefix.len().min(20);
    id[..len].copy_from_slice(&prefix[..len]);
    rand::rng().fill_bytes(&mut id[len..]);
    Id20::new(id)
}

impl DhtState {
    pub async fn new() -> crate::dht::Result<Arc<Self>> {
        Self::with_config(DhtConfig::default()).await
    }
    pub fn cancellation_token(&self) -> &CancellationToken {
        &self.cancellation_token
    }

    #[inline(never)]
    pub fn with_config<'a>(
        mut config: DhtConfig<'a>,
    ) -> BoxFuture<'a, crate::dht::Result<Arc<Self>>> {
        async move {
            let socket = match config.socket.take() {
                Some(socket) => DhtSocket::Custom(socket),
                None => {
                    let addr = config
                        .listen_addr
                        .unwrap_or((Ipv6Addr::UNSPECIFIED, 0).into());
                    DhtSocket::Udp(
                        UdpSocket::bind_udp(
                            addr,
                            librqbit_dualstack_sockets::BindOpts {
                                request_dualstack: true,
                                reuseport: false,
                                device: config.bind_device,
                            },
                        )
                        .map_err(|e| Error::Bind(Box::new(e)))?,
                    )
                }
            };

            let listen_addr = socket.bind_addr();
            info!("DHT listening on {:?}", listen_addr);

            let peer_id = match config.node_id_prefix.as_deref() {
                None => config
                    .peer_id
                    .unwrap_or_else(|| generate_azereus_style(*b"WV", crate_version!())),
                Some(prefix) => {
                    let prefix = &prefix[..prefix.len().min(20)];
                    config
                        .peer_id
                        .filter(|id| id.0.starts_with(prefix))
                        .unwrap_or_else(|| generate_node_id(prefix))
                }
            };
            info!("starting up DHT with peer id {:?}", peer_id);
            // A routing table is laid out by distance from its own node's id, and the
            // peer store hands out tokens for it: neither carries over to another id.
            let routing_table = config.routing_table.filter(|t| t.id() == peer_id);
            let routing_table_v6 = config.routing_table_v6.filter(|t| t.id() == peer_id);
            let peer_store = config.peer_store.filter(|s| s.self_id == peer_id);
            let bootstrap_addrs = config.bootstrap_addrs.unwrap_or_else(|| {
                crate::dht::DHT_BOOTSTRAP
                    .iter()
                    .map(|v| v.to_string())
                    .collect()
            });

            let token = config.cancellation_token.take().unwrap_or_default();

            let (in_tx, in_rx) = unbounded_channel();
            let state = Arc::new(Self::new_internal(
                peer_id,
                in_tx,
                routing_table,
                routing_table_v6,
                listen_addr,
                peer_store.unwrap_or_else(|| PeerStore::new(peer_id)),
                token,
                config.client_version.map(ByteBufOwned::from),
            ));

            spawn_with_cancel(
                debug_span!("dht"),
                "dht",
                state.cancellation_token.clone(),
                {
                    let state = state.clone();
                    let resolver = config.resolver.take();
                    let worker = async move {
                        let worker = DhtWorker {
                            socket,
                            resolver,
                            dht: state,
                        };
                        worker.start(in_rx, &bootstrap_addrs).await
                    };
                    config.tasks.unwrap_or_default().track_future(worker)
                },
            );
            Ok(state)
        }
        .boxed()
    }

    pub fn get_peers(
        self: &Arc<Self>,
        info_hash: Id20,
        announce_port: Option<u16>,
    ) -> RequestPeersStream {
        RequestPeersStream::new(self.clone(), info_hash, announce_port)
    }

    pub fn listen_addr(&self) -> SocketAddr {
        self.listen_addr
    }

    pub fn stats(&self) -> DhtStats {
        self.get_stats()
    }

    pub fn with_routing_tables<R, F: FnOnce(&RoutingTable, &RoutingTable) -> R>(&self, f: F) -> R {
        f(&self.routing_table_v4.read(), &self.routing_table_v6.read())
    }

    // pub fn clone_routing_table(&self) -> RoutingTable {
    //     self.routing_table.read().clone()
    // }
}

trait FromSocketAddr: Sized {
    fn from_socket_addr(addr: SocketAddr) -> Option<Self>;
}

impl FromSocketAddr for SocketAddrV4 {
    fn from_socket_addr(addr: SocketAddr) -> Option<Self> {
        match addr {
            SocketAddr::V4(a) => Some(a),
            _ => None,
        }
    }
}

impl FromSocketAddr for SocketAddrV6 {
    fn from_socket_addr(addr: SocketAddr) -> Option<Self> {
        match addr {
            SocketAddr::V6(a) => Some(a),
            _ => None,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{
        net::{Ipv4Addr, SocketAddr},
        time::Duration,
    };

    use crate::bencode::ByteBufOwned;
    use tokio::net::UdpSocket;

    use super::{DhtConfig, DhtState};
    use crate::dht::{
        Dht,
        bprotocol::{self, MessageKind},
    };

    async fn start_dht(bootstrap_addr: SocketAddr) -> Dht {
        DhtState::with_config(DhtConfig {
            listen_addr: Some((Ipv4Addr::LOCALHOST, 0).into()),
            bootstrap_addrs: Some(vec![bootstrap_addr.to_string()]),
            ..Default::default()
        })
        .await
        .unwrap()
    }

    async fn assert_answers_ping(dht: &Dht) {
        let client = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
        client
            .send_to(
                b"d1:ad2:id20:abcdefghij0123456789e1:q4:ping1:t2:aa1:y1:qe",
                dht.listen_addr(),
            )
            .await
            .unwrap();
        let mut buf = [0u8; 1024];
        let len = tokio::time::timeout(Duration::from_secs(5), async {
            loop {
                match client.recv_from(&mut buf).await {
                    Ok((len, addr)) if addr == dht.listen_addr() => return len,
                    _ => continue,
                }
            }
        })
        .await
        .expect("the DHT stopped answering");
        let response = bprotocol::deserialize_message::<ByteBufOwned>(&buf[..len]).unwrap();
        assert!(matches!(response.kind, MessageKind::Response(_)));
    }

    // On Windows, an ICMP port unreachable that comes back for an earlier send_to makes the next
    // recv_from on the same UDP socket fail with WSAECONNRESET (os error 10054).
    #[tokio::test]
    async fn test_dht_answers_after_icmp_port_unreachable() {
        // Nothing listens on this port once the socket is dropped.
        let closed = std::net::UdpSocket::bind((Ipv4Addr::LOCALHOST, 0))
            .unwrap()
            .local_addr()
            .unwrap();
        let dht = start_dht(closed).await;

        // Let the bootstrap query reach the closed port and the ICMP error come back.
        tokio::time::sleep(Duration::from_millis(500)).await;
        assert_answers_ping(&dht).await;
    }

    // On Windows, a datagram longer than the receive buffer makes recv_from fail with
    // WSAEMSGSIZE (os error 10040).
    #[tokio::test]
    async fn test_dht_answers_after_oversized_datagram() {
        // Stays open, so the bootstrap query gets neither an answer nor an ICMP error.
        let silent = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
        let dht = start_dht(silent.local_addr().unwrap()).await;

        // Some platforms refuse to send a datagram this large, which is fine.
        let _ = silent.send_to(&[0u8; 20_000], dht.listen_addr()).await;
        assert_answers_ping(&dht).await;
    }

    #[tokio::test]
    async fn test_dht_answers_ping_through_custom_socket() {
        use crate::core::net::DatagramSocket;
        use std::sync::Arc;

        // Stays open, so the bootstrap query gets neither an answer nor an ICMP error.
        let silent = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
        let custom = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
        let custom_addr = custom.local_addr().unwrap();
        let custom: Arc<dyn DatagramSocket> = Arc::new(custom);

        let dht = DhtState::with_config(DhtConfig {
            bootstrap_addrs: Some(vec![silent.local_addr().unwrap().to_string()]),
            socket: Some(custom),
            ..Default::default()
        })
        .await
        .unwrap();
        assert_eq!(dht.listen_addr(), custom_addr);

        let client = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
        client
            .send_to(
                b"d1:ad2:id20:abcdefghij0123456789e1:q4:ping1:t2:aa1:y1:qe",
                custom_addr,
            )
            .await
            .unwrap();
        let mut buf = [0u8; 1024];
        let len = loop {
            let (len, addr) = client.recv_from(&mut buf).await.unwrap();
            if addr == custom_addr {
                break len;
            }
        };
        let response = bprotocol::deserialize_message::<ByteBufOwned>(&buf[..len]).unwrap();
        assert!(matches!(response.kind, MessageKind::Response(_)));
        assert_eq!(response.transaction_id.as_ref(), b"aa");
        dht.cancellation_token().cancel();
    }

    #[tokio::test]
    async fn test_dht_bootstrap_lookup_through_custom_resolver() {
        use crate::core::net::{BoxFuture, Resolver};
        use std::sync::{
            Arc,
            atomic::{AtomicUsize, Ordering},
        };

        struct CountingResolver {
            target: SocketAddr,
            calls: AtomicUsize,
        }

        impl Resolver for CountingResolver {
            fn resolve<'a>(
                &'a self,
                host: &'a str,
                port: u16,
            ) -> BoxFuture<'a, std::io::Result<Vec<SocketAddr>>> {
                Box::pin(async move {
                    assert_eq!(host, "bootstrap.invalid");
                    assert_eq!(port, self.target.port());
                    self.calls.fetch_add(1, Ordering::SeqCst);
                    Ok(vec![self.target])
                })
            }
        }

        let bootstrap = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
        let bootstrap_addr = bootstrap.local_addr().unwrap();
        let resolver = Arc::new(CountingResolver {
            target: bootstrap_addr,
            calls: AtomicUsize::new(0),
        });

        let dht = DhtState::with_config(DhtConfig {
            listen_addr: Some((Ipv4Addr::LOCALHOST, 0).into()),
            bootstrap_addrs: Some(vec![format!("bootstrap.invalid:{}", bootstrap_addr.port())]),
            resolver: Some(resolver.clone()),
            ..Default::default()
        })
        .await
        .unwrap();

        // The only way the DHT learns this address is through the resolver.
        let mut buf = [0u8; 1024];
        let len = loop {
            let (len, addr) = bootstrap.recv_from(&mut buf).await.unwrap();
            if addr == dht.listen_addr() {
                break len;
            }
        };
        let request = bprotocol::deserialize_message::<ByteBufOwned>(&buf[..len]).unwrap();
        assert!(matches!(request.kind, MessageKind::FindNodeRequest(_)));
        assert!(resolver.calls.load(Ordering::SeqCst) >= 1);
        dht.cancellation_token().cancel();
    }

    // Start a DHT on loopback whose one bootstrap node is `bootstrap`, a socket the
    // test holds: whatever the DHT sends lands there and nowhere else.
    async fn start_dht_with(bootstrap: &UdpSocket, config: DhtConfig<'static>) -> Dht {
        DhtState::with_config(DhtConfig {
            listen_addr: Some((Ipv4Addr::LOCALHOST, 0).into()),
            bootstrap_addrs: Some(vec![bootstrap.local_addr().unwrap().to_string()]),
            ..config
        })
        .await
        .unwrap()
    }

    async fn recv_message_from(
        sock: &UdpSocket,
        from: SocketAddr,
    ) -> bprotocol::Message<ByteBufOwned> {
        let mut buf = [0u8; 2048];
        loop {
            let (len, addr) = sock.recv_from(&mut buf).await.unwrap();
            if addr == from {
                return bprotocol::deserialize_message::<ByteBufOwned>(&buf[..len]).unwrap();
            }
        }
    }

    // The "v" on the DHT's own bootstrap query and on its answer to our ping.
    async fn versions_sent(client_version: Option<Vec<u8>>) -> [Option<Vec<u8>>; 2] {
        let bootstrap = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();
        let dht = start_dht_with(
            &bootstrap,
            DhtConfig {
                client_version,
                ..Default::default()
            },
        )
        .await;
        let query = recv_message_from(&bootstrap, dht.listen_addr()).await;

        bootstrap
            .send_to(
                b"d1:ad2:id20:abcdefghij0123456789e1:q4:ping1:t2:aa1:y1:qe",
                dht.listen_addr(),
            )
            .await
            .unwrap();
        let answer = loop {
            let m = recv_message_from(&bootstrap, dht.listen_addr()).await;
            if m.transaction_id.as_ref() == b"aa" {
                break m;
            }
        };
        assert!(matches!(answer.kind, MessageKind::Response(_)));
        dht.cancellation_token().cancel();
        [query, answer].map(|m| m.version.map(|v| v.as_ref().to_vec()))
    }

    #[tokio::test]
    async fn test_client_version_is_sent_only_when_configured() {
        assert_eq!(versions_sent(None).await, [None, None]);
        let v = b"XY\x01\x02".to_vec();
        assert_eq!(
            versions_sent(Some(v.clone())).await,
            [Some(v.clone()), Some(v)]
        );
    }

    #[tokio::test]
    async fn test_node_id_prefix() {
        use crate::core::hash_id::Id20;
        use crate::dht::routing_table::RoutingTable;

        let bootstrap = UdpSocket::bind((Ipv4Addr::LOCALHOST, 0)).await.unwrap();

        // By default: rqbit's own fingerprint.
        let dht = start_dht_with(&bootstrap, DhtConfig::default()).await;
        assert!(dht.stats().id.0.starts_with(b"-WV"));
        dht.cancellation_token().cancel();

        // A prefix replaces it.
        let prefix = b"-XY1234-".to_vec();
        let dht = start_dht_with(
            &bootstrap,
            DhtConfig {
                node_id_prefix: Some(prefix.clone()),
                ..Default::default()
            },
        )
        .await;
        assert!(dht.stats().id.0.starts_with(&prefix));
        dht.cancellation_token().cancel();

        // A restored id that already carries the prefix is kept, with its table.
        let mut kept = [7u8; 20];
        kept[..8].copy_from_slice(&prefix);
        let kept = Id20::new(kept);
        let dht = start_dht_with(
            &bootstrap,
            DhtConfig {
                peer_id: Some(kept),
                routing_table: Some(RoutingTable::new(kept, None)),
                node_id_prefix: Some(prefix.clone()),
                ..Default::default()
            },
        )
        .await;
        assert_eq!(dht.stats().id, kept);
        dht.cancellation_token().cancel();

        // One made under another identity is replaced, and so is the table built
        // around it.
        let other = Id20::new([9u8; 20]);
        let dht = start_dht_with(
            &bootstrap,
            DhtConfig {
                peer_id: Some(other),
                routing_table: Some(RoutingTable::new(other, None)),
                node_id_prefix: Some(prefix.clone()),
                ..Default::default()
            },
        )
        .await;
        let id = dht.stats().id;
        assert!(id.0.starts_with(&prefix));
        dht.with_routing_tables(|v4, v6| {
            assert_eq!(v4.id(), id);
            assert_eq!(v6.id(), id);
        });
        dht.cancellation_token().cancel();

        // An empty prefix: an entirely random id, not rqbit's fingerprint.
        let ids: Vec<Id20> = futures::future::join_all((0..4).map(|_| async {
            let dht = start_dht_with(
                &bootstrap,
                DhtConfig {
                    node_id_prefix: Some(Vec::new()),
                    ..Default::default()
                },
            )
            .await;
            let id = dht.stats().id;
            dht.cancellation_token().cancel();
            id
        }))
        .await;
        assert!(ids.iter().any(|id| !id.0.starts_with(b"-WV")));
    }

    // A DHT whose outgoing datagrams land in the returned receiver instead of on
    // a socket: the test sees every request sent, and nothing is ever answered
    // unless the test answers it.
    fn fixture_dht() -> (
        std::sync::Arc<DhtState>,
        tokio::sync::mpsc::UnboundedReceiver<super::WorkerSendRequest>,
    ) {
        let (dht, rx) = fixture_state();
        (std::sync::Arc::new(dht), rx)
    }

    fn fixture_state() -> (
        DhtState,
        tokio::sync::mpsc::UnboundedReceiver<super::WorkerSendRequest>,
    ) {
        use crate::dht::peer_store::PeerStore;
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        let id = crate::core::hash_id::Id20::new([0x11; 20]);
        let dht = DhtState::new_internal(
            id,
            tx,
            None,
            None,
            (Ipv4Addr::LOCALHOST, 0).into(),
            PeerStore::new(id),
            tokio_util::sync::CancellationToken::new(),
            None,
        );
        (dht, rx)
    }

    fn get_peers_search(
        dht: &std::sync::Arc<DhtState>,
    ) -> super::RecursiveRequest<super::RecursiveRequestCallbacksGetPeers> {
        super::RecursiveRequest::get_peers(
            dht.clone(),
            crate::core::hash_id::Id20::new([0x22; 20]),
            None,
            tokio::sync::mpsc::unbounded_channel().0,
        )
    }

    // `n` nodes, each with its own id and address, none of which ever answers.
    fn silent_nodes(n: u8) -> Vec<(Option<crate::core::hash_id::Id20>, SocketAddr)> {
        (0..n)
            .map(|i| {
                (
                    Some(crate::core::hash_id::Id20::new([i; 20])),
                    SocketAddr::from(([10, 0, 0, i], 6881)),
                )
            })
            .collect()
    }

    #[tokio::test(start_paused = true)]
    async fn test_search_keeps_at_most_branching_requests_in_flight_and_ends() {
        use super::{SEARCH_BRANCHING, SearchOutcome};
        use crate::dht::RESPONSE_TIMEOUT;
        use std::{collections::HashSet, task::Poll};

        let (dht, mut sent) = fixture_dht();
        let nodes = silent_nodes(20);
        let req = get_peers_search(&dht);
        let mut search = std::pin::pin!(req.search(nodes.clone().into_iter()));

        let mut asked = HashSet::new();
        let mut most_in_flight = 0;
        let outcome = loop {
            let polled = futures::poll!(search.as_mut());
            while let Ok(request) = sent.try_recv() {
                assert!(asked.insert(request.addr), "{} asked twice", request.addr);
            }
            let in_flight = dht.get_stats().outstanding_requests;
            assert!(in_flight <= SEARCH_BRANCHING, "{in_flight} in flight");
            most_in_flight = most_in_flight.max(in_flight);
            if let Poll::Ready(outcome) = polled {
                break outcome.unwrap();
            }
            // Nobody answers: the requests in flight run out their time.
            tokio::time::advance(RESPONSE_TIMEOUT).await;
        };

        assert_eq!(most_in_flight, SEARCH_BRANCHING);
        assert_eq!(asked, nodes.iter().map(|(_, addr)| *addr).collect());
        assert_eq!(
            outcome,
            SearchOutcome {
                answered: 0,
                failed: 20
            }
        );
        assert_eq!(dht.get_stats().outstanding_requests, 0);
    }

    #[tokio::test(start_paused = true)]
    async fn test_cancelling_a_search_withdraws_its_requests_waiting_on_the_limiter() {
        use super::SEARCH_BRANCHING;
        use crate::dht::RESPONSE_TIMEOUT;
        use std::task::Poll;

        let (dht, mut sent) = fixture_dht();
        // Nothing more goes out until the limiter refills, and the clock stands
        // still until the test moves it.
        while dht.rate_limiter.check().is_ok() {}
        let req = get_peers_search(&dht);
        let mut search = Box::pin(req.search(silent_nodes(8).into_iter()));

        assert!(matches!(futures::poll!(search.as_mut()), Poll::Pending));
        assert_eq!(dht.get_stats().outstanding_requests, SEARCH_BRANCHING);
        assert!(sent.try_recv().is_err(), "sent past an empty limiter");

        drop(search);
        assert_eq!(dht.get_stats().outstanding_requests, 0);
        // The limiter refills; nobody is left waiting to send.
        tokio::time::advance(RESPONSE_TIMEOUT).await;
        assert!(
            sent.try_recv().is_err(),
            "a cancelled search sent a request"
        );
    }

    #[tokio::test(start_paused = true)]
    async fn test_a_request_the_limiter_holds_past_its_deadline_is_dropped_with_its_search() {
        use super::{RateLimiter, SEARCH_BRANCHING, SearchOutcome, TokioClock};
        use crate::dht::RESPONSE_TIMEOUT;
        use std::{num::NonZeroU32, task::Poll};

        let (mut state, mut sent) = fixture_state();
        // One request a minute, already spent: every request of the search would
        // have to wait longer than it lives.
        state.rate_limiter = RateLimiter::direct_with_clock(
            governor::Quota::per_minute(NonZeroU32::new(1).unwrap()),
            TokioClock {
                start: tokio::time::Instant::now(),
            },
        );
        assert!(state.rate_limiter.check().is_ok());
        let dht = std::sync::Arc::new(state);
        let req = get_peers_search(&dht);
        let mut search = std::pin::pin!(req.search(silent_nodes(8).into_iter()));

        let mut rounds = 0;
        let outcome = loop {
            let polled = futures::poll!(search.as_mut());
            assert!(sent.try_recv().is_err(), "sent past an empty limiter");
            assert!(dht.get_stats().outstanding_requests <= SEARCH_BRANCHING);
            if let Poll::Ready(outcome) = polled {
                break outcome.unwrap();
            }
            rounds += 1;
            tokio::time::advance(RESPONSE_TIMEOUT).await;
        };
        // 5 then 3 requests, each dropped at its deadline.
        assert_eq!(rounds, 2);
        assert_eq!(
            outcome,
            SearchOutcome {
                answered: 0,
                failed: 8
            }
        );
        assert_eq!(dht.get_stats().outstanding_requests, 0);
    }
}
