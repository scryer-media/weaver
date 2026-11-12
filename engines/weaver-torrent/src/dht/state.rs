// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver
//
// Derived from rqbit (https://github.com/ikatson/rqbit), Copyright 2021 Igor Katson,
// licensed under the Apache License 2.0 (see LICENSE-APACHE). Modified by the
// Weaver authors from 2026-10-20 onward.

use std::{
    collections::BTreeSet,
    net::{IpAddr, Ipv6Addr, SocketAddr, SocketAddrV4, SocketAddrV6},
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU16, AtomicU64, Ordering},
    },
    task::Poll,
    time::{Duration, Instant},
};

use crate::bencode::ByteBufOwned;
use crate::dht::{
    DHT_ANNOUNCE_INTERVAL, Error, INACTIVITY_TIMEOUT, RESPONSE_TIMEOUT,
    bprotocol::{
        self, AnnouncePeer, CompactNodeInfo, CompactNodeInfoOwned, ErrorDescription,
        FindNodeRequest, GetPeersRequest, Message, MessageKind, Node, PingRequest, Response, Want,
    },
    peer_store::{PEER_PURGE_INTERVAL, PeerStore},
    query_limiter::QueryLimiter,
    routing_table::{InsertResult, NodeStatus, RoutingTable},
    send_quota::{DHT_UPLOAD_RATE_LIMIT, SendQuota},
};
use backon::{ExponentialBuilder, Retryable};
use dashmap::DashMap;
use futures::{FutureExt, Stream, StreamExt, future::BoxFuture, stream::FuturesUnordered};

use crate::core::{
    compact_ip::{CompactSerialize, CompactSerializeFixedLen},
    hash_id::Id20,
    net::{DatagramSocket, NoNames, Resolved, Resolver},
    peer_id::try_decode_peer_id,
    spawn_utils::{spawn, spawn_with_cancel},
};
use crate::network::SocketFactory;
use governor::{
    Quota, clock::Clock, middleware::NoOpMiddleware, nanos::Nanos, state::InMemoryState,
    state::NotKeyed,
};
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
// A search that starts from fewer nodes than this also asks the routers
// (libtorrent `traversal_algorithm::start`).
const SEARCH_MIN_SEEDS: usize = 3;
// A table this many buckets deep no longer bootstraps again, and how often
// it searches for our own id once it is (libtorrent `node::tick`,
// node.cpp:654, v2.0.15).
const SHALLOW_DEPTH: usize = 4;
const SELF_REFRESH_INTERVAL: Duration = Duration::from_secs(10 * 60);
// The first wait before a shallow table bootstraps again: libtorrent's
// `dht_tracker` ticks its node every 5 seconds (dht_tracker.cpp:268-283).
const BOOTSTRAP_RETRY_MIN: Duration = Duration::from_secs(5);
// How long a torrent's search that fell short waits before it may search
// again ahead of its turn; each early search doubles it, and its turn sets
// it back. Between two turns, DHT_ANNOUNCE_INTERVAL apart, that is at most
// four early searches (30 + 60 + 120 + 240 s; the fifth would wait 480 s
// more).
const EARLY_SEARCH_MIN: Duration = Duration::from_secs(30);
// The early searches the torrents of a family may start for each time its
// table grows: a growth wakes the torrents waiting on it, and this many
// search; the rest wait for the next growth.
const EARLY_SEARCHES_PER_GROWTH: usize = WALKS_AT_ONCE;
// The least time between two growths that nodes entering the table make:
// the first node to enter makes one at once, and the nodes that enter in
// the next GROWTH_SPACING make one between them as it ends. libtorrent
// looks at its table on a 5 s tick (dht_tracker.cpp:268-283, v2.0.15), as
// BOOTSTRAP_RETRY_MIN does here.
const GROWTH_SPACING: Duration = BOOTSTRAP_RETRY_MIN;

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
    // The write token it answered with, if any.
    token: Option<ByteBufOwned>,
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
                token: None,
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

    fn give_token(&mut self, addr: SocketAddr, token: Option<ByteBufOwned>) {
        if let Some(c) = self.list.iter_mut().find(|c| c.addr == addr) {
            c.token = token;
        }
    }

    // The k closest nodes that answered with a write token: the ones a search
    // for peers announces to when it ends (libtorrent `find_data::done`, which
    // walks its results closest first and keeps up to a bucket of the live ones
    // holding a token).
    fn token_holders(&self) -> impl Iterator<Item = (SocketAddr, &ByteBufOwned)> {
        self.list
            .iter()
            .filter(|c| c.state == CandidateState::Answered)
            .filter_map(|c| Some((c.addr, c.token.as_ref()?)))
            .take(SEARCH_K)
    }
}

// Whether a search should not ask `node`, named by the node at `source`: an
// address nothing can be sent to (port 0, unspecified, multicast, broadcast,
// the reserved 0/8 and 240/4), or a local one named by a remote node, which
// can only point us inside our own network (libtorrent `aux::is_global` for
// the first, and the rule `ut_pex.cpp` applies to PEX peers, a local address
// only from a local source, for the second).
fn is_martian(node: SocketAddr, source: SocketAddr) -> bool {
    use crate::torrent_state::live::peers::holepunch::is_local;
    let unusable = node.port() == 0
        || match node.ip().to_canonical() {
            IpAddr::V4(a) => {
                a.is_unspecified()
                    || a.is_multicast()
                    || a.is_broadcast()
                    || a.octets()[0] == 0
                    || a.octets()[0] >= 240
            }
            IpAddr::V6(a) => a.is_unspecified() || a.is_multicast(),
        };
    unusable || (is_local(node.ip()) && !is_local(source.ip()))
}

#[derive(Debug, Default, PartialEq, Eq)]
struct SearchOutcome {
    answered: usize,
    failed: usize,
    // Of `answered`, the nodes, not counting the routers: a router's answer
    // names nodes, it is no node near the target.
    nodes: usize,
}

impl SearchOutcome {
    // Fewer than k nodes answered: the search did not reach the target.
    fn is_short(&self) -> bool {
        self.nodes < SEARCH_K
    }
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
    fn on_search_end(&self, _req: &RecursiveRequest<Self>, _candidates: &Candidates) {}
}

struct RecursiveRequestCallbacksGetPeers {
    announce_port: Option<u16>,
}

impl RecursiveRequestCallbacks for RecursiveRequestCallbacksGetPeers {
    fn on_request_start(&self, _: &RecursiveRequest<Self>, _: Id20, _: SocketAddr) {}

    fn on_request_end(
        &self,
        _: &RecursiveRequest<Self>,
        _: Id20,
        _: SocketAddr,
        _: &crate::dht::Result<ResponseOrError>,
    ) {
    }

    // A search that ends announces us to the k closest nodes that gave it a
    // token (libtorrent `node::announce`, whose get_peers traversal ends in
    // `announce_fun`).
    fn on_search_end(&self, req: &RecursiveRequest<Self>, candidates: &Candidates) {
        let Some(port) = self.announce_port else {
            return;
        };
        for (addr, token) in candidates.token_holders() {
            let (tid, message) = req.dht.create_request(
                Request::Announce {
                    info_hash: req.info_hash,
                    token: token.clone(),
                    port,
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
}

struct RecursiveRequestCallbacksFindNodes {}
impl RecursiveRequestCallbacks for RecursiveRequestCallbacksFindNodes {
    // A node a search is told of enters the table only once it answers
    // (libtorrent routing_table.cpp:816-822, 1085-1099, v2.0.15: a node not
    // yet pinged never takes a place in a bucket). One the table holds
    // already is marked as asked. An error reply is no answer: it counts as
    // a failure, as a timeout does, and enters nothing (rpc_manager.cpp:
    // 316-350, where an error calls the observer's `timeout`, and only a
    // response reaches `node_seen` at 389).
    fn on_request_start(&self, req: &RecursiveRequest<Self>, target_node: Id20, addr: SocketAddr) {
        req.dht
            .get_table_for_addr(addr)
            .write()
            .mark_outgoing_request(&target_node, now());
    }

    fn on_request_end(
        &self,
        req: &RecursiveRequest<Self>,
        target_node: Id20,
        addr: SocketAddr,
        resp: &crate::dht::Result<ResponseOrError>,
    ) {
        let mut table = req.dht.get_table_for_addr(addr).write();
        if let Ok(ResponseOrError::Response(response)) = resp {
            // The node enters under the id it answered with.
            table.add_node(response.id, addr);
            table.mark_response(&response.id, now());
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
    /// Searches over both families, each announcing the port given for
    /// it, as each of libtorrent's DHT nodes announces the port of its own
    /// listen socket (kademlia/node.cpp:449-453, `get_listen_port`).
    fn new(
        dht: Arc<DhtState>,
        info_hash: Id20,
        port_v4: Option<u16>,
        port_v6: Option<u16>,
    ) -> Self {
        let (peer_tx, peer_rx) = unbounded_channel();
        let make = |is_v4: bool, dht: Arc<DhtState>, peer_tx: UnboundedSender<SocketAddr>| {
            Arc::new(RecursiveRequest::get_peers(
                dht,
                info_hash,
                if is_v4 { port_v4 } else { port_v6 },
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
    // A find_node search of the `is_v4` family for `target`, from `addrs`, or
    // with none from the table (see `start`): a refresh of our own id or of a
    // bucket.
    async fn find_node_for_routing_table(
        dht: Arc<DhtState>,
        target: Id20,
        is_v4: bool,
        addrs: impl Iterator<Item = SocketAddr>,
    ) -> crate::dht::Result<SearchOutcome> {
        let req = RecursiveRequest {
            info_hash: target,
            request: Request::FindNode(target),
            dht,
            peer_tx: unbounded_channel().0,
            callbacks: RecursiveRequestCallbacksFindNodes {},
        };
        let outcome = req
            .search(is_v4, addrs.map(|addr| (None, addr)))
            .instrument(debug_span!("find_node", target = format!("{target:?}")))
            .await?;
        debug!(
            "finished, answered = {}, failed = {}",
            outcome.answered, outcome.failed
        );
        Ok(outcome)
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
            callbacks: RecursiveRequestCallbacksGetPeers { announce_port },
        }
    }

    // One search at a time, from the table (see `start`): the
    // first as soon as the torrent asks for peers and one of the DHT's
    // WALKS_AT_ONCE turns is free, that is when the stream starts, then one at the torrent's own point in the interval (see
    // `announce_slot`) and one every DHT_ANNOUNCE_INTERVAL after. Each announces us when it
    // ends (libtorrent `torrent::start_announcing` asks
    // `session_impl::prioritize_dht` for a search at once, and
    // `session_impl::on_dht_announce` gives every torrent its
    // `torrent::dht_announce` once a `dht_announce_interval`). A search due
    // while the table is empty and no router is known starts as the first
    // node enters the table or the first router becomes known, whichever is
    // first. A search that fell short, fewer than k nodes answering it, as
    // one made before the route carried datagrams or before the table
    // filled, searches again ahead of its turn once the table has grown
    // since it started (see `DhtState::grow`) and EARLY_SEARCH_MIN has
    // passed, doubling with each early search, and only with one of the
    // EARLY_SEARCHES_PER_GROWTH early turns that growth gave out. It goes
    // on so until a search is not short or its turn comes; the turn after
    // stays where it was. libtorrent searches again only at the torrent's
    // next turn (`torrent::on_dht_announce_response` drops an empty
    // answer, torrent.cpp:2931-2944, v2.0.15), where its DHT started on a
    // route that already carried datagrams.
    fn request_peers_forever(self: Arc<Self>, is_v4: bool) -> tokio::task::JoinHandle<()> {
        spawn::<Error>(
            debug_span!(parent: None, "get_peers", is_v4, info_hash = format!("{:?}", self.info_hash)),
            "get_peers",
            async move {
                let entered = self.table(is_v4).read().entered();
                let growth = self.dht.growth(is_v4);
                let mut wait = announce_slot(&self.info_hash);
                // When the next search is due: set as a search ends, kept
                // when a short one is made again early.
                let mut due = None;
                let mut early = EARLY_SEARCH_MIN;
                loop {
                    let mut next_entered = std::pin::pin!(entered.notified());
                    next_entered.as_mut().enable();
                    let mut next_router = std::pin::pin!(self.dht.routers_known.notified());
                    next_router.as_mut().enable();
                    let turn = self
                        .dht
                        .walks
                        .acquire()
                        .await
                        .expect("the DHT's walk turns are never closed");
                    let candidates = self.start(is_v4, std::iter::empty());
                    if candidates.list.is_empty() {
                        drop(turn);
                        tokio::select! {
                            () = next_entered => {}
                            () = next_router => {}
                        }
                        continue;
                    }
                    let seen = growth.generation.load(Ordering::Acquire);
                    let outcome = self.walk(candidates).await?;
                    drop(turn);
                    trace!(?outcome, "search finished");
                    let now = tokio::time::Instant::now();
                    let due_at = *due.get_or_insert_with(|| now + wait);
                    let early_at = now + early;
                    if outcome.is_short() && early_at < due_at {
                        early *= 2;
                        tokio::select! {
                            () = tokio::time::sleep_until(due_at) => {}
                            () = growth.early_turn(seen, early_at) => continue,
                        }
                    } else {
                        tokio::time::sleep_until(due_at).await;
                    }
                    due = None;
                    wait = DHT_ANNOUNCE_INTERVAL;
                    early = EARLY_SEARCH_MIN;
                }
            },
        )
    }
}

// Where in DHT_ANNOUNCE_INTERVAL a torrent's announces fall, from its info
// hash: torrents started together, as at a session's start, announce spread
// over the interval instead of all at once every interval. libtorrent's
// `session_impl::on_dht_announce` walks its torrents one per
// `dht_announce_interval / torrents` for the same spread; a slot from the
// hash gives it with no session-wide walk.
fn announce_slot(info_hash: &Id20) -> std::time::Duration {
    let mut first = [0u8; 8];
    first.copy_from_slice(&info_hash.0[..8]);
    let at = u64::from_be_bytes(first) as u128;
    // Whole milliseconds, the timer's grain.
    let interval = DHT_ANNOUNCE_INTERVAL.as_millis();
    std::time::Duration::from_millis(((interval * at) >> 64) as u64)
}

impl<C: RecursiveRequestCallbacks> RecursiveRequest<C> {
    fn table(&self, is_v4: bool) -> &RwLock<RoutingTable> {
        self.dht.table(is_v4)
    }

    // The candidates every search of the `is_v4` family starts from, whatever
    // it looks for (libtorrent `find_data::start`, then
    // `traversal_algorithm::start`, which a get_peers, a bootstrap and a
    // refresh each run): the nodes `given`, or with none given the table's
    // SEARCH_K closest to the target; and when fewer than SEARCH_MIN_SEEDS of
    // those are nodes we can count on, every router of the family too
    // (`add_router_entries`). A node given is counted; one from the table
    // only once it has answered and not failed since, as libtorrent's table
    // holds no other: its saved nodes seed a bootstrap without entering the
    // table (dht_tracker.cpp:184-191, v2.0.15), and a node that keeps
    // failing leaves it. So a table of stale or silent nodes does not keep a
    // search from the routers. A router already among them is asked once,
    // as `offer` keeps one candidate per address.
    fn start(
        &self,
        is_v4: bool,
        given: impl Iterator<Item = (Option<Id20>, SocketAddr)>,
    ) -> Candidates {
        let mut candidates = Candidates::new(self.info_hash);
        for (id, addr) in given {
            candidates.offer(id, addr);
        }
        let mut seeds = candidates.list.len();
        if candidates.list.is_empty() {
            // Good nodes first, then questionable, unknown and bad, each
            // tier nearest first.
            let table = self.table(is_v4).read();
            let now = now();
            let mut closest = Vec::with_capacity(SEARCH_K);
            for tier in [
                NodeStatus::Good,
                NodeStatus::Questionable,
                NodeStatus::Unknown,
                NodeStatus::Bad,
            ] {
                if closest.len() == SEARCH_K {
                    break;
                }
                closest.extend(
                    table
                        .closest(&self.info_hash, SEARCH_K - closest.len(), |n| {
                            n.status(now) == tier
                        })
                        .iter()
                        .map(|n| (n.id(), n.addr(), n.is_confirmed(now))),
                );
            }
            drop(table);
            for (id, addr, confirmed) in closest {
                candidates.offer(Some(id), addr);
                seeds += usize::from(confirmed);
            }
        }
        if seeds < SEARCH_MIN_SEEDS {
            for router in self.dht.routers.read().iter() {
                if router.is_ipv4() == is_v4 {
                    candidates.offer(None, *router);
                }
            }
        }
        candidates
    }

    // A search of the `is_v4` family from `given` (see `start`).
    async fn search(
        &self,
        is_v4: bool,
        given: impl Iterator<Item = (Option<Id20>, SocketAddr)>,
    ) -> crate::dht::Result<SearchOutcome> {
        self.walk(self.start(is_v4, given)).await
    }

    // Walk towards the target from `candidates`: ask the closest not asked
    // yet, at most SEARCH_BRANCHING at a time, and offer the nodes each answer
    // names as new candidates. A node that fails or does not answer in time is
    // marked failed and skipped. A node named at an address it cannot be
    // reached at is not offered. The search ends when nothing is in flight and
    // no candidate is left to ask.
    async fn walk(&self, mut candidates: Candidates) -> crate::dht::Result<SearchOutcome> {
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
                        .map(move |r| ((id, addr), r))
                        .instrument(debug_span!("ask", addr = addr.to_string())),
                );
            }
            let Some(((id, addr), answer)) = in_flight.next().await else {
                self.callbacks.on_search_end(self, &candidates);
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
            outcome.nodes += usize::from(id.is_some());
            candidates.settle(addr, CandidateState::Answered);
            candidates.give_token(addr, response.token);
            // A search that wants no peers (a bootstrap or a refresh) has no
            // receiver: a node that names peers anyway is still walked past,
            // never the end of the search.
            for peer in response.values.into_iter().flatten() {
                let _ = self.peer_tx.send(peer.0);
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
                .filter(|node| addr.is_ipv4() == node.addr.is_ipv4())
                .filter(|node| !is_martian(node.addr, addr));
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

// One family's table growth, as `DhtState::grow` records it.
struct Growth {
    // How many times the table has grown.
    generation: AtomicU64,
    // Rings as it grows.
    grew: tokio::sync::Notify,
    // The early searches left until it grows again.
    turns: tokio::sync::Semaphore,
}

impl Growth {
    fn new() -> Self {
        Self {
            generation: AtomicU64::new(0),
            grew: tokio::sync::Notify::new(),
            turns: tokio::sync::Semaphore::new(0),
        }
    }

    // Resolves at `at` or later, once the table has grown past `seen` and an
    // early turn is free; the turn is spent. A wake signal throughout: the
    // growth, then the turn, which wakes the torrents waiting for one in the
    // order they asked.
    async fn early_turn(&self, seen: u64, at: tokio::time::Instant) {
        tokio::time::sleep_until(at).await;
        loop {
            let mut grew = std::pin::pin!(self.grew.notified());
            grew.as_mut().enable();
            if self.generation.load(Ordering::Acquire) > seen {
                break;
            }
            grew.await;
        }
        self.turns
            .acquire()
            .await
            .expect("the early turns are never closed")
            .forget();
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
    // The addresses the bootstrap names resolved to: the routers a search
    // short of nodes also asks (libtorrent `routing_table::m_router_nodes`).
    routers: RwLock<BTreeSet<SocketAddr>>,
    // Rings as a router address becomes known: a bootstrap or a search that
    // had nothing to ask has something now.
    routers_known: tokio::sync::Notify,
    // How the table of each family has grown (see `grow`): what a torrent's
    // search that fell short waits on to search again.
    growth_v4: Growth,
    growth_v6: Growth,
    listen_addr: SocketAddr,

    // Sending requests to the worker.
    rate_limiter: RateLimiter,
    // This is to send raw messages
    worker_sender: UnboundedSender<WorkerSendRequest>,

    cancellation_token: CancellationToken,

    pub(crate) peer_store: PeerStore,

    // Which sources have sent too many datagrams to be read.
    query_limiter: parking_lot::Mutex<QueryLimiter>,

    // The bytes the DHT may send: every packet spends it, and a query that
    // arrives while it is spent goes unanswered.
    send_quota: parking_lot::Mutex<SendQuota>,

    // BEP 5 "v": sent with every outgoing message when set.
    client_version: Option<ByteBufOwned>,

    // The torrents' peer searches walking at once; the rest wait their
    // turn, so a session that starts ten thousand torrents does not walk
    // for all of them in its first seconds.
    walks: tokio::sync::Semaphore,
}

// libtorrent queues the torrents that want a DHT announce now
// (`session_impl::prioritize_dht`) and starts one per `on_dht_announce`;
// this keeps a few walking at once instead, so a lone torrent added to an
// idle session starts at once.
const WALKS_AT_ONCE: usize = 16;

impl DhtState {
    fn new_internal(
        id: Id20,
        sender: UnboundedSender<WorkerSendRequest>,
        routing_table_v4: Option<RoutingTable>,
        routing_table_v6: Option<RoutingTable>,
        listen_addr: SocketAddr,
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
            routers: RwLock::default(),
            routers_known: tokio::sync::Notify::new(),
            growth_v4: Growth::new(),
            growth_v6: Growth::new(),
            worker_sender: sender,
            listen_addr,
            rate_limiter: make_rate_limiter(),
            peer_store: PeerStore::new(id),
            query_limiter: parking_lot::Mutex::new(QueryLimiter::new()),
            send_quota: parking_lot::Mutex::new(SendQuota::new(
                DHT_UPLOAD_RATE_LIMIT,
                tokio::time::Instant::now(),
            )),
            cancellation_token,
            client_version,
            walks: tokio::sync::Semaphore::new(WALKS_AT_ONCE),
        }
    }

    fn table(&self, is_v4: bool) -> &RwLock<RoutingTable> {
        if is_v4 {
            &self.routing_table_v4
        } else {
            &self.routing_table_v6
        }
    }

    fn growth(&self, is_v4: bool) -> &Growth {
        if is_v4 {
            &self.growth_v4
        } else {
            &self.growth_v6
        }
    }

    // The `is_v4` table has grown: nodes that answered have entered it (see
    // `watch_growth`), or a self-refresh round ended with the table turned
    // deep. Torrents whose search fell short before this may search again,
    // at most EARLY_SEARCHES_PER_GROWTH of them until the next growth: the
    // early turns are topped up to that, never added up, so ten thousand
    // short torrents are woken but search EARLY_SEARCHES_PER_GROWTH at a
    // time, one batch per growth. Nodes entering grow the table at most once
    // every GROWTH_SPACING, however many enter; a table turning deep, at most
    // once a self-refresh round. So a family starts at most
    // EARLY_SEARCHES_PER_GROWTH early searches every GROWTH_SPACING, and one
    // more batch as its table turns deep.
    fn grow(&self, is_v4: bool) {
        let growth = self.growth(is_v4);
        growth.generation.fetch_add(1, Ordering::Release);
        let spent = EARLY_SEARCHES_PER_GROWTH.saturating_sub(growth.turns.available_permits());
        growth.turns.add_permits(spent);
        growth.grew.notify_waiters();
    }

    // libtorrent `dht_tracker::add_router_node` for each address a router's
    // name resolves to. Wakes what waits for a router when one is new.
    fn add_routers(&self, addrs: &[SocketAddr]) {
        let mut routers = self.routers.write();
        let before = routers.len();
        routers.extend(addrs);
        let added = routers.len() > before;
        drop(routers);
        if added {
            self.routers_known.notify_waiters();
        }
    }

    // Keep the `is_v4` table filled: a find_node search for our own id,
    // from the table and, while it holds fewer than SEARCH_MIN_SEEDS nodes
    // that answer, the routers (see `start`). The first runs at once. While
    // the table is shallow, under SHALLOW_DEPTH deep, the next runs after a
    // backoff, from BOOTSTRAP_RETRY_MIN doubling to SELF_REFRESH_INTERVAL,
    // back to the start as a round adds nodes; once it is deep, one every
    // SELF_REFRESH_INTERVAL. libtorrent bootstraps from its saved nodes and
    // routers at start (dht_tracker.cpp:184-195, v2.0.15) and bootstraps
    // again from `node::tick` whenever the table is under 4 deep and its
    // last one is 10 minutes old (node.cpp:647-662), so a bootstrap made
    // while nothing reached the network is not the last. The backoff makes
    // the first retries sooner and keeps a route that carries nothing to a
    // few datagrams a round, a round every SELF_REFRESH_INTERVAL at most.
    // A round that had no one to ask waits for a node to enter the table or
    // a router to become known, and a new router also ends a backoff early.
    // Beside the rounds, nodes that answered entering the table are a
    // growth as they enter, not only as the round that met them ends (see
    // `watch_growth`); a round after which the table has turned deep is one
    // too (see `grow`).
    async fn refresh_self(self: Arc<Self>, is_v4: bool) -> crate::dht::Result<()> {
        tokio::select! {
            done = self.clone().refresh_self_rounds(is_v4) => done,
            never = self.watch_growth(is_v4) => match never {},
        }
    }

    // The table grows as nodes that answered enter it: the first entry
    // after the last growth is a growth at once, and the entries made in
    // the GROWTH_SPACING after a growth are one more as it ends. A round of
    // our own id on a tunnel can take a minute while the nodes that answer
    // it enter in its first seconds; a torrent's search that fell short
    // need not wait for the round's end. libtorrent searches again only at
    // a torrent's next turn (see `request_peers_forever`); the spacing is
    // its node tick (dht_tracker.cpp:268-283, v2.0.15). Woken by the
    // table's `entered`, never by a timer while nothing enters, and only the
    // entry count is read per wake.
    async fn watch_growth(&self, is_v4: bool) -> std::convert::Infallible {
        let (entered, mut grown) = {
            let table = self.table(is_v4).read();
            (table.entered(), table.entries())
        };
        loop {
            let mut next_entered = std::pin::pin!(entered.notified());
            next_entered.as_mut().enable();
            let entries = self.table(is_v4).read().entries();
            if entries == grown {
                next_entered.await;
                continue;
            }
            grown = entries;
            self.grow(is_v4);
            tokio::time::sleep(GROWTH_SPACING).await;
        }
    }

    async fn refresh_self_rounds(self: Arc<Self>, is_v4: bool) -> crate::dht::Result<()> {
        let entered = self.table(is_v4).read().entered();
        let mut retry = BOOTSTRAP_RETRY_MIN;
        let mut deep = false;
        loop {
            let mut next_entered = std::pin::pin!(entered.notified());
            next_entered.as_mut().enable();
            let mut next_router = std::pin::pin!(self.routers_known.notified());
            next_router.as_mut().enable();
            let before = self.table(is_v4).read().entries();
            let outcome = RecursiveRequest::find_node_for_routing_table(
                self.clone(),
                self.id,
                is_v4,
                std::iter::empty(),
            )
            .instrument(debug_span!("refresh_self"))
            .await?;
            let (nodes, entries, depth) = {
                let table = self.table(is_v4).read();
                (table.len(), table.entries(), table.depth(now()))
            };
            let added = entries > before;
            // The nodes that entered grew the table as they entered.
            if depth >= SHALLOW_DEPTH && !deep {
                self.grow(is_v4);
            }
            debug!(
                answered = outcome.answered,
                failed = outcome.failed,
                nodes,
                depth,
                "own id refreshed"
            );
            if outcome.answered + outcome.failed == 0 {
                tokio::select! {
                    () = next_entered => {}
                    () = next_router => {}
                }
                retry = BOOTSTRAP_RETRY_MIN;
                continue;
            }
            if depth >= SHALLOW_DEPTH {
                deep = true;
                retry = BOOTSTRAP_RETRY_MIN;
                tokio::time::sleep(SELF_REFRESH_INTERVAL).await;
                continue;
            }
            deep = false;
            if added {
                retry = BOOTSTRAP_RETRY_MIN;
            }
            tokio::select! {
                () = tokio::time::sleep(retry) => {}
                () = next_router => {}
            }
            retry = (retry * 2).min(SELF_REFRESH_INTERVAL);
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
        // The eight good or questionable nodes of the family nearest the
        // target, nearest first; unknown and bad nodes are not handed out.
        let it = table
            .closest(&target, 8, |n| {
                matches!(n.status(now), NodeStatus::Good | NodeStatus::Questionable)
                    && A::from_socket_addr(n.addr()).is_some()
            })
            .into_iter()
            .filter_map(|r| {
                Some(Node {
                    id: r.id(),
                    addr: A::from_socket_addr(r.addr())?,
                })
            });
        CompactNodeInfo::new_from_iter(it)
    }

    /// The message in a datagram from `addr`, or none for a datagram that is
    /// not one, or that comes from a source sending too many. As libtorrent's
    /// `dht_tracker::incoming_packet` (dht_tracker.cpp:512-548, v2.0.15), the
    /// source is counted for every datagram shaped like a dictionary, answers
    /// to our own queries too, before a byte of it is parsed.
    fn read_datagram(&self, buf: &[u8], addr: SocketAddr) -> Option<Message<ByteBufOwned>> {
        if buf.len() <= 20 || buf.first() != Some(&b'd') || buf.last() != Some(&b'e') {
            return None;
        }
        if !self
            .query_limiter
            .lock()
            .admit(addr.ip(), tokio::time::Instant::now())
        {
            trace!("ignoring a datagram from {addr}: it sends too many");
            return None;
        }
        match bprotocol::deserialize_message::<ByteBufOwned>(buf) {
            Ok(message) => Some(message),
            Err(e) => {
                trace!("{addr}: error deserializing incoming message: {e:#}");
                None
            }
        }
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

        // libtorrent node.cpp:324-328, v2.0.15: no budget, no answer.
        if !self
            .send_quota
            .lock()
            .has_quota(tokio::time::Instant::now())
        {
            trace!("dropping a query from {addr}: the DHT's send budget is spent");
            return Ok(());
        }

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
                            &self.peer_store.gen_token_for(req.info_hash, addr)[..],
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

struct DhtWorker {
    socket: Arc<dyn DatagramSocket>,
    resolver: Arc<dyn Resolver>,
    dht: Arc<DhtState>,
}

// Windows fails recv_from on a UDP socket with these for a single datagram, and the socket keeps
// working afterwards:
// - WSAEMSGSIZE: the datagram was longer than the buffer and got truncated.
// - WSAENETRESET: an earlier send_to got an ICMP time exceeded back.
// - WSAECONNRESET: an earlier send_to got an ICMP port unreachable back.
// A socket carried through a tunnel fails it with InvalidData for a datagram
// longer than the buffer, and also keeps working.
fn is_datagram_error(e: &std::io::Error) -> bool {
    const WSAEMSGSIZE: i32 = 10040;
    const WSAENETRESET: i32 = 10052;
    const WSAECONNRESET: i32 = 10054;
    e.kind() == std::io::ErrorKind::InvalidData
        || cfg!(windows)
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

    /// Resolve a bootstrap `host:port` (or `ip:port`) string. A node is
    /// asked at an address, so a name the network resolves where it exits
    /// gives none.
    async fn lookup_bootstrap(&self, hostname: &str) -> std::io::Result<Vec<SocketAddr>> {
        if let Ok(addr) = hostname.parse::<SocketAddr>() {
            return Ok(vec![addr]);
        }
        let (host, port) = hostname
            .rsplit_once(':')
            .and_then(|(host, port)| Some((host, port.parse::<u16>().ok()?)))
            .ok_or_else(|| {
                std::io::Error::new(std::io::ErrorKind::InvalidInput, "invalid socket address")
            })?;
        match self.resolver.resolve(host, port).await? {
            Resolved::Addresses(found) => Ok(found),
            Resolved::AtExit => Err(std::io::Error::new(
                std::io::ErrorKind::Unsupported,
                "the name is resolved where the leg exits, and a node needs its address",
            )),
        }
    }

    // Resolve the router `hostname` until it resolves, at most a minute
    // apart, and make its addresses known (libtorrent
    // `dht_tracker::add_router_node` for each address). A name that cannot
    // give an address, a malformed one or one resolved where a leg exits, is
    // not tried again. The first failure of all the routers is logged at
    // WARN, every later one at DEBUG: `warned` is shared by them.
    async fn resolve_router(&self, hostname: &str, warned: &AtomicBool) {
        let backoff = ExponentialBuilder::new()
            .with_max_delay(Duration::from_secs(60))
            .with_jitter()
            .without_max_times();
        let retried = |err: &std::io::Error| {
            !matches!(
                err.kind(),
                std::io::ErrorKind::InvalidInput | std::io::ErrorKind::Unsupported
            )
        };
        let found = (|| self.lookup_bootstrap(hostname))
            .retry(backoff)
            .when(retried)
            .notify(|err, retry_in| {
                if warned.swap(true, Ordering::Relaxed) {
                    debug!(?retry_in, "error looking up {hostname}: {err:#}");
                } else {
                    warn!(
                        ?retry_in,
                        "error looking up {hostname}: {err:#}; the DHT runs from its saved nodes and the nodes peers name, and keeps retrying"
                    );
                }
            })
            .instrument(debug_span!("bootstrap", hostname))
            .await;
        match found {
            Ok(addrs) => {
                debug!(hostname, routers = addrs.len(), "router resolved");
                self.dht.add_routers(&addrs);
            }
            Err(err) => warn!("error looking up {hostname}: {err:#}; it is not tried again"),
        }
    }

    // Routers are one way to seed the routing table; the table saved from an
    // earlier run and the nodes peers name (`add_node`) are the others. Each
    // router's name is resolved until it resolves; `refresh_self` asks the
    // routers, as many times as the table needs them.
    async fn bootstrap(&self, bootstrap_addrs: &[String]) {
        if bootstrap_addrs.is_empty() {
            info!(
                "no DHT bootstrap hosts: the DHT runs from its saved nodes and the nodes peers name"
            );
            return;
        }
        let warned = AtomicBool::new(false);
        let mut futs = bootstrap_addrs
            .iter()
            .map(|addr| self.resolve_router(addr, &warned))
            .collect::<FuturesUnordered<_>>();
        while futs.next().await.is_some() {}
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
                    futs.push(
                        RecursiveRequest::find_node_for_routing_table(
                            self.dht.clone(), random_id, is_v4, std::iter::empty()
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
                        // An error reply is no answer (see
                        // `RecursiveRequestCallbacksFindNodes`).
                        match self.dht.request(Request::Ping, addr).await {
                            Ok(ResponseOrError::Response(_)) => {
                                table.write().mark_response(&id, now());
                            },
                            Ok(ResponseOrError::Error(e)) => {
                                table.write().mark_error(&id);
                                debug!(?e, "node refused the ping");
                            }
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
        socket: &dyn DatagramSocket,
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
                self.dht.send_quota.lock().spend(buf.len());
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
                    Err(e) if e.kind() == std::io::ErrorKind::BrokenPipe => {
                        debug!("the DHT's socket has closed: {e:#}");
                        return Err(Error::SocketClosed);
                    }
                    Err(e) => return Err(Error::Recv(e)),
                };
                if let Some(msg) = self.dht.read_datagram(&buf[..size], addr)
                    && output_tx.send((msg, addr)).await.is_err()
                {
                    return Err(Error::DhtDead);
                }
            }
        };
        let result = tokio::select! {
            err = writer => err,
            err = reader => err,
        };
        result
    }

    async fn peer_store_purger(&self) -> crate::dht::Result<()> {
        let mut interval = tokio::time::interval(PEER_PURGE_INTERVAL);
        loop {
            interval.tick().await;
            self.dht.peer_store.garbage_collect_peers();
        }
    }

    async fn start(
        self,
        in_rx: UnboundedReceiver<WorkerSendRequest>,
        bootstrap_addrs: &[String],
    ) -> crate::dht::Result<()> {
        let (out_tx, mut out_rx) = channel(1);
        let framer = self
            .framer(&*self.socket, in_rx, out_tx)
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

        let peer_store_purger = self
            .peer_store_purger()
            .instrument(debug_span!("peer_store_purger"));

        let refresh_self_v4 = self
            .dht
            .clone()
            .refresh_self(true)
            .instrument(debug_span!("refresh_self_v4"));
        let refresh_self_v6 = self
            .dht
            .clone()
            .refresh_self(false)
            .instrument(debug_span!("refresh_self_v6"));

        tokio::pin!(refresh_self_v4);
        tokio::pin!(refresh_self_v6);
        tokio::pin!(framer);
        tokio::pin!(bootstrap);
        tokio::pin!(response_reader);
        tokio::pin!(pinger_v4);
        tokio::pin!(bucket_refresher_v4);
        tokio::pin!(pinger_v6);
        tokio::pin!(bucket_refresher_v6);
        tokio::pin!(peer_store_purger);

        loop {
            tokio::select! {
                err = &mut framer => {
                    // A socket closed under the DHT ends it, as its session
                    // closing does: not a fault.
                    if let Err(Error::SocketClosed) = err {
                        return Ok(());
                    }
                    return Error::task_finished(&"framer", err);
                },
                () = &mut bootstrap, if !bootstrap_done => {
                    bootstrap_done = true;
                },
                err = &mut refresh_self_v4 => {
                    return Error::task_finished(&"refresh_self_v4", err);
                },
                err = &mut refresh_self_v6 => {
                    return Error::task_finished(&"refresh_self_v6", err);
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
                },
                err = &mut peer_store_purger => {
                    return Error::task_finished(&"peer_store_purger", err);
                }
            }
        }
    }
}

#[derive(Default)]
pub struct DhtConfig {
    /// Our node id, as restored or given. It is kept unless it reads as a
    /// BEP 20 peer id; otherwise, and when none is given, the id is
    /// entirely random (BEP 5), and routing tables built around another id
    /// are dropped.
    pub peer_id: Option<Id20>,
    pub bootstrap_addrs: Option<Vec<String>>,
    pub routing_table: Option<RoutingTable>,
    pub routing_table_v6: Option<RoutingTable>,
    pub listen_addr: Option<SocketAddr>,
    pub cancellation_token: Option<CancellationToken>,
    /// Send and receive DHT traffic through this socket. When set,
    /// `listen_addr` and `sockets` are ignored and the DHT reports the
    /// socket's `bind_addr()` as its listen address.
    pub socket: Option<Arc<dyn DatagramSocket>>,
    /// With no `socket`, the factory that binds the DHT's socket at
    /// `listen_addr`. A DHT given neither does not start.
    pub sockets: Option<Arc<dyn SocketFactory>>,
    /// Look up bootstrap hostnames through this resolver; with none, only
    /// bootstrap addresses are asked.
    pub resolver: Option<Arc<dyn Resolver>>,
    /// The BEP 5 client version ("v"): by convention the two BEP 20 client
    /// letters followed by two version bytes. Sent with every outgoing message
    /// when set; when not, no "v" is sent.
    pub client_version: Option<Vec<u8>>,
    /// Track the DHT's worker in this tracker. The worker owns the socket, so once the
    /// tracker is empty after cancellation, the socket is gone.
    pub tasks: Option<TaskTracker>,
}

// An entirely random node id.
//
// TODO(BEP 42): tie the node id to the node's external IP. Its first 21
// bits are then a CRC32C of the masked IP and a small random number kept in
// the last byte, so a node cannot choose where it sits among the ids.
// libtorrent keeps a node whose id passes that check over one whose id does
// not when a routing table slot is contested (`dht_prefer_verified_node_ids`,
// on by default), and refuses none (`dht_enforce_node_id`, off by default).
// What it needs here:
// - the external IP, read from the `ip` of replies once several nodes agree
//   on it; the DHT stores none today, and the id is made before any reply;
// - the id built from that IP;
// - the routing tables rebuilt around the new id when it is first known and
//   whenever the external IP changes.
fn random_node_id() -> Id20 {
    use rand::Rng as _;
    let mut id = [0u8; 20];
    rand::rng().fill_bytes(&mut id);
    Id20::new(id)
}

impl DhtState {
    pub fn cancellation_token(&self) -> &CancellationToken {
        &self.cancellation_token
    }

    #[inline(never)]
    pub fn with_config<'a>(mut config: DhtConfig) -> BoxFuture<'a, crate::dht::Result<Arc<Self>>> {
        async move {
            let socket = match (config.socket.take(), config.sockets.take()) {
                (Some(socket), _) => socket,
                (None, Some(sockets)) => {
                    let addr = config
                        .listen_addr
                        .unwrap_or((Ipv6Addr::UNSPECIFIED, 0).into());
                    sockets.bind_udp(addr).await.map_err(Error::Bind)?
                }
                (None, None) => {
                    return Err(Error::Bind(std::io::Error::other(
                        "the DHT was given no socket",
                    )));
                }
            };

            let listen_addr = socket.bind_addr();
            info!("DHT listening on {:?}", listen_addr);

            // A node id carries no client's fingerprint.
            let peer_id = config
                .peer_id
                .filter(|id| try_decode_peer_id(*id).is_none())
                .unwrap_or_else(random_node_id);
            info!("starting up DHT with peer id {:?}", peer_id);
            // A routing table is laid out by distance from its own node's id: it
            // does not carry over to another id.
            let routing_table = config.routing_table.filter(|t| t.id() == peer_id);
            let routing_table_v6 = config.routing_table_v6.filter(|t| t.id() == peer_id);
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
                token,
                config.client_version.map(ByteBufOwned::from),
            ));

            spawn_with_cancel(
                debug_span!("dht"),
                "dht",
                state.cancellation_token.clone(),
                {
                    let state = state.clone();
                    let resolver = config.resolver.take().unwrap_or_else(|| Arc::new(NoNames));
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
        RequestPeersStream::new(self.clone(), info_hash, announce_port, announce_port)
    }

    /// [`Self::get_peers`], announcing `port_v4` to IPv4 nodes and
    /// `port_v6` to IPv6 ones.
    pub fn get_peers_announcing(
        self: &Arc<Self>,
        info_hash: Id20,
        port_v4: Option<u16>,
        port_v6: Option<u16>,
    ) -> RequestPeersStream {
        RequestPeersStream::new(self.clone(), info_hash, port_v4, port_v6)
    }

    pub fn listen_addr(&self) -> SocketAddr {
        self.listen_addr
    }

    /// Pings a node a peer named (BEP 5 PORT) and, when it answers, adds it
    /// to the routing table under the id it answered with (libtorrent
    /// `node::add_node`: "ping the node, and if we get a reply, it will be
    /// added to the routing table").
    pub fn add_node(self: &Arc<Self>, addr: SocketAddr) {
        let addr = SocketAddr::new(addr.ip().to_canonical(), addr.port());
        let dht = self.clone();
        spawn_with_cancel(
            debug_span!("dht_add_node", %addr),
            "dht_add_node",
            self.cancellation_token.clone(),
            async move {
                // A node that does not answer, or answers with an error, is
                // simply not added: most nodes a peer names are gone.
                match dht.request(Request::Ping, addr).await {
                    Ok(ResponseOrError::Response(r)) => {
                        let mut table = dht.get_table_for_addr(addr).write();
                        if !matches!(table.add_node(r.id, addr), InsertResult::Ignored) {
                            table.mark_response(&r.id, now());
                        }
                    }
                    Ok(ResponseOrError::Error(e)) => debug!(?e, "node refused the ping"),
                    Err(e) => debug!(error = %e, "node did not answer the ping"),
                }
                Ok::<_, Error>(())
            },
        );
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
    use crate::core::net::DatagramSocket;
    use std::sync::Arc;

    use super::{DhtConfig, DhtState};
    use crate::dht::{
        Dht,
        bprotocol::{self, MessageKind},
    };

    async fn start_dht(bootstrap_addr: SocketAddr) -> Dht {
        DhtState::with_config(DhtConfig {
            listen_addr: Some((Ipv4Addr::LOCALHOST, 0).into()),
            sockets: Some(crate::test_support::socket_factory()),
            bootstrap_addrs: Some(vec![bootstrap_addr.to_string()]),
            ..Default::default()
        })
        .await
        .unwrap()
    }

    async fn assert_answers_ping(dht: &Dht) {
        let client = crate::test_support::loopback_udp().await;
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
        let closed = crate::test_support::loopback_udp().await.bind_addr();
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
        let silent = crate::test_support::loopback_udp().await;
        let dht = start_dht(silent.bind_addr()).await;

        // Some platforms refuse to send a datagram this large, which is fine.
        let _ = silent.send_to(&[0u8; 20_000], dht.listen_addr()).await;
        assert_answers_ping(&dht).await;
    }

    #[tokio::test]
    async fn test_dht_answers_ping_through_custom_socket() {
        // Stays open, so the bootstrap query gets neither an answer nor an ICMP error.
        let silent = crate::test_support::loopback_udp().await;
        let custom = crate::test_support::loopback_udp().await;
        let custom_addr = custom.bind_addr();

        let dht = DhtState::with_config(DhtConfig {
            bootstrap_addrs: Some(vec![silent.bind_addr().to_string()]),
            socket: Some(custom),
            ..Default::default()
        })
        .await
        .unwrap();
        assert_eq!(dht.listen_addr(), custom_addr);

        let client = crate::test_support::loopback_udp().await;
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
            ) -> BoxFuture<'a, std::io::Result<crate::core::net::Resolved>> {
                Box::pin(async move {
                    assert_eq!(host, "bootstrap.invalid");
                    assert_eq!(port, self.target.port());
                    self.calls.fetch_add(1, Ordering::SeqCst);
                    Ok(crate::core::net::Resolved::Addresses(vec![self.target]))
                })
            }
        }

        let bootstrap = crate::test_support::loopback_udp().await;
        let bootstrap_addr = bootstrap.bind_addr();
        let resolver = Arc::new(CountingResolver {
            target: bootstrap_addr,
            calls: AtomicUsize::new(0),
        });

        let dht = DhtState::with_config(DhtConfig {
            listen_addr: Some((Ipv4Addr::LOCALHOST, 0).into()),
            sockets: Some(crate::test_support::socket_factory()),
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

    // With no bootstrap host the DHT still runs: it answers, a node a peer
    // names enters its table, and a torrent's search, asked for while the
    // table was empty, goes to that node.
    #[tokio::test]
    async fn test_a_dht_with_no_bootstrap_host_runs_from_the_nodes_peers_name() {
        use crate::core::hash_id::Id20;
        use futures::StreamExt;

        let dht = DhtState::with_config(DhtConfig {
            listen_addr: Some((Ipv4Addr::LOCALHOST, 0).into()),
            sockets: Some(crate::test_support::socket_factory()),
            bootstrap_addrs: Some(Vec::new()),
            ..Default::default()
        })
        .await
        .unwrap();
        let info_hash = Id20::new([0x22; 20]);
        let mut peers = dht.get_peers(info_hash, None);
        assert!(futures::poll!(peers.next()).is_pending());

        // A node a peer named (BEP 5 PORT).
        let node = crate::test_support::loopback_udp().await;
        dht.add_node(node.bind_addr());
        let ping = recv_message_from(&node, dht.listen_addr()).await;
        assert!(matches!(ping.kind, MessageKind::PingRequest(_)));
        let mut answer = Vec::new();
        bprotocol::serialize_message(
            &mut answer,
            ping.transaction_id,
            None,
            None,
            MessageKind::<ByteBufOwned>::Response(bprotocol::Response {
                id: Id20::new([0x33; 20]),
                ..Default::default()
            }),
        )
        .unwrap();
        node.send_to(&answer, dht.listen_addr()).await.unwrap();

        let search = loop {
            let m = recv_message_from(&node, dht.listen_addr()).await;
            if let MessageKind::GetPeersRequest(r) = m.kind {
                break r;
            }
        };
        assert_eq!(search.info_hash, info_hash);
        assert_eq!(dht.stats().routing_table_size, 1);
        dht.cancellation_token().cancel();
    }

    // Start a DHT on loopback whose one bootstrap node is `bootstrap`, a socket the
    // test holds: whatever the DHT sends lands there and nowhere else.
    async fn start_dht_with(bootstrap: &Arc<dyn DatagramSocket>, config: DhtConfig) -> Dht {
        DhtState::with_config(DhtConfig {
            listen_addr: Some((Ipv4Addr::LOCALHOST, 0).into()),
            sockets: Some(crate::test_support::socket_factory()),
            bootstrap_addrs: Some(vec![bootstrap.bind_addr().to_string()]),
            ..config
        })
        .await
        .unwrap()
    }

    async fn recv_message_from(
        sock: &Arc<dyn DatagramSocket>,
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
        let bootstrap = crate::test_support::loopback_udp().await;
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
    async fn test_node_id_carries_no_fingerprint() {
        use crate::core::hash_id::Id20;
        use crate::core::peer_id::generate_azereus_style;
        use crate::dht::routing_table::RoutingTable;

        let bootstrap = crate::test_support::loopback_udp().await;
        let weaver = generate_azereus_style(*b"WV", crate::crate_version!());

        // By default: an entirely random id, not Weaver's fingerprint.
        let dht = start_dht_with(&bootstrap, DhtConfig::default()).await;
        assert!(!dht.stats().id.0.starts_with(&weaver.0[..8]));
        dht.cancellation_token().cancel();

        // A restored id is kept, with its table.
        let kept = Id20::new([7u8; 20]);
        let dht = start_dht_with(
            &bootstrap,
            DhtConfig {
                peer_id: Some(kept),
                routing_table: Some(RoutingTable::new(kept, None)),
                ..Default::default()
            },
        )
        .await;
        assert_eq!(dht.stats().id, kept);
        dht.cancellation_token().cancel();

        // One that reads as a BEP 20 peer id is replaced, and so is the table
        // built around it.
        let mut fingerprinted = [9u8; 20];
        fingerprinted[..8].copy_from_slice(b"-XY1234-");
        let fingerprinted = Id20::new(fingerprinted);
        let dht = start_dht_with(
            &bootstrap,
            DhtConfig {
                peer_id: Some(fingerprinted),
                routing_table: Some(RoutingTable::new(fingerprinted, None)),
                ..Default::default()
            },
        )
        .await;
        let id = dht.stats().id;
        assert!(!id.0.starts_with(b"-XY1234-"));
        dht.with_routing_tables(|v4, v6| {
            assert_eq!(v4.id(), id);
            assert_eq!(v6.id(), id);
        });
        dht.cancellation_token().cancel();
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
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        let id = crate::core::hash_id::Id20::new([0x11; 20]);
        let dht = DhtState::new_internal(
            id,
            tx,
            None,
            None,
            (Ipv4Addr::LOCALHOST, 0).into(),
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
        let mut search = std::pin::pin!(req.search(true, nodes.clone().into_iter()));

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
                failed: 20,
                nodes: 0
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
        let mut search = Box::pin(req.search(true, silent_nodes(8).into_iter()));

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
        let mut search = std::pin::pin!(req.search(true, silent_nodes(8).into_iter()));

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
                failed: 8,
                nodes: 0
            }
        );
        assert_eq!(dht.get_stats().outstanding_requests, 0);
    }

    #[tokio::test(start_paused = true)]
    async fn test_a_source_over_the_query_limit_is_ignored_until_its_block_lapses() {
        use crate::dht::query_limiter::{BLOCK, QUERY_LIMIT, WINDOW};
        use tokio::sync::mpsc::error::TryRecvError;

        let (dht, mut sent) = fixture_dht();
        let flooder = SocketAddr::from(([10, 0, 0, 1], 6881));
        let quiet = SocketAddr::from(([10, 0, 0, 2], 6881));
        // `from` pings us; whether it was answered.
        let mut ping = |from: SocketAddr| {
            if let Some(message) = dht.read_datagram(PING, from) {
                dht.on_received_message(message, from).unwrap();
            }
            match sent.try_recv() {
                Ok(reply) => {
                    assert_eq!(reply.addr, from);
                    assert!(matches!(reply.message.kind, MessageKind::Response(_)));
                    true
                }
                Err(TryRecvError::Empty) => false,
                Err(e) => panic!("{e}"),
            }
        };

        // Every query short of the limit is answered; the one that reaches it,
        // and every one after, is not.
        for _ in 1..QUERY_LIMIT {
            assert!(ping(flooder));
        }
        assert!(!ping(flooder));
        assert!(!ping(flooder));

        // A source under the limit is answered meanwhile.
        assert!(ping(quiet));

        // The block outlasts the window, and the queries sent during it do
        // not lengthen it.
        tokio::time::advance(WINDOW).await;
        assert!(!ping(flooder));
        assert!(ping(quiet));
        tokio::time::advance(BLOCK - WINDOW - Duration::from_secs(1)).await;
        assert!(!ping(flooder));

        // Once it lapses the source is answered again.
        tokio::time::advance(Duration::from_secs(1)).await;
        assert!(ping(flooder));
    }

    const PING: &[u8] = b"d1:ad2:id20:abcdefghij0123456789e1:q4:ping1:t2:aa1:y1:qe";

    // An answer to `request`, as the bytes a node sends.
    fn answer_datagram(request: &super::WorkerSendRequest) -> Vec<u8> {
        let mut buf = Vec::new();
        bprotocol::serialize_message(
            &mut buf,
            request.message.transaction_id.clone(),
            None,
            None,
            MessageKind::Response(bprotocol::Response {
                id: crate::core::hash_id::Id20::new([0x33; 20]),
                ..Default::default()
            }),
        )
        .unwrap();
        buf
    }

    // libtorrent dht_tracker.cpp:538-548, v2.0.15: the source is counted
    // before the datagram is parsed, so datagrams that do not parse count.
    #[tokio::test(start_paused = true)]
    async fn test_a_source_is_counted_for_datagrams_that_do_not_parse() {
        use crate::dht::query_limiter::QUERY_LIMIT;

        let (dht, mut sent) = fixture_dht();
        let mut ping = |from: SocketAddr| {
            if let Some(message) = dht.read_datagram(PING, from) {
                dht.on_received_message(message, from).unwrap();
            }
            sent.try_recv().is_ok()
        };
        // A dictionary, but no KRPC message.
        let junk = b"d1:wi1e1:xi2e1:yi3e1:zi4ee";
        let flooder = SocketAddr::from(([10, 0, 0, 1], 6881));
        let under = SocketAddr::from(([10, 0, 0, 2], 6881));
        for _ in 1..QUERY_LIMIT {
            assert!(dht.read_datagram(junk, flooder).is_none());
        }
        for _ in 2..QUERY_LIMIT {
            assert!(dht.read_datagram(junk, under).is_none());
        }
        // The datagram that reaches the limit is not read; one short of it is.
        assert!(!ping(flooder));
        assert!(ping(under));

        // A datagram that is not shaped like a dictionary is not counted.
        let stranger = SocketAddr::from(([10, 0, 0, 3], 6881));
        for _ in 0..2 * QUERY_LIMIT {
            assert!(
                dht.read_datagram(b"not bencode, not counted", stranger)
                    .is_none()
            );
            assert!(dht.read_datagram(b"d1:xi1ee", stranger).is_none());
        }
        assert!(ping(stranger));
    }

    // A node that answers every query we send it, as fast as a busy DHT asks
    // one node, keeps being heard: its answers are counted, and they stay
    // under the limit.
    #[tokio::test(start_paused = true)]
    async fn test_a_node_answering_our_queries_is_never_ignored() {
        use crate::dht::query_limiter::{QUERY_LIMIT, WINDOW};
        use std::task::Poll;

        let (dht, mut sent) = fixture_dht();
        let node = SocketAddr::from(([10, 0, 0, 1], 6881));
        // Four a second, 40 of the 50 a window allows, for ten windows.
        let per_window = 40;
        assert!(per_window < QUERY_LIMIT);
        for _ in 0..10 * per_window {
            let mut asked = std::pin::pin!(dht.request(super::Request::Ping, node));
            assert!(matches!(futures::poll!(asked.as_mut()), Poll::Pending));
            let request = sent.try_recv().unwrap();
            let message = dht
                .read_datagram(&answer_datagram(&request), node)
                .expect("an answer was not read");
            dht.on_received_message(message, node).unwrap();
            assert!(matches!(
                futures::poll!(asked.as_mut()),
                Poll::Ready(Ok(super::ResponseOrError::Response(_)))
            ));
            tokio::time::advance(WINDOW / per_window).await;
        }
    }

    // Answers `request`, a get_peers a search sent, as the node `id`, with
    // `token`, naming `nodes`.
    fn answer(
        dht: &std::sync::Arc<DhtState>,
        request: &super::WorkerSendRequest,
        id: crate::core::hash_id::Id20,
        token: Option<&[u8]>,
        nodes: &[(crate::core::hash_id::Id20, std::net::SocketAddrV4)],
    ) {
        assert!(matches!(
            request.message.kind,
            MessageKind::GetPeersRequest(_)
        ));
        let nodes = (!nodes.is_empty()).then(|| {
            bprotocol::CompactNodeInfoOwned::new_from_iter(
                nodes.iter().map(|&(id, addr)| bprotocol::Node { id, addr }),
            )
        });
        dht.on_received_message(
            bprotocol::Message {
                transaction_id: request.message.transaction_id.clone(),
                version: None,
                ip: None,
                kind: MessageKind::Response(bprotocol::Response {
                    id,
                    nodes,
                    token: token.map(ByteBufOwned::from),
                    ..Default::default()
                }),
            },
            request.addr,
        )
        .unwrap();
    }

    // libtorrent `torrent::start_announcing` / `session_impl::prioritize_dht`
    // and `dht_announce_interval`: a torrent searches as it starts, then every
    // fifteen minutes, and every search ends in an announce.
    #[tokio::test(start_paused = true)]
    async fn test_peers_are_searched_and_announced_every_fifteen_minutes() {
        assert!(super::announce_slot(&Id20::new([0x22; 20])) < DHT_ANNOUNCE_INTERVAL);
        assert!(super::announce_slot(&Id20::new([0xff; 20])) < DHT_ANNOUNCE_INTERVAL);
        use crate::core::hash_id::Id20;
        use crate::dht::DHT_ANNOUNCE_INTERVAL;
        use std::collections::BTreeMap;

        let (dht, mut sent) = fixture_dht();
        for i in 0..8u8 {
            dht.routing_table_v4
                .write()
                .add_node(Id20::new([i; 20]), SocketAddr::from(([10, 0, i, 1], 6881)));
        }
        let req = std::sync::Arc::new(super::RecursiveRequest::get_peers(
            dht.clone(),
            Id20::new([0x22; 20]),
            Some(6881),
            tokio::sync::mpsc::unbounded_channel().0,
        ));
        let start = tokio::time::Instant::now();
        let task = req.request_peers_forever(true);

        // Requests sent at each moment, until the first past half an hour.
        let mut searched = BTreeMap::<Duration, usize>::new();
        let mut announced = BTreeMap::<Duration, usize>::new();
        loop {
            let request = sent.recv().await.unwrap();
            let at = tokio::time::Instant::now() - start;
            if at > 2 * DHT_ANNOUNCE_INTERVAL {
                break;
            }
            match &request.message.kind {
                MessageKind::GetPeersRequest(_) => {
                    *searched.entry(at).or_default() += 1;
                    answer(&dht, &request, Id20::new([0x33; 20]), Some(b"t"), &[]);
                }
                MessageKind::AnnouncePeer(_) => *announced.entry(at).or_default() += 1,
                kind => panic!("unexpected {kind:?}"),
            }
        }
        task.abort();

        // At start, at the torrent's slot, and an interval after that.
        let slot = super::announce_slot(&Id20::new([0x22; 20]));
        let at: BTreeMap<Duration, usize> = [Duration::ZERO, slot, slot + DHT_ANNOUNCE_INTERVAL]
            .into_iter()
            .map(|at| (at, 8))
            .collect();
        assert_eq!(searched, at);
        assert_eq!(announced, at);
    }

    // A table of `nodes` nodes at 10.0.i.1, each one that has answered when
    // `answered`, one only heard of or restored from an earlier run when not.
    fn add_table_nodes(dht: &DhtState, nodes: u8, answered: bool) {
        use crate::core::hash_id::Id20;
        let mut table = dht.routing_table_v4.write();
        for i in 0..nodes {
            let id = Id20::new([i + 1; 20]);
            table.add_node(id, SocketAddr::from(([10, 0, i, 1], 6881)));
            if answered {
                table.mark_response(&id, std::time::Instant::now());
            }
        }
    }

    // A search that starts from fewer than three nodes that have answered
    // also asks the routers of its family; one with three does not (libtorrent
    // `traversal_algorithm::start`, `add_router_entries`, over a table that
    // holds only nodes that answered). With no node at all, the routers alone
    // start it, at once; three nodes that never answered do not keep it from
    // them.
    #[tokio::test(start_paused = true)]
    async fn test_a_search_short_of_nodes_also_asks_the_routers() {
        use crate::core::hash_id::Id20;
        use std::collections::HashSet;

        let router: SocketAddr = ([192, 0, 2, 1], 6881).into();
        let router_v6: SocketAddr = "[2001:db8::1]:6881".parse().unwrap();
        let node = |i: u8| SocketAddr::from(([10, 0, i, 1], 6881));
        for (nodes, answered, routers_asked) in [
            (0, true, true),
            (2, true, true),
            (3, true, false),
            (3, false, true),
        ] {
            let (dht, mut sent) = fixture_dht();
            dht.routers.write().extend([router, router_v6]);
            add_table_nodes(&dht, nodes, answered);
            let req = std::sync::Arc::new(super::RecursiveRequest::get_peers(
                dht.clone(),
                Id20::new([0x22; 20]),
                Some(6881),
                tokio::sync::mpsc::unbounded_channel().0,
            ));
            let start = tokio::time::Instant::now();
            let task = req.request_peers_forever(true);
            let mut want: HashSet<SocketAddr> = (0..nodes).map(node).collect();
            if routers_asked {
                want.insert(router);
            }
            let mut asked = HashSet::new();
            while asked.len() < want.len() {
                asked.insert(sent.recv().await.unwrap().addr);
                assert_eq!(tokio::time::Instant::now(), start, "{nodes} nodes");
            }
            assert!(sent.try_recv().is_err(), "{nodes} nodes");
            assert_eq!(asked, want, "{nodes} nodes");
            assert_eq!(tokio::time::Instant::now(), start, "{nodes} nodes");
            task.abort();
        }
    }

    // `traversal_algorithm::start` runs for a refresh too: a bucket refresh,
    // a find_node from the table's nodes nearest its target as
    // `bucket_refresher` starts it, that finds fewer than three nodes that
    // have answered also asks the routers of its family; one that finds three
    // does not.
    #[tokio::test(start_paused = true)]
    async fn test_a_bucket_refresh_short_of_nodes_also_asks_the_routers() {
        use crate::core::hash_id::Id20;
        use std::collections::HashSet;

        let router: SocketAddr = ([192, 0, 2, 1], 6881).into();
        let router_v6: SocketAddr = "[2001:db8::1]:6881".parse().unwrap();
        let node = |i: u8| SocketAddr::from(([10, 0, i, 1], 6881));
        for (nodes, answered, routers_asked) in [
            (0, true, true),
            (2, true, true),
            (3, true, false),
            (3, false, true),
        ] {
            let (dht, mut sent) = fixture_dht();
            dht.routers.write().extend([router, router_v6]);
            add_table_nodes(&dht, nodes, answered);
            let mut refresh = std::pin::pin!(super::RecursiveRequest::find_node_for_routing_table(
                dht.clone(),
                Id20::new([0x22; 20]),
                true,
                std::iter::empty(),
            ));
            assert!(futures::poll!(refresh.as_mut()).is_pending());

            let mut want: HashSet<SocketAddr> = (0..nodes).map(node).collect();
            if routers_asked {
                want.insert(router);
            }
            let mut asked = HashSet::new();
            while let Ok(request) = sent.try_recv() {
                assert!(matches!(
                    request.message.kind,
                    MessageKind::FindNodeRequest(_)
                ));
                assert!(asked.insert(request.addr), "{} asked twice", request.addr);
            }
            assert_eq!(asked, want, "{nodes} nodes");
        }
    }

    // libtorrent routing_table.cpp:816-822, 1085-1099: a node a search is
    // told of takes a place in the table once it answers, never on being
    // asked; one that never answers never enters it.
    #[tokio::test(start_paused = true)]
    async fn test_a_node_enters_the_table_only_once_it_answers() {
        use crate::core::hash_id::Id20;
        use crate::dht::RESPONSE_TIMEOUT;
        use std::task::Poll;

        let (dht, mut sent) = fixture_dht();
        let answers = (
            Id20::new([0x21; 20]),
            SocketAddr::from(([10, 0, 1, 1], 6881)),
        );
        let silent = (
            Id20::new([0x23; 20]),
            SocketAddr::from(([10, 0, 2, 1], 6881)),
        );
        let in_table = |id: Id20| dht.routing_table_v4.read().iter().any(|n| n.id() == id);
        let target = Id20::new([0x22; 20]);
        let req = super::RecursiveRequest {
            info_hash: target,
            request: super::Request::FindNode(target),
            dht: dht.clone(),
            peer_tx: tokio::sync::mpsc::unbounded_channel().0,
            callbacks: super::RecursiveRequestCallbacksFindNodes {},
        };
        let mut refresh = std::pin::pin!(req.search(
            true,
            [(Some(answers.0), answers.1), (Some(silent.0), silent.1)].into_iter(),
        ));
        assert!(futures::poll!(refresh.as_mut()).is_pending());
        let mut asked = Vec::new();
        while let Ok(request) = sent.try_recv() {
            asked.push(request);
        }
        assert_eq!(asked.len(), 2);
        assert!(
            !in_table(answers.0) && !in_table(silent.0),
            "asked is not answered"
        );

        let request = asked.iter().find(|r| r.addr == answers.1).unwrap();
        dht.on_received_message(
            bprotocol::Message {
                transaction_id: request.message.transaction_id.clone(),
                version: None,
                ip: None,
                kind: MessageKind::Response(bprotocol::Response {
                    id: answers.0,
                    ..Default::default()
                }),
            },
            answers.1,
        )
        .unwrap();
        let outcome = loop {
            if let Poll::Ready(outcome) = futures::poll!(refresh.as_mut()) {
                break outcome;
            }
            assert!(in_table(answers.0) || sent.try_recv().is_err());
            tokio::time::advance(RESPONSE_TIMEOUT).await;
        };
        let _ = outcome;
        assert!(
            in_table(answers.0),
            "the node that answered is in the table"
        );
        assert!(!in_table(silent.0), "the silent node entered the table");
    }

    // A router name's addresses join the routers (libtorrent
    // `dht_tracker::add_router_node`), and the next search for our own id
    // asks every router of the family, the ones the name gave among them,
    // each once (`traversal_algorithm::start` and `add_router_entries`).
    #[tokio::test(start_paused = true)]
    async fn test_a_bootstrap_from_the_routers_asks_each_router_once() {
        use crate::core::net::{BoxFuture, Resolver};
        use std::sync::Arc;

        struct Names(Vec<SocketAddr>);

        impl Resolver for Names {
            fn resolve<'a>(
                &'a self,
                host: &'a str,
                _port: u16,
            ) -> BoxFuture<'a, std::io::Result<crate::core::net::Resolved>> {
                Box::pin(async move {
                    assert_eq!(host, "router.invalid");
                    Ok(crate::core::net::Resolved::Addresses(self.0.clone()))
                })
            }
        }

        // Known from earlier names: one this name gives again, one it does
        // not.
        let known: SocketAddr = ([192, 0, 2, 1], 6881).into();
        let other: SocketAddr = ([192, 0, 2, 3], 6881).into();
        let new: SocketAddr = ([192, 0, 2, 2], 6881).into();
        let (dht, mut sent) = fixture_dht();
        dht.routers.write().extend([known, other]);
        let socket = crate::test_support::loopback_udp().await;
        let worker = super::DhtWorker {
            socket,
            resolver: Arc::new(Names(vec![new, known])),
            dht: dht.clone(),
        };
        worker
            .resolve_router(
                "router.invalid:6881",
                &std::sync::atomic::AtomicBool::new(false),
            )
            .await;
        let mut refresh = std::pin::pin!(dht.clone().refresh_self(true));
        assert!(futures::poll!(refresh.as_mut()).is_pending());

        let mut asked = Vec::new();
        while let Ok(request) = sent.try_recv() {
            assert!(matches!(
                request.message.kind,
                MessageKind::FindNodeRequest(_)
            ));
            asked.push(request.addr);
        }
        asked.sort();
        assert_eq!(asked, vec![known, new, other]);
    }

    // A search due while the routing table is empty and no router is known
    // starts as the first node enters it, not at a later look at the table.
    #[tokio::test(start_paused = true)]
    async fn test_a_search_starts_as_the_first_node_enters_the_table() {
        use crate::core::hash_id::Id20;

        let (dht, mut sent) = fixture_dht();
        let req = std::sync::Arc::new(super::RecursiveRequest::get_peers(
            dht.clone(),
            Id20::new([0x22; 20]),
            Some(6881),
            tokio::sync::mpsc::unbounded_channel().0,
        ));
        let task = req.request_peers_forever(true);
        tokio::time::advance(Duration::from_millis(10_500)).await;
        assert!(sent.try_recv().is_err());

        let entered = tokio::time::Instant::now();
        dht.routing_table_v4
            .write()
            .add_node(Id20::new([1; 20]), SocketAddr::from(([10, 0, 1, 1], 6881)));
        let request = sent.recv().await.unwrap();
        assert!(matches!(
            request.message.kind,
            MessageKind::GetPeersRequest(_)
        ));
        assert_eq!(tokio::time::Instant::now(), entered);
        task.abort();
    }

    // libtorrent `find_data::done` / `announce_fun`: nothing is announced while
    // the search runs; at its end, the k closest nodes that answered with a
    // token are announced to, with their token.
    #[tokio::test(start_paused = true)]
    async fn test_a_search_announces_to_the_k_closest_token_holders_when_it_ends() {
        use crate::core::hash_id::Id20;
        use std::{collections::BTreeMap, net::IpAddr, task::Poll};

        let (dht, mut sent) = fixture_dht();
        let info_hash = Id20::new([0x22; 20]);
        // Node i is i + 1 from the info-hash, at 10.0.i.1.
        let node = |i: u8| {
            let mut id = [0x22; 20];
            id[19] ^= i + 1;
            (Id20::new(id), SocketAddr::from(([10, 0, i, 1], 6881)))
        };
        let index = |addr: SocketAddr| match addr.ip() {
            IpAddr::V4(a) => a.octets()[2],
            IpAddr::V6(_) => unreachable!(),
        };
        // Nodes 1 and 3 answer without a token.
        let token = |i: u8| (i != 1 && i != 3).then_some([i]);
        let req = super::RecursiveRequest::get_peers(
            dht.clone(),
            info_hash,
            Some(6881),
            tokio::sync::mpsc::unbounded_channel().0,
        );
        let mut search = std::pin::pin!(req.search(
            true,
            (0..14).map(|i| {
                let (id, addr) = node(i);
                (Some(id), addr)
            })
        ));

        let mut asked = Vec::new();
        loop {
            if let Poll::Ready(outcome) = futures::poll!(search.as_mut()) {
                outcome.unwrap();
                break;
            }
            // Only get_peers go out while it runs.
            while let Ok(request) = sent.try_recv() {
                let i = index(request.addr);
                asked.push(i);
                answer(
                    &dht,
                    &request,
                    node(i).0,
                    token(i).as_ref().map(|t| &t[..]),
                    &[],
                );
            }
        }
        asked.sort();
        // 10 and 11 answered with a token too, but eight token holders are
        // closer.
        assert_eq!(asked, (0..12).collect::<Vec<_>>());

        let mut announced = BTreeMap::new();
        while let Ok(request) = sent.try_recv() {
            let MessageKind::AnnouncePeer(a) = request.message.kind else {
                panic!("not an announce: {:?}", request.message.kind);
            };
            assert_eq!((a.info_hash, a.port), (info_hash, 6881));
            announced.insert(index(request.addr), a.token.as_ref().to_vec());
        }
        assert_eq!(
            announced,
            [0, 2, 4, 5, 6, 7, 8, 9]
                .into_iter()
                .map(|i| (i, vec![i]))
                .collect()
        );
    }

    // A remote node naming a local address, or anyone naming an address
    // nothing can be sent to, is not followed; a local node naming a local
    // address is (libtorrent `aux::is_global`, `aux::is_local`, and the rule
    // `ut_pex.cpp` applies to PEX peers).
    #[tokio::test(start_paused = true)]
    async fn test_a_search_asks_no_node_named_at_an_unreachable_or_foreign_local_address() {
        use crate::core::hash_id::Id20;
        use std::{
            collections::HashSet,
            net::{Ipv4Addr, SocketAddrV4},
            task::Poll,
        };

        let (dht, mut sent) = fixture_dht();
        let id = |n: u8| Id20::new([n; 20]);
        let v4 = |ip: [u8; 4], port: u16| SocketAddrV4::new(Ipv4Addr::from(ip), port);
        let public = SocketAddr::from(([5, 6, 7, 8], 6881));
        let local = SocketAddr::from(([10, 2, 0, 1], 6881));
        let named_by_public: Vec<_> = [
            v4([127, 0, 0, 1], 6881),
            v4([10, 1, 0, 1], 6881),
            v4([192, 168, 1, 1], 6881),
            v4([169, 254, 1, 1], 6881),
            v4([100, 64, 0, 1], 6881),
            v4([224, 0, 0, 1], 6881),
            v4([0, 1, 2, 3], 6881),
            v4([240, 0, 0, 1], 6881),
            v4([255, 255, 255, 255], 6881),
            v4([5, 6, 9, 9], 0),
            v4([5, 6, 10, 10], 6881),
        ]
        .into_iter()
        .zip(0x40..)
        .map(|(addr, n)| (id(n), addr))
        .collect();
        let named_by_local = [(id(0x60), v4([10, 3, 0, 1], 6881))];

        let req = get_peers_search(&dht);
        let mut search = std::pin::pin!(req.search(
            true,
            [(Some(id(0x30)), public), (Some(id(0x31)), local)].into_iter()
        ));
        let mut asked = HashSet::new();
        loop {
            if let Poll::Ready(outcome) = futures::poll!(search.as_mut()) {
                outcome.unwrap();
                break;
            }
            while let Ok(request) = sent.try_recv() {
                asked.insert(request.addr);
                let nodes: &[_] = match request.addr {
                    a if a == public => &named_by_public,
                    a if a == local => &named_by_local,
                    _ => &[],
                };
                answer(&dht, &request, id(0x50), None, nodes);
            }
        }

        assert_eq!(
            asked,
            HashSet::from([
                public,
                local,
                SocketAddr::from(([5, 6, 10, 10], 6881)),
                SocketAddr::from(([10, 3, 0, 1], 6881)),
            ])
        );
    }

    // A DHT whose shared socket closes under it, as a session's uTP socket
    // does when the session closes, ends quietly: no error is logged.
    #[tokio::test]
    async fn a_dht_whose_socket_closes_ends_without_an_error() {
        use crate::core::net::BoxFuture;

        // Sends go nowhere; a read waits until `closed` is told, then says
        // the socket has closed.
        struct Closing {
            closed: tokio::sync::Notify,
        }
        impl DatagramSocket for Closing {
            fn bind_addr(&self) -> SocketAddr {
                (Ipv4Addr::LOCALHOST, 6881).into()
            }
            fn recv_from<'a>(
                &'a self,
                _: &'a mut [u8],
            ) -> BoxFuture<'a, std::io::Result<(usize, SocketAddr)>> {
                Box::pin(async move {
                    self.closed.notified().await;
                    Err(std::io::Error::new(
                        std::io::ErrorKind::BrokenPipe,
                        "the uTP socket the DHT shares has closed",
                    ))
                })
            }
            fn send_to<'a>(
                &'a self,
                buf: &'a [u8],
                _: SocketAddr,
            ) -> BoxFuture<'a, std::io::Result<usize>> {
                Box::pin(async move { Ok(buf.len()) })
            }
            fn poll_send_to(
                &self,
                _: &mut std::task::Context<'_>,
                buf: &[u8],
                _: SocketAddr,
            ) -> std::task::Poll<std::io::Result<usize>> {
                std::task::Poll::Ready(Ok(buf.len()))
            }
        }

        #[derive(Clone, Default)]
        struct Lines(Arc<std::sync::Mutex<Vec<u8>>>);
        impl std::io::Write for Lines {
            fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
                self.0.lock().unwrap().extend_from_slice(buf);
                Ok(buf.len())
            }
            fn flush(&mut self) -> std::io::Result<()> {
                Ok(())
            }
        }
        let lines = Lines::default();
        let written = lines.clone();
        let _capture = tracing::subscriber::set_default(
            tracing_subscriber::fmt()
                .with_env_filter(tracing_subscriber::EnvFilter::new("weaver_torrent=error"))
                .with_ansi(false)
                .with_writer(move || written.clone())
                .finish(),
        );

        let socket = Arc::new(Closing {
            closed: tokio::sync::Notify::new(),
        });
        let tasks = tokio_util::task::TaskTracker::new();
        let dht = DhtState::with_config(DhtConfig {
            bootstrap_addrs: Some(Vec::new()),
            socket: Some(socket.clone()),
            tasks: Some(tasks.clone()),
            ..Default::default()
        })
        .await
        .unwrap();
        socket.closed.notify_one();
        tasks.close();
        tasks.wait().await;

        assert!(!dht.cancellation_token().is_cancelled());
        let logged = String::from_utf8(lines.0.lock().unwrap().clone()).unwrap();
        assert_eq!(logged, "");
    }

    // Reply to `request` as the node `id`, naming `nodes` and `values`.
    fn reply(
        dht: &std::sync::Arc<DhtState>,
        request: &super::WorkerSendRequest,
        id: crate::core::hash_id::Id20,
        nodes: &[(crate::core::hash_id::Id20, std::net::SocketAddrV4)],
        values: &[SocketAddr],
    ) {
        let nodes = (!nodes.is_empty()).then(|| {
            bprotocol::CompactNodeInfoOwned::new_from_iter(
                nodes.iter().map(|&(id, addr)| bprotocol::Node { id, addr }),
            )
        });
        let values = (!values.is_empty()).then(|| values.iter().map(|&v| v.into()).collect());
        dht.on_received_message(
            bprotocol::Message {
                transaction_id: request.message.transaction_id.clone(),
                version: None,
                ip: None,
                kind: MessageKind::Response(bprotocol::Response {
                    id,
                    nodes,
                    values,
                    ..Default::default()
                }),
            },
            request.addr,
        )
        .unwrap();
    }

    // Reply to `request` with a KRPC error.
    fn refuse(dht: &std::sync::Arc<DhtState>, request: &super::WorkerSendRequest) {
        dht.on_received_message(
            bprotocol::Message {
                transaction_id: request.message.transaction_id.clone(),
                version: None,
                ip: None,
                kind: MessageKind::Error(bprotocol::ErrorDescription {
                    code: 202,
                    description: ByteBufOwned::from(&b"Server Error"[..]),
                }),
            },
            request.addr,
        )
        .unwrap();
    }

    // On the same network as the nodes the tests have it name: a node named
    // at a local address is asked only when a local node names it.
    const ROUTER: ([u8; 4], u16) = ([10, 200, 0, 1], 6881);

    // A first bootstrap whose router answers while the nodes it names do not,
    // as on a route that carries a datagram now and then before it settles,
    // does not leave the table empty for good: the table is still shallow,
    // so the routers are asked again a backoff later, and once the nodes
    // answer they fill the table; the round after that starts from them.
    // libtorrent bootstraps again from `node::tick` while the table is under
    // 4 deep (node.cpp:647-662, v2.0.15).
    #[tokio::test(start_paused = true)]
    async fn test_a_bootstrap_that_got_no_answers_fills_the_table_once_the_path_comes_up() {
        use super::BOOTSTRAP_RETRY_MIN;
        use crate::core::hash_id::Id20;
        use crate::dht::RESPONSE_TIMEOUT;
        use std::collections::BTreeSet;

        let (dht, mut sent) = fixture_dht();
        let router = SocketAddr::from(ROUTER);
        dht.add_routers(&[router]);
        let named: Vec<(Id20, std::net::SocketAddrV4)> = (1..=4u8)
            .map(|i| {
                (
                    Id20::new([0x80 + i; 20]),
                    std::net::SocketAddrV4::new([10, 0, i, 1].into(), 6881),
                )
            })
            .collect();
        let named_addrs: BTreeSet<SocketAddr> = named.iter().map(|n| n.1.into()).collect();
        let start = tokio::time::Instant::now();
        let task = tokio::spawn(dht.clone().refresh_self(true));

        // Round one: the router answers, the nodes it names never do.
        let request = sent.recv().await.unwrap();
        assert_eq!(request.addr, router);
        assert!(matches!(
            request.message.kind,
            MessageKind::FindNodeRequest(_)
        ));
        reply(&dht, &request, Id20::new([0x99; 20]), &named, &[]);
        let mut asked = BTreeSet::new();
        for _ in 0..named.len() {
            asked.insert(sent.recv().await.unwrap().addr);
        }
        assert_eq!(asked, named_addrs);
        assert_eq!(dht.routing_table_v4.read().len(), 0);

        // Round two, once those time out and a backoff after: the router
        // again; now the nodes answer.
        let request = sent.recv().await.unwrap();
        assert_eq!(request.addr, router);
        assert_eq!(
            tokio::time::Instant::now() - start,
            RESPONSE_TIMEOUT + BOOTSTRAP_RETRY_MIN
        );
        reply(&dht, &request, Id20::new([0x99; 20]), &named, &[]);
        for _ in 0..named.len() {
            let request = sent.recv().await.unwrap();
            let &(id, _) = named
                .iter()
                .find(|n| SocketAddr::from(n.1) == request.addr)
                .unwrap();
            reply(&dht, &request, id, &[], &[]);
        }

        // Round three starts from the four nodes that answered, not the router.
        let mut asked = BTreeSet::new();
        for _ in 0..named.len() {
            asked.insert(sent.recv().await.unwrap().addr);
        }
        assert_eq!(asked, named_addrs);
        assert_eq!(dht.routing_table_v4.read().len(), 4);
        task.abort();
    }

    // A torrent's search asked for while the table is empty and no router is
    // known starts as soon as a router becomes known, as its name resolves.
    #[tokio::test(start_paused = true)]
    async fn test_a_search_starts_as_the_first_router_becomes_known() {
        use crate::core::hash_id::Id20;

        let (dht, mut sent) = fixture_dht();
        let req = std::sync::Arc::new(super::RecursiveRequest::get_peers(
            dht.clone(),
            Id20::new([0x22; 20]),
            Some(6881),
            tokio::sync::mpsc::unbounded_channel().0,
        ));
        let task = req.request_peers_forever(true);
        tokio::time::advance(Duration::from_millis(8_400)).await;
        assert!(sent.try_recv().is_err());

        let known = tokio::time::Instant::now();
        dht.add_routers(&[SocketAddr::from(ROUTER)]);
        let request = sent.recv().await.unwrap();
        assert!(matches!(
            request.message.kind,
            MessageKind::GetPeersRequest(_)
        ));
        assert_eq!(request.addr, SocketAddr::from(ROUTER));
        assert_eq!(tokio::time::Instant::now(), known);
        task.abort();
    }

    // Our id with bit `bit` flipped, and `n` in its last byte: a node of the
    // bucket `bit` buckets from the farthest.
    fn id_in_bucket(own: crate::core::hash_id::Id20, bit: u8, n: u8) -> crate::core::hash_id::Id20 {
        let mut id = own.0;
        id[usize::from(bit / 8)] ^= 0x80 >> (bit % 8);
        id[19] ^= n + 1;
        crate::core::hash_id::Id20::new(id)
    }

    // A table `buckets` buckets past the farthest, each a full bucket of
    // nodes that answered, each node in its own /24.
    fn fill_deep_table(
        dht: &DhtState,
        buckets: u8,
    ) -> Vec<(crate::core::hash_id::Id20, SocketAddr)> {
        let mut table = dht.routing_table_v4.write();
        let mut nodes = Vec::new();
        for bit in 0..=buckets {
            for n in 0..8u8 {
                let id = id_in_bucket(dht.id, bit, n);
                let addr = SocketAddr::from(([10, bit + 1, n + 1, 1], 6881));
                table.add_node(id, addr);
                table.mark_response(&id, std::time::Instant::now());
                nodes.push((id, addr));
            }
        }
        nodes
    }

    // libtorrent `routing_table::depth` (routing_table.cpp:368-391, v2.0.15):
    // past the farthest bucket, the buckets in a row with half a bucket of
    // nodes that answered. Nodes only heard of do not count.
    #[test]
    fn test_table_depth_counts_buckets_of_nodes_that_answered() {
        let (dht, _sent) = fixture_state();
        assert_eq!(
            dht.routing_table_v4.read().depth(std::time::Instant::now()),
            0
        );
        fill_deep_table(&dht, 5);
        assert_eq!(
            dht.routing_table_v4.read().depth(std::time::Instant::now()),
            5
        );

        let (dht, _sent) = fixture_state();
        {
            let mut table = dht.routing_table_v4.write();
            for bit in 0..=5 {
                for n in 0..8u8 {
                    table.add_node(
                        id_in_bucket(dht.id, bit, n),
                        SocketAddr::from(([10, bit + 1, n + 1, 1], 6881)),
                    );
                }
            }
        }
        assert_eq!(
            dht.routing_table_v4.read().depth(std::time::Instant::now()),
            0
        );
    }

    // A deep table searches for our own id once every SELF_REFRESH_INTERVAL
    // and not from the routers. When its nodes later stop answering, the
    // next round has no node to count on and asks the routers again
    // (node.cpp:647-662: a table under 4 deep bootstraps again).
    #[tokio::test(start_paused = true)]
    async fn test_a_table_that_empties_later_bootstraps_again() {
        use super::SELF_REFRESH_INTERVAL;
        use crate::dht::RESPONSE_TIMEOUT;

        let (dht, mut sent) = fixture_dht();
        let router = SocketAddr::from(ROUTER);
        dht.add_routers(&[router]);
        let nodes = fill_deep_table(&dht, 5);
        let deepened = dht.growth(true).grew.notified();
        let mut deepened = std::pin::pin!(deepened);
        deepened.as_mut().enable();
        let start = tokio::time::Instant::now();
        let task = tokio::spawn(dht.clone().refresh_self(true));

        // Round one: the eight nodes nearest our id, and they answer; the
        // round ends with the table deep.
        for _ in 0..8 {
            let request = sent.recv().await.unwrap();
            assert_ne!(request.addr, router);
            let &(id, _) = nodes.iter().find(|n| n.1 == request.addr).unwrap();
            reply(&dht, &request, id, &[], &[]);
        }
        deepened.await;

        // Every node stops answering.
        {
            let mut table = dht.routing_table_v4.write();
            for (id, _) in &nodes {
                table.mark_outgoing_request(id, std::time::Instant::now());
                table.mark_error(id);
                table.mark_error(id);
            }
        }

        // Round two, SELF_REFRESH_INTERVAL later, asks the router too.
        let first = sent.recv().await.unwrap();
        assert_eq!(tokio::time::Instant::now() - start, SELF_REFRESH_INTERVAL);
        let mut request = first;
        while request.addr != router {
            request = sent.recv().await.unwrap();
            assert!(
                tokio::time::Instant::now() - start <= SELF_REFRESH_INTERVAL + RESPONSE_TIMEOUT,
                "round two ended without asking the router"
            );
        }
        task.abort();
    }

    // An error reply is no answer (libtorrent rpc_manager.cpp:316-350): the
    // node that sent it counts as failed and does not enter the table.
    #[tokio::test(start_paused = true)]
    async fn test_a_node_that_replies_with_an_error_does_not_enter_the_table() {
        use crate::core::hash_id::Id20;

        let (dht, mut sent) = fixture_dht();
        let node = (
            Id20::new([0x21; 20]),
            SocketAddr::from(([10, 0, 1, 1], 6881)),
        );
        let target = dht.id;
        let req = super::RecursiveRequest {
            info_hash: target,
            request: super::Request::FindNode(target),
            dht: dht.clone(),
            peer_tx: tokio::sync::mpsc::unbounded_channel().0,
            callbacks: super::RecursiveRequestCallbacksFindNodes {},
        };
        let search =
            tokio::spawn(
                async move { req.search(true, [(Some(node.0), node.1)].into_iter()).await },
            );
        let request = sent.recv().await.unwrap();
        refuse(&dht, &request);
        let outcome = search.await.unwrap().unwrap();
        assert_eq!(
            outcome,
            super::SearchOutcome {
                answered: 0,
                failed: 1,
                nodes: 0
            }
        );
        assert_eq!(dht.routing_table_v4.read().len(), 0);
    }

    // A router that refuses the bootstrap does not end it: the table is still
    // empty, so the router is asked again a backoff later.
    #[tokio::test(start_paused = true)]
    async fn test_a_bootstrap_refused_by_its_router_is_made_again() {
        use super::BOOTSTRAP_RETRY_MIN;

        let (dht, mut sent) = fixture_dht();
        let router = SocketAddr::from(ROUTER);
        dht.add_routers(&[router]);
        let start = tokio::time::Instant::now();
        let task = tokio::spawn(dht.clone().refresh_self(true));
        let request = sent.recv().await.unwrap();
        refuse(&dht, &request);
        let request = sent.recv().await.unwrap();
        assert_eq!(request.addr, router);
        assert_eq!(tokio::time::Instant::now() - start, BOOTSTRAP_RETRY_MIN);
        task.abort();
    }

    // A find_node reply that also names peers, as some nodes send, is walked
    // past: the nodes it names are asked next, and the search goes on.
    #[tokio::test(start_paused = true)]
    async fn test_a_find_node_reply_naming_peers_does_not_end_the_search() {
        use crate::core::hash_id::Id20;

        let (dht, mut sent) = fixture_dht();
        let router = SocketAddr::from(ROUTER);
        dht.add_routers(&[router]);
        let task = tokio::spawn(dht.clone().refresh_self(true));
        let request = sent.recv().await.unwrap();
        assert_eq!(request.addr, router);
        let named = (
            Id20::new([0x81; 20]),
            std::net::SocketAddrV4::new([10, 0, 1, 1].into(), 6881),
        );
        reply(
            &dht,
            &request,
            Id20::new([0x99; 20]),
            &[named],
            &[SocketAddr::from(([10, 9, 9, 9], 51413))],
        );
        let request = sent.recv().await.unwrap();
        assert_eq!(request.addr, SocketAddr::from(named.1));
        task.abort();
    }

    // A route that carries nothing costs one router query a round: rounds
    // back off from BOOTSTRAP_RETRY_MIN, doubling to SELF_REFRESH_INTERVAL
    // apart, each after the last one's queries time out.
    #[tokio::test(start_paused = true)]
    async fn test_a_dead_route_backs_off_and_sends_a_bounded_number_of_datagrams() {
        let (dht, mut sent) = fixture_dht();
        let router = SocketAddr::from(ROUTER);
        dht.add_routers(&[router]);
        let start = tokio::time::Instant::now();
        let task = tokio::spawn(dht.clone().refresh_self(true));
        let hour = Duration::from_secs(3600);
        let mut at = Vec::new();
        loop {
            let request = sent.recv().await.unwrap();
            assert_eq!(request.addr, router);
            let elapsed = tokio::time::Instant::now() - start;
            if elapsed > hour {
                break;
            }
            at.push(elapsed.as_secs());
        }
        task.abort();
        assert_eq!(
            at,
            [0, 15, 35, 65, 115, 205, 375, 705, 1315, 1925, 2535, 3145]
        );
    }

    // A table restored from an earlier run holds nodes that have not answered
    // this run: they do not keep a search from the routers. libtorrent seeds
    // its bootstrap with its saved nodes and adds the routers
    // (dht_tracker.cpp:184-191, v2.0.15).
    #[tokio::test(start_paused = true)]
    async fn test_a_restored_table_of_stale_nodes_does_not_keep_the_routers_out() {
        use crate::core::hash_id::Id20;
        use crate::dht::routing_table::RoutingTable;

        let (saved, _sent) = fixture_state();
        add_table_nodes(&saved, 5, true);
        let json = serde_json::to_string(&*saved.routing_table_v4.read()).unwrap();
        let restored: RoutingTable = serde_json::from_str(&json).unwrap();
        assert_eq!(restored.len(), 5);

        let router = SocketAddr::from(ROUTER);
        for refresh in [true, false] {
            let (tx, mut sent) = tokio::sync::mpsc::unbounded_channel();
            let dht = std::sync::Arc::new(DhtState::new_internal(
                saved.id,
                tx,
                Some(restored.clone()),
                None,
                (Ipv4Addr::LOCALHOST, 0).into(),
                tokio_util::sync::CancellationToken::new(),
                None,
            ));
            dht.add_routers(&[router]);
            let task = if refresh {
                tokio::spawn(dht.clone().refresh_self(true)).abort_handle()
            } else {
                let req = std::sync::Arc::new(super::RecursiveRequest::get_peers(
                    dht.clone(),
                    Id20::new([0x22; 20]),
                    Some(6881),
                    tokio::sync::mpsc::unbounded_channel().0,
                ));
                req.request_peers_forever(true).abort_handle()
            };
            // The router is asked in the first search, after the nodes ahead
            // of it fail.
            let start = tokio::time::Instant::now();
            loop {
                let request = sent.recv().await.unwrap();
                if request.addr == router {
                    break;
                }
                assert!(
                    tokio::time::Instant::now() - start <= crate::dht::RESPONSE_TIMEOUT,
                    "the first search ended without asking the router"
                );
            }
            task.abort();
        }
    }

    // A route whose routers become known 3.1 s in, as their names resolve
    // through a tunnel: the torrent's first search asks the router and gets
    // only a timeout. The self-refresh that starts with it learns four nodes
    // from the router; two answer and enter the table, two never answer and
    // are asked again each round; the table stays under 4 deep. The torrent
    // searches again EARLY_SEARCH_MIN after its first search ended, from the
    // nodes that answered, long before its turn, and finds peers.
    #[tokio::test(start_paused = true)]
    async fn test_a_short_search_searches_again_once_nodes_that_answer_enter_the_table() {
        use super::{EARLY_SEARCH_MIN, SHALLOW_DEPTH};
        use crate::core::hash_id::Id20;
        use crate::dht::RESPONSE_TIMEOUT;
        use std::collections::BTreeMap;

        let (dht, mut sent) = fixture_dht();
        let router = SocketAddr::from(ROUTER);
        let named: Vec<(Id20, std::net::SocketAddrV4)> = (1..=4u8)
            .map(|i| {
                (
                    Id20::new([0x80 + i; 20]),
                    std::net::SocketAddrV4::new([10, 0, i, 1].into(), 6881),
                )
            })
            .collect();
        // Nodes 1 and 2 answer; 3 and 4 never do, nor does the router to a
        // get_peers.
        let answering = |addr: SocketAddr| {
            named[..2]
                .iter()
                .find(|n| SocketAddr::from(n.1) == addr)
                .map(|n| n.0)
        };
        let peer = SocketAddr::from(([10, 9, 9, 9], 51413));
        let info_hash = Id20::new([0x22; 20]);
        let slot = super::announce_slot(&info_hash);
        let (peer_tx, mut peers) = tokio::sync::mpsc::unbounded_channel();
        let req = Arc::new(super::RecursiveRequest::get_peers(
            dht.clone(),
            info_hash,
            None,
            peer_tx,
        ));
        let start = tokio::time::Instant::now();
        let refresh = tokio::spawn(dht.clone().refresh_self(true));
        let task = req.request_peers_forever(true);

        let known = Duration::from_millis(3_100);
        tokio::time::sleep(known).await;
        dht.add_routers(&[router]);
        // Both ask the router before anything answers.
        let mut first = Vec::new();
        while first.len() < 2 {
            let request = sent.recv().await.unwrap();
            assert_eq!(request.addr, router);
            first.push(request);
        }

        let mut searched = BTreeMap::<Duration, Vec<SocketAddr>>::new();
        let mut first = first.into_iter();
        let (found, found_at) = loop {
            let request = match first.next() {
                Some(request) => request,
                None => tokio::select! {
                    biased;
                    found = peers.recv() => {
                        break (found.unwrap(), tokio::time::Instant::now() - start);
                    }
                    request = sent.recv() => request.unwrap(),
                },
            };
            match request.message.kind {
                MessageKind::FindNodeRequest(_) if request.addr == router => {
                    reply(&dht, &request, Id20::new([0x99; 20]), &named, &[]);
                }
                MessageKind::FindNodeRequest(_) => {
                    if let Some(id) = answering(request.addr) {
                        reply(&dht, &request, id, &[], &[]);
                    }
                }
                MessageKind::GetPeersRequest(_) => {
                    let at = tokio::time::Instant::now() - start;
                    searched.entry(at).or_default().push(request.addr);
                    if let Some(id) = answering(request.addr) {
                        reply(&dht, &request, id, &[], &[peer]);
                    }
                }
                ref kind => panic!("unexpected {kind:?}"),
            }
        };
        task.abort();
        refresh.abort();

        let again = known + RESPONSE_TIMEOUT + EARLY_SEARCH_MIN;
        assert!(again < slot);
        assert_eq!(found, peer);
        assert_eq!(found_at, again);
        assert_eq!(
            searched.keys().copied().collect::<Vec<_>>(),
            vec![known, again]
        );
        assert_eq!(searched[&known], vec![router]);
        let table = dht.routing_table_v4.read();
        assert_eq!(table.len(), 2);
        assert!(table.depth(std::time::Instant::now()) < SHALLOW_DEPTH);
    }

    // A route that carries nothing: the torrent searches as it starts, at
    // its slot and an interval after each search, each time asking the
    // router once and waiting out its timeout; the table never grows, so it
    // never searches early. Its first hour costs five get_peers, and the
    // self-refresh beside it twelve find_node.
    #[tokio::test(start_paused = true)]
    async fn test_a_torrent_on_a_dead_route_searches_only_at_its_turns() {
        use crate::core::hash_id::Id20;
        use crate::dht::{DHT_ANNOUNCE_INTERVAL, RESPONSE_TIMEOUT};

        let (dht, mut sent) = fixture_dht();
        let router = SocketAddr::from(ROUTER);
        dht.add_routers(&[router]);
        let info_hash = Id20::new([0x22; 20]);
        let req = Arc::new(super::RecursiveRequest::get_peers(
            dht.clone(),
            info_hash,
            None,
            tokio::sync::mpsc::unbounded_channel().0,
        ));
        let start = tokio::time::Instant::now();
        let refresh = tokio::spawn(dht.clone().refresh_self(true));
        let task = req.request_peers_forever(true);
        let hour = Duration::from_secs(3600);
        let (mut searched, mut refreshed) = (Vec::new(), Vec::new());
        loop {
            let request = sent.recv().await.unwrap();
            let at = tokio::time::Instant::now() - start;
            if at > hour {
                break;
            }
            assert_eq!(request.addr, router);
            match request.message.kind {
                MessageKind::GetPeersRequest(_) => searched.push(at),
                MessageKind::FindNodeRequest(_) => refreshed.push(at.as_secs()),
                ref kind => panic!("unexpected {kind:?}"),
            }
        }
        task.abort();
        refresh.abort();

        // Each due an interval (the slot, first) after the last search ended.
        let slot = super::announce_slot(&info_hash);
        let want: Vec<Duration> = (0..5u32)
            .map(|n| match n {
                0 => Duration::ZERO,
                n => n * RESPONSE_TIMEOUT + slot + (n - 1) * DHT_ANNOUNCE_INTERVAL,
            })
            .collect();
        assert!(*want.last().unwrap() <= hour);
        assert_eq!(searched, want);
        assert_eq!(
            refreshed,
            [0, 15, 35, 65, 115, 205, 375, 705, 1315, 1925, 2535, 3145]
        );
    }

    // Twenty torrents whose searches all fell short at once: the first
    // growth of the table lets EARLY_SEARCHES_PER_GROWTH of them search
    // again, the others wait for the next growth, not for one of those
    // searches to end.
    #[tokio::test(start_paused = true)]
    async fn test_many_short_searches_search_again_a_batch_per_growth() {
        use super::{EARLY_SEARCH_MIN, EARLY_SEARCHES_PER_GROWTH};
        use crate::core::hash_id::Id20;
        use crate::dht::RESPONSE_TIMEOUT;
        use std::collections::BTreeMap;

        let (dht, mut sent) = fixture_dht();
        let router = SocketAddr::from(ROUTER);
        dht.add_routers(&[router]);
        let node = |i: u8| {
            (
                Id20::new([0x80 + i; 20]),
                std::net::SocketAddrV4::new([10, 0, i, 1].into(), 6881),
            )
        };
        let id_of = |addr: SocketAddr| {
            (1..=2u8)
                .map(node)
                .find(|n| SocketAddr::from(n.1) == addr)
                .map(|n| n.0)
        };
        // The router names node 1 from 15 s, node 2 too at 50 s: the table
        // grows at those two moments only.
        let first_growth = Duration::from_secs(15);
        let second_growth = Duration::from_secs(50);
        let start = tokio::time::Instant::now();
        let refresh = tokio::spawn(dht.clone().refresh_self(true));
        // Their slots are far past the end of the test.
        let torrents: Vec<_> = (0..20u8)
            .map(|i| {
                let info_hash = Id20::new([0x80 + i; 20]);
                assert!(super::announce_slot(&info_hash) > Duration::from_secs(300));
                Arc::new(super::RecursiveRequest::get_peers(
                    dht.clone(),
                    info_hash,
                    None,
                    tokio::sync::mpsc::unbounded_channel().0,
                ))
                .request_peers_forever(true)
            })
            .collect();

        // Searches started at each moment, each counted by its query to the
        // router, until the torrents that searched again first may do so
        // once more.
        let next_round = EARLY_SEARCH_MIN + 2 * EARLY_SEARCH_MIN;
        let mut searched = BTreeMap::<Duration, usize>::new();
        loop {
            let request = sent.recv().await.unwrap();
            let at = tokio::time::Instant::now() - start;
            if at >= next_round {
                break;
            }
            match request.message.kind {
                MessageKind::FindNodeRequest(_) if request.addr == router => {
                    let named: Vec<_> = match at {
                        at if at < first_growth => continue,
                        at if at == second_growth => vec![node(1), node(2)],
                        _ => vec![node(1)],
                    };
                    reply(&dht, &request, Id20::new([0x99; 20]), &named, &[]);
                }
                MessageKind::GetPeersRequest(_) if request.addr == router => {
                    *searched.entry(at).or_default() += 1;
                    reply(&dht, &request, Id20::new([0x99; 20]), &[], &[]);
                }
                MessageKind::FindNodeRequest(_) | MessageKind::GetPeersRequest(_) => {
                    reply(&dht, &request, id_of(request.addr).unwrap(), &[], &[]);
                }
                ref kind => panic!("unexpected {kind:?}"),
            }
        }
        for task in torrents {
            task.abort();
        }
        refresh.abort();

        // The first round of the refresh times out; the second, at 15 s,
        // grows the table.
        assert_eq!(first_growth, RESPONSE_TIMEOUT + super::BOOTSTRAP_RETRY_MIN);
        assert_eq!(
            searched,
            BTreeMap::from([
                (Duration::ZERO, 20),
                (EARLY_SEARCH_MIN, EARLY_SEARCHES_PER_GROWTH),
                (second_growth, 20 - EARLY_SEARCHES_PER_GROWTH),
            ])
        );
    }

    // A self-refresh round that walks for a minute, as one over a tunnel
    // does: the router names two nodes near our id that answer at once and
    // forty farther ones that never answer. The torrent's first search,
    // asked only of the router, times out. The two nodes enter the table in
    // the round's first moment, and the torrent searches again
    // EARLY_SEARCH_MIN after its first search ended, from them, while the
    // round is still asking the silent nodes.
    #[tokio::test(start_paused = true)]
    async fn test_a_short_search_searches_again_before_the_round_that_grew_the_table_ends() {
        use super::EARLY_SEARCH_MIN;
        use crate::core::hash_id::Id20;
        use crate::dht::RESPONSE_TIMEOUT;
        use std::collections::BTreeMap;

        let (dht, mut sent) = fixture_dht();
        let router = SocketAddr::from(ROUTER);
        dht.add_routers(&[router]);
        let answering: Vec<(Id20, std::net::SocketAddrV4)> = (1..=2u8)
            .map(|i| {
                let mut id = dht.id.0;
                id[19] ^= i;
                (
                    Id20::new(id),
                    std::net::SocketAddrV4::new([10, 0, i, 1].into(), 6881),
                )
            })
            .collect();
        let silent: Vec<(Id20, std::net::SocketAddrV4)> = (1..=40u8)
            .map(|i| {
                let mut id = [0xc0; 20];
                id[19] = i;
                (
                    Id20::new(id),
                    std::net::SocketAddrV4::new([10, 1, i, 1].into(), 6881),
                )
            })
            .collect();
        let named: Vec<_> = answering.iter().chain(&silent).copied().collect();
        let id_of = |addr: SocketAddr| {
            answering
                .iter()
                .find(|n| SocketAddr::from(n.1) == addr)
                .map(|n| n.0)
        };
        let peer = SocketAddr::from(([10, 9, 9, 9], 51413));
        let info_hash = Id20::new([0x22; 20]);
        let (peer_tx, mut peers) = tokio::sync::mpsc::unbounded_channel();
        let req = Arc::new(super::RecursiveRequest::get_peers(
            dht.clone(),
            info_hash,
            None,
            peer_tx,
        ));
        let start = tokio::time::Instant::now();
        let refresh = tokio::spawn(dht.clone().refresh_self(true));
        let task = req.request_peers_forever(true);

        // Nothing answers until both have asked the router: the torrent's
        // search has started before the table grows.
        let mut first = Vec::new();
        while first.len() < 2 {
            let request = sent.recv().await.unwrap();
            assert_eq!(request.addr, router);
            first.push(request);
        }
        let mut first = first.into_iter();
        let mut searched = BTreeMap::<Duration, usize>::new();
        let mut silent_asked = 0;
        let (found, found_at) = loop {
            let request = match first.next() {
                Some(request) => request,
                None => tokio::select! {
                    biased;
                    found = peers.recv() => {
                        break (found.unwrap(), tokio::time::Instant::now() - start);
                    }
                    request = sent.recv() => request.unwrap(),
                },
            };
            match request.message.kind {
                MessageKind::FindNodeRequest(_) if request.addr == router => {
                    reply(&dht, &request, Id20::new([0x99; 20]), &named, &[]);
                }
                MessageKind::FindNodeRequest(_) => match id_of(request.addr) {
                    Some(id) => reply(&dht, &request, id, &[], &[]),
                    None => silent_asked += 1,
                },
                MessageKind::GetPeersRequest(_) => {
                    if request.addr == router {
                        *searched
                            .entry(tokio::time::Instant::now() - start)
                            .or_default() += 1;
                    } else {
                        let id = id_of(request.addr).unwrap();
                        reply(&dht, &request, id, &[], &[peer]);
                    }
                }
                ref kind => panic!("unexpected {kind:?}"),
            }
        };
        task.abort();
        refresh.abort();

        let again = RESPONSE_TIMEOUT + EARLY_SEARCH_MIN;
        assert_eq!(found, peer);
        assert_eq!(found_at, again);
        assert_eq!(searched, BTreeMap::from([(Duration::ZERO, 1), (again, 1)]));
        // The round has silent nodes left to ask: it has not ended.
        assert!(silent_asked < silent.len(), "{silent_asked}");
        assert_eq!(dht.routing_table_v4.read().len(), answering.len());
    }

    // Many nodes entering at once are one growth, and the nodes that enter
    // in the GROWTH_SPACING after it one more as it ends: the early turns
    // are topped up to EARLY_SEARCHES_PER_GROWTH each time, never added up.
    // A table nothing enters does not grow.
    #[tokio::test(start_paused = true)]
    async fn test_nodes_entering_grow_the_table_at_most_once_a_spacing() {
        use super::{EARLY_SEARCHES_PER_GROWTH, GROWTH_SPACING};
        use std::sync::atomic::Ordering;

        let (dht, _sent) = fixture_dht();
        let growth = dht.growth(true);
        let mut watch = std::pin::pin!(dht.watch_growth(true));
        assert!(futures::poll!(watch.as_mut()).is_pending());
        let start = tokio::time::Instant::now();

        // Waits for the next growth while the watch runs; when it came.
        macro_rules! next_growth {
            () => {{
                let grew = growth.grew.notified();
                let mut grew = std::pin::pin!(grew);
                grew.as_mut().enable();
                tokio::select! {
                    never = watch.as_mut() => match never {},
                    () = grew => tokio::time::Instant::now() - start,
                }
            }};
        }
        macro_rules! run_until {
            ($at:expr) => {
                tokio::select! {
                    never = watch.as_mut() => match never {},
                    () = tokio::time::sleep_until(start + $at) => {}
                }
            };
        }
        let enter = |bit: u8, n: u8| {
            let id = id_in_bucket(dht.id, bit, n);
            let mut table = dht.routing_table_v4.write();
            table.add_node(id, SocketAddr::from(([10, bit + 1, n + 1, 1], 6881)));
            table.mark_response(&id, std::time::Instant::now());
        };

        // Forty-eight nodes enter at once.
        let at = {
            let grew = growth.grew.notified();
            let mut grew = std::pin::pin!(grew);
            grew.as_mut().enable();
            fill_deep_table(&dht, 5);
            tokio::select! {
                never = watch.as_mut() => match never {},
                () = grew => tokio::time::Instant::now() - start,
            }
        };
        assert_eq!(dht.routing_table_v4.read().entries(), 48);
        assert_eq!(at, Duration::ZERO);
        assert_eq!(growth.generation.load(Ordering::Acquire), 1);
        assert_eq!(growth.turns.available_permits(), EARLY_SEARCHES_PER_GROWTH);

        // More enter one and two seconds later: one growth between them,
        // as the spacing ends.
        for (second, bit) in [(1, 6), (2, 7)] {
            run_until!(Duration::from_secs(second));
            for n in 0..4 {
                enter(bit, n);
            }
        }
        assert_eq!(dht.routing_table_v4.read().entries(), 56);
        assert_eq!(growth.generation.load(Ordering::Acquire), 1);
        assert_eq!(next_growth!(), GROWTH_SPACING);
        assert_eq!(growth.generation.load(Ordering::Acquire), 2);
        assert_eq!(growth.turns.available_permits(), EARLY_SEARCHES_PER_GROWTH);

        // Nothing enters for a minute: no growth.
        run_until!(Duration::from_secs(60));
        assert_eq!(growth.generation.load(Ordering::Acquire), 2);

        // The next node to enter is a growth at once.
        enter(8, 0);
        assert_eq!(next_growth!(), Duration::from_secs(60));
        assert_eq!(growth.generation.load(Ordering::Acquire), 3);
    }

    // A short search whose table grows before every early search still
    // waits EARLY_SEARCH_MIN after its first, doubling, and searches at its
    // turn: the table's growth never brings it sooner.
    #[tokio::test(start_paused = true)]
    async fn test_a_short_search_backs_off_however_often_the_table_grows() {
        use super::EARLY_SEARCH_MIN;
        use crate::core::hash_id::Id20;
        use crate::dht::RESPONSE_TIMEOUT;
        use std::sync::atomic::Ordering;

        let (dht, mut sent) = fixture_dht();
        dht.add_routers(&[SocketAddr::from(ROUTER)]);
        let info_hash = Id20::new([0xf0; 20]);
        let slot = super::announce_slot(&info_hash);
        let req = Arc::new(super::RecursiveRequest::get_peers(
            dht.clone(),
            info_hash,
            None,
            tokio::sync::mpsc::unbounded_channel().0,
        ));
        let mut watch = std::pin::pin!(dht.watch_growth(true));
        assert!(futures::poll!(watch.as_mut()).is_pending());
        let start = tokio::time::Instant::now();
        let task = req.request_peers_forever(true);

        // Nothing answers, so each search lasts RESPONSE_TIMEOUT; each asks
        // at most five, all as it starts. As each starts, a node that
        // answered enters the table.
        let due = RESPONSE_TIMEOUT + slot;
        let mut searched = Vec::new();
        loop {
            let request = tokio::select! {
                never = watch.as_mut() => match never {},
                request = sent.recv() => request.unwrap(),
            };
            assert!(matches!(
                request.message.kind,
                MessageKind::GetPeersRequest(_)
            ));
            let at = tokio::time::Instant::now() - start;
            if searched.last() == Some(&at) {
                continue;
            }
            searched.push(at);
            if at >= due {
                break;
            }
            let n = u8::try_from(searched.len()).unwrap();
            let id = id_in_bucket(dht.id, n, 0);
            let mut table = dht.routing_table_v4.write();
            table.add_node(id, SocketAddr::from(([10, n, 1, 1], 6881)));
            table.mark_response(&id, std::time::Instant::now());
        }
        task.abort();

        let mut want = vec![Duration::ZERO];
        let (mut end, mut early) = (RESPONSE_TIMEOUT, EARLY_SEARCH_MIN);
        while end + early < due {
            want.push(end + early);
            end += early + RESPONSE_TIMEOUT;
            early *= 2;
        }
        want.push(due);
        // Four early searches: 30, 60, 120 and 240 s after the one before.
        assert_eq!(want.len(), 6);
        assert_eq!(searched, want);
        assert!(dht.growth(true).generation.load(Ordering::Acquire) >= 5);
    }

    // A search that reached k nodes is not made again ahead of its turn,
    // however the table grows after it.
    #[tokio::test(start_paused = true)]
    async fn test_a_search_that_was_not_short_waits_for_its_turn() {
        use crate::core::hash_id::Id20;
        use std::collections::BTreeMap;

        let (dht, mut sent) = fixture_dht();
        add_table_nodes(&dht, 8, true);
        let info_hash = Id20::new([0x22; 20]);
        let slot = super::announce_slot(&info_hash);
        let req = Arc::new(super::RecursiveRequest::get_peers(
            dht.clone(),
            info_hash,
            None,
            tokio::sync::mpsc::unbounded_channel().0,
        ));
        let start = tokio::time::Instant::now();
        let task = req.request_peers_forever(true);
        let mut searched = BTreeMap::<Duration, usize>::new();
        let mut grown = false;
        loop {
            let request = tokio::select! {
                request = sent.recv() => request.unwrap(),
                () = tokio::time::sleep_until(start + Duration::from_secs(40)), if !grown => {
                    grown = true;
                    dht.grow(true);
                    continue;
                }
            };
            let at = tokio::time::Instant::now() - start;
            *searched.entry(at).or_default() += 1;
            if at >= slot {
                break;
            }
            answer(&dht, &request, Id20::new([0x33; 20]), None, &[]);
        }
        task.abort();
        assert!(grown);
        assert_eq!(searched, BTreeMap::from([(Duration::ZERO, 8), (slot, 1)]));
    }

    // A search is short while fewer than k nodes have answered it: the
    // routers that answered it do not count.
    #[tokio::test(start_paused = true)]
    async fn test_routers_that_answer_do_not_make_a_search_complete() {
        use crate::core::hash_id::Id20;

        let (dht, mut sent) = fixture_dht();
        let router = SocketAddr::from(ROUTER);
        dht.add_routers(&[router]);
        let given = (
            Id20::new([0x21; 20]),
            SocketAddr::from(([10, 0, 1, 1], 6881)),
        );
        let named: Vec<(Id20, std::net::SocketAddrV4)> = (2..8u8)
            .map(|i| {
                (
                    Id20::new([0x20 + i; 20]),
                    std::net::SocketAddrV4::new([10, 0, i, 1].into(), 6881),
                )
            })
            .collect();
        let req = get_peers_search(&dht);
        let search = tokio::spawn(async move {
            req.search(true, [(Some(given.0), given.1)].into_iter())
                .await
        });
        for _ in 0..2 + named.len() {
            let request = sent.recv().await.unwrap();
            if request.addr == router {
                reply(&dht, &request, Id20::new([0x99; 20]), &named, &[]);
            } else if request.addr == given.1 {
                reply(&dht, &request, given.0, &[], &[]);
            } else {
                let &(id, _) = named
                    .iter()
                    .find(|n| SocketAddr::from(n.1) == request.addr)
                    .unwrap();
                reply(&dht, &request, id, &[], &[]);
            }
        }
        let outcome = search.await.unwrap().unwrap();
        assert_eq!(
            outcome,
            super::SearchOutcome {
                answered: 8,
                failed: 0,
                nodes: 7
            }
        );
        assert!(outcome.is_short());
    }

    // A node that answers and takes the place of a bad one leaves the table
    // as many nodes as before.
    #[test]
    fn test_a_node_taking_a_bad_ones_place_leaves_the_count_as_it_was() {
        use crate::core::hash_id::Id20;
        use crate::dht::routing_table::InsertResult;

        let (dht, _sent) = fixture_state();
        let mut table = dht.routing_table_v4.write();
        let bad = Id20::new([0x21; 20]);
        table.add_node(bad, SocketAddr::from(([10, 0, 1, 1], 6881)));
        table.mark_outgoing_request(&bad, std::time::Instant::now());
        table.mark_error(&bad);
        table.mark_error(&bad);
        assert_eq!(table.len(), 1);
        let new = Id20::new([0x23; 20]);
        assert!(matches!(
            table.add_node(new, SocketAddr::from(([10, 0, 2, 1], 6881))),
            InsertResult::ReplacedBad(_)
        ));
        assert_eq!(table.iter().count(), 1);
        assert_eq!(table.len(), 1);
    }

    // A datagram too long for the buffer, which a socket carried through a
    // tunnel reports as InvalidData, costs that datagram only: the DHT reads
    // on and answers the next query.
    #[tokio::test]
    async fn test_a_datagram_that_does_not_fit_costs_only_that_datagram() {
        use crate::core::net::BoxFuture;

        type Incoming = std::io::Result<(Vec<u8>, SocketAddr)>;
        struct Queued {
            incoming: tokio::sync::Mutex<tokio::sync::mpsc::UnboundedReceiver<Incoming>>,
            sent: tokio::sync::mpsc::UnboundedSender<Vec<u8>>,
        }
        impl DatagramSocket for Queued {
            fn bind_addr(&self) -> SocketAddr {
                (Ipv4Addr::LOCALHOST, 6881).into()
            }
            fn recv_from<'a>(
                &'a self,
                buf: &'a mut [u8],
            ) -> BoxFuture<'a, std::io::Result<(usize, SocketAddr)>> {
                Box::pin(async move {
                    match self.incoming.lock().await.recv().await {
                        Some(Ok((datagram, from))) => {
                            buf[..datagram.len()].copy_from_slice(&datagram);
                            Ok((datagram.len(), from))
                        }
                        Some(Err(e)) => Err(e),
                        None => std::future::pending().await,
                    }
                })
            }
            fn send_to<'a>(
                &'a self,
                buf: &'a [u8],
                _: SocketAddr,
            ) -> BoxFuture<'a, std::io::Result<usize>> {
                Box::pin(async move {
                    let _ = self.sent.send(buf.to_vec());
                    Ok(buf.len())
                })
            }
            fn poll_send_to(
                &self,
                _: &mut std::task::Context<'_>,
                buf: &[u8],
                _: SocketAddr,
            ) -> std::task::Poll<std::io::Result<usize>> {
                let _ = self.sent.send(buf.to_vec());
                std::task::Poll::Ready(Ok(buf.len()))
            }
        }

        let (incoming, incoming_rx) = tokio::sync::mpsc::unbounded_channel();
        let (sent_tx, mut sent) = tokio::sync::mpsc::unbounded_channel();
        let socket = Arc::new(Queued {
            incoming: tokio::sync::Mutex::new(incoming_rx),
            sent: sent_tx,
        });
        let tasks = tokio_util::task::TaskTracker::new();
        let dht = DhtState::with_config(DhtConfig {
            bootstrap_addrs: Some(Vec::new()),
            socket: Some(socket),
            tasks: Some(tasks.clone()),
            ..Default::default()
        })
        .await
        .unwrap();
        tasks.close();

        let from = SocketAddr::from(([10, 0, 0, 1], 6881));
        incoming
            .send(Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "the datagram did not fit",
            )))
            .unwrap();
        incoming
            .send(Ok((
                b"d1:ad2:id20:abcdefghij0123456789e1:q4:ping1:t2:aa1:y1:qe".to_vec(),
                from,
            )))
            .unwrap();
        let answer = tokio::select! {
            answer = sent.recv() => answer.unwrap(),
            () = tasks.wait() => panic!("the DHT ended"),
        };
        let answer = bprotocol::deserialize_message::<ByteBufOwned>(&answer).unwrap();
        assert!(matches!(answer.kind, MessageKind::Response(_)));
        assert_eq!(answer.transaction_id.as_ref(), b"aa");
        dht.cancellation_token().cancel();
    }
}
