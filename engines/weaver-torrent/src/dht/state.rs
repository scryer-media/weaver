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
        atomic::{AtomicBool, AtomicU16, AtomicU64, AtomicUsize, Ordering},
    },
    task::Poll,
    time::{Duration, Instant},
};

use crate::bencode::ByteBufOwned;
use crate::dht::{
    DHT_ANNOUNCE_INTERVAL, Error, RESPONSE_TIMEOUT, SEARCH_MIN_SEEDS,
    bprotocol::{
        self, AnnouncePeer, CompactNodeInfo, CompactNodeInfoOwned, ErrorDescription,
        FindNodeRequest, GetPeersRequest, Message, MessageKind, Node, PingRequest, Response, Want,
    },
    peer_store::{PEER_PURGE_INTERVAL, PeerStore},
    query_limiter::QueryLimiter,
    routing_table::{
        InsertResult, Marked, NodeStatus, Refresh, RoutingTable, RoutingTableNode, is_v4_mapped,
    },
    send_quota::{DHT_UPLOAD_RATE_LIMIT, SendQuota},
    send_windows::SendWindows,
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

// The requests one search keeps in flight, all of them counted wherever
// they are in its list: libtorrent's `dht_search_branching`.
const SEARCH_BRANCHING: usize = 5;
// How long a search's query may go unanswered, from the moment it is sent,
// before the search asks one more node in its place; the query still waits
// for its answer until RESPONSE_TIMEOUT (libtorrent `rpc_manager::tick`,
// rpc_manager.cpp:396-459, v2.0.15).
const SHORT_TIMEOUT: Duration = Duration::from_secs(1);
// The most a search's branching may grow to with queries past SHORT_TIMEOUT:
// libtorrent keeps its branch factor in an int8 (traversal_algorithm.cpp:380).
const SEARCH_BRANCHING_MAX: usize = i8::MAX as usize;
// The candidates one search keeps, closest first; libtorrent's traversal keeps
// 100. A candidate pushed past the end is dropped, and since the kept ones only
// get closer, it never comes back.
const SEARCH_CANDIDATES: usize = 100;
// k: a search is complete once the k closest candidates have answered.
const SEARCH_K: usize = 8;
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
// search; the rest wait for the next growth. This paces the searches made
// again ahead of a torrent's turn, so a table that grows node by node does
// not set every short torrent searching at each growth; it does not decide
// how many searches walk at once, which the query budget does (see
// `DhtState::admit_walk`).
const EARLY_SEARCHES_PER_GROWTH: usize = 16;
// The least time between two growths that nodes entering the table make:
// the first node to enter makes one at once, and the nodes that enter in
// the next GROWTH_SPACING make one between them as it ends. libtorrent
// looks at its table on a 5 s tick (dht_tracker.cpp:268-283, v2.0.15), as
// BOOTSTRAP_RETRY_MIN does here.
const GROWTH_SPACING: Duration = BOOTSTRAP_RETRY_MIN;
// The time between two refresh queries to the nodes of a table and the nodes
// heard of: one per 5 s tick, as libtorrent's `node::tick` sends one refresh
// per tick
// (node.cpp:664-673, dht_tracker.cpp:268-283, v2.0.15).
const REFRESH_SPACING: Duration = BOOTSTRAP_RETRY_MIN;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CandidateState {
    Fresh,
    // Not asked yet: its address's send window was full (see `SendGuard`).
    Deferred,
    InFlight,
    Answered,
    Failed,
}

// A search's hold on one of its queries (see `RecursiveRequest::asking`):
// dropped, the query is withdrawn.
type Hold = tokio::sync::oneshot::Receiver<()>;

#[derive(Debug)]
struct Candidate {
    // None for a bootstrap address, whose id we do not know yet.
    id: Option<Id20>,
    addr: SocketAddr,
    state: CandidateState,
    // The write token it answered with, if any.
    token: Option<ByteBufOwned>,
    // A deferred candidate's place in its address's line, while the search
    // may still ask it.
    lined_up: Option<LinedUp>,
    // When it was first deferred.
    deferred_at: Option<tokio::time::Instant>,
    // Which of the search's queries went to it, counting from 0: its answer
    // is the one to that query, never one to an earlier candidate at the
    // same address.
    asked: Option<usize>,
    // The search's hold on its query, while it is in flight.
    hold: Option<Hold>,
    // In flight past SHORT_TIMEOUT: the search's branching counts one more
    // for it until it answers or fails.
    late: bool,
}

// The part of an address a search keeps one node of: its /24, or for IPv6
// its first 64 bits (libtorrent `traversal_algorithm::add_entry`,
// traversal_algorithm.cpp:191-226, v2.0.15, with `dht_restrict_search_ips`,
// its default, settings_pack.cpp:230).
fn search_prefix(ip: IpAddr) -> IpAddr {
    match ip {
        IpAddr::V4(a) => {
            let [a, b, c, _] = a.octets();
            IpAddr::V4(std::net::Ipv4Addr::new(a, b, c, 0))
        }
        IpAddr::V6(a) => IpAddr::V6(Ipv6Addr::from(u128::from(a) >> 64 << 64)),
    }
}

// The nodes one search knows of, closest to its target first, the ones without
// an id last. Each is asked at most once.
struct Candidates {
    target: Id20,
    list: Vec<Candidate>,
    // Fewer than SEARCH_MIN_SEEDS of the nodes it started from were nodes we
    // can count on, so it asks the routers too (see `RecursiveRequest::start`).
    leans_on_routers: bool,
    // The queries the search has sent: each takes the next number.
    queries: usize,
    // The /24s and /64s of the nodes offered by an answer (see `offer`).
    prefixes: std::collections::HashSet<IpAddr>,
}

impl Candidates {
    fn new(target: Id20) -> Self {
        Self {
            target,
            list: Vec::new(),
            leans_on_routers: false,
            queries: 0,
            prefixes: std::collections::HashSet::new(),
        }
    }

    fn rank(&self, id: Option<Id20>) -> (bool, Option<Id20>) {
        (id.is_none(), id.map(|id| id.distance(&self.target)))
    }

    // An IPv4 address written as IPv6 is never asked: it is no node. A node
    // an answer names (not `initial`) is not offered when one offered before
    // it was in its /24 or /64: an answer that names many nodes of one
    // network, as a node can that wants us to send there, adds one of them
    // (libtorrent `add_entry`, traversal_algorithm.cpp:191-226, v2.0.15,
    // which applies this to every node but the ones a search starts from,
    // `flag_initial`). Returns the candidate pushed past the end, if any.
    fn offer(&mut self, id: Option<Id20>, addr: SocketAddr, initial: bool) -> Option<Candidate> {
        if is_v4_mapped(addr)
            || self
                .list
                .iter()
                .any(|c| c.addr == addr || (id.is_some() && c.id == id))
        {
            return None;
        }
        if !initial && !self.prefixes.insert(search_prefix(addr.ip())) {
            return None;
        }
        let rank = self.rank(id);
        let pos = self.list.partition_point(|c| self.rank(c.id) <= rank);
        if pos >= SEARCH_CANDIDATES {
            return None;
        }
        self.list.insert(
            pos,
            Candidate {
                id,
                addr,
                state: CandidateState::Fresh,
                token: None,
                lined_up: None,
                deferred_at: None,
                asked: None,
                hold: None,
                late: false,
            },
        );
        if self.list.len() > SEARCH_CANDIDATES {
            self.list.pop()
        } else {
            None
        }
    }

    // Asks, through `ask`, the candidates the search wants in flight at
    // `now`, and says whether the search is done and when it should look
    // again for a deferred candidate (libtorrent
    // `traversal_algorithm::add_requests`, traversal_algorithm.cpp:492-580,
    // v2.0.15, as with `dht_aggressive_lookups` off: its loop runs while
    // `m_invoke_count`, every request of the search in flight, is under the
    // branch factor, 519-523). Closest first, while fewer than k have
    // answered and fewer than `branching` of all its requests are in
    // flight, each not asked yet is asked.
    // So however many closer nodes an answer names, the search has at most
    // `branching` in flight, SEARCH_BRANCHING and one more for each query
    // past SHORT_TIMEOUT. One whose address's send window is full (see
    // `SendGuard`) is deferred instead, holding no request in flight, and
    // asked once its line says it may have room, if the search still wants
    // it then; libtorrent fails a query it cannot send. One still deferred
    // RESPONSE_TIMEOUT after it was first fails for this search, never for
    // the table: an address that keeps its window full holds no search
    // open. A deferred candidate past the k closest that answered is never
    // asked: it leaves its line. The search is done once the k closest that
    // answered have none in flight among or nearer than them, or once it
    // has none in flight at all; in both cases with no deferred candidate
    // nearer still to ask. Requests in flight to nodes farther than those k
    // are not waited for.
    fn add_requests(
        &mut self,
        guard: &Arc<SendGuard>,
        branching: usize,
        now: tokio::time::Instant,
        mut ask: impl FnMut(&Candidate, SendSlot) -> Hold,
    ) -> (bool, Option<tokio::time::Instant>) {
        let mut results_target = SEARCH_K;
        // In flight among the candidates walked so far.
        let mut outstanding = 0;
        let mut in_flight = self
            .list
            .iter()
            .filter(|c| c.state == CandidateState::InFlight)
            .count();
        let mut queries = self.queries;
        let mut wake: Option<tokio::time::Instant> = None;
        let mut wake_at = |at: tokio::time::Instant| {
            wake = Some(wake.map_or(at, |w| w.min(at)));
        };
        let mut walked = 0;
        for c in self.list.iter_mut() {
            if results_target == 0 || in_flight >= branching {
                break;
            }
            walked += 1;
            match c.state {
                CandidateState::Answered => results_target -= 1,
                CandidateState::InFlight => outstanding += 1,
                CandidateState::Failed => {}
                CandidateState::Fresh | CandidateState::Deferred => {
                    // Asked at its turn in line, or failed if that turn
                    // comes RESPONSE_TIMEOUT or more after it was deferred.
                    let ready = c.lined_up.as_ref().map(|l| l.ready).filter(|r| *r > now);
                    let slot = match ready {
                        Some(ready) => Err(ready),
                        None => match c.lined_up.take() {
                            Some(lined_up) => lined_up.try_send(now),
                            None => guard.try_send(c.addr.ip(), now),
                        }
                        .map_err(|lined_up| {
                            let ready = lined_up.ready;
                            c.lined_up = Some(lined_up);
                            ready
                        }),
                    };
                    match slot {
                        Ok(slot) => {
                            c.state = CandidateState::InFlight;
                            c.asked = Some(queries);
                            queries += 1;
                            outstanding += 1;
                            in_flight += 1;
                            c.hold = Some(ask(c, slot));
                        }
                        Err(ready) => {
                            let give_up = *c.deferred_at.get_or_insert(now) + RESPONSE_TIMEOUT;
                            if give_up <= now {
                                c.state = CandidateState::Failed;
                                c.lined_up = None;
                            } else {
                                c.state = CandidateState::Deferred;
                                wake_at(ready.min(give_up));
                            }
                        }
                    }
                }
            }
        }
        self.queries = queries;
        if results_target == 0 {
            for c in &mut self.list[walked..] {
                c.lined_up = None;
            }
        }
        let done = wake.is_none() && ((results_target == 0 && outstanding == 0) || in_flight == 0);
        (done, wake)
    }

    // The candidate the search's query number `query` went to, while that
    // query is in flight: none once it was pushed past the end.
    fn in_flight(&mut self, query: usize) -> Option<&mut Candidate> {
        self.list
            .iter_mut()
            .find(|c| c.asked == Some(query) && c.state == CandidateState::InFlight)
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

// The nodes `response`, from the node at `source`, names that may be asked:
// of `source`'s family, never an IPv4 address written as IPv6 in `nodes6`,
// and not martian.
fn named_nodes(
    response: &Response<ByteBufOwned>,
    source: SocketAddr,
) -> impl Iterator<Item = (Id20, SocketAddr)> + '_ {
    let is_v4 = source.is_ipv4();
    response
        .nodes
        .iter()
        .flat_map(|n| n.iter().map(|n| n.as_socketaddr()))
        .chain(
            response
                .nodes6
                .iter()
                .flat_map(|n| n.iter().map(|n| n.as_socketaddr())),
        )
        .filter(move |node| is_v4 == node.addr.is_ipv4() && !is_v4_mapped(node.addr))
        .filter(move |node| !is_martian(node.addr, source))
        .map(|node| (node.id, node.addr))
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

// How one of a search's queries ended, by its number (see
// `RecursiveRequest::asking`): its answer, or None when the search withdrew
// it before it was sent.
type Asked = (usize, Option<crate::dht::Result<Response<ByteBufOwned>>>);

// The outgoing rate limiter reads tokio's clock, the one every DHT timeout
// runs on, so a wait on the limiter and the timeouts after it are measured in
// the same time.
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

// How the torrents' peer searches are let in to walk: by the query budget,
// the global rate limiter, with no count of their own (see
// `DhtState::admit_walk`).
#[derive(Debug)]
struct WalkAdmission {
    // The searches waiting to walk, in line: the first holds its one permit.
    door: tokio::sync::Semaphore,
    // The queries waiting on the rate limiter now (see `take_budget`).
    queries_waiting: AtomicUsize,
    // Rung as `queries_waiting` falls to 0.
    budget_free: tokio::sync::Notify,
    // The searches walking now.
    open: AtomicUsize,
}

impl WalkAdmission {
    fn new() -> Self {
        Self {
            door: tokio::sync::Semaphore::new(1),
            queries_waiting: AtomicUsize::new(0),
            budget_free: tokio::sync::Notify::new(),
            open: AtomicUsize::new(0),
        }
    }
}

// A query counted in `WalkAdmission::queries_waiting` while it waits.
struct BudgetWait<'a>(&'a WalkAdmission);

impl<'a> BudgetWait<'a> {
    fn enter(admission: &'a WalkAdmission) -> Self {
        admission.queries_waiting.fetch_add(1, Ordering::AcqRel);
        Self(admission)
    }
}

impl Drop for BudgetWait<'_> {
    fn drop(&mut self) {
        if self.0.queries_waiting.fetch_sub(1, Ordering::AcqRel) == 1 {
            self.0.budget_free.notify_waiters();
        }
    }
}

// A search let in to walk, counted in `WalkAdmission::open` until dropped.
struct OpenWalk<'a>(&'a WalkAdmission);

impl Drop for OpenWalk<'_> {
    fn drop(&mut self) {
        self.0.open.fetch_sub(1, Ordering::AcqRel);
    }
}

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
    // The queries a search sends as it ends, each to its node.
    fn on_search_end(
        &self,
        _req: &RecursiveRequest<Self>,
        _candidates: &Candidates,
    ) -> Vec<(SocketAddr, Request)> {
        Vec::new()
    }
}

struct RecursiveRequestCallbacksGetPeers {
    announce_port: Option<u16>,
}

// A search's query to a node, as the table sees it: a get_peers search
// feeds the table as a find_node one does, as every reply that matches a
// request reaches `node_seen` in libtorrent (rpc_manager.cpp:389, v2.0.15),
// whatever the query. A node a search is told of enters the table only once
// it answers (routing_table.cpp:816-822, 1085-1099: a node not yet pinged
// never takes a place in a bucket). One the table holds already is marked as
// asked as the query is sent. An error reply is no answer: it counts as a
// failure, as a timeout does, and enters nothing (rpc_manager.cpp:316-350,
// where an error calls the observer's `timeout`, and only a response reaches
// `node_seen`). A query the search withdrew once it was sent still feeds the
// table its answer, but its failure is no failure of the node, and a node
// heard of that it marked as asked is as it was before. libtorrent marks
// such a query done (traversal_algorithm.cpp:265, 455), so it never times
// out; a reply to it reaches `node_seen` (rpc_manager.cpp:276-389) until the
// next rpc_manager tick erases it, at most a second later
// (rpc_manager.cpp:396-417).
fn table_asked(dht: &DhtState, target_node: Id20, addr: SocketAddr) -> Marked {
    dht.get_table_for_addr(addr)
        .write()
        .mark_outgoing_request(&target_node, now())
}

fn table_answered(
    dht: &DhtState,
    target_node: Id20,
    addr: SocketAddr,
    resp: &crate::dht::Result<ResponseOrError>,
    withdrawn: bool,
    marked: Marked,
) {
    let mut table = dht.get_table_for_addr(addr).write();
    if let Ok(ResponseOrError::Response(response)) = resp {
        // The node enters under the id it answered with.
        table.add_node(response.id, addr);
        table.mark_response(&response.id, now());
    } else if !withdrawn {
        table.mark_error(&target_node, addr);
    } else if marked == Marked::HeardOf {
        table.unmark_heard_of(&target_node);
    }
}

impl RecursiveRequestCallbacks for RecursiveRequestCallbacksGetPeers {
    // A search that ends announces us to the k closest nodes that gave it a
    // token (libtorrent `node::announce`, whose get_peers traversal ends in
    // `announce_fun`).
    fn on_search_end(
        &self,
        req: &RecursiveRequest<Self>,
        candidates: &Candidates,
    ) -> Vec<(SocketAddr, Request)> {
        let Some(port) = self.announce_port else {
            return Vec::new();
        };
        candidates
            .token_holders()
            .map(|(addr, token)| {
                (
                    addr,
                    Request::Announce {
                        info_hash: req.info_hash,
                        token: token.clone(),
                        port,
                    },
                )
            })
            .collect()
    }
}

struct RecursiveRequestCallbacksFindNodes {}
impl RecursiveRequestCallbacks for RecursiveRequestCallbacksFindNodes {}

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

    // One search at a time, from the table (see `start`): the first as soon
    // as the torrent asks for peers, the family's first bootstrap has been
    // sent (see `DhtState::bootstrap_sent_v4`) and the query budget lets it
    // in (see `DhtState::admit_walk`), then one at the torrent's own point in
    // the interval (see `announce_slot`) and one every
    // DHT_ANNOUNCE_INTERVAL after. Each announces us when it
    // ends (libtorrent `torrent::start_announcing` asks
    // `session_impl::prioritize_dht` for a search at once, and
    // `session_impl::on_dht_announce` gives every torrent its
    // `torrent::dht_announce` once a `dht_announce_interval`). A search due
    // while the table is empty and no router is known starts as the first
    // node enters the table or the first router becomes known, whichever is
    // first. The first search, when the table held too few nodes we can
    // count on and it leaned on the routers (see `start`), was made before
    // the bootstrap seeded the table; if it fell short it is made again as
    // soon as the table holds SEARCH_MIN_SEEDS such nodes, with one of the
    // early turns a growth gives out, and EARLY_SEARCH_MIN does not apply
    // to it. A router that names only nodes that do not answer, or does not
    // answer a get_peers at all, leaves such a search with nothing to walk.
    // A search that fell short, fewer than k nodes answering it, as
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
                self.dht.bootstrap_sent(is_v4).opened().await;
                // When the next search is due: set as a search ends, kept
                // when a short one is made again early.
                let mut due = None;
                let mut early = EARLY_SEARCH_MIN;
                let mut first = true;
                loop {
                    let mut next_entered = std::pin::pin!(entered.notified());
                    next_entered.as_mut().enable();
                    let mut next_router = std::pin::pin!(self.dht.routers_known.notified());
                    next_router.as_mut().enable();
                    let turn = self.dht.admit_walk().await;
                    // Before the search starts: a growth from here on is
                    // one it did not start from.
                    let seen = growth.generation.load(Ordering::Acquire);
                    let candidates = self.start(is_v4, std::iter::empty());
                    if candidates.list.is_empty() {
                        drop(turn);
                        tokio::select! {
                            () = next_entered => {}
                            () = next_router => {}
                        }
                        continue;
                    }
                    let reissue =
                        std::mem::replace(&mut first, false) && candidates.leans_on_routers;
                    let outcome = self.walk(candidates).await?;
                    drop(turn);
                    trace!(?outcome, "search finished");
                    let now = tokio::time::Instant::now();
                    let due_at = *due.get_or_insert_with(|| now + wait);
                    let early_at = now + early;
                    let reissue = reissue && outcome.is_short();
                    if outcome.is_short() && (early_at < due_at || reissue) {
                        tokio::select! {
                            () = tokio::time::sleep_until(due_at) => {}
                            () = growth.early_turn(seen, early_at), if early_at < due_at => {
                                early *= 2;
                                continue;
                            }
                            () = self.dht.seeded_turn(is_v4), if reissue => continue,
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
            candidates.offer(id, addr, true);
        }
        let mut seeds = candidates.list.len();
        if candidates.list.is_empty() {
            // Good nodes first, then the other confirmed ones, then the
            // questionable, unknown and bad ones not confirmed, each tier
            // nearest first. So a table that holds SEARCH_MIN_SEEDS
            // confirmed nodes, a seeded one (`RoutingTable::is_seeded`),
            // gives at least that many, and the search does not lean on the
            // routers.
            let table = self.table(is_v4).read();
            let now = now();
            let mut closest = Vec::with_capacity(SEARCH_K);
            for (confirmed, tier) in [
                (true, Some(NodeStatus::Good)),
                (true, None),
                (false, Some(NodeStatus::Questionable)),
                (false, Some(NodeStatus::Unknown)),
                (false, Some(NodeStatus::Bad)),
            ] {
                if closest.len() == SEARCH_K {
                    break;
                }
                // Good nodes are all confirmed: the second tier is the
                // confirmed ones that are not good.
                let in_tier = |n: &RoutingTableNode| {
                    let status = n.status(now);
                    n.is_confirmed(now) == confirmed
                        && match tier {
                            Some(tier) => status == tier,
                            None => status != NodeStatus::Good,
                        }
                };
                closest.extend(
                    table
                        .closest(&self.info_hash, SEARCH_K - closest.len(), in_tier)
                        .iter()
                        .map(|n| (n.id(), n.addr(), n.is_confirmed(now))),
                );
            }
            drop(table);
            for (id, addr, confirmed) in closest {
                candidates.offer(Some(id), addr, true);
                seeds += usize::from(confirmed);
            }
        }
        if seeds < SEARCH_MIN_SEEDS {
            candidates.leans_on_routers = true;
            for router in self.dht.routers.read().iter() {
                if router.is_ipv4() == is_v4 {
                    candidates.offer(None, *router, true);
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

    // Walk towards the target from `candidates`: keep the closest not asked
    // yet in flight, SEARCH_BRANCHING in all at a time, and offer the nodes
    // each answer names as new candidates (see `Candidates::add_requests`
    // and `offer`). A node that fails or does not answer in time is marked
    // failed and skipped. A node named at an address it cannot be reached at
    // is not offered. A query sent SHORT_TIMEOUT ago with no answer yet lets
    // the walk ask one more node, up to SEARCH_BRANCHING_MAX in flight, and
    // still waits for its answer, which the walk uses as any other; the
    // extra room goes once it answers or fails (libtorrent
    // `traversal_algorithm::failed` and `finished`,
    // traversal_algorithm.cpp:322-408, v2.0.15). A query not sent yet,
    // waiting on its window or the rate limiter, has no such timer. The
    // query to a candidate pushed past the end is withdrawn (`add_entry`,
    // 254-282): not sent yet, it is never sent; sent, its answer feeds only
    // the table (see `table_answered`). The walk ends as `add_requests` says
    // it is done: the queries still in flight then, to nodes farther than
    // the k closest that answered, are withdrawn the same way, as
    // libtorrent's `done` marks them (427-490), and wait for their answers
    // apart from the walk, until RESPONSE_TIMEOUT. As it ends it sends what
    // its callbacks ask for, each under the same limits.
    async fn walk(&self, mut candidates: Candidates) -> crate::dht::Result<SearchOutcome> {
        let mut in_flight = FuturesUnordered::new();
        let (late_tx, mut late_rx) = unbounded_channel();
        let mut outcome = SearchOutcome::default();
        let mut branching = SEARCH_BRANCHING;
        loop {
            let now = tokio::time::Instant::now();
            let (done, wake) =
                candidates.add_requests(&self.dht.send_guard, branching, now, |c, slot| {
                    let (kept, hold) = tokio::sync::oneshot::channel();
                    let query = c.asked.expect("a candidate is numbered as it is asked");
                    in_flight.push(self.asking(query, c.id, c.addr, slot, kept, late_tx.clone()));
                    hold
                });
            if done {
                break;
            }
            let (query, answer) = tokio::select! {
                biased;
                Some(query) = late_rx.recv() => {
                    if let Some(c) = candidates.in_flight(query)
                        && !c.late
                        && branching < SEARCH_BRANCHING_MAX
                    {
                        c.late = true;
                        branching += 1;
                    }
                    continue;
                }
                Some(asked) = in_flight.next() => asked,
                () = sleep_until_some(wake) => continue,
            };
            // Withdrawn before it was sent.
            let Some(answer) = answer else {
                continue;
            };
            let Some(c) = candidates.in_flight(query) else {
                continue;
            };
            c.hold = None;
            if std::mem::take(&mut c.late) {
                branching -= 1;
            }
            let addr = c.addr;
            let response = match answer {
                Ok(response) => response,
                Err(e) => {
                    debug!("{addr}: {e:#}");
                    outcome.failed += 1;
                    c.state = CandidateState::Failed;
                    continue;
                }
            };
            outcome.answered += 1;
            outcome.nodes += usize::from(c.id.is_some());
            c.state = CandidateState::Answered;
            c.token = response.token.clone();
            // A search that wants no peers (a bootstrap or a refresh) has no
            // receiver: a node that names peers anyway is still walked past,
            // never the end of the search.
            for peer in response.values.iter().flatten() {
                let _ = self.peer_tx.send(peer.0);
            }
            // The nodes named are heard of (libtorrent
            // traversal_algorithm.cpp:317, v2.0.15).
            let mut table = self.dht.table(addr.is_ipv4()).write();
            for (id, node) in named_nodes(&response, addr) {
                table.heard_about(id, node);
                if let Some(gone) = candidates.offer(Some(id), node, false)
                    && gone.state == CandidateState::InFlight
                    && gone.late
                {
                    branching -= 1;
                }
            }
        }
        let at_end = self.callbacks.on_search_end(self, &candidates);
        // Withdraws the queries still in flight.
        drop(candidates);
        if !in_flight.is_empty() {
            spawn_with_cancel(
                debug_span!("withdrawn"),
                "dht_withdrawn_queries",
                self.dht.cancellation_token.clone(),
                async move {
                    while in_flight.next().await.is_some() {}
                    Ok::<_, Error>(())
                },
            );
        }
        let mut sends = at_end
            .into_iter()
            .map(|(addr, request)| self.dht.send_unanswered(request, addr))
            .collect::<FuturesUnordered<_>>();
        while sends.next().await.is_some() {}
        Ok(outcome)
    }

    // The search's query number `query` to the node `id` at `addr`, in
    // `slot`, as the walk waits on it: the table is fed as it is sent and as
    // it ends (see `table_asked` and `table_answered`), and `late` is told
    // its number once SHORT_TIMEOUT has passed since it was sent with no
    // answer yet; it still waits for one. A query that ends before it is
    // sent has no such timer. `kept` is the walk's hold on it (see `Hold`):
    // dropped before the query is sent, it is never sent; dropped after, the
    // query is withdrawn.
    fn asking(
        &self,
        query: usize,
        id: Option<Id20>,
        addr: SocketAddr,
        slot: SendSlot,
        mut kept: tokio::sync::oneshot::Sender<()>,
        late: UnboundedSender<usize>,
    ) -> BoxFuture<'static, Asked> {
        let dht = self.dht.clone();
        let request = self.request.clone();
        async move {
            let marked = parking_lot::Mutex::new(Marked::Nothing);
            let on_wire = AtomicBool::new(false);
            let (sent_tx, sent_rx) = tokio::sync::oneshot::channel();
            let mut sent_rx = sent_rx.fuse();
            let ask = dht.request_in(slot, request, addr, |at| {
                on_wire.store(true, Ordering::Relaxed);
                if let Some(id) = id {
                    *marked.lock() = table_asked(&dht, id, addr);
                }
                let _ = sent_tx.send(at);
            });
            let mut ask = std::pin::pin!(ask);
            let mut late_at = None;
            let mut told = false;
            let answer = loop {
                tokio::select! {
                    biased;
                    // Looked at before the query may go out on this turn.
                    () = kept.closed(), if !on_wire.load(Ordering::Relaxed) => return (query, None),
                    answer = &mut ask => break answer,
                    sent = &mut sent_rx, if late_at.is_none() => {
                        late_at = sent.ok().map(|at| at + SHORT_TIMEOUT);
                    }
                    () = sleep_until_some(late_at), if !told => {
                        told = true;
                        // Late, the node has failed once already; silent to
                        // the end, it fails again then (libtorrent
                        // `traversal_algorithm::failed`, traversal_algorithm.cpp:
                        // 352-358, v2.0.15, called at the short timeout and
                        // at the full one alike).
                        if let Some(id) = id
                            && !kept.is_closed()
                        {
                            dht.get_table_for_addr(addr).write().mark_error(&id, addr);
                        }
                        let _ = late.send(query);
                    }
                }
            };
            if let Some(id) = id {
                // A query that never went out is no failure of the node, as
                // a withdrawn one is not.
                let withdrawn = kept.is_closed() || !on_wire.load(Ordering::Relaxed);
                table_answered(&dht, id, addr, &answer, withdrawn, *marked.lock());
            }
            let answer = answer.and_then(|answer| match answer {
                ResponseOrError::Response(r) => Ok(r),
                ResponseOrError::Error(e) => {
                    debug!("error response: {e:?}");
                    Err(Error::ErrorResponse)
                }
            });
            (query, Some(answer))
        }
        .instrument(debug_span!("ask", addr = addr.to_string()))
        .boxed()
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

// Shut until opened, then open for good: what waits on it waits for the
// opening, never on a timer.
struct Latch {
    open: AtomicBool,
    opened: tokio::sync::Notify,
}

impl Latch {
    fn new() -> Self {
        Self {
            open: AtomicBool::new(false),
            opened: tokio::sync::Notify::new(),
        }
    }

    fn open(&self) {
        if !self.open.swap(true, Ordering::AcqRel) {
            self.opened.notify_waiters();
        }
    }

    async fn opened(&self) {
        loop {
            let mut opened = std::pin::pin!(self.opened.notified());
            opened.as_mut().enable();
            if self.open.load(Ordering::Acquire) {
                return;
            }
            opened.await;
        }
    }
}

// Resolves at `at`, or never with none.
async fn sleep_until_some(at: Option<tokio::time::Instant>) {
    match at {
        Some(at) => tokio::time::sleep_until(at).await,
        None => std::future::pending().await,
    }
}

// What the DHT sends to each address over the last 10 seconds (see
// `send_windows`): every datagram is counted as it is handed to the socket,
// and a query goes only with room in its address's window. libtorrent sends
// with no such count; this is Weaver's own, so that however many searches
// want one node at once, a router too, it is never sent the 50 datagrams in
// 10 seconds at which a libtorrent node ignores us for 5 minutes
// (dos_blocker.cpp:52-99, v2.0.15).
struct SendGuard {
    windows: parking_lot::Mutex<SendWindows>,
    // Rings as a datagram is sent while the sweep rests.
    sent: tokio::sync::Notify,
}

impl SendGuard {
    fn new() -> Self {
        Self {
            windows: parking_lot::Mutex::new(SendWindows::new()),
            sent: tokio::sync::Notify::new(),
        }
    }

    // Room for a query to `ip`, or its place in that address's line.
    fn try_send(
        self: &Arc<Self>,
        ip: IpAddr,
        now: tokio::time::Instant,
    ) -> Result<SendSlot, LinedUp> {
        match self.windows.lock().take(ip, now) {
            Ok(()) => Ok(SendSlot {
                guard: self.clone(),
                ip,
                sent: false,
            }),
            Err(ready) => Err(LinedUp {
                guard: self.clone(),
                ip,
                ready,
                admitted: false,
            }),
        }
    }

    // A reply went to `ip`: it counts, and never waits.
    fn record(&self, ip: IpAddr) {
        if self.windows.lock().record(ip, tokio::time::Instant::now()) {
            self.sent.notify_one();
        }
    }

    // Forgets the windows that have closed, when the next one may close,
    // and rests while nothing has been sent since the last one did. Woken
    // by a send, never by a timer while the map is empty.
    async fn sweep(&self) -> std::convert::Infallible {
        loop {
            let mut sent = std::pin::pin!(self.sent.notified());
            sent.as_mut().enable();
            let next = self.windows.lock().sweep(tokio::time::Instant::now());
            match next {
                Some(at) => tokio::time::sleep_until(at).await,
                None => sent.await,
            }
        }
    }
}

// Room in a query's address's window. It counts as sent until it is sent
// or dropped; dropped unsent, the room is given back.
struct SendSlot {
    guard: Arc<SendGuard>,
    ip: IpAddr,
    sent: bool,
}

impl SendSlot {
    fn sent(mut self) {
        self.sent = true;
        if self
            .guard
            .windows
            .lock()
            .sent(self.ip, tokio::time::Instant::now())
        {
            self.guard.sent.notify_one();
        }
    }
}

impl Drop for SendSlot {
    fn drop(&mut self) {
        if !self.sent {
            self.guard.windows.lock().release(self.ip);
        }
    }
}

// A query lined up for room in its address's window, the queries lined up
// before it first. Dropped before it has room, it leaves the line.
struct LinedUp {
    guard: Arc<SendGuard>,
    ip: IpAddr,
    // When it may have room.
    ready: tokio::time::Instant,
    admitted: bool,
}

impl std::fmt::Debug for LinedUp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LinedUp")
            .field("ip", &self.ip)
            .field("ready", &self.ready)
            .finish()
    }
}

impl LinedUp {
    fn try_send(mut self, now: tokio::time::Instant) -> Result<SendSlot, LinedUp> {
        let taken = self.guard.windows.lock().take_lined_up(self.ip, now);
        match taken {
            Ok(()) => {
                self.admitted = true;
                Ok(SendSlot {
                    guard: self.guard.clone(),
                    ip: self.ip,
                    sent: false,
                })
            }
            Err(ready) => {
                self.ready = ready;
                Err(self)
            }
        }
    }

    // Waits in line for room.
    async fn room(mut self) -> SendSlot {
        loop {
            tokio::time::sleep_until(self.ready).await;
            match self.try_send(tokio::time::Instant::now()) {
                Ok(slot) => return slot,
                Err(lined_up) => self = lined_up,
            }
        }
    }
}

impl Drop for LinedUp {
    fn drop(&mut self) {
        if !self.admitted {
            self.guard.windows.lock().leave(self.ip);
        }
    }
}

// Runs `future`, and calls `then` once its first poll is over: for a search,
// once the queries it starts with are sent.
async fn after_first_poll<F: std::future::Future>(future: F, then: impl FnOnce()) -> F::Output {
    let mut future = std::pin::pin!(future);
    let mut then = Some(then);
    std::future::poll_fn(|cx| {
        let polled = future.as_mut().poll(cx);
        if let Some(then) = then.take() {
            then();
        }
        polled
    })
    .await
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
    // Opens once every bootstrap name has been looked up once, whether it
    // resolved or not; no search starts before. libtorrent's
    // `session_impl::start_dht` starts nothing while
    // `m_outstanding_router_lookups` is above zero, and each lookup's end,
    // an error too, takes one off (session_impl.cpp:5897-5904, 6120-6145,
    // v2.0.15). So the first search of each family starts from every
    // router, not only from the one whose name resolved first.
    routers_looked_up: Latch,
    // Open once the first bootstrap of the family has sent the queries it
    // starts with; a torrent's search starts only after. libtorrent's
    // `start_dht` sends each node's bootstrap as it starts the DHT
    // (`dht_tracker::start`, dht_tracker.cpp:174-195), and a torrent's
    // search starts later, from the announce timer
    // (`session_impl::prioritize_dht`, session_impl.cpp:3823-3845).
    bootstrap_sent_v4: Latch,
    bootstrap_sent_v6: Latch,
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

    // What each address has been sent of late; no query goes past it.
    send_guard: Arc<SendGuard>,

    // BEP 5 "v": sent with every outgoing message when set.
    client_version: Option<ByteBufOwned>,

    // Lets the torrents' peer searches in to walk as the query budget has
    // room (see `admit_walk`), so a session that starts ten thousand
    // torrents does not walk for all of them in its first seconds.
    walk_admission: WalkAdmission,
}

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
            routers_looked_up: Latch::new(),
            bootstrap_sent_v4: Latch::new(),
            bootstrap_sent_v6: Latch::new(),
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
            send_guard: Arc::new(SendGuard::new()),
            cancellation_token,
            client_version,
            walk_admission: WalkAdmission::new(),
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

    // Resolves once the `is_v4` table holds SEARCH_MIN_SEEDS nodes we can
    // count on, so that a search from it no longer leans on the routers
    // (see `RecursiveRequest::start`), and an early turn is free; the turn
    // is spent. Woken as the table becomes seeded, a node that answered
    // after a restart too, whether or not the table grew, and never on a
    // timer. Becoming seeded is a growth too (see `watch_growth`), which
    // gives out the early turns.
    async fn seeded_turn(&self, is_v4: bool) {
        let seeded = self.table(is_v4).read().seeded();
        loop {
            let mut next = std::pin::pin!(seeded.notified());
            next.as_mut().enable();
            if self.table(is_v4).read().is_seeded() {
                break;
            }
            next.await;
        }
        self.growth(is_v4)
            .turns
            .acquire()
            .await
            .expect("the early turns are never closed")
            .forget();
    }

    fn bootstrap_sent(&self, is_v4: bool) -> &Latch {
        if is_v4 {
            &self.bootstrap_sent_v4
        } else {
            &self.bootstrap_sent_v6
        }
    }

    // As a DHT whose bootstrap names have all been looked up and whose
    // bootstraps have been sent: what a test that drives the searches itself
    // starts from.
    #[cfg(test)]
    fn started_for_test(&self) {
        self.routers_looked_up.open();
        self.bootstrap_sent_v4.open();
        self.bootstrap_sent_v6.open();
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
    // name resolves to. Wakes what waits for a router when one is new. An
    // IPv4 address written as IPv6 is kept as the IPv4 one it is.
    fn add_routers(&self, addrs: &[SocketAddr]) {
        let mut routers = self.routers.write();
        let before = routers.len();
        routers.extend(
            addrs
                .iter()
                .map(|a| SocketAddr::new(a.ip().to_canonical(), a.port())),
        );
        let added = routers.len() > before;
        drop(routers);
        if added {
            self.routers_known.notify_waiters();
        }
    }

    // Keep the `is_v4` table filled: a find_node search for our own id,
    // from the table and, while it holds fewer than SEARCH_MIN_SEEDS nodes
    // that answer, the routers (see `start`). The first runs once every
    // bootstrap name has been looked up, as libtorrent starts its DHT and
    // its bootstrap only then (see `routers_looked_up`). While
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
    // its node tick (dht_tracker.cpp:268-283, v2.0.15). The table becoming
    // seeded (see `RoutingTable::is_seeded`) is a growth too, the same way,
    // though no node entered: nodes restored from an earlier run are in the
    // table already and only answer. Woken by the table's `entered` and
    // `seeded`, never by a timer while nothing enters, and only the entry
    // count and whether it is seeded are read per wake.
    async fn watch_growth(&self, is_v4: bool) -> std::convert::Infallible {
        let (entered, seeded, mut grown, mut was_seeded) = {
            let table = self.table(is_v4).read();
            (
                table.entered(),
                table.seeded(),
                table.entries(),
                table.is_seeded(),
            )
        };
        loop {
            let mut next_entered = std::pin::pin!(entered.notified());
            next_entered.as_mut().enable();
            let mut next_seeded = std::pin::pin!(seeded.notified());
            next_seeded.as_mut().enable();
            let (entries, is_seeded) = {
                let table = self.table(is_v4).read();
                (table.entries(), table.is_seeded())
            };
            let became_seeded = is_seeded && !was_seeded;
            was_seeded = is_seeded;
            if entries == grown && !became_seeded {
                tokio::select! {
                    () = next_entered => {}
                    () = next_seeded => {}
                }
                continue;
            }
            grown = entries;
            self.grow(is_v4);
            tokio::time::sleep(GROWTH_SPACING).await;
        }
    }

    // The first round waits for every bootstrap name to be looked up once
    // (see `routers_looked_up`), and torrents' searches of the family wait
    // for its first queries to be sent (see `bootstrap_sent`).
    async fn refresh_self_rounds(self: Arc<Self>, is_v4: bool) -> crate::dht::Result<()> {
        let entered = self.table(is_v4).read().entered();
        let mut retry = BOOTSTRAP_RETRY_MIN;
        let mut deep = false;
        self.routers_looked_up.opened().await;
        loop {
            let mut next_entered = std::pin::pin!(entered.notified());
            next_entered.as_mut().enable();
            let mut next_router = std::pin::pin!(self.routers_known.notified());
            next_router.as_mut().enable();
            let before = self.table(is_v4).read().entries();
            let round = RecursiveRequest::find_node_for_routing_table(
                self.clone(),
                self.id,
                is_v4,
                std::iter::empty(),
            )
            .instrument(debug_span!("refresh_self"));
            let outcome = after_first_poll(round, || self.bootstrap_sent(is_v4).open()).await?;
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

    // Keeps the `is_v4` table fresh as libtorrent's `node::tick` does, one
    // query every REFRESH_SPACING (node.cpp:647-674, dht_tracker.cpp:268-283,
    // v2.0.15) to the node `routing_table::next_refresh` picks
    // (routing_table.cpp:392-445): a node of the table never asked, else a
    // node heard of that its bucket has room for, else the node of the table
    // asked least recently. The query is a get_peers for an id between us and
    // the node, or a ping when the node's bucket and its replacements are full
    // (`node::send_single_refresh`, node.cpp:676-716). Each query waits on
    // the node's send window and on the rate limiter as any other does. Woken
    // by the table's `heard` and `entered`, never by a timer while the table
    // is empty.
    async fn refresh_table(self: Arc<Self>, is_v4: bool) -> crate::dht::Result<()> {
        let (heard, entered) = {
            let table = self.table(is_v4).read();
            (table.heard(), table.entered())
        };
        let mut asks = FuturesUnordered::new();
        loop {
            let mut grew = std::pin::pin!(entered.notified());
            grew.as_mut().enable();
            let next = self.table(is_v4).write().next_refresh(now());
            let Some(next) = next else {
                tokio::select! {
                    () = heard.notified() => {}
                    () = grew => {}
                    _ = asks.next(), if !asks.is_empty() => {}
                }
                continue;
            };
            asks.push(
                self.ask_refresh(is_v4, next)
                    .instrument(debug_span!("refresh", addr = %next.addr)),
            );
            let spacing = tokio::time::sleep(REFRESH_SPACING);
            tokio::pin!(spacing);
            loop {
                tokio::select! {
                    () = &mut spacing => break,
                    _ = asks.next(), if !asks.is_empty() => {}
                }
            }
        }
    }

    // One refresh query, as `refresh_table` picked it. A node of the table
    // that does not answer fails once at SHORT_TIMEOUT and again at
    // RESPONSE_TIMEOUT, and an error reply is one failure (libtorrent
    // `rpc_manager::tick`, rpc_manager.cpp:392-462, and `incoming_error`,
    // 316-350, v2.0.15); a query held until it is dropped is no failure. A
    // node heard of that does not answer stays asked, and makes way first for
    // the next one heard of. A node that answers enters the table under the
    // id it answered with, or, with no room for it, waits to take the place
    // of a node that fails; the nodes its answer names are heard of in turn
    // (node.cpp:642-643).
    async fn ask_refresh(&self, is_v4: bool, next: Refresh) {
        let request = if next.ping {
            Request::Ping
        } else {
            Request::GetPeers(next.target)
        };
        let table = self.table(is_v4);
        table.write().mark_outgoing_request(&next.id, now());
        let (sent_tx, sent_rx) = tokio::sync::oneshot::channel();
        let on_wire = AtomicBool::new(false);
        let slot = self.send_slot(next.addr.ip()).await;
        let ask = self.request_in(slot, request, next.addr, |at| {
            on_wire.store(true, Ordering::Relaxed);
            let _ = sent_tx.send(at);
        });
        let mut ask = std::pin::pin!(ask);
        let mut sent_rx = sent_rx.fuse();
        let mut late_at = None;
        let answer = loop {
            tokio::select! {
                biased;
                answer = &mut ask => break answer,
                sent = &mut sent_rx, if late_at.is_none() => {
                    late_at = sent.ok().map(|at| at + SHORT_TIMEOUT);
                }
                () = sleep_until_some(late_at) => {
                    late_at = None;
                    table.write().mark_error(&next.id, next.addr);
                }
            }
        };
        let response = match answer {
            Ok(ResponseOrError::Response(response)) => response,
            Ok(ResponseOrError::Error(e)) => {
                debug!(addr = %next.addr, ?e, "node refused the refresh");
                table.write().mark_error(&next.id, next.addr);
                return;
            }
            Err(e) => {
                debug!(addr = %next.addr, "node did not answer the refresh: {e:#}");
                if on_wire.load(Ordering::Relaxed) {
                    table.write().mark_error(&next.id, next.addr);
                }
                return;
            }
        };
        let mut table = table.write();
        if !matches!(
            table.add_node(response.id, next.addr),
            InsertResult::Ignored
        ) {
            table.mark_response(&response.id, now());
        }
        for (id, addr) in named_nodes(&response, next.addr) {
            table.heard_about(id, addr);
        }
    }

    // Room for a query to `ip` in its send window, waiting in line for it
    // when the window is full (see `SendGuard`).
    async fn send_slot(&self, ip: IpAddr) -> SendSlot {
        match self.send_guard.try_send(ip, tokio::time::Instant::now()) {
            Ok(slot) => slot,
            Err(lined_up) => lined_up.room().await,
        }
    }

    // Hands `datagram` to the socket, counted in its address's send window:
    // in `slot` for a query, as a reply with none.
    fn hand_off(
        &self,
        datagram: WorkerSendRequest,
        slot: Option<SendSlot>,
    ) -> crate::dht::Result<()> {
        let ip = datagram.addr.ip();
        self.worker_sender
            .send(datagram)
            .map_err(|_| Error::DhtDead)?;
        match slot {
            Some(slot) => slot.sent(),
            None => self.send_guard.record(ip),
        }
        Ok(())
    }

    // Resolves once the rate limiter lets one more query out. A query that
    // has to wait is counted in `queries_waiting` while it does, which holds
    // back the searches waiting to walk (see `admit_walk`); one that finds a
    // token at once is not counted and wakes no one.
    async fn take_budget(&self) {
        if self.rate_limiter.check().is_ok() {
            return;
        }
        let _waiting = BudgetWait::enter(&self.walk_admission);
        until_ready(&self.rate_limiter).await;
    }

    // Lets a torrent's peer search in to walk, once the query budget has
    // room: the global rate limiter has a token and no query is waiting on
    // it. The searches waiting are let in one at a time in the order they
    // came (`door` is a fair semaphore of one), and each one let in spends a
    // token, so no more are let in than the limiter allows. So the queries of
    // the searches already walking always come before a new search: while
    // one of them waits on the limiter, none is let in. A search whose
    // candidates all wait on their addresses' send windows has no query on
    // the limiter, so it holds no one back. The search waiting first is woken
    // as the last query waiting on the limiter is sent, or, with none
    // waiting and the limiter spent, as the limiter's next token comes; with
    // no search waiting, nothing runs.
    // In flight: every query sent took a token and is answered or given up
    // within RESPONSE_TIMEOUT, so at most the limiter's burst and
    // RESPONSE_TIMEOUT of its rate are in flight, 250 + 250 * 15 = 4000 at
    // the default 250 a second; each search adds at most SEARCH_BRANCHING
    // (more only for queries sent SHORT_TIMEOUT ago) waiting to be sent.
    // Walking: as a search is let in, every search walking has a query in
    // flight or candidates deferred, and each of those spent a token in the
    // last 2 * RESPONSE_TIMEOUT (a deferred candidate gives up
    // RESPONSE_TIMEOUT after it was deferred, which is at most
    // RESPONSE_TIMEOUT after the walk's last query was sent, or after it was
    // let in), so at most 250 + 250 * 30 = 7750 searches walk at once.
    // libtorrent has no such bound: it starts one torrent's announce per
    // `dht_announce_interval / torrents`, at most 4 s apart while torrents
    // wait to announce at once (`session_impl::update_dht_announce_interval`,
    // session_impl.cpp:6632-6670, v2.0.15), however many are walking.
    async fn admit_walk(&self) -> OpenWalk<'_> {
        let admission = &self.walk_admission;
        let _door = admission
            .door
            .acquire()
            .await
            .expect("the DHT's walk door is never closed");
        loop {
            let mut free = std::pin::pin!(admission.budget_free.notified());
            free.as_mut().enable();
            if admission.queries_waiting.load(Ordering::Acquire) == 0 {
                match self.rate_limiter.check() {
                    Ok(()) => break,
                    Err(not_until) => {
                        tokio::time::sleep(
                            not_until.wait_time_from(self.rate_limiter.clock().now()),
                        )
                        .await;
                        continue;
                    }
                }
            }
            free.await;
        }
        admission.open.fetch_add(1, Ordering::AcqRel);
        OpenWalk(admission)
    }

    // A query no answer is waited for (an announce_peer), sent once its
    // address's send window and the rate limiter have room.
    async fn send_unanswered(&self, request: Request, addr: SocketAddr) {
        let slot = self.send_slot(addr.ip()).await;
        self.take_budget().await;
        let (tid, message) = self.create_request(request, addr);
        let datagram = WorkerSendRequest {
            our_tid: Some(tid),
            message,
            addr,
        };
        if let Err(e) = self.hand_off(datagram, Some(slot)) {
            debug!("{addr}: {e:#}");
        }
    }

    // A query to `addr`, once its send window and the rate limiter have
    // room; its RESPONSE_TIMEOUT starts as it is sent (see `request_in`).
    async fn request(
        &self,
        request: Request,
        addr: SocketAddr,
    ) -> crate::dht::Result<ResponseOrError> {
        let slot = self.send_slot(addr.ip()).await;
        self.request_in(slot, request, addr, |_| {}).await
    }

    // A query in `slot`. `sent` is called as it is sent, with when.
    async fn request_in(
        &self,
        slot: SendSlot,
        request: Request,
        addr: SocketAddr,
        sent: impl FnOnce(tokio::time::Instant),
    ) -> crate::dht::Result<ResponseOrError> {
        // A request waits on the rate limiter, then lives RESPONSE_TIMEOUT
        // from the moment it is sent. It is outstanding until then, and
        // withdrawn at its deadline or when whoever issued it drops it, so a
        // request never outlives the search that wanted it.
        let (tid, message) = self.create_request(request, addr);
        let (_outstanding, answer) = Outstanding::register(self, (tid, addr));
        self.take_budget().await;
        trace!("sending {message:?}");
        self.hand_off(
            WorkerSendRequest {
                our_tid: Some(tid),
                message,
                addr,
            },
            Some(slot),
        )?;
        let sent_at = tokio::time::Instant::now();
        sent(sent_at);
        let response = tokio::time::timeout_at(sent_at + RESPONSE_TIMEOUT, answer)
            .await
            .unwrap_or(Ok(Err(Error::ResponseTimeout(RESPONSE_TIMEOUT))))
            .map_err(|_| Error::DhtDead)??;
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

    // A query from the node `id` at `addr`: a node the table holds is
    // marked as having queried us, and one it does not is heard of
    // (libtorrent node.cpp:831-832, v2.0.15, `heard_about` for every query
    // not marked read-only; BEP 43's read-only flag is not read here).
    fn queried_by(&self, id: Id20, addr: SocketAddr) {
        let mut table = self.get_table_for_addr(addr).write();
        if !table.mark_last_query(&id, addr, now()) {
            table.heard_about(id, addr);
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
                self.queried_by(req.id, addr);
                self.hand_off(
                    WorkerSendRequest {
                        our_tid: None,
                        message,
                        addr,
                    },
                    None,
                )?;
                Ok(())
            }
            MessageKind::AnnouncePeer(ann) => {
                self.queried_by(ann.id, addr);
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
                self.hand_off(
                    WorkerSendRequest {
                        our_tid: None,
                        message,
                        addr,
                    },
                    None,
                )?;
                Ok(())
            }
            MessageKind::GetPeersRequest(req) => {
                let want = req
                    .want
                    .unwrap_or(if addr.is_ipv6() { Want::V6 } else { Want::V4 });
                let (nodes, nodes6) = self.generate_compact_nodes_both(req.info_hash, want);
                let compact_peer_info = self.peer_store.get_for_info_hash(req.info_hash, want);
                self.queried_by(req.id, addr);
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
                self.hand_off(
                    WorkerSendRequest {
                        our_tid: None,
                        message,
                        addr,
                    },
                    None,
                )?;
                Ok(())
            }
            MessageKind::FindNodeRequest(req) => {
                let want = req
                    .want
                    .unwrap_or(if addr.is_ipv6() { Want::V6 } else { Want::V4 });
                let (nodes, nodes6) = self.generate_compact_nodes_both(req.target, want);
                self.queried_by(req.id, addr);
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
                self.hand_off(
                    WorkerSendRequest {
                        our_tid: None,
                        message,
                        addr,
                    },
                    None,
                )?;
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
    // WARN, every later one at DEBUG: `warned` is shared by them. The first
    // lookup's end, an error too, takes one off `outstanding`, after the
    // addresses it gave are known; the last opens `routers_looked_up`
    // (libtorrent `session_impl::on_dht_router_name_lookup`,
    // session_impl.cpp:6120-6145, v2.0.15). A name that resolves only on a
    // later try joins the routers then, as `add_router_node` adds a router
    // to a running DHT.
    async fn resolve_router(&self, hostname: &str, warned: &AtomicBool, outstanding: &AtomicUsize) {
        let first = AtomicBool::new(true);
        let looked_up = || {
            if first.swap(false, Ordering::Relaxed)
                && outstanding.fetch_sub(1, Ordering::AcqRel) == 1
            {
                self.dht.routers_looked_up.open();
            }
        };
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
                looked_up();
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
        looked_up();
    }

    // Routers are one way to seed the routing table; the table saved from an
    // earlier run and the nodes peers name (`add_node`) are the others. Each
    // router's name is resolved until it resolves; `refresh_self` asks the
    // routers, as many times as the table needs them. The searches start
    // once each name has been looked up once (see `resolve_router`).
    async fn bootstrap(&self, bootstrap_addrs: &[String]) {
        if bootstrap_addrs.is_empty() {
            info!(
                "no DHT bootstrap hosts: the DHT runs from its saved nodes and the nodes peers name"
            );
            self.dht.routers_looked_up.open();
            return;
        }
        let warned = AtomicBool::new(false);
        let outstanding = AtomicUsize::new(bootstrap_addrs.len());
        let mut futs = bootstrap_addrs
            .iter()
            .map(|addr| self.resolve_router(addr, &warned, &outstanding))
            .collect::<FuturesUnordered<_>>();
        while futs.next().await.is_some() {}
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
                // A dual-stack socket reports an IPv4 source written as
                // IPv6; it is the IPv4 node it is, which our requests to it
                // are keyed under.
                let addr = SocketAddr::new(addr.ip().to_canonical(), addr.port());
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

        let refresh_table_v4 = self
            .dht
            .clone()
            .refresh_table(true)
            .instrument(debug_span!("refresh_table_v4"));
        let refresh_table_v6 = self
            .dht
            .clone()
            .refresh_table(false)
            .instrument(debug_span!("refresh_table_v6"));

        let send_windows_sweep = self.dht.send_guard.sweep();

        tokio::pin!(send_windows_sweep);
        tokio::pin!(refresh_self_v4);
        tokio::pin!(refresh_self_v6);
        tokio::pin!(refresh_table_v4);
        tokio::pin!(refresh_table_v6);
        tokio::pin!(framer);
        tokio::pin!(bootstrap);
        tokio::pin!(response_reader);
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
                never = &mut send_windows_sweep => match never {},
                err = &mut refresh_self_v4 => {
                    return Error::task_finished(&"refresh_self_v4", err);
                },
                err = &mut refresh_self_v6 => {
                    return Error::task_finished(&"refresh_self_v6", err);
                },
                err = &mut refresh_table_v4 => {
                    return Error::task_finished(&"refresh_table_v4", err);
                },
                err = &mut refresh_table_v6 => {
                    return Error::task_finished(&"refresh_table_v6", err);
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
            // Each table keeps only the nodes of its family.
            let routing_table = config
                .routing_table
                .filter(|t| t.id() == peer_id)
                .map(|t| t.of_family(true));
            let routing_table_v6 = config
                .routing_table_v6
                .filter(|t| t.id() == peer_id)
                .map(|t| t.of_family(false));
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

    #[cfg(test)]
    pub(crate) fn with_routing_tables_mut<R>(
        &self,
        f: impl FnOnce(&mut RoutingTable, &mut RoutingTable) -> R,
    ) -> R {
        f(
            &mut self.routing_table_v4.write(),
            &mut self.routing_table_v6.write(),
        )
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

        // The table's refresh of the node is a get_peers too, for an id of
        // its own: the search is the one for `info_hash`.
        loop {
            let m = recv_message_from(&node, dht.listen_addr()).await;
            if let MessageKind::GetPeersRequest(r) = m.kind
                && r.info_hash == info_hash
            {
                break;
            }
        }
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
        dht.started_for_test();
        (dht, rx)
    }

    // `fixture_dht`, before its bootstrap names are looked up: nothing is
    // started.
    fn unstarted_fixture_dht() -> (
        std::sync::Arc<DhtState>,
        tokio::sync::mpsc::UnboundedReceiver<super::WorkerSendRequest>,
    ) {
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        let dht = DhtState::new_internal(
            crate::core::hash_id::Id20::new([0x11; 20]),
            tx,
            None,
            None,
            (Ipv4Addr::LOCALHOST, 0).into(),
            tokio_util::sync::CancellationToken::new(),
            None,
        );
        (std::sync::Arc::new(dht), rx)
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

    // Node i, at its own /24 (see `walk_node`), answers at once naming the
    // eight nodes just closer to the target than itself, unless it is a
    // third one, which never answers. However many closer nodes the answers
    // name, the walk never has more than SEARCH_BRANCHING queries in flight
    // while no query is late (no time passes but in jumps of
    // RESPONSE_TIMEOUT, past every short timeout): libtorrent with
    // `dht_aggressive_lookups` off, traversal_algorithm.cpp:519-523,
    // v2.0.15. It asks each node at most once, and ends.
    #[tokio::test(start_paused = true)]
    async fn test_search_keeps_at_most_branching_requests_in_flight_and_ends() {
        use super::SEARCH_BRANCHING;
        use crate::dht::RESPONSE_TIMEOUT;
        use std::{collections::HashSet, task::Poll};

        let (dht, mut sent) = fixture_dht();
        let req = get_peers_search(&dht);
        let silent = |i: u8| i.is_multiple_of(3);
        let given = (60..65u8).map(|i| {
            let (id, addr) = walk_node(i);
            (Some(id), addr)
        });
        let mut search = std::pin::pin!(req.search(true, given));

        let mut asked = HashSet::new();
        let mut most_in_flight = 0;
        let outcome = loop {
            let polled = futures::poll!(search.as_mut());
            let in_flight = dht.get_stats().outstanding_requests;
            assert!(in_flight <= SEARCH_BRANCHING, "{in_flight} in flight");
            most_in_flight = most_in_flight.max(in_flight);
            if let Poll::Ready(outcome) = polled {
                break outcome.unwrap();
            }
            let mut answered = false;
            while let Ok(request) = sent.try_recv() {
                let i = walk_index(request.addr);
                assert!(asked.insert(i), "{i} asked twice");
                if !silent(i) {
                    let closer: Vec<_> = (i.saturating_sub(8)..i)
                        .map(|j| {
                            let (id, addr) = walk_node(j);
                            let SocketAddr::V4(addr) = addr else {
                                unreachable!()
                            };
                            (id, addr)
                        })
                        .collect();
                    reply(&dht, &request, walk_node(i).0, &closer, &[]);
                    answered = true;
                }
            }
            if !answered {
                // The queries in flight run out their time.
                tokio::time::advance(RESPONSE_TIMEOUT).await;
            }
        };

        assert_eq!(most_in_flight, SEARCH_BRANCHING);
        // It walked down to the nodes nearest the target.
        assert!(asked.contains(&0) && asked.contains(&1));
        // 38 asked: 25 answered, 12 silent ones failed, and one silent one,
        // farther than the eight closest that answered, was withdrawn as
        // the walk ended.
        assert_eq!(asked.len(), 38);
        assert_eq!((outcome.answered, outcome.failed), (25, 12));
        assert_eq!(asked.iter().filter(|&&i| silent(i)).count(), 13);
        assert_eq!(dht.get_stats().outstanding_requests, 1);
    }

    // The walk starts from five far nodes. One answers at once, naming eight
    // nodes closer than all five; the other four stay silent, in flight. No
    // time passes, so no query is late: the walk asks one more node, the
    // closest, in the room the answer left, not five.
    #[tokio::test(start_paused = true)]
    async fn test_an_answer_naming_closer_nodes_keeps_branching_in_flight() {
        use super::SEARCH_BRANCHING;
        use std::task::Poll;

        let (dht, mut sent) = fixture_dht();
        let req = get_peers_search(&dht);
        let far: Vec<_> = (20..25u8)
            .map(|i| {
                let (id, addr) = walk_node(i);
                (Some(id), addr)
            })
            .collect();
        let mut search = std::pin::pin!(req.search(true, far.into_iter()));
        assert!(matches!(futures::poll!(search.as_mut()), Poll::Pending));
        let first: Vec<_> = std::iter::from_fn(|| sent.try_recv().ok()).collect();
        assert_eq!(first.len(), SEARCH_BRANCHING);

        let closer: Vec<_> = (0..8u8)
            .map(|i| {
                let (id, addr) = walk_node(i);
                let SocketAddr::V4(addr) = addr else {
                    unreachable!()
                };
                (id, addr)
            })
            .collect();
        let i = walk_index(first[0].addr);
        reply(&dht, &first[0], walk_node(i).0, &closer, &[]);
        assert!(matches!(futures::poll!(search.as_mut()), Poll::Pending));

        let more: Vec<_> = std::iter::from_fn(|| sent.try_recv().ok())
            .map(|request| walk_index(request.addr))
            .collect();
        assert_eq!(more, vec![0]);
        assert_eq!(dht.get_stats().outstanding_requests, SEARCH_BRANCHING);
    }

    // A node `d` from the target [0x22; 20], at `addr`.
    fn node_at(d: u32, addr: [u8; 4]) -> (crate::core::hash_id::Id20, std::net::SocketAddrV4) {
        let mut id = [0x22; 20];
        for (b, x) in id[16..].iter_mut().zip(d.to_be_bytes()) {
            *b ^= x;
        }
        (
            crate::core::hash_id::Id20::new(id),
            std::net::SocketAddrV4::new(addr.into(), 6881),
        )
    }

    fn given(
        nodes: impl IntoIterator<Item = (crate::core::hash_id::Id20, std::net::SocketAddrV4)>,
    ) -> impl Iterator<Item = (Option<crate::core::hash_id::Id20>, SocketAddr)> {
        nodes
            .into_iter()
            .map(|(id, addr)| (Some(id), SocketAddr::V4(addr)))
    }

    // A node answers naming 50 nodes of one /24, as one could that wants us
    // to send there: the walk asks one of them, the first named
    // (libtorrent `dht_restrict_search_ips`, traversal_algorithm.cpp:
    // 191-226, v2.0.15).
    #[tokio::test(start_paused = true)]
    async fn test_an_answer_naming_many_nodes_of_one_network_adds_one_of_them() {
        use crate::dht::RESPONSE_TIMEOUT;
        use std::task::Poll;

        let (dht, mut sent) = fixture_dht();
        let req = get_peers_search(&dht);
        let first = node_at(1000, [10, 0, 0, 1]);
        let hostile: Vec<_> = (0..50u8)
            .map(|i| node_at(u32::from(i) + 1, [10, 5, 0, i + 1]))
            .collect();
        let mut search = std::pin::pin!(req.search(true, given([first])));
        let mut asked = Vec::new();
        let outcome = loop {
            let polled = futures::poll!(search.as_mut());
            let mut answered = false;
            while let Ok(request) = sent.try_recv() {
                asked.push(request.addr);
                if request.addr == SocketAddr::V4(first.1) {
                    reply(&dht, &request, first.0, &hostile, &[]);
                    answered = true;
                }
            }
            if let Poll::Ready(outcome) = polled {
                break outcome.unwrap();
            }
            if !answered {
                tokio::time::advance(RESPONSE_TIMEOUT).await;
            }
        };
        assert_eq!(
            asked,
            vec![SocketAddr::V4(first.1), SocketAddr::V4(hostile[0].1)]
        );
        assert_eq!((outcome.answered, outcome.failed), (1, 1));
    }

    // A chain of nodes, each answering with a token and naming one node
    // closer than itself, as long as the list of candidates the walk keeps.
    // The answer to the hundredth query names eight nodes closer to the
    // target than any asked. They are asked all the same: the walk has no
    // count of queries to spend, and ends as libtorrent's does, once the
    // eight closest it knows have answered (traversal_algorithm.cpp:519-523,
    // v2.0.15). It announces to those eight.
    #[tokio::test(start_paused = true)]
    async fn test_closer_nodes_named_by_the_hundredth_answer_are_asked() {
        use super::SEARCH_CANDIDATES;
        use std::{collections::BTreeSet, task::Poll};

        let (dht, mut sent) = fixture_dht();
        let info_hash = crate::core::hash_id::Id20::new([0x22; 20]);
        let req = super::RecursiveRequest::get_peers(
            dht.clone(),
            info_hash,
            Some(6881),
            tokio::sync::mpsc::unbounded_channel().0,
        );
        let chain = |n: u32| {
            let [_, _, a, b] = n.to_be_bytes();
            node_at(1_000_000 - n, [10, a, b, 1])
        };
        let index = |addr: SocketAddr| match addr.ip() {
            std::net::IpAddr::V4(a) => u32::from(a.octets()[1]) << 8 | u32::from(a.octets()[2]),
            std::net::IpAddr::V6(_) => unreachable!(),
        };
        let last = u32::try_from(SEARCH_CANDIDATES).unwrap() - 1;
        let closer: Vec<_> = (1..=8u8)
            .map(|i| node_at(u32::from(i), [10, 200, i, 1]))
            .collect();
        let mut search = std::pin::pin!(req.search(true, given([chain(0)])));
        let mut asked = 0;
        let outcome = loop {
            if let Poll::Ready(outcome) = futures::poll!(search.as_mut()) {
                break outcome.unwrap();
            }
            let mut answered = false;
            while let Ok(request) = sent.try_recv() {
                asked += 1;
                let n = index(request.addr);
                if let Some(c) = closer.iter().find(|c| SocketAddr::V4(c.1) == request.addr) {
                    answer(&dht, &request, c.0, Some(b"t"), &[]);
                } else if n == last {
                    answer(&dht, &request, chain(n).0, Some(b"t"), &closer);
                } else {
                    answer(&dht, &request, chain(n).0, Some(b"t"), &[chain(n + 1)]);
                }
                answered = true;
            }
            if !answered {
                tokio::time::advance(Duration::from_millis(100)).await;
            }
        };
        assert_eq!(asked, SEARCH_CANDIDATES + closer.len());
        assert_eq!(
            (outcome.answered, outcome.failed),
            (SEARCH_CANDIDATES + closer.len(), 0)
        );

        let mut announced = BTreeSet::new();
        while let Ok(request) = sent.try_recv() {
            assert!(matches!(request.message.kind, MessageKind::AnnouncePeer(_)));
            announced.insert(request.addr);
        }
        assert_eq!(
            announced,
            closer.iter().map(|c| SocketAddr::V4(c.1)).collect()
        );
    }

    // The closest node's address has its send window kept full, by traffic
    // that is not the walk's. Its query is deferred, and RESPONSE_TIMEOUT
    // after that it fails for the walk, which then ends: the eight others
    // answered at once.
    #[tokio::test(start_paused = true)]
    async fn test_an_address_whose_window_stays_full_does_not_hold_a_walk_open() {
        use crate::dht::{RESPONSE_TIMEOUT, send_windows::SEND_LIMIT};
        use std::task::Poll;

        let (dht, mut sent) = fixture_dht();
        let full = walk_node(0).1;
        let fill = || {
            for _ in 0..SEND_LIMIT {
                dht.send_guard.record(full.ip());
            }
        };
        let req = get_peers_search(&dht);
        let start = tokio::time::Instant::now();
        let mut search = std::pin::pin!(req.search(
            true,
            (0..9).map(|i| {
                let (id, addr) = walk_node(i);
                (Some(id), addr)
            })
        ));
        let outcome = loop {
            fill();
            let polled = futures::poll!(search.as_mut());
            let mut answered = false;
            while let Ok(request) = sent.try_recv() {
                assert_ne!(request.addr, full, "sent past a full window");
                let i = walk_index(request.addr);
                reply(&dht, &request, walk_node(i).0, &[], &[]);
                answered = true;
            }
            if let Poll::Ready(outcome) = polled {
                break outcome.unwrap();
            }
            if !answered {
                tokio::time::advance(Duration::from_secs(1)).await;
            }
        };
        assert_eq!((outcome.answered, outcome.failed), (8, 0));
        assert_eq!(tokio::time::Instant::now() - start, RESPONSE_TIMEOUT);
        assert_eq!(dht.send_guard.windows.lock().lined_up(full.ip()), 0);
    }

    // A walk keeps 100 candidates. The first of the five it asks answers
    // naming 100 nodes closer than all of them, so the other four are
    // pushed off while they wait on the rate limiter, one query a second:
    // they are never sent. The walk ends with the queries it no longer
    // needs still waiting, and those are never sent either.
    #[tokio::test(start_paused = true)]
    async fn test_a_query_for_a_candidate_pushed_off_is_never_sent() {
        use super::{RateLimiter, TokioClock};
        use crate::dht::RESPONSE_TIMEOUT;
        use std::{num::NonZeroU32, task::Poll};

        let (mut state, mut sent) = fixture_state();
        state.rate_limiter = RateLimiter::direct_with_clock(
            governor::Quota::per_second(NonZeroU32::new(1).unwrap()),
            TokioClock {
                start: tokio::time::Instant::now(),
            },
        );
        let dht = std::sync::Arc::new(state);
        let far = |i: u8| node_at(0x1000 + u32::from(i), [10, 1, i, 1]);
        let near: Vec<_> = (0..100u8)
            .map(|j| node_at(0x100 + u32::from(j), [10, 2, j, 1]))
            .collect();
        let req = get_peers_search(&dht);
        let mut search = std::pin::pin!(req.search(true, given((0..100).map(far))));
        let mut asked = Vec::new();
        let outcome = loop {
            let polled = futures::poll!(search.as_mut());
            let mut answered = false;
            while let Ok(request) = sent.try_recv() {
                asked.push(request.addr);
                if request.addr == SocketAddr::V4(far(0).1) {
                    reply(&dht, &request, far(0).0, &near, &[]);
                } else {
                    let n = near
                        .iter()
                        .find(|n| SocketAddr::V4(n.1) == request.addr)
                        .expect("only far node 0 and the near ones are asked");
                    reply(&dht, &request, n.0, &[], &[]);
                }
                answered = true;
            }
            if let Poll::Ready(outcome) = polled {
                break outcome.unwrap();
            }
            if !answered {
                tokio::time::advance(Duration::from_secs(1)).await;
            }
        };
        // The walk ended once the eight closest answered.
        assert_eq!(outcome.answered, 9);
        let mut want: Vec<_> = std::iter::once(far(0).1)
            .chain(near[..8].iter().map(|n| n.1))
            .map(SocketAddr::V4)
            .collect();
        want.sort();
        asked.sort();
        assert_eq!(asked, want);
        for _ in 0..2 {
            tokio::time::advance(RESPONSE_TIMEOUT).await;
        }
        assert!(sent.try_recv().is_err(), "a withdrawn query was sent");
        assert_eq!(dht.get_stats().outstanding_requests, 0);
    }

    // A query is sent to a node that is then pushed off the walk's list; a
    // node named later at the same address, under another id, is asked in
    // turn. The answer to the first query, which comes then, is not the
    // second one's: the walk waits for that until it fails. The answer
    // still reaches the table.
    #[tokio::test(start_paused = true)]
    async fn test_an_answer_to_a_withdrawn_query_is_not_credited_to_a_new_candidate_at_its_address()
    {
        use crate::dht::RESPONSE_TIMEOUT;
        use std::task::Poll;

        let (dht, mut sent) = fixture_dht();
        let far = |i: u8| node_at(0x1000 + u32::from(i), [10, 1, i, 1]);
        let near: Vec<_> = (0..100u8)
            .map(|j| node_at(0x100 + u32::from(j), [10, 2, j, 1]))
            .collect();
        // At far node 0's address, closer than all.
        let again = (node_at(0x10, [0; 4]).0, far(0).1);
        let req = get_peers_search(&dht);
        let mut search = std::pin::pin!(req.search(true, given((0..100).map(far))));
        let drain = |sent: &mut tokio::sync::mpsc::UnboundedReceiver<super::WorkerSendRequest>| {
            std::iter::from_fn(|| sent.try_recv().ok()).collect::<Vec<_>>()
        };
        let addrs = |requests: &[super::WorkerSendRequest]| {
            let mut addrs: Vec<_> = requests.iter().map(|r| r.addr).collect();
            addrs.sort();
            addrs
        };
        let v4 = |nodes: &[(crate::core::hash_id::Id20, std::net::SocketAddrV4)]| {
            let mut addrs: Vec<_> = nodes.iter().map(|n| SocketAddr::V4(n.1)).collect();
            addrs.sort();
            addrs
        };

        assert!(matches!(futures::poll!(search.as_mut()), Poll::Pending));
        let first = drain(&mut sent);
        assert_eq!(addrs(&first), v4(&(0..5).map(far).collect::<Vec<_>>()));
        let old = first
            .iter()
            .find(|r| r.addr == SocketAddr::V4(far(0).1))
            .unwrap();
        let one = first
            .iter()
            .find(|r| r.addr == SocketAddr::V4(far(1).1))
            .unwrap();
        // Far nodes 0 and 2-4 are pushed off with their queries in flight.
        reply(&dht, one, far(1).0, &near, &[]);
        assert!(matches!(futures::poll!(search.as_mut()), Poll::Pending));

        let second = drain(&mut sent);
        assert_eq!(addrs(&second), v4(&near[..5]));
        for request in &second {
            let n = near
                .iter()
                .find(|n| SocketAddr::V4(n.1) == request.addr)
                .unwrap();
            let named: &[_] = if n == &near[0] { &[again] } else { &[] };
            reply(&dht, request, n.0, named, &[]);
        }
        assert!(matches!(futures::poll!(search.as_mut()), Poll::Pending));

        let third = drain(&mut sent);
        let mut want = v4(&near[5..9]);
        want.push(SocketAddr::V4(again.1));
        want.sort();
        assert_eq!(addrs(&third), want);
        // The first query to that address is answered now, while the
        // table's bucket for these ids still has room.
        reply(&dht, old, far(0).0, &[], &[]);
        assert!(matches!(futures::poll!(search.as_mut()), Poll::Pending));
        assert!(
            dht.routing_table_v4
                .read()
                .iter()
                .any(|n| n.id() == far(0).0)
        );
        for request in third.iter().filter(|r| r.addr != SocketAddr::V4(again.1)) {
            let n = near
                .iter()
                .find(|n| SocketAddr::V4(n.1) == request.addr)
                .unwrap();
            reply(&dht, request, n.0, &[], &[]);
        }
        assert!(matches!(futures::poll!(search.as_mut()), Poll::Pending));

        tokio::time::advance(RESPONSE_TIMEOUT).await;
        let Poll::Ready(outcome) = futures::poll!(search.as_mut()) else {
            panic!("the walk did not end as the new candidate failed");
        };
        let outcome = outcome.unwrap();
        // Far node 1 and near nodes 0-8 answered; the new candidate failed.
        assert_eq!((outcome.answered, outcome.failed), (10, 1));
    }

    // Two of the ten nodes are still in flight when the walk ends: node 8,
    // which answers after it ended, and node 9, heard of into the table
    // before, which never does. Node 8's answer still enters it in the
    // table (libtorrent rpc_manager.cpp:276-389, 413-417, v2.0.15). Node 9
    // is not failed for the table and is no longer marked as asked as its
    // query times out, so the table asks it again at its next tick. The two have ids
    // near our own, in the bucket that has room once the eight closest
    // have filled theirs.
    #[tokio::test(start_paused = true)]
    async fn test_a_withdrawn_query_still_feeds_the_table() {
        use super::{REFRESH_SPACING, SHORT_TIMEOUT};
        use crate::core::hash_id::Id20;
        use crate::dht::RESPONSE_TIMEOUT;

        let (dht, mut sent) = fixture_dht();
        let id_of = |i: u8| {
            if i < 8 {
                walk_node(i).0
            } else {
                let mut id = dht.id.0;
                id[19] ^= i;
                Id20::new(id)
            }
        };
        let addr9 = walk_node(9).1;
        assert!(dht.routing_table_v4.write().heard_about(id_of(9), addr9));
        let slow = Duration::from_secs(2);
        let held = std::sync::Mutex::new(None);
        let req = get_peers_search(&dht);
        let start = tokio::time::Instant::now();
        let given: Vec<_> = (0..10).map(|i| (Some(id_of(i)), walk_node(i).1)).collect();
        let search = tokio::spawn(async move { req.search(true, given.into_iter()).await });
        let (outcome, log) = drive(
            &mut sent,
            search,
            |i| match i {
                0..5 => Some(slow),
                5..9 => Some(Duration::ZERO),
                _ => None,
            },
            |request| {
                let i = walk_index(request.addr);
                if i == 8 {
                    *held.lock().unwrap() = Some(request.message.transaction_id.clone());
                } else {
                    reply(&dht, request, walk_node(i).0, &[], &[]);
                }
            },
        )
        .await;
        assert_eq!(outcome.unwrap().answered, 8);
        assert_eq!(tokio::time::Instant::now() - start, slow);
        assert_eq!(dht.get_stats().outstanding_requests, 2);
        assert_eq!(queried_at(&log, SHORT_TIMEOUT), (5..10).collect::<Vec<_>>());

        let refresher = tokio::spawn(dht.clone().refresh_table(true));
        let entered = dht.routing_table_v4.read().entered();
        let mut rang = std::pin::pin!(entered.notified());
        rang.as_mut().enable();
        dht.on_received_message(
            bprotocol::Message {
                transaction_id: held.lock().unwrap().take().unwrap(),
                version: None,
                ip: None,
                kind: MessageKind::Response(bprotocol::Response {
                    id: id_of(8),
                    ..Default::default()
                }),
            },
            walk_node(8).1,
        )
        .unwrap();
        rang.await;
        assert!(
            dht.routing_table_v4
                .read()
                .iter()
                .any(|n| n.id() == id_of(8))
        );

        // The table refreshes its nodes one per tick from `slow` on; node 9
        // is asked at the first tick after its query timed out.
        let request = loop {
            let request = sent.recv().await.unwrap();
            if request.addr == addr9 {
                break request;
            }
            assert!((0..9).any(|i| walk_node(i).1 == request.addr));
        };
        assert!(matches!(
            request.message.kind,
            MessageKind::GetPeersRequest(_)
        ));
        let asked = tokio::time::Instant::now() - start;
        assert!(asked > SHORT_TIMEOUT + RESPONSE_TIMEOUT);
        assert_eq!(asked, slow + 3 * REFRESH_SPACING);
        assert!(asked - REFRESH_SPACING < SHORT_TIMEOUT + RESPONSE_TIMEOUT);
        refresher.abort();
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

    // A DHT whose rate limiter lets one query out a minute and has spent it.
    fn fixture_dht_limited() -> (
        Arc<DhtState>,
        tokio::sync::mpsc::UnboundedReceiver<super::WorkerSendRequest>,
    ) {
        use super::{RateLimiter, TokioClock};
        use std::num::NonZeroU32;

        let (mut state, sent) = fixture_state();
        state.rate_limiter = RateLimiter::direct_with_clock(
            governor::Quota::per_minute(NonZeroU32::new(1).unwrap()),
            TokioClock {
                start: tokio::time::Instant::now(),
            },
        );
        assert!(state.rate_limiter.check().is_ok());
        (Arc::new(state), sent)
    }

    // A query the rate limiter holds goes out once the limiter has room, a
    // minute later here, and waits RESPONSE_TIMEOUT for its answer from then,
    // not from when it was made.
    #[tokio::test(start_paused = true)]
    async fn test_a_query_the_limiter_holds_waits_its_timeout_from_its_send() {
        use crate::dht::RESPONSE_TIMEOUT;

        let (dht, mut sent) = fixture_dht_limited();
        let addr = SocketAddr::from(([10, 0, 1, 1], 6881));
        let start = tokio::time::Instant::now();
        let ask = tokio::spawn({
            let dht = dht.clone();
            async move { dht.request(super::Request::Ping, addr).await }
        });
        let ping = sent.recv().await.unwrap();
        assert_eq!(ping.addr, addr);
        let held = tokio::time::Instant::now() - start;
        assert_eq!(held, Duration::from_secs(60));
        assert!(matches!(
            ask.await.unwrap(),
            Err(crate::dht::Error::ResponseTimeout(_))
        ));
        assert_eq!(tokio::time::Instant::now() - start, held + RESPONSE_TIMEOUT);
        assert_eq!(dht.get_stats().outstanding_requests, 0);
    }

    fn errors_of(dht: &DhtState, id: crate::core::hash_id::Id20) -> usize {
        let table = dht.routing_table_v4.read();
        table
            .iter()
            .find(|n| n.id() == id)
            .unwrap()
            .errors_in_a_row()
    }

    // A node of `dht`'s IPv4 table that has answered, and the refresh that
    // pings it.
    fn refreshing_one_node(dht: &DhtState) -> super::Refresh {
        let id = crate::core::hash_id::Id20::new([0x21; 20]);
        let addr = SocketAddr::from(([10, 0, 1, 1], 6881));
        let mut table = dht.routing_table_v4.write();
        table.add_node(id, addr);
        assert!(table.mark_response(&id, std::time::Instant::now()));
        super::Refresh {
            id,
            addr,
            target: id,
            ping: true,
        }
    }

    // A refresh the rate limiter still holds when it is dropped never went
    // out: the node is not failed for it.
    #[tokio::test(start_paused = true)]
    async fn test_a_refresh_dropped_before_it_is_sent_fails_no_table_node() {
        let (dht, mut sent) = fixture_dht_limited();
        let next = refreshing_one_node(&dht);
        {
            let mut ask = std::pin::pin!(dht.ask_refresh(true, next));
            while dht.get_stats().outstanding_requests == 0 {
                assert!(futures::poll!(ask.as_mut()).is_pending());
            }
        }
        assert_eq!(dht.get_stats().outstanding_requests, 0);
        assert!(sent.try_recv().is_err(), "sent past an empty limiter");
        assert_eq!(errors_of(&dht, next.id), 0);
    }

    // A refresh sent and never answered fails its node once at SHORT_TIMEOUT
    // and once more at RESPONSE_TIMEOUT, both counted from the send. The node
    // has answered before and its bucket holds no replacement, so it stays.
    #[tokio::test(start_paused = true)]
    async fn test_a_refresh_not_answered_fails_its_node_at_both_timeouts() {
        use super::SHORT_TIMEOUT;
        use crate::dht::RESPONSE_TIMEOUT;

        let (dht, mut sent) = fixture_dht();
        let next = refreshing_one_node(&dht);
        let mut ask = std::pin::pin!(dht.ask_refresh(true, next));
        let ping = loop {
            assert!(futures::poll!(ask.as_mut()).is_pending());
            if let Ok(ping) = sent.try_recv() {
                break ping;
            }
        };
        assert!(matches!(ping.message.kind, MessageKind::PingRequest(_)));
        assert_eq!(errors_of(&dht, next.id), 0);
        tokio::time::advance(SHORT_TIMEOUT).await;
        while errors_of(&dht, next.id) == 0 {
            assert!(futures::poll!(ask.as_mut()).is_pending());
        }
        assert_eq!(errors_of(&dht, next.id), 1);
        tokio::time::advance(RESPONSE_TIMEOUT - SHORT_TIMEOUT).await;
        while futures::poll!(ask.as_mut()).is_pending() {}
        assert_eq!(errors_of(&dht, next.id), 2);
        assert_eq!(dht.get_stats().outstanding_requests, 0);
    }

    // A refresh answered with an error fails its node once; one answered
    // late, past SHORT_TIMEOUT, forgives the failure the short timeout gave.
    #[tokio::test(start_paused = true)]
    async fn test_a_refresh_refused_fails_once_and_a_late_answer_forgives() {
        use super::SHORT_TIMEOUT;

        let (dht, mut sent) = fixture_dht();
        let next = refreshing_one_node(&dht);
        {
            let mut ask = std::pin::pin!(dht.ask_refresh(true, next));
            let ping = loop {
                assert!(futures::poll!(ask.as_mut()).is_pending());
                if let Ok(ping) = sent.try_recv() {
                    break ping;
                }
            };
            refuse(&dht, &ping);
            while futures::poll!(ask.as_mut()).is_pending() {}
        }
        assert_eq!(errors_of(&dht, next.id), 1);

        let mut ask = std::pin::pin!(dht.ask_refresh(true, next));
        let ping = loop {
            assert!(futures::poll!(ask.as_mut()).is_pending());
            if let Ok(ping) = sent.try_recv() {
                break ping;
            }
        };
        tokio::time::advance(SHORT_TIMEOUT).await;
        while errors_of(&dht, next.id) == 1 {
            assert!(futures::poll!(ask.as_mut()).is_pending());
        }
        assert_eq!(errors_of(&dht, next.id), 2);
        reply(&dht, &ping, next.id, &[], &[]);
        while futures::poll!(ask.as_mut()).is_pending() {}
        assert_eq!(errors_of(&dht, next.id), 0);
    }

    // A node of the table, which has answered, is the one candidate of a
    // search the rate limiter holds, and the search is dropped before the
    // query goes out: the table counts no failure, and the node is still one
    // we can count on.
    #[tokio::test(start_paused = true)]
    async fn test_a_query_dropped_before_it_is_sent_fails_no_table_node() {
        let (dht, mut sent) = fixture_dht_limited();
        let id = crate::core::hash_id::Id20::new([0x21; 20]);
        let addr = SocketAddr::from(([10, 0, 1, 1], 6881));
        {
            let mut table = dht.routing_table_v4.write();
            table.add_node(id, addr);
            assert!(table.mark_response(&id, std::time::Instant::now()));
        }
        {
            let req = get_peers_search(&dht);
            let mut search = std::pin::pin!(req.search(true, std::iter::once((Some(id), addr))));
            while dht.get_stats().outstanding_requests == 0 {
                assert!(futures::poll!(search.as_mut()).is_pending());
            }
        }
        assert_eq!(dht.get_stats().outstanding_requests, 0);
        assert!(sent.try_recv().is_err(), "sent past an empty limiter");
        let table = dht.routing_table_v4.read();
        let node = table.iter().find(|n| n.id() == id).unwrap();
        assert_eq!(node.errors_in_a_row(), 0);
        assert!(node.is_confirmed(std::time::Instant::now()));
    }

    // A search's one candidate, a node of the table that has answered
    // before, stays silent: the table counts one failure at SHORT_TIMEOUT
    // and a second at RESPONSE_TIMEOUT, both from the send.
    #[tokio::test(start_paused = true)]
    async fn test_a_silent_search_candidate_fails_at_both_timeouts() {
        use super::SHORT_TIMEOUT;
        use crate::dht::RESPONSE_TIMEOUT;

        let (dht, mut sent) = fixture_dht();
        let id = crate::core::hash_id::Id20::new([0x21; 20]);
        let addr = SocketAddr::from(([10, 0, 1, 1], 6881));
        {
            let mut table = dht.routing_table_v4.write();
            table.add_node(id, addr);
            assert!(table.mark_response(&id, std::time::Instant::now()));
        }
        let req = get_peers_search(&dht);
        let mut search = std::pin::pin!(req.search(true, std::iter::once((Some(id), addr))));
        loop {
            assert!(futures::poll!(search.as_mut()).is_pending());
            if sent.try_recv().is_ok() {
                break;
            }
        }
        assert_eq!(errors_of(&dht, id), 0);
        tokio::time::advance(SHORT_TIMEOUT).await;
        while errors_of(&dht, id) == 0 {
            assert!(futures::poll!(search.as_mut()).is_pending());
        }
        tokio::time::advance(RESPONSE_TIMEOUT - SHORT_TIMEOUT).await;
        let outcome = loop {
            if let std::task::Poll::Ready(outcome) = futures::poll!(search.as_mut()) {
                break outcome.unwrap();
            }
        };
        assert_eq!((outcome.answered, outcome.failed), (0, 1));
        assert_eq!(errors_of(&dht, id), 2);
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

    // A node that answers every query we send it, as fast as our send window
    // lets us ask one node, keeps being heard: its answers are counted, and
    // they stay under the limit.
    #[tokio::test(start_paused = true)]
    async fn test_a_node_answering_our_queries_is_never_ignored() {
        use crate::dht::{
            query_limiter::{QUERY_LIMIT, WINDOW},
            send_windows::SEND_LIMIT,
        };
        use std::task::Poll;

        let (dht, mut sent) = fixture_dht();
        let node = SocketAddr::from(([10, 0, 0, 1], 6881));
        // SEND_LIMIT a window, evenly spaced, for ten windows. The spacing is
        // rounded up, so no window holds more than SEND_LIMIT.
        let per_window = u32::try_from(SEND_LIMIT).unwrap();
        assert!(per_window < QUERY_LIMIT);
        let step = WINDOW / per_window + Duration::from_nanos(1);
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
            tokio::time::advance(step).await;
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

    // `traversal_algorithm::start` runs for a refresh too: a walk for the
    // table, a find_node from the table's nodes nearest its target, that
    // finds fewer than three nodes that
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
                &std::sync::atomic::AtomicUsize::new(1),
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
    // the search runs; at its end, the closest nodes that had answered with
    // a token by then, up to k, are announced to, with their token.
    #[tokio::test(start_paused = true)]
    async fn test_a_search_announces_to_the_closest_nodes_that_answered_with_a_token_when_it_ends()
    {
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
        // The search ends as the eight closest have answered, before it
        // sends to 10 and 11, which it would have asked next.
        assert_eq!(asked, (0..10).collect::<Vec<_>>());

        let mut announced = BTreeMap::new();
        while let Ok(request) = sent.try_recv() {
            let MessageKind::AnnouncePeer(a) = request.message.kind else {
                panic!("not an announce: {:?}", request.message.kind);
            };
            assert_eq!((a.info_hash, a.port), (info_hash, 6881));
            announced.insert(index(request.addr), a.token.as_ref().to_vec());
        }
        // It announces as it ends, to the token holders among the nodes that
        // had answered by then: 8 and 9 answer after the eight closest have,
        // and the search no longer waits for them (libtorrent
        // `find_data::done`, find_data.cpp:139-194, v2.0.15).
        assert_eq!(
            announced,
            [0, 2, 4, 5, 6, 7]
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
    // nodes that answered (128, 64, 32, 16, then 8: see
    // `routing_table::bucket_limit`), each node in its own /24.
    fn fill_deep_table(
        dht: &DhtState,
        buckets: u8,
    ) -> Vec<(crate::core::hash_id::Id20, SocketAddr)> {
        let mut table = dht.routing_table_v4.write();
        let mut nodes = Vec::new();
        for bit in 0..=buckets {
            let full = crate::dht::routing_table::bucket_limit(usize::from(bit));
            for n in 0..u8::try_from(full).unwrap() {
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
                let full = crate::dht::routing_table::bucket_limit(usize::from(bit));
                for n in 0..u8::try_from(full).unwrap() {
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
            for (id, addr) in &nodes {
                table.mark_outgoing_request(id, std::time::Instant::now());
                table.mark_error(id, *addr);
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
            [0, 20, 45, 80, 135, 230, 405, 740, 1355, 1970, 2585, 3200]
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
            dht.started_for_test();
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
            [0, 20, 45, 80, 135, 230, 405, 740, 1355, 1970, 2585, 3200]
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
        // The router names node 1 from 20 s, node 2 too at 55 s, the fourth
        // round after it: the table grows at those two moments only.
        let first_growth = Duration::from_secs(20);
        let second_growth = Duration::from_secs(55);
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

        // The first round of the refresh times out; the second, at 20 s,
        // grows the table.
        assert_eq!(first_growth, RESPONSE_TIMEOUT + super::BOOTSTRAP_RETRY_MIN);
        // Each moment's searches start as the query budget lets them in,
        // two tokens each (the search let in and its query to the router):
        // all within 100 ms of it.
        let moments = [Duration::ZERO, EARLY_SEARCH_MIN, second_growth];
        let mut by_moment = BTreeMap::<Duration, usize>::new();
        for (at, n) in &searched {
            let moment = *moments.iter().rev().find(|m| *m <= at).unwrap();
            assert!(*at - moment < Duration::from_millis(100), "{searched:?}");
            *by_moment.entry(moment).or_default() += n;
        }
        assert_eq!(
            by_moment,
            BTreeMap::from([
                (Duration::ZERO, 20),
                (EARLY_SEARCH_MIN, EARLY_SEARCHES_PER_GROWTH),
                (second_growth, 20 - EARLY_SEARCHES_PER_GROWTH),
            ])
        );
    }

    // A self-refresh round that walks for longer than EARLY_SEARCH_MIN: the
    // router answers it ROUTER_DELAY late, naming two nodes near our id that
    // answer at once and 98 farther ones, each in its own /24, that never
    // answer, so the round asks them five at a time and one more for each
    // late one, and waits on each for RESPONSE_TIMEOUT. The torrent's first
    // search, asked only of the router, is refused at once. The two nodes
    // enter the table as the router answers, and the torrent searches again
    // EARLY_SEARCH_MIN after its first search ended, from them, while the
    // round is still waiting on silent nodes.
    #[tokio::test(start_paused = true)]
    async fn test_a_short_search_searches_again_before_the_round_that_grew_the_table_ends() {
        use super::EARLY_SEARCH_MIN;
        use crate::core::hash_id::Id20;
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
        let is_silent = |addr: SocketAddr| match addr.ip() {
            std::net::IpAddr::V4(a) => a.octets()[..2] == [10, 1],
            std::net::IpAddr::V6(_) => false,
        };
        let silent: Vec<(Id20, std::net::SocketAddrV4)> = (1..=98u8)
            .map(|i| {
                let mut id = [0xc0; 20];
                id[19] = i;
                (
                    Id20::new(id),
                    std::net::SocketAddrV4::new([10, 1, i, 1].into(), 6881),
                )
            })
            .collect();
        const ROUTER_DELAY: Duration = Duration::from_secs(5);
        let named: Vec<_> = answering.iter().chain(&silent).copied().collect();
        let id_of = |addr: SocketAddr| {
            answering
                .iter()
                .find(|n| SocketAddr::from(n.1) == addr)
                .map(|n| n.0)
        };
        let peer = SocketAddr::from(([10, 9, 9, 9], 51413));
        let info_hash = Id20::new([0x22; 20]);
        assert!(super::announce_slot(&info_hash) > EARLY_SEARCH_MIN);
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
        // The router's answer to the round, until ROUTER_DELAY.
        let mut held = None;
        let (found, found_at) = loop {
            let request = match first.next() {
                Some(request) => request,
                None => tokio::select! {
                    biased;
                    found = peers.recv() => {
                        break (found.unwrap(), tokio::time::Instant::now() - start);
                    }
                    () = super::sleep_until_some(held.as_ref().map(|_| start + ROUTER_DELAY)) => {
                        let request = held.take().unwrap();
                        reply(&dht, &request, Id20::new([0x99; 20]), &named, &[]);
                        continue;
                    }
                    request = sent.recv() => request.unwrap(),
                },
            };
            match request.message.kind {
                MessageKind::FindNodeRequest(_) if request.addr == router => {
                    assert!(held.replace(request).is_none());
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
                        refuse(&dht, &request);
                    } else {
                        let id = id_of(request.addr).unwrap();
                        reply(&dht, &request, id, &[], &[peer]);
                    }
                }
                ref kind => panic!("unexpected {kind:?}"),
            }
        };
        task.abort();
        // The round is still waiting on silent nodes: it has not ended.
        let waiting = dht
            .inflight_by_transaction_id
            .iter()
            .filter(|e| is_silent(e.key().1))
            .count();
        refresh.abort();

        assert_eq!(found, peer);
        assert_eq!(found_at, EARLY_SEARCH_MIN);
        assert_eq!(
            searched,
            BTreeMap::from([(Duration::ZERO, 1), (EARLY_SEARCH_MIN, 1)])
        );
        assert!(waiting > 0);
        assert!(silent_asked > 0);
        assert_eq!(dht.routing_table_v4.read().len(), answering.len());
    }

    // The routers of a fresh DHT as they were met on a public swarm: one
    // never answers a get_peers, though it names nodes to a find_node; the
    // other names one node that never answers, eight times over, to
    // anything. The torrent's first search, made before any node answered,
    // has only the routers and that silent node to ask, and ends short as
    // their timeouts run out. The bootstrap has by then put
    // SEARCH_MIN_SEEDS nodes that answer in the table, so the search is made
    // again at once, from them, not EARLY_SEARCH_MIN later, and finds the
    // peer; the round that seeded the table is still asking silent nodes.
    #[tokio::test(start_paused = true)]
    async fn test_a_first_search_made_from_the_routers_alone_searches_again_once_the_table_is_seeded()
     {
        use super::{EARLY_SEARCH_MIN, SEARCH_MIN_SEEDS};
        use crate::core::hash_id::Id20;
        use crate::dht::RESPONSE_TIMEOUT;
        use std::collections::BTreeMap;

        let seeds_u8 = u8::try_from(SEARCH_MIN_SEEDS).unwrap();
        let (dht, mut sent) = fixture_dht();
        let router = SocketAddr::from(ROUTER);
        let echo = SocketAddr::from(([10, 200, 0, 2], 6881));
        dht.add_routers(&[router, echo]);
        let node = |id: [u8; 20], net: u8, i: u8| {
            (
                Id20::new(id),
                std::net::SocketAddrV4::new([10, net, i, 1].into(), 6881),
            )
        };
        // The nodes that answer are near our own id, so the round that
        // looks for it asks them first.
        let answering: Vec<_> = (1..=seeds_u8)
            .map(|i| {
                let mut id = dht.id.0;
                id[19] ^= i;
                node(id, 0, i)
            })
            .collect();
        let silent: Vec<_> = (1..=40u8)
            .map(|i| {
                let mut id = [0xc0; 20];
                id[19] = i;
                node(id, 1, i)
            })
            .collect();
        let echoed = vec![node([0xa0; 20], 2, 1); 8];
        let named: Vec<_> = answering.iter().chain(&silent).copied().collect();
        let id_of = |addr: SocketAddr| {
            answering
                .iter()
                .find(|n| SocketAddr::from(n.1) == addr)
                .map(|n| n.0)
        };
        let peer = SocketAddr::from(([10, 9, 9, 9], 51413));
        let info_hash = Id20::new([0x22; 20]);
        assert!(super::announce_slot(&info_hash) > RESPONSE_TIMEOUT + EARLY_SEARCH_MIN);
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

        // Nothing answers until the round and the search have both asked
        // both routers.
        let mut first = Vec::new();
        while first.len() < 4 {
            let request = sent.recv().await.unwrap();
            assert!(request.addr == router || request.addr == echo);
            first.push(request);
        }
        let mut first = first.into_iter();
        // Each get_peers by when it was sent and to whom.
        let mut searched = BTreeMap::<Duration, Vec<SocketAddr>>::new();
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
                MessageKind::FindNodeRequest(_) if request.addr == echo => {
                    reply(&dht, &request, Id20::new([0x98; 20]), &echoed, &[]);
                }
                MessageKind::FindNodeRequest(_) if request.addr == router => {
                    reply(&dht, &request, Id20::new([0x99; 20]), &named, &[]);
                }
                MessageKind::FindNodeRequest(_) => match id_of(request.addr) {
                    Some(id) => reply(&dht, &request, id, &[], &[]),
                    None => silent_asked += 1,
                },
                MessageKind::GetPeersRequest(_) => {
                    searched
                        .entry(tokio::time::Instant::now() - start)
                        .or_default()
                        .push(request.addr);
                    if request.addr == echo {
                        reply(&dht, &request, Id20::new([0x98; 20]), &echoed, &[]);
                    } else if let Some(id) = id_of(request.addr) {
                        reply(&dht, &request, id, &[], &[peer]);
                    }
                }
                ref kind => panic!("unexpected {kind:?}"),
            }
        };
        task.abort();
        // The round that seeded the table is still waiting on silent nodes.
        let waiting = dht
            .inflight_by_transaction_id
            .iter()
            .filter(|e| silent.iter().any(|n| SocketAddr::from(n.1) == e.key().1))
            .count();
        assert!(waiting > 0);
        assert!(silent_asked > 0);
        refresh.abort();

        assert_eq!(found, peer);
        assert_eq!(found_at, RESPONSE_TIMEOUT);
        let mut at_start = searched[&Duration::ZERO].clone();
        at_start.sort();
        let mut leaned = vec![router, echo, SocketAddr::from(echoed[0].1)];
        leaned.sort();
        assert_eq!(at_start, leaned);
        // Made again from the table alone: it no longer leans on the routers.
        let mut again = searched[&RESPONSE_TIMEOUT].clone();
        again.sort();
        let mut seeds: Vec<_> = answering.iter().map(|n| SocketAddr::from(n.1)).collect();
        seeds.sort();
        assert_eq!(again, seeds);
        assert_eq!(searched.len(), 2);
    }

    // After a restart the table holds SEARCH_MIN_SEEDS nodes from the last
    // run, none of which has answered yet, and a router that never does.
    // The first search leans on the router, and the nodes answer it, which
    // seeds the table without any node entering it. The search ends short
    // as the router's query times out, and is made again at once, from the
    // nodes alone.
    #[tokio::test(start_paused = true)]
    async fn test_a_restored_table_that_becomes_seeded_without_growing_searches_again_at_once() {
        use super::{EARLY_SEARCH_MIN, SEARCH_MIN_SEEDS};
        use crate::core::hash_id::Id20;
        use crate::dht::RESPONSE_TIMEOUT;

        let (dht, mut sent) = fixture_dht();
        let router = SocketAddr::from(ROUTER);
        dht.add_routers(&[router]);
        let restored: Vec<_> = (1..=u8::try_from(SEARCH_MIN_SEEDS).unwrap())
            .map(|i| node_at(u32::from(i), [10, 0, i, 1]))
            .collect();
        {
            let mut table = dht.routing_table_v4.write();
            for (id, addr) in &restored {
                table.add_node(*id, SocketAddr::V4(*addr));
            }
            assert!(!table.is_seeded());
        }
        let growth = {
            let dht = dht.clone();
            tokio::spawn(async move { dht.watch_growth(true).await })
        };
        let info_hash = Id20::new([0x22; 20]);
        assert!(super::announce_slot(&info_hash) > RESPONSE_TIMEOUT + EARLY_SEARCH_MIN);
        let req = Arc::new(super::RecursiveRequest::get_peers(
            dht.clone(),
            info_hash,
            None,
            tokio::sync::mpsc::unbounded_channel().0,
        ));
        let start = tokio::time::Instant::now();
        let task = req.request_peers_forever(true);

        let mut searched = Vec::new();
        while searched.len() < 2 * SEARCH_MIN_SEEDS + 1 {
            let request = sent.recv().await.unwrap();
            assert!(matches!(
                request.message.kind,
                MessageKind::GetPeersRequest(_)
            ));
            searched.push((tokio::time::Instant::now() - start, request.addr));
            if let Some((id, _)) = restored
                .iter()
                .find(|n| SocketAddr::V4(n.1) == request.addr)
            {
                reply(&dht, &request, *id, &[], &[]);
            }
        }
        task.abort();
        growth.abort();
        assert!(dht.routing_table_v4.read().is_seeded());
        let at = |when: Duration| {
            let mut addrs: Vec<_> = searched
                .iter()
                .filter(|s| s.0 == when)
                .map(|s| s.1)
                .collect();
            addrs.sort();
            addrs
        };
        let mut nodes: Vec<_> = restored.iter().map(|n| SocketAddr::V4(n.1)).collect();
        nodes.sort();
        let mut leaned = nodes.clone();
        leaned.push(router);
        leaned.sort();
        assert_eq!(at(Duration::ZERO), leaned);
        assert_eq!(at(RESPONSE_TIMEOUT), nodes);
    }

    // 200 torrents start on a restored table of SEARCH_MIN_SEEDS nodes and a
    // router, none of which answers. Every first search leans on the router,
    // ends short and waits to be made again once the table is seeded; the
    // three nodes, which never answered, leave the table at their first
    // failure. Then the three nodes answer and enter it again, which seeds
    // the table. Inside every limit:
    // - that growth lets EARLY_SEARCHES_PER_GROWTH torrents search again, and
    //   no more come before the next growth;
    // - the searches walking at once and the queries outstanding stay inside
    //   what the query budget allows (see `DhtState::admit_walk`): with 20 a
    //   second, no more than 20 + 20 * 30 = 620 walking, and no more than
    //   20 + 20 * 15 = 320 sent and in flight, with the four of the search
    //   let in last still waiting to be sent; the second searches, at most
    //   EARLY_SEARCHES_PER_GROWTH, have three or four each;
    // - every query waits on the global rate limiter, here 20 a second with
    //   a burst of 20, so by any time t no more than 20 + t / 50 ms went;
    // - no address is sent more than SEND_LIMIT in any WINDOW.
    #[tokio::test(start_paused = true)]
    async fn test_torrents_waiting_for_a_seeded_table_search_again_inside_every_limit() {
        use super::{EARLY_SEARCHES_PER_GROWTH, RateLimiter, SEARCH_MIN_SEEDS, TokioClock};
        use crate::core::hash_id::Id20;
        use crate::dht::send_windows::SEND_LIMIT;
        use std::{
            collections::{BTreeSet, HashMap},
            num::NonZeroU32,
        };

        const TORRENTS: u16 = 200;
        let (mut state, mut sent) = fixture_state();
        let start = tokio::time::Instant::now();
        let per_second = 20;
        state.rate_limiter = RateLimiter::direct_with_clock(
            governor::Quota::per_second(NonZeroU32::new(per_second).unwrap()),
            TokioClock { start },
        );
        let dht = Arc::new(state);
        let router = SocketAddr::from(ROUTER);
        dht.add_routers(&[router]);
        let restored: Vec<_> = (1..=u8::try_from(SEARCH_MIN_SEEDS).unwrap())
            .map(|i| node_at(u32::from(i), [10, 0, i, 1]))
            .collect();
        {
            let mut table = dht.routing_table_v4.write();
            for (id, addr) in &restored {
                table.add_node(*id, SocketAddr::V4(*addr));
            }
            assert!(!table.is_seeded());
        }
        let growth = {
            let dht = dht.clone();
            tokio::spawn(async move { dht.watch_growth(true).await })
        };
        let tasks: Vec<_> = (0..TORRENTS)
            .map(|i| {
                let mut hash = [0xf0; 20];
                hash[1..3].copy_from_slice(&i.to_be_bytes());
                let info_hash = Id20::new(hash);
                // The torrent's own turn is far past this test.
                assert!(super::announce_slot(&info_hash) > Duration::from_secs(600));
                Arc::new(super::RecursiveRequest::get_peers(
                    dht.clone(),
                    info_hash,
                    None,
                    tokio::sync::mpsc::unbounded_channel().0,
                ))
                .request_peers_forever(true)
            })
            .collect();
        let record = |request: super::WorkerSendRequest| {
            let MessageKind::GetPeersRequest(g) = request.message.kind else {
                panic!("not a get_peers: {:?}", request.message.kind);
            };
            (tokio::time::Instant::now(), request.addr, g.info_hash)
        };
        let walks_ended = || {
            dht.get_stats().outstanding_requests == 0
                && dht
                    .walk_admission
                    .open
                    .load(std::sync::atomic::Ordering::Acquire)
                    == 0
                && dht.walk_admission.door.available_permits() == 1
        };
        let open = || {
            dht.walk_admission
                .open
                .load(std::sync::atomic::Ordering::Acquire)
        };
        let mut most_open = 0;

        // The first searches: the first ones ask the three nodes and the
        // router. The three, never answered, fail at their first short
        // timeout and leave the table, so the later ones ask the router
        // alone, until the router's send window is full: a search whose
        // router waits on it past RESPONSE_TIMEOUT ends with nothing sent.
        // The phase is over once every torrent's search was let in and
        // ended: none waits at the door and none walks.
        let mut first = Vec::new();
        let mut most_in_flight = 0;
        // Each query is taken as it is sent, so its time is when it went.
        loop {
            tokio::select! {
                biased;
                Some(request) = sent.recv() => first.push(record(request)),
                () = tokio::time::sleep(Duration::from_millis(50)) => {
                    if walks_ended() {
                        break;
                    }
                }
            }
            most_in_flight = most_in_flight.max(dht.get_stats().outstanding_requests);
            most_open = most_open.max(open());
        }
        // The budget held them, not a count: far more than
        // EARLY_SEARCHES_PER_GROWTH walked at once.
        assert!(most_open > EARLY_SEARCHES_PER_GROWTH, "{most_open}");
        assert!(most_open <= 20 + 20 * 30, "{most_open}");
        assert!(most_in_flight <= 20 + 20 * 15 + 4, "{most_in_flight}");
        assert_eq!(dht.routing_table_v4.read().len(), 0);

        // The three nodes answer, and enter the table again: it is seeded,
        // which is a growth.
        let seeded_at = tokio::time::Instant::now();
        {
            let mut table = dht.routing_table_v4.write();
            for (id, addr) in &restored {
                table.add_node(*id, SocketAddr::V4(*addr));
                assert!(table.mark_response(id, std::time::Instant::now()));
            }
            assert!(table.is_seeded());
        }
        let mut again = Vec::new();
        let mut most_in_flight = 0;
        while tokio::time::Instant::now() < seeded_at + Duration::from_secs(60) {
            tokio::time::advance(Duration::from_millis(50)).await;
            while let Ok(request) = sent.try_recv() {
                again.push(record(request));
            }
            most_in_flight = most_in_flight.max(dht.get_stats().outstanding_requests);
        }
        assert!(walks_ended());
        growth.abort();
        for task in tasks {
            task.abort();
        }

        // EARLY_SEARCHES_PER_GROWTH torrents searched again, each from the
        // three nodes: alone for the ones let in at the growth, and with the
        // router for the ones let in after the three, silent again, failed
        // their first queries and the table no longer counted on them.
        let mut by_torrent: HashMap<Id20, BTreeSet<SocketAddr>> = HashMap::new();
        for (_, addr, info_hash) in &again {
            assert!(by_torrent.entry(*info_hash).or_default().insert(*addr));
        }
        assert_eq!(by_torrent.len(), EARLY_SEARCHES_PER_GROWTH);
        let nodes: BTreeSet<_> = restored.iter().map(|n| SocketAddr::V4(n.1)).collect();
        let with_router: BTreeSet<_> = nodes.iter().copied().chain([router]).collect();
        assert!(
            by_torrent
                .values()
                .all(|asked| *asked == nodes || *asked == with_router)
        );
        let at_growth: BTreeSet<_> = again
            .iter()
            .filter(|s| s.0 == again[0].0)
            .map(|s| s.2)
            .collect();
        assert!(!at_growth.is_empty());
        assert!(at_growth.iter().all(|t| by_torrent[t] == nodes));
        assert!(most_in_flight <= 4 * EARLY_SEARCHES_PER_GROWTH);

        // The global rate limiter, over both phases: every query sent and
        // every search let in spent a token, and each torrent seen sending
        // in a phase was let in once in it before its first query. It held
        // them back: some went the moment its bucket allowed.
        let all = first
            .iter()
            .map(|s| (0, s))
            .chain(again.iter().map(|s| (1, s)));
        let mut let_in = BTreeSet::new();
        let mut held = 0;
        let mut times = Vec::new();
        for (k, (phase, (at, addr, info_hash))) in all.enumerate() {
            let_in.insert((phase, *info_hash));
            let spent = (k + 1 + let_in.len()) as u128;
            let tokens =
                per_second as u128 + (*at - start).as_millis() / (1000 / per_second as u128);
            assert!(spent <= tokens, "query {k} at {:?}", *at - start);
            held += usize::from(spent == tokens);
            times.push((*at, *addr));
        }
        assert!(held > 0);
        // Each address's send window.
        for addr in nodes.iter().chain([&router]) {
            let times: Vec<_> = times.iter().filter(|s| s.1 == *addr).map(|s| s.0).collect();
            assert!(most_in_any_window(&times) <= SEND_LIMIT);
        }
    }

    // A torrent's peer search, for `info_hash`, that never yields peers.
    fn peer_search(
        dht: &Arc<DhtState>,
        info_hash: crate::core::hash_id::Id20,
    ) -> tokio::task::JoinHandle<()> {
        Arc::new(super::RecursiveRequest::get_peers(
            dht.clone(),
            info_hash,
            None,
            tokio::sync::mpsc::unbounded_channel().0,
        ))
        .request_peers_forever(true)
    }

    fn walking(dht: &DhtState) -> usize {
        dht.walk_admission
            .open
            .load(std::sync::atomic::Ordering::Acquire)
    }

    // A lone torrent added to an idle DHT walks at once: its first query
    // goes out the moment it asks for peers.
    #[tokio::test(start_paused = true)]
    async fn test_a_lone_torrent_walks_at_once() {
        let (dht, mut sent) = fixture_dht();
        let (id, addr) = node_at(1, [10, 0, 1, 1]);
        dht.routing_table_v4
            .write()
            .add_node(id, SocketAddr::V4(addr));
        let start = tokio::time::Instant::now();
        let task = peer_search(&dht, crate::core::hash_id::Id20::new([0xf0; 20]));
        let request = sent.recv().await.unwrap();
        assert_eq!(request.addr, SocketAddr::V4(addr));
        assert_eq!(tokio::time::Instant::now(), start);
        assert_eq!(walking(&dht), 1);
        task.abort();
    }

    // Sixteen torrents walk with the send window of every node they would
    // ask full: each waits on the windows, with no query on the rate
    // limiter. A seventeenth, whose nodes have room, is let in and sends at
    // once: a walk that can only wait holds no one back.
    #[tokio::test(start_paused = true)]
    async fn test_walks_that_can_only_wait_hold_no_one_back() {
        use crate::dht::send_windows::SEND_LIMIT;

        let (dht, mut sent) = fixture_dht();
        // Two groups of eight nodes, far apart: each torrent's eight closest
        // are one group.
        let a = |j: u8| node_at(u32::from(j), [10, 1, j, 1]);
        let b = |j: u8| node_at(0x8000_0000 | u32::from(j), [10, 2, j, 1]);
        {
            let mut table = dht.routing_table_v4.write();
            for j in 0..8 {
                for (id, addr) in [a(j), b(j)] {
                    table.add_node(id, SocketAddr::V4(addr));
                }
            }
            assert_eq!(table.len(), 16);
        }
        for j in 0..8 {
            for _ in 0..SEND_LIMIT {
                dht.send_guard.record(std::net::IpAddr::V4(*a(j).1.ip()));
            }
        }
        let start = tokio::time::Instant::now();
        let mut tasks: Vec<_> = (0..16)
            .map(|i| peer_search(&dht, node_at(1000 + i, [0; 4]).0))
            .collect();
        while walking(&dht) < 16 {
            tokio::task::yield_now().await;
        }
        assert!(sent.try_recv().is_err());
        tasks.push(peer_search(&dht, node_at(0x8000_0000 | 1000, [0; 4]).0));
        let request = sent.recv().await.unwrap();
        assert_eq!(tokio::time::Instant::now(), start);
        assert!((0..8).any(|j| request.addr == SocketAddr::V4(b(j).1)));
        assert_eq!(walking(&dht), 17);
        for task in tasks {
            task.abort();
        }
    }

    // Three torrents ask for peers in turn, with the rate limiter at 5 a
    // second and a burst of 5, on a table of five nodes that answer every
    // query naming nothing: each walk asks the five and ends as they
    // answer. The first is let in and sends four at once; its fifth waits
    // on the limiter, so the second and third wait too. Each is let in only
    // once every query of the walks before it was sent, and in the order
    // they asked; every search let in and every query sent spends a token.
    #[tokio::test(start_paused = true)]
    async fn test_walks_are_let_in_in_order_once_the_open_ones_have_sent() {
        use super::{RateLimiter, TokioClock};
        use std::{collections::HashMap, num::NonZeroU32};

        let (mut state, mut sent) = fixture_state();
        let start = tokio::time::Instant::now();
        state.rate_limiter = RateLimiter::direct_with_clock(
            governor::Quota::per_second(NonZeroU32::new(5).unwrap()),
            TokioClock { start },
        );
        let dht = Arc::new(state);
        let nodes: HashMap<SocketAddr, crate::core::hash_id::Id20> = (0..5u8)
            .map(|j| {
                let (id, addr) = node_at(u32::from(j), [10, 1, j, 1]);
                (SocketAddr::V4(addr), id)
            })
            .collect();
        {
            let mut table = dht.routing_table_v4.write();
            for (addr, id) in &nodes {
                table.add_node(*id, *addr);
            }
        }
        let hashes: Vec<_> = (0..3).map(|i| node_at(1000 + i, [0; 4]).0).collect();
        let tasks: Vec<_> = hashes.iter().map(|h| peer_search(&dht, *h)).collect();
        let mut sends = Vec::new();
        while sends.len() < 15 {
            let request = sent.recv().await.unwrap();
            let MessageKind::GetPeersRequest(g) = &request.message.kind else {
                panic!("not a get_peers: {:?}", request.message.kind);
            };
            sends.push((tokio::time::Instant::now(), g.info_hash));
            reply(&dht, &request, nodes[&request.addr], &[], &[]);
        }
        let order: Vec<_> = sends.iter().map(|s| s.1).collect();
        let expected: Vec<_> = hashes.iter().flat_map(|h| [*h; 5]).collect();
        assert_eq!(order, expected);
        for (k, (at, _)) in sends.iter().enumerate() {
            let spent = (k + 1 + k / 5 + 1) as u128;
            assert!(spent <= 5 + (*at - start).as_millis() / 200, "query {k}");
        }
        for task in tasks {
            task.abort();
        }
    }

    // 10,000 torrents ask for peers at once, with the default query budget
    // (250 a second, 25 to start with) and one router that never answers.
    // The router's send window fills, so most searches have nothing to
    // send and end RESPONSE_TIMEOUT after they were let in. Every search let
    // in spends a token, so by any time t no more than 25 + 250 t were, and
    // at no time do more walk, or more queries wait for an answer, than the
    // budget allows: 250 + 250 * 30 = 7750 walking, 250 + 250 * 15 = 4000 in
    // flight. A torrent not let in yet holds no search state: its
    // candidates are made as it is let in. Every torrent is let in, and
    // once the last search ends none walks.
    #[tokio::test(start_paused = true)]
    async fn test_ten_thousand_torrents_walk_inside_the_query_budget() {
        use crate::dht::send_windows::SEND_LIMIT;

        const TORRENTS: u32 = 10_000;
        let (dht, mut sent) = fixture_dht();
        let router = SocketAddr::from(ROUTER);
        dht.add_routers(&[router]);
        let start = tokio::time::Instant::now();
        let tasks: Vec<_> = (0..TORRENTS)
            .map(|i| {
                let mut hash = [0xf0; 20];
                hash[1..4].copy_from_slice(&i.to_be_bytes()[1..]);
                let info_hash = crate::core::hash_id::Id20::new(hash);
                // The torrent's own turn is far past this test.
                assert!(super::announce_slot(&info_hash) > Duration::from_secs(600));
                peer_search(&dht, info_hash)
            })
            .collect();
        let mut most_walking = 0;
        let mut sends = Vec::new();
        loop {
            tokio::time::advance(Duration::from_millis(100)).await;
            while let Ok(request) = sent.try_recv() {
                assert_eq!(request.addr, router);
                sends.push(tokio::time::Instant::now());
            }
            let t = (tokio::time::Instant::now() - start).as_millis();
            let now_walking = walking(&dht);
            most_walking = most_walking.max(now_walking);
            assert!(
                now_walking as u128 <= 25 + 250 * t / 1000,
                "{now_walking} at {t} ms"
            );
            assert!(now_walking <= 250 + 250 * 30, "{now_walking}");
            assert!(dht.get_stats().outstanding_requests <= 250 + 250 * 15);
            if now_walking == 0 && dht.walk_admission.door.available_permits() == 1 {
                break;
            }
        }
        assert!(most_walking > 1000, "{most_walking}");
        assert!(most_in_any_window(&sends) <= SEND_LIMIT);
        for task in tasks {
            task.abort();
        }
    }

    // Only the first search is made again once the table is seeded: the
    // routers name SEARCH_MIN_SEEDS nodes that answer a find_node but never
    // a get_peers, and a fourth that answers from the second round on. The
    // first search times out on the routers and is made again at once from
    // the three; that one times out on them too, and the next search waits
    // EARLY_SEARCH_MIN after it, as any short search does.
    #[tokio::test(start_paused = true)]
    async fn test_a_search_made_again_once_the_table_is_seeded_that_falls_short_waits_early_search_min()
     {
        use super::{EARLY_SEARCH_MIN, SEARCH_MIN_SEEDS};
        use crate::core::hash_id::Id20;
        use crate::dht::RESPONSE_TIMEOUT;
        use std::collections::BTreeSet;

        let seeds_u8 = u8::try_from(SEARCH_MIN_SEEDS).unwrap();
        let (dht, mut sent) = fixture_dht();
        let router = SocketAddr::from(ROUTER);
        dht.add_routers(&[router]);
        let node = |i: u8| {
            let mut id = [0x40; 20];
            id[19] = i;
            (
                Id20::new(id),
                std::net::SocketAddrV4::new([10, 0, i, 1].into(), 6881),
            )
        };
        let id_of = |addr: SocketAddr| {
            (1..=seeds_u8 + 1)
                .map(node)
                .find(|n| SocketAddr::from(n.1) == addr)
                .map(|n| n.0)
        };
        let info_hash = Id20::new([0x22; 20]);
        let ends = 2 * RESPONSE_TIMEOUT + EARLY_SEARCH_MIN;
        assert!(super::announce_slot(&info_hash) > ends);
        let req = Arc::new(super::RecursiveRequest::get_peers(
            dht.clone(),
            info_hash,
            None,
            tokio::sync::mpsc::unbounded_channel().0,
        ));
        let start = tokio::time::Instant::now();
        let refresh = tokio::spawn(dht.clone().refresh_self(true));
        let task = req.request_peers_forever(true);

        let mut first = Vec::new();
        while first.len() < 2 {
            let request = sent.recv().await.unwrap();
            assert_eq!(request.addr, router);
            first.push(request);
        }
        let mut first = first.into_iter();
        let mut searched = BTreeSet::<Duration>::new();
        loop {
            let request = match first.next() {
                Some(request) => request,
                None => sent.recv().await.unwrap(),
            };
            let at = tokio::time::Instant::now() - start;
            match request.message.kind {
                MessageKind::FindNodeRequest(_) if request.addr == router => {
                    let named: Vec<_> = (1..=seeds_u8).map(node).collect();
                    reply(&dht, &request, Id20::new([0x99; 20]), &named, &[]);
                }
                // From the round after the first search ended, the nodes
                // name a fourth: the table grows while the search made
                // again is walking.
                MessageKind::FindNodeRequest(_) => {
                    let named = if at > RESPONSE_TIMEOUT {
                        vec![node(seeds_u8 + 1)]
                    } else {
                        Vec::new()
                    };
                    reply(&dht, &request, id_of(request.addr).unwrap(), &named, &[]);
                }
                MessageKind::GetPeersRequest(_) => {
                    searched.insert(at);
                    if at >= ends {
                        break;
                    }
                }
                ref kind => panic!("unexpected {kind:?}"),
            }
        }
        task.abort();
        refresh.abort();

        assert_eq!(
            searched,
            BTreeSet::from([Duration::ZERO, RESPONSE_TIMEOUT, ends])
        );
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

        // The six farthest buckets' nodes enter at once.
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
        assert_eq!(
            dht.routing_table_v4.read().entries(),
            128 + 64 + 32 + 16 + 8 + 8
        );
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
        assert_eq!(
            dht.routing_table_v4.read().entries(),
            128 + 64 + 32 + 16 + 8 + 8 + 4 + 4
        );
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

    // A node that answers and takes the place of a bad one, in a full bucket
    // that cannot split, leaves the table as many nodes as before.
    #[test]
    fn test_a_node_taking_a_bad_ones_place_leaves_the_count_as_it_was() {
        use crate::dht::routing_table::InsertResult;

        let (dht, _sent) = fixture_state();
        // The farthest bucket full, and the next one, which holds our id.
        fill_deep_table(&dht, 1);
        let mut table = dht.routing_table_v4.write();
        let bad = id_in_bucket(dht.id, 0, 3);
        table.mark_outgoing_request(&bad, std::time::Instant::now());
        let addr = table.iter().find(|n| n.id() == bad).unwrap().addr();
        table.mark_error(&bad, addr);
        let full = 128 + 64;
        assert_eq!(table.len(), full);
        let new = id_in_bucket(dht.id, 0, 200);
        assert!(matches!(
            table.add_node(new, SocketAddr::from(([10, 1, 201, 1], 6881))),
            InsertResult::ReplacedBad(_)
        ));
        assert_eq!(table.iter().count(), full);
        assert_eq!(table.len(), full);
    }

    // A datagram too long for the buffer, which a socket carried through a
    // tunnel reports as InvalidData, costs that datagram only: the DHT reads
    // on and answers the next query.
    #[tokio::test]
    async fn test_a_datagram_that_does_not_fit_costs_only_that_datagram() {
        let (socket, incoming, mut sent) = queued_socket();
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

    type Incoming = std::io::Result<(Vec<u8>, SocketAddr)>;

    // A socket that reads what is sent into the returned sender, and sends
    // into the returned receiver.
    fn queued_socket() -> (
        Arc<dyn DatagramSocket>,
        tokio::sync::mpsc::UnboundedSender<Incoming>,
        tokio::sync::mpsc::UnboundedReceiver<Vec<u8>>,
    ) {
        use crate::core::net::BoxFuture;

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
        let (sent_tx, sent) = tokio::sync::mpsc::unbounded_channel();
        let socket = Arc::new(Queued {
            incoming: tokio::sync::Mutex::new(incoming_rx),
            sent: sent_tx,
        });
        (socket, incoming, sent)
    }

    // A dual-stack socket reports an IPv4 node's reply as coming from
    // `[::ffff:a.b.c.d]`: it answers the request sent to `a.b.c.d`, and the
    // node enters the IPv4 table, not the IPv6 one.
    #[tokio::test]
    async fn test_a_reply_from_a_mapped_source_answers_the_ipv4_request() {
        use crate::core::hash_id::Id20;

        let (socket, incoming, mut sent) = queued_socket();
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

        let node = SocketAddr::from(([10, 0, 0, 2], 6881));
        let id = Id20::new([0x44; 20]);
        let entered = dht.with_routing_tables(|v4, _| v4.entered());
        let mut entered = std::pin::pin!(entered.notified());
        entered.as_mut().enable();
        dht.add_node(node);
        let ping = tokio::select! {
            ping = sent.recv() => ping.unwrap(),
            () = tasks.wait() => panic!("the DHT ended"),
        };
        let ping = bprotocol::deserialize_message::<ByteBufOwned>(&ping).unwrap();
        assert!(matches!(ping.kind, MessageKind::PingRequest(_)));
        let mut reply = Vec::new();
        bprotocol::serialize_message(
            &mut reply,
            ping.transaction_id,
            None,
            None,
            MessageKind::Response(bprotocol::Response {
                id,
                ..Default::default()
            }),
        )
        .unwrap();
        incoming
            .send(Ok((reply, "[::ffff:10.0.0.2]:6881".parse().unwrap())))
            .unwrap();
        tokio::select! {
            () = entered => {}
            () = tasks.wait() => panic!("the DHT ended"),
        }
        dht.with_routing_tables(|v4, v6| {
            assert!(v4.iter().any(|n| n.id() == id && n.addr() == node));
            assert_eq!(v6.len(), 0);
        });
        dht.cancellation_token().cancel();
    }

    // Every reply that answers a request feeds the table, a get_peers one
    // as a find_node one does (libtorrent rpc_manager.cpp:389, v2.0.15).
    #[tokio::test(start_paused = true)]
    async fn test_a_get_peers_reply_enters_the_table() {
        use crate::core::hash_id::Id20;

        let (dht, mut sent) = fixture_dht();
        let node = (
            Id20::new([0x21; 20]),
            SocketAddr::from(([10, 0, 1, 1], 6881)),
        );
        let req = get_peers_search(&dht);
        let mut search = std::pin::pin!(req.search(true, std::iter::once((Some(node.0), node.1))));
        let request = tokio::select! {
            request = sent.recv() => request.unwrap(),
            _ = search.as_mut() => panic!("the search ended unasked"),
        };
        assert_eq!(request.addr, node.1);
        answer(&dht, &request, node.0, Some(b"t"), &[]);
        let outcome = search.await.unwrap();
        assert_eq!(outcome.answered, 1);
        let table = dht.routing_table_v4.read();
        assert_eq!(table.len(), 1);
        assert!(
            table
                .iter()
                .any(|n| n.id() == node.0 && n.is_confirmed(std::time::Instant::now()))
        );
    }

    // An IPv6 search is never sent to an IPv4 address written as IPv6 that
    // a node names in `nodes6`, and neither table hears of it; the IPv6 node
    // named beside it is asked.
    #[tokio::test(start_paused = true)]
    async fn test_a_mapped_address_in_nodes6_is_never_asked() {
        use crate::core::hash_id::Id20;
        use std::net::SocketAddrV6;

        let (dht, mut sent) = fixture_dht();
        let first = (
            Id20::new([0x21; 20]),
            "[2001:db8:1::1]:6881".parse::<SocketAddr>().unwrap(),
        );
        let mapped: SocketAddrV6 = "[::ffff:5.6.7.8]:6881".parse().unwrap();
        let v6: SocketAddrV6 = "[2001:db8:2::1]:6881".parse().unwrap();
        let req = get_peers_search(&dht);
        let mut search =
            std::pin::pin!(req.search(false, std::iter::once((Some(first.0), first.1))));
        let request = tokio::select! {
            request = sent.recv() => request.unwrap(),
            _ = search.as_mut() => panic!("the search ended unasked"),
        };
        assert_eq!(request.addr, first.1);
        dht.on_received_message(
            bprotocol::Message {
                transaction_id: request.message.transaction_id.clone(),
                version: None,
                ip: None,
                kind: MessageKind::Response(bprotocol::Response {
                    id: first.0,
                    nodes6: Some(bprotocol::CompactNodeInfoOwned::new_from_iter(
                        [
                            bprotocol::Node {
                                id: Id20::new([0x22; 20]),
                                addr: mapped,
                            },
                            bprotocol::Node {
                                id: Id20::new([0x23; 20]),
                                addr: v6,
                            },
                        ]
                        .into_iter(),
                    )),
                    ..Default::default()
                }),
            },
            request.addr,
        )
        .unwrap();
        // The IPv6 node is asked; nothing else is, and the search ends as
        // it fails to answer.
        let mut asked = Vec::new();
        let outcome = loop {
            tokio::select! {
                request = sent.recv() => asked.push(request.unwrap().addr),
                outcome = search.as_mut() => break outcome.unwrap(),
            }
        };
        assert_eq!(asked, vec![SocketAddr::V6(v6)]);
        assert_eq!(outcome.answered, 1);
        dht.with_routing_tables(|v4, v6_table| {
            assert_eq!(v4.len() + v4.heard_of(), 0);
            assert!(
                v6_table
                    .iter()
                    .all(|n| !crate::dht::routing_table::is_v4_mapped(n.addr()))
            );
            assert_eq!(v6_table.heard_of(), 1);
        });
    }

    // A query from a node the table does not hold makes it heard of
    // (libtorrent node.cpp:831-832, v2.0.15); it does not enter the table.
    #[tokio::test(start_paused = true)]
    async fn test_a_query_from_an_unknown_node_is_heard_of() {
        use crate::core::hash_id::Id20;

        let (dht, mut sent) = fixture_dht();
        dht.on_received_message(
            bprotocol::Message {
                transaction_id: ByteBufOwned::from(&b"aa"[..]),
                version: None,
                ip: None,
                kind: MessageKind::PingRequest(bprotocol::PingRequest {
                    id: Id20::new([0x21; 20]),
                }),
            },
            SocketAddr::from(([10, 0, 1, 1], 6881)),
        )
        .unwrap();
        assert!(sent.recv().await.is_some());
        let table = dht.routing_table_v4.read();
        assert_eq!(table.len(), 0);
        assert_eq!(table.heard_of(), 1);
    }

    // libtorrent `node::tick` (node.cpp:647-716, v2.0.15): nodes heard of
    // are asked into the table one per REFRESH_SPACING, the first at
    // once, with a get_peers into their bucket. One that answers enters,
    // and the nodes it names are heard of and asked in turn; once none is
    // left, the tick refreshes the table's nodes. With an empty table and
    // nothing heard of, nothing is sent.
    #[tokio::test(start_paused = true)]
    async fn test_nodes_heard_of_are_asked_one_per_tick() {
        use super::REFRESH_SPACING;
        use std::net::SocketAddrV4;

        let (dht, mut sent) = fixture_dht();
        let task = tokio::spawn(dht.clone().refresh_table(true));
        tokio::time::sleep(Duration::from_secs(3600)).await;
        assert!(sent.try_recv().is_err(), "sent with nothing heard of");

        let start = tokio::time::Instant::now();
        let a = (
            id_in_bucket(dht.id, 0, 0),
            SocketAddrV4::new([10, 1, 1, 1].into(), 6881),
        );
        let b = (
            id_in_bucket(dht.id, 1, 0),
            SocketAddrV4::new([10, 2, 1, 1].into(), 6881),
        );
        let c = (
            id_in_bucket(dht.id, 2, 0),
            SocketAddrV4::new([10, 3, 1, 1].into(), 6881),
        );
        {
            let mut table = dht.routing_table_v4.write();
            table.heard_about(a.0, a.1.into());
            table.heard_about(b.0, b.1.into());
        }
        let first = sent.recv().await.unwrap();
        assert_eq!(tokio::time::Instant::now() - start, Duration::ZERO);
        assert_eq!(first.addr, SocketAddr::V4(a.1));
        answer(&dht, &first, a.0, None, &[c]);

        let second = sent.recv().await.unwrap();
        assert_eq!(tokio::time::Instant::now() - start, REFRESH_SPACING);
        assert_eq!(second.addr, SocketAddr::V4(b.1));
        assert!(matches!(
            second.message.kind,
            MessageKind::GetPeersRequest(_)
        ));
        {
            let table = dht.routing_table_v4.read();
            assert_eq!(table.len(), 1);
            assert!(table.iter().any(|n| n.id() == a.0));
        }

        let third = sent.recv().await.unwrap();
        assert_eq!(tokio::time::Instant::now() - start, 2 * REFRESH_SPACING);
        assert_eq!(third.addr, SocketAddr::V4(c.1));

        // b and c never answer. With no node heard of left to ask, the next
        // tick refreshes the node of the table, a.
        let fourth = sent.recv().await.unwrap();
        assert_eq!(tokio::time::Instant::now() - start, 3 * REFRESH_SPACING);
        assert_eq!(fourth.addr, SocketAddr::V4(a.1));
        assert!(matches!(
            fourth.message.kind,
            MessageKind::GetPeersRequest(_)
        ));
        assert_eq!(dht.routing_table_v4.read().len(), 1);
        task.abort();
    }

    // A restart whose saved nodes are all stale: the routers, known only
    // once the first round is under way, are asked in the round right after
    // it, and the nodes they name that answer enter the table.
    #[tokio::test(start_paused = true)]
    async fn test_a_restart_from_stale_nodes_asks_the_routers_that_resolve_later() {
        use crate::core::hash_id::Id20;
        use crate::dht::RESPONSE_TIMEOUT;
        use crate::dht::routing_table::RoutingTable;
        use std::net::SocketAddrV4;

        let (saved, _sent) = fixture_state();
        add_table_nodes(&saved, 2, true);
        let json = serde_json::to_string(&*saved.routing_table_v4.read()).unwrap();
        let restored: RoutingTable = serde_json::from_str(&json).unwrap();
        let stale: Vec<SocketAddr> = restored.iter().map(|n| n.addr()).collect();
        assert_eq!(stale.len(), 2);

        let (tx, mut sent) = tokio::sync::mpsc::unbounded_channel();
        let dht = std::sync::Arc::new(DhtState::new_internal(
            saved.id,
            tx,
            Some(restored),
            None,
            (Ipv4Addr::LOCALHOST, 0).into(),
            tokio_util::sync::CancellationToken::new(),
            None,
        ));
        dht.started_for_test();
        let start = tokio::time::Instant::now();
        let task = tokio::spawn(dht.clone().refresh_self(true));

        // Round one asks the saved nodes, which never answer; the router
        // becomes known while it waits on them.
        for _ in 0..2 {
            let request = sent.recv().await.unwrap();
            assert!(stale.contains(&request.addr));
        }
        let router = SocketAddr::from(ROUTER);
        dht.add_routers(&[router]);

        // Round two starts as round one ends and asks the router.
        let mut request = sent.recv().await.unwrap();
        while request.addr != router {
            assert!(stale.contains(&request.addr));
            request = sent.recv().await.unwrap();
        }
        assert_eq!(tokio::time::Instant::now() - start, RESPONSE_TIMEOUT);

        let named = [
            (
                Id20::new([0x31; 20]),
                SocketAddrV4::new([10, 0, 5, 1].into(), 6881),
            ),
            (
                Id20::new([0x32; 20]),
                SocketAddrV4::new([10, 0, 6, 1].into(), 6881),
            ),
        ];
        reply(&dht, &request, Id20::new([0x30; 20]), &named, &[]);
        let entered = dht.routing_table_v4.read().entered();
        let mut answered = 0;
        while answered < named.len() {
            let request = sent.recv().await.unwrap();
            if let Some(&(id, _)) = named.iter().find(|n| SocketAddr::V4(n.1) == request.addr) {
                let mut next = std::pin::pin!(entered.notified());
                next.as_mut().enable();
                reply(&dht, &request, id, &[], &[]);
                next.await;
                answered += 1;
            }
        }
        let table = dht.routing_table_v4.read();
        for (id, addr) in named {
            assert!(
                table
                    .iter()
                    .any(|n| n.id() == id && n.addr() == SocketAddr::V4(addr))
            );
        }
        drop(table);
        task.abort();
    }

    // The two routers of `cold_start`, and the node and peer the second one
    // leads to.
    const ROUTER_A: ([u8; 4], u16) = ([10, 200, 0, 1], 6881);
    const ROUTER_B: ([u8; 4], u16) = ([10, 200, 0, 2], 6881);
    const NODE_N: ([u8; 4], u16) = ([10, 0, 1, 1], 6881);
    const PEER_P: ([u8; 4], u16) = ([10, 9, 9, 9], 51413);

    // A fresh DHT's start on a fake network. Its bootstrap names are
    // `a.invalid` and `b.invalid`: A's resolves at once, B's `b_late` in, on
    // the paused clock. Router A never answers, or, when
    // `a_answers_first_only`, answers the first query it gets and no other.
    // Router B answers every query: a get_peers naming the node N, which
    // answers a get_peers with the peer P. The self-lookup and one torrent's
    // search run beside the bootstrap, as a session starts them together.
    // Returns the queries sent until P is found, each with when and what,
    // and when P was found.
    async fn cold_start(
        b_late: Duration,
        a_answers_first_only: bool,
    ) -> (Vec<(Duration, SocketAddr, &'static str)>, Duration) {
        use crate::core::hash_id::Id20;
        use crate::core::net::{BoxFuture, Resolved, Resolver};
        use std::sync::Arc;

        struct Names {
            start: tokio::time::Instant,
            b_late: Duration,
        }

        impl Resolver for Names {
            fn resolve<'a>(
                &'a self,
                host: &'a str,
                _port: u16,
            ) -> BoxFuture<'a, std::io::Result<Resolved>> {
                Box::pin(async move {
                    match host {
                        "a.invalid" => Ok(Resolved::Addresses(vec![ROUTER_A.into()])),
                        "b.invalid" => {
                            tokio::time::sleep_until(self.start + self.b_late).await;
                            Ok(Resolved::Addresses(vec![ROUTER_B.into()]))
                        }
                        host => panic!("unexpected name {host}"),
                    }
                })
            }
        }

        let (a, b, n) = (
            SocketAddr::from(ROUTER_A),
            SocketAddr::from(ROUTER_B),
            SocketAddr::from(NODE_N),
        );
        let peer = SocketAddr::from(PEER_P);
        let start = tokio::time::Instant::now();
        let (dht, mut sent) = unstarted_fixture_dht();
        let worker = Arc::new(super::DhtWorker {
            socket: crate::test_support::loopback_udp().await,
            resolver: Arc::new(Names { start, b_late }),
            dht: dht.clone(),
        });
        let bootstrap = tokio::spawn({
            let worker = worker.clone();
            async move {
                worker
                    .bootstrap(&["a.invalid:6881".into(), "b.invalid:6881".into()])
                    .await
            }
        });
        // The torrent's search is started ahead of the self-lookup: the order
        // its queries go out in is the DHT's, not the order the two began in.
        let (peer_tx, mut peers) = tokio::sync::mpsc::unbounded_channel();
        let search = Arc::new(super::RecursiveRequest::get_peers(
            dht.clone(),
            Id20::new([0x22; 20]),
            None,
            peer_tx,
        ))
        .request_peers_forever(true);
        let refresh = tokio::spawn(dht.clone().refresh_self(true));

        let mut queries = Vec::new();
        let mut a_answered = false;
        let found_at = loop {
            let request = tokio::select! {
                biased;
                found = peers.recv() => {
                    assert_eq!(found, Some(peer));
                    break tokio::time::Instant::now() - start;
                }
                request = sent.recv() => request.unwrap(),
            };
            let kind = match request.message.kind {
                MessageKind::FindNodeRequest(_) => "find_node",
                MessageKind::GetPeersRequest(_) => "get_peers",
                _ => "other",
            };
            queries.push((tokio::time::Instant::now() - start, request.addr, kind));
            match (request.addr, kind) {
                (addr, _) if addr == a => {
                    if a_answers_first_only && !a_answered {
                        a_answered = true;
                        reply(&dht, &request, Id20::new([0xa0; 20]), &[], &[]);
                    }
                }
                (addr, "find_node") if addr == b => {
                    reply(&dht, &request, Id20::new([0xb0; 20]), &[], &[]);
                }
                (addr, "get_peers") if addr == b => {
                    let named = (
                        Id20::new([0x23; 20]),
                        std::net::SocketAddrV4::new(NODE_N.0.into(), NODE_N.1),
                    );
                    reply(&dht, &request, Id20::new([0xb0; 20]), &[named], &[]);
                }
                (addr, "get_peers") if addr == n => {
                    reply(&dht, &request, Id20::new([0x23; 20]), &[], &[peer]);
                }
                _ => {}
            }
        };
        search.abort();
        refresh.abort();
        bootstrap.abort();
        (queries, found_at)
    }

    // The queries in `queries` sent to `addr`, by kind, in order.
    fn kinds_to(
        queries: &[(Duration, SocketAddr, &'static str)],
        addr: SocketAddr,
    ) -> Vec<&'static str> {
        queries
            .iter()
            .filter(|q| q.1 == addr)
            .map(|q| q.2)
            .collect()
    }

    // A router name that resolves after another: nothing is asked until
    // both have resolved (libtorrent `session_impl::start_dht` waits for
    // every router lookup), and then the bootstrap and the torrent's first
    // search each ask both routers, the bootstrap first. Router A never
    // answers; the torrent's search reaches P through router B at once,
    // with no timeout and no second search.
    #[tokio::test(start_paused = true)]
    async fn test_a_router_resolving_late_joins_the_first_search() {
        let late = Duration::from_millis(3_100);
        let (queries, found_at) = cold_start(late, false).await;
        let (a, b) = (SocketAddr::from(ROUTER_A), SocketAddr::from(ROUTER_B));

        assert_eq!(
            queries[0].0, late,
            "asked before every router name was looked up"
        );
        let first: Vec<_> = queries[..4].iter().map(|q| (q.1, q.2)).collect();
        let mut bootstrap: Vec<_> = first[..2].to_vec();
        bootstrap.sort();
        let mut search: Vec<_> = first[2..].to_vec();
        search.sort();
        assert_eq!(bootstrap, vec![(a, "find_node"), (b, "find_node")]);
        assert_eq!(search, vec![(a, "get_peers"), (b, "get_peers")]);
        assert_eq!(found_at, late);
        assert_eq!(kinds_to(&queries, a), vec!["find_node", "get_peers"]);
    }

    // Two queries reaching one router at once, of which the router answers
    // only the first: the bootstrap's goes first, every time, so it is the
    // one answered (libtorrent's `start_dht` sends the bootstrap before any
    // torrent's search). The torrent's search, unanswered by that router,
    // reaches P through the other router at once.
    #[tokio::test(start_paused = true)]
    async fn test_the_bootstrap_reaches_a_router_before_a_torrents_search() {
        let (queries, found_at) = cold_start(Duration::ZERO, true).await;
        let (a, b) = (SocketAddr::from(ROUTER_A), SocketAddr::from(ROUTER_B));

        assert_eq!(kinds_to(&queries, a), vec!["find_node", "get_peers"]);
        assert_eq!(kinds_to(&queries, b)[..2], ["find_node", "get_peers"]);
        let first_search = queries.iter().position(|q| q.2 == "get_peers").unwrap();
        assert!(
            queries[..first_search]
                .iter()
                .filter(|q| q.2 == "find_node")
                .count()
                == 2,
            "the torrent searched before the bootstrap asked both routers"
        );
        assert_eq!(found_at, Duration::ZERO);
    }

    // The most datagrams sent at `times` (in order) inside any WINDOW.
    fn most_in_any_window(times: &[tokio::time::Instant]) -> usize {
        use crate::dht::query_limiter::WINDOW;
        let mut first = 0;
        let mut most = 0;
        for (last, &at) in times.iter().enumerate() {
            while times[first] + WINDOW <= at {
                first += 1;
            }
            most = most.max(last + 1 - first);
        }
        most
    }

    // `torrents` torrents added at once on a cold table of two nodes that
    // have answered, so every search also asks the two routers: four
    // addresses every search shares. Each answers every get_peers at once,
    // with a token, naming no node. Returns when each address was sent a
    // datagram, up to the time every torrent's first search has announced.
    async fn shared_nodes_campaign(
        torrents: u32,
    ) -> std::collections::HashMap<std::net::IpAddr, Vec<tokio::time::Instant>> {
        use crate::core::hash_id::Id20;
        use std::collections::{HashMap, HashSet};
        use std::net::IpAddr;

        let (dht, mut sent) = fixture_dht();
        dht.routers.write().extend([
            SocketAddr::from(([192, 0, 2, 1], 6881)),
            SocketAddr::from(([192, 0, 2, 2], 6881)),
        ]);
        add_table_nodes(&dht, 2, true);
        // The id each address answers with: a table node's own.
        let id_of = |addr: SocketAddr| match addr.ip() {
            IpAddr::V4(a) if a.octets()[0] == 10 => Id20::new([a.octets()[2] + 1; 20]),
            _ => Id20::new([0x77; 20]),
        };
        let tasks: Vec<_> = (0..torrents)
            .map(|i| {
                let mut hash = [0x55; 20];
                hash[..4].copy_from_slice(&i.to_be_bytes());
                Arc::new(super::RecursiveRequest::get_peers(
                    dht.clone(),
                    Id20::new(hash),
                    Some(6881),
                    tokio::sync::mpsc::unbounded_channel().0,
                ))
                .request_peers_forever(true)
            })
            .collect();
        let mut sends = HashMap::<IpAddr, Vec<tokio::time::Instant>>::new();
        let mut announced = HashSet::new();
        while announced.len() < torrents as usize {
            let request = sent.recv().await.unwrap();
            let now = tokio::time::Instant::now();
            sends.entry(request.addr.ip()).or_default().push(now);
            match &request.message.kind {
                MessageKind::GetPeersRequest(_) => {
                    answer(&dht, &request, id_of(request.addr), Some(b"t"), &[]);
                }
                MessageKind::AnnouncePeer(a) => {
                    announced.insert(a.info_hash);
                }
                kind => panic!("unexpected {kind:?}"),
            }
        }
        for task in tasks {
            task.abort();
        }
        sends
    }

    // However many torrents search at once from the same few nodes and
    // routers, none of them is sent the datagrams in any 10 seconds at which
    // a libtorrent node ignores us (dos_blocker.cpp:52-99, v2.0.15), and
    // every torrent's first search still ends.
    #[tokio::test(start_paused = true)]
    async fn test_torrents_searching_from_the_same_nodes_never_flood_one() {
        use crate::dht::{query_limiter::QUERY_LIMIT, send_windows::SEND_LIMIT};
        for torrents in [100, 1000] {
            let sends = shared_nodes_campaign(torrents).await;
            assert_eq!(sends.len(), 4, "{torrents} torrents");
            for (ip, times) in sends {
                let most = most_in_any_window(&times);
                assert!(
                    most <= SEND_LIMIT && most < QUERY_LIMIT as usize,
                    "{torrents} torrents: {ip} was sent {most} in one window"
                );
            }
        }
    }

    // Searches whose nodes differ are not held back by a node whose window
    // is full; a search that wants that node asks its others at once, and
    // that one as its window opens.
    #[tokio::test(start_paused = true)]
    async fn test_a_full_window_holds_back_only_its_own_address() {
        use crate::core::hash_id::Id20;
        use crate::dht::{query_limiter::WINDOW, send_windows::SEND_LIMIT};
        use futures::{FutureExt, StreamExt};

        let (dht, mut sent) = fixture_dht();
        let full = SocketAddr::from(([10, 9, 9, 9], 6881));
        for _ in 0..SEND_LIMIT {
            dht.send_guard.record(full.ip());
        }
        let start = tokio::time::Instant::now();
        let nodes = |search: u8, with_full: bool| {
            (0..8u8)
                .map(move |i| {
                    let addr = if with_full && i == 0 {
                        full
                    } else {
                        SocketAddr::from(([10, 1, search, i], 6881))
                    };
                    (Some(Id20::new([search * 8 + i + 1; 20])), addr)
                })
                .collect::<Vec<_>>()
        };
        let reqs = [get_peers_search(&dht), get_peers_search(&dht)];
        let mut searches = [(0u8, false), (1, true)]
            .into_iter()
            .zip(&reqs)
            .map(|((search, with_full), req)| {
                req.search(true, nodes(search, with_full).into_iter())
                    .map(move |outcome| (search, outcome))
            })
            .collect::<futures::stream::FuturesUnordered<_>>();
        let mut asked = Vec::new();
        let mut ended = Vec::new();
        loop {
            tokio::select! {
                done = searches.next() => match done {
                    Some((search, outcome)) => {
                        assert_eq!(outcome.unwrap().answered, 8);
                        ended.push((search, tokio::time::Instant::now() - start));
                    }
                    None => break,
                },
                request = sent.recv() => {
                    let request = request.unwrap();
                    asked.push((request.addr, tokio::time::Instant::now() - start));
                    let id = match &request.message.kind {
                        MessageKind::GetPeersRequest(_) if request.addr == full => Id20::new([9; 20]),
                        MessageKind::GetPeersRequest(_) => match request.addr.ip() {
                            std::net::IpAddr::V4(a) => Id20::new([a.octets()[2] * 8 + a.octets()[3] + 1; 20]),
                            std::net::IpAddr::V6(_) => unreachable!(),
                        },
                        kind => panic!("unexpected {kind:?}"),
                    };
                    answer(&dht, &request, id, None, &[]);
                }
            }
        }
        // Every other node is asked at once; the full one as its window opens.
        assert_eq!(asked.len(), 16);
        for (addr, at) in &asked {
            let want = if *addr == full {
                WINDOW
            } else {
                Duration::ZERO
            };
            assert_eq!(*at, want, "{addr}");
        }
        assert_eq!(asked.last().unwrap().0, full);
        ended.sort();
        assert_eq!(ended, vec![(0, Duration::ZERO), (1, WINDOW)]);
    }

    // A node whose window is full, farther than the k closest that answer,
    // is never asked: the search ends once they have, and the node leaves
    // its address's line.
    #[tokio::test(start_paused = true)]
    async fn test_a_search_ends_without_waiting_for_a_deferred_node_it_no_longer_needs() {
        use crate::core::hash_id::Id20;
        use crate::dht::send_windows::SEND_LIMIT;
        use std::task::Poll;

        let (dht, mut sent) = fixture_dht();
        // Node i is i + 1 from the info-hash; the farthest, 8, is full.
        let node = |i: u8| {
            let mut id = [0x22; 20];
            id[19] ^= i + 1;
            (Id20::new(id), SocketAddr::from(([10, 0, i, 1], 6881)))
        };
        let full = node(8).1;
        for _ in 0..SEND_LIMIT {
            dht.send_guard.record(full.ip());
        }
        let req = get_peers_search(&dht);
        let start = tokio::time::Instant::now();
        let mut search = std::pin::pin!(req.search(
            true,
            (0..9).map(|i| {
                let (id, addr) = node(i);
                (Some(id), addr)
            })
        ));
        let outcome = loop {
            if let Poll::Ready(outcome) = futures::poll!(search.as_mut()) {
                break outcome.unwrap();
            }
            let mut answered = false;
            while let Ok(request) = sent.try_recv() {
                assert_ne!(request.addr, full, "the deferred node was asked");
                let i = match request.addr.ip() {
                    std::net::IpAddr::V4(a) => a.octets()[2],
                    std::net::IpAddr::V6(_) => unreachable!(),
                };
                answer(&dht, &request, node(i).0, None, &[]);
                answered = true;
            }
            assert!(answered, "the search waited with nothing to answer");
        };
        assert_eq!(outcome.answered, 8);
        assert_eq!(tokio::time::Instant::now(), start);
        assert_eq!(dht.send_guard.windows.lock().lined_up(full.ip()), 0);
    }

    // Node i of a walk test: i + 1 from the target [0x22; 20], at 10.0.i.1.
    fn walk_node(i: u8) -> (crate::core::hash_id::Id20, SocketAddr) {
        let mut id = [0x22; 20];
        id[19] ^= i + 1;
        (
            crate::core::hash_id::Id20::new(id),
            SocketAddr::from(([10, 0, i, 1], 6881)),
        )
    }

    fn walk_index(addr: SocketAddr) -> u8 {
        match addr.ip() {
            std::net::IpAddr::V4(a) => a.octets()[2],
            std::net::IpAddr::V6(_) => unreachable!(),
        }
    }

    // What `drive` saw sent: to which node, whether it was an announce, and
    // when, from the start.
    type Sent = Vec<(u8, bool, Duration)>;

    // Runs `search` to its end, answering each query `delay` says to,
    // through `respond`, that long after it was sent; `delay` None leaves it
    // unanswered. Returns the search's result and every datagram sent.
    async fn drive<T: Send + 'static>(
        sent: &mut tokio::sync::mpsc::UnboundedReceiver<super::WorkerSendRequest>,
        mut search: tokio::task::JoinHandle<T>,
        delay: impl Fn(u8) -> Option<Duration>,
        respond: impl Fn(&super::WorkerSendRequest),
    ) -> (T, Sent) {
        use std::collections::BTreeMap;
        let start = tokio::time::Instant::now();
        let mut log = Vec::new();
        let mut later = BTreeMap::new();
        let mut seq = 0u64;
        loop {
            let due = later.keys().next().map(|&(at, _)| at);
            tokio::select! {
                biased;
                () = super::sleep_until_some(due) => {
                    let (_, request) = later.pop_first().unwrap();
                    respond(&request);
                }
                request = sent.recv() => {
                    let request = request.unwrap();
                    let i = walk_index(request.addr);
                    let announce = matches!(request.message.kind, MessageKind::AnnouncePeer(_));
                    let now = tokio::time::Instant::now();
                    log.push((i, announce, now - start));
                    match delay(i) {
                        Some(_) if announce => {}
                        Some(d) if d.is_zero() => respond(&request),
                        Some(d) => {
                            seq += 1;
                            later.insert((now + d, seq), request);
                        }
                        None => {}
                    }
                }
                done = &mut search => return (done.unwrap(), log),
            }
        }
    }

    // The queries of `log` sent at `at`, by node.
    fn queried_at(log: &Sent, at: Duration) -> Vec<u8> {
        let mut nodes: Vec<_> = log
            .iter()
            .filter(|&&(_, announce, when)| !announce && when == at)
            .map(|&(i, _, _)| i)
            .collect();
        nodes.sort();
        nodes
    }

    // The five closest nodes never answer. A query unanswered SHORT_TIMEOUT
    // after it was sent lets the walk ask one more node, so the live nodes
    // are asked, and their peers found, at SHORT_TIMEOUT, not as the five
    // run out RESPONSE_TIMEOUT. The walk still waits for the five, closer
    // than every node that answered, until they fail (libtorrent
    // traversal_algorithm.cpp:353-408, 492-580, v2.0.15). Each node is asked
    // once: 13 queries, as many as without the short timeout.
    #[tokio::test(start_paused = true)]
    async fn test_a_walk_whose_five_closest_are_dead_reaches_live_nodes_after_the_short_timeout() {
        use super::{SHORT_TIMEOUT, SearchOutcome};
        use crate::core::hash_id::Id20;
        use crate::dht::RESPONSE_TIMEOUT;

        let (dht, mut sent) = fixture_dht();
        let peer = SocketAddr::from(([10, 9, 9, 9], 51413));
        let (peer_tx, mut peers) = tokio::sync::mpsc::unbounded_channel();
        let req =
            super::RecursiveRequest::get_peers(dht.clone(), Id20::new([0x22; 20]), None, peer_tx);
        let start = tokio::time::Instant::now();
        let search = tokio::spawn(async move {
            let outcome = req
                .search(
                    true,
                    (0..13).map(|i| {
                        let (id, addr) = walk_node(i);
                        (Some(id), addr)
                    }),
                )
                .await;
            (outcome, tokio::time::Instant::now())
        });
        let ((outcome, ended), log) = drive(
            &mut sent,
            search,
            |i| (i >= 5).then_some(Duration::ZERO),
            |request| {
                reply(
                    &dht,
                    request,
                    walk_node(walk_index(request.addr)).0,
                    &[],
                    &[peer],
                )
            },
        )
        .await;

        assert_eq!(queried_at(&log, Duration::ZERO), (0..5).collect::<Vec<_>>());
        assert_eq!(queried_at(&log, SHORT_TIMEOUT), (5..13).collect::<Vec<_>>());
        assert_eq!(log.len(), 13);
        assert_eq!(peers.try_recv().unwrap(), peer);
        assert_eq!(ended - start, RESPONSE_TIMEOUT);
        assert_eq!(
            outcome.unwrap(),
            SearchOutcome {
                answered: 8,
                failed: 5,
                nodes: 8
            }
        );
        assert_eq!(dht.get_stats().outstanding_requests, 0);
    }

    // The eight closest nodes answer 2 s after they are asked, the twelve
    // farther ones at once. At SHORT_TIMEOUT the five asked first have not
    // answered, so the walk may keep five more in flight: it asks the other
    // three slow ones and farther fast ones, a fast one again as each
    // answers, until eight have answered: nine fast ones in all. It ends as
    // the slow ones, the closest, have all answered. 17 queries: without
    // the short timeout the walk asked the slow ones five at a time and
    // sent 12, the 5 more being farther nodes asked in the room the five
    // late queries opened; a node is never asked twice.
    #[tokio::test(start_paused = true)]
    async fn test_a_walk_asks_farther_nodes_only_in_the_room_its_late_queries_open() {
        use super::{SHORT_TIMEOUT, SearchOutcome};

        let (dht, mut sent) = fixture_dht();
        let slow = Duration::from_secs(2);
        let req = get_peers_search(&dht);
        let start = tokio::time::Instant::now();
        let search = tokio::spawn(async move {
            let outcome = req
                .search(
                    true,
                    (0..20).map(|i| {
                        let (id, addr) = walk_node(i);
                        (Some(id), addr)
                    }),
                )
                .await;
            (outcome, tokio::time::Instant::now())
        });
        let ((outcome, ended), log) = drive(
            &mut sent,
            search,
            |i| Some(if i < 8 { slow } else { Duration::ZERO }),
            |request| {
                reply(
                    &dht,
                    request,
                    walk_node(walk_index(request.addr)).0,
                    &[],
                    &[],
                )
            },
        )
        .await;

        assert_eq!(queried_at(&log, Duration::ZERO), (0..5).collect::<Vec<_>>());
        assert_eq!(queried_at(&log, SHORT_TIMEOUT), (5..17).collect::<Vec<_>>());
        assert_eq!(log.len(), 17);
        assert_eq!(ended - start, SHORT_TIMEOUT + slow);
        assert_eq!(
            outcome.unwrap(),
            SearchOutcome {
                answered: 17,
                failed: 0,
                nodes: 17
            }
        );
    }

    // The five closest nodes answer 2 s after they are asked, the next three
    // at once, and the two farthest never. At SHORT_TIMEOUT the walk asks the
    // other eight. Once the eight closest have answered, at 2 s, the walk
    // ends without waiting for the two farther ones it still has in flight,
    // and announces to the eight then, as libtorrent's `find_data::done`
    // does (traversal_algorithm.cpp:492-580, find_data.cpp:139-194,
    // v2.0.15). The two are withdrawn: they wait for their answers apart
    // from the walk (see `test_a_withdrawn_query_still_feeds_the_table`).
    #[tokio::test(start_paused = true)]
    async fn test_a_walk_ends_once_the_k_closest_answer_with_farther_queries_in_flight() {
        use super::SHORT_TIMEOUT;
        use crate::core::hash_id::Id20;

        let (dht, mut sent) = fixture_dht();
        let slow = Duration::from_secs(2);
        let req = super::RecursiveRequest::get_peers(
            dht.clone(),
            Id20::new([0x22; 20]),
            Some(6881),
            tokio::sync::mpsc::unbounded_channel().0,
        );
        let start = tokio::time::Instant::now();
        let search = tokio::spawn(async move {
            let outcome = req
                .search(
                    true,
                    (0..10).map(|i| {
                        let (id, addr) = walk_node(i);
                        (Some(id), addr)
                    }),
                )
                .await;
            (outcome, tokio::time::Instant::now())
        });
        let ((outcome, ended), log) = drive(
            &mut sent,
            search,
            |i| match i {
                0..5 => Some(slow),
                5..8 => Some(Duration::ZERO),
                _ => None,
            },
            |request| {
                let i = walk_index(request.addr);
                answer(&dht, request, walk_node(i).0, Some(&[i]), &[]);
            },
        )
        .await;

        assert_eq!(queried_at(&log, Duration::ZERO), (0..5).collect::<Vec<_>>());
        assert_eq!(queried_at(&log, SHORT_TIMEOUT), (5..10).collect::<Vec<_>>());
        assert_eq!(ended - start, slow);
        let outcome = outcome.unwrap();
        assert_eq!((outcome.answered, outcome.failed), (8, 0));
        let mut announced: Vec<_> = log
            .iter()
            .filter(|&&(_, announce, _)| announce)
            .map(|&(i, _, at)| (i, at))
            .collect();
        announced.sort();
        assert_eq!(announced, (0..8).map(|i| (i, slow)).collect::<Vec<_>>());
        assert_eq!(dht.get_stats().outstanding_requests, 2);
    }

    // A node that answers after SHORT_TIMEOUT, here a find_node of a
    // routing-table walk at 5 s, is not failed: its answer is used as any
    // other, so the node it names is asked then, and enters the table as
    // it answers.
    #[tokio::test(start_paused = true)]
    async fn test_a_late_answer_is_still_used() {
        use super::{RecursiveRequest, SearchOutcome};
        use crate::core::hash_id::Id20;

        let (dht, mut sent) = fixture_dht();
        let late = Duration::from_secs(5);
        let start = tokio::time::Instant::now();
        let search = tokio::spawn(RecursiveRequest::find_node_for_routing_table(
            dht.clone(),
            Id20::new([0x22; 20]),
            true,
            std::iter::once(walk_node(0).1),
        ));
        let named = |i: u8| {
            let (id, addr) = walk_node(i);
            let SocketAddr::V4(addr) = addr else {
                unreachable!()
            };
            (id, addr)
        };
        let (outcome, log) = drive(
            &mut sent,
            search,
            |i| Some(if i == 0 { late } else { Duration::ZERO }),
            |request| {
                let i = walk_index(request.addr);
                let nodes: Vec<_> = (i == 0).then(|| named(1)).into_iter().collect();
                reply(&dht, request, walk_node(i).0, &nodes, &[]);
            },
        )
        .await;

        assert_eq!(log, vec![(0, false, Duration::ZERO), (1, false, late)]);
        assert_eq!(
            outcome.unwrap(),
            SearchOutcome {
                answered: 2,
                failed: 0,
                nodes: 1
            }
        );
        assert_eq!(tokio::time::Instant::now() - start, late);
        // The node the late answer named answered and entered the table; the
        // first was given by address only, as a router is.
        assert_eq!(dht.routing_table_v4.read().len(), 1);
    }

    // Queries lined up for a full window go as it opens, one as each datagram
    // sent leaves it, in the order they were lined up.
    #[tokio::test(start_paused = true)]
    async fn test_deferred_queries_go_in_order_as_their_window_opens() {
        use crate::core::hash_id::Id20;
        use crate::dht::{query_limiter::WINDOW, send_windows::SEND_LIMIT};
        use std::task::Poll;

        let (dht, mut sent) = fixture_dht();
        let node = SocketAddr::from(([10, 0, 0, 1], 6881));
        let start = tokio::time::Instant::now();
        let step = Duration::from_millis(100);
        for _ in 0..SEND_LIMIT {
            dht.send_guard.record(node.ip());
            tokio::time::advance(step).await;
        }
        let queries = [1u8, 2, 3]
            .map(|i| Box::pin(dht.request(super::Request::FindNode(Id20::new([i; 20])), node)));
        let mut queries = queries;
        for query in &mut queries {
            assert!(matches!(futures::poll!(query.as_mut()), Poll::Pending));
        }
        assert!(sent.try_recv().is_err(), "sent into a full window");
        assert_eq!(dht.send_guard.windows.lock().lined_up(node.ip()), 3);
        let mut all = futures::future::join_all(queries);
        let mut went = Vec::new();
        while went.len() < 3 {
            tokio::select! {
                _ = &mut all => unreachable!("the queries are not answered"),
                request = sent.recv() => {
                    let request = request.unwrap();
                    let MessageKind::FindNodeRequest(f) = request.message.kind else {
                        panic!("not a find_node");
                    };
                    went.push((f.target, tokio::time::Instant::now() - start));
                }
            }
        }
        assert_eq!(
            went,
            (0..3u8)
                .map(|i| (Id20::new([i + 1; 20]), WINDOW + step * u32::from(i)))
                .collect::<Vec<_>>()
        );
    }

    // An announce_peer counts in its node's window, and one past a full
    // window waits for it to open.
    #[tokio::test(start_paused = true)]
    async fn test_an_announce_is_counted_and_waits_for_room() {
        use crate::core::hash_id::Id20;
        use crate::dht::{query_limiter::WINDOW, send_windows::SEND_LIMIT};
        use futures::StreamExt;

        let (dht, mut sent) = fixture_dht();
        let node = SocketAddr::from(([10, 0, 0, 1], 6881));
        let start = tokio::time::Instant::now();
        let mut announces = (0..=SEND_LIMIT)
            .map(|_| {
                dht.send_unanswered(
                    super::Request::Announce {
                        info_hash: Id20::new([0x22; 20]),
                        token: ByteBufOwned::from(&b"t"[..]),
                        port: 6881,
                    },
                    node,
                )
            })
            .collect::<futures::stream::FuturesUnordered<_>>();
        let mut times = Vec::new();
        loop {
            tokio::select! {
                done = announces.next() => if done.is_none() { break },
                request = sent.recv() => {
                    let request = request.unwrap();
                    assert!(matches!(request.message.kind, MessageKind::AnnouncePeer(_)));
                    times.push(tokio::time::Instant::now());
                }
            }
        }
        while let Ok(request) = sent.try_recv() {
            assert!(matches!(request.message.kind, MessageKind::AnnouncePeer(_)));
            times.push(tokio::time::Instant::now());
        }
        assert_eq!(times.len(), SEND_LIMIT + 1);
        assert_eq!(most_in_any_window(&times), SEND_LIMIT);
        // The last waited for the first to leave the window.
        assert_eq!(*times.last().unwrap(), times[0] + WINDOW);
        assert!(times[0] - start < WINDOW);
    }

    // Once nothing is sent, the windows close and the map empties; the sweep
    // then rests until the next datagram.
    #[tokio::test(start_paused = true)]
    async fn test_the_send_windows_empty_once_traffic_stops() {
        use crate::dht::query_limiter::WINDOW;
        use std::task::Poll;

        let (dht, _sent) = fixture_dht();
        let guard = dht.send_guard.clone();
        let mut sweep = std::pin::pin!(guard.sweep());
        assert!(matches!(futures::poll!(sweep.as_mut()), Poll::Pending));
        for i in 1..=3u8 {
            dht.send_guard
                .record(SocketAddr::from(([10, 0, 0, i], 6881)).ip());
        }
        // Woken by the first datagram.
        assert!(matches!(futures::poll!(sweep.as_mut()), Poll::Pending));
        assert_eq!(dht.send_guard.windows.lock().len(), 3);
        tokio::time::advance(WINDOW).await;
        assert!(matches!(futures::poll!(sweep.as_mut()), Poll::Pending));
        assert_eq!(dht.send_guard.windows.lock().len(), 0);
        // Resting until a datagram is sent, whatever time passes.
        tokio::time::advance(10 * WINDOW).await;
        assert!(matches!(futures::poll!(sweep.as_mut()), Poll::Pending));
        assert_eq!(dht.send_guard.windows.lock().len(), 0);
        assert!(
            dht.send_guard.windows.lock().record(
                SocketAddr::from(([10, 0, 0, 1], 6881)).ip(),
                tokio::time::Instant::now()
            ),
            "the sweep was not resting"
        );
    }
}
