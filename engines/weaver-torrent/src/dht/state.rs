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

use crate::session_stats::dht::{DhtCounts, DhtIgnored, DhtQuery, DhtQueryEnd, DhtSnapshot};

// The routing table goes by tokio's clock, the one every other DHT timer
// runs on, so a paused runtime stands it still with them. Outside a test
// it is the machine's own monotonic clock.
fn now() -> Instant {
    tokio::time::Instant::now().into_std()
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
                    // No search while the IPv6 node is stopped or idle.
                    if !is_v4 {
                        self.dht.ipv6.running().await;
                    }
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
            let mut named = 0;
            for peer in response.values.iter().flatten() {
                let _ = self.peer_tx.send(peer.0);
                named += 1;
            }
            self.dht.counts.peers_found(named);
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

    fn is_open(&self) -> bool {
        self.open.load(Ordering::Acquire)
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

/// Where the IPv6 node of a DHT stands.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Ipv6Phase {
    /// Not started: the route has given it no address yet. It holds no
    /// table, sends nothing and reads nothing.
    Waits,
    /// It runs. `proving` while its route is a proxy's and its first
    /// bootstrap has not yet heard an IPv6 node answer.
    Runs { proving: bool },
    /// Stopped as the route's address went (libtorrent deletes the node of
    /// a listen socket it closes, session_impl.cpp:2159-2163, v2.0.15). Its
    /// table is kept, so a returning address resumes from it; it sends
    /// nothing and reads nothing.
    Stopped,
    /// Idle: its proxy carried no IPv6. It sends nothing and reads nothing
    /// until the route or the proxy setting changes, which opens a new
    /// session; never on a timer.
    Idle,
}

// The IPv6 node's phase, and a wake for what waits on it changing. Read
// per datagram, so the running bit is kept beside it.
struct Ipv6Node {
    phase: tokio::sync::watch::Sender<Ipv6Phase>,
    running: AtomicBool,
    // Its route is a proxy's: it never stops on an interface change.
    proxied: bool,
}

impl Ipv6Node {
    fn new(phase: Ipv6Phase) -> Self {
        Self {
            running: AtomicBool::new(matches!(phase, Ipv6Phase::Runs { .. })),
            proxied: phase == Ipv6Phase::Runs { proving: true },
            phase: tokio::sync::watch::Sender::new(phase),
        }
    }

    fn runs(&self) -> bool {
        self.running.load(Ordering::Acquire)
    }

    fn phase(&self) -> Ipv6Phase {
        *self.phase.borrow()
    }

    // Moves to `to` when `from` holds of the phase now; whether it moved.
    fn shift(&self, from: impl Fn(Ipv6Phase) -> bool, to: Ipv6Phase) -> bool {
        self.phase.send_if_modified(|phase| {
            if *phase == to || !from(*phase) {
                return false;
            }
            *phase = to;
            self.running
                .store(matches!(to, Ipv6Phase::Runs { .. }), Ordering::Release);
            true
        })
    }

    // Resolves once the phase holds `wanted`; a wake signal, never a timer.
    async fn until(&self, wanted: impl Fn(Ipv6Phase) -> bool) {
        let mut phase = self.phase.subscribe();
        let _ = phase.wait_for(|phase| wanted(*phase)).await;
    }

    async fn running(&self) {
        self.until(|phase| matches!(phase, Ipv6Phase::Runs { .. }))
            .await;
    }

    async fn halted(&self) {
        self.until(|phase| !matches!(phase, Ipv6Phase::Runs { .. }))
            .await;
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
    // Where the IPv6 node stands (see `Ipv6Phase`). While it does not run,
    // it sends nothing and reads nothing: its refreshes and searches wait,
    // an IPv6 datagram is dropped unread, and an IPv6 node a peer names is
    // not asked. libtorrent starts no IPv6 node until a listen socket on an
    // IPv6 address it gives a node is opened (session_impl.cpp:2206-2211,
    // v2.0.15; see `route_interface::ipv6_dht`).
    ipv6: Ipv6Node,
    // Open once a datagram of the family has arrived: what the IPv6 node
    // on a proxied route judges its proxy by (see `refresh_self_rounds`).
    heard_v4: Latch,
    heard_v6: Latch,
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

    // What the node sent, was sent and found.
    counts: DhtCounts,
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
            ipv6: Ipv6Node::new(Ipv6Phase::Runs { proving: false }),
            heard_v4: Latch::new(),
            heard_v6: Latch::new(),
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
            counts: DhtCounts::default(),
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
            if !is_v4 && outcome.answered + outcome.failed > 0 {
                self.judge_proxy(outcome.answered > 0).await;
            }
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

    // The IPv6 node's refreshes, while it runs: they wait for it to start,
    // end as it stops or goes idle, and start again from the table it kept
    // as it runs again, so a returning address does not bootstrap cold.
    async fn run_ipv6(self: Arc<Self>) -> crate::dht::Result<()> {
        loop {
            self.ipv6.running().await;
            tokio::select! {
                done = self.clone().refresh_self(false) => done?,
                done = self.clone().refresh_table(false) => done?,
                () = self.ipv6.halted() => {}
            }
        }
    }

    // After a round of the IPv6 node's own id that asked someone: on a
    // proxied route, still proving, the round tells whether the proxy
    // carries IPv6. An IPv6 answer, or any IPv6 datagram read, proves it,
    // and the node runs on as any other. None while the route carries IPv4
    // answers means the proxy refused the IPv6 targets or dropped them in
    // silence (a SOCKS5 relay drops what it cannot relay, and reports
    // nothing per datagram), and the node goes idle: it sends nothing more,
    // and never tries again on a timer; a change of route or proxy setting
    // opens a new session, whose node proves its proxy afresh. Neither yet
    // waits for the first datagram of either family, without a timer.
    async fn judge_proxy(&self, answered: bool) {
        if self.ipv6.phase() != (Ipv6Phase::Runs { proving: true }) {
            return;
        }
        if !answered && !self.heard_v6.is_open() {
            tokio::select! {
                () = self.heard_v6.opened() => {}
                () = self.heard_v4.opened() => {}
            }
        }
        if answered || self.heard_v6.is_open() {
            self.ipv6.shift(
                |p| p == Ipv6Phase::Runs { proving: true },
                Ipv6Phase::Runs { proving: false },
            );
            debug!("the proxy carries IPv6: the IPv6 node runs");
            return;
        }
        // operator ruling 2026-11-17: a proxied route keeps the IPv6 DHT
        // when the proxy carries IPv6; libtorrent runs none
        // (session_impl.cpp:2078-2090, v2.0.15).
        if self
            .ipv6
            .shift(|p| p == Ipv6Phase::Runs { proving: true }, Ipv6Phase::Idle)
        {
            debug!("the proxy carried no IPv6: the IPv6 node is idle");
        }
        std::future::pending::<()>().await;
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
        if ip.is_ipv6() && !self.ipv6.runs() {
            return Err(Error::Send(std::io::Error::new(
                std::io::ErrorKind::NetworkUnreachable,
                "the IPv6 node does not run",
            )));
        }
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
        let query = request.counted_as();
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
        self.counts.sent(query);
        let sent_at = tokio::time::Instant::now();
        sent(sent_at);
        let response = tokio::time::timeout_at(sent_at + RESPONSE_TIMEOUT, answer)
            .await
            .unwrap_or(Ok(Err(Error::ResponseTimeout(RESPONSE_TIMEOUT))))
            .map_err(|_| Error::DhtDead)?;
        match &response {
            Ok(ResponseOrError::Response(_)) => self.counts.ended(query, DhtQueryEnd::Reply),
            Ok(ResponseOrError::Error(_)) => self.counts.ended(query, DhtQueryEnd::Error),
            Err(Error::ResponseTimeout(_)) => self.counts.ended(query, DhtQueryEnd::Timeout),
            Err(_) => {}
        }
        let response = response?;
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
            self.counts.ignored(DhtIgnored::Malformed);
            return None;
        }
        if !self
            .query_limiter
            .lock()
            .admit(addr.ip(), tokio::time::Instant::now())
        {
            trace!("ignoring a datagram from {addr}: it sends too many");
            self.counts.ignored(DhtIgnored::SourceLimit);
            return None;
        }
        match bprotocol::deserialize_message::<ByteBufOwned>(buf) {
            Ok(message) => Some(message),
            Err(e) => {
                trace!("{addr}: error deserializing incoming message: {e:#}");
                self.counts.ignored(DhtIgnored::Malformed);
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
                let Some(tid) = msg.get_our_transaction_id() else {
                    self.counts.ignored(DhtIgnored::Malformed);
                    return Err(Error::BadTransactionId);
                };
                let request = match self
                    .inflight_by_transaction_id
                    .remove(&(tid, addr))
                    .map(|(_, v)| v)
                {
                    Some(req) => req,
                    None => {
                        trace!(?msg, "outstanding request not found");
                        self.counts.ignored(DhtIgnored::Unmatched);
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
            self.counts.ignored(DhtIgnored::NoBudget);
            return Ok(());
        }

        trace!("received query from {addr}: {msg:?}");

        match &msg.kind {
            // Otherwise, respond to a query.
            MessageKind::PingRequest(req) => {
                self.counts.received(DhtQuery::Ping);
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
                self.counts.received(DhtQuery::AnnouncePeer);
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
                self.counts.received(DhtQuery::GetPeers);
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
                self.counts.received(DhtQuery::FindNode);
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

impl Request {
    fn counted_as(&self) -> DhtQuery {
        match self {
            Self::GetPeers(_) => DhtQuery::GetPeers,
            Self::FindNode(_) => DhtQuery::FindNode,
            Self::Announce { .. } => DhtQuery::AnnouncePeer,
            Self::Ping => DhtQuery::Ping,
        }
    }
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
                // A datagram queued before the IPv6 node stopped or went
                // idle is not sent.
                if addr.is_ipv6() && !self.dht.ipv6.runs() {
                    if let Some(tid) = our_tid {
                        self.on_send_error(
                            tid,
                            addr,
                            Error::Send(std::io::Error::new(
                                std::io::ErrorKind::NetworkUnreachable,
                                "the IPv6 node does not run",
                            )),
                        );
                    }
                    continue;
                }
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
                // No IPv6 node running, nothing read from IPv6.
                if addr.is_ipv6() {
                    if !self.dht.ipv6.runs() {
                        continue;
                    }
                    self.dht.heard_v6.open();
                } else {
                    self.dht.heard_v4.open();
                }
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
        let ipv6 = self
            .dht
            .clone()
            .run_ipv6()
            .instrument(debug_span!("ipv6_node"));

        let refresh_table_v4 = self
            .dht
            .clone()
            .refresh_table(true)
            .instrument(debug_span!("refresh_table_v4"));

        let send_windows_sweep = self.dht.send_guard.sweep();

        tokio::pin!(send_windows_sweep);
        tokio::pin!(refresh_self_v4);
        tokio::pin!(ipv6);
        tokio::pin!(refresh_table_v4);
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
                err = &mut ipv6 => {
                    return Error::task_finished(&"ipv6_node", err);
                },
                err = &mut refresh_table_v4 => {
                    return Error::task_finished(&"refresh_table_v4", err);
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
    /// Start the IPv6 node only once [`DhtState::start_ipv6`] is called, for
    /// a route with no IPv6 address the internet reaches yet. Until then it
    /// holds no table, a saved one too, and sends and reads nothing.
    pub ipv6_waits: bool,
    /// The route is a proxy's: the IPv6 node starts, and goes idle unless
    /// its first bootstrap hears IPv6 back (see [`Ipv6Phase::Idle`]).
    /// Ignored with `ipv6_waits`.
    pub ipv6_proving: bool,
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
            // An IPv6 node that waits keeps no saved table: libtorrent keeps
            // the nodes of a family only in a node of it, and starts each
            // node from the routers (dht_tracker.cpp:124-157, 184-197).
            let routing_table_v6 = config
                .routing_table_v6
                .filter(|t| t.id() == peer_id && !config.ipv6_waits)
                .map(|t| t.of_family(false));
            let bootstrap_addrs = config.bootstrap_addrs.unwrap_or_else(|| {
                crate::dht::DHT_BOOTSTRAP
                    .iter()
                    .map(|v| v.to_string())
                    .collect()
            });

            let token = config.cancellation_token.take().unwrap_or_default();

            let (in_tx, in_rx) = unbounded_channel();
            let mut state = Self::new_internal(
                peer_id,
                in_tx,
                routing_table,
                routing_table_v6,
                listen_addr,
                token,
                config.client_version.map(ByteBufOwned::from),
            );
            if config.ipv6_waits {
                info!("no IPv6 address that gives a DHT node: the IPv6 DHT waits for one");
                state.ipv6 = Ipv6Node::new(Ipv6Phase::Waits);
            } else if config.ipv6_proving {
                info!("a proxied route: the IPv6 DHT runs while the proxy carries IPv6");
                state.ipv6 = Ipv6Node::new(Ipv6Phase::Runs { proving: true });
            }
            let state = Arc::new(state);

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

    /// Whether the IPv6 node runs.
    pub fn runs_ipv6(&self) -> bool {
        self.ipv6.runs()
    }

    /// Where the IPv6 node stands.
    pub fn ipv6_phase(&self) -> Ipv6Phase {
        self.ipv6.phase()
    }

    // Resolves once the IPv6 node stands at `phase`.
    #[cfg(test)]
    pub(crate) async fn until_ipv6_phase(&self, phase: Ipv6Phase) {
        self.ipv6.until(|now| now == phase).await;
    }

    // Resolves once the IPv6 table `holds`, woken as nodes enter it.
    #[cfg(test)]
    pub(crate) async fn until_v6_table(&self, holds: impl Fn(&RoutingTable) -> bool) {
        let entered = self.routing_table_v6.read().entered();
        loop {
            let mut next = std::pin::pin!(entered.notified());
            next.as_mut().enable();
            if holds(&self.routing_table_v6.read()) {
                return;
            }
            next.await;
        }
    }

    /// Starts the IPv6 node of a DHT made with [`DhtConfig::ipv6_waits`],
    /// or one stopped. A node that never ran fills its table from the
    /// routers, as libtorrent starts a node for a listen socket opened later
    /// (`dht_tracker::new_socket`, dht_tracker.cpp:124-157, v2.0.15); a
    /// stopped one goes on from the table it kept. A node that runs, or an
    /// idle one, is left alone.
    pub fn start_ipv6(&self) {
        if self.ipv6.shift(
            |p| matches!(p, Ipv6Phase::Waits | Ipv6Phase::Stopped),
            Ipv6Phase::Runs { proving: false },
        ) {
            info!("an IPv6 address that gives a DHT node: the IPv6 DHT runs");
        }
    }

    /// Stops the IPv6 node as its route's address goes, as libtorrent
    /// deletes the node of a listen socket it closes
    /// (session_impl.cpp:2159-2163, v2.0.15). It sends and reads nothing
    /// until [`Self::start_ipv6`], and keeps its table for then. Only a node
    /// that runs from an address of its own stops: one that waits, one on a
    /// proxied route and an idle one are left alone.
    pub fn stop_ipv6(&self) {
        if self.ipv6.shift(
            |p| p == Ipv6Phase::Runs { proving: false } && !self.ipv6.proxied,
            Ipv6Phase::Stopped,
        ) {
            info!("no IPv6 address that gives a DHT node: the IPv6 DHT stops");
        }
    }

    /// Pings a node a peer named (BEP 5 PORT) and, when it answers, adds it
    /// to the routing table under the id it answered with (libtorrent
    /// `node::add_node`: "ping the node, and if we get a reply, it will be
    /// added to the routing table").
    pub fn add_node(self: &Arc<Self>, addr: SocketAddr) {
        let addr = SocketAddr::new(addr.ip().to_canonical(), addr.port());
        if addr.is_ipv6() && !self.ipv6.runs() {
            return;
        }
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

    /// What the node has counted since it started.
    pub fn counted(&self) -> DhtSnapshot {
        self.counts.snapshot()
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
mod bootstrap_tests;
#[cfg(test)]
mod tests;
#[cfg(test)]
mod walk_tests;
