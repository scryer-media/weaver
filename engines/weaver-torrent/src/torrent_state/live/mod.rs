// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver
//
// Derived from rqbit (https://github.com/ikatson/rqbit), Copyright 2021 Igor Katson,
// licensed under the Apache License 2.0 (see LICENSE-APACHE). Modified by the
// Weaver authors from 2026-10-20 onward.

// The main logic of rqbit is here - connecting to peers, reading and writing messages
// to them, tracking peer state etc.
//
// ## Architecture
// There are many tasks cooperating to download the torrent. Tasks communicate both with message passing
// and shared memory.
//
// ### Shared locked state
// Shared state is access by almost all actors through RwLocks.
//
// There's one source of truth (TorrentStateLocked) for which chunks we have, need, and what peers are we waiting them from.
//
// Peer states that are important to the outsiders (tasks other than manage_peer) are in a sharded hash-map (DashMap)
//
// ### Tasks (actors)
// Peer adder task:
// - connects to the connect candidates in the peer list (PeerStates) as their reconnect time comes
//   and slots free up. Trackers, DHT, PEX and incoming connections fill the list.
//
// Each peer has one main task "manage_peer". It's composed of 2 futures running as one task through tokio::select:
// - "manage_peer" - this talks to the peer over network and calls callbacks on PeerHandler. The callbacks are not async,
//   and are supposed to finish quickly (apart from writing to disk, which is accounted for as "spawn_blocking").
// - "peer_chunk_requester" - this continuously sends requests for chunks to the peer.
//   it may steal chunks/pieces from other peers.
//
// ## Peer lifecycle
// State transitions:
// - queued (initial state) -> connected
// - connected -> live
// - connecting/live -> queued or dead (on error: queued until it has failed max_failcount times
//   in a row, then dead until someone names it again)
// - ANY STATE -> not_needed (when we don't need to talk to the peer anymore)
//
// A failed peer is reconnected (failcount + 1) * min_reconnect_time after its last failure.
//
// > NOTE: deadlock notice:
// > peers and stateLocked are behind 2 different locks.
// > if you lock them in different order, this may deadlock.
// >
// > so don't lock them both at the same time at all, or at the worst lock them in the
// > same order (peers one first, then the global one).

pub(crate) mod choker;
mod hash_fetch;
pub mod peer;
pub mod peers;
mod smart_ban;
pub mod stats;
#[cfg(test)]
mod tests;
pub(crate) mod web_seed;

use std::{
    borrow::Cow,
    collections::{HashMap, HashSet},
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::{Duration, Instant},
};

use crate::buffers::{ByteBuf, ByteBufOwned};
use crate::core::{
    hash_id::Id20,
    lengths::{ChunkInfo, Lengths, ValidPieceIndex},
    net::{HttpClients, Resolver},
    spawn_utils::spawn_with_cancel,
    speed_estimator::SpeedEstimator,
    torrent_metainfo::ValidatedTorrentMetaV1Info,
};
use crate::peer_protocol::{
    Handshake, MY_EXTENDED_UT_HOLEPUNCH, Message, Piece, Request,
    extended::{
        ExtendedMessage,
        handshake::ExtendedHandshake,
        ut_holepunch::{self, UtHolepunch},
        ut_metadata::{UtMetadata, UtMetadataData},
        ut_pex::{PexPeerInfo, UtPex},
    },
};
use anyhow::Context;
use futures::FutureExt;
use parking_lot::{Mutex, MutexGuard, RwLock, RwLockReadGuard, RwLockWriteGuard};
use tokio::sync::{Notify, OwnedSemaphorePermit, Semaphore, mpsc::unbounded_channel, watch};
use tokio_util::sync::CancellationToken;
use tracing::{Instrument, debug, debug_span, error, info, trace, warn};

use crate::{
    Error,
    chunk_tracker::{ChunkMarkingResult, ChunkTracker, HaveNeededSelected},
    connection_slots::{ConnectionSlot, ConnectionSlots},
    file_info::FileInfo,
    file_ops::{FileOps, Verdict, inconsistent_hashes},
    limits::{Channel, LimitKind},
    peer_connection::{
        DEFAULT_CONNECT_TIMEOUT, PeerConnection, PeerConnectionHandler, PeerConnectionOptions,
        WriterRequest,
    },
    piece_priority::PiecePriority,
    piece_tracker::{AcquireRequest, AcquireResult, Arrival, PickOptions, PieceTracker},
    session::CheckedIncomingConnection,
    session_stats::SessionStats,
    stream_connect::ConnectionKind,
    torrent_state::utils::atomic_inc,
    type_aliases::{FileStorage, PeerHandle, PeerSourceFlag},
};

use self::{
    choker::CHOKED_REQUEST_GRACE,
    peer::{
        DisconnectRank, InflightRequest, LivePeerState, MAX_ALLOWED_IN_REQUEST_QUEUE, PeerRx,
        PeerState, PeerTx, RATE_SAMPLE_INTERVAL, Refused, RemoveInflightRequestResult,
        empty_bitfield,
        info::PeerInfo,
        ingest_bitfield, opening_rank,
        stats::{
            atomic::PeerCountersAtomic as AtomicPeerCounters,
            snapshot::{PeerStatsFilter, PeerStatsSnapshot},
        },
    },
    peers::{IncomingRefused, PeerStates, holepunch::is_local},
    stats::{atomic::AtomicStats, snapshot::StatsSnapshot},
    web_seed::{SeedLink, WEB_SEED_TICK, WebSeedConnection},
};

use super::{
    ManagedTorrentShared, RunCounters, TorrentMetadata,
    paused::TorrentStatePaused,
    streaming::TorrentStreams,
    utils::{TimedExistence, timeit},
};

pub(crate) struct TorrentStateLocked {
    // Coordinates piece state: what chunks we have, need, and what pieces are in-flight.
    // If this is None, the torrent was paused, and this live state is useless, and needs to be dropped.
    pub(crate) pieces: Option<PieceTracker>,

    // If this is None, then it was already used
    fatal_errors_tx: Option<tokio::sync::oneshot::Sender<anyhow::Error>>,

    unflushed_bitv_bytes: u64,
}

impl TorrentStateLocked {
    pub(crate) fn get_chunks(&self) -> crate::Result<&ChunkTracker> {
        self.pieces
            .as_ref()
            .map(|p| p.chunks())
            .ok_or(Error::ChunkTrackerEmpty)
    }

    pub(crate) fn get_pieces(&self) -> crate::Result<&PieceTracker> {
        self.pieces.as_ref().ok_or(Error::ChunkTrackerEmpty)
    }

    pub(crate) fn get_pieces_mut(&mut self) -> crate::Result<&mut PieceTracker> {
        self.pieces.as_mut().ok_or(Error::ChunkTrackerEmpty)
    }

    fn try_flush_bitv(&mut self, shared: &ManagedTorrentShared, flush_async: bool) {
        if self.unflushed_bitv_bytes == 0 {
            return;
        }
        trace!("trying to flush bitfield");
        if let Some(Err(e)) = self
            .pieces
            .as_mut()
            .map(|pt| pt.flush_have_pieces(flush_async))
        {
            warn!(id=?shared.id, info_hash = ?shared.info_hash, "error flushing bitfield: {e:#}");
        } else {
            trace!("flushed bitfield");
            self.unflushed_bitv_bytes = 0;
        }
    }
}

const FLUSH_BITV_EVERY_BYTES: u64 = 16 * 1024 * 1024;

// The share of its connections, in percent, a torrent closes when it turns
// peers over (libtorrent `peer_turnover`).
const PEER_TURNOVER: usize = 4;

// How long a peer given nothing to request waits before it picks again,
// when nothing it waits on changes first (libtorrent
// `peer_connection::second_tick`: 5 s after the last request).
const REPICK_INTERVAL: Duration = Duration::from_secs(5);

// How often a peer whose requests are overdue is snubbed again, each time
// giving back the pieces that came to wait on it alone (libtorrent
// `peer_connection::second_tick`, once a second).
const SNUB_INTERVAL: Duration = Duration::from_secs(1);

// How long upload mode lasts before the torrent tries to download again, in
// case the disk has room by then (libtorrent `optimistic_disk_retry`, ten
// minutes, read by `torrent::second_tick`).
const OPTIMISTIC_DISK_RETRY: Duration = Duration::from_secs(600);

pub enum AddIncomingPeerResult {
    Added,
    AlreadyActive,
    ConcurrencyLimitReached,
    // Finished, with its upload closed: the torrent wants no peers.
    NotWanted,
    PeerListFull,
    // The peer's address is banned for sending bad data.
    Banned,
}

pub struct TorrentStateLive {
    peers: PeerStates,
    pub(crate) shared: Arc<ManagedTorrentShared>,
    metadata: Arc<TorrentMetadata>,
    _locked: RwLock<TorrentStateLocked>,

    pub(crate) files: FileStorage,

    per_piece_locks: Vec<RwLock<()>>,

    stats: AtomicStats,
    lengths: Lengths,

    // Limits how many active (occupying network resources) peers there are at a moment in time.
    peer_semaphore: Arc<Semaphore>,
    // The semaphore's permits: the most connections the torrent holds
    // (libtorrent `torrent::max_connections`).
    peer_limit: usize,
    // The place each connection holds under the torrent's cap and the
    // session's, by the connection's address. A connection is counted here
    // until it ends or another displaces it: one asked to close for another
    // no longer counts, as libtorrent drops a disconnecting connection from
    // `torrent::m_connections` at once. Taken before `peers`.
    places: Mutex<HashMap<PeerHandle, Place>>,

    finished_notify: Notify,
    new_pieces_notify: Arc<Notify>,
    layer_fetch: hash_fetch::LayerFetch,
    // Whether every wanted piece was already present when this run began.
    finished_at_start: bool,

    down_speed_estimator: SpeedEstimator,
    up_speed_estimator: SpeedEstimator,
    cancellation_token: CancellationToken,

    session_stats: Arc<SessionStats>,

    pub(crate) streams: Arc<TorrentStreams>,

    // The peers owed a turn with the upload scheduler, in the order they
    // became ready for one.
    upload_turns: tokio::sync::mpsc::UnboundedSender<PeerHandle>,

    // Whether the torrent speaks ut_pex: advertises it, sends it and reads
    // it. Not on a private torrent, nor in a session without it (libtorrent
    // `ut_pex_plugin::new_connection`, `session::add_extension`).
    pex: bool,

    // Whether the torrent super-seeds: asked to, and a seed when this run
    // began (libtorrent `torrent::m_super_seeding`, turned off for a torrent
    // that is not a seed).
    super_seeding: bool,
    // Whether the peers were last told we only upload (libtorrent tells
    // them on each change, `torrent::send_upload_only`).
    told_upload_only: AtomicBool,
    // When we last saw a complete copy of the torrent: our own, or a seed
    // that sent us data (libtorrent `torrent::m_last_seen_complete`). It is
    // what we send as `complete_ago`; a peer's word never moves it.
    last_seen_complete: Mutex<Option<tokio::time::Instant>>,
    // When the torrent entered upload mode, while it is in it: a write
    // found no room, so it fetches nothing and keeps seeding (libtorrent
    // `torrent::m_upload_mode`, `m_upload_mode_time`).
    upload_mode: watch::Sender<Option<tokio::time::Instant>>,
    // The torrent's files as its web seeds serve them.
    web_layout: web_seed::Layout,

    // Whether the session connects over uTP, which a holepunch needs.
    utp: bool,
}

// One connection's place under the torrent's cap and under the session's.
struct Place {
    // The writer queue of the connection that holds it, which closes it.
    tx: PeerTx,
    // When the connection began (libtorrent `peer_connection::connected_time`).
    since: tokio::time::Instant,
    permit: OwnedSemaphorePermit,
    slot: ConnectionSlot,
}

/// A connection's hold on its place: the place is freed when the
/// connection's task ends, however it ends, unless another connection took
/// it over first.
struct Held {
    state: Arc<TorrentStateLive>,
    addr: PeerHandle,
    tx: PeerTx,
}

impl Drop for Held {
    fn drop(&mut self) {
        let mut places = self.state.places.lock();
        if places
            .get(&self.addr)
            .is_some_and(|place| place.tx.same_channel(&self.tx))
        {
            places.remove(&self.addr);
        }
    }
}

// Where a connection that came to a torrent at its cap finds a place.
enum Room {
    Free(OwnedSemaphorePermit),
    Displacing(PeerHandle),
}

impl TorrentStateLive {
    fn max_request_window(&self) -> usize {
        self.shared
            .options
            .peer_opts
            .max_request_window
            .unwrap_or(DEFAULT_PEER_REQUEST_WINDOW)
            .max(1)
    }

    pub(crate) fn new(
        paused: TorrentStatePaused,
        fatal_errors_tx: tokio::sync::oneshot::Sender<anyhow::Error>,
        cancellation_token: CancellationToken,
    ) -> anyhow::Result<Arc<Self>> {
        let session = paused
            .shared
            .session
            .upgrade()
            .context("session is dead, cannot start torrent")?;
        let session_stats = session.stats.clone();
        let down_speed_estimator = SpeedEstimator::default();
        let up_speed_estimator = SpeedEstimator::default();

        let have_bytes = paused.chunk_tracker.get_hns().have_bytes;
        let finished_at_start = paused.chunk_tracker.get_hns().finished();
        let lengths = *paused.chunk_tracker.get_lengths();
        let super_seeding =
            paused.shared.options.super_seeding && paused.chunk_tracker.bytes_left() == 0;
        let peer_limit = paused.shared.options.peer_limit.unwrap_or(128);

        let (upload_turns, upload_turns_rx) = unbounded_channel::<PeerHandle>();

        let state = Arc::new(TorrentStateLive {
            shared: paused.shared.clone(),
            metadata: paused.metadata.clone(),
            peers: PeerStates::founded_on(
                paused.peers,
                session_stats.peers.clone(),
                lengths.total_pieces(),
            )
            .allowing_multiple_connections_per_ip(session.allow_multiple_connections_per_ip),
            _locked: RwLock::new(TorrentStateLocked {
                pieces: Some(PieceTracker::new(paused.chunk_tracker)),
                fatal_errors_tx: Some(fatal_errors_tx),
                unflushed_bitv_bytes: 0,
            }),
            files: paused.files,
            stats: AtomicStats {
                have_bytes: AtomicU64::new(have_bytes),
                ..Default::default()
            },
            lengths,
            peer_semaphore: Arc::new(Semaphore::new(peer_limit)),
            peer_limit,
            places: Mutex::new(HashMap::new()),
            new_pieces_notify: Arc::new(Notify::new()),
            layer_fetch: hash_fetch::LayerFetch::new(paused.metadata.v2.as_ref()),
            finished_notify: Notify::new(),
            finished_at_start,
            down_speed_estimator,
            up_speed_estimator,
            cancellation_token,
            session_stats,
            streams: paused.streams,
            per_piece_locks: (0..lengths.total_pieces())
                .map(|_| RwLock::new(()))
                .collect(),
            upload_turns,
            pex: !session.disable_pex && !paused.metadata.info.info().private,
            super_seeding,
            // What a handshake of this run's first connections says.
            told_upload_only: AtomicBool::new(finished_at_start && !super_seeding),
            last_seen_complete: Mutex::new(None),
            upload_mode: watch::Sender::new(None),
            web_layout: web_seed::Layout::new(&paused.metadata, paused.shared.info_hash),
            utp: session.utp,
        });

        // Upload mode ends once it lasted OPTIMISTIC_DISK_RETRY (libtorrent
        // `torrent::second_tick`).
        state.spawn(
            debug_span!(parent: state.shared.span.clone(), "upload_mode_retry"),
            format!("[{}]upload_mode_retry", state.shared.id),
            {
                let mut entered = state.upload_mode.subscribe();
                let state = Arc::downgrade(&state);
                async move {
                    loop {
                        let Ok(Some(at)) = entered.wait_for(Option::is_some).await.map(|at| *at)
                        else {
                            return Ok(());
                        };
                        tokio::time::sleep_until(at + OPTIMISTIC_DISK_RETRY).await;
                        let Some(state) = state.upgrade() else {
                            return Ok(());
                        };
                        state.set_upload_mode(false);
                    }
                }
            },
        );

        state.spawn(
            debug_span!(parent: state.shared.span.clone(), "speed_estimator_updater"),
            format!("[{}]speed_estimator_updater", state.shared.id),
            {
                let state = Arc::downgrade(&state);
                async move {
                    loop {
                        let state = match state.upgrade() {
                            Some(state) => state,
                            None => return Ok(()),
                        };
                        let now = Instant::now();
                        let stats = state.stats_snapshot();
                        let fetched = stats.fetched_bytes;
                        let remaining = state
                            .lock_read("get_remaining_bytes")
                            .get_chunks()?
                            .get_remaining_bytes();
                        state
                            .down_speed_estimator
                            .add_snapshot(fetched, Some(remaining), now);
                        state
                            .up_speed_estimator
                            .add_snapshot(stats.uploaded_bytes, None, now);
                        for peer in state.peers.states.iter() {
                            if let Some(live) = peer.value().get_live() {
                                live.sample_rates(&peer.value().stats.counters, now);
                            }
                        }
                        tokio::time::sleep(RATE_SAMPLE_INTERVAL).await;
                    }
                }
            },
        );

        state.spawn(
            debug_span!(parent: state.shared.span.clone(), "peer_adder"),
            format!("[{}]peer_adder", state.shared.id),
            state.clone().task_peer_adder(),
        );

        state.spawn(
            debug_span!(parent: state.shared.span.clone(), "upload_scheduler"),
            format!("[{}]upload_scheduler", state.shared.id),
            state.clone().task_upload_scheduler(upload_turns_rx),
        );

        state.spawn(
            debug_span!(parent: state.shared.span.clone(), "web_seeds"),
            format!("[{}]web_seeds", state.shared.id),
            state.clone().task_web_seeds(),
        );
        Ok(state)
    }

    /// Connects to the web seeds that are due every WEB_SEED_TICK, and as
    /// soon as one may have become due (libtorrent `torrent::second_tick`,
    /// `maybe_connect_web_seeds`).
    async fn task_web_seeds(self: Arc<Self>) -> crate::Result<()> {
        let mut tick = tokio::time::interval(WEB_SEED_TICK);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                _ = tick.tick() => {}
                _ = self.shared.web_seeds.changed() => {}
            }
            self.maybe_connect_web_seeds()?;
        }
    }

    /// Connects to the web seeds that are due, while the torrent misses a
    /// piece and has a connection to spare under its own cap and the
    /// session's. A web seed is not on the peer list and takes no connect
    /// from the per-second budget, which paces the peer list's connects
    /// only (libtorrent `torrent::maybe_connect_web_seeds`,
    /// `session_impl::try_connect_more_peers`).
    fn maybe_connect_web_seeds(self: &Arc<Self>) -> crate::Result<()> {
        if self.is_finished_and_no_active_streams() {
            return Ok(());
        }
        let session = self
            .shared
            .session
            .upgrade()
            .ok_or(Error::SessionDestroyed)?;
        let registry = &self.shared.web_seeds;
        let due = registry.due(
            tokio::time::Instant::now(),
            |handle| self.peers.is_banned(handle.ip()),
            |lacks| self.web_layout.bitfield(lacks).any(),
        );
        for due in due {
            // The IP filter: a seed it blocks is tried again later.
            if due.endpoint.is_some_and(|e| !self.may_connect_to(e)) {
                continue;
            }
            let Ok(permit) = self.peer_semaphore.clone().try_acquire_owned() else {
                return Ok(());
            };
            let Some(slot) = session.connection_slots.try_open() else {
                return Ok(());
            };
            let Some(link) = registry.link(due.id) else {
                continue;
            };
            let Some((rx, tx)) = self.peers.admit_web_seed(link.handle()) else {
                continue;
            };
            let held = self.hold_place(link.handle(), &tx, permit, slot);
            let clients = session.http_clients.clone();
            let resolver = session.resolver.clone();
            self.spawn(
                debug_span!(parent: self.shared.span.clone(), "web_seed", seed = ?link.handle()),
                format!(
                    "[{}][web_seed={}]manage_web_seed",
                    self.shared.id,
                    link.handle()
                ),
                aframe!(
                    self.clone()
                        .task_manage_web_seed(link, clients, resolver, rx, tx, held)
                ),
            );
        }
        Ok(())
    }

    /// The life of one connection to a web seed: the requester asks it for
    /// blocks as it asks any peer.
    async fn task_manage_web_seed(
        self: Arc<Self>,
        link: SeedLink,
        clients: Arc<dyn HttpClients>,
        resolver: Arc<dyn Resolver>,
        rx: PeerRx,
        tx: PeerTx,
        _held: Held,
    ) -> crate::Result<()> {
        let addr = link.handle();
        let counters = self
            .peers
            .with_peer(addr, |p| p.stats.counters.clone())
            .ok_or(Error::BugPeerNotFound)?;
        let handler = PeerHandler {
            addr,
            incoming: false,
            on_bitfield_notify: Default::default(),
            speaks_v2: AtomicBool::new(false),
            sent_dht_port: AtomicBool::new(false),
            flow_control: Mutex::new(PeerFlowControl::new(self.max_request_window())),
            state: self.clone(),
            tx,
            counters,
            first_message_received: AtomicBool::new(false),
            cancel_token: self.cancellation_token.child_token(),
            client_name_and_version: self.shared.client_name_and_version().to_owned(),
        };
        let _token_guard = handler.cancel_token.clone().drop_guard();
        handler
            .counters
            .outgoing_connection_attempts
            .fetch_add(1, Ordering::Relaxed);
        // The session's IP filter, on the address the seed's host resolved to.
        let allowed = |addr| self.may_connect_to(addr);
        let connection =
            WebSeedConnection::new(clients, resolver, &allowed, &self.web_layout, &link);
        let res = tokio::select! {
            r = handler.task_peer_chunk_requester() => r,
            r = connection.run(&handler, rx) => r,
        };
        match res {
            Ok(()) => handler.on_peer_died(None)?,
            Err(e) => {
                debug!("web seed connection failed: {e:#}");
                link.failed();
                handler.on_peer_died(Some(e))?;
            }
        }
        Ok(())
    }

    #[track_caller]
    pub(crate) fn spawn(
        &self,
        span: tracing::Span,
        name: impl Into<Cow<'static, str>>,
        fut: impl std::future::Future<Output = crate::Result<()>> + Send + 'static,
    ) {
        spawn_with_cancel(span, name, self.cancellation_token.clone(), fut);
    }

    pub fn down_speed_estimator(&self) -> &SpeedEstimator {
        &self.down_speed_estimator
    }

    pub fn up_speed_estimator(&self) -> &SpeedEstimator {
        &self.up_speed_estimator
    }

    /// Takes an incoming connection that holds `slot` under the session's
    /// cap. At the torrent's own cap it displaces a connection that ranks
    /// lower, or is refused (libtorrent `torrent::attach_peer`).
    pub(crate) fn add_incoming_peer(
        self: &Arc<Self>,
        checked_peer: CheckedIncomingConnection,
        slot: ConnectionSlot,
    ) -> anyhow::Result<AddIncomingPeerResult> {
        if !self.wants_peers() {
            debug!(addr = %checked_peer.addr, "finished with upload closed, refusing incoming peer");
            return Ok(AddIncomingPeerResult::NotWanted);
        }
        let (tx, rx) = unbounded_channel();
        // Held until the newcomer holds its place, so the connection it
        // displaces keeps its own until then.
        let mut places = self.places.lock();
        let room = match self.peer_semaphore.clone().try_acquire_owned() {
            Ok(permit) => Room::Free(permit),
            Err(_) => match self.displaced_by(&places, checked_peer.addr) {
                Some(displaced) => Room::Displacing(displaced),
                None => {
                    drop(places);
                    debug!("limit of live peers reached, keeping incoming peer for later");
                    if self.may_connect_to(checked_peer.addr) {
                        self.peers
                            .add_refused_incoming(checked_peer.addr, checked_peer.kind);
                    }
                    return Ok(AddIncomingPeerResult::ConcurrencyLimitReached);
                }
            },
        };

        // The port the connection came to: the one we listen on.
        let our_port = self
            .shared
            .session
            .upgrade()
            .and_then(|s| s.listen_addr())
            .map_or(0, |a| a.port());
        let counters = match self.peers.add_incoming(
            checked_peer.addr,
            checked_peer.handshake.peer_id,
            tx.clone(),
            checked_peer.kind,
            our_port,
        ) {
            Ok(counters) => counters,
            Err(IncomingRefused::AlreadyActive) => {
                debug!(
                    addr = %checked_peer.addr,
                    kind = %checked_peer.kind,
                    "peer already active, ignoring incoming connection"
                );
                return Ok(AddIncomingPeerResult::AlreadyActive);
            }
            Err(IncomingRefused::ListFull) => {
                debug!(
                    addr = %checked_peer.addr,
                    "peer list holds only connected peers, dropping incoming peer"
                );
                return Ok(AddIncomingPeerResult::PeerListFull);
            }
            Err(IncomingRefused::Banned) => {
                debug!(addr = %checked_peer.addr, "peer is banned, dropping incoming peer");
                return Ok(AddIncomingPeerResult::Banned);
            }
        };
        atomic_inc(&counters.incoming_connections);
        // The newcomer takes the place of the connection it displaces, under
        // both caps; the slot it came with is let go.
        let (permit, slot) = match room {
            Room::Free(permit) => (permit, slot),
            Room::Displacing(displaced) => {
                let place = Self::give_up_place(&mut places, displaced)
                    .expect("a place is given up under the lock it was found under");
                debug!(%displaced, "displaced by an incoming connection");
                (place.permit, place.slot)
            }
        };
        let held = self.hold_place_in(&mut places, checked_peer.addr, &tx, permit, slot);
        drop(places);

        self.spawn(
            debug_span!(
                parent: self.shared.span.clone(),
                "manage_incoming_peer",
                addr = %checked_peer.addr
            ),
            format!(
                "[{}][addr={}]manage_incoming_peer",
                self.shared.id, checked_peer.addr
            ),
            aframe!(
                self.clone()
                    .task_manage_incoming_peer(checked_peer, counters, tx, rx, held)
            ),
        );
        Ok(AddIncomingPeerResult::Added)
    }

    async fn task_upload_scheduler(
        self: Arc<Self>,
        mut turns: tokio::sync::mpsc::UnboundedReceiver<PeerHandle>,
    ) -> crate::Result<()> {
        // The peers take turns in the order they became ready for one, and a
        // turn hands one block. A peer is ready again only once its writer
        // takes the block, and then waits behind every peer that became
        // ready meanwhile: however many blocks one peer asked for, the next
        // peer waits one block of it (libtorrent `fill_send_buffer`, one
        // send buffer per peer, each held to its watermark). The writer pays
        // for the block as it sends it.
        while let Some(addr) = turns.recv().await {
            self.peers
                .with_live_mut(addr, "hand_upload", |live| live.hand_upload());
        }
        Ok(())
    }

    /// `addr` became ready to be handed a block: it takes its turn behind
    /// those that became ready before it.
    fn upload_turn(&self, addr: PeerHandle) {
        let _ = self.upload_turns.send(addr);
    }

    async fn task_manage_incoming_peer(
        self: Arc<Self>,
        checked_peer: CheckedIncomingConnection,
        counters: Arc<AtomicPeerCounters>,
        tx: PeerTx,
        rx: PeerRx,
        _held: Held,
    ) -> crate::Result<()> {
        let handler = PeerHandler {
            addr: checked_peer.addr,
            incoming: true,
            on_bitfield_notify: Default::default(),
            speaks_v2: AtomicBool::new(false),
            sent_dht_port: AtomicBool::new(false),
            flow_control: Mutex::new(PeerFlowControl::new(self.max_request_window())),
            state: self.clone(),
            tx,
            counters,
            first_message_received: AtomicBool::new(false),
            cancel_token: self.cancellation_token.child_token(),
            client_name_and_version: self.shared.client_name_and_version().to_owned(),
        };
        let _token_guard = handler.cancel_token.clone().drop_guard();
        let options = self.shared.options.peer_opts;
        let peer_connection = PeerConnection::new(
            checked_peer.addr,
            self.shared.info_hashes,
            self.shared.peer_id,
            &handler,
            Some(options),
            self.shared.spawner.clone(),
            self.shared.connector.clone(),
        );
        let requester = handler.task_peer_chunk_requester();

        let res = tokio::select! {
            r = requester => {r}
            r = peer_connection.manage_peer_incoming(rx, checked_peer) => {r}
        };

        match res {
            // We disconnected the peer ourselves as we don't need it
            Ok(()) => {
                handler.on_peer_died(None)?;
            }
            Err(e) => {
                debug!("error managing peer: {:#}", e);
                handler.on_peer_died(Some(e))?;
            }
        };
        Ok(())
    }

    async fn task_manage_outgoing_peer(
        self: Arc<Self>,
        addr: SocketAddr,
        rx: PeerRx,
        tx: PeerTx,
        _held: Held,
    ) -> crate::Result<()> {
        let state = self;
        let options = state.shared.options.peer_opts;
        let base_timeout = options.connect_timeout.unwrap_or(DEFAULT_CONNECT_TIMEOUT);
        let (counters, transports, connect_timeout) = state
            .peers
            .with_peer(addr, |p| {
                (
                    p.stats.counters.clone(),
                    p.transports(),
                    p.connect_timeout(base_timeout),
                )
            })
            .ok_or(Error::BugPeerNotFound)?;

        let handler = PeerHandler {
            addr,
            incoming: false,
            on_bitfield_notify: Default::default(),
            speaks_v2: AtomicBool::new(false),
            sent_dht_port: AtomicBool::new(false),
            flow_control: Mutex::new(PeerFlowControl::new(state.max_request_window())),
            state: state.clone(),
            tx,
            counters,
            first_message_received: AtomicBool::new(false),
            cancel_token: state.cancellation_token.child_token(),
            client_name_and_version: state.shared.client_name_and_version().to_owned(),
        };
        let _token_guard = handler.cancel_token.clone().drop_guard();

        let options = PeerConnectionOptions {
            connect_timeout: Some(connect_timeout),
            ..options
        };
        let peer_connection = PeerConnection::new(
            addr,
            state.shared.info_hashes,
            state.shared.peer_id,
            &handler,
            Some(options),
            state.shared.spawner.clone(),
            state.shared.connector.clone(),
        )
        .over(transports);
        let requester = aframe!(
            handler
                .task_peer_chunk_requester()
                .instrument(debug_span!("chunk_requester"))
        );
        let conn_manager = aframe!(
            peer_connection
                .manage_peer_outgoing(rx)
                .instrument(debug_span!("peer_connection"))
        );

        handler
            .counters
            .outgoing_connection_attempts
            .fetch_add(1, Ordering::Relaxed);
        let res = tokio::select! {
            r = requester => {r}
            r = conn_manager => {r}
        };

        match res {
            // We disconnected the peer ourselves as we don't need it
            Ok(()) => {
                handler.on_peer_died(None)?;
            }
            Err(e) => {
                debug!("error managing peer: {:#}", e);
                handler.on_peer_died(Some(e))?;
            }
        }
        Ok(())
    }

    async fn task_peer_adder(self: Arc<Self>) -> crate::Result<()> {
        let state = self;
        // Whether we were finished when the connect candidates were last
        // looked over; None before they ever were.
        let mut candidates_finished = None;
        loop {
            if !state.wants_peers() {
                // Nothing to fetch and nothing to give: connect to no one until
                // the peers are wanted again.
                state.peers.candidates_changed().await;
                continue;
            }
            state.peers.wait_for_candidate().await;
            let slots = state
                .shared
                .session
                .upgrade()
                .ok_or(Error::SessionDestroyed)?
                .connection_slots
                .clone();
            // The torrent's own cap first, so a torrent at it holds no place
            // under the session's; then a place under the session's cap and
            // a connect from its per-second budget, both shared with every
            // torrent in turn.
            let permit = state.peer_semaphore.clone().acquire_owned().await?;
            let slot = slots.open().await?;
            // Whether we are finished decides who is a connect candidate:
            // when it changed, every peer is looked at again (libtorrent
            // `peer_list::connect_one_peer`, `recalculate_connect_candidates`).
            let finished = state.is_finished_and_no_active_streams();
            if candidates_finished != Some(finished) {
                for mut peer in state.peers.states.iter_mut() {
                    peer.value_mut().finished(finished, &state.peers);
                }
                candidates_finished = Some(finished);
            }
            // The slots may have taken a while; connect to whoever is due now.
            let Some((addr, rx, tx)) = state.peers.connect_next(tokio::time::Instant::now()) else {
                continue;
            };
            slots.begin_connect().await;
            let held = state.hold_place(addr, &tx, permit, slot);
            state.spawn(
                debug_span!(parent: state.shared.span.clone(), "manage_peer", peer = ?addr),
                format!("[{}][addr={addr}]manage_peer", state.shared.id),
                aframe!(state.clone().task_manage_outgoing_peer(addr, rx, tx, held)),
            );
        }
    }

    // Whether we may open a connection to `addr`: a port to connect to
    // (libtorrent `peer_list::add_peer` takes no port-0 peer), at an
    // address the session may reach (`Session::may_reach`, the rule
    // trackers pass too). The peer list holds no address we may not
    // connect to.
    fn may_connect_to(&self, addr: SocketAddr) -> bool {
        use crate::tracker::Unreachable;
        let Some(session) = self.shared.session.upgrade() else {
            return false;
        };

        if addr.port() == 0 {
            debug!(?addr, "skipping peer with port 0");
            return false;
        }

        match session.may_reach(addr.ip()) {
            Ok(()) => true,
            Err(Unreachable::Family) => {
                debug!(?addr, "skipping ipv6 peer (ipv4_only=true)");
                false
            }
            // libtorrent posts `peer_blocked_alert` for each.
            Err(Unreachable::Filtered) => {
                session
                    .stats
                    .counters
                    .blocked_outgoing
                    .fetch_add(1, Ordering::Relaxed);
                debug!(?addr, "blocked outgoing connection (by the IP filter)");
                false
            }
        }
    }

    pub fn torrent(&self) -> &ManagedTorrentShared {
        &self.shared
    }

    pub fn info(&self) -> &ValidatedTorrentMetaV1Info<ByteBufOwned> {
        &self.metadata.info
    }
    pub fn info_hash(&self) -> Id20 {
        self.shared.info_hash
    }
    /// What a checkpoint of the run records; see
    /// [`crate::chunk_tracker::ChunkTracker::checkpoint`].
    pub(crate) fn checkpoint(&self) -> anyhow::Result<super::initializing::Checkpoint> {
        let g = self.lock_read("checkpoint");
        Ok(g.get_chunks()?
            .checkpoint(&*self.files, self.metadata.file_infos.len()))
    }

    /// The peers and bans a restart is to begin with; see
    /// [`peers::ResumePeers`].
    pub(crate) fn resume_peers(&self) -> peers::ResumePeers {
        self.peers.resume_peers()
    }

    /// Bans `ip`; see [`PeerStates::ban`].
    pub(crate) fn ban_peer(&self, ip: std::net::IpAddr) {
        self.peers.ban(ip);
    }

    pub fn peer_id(&self) -> Id20 {
        self.shared.peer_id
    }
    pub(crate) fn file_ops(&self) -> FileOps<'_> {
        FileOps::new(
            &self.metadata,
            &*self.files,
            self.shared.options.disable_v1_hashes,
        )
    }

    pub(crate) fn lock_read(
        &self,
        reason: &'static str,
    ) -> TimedExistence<RwLockReadGuard<'_, TorrentStateLocked>> {
        TimedExistence::new(timeit(reason, || self._locked.read()), reason)
    }
    pub(crate) fn lock_write(
        &self,
        reason: &'static str,
    ) -> TimedExistence<RwLockWriteGuard<'_, TorrentStateLocked>> {
        TimedExistence::new(timeit(reason, || self._locked.write()), reason)
    }

    fn set_peer_live(&self, handle: PeerHandle, h: Handshake, connection_kind: ConnectionKind) {
        self.peers.with_peer_mut(handle, "set_peer_live", |p| {
            p.connecting_to_live(h.peer_id, &self.peers, connection_kind);
            // An incoming connection is live before its handshake gets here.
            if let Some(live) = p.get_live_mut() {
                live.fast = h.supports_fast();
            }
        });
    }

    pub fn get_uploaded_bytes(&self) -> u64 {
        self.stats.uploaded_bytes.load(Ordering::Relaxed)
    }
    pub fn get_downloaded_bytes(&self) -> u64 {
        self.stats
            .downloaded_and_checked_bytes
            .load(Ordering::Acquire)
    }

    /// Bytes of the pieces that failed their hash check this run.
    pub fn get_failed_bytes(&self) -> u64 {
        self.stats.failed_bytes.load(Ordering::Relaxed)
    }

    /// Bytes of the blocks received this run and not used.
    pub fn get_redundant_bytes(&self) -> u64 {
        self.stats.redundant_bytes.load(Ordering::Relaxed)
    }

    /// A block of `len` bytes arrived that is not used: it was not asked of
    /// the peer any more, or another copy came first (libtorrent
    /// `torrent::add_redundant_bytes`, from `peer_connection::incoming_piece`).
    fn add_redundant_bytes(&self, len: u64) {
        self.stats.redundant_bytes.fetch_add(len, Ordering::Relaxed);
    }

    pub fn get_approx_have_bytes(&self) -> u64 {
        self.stats.have_bytes.load(Ordering::Relaxed)
    }

    /// How long ago we last saw a complete copy of the torrent, none if we
    /// never did; no time at all while we have every piece (libtorrent
    /// `torrent::time_since_complete`). The `complete_ago` we send.
    pub fn complete_ago(&self) -> Option<Duration> {
        let now = tokio::time::Instant::now();
        self.last_seen_complete(now)
            .map(|last| now.saturating_duration_since(last))
    }

    /// How long ago anyone connected to us saw a complete copy: we, or a
    /// peer by its `complete_ago`, the latest sighting standing; none if
    /// none did (libtorrent `torrent::second_tick`'s
    /// `m_swarm_last_seen_complete`). For status only, never sent.
    pub fn swarm_complete_ago(&self) -> Option<Duration> {
        let now = tokio::time::Instant::now();
        let theirs = self
            .peers
            .states
            .iter()
            .filter_map(|e| e.value().get_live()?.last_seen_complete)
            .max();
        self.last_seen_complete(now)
            .max(theirs)
            .map(|last| now.saturating_duration_since(last))
    }

    // Our last sighting of a complete copy, which is now while we have
    // every piece.
    fn last_seen_complete(&self, now: tokio::time::Instant) -> Option<tokio::time::Instant> {
        if self.get_approx_have_bytes() == self.lengths.total_length() {
            self.seen_complete(now);
        }
        *self.last_seen_complete.lock()
    }

    /// A complete copy of the torrent was seen at `at`; the latest sighting
    /// stands (libtorrent `torrent::seen_complete`).
    fn seen_complete(&self, at: tokio::time::Instant) {
        let mut last = self.last_seen_complete.lock();
        if last.is_none_or(|last| last < at) {
            *last = Some(at);
        }
    }

    pub fn get_hns(&self) -> Option<HaveNeededSelected> {
        self.lock_read("get_hns")
            .get_chunks()
            .ok()
            .map(|c| *c.get_hns())
    }

    fn transmit_haves(&self, index: ValidPieceIndex) {
        if self.shared.options.disable_upload() {
            return;
        }
        self.peers.announce_have(index);
    }

    /// Our interest in the peer `live` is whether it has a piece we want and
    /// lack: a selected one we do not have, or one a stream reads next; none
    /// in upload mode. The peer is told when that changes. Returns it
    /// (libtorrent `peer_connection::do_update_interest`, not interested in
    /// a torrent that only uploads).
    fn update_interest(&self, live: &mut LivePeerState) -> bool {
        let interesting = !self.upload_mode() && {
            let g = self.lock_read("update_interest");
            g.get_chunks().is_ok_and(|chunks| {
                let has = |p: ValidPieceIndex| live.bitfield.get(p.get_usize()).is_some_and(|b| *b);
                chunks.wants_any_of(&live.bitfield)
                    || self
                        .streams
                        .iter_next_pieces(&self.lengths)
                        .any(|p| has(p) && !chunks.is_piece_have(p))
            })
        };
        if live.interest(interesting, tokio::time::Instant::now()) {
            let message = if interesting {
                Message::Interested
            } else {
                Message::NotInterested
            };
            let _ = live.tx.send(WriterRequest::Message(message));
        }
        interesting
    }

    /// We have `piece` now: a peer we want it of may have nothing else we
    /// want (libtorrent `torrent::piece_passed` updates the interest of each
    /// interesting peer that has the piece).
    fn update_interest_of_holders(&self, piece: ValidPieceIndex) {
        for mut entry in self.peers.states.iter_mut() {
            if let Some(live) = entry.value_mut().get_live_mut()
                && live.is_interesting()
                && live.bitfield.get(piece.get_usize()).is_some_and(|b| *b)
            {
                self.update_interest(live);
            }
        }
    }

    /// `source` named `addr` (libtorrent `torrent::add_peer`).
    pub(crate) fn add_peer(&self, addr: SocketAddr, source: PeerSourceFlag) {
        if self.may_connect_to(addr) {
            self.peers.add_peer(addr, source);
        }
    }

    /// A holepunch asks us to connect to `addr`: over uTP, so only when we
    /// speak it (libtorrent `bt_peer_connection::on_holepunch`, `connect`).
    fn holepunch(&self, addr: SocketAddr) {
        if self.utp && self.may_connect_to(addr) {
            self.peers.holepunch(addr);
        }
    }

    /// The torrent's limits grant bytes of the `want` a peer asks for, and
    /// the session's then grant some or all of those; the torrent has back
    /// what the session did not grant (libtorrent
    /// `peer_connection::request_bandwidth` asks every channel over the peer
    /// for one request).
    async fn grant_bandwidth(&self, channel: Channel, want: u64) -> u64 {
        let torrent = &self.shared.options.ratelimits;
        let granted = torrent.request(channel, want).await;
        let Some(session) = self.shared.session.upgrade() else {
            return granted;
        };
        let both = session.ratelimits.request(channel, granted).await;
        torrent.give_back(channel, granted - both);
        both
    }

    /// Bytes granted to a peer that will not cross its socket go back to
    /// the torrent's limits and the session's.
    fn give_back_bandwidth(&self, channel: Channel, bytes: u64) {
        self.shared.options.ratelimits.give_back(channel, bytes);
        if let Some(session) = self.shared.session.upgrade() {
            session.ratelimits.give_back(channel, bytes);
        }
    }

    pub fn stats_snapshot(&self) -> StatsSnapshot {
        use Ordering::*;
        let downloaded_bytes = self.stats.downloaded_and_checked_bytes.load(Relaxed);
        StatsSnapshot {
            downloaded_and_checked_bytes: downloaded_bytes,
            downloaded_and_checked_pieces: self.stats.downloaded_and_checked_pieces.load(Relaxed),
            fetched_bytes: self.stats.fetched_bytes.load(Relaxed),
            uploaded_bytes: self.stats.uploaded_bytes.load(Relaxed),
            fetched_protocol_bytes: self.stats.fetched_protocol_bytes.load(Relaxed),
            uploaded_protocol_bytes: self.stats.uploaded_protocol_bytes.load(Relaxed),
            total_piece_download_ms: self.stats.total_piece_download_ms.load(Relaxed),
            peer_stats: self.peers.stats(),
        }
    }

    pub fn per_peer_stats_snapshot(&self, filter: PeerStatsFilter) -> PeerStatsSnapshot {
        PeerStatsSnapshot {
            peers: self
                .peers
                .states
                .iter()
                .filter(|e| filter.state.matches(e.value().get_state()))
                .map(|e| (e.key().to_string(), e.value().into()))
                .collect(),
        }
    }

    /// The torrent's connections to peers, by address (libtorrent
    /// `torrent::get_peer_info`). A web seed is no peer of the swarm, and
    /// is not listed.
    pub fn peer_infos(&self) -> Vec<PeerInfo> {
        // The choker's lock goes before the peer table's.
        let optimistic = self
            .shared
            .session
            .upgrade()
            .map(|session| session.choker.optimistic(self.shared.id))
            .unwrap_or_default();
        let total_pieces = self.lengths.total_pieces();
        let mut infos: Vec<PeerInfo> = self
            .peers
            .states
            .iter()
            .filter(|e| !web_seed::is_web_seed(*e.key()))
            .filter_map(|e| PeerInfo::of(e.value(), total_pieces, optimistic.contains(e.key())))
            .collect();
        infos.sort_by_key(|info| info.addr);
        infos
    }

    /// Resolves once the torrent is finished. The wait is taken before the
    /// check, so a finish between the two still wakes it (libtorrent
    /// `alert_manager::wait_for_alert` checks the queue and waits under the
    /// one lock `maybe_notify` signals under).
    pub async fn wait_until_completed(&self) {
        let mut finished = std::pin::pin!(self.finished_notify.notified());
        finished.as_mut().enable();
        if self.is_finished() {
            return;
        }
        finished.await;
    }

    pub fn pause(&self) -> anyhow::Result<TorrentStatePaused> {
        self.cancellation_token.cancel();

        // The peer list is read before the state lock: a dying peer holds
        // its own entry while it takes the state lock, so the state lock
        // never waits on a peer entry.
        let peers = self.peers.idle_list();
        let mut g = self.lock_write("pause");

        // It should be impossible to make a fatal error after pausing.
        g.fatal_errors_tx.take();

        let piece_tracker = g
            .pieces
            .take()
            .context("bug: pausing already paused torrent")?;
        // into_chunks() will requeue any in-flight pieces
        let chunk_tracker = piece_tracker.into_chunks();

        *self.shared.last_run.lock() = Some(RunCounters {
            uploaded_bytes: self.get_uploaded_bytes(),
            downloaded_bytes: self.get_downloaded_bytes(),
            corrupt_bytes: self.get_failed_bytes(),
            redundant_bytes: self.get_redundant_bytes(),
            left_bytes: chunk_tracker.bytes_left(),
        });

        Ok(TorrentStatePaused {
            shared: self.shared.clone(),
            metadata: self.metadata.clone(),
            files: self.files.take()?,
            chunk_tracker,
            streams: self.streams.clone(),
            peers,
        })
    }

    fn on_fatal_error(&self, e: anyhow::Error) -> anyhow::Result<()> {
        let mut g = self.lock_write("fatal_error");
        let tx = g
            .fatal_errors_tx
            .take()
            .context("fatal_errors_tx already taken")?;
        let res = anyhow::anyhow!("fatal error: {:?}", e);
        if tx.send(e).is_err() {
            warn!(id=self.shared.id, info_hash=?self.shared.info_hash, "there's nowhere to send fatal error, receiver is dead");
        }
        Err(res)
    }

    pub(crate) fn update_only_files(&self, only_files: &HashSet<usize>) -> anyhow::Result<()> {
        let hns = self
            .lock_write("update_only_files")
            .get_pieces_mut()?
            .update_only_files(&self.metadata.file_infos, only_files)?;
        // The peers' requesters look again at what they may ask for, and
        // whether we want anything at all (libtorrent
        // `torrent::update_piece_priorities` calls `update_interest` on
        // every peer).
        self.new_pieces_notify.notify_waiters();
        // With the state lock released. A dying peer holds its shard of the peer table
        // while it asks whether the torrent is finished (on_peer_died), so touching the
        // table under the state lock is the reverse order, and the two deadlock.
        self.update_upload_only();
        if !hns.finished() {
            self.reconnect_all_not_needed_peers();
        }
        Ok(())
    }

    pub(crate) fn set_piece_priorities(
        &self,
        priorities: impl IntoIterator<Item = (ValidPieceIndex, PiecePriority)>,
    ) -> anyhow::Result<()> {
        let hns = self
            .lock_write("set_piece_priorities")
            .get_pieces_mut()?
            .set_piece_priorities(priorities);
        // As in update_only_files().
        self.new_pieces_notify.notify_waiters();
        // Wanting a skipped piece again may make peers we let go useful. With the
        // state lock released, for the reason given in update_only_files().
        self.update_upload_only();
        if !hns.finished() {
            self.reconnect_all_not_needed_peers();
        }
        Ok(())
    }

    // Tells the peers that take upload_only when whether we only upload
    // changed: we finished, or want pieces again (libtorrent
    // `torrent::send_upload_only`, from `finished` and `resume_download`).
    fn update_upload_only(&self) {
        let upload_only = self.is_upload_only();
        if self.told_upload_only.swap(upload_only, Ordering::Relaxed) != upload_only {
            self.peers.send_upload_only(upload_only);
        }
    }

    // We only upload the torrent: it is in upload mode, or has every piece
    // it wants and no stream wants more; a super-seed never says so, as a
    // peer may take it to have one piece and leave (libtorrent
    // `torrent::is_upload_only`, `bt_peer_connection::write_extensions`).
    fn is_upload_only(&self) -> bool {
        !self.super_seeding && (self.upload_mode() || self.is_finished_and_no_active_streams())
    }

    /// Whether the torrent is in upload mode: a write found no room, so it
    /// fetches nothing and keeps seeding (libtorrent `torrent::upload_mode`).
    pub fn upload_mode(&self) -> bool {
        self.upload_mode.borrow().is_some()
    }

    /// Resolves once the torrent is in upload mode, when `on` holds, or out
    /// of it.
    pub async fn wait_for_upload_mode(&self, on: bool) {
        let mut mode = self.upload_mode.subscribe();
        let _ = mode.wait_for(|at| at.is_some() == on).await;
    }

    /// Enters or leaves upload mode (libtorrent `torrent::set_upload_mode`).
    /// Entering, every request outstanding is cancelled and every piece in
    /// flight goes back to the queue, each under its peer's lock, under
    /// which a requester picks and asks; leaving, the requesters ask again.
    /// Either way each requester looks at its interest again, and the peers
    /// are told whether we only upload.
    fn set_upload_mode(&self, on: bool) {
        let now = tokio::time::Instant::now();
        let changed = self.upload_mode.send_if_modified(|at| {
            let changed = at.is_some() != on;
            if changed {
                *at = on.then_some(now);
            }
            changed
        });
        if !changed {
            return;
        }
        debug!(id = self.shared.id, on, "upload mode");
        if on {
            for mut entry in self.peers.states.iter_mut() {
                let addr = *entry.key();
                if let Some(live) = entry.value_mut().get_live_mut() {
                    if let Ok(pieces) = self.lock_write("set_upload_mode").get_pieces_mut() {
                        pieces.release_pieces_owned_by(addr);
                    }
                    live.cancel_all_requests();
                }
            }
        }
        self.new_pieces_notify.notify_waiters();
        self.update_upload_only();
    }

    /// A write failed. One that later writes will likely repeat puts the
    /// torrent in upload mode; returns whether it was one (libtorrent
    /// `torrent::handle_disk_error`).
    fn handle_disk_error(&self, error: &anyhow::Error) -> bool {
        let refused = crate::storage::write_refused(error);
        if refused {
            warn!(
                id = self.shared.id,
                info_hash = ?self.shared.info_hash,
                "a write found no room, only uploading: {error:#}"
            );
            self.set_upload_mode(true);
        }
        refused
    }

    // If we have all selected pieces but not necessarily all pieces.
    pub(crate) fn is_finished(&self) -> bool {
        self.get_hns().map(|h| h.finished()).unwrap_or_default()
    }

    /// Whether the torrent was missing wanted pieces when this run began and
    /// has all of them now.
    pub(crate) fn finished_during_run(&self) -> bool {
        !self.finished_at_start && self.is_finished()
    }

    fn has_active_streams_unfinished_files(&self, state: &TorrentStateLocked) -> bool {
        let chunks = match state.get_chunks() {
            Ok(c) => c,
            Err(_) => return false,
        };
        self.streams
            .streamed_file_ids()
            .any(|file_id| !chunks.is_file_finished(&self.metadata.file_infos[file_id]))
    }

    // We might have the torrent "finished" i.e. no selected files. But if someone is streaming files despite
    // them being selected, we aren't fully "finished".
    fn is_finished_and_no_active_streams(&self) -> bool {
        self.is_finished()
            && !self.has_active_streams_unfinished_files(
                &self.lock_read("is_finished_and_dont_need_peers"),
            )
    }

    /// The connections the torrent holds, open or opening (libtorrent
    /// `torrent::num_peers`), and the most it may.
    pub(crate) fn connections(&self) -> (usize, usize) {
        let held = self
            .peer_limit
            .saturating_sub(self.peer_semaphore.available_permits());
        (held, self.peer_limit)
    }

    /// Closes PEER_TURNOVER percent of the torrent's connections, at least
    /// one, and no more than there are connect candidates to take their
    /// place, those it would keep least first (libtorrent
    /// `session_impl::second_tick`, `torrent::disconnect_peers`).
    pub(crate) fn turn_peers_over(&self) {
        let (held, _) = self.connections();
        let n = (held * PEER_TURNOVER / 100)
            .max(1)
            .min(self.peers.connect_candidates());
        self.peers.disconnect_lowest(n, tokio::time::Instant::now());
    }

    // The connection at `addr`, whose writer queue is `tx`, begins: it holds
    // `permit` under the torrent's cap and `slot` under the session's.
    fn hold_place(
        self: &Arc<Self>,
        addr: PeerHandle,
        tx: &PeerTx,
        permit: OwnedSemaphorePermit,
        slot: ConnectionSlot,
    ) -> Held {
        self.hold_place_in(&mut self.places.lock(), addr, tx, permit, slot)
    }

    fn hold_place_in(
        self: &Arc<Self>,
        places: &mut HashMap<PeerHandle, Place>,
        addr: PeerHandle,
        tx: &PeerTx,
        permit: OwnedSemaphorePermit,
        slot: ConnectionSlot,
    ) -> Held {
        let place = Place {
            tx: tx.clone(),
            since: tokio::time::Instant::now(),
            permit,
            slot,
        };
        places.insert(addr, place);
        Held {
            state: self.clone(),
            addr,
            tx: tx.clone(),
        }
    }

    // The connection at `addr` is closed and gives its place up to whoever
    // displaced it; it no longer counts (libtorrent
    // `peer_connection::disconnect` with `too_many_connections`, a close
    // that is no failure).
    fn give_up_place(places: &mut HashMap<PeerHandle, Place>, addr: PeerHandle) -> Option<Place> {
        let place = places.remove(&addr)?;
        let _ = place.tx.send(WriterRequest::Disconnect(Ok(())));
        Some(place)
    }

    // The connection that ranks lowest to keep, with its rank (libtorrent
    // `torrent::find_lowest_ranking_peer`). libtorrent ranks by BEP 40's
    // `peer_connection::peer_rank`; here the order the torrent turns peers
    // over by ranks them ([`peer::Peer::disconnect_rank`]).
    fn lowest_ranked(
        &self,
        places: &HashMap<PeerHandle, Place>,
        now: tokio::time::Instant,
    ) -> Option<(DisconnectRank, PeerHandle)> {
        places
            .keys()
            .filter_map(|&addr| {
                Some((
                    self.peers.with_peer(addr, |p| p.disconnect_rank(now))??,
                    addr,
                ))
            })
            .min()
    }

    // Whom a connection from `addr` displaces at the torrent's cap, if
    // anyone (libtorrent `torrent::attach_peer`). While more than one in ten
    // of the most connections the torrent holds are still connecting, the
    // connect begun longest ago gives way; otherwise the connection that
    // ranks lowest, when the newcomer ranks higher. The newcomer ranks as a
    // connection just opened, by what the peer list knows of its address.
    fn displaced_by(
        &self,
        places: &HashMap<PeerHandle, Place>,
        addr: PeerHandle,
    ) -> Option<PeerHandle> {
        let connecting = places
            .iter()
            .filter(|(addr, _)| {
                self.peers
                    .with_peer(**addr, |p| {
                        matches!(p.get_state(), PeerState::Connecting(_))
                    })
                    .unwrap_or(false)
            })
            .map(|(addr, place)| (place.since, *addr))
            .collect::<Vec<_>>();
        if connecting.len() > self.peer_limit / 10 {
            return connecting.into_iter().min().map(|(_, addr)| addr);
        }
        let newcomer = opening_rank(
            self.peers
                .with_peer(addr, |p| p.trust())
                .unwrap_or_default(),
        );
        let (lowest, displaced) = self.lowest_ranked(places, tokio::time::Instant::now())?;
        (lowest < newcomer).then_some(displaced)
    }

    /// Closes the connection that ranks lowest, and returns the place under
    /// the session's cap it gave up; see [`Self::lowest_ranked`].
    pub(crate) fn give_up_lowest(&self) -> Option<ConnectionSlot> {
        let mut places = self.places.lock();
        let (_, lowest) = self.lowest_ranked(&places, tokio::time::Instant::now())?;
        debug!(%lowest, "closed for a connection past the session's cap");
        Some(Self::give_up_place(&mut places, lowest)?.slot)
    }

    /// Whether the connection at `addr` holds a place past the session's
    /// cap, in its slack.
    pub(crate) fn exceeds_limit(&self, addr: PeerHandle, slots: &ConnectionSlots) -> bool {
        self.places
            .lock()
            .get(&addr)
            .is_some_and(|place| slots.exceeds_limit(&place.slot))
    }

    /// The connection at `addr` takes `slot` for its place under the
    /// session's cap; with none to take, it is closed.
    pub(crate) fn settle_slot(&self, addr: PeerHandle, slot: Option<ConnectionSlot>) {
        let mut places = self.places.lock();
        match slot {
            Some(slot) => {
                if let Some(place) = places.get_mut(&addr) {
                    place.slot = slot;
                }
            }
            None => {
                debug!(%addr, "no place under the session's cap, closing incoming connection");
                Self::give_up_place(&mut places, addr);
            }
        }
    }

    // Peer `peer` is known to have sent bad data of `piece`, which failed
    // its hash check: whether it may be banned for that (libtorrent
    // `peer_connection::received_invalid_data`). A web seed serves none of
    // what the piece lies in from then on and has its pieces told again,
    // and may be banned only when it is left nothing to serve
    // (`web_peer_connection::received_invalid_data`); any other peer may.
    fn received_invalid_data(&self, peer: PeerHandle, piece: ValidPieceIndex) -> bool {
        let Some(have) = self
            .shared
            .web_seeds
            .piece_failed(peer, piece, &self.web_layout)
        else {
            return true;
        };
        let nothing_left = !have.any();
        self.peers.update_bitfield(peer, have);
        nothing_left
    }

    /// `peers` sent bad data of `piece`: each loses points and goes on
    /// parole; one at the floor, or `known_bad`, the bad data known to be
    /// its, when its connection lets it go for that, is banned (libtorrent
    /// `torrent::penalize_peers`).
    fn penalize_peers(
        &self,
        peers: impl IntoIterator<Item = PeerHandle>,
        piece: ValidPieceIndex,
        known_bad: bool,
    ) {
        for peer in peers {
            let bannable = known_bad && self.received_invalid_data(peer, piece);
            let to_ban = self
                .peers
                .with_peer_mut(peer, "piece_failed", |p| p.piece_failed(bannable))
                .unwrap_or(bannable);
            if to_ban {
                self.peers.ban(peer.ip());
            }
        }
    }

    // A torrent wants peers, outgoing and incoming, unless it has nothing
    // left to fetch and its own upload is closed, so it has nothing to give.
    fn wants_peers(&self) -> bool {
        self.shared.options.ratelimits.get_upload_bps() != LimitKind::Closed
            || !self.is_finished_and_no_active_streams()
    }

    /// The piece did not pass its hash check, or the storage refused it: it
    /// is downloaded again.
    fn requeue_piece(&self, piece: ValidPieceIndex) -> anyhow::Result<()> {
        // Before it is queued again, so the storage has let go of the failed
        // bytes by the time another peer is handed the piece.
        if let Err(e) = self.files.on_piece_hash_failed(piece) {
            debug!(
                ?piece,
                "file storage errored in on_piece_hash_failed(): {e:#}"
            );
        }
        self.lock_write("mark_piece_broken")
            .get_pieces_mut()?
            .mark_piece_hash_failed(piece);
        self.new_pieces_notify.notify_waiters();
        Ok(())
    }

    /// The piece, which the storage took, is had: it is counted into its
    /// files (libtorrent `torrent::we_have`).
    fn on_piece_completed(&self, id: ValidPieceIndex) -> anyhow::Result<()> {
        let mut g = self.lock_write("on_piece_completed");
        let locked = &mut **g;
        let pieces = locked.get_pieces_mut()?;
        for (idx, file_info) in files_in_piece(&self.metadata.file_infos, id.get()) {
            let _remaining = pieces.update_file_have_on_piece_completed(id, idx, file_info);
        }

        self.streams
            .wake_streams_on_piece_completed(id, self.metadata.lengths());

        locked.unflushed_bitv_bytes += self.metadata.lengths().piece_length(id) as u64;
        if locked.unflushed_bitv_bytes >= FLUSH_BITV_EVERY_BYTES {
            locked.try_flush_bitv(&self.shared, true)
        }

        let chunks = locked.get_chunks()?;
        if chunks.is_finished() {
            if chunks.get_selected_pieces()[id.get_usize()] {
                locked.try_flush_bitv(&self.shared, false);
                info!(id=self.shared.id, info_hash=?self.shared.info_hash, "torrent finished downloading");
            }
            self.finished_notify.notify_waiters();

            if !self.has_active_streams_unfinished_files(locked) {
                // prevent deadlocks.
                drop(g);
                // Each peer's requester, woken by finished_notify, lets a
                // peer that only uploads go (PeerHandler::request_chunks).
                // A web seed only uploads too (libtorrent
                // `torrent::finished`).
                self.shared.web_seeds.disconnect_all();
                // The peers left are told we only upload now (libtorrent
                // `torrent::finished`).
                self.update_upload_only();
            }
        }
        Ok(())
    }

    pub(crate) fn reconnect_all_not_needed_peers(&self) {
        self.peers.requeue_not_needed();
    }

    /// The next PEX message to the peer at `to`, which our earlier ones
    /// told of the peers in `told`, and `told` brought up to it; None when
    /// nothing changed (libtorrent `ut_pex_plugin::tick`,
    /// `ut_pex_peer_plugin::send_ut_peer_diff`). It adds the peers we list
    /// that `to` was not told of, and drops those told of that we no longer
    /// list. A peer on a local network is not told to one that is not: it
    /// drops them (`ut_pex_peer_plugin::on_extended`).
    fn next_pex_message(
        &self,
        to: SocketAddr,
        told: &mut HashSet<SocketAddr>,
    ) -> Option<UtPex<ByteBufOwned>> {
        // BEP 11: at most 50 added and 50 dropped peers in one message.
        const MAX_SENT_PEERS: usize = 50;
        let listed = self.peers.pex_listed();
        let shown = |addr: &SocketAddr| is_local(to.ip()) || !is_local(addr.ip());
        let added: Vec<PexPeerInfo> = listed
            .iter()
            .filter(|(addr, _)| shown(addr) && !told.contains(*addr))
            .take(MAX_SENT_PEERS)
            .map(|(&addr, &flags)| PexPeerInfo { flags, addr })
            .collect();
        let dropped: Vec<SocketAddr> = told
            .iter()
            .filter(|addr| !listed.contains_key(addr))
            .take(MAX_SENT_PEERS)
            .copied()
            .collect();
        if added.is_empty() && dropped.is_empty() {
            return None;
        }
        for addr in &dropped {
            told.remove(addr);
        }
        told.extend(added.iter().map(|p| p.addr));
        Some(UtPex::from_peers(added.into_iter(), dropped.into_iter()))
    }

    async fn task_send_pex_to_peer(
        self: Arc<Self>,
        this_peer_addr: SocketAddr,
        tx: PeerTx,
    ) -> anyhow::Result<()> {
        // As per BEP 11 recommended interval is min 60 seconds
        const PEX_MESSAGE_INTERVAL: Duration = Duration::from_secs(60);

        let mut told = HashSet::new();

        // Wait 10 seconds before sending the first message to assure that peer will stay with us
        tokio::time::sleep(Duration::from_secs(10)).await;

        let mut interval = tokio::time::interval(PEX_MESSAGE_INTERVAL);

        loop {
            interval.tick().await;

            // This task should die with the cancellation token, but check defensively just in case.
            if tx.is_closed() {
                return Ok(());
            }

            if let Some(pex_msg) = self.next_pex_message(this_peer_addr, &mut told) {
                trace!(?pex_msg, "sending PEX");
                if tx.send(WriterRequest::UtPex(pex_msg)).is_err() {
                    return Ok(()); // Peer disconnected
                }
            }
        }
    }
}

// libtorrent `ut_pex_peer_plugin::on_extended`: the longest PEX message a
// peer may send (`pex_message_too_large`).
const MAX_PEX_MESSAGE_LEN: usize = 500 * 1024;

// libtorrent `settings_pack::max_out_request_queue`: the most requests
// outstanding to a peer, however fast it delivers.
const DEFAULT_PEER_REQUEST_WINDOW: usize = 500;

struct PeerFlowControl {
    // The most requests outstanding to the peer: the session's limit, or
    // the peer's `reqq` when that is lower (libtorrent
    // `peer_connection::m_max_out_request_queue`). How many the peer is
    // kept busy with below it follows its pace
    // (LivePeerState::desired_queue_size).
    request_window: usize,
}

impl PeerFlowControl {
    fn new(max_request_window: usize) -> Self {
        Self {
            request_window: max_request_window,
        }
    }
}

// All peer state that would never be used by other actors should pe put here.
// This state tracks a live peer.
struct PeerHandler {
    state: Arc<TorrentStateLive>,
    counters: Arc<AtomicPeerCounters>,
    // Semantically, we don't need a lock here, as this is only requested from
    // one future (requester + manage_peer).
    //
    // However as PeerConnectionHandler takes &self everywhere, we need shared mutability.
    // RefCell would do, but tokio is unhappy when we use it.
    flow_control: Mutex<PeerFlowControl>,

    // This is used to unpause chunk requester once the bitfield
    // is received.
    on_bitfield_notify: Notify,

    // The peer speaks v2 on this connection, and so exchanges hashes.
    speaks_v2: AtomicBool,

    // Our DHT port went to the peer on this connection (BEP 5 PORT).
    sent_dht_port: AtomicBool,

    addr: SocketAddr,
    incoming: bool,
    tx: PeerTx,

    first_message_received: AtomicBool,

    cancel_token: CancellationToken,

    client_name_and_version: String,
}

/// The files with bytes in piece `piece`, by index. Files are in torrent
/// order, so they are the run from the first that ends past the piece to
/// the last that starts in it. An empty file inside that run holds no piece
/// and adds nothing, but does not end it.
fn files_in_piece(file_infos: &[FileInfo], piece: u32) -> impl Iterator<Item = (usize, &FileInfo)> {
    file_infos
        .iter()
        .enumerate()
        .skip_while(move |(_, fi)| fi.piece_range.end <= piece)
        .take_while(move |(_, fi)| fi.piece_range.start <= piece)
}

impl PeerConnectionHandler for &'_ PeerHandler {
    fn on_connected(&self, connection_time: Duration) {
        self.counters
            .outgoing_connections
            .fetch_add(1, Ordering::Relaxed);
        #[allow(clippy::cast_possible_truncation)]
        self.counters
            .total_time_connecting_ms
            .fetch_add(connection_time.as_millis() as u64, Ordering::Relaxed);
    }

    fn encrypt_outgoing(&self) -> bool {
        self.state.peers.encrypt_outgoing(self.addr)
    }

    fn on_handshake_encryption(&self, encryption: crate::mse::Encryption) {
        self.state.peers.handshake_encryption(self.addr, encryption);
    }

    async fn on_received_message(&self, message: Message<'_>) -> anyhow::Result<()> {
        let first = !self.first_message_received.swap(true, Ordering::Relaxed);
        if first {
            // The connection works, so the failures before it no longer count.
            self.state.peers.connection_delivered(self.addr);
            // The peer's pieces are known from its first message. One that
            // is not a bitfield, Have All or Have None means the bitfield
            // was omitted, which is the same as Have None (libtorrent
            // `peer_connection::incoming_have`, `incoming_request`,
            // `incoming_piece`). It is taken before the message is, so a
            // first HAVE lands in pieces of the torrent's size.
            if !matches!(
                &message,
                Message::Bitfield(..) | Message::HaveAll | Message::HaveNone
            ) {
                self.on_have_all_or_none(0).context("on_have_none")?;
            }
        }

        match message {
            Message::Request(request) => {
                self.on_download_request(request)
                    .context("on_download_request")?;
            }
            Message::Bitfield(b) => self.on_bitfield(b.as_ref()).context("on_bitfield")?,
            Message::HaveAll => self.on_have_all_or_none(0xff).context("on_have_all")?,
            Message::HaveNone => self.on_have_all_or_none(0).context("on_have_none")?,
            Message::Choke => self.on_i_am_choked().context("on_i_am_choked")?,
            Message::Reject(request) => self.on_rejected(request).context("on_rejected")?,
            Message::Unchoke => self.on_i_am_unchoked(),
            Message::Interested => self.on_peer_interested(),
            Message::AllowedFast(index) => {
                self.on_allowed_fast(index).context("on_allowed_fast")?
            }
            Message::Suggest(index) => self.on_suggest(index).context("on_suggest")?,
            Message::Piece(piece) => self
                .on_received_piece(piece)
                .await
                .context("on_received_piece")?,
            Message::KeepAlive => {
                trace!("keepalive received");
            }
            Message::Have(h) => self.on_have(h).context("on_have")?,
            Message::Port(port) => self.on_dht_port(port),
            Message::Hashes(hashes) => self.on_hashes(hashes).context("on_hashes")?,
            Message::HashReject(request) => self.on_hash_reject(request),
            Message::HashRequest(request) => {
                self.on_hash_request(request).context("on_hash_request")?
            }
            Message::NotInterested => {
                if let Some(session) = self.state.shared.session.upgrade() {
                    session.choker.peer_not_interested(&self.state, self.addr);
                }
            }
            Message::Cancel(request) => self.on_upload_cancelled(request),
            Message::Extended(ExtendedMessage::UtMetadata(UtMetadata::Request(
                metadata_piece_id,
            ))) => {
                if self.state.metadata.info.info().private {
                    warn!(
                        id = self.state.shared.id,
                        info_hash = ?self.state.shared.info_hash,
                        "received noncompliant ut_metadata message from {}, ignoring",
                        self.addr
                    );
                } else {
                    self.send_metadata_piece(metadata_piece_id)
                        .with_context(|| {
                            format!("error sending metadata piece {metadata_piece_id}")
                        })?;
                }
            }
            Message::Extended(ExtendedMessage::UtHolepunch(msg)) => self.on_holepunch(msg),
            Message::Extended(ExtendedMessage::UploadOnly(upload_only)) => {
                self.on_upload_only(upload_only)
            }
            Message::Extended(ExtendedMessage::DontHave(index)) => self.on_dont_have(index),
            Message::Extended(ExtendedMessage::UtPex(pex)) => {
                if self.state.pex {
                    self.on_pex_message(pex)?;
                } else {
                    warn!(
                        id = self.state.shared.id,
                        info_hash = ?self.state.shared.info_hash,
                        "received a PEX message from {} we did not advertise, ignoring",
                        self.addr
                    );
                }
            }
            message => {
                warn!(
                    id = self.state.shared.id,
                    info_hash = ?self.state.shared.info_hash,
                    "received unsupported message {:?}, ignoring", message
                );
            }
        };
        // A seed that has sent us data is a complete copy seen now; one that
        // has not may only say it is a seed (libtorrent
        // `peer_connection::incoming_have` and `on_receive_data`).
        if self.counters.fetched_bytes.load(Ordering::Relaxed) > 0
            && self
                .state
                .peers
                .with_live(self.addr, |live| live.seeder)
                .unwrap_or(false)
        {
            self.state.seen_complete(tokio::time::Instant::now());
        }
        if first {
            // The peer's pieces are known now: its bitfield, or none.
            self.super_seed();
        }
        Ok(())
    }

    /// What we have, as libtorrent `bt_peer_connection::write_bitfield` tells
    /// it: on a fast connection Have All when we have every piece and Have
    /// None when we show none, a bitfield otherwise. With upload off, or
    /// while we super-seed, we show no piece.
    fn serialize_bitfield_message_to_buf(&self, buf: &mut [u8]) -> anyhow::Result<usize> {
        let fast = self.speaks_fast();
        let total_pieces = self.state.lengths.total_pieces() as usize;
        let upload = !self.state.torrent().options.disable_upload();
        let g = self.state.lock_read("serialize_bitfield_message_to_buf");
        let have = g.get_chunks()?.get_have_pieces();
        let pieces = &have.as_slice()[..total_pieces];
        let msg = if fast && (!upload || pieces.not_any() || self.state.super_seeding) {
            Message::HaveNone
        } else if fast && pieces.all() {
            Message::HaveAll
        } else {
            Message::Bitfield(ByteBuf(have.as_bytes()))
        };
        let len = msg.serialize(buf, &Default::default)?;
        trace!("sending: {:?}, length={}", &msg, len);
        Ok(len)
    }

    fn on_handshake(
        &self,
        handshake: Handshake,
        ckind: ConnectionKind,
        speaks_v2: bool,
    ) -> anyhow::Result<()> {
        self.speaks_v2.store(speaks_v2, Ordering::Relaxed);
        if handshake.supports_dht() {
            self.send_dht_port();
        }
        self.state.set_peer_live(self.addr, handshake, ckind);
        Ok(())
    }

    async fn request_bandwidth(&self, channel: Channel, bytes: u64) -> u64 {
        let mut grant = std::pin::pin!(self.state.grant_bandwidth(channel, bytes));
        if let Some(granted) = (&mut grant).now_or_never() {
            return granted;
        }
        // While its reads wait on the limits, the peer is not held to its
        // request and piece timeouts (libtorrent `peer_connection::second_tick`
        // snubs and times out only a peer it reads, `bw_network`).
        let gate = |waiting: bool| {
            self.state
                .peers
                .with_live_mut(self.addr, "download quota", |live| {
                    live.set_waiting_for_download_quota(waiting)
                })
        };
        if channel == Channel::Download {
            gate(true);
        }
        let granted = grant.await;
        if channel == Channel::Download {
            gate(false);
        }
        granted
    }

    fn return_bandwidth(&self, channel: Channel, bytes: u64) {
        self.state.give_back_bandwidth(channel, bytes);
    }

    fn on_protocol_bytes(&self, channel: Channel, bytes: u64) {
        let (peer, torrent, session) = match channel {
            Channel::Download => (
                &self.counters.fetched_protocol_bytes,
                &self.state.stats.fetched_protocol_bytes,
                &self.state.session_stats.counters.fetched_protocol_bytes,
            ),
            Channel::Upload => (
                &self.counters.uploaded_protocol_bytes,
                &self.state.stats.uploaded_protocol_bytes,
                &self.state.session_stats.counters.uploaded_protocol_bytes,
            ),
        };
        for counter in [peer, torrent, session] {
            counter.fetch_add(bytes, Ordering::Relaxed);
        }
    }

    fn on_uploaded_bytes(&self, bytes: u32) {
        self.counters
            .uploaded_bytes
            .fetch_add(bytes as u64, Ordering::Relaxed);
        self.state
            .stats
            .uploaded_bytes
            .fetch_add(bytes as u64, Ordering::Relaxed);
        self.state
            .session_stats
            .counters
            .uploaded_bytes
            .fetch_add(bytes as u64, Ordering::Relaxed);
    }

    fn claim_upload(&self, chunk: &ChunkInfo) -> bool {
        let (claimed, ready) = self
            .state
            .peers
            .with_live_mut(self.addr, "claim_upload", |live| {
                (live.claim_upload(chunk), live.ready_to_hand())
            })
            .unwrap_or_default();
        // The writer has room again: the peer's next block waits its turn.
        if ready {
            self.state.upload_turn(self.addr);
        }
        claimed
    }

    fn read_chunk(&self, chunk: &ChunkInfo, buf: &mut [u8]) -> anyhow::Result<()> {
        self.state.file_ops().read_chunk(self.addr, chunk, buf)
    }

    fn on_extended_handshake(&self, hs: &ExtendedHandshake<ByteBuf>) -> anyhow::Result<()> {
        if let Some(client_name) = hs.v.as_ref().and_then(format_peer_client_name) {
            self.state
                .peers
                .with_live_mut(self.addr, "update peer client name", |live| {
                    live.client_name = Some(client_name);
                });
        }
        self.state
            .peers
            .with_live_mut(self.addr, "update peer holepunch support", |live| {
                live.supports_holepunch = hs.m.ut_holepunch.is_some();
                live.supports_upload_only = hs.m.upload_only.is_some();
            });
        // A nonzero `upload_only` says the peer only uploads; zero or none
        // says nothing (libtorrent `bt_peer_connection::on_extended_handshake`).
        if hs.upload_only.is_some_and(|upload_only| upload_only != 0) {
            self.on_upload_only(true);
        }
        // The peer saw a complete copy `complete_ago` seconds ago; a
        // negative one says it never did (libtorrent
        // `bt_peer_connection::on_extended_handshake`, into
        // `peer_connection::set_last_seen_complete`). Its word is the
        // swarm's status, never our own sighting.
        if let Some(ago) = hs.complete_ago.and_then(|ago| u64::try_from(ago).ok())
            && let Some(at) = tokio::time::Instant::now().checked_sub(Duration::from_secs(ago))
        {
            self.state
                .peers
                .with_live_mut(self.addr, "complete_ago", |live| {
                    live.last_seen_complete = Some(at)
                });
        }

        // The peer holds at most its `reqq` requests (libtorrent
        // `bt_peer_connection::on_extended_handshake`); our own limit stays
        // the ceiling.
        if let Some(reqq) = hs.reqq.and_then(|reqq| usize::try_from(reqq).ok())
            && reqq > 0
        {
            let request_window = reqq.min(self.state.max_request_window());
            let mut flow = self.lock_flow_control("update request window");
            if flow.request_window != request_window {
                debug!(
                    reqq,
                    request_window, "updated peer request window from extended handshake"
                );
                flow.request_window = request_window;
                self.notify_request_slots_changed();
            }
        }

        if self.state.pex && hs.m.ut_pex.is_some() {
            spawn_with_cancel(
                debug_span!(
                    parent: self.state.shared.span.clone(),
                    "sending_pex_to_peer",
                    peer = ?self.addr,
                ),
                format!(
                    "[{}][addr={}]sending_pex_to_peer",
                    self.state.shared.id, self.addr
                ),
                self.cancel_token.clone(),
                self.state
                    .clone()
                    .task_send_pex_to_peer(self.addr, self.tx.clone()),
            );
        }
        // `yourip`, our address as the peer sees it, is the peer's vote on
        // our external address (libtorrent
        // `bt_peer_connection::on_extended_handshake`, into
        // `session_impl::set_external_address`).
        if let Some(yourip) = hs.yourip
            && let Some(session) = self.state.shared.session.upgrade()
        {
            session.external_ips.cast_vote(
                yourip.0,
                crate::session::external_ip::Voter::Peer(self.addr.ip()),
                tokio::time::Instant::now(),
            );
        }
        // A peer that connected to us says where it listens: at the address
        // it connected from, on port `p`. Its own word on its address
        // (`ipv4`, `ipv6`) is never taken (libtorrent
        // `bt_peer_connection::on_extended_handshake`, which reads only `p`,
        // into `peer_list::update_peer_port`).
        if self.incoming
            && let Some(port) = hs.port().filter(|&port| port != 0)
        {
            let listen = SocketAddr::new(self.addr.ip(), port);
            self.state
                .peers
                .with_peer_mut(self.addr, "update outgoing addr", |peer| {
                    peer.outgoing_address = Some(listen)
                });
        }

        Ok(())
    }

    /// A fast connection always opens with what we have (BEP 6); a plain one
    /// only when we show a piece (libtorrent
    /// `bt_peer_connection::write_bitfield`).
    fn should_send_bitfield(&self) -> bool {
        if self.speaks_fast() {
            return true;
        }
        if self.state.torrent().options.disable_upload() {
            return false;
        }

        // A super-seed tells of no piece but those it hands out
        // (libtorrent `bt_peer_connection::write_bitfield`).
        !self.state.super_seeding && self.state.get_approx_have_bytes() > 0
    }

    fn update_my_extended_handshake(
        &self,
        handshake: &mut ExtendedHandshake<ByteBuf>,
    ) -> anyhow::Result<()> {
        let info_bytes = &self.state.metadata.info_bytes;
        if !info_bytes.is_empty()
            && let Ok(len) = info_bytes.len().try_into()
        {
            handshake.metadata_size = Some(len);
        }
        handshake.m.ut_pex = handshake.m.ut_pex.filter(|_| self.state.pex);
        // The most blocks the peer may have asked us for and not been sent
        // (libtorrent `bt_peer_connection::write_extensions`).
        handshake.reqq = Some(MAX_ALLOWED_IN_REQUEST_QUEUE);
        // Every connection speaks it; we act on `connect` only over uTP
        // (libtorrent `bt_peer_connection::write_extensions`).
        handshake.m.ut_holepunch = Some(MY_EXTENDED_UT_HOLEPUNCH);
        // We only upload (libtorrent `bt_peer_connection::write_extensions`).
        handshake.upload_only = self.state.is_upload_only().then_some(1);
        // Seconds since we last saw a complete copy, -1 for never
        // (libtorrent `bt_peer_connection::write_extensions`).
        handshake.complete_ago = Some(
            self.state
                .complete_ago()
                .map_or(-1, |ago| i32::try_from(ago.as_secs()).unwrap_or(i32::MAX)),
        );
        // Where we listen, on a connection we made: a peer that reached us
        // knows it (libtorrent `bt_peer_connection::write_extensions`, the
        // port `session_impl::listen_port` gives: the one mapped on the
        // gateway, else the one we listen on).
        if !self.incoming {
            handshake.p = self
                .state
                .shared
                .session
                .upgrade()
                .and_then(|session| session.announce_port())
                .map(u32::from);
        }

        Ok(())
    }

    fn client_name_and_version(&self) -> &str {
        &self.client_name_and_version
    }

    fn num_pieces(&self) -> Option<u32> {
        Some(self.state.lengths.total_pieces())
    }

    fn dht_port(&self) -> Option<u16> {
        self.dht().map(|dht| dht.listen_addr().port())
    }
}

impl PeerHandler {
    fn dht(&self) -> Option<crate::dht::Dht> {
        self.state.shared.session.upgrade()?.get_dht().cloned()
    }

    /// Sends our DHT port, once per connection (libtorrent
    /// `bt_peer_connection::write_dht_port`). The writer sends the bitfield
    /// before anything queued, so a port queued at the handshake follows it.
    fn send_dht_port(&self) {
        let Some(port) = self.dht().map(|dht| dht.listen_addr().port()) else {
            return;
        };
        if !self.sent_dht_port.swap(true, Ordering::Relaxed) {
            let _ = self.tx.send(WriterRequest::Message(Message::Port(port)));
        }
    }

    /// A peer named its DHT node: the DHT pings it and adds it on reply
    /// (libtorrent `peer_connection::incoming_dht_port`), once for each
    /// port the connection names, so a repeated PORT is no new ping. A peer
    /// that sends PORT runs a DHT node even without the handshake bit, so
    /// it gets ours (`bt_peer_connection::on_dht_port`).
    fn on_dht_port(&self, port: u16) {
        let news = self
            .state
            .peers
            .with_live_mut(self.addr, "dht_port", |live| live.dht_node_named(port))
            .unwrap_or(false);
        if news && let Some(dht) = self.dht() {
            dht.add_node(SocketAddr::new(self.addr.ip(), port));
        }
        self.send_dht_port();
    }

    /// A holepunch message (BEP 55), from a peer that advertised the
    /// extension; from one that did not it is ignored (libtorrent
    /// `bt_peer_connection::on_holepunch`).
    fn on_holepunch(&self, msg: UtHolepunch) {
        let supported = self
            .state
            .peers
            .with_live(self.addr, |live| live.supports_holepunch)
            .unwrap_or(false);
        if !supported {
            debug!(
                ?msg,
                "holepunch message from a peer that did not advertise it, ignoring"
            );
            return;
        }
        match msg {
            UtHolepunch::Rendezvous(target) => self.on_rendezvous(target),
            // A local address is taken only from a local peer.
            UtHolepunch::Connect(addr) => {
                if !is_local(addr.ip()) || is_local(self.addr.ip()) {
                    self.state.holepunch(addr);
                }
            }
            UtHolepunch::Error(addr, code) => {
                debug!(?addr, code, "the peer could not introduce us");
            }
        }
    }

    /// The peer asks us to introduce it to the peer at `target`: when we are
    /// connected to that peer and it speaks the extension, each is told to
    /// connect to the other; else the peer is told why not. A local peer and
    /// one that is not are never introduced (libtorrent
    /// `bt_peer_connection::on_holepunch`, `rendezvous`).
    fn on_rendezvous(&self, target: SocketAddr) {
        let send = |tx: &PeerTx, msg: UtHolepunch| {
            let _ = tx.send(WriterRequest::Message(Message::Extended(
                ExtendedMessage::UtHolepunch(msg),
            )));
        };
        let found = self
            .state
            .peers
            .with_live(target, |live| (live.supports_holepunch, live.tx.clone()));
        let refused = match &found {
            None => Some(ut_holepunch::error::NOT_CONNECTED),
            Some((false, _)) => Some(ut_holepunch::error::NO_SUPPORT),
            Some(_) if target == self.addr => Some(ut_holepunch::error::NO_SELF),
            Some(_) => None,
        };
        if let Some(code) = refused {
            send(&self.tx, UtHolepunch::Error(target, code));
            return;
        }
        if is_local(target.ip()) != is_local(self.addr.ip()) {
            debug!(?target, "not introducing a local peer to one that is not");
            return;
        }
        if let Some((_, target_tx)) = found {
            send(&self.tx, UtHolepunch::Connect(target));
            send(&target_tx, UtHolepunch::Connect(self.addr));
        }
    }
    fn on_peer_died(self, error: Option<crate::Error>) -> crate::Result<()> {
        let peers = &self.state.peers;
        let handle = self.addr;
        let mut pe = match peers.states.get_mut(&handle) {
            Some(peer) => TimedExistence::new(peer, "on_peer_died"),
            None => {
                warn!(
                    id = self.state.shared.id,
                    info_hash = ?self.state.shared.info_hash,
                    addr=?handle,
                    "bug: peer not found in table. Forgetting it forever"
                );
                return Ok(());
            }
        };
        // A connection we asked to close did not fail (libtorrent
        // `peer_connection::failed`).
        let failed = error
            .as_ref()
            .is_some_and(|e| !matches!(e, crate::Error::Disconnect));
        // A connection to a peer that speaks the holepunch extension failed,
        // and no holepunch asked for it: a peer that can introduce us is
        // asked to (libtorrent `peer_connection::connect_failed`).
        let rendezvous = matches!(pe.value().get_state(), PeerState::Connecting(_))
            && failed
            && pe.value().supports_holepunch
            && !pe.value().holepunched();
        pe.value_mut().connection_closing();
        let prev = pe.value_mut().take_state(peers);
        self.state.layer_fetch.forget(handle);

        // Whether the connection held an upload slot, when it was live. The
        // choker hears of it once the peer table is let go: it takes its own
        // lock before the table's.
        let mut held_slot = None;
        match prev {
            PeerState::Connecting(_) => {}
            PeerState::Live(live) => {
                held_slot = Some(!live.is_choked());
                let mut g = self.state.lock_write("mark_chunk_requests_canceled");

                // Release all pieces owned by this peer (fixes the bug where pieces
                // could be in both queue_pieces AND inflight_pieces after peer death)
                let released = g.get_pieces_mut()?.release_pieces_owned_by(self.addr);
                if released > 0 {
                    trace!(
                        "peer dead, released {} in-flight pieces back to queue",
                        released
                    );
                }

                // Also handle any active chunk-level inflight requests.
                let mut had_inflight = false;
                for req in live.inflight_requests() {
                    had_inflight = true;
                    trace!(
                        "peer dead, marking chunk request cancelled, index={}, chunk={}",
                        req.piece_index.get(),
                        req.chunk_index
                    );
                }

                if released > 0 || had_inflight {
                    self.state.new_pieces_notify.notify_waiters();
                }
            }
            s @ (PeerState::Queued | PeerState::Dead | PeerState::NotNeeded) => {
                warn!(
                    id = self.state.shared.id,
                    info_hash = ?self.state.shared.info_hash,
                    addr = ?handle,
                    "bug: peer was in a wrong state {s:?}, ignoring it forever"
                );
                // Prevent deadlocks.
                drop(pe);
                self.state.peers.drop_peer(handle);
                return Ok(());
            }
        };

        // A connection that ended in an error counts against the peer; one we
        // closed ourselves does not. Either way the peer stays a connect
        // candidate unless it only uploads and so do we (libtorrent
        // `peer_list::connection_closed`, `is_connect_candidate`).
        if failed {
            self.counters.errors.fetch_add(1, Ordering::Relaxed);
        }
        let needed = pe
            .value()
            .needed(self.state.is_finished_and_no_active_streams());
        peers.connection_closed(pe.value_mut(), tokio::time::Instant::now(), failed, needed);
        trace!(state = %pe.value().get_state(), "peer settled after its connection closed");
        drop(pe);
        if rendezvous && let Some(introducer) = peers.introducer(handle) {
            let _ = introducer.send(WriterRequest::Message(Message::Extended(
                ExtendedMessage::UtHolepunch(UtHolepunch::Rendezvous(handle)),
            )));
        }
        if let Some(held_slot) = held_slot
            && let Some(session) = self.state.shared.session.upgrade()
        {
            session
                .choker
                .peer_gone(self.state.shared.id, handle, held_slot);
        }
        Ok(())
    }

    /// Acquire a piece for this peer: try steal (10x) → reserve → steal (3x).
    ///
    /// Returns the piece index to download, or None if no pieces are available.
    fn acquire_next_piece(&self) -> crate::Result<Option<ValidPieceIndex>> {
        // While the peer chokes us we may fetch only the pieces it allows
        // (libtorrent `request_a_block` masks the peer's bitfield with them).
        let choked = self.choked_by_peer();

        // Steal info to process after releasing the peer lock
        let mut steal_info: Option<(SocketAddr, ValidPieceIndex)> = None;

        // A peer on parole downloads whole pieces alone until one passes
        // (libtorrent `peer_connection::picker_options`).
        let on_parole = self
            .state
            .peers
            .with_peer(self.addr, |peer| peer.trust().on_parole())
            .unwrap_or_default();

        let result = self
            .state
            .peers
            .with_live_mut(self.addr, "acquire_next_piece", |live| {
                let mut g = self.state.lock_write("acquire_next_piece");

                // Nothing is asked in upload mode (libtorrent
                // `peer_connection::send_block_requests`, `add_request`).
                // Read under the peer's lock, which set_upload_mode takes to
                // give back the peer's pieces: a piece is picked before and
                // given back there, or not picked.
                if self.state.upload_mode() {
                    return Ok(None);
                }
                let pieces = g.pieces.as_mut().ok_or(Error::ChunkTrackerEmpty)?;
                // A piece we have is no longer the peer's to offer (libtorrent
                // `peer_connection::received_piece`, `announce_piece`).
                live.allowed_fast_from_peer
                    .retain(|p| !pieces.chunks().is_piece_have(*p));
                live.suggested
                    .retain(|p| !pieces.chunks().is_piece_have(*p));
                let bf = &live.bitfield;
                let allowed_fast = &live.allowed_fast_from_peer;
                // A piece the storage has no room for waits on new_pieces_notify,
                // which the storage rings when it has room again.
                let result = pieces.acquire_admitted_piece(
                    AcquireRequest {
                        peer: self.addr,
                        peer_avg_time: if live.is_snubbed() {
                            None
                        } else {
                            self.counters.average_piece_download_time()
                        },
                        priority_pieces: self.state.streams.iter_next_pieces(&self.state.lengths),
                        // Only a piece that can be checked once it arrives.
                        peer_has_piece: |p| {
                            bf.get(p.get() as usize).map(|v| *v) == Some(true)
                                && self.state.metadata.can_verify(p)
                                && (!choked || allowed_fast.contains(&p))
                        },
                        availability: |p| self.state.peers.availability.get(p),
                        moved: self.state.peers.availability.take_moved(),
                        num_peers: self.state.peers.stats().live as usize,
                        outstanding_requests: live.requested_inflight_count(),
                        options: PickOptions {
                            reverse: live.is_snubbed(),
                            on_parole,
                            suggested: &live.suggested,
                        },
                        can_steal: |p| {
                            self.state.per_piece_locks[p.get_usize()]
                                .try_write()
                                .is_some()
                        },
                    },
                    |p| {
                        self.state
                            .files
                            .reserve_piece(p, &self.state.new_pieces_notify)
                    },
                );

                match result {
                    AcquireResult::Reserved(piece) => {
                        trace!("reserved piece {}", piece);
                        Ok(Some(piece))
                    }
                    AcquireResult::Stolen { piece, from_peer } => {
                        debug!("stole piece {} from {}", piece, from_peer);
                        // Store steal info to process after releasing peer lock to avoid deadlock
                        steal_info = Some((from_peer, piece));
                        Ok(Some(piece))
                    }
                    AcquireResult::EndGame { piece, chunk } => {
                        debug!("end game: asking for {chunk:?}, asked of another peer too");
                        Ok(Some(piece))
                    }
                    AcquireResult::NoneAvailable => Ok(None),
                }
            })
            .transpose()
            .map(|r| r.flatten());

        // Process steal notification outside the peer lock to avoid deadlock
        if let Some((from_peer, piece)) = steal_info {
            self.state.peers.on_steal(from_peer, self.addr, piece);
        }

        result
    }

    /// The block of the torrent `request` names, if it names one a peer may
    /// ask for.
    fn requested_chunk(&self, request: &Request) -> Option<ChunkInfo> {
        let piece_index = self.state.lengths.validate_piece_index(request.index)?;
        self.state
            .lengths
            .requested_block(piece_index, request.begin, request.length)
    }

    fn on_download_request(&self, request: Request) -> anyhow::Result<()> {
        if self.state.torrent().options.disable_upload() {
            anyhow::bail!("upload disabled, but peer requested a piece")
        }

        // A request for a block that is not one, or of a piece we do not
        // have, is dropped (libtorrent `peer_connection::incoming_request`).
        // While we super-seed, so is one for a piece other than the one
        // the peer was handed (libtorrent `peer_connection::incoming_request`).
        let chunk_info = match self.requested_chunk(&request) {
            Some(chunk)
                if !self.state.super_seeding
                    || self.state.peers.super_seeded(self.addr, chunk.piece_index) =>
            {
                self.state
                    .lock_read("is_chunk_ready_to_upload")
                    .get_chunks()?
                    .is_chunk_ready_to_upload(&chunk)
                    .then_some(chunk)
            }
            _ => None,
        };
        let Some(chunk_info) = chunk_info else {
            return self.on_invalid_request(request);
        };

        // A peer that asks without having said it is interested is taken to
        // have said so.
        let interested = self
            .state
            .peers
            .with_live(self.addr, |live| live.peer_interested);
        if interested == Some(false) {
            self.tell_choker_interested();
        }

        let chunks_per_piece = self.state.lengths.default_chunks_per_piece();
        let Some(queued) = self
            .state
            .peers
            .with_live_mut(self.addr, "queue_upload", |live| {
                live.queue_upload(chunk_info, chunks_per_piece)
            })
        else {
            return Ok(());
        };
        match queued {
            Ok(ready) => {
                if ready {
                    self.state.upload_turn(self.addr);
                }
                Ok(())
            }
            Err(Refused::Choked(choked_at)) => {
                let since = tokio::time::Instant::now().saturating_duration_since(choked_at);
                if since > CHOKED_REQUEST_GRACE {
                    anyhow::bail!("received {request:?} {since:?} after we choked the peer");
                }
                trace!(?request, "ignoring a request from a peer we choke");
                Ok(())
            }
            Err(Refused::AllowedFastSpent) => anyhow::bail!(
                "received {request:?} while choked, past three times the blocks of the piece"
            ),
            Err(Refused::QueueFull) => {
                trace!(?request, "ignoring a request past the peer's request queue");
                Ok(())
            }
            Err(Refused::Queued) => Ok(()),
        }
    }

    /// The peer asked for what we cannot serve. The request is dropped: with
    /// no fast extension there is no Reject to send.
    fn on_invalid_request(&self, request: Request) -> anyhow::Result<()> {
        let peer_chokes_us = self.choked_by_peer();
        let keep = self
            .state
            .peers
            .with_live_mut(self.addr, "invalid_request", |live| {
                live.invalid_request(peer_chokes_us)
            });
        if keep == Some(false) {
            anyhow::bail!("too many invalid requests while choked, the last {request:?}");
        }
        trace!(?request, "dropping a request we cannot serve");
        Ok(())
    }

    /// The peer no longer wants the block `request` names: it is not sent
    /// unless it already was.
    fn on_upload_cancelled(&self, request: Request) {
        let Some(chunk) = self.requested_chunk(&request) else {
            trace!(?request, "cancel names no block of the torrent");
            return;
        };
        self.state
            .peers
            .with_live_mut(self.addr, "cancel_upload", |live| {
                live.cancel_upload(&chunk)
            });
    }

    /// The peer says whether it only uploads (libtorrent
    /// `peer_connection::set_upload_only`). Whether the connection is still
    /// of use is decided where interest is: the requester looks again.
    fn on_upload_only(&self, upload_only: bool) {
        self.state
            .peers
            .with_live_mut(self.addr, "upload_only", |live| {
                live.set_upload_only(upload_only)
            });
        self.on_bitfield_notify.notify_waiters();
    }

    /// The peer said it is interested. A fast peer is first sent the pieces it
    /// may fetch while we choke it, unless we upload nothing.
    fn on_peer_interested(&self) {
        if !self.state.torrent().options.disable_upload() {
            let info_hash = self
                .state
                .shared
                .info_hashes
                .for_peer(self.speaks_v2.load(Ordering::Relaxed));
            self.state
                .peers
                .with_live_mut(self.addr, "send_allowed_fast", |live| {
                    live.send_allowed_fast(
                        self.addr.ip(),
                        info_hash,
                        &self.state.lengths,
                        self.state.super_seeding,
                    )
                });
        }
        self.tell_choker_interested();
    }

    // The session's choker hears that the peer is interested.
    fn tell_choker_interested(&self) {
        if let Some(session) = self.state.shared.session.upgrade() {
            session.peer_interested(&self.state, self.addr);
        }
    }

    /// The peer lets us fetch `index` while it chokes us (libtorrent
    /// `peer_connection::incoming_allowed_fast`). An index past the last
    /// piece, or a piece we have, is ignored.
    fn on_allowed_fast(&self, index: u32) -> anyhow::Result<()> {
        let Some(piece) = self.state.lengths.validate_piece_index(index) else {
            trace!(index, "allowed fast names no piece of the torrent");
            return Ok(());
        };
        if self
            .state
            .lock_read("on_allowed_fast")
            .get_chunks()?
            .is_piece_have(piece)
        {
            return Ok(());
        }
        self.state
            .peers
            .with_live_mut(self.addr, "on_allowed_fast", |live| {
                if !live.allowed_fast_from_peer.contains(&piece) {
                    live.allowed_fast_from_peer.push(piece);
                }
            });
        self.notify_request_slots_changed();
        Ok(())
    }

    /// The peer suggests we fetch `index` (libtorrent
    /// `peer_connection::incoming_suggest`). An index past the last piece,
    /// or a piece we have, is ignored.
    fn on_suggest(&self, index: u32) -> anyhow::Result<()> {
        let Some(piece) = self.state.lengths.validate_piece_index(index) else {
            trace!(index, "suggest names no piece of the torrent");
            return Ok(());
        };
        if self
            .state
            .lock_read("on_suggest")
            .get_chunks()?
            .is_piece_have(piece)
        {
            return Ok(());
        }
        self.state
            .peers
            .with_live_mut(self.addr, "on_suggest", |live| live.suggest(piece));
        Ok(())
    }

    /// A HAVE names a piece the peer now has; one at or past the piece count
    /// ends the connection.
    fn on_have(&self, have: u32) -> anyhow::Result<()> {
        let Some(piece) = self.state.lengths.validate_piece_index(have) else {
            anyhow::bail!(
                "have {have} is past the last of {} pieces",
                self.state.lengths.total_pieces()
            );
        };
        trace!(have, "peer has piece");
        self.state.peers.peer_has(self.addr, piece);
        if self.state.super_seeding {
            self.state
                .peers
                .super_seed_have(self.addr, piece, &self.state.lengths);
        }
        self.on_bitfield_notify.notify_waiters();
        Ok(())
    }

    /// A DONT_HAVE names a piece the peer no longer has (libtorrent
    /// `peer_connection::incoming_dont_have`); one past the last piece is
    /// ignored.
    fn on_dont_have(&self, index: u32) {
        let Some(piece) = self.state.lengths.validate_piece_index(index) else {
            trace!(index, "dont have names no piece of the torrent");
            return;
        };
        trace!(index, "peer no longer has piece");
        self.state.peers.peer_lost(self.addr, piece);
        self.on_bitfield_notify.notify_waiters();
    }

    fn on_bitfield(&self, bitfield: &[u8]) -> anyhow::Result<()> {
        let bf = ingest_bitfield(bitfield, self.state.lengths.total_pieces())?;
        self.state.peers.update_bitfield(self.addr, bf);
        self.on_bitfield_notify.notify_waiters();
        Ok(())
    }

    /// Have All and Have None stand for a bitfield of every bit set or none
    /// (libtorrent `peer_connection::incoming_have_all`,
    /// `incoming_have_none`), and replace any the peer sent before.
    fn on_have_all_or_none(&self, byte: u8) -> anyhow::Result<()> {
        let total_pieces = self.state.lengths.total_pieces();
        let len = empty_bitfield(total_pieces).as_raw_slice().len();
        self.on_bitfield(&vec![byte; len])
    }

    /// Whether the connection speaks the fast extension.
    fn speaks_fast(&self) -> bool {
        self.state
            .peers
            .with_live(self.addr, |live| live.fast)
            .unwrap_or(false)
    }

    /// The peer's pieces are known, from its first message: while we
    /// super-seed, it is handed two.
    fn super_seed(&self) {
        if self.state.super_seeding {
            self.state.peers.super_seed(self.addr, &self.state.lengths);
        }
    }

    async fn wait_for_any_notify(&self, notify: &Notify, check: impl Fn() -> bool) {
        loop {
            // To remove possibility of races, we first grab a token, then check
            // if we need it, and only if so, await.
            let notified = notify.notified();
            if check() {
                return;
            }
            notified.await;
        }
    }

    /// Resolves once the peer's pieces are known: its first message was
    /// read, and its bitfield sized to the torrent.
    async fn wait_for_bitfield(&self) {
        self.wait_for_any_notify(&self.on_bitfield_notify, || {
            self.state
                .peers
                .with_live(self.addr, |live| !live.bitfield.is_empty())
                .unwrap_or_default()
        })
        .await;
    }

    /// Resolves once what we want of the peer may have changed: pieces
    /// were queued again or the wanted ones changed (new_pieces_notify), the
    /// torrent finished, a stream started or ended, or the peer's pieces
    /// changed. Each wait is
    /// taken when this is called, not when it is first polled.
    fn wants_changed(&self) -> impl Future<Output = ()> + '_ {
        let pieces = self.state.new_pieces_notify.notified();
        let finished = self.state.finished_notify.notified();
        let streams = self.state.streams.changed.notified();
        let peer_pieces = self.on_bitfield_notify.notified();
        async move {
            tokio::select! {
                () = pieces => {}
                () = finished => {}
                () = streams => {}
                () = peer_pieces => {}
            }
        }
    }

    async fn wait_for_request_slot(&self) {
        loop {
            let Some(notify) = self.request_slots_changed() else {
                return;
            };
            let notified = notify.notified();
            if self.can_send_request() {
                return;
            }
            notified.await;
        }
    }

    // The job of this is to request chunks, time out the requests the peer
    // does not answer, fetch the piece layers a v2 torrent lacks, and also
    // to keep peer alive.
    // The moment this ends, the peer is disconnected.
    async fn task_peer_chunk_requester(&self) -> crate::Result<()> {
        tokio::select! {
            r = self.request_chunks() => r,
            r = self.time_out_requests() => r,
            r = self.fetch_layers() => r,
            r = self.time_out_idle() => r,
        }
    }

    /// Returns, ending the connection with no fault of the peer's, once it
    /// is idle long enough (LivePeerState::timed_out), looked at each second
    /// from the peer's first message on (libtorrent
    /// `peer_connection::second_tick`). Connections run short within five of
    /// the torrent's limit or the session's.
    async fn time_out_idle(&self) -> crate::Result<()> {
        self.wait_for_bitfield().await;
        let mut tick = tokio::time::interval(Duration::from_secs(1));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tick.tick().await;
            let upload_only = self.state.is_finished_and_no_active_streams();
            let full = self.state.peer_semaphore.available_permits() <= 5
                || self
                    .state
                    .shared
                    .session
                    .upgrade()
                    .is_some_and(|session| session.connection_slots.free() <= 5);
            let now = tokio::time::Instant::now();
            if self.state.super_seeding {
                self.state
                    .peers
                    .super_seed_tick(self.addr, now, &self.state.lengths);
            }
            let Some(timed_out) = self
                .state
                .peers
                .with_live(self.addr, |live| live.timed_out(now, upload_only, full))
            else {
                return Ok(());
            };
            if let Some(why) = timed_out {
                debug!(?why, "idle, disconnecting peer");
                return Ok(());
            }
        }
    }

    /// Snubs this peer when it goes its request timeout
    /// (LivePeerState::request_deadline) without delivering a block, and
    /// again every [`SNUB_INTERVAL`] while its requests stay overdue.
    async fn time_out_requests(&self) -> crate::Result<()> {
        self.wait_for_bitfield().await;
        let mut next_snub = None;
        loop {
            let Some(notify) = self.request_slots_changed() else {
                return Ok(());
            };
            let changed = notify.notified();
            let Some(deadline) = self
                .state
                .peers
                .with_live(self.addr, |live| live.request_deadline())
            else {
                return Ok(());
            };
            let Some(deadline) = deadline else {
                changed.await;
                continue;
            };
            let deadline = next_snub.map_or(deadline, |at: tokio::time::Instant| deadline.max(at));
            tokio::select! {
                // A block that arrived moved the deadline.
                biased;
                _ = changed => {}
                _ = tokio::time::sleep_until(deadline) => {
                    self.on_requests_timed_out()?;
                    next_snub = Some(tokio::time::Instant::now() + SNUB_INTERVAL);
                }
            }
        }
    }

    /// The peer is snubbed, and the newest piece it owns that waits on it
    /// alone goes back to the queue for other peers to fetch: one a snub, so
    /// a peer that stays overdue gives its pieces back one at a time
    /// (libtorrent `peer_connection::snub_peer` times out one block each
    /// `second_tick`). Its requests stay outstanding, so a block it still
    /// sends is kept. A peer on parole is left its pieces: it was handed
    /// only pieces no one else had started (`snub_peer` returns early on
    /// parole).
    fn on_requests_timed_out(&self) -> crate::Result<()> {
        let on_parole = self
            .state
            .peers
            .with_peer(self.addr, |peer| peer.trust().on_parole())
            .unwrap_or(false);
        let released = self
            .state
            .peers
            .with_live_mut(self.addr, "snub", |live| {
                live.snub();
                if on_parole {
                    return Ok(None);
                }
                Ok::<_, Error>(
                    self.state
                        .lock_write("snub")
                        .get_pieces_mut()?
                        .release_piece_blocked_by(self.addr, |chunk| {
                            live.has_inflight_request(chunk)
                        }),
                )
            })
            .transpose()?
            .flatten();
        debug!(?released, on_parole, "requests timed out");
        if released.is_some() {
            self.state.new_pieces_notify.notify_waiters();
        }
        Ok(())
    }

    async fn request_chunks(&self) -> crate::Result<()> {
        let handle = self.addr;
        self.wait_for_bitfield().await;

        loop {
            // Taken before what they guard is read, so no change in between
            // is missed.
            let mut wants_changed = std::pin::pin!(self.wants_changed());
            let finished = self.state.is_finished_and_no_active_streams();
            let Some((interesting, redundant)) =
                self.state
                    .peers
                    .with_live_mut(self.addr, "update_interest", |live| {
                        let interesting = self.state.update_interest(live);
                        (interesting, live.is_redundant(finished))
                    })
            else {
                return Ok(());
            };
            // The peer only uploads, and we only upload or want nothing it
            // has: the connection is of no use (libtorrent
            // `peer_connection::disconnect_if_redundant`, from
            // `do_update_interest`, `set_upload_only` and `torrent::finished`).
            if redundant {
                debug!("the peer only uploads and we want nothing of it, disconnecting peer");
                self.tx
                    .send(WriterRequest::Disconnect(Ok(())))
                    .ok()
                    .ok_or(Error::PeerTaskDead)?;
                // The writer sends what was queued before, NotInterested
                // among it, then closes the connection, which ends this
                // task.
                return std::future::pending().await;
            }
            // The peer has nothing we want: we wait until it, or what we
            // want, changes.
            if !interesting {
                aframe!(wants_changed).await;
                continue;
            }

            // What we want of the peer may change while we wait to ask it:
            // we look again.
            tokio::select! {
                () = aframe!(self.wait_for_request_slot()) => {}
                () = wants_changed.as_mut() => continue,
            }

            // Acquire a piece using the strategy: try steal (10x) → reserve → steal (3x).
            // What the peer lets us request changes too: it unchokes us, or
            // lets us fetch another piece while it chokes us.
            let slots_changed = self.request_slots_changed();
            let slot_notify = slots_changed.as_deref().map(Notify::notified);
            let next = match self.acquire_next_piece()? {
                Some(next) => next,
                None => {
                    debug!("no pieces to request");
                    let slot_changed = async {
                        match slot_notify {
                            Some(notified) => notified.await,
                            None => std::future::pending().await,
                        }
                    };
                    // Nothing changes while a piece another peer holds grows
                    // old enough to take over, or blocks asked of others
                    // become ones to double: the peer picks again after
                    // REPICK_INTERVAL (libtorrent `peer_connection::second_tick`
                    // picks again 5 s after the last request in the end game).
                    tokio::select! {
                        () = wants_changed => debug!("woken up, new pieces might be available"),
                        () = slot_changed => debug!("woken up, request slots changed"),
                        () = tokio::time::sleep(REPICK_INTERVAL) => debug!("picking again"),
                    }
                    continue;
                }
            };

            // Only the chunks not yet received: a stolen piece keeps the
            // chunks its previous owner delivered. In the end game, the one
            // block this peer doubles.
            let missing = self
                .state
                .lock_read("missing chunks")
                .get_pieces()?
                .chunks_to_request(next, self.addr)
                .collect::<Vec<_>>();

            for chunk in missing {
                let request = Request {
                    index: next.get(),
                    begin: chunk.offset,
                    length: chunk.size,
                };

                // The request goes out at once: the limits charge the block
                // as it crosses the socket, not before it is asked for
                // (libtorrent `peer_connection::send_block_requests` asks no
                // bandwidth; `setup_receive` reads within the quota).
                if !aframe!(self.wait_to_request(next)).await? {
                    break;
                }

                match self
                    .state
                    .peers
                    .with_live_mut(handle, "add chunk request", |live| {
                        // Only the piece's owner requests its chunks. The check
                        // and the request are one step under this peer's lock,
                        // so a steal either comes after and cancels the request,
                        // or comes first and this peer stops requesting.
                        let owned = self
                            .state
                            .lock_read("add chunk request")
                            .get_pieces()
                            .is_ok_and(|pieces| pieces.may_request(next, self.addr));
                        owned.then(|| live.add_inflight_request(chunk))
                    }) {
                    Some(Some(true)) => {}
                    // Stolen or released: the piece is no longer ours to request.
                    Some(None) => break,
                    Some(Some(false)) => {
                        // This request was already in-flight for this peer for this chunk.
                        // This might happen in theory, but not very likely.
                        //
                        // Example:
                        // someone stole a piece from us, and then died, the piece became "needed" again, and we reserved it
                        // all before the piece request was processed by us.
                        warn!(
                            id = self.state.shared.id,
                            info_hash = ?self.state.shared.info_hash,
                            addr = ?self.addr,
                            "we already requested {:?} previously",
                            chunk
                        );
                        continue;
                    }
                    // peer died
                    None => return Ok(()),
                };

                if self
                    .tx
                    .send(WriterRequest::Message(Message::Request(request)))
                    .is_err()
                {
                    return Ok(());
                }
            }
        }
    }

    fn on_i_am_choked(&self) -> crate::Result<()> {
        self.state
            .peers
            .with_live_mut(self.addr, "choked by the peer", |live| {
                live.choked_by_peer = true;
            });
        // A plain peer that chokes us drops every request we sent it (BEP 3).
        // A fast one answers each with its block or a Reject (BEP 6), so the
        // requests stay until it does (libtorrent
        // `bt_peer_connection::on_choke` takes them as rejected only without
        // the fast extension).
        if !self.speaks_fast() {
            self.return_requests()?;
        }
        self.notify_request_slots_changed();
        Ok(())
    }

    /// The peer will not send the block `request` names (BEP 6). The peer
    /// gives back every piece it owns and every request, as when its
    /// requests time out: a piece has one owner, which must be able to
    /// deliver all of it (libtorrent `peer_connection::incoming_reject_request`
    /// gives back only the block). A Reject for a request we never sent ends
    /// the connection (BEP 6: the peer should close it).
    fn on_rejected(&self, request: Request) -> anyhow::Result<()> {
        let Some(chunk) = self.requested_chunk(&request) else {
            anyhow::bail!("peer rejected {request:?}, which names no block of the torrent");
        };
        // A piece the peer rejects is no longer one it lets us fetch while
        // it chokes us, or, unchoked, one it suggests (libtorrent
        // `incoming_reject_request`).
        let choked = self.choked_by_peer();
        let Some(answer) =
            self.state
                .peers
                .with_live_mut(self.addr, "reject_inflight_request", |live| {
                    let offered = if choked {
                        &mut live.allowed_fast_from_peer
                    } else {
                        &mut live.suggested
                    };
                    offered.retain(|p| *p != chunk.piece_index);
                    live.reject_inflight_request(&chunk)
                })
        else {
            return Ok(());
        };
        match answer {
            RemoveInflightRequestResult::Expected => {
                self.return_requests()?;
            }
            RemoveInflightRequestResult::LateCanceled => {
                trace!(
                    ?request,
                    "peer rejected a request we had stopped waiting for"
                );
            }
            RemoveInflightRequestResult::Unexpected => {
                anyhow::bail!("peer rejected {request:?}, which we never asked it for")
            }
        }
        Ok(())
    }

    /// Every request outstanding to this peer stops being in flight, and every
    /// piece it owns goes back to the queue with the chunks it already
    /// delivered, for any peer to take. One step under this peer's lock, as a
    /// request is checked and added: the requester either finds the piece gone
    /// or has its request taken here. Returns the requests taken.
    fn return_requests(&self) -> crate::Result<Vec<InflightRequest>> {
        let returned = self
            .state
            .peers
            .with_live_mut(self.addr, "return_requests", |live| {
                let released = self
                    .state
                    .lock_write("return_requests")
                    .get_pieces_mut()?
                    .release_pieces_owned_by(self.addr);
                Ok::<_, Error>((released, live.take_inflight_requests()))
            })
            .transpose()?;
        let Some((released, taken)) = returned else {
            return Ok(Vec::new());
        };
        if released > 0 {
            self.state.new_pieces_notify.notify_waiters();
        }
        Ok(taken)
    }

    fn on_i_am_unchoked(&self) {
        trace!("we are unchoked");
        self.state
            .peers
            .with_live_mut(self.addr, "unchoked by the peer", |live| {
                live.choked_by_peer = false;
            });
        self.notify_request_slots_changed();
    }

    async fn on_received_piece(&self, piece: Piece<ByteBuf<'_>>) -> anyhow::Result<()> {
        let piece_index = self
            .state
            .lengths
            .validate_piece_index(piece.index)
            .with_context(|| format!("peer sent an invalid piece {}", piece.index))?;
        let chunk_info = match self.state.lengths.chunk_info_from_received_data(
            piece_index,
            piece.begin,
            piece.len().try_into().context("bug")?,
        ) {
            Some(i) => i,
            None => {
                anyhow::bail!("peer sent us an invalid piece {:?}", piece,);
            }
        };

        // Peer chunk/byte counters.
        self.counters
            .fetched_bytes
            .fetch_add(piece.len() as u64, Ordering::Relaxed);
        self.counters.fetched_chunks.fetch_add(1, Ordering::Relaxed);

        // A block we had stopped waiting for is data all the same: whether
        // it is written is the piece tracker's call (libtorrent
        // `peer_connection::incoming_piece`).
        self.state
            .peers
            .with_live_mut(self.addr, "inflight_requests.remove", |h| {
                match h.remove_inflight_request(&chunk_info) {
                    RemoveInflightRequestResult::Expected => Ok(()),
                    RemoveInflightRequestResult::LateCanceled => {
                        trace!(?piece, "peer sent a block after we cancelled it");
                        Ok(())
                    }
                    RemoveInflightRequestResult::Unexpected => anyhow::bail!(
                        "peer sent us a piece we did not ask. Inflight requests: {:?}. Got: {:?}",
                        h.inflight_requests_debug(),
                        piece,
                    ),
                }
            })
            .context("peer not found")??;

        // This one is used to calculate download speed.
        self.state
            .stats
            .fetched_bytes
            .fetch_add(piece.len() as u64, Ordering::Relaxed);
        self.state
            .session_stats
            .counters
            .fetched_bytes
            .fetch_add(piece.len() as u64, Ordering::Relaxed);

        fn write_to_disk(
            state: &TorrentStateLive,
            addr: PeerHandle,
            counters: &AtomicPeerCounters,
            piece: &Piece<ByteBuf<'_>>,
            chunk_info: &ChunkInfo,
        ) -> anyhow::Result<()> {
            let index = piece.index;

            // A block we lack is written, whoever owns its piece now. While
            // it is, no one steals the piece, so that by the time we are done
            // writing AND if it was the last block, we can actually checksum
            // etc.
            let (ppl_guard, cancel) = {
                let mut g = state.lock_write("check_steal");

                let ppl = state
                    .per_piece_locks
                    .get(piece.index as usize)
                    .map(|l| l.read());

                match g.get_pieces_mut()?.block_arrived(chunk_info, addr) {
                    Arrival::Write { cancel } => (ppl, cancel),
                    Arrival::Discard => {
                        state.add_redundant_bytes(piece.len() as u64);
                        debug!("{chunk_info:?} is had, or arrived from another first, ignoring");
                        return Ok(());
                    }
                }
            };
            // The block was asked of other peers too: they need not send it
            // (libtorrent `torrent::cancel_block`).
            for other in cancel {
                state
                    .peers
                    .with_live_mut(other, "cancel_duplicate", |live| {
                        live.cancel_inflight_request(chunk_info)
                    });
            }

            // While we hold per piece lock, noone can steal it.
            // So we can proceed writing knowing that the piece is ours now and will still be by the time
            // the write is finished.
            //

            // A block whose write failed is no longer being written, and is
            // fetched again (libtorrent `peer_connection::on_disk_write_complete`
            // calls `piece_picker::write_failed`, then
            // `torrent::handle_disk_error`).
            if let Err(e) = state.file_ops().write_chunk(addr, piece, chunk_info) {
                state
                    .lock_write("write_failed")
                    .get_pieces_mut()?
                    .write_failed(chunk_info);
                if state.handle_disk_error(&e) {
                    return Ok(());
                }
                error!(
                    id = state.shared.id,
                    info_hash = ?state.shared.info_hash,
                    "FATAL: error writing chunk to disk: {e:#}"
                );
                return state.on_fatal_error(e);
            }

            let piece_index = chunk_info.piece_index;
            let full_piece_download = {
                let mut g = state.lock_write("mark_chunk_downloaded");
                let chunk_marking_result = g.get_pieces_mut()?.mark_chunk_downloaded(piece, addr);
                trace!(?piece, chunk_marking_result=?chunk_marking_result);

                match chunk_marking_result {
                    Some(ChunkMarkingResult::Completed) => {
                        trace!("piece={} done, will write and checksum", piece.index);
                        // Out of flight and the queue, so no one takes it during
                        // its hash check.
                        Some(g.get_pieces_mut()?.take_inflight(piece_index))
                    }
                    Some(ChunkMarkingResult::PreviouslyCompleted) => {
                        // TODO: we might need to send cancellations here.
                        debug!("piece={} was done by someone else, ignoring", piece.index);
                        return Ok(());
                    }
                    Some(ChunkMarkingResult::NotCompleted) => None,
                    None => {
                        anyhow::bail!(
                            "bogus data received: {:?}, cannot map this to a chunk, dropping peer",
                            piece
                        );
                    }
                }
            };

            // We don't care about per piece lock anymore, as it's removed from inflight pieces.
            // It shouldn't impact perf anyway, but dropping just in case.
            drop(ppl_guard);

            // The time is known when the piece was in flight: one whose last
            // block came after it was given back has none.
            let Some((full_piece_download_time, senders)) = full_piece_download else {
                return Ok(());
            };
            let read_block = |who: PeerHandle, chunk: &ChunkInfo, buf: &mut [u8]| {
                state.file_ops().read_chunk(who, chunk, buf)
            };

            match state
                .file_ops()
                .check_piece(chunk_info.piece_index)
                .with_context(|| format!("error checking piece={index}"))?
            {
                Verdict::Inconsistent => {
                    return state.on_fatal_error(inconsistent_hashes(chunk_info.piece_index));
                }
                Verdict::Passed => {
                    state.layer_fetch.piece_passed(piece_index);
                    let failed = state.peers.piece_passed(piece_index);
                    // Before the storage takes the piece, while the good bytes
                    // are still there to read back.
                    for sender in smart_ban::senders_of_bad_blocks(&failed, read_block) {
                        state.peers.ban(sender.ip());
                    }
                    // The piece is ours only once the storage has taken it
                    // (libtorrent: the write completes before the hash job,
                    // and only then `torrent::piece_passed`). One it refuses
                    // is downloaded again, and its senders are neither
                    // credited nor blamed: on a disk error libtorrent
                    // `torrent::on_piece_verified` calls neither
                    // `piece_passed` nor `piece_failed`. A refusal for want
                    // of room puts the torrent in upload mode
                    // (`torrent::handle_disk_error`).
                    if let Err(e) = state.files.on_piece_completed(piece_index) {
                        warn!(
                            id = state.shared.id,
                            info_hash = ?state.shared.info_hash,
                            "the storage refused piece={index}: {e:#}"
                        );
                        state.handle_disk_error(&e);
                        return state.requeue_piece(piece_index);
                    }
                    state
                        .lock_write("mark_piece_downloaded")
                        .get_pieces_mut()?
                        .mark_piece_hash_ok(piece_index);
                    // Every peer that sent a block of it earns a point and
                    // leaves parole (libtorrent `torrent::piece_passed`).
                    for peer in senders.participants() {
                        state
                            .peers
                            .with_peer_mut(peer, "piece_passed", |p| p.piece_passed());
                    }

                    // Global piece counters.
                    let piece_len = state.lengths.piece_length(chunk_info.piece_index) as u64;
                    state
                        .stats
                        .downloaded_and_checked_bytes
                        // This counter is used to compute "is_finished", so using
                        // stronger ordering.
                        .fetch_add(piece_len, Ordering::Release);
                    state
                        .stats
                        .downloaded_and_checked_pieces
                        // This counter is used to compute "is_finished", so using
                        // stronger ordering.
                        .fetch_add(1, Ordering::Release);
                    state
                        .stats
                        .have_bytes
                        .fetch_add(piece_len, Ordering::Relaxed);
                    if let Some(time) = full_piece_download_time {
                        #[allow(clippy::cast_possible_truncation)]
                        state
                            .stats
                            .total_piece_download_ms
                            .fetch_add(time.as_millis() as u64, Ordering::Relaxed);

                        // Per-peer piece counters.
                        counters.on_piece_completed(piece_len, time);
                    }

                    trace!(piece = index, "successfully downloaded and verified");

                    state.on_piece_completed(chunk_info.piece_index)?;

                    state.transmit_haves(chunk_info.piece_index);
                    state.update_interest_of_holders(chunk_info.piece_index);
                }
                Verdict::Failed => {
                    warn!(
                        id = state.shared.id,
                        info_hash = ?state.shared.info_hash,
                        ?addr,
                        "checksum for piece={} did not validate", index
                    );
                    // The whole piece counts as corrupt (libtorrent
                    // `torrent::piece_failed` calls `add_failed_bytes` with
                    // the piece size).
                    state.stats.failed_bytes.fetch_add(
                        u64::from(state.lengths.piece_length(piece_index)),
                        Ordering::Relaxed,
                    );
                    // Before the storage lets go of the failed bytes.
                    let blocks =
                        smart_ban::read_back(&state.lengths, piece_index, &senders, read_block);
                    for liar in state.peers.piece_failed(piece_index, blocks) {
                        state.peers.ban(liar.ip());
                    }
                    // Checked by v2, a piece of more than one sender has its
                    // block hashes fetched, which name the senders of its bad
                    // blocks; until then no one is blamed (libtorrent
                    // `torrent::piece_failed` calls `verify_block_hashes`).
                    // Otherwise, or when one peer sent it all, every sender
                    // is blamed, and the sole sender known to be bad
                    // (libtorrent `torrent::piece_failed`,
                    // `torrent::penalize_peers`).
                    let v2_request = state.metadata.v2.as_ref().and_then(|v2| {
                        let request = hash_fetch::block_request(
                            v2,
                            state.lengths.default_piece_length().into(),
                            piece_index.get(),
                        )?;
                        Some((request, v2.share(piece_index.get())?))
                    });
                    match v2_request {
                        Some((request, share)) if !senders.sole_sender() => {
                            let failed = hash_fetch::failed_blocks(
                                &state.lengths,
                                piece_index,
                                share,
                                &senders,
                                read_block,
                            );
                            state
                                .layer_fetch
                                .verify_blocks(request, piece_index, failed);
                        }
                        _ => state.penalize_peers(
                            senders.participants(),
                            piece_index,
                            senders.sole_sender(),
                        ),
                    }
                    state.requeue_piece(piece_index)?;
                }
                // No hash to check the piece against: nobody is to blame
                // (libtorrent `torrent::on_piece_verified` neither passes nor
                // fails it).
                Verdict::Unknown => state.requeue_piece(piece_index)?,
            };
            Ok(())
        }

        self.state
            .shared
            .spawner
            .block_in_place_with_semaphore(|| {
                write_to_disk(&self.state, self.addr, &self.counters, &piece, &chunk_info)
            })
            .await
            .with_context(|| format!("error processing received chunk {chunk_info:?}"))?;

        Ok(())
    }

    fn send_metadata_piece(&self, piece_id: u32) -> anyhow::Result<()> {
        let data = UtMetadataData::piece_of(&self.state.metadata.info_bytes, piece_id)?;
        self.tx
            .send(WriterRequest::UtMetadata(UtMetadata::Data(data)))
            .context("error sending UtMetadata: channel closed")?;
        Ok(())
    }

    /// A PEX message from the peer (libtorrent
    /// `ut_pex_peer_plugin::on_extended`). One past [`MAX_PEX_MESSAGE_LEN`],
    /// or past the peer's rate, ends the connection. A peer on a local
    /// network it names is taken only from a peer on one. Each added peer
    /// it had not named before is news, a candidate for our peer list, up
    /// to [`peer::MAX_PEX_PEERS`] of each family; a dropped one is only
    /// forgotten as named, as the sender lost it, not we.
    fn on_pex_message(&self, msg: UtPex<ByteBuf<'_>>) -> anyhow::Result<()> {
        let len = msg.lists_len();
        if len > MAX_PEX_MESSAGE_LEN {
            anyhow::bail!("PEX message of {len} bytes, past {MAX_PEX_MESSAGE_LEN}");
        }
        let from_local = is_local(self.addr.ip());
        let news = self
            .state
            .peers
            .with_live_mut(self.addr, "pex_named", |live| {
                if !live.pex_arrived(tokio::time::Instant::now()) {
                    anyhow::bail!("PEX messages too frequent");
                }
                Ok(live.pex_named(
                    msg.added_peers()
                        .filter(|p| from_local || !is_local(p.addr.ip())),
                    msg.dropped_peers().map(|p| p.addr),
                ))
            })
            .transpose()?
            .unwrap_or_default();
        for peer in news {
            self.state.add_peer(peer.addr, PeerSourceFlag::Pex);
            self.state.peers.pex_flags(peer.addr, peer.flags);
        }
        Ok(())
    }

    // Whether the peer chokes us; it does until it says otherwise.
    fn choked_by_peer(&self) -> bool {
        self.state
            .peers
            .with_live(self.addr, |live| live.choked_by_peer)
            .unwrap_or(true)
    }

    /// Whether we may send the peer another request: it has room for one,
    /// and it does not choke us or lets us fetch some piece while it does
    /// (BEP 6).
    fn can_send_request(&self) -> bool {
        let request_window = self.lock_flow_control("can_send_request").request_window;

        self.state
            .peers
            .with_live(self.addr, |live| {
                (!live.choked_by_peer || !live.allowed_fast_from_peer.is_empty())
                    && live.can_hold_another_request(request_window)
            })
            .unwrap_or(false)
    }

    /// Waits until the peer has room for a request for a block of `piece`.
    /// False when the peer chokes us and does not let us fetch `piece`: like
    /// the requests libtorrent has not sent when it is choked
    /// (`peer_connection::clear_request_queue`), the rest of the piece is not
    /// asked for, so the piece goes back, and with it all the peer holds,
    /// which go back only together.
    async fn wait_to_request(&self, piece: ValidPieceIndex) -> crate::Result<bool> {
        loop {
            let Some(notify) = self.request_slots_changed() else {
                return Ok(true);
            };
            let notified = notify.notified();
            let request_window = self.lock_flow_control("wait_to_request").request_window;
            let slot = self.state.peers.with_live(self.addr, |live| {
                (
                    !live.choked_by_peer || live.allowed_fast_from_peer.contains(&piece),
                    live.can_hold_another_request(request_window),
                )
            });
            match slot {
                None | Some((true, true)) => return Ok(true),
                Some((false, _)) => {
                    self.return_requests()?;
                    return Ok(false);
                }
                Some((true, false)) => notified.await,
            }
        }
    }

    fn lock_flow_control(
        &self,
        reason: &'static str,
    ) -> TimedExistence<MutexGuard<'_, PeerFlowControl>> {
        TimedExistence::new(timeit(reason, || self.flow_control.lock()), reason)
    }

    fn request_slots_changed(&self) -> Option<Arc<Notify>> {
        self.state
            .peers
            .with_live(self.addr, |live| live.request_slots_changed())
    }

    fn notify_request_slots_changed(&self) {
        if let Some(notify) = self.request_slots_changed() {
            notify.notify_waiters();
        }
    }
}

fn format_peer_client_name(value: &ByteBuf<'_>) -> Option<String> {
    let client_name = String::from_utf8_lossy(value.as_ref()).trim().to_string();
    if client_name.is_empty() {
        return None;
    }

    Some(client_name)
}
