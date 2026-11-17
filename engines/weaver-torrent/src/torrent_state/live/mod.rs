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
mod rate_tick;
pub(crate) use rate_tick::RateTick;
mod admission;
mod blocks;
mod disk_jobs;
mod handler;
mod lifecycle;
mod peer_events;
mod pex;
mod pieces;
mod requests;
mod smart_ban;
mod snapshots;
pub mod stats;
#[cfg(test)]
pub(crate) mod tests;
mod upload;
pub(crate) mod web_seed;
mod web_seeding;

use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU8, AtomicU64, AtomicUsize},
    },
    time::Duration,
};

use crate::buffers::{ByteBuf, ByteBufOwned};
use crate::core::{
    hash_id::Id20,
    lengths::{ChunkInfo, Lengths, ValidPieceIndex},
    speed_estimator::SpeedEstimator,
    torrent_metainfo::ValidatedTorrentMetaV1Info,
};
use crate::peer_protocol::Handshake;
use crate::spawn_utils::DropOffShard;
use parking_lot::{Mutex, RwLock, RwLockReadGuard, RwLockWriteGuard};
use tokio::sync::{Notify, OwnedSemaphorePermit, Semaphore, watch};
use tokio_util::{sync::CancellationToken, task::TaskTracker};
use tracing::{debug, trace, warn};

use crate::{
    Error,
    chunk_tracker::{ChunkTracker, HaveNeededSelected},
    connection_slots::ConnectionSlot,
    disk::{Block, PeerDisk},
    file_info::FileInfo,
    file_ops::FileOps,
    limits::Channel,
    peer_connection::AnswerBacklog,
    piece_tracker::PieceTracker,
    session_stats::{
        SessionStats,
        swarm::{self},
    },
    stream_connect::ConnectionKind,
    type_aliases::{FileStorage, PeerHandle, PeerSourceFlag},
};

use self::{
    peer::{PeerTx, UploadLedger, stats::atomic::PeerCountersAtomic as AtomicPeerCounters},
    peers::{
        PeerIdClaim, PeerStates,
        stats::{AggregatePeerStats, AggregatePeerStatsAtomic},
    },
    stats::atomic::AtomicStats,
};

use super::{
    ManagedTorrentShared, TorrentMetadata,
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

    // Whether the torrent was finished when it was last asked whether it
    // had turned so (Self::turned_finished).
    finished: bool,
}

impl TorrentStateLocked {
    /// Whether the torrent turned finished since this was last asked, by a
    /// piece had or by a piece no longer wanted: true once for each time it
    /// does, so what follows the torrent finishing follows it once. A
    /// torrent that wants a piece again turns finished again when it has
    /// it.
    fn turned_finished(&mut self) -> crate::Result<bool> {
        let finished = self.get_chunks()?.is_finished();
        let was = std::mem::replace(&mut self.finished, finished);
        Ok(finished && !was)
    }

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

// Upload mode as it was entered: when, and whether it ends by itself once
// it lasted OPTIMISTIC_DISK_RETRY, or only when the torrent's owner says
// room was made.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct UploadMode {
    at: tokio::time::Instant,
    retry: bool,
}

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

/// What a torrent's running run holds now, read without the torrent's lock:
/// its peers by state and the rate it receives at. The counters are the
/// run's own, moved by each peer's change of state; the torrent lets go of
/// them as the run ends.
pub struct RunGauges {
    peers: Arc<AggregatePeerStatsAtomic>,
    download: Arc<SpeedEstimator>,
}

impl RunGauges {
    /// The run's peers by state now.
    pub fn peers(&self) -> AggregatePeerStats {
        self.peers.snapshot()
    }

    /// The bytes a second the run receives, over its rate window.
    pub fn download_bps(&self) -> u64 {
        self.download.bps()
    }
}

pub struct TorrentStateLive {
    peers: PeerStates,
    pub(crate) shared: Arc<ManagedTorrentShared>,
    metadata: Arc<TorrentMetadata>,
    _locked: RwLock<TorrentStateLocked>,

    // Closed off the uTP threads: the last hold of the run may be a peer's
    // task on one.
    pub(crate) files: DropOffShard<FileStorage>,
    // The read turns of the volume the files are on, asked of the storage
    // when the run begins and again when the files move: every read of the
    // files takes one of them. A connection takes its copy once, when it
    // starts, and is handed the new one when the files move.
    pub(crate) reads: arc_swap::ArcSwap<crate::storage::ReadTurns>,

    per_piece_locks: Vec<RwLock<()>>,

    stats: AtomicStats,
    // The run ended and its counts went into what the torrent moved: what a
    // connection still closing counts goes there too. Set under the lock of
    // what the torrent moved.
    run_counted: AtomicBool,
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
    // The torrent's want for a TCP socket its route held for others, while
    // it still has peers to dial: the route counts it pressing for as long.
    pub(crate) tcp_wants: crate::stream_connect::TcpWants,

    finished_notify: Notify,
    // Rings when the torrent may want pieces again after it finished: more
    // wanted, or a stream on a file it lacks.
    wanting_notify: Notify,
    // Rings when the pieces a peer may be handed change: pieces queued
    // again, or the wanted ones changed.
    new_pieces_notify: Notify,
    // The storage rings it when it has room again after refusing a piece.
    // Only a requester that was refused room waits on it: the storage gives
    // room back with every piece it takes.
    room_notify: Arc<Notify>,
    layer_fetch: hash_fetch::LayerFetch,
    // Whether every piece, wanted or not, was already present when this run
    // began: a run that began as a seed never completes (libtorrent
    // `torrent::files_checked` presets `m_complete_sent` for a torrent added as a
    // seed).
    seed_at_start: bool,

    down_speed_estimator: Arc<SpeedEstimator>,
    up_speed_estimator: SpeedEstimator,
    // Whether the run is on the session's rate tick.
    rate_ticked: AtomicBool,
    cancellation_token: CancellationToken,
    // Held, read, by each write and check of the run while it touches the
    // storage, and passed once, written, by the run's end: see
    // `storage_turn`.
    storage_fence: RwLock<()>,
    // Every task of the run, its connections' among them: once each has
    // ended after the run's end, every byte a closing connection drops has
    // been counted.
    tasks: TaskTracker,

    session_stats: Arc<SessionStats>,

    pub(crate) streams: Arc<TorrentStreams>,

    // The peers owed a turn with the upload scheduler, in the order they
    // became ready for one.
    upload_turns: tokio::sync::mpsc::UnboundedSender<PeerHandle>,

    // Whether the torrent speaks ut_pex: advertises it, sends it and reads
    // it. Not on a torrent that takes peers from its trackers alone, nor on
    // one added without it, nor in a session without it (libtorrent
    // `ut_pex_plugin::new_connection`, `torrent_flags::disable_pex`,
    // `session::add_extension`).
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
    upload_mode: watch::Sender<Option<UploadMode>>,
    // How many times upload mode was left. A refused write is read against
    // the count taken before it was made: one made before a leave enters
    // nothing after it, so a refusal that lands late cannot undo a wake.
    // Moved and checked only under the upload mode's own lock.
    upload_epoch: AtomicU64,
    // How many times the retry task saw an upload mode that waits for its
    // owner and left it be: what a test waits on to know it ran.
    #[cfg(test)]
    retry_declined: watch::Sender<u64>,
    // The torrent's files as its web seeds serve them.
    web_layout: web_seed::Layout,
    // Rings once per write that carried block bytes while an
    // [`UploadWatch`] listens. The write stamps its time, counts its bytes,
    // then rings only when `upload_watches` says someone listens: a torrent
    // nobody watches takes no lock for it.
    uploaded_notify: Notify,
    upload_watches: AtomicUsize,
    // When the last write that carried block bytes was handed to its
    // connection's writer, in nanoseconds after `upload_origin`, plus one;
    // zero before any (libtorrent `peer_connection::m_last_sent_payload`,
    // kept per torrent).
    upload_origin: tokio::time::Instant,
    last_upload: AtomicU64,

    // Whether the session connects over uTP, which a holepunch needs.
    utp: bool,
}

/// The block bytes a run sends its peers, as each connection hands them to
/// its writer: the payload of piece messages alone, before any encryption,
/// on every transport (libtorrent `stat::total_payload_upload`). While a
/// watch lives, each write that carried block bytes rings it.
pub struct UploadWatch {
    live: Arc<TorrentStateLive>,
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

    pub fn down_speed_estimator(&self) -> &SpeedEstimator {
        &self.down_speed_estimator
    }

    pub fn up_speed_estimator(&self) -> &SpeedEstimator {
        &self.up_speed_estimator
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

    /// The torrent's peer list, for tests outside the run's own module.
    #[cfg(test)]
    pub(crate) fn peer_states(&self) -> &PeerStates {
        &self.peers
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

    /// The torrent's files moved: the reads of the run take the turns of
    /// the volume they are on now, asked of the storage again. Each live
    /// connection is handed them on its writer's queue, and takes them
    /// between two of its batches; one that goes live after this reads
    /// them when it starts.
    pub(crate) fn files_moved(&self) {
        let turns = self.files.read_turns();
        self.reads.store(Arc::new(turns.clone()));
        self.peers.for_each_live(|_, _, live| {
            let _ = live
                .tx
                .send(crate::peer_connection::WriterRequest::ReadTurns(
                    turns.clone(),
                ));
        });
    }

    /// Bans `peer` for the bad data it sent. A peer of the swarm is banned
    /// by its host: here, and on every torrent of the session while it
    /// runs, each closing its connections to it (see
    /// [`PeerStates::ban_for_bad_data`]). libtorrent bans a peer on the one
    /// torrent it sent the data to (`torrent::ban_peer`). A web seed, at no
    /// address, is banned on this torrent alone and not connected to again
    /// ([`web_seed::WebSeeds::ban`]).
    pub(crate) fn ban_for_bad_data(&self, peer: PeerHandle) {
        let Some(ip) = peer.ip() else {
            if self.shared.web_seeds.ban(peer) {
                // A seed's URL can carry a credential: the line names the
                // seed by its entry alone.
                warn!(id = ?self.shared.id, %peer, "banning the web seed");
                self.peers
                    .with_peer_mut(peer, "ban_web_seed", |p| p.ban(&self.peers));
            }
            return;
        };
        if !self.peers.ban_for_bad_data(ip) {
            return;
        }
        let Some(session) = self.shared.session.upgrade() else {
            return;
        };
        for torrent in session.live_torrents() {
            if !std::ptr::eq(Arc::as_ptr(&torrent), self) {
                torrent.peers.close_session_banned(ip);
            }
        }
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

    /// The connection at `handle` completed its handshake `h`: it is live,
    /// and holds the peer id it sent. False when another connection holds
    /// that id and this one is to close; see [`PeerStates::claim_peer_id`].
    /// A web seed sends no peer id.
    ///
    /// The connection's send buffer is bound to `upload` here, under the
    /// hold of the peer that makes it live: the writer of this connection
    /// never looks it up by address, where a later connection could stand.
    fn set_peer_live(
        &self,
        handle: PeerHandle,
        h: Handshake,
        connection_kind: ConnectionKind,
        upload: &std::sync::OnceLock<Arc<UploadLedger>>,
    ) -> bool {
        let claim = self.peers.with_peer_mut(handle, "set_peer_live", |p| {
            p.connecting_to_live(h.peer_id, &self.peers, connection_kind);
            // An incoming connection is live before its handshake gets here.
            if let Some(live) = p.get_live_mut() {
                live.fast = h.supports_fast();
                upload.get_or_init(|| live.upload_ledger());
            }
            let outgoing = p.connected_out() == Some(true);
            match p.get_live() {
                Some(live) if handle.sends_peer_id() => Some(self.peers.claim_peer_id(
                    handle,
                    live.peer_id(),
                    outgoing,
                    self.shared.peer_id,
                )),
                _ => None,
            }
        });
        match claim.flatten() {
            Some(PeerIdClaim::TakenFrom(other)) => {
                debug!(%other, "closing the connection that sent the same peer id");
                self.peers.with_peer(other, peer::Peer::disconnect);
                true
            }
            Some(PeerIdClaim::Refused) => {
                debug!("closing: another connection sent the same peer id");
                false
            }
            Some(PeerIdClaim::Held) | None => true,
        }
    }

    pub fn get_hns(&self) -> Option<HaveNeededSelected> {
        self.lock_read("get_hns")
            .get_chunks()
            .ok()
            .map(|c| *c.get_hns())
    }

    /// `source` named `addr` (libtorrent `torrent::add_peer`). A mapped
    /// address names its IPv4 peer, so one peer has one entry.
    pub(crate) fn add_peer(&self, addr: SocketAddr, source: PeerSourceFlag) {
        let addr = crate::network::canonical_socket_addr(addr);
        self.session_stats
            .swarm
            .peer_named(swarm::PeerSource::of(source));
        if self.may_connect_to(addr) {
            self.peers.add_peer(addr, source);
        }
    }

    /// A PEX message added `addr` with `flags`, as [`Self::add_peer`] takes
    /// a peer. While the torrent's connections are plain only, a peer new to
    /// it that its sender reached encrypted is not taken, so never dialled,
    /// and only counted.
    pub(crate) fn add_pex_peer(&self, addr: SocketAddr, flags: u8) {
        let addr = crate::network::canonical_socket_addr(addr);
        let swarm = &self.session_stats.swarm;
        swarm.peer_named(swarm::PeerSource::Pex);
        if self.may_connect_to(addr) {
            let plain_only = self.encryption() == crate::EncryptionPolicy::Disabled;
            if self.peers.add_pex_peer(addr, flags, plain_only) {
                swarm.pex_encrypted_skipped();
            }
        }
    }

    /// The encryption the torrent's connections are made and kept under
    /// now: its own, else the session's.
    pub(crate) fn encryption(&self) -> crate::EncryptionPolicy {
        self.shared
            .options
            .peer_opts
            .encryption_under(self.shared.connector.encryption())
    }

    /// A holepunch asks us to connect to `addr`: over uTP, so only when we
    /// speak it (libtorrent `bt_peer_connection::on_holepunch`, `connect`).
    fn holepunch(&self, addr: SocketAddr) {
        let addr = crate::network::canonical_socket_addr(addr);
        if self.utp && self.may_connect_to(addr) {
            self.peers.holepunch(addr);
        }
    }

    /// The torrent's limits and the session's grant bytes of the `want` a
    /// peer asks for (`Limits::request_within`).
    async fn grant_bandwidth(&self, channel: Channel, want: u64) -> u64 {
        let session = self.shared.session.upgrade();
        self.shared
            .options
            .ratelimits
            .request_within(session.as_ref().map(|s| &s.ratelimits), channel, want)
            .await
    }

    /// Bytes granted to a peer that will not cross its socket go back to
    /// the torrent's limits and the session's.
    fn give_back_bandwidth(&self, channel: Channel, bytes: u64) {
        let session = self.shared.session.upgrade();
        self.shared.options.ratelimits.give_back_within(
            session.as_ref().map(|s| &s.ratelimits),
            channel,
            bytes,
        );
    }
}

// The ticks a peer's PEX messages go out on. A tick missed while the task
// was held up is taken late and the next comes a full `period` after it, so
// two messages never go out back to back (libtorrent
// `ut_pex_peer_plugin::tick`, ut_pex.cpp:92: a minute from the last message).
fn pex_interval(period: Duration) -> tokio::time::Interval {
    let mut interval = tokio::time::interval(period);
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
    interval
}

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

/// What a pick hands a peer.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Pick {
    /// The piece to request blocks of.
    Piece(ValidPieceIndex),
    /// Nothing the peer may fetch now.
    Nothing,
    /// The piece the peer would fetch waits on room the storage refused.
    NoRoom,
}

impl Pick {
    #[cfg(test)]
    fn piece(self) -> Option<ValidPieceIndex> {
        match self {
            Self::Piece(piece) => Some(piece),
            Self::Nothing | Self::NoRoom => None,
        }
    }
}

// All peer state that would never be used by other actors should pe put here.
// This state tracks a live peer.
struct PeerHandler {
    state: Arc<TorrentStateLive>,
    counters: Arc<AtomicPeerCounters>,
    // The connection's writes queued for the disk threads.
    disk: Arc<PeerDisk>,
    // The peer's send buffer, which the writer takes blocks from and
    // settles them in without the peer map; bound when the connection goes
    // live, and empty before.
    upload: std::sync::OnceLock<Arc<UploadLedger>>,
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

    // We let the connection go as of no use while in upload mode: one more
    // than the upload-mode count then, 0 when we did not. A close heard
    // after upload mode was left since is due again at once.
    let_go_in_upload_mode: AtomicU64,

    // The connection's PEX sender is started (BEP 11): once, at the first
    // extended handshake that names ut_pex.
    pex_started: AtomicBool,

    addr: PeerHandle,
    incoming: bool,
    tx: PeerTx,
    // The answers to the peer's requests that wait for the writer.
    answers: AnswerBacklog,

    first_message_received: AtomicBool,

    // What carries the connection, once its handshake says: a
    // `swarm::Transport` and one, none before.
    transport: AtomicU8,

    cancel_token: CancellationToken,

    client_name_and_version: String,
}

// A connection leaves the live set even when cancellation drops its task
// before it can report a result. Both exits use the same transition.
struct PeerLifetime(Option<PeerHandler>);

impl std::ops::Deref for PeerLifetime {
    type Target = PeerHandler;

    fn deref(&self) -> &Self::Target {
        self.0
            .as_ref()
            .expect("the connection still owns its handler")
    }
}

impl PeerLifetime {
    fn on_peer_died(mut self, error: Option<crate::Error>) -> crate::Result<()> {
        self.0
            .take()
            .expect("the connection owns its handler")
            .on_peer_died(error)
    }
}

impl Drop for PeerLifetime {
    fn drop(&mut self) {
        if let Some(handler) = self.0.take() {
            let _ = handler.on_peer_died(None);
        }
    }
}

/// The files with bytes in piece `piece`, by index. Files are in torrent
/// order, so they are the run from the first that ends past the piece's
/// first byte, found by halving as a torrent may have many thousands of
/// files and this runs under the torrent's lock for every piece had, to
/// the last that starts in the piece. An empty file inside that run holds
/// no piece and adds nothing, but does not end it.
fn files_in_piece<'a>(
    file_infos: &'a [FileInfo],
    lengths: &Lengths,
    piece: ValidPieceIndex,
) -> impl Iterator<Item = (usize, &'a FileInfo)> {
    let piece_start = lengths.piece_offset(piece);
    let piece = piece.get();
    let first = file_infos.partition_point(|fi| fi.offset_in_torrent + fi.len <= piece_start);
    file_infos
        .iter()
        .enumerate()
        .skip(first)
        .skip_while(move |(_, fi)| fi.piece_range.end <= piece)
        .take_while(move |(_, fi)| fi.piece_range.start <= piece)
}

/// What became of a block handed to the disk: dropped, written, or written
/// as its piece's last, with the piece's download time and senders and the
/// piece's check, which owns the piece from when it left flight.
type Landed = anyhow::Result<Option<(Option<Duration>, crate::piece_tracker::Senders, PieceCheck)>>;

/// Blocks admitted to be written and not yet marked. A panic between the
/// two lets them go, so a later copy is taken (libtorrent
/// `piece_picker::write_failed`).
struct Admitted<'a> {
    state: &'a TorrentStateLive,
    chunks: Vec<ChunkInfo>,
}

/// Reads the blocks a connection uploads, on the thread it is handed to.
struct UploadReader {
    state: Arc<TorrentStateLive>,
    addr: PeerHandle,
}

impl crate::peer_connection::ChunkReader for UploadReader {
    fn read_chunks(
        &self,
        chunks: &[ChunkInfo],
        bufs: &mut [&mut [u8]],
        held: Option<&mut crate::file_ops::HeldPiece>,
    ) -> std::result::Result<(), (usize, anyhow::Error)> {
        self.state
            .file_ops()
            .read_chunks(self.addr, chunks, bufs, held)
    }
}

/// How an outgoing connection's first part ended: the connection over, or
/// open and moving to its stream's thread.
enum Ran {
    Ended(crate::Result<()>),
    Moving(Box<crate::peer_connection::Connected>),
}

/// Whether a connection's stream runs on a runtime other than the one the
/// connect ran on, so the connection moves there. A session has more than
/// one uTP thread: being on one of them is not being on the stream's.
fn runs_elsewhere(home: Option<&tokio::runtime::Handle>) -> bool {
    home.is_some_and(|home| {
        !tokio::runtime::Handle::try_current().is_ok_and(|here| here.id() == home.id())
    })
}

/// Tells the torrent a connection ended, and how.
fn peer_ended(handler: PeerLifetime, res: crate::Result<()>) -> crate::Result<()> {
    match res {
        // We disconnected the peer ourselves as we don't need it
        Ok(()) => handler.on_peer_died(None)?,
        Err(e) => {
            debug!("error managing peer: {:#}", e);
            handler.on_peer_died(Some(e))?;
        }
    }
    Ok(())
}

/// A block received into its own buffer, queued for the disk threads. The
/// job belongs to the torrent: however the connection that received the
/// block ends, the block is written and its piece checked, and the run's end
/// waits for it.
pub(crate) struct WriteJob {
    state: Arc<TorrentStateLive>,
    tx: PeerTx,
    addr: PeerHandle,
    counters: Arc<AtomicPeerCounters>,
    chunk: ChunkInfo,
    // Before `written`: the buffer goes back before the peer is told.
    block: Block,
    written: Written,
    _run: tokio_util::task::task_tracker::TaskTrackerToken,
}

/// Tells the peer its write is done, however the write ends: landed, or
/// dropped unrun, as a job of a torrent paused or of a disk thread that
/// panicked or never started is. A peer stopped at the high mark reads
/// again once its writes are done.
struct Written(Option<Arc<PeerDisk>>);

/// A whole piece its storage held in memory, checked and passed, handed to
/// the storage to write (`TorrentStorage::store_verified_piece`). The
/// storage writes it on a thread of its own and says how the write ended
/// with [`StoredPiece::stored`]; the piece is had only then, on the hashing
/// thread, as it would be had the write been made there. Dropped unsaid,
/// the piece was not written and is fetched again, as a check dropped
/// unrun is.
pub struct StoredPiece {
    job: Option<HashJob>,
    // The upload epoch taken before the storage was handed the piece, which
    // its refusal is judged at (`handle_disk_error`).
    epoch: u64,
}

/// How the storage's write of a passed piece ended, carried back to the
/// hashing thread.
pub(crate) enum Stored {
    Written(anyhow::Result<()>, u64),
    Dropped,
}

/// The check of a piece whose last block was written. Like the write, it
/// belongs to the torrent and runs whatever became of the connection
/// (libtorrent `torrent::on_piece_verified` takes the verdict whatever
/// became of the sender), and the run's end waits for it.
pub(crate) struct HashJob {
    state: Arc<TorrentStateLive>,
    tx: PeerTx,
    addr: PeerHandle,
    counters: Arc<AtomicPeerCounters>,
    chunk: ChunkInfo,
    time: Option<Duration>,
    senders: crate::piece_tracker::Senders,
    check: PieceCheck,
    // The piece counted while it waits whole in its storage's memory, and
    // the connection told when it waits no more: both until the storage
    // said the piece is written, or its check failed, or the job is
    // dropped.
    waiting: Option<crate::disk::Span>,
    written: Written,
    // Counted among its session's checks that wait for the hashing thread,
    // until the thread takes it.
    queued: Option<crate::spawn_utils::QueuedCheck>,
    // Dropped when the check is over, for a connection that waits for it.
    ended: Option<tokio::sync::oneshot::Sender<()>>,
    // Set on a passed piece its storage has written or dropped: the job is
    // back on the hashing thread to act on that, not to hash.
    stored: Option<Stored>,
    _run: tokio_util::task::task_tracker::TaskTrackerToken,
}

/// A piece out of flight and out of the queue for its check, which no one
/// else takes until its verdict. A check dropped before it gives one, as
/// one whose disk thread panicked or never started is, puts the piece back
/// to be fetched again, so it is not lost.
struct PieceCheck {
    piece: ValidPieceIndex,
    state: Option<Arc<TorrentStateLive>>,
}

fn format_peer_client_name(value: &ByteBuf<'_>) -> Option<String> {
    let client_name = String::from_utf8_lossy(value.as_ref()).trim().to_string();
    if client_name.is_empty() {
        return None;
    }

    Some(client_name)
}
