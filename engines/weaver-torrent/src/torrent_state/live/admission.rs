// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver
//
// Derived from rqbit (https://github.com/ikatson/rqbit), Copyright 2021 Igor Katson,
// licensed under the Apache License 2.0 (see LICENSE-APACHE). Modified by the
// Weaver authors from 2026-10-20 onward.

use super::peers;
use std::{
    collections::HashMap,
    net::SocketAddr,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU8, AtomicU64, Ordering},
    },
};

use parking_lot::Mutex;
use tokio::sync::{OwnedSemaphorePermit, mpsc::unbounded_channel};
use tracing::{Instrument, debug, debug_span};

use crate::{
    Error,
    connection_slots::{ConnectionSlot, ConnectionSlots},
    limits::LimitKind,
    peer_connection::{
        AnswerBacklog, DEFAULT_CONNECT_TIMEOUT, PeerConnection, PeerConnectionOptions,
        WriterRequest,
    },
    session::CheckedIncomingConnection,
    torrent_state::utils::atomic_inc,
    type_aliases::PeerHandle,
};

use super::{
    peer::{DisconnectRank, PeerRx, PeerState, PeerTx, opening_rank},
    peers::IncomingRefused,
};

use super::{
    AddIncomingPeerResult, Held, PEER_TURNOVER, PeerFlowControl, PeerHandler, PeerLifetime, Place,
    Ran, Room, TorrentStateLive, peer_ended, runs_elsewhere,
};

impl TorrentStateLive {
    /// Takes an incoming connection that holds `slot` under the session's
    /// cap. At the torrent's own cap it displaces a connection that ranks
    /// lower, or is refused (libtorrent `torrent::attach_peer`).
    /// A connection over uTP gives the thread its stream runs on as `home`:
    /// its task runs there.
    pub(crate) fn add_incoming_peer(
        self: &Arc<Self>,
        checked_peer: CheckedIncomingConnection,
        slot: ConnectionSlot,
        home: Option<tokio::runtime::Handle>,
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

        // Incoming peers are live before their task is polled, so its
        // lifetime must already be owned when the task is scheduled.
        let handler = PeerLifetime(Some(PeerHandler {
            addr: checked_peer.addr,
            incoming: true,
            on_bitfield_notify: Default::default(),
            speaks_v2: AtomicBool::new(false),
            sent_dht_port: AtomicBool::new(false),
            let_go_in_upload_mode: AtomicU64::new(0),
            pex_started: AtomicBool::new(false),
            flow_control: Mutex::new(PeerFlowControl::new(self.max_request_window())),
            state: self.clone(),
            tx,
            answers: AnswerBacklog::default(),
            counters,
            first_message_received: AtomicBool::new(false),
            transport: AtomicU8::new(0),
            disk: Default::default(),
            upload: Default::default(),
            cancel_token: self.cancellation_token.child_token(),
            client_name_and_version: self.shared.client_name_and_version().to_owned(),
        }));
        self.spawn_on(
            home.as_ref(),
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
                    .task_manage_incoming_peer(checked_peer, handler, rx, held)
            ),
        );
        Ok(AddIncomingPeerResult::Added)
    }

    async fn task_manage_incoming_peer(
        self: Arc<Self>,
        checked_peer: CheckedIncomingConnection,
        handler: PeerLifetime,
        rx: PeerRx,
        _held: Held,
    ) -> crate::Result<()> {
        let _token_guard = handler.cancel_token.clone().drop_guard();
        let options = self.shared.options.peer_opts;
        let peer_connection = PeerConnection::new(
            checked_peer.addr,
            self.shared.info_hashes,
            self.shared.peer_id,
            &*handler,
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
            let_go_in_upload_mode: AtomicU64::new(0),
            pex_started: AtomicBool::new(false),
            flow_control: Mutex::new(PeerFlowControl::new(state.max_request_window())),
            state: state.clone(),
            tx,
            answers: AnswerBacklog::default(),
            counters,
            first_message_received: AtomicBool::new(false),
            transport: AtomicU8::new(0),
            disk: Default::default(),
            upload: Default::default(),
            cancel_token: state.cancellation_token.child_token(),
            client_name_and_version: state.shared.client_name_and_version().to_owned(),
        };
        let handler = PeerLifetime(Some(handler));
        let token_guard = handler.cancel_token.clone().drop_guard();

        let options = PeerConnectionOptions {
            connect_timeout: Some(connect_timeout),
            ..options
        };

        handler
            .counters
            .outgoing_connection_attempts
            .fetch_add(1, Ordering::Relaxed);
        let mut rx = Some(rx);
        let ran = {
            let peer_connection = state.outgoing_connection(addr, &handler, options, transports);
            let requester = aframe!(
                handler
                    .task_peer_chunk_requester()
                    .instrument(debug_span!("chunk_requester"))
            );
            tokio::pin!(requester);
            let connecting = aframe!(
                peer_connection
                    .connect_outgoing()
                    .instrument(debug_span!("peer_connection"))
            );
            tokio::select! {
                r = &mut requester => Ran::Ended(r),
                r = connecting => match r {
                    // A uTP stream runs on a thread of its own: the
                    // connection moves there, so the stream and the peer
                    // that reads and writes it hand bytes to one another
                    // without waking another thread.
                    Ok(connected) if runs_elsewhere(connected.link.home.as_ref()) => {
                        Ran::Moving(Box::new(connected))
                    }
                    Ok(connected) => {
                        let rx = rx.take().expect("the connection's queue is taken once");
                        let managed = aframe!(
                            peer_connection
                                .manage_connected(connected, rx)
                                .instrument(debug_span!("peer_connection"))
                        );
                        Ran::Ended(tokio::select! {
                            r = &mut requester => r,
                            r = managed => r,
                        })
                    }
                    Err(e) => Ran::Ended(Err(e)),
                },
            }
        };
        let connected = match ran {
            Ran::Ended(res) => return peer_ended(handler, res),
            Ran::Moving(connected) => *connected,
        };

        // The rest of the connection runs on the stream's thread, as a task
        // of the torrent's run.
        let home = connected
            .link
            .home
            .clone()
            .expect("a moving connection has a home");
        let rx = rx.take().expect("the connection's queue is taken once");
        let run = state.clone();
        let moved = async move {
            let _token_guard = token_guard;
            let _held = _held;
            let peer_connection = run.outgoing_connection(addr, &handler, options, transports);
            let requester = aframe!(
                handler
                    .task_peer_chunk_requester()
                    .instrument(debug_span!("chunk_requester"))
            );
            let managed = aframe!(
                peer_connection
                    .manage_connected(connected, rx)
                    .instrument(debug_span!("peer_connection"))
            );
            let res = tokio::select! {
                r = requester => r,
                r = managed => r,
            };
            drop(peer_connection);
            peer_ended(handler, res)
        };
        state.spawn_on(
            Some(&home),
            tracing::Span::current(),
            format!("[{}][addr={addr}]manage_outgoing_peer", state.shared.id),
            moved,
        );
        Ok(())
    }

    /// The connection to `addr` an outgoing peer's task runs.
    fn outgoing_connection<'a>(
        &self,
        addr: SocketAddr,
        handler: &'a PeerHandler,
        options: PeerConnectionOptions,
        transports: crate::stream_connect::Transports,
    ) -> PeerConnection<&'a PeerHandler> {
        PeerConnection::new(
            addr,
            self.shared.info_hashes,
            self.shared.peer_id,
            handler,
            Some(options),
            self.shared.spawner.clone(),
            self.shared.connector.clone(),
        )
        .over(transports)
        .under(self.shared.ssl.clone())
    }

    pub(super) async fn task_peer_adder(self: Arc<Self>) -> crate::Result<()> {
        let state = self;
        loop {
            // Whether we are finished decides who is a connect candidate:
            // the list looks again before it is waited on, so a known seed
            // wakes nothing once we seed too (libtorrent
            // `peer_list::connect_one_peer`, `recalculate_connect_candidates`).
            state
                .peers
                .set_finished(state.is_finished_and_no_active_streams());
            if !state.wants_peers() {
                // Nothing to fetch and nothing to give: connect to no one until
                // the peers are wanted again.
                debug!("connect loop idle: no peers are wanted");
                state.peers.candidates_changed().await;
                continue;
            }
            // Why the loop waits, so a torrent that connects to no one shows
            // whether it knows no one, or is held by a cap.
            if tracing::enabled!(tracing::Level::DEBUG) {
                let next = state.peers.next_candidate(tokio::time::Instant::now());
                if next != peers::NextCandidate::Due {
                    let stats = state.peers.stats();
                    debug!(
                        ?next,
                        seen = stats.seen,
                        queued = stats.queued,
                        connecting = stats.connecting,
                        live = stats.live,
                        dead = stats.dead,
                        not_needed = stats.not_needed,
                        "connect loop waiting for a candidate"
                    );
                }
            }
            state.peers.wait_for_candidate().await;
            // A session closed under the torrent ends its adder: that is
            // how a torrent leaves a session, not a fault.
            let Some(session) = state.shared.session.upgrade() else {
                debug!("the session is gone: no more peers are added");
                return Ok(());
            };
            let slots = session.connection_slots.clone();
            drop(session);
            // The torrent's own cap first, so a torrent at it holds no place
            // under the session's; then a place under the session's cap and
            // a connect from its per-second budget, both shared with every
            // torrent in turn.
            let permit = match state.peer_semaphore.clone().try_acquire_owned() {
                Ok(permit) => permit,
                Err(_) => {
                    debug!(
                        limit = state.peer_limit,
                        "connect loop waiting: the torrent is at its peer limit"
                    );
                    state.peer_semaphore.clone().acquire_owned().await?
                }
            };
            let slot = match slots.try_open() {
                Some(slot) => slot,
                None => {
                    debug!(
                        in_use = slots.in_use(),
                        limit = slots.limit(),
                        "connect loop waiting: the session is at its connection limit"
                    );
                    slots.open().await?
                }
            };
            // The slots may have taken a while: we may have finished since,
            // so the list looks again, and we connect to whoever is due now.
            state
                .peers
                .set_finished(state.is_finished_and_no_active_streams());
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

    // Whether we may connect to a peer at `addr`: one on a port the session
    // does not refuse (libtorrent `torrent::add_peer` takes no peer below
    // port 1024 under `no_connect_privileged_ports`), that we may reach. The
    // peer list holds no address we may not connect to.
    pub(super) fn may_connect_to(&self, addr: SocketAddr) -> bool {
        let Some(session) = self.shared.session.upgrade() else {
            return false;
        };
        if session.refuses_port(addr.port()) {
            debug!(?addr, "skipping peer on a privileged port");
            return false;
        }
        self.may_reach(addr)
    }

    // Whether the session's IP filter now refuses the peer at `addr`, which
    // is then to leave the peer list (`refilter`).
    pub(super) fn refuses(&self, addr: SocketAddr) -> bool {
        self.shared.session.upgrade().is_some_and(|session| {
            session.refuses_port(addr.port()) || session.may_reach(addr.ip()).is_err()
        })
    }

    /// The session's IP filter changed: every peer on the list it now
    /// refuses leaves it, an idle one at once, a connected one when its
    /// connection, asked to close, has closed (libtorrent
    /// `torrent::ip_filter_updated` and `privileged_port_updated`, through
    /// `peer_list::apply_ip_filter` and `apply_port_filter`).
    pub(crate) fn refilter(&self) {
        self.peers.refuse(|addr| self.refuses(addr));
    }

    /// Whether the peers of a private magnet that no tracker named still
    /// wait for a tracker's answer to leave the list; see
    /// [`PeerStates::drop_untracked`].
    pub(crate) fn owes_untracked(&self) -> bool {
        self.peers.owes_untracked()
    }

    /// A tracker answered an announce of the torrent; see
    /// [`PeerStates::drop_untracked`].
    pub(crate) fn tracker_answered(&self) {
        self.peers.drop_untracked();
    }

    /// The session's `allow_multiple_connections_per_ip` changed: the
    /// connect loop looks again at the peers it held back.
    pub(crate) fn connections_per_ip_changed(&self) {
        self.peers.connections_per_ip_changed();
    }

    // Whether we may open a connection to `addr`, a peer or a web seed: a
    // port to connect to (libtorrent `peer_list::add_peer` takes no port-0
    // peer), at an address the session may reach (`Session::may_reach`, the
    // rule trackers pass too).
    pub(super) fn may_reach(&self, addr: SocketAddr) -> bool {
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
    pub(super) fn hold_place(
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

    // A torrent wants peers, outgoing and incoming, unless it has nothing
    // left to fetch and its own upload is closed, so it has nothing to give.
    fn wants_peers(&self) -> bool {
        self.shared.options.ratelimits.get_upload_bps() != LimitKind::Closed
            || !self.is_finished_and_no_active_streams()
    }

    /// The torrent may want pieces again: the peers let go for being of no
    /// use come back, and the web seeds are looked at again.
    pub(crate) fn reconnect_all_not_needed_peers(&self) {
        self.peers.requeue_not_needed();
        self.wanting_notify.notify_waiters();
    }
}
