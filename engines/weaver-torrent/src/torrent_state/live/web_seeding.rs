// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver
//
// Derived from rqbit (https://github.com/ikatson/rqbit), Copyright 2021 Igor Katson,
// licensed under the Apache License 2.0 (see LICENSE-APACHE). Modified by the
// Weaver authors from 2026-10-20 onward.

use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicU8, AtomicU64, Ordering},
};

use crate::core::net::{HttpClients, Resolver};
use parking_lot::Mutex;
use tracing::{debug, debug_span};

use crate::{Error, peer_connection::AnswerBacklog};

use super::{
    peer::{PeerRx, PeerTx},
    web_seed::{SeedLink, WEB_SEED_TICK, WebSeedConnection},
};

use super::{Held, PeerFlowControl, PeerHandler, PeerLifetime, TorrentStateLive};

impl TorrentStateLive {
    /// Connects to the web seeds that are due every WEB_SEED_TICK, and as
    /// soon as one may have become due (libtorrent `torrent::second_tick`,
    /// `maybe_connect_web_seeds`). A torrent with no web seed, or one that
    /// is finished with no stream wanting more, wants nothing of one: it
    /// waits without a timer for a web seed to change, a stream to open or
    /// close, or the torrent to want pieces again.
    pub(super) async fn task_web_seeds(self: Arc<Self>) -> crate::Result<()> {
        let mut tick = tokio::time::interval(WEB_SEED_TICK);
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            // Taken before the look, so wanting again or a stream opened
            // between the two still wakes it.
            let mut wanting = std::pin::pin!(self.wanting_notify.notified());
            wanting.as_mut().enable();
            let mut streamed = std::pin::pin!(self.streams.changed.notified());
            streamed.as_mut().enable();
            if self.shared.web_seeds.is_empty() {
                self.shared.web_seeds.changed().await;
            } else if self.is_finished_and_no_active_streams() {
                tokio::select! {
                    _ = wanting => {}
                    _ = streamed => {}
                    _ = self.shared.web_seeds.changed() => {}
                }
                tick.reset();
            } else {
                tokio::select! {
                    _ = tick.tick() => {}
                    _ = self.shared.web_seeds.changed() => {}
                }
            }
            self.maybe_connect_web_seeds()?;
            // A session closed under the torrent ends this task, as it ends
            // the peer adder: not a fault.
            if self.shared.session.strong_count() == 0 {
                debug!("the session is gone: no more web seeds are tried");
                return Ok(());
            }
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
        // A session closed under the torrent has no web seed to try: its
        // task ends at its next turn.
        let Some(session) = self.shared.session.upgrade() else {
            return Ok(());
        };
        let registry = &self.shared.web_seeds;
        let due = registry.due(
            tokio::time::Instant::now(),
            |lacks| self.web_layout.bitfield(lacks).any(),
            session.allow_idna,
        );
        for due in due {
            // The IP filter: a seed it blocks is tried again later.
            if due.endpoint.is_some_and(|e| !self.may_reach(e)) {
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
            let clients = self.shared.naming.http_clients.clone();
            let resolver = self.shared.naming.resolver.clone();
            self.spawn(
                debug_span!(parent: self.shared.span.clone(), "web_seed", seed = ?link.handle()),
                format!("[{}][{}]manage_web_seed", self.shared.id, link.handle()),
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
        };
        let handler = PeerLifetime(Some(handler));
        let _token_guard = handler.cancel_token.clone().drop_guard();
        handler
            .counters
            .outgoing_connection_attempts
            .fetch_add(1, Ordering::Relaxed);
        // The session's IP filter, on the address the seed's host resolved to.
        let allowed = |addr| self.may_reach(addr);
        let allow_idna = self
            .shared
            .session
            .upgrade()
            .is_some_and(|session| session.allow_idna);
        let counts = &self.session_stats.web_seeds;
        let connection = WebSeedConnection::new(
            clients,
            resolver,
            &allowed,
            &self.web_layout,
            &link,
            &self.shared.user_agent,
        )
        .allowing_idna(allow_idna)
        .counting(counts.clone());
        let res = tokio::select! {
            r = handler.task_peer_chunk_requester() => r,
            r = connection.run(&*handler, rx) => r,
        };
        match res {
            Ok(()) => handler.on_peer_died(None)?,
            Err(e) => {
                debug!("web seed connection failed: {e:#}");
                counts.failed();
                link.failed();
                handler.on_peer_died(Some(e))?;
            }
        }
        Ok(())
    }
}
