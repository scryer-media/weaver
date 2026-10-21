// SPDX-License-Identifier: GPL-3.0-or-later

//! Fetching the piece layers a v2 torrent lacks from the peers that speak
//! v2, one run of [`LAYER_CHUNK`] pieces of a file at a time (libtorrent
//! `hash_picker`). No piece is requested before the run of the layer it
//! lies in is held ([`TorrentMetadata::can_verify`]).
//!
//! [`TorrentMetadata::can_verify`]: crate::torrent_state::TorrentMetadata::can_verify
//! [`LAYER_CHUNK`]: crate::v2_hashes::LAYER_CHUNK

use std::{net::SocketAddr, ops::Range, sync::atomic::Ordering, time::Duration};

use anyhow::Context;
use parking_lot::Mutex;
use tokio::{sync::Notify, time::Instant};

use super::PeerHandler;
use crate::{
    buffers::ByteBuf,
    peer_connection::WriterRequest,
    peer_protocol::{HashRequest, Hashes, Message},
    v2_hashes::V2Hashes,
};

/// The hash requests a peer holds at once (libtorrent
/// `bt_peer_connection::maybe_send_hash_request`).
const REQUESTS_PER_PEER: usize = 2;

/// A request out to a peer, and when the run it asks for may go to another.
struct Out {
    peer: SocketAddr,
    deadline: Instant,
}

/// A run of a file's piece layer not yet held.
struct Wanted {
    request: HashRequest,
    pieces: Range<u32>,
    /// The peers asked for it whose answer is still due.
    out: Vec<Out>,
    /// The peers that rejected it, which are not asked for it again.
    rejected_by: Vec<SocketAddr>,
}

/// What a peer is to do about the layers.
pub(crate) enum Pick {
    /// Send it these requests.
    Send(Vec<HashRequest>),
    /// Ask again when a request is answered or rejected, the peer's pieces
    /// change, or at the instant given, when a request out to another peer
    /// times out.
    Wait(Option<Instant>),
    /// Every layer is held.
    Done,
}

/// The runs of the piece layers a torrent lacks, and which peers they are
/// out to (libtorrent `hash_picker::m_piece_hash_requested`).
pub(crate) struct LayerFetch {
    wanted: Mutex<Vec<Wanted>>,
    /// Rung when a run is held, a request is answered or rejected, or a
    /// peer leaves.
    changed: Notify,
}

impl LayerFetch {
    pub fn new(v2: Option<&V2Hashes>) -> Self {
        let wanted = v2
            .into_iter()
            .flat_map(|v2| {
                v2.missing_chunks().map(|(file, chunk)| Wanted {
                    request: v2.chunk_request(file, chunk),
                    pieces: v2.chunk_pieces(file, chunk),
                    out: Vec::new(),
                    rejected_by: Vec::new(),
                })
            })
            .collect();
        Self {
            wanted: Mutex::new(wanted),
            changed: Notify::new(),
        }
    }

    /// The requests to send `peer`, which has the pieces `has` says and is
    /// given `timeout` to answer: the runs it has a piece of that are out
    /// to no one, or only to peers past their timeout, up to
    /// [`REQUESTS_PER_PEER`] held at once (libtorrent
    /// `hash_picker::pick_hashes`).
    pub fn pick(
        &self,
        peer: SocketAddr,
        has: impl Fn(u32) -> bool,
        timeout: Duration,
        now: Instant,
    ) -> Pick {
        let mut wanted = self.wanted.lock();
        if wanted.is_empty() {
            return Pick::Done;
        }
        let mut held = wanted
            .iter()
            .filter(|w| w.out.iter().any(|o| o.peer == peer))
            .count();
        let mut send = Vec::new();
        let mut wake: Option<Instant> = None;
        for w in wanted.iter_mut() {
            if held >= REQUESTS_PER_PEER {
                break;
            }
            if w.out.iter().any(|o| o.peer == peer) || w.rejected_by.contains(&peer) {
                continue;
            }
            if let Some(due) = w.out.iter().map(|o| o.deadline).max()
                && due > now
            {
                wake = Some(wake.map_or(due, |at| at.min(due)));
                continue;
            }
            if !w.pieces.clone().any(&has) {
                continue;
            }
            w.out.push(Out {
                peer,
                deadline: now + timeout,
            });
            send.push(w.request);
            held += 1;
        }
        if send.is_empty() {
            Pick::Wait(wake)
        } else {
            Pick::Send(send)
        }
    }

    /// `peer` answered `request`: whether that request was out to it
    /// (libtorrent `bt_peer_connection::on_hashes` ignores the others).
    fn answered(&self, peer: SocketAddr, request: &HashRequest) -> bool {
        let asked = self.take_out(peer, request).is_some();
        self.changed.notify_waiters();
        asked
    }

    /// The run `request` asks for is held.
    fn landed(&self, request: &HashRequest) {
        self.wanted.lock().retain(|w| w.request != *request);
        self.changed.notify_waiters();
    }

    /// `peer` rejected `request`: the run may go to another peer at once
    /// (libtorrent `hash_picker::hashes_rejected`), and is not asked of it
    /// again.
    fn rejected(&self, peer: SocketAddr, request: &HashRequest) {
        if let Some(mut w) = self.take_out(peer, request) {
            w.rejected_by.push(peer);
        }
        self.changed.notify_waiters();
    }

    /// `peer` left: what it held may go to another.
    pub fn forget(&self, peer: SocketAddr) {
        for w in self.wanted.lock().iter_mut() {
            w.out.retain(|o| o.peer != peer);
            w.rejected_by.retain(|p| *p != peer);
        }
        self.changed.notify_waiters();
    }

    /// The run `request` asks for, taken off `peer`'s requests, when it was
    /// out to it.
    fn take_out(
        &self,
        peer: SocketAddr,
        request: &HashRequest,
    ) -> Option<parking_lot::MappedMutexGuard<'_, Wanted>> {
        let wanted = self.wanted.lock();
        parking_lot::MutexGuard::try_map(wanted, |wanted| {
            let w = wanted.iter_mut().find(|w| w.request == *request)?;
            let at = w.out.iter().position(|o| o.peer == peer)?;
            w.out.swap_remove(at);
            Some(w)
        })
        .ok()
    }
}

impl PeerHandler {
    /// Sends the peer, while it speaks v2, the requests for the layers it
    /// can answer, as runs become free (libtorrent
    /// `bt_peer_connection::maybe_send_hash_request`). Never ends while the
    /// peer is live.
    pub(super) async fn fetch_layers(&self) -> crate::Result<()> {
        self.wait_for_bitfield().await;
        if !self.speaks_v2.load(Ordering::Relaxed) {
            return std::future::pending().await;
        }
        let fetch = &self.state.layer_fetch;
        loop {
            let changed = fetch.changed.notified();
            let pieces_changed = self.on_bitfield_notify.notified();
            let Some(pick) = self.state.peers.with_live(self.addr, |live| {
                fetch.pick(
                    self.addr,
                    |p| live.bitfield.get(p as usize).is_some_and(|b| *b),
                    live.request_timeout(),
                    Instant::now(),
                )
            }) else {
                return Ok(());
            };
            match pick {
                Pick::Send(requests) => {
                    for request in requests {
                        let msg = WriterRequest::Message(Message::HashRequest(request));
                        if self.tx.send(msg).is_err() {
                            return Ok(());
                        }
                    }
                }
                Pick::Wait(at) => {
                    let timed_out = async {
                        match at {
                            Some(at) => tokio::time::sleep_until(at).await,
                            None => std::future::pending().await,
                        }
                    };
                    tokio::select! {
                        _ = changed => {}
                        _ = pieces_changed => {}
                        _ = timed_out => {}
                    }
                }
                Pick::Done => return std::future::pending().await,
            }
        }
    }

    /// The peer's answer to a request for a run of a piece layer: proven,
    /// the run is held and its pieces may be fetched; not proven, the peer
    /// is dropped (libtorrent `bt_peer_connection::on_hashes`,
    /// `torrent::add_hashes`).
    pub(super) fn on_hashes(&self, hashes: Hashes<ByteBuf<'_>>) -> anyhow::Result<()> {
        let fetch = &self.state.layer_fetch;
        if !fetch.answered(self.addr, &hashes.request) {
            return Ok(());
        }
        self.state
            .metadata
            .add_layer_chunk(&hashes.request, &hashes.hashes())
            .context("invalid hashes")?;
        fetch.landed(&hashes.request);
        self.state.new_pieces_notify.notify_waiters();
        Ok(())
    }

    /// A peer's request for hashes: answered from the file's tree, or
    /// rejected when the request is malformed or asks below the piece
    /// layer or for a file whose layer is not held (libtorrent
    /// `bt_peer_connection::on_hash_request`). A v1 torrent has no hashes
    /// to give, and the peer is dropped.
    pub(super) fn on_hash_request(&self, request: HashRequest) -> anyhow::Result<()> {
        let v2 = self
            .state
            .metadata
            .v2
            .as_ref()
            .context("a hash request for a v1 torrent")?;
        let reply = match v2.answer(&request) {
            Some(hashes) => WriterRequest::Hashes(Hashes::new(request, &hashes)),
            None => WriterRequest::Message(Message::HashReject(request)),
        };
        // A closed writer means the peer is gone.
        let _ = self.tx.send(reply);
        Ok(())
    }

    /// The peer rejected a request (libtorrent
    /// `bt_peer_connection::on_hash_reject`).
    pub(super) fn on_hash_reject(&self, request: HashRequest) {
        self.state.layer_fetch.rejected(self.addr, &request);
    }
}
