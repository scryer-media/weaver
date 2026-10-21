// SPDX-License-Identifier: GPL-3.0-or-later

//! Fetching the piece layers a v2 torrent lacks from the peers that speak
//! v2, one run of [`LAYER_CHUNK`] pieces of a file at a time (libtorrent
//! `hash_picker`). No piece is requested before the run of the layer it
//! lies in is held ([`TorrentMetadata::can_verify`]). The block hashes of
//! a piece that failed its v2 check are fetched the same way, ahead of the
//! layers, to learn which of its blocks were bad (libtorrent
//! `hash_picker::verify_block_hashes`).
//!
//! [`TorrentMetadata::can_verify`]: crate::torrent_state::TorrentMetadata::can_verify
//! [`LAYER_CHUNK`]: crate::v2_hashes::LAYER_CHUNK

use std::{net::SocketAddr, ops::Range, sync::atomic::Ordering, time::Duration};

use anyhow::Context;
use parking_lot::Mutex;
use tokio::{sync::Notify, time::Instant};

use super::{PeerHandler, smart_ban::ReadBlock};
use crate::{
    buffers::ByteBuf,
    core::{
        Id32,
        lengths::{Lengths, ValidPieceIndex},
        merkle::{self, BLOCK_SIZE},
    },
    peer_connection::WriterRequest,
    peer_protocol::{HashRequest, Hashes, MAX_HASHES_IN_REPLY, Message},
    piece_tracker::Senders,
    type_aliases::PeerHandle,
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

/// A block of a piece that failed its v2 check: who sent it, and the hash
/// of its bytes as they failed, its leaf in its file's tree had they been
/// good.
#[derive(Debug, Clone, Copy)]
pub(crate) struct FailedBlock {
    block: u32,
    sender: PeerHandle,
    leaf: Id32,
}

/// What a want asks for.
enum Kind {
    /// A run of a file's piece layer.
    Layer,
    /// The block hashes of a piece that failed its v2 check, and its blocks
    /// as they failed, of every failure since.
    Blocks {
        piece: ValidPieceIndex,
        failed: Vec<FailedBlock>,
    },
}

/// Hashes not yet held: a run of a file's piece layer, or the block
/// hashes of a piece.
struct Wanted {
    request: HashRequest,
    /// The pieces a peer must have one of to be asked.
    pieces: Range<u32>,
    kind: Kind,
    /// The peers asked for it whose answer is still due.
    out: Vec<Out>,
    /// The peers that rejected it, which are not asked for it again.
    rejected_by: Vec<SocketAddr>,
}

/// What a peer is to do about the hashes.
pub(crate) enum Pick {
    /// Send it these requests.
    Send(Vec<HashRequest>),
    /// Ask again when a request is answered or rejected, hashes are wanted,
    /// the peer's pieces change, or at the instant given, when a request
    /// out to another peer times out.
    Wait(Option<Instant>),
}

/// What an answered request asked for.
enum Asked {
    Layer,
    Blocks(ValidPieceIndex),
}

/// The request for the block hashes of `piece`, which failed its v2 check:
/// the leaves under its node of its file's tree, proven to that node
/// alone, which is held (libtorrent `hash_picker::pick_hashes` for a
/// `piece_block_request`, with `layers_to_verify`). A file no larger than
/// a piece spans as many leaves as its tree has. `None` when the piece's
/// node is not held, the piece is one block, which is its own leaf, or the
/// answer would carry more hashes than a message may.
pub(crate) fn block_request(v2: &V2Hashes, piece_length: u64, piece: u32) -> Option<HashRequest> {
    v2.expected(piece)?;
    let f = v2.file_of(piece)?;
    let blocks_per_piece = piece_length / BLOCK_SIZE;
    let width = if f.length < piece_length {
        merkle::num_leafs(f.length.div_ceil(BLOCK_SIZE))
    } else {
        blocks_per_piece
    };
    if width > u64::from(MAX_HASHES_IN_REPLY) {
        return None;
    }
    Some(HashRequest {
        root: f.root,
        base: 0,
        index: u32::try_from(u64::from(piece - f.first_piece) * blocks_per_piece).ok()?,
        count: width as u32,
        proof_layers: merkle::num_layers(width).checked_sub(1)?,
    })
}

/// The blocks of `piece` with a known sender, each with the hash of its
/// bytes in the piece's v2 `share`, as `read` reads them back; a block
/// past the share is padding and has no leaf (libtorrent `disk_io_thread`
/// `hash` job hashes the blocks of the file alone).
pub(crate) fn failed_blocks(
    lengths: &Lengths,
    piece: ValidPieceIndex,
    share: u64,
    senders: &Senders,
    mut read: impl ReadBlock,
) -> Vec<FailedBlock> {
    let mut buf = Vec::new();
    lengths
        .iter_chunk_infos(piece)
        .zip(senders.blocks())
        .filter_map(|(chunk, sender)| {
            let sender = (*sender)?;
            let len = share
                .checked_sub(u64::from(chunk.offset))?
                .min(u64::from(chunk.size));
            if len == 0 {
                return None;
            }
            buf.resize(chunk.size as usize, 0);
            // A block that cannot be read blames nobody.
            read(sender, &chunk, &mut buf).ok()?;
            Some(FailedBlock {
                block: chunk.chunk_index,
                sender,
                leaf: merkle::hash_block(&buf[..len as usize]),
            })
        })
        .collect()
}

/// The hashes a torrent lacks, and which peers they are out to (libtorrent
/// `hash_picker::m_piece_hash_requested`, `m_piece_block_requests`).
pub(crate) struct LayerFetch {
    wanted: Mutex<Vec<Wanted>>,
    /// Rung when hashes are wanted or held, a request is answered or
    /// rejected, or a peer leaves.
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
                    kind: Kind::Layer,
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
    /// given `timeout` to answer: the wants it has a piece of that are out
    /// to no one, or only to peers past their timeout, block hashes first,
    /// up to [`REQUESTS_PER_PEER`] held at once (libtorrent
    /// `hash_picker::pick_hashes`).
    pub fn pick(
        &self,
        peer: SocketAddr,
        has: impl Fn(u32) -> bool,
        timeout: Duration,
        now: Instant,
    ) -> Pick {
        let mut wanted = self.wanted.lock();
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

    /// `peer` answered `request`: what it asked for, when it was out to it
    /// (libtorrent `bt_peer_connection::on_hashes` ignores the others).
    fn answered(&self, peer: SocketAddr, request: &HashRequest) -> Option<Asked> {
        let asked = self.take_out(peer, request).map(|w| match w.kind {
            Kind::Layer => Asked::Layer,
            Kind::Blocks { piece, .. } => Asked::Blocks(piece),
        });
        self.changed.notify_waiters();
        asked
    }

    /// The hashes `request` asks for are held: the blocks they were wanted
    /// to judge, if any.
    fn landed(&self, request: &HashRequest) -> Vec<FailedBlock> {
        let mut wanted = self.wanted.lock();
        let failed = match wanted.iter().position(|w| w.request == *request) {
            Some(at) => match wanted.remove(at).kind {
                Kind::Blocks { failed, .. } => failed,
                Kind::Layer => Vec::new(),
            },
            None => Vec::new(),
        };
        drop(wanted);
        self.changed.notify_waiters();
        failed
    }

    /// `piece` failed its v2 check with `failed` its blocks: its block
    /// hashes, which `request` asks for, are wanted ahead of the layers,
    /// from the peers that have it, once however often it fails
    /// (libtorrent `hash_picker::verify_block_hashes`).
    pub fn verify_blocks(
        &self,
        request: HashRequest,
        piece: ValidPieceIndex,
        failed: Vec<FailedBlock>,
    ) {
        let mut wanted = self.wanted.lock();
        if let Some(Kind::Blocks { failed: had, .. }) = wanted
            .iter_mut()
            .find(|w| w.request == request)
            .map(|w| &mut w.kind)
        {
            had.extend(failed);
        } else {
            wanted.insert(
                0,
                Wanted {
                    request,
                    pieces: piece.get()..piece.get() + 1,
                    kind: Kind::Blocks { piece, failed },
                    out: Vec::new(),
                    rejected_by: Vec::new(),
                },
            );
        }
        drop(wanted);
        self.changed.notify_waiters();
    }

    /// `piece` passed: the blocks it failed with are judged by the good
    /// copy, and its block hashes are no longer wanted.
    pub fn piece_passed(&self, piece: ValidPieceIndex) {
        self.wanted
            .lock()
            .retain(|w| !matches!(w.kind, Kind::Blocks { piece: p, .. } if p == piece));
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
    /// Sends the peer, while it speaks v2, the requests for the hashes it
    /// can answer, as they become wanted or free (libtorrent
    /// `bt_peer_connection::maybe_send_hash_request`). Never ends while the
    /// peer is live: a piece may fail, and want its block hashes, after
    /// every layer is held.
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
            }
        }
    }

    /// The peer's answer to a request we sent it, unless they prove
    /// nothing, when the peer is dropped (libtorrent
    /// `bt_peer_connection::on_hashes`, `torrent::add_hashes`): a run of a
    /// piece layer is held and its pieces may be fetched; the block hashes
    /// of a failed piece name the senders of the blocks that differ from
    /// them, which are penalised (libtorrent `hash_picker::add_hashes`
    /// reports the failed blocks, and `torrent::add_hashes` hands them to
    /// `torrent::piece_failed`).
    pub(super) fn on_hashes(&self, hashes: Hashes<ByteBuf<'_>>) -> anyhow::Result<()> {
        let fetch = &self.state.layer_fetch;
        let request = &hashes.request;
        match fetch.answered(self.addr, request) {
            None => {}
            Some(Asked::Layer) => {
                self.state
                    .metadata
                    .add_layer_chunk(request, &hashes.hashes())
                    .context("invalid hashes")?;
                fetch.landed(request);
                self.state.new_pieces_notify.notify_waiters();
            }
            Some(Asked::Blocks(piece)) => {
                let hashes = hashes.hashes();
                let (leaves, uncles) = hashes
                    .split_at_checked(request.count as usize)
                    .context("invalid hashes")?;
                let node = self
                    .state
                    .metadata
                    .v2
                    .as_ref()
                    .and_then(|v2| v2.expected(piece.get()));
                anyhow::ensure!(
                    Some(merkle::proven_root(
                        leaves,
                        request.index.into(),
                        Id32::default(),
                        uncles
                    )) == node,
                    "invalid hashes"
                );
                let bad = fetch
                    .landed(request)
                    .into_iter()
                    .filter(|b| leaves.get(b.block as usize) != Some(&b.leaf))
                    .map(|b| b.sender)
                    .collect::<std::collections::BTreeSet<_>>();
                self.state.penalize_peers(bad, piece, true);
            }
        }
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
