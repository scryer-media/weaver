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

use std::{
    collections::{BTreeMap, BTreeSet, HashMap, HashSet},
    ops::Range,
    sync::{Arc, atomic::Ordering},
    time::Duration,
};

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
    peer_connection::{Answer, WriterRequest},
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
    peer: PeerHandle,
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
    /// The pieces a peer must have one of to be asked: a run of a layer
    /// covers pieces in every file of its root.
    pieces: Vec<Range<u32>>,
    kind: Kind,
    /// The peers asked for it whose answer is still due.
    out: Vec<Out>,
    /// The peers that rejected it, which are not asked for it again.
    rejected_by: Vec<PeerHandle>,
}

impl Wanted {
    // The instant the want may go to another peer: the latest deadline of
    // the peers it is out to; none while it is out to no one.
    fn due(&self) -> Option<Instant> {
        self.out.iter().map(|o| o.deadline).max()
    }
}

/// Which waiting connections a change may give new work to.
enum Wake {
    /// None: no want came free, and no connection's held requests fell.
    Nobody,
    /// These, whose held requests fell, and none other.
    Peers(Vec<PeerHandle>),
    /// Every connection: a want came free or may go to another sooner, or a
    /// new one is wanted.
    All,
}

impl Wake {
    fn and(self, other: Wake) -> Wake {
        match (self, other) {
            (Wake::All, _) | (_, Wake::All) => Wake::All,
            (Wake::Nobody, w) | (w, Wake::Nobody) => w,
            (Wake::Peers(mut a), Wake::Peers(b)) => {
                a.extend(b);
                Wake::Peers(a)
            }
        }
    }

    // `peer` is no longer out on a want that went from due at `was` to due
    // at `now`: its own held requests fell, and the want came free or may
    // go to another sooner when it is due earlier than it was, or not at
    // all.
    fn taken_off(peer: PeerHandle, was: Option<Instant>, now: Option<Instant>) -> Wake {
        match (was, now) {
            (Some(was), Some(now)) if now >= was => Wake::Peers(vec![peer]),
            _ => Wake::All,
        }
    }
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
        count: u32::try_from(width).ok()?,
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
                leaf: merkle::hash_block(&buf[..usize::try_from(len).ok()?]),
            })
        })
        .collect()
}

/// A want's place in [`Wants::by_order`].
type Key = i64;

/// What a request is found by: its fields, in order.
type RequestKey = (Id32, u32, u32, u32, u32);

fn request_key(r: &HashRequest) -> RequestKey {
    (r.root, r.base, r.index, r.count, r.proof_layers)
}

/// The hashes wanted, in the order they are picked, and found by their
/// request, by the peers they are out to or rejected by, and by the piece
/// whose block hashes they are, so no change looks at every want.
#[derive(Default)]
struct Wants {
    /// In the order they are picked: the block hashes of failed pieces,
    /// the latest first, then the runs of the layers in order.
    by_order: BTreeMap<Key, Wanted>,
    /// Each want by its request, the first of a request first.
    by_request: BTreeSet<(RequestKey, Key)>,
    /// The wants each peer is asked for and has not answered.
    out_of: HashMap<PeerHandle, HashSet<Key>>,
    /// The wants each peer rejected.
    rejected_of: HashMap<PeerHandle, HashSet<Key>>,
    /// The wants for the block hashes of each piece.
    blocks_of: BTreeSet<(u32, Key)>,
    /// The key the next want put first takes, less one.
    front: Key,
    /// The key the next want put last takes.
    back: Key,
}

impl Wants {
    fn new(wanted: impl IntoIterator<Item = Wanted>) -> Self {
        let mut wants = Self::default();
        for w in wanted {
            wants.insert(w, false);
        }
        wants
    }

    // Puts `w`, which no peer is asked for or rejected, first or last.
    fn insert(&mut self, w: Wanted, first: bool) {
        let key = if first {
            self.front -= 1;
            self.front
        } else {
            self.back += 1;
            self.back - 1
        };
        self.by_request.insert((request_key(&w.request), key));
        if let Kind::Blocks { piece, .. } = w.kind {
            self.blocks_of.insert((piece.get(), key));
        }
        self.by_order.insert(key, w);
    }

    // The first want of `request`.
    fn first(&self, request: &HashRequest) -> Option<Key> {
        let r = request_key(request);
        self.by_request
            .range((r, Key::MIN)..=(r, Key::MAX))
            .next()
            .map(|&(_, key)| key)
    }

    // Takes the want of `key` out, and the peers it was out to.
    fn remove(&mut self, key: Key) -> Option<Wanted> {
        let w = self.by_order.remove(&key)?;
        self.by_request.remove(&(request_key(&w.request), key));
        if let Kind::Blocks { piece, .. } = w.kind {
            self.blocks_of.remove(&(piece.get(), key));
        }
        for o in &w.out {
            unmark(&mut self.out_of, o.peer, key);
        }
        for &p in &w.rejected_by {
            unmark(&mut self.rejected_of, p, key);
        }
        Some(w)
    }

    // How many wants `peer` is asked for.
    fn held(&self, peer: PeerHandle) -> usize {
        self.out_of.get(&peer).map_or(0, HashSet::len)
    }
}

// `key` is no longer one of `peer`'s in `index`.
fn unmark(index: &mut HashMap<PeerHandle, HashSet<Key>>, peer: PeerHandle, key: Key) {
    if let Some(keys) = index.get_mut(&peer) {
        keys.remove(&key);
        if keys.is_empty() {
            index.remove(&peer);
        }
    }
}

/// The hashes a torrent lacks, and which peers they are out to (libtorrent
/// `hash_picker::m_piece_hash_requested`, `m_piece_block_requests`).
pub(crate) struct LayerFetch {
    wanted: Mutex<Wants>,
    /// Each v2 connection's bell, rung when a change may give it new work:
    /// a want came free, or may go to another sooner, or is new, rung for
    /// every connection; a request of its own answered, rejected or
    /// dropped, for it alone. A connection no change can give work is left
    /// asleep. Taken after `wanted` is let go.
    waiters: Mutex<HashMap<PeerHandle, Arc<Notify>>>,
}

impl LayerFetch {
    pub fn new(v2: Option<&V2Hashes>) -> Self {
        let wanted = v2.into_iter().flat_map(|v2| {
            v2.missing_chunks().map(|(file, chunk)| Wanted {
                request: v2.chunk_request(file, chunk),
                pieces: v2.root_chunk_pieces(file, chunk),
                kind: Kind::Layer,
                out: Vec::new(),
                rejected_by: Vec::new(),
            })
        });
        Self {
            wanted: Mutex::new(Wants::new(wanted)),
            waiters: Mutex::default(),
        }
    }

    /// The bell of `peer`'s connection; see [`Self::waiters`]. A ring that
    /// comes before the connection waits is kept for it.
    pub(super) fn waiter(&self, peer: PeerHandle) -> Arc<Notify> {
        self.waiters.lock().entry(peer).or_default().clone()
    }

    fn wake(&self, wake: Wake) {
        let waiters = self.waiters.lock();
        match wake {
            Wake::Nobody => {}
            Wake::Peers(peers) => {
                for peer in peers {
                    if let Some(waiter) = waiters.get(&peer) {
                        waiter.notify_one();
                    }
                }
            }
            Wake::All => {
                for waiter in waiters.values() {
                    waiter.notify_one();
                }
            }
        }
    }

    /// The requests to send `peer`, which has the pieces `has` says and is
    /// given `timeout` to answer: the wants it has a piece of that are out
    /// to no one, or only to peers past their timeout, block hashes first,
    /// up to [`REQUESTS_PER_PEER`] held at once (libtorrent
    /// `hash_picker::pick_hashes`).
    pub fn pick(
        &self,
        peer: PeerHandle,
        has_any: impl Fn(Range<u32>) -> bool,
        timeout: Duration,
        now: Instant,
    ) -> Pick {
        let mut guard = self.wanted.lock();
        let wants = &mut *guard;
        let mut held = wants.held(peer);
        let mut send = Vec::new();
        let mut wake: Option<Instant> = None;
        for (&key, w) in wants.by_order.iter_mut() {
            if held >= REQUESTS_PER_PEER {
                break;
            }
            if w.out.iter().any(|o| o.peer == peer) || w.rejected_by.contains(&peer) {
                continue;
            }
            if let Some(due) = w.due()
                && due > now
            {
                wake = Some(wake.map_or(due, |at| at.min(due)));
                continue;
            }
            if !w.pieces.iter().any(|pieces| has_any(pieces.clone())) {
                continue;
            }
            w.out.push(Out {
                peer,
                deadline: now + timeout,
            });
            wants.out_of.entry(peer).or_default().insert(key);
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
    fn answered(&self, peer: PeerHandle, request: &HashRequest) -> Option<Asked> {
        let mut wants = self.wanted.lock();
        let (asked, wake) = match take_out(&mut wants, peer, request) {
            Some((key, wake)) => (
                wants.by_order.get(&key).map(|w| match w.kind {
                    Kind::Layer => Asked::Layer,
                    Kind::Blocks { piece, .. } => Asked::Blocks(piece),
                }),
                wake,
            ),
            None => (None, Wake::Nobody),
        };
        drop(wants);
        self.wake(wake);
        asked
    }

    /// The hashes `request` asks for are held: the blocks they were wanted
    /// to judge, if any.
    fn landed(&self, request: &HashRequest) -> Vec<FailedBlock> {
        let mut wants = self.wanted.lock();
        // The peers it is still out to hold one request fewer.
        let (failed, wake) = match wants.first(request).and_then(|key| wants.remove(key)) {
            Some(w) => {
                let wake = Wake::Peers(w.out.iter().map(|o| o.peer).collect());
                match w.kind {
                    Kind::Blocks { failed, .. } => (failed, wake),
                    Kind::Layer => (Vec::new(), wake),
                }
            }
            None => (Vec::new(), Wake::Nobody),
        };
        drop(wants);
        self.wake(wake);
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
        let mut wants = self.wanted.lock();
        // More failed blocks to judge change no pick: only a new want wakes.
        let had = wants
            .first(&request)
            .and_then(|key| wants.by_order.get_mut(&key));
        let wake = if let Some(Kind::Blocks { failed: had, .. }) = had.map(|w| &mut w.kind) {
            had.extend(failed);
            Wake::Nobody
        } else {
            wants.insert(
                Wanted {
                    request,
                    pieces: std::iter::once(piece.get()..piece.get() + 1).collect(),
                    kind: Kind::Blocks { piece, failed },
                    out: Vec::new(),
                    rejected_by: Vec::new(),
                },
                true,
            );
            Wake::All
        };
        drop(wants);
        self.wake(wake);
    }

    /// `piece` passed: the blocks it failed with are judged by the good
    /// copy, and its block hashes are no longer wanted. The peers they
    /// were out to hold one request fewer each, and may be given another.
    pub fn piece_passed(&self, piece: ValidPieceIndex) {
        let mut wants = self.wanted.lock();
        let p = piece.get();
        let keys: Vec<Key> = wants
            .blocks_of
            .range((p, Key::MIN)..=(p, Key::MAX))
            .map(|&(_, key)| key)
            .collect();
        let mut wake = Wake::Nobody;
        for key in keys {
            if let Some(w) = wants.remove(key)
                && !w.out.is_empty()
            {
                wake = wake.and(Wake::Peers(w.out.iter().map(|o| o.peer).collect()));
            }
        }
        drop(wants);
        self.wake(wake);
    }

    /// `peer` rejected `request`: the run may go to another peer at once
    /// (libtorrent `hash_picker::hashes_rejected`), and is not asked of it
    /// again.
    fn rejected(&self, peer: PeerHandle, request: &HashRequest) {
        let mut guard = self.wanted.lock();
        let wants = &mut *guard;
        let wake = match take_out(wants, peer, request) {
            Some((key, wake)) => {
                if let Some(w) = wants.by_order.get_mut(&key) {
                    w.rejected_by.push(peer);
                    wants.rejected_of.entry(peer).or_default().insert(key);
                }
                wake
            }
            None => Wake::Nobody,
        };
        drop(guard);
        self.wake(wake);
    }

    /// `peer` left: what it held may go to another, and its bell goes.
    pub fn forget(&self, peer: PeerHandle) {
        let mut wake = Wake::Nobody;
        {
            let mut guard = self.wanted.lock();
            let wants = &mut *guard;
            for key in wants.out_of.remove(&peer).unwrap_or_default() {
                let Some(w) = wants.by_order.get_mut(&key) else {
                    continue;
                };
                let was = w.due();
                w.out.retain(|o| o.peer != peer);
                wake = wake.and(Wake::taken_off(peer, was, w.due()));
            }
            for key in wants.rejected_of.remove(&peer).unwrap_or_default() {
                if let Some(w) = wants.by_order.get_mut(&key) {
                    w.rejected_by.retain(|p| *p != peer);
                }
            }
        }
        self.waiters.lock().remove(&peer);
        self.wake(wake);
    }
}

/// The want `request` asks for, its first, taken off `peer`'s requests when
/// it was out to it, and whom that may give work.
fn take_out(wants: &mut Wants, peer: PeerHandle, request: &HashRequest) -> Option<(Key, Wake)> {
    let key = wants.first(request)?;
    let w = wants.by_order.get_mut(&key)?;
    let at = w.out.iter().position(|o| o.peer == peer)?;
    let was = w.due();
    w.out.swap_remove(at);
    let wake = Wake::taken_off(peer, was, w.due());
    unmark(&mut wants.out_of, peer, key);
    Some((key, wake))
}

// Whether `bitfield` has any of `pieces`, a word at a time; a piece past
// its end is one the peer lacks.
fn has_any(bitfield: &crate::type_aliases::BF, pieces: Range<u32>) -> bool {
    let len = bitfield.len();
    let start = (pieces.start as usize).min(len);
    let end = (pieces.end as usize).min(len);
    start < end && bitfield[start..end].any()
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
        let waiter = fetch.waiter(self.addr);
        loop {
            let changed = waiter.notified();
            let pieces_changed = self.on_bitfield_notify.notified();
            let Some(pick) = self.state.peers.with_live(self.addr, |live| {
                fetch.pick(
                    self.addr,
                    |pieces| has_any(&live.bitfield, pieces),
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
            Some(hashes) => Answer::Hashes(Hashes::new(request, &hashes)),
            None => Answer::HashReject(request),
        };
        // A closed writer means the peer is gone.
        let _ = self.answer(reply);
        Ok(())
    }

    /// The peer rejected a request (libtorrent
    /// `bt_peer_connection::on_hash_reject`).
    pub(super) fn on_hash_reject(&self, request: HashRequest) {
        self.state.layer_fetch.rejected(self.addr, &request);
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, net::SocketAddr};

    use futures::FutureExt;

    use super::*;

    // One side of the comparison: a fetch, and what each connection sent
    // and when its timer rings.
    struct Side {
        fetch: LayerFetch,
        sent: HashMap<SocketAddr, Vec<HashRequest>>,
        wake_at: HashMap<SocketAddr, Option<Instant>>,
    }

    impl Side {
        fn new(wants: &[(HashRequest, Range<u32>)]) -> Self {
            let wanted = wants.iter().map(|(request, pieces)| Wanted {
                request: *request,
                pieces: vec![pieces.clone()],
                kind: Kind::Layer,
                out: Vec::new(),
                rejected_by: Vec::new(),
            });
            Self {
                fetch: LayerFetch {
                    wanted: Mutex::new(Wants::new(wanted)),
                    waiters: Mutex::default(),
                },
                sent: HashMap::new(),
                wake_at: HashMap::new(),
            }
        }

        // The connection's loop until it waits, as `fetch_layers` runs it.
        fn run(&mut self, peer: SocketAddr, has: &[bool], timeout: Duration, now: Instant) {
            loop {
                let pick = self.fetch.pick(
                    peer.into(),
                    |pieces| pieces.into_iter().any(|p| has[p as usize]),
                    timeout,
                    now,
                );
                match pick {
                    Pick::Send(requests) => self.sent.entry(peer).or_default().extend(requests),
                    Pick::Wait(at) => {
                        self.wake_at.insert(peer, at);
                        return;
                    }
                }
            }
        }

        fn timer_due(&self, peer: SocketAddr, now: Instant) -> bool {
            self.wake_at
                .get(&peer)
                .is_some_and(|at| at.is_some_and(|at| at <= now))
        }
    }

    // Files of the same content share a root and one layer: a peer that
    // has pieces of any of them is asked for its runs, not only one with
    // pieces of the first.
    #[test]
    fn a_peer_with_pieces_of_any_file_of_a_root_is_asked_for_its_layer() {
        use crate::core::{
            torrent_metainfo::torrent_from_bytes,
            v2_test_torrent::{TestTorrent, payload},
        };
        let data = payload(3 * 16384, 1);
        let mut t = TestTorrent::new(vec![("a", data.clone()), ("b", data)], 16384);
        t.layers = false;
        let bytes = t.torrent_bytes();
        let meta = torrent_from_bytes(&bytes).unwrap();
        let info = meta.info.data.clone().validate().unwrap();
        let v2 = V2Hashes::new(&info, Vec::new()).unwrap();
        let fetch = LayerFetch::new(Some(&v2));
        let peer = PeerHandle::from(SocketAddr::from(([10, 0, 0, 1], 6881)));
        // The peer has b's pieces, 3..6, and none of a's.
        let pick = fetch.pick(
            peer,
            |pieces| pieces.start.max(3) < pieces.end.min(6),
            Duration::from_secs(5),
            Instant::now(),
        );
        let Pick::Send(sent) = pick else {
            panic!("the peer with b's pieces was not asked");
        };
        assert_eq!(sent, vec![v2.chunk_request(0, 0)]);
        // A peer with no piece of the root is not.
        let other = PeerHandle::from(SocketAddr::from(([10, 0, 0, 2], 6881)));
        assert!(matches!(
            fetch.pick(other, |_| false, Duration::from_secs(5), Instant::now()),
            Pick::Wait(_)
        ));
    }

    // A piece passes while the request for its block hashes is out: the
    // peer asked for them holds one request fewer, and its connection is
    // woken to be given another.
    #[test]
    fn a_passed_piece_wakes_the_peers_asked_for_its_block_hashes() {
        let lengths = Lengths::new(64, 1).unwrap();
        let request = |index| HashRequest {
            root: Id32::default(),
            base: 0,
            index,
            count: 1,
            proof_layers: 0,
        };
        let fetch = LayerFetch::new(None);
        let (peer, other): (PeerHandle, PeerHandle) = (
            SocketAddr::from(([10, 0, 0, 1], 6881)).into(),
            SocketAddr::from(([10, 0, 0, 2], 6881)).into(),
        );
        let (bell, other_bell) = (fetch.waiter(peer), fetch.waiter(other));
        let piece = lengths.validate_piece_index(3).unwrap();
        fetch.verify_blocks(request(1), piece, Vec::new());
        // The new want rang every connection.
        for b in [&bell, &other_bell] {
            assert!(b.notified().now_or_never().is_some());
        }
        let now = Instant::now();
        let Pick::Send(sent) = fetch.pick(peer, |_| true, Duration::from_secs(5), now) else {
            panic!("the block hashes were not asked for");
        };
        assert_eq!(sent, vec![request(1)]);
        fetch.piece_passed(piece);
        assert!(bell.notified().now_or_never().is_some());
        // The other peer was asked for nothing, and is left asleep.
        assert!(other_bell.notified().now_or_never().is_none());
        assert_eq!(fetch.wanted.lock().held(peer), 0);
        assert!(fetch.wanted.lock().by_order.is_empty());
    }

    /// The fetch as it was before it was indexed: a list searched for each
    /// change. The reference the indexed fetch is compared with.
    struct Listed(Vec<Wanted>);

    impl Listed {
        fn pick(
            &mut self,
            peer: PeerHandle,
            has_any: impl Fn(Range<u32>) -> bool,
            timeout: Duration,
            now: Instant,
        ) -> Pick {
            let mut held = self
                .0
                .iter()
                .filter(|w| w.out.iter().any(|o| o.peer == peer))
                .count();
            let mut send = Vec::new();
            let mut wake: Option<Instant> = None;
            for w in self.0.iter_mut() {
                if held >= REQUESTS_PER_PEER {
                    break;
                }
                if w.out.iter().any(|o| o.peer == peer) || w.rejected_by.contains(&peer) {
                    continue;
                }
                if let Some(due) = w.due()
                    && due > now
                {
                    wake = Some(wake.map_or(due, |at| at.min(due)));
                    continue;
                }
                if !w.pieces.iter().any(|pieces| has_any(pieces.clone())) {
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

        fn take_out(&mut self, peer: PeerHandle, request: &HashRequest) -> Option<&mut Wanted> {
            let w = self.0.iter_mut().find(|w| w.request == *request)?;
            let at = w.out.iter().position(|o| o.peer == peer)?;
            w.out.swap_remove(at);
            Some(w)
        }

        fn answered(&mut self, peer: PeerHandle, request: &HashRequest) -> Option<Asked> {
            self.take_out(peer, request).map(|w| match w.kind {
                Kind::Layer => Asked::Layer,
                Kind::Blocks { piece, .. } => Asked::Blocks(piece),
            })
        }

        fn landed(&mut self, request: &HashRequest) -> Vec<FailedBlock> {
            match self.0.iter().position(|w| w.request == *request) {
                Some(at) => match self.0.remove(at).kind {
                    Kind::Blocks { failed, .. } => failed,
                    Kind::Layer => Vec::new(),
                },
                None => Vec::new(),
            }
        }

        fn verify_blocks(
            &mut self,
            request: HashRequest,
            piece: ValidPieceIndex,
            failed: Vec<FailedBlock>,
        ) {
            if let Some(Kind::Blocks { failed: had, .. }) = self
                .0
                .iter_mut()
                .find(|w| w.request == request)
                .map(|w| &mut w.kind)
            {
                had.extend(failed);
            } else {
                self.0.insert(
                    0,
                    Wanted {
                        request,
                        pieces: std::iter::once(piece.get()..piece.get() + 1).collect(),
                        kind: Kind::Blocks { piece, failed },
                        out: Vec::new(),
                        rejected_by: Vec::new(),
                    },
                );
            }
        }

        fn piece_passed(&mut self, piece: ValidPieceIndex) {
            self.0
                .retain(|w| !matches!(w.kind, Kind::Blocks { piece: p, .. } if p == piece));
        }

        fn rejected(&mut self, peer: PeerHandle, request: &HashRequest) {
            if let Some(w) = self.take_out(peer, request) {
                w.rejected_by.push(peer);
            }
        }

        fn forget(&mut self, peer: PeerHandle) {
            for w in self.0.iter_mut() {
                w.out.retain(|o| o.peer != peer);
                w.rejected_by.retain(|p| *p != peer);
            }
        }
    }

    // What a pick says, to compare.
    fn picked(pick: Pick) -> (Vec<HashRequest>, Option<Option<Instant>>) {
        match pick {
            Pick::Send(sent) => (sent, None),
            Pick::Wait(at) => (Vec::new(), Some(at)),
        }
    }

    fn asked(asked: Option<Asked>) -> Option<Option<u32>> {
        asked.map(|a| match a {
            Asked::Layer => None,
            Asked::Blocks(piece) => Some(piece.get()),
        })
    }

    fn blocks(failed: Vec<FailedBlock>) -> Vec<(u32, PeerHandle, Id32)> {
        failed
            .into_iter()
            .map(|b| (b.block, b.sender, b.leaf))
            .collect()
    }

    // The indexed fetch picks what the listed one picks, for each peer and
    // in the same order, holds the same requests, and answers each change
    // as it does, over seeded runs of picks, answers, rejects, block hash
    // wants (some for a request a layer run has too), passed pieces,
    // peers leaving and joining, and time passing.
    #[test]
    fn the_indexed_fetch_does_what_the_listed_one_did() {
        const PIECES: u32 = 64;
        let mut rng = 0x1f83_d9ab_fb41_bd6bu64;
        let mut next = move || {
            rng ^= rng << 13;
            rng ^= rng >> 7;
            rng ^= rng << 17;
            rng
        };
        let lengths = Lengths::new(u64::from(PIECES), 1).unwrap();
        let request = |index: u32| HashRequest {
            root: Id32::default(),
            base: 0,
            index,
            count: 1,
            proof_layers: 0,
        };
        for run in 0..40 {
            let wants: Vec<Wanted> = (0..30)
                .map(|i| {
                    let start = u32::try_from(next() % u64::from(PIECES)).unwrap();
                    let end = (start + 1 + u32::try_from(next() % 16).unwrap()).min(PIECES);
                    let more = u32::try_from(next() % u64::from(PIECES)).unwrap();
                    Wanted {
                        request: request(i),
                        pieces: vec![start..end, more..more + 1],
                        kind: Kind::Layer,
                        out: Vec::new(),
                        rejected_by: Vec::new(),
                    }
                })
                .collect();
            let copy = |w: &Wanted| Wanted {
                request: w.request,
                pieces: w.pieces.clone(),
                kind: Kind::Layer,
                out: Vec::new(),
                rejected_by: Vec::new(),
            };
            let mut listed = Listed(wants.iter().map(copy).collect());
            let indexed = LayerFetch {
                wanted: Mutex::new(Wants::new(wants.iter().map(copy))),
                waiters: Mutex::default(),
            };
            let peers: Vec<(PeerHandle, Vec<bool>, Duration)> = (0..6u16)
                .map(|i| {
                    let has = (0..PIECES).map(|_| next() % 3 == 0).collect();
                    let timeout = Duration::from_millis(1000 + next() % 4000);
                    (
                        SocketAddr::from(([10, 0, 0, 1], 6881 + i)).into(),
                        has,
                        timeout,
                    )
                })
                .collect();
            let mut now = Instant::now();
            for step in 0..500 {
                let r = next();
                let (peer, ..) = peers[(r >> 8) as usize % peers.len()];
                let held: Vec<HashRequest> = listed
                    .0
                    .iter()
                    .filter(|w| w.out.iter().any(|o| o.peer == peer))
                    .map(|w| w.request)
                    .collect();
                let failing: Vec<ValidPieceIndex> = listed
                    .0
                    .iter()
                    .filter_map(|w| match w.kind {
                        Kind::Blocks { piece, .. } => Some(piece),
                        Kind::Layer => None,
                    })
                    .collect();
                match r % 7 {
                    0 | 1 if !held.is_empty() => {
                        let a = held[(r >> 16) as usize % held.len()];
                        assert_eq!(
                            asked(indexed.answered(peer, &a)),
                            asked(listed.answered(peer, &a)),
                            "run {run} step {step}"
                        );
                        if r & 0x100 != 0 {
                            assert_eq!(
                                blocks(indexed.landed(&a)),
                                blocks(listed.landed(&a)),
                                "run {run} step {step}"
                            );
                        }
                    }
                    2 if !held.is_empty() => {
                        let a = held[(r >> 16) as usize % held.len()];
                        indexed.rejected(peer, &a);
                        listed.rejected(peer, &a);
                    }
                    3 => {
                        let piece = lengths
                            .validate_piece_index(
                                u32::try_from((r >> 16) % u64::from(PIECES)).unwrap(),
                            )
                            .unwrap();
                        // Now and then the request of a layer run, or of a
                        // piece that failed before.
                        let index = match r & 0x300 {
                            0x100 => u32::try_from((r >> 24) % 30).unwrap(),
                            0x200 => 1000,
                            _ => 1001 + u32::try_from(step).unwrap(),
                        };
                        let failed: Vec<FailedBlock> = (0..(r >> 40) % 3)
                            .map(|b| FailedBlock {
                                block: u32::try_from(b).unwrap(),
                                sender: peer,
                                leaf: Id32::new([u8::try_from(step % 256).unwrap(); 32]),
                            })
                            .collect();
                        indexed.verify_blocks(request(index), piece, failed.clone());
                        listed.verify_blocks(request(index), piece, failed);
                    }
                    4 => {
                        indexed.forget(peer);
                        listed.forget(peer);
                    }
                    5 if !failing.is_empty() => {
                        let piece = failing[(r >> 16) as usize % failing.len()];
                        indexed.piece_passed(piece);
                        listed.piece_passed(piece);
                    }
                    _ => now += Duration::from_millis(next() % 3000),
                }
                for (peer, has, timeout) in &peers {
                    let has = |pieces: Range<u32>| pieces.into_iter().any(|p| has[p as usize]);
                    assert_eq!(
                        picked(indexed.pick(*peer, has, *timeout, now)),
                        picked(listed.pick(*peer, has, *timeout, now)),
                        "run {run} step {step} peer {peer:?}"
                    );
                }
                let wanted = indexed.wanted.lock();
                let order: Vec<HashRequest> = wanted.by_order.values().map(|w| w.request).collect();
                let reference: Vec<HashRequest> = listed.0.iter().map(|w| w.request).collect();
                assert_eq!(order, reference, "run {run} step {step}");
                for (peer, ..) in &peers {
                    let reference = listed
                        .0
                        .iter()
                        .filter(|w| w.out.iter().any(|o| o.peer == *peer))
                        .count();
                    assert_eq!(wanted.held(*peer), reference, "run {run} step {step}");
                }
            }
        }
    }

    // A run's pieces read a word at a time say what reading each piece's
    // bit says, a piece past the bitfield's end lacked, over seeded
    // bitfields and runs.
    #[test]
    fn has_any_reads_what_each_bit_read_says() {
        let mut rng = 0x3c6e_f372_fe94_f82bu64;
        for _ in 0..2000 {
            rng ^= rng << 13;
            rng ^= rng >> 7;
            rng ^= rng << 17;
            let bytes: Vec<u8> = (0..(rng % 9))
                .map(|i| (rng >> (i * 7)).to_le_bytes()[0] & (rng >> 3).to_le_bytes()[0])
                .collect();
            let bitfield = crate::type_aliases::BF::from_boxed_slice(bytes.into_boxed_slice());
            let start = u32::try_from((rng >> 20) % 80).unwrap();
            let end = start + u32::try_from((rng >> 30) % 40).unwrap();
            let each = (start..end).any(|p| bitfield.get(p as usize).is_some_and(|b| *b));
            assert_eq!(has_any(&bitfield, start..end), each, "{start}..{end}");
        }
    }

    // Connections woken only when a change may give them work send the same
    // requests, in the same order each, as connections all woken by every
    // change, over seeded runs of answers, rejects, block-hash wants,
    // connections leaving and joining, and time passing, each connection
    // with its own pieces and timeout.
    #[test]
    fn connections_woken_only_for_new_work_send_what_all_woken_send() {
        const PIECES: u32 = 64;
        let mut rng = 0xbb67_ae85_84ca_a73bu64;
        let mut next = move || {
            rng ^= rng << 13;
            rng ^= rng >> 7;
            rng ^= rng << 17;
            rng
        };
        let lengths = Lengths::new(u64::from(PIECES), 1).unwrap();
        let request = |index: u32| HashRequest {
            root: Id32::default(),
            base: 0,
            index,
            count: 1,
            proof_layers: 0,
        };
        for run in 0..40 {
            let wants: Vec<(HashRequest, Range<u32>)> = (0..12)
                .map(|i| {
                    let start = u32::try_from(next() % u64::from(PIECES)).unwrap();
                    let end = (start + 1 + u32::try_from(next() % 16).unwrap()).min(PIECES);
                    (request(i), start..end)
                })
                .collect();
            let peers: Vec<(SocketAddr, Vec<bool>, Duration)> = (0..6u16)
                .map(|i| {
                    let has = (0..PIECES).map(|_| next() % 3 == 0).collect();
                    let timeout = Duration::from_millis(1000 + next() % 4000);
                    (SocketAddr::from(([10, 0, 0, 1], 6881 + i)), has, timeout)
                })
                .collect();
            let mut old = Side::new(&wants);
            let mut new = Side::new(&wants);
            let mut joined = vec![true; peers.len()];
            let mut now = Instant::now();
            for (peer, ..) in &peers {
                new.fetch.waiter((*peer).into());
            }
            for (peer, has, timeout) in &peers {
                old.run(*peer, has, *timeout, now);
                new.run(*peer, has, *timeout, now);
            }
            let mut next_index = 1000;
            for step in 0..400 {
                let r = next();
                let at = (r >> 8) as usize % peers.len();
                let (peer, ..) = peers[at];
                let held: Vec<HashRequest> = old
                    .fetch
                    .wanted
                    .lock()
                    .by_order
                    .values()
                    .filter(|w| w.out.iter().any(|o| o.peer == peer.into()))
                    .map(|w| w.request)
                    .collect();
                let failing: Vec<ValidPieceIndex> = old
                    .fetch
                    .wanted
                    .lock()
                    .by_order
                    .values()
                    .filter_map(|w| match w.kind {
                        Kind::Blocks { piece, .. } => Some(piece),
                        Kind::Layer => None,
                    })
                    .collect();
                let event = match r % 7 {
                    0 | 1 if !held.is_empty() => {
                        let asked = held[(r >> 16) as usize % held.len()];
                        let landed = r & 0x100 != 0;
                        for side in [&mut old, &mut new] {
                            side.fetch.answered(peer.into(), &asked);
                            if landed {
                                side.fetch.landed(&asked);
                            }
                        }
                        true
                    }
                    2 if !held.is_empty() => {
                        let asked = held[(r >> 16) as usize % held.len()];
                        for side in [&mut old, &mut new] {
                            side.fetch.rejected(peer.into(), &asked);
                        }
                        true
                    }
                    3 => {
                        let piece = lengths
                            .validate_piece_index(
                                u32::try_from((r >> 16) % u64::from(PIECES)).unwrap(),
                            )
                            .unwrap();
                        // Now and then a piece that already failed fails again.
                        let index = if r & 0x200 != 0 { 1000 } else { next_index };
                        next_index += 1;
                        for side in [&mut old, &mut new] {
                            side.fetch.verify_blocks(request(index), piece, Vec::new());
                        }
                        true
                    }
                    // A piece whose block hashes are wanted passes.
                    5 if !failing.is_empty() => {
                        let piece = failing[(r >> 16) as usize % failing.len()];
                        for side in [&mut old, &mut new] {
                            side.fetch.piece_passed(piece);
                        }
                        true
                    }
                    4 if r & 0x400 != 0 => {
                        // The connection leaves, or one that left joins again.
                        if joined[at] {
                            joined[at] = false;
                            for side in [&mut old, &mut new] {
                                side.fetch.forget(peer.into());
                                side.wake_at.remove(&peer);
                            }
                        } else {
                            joined[at] = true;
                            new.fetch.waiter(peer.into());
                            let (_, has, timeout) = &peers[at];
                            old.run(peer, has, *timeout, now);
                            new.run(peer, has, *timeout, now);
                        }
                        true
                    }
                    _ => {
                        now += Duration::from_millis(next() % 3000);
                        false
                    }
                };
                for (i, (peer, has, timeout)) in peers.iter().enumerate() {
                    if !joined[i] {
                        continue;
                    }
                    if event || old.timer_due(*peer, now) {
                        old.run(*peer, has, *timeout, now);
                    }
                    let rung = new
                        .fetch
                        .waiter((*peer).into())
                        .notified()
                        .now_or_never()
                        .is_some();
                    if rung || new.timer_due(*peer, now) {
                        new.run(*peer, has, *timeout, now);
                    }
                }
                for (peer, ..) in &peers {
                    assert_eq!(
                        old.sent.get(peer),
                        new.sent.get(peer),
                        "run {run} step {step} peer {peer}"
                    );
                }
            }
        }
    }
}
