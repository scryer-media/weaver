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
    collections::HashMap,
    net::SocketAddr,
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
    Peers(Vec<SocketAddr>),
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
    fn taken_off(peer: SocketAddr, was: Option<Instant>, now: Option<Instant>) -> Wake {
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

/// The hashes a torrent lacks, and which peers they are out to (libtorrent
/// `hash_picker::m_piece_hash_requested`, `m_piece_block_requests`).
pub(crate) struct LayerFetch {
    wanted: Mutex<Vec<Wanted>>,
    /// Each v2 connection's bell, rung when a change may give it new work:
    /// a want came free, or may go to another sooner, or is new, rung for
    /// every connection; a request of its own answered, rejected or
    /// dropped, for it alone. A connection no change can give work is left
    /// asleep. Taken after `wanted` is let go.
    waiters: Mutex<HashMap<SocketAddr, Arc<Notify>>>,
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
            waiters: Mutex::default(),
        }
    }

    /// The bell of `peer`'s connection; see [`Self::waiters`]. A ring that
    /// comes before the connection waits is kept for it.
    pub(super) fn waiter(&self, peer: SocketAddr) -> Arc<Notify> {
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
        peer: SocketAddr,
        has_any: impl Fn(Range<u32>) -> bool,
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
            if let Some(due) = w.due()
                && due > now
            {
                wake = Some(wake.map_or(due, |at| at.min(due)));
                continue;
            }
            if !has_any(w.pieces.clone()) {
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
        let (asked, wake) = match self.take_out(peer, request) {
            Some((w, wake)) => (
                Some(match w.kind {
                    Kind::Layer => Asked::Layer,
                    Kind::Blocks { piece, .. } => Asked::Blocks(piece),
                }),
                wake,
            ),
            None => (None, Wake::Nobody),
        };
        self.wake(wake);
        asked
    }

    /// The hashes `request` asks for are held: the blocks they were wanted
    /// to judge, if any.
    fn landed(&self, request: &HashRequest) -> Vec<FailedBlock> {
        let mut wanted = self.wanted.lock();
        // The peers it is still out to hold one request fewer.
        let (failed, wake) = match wanted.iter().position(|w| w.request == *request) {
            Some(at) => {
                let w = wanted.remove(at);
                let wake = Wake::Peers(w.out.iter().map(|o| o.peer).collect());
                match w.kind {
                    Kind::Blocks { failed, .. } => (failed, wake),
                    Kind::Layer => (Vec::new(), wake),
                }
            }
            None => (Vec::new(), Wake::Nobody),
        };
        drop(wanted);
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
        let mut wanted = self.wanted.lock();
        // More failed blocks to judge change no pick: only a new want wakes.
        let wake = if let Some(Kind::Blocks { failed: had, .. }) = wanted
            .iter_mut()
            .find(|w| w.request == request)
            .map(|w| &mut w.kind)
        {
            had.extend(failed);
            Wake::Nobody
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
            Wake::All
        };
        drop(wanted);
        self.wake(wake);
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
        let wake = match self.take_out(peer, request) {
            Some((mut w, wake)) => {
                w.rejected_by.push(peer);
                wake
            }
            None => Wake::Nobody,
        };
        self.wake(wake);
    }

    /// `peer` left: what it held may go to another, and its bell goes.
    pub fn forget(&self, peer: SocketAddr) {
        let mut wake = Wake::Nobody;
        for w in self.wanted.lock().iter_mut() {
            let was = w.due();
            let before = w.out.len();
            w.out.retain(|o| o.peer != peer);
            if w.out.len() != before {
                wake = wake.and(Wake::taken_off(peer, was, w.due()));
            }
            w.rejected_by.retain(|p| *p != peer);
        }
        self.waiters.lock().remove(&peer);
        self.wake(wake);
    }

    /// The run `request` asks for, taken off `peer`'s requests, when it was
    /// out to it, and whom that may give work.
    fn take_out(
        &self,
        peer: SocketAddr,
        request: &HashRequest,
    ) -> Option<(parking_lot::MappedMutexGuard<'_, Wanted>, Wake)> {
        let wanted = self.wanted.lock();
        let mut wake = Wake::Nobody;
        let w = parking_lot::MutexGuard::try_map(wanted, |wanted| {
            let w = wanted.iter_mut().find(|w| w.request == *request)?;
            let at = w.out.iter().position(|o| o.peer == peer)?;
            let was = w.due();
            w.out.swap_remove(at);
            wake = Wake::taken_off(peer, was, w.due());
            Some(w)
        })
        .ok()?;
        Some((w, wake))
    }
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
    use std::collections::HashMap;

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
            let wanted = wants
                .iter()
                .map(|(request, pieces)| Wanted {
                    request: *request,
                    pieces: pieces.clone(),
                    kind: Kind::Layer,
                    out: Vec::new(),
                    rejected_by: Vec::new(),
                })
                .collect();
            Self {
                fetch: LayerFetch {
                    wanted: Mutex::new(wanted),
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
                    peer,
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
                new.fetch.waiter(*peer);
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
                    .iter()
                    .filter(|w| w.out.iter().any(|o| o.peer == peer))
                    .map(|w| w.request)
                    .collect();
                let event = match r % 6 {
                    0 | 1 if !held.is_empty() => {
                        let asked = held[(r >> 16) as usize % held.len()];
                        let landed = r & 0x100 != 0;
                        for side in [&mut old, &mut new] {
                            side.fetch.answered(peer, &asked);
                            if landed {
                                side.fetch.landed(&asked);
                            }
                        }
                        true
                    }
                    2 if !held.is_empty() => {
                        let asked = held[(r >> 16) as usize % held.len()];
                        for side in [&mut old, &mut new] {
                            side.fetch.rejected(peer, &asked);
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
                    4 if r & 0x400 != 0 => {
                        // The connection leaves, or one that left joins again.
                        if joined[at] {
                            joined[at] = false;
                            for side in [&mut old, &mut new] {
                                side.fetch.forget(peer);
                                side.wake_at.remove(&peer);
                            }
                        } else {
                            joined[at] = true;
                            new.fetch.waiter(peer);
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
                    let rung = new.fetch.waiter(*peer).notified().now_or_never().is_some();
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
