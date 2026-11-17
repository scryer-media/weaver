// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// How a WireGuard tunnel's memory is shared out, in bytes.
//
// A tunnel holds a fixed number of bytes, and every byte it allocates is
// charged against it: first its own overhead (the OS socket's buffer, the
// packet queues, the engine's receive pool, the resolver's socket), then each
// socket at the size the tunnel allocates for it, from the moment it is handed
// out until the tunnel frees it. A TCP stream's socket stays in the tunnel
// after the stream is dropped, until its close finishes or the tunnel's
// linger runs out, so the tunnel holds its bytes and hands them back the
// moment it frees the socket (`StreamSocket`); a datagram socket, a listener
// and a connect that never completed come back at once.
//
// The bytes after the overhead are split into a reserve, kept for what
// serves every kind of traffic (name lookups, probes, feeds, other outbound
// HTTP and the carriers proxies above share), the bytes kept for the torrent
// sessions' listeners and datagram sockets, which they hold for their whole
// life and nothing can ask back, and the sides, one per class of job, that
// the bandwidth split divides: one side alone may take every byte, and sides
// that all press get their share of the split in force. The rule itself is
// the bandwidth split's, behind `SocketDivider`.
//
// Together they never hold more than the tunnel's bytes: a hand-out that
// would pass them is refused, whatever part it is of.
//
// A side that holds more than it is given while another side presses owes
// the rest: a level the ledger reads from what each holds and is given, not
// an ask that is handed out and can be lost. The ledger wakes the side when
// its debt grows or another side begins to press, and the server answers the
// wake by asking the class to close what it owes less what it has already
// marked to close. Bytes already closing are owed no more. A side whose
// answer found nothing to give is not woken again until its own holdings
// change.
//
// A side presses exactly while something holds one of its `Demand`s: the
// want a refused hand-out leaves, held by what truly wants the socket (a
// download lane parked for the route, a torrent with peers it could not dial
// over TCP) until it gets one or stops wanting it. With the last one dropped
// the side stops pressing, and the other side owes nothing for it.
// A look alone never leaves one: a side refused only on a look at its room,
// as a dialer that tries another transport when there is none, presses for
// that look once it uses what it was given, as the bandwidth rule counts a
// side pressing, and for that division only.
//
// Connections kept between lookups (`Keeper`) hold bytes nothing is using,
// each the bytes its socket reserved when it was dialled. A claim that does
// not fit closes as many of them as it needs, the oldest first, and no more:
// bytes already closing count as on their way back, so a second claim closes
// nothing for bytes the first freed. One on a tunnel socket is reset, so the
// tunnel frees the socket and its bytes are back before the claim looks
// again; any other comes back as a closing stream does, when its socket is
// freed. A claim refused only for the bytes of kept connections closed for
// a claim, its own or an earlier one's, waits for them (`claim`): without
// those connections kept the bytes would have been there, so nothing kept
// makes it fail. A look at room closes nothing, and counts what closing the
// kept connections would give. A connection newly kept rings its class's
// slot, so a waiter refused while it was in use comes back to close it.
use std::collections::BTreeMap;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

use tokio::sync::Notify;

use super::{Held, Keeper, LISTEN_BACKLOG, Purpose};
use crate::wireguard::{TcpBuffers, WgTcpAborter, WgTcpStream};
use proxy_tunnels::SlotFreed;

const KIB: u64 = 1024;
const MIB: u64 = 1024 * KIB;

// A TCP socket's buffers, each way, on a tunnel built with download tuning
// when the socket asks for no sizes of its own (proxy-tunnels
// `TUNED_TCP_BUFFER_BYTES`).
const TUNED_TCP_BUFFER_BYTES: u64 = MIB;

// The sizes a tunnel built with download tuning gives a TCP socket that asks
// for none, asked explicitly.
pub(crate) const fn tuned_buffers() -> TcpBuffers {
    TcpBuffers::new(
        TUNED_TCP_BUFFER_BYTES as usize,
        TUNED_TCP_BUFFER_BYTES as usize,
    )
}

// What the tunnel keeps for one socket beside its buffers: the socket's own
// record and its place in the socket set. Charged at a fixed worst case.
const SOCKET_RECORD_BYTES: u64 = 4 * KIB;

// What the tunnel keeps for each datagram or packet slot beside its payload.
// Charged at a fixed worst case.
const PACKET_RECORD_BYTES: u64 = 64;

// The packets the tunnel's link queues each way (proxy-tunnels
// `PACKET_QUEUE_DEPTH`), and the engine passes a carrying socket receives
// (`TRANSPORT_RECEIVE_PASSES`).
const PACKET_QUEUE_DEPTH: u64 = 512;
const TRANSPORT_RECEIVE_PASSES: u64 = 8;

// The OS socket's receive buffer the tunnel asks for (proxy-tunnels
// `TUNED_RECEIVE_BUFFER_BYTES`), charged twice since Linux doubles what is
// asked, and the send buffer the OS keeps beside it.
const OS_RECEIVE_BYTES: u64 = 2 * 4 * MIB;
const OS_SEND_BYTES: u64 = MIB;

// The engine's pool of receive buffers (proxy-tunnels `RECEIVE_BUFFERS`, of
// gotatun's 4096-byte packet buffers).
const ENGINE_RECEIVE_BYTES: u64 = (PACKET_QUEUE_DEPTH + 64) * 4096;

// The tunnel's one resolver socket and its query list, at a worst case.
const RESOLVER_BYTES: u64 = 64 * KIB;

// What a tunnel of `mtu` holds before any socket is handed out.
pub const fn overhead_bytes(mtu: u16) -> u64 {
    let packet = mtu as u64 + PACKET_RECORD_BYTES;
    OS_RECEIVE_BYTES
        + OS_SEND_BYTES
        + 2 * PACKET_QUEUE_DEPTH * packet
        + ENGINE_RECEIVE_BYTES
        + RESOLVER_BYTES
}

// What one TCP socket holds, asked `buffers` or the tunnel's own sizes.
pub fn stream_bytes(buffers: Option<TcpBuffers>) -> u64 {
    let (receive, send) = buffers.map_or((TUNED_TCP_BUFFER_BYTES, TUNED_TCP_BUFFER_BYTES), |b| {
        (b.receive() as u64, b.send() as u64)
    });
    receive + send + SOCKET_RECORD_BYTES
}

// What one datagram socket holds on a tunnel of `mtu`: the tunnel sizes
// every datagram socket it hands out as a carrier of another tunnel, one
// pass of packets to send and `TRANSPORT_RECEIVE_PASSES` passes to receive.
pub const fn datagram_bytes(mtu: u16) -> u64 {
    let packets = PACKET_QUEUE_DEPTH * (1 + TRANSPORT_RECEIVE_PASSES);
    packets * (mtu as u64 + PACKET_RECORD_BYTES) + SOCKET_RECORD_BYTES
}

// What one listener holds for as long as it listens: its backlog of TCP
// sockets at the tunnel's own sizes.
pub fn listener_bytes() -> u64 {
    LISTEN_BACKLOG as u64 * stream_bytes(None)
}

// The classes of job the bandwidth split divides, one side each. A new class
// is one more side here and one more `SocketClass`.
pub const SIDES: usize = 2;
const USENET: usize = 0;
const TORRENT: usize = 1;
const RESERVE: usize = SIDES;
const KEPT: usize = SIDES + 1;
const KINDS: usize = SIDES + 2;

// Which part of the tunnel's bytes a socket counts against.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum SocketClass {
    // Usenet connections and tests of usenet servers.
    Nntp,
    // Torrent peers over TCP, and web seed transfers over HTTP.
    Torrent,
    // A torrent session's listeners and datagram sockets (DHT, uTP, UDP
    // trackers), held for its whole life: off the split, in the bytes kept
    // for them, and past those in what they hold, as a speed test's session
    // beside the routes' does while it lives.
    TorrentSession,
    // Name lookups, probes, feeds, other outbound HTTP, the torrent
    // sessions' trackers and metainfo, and the carriers a proxy's session
    // shares: off the split, so a torrent's own peers never starve its
    // announces.
    Reserve,
}

impl SocketClass {
    pub fn of(purpose: &Purpose) -> Self {
        match purpose {
            Purpose::Nntp { .. } => Self::Nntp,
            Purpose::Torrent | Purpose::TorrentWebSeed => Self::Torrent,
            Purpose::Datagram => Self::TorrentSession,
            Purpose::ProxyEndpoint {
                carrying: Some(class),
                ..
            } => *class,
            // A probe may test a server before it is saved or turned on,
            // when its route counts no connection on the tunnel yet.
            Purpose::NntpProbe { .. }
            | Purpose::ProxyEndpoint { .. }
            | Purpose::Probe
            | Purpose::Dns
            | Purpose::TorrentHttp
            | Purpose::Feed { .. }
            | Purpose::Outbound => Self::Reserve,
        }
    }

    // The side of the split the class takes, or none for bytes kept off it.
    pub const fn side(self) -> Option<usize> {
        match self {
            Self::Nntp => Some(USENET),
            Self::Torrent => Some(TORRENT),
            Self::Reserve | Self::TorrentSession => None,
        }
    }

    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Nntp => "usenet",
            Self::Torrent => "torrent",
            Self::Reserve => "reserve",
            Self::TorrentSession => "torrent session",
        }
    }

    const fn index(self) -> usize {
        match self {
            Self::Nntp => USENET,
            Self::Torrent => TORRENT,
            Self::Reserve => RESERVE,
            Self::TorrentSession => KEPT,
        }
    }
}

// The class of each side, by side.
pub const SIDE_CLASSES: [SocketClass; SIDES] = [SocketClass::Nntp, SocketClass::Torrent];

// The rule that divides the sides' bytes: the bandwidth split's.
pub trait SocketDivider: Send + Sync {
    // Each side's share of `cap` by the split in force, adding up to it.
    fn shares(&self, cap: u64) -> [u64; SIDES];
    // Each side's bytes out of `cap`, from its share, what it was `given` at
    // the last division and what it `used`: held, and asked for by a refused
    // hand-out. Adding up to `cap`.
    fn share_out(&self, cap: u64, shares: &[u64], given: &[Option<u64>], used: &[u64]) -> Vec<u64>;
    // Whether a side that `used` this much of what it was `given` presses.
    fn presses(&self, used: u64, given: u64) -> bool;
}

// A tunnel's bytes: everything it may hold, of which the reserve and those
// kept for the torrent sessions' listeners and datagram sockets, on a link of
// `mtu`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct SocketBudget {
    pub bytes: u64,
    pub reserve: u64,
    pub torrent_kept: u64,
    pub mtu: u16,
}

impl SocketBudget {
    // The bytes the sockets may hold, after the overhead.
    pub const fn sockets(&self) -> u64 {
        self.bytes.saturating_sub(overhead_bytes(self.mtu))
    }

    // The bytes the sides divide.
    pub const fn sides(&self) -> u64 {
        self.sockets()
            .saturating_sub(self.reserve)
            .saturating_sub(self.torrent_kept)
    }
}

#[derive(Clone)]
struct Ledger {
    budget: SocketBudget,
    // Bytes and sockets held by each side, then the reserve, and the bytes
    // among them of streams dropped whose sockets the tunnel still holds.
    held: [u64; KINDS],
    open: [usize; KINDS],
    closing: [u64; KINDS],
    // The bytes among them of kept connections closed to make room, whose
    // sockets the tunnel still holds.
    reclaimed: [u64; KINDS],
    // What each side was given at the last division; none before the first.
    given: [Option<u64>; SIDES],
    // Each side's live demands: how many ask each size of socket. A side
    // presses while it has one.
    demand: [BTreeMap<u64, usize>; SIDES],
    // What each side owed when the ledger last settled, and who pressed
    // then: a settle wakes a side whose debt grew, or that owes when
    // another side begins to press.
    level: [u64; SIDES],
    pressed: [bool; SIDES],
    // A side whose answer found nothing it could give: not woken again
    // until its own holdings change.
    dry: [bool; SIDES],
    // A dry side one of whose dials connected since: its answer may find
    // that socket now, so the next settle wakes it while it owes.
    rewake: [bool; SIDES],
    // The sides to wake, read once by whoever answers them.
    wake: [bool; SIDES],
}

impl Ledger {
    // The most a live demand of `side` asks; none without one.
    fn asked(&self, side: usize) -> u64 {
        self.demand[side].keys().next_back().copied().unwrap_or(0)
    }

    fn pressing(&self) -> [bool; SIDES] {
        std::array::from_fn(|side| !self.demand[side].is_empty())
    }

    // One more demand of `side` for `bytes`.
    fn press(&mut self, side: usize, bytes: u64) {
        *self.demand[side].entry(bytes).or_default() += 1;
    }

    // One demand of `side` for `bytes` let go.
    fn unpress(&mut self, side: usize, bytes: u64) {
        if let Some(count) = self.demand[side].get_mut(&bytes) {
            *count -= 1;
            if *count == 0 {
                self.demand[side].remove(&bytes);
            }
        }
    }

    // What `side` owes, a level and not an ask: the bytes it holds past
    // what it is given, not counting those already closing, while another
    // side presses or the sides hold past their bytes, as a lowered budget
    // leaves them. Nothing otherwise.
    fn owed(&self, divider: &dyn SocketDivider, side: usize) -> u64 {
        let pressing = self.pressing();
        let pressed = (0..SIDES).any(|other| other != side && pressing[other]) || self.over();
        if pressed {
            self.active(side).saturating_sub(self.given(divider, side))
        } else {
            0
        }
    }

    // `side`'s own holdings changed: an answer that found nothing to give
    // may find something now.
    fn moved(&mut self, kind: usize) {
        if kind < SIDES {
            self.dry[kind] = false;
        }
    }

    // One of `kind`'s sockets went from dialling to connected. The bytes
    // were held from the dial, so nothing grows, but an answer that found
    // nothing to give counted the dial as nothing: a dry side is woken again.
    fn connected(&mut self, kind: usize) {
        if kind < SIDES && self.dry[kind] {
            self.dry[kind] = false;
            self.rewake[kind] = true;
        }
    }

    // Read what each side owes now and mark for waking each that owes and
    // is not dry, when its debt grew, another side began to press, or a dial
    // of a side found dry connected. True when a side is to be woken.
    fn settle(&mut self, divider: &dyn SocketDivider) -> bool {
        let pressing = self.pressing();
        let mut woke = false;
        let rewake = std::mem::take(&mut self.rewake);
        for (side, rewake) in rewake.into_iter().enumerate() {
            let owed = self.owed(divider, side);
            let began =
                (0..SIDES).any(|other| other != side && pressing[other] && !self.pressed[other]);
            if owed > 0 && !self.dry[side] && (owed > self.level[side] || began || rewake) {
                crate::metrics::record_socket_give_back(owed - owed.min(self.level[side]));
                self.wake[side] = true;
                woke = true;
            }
            self.level[side] = owed;
        }
        self.pressed = pressing;
        woke
    }

    fn sides_held(&self) -> u64 {
        self.held[..SIDES].iter().sum()
    }

    // The bytes kept off the split for the torrent sessions' sockets: what
    // the budget keeps for them, or what they hold when they hold more.
    fn kept(&self) -> u64 {
        self.budget.torrent_kept.max(self.held[KEPT])
    }

    // The bytes the sides divide: the budget's, less any torrent session
    // sockets held past what it keeps for them.
    fn sides(&self) -> u64 {
        self.budget
            .sockets()
            .saturating_sub(self.budget.reserve)
            .saturating_sub(self.kept())
    }

    // Whether the sides hold past their bytes.
    fn over(&self) -> bool {
        self.sides_held() > self.sides()
    }

    // Bytes a side holds that are not already closing.
    fn active(&self, side: usize) -> u64 {
        self.held[side] - self.closing[side]
    }

    fn given(&self, divider: &dyn SocketDivider, side: usize) -> u64 {
        self.given[side].unwrap_or_else(|| divider.shares(self.sides())[side])
    }

    // Whether a hand-out of `bytes` to `side` is refused only because
    // another side holds past its own share: the side has room in its own
    // share, and gets it once the other gives back. Read against the shares,
    // not what each side was given, since a look shares those out to what
    // each side holds.
    fn crowded_out(&self, divider: &dyn SocketDivider, side: usize, bytes: u64) -> bool {
        let shares = divider.shares(self.sides());
        self.held[side] + bytes <= shares[side]
            && (0..SIDES).any(|other| other != side && self.active(other) > shares[other])
    }

    // Divide the sides' bytes again. `looked` is what a look at a side's
    // room asked: it shares out what each side is given, for this division
    // only, and is no press. What a side then owes is read by `settle`.
    fn divide(&mut self, divider: &dyn SocketDivider, looked: [u64; SIDES]) {
        let cap = self.sides();
        let shares = divider.shares(cap);
        // A side with a demand presses however little it asked: it uses all
        // it was given, and the bytes it was refused.
        let demand: [u64; SIDES] = std::array::from_fn(|side| self.asked(side));
        let wants: [u64; SIDES] = std::array::from_fn(|side| demand[side].max(looked[side]));
        let used: Vec<u64> = (0..SIDES)
            .map(|side| match wants[side] {
                0 => self.held[side],
                asked => (self.held[side] + asked).max(self.given(divider, side)),
            })
            .collect();
        let given = divider.share_out(cap, &shares, &self.given, &used);
        for (side, given) in given.into_iter().enumerate() {
            self.given[side] = Some(given);
        }
    }
}

struct Shares {
    ledger: Mutex<Ledger>,
    divider: Arc<dyn SocketDivider>,
    freed: [SlotFreed; KINDS],
    // Rings when a side is to be woken to give bytes back.
    give_back: Arc<Notify>,
    // The bytes held now, overhead included, and the limit, for metrics.
    reserved: AtomicU64,
    limit: AtomicU64,
}

impl Shares {
    fn ledger(&self) -> std::sync::MutexGuard<'_, Ledger> {
        self.ledger
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn publish(&self, ledger: &Ledger) {
        let held: u64 = ledger.held.iter().sum();
        self.reserved
            .store(held + overhead_bytes(ledger.budget.mtu), Ordering::Relaxed);
        self.limit.store(ledger.budget.bytes, Ordering::Relaxed);
    }

    // Wake every waiter: each class's slot rings as bytes come back.
    fn wake(&self) {
        for freed in &self.freed {
            freed.notify().notify_waiters();
        }
    }

    // `bytes` of `kind` come back to the tunnel, of a stream already counted
    // closing when `closed`. True when a side is to be woken.
    fn give(&self, ledger: &mut Ledger, kind: usize, bytes: u64, closed: bool) -> bool {
        ledger.held[kind] -= bytes;
        ledger.open[kind] -= 1;
        if closed {
            ledger.closing[kind] -= bytes;
        } else {
            ledger.moved(kind);
        }
        if ledger.pressing().contains(&true) || ledger.over() {
            ledger.divide(&*self.divider, [0; SIDES]);
        }
        ledger.settle(&*self.divider)
    }

    // Return `bytes` of `kind` now: the socket is gone. `stream` is the
    // stream's side of a tunnel stream's bytes, when the tunnel held them.
    // Takes only the ledger's lock and wakes waiters, never a runtime, so it
    // runs from any thread, the tunnel's own with its stack lock held.
    fn release(&self, kind: usize, bytes: u64, stream: Option<&StreamEnd>) {
        let mut ledger = self.ledger();
        let closed = stream.is_some_and(|stream| {
            stream.freed.store(true, Ordering::Relaxed);
            if stream.reclaimed.load(Ordering::Relaxed) {
                ledger.reclaimed[kind] -= bytes;
            }
            stream.closed.load(Ordering::Relaxed)
        });
        let asked = self.give(&mut ledger, kind, bytes, closed);
        self.publish(&ledger);
        drop(ledger);
        self.settled(asked);
    }

    // The stream holding `bytes` of `kind` was dropped while the tunnel still
    // holds its socket: the bytes are closing, and come back when the tunnel
    // frees the socket. Nothing, when it already has.
    fn close(&self, kind: usize, bytes: u64, stream: &StreamEnd) {
        let mut ledger = self.ledger();
        if stream.freed.load(Ordering::Relaxed) {
            return;
        }
        stream.closed.store(true, Ordering::Relaxed);
        ledger.closing[kind] += bytes;
        ledger.moved(kind);
        // Closing, the bytes are owed no more: nothing grows, so none wakes.
        ledger.settle(&*self.divider);
    }

    // The kept connection holding `bytes` of `kind` on `stream` was closed to
    // make room: a claim refused for those bytes alone waits for them.
    // Nothing, when the tunnel already freed the socket.
    fn reclaimed(&self, kind: usize, bytes: u64, stream: &StreamEnd) {
        let mut ledger = self.ledger();
        if stream.freed.load(Ordering::Relaxed) || stream.reclaimed.load(Ordering::Relaxed) {
            return;
        }
        stream.reclaimed.store(true, Ordering::Relaxed);
        ledger.reclaimed[kind] += bytes;
    }

    // A socket of `kind` connected: no bytes move, so only the side woken
    // to give back is told.
    fn connected(&self, kind: usize) {
        let mut ledger = self.ledger();
        ledger.connected(kind);
        let woke = ledger.settle(&*self.divider);
        drop(ledger);
        if woke {
            self.give_back.notify_waiters();
        }
    }

    fn settled(&self, asked: bool) {
        self.wake();
        if asked {
            self.give_back.notify_waiters();
        }
    }
}

// The bytes one socket holds of the tunnel, given back when it is dropped.
pub struct SocketPermit {
    shares: Arc<Shares>,
    kind: usize,
    bytes: u64,
    // The stream's side, for a permit a tunnel holds for a TCP stream.
    stream: Option<Arc<StreamEnd>>,
}

impl SocketPermit {
    pub const fn bytes(&self) -> u64 {
        self.bytes
    }

    // What the socket's owner calls once it counts the socket connected,
    // not still dialling: the side's holdings moved, so a side found dry is
    // woken again. Holds the tunnel weakly; a call after it is gone does
    // nothing.
    pub fn connected_signal(&self) -> impl Fn() + Send + Sync + 'static {
        let shares = Arc::downgrade(&self.shares);
        let kind = self.kind;
        move || {
            if let Some(shares) = shares.upgrade() {
                shares.connected(kind);
            }
        }
    }

    // Split the bytes of a TCP stream whose socket outlives it: the permit,
    // for the tunnel to drop when it frees the socket, and the stream's
    // `StreamOpen`, whose drop counts the bytes closing, owed no more, until
    // the permit gives them back. Whichever goes first, the bytes come back
    // exactly once, when the permit goes.
    pub fn for_stream(self) -> (SocketPermit, StreamOpen) {
        let mut permit = self;
        // The stream's side, made already when the permit was marked
        // reclaimable.
        let stream = permit.stream.take().unwrap_or_default();
        let open = StreamOpen {
            shares: Arc::clone(&permit.shares),
            kind: permit.kind,
            bytes: permit.bytes,
            stream: Arc::clone(&stream),
        };
        permit.stream = Some(stream);
        (permit, open)
    }

    // What a connection kept on this socket of `class` registers with its
    // keeper: the bytes, and the stream's side to mark as reclaimed.
    pub(crate) fn reclaimable(&mut self, class: SocketClass) -> Reclaimable {
        let stream = self.stream.get_or_insert_with(Arc::default);
        Reclaimable {
            shares: Arc::downgrade(&self.shares),
            class,
            bytes: self.bytes,
            stream: Arc::clone(stream),
        }
    }
}

impl Drop for SocketPermit {
    fn drop(&mut self) {
        self.shares
            .release(self.kind, self.bytes, self.stream.as_deref());
    }
}

// What a split stream's two sides know of each other, the flags read and
// written only under the ledger's lock: whether the stream was dropped and
// counted closing, and whether the tunnel freed the socket.
#[derive(Default)]
struct StreamEnd {
    closed: AtomicBool,
    freed: AtomicBool,
    // Whether a kept connection on the stream was closed to make room.
    reclaimed: AtomicBool,
    // What resets the stream's tunnel socket, for a stream a kept connection
    // may ride: set once the tunnel holds the socket.
    aborter: OnceLock<WgTcpAborter>,
}

// The socket a kept connection holds, as its keeper's share reads and
// reclaims it: the class and bytes it reserved, and the stream's side.
// Holds the tunnel's bytes weakly.
#[derive(Clone)]
pub(crate) struct Reclaimable {
    shares: std::sync::Weak<Shares>,
    class: SocketClass,
    bytes: u64,
    stream: Arc<StreamEnd>,
}

impl Reclaimable {
    pub(crate) fn held(&self) -> Held {
        (self.class, self.bytes)
    }

    // The connection was closed to make room. On a tunnel socket it is reset
    // first: the tunnel frees the socket at once, without waiting on the far
    // side, and the bytes are back before this returns, never counted as
    // reclaimed. Otherwise a claim refused for its bytes alone waits for
    // them. Called with no lock of the ledger's or a keeper's held: the
    // reset hands the bytes back, which takes the ledger's lock.
    pub(crate) fn reclaimed(&self) {
        if let Some(aborter) = self.stream.aborter.get() {
            aborter.abort();
        }
        if let Some(shares) = self.shares.upgrade() {
            shares.reclaimed(self.class.index(), self.bytes, &self.stream);
        }
    }

    // Whether the stream can be reset to free its socket at once.
    #[cfg(test)]
    pub(crate) fn aborts(&self) -> bool {
        self.stream.aborter.get().is_some()
    }

    // The connection is now kept, its bytes reclaimable: a claim of its
    // class waiting for bytes looks again.
    pub(crate) fn kept(&self) {
        if let Some(shares) = self.shares.upgrade() {
            shares.freed[self.class.index()].notify().notify_waiters();
        }
    }
}

// The stream's side of a TCP stream's bytes, held by the stream while it is
// open. Dropped, it counts the bytes closing until the tunnel frees them.
pub struct StreamOpen {
    shares: Arc<Shares>,
    kind: usize,
    bytes: u64,
    stream: Arc<StreamEnd>,
}

impl Drop for StreamOpen {
    fn drop(&mut self) {
        self.shares.close(self.kind, self.bytes, &self.stream);
    }
}

// A TCP stream's bytes. The dial or accept that opens the stream holds the
// permit and gives the bytes back if no stream comes of it. Once the
// stream's socket is on a tunnel's stack, the tunnel holds the permit and
// gives the bytes back the moment it frees the socket's buffers, however
// long after the stream is dropped that is; the stream keeps its
// `StreamOpen`. A stream of any other kind keeps the permit, and its bytes
// come back as it is dropped.
#[derive(Default)]
pub struct StreamSocket {
    permit: Option<SocketPermit>,
    open: Option<StreamOpen>,
}

impl StreamSocket {
    pub fn new(permit: Option<SocketPermit>) -> Self {
        Self { permit, open: None }
    }

    // `stream`'s socket holds these bytes: its tunnel holds the permit
    // until it frees the socket. A stream a kept connection may ride, marked
    // reclaimable, keeps what resets the socket; no other makes one. Taking
    // the tunnel's stack lock here, this holds no lock of the ledger's.
    pub fn hold_in(&mut self, stream: &WgTcpStream) {
        if let Some(permit) = self.permit.take() {
            if let Some(end) = &permit.stream {
                end.aborter.get_or_init(|| stream.aborter());
            }
            let (permit, open) = permit.for_stream();
            stream.hold_until_freed(permit);
            self.open = Some(open);
        }
    }

    // What a connection kept on this stream of `class` registers with its
    // keeper, while the dial still holds the stream's bytes: none for a
    // stream that holds none.
    pub(crate) fn reclaimable(&mut self, class: SocketClass) -> Option<Reclaimable> {
        Some(self.permit.as_mut()?.reclaimable(class))
    }

    // Whether the stream's tunnel socket can be reset to free it at once.
    #[cfg(test)]
    pub(crate) fn aborts(&self) -> bool {
        self.open
            .as_ref()
            .is_some_and(|open| open.stream.aborter.get().is_some())
    }

    // `SocketPermit::connected_signal`, while this stream holds bytes.
    pub fn connected_signal(&self) -> Option<impl Fn() + Send + Sync + 'static> {
        let (shares, kind) = match (&self.permit, &self.open) {
            (Some(permit), _) => (&permit.shares, permit.kind),
            (None, Some(open)) => (&open.shares, open.kind),
            (None, None) => return None,
        };
        let shares = Arc::downgrade(shares);
        Some(move || {
            if let Some(shares) = shares.upgrade() {
                shares.connected(kind);
            }
        })
    }
}

// A hand-out or look refused: the sockets of its class open now, the slot
// that rings as bytes come back, and the demand a side's refusal leaves.
#[derive(Debug)]
pub struct Refused {
    pub open: usize,
    pub freed: SlotFreed,
    pub demand: Option<Demand>,
    // Whether it fits once the bytes of kept connections closed to make
    // room are back, which they are once the tunnel frees their sockets.
    pub reclaiming: bool,
}

// A side's want for a socket it was refused. The side presses exactly while
// one is held, and the other side owes as long: whoever truly wants the
// socket holds it, and drops it once it is served or wants the socket no
// more. Clones share the one want.
#[derive(Clone)]
pub struct Demand(Arc<Want>);

struct Want {
    shares: Arc<Shares>,
    side: usize,
    bytes: u64,
}

impl Demand {
    // The want for a press already counted in the ledger.
    fn pressed(shares: &Arc<Shares>, side: usize, bytes: u64) -> Self {
        Self(Arc::new(Want {
            shares: Arc::clone(shares),
            side,
            bytes,
        }))
    }

    // The bytes the refused socket asked.
    pub fn bytes(&self) -> u64 {
        self.0.bytes
    }

    // The class whose side presses.
    pub fn class(&self) -> SocketClass {
        SIDE_CLASSES[self.0.side]
    }

    // Whether both press on one tunnel's bytes.
    pub fn same_tunnel(&self, other: &Self) -> bool {
        Arc::ptr_eq(&self.0.shares, &other.0.shares)
    }
}

impl std::fmt::Debug for Demand {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Demand")
            .field("class", &self.class().as_str())
            .field("bytes", &self.0.bytes)
            .finish()
    }
}

impl Drop for Want {
    fn drop(&mut self) {
        let mut ledger = self.shares.ledger();
        ledger.unpress(self.side, self.bytes);
        // The press ends: what the other side owes falls.
        let woke = ledger.settle(&*self.shares.divider);
        drop(ledger);
        if woke {
            self.shares.give_back.notify_waiters();
        }
    }
}

// A session's bytes. A session without them, any but a WireGuard one, holds
// its sockets to no budget at all. Its keeper holds the connections kept
// between lookups that hold its reserve bytes.
#[derive(Default)]
pub struct SessionSockets {
    shares: Option<Arc<Shares>>,
    keeper: Keeper,
}

impl SessionSockets {
    pub fn new(budget: SocketBudget, divider: Arc<dyn SocketDivider>) -> Self {
        let shares = Arc::new(Shares {
            ledger: Mutex::new(Ledger {
                budget,
                held: [0; KINDS],
                open: [0; KINDS],
                closing: [0; KINDS],
                reclaimed: [0; KINDS],
                given: [None; SIDES],
                demand: Default::default(),
                level: [0; SIDES],
                pressed: [false; SIDES],
                dry: [false; SIDES],
                rewake: [false; SIDES],
                wake: [false; SIDES],
            }),
            divider,
            freed: Default::default(),
            give_back: Arc::new(Notify::new()),
            reserved: AtomicU64::new(0),
            limit: AtomicU64::new(0),
        });
        shares.publish(&shares.ledger());
        Self {
            shares: Some(shares),
            keeper: Keeper::default(),
        }
    }

    // What keeps connections between lookups on this session's bytes.
    pub fn keeper(&self) -> &Keeper {
        &self.keeper
    }

    // How many kept connections a claim of `bytes` of `class` needs closed
    // to fit, counting the bytes of streams already closing as back: none
    // when it fits without, when those bytes suffice, or when closing every
    // kept connection would not make it fit. A claim fitting as the ledger
    // stands reads nothing more. Kept connections help a claim of the class
    // their bytes are held in, and another class's only while the reserve
    // holds past its bytes: within them, the reserve's bytes are off the
    // sides' and below the tunnel's, so closing one never fits another. Reads
    // the ledger alone, then the keeper alone, then the ledger again, and
    // closes nothing.
    fn kept_needed(&self, inner: &Shares, class: SocketClass, bytes: u64) -> usize {
        let over = {
            let ledger = inner.ledger();
            if fits(&ledger, &*inner.divider, class, bytes) {
                return 0;
            }
            ledger.held[RESERVE] > ledger.budget.reserve
        };
        let kept = self.keeper.held();
        if !kept.iter().any(|(holds, _)| *holds == class || over) {
            return 0;
        }
        let mut ledger = inner.ledger().clone();
        reclaim_count(&mut ledger, &*inner.divider, class, bytes, &kept)
    }

    // Close the kept connections a claim of `bytes` of `class` needs, the
    // oldest first. One on a tunnel socket is reset and its bytes are back
    // before this returns; any other's come back when its socket is freed.
    // A reclaim that found its connection already taken for a
    // lookup closed nothing and is not counted. Runs with no lock of the
    // ledger's held: closing takes it.
    fn make_room(&self, class: SocketClass, bytes: u64) {
        let Some(inner) = &self.shares else {
            return;
        };
        for _ in 0..self.kept_needed(inner, class, bytes) {
            if !self.keeper.reclaim() {
                break;
            }
        }
    }

    // As `take`, but a claim refused only for the bytes of kept connections
    // closed to make room, by it or an earlier claim, waits for them: the
    // tunnel gives them back as it frees their sockets, and without those
    // connections kept the bytes would have been there. Any other refusal
    // is returned at once, as `take`'s. Never past the budget: each look is
    // a `take`.
    pub async fn claim(
        &self,
        class: SocketClass,
        bytes: u64,
    ) -> Result<Option<SocketPermit>, Refused> {
        let Some(inner) = &self.shares else {
            return Ok(None);
        };
        let freed = inner.freed[class.index()].notify();
        loop {
            // Armed before the look, so bytes that come back between the
            // refusal and the wait are not missed.
            let notified = freed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            match self.take(class, bytes) {
                Err(refused) if refused.reclaiming => {
                    drop(refused);
                    notified.await;
                }
                taken => return taken,
            }
        }
    }

    // Put a new budget in force. Sockets already open stay open; the sides
    // are divided afresh from their shares, and a side left holding past
    // what it is given, while another presses or past the sides' bytes,
    // owes the rest.
    pub fn set_budget(&self, budget: SocketBudget) {
        let Some(inner) = &self.shares else {
            return;
        };
        let mut ledger = inner.ledger();
        if ledger.budget == budget {
            return;
        }
        ledger.budget = budget;
        ledger.given = [None; SIDES];
        ledger.divide(&*inner.divider, [0; SIDES]);
        let asked = ledger.settle(&*inner.divider);
        inner.publish(&ledger);
        drop(ledger);
        inner.settled(asked);
    }

    // The budget in force, when the session has one.
    pub fn budget(&self) -> Option<SocketBudget> {
        Some(self.shares.as_ref()?.ledger().budget)
    }

    // Whether `bytes` of `class` fit now.
    pub fn has_room(&self, class: SocketClass, bytes: u64) -> bool {
        self.room(class, bytes).is_ok()
    }

    // Room for `bytes` of `class` now, or the signal that bytes come back.
    // A side refused that uses what it was given looks with its want: what
    // each side is given is shared out again with it. A look alone asks no
    // side to give anything back, and leaves no demand.
    //
    // Closing kept connections would make room counts as room: the claim
    // that follows closes them. The look itself closes nothing.
    pub fn room(&self, class: SocketClass, bytes: u64) -> Result<(), SlotFreed> {
        match self.look(class, bytes, false) {
            Ok(()) => Ok(()),
            Err(refused) => match &self.shares {
                Some(inner) if self.kept_needed(inner, class, bytes) > 0 => Ok(()),
                _ => Err(refused.freed),
            },
        }
    }

    // As `room`, but a side refused only because another holds past its
    // share is handed a `Demand`, as a refused hand-out is: while the caller
    // holds it the side presses and the other owes what it holds past its
    // share. A side at or past its own share is handed none. First closes
    // the kept connections whose bytes the caller needs, so it waits for
    // them to come back rather than behind a connection nobody closes.
    pub fn wait_room(&self, class: SocketClass, bytes: u64) -> Result<(), Refused> {
        self.make_room(class, bytes);
        self.look(class, bytes, true)
    }

    fn look(&self, class: SocketClass, bytes: u64, wait: bool) -> Result<(), Refused> {
        let Some(inner) = &self.shares else {
            return Ok(());
        };
        let divider = &*inner.divider;
        let mut ledger = inner.ledger();
        let mut fit = fits(&ledger, divider, class, bytes);
        if !fit
            && let Some(side) = class.side()
            && divider.presses(ledger.held[side], ledger.given(divider, side))
        {
            let mut looked = [0; SIDES];
            looked[side] = bytes;
            ledger.divide(divider, looked);
            fit = fits(&ledger, divider, class, bytes);
        }
        let mut demand = None;
        if !fit
            && wait
            && let Some(side) = class.side()
            && ledger.crowded_out(divider, side, bytes)
        {
            ledger.press(side, bytes);
            ledger.divide(divider, [0; SIDES]);
            demand = Some(Demand::pressed(inner, side, bytes));
        }
        let asked = ledger.settle(divider);
        let open = ledger.open[class.index()];
        drop(ledger);
        if asked {
            inner.settled(asked);
        }
        if fit {
            Ok(())
        } else {
            Err(Refused {
                open,
                freed: inner.freed[class.index()].clone(),
                demand,
                reclaiming: false,
            })
        }
    }

    // `bytes` of `class`, held until the permit is dropped, or the refusal:
    // the sockets of `class` open now, the signal that bytes come back and,
    // for a side, the `Demand` the refusal leaves. A refusal divides the
    // sides again with the side refused pressing. A session without a
    // budget hands out no permits. First closes the kept connections whose
    // bytes it needs: refused while their bytes are closing, the caller
    // waits for them as for any other closing socket.
    pub fn take(&self, class: SocketClass, bytes: u64) -> Result<Option<SocketPermit>, Refused> {
        let Some(inner) = &self.shares else {
            return Ok(None);
        };
        let kind = class.index();
        let mut ledger = inner.ledger();
        let mut fit = fits(&ledger, &*inner.divider, class, bytes);
        if !fit {
            // Only a claim that does not fit reads what is kept.
            drop(ledger);
            self.make_room(class, bytes);
            ledger = inner.ledger();
            fit = fits(&ledger, &*inner.divider, class, bytes);
        }
        let mut pressed = None;
        if !fit && let Some(side) = class.side() {
            // The refusal presses for the division that may yet fit it.
            ledger.press(side, bytes);
            ledger.divide(&*inner.divider, [0; SIDES]);
            fit = fits(&ledger, &*inner.divider, class, bytes);
            pressed = Some(side);
        }
        if !fit {
            let reclaiming = ledger.reclaimed.iter().any(|&bytes| bytes > 0) && {
                let mut back = ledger.clone();
                if let Some(side) = pressed {
                    back.unpress(side, bytes);
                }
                for kind in 0..KINDS {
                    back.held[kind] -= back.reclaimed[kind];
                    back.closing[kind] = back.closing[kind].saturating_sub(back.reclaimed[kind]);
                }
                fits_divided(&mut back, &*inner.divider, class, bytes)
            };
            let asked = ledger.settle(&*inner.divider);
            let open = ledger.open[kind];
            drop(ledger);
            if asked {
                inner.settled(asked);
            }
            return Err(Refused {
                open,
                freed: inner.freed[kind].clone(),
                demand: pressed.map(|side| Demand::pressed(inner, side, bytes)),
                reclaiming,
            });
        }
        // Divided again, it fits: the refusal it nearly was wants nothing.
        if let Some(side) = pressed {
            ledger.unpress(side, bytes);
        }
        let sides = ledger.sides();
        ledger.held[kind] += bytes;
        ledger.open[kind] += 1;
        ledger.moved(kind);
        // Torrent session sockets past what is kept for them leave the sides
        // fewer bytes: each side is given its part of those, and one left
        // holding past it owes the rest.
        if ledger.sides() < sides {
            ledger.divide(&*inner.divider, [0; SIDES]);
        }
        let asked = ledger.settle(&*inner.divider);
        inner.publish(&ledger);
        drop(ledger);
        if asked {
            inner.settled(asked);
        }
        Ok(Some(SocketPermit {
            shares: Arc::clone(inner),
            kind,
            bytes,
            stream: None,
        }))
    }

    // Bytes of `class` held now, closing ones among them.
    pub fn held(&self, class: SocketClass) -> u64 {
        self.shares
            .as_ref()
            .map_or(0, |inner| inner.ledger().held[class.index()])
    }

    // Bytes of `class` of kept connections closed to make room, whose
    // sockets are not yet freed.
    #[cfg(test)]
    pub(crate) fn reclaimed(&self, class: SocketClass) -> u64 {
        self.shares
            .as_ref()
            .map_or(0, |inner| inner.ledger().reclaimed[class.index()])
    }

    // Bytes of `class` whose streams were dropped and whose sockets the
    // tunnel still holds.
    pub fn closing(&self, class: SocketClass) -> u64 {
        self.shares
            .as_ref()
            .map_or(0, |inner| inner.ledger().closing[class.index()])
    }

    // What `class`'s side was given at the last division: its share before
    // the first. The reserve's is the reserve, and the torrent sessions'
    // what is kept for them.
    pub fn given(&self, class: SocketClass) -> u64 {
        let Some(inner) = &self.shares else {
            return 0;
        };
        let ledger = inner.ledger();
        match class {
            SocketClass::Reserve => ledger.budget.reserve,
            SocketClass::TorrentSession => ledger.budget.torrent_kept,
            SocketClass::Nntp | SocketClass::Torrent => {
                ledger.given(&*inner.divider, class.index())
            }
        }
    }

    // Whether `class`'s side presses: something holds one of its demands.
    pub fn pressing(&self, class: SocketClass) -> bool {
        let (Some(inner), Some(side)) = (&self.shares, class.side()) else {
            return false;
        };
        inner.ledger().asked(side) > 0
    }

    // What `class`'s side owes now: the bytes it holds past what it is
    // given, not counting those already closing, while another side
    // presses. A level read at the moment it is acted on, never an ask that
    // can be lost: an answer covers it less what it already has marked to
    // close, and a mark closes only while it is above nothing.
    pub fn owed(&self, class: SocketClass) -> u64 {
        let (Some(inner), Some(side)) = (&self.shares, class.side()) else {
            return 0;
        };
        inner.ledger().owed(&*inner.divider, side)
    }

    // Whether `class`'s side owes anything now.
    pub fn owes(&self, class: SocketClass) -> bool {
        self.owed(class) > 0
    }

    // `class`'s side was woken and found nothing it could give: it is not
    // woken again until its own holdings change.
    pub fn dry(&self, class: SocketClass) {
        let (Some(inner), Some(side)) = (&self.shares, class.side()) else {
            return;
        };
        let mut ledger = inner.ledger();
        if ledger.owed(&*inner.divider, side) > 0 {
            ledger.dry[side] = true;
        }
    }

    // How far `class`'s side holds past what it is given, not counting the
    // bytes already closing. A cheap read; nothing acts on it.
    pub fn over_share(&self, class: SocketClass) -> u64 {
        let (Some(inner), Some(side)) = (&self.shares, class.side()) else {
            return 0;
        };
        let ledger = inner.ledger();
        ledger
            .active(side)
            .saturating_sub(ledger.given(&*inner.divider, side))
    }

    // Rings when a side is to be woken to give bytes back; `woken` says
    // which.
    pub fn give_back_asked(&self) -> Option<Arc<Notify>> {
        Some(Arc::clone(&self.shares.as_ref()?.give_back))
    }

    // The sides to wake since the last read, by side. A wake carries no
    // amount: the side woken reads what it owes when it answers.
    pub fn woken(&self) -> [bool; SIDES] {
        let Some(inner) = &self.shares else {
            return [false; SIDES];
        };
        std::mem::take(&mut inner.ledger().wake)
    }

    // The bytes held now, the tunnel's overhead included, and its limit.
    pub fn reserved(&self) -> u64 {
        self.shares
            .as_ref()
            .map_or(0, |inner| inner.reserved.load(Ordering::Relaxed))
    }

    pub fn limit(&self) -> u64 {
        self.shares
            .as_ref()
            .map_or(0, |inner| inner.limit.load(Ordering::Relaxed))
    }
}

// Whether `bytes` of `class` fit: within the tunnel's bytes, and the reserve
// within the reserve, a side within what it is given and the sides together
// within their bytes. A torrent session's socket fits in what is kept for
// them, or past it in bytes neither the sides nor the reserve hold: it is
// never given back, so it takes none a give-back would have to close.
fn fits(ledger: &Ledger, divider: &dyn SocketDivider, class: SocketClass, bytes: u64) -> bool {
    let held: u64 = ledger.held.iter().sum();
    held + bytes <= ledger.budget.sockets()
        && match class {
            SocketClass::Reserve => ledger.held[RESERVE] + bytes <= ledger.budget.reserve,
            SocketClass::TorrentSession => {
                ledger.held[KEPT] + bytes <= ledger.budget.torrent_kept
                    || ledger.sides_held()
                        + ledger.held[KEPT]
                        + bytes
                        + ledger.budget.reserve.max(ledger.held[RESERVE])
                        <= ledger.budget.sockets()
            }
            SocketClass::Nntp | SocketClass::Torrent => {
                let side = class.index();
                ledger.held[side] + bytes <= ledger.given(divider, side)
                    && ledger.sides_held() + bytes <= ledger.sides()
            }
        }
}

// Whether `bytes` of `class` fit in `ledger`, a copy, as `take` would divide
// for it: as it stands, or for a side, divided again with it pressing.
fn fits_divided(
    ledger: &mut Ledger,
    divider: &dyn SocketDivider,
    class: SocketClass,
    bytes: u64,
) -> bool {
    if fits(ledger, divider, class, bytes) {
        return true;
    }
    let Some(side) = class.side() else {
        return false;
    };
    ledger.press(side, bytes);
    ledger.divide(divider, [0; SIDES]);
    let fit = fits(ledger, divider, class, bytes);
    ledger.unpress(side, bytes);
    fit
}

// How many of the `kept` connections, oldest first, each at the class and
// bytes its socket reserved, `ledger`, a copy, must close for `bytes` of
// `class` to fit, as `take` would divide for it: none when it fits as it
// is, or once the bytes of streams already closing are back, and none when
// closing all of them would not make it fit.
fn reclaim_count(
    ledger: &mut Ledger,
    divider: &dyn SocketDivider,
    class: SocketClass,
    bytes: u64,
    kept: &[Held],
) -> usize {
    let fit = |ledger: &mut Ledger| fits_divided(ledger, divider, class, bytes);
    if fit(ledger) {
        return 0;
    }
    for kind in 0..KINDS {
        ledger.held[kind] -= ledger.closing[kind];
        ledger.closing[kind] = 0;
    }
    if fit(ledger) {
        return 0;
    }
    for (needed, (holds, each)) in (1..).zip(kept) {
        let kind = holds.index();
        let Some(held) = ledger.held[kind].checked_sub(*each) else {
            return 0;
        };
        ledger.held[kind] = held;
        if fit(ledger) {
            return needed;
        }
    }
    0
}

// A session whose reserve holds `streams` DNS streams, and nothing else: no
// side, and nothing kept for torrent sessions.
#[cfg(test)]
pub(crate) fn reserve_of(streams: u64) -> SessionSockets {
    struct NoSides;
    impl SocketDivider for NoSides {
        fn shares(&self, _: u64) -> [u64; SIDES] {
            [0; SIDES]
        }
        fn share_out(&self, _: u64, _: &[u64], _: &[Option<u64>], _: &[u64]) -> Vec<u64> {
            vec![0; SIDES]
        }
        fn presses(&self, _: u64, _: u64) -> bool {
            false
        }
    }
    const MTU: u16 = 1420;
    let reserve = streams * dns_stream_bytes();
    SessionSockets::new(
        SocketBudget {
            bytes: overhead_bytes(MTU) + reserve,
            reserve,
            torrent_kept: 0,
            mtu: MTU,
        },
        Arc::new(NoSides),
    )
}

// What one DNS stream holds.
#[cfg(test)]
pub(crate) fn dns_stream_bytes() -> u64 {
    stream_bytes(super::socket_buffers(&Purpose::Dns))
}

#[cfg(test)]
#[path = "pipe_sockets_tests.rs"]
mod tests;
