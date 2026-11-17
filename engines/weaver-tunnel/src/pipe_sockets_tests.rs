// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The ledger's mechanics: closing streams, wakes and the parts kept off the
// split. How the sides divide and give back is tested on the bandwidth
// split's own rule, in the server's `socket_ledger_tests`.

use std::sync::Arc;
use std::sync::atomic::AtomicBool;

use super::*;

const MTU: u16 = 1420;

// One peer socket at the tunnel's own sizes: the unit these tests count in.
fn peer() -> u64 {
    stream_bytes(None)
}

// A tunnel whose reserve holds `reserve` peer sockets, whose torrent
// sessions are kept `kept` and whose sides hold `sides`.
fn budget(reserve: u64, kept: u64, sides: u64) -> SocketBudget {
    SocketBudget {
        bytes: overhead_bytes(MTU) + (reserve + sides) * peer() + kept,
        reserve: reserve * peer(),
        torrent_kept: kept,
        mtu: MTU,
    }
}

// Each side holds its half and never more: a division that leaves these
// tests' counts where they set them.
struct Halves;

impl SocketDivider for Halves {
    fn shares(&self, cap: u64) -> [u64; SIDES] {
        [cap / 2, cap - cap / 2]
    }

    fn share_out(&self, cap: u64, _: &[u64], _: &[Option<u64>], _: &[u64]) -> Vec<u64> {
        self.shares(cap).to_vec()
    }

    fn presses(&self, used: u64, given: u64) -> bool {
        used >= given
    }
}

fn sockets(reserve: u64, sides: u64) -> SessionSockets {
    SessionSockets::new(budget(reserve, 0, sides), Arc::new(Halves))
}

fn take(sockets: &SessionSockets, class: SocketClass, bytes: u64) -> Option<SocketPermit> {
    sockets.take(class, bytes).ok().flatten()
}

// Fill `class` one peer socket at a time until the tunnel refuses one.
fn fill(sockets: &SessionSockets, class: SocketClass) -> Vec<SocketPermit> {
    std::iter::from_fn(|| take(sockets, class, peer())).collect()
}

fn alive_tasks() -> usize {
    tokio::runtime::Handle::current()
        .metrics()
        .num_alive_tasks()
}

// A dropped stream's bytes come back when the tunnel frees its socket, and
// not a moment before; until then they count closing. A socket the tunnel
// frees before its stream is dropped, as a stopped tunnel does, gives its
// bytes back then, and the stream's drop counts nothing more. A datagram
// socket's, a listener's and those of a connect that never completed come
// back as they are dropped.
#[test]
fn bytes_come_back_when_the_tunnel_frees_the_socket() {
    let sockets = SessionSockets::new(budget(4, listener_bytes(), 40), Arc::new(Halves));
    let stream = take(&sockets, SocketClass::Nntp, peer()).expect("room");
    let (freed, open) = stream.for_stream();
    assert_eq!(sockets.closing(SocketClass::Nntp), 0, "closing while open");
    drop(open);
    assert_eq!(sockets.held(SocketClass::Nntp), peer());
    assert_eq!(sockets.closing(SocketClass::Nntp), peer());
    drop(freed);
    assert_eq!(sockets.held(SocketClass::Nntp), 0);
    assert_eq!(sockets.closing(SocketClass::Nntp), 0);

    let stream = take(&sockets, SocketClass::Nntp, peer()).expect("room");
    let (freed, open) = stream.for_stream();
    drop(freed);
    assert_eq!(
        sockets.held(SocketClass::Nntp),
        0,
        "freed under an open stream"
    );
    drop(open);
    assert_eq!(sockets.held(SocketClass::Nntp), 0);
    assert_eq!(sockets.closing(SocketClass::Nntp), 0, "closed after freed");

    for (class, bytes) in [
        (SocketClass::Torrent, datagram_bytes(MTU)),
        (SocketClass::TorrentSession, listener_bytes()),
        // A connect that never completed holds no socket in the tunnel.
        (SocketClass::Nntp, peer()),
        (SocketClass::Reserve, peer()),
    ] {
        let permit = take(&sockets, class, bytes).expect("room");
        assert_eq!(sockets.held(class), bytes);
        drop(permit);
        assert_eq!(sockets.held(class), 0, "{class:?} waited");
    }
}

// A hand-out the tunnel refused wakes as bytes come back, without polling.
#[tokio::test]
async fn a_refused_dial_wakes_when_bytes_come_back() {
    let sockets = sockets(4, 20);
    let mut held = fill(&sockets, SocketClass::Torrent);
    assert_eq!(held.len(), 10);
    let Err(Refused { open, freed, .. }) = sockets.take(SocketClass::Torrent, peer()) else {
        panic!("a full tunnel handed out bytes");
    };
    assert_eq!(open, 10);
    let notify = freed.notify();
    let woken = notify.notified();
    tokio::pin!(woken);
    woken.as_mut().enable();
    let (freed, stream) = held.pop().expect("a stream").for_stream();
    drop(stream);
    assert!(
        !sockets.has_room(SocketClass::Torrent, peer()),
        "back while closing"
    );
    // The wake comes with the bytes, when the tunnel frees the socket.
    drop(freed);
    woken.await;
    assert!(sockets.has_room(SocketClass::Torrent, peer()));
    assert!(take(&sockets, SocketClass::Torrent, peer()).is_some());
}

// The tunnel frees a socket on whatever thread it is on, holding its stack
// lock, often with no runtime: giving the bytes back there wakes a waiter on
// a runtime, and closing or freeing starts no task.
#[test]
fn bytes_come_back_off_any_runtime() {
    let sockets = sockets(4, 2);
    let held = fill(&sockets, SocketClass::Nntp);
    assert_eq!(held.len(), 1);
    let Err(refused) = sockets.take(SocketClass::Nntp, peer()) else {
        panic!("a full side handed out bytes");
    };
    let runtime = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    runtime.block_on(async {
        let before = alive_tasks();
        let notify = refused.freed.notify();
        let woken = notify.notified();
        tokio::pin!(woken);
        woken.as_mut().enable();
        let (freed, stream) = held.into_iter().next().expect("a stream").for_stream();
        drop(stream);
        assert_eq!(alive_tasks(), before, "a closing stream started a task");
        std::thread::spawn(move || {
            assert!(tokio::runtime::Handle::try_current().is_err());
            drop(freed);
        })
        .join()
        .unwrap();
        woken.await;
        assert_eq!(alive_tasks(), before, "a freed socket started a task");
    });
    assert_eq!(sockets.held(SocketClass::Nntp), 0);
    assert_eq!(sockets.closing(SocketClass::Nntp), 0);
    drop(refused);
}

// A refused hand-out of the reserve presses no side, and no side owes.
#[tokio::test(start_paused = true)]
async fn the_reserve_is_off_the_split() {
    let sockets = sockets(2, 10);
    let _peers = fill(&sockets, SocketClass::Torrent);
    let reserve = fill(&sockets, SocketClass::Reserve);
    assert_eq!(reserve.len(), 2, "torrents took none of the reserve");
    assert_eq!(sockets.woken(), [false, false]);
    assert!(!sockets.owes(SocketClass::Torrent));
    assert_eq!(sockets.given(SocketClass::Reserve), 2 * peer());
}

// A torrent session's listeners and datagram sockets take the bytes kept for
// them, off the split, and past those the kept part grows while they live.
#[tokio::test]
async fn torrent_sessions_hold_what_is_kept_for_them_first() {
    let kept = listener_bytes() + 2 * datagram_bytes(MTU);
    let sockets = SessionSockets::new(budget(2, kept, 20), Arc::new(Halves));
    let listener = take(&sockets, SocketClass::TorrentSession, listener_bytes()).expect("room");
    let datagrams: Vec<_> = (0..2)
        .map(|_| take(&sockets, SocketClass::TorrentSession, datagram_bytes(MTU)).expect("room"))
        .collect();
    assert_eq!(sockets.held(SocketClass::TorrentSession), kept);
    assert_eq!(
        sockets.held(SocketClass::Torrent),
        0,
        "the split gave nothing"
    );
    assert_eq!(sockets.given(SocketClass::TorrentSession), kept);

    // One more, as a speed test's session beside the route's holds, comes
    // off the split while it lives: the sides are given that much less, the
    // torrent side holds none of it, and no side holding within what it is
    // given is asked for it.
    let sides = |sockets: &SessionSockets| {
        sockets.given(SocketClass::Nntp) + sockets.given(SocketClass::Torrent)
    };
    let before = sides(&sockets);
    let past = take(&sockets, SocketClass::TorrentSession, datagram_bytes(MTU)).expect("room");
    assert_eq!(
        sockets.held(SocketClass::TorrentSession),
        kept + datagram_bytes(MTU)
    );
    assert_eq!(sockets.held(SocketClass::Torrent), 0);
    assert_eq!(sides(&sockets), before - datagram_bytes(MTU));
    assert_eq!(sockets.woken(), [false, false]);
    drop(past);
    assert_eq!(sockets.held(SocketClass::TorrentSession), kept);
    drop((listener, datagrams));
    assert_eq!(sockets.held(SocketClass::TorrentSession), 0);
}

// Nothing is handed out past the tunnel's bytes, whatever part has room:
// with the sides over theirs after a lowered budget, the reserve refuses too,
// and the side holding past what it is given owes the rest.
#[tokio::test]
async fn nothing_is_handed_out_past_the_tunnels_bytes() {
    let sockets = sockets(4, 20);
    let _held = fill(&sockets, SocketClass::Nntp);
    assert_eq!(_held.len(), 10);
    // Room for the reserve's 4 and the sides' 6.
    let mut lowered = budget(4, 0, 20);
    lowered.bytes -= 14 * peer();
    sockets.set_budget(lowered);
    assert!(take(&sockets, SocketClass::Reserve, peer()).is_none());
    assert!(take(&sockets, SocketClass::Torrent, peer()).is_none());
    assert!(take(&sockets, SocketClass::TorrentSession, peer()).is_none());
    assert_eq!(sockets.owed(SocketClass::Nntp), 7 * peer());
    assert_eq!(sockets.woken(), [true, false]);
}

// `count` connections kept between lookups, each holding a reserve stream's
// bytes as a tunnel stream does: the switch each one's closer turns, and the
// tunnel's half of each stream's bytes, which the test drops as the tunnel
// would free the socket.
struct Kept {
    _registered: Vec<crate::pipe::KeptRegistration>,
    closed: Vec<Arc<AtomicBool>>,
    tunnel: Vec<Option<SocketPermit>>,
}

impl Kept {
    fn closed(&self, at: usize) -> bool {
        self.closed[at].load(Ordering::Relaxed)
    }

    // The tunnel frees the socket of kept connection `at`.
    fn free(&mut self, at: usize) {
        self.tunnel[at] = None;
    }
}

fn kept(sockets: &SessionSockets, count: usize) -> Kept {
    kept_of(sockets, count, peer())
}

// As `kept`, each holding `bytes`.
fn kept_of(sockets: &SessionSockets, count: usize, bytes: u64) -> Kept {
    let handle = sockets.keeper().handle();
    let mut kept = Kept {
        _registered: Vec::new(),
        closed: Vec::new(),
        tunnel: Vec::new(),
    };
    for _ in 0..count {
        let mut permit = take(sockets, SocketClass::Reserve, bytes).expect("room");
        let socket = permit.reclaimable(SocketClass::Reserve);
        let (tunnel, open) = permit.for_stream();
        let closed = Arc::new(AtomicBool::new(false));
        let switch = closed.clone();
        kept._registered.push(
            handle
                .hold(
                    Some(socket),
                    Box::new(move || {
                        drop(open);
                        switch.store(true, Ordering::Relaxed);
                        true
                    }),
                )
                .expect("a live keeper"),
        );
        kept.closed.push(closed);
        kept.tunnel.push(Some(tunnel));
    }
    kept
}

// Kept connections fill the reserve: a claim of it closes the oldest, and
// is refused while its bytes are closing, as for any closing socket. Asked
// again before they are back, it closes nothing more: the bytes closing
// count as on their way. Once the tunnel frees the socket the claim takes
// the bytes, and the next claim closes the next kept connection.
#[test]
fn a_claim_on_a_full_reserve_closes_a_kept_connection_and_takes_its_bytes_once_back() {
    let sockets = sockets(2, 4);
    let mut kept = kept(&sockets, 2);
    assert_eq!(sockets.held(SocketClass::Reserve), 2 * peer());

    assert!(sockets.take(SocketClass::Reserve, peer()).is_err());
    assert!(kept.closed(0));
    assert!(!kept.closed(1));
    assert_eq!(sockets.keeper().holding(), 1);
    assert_eq!(sockets.closing(SocketClass::Reserve), peer());
    assert_eq!(sockets.held(SocketClass::Reserve), 2 * peer());

    // Closing bytes are not counted twice: nothing more closes.
    assert!(sockets.take(SocketClass::Reserve, peer()).is_err());
    assert!(sockets.wait_room(SocketClass::Reserve, peer()).is_err());
    assert!(!kept.closed(1));
    assert_eq!(sockets.keeper().holding(), 1);

    kept.free(0);
    let claim = take(&sockets, SocketClass::Reserve, peer()).expect("the kept bytes are back");
    assert!(!kept.closed(1));
    assert_eq!(sockets.held(SocketClass::Reserve), 2 * peer());

    assert!(sockets.take(SocketClass::Reserve, peer()).is_err());
    assert!(kept.closed(1));
    assert_eq!(sockets.keeper().holding(), 0);
    kept.free(1);
    let second = take(&sockets, SocketClass::Reserve, peer()).expect("back");
    assert!(sockets.take(SocketClass::Reserve, peer()).is_err());
    drop((claim, second));
}

// A look at room counts what closing a kept connection would give, and
// closes nothing. Once a claim closed it, its bytes are on their way and no
// longer room until they are back.
#[test]
fn room_closes_nothing() {
    let sockets = sockets(1, 2);
    let mut kept = kept(&sockets, 1);
    assert!(sockets.has_room(SocketClass::Reserve, peer()));
    assert!(sockets.room(SocketClass::Reserve, peer()).is_ok());
    assert!(!kept.closed(0));
    assert_eq!(sockets.keeper().holding(), 1);
    assert_eq!(sockets.closing(SocketClass::Reserve), 0);

    assert!(sockets.take(SocketClass::Reserve, peer()).is_err());
    assert!(kept.closed(0));
    assert!(!sockets.has_room(SocketClass::Reserve, peer()));
    kept.free(0);
    assert!(sockets.has_room(SocketClass::Reserve, peer()));
}

// A side's bytes are its own: a claim of a full side, or a look at it,
// closes no kept connection of the reserve, whose bytes it could not use.
#[test]
fn a_claim_closes_nothing_it_cannot_use() {
    let sockets = sockets(2, 2);
    let kept = kept(&sockets, 2);
    let usenet = fill(&sockets, SocketClass::Nntp);
    assert_eq!(usenet.len(), 1);
    assert!(!sockets.has_room(SocketClass::Nntp, peer()));
    assert!(sockets.take(SocketClass::Nntp, peer()).is_err());
    assert!(sockets.wait_room(SocketClass::Nntp, peer()).is_err());
    assert!(!kept.closed(0) && !kept.closed(1));
    assert_eq!(sockets.keeper().holding(), 2);
    // Bigger than the reserve holds: closing every kept one would not fit.
    assert!(sockets.take(SocketClass::Reserve, 3 * peer()).is_err());
    assert!(!sockets.has_room(SocketClass::Reserve, 3 * peer()));
    assert!(!kept.closed(0) && !kept.closed(1));
}

// A reserve lowered below what its kept connections hold leaves them holding
// bytes the sides now divide: a side's claim the tunnel's bytes refuse
// closes one kept connection, as many as it needs, and takes the bytes once
// back. A claim past the side's own share closes none.
#[test]
fn a_claim_closes_a_kept_connection_of_a_reserve_over_its_bytes() {
    let sockets = sockets(2, 2);
    let mut kept = kept(&sockets, 2);
    sockets.set_budget(budget(0, 0, 4));
    let usenet = fill(&sockets, SocketClass::Nntp);
    assert_eq!(usenet.len(), 2);
    assert!(!kept.closed(0));
    // Usenet holds its share: its claim closes nothing.
    assert!(sockets.take(SocketClass::Nntp, peer()).is_err());
    assert!(!kept.closed(0));

    assert!(sockets.has_room(SocketClass::Torrent, peer()));
    assert!(!kept.closed(0));
    assert!(sockets.take(SocketClass::Torrent, peer()).is_err());
    assert!(kept.closed(0));
    assert!(!kept.closed(1));
    kept.free(0);
    let first = take(&sockets, SocketClass::Torrent, peer()).expect("back");
    assert!(sockets.take(SocketClass::Torrent, peer()).is_err());
    assert!(kept.closed(1));
    kept.free(1);
    let second = take(&sockets, SocketClass::Torrent, peer()).expect("back");
    assert!(sockets.take(SocketClass::Torrent, peer()).is_err());
    drop((usenet, first, second));
}

// A keeper cut closes everything it keeps; the bytes come back as the
// tunnel frees each socket.
#[test]
fn a_cut_closes_what_is_kept() {
    let sockets = sockets(2, 2);
    let mut kept = kept(&sockets, 2);
    sockets.keeper().cut();
    assert!(kept.closed(0) && kept.closed(1));
    assert_eq!(sockets.keeper().holding(), 0);
    assert_eq!(sockets.closing(SocketClass::Reserve), 2 * peer());
    kept.free(0);
    kept.free(1);
    assert_eq!(sockets.held(SocketClass::Reserve), 0);
}

// A waiter for room is not parked behind a kept connection nobody closes:
// its wait closes the kept connection, and it is woken as the tunnel frees
// the socket.
#[tokio::test]
async fn a_waiter_is_woken_when_a_kept_connections_bytes_come_back() {
    let sockets = sockets(1, 2);
    let mut kept = kept(&sockets, 1);
    let Err(Refused { freed, demand, .. }) = sockets.wait_room(SocketClass::Reserve, peer()) else {
        panic!("room while the kept connection's bytes are closing");
    };
    assert!(demand.is_none());
    assert!(kept.closed(0));
    let notify = freed.notify();
    let woken = notify.notified();
    tokio::pin!(woken);
    woken.as_mut().enable();
    kept.free(0);
    woken.await;
    assert!(sockets.wait_room(SocketClass::Reserve, peer()).is_ok());
}

// Each kept connection is reclaimed at the bytes its socket reserved: one
// holding two peer sockets' bytes is enough for a claim of two, and closing
// it alone makes room.
#[test]
fn a_kept_connection_is_reclaimed_at_the_bytes_it_reserved() {
    let sockets = sockets(4, 2);
    let mut kept = kept_of(&sockets, 1, 2 * peer());
    let others: Vec<_> = (0..2)
        .map(|_| take(&sockets, SocketClass::Reserve, peer()).expect("room"))
        .collect();
    assert!(!kept.closed(0));
    assert_eq!(
        sockets.keeper().held(),
        [(SocketClass::Reserve, 2 * peer())]
    );
    assert!(sockets.has_room(SocketClass::Reserve, 2 * peer()));
    assert!(!sockets.has_room(SocketClass::Reserve, 3 * peer()));
    assert!(sockets.take(SocketClass::Reserve, 2 * peer()).is_err());
    assert!(kept.closed(0));
    assert_eq!(sockets.closing(SocketClass::Reserve), 2 * peer());
    kept.free(0);
    assert!(take(&sockets, SocketClass::Reserve, 2 * peer()).is_some());
    drop(others);
}

// A claim refused only for the bytes of kept connections closed to make room
// waits for them rather than fail: refused once by a claim that closed the
// kept connection, the next claim waits too, and takes the bytes as the
// tunnel frees the socket. Kept, the connection made nothing fail.
#[tokio::test]
async fn a_claim_on_a_reserve_full_of_kept_connections_waits_for_their_bytes() {
    let sockets = sockets(1, 2);
    let mut kept = kept(&sockets, 1);
    // A claim closes the kept connection; its bytes are closing.
    let Err(refused) = sockets.take(SocketClass::Reserve, peer()) else {
        panic!("room while the kept connection's bytes are closing");
    };
    assert!(refused.reclaiming);
    assert!(kept.closed(0));
    let (claim, ()) = tokio::join!(sockets.claim(SocketClass::Reserve, peer()), async {
        // The claim, polled first, is waiting: the tunnel frees the socket.
        assert_eq!(sockets.closing(SocketClass::Reserve), peer());
        kept.free(0);
    });
    let claim = claim.expect("the kept bytes are back").expect("a budget");
    assert_eq!(sockets.held(SocketClass::Reserve), peer());
    drop(claim);

    // With nothing refused, a claim on a reserve kept full closes the kept
    // connection itself and waits for its bytes.
    let mut kept = kept_of(&sockets, 1, peer());
    let (claim, ()) = tokio::join!(sockets.claim(SocketClass::Reserve, peer()), async {
        assert!(kept.closed(0));
        kept.free(0);
    });
    assert!(claim.expect("the kept bytes are back").is_some());
}

// A reserve full of connections in use has nothing to reclaim: a claim is
// refused at once, as before. So is one past what closing every kept
// connection would give.
#[tokio::test]
async fn a_claim_on_a_reserve_full_of_connections_in_use_is_refused_at_once() {
    let sockets = sockets(1, 2);
    let in_use = take(&sockets, SocketClass::Reserve, peer()).expect("room");
    let Err(refused) = sockets.claim(SocketClass::Reserve, peer()).await else {
        panic!("room in a full reserve");
    };
    assert!(!refused.reclaiming);
    drop(in_use);

    let kept = kept(&sockets, 1);
    assert!(
        sockets
            .claim(SocketClass::Reserve, 2 * peer())
            .await
            .is_err()
    );
    assert!(!kept.closed(0));
}

// A waiter refused while the reserve's connections are in use is woken when
// one of them becomes kept: it looks again, closes it, and gets its bytes
// once the tunnel frees the socket.
#[tokio::test]
async fn a_waiter_refused_while_connections_are_in_use_is_woken_when_they_are_kept() {
    let sockets = sockets(1, 2);
    let mut permit = take(&sockets, SocketClass::Reserve, peer()).expect("room");
    let socket = permit.reclaimable(SocketClass::Reserve);
    let (tunnel, open) = permit.for_stream();

    let Err(Refused { freed, .. }) = sockets.wait_room(SocketClass::Reserve, peer()) else {
        panic!("room in a full reserve");
    };
    let notify = freed.notify();
    let woken = notify.notified();
    tokio::pin!(woken);
    woken.as_mut().enable();

    // The lookup ends and keeps its connection.
    let closed = Arc::new(AtomicBool::new(false));
    let switch = closed.clone();
    let _kept = sockets
        .keeper()
        .handle()
        .hold(
            Some(socket),
            Box::new(move || {
                drop(open);
                switch.store(true, Ordering::Relaxed);
                true
            }),
        )
        .expect("a live keeper");
    woken.await;

    let woken = notify.notified();
    tokio::pin!(woken);
    woken.as_mut().enable();
    assert!(sockets.wait_room(SocketClass::Reserve, peer()).is_err());
    assert!(closed.load(Ordering::Relaxed));
    drop(tunnel);
    woken.await;
    assert!(sockets.wait_room(SocketClass::Reserve, peer()).is_ok());
    assert!(take(&sockets, SocketClass::Reserve, peer()).is_some());
}
