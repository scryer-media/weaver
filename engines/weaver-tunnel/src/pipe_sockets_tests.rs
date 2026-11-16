// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The ledger's mechanics: closing streams, wakes and the parts kept off the
// split. How the sides divide and give back is tested on the bandwidth
// split's own rule, in the server's `socket_ledger_tests`.

use std::sync::Arc;

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
