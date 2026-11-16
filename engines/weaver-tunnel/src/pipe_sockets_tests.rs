// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The ledger's mechanics: lingers, timers, wakes and the parts kept off the
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

fn paused() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .start_paused(true)
        .build()
        .unwrap()
}

// A dropped stream's bytes come back after the tunnel's linger, and not a
// moment before; a datagram socket's, a listener's and those of a connect
// that never completed come back at once.
#[tokio::test(start_paused = true)]
async fn bytes_come_back_when_the_tunnel_frees_the_socket() {
    let sockets = SessionSockets::new(budget(4, listener_bytes(), 40), Arc::new(Halves));
    let stream = take(&sockets, SocketClass::Nntp, peer()).expect("room");
    drop(stream.lingering());
    assert_eq!(sockets.held(SocketClass::Nntp), peer());
    assert_eq!(sockets.lingering(SocketClass::Nntp), peer());
    tokio::time::advance(TUNNEL_LINGER - Duration::from_millis(1)).await;
    tokio::task::yield_now().await;
    assert_eq!(
        sockets.held(SocketClass::Nntp),
        peer(),
        "back before the linger"
    );
    tokio::time::advance(Duration::from_millis(1)).await;
    while sockets.held(SocketClass::Nntp) != 0 {
        tokio::task::yield_now().await;
    }
    assert_eq!(sockets.lingering(SocketClass::Nntp), 0);

    for (class, bytes) in [
        (SocketClass::Torrent, datagram_bytes(MTU)),
        (SocketClass::TorrentSession, listener_bytes()),
        // A connect that never completed holds no stream to linger.
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
#[tokio::test(start_paused = true)]
async fn a_refused_dial_wakes_when_bytes_come_back() {
    let sockets = sockets(4, 20);
    let held = fill(&sockets, SocketClass::Torrent);
    assert_eq!(held.len(), 10);
    let Err(Refused { open, freed, .. }) = sockets.take(SocketClass::Torrent, peer()) else {
        panic!("a full tunnel handed out bytes");
    };
    assert_eq!(open, 10);
    let notify = freed.notify();
    let woken = notify.notified();
    tokio::pin!(woken);
    woken.as_mut().enable();
    drop(held.into_iter().next().map(SocketPermit::lingering));
    // The wake comes with the bytes, after the linger.
    woken.await;
    assert!(sockets.has_room(SocketClass::Torrent, peer()));
    assert!(take(&sockets, SocketClass::Torrent, peer()).is_some());
}

// The linger's one timer runs only while something lingers: none before,
// and none once the last lingering stream is back, however long after.
#[tokio::test(start_paused = true)]
async fn the_linger_timer_is_silent_when_nothing_lingers() {
    let sockets = sockets(4, 10);
    let before = alive_tasks();
    let datagram = take(&sockets, SocketClass::Torrent, datagram_bytes(MTU)).expect("room");
    drop(datagram);
    assert_eq!(alive_tasks(), before, "a datagram socket started a timer");
    let streams = fill(&sockets, SocketClass::Nntp);
    for stream in streams {
        drop(stream.lingering());
    }
    assert_eq!(alive_tasks(), before + 1, "one timer for the whole tunnel");
    tokio::time::advance(TUNNEL_LINGER).await;
    while sockets.held(SocketClass::Nntp) != 0 {
        tokio::task::yield_now().await;
    }
    while alive_tasks() != before {
        tokio::task::yield_now().await;
    }
    let shares = sockets.shares.as_ref().expect("a budget");
    assert!(!shares.ledger().timer);
    tokio::time::advance(10 * TUNNEL_LINGER).await;
    tokio::task::yield_now().await;
    assert_eq!(alive_tasks(), before);
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

// A stream dropped off any runtime still waits out the linger. The timer
// cancelled with its runtime starts again on the next look, and the bytes
// come back after the linger.
#[test]
fn a_linger_outlives_its_runtime_and_a_dropped_off_a_runtime_stream() {
    let sockets = sockets(4, 20);
    let off = take(&sockets, SocketClass::Nntp, peer()).expect("room");
    drop(off.lingering());
    assert_eq!(sockets.lingering(SocketClass::Nntp), peer(), "freed early");

    // Lingering under a runtime that ends before the linger does.
    let first = paused();
    first.block_on(async {
        let stream = take(&sockets, SocketClass::Nntp, peer()).expect("room");
        drop(stream.lingering());
        assert!(sockets.shares.as_ref().unwrap().ledger().timer);
    });
    drop(first);
    let shares = sockets.shares.as_ref().expect("a budget");
    assert!(!shares.ledger().timer, "a cancelled timer left its mark");
    assert_eq!(sockets.lingering(SocketClass::Nntp), 2 * peer());

    paused().block_on(async {
        assert!(sockets.has_room(SocketClass::Nntp, peer()));
        assert!(shares.ledger().timer, "the look started the timer");
        tokio::time::advance(TUNNEL_LINGER).await;
        while sockets.held(SocketClass::Nntp) != 0 {
            tokio::task::yield_now().await;
        }
    });
}
