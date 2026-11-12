// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A peer that keeps libtorrent's receive rules (v2.0.15): it holds at most
// `capacity` bytes, in order and out of order together, and drops any data
// packet that does not fit, the next one in order included
// (utp_stream.cpp:2364-2377, 2384-2389, 2443-2448). It advertises what is
// left (1781-1783) and acknowledges every data packet it is given, kept or
// not (2994-3006). Datagrams may also be lost before the peer sees them.
// Driven on the mock clock, one step at a time; everything is counted.

use std::{
    collections::{BTreeMap, VecDeque},
    sync::atomic::Ordering,
    task::Poll,
    time::Duration,
};

use tokio::{io::AsyncWriteExt, time::Instant};

use crate::{
    Error, SocketOpts,
    raw::{Type::*, UtpHeader, selective_ack::SelectiveAck},
    rtte::RttEstimator,
    stream_dispatch::{
        SendTotals,
        tests::{TestVsock, calc_mtu_for_mss, make_test_vsock},
    },
    test_util::setup_test_logging,
    traits::UtpEnvironment,
};

const MSS: usize = 1000;
const ONE_WAY: Duration = Duration::from_micros(500);
const STEP: Duration = Duration::from_micros(100);
const MIN_TIMEOUT: Duration = Duration::from_millis(500);
// The sequence number of the first data packet a test stream sends.
const FIRST: u16 = 101;

// Datagrams lost before the peer reads them: a fixed pseudo-random draw,
// so every run loses the same ones.
struct Loss {
    state: u64,
    per_mille: u64,
}

impl Loss {
    fn lost(&mut self) -> bool {
        self.state = self
            .state
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        (self.state >> 33) % 1000 < self.per_mille
    }
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
struct PeerCounters {
    // Data packets lost on the way, and those the peer dropped for want of
    // room.
    lost_on_path: u64,
    dropped_full: u64,
    // Bytes the peer took in order.
    delivered: usize,
}

struct Peer {
    ack_nr: u16,
    held: BTreeMap<u16, usize>,
    held_bytes: usize,
    capacity: usize,
    // A data packet, and how many of its arrivals are lost on the way.
    drop: (u16, usize),
    loss: Option<Loss>,
    counters: PeerCounters,
}

impl Peer {
    fn new(capacity: usize) -> Self {
        Self {
            ack_nr: FIRST - 1,
            held: BTreeMap::new(),
            held_bytes: 0,
            capacity,
            drop: (FIRST, 0),
            loss: None,
            counters: PeerCounters::default(),
        }
    }

    fn window(&self) -> u32 {
        (self.capacity - self.held_bytes) as u32
    }

    // The ACK for a data packet that arrived, or none when it was lost on
    // the way. The application reads all it is given at once.
    fn on_data(&mut self, seq_nr: u16, len: usize) -> Option<UtpHeader> {
        if seq_nr == self.drop.0 && self.drop.1 > 0 {
            self.drop.1 -= 1;
            self.counters.lost_on_path += 1;
            return None;
        }
        if self.loss.as_mut().is_some_and(Loss::lost) {
            self.counters.lost_on_path += 1;
            return None;
        }
        let ahead = seq_nr.wrapping_sub(self.ack_nr);
        if ahead == 0 || ahead >= u16::MAX / 2 || self.held.contains_key(&seq_nr) {
            // Already held or delivered.
        } else if self.held_bytes + len > self.capacity {
            self.counters.dropped_full += 1;
        } else if ahead == 1 {
            self.ack_nr = seq_nr;
            self.counters.delivered += len;
            while let Some(len) = self.held.remove(&self.ack_nr.wrapping_add(1)) {
                self.ack_nr = self.ack_nr.wrapping_add(1);
                self.held_bytes -= len;
                self.counters.delivered += len;
            }
        } else {
            self.held.insert(seq_nr, len);
            self.held_bytes += len;
        }
        let sacked = self
            .held
            .keys()
            .map(|seq| usize::from(seq.wrapping_sub(self.ack_nr).wrapping_sub(2)))
            .filter(|bit| *bit < 255 * 8);
        Some(UtpHeader {
            htype: State,
            seq_nr: 1.into(),
            ack_nr: self.ack_nr.into(),
            wnd_size: self.window(),
            // A path with no queue: every delay sample is its base.
            timestamp_microseconds: 1,
            timestamp_difference_microseconds: ONE_WAY.as_micros() as u32,
            extensions: crate::raw::Extensions {
                selective_ack: SelectiveAck::new_test(sacked),
                ..Default::default()
            },
            ..Default::default()
        })
    }
}

#[derive(Debug)]
struct Run {
    // `None` when every byte arrived.
    error: Option<Error>,
    // From the first send to the end.
    took: Duration,
    peer: PeerCounters,
    // When each send of the first data packet left.
    first_sends: Vec<Duration>,
    // The stream's own totals, as its closing line logs them.
    totals: SendTotals,
}

async fn stream_to(peer: &mut Peer, bytes: usize, rtt_sample: Duration) -> Run {
    setup_test_logging();
    let mut t = make_test_vsock(
        SocketOpts {
            disable_nagle: true,
            link_mtu: Some(calc_mtu_for_mss(MSS)),
            min_timeout: Some(MIN_TIMEOUT),
            vsock_tx_bufsize_bytes_initial: std::num::NonZeroUsize::new(bytes),
            ..Default::default()
        },
        false,
    );
    t.vsock.rtte = RttEstimator::new(MIN_TIMEOUT);
    t.vsock.rtte.confirm();
    t.vsock.rtte.sample(rtt_sample);
    t.stream
        .as_mut()
        .unwrap()
        .write_all(&vec![7u8; bytes])
        .await
        .unwrap();
    drive(&mut t, peer, bytes).await
}

async fn drive(t: &mut TestVsock, peer: &mut Peer, bytes: usize) -> Run {
    let start = t.env.now();
    let mut to_peer: VecDeque<(Instant, u16, usize)> = VecDeque::new();
    let mut to_us: VecDeque<(Instant, UtpHeader)> = VecDeque::new();
    let mut first_sends = Vec::new();
    let error = loop {
        let now = t.env.now();
        while to_peer.front().is_some_and(|(at, ..)| *at <= now) {
            let (_, seq_nr, len) = to_peer.pop_front().unwrap();
            if let Some(ack) = peer.on_data(seq_nr, len) {
                to_us.push_back((now + ONE_WAY, ack));
            }
        }
        while to_us.front().is_some_and(|(at, _)| *at <= now) {
            let (_, ack) = to_us.pop_front().unwrap();
            t.send_msg(ack, "");
        }
        if peer.counters.delivered == bytes {
            break None;
        }
        match t.poll_once().await {
            Poll::Ready(result) => break Some(result.unwrap_err()),
            Poll::Pending => {}
        }
        for msg in t.take_sent() {
            if msg.header.get_type() == Data {
                if msg.header.seq_nr.0 == FIRST {
                    first_sends.push(now - start);
                }
                to_peer.push_back((now + ONE_WAY, msg.header.seq_nr.0, msg.payload().len()));
            }
        }
        t.env.increment_now(STEP);
        // The test task polls the stream without ever returning to the
        // runtime; a yield each step renews tokio's cooperative budget, so
        // the stream's ingress channel keeps handing over what arrived.
        tokio::task::yield_now().await;
    };
    let stats = &t.vsock.socket.stats;
    let totals = t.vsock.totals;
    // One stream on the socket: its totals are the socket's.
    assert_eq!(
        totals.packets_sent,
        stats.send_count.load(Ordering::Relaxed)
            + stats.data_retransmissions.load(Ordering::Relaxed)
    );
    assert_eq!(
        totals.timer_expiries,
        stats.timeouts.load(Ordering::Relaxed)
    );
    assert_eq!(
        totals.fast_resends,
        stats
            .recovery_retransmitted_segments_count
            .load(Ordering::Relaxed)
    );
    Run {
        error,
        took: t.env.now() - start,
        peer: peer.counters,
        first_sends,
        totals,
    }
}

// A peer with room for four packets loses the first one on the way `n`
// times and takes it after; the three behind it wait in its buffer. The
// first resend follows the duplicate ACKs, the next the timer. Twice lost,
// the third send still finds room and the stream goes on. Three times lost,
// the stream ends at the next timeout: three sends are all a packet gets,
// as in libtorrent, which counts every send (utp_stream.cpp:1864, 2091) and
// ends the stream at the timeout that finds the oldest packet sent three
// times (3523). Both back the timer off per timeout, 500 ms and then 1.5 s
// (3379-3383, 3435), so a peer that stays silent ends the stream 2 s after
// the first send, here and in libtorrent.
#[tokio::test]
async fn a_peer_that_loses_the_head_three_times_ends_the_stream() {
    let mut survived = Peer::new(4 * MSS);
    survived.drop = (FIRST, 2);
    let run = stream_to(&mut survived, 8 * MSS, Duration::from_millis(1)).await;
    eprintln!("lost twice: {run:?}");
    assert!(run.error.is_none(), "{run:?}");
    assert_eq!(run.peer.dropped_full, 0, "{run:?}");
    assert_eq!(run.peer.delivered, 8 * MSS, "{run:?}");
    assert_eq!(run.first_sends.len(), 3, "{run:?}");
    assert_eq!(
        run.totals,
        SendTotals {
            packets_sent: 12,
            fast_resends: 1,
            tail_probes: 0,
            timer_expiries: 1,
        },
        "{run:?}"
    );

    let mut died = Peer::new(4 * MSS);
    died.drop = (FIRST, 3);
    let run = stream_to(&mut died, 8 * MSS, Duration::from_millis(1)).await;
    eprintln!("lost three times: {run:?}");
    assert!(
        matches!(run.error, Some(Error::MaxRetransmissionsReached)),
        "{run:?}"
    );
    assert_eq!(run.peer.dropped_full, 0, "{run:?}");
    // Four first sends, the fast resend, the timer's resend, and the
    // timeout that ends the stream.
    assert_eq!(
        run.totals,
        SendTotals {
            packets_sent: 7,
            fast_resends: 1,
            tail_probes: 0,
            timer_expiries: 2,
        },
        "{run:?}"
    );
    assert_eq!(
        run.first_sends,
        [
            Duration::ZERO,
            Duration::from_millis(2),
            Duration::from_millis(503)
        ],
        "{run:?}"
    );
    // The timer that found the third send unanswered: 1.5 s after it.
    assert_eq!(
        run.took,
        Duration::from_millis(503) + Duration::from_millis(1500),
        "{run:?}"
    );
}

// A peer with room for 64 KiB, one datagram in eight lost on the way at
// random: a sender that keeps every unacknowledged byte inside the window
// the peer advertised never sends a packet the peer has no room for.
#[tokio::test]
async fn a_sender_inside_the_advertised_window_is_never_dropped_for_room() {
    let mut peer = Peer::new(64 * 1024);
    peer.loss = Some(Loss {
        state: 7,
        per_mille: 120,
    });
    let run = stream_to(&mut peer, 2 * 1024 * 1024, Duration::from_millis(1)).await;
    eprintln!("12% loss, 64 KiB peer: {run:?}");
    assert!(run.error.is_none(), "{run:?}");
    assert_eq!(run.peer.delivered, 2 * 1024 * 1024, "{run:?}");
    assert_eq!(run.peer.dropped_full, 0, "{run:?}");
    // Every resend that a later send's delivery outran goes again a round
    // trip and a quarter on, without the timer; one timeout is left.
    assert_eq!(run.totals.fast_resends, 265, "{run:?}");
    assert_eq!(run.totals.timer_expiries, 1, "{run:?}");
}

// 8000 bytes leave in ten packets, 101 to 110: slow start cuts the first
// ones short. The last is lost once on the way. Nothing sent after it can
// draw duplicate ACKs, so two round trips on, the tail goes again as a
// probe, and the stream ends with no timer expiry and no fast resend.
#[tokio::test]
async fn a_lost_tail_is_resent_by_one_probe() {
    let mut peer = Peer::new(64 * 1024);
    peer.drop = (FIRST + 9, 1);
    let run = stream_to(&mut peer, 8 * MSS, Duration::from_millis(1)).await;
    eprintln!("tail lost once: {run:?}");
    assert!(run.error.is_none(), "{run:?}");
    assert_eq!(run.peer.delivered, 8 * MSS, "{run:?}");
    assert_eq!(
        run.totals,
        SendTotals {
            packets_sent: 11,
            fast_resends: 0,
            tail_probes: 1,
            timer_expiries: 0,
        },
        "{run:?}"
    );
}

// The third packet from the end is lost once. The two after it arrive, so
// the peer's ACKs show it missing, but two later packets are not the three
// the SACK rule needs, and nothing new is left to send. The probe is not
// due: the tail itself arrived. The retransmission timer resends the hole.
// libtorrent waits for the timer here too: it resends from a SACK only when
// more than three packets after the hole are acknowledged
// (utp_stream.cpp:1290-1355), from duplicate ACKs only after three
// (2814-2834), and has no tail probe.
#[tokio::test]
async fn a_hole_with_two_packets_behind_it_waits_for_the_timer() {
    let mut peer = Peer::new(64 * 1024);
    peer.drop = (FIRST + 7, 1);
    let run = stream_to(&mut peer, 8 * MSS, Duration::from_millis(1)).await;
    eprintln!("third from last lost once: {run:?}");
    assert!(run.error.is_none(), "{run:?}");
    assert_eq!(run.peer.delivered, 8 * MSS, "{run:?}");
    assert_eq!(run.totals.tail_probes, 0, "{run:?}");
    assert_eq!(run.totals.fast_resends, 0, "{run:?}");
    assert_eq!(run.totals.timer_expiries, 1, "{run:?}");
}
