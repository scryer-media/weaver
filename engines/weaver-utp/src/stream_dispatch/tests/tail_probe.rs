// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The tail-loss probe: a flight's tail unacknowledged for two round trips,
// with nothing new to send, is resent once (RFC 8985 section 7). The probe
// leaves the window and the count toward giving up as they are.

use std::{sync::atomic::Ordering, task::Poll, time::Duration};

use tokio::io::AsyncWriteExt;

use crate::{
    Error, SocketOpts,
    raw::{Type::*, UtpHeader},
    rtte::RttEstimator,
    stream_dispatch::tests::{TestVsock, calc_mtu_for_mss, make_test_vsock},
    test_util::setup_test_logging,
};

const ROUND_TRIP: Duration = Duration::from_millis(50);
const MIN_TIMEOUT: Duration = Duration::from_millis(500);

// A stream of four 100-byte packets in flight, 101 to 104. With `measured`,
// the round trip is a measured 50 ms; without, no round trip was measured.
// The timer waits 500 ms either way.
async fn four_in_flight(measured: bool) -> TestVsock {
    setup_test_logging();
    let mut t = make_test_vsock(
        SocketOpts {
            disable_nagle: true,
            link_mtu: Some(calc_mtu_for_mss(100)),
            min_timeout: Some(MIN_TIMEOUT),
            ..Default::default()
        },
        false,
    );
    t.vsock.rtte = RttEstimator::new(MIN_TIMEOUT);
    t.vsock.rtte.confirm();
    if measured {
        t.vsock.rtte.sample(ROUND_TRIP);
    }
    t.stream
        .as_mut()
        .unwrap()
        .write_all(&[7u8; 400])
        .await
        .unwrap();
    let sent = t.poll_until_sent(4).await;
    let seq_nrs: Vec<u16> = sent.iter().map(|m| m.header.seq_nr.0).collect();
    assert_eq!(seq_nrs, [101, 102, 103, 104]);
    t
}

fn ack(t: &mut TestVsock, ack_nr: u16) {
    t.send_msg(
        UtpHeader {
            htype: State,
            seq_nr: 1.into(),
            ack_nr: ack_nr.into(),
            wnd_size: 1024 * 1024,
            ..Default::default()
        },
        "",
    );
}

fn rtos(t: &TestVsock) -> u64 {
    t.vsock
        .socket
        .stats
        .rto_timeouts_count
        .load(Ordering::Relaxed)
}

// Steps the clock 1 ms a pass until a data packet goes out, and returns the
// sequence numbers sent and the time stepped.
async fn step_until_sent(t: &mut TestVsock) -> (Vec<u16>, Duration) {
    let mut waited = Duration::ZERO;
    loop {
        t.env.increment_now(Duration::from_millis(1));
        waited += Duration::from_millis(1);
        t.poll_once_assert_pending().await;
        let sent: Vec<u16> = t
            .take_sent()
            .iter()
            .filter(|m| m.header.get_type() == Data)
            .map(|m| m.header.seq_nr.0)
            .collect();
        if !sent.is_empty() {
            return (sent, waited);
        }
    }
}

#[tokio::test]
async fn a_lost_tail_is_resent_before_the_timer() {
    let mut t = four_in_flight(true).await;
    let window = t.vsock.congestion_controller.window();

    // 101 to 103 arrive; 104, the tail, is lost.
    t.env.increment_now(ROUND_TRIP);
    ack(&mut t, 103);
    t.poll_once_assert_pending().await;
    t.take_sent();

    // Two round trips and 200 ms after the last ACK, as it is the one packet
    // out and its ACK may come late, the tail goes again.
    let (sent, waited) = step_until_sent(&mut t).await;
    assert_eq!(sent, [104]);
    assert_eq!(waited, ROUND_TRIP * 2 + Duration::from_millis(200));
    assert_eq!(rtos(&t), 0);
    assert_eq!(t.vsock.congestion_controller.window(), window);

    // Its ACK ends the flight: no timer runs, none fired.
    t.env.increment_now(ROUND_TRIP);
    ack(&mut t, 104);
    t.poll_once_assert_pending().await;
    assert!(t.vsock.user_tx_segments.is_empty());
    assert_eq!(t.vsock.timers.retransmit, super::super::Timer::Idle);
    assert_eq!(t.vsock.timers.tail_probe, super::super::Timer::Idle);
    assert_eq!(rtos(&t), 0);
}

#[tokio::test]
async fn a_tail_merely_late_costs_one_duplicate() {
    let mut t = four_in_flight(true).await;
    let window = t.vsock.congestion_controller.window();
    let resends = || {
        t.vsock
            .socket
            .stats
            .data_retransmissions
            .load(Ordering::Relaxed)
    };
    assert_eq!(resends(), 0);

    t.env.increment_now(ROUND_TRIP);
    ack(&mut t, 103);
    t.poll_once_assert_pending().await;
    let (sent, _) = step_until_sent(&mut t).await;
    assert_eq!(sent, [104]);

    // The tail's own ACK arrives: the probe was a duplicate, and nothing
    // else changed.
    ack(&mut t, 104);
    t.poll_once_assert_pending().await;
    assert!(t.take_sent().iter().all(|m| m.header.get_type() != Data));
    assert!(t.vsock.user_tx_segments.is_empty());
    assert_eq!(
        t.vsock
            .socket
            .stats
            .data_retransmissions
            .load(Ordering::Relaxed),
        1
    );
    assert_eq!(rtos(&t), 0);
    assert!(!t.vsock.recovery.is_recovering());
    assert_eq!(t.vsock.congestion_controller.window(), window);
}

#[tokio::test]
async fn one_probe_per_flight_and_it_does_not_count_toward_giving_up() {
    let mut t = four_in_flight(true).await;
    t.env.increment_now(ROUND_TRIP);
    ack(&mut t, 103);
    t.poll_once_assert_pending().await;
    let (sent, _) = step_until_sent(&mut t).await;
    assert_eq!(sent, [104]);

    let tail = t
        .vsock
        .user_tx_segments
        .iter_mut_for_sending(Some(104.into()))
        .next()
        .unwrap();
    assert_eq!(tail.send_count(), 2);
    assert_eq!(tail.counted_sends(), 1, "the probe is not counted");

    // No second probe: the next send of 104 is the timer's.
    let (sent, _) = step_until_sent(&mut t).await;
    assert_eq!(sent, [104]);
    assert_eq!(rtos(&t), 1);
    let tail = t
        .vsock
        .user_tx_segments
        .iter_mut_for_sending(Some(104.into()))
        .next()
        .unwrap();
    assert_eq!(tail.send_count(), 3);
    assert_eq!(tail.counted_sends(), 2);
}

// Steps the clock 1 ms a pass until the stream gives up, and returns its
// error, the timer expiries and the data packets sent.
async fn run_to_death(t: &mut TestVsock) -> (Error, u64, usize) {
    let mut sent = 4;
    loop {
        t.env.increment_now(Duration::from_millis(1));
        match t.poll_once().await {
            Poll::Pending => {
                sent += t
                    .take_sent()
                    .iter()
                    .filter(|m| m.header.get_type() == Data)
                    .count();
            }
            Poll::Ready(result) => return (result.unwrap_err(), rtos(t), sent),
        }
    }
}

#[tokio::test]
async fn a_dead_path_gives_up_after_the_same_timer_expiries() {
    // Without a measured round trip no probe goes out.
    let mut control = four_in_flight(false).await;
    let (control_error, control_rtos, control_sent) = run_to_death(&mut control).await;

    let mut probed = four_in_flight(true).await;
    let (error, expiries, sent) = run_to_death(&mut probed).await;

    assert_eq!(
        std::mem::discriminant(&error),
        std::mem::discriminant(&control_error),
        "{error:?} against {control_error:?}"
    );
    assert_eq!(expiries, control_rtos);
    assert_eq!(sent, control_sent + 1, "one probe more");
}
