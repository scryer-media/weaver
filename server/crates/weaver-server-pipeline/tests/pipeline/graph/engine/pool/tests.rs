// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::*;

const MIB: u64 = 1 << 20;
const GIB: u64 = 1 << 30;

fn run(memory: Option<u64>, window: Option<u64>) -> Budget {
    Budget {
        memory_bytes: memory,
        window_bytes: window,
        ..Default::default()
    }
}

// The pool fits one run at the ceiling the profile grants, the wider of
// its memory and its window, and the run's overhead. Runs granted no
// ceiling share a pool with no limit.
#[test]
fn the_pool_fits_one_decode_at_the_profiles_ceiling() {
    let room = |budget| DecodePool::for_run(&budget).room().limit;
    assert_eq!(
        room(run(Some(512 * MIB), Some(256 * MIB))),
        512 * MIB + RUN_OVERHEAD
    );
    assert_eq!(room(run(Some(GIB), Some(4 * GIB))), 4 * GIB + RUN_OVERHEAD);
    assert_eq!(room(run(None, None)), u64::MAX);
}

// Runs share the pool up to its limit; a release makes room again, and a
// resize leaves what is reserved where it stands.
#[test]
fn reservations_share_the_pool_and_a_resize_keeps_them() {
    let pool = DecodePool::new(10);
    assert!(pool.reserve(6));
    assert!(!pool.reserve(5), "it does not fit beside the first");
    assert!(pool.reserve(4));
    pool.set_limit(3);
    assert_eq!(
        pool.room(),
        Room {
            reserved: 10,
            limit: 3,
            above: 0,
            freed: 0,
            whole: 0,
        }
    );
    pool.release(6);
    pool.release(4);
    assert!(pool.reserve(3));
    assert!(!pool.reserve(1));
    assert!(!pool.reserve(u64::MAX), "an overflow never fits");
}

// A reservation above the ceiling starts at once beside the pool's
// runs while it, the others above it, and the whole pool fit under the
// hard stop; the pool's runs share the pool beside it as before.
#[test]
fn a_reservation_above_the_ceiling_starts_beside_the_pool_under_the_hard_stop() {
    let pool = DecodePool::new(10);
    assert!(pool.reserve(4));
    let mut waiting = false;
    assert_eq!(pool.take_above(25, 60, &mut waiting), Ok(()));
    assert!(!waiting);
    assert!(pool.reserve(6), "the pool is shared beside it as before");
    assert!(!pool.reserve(1));
    assert_eq!(
        pool.room(),
        Room {
            reserved: 10,
            limit: 10,
            above: 25,
            freed: 0,
            whole: 0,
        }
    );
    // A second above the ceiling waits for the first: 10 + 25 + 30 is
    // over 60. It holds no one in the pool back.
    let mut second = false;
    let turned = pool.take_above(30, 60, &mut second).unwrap_err();
    assert!(second);
    assert_eq!(turned.whole, 1);
    pool.release(4);
    assert!(pool.reserve(4), "the pool's runs never wait on it");
    // A third that would fit waits behind the second.
    let mut third = false;
    assert!(pool.take_above(1, 60, &mut third).is_err());
    pool.release_above(25);
    assert!(pool.room().freed > turned.freed, "the release wakes it");
    assert_eq!(pool.take_above(30, 60, &mut second), Ok(()));
    assert!(!second);
    assert_eq!(pool.take_above(1, 60, &mut third), Ok(()));
    assert_eq!(pool.room().above, 31);
    assert_eq!(pool.room().whole, 0);
    pool.release_above(30);
    pool.release_above(1);
    assert_eq!(pool.room().above, 0);
    assert!(pool.could_fit_above(50, 60));
    assert!(!pool.could_fit_above(51, 60), "never beside the whole pool");
}

// A run that stops waiting above the ceiling lets the others in again,
// and wakes those it turned away.
#[test]
fn a_run_that_stops_waiting_above_the_ceiling_wakes_the_others() {
    let pool = DecodePool::new(10);
    let mut first = false;
    assert_eq!(pool.take_above(40, 60, &mut first), Ok(()));
    let mut waiting = false;
    assert!(pool.take_above(25, 60, &mut waiting).is_err());
    let mut behind = false;
    let turned = pool.take_above(5, 60, &mut behind).unwrap_err();
    pool.stop_waiting(&mut waiting);
    assert!(!waiting);
    assert!(pool.room().freed > turned.freed);
    assert_eq!(pool.take_above(5, 60, &mut behind), Ok(()));
    pool.stop_waiting(&mut waiting);
    assert_eq!(pool.room().whole, 0, "stopping twice counts once");
}
