// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::DecodeClock;
use std::time::Duration;

const SECOND: Duration = Duration::from_secs(1);

// Nothing is spent before a decoder executes, while it waits, or after
// it is done.
#[tokio::test(start_paused = true)]
async fn only_the_time_a_decoder_executes_is_spent() {
    let clock = DecodeClock::default();
    tokio::time::advance(SECOND * 10).await;
    assert_eq!(clock.spent(), Duration::ZERO);

    let decoding = clock.decoding();
    tokio::time::advance(SECOND * 2).await;
    {
        let _waiting = decoding.wait();
        let _again = decoding.wait();
        tokio::time::advance(SECOND * 100).await;
    }
    assert_eq!(clock.spent(), SECOND * 2);
    tokio::time::advance(SECOND * 3).await;
    assert_eq!(clock.spent(), SECOND * 5);
    drop(decoding);
    tokio::time::advance(SECOND * 7).await;
    assert_eq!(clock.spent(), SECOND * 5);
}

// Two decoders at once spend the clock once, and a wait of one leaves
// the other's time running.
#[tokio::test(start_paused = true)]
async fn decoders_running_together_spend_the_clock_once() {
    let clock = DecodeClock::default();
    let first = clock.decoding();
    let second = clock.decoding();
    tokio::time::advance(SECOND * 3).await;
    assert_eq!(clock.spent(), SECOND * 3);
    {
        let _waiting = first.wait();
        tokio::time::advance(SECOND * 2).await;
    }
    assert_eq!(clock.spent(), SECOND * 5);
    drop(first);
    drop(second);
    tokio::time::advance(SECOND * 2).await;
    assert_eq!(clock.spent(), SECOND * 5);
}

// Each decoder takes its part of every second spent while it runs,
// executing or waiting: the whole second alone, half of it beside
// another. The parts add up to the clock.
#[tokio::test(start_paused = true)]
async fn each_decoder_takes_its_part_of_the_seconds_it_runs_in() {
    let clock = DecodeClock::default();
    let first = clock.decoding();
    tokio::time::advance(SECOND * 2).await;
    let second = clock.decoding();
    {
        let _waiting = first.wait();
        tokio::time::advance(SECOND * 4).await;
    }
    tokio::time::advance(SECOND).await;
    assert_eq!(first.spent(), SECOND * 4 + SECOND / 2);
    assert_eq!(second.spent(), SECOND * 2 + SECOND / 2);
    assert_eq!(clock.spent(), SECOND * 7);
    assert_eq!(first.spent() + second.spent(), clock.spent());
}

// Eight decoders at once for ten seconds take a part of an eighth each:
// the parts of all of them are the ten seconds, not eighty.
#[tokio::test(start_paused = true)]
async fn decoders_together_take_no_more_than_the_clock() {
    let clock = DecodeClock::default();
    let decoders: Vec<_> = (0..8).map(|_| clock.decoding()).collect();
    tokio::time::advance(SECOND * 10).await;
    for decoding in &decoders {
        assert_eq!(decoding.spent(), SECOND * 10 / 8);
    }
    let parts: Duration = decoders.iter().map(super::Decoding::spent).sum();
    assert_eq!(parts, clock.spent());
}

// While the job is paused the clock runs, but no decoder takes a part:
// that time is the pause's.
#[tokio::test(start_paused = true)]
async fn a_decoder_takes_no_part_while_the_job_is_paused() {
    let clock = DecodeClock::default();
    let decoding = clock.decoding();
    tokio::time::advance(SECOND * 2).await;
    clock.hold(true);
    tokio::time::advance(SECOND * 5).await;
    clock.hold(false);
    tokio::time::advance(SECOND).await;
    assert_eq!(clock.spent(), SECOND * 8);
    assert_eq!(decoding.spent(), SECOND * 3);
}
