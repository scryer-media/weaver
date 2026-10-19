// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The usenet/torrent split of the global download limit as the pipeline puts
// it in force, and the global pause it publishes to the torrents.

use super::*;

use std::sync::Mutex as StdMutex;
use std::sync::atomic::AtomicU64 as StdAtomicU64;

use crate::bandwidth::{BandwidthSplit, ScheduleAction, SplitRates, SplitState};
use crate::jobs::handle::TorrentRate;
use crate::jobs::{HoldOwner, HoldSet};

const MB: u64 = 1_000_000;

// Torrents downloading at a rate the test sets, recording the rate they are
// given.
#[derive(Default)]
struct Torrents {
    used: StdAtomicU64,
    given: StdMutex<Vec<Option<u64>>>,
}

impl TorrentRate for Torrents {
    fn download_bps(&self) -> u64 {
        self.used.load(std::sync::atomic::Ordering::Relaxed)
    }

    fn set_download_rate(&self, rate: Option<u64>) {
        self.given.lock().unwrap().push(rate);
    }
}

impl Torrents {
    fn given(&self) -> Option<u64> {
        *self.given.lock().unwrap().last().expect("a rate was given")
    }
}

fn split(usenet_pct: u8) -> Option<BandwidthSplit> {
    Some(BandwidthSplit { usenet_pct })
}

fn bandwidth(download_bps: Option<u64>, usenet_pct: Option<u8>) -> crate::proxies::Bandwidth {
    crate::proxies::Bandwidth {
        download_bps,
        upload_bps: None,
        split: usenet_pct.and_then(split),
    }
}

async fn set_bandwidth(pipeline: &mut Pipeline, bandwidth: crate::proxies::Bandwidth) {
    let (reply, received) = oneshot::channel();
    pipeline
        .handle_command(SchedulerCommand::SetBandwidth {
            bandwidth,
            replaces_schedule: true,
            reply,
        })
        .await;
    received.await.unwrap();
}

async fn schedule(pipeline: &mut Pipeline, action: ScheduleAction) {
    let (reply, received) = oneshot::channel();
    pipeline
        .handle_command(SchedulerCommand::ApplyScheduleAction { action, reply })
        .await;
    received.await.unwrap();
}

fn rates(pipeline: &Pipeline, torrents: &Torrents) -> (Option<u64>, Option<u64>) {
    (pipeline.rate_limiter.rate(), torrents.given())
}

#[tokio::test]
async fn the_split_shares_the_download_limit_between_usenet_and_torrents() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let torrents = Arc::new(Torrents::default());
    pipeline
        .shared_state
        .seat_torrent_rate(Arc::clone(&torrents) as Arc<dyn TorrentRate>);

    // Nothing moves: each side is held to its share.
    set_bandwidth(&mut pipeline, bandwidth(Some(100 * MB), Some(80))).await;
    assert_eq!(rates(&pipeline, &torrents), (Some(80 * MB), Some(20 * MB)));
    assert_eq!(
        pipeline.shared_state.bandwidth_split(),
        SplitState {
            stored: split(80),
            in_force: split(80),
            cap: Some(100 * MB),
            rates: SplitRates {
                usenet: Some(80 * MB),
                torrent: Some(20 * MB),
            },
        }
    );

    // Torrents want more while usenet is idle: the bandwidth tick hands them
    // the whole limit, and usenet keeps its share to wake into.
    torrents
        .used
        .store(20 * MB, std::sync::atomic::Ordering::Relaxed);
    pipeline.refresh_periodic_snapshot();
    assert_eq!(rates(&pipeline, &torrents), (Some(80 * MB), Some(100 * MB)));

    // A split rule takes over the operator's, and lifting it goes back.
    torrents.used.store(0, std::sync::atomic::Ordering::Relaxed);
    schedule(
        &mut pipeline,
        ScheduleAction::BandwidthSplit {
            usenet_pct: Some(50),
        },
    )
    .await;
    assert_eq!(rates(&pipeline, &torrents), (Some(50 * MB), Some(50 * MB)));
    assert_eq!(pipeline.shared_state.bandwidth_split().stored, split(80));
    assert_eq!(pipeline.shared_state.bandwidth_split().in_force, split(50));

    // A speed rule and a split rule hold separate tracks: the speed rule
    // lowers the limit the split shares out, and neither ends the other.
    schedule(
        &mut pipeline,
        ScheduleAction::SpeedLimit {
            limits: vec![crate::bandwidth::SpeedLimitChange {
                target: crate::bandwidth::SpeedTarget::Global,
                bytes_per_sec: Some(10 * MB),
                upload_bytes_per_sec: None,
            }],
        },
    )
    .await;
    assert_eq!(rates(&pipeline, &torrents), (Some(5 * MB), Some(5 * MB)));
    schedule(
        &mut pipeline,
        ScheduleAction::BandwidthSplit { usenet_pct: None },
    )
    .await;
    assert_eq!(rates(&pipeline, &torrents), (Some(8 * MB), Some(2 * MB)));
    assert!(pipeline.scheduled_bandwidth.is_some());

    // Without a limit the split is kept and does nothing.
    pipeline.scheduled_bandwidth = None;
    set_bandwidth(&mut pipeline, bandwidth(None, Some(80))).await;
    assert_eq!(rates(&pipeline, &torrents), (None, None));
    assert_eq!(pipeline.shared_state.bandwidth_split().stored, split(80));
    assert_eq!(pipeline.shared_state.bandwidth_split().cap, None);

    // Without a split the two share the limit equally, borrowing as either
    // moves, so together they never pass it.
    torrents.used.store(0, std::sync::atomic::Ordering::Relaxed);
    set_bandwidth(&mut pipeline, bandwidth(Some(100 * MB), None)).await;
    assert_eq!(rates(&pipeline, &torrents), (Some(50 * MB), Some(50 * MB)));
}

#[tokio::test]
async fn editing_the_limit_keeps_a_scheduled_split_and_setting_a_split_lifts_it() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let torrents = Arc::new(Torrents::default());
    pipeline
        .shared_state
        .seat_torrent_rate(Arc::clone(&torrents) as Arc<dyn TorrentRate>);

    set_bandwidth(&mut pipeline, bandwidth(Some(100 * MB), Some(80))).await;
    schedule(
        &mut pipeline,
        ScheduleAction::BandwidthSplit {
            usenet_pct: Some(30),
        },
    )
    .await;
    set_bandwidth(&mut pipeline, bandwidth(Some(200 * MB), Some(80))).await;
    assert_eq!(rates(&pipeline, &torrents), (Some(60 * MB), Some(140 * MB)));

    set_bandwidth(&mut pipeline, bandwidth(Some(200 * MB), Some(60))).await;
    assert_eq!(rates(&pipeline, &torrents), (Some(120 * MB), Some(80 * MB)));
    assert_eq!(pipeline.shared_state.bandwidth_split().in_force, split(60));
}

// What a hold set stops: usenet downloads, torrent downloads, seeding.
fn stopped(holds: &HoldSet) -> (bool, bool, bool) {
    (
        holds.stops(HoldOwner::All, false),
        holds.stops(HoldOwner::Torrents, false),
        holds.stops(HoldOwner::Torrents, true),
    )
}

#[tokio::test]
async fn pauses_publish_what_they_stop_to_the_torrents() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, _, _) = new_direct_pipeline(&temp_dir).await;
    let mut holds = pipeline.shared_state.subscribe_holds();

    for (action, expected) in [
        (ScheduleAction::Pause, (true, true, false)),
        (ScheduleAction::Resume, (false, false, false)),
        (ScheduleAction::PauseAll, (true, true, true)),
        (ScheduleAction::Resume, (false, false, false)),
        (ScheduleAction::PauseTorrents, (false, true, true)),
        (ScheduleAction::Resume, (false, true, true)),
        (ScheduleAction::ResumeTorrents, (false, false, false)),
    ] {
        schedule(&mut pipeline, action.clone()).await;
        assert_eq!(stopped(&holds.borrow_and_update()), expected, "{action:?}");
    }

    let (reply, received) = oneshot::channel();
    pipeline
        .handle_command(SchedulerCommand::Pause {
            hold: crate::jobs::Hold::manual(HoldOwner::All),
            reply,
        })
        .await;
    received.await.unwrap();
    assert!(holds.has_changed().unwrap());
    assert_eq!(stopped(&holds.borrow_and_update()), (true, true, false));
    let (reply, received) = oneshot::channel();
    pipeline
        .handle_command(SchedulerCommand::Resume {
            origin: crate::jobs::HoldOrigin::Manual,
            owner: HoldOwner::All,
            reply,
        })
        .await;
    received.await.unwrap();
    assert_eq!(stopped(&holds.borrow_and_update()), (false, false, false));
}
