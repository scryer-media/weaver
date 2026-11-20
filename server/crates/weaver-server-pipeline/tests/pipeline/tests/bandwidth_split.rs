// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// The usenet/torrent split of the global download limit as the pipeline puts
// it in force, and the global pause it publishes to the torrents.

use std::sync::Arc;
use weaver_server_composition::Database;
use weaver_server_pipeline::SchedulerHandle;
use weaver_server_pipeline::pipeline::tests::bandwidth_fixture::BandwidthFixture;

use std::sync::Mutex as StdMutex;
use std::sync::atomic::AtomicU64 as StdAtomicU64;

use weaver_server_pipeline::bandwidth::{
    BandwidthSplit, ClassRates, ScheduleAction, SplitRates, SplitState,
};
use weaver_server_pipeline::jobs::Protocol;
use weaver_server_pipeline::jobs::class::{ClassRate, JobClass};
use weaver_server_pipeline::jobs::{HoldOwner, HoldSet};

const MB: u64 = 1_000_000;

// Torrents downloading at a rate the test sets, recording the rates they
// are given.
#[derive(Default)]
struct Torrents {
    used: StdAtomicU64,
    given: StdMutex<Vec<ClassRates>>,
}

// Only the rate of the torrent class: no job of it is acted on here.
#[async_trait::async_trait]
impl JobClass for Torrents {
    fn protocol(&self) -> Protocol {
        Protocol::Torrent
    }

    async fn act(
        &self,
        actions: Vec<(
            weaver_server_composition::JobId,
            weaver_server_pipeline::jobs::handle::QueueAction,
        )>,
    ) -> std::collections::BTreeMap<
        weaver_server_composition::JobId,
        Result<(), weaver_server_composition::SchedulerError>,
    > {
        actions
            .into_iter()
            .map(|(id, _)| {
                (
                    id,
                    Err(weaver_server_composition::SchedulerError::JobNotFound(id)),
                )
            })
            .collect()
    }

    fn rate(&self) -> Option<&dyn ClassRate> {
        Some(self)
    }
}

impl ClassRate for Torrents {
    fn sample(&self) -> u64 {
        self.used.load(std::sync::atomic::Ordering::Relaxed)
    }

    fn set_rates(&self, rates: ClassRates) {
        self.given.lock().unwrap().push(rates);
    }
}

impl Torrents {
    fn rates(&self) -> ClassRates {
        *self.given.lock().unwrap().last().expect("rates were given")
    }

    fn given(&self) -> Option<u64> {
        self.rates().download
    }

    fn downloading(&self, used: u64) {
        self.used.store(used, std::sync::atomic::Ordering::Relaxed);
    }
}

fn split(usenet_pct: u8) -> Option<BandwidthSplit> {
    Some(BandwidthSplit { usenet_pct })
}

fn bandwidth(
    download_bps: Option<u64>,
    usenet_pct: Option<u8>,
) -> weaver_server_composition::proxies::Bandwidth {
    weaver_server_composition::proxies::Bandwidth {
        download_bps,
        upload_bps: None,
        split: usenet_pct.and_then(split),
    }
}

fn speed_limit(download: Option<u64>, upload: Option<u64>) -> ScheduleAction {
    ScheduleAction::SpeedLimit {
        limits: vec![weaver_server_pipeline::bandwidth::SpeedLimitChange {
            target: weaver_server_pipeline::bandwidth::SpeedTarget::Global,
            bytes_per_sec: download,
            upload_bytes_per_sec: upload,
        }],
    }
}

async fn seated(temp_dir: &tempfile::TempDir) -> (BandwidthFixture, Arc<Torrents>) {
    let pipeline = BandwidthFixture::direct(temp_dir).await;
    let torrents = Arc::new(Torrents::default());
    pipeline
        .state()
        .classes()
        .seat(Arc::clone(&torrents) as Arc<dyn JobClass>);
    (pipeline, torrents)
}

fn handle(pipeline: &BandwidthFixture) -> SchedulerHandle {
    pipeline.handle()
}

async fn set_bandwidth(
    pipeline: &mut BandwidthFixture,
    bandwidth: weaver_server_composition::proxies::Bandwidth,
) {
    handle(pipeline).set_bandwidth(bandwidth).await.unwrap();
    pipeline.apply_queue_intents().await;
}

async fn schedule(pipeline: &mut BandwidthFixture, action: ScheduleAction) {
    handle(pipeline)
        .apply_schedule_action(action)
        .await
        .unwrap();
    pipeline.apply_queue_intents().await;
}

// One split tick on the mocked clock: the split measures what each side
// moved over it and puts the shares that follow in force.
async fn tick(pipeline: &mut BandwidthFixture) {
    pipeline.measure_split();
    tokio::time::advance(weaver_server_pipeline::bandwidth::SPLIT_TICK).await;
    pipeline.refresh_periodic_snapshot();
}

fn rates(pipeline: &BandwidthFixture, torrents: &Torrents) -> (Option<u64>, Option<u64>) {
    (pipeline.rate(), torrents.given())
}

#[tokio::test(start_paused = true)]
async fn the_split_shares_the_download_limit_between_usenet_and_torrents() {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut pipeline = BandwidthFixture::direct(&temp_dir).await;
    let torrents = Arc::new(Torrents::default());
    pipeline
        .state()
        .classes()
        .seat(Arc::clone(&torrents) as Arc<dyn JobClass>);

    // Nothing moves: each side is held to its share.
    set_bandwidth(&mut pipeline, bandwidth(Some(100 * MB), Some(80))).await;
    assert_eq!(rates(&pipeline, &torrents), (Some(80 * MB), Some(20 * MB)));
    assert_eq!(
        pipeline.state().bandwidth_split(),
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

    // Torrents press their share while usenet is idle: the split's tick
    // hands them the whole limit but the hundredth usenet keeps to wake into.
    torrents
        .used
        .store(20 * MB, std::sync::atomic::Ordering::Relaxed);
    tick(&mut pipeline).await;
    assert_eq!(rates(&pipeline, &torrents), (Some(MB), Some(99 * MB)));

    // Both idle: the next tick hands the shares back. A split rule takes
    // over the operator's, and lifting it goes back.
    torrents.used.store(0, std::sync::atomic::Ordering::Relaxed);
    tick(&mut pipeline).await;
    assert_eq!(rates(&pipeline, &torrents), (Some(80 * MB), Some(20 * MB)));
    schedule(
        &mut pipeline,
        ScheduleAction::BandwidthSplit {
            usenet_pct: Some(50),
        },
    )
    .await;
    assert_eq!(rates(&pipeline, &torrents), (Some(50 * MB), Some(50 * MB)));
    assert_eq!(pipeline.state().bandwidth_split().stored, split(80));
    assert_eq!(pipeline.state().bandwidth_split().in_force, split(50));

    // A speed rule and a split rule hold separate tracks: the speed rule
    // lowers the limit the split shares out, and neither ends the other.
    schedule(
        &mut pipeline,
        ScheduleAction::SpeedLimit {
            limits: vec![weaver_server_pipeline::bandwidth::SpeedLimitChange {
                target: weaver_server_pipeline::bandwidth::SpeedTarget::Global,
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
    assert!(pipeline.scheduled_bandwidth().is_some());

    // Without a limit the split is kept and does nothing.
    pipeline.clear_scheduled_bandwidth();
    set_bandwidth(&mut pipeline, bandwidth(None, Some(80))).await;
    assert_eq!(rates(&pipeline, &torrents), (None, None));
    assert_eq!(pipeline.state().bandwidth_split().stored, split(80));
    assert_eq!(pipeline.state().bandwidth_split().cap, None);

    // Without a split the two share the limit equally, borrowing as either
    // moves, so together they never pass it.
    torrents.used.store(0, std::sync::atomic::Ordering::Relaxed);
    set_bandwidth(&mut pipeline, bandwidth(Some(100 * MB), None)).await;
    assert_eq!(rates(&pipeline, &torrents), (Some(50 * MB), Some(50 * MB)));
}

#[tokio::test]
async fn editing_the_limit_keeps_a_scheduled_split_and_setting_a_split_lifts_it() {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut pipeline = BandwidthFixture::direct(&temp_dir).await;
    let torrents = Arc::new(Torrents::default());
    pipeline
        .state()
        .classes()
        .seat(Arc::clone(&torrents) as Arc<dyn JobClass>);

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
    assert_eq!(pipeline.state().bandwidth_split().in_force, split(60));
}

#[tokio::test]
async fn the_whole_upload_limit_is_the_torrents() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, torrents) = seated(&temp_dir).await;

    // The operator's upload limit reaches the torrents whole, beside their
    // side of the download split.
    set_bandwidth(
        &mut pipeline,
        weaver_server_composition::proxies::Bandwidth {
            upload_bps: Some(3 * MB),
            ..bandwidth(Some(100 * MB), Some(80))
        },
    )
    .await;
    assert_eq!(
        torrents.rates(),
        ClassRates {
            download: Some(20 * MB),
            upload: Some(3 * MB),
        }
    );
    // The bandwidth tick keeps it in force.
    pipeline.refresh_periodic_snapshot();
    assert_eq!(torrents.rates().upload, Some(3 * MB));

    // A speed rule's upload limit lowers it, and a looser one does not lift
    // it past the operator's.
    schedule(&mut pipeline, speed_limit(None, Some(MB))).await;
    assert_eq!(torrents.rates().upload, Some(MB));
    schedule(&mut pipeline, speed_limit(None, Some(9 * MB))).await;
    assert_eq!(torrents.rates().upload, Some(3 * MB));

    // An upload limit of zero is passed on as zero.
    schedule(&mut pipeline, speed_limit(None, Some(0))).await;
    assert_eq!(torrents.rates().upload, Some(0));

    // With no upload limit anywhere, the torrents upload unlimited.
    set_bandwidth(&mut pipeline, bandwidth(Some(100 * MB), Some(80))).await;
    assert_eq!(torrents.rates().upload, None);
    schedule(&mut pipeline, speed_limit(Some(10 * MB), Some(2 * MB))).await;
    assert_eq!(
        torrents.rates(),
        ClassRates {
            download: Some(2 * MB),
            upload: Some(2 * MB),
        }
    );
    // A speed rule with no upload limit lifts the scheduled one.
    schedule(&mut pipeline, speed_limit(Some(10 * MB), None)).await;
    assert_eq!(torrents.rates().upload, None);
}

#[tokio::test(start_paused = true)]
async fn the_published_split_settles_back_to_the_shares_once_both_sides_idle() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, torrents) = seated(&temp_dir).await;
    let shares = SplitRates {
        usenet: Some(80 * MB),
        torrent: Some(20 * MB),
    };
    set_bandwidth(&mut pipeline, bandwidth(Some(100 * MB), Some(80))).await;
    assert_eq!(pipeline.state().bandwidth_split().rates, shares);

    // Torrents busy while usenet idles: they are lent the whole limit but
    // usenet's probe.
    torrents.downloading(20 * MB);
    tick(&mut pipeline).await;
    assert_eq!(
        pipeline.state().bandwidth_split().rates.torrent,
        Some(99 * MB)
    );
    torrents.downloading(99 * MB);
    tick(&mut pipeline).await;
    assert_eq!(
        pipeline.state().bandwidth_split().rates.torrent,
        Some(99 * MB)
    );
    // Between two of the split's ticks the shares hold.
    pipeline.refresh_periodic_snapshot();
    assert_eq!(
        pipeline.state().bandwidth_split().rates.torrent,
        Some(99 * MB)
    );

    // Both idle: the next tick hands the lent limit back.
    torrents.downloading(0);
    tick(&mut pipeline).await;
    assert_eq!(pipeline.state().bandwidth_split().rates, shares);
    assert_eq!(torrents.given(), Some(20 * MB));
    tick(&mut pipeline).await;
    assert_eq!(pipeline.state().bandwidth_split().rates, shares);
}

// Start the split's measurement afresh: what each side moves from now on
// is the next tick's.
fn baseline(pipeline: &mut BandwidthFixture) {
    pipeline.reset_split_measurement();
}

#[tokio::test(start_paused = true)]
async fn usenet_is_measured_off_the_wire_at_each_tick() {
    let temp_dir = tempfile::tempdir().unwrap();
    let (mut pipeline, torrents) = seated(&temp_dir).await;
    let policy = Arc::new(
        weaver_server_composition::servers::transfer_policy::ServerTransferPolicyRegistry::new(
            Database::open_in_memory().unwrap(),
            &[],
        )
        .unwrap(),
    );
    pipeline
        .state()
        .set_server_transfer_policy(Arc::clone(&policy));
    let server = policy
        .transfer_registry()
        .control(weaver_nntp::transfer::StableServerId(1));
    set_bandwidth(&mut pipeline, bandwidth(Some(100 * MB), Some(50))).await;
    assert_eq!(rates(&pipeline, &torrents), (Some(50 * MB), Some(50 * MB)));

    // Usenet reads its whole share off the wire over the tick, torrents
    // nothing: usenet has the limit but the torrents' probe.
    baseline(&mut pipeline);
    for _ in 0..50 {
        server.pace_read_without_wait(MB as usize);
    }
    tokio::time::advance(weaver_server_pipeline::bandwidth::SPLIT_TICK).await;
    pipeline.refresh_periodic_snapshot();
    assert_eq!(rates(&pipeline, &torrents), (Some(99 * MB), Some(MB)));

    // An article still in flight at the tick's end is counted read by read:
    // no article finished in the tick, and usenet still presses its rate.
    for _ in 0..95 {
        server.pace_read_without_wait(MB as usize);
    }
    tokio::time::advance(weaver_server_pipeline::bandwidth::SPLIT_TICK).await;
    pipeline.refresh_periodic_snapshot();
    assert_eq!(pipeline.recorded_bytes(), 0);
    assert_eq!(rates(&pipeline, &torrents), (Some(99 * MB), Some(MB)));
}

// What a hold set stops: usenet downloads, torrent downloads, seeding.
fn stopped(holds: &HoldSet) -> (bool, bool, bool) {
    (
        holds.stops(HoldOwner::All, false),
        holds.stops(
            HoldOwner::Class(weaver_server_pipeline::torrents::CLASS),
            false,
        ),
        holds.stops(
            HoldOwner::Class(weaver_server_pipeline::torrents::CLASS),
            true,
        ),
    )
}

#[tokio::test]
async fn pauses_publish_what_they_stop_to_the_torrents() {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut pipeline = BandwidthFixture::direct(&temp_dir).await;
    let mut holds = pipeline.state().subscribe_holds();

    for (action, expected) in [
        (ScheduleAction::Pause, (true, true, false)),
        (ScheduleAction::Resume, (false, false, false)),
        (ScheduleAction::PauseAll, (true, true, true)),
        (ScheduleAction::Resume, (false, false, false)),
        (
            ScheduleAction::PauseClass {
                class: weaver_server_pipeline::torrents::CLASS,
            },
            (false, true, true),
        ),
        (ScheduleAction::Resume, (false, true, true)),
        (
            ScheduleAction::ResumeClass {
                class: weaver_server_pipeline::torrents::CLASS,
            },
            (false, false, false),
        ),
    ] {
        schedule(&mut pipeline, action.clone()).await;
        assert_eq!(stopped(&holds.borrow_and_update()), expected, "{action:?}");
    }

    handle(&pipeline)
        .pause(weaver_server_pipeline::jobs::Hold::manual(HoldOwner::All))
        .await
        .unwrap();
    pipeline.apply_queue_intents().await;
    assert!(holds.has_changed().unwrap());
    assert_eq!(stopped(&holds.borrow_and_update()), (true, true, false));
    handle(&pipeline)
        .resume(
            weaver_server_pipeline::jobs::HoldOrigin::Manual,
            HoldOwner::All,
        )
        .await
        .unwrap();
    pipeline.apply_queue_intents().await;
    assert_eq!(stopped(&holds.borrow_and_update()), (false, false, false));
}
