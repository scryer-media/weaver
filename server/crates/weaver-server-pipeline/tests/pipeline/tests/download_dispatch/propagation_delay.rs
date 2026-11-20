// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use std::time::Duration;
use weaver_server_pipeline::JobId;
use weaver_server_pipeline::pipeline::tests::bandwidth_fixture::BandwidthFixture;
use weaver_server_pipeline::pipeline::tests::propagation_fixture::{
    PropagationFixture, now_epoch_secs,
};
use weaver_server_pipeline::{bandwidth, proxies::Bandwidth};

#[tokio::test]
async fn download_block_refresh_preserves_scheduled_speed_limit() {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut pipeline = BandwidthFixture::new(&temp_dir, 5632 * 1024, 1).await;

    pipeline.publish_scheduled_limit(Bandwidth {
        download_bps: Some(131_072),
        upload_bps: None,
        split: None,
    });
    assert_eq!(
        pipeline.state().download_block().scheduled_speed_limit,
        Some(131_072)
    );

    pipeline.record_bandwidth(1_024);
    assert_eq!(
        pipeline.state().download_block().scheduled_speed_limit,
        Some(131_072)
    );

    pipeline.sample_bandwidth();
    assert_eq!(
        pipeline.state().download_block().scheduled_speed_limit,
        Some(131_072)
    );
}

fn global_speed(bytes_per_sec: Option<u64>) -> bandwidth::ScheduleAction {
    bandwidth::ScheduleAction::SpeedLimit {
        limits: vec![bandwidth::SpeedLimitChange {
            target: bandwidth::SpeedTarget::Global,
            bytes_per_sec,
            upload_bytes_per_sec: None,
        }],
    }
}

fn download_bandwidth(bytes_per_sec: u64) -> Bandwidth {
    Bandwidth {
        download_bps: Some(bytes_per_sec),
        upload_bps: None,
        split: None,
    }
}

#[tokio::test]
async fn a_scheduled_speed_limit_holds_over_pauses_and_reloaded_limits() {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut pipeline = BandwidthFixture::new(&temp_dir, 16 * 1024 * 1024, 0).await;

    pipeline
        .reported_handle()
        .set_bandwidth(download_bandwidth(512 * 1024))
        .await
        .unwrap();
    pipeline.apply_queue_intents().await;
    assert_eq!(
        pipeline.state().download_block().speed_limit_in_force,
        Some(512 * 1024)
    );
    assert!(!pipeline.state().download_block().speed_limit_by_schedule);

    pipeline
        .reported_handle()
        .apply_schedule_action(global_speed(Some(128 * 1024)))
        .await
        .unwrap();
    pipeline.apply_queue_intents().await;
    assert_eq!(pipeline.rate(), Some(128 * 1024));

    for action in [
        bandwidth::ScheduleAction::Pause,
        bandwidth::ScheduleAction::Resume,
    ] {
        pipeline
            .reported_handle()
            .apply_schedule_action(action.clone())
            .await
            .unwrap();
        pipeline.apply_queue_intents().await;
        assert_eq!(
            pipeline.state().is_paused(),
            action == bandwidth::ScheduleAction::Pause
        );
        assert_eq!(pipeline.rate(), Some(128 * 1024));
        assert_eq!(
            pipeline.scheduled_bandwidth(),
            Some(download_bandwidth(128 * 1024))
        );
        assert_eq!(
            pipeline.state().download_block().scheduled_speed_limit,
            Some(128 * 1024)
        );
        assert_eq!(
            pipeline.state().download_block().speed_limit_in_force,
            Some(128 * 1024)
        );
        assert!(pipeline.state().download_block().speed_limit_by_schedule);
    }

    pipeline
        .reported_handle()
        .restore_bandwidth(download_bandwidth(768 * 1024))
        .await
        .unwrap();
    pipeline.apply_queue_intents().await;
    // A reload records the saved limit but leaves the scheduled one in force.
    assert_eq!(
        pipeline.configured_bandwidth(),
        download_bandwidth(768 * 1024)
    );
    assert_eq!(pipeline.rate(), Some(128 * 1024));

    pipeline
        .reported_handle()
        .apply_schedule_action(bandwidth::ScheduleAction::Pause)
        .await
        .unwrap();
    pipeline.apply_queue_intents().await;
    // A schedule without a limit leaves the saved one in force.
    pipeline
        .reported_handle()
        .apply_schedule_action(global_speed(None))
        .await
        .unwrap();
    pipeline.apply_queue_intents().await;
    assert_eq!(pipeline.rate(), Some(768 * 1024));
    assert!(pipeline.state().is_paused());
    // The published limit in force is the saved one, and not the schedule's.
    assert_eq!(
        pipeline.state().download_block().speed_limit_in_force,
        Some(768 * 1024)
    );
    assert!(!pipeline.state().download_block().speed_limit_by_schedule);

    // A schedule's zero lets nothing through.
    pipeline
        .reported_handle()
        .apply_schedule_action(global_speed(Some(0)))
        .await
        .unwrap();
    pipeline.apply_queue_intents().await;
    assert_eq!(pipeline.rate(), Some(0));
    assert_eq!(
        pipeline.state().download_block().speed_limit_in_force,
        Some(0)
    );
    assert!(pipeline.state().download_block().speed_limit_by_schedule);
}

#[tokio::test]
async fn speed_test_hold_preserves_later_schedule_pause() {
    use weaver_server_pipeline::jobs::{Hold, HoldOrigin, HoldOwner, HoldScope};
    let temp_dir = tempfile::tempdir().unwrap();
    let mut pipeline = BandwidthFixture::new(&temp_dir, 16 * 1024 * 1024, 0).await;
    let speed_test = Hold {
        origin: HoldOrigin::SpeedTest,
        scope: HoldScope::Everything,
        owner: HoldOwner::All,
    };
    pipeline.reported_handle().pause(speed_test).await.unwrap();
    pipeline.apply_queue_intents().await;
    assert!(pipeline.state().is_paused());
    assert_eq!(pipeline.global_pause(), Some(HoldOrigin::SpeedTest));
    assert_eq!(
        pipeline.persisted_pause(),
        None,
        "the speed test's hold is not persisted"
    );
    // A manual resume leaves the running test's hold in place.
    pipeline
        .reported_handle()
        .resume(HoldOrigin::Manual, HoldOwner::All)
        .await
        .unwrap();
    pipeline.apply_queue_intents().await;
    assert_eq!(pipeline.global_pause(), Some(HoldOrigin::SpeedTest));
    pipeline
        .reported_handle()
        .apply_schedule_action(bandwidth::ScheduleAction::Pause)
        .await
        .unwrap();
    pipeline.apply_queue_intents().await;
    pipeline
        .reported_handle()
        .resume(HoldOrigin::SpeedTest, HoldOwner::All)
        .await
        .unwrap();
    pipeline.apply_queue_intents().await;
    assert!(pipeline.state().is_paused());
    assert_eq!(pipeline.global_pause(), Some(HoldOrigin::Scheduled));
    pipeline
        .reported_handle()
        .apply_schedule_action(bandwidth::ScheduleAction::Resume)
        .await
        .unwrap();
    pipeline.apply_queue_intents().await;
    assert!(!pipeline.state().is_paused());
}

#[tokio::test]
async fn an_operator_speed_limit_replaces_the_scheduled_one_until_the_next_rule_fires() {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut pipeline = BandwidthFixture::new(&temp_dir, 16 * 1024 * 1024, 0).await;
    let handle = pipeline.handle();
    handle
        .apply_schedule_action(global_speed(Some(128 * 1024)))
        .await
        .unwrap();
    pipeline.apply_queue_intents().await;
    assert_eq!(pipeline.rate(), Some(128 * 1024));

    // The operator sets a limit: it is in force at once.
    pipeline
        .reported_handle()
        .set_bandwidth(download_bandwidth(640 * 1024))
        .await
        .unwrap();
    pipeline.apply_queue_intents().await;
    assert_eq!(pipeline.scheduled_bandwidth(), None);
    assert_eq!(
        pipeline.configured_bandwidth(),
        download_bandwidth(640 * 1024)
    );
    assert_eq!(pipeline.rate(), Some(640 * 1024));
    assert_eq!(
        pipeline.state().download_block().scheduled_speed_limit,
        None
    );

    // The next scheduled speed rule takes over again.
    handle
        .apply_schedule_action(global_speed(Some(256 * 1024)))
        .await
        .unwrap();
    pipeline.apply_queue_intents().await;
    assert_eq!(
        pipeline.scheduled_bandwidth(),
        Some(download_bandwidth(256 * 1024))
    );
    assert_eq!(pipeline.rate(), Some(256 * 1024));
    assert_eq!(
        pipeline.configured_bandwidth(),
        download_bandwidth(640 * 1024)
    );
}

#[tokio::test]
async fn the_deferral_boundary_is_posted_at_plus_the_delay() {
    // Pin both sides of the boundary with enough margin that a wall-clock
    // second ticking during fixture setup cannot change the expected side.
    let temp_dir = tempfile::tempdir().unwrap();
    let mut pipeline = PropagationFixture::new(&temp_dir, Duration::from_secs(3600)).await;
    let now = now_epoch_secs();

    let young = JobId(20201);
    pipeline
        .add_post(
            young,
            "Silver Horizon Young",
            &[("queued.bin", Some(now - 3540))],
        )
        .await;
    assert!(
        pipeline.propagation_hold_until(young).is_some(),
        "a post inside the delay is still deferred"
    );

    let old = JobId(20202);
    pipeline
        .add_post(
            old,
            "Silver Horizon Old",
            &[("queued.bin", Some(now - 3660))],
        )
        .await;
    assert!(
        pipeline.propagation_hold_until(old).is_none(),
        "a post older than the delay starts immediately"
    );
    assert!(
        !pipeline.has_cached_hold(old),
        "and an eligible job leaves no deferral state behind"
    );
}

#[tokio::test]
async fn a_job_with_no_parseable_dates_is_never_deferred() {
    // No date is not the same question as "posted just now". An NZB that
    // carries no usable dates has no anchor to defer against, so it starts
    // immediately — the same stance retention takes for the same reason.
    let temp_dir = tempfile::tempdir().unwrap();
    let mut pipeline = PropagationFixture::new(&temp_dir, Duration::from_secs(3600)).await;
    let job_id = JobId(20204);
    pipeline
        .add_post(job_id, "Silver Horizon Undated", &[("queued.bin", None)])
        .await;

    assert!(pipeline.propagation_hold_until(job_id).is_none());
    assert!(pipeline.next_propagation_delay().is_none());
}

#[tokio::test]
async fn the_newest_date_in_the_nzb_is_the_anchor() {
    // Files with no date contribute nothing, and the anchor is the newest date
    // the NZB does carry — that is the article most likely to still be in
    // flight, and deferring to it is what makes the wait cover the whole post.
    let temp_dir = tempfile::tempdir().unwrap();
    let mut pipeline = PropagationFixture::new(&temp_dir, Duration::from_secs(3600)).await;
    let now = now_epoch_secs();
    let job_id = JobId(20205);
    pipeline
        .add_post(
            job_id,
            "Silver Horizon Mixed Dates",
            &[("old.bin", Some(now - 86_400)), ("new.bin", None)],
        )
        .await;
    assert!(
        pipeline.propagation_hold_until(job_id).is_none(),
        "a day-old anchor beside an undated file is eligible"
    );

    let fresh = JobId(20206);
    pipeline
        .add_post(
            fresh,
            "Silver Horizon Fresh Tail",
            &[("old.bin", Some(now - 86_400)), ("new.bin", Some(now))],
        )
        .await;
    assert!(
        pipeline.propagation_hold_until(fresh).is_some(),
        "one fresh file is enough to hold the job — its articles are the ones \
         still propagating"
    );
}

#[tokio::test]
async fn a_zero_delay_disables_deferral_entirely() {
    let temp_dir = tempfile::tempdir().unwrap();
    let mut pipeline = PropagationFixture::new(&temp_dir, Duration::ZERO).await;
    let job_id = JobId(20207);
    pipeline
        .add_post(
            job_id,
            "Silver Horizon Disabled",
            &[("queued.bin", Some(now_epoch_secs()))],
        )
        .await;

    assert!(
        pipeline.propagation_hold_until(job_id).is_none(),
        "WEAVER_PROPAGATION_DELAY_SECS=0 is the off switch"
    );
    assert_eq!(pipeline.cached_hold_count(), 0);
}
