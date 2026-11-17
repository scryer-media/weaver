// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::*;

// The queue lists torrents among the pipeline's jobs by when each was added,
// and a torrent that leaves goes to the finished jobs.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_queue_orders_torrents_and_jobs_by_when_they_were_added() {
    let rig = rig(set_clock());
    let queue = queue();
    let state = queue.state().clone();
    rig.service.publish_to(queue);
    rig.service.open().await.unwrap();
    let revision = state.job_revision();
    let job_id = rig
        .service
        .add_opened(request(AddSource::Metainfo(plain("set", 1))))
        .await
        .unwrap();
    assert!(state.job_revision() > revision);
    let listed = state.list_jobs();
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].job_id, job_id);
    assert_eq!(listed[0].protocol, Protocol::Torrent);
    assert_eq!(listed[0].total_bytes, 3000);
    let added = listed[0].created_at_epoch_ms;

    let mut earlier = listed[0].clone();
    earlier.job_id = JobId(job_id.0 + 1000);
    earlier.protocol = Protocol::Nzb;
    earlier.created_at_epoch_ms = added - 1.0;
    let mut later = earlier.clone();
    later.job_id = JobId(job_id.0 + 1001);
    later.created_at_epoch_ms = added + 1.0;
    state.publish_jobs(vec![earlier.clone(), later.clone()]);
    let order: Vec<JobId> = state.list_jobs().iter().map(|job| job.job_id).collect();
    assert_eq!(order, [earlier.job_id, job_id, later.job_id]);
    assert_eq!(state.get_job(job_id).unwrap().protocol, Protocol::Torrent);
    // The two NZB jobs go away again, so the finished torrent is all the
    // queue holds below.
    state.publish_jobs(Vec::new());

    let mut changes = state.subscribe_job_changes();
    changes.borrow_and_update();
    rig.service.remove(job_id, false, false).await.unwrap();
    let finished = loop {
        let jobs = state.list_jobs();
        if let Some(job) = jobs.iter().find(|job| job.job_id == job_id) {
            break (job.clone(), jobs.len());
        }
        changes.changed().await.unwrap();
    };
    assert_eq!(
        finished.0.status,
        JobStatus::Failed {
            error: "cancelled".into()
        }
    );
    assert_eq!(finished.0.protocol, Protocol::Torrent);
    assert_eq!(finished.1, 1);
}

// Each queue action by job id reaches the torrent through the queue's handle.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_queues_actions_reach_a_torrent() {
    use crate::jobs::handle::QueueMoveTarget;

    let rig = rig(set_clock());
    let queue = queue();
    rig.service.publish_to(queue.clone());
    rig.service.open().await.unwrap();
    let job_id = rig
        .service
        .add_opened(request(AddSource::Metainfo(plain("set", 1))))
        .await
        .unwrap();
    let record = |rig: &Rig| rig.db.get_torrent(job_id).unwrap().unwrap();

    queue.pause_job(job_id).await.unwrap();
    assert_eq!(record(&rig).intent(), TorrentIntent::Paused);
    queue.resume_job(job_id).await.unwrap();
    assert_eq!(record(&rig).intent(), TorrentIntent::Resumed);

    queue
        .update_job(
            job_id,
            JobUpdate {
                category: FieldUpdate::Set("tv".into()),
                metadata: FieldUpdate::Set(vec![("priority".into(), "HIGH".into())]),
                ..JobUpdate::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(record(&rig).category_id, Some(1));
    assert!(record(&rig).forced);
    queue
        .update_job(
            job_id,
            JobUpdate {
                category: FieldUpdate::Clear,
                metadata: FieldUpdate::Set(vec![("priority".into(), "NORMAL".into())]),
                ..JobUpdate::default()
            },
        )
        .await
        .unwrap();
    assert_eq!(record(&rig).category_id, None);
    assert!(!record(&rig).forced);
    assert!(matches!(
        queue
            .update_job(
                job_id,
                JobUpdate {
                    name: Some("other".into()),
                    ..JobUpdate::default()
                },
            )
            .await,
        Err(crate::SchedulerError::InvalidInput(_))
    ));

    // A move changes its place in the queue and never forces it.
    queue
        .reorder_job(job_id, QueueMoveTarget::Top)
        .await
        .unwrap();
    queue
        .reorder_jobs(vec![(job_id, QueueMoveTarget::Bottom)])
        .await
        .unwrap();
    queue
        .reorder_job(job_id, QueueMoveTarget::Offset(1))
        .await
        .unwrap();
    assert!(!record(&rig).forced);

    queue.cancel_job_keeping_files(job_id).await.unwrap();
    assert_eq!(rig.db.get_torrent(job_id).unwrap(), None);

    // A delete with data goes the same way.
    let other = rig
        .service
        .add_opened(request(AddSource::Metainfo(plain("other", 2))))
        .await
        .unwrap();
    queue.cancel_job(other).await.unwrap();
    assert_eq!(rig.db.get_torrent(other).unwrap(), None);
}

#[test]
fn a_downloading_torrent_carries_its_speed_and_eta_to_the_queue() {
    let view = TorrentView {
        job_id: JobId(7),
        name: "set".into(),
        infohash: String::new(),
        infohash_v1: None,
        infohash_v2: None,
        version: None,
        magnet: String::new(),
        source: TorrentSource::Metainfo,
        state: TorrentState::Downloading,
        detail: None,
        stated: None,
        route_reason: None,
        paused_by: None,
        category: None,
        scope: TorrentScope::Default,
        private: false,
        forced: false,
        progress: 0.25,
        check_progress: None,
        total_bytes: 4000,
        have_bytes: 1000,
        wanted_bytes: 4000,
        wanted_have_bytes: 1000,
        uploaded_bytes: 0,
        downloaded_bytes: 1000,
        run_uploaded_bytes: 0,
        run_downloaded_bytes: 1000,
        ratio: 0.0,
        download_bps: 500,
        upload_bps: 0,
        upload_limit: None,
        download_limit: None,
        eta_seconds: Some(6),
        peers: 1,
        seeds: 1,
        leechers: 0,
        max_peers: 100,
        swarm_seeds: None,
        swarm_leechers: None,
        reannounce_seconds: None,
        active_seconds: 0,
        seeding_seconds: 0,
        consecutive_seeding_seconds: 0,
        download_avg_bps: 0,
        upload_avg_bps: 0,
        wasted_bytes: 0,
        last_seen_complete_ms: None,
        last_activity_ms: None,
        pieces: None,
        created_by: None,
        created_at_ms: None,
        comment: None,
        added_at_ms: EPOCH_MS,
        completed_at_ms: None,
        storage_dir: None,
        files_dir: None,
        queue_position: None,
        seeding_left: None,
        route: None,
        files: Vec::new(),
        sequential: false,
        first_last_piece_first: false,
        trackers: Vec::new(),
        file_have: Vec::new(),
        metadata: Vec::new(),
        tags: Vec::new(),
        seeding_override: None,
        seeding_policy: Seeding::default(),
        seeding_levels: SeedingLevels::default(),
        tracker_rule: None,
        upload_cap: None,
        unpack: true,
        unpack_override: None,
    };
    let job_info =
        |view: TorrentView, now: i64| report(view.clone(), job_status(&view), now).job_info();
    let info = job_info(view.clone(), EPOCH_MS + 1000);
    assert_eq!(info.status, JobStatus::Downloading);
    assert_eq!((info.wanted_bytes, info.remaining_bytes), (4000, 3000));
    assert_eq!(
        crate::jobs::handle::job_download_rates(std::slice::from_ref(&info)),
        [(JobId(7), 500)]
    );
    assert_eq!(info.phase_progress[0].estimated_remaining_ms, Some(6000));
    let paused = job_info(
        TorrentView {
            state: TorrentState::Paused,
            ..view
        },
        EPOCH_MS,
    );
    assert!(paused.phase_progress.is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pause_resume_and_force_are_kept() {
    let rig = rig(set_clock());
    rig.service.open().await.unwrap();
    let job_id = rig
        .service
        .add_opened(request(AddSource::Metainfo(plain("set", 1))))
        .await
        .unwrap();
    rig.service.pause(job_id).await.unwrap();
    assert_eq!(
        rig.service.torrent(job_id).await.unwrap().state,
        TorrentState::Paused
    );
    assert_eq!(
        rig.db.get_torrent(job_id).unwrap().unwrap().intent(),
        TorrentIntent::Paused
    );
    rig.service.tick().await;
    rig.service.restored().await;
    assert_eq!(
        rig.db.get_torrent(job_id).unwrap().unwrap().intent(),
        TorrentIntent::Paused
    );
    rig.service.resume(job_id).await.unwrap();
    assert_ne!(
        rig.service.torrent(job_id).await.unwrap().state,
        TorrentState::Paused
    );
    rig.service.set_forced(job_id, true).await.unwrap();
    assert!(rig.db.get_torrent(job_id).unwrap().unwrap().forced);
}

// A run the session refuses leaves the operator's intent as it was, and the
// state shows the session's error, never a met goal.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_refused_unpause_keeps_the_intent_and_shows_the_error() {
    let rig = rig(set_clock());
    rig.service.open().await.unwrap();
    let job_id = rig
        .service
        .add_opened(request(AddSource::Metainfo(plain("set", 1))))
        .await
        .unwrap();
    rig.service.pause(job_id).await.unwrap();
    rig.service.resume(job_id).await.unwrap();
    {
        let mut live = rig.service.inner.live.lock().await;
        let torrent = live.get_mut(&job_id).unwrap();
        torrent.ran(Err(anyhow::anyhow!("already live")));
        torrent.record.completed_at_ms = Some(EPOCH_MS);
    }
    assert_eq!(
        rig.db.get_torrent(job_id).unwrap().unwrap().intent(),
        TorrentIntent::Resumed
    );
    let view = rig.service.torrent(job_id).await.unwrap();
    assert_eq!(view.state, TorrentState::Stopped);
    assert_eq!(view.detail.as_deref(), Some("already live"));
    assert_eq!(
        job_status(&view),
        JobStatus::Failed {
            error: "already live".into()
        }
    );
}

// A forced torrent seeds past its goal; unforced, the goal stops it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_forced_torrent_seeds_past_its_goal() {
    let rig = rig(set_clock());
    rig.service.open().await.unwrap();
    let job_id = rig
        .service
        .add_opened(AddRequest {
            policy_override: Some(SeedingPolicy {
                seed_time_counting: Default::default(),
                ratio_goal: Leaf::Set(0.0),
                seed_time_goal_minutes: Leaf::Inherit,
                inactive_goal_minutes: Leaf::Inherit,
                mode: Leaf::Set(SeedingGoalMode::Any),
                action: Leaf::Set(SeedingAction::Stop),
            }),
            forced: true,
            ..request(AddSource::Metainfo(plain("set", 1)))
        })
        .await
        .unwrap();
    rig.service
        .inner
        .live
        .lock()
        .await
        .get_mut(&job_id)
        .unwrap()
        .record
        .completed_at_ms = Some(EPOCH_MS);
    rig.service.apply_goals(EPOCH_MS).await;
    let intent = |rig: &Rig| rig.db.get_torrent(job_id).unwrap().unwrap().intent();
    assert_eq!(intent(&rig), TorrentIntent::Resumed);

    rig.service.set_forced(job_id, false).await.unwrap();
    rig.service.apply_goals(EPOCH_MS).await;
    assert_eq!(intent(&rig), TorrentIntent::Stopped);
    let view = rig.service.torrent(job_id).await.unwrap();
    assert_eq!(
        (view.state, view.stated, view.detail.as_deref()),
        (TorrentState::Stopped, Some(status::Stated::GoalMet), None)
    );
}

// The holds the queue publishes reach the torrents: a pause stops
// downloading and leaves what is complete seeding, pausing everything or
// pausing the torrents stops seeding too, and a release lets both run again.
// None of them touches a torrent's own intent, and a torrent its operator
// paused stays paused through them all.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_global_pause_reaches_the_torrents() {
    let rig = rig(set_clock());
    let queue = queue();
    rig.service.publish_to(queue.clone());
    rig.service.open().await.unwrap();
    let downloading = rig
        .service
        .add_opened(request(AddSource::Metainfo(plain("downloading", 1))))
        .await
        .unwrap();
    let seeding = rig
        .service
        .add_opened(request(AddSource::Metainfo(plain("seeding", 2))))
        .await
        .unwrap();
    rig.service
        .inner
        .live
        .lock()
        .await
        .get_mut(&seeding)
        .unwrap()
        .record
        .completed_at_ms = Some(EPOCH_MS);

    let kept = rig
        .service
        .add_opened(request(AddSource::Metainfo(plain("kept", 3))))
        .await
        .unwrap();
    rig.service.pause(kept).await.unwrap();

    let mut published = queue.state().subscribe_job_changes();
    // A hold keeps a torrent in the queue and says whose it is; its own
    // pause is a pause.
    let state = |job_id| {
        let service = rig.service.clone();
        async move {
            let view = service.torrent(job_id).await.unwrap();
            (view.state, view.stated)
        }
    };
    let held = |job_id| async move {
        matches!(
            state(job_id).await,
            (TorrentState::Queued, Some(status::Stated::Held(_)))
        )
    };
    let scheduled = |scope, owner| Hold {
        origin: HoldOrigin::Scheduled,
        scope,
        owner,
    };
    for (holds, stopped) in [
        (vec![Hold::manual(HoldOwner::All)], (true, false)),
        (
            vec![scheduled(HoldScope::Everything, HoldOwner::All)],
            (true, true),
        ),
        (
            vec![Hold::manual(HoldOwner::Class(crate::torrents::CLASS))],
            (true, true),
        ),
        (
            vec![scheduled(
                HoldScope::Everything,
                HoldOwner::Class(crate::torrents::CLASS),
            )],
            (true, true),
        ),
        (vec![], (false, false)),
    ] {
        published.borrow_and_update();
        let mut set = HoldSet::default();
        for hold in holds {
            set.hold(hold);
        }
        queue.state().restore_holds(set);
        while (held(downloading).await, held(seeding).await) != stopped {
            published.changed().await.unwrap();
        }
        assert_eq!(state(kept).await, (TorrentState::Paused, None));
    }
    for (job_id, intent) in [
        (downloading, TorrentIntent::Resumed),
        (seeding, TorrentIntent::Resumed),
        (kept, TorrentIntent::Paused),
    ] {
        assert_eq!(
            rig.db.get_torrent(job_id).unwrap().unwrap().intent(),
            intent
        );
    }
}

// A torrent under a tracker rule that never pauses seeds through the
// schedule's pause until its goal is met; the operator's pause stops it, and
// so does the schedule once the rule is gone. A torrent under no rule pauses.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_never_pause_rule_keeps_its_torrents_seeding_through_the_schedule() {
    let rig = rig(set_clock());
    // No session is built while the route is down.
    rig.routes.set_down(Some("down"));
    let queue = queue();
    rig.service.publish_to(queue.clone());
    rig.service.open().await.unwrap();
    let rule = rig
        .db
        .insert_tracker_rule(
            &TrackerRule {
                transports: Default::default(),
                archive_passwords: Default::default(),
                bound: None,
                id: 0,
                host_pattern: "*.test".into(),
                seeding: Some(SeedingPolicy {
                    ratio_goal: Leaf::Set(2.0),
                    ..SeedingPolicy::default()
                }),
                never_pause: true,
                unpack: None,
                client: None,
                torrent_route: false,
                dht: None,
                pex: None,
                lsd: None,
                encryption: None,
                upload_cap: Default::default(),
                forced: false,
                ssl: None,
            },
            EPOCH_MS,
        )
        .unwrap();
    rig.service.reload_tracker_rules().await.unwrap();
    let kept = rig
        .service
        .add_opened(request(AddSource::Metainfo(announcing(
            &plain("kept", 1),
            "http://tracker.test/announce",
        ))))
        .await
        .unwrap();
    let plain = rig
        .service
        .add_opened(request(AddSource::Metainfo(plain("plain", 2))))
        .await
        .unwrap();
    assert_eq!(
        rig.service.inner.live.lock().await[&kept].rule_id,
        Some(rule.id)
    );
    assert_eq!(rig.service.inner.live.lock().await[&plain].rule_id, None);
    for job_id in [kept, plain] {
        rig.service
            .inner
            .live
            .lock()
            .await
            .get_mut(&job_id)
            .unwrap()
            .record
            .completed_at_ms = Some(EPOCH_MS);
    }
    assert_eq!(
        rig.service
            .torrent(kept)
            .await
            .unwrap()
            .tracker_rule
            .as_deref(),
        Some("*.test")
    );

    let hold = |hold: Hold| {
        let mut set = HoldSet::default();
        set.hold(hold);
        queue.state().restore_holds(set);
    };
    let schedule = Hold {
        origin: HoldOrigin::Scheduled,
        scope: HoldScope::Everything,
        owner: HoldOwner::Class(crate::torrents::CLASS),
    };
    // Subscribed before the first look, so the publish that follows the
    // change wakes it.
    let wait = |job_id, origin| {
        let service = rig.service.clone();
        let mut published = queue.state().subscribe_job_changes();
        async move {
            while paused_by(&service, job_id).await != origin {
                published.changed().await.unwrap();
            }
        }
    };

    for owner in [HoldOwner::Class(crate::torrents::CLASS), HoldOwner::All] {
        hold(Hold { owner, ..schedule });
        wait(plain, Some(HoldOrigin::Scheduled)).await;
        assert_eq!(paused_by(&rig.service, kept).await, None);
        assert_eq!(
            rig.service.schedule_seeding().await,
            [ScheduleSeeding {
                host_pattern: "*.test".into(),
                torrents: 1,
            }]
        );
        queue.state().restore_holds(HoldSet::default());
        wait(plain, None).await;
    }

    // Still downloading, it owes its tracker as much.
    let complete = |job_id, at| {
        let service = rig.service.clone();
        async move {
            service
                .inner
                .live
                .lock()
                .await
                .get_mut(&job_id)
                .unwrap()
                .record
                .completed_at_ms = at;
        }
    };
    complete(kept, None).await;
    hold(schedule);
    wait(plain, Some(HoldOrigin::Scheduled)).await;
    assert_eq!(paused_by(&rig.service, kept).await, None);
    queue.state().restore_holds(HoldSet::default());
    wait(plain, None).await;
    complete(kept, Some(EPOCH_MS)).await;

    hold(Hold::manual(HoldOwner::Class(crate::torrents::CLASS)));
    wait(kept, Some(HoldOrigin::Manual)).await;
    assert_eq!(
        paused_by(&rig.service, plain).await,
        Some(HoldOrigin::Manual)
    );
    assert!(rig.service.schedule_seeding().await.is_empty());
    queue.state().restore_holds(HoldSet::default());
    wait(kept, None).await;

    // Its ratio goal met, it pauses with the rest: twice the 3,000 bytes
    // it downloaded.
    {
        let mut live = rig.service.inner.live.lock().await;
        let torrent = live.get_mut(&kept).unwrap();
        torrent.counters.downloaded_bytes = 3000;
        torrent.counters.uploaded_bytes = 6000;
    }
    hold(schedule);
    wait(kept, Some(HoldOrigin::Scheduled)).await;
    assert!(rig.service.schedule_seeding().await.is_empty());
    rig.service
        .inner
        .live
        .lock()
        .await
        .get_mut(&kept)
        .unwrap()
        .counters
        .uploaded_bytes = 0;
    rig.service.reload_tracker_rules().await.unwrap();
    assert_eq!(paused_by(&rig.service, kept).await, None);

    // With its rule gone it pauses as a torrent under no rule.
    assert!(rig.db.delete_tracker_rule(rule.id).unwrap());
    rig.service.reload_tracker_rules().await.unwrap();
    assert_eq!(
        paused_by(&rig.service, kept).await,
        Some(HoldOrigin::Scheduled)
    );
    assert_eq!(rig.service.inner.live.lock().await[&kept].rule_id, None);
    assert_eq!(rig.service.torrent(kept).await.unwrap().tracker_rule, None);
}

// The download limit counts the torrents still downloading, and a change to
// it applies when the settings are taken, not at the next tick.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_download_limit_counts_downloading_torrents_as_it_is_set() {
    let rig = rig(set_clock());
    // No session is built while the route is down.
    rig.routes.set_down(Some("down"));
    rig.service.open().await.unwrap();
    let mut ids = Vec::new();
    for seed in 1..=4 {
        ids.push(
            rig.service
                .add_opened(request(AddSource::Metainfo(plain(
                    &format!("set{seed}"),
                    seed,
                ))))
                .await
                .unwrap(),
        );
    }
    rig.service
        .inner
        .live
        .lock()
        .await
        .get_mut(&ids[0])
        .unwrap()
        .record
        .completed_at_ms = Some(EPOCH_MS);
    let queued = || async {
        let live = rig.service.inner.live.lock().await;
        ids.iter().map(|id| live[id].queued).collect::<Vec<_>>()
    };
    let set = |max_active_downloads| {
        let mut settings = rig.service.settings();
        settings.torrent.max_active_downloads = max_active_downloads;
        rig.service.set_settings(settings)
    };
    set(1).await;
    assert_eq!(queued().await, [false, false, true, true]);
    set(0).await;
    assert_eq!(queued().await, [false, true, true, true]);
    set(3).await;
    assert_eq!(queued().await, [false, false, false, false]);
}

// The torrents still downloading keep their own order, kept with them: the
// first in it download up to the limit, a forced one runs past it, a seed
// stands outside it, and a move takes the torrents named as qBittorrent's
// queue actions do, keeping their order among themselves.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_download_queue_keeps_its_order_and_moves_as_qbittorrent_does() {
    use crate::jobs::handle::QueueMoveTarget;

    let rig = rig(set_clock());
    // No session is built while the route is down.
    rig.routes.set_down(Some("down"));
    rig.service.open().await.unwrap();
    let mut ids = Vec::new();
    for seed in 1..=5 {
        ids.push(
            rig.service
                .add_opened(request(AddSource::Metainfo(plain(
                    &format!("set{seed}"),
                    seed,
                ))))
                .await
                .unwrap(),
        );
    }
    let [a, b, c, d, e] = ids[..] else {
        unreachable!()
    };
    rig.service
        .inner
        .live
        .lock()
        .await
        .get_mut(&e)
        .unwrap()
        .record
        .completed_at_ms = Some(EPOCH_MS);
    let mut settings = rig.service.settings();
    settings.torrent.max_active_downloads = 1;
    rig.service.set_settings(settings).await;

    let queued = || async {
        let live = rig.service.inner.live.lock().await;
        ids.iter().map(|id| live[id].queued).collect::<Vec<_>>()
    };
    let places = || async {
        let mut places = Vec::new();
        for id in &ids {
            places.push(rig.service.torrent(*id).await.unwrap().queue_position);
        }
        places
    };
    // The order a restart would bring back, the seed's place left as it was.
    let kept = || {
        let mut kept: Vec<(i64, JobId)> = ids
            .iter()
            .map(|id| {
                (
                    rig.db.get_torrent(*id).unwrap().unwrap().queue_position,
                    *id,
                )
            })
            .collect();
        kept.sort();
        kept.into_iter().map(|(_, id)| id).collect::<Vec<_>>()
    };

    assert_eq!(queued().await, [false, true, true, true, false]);
    assert_eq!(places().await, [Some(1), Some(2), Some(3), Some(4), None]);

    rig.service
        .reorder(&[c], QueueMoveTarget::Top)
        .await
        .unwrap();
    assert_eq!(kept(), [c, a, b, d, e]);
    assert_eq!(queued().await, [true, true, false, true, false]);
    assert_eq!(places().await, [Some(2), Some(3), Some(1), Some(4), None]);

    // Up one each, the first first, so the two keep their order.
    rig.service
        .reorder(&[b, a], QueueMoveTarget::Offset(-1))
        .await
        .unwrap();
    assert_eq!(kept(), [a, b, c, d, e]);
    // Down one each, the last first.
    rig.service
        .reorder(&[a, b], QueueMoveTarget::Offset(1))
        .await
        .unwrap();
    assert_eq!(kept(), [c, a, b, d, e]);
    // To the bottom the first first, to the top the last first.
    rig.service
        .reorder(&[a, c], QueueMoveTarget::Bottom)
        .await
        .unwrap();
    assert_eq!(kept(), [b, d, c, a, e]);
    rig.service
        .reorder(&[a, c], QueueMoveTarget::Top)
        .await
        .unwrap();
    assert_eq!(kept(), [c, a, b, d, e]);

    // A seed has no place to move to.
    rig.service
        .reorder(&[e], QueueMoveTarget::Top)
        .await
        .unwrap();
    assert_eq!(kept(), [c, a, b, d, e]);
    assert!(matches!(
        rig.service
            .reorder(&[JobId(9_999)], QueueMoveTarget::Top)
            .await,
        Err(ActionError::NotFound(JobId(9_999)))
    ));

    // A forced torrent runs past the limit and keeps its place.
    rig.service.set_forced(d, true).await.unwrap();
    assert_eq!(queued().await, [true, true, false, false, false]);
    assert_eq!(places().await, [Some(2), Some(3), Some(1), Some(4), None]);
}

// An add reads no other torrent, so adding one costs the same however many
// there are: it takes its turn and rank from the end of the queue as the
// last pass left it, and puts its own job in the queue. A change to an
// earlier torrent waits for the next pass, and the exporter's reading for
// the next publish of every torrent.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_add_reads_no_other_torrent() {
    let rig = rig(set_clock());
    // No session is built while the route is down.
    rig.routes.set_down(Some("down"));
    let queue = queue();
    let state = queue.state().clone();
    rig.service.publish_to(queue);
    // Not opened: no job mail is heard and nothing settles, so only what
    // the test does publishes.
    let mut settings = rig.service.settings();
    settings.torrent.max_active_downloads = 1;
    rig.service.set_settings(settings).await;
    let add = |seed: u8| {
        let service = rig.service.clone();
        async move {
            service
                .add_opened(request(AddSource::Metainfo(plain(
                    &format!("set{seed}"),
                    seed,
                ))))
                .await
                .unwrap()
        }
    };
    let a = add(1).await;
    let b = add(2).await;
    // `a` finishes after the last pass: it seeds, and gives up its slot
    // and its rank at the next.
    rig.service
        .inner
        .live
        .lock()
        .await
        .get_mut(&a)
        .unwrap()
        .record
        .completed_at_ms = Some(EPOCH_MS);
    let mut exported = rig.service.inner.published.subscribe();
    exported.mark_unchanged();

    let c = add(3).await;
    let turns = || async {
        let live = rig.service.inner.live.lock().await;
        [a, b, c].map(|id| (live[&id].queued, live[&id].queue_rank))
    };
    assert_eq!(
        turns().await,
        [(false, Some(1)), (true, Some(2)), (true, Some(3))]
    );
    assert!(!exported.has_changed().unwrap());
    let jobs: Vec<JobId> = state.list_jobs().iter().map(|job| job.job_id).collect();
    assert_eq!(jobs, [a, b, c]);

    // The next pass, as the tick runs it, settles what changed.
    rig.service.apply_queue().await;
    assert_eq!(
        turns().await,
        [(false, None), (false, Some(1)), (true, Some(2))]
    );
}

// A rig whose service has one download slot and no session (the route is
// down), not opened: nothing settles but what the test runs. With a way
// to add the torrent of `seed`, and each torrent's turn: waiting for a
// slot, and its rank.
struct OneSlot {
    rig: Rig,
}

impl OneSlot {
    async fn new() -> Self {
        let rig = rig(set_clock());
        rig.routes.set_down(Some("down"));
        rig.service.publish_to(queue());
        let mut settings = rig.service.settings();
        settings.torrent.max_active_downloads = 1;
        rig.service.set_settings(settings).await;
        Self { rig }
    }

    async fn add(&self, seed: u8) -> JobId {
        self.rig
            .service
            .add_opened(request(AddSource::Metainfo(plain(
                &format!("set{seed}"),
                seed,
            ))))
            .await
            .unwrap()
    }

    async fn turns(&self, jobs: &[JobId]) -> Vec<(bool, Option<u32>)> {
        let live = self.rig.service.inner.live.lock().await;
        jobs.iter()
            .map(|id| (live[id].queued, live[id].queue_rank))
            .collect()
    }
}

// A slot and a rank freed where no pass runs (a torrent removed, failed or
// finished) go to the torrents after it at the next settle, not the next
// tick (libtorrent `remove_torrent` and `torrent::finished` trigger the
// recalculation).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_slot_freed_without_a_pass_goes_out_at_the_next_settle() {
    let one = OneSlot::new().await;
    let service = &one.rig.service;
    let [a, b, c, d] = [
        one.add(1).await,
        one.add(2).await,
        one.add(3).await,
        one.add(4).await,
    ];
    assert_eq!(
        one.turns(&[a, b, c, d]).await,
        [
            (false, Some(1)),
            (true, Some(2)),
            (true, Some(3)),
            (true, Some(4))
        ]
    );

    service.remove(a, false, true).await.unwrap();
    service.settle().await;
    assert_eq!(
        one.turns(&[b, c, d]).await,
        [(false, Some(1)), (true, Some(2)), (true, Some(3))]
    );

    service.fail(b, "gone".into()).await;
    service.settle().await;
    assert_eq!(
        one.turns(&[c, d]).await,
        [(false, Some(1)), (true, Some(2))]
    );

    service.completed(c).await;
    service.settle().await;
    assert_eq!(one.turns(&[c, d]).await, [(false, None), (false, Some(1))]);
}

// A removal whose mark is refused changes nothing of the queue: the
// torrent keeps its slot and its rank, and the one behind it still waits.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_removal_whose_mark_is_refused_keeps_its_slot() {
    let one = OneSlot::new().await;
    let service = &one.rig.service;
    let [a, b] = [one.add(1).await, one.add(2).await];
    assert_eq!(
        one.turns(&[a, b]).await,
        [(false, Some(1)), (true, Some(2))]
    );
    crate::torrents::persistence::MARK_REFUSED.store(2, Ordering::Relaxed);
    let removed = service.remove(a, false, true).await;
    crate::torrents::persistence::MARK_REFUSED.store(0, Ordering::Relaxed);
    removed.expect_err("the removal fails");
    service.settle().await;
    assert_eq!(
        one.turns(&[a, b]).await,
        [(false, Some(1)), (true, Some(2))]
    );
}

// A seed whose removal's mark is refused still seeds and still announces.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_seed_whose_removals_mark_is_refused_still_announces() {
    let mut seeded = seeded().await;
    let service = seeded.rig.service.clone();
    crate::torrents::persistence::MARK_REFUSED.store(2, Ordering::Relaxed);
    let removed = service.remove(seeded.job_id, false, true).await;
    crate::torrents::persistence::MARK_REFUSED.store(0, Ordering::Relaxed);
    removed.expect_err("the removal fails");
    assert_eq!(seeded.view().await.state, TorrentState::Seeding);
    seeded.reannounced().await;
    assert_eq!(seeded.view().await.state, TorrentState::Seeding);
    seeded.stop().await;
}

// Two adds at once take their places in one order and can join the live
// torrents in the other. The one that arrives after a torrent placed
// behind it asks for a pass, and the next settle gives the slot and the
// first rank back to the one placed first.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_add_arriving_out_of_place_has_its_turn_settled_by_a_pass() {
    let one = OneSlot::new().await;
    let service = &one.rig.service;
    // The second add's place is taken before the first's, as when its
    // placing waits longer.
    service
        .inner
        .next_queue_position
        .store(5, Ordering::Relaxed);
    let behind = one.add(1).await;
    service
        .inner
        .next_queue_position
        .store(2, Ordering::Relaxed);
    let before = one.add(2).await;
    assert_eq!(
        one.turns(&[before, behind]).await,
        [(true, Some(2)), (false, Some(1))]
    );

    service.settle().await;
    assert_eq!(
        one.turns(&[before, behind]).await,
        [(false, Some(1)), (true, Some(2))]
    );
}

// A pause and a completion at once keep both: each writes its own column
// under the torrent's lock.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_pause_racing_completion_keeps_the_completion() {
    let rig = rig(set_clock());
    rig.routes.set_down(Some("down"));
    rig.service.open().await.unwrap();
    let job_id = rig
        .service
        .add_opened(request(AddSource::Metainfo(plain("set", 1))))
        .await
        .unwrap();
    let (paused, ()) = tokio::join!(rig.service.pause(job_id), rig.service.completed(job_id));
    paused.unwrap();
    let stored = rig.db.get_torrent(job_id).unwrap().unwrap();
    assert_eq!(stored.intent(), TorrentIntent::Paused);
    assert_eq!(stored.completed_at_ms, Some(EPOCH_MS));
    rig.service.resume(job_id).await.unwrap();
    let stored = rig.db.get_torrent(job_id).unwrap().unwrap();
    assert_eq!(stored.intent(), TorrentIntent::Resumed);
    assert_eq!(stored.completed_at_ms, Some(EPOCH_MS));
}

// A client that failed to start is never seated: every torrent intake is
// refused as the registry's "not running", as though no client were there,
// and one that starts is seated.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_client_that_fails_to_start_is_not_seated_as_the_torrent_class() {
    use crate::ingest::Kind;
    use crate::jobs::{AddRefusal, RefusalReason};
    use crate::persistence::sql_runtime::SqlRuntime;

    let magnet = "magnet:?xt=urn:btih:0123456789abcdef0123456789abcdef01234567".to_owned();
    let kinds = [Kind::Metainfo, Kind::Magnet(magnet)];

    let rig = rig(set_clock());
    let queue = queue();
    let state = queue.state().clone();
    let store = rig.db.datastore();
    rig.db
        .run_sql_blocking(async move {
            SqlRuntime::run_in_transaction(&store, "missing_tracker_rules_fixture", |tx| {
                Box::pin(async move {
                    tx.execute("DROP TABLE tracker_rules", &[]).await?;
                    Ok(())
                })
            })
            .await
        })
        .unwrap();
    rig.service.wire_to(queue);
    assert!(rig.service.start_seated().await.is_err());
    assert!(state.classes().get(Protocol::Torrent).is_none());
    for kind in &kinds {
        let refusal = match state.classes().route(kind) {
            Err(refusal) => refusal,
            Ok(_) => panic!("{kind:?} was taken by a client that did not start"),
        };
        assert_eq!(refusal, AddRefusal::not_running(Protocol::Torrent));
        assert_eq!(refusal.reason, RefusalReason::NotRunning);
    }

    let started = super::rig(set_clock());
    let queue = super::queue();
    let state = queue.state().clone();
    started.service.wire_to(queue);
    started.service.start_seated().await.unwrap();
    assert!(state.classes().get(Protocol::Torrent).is_some());
    for kind in &kinds {
        assert!(
            matches!(
                state.classes().route(kind),
                Ok(crate::jobs::Route::Class(_))
            ),
            "{kind:?}"
        );
    }
}

// A batch of actions on torrents is one unit: one change to the published
// queue for a run of pauses or resumes where one at a time made one each,
// and the same results and the same rows as those one at a time. A move
// changes no published job, so its one publish is not counted here.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_batch_of_torrent_actions_ends_as_the_same_actions_one_at_a_time() {
    use crate::jobs::handle::{CancellationOrigin, QueueAction, QueueMoveTarget};

    async fn seeded() -> (Rig, SchedulerHandle, Vec<JobId>) {
        let rig = rig(set_clock());
        // No session is built while the route is down.
        rig.routes.set_down(Some("down"));
        let queue = queue();
        rig.service.publish_to(queue.clone());
        rig.service.open().await.unwrap();
        let mut ids = Vec::new();
        for seed in 1..=5 {
            ids.push(
                rig.service
                    .add_opened(request(AddSource::Metainfo(plain(
                        &format!("set{seed}"),
                        seed,
                    ))))
                    .await
                    .unwrap(),
            );
        }
        (rig, queue, ids)
    }
    let rows = |rig: &Rig, ids: &[JobId]| {
        ids.iter()
            .map(|id| {
                rig.db.get_torrent(*id).unwrap().map(|record| {
                    (
                        record.intent(),
                        record.category_id,
                        record.forced,
                        record.queue_position,
                    )
                })
            })
            .collect::<Vec<_>>()
    };
    let (batched, batched_queue, ids) = seeded().await;
    let (single, single_queue, single_ids) = seeded().await;
    assert_eq!(ids, single_ids);
    let [a, b, c, d, e] = ids[..] else {
        unreachable!()
    };
    let missing = JobId(9_999);
    let update = |category: FieldUpdate<String>, priority: &str| {
        QueueAction::Update(JobUpdate {
            category,
            metadata: FieldUpdate::Set(vec![("priority".into(), priority.into())]),
            ..JobUpdate::default()
        })
    };
    // Each step's actions, and how many changes one at a time makes to the
    // published queue when it is counted.
    type Step = (Vec<(JobId, QueueAction)>, Option<u64>);
    let steps: Vec<Step> = vec![
        (
            vec![
                (a, QueueAction::Pause),
                (b, QueueAction::Pause),
                (missing, QueueAction::Pause),
                (c, QueueAction::Pause),
            ],
            Some(3),
        ),
        (
            vec![
                (a, QueueAction::Resume),
                (missing, QueueAction::Resume),
                (b, QueueAction::Resume),
            ],
            Some(2),
        ),
        (
            vec![
                (a, update(FieldUpdate::Set("tv".into()), "HIGH")),
                (
                    b,
                    QueueAction::Update(JobUpdate {
                        name: Some("other".into()),
                        ..JobUpdate::default()
                    }),
                ),
                (c, update(FieldUpdate::Set("nope".into()), "HIGH")),
                (d, update(FieldUpdate::Clear, "HIGH")),
                (missing, update(FieldUpdate::Unchanged, "HIGH")),
            ],
            None,
        ),
        (
            vec![
                (c, QueueAction::Move(QueueMoveTarget::Top)),
                (a, QueueAction::Move(QueueMoveTarget::Offset(1))),
                (e, QueueAction::Move(QueueMoveTarget::Bottom)),
                (b, QueueAction::Move(QueueMoveTarget::Offset(-1))),
            ],
            None,
        ),
        // A move of a torrent not found ends the moves after it.
        (
            vec![
                (d, QueueAction::Move(QueueMoveTarget::Top)),
                (missing, QueueAction::Move(QueueMoveTarget::Top)),
                (a, QueueAction::Move(QueueMoveTarget::Top)),
            ],
            None,
        ),
        (
            vec![
                (a, QueueAction::Cancel(CancellationOrigin::User)),
                (missing, QueueAction::Cancel(CancellationOrigin::User)),
                (b, QueueAction::Cancel(CancellationOrigin::UserKeepingFiles)),
            ],
            None,
        ),
    ];
    let class = batched.service.class();
    for (actions, singles_published) in steps {
        let before = (batched_queue.job_revision(), single_queue.job_revision());
        let together: Vec<(JobId, String)> = class
            .act(actions.clone())
            .await
            .into_iter()
            .map(|(job_id, result)| (job_id, format!("{result:?}")))
            .collect();
        let mut alone = BTreeMap::new();
        let mut moves_failed = false;
        for (job_id, action) in actions {
            let moving = matches!(action, QueueAction::Move(_));
            if moving && moves_failed {
                continue;
            }
            let result = single
                .service
                .act(job_id, action)
                .await
                .map_err(super::super::queue::scheduler_error);
            moves_failed |= moving && result.is_err();
            alone.insert(job_id, format!("{result:?}"));
        }
        let alone: Vec<(JobId, String)> = alone.into_iter().collect();
        assert_eq!(together, alone);
        assert_eq!(rows(&batched, &ids), rows(&single, &ids));
        if let Some(published) = singles_published {
            assert_eq!(
                (
                    batched_queue.job_revision() - before.0,
                    single_queue.job_revision() - before.1
                ),
                (1, published),
                "{together:?}"
            );
        }
    }
    // What the steps leave, read apart from either path.
    let left = rows(&batched, &ids);
    assert_eq!(left[0], None);
    assert_eq!(left[1], None);
    assert_eq!(left[2].map(|row| row.0), Some(TorrentIntent::Paused));
    assert_eq!(left[3].map(|row| (row.1, row.2)), Some((None, true)));
}

// An ask the tunnel withdraws before torrents act on it closes no peer, and
// one withdrawn while they act closes none past the session it was read
// before: it is read just before each session's peers close.
#[test]
fn a_give_back_withdrawn_before_torrents_act_closes_nothing() {
    use crate::jobs::{Owed, TunnelGiveBack};
    use crate::torrents::sessions::RouteKey;
    use std::sync::atomic::AtomicBool;
    let route = |egress| {
        RouteKey::of(
            egress,
            &crate::proxies::EgressBinding::System,
            &crate::proxies::LegPath::Direct,
        )
    };
    let running = std::collections::BTreeMap::from([(route(1), 3usize), (route(2), 3)]);
    let held = Arc::new(AtomicBool::new(false));
    let ask = TunnelGiveBack {
        egress: 1,
        hops: vec![11],
        torrent_routes: vec![route(1), route(2)],
        owed: Owed::new(
            {
                let held = Arc::clone(&held);
                move || u64::from(held.load(Ordering::Acquire))
            },
            || {},
        ),
    };
    let closed = std::sync::Mutex::new(Vec::new());
    let close = |open: &usize, peers: usize| {
        closed.lock().unwrap().push(peers);
        // The waiter is served as the first session closes its peers.
        held.store(false, Ordering::Release);
        peers.min(*open)
    };

    assert_eq!(
        super::super::queue::give_back_through(&running, &ask, 4, close),
        0
    );
    assert!(
        closed.lock().unwrap().is_empty(),
        "withdrawn before it acted"
    );

    held.store(true, Ordering::Release);
    assert_eq!(
        super::super::queue::give_back_through(&running, &ask, 4, close),
        3
    );
    assert_eq!(
        *closed.lock().unwrap(),
        [4],
        "withdrawn after the first session"
    );
}

use crate::jobs::handle::QueueMoveTarget;

// A fixed run of numbers for the seeded orders below.
fn numbers(seed: u64) -> impl Iterator<Item = u64> {
    let mut state = seed;
    std::iter::repeat_with(move || {
        state = state
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(1_442_695_040_888_963_407);
        state >> 33
    })
}

const TARGETS: [QueueMoveTarget; 11] = [
    QueueMoveTarget::Top,
    QueueMoveTarget::Bottom,
    QueueMoveTarget::Offset(0),
    QueueMoveTarget::Offset(-1),
    QueueMoveTarget::Offset(1),
    QueueMoveTarget::Offset(-3),
    QueueMoveTarget::Offset(7),
    QueueMoveTarget::Offset(-250),
    QueueMoveTarget::Offset(400),
    QueueMoveTarget::Offset(i64::MIN),
    QueueMoveTarget::Offset(i64::MAX),
];

// A move of many torrents ends where one splice of each after another
// ended: every target, over queues with few, some and most torrents named.
#[test]
fn a_move_of_many_ends_where_the_splices_ended() {
    use crate::torrents::service::queue::moved_order;
    use std::collections::HashSet;
    for (seed, length, share) in [
        (1, 600, 2),
        (2, 600, 50),
        (3, 600, 95),
        (4, 3, 50),
        (5, 1, 100),
    ] {
        let order: Vec<JobId> = (0..length).map(JobId).collect();
        let named: Vec<JobId> = order
            .iter()
            .copied()
            .zip(numbers(seed))
            .filter(|(_, number)| number % 100 < share)
            .map(|(job_id, _)| job_id)
            .collect();
        let set: HashSet<JobId> = named.iter().copied().collect();
        for target in TARGETS {
            let first_first = match target {
                QueueMoveTarget::Top => false,
                QueueMoveTarget::Bottom => true,
                QueueMoveTarget::Offset(offset) => offset < 0,
            };
            let mut spliced = order.clone();
            let moving: Box<dyn Iterator<Item = &JobId>> = if first_first {
                Box::new(named.iter())
            } else {
                Box::new(named.iter().rev())
            };
            for &job_id in moving {
                crate::jobs::handle::splice_job_order(&mut spliced, job_id, target).unwrap();
            }
            assert_eq!(
                moved_order(order.clone(), &set, target),
                spliced,
                "{length} torrents, {} named, {target:?}",
                named.len()
            );
        }
    }
}

// A batch of moves, one torrent each, ends where each splice followed by
// a fresh read's order of torrents that share a place ended.
#[test]
fn a_batch_of_moves_ends_where_the_splices_and_reads_ended() {
    use crate::torrents::service::batch::Spliced;
    fn ties_by_job(order: &mut [JobId], places: &[i64]) {
        let mut start = 0;
        while start < places.len() {
            let mut end = start + 1;
            while end < places.len() && places[end] == places[start] {
                end += 1;
            }
            if end - start > 1 {
                order[start..end].sort_unstable();
            }
            start = end;
        }
    }
    for (seed, length, tie_share) in [(11, 500, 0), (12, 500, 30), (13, 500, 90), (14, 2, 100)] {
        // Places that rise, some shared by a run of torrents.
        let mut place = 0;
        let mut rows: Vec<(i64, JobId)> = (0..length)
            .zip(numbers(seed))
            .map(|(job, number)| {
                if number % 100 >= tie_share {
                    place += 1;
                }
                (place, JobId(number % 100_000 * 1_000 + job))
            })
            .collect();
        rows.sort_unstable();
        let places: Vec<i64> = rows.iter().map(|(place, _)| *place).collect();
        let order: Vec<JobId> = rows.iter().map(|(_, job_id)| *job_id).collect();
        let moves: Vec<(JobId, QueueMoveTarget)> = numbers(seed + 100)
            .take(800)
            .map(|number| {
                let job_id = order[(number as usize) % order.len()];
                (job_id, TARGETS[(number as usize / 7) % TARGETS.len()])
            })
            .chain([(JobId(u64::MAX), QueueMoveTarget::Top)])
            .collect();
        let mut by_splices = order.clone();
        let mut spliced_then = Vec::new();
        for &(job_id, target) in &moves {
            let spliced = crate::jobs::handle::splice_job_order(&mut by_splices, job_id, target)
                .unwrap_or(false);
            ties_by_job(&mut by_splices, &places);
            spliced_then.push(spliced);
        }
        let mut batch = Spliced::new(order.clone(), &places);
        let spliced_now: Vec<bool> = moves
            .iter()
            .map(|&(job_id, target)| batch.splice(job_id, target))
            .collect();
        assert_eq!(spliced_now, spliced_then, "seed {seed}");
        assert_eq!(batch.order, by_splices, "seed {seed}");
    }
}
