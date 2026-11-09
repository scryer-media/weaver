// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A torrent's job raises the queue events an NZB job raises, at the
// matching points, and Weaver's seeding events, each once and with the
// torrent's variables.

use super::*;
use crate::post_processing::events::EventContext;
use crate::post_processing::model::{QueueEvent, vars};

// The variables every event of `view`'s job carries.
fn assert_torrent_vars(context: &EventContext, view: &TorrentView) {
    let (_, content) = view.paths();
    assert_eq!(context.job_id, Some(view.job_id.0));
    assert_eq!(context.env[vars::PROTOCOL], "torrent");
    assert_eq!(
        context.env[vars::INFOHASH_V1],
        view.infohash_v1.clone().unwrap()
    );
    assert_eq!(context.env[vars::INFOHASH_V2], "");
    assert_eq!(context.env["NZBNA_NZBNAME"], view.name);
    assert_eq!(context.env["NZBNA_DIRECTORY"], content);
}

// Added, downloaded in this run, paused while seeding, restarted with no
// event, then removed while seeding: NZB_ADDED, NZB_DOWNLOADED and each
// SEEDING_STOPPED once, with the torrent's variables.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_torrents_job_raises_each_queue_event_once_with_its_variables() {
    let data: Vec<u8> = (0..65_536u32).map(|at| (at % 251) as u8).collect();
    let files = [("set/a.bin", data.as_slice())];
    let private = metainfo("set", &files, 16_384, true);
    let seeds = tempfile::tempdir().unwrap();
    let seeding = seeder(seeds.path(), &private, &files).await;
    let std::net::SocketAddr::V4(peer) = seeding.listen_addr().unwrap() else {
        panic!("the seeder listens on IPv4 loopback");
    };
    let url = tracker(peer).await;

    let rig = rig(set_clock());
    rig.db.enable_queue_scripts_for_test(rig.dir.path());
    on_loopback(&rig.routes);
    rig.service.publish_to(queue());
    rig.service.open().await.unwrap();
    let job_id = rig
        .service
        .add(request(AddSource::Metainfo(private.to_vec())))
        .await
        .unwrap();
    rig.db.admitted_so_far().await;
    let added = rig.db.queued_script_events(QueueEvent::NzbAdded);
    assert_eq!(added.len(), 1);
    let view = rig.service.torrent(job_id).await.unwrap();
    assert_torrent_vars(&added[0], &view);
    assert_eq!(added[0].env["NZBNA_EVENT"], "NZB_ADDED");

    tracked(&rig.service, job_id, &url).await;
    handle_of(&rig.service, job_id)
        .await
        .wait_until_completed()
        .await
        .unwrap();
    let downloaded = rig
        .db
        .queued_script_events_once(QueueEvent::NzbDownloaded, 1)
        .await;
    let view = rig.service.torrent(job_id).await.unwrap();
    assert_torrent_vars(&downloaded[0], &view);
    // The tracker it announced to.
    assert_eq!(downloaded[0].env[vars::TRACKER], url);

    rig.service.pause(job_id).await.unwrap();
    rig.db.admitted_so_far().await;
    let stopped = rig.db.queued_script_events(QueueEvent::SeedingStopped);
    assert_eq!(stopped.len(), 1);
    assert_torrent_vars(&stopped[0], &view);
    assert_eq!(stopped[0].env[vars::SEEDING_STOPPED_REASON], "paused");
    // Paused again, it seeds no more: nothing new.
    rig.service.pause(job_id).await.unwrap();
    rig.service.stop().await;
    let added_before = rig.db.queued_script_events(QueueEvent::NzbAdded).len();

    // A restart raises nothing: the torrent was added, and downloaded, in
    // an earlier run.
    let again = service_on(&rig.db, rig.dir.path(), &rig.routes, job_env(), set_clock());
    again.publish_to(queue());
    reopen(&again).await;
    again.resume(job_id).await.unwrap();
    again.remove(job_id, false, true).await.unwrap();
    rig.db.admitted_so_far().await;
    assert_eq!(
        rig.db.queued_script_events(QueueEvent::NzbAdded).len(),
        added_before
    );
    assert_eq!(
        rig.db.queued_script_events(QueueEvent::NzbDownloaded).len(),
        1
    );
    let stopped = rig.db.queued_script_events(QueueEvent::SeedingStopped);
    let reasons: Vec<&str> = stopped
        .iter()
        .map(|context| context.env[vars::SEEDING_STOPPED_REASON].as_str())
        .collect();
    assert_eq!(reasons, ["paused", "removed"]);
    // A complete torrent removed is not a deleted download.
    assert!(
        rig.db
            .queued_script_events(QueueEvent::NzbDeleted)
            .is_empty()
    );
    again.stop().await;
    seeding.stop().await;
}

// A torrent added with its data in place downloads nothing in this run, so
// it seeds without NZB_DOWNLOADED (libtorrent sends no `completed`, and
// qBittorrent no `torrentFinished`, for it).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_torrent_added_complete_raises_no_download_event() {
    let (metainfo, data) = placed_torrent();
    let rig = rig(set_clock());
    rig.db.enable_queue_scripts_for_test(rig.dir.path());
    on_loopback(&rig.routes);
    rig.service.open().await.unwrap();
    let save = rig.dir.path().join("placed-here");
    std::fs::create_dir_all(save.join("placed")).unwrap();
    std::fs::write(save.join("placed/a.bin"), &data).unwrap();
    let (job_id, view, _) = completed_placed(
        &rig,
        metainfo,
        Placement {
            save_path: Some(save.to_string_lossy().into_owned()),
            ..Placement::default()
        },
    )
    .await;
    assert_eq!(view.downloaded_bytes, 0);
    // Whether it downloaded is settled before it seeds.
    {
        let live = rig.service.inner.live.lock().await;
        let torrent = &live[&job_id];
        assert_eq!(
            torrent.counters.downloaded_bytes,
            torrent.downloaded_at_start
        );
    }
    rig.db.admitted_so_far().await;
    assert_eq!(rig.db.queued_script_events(QueueEvent::NzbAdded).len(), 1);
    assert!(
        rig.db
            .queued_script_events(QueueEvent::NzbDownloaded)
            .is_empty()
    );
    rig.service.stop().await;
}

// A torrent removed before it finished is a deleted download, as an NZB
// job deleted by hand is.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_torrent_removed_before_it_finished_raises_its_deletion() {
    let rig = rig(set_clock());
    rig.db.enable_queue_scripts_for_test(rig.dir.path());
    rig.service.publish_to(queue());
    rig.service.open().await.unwrap();
    let job_id = rig
        .service
        .add(request(AddSource::Metainfo(plain("set", 1))))
        .await
        .unwrap();
    let view = rig.service.torrent(job_id).await.unwrap();
    rig.service.remove(job_id, false, true).await.unwrap();
    rig.db.admitted_so_far().await;
    let deleted = rig.db.queued_script_events(QueueEvent::NzbDeleted);
    assert_eq!(deleted.len(), 1);
    assert_torrent_vars(&deleted[0], &view);
    assert_eq!(deleted[0].env["NZBNA_DELETESTATUS"], "MANUAL");
    assert!(
        rig.db
            .queued_script_events(QueueEvent::SeedingStopped)
            .is_empty()
    );
}

// A torrent that meets its goals raises SEEDING_FINISHED once, with what
// the goal does: stopped, it seeds no more and raises nothing again;
// removed, its event is raised once its job is in history.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_torrent_that_meets_its_goals_raises_seeding_finished_once() {
    let rig = rig(set_clock());
    rig.db.enable_queue_scripts_for_test(rig.dir.path());
    rig.service.publish_to(queue());
    rig.service.open().await.unwrap();
    let goal = |action| SeedingPolicy {
        seed_time_counting: Default::default(),
        ratio_goal: Leaf::Set(0.0),
        seed_time_goal_minutes: Leaf::Inherit,
        inactive_goal_minutes: Leaf::Inherit,
        mode: Leaf::Set(SeedingGoalMode::Any),
        action: Leaf::Set(action),
    };
    let mut views = Vec::new();
    for (name, action) in [
        ("stops", SeedingAction::Stop),
        ("goes", SeedingAction::Remove),
    ] {
        let job_id = rig
            .service
            .add(AddRequest {
                policy_override: Some(goal(action)),
                ..request(AddSource::Metainfo(plain(name, 1)))
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
        views.push(rig.service.torrent(job_id).await.unwrap());
    }
    rig.service.apply_goals(EPOCH_MS).await;
    rig.service.apply_goals(EPOCH_MS).await;
    rig.db.admitted_so_far().await;
    let finished = rig.db.queued_script_events(QueueEvent::SeedingFinished);
    assert_eq!(finished.len(), 2);
    for (context, (view, action)) in finished.iter().zip(views.iter().zip(["stop", "remove"])) {
        assert_torrent_vars(context, view);
        assert_eq!(context.env[vars::SEEDING_ACTION], action);
    }
    // The goal ends its seeding; it is not stopped short of it.
    assert!(
        rig.db
            .queued_script_events(QueueEvent::SeedingStopped)
            .is_empty()
    );
    assert_eq!(
        rig.db.get_torrent(views[0].job_id).unwrap().unwrap().intent,
        TorrentIntent::Stopped
    );
    assert!(rig.db.get_torrent(views[1].job_id).unwrap().is_none());
}

// With no queue script saved, a torrent raises nothing: NZB jobs and the
// queue are as they were.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_torrent_raises_nothing_without_queue_scripts() {
    let rig = rig(set_clock());
    rig.service.publish_to(queue());
    rig.service.open().await.unwrap();
    let job_id = rig
        .service
        .add(request(AddSource::Metainfo(plain("set", 1))))
        .await
        .unwrap();
    rig.service.remove(job_id, false, true).await.unwrap();
    rig.db.admitted_so_far().await;
    assert_eq!(rig.db.queue_script_count().unwrap(), 0);
}

// A torrent's queue script is told its job's tags as it starts, as an NZB
// job's is: one put on before the event was queued and one put on after.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_torrents_queue_script_is_told_its_jobs_tags() {
    let rig = rig(set_clock());
    rig.db.enable_queue_scripts_for_test(rig.dir.path());
    let out = rig.dir.path().join("told");
    let supervisor = rig.db.tag_reporting_queue_script_for_test(&out);
    rig.service.publish_to(queue());
    rig.service.open().await.unwrap();
    let job_id = rig
        .service
        .add(AddRequest {
            tags: vec!["linux".into()],
            ..request(AddSource::Metainfo(plain("set", 1)))
        })
        .await
        .unwrap();
    rig.db.admitted_so_far().await;
    let mut added = rig.db.queued_script_events(QueueEvent::NzbAdded);
    assert_eq!(added.len(), 1);
    rig.db.add_job_tags(&[job_id], ["iso"]).unwrap();
    let results = crate::post_processing::events::run_event(
        &rig.db,
        &mut added[0],
        "torrent-tags",
        None,
        Some(supervisor),
    )
    .await
    .unwrap();
    assert_eq!(results.len(), 1);
    assert_eq!(
        std::fs::read_to_string(out.join(job_id.0.to_string())).unwrap(),
        "iso,linux"
    );
    rig.service.stop().await;
}
