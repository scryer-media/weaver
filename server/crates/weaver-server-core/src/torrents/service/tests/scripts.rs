// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A torrent's job raises TORRENT_ADDED, TORRENT_DOWNLOADED and
// TORRENT_DELETED where an NZB job raises its NZB_ events, each with its JOB_
// twin and never an NZB_ event, and Weaver's seeding events, each once and
// with the torrent's variables.

use super::*;
use crate::post_processing::events::EventContext;
use crate::post_processing::model::{QueueEvent, ScriptEventLabel, vars};

// No NZB_ event was queued for any job.
fn assert_no_nzb_events(db: &Database) {
    for event in [
        QueueEvent::NzbMarked,
        QueueEvent::NzbAdded,
        QueueEvent::NzbNamed,
        QueueEvent::NzbDownloaded,
        QueueEvent::NzbDeleted,
    ] {
        assert!(db.queued_script_events(event).is_empty(), "{event:?}");
    }
}

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
// event, then removed while seeding: TORRENT_ADDED, TORRENT_DOWNLOADED, their
// JOB_ twins and each SEEDING_STOPPED once, with the torrent's variables.
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
        .add_opened(request(AddSource::Metainfo(private.to_vec())))
        .await
        .unwrap();
    rig.db.admitted_so_far().await;
    let added = rig.db.queued_script_events(QueueEvent::TorrentAdded);
    assert_eq!(added.len(), 1);
    let view = rig.service.torrent(job_id).await.unwrap();
    assert_torrent_vars(&added[0], &view);
    assert_eq!(added[0].env["NZBNA_EVENT"], "TORRENT_ADDED");
    let job_added = rig.db.queued_script_events(QueueEvent::JobAdded);
    assert_eq!(job_added.len(), 1);
    assert_torrent_vars(&job_added[0], &view);
    assert_eq!(job_added[0].env["NZBNA_EVENT"], "JOB_ADDED");

    tracked(&rig.service, job_id, &url).await;
    handle_of(&rig.service, job_id)
        .await
        .wait_until_completed()
        .await
        .unwrap();
    let downloaded = rig
        .db
        .queued_script_events_once(QueueEvent::TorrentDownloaded, 1)
        .await;
    let view = rig.service.torrent(job_id).await.unwrap();
    assert_torrent_vars(&downloaded[0], &view);
    // The tracker it announced to.
    assert_eq!(downloaded[0].env[vars::TRACKER], url);
    assert_eq!(downloaded[0].env["NZBNA_EVENT"], "TORRENT_DOWNLOADED");
    let job_downloaded = rig
        .db
        .queued_script_events_once(QueueEvent::JobDownloaded, 1)
        .await;
    assert_torrent_vars(&job_downloaded[0], &view);
    assert_eq!(job_downloaded[0].env[vars::TRACKER], url);
    assert_eq!(job_downloaded[0].env["NZBNA_EVENT"], "JOB_DOWNLOADED");

    rig.service.pause(job_id).await.unwrap();
    rig.db.admitted_so_far().await;
    let stopped = rig.db.queued_script_events(QueueEvent::SeedingStopped);
    assert_eq!(stopped.len(), 1);
    assert_torrent_vars(&stopped[0], &view);
    assert_eq!(stopped[0].env[vars::SEEDING_STOPPED_REASON], "paused");
    // Paused again, it seeds no more: nothing new.
    rig.service.pause(job_id).await.unwrap();
    rig.service.stop().await;
    let added_before = rig.db.queued_script_events(QueueEvent::TorrentAdded).len();
    let job_added_before = rig.db.queued_script_events(QueueEvent::JobAdded).len();

    // A restart raises nothing: the torrent was added, and downloaded, in
    // an earlier run.
    let again = service_on(&rig.db, rig.dir.path(), &rig.routes, job_env(), set_clock());
    again.publish_to(queue());
    reopen(&again).await;
    again.resume(job_id).await.unwrap();
    again.remove(job_id, false, true).await.unwrap();
    rig.db.admitted_so_far().await;
    assert_eq!(
        rig.db.queued_script_events(QueueEvent::TorrentAdded).len(),
        added_before
    );
    assert_eq!(
        rig.db
            .queued_script_events(QueueEvent::TorrentDownloaded)
            .len(),
        1
    );
    assert_eq!(
        rig.db.queued_script_events(QueueEvent::JobAdded).len(),
        job_added_before
    );
    assert_eq!(
        rig.db.queued_script_events(QueueEvent::JobDownloaded).len(),
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
            .queued_script_events(QueueEvent::TorrentDeleted)
            .is_empty()
    );
    assert!(
        rig.db
            .queued_script_events(QueueEvent::JobDeleted)
            .is_empty()
    );
    assert_no_nzb_events(&rig.db);
    again.stop().await;
    seeding.stop().await;
}

// A torrent added with its data in place downloads nothing in this run, so
// it seeds without TORRENT_DOWNLOADED or JOB_DOWNLOADED (libtorrent sends no `completed`, and
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
    assert_eq!(
        rig.db.queued_script_events(QueueEvent::TorrentAdded).len(),
        1
    );
    assert!(
        rig.db
            .queued_script_events(QueueEvent::TorrentDownloaded)
            .is_empty()
    );
    assert!(
        rig.db
            .queued_script_events(QueueEvent::JobDownloaded)
            .is_empty()
    );
    rig.service.stop().await;
}

// A torrent removed before it finished is a deleted download, as an NZB
// job deleted by hand is: TORRENT_DELETED and JOB_DELETED, no NZB_ event.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_torrent_removed_before_it_finished_raises_its_deletion() {
    let rig = rig(set_clock());
    rig.db.enable_queue_scripts_for_test(rig.dir.path());
    rig.service.publish_to(queue());
    rig.service.open().await.unwrap();
    let job_id = rig
        .service
        .add_opened(request(AddSource::Metainfo(plain("set", 1))))
        .await
        .unwrap();
    let view = rig.service.torrent(job_id).await.unwrap();
    rig.service.remove(job_id, false, true).await.unwrap();
    rig.db.admitted_so_far().await;
    let deleted = rig.db.queued_script_events(QueueEvent::TorrentDeleted);
    assert_eq!(deleted.len(), 1);
    assert_torrent_vars(&deleted[0], &view);
    assert_eq!(deleted[0].env["NZBNA_DELETESTATUS"], "MANUAL");
    assert_eq!(deleted[0].env["NZBNA_EVENT"], "TORRENT_DELETED");
    let job_deleted = rig.db.queued_script_events(QueueEvent::JobDeleted);
    assert_eq!(job_deleted.len(), 1);
    assert_torrent_vars(&job_deleted[0], &view);
    assert_eq!(job_deleted[0].env["NZBNA_DELETESTATUS"], "MANUAL");
    assert_eq!(job_deleted[0].env["NZBNA_EVENT"], "JOB_DELETED");
    assert_no_nzb_events(&rig.db);
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
            .add_opened(AddRequest {
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

// A torrent whose goal removes it queues SEEDING_FINISHED once its row is
// gone, and the queue worker still runs it. Its TORRENT_ADDED, run only now,
// is skipped: the file its script writes comes from SEEDING_FINISHED.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_queue_runs_seeding_finished_of_a_torrent_its_goal_removed() {
    let rig = rig(set_clock());
    rig.db.enable_queue_scripts_for_test(rig.dir.path());
    let out = rig.dir.path().join("told");
    let supervisor = rig.db.tag_reporting_queue_script_for_test(&out);
    rig.service.publish_to(queue());
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
                action: Leaf::Set(SeedingAction::Remove),
            }),
            ..request(AddSource::Metainfo(plain("goes", 1)))
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
    rig.db.admitted_so_far().await;
    assert!(rig.db.get_torrent(job_id).unwrap().is_none());
    let mut ran = Vec::new();
    while let Some(context) = rig.db.run_next_queue_event_for_test(&supervisor).await {
        ran.push((context.job_id, context.event));
    }
    assert!(ran.contains(&(
        Some(job_id.0),
        ScriptEventLabel::Queue(QueueEvent::SeedingFinished)
    )));
    assert!(ran.contains(&(
        Some(job_id.0),
        ScriptEventLabel::Queue(QueueEvent::TorrentAdded)
    )));
    assert_eq!(
        std::fs::read_to_string(out.join(job_id.0.to_string())).unwrap(),
        ""
    );
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
        .add_opened(request(AddSource::Metainfo(plain("set", 1))))
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
        .add_opened(AddRequest {
            tags: vec!["linux".into()],
            ..request(AddSource::Metainfo(plain("set", 1)))
        })
        .await
        .unwrap();
    rig.db.admitted_so_far().await;
    let mut added = rig.db.queued_script_events(QueueEvent::TorrentAdded);
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

// The queue worker runs a live torrent's event: its job has a row in
// `torrents`, not `active_jobs`.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_queue_runs_a_live_torrents_event() {
    let rig = rig(set_clock());
    rig.db.enable_queue_scripts_for_test(rig.dir.path());
    let out = rig.dir.path().join("told");
    let supervisor = rig.db.tag_reporting_queue_script_for_test(&out);
    rig.service.publish_to(queue());
    rig.service.open().await.unwrap();
    let job_id = rig
        .service
        .add_opened(AddRequest {
            tags: vec!["linux".into()],
            ..request(AddSource::Metainfo(plain("set", 1)))
        })
        .await
        .unwrap();
    rig.db.admitted_so_far().await;
    let mut ran = Vec::new();
    while let Some(context) = rig.db.run_next_queue_event_for_test(&supervisor).await {
        ran.push((context.job_id, context.event));
    }
    assert!(ran.contains(&(
        Some(job_id.0),
        ScriptEventLabel::Queue(QueueEvent::TorrentAdded)
    )));
    assert_eq!(
        std::fs::read_to_string(out.join(job_id.0.to_string())).unwrap(),
        "linux"
    );
    rig.service.stop().await;
}

// One script saved for both TORRENT_ADDED and JOB_ADDED runs once for each,
// told the name of the one it runs for.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_script_on_the_own_and_the_generic_event_runs_for_each() {
    use std::os::unix::fs::PermissionsExt;
    let rig = rig(set_clock());
    rig.db.enable_queue_scripts_for_test(rig.dir.path());
    let out = rig.dir.path().join("told");
    let supervisor = rig.db.tag_reporting_queue_script_for_test(&out);
    let told = out.join("events");
    let script = rig
        .db
        .post_processing_script_directory()
        .unwrap()
        .join("queue.sh");
    std::fs::write(
        &script,
        format!(
            "#!/bin/sh\n### NZBGET QUEUE SCRIPT ###\necho \"$NZBNA_EVENT\" >> '{}'\n",
            told.display()
        ),
    )
    .unwrap();
    std::fs::set_permissions(&script, std::fs::Permissions::from_mode(0o755)).unwrap();
    rig.service.publish_to(queue());
    rig.service.open().await.unwrap();
    let job_id = rig
        .service
        .add_opened(request(AddSource::Metainfo(plain("set", 1))))
        .await
        .unwrap();
    rig.db.admitted_so_far().await;
    while rig
        .db
        .run_next_queue_event_for_test(&supervisor)
        .await
        .is_some()
    {}
    let mut ran: Vec<String> = std::fs::read_to_string(&told)
        .unwrap()
        .lines()
        .map(str::to_owned)
        .collect();
    ran.sort();
    assert_eq!(ran, ["JOB_ADDED", "TORRENT_ADDED"]);
    assert!(rig.service.torrent(job_id).await.is_some());
    rig.service.stop().await;
}

// A torrent of three files, each `seed` apart so no two tests share an info
// hash, added to a service with queue scripts whose settle runs only when the
// test calls it; and the storage its session writes through, whose
// `on_file_completed` is what the session says when a piece makes a file
// whole.
async fn three_files(rig: &Rig, seed: u8) -> (JobId, PieceStorage) {
    rig.db.enable_queue_scripts_for_test(rig.dir.path());
    rig.service.publish_to(queue());
    rig.service.restore().await.unwrap();
    let [a, b, c] = [[seed; 3000], [seed + 1; 3000], [seed + 2; 3000]];
    let bytes = metainfo(
        "set",
        &[("set/a.bin", &a), ("set/b.bin", &b), ("set/c.bin", &c)],
        1024,
        false,
    );
    let job_id = rig
        .service
        .add_opened(request(AddSource::Metainfo(bytes)))
        .await
        .unwrap();
    let (layout, intake) = {
        let live = rig.service.inner.live.lock().await;
        let torrent = &live[&job_id];
        (
            torrent.layout.clone().unwrap(),
            torrent.intake.clone().unwrap(),
        )
    };
    let storage = PieceStorage::new(
        layout,
        intake as Arc<dyn Pieces>,
        rig.service.resident(job_id),
    );
    (job_id, storage)
}

fn file_downloaded_interval(db: &Database, interval: i64) {
    let mut settings = db.post_processing_settings().unwrap();
    settings.event_scripts.file_downloaded_event_interval = interval;
    db.save_post_processing_settings(&settings).unwrap();
}

// A file of a torrent made whole queues FILE_DOWNLOADED for the torrent's
// job, with the variables its other events carry and no file named, as an
// NZB job's FILE_DOWNLOADED names none.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_torrent_file_made_whole_queues_file_downloaded() {
    let rig = rig(set_clock());
    let (job_id, storage) = three_files(&rig, 10).await;
    storage.on_file_completed(0);
    rig.service.settle().await;
    rig.db.admitted_so_far().await;
    let queued = rig.db.queued_script_events(QueueEvent::FileDownloaded);
    assert_eq!(queued.len(), 1);
    let view = rig.service.torrent(job_id).await.unwrap();
    assert_torrent_vars(&queued[0], &view);
    assert_eq!(queued[0].env["NZBNA_EVENT"], "FILE_DOWNLOADED");
    assert_no_nzb_events(&rig.db);
    rig.service.stop().await;
}

// With FILE_DOWNLOADED's interval at -1 a file made whole queues nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_torrent_file_made_whole_queues_nothing_at_interval_minus_one() {
    let rig = rig(set_clock());
    let (_, storage) = three_files(&rig, 20).await;
    file_downloaded_interval(&rig.db, -1);
    storage.on_file_completed(0);
    rig.service.settle().await;
    rig.db.admitted_so_far().await;
    assert!(
        rig.db
            .queued_script_events(QueueEvent::FileDownloaded)
            .is_empty()
    );
    rig.service.stop().await;
}

// At interval 0 each file queues FILE_DOWNLOADED, but a job holds at most
// one not yet started: files made whole while one waits add none. Once the
// torrent's download is over, a file heard after raises nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn each_file_of_a_torrent_queues_file_downloaded_one_waiting_at_a_time() {
    let rig = rig(set_clock());
    let out = rig.dir.path().join("told");
    let (job_id, storage) = three_files(&rig, 30).await;
    let supervisor = rig.db.tag_reporting_queue_script_for_test(&out);
    file_downloaded_interval(&rig.db, 0);

    // Two files whole before the first event runs: one waits.
    storage.on_file_completed(0);
    storage.on_file_completed(1);
    rig.service.settle().await;
    rig.db.admitted_so_far().await;
    assert_eq!(
        rig.db
            .queued_script_events(QueueEvent::FileDownloaded)
            .len(),
        1
    );
    while rig
        .db
        .run_next_queue_event_for_test(&supervisor)
        .await
        .is_some()
    {}

    // The one that waited ran: the next file queues another.
    storage.on_file_completed(2);
    rig.service.settle().await;
    rig.db.admitted_so_far().await;
    assert_eq!(
        rig.db
            .queued_script_events(QueueEvent::FileDownloaded)
            .len(),
        2
    );

    // Its download over, a file heard late raises nothing.
    rig.service
        .inner
        .live
        .lock()
        .await
        .get_mut(&job_id)
        .unwrap()
        .record
        .completed_at_ms = Some(EPOCH_MS);
    while rig
        .db
        .run_next_queue_event_for_test(&supervisor)
        .await
        .is_some()
    {}
    let before = rig
        .db
        .queued_script_events(QueueEvent::FileDownloaded)
        .len();
    storage.on_file_completed(2);
    rig.service.settle().await;
    rig.db.admitted_so_far().await;
    assert_eq!(
        rig.db
            .queued_script_events(QueueEvent::FileDownloaded)
            .len(),
        before
    );
    rig.service.stop().await;
}
