// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// One tick over torrents in every state the tick's rules look at, with what
// it leaves in the sessions, the rows, the script events and the views
// written out. The expectations below were taken by running these tests on
// the commit before the policy pass (e53173421) and writing out what they
// printed: the pass must leave the same.

use super::*;
use crate::post_processing::events::EventContext;
use crate::post_processing::model::QueueEvent;
use crate::torrents::FilePriority;
use crate::torrents::service::disk_room::DiskHeld;
use crate::torrents::service::settings::WindDown;

// A seeding goal met as soon as the download is done, then `action`.
pub(super) fn done_then(action: SeedingAction) -> Option<SeedingPolicy> {
    Some(SeedingPolicy {
        seed_time_counting: Default::default(),
        ratio_goal: Leaf::Set(0.0),
        seed_time_goal_minutes: Leaf::Inherit,
        inactive_goal_minutes: Leaf::Inherit,
        mode: Leaf::Set(SeedingGoalMode::Any),
        action: Leaf::Set(action),
    })
}

// The script events raised so far, of every kind, by kind, once each is
// admitted. The marker that waits for them is a URL's, which no torrent
// raises, and is left out.
pub(super) async fn events(db: &Database) -> Vec<(QueueEvent, Vec<EventContext>)> {
    db.admitted_so_far().await;
    QueueEvent::ALL
        .iter()
        .filter(|event| **event != QueueEvent::UrlCompleted)
        .map(|event| (*event, db.queued_script_events(*event)))
        .collect()
}

// What a tick leaves of `jobs`, by name: each session's run, picks and
// limits, the row kept, the view published and what the pass holds in
// memory; and the script events queued since `before`. The rig's folder
// reads as `<dir>`.
pub(super) async fn written(
    rig: &Rig,
    jobs: &[(&str, JobId)],
    before: &[(QueueEvent, Vec<EventContext>)],
) -> String {
    let mut out = String::new();
    for (name, job_id) in jobs {
        out.push_str(&format!("== {name}\n"));
        {
            let live = rig.service.inner.live.lock().await;
            match live.get(job_id) {
                None => out.push_str("live: none\n"),
                Some(torrent) => {
                    out.push_str(&format!(
                        "live: kept {:?} seed_mode {} seed_verified {} queued {} disk_held {:?} levels {:?}\n",
                        torrent.record.kept,
                        torrent.record.seed_mode,
                        torrent.seed_verified,
                        torrent.queued,
                        torrent.disk_held,
                        torrent.levels,
                    ));
                    if let Some(attached) = &torrent.attached {
                        let handle = &attached.handle;
                        out.push_str(&format!(
                            "session: paused {} seed_mode {} sequential {} first_last {} levels {:?} limits {:?}\n",
                            handle.is_paused(),
                            handle.seed_mode(),
                            handle.sequential(),
                            handle.first_last_piece_first(),
                            handle.file_priorities(),
                            handle.ratelimits(),
                        ));
                    } else {
                        out.push_str("session: none\n");
                    }
                }
            }
        }
        match rig.db.get_torrent(*job_id).unwrap() {
            None => out.push_str("row: none\n"),
            Some(row) => out.push_str(&format!(
                "row: intent {:?} wound_down {} forced {} seed_mode {} queue_position {} completed {:?} sequential {} first_last {} upload_limit {:?} seed_verified {:?}\n",
                row.intent(),
                row.wound_down(),
                row.forced,
                row.seed_mode,
                row.queue_position,
                row.completed_at_ms,
                row.sequential,
                row.first_last_piece_first,
                row.upload_limit,
                rig.db.torrent_seed_verified(*job_id).unwrap(),
            )),
        }
        match rig.service.torrent(*job_id).await {
            None => out.push_str("view: none\n"),
            Some(view) => out.push_str(&format!(
                "view: state {:?} stated {:?} detail {:?} paused_by {:?} forced {} queue_position {:?}\n",
                view.state,
                view.stated,
                view.detail,
                view.paused_by,
                view.forced,
                view.queue_position,
            )),
        }
    }
    let after = events(&rig.db).await;
    for ((event, was), (_, now)) in before.iter().zip(&after) {
        for context in &now[was.len()..] {
            out.push_str(&format!(
                "event {event:?}: job {:?} env {:?}\n",
                context.job_id, context.env,
            ));
        }
    }
    let dir = rig.dir.path().to_string_lossy().into_owned();
    out.replace(&dir, "<dir>")
}

// A torrent added and in its session.
pub(super) async fn add(rig: &Rig, name: &str, seed: u8, request: AddRequest) -> JobId {
    rig.service
        .add_opened(AddRequest {
            source: AddSource::Metainfo(plain(name, seed)),
            ..request
        })
        .await
        .unwrap()
}

// One tick with torrents on and one download at a time: one downloading,
// one queued past the limit, one forced past it, a seed meeting its goal
// for each action, one held for disk room its queue turn no longer lets
// start, one whose seed mode ended in the session, and one whose stored
// picks and limits differ from its session's.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_tick_over_torrents_in_every_state_leaves_what_it_left() {
    let rig = rig(set_clock());
    rig.db.enable_queue_scripts_for_test(rig.dir.path());
    rig.service.open().await.unwrap();
    let mut settings = rig.service.settings();
    settings.torrent.max_active_downloads = 1;
    rig.service.set_settings(settings).await;

    let base = || request(AddSource::Metainfo(Vec::new()));
    let downloading = add(&rig, "downloading", 1, base()).await;
    let queued = add(&rig, "queued", 2, base()).await;
    let forced = add(
        &rig,
        "forced",
        3,
        AddRequest {
            forced: true,
            ..base()
        },
    )
    .await;
    let mut met = Vec::new();
    for (name, seed, action) in [
        ("stop", 4, SeedingAction::Stop),
        ("remove", 5, SeedingAction::Remove),
        ("remove_data", 6, SeedingAction::RemoveWithData),
    ] {
        let job_id = add(
            &rig,
            name,
            seed,
            AddRequest {
                policy_override: done_then(action),
                ..base()
            },
        )
        .await;
        complete(&rig, job_id).await;
        met.push((name, job_id));
    }
    let held = add(&rig, "held", 7, base()).await;
    let seed_mode = add(&rig, "seed_mode", 8, base()).await;
    complete(&rig, seed_mode).await;
    rig.db
        .set_torrent_seed_verified(seed_mode, &[0b1000_0000])
        .unwrap();
    let picks = add(&rig, "picks", 9, base()).await;
    {
        let mut live = rig.service.inner.live.lock().await;
        live.get_mut(&held).unwrap().disk_held = Some(DiskHeld::Starting);
        let torrent = live.get_mut(&seed_mode).unwrap();
        torrent.record.seed_mode = true;
        torrent.seed_verified = 1;
        let torrent = live.get_mut(&picks).unwrap();
        torrent.record.sequential = true;
        torrent.record.first_last_piece_first = true;
        torrent.record.upload_limit = Some(4096);
        torrent.levels[0] = FilePriority::High;
    }

    let mut jobs = vec![
        ("downloading", downloading),
        ("queued", queued),
        ("forced", forced),
    ];
    jobs.extend(met);
    jobs.extend([("held", held), ("seed_mode", seed_mode), ("picks", picks)]);
    let before = events(&rig.db).await;
    rig.service.tick().await;
    rig.service.restored().await;
    let written = written(&rig, &jobs, &before).await;
    println!("{written}");
    assert_eq!(written, IN_EVERY_STATE);
}

// One tick with torrents off, winding down to the downloads alone: one still
// downloading, one done that stops for the wind-down, and a seed meeting its
// goal in the same pass, which stops by its goal.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_tick_winding_down_to_the_downloads_leaves_what_it_left() {
    let rig = rig(set_clock());
    rig.db.enable_queue_scripts_for_test(rig.dir.path());
    rig.service.open().await.unwrap();
    let base = || request(AddSource::Metainfo(Vec::new()));
    let downloading = add(&rig, "downloading", 1, base()).await;
    let done = add(&rig, "done", 2, base()).await;
    complete(&rig, done).await;
    let met = add(
        &rig,
        "met",
        3,
        AddRequest {
            policy_override: done_then(SeedingAction::Stop),
            ..base()
        },
    )
    .await;
    complete(&rig, met).await;
    rig.service
        .wind_down_when_off(Some(WindDown::FinishDownloads))
        .await;
    let mut off = rig.service.settings();
    off.torrent.enabled = false;
    rig.service.set_settings(off).await;

    let jobs = [("downloading", downloading), ("done", done), ("met", met)];
    let before = events(&rig.db).await;
    rig.service.tick().await;
    rig.service.restored().await;
    let written = written(&rig, &jobs, &before).await;
    println!("{written}");
    assert_eq!(written, WINDING_DOWN);
}

const IN_EVERY_STATE: &str = r#"== downloading
live: kept Intent(Resumed) seed_mode false seed_verified 0 queued false disk_held None levels [Normal]
session: paused false seed_mode false sequential false first_last false levels [Normal] limits LimitsConfig { upload_bps: Unlimited, download_bps: Unlimited }
row: intent Resumed wound_down false forced false seed_mode false queue_position 0 completed None sequential false first_last false upload_limit None seed_verified None
view: state Stalled stated None detail None paused_by None forced false queue_position Some(1)
== queued
live: kept Intent(Resumed) seed_mode false seed_verified 0 queued true disk_held None levels [Normal]
session: paused true seed_mode false sequential false first_last false levels [Normal] limits LimitsConfig { upload_bps: Unlimited, download_bps: Unlimited }
row: intent Resumed wound_down false forced false seed_mode false queue_position 1 completed None sequential false first_last false upload_limit None seed_verified None
view: state Queued stated None detail None paused_by None forced false queue_position Some(2)
== forced
live: kept Intent(Resumed) seed_mode false seed_verified 0 queued false disk_held None levels [Normal]
session: paused false seed_mode false sequential false first_last false levels [Normal] limits LimitsConfig { upload_bps: Unlimited, download_bps: Unlimited }
row: intent Resumed wound_down false forced true seed_mode false queue_position 2 completed None sequential false first_last false upload_limit None seed_verified None
view: state Stalled stated None detail None paused_by None forced true queue_position Some(3)
== stop
live: kept Intent(Stopped) seed_mode false seed_verified 0 queued false disk_held None levels [Normal]
session: paused true seed_mode false sequential false first_last false levels [Normal] limits LimitsConfig { upload_bps: Unlimited, download_bps: Unlimited }
row: intent Stopped wound_down false forced false seed_mode false queue_position 3 completed None sequential false first_last false upload_limit None seed_verified None
view: state Stopped stated Some(GoalMet) detail None paused_by None forced false queue_position None
== remove
live: none
row: none
view: none
== remove_data
live: none
row: none
view: none
== held
live: kept Intent(Resumed) seed_mode false seed_verified 0 queued true disk_held None levels [Normal]
session: paused true seed_mode false sequential false first_last false levels [Normal] limits LimitsConfig { upload_bps: Unlimited, download_bps: Unlimited }
row: intent Resumed wound_down false forced false seed_mode false queue_position 6 completed None sequential false first_last false upload_limit None seed_verified None
view: state Queued stated None detail None paused_by None forced false queue_position Some(4)
== seed_mode
live: kept Intent(Resumed) seed_mode false seed_verified 0 queued false disk_held None levels [Normal]
session: paused false seed_mode false sequential false first_last false levels [Normal] limits LimitsConfig { upload_bps: Unlimited, download_bps: Unlimited }
row: intent Resumed wound_down false forced false seed_mode false queue_position 7 completed None sequential false first_last false upload_limit None seed_verified None
view: state Seeding stated None detail None paused_by None forced false queue_position None
== picks
live: kept Intent(Resumed) seed_mode false seed_verified 0 queued true disk_held None levels [High]
session: paused true seed_mode false sequential true first_last true levels [High] limits LimitsConfig { upload_bps: Bps(4096), download_bps: Unlimited }
row: intent Resumed wound_down false forced false seed_mode false queue_position 8 completed None sequential false first_last false upload_limit None seed_verified None
view: state Queued stated None detail None paused_by None forced false queue_position Some(5)
event SeedingFinished: job Some(10003) env {"NZBNA_CATEGORY": "", "NZBNA_DELETESTATUS": "NONE", "NZBNA_DIRECTORY": "<dir>/complete/stop", "NZBNA_DUPEKEY": "", "NZBNA_DUPEMODE": "SCORE", "NZBNA_DUPESCORE": "0", "NZBNA_EVENT": "SEEDING_FINISHED", "NZBNA_FILENAME": "", "NZBNA_LASTID": "10003", "NZBNA_MARKSTATUS": "NONE", "NZBNA_NZBID": "10003", "NZBNA_NZBNAME": "stop", "NZBNA_PRIORITY": "0", "NZBNA_QUEUEDFILE": "", "NZBNA_URL": "", "NZBNA_URLSTATUS": "NONE", "WEAVER_INFOHASH_V1": "f6455c06663ea48c494fc8af85e4e47adc33237d", "WEAVER_INFOHASH_V2": "", "WEAVER_PROTOCOL": "torrent", "WEAVER_SEEDING_ACTION": "stop", "WEAVER_TRACKER": ""}
event SeedingFinished: job Some(10004) env {"NZBNA_CATEGORY": "", "NZBNA_DELETESTATUS": "NONE", "NZBNA_DIRECTORY": "<dir>/complete/remove", "NZBNA_DUPEKEY": "", "NZBNA_DUPEMODE": "SCORE", "NZBNA_DUPESCORE": "0", "NZBNA_EVENT": "SEEDING_FINISHED", "NZBNA_FILENAME": "", "NZBNA_LASTID": "10004", "NZBNA_MARKSTATUS": "NONE", "NZBNA_NZBID": "10004", "NZBNA_NZBNAME": "remove", "NZBNA_PRIORITY": "0", "NZBNA_QUEUEDFILE": "", "NZBNA_URL": "", "NZBNA_URLSTATUS": "NONE", "WEAVER_INFOHASH_V1": "2e0805d045a666ea2bb24f1ce2ebd11def4ab128", "WEAVER_INFOHASH_V2": "", "WEAVER_PROTOCOL": "torrent", "WEAVER_SEEDING_ACTION": "remove", "WEAVER_TRACKER": ""}
event SeedingFinished: job Some(10005) env {"NZBNA_CATEGORY": "", "NZBNA_DELETESTATUS": "NONE", "NZBNA_DIRECTORY": "<dir>/complete/remove_data", "NZBNA_DUPEKEY": "", "NZBNA_DUPEMODE": "SCORE", "NZBNA_DUPESCORE": "0", "NZBNA_EVENT": "SEEDING_FINISHED", "NZBNA_FILENAME": "", "NZBNA_LASTID": "10005", "NZBNA_MARKSTATUS": "NONE", "NZBNA_NZBID": "10005", "NZBNA_NZBNAME": "remove_data", "NZBNA_PRIORITY": "0", "NZBNA_QUEUEDFILE": "", "NZBNA_URL": "", "NZBNA_URLSTATUS": "NONE", "WEAVER_INFOHASH_V1": "d12fad486f13bff07f5582e2bcd09eef2415bc7e", "WEAVER_INFOHASH_V2": "", "WEAVER_PROTOCOL": "torrent", "WEAVER_SEEDING_ACTION": "remove_with_data", "WEAVER_TRACKER": ""}
"#;

const WINDING_DOWN: &str = r#"== downloading
live: kept Intent(Resumed) seed_mode false seed_verified 0 queued false disk_held None levels [Normal]
session: paused false seed_mode false sequential false first_last false levels [Normal] limits LimitsConfig { upload_bps: Unlimited, download_bps: Unlimited }
row: intent Resumed wound_down false forced false seed_mode false queue_position 0 completed None sequential false first_last false upload_limit None seed_verified None
view: state Stalled stated None detail None paused_by None forced false queue_position Some(1)
== done
live: kept WoundDown seed_mode false seed_verified 0 queued false disk_held None levels [Normal]
session: paused true seed_mode false sequential false first_last false levels [Normal] limits LimitsConfig { upload_bps: Unlimited, download_bps: Unlimited }
row: intent Stopped wound_down true forced false seed_mode false queue_position 1 completed None sequential false first_last false upload_limit None seed_verified None
view: state Stopped stated Some(GoalMet) detail None paused_by None forced false queue_position None
== met
live: kept Intent(Stopped) seed_mode false seed_verified 0 queued false disk_held None levels [Normal]
session: paused true seed_mode false sequential false first_last false levels [Normal] limits LimitsConfig { upload_bps: Unlimited, download_bps: Unlimited }
row: intent Stopped wound_down false forced false seed_mode false queue_position 2 completed None sequential false first_last false upload_limit None seed_verified None
view: state Stopped stated Some(GoalMet) detail None paused_by None forced false queue_position None
event SeedingFinished: job Some(10002) env {"NZBNA_CATEGORY": "", "NZBNA_DELETESTATUS": "NONE", "NZBNA_DIRECTORY": "<dir>/complete/met", "NZBNA_DUPEKEY": "", "NZBNA_DUPEMODE": "SCORE", "NZBNA_DUPESCORE": "0", "NZBNA_EVENT": "SEEDING_FINISHED", "NZBNA_FILENAME": "", "NZBNA_LASTID": "10002", "NZBNA_MARKSTATUS": "NONE", "NZBNA_NZBID": "10002", "NZBNA_NZBNAME": "met", "NZBNA_PRIORITY": "0", "NZBNA_QUEUEDFILE": "", "NZBNA_URL": "", "NZBNA_URLSTATUS": "NONE", "WEAVER_INFOHASH_V1": "532dddb6b739a1b5e37ae712d508fbeaf30810d2", "WEAVER_INFOHASH_V2": "", "WEAVER_PROTOCOL": "torrent", "WEAVER_SEEDING_ACTION": "stop", "WEAVER_TRACKER": ""}
"#;

// An idle tick, nothing changed since the last: the pass takes `live` once,
// walks the torrents twice, writes no row and queues no write, and the
// queue sees no job change.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_idle_tick_takes_the_lock_once_and_writes_nothing() {
    let rig = rig(set_clock());
    let handle = queue();
    rig.service.publish_to(handle.clone());
    rig.service.open().await.unwrap();
    let base = || request(AddSource::Metainfo(Vec::new()));
    add(&rig, "downloading", 1, base()).await;
    add(&rig, "queued", 2, base()).await;
    let seed = add(&rig, "seed", 3, base()).await;
    complete(&rig, seed).await;
    let mut settings = rig.service.settings();
    settings.torrent.max_active_downloads = 1;
    rig.service.set_settings(settings).await;
    // The first ticks write what the adds left.
    rig.service.tick().await;
    rig.service.restored().await;
    rig.service.tick().await;
    rig.service.restored().await;

    let mut changes = handle.state().subscribe_job_changes();
    changes.borrow_and_update();
    // Counted by the tick's pass alone: the settle loop `open` started
    // takes `live` and walks the torrents on its own, and is not counted.
    let counts = *rig.service.inner.pass_counts.borrow();
    let taken = rig.db.writes_taken();
    rig.service.tick().await;
    rig.service.restored().await;
    let after = *rig.service.inner.pass_counts.borrow();
    assert_eq!(
        (
            after.passes - counts.passes,
            after.locks - counts.locks,
            after.walks - counts.walks,
            after.writes - counts.writes,
        ),
        (1, 1, 2, 0)
    );
    assert_eq!(rig.db.writes_taken(), taken);
    assert!(!changes.has_changed().unwrap());
}
