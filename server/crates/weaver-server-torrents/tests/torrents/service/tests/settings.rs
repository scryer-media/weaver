// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::*;
use crate::settings::persistence::SettingsPersistenceQueries;
use crate::settings::queries::SettingsQueries;
use crate::settings::repository::SettingsRepositoryQueries;
use crate::torrents::queries::TorrentsQueries;

// Turning torrents off closes their sessions and stops each torrent, and
// refuses new ones; turning them on starts the tick, which takes them up.
#[tokio::test(start_paused = true)]
async fn torrents_turn_off_and_on_while_running() {
    let clock = Arc::new(RuntimeClock::at_unix_ms(EPOCH_MS));
    let rig = rig(Arc::clone(&clock) as Arc<dyn Clock>);
    rig.service.open().await.unwrap();
    let job_id = rig
        .service
        .add_opened(request(AddSource::Metainfo(plain("set", 1))))
        .await
        .unwrap();
    assert!(rig.service.torrent(job_id).await.unwrap().route.is_some());

    let mut settings = rig.service.settings();
    settings.torrent.enabled = false;
    rig.service.set_settings(settings.clone()).await;
    let view = rig.service.torrent(job_id).await.unwrap();
    assert_eq!(view.state, TorrentState::Stopped);
    assert_eq!(
        (view.detail.as_deref(), view.route_reason),
        (None, Some(TorrentRouteReason::TorrentsDisabled))
    );
    assert!(rig.service.inner.sessions.views().await.is_empty());
    assert_eq!(
        rig.service
            .add_opened(request(AddSource::Metainfo(plain("other", 2))))
            .await,
        Err(AddError::Off)
    );
    assert!(rig.db.get_torrent(job_id).unwrap().is_some());

    settings.torrent.enabled = true;
    let mut ticks = rig.service.ticks();
    let base = *ticks.borrow_and_update();
    rig.service.set_settings(settings).await;
    ticks.wait_for(|ticks| *ticks > base).await.unwrap();
    let view = rig.service.torrent(job_id).await.unwrap();
    assert_ne!(view.state, TorrentState::Stopped);
    assert_eq!(view.route_reason, None);
    assert!(view.route.is_some());
    rig.service.stop().await;
}

// The settings with torrents turned off.
fn turned_off(service: &TorrentService) -> TorrentSettings {
    let mut settings = service.settings();
    settings.torrent.enabled = false;
    settings
}

// The wind-down a restart would read.
fn kept_wind_down(rig: &Rig) -> Option<String> {
    rig.db.get_setting(WIND_DOWN_KEY).unwrap()
}

// A seeding goal met as soon as the download is done, then a stop.
fn stops_once_done() -> Option<SeedingPolicy> {
    Some(SeedingPolicy {
        seed_time_counting: Default::default(),
        ratio_goal: Leaf::Set(0.0),
        seed_time_goal_minutes: Leaf::Inherit,
        inactive_goal_minutes: Leaf::Inherit,
        mode: Leaf::Set(SeedingGoalMode::Any),
        action: Leaf::Set(SeedingAction::Stop),
    })
}

// Turning off with "stop now" asked for is turning off as ever: every
// torrent stops at once, nothing is kept for a restart.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn turning_torrents_off_to_stop_now_stops_them_at_once() {
    let rig = rig(set_clock());
    rig.service.open().await.unwrap();
    let job_id = rig
        .service
        .add_opened(request(AddSource::Metainfo(plain("set", 1))))
        .await
        .unwrap();
    rig.service.wind_down_when_off(None).await;
    rig.service.set_settings(turned_off(&rig.service)).await;
    assert!(!rig.service.inner.sessions.on());
    assert_eq!(rig.service.wind_down(), None);
    assert_eq!(kept_wind_down(&rig), None);
    let view = rig.service.torrent(job_id).await.unwrap();
    assert_eq!(
        (view.state, view.route_reason),
        (
            TorrentState::Stopped,
            Some(TorrentRouteReason::TorrentsDisabled)
        )
    );
    assert_eq!(
        rig.service
            .add_opened(request(AddSource::Metainfo(plain("other", 2))))
            .await,
        Err(AddError::Off)
    );
}

// Turning off to let torrents finish keeps them running, takes no new
// one, and is kept for a restart; once the last has met its goal and
// stopped, torrents are off and nothing is kept.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn turning_torrents_off_to_let_them_finish_ends_after_the_last_goal() {
    let rig = rig(set_clock());
    rig.service.open().await.unwrap();
    let job_id = rig
        .service
        .add_opened(AddRequest {
            policy_override: stops_once_done(),
            ..request(AddSource::Metainfo(plain("set", 1)))
        })
        .await
        .unwrap();
    assert_eq!(
        rig.service.presence(),
        Presence {
            jobs: true,
            running: true
        }
    );
    rig.service.wind_down_when_off(Some(WindDown::Finish)).await;
    rig.service.set_settings(turned_off(&rig.service)).await;

    // Still running, and no torrent is new.
    assert!(rig.service.inner.sessions.on());
    assert_eq!(rig.service.wind_down(), Some(WindDown::Finish));
    assert_eq!(kept_wind_down(&rig).as_deref(), Some("finish"));
    let view = rig.service.torrent(job_id).await.unwrap();
    assert_ne!(view.state, TorrentState::Stopped);
    assert_eq!(view.route_reason, None);
    assert_eq!(
        rig.service
            .add_opened(request(AddSource::Metainfo(plain("other", 2))))
            .await,
        Err(AddError::Off)
    );
    // A tick with the torrent still downloading ends nothing.
    rig.service.tick().await;
    rig.service.restored().await;
    assert_eq!(rig.service.wind_down(), Some(WindDown::Finish));

    // Its download done, its goal is met: it stops, and torrents are off.
    complete(&rig, job_id).await;
    let mut wind_downs = rig.service.wind_downs();
    rig.service.tick().await;
    rig.service.restored().await;
    wind_downs
        .wait_for(|wind_down| wind_down.is_none())
        .await
        .unwrap();
    let _turning = rig.service.inner.turning.lock().await;
    assert!(!rig.service.inner.sessions.on());
    assert_eq!(kept_wind_down(&rig), None);
    assert_eq!(
        rig.db.get_torrent(job_id).unwrap().unwrap().intent(),
        TorrentIntent::Stopped
    );
    assert_eq!(
        rig.service.presence(),
        Presence {
            jobs: true,
            running: false
        }
    );
}

// Turning off to finish the downloads stops every torrent done
// downloading, a forced one included, and lets the others download; once
// the last is done, torrents are off.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn turning_torrents_off_to_finish_downloads_stops_seeding() {
    let rig = rig(set_clock());
    rig.service.open().await.unwrap();
    let seeding = rig
        .service
        .add_opened(AddRequest {
            forced: true,
            ..request(AddSource::Metainfo(plain("seeding", 1)))
        })
        .await
        .unwrap();
    let downloading = rig
        .service
        .add_opened(request(AddSource::Metainfo(plain("downloading", 2))))
        .await
        .unwrap();
    complete(&rig, seeding).await;
    rig.service
        .wind_down_when_off(Some(WindDown::FinishDownloads))
        .await;
    rig.service.set_settings(turned_off(&rig.service)).await;
    assert_eq!(kept_wind_down(&rig).as_deref(), Some("finish_downloads"));

    let intent = |job_id| rig.db.get_torrent(job_id).unwrap().unwrap().intent();
    rig.service.tick().await;
    rig.service.restored().await;
    assert_eq!(intent(seeding), TorrentIntent::Stopped);
    assert_eq!(intent(downloading), TorrentIntent::Resumed);
    assert!(rig.service.inner.sessions.on());
    assert_eq!(rig.service.wind_down(), Some(WindDown::FinishDownloads));

    complete(&rig, downloading).await;
    let mut wind_downs = rig.service.wind_downs();
    rig.service.tick().await;
    rig.service.restored().await;
    wind_downs
        .wait_for(|wind_down| wind_down.is_none())
        .await
        .unwrap();
    let _turning = rig.service.inner.turning.lock().await;
    assert_eq!(intent(downloading), TorrentIntent::Stopped);
    assert!(!rig.service.inner.sessions.on());
    assert_eq!(kept_wind_down(&rig), None);
}

// A wind-down asked for with nothing running is over before it starts:
// torrents are off at once. One asked for while torrents are already off
// is not taken.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_wind_down_with_nothing_running_turns_torrents_off_at_once() {
    let rig = rig(set_clock());
    rig.service.open().await.unwrap();
    rig.service.wind_down_when_off(Some(WindDown::Finish)).await;
    rig.service.set_settings(turned_off(&rig.service)).await;
    assert!(!rig.service.inner.sessions.on());
    assert_eq!(rig.service.wind_down(), None);
    assert_eq!(kept_wind_down(&rig), None);

    rig.service.wind_down_when_off(Some(WindDown::Finish)).await;
    assert_eq!(rig.service.wind_down(), None);
    assert_eq!(kept_wind_down(&rig), None);
    assert_eq!(
        rig.service
            .add_opened(request(AddSource::Metainfo(plain("set", 1))))
            .await,
        Err(AddError::Off)
    );
}

// A restart while torrents wind down keeps winding them down, sessions
// open; a restart with torrents on drops a wind-down kept.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_wind_down_is_kept_across_a_restart() {
    let rig = rig(set_clock());
    rig.service.open().await.unwrap();
    rig.service
        .add_opened(request(AddSource::Metainfo(plain("set", 1))))
        .await
        .unwrap();
    rig.service
        .wind_down_when_off(Some(WindDown::FinishDownloads))
        .await;
    rig.service.set_settings(turned_off(&rig.service)).await;
    rig.service.stop().await;

    let again = service_on(&rig.db, rig.dir.path(), &rig.routes, job_env(), set_clock());
    again.turn_at_start(false).await;
    assert_eq!(again.wind_down(), Some(WindDown::FinishDownloads));
    assert!(again.inner.sessions.on());

    let on = service_on(&rig.db, rig.dir.path(), &rig.routes, job_env(), set_clock());
    on.turn_at_start(true).await;
    assert_eq!(on.wind_down(), None);
    assert!(on.inner.sessions.on());
    assert_eq!(kept_wind_down(&rig), None);
}

// A rig winding down to let its one torrent finish, and that torrent.
async fn winding_down(wind_down: WindDown) -> (Rig, JobId) {
    let rig = rig(set_clock());
    rig.service.open().await.unwrap();
    let job_id = rig
        .service
        .add_opened(request(AddSource::Metainfo(plain("set", 1))))
        .await
        .unwrap();
    rig.service.wind_down_when_off(Some(wind_down)).await;
    rig.service.set_settings(turned_off(&rig.service)).await;
    assert_eq!(rig.service.wind_down(), Some(wind_down));
    assert!(kept_wind_down(&rig).is_some());
    (rig, job_id)
}

// Turning torrents on again ends a wind-down, and a restart finds none.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn turning_torrents_on_again_ends_a_wind_down() {
    let (rig, job_id) = winding_down(WindDown::Finish).await;
    let mut on = rig.service.settings();
    on.torrent.enabled = true;
    rig.service.set_settings(on).await;
    assert_eq!(rig.service.wind_down(), None);
    assert_eq!(kept_wind_down(&rig), None);
    assert!(rig.service.inner.sessions.on());
    assert_eq!(
        rig.db.get_torrent(job_id).unwrap().unwrap().intent(),
        TorrentIntent::Resumed
    );
}

// "Stop now" asked for while torrents wind down stops every one at once.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn stop_now_during_a_wind_down_stops_them_at_once() {
    let (rig, job_id) = winding_down(WindDown::Finish).await;
    // As the settings save sends it: the wind-down first, then the settings.
    assert_eq!(
        rig.service.wind_down_when_off(None).await,
        Some(WindDown::Finish)
    );
    rig.service.set_settings(turned_off(&rig.service)).await;
    assert!(!rig.service.inner.sessions.on());
    assert_eq!(rig.service.wind_down(), None);
    assert_eq!(kept_wind_down(&rig), None);
    let view = rig.service.torrent(job_id).await.unwrap();
    assert_eq!(
        (view.state, view.route_reason),
        (
            TorrentState::Stopped,
            Some(TorrentRouteReason::TorrentsDisabled)
        )
    );
}

// Removing the last torrent still running ends the wind-down at the next
// tick.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn removing_the_last_torrent_ends_a_wind_down() {
    let (rig, job_id) = winding_down(WindDown::Finish).await;
    rig.service.remove(job_id, false).await.unwrap();
    assert!(!rig.service.presence().running);
    let mut wind_downs = rig.service.wind_downs();
    rig.service.tick().await;
    rig.service.restored().await;
    wind_downs
        .wait_for(|wind_down| wind_down.is_none())
        .await
        .unwrap();
    let _turning = rig.service.inner.turning.lock().await;
    assert!(!rig.service.inner.sessions.on());
    assert_eq!(kept_wind_down(&rig), None);
}

// A wind-down a restart found, with nothing left running, ends at the
// first tick after it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_restart_with_nothing_running_ends_its_wind_down_at_the_first_tick() {
    let rig = rig(set_clock());
    rig.service.stop().await;
    rig.db.set_setting(WIND_DOWN_KEY, "finish").unwrap();
    let again = service_on(&rig.db, rig.dir.path(), &rig.routes, job_env(), set_clock());
    let mut settings = again.settings();
    settings.torrent.enabled = false;
    *again.inner.settings.write().unwrap() = settings;
    again.turn_at_start(false).await;
    assert_eq!(again.wind_down(), Some(WindDown::Finish));
    assert!(again.inner.sessions.on());

    let mut wind_downs = again.wind_downs();
    again.tick().await;
    again.restored().await;
    wind_downs
        .wait_for(|wind_down| wind_down.is_none())
        .await
        .unwrap();
    let _turning = again.inner.turning.lock().await;
    assert!(!again.inner.sessions.on());
    assert_eq!(kept_wind_down(&rig), None);
}

// The settings save takes the wind-down before it saves the settings that
// turn torrents off. Any caller that hands settings to the torrents in
// between, the old ones or the saved ones, keeps it: none turns a wind-down
// into a stop.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn settings_handed_on_around_the_save_keep_its_wind_down() {
    let rig = rig(set_clock());
    rig.service.open().await.unwrap();
    let job_id = rig
        .service
        .add_opened(request(AddSource::Metainfo(plain("set", 1))))
        .await
        .unwrap();
    let before = rig.service.settings();
    // The save's first step.
    rig.service.wind_down_when_off(Some(WindDown::Finish)).await;
    // Another caller, before the settings are saved, hands on the old ones.
    rig.service.set_settings(before.clone()).await;
    assert_eq!(rig.service.wind_down(), Some(WindDown::Finish));
    // Another caller, after they are saved, hands on the new ones before
    // the save does.
    let mut off = before;
    off.torrent.enabled = false;
    rig.service.set_settings(off.clone()).await;
    // And the save's own.
    rig.service.set_settings(off).await;
    assert_eq!(rig.service.wind_down(), Some(WindDown::Finish));
    assert_eq!(kept_wind_down(&rig).as_deref(), Some("finish"));
    assert!(rig.service.inner.sessions.on());
    let view = rig.service.torrent(job_id).await.unwrap();
    assert_ne!(view.state, TorrentState::Stopped);
    assert_eq!(view.route_reason, None);
}

// A save that failed after the wind-down was taken puts back what was
// there: nothing while torrents are on.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_failed_save_drops_the_wind_down_it_took() {
    let rig = rig(set_clock());
    rig.service.open().await.unwrap();
    rig.service
        .add_opened(request(AddSource::Metainfo(plain("set", 1))))
        .await
        .unwrap();
    let was = rig.service.wind_down_when_off(Some(WindDown::Finish)).await;
    assert_eq!(was, None);
    rig.service.keep_wind_down_after_failed_save(was).await;
    assert_eq!(rig.service.wind_down(), None);
    assert_eq!(kept_wind_down(&rig), None);
}

// Many seeds done downloading are stopped by one pass of the wind-down,
// written together, with the rows each stop alone wrote before, marked as
// stopped by the wind-down.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn finishing_downloads_stops_many_seeds_in_one_pass_as_one_by_one_did() {
    const SEEDS: u8 = 24;
    async fn seeded() -> (Rig, Vec<JobId>) {
        let rig = rig(set_clock());
        rig.service.open().await.unwrap();
        let mut ids = Vec::new();
        for seed in 1..=SEEDS {
            let job_id = rig
                .service
                .add_opened(request(AddSource::Metainfo(plain(
                    &format!("seed{seed}"),
                    seed,
                ))))
                .await
                .unwrap();
            complete(&rig, job_id).await;
            ids.push(job_id);
        }
        rig.service
            .wind_down_when_off(Some(WindDown::FinishDownloads))
            .await;
        rig.service.set_settings(turned_off(&rig.service)).await;
        (rig, ids)
    }

    // One by one, as each stop wrote its own row.
    let (old, old_ids) = seeded().await;
    let now = old.service.inner.clock.unix_ms();
    let landings: Vec<_> = {
        let mut live = old.service.inner.live.lock().await;
        old_ids
            .iter()
            .filter_map(|job_id| old.service.stop_torrent(live.get_mut(job_id).unwrap(), now))
            .collect()
    };
    for landing in landings {
        let _ = old.service.landed(Some(landing)).await;
    }

    // One pass.
    let (new, new_ids) = seeded().await;
    new.service
        .apply_goals(new.service.inner.clock.unix_ms())
        .await;

    // Each rig keeps its files in a folder of its own.
    let rows = |rig: &Rig, ids: &[JobId]| {
        ids.iter()
            .map(|job_id| {
                let mut row = rig.db.get_torrent(*job_id).unwrap().unwrap();
                let root = rig.dir.path().to_str().unwrap();
                row.storage_dir = row
                    .storage_dir
                    .map(|dir| dir.strip_prefix(root).unwrap().to_owned());
                row
            })
            .collect::<Vec<_>>()
    };
    let (old_rows, mut new_rows) = (rows(&old, &old_ids), rows(&new, &new_ids));
    assert!(
        new_rows
            .iter()
            .all(|row| row.intent() == TorrentIntent::Stopped && row.wound_down())
    );
    for row in &mut new_rows {
        row.kept = KeptIntent::Intent(TorrentIntent::Stopped);
    }
    assert_eq!(new_rows, old_rows);
}

// Many seeds that met their goals with a stop are stopped by one pass,
// written together, with the rows each stop alone wrote before.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn seeds_meeting_their_goals_stop_in_one_pass_as_one_by_one_did() {
    const SEEDS: u8 = 24;
    async fn seeded() -> (Rig, Vec<JobId>) {
        let rig = rig(set_clock());
        rig.service.open().await.unwrap();
        let mut ids = Vec::new();
        for seed in 1..=SEEDS {
            let job_id = rig
                .service
                .add_opened(AddRequest {
                    policy_override: stops_once_done(),
                    ..request(AddSource::Metainfo(plain(&format!("seed{seed}"), seed)))
                })
                .await
                .unwrap();
            complete(&rig, job_id).await;
            ids.push(job_id);
        }
        (rig, ids)
    }

    // One by one, as each stop wrote its own row.
    let (old, old_ids) = seeded().await;
    let now = old.service.inner.clock.unix_ms();
    let landings: Vec<_> = {
        let mut live = old.service.inner.live.lock().await;
        old_ids
            .iter()
            .filter_map(|job_id| old.service.stop_torrent(live.get_mut(job_id).unwrap(), now))
            .collect()
    };
    for landing in landings {
        let _ = old.service.landed(Some(landing)).await;
    }

    // One pass.
    let (new, new_ids) = seeded().await;
    new.service
        .apply_goals(new.service.inner.clock.unix_ms())
        .await;

    // Each rig keeps its files in a folder of its own.
    let rows = |rig: &Rig, ids: &[JobId]| {
        ids.iter()
            .map(|job_id| {
                let mut row = rig.db.get_torrent(*job_id).unwrap().unwrap();
                let root = rig.dir.path().to_str().unwrap();
                row.storage_dir = row
                    .storage_dir
                    .map(|dir| dir.strip_prefix(root).unwrap().to_owned());
                row
            })
            .collect::<Vec<_>>()
    };
    let (old_rows, new_rows) = (rows(&old, &old_ids), rows(&new, &new_ids));
    assert!(
        new_rows
            .iter()
            .all(|row| row.intent() == TorrentIntent::Stopped && !row.wound_down())
    );
    assert_eq!(new_rows, old_rows);
}

// Seeds a wind-down to the downloads stopped resume by themselves when
// torrents are turned on again. Those the operator paused, those their
// goals stopped, and one the wind-down stopped that the operator then
// paused stay as they are.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn seeds_a_wind_down_stopped_resume_when_torrents_are_on_again() {
    let rig = rig(set_clock());
    rig.service.open().await.unwrap();
    let add = |name: &'static str, seed: u8, policy_override| {
        let service = rig.service.clone();
        async move {
            service
                .add_opened(AddRequest {
                    policy_override,
                    ..request(AddSource::Metainfo(plain(name, seed)))
                })
                .await
                .unwrap()
        }
    };
    let wound = add("wound", 1, None).await;
    let then_paused = add("then-paused", 2, None).await;
    let paused = add("paused", 3, None).await;
    let met = add("met", 4, stops_once_done()).await;
    for job_id in [wound, then_paused, paused, met] {
        complete(&rig, job_id).await;
    }
    rig.service.pause(paused).await.unwrap();
    // Its goal met while torrents are on: stopped by its goal.
    rig.service.tick().await;
    rig.service.restored().await;
    let kept = |job_id| {
        let row = rig.db.get_torrent(job_id).unwrap().unwrap();
        (row.intent(), row.wound_down())
    };
    assert_eq!(kept(met), (TorrentIntent::Stopped, false));

    rig.service
        .wind_down_when_off(Some(WindDown::FinishDownloads))
        .await;
    rig.service.set_settings(turned_off(&rig.service)).await;
    rig.service.tick().await;
    rig.service.restored().await;
    assert_eq!(kept(wound), (TorrentIntent::Stopped, true));
    assert_eq!(kept(then_paused), (TorrentIntent::Stopped, true));
    assert_eq!(kept(paused), (TorrentIntent::Paused, false));
    // The operator's word ends the wind-down's hold.
    rig.service.pause(then_paused).await.unwrap();
    assert_eq!(kept(then_paused), (TorrentIntent::Paused, false));

    let mut on = rig.service.settings();
    on.torrent.enabled = true;
    rig.service.set_settings(on).await;
    assert_eq!(kept(wound), (TorrentIntent::Resumed, false));
    assert_eq!(kept(then_paused), (TorrentIntent::Paused, false));
    assert_eq!(kept(paused), (TorrentIntent::Paused, false));
    assert_eq!(kept(met), (TorrentIntent::Stopped, false));
    let view = rig.service.torrent(wound).await.unwrap();
    assert_ne!(view.state, TorrentState::Stopped);
    assert!(rig.service.presence().running);
}

// Seeds a wind-down stopped, kept across a restart with torrents on, resume
// as the restart opens.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn seeds_a_wind_down_stopped_resume_at_a_restart_with_torrents_on() {
    let rig = rig(set_clock());
    rig.service.open().await.unwrap();
    let job_id = rig
        .service
        .add_opened(request(AddSource::Metainfo(plain("seed", 1))))
        .await
        .unwrap();
    complete(&rig, job_id).await;
    rig.service
        .wind_down_when_off(Some(WindDown::FinishDownloads))
        .await;
    rig.service.set_settings(turned_off(&rig.service)).await;
    rig.service.tick().await;
    rig.service.restored().await;
    let kept = |job_id| {
        let row = rig.db.get_torrent(job_id).unwrap().unwrap();
        (row.intent(), row.wound_down())
    };
    assert_eq!(kept(job_id), (TorrentIntent::Stopped, true));
    rig.service.stop().await;

    let again = service_on(&rig.db, rig.dir.path(), &rig.routes, job_env(), set_clock());
    again.open().await.unwrap();
    assert_eq!(kept(job_id), (TorrentIntent::Resumed, false));
    assert_ne!(
        again.torrent(job_id).await.unwrap().state,
        TorrentState::Stopped
    );
    again.stop().await;
}

// A seed whose goal is met in the pass a wind-down to the downloads stops
// it ends by its goal: no mark, so it does not resume when torrents are on.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_seed_meeting_its_goal_as_a_wind_down_stops_it_is_stopped_by_its_goal() {
    let rig = rig(set_clock());
    rig.service.open().await.unwrap();
    let job_id = rig
        .service
        .add_opened(AddRequest {
            policy_override: stops_once_done(),
            ..request(AddSource::Metainfo(plain("met", 1)))
        })
        .await
        .unwrap();
    complete(&rig, job_id).await;
    rig.service
        .wind_down_when_off(Some(WindDown::FinishDownloads))
        .await;
    rig.service.set_settings(turned_off(&rig.service)).await;

    rig.service.tick().await;
    rig.service.restored().await;

    let row = rig.db.get_torrent(job_id).unwrap().unwrap();
    assert_eq!(
        (row.intent(), row.wound_down()),
        (TorrentIntent::Stopped, false)
    );
    let mut on = rig.service.settings();
    on.torrent.enabled = true;
    rig.service.set_settings(on).await;
    let row = rig.db.get_torrent(job_id).unwrap().unwrap();
    assert_eq!(
        (row.intent(), row.wound_down()),
        (TorrentIntent::Stopped, false)
    );
}

// A seed the wind-down stopped whose goal is met by the time torrents are
// on again (here the goal set while they were off) is not resumed: its mark
// is cleared and it stays stopped, beside one with a goal left, which
// resumes, in the same write.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_wound_down_seed_whose_goal_is_met_stays_stopped_when_torrents_are_on() {
    let rig = rig(set_clock());
    rig.service.open().await.unwrap();
    let met = rig
        .service
        .add_opened(request(AddSource::Metainfo(plain("met", 1))))
        .await
        .unwrap();
    let forced = rig
        .service
        .add_opened(AddRequest {
            forced: true,
            ..request(AddSource::Metainfo(plain("forced", 2)))
        })
        .await
        .unwrap();
    for job_id in [met, forced] {
        complete(&rig, job_id).await;
    }
    rig.service
        .wind_down_when_off(Some(WindDown::FinishDownloads))
        .await;
    rig.service.set_settings(turned_off(&rig.service)).await;
    rig.service.tick().await;
    rig.service.restored().await;
    let kept = |job_id| {
        let row = rig.db.get_torrent(job_id).unwrap().unwrap();
        (row.intent(), row.wound_down())
    };
    assert_eq!(kept(met), (TorrentIntent::Stopped, true));
    assert_eq!(kept(forced), (TorrentIntent::Stopped, true));

    let mut on = rig.service.settings();
    on.torrent.enabled = true;
    on.seeding = stops_once_done();
    let mut ticks = rig.service.ticks();
    let base = *ticks.borrow_and_update();
    rig.service.set_settings(on).await;

    assert_eq!(kept(met), (TorrentIntent::Stopped, false));
    // The tick that turning them on runs puts each in its session; each
    // then reads as checking what was saved of it until its session has it.
    ticks.wait_for(|ticks| *ticks > base).await.unwrap();
    super::listing::settled(&rig.service, 2).await;
    assert_eq!(
        rig.service.torrent(met).await.unwrap().state,
        TorrentState::Stopped
    );
    // A forced torrent seeds past its goal, so it resumes.
    assert_eq!(kept(forced), (TorrentIntent::Resumed, false));
}

#[test]
fn unpack_comes_from_the_torrent_then_its_rule_then_its_category_then_the_default() {
    let mut record = TorrentRecord {
        unpack_in_force: None,
        unpack_over: false,
        unpack_end: None,
        download_ends: 0,
        job_id: JobId(1),
        infohash_v1: Some([1; 20]),
        infohash_v2: None,
        name: "set".into(),
        source: TorrentSource::Metainfo,
        metainfo: None,
        category_id: Some(1),
        added_at_ms: EPOCH_MS,
        kept: KeptIntent::Intent(TorrentIntent::Resumed),
        forced: false,
        completed_at_ms: None,
        storage_dir: None,
        policy_override: None,
        unpack: None,
        metadata: Vec::new(),
        upload_limit: None,
        download_limit: None,
        sequential: false,
        first_last_piece_first: false,
        save_path: None,
        content_layout: None,
        files_dir: None,
        seed_mode: false,
        move_to: None,
        queue_position: 0,
        pool_member: None,
        private: false,
    };
    let mut rules = [TrackerRule {
        transports: Default::default(),
        archive_passwords: Default::default(),
        bound: None,
        id: 1,
        host_pattern: "tracker.test".into(),
        seeding: None,
        never_pause: false,
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
    }];
    let mut settings = TorrentSettings {
        upload_cap: None,
        complete_dir: PathBuf::from("/complete"),
        categories: vec![CategoryConfig {
            upload_cap: Default::default(),
            id: 1,
            name: "tv".into(),
            dest_dir: None,
            aliases: String::new(),
            seeding: None,
            torrent_route: false,
            unpack: None,
        }]
        .into(),
        seeding: None,
        torrent: TorrentConfig::default(),
    };

    assert!(settings.unpack(&record, rules.first()));
    settings.torrent.unpack = false;
    assert!(!settings.unpack(&record, rules.first()));
    settings.categories.edit(|list| list[0].unpack = Some(true));
    assert!(settings.unpack(&record, rules.first()));
    rules[0].unpack = Some(false);
    assert!(!settings.unpack(&record, rules.first()));
    record.unpack = Some(true);
    assert!(settings.unpack(&record, rules.first()));
}

// Each seeding term resolves on its own over the levels. A torrent's terms
// kept before terms were leaves read with their mode and action set, so
// they resolve as they did; terms set now that give only a ratio take the
// time goal from the category and the action from the global terms, and
// each term names the level it came from. The mode is never inherited: the
// torrent sets a goal, so its own mode holds, Any, over the category's All.
#[test]
fn each_seeding_term_resolves_over_the_levels_on_its_own() {
    let (mut record, rule, mut settings) = chain();
    settings.categories.edit(|list| {
        list[0].seeding = Some(SeedingPolicy {
            ratio_goal: Leaf::Set(1.0),
            seed_time_goal_minutes: Leaf::Set(72 * 60),
            mode: Leaf::Set(SeedingGoalMode::All),
            ..SeedingPolicy::default()
        })
    });

    // Kept before: every policy then carried its mode and action.
    record.policy_override = SeedingPolicy::kept(Some(
        serde_json::from_str(r#"{"ratio_goal":2.0,"mode":"any","action":"stop"}"#).unwrap(),
    ));
    let (seeding, levels) = settings.seeding(&record, Some(&rule));
    assert_eq!(
        seeding,
        Seeding {
            seed_time_counting: Default::default(),
            ratio_goal: Some(2.0),
            seed_time_goal_minutes: Some(72 * 60),
            inactive_goal_minutes: None,
            mode: SeedingGoalMode::Any,
            action: SeedingAction::Stop,
        }
    );
    assert_eq!(levels.mode, Level::Torrent);
    assert_eq!(levels.action, Level::Torrent);

    // Set now, with a ratio alone.
    record.policy_override = Some(SeedingPolicy {
        ratio_goal: Leaf::Set(2.0),
        ..SeedingPolicy::default()
    });
    let (seeding, levels) = settings.seeding(&record, Some(&rule));
    assert_eq!(
        seeding,
        Seeding {
            seed_time_counting: Default::default(),
            ratio_goal: Some(2.0),
            seed_time_goal_minutes: Some(72 * 60),
            inactive_goal_minutes: None,
            mode: SeedingGoalMode::Any,
            action: SeedingAction::Stop,
        }
    );
    assert_eq!(
        levels,
        SeedingLevels {
            seed_time_counting: Level::Global,
            ratio_goal: Level::Torrent,
            seed_time_goal_minutes: Level::Category,
            inactive_goal_minutes: Level::Global,
            mode: Level::Torrent,
            action: Level::Global,
            upload_cap: Level::Global,
        }
    );

    // A torrent with no goal of its own takes the category's mode with the
    // category's goals.
    record.policy_override = None;
    let (seeding, levels) = settings.seeding(&record, Some(&rule));
    assert_eq!(seeding.mode, SeedingGoalMode::All);
    assert_eq!(levels.mode, Level::Category);
}

// A term switched off at a level drops what the levels above set; one left
// to inherit takes it from them.
#[test]
fn off_drops_an_inherited_goal_and_inherit_takes_the_one_above() {
    let (mut record, mut rule, mut settings) = chain();
    settings.categories.edit(|list| {
        list[0].seeding = Some(SeedingPolicy {
            inactive_goal_minutes: Leaf::Set(60),
            ..SeedingPolicy::default()
        })
    });
    let inactive = |record: &TorrentRecord, rule: &TrackerRule| {
        let (seeding, levels) = settings.seeding(record, Some(rule));
        (seeding.inactive_goal_minutes, levels.inactive_goal_minutes)
    };
    assert_eq!(inactive(&record, &rule), (Some(60), Level::Category));

    rule.seeding = Some(SeedingPolicy {
        inactive_goal_minutes: Leaf::Off,
        ..SeedingPolicy::default()
    });
    assert_eq!(inactive(&record, &rule), (None, Level::TrackerRule));

    record.policy_override = Some(SeedingPolicy {
        inactive_goal_minutes: Leaf::Set(30),
        ..SeedingPolicy::default()
    });
    assert_eq!(inactive(&record, &rule), (Some(30), Level::Torrent));

    rule.seeding = None;
    record.policy_override = Some(SeedingPolicy {
        inactive_goal_minutes: Leaf::Off,
        ..SeedingPolicy::default()
    });
    assert_eq!(inactive(&record, &rule), (None, Level::Torrent));
    // A torrent's terms that set nothing are kept as none.
    assert_eq!(SeedingPolicy::kept(Some(SeedingPolicy::default())), None);
}

// The upload cap is the first level's that does not inherit: the global
// one, a category's over it, a tracker rule's over that, and a rule's off
// lifts a category's cap. The torrent's own limit is not a level of it: the
// tighter of the two holds.
#[test]
fn the_upload_cap_comes_from_the_first_level_that_sets_it() {
    use weaver_torrent::limits::LimitKind;

    let bps = |bps: u32| LimitKind::Bps(std::num::NonZeroU32::new(bps).unwrap());
    let (mut record, mut rule, mut settings) = chain();
    let cap = |settings: &TorrentSettings, record: &TorrentRecord, rule: &TrackerRule| {
        (
            settings.upload_cap(record, Some(rule)),
            settings.seeding(record, Some(rule)).1.upload_cap,
        )
    };
    assert_eq!(cap(&settings, &record, &rule), (None, Level::Global));
    settings.upload_cap = Some(5_000);
    assert_eq!(cap(&settings, &record, &rule), (Some(5_000), Level::Global));
    settings
        .categories
        .edit(|list| list[0].upload_cap = Leaf::Set(3_000));
    assert_eq!(
        cap(&settings, &record, &rule),
        (Some(3_000), Level::Category)
    );
    rule.upload_cap = Leaf::Off;
    assert_eq!(cap(&settings, &record, &rule), (None, Level::TrackerRule));
    rule.upload_cap = Leaf::Set(1_000);
    assert_eq!(
        cap(&settings, &record, &rule),
        (Some(1_000), Level::TrackerRule)
    );
    settings
        .categories
        .edit(|list| list[0].upload_cap = Leaf::Off);
    rule.upload_cap = Leaf::Inherit;
    assert_eq!(cap(&settings, &record, &rule), (None, Level::Category));

    let upload = |record: &TorrentRecord, cap| ratelimits_of(record, cap).upload_bps;
    assert_eq!(upload(&record, Some(1_000)), bps(1_000));
    record.upload_limit = Some(2_000);
    assert_eq!(upload(&record, Some(1_000)), bps(1_000));
    assert_eq!(upload(&record, None), bps(2_000));
    record.upload_limit = Some(500);
    assert_eq!(upload(&record, Some(1_000)), bps(500));
}

// A category is found by its id as the scan of the list found it: the first
// of an id, after a push or an edit too.
#[test]
fn a_category_by_its_id_is_the_first_of_that_id_in_the_list() {
    let category = |id: u32, name: &str| CategoryConfig {
        upload_cap: Default::default(),
        id,
        name: name.to_owned(),
        dest_dir: None,
        aliases: String::new(),
        seeding: None,
        torrent_route: false,
        unpack: None,
    };
    let scan = |list: &[CategoryConfig], id: u32| {
        list.iter()
            .find(|category| category.id == id)
            .map(|category| category.name.clone())
    };
    let list: Vec<CategoryConfig> = (0..200u32)
        .map(|n| category(n % 70, &format!("c{n}")))
        .collect();
    let mut categories = Categories::from(list);
    let same = |categories: &Categories| {
        for id in 0..80 {
            assert_eq!(
                categories.of(id).map(|category| category.name.clone()),
                scan(categories, id),
                "id {id}"
            );
        }
    };
    same(&categories);
    categories.push(category(75, "late"));
    categories.push(category(3, "shadowed"));
    same(&categories);
    categories.edit(|list| {
        list.remove(0);
        list[5].id = 79;
    });
    same(&categories);
}

// A torrent takes its tracker rule's route when the rule routes its torrents
// itself, else its category's when that does, else the default.
#[test]
fn a_torrents_route_is_its_rules_then_its_categorys_then_the_default() {
    let category = |id, torrent_route| CategoryConfig {
        upload_cap: Default::default(),
        id,
        name: format!("c{id}"),
        dest_dir: None,
        aliases: String::new(),
        seeding: None,
        torrent_route,
        unpack: None,
    };
    let settings = TorrentSettings {
        upload_cap: None,
        complete_dir: PathBuf::from("/complete"),
        categories: vec![category(1, true), category(2, false)].into(),
        seeding: None,
        torrent: TorrentConfig::default(),
    };
    let mut rule = TrackerRule {
        transports: Default::default(),
        archive_passwords: Default::default(),
        bound: None,
        id: 3,
        host_pattern: "tracker.test".into(),
        seeding: None,
        never_pause: false,
        unpack: None,
        client: None,
        torrent_route: true,
        dht: None,
        pex: None,
        lsd: None,
        encryption: None,
        upload_cap: Default::default(),
        forced: false,
        ssl: None,
    };

    for category in [Some(1), Some(2), None] {
        assert_eq!(
            settings.scope(category, Some(&rule)),
            TorrentScope::TrackerRule(3)
        );
    }
    rule.torrent_route = false;
    for rule in [Some(&rule), None] {
        assert_eq!(settings.scope(Some(1), rule), TorrentScope::Category(1));
        assert_eq!(settings.scope(Some(2), rule), TorrentScope::Default);
        assert_eq!(settings.scope(None, rule), TorrentScope::Default);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_torrent_keeps_the_unpack_in_force_once_its_job_is_built() {
    let rig = rig(set_clock());
    rig.service.open().await.unwrap();
    let inherits = rig
        .service
        .add_opened(request(AddSource::Metainfo(plain("inherits", 1))))
        .await
        .unwrap();
    let off = rig
        .service
        .add_opened(AddRequest {
            unpack: Some(false),
            ..request(AddSource::Metainfo(plain("off", 2)))
        })
        .await
        .unwrap();

    // What was asked is kept apart from what is in force.
    let kept = |job_id| {
        let record = rig.db.get_torrent(job_id).unwrap().unwrap();
        (record.unpack, record.unpack_in_force)
    };
    assert_eq!(kept(inherits), (None, Some(true)));
    assert_eq!(kept(off), (Some(false), Some(false)));
    let view = rig.service.torrent(inherits).await.unwrap();
    assert!(view.unpack);
    assert_eq!(view.unpack_override, None);
    assert!(!rig.service.torrent(off).await.unwrap().unpack);

    // A change of the level it inherits from leaves the built job's value
    // in force, and the torrent still asks for nothing of its own.
    let mut settings = rig.service.settings();
    settings.torrent.unpack = false;
    rig.service.set_settings(settings).await;
    rig.service.tick().await;
    rig.service.restored().await;
    assert_eq!(kept(inherits), (None, Some(true)));
    let view = rig.service.torrent(inherits).await.unwrap();
    assert!(view.unpack);
    assert_eq!(view.unpack_override, None);
    rig.service.stop().await;
}

// Seeding terms that set nothing are no terms: a torrent given them, on add
// or after, inherits its tracker rule's goal, mode and action instead of
// seeding with no goal under the default ones.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn empty_seeding_terms_inherit_the_chain() {
    let rig = rig(set_clock());
    on_loopback(&rig.routes);
    rig.service.open().await.unwrap();
    let ruled = SeedingPolicy {
        ratio_goal: Leaf::Set(2.0),
        mode: Leaf::Set(SeedingGoalMode::All),
        action: Leaf::Set(SeedingAction::Remove),
        ..SeedingPolicy::default()
    };
    rig.db
        .insert_tracker_rule(
            &TrackerRule {
                transports: Default::default(),
                archive_passwords: Default::default(),
                bound: None,
                id: 0,
                host_pattern: "*.test".into(),
                seeding: Some(ruled),
                never_pause: false,
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
    let job_id = rig
        .service
        .add_opened(AddRequest {
            paused: true,
            policy_override: Some(SeedingPolicy::default()),
            ..request(AddSource::Metainfo(announcing(
                &plain("ruled", 1),
                "http://tracker.test/announce",
            )))
        })
        .await
        .unwrap();
    let inherits = |view: TorrentView| {
        assert_eq!(view.seeding_override, None);
        assert_eq!(view.tracker_rule.as_deref(), Some("*.test"));
        assert_eq!(view.seeding_levels.ratio_goal, Level::TrackerRule);
        assert_eq!(view.seeding_levels.mode, Level::TrackerRule);
        assert_eq!(view.seeding_levels.action, Level::TrackerRule);
        assert_eq!(
            view.seeding_policy,
            Seeding {
                ratio_goal: Some(2.0),
                mode: SeedingGoalMode::All,
                action: SeedingAction::Remove,
                ..Seeding::default()
            }
        );
    };
    inherits(rig.service.torrent(job_id).await.unwrap());
    assert_eq!(
        rig.db.get_torrent(job_id).unwrap().unwrap().policy_override,
        None
    );

    let own = SeedingPolicy {
        seed_time_goal_minutes: Leaf::Set(60),
        ..SeedingPolicy::default()
    };
    rig.service
        .set_policy_override(job_id, Some(own))
        .await
        .unwrap();
    let view = rig.service.torrent(job_id).await.unwrap();
    assert_eq!(view.seeding_levels.seed_time_goal_minutes, Level::Torrent);
    assert_eq!(view.seeding_levels.ratio_goal, Level::TrackerRule);
    assert_eq!(view.seeding_policy.seed_time_goal_minutes, Some(60));
    assert_eq!(view.seeding_policy.ratio_goal, Some(2.0));
    rig.service
        .set_policy_override(job_id, Some(SeedingPolicy::default()))
        .await
        .unwrap();
    inherits(rig.service.torrent(job_id).await.unwrap());
    assert_eq!(
        rig.db.get_torrent(job_id).unwrap().unwrap().policy_override,
        None
    );
    rig.service.stop().await;
}

// The encryption setting reaches the open session when it changes: the
// next announce says it, and every connection from then on is made under
// it, with no reopen.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_new_encryption_setting_reaches_the_open_session_and_its_next_announce() {
    use crate::settings::TorrentEncryption;
    use tokio::io::AsyncWriteExt;
    use weaver_torrent::EncryptionPolicy;

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/announce", listener.local_addr().unwrap());
    let rig = rig(set_clock());
    on_loopback(&rig.routes);
    rig.service.open().await.unwrap();
    let job_id = rig
        .service
        .add_opened(request(AddSource::Metainfo(plain("crypt", 4))))
        .await
        .unwrap();
    tracked(&rig.service, job_id, &url).await;
    let body = b"d8:intervali1800e5:peers0:e";
    let head = format!(
        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    let answer = [head.as_bytes(), body].concat();
    let flags = |request: &str| {
        let line = request.lines().next().unwrap().to_owned();
        (
            line.contains("&supportcrypto=1"),
            line.contains("&requirecrypto=1"),
        )
    };

    // The answer to the announce just heard is in the table: one forced
    // while it waits would be overwritten by it (libtorrent
    // `tracker_response`). `before` is the answer the table held until then.
    let answered = |before: Option<std::time::SystemTime>| {
        move |announces: &[TrackerAnnounce]| {
            !announces[0].in_flight
                && announces[0]
                    .last
                    .as_ref()
                    .is_some_and(|last| last.result.is_ok() && Some(last.at) != before)
        }
    };
    let (mut started, request) = heard_request(&listener).await;
    assert_eq!(flags(&request), (true, false));
    started.write_all(&answer).await.unwrap();
    trackers_once(&rig.service, job_id, answered(None)).await;
    let session = session_of(&rig.service, job_id).await;
    assert_eq!(session.encryption(), EncryptionPolicy::Prefer);

    for (setting, policy, said) in [
        (
            TorrentEncryption::Require,
            EncryptionPolicy::Require,
            (true, true),
        ),
        (
            TorrentEncryption::Prefer,
            EncryptionPolicy::Prefer,
            (true, false),
        ),
    ] {
        let mut settings = rig.service.settings();
        settings.torrent.encryption = setting;
        rig.service.set_settings(settings).await;
        rig.service.inner.sessions.settled().await;
        let before = handle_of(&rig.service, job_id).await.tracker_announces()[0]
            .last
            .as_ref()
            .map(|last| last.at);
        rig.service.reannounce(job_id).await.unwrap();
        let (mut again, request) = heard_request(&listener).await;
        assert_eq!(flags(&request), said);
        again.write_all(&answer).await.unwrap();
        trackers_once(&rig.service, job_id, answered(before)).await;
        // The policy each connection is made under as it is made.
        assert_eq!(session.encryption(), policy);
    }
    assert!(Arc::ptr_eq(
        &session,
        &session_of(&rig.service, job_id).await
    ));
    rig.service.stop().await;
}
