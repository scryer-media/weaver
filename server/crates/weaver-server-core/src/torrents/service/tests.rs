// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use std::sync::atomic::{AtomicI64, Ordering};

use librqbit::storage::TorrentStorage;
use librqbit_core::lengths::Lengths;
use sha1::{Digest, Sha1};

use super::*;
use crate::jobs::{Hold, HoldOwner, HoldScope};
use crate::post_processing::model::PostProcessingSettings;
use crate::settings::{SeedingAction, SeedingGoalMode};
use crate::torrents::TrackerStatus;
use crate::torrents::sessions::Routes;
use crate::torrents::sessions::tests::{FakeRoutes, env};
use crate::torrents::status::ROUTE_DOWN_FETCHING;
use crate::torrents::storage::PieceStorage;

const EPOCH_MS: i64 = 1_700_000_000_000;

// A clock the test sets.
struct SetClock(AtomicI64);

impl Clock for SetClock {
    fn now_ms(&self) -> i64 {
        self.0.load(Ordering::SeqCst)
    }
}

// The runtime's clock, which a paused runtime moves only as timers fire.
struct RuntimeClock(tokio::time::Instant);

impl Clock for RuntimeClock {
    fn now_ms(&self) -> i64 {
        EPOCH_MS + self.0.elapsed().as_millis() as i64
    }
}

fn job_env() -> JobEnv {
    JobEnv::unbounded()
}

fn bstr(out: &mut Vec<u8>, bytes: &[u8]) {
    out.extend_from_slice(format!("{}:", bytes.len()).as_bytes());
    out.extend_from_slice(bytes);
}

fn bint(out: &mut Vec<u8>, value: usize) {
    out.extend_from_slice(format!("i{value}e").as_bytes());
}

// A torrent's metainfo: one file when the only file is named as the
// torrent, files under the torrent's directory otherwise.
fn metainfo(name: &str, files: &[(&str, &[u8])], piece: usize, private: bool) -> Vec<u8> {
    let whole: Vec<u8> = files
        .iter()
        .flat_map(|(_, bytes)| bytes.iter().copied())
        .collect();
    let pieces: Vec<u8> = whole
        .chunks(piece)
        .flat_map(|chunk| Sha1::digest(chunk).to_vec())
        .collect();
    let mut info = b"d".to_vec();
    if files.len() == 1 && files[0].0 == name {
        bstr(&mut info, b"length");
        bint(&mut info, whole.len());
    } else {
        bstr(&mut info, b"files");
        info.push(b'l');
        for (path, bytes) in files {
            info.push(b'd');
            bstr(&mut info, b"length");
            bint(&mut info, bytes.len());
            bstr(&mut info, b"path");
            info.push(b'l');
            for part in path.split('/') {
                bstr(&mut info, part.as_bytes());
            }
            info.extend_from_slice(b"ee");
        }
        info.push(b'e');
    }
    bstr(&mut info, b"name");
    bstr(&mut info, name.as_bytes());
    bstr(&mut info, b"piece length");
    bint(&mut info, piece);
    bstr(&mut info, b"pieces");
    bstr(&mut info, &pieces);
    if private {
        bstr(&mut info, b"private");
        bint(&mut info, 1);
    }
    info.push(b'e');
    let mut out = b"d".to_vec();
    bstr(&mut out, b"info");
    out.extend_from_slice(&info);
    out.push(b'e');
    out
}

fn plain(name: &str, seed: u8) -> Vec<u8> {
    metainfo(
        name,
        &[(&format!("{name}/a.bin"), &[seed; 3000])],
        1024,
        false,
    )
}

struct Rig {
    service: TorrentService,
    routes: Arc<FakeRoutes>,
    db: Database,
    dir: tempfile::TempDir,
}

fn rig(clock: Arc<dyn Clock>) -> Rig {
    rig_with(clock, job_env())
}

fn rig_with(clock: Arc<dyn Clock>, job_env: JobEnv) -> Rig {
    let dir = tempfile::tempdir().unwrap();
    let db = Database::open_in_memory().unwrap();
    let routes = Arc::new(FakeRoutes::default());
    let service = service_on(&db, dir.path(), &routes, job_env, clock);
    Rig {
        service,
        routes,
        db,
        dir,
    }
}

// A service over `db` and `dir`, as a restart would build it.
fn service_on(
    db: &Database,
    dir: &Path,
    routes: &Arc<FakeRoutes>,
    job_env: JobEnv,
    clock: Arc<dyn Clock>,
) -> TorrentService {
    let sessions = Sessions::new(
        Arc::clone(routes) as Arc<dyn Routes>,
        env(&dir.join("sessions")),
        db.clone(),
    );
    let settings = TorrentSettings {
        complete_dir: dir.join("complete"),
        categories: vec![CategoryConfig {
            id: 1,
            name: "tv".into(),
            dest_dir: None,
            aliases: String::new(),
            seeding: None,
            torrent_route: true,
            unpack: None,
        }],
        seeding: None,
        torrent: TorrentConfig {
            enabled: true,
            ..TorrentConfig::default()
        },
    };
    TorrentService::new(db.clone(), sessions, settings, job_env, clock)
}

fn set_clock() -> Arc<SetClock> {
    Arc::new(SetClock(AtomicI64::new(EPOCH_MS)))
}

fn request(source: AddSource) -> AddRequest {
    AddRequest {
        source,
        category: None,
        policy_override: None,
        paused: false,
        forced: false,
        unpack: None,
    }
}

// A queue whose pipeline holds no live jobs and keeps the finished jobs
// handed to it, newest first.
fn queue() -> SchedulerHandle {
    use crate::jobs::handle::SchedulerCommand;

    let state = crate::jobs::SharedPipelineState::new(crate::PipelineMetrics::new(), vec![]);
    let (cmd_tx, mut cmd_rx) = tokio::sync::mpsc::channel(16);
    let (event_tx, _) = tokio::sync::broadcast::channel(16);
    let handle = SchedulerHandle::new(cmd_tx, event_tx, state.clone());
    tokio::spawn(async move {
        let mut finished: Vec<JobInfo> = Vec::new();
        while let Some(command) = cmd_rx.recv().await {
            if let SchedulerCommand::RetireJob { info } = command {
                finished.insert(0, *info);
                state.publish_live_jobs(Vec::new(), Arc::from(finished.clone()));
            }
        }
    });
    handle
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_metainfo_becomes_a_job_and_its_duplicate_is_refused() {
    let rig = rig(set_clock());
    rig.service.open().await.unwrap();
    let bytes = plain("set", 1);
    let job_id = rig
        .service
        .add(request(AddSource::Metainfo(bytes.clone())))
        .await
        .unwrap();
    let stored = rig.db.get_torrent(job_id).unwrap().unwrap();
    assert_eq!(stored.name, "set");
    assert_eq!(stored.source, TorrentSource::Metainfo);
    assert_eq!(
        stored.storage_dir.as_deref(),
        Some(rig.dir.path().join("complete/set").to_str().unwrap())
    );
    let view = rig.service.torrent(job_id).await.unwrap();
    assert_eq!(view.total_bytes, 3000);
    assert_eq!(view.files.len(), 1);
    assert_eq!(view.files[0].path, "set/a.bin");
    assert!(view.route.is_some());
    assert_eq!(
        rig.service.add(request(AddSource::Metainfo(bytes))).await,
        Err(AddError::Duplicate(job_id))
    );
    let infos = rig.service.job_infos().await;
    assert_eq!(infos.len(), 1);
    assert_eq!(infos[0].protocol, Protocol::Torrent);
}

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
        .add(request(AddSource::Metainfo(plain("set", 1))))
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
    state.publish_live_jobs(vec![earlier.clone(), later.clone()], Arc::from(Vec::new()));
    let order: Vec<JobId> = state.list_jobs().iter().map(|job| job.job_id).collect();
    assert_eq!(order, [earlier.job_id, job_id, later.job_id]);
    assert_eq!(state.get_job(job_id).unwrap().protocol, Protocol::Torrent);

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
    // The stub pipeline publishes no live jobs of its own, so the finished
    // torrent is all the queue holds.
    assert_eq!(finished.1, 1);
}

// A torrent that cannot go on reaches the finished jobs as failed, once.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_failed_torrent_reaches_the_finished_jobs() {
    let rig = rig(set_clock());
    let queue = queue();
    let state = queue.state().clone();
    rig.service.publish_to(queue);
    rig.service.open().await.unwrap();
    let job_id = rig
        .service
        .add(request(AddSource::Metainfo(plain("set", 1))))
        .await
        .unwrap();
    let mut changes = state.subscribe_job_changes();
    changes.borrow_and_update();
    rig.service.fail(job_id, "no room".into()).await;
    loop {
        let jobs = state.list_jobs();
        if jobs.iter().any(|job| {
            job.job_id == job_id
                && job.status
                    == JobStatus::Failed {
                        error: "no room".into(),
                    }
        }) {
            assert_eq!(jobs.len(), 1);
            break;
        }
        changes.changed().await.unwrap();
    }
    assert!(rig.service.torrent(job_id).await.is_none());
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
        .add(request(AddSource::Metainfo(plain("set", 1))))
        .await
        .unwrap();
    let record = |rig: &Rig| rig.db.get_torrent(job_id).unwrap().unwrap();

    queue.pause_job(job_id).await.unwrap();
    assert_eq!(record(&rig).intent, TorrentIntent::Paused);
    queue.resume_job(job_id).await.unwrap();
    assert_eq!(record(&rig).intent, TorrentIntent::Resumed);

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

    queue
        .reorder_job(job_id, QueueMoveTarget::Top)
        .await
        .unwrap();
    assert!(record(&rig).forced);
    queue
        .reorder_jobs(vec![(job_id, QueueMoveTarget::Bottom)])
        .await
        .unwrap();
    assert!(!record(&rig).forced);
    assert!(matches!(
        queue.reorder_job(job_id, QueueMoveTarget::Offset(1)).await,
        Err(crate::SchedulerError::InvalidInput(_))
    ));

    queue.cancel_job_keeping_files(job_id).await.unwrap();
    assert_eq!(rig.db.get_torrent(job_id).unwrap(), None);

    // A delete with data goes the same way.
    let other = rig
        .service
        .add(request(AddSource::Metainfo(plain("other", 2))))
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
        source: TorrentSource::Metainfo,
        state: TorrentState::Downloading,
        detail: None,
        category: None,
        private: false,
        forced: false,
        progress: 0.25,
        total_bytes: 4000,
        have_bytes: 1000,
        uploaded_bytes: 0,
        downloaded_bytes: 1000,
        ratio: 0.0,
        download_bps: 500,
        upload_bps: 0,
        eta_seconds: Some(6),
        peers: 1,
        seeds: 1,
        leechers: 0,
        max_peers: 100,
        swarm_seeds: None,
        swarm_leechers: None,
        reannounce_seconds: None,
        active_seconds: 0,
        download_avg_bps: 0,
        upload_avg_bps: 0,
        wasted_bytes: 0,
        last_seen_complete_ms: None,
        pieces: None,
        created_by: None,
        created_at_ms: None,
        comment: None,
        added_at_ms: EPOCH_MS,
        completed_at_ms: None,
        storage_dir: None,
        seeding_left: None,
        route: None,
        files: Vec::new(),
        trackers: Vec::new(),
        file_have: Vec::new(),
        seeding_override: None,
        seeding_inherited: None,
        seeding_policy: SeedingPolicy::default(),
        seeding_source: SeedingSource::Global,
        unpack: true,
    };
    let job_info =
        |view: TorrentView, now: i64| report(view.clone(), job_status(&view), now).job_info();
    let info = job_info(view.clone(), EPOCH_MS + 1000);
    assert_eq!(info.status, JobStatus::Downloading);
    assert_eq!(
        crate::jobs::handle::job_download_rates(&[info.clone()]),
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
async fn admission_refuses_unwanted_extensions_and_executables() {
    let rig = rig(set_clock());
    rig.service.open().await.unwrap();
    rig.db
        .save_post_processing_settings(&PostProcessingSettings {
            unacceptable_extensions: vec!["nfo".into()],
            ..PostProcessingSettings::default()
        })
        .unwrap();
    let unwanted = metainfo(
        "set",
        &[("set/a.bin", &[1; 100]), ("set/b.nfo", &[2; 10])],
        64,
        false,
    );
    assert_eq!(
        rig.service
            .add(request(AddSource::Metainfo(unwanted)))
            .await,
        Err(AddError::Refused(Refusal::Unwanted {
            path: "set/b.nfo".into(),
            pattern: "nfo".into()
        }))
    );
    let executable = metainfo("setup.exe", &[("setup.exe", &[1; 100])], 64, false);
    assert_eq!(
        rig.service
            .add(request(AddSource::Metainfo(executable)))
            .await,
        Err(AddError::Refused(Refusal::Executable("setup.exe".into())))
    );
    assert!(rig.db.list_torrents().unwrap().is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_magnet_is_refused_without_a_network_to_fetch_it_on() {
    let rig = rig(set_clock());
    rig.service.open().await.unwrap();
    let hash = "0123456789abcdef0123456789abcdef01234567";
    rig.routes.set_down(Some("tunnel gone"));
    let magnet = format!("magnet:?xt=urn:btih:{hash}&tr=http%3A%2F%2F127.0.0.1%3A9%2Fannounce");
    assert!(matches!(
        rig.service.add(request(AddSource::Magnet(magnet))).await,
        Err(AddError::NoNetwork(_))
    ));
    // A base32 hash names the same torrent as its hex form.
    let base32 = "AERUKZ4JVPG66AJDIVTYTK6N54ASGRLH";
    let magnet = format!("magnet:?xt=urn:btih:{base32}&tr=http%3A%2F%2F127.0.0.1%3A9%2Fannounce");
    assert!(matches!(
        rig.service.add(request(AddSource::Magnet(magnet))).await,
        Err(AddError::NoNetwork(_))
    ));
    // A route with no DHT cannot find peers for a magnet with no trackers.
    rig.routes.set_down(None);
    let bare = format!("magnet:?xt=urn:btih:{hash}");
    assert!(matches!(
        rig.service.add(request(AddSource::Magnet(bare))).await,
        Err(AddError::NoNetwork(_))
    ));
    assert!(rig.db.list_torrents().unwrap().is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_private_torrent_with_an_unmet_goal_stays_unless_the_removal_confirms() {
    let rig = rig(set_clock());
    rig.service.open().await.unwrap();
    let private = metainfo("p", &[("p/a.bin", &[7; 2000])], 1024, true);
    let job_id = rig
        .service
        .add(AddRequest {
            policy_override: Some(SeedingPolicy {
                ratio_goal: Some(1.0),
                seed_time_goal_minutes: None,
                inactive_goal_minutes: None,
                mode: SeedingGoalMode::Any,
                action: SeedingAction::Stop,
                max_upload_bps: None,
            }),
            ..request(AddSource::Metainfo(private))
        })
        .await
        .unwrap();
    assert_eq!(
        rig.service.remove(job_id, false, false).await,
        Err(ActionError::UnmetGoal("ratio 0.00 of 1.00".into()))
    );
    rig.service.remove(job_id, false, true).await.unwrap();
    assert_eq!(rig.db.get_torrent(job_id).unwrap(), None);
    let history = rig.db.get_job_history(job_id.0).unwrap().unwrap();
    assert_eq!(history.status, "cancelled");
    assert_eq!(history.protocol, Protocol::Torrent);

    // A public torrent with no goal leaves when asked.
    let public = rig
        .service
        .add(request(AddSource::Metainfo(plain("q", 3))))
        .await
        .unwrap();
    rig.service.remove(public, true, false).await.unwrap();
    assert_eq!(
        rig.service.remove(public, false, false).await,
        Err(ActionError::NotFound(public))
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_route_that_goes_down_holds_its_torrents_and_a_category_move_changes_session() {
    let rig = rig(set_clock());
    rig.service.open().await.unwrap();
    let job_id = rig
        .service
        .add(request(AddSource::Metainfo(plain("set", 1))))
        .await
        .unwrap();
    let first = rig.service.torrent(job_id).await.unwrap().route.unwrap();

    rig.routes.set_down(Some("tunnel gone"));
    rig.service.tick().await;
    let view = rig.service.torrent(job_id).await.unwrap();
    assert_eq!(view.state, TorrentState::Stalled);
    assert_eq!(
        view.detail.as_deref(),
        Some(format!("{ROUTE_DOWN_FETCHING} (tunnel gone)").as_str())
    );
    let held = rig.service.inner.sessions.views().await;
    assert_eq!(held.len(), 1);
    assert_eq!(held[0].held.as_deref(), Some("tunnel gone"));

    rig.routes.set_down(None);
    rig.service.tick().await;
    let view = rig.service.torrent(job_id).await.unwrap();
    assert_ne!(
        view.detail
            .as_deref()
            .map(|detail| detail.starts_with(ROUTE_DOWN_FETCHING)),
        Some(true)
    );
    assert_eq!(view.route.as_deref(), Some(first.as_str()));
    assert!(
        rig.service.inner.live.lock().await[&job_id]
            .attached
            .is_some()
    );

    // The category routes through its own proxy: the torrent moves there,
    // and the session it left closes once nothing uses it.
    rig.routes.route(TorrentScope::Category(1), 9);
    rig.service
        .set_category(job_id, Some("tv".into()))
        .await
        .unwrap();
    let moved = rig.service.torrent(job_id).await.unwrap();
    assert_eq!(moved.category.as_deref(), Some("tv"));
    assert_ne!(moved.route.as_deref(), Some(first.as_str()));
    rig.service.tick().await;
    let keys: Vec<String> = rig
        .service
        .inner
        .sessions
        .views()
        .await
        .into_iter()
        .map(|view| view.key.as_str().to_owned())
        .collect();
    assert_eq!(keys, [moved.route.unwrap()]);
    assert_eq!(
        rig.db.get_torrent(job_id).unwrap().unwrap().category_id,
        Some(1)
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_torrent_past_the_session_limit_says_so() {
    use crate::torrents::sessions::MAX_SESSIONS;
    use crate::torrents::status::SESSION_LIMIT_FETCHING;

    let rig = rig(set_clock());
    rig.service.open().await.unwrap();
    for id in 0..MAX_SESSIONS as u32 {
        rig.routes.route(TorrentScope::Category(100 + id), 100 + id);
        let plan = rig.routes.plan(TorrentScope::Category(100 + id)).unwrap();
        rig.service.inner.sessions.session(&plan).await.unwrap();
    }
    let job_id = rig
        .service
        .add(request(AddSource::Metainfo(plain("set", 1))))
        .await
        .unwrap();
    let view = rig.service.torrent(job_id).await.unwrap();
    assert_eq!(view.state, TorrentState::Stalled);
    assert_eq!(view.detail.as_deref(), Some(SESSION_LIMIT_FETCHING));
    rig.service.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_session_that_cannot_be_built_says_why() {
    let rig = rig(set_clock());
    rig.service.open().await.unwrap();
    rig.routes.set_refused(Some("no listening port"));
    let job_id = rig
        .service
        .add(request(AddSource::Metainfo(plain("set", 1))))
        .await
        .unwrap();
    let view = rig.service.torrent(job_id).await.unwrap();
    assert_eq!(view.state, TorrentState::Stalled);
    assert_eq!(
        view.detail.as_deref(),
        Some("Not downloading: no listening port")
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pause_resume_and_force_are_kept() {
    let rig = rig(set_clock());
    rig.service.open().await.unwrap();
    let job_id = rig
        .service
        .add(request(AddSource::Metainfo(plain("set", 1))))
        .await
        .unwrap();
    rig.service.pause(job_id).await.unwrap();
    assert_eq!(
        rig.service.torrent(job_id).await.unwrap().state,
        TorrentState::Paused
    );
    assert_eq!(
        rig.db.get_torrent(job_id).unwrap().unwrap().intent,
        TorrentIntent::Paused
    );
    rig.service.tick().await;
    assert_eq!(
        rig.db.get_torrent(job_id).unwrap().unwrap().intent,
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

// The running tick writes the ledger, which is never left longer than its
// limit, on the runtime's own clock.
#[tokio::test(start_paused = true)]
async fn the_ledger_is_kept_on_the_tick() {
    let clock = Arc::new(RuntimeClock(tokio::time::Instant::now()));
    let rig = rig(Arc::clone(&clock) as Arc<dyn Clock>);
    // No session is built while the route is down.
    rig.routes.set_down(Some("down"));
    let mut written = rig.service.written();
    rig.service.start().await.unwrap();
    let job_id = rig
        .service
        .add(request(AddSource::Metainfo(plain("set", 1))))
        .await
        .unwrap();
    for _ in 0..8 {
        let ledgers = written.borrow_and_update().ledgers;
        written
            .wait_for(|written| written.ledgers > ledgers)
            .await
            .unwrap();
        let ledger = rig.db.get_torrent_counters(job_id).unwrap().unwrap();
        assert!(clock.now_ms() - ledger.updated_at_ms < LEDGER_EVERY_MS);
    }
}

// A member with an extension the operator does not want, found inside an
// archive the torrent carries, fails the job into history. The torrent
// stays, stopped with its data, and may be resumed to seed.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_unwanted_member_found_while_unpacking_fails_the_job_and_keeps_the_torrent() {
    let archive = include_bytes!(concat!(
        std::env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/rar5/rar5_store.rar"
    ));
    let rig = rig(set_clock());
    rig.service.open().await.unwrap();
    rig.db
        .save_post_processing_settings(&PostProcessingSettings {
            unacceptable_extensions: vec!["txt".into()],
            ..PostProcessingSettings::default()
        })
        .unwrap();
    let bytes = metainfo("set.rar", &[("set.rar", archive)], 128, false);
    let job_id = rig
        .service
        .add(request(AddSource::Metainfo(bytes)))
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
    let mut retired = rig.service.retired();
    let storage = PieceStorage::new(Arc::clone(&layout), intake as Arc<dyn Pieces>);
    let lengths = Lengths::new(
        layout.files().map(|file| file.length).sum(),
        layout.piece_length() as u32,
    )
    .unwrap();
    for piece in 0..layout.piece_count() {
        let range = layout.piece_range(piece);
        for span in layout.spans(piece) {
            let at = (range.start + span.at) as usize;
            storage
                .pwrite_all(
                    span.file,
                    span.range.start,
                    &archive[at..at + span.range.len() as usize],
                )
                .unwrap();
        }
        // A piece past the refusal is refused too.
        if let Err(error) =
            storage.on_piece_completed(lengths.try_validate_piece_index(piece).unwrap())
        {
            assert!(format!("{error:#}").contains("txt"), "{error:#}");
        }
    }
    retired.wait_for(|retired| *retired >= 1).await.unwrap();
    let history = rig.db.get_job_history(job_id.0).unwrap().unwrap();
    assert_eq!(history.status, "failed");
    assert!(history.error_message.unwrap().contains("txt"));
    let record = rig.db.get_torrent(job_id).unwrap().unwrap();
    assert_eq!(record.intent, TorrentIntent::Stopped);
    let view = rig.service.torrent(job_id).await.unwrap();
    assert_eq!(view.state, TorrentState::Stopped);
    assert!(view.detail.unwrap().contains("txt"));

    // The job's outcome is kept once: resumed, the torrent seeds on.
    rig.service.resume(job_id).await.unwrap();
    rig.service.inner.signal.mark(job_id);
    rig.service.settle().await;
    assert_eq!(
        rig.db.get_torrent(job_id).unwrap().unwrap().intent,
        TorrentIntent::Resumed
    );
    assert_eq!(*retired.borrow(), 1);
}

// The metainfo's own word on who made it, when, and why stays with the
// torrent; a torrent without it says nothing.
#[test]
fn a_metainfo_says_who_made_it_and_when() {
    let bare = plain("set", 1);
    let mut said = b"d".to_vec();
    bstr(&mut said, b"comment");
    bstr(&mut said, b"  a note ");
    bstr(&mut said, b"created by");
    bstr(&mut said, b"maker 1.0");
    bstr(&mut said, b"creation date");
    bint(&mut said, 1_600_000_000);
    said.extend_from_slice(&bare[1..]);
    let about = parse(&said).unwrap().about;
    assert_eq!(
        (about.created_by, about.created_at_ms, about.comment),
        (
            Some("maker 1.0".to_owned()),
            Some(1_600_000_000_000),
            Some("a note".to_owned()),
        )
    );
    let about = parse(&bare).unwrap().about;
    assert_eq!(
        (about.created_by, about.created_at_ms, about.comment),
        (None, None, None)
    );
}

#[test]
fn the_disk_warning_names_the_volume_and_the_waiting() {
    assert_eq!(
        disk_warning(Path::new("/data/complete"), &["a", "b"]),
        "Disk full on /data/complete: waiting a, b"
    );
}

// The metainfo with a tracker to announce to.
fn announcing(metainfo: &[u8], url: &str) -> Vec<u8> {
    let mut out = b"d".to_vec();
    bstr(&mut out, b"announce");
    bstr(&mut out, url.as_bytes());
    out.extend_from_slice(&metainfo[1..]);
    out
}

// A tracker on loopback that names `peer` to every announce.
async fn tracker(peer: std::net::SocketAddrV4) -> String {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/announce", listener.local_addr().unwrap());
    tokio::spawn(async move {
        while let Ok((mut stream, _)) = listener.accept().await {
            tokio::spawn(async move {
                let mut request = Vec::new();
                let mut buf = [0u8; 1024];
                while !request.windows(4).any(|end| end == b"\r\n\r\n") {
                    match stream.read(&mut buf).await {
                        Ok(0) | Err(_) => return,
                        Ok(read) => request.extend_from_slice(&buf[..read]),
                    }
                }
                let mut body = b"d8:intervali1800e5:peers6:".to_vec();
                body.extend_from_slice(&peer.ip().octets());
                body.extend_from_slice(&peer.port().to_be_bytes());
                body.push(b'e');
                let head = format!(
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    body.len()
                );
                let answer = [head.as_bytes(), &body].concat();
                if let Err(error) = stream.write_all(&answer).await {
                    tracing::debug!(error = %error, "a client hung up before its answer");
                }
            });
        }
    });
    url
}

// A session on loopback seeding `metainfo` from `files` under `dir`, with
// no DHT, trackers or discovery.
async fn seeder(dir: &std::path::Path, metainfo: &[u8], files: &[(&str, &[u8])]) -> Arc<Session> {
    use librqbit::{AddTorrent, AddTorrentOptions, ListenerMode, ListenerOptions, SessionOptions};

    for (path, bytes) in files {
        let path = dir.join(path);
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(path, bytes).unwrap();
    }
    let session = Session::new_with_opts(
        dir.join("state"),
        SessionOptions {
            dht: None,
            disable_trackers: true,
            disable_local_service_discovery: true,
            fastresume: false,
            persistence: None,
            listen: Some(ListenerOptions {
                mode: ListenerMode::TcpOnly,
                listen_addr: "127.0.0.1:0".parse().unwrap(),
                enable_upnp_port_forwarding: false,
                ..ListenerOptions::default()
            }),
            ..SessionOptions::default()
        },
    )
    .await
    .unwrap();
    let handle = session
        .add_torrent(
            AddTorrent::from_bytes(metainfo.to_vec()),
            Some(AddTorrentOptions {
                output_folder: Some(dir.to_str().unwrap().to_owned()),
                overwrite: true,
                ..AddTorrentOptions::default()
            }),
        )
        .await
        .unwrap()
        .into_handle()
        .unwrap();
    handle.wait_until_completed().await.unwrap();
    session
}

// A full disk holds what the torrent fetched and selects nothing more; once
// the disk has room the tick lets it down and the torrent completes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_full_disk_holds_the_torrent_until_there_is_room() {
    use crate::operations::disk::{Capacity, CapacityReading};
    use crate::torrents::net::{LegKind, LegSockets};
    use crate::torrents::status::DISK_FULL;

    let data: Vec<u8> = (0..65_536u32).map(|at| (at % 251) as u8).collect();
    let files = [("set/a.bin", data.as_slice())];
    let plain = metainfo("set", &files, 16_384, false);

    let seeds = tempfile::tempdir().unwrap();
    let seeding = seeder(seeds.path(), &plain, &files).await;
    let std::net::SocketAddr::V4(peer) = seeding.listen_addr().unwrap() else {
        panic!("the seeder listens on IPv4 loopback");
    };
    let bytes = announcing(&plain, &tracker(peer).await);

    let sampled_at = std::time::Instant::now();
    let reading = Arc::new(Mutex::new(CapacityReading {
        available_bytes: 0,
        total_bytes: 1 << 30,
        sampled_at,
        stale: false,
    }));
    let read = Arc::clone(&reading);
    let mut limits = ExtractionLimits::unbounded();
    limits.min_free_bytes = 100;
    let env = JobEnv {
        free_disk: CapacityReader::from_fn(move || Capacity::Known(*read.lock().unwrap())),
        limits: Arc::new(limits),
        ..JobEnv::unbounded()
    };
    let rig = rig_with(set_clock(), env);
    let loopback = Arc::new(weaver_tunnel::pipe::Revocable::new(Arc::new(
        weaver_tunnel::pipe::Egress::new(
            4,
            weaver_tunnel::egress::SocketEgress::SourceAddress("127.0.0.1".parse().unwrap()),
            // No connect deadline: the test waits on what it checks.
            std::time::Duration::MAX,
        ),
    )));
    rig.routes.set_leg(
        LegKind::Ladder,
        Arc::new(LegSockets::new(loopback, &LegKind::Ladder)),
    );
    let handle = queue();
    let queue = handle.state().clone();
    rig.service.publish_to(handle);
    let mut changes = queue.subscribe_job_changes();
    rig.service.open().await.unwrap();
    let job_id = rig
        .service
        .add(request(AddSource::Metainfo(bytes)))
        .await
        .unwrap();

    // The first piece finds no room and is held, and nothing more is
    // selected.
    loop {
        changes.borrow_and_update();
        {
            let live = rig.service.inner.live.lock().await;
            if live[&job_id].holding() && live[&job_id].applied == Some(Vec::new()) {
                break;
            }
        }
        changes.changed().await.unwrap();
    }
    rig.service.tick().await;
    let view = rig.service.torrent(job_id).await.unwrap();
    assert_eq!(view.state, TorrentState::Stalled);
    assert_eq!(view.detail.as_deref(), Some(DISK_FULL));
    assert_eq!(view.completed_at_ms, None);
    assert_eq!(
        rig.service.inner.live.lock().await[&job_id].applied,
        Some(Vec::new())
    );
    let warnings = rig.service.warnings().await;
    assert_eq!(warnings.len(), 1);
    assert!(warnings[0].contains(rig.dir.path().join("complete").to_str().unwrap()));
    assert!(warnings[0].contains("set"));
    assert_eq!(
        queue
            .get_job(job_id)
            .unwrap()
            .download_wait_reason
            .as_deref(),
        Some(DISK_FULL)
    );

    // Room again: the tick lets the held pieces down and the torrent goes on.
    {
        let mut now = reading.lock().unwrap();
        now.available_bytes = 1 << 30;
        now.sampled_at = sampled_at + std::time::Duration::from_secs(1);
    }
    rig.service.tick().await;
    assert!(rig.service.warnings().await.is_empty());
    loop {
        changes.borrow_and_update();
        if rig
            .service
            .torrent(job_id)
            .await
            .unwrap()
            .completed_at_ms
            .is_some()
        {
            break;
        }
        changes.changed().await.unwrap();
    }
    rig.service.tick().await;
    let view = rig.service.torrent(job_id).await.unwrap();
    assert_eq!(view.have_bytes, data.len() as u64);
    assert_ne!(view.detail.as_deref(), Some(DISK_FULL));
    rig.service.stop().await;
    seeding.stop().await;
}

// While the session checks the files, the bytes in hand are the checkpoint's;
// once it reports, they are the session's.
#[test]
fn a_checking_torrent_keeps_the_checkpoints_bytes() {
    let mut have = vec![1000, 2500];
    take_file_have(&mut have, &Engine::Checking, &[0, 0]);
    assert_eq!(have, [1000, 2500]);
    let live = Engine::Live {
        finished: false,
        peers: 0,
    };
    take_file_have(&mut have, &live, &[]);
    assert_eq!(have, [1000, 2500]);
    take_file_have(&mut have, &live, &[1024, 2500]);
    assert_eq!(have, [1024, 2500]);
}

// Turning torrents off closes their sessions and stops each torrent, and
// refuses new ones; turning them on starts the tick, which takes them up.
#[tokio::test(start_paused = true)]
async fn torrents_turn_off_and_on_while_running() {
    let clock = Arc::new(RuntimeClock(tokio::time::Instant::now()));
    let rig = rig(Arc::clone(&clock) as Arc<dyn Clock>);
    rig.service.open().await.unwrap();
    let job_id = rig
        .service
        .add(request(AddSource::Metainfo(plain("set", 1))))
        .await
        .unwrap();
    assert!(rig.service.torrent(job_id).await.unwrap().route.is_some());

    let mut settings = rig.service.settings();
    settings.torrent.enabled = false;
    rig.service.set_settings(settings.clone()).await;
    let view = rig.service.torrent(job_id).await.unwrap();
    assert_eq!(view.state, TorrentState::Stopped);
    assert_eq!(view.detail.as_deref(), Some(status::OFF));
    assert!(rig.service.inner.sessions.views().await.is_empty());
    assert_eq!(
        rig.service
            .add(request(AddSource::Metainfo(plain("other", 2))))
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
    assert_ne!(view.detail.as_deref(), Some(status::OFF));
    assert!(view.route.is_some());
    rig.service.stop().await;
}

// A run the session refuses leaves the operator's intent as it was, and the
// state shows the session's error, never a met goal.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_refused_unpause_keeps_the_intent_and_shows_the_error() {
    let rig = rig(set_clock());
    rig.service.open().await.unwrap();
    let job_id = rig
        .service
        .add(request(AddSource::Metainfo(plain("set", 1))))
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
        rig.db.get_torrent(job_id).unwrap().unwrap().intent,
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

// A restart brings back what the operator asked, not the state last shown.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_restart_restores_the_intent_not_the_state() {
    let rig = rig(set_clock());
    rig.service.open().await.unwrap();
    let job_id = rig
        .service
        .add(request(AddSource::Metainfo(plain("set", 1))))
        .await
        .unwrap();
    let mut settings = rig.service.settings();
    settings.torrent.enabled = false;
    rig.service.set_settings(settings).await;
    rig.service.tick().await;
    assert_eq!(
        rig.service.torrent(job_id).await.unwrap().state,
        TorrentState::Stopped
    );
    rig.service.stop().await;

    let again = service_on(&rig.db, rig.dir.path(), &rig.routes, job_env(), set_clock());
    again.open().await.unwrap();
    assert_eq!(
        rig.db.get_torrent(job_id).unwrap().unwrap().intent,
        TorrentIntent::Resumed
    );
    let view = again.torrent(job_id).await.unwrap();
    assert_ne!(view.state, TorrentState::Stopped);
    assert_ne!(view.state, TorrentState::Paused);
    again.stop().await;
}

// A forced torrent seeds past its goal; unforced, the goal stops it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_forced_torrent_seeds_past_its_goal() {
    let rig = rig(set_clock());
    rig.service.open().await.unwrap();
    let job_id = rig
        .service
        .add(AddRequest {
            policy_override: Some(SeedingPolicy {
                ratio_goal: Some(0.0),
                seed_time_goal_minutes: None,
                inactive_goal_minutes: None,
                mode: SeedingGoalMode::Any,
                action: SeedingAction::Stop,
                max_upload_bps: None,
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
    let intent = |rig: &Rig| rig.db.get_torrent(job_id).unwrap().unwrap().intent;
    assert_eq!(intent(&rig), TorrentIntent::Resumed);

    rig.service.set_forced(job_id, false).await.unwrap();
    rig.service.apply_goals(EPOCH_MS).await;
    assert_eq!(intent(&rig), TorrentIntent::Stopped);
    let view = rig.service.torrent(job_id).await.unwrap();
    assert_eq!(
        (view.state, view.detail.as_deref()),
        (TorrentState::Stopped, Some(status::GOAL_MET))
    );
}

// Two torrents with one name each get a directory of their own, made when
// they are added.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn torrents_with_one_name_get_their_own_directories() {
    let rig = rig(set_clock());
    rig.service.open().await.unwrap();
    let mut dirs = Vec::new();
    for seed in [1, 2] {
        let job_id = rig
            .service
            .add(request(AddSource::Metainfo(plain("set", seed))))
            .await
            .unwrap();
        let dir = rig
            .db
            .get_torrent(job_id)
            .unwrap()
            .unwrap()
            .storage_dir
            .unwrap();
        assert!(Path::new(&dir).is_dir());
        dirs.push(dir);
    }
    assert_eq!(
        dirs[0],
        rig.dir.path().join("complete/set").to_str().unwrap()
    );
    assert_ne!(dirs[0], dirs[1]);
}

// However many pieces a job verifies between ticks, its checkpoint is
// written once.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_checkpoint_is_written_once_a_tick() {
    let rig = rig(set_clock());
    // No session is built while the route is down.
    rig.routes.set_down(Some("down"));
    rig.service.open().await.unwrap();
    let job_id = rig
        .service
        .add(request(AddSource::Metainfo(plain("set", 1))))
        .await
        .unwrap();
    let written = rig.service.written();
    rig.service.tick().await;
    let base = written.borrow().checkpoints;
    for _ in 0..3 {
        rig.service.inner.signal.mark(job_id);
    }
    rig.service.tick().await;
    assert_eq!(written.borrow().checkpoints, base + 1);
    rig.service.tick().await;
    assert_eq!(written.borrow().checkpoints, base + 1);
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
        .add(request(AddSource::Metainfo(plain("downloading", 1))))
        .await
        .unwrap();
    let seeding = rig
        .service
        .add(request(AddSource::Metainfo(plain("seeding", 2))))
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
        .add(request(AddSource::Metainfo(plain("kept", 3))))
        .await
        .unwrap();
    rig.service.pause(kept).await.unwrap();

    let mut published = queue.state().subscribe_job_changes();
    let paused = |job_id| {
        let service = rig.service.clone();
        async move { service.torrent(job_id).await.unwrap().state == TorrentState::Paused }
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
        (vec![Hold::manual(HoldOwner::Torrents)], (true, true)),
        (
            vec![scheduled(HoldScope::Everything, HoldOwner::Torrents)],
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
        while (paused(downloading).await, paused(seeding).await) != stopped {
            published.changed().await.unwrap();
        }
        assert!(paused(kept).await);
    }
    for (job_id, intent) in [
        (downloading, TorrentIntent::Resumed),
        (seeding, TorrentIntent::Resumed),
        (kept, TorrentIntent::Paused),
    ] {
        assert_eq!(rig.db.get_torrent(job_id).unwrap().unwrap().intent, intent);
    }
}

// The rig's torrents dial out on loopback.
fn on_loopback(routes: &FakeRoutes) {
    use crate::torrents::net::{LegKind, LegSockets};

    let loopback = Arc::new(weaver_tunnel::pipe::Revocable::new(Arc::new(
        weaver_tunnel::pipe::Egress::new(
            4,
            weaver_tunnel::egress::SocketEgress::SourceAddress("127.0.0.1".parse().unwrap()),
            // No connect deadline: the test waits on what it checks.
            std::time::Duration::MAX,
        ),
    )));
    routes.set_leg(
        LegKind::Ladder,
        Arc::new(LegSockets::new(loopback, &LegKind::Ladder)),
    );
}

// The torrent's place in its session.
async fn handle_of(service: &TorrentService, job_id: JobId) -> Arc<ManagedTorrent> {
    let live = service.inner.live.lock().await;
    Arc::clone(&live[&job_id].attached.as_ref().unwrap().handle)
}

// The torrent's view once it seeds, read as the queue publishes it.
async fn seeding_view(
    service: &TorrentService,
    changes: &mut watch::Receiver<u64>,
    job_id: JobId,
) -> TorrentView {
    loop {
        changes.borrow_and_update();
        let view = service.torrent(job_id).await.unwrap();
        if view.state == TorrentState::Seeding {
            return view;
        }
        changes.changed().await.unwrap();
    }
}

// The bytes the seeder has sent.
fn uploaded(seeder: &Session) -> u64 {
    seeder
        .get(TorrentIdOrHash::Id(0))
        .unwrap()
        .stats()
        .uploaded_bytes
}

// A torrent shows what it has the moment it seeds, and after a restart it
// seeds from what it kept: nothing is fetched again and its counters stand.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_complete_torrent_seeds_after_a_restart_without_fetching_again() {
    let data: Vec<u8> = (0..65_536u32).map(|at| (at % 251) as u8).collect();
    let files = [("set/a.bin", data.as_slice())];
    let private = metainfo("set", &files, 16_384, true);

    let seeds = tempfile::tempdir().unwrap();
    let seeding = seeder(seeds.path(), &private, &files).await;
    let std::net::SocketAddr::V4(peer) = seeding.listen_addr().unwrap() else {
        panic!("the seeder listens on IPv4 loopback");
    };
    let bytes = announcing(&private, &tracker(peer).await);

    let clock = set_clock();
    let rig = rig(Arc::clone(&clock) as Arc<dyn Clock>);
    on_loopback(&rig.routes);
    let handle = queue();
    let mut changes = handle.state().subscribe_job_changes();
    rig.service.publish_to(handle);
    rig.service.open().await.unwrap();
    let job_id = rig
        .service
        .add(request(AddSource::Metainfo(bytes)))
        .await
        .unwrap();
    let total = data.len() as u64;
    handle_of(&rig.service, job_id)
        .await
        .wait_until_completed()
        .await
        .unwrap();
    let view = seeding_view(&rig.service, &mut changes, job_id).await;
    assert_eq!(
        (view.have_bytes, view.downloaded_bytes, view.progress),
        (total, total, 1.0)
    );
    let sent = uploaded(&seeding);
    assert!(sent >= total, "the seeder sent {sent} of {total}");
    // Every piece verified, so nothing fetched was wasted.
    assert_eq!(view.wasted_bytes, 0);
    assert_eq!(
        view.pieces,
        Some(TorrentPieces {
            count: 4,
            size: 16_384,
            have: 4,
        })
    );
    // The ledger as the running tick keeps it: the time between two ticks
    // the torrent ran in its session is active, and it ran complete.
    rig.service.tick().await;
    clock.0.fetch_add(30_000, Ordering::SeqCst);
    rig.service.tick().await;
    let ledger = rig.db.get_torrent_counters(job_id).unwrap().unwrap();
    assert_eq!(
        (
            ledger.downloaded_bytes,
            ledger.checked_bytes,
            ledger.active_duration_s,
            ledger.last_seen_complete_ms,
        ),
        (total, total, 30, Some(EPOCH_MS + 30_000))
    );
    rig.service.stop().await;
    // What the server's recovery clears before anything starts.
    rig.db.prune_orphan_active_state().unwrap();

    let again = service_on(&rig.db, rig.dir.path(), &rig.routes, job_env(), set_clock());
    let handle = queue();
    let mut changes = handle.state().subscribe_job_changes();
    again.publish_to(handle);
    again.open().await.unwrap();
    handle_of(&again, job_id)
        .await
        .wait_until_completed()
        .await
        .unwrap();
    let view = seeding_view(&again, &mut changes, job_id).await;
    assert_eq!(
        (view.have_bytes, view.downloaded_bytes, view.progress),
        (total, total, 1.0)
    );
    assert_eq!(view.file_have, [total]);
    assert_eq!((view.active_seconds, view.wasted_bytes), (30, 0));
    assert_eq!(uploaded(&seeding), sent);
    again.stop().await;
    seeding.stop().await;
}

// The next announce the tracker on `listener` hears, unanswered.
async fn heard_announce(listener: &tokio::net::TcpListener) -> tokio::net::TcpStream {
    use tokio::io::AsyncReadExt;

    let (mut stream, _) = listener.accept().await.unwrap();
    let mut request = Vec::new();
    let mut buf = [0u8; 1024];
    while !request.windows(4).any(|end| end == b"\r\n\r\n") {
        let read = stream.read(&mut buf).await.unwrap();
        assert!(read > 0, "the announce ended before its request was read");
        request.extend_from_slice(&buf[..read]);
    }
    stream
}

// The torrent's trackers once its session's announce table says `done`.
async fn trackers_once(
    service: &TorrentService,
    job_id: JobId,
    done: impl Fn(&[TrackerAnnounce]) -> bool,
) -> Vec<TorrentTracker> {
    let table = handle_of(service, job_id).await.tracker_status().unwrap();
    table
        .subscribe()
        .wait_for(|announces| done(announces))
        .await
        .unwrap();
    service.torrent(job_id).await.unwrap().trackers
}

// A tracker reads as the torrent's session last heard it: updating while an
// announce waits, working with its counts once it answers, and not working
// once it stops answering.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_trackers_status_follows_its_announces() {
    use tokio::io::AsyncWriteExt;

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/announce", listener.local_addr().unwrap());
    let rig = rig(set_clock());
    on_loopback(&rig.routes);
    rig.service.open().await.unwrap();
    let job_id = rig
        .service
        .add(request(AddSource::Metainfo(announcing(
            &plain("set", 1),
            &url,
        ))))
        .await
        .unwrap();

    let mut started = heard_announce(&listener).await;
    let trackers = rig.service.torrent(job_id).await.unwrap().trackers;
    assert_eq!(
        trackers
            .iter()
            .map(|tracker| (tracker.tier, tracker.url.as_str(), tracker.status))
            .collect::<Vec<_>>(),
        [(0, url.as_str(), TrackerStatus::Updating)]
    );

    // The tracker stops once it has answered.
    drop(listener);
    let body = b"d8:completei3e10:incompletei5e8:intervali1800e5:peers0:e";
    let head = format!(
        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    started
        .write_all(&[head.as_bytes(), body].concat())
        .await
        .unwrap();
    let trackers = trackers_once(&rig.service, job_id, |announces| {
        announces[0].last.is_some()
    })
    .await;
    let working = &trackers[0];
    assert_eq!(working.status, TrackerStatus::Working);
    assert_eq!((working.seeders, working.leechers), (Some(3), Some(5)));
    assert_eq!(working.message, None);
    let at = working.last_announce_ms.unwrap();
    assert_eq!(working.next_announce_ms, Some(at + 1_800_000));

    // Paused, the answer stands with nothing in flight.
    rig.service.pause(job_id).await.unwrap();
    assert_eq!(
        rig.service.torrent(job_id).await.unwrap().trackers,
        trackers
    );

    // The next run's first announce finds no tracker.
    rig.service.resume(job_id).await.unwrap();
    let trackers = trackers_once(&rig.service, job_id, |announces| {
        announces[0].last.is_some() && !announces[0].in_flight
    })
    .await;
    let failed = &trackers[0];
    assert_eq!(failed.status, TrackerStatus::NotWorking);
    assert!(failed.message.is_some());
    assert_eq!((failed.seeders, failed.leechers), (None, None));
    assert!(failed.last_announce_ms.is_some());
    rig.service.stop().await;
}

// A follower hears a tracker's answer as it lands, with nothing in the queue
// to say so: the rig has no queue, and the beat is far past the test.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_followed_torrent_pushes_its_trackers_answer() {
    use tokio::io::AsyncWriteExt;

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/announce", listener.local_addr().unwrap());
    let rig = rig(set_clock());
    on_loopback(&rig.routes);
    rig.service.open().await.unwrap();
    let job_id = rig
        .service
        .add(request(AddSource::Metainfo(announcing(
            &plain("pushed", 2),
            &url,
        ))))
        .await
        .unwrap();
    let mut updates = rig
        .service
        .updates(Some(job_id), Duration::from_secs(86_400));

    let mut started = heard_announce(&listener).await;
    let first = updates.next().await;
    assert_eq!(first[0].trackers[0].status, TrackerStatus::Updating);

    let body = b"d8:completei3e10:incompletei5e8:intervali1800e5:peers0:e";
    let head = format!(
        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    started
        .write_all(&[head.as_bytes(), body].concat())
        .await
        .unwrap();
    let working = loop {
        let views = updates.next().await;
        if views[0].trackers[0].status == TrackerStatus::Working {
            break views[0].trackers[0].clone();
        }
    };
    assert_eq!((working.seeders, working.leechers), (Some(3), Some(5)));
    rig.service.stop().await;
}

// Who holds `job_id` stopped, as the queue last applied the holds.
async fn paused_by(service: &TorrentService, job_id: JobId) -> Option<HoldOrigin> {
    service.inner.live.lock().await[&job_id].paused_by
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
                id: 0,
                host_pattern: "*.test".into(),
                seeding: Some(SeedingPolicy {
                    ratio_goal: Some(2.0),
                    ..SeedingPolicy::default()
                }),
                never_pause: true,
                unpack: None,
            },
            EPOCH_MS,
        )
        .unwrap();
    rig.service.reload_tracker_rules().await.unwrap();
    let kept = rig
        .service
        .add(request(AddSource::Metainfo(announcing(
            &plain("kept", 1),
            "http://tracker.test/announce",
        ))))
        .await
        .unwrap();
    let plain = rig
        .service
        .add(request(AddSource::Metainfo(plain("plain", 2))))
        .await
        .unwrap();
    assert_eq!(
        rig.db.get_torrent(kept).unwrap().unwrap().tracker_rule_id,
        Some(rule.id)
    );
    assert_eq!(
        rig.db.get_torrent(plain).unwrap().unwrap().tracker_rule_id,
        None
    );
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
        rig.service.torrent(kept).await.unwrap().seeding_source,
        SeedingSource::TrackerRule("*.test".into())
    );

    let hold = |hold: Hold| {
        let mut set = HoldSet::default();
        set.hold(hold);
        queue.state().restore_holds(set);
    };
    let schedule = Hold {
        origin: HoldOrigin::Scheduled,
        scope: HoldScope::Everything,
        owner: HoldOwner::Torrents,
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

    for owner in [HoldOwner::Torrents, HoldOwner::All] {
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

    hold(Hold::manual(HoldOwner::Torrents));
    wait(kept, Some(HoldOrigin::Manual)).await;
    assert_eq!(
        paused_by(&rig.service, plain).await,
        Some(HoldOrigin::Manual)
    );
    assert!(rig.service.schedule_seeding().await.is_empty());
    queue.state().restore_holds(HoldSet::default());
    wait(kept, None).await;

    // Its ratio goal met, it pauses with the rest.
    rig.service
        .inner
        .live
        .lock()
        .await
        .get_mut(&kept)
        .unwrap()
        .counters
        .uploaded_bytes = 6000;
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
    assert_eq!(
        rig.db.get_torrent(kept).unwrap().unwrap().tracker_rule_id,
        None
    );
    assert_eq!(
        rig.service.torrent(kept).await.unwrap().seeding_source,
        SeedingSource::Global
    );
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
                .add(request(AddSource::Metainfo(plain(
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

#[test]
fn unpack_comes_from_the_torrent_then_its_rule_then_its_category_then_the_default() {
    let mut record = TorrentRecord {
        job_id: JobId(1),
        infohash_v1: Some([1; 20]),
        infohash_v2: None,
        name: "set".into(),
        source: TorrentSource::Metainfo,
        metainfo: None,
        private: false,
        category_id: Some(1),
        added_at_ms: EPOCH_MS,
        intent: TorrentIntent::Resumed,
        forced: false,
        completed_at_ms: None,
        storage_dir: None,
        policy_override: None,
        tracker_rule_id: Some(1),
        unpack: None,
    };
    let mut rules = vec![TrackerRule {
        id: 1,
        host_pattern: "tracker.test".into(),
        seeding: None,
        never_pause: false,
        unpack: None,
    }];
    let mut settings = TorrentSettings {
        complete_dir: PathBuf::from("/complete"),
        categories: vec![CategoryConfig {
            id: 1,
            name: "tv".into(),
            dest_dir: None,
            aliases: String::new(),
            seeding: None,
            torrent_route: false,
            unpack: None,
        }],
        seeding: None,
        torrent: TorrentConfig::default(),
    };

    assert!(settings.unpack(&record, &rules));
    settings.torrent.unpack = false;
    assert!(!settings.unpack(&record, &rules));
    settings.categories[0].unpack = Some(true);
    assert!(settings.unpack(&record, &rules));
    rules[0].unpack = Some(false);
    assert!(!settings.unpack(&record, &rules));
    record.unpack = Some(true);
    assert!(settings.unpack(&record, &rules));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_torrent_keeps_the_unpack_in_force_once_its_job_is_built() {
    let rig = rig(set_clock());
    rig.service.open().await.unwrap();
    let inherits = rig
        .service
        .add(request(AddSource::Metainfo(plain("inherits", 1))))
        .await
        .unwrap();
    let off = rig
        .service
        .add(AddRequest {
            unpack: Some(false),
            ..request(AddSource::Metainfo(plain("off", 2)))
        })
        .await
        .unwrap();

    let kept = |job_id| rig.db.get_torrent(job_id).unwrap().unwrap().unpack;
    assert_eq!(kept(inherits), Some(true));
    assert_eq!(kept(off), Some(false));
    assert!(rig.service.torrent(inherits).await.unwrap().unpack);
    assert!(!rig.service.torrent(off).await.unwrap().unpack);
}
