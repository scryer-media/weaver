// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use std::sync::atomic::{AtomicI64, Ordering};

use sha1::{Digest, Sha1};
use weaver_torrent::core::lengths::Lengths;
use weaver_torrent::storage::TorrentStorage;

use super::*;
use crate::jobs::{Hold, HoldOwner, HoldScope};
use crate::post_processing::model::PostProcessingSettings;
use crate::settings::{SeedingAction, SeedingGoalMode};
use crate::torrents::TrackerStatus;
use crate::torrents::sessions::Routes;
use crate::torrents::sessions::tests::{FakeRoutes, env, unbound_leg};
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

// A restarted service opened, its tick having put the torrents it restored
// in their routes' sessions.
async fn reopen(service: &TorrentService) {
    service.open().await.unwrap();
    service.tick().await;
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
        metadata: Vec::new(),
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
        infohash_v1: None,
        infohash_v2: None,
        version: None,
        magnet: String::new(),
        source: TorrentSource::Metainfo,
        state: TorrentState::Downloading,
        detail: None,
        paused_by: None,
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
        sequential: false,
        first_last_piece_first: false,
        trackers: Vec::new(),
        file_have: Vec::new(),
        metadata: Vec::new(),
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

const V2_ONLY: &[u8] = include_bytes!(concat!(
    std::env!("CARGO_MANIFEST_DIR"),
    "/../../../engines/weaver-torrent/resources/test/libtorrent/v2_only.torrent"
));
const V2_HYBRID: &[u8] = include_bytes!(concat!(
    std::env!("CARGO_MANIFEST_DIR"),
    "/../../../engines/weaver-torrent/resources/test/libtorrent/v2_hybrid.torrent"
));

// A v2 or hybrid torrent is one job, found by either of its hashes, whether
// it arrives as metainfo or as a magnet naming one hash.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_v2_torrent_is_added_and_found_by_either_hash() {
    let rig = rig(set_clock());
    rig.service.open().await.unwrap();

    let v2_only = rig
        .service
        .add(request(AddSource::Metainfo(V2_ONLY.to_vec())))
        .await
        .unwrap();
    let stored = rig.db.get_torrent(v2_only).unwrap().unwrap();
    assert_eq!(stored.infohash_v1, None);
    let v2 = Id32::new(stored.infohash_v2.unwrap()).as_string();
    let view = rig.service.torrent(v2_only).await.unwrap();
    assert_eq!(view.version, Some(TorrentVersion::V2));
    assert_eq!(view.infohash_v1, None);
    assert_eq!(view.infohash_v2.as_deref(), Some(v2.as_str()));
    assert_eq!(view.infohash, v2[..40]);
    assert_eq!(
        rig.service
            .add(request(AddSource::Metainfo(V2_ONLY.to_vec())))
            .await,
        Err(AddError::Duplicate(v2_only))
    );

    let hybrid = rig
        .service
        .add(request(AddSource::Metainfo(V2_HYBRID.to_vec())))
        .await
        .unwrap();
    let stored = rig.db.get_torrent(hybrid).unwrap().unwrap();
    let v1 = Id20::new(stored.infohash_v1.unwrap()).as_string();
    let v2 = Id32::new(stored.infohash_v2.unwrap()).as_string();
    let view = rig.service.torrent(hybrid).await.unwrap();
    assert_eq!(view.version, Some(TorrentVersion::Hybrid));
    assert_eq!(view.infohash, v1);
    assert!(view.magnet.contains(&format!("urn:btih:{v1}")));
    assert!(view.magnet.contains(&format!("urn:btmh:1220{v2}")));
    let tracker = "&tr=http%3A%2F%2F127.0.0.1%3A9%2Fannounce";
    for xt in [format!("urn:btih:{v1}"), format!("urn:btmh:1220{v2}")] {
        assert_eq!(
            rig.service
                .add(request(AddSource::Magnet(format!(
                    "magnet:?xt={xt}{tracker}"
                ))))
                .await,
            Err(AddError::Duplicate(hybrid))
        );
    }

    // A magnet naming only a v2 hash waits for its metainfo with that hash.
    let fresh = "ab".repeat(32);
    let magnet = rig
        .service
        .add(request(AddSource::Magnet(format!(
            "magnet:?xt=urn:btmh:1220{fresh}{tracker}"
        ))))
        .await
        .unwrap();
    let stored = rig.db.get_torrent(magnet).unwrap().unwrap();
    assert_eq!(stored.infohash_v1, None);
    assert_eq!(stored.infohash_v2, Some([0xab; 32]));
}

// The attributes a torrent is added with stay with its job, from the queue
// to history.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_torrent_keeps_the_attributes_it_was_added_with_into_history() {
    let rig = rig(set_clock());
    rig.service.open().await.unwrap();
    let metadata = vec![
        ("rss.feed_id".to_string(), "4".to_string()),
        ("tier".to_string(), "gold".to_string()),
    ];
    let job_id = rig
        .service
        .add(AddRequest {
            metadata: metadata.clone(),
            ..request(AddSource::Metainfo(plain("set", 1)))
        })
        .await
        .unwrap();
    assert_eq!(
        rig.db.get_torrent(job_id).unwrap().unwrap().metadata,
        metadata
    );
    let infos = rig.service.job_infos().await;
    assert_eq!(infos.len(), 1);
    assert_eq!(infos[0].metadata, metadata);

    rig.service.remove(job_id, false, false).await.unwrap();
    let history = rig.db.get_job_history(job_id.0).unwrap().unwrap();
    let kept: Vec<(String, String)> =
        serde_json::from_str(history.metadata.as_deref().unwrap()).unwrap();
    assert_eq!(kept, metadata);
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

// The session a torrent runs in, while it runs in one.
async fn session_of(service: &TorrentService, job_id: JobId) -> Arc<Session> {
    let live = service.inner.live.lock().await;
    Arc::clone(&live[&job_id].attached.as_ref().unwrap().session)
}

// A listen port given while a torrent runs reopens its route there at the
// next tick, the torrent going through the new session; settings that change
// nothing a session opens with leave it be.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_new_listen_port_reopens_the_torrents_route_on_it() {
    let rig = rig(set_clock());
    unbound_leg(&rig.routes);
    rig.service.open().await.unwrap();
    let job_id = rig
        .service
        .add(AddRequest {
            paused: true,
            ..request(AddSource::Metainfo(plain("set", 1)))
        })
        .await
        .unwrap();
    let listening = || async {
        let views = rig.service.inner.sessions.views().await;
        assert_eq!(views.len(), 1);
        views[0].listen.map(|listen| listen.port())
    };
    let before = session_of(&rig.service, job_id).await;
    assert_eq!(listening().await, Some(6881));

    let mut settings = rig.service.settings();
    settings.torrent.max_peers_total = 12;
    rig.service.set_settings(settings.clone()).await;
    rig.service.tick().await;
    assert!(Arc::ptr_eq(
        &before,
        &session_of(&rig.service, job_id).await
    ));

    settings.torrent.base_port = 41_000;
    rig.service.set_settings(settings).await;
    rig.service.tick().await;
    let after = session_of(&rig.service, job_id).await;
    assert!(!Arc::ptr_eq(&before, &after));
    assert!(before.cancellation_token().is_cancelled());
    assert_eq!(listening().await, Some(41_000));
    assert_eq!(
        after.listen_addr().map(|listen| listen.port()),
        Some(41_000)
    );
    assert!(
        after
            .get(TorrentIdOrHash::Id(
                handle_of(&rig.service, job_id).await.id()
            ))
            .is_some()
    );
    rig.service.stop().await;
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

// The tick writes a ledger that changed once a minute has passed since it
// was last written, and nothing while it stands still.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_ledger_is_written_once_a_minute_while_it_changes() {
    let clock = set_clock();
    let rig = rig(Arc::clone(&clock) as Arc<dyn Clock>);
    // No session is built while the route is down.
    rig.routes.set_down(Some("down"));
    rig.service.open().await.unwrap();
    let job_id = rig
        .service
        .add(request(AddSource::Metainfo(plain("set", 1))))
        .await
        .unwrap();
    let written = rig.service.written();
    let base = written.borrow().ledgers;
    // What a session moved for it.
    rig.service
        .inner
        .live
        .lock()
        .await
        .get_mut(&job_id)
        .unwrap()
        .counters
        .uploaded_bytes = 5;
    clock.0.fetch_add(TICK.as_millis() as i64, Ordering::SeqCst);
    rig.service.tick().await;
    assert_eq!(written.borrow().ledgers, base);
    assert_eq!(
        rig.db.get_torrent_counters(job_id).unwrap().unwrap(),
        TorrentCounters::default()
    );
    clock.0.fetch_add(LEDGER_EVERY_MS, Ordering::SeqCst);
    rig.service.tick().await;
    assert_eq!(written.borrow().ledgers, base + 1);
    assert_eq!(
        rig.db
            .get_torrent_counters(job_id)
            .unwrap()
            .unwrap()
            .uploaded_bytes,
        5
    );
    clock.0.fetch_add(LEDGER_EVERY_MS, Ordering::SeqCst);
    rig.service.tick().await;
    assert_eq!(written.borrow().ledgers, base + 1);
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
    let storage = PieceStorage::new(
        Arc::clone(&layout),
        intake as Arc<dyn Pieces>,
        rig.service.resident(job_id),
    );
    let lengths = Lengths::new(
        layout.files().map(|file| file.length).sum(),
        layout.piece_length() as u32,
    )
    .unwrap();
    for piece in 0..layout.piece_count() {
        let range = layout.piece_range(piece);
        let index = lengths.try_validate_piece_index(piece).unwrap();
        assert!(storage.reserve_piece(index, &Arc::default()));
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
        if let Err(error) = storage.on_piece_completed(index) {
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
    use weaver_torrent::{
        AddTorrent, AddTorrentOptions, ListenerMode, ListenerOptions, SessionOptions,
    };

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

// A full disk puts the session in upload mode: the torrent shows the disk as
// full and the file selection stays as it was (libtorrent
// `torrent::handle_disk_error` enters upload mode and leaves the file
// priorities alone).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_full_disk_leaves_the_selection_as_it_was() {
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

    let reading = CapacityReading {
        available_bytes: 0,
        total_bytes: 1 << 30,
        sampled_at: std::time::Instant::now(),
        stale: false,
    };
    let mut limits = ExtractionLimits::unbounded();
    limits.min_free_bytes = 100;
    let env = JobEnv {
        free_disk: CapacityReader::from_fn(move || Capacity::Known(reading)),
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
    rig.service.open().await.unwrap();
    let job_id = rig
        .service
        .add(request(AddSource::Metainfo(bytes)))
        .await
        .unwrap();

    // The first piece finds no room and the session enters upload mode.
    let torrent = handle_of(&rig.service, job_id).await;
    torrent.wait_until_initialized().await.unwrap();
    torrent.live().unwrap().wait_for_upload_mode(true).await;

    rig.service.tick().await;
    let view = rig.service.torrent(job_id).await.unwrap();
    assert_eq!(view.state, TorrentState::Stalled);
    assert_eq!(view.detail.as_deref(), Some(DISK_FULL));
    assert_eq!(view.completed_at_ms, None);
    assert_eq!(torrent.file_priorities(), [FilePriority::Normal]);
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
        upload_mode: false,
    };
    take_file_have(&mut have, &live, &[]);
    assert_eq!(have, [1000, 2500]);
    take_file_have(&mut have, &live, &[1024, 2500]);
    assert_eq!(have, [1024, 2500]);
}

// The bytes in hand over the torrent's total leave padding out, as the total
// and the session's progress do.
#[test]
fn padding_in_hand_is_not_progress() {
    use crate::pipeline::graph::root::pieces::PieceFile;

    let file = |name: &str, length, padding| PieceFile {
        name: name.to_owned(),
        length,
        padding,
    };
    let layout = PieceLayout::new(
        16,
        vec![
            file("a", 10, false),
            file(".pad/6", 6, true),
            file("b", 16, false),
        ],
    )
    .unwrap();
    assert_eq!(on_disk(Some(&layout), &[10, 6, 4]), 14);
    assert_eq!(on_disk(None, &[]), 0);
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
    reopen(&again).await;
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
    // A hold keeps a torrent in the queue and says whose it is; its own
    // pause is a pause.
    let state = |job_id| {
        let service = rig.service.clone();
        async move {
            let view = service.torrent(job_id).await.unwrap();
            (view.state, view.detail)
        }
    };
    let held = |job_id| {
        let state = state.clone();
        async move { matches!(state(job_id).await, (TorrentState::Queued, Some(_))) }
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
    // the torrent ran in its session is active, and it ran complete. Inside
    // a minute of the last write nothing is written, and a clean stop
    // writes the ledger as it stands.
    rig.service.tick().await;
    clock.0.fetch_add(30_000, Ordering::SeqCst);
    rig.service.tick().await;
    let kept = rig.db.get_torrent_counters(job_id).unwrap().unwrap();
    assert_eq!(kept.active_duration_s, 0);
    rig.service.stop().await;
    let ledger = rig.db.get_torrent_counters(job_id).unwrap().unwrap();
    assert_eq!(
        (
            ledger.downloaded_bytes,
            ledger.active_duration_s,
            ledger.last_seen_complete_ms,
        ),
        (total, 30, Some(EPOCH_MS + 30_000))
    );
    // What the server's recovery clears before anything starts.
    rig.db.prune_orphan_active_state().unwrap();

    let again = service_on(&rig.db, rig.dir.path(), &rig.routes, job_env(), set_clock());
    let handle = queue();
    let mut changes = handle.state().subscribe_job_changes();
    again.publish_to(handle);
    reopen(&again).await;
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

// Starting never waits on a route. After a restart whose route has not
// answered, the service starts with its torrents waiting: the torrent at
// the tick, the magnet on the task that fetches its metainfo. Once the
// route answers, the torrent goes into its session.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_restart_starts_before_the_torrents_route_answers() {
    let rig = rig(set_clock());
    rig.service.open().await.unwrap();
    let job_id = rig
        .service
        .add(request(AddSource::Metainfo(plain("set", 1))))
        .await
        .unwrap();
    let hash = "0123456789abcdef0123456789abcdef01234567";
    let magnet = rig
        .service
        .add(request(AddSource::Magnet(format!(
            "magnet:?xt=urn:btih:{hash}&tr=http%3A%2F%2F127.0.0.1%3A9%2Fannounce"
        ))))
        .await
        .unwrap();
    rig.service.stop().await;

    rig.routes.unanswered.send_replace(true);
    let before = *rig.routes.asked.borrow();
    let mut asked = rig.routes.asked.subscribe();
    let again = service_on(&rig.db, rig.dir.path(), &rig.routes, job_env(), set_clock());
    let mut ticks = again.ticks();
    // As the server starts it.
    again.inner.sessions.turn(true).await;
    again.start().await.unwrap();
    asked.wait_for(|asked| *asked > before).await.unwrap();
    let view = again.torrent(job_id).await.unwrap();
    assert_eq!(view.state, TorrentState::Queued);
    assert!(again.inner.live.lock().await[&job_id].attached.is_none());
    assert_eq!(
        again.torrent(magnet).await.unwrap().state,
        TorrentState::FetchingMetadata
    );

    rig.routes.unanswered.send_replace(false);
    ticks.wait_for(|ticks| *ticks >= 1).await.unwrap();
    assert!(again.inner.live.lock().await[&job_id].attached.is_some());
    assert!(again.torrent(job_id).await.unwrap().route.is_some());
    again.stop().await;
}

// A peer on `listener` that says it saw a complete copy `ago` seconds ago,
// then that it has piece 0: the BitTorrent handshake with the extension
// bit, an extended handshake carrying `complete_ago` (BEP 10), and a have.
// Returns once the session says it is interested, which it reads after the
// extended handshake. A connection that opens encrypted is let go: the
// session tries plaintext next.
async fn claims_complete(listener: &tokio::net::TcpListener, ago: u64) -> tokio::net::TcpStream {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    const PROTOCOL: &[u8] = b"\x13BitTorrent protocol";
    loop {
        let (mut stream, _) = listener.accept().await.unwrap();
        let mut theirs = [0u8; 68];
        if stream.read_exact(&mut theirs).await.is_err() || &theirs[..20] != PROTOCOL {
            continue;
        }
        let mut ours = PROTOCOL.to_vec();
        ours.extend_from_slice(&[0, 0, 0, 0, 0, 0x10, 0, 0]);
        ours.extend_from_slice(&theirs[28..48]);
        ours.extend_from_slice(b"-XX0000-claimscomplt");
        let extended = format!("d12:complete_agoi{ago}e1:mdee").into_bytes();
        let mut message = |id: &[u8], payload: &[u8]| {
            ours.extend_from_slice(
                &u32::try_from(id.len() + payload.len())
                    .unwrap()
                    .to_be_bytes(),
            );
            ours.extend_from_slice(id);
            ours.extend_from_slice(payload);
        };
        message(&[20, 0], &extended);
        message(&[4], &0u32.to_be_bytes());
        stream.write_all(&ours).await.unwrap();
        loop {
            let mut len = [0u8; 4];
            stream.read_exact(&mut len).await.unwrap();
            let mut body = vec![0u8; u32::from_be_bytes(len) as usize];
            stream.read_exact(&mut body).await.unwrap();
            if body.first() == Some(&2) {
                return stream;
            }
        }
    }
}

// libtorrent `torrent::second_tick` keeps `m_swarm_last_seen_complete`, the
// latest of our own sighting and each connected peer's `complete_ago`, and
// `torrent::status` reports it as `last_seen_complete` (qBittorrent's
// `seen_complete`). A peer's word that it saw a complete copy dates the
// torrent's, though nobody connected is a seed; once the peer is gone the
// date stands.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_peers_sighting_of_a_complete_copy_dates_the_torrents() {
    let data: Vec<u8> = (0..65_536u32).map(|at| (at % 251) as u8).collect();
    let files = [("set/a.bin", data.as_slice())];
    let private = metainfo("set", &files, 16_384, true);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let std::net::SocketAddr::V4(peer) = listener.local_addr().unwrap() else {
        panic!("the peer listens on IPv4 loopback");
    };
    let bytes = announcing(&private, &tracker(peer).await);

    let rig = rig(set_clock());
    on_loopback(&rig.routes);
    rig.service.open().await.unwrap();
    let job_id = rig
        .service
        .add(request(AddSource::Metainfo(bytes)))
        .await
        .unwrap();
    let seen = || {
        rig.db
            .get_torrent_counters(job_id)
            .unwrap()
            .unwrap()
            .last_seen_complete_ms
    };
    rig.service.tick().await;
    assert_eq!(seen(), None);

    let connection = claims_complete(&listener, 100).await;
    rig.service.tick().await;
    let at = rig
        .service
        .torrent(job_id)
        .await
        .unwrap()
        .last_seen_complete_ms
        .expect("the peer's sighting is the torrent's");
    assert!(at <= EPOCH_MS - 100_000, "{at}");

    // With the peer gone, as a pause disconnects every peer, nobody is
    // left to say it: the date kept stands, written as the pause is.
    rig.service.pause(job_id).await.unwrap();
    assert_eq!(seen(), Some(at));
    let handle = handle_of(&rig.service, job_id).await;
    assert!(handle.live().is_none());
    assert_eq!(rig.service.torrent(job_id).await.unwrap().peers, 0);
    drop(connection);
    rig.service.tick().await;
    assert_eq!(seen(), Some(at));
    rig.service.stop().await;
}

// The one file of `dir`'s tree.
fn only_file(dir: &Path) -> PathBuf {
    let mut found = Vec::new();
    let mut dirs = vec![dir.to_path_buf()];
    while let Some(dir) = dirs.pop() {
        for entry in std::fs::read_dir(dir).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                dirs.push(path);
            } else {
                found.push(path);
            }
        }
    }
    assert_eq!(found.len(), 1, "{found:?}");
    found.remove(0)
}

// libtorrent `torrent::force_recheck`: a complete torrent rechecked after
// its data was damaged forgets the damaged piece, trusting nothing saved,
// and a running torrent runs again once checked.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_rechecked_torrent_loses_a_damaged_piece_and_runs_again() {
    use weaver_torrent::TorrentStatsState;

    const PIECE: usize = 16_384;
    let data: Vec<u8> = (0..4 * PIECE as u32).map(|at| (at % 251) as u8).collect();
    let files = [("set/a.bin", data.as_slice())];
    let private = metainfo("set", &files, PIECE, true);

    let seeds = tempfile::tempdir().unwrap();
    let seeding = seeder(seeds.path(), &private, &files).await;
    let std::net::SocketAddr::V4(peer) = seeding.listen_addr().unwrap() else {
        panic!("the seeder listens on IPv4 loopback");
    };
    let bytes = announcing(&private, &tracker(peer).await);

    let rig = rig(set_clock());
    on_loopback(&rig.routes);
    rig.service.open().await.unwrap();
    let job_id = rig
        .service
        .add(request(AddSource::Metainfo(bytes)))
        .await
        .unwrap();
    let handle = handle_of(&rig.service, job_id).await;
    handle.wait_until_completed().await.unwrap();
    // Nobody is left to fetch the damaged piece from again.
    seeding.stop().await;

    let dir = rig
        .service
        .torrent(job_id)
        .await
        .unwrap()
        .storage_dir
        .unwrap();
    {
        use std::io::{Seek, SeekFrom, Write};
        let mut file = std::fs::OpenOptions::new()
            .write(true)
            .open(only_file(Path::new(&dir)))
            .unwrap();
        file.seek(SeekFrom::Start(2 * PIECE as u64 + 100)).unwrap();
        file.write_all(&[0x5a; 16]).unwrap();
    }
    rig.service.recheck(job_id).await.unwrap();
    handle.wait_until_initialized().await.unwrap();

    let stats = handle.stats();
    assert!(matches!(stats.state, TorrentStatsState::Live));
    assert!(!handle.is_paused());
    assert!(!stats.finished);
    assert_eq!(stats.progress_bytes, 3 * PIECE as u64);
    assert_eq!(
        rig.service.torrent(job_id).await.unwrap().have_bytes,
        3 * PIECE as u64
    );
    rig.service.stop().await;
}

// A torrent in no session has nothing to check it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_torrent_in_no_session_is_not_rechecked() {
    let rig = rig(set_clock());
    rig.service.open().await.unwrap();
    let job_id = rig
        .service
        .add(request(AddSource::Metainfo(plain("set", 1))))
        .await
        .unwrap();
    rig.routes.set_down(Some("tunnel gone"));
    rig.service.tick().await;
    assert!(matches!(
        rig.service.recheck(job_id).await,
        Err(ActionError::Invalid(_))
    ));
    assert_eq!(
        rig.service.recheck(JobId(job_id.0 + 1)).await,
        Err(ActionError::NotFound(JobId(job_id.0 + 1)))
    );
    rig.service.stop().await;
}

const RECHECKED_PIECE: usize = 16_384;

// A torrent of four pieces fetched whole from a seeder on loopback, then
// its file's piece 2 damaged on disk.
struct Damaged {
    rig: Rig,
    job_id: JobId,
    handle: Arc<ManagedTorrent>,
    seeding: Arc<Session>,
    // The seeder's files, read when a peer asks.
    _seeds: tempfile::TempDir,
    // What the file should hold.
    data: Vec<u8>,
    file: PathBuf,
}

async fn complete_then_damaged() -> Damaged {
    complete_then_damaged_of(4).await
}

// As `complete_then_damaged`, of `pieces` pieces.
async fn complete_then_damaged_of(pieces: usize) -> Damaged {
    const PIECE: usize = RECHECKED_PIECE;
    let data: Vec<u8> = (0..(pieces * PIECE) as u32)
        .map(|at| (at % 251) as u8)
        .collect();
    let files = [("set/a.bin", data.as_slice())];
    let private = metainfo("set", &files, PIECE, true);

    let seeds = tempfile::tempdir().unwrap();
    let seeding = seeder(seeds.path(), &private, &files).await;
    let std::net::SocketAddr::V4(peer) = seeding.listen_addr().unwrap() else {
        panic!("the seeder listens on IPv4 loopback");
    };
    let bytes = announcing(&private, &tracker(peer).await);

    let rig = rig(set_clock());
    on_loopback(&rig.routes);
    rig.service.open().await.unwrap();
    let job_id = rig
        .service
        .add(request(AddSource::Metainfo(bytes)))
        .await
        .unwrap();
    let handle = handle_of(&rig.service, job_id).await;
    handle.wait_until_completed().await.unwrap();
    let dir = rig
        .service
        .torrent(job_id)
        .await
        .unwrap()
        .storage_dir
        .unwrap();
    let file = only_file(Path::new(&dir));
    {
        use std::io::{Seek, SeekFrom, Write};
        let mut opened = std::fs::OpenOptions::new().write(true).open(&file).unwrap();
        opened
            .seek(SeekFrom::Start(2 * PIECE as u64 + 100))
            .unwrap();
        opened.write_all(&[0x5a; 16]).unwrap();
    }
    Damaged {
        rig,
        job_id,
        handle,
        seeding,
        _seeds: seeds,
        data,
        file,
    }
}

// The job's pieces in hand, as its view shows them.
async fn job_have(service: &TorrentService, job_id: JobId) -> u32 {
    service.torrent(job_id).await.unwrap().pieces.unwrap().have
}

// libtorrent `torrent::force_recheck` forgets every piece and
// `torrent::on_piece_hashed` has only the ones that pass: the session's
// check is the one word on which pieces are had. The job lets the damaged
// piece go, a peer sends it again, and the file holds the torrent's bytes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_rechecked_piece_the_job_let_go_is_fetched_again_and_repairs_the_file() {
    let Damaged {
        rig,
        job_id,
        handle,
        seeding,
        _seeds,
        data,
        file,
    } = complete_then_damaged().await;
    assert_ne!(std::fs::read(&file).unwrap(), data);

    rig.service.recheck(job_id).await.unwrap();
    handle.wait_until_initialized().await.unwrap();
    handle.wait_until_completed().await.unwrap();

    assert_eq!(std::fs::read(&file).unwrap(), data);
    assert_eq!(job_have(&rig.service, job_id).await, 4);
    assert_eq!(
        rig.service.torrent(job_id).await.unwrap().have_bytes,
        data.len() as u64
    );
    rig.service.stop().await;
    seeding.stop().await;
}

// The job's checkpoint says what the check found: a restart before the
// damaged piece came again neither has it nor trusts it had.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_restart_after_a_recheck_does_not_have_the_piece_it_lost() {
    use weaver_torrent::TorrentStatsState;

    let Damaged {
        rig,
        job_id,
        handle,
        seeding,
        data,
        ..
    } = complete_then_damaged().await;
    // Nobody is left to fetch the damaged piece from again.
    seeding.stop().await;
    assert_eq!(job_have(&rig.service, job_id).await, 4);

    rig.service.recheck(job_id).await.unwrap();
    handle.wait_until_initialized().await.unwrap();
    assert_eq!(job_have(&rig.service, job_id).await, 3);
    rig.service.stop().await;
    rig.db.prune_orphan_active_state().unwrap();

    let again = service_on(&rig.db, rig.dir.path(), &rig.routes, job_env(), set_clock());
    reopen(&again).await;
    let handle = handle_of(&again, job_id).await;
    handle.wait_until_initialized().await.unwrap();
    let stats = handle.stats();
    assert!(matches!(stats.state, TorrentStatsState::Live));
    assert!(!stats.finished);
    assert_eq!(
        stats.progress_bytes,
        data.len() as u64 - RECHECKED_PIECE as u64
    );
    assert_eq!(job_have(&again, job_id).await, 3);
    again.stop().await;
}

// libtorrent `torrent::on_piece_hashed` skips the rest of a file only when
// the file ends: a recheck of a torrent the job holds only some pieces of
// keeps every piece it holds after the one it does not.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_recheck_of_a_partial_torrent_keeps_its_pieces_after_a_hole() {
    const PIECES: u32 = 16;
    let Damaged {
        rig,
        job_id,
        handle,
        seeding,
        data,
        ..
    } = complete_then_damaged_of(PIECES as usize).await;
    // Nobody is left to fetch the damaged piece from again.
    seeding.stop().await;
    rig.service.recheck(job_id).await.unwrap();
    handle.wait_until_initialized().await.unwrap();
    assert_eq!(job_have(&rig.service, job_id).await, PIECES - 1);

    // Piece 2 is a hole in the job now; piece 3 follows it in the file.
    rig.service.recheck(job_id).await.unwrap();
    handle.wait_until_initialized().await.unwrap();
    assert_eq!(job_have(&rig.service, job_id).await, PIECES - 1);
    assert_eq!(
        handle.stats().progress_bytes,
        data.len() as u64 - RECHECKED_PIECE as u64
    );
    rig.service.stop().await;
}

// libtorrent `torrent::force_recheck` forgets every piece before checking,
// and `torrent::write_resume_data` saves none the check has not found: a
// stop after a check of every piece began, before it found every piece,
// trusts nothing on the next start, which checks every piece and does not
// have the damaged one. The check is begun as the session begins it, by
// telling the job's intake, and the service stopped before any piece is
// checked.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_stop_before_a_recheck_found_every_piece_checks_every_piece_on_the_next_start() {
    use weaver_torrent::TorrentStatsState;

    let Damaged {
        rig,
        job_id,
        seeding,
        data,
        ..
    } = complete_then_damaged().await;
    // Nobody is left to fetch the damaged piece from again.
    seeding.stop().await;
    let intake = rig
        .service
        .with_torrent(job_id, |torrent| Ok(torrent.intake.clone()))
        .await
        .unwrap()
        .unwrap();
    assert!(intake.checking().unwrap());
    assert_eq!(job_have(&rig.service, job_id).await, 0);
    rig.service.stop().await;
    rig.db.prune_orphan_active_state().unwrap();

    let again = service_on(&rig.db, rig.dir.path(), &rig.routes, job_env(), set_clock());
    reopen(&again).await;
    let handle = handle_of(&again, job_id).await;
    handle.wait_until_initialized().await.unwrap();
    let stats = handle.stats();
    assert!(matches!(stats.state, TorrentStatsState::Live));
    assert!(!stats.finished);
    assert_eq!(
        stats.progress_bytes,
        data.len() as u64 - RECHECKED_PIECE as u64
    );
    assert_eq!(job_have(&again, job_id).await, 3);
    again.stop().await;
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
            .map(|tracker| (
                tracker.tier,
                tracker.url.as_str(),
                tracker.status,
                tracker.updating
            ))
            .collect::<Vec<_>>(),
        [(0, url.as_str(), TrackerStatus::NotContacted, true)]
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
    assert!(!working.updating);
    assert_eq!((working.seeders, working.leechers), (Some(3), Some(5)));
    assert_eq!((working.message.as_deref(), working.fails), (None, 0));
    let at = working.last_announce_ms.unwrap();
    assert_eq!(working.next_announce_ms, Some(at + 1_800_000));

    // Paused, the stopped announce is the row's last outcome (refused: the
    // tracker is gone) and nothing is due until the torrent runs again.
    rig.service.pause(job_id).await.unwrap();
    let trackers = trackers_once(&rig.service, job_id, |announces| {
        !announces[0].in_flight
            && announces[0]
                .last
                .as_ref()
                .is_some_and(|last| last.result.is_err())
    })
    .await;
    let paused = &trackers[0];
    assert_eq!(paused.status, TrackerStatus::NotWorking);
    assert!(!paused.updating);
    assert_eq!((paused.next_announce_ms, paused.fails), (None, 1));

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
    assert!(first[0].trackers[0].updating);

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

// A torrent under a tracker rule that names a client names itself as that
// client to its peers and trackers; a torrent under no rule names itself as
// Weaver.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_tracker_rules_client_names_its_torrents() {
    use crate::torrents::identity::IDENTITY;

    let rig = rig(set_clock());
    rig.service.open().await.unwrap();
    rig.db
        .insert_tracker_rule(
            &TrackerRule {
                id: 0,
                host_pattern: "*.test".into(),
                seeding: None,
                never_pause: false,
                unpack: None,
                client: Some(Client::QBittorrent),
                torrent_route: false,
                discovery: None,
                encryption: None,
                upload_limit: None,
                forced: false,
                ssl: None,
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

    let named = |identity: weaver_torrent::Identity| {
        (
            identity.peer_id.0[..8].to_vec(),
            identity.user_agent,
            identity.version,
        )
    };
    let qbittorrent = Client::QBittorrent.name();
    assert_eq!(
        named(handle_of(&rig.service, kept).await.shared().identity()),
        (
            qbittorrent.prefix().to_vec(),
            qbittorrent.user_agent().to_owned(),
            qbittorrent.version().to_owned()
        )
    );
    assert_eq!(
        named(handle_of(&rig.service, plain).await.shared().identity()),
        (
            IDENTITY.prefix().to_vec(),
            IDENTITY.user_agent().to_owned(),
            IDENTITY.version().to_owned()
        )
    );
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
                client: None,
                torrent_route: false,
                discovery: None,
                encryption: None,
                upload_limit: None,
                forced: false,
                ssl: None,
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
    assert_eq!(rig.service.inner.live.lock().await[&kept].rule_id, None);
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
        category_id: Some(1),
        added_at_ms: EPOCH_MS,
        intent: TorrentIntent::Resumed,
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
    };
    let mut rules = vec![TrackerRule {
        id: 1,
        host_pattern: "tracker.test".into(),
        seeding: None,
        never_pause: false,
        unpack: None,
        client: None,
        torrent_route: false,
        discovery: None,
        encryption: None,
        upload_limit: None,
        forced: false,
        ssl: None,
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

    assert!(settings.unpack(&record, rules.first()));
    settings.torrent.unpack = false;
    assert!(!settings.unpack(&record, rules.first()));
    settings.categories[0].unpack = Some(true);
    assert!(settings.unpack(&record, rules.first()));
    rules[0].unpack = Some(false);
    assert!(!settings.unpack(&record, rules.first()));
    record.unpack = Some(true);
    assert!(settings.unpack(&record, rules.first()));
}

// A torrent takes its tracker rule's route when the rule routes its torrents
// itself, else its category's when that does, else the default.
#[test]
fn a_torrents_route_is_its_rules_then_its_categorys_then_the_default() {
    let category = |id, torrent_route| CategoryConfig {
        id,
        name: format!("c{id}"),
        dest_dir: None,
        aliases: String::new(),
        seeding: None,
        torrent_route,
        unpack: None,
    };
    let settings = TorrentSettings {
        complete_dir: PathBuf::from("/complete"),
        categories: vec![category(1, true), category(2, false)],
        seeding: None,
        torrent: TorrentConfig::default(),
    };
    let mut rule = TrackerRule {
        id: 3,
        host_pattern: "tracker.test".into(),
        seeding: None,
        never_pause: false,
        unpack: None,
        client: None,
        torrent_route: true,
        discovery: None,
        encryption: None,
        upload_limit: None,
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

// A torrent under a tracker rule that routes its torrents itself joins the
// rule's session ahead of its category's; one under no rule joins its
// category's. Once the rule's route is off, its torrent moves to its
// category's session at the next tick.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_tracker_rules_route_carries_its_torrents_ahead_of_their_categorys() {
    let rig = rig(set_clock());
    rig.service.open().await.unwrap();
    let rule = rig
        .db
        .insert_tracker_rule(
            &TrackerRule {
                id: 0,
                host_pattern: "*.test".into(),
                seeding: None,
                never_pause: false,
                unpack: None,
                client: None,
                torrent_route: true,
                discovery: None,
                encryption: None,
                upload_limit: None,
                forced: false,
                ssl: None,
            },
            EPOCH_MS,
        )
        .unwrap();
    rig.service.reload_tracker_rules().await.unwrap();
    rig.routes.route(TorrentScope::Category(1), 9);
    rig.routes.route(TorrentScope::TrackerRule(rule.id), 7);
    let key = |scope| rig.routes.plan(scope).unwrap().key.as_str().to_owned();
    // Paused, so nothing announces.
    let in_tv = |metainfo| AddRequest {
        category: Some("tv".into()),
        paused: true,
        ..request(AddSource::Metainfo(metainfo))
    };
    let ruled = rig
        .service
        .add(in_tv(announcing(
            &plain("ruled", 1),
            "http://tracker.test/announce",
        )))
        .await
        .unwrap();
    let unruled = rig.service.add(in_tv(plain("unruled", 2))).await.unwrap();
    let route = async |job_id| rig.service.torrent(job_id).await.unwrap().route;

    assert_eq!(
        route(ruled).await,
        Some(key(TorrentScope::TrackerRule(rule.id)))
    );
    assert_eq!(route(unruled).await, Some(key(TorrentScope::Category(1))));

    rig.db
        .update_tracker_rule(
            &TrackerRule {
                torrent_route: false,
                discovery: None,
                encryption: None,
                upload_limit: None,
                forced: false,
                ..rule
            },
            EPOCH_MS,
        )
        .unwrap();
    rig.service.reload_tracker_rules().await.unwrap();
    rig.service.tick().await;
    assert_eq!(route(ruled).await, Some(key(TorrentScope::Category(1))));
    rig.service.stop().await;
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

// libtorrent `torrent::write_resume_data` and `read_resume_data`: the peers
// and bans a torrent saved come back when it next starts, before it runs,
// and it saves them again as it stops.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_torrents_saved_peers_and_bans_outlive_a_restart() {
    use weaver_torrent::ResumePeers;

    let rig = rig(set_clock());
    on_loopback(&rig.routes);
    rig.service.open().await.unwrap();
    let job_id = rig
        .service
        .add(AddRequest {
            paused: true,
            ..request(AddSource::Metainfo(plain("set", 1)))
        })
        .await
        .unwrap();
    assert_eq!(
        handle_of(&rig.service, job_id).await.resume_peers(),
        Some(ResumePeers::default())
    );
    rig.service.stop().await;
    assert_eq!(
        rig.db.torrent_peers(job_id).unwrap(),
        ResumePeers::default()
    );
    let saved = ResumePeers {
        peers: vec!["127.0.0.2:6881".parse().unwrap()],
        banned: vec!["127.0.0.3".parse().unwrap()],
    };
    assert!(rig.db.save_torrent_peers(job_id, &saved).unwrap());

    let again = service_on(&rig.db, rig.dir.path(), &rig.routes, job_env(), set_clock());
    reopen(&again).await;
    assert_eq!(
        handle_of(&again, job_id).await.resume_peers(),
        Some(saved.clone())
    );
    again.stop().await;
    assert_eq!(rig.db.torrent_peers(job_id).unwrap(), saved);
}

// libtorrent sends every resume-data write of a torrent through its disk job
// queue, and `torrent::abort` waits out the jobs outstanding: a save already
// on its way when the torrent stops lands before stop's own, never after it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_save_on_its_way_at_stop_lands_before_stops_own() {
    use std::task::Poll;
    use weaver_torrent::ResumePeers;

    let rig = rig(set_clock());
    on_loopback(&rig.routes);
    rig.service.open().await.unwrap();
    let job_id = rig
        .service
        .add(AddRequest {
            paused: true,
            ..request(AddSource::Metainfo(plain("set", 1)))
        })
        .await
        .unwrap();
    // A ban the session holds and the database does not yet: stop writes
    // only what changed.
    let handle = handle_of(&rig.service, job_id).await;
    handle.ban_peers(&["127.0.0.9".parse().unwrap()]);
    let current = handle.resume_peers().unwrap();
    // An older save of the torrent on its way: the writer runs it, held
    // until the test lets it go.
    let older = ResumePeers {
        peers: vec!["127.0.0.2:6881".parse().unwrap()],
        banned: vec!["127.0.0.3".parse().unwrap()],
    };
    assert_ne!(older, current);
    let (running, run) = tokio::sync::oneshot::channel();
    let (release, held) = std::sync::mpsc::channel::<()>();
    rig.db
        .try_queue_job_write(job_id, "older_peers", move |db| {
            let _ = running.send(());
            let _ = held.recv();
            db.save_torrent_peers(job_id, &older).map(drop)
        })
        .unwrap();
    run.await.unwrap();

    let mut stopping = std::pin::pin!(rig.service.stop());
    let waiting =
        std::future::poll_fn(|cx| Poll::Ready(stopping.as_mut().poll(cx).is_pending())).await;
    assert!(waiting);
    // Stop's save waits behind the one on its way.
    assert_eq!(rig.db.queued_writes(), 1);
    release.send(()).unwrap();
    stopping.await;
    assert_eq!(rig.db.torrent_peers(job_id).unwrap(), current);
}

// qBittorrent `TorrentImpl::setSavePath`: the torrent's directory moves
// under the new path and the torrent goes on from there; the record follows
// the move, a move to where it already is does nothing, and a relative path
// is refused.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_torrent_moves_under_another_directory_and_its_record_follows() {
    let rig = rig(set_clock());
    rig.routes.set_down(Some("down"));
    rig.service.open().await.unwrap();
    let job_id = rig
        .service
        .add(request(AddSource::Metainfo(plain("set", 1))))
        .await
        .unwrap();
    let stored = |rig: &Rig| {
        rig.db
            .get_torrent(job_id)
            .unwrap()
            .unwrap()
            .storage_dir
            .unwrap()
    };
    let from = stored(&rig);
    assert!(Path::new(&from).is_dir());

    let elsewhere = rig.dir.path().join("elsewhere");
    let location = elsewhere.to_str().unwrap().to_owned();
    rig.service
        .set_location(job_id, location.clone())
        .await
        .unwrap();
    let to = stored(&rig);
    assert_eq!(Path::new(&to), elsewhere.join("set"));
    assert!(Path::new(&to).is_dir());
    assert!(!Path::new(&from).exists());
    assert!(
        rig.service
            .inner
            .live
            .lock()
            .await
            .get(&job_id)
            .unwrap()
            .moving
            .is_none()
    );

    rig.service.set_location(job_id, location).await.unwrap();
    assert_eq!(stored(&rig), to);
    assert!(matches!(
        rig.service
            .set_location(job_id, "elsewhere".to_owned())
            .await,
        Err(ActionError::Invalid(_))
    ));
    assert_eq!(stored(&rig), to);
}

// libtorrent `torrent_handle::get_peer_info` lists a torrent's connections:
// none while it is paused, and a torrent the service does not hold has none
// to list.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_paused_torrents_peers_are_none_and_an_unknown_ones_are_not_found() {
    let rig = rig(set_clock());
    on_loopback(&rig.routes);
    rig.service.open().await.unwrap();
    let job_id = rig
        .service
        .add(AddRequest {
            paused: true,
            ..request(AddSource::Metainfo(plain("set", 1)))
        })
        .await
        .unwrap();
    assert!(rig.service.peers(job_id).await.unwrap().is_empty());
    let unknown = JobId(job_id.0 + 100);
    assert!(matches!(
        rig.service.peers(unknown).await,
        Err(ActionError::NotFound(id)) if id == unknown
    ));
    assert!(matches!(
        rig.service
            .ban_peers(unknown, &["127.0.0.3".parse().unwrap()])
            .await,
        Err(ActionError::NotFound(id)) if id == unknown
    ));
    rig.service.stop().await;
}

// libtorrent `torrent::ban_peer` bans the address on the torrent's peer
// list, and `write_resume_data` saves it with the bans: a ban made in a
// session, and one made while the torrent is in none, are both on the list
// its next start begins with, its saved peer at a banned address gone.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_banned_peer_stays_banned_across_a_restart() {
    use std::net::IpAddr;
    use weaver_torrent::ResumePeers;

    let rig = rig(set_clock());
    on_loopback(&rig.routes);
    rig.service.open().await.unwrap();
    let job_id = rig
        .service
        .add(AddRequest {
            paused: true,
            ..request(AddSource::Metainfo(plain("set", 1)))
        })
        .await
        .unwrap();
    let in_session: IpAddr = "127.0.0.3".parse().unwrap();
    rig.service.ban_peers(job_id, &[in_session]).await.unwrap();
    let held = handle_of(&rig.service, job_id)
        .await
        .resume_peers()
        .unwrap();
    assert_eq!(held.banned, [in_session]);
    assert_eq!(rig.db.torrent_peers(job_id).unwrap().banned, [in_session]);
    rig.service.stop().await;
    // Two peers it knows of when it next starts, beside the ban written.
    let mut known = rig.db.torrent_peers(job_id).unwrap();
    known.peers = vec![
        "127.0.0.4:6881".parse().unwrap(),
        "127.0.0.5:6881".parse().unwrap(),
    ];
    assert!(rig.db.save_torrent_peers(job_id, &known).unwrap());

    // The ban made in the session holds in the next one.
    let again = service_on(&rig.db, rig.dir.path(), &rig.routes, job_env(), set_clock());
    reopen(&again).await;
    assert_eq!(
        handle_of(&again, job_id)
            .await
            .resume_peers()
            .unwrap()
            .banned,
        [in_session]
    );

    // In no session: torrents are off.
    let mut settings = again.settings();
    settings.torrent.enabled = false;
    again.set_settings(settings).await;
    assert!(again.peers(job_id).await.unwrap().is_empty());
    let off: IpAddr = "127.0.0.4".parse().unwrap();
    again.ban_peers(job_id, &[off]).await.unwrap();
    let saved = ResumePeers {
        peers: vec!["127.0.0.5:6881".parse().unwrap()],
        banned: vec![in_session, off],
    };
    assert_eq!(rig.db.torrent_peers(job_id).unwrap(), saved);
    again.stop().await;

    let third = service_on(&rig.db, rig.dir.path(), &rig.routes, job_env(), set_clock());
    reopen(&third).await;
    assert_eq!(
        handle_of(&third, job_id).await.resume_peers(),
        Some(saved.clone())
    );
    third.stop().await;
    assert_eq!(rig.db.torrent_peers(job_id).unwrap(), saved);
}

// A ban made while the torrent is in no session is on its list once it is
// in one: attach hands the session the bans the torrent holds, with no
// read of the database between.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_ban_made_in_no_session_holds_once_the_torrent_is_in_one() {
    use std::net::IpAddr;

    let rig = rig(set_clock());
    rig.routes.set_down(Some("down"));
    rig.service.open().await.unwrap();
    let job_id = rig
        .service
        .add(AddRequest {
            paused: true,
            ..request(AddSource::Metainfo(plain("set", 1)))
        })
        .await
        .unwrap();
    let early: IpAddr = "127.0.0.6".parse().unwrap();
    rig.service.ban_peers(job_id, &[early]).await.unwrap();
    assert_eq!(rig.db.torrent_peers(job_id).unwrap().banned, [early]);
    rig.routes.set_down(None);
    on_loopback(&rig.routes);
    rig.service.attach(job_id).await;
    assert_eq!(
        handle_of(&rig.service, job_id)
            .await
            .resume_peers()
            .unwrap()
            .banned,
        [early]
    );
    rig.service.stop().await;
}

// A checkpoint writes only what changed since the database last held the
// job: a second save of a job that did nothing writes no row, and a job
// brought back from its checkpoint writes none either.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_second_checkpoint_writes_only_what_changed() {
    let rig = rig(set_clock());
    rig.routes.set_down(Some("down"));
    rig.service.open().await.unwrap();
    let job_id = rig
        .service
        .add(request(AddSource::Metainfo(plain("set", 1))))
        .await
        .unwrap();
    let job = |service: &TorrentService| {
        let service = service.clone();
        async move {
            service.inner.live.lock().await[&job_id]
                .job
                .clone()
                .unwrap()
        }
    };
    let first = job(&rig.service).await;
    rig.service.save_checkpoint(job_id, &first).await;
    let whole = first.with(|job| job.pending().tally());
    assert!(whole.rows > 0);
    rig.service.save_checkpoint(job_id, &first).await;
    let again = first.with(|job| job.pending().tally());
    assert_eq!(again.checkpoints, whole.checkpoints + 1);
    assert_eq!(again.rows, whole.rows);
    assert!(rig.db.load_graph_checkpoint(job_id).unwrap().is_some());
    rig.service.stop().await;

    let restarted = service_on(&rig.db, rig.dir.path(), &rig.routes, job_env(), set_clock());
    reopen(&restarted).await;
    let restored = job(&restarted).await;
    restarted.save_checkpoint(job_id, &restored).await;
    assert_eq!(restored.with(|job| job.pending().tally()).rows, 0);
    restarted.stop().await;
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
        .add(request(AddSource::Metainfo(plain("set", 1))))
        .await
        .unwrap();
    let (paused, ()) = tokio::join!(rig.service.pause(job_id), rig.service.completed(job_id));
    paused.unwrap();
    let stored = rig.db.get_torrent(job_id).unwrap().unwrap();
    assert_eq!(stored.intent, TorrentIntent::Paused);
    assert_eq!(stored.completed_at_ms, Some(EPOCH_MS));
    rig.service.resume(job_id).await.unwrap();
    let stored = rig.db.get_torrent(job_id).unwrap().unwrap();
    assert_eq!(stored.intent, TorrentIntent::Resumed);
    assert_eq!(stored.completed_at_ms, Some(EPOCH_MS));
}

// libtorrent `torrent::set_upload_limit` / `set_download_limit` throttle the
// torrent's own channels in place, and qBittorrent's `TorrentImpl` keeps
// them in its resume data: the limits reach the running torrent's session
// at once, are what it reads back, and are handed to its session again
// after a restart.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_torrents_rate_limits_reach_its_session_and_outlive_a_restart() {
    use weaver_torrent::limits::{LimitKind, LimitsConfig};

    let bps = |bps: u32| LimitKind::Bps(std::num::NonZeroU32::new(bps).unwrap());
    let rig = rig(set_clock());
    on_loopback(&rig.routes);
    rig.service.open().await.unwrap();
    let job_id = rig
        .service
        .add(AddRequest {
            paused: true,
            ..request(AddSource::Metainfo(plain("set", 1)))
        })
        .await
        .unwrap();
    let handle = handle_of(&rig.service, job_id).await;
    assert_eq!(handle.ratelimits(), LimitsConfig::default());
    let view = rig.service.torrent(job_id).await.unwrap();
    assert_eq!((view.upload_limit, view.download_limit), (None, None));

    // Zero holds the download closed.
    rig.service
        .set_rate_limits(job_id, Some(Some(65_536)), Some(Some(0)))
        .await
        .unwrap();
    let limited = LimitsConfig {
        upload_bps: bps(65_536),
        download_bps: LimitKind::Closed,
    };
    assert_eq!(handle.ratelimits(), limited);
    let view = rig.service.torrent(job_id).await.unwrap();
    assert_eq!(
        (view.upload_limit, view.download_limit),
        (Some(65_536), Some(0))
    );
    let stored = rig.db.get_torrent(job_id).unwrap().unwrap();
    assert_eq!(
        (stored.upload_limit, stored.download_limit),
        (Some(65_536), Some(0))
    );
    assert!(matches!(
        rig.service
            .set_rate_limits(JobId(999), Some(None), None)
            .await,
        Err(ActionError::NotFound(_))
    ));
    // A limit past what the row holds is refused before the torrent or
    // its row takes it.
    assert!(matches!(
        rig.service
            .set_rate_limits(job_id, Some(Some(u64::MAX)), Some(None))
            .await,
        Err(ActionError::Invalid(_))
    ));
    assert_eq!(handle.ratelimits(), limited);
    let stored = rig.db.get_torrent(job_id).unwrap().unwrap();
    assert_eq!(
        (stored.upload_limit, stored.download_limit),
        (Some(65_536), Some(0))
    );
    rig.service.stop().await;

    let again = service_on(&rig.db, rig.dir.path(), &rig.routes, job_env(), set_clock());
    reopen(&again).await;
    assert_eq!(handle_of(&again, job_id).await.ratelimits(), limited);
    let view = again.torrent(job_id).await.unwrap();
    assert_eq!(
        (view.upload_limit, view.download_limit),
        (Some(65_536), Some(0))
    );
    again.stop().await;
}

// A seeding policy's upload cap holds the torrent beside its own limit, as
// a second channel over its peers would: the tighter of the two is in
// force, and a change of either reaches the session without a restart.
// The torrent reads back its own limit.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_tighter_of_a_torrents_limit_and_its_policys_upload_cap_holds() {
    use weaver_torrent::limits::LimitKind;

    let bps = |bps: u32| LimitKind::Bps(std::num::NonZeroU32::new(bps).unwrap());
    let rig = rig(set_clock());
    on_loopback(&rig.routes);
    rig.service.open().await.unwrap();
    let job_id = rig
        .service
        .add(AddRequest {
            paused: true,
            ..request(AddSource::Metainfo(plain("set", 1)))
        })
        .await
        .unwrap();
    let handle = handle_of(&rig.service, job_id).await;
    rig.service
        .set_rate_limits(job_id, Some(Some(65_536)), None)
        .await
        .unwrap();
    let capped = |max_upload_bps| SeedingPolicy {
        max_upload_bps,
        ..SeedingPolicy::default()
    };
    rig.service
        .set_policy_override(job_id, Some(capped(Some(1_000))))
        .await
        .unwrap();
    rig.service.tick().await;
    assert_eq!(handle.ratelimits().upload_bps, bps(1_000));
    assert_eq!(
        rig.service.torrent(job_id).await.unwrap().upload_limit,
        Some(65_536)
    );

    rig.service
        .set_policy_override(job_id, Some(capped(Some(1_000_000))))
        .await
        .unwrap();
    rig.service.tick().await;
    assert_eq!(handle.ratelimits().upload_bps, bps(65_536));

    rig.service
        .set_rate_limits(job_id, Some(None), None)
        .await
        .unwrap();
    assert_eq!(handle.ratelimits().upload_bps, bps(1_000_000));
    rig.service.stop().await;
}

// A tracker rule's terms reach its torrents: one admitted under it starts
// forced, takes peers from its trackers alone, makes its connections under
// the rule's encryption and uploads under the rule's limit in place of the
// global seeding cap. A change of the rule reaches the running torrent at
// the next tick, through its session again; a torrent under no rule keeps
// the global terms throughout.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_tracker_rules_privacy_terms_reach_its_torrents() {
    use crate::settings::{TorrentEncryption, TrackerUploadLimit};
    use weaver_torrent::EncryptionPolicy;
    use weaver_torrent::limits::LimitKind;

    let bps = |bps: u32| LimitKind::Bps(std::num::NonZeroU32::new(bps).unwrap());
    let rig = rig(set_clock());
    on_loopback(&rig.routes);
    rig.service.open().await.unwrap();
    let mut settings = rig.service.settings();
    settings.seeding = Some(SeedingPolicy {
        max_upload_bps: Some(2_000),
        ..SeedingPolicy::default()
    });
    rig.service.set_settings(settings).await;
    let rule = rig
        .db
        .insert_tracker_rule(
            &TrackerRule {
                id: 0,
                host_pattern: "*.example-tracker.test".into(),
                seeding: None,
                never_pause: false,
                unpack: None,
                client: None,
                torrent_route: false,
                discovery: Some(false),
                encryption: Some(TorrentEncryption::Prefer),
                upload_limit: Some(TrackerUploadLimit::Unlimited),
                forced: true,
                ssl: None,
            },
            EPOCH_MS,
        )
        .unwrap();
    rig.service.reload_tracker_rules().await.unwrap();
    // Paused, so nothing announces.
    let paused = |metainfo| AddRequest {
        paused: true,
        ..request(AddSource::Metainfo(metainfo))
    };
    let ruled = rig
        .service
        .add(paused(announcing(
            &plain("ruled", 1),
            "http://tracker.example-tracker.test/announce",
        )))
        .await
        .unwrap();
    let unruled = rig.service.add(paused(plain("unruled", 2))).await.unwrap();

    assert!(rig.service.torrent(ruled).await.unwrap().forced);
    assert!(rig.db.get_torrent(ruled).unwrap().unwrap().forced);
    assert!(!rig.service.torrent(unruled).await.unwrap().forced);
    let handle = handle_of(&rig.service, ruled).await;
    assert!(handle.takes_peers_from_trackers_only());
    assert_eq!(handle.encryption(), Some(EncryptionPolicy::Prefer));
    assert_eq!(handle.ratelimits().upload_bps, LimitKind::Unlimited);
    let other = handle_of(&rig.service, unruled).await;
    assert!(!other.takes_peers_from_trackers_only());
    assert_eq!(other.encryption(), None);
    assert_eq!(other.ratelimits().upload_bps, bps(2_000));

    rig.db
        .update_tracker_rule(
            &TrackerRule {
                discovery: None,
                encryption: Some(TorrentEncryption::Require),
                upload_limit: Some(TrackerUploadLimit::Bps(1_000)),
                ..rule
            },
            EPOCH_MS,
        )
        .unwrap();
    rig.service.reload_tracker_rules().await.unwrap();
    rig.service.tick().await;
    let again = handle_of(&rig.service, ruled).await;
    assert!(!Arc::ptr_eq(&handle, &again));
    assert!(!again.takes_peers_from_trackers_only());
    assert_eq!(again.encryption(), Some(EncryptionPolicy::Require));
    assert_eq!(again.ratelimits().upload_bps, bps(1_000));
    assert!(Arc::ptr_eq(&other, &handle_of(&rig.service, unruled).await));
    assert_eq!(other.ratelimits().upload_bps, bps(2_000));
    rig.service.stop().await;
}

// The job's raise puts the files it names at Maximum and leaves every other
// file at its stored level; no raise leaves every level as stored, and an
// ignored file stays ignored.
#[test]
fn a_raise_sets_maximum_on_the_raised_files_and_leaves_the_rest() {
    use FilePriority::*;
    let stored = [Normal, High, Ignored, Normal];
    assert_eq!(raised_levels(&stored, vec![]), stored);
    assert_eq!(
        raised_levels(&stored, vec![1]),
        [Normal, Maximum, Ignored, Normal]
    );
    assert_eq!(
        raised_levels(&stored, vec![0, 2]),
        [Maximum, High, Ignored, Normal]
    );
}

// A file the job raises reaches the session at Maximum through the one
// reconcile, an ignored file stays ignored under a raise, and a toggle the
// session runs against what is stored is put back on the next pass.
#[tokio::test(start_paused = true)]
async fn a_raise_and_the_stored_toggles_reach_the_session_through_one_reconcile() {
    use FilePriority::*;
    let rig = rig(set_clock());
    on_loopback(&rig.routes);
    rig.service.open().await.unwrap();
    let bytes = metainfo(
        "trio",
        // Whole pieces each, so a raise of one file raises no piece of another.
        &[
            ("trio/a.bin", &[1; 3072]),
            ("trio/b.bin", &[2; 3072]),
            ("trio/c.bin", &[3; 3072]),
        ],
        1024,
        false,
    );
    let job_id = rig
        .service
        .add(request(AddSource::Metainfo(bytes)))
        .await
        .unwrap();
    let handle = handle_of(&rig.service, job_id).await;
    handle.wait_until_initialized().await.unwrap();
    rig.service
        .set_file_priorities(job_id, &[(0, High), (2, Ignored)])
        .await
        .unwrap();
    assert_eq!(handle.file_priorities(), [High, Normal, Ignored]);

    {
        let live = rig.service.inner.live.lock().await;
        let job = live[&job_id].job.as_ref().unwrap();
        job.with(|job| {
            job.raise(1);
            job.raise(2);
        });
    }
    rig.service.apply_picking().await;
    assert_eq!(handle.file_priorities(), [High, Maximum, Ignored]);
    // The operator's levels are what is stored; the raise is the job's.
    let stored = rig.db.torrent_file_priorities(job_id).unwrap();
    assert_eq!(stored, [High, Normal, Ignored]);

    rig.service
        .set_first_last_piece_first(job_id, true)
        .await
        .unwrap();
    handle.set_sequential(true);
    handle.set_first_last_piece_first(false).unwrap();
    rig.service.apply_picking().await;
    assert!(!handle.sequential());
    assert!(handle.first_last_piece_first());
    rig.service.stop().await;
}

// The two picking toggles set at once both hold: each call writes only its
// own, so neither puts back the other's old value, in the database or in
// memory.
#[tokio::test(start_paused = true)]
async fn the_two_picking_toggles_set_at_once_both_hold() {
    use std::future::Future;
    use std::task::Poll;
    let rig = rig(set_clock());
    on_loopback(&rig.routes);
    rig.service.open().await.unwrap();
    let bytes = metainfo("pair", &[("pair/a.bin", &[1; 3000])], 1024, false);
    let job_id = rig
        .service
        .add(request(AddSource::Metainfo(bytes)))
        .await
        .unwrap();
    let handle = handle_of(&rig.service, job_id).await;
    handle.wait_until_initialized().await.unwrap();

    // Both calls queue on the live lock before either runs, so each reads
    // the torrent before the other has changed it.
    let live = rig.service.inner.live.lock().await;
    let mut both = std::pin::pin!(async {
        tokio::join!(
            rig.service.set_sequential(job_id, true),
            rig.service.set_first_last_piece_first(job_id, true),
        )
    });
    std::future::poll_fn(|cx| {
        assert!(both.as_mut().poll(cx).is_pending());
        Poll::Ready(())
    })
    .await;
    drop(live);
    let (sequential, first_last) = both.await;
    sequential.unwrap();
    first_last.unwrap();

    let record = rig.db.get_torrent(job_id).unwrap().unwrap();
    assert!(record.sequential && record.first_last_piece_first);
    let view = rig.service.torrent(job_id).await.unwrap();
    assert!(view.sequential && view.first_last_piece_first);
    assert!(handle.sequential() && handle.first_last_piece_first());
    rig.service.stop().await;
}

// A level change and the two picking toggles are stored, reach the
// torrent's session at once, and come back with it after a restart.
#[tokio::test(start_paused = true)]
async fn file_levels_and_picking_toggles_reach_the_session_and_outlive_a_restart() {
    use FilePriority::*;
    let rig = rig(set_clock());
    on_loopback(&rig.routes);
    rig.service.open().await.unwrap();
    let bytes = metainfo(
        "pair",
        &[("pair/a.bin", &[1; 3000]), ("pair/b.bin", &[2; 3000])],
        1024,
        false,
    );
    let job_id = rig
        .service
        .add(request(AddSource::Metainfo(bytes)))
        .await
        .unwrap();
    let handle = handle_of(&rig.service, job_id).await;
    handle.wait_until_initialized().await.unwrap();
    assert_eq!(handle.file_priorities(), [Normal, Normal]);

    rig.service
        .set_file_priorities(job_id, &[(0, Ignored), (1, High)])
        .await
        .unwrap();
    assert!(
        rig.service
            .set_file_priorities(job_id, &[(2, High)])
            .await
            .is_err()
    );
    let stored = rig.db.torrent_file_priorities(job_id).unwrap();
    assert_eq!(stored, [Ignored, High]);
    assert_eq!(handle.file_priorities(), [Ignored, High]);
    let view = rig.service.torrent(job_id).await.unwrap();
    assert_eq!(
        view.files
            .iter()
            .map(|file| file.selected())
            .collect::<Vec<_>>(),
        [false, true]
    );

    rig.service.set_sequential(job_id, true).await.unwrap();
    rig.service
        .set_first_last_piece_first(job_id, true)
        .await
        .unwrap();
    assert!(handle.sequential() && handle.first_last_piece_first());
    let view = rig.service.torrent(job_id).await.unwrap();
    assert!(view.sequential && view.first_last_piece_first);
    rig.service.set_sequential(job_id, false).await.unwrap();
    assert!(!handle.sequential());
    rig.service.stop().await;

    let again = service_on(&rig.db, rig.dir.path(), &rig.routes, job_env(), set_clock());
    reopen(&again).await;
    let handle = handle_of(&again, job_id).await;
    assert_eq!(handle.file_priorities(), [Ignored, High]);
    assert!(!handle.sequential());
    assert!(handle.first_last_piece_first());
    let view = again.torrent(job_id).await.unwrap();
    assert!(!view.sequential && view.first_last_piece_first);
    again.stop().await;
}

// The torrent's trackers as the service lists them: tier and URL.
async fn listed_trackers(service: &TorrentService, job_id: JobId) -> Vec<(u32, String)> {
    service
        .trackers(job_id)
        .await
        .unwrap()
        .into_iter()
        .map(|tracker| (tracker.tier, tracker.url))
        .collect()
}

// Added, replaced and removed trackers are written where the torrent's
// trackers live, reach its session, and are what a restart starts with.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tracker_edits_outlive_a_restart() {
    let a = "http://a.invalid/announce";
    let b = "http://b.invalid/announce";
    let c = "udp://c.invalid:80";
    let rig = rig(set_clock());
    rig.service.open().await.unwrap();
    // Paused, it announces to none of them.
    let job_id = rig
        .service
        .add(AddRequest {
            paused: true,
            ..request(AddSource::Metainfo(announcing(&plain("edited", 3), a)))
        })
        .await
        .unwrap();

    // Added tiers go after the last; a tracker it lists is left out.
    rig.service
        .add_trackers(
            job_id,
            vec![vec![b.into(), a.into()], vec![], vec![c.into()]],
        )
        .await
        .unwrap();
    assert_eq!(
        listed_trackers(&rig.service, job_id).await,
        [(0, a.into()), (1, b.into()), (2, c.into())]
    );

    rig.service
        .replace_trackers(
            job_id,
            vec![
                TrackerEntry {
                    tier: 1,
                    url: a.into(),
                },
                TrackerEntry {
                    tier: 0,
                    url: c.into(),
                },
                TrackerEntry {
                    tier: 0,
                    url: c.into(),
                },
            ],
        )
        .await
        .unwrap();
    assert_eq!(
        listed_trackers(&rig.service, job_id).await,
        [(0, c.into()), (1, a.into())]
    );

    rig.service
        .remove_trackers(job_id, &[a.into()])
        .await
        .unwrap();
    assert_eq!(listed_trackers(&rig.service, job_id).await, [(0, c.into())]);
    assert_eq!(
        handle_of(&rig.service, job_id)
            .await
            .shared()
            .tracker_tiers(),
        [vec![weaver_torrent::Url::parse(c).unwrap()]]
    );

    // An edit naming no URL changes nothing.
    assert!(matches!(
        rig.service
            .add_trackers(job_id, vec![vec!["not a tracker".into()]])
            .await,
        Err(ActionError::Invalid(_))
    ));
    assert_eq!(
        rig.service
            .add_trackers(JobId(9_999), vec![vec![b.into()]])
            .await,
        Err(ActionError::NotFound(JobId(9_999)))
    );
    rig.service.stop().await;

    let again = service_on(&rig.db, rig.dir.path(), &rig.routes, job_env(), set_clock());
    reopen(&again).await;
    assert_eq!(listed_trackers(&again, job_id).await, [(0, c.into())]);
    assert_eq!(
        handle_of(&again, job_id).await.shared().tracker_tiers(),
        [vec![weaver_torrent::Url::parse(c).unwrap()]]
    );
    again.stop().await;
}

// A reannounce reaches the tracker now, long before the interval it set
// is up, for every tracker or for the one named.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_reannounce_reaches_the_tracker_now() {
    use tokio::io::AsyncWriteExt;

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/announce", listener.local_addr().unwrap());
    let rig = rig(set_clock());
    on_loopback(&rig.routes);
    rig.service.open().await.unwrap();
    let job_id = rig
        .service
        .add(request(AddSource::Metainfo(announcing(
            &plain("again", 4),
            &url,
        ))))
        .await
        .unwrap();
    let body = b"d8:completei3e10:incompletei5e8:intervali1800e12:min intervali1800e5:peers0:e";
    let head = format!(
        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    let answer = [head.as_bytes(), body].concat();
    // The announce table once an answer other than `before` has landed.
    let answered = |before: Option<std::time::SystemTime>| {
        move |announces: &[TrackerAnnounce]| {
            !announces[0].in_flight
                && announces[0]
                    .last
                    .as_ref()
                    .is_some_and(|last| last.result.is_ok() && Some(last.at) != before)
        }
    };
    let last_at = |service: &TorrentService| {
        let service = service.clone();
        async move {
            handle_of(&service, job_id).await.tracker_announces()[0]
                .last
                .as_ref()
                .map(|last| last.at)
        }
    };

    let mut started = heard_announce(&listener).await;
    started.write_all(&answer).await.unwrap();
    trackers_once(&rig.service, job_id, answered(None)).await;
    let first = last_at(&rig.service).await;

    // The tracker asked for 30 minutes between announces.
    rig.service.reannounce(job_id).await.unwrap();
    let mut again = heard_announce(&listener).await;
    again.write_all(&answer).await.unwrap();
    let second = trackers_once(&rig.service, job_id, answered(first)).await;
    assert_eq!(second[0].status, TrackerStatus::Working);

    assert!(matches!(
        rig.service
            .reannounce_tracker(job_id, "http://other.invalid/announce")
            .await,
        Err(ActionError::Invalid(_))
    ));
    rig.service.reannounce_tracker(job_id, &url).await.unwrap();
    let mut once_more = heard_announce(&listener).await;
    once_more.write_all(&answer).await.unwrap();

    rig.service.dht_announce(job_id).await.unwrap();
    assert_eq!(
        rig.service.reannounce(JobId(9_999)).await,
        Err(ActionError::NotFound(JobId(9_999)))
    );
    rig.service.stop().await;
}

// A torrent of two files where only the second is fetched: the first's
// front is never in hand, while its last bytes come in the piece that
// starts the second, so they are verified before anything says what the
// first file is.
const SPLIT_PIECE: usize = 16_384;

struct PartlyFetched {
    rig: Rig,
    job_id: JobId,
    seeding: Arc<Session>,
    _seeds: tempfile::TempDir,
    sizes: [u64; 2],
    sent: u64,
}

async fn partly_fetched() -> PartlyFetched {
    use FilePriority::*;
    let first: Vec<u8> = (0..(3 * SPLIT_PIECE + SPLIT_PIECE / 2) as u32)
        .map(|at| (at % 251) as u8)
        .collect();
    let second: Vec<u8> = (0..(SPLIT_PIECE / 2 + 2 * SPLIT_PIECE) as u32)
        .map(|at| (at % 241) as u8 ^ 0x5a)
        .collect();
    let files = [
        ("set/a.bin", first.as_slice()),
        ("set/b.bin", second.as_slice()),
    ];
    let private = metainfo("set", &files, SPLIT_PIECE, true);

    let seeds = tempfile::tempdir().unwrap();
    let seeding = seeder(seeds.path(), &private, &files).await;
    let std::net::SocketAddr::V4(peer) = seeding.listen_addr().unwrap() else {
        panic!("the seeder listens on IPv4 loopback");
    };
    let bytes = announcing(&private, &tracker(peer).await);

    let rig = rig(set_clock());
    on_loopback(&rig.routes);
    rig.service.open().await.unwrap();
    let job_id = rig
        .service
        .add(AddRequest {
            paused: true,
            ..request(AddSource::Metainfo(bytes))
        })
        .await
        .unwrap();
    rig.service
        .set_file_priorities(job_id, &[(0, Ignored)])
        .await
        .unwrap();
    rig.service.resume(job_id).await.unwrap();
    handle_of(&rig.service, job_id)
        .await
        .wait_until_completed()
        .await
        .unwrap();
    // Pieces 3, 4 and 5: the second file whole, and the first file's last
    // half piece with it.
    assert_eq!(job_have(&rig.service, job_id).await, 3);
    let sent = uploaded(&seeding);
    PartlyFetched {
        rig,
        job_id,
        seeding,
        _seeds: seeds,
        sizes: [first.len() as u64, second.len() as u64],
        sent,
    }
}

// The restart of a partly fetched torrent: it has every piece it had, and
// once the first file is wanted it fetches that file's three missing pieces
// and nothing else.
async fn resumes_with_its_pieces(fetched: PartlyFetched) {
    use FilePriority::*;
    let PartlyFetched {
        rig,
        job_id,
        seeding,
        sizes,
        sent,
        ..
    } = fetched;
    // What the server's recovery clears before anything starts.
    rig.db.prune_orphan_active_state().unwrap();

    let again = service_on(&rig.db, rig.dir.path(), &rig.routes, job_env(), set_clock());
    reopen(&again).await;
    let handle = handle_of(&again, job_id).await;
    handle.wait_until_completed().await.unwrap();
    assert_eq!(job_have(&again, job_id).await, 3);
    assert_eq!(uploaded(&seeding), sent);

    again
        .set_file_priorities(job_id, &[(0, Normal)])
        .await
        .unwrap();
    handle.wait_until_completed().await.unwrap();
    assert_eq!(job_have(&again, job_id).await, 6);
    assert_eq!(again.torrent(job_id).await.unwrap().file_have, sizes);
    assert_eq!(uploaded(&seeding) - sent, 3 * SPLIT_PIECE as u64);
    again.stop().await;
    seeding.stop().await;
}

// A verified piece is durable: a torrent stopped part-way restarts with
// every piece it had, a piece no file had yet said what it was included,
// and fetches only the pieces it lacks.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_torrent_stopped_part_way_restarts_with_its_pieces() {
    let fetched = partly_fetched().await;
    fetched.rig.service.stop().await;
    resumes_with_its_pieces(fetched).await;
}

// As a clean stop, a crash: the checkpoint the last tick wrote is all the
// restart has, and it has every piece the torrent had by then.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_torrent_that_crashed_part_way_restarts_with_its_pieces() {
    let fetched = partly_fetched().await;
    let written = fetched.rig.service.written();
    let before = written.borrow().checkpoints;
    fetched.rig.service.tick().await;
    assert!(written.borrow().checkpoints > before);
    // The process ends here: its sessions go with it, and nothing else is
    // written.
    fetched.rig.service.inner.sessions.stop_all().await;
    resumes_with_its_pieces(fetched).await;
}

// A checkpoint that does not restore says so, naming the job and why, and
// the torrent starts fresh rather than not at all.
#[tokio::test]
async fn a_corrupted_checkpoint_is_logged_and_the_torrent_starts_fresh() {
    use crate::persistence::sql_runtime::{SqlArg, SqlRuntime};

    let rig = rig(set_clock());
    rig.routes.set_down(Some("down"));
    rig.service.open().await.unwrap();
    let job_id = rig
        .service
        .add(request(AddSource::Metainfo(plain("set", 1))))
        .await
        .unwrap();
    rig.service.inner.signal.mark(job_id);
    rig.service.tick().await;
    rig.service.stop().await;
    assert!(rig.db.load_graph_checkpoint(job_id).unwrap().is_some());
    let datastore = rig.db.datastore();
    rig.db
        .run_sql_blocking(async move {
            SqlRuntime::execute(
                datastore.read_exec(),
                "UPDATE graph_nodes SET facts = {} WHERE job_id = {} AND node_id = 0",
                &[SqlArg::Bytes(vec![0xff; 4]), SqlArg::I64(job_id.0 as i64)],
            )
            .await?;
            Ok(())
        })
        .unwrap();

    let (lines, _guard) = crate::pipeline::graph::lines::Lines::capture_at(
        "weaver_server_core::torrents",
        tracing::Level::WARN,
    );
    let again = service_on(&rig.db, rig.dir.path(), &rig.routes, job_env(), set_clock());
    reopen(&again).await;
    let heard = lines.of("a torrent's checkpoint did not restore; it starts fresh");
    assert_eq!(heard.len(), 1, "{heard:?}");
    assert!(
        heard[0].contains(&format!(" job_id={} error=", job_id.0)),
        "{heard:?}"
    );
    let view = again.torrent(job_id).await.unwrap();
    assert_eq!(view.pieces.unwrap().have, 0);
    again.stop().await;
}
