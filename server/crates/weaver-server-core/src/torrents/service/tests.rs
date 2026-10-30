// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use std::sync::atomic::{AtomicI64, Ordering};

use sha1::{Digest, Sha1};
use weaver_torrent::core::lengths::Lengths;
use weaver_torrent::storage::TorrentStorage;

use super::*;
use crate::jobs::{Hold, HoldOwner, HoldScope};
use crate::post_processing::model::PostProcessingSettings;
use crate::proxies::{Consumer, LegPath, RouteLeg, RoutingPolicy, SYSTEM_EGRESS_ID};
use crate::settings::{SeedingAction, SeedingGoalMode};
use crate::torrents::TrackerStatus;
use crate::torrents::sessions::Routes;
use crate::torrents::sessions::tests::{
    FakeRoutes, env, loopback_proxy, on_network, save_again, unbound_leg,
};
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
    service_over(
        db,
        dir,
        Arc::clone(routes) as Arc<dyn Routes>,
        job_env,
        clock,
    )
}

// A service over `db` and `dir` whose sessions open on `routes`.
fn service_over(
    db: &Database,
    dir: &Path,
    routes: Arc<dyn Routes>,
    job_env: JobEnv,
    clock: Arc<dyn Clock>,
) -> TorrentService {
    let sessions = Sessions::new(routes, env(&dir.join("sessions")), db.clone());
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
        upload_limit: None,
        download_limit: None,
        placement: Default::default(),
    }
}

// A queue whose pipeline holds no live jobs. A retired job goes to its
// finished jobs, newest first.
fn queue() -> SchedulerHandle {
    let state = crate::jobs::SharedPipelineState::new(crate::PipelineMetrics::new(), vec![]);
    let (event_tx, _) = tokio::sync::broadcast::channel(16);
    SchedulerHandle::new(event_tx, state)
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
        route_reason: None,
        paused_by: None,
        category: None,
        scope: TorrentScope::Default,
        private: false,
        forced: false,
        progress: 0.25,
        total_bytes: 4000,
        have_bytes: 1000,
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
        (view.detail.as_deref(), view.route_reason),
        (None, Some(TorrentRouteReason::LegDown))
    );
    let held = rig.service.inner.sessions.views().await;
    assert_eq!(held.len(), 1);
    assert_eq!(
        held[0]
            .held
            .as_ref()
            .and_then(|failure| failure.detail.as_deref()),
        Some("tunnel gone")
    );

    rig.routes.set_down(None);
    rig.service.tick().await;
    let view = rig.service.torrent(job_id).await.unwrap();
    assert_eq!(view.route_reason, None);
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

// `policy` stored as `consumer`'s route as it is, past the checks a save
// makes: a route kept from before they held.
fn store_route_unchecked(db: &Database, consumer: Consumer, policy: &RoutingPolicy) {
    use crate::persistence::sql_runtime::{SqlArg, SqlRuntime};
    let store = db.datastore();
    let row = (consumer.key(), serde_json::to_string(policy).unwrap());
    db.run_sql_blocking(async move {
        SqlRuntime::run_in_transaction(&store, "route_fixture", |tx| {
            let (key, policy) = row.clone();
            Box::pin(async move {
                tx.execute(
                    "INSERT INTO proxy_routes (consumer, policy) VALUES ({}, {})",
                    &[SqlArg::Text(key), SqlArg::Text(policy)],
                )
                .await?;
                Ok(())
            })
        })
        .await
    })
    .unwrap();
}

// A torrent added by URL is asked for as the client of the tracker rule its
// URL's host falls under, and as Weaver under no rule.
#[tokio::test(flavor = "multi_thread")]
async fn a_torrent_url_is_asked_for_as_the_client_its_rule_names() {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let dir = tempfile::tempdir().unwrap();
    let db = Database::open_in_memory().unwrap();
    let proxies =
        crate::proxies::ProxyRuntime::new(db.clone(), tokio::runtime::Handle::current()).unwrap();
    let routes = Arc::new(crate::torrents::sessions::NetworkRoutes::new(
        db.clone(),
        Arc::clone(&proxies),
        Duration::from_secs(30),
    ));
    let service = service_over(&db, dir.path(), routes, job_env(), set_clock());
    service.open().await.unwrap();

    let site = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url =
        reqwest::Url::parse(&format!("http://{}/a.torrent", site.local_addr().unwrap())).unwrap();
    let body = b"d4:infod4:name1:aee";
    let fetch_as = async || {
        let served = async {
            let (mut stream, _) = site.accept().await.unwrap();
            let mut request = Vec::new();
            let mut buf = [0u8; 1024];
            while !request.windows(4).any(|end| end == b"\r\n\r\n") {
                let read = stream.read(&mut buf).await.unwrap();
                assert!(read > 0, "the request ended before it was read");
                request.extend_from_slice(&buf[..read]);
            }
            let head = format!(
                "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                body.len()
            );
            stream.write_all(head.as_bytes()).await.unwrap();
            stream.write_all(body).await.unwrap();
            String::from_utf8(request)
                .unwrap()
                .lines()
                .find_map(|line| {
                    let (name, value) = line.split_once(':')?;
                    name.eq_ignore_ascii_case("user-agent")
                        .then(|| value.trim().to_owned())
                })
        };
        let (fetched, agent) = tokio::join!(service.fetch_url(&proxies, None, &url, None), served);
        assert_eq!(fetched.unwrap(), body);
        agent
    };

    assert_eq!(
        fetch_as().await.as_deref(),
        Some(crate::torrents::identity::IDENTITY.user_agent())
    );

    db.insert_tracker_rule(
        &TrackerRule {
            id: 0,
            host_pattern: "127.0.0.1".into(),
            seeding: None,
            never_pause: false,
            unpack: None,
            client: Some(Client::QBittorrent),
            torrent_route: false,
            dht: None,
            pex: None,
            lsd: None,
            encryption: None,
            upload_limit: None,
            forced: false,
            ssl: None,
        },
        EPOCH_MS,
    )
    .unwrap();
    service.reload_tracker_rules().await.unwrap();
    assert_eq!(
        fetch_as().await.as_deref(),
        Some(Client::QBittorrent.name().user_agent())
    );
    service.stop().await;
}

#[tokio::test]
async fn stored_route_faults_keep_their_typed_reason() {
    use crate::proxies::{Rung, TorrentRouteFault};
    use crate::torrents::sessions::{NetworkRoutes, TorrentRouteReason};

    let direct = RouteLeg {
        egress_id: SYSTEM_EGRESS_ID,
        weight: 100,
        path: LegPath::Direct,
    };
    let ladder = |rungs, direct_fallback| RouteLeg {
        path: LegPath::Ladder {
            rungs,
            direct_fallback,
        },
        ..direct.clone()
    };
    for (legs, fault) in [
        (
            vec![direct.clone(), direct.clone()],
            TorrentRouteFault::Legs,
        ),
        (
            vec![ladder(vec![Rung::Proxy { id: 1 }], true)],
            TorrentRouteFault::DirectFallback,
        ),
        (
            vec![ladder(
                vec![Rung::Proxy { id: 1 }, Rung::Proxy { id: 2 }],
                false,
            )],
            TorrentRouteFault::Rungs,
        ),
        (
            vec![ladder(vec![Rung::Pool { id: 1 }], false)],
            TorrentRouteFault::Pool,
        ),
    ] {
        let db = Database::open_in_memory().unwrap();
        let proxies =
            crate::proxies::ProxyRuntime::new(db.clone(), tokio::runtime::Handle::current())
                .unwrap();
        store_route_unchecked(
            &db,
            Consumer::Torrent(TorrentScope::Default),
            &RoutingPolicy {
                legs,
                ..Default::default()
            },
        );
        let routes = NetworkRoutes::new(db, Arc::clone(&proxies), Duration::from_secs(30));
        let failure = routes.plan(TorrentScope::Default).unwrap().down.unwrap();
        assert_eq!(failure.reason, TorrentRouteReason::RouteBroken(fault));
        assert_eq!(failure.message(), fault.message());
        proxies.network.shutdown().await;
    }
}

#[tokio::test]
async fn a_missing_egress_has_a_typed_route_reason() {
    use crate::torrents::sessions::{NetworkRoutes, TorrentRouteReason};
    let db = Database::open_in_memory().unwrap();
    let proxies =
        crate::proxies::ProxyRuntime::new(db.clone(), tokio::runtime::Handle::current()).unwrap();
    store_route_unchecked(
        &db,
        Consumer::Torrent(TorrentScope::Default),
        &RoutingPolicy {
            legs: vec![RouteLeg {
                egress_id: 999,
                weight: 100,
                path: LegPath::Direct,
            }],
            ..Default::default()
        },
    );
    let routes = NetworkRoutes::new(db, Arc::clone(&proxies), Duration::from_secs(30));
    assert_eq!(
        routes
            .plan(TorrentScope::Default)
            .unwrap()
            .down
            .unwrap()
            .reason,
        TorrentRouteReason::LegMissing
    );
    proxies.network.shutdown().await;
}

#[tokio::test]
async fn a_route_plan_read_failure_stays_typed_in_the_torrents_status() {
    use crate::persistence::sql_runtime::SqlRuntime;
    use crate::torrents::sessions::{NetworkRoutes, TorrentRouteReason};
    let dir = tempfile::tempdir().unwrap();
    let db = Database::open_in_memory().unwrap();
    let proxies =
        crate::proxies::ProxyRuntime::new(db.clone(), tokio::runtime::Handle::current()).unwrap();
    store_route_unchecked(
        &db,
        Consumer::Torrent(TorrentScope::Default),
        &RoutingPolicy {
            legs: vec![RouteLeg {
                egress_id: 999,
                weight: 100,
                path: LegPath::Direct,
            }],
            ..Default::default()
        },
    );
    let routes = Arc::new(NetworkRoutes::new(
        db.clone(),
        Arc::clone(&proxies),
        Duration::from_secs(30),
    ));
    let service = service_over(&db, dir.path(), routes, job_env(), set_clock());
    service.open().await.unwrap();
    let job_id = service
        .add(request(AddSource::Metainfo(plain("set", 1))))
        .await
        .unwrap();
    let store = db.datastore();
    db.run_sql_blocking(async move {
        SqlRuntime::run_in_transaction(&store, "missing_routes_fixture", |tx| {
            Box::pin(async move {
                tx.execute("DROP TABLE proxy_routes", &[]).await?;
                Ok(())
            })
        })
        .await
    })
    .unwrap();
    service.tick().await;
    let live = service.inner.live.lock().await;
    let Some(Held::RouteDown(failure)) = &live[&job_id].held else {
        panic!("the failed plan did not hold its torrent")
    };
    assert_eq!(failure.reason, TorrentRouteReason::RoutePlanFailed);
    assert!(
        failure
            .detail
            .as_ref()
            .is_some_and(|detail| !detail.is_empty())
    );
    drop(live);
    // The route view lists the scope with the same reason.
    let listed = service.routes().await;
    let route = listed
        .iter()
        .find(|route| {
            route
                .scopes
                .iter()
                .any(|(scope, _)| *scope == TorrentScope::Default)
        })
        .expect("the scope whose plan failed is listed");
    assert_eq!(
        route.down.as_ref().map(|failure| failure.reason),
        Some(TorrentRouteReason::RoutePlanFailed)
    );
    service.stop().await;
    proxies.network.shutdown().await;
}

// A torrent added by URL is fetched over its route's one leg: up, the
// request reaches the site; down, the add fails for the route being down,
// and nothing is asked.
#[tokio::test(flavor = "multi_thread")]
async fn a_torrent_url_is_fetched_over_the_routes_one_leg_or_not_at_all() {
    url_route(None, false).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_torrent_url_takes_its_matched_rules_route_before_its_categorys() {
    url_route(Some("tv"), true).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn a_torrent_url_without_a_matched_rule_takes_its_categorys_route() {
    url_route(Some("tv"), false).await;
}

async fn url_route(category: Option<&str>, matched: bool) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let dir = tempfile::tempdir().unwrap();
    let db = Database::open_in_memory().unwrap();
    let proxies =
        crate::proxies::ProxyRuntime::new(db.clone(), tokio::runtime::Handle::current()).unwrap();
    let routes = Arc::new(crate::torrents::sessions::NetworkRoutes::new(
        db.clone(),
        Arc::clone(&proxies),
        Duration::from_secs(30),
    ));
    let service = service_over(&db, dir.path(), routes, job_env(), set_clock());
    let rule = db
        .insert_tracker_rule(
            &TrackerRule {
                id: 0,
                host_pattern: if matched {
                    "127.0.0.1"
                } else {
                    "unmatched.test"
                }
                .into(),
                seeding: None,
                never_pause: false,
                unpack: None,
                client: None,
                torrent_route: true,
                dht: None,
                pex: None,
                lsd: None,
                encryption: None,
                upload_limit: None,
                forced: false,
                ssl: None,
            },
            EPOCH_MS,
        )
        .unwrap();
    service.open().await.unwrap();
    service.reload_tracker_rules().await.unwrap();
    let scope = if matched {
        TorrentScope::TrackerRule(rule.id)
    } else {
        service.scope_of(category, None)
    };
    let category_config = &service.settings().categories[0];
    db.insert_category(category_config).unwrap();
    let category_scope = TorrentScope::Category(category_config.id);
    db.save_proxy_routing_policy(
        Consumer::Torrent(category_scope),
        &RoutingPolicy {
            legs: vec![RouteLeg {
                egress_id: SYSTEM_EGRESS_ID,
                weight: 100,
                path: LegPath::Direct,
            }],
            ..Default::default()
        },
    )
    .unwrap();
    proxies.reload().await.unwrap();

    let site = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url =
        reqwest::Url::parse(&format!("http://{}/a.torrent", site.local_addr().unwrap())).unwrap();
    let body = b"d4:infod4:name1:aee";
    let served = async {
        let (mut stream, _) = site.accept().await.unwrap();
        let mut request = Vec::new();
        let mut buf = [0u8; 1024];
        while !request.windows(4).any(|end| end == b"\r\n\r\n") {
            let read = stream.read(&mut buf).await.unwrap();
            assert!(read > 0, "the request ended before it was read");
            request.extend_from_slice(&buf[..read]);
        }
        let head = format!(
            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
            body.len()
        );
        stream.write_all(head.as_bytes()).await.unwrap();
        stream.write_all(body).await.unwrap();
    };
    let (fetched, ()) =
        tokio::try_join!(service.fetch_url(&proxies, category, &url, None), async {
            served.await;
            Ok::<_, String>(())
        })
        .unwrap();
    assert_eq!(fetched, body);

    // The route's one leg on an interface the host does not have.
    let absent = db
        .create_egress_interface(&crate::proxies::EgressInterface {
            id: 0,
            name: "absent".into(),
            binding: crate::proxies::EgressBinding::Interface {
                name: "weaverabsent0".into(),
            },
            enabled: true,
            download_bps: None,
            upload_bps: None,
            quota: Default::default(),
        })
        .unwrap();
    db.save_proxy_routing_policy(
        Consumer::Torrent(scope),
        &RoutingPolicy {
            legs: vec![RouteLeg {
                egress_id: absent.id,
                weight: 100,
                path: LegPath::Direct,
            }],
            ..Default::default()
        },
    )
    .unwrap();
    proxies.network.reload().await.unwrap();
    let down = service
        .inner
        .sessions
        .routes()
        .plan(scope)
        .unwrap()
        .down
        .expect("the leg is down");

    if matched {
        assert!(
            service
                .inner
                .sessions
                .routes()
                .plan(category_scope)
                .unwrap()
                .down
                .is_none()
        );
    } else {
        assert!(
            service
                .inner
                .sessions
                .routes()
                .plan(TorrentScope::TrackerRule(rule.id))
                .unwrap()
                .down
                .is_none()
        );
    }
    let refused = tokio::select! {
        refused = service.fetch_url(&proxies, category, &url, None) => refused,
        asked = site.accept() => panic!("the down route reached the site: {asked:?}"),
    };
    assert_eq!(
        down.reason,
        crate::torrents::sessions::TorrentRouteReason::LegDown
    );
    assert_eq!(refused, Err(format!("route down: {}", down.message())));
    let site = site.into_std().unwrap();
    site.set_nonblocking(true).unwrap();
    let asked = site.accept();
    assert!(
        matches!(&asked, Err(error) if error.kind() == std::io::ErrorKind::WouldBlock),
        "the site was asked: {asked:?}"
    );
    service.stop().await;
}

// A torrent route stored with two legs is no route a torrent may run on: it
// reads down for the refusal, the route view shows why, and its torrent is
// held. Saved again as its one leg, the route and the torrent are released.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_stored_route_with_two_exits_holds_until_it_is_edited_to_one() {
    let dir = tempfile::tempdir().unwrap();
    let db = Database::open_in_memory().unwrap();
    db.save_proxy_profile(&loopback_proxy(1)).unwrap();
    let through = |weight| RouteLeg {
        egress_id: SYSTEM_EGRESS_ID,
        weight,
        path: LegPath::Ladder {
            rungs: vec![crate::proxies::Rung::Proxy { id: 1 }],
            direct_fallback: false,
        },
    };
    let consumer = Consumer::Torrent(TorrentScope::Default);
    store_route_unchecked(
        &db,
        consumer,
        &RoutingPolicy {
            legs: vec![
                through(50),
                RouteLeg {
                    weight: 50,
                    path: LegPath::Direct,
                    ..through(50)
                },
            ],
            ..Default::default()
        },
    );
    let proxies =
        crate::proxies::ProxyRuntime::new(db.clone(), tokio::runtime::Handle::current()).unwrap();
    let routes = Arc::new(crate::torrents::sessions::NetworkRoutes::new(
        db.clone(),
        Arc::clone(&proxies),
        Duration::from_secs(30),
    ));
    let service = service_over(&db, dir.path(), routes, job_env(), set_clock());
    service.open().await.unwrap();
    let job_id = service
        .add(request(AddSource::Metainfo(plain("set", 1))))
        .await
        .unwrap();
    service.tick().await;

    let refusal = crate::torrents::sessions::TorrentRouteReason::RouteBroken(
        crate::proxies::TorrentRouteFault::Legs,
    );
    assert_eq!(
        service.inner.live.lock().await[&job_id].held,
        Some(Held::RouteDown(
            crate::torrents::sessions::TorrentRouteReason::RouteBroken(
                crate::proxies::TorrentRouteFault::Legs
            )
            .into()
        ))
    );
    let view = service.torrent(job_id).await.unwrap();
    assert_eq!(
        (view.detail.as_deref(), view.route_reason),
        (None, Some(refusal))
    );
    let listed = service.routes().await;
    let held = listed
        .iter()
        .find(|route| {
            route
                .scopes
                .iter()
                .any(|(scope, _)| *scope == TorrentScope::Default)
        })
        .unwrap();
    assert_eq!(
        held.down.as_ref().map(|failure| failure.reason),
        Some(refusal)
    );
    assert_eq!(
        held.session
            .as_ref()
            .unwrap()
            .held
            .as_ref()
            .map(|failure| failure.reason),
        Some(refusal)
    );

    db.save_proxy_routing_policy(
        consumer,
        &RoutingPolicy {
            legs: vec![through(100)],
            ..Default::default()
        },
    )
    .unwrap();
    proxies.network.reload().await.unwrap();
    service.tick().await;

    let live = service.inner.live.lock().await;
    assert_eq!(live[&job_id].held, None);
    assert!(live[&job_id].attached.is_some());
    let key = live[&job_id].route.clone().unwrap();
    drop(live);
    let listed = service.routes().await;
    let route = listed.iter().find(|route| route.key == key).unwrap();
    assert_eq!(route.down, None);
    assert_eq!(route.session.as_ref().unwrap().held, None);
    service.stop().await;
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
    before.cancellation_token().cancelled().await;
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

// A listen port given while the tick runs reopens the route there at once:
// the settings change wakes the tick, and the paused clock never reaches the
// tick's next turn.
#[tokio::test(start_paused = true)]
async fn a_new_listen_port_reaches_the_route_without_waiting_for_the_tick() {
    let clock = Arc::new(RuntimeClock(tokio::time::Instant::now()));
    let rig = rig(Arc::clone(&clock) as Arc<dyn Clock>);
    unbound_leg(&rig.routes);
    let mut ticks = rig.service.ticks();
    rig.service.start().await.unwrap();
    let job_id = rig
        .service
        .add(AddRequest {
            paused: true,
            ..request(AddSource::Metainfo(plain("set", 1)))
        })
        .await
        .unwrap();
    // The tick's first turn, at its start, is behind it.
    ticks.wait_for(|ticks| *ticks > 0).await.unwrap();
    let before = session_of(&rig.service, job_id).await;
    assert_eq!(before.listen_addr().map(|listen| listen.port()), Some(6881));

    let changed = tokio::time::Instant::now();
    let base = *ticks.borrow_and_update();
    let mut settings = rig.service.settings();
    settings.torrent.base_port = 41_000;
    rig.service.set_settings(settings).await;
    ticks.wait_for(|ticks| *ticks > base).await.unwrap();
    assert!(tokio::time::Instant::now() < changed + TICK);
    let after = session_of(&rig.service, job_id).await;
    assert!(!Arc::ptr_eq(&before, &after));
    assert_eq!(
        after.listen_addr().map(|listen| listen.port()),
        Some(41_000)
    );
    rig.service.stop().await;
}

// A leg revoked under a torrent's session, as saving its proxy's profile
// again revokes it, wakes the tick: the torrent moves to a session opened on
// the leg the network gives its route now, and the revoked one stops.
#[tokio::test(start_paused = true)]
async fn a_torrent_whose_leg_is_revoked_moves_to_a_session_on_the_new_leg() {
    let clock = Arc::new(RuntimeClock(tokio::time::Instant::now()));
    let rig = rig(Arc::clone(&clock) as Arc<dyn Clock>);
    let (db, network) = on_network(&rig.routes);
    let mut ticks = rig.service.ticks();
    rig.service.start().await.unwrap();
    let job_id = rig
        .service
        .add(AddRequest {
            paused: true,
            ..request(AddSource::Metainfo(plain("set", 1)))
        })
        .await
        .unwrap();
    ticks.wait_for(|ticks| *ticks > 0).await.unwrap();
    let before = session_of(&rig.service, job_id).await;
    let stage = || {
        network
            .route(
                crate::proxies::Consumer::Torrent(TorrentScope::Default),
                8,
                std::time::Duration::from_secs(30),
            )
            .unwrap()
            .legs
            .read()
            .unwrap()[0]
            .stage
            .clone()
    };
    let old_stage = stage();

    save_again(&db, &network).await;
    assert!(old_stage.is_revoked());
    // The torrent's attachment names another session once the tick that the
    // revocation woke has moved it.
    let attached = || async {
        let live = rig.service.inner.live.lock().await;
        live[&job_id]
            .attached
            .as_ref()
            .map(|attached| Arc::clone(&attached.session))
    };
    loop {
        ticks.borrow_and_update();
        if attached()
            .await
            .is_some_and(|session| !Arc::ptr_eq(&session, &before))
        {
            break;
        }
        ticks.changed().await.unwrap();
    }
    let after = session_of(&rig.service, job_id).await;
    before.cancellation_token().cancelled().await;
    assert!(!stage().is_revoked());
    let views = rig.service.inner.sessions.views().await;
    assert_eq!(views.len(), 1);
    assert_eq!(views[0].held, None);
    assert!(
        after
            .get(TorrentIdOrHash::Id(
                handle_of(&rig.service, job_id).await.id()
            ))
            .is_some()
    );
    rig.service.stop().await;
    network.shutdown().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_torrent_past_the_session_limit_says_so() {
    use crate::torrents::sessions::MAX_SESSIONS;

    let rig = rig(set_clock());
    rig.service.open().await.unwrap();
    for id in 0..MAX_SESSIONS as u32 {
        rig.routes.route(TorrentScope::Category(100 + id), 100 + id);
        let plan = rig.routes.plan(TorrentScope::Category(100 + id)).unwrap();
        rig.service
            .inner
            .sessions
            .session(&plan, &crate::torrents::identity::IDENTITY.identity())
            .await
            .unwrap();
    }
    let job_id = rig
        .service
        .add(request(AddSource::Metainfo(plain("set", 1))))
        .await
        .unwrap();
    let view = rig.service.torrent(job_id).await.unwrap();
    assert_eq!(view.state, TorrentState::Stalled);
    assert_eq!(
        (view.detail.as_deref(), view.route_reason),
        (None, Some(TorrentRouteReason::SessionLimit))
    );
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
        (view.detail.as_deref(), view.route_reason),
        (None, Some(TorrentRouteReason::LegOpenFailed))
    );
}

// The list of torrents carries each torrent's trackers, the same rows its
// own view does, and no other torrent's.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_list_carries_each_torrents_trackers() {
    let rig = rig(set_clock());
    rig.service.open().await.unwrap();
    let add = |name: &'static str, seed| {
        rig.service.add(AddRequest {
            paused: true,
            ..request(AddSource::Metainfo(plain(name, seed)))
        })
    };
    let tracked = add("tracked", 1).await.unwrap();
    let bare = add("bare", 2).await.unwrap();
    rig.service
        .add_trackers(
            tracked,
            vec![
                vec!["http://127.0.0.1:1/announce".to_owned()],
                vec!["http://127.0.0.1:2/announce".to_owned()],
            ],
        )
        .await
        .unwrap();

    let own = rig.service.torrent(tracked).await.unwrap().trackers;
    assert_eq!(
        own.iter()
            .map(|tracker| (tracker.tier, tracker.url.as_str()))
            .collect::<Vec<_>>(),
        [
            (0, "http://127.0.0.1:1/announce"),
            (1, "http://127.0.0.1:2/announce")
        ]
    );
    let listed: Vec<(JobId, Vec<TorrentTracker>)> = rig
        .service
        .torrents(None)
        .await
        .into_iter()
        .map(|view| (view.job_id, view.trackers))
        .collect();
    assert_eq!(listed, [(tracked, own), (bare, Vec::new())]);
    assert_eq!(
        rig.service.torrent(bare).await.unwrap().trackers,
        Vec::new()
    );
    rig.service.stop().await;
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

// The tick writes a ledger whenever it changed, as it writes the
// checkpoint, and nothing while it stands still.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_ledger_is_written_at_each_tick_it_changed() {
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
    assert_eq!(written.borrow().ledgers, base + 1);
    assert_eq!(
        rig.db
            .get_torrent_counters(job_id)
            .unwrap()
            .unwrap()
            .uploaded_bytes,
        5
    );
    clock.0.fetch_add(TICK.as_millis() as i64, Ordering::SeqCst);
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

// After a restart, before the tick restores its job, a torrent unpacking
// refused reads as failed, as its history keeps it, and a removal then
// goes to history as failed, as it would once the job is back.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_refused_torrent_reads_and_is_removed_as_failed_before_its_job_is_back() {
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
        let _ = storage.on_piece_completed(index);
    }
    retired.wait_for(|retired| *retired >= 1).await.unwrap();
    rig.service.stop().await;
    rig.db.prune_orphan_active_state().unwrap();

    let again = service_on(&rig.db, rig.dir.path(), &rig.routes, job_env(), set_clock());
    again.open().await.unwrap();
    let view = again.torrent(job_id).await.unwrap();
    assert_eq!(view.state, TorrentState::Stopped);
    assert!(view.detail.unwrap().contains("txt"));

    again.remove(job_id, false, false).await.unwrap();
    let history = rig.db.get_job_history(job_id.0).unwrap().unwrap();
    assert_eq!(history.status, "failed");
    assert!(history.error_message.unwrap().contains("txt"));
    again.stop().await;
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

// `job_id` given `url` as a tracker the operator names, in place of the
// torrent's own listing of it. A tracker on loopback is asked only when the
// operator named it, never when the torrent did.
async fn tracked(service: &TorrentService, job_id: JobId, url: &str) {
    service
        .remove_trackers(job_id, &[url.to_owned()])
        .await
        .unwrap();
    service
        .add_trackers(job_id, vec![vec![url.to_owned()]])
        .await
        .unwrap();
}

// The magnet of `hash` fetching its metainfo from the tracker at
// 127.0.0.1:`port`, which the operator names: added paused, its tracker
// named again, then resumed.
async fn fetching_from(service: &TorrentService, hash: &str, port: u16) -> JobId {
    let job_id = service
        .add(AddRequest {
            paused: true,
            ..request(AddSource::Magnet(format!(
                "magnet:?xt=urn:btih:{hash}&tr=http%3A%2F%2F127.0.0.1%3A{port}%2Fannounce"
            )))
        })
        .await
        .unwrap();
    tracked(
        service,
        job_id,
        &format!("http://127.0.0.1:{port}/announce"),
    )
    .await;
    service.resume(job_id).await.unwrap();
    job_id
}

// A session on loopback seeding `metainfo` from `files` under `dir`, with
// no DHT, trackers or discovery.
async fn seeder(dir: &std::path::Path, metainfo: &[u8], files: &[(&str, &[u8])]) -> Arc<Session> {
    let (session, handle) = sharing(dir, metainfo, files).await;
    handle.wait_until_completed().await.unwrap();
    session
}

// A session on loopback running `metainfo` with `files` under `dir` in
// hand and wanting the rest, with no DHT, trackers or discovery.
async fn sharing(
    dir: &std::path::Path,
    metainfo: &[u8],
    files: &[(&str, &[u8])],
) -> (Arc<Session>, Arc<ManagedTorrent>) {
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
    (session, handle)
}

// A queue whose network is `proxies`', published to `service`: the
// torrents it runs count on their legs of that network's routes.
fn counted_on(service: &TorrentService, proxies: &Arc<crate::proxies::ProxyRuntime>) {
    let queue = queue();
    queue.set_proxy_runtime(Arc::clone(proxies));
    service.publish_to(queue);
}

// The default torrent leg as a network snapshot reads it now: whether it
// is live, and its open and opening connections.
fn default_torrent_leg(network: &NetworkRuntime) -> (bool, u64, u64) {
    let snapshot = crate::proxies::network_metrics::snapshot(network);
    let leg = snapshot
        .legs
        .iter()
        .find(|leg| {
            leg.consumer == Consumer::Torrent(TorrentScope::Default).key()
                && leg.position == crate::torrents::sessions::TORRENT_LEG
        })
        .expect("the default torrent leg");
    (leg.live, leg.open, leg.opening)
}

// Two files of whole pieces: a sharer holding only the first and the rig's
// torrent connect, and neither has anything of the second, so neither only
// uploads and the connection stays up. Returns the sharer's session and the
// rig's torrent, attached on the default leg of `proxies`' network and told
// of the sharer by a loopback tracker.
async fn connected_to_a_sharer(
    rig: &Rig,
    proxies: &Arc<crate::proxies::ProxyRuntime>,
    seeds: &std::path::Path,
) -> (JobId, Arc<Session>, Arc<ManagedTorrent>) {
    let (first, second) = (vec![1u8; 2048], vec![2u8; 2048]);
    let files = [
        ("pair/a.bin", first.as_slice()),
        ("pair/b.bin", second.as_slice()),
    ];
    let bytes = metainfo("pair", &files, 1024, false);
    let (session, _) = sharing(seeds, &bytes, &files[..1]).await;
    let std::net::SocketAddr::V4(peer) = session.listen_addr().unwrap() else {
        panic!("the sharer listens on IPv4 loopback");
    };
    let url = tracker(peer).await;
    on_loopback(&rig.routes);
    counted_on(&rig.service, proxies);
    rig.service.open().await.unwrap();
    let job_id = rig
        .service
        .add(request(AddSource::Metainfo(bytes)))
        .await
        .unwrap();
    tracked(&rig.service, job_id, &url).await;
    let torrent = handle_of(&rig.service, job_id).await;
    (job_id, session, torrent)
}

// Until `torrent`'s run holds an established peer, read from its gauge
// after each turn the other workers run the sessions in; returns how many
// it holds. The engine publishes no wake for a peer becoming established,
// and adding one would put a lock on that path.
async fn established(torrent: &ManagedTorrent) -> u32 {
    loop {
        let held = torrent.gauges().map_or(0, |run| run.peers().live);
        if held > 0 {
            return held;
        }
        tokio::task::yield_now().await;
    }
}

// A torrent's leg shows the peers the torrent holds the instant a network
// snapshot is taken: no tick ran since the peer was established.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_torrent_leg_shows_an_established_peer_the_instant_it_is_read() {
    let rig = rig(set_clock());
    let proxies =
        crate::proxies::ProxyRuntime::new(rig.db.clone(), tokio::runtime::Handle::current())
            .unwrap();
    let seeds = tempfile::tempdir().unwrap();
    assert_eq!(default_torrent_leg(&proxies.network), (false, 0, 0));
    let (_, sharer, torrent) = connected_to_a_sharer(&rig, &proxies, seeds.path()).await;

    let held = established(&torrent).await;
    assert_eq!(
        default_torrent_leg(&proxies.network),
        (true, u64::from(held), 0)
    );
    rig.service.stop().await;
    sharer.stop().await;
    proxies.network.shutdown().await;
}

// A removed torrent leaves nothing on its leg: the next snapshot reads the
// leg empty, though the tick before the removal saw the torrent's peer.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_removed_torrent_leaves_its_leg_empty_at_the_next_snapshot() {
    let rig = rig(set_clock());
    let proxies =
        crate::proxies::ProxyRuntime::new(rig.db.clone(), tokio::runtime::Handle::current())
            .unwrap();
    let seeds = tempfile::tempdir().unwrap();
    let (job_id, sharer, torrent) = connected_to_a_sharer(&rig, &proxies, seeds.path()).await;
    established(&torrent).await;
    rig.service.tick().await;

    rig.service.remove(job_id, false, false).await.unwrap();
    assert_eq!(default_torrent_leg(&proxies.network), (false, 0, 0));
    rig.service.stop().await;
    sharer.stop().await;
    proxies.network.shutdown().await;
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
    let (bytes, url) = (plain.to_vec(), tracker(peer).await);

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
    tracked(&rig.service, job_id, &url).await;

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
    assert_eq!(
        (view.detail.as_deref(), view.route_reason),
        (None, Some(TorrentRouteReason::TorrentsDisabled))
    );
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
    assert_eq!(view.route_reason, None);
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

// However often a job is marked between ticks with no piece reaching it,
// its checkpoint is written once.
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

// A downloading torrent's checkpoint follows its pieces with no tick: it is
// written each time [`CHECKPOINT_PIECES`] more pieces reach the job, so the
// checkpoint on disk is never that many pieces behind what verified.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_checkpoint_follows_the_verified_pieces_between_ticks() {
    const PIECE: usize = 1024;
    let rig = rig(set_clock());
    rig.routes.set_down(Some("down"));
    rig.service.open().await.unwrap();
    let pieces = 3 * CHECKPOINT_PIECES + CHECKPOINT_PIECES / 2;
    let bytes: Vec<u8> = (0..pieces as usize * PIECE)
        .map(|at| (at % 251) as u8)
        .collect();
    let job_id = rig
        .service
        .add(request(AddSource::Metainfo(metainfo(
            "set",
            &[("set/a.bin", &bytes)],
            PIECE,
            false,
        ))))
        .await
        .unwrap();
    let (intake, layout) = {
        let live = rig.service.inner.live.lock().await;
        let torrent = &live[&job_id];
        (
            torrent.intake.clone().unwrap(),
            torrent.layout.clone().unwrap(),
        )
    };
    let on_disk = || {
        rig.service
            .kept_root(job_id, &layout)
            .map_or(0, |root| root.have().count_ones() as u32)
    };
    let mut written = rig.service.written();
    let base = written.borrow().checkpoints;
    for piece in 0..pieces {
        let range = layout.piece_range(piece);
        let shares = layout
            .spans(piece)
            .into_iter()
            .filter_map(|span| {
                let at = (range.start + span.at) as usize;
                Some(crate::pipeline::graph::root::pieces::Verified {
                    volume: span.volume?,
                    piece,
                    offset: span.range.start,
                    bytes: bytes::Bytes::copy_from_slice(
                        &bytes[at..at + span.range.len() as usize],
                    ),
                })
            })
            .collect();
        intake.verified(shares).unwrap();
        let verified = piece + 1;
        if verified % CHECKPOINT_PIECES == 0 {
            let saves = base + u64::from(verified / CHECKPOINT_PIECES);
            written
                .wait_for(|written| written.checkpoints >= saves)
                .await
                .unwrap();
            assert_eq!(on_disk(), verified);
        }
        assert!(verified - on_disk() < CHECKPOINT_PIECES, "piece {piece}");
    }
    rig.service.stop().await;
}

// The checkpoint written as pieces reach the job, between ticks, carries the
// ledger the service holds as it is written: a crash recovers the bytes and
// the active seconds of the moment its pieces were taken, not those of the
// last tick.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_checkpoint_between_ticks_carries_the_ledger_with_its_pieces() {
    const PIECE: usize = 1024;
    let rig = rig(set_clock());
    rig.routes.set_down(Some("down"));
    rig.service.open().await.unwrap();
    // Twice what one write takes, so the job is not complete, and its
    // completion's own write of the ledger cannot stand in for this one.
    let pieces = CHECKPOINT_PIECES;
    let bytes: Vec<u8> = (0..2 * pieces as usize * PIECE)
        .map(|at| (at % 251) as u8)
        .collect();
    let job_id = rig
        .service
        .add(request(AddSource::Metainfo(metainfo(
            "set",
            &[("set/a.bin", &bytes)],
            PIECE,
            false,
        ))))
        .await
        .unwrap();
    let (intake, layout) = {
        let mut live = rig.service.inner.live.lock().await;
        let torrent = live.get_mut(&job_id).unwrap();
        // What a session moved for it since the last tick.
        torrent.counters.downloaded_bytes = 1_805_238_272;
        torrent.counters.uploaded_bytes = 7;
        torrent.counters.active_duration_s = 24;
        (
            torrent.intake.clone().unwrap(),
            torrent.layout.clone().unwrap(),
        )
    };
    let mut written = rig.service.written();
    let base = written.borrow().checkpoints;
    for piece in 0..pieces {
        let range = layout.piece_range(piece);
        let shares = layout
            .spans(piece)
            .into_iter()
            .filter_map(|span| {
                let at = (range.start + span.at) as usize;
                Some(crate::pipeline::graph::root::pieces::Verified {
                    volume: span.volume?,
                    piece,
                    offset: span.range.start,
                    bytes: bytes::Bytes::copy_from_slice(
                        &bytes[at..at + span.range.len() as usize],
                    ),
                })
            })
            .collect();
        intake.verified(shares).unwrap();
    }
    written
        .wait_for(|written| written.checkpoints > base)
        .await
        .unwrap();
    let held = rig.service.inner.live.lock().await[&job_id].counters;
    let on_disk = rig.db.get_torrent_counters(job_id).unwrap().unwrap();
    assert_eq!(
        (
            on_disk.downloaded_bytes,
            on_disk.uploaded_bytes,
            on_disk.active_duration_s
        ),
        (
            held.downloaded_bytes,
            held.uploaded_bytes,
            held.active_duration_s
        )
    );
    assert_eq!(on_disk.downloaded_bytes, 1_805_238_272);
    assert_eq!(on_disk.active_duration_s, 24);
    let root = rig.service.kept_root(job_id, &layout).unwrap();
    assert_eq!(root.have().count_ones() as u32, pieces);
    rig.service.stop().await;
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
    let (bytes, url) = (private.to_vec(), tracker(peer).await);

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
    tracked(&rig.service, job_id, &url).await;
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
    // the torrent ran in its session is active, and it ran complete. The
    // tick writes the ledger it changed, and a clean stop writes the ledger
    // as it stands.
    rig.service.tick().await;
    clock.0.fetch_add(30_000, Ordering::SeqCst);
    rig.service.tick().await;
    let kept = rig.db.get_torrent_counters(job_id).unwrap().unwrap();
    assert_eq!(kept.active_duration_s, 30);
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
    again.open().await.unwrap();
    // Before the tick restores its job, it shows what was stored: complete,
    // so its file whole and every piece.
    let kept = again.torrent(job_id).await.unwrap();
    assert_eq!(
        (kept.have_bytes, kept.progress, kept.file_have),
        (total, 1.0, vec![total])
    );
    assert_eq!(kept.pieces.map(|pieces| pieces.have), Some(4));
    again.tick().await;
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

// A torrent fetched whole from a seed on loopback and seeding, on a clock
// the test sets.
struct Seeded {
    rig: Rig,
    clock: Arc<SetClock>,
    job_id: JobId,
    seeder: Arc<Session>,
    _seeds: tempfile::TempDir,
}

async fn seeded() -> Seeded {
    let data: Vec<u8> = (0..65_536u32).map(|at| (at % 251) as u8).collect();
    let files = [("set/a.bin", data.as_slice())];
    let private = metainfo("set", &files, 16_384, true);
    let seeds = tempfile::tempdir().unwrap();
    let seeder = seeder(seeds.path(), &private, &files).await;
    let std::net::SocketAddr::V4(peer) = seeder.listen_addr().unwrap() else {
        panic!("the seeder listens on IPv4 loopback");
    };
    let (bytes, url) = (private.to_vec(), tracker(peer).await);

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
    tracked(&rig.service, job_id, &url).await;
    handle_of(&rig.service, job_id)
        .await
        .wait_until_completed()
        .await
        .unwrap();
    seeding_view(&rig.service, &mut changes, job_id).await;
    Seeded {
        rig,
        clock,
        job_id,
        seeder,
        _seeds: seeds,
    }
}

impl Seeded {
    fn advance(&self, ms: i64) {
        self.clock.0.fetch_add(ms, Ordering::SeqCst);
    }

    async fn view(&self) -> TorrentView {
        self.rig.service.torrent(self.job_id).await.unwrap()
    }

    fn kept(&self) -> TorrentCounters {
        self.rig
            .db
            .get_torrent_counters(self.job_id)
            .unwrap()
            .unwrap()
    }

    async fn stop(self) {
        self.rig.service.stop().await;
        self.seeder.stop().await;
    }
}

// A torrent's averages run over the seconds it reads as active, a second
// begun counting whole: its first second reads its bytes as its rate, not
// 0, and past it the bytes over the seconds is the average.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_torrents_first_second_reads_a_rate() {
    let seeded = seeded().await;
    seeded.advance(500);
    let view = seeded.view().await;
    assert!(view.downloaded_bytes > 0);
    assert_eq!(
        (view.active_seconds, view.download_avg_bps),
        (1, view.downloaded_bytes)
    );
    seeded.advance(3_443);
    let view = seeded.view().await;
    assert_eq!(
        (view.active_seconds, view.download_avg_bps),
        (4, view.downloaded_bytes / 4)
    );
    seeded.stop().await;
}

// A torrent's active time is read as of now, with its bytes: a view between
// ticks counts the time since the last one, its averages are its bytes
// over that time, and the tick lands the same figures in the ledger with no
// step. Seeding time runs with it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_torrents_active_time_reads_as_of_now_between_ticks() {
    let seeded = seeded().await;
    seeded.advance(1_000);
    seeded.rig.service.tick().await;
    let at_tick = seeded.view().await;
    let kept = seeded.kept();
    assert_eq!(at_tick.active_seconds, 1);
    assert_eq!(
        (
            kept.active_duration_s,
            kept.finished_duration_s,
            kept.downloaded_bytes,
        ),
        (
            at_tick.active_seconds,
            at_tick.active_seconds,
            at_tick.downloaded_bytes,
        )
    );

    seeded.advance(7_000);
    let later = seeded.view().await;
    assert_eq!(later.active_seconds, at_tick.active_seconds + 7);
    assert_eq!(
        (later.download_avg_bps, later.upload_avg_bps),
        (
            later.downloaded_bytes / later.active_seconds,
            later.uploaded_bytes / later.active_seconds,
        )
    );

    seeded.advance(3_000);
    let before = seeded.view().await;
    seeded.rig.service.tick().await;
    let after = seeded.view().await;
    assert_eq!(before.active_seconds, at_tick.active_seconds + 10);
    assert_eq!(
        (after.active_seconds, after.download_avg_bps),
        (before.active_seconds, before.download_avg_bps)
    );
    let kept = seeded.kept();
    assert_eq!(
        (kept.active_duration_s, kept.finished_duration_s),
        (after.active_seconds, after.active_seconds)
    );
    seeded.stop().await;
}

// A pause takes the torrent's reading first: the time it ran since the
// last tick is active and seeding, and the time it stays paused is
// neither, between ticks or across one.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_pause_credits_the_time_before_it_and_nothing_after() {
    let seeded = seeded().await;
    seeded.advance(1_000);
    seeded.rig.service.tick().await;
    let at_tick = seeded.view().await;
    seeded.advance(7_000);
    seeded.rig.service.pause(seeded.job_id).await.unwrap();
    let paused = seeded.view().await;
    assert_eq!(paused.state, TorrentState::Paused);
    assert_eq!(paused.active_seconds, at_tick.active_seconds + 7);
    let kept = seeded.kept();
    assert_eq!(
        (kept.active_duration_s, kept.finished_duration_s),
        (paused.active_seconds, paused.active_seconds)
    );
    seeded.advance(7_000);
    assert_eq!(seeded.view().await.active_seconds, paused.active_seconds);
    seeded.advance(3_000);
    seeded.rig.service.tick().await;
    assert_eq!(seeded.view().await.active_seconds, paused.active_seconds);
    let kept = seeded.kept();
    assert_eq!(
        (kept.active_duration_s, kept.finished_duration_s),
        (paused.active_seconds, paused.active_seconds)
    );
    seeded.stop().await;
}

// A torrent that wants none of its files has every piece it wants: it seeds
// at 100 % with nothing in hand and no peers, though its job is not
// complete, and its seeding time runs. Wanting the file again takes it back
// to fetching, and its seeding time stops.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_torrent_that_wants_nothing_seeds_until_it_wants_a_file() {
    use FilePriority::*;
    let clock = set_clock();
    let rig = rig(Arc::clone(&clock) as Arc<dyn Clock>);
    rig.service.open().await.unwrap();
    let job_id = rig
        .service
        .add(AddRequest {
            paused: true,
            ..request(AddSource::Metainfo(plain("set", 1)))
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
    let view = rig.service.torrent(job_id).await.unwrap();
    assert_eq!(
        (
            view.state,
            view.progress,
            view.have_bytes,
            view.total_bytes,
            view.peers,
            view.completed_at_ms,
        ),
        (TorrentState::Seeding, 1.0, 0, 3000, 0, None)
    );

    rig.service.tick().await;
    clock.0.fetch_add(5_000, Ordering::SeqCst);
    rig.service.tick().await;
    let kept = rig.db.get_torrent_counters(job_id).unwrap().unwrap();
    assert_eq!((kept.active_duration_s, kept.finished_duration_s), (5, 5));

    rig.service
        .set_file_priorities(job_id, &[(0, Normal)])
        .await
        .unwrap();
    let view = rig.service.torrent(job_id).await.unwrap();
    assert_eq!(
        (view.state, view.progress, view.completed_at_ms),
        (TorrentState::Stalled, 0.0, None)
    );
    clock.0.fetch_add(5_000, Ordering::SeqCst);
    rig.service.tick().await;
    let kept = rig.db.get_torrent_counters(job_id).unwrap().unwrap();
    assert_eq!((kept.active_duration_s, kept.finished_duration_s), (10, 5));
    rig.service.stop().await;
}

// Turning torrents off reads every torrent before its session closes: the
// time it ran since the last tick is in the ledger.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn turning_torrents_off_keeps_the_time_they_ran_since_the_tick() {
    let seeded = seeded().await;
    seeded.advance(1_000);
    seeded.rig.service.tick().await;
    let at_tick = seeded.view().await;
    seeded.advance(7_000);
    let mut settings = seeded.rig.service.settings();
    settings.torrent.enabled = false;
    seeded.rig.service.set_settings(settings).await;
    let kept = seeded.kept();
    assert_eq!(
        (
            kept.active_duration_s,
            kept.finished_duration_s,
            kept.downloaded_bytes,
        ),
        (
            at_tick.active_seconds + 7,
            at_tick.active_seconds + 7,
            at_tick.downloaded_bytes,
        )
    );
    let off = seeded.view().await;
    assert_eq!(off.active_seconds, at_tick.active_seconds + 7);
    seeded.advance(7_000);
    assert_eq!(seeded.view().await.active_seconds, off.active_seconds);
    seeded.stop().await;
}

// A clock stepped back credits nothing for the reading that sees it, and
// the time from there on counts again.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_clock_stepped_back_credits_nothing_and_counts_on() {
    let seeded = seeded().await;
    seeded.advance(1_000);
    seeded.rig.service.tick().await;
    let at_tick = seeded.view().await.active_seconds;
    seeded.advance(-60_000);
    seeded.rig.service.tick().await;
    assert_eq!(seeded.kept().active_duration_s, at_tick);
    seeded.advance(7_000);
    assert_eq!(seeded.view().await.active_seconds, at_tick + 7);
    seeded.rig.service.tick().await;
    assert_eq!(seeded.kept().active_duration_s, at_tick + 7);
    seeded.stop().await;
}

// No part of a second is lost at a tick: ticks 2.4 s, 2.4 s, 1.6 s and
// 1.6 s apart add the 8 s they span.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ticks_lose_no_part_of_a_second() {
    let seeded = seeded().await;
    seeded.rig.service.tick().await;
    let base = seeded.view().await.active_seconds;
    for ms in [2_400, 2_400] {
        seeded.advance(ms);
        seeded.rig.service.tick().await;
    }
    assert_eq!(seeded.kept().active_duration_s, base + 4);
    for ms in [1_600, 1_600] {
        seeded.advance(ms);
        seeded.rig.service.tick().await;
    }
    assert_eq!(seeded.kept().active_duration_s, base + 8);
    assert_eq!(seeded.view().await.active_seconds, base + 8);
    seeded.stop().await;
}

// Readings 2.4 s and 2.4 s apart add 4 s, the 4.8 s rounded down once;
// 1.6 s and 1.6 s add 3. A duration the torrent was not in takes nothing.
#[test]
fn a_reading_carries_the_part_of_a_second_to_the_next() {
    let run = |steps: &[i64]| {
        let mut counters = TorrentCounters::default();
        let mut timed = Timed::at(EPOCH_MS);
        let mut now = EPOCH_MS;
        for step in steps {
            now += step;
            timed = timed.advanced(now, true, false, &mut counters);
        }
        (counters.active_duration_s, counters.finished_duration_s)
    };
    assert_eq!(run(&[2_400, 2_400]), (4, 0));
    assert_eq!(run(&[1_600, 1_600]), (3, 0));
    assert_eq!(run(&[999]), (0, 0));
    assert_eq!(run(&[999, 1]), (1, 0));
    // Stepped back 60 s: nothing for that reading, then 7 s from there.
    assert_eq!(run(&[1_000, -60_000]), (1, 0));
    assert_eq!(run(&[1_000, -60_000, 7_000]), (8, 0));
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
    let (bytes, url) = (private.to_vec(), tracker(peer).await);

    let rig = rig(set_clock());
    on_loopback(&rig.routes);
    rig.service.open().await.unwrap();
    let job_id = rig
        .service
        .add(request(AddSource::Metainfo(bytes)))
        .await
        .unwrap();
    tracked(&rig.service, job_id, &url).await;
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
    let (bytes, url) = (private.to_vec(), tracker(peer).await);

    let rig = rig(set_clock());
    on_loopback(&rig.routes);
    rig.service.open().await.unwrap();
    let job_id = rig
        .service
        .add(request(AddSource::Metainfo(bytes)))
        .await
        .unwrap();
    tracked(&rig.service, job_id, &url).await;
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
    let (bytes, url) = (private.to_vec(), tracker(peer).await);

    let rig = rig(set_clock());
    on_loopback(&rig.routes);
    rig.service.open().await.unwrap();
    let job_id = rig
        .service
        .add(request(AddSource::Metainfo(bytes)))
        .await
        .unwrap();
    tracked(&rig.service, job_id, &url).await;
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
    heard_request(listener).await.0
}

// The next announce the tracker on `listener` hears, unanswered, and its
// request.
async fn heard_request(listener: &tokio::net::TcpListener) -> (tokio::net::TcpStream, String) {
    use tokio::io::AsyncReadExt;

    let (mut stream, _) = listener.accept().await.unwrap();
    let mut request = Vec::new();
    let mut buf = [0u8; 1024];
    while !request.windows(4).any(|end| end == b"\r\n\r\n") {
        let read = stream.read(&mut buf).await.unwrap();
        assert!(read > 0, "the announce ended before its request was read");
        request.extend_from_slice(&buf[..read]);
    }
    (stream, String::from_utf8_lossy(&request).into_owned())
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
// announce waits, working with its counts once it answers and once its
// `stopped` is answered as the torrent pauses, and not working once it
// stops answering.
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
        .add(request(AddSource::Metainfo(plain("set", 1))))
        .await
        .unwrap();
    tracked(&rig.service, job_id, &url).await;

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

    let answer = |complete: u32| {
        let body = format!("d8:completei{complete}e10:incompletei5e8:intervali1800e5:peers0:e");
        format!(
            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        )
    };
    started.write_all(answer(3).as_bytes()).await.unwrap();
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

    // Paused, the stopped announce is the row's last outcome: nothing is
    // due until the torrent runs again, and no floor stands for a next
    // announce.
    rig.service.pause(job_id).await.unwrap();
    let mut stopped = heard_announce(&listener).await;
    stopped.write_all(answer(7).as_bytes()).await.unwrap();
    let trackers = trackers_once(&rig.service, job_id, |announces| {
        !announces[0].in_flight
            && announces[0]
                .last
                .as_ref()
                .and_then(|last| last.result.as_ref().ok())
                .is_some_and(|accepted| accepted.seeders == Some(7))
    })
    .await;
    let paused = &trackers[0];
    assert_eq!(paused.status, TrackerStatus::Working);
    assert!(!paused.updating);
    assert_eq!(
        (
            paused.next_announce_ms,
            paused.min_announce_ms,
            paused.fails
        ),
        (None, None, 0)
    );
    assert_eq!((paused.seeders, paused.peers), (Some(7), Some(0)));
    assert!(paused.last_announce_ms >= Some(at));

    // The tracker is gone: the next run's first announce finds no tracker.
    drop(listener);
    rig.service.resume(job_id).await.unwrap();
    let trackers = trackers_once(&rig.service, job_id, |announces| {
        !announces[0].in_flight
            && announces[0]
                .last
                .as_ref()
                .is_some_and(|last| last.result.is_err())
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
        .add(request(AddSource::Metainfo(plain("pushed", 2))))
        .await
        .unwrap();
    tracked(&rig.service, job_id, &url).await;
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

// A magnet's trackers are announced to while its metainfo is fetched, and
// its rows say so: updating while the announce waits, working once the
// tracker answers, with the next announce due at the tracker's interval.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_magnets_trackers_read_as_announced_while_its_metainfo_is_fetched() {
    use tokio::io::AsyncWriteExt;

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let url = format!("http://127.0.0.1:{port}/announce");
    let rig = rig(set_clock());
    on_loopback(&rig.routes);
    rig.service.open().await.unwrap();
    let job_id = fetching_from(&rig.service, &"fe".repeat(20), port).await;
    // A fetch's table is followed like a run's; the beat covers the view
    // changes nothing pushes.
    let mut updates = rig.service.updates(Some(job_id), Duration::from_millis(10));

    let mut started = heard_announce(&listener).await;
    let updating = loop {
        let views = updates.next().await;
        if views[0].trackers[0].updating {
            break views[0].clone();
        }
    };
    assert_eq!(updating.state, TorrentState::FetchingMetadata);
    assert_eq!(updating.trackers[0].url, url);

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
            break views[0].clone();
        }
    };
    assert_eq!(working.state, TorrentState::FetchingMetadata);
    let tracker = &working.trackers[0];
    assert!(!tracker.updating);
    assert_eq!((tracker.seeders, tracker.leechers), (Some(3), Some(5)));
    let at = tracker.last_announce_ms.unwrap();
    assert_eq!(tracker.next_announce_ms, Some(at + 1_800_000));
    // Gone, the tracker refuses the stopped announce at once.
    drop(listener);
    rig.service.stop().await;
}

// The next announce the tracker on `listener` hears, unanswered, and its
// event.
async fn heard_event(
    listener: &tokio::net::TcpListener,
) -> (tokio::net::TcpStream, Option<String>) {
    use tokio::io::AsyncReadExt;

    let (mut stream, _) = listener.accept().await.unwrap();
    let mut request = Vec::new();
    let mut buf = [0u8; 1024];
    while !request.windows(4).any(|end| end == b"\r\n\r\n") {
        let read = stream.read(&mut buf).await.unwrap();
        assert!(read > 0, "the announce ended before its request was read");
        request.extend_from_slice(&buf[..read]);
    }
    let request = String::from_utf8_lossy(&request);
    let path = request.split(' ').nth(1).unwrap();
    let event = path
        .split_once('?')
        .map(|(_, query)| query)
        .unwrap_or_default()
        .split('&')
        .find_map(|kv| kv.strip_prefix("event="))
        .map(str::to_owned);
    (stream, event)
}

// Answers an announce with no peers.
async fn answer_no_peers(mut stream: tokio::net::TcpStream) {
    use tokio::io::AsyncWriteExt;

    let body = b"d8:intervali1800e5:peers0:e";
    let head = format!(
        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    stream
        .write_all(&[head.as_bytes(), body].concat())
        .await
        .unwrap();
}

// A magnet's fetch announces `started`, held unanswered until its row reads
// updating, then answered: the tracker is owed `stopped`.
async fn started_and_answered(
    listener: &tokio::net::TcpListener,
    updates: &mut updates::TorrentUpdates,
) {
    let (stream, event) = heard_event(listener).await;
    assert_eq!(event.as_deref(), Some("started"));
    while !updates.next().await[0].trackers[0].updating {}
    answer_no_peers(stream).await;
    loop {
        let views = updates.next().await;
        let tracker = &views[0].trackers[0];
        if !tracker.updating && tracker.status == TrackerStatus::Working {
            break;
        }
    }
}

// A magnet paused while its metainfo is fetched stops at its tracker, once,
// and fetches again when resumed; removed while fetching, it stops again.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_magnet_paused_or_removed_while_its_metainfo_is_fetched_stops_at_its_tracker() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let rig = rig(set_clock());
    on_loopback(&rig.routes);
    rig.service.open().await.unwrap();
    let job_id = fetching_from(&rig.service, &"fd".repeat(20), port).await;
    let mut updates = rig.service.updates(Some(job_id), Duration::from_millis(10));

    started_and_answered(&listener, &mut updates).await;
    let table = rig.service.inner.live.lock().await[&job_id]
        .fetching
        .clone();

    rig.service.pause(job_id).await.unwrap();
    let (stream, event) = heard_event(&listener).await;
    assert_eq!(event.as_deref(), Some("stopped"));
    answer_no_peers(stream).await;
    // The ended fetch's table, given late, is not the paused magnet's.
    assert!(table.is_some());
    rig.service.fetching(job_id, table).await;
    assert!(
        rig.service.inner.live.lock().await[&job_id]
            .fetching
            .is_none()
    );
    // Paused, the magnet shows no fetch's table, so the rows read from here
    // on are the next fetch's.
    while updates.next().await[0].trackers[0].status != TrackerStatus::NotContacted {}

    rig.service.resume(job_id).await.unwrap();
    started_and_answered(&listener, &mut updates).await;

    rig.service.remove(job_id, false, false).await.unwrap();
    let (stream, event) = heard_event(&listener).await;
    assert_eq!(event.as_deref(), Some("stopped"));
    answer_no_peers(stream).await;
    rig.service.stop().await;
}

// A magnet claims its directory only once its metainfo is in hand: paused
// and then removed while it is fetched, it leaves no directory behind.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_magnet_removed_before_its_metainfo_leaves_no_directory() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let rig = rig(set_clock());
    on_loopback(&rig.routes);
    rig.service.open().await.unwrap();
    let complete = rig.dir.path().join("complete");
    let left = || {
        std::fs::read_dir(&complete)
            .map(|entries| entries.count())
            .unwrap_or(0)
    };
    let job_id = fetching_from(&rig.service, &"fc".repeat(20), port).await;
    let mut updates = rig.service.updates(Some(job_id), Duration::from_millis(10));
    started_and_answered(&listener, &mut updates).await;
    assert_eq!(left(), 0);

    rig.service.pause(job_id).await.unwrap();
    let (stream, event) = heard_event(&listener).await;
    assert_eq!(event.as_deref(), Some("stopped"));
    answer_no_peers(stream).await;
    rig.service.remove(job_id, false, false).await.unwrap();
    assert!(rig.service.torrent(job_id).await.is_none());
    assert_eq!(left(), 0);
    rig.service.stop().await;
}

// A magnet whose route changes while its metainfo is fetched ends that
// fetch, which its old session lets go with `stopped`, and fetches again on
// the new route's session.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_magnet_fetching_when_its_route_changes_fetches_again_on_the_new_route() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let rig = rig(set_clock());
    on_loopback(&rig.routes);
    rig.service.open().await.unwrap();
    let job_id = fetching_from(&rig.service, &"fc".repeat(20), port).await;
    let mut updates = rig.service.updates(Some(job_id), Duration::from_millis(10));
    started_and_answered(&listener, &mut updates).await;
    let old = rig.service.inner.live.lock().await[&job_id]
        .route
        .clone()
        .unwrap();

    rig.routes.route(TorrentScope::Default, 2);
    let new = rig.routes.plan(TorrentScope::Default).unwrap().key;
    assert_ne!(old, new);
    rig.service.follow_routes().await;

    // The old fetch's `stopped` and the new one's `started`, in either order.
    let mut events = Vec::new();
    for _ in 0..2 {
        let (stream, event) = heard_event(&listener).await;
        events.push(event.unwrap());
        answer_no_peers(stream).await;
    }
    events.sort();
    assert_eq!(events, ["started", "stopped"]);
    let live = rig.service.inner.live.lock().await;
    assert_eq!(live[&job_id].route.as_ref(), Some(&new));
    assert!(matches!(
        live[&job_id].resolving,
        Some(Resolving::Fetching(_))
    ));
    drop(live);
    let running = rig.service.inner.sessions.running().await;
    assert!(running.contains_key(&new), "{:?}", running.keys());
    rig.service.stop().await;
}

// A magnet added paused fetches nothing and announces nothing until it is
// resumed; resumed, its fetch announces `started` once, and removed, it
// stops.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_magnet_added_paused_announces_only_once_resumed() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let rig = rig(set_clock());
    on_loopback(&rig.routes);
    rig.service.open().await.unwrap();
    let job_id = rig
        .service
        .add(AddRequest {
            paused: true,
            ..request(AddSource::Magnet(format!(
                "magnet:?xt=urn:btih:{}&tr=http%3A%2F%2F127.0.0.1%3A{port}%2Fannounce",
                "fe".repeat(20)
            )))
        })
        .await
        .unwrap();
    // The add started no fetch: the magnet waits for its resume.
    assert!(matches!(
        rig.service.inner.live.lock().await[&job_id].resolving,
        Some(Resolving::Paused)
    ));
    tracked(
        &rig.service,
        job_id,
        &format!("http://127.0.0.1:{port}/announce"),
    )
    .await;
    let mut updates = rig.service.updates(Some(job_id), Duration::from_millis(10));

    rig.service.resume(job_id).await.unwrap();
    started_and_answered(&listener, &mut updates).await;

    // The next announce the tracker hears is the remove's `stopped`: one
    // fetch, one presence.
    rig.service.remove(job_id, false, false).await.unwrap();
    let (stream, event) = heard_event(&listener).await;
    assert_eq!(event.as_deref(), Some("stopped"));
    answer_no_peers(stream).await;
    rig.service.stop().await;
}

// A torrent under a tracker rule that names a client names itself as that
// client to its peers and trackers; a torrent under no rule names itself as
// Weaver. The route both run on names the client of the torrent that opened
// it, the rule's, though the second torrent names another.
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
                dht: None,
                pex: None,
                lsd: None,
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
    let session = session_of(&rig.service, kept).await;
    assert!(Arc::ptr_eq(
        &session,
        &session_of(&rig.service, plain).await
    ));
    assert_eq!(
        named(session.identity().clone()),
        (
            qbittorrent.prefix().to_vec(),
            qbittorrent.user_agent().to_owned(),
            qbittorrent.version().to_owned()
        )
    );
}

// A route opened by a torrent under no rule names Weaver.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_route_opened_by_a_torrent_under_no_rule_names_weaver() {
    use crate::torrents::identity::IDENTITY;

    let rig = rig(set_clock());
    rig.service.open().await.unwrap();
    let plain = rig
        .service
        .add(request(AddSource::Metainfo(plain("plain", 1))))
        .await
        .unwrap();
    let identity = session_of(&rig.service, plain).await.identity().clone();
    assert_eq!(&identity.peer_id.0[..8], IDENTITY.prefix());
    assert_eq!(identity.user_agent, IDENTITY.user_agent());
    assert_eq!(identity.version, IDENTITY.version());
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
                dht: None,
                pex: None,
                lsd: None,
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
                .add(request(AddSource::Metainfo(plain(
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
        save_path: None,
        content_layout: None,
        files_dir: None,
        seed_mode: false,
        move_to: None,
        queue_position: 0,
    };
    let mut rules = vec![TrackerRule {
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
        dht: None,
        pex: None,
        lsd: None,
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
                dht: None,
                pex: None,
                lsd: None,
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
                dht: None,
                pex: None,
                lsd: None,
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
    rig.service.attach(job_id, None).await;
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
    rig.service.save_checkpoints([job_id].into()).await;
    let whole = first.with(|job| job.pending().tally());
    assert!(whole.rows > 0);
    rig.service.save_checkpoints([job_id].into()).await;
    let again = first.with(|job| job.pending().tally());
    assert_eq!(again.checkpoints, whole.checkpoints + 1);
    assert_eq!(again.rows, whole.rows);
    assert!(rig.db.load_graph_checkpoint(job_id).unwrap().is_some());
    rig.service.stop().await;

    let restarted = service_on(&rig.db, rig.dir.path(), &rig.routes, job_env(), set_clock());
    reopen(&restarted).await;
    let restored = job(&restarted).await;
    restarted.save_checkpoints([job_id].into()).await;
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

// Seeding terms that set nothing are no terms: a torrent given them, on add
// or after, inherits its tracker rule's goal, mode and action instead of
// seeding with no goal under the default ones.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn empty_seeding_terms_inherit_the_chain() {
    let rig = rig(set_clock());
    on_loopback(&rig.routes);
    rig.service.open().await.unwrap();
    let ruled = SeedingPolicy {
        ratio_goal: Some(2.0),
        mode: SeedingGoalMode::All,
        action: SeedingAction::Remove,
        ..SeedingPolicy::default()
    };
    rig.db
        .insert_tracker_rule(
            &TrackerRule {
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
                upload_limit: None,
                forced: false,
                ssl: None,
            },
            EPOCH_MS,
        )
        .unwrap();
    rig.service.reload_tracker_rules().await.unwrap();
    let job_id = rig
        .service
        .add(AddRequest {
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
        assert_eq!(
            view.seeding_source,
            SeedingSource::TrackerRule("*.test".into())
        );
        assert_eq!(view.seeding_policy, ruled);
    };
    inherits(rig.service.torrent(job_id).await.unwrap());
    assert_eq!(
        rig.db.get_torrent(job_id).unwrap().unwrap().policy_override,
        None
    );

    let own = SeedingPolicy {
        seed_time_goal_minutes: Some(60),
        ..SeedingPolicy::default()
    };
    rig.service
        .set_policy_override(job_id, Some(own))
        .await
        .unwrap();
    assert_eq!(
        rig.service.torrent(job_id).await.unwrap().seeding_source,
        SeedingSource::Torrent
    );
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
                dht: Some(false),
                pex: Some(false),
                lsd: Some(false),
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
                dht: None,
                pex: None,
                lsd: None,
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

// Each of a tracker rule's DHT, peer exchange and local discovery switches
// reaches its torrents on its own: one turned off holds them off that one
// source, one unset leaves it on, and a torrent with any source left is not
// held to its trackers. A change reaches the running torrent at the next
// tick, through its session again.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_tracker_rules_dht_pex_and_lsd_each_reach_its_torrents() {
    use weaver_torrent::Discovery;

    let rig = rig(set_clock());
    on_loopback(&rig.routes);
    rig.service.open().await.unwrap();
    let rule = rig
        .db
        .insert_tracker_rule(
            &TrackerRule {
                host_pattern: "*.example-tracker.test".into(),
                dht: Some(false),
                pex: Some(false),
                lsd: None,
                ..TrackerRule::default()
            },
            EPOCH_MS,
        )
        .unwrap();
    rig.service.reload_tracker_rules().await.unwrap();
    let ruled = rig
        .service
        .add(AddRequest {
            paused: true,
            ..request(AddSource::Metainfo(announcing(
                &plain("ruled", 1),
                "http://tracker.example-tracker.test/announce",
            )))
        })
        .await
        .unwrap();

    let handle = handle_of(&rig.service, ruled).await;
    assert!(!handle.takes_peers_from_trackers_only());
    assert_eq!(
        handle.discovery(),
        Discovery {
            dht: false,
            lsd: true
        }
    );
    assert!(!handle.speaks_pex());

    rig.db
        .update_tracker_rule(
            &TrackerRule {
                dht: None,
                pex: Some(true),
                lsd: Some(false),
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
    assert_eq!(
        again.discovery(),
        Discovery {
            dht: true,
            lsd: false
        }
    );
    assert!(again.speaks_pex());
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

// The torrent's trackers as the service lists them: tier, URL and who
// named it.
async fn listed_trackers(service: &TorrentService, job_id: JobId) -> Vec<(u32, String, NamedBy)> {
    service
        .trackers(job_id)
        .await
        .unwrap()
        .into_iter()
        .map(|tracker| (tracker.tier, tracker.url, tracker.named_by))
        .collect()
}

// The trackers a metainfo or a magnet link names are the torrent's.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_trackers_a_torrent_comes_with_are_the_torrents() {
    let a = "http://a.invalid/announce";
    let rig = rig(set_clock());
    rig.service.open().await.unwrap();
    let metainfo = rig
        .service
        .add(AddRequest {
            paused: true,
            ..request(AddSource::Metainfo(announcing(&plain("named", 3), a)))
        })
        .await
        .unwrap();
    let magnet = rig
        .service
        .add(AddRequest {
            paused: true,
            ..request(AddSource::Magnet(format!(
                "magnet:?xt=urn:btih:{}&tr=http%3A%2F%2Fa.invalid%2Fannounce&tr=udp%3A%2F%2Fb.invalid%3A80",
                "ab".repeat(20)
            )))
        })
        .await
        .unwrap();
    assert_eq!(
        listed_trackers(&rig.service, metainfo).await,
        [(0, a.into(), NamedBy::Torrent)]
    );
    assert_eq!(
        listed_trackers(&rig.service, magnet).await,
        [
            (0, a.into(), NamedBy::Torrent),
            (1, "udp://b.invalid:80".into(), NamedBy::Torrent)
        ]
    );
    // As stored.
    let stored: Vec<_> = rig
        .service
        .inner
        .db
        .list_torrent_trackers(magnet)
        .unwrap()
        .into_iter()
        .map(|tracker| tracker.named_by)
        .collect();
    assert_eq!(stored, [NamedBy::Torrent, NamedBy::Torrent]);
    rig.service.stop().await;
}

// Added, replaced and removed trackers are written where the torrent's
// trackers live, reach its session, and are what a restart starts with. A
// tracker an edit names that the torrent did not list is the operator's;
// one it listed keeps who named it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn tracker_edits_outlive_a_restart() {
    let a = "http://a.invalid/announce";
    let b = "http://b.invalid/announce";
    let c = "udp://c.invalid:80";
    let d = "http://d.invalid/announce";
    let (torrent, operator) = (NamedBy::Torrent, NamedBy::Operator);
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
        [
            (0, a.into(), torrent),
            (1, b.into(), operator),
            (2, c.into(), operator)
        ]
    );

    rig.service
        .replace_trackers(
            job_id,
            vec![(1, a.into()), (0, c.into()), (0, c.into()), (2, d.into())],
        )
        .await
        .unwrap();
    assert_eq!(
        listed_trackers(&rig.service, job_id).await,
        [
            (0, c.into(), operator),
            (1, a.into(), torrent),
            (2, d.into(), operator)
        ]
    );

    rig.service
        .remove_trackers(job_id, &[a.into(), d.into()])
        .await
        .unwrap();
    assert_eq!(
        listed_trackers(&rig.service, job_id).await,
        [(0, c.into(), operator)]
    );
    let only_c = [vec![weaver_torrent::tracker::Tracker::operator(
        weaver_torrent::Url::parse(c).unwrap(),
    )]];
    assert_eq!(
        handle_of(&rig.service, job_id)
            .await
            .shared()
            .tracker_tiers(),
        only_c
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
    assert_eq!(
        listed_trackers(&again, job_id).await,
        [(0, c.into(), operator)]
    );
    assert_eq!(
        handle_of(&again, job_id).await.shared().tracker_tiers(),
        only_c
    );
    again.stop().await;
}

// A tracker edit picks the rule the torrent's trackers now match, and a
// restart reads that rule, not the one its first trackers matched.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_rule_a_tracker_edit_picks_is_the_one_a_restart_reads() {
    let a = "http://a.invalid/announce";
    let b = "http://b.invalid/announce";
    let rig = rig(set_clock());
    let rule = rig
        .db
        .insert_tracker_rule(
            &TrackerRule {
                id: 0,
                host_pattern: "b.invalid".into(),
                seeding: None,
                never_pause: false,
                unpack: None,
                client: None,
                torrent_route: false,
                dht: None,
                pex: None,
                lsd: None,
                encryption: None,
                upload_limit: None,
                forced: false,
                ssl: None,
            },
            EPOCH_MS,
        )
        .unwrap();
    rig.service.open().await.unwrap();
    let job_id = rig
        .service
        .add(AddRequest {
            paused: true,
            ..request(AddSource::Metainfo(announcing(&plain("ruled", 5), a)))
        })
        .await
        .unwrap();
    let rule_of = async |service: &TorrentService| service.inner.live.lock().await[&job_id].rule_id;
    assert_eq!(rule_of(&rig.service).await, None);

    rig.service
        .replace_trackers(job_id, vec![(0, b.into())])
        .await
        .unwrap();
    assert_eq!(rule_of(&rig.service).await, Some(rule.id));
    rig.service.stop().await;

    let again = service_on(&rig.db, rig.dir.path(), &rig.routes, job_env(), set_clock());
    reopen(&again).await;
    assert_eq!(rule_of(&again).await, Some(rule.id));
    again.stop().await;
}

// A tracker edit that lands after the attach read the list and before the
// session took the torrent reaches the session: the torrent runs on the
// edited list, not the one the attach read. The attach is polled by hand on
// a runtime with no other worker, so the edit lands at that one point, while
// the attach waits for its route's session.
#[tokio::test(flavor = "current_thread")]
async fn a_tracker_edit_racing_an_attach_reaches_the_session() {
    use std::task::Poll;

    let a = "http://a.invalid/announce";
    let b = "http://b.invalid/announce";
    let rig = rig(set_clock());
    rig.service.open().await.unwrap();
    let job_id = rig
        .service
        .add(AddRequest {
            paused: true,
            ..request(AddSource::Metainfo(announcing(&plain("raced", 6), a)))
        })
        .await
        .unwrap();
    {
        let mut live = rig.service.inner.live.lock().await;
        let torrent = live.get_mut(&job_id).unwrap();
        rig.service.detach(torrent).await;
        torrent.route = None;
    }
    // Its route's session goes with it, so the attach waits for a new one.
    rig.service.drop_idle_sessions().await;

    let mut attach = std::pin::pin!(rig.service.attach(job_id, None));
    let first = std::future::poll_fn(|cx| Poll::Ready(attach.as_mut().poll(cx))).await;
    assert!(first.is_pending(), "the session took the torrent at once");
    assert!(
        rig.service.inner.live.lock().await[&job_id]
            .attached
            .is_none(),
        "the attach finished before the edit"
    );
    rig.service
        .replace_trackers(job_id, vec![(0, b.into())])
        .await
        .unwrap();
    attach.await;

    assert_eq!(
        handle_of(&rig.service, job_id)
            .await
            .shared()
            .tracker_tiers(),
        [vec![weaver_torrent::tracker::Tracker::operator(
            weaver_torrent::Url::parse(b).unwrap(),
        )]]
    );
    rig.service.stop().await;
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
        .add(request(AddSource::Metainfo(plain("again", 4))))
        .await
        .unwrap();
    tracked(&rig.service, job_id, &url).await;
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
        .add(request(AddSource::Metainfo(plain("crypt", 4))))
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

    let (mut started, request) = heard_request(&listener).await;
    assert_eq!(flags(&request), (true, false));
    started.write_all(&answer).await.unwrap();
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
        rig.service.reannounce(job_id).await.unwrap();
        let (mut again, request) = heard_request(&listener).await;
        assert_eq!(flags(&request), said);
        again.write_all(&answer).await.unwrap();
        // The policy each connection is made under as it is made.
        assert_eq!(session.encryption(), policy);
    }
    assert!(Arc::ptr_eq(
        &session,
        &session_of(&rig.service, job_id).await
    ));
    rig.service.stop().await;
}

// A torrent of two files where only the second is fetched: the first's
// front is never in hand, while its last bytes come in the piece that
// starts the second, so they are verified before anything says what the
// first file is.
const SPLIT_PIECE: usize = 16_384;

struct PartlyFetched {
    rig: Rig,
    clock: Arc<SetClock>,
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
    let (bytes, url) = (private.to_vec(), tracker(peer).await);

    let clock = set_clock();
    let rig = rig(Arc::clone(&clock) as Arc<dyn Clock>);
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
    tracked(&rig.service, job_id, &url).await;
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
        clock,
        job_id,
        seeding,
        _seeds: seeds,
        sizes: [first.len() as u64, second.len() as u64],
        sent,
    }
}

// The restart of a partly fetched torrent: it has every piece it had, and
// with the first file wanted it fetches that file's three missing pieces
// and nothing else. The file is wanted before the torrent goes into its
// session, so the seeder is of use from its first connection: a session
// that wants nothing of a seed drops it, and dials it again only much
// later.
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
    again.open().await.unwrap();
    assert_eq!(job_have(&again, job_id).await, 3);
    again
        .set_file_priorities(job_id, &[(0, Normal)])
        .await
        .unwrap();
    again.tick().await;
    let handle = handle_of(&again, job_id).await;
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

// Starting never reads a torrent's files. The service opens with each
// torrent's record, and its view is served from what was kept, before its
// job is restored from its checkpoint; the job is restored at the tick, as
// the torrent goes into its session, with every piece it had.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_restart_restores_a_torrents_job_at_the_tick_not_at_start() {
    let PartlyFetched {
        rig,
        job_id,
        seeding,
        sizes,
        ..
    } = partly_fetched().await;
    rig.service.stop().await;
    rig.db.prune_orphan_active_state().unwrap();
    let ledger = rig.db.get_torrent_counters(job_id).unwrap().unwrap();

    let again = service_on(&rig.db, rig.dir.path(), &rig.routes, job_env(), set_clock());
    let written = again.written();
    let reads = || written.borrow().checkpoints_read;
    again.open().await.unwrap();
    // The tick has not run: the job's checkpoint is unread, and the torrent
    // shows what was stored of it: its ledger, its files, and what its
    // checkpoint has in hand of them.
    assert_eq!(reads(), 0);
    let kept = again.torrent(job_id).await.unwrap();
    assert_eq!(
        lifetime(&kept),
        (
            ledger.downloaded_bytes,
            ledger.uploaded_bytes,
            ledger.active_duration_s
        )
    );
    assert_eq!(kept.total_bytes, sizes.iter().sum::<u64>());
    assert_eq!(kept.files.len(), 2);
    assert_eq!(in_hand(&kept), checkpointed(sizes));

    again.tick().await;
    assert_eq!(reads(), 1);
    assert!(again.inner.live.lock().await[&job_id].attached.is_some());
    assert_eq!(job_have(&again, job_id).await, 3);
    let ticked = again.torrent(job_id).await.unwrap();
    assert_eq!(in_hand(&ticked), checkpointed(sizes));
    again.stop().await;
    seeding.stop().await;
}

// A restart while a check of every piece had found only some of them: the
// torrent shows before its job is back exactly what the job restored from
// its checkpoint holds, the pieces the check found and none it had yet to
// find.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_restart_during_a_check_shows_what_its_job_will_hold() {
    let PartlyFetched {
        rig,
        job_id,
        seeding,
        sizes,
        ..
    } = partly_fetched().await;
    // The process ends while its session checks every piece and has found
    // piece 4 of the three the job held.
    rig.service.inner.sessions.stop_all().await;
    let intake = rig.service.inner.live.lock().await[&job_id]
        .intake
        .clone()
        .unwrap();
    assert!(intake.checking().unwrap());
    assert!(intake.found(4).unwrap());
    rig.service.save_checkpoints([job_id].into()).await;
    rig.db.prune_orphan_active_state().unwrap();

    let again = service_on(&rig.db, rig.dir.path(), &rig.routes, job_env(), set_clock());
    let written = again.written();
    again.open().await.unwrap();
    assert_eq!(written.borrow().checkpoints_read, 0);
    let kept = in_hand(&again.torrent(job_id).await.unwrap());
    // Its progress is over the second file, the one it wants.
    let found = SPLIT_PIECE as u64;
    assert_eq!(
        kept,
        (
            vec![0, found],
            found,
            found as f64 / sizes[1] as f64,
            Some(1)
        )
    );

    again.restore_jobs().await;
    assert_eq!(written.borrow().checkpoints_read, 1);
    assert!(again.inner.live.lock().await[&job_id].job.is_some());
    assert_eq!(in_hand(&again.torrent(job_id).await.unwrap()), kept);
    again.stop().await;
    seeding.stop().await;
}

// What a view shows it has in hand: each file's bytes, all of them, the
// progress, and the pieces.
fn in_hand(view: &TorrentView) -> (Vec<u64>, u64, f64, Option<u32>) {
    (
        view.file_have.clone(),
        view.have_bytes,
        view.progress,
        view.pieces.map(|pieces| pieces.have),
    )
}

// What a partly fetched torrent of files `sizes` has in hand: pieces 3, 4
// and 5, the first file's last half piece and the second file whole, which
// is all it wants.
fn checkpointed(sizes: [u64; 2]) -> (Vec<u64>, u64, f64, Option<u32>) {
    let have = [SPLIT_PIECE as u64 / 2, sizes[1]];
    let all = have.iter().sum::<u64>();
    (have.to_vec(), all, 1.0, Some(3))
}

// A paused torrent restarts paused, showing what its checkpoint has in hand
// before its job is back, and the same once it is.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_paused_torrent_restarts_showing_what_it_has() {
    let PartlyFetched {
        rig,
        job_id,
        seeding,
        sizes,
        ..
    } = partly_fetched().await;
    rig.service.pause(job_id).await.unwrap();
    rig.service.stop().await;
    rig.db.prune_orphan_active_state().unwrap();

    let again = service_on(&rig.db, rig.dir.path(), &rig.routes, job_env(), set_clock());
    let written = again.written();
    again.open().await.unwrap();
    assert_eq!(written.borrow().checkpoints_read, 0);
    let kept = again.torrent(job_id).await.unwrap();
    assert_eq!(kept.state, TorrentState::Paused);
    assert_eq!(in_hand(&kept), checkpointed(sizes));

    again.tick().await;
    assert_eq!(written.borrow().checkpoints_read, 1);
    let ticked = again.torrent(job_id).await.unwrap();
    assert_eq!(ticked.state, TorrentState::Paused);
    assert_eq!(in_hand(&ticked), checkpointed(sizes));
    again.stop().await;
    seeding.stop().await;
}

// What a torrent's view shows of its ledger: the figures that outlive a
// run.
fn lifetime(view: &TorrentView) -> (u64, u64, u64) {
    (
        view.downloaded_bytes,
        view.uploaded_bytes,
        view.active_seconds,
    )
}

// A partly fetched torrent read at two ticks a tick apart: what it shows of
// its ledger then.
async fn ticked(fetched: &PartlyFetched) -> (u64, u64, u64) {
    let service = &fetched.rig.service;
    service.tick().await;
    fetched
        .clock
        .0
        .fetch_add(TICK.as_millis() as i64, Ordering::SeqCst);
    service.tick().await;
    let shown = lifetime(&service.torrent(fetched.job_id).await.unwrap());
    assert!(shown.0 >= 3 * SPLIT_PIECE as u64, "{shown:?}");
    assert_eq!(shown.2, TICK.as_secs(), "{shown:?}");
    shown
}

// The restart of a partly fetched torrent shows `shown` of its ledger.
async fn restarts_showing(fetched: PartlyFetched, shown: (u64, u64, u64)) {
    let PartlyFetched {
        rig,
        job_id,
        seeding,
        ..
    } = fetched;
    // What the server's recovery clears before anything starts.
    rig.db.prune_orphan_active_state().unwrap();
    let again = service_on(&rig.db, rig.dir.path(), &rig.routes, job_env(), set_clock());
    reopen(&again).await;
    handle_of(&again, job_id)
        .await
        .wait_until_completed()
        .await
        .unwrap();
    assert_eq!(lifetime(&again.torrent(job_id).await.unwrap()), shown);
    again.stop().await;
    seeding.stop().await;
}

// A clean stop keeps the ledger exactly as the torrent showed it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_torrent_stopped_part_way_restarts_with_its_ledger() {
    let fetched = partly_fetched().await;
    let shown = ticked(&fetched).await;
    fetched.rig.service.stop().await;
    restarts_showing(fetched, shown).await;
}

// As its pieces do, a torrent's ledger outlives a crash: the restart shows
// what the last tick knew.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_torrent_that_crashed_part_way_restarts_with_its_last_ticks_ledger() {
    let fetched = partly_fetched().await;
    let shown = ticked(&fetched).await;
    // The process ends here: its sessions go with it, and nothing else is
    // written.
    fetched.rig.service.inner.sessions.stop_all().await;
    restarts_showing(fetched, shown).await;
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

// An announce a tracker heard: its event, the torrent's key and peer id,
// and the counters it carried.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Heard {
    event: Option<String>,
    key: String,
    peer_id: String,
    downloaded: u64,
    uploaded: u64,
}

impl Heard {
    fn of(query: &str) -> Self {
        let field = |name: &str| {
            query
                .split('&')
                .find_map(|kv| kv.strip_prefix(name)?.strip_prefix('='))
                .map(str::to_owned)
        };
        let count = |name: &str| field(name).unwrap().parse().unwrap();
        Self {
            event: field("event"),
            key: field("key").unwrap(),
            peer_id: field("peer_id").unwrap(),
            downloaded: count("downloaded"),
            uploaded: count("uploaded"),
        }
    }

    fn identity(&self) -> (&str, &str) {
        (&self.key, &self.peer_id)
    }
}

// A tracker on loopback that names `peers` to every announce and hands the
// test each announce in the order it read them.
async fn heard_tracker(
    peers: &[std::net::SocketAddrV4],
) -> (String, tokio::sync::mpsc::UnboundedReceiver<Heard>) {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/announce", listener.local_addr().unwrap());
    let mut body = format!("d8:intervali1800e5:peers{}:", peers.len() * 6).into_bytes();
    for peer in peers {
        body.extend_from_slice(&peer.ip().octets());
        body.extend_from_slice(&peer.port().to_be_bytes());
    }
    body.push(b'e');
    let (heard, announces) = tokio::sync::mpsc::unbounded_channel();
    tokio::spawn(async move {
        while let Ok((mut stream, _)) = listener.accept().await {
            let heard = heard.clone();
            let body = body.clone();
            tokio::spawn(async move {
                let mut request = Vec::new();
                let mut buf = [0u8; 1024];
                while !request.windows(4).any(|end| end == b"\r\n\r\n") {
                    match stream.read(&mut buf).await {
                        Ok(0) | Err(_) => return,
                        Ok(read) => request.extend_from_slice(&buf[..read]),
                    }
                }
                let request = String::from_utf8_lossy(&request);
                let path = request.split(' ').nth(1).unwrap();
                let (_, query) = path.split_once('?').unwrap();
                let _ = heard.send(Heard::of(query));
                let head = format!(
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
                    body.len()
                );
                if let Err(error) = stream.write_all(&[head.as_bytes(), &body].concat()).await {
                    tracing::debug!(error = %error, "a client hung up before its answer");
                }
            });
        }
    });
    (url, announces)
}

// Once the torrent's session has the answer to its `started`: a tracker is
// owed `stopped` only once it accepted `started`.
async fn started_accepted(service: &TorrentService, job_id: JobId) {
    trackers_once(service, job_id, |announces| {
        announces.iter().any(|announce| {
            announce
                .last
                .as_ref()
                .is_some_and(|last| last.result.is_ok())
        })
    })
    .await;
}

// The next announce heard that carries an event.
async fn next_event(announces: &mut tokio::sync::mpsc::UnboundedReceiver<Heard>) -> Heard {
    loop {
        let heard = announces.recv().await.unwrap();
        if heard.event.is_some() {
            return heard;
        }
    }
}

// A torrent paused and resumed announces as the same torrent: one key and
// one peer id from its `started` through its `stopped` to its next
// `started`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_paused_and_resumed_torrent_keeps_its_key_and_peer_id() {
    let (url, mut announces) = heard_tracker(&[]).await;
    let rig = rig(set_clock());
    on_loopback(&rig.routes);
    rig.service.open().await.unwrap();
    let job_id = rig
        .service
        .add(AddRequest {
            paused: true,
            ..request(AddSource::Metainfo(plain("kept", 7)))
        })
        .await
        .unwrap();
    tracked(&rig.service, job_id, &url).await;
    rig.service.resume(job_id).await.unwrap();
    let started = next_event(&mut announces).await;
    assert_eq!(started.event.as_deref(), Some("started"));
    started_accepted(&rig.service, job_id).await;

    rig.service.pause(job_id).await.unwrap();
    let stopped = next_event(&mut announces).await;
    assert_eq!(stopped.event.as_deref(), Some("stopped"));
    rig.service.resume(job_id).await.unwrap();
    let again = next_event(&mut announces).await;
    assert_eq!(again.event.as_deref(), Some("started"));

    assert_eq!(stopped.identity(), started.identity());
    assert_eq!(again.identity(), started.identity());
    rig.service.stop().await;
}

// A torrent whose route changes while its old leg stands says `stopped`
// over the old leg and `started` over the new one as the same torrent, and
// its counters go on from where they were: the new session's totals are the
// old session's, and the trackers hear them so.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_torrent_moved_to_a_new_route_keeps_its_identity_and_counters() {
    let data: Vec<u8> = (0..65_536u32).map(|at| (at % 251) as u8).collect();
    let files = [("set/a.bin", data.as_slice())];
    let private = metainfo("set", &files, 16_384, true);
    let seeds = tempfile::tempdir().unwrap();
    let seeding = seeder(seeds.path(), &private, &files).await;
    let std::net::SocketAddr::V4(peer) = seeding.listen_addr().unwrap() else {
        panic!("the seeder listens on IPv4 loopback");
    };
    let (url, mut announces) = heard_tracker(&[peer]).await;

    let rig = rig(set_clock());
    on_loopback(&rig.routes);
    rig.service.open().await.unwrap();
    let job_id = rig
        .service
        .add(AddRequest {
            paused: true,
            ..request(AddSource::Metainfo(private.to_vec()))
        })
        .await
        .unwrap();
    tracked(&rig.service, job_id, &url).await;
    rig.service.resume(job_id).await.unwrap();
    let started = next_event(&mut announces).await;
    assert_eq!(started.event.as_deref(), Some("started"));
    let old = handle_of(&rig.service, job_id).await;
    old.wait_until_completed().await.unwrap();
    let before = old.totals();
    assert_eq!(before.downloaded_bytes, data.len() as u64);

    rig.routes.route(TorrentScope::Default, 2);
    rig.service.follow_routes().await;

    // The old session's `stopped` and the new one's `started`, in either
    // order; a `completed` the old session sent first says nothing here.
    let mut moved = Vec::new();
    while moved.len() < 2 {
        let heard = next_event(&mut announces).await;
        if heard.event.as_deref() != Some("completed") {
            moved.push(heard);
        }
    }
    moved.sort_by(|a, b| a.event.cmp(&b.event));
    let [again, stopped] = <[Heard; 2]>::try_from(moved).unwrap();
    assert_eq!(again.event.as_deref(), Some("started"));
    assert_eq!(stopped.event.as_deref(), Some("stopped"));
    for heard in [&again, &stopped] {
        assert_eq!(heard.identity(), started.identity());
        assert_eq!(
            (heard.downloaded, heard.uploaded),
            (before.downloaded_bytes, before.uploaded_bytes)
        );
    }

    // The new session starts from the old one's totals as they ended; only
    // the bytes of its own connections move after, and no payload does.
    let ended = old.totals();
    let now = handle_of(&rig.service, job_id).await.totals();
    assert_eq!(
        (now.downloaded_bytes, now.uploaded_bytes, now.fetched_bytes),
        (
            ended.downloaded_bytes,
            ended.uploaded_bytes,
            ended.fetched_bytes
        )
    );
    assert_eq!(
        (ended.downloaded_bytes, ended.uploaded_bytes),
        (before.downloaded_bytes, before.uploaded_bytes)
    );
    assert!(now.fetched_protocol_bytes >= ended.fetched_protocol_bytes);
    assert!(now.uploaded_protocol_bytes >= ended.uploaded_protocol_bytes);
    rig.service.stop().await;
}

// A torrent whose route changes while its old leg is out of quota says
// nothing more over that leg: the tracker hears the new session's `started`
// as the same torrent, and no `stopped` until the torrent is removed.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_torrent_moved_off_a_spent_leg_sends_no_stopped_over_it() {
    let (url, mut announces) = heard_tracker(&[]).await;
    let rig = rig(set_clock());
    on_loopback(&rig.routes);
    rig.service.open().await.unwrap();
    let job_id = rig
        .service
        .add(AddRequest {
            paused: true,
            ..request(AddSource::Metainfo(plain("spent", 8)))
        })
        .await
        .unwrap();
    tracked(&rig.service, job_id, &url).await;
    rig.service.resume(job_id).await.unwrap();
    let started = next_event(&mut announces).await;
    assert_eq!(started.event.as_deref(), Some("started"));

    rig.routes.spend(0);
    rig.routes.route(TorrentScope::Default, 2);
    rig.service.follow_routes().await;
    let again = next_event(&mut announces).await;
    assert_eq!(again.event.as_deref(), Some("started"));
    assert_eq!(again.identity(), started.identity());
    started_accepted(&rig.service, job_id).await;

    // The one `stopped` the tracker hears is the new session's, at the
    // remove.
    rig.service.remove(job_id, false, false).await.unwrap();
    let stopped = next_event(&mut announces).await;
    assert_eq!(stopped.event.as_deref(), Some("stopped"));
    assert_eq!(stopped.identity(), started.identity());
    rig.service.stop().await;
    while let Ok(heard) = announces.try_recv() {
        assert_eq!(heard.event, None, "the tracker heard more: {heard:?}");
    }
}

// A torrent whose leg is revoked under its session, as saving its proxy's
// profile again revokes it, moves to a session on the leg its route has now:
// the tracker hears the new session's `started` as the same torrent, and no
// `stopped` from the old session, which the revocation withheld, until the
// torrent is removed. The old leg's sockets still reach the tracker, so a
// `stopped` the old session sent would be heard.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_torrent_moved_off_a_revoked_leg_sends_no_stopped_over_it() {
    let (url, mut announces) = heard_tracker(&[]).await;
    let rig = rig(set_clock());
    let (db, network) = on_network(&rig.routes);
    on_loopback(&rig.routes);
    rig.service.open().await.unwrap();
    let job_id = rig
        .service
        .add(AddRequest {
            paused: true,
            ..request(AddSource::Metainfo(plain("revoked", 10)))
        })
        .await
        .unwrap();
    tracked(&rig.service, job_id, &url).await;
    rig.service.resume(job_id).await.unwrap();
    let started = next_event(&mut announces).await;
    assert_eq!(started.event.as_deref(), Some("started"));
    started_accepted(&rig.service, job_id).await;
    let before = session_of(&rig.service, job_id).await;

    save_again(&db, &network).await;
    rig.service.follow_routes().await;
    let again = next_event(&mut announces).await;
    assert_eq!(again.event.as_deref(), Some("started"));
    assert_eq!(again.identity(), started.identity());
    assert!(!Arc::ptr_eq(
        &session_of(&rig.service, job_id).await,
        &before
    ));
    started_accepted(&rig.service, job_id).await;
    before.cancellation_token().cancelled().await;

    // The one `stopped` the tracker hears is the new session's, at the
    // remove.
    rig.service.remove(job_id, false, false).await.unwrap();
    let stopped = next_event(&mut announces).await;
    assert_eq!(stopped.event.as_deref(), Some("stopped"));
    assert_eq!(stopped.identity(), started.identity());
    rig.service.stop().await;
    while let Ok(heard) = announces.try_recv() {
        assert_eq!(heard.event, None, "the tracker heard more: {heard:?}");
    }
    network.shutdown().await;
}

// A torrent removed and added again is a new torrent to its trackers: a new
// key and a new peer id.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_torrent_removed_and_added_again_takes_a_new_identity() {
    let (url, mut announces) = heard_tracker(&[]).await;
    let bytes = plain("again", 9);
    let rig = rig(set_clock());
    on_loopback(&rig.routes);
    rig.service.open().await.unwrap();
    let job_id = rig
        .service
        .add(AddRequest {
            paused: true,
            ..request(AddSource::Metainfo(bytes.clone()))
        })
        .await
        .unwrap();
    tracked(&rig.service, job_id, &url).await;
    rig.service.resume(job_id).await.unwrap();
    let first = next_event(&mut announces).await;
    assert_eq!(first.event.as_deref(), Some("started"));
    started_accepted(&rig.service, job_id).await;
    rig.service.remove(job_id, false, false).await.unwrap();
    let stopped = next_event(&mut announces).await;
    assert_eq!(stopped.event.as_deref(), Some("stopped"));

    let job_id = rig
        .service
        .add(AddRequest {
            paused: true,
            ..request(AddSource::Metainfo(bytes))
        })
        .await
        .unwrap();
    tracked(&rig.service, job_id, &url).await;
    rig.service.resume(job_id).await.unwrap();
    let second = next_event(&mut announces).await;
    assert_eq!(second.event.as_deref(), Some("started"));
    assert_ne!(second.key, first.key);
    assert_ne!(second.peer_id, first.peer_id);
    rig.service.stop().await;
}

// The figures the exporter reads are the ones the tick read: a torrent
// whose ledger and policy are known renders them, its trackers one series
// per host, and each add and removal counts once.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_metrics_carry_what_the_tick_read() {
    use crate::torrents::metrics::{FetchOutcome, SeedingGoal, TorrentEvent};

    let clock = set_clock();
    let rig = rig(Arc::clone(&clock) as Arc<dyn Clock>);
    // No session is built while the route is down, so nothing but the
    // test moves the ledger.
    rig.routes.set_down(Some("down"));
    rig.service.open().await.unwrap();
    let job_id = rig
        .service
        .add(AddRequest {
            category: Some("tv".into()),
            policy_override: Some(SeedingPolicy {
                ratio_goal: Some(3.0),
                seed_time_goal_minutes: Some(10),
                inactive_goal_minutes: None,
                mode: SeedingGoalMode::All,
                action: SeedingAction::Stop,
                max_upload_bps: None,
            }),
            ..request(AddSource::Metainfo(plain("set", 1)))
        })
        .await
        .unwrap();
    let gone = rig
        .service
        .add(request(AddSource::Metainfo(plain("gone", 2))))
        .await
        .unwrap();
    rig.service
        .add_trackers(
            job_id,
            vec![
                vec!["http://127.0.0.1:1/announce".to_owned()],
                vec!["http://127.0.0.1:2/announce".to_owned()],
            ],
        )
        .await
        .unwrap();
    rig.service.remove(gone, true, false).await.unwrap();
    {
        let mut live = rig.service.inner.live.lock().await;
        let torrent = live.get_mut(&job_id).unwrap();
        torrent.counters.downloaded_bytes = 3000;
        torrent.counters.uploaded_bytes = 6000;
        torrent.counters.finished_duration_s = 120;
    }
    clock.0.fetch_add(TICK.as_millis() as i64, Ordering::SeqCst);
    rig.service.tick().await;

    let metrics = rig.service.metrics();
    let view = rig.service.torrent(job_id).await.unwrap();
    let published = &metrics.published;
    assert_eq!(published.torrents.len(), 1, "{published:?}");
    let job = published.job(job_id).unwrap();
    let torrent = job.torrent;
    assert_eq!(torrent.category, "tv");
    assert_eq!(Some(torrent.route.as_str()), view.route.as_deref());
    assert_eq!(torrent.total_bytes, 3000);
    assert_eq!(torrent.wanted_bytes, 3000);
    assert_eq!(torrent.downloaded_bytes, 3000);
    assert_eq!(torrent.uploaded_bytes, 6000);
    assert_eq!(torrent.seeding_seconds, 120);
    assert_eq!(torrent.ratio, 2.0);
    assert_eq!(torrent.ratio, view.ratio);
    assert_eq!(torrent.state, view.state);
    assert_eq!(torrent.fetch, None);
    assert_eq!(
        torrent.seeding.goals,
        [
            (SeedingGoal::Ratio, 3.0, 1.0),
            (SeedingGoal::SeedTime, 600.0, 480.0)
        ]
    );
    assert!(!torrent.seeding.met);
    // Both trackers are on one host: one series, the first listed.
    assert_eq!(torrent.trackers.len(), 1, "{:?}", torrent.trackers);
    assert_eq!(torrent.trackers[0].host, "127.0.0.1");
    assert_eq!(torrent.trackers[0].status, view.trackers[0].status);
    assert_eq!(torrent.trackers[0].last_announce_ms, None);
    // The route is down: no session runs on it.
    assert!(job.session.is_none_or(|session| !session.open));

    assert_eq!(
        metrics.events,
        [
            (TorrentEvent::Added, 2),
            (TorrentEvent::Completed, 0),
            (TorrentEvent::Removed, 1),
            (TorrentEvent::Failed, 0),
        ]
    );
    assert_eq!(
        metrics.fetches,
        FetchOutcome::ALL.map(|outcome| (outcome, 0)).to_vec()
    );
    rig.service.stop().await;
}

// The figures the service publishes for `job_id`, once `holds` says they
// hold of them.
async fn published_once(
    service: &TorrentService,
    job_id: JobId,
    holds: impl Fn(&TorrentSeries) -> bool,
) -> TorrentSeries {
    let mut published = service.inner.published.subscribe();
    let published = published
        .wait_for(|published| published.job(job_id).is_some_and(|job| holds(job.torrent)))
        .await
        .unwrap();
    published.job(job_id).unwrap().torrent.clone()
}

// A magnet's fetch is published as it runs, with no tick: its phase while
// its tracker holds the announce, and the phase the tracker's answer moves
// it to, one phase at a time.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_fetching_magnets_phase_is_published_as_it_moves() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let rig = rig(set_clock());
    on_loopback(&rig.routes);
    rig.service.open().await.unwrap();
    let job_id = fetching_from(&rig.service, &"fd".repeat(20), port).await;

    let (started, event) = heard_event(&listener).await;
    assert_eq!(event.as_deref(), Some("started"));
    let fetching = published_once(&rig.service, job_id, |torrent| torrent.fetch.is_some()).await;
    assert_eq!(fetching.state, TorrentState::FetchingMetadata);
    assert_eq!(fetching.fetch, Some(weaver_torrent::FetchPhase::Trackers));

    answer_no_peers(started).await;
    let moved = published_once(&rig.service, job_id, |torrent| {
        torrent
            .fetch
            .is_some_and(|phase| phase != weaver_torrent::FetchPhase::Trackers)
    })
    .await;
    let phase = {
        let live = rig.service.inner.live.lock().await;
        let fetch = live[&job_id].fetching.as_ref().unwrap();
        *fetch.phase.borrow()
    };
    assert_eq!(moved.fetch, Some(phase));
    assert_eq!(moved.state, TorrentState::FetchingMetadata);
    drop(listener);
    rig.service.stop().await;
}

// A tracker's answer is published as it lands, with no tick: the exporter
// reads the tracker as the view does.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_trackers_answer_is_published_as_it_lands() {
    use tokio::io::AsyncWriteExt;

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!(
        "http://127.0.0.1:{}/announce",
        listener.local_addr().unwrap().port()
    );
    let rig = rig(set_clock());
    on_loopback(&rig.routes);
    rig.service.open().await.unwrap();
    let job_id = rig
        .service
        .add(AddRequest {
            paused: true,
            ..request(AddSource::Metainfo(plain("answered", 3)))
        })
        .await
        .unwrap();
    tracked(&rig.service, job_id, &url).await;
    rig.service.resume(job_id).await.unwrap();

    let (mut started, event) = heard_event(&listener).await;
    assert_eq!(event.as_deref(), Some("started"));
    let body = b"d8:completei3e10:incompletei5e8:intervali1800e5:peers0:e";
    let head = format!(
        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    started
        .write_all(&[head.as_bytes(), body].concat())
        .await
        .unwrap();
    let answered = published_once(&rig.service, job_id, |torrent| {
        torrent
            .trackers
            .first()
            .is_some_and(|tracker| tracker.status == TrackerStatus::Working)
    })
    .await;
    let tracker = &answered.trackers[0];
    assert_eq!((tracker.seeders, tracker.leechers), (Some(3), Some(5)));
    assert_eq!(tracker.interval_seconds, Some(1800));
    assert_eq!(tracker.fails, 0);
    let view = rig.service.torrent(job_id).await.unwrap();
    assert_eq!(view.trackers[0].status, TrackerStatus::Working);
    assert_eq!(tracker.last_announce_ms, view.trackers[0].last_announce_ms);
    drop(listener);
    rig.service.stop().await;
}

// A magnet's metainfo is published as it arrives, with no tick: its size
// is the metainfo's from then on. Its tracker holds the announce until the
// resume has published, then names a peer that has the metainfo and none
// of the pieces, so no piece landing publishes it instead.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_magnets_metainfo_is_published_as_it_arrives() {
    use tokio::io::AsyncWriteExt;

    let data: Vec<u8> = (0..65_536u32).map(|at| (at % 251) as u8).collect();
    let files = [("set/a.bin", data.as_slice())];
    let shared = metainfo("set", &files, 16_384, false);
    let seeds = tempfile::tempdir().unwrap();
    let (seeding, _) = sharing(seeds.path(), &shared, &[]).await;
    let std::net::SocketAddr::V4(peer) = seeding.listen_addr().unwrap() else {
        panic!("the seeder listens on IPv4 loopback");
    };
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let port = listener.local_addr().unwrap().port();
    let hash = parse(&shared).unwrap().hashes.v1.unwrap().as_string();

    let rig = rig(set_clock());
    on_loopback(&rig.routes);
    rig.service.open().await.unwrap();
    let job_id = fetching_from(&rig.service, &hash, port).await;
    let (mut started, event) = heard_event(&listener).await;
    assert_eq!(event.as_deref(), Some("started"));
    let mut body = b"d8:intervali1800e5:peers6:".to_vec();
    body.extend_from_slice(&peer.ip().octets());
    body.extend_from_slice(&peer.port().to_be_bytes());
    body.push(b'e');
    let head = format!(
        "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n",
        body.len()
    );
    started
        .write_all(&[head.as_bytes(), &body].concat())
        .await
        .unwrap();

    let total = data.len() as u64;
    let arrived = published_once(&rig.service, job_id, |torrent| torrent.total_bytes != 0).await;
    assert_eq!(arrived.total_bytes, total);
    assert_eq!(arrived.wanted_bytes, total);
    assert_eq!(
        rig.service.torrent(job_id).await.unwrap().total_bytes,
        total
    );
    drop(listener);
    rig.service.stop().await;
    seeding.stop().await;
}

// Turning torrents off closes their sessions, and what is published says
// so at once: the route a torrent names holds no session, and shows none
// of the closed session's figures.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn turning_torrents_off_publishes_their_routes_closed() {
    let rig = rig(set_clock());
    on_loopback(&rig.routes);
    rig.service.open().await.unwrap();
    let job_id = rig
        .service
        .add(request(AddSource::Metainfo(plain("closed", 4))))
        .await
        .unwrap();
    rig.service.tick().await;
    let route = {
        let metrics = rig.service.metrics();
        let job = metrics.published.job(job_id).unwrap();
        let session = job.session.unwrap();
        assert!(session.open, "{session:?}");
        job.torrent.route.clone()
    };

    let mut settings = rig.service.settings();
    settings.torrent.enabled = false;
    rig.service.set_settings(settings).await;
    let metrics = rig.service.metrics();
    let published = &metrics.published;
    let job = published.job(job_id).unwrap();
    assert_eq!(job.torrent.route, route);
    // A held route carries its name and that it is not open, nothing more:
    // no figure of the closed session, no listener, no DHT.
    let held = SessionSeries {
        route: route.clone(),
        open: false,
        ..SessionSeries::default()
    };
    assert_eq!(job.session, Some(&held));
    assert_eq!(SessionSeries::held(&route), held);
    assert_eq!(
        published
            .sessions
            .iter()
            .filter(|session| session.route == route)
            .count(),
        1,
        "{:?}",
        published.sessions
    );

    // Turned on again, the first publish that shows the torrent back in
    // its session shows the session open.
    let mut watch = rig.service.inner.published.subscribe();
    let mut settings = rig.service.settings();
    settings.torrent.enabled = true;
    rig.service.set_settings(settings).await;
    let back = watch
        .wait_for(|published| {
            published.job(job_id).is_some_and(|job| {
                !matches!(
                    job.torrent.state,
                    TorrentState::Stopped | TorrentState::Queued
                )
            })
        })
        .await
        .unwrap();
    let job = back.job(job_id).unwrap();
    assert!(job.session.is_some_and(|session| session.open), "{job:?}");
    drop(back);
    rig.service.stop().await;
}

// A torrent put in its route's session is published in an open session by
// any publish from then on, not only once the tick has read the sessions:
// an action's publish can come between the two.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_torrent_given_its_session_is_published_in_it_open() {
    let rig = rig(set_clock());
    on_loopback(&rig.routes);
    rig.service.open().await.unwrap();
    let job_id = rig
        .service
        .add(request(AddSource::Metainfo(plain("given", 4))))
        .await
        .unwrap();
    rig.service.follow_routes().await;
    rig.service.publish().await;
    let metrics = rig.service.metrics();
    let job = metrics.published.job(job_id).unwrap();
    assert!(
        !matches!(
            job.torrent.state,
            TorrentState::Stopped | TorrentState::Queued
        ),
        "{job:?}"
    );
    assert!(job.session.is_some_and(|session| session.open), "{job:?}");
    rig.service.stop().await;
}

// A torrent of one file, `a.bin`, under a folder of its own, `placed`, and
// that file's bytes: four pieces.
fn placed_torrent() -> (Vec<u8>, Vec<u8>) {
    let data: Vec<u8> = (0..65_536u32).map(|at| (at % 241) as u8).collect();
    let metainfo = metainfo("placed", &[("a.bin", data.as_slice())], 16_384, true);
    (metainfo, data)
}

// The torrent added with `placement`, its view once it seeds, as the queue
// publishes it.
async fn seeding_placed(
    rig: &Rig,
    metainfo: Vec<u8>,
    placement: Placement,
) -> (JobId, TorrentView) {
    let handle = queue();
    let mut changes = handle.state().subscribe_job_changes();
    rig.service.publish_to(handle);
    let job_id = rig
        .service
        .add(AddRequest {
            placement,
            ..request(AddSource::Metainfo(metainfo))
        })
        .await
        .unwrap();
    let view = seeding_view(&rig.service, &mut changes, job_id).await;
    (job_id, view)
}

// The torrent's view once `holds` holds of it, read as the queue publishes
// it.
async fn view_once(
    service: &TorrentService,
    changes: &mut watch::Receiver<u64>,
    job_id: JobId,
    holds: impl Fn(&TorrentView) -> bool,
) -> TorrentView {
    loop {
        changes.borrow_and_update();
        let view = service.torrent(job_id).await.unwrap();
        if holds(&view) {
            return view;
        }
        changes.changed().await.unwrap();
    }
}

// The torrent added with `placement`, its view once its job is done and it
// seeds, and the queue's changes from then on.
async fn completed_placed(
    rig: &Rig,
    metainfo: Vec<u8>,
    placement: Placement,
) -> (JobId, TorrentView, watch::Receiver<u64>) {
    let handle = queue();
    let mut changes = handle.state().subscribe_job_changes();
    rig.service.publish_to(handle);
    let job_id = rig
        .service
        .add(AddRequest {
            placement,
            ..request(AddSource::Metainfo(metainfo))
        })
        .await
        .unwrap();
    let view = view_once(&rig.service, &mut changes, job_id, |view| {
        view.state == TorrentState::Seeding && view.completed_at_ms.is_some()
    })
    .await;
    (job_id, view, changes)
}

// qBittorrent `addAction` with a save path: the files under it are the
// torrent's, laid out as its metainfo lists them, and the check as it is
// added finds them. Nothing is fetched.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_torrent_added_at_a_save_path_has_the_files_already_there() {
    let (metainfo, data) = placed_torrent();
    let rig = rig(set_clock());
    on_loopback(&rig.routes);
    rig.service.open().await.unwrap();
    let save = rig.dir.path().join("placed-here");
    std::fs::create_dir_all(save.join("placed")).unwrap();
    std::fs::write(save.join("placed/a.bin"), &data).unwrap();

    let (_, view) = seeding_placed(
        &rig,
        metainfo,
        Placement {
            save_path: Some(save.to_string_lossy().into_owned()),
            ..Placement::default()
        },
    )
    .await;
    let total = data.len() as u64;
    assert_eq!(
        (view.have_bytes, view.downloaded_bytes, view.progress),
        (total, 0, 1.0)
    );
    assert_eq!(
        view.storage_dir.as_deref(),
        Some(save.join("placed").to_string_lossy().as_ref())
    );
    assert_eq!(std::fs::read(save.join("placed/a.bin")).unwrap(), data);
    rig.service.stop().await;
}

// qBittorrent `NoSubfolder`: the files sit straight in the save path, with no
// folder of the torrent's own, and are found there.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_torrent_laid_out_without_its_folder_has_the_files_in_the_save_path() {
    let (metainfo, data) = placed_torrent();
    let rig = rig(set_clock());
    on_loopback(&rig.routes);
    rig.service.open().await.unwrap();
    let save = rig.dir.path().join("flat");
    std::fs::create_dir_all(&save).unwrap();
    std::fs::write(save.join("a.bin"), &data).unwrap();

    let (_, view) = seeding_placed(
        &rig,
        metainfo,
        Placement {
            save_path: Some(save.to_string_lossy().into_owned()),
            layout: Some(ContentLayout::NoSubfolder),
            ..Placement::default()
        },
    )
    .await;
    let total = data.len() as u64;
    assert_eq!((view.have_bytes, view.downloaded_bytes), (total, 0));
    assert_eq!(
        view.files_dir.as_deref(),
        Some(save.to_string_lossy().as_ref())
    );
    assert_eq!(std::fs::read(save.join("a.bin")).unwrap(), data);
    rig.service.stop().await;
}

// qBittorrent `TorrentImpl::setSavePath`, libtorrent `torrent::move_storage`:
// files that sat straight in their save path move straight into the new
// one, the torrent's save path and files directory follow them, and it
// seeds on from there.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn placed_files_move_to_a_new_location_and_the_record_follows() {
    let (metainfo, data) = placed_torrent();
    let rig = rig(set_clock());
    on_loopback(&rig.routes);
    rig.service.open().await.unwrap();
    let save = rig.dir.path().join("flat");
    std::fs::create_dir_all(&save).unwrap();
    std::fs::write(save.join("a.bin"), &data).unwrap();
    // Its job is done, so nothing of it is out and each move happens at
    // once.
    let (job_id, before, _) = completed_placed(
        &rig,
        metainfo,
        Placement {
            save_path: Some(save.to_string_lossy().into_owned()),
            layout: Some(ContentLayout::NoSubfolder),
            ..Placement::default()
        },
    )
    .await;

    let moved = rig.dir.path().join("moved/here");
    let moved_text = moved.to_string_lossy().into_owned();
    rig.service
        .set_location(job_id, moved_text.clone())
        .await
        .unwrap();
    assert!(!save.join("a.bin").exists());
    assert_eq!(std::fs::read(moved.join("a.bin")).unwrap(), data);
    let stored = rig.db.get_torrent(job_id).unwrap().unwrap();
    assert_eq!(stored.files_dir.as_deref(), Some(moved_text.as_str()));
    assert_eq!(stored.save_path.as_deref(), Some(moved_text.as_str()));
    assert_eq!(stored.storage_dir, before.storage_dir);
    let view = rig.service.torrent(job_id).await.unwrap();
    assert_eq!(view.files_dir.as_deref(), Some(moved_text.as_str()));
    assert_eq!(view.paths().0, moved_text);
    assert_eq!(
        (view.state, view.have_bytes),
        (TorrentState::Seeding, data.len() as u64)
    );
    rig.service.stop().await;
}

// qBittorrent `TorrentImpl::moveStorage`, a user's move under
// `MoveStorageMode::KeepExistingFiles` (libtorrent `dont_replace`): a file
// already at the new location stays, the torrent's own copy stays where it
// was, the record follows the move, and the torrent is checked again from
// its new place (libtorrent `torrent::on_storage_moved`), which finds the
// file there is not its own.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_move_onto_a_file_already_there_keeps_it_and_checks_again() {
    let (metainfo, data) = placed_torrent();
    let rig = rig(set_clock());
    on_loopback(&rig.routes);
    rig.service.open().await.unwrap();
    let save = rig.dir.path().join("flat");
    std::fs::create_dir_all(&save).unwrap();
    std::fs::write(save.join("a.bin"), &data).unwrap();
    let (job_id, _, mut changes) = completed_placed(
        &rig,
        metainfo,
        Placement {
            save_path: Some(save.to_string_lossy().into_owned()),
            layout: Some(ContentLayout::NoSubfolder),
            ..Placement::default()
        },
    )
    .await;

    let taken = rig.dir.path().join("taken");
    let taken_text = taken.to_string_lossy().into_owned();
    std::fs::create_dir_all(&taken).unwrap();
    std::fs::write(taken.join("a.bin"), b"theirs").unwrap();
    rig.service
        .set_location(job_id, taken_text.clone())
        .await
        .unwrap();
    assert_eq!(std::fs::read(taken.join("a.bin")).unwrap(), b"theirs");
    assert_eq!(std::fs::read(save.join("a.bin")).unwrap(), data);
    let stored = rig.db.get_torrent(job_id).unwrap().unwrap();
    assert_eq!(stored.files_dir.as_deref(), Some(taken_text.as_str()));
    assert_eq!(stored.save_path.as_deref(), Some(taken_text.as_str()));

    // It had every piece; the check from the new place finds none.
    let view = view_once(&rig.service, &mut changes, job_id, |view| {
        view.have_bytes == 0 && view.state != TorrentState::CheckingFiles
    })
    .await;
    assert_eq!(view.files_dir.as_deref(), Some(taken_text.as_str()));
    rig.service.stop().await;
}

// qBittorrent `TorrentImpl::handleTorrentFinished`: a torrent laid out with
// no folder of its own that downloaded under its download path moves under
// its save path once complete, over a file of the same name already there
// (`adjustStorageLocation`, `MoveStorageMode::Overwrite`), and its save path
// and files directory follow.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_complete_torrent_without_its_folder_moves_from_its_download_path() {
    let (metainfo, data) = placed_torrent();
    let rig = rig(set_clock());
    on_loopback(&rig.routes);
    rig.service.open().await.unwrap();
    let download = rig.dir.path().join("incoming");
    std::fs::create_dir_all(&download).unwrap();
    std::fs::write(download.join("a.bin"), &data).unwrap();
    let save = rig.dir.path().join("done");
    std::fs::create_dir_all(&save).unwrap();
    std::fs::write(save.join("a.bin"), b"older").unwrap();
    let save_text = save.to_string_lossy().into_owned();

    let (job_id, _, mut changes) = completed_placed(
        &rig,
        metainfo,
        Placement {
            save_path: Some(save_text.clone()),
            download_path: Some(download.to_string_lossy().into_owned()),
            layout: Some(ContentLayout::NoSubfolder),
            ..Placement::default()
        },
    )
    .await;
    let view = view_once(&rig.service, &mut changes, job_id, |view| {
        view.files_dir.as_deref() == Some(save_text.as_str())
    })
    .await;
    assert_eq!(std::fs::read(save.join("a.bin")).unwrap(), data);
    assert!(!download.join("a.bin").exists());
    let stored = rig.db.get_torrent(job_id).unwrap().unwrap();
    assert_eq!(stored.files_dir.as_deref(), Some(save_text.as_str()));
    assert_eq!(stored.save_path.as_deref(), Some(save_text.as_str()));
    assert_eq!(stored.move_to, None);
    assert_eq!(
        (view.state, view.have_bytes),
        (TorrentState::Seeding, data.len() as u64)
    );
    rig.service.stop().await;
}

// qBittorrent `SessionImpl::initLoadTorrentParams`: a relative save path is
// under the default save path, and a missing one is the default save path
// itself; a relative download path is under the save path when there is no
// default download path, and an absolute one stands.
#[test]
fn relative_paths_resolve_under_the_default_save_path() {
    let default = Path::new("/complete/tv");
    let placed = |save: Option<&str>, download: Option<&str>| {
        let placement = absolute_placement(
            &Placement {
                save_path: save.map(str::to_owned),
                download_path: download.map(str::to_owned),
                layout: Some(ContentLayout::NoSubfolder),
                seed_mode: false,
            },
            default,
        );
        assert_eq!(placement.layout, Some(ContentLayout::NoSubfolder));
        (placement.save_path, placement.download_path)
    };
    let some = |path: &str| Some(path.to_owned());
    assert_eq!(
        placed(Some("shows"), Some("incoming")),
        (
            some("/complete/tv/shows"),
            some("/complete/tv/shows/incoming")
        )
    );
    assert_eq!(
        placed(None, Some("incoming")),
        (some("/complete/tv"), some("/complete/tv/incoming"))
    );
    assert_eq!(
        placed(None, Some("/scratch")),
        (some("/complete/tv"), some("/scratch"))
    );
    assert_eq!(placed(None, None), (None, None));
    assert_eq!(
        placed(Some("/data/shows"), Some("incoming")),
        (some("/data/shows"), some("/data/shows/incoming"))
    );
    assert_eq!(
        placed(Some("shows"), Some("/scratch")),
        (some("/complete/tv/shows"), some("/scratch"))
    );
}

// A torrent added with relative paths keeps them absolute, so the move
// under its save path once complete finds where to go.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_torrent_added_with_relative_paths_keeps_them_absolute() {
    let (metainfo, data) = placed_torrent();
    let rig = rig(set_clock());
    on_loopback(&rig.routes);
    rig.service.open().await.unwrap();
    let complete = rig.dir.path().join("complete");
    let save = complete.join("shows");
    let download = save.join("incoming");
    std::fs::create_dir_all(&download).unwrap();
    std::fs::write(download.join("a.bin"), &data).unwrap();
    let save_text = save.to_string_lossy().into_owned();

    let (job_id, _, mut changes) = completed_placed(
        &rig,
        metainfo,
        Placement {
            save_path: Some("shows".to_owned()),
            download_path: Some("incoming".to_owned()),
            layout: Some(ContentLayout::NoSubfolder),
            ..Placement::default()
        },
    )
    .await;
    let view = view_once(&rig.service, &mut changes, job_id, |view| {
        view.files_dir.as_deref() == Some(save_text.as_str())
    })
    .await;
    assert_eq!(std::fs::read(save.join("a.bin")).unwrap(), data);
    assert!(!download.join("a.bin").exists());
    let stored = rig.db.get_torrent(job_id).unwrap().unwrap();
    assert_eq!(stored.save_path.as_deref(), Some(save_text.as_str()));
    assert_eq!(view.have_bytes, data.len() as u64);
    rig.service.stop().await;
}

// qBittorrent `SessionImpl::initLoadTorrentParams`: a torrent added with a
// download path and no save path takes the default save path, here its
// category's directory, and moves there once complete.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_download_path_without_a_save_path_moves_to_the_default_save_path() {
    let (metainfo, data) = placed_torrent();
    let rig = rig(set_clock());
    on_loopback(&rig.routes);
    rig.service.open().await.unwrap();
    let complete = rig.dir.path().join("complete");
    let complete_text = complete.to_string_lossy().into_owned();
    let download = rig.dir.path().join("incoming");
    std::fs::create_dir_all(&download).unwrap();
    std::fs::write(download.join("a.bin"), &data).unwrap();

    let (job_id, _, mut changes) = completed_placed(
        &rig,
        metainfo,
        Placement {
            download_path: Some(download.to_string_lossy().into_owned()),
            layout: Some(ContentLayout::NoSubfolder),
            ..Placement::default()
        },
    )
    .await;
    let view = view_once(&rig.service, &mut changes, job_id, |view| {
        view.files_dir.as_deref() == Some(complete_text.as_str())
    })
    .await;
    assert_eq!(std::fs::read(complete.join("a.bin")).unwrap(), data);
    assert!(!download.join("a.bin").exists());
    let stored = rig.db.get_torrent(job_id).unwrap().unwrap();
    assert_eq!(stored.save_path.as_deref(), Some(complete_text.as_str()));
    assert_eq!(stored.move_to, None);
    assert_eq!(view.have_bytes, data.len() as u64);
    rig.service.stop().await;
}

// libtorrent's `verified_pieces` in resume data: a torrent still in seed mode
// after a restart takes back the pieces that passed their check before, and
// serves them without a second one; leaving seed mode forgets them.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_seed_mode_torrent_keeps_its_checked_pieces_through_a_restart() {
    let (metainfo, data) = placed_torrent();
    let rig = rig(set_clock());
    on_loopback(&rig.routes);
    rig.service.open().await.unwrap();
    let save = rig.dir.path().join("seeded");
    std::fs::create_dir_all(save.join("placed")).unwrap();
    std::fs::write(save.join("placed/a.bin"), &data).unwrap();
    let (job_id, _) = seeding_placed(
        &rig,
        metainfo,
        Placement {
            save_path: Some(save.to_string_lossy().into_owned()),
            seed_mode: true,
            ..Placement::default()
        },
    )
    .await;
    assert_eq!(rig.db.torrent_seed_verified(job_id).unwrap(), None);
    // Pieces 0 and 2 of 4 passed, as the running tick keeps them.
    let passed = [0b1010_0000u8];
    rig.db.set_torrent_seed_verified(job_id, &passed).unwrap();
    rig.service.stop().await;

    let restarted = service_on(&rig.db, rig.dir.path(), &rig.routes, job_env(), set_clock());
    reopen(&restarted).await;
    let handle = handle_of(&restarted, job_id).await;
    assert!(handle.seed_mode());
    let verified = handle.seed_verified().unwrap();
    assert_eq!(verified.iter_ones().collect::<Vec<_>>(), [0, 2]);
    restarted.tick().await;
    assert_eq!(
        rig.db.torrent_seed_verified(job_id).unwrap(),
        Some(passed.to_vec())
    );

    rig.db.clear_torrent_seed_mode(job_id).unwrap();
    assert_eq!(rig.db.torrent_seed_verified(job_id).unwrap(), None);
    assert!(!rig.db.get_torrent(job_id).unwrap().unwrap().seed_mode);
    restarted.stop().await;
}

// libtorrent `torrent_flags::seed_mode`, as qBittorrent `seedMode` sets it: a
// torrent whose files are complete where it is added seeds them at once,
// and its trackers hear it downloaded nothing, as they first hear of it and
// after.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_seed_mode_add_of_complete_files_announces_nothing_downloaded() {
    use tokio::io::AsyncWriteExt;

    let (metainfo, data) = placed_torrent();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let url = format!("http://{}/announce", listener.local_addr().unwrap());
    let rig = rig(set_clock());
    on_loopback(&rig.routes);
    rig.service.open().await.unwrap();
    let save = rig.dir.path().join("seeded");
    std::fs::create_dir_all(save.join("placed")).unwrap();
    std::fs::write(save.join("placed/a.bin"), &data).unwrap();

    let (job_id, view) = seeding_placed(
        &rig,
        metainfo,
        Placement {
            save_path: Some(save.to_string_lossy().into_owned()),
            seed_mode: true,
            ..Placement::default()
        },
    )
    .await;
    let total = data.len() as u64;
    assert_eq!((view.have_bytes, view.downloaded_bytes), (total, 0));
    assert!(handle_of(&rig.service, job_id).await.seed_mode());

    tracked(&rig.service, job_id, &url).await;
    let answer = {
        let body = "d8:intervali1800e5:peers0:e";
        format!(
            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
            body.len()
        )
    };
    let (mut started, request) = heard_request(&listener).await;
    assert!(request.contains("&downloaded=0&"), "{request}");
    assert!(request.contains("&left=0&"), "{request}");
    started.write_all(answer.as_bytes()).await.unwrap();
    trackers_once(&rig.service, job_id, |announces| {
        !announces[0].in_flight
            && announces[0]
                .last
                .as_ref()
                .is_some_and(|last| last.result.is_ok())
    })
    .await;

    rig.service.reannounce(job_id).await.unwrap();
    let (mut next, request) = heard_request(&listener).await;
    assert!(request.contains("&downloaded=0&"), "{request}");
    assert!(request.contains("&left=0&"), "{request}");
    next.write_all(answer.as_bytes()).await.unwrap();
    assert_eq!(std::fs::read(save.join("placed/a.bin")).unwrap(), data);
    rig.service.stop().await;
}
