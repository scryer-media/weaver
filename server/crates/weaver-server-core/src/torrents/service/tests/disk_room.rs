// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A torrent whose write finds no room waits for room on the disk by the
// wait a usenet job held for a full disk shares: in upload mode, seeding,
// shown waiting for disk space, until a reading shows room made or its
// user resumes it. One with bytes to fetch whose disk has no room over the
// reserve is kept from starting by the check a usenet job starts by; a
// complete one starts and seeds.

use std::path::Path;
use std::sync::atomic::AtomicU64;

use super::*;
use crate::jobs::handle::DISK_SPACE_WAIT_REASON;
use crate::operations::disk::{DiskProbeError, DiskSpace, StorageCapacity, StorageRoot};
use crate::operations::room_wait::Disk;

// What the disk keeps free, the room it reads over that, and a sampler
// that reads only when asked.
const RESERVE: u64 = 100;
const PLENTY: u64 = 1 << 30;
const NEVER: Duration = Duration::from_secs(365 * 24 * 60 * 60);

struct OnDisk {
    rig: Rig,
    capacity: Arc<StorageCapacity>,
    job_id: JobId,
    seeding: Arc<Session>,
    changes: watch::Receiver<u64>,
    _seeds: tempfile::TempDir,
}

// A torrent of four 16 KiB pieces fetched from a loopback seeder, on a disk
// that reads `room` bytes over a reserve of `RESERVE`.
async fn on_disk(room: &Arc<AtomicU64>) -> OnDisk {
    let read = Arc::clone(room);
    on_disks(
        move |_: &Path| {
            Ok(DiskSpace {
                total_bytes: 1 << 40,
                available_bytes: RESERVE + read.load(Ordering::Acquire),
            })
        },
        None,
        None,
        |_| {},
    )
    .await
}

// The torrent `on_disk` fetches, on disks `probe` reads: the roots under
// `roots`, else all in the seeder's directory, saved under `save` when
// given. `before` sees the disks before the torrent is added.
async fn on_disks(
    probe: impl Fn(&Path) -> Result<DiskSpace, DiskProbeError> + Send + Sync + 'static,
    roots: Option<[std::path::PathBuf; 3]>,
    save: Option<std::path::PathBuf>,
    before: impl FnOnce(&StorageCapacity),
) -> OnDisk {
    use crate::torrents::net::{LegKind, LegSockets};

    let data: Vec<u8> = (0..65_536u32).map(|at| (at % 251) as u8).collect();
    let files = [("set/a.bin", data.as_slice())];
    let plain = metainfo("set", &files, 16_384, false);
    let seeds = tempfile::tempdir().unwrap();
    let seeding = seeder(seeds.path(), &plain, &files).await;
    let std::net::SocketAddr::V4(peer) = seeding.listen_addr().unwrap() else {
        panic!("the seeder listens on IPv4 loopback");
    };
    let (bytes, url) = (plain.to_vec(), tracker(peer).await);

    let roots = roots.unwrap_or_else(|| [0, 1, 2].map(|_| seeds.path().to_path_buf()));
    let capacity = Arc::new(StorageCapacity::with_probe(roots, NEVER, Arc::new(probe)));
    for root in StorageRoot::ALL {
        capacity.wait_for_probes(root, 1);
    }
    before(&capacity);
    let rig = rig_with(set_clock(), disk_env(&capacity));
    let loopback = Arc::new(weaver_tunnel::pipe::Revocable::new(Arc::new(
        weaver_tunnel::pipe::Egress::new(
            4,
            weaver_tunnel::egress::SocketEgress::SourceAddress("127.0.0.1".parse().unwrap()),
            // No connect deadline: the test waits on what it checks.
            Duration::MAX,
        ),
    )));
    rig.routes.set_leg(
        LegKind::Ladder,
        Arc::new(LegSockets::new(loopback, &LegKind::Ladder)),
    );
    let handle = queue();
    let changes = handle.state().subscribe_job_changes();
    rig.service.publish_to(handle);
    rig.service.open().await.unwrap();
    let mut add = request(AddSource::Metainfo(bytes));
    add.placement.save_path = save.map(|save| save.to_string_lossy().into_owned());
    let job_id = rig.service.add_opened(add).await.unwrap();
    tracked(&rig.service, job_id, &url).await;
    OnDisk {
        rig,
        capacity,
        job_id,
        seeding,
        changes,
        _seeds: seeds,
    }
}

// The jobs' budget on `capacity`'s disks, with a reserve of `RESERVE`.
fn disk_env(capacity: &Arc<StorageCapacity>) -> JobEnv {
    let mut limits = ExtractionLimits::unbounded();
    limits.min_free_bytes = RESERVE;
    JobEnv {
        free_disk: capacity.reader(StorageRoot::Complete),
        capacity: Some(Arc::clone(capacity)),
        limits: Arc::new(limits),
        ..JobEnv::unbounded()
    }
}

// Until the torrent waits for room, or no longer does.
async fn waits_for_room(service: &TorrentService, job_id: JobId, waits: bool) {
    let mut moved = service.inner.disk_waits_moved.subscribe();
    moved
        .wait_for(|_| locked(&service.inner.disk_waits).contains_key(&job_id) == waits)
        .await
        .unwrap();
}

// Until the torrent waits for room in upload mode: its handle, then.
async fn held(disk: &OnDisk) -> Arc<ManagedTorrent> {
    let torrent = handle_of(&disk.rig.service, disk.job_id).await;
    torrent.wait_until_initialized().await.unwrap();
    torrent.live().unwrap().wait_for_upload_mode(true).await;
    waits_for_room(&disk.rig.service, disk.job_id, true).await;
    torrent
}

// A piece that would eat into the reserve finds no room: the torrent goes
// upload-only and keeps its run, shown waiting for disk space as a usenet
// job is, and only the disk's ledger is waited on. Room made on the disk
// wakes it at the next reading, with no click.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_full_disk_holds_the_torrent_seeding_until_a_reading_shows_room() {
    // Room to start over the reserve, none for a piece.
    let room = Arc::new(AtomicU64::new(1));
    let disk = on_disk(&room).await;
    let service = &disk.rig.service;
    let torrent = held(&disk).await;
    let run = torrent.live().unwrap();
    assert!(!torrent.is_paused(), "the run goes on, seeding");
    assert!(run.upload_mode());
    let view = service.torrent(disk.job_id).await.unwrap();
    assert_eq!(view.state, TorrentState::Stalled);
    assert_eq!(view.detail.as_deref(), Some(DISK_SPACE_WAIT_REASON));
    service.publish_job(disk.job_id).await;
    assert_eq!(
        service.inner.queue.lock().unwrap().as_ref().map(|queue| {
            queue
                .state()
                .get_job(disk.job_id)
                .and_then(|job| job.download_wait_reason)
        }),
        Some(Some(DISK_SPACE_WAIT_REASON.to_owned()))
    );

    // Room is made: the next reading lets it fetch again. The run leaves
    // upload mode before its wait ends. Whether it completes then rests on
    // the seeder taking it back, which is the engine's to time.
    room.store(PLENTY, Ordering::Release);
    disk.capacity.request_refresh(StorageRoot::Complete);
    waits_for_room(service, disk.job_id, false).await;
    assert!(!run.upload_mode());
    assert!(!torrent.is_paused());
    assert!(!disk.capacity.ledger(StorageRoot::Complete).waited_on());
    service.stop().await;
    disk.seeding.stop().await;
}

// A resume lets the torrent fetch again at once, whatever the readings
// say, as a usenet job held for a full disk is let go by its user.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_resume_lets_a_torrent_waiting_for_room_fetch_again() {
    // Room to start over the reserve, none for a piece.
    let room = Arc::new(AtomicU64::new(1));
    let disk = on_disk(&room).await;
    let service = &disk.rig.service;
    let run = held(&disk).await.live().unwrap();
    // Nothing more comes, so no write finds the disk full again.
    disk.seeding.stop().await;

    service.resume(disk.job_id).await.unwrap();
    assert!(!locked(&service.inner.disk_waits).contains_key(&disk.job_id));
    assert!(!run.upload_mode());
    let view = service.torrent(disk.job_id).await.unwrap();
    assert_ne!(view.detail.as_deref(), Some(DISK_SPACE_WAIT_REASON));
    assert!(!disk.capacity.ledger(StorageRoot::Complete).waited_on());
    service.stop().await;
}

// Whether the torrent is in no session, or paused in one.
async fn not_running(service: &TorrentService, job_id: JobId) -> bool {
    let live = service.inner.live.lock().await;
    live[&job_id]
        .attached
        .as_ref()
        .is_none_or(|attached| attached.handle.is_paused())
}

// A torrent with bytes to fetch whose turn comes while the disk has no room
// over the reserve stays paused, waiting in the queue for disk space as a
// usenet job does, and the disk's ledger is waited on so readings come. A
// reading with room lets it start, with no click, and it completes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_short_disk_keeps_a_torrent_with_bytes_to_fetch_from_starting() {
    let room = Arc::new(AtomicU64::new(0));
    let mut disk = on_disk(&room).await;
    let service = &disk.rig.service;
    let view = view_once(service, &mut disk.changes, disk.job_id, |view| {
        view.detail.as_deref() == Some(DISK_SPACE_WAIT_REASON)
    })
    .await;
    assert_eq!(view.state, TorrentState::Queued);
    assert_eq!(view.downloaded_bytes, 0);
    assert!(not_running(service, disk.job_id).await, "it does not start");
    assert!(locked(&service.inner.start_held).iter().any(|held| *held));
    assert!(disk.capacity.ledger(StorageRoot::Complete).waited_on());

    room.store(PLENTY, Ordering::Release);
    disk.capacity.request_refresh(StorageRoot::Complete);
    view_once(service, &mut disk.changes, disk.job_id, |view| {
        view.detail.as_deref() != Some(DISK_SPACE_WAIT_REASON)
    })
    .await;
    let torrent = handle_of(service, disk.job_id).await;
    torrent.wait_until_completed().await.unwrap();
    assert!(!locked(&service.inner.start_held).iter().any(|held| *held));
    assert!(!disk.capacity.ledger(StorageRoot::Complete).waited_on());
    service.stop().await;
    disk.seeding.stop().await;
}

// A complete torrent starts and seeds on a disk with no room over the
// reserve: it has nothing to fetch, and the check holds nothing.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_short_disk_starts_a_complete_torrent_seeding() {
    let (metainfo, data) = placed_torrent();
    let dir = tempfile::tempdir().unwrap();
    let probe = Arc::new(move |_: &Path| {
        Ok(DiskSpace {
            total_bytes: 1 << 40,
            available_bytes: RESERVE,
        })
    });
    let roots = [0, 1, 2].map(|_| dir.path().to_path_buf());
    let capacity = Arc::new(StorageCapacity::with_probe(roots, NEVER, probe));
    for root in StorageRoot::ALL {
        capacity.wait_for_probes(root, 1);
    }
    let mut limits = ExtractionLimits::unbounded();
    limits.min_free_bytes = RESERVE;
    let env = JobEnv {
        free_disk: capacity.reader(StorageRoot::Complete),
        capacity: Some(Arc::clone(&capacity)),
        limits: Arc::new(limits),
        ..JobEnv::unbounded()
    };
    let rig = rig_with(set_clock(), env);
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
    assert_eq!(view.state, TorrentState::Seeding);
    assert!(!handle_of(&rig.service, job_id).await.is_paused());
    assert!(
        !locked(&rig.service.inner.start_held)
            .iter()
            .any(|held| *held)
    );
    rig.service.stop().await;
}

// What the torrent's disk wait is: on its ledger, and which.
fn ledger_wait(service: &TorrentService, job_id: JobId) -> Option<u64> {
    locked(&service.inner.disk_waits)
        .get(&job_id)
        .and_then(|wait| match &wait.how {
            Waiting::Ledger(wait, _) => Some(wait.needs),
            _ => None,
        })
}

// Until the torrent's disk is known: where its pieces land.
async fn placed(service: &TorrentService, job_id: JobId) -> crate::torrents::intake::Place {
    let mut moved = service.inner.disk_waits_moved.subscribe();
    loop {
        let place = service.inner.live.lock().await[&job_id]
            .intake
            .as_ref()
            .and_then(|intake| intake.room().place());
        if let Some(place) = place {
            return place;
        }
        moved.changed().await.unwrap();
    }
}

// A torrent saved on a disk with no room, while the complete root's has
// plenty: its pieces book on its own disk's ledger, it waits there, and a
// reading of that disk with room wakes it. Nothing is booked or waited on
// on the complete root's.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_torrent_saved_on_another_disk_waits_on_that_disk() {
    let complete = tempfile::tempdir().unwrap();
    let working = tempfile::tempdir().unwrap();
    let save = working.path().join("saved");
    std::fs::create_dir_all(&save).unwrap();
    crate::runtime::fs::test_devices::assign(complete.path(), 71_001);
    crate::runtime::fs::test_devices::assign(working.path(), 71_002);
    // Room to start over the reserve on the working disk, none for a piece.
    let room = Arc::new(AtomicU64::new(1));
    let read = Arc::clone(&room);
    let full = working.path().to_path_buf();
    let disk = on_disks(
        move |path: &Path| {
            let over = if path.starts_with(&full) {
                read.load(Ordering::Acquire)
            } else {
                PLENTY
            };
            Ok(DiskSpace {
                total_bytes: 1 << 40,
                available_bytes: RESERVE + over,
            })
        },
        Some([
            complete.path().to_path_buf(),
            working.path().to_path_buf(),
            complete.path().to_path_buf(),
        ]),
        Some(save.clone()),
        |_| {},
    )
    .await;
    let service = &disk.rig.service;
    let place = placed(service, disk.job_id).await;
    assert_eq!(place.disk, Disk::Root(StorageRoot::Intermediate));
    let run = held(&disk).await.live().unwrap();
    assert_eq!(ledger_wait(service, disk.job_id), Some(16_384));
    assert!(disk.capacity.ledger(StorageRoot::Intermediate).waited_on());
    let complete_ledger = disk.capacity.ledger(StorageRoot::Complete);
    assert!(!complete_ledger.waited_on());
    assert_eq!(complete_ledger.promised(), 0);

    // Room on its own disk wakes it.
    room.store(PLENTY, Ordering::Release);
    disk.capacity.request_refresh(StorageRoot::Intermediate);
    waits_for_room(service, disk.job_id, false).await;
    assert!(!run.upload_mode());
    assert!(!disk.capacity.ledger(StorageRoot::Intermediate).waited_on());
    assert_eq!(complete_ledger.promised(), 0);
    service.stop().await;
    disk.seeding.stop().await;
}

// A piece the ledger refuses because what other writers promised leaves
// no room over the reserve waits on the ledger, as a usenet job's refused
// booking does. Those writers give their promises back: the disk's free
// figure never moves, and the torrent is woken by the ledger's ring, its
// piece promised to it at once. The seeds it let go while it only
// uploaded are dialled again at once, and it completes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_torrent_held_by_promises_wakes_when_they_are_released() {
    // 70 KiB over the reserve, 60 of them promised elsewhere: room to start,
    // none for a 16 KiB piece. Given back, all four pieces fit.
    let room = Arc::new(AtomicU64::new(70 << 10));
    let disk = on_disk_promised(&room, 60 << 10).await;
    let service = &disk.rig.service;
    let ledger = disk.capacity.ledger(StorageRoot::Complete);
    let torrent = held(&disk).await;
    let run = torrent.live().unwrap();
    assert_eq!(ledger_wait(service, disk.job_id), Some(16_384));

    ledger.release(60 << 10);
    run.wait_for_upload_mode(false).await;
    waits_for_room(service, disk.job_id, false).await;
    torrent.wait_until_completed().await.unwrap();
    assert!(!run.upload_mode());
    assert!(!locked(&service.inner.disk_waits).contains_key(&disk.job_id));
    assert!(!ledger.waited_on());
    service.stop().await;
    disk.seeding.stop().await;
}

// The wake ends upload mode. A refusal heard once it has, from a write the
// run made before the wake, installs no wait: nothing could end one while
// the run is out of upload mode. It is heard again only if the run enters
// upload mode, which a refusal from before the wake cannot make it do.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_refusal_heard_after_the_wake_installs_no_wait() {
    let room = Arc::new(AtomicU64::new(70 << 10));
    let disk = on_disk_promised(&room, 60 << 10).await;
    let service = &disk.rig.service;
    let ledger = disk.capacity.ledger(StorageRoot::Complete);
    let run = held(&disk).await.live().unwrap();
    // The seeder goes first, so no piece written after the wake can be
    // refused and enter upload mode on its own.
    disk.seeding.stop().await;
    ledger.release(60 << 10);
    run.wait_for_upload_mode(false).await;
    waits_for_room(service, disk.job_id, false).await;
    let intake = service.inner.live.lock().await[&disk.job_id]
        .intake
        .clone()
        .unwrap();
    // What the wake handed the torrent goes back, and the room is promised
    // elsewhere: the piece below books on the ledger and is refused.
    intake.room().give_back();
    ledger.promise(1 << 40);
    let piece = bytes::Bytes::from(vec![0u8; 16_384]);
    assert!(weaver_torrent::storage::write_refused(
        &intake.verified(0, &piece).unwrap_err()
    ));
    service
        .inner
        .settled
        .subscribe()
        .wait_for(|_| locked(&service.inner.entering).contains_key(&disk.job_id))
        .await
        .unwrap();
    assert!(!run.upload_mode());
    assert!(!locked(&service.inner.disk_waits).contains_key(&disk.job_id));
    let view = service.torrent(disk.job_id).await.unwrap();
    assert_ne!(view.detail.as_deref(), Some(DISK_SPACE_WAIT_REASON));
    ledger.release(1 << 40);
    assert!(!ledger.waited_on());
    service.stop().await;
}

// `on_disk`, with `promised` of the room promised on the complete root's
// ledger by another writer before the torrent is added.
async fn on_disk_promised(room: &Arc<AtomicU64>, promised: u64) -> OnDisk {
    let read = Arc::clone(room);
    on_disks(
        move |_: &Path| {
            Ok(DiskSpace {
                total_bytes: 1 << 40,
                available_bytes: RESERVE + read.load(Ordering::Acquire),
            })
        },
        None,
        None,
        |capacity| capacity.ledger(StorageRoot::Complete).promise(promised),
    )
    .await
}

// Two torrents that never fetched a byte on a disk with no room over the
// reserve are kept from starting, as a usenet job that never downloaded
// is. One that downloaded before, by the bytes its ledger counts, starts
// at the next pass: what it writes next waits for room as a write that
// finds none does.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_short_disk_holds_a_torrent_that_never_downloaded_and_starts_one_that_did() {
    let room = Arc::new(AtomicU64::new(0));
    let mut disk = on_disk(&room).await;
    let service = &disk.rig.service;
    let second = service
        .add_opened(request(AddSource::Metainfo(plain("other", 1))))
        .await
        .unwrap();
    for job_id in [disk.job_id, second] {
        // A torrent shows its start from what was saved first, and nothing
        // is published as that start ends: the view is read once it has.
        let torrent = loop {
            disk.changes.borrow_and_update();
            let attached = service.inner.live.lock().await[&job_id]
                .attached
                .as_ref()
                .map(|attached| Arc::clone(&attached.handle));
            if let Some(torrent) = attached {
                break torrent;
            }
            disk.changes.changed().await.unwrap();
        };
        torrent.wait_until_initialized().await.unwrap();
        view_once(service, &mut disk.changes, job_id, |view| {
            view.detail.as_deref() == Some(DISK_SPACE_WAIT_REASON)
        })
        .await;
        assert!(not_running(service, job_id).await);
    }
    // The second fetched half of itself in a run before this one.
    service
        .inner
        .live
        .lock()
        .await
        .get_mut(&second)
        .unwrap()
        .counters
        .downloaded_bytes = 8_192;
    service.apply_queue().await;
    let view = service.torrent(second).await.unwrap();
    assert_ne!(view.detail.as_deref(), Some(DISK_SPACE_WAIT_REASON));
    assert!(
        !not_running(service, second).await,
        "the one that downloaded starts"
    );
    let view = service.torrent(disk.job_id).await.unwrap();
    assert_eq!(view.detail.as_deref(), Some(DISK_SPACE_WAIT_REASON));
    assert!(
        not_running(service, disk.job_id).await,
        "the other stays held"
    );
    service.stop().await;
    disk.seeding.stop().await;
}

// The wait for room ends with the run it began in: no new run, and no
// torrent gone, keeps it. Nothing stays promised or waited on, and nothing
// its intake said is kept for a torrent removed.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_wait_for_room_ends_with_its_run() {
    for how in ["remove", "pause", "recheck", "route move"] {
        let room = Arc::new(AtomicU64::new(1));
        let disk = on_disk(&room).await;
        let service = &disk.rig.service;
        let job_id = disk.job_id;
        held(&disk).await;
        // Nothing more comes, so no write finds the disk full again.
        disk.seeding.stop().await;
        match how {
            "remove" => service.remove(job_id, false, true).await.unwrap(),
            "pause" => service.pause(job_id).await.unwrap(),
            "recheck" => service.recheck(job_id).await.unwrap(),
            _ => {
                disk.rig.routes.route(TorrentScope::Category(1), 9);
                service
                    .set_category(job_id, Some("tv".into()))
                    .await
                    .unwrap();
            }
        }
        // A settle after the run ended takes whatever its intake said
        // before: dropped for a torrent gone, and held for no run that is
        // over.
        let mut settled = service.inner.settled.subscribe();
        let before = *settled.borrow_and_update();
        service.inner.signal.mark(job_id);
        settled
            .wait_for(|settles| *settles > before && !service.inner.signal.holds_writes_of(job_id))
            .await
            .unwrap();
        assert!(
            !locked(&service.inner.disk_waits).contains_key(&job_id),
            "{how}: the wait ends"
        );
        let ledger = disk.capacity.ledger(StorageRoot::Complete);
        assert!(!ledger.waited_on(), "{how}");
        assert_eq!(ledger.promised(), 0, "{how}");
        if let Some(torrent) = service.inner.live.lock().await.get(&job_id) {
            assert_eq!(torrent.disk_held, None, "{how}");
            let room = torrent.intake.as_ref().unwrap().room();
            assert_eq!(room.in_hand(), 0, "{how}");
        }
        service.stop().await;
    }
}

// A torrent that downloaded in a run before, restarted, books its pieces
// on its disk's ledger from the first: its disk is looked up as its store
// is handed over, and no piece is let through before it is known. With no
// room for a piece over the reserve, its first piece waits on the ledger
// and nothing of it lands.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_restarted_torrent_that_downloaded_books_its_pieces_on_its_disk() {
    let room = Arc::new(AtomicU64::new(1));
    let disk = on_disk(&room).await;
    let job_id = disk.job_id;
    held(&disk).await;
    // It fetched half of itself in a run before this one.
    disk.rig
        .service
        .inner
        .live
        .lock()
        .await
        .get_mut(&job_id)
        .unwrap()
        .counters
        .downloaded_bytes = 32_768;
    disk.rig.service.stop().await;

    // The process starts again on the same disks, read afresh.
    let read = Arc::clone(&room);
    let probe = Arc::new(move |_: &Path| {
        Ok(DiskSpace {
            total_bytes: 1 << 40,
            available_bytes: RESERVE + read.load(Ordering::Acquire),
        })
    });
    let roots = [0, 1, 2].map(|_| disk._seeds.path().to_path_buf());
    let capacity = Arc::new(StorageCapacity::with_probe(roots, NEVER, probe));
    for root in StorageRoot::ALL {
        capacity.wait_for_probes(root, 1);
    }
    let again = service_on(
        &disk.rig.db,
        disk.rig.dir.path(),
        &disk.rig.routes,
        disk_env(&capacity),
        set_clock(),
    );
    again.publish_to(queue());
    reopen(&again).await;
    let torrent = handle_of(&again, job_id).await;
    torrent.wait_until_initialized().await.unwrap();
    assert!(
        again.inner.live.lock().await[&job_id]
            .counters
            .downloaded_bytes
            >= 32_768
    );
    torrent.live().unwrap().wait_for_upload_mode(true).await;
    waits_for_room(&again, job_id, true).await;
    assert_eq!(ledger_wait(&again, job_id), Some(16_384));
    assert_eq!(
        placed(&again, job_id).await.disk,
        Disk::Root(StorageRoot::Complete)
    );
    assert!(capacity.ledger(StorageRoot::Complete).waited_on());
    assert_eq!(
        job_have(&again, job_id).await,
        0,
        "nothing landed unchecked"
    );
    again.stop().await;
    disk.seeding.stop().await;
}

// A torrent that joins its session again, after a pause, keeps the disk
// found for its folder: a piece of its new run books there, refused on
// that disk's ledger with no room over the reserve, with no new look. The
// run is not in upload mode for this refusal, so it is kept to be heard
// once it is.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_torrent_that_joins_again_books_on_the_disk_it_kept() {
    let room = Arc::new(AtomicU64::new(1));
    let disk = on_disk(&room).await;
    let service = &disk.rig.service;
    let job_id = disk.job_id;
    held(&disk).await;
    // Nothing more comes: the piece below is the new run's only one.
    disk.seeding.stop().await;
    let place = placed(service, job_id).await;
    service.pause(job_id).await.unwrap();
    waits_for_room(service, job_id, false).await;
    let looks = disk.capacity.root_looks();

    service.resume(job_id).await.unwrap();
    let torrent = handle_of(service, job_id).await;
    torrent.wait_until_initialized().await.unwrap();
    assert!(!torrent.is_paused());
    let intake = service.inner.live.lock().await[&job_id]
        .intake
        .clone()
        .unwrap();
    let piece = bytes::Bytes::from(vec![0u8; 16_384]);
    assert!(weaver_torrent::storage::write_refused(
        &intake.verified(0, &piece).unwrap_err()
    ));
    let entering = |_: &u64| {
        locked(&service.inner.entering)
            .get(&job_id)
            .map(|(_, no_room)| *no_room)
    };
    service
        .inner
        .settled
        .subscribe()
        .wait_for(|seen| entering(seen).is_some())
        .await
        .unwrap();
    assert_eq!(entering(&0), Some(NoRoom::Ledger(16_384)));
    let kept = placed(service, job_id).await;
    assert_eq!((kept.disk, kept.dir), (place.disk, place.dir));
    assert_eq!(disk.capacity.root_looks(), looks);
    service.stop().await;
}

// A refusal kept while the run was out of upload mode is heard again once
// the run enters it: a seeder named by a new tracker sends a piece, its
// write finds no room, and the run waits on the ledger, shown waiting for
// disk space, with nothing kept aside.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_refusal_kept_aside_is_heard_once_the_run_enters_upload_mode() {
    let room = Arc::new(AtomicU64::new(1));
    let disk = on_disk(&room).await;
    let service = &disk.rig.service;
    let job_id = disk.job_id;
    held(&disk).await;
    // No peer is left until the new seeder below is named.
    disk.seeding.stop().await;
    service.pause(job_id).await.unwrap();
    waits_for_room(service, job_id, false).await;
    service.resume(job_id).await.unwrap();
    let torrent = handle_of(service, job_id).await;
    torrent.wait_until_initialized().await.unwrap();
    let run = torrent.live().unwrap();
    let intake = service.inner.live.lock().await[&job_id]
        .intake
        .clone()
        .unwrap();
    let piece = bytes::Bytes::from(vec![0u8; 16_384]);
    assert!(weaver_torrent::storage::write_refused(
        &intake.verified(0, &piece).unwrap_err()
    ));
    service
        .inner
        .settled
        .subscribe()
        .wait_for(|_| locked(&service.inner.entering).contains_key(&job_id))
        .await
        .unwrap();
    assert!(!run.upload_mode());

    // A seeder of the same torrent, named by a tracker of its own.
    let data: Vec<u8> = (0..65_536u32).map(|at| (at % 251) as u8).collect();
    let files = [("set/a.bin", data.as_slice())];
    let plain = metainfo("set", &files, 16_384, false);
    let seeds = tempfile::tempdir().unwrap();
    let seeding = seeder(seeds.path(), &plain, &files).await;
    let std::net::SocketAddr::V4(peer) = seeding.listen_addr().unwrap() else {
        panic!("the seeder listens on IPv4 loopback");
    };
    let url = tracker(peer).await;
    service.add_trackers(job_id, vec![vec![url]]).await.unwrap();
    run.wait_for_upload_mode(true).await;
    waits_for_room(service, job_id, true).await;
    assert_eq!(ledger_wait(service, job_id), Some(16_384));
    service
        .inner
        .settled
        .subscribe()
        .wait_for(|_| !locked(&service.inner.entering).contains_key(&job_id))
        .await
        .unwrap();
    let view = service.torrent(job_id).await.unwrap();
    assert_eq!(view.detail.as_deref(), Some(DISK_SPACE_WAIT_REASON));
    service.stop().await;
    seeding.stop().await;
}

// A piece refused on the ledger while the place known is that of a folder
// the torrent no longer goes to waits for the disk of its new folder,
// then on that disk's ledger: its run stays in upload mode with a wait,
// never with none. Room made there wakes it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_refusal_for_a_folder_moved_since_waits_for_its_disk_then_its_ledger() {
    let room = Arc::new(AtomicU64::new(1));
    let disk = on_disk(&room).await;
    let service = &disk.rig.service;
    let job_id = disk.job_id;
    let run = held(&disk).await.live().unwrap();
    // Nothing more comes, so no other write finds the disk full.
    disk.seeding.stop().await;
    let moved = disk._seeds.path().join("moved");
    std::fs::create_dir_all(&moved).unwrap();
    {
        let mut live = service.inner.live.lock().await;
        let torrent = live.get_mut(&job_id).unwrap();
        service.end_disk_wait(job_id, Some(torrent));
        torrent.record.files_dir = Some(moved.to_string_lossy().into_owned());
    }
    service.hold_for_room(job_id, NoRoom::Ledger(16_384)).await;
    assert!(run.upload_mode());
    assert!(locked(&service.inner.disk_waits).contains_key(&job_id));
    let mut waits = service.inner.disk_waits_moved.subscribe();
    waits
        .wait_for(|_| ledger_wait(service, job_id) == Some(16_384))
        .await
        .unwrap();
    let place = placed(service, job_id).await;
    assert_eq!(place.dir, moved);
    assert_eq!(place.disk, Disk::Root(StorageRoot::Complete));

    room.store(PLENTY, Ordering::Release);
    disk.capacity.request_refresh(StorageRoot::Complete);
    waits_for_room(service, job_id, false).await;
    assert!(!run.upload_mode());
    service.stop().await;
}

// A torrent whose folder's disk cannot be told, as a mount that fails its
// reads, is never taken to lie off the roots': it stays unplaced and does
// not start, and the complete root's ledger is waited on so a reading
// comes. The look made at a reading after the mount reads again places it
// on the root its folder lies on, and it starts and completes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_torrent_whose_disk_cannot_be_told_waits_unplaced_and_a_later_look_places_it() {
    let complete = tempfile::tempdir().unwrap();
    let working = tempfile::tempdir().unwrap();
    let save = working.path().join("saved");
    std::fs::create_dir_all(&save).unwrap();
    crate::runtime::fs::test_devices::assign(complete.path(), 73_001);
    crate::runtime::fs::test_devices::assign(working.path(), 73_002);
    let refused = crate::runtime::fs::test_devices::refuse(&save);
    let disk = on_disks(
        |_: &Path| {
            Ok(DiskSpace {
                total_bytes: 1 << 40,
                available_bytes: RESERVE + PLENTY,
            })
        },
        Some([
            complete.path().to_path_buf(),
            working.path().to_path_buf(),
            complete.path().to_path_buf(),
        ]),
        Some(save.clone()),
        |_| {},
    )
    .await;
    let service = &disk.rig.service;
    let job_id = disk.job_id;
    let intake = service.inner.live.lock().await[&job_id]
        .intake
        .clone()
        .unwrap();
    // The look at the handover failed: it is listed to look again.
    let failed =
        |_: &u64| locked(&service.inner.relook).jobs.contains(&job_id) && !intake.room().placing();
    let mut moved = service.inner.disk_waits_moved.subscribe();
    moved.wait_for(failed).await.unwrap();
    assert!(
        intake.room().place().is_none(),
        "never placed off the roots'"
    );
    assert!(not_running(service, job_id).await, "it does not start");
    assert!(disk.capacity.ledger(StorageRoot::Complete).waited_on());

    // The mount reads again. A look begun before may still fail: once
    // none is out, a reading looks again.
    drop(refused);
    moved
        .wait_for(|seen| intake.room().place().is_some() || failed(seen))
        .await
        .unwrap();
    disk.capacity.request_refresh(StorageRoot::Complete);
    let place = placed(service, job_id).await;
    assert_eq!(place.disk, Disk::Root(StorageRoot::Intermediate));
    assert_eq!(place.dir, save.join("set"));
    handle_of(service, job_id)
        .await
        .wait_until_completed()
        .await
        .unwrap();
    assert!(!locked(&service.inner.relook).waiting);
    assert!(!disk.capacity.ledger(StorageRoot::Complete).waited_on());
    service.stop().await;
    disk.seeding.stop().await;
}

// A folder named while a look for the torrent's disk is out does not start
// a second look: the one out looks the new folder up as it ends, so the
// place found is the new folder's and no look is left marked out.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_folder_named_during_a_look_is_placed_by_that_look() {
    let room = Arc::new(AtomicU64::new(1));
    let disk = on_disk(&room).await;
    let service = &disk.rig.service;
    let job_id = disk.job_id;
    held(&disk).await;
    disk.seeding.stop().await;
    let (before, after) = (
        disk._seeds.path().join("before"),
        disk._seeds.path().join("after"),
    );
    std::fs::create_dir_all(&before).unwrap();
    std::fs::create_dir_all(&after).unwrap();
    let (hold, mut reads) = crate::runtime::fs::test_devices::hold(&before);
    let intake = {
        let live = service.inner.live.lock().await;
        live[&job_id].intake.clone().unwrap()
    };
    assert!(intake.room().aim(&before));
    assert!(intake.room().begin_placing());
    service.spawn_place(job_id, &intake, before.clone(), false);
    assert_eq!(reads.recv().await.unwrap(), before);

    // The folder moves while the look of the one before is held.
    assert!(intake.room().aim(&after));
    assert!(!intake.room().begin_placing(), "one look at a time");
    drop(hold);
    let mut moved = service.inner.disk_waits_moved.subscribe();
    moved
        .wait_for(|_| intake.room().place().is_some() && !intake.room().placing())
        .await
        .unwrap();
    let place = intake.room().place().unwrap();
    assert_eq!(place.dir, after);
    assert_eq!(place.disk, Disk::Root(StorageRoot::Complete));
    service.stop().await;
}

#[test]
fn the_disk_warning_names_the_volume_and_the_waiting() {
    assert_eq!(
        disk_warning(Path::new("/data/complete"), &["a", "b"]),
        "Disk full on /data/complete: waiting a, b"
    );
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
        .add_opened(request(AddSource::Metainfo(bytes)))
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
