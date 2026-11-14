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
use crate::operations::disk::{DiskSpace, StorageCapacity, StorageRoot};

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

    let read = Arc::clone(room);
    let probe = Arc::new(move |_: &Path| {
        Ok(DiskSpace {
            total_bytes: 1 << 40,
            available_bytes: RESERVE + read.load(Ordering::Acquire),
        })
    });
    let roots = [0, 1, 2].map(|_| seeds.path().to_path_buf());
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
    let job_id = rig
        .service
        .add_opened(request(AddSource::Metainfo(bytes)))
        .await
        .unwrap();
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
    assert!(*locked(&service.inner.start_held));
    assert!(disk.capacity.ledger(StorageRoot::Complete).waited_on());

    room.store(PLENTY, Ordering::Release);
    disk.capacity.request_refresh(StorageRoot::Complete);
    view_once(service, &mut disk.changes, disk.job_id, |view| {
        view.detail.as_deref() != Some(DISK_SPACE_WAIT_REASON)
    })
    .await;
    let torrent = handle_of(service, disk.job_id).await;
    torrent.wait_until_completed().await.unwrap();
    assert!(!*locked(&service.inner.start_held));
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
    assert!(!*locked(&rig.service.inner.start_held));
    rig.service.stop().await;
}
