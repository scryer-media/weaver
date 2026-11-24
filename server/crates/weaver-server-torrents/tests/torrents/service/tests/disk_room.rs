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
use crate::torrents::service::disk_room::DiskHeld;
use crate::torrents::status::Stated;
use weaver_server_disk::operations::disk::{
    DiskProbeError, DiskSpace, StorageCapacity, StorageRoot,
};
use weaver_server_disk::operations::room_wait::Disk;

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
    on_disks_keeping(probe, roots, save, before, RESERVE).await
}

// `on_disks`, the jobs keeping `reserve` free.
async fn on_disks_keeping(
    probe: impl Fn(&Path) -> Result<DiskSpace, DiskProbeError> + Send + Sync + 'static,
    roots: Option<[std::path::PathBuf; 3]>,
    save: Option<std::path::PathBuf>,
    before: impl FnOnce(&StorageCapacity),
    reserve: u64,
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
    let rig = rig_with(set_clock(), disk_env_keeping(&capacity, reserve));
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
    rig.service.publish_to(queue());
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
        _seeds: seeds,
    }
}

// The jobs' budget on `capacity`'s disks, with a reserve of `RESERVE`.
fn disk_env(capacity: &Arc<StorageCapacity>) -> JobEnv {
    disk_env_keeping(capacity, RESERVE)
}

// `disk_env`, keeping `reserve` free.
fn disk_env_keeping(capacity: &Arc<StorageCapacity>, reserve: u64) -> JobEnv {
    let mut limits = ExtractionLimits::unbounded();
    limits.min_free_bytes = reserve;
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

// The torrent's disk hold.
async fn disk_held(service: &TorrentService, job_id: JobId) -> Option<DiskHeld> {
    service
        .inner
        .live
        .lock()
        .await
        .get(&job_id)
        .and_then(|torrent| torrent.disk_held)
}

// Until the torrent, kept from starting by a disk with no room for its
// 65,536 bytes, runs wanting none of them: its start is judged with nothing
// to fetch. Its handle, then.
async fn runs_wanting_nothing(disk: &OnDisk) -> Arc<ManagedTorrent> {
    let service = &disk.rig.service;
    view_once(service, disk.job_id, |view| {
        view.stated == Some(Stated::DiskSpace)
    })
    .await;
    service
        .set_file_priorities(disk.job_id, &[(0, FilePriority::Ignored)])
        .await
        .unwrap();
    service.apply_queue().await;
    let torrent = handle_of(service, disk.job_id).await;
    torrent.wait_until_initialized().await.unwrap();
    assert!(!torrent.is_paused(), "it runs, wanting nothing");
    torrent
}

// Until the torrent, running and wanting none of its bytes, wants them
// again on a disk with no room for them: the start rule refuses what it has
// left, its download is held and it seeds, in upload mode, not paused,
// waiting on the start rule's wait. Its handle, then.
async fn held(disk: &OnDisk) -> Arc<ManagedTorrent> {
    let torrent = runs_wanting_nothing(disk).await;
    let service = &disk.rig.service;
    service
        .set_file_priorities(disk.job_id, &[(0, FilePriority::Normal)])
        .await
        .unwrap();
    assert!(torrent.live().unwrap().upload_mode());
    assert!(!torrent.is_paused());
    assert_eq!(
        disk_held(service, disk.job_id).await,
        Some(DiskHeld::Fetching)
    );
    torrent
}

// A running torrent that wants its file again on a disk with no room over
// the reserve for it has its download held: the start rule refuses what it
// has left, its promise goes back, and its session goes upload-only and
// keeps its run, seeding, shown waiting for disk space with the figures of
// why, as a usenet job kept from starting is. Only the disk's ledger is
// waited on, and no wait for a piece's room is installed. Room made on the
// disk wakes it at the next reading, with no click: it is judged and
// promised what it has left, and fetches again. No piece came while it was
// held: what it is promised is all 65,536 bytes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_full_disk_holds_the_torrent_seeding_until_a_reading_shows_room() {
    let room = Arc::new(AtomicU64::new(1));
    let disk = on_disk(&room).await;
    let service = &disk.rig.service;
    let ledger = disk.capacity.ledger(StorageRoot::Complete);
    let torrent = held(&disk).await;
    let run = torrent.live().unwrap();
    assert!(!torrent.is_paused(), "the run goes on, seeding");
    let view = service.torrent(disk.job_id).await.unwrap();
    assert_eq!(view.state, TorrentState::Stalled);
    assert_eq!(view.stated, Some(Stated::DiskSpace));
    assert_eq!(
        (view.disk_need_bytes, view.disk_free_bytes),
        (Some(65_536), Some(1))
    );
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
    assert_eq!(ledger.promised_total(), 0, "its promise went back");
    assert!(ledger.waited_on());
    assert!(
        locked(&service.inner.start_held)
            .iter()
            .any(|wait| wait.held())
    );
    assert!(!locked(&service.inner.disk_waits).contains_key(&disk.job_id));

    // Nothing more comes from here, so what it is promised as it is let go
    // is what it had left while held.
    disk.seeding.stop().await;
    room.store(PLENTY, Ordering::Release);
    disk.capacity.request_refresh(StorageRoot::Complete);
    run.wait_for_upload_mode(false).await;
    assert_eq!(disk_held(service, disk.job_id).await, None);
    assert_eq!(ledger.promised_total(), 65_536);
    assert!(!torrent.is_paused());
    assert!(!ledger.waited_on());
    assert!(
        !locked(&service.inner.start_held)
            .iter()
            .any(|wait| wait.held())
    );
    service.stop().await;
}

// A resume of a torrent whose download is held is judged by the start rule,
// as a usenet job's resume is: with no room it stays held, seeding, with
// its figures, and is not let go whatever the readings say.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_resume_lets_a_torrent_waiting_for_room_fetch_again() {
    let room = Arc::new(AtomicU64::new(1));
    let disk = on_disk(&room).await;
    let service = &disk.rig.service;
    let run = held(&disk).await.live().unwrap();
    disk.seeding.stop().await;

    service.resume(disk.job_id).await.unwrap();
    assert!(run.upload_mode());
    assert_eq!(
        disk_held(service, disk.job_id).await,
        Some(DiskHeld::Fetching)
    );
    let view = service.torrent(disk.job_id).await.unwrap();
    assert_eq!(view.stated, Some(Stated::DiskSpace));
    assert_eq!(view.disk_need_bytes, Some(65_536));
    assert!(!not_running(service, disk.job_id).await);
    assert!(disk.capacity.ledger(StorageRoot::Complete).waited_on());
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
    let disk = on_disk(&room).await;
    let service = &disk.rig.service;
    let view = view_once(service, disk.job_id, |view| {
        view.stated == Some(Stated::DiskSpace)
    })
    .await;
    assert_eq!(view.state, TorrentState::Queued);
    assert_eq!(view.downloaded_bytes, 0);
    assert!(not_running(service, disk.job_id).await, "it does not start");
    assert!(
        locked(&service.inner.start_held)
            .iter()
            .any(|wait| wait.held())
    );
    assert!(disk.capacity.ledger(StorageRoot::Complete).waited_on());

    room.store(PLENTY, Ordering::Release);
    disk.capacity.request_refresh(StorageRoot::Complete);
    view_once(service, disk.job_id, |view| {
        view.stated != Some(Stated::DiskSpace)
    })
    .await;
    let torrent = handle_of(service, disk.job_id).await;
    torrent.wait_until_completed().await.unwrap();
    assert!(
        !locked(&service.inner.start_held)
            .iter()
            .any(|wait| wait.held())
    );
    assert!(!disk.capacity.ledger(StorageRoot::Complete).waited_on());
    service.stop().await;
    disk.seeding.stop().await;
}

// With no reserve kept, a torrent's start past the disk's free bytes is
// refused, as a usenet job's is: 1,000 bytes free leave no room for its
// 65,536, and it stays paused, waiting for disk space with the figures of
// why. Nothing is promised.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn with_no_reserve_a_start_past_the_free_bytes_is_refused() {
    let disk = on_disks_keeping(
        |_: &Path| {
            Ok(DiskSpace {
                total_bytes: 1 << 40,
                available_bytes: 1_000,
            })
        },
        None,
        None,
        |_| {},
        0,
    )
    .await;
    let service = &disk.rig.service;
    let view = view_once(service, disk.job_id, |view| {
        view.stated == Some(Stated::DiskSpace)
    })
    .await;
    assert_eq!(view.state, TorrentState::Queued);
    assert_eq!(
        (view.disk_need_bytes, view.disk_free_bytes),
        (Some(65_536), Some(1_000))
    );
    assert_eq!(view.downloaded_bytes, 0);
    assert!(not_running(service, disk.job_id).await, "it does not start");
    assert_eq!(
        disk.capacity.ledger(StorageRoot::Complete).promised_total(),
        0
    );
    service.stop().await;
    disk.seeding.stop().await;
}

// A disk that never holds what a torrent has left to fetch keeps it from
// starting and says the disk is too small, as a usenet job's row does, with
// the most the disk holds above its reserve.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_disk_too_small_for_a_torrent_says_so() {
    let disk = on_disks(
        |_: &Path| {
            Ok(DiskSpace {
                total_bytes: RESERVE + 1_000,
                available_bytes: RESERVE + 1_000,
            })
        },
        None,
        None,
        |_| {},
    )
    .await;
    let service = &disk.rig.service;
    let view = view_once(service, disk.job_id, |view| {
        view.stated == Some(Stated::DiskTooSmall)
    })
    .await;
    assert_eq!(view.state, TorrentState::Queued);
    assert_eq!(
        (view.disk_need_bytes, view.disk_free_bytes),
        (Some(65_536), Some(1_000))
    );
    assert!(not_running(service, disk.job_id).await, "it does not start");
    service.publish_job(disk.job_id).await;
    assert_eq!(
        service.inner.queue.lock().unwrap().as_ref().map(|queue| {
            queue
                .state()
                .get_job(disk.job_id)
                .and_then(|job| job.download_wait_reason)
        }),
        Some(Some(
            crate::jobs::handle::DISK_TOO_SMALL_WAIT_REASON.to_owned()
        ))
    );
    service.stop().await;
    disk.seeding.stop().await;
}

// The file named `name` in `dir`'s tree.
fn file_named(dir: &Path, name: &str) -> std::path::PathBuf {
    let mut dirs = vec![dir.to_path_buf()];
    while let Some(dir) = dirs.pop() {
        for entry in std::fs::read_dir(dir).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                dirs.push(path);
            } else if path.file_name() == Some(std::ffi::OsStr::new(name)) {
                return path;
            }
        }
    }
    panic!("no {name} under {}", dir.display());
}

// A recheck that finds less than the torrent was believed to hold judges
// it again once the check ends, as a change of the files it wants does:
// with no room over the reserve for the piece it lost, its download is
// held, its session in upload mode, seeding what it has, not paused, with
// the figures of why, and nothing is promised for it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_recheck_that_finds_less_judges_the_start_again() {
    let room = Arc::new(AtomicU64::new(PLENTY));
    let disk = on_disk(&room).await;
    let service = &disk.rig.service;
    let ledger = disk.capacity.ledger(StorageRoot::Complete);
    let torrent = handle_of(service, disk.job_id).await;
    torrent.wait_until_completed().await.unwrap();
    // Nobody is left to fetch the lost piece from.
    disk.seeding.stop().await;
    let dir = service
        .torrent(disk.job_id)
        .await
        .unwrap()
        .storage_dir
        .unwrap();
    {
        use std::io::{Seek, SeekFrom, Write};
        let file = file_named(Path::new(&dir), "a.bin");
        let mut opened = std::fs::OpenOptions::new().write(true).open(&file).unwrap();
        opened.seek(SeekFrom::Start(2 * 16_384 + 100)).unwrap();
        opened.write_all(&[0x5a; 16]).unwrap();
    }
    room.store(1, Ordering::Release);
    let capacity = Arc::clone(&disk.capacity);
    tokio::task::spawn_blocking(move || capacity.reread_roots())
        .await
        .unwrap();

    let mut moved = service.inner.disk_waits_moved.subscribe();
    service.recheck(disk.job_id).await.unwrap();
    moved
        .wait_for(|_| {
            locked(&service.inner.start_held)
                .iter()
                .any(|wait| wait.held())
        })
        .await
        .unwrap();
    assert_eq!(
        disk_held(service, disk.job_id).await,
        Some(DiskHeld::Fetching)
    );
    assert!(torrent.live().unwrap().upload_mode());
    assert!(!torrent.is_paused(), "the run goes on, seeding");
    let view = service.torrent(disk.job_id).await.unwrap();
    assert_eq!(
        (view.disk_need_bytes, view.disk_free_bytes),
        (Some(16_384), Some(1))
    );
    assert_eq!(ledger.promised_total(), 0);
    assert!(ledger.waited_on());
    service.stop().await;
}

// The judgements of check ends made so far: a test waits past it for the
// one its own recheck makes.
fn checks_judged(service: &TorrentService) -> (tokio::sync::watch::Receiver<u64>, u64) {
    let mut judged = service.inner.checks_judged.subscribe();
    let at = *judged.borrow_and_update();
    (judged, at)
}

// Until a check end past `at` was judged.
async fn judged_past(judged: &mut tokio::sync::watch::Receiver<u64>, at: u64) {
    judged.wait_for(|judged| *judged > at).await.unwrap();
}

// A complete torrent whose folder is gone at a restart is held for its
// files. A recheck lets go of every piece, and once the check of the
// session it joins finds none of the files it wants, it is short of
// complete again: it shows downloading, not seeding, its disk is looked up,
// and its start is promised every byte it has left, all before any block
// arrives (no peer has one to send).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_recheck_that_finds_a_complete_torrent_gone_downloads_it_again() {
    let disks = tempfile::tempdir().unwrap();
    let probe = |_: &Path| {
        Ok(DiskSpace {
            total_bytes: 1 << 40,
            available_bytes: RESERVE + PLENTY,
        })
    };
    let roots = [0, 1, 2].map(|_| disks.path().to_path_buf());
    let capacity = Arc::new(StorageCapacity::with_probe(roots, NEVER, Arc::new(probe)));
    for root in StorageRoot::ALL {
        capacity.wait_for_probes(root, 1);
    }
    let ledger = capacity.ledger(StorageRoot::Complete);
    let rig = rig(set_clock());
    rig.service.open().await.unwrap();
    let (job_id, dir) = with_every_piece(&rig).await;
    view_once(&rig.service, job_id, |view| view.completed_at_ms.is_some()).await;
    rig.service.stop().await;
    rig.db.prune_orphan_active_state(JobId(u64::MAX)).unwrap();
    std::fs::remove_dir_all(&dir).unwrap();

    let again = service_on(
        &rig.db,
        rig.dir.path(),
        &rig.routes,
        disk_env(&capacity),
        set_clock(),
    );
    again.publish_to(queue());
    reopen(&again).await;
    let view = again.torrent(job_id).await.unwrap();
    assert_eq!(view.state, TorrentState::FilesMissing);
    assert!(view.completed_at_ms.is_some());
    assert_eq!(ledger.promised_total(), 0);

    let (mut judged, at) = checks_judged(&again);
    again.recheck(job_id).await.unwrap();
    judged_past(&mut judged, at).await;
    let view = again.torrent(job_id).await.unwrap();
    assert_eq!(view.completed_at_ms, None, "short of complete again");
    assert!(
        matches!(
            view.state,
            TorrentState::Downloading | TorrentState::Stalled
        ),
        "it shows downloading, not {:?}",
        view.state
    );
    assert_eq!(
        rig.db.get_torrent(job_id).unwrap().unwrap().completed_at_ms,
        None,
        "stored short of complete"
    );
    placed(&again, job_id).await;
    promised(ledger, 3_000).await;
    assert_eq!(job_have(&again, job_id).await, 0, "no block came");
    let handle = handle_of(&again, job_id).await;
    assert!(!handle.is_paused());
    assert!(!handle.live().unwrap().upload_mode(), "it fetches");
    again.stop().await;
}

// A recheck of a complete torrent in its session that finds every byte of
// its file gone judges the check's end the same way: it is short of
// complete again, shows downloading, and is promised what it has left.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_recheck_in_its_session_that_finds_the_files_gone_downloads_again() {
    let room = Arc::new(AtomicU64::new(PLENTY));
    let disk = on_disk(&room).await;
    let service = &disk.rig.service;
    let ledger = disk.capacity.ledger(StorageRoot::Complete);
    let torrent = handle_of(service, disk.job_id).await;
    torrent.wait_until_completed().await.unwrap();
    // Nobody is left to fetch the lost bytes from.
    disk.seeding.stop().await;
    view_once(service, disk.job_id, |view| {
        view.completed_at_ms.is_some() && view.state == TorrentState::Seeding
    })
    .await;
    promised(ledger, 0).await;
    let dir = service
        .torrent(disk.job_id)
        .await
        .unwrap()
        .storage_dir
        .unwrap();
    // Emptied in place, so a handle the store keeps open reads it so too.
    std::fs::OpenOptions::new()
        .write(true)
        .open(file_named(Path::new(&dir), "a.bin"))
        .unwrap()
        .set_len(0)
        .unwrap();

    let (mut judged, at) = checks_judged(service);
    service.recheck(disk.job_id).await.unwrap();
    judged_past(&mut judged, at).await;
    let view = service.torrent(disk.job_id).await.unwrap();
    assert_eq!(view.completed_at_ms, None, "short of complete again");
    assert!(
        matches!(
            view.state,
            TorrentState::Downloading | TorrentState::Stalled
        ),
        "it shows downloading, not {:?}",
        view.state
    );
    promised(ledger, 65_536).await;
    assert_eq!(disk_held(service, disk.job_id).await, None);
    assert!(!torrent.is_paused());
    service.stop().await;
}

// A recheck that finds every file a complete torrent wants changes
// nothing: it stays complete, where it was in the queue, seeding, and
// nothing is promised for it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_recheck_that_finds_everything_leaves_the_torrent_complete() {
    let room = Arc::new(AtomicU64::new(PLENTY));
    let disk = on_disk(&room).await;
    let service = &disk.rig.service;
    let ledger = disk.capacity.ledger(StorageRoot::Complete);
    let torrent = handle_of(service, disk.job_id).await;
    torrent.wait_until_completed().await.unwrap();
    disk.seeding.stop().await;
    let before = view_once(service, disk.job_id, |view| {
        view.completed_at_ms.is_some() && view.state == TorrentState::Seeding
    })
    .await;
    promised(ledger, 0).await;
    let place = service.inner.live.lock().await[&disk.job_id]
        .record
        .queue_position;

    let (mut judged, at) = checks_judged(service);
    service.recheck(disk.job_id).await.unwrap();
    judged_past(&mut judged, at).await;
    let view = service.torrent(disk.job_id).await.unwrap();
    assert_eq!(view.completed_at_ms, before.completed_at_ms);
    assert_eq!(view.state, TorrentState::Seeding);
    assert_eq!(
        service.inner.live.lock().await[&disk.job_id]
            .record
            .queue_position,
        place
    );
    assert_eq!(
        disk.rig
            .db
            .get_torrent(disk.job_id)
            .unwrap()
            .unwrap()
            .completed_at_ms,
        before.completed_at_ms
    );
    assert_eq!(ledger.promised_total(), 0);
    assert_eq!(disk_held(service, disk.job_id).await, None);
    service.stop().await;
}

// A running torrent moved to another disk takes its promise with it: what
// it was promised goes back on the old disk, and what it has left is judged
// on the new one by the start rule. With no room there, its download is
// held and it seeds, not paused, waiting on the new disk only.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_moved_torrent_is_judged_on_its_new_disk() {
    const PIECE: usize = 16_384;
    let complete = tempfile::tempdir().unwrap();
    let working = tempfile::tempdir().unwrap();
    weaver_server_disk::runtime::fs::test_devices::assign(complete.path(), 71_101);
    weaver_server_disk::runtime::fs::test_devices::assign(working.path(), 71_102);
    let save = complete.path().join("saved");
    let to = working.path().join("moved");
    std::fs::create_dir_all(&save).unwrap();
    std::fs::create_dir_all(&to).unwrap();
    let room = Arc::new(AtomicU64::new(1));
    let read = Arc::clone(&room);
    let full = working.path().to_path_buf();
    let probe = Arc::new(move |path: &Path| {
        let over = if path.starts_with(&full) {
            read.load(Ordering::Acquire)
        } else {
            PLENTY
        };
        Ok(DiskSpace {
            total_bytes: 1 << 40,
            available_bytes: RESERVE + over,
        })
    });
    let roots = [
        complete.path().to_path_buf(),
        working.path().to_path_buf(),
        complete.path().to_path_buf(),
    ];
    let capacity = Arc::new(StorageCapacity::with_probe(roots, NEVER, probe));
    for root in StorageRoot::ALL {
        capacity.wait_for_probes(root, 1);
    }
    let rig = rig_with(set_clock(), disk_env(&capacity));
    on_loopback(&rig.routes);
    rig.service.publish_to(queue());
    rig.service.open().await.unwrap();
    let service = &rig.service;

    // A peer with only the first of its two files: the torrent gets that
    // one and waits for the other.
    let data: Vec<u8> = (0..4 * PIECE as u32).map(|at| (at % 251) as u8).collect();
    let files = [
        ("set/a.bin", &data[..2 * PIECE]),
        ("set/b.bin", &data[2 * PIECE..]),
    ];
    let meta = metainfo("set", &files, PIECE, false);
    let seeds = tempfile::tempdir().unwrap();
    let (sharer, shared) = sharing(seeds.path(), &meta, &files[..1]).await;
    shared.wait_until_initialized().await.unwrap();
    let std::net::SocketAddr::V4(peer) = sharer.listen_addr().unwrap() else {
        panic!("the sharer listens on IPv4 loopback");
    };
    let url = tracker(peer).await;
    let mut add = request(AddSource::Metainfo(meta));
    add.placement.save_path = Some(save.to_string_lossy().into_owned());
    let job_id = service.add_opened(add).await.unwrap();
    tracked(service, job_id, &url).await;
    let store = service.inner.live.lock().await[&job_id]
        .store
        .clone()
        .unwrap();
    service
        .inner
        .settled
        .subscribe()
        .wait_for(|_| store.has_files(|file| file == 0))
        .await
        .unwrap();
    sharer.stop().await;
    let complete_ledger = capacity.ledger(StorageRoot::Complete);
    let working_ledger = capacity.ledger(StorageRoot::Intermediate);
    assert_eq!(complete_ledger.promised_total(), 2 * PIECE as u64);
    assert_eq!(working_ledger.promised(), 0);

    let mut moved = service.inner.disk_waits_moved.subscribe();
    service
        .set_location(job_id, to.to_string_lossy().into_owned())
        .await
        .unwrap();
    moved
        .wait_for(|_| {
            locked(&service.inner.start_held)
                .iter()
                .any(|wait| wait.held())
        })
        .await
        .unwrap();
    assert_eq!(disk_held(service, job_id).await, Some(DiskHeld::Fetching));
    let torrent = handle_of(service, job_id).await;
    assert!(torrent.live().unwrap().upload_mode());
    assert!(!torrent.is_paused(), "the run goes on, seeding");
    let view = service.torrent(job_id).await.unwrap();
    assert_eq!(
        (view.disk_need_bytes, view.disk_free_bytes),
        (Some(2 * PIECE as u64), Some(1))
    );
    assert_eq!(complete_ledger.promised_total(), 0, "its promise went back");
    assert!(!complete_ledger.waited_on());
    assert_eq!(working_ledger.promised(), 0);
    assert!(working_ledger.waited_on());
    service.stop().await;
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
            .any(|wait| wait.held())
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
// plenty: what it has left is judged on its own disk, its download is held
// there, and a reading of that disk with room lets it go, promised there.
// Nothing is promised or waited on on the complete root's.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_torrent_saved_on_another_disk_waits_on_that_disk() {
    let complete = tempfile::tempdir().unwrap();
    let working = tempfile::tempdir().unwrap();
    let save = working.path().join("saved");
    std::fs::create_dir_all(&save).unwrap();
    weaver_server_disk::runtime::fs::test_devices::assign(complete.path(), 71_001);
    weaver_server_disk::runtime::fs::test_devices::assign(working.path(), 71_002);
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
    let view = service.torrent(disk.job_id).await.unwrap();
    assert_eq!(
        (view.disk_need_bytes, view.disk_free_bytes),
        (Some(65_536), Some(1))
    );
    let working_ledger = disk.capacity.ledger(StorageRoot::Intermediate);
    assert!(working_ledger.waited_on());
    let complete_ledger = disk.capacity.ledger(StorageRoot::Complete);
    assert!(!complete_ledger.waited_on());
    assert_eq!(complete_ledger.promised(), 0);

    // Room on its own disk lets it go.
    disk.seeding.stop().await;
    room.store(PLENTY, Ordering::Release);
    disk.capacity.request_refresh(StorageRoot::Intermediate);
    run.wait_for_upload_mode(false).await;
    assert_eq!(disk_held(service, disk.job_id).await, None);
    assert_eq!(working_ledger.promised_total(), 65_536);
    assert!(!working_ledger.waited_on());
    assert_eq!(complete_ledger.promised(), 0);
    service.stop().await;
}

// A torrent whose grown files do not fit because what other writers
// promised leaves no room over the reserve has its download held, showing
// the room those promises leave. Those writers give their promises back:
// the disk's free figure never moves, and the torrent is let go by the
// ledger's ring, promised what it has left. The seeds it let go while it
// only uploaded are dialled again at once, and it completes with nothing
// left promised.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_torrent_held_by_promises_wakes_when_they_are_released() {
    // 70 KiB over the reserve, 60 of them promised elsewhere: no room for
    // 64 KiB. Given back, it fits.
    let room = Arc::new(AtomicU64::new(70 << 10));
    let disk = on_disk_promised(&room, 60 << 10).await;
    let service = &disk.rig.service;
    let ledger = disk.capacity.ledger(StorageRoot::Complete);
    let torrent = held(&disk).await;
    let run = torrent.live().unwrap();
    let view = service.torrent(disk.job_id).await.unwrap();
    assert_eq!(
        (view.disk_need_bytes, view.disk_free_bytes),
        (Some(65_536), Some(10 << 10))
    );

    ledger.release(60 << 10);
    run.wait_for_upload_mode(false).await;
    torrent.wait_until_completed().await.unwrap();
    assert!(!run.upload_mode());
    assert_eq!(disk_held(service, disk.job_id).await, None);
    assert!(!locked(&service.inner.disk_waits).contains_key(&disk.job_id));
    promised(ledger, 0).await;
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
    disk.seeding.stop().await;
    ledger.release(60 << 10);
    run.wait_for_upload_mode(false).await;
    assert_eq!(disk_held(service, disk.job_id).await, None);
    // The refusal of a write made before the wake, heard now. It is said as
    // the intake says one, and the ledger is left with room: with the
    // seeder gone, no write is made after the wake to be refused and enter
    // upload mode on its own.
    service.inner.signal.heard(
        disk.job_id,
        crate::torrents::intake::Heard::NotBooked(16_384),
    );
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
    assert_ne!(view.stated, Some(Stated::DiskSpace));
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

// Until `ledger` holds `bytes` promised.
async fn promised(ledger: &weaver_server_disk::operations::disk::DiskLedger, bytes: u64) {
    while ledger.promised_total() != bytes {
        tokio::task::yield_now().await;
    }
}

// The start rule usenet jobs start by, on the complete root.
fn usenet_rule() -> weaver_server_disk::operations::start_rule::StartRule {
    weaver_server_disk::operations::start_rule::StartRule {
        working: StorageRoot::Complete,
        output: StorageRoot::Complete,
        reserve: RESERVE,
    }
}

// 4,000 bytes over the reserve: the 65,536 byte torrent does not fit and
// waits, showing what it needs and what the disk leaves; a 3,000 byte one
// added after it fits, starts, and is promised its bytes on the ledger. A
// usenet start of 2,000 is then refused for the 1,000 left. A pause gives
// the promise back, and a resume promises it again.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_start_is_promised_what_it_fetches_and_one_that_does_not_fit_waits() {
    let room = Arc::new(AtomicU64::new(4_000));
    let disk = on_disk(&room).await;
    let service = &disk.rig.service;
    let capacity = &disk.capacity;
    let ledger = capacity.ledger(StorageRoot::Complete);
    let view = view_once(service, disk.job_id, |view| {
        view.stated == Some(Stated::DiskSpace)
    })
    .await;
    assert_eq!(view.disk_need_bytes, Some(65_536));
    assert!(matches!(view.disk_free_bytes, Some(4_000 | 1_000)));
    assert!(not_running(service, disk.job_id).await);

    let small = service
        .add_opened(request(AddSource::Metainfo(plain("small", 1))))
        .await
        .unwrap();
    promised(ledger, 3_000).await;
    assert!(!not_running(service, small).await, "the smaller one starts");
    assert_eq!(
        usenet_rule().book_on(capacity, StorageRoot::Complete, 2_000, 2_000),
        Err(weaver_server_disk::operations::disk::StartBooking::Short {
            room: 1_000,
            need: 2_000,
            part: 0
        })
    );
    assert!(
        not_running(service, disk.job_id).await,
        "the large one waits"
    );

    service.pause(small).await.unwrap();
    promised(ledger, 0).await;
    service.resume(small).await.unwrap();
    promised(ledger, 3_000).await;
    service.stop().await;
    disk.seeding.stop().await;
}

// A usenet start's promise counts against a torrent: with 70,000 bytes
// over the reserve and 10,000 of them promised to a usenet job by the
// shared rule, the 65,536 byte torrent waits for the 60,000 left. Given
// back, it starts, and its pieces draw on its own promise: once it is
// complete nothing stays promised, so no byte was charged twice.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_usenet_promise_holds_a_torrent_and_its_pieces_draw_on_its_own() {
    let room = Arc::new(AtomicU64::new(70_000));
    let disk = on_disks(
        {
            let read = Arc::clone(&room);
            move |_: &Path| {
                Ok(DiskSpace {
                    total_bytes: 1 << 40,
                    available_bytes: RESERVE + read.load(Ordering::Acquire),
                })
            }
        },
        None,
        None,
        |capacity| {
            assert_eq!(
                usenet_rule().book_on(capacity, StorageRoot::Complete, 10_000, 10_000),
                Ok((10_000, 0))
            );
        },
    )
    .await;
    let service = &disk.rig.service;
    let ledger = disk.capacity.ledger(StorageRoot::Complete);
    let view = view_once(service, disk.job_id, |view| {
        view.stated == Some(Stated::DiskSpace)
    })
    .await;
    assert_eq!(view.disk_need_bytes, Some(65_536));
    assert_eq!(view.disk_free_bytes, Some(60_000));

    usenet_rule().give_back(&disk.capacity, 10_000, 0);
    let torrent = handle_of(service, disk.job_id).await;
    torrent.wait_until_completed().await.unwrap();
    promised(ledger, 0).await;
    service.stop().await;
    disk.seeding.stop().await;
}

// A usenet job held for a full disk keeps a torrent from starting: with
// 70,000 bytes over the reserve and 10,000 of them waited for by the held
// job, the 65,536 byte torrent waits for the 60,000 left. The hold ends,
// which rings as a promise given back does, and the torrent starts.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_usenet_job_waiting_for_room_holds_a_torrent_start() {
    let disk = on_disks(
        |_: &Path| {
            Ok(DiskSpace {
                total_bytes: 1 << 40,
                available_bytes: RESERVE + 70_000,
            })
        },
        None,
        None,
        |capacity| capacity.ledger(StorageRoot::Complete).hold_waiting(10_000),
    )
    .await;
    let service = &disk.rig.service;
    let ledger = disk.capacity.ledger(StorageRoot::Complete);
    let view = view_once(service, disk.job_id, |view| {
        view.stated == Some(Stated::DiskSpace)
    })
    .await;
    assert_eq!(
        (view.disk_need_bytes, view.disk_free_bytes),
        (Some(65_536), Some(60_000))
    );
    assert!(not_running(service, disk.job_id).await, "it does not start");

    ledger.end_waiting(10_000);
    let torrent = handle_of(service, disk.job_id).await;
    torrent.wait_until_completed().await.unwrap();
    promised(ledger, 0).await;
    service.stop().await;
    disk.seeding.stop().await;
}

// A torrent kept from starting is paused: no torrent on its disk is kept by
// the start wait any more, so the wait ends at that pass and the disk is no
// longer waited on.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_start_wait_ends_when_its_last_torrent_is_paused() {
    let room = Arc::new(AtomicU64::new(0));
    let disk = on_disk(&room).await;
    let service = &disk.rig.service;
    view_once(service, disk.job_id, |view| {
        view.stated == Some(Stated::DiskSpace)
    })
    .await;
    assert!(
        locked(&service.inner.start_held)
            .iter()
            .any(|wait| wait.held())
    );
    assert!(disk.capacity.ledger(StorageRoot::Complete).waited_on());

    service.pause(disk.job_id).await.unwrap();
    service
        .inner
        .disk_waits_moved
        .subscribe()
        .wait_for(|_| {
            !locked(&service.inner.start_held)
                .iter()
                .any(|wait| wait.held())
        })
        .await
        .unwrap();
    assert!(!disk.capacity.ledger(StorageRoot::Complete).waited_on());
    service.stop().await;
    disk.seeding.stop().await;
}

// A magnet's bytes are known only at its metadata, which comes while it
// runs: it is judged then by the start rule. With 1,000 bytes over the
// reserve its 3,000 bytes do not fit, so it is held as a torrent refused
// at start is, showing need 3,000 and free 1,000. A reading with room lets
// it go, promised its 3,000 bytes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_magnet_is_judged_when_its_metadata_comes() {
    use crate::torrents::net::{LegKind, LegSockets};

    let room = Arc::new(AtomicU64::new(1_000));
    let read = Arc::clone(&room);
    let dir = tempfile::tempdir().unwrap();
    let capacity = Arc::new(StorageCapacity::with_probe(
        [0, 1, 2].map(|_| dir.path().to_path_buf()),
        NEVER,
        Arc::new(move |_: &Path| {
            Ok(DiskSpace {
                total_bytes: 1 << 40,
                available_bytes: RESERVE + read.load(Ordering::Acquire),
            })
        }),
    ));
    for root in StorageRoot::ALL {
        capacity.wait_for_probes(root, 1);
    }
    let ledger = capacity.ledger(StorageRoot::Complete);
    let rig = rig_with(set_clock(), disk_env(&capacity));
    let loopback = Arc::new(weaver_tunnel::pipe::Revocable::new(Arc::new(
        weaver_tunnel::pipe::Egress::new(
            4,
            weaver_tunnel::egress::SocketEgress::SourceAddress("127.0.0.1".parse().unwrap()),
            Duration::MAX,
        ),
    )));
    rig.routes.set_leg(
        LegKind::Ladder,
        Arc::new(LegSockets::new(loopback, &LegKind::Ladder)),
    );
    rig.service.publish_to(queue());
    rig.service.open().await.unwrap();
    let service = &rig.service;
    let bytes = plain("late", 1);
    let hash = parse(&bytes).unwrap().hashes.v1.unwrap().as_string();
    // Its one named peer is a closed loopback port: no metadata comes but
    // the one handed over below.
    let job_id = service
        .add_opened(request(AddSource::Magnet(format!(
            "magnet:?xt=urn:btih:{hash}&x.pe=127.0.0.1:1"
        ))))
        .await
        .unwrap();
    // It runs: its metadata is fetched.
    while !matches!(
        service.inner.live.lock().await[&job_id].resolving,
        Some(Resolving::Fetching(_))
    ) {
        tokio::task::yield_now().await;
    }
    service
        .resolved(job_id, Bytes::from(bytes), &[], None)
        .await
        .unwrap();
    let view = view_once(service, job_id, |view| {
        view.stated == Some(Stated::DiskSpace)
    })
    .await;
    assert_eq!(view.disk_need_bytes, Some(3_000));
    assert_eq!(view.disk_free_bytes, Some(1_000));
    assert!(not_running(service, job_id).await, "it is held");
    assert_eq!(ledger.promised_total(), 0);

    room.store(PLENTY, Ordering::Release);
    capacity.request_refresh(StorageRoot::Complete);
    view_once(service, job_id, |view| {
        view.stated != Some(Stated::DiskSpace)
    })
    .await;
    promised(ledger, 3_000).await;
    service.stop().await;
}

// A change to what a running torrent wants judges its start again. With
// 4,000 bytes over the reserve a torrent of a 1,000 and a 2,000 byte file,
// no peer serving it, is promised 3,000; skipping the second leaves 1,000
// promised. A usenet start then takes 2,500. Wanting the second again
// needs 3,000 of the 1,500 left: refused, so its promise goes back and
// its download is held, its session in upload mode, not paused, showing
// need 3,000 and free 1,500. The usenet promise given back lets it go:
// judged, promised its 3,000, and out of upload mode.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_wanted_change_judges_the_start_again() {
    let room = Arc::new(AtomicU64::new(4_000));
    let disk = on_disk(&room).await;
    let service = &disk.rig.service;
    let capacity = &disk.capacity;
    let ledger = capacity.ledger(StorageRoot::Complete);
    view_once(service, disk.job_id, |view| {
        view.stated == Some(Stated::DiskSpace)
    })
    .await;
    let two = metainfo(
        "two",
        &[("two/a.bin", &[1; 1_000]), ("two/b.bin", &[2; 2_000])],
        1024,
        false,
    );
    let job_id = service
        .add_opened(request(AddSource::Metainfo(two)))
        .await
        .unwrap();
    promised(ledger, 3_000).await;

    service
        .set_file_priorities(job_id, &[(1, FilePriority::Ignored)])
        .await
        .unwrap();
    promised(ledger, 1_000).await;
    assert_eq!(
        usenet_rule().book_on(capacity, StorageRoot::Complete, 2_500, 2_500),
        Ok((2_500, 0))
    );
    assert_eq!(ledger.promised_total(), 3_500);

    service
        .set_file_priorities(job_id, &[(1, FilePriority::Normal)])
        .await
        .unwrap();
    assert_eq!(ledger.promised_total(), 2_500, "its promise went back");
    assert!(!not_running(service, job_id).await, "it keeps running");
    assert_eq!(disk_held(service, job_id).await, Some(DiskHeld::Fetching));
    let run = handle_of(service, job_id).await.live().unwrap();
    assert!(run.upload_mode(), "its download is held");
    let view = service.torrent(job_id).await.unwrap();
    assert_eq!(view.stated, Some(Stated::DiskSpace));
    assert_eq!(
        (view.disk_need_bytes, view.disk_free_bytes),
        (Some(3_000), Some(1_500))
    );

    usenet_rule().give_back(capacity, 2_500, 0);
    run.wait_for_upload_mode(false).await;
    assert_eq!(disk_held(service, job_id).await, None);
    assert_eq!(ledger.promised_total(), 3_000);
    service.stop().await;
    disk.seeding.stop().await;
}

// A torrent that downloaded in a run before is promised what it has left
// at a restart, unjudged: with 1 byte over the reserve it starts, and its
// pieces draw on that promise, so none is refused and it completes with
// nothing left promised.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_restart_promises_a_torrent_that_downloaded_unjudged() {
    let room = Arc::new(AtomicU64::new(1));
    let disk = on_disk(&room).await;
    let job_id = disk.job_id;
    view_once(&disk.rig.service, job_id, |view| {
        view.stated == Some(Stated::DiskSpace)
    })
    .await;
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
    torrent.wait_until_completed().await.unwrap();
    assert!(!torrent.live().is_some_and(|run| run.upload_mode()));
    assert!(!locked(&again.inner.disk_waits).contains_key(&job_id));
    promised(capacity.ledger(StorageRoot::Complete), 0).await;
    again.stop().await;
    disk.seeding.stop().await;
}

// Two torrents on a disk with no room over the reserve are kept from
// starting, as usenet jobs are. One that downloaded earlier in this run is
// judged by the same rule and stays held.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_short_disk_holds_a_torrent_whatever_it_fetched_in_this_run() {
    let room = Arc::new(AtomicU64::new(0));
    let disk = on_disk(&room).await;
    let service = &disk.rig.service;
    let second = service
        .add_opened(request(AddSource::Metainfo(plain("other", 1))))
        .await
        .unwrap();
    for job_id in [disk.job_id, second] {
        // A torrent shows its start from what was saved first: the view is
        // read once it has. Its join to its session rings nothing (neither
        // the queue's row nor the service's carries it), so the wait looks
        // again each turn until it has joined.
        let torrent = loop {
            let attached = service.inner.live.lock().await[&job_id]
                .attached
                .as_ref()
                .map(|attached| Arc::clone(&attached.handle));
            if let Some(torrent) = attached {
                break torrent;
            }
            tokio::task::yield_now().await;
        };
        torrent.wait_until_initialized().await.unwrap();
        view_once(service, job_id, |view| {
            view.stated == Some(Stated::DiskSpace)
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
    // In this run it is judged as the other is: what it has left does not
    // fit, so it stays held. Only a restart keeps a start unjudged.
    let view = service.torrent(second).await.unwrap();
    assert_eq!(view.stated, Some(Stated::DiskSpace));
    assert!(
        not_running(service, second).await,
        "the one that downloaded is judged too"
    );
    let view = service.torrent(disk.job_id).await.unwrap();
    assert_eq!(view.stated, Some(Stated::DiskSpace));
    assert!(
        not_running(service, disk.job_id).await,
        "the other stays held"
    );
    service.stop().await;
    disk.seeding.stop().await;
}

// A torrent's download held for room ends with the run it was held in: no
// new run, and no torrent gone, keeps it. Nothing stays promised or handed
// to it, no wait for a piece's room is left, and nothing its intake said is
// kept for a torrent removed. A run that begins again is judged as a start.
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
            "remove" => service.remove(job_id, false).await.unwrap(),
            "pause" => service.pause(job_id).await.unwrap(),
            "recheck" => {
                // The check ends a run, and once it ends the start rule
                // judges the torrent again: with no room over the reserve
                // for what it has left, its download is held from starting.
                // The recheck lets go of the run's hold before it answers;
                // what the check's end is judged to is read once that
                // judgement is made, whatever it made of the hold.
                let (mut judged, at) = checks_judged(service);
                service.recheck(job_id).await.unwrap();
                judged_past(&mut judged, at).await;
            }
            _ => {
                // The move is answered before the torrent joins the new
                // route's session; the run it begins there is judged as a
                // start at the next pass, made once it has joined.
                disk.rig.routes.route(TorrentScope::Category(1), 9);
                service
                    .set_category(job_id, Some("tv".into()))
                    .await
                    .unwrap();
                service.opened(job_id).await;
                service.apply_queue().await;
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
            "{how}: no wait for a piece's room"
        );
        let ledger = disk.capacity.ledger(StorageRoot::Complete);
        assert_eq!(ledger.promised(), 0, "{how}");
        if let Some(torrent) = service.inner.live.lock().await.get(&job_id) {
            // Held again only from starting, by the start rule: its download
            // held after a check, kept from starting in the run a move
            // begins; never the wait of the run that ended.
            let start_held = match how {
                "recheck" => Some(DiskHeld::Fetching),
                "route move" => Some(DiskHeld::Starting),
                _ => None,
            };
            assert_eq!(torrent.disk_held, start_held, "{how}");
            assert_ne!(torrent.disk_held, Some(DiskHeld::Writing), "{how}");
            let room = torrent.intake.as_ref().unwrap().room();
            assert_eq!(room.in_hand(), 0, "{how}");
            assert!(!room.started(), "{how}: judged again as it starts");
        }
        service.stop().await;
    }
}

// A recheck of a torrent with no room for what it has left keeps its run:
// a queue pass made while the check runs holds only its download, as the
// judgement of the check's end does, and never pauses it as a start. Once
// the check ends it runs, seeding, its download held.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_queue_pass_during_a_recheck_holds_only_its_download() {
    let room = Arc::new(AtomicU64::new(1));
    let disk = on_disk(&room).await;
    let service = &disk.rig.service;
    let job_id = disk.job_id;
    let handle = held(&disk).await;
    // Nothing more comes, so no write finds the disk full again.
    disk.seeding.stop().await;
    let session = {
        let live = service.inner.live.lock().await;
        Arc::clone(&live[&job_id].attached.as_ref().unwrap().session)
    };
    let (mut judged, at) = checks_judged(service);
    // The recheck's halves, with a queue pass between them: its pieces are
    // forgotten and its hold ends, as a recheck's do, and the pass comes
    // before the check of its files begins.
    session.forget_for_recheck(&handle).await.unwrap();
    {
        let mut live = service.inner.live.lock().await;
        let torrent = live.get_mut(&job_id).unwrap();
        service.end_disk_room(torrent);
        service.rejudge_after_check(torrent, &session, &handle);
    }
    service.apply_queue().await;
    assert_eq!(
        disk_held(service, job_id).await,
        Some(DiskHeld::Fetching),
        "the pass holds only its download"
    );
    assert!(!handle.is_paused(), "the pass does not pause it as a start");

    session.start_check(&handle).unwrap();
    judged_past(&mut judged, at).await;
    assert_eq!(disk_held(service, job_id).await, Some(DiskHeld::Fetching));
    assert!(!handle.is_paused(), "it runs, seeding");
    assert!(!not_running(service, job_id).await);
    assert!(handle.live().unwrap().upload_mode());
    service.stop().await;
}

// A torrent that joins its session again, after a pause, keeps the disk
// found for its folder: what it has left is judged there with no new look,
// and with nothing of it to seed it is kept from starting there. A piece
// written there books on that disk's ledger, refused with no room over the
// reserve.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_torrent_that_joins_again_books_on_the_disk_it_kept() {
    let room = Arc::new(AtomicU64::new(1));
    let disk = on_disk(&room).await;
    let service = &disk.rig.service;
    let job_id = disk.job_id;
    held(&disk).await;
    disk.seeding.stop().await;
    let place = placed(service, job_id).await;
    service.pause(job_id).await.unwrap();
    assert_eq!(disk_held(service, job_id).await, None);
    let looks = disk.capacity.root_looks();

    service.resume(job_id).await.unwrap();
    let view = view_once(service, job_id, |view| {
        view.stated == Some(Stated::DiskSpace)
    })
    .await;
    assert_eq!(view.disk_need_bytes, Some(65_536));
    assert!(
        not_running(service, job_id).await,
        "nothing to seed: paused"
    );
    assert_eq!(disk_held(service, job_id).await, Some(DiskHeld::Starting));
    let intake = service.inner.live.lock().await[&job_id]
        .intake
        .clone()
        .unwrap();
    let piece = bytes::Bytes::from(vec![0u8; 16_384]);
    assert!(weaver_torrent::storage::write_refused(
        &intake.verified(0, &piece).unwrap_err()
    ));
    let kept = placed(service, job_id).await;
    assert_eq!((kept.disk, kept.dir), (place.disk, place.dir));
    assert_eq!(disk.capacity.root_looks(), looks);
    service.stop().await;
}

// A refusal kept while the run was out of upload mode is heard again once
// the run enters it. A piece of a running torrent wanting nothing, promised
// nothing, is refused on a disk with no room over the reserve and kept
// aside; the torrent then wants its file again, which the start rule
// refuses, so its download is held and the run enters upload mode. The
// refusal is heard then, and the start rule's wait is the torrent's only
// one: no wait for a piece's room is installed beside it, and nothing is
// left kept aside.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_refusal_kept_aside_is_heard_once_the_run_enters_upload_mode() {
    let room = Arc::new(AtomicU64::new(1));
    let disk = on_disk(&room).await;
    let service = &disk.rig.service;
    let job_id = disk.job_id;
    let torrent = runs_wanting_nothing(&disk).await;
    disk.seeding.stop().await;
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

    service
        .set_file_priorities(job_id, &[(0, FilePriority::Normal)])
        .await
        .unwrap();
    assert!(run.upload_mode());
    service
        .inner
        .settled
        .subscribe()
        .wait_for(|_| !locked(&service.inner.entering).contains_key(&job_id))
        .await
        .unwrap();
    assert!(!locked(&service.inner.disk_waits).contains_key(&job_id));
    assert_eq!(disk_held(service, job_id).await, Some(DiskHeld::Fetching));
    let view = service.torrent(job_id).await.unwrap();
    assert_eq!(view.stated, Some(Stated::DiskSpace));
    service.stop().await;
}

// A piece refused on the ledger while the place known is that of a folder
// the torrent no longer goes to waits for the disk of its new folder,
// then on that disk's ledger: its run stays in upload mode with a wait,
// never with none. Room made there wakes it. The run is a running torrent
// wanting nothing whose session is in upload mode, as one is once a write
// it made was refused.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_refusal_for_a_folder_moved_since_waits_for_its_disk_then_its_ledger() {
    let room = Arc::new(AtomicU64::new(1));
    let disk = on_disk(&room).await;
    let service = &disk.rig.service;
    let job_id = disk.job_id;
    let run = runs_wanting_nothing(&disk).await.live().unwrap();
    disk.seeding.stop().await;
    run.hold_download();
    let moved = disk._seeds.path().join("moved");
    std::fs::create_dir_all(&moved).unwrap();
    {
        let mut live = service.inner.live.lock().await;
        let torrent = live.get_mut(&job_id).unwrap();
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
    weaver_server_disk::runtime::fs::test_devices::assign(complete.path(), 73_001);
    weaver_server_disk::runtime::fs::test_devices::assign(working.path(), 73_002);
    let refused = weaver_server_disk::runtime::fs::test_devices::refuse(&save);
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
    let (hold, mut reads) = weaver_server_disk::runtime::fs::test_devices::hold(&before);
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
    use crate::torrents::net::{LegKind, LegSockets};
    use weaver_server_disk::operations::disk::{Capacity, CapacityReading};

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
    rig.service.restored().await;
    let view = rig.service.torrent(job_id).await.unwrap();
    assert_eq!(view.state, TorrentState::Stalled);
    assert_eq!(
        (view.stated, view.detail.as_deref()),
        (Some(Stated::DiskFull), None)
    );
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
        Some("disk_full")
    );
    rig.service.stop().await;
    seeding.stop().await;
}

// The looks begun together for many torrents ask one pass between them: a
// pass is a publish of every torrent, so a pass a look would build the
// views of every torrent once for each. The looks are let go one at a
// time, each with what it asked settled before the next, so a pass each
// is seen as a pass each, whatever the machine's load.
const LOOKS: usize = 20;

// The disks: the roots under `dir`'s `roots`, the torrents under its
// `complete`, both on the one device the test runs on.
fn looked_disks(dir: &Path) -> Arc<StorageCapacity> {
    let roots = dir.join("roots");
    std::fs::create_dir_all(&roots).unwrap();
    let probe = |_: &Path| {
        Ok(DiskSpace {
            total_bytes: 1 << 40,
            available_bytes: RESERVE + PLENTY,
        })
    };
    let capacity = Arc::new(StorageCapacity::with_probe(
        [roots.clone(), roots.clone(), roots],
        NEVER,
        Arc::new(probe),
    ));
    for root in StorageRoot::ALL {
        capacity.wait_for_probes(root, 1);
    }
    capacity
}

// `LOOKS` torrents added with their route down, each with its folder.
async fn add_looked(service: &TorrentService) -> Vec<(JobId, PathBuf)> {
    let mut added = Vec::new();
    for n in 0..LOOKS {
        let source = AddSource::Metainfo(plain(&format!("look{n}"), n as u8));
        let job_id = service.add_opened(request(source)).await.unwrap();
        let live = service.inner.live.lock().await;
        added.push((job_id, files_dir(&live[&job_id])));
    }
    added
}

// Until no pass is asked for and no settle is under way: every pass asked
// so far has published.
async fn passes_settled(service: &TorrentService) {
    let mut settled = service.inner.settled.subscribe();
    settled
        .wait_for(|_| {
            !service.inner.signal.pass_wanted.load(Ordering::SeqCst)
                && !service.inner.settling.load(Ordering::SeqCst)
        })
        .await
        .unwrap();
}

// The looks of `added`, each held at its folder's device read: let go one
// at a time, each settled before the next. The views built meanwhile.
async fn views_of_looks(
    service: &TorrentService,
    added: &[(JobId, PathBuf)],
    mut holds: Vec<(
        weaver_server_disk::runtime::fs::test_devices::HeldDevice,
        tokio::sync::mpsc::UnboundedReceiver<PathBuf>,
    )>,
    views: usize,
) -> usize {
    for ((_, dir), (_, reads)) in added.iter().zip(&mut holds) {
        assert_eq!(&reads.recv().await.unwrap(), dir);
    }
    let intakes: Vec<_> = {
        let live = service.inner.live.lock().await;
        added
            .iter()
            .map(|(job_id, _)| live[job_id].intake.clone().unwrap())
            .collect()
    };
    let mut moved = service.inner.disk_waits_moved.subscribe();
    for ((hold, _), intake) in holds.into_iter().zip(&intakes) {
        let mark = *moved.borrow_and_update();
        drop(hold);
        moved
            .wait_for(|seen| {
                *seen > mark && intake.room().place().is_some() && !intake.room().placing()
            })
            .await
            .unwrap();
        passes_settled(service).await;
    }
    service.views_built() - views
}

// Many torrents whose disks could not be told are looked up again at one
// reading: their looks ask one pass between them, not one each.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_looks_of_one_reading_ask_one_pass() {
    let dir = tempfile::tempdir().unwrap();
    let capacity = looked_disks(dir.path());
    let db = Database::open_in_memory().unwrap();
    let routes = Arc::new(FakeRoutes::default());
    routes.set_down(Some("the leg is down"));
    let service = service_on(&db, dir.path(), &routes, disk_env(&capacity), set_clock());
    service.open().await.unwrap();
    // The folders' disks cannot be told: each look at the add fails, and
    // its torrent waits for a reading to be looked up again.
    let refused =
        weaver_server_disk::runtime::fs::test_devices::refuse(&dir.path().join("complete"));
    let added = add_looked(&service).await;
    let intakes: Vec<_> = {
        let live = service.inner.live.lock().await;
        added
            .iter()
            .map(|(job_id, _)| live[job_id].intake.clone().unwrap())
            .collect()
    };
    let mut moved = service.inner.disk_waits_moved.subscribe();
    moved
        .wait_for(|_| {
            locked(&service.inner.relook).jobs.len() == LOOKS
                && intakes.iter().all(|intake| !intake.room().placing())
        })
        .await
        .unwrap();
    passes_settled(&service).await;

    let holds: Vec<_> = added
        .iter()
        .map(|(_, dir)| weaver_server_disk::runtime::fs::test_devices::hold(dir))
        .collect();
    drop(refused);
    let views = service.views_built();
    capacity.request_refresh(StorageRoot::Complete);
    let built = views_of_looks(&service, &added, holds, views).await;
    assert!(
        built <= 3 * LOOKS,
        "{built} views built for {LOOKS} torrents looked up at one reading"
    );
    assert!(!locked(&service.inner.relook).waiting);
    service.stop().await;
}

// Many torrents restored at a start each look their disk up as their job
// is back: the looks of the restore pass ask one pass between them.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_looks_of_one_restore_pass_ask_one_pass() {
    let dir = tempfile::tempdir().unwrap();
    let db = Database::open_in_memory().unwrap();
    let routes = Arc::new(FakeRoutes::default());
    routes.set_down(Some("the leg is down"));
    let added = {
        let service = service_on(&db, dir.path(), &routes, job_env(), set_clock());
        service.open().await.unwrap();
        let added = add_looked(&service).await;
        service.stop().await;
        added
    };

    let capacity = looked_disks(dir.path());
    let service = service_on(&db, dir.path(), &routes, disk_env(&capacity), set_clock());
    service.open().await.unwrap();
    let holds: Vec<_> = added
        .iter()
        .map(|(_, dir)| weaver_server_disk::runtime::fs::test_devices::hold(dir))
        .collect();
    // The tick restores their jobs, each store's look held at its read.
    service.tick().await;
    service.restored().await;
    passes_settled(&service).await;
    let views = service.views_built();
    let built = views_of_looks(&service, &added, holds, views).await;
    assert!(
        built <= 3 * LOOKS,
        "{built} views built for {LOOKS} torrents restored in one pass"
    );
    service.stop().await;
}
