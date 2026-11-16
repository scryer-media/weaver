// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::*;

// The one file of `dir`'s tree.
fn only_file(dir: &Path) -> PathBuf {
    let mut found = Vec::new();
    let mut dirs = vec![dir.to_path_buf()];
    while let Some(dir) = dirs.pop() {
        for entry in std::fs::read_dir(dir).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                dirs.push(path);
            } else if path.file_name() != Some(std::ffi::OsStr::new(".weaver-output-dir")) {
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
        .add_opened(request(AddSource::Metainfo(bytes)))
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
        .add_opened(request(AddSource::Metainfo(plain("set", 1))))
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
        .add_opened(request(AddSource::Metainfo(bytes)))
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
    rig.db.prune_orphan_active_state(JobId(u64::MAX)).unwrap();

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
    rig.db.prune_orphan_active_state(JobId(u64::MAX)).unwrap();

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
        .add_opened(request(AddSource::Metainfo(plain("set", 1))))
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
