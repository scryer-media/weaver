// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::*;

// A member with an extension the operator does not want, found inside an
// archive the torrent carries while it is short of a piece, fails the job
// into history. The torrent stays, stopped with its data, and may be
// resumed.
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
            unwanted_extension_action:
                crate::post_processing::model::UnwantedExtensionAction::FailJob,
            ..PostProcessingSettings::default()
        })
        .unwrap();
    let bytes = metainfo("set.rar", &[("set.rar", archive)], 128, false);
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
    // The last piece never comes: the torrent is short when it is refused.
    for piece in 0..layout.piece_count() - 1 {
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
    assert_eq!(record.intent(), TorrentIntent::Stopped);
    let view = rig.service.torrent(job_id).await.unwrap();
    assert_eq!(view.state, TorrentState::Stopped);
    assert!(view.detail.unwrap().contains("txt"));

    // The job's outcome is kept once: resumed, the torrent seeds on.
    rig.service.resume(job_id).await.unwrap();
    rig.service.inner.signal.mark(job_id);
    rig.service.settle().await;
    assert_eq!(
        rig.db.get_torrent(job_id).unwrap().unwrap().intent(),
        TorrentIntent::Resumed
    );
    assert_eq!(*retired.borrow(), 1);
}

// A torrent of an archive keeps its archive as it came, seeds from it, and
// unpacks what it holds beside it; it completes and seeds on.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_archive_torrent_unpacks_beside_its_files_and_seeds_from_them() {
    let archive = include_bytes!(concat!(
        std::env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/rar5/rar5_store.rar"
    ));
    unpacks_beside_and_seeds("set.rar", archive).await;
}

// A torrent of a gzipped tar is an archive's too: its pieces feed the graph,
// which unpacks the tar's member beside it, and the torrent keeps the
// `.tar.gz` as it came and seeds from it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_tar_gz_torrent_unpacks_beside_its_file_and_seeds_from_it() {
    let mut tar = tar::Builder::new(Vec::new());
    let mut header = tar::Header::new_gnu();
    header.set_size(SMALL_TXT.len() as u64);
    header.set_mode(0o644);
    header.set_cksum();
    tar.append_data(&mut header, "small.txt", SMALL_TXT)
        .unwrap();
    let mut gzip = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
    std::io::Write::write_all(&mut gzip, &tar.into_inner().unwrap()).unwrap();
    unpacks_beside_and_seeds("set.tar.gz", &gzip.finish().unwrap()).await;
}

const SMALL_TXT: &[u8] = include_bytes!(concat!(
    std::env!("CARGO_MANIFEST_DIR"),
    "/tests/fixtures/originals/small.txt"
));

// Adds a torrent of one file, `name`, whose bytes are `archive` holding
// `small.txt`, and downloads it whole: the torrent gets a tee, completes
// without a history row, keeps `archive` as it came, unpacks `small.txt`
// beside it, and with that file gone still reads every piece from `archive`.
async fn unpacks_beside_and_seeds(name: &str, archive: &[u8]) {
    let member = SMALL_TXT;
    let rig = rig(set_clock());
    rig.service.open().await.unwrap();
    let bytes = metainfo(name, &[(name, archive)], 128, false);
    let job_id = rig
        .service
        .add_opened(request(AddSource::Metainfo(bytes)))
        .await
        .unwrap();
    let (layout, intake) = {
        let live = rig.service.inner.live.lock().await;
        let torrent = &live[&job_id];
        assert!(torrent.store.as_ref().unwrap().tee().is_some(), "{name}");
        (
            torrent.layout.clone().unwrap(),
            torrent.intake.clone().unwrap(),
        )
    };
    let mut published = rig.service.inner.published.subscribe();
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
        storage.on_piece_completed(index).unwrap();
    }
    // Every settle publishes what it did, completion among it.
    published
        .wait_for(|_| {
            rig.db
                .get_torrent(job_id)
                .unwrap()
                .unwrap()
                .completed_at_ms
                .is_some()
        })
        .await
        .unwrap();
    assert!(rig.db.get_job_history(job_id.0).unwrap().is_none());
    let view = rig.service.torrent(job_id).await.unwrap();
    let dir = PathBuf::from(view.storage_dir.unwrap());
    assert_eq!(std::fs::read(dir.join(name)).unwrap(), archive);
    assert_eq!(std::fs::read(dir.join("small.txt")).unwrap(), member);

    // The unpacked file gone, every piece still reads from the archive.
    std::fs::remove_file(dir.join("small.txt")).unwrap();
    for piece in 0..layout.piece_count() {
        let range = layout.piece_range(piece);
        for span in layout.spans(piece) {
            let at = (range.start + span.at) as usize;
            let mut read = vec![0; span.range.len() as usize];
            storage
                .pread_exact(span.file, span.range.start, &mut read)
                .unwrap();
            assert_eq!(read, archive[at..at + read.len()], "piece {piece}");
        }
    }
}

// A torrent with every file it wants whose unpacking refused its output
// is done downloading: it completes and seeds on, saying why, and its job
// goes to history as failed when nothing it delivered is a usable file, an
// archive volume being none. After a restart, before the tick restores its
// job, a removal goes to history as failed for the same reason.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_finished_torrent_unpacking_refused_seeds_on_and_fails_with_no_usable_file() {
    let archive = include_bytes!(concat!(
        std::env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/rar5/rar5_store.rar"
    ));
    let rig = rig(set_clock());
    rig.service.open().await.unwrap();
    rig.db
        .save_post_processing_settings(&PostProcessingSettings {
            unacceptable_extensions: vec!["txt".into()],
            unwanted_extension_action:
                crate::post_processing::model::UnwantedExtensionAction::FailJob,
            ..PostProcessingSettings::default()
        })
        .unwrap();
    let bytes = metainfo("set.rar", &[("set.rar", archive)], 128, false);
    let job_id = rig
        .service
        .add_opened(request(AddSource::Metainfo(bytes)))
        .await
        .unwrap();
    let mut retired = rig.service.retired();
    fetch_every_piece(&rig.service, job_id, archive).await;
    retired.wait_for(|retired| *retired >= 1).await.unwrap();
    let history = rig.db.get_job_history(job_id.0).unwrap().unwrap();
    assert_eq!(history.status, "failed");
    assert!(history.error_message.unwrap().contains("txt"));
    let view = rig.service.torrent(job_id).await.unwrap();
    assert!(view.completed_at_ms.is_some());
    assert_ne!(view.state, TorrentState::Stopped);
    rig.service.stop().await;
    rig.db.prune_orphan_active_state(JobId(u64::MAX)).unwrap();

    let again = service_on(&rig.db, rig.dir.path(), &rig.routes, job_env(), set_clock());
    again.open().await.unwrap();
    let view = again.torrent(job_id).await.unwrap();
    assert!(view.completed_at_ms.is_some());
    assert_ne!(view.state, TorrentState::Stopped);

    again.remove(job_id, false, false).await.unwrap();
    let history = rig.db.get_job_history(job_id.0).unwrap().unwrap();
    assert_eq!(history.status, "failed");
    assert!(history.error_message.unwrap().contains("txt"));
    again.stop().await;
}

// AGENTS.md "Job status: Partial": a finished torrent whose unpacking
// refused its archive, beside a file people open that it delivered whole,
// ends partial, with the refusal kept as why, and seeds on; its removal
// keeps it partial.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_finished_torrent_unpacking_refused_beside_a_usable_file_is_partial() {
    let archive = include_bytes!(concat!(
        std::env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/rar5/rar5_store.rar"
    ));
    let rig = rig(set_clock());
    rig.service.open().await.unwrap();
    rig.db
        .save_post_processing_settings(&PostProcessingSettings {
            unacceptable_extensions: vec!["txt".into()],
            unwanted_extension_action:
                crate::post_processing::model::UnwantedExtensionAction::FailJob,
            ..PostProcessingSettings::default()
        })
        .unwrap();
    let sample = vec![0x5a_u8; 300];
    let files: [(&str, &[u8]); 2] = [("set.rar", archive), ("sample.mkv", &sample)];
    let bytes = metainfo("set", &files, 128, false);
    let job_id = rig
        .service
        .add_opened(request(AddSource::Metainfo(bytes)))
        .await
        .unwrap();
    let mut retired = rig.service.retired();
    let whole: Vec<u8> = files.iter().flat_map(|(_, bytes)| bytes.to_vec()).collect();
    fetch_every_piece(&rig.service, job_id, &whole).await;
    retired.wait_for(|retired| *retired >= 1).await.unwrap();
    let history = rig.db.get_job_history(job_id.0).unwrap().unwrap();
    assert_eq!(history.status, "partial");
    assert!(history.error_message.unwrap().contains("txt"));
    let view = rig.service.torrent(job_id).await.unwrap();
    assert!(view.completed_at_ms.is_some());
    assert_ne!(view.state, TorrentState::Stopped);

    rig.service.remove(job_id, false, false).await.unwrap();
    let history = rig.db.get_job_history(job_id.0).unwrap().unwrap();
    assert_eq!(history.status, "partial");
    assert!(history.error_message.unwrap().contains("txt"));
    rig.service.stop().await;
}

// Under the delete action, a file unpacking made with an unwanted extension
// is deleted and never placed: the torrent's own files stay as they came,
// it completes and seeds on, and its job ends partial beside a file people
// open, naming what was removed.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_unwanted_unpacked_file_is_deleted_under_the_delete_action_and_the_job_is_partial() {
    use crate::post_processing::model::UnwantedExtensionAction;
    let archive = include_bytes!(concat!(
        std::env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/rar5/rar5_store.rar"
    ));
    let rig = rig(set_clock());
    rig.service.open().await.unwrap();
    rig.db
        .save_post_processing_settings(&PostProcessingSettings {
            unacceptable_extensions: vec!["txt".into()],
            unwanted_extension_action: UnwantedExtensionAction::DeleteFiles,
            ..PostProcessingSettings::default()
        })
        .unwrap();
    let sample = vec![0x5a_u8; 300];
    let files: [(&str, &[u8]); 2] = [("set.rar", archive), ("sample.mkv", &sample)];
    let bytes = metainfo("set", &files, 128, false);
    let job_id = rig
        .service
        .add_opened(request(AddSource::Metainfo(bytes)))
        .await
        .unwrap();
    let work = rig.service.unpacking_dir(job_id);
    let mut retired = rig.service.retired();
    let whole: Vec<u8> = files.iter().flat_map(|(_, bytes)| bytes.to_vec()).collect();
    fetch_every_piece(&rig.service, job_id, &whole).await;
    retired.wait_for(|retired| *retired >= 1).await.unwrap();
    let history = rig.db.get_job_history(job_id.0).unwrap().unwrap();
    assert_eq!(history.status, "partial");
    assert_eq!(
        history.error_message.as_deref(),
        Some("removed for an unwanted extension: small.txt")
    );
    let view = rig.service.torrent(job_id).await.unwrap();
    assert!(view.completed_at_ms.is_some());
    assert_ne!(view.state, TorrentState::Stopped);
    let dir = PathBuf::from(view.storage_dir.unwrap());
    assert_eq!(std::fs::read(dir.join("set.rar")).unwrap(), archive);
    assert_eq!(std::fs::read(dir.join("sample.mkv")).unwrap(), sample);
    assert!(!dir.join("small.txt").exists());
    assert!(!work.join("small.txt").exists());
    rig.service.stop().await;
}

// Unpacked files that cannot be placed beside the torrent's are never
// removed for it: they stay in its unpacking directory, the torrent
// completes and seeds on, and its job ends as what it delivered says,
// partial beside a file people open, with why and where they are. The
// directory stays past a restart, and a removal that deletes the
// torrent's files takes it.
#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unpacked_files_that_cannot_be_placed_are_kept_and_the_job_is_partial() {
    use std::os::unix::fs::PermissionsExt;
    let archive = include_bytes!(concat!(
        std::env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/rar5/rar5_store.rar"
    ));
    let rig = rig(set_clock());
    rig.service.open().await.unwrap();
    let sample = vec![0x5a_u8; 300];
    let files: [(&str, &[u8]); 2] = [("set.rar", archive), ("sample.mkv", &sample)];
    let bytes = metainfo("set", &files, 128, false);
    let job_id = rig
        .service
        .add_opened(request(AddSource::Metainfo(bytes)))
        .await
        .unwrap();
    let work = rig.service.unpacking_dir(job_id);
    let mut retired = rig.service.retired();
    let whole: Vec<u8> = files.iter().flat_map(|(_, bytes)| bytes.to_vec()).collect();
    let mut dir = PathBuf::new();
    fetch_pieces(&rig.service, job_id, &whole, None, |torrent| {
        dir = torrent.store.as_ref().unwrap().files().dir();
        std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o555)).unwrap();
    })
    .await;
    retired.wait_for(|retired| *retired >= 1).await.unwrap();
    std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o755)).unwrap();
    let history = rig.db.get_job_history(job_id.0).unwrap().unwrap();
    assert_eq!(history.status, "partial");
    let error = history.error_message.unwrap();
    assert!(error.contains("could not be placed"), "{error}");
    assert!(error.contains(&work.display().to_string()), "{error}");
    assert!(!dir.join("small.txt").exists());
    assert_eq!(std::fs::read(work.join("small.txt")).unwrap(), SMALL_TXT);
    assert!(rig.db.torrent_outputs(job_id).unwrap().is_empty());
    let view = rig.service.torrent(job_id).await.unwrap();
    assert!(view.completed_at_ms.is_some());
    assert_ne!(view.state, TorrentState::Stopped);
    rig.service.stop().await;
    rig.db.prune_orphan_active_state(JobId(u64::MAX)).unwrap();

    // The same unpacking root, as a restart has.
    let mut env = job_env();
    env.work = rig.service.inner.env.work.clone();
    let again = service_on(&rig.db, rig.dir.path(), &rig.routes, env, set_clock());
    again.open().await.unwrap();
    assert_eq!(std::fs::read(work.join("small.txt")).unwrap(), SMALL_TXT);
    again.remove(job_id, true, false).await.unwrap();
    assert!(!work.exists());
    assert!(!dir.join("sample.mkv").exists());
    let history = rig.db.get_job_history(job_id.0).unwrap().unwrap();
    assert_eq!(history.status, "partial");
    again.stop().await;
}

// A tee checkpoints its job as a job of the pipeline does: the pieces it
// takes write nothing while their bytes stay under the byte bound and the
// first of them is younger than the age bound; crossing either queues one
// write. Taken out of its session, the torrent keeps its tee, which writes
// on the same bounds.
#[tokio::test(start_paused = true)]
async fn a_tee_writes_its_checkpoint_by_bytes_and_age_in_a_session_or_out() {
    use crate::pipeline::graph::checkpoint::{CHECKPOINT_AGE, CHECKPOINT_BYTES};
    // The paused clock moves only when the test moves it: the database
    // works on its own thread, and is waited on from a blocking task, which
    // keeps the paused clock from moving by itself while it runs.
    let flushed = |db: &Database| {
        let (db, runtime) = (db.clone(), tokio::runtime::Handle::current());
        tokio::task::spawn_blocking(move || runtime.block_on(db.flush_write_queue()).unwrap())
    };
    let writes = |db: &Database| {
        db.graph_checkpoint_writes
            .load(std::sync::atomic::Ordering::Relaxed)
    };
    let archive = include_bytes!(concat!(
        std::env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/rar5/rar5_store.rar"
    ));
    let piece = 32;
    let rig = rig(set_clock());
    rig.routes.set_down(Some("down"));
    rig.service.open().await.unwrap();
    let job_id = rig
        .service
        .add_opened(request(AddSource::Metainfo(metainfo(
            "set.rar",
            &[("set.rar", archive)],
            piece,
            false,
        ))))
        .await
        .unwrap();
    let tee = tee_of(&rig.service, job_id).await;
    let deliver = |index: usize| {
        let held = bytes::Bytes::copy_from_slice(&archive[index * piece..(index + 1) * piece]);
        assert!(tee.deliver(index as u32, &held));
    };
    tee.checkpoint(true).unwrap();
    flushed(&rig.db).await.unwrap();
    let before = writes(&rig.db);

    // Under both bounds: nothing is written.
    deliver(0);
    flushed(&rig.db).await.unwrap();
    assert_eq!(writes(&rig.db), before);

    // The byte bound crossed: one write.
    tee.with(|job| job.pending().delivered(CHECKPOINT_BYTES));
    deliver(1);
    flushed(&rig.db).await.unwrap();
    assert_eq!(writes(&rig.db), before + 1);

    // The age bound crossed: the wake the step armed writes one, once the
    // tee has taken it.
    let mut taken = tee.taken();
    deliver(2);
    flushed(&rig.db).await.unwrap();
    assert_eq!(writes(&rig.db), before + 1);
    tokio::time::advance(CHECKPOINT_AGE).await;
    while writes(&rig.db) == before + 1 {
        taken.changed().await.unwrap();
        flushed(&rig.db).await.unwrap();
    }
    assert_eq!(writes(&rig.db), before + 2);

    // Taken out of its session, the torrent writes nothing of its tee's
    // until the tee's write is due.
    {
        let mut live = rig.service.inner.live.lock().await;
        let torrent = live.get_mut(&job_id).unwrap();
        rig.service.detach(torrent).await;
    }
    deliver(3);
    flushed(&rig.db).await.unwrap();
    assert_eq!(writes(&rig.db), before + 2);
    tokio::time::advance(CHECKPOINT_AGE).await;
    while writes(&rig.db) == before + 2 {
        taken.changed().await.unwrap();
        flushed(&rig.db).await.unwrap();
    }
    assert_eq!(writes(&rig.db), before + 3);
    rig.service.stop().await;
}

// Pausing one torrent holds its tee's extraction clock, as every download
// paused does; the clock runs again only once neither holds it.
#[tokio::test(start_paused = true)]
async fn a_paused_torrent_holds_its_tees_extraction_clock() {
    // Waits for the tee to have taken `count` words on its clock.
    async fn clocked_to(clocked: &mut tokio::sync::watch::Receiver<u64>, count: u64) {
        while *clocked.borrow_and_update() < count {
            clocked.changed().await.unwrap();
        }
    }
    let archive = include_bytes!(concat!(
        std::env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/rar5/rar5_store.rar"
    ));
    let rig = rig(set_clock());
    rig.routes.set_down(Some("down"));
    rig.service.open().await.unwrap();
    let job_id = rig
        .service
        .add_opened(request(AddSource::Metainfo(metainfo(
            "set.rar",
            &[("set.rar", archive)],
            128,
            false,
        ))))
        .await
        .unwrap();
    let tee = tee_of(&rig.service, job_id).await;
    let held = || tee.with(|job| job.clock_held());
    let mut clocked = tee.clocked();
    // Its clock's words as it joined and as its store was given.
    clocked_to(&mut clocked, 2).await;
    assert!(!held());

    rig.service.pause(job_id).await.unwrap();
    clocked_to(&mut clocked, 3).await;
    assert!(held());

    // Every download paused too: resuming the torrent leaves the clock
    // held until they resume.
    rig.service.inner.env.outside.hold_clocks(true);
    clocked_to(&mut clocked, 4).await;
    rig.service.resume(job_id).await.unwrap();
    clocked_to(&mut clocked, 5).await;
    assert!(held());
    rig.service.inner.env.outside.hold_clocks(false);
    clocked_to(&mut clocked, 6).await;
    assert!(!held());
    rig.service.stop().await;
}

// A torrent paused on its own takes no slot, as a paused job of the
// pipeline's takes none: its pieces go to its job and no run starts. Once
// it resumes, its runs start again.
#[tokio::test(start_paused = true)]
async fn a_paused_torrents_tee_starts_no_run_until_resumed() {
    async fn clocked_to(clocked: &mut tokio::sync::watch::Receiver<u64>, count: u64) {
        while *clocked.borrow_and_update() < count {
            clocked.changed().await.unwrap();
        }
    }
    let archive = include_bytes!(concat!(
        std::env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/rar5/rar5_multifile_lz.rar"
    ));
    let piece = 64;
    let rig = rig(set_clock());
    rig.routes.set_down(Some("down"));
    rig.service.open().await.unwrap();
    let job_id = rig
        .service
        .add_opened(request(AddSource::Metainfo(metainfo(
            "set.rar",
            &[("set.rar", archive)],
            piece,
            false,
        ))))
        .await
        .unwrap();
    let tee = tee_of(&rig.service, job_id).await;
    let mut clocked = tee.clocked();
    clocked_to(&mut clocked, 2).await;
    rig.service.pause(job_id).await.unwrap();
    clocked_to(&mut clocked, 3).await;

    for (index, held) in archive.chunks(piece).enumerate() {
        assert!(tee.deliver(index as u32, &bytes::Bytes::copy_from_slice(held)));
    }
    assert!(!tee.with(|job| job.workers_out()));
    assert!(!tee.done());

    rig.service.resume(job_id).await.unwrap();
    clocked_to(&mut clocked, 4).await;
    assert!(tee.done() || tee.with(|job| job.workers_out()));
    let mut taken = tee.taken();
    while !tee.done() {
        taken.changed().await.unwrap();
    }
    rig.service.stop().await;
}

// A stop does not hold the service's lock while a tee's checkpoint waits
// on the job: an intake writing to disk may hold the job, and every other
// torrent goes on meanwhile.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_stop_waiting_on_an_unpacking_job_holds_no_other_torrent_up() {
    let archive = include_bytes!(concat!(
        std::env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/rar5/rar5_store.rar"
    ));
    let rig = rig(set_clock());
    rig.service.open().await.unwrap();
    let job_id = rig
        .service
        .add_opened(request(AddSource::Metainfo(metainfo(
            "set.rar",
            &[("set.rar", archive)],
            128,
            false,
        ))))
        .await
        .unwrap();
    let tee = {
        let live = rig.service.inner.live.lock().await;
        Arc::clone(live[&job_id].store.as_ref().unwrap().tee().unwrap())
    };
    let tee_asked = Arc::clone(&tee);
    let tee_asked = &tee_asked.asked;
    let (held_tx, held_rx) = std::sync::mpsc::channel();
    let (release_tx, release_rx) = std::sync::mpsc::channel::<()>();
    let holder = std::thread::spawn(move || {
        tee.with(|_| {
            held_tx.send(()).unwrap();
            release_rx.recv().unwrap();
        });
    });
    held_rx.recv().unwrap();
    let mut asked = tee_asked.subscribe();
    let service = rig.service.clone();
    let save = tokio::spawn(async move { service.stop().await });
    // The stop reached the job's checkpoint, and cannot finish while the
    // job is held; the service's lock is free all the same.
    asked.wait_for(|asked| *asked > 0).await.unwrap();
    drop(rig.service.inner.live.lock().await);
    assert!(!save.is_finished());
    release_tx.send(()).unwrap();
    holder.join().unwrap();
    save.await.unwrap();
    rig.db.flush_write_queue().await.unwrap();
}

// A recovery set made over the files of a torrent, plain ones among them.
fn torrent_parity(files: &[(&str, &[u8])], blocks: u32) -> Vec<(String, Vec<u8>)> {
    use par2_rs::create::{BlockSizing, Par2Creator, Par2CreatorOptions, RecoveryAmount};
    let directory = tempfile::tempdir().unwrap();
    let base = directory.path().join("protected");
    std::fs::create_dir_all(&base).unwrap();
    let paths = files
        .iter()
        .map(|(name, bytes)| {
            let path = base.join(name);
            std::fs::write(&path, bytes).unwrap();
            path
        })
        .collect();
    let output = directory.path().join("out");
    std::fs::create_dir_all(&output).unwrap();
    let mut options = Par2CreatorOptions::with_output(output.join("set.par2"), Some(base), paths);
    options.block_sizing = BlockSizing::Bytes(512);
    options.recovery_amount = RecoveryAmount::Count(blocks);
    let creator = Par2Creator::new(options);
    let created = creator.create(&creator.plan().unwrap()).unwrap();
    created
        .output_paths
        .iter()
        .map(|path| {
            (
                path.file_name().unwrap().to_str().unwrap().to_owned(),
                std::fs::read(path).unwrap(),
            )
        })
        .collect()
}

// A torrent of an archive and a plain file under one recovery set: the
// plain file never goes to the graph, the set's word on it notwithstanding.
// Every piece whole, the archive unpacks, the torrent completes, and no
// repair is tried over the file the graph never had.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_archive_and_a_plain_file_under_one_recovery_set_unpack_and_complete() {
    let archive: &[u8] = include_bytes!(concat!(
        std::env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/rar5/rar5_store.rar"
    ));
    let member = include_bytes!(concat!(
        std::env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/originals/small.txt"
    ));
    let notes: Vec<u8> = (0..1500u32).map(|at| (at * 7 % 251) as u8).collect();
    let parity = torrent_parity(&[("set.rar", archive), ("notes.txt", &notes)], 2);
    let mut files: Vec<(String, Vec<u8>)> = vec![
        ("set.rar".to_owned(), archive.to_vec()),
        ("notes.txt".to_owned(), notes.clone()),
    ];
    files.extend(parity);
    let listed: Vec<(&str, &[u8])> = files
        .iter()
        .map(|(name, bytes)| (name.as_str(), bytes.as_slice()))
        .collect();
    let whole: Vec<u8> = files.iter().flat_map(|(_, bytes)| bytes.clone()).collect();
    let rig = rig(set_clock());
    rig.service.open().await.unwrap();
    let job_id = rig
        .service
        .add_opened(request(AddSource::Metainfo(metainfo(
            "mixed", &listed, 256, false,
        ))))
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
    let plan = crate::torrents::tee::Plan::of(&layout).unwrap();
    let notes_at = layout
        .files()
        .position(|file| file.name.ends_with("notes.txt"))
        .unwrap();
    assert_eq!(plan.volume(notes_at), None);
    let mut published = rig.service.inner.published.subscribe();
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
                    &whole[at..at + span.range.len() as usize],
                )
                .unwrap();
        }
        storage.on_piece_completed(index).unwrap();
    }
    published
        .wait_for(|_| {
            rig.db.get_job_history(job_id.0).unwrap().is_some()
                || rig
                    .db
                    .get_torrent(job_id)
                    .unwrap()
                    .unwrap()
                    .completed_at_ms
                    .is_some()
        })
        .await
        .unwrap();
    assert_eq!(
        rig.db
            .get_job_history(job_id.0)
            .unwrap()
            .map(|row| row.error_message),
        None
    );
    let view = rig.service.torrent(job_id).await.unwrap();
    let dir = PathBuf::from(view.storage_dir.unwrap());
    assert_eq!(std::fs::read(dir.join("small.txt")).unwrap(), member);
    assert_eq!(std::fs::read(dir.join("notes.txt")).unwrap(), notes);
    assert_eq!(std::fs::read(dir.join("set.rar")).unwrap(), archive);
    rig.service.stop().await;
}

// Feed `pieces` of a torrent whole, from `payload`, through its storage.
async fn feed(
    service: &TorrentService,
    job_id: JobId,
    payload: &[u8],
    pieces: std::ops::Range<u32>,
) {
    let (layout, intake) = {
        let live = service.inner.live.lock().await;
        let torrent = &live[&job_id];
        (
            torrent.layout.clone().unwrap(),
            torrent.intake.clone().unwrap(),
        )
    };
    let storage = PieceStorage::new(
        Arc::clone(&layout),
        intake as Arc<dyn Pieces>,
        service.resident(job_id),
    );
    let lengths = Lengths::new(
        layout.files().map(|file| file.length).sum(),
        layout.piece_length() as u32,
    )
    .unwrap();
    for piece in pieces {
        let range = layout.piece_range(piece);
        let index = lengths.try_validate_piece_index(piece).unwrap();
        assert!(storage.reserve_piece(index, &Arc::default()));
        for span in layout.spans(piece) {
            let at = (range.start + span.at) as usize;
            storage
                .pwrite_all(
                    span.file,
                    span.range.start,
                    &payload[at..at + span.range.len() as usize],
                )
                .unwrap();
        }
        storage.on_piece_completed(index).unwrap();
    }
}

// A torrent part delivered when the server stops takes its unpacking back
// from the checkpoint it wrote, not from nothing, and with the rest of its
// pieces unpacks and completes.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_part_delivered_unpacking_restored_from_its_checkpoint_unpacks() {
    let archive: &[u8] = include_bytes!(concat!(
        std::env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/rar5/rar5_store.rar"
    ));
    let member = include_bytes!(concat!(
        std::env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/originals/small.txt"
    ));
    let rig = rig(set_clock());
    rig.service.open().await.unwrap();
    let job_id = rig
        .service
        .add_opened(request(AddSource::Metainfo(metainfo(
            "set.rar",
            &[("set.rar", archive)],
            128,
            false,
        ))))
        .await
        .unwrap();
    let count = {
        let live = rig.service.inner.live.lock().await;
        live[&job_id].layout.as_ref().unwrap().piece_count()
    };
    let half = count / 2;
    assert!(half > 0 && half < count);
    feed(&rig.service, job_id, archive, 0..half).await;
    save_with_tee(&rig.service, job_id).await;
    rig.service.stop().await;
    rig.db.flush_write_queue().await.unwrap();
    let rows = rig.db.load_graph_checkpoint(job_id).unwrap().unwrap();
    assert!(!rows.nodes.is_empty());

    let (lines, _guard) = crate::pipeline::graph::lines::Lines::capture_at(
        "weaver_server_core::torrents",
        tracing::Level::WARN,
    );
    let again = service_on(&rig.db, rig.dir.path(), &rig.routes, job_env(), set_clock());
    reopen(&again).await;
    assert!(
        lines
            .of("a torrent's unpacking checkpoint did not restore; it starts fresh")
            .is_empty()
    );
    let mut published = again.inner.published.subscribe();
    feed(&again, job_id, archive, half..count).await;
    published
        .wait_for(|_| {
            rig.db.get_job_history(job_id.0).unwrap().is_some()
                || rig
                    .db
                    .get_torrent(job_id)
                    .unwrap()
                    .unwrap()
                    .completed_at_ms
                    .is_some()
        })
        .await
        .unwrap();
    assert!(rig.db.get_job_history(job_id.0).unwrap().is_none());
    let dir = PathBuf::from(again.torrent(job_id).await.unwrap().storage_dir.unwrap());
    assert_eq!(std::fs::read(dir.join("small.txt")).unwrap(), member);
    assert_eq!(std::fs::read(dir.join("set.rar")).unwrap(), archive);
    again.stop().await;
}

// A seed-mode add of an archive already in place unpacks it beside its
// files from what the files hold, and completes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_seed_mode_add_of_an_archive_unpacks_it_and_completes() {
    let archive: &[u8] = include_bytes!(concat!(
        std::env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/rar5/rar5_store.rar"
    ));
    let member = include_bytes!(concat!(
        std::env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/originals/small.txt"
    ));
    let rig = rig(set_clock());
    on_loopback(&rig.routes);
    rig.service.open().await.unwrap();
    let save = rig.dir.path().join("seeded");
    std::fs::create_dir_all(save.join("placed")).unwrap();
    std::fs::write(save.join("placed/set.rar"), archive).unwrap();
    let handle = queue();
    let mut changes = handle.state().subscribe_job_changes();
    rig.service.publish_to(handle);
    let job_id = rig
        .service
        .add_opened(AddRequest {
            placement: Placement {
                save_path: Some(save.to_string_lossy().into_owned()),
                seed_mode: true,
                ..Placement::default()
            },
            unpack: Some(true),
            ..request(AddSource::Metainfo(metainfo(
                "placed",
                &[("set.rar", archive)],
                1024,
                false,
            )))
        })
        .await
        .unwrap();
    loop {
        changes.borrow_and_update();
        let stored = rig.db.get_torrent(job_id).unwrap().unwrap();
        if stored.completed_at_ms.is_some() || rig.db.get_job_history(job_id.0).unwrap().is_some() {
            break;
        }
        changes.changed().await.unwrap();
    }
    assert!(rig.db.get_job_history(job_id.0).unwrap().is_none());
    assert_eq!(
        std::fs::read(save.join("placed/small.txt")).unwrap(),
        member
    );
    rig.service.stop().await;
}

// A seed-mode add whose archive file is damaged gives the graph none of
// it: the piece's seed-mode check fails, seed mode ends, every piece is
// checked again, and the piece fetched from a peer is the one the graph
// takes. The archive unpacks whole and the torrent completes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_seed_mode_archive_with_a_bad_file_is_fetched_again_and_unpacks_whole() {
    let archive: &[u8] = include_bytes!(concat!(
        std::env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/rar5/rar5_store.rar"
    ));
    let member = include_bytes!(concat!(
        std::env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/originals/small.txt"
    ));
    let files = [("set.rar", archive)];
    let bytes = metainfo("placed", &files, 1024, false);
    let seeds = tempfile::tempdir().unwrap();
    let seeding = seeder(seeds.path(), &bytes, &files).await;
    let std::net::SocketAddr::V4(peer) = seeding.listen_addr().unwrap() else {
        panic!("the seeder listens on IPv4 loopback");
    };
    let url = tracker(peer).await;

    let rig = rig(set_clock());
    on_loopback(&rig.routes);
    rig.service.open().await.unwrap();
    let save = rig.dir.path().join("seeded");
    std::fs::create_dir_all(save.join("placed")).unwrap();
    let mut damaged = archive.to_vec();
    let middle = damaged.len() / 2;
    damaged[middle] ^= 0xff;
    std::fs::write(save.join("placed/set.rar"), &damaged).unwrap();
    let handle = queue();
    let mut changes = handle.state().subscribe_job_changes();
    rig.service.publish_to(handle);
    let job_id = rig
        .service
        .add_opened(AddRequest {
            placement: Placement {
                save_path: Some(save.to_string_lossy().into_owned()),
                seed_mode: true,
                ..Placement::default()
            },
            unpack: Some(true),
            ..request(AddSource::Metainfo(bytes))
        })
        .await
        .unwrap();
    tracked(&rig.service, job_id, &url).await;
    loop {
        changes.borrow_and_update();
        let stored = rig.db.get_torrent(job_id).unwrap().unwrap();
        if stored.completed_at_ms.is_some() || rig.db.get_job_history(job_id.0).unwrap().is_some() {
            break;
        }
        changes.changed().await.unwrap();
    }
    assert!(rig.db.get_job_history(job_id.0).unwrap().is_none());
    assert_eq!(std::fs::read(save.join("placed/set.rar")).unwrap(), archive);
    assert_eq!(
        std::fs::read(save.join("placed/small.txt")).unwrap(),
        member
    );
    rig.service.stop().await;
    seeding.stop().await;
}
