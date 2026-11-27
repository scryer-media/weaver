// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

use super::*;
use crate::history::queries::HistoryQueries;
use crate::jobs::graph_checkpoint::JobsGraphCheckpointInternalQueries;
use crate::jobs::repository::JobsRepositoryQueries;
use crate::post_processing::settings::PostProcessingSettingsQueries;
use crate::settings::SettingsArchivePasswordsQueries;
use crate::torrents::persistence::TorrentsPersistenceInsertPlacedTorrentInternalQueries;
use crate::torrents::queries::TorrentsQueries;

// A member with an extension the operator does not want, found inside an
// archive the torrent carries while it is short of a piece, ends its
// unpacking refused and nothing of the torrent's: no history row, no
// failure, no stop. It downloads on, completes and seeds, as a torrent with
// nothing to unpack does, its unpacking line saying why.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_unwanted_member_found_while_unpacking_refuses_the_unpacking_and_the_torrent_completes()
{
    let archive = include_bytes!(concat!(
        std::env!("CARGO_MANIFEST_DIR"),
        "/../weaver-server-core/tests/fixtures/rar5/rar5_store.rar"
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
    let count = {
        let live = rig.service.inner.live.lock().await;
        live[&job_id].layout.as_ref().unwrap().piece_count()
    };
    let retired = rig.service.retired();
    let mut settles = rig.service.inner.settled.subscribe();
    // The last piece not yet come: the torrent is short when it is refused.
    feed(&rig.service, job_id, archive, 0..count - 1).await;
    rig.service.inner.signal.look(job_id);
    settles
        .wait_for(|_| {
            rig.db
                .get_torrent(job_id)
                .unwrap()
                .unwrap()
                .unpack_end
                .is_some()
        })
        .await
        .unwrap();
    let end = rig
        .db
        .get_torrent(job_id)
        .unwrap()
        .unwrap()
        .unpack_end
        .unwrap();
    assert_eq!(end.result, UnpackResult::Refused);
    assert_eq!(end.cause, Some(UnpackCause::Refused));
    assert!(end.why.as_deref().unwrap().contains("txt"), "{end:?}");
    assert!(rig.db.get_job_history(job_id.0).unwrap().is_none());
    assert_eq!(
        rig.db.get_torrent(job_id).unwrap().unwrap().intent(),
        TorrentIntent::Resumed
    );
    let view = rig.service.torrent(job_id).await.unwrap();
    assert_ne!(view.state, TorrentState::Stopped);
    assert_eq!(view.detail, None);
    assert_eq!(view.unpack_end, Some(end.clone()));

    // The rest comes: it completes and seeds, nothing unpacked.
    feed(&rig.service, job_id, archive, count - 1..count).await;
    stored_complete(&rig.service, &rig.db, job_id).await;
    let view = rig.service.torrent(job_id).await.unwrap();
    assert_eq!(view.state, TorrentState::Seeding);
    assert_eq!(view.detail, None);
    assert_eq!(job_status(&view), JobStatus::Seeding);
    assert_eq!(view.unpack_end, Some(end));
    assert!(rig.db.get_job_history(job_id.0).unwrap().is_none());
    assert!(rig.db.torrent_outputs(job_id).unwrap().is_empty());
    assert_eq!(*retired.borrow(), 0);
    assert_eq!(rig.service.inner.counters.count(TorrentEvent::Failed), 0);
    rig.service.stop().await;
}

// A torrent of an archive keeps its archive as it came, seeds from it, and
// unpacks what it holds beside it; it completes and seeds on.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_archive_torrent_unpacks_beside_its_files_and_seeds_from_them() {
    let archive = include_bytes!(concat!(
        std::env!("CARGO_MANIFEST_DIR"),
        "/../weaver-server-core/tests/fixtures/rar5/rar5_store.rar"
    ));
    unpacks_beside_and_seeds("set.rar", archive, 128, small_txt_beside).await;
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
    unpacks_beside_and_seeds("set.tar.gz", &gzip.finish().unwrap(), 128, small_txt_beside).await;
}

// A torrent of a compressed RAR that holds a file beside an `.xz` of
// another unpacks both layers: the file, and what the `.xz` holds in place
// of it, beside the archive. It completes and seeds on.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_compressed_rar_torrent_holding_an_xz_unpacks_both_layers_and_completes() {
    let archive = std::fs::read(concat!(
        std::env!("CARGO_MANIFEST_DIR"),
        "/../../../e2e/testdata/nested-xz-rar/archive.rar"
    ))
    .unwrap();
    unpacks_beside_and_seeds("archive.rar", &archive, 64 * 1024, |dir| {
        assert!(dir.join("test-media.mkv").is_file());
        assert!(
            std::fs::read_to_string(dir.join("release.nfo"))
                .unwrap()
                .starts_with("Nested XZ sidecar beside ordinary media.")
        );
        assert!(!dir.join("release.nfo.xz").exists());
        std::fs::remove_file(dir.join("test-media.mkv")).unwrap();
    })
    .await;
}

// `small.txt` lies beside the archive, as it came; it is taken away.
fn small_txt_beside(dir: &Path) {
    assert_eq!(std::fs::read(dir.join("small.txt")).unwrap(), SMALL_TXT);
    std::fs::remove_file(dir.join("small.txt")).unwrap();
}

const SMALL_TXT: &[u8] = include_bytes!(concat!(
    std::env!("CARGO_MANIFEST_DIR"),
    "/../weaver-server-core/tests/fixtures/originals/small.txt"
));

// Adds a torrent of one file, `name`, whose bytes are `archive`, in pieces
// of `piece` bytes, and downloads it whole: the torrent gets a tee,
// completes without a history row, keeps `archive` as it came, and has what
// `beside` checks unpacked beside it; with that taken away, it still reads
// every piece from `archive`.
async fn unpacks_beside_and_seeds(
    name: &str,
    archive: &[u8],
    piece: usize,
    beside: impl Fn(&Path),
) {
    let rig = rig(set_clock());
    rig.service.open().await.unwrap();
    let bytes = metainfo(name, &[(name, archive)], piece, false);
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
    beside(&dir);

    // The unpacked files gone, every piece still reads from the archive.
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
// is done downloading: it completes and seeds on with no history row, its
// unpacking line saying why. After a restart its line is the same, and a
// removal goes to history complete, as any complete torrent's does.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_finished_torrent_unpacking_refused_seeds_on_and_its_removal_is_complete() {
    let archive = include_bytes!(concat!(
        std::env!("CARGO_MANIFEST_DIR"),
        "/../weaver-server-core/tests/fixtures/rar5/rar5_store.rar"
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
    let whole: Vec<u8> = files.iter().flat_map(|(_, bytes)| bytes.to_vec()).collect();
    fetch_every_piece(&rig.service, job_id, &whole).await;
    stored_complete(&rig.service, &rig.db, job_id).await;
    let view = rig.service.torrent(job_id).await.unwrap();
    assert_eq!(view.state, TorrentState::Seeding);
    assert_eq!(view.detail, None);
    let end = view.unpack_end.clone().unwrap();
    assert_eq!(end.result, UnpackResult::Refused);
    assert_eq!(end.cause, Some(UnpackCause::Refused));
    assert!(end.why.as_deref().unwrap().contains("txt"), "{end:?}");
    assert!(rig.db.get_job_history(job_id.0).unwrap().is_none());
    assert_eq!(*rig.service.retired().borrow(), 0);
    rig.service.stop().await;
    rig.db.prune_orphan_active_state(JobId(u64::MAX)).unwrap();

    let again = service_on(&rig.db, rig.dir.path(), &rig.routes, job_env(), set_clock());
    again.open().await.unwrap();
    let view = again.torrent(job_id).await.unwrap();
    assert!(view.completed_at_ms.is_some());
    assert_ne!(view.state, TorrentState::Stopped);
    assert_eq!(view.unpack_end, Some(end));

    again.remove(job_id, false).await.unwrap();
    let history = rig.db.get_job_history(job_id.0).unwrap().unwrap();
    assert_eq!(
        (history.status.as_str(), history.error_message),
        ("complete", None)
    );
    again.stop().await;
}

// Under the delete action, a file unpacking made with an unwanted extension
// is deleted and never placed: the torrent's own files stay as they came,
// it completes and seeds on with no history row, and its unpacking line
// names what was removed.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_unwanted_unpacked_file_is_deleted_under_the_delete_action_and_said_on_the_unpacking() {
    use crate::post_processing::model::UnwantedExtensionAction;
    let archive = include_bytes!(concat!(
        std::env!("CARGO_MANIFEST_DIR"),
        "/../weaver-server-core/tests/fixtures/rar5/rar5_store.rar"
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
    let whole: Vec<u8> = files.iter().flat_map(|(_, bytes)| bytes.to_vec()).collect();
    fetch_every_piece(&rig.service, job_id, &whole).await;
    stored_complete(&rig.service, &rig.db, job_id).await;
    assert!(rig.db.get_job_history(job_id.0).unwrap().is_none());
    let view = rig.service.torrent(job_id).await.unwrap();
    assert_eq!(view.state, TorrentState::Seeding);
    assert_eq!(view.detail, None);
    assert_eq!(
        view.unpack_end,
        Some(UnpackEnd::short(
            false,
            UnpackCause::DeliveredInPart,
            "delivered in part; removed for an unwanted extension: small.txt".into()
        ))
    );
    let dir = PathBuf::from(view.storage_dir.unwrap());
    assert_eq!(std::fs::read(dir.join("set.rar")).unwrap(), archive);
    assert_eq!(std::fs::read(dir.join("sample.mkv")).unwrap(), sample);
    assert!(!dir.join("small.txt").exists());
    assert!(!work.join("small.txt").exists());
    rig.service.stop().await;
}

// A placing that moved every file whole ends short when the policy removed
// a file it made, under UNWANTED_REMOVED, and under UNWANTED_NOT_REMOVED
// when the removal failed; the row keeps that cause and the torrent shows
// it. A delivery in part or a file not placed wins over either.
#[tokio::test]
async fn a_placing_the_policy_left_a_file_out_of_ends_on_the_removal() {
    use crate::torrents::service::placing_end;
    let removed = placing_end(true, None, None, Ok(vec!["small.txt".to_owned()]));
    assert_eq!(
        removed,
        UnpackEnd::short(
            true,
            UnpackCause::UnwantedRemoved,
            "removed for an unwanted extension: small.txt".into()
        )
    );
    let not_removed = placing_end(true, None, None, Err("could not remove small.txt".into()));
    assert_eq!(
        not_removed,
        UnpackEnd::short(
            true,
            UnpackCause::UnwantedNotRemoved,
            "could not remove small.txt".into()
        )
    );
    assert_eq!(
        placing_end(true, None, None, Ok(Vec::new())),
        UnpackEnd::whole()
    );
    assert_eq!(
        placing_end(true, None, Some("in part".into()), Ok(vec!["a.txt".into()])).cause,
        Some(UnpackCause::DeliveredInPart)
    );
    assert_eq!(
        placing_end(true, Some("not moved".into()), None, Err("x".into())).cause,
        Some(UnpackCause::NotPlaced)
    );

    let rig = rig(set_clock());
    rig.service.open().await.unwrap();
    let job_id = rig
        .service
        .add_opened(request(AddSource::Metainfo(metainfo(
            "set.rar",
            &[("set.rar", b"rar")],
            128,
            false,
        ))))
        .await
        .unwrap();
    for end in [&removed, &not_removed] {
        rig.db
            .finish_torrent_unpack(job_id, &["notes.mkv".to_owned()], &[], &[], end)
            .unwrap();
        assert_eq!(
            rig.db
                .get_torrent(job_id)
                .unwrap()
                .unwrap()
                .unpack_end
                .as_ref(),
            Some(end)
        );
    }
    rig.service.stop().await;
    rig.db.prune_orphan_active_state(JobId(u64::MAX)).unwrap();
    let again = service_on(&rig.db, rig.dir.path(), &rig.routes, job_env(), set_clock());
    again.open().await.unwrap();
    let view = again.torrent(job_id).await.unwrap();
    assert_eq!(view.unpack_end, Some(not_removed));
    again.stop().await;
}

// Unpacked files that cannot be placed beside the torrent's are never
// removed for it: they stay in its unpacking directory, the torrent
// completes and seeds on with no history row, and its unpacking line says
// why and where they are. The directory stays past a restart, and a removal
// that deletes the torrent's files takes it.
#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn unpacked_files_that_cannot_be_placed_are_kept_and_said_on_the_unpacking() {
    use std::os::unix::fs::PermissionsExt;
    let archive = include_bytes!(concat!(
        std::env!("CARGO_MANIFEST_DIR"),
        "/../weaver-server-core/tests/fixtures/rar5/rar5_store.rar"
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
    let whole: Vec<u8> = files.iter().flat_map(|(_, bytes)| bytes.to_vec()).collect();
    let mut dir = PathBuf::new();
    fetch_pieces(&rig.service, job_id, &whole, None, |torrent| {
        dir = torrent.store.as_ref().unwrap().files().dir();
        std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o555)).unwrap();
    })
    .await;
    stored_complete(&rig.service, &rig.db, job_id).await;
    std::fs::set_permissions(&dir, std::fs::Permissions::from_mode(0o755)).unwrap();
    assert!(rig.db.get_job_history(job_id.0).unwrap().is_none());
    let end = rig
        .db
        .get_torrent(job_id)
        .unwrap()
        .unwrap()
        .unpack_end
        .unwrap();
    assert_eq!(end.result, UnpackResult::Failed);
    assert_eq!(end.cause, Some(UnpackCause::NotPlaced));
    let error = end.why.unwrap();
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
    again.remove(job_id, true).await.unwrap();
    assert!(!work.exists());
    assert!(!dir.join("sample.mkv").exists());
    let history = rig.db.get_job_history(job_id.0).unwrap().unwrap();
    assert_eq!(history.status, "complete");
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
        "/../weaver-server-core/tests/fixtures/rar5/rar5_store.rar"
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
        "/../weaver-server-core/tests/fixtures/rar5/rar5_store.rar"
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
        "/../weaver-server-core/tests/fixtures/rar5/rar5_multifile_lz.rar"
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
        "/../weaver-server-core/tests/fixtures/rar5/rar5_store.rar"
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
        live[&job_id].store.as_ref().unwrap().tee().unwrap()
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
// plain file goes to the graph, as a post's does, and the set finds it
// whole. Every piece whole, the archive unpacks, the torrent completes, and
// the torrent's own files stay as they came.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_archive_and_a_plain_file_under_one_recovery_set_unpack_and_complete() {
    let archive: &[u8] = include_bytes!(concat!(
        std::env!("CARGO_MANIFEST_DIR"),
        "/../weaver-server-core/tests/fixtures/rar5/rar5_store.rar"
    ));
    let member = include_bytes!(concat!(
        std::env!("CARGO_MANIFEST_DIR"),
        "/../weaver-server-core/tests/fixtures/originals/small.txt"
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
    assert!(plan.volume(notes_at).is_some());
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
    // The graph's copies of the torrent's files went with its folder.
    assert!(!rig.service.unpacking_dir(job_id).exists());
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
        "/../weaver-server-core/tests/fixtures/rar5/rar5_store.rar"
    ));
    let member = include_bytes!(concat!(
        std::env!("CARGO_MANIFEST_DIR"),
        "/../weaver-server-core/tests/fixtures/originals/small.txt"
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
        "weaver_server_torrents::torrents",
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
        "/../weaver-server-core/tests/fixtures/rar5/rar5_store.rar"
    ));
    let member = include_bytes!(concat!(
        std::env!("CARGO_MANIFEST_DIR"),
        "/../weaver-server-core/tests/fixtures/originals/small.txt"
    ));
    let rig = rig(set_clock());
    on_loopback(&rig.routes);
    rig.service.open().await.unwrap();
    let save = rig.dir.path().join("seeded");
    std::fs::create_dir_all(save.join("placed")).unwrap();
    std::fs::write(save.join("placed/set.rar"), archive).unwrap();
    rig.service.publish_to(queue());
    let mut changes = completions(&rig.service);
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

// What a wait for a torrent's completion wakes on: the service's rows. A
// seed-mode torrent's queue row reads the same before and after it
// completes (seeding, every byte in), so the queue says nothing of the
// completion; its row here carries it, and moves once it is written.
fn completions(service: &TorrentService) -> tokio::sync::watch::Receiver<u64> {
    service.snapshot().subscribe()
}

// A seed-mode add whose archive file is damaged gives the graph none of
// it: the piece's seed-mode check fails, seed mode ends, every piece is
// checked again, and the piece fetched from a peer is the one the graph
// takes. The archive unpacks whole and the torrent completes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_seed_mode_archive_with_a_bad_file_is_fetched_again_and_unpacks_whole() {
    let archive: &[u8] = include_bytes!(concat!(
        std::env!("CARGO_MANIFEST_DIR"),
        "/../weaver-server-core/tests/fixtures/rar5/rar5_store.rar"
    ));
    let member = include_bytes!(concat!(
        std::env!("CARGO_MANIFEST_DIR"),
        "/../weaver-server-core/tests/fixtures/originals/small.txt"
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
    rig.service.publish_to(queue());
    let mut changes = completions(&rig.service);
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

// The pieces of the torrent that hold a byte of a file `of` picks.
async fn pieces_of(
    service: &TorrentService,
    job_id: JobId,
    of: impl Fn(usize) -> bool,
) -> Vec<u32> {
    let layout = service.inner.live.lock().await[&job_id]
        .layout
        .clone()
        .unwrap();
    (0..layout.piece_count())
        .filter(|&piece| layout.spans(piece).iter().any(|span| of(span.file)))
        .collect()
}

// Waits for the torrent's completion as stored: every settle publishes what
// it did, completion among it.
async fn stored_complete(service: &TorrentService, db: &Database, job_id: JobId) {
    let mut settles = service.inner.settled.subscribe();
    service.inner.signal.look(job_id);
    settles
        .wait_for(|_| {
            db.get_torrent(job_id)
                .unwrap()
                .unwrap()
                .completed_at_ms
                .is_some()
        })
        .await
        .unwrap();
}

// The names under `dir`, sorted, with what each holds.
fn contents(dir: &Path) -> Vec<(String, Vec<u8>)> {
    let mut found: Vec<_> = std::fs::read_dir(dir)
        .unwrap()
        .map(|entry| {
            let entry = entry.unwrap();
            let bytes = std::fs::read(entry.path()).unwrap_or_default();
            (entry.file_name().to_string_lossy().into_owned(), bytes)
        })
        .collect();
    found.sort();
    found
}

// An archive and a plain file, the plain file ignored: the archive unpacks
// whole, and the unpacking ends in part for the plain file the graph never
// had, as a post's does, with no history row. The plain file wanted again
// reopens the torrent, which downloads it and completes again with no tee:
// its unpacking does not run again, and its outputs, the names its row keeps
// and its unpacking line stay as they were.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_unpacked_torrent_reopened_for_a_plain_file_completes_without_unpacking_again() {
    reopened_for_a_plain_file(false).await;
}

// The same with a restart while it is reopened: the store made after it
// has no tee.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_unpacked_torrent_reopened_across_a_restart_completes_without_unpacking_again() {
    reopened_for_a_plain_file(true).await;
}

async fn reopened_for_a_plain_file(restart: bool) {
    use FilePriority::*;
    let archive: &[u8] = include_bytes!(concat!(
        std::env!("CARGO_MANIFEST_DIR"),
        "/../weaver-server-core/tests/fixtures/rar5/rar5_store.rar"
    ));
    let plain: Vec<u8> = (0..1024u32).map(|at| (at * 7 % 251) as u8).collect();
    let files: [(&str, &[u8]); 2] = [("set.rar", archive), ("extra.bin", &plain)];
    let whole: Vec<u8> = files.iter().flat_map(|(_, bytes)| bytes.to_vec()).collect();
    let rig = rig(set_clock());
    rig.service.open().await.unwrap();
    let job_id = rig
        .service
        .add_opened(request(AddSource::Metainfo(metainfo(
            "set", &files, 128, false,
        ))))
        .await
        .unwrap();
    rig.service
        .set_file_priorities(job_id, &[(1, Ignored)])
        .await
        .unwrap();
    let mut retired = rig.service.retired();
    let first = pieces_of(&rig.service, job_id, |file| file == 0).await;
    fetch_pieces(&rig.service, job_id, &whole, Some(&first), |_| {}).await;
    stored_complete(&rig.service, &rig.db, job_id).await;
    let view = rig.service.torrent(job_id).await.unwrap();
    let dir = PathBuf::from(view.storage_dir.unwrap());
    let outputs = rig.db.torrent_outputs(job_id).unwrap();
    assert_eq!(outputs, ["small.txt"]);
    let placed = contents(&dir);
    assert!(placed.contains(&("small.txt".to_owned(), SMALL_TXT.to_vec())));
    // The plain file left out is lost to the graph, as a posted file no
    // article of which came: the unpacking ends in part, the torrent with
    // no row.
    assert!(rig.db.get_job_history(job_id.0).unwrap().is_none());
    let ended = rig.db.get_torrent(job_id).unwrap().unwrap().unpack_end;
    assert_eq!(
        ended,
        Some(UnpackEnd::short(
            true,
            UnpackCause::DeliveredInPart,
            "delivered in part; damaged: extra.bin".into()
        ))
    );
    assert!(rig.db.get_torrent(job_id).unwrap().unwrap().unpack_over);
    let retirements = *retired.borrow_and_update();

    rig.service
        .set_file_priorities(job_id, &[(1, Normal)])
        .await
        .unwrap();
    assert!(
        rig.db
            .get_torrent(job_id)
            .unwrap()
            .unwrap()
            .completed_at_ms
            .is_none()
    );
    let again;
    let service = if restart {
        rig.service.stop().await;
        rig.db.prune_orphan_active_state(JobId(u64::MAX)).unwrap();
        again = service_on(&rig.db, rig.dir.path(), &rig.routes, job_env(), set_clock());
        again.open().await.unwrap();
        again.tick().await;
        again.restored().await;
        let live = again.inner.live.lock().await;
        assert!(live[&job_id].store.as_ref().unwrap().tee().is_none());
        assert!(!live[&job_id].complete());
        drop(live);
        &again
    } else {
        &rig.service
    };
    let rest: Vec<u32> = pieces_of(service, job_id, |file| file == 1)
        .await
        .into_iter()
        .filter(|piece| !first.contains(piece))
        .collect();
    // A placing run again would panic, and end the torrent short.
    PLACE_PANICS.store(true, std::sync::atomic::Ordering::Release);
    fetch_pieces(service, job_id, &whole, Some(&rest), |_| {}).await;
    stored_complete(service, &rig.db, job_id).await;
    assert!(
        PLACE_PANICS.swap(false, std::sync::atomic::Ordering::AcqRel),
        "no placing runs again"
    );

    assert_eq!(rig.db.torrent_outputs(job_id).unwrap(), outputs);
    let mut expected = placed.clone();
    if !expected.iter().any(|(name, _)| name == "extra.bin") {
        expected.push(("extra.bin".to_owned(), plain.clone()));
        expected.sort();
    }
    assert_eq!(contents(&dir), expected);
    assert!(
        expected.contains(&("extra.bin".to_owned(), plain.clone())),
        "{expected:?}"
    );
    assert!(rig.db.get_job_history(job_id.0).unwrap().is_none());
    assert_eq!(
        rig.db.get_torrent(job_id).unwrap().unwrap().unpack_end,
        ended
    );
    assert_eq!(service.torrent(job_id).await.unwrap().unpack_end, ended);
    assert_eq!(*retired.borrow_and_update(), retirements);
    service.stop().await;
}

// Unpacking that ends short with no placing written, its archive refused,
// beside a plain file ignored: the torrent completes with no history row,
// its unpacking over. The plain file wanted again reopens it, and it
// downloads the file and completes again with no tee: no second unpacking,
// and its outputs and unpacking line stay as they were, in the same run and
// across a restart.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_short_unpacked_torrent_reopened_for_a_plain_file_keeps_its_unpacking_line() {
    reopened_after_ending_short(false, false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_short_unpacked_torrent_reopened_across_a_restart_keeps_its_unpacking_line() {
    reopened_after_ending_short(true, false).await;
}

// The same across a restart when the mark's own write fails: it stays owed,
// and the checkpoint its failure made due writes it, so the store made
// after the restart has no tee and nothing unpacks again.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_short_unpacking_mark_whose_write_failed_is_kept_by_its_checkpoint() {
    reopened_after_ending_short(true, true).await;
}

async fn reopened_after_ending_short(restart: bool, mark_fails: bool) {
    use FilePriority::*;
    let archive: &[u8] = include_bytes!(concat!(
        std::env!("CARGO_MANIFEST_DIR"),
        "/../weaver-server-core/tests/fixtures/rar5/rar5_store.rar"
    ));
    let plain: Vec<u8> = (0..1024u32).map(|at| (at * 7 % 251) as u8).collect();
    let files: [(&str, &[u8]); 2] = [("set.rar", archive), ("extra.bin", &plain)];
    let whole: Vec<u8> = files.iter().flat_map(|(_, bytes)| bytes.to_vec()).collect();
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
    let job_id = rig
        .service
        .add_opened(request(AddSource::Metainfo(metainfo(
            "set", &files, 128, false,
        ))))
        .await
        .unwrap();
    rig.service
        .set_file_priorities(job_id, &[(1, Ignored)])
        .await
        .unwrap();
    let first = pieces_of(&rig.service, job_id, |file| file == 0).await;
    if mark_fails {
        UNPACK_OVER_FAILS.store(true, std::sync::atomic::Ordering::Release);
    }
    fetch_pieces(&rig.service, job_id, &whole, Some(&first), |_| {}).await;
    stored_complete(&rig.service, &rig.db, job_id).await;
    if mark_fails {
        assert!(
            !UNPACK_OVER_FAILS.load(std::sync::atomic::Ordering::Acquire),
            "the mark's own write was refused"
        );
        let mut settles = rig.service.inner.settled.subscribe();
        settles
            .wait_for(|_| rig.db.get_torrent(job_id).unwrap().unwrap().unpack_over)
            .await
            .unwrap();
    }
    assert!(rig.db.get_job_history(job_id.0).unwrap().is_none());
    let view = rig.service.torrent(job_id).await.unwrap();
    assert_eq!(view.state, TorrentState::Seeding);
    let ended = view.unpack_end.clone().unwrap();
    assert_eq!(ended.result, UnpackResult::Refused);
    assert_eq!(ended.cause, Some(UnpackCause::Refused));
    assert!(ended.why.as_deref().unwrap().contains("txt"), "{ended:?}");
    let dir = PathBuf::from(view.storage_dir.unwrap());
    let outputs = rig.db.torrent_outputs(job_id).unwrap();
    let placed = contents(&dir);
    // Kept by a write of its own, or its checkpoint when that failed: no
    // placing was written for it.
    let stored = rig.db.get_torrent(job_id).unwrap().unwrap();
    assert!(stored.unpack_over);
    assert_eq!(stored.unpack_end.as_ref(), Some(&ended));

    rig.service
        .set_file_priorities(job_id, &[(1, Normal)])
        .await
        .unwrap();
    assert!(
        rig.db
            .get_torrent(job_id)
            .unwrap()
            .unwrap()
            .completed_at_ms
            .is_none()
    );
    let again;
    let service = if restart {
        rig.service.stop().await;
        rig.db.prune_orphan_active_state(JobId(u64::MAX)).unwrap();
        again = service_on(&rig.db, rig.dir.path(), &rig.routes, job_env(), set_clock());
        again.open().await.unwrap();
        again.tick().await;
        again.restored().await;
        let live = again.inner.live.lock().await;
        assert!(live[&job_id].store.as_ref().unwrap().tee().is_none());
        drop(live);
        &again
    } else {
        &rig.service
    };
    let mut retired = service.retired();
    let before = *retired.borrow_and_update();
    let rest: Vec<u32> = pieces_of(service, job_id, |file| file == 1)
        .await
        .into_iter()
        .filter(|piece| !first.contains(piece))
        .collect();
    // A placing run again would panic, and end the torrent short.
    PLACE_PANICS.store(true, std::sync::atomic::Ordering::Release);
    fetch_pieces(service, job_id, &whole, Some(&rest), |_| {}).await;
    stored_complete(service, &rig.db, job_id).await;
    assert!(
        PLACE_PANICS.swap(false, std::sync::atomic::Ordering::AcqRel),
        "no placing runs again"
    );

    assert!(rig.db.get_job_history(job_id.0).unwrap().is_none());
    assert_eq!(
        rig.db.get_torrent(job_id).unwrap().unwrap().unpack_end,
        Some(ended.clone())
    );
    assert_eq!(
        service.torrent(job_id).await.unwrap().unpack_end,
        Some(ended)
    );
    assert_eq!(*retired.borrow_and_update(), before);
    assert_eq!(rig.db.torrent_outputs(job_id).unwrap(), outputs);
    let mut expected = placed.clone();
    expected.retain(|(name, _)| name != "extra.bin");
    expected.push(("extra.bin".to_owned(), plain.clone()));
    expected.sort();
    assert_eq!(contents(&dir), expected);
    service.stop().await;
}

// A torrent whose store was made complete with its unpacking on and never
// run, as a row kept complete without the mark leaves it, has no tee.
// Reopened for a plain file, it completes with no first unpacking, in the
// run it was reopened in and across a restart before it completes: the
// reopen stores its unpacking over, so a store made after it has no tee
// either. A restart does not change the outcome.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_torrent_made_complete_and_reopened_completes_without_a_first_unpack() {
    made_complete_and_reopened(false).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_torrent_made_complete_reopened_and_restarted_completes_without_a_first_unpack() {
    made_complete_and_reopened(true).await;
}

async fn made_complete_and_reopened(restart: bool) {
    use crate::persistence::sql_runtime::SqlRuntime;
    use FilePriority::*;
    let archive: &[u8] = include_bytes!(concat!(
        std::env!("CARGO_MANIFEST_DIR"),
        "/../weaver-server-core/tests/fixtures/rar5/rar5_store.rar"
    ));
    let plain: Vec<u8> = (0..1024u32).map(|at| (at * 7 % 251) as u8).collect();
    let files: [(&str, &[u8]); 2] = [("set.rar", archive), ("extra.bin", &plain)];
    let whole: Vec<u8> = files.iter().flat_map(|(_, bytes)| bytes.to_vec()).collect();
    let rig = rig(set_clock());
    rig.service.open().await.unwrap();
    let job_id = rig
        .service
        .add_opened(request(AddSource::Metainfo(metainfo(
            "set", &files, 128, false,
        ))))
        .await
        .unwrap();
    rig.service
        .set_file_priorities(job_id, &[(1, Ignored)])
        .await
        .unwrap();
    let first = pieces_of(&rig.service, job_id, |file| file == 0).await;
    fetch_pieces(&rig.service, job_id, &whole, Some(&first), |_| {}).await;
    stored_complete(&rig.service, &rig.db, job_id).await;
    let dir = PathBuf::from(
        rig.service
            .torrent(job_id)
            .await
            .unwrap()
            .storage_dir
            .unwrap(),
    );
    rig.service.stop().await;
    // Its row complete, its unpacking never marked over.
    let datastore = rig.db.datastore();
    let id = job_id.0;
    rig.db
        .run_sql_blocking(async move {
            SqlRuntime::execute(
                datastore.write_exec(),
                &format!("UPDATE torrents SET unpack_over = FALSE WHERE job_id = {id}"),
                &[],
            )
            .await?;
            Ok(())
        })
        .unwrap();
    rig.db.prune_orphan_active_state(JobId(u64::MAX)).unwrap();
    let restarted =
        |rig: &Rig| service_on(&rig.db, rig.dir.path(), &rig.routes, job_env(), set_clock());
    let made = restarted(&rig);
    made.open().await.unwrap();
    made.tick().await;
    made.restored().await;
    {
        let live = made.inner.live.lock().await;
        assert!(live[&job_id].complete());
        assert!(live[&job_id].unpacking());
        assert!(live[&job_id].store.as_ref().unwrap().tee().is_none());
    }
    made.set_file_priorities(job_id, &[(1, Normal)])
        .await
        .unwrap();
    let stored = rig.db.get_torrent(job_id).unwrap().unwrap();
    assert!(stored.completed_at_ms.is_none() && stored.unpack_over);
    let again;
    let service = if restart {
        made.stop().await;
        rig.db.prune_orphan_active_state(JobId(u64::MAX)).unwrap();
        again = restarted(&rig);
        again.open().await.unwrap();
        again.tick().await;
        again.restored().await;
        let live = again.inner.live.lock().await;
        assert!(!live[&job_id].complete());
        assert!(live[&job_id].store.as_ref().unwrap().tee().is_none());
        drop(live);
        &again
    } else {
        &made
    };
    let placed = contents(&dir);
    let unpacking = service.unpacking_dir(job_id);
    let held = unpacking.exists().then(|| contents(&unpacking));
    let rest: Vec<u32> = pieces_of(service, job_id, |file| file == 1)
        .await
        .into_iter()
        .filter(|piece| !first.contains(piece))
        .collect();
    // A placing run would panic, and end the torrent short.
    PLACE_PANICS.store(true, std::sync::atomic::Ordering::Release);
    fetch_pieces(service, job_id, &whole, Some(&rest), |_| {}).await;
    stored_complete(service, &rig.db, job_id).await;
    assert!(
        PLACE_PANICS.swap(false, std::sync::atomic::Ordering::AcqRel),
        "no placing runs"
    );
    // No unpacking ran: its folder is as the first end left it.
    assert_eq!(unpacking.exists().then(|| contents(&unpacking)), held);
    let mut expected = placed;
    expected.retain(|(name, _)| name != "extra.bin");
    expected.push(("extra.bin".to_owned(), plain.clone()));
    expected.sort();
    assert_eq!(contents(&dir), expected);
    service.stop().await;
}

// The five volumes of the encrypted RAR set, by name, with their bytes.
fn rar_set() -> Vec<(String, Vec<u8>)> {
    fixtures(
        "rar5",
        (1..=5).map(|n| format!("rar5_enc_mv_video.part{n}.rar")),
    )
}

// The named fixtures of `dir`, with their bytes.
fn fixtures(dir: &str, names: impl Iterator<Item = String>) -> Vec<(String, Vec<u8>)> {
    names
        .map(|name| {
            let path = format!(
                "{}/../weaver-server-core/tests/fixtures/{dir}/{name}",
                std::env!("CARGO_MANIFEST_DIR")
            );
            let bytes = std::fs::read(path).unwrap();
            (name, bytes)
        })
        .collect()
}

// A torrent unpacking the five volumes of a RAR set, `ignored` among them
// left out, done: it completes, the set's give-up run to its end, a volume
// a turn. Returns the rig, the job, every volume's bytes end to end, and the
// tee its store held, which it holds no more: its unpacking is over.
async fn ignoring_a_volume(
    ignored: usize,
) -> (Rig, JobId, Vec<u8>, Arc<crate::torrents::tee::Tee>) {
    ignoring(rar_set(), &[ignored], 16384).await
}

// A torrent of `parts` unpacking, the files at `ignored` left out, done: it
// completes, any give-up run to its end. Returns the rig, the job, every
// file's bytes end to end, and the tee its store held, which it holds no
// more: its unpacking is over.
async fn ignoring(
    parts: Vec<(String, Vec<u8>)>,
    ignored: &[usize],
    piece: usize,
) -> (Rig, JobId, Vec<u8>, Arc<crate::torrents::tee::Tee>) {
    use FilePriority::*;
    let files: Vec<(&str, &[u8])> = parts
        .iter()
        .map(|(name, bytes)| (name.as_str(), &bytes[..]))
        .collect();
    let whole: Vec<u8> = parts.iter().flat_map(|(_, bytes)| bytes.clone()).collect();
    let rig = rig(set_clock());
    rig.service.open().await.unwrap();
    rig.db
        .save_archive_password_settings(Some(vec!["testpass123".into()]), None)
        .unwrap();
    let job_id = rig
        .service
        .add_opened(request(AddSource::Metainfo(metainfo(
            "set", &files, piece, false,
        ))))
        .await
        .unwrap();
    let left_out: Vec<(u32, FilePriority)> =
        ignored.iter().map(|&file| (file as u32, Ignored)).collect();
    rig.service
        .set_file_priorities(job_id, &left_out)
        .await
        .unwrap();
    let tee = rig.service.inner.live.lock().await[&job_id]
        .store
        .as_ref()
        .unwrap()
        .tee()
        .unwrap();
    let wanted = pieces_of(&rig.service, job_id, |file| !ignored.contains(&file)).await;
    fetch_pieces(&rig.service, job_id, &whole, Some(&wanted), |_| {}).await;
    stored_complete(&rig.service, &rig.db, job_id).await;
    assert!(!tee.with(|job| job.giving_up()));
    assert!(rig.db.get_torrent(job_id).unwrap().unwrap().unpack_over);
    assert!(
        rig.service.inner.live.lock().await[&job_id]
            .store
            .as_ref()
            .unwrap()
            .tee()
            .is_none()
    );
    (rig, job_id, whole, tee)
}

// How a torrent's unpacking ended, as its unpacking line says it, and what
// it placed beside its files: its row's outputs and the names in its
// directory that are none of its own files nor the directory's hidden
// marker. However it ended, the torrent is one with nothing to unpack: it
// seeds, saying nothing of it, with no history row, nothing retired and no
// failure counted.
async fn ended(
    rig: &Rig,
    job_id: JobId,
    own: &[(String, Vec<u8>)],
) -> (String, Option<String>, Vec<String>, Vec<String>) {
    let view = rig.service.torrent(job_id).await.unwrap();
    assert_eq!(view.state, TorrentState::Seeding);
    assert_eq!(view.detail, None);
    assert_eq!(job_status(&view), JobStatus::Seeding);
    assert!(rig.db.get_job_history(job_id.0).unwrap().is_none());
    assert_eq!(*rig.service.retired().borrow(), 0);
    assert_eq!(rig.service.inner.counters.count(TorrentEvent::Failed), 0);
    let end = view.unpack_end.clone().unwrap();
    assert_eq!(
        rig.db.get_torrent(job_id).unwrap().unwrap().unpack_end,
        Some(end.clone())
    );
    let dir = PathBuf::from(view.storage_dir.unwrap());
    let beside = contents(&dir)
        .into_iter()
        .map(|(name, _)| name)
        .filter(|name| !name.starts_with('.') && !own.iter().any(|(own, _)| own == name))
        .collect();
    (
        end.result.as_str().to_owned(),
        end.why,
        rig.db.torrent_outputs(job_id).unwrap(),
        beside,
    )
}

// A torrent of the five-volume RAR set that leaves out its first, a middle
// or its last volume delivers nothing whole: its unpacking fails, as a post
// missing that volume does, and it places nothing. The torrent seeds on as
// one with nothing to unpack, stopped reads as stopped and nothing else,
// and its unpacking line, failed with why, is the same after a restart.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_unpacking_torrent_without_its_first_volume_places_nothing_and_fails() {
    ignored_volume_fails(0).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_unpacking_torrent_without_a_middle_volume_places_nothing_and_fails() {
    ignored_volume_fails(2).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_unpacking_torrent_without_its_last_volume_places_nothing_and_fails() {
    ignored_volume_fails(4).await;
}

async fn ignored_volume_fails(ignored: usize) {
    let (rig, job_id, _, _) = ignoring_a_volume(ignored).await;
    let (status, error, outputs, beside) = ended(&rig, job_id, &rar_set()).await;
    eprintln!("ignored {ignored}: {status} {error:?}");
    assert_eq!(status, "failed");
    assert!(error.is_some());
    assert!(outputs.is_empty(), "{outputs:?}");
    assert!(beside.is_empty(), "{beside:?}");
    assert!(rig.service.unpacked_files(job_id).await.unwrap().is_empty());
    let end = rig.service.torrent(job_id).await.unwrap().unpack_end;
    assert_eq!(
        end.as_ref().and_then(|end| end.cause),
        Some(UnpackCause::NothingDelivered)
    );

    rig.service.pause(job_id).await.unwrap();
    let view = rig.service.torrent(job_id).await.unwrap();
    assert_eq!(
        (view.state, view.detail.clone()),
        (TorrentState::Paused, None)
    );
    assert_eq!(job_status(&view), JobStatus::Paused);
    rig.service.resume(job_id).await.unwrap();
    rig.service.stop().await;
    rig.db.prune_orphan_active_state(JobId(u64::MAX)).unwrap();

    let again = service_on(&rig.db, rig.dir.path(), &rig.routes, job_env(), set_clock());
    again.open().await.unwrap();
    let view = again.torrent(job_id).await.unwrap();
    assert_eq!(view.unpack_end, end);
    assert_eq!(view.detail, None);
    assert!(rig.db.get_job_history(job_id.0).unwrap().is_none());
    again.stop().await;
}

// The set, nothing left out, unpacks whole and places what it holds.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_unpacking_torrent_that_leaves_nothing_out_completes() {
    let (rig, job_id, _, _) = ignoring(rar_set(), &[], 16384).await;
    let (status, error, outputs, beside) = ended(&rig, job_id, &rar_set()).await;
    assert_eq!((status.as_str(), error), ("whole", None));
    assert_eq!(outputs, vec!["test_clip.mkv".to_owned()]);
    assert_eq!(beside, outputs);
    // What Scryer and Arr tools read it downloaded lists it, with its
    // length on disk.
    let unpacked = rig.service.unpacked_files(job_id).await.unwrap();
    assert_eq!(
        unpacked
            .iter()
            .map(|file| file.name.as_str())
            .collect::<Vec<_>>(),
        ["test_clip.mkv"]
    );
    assert!(unpacked[0].length > 0);
    rig.service.stop().await;
}

// Two sets, one whole and the other short of a middle volume: the whole
// set's file is delivered and placed, so the unpacking ends in part, as a
// post with the same two sets does, saying what it lacks.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_torrent_of_two_sets_one_short_of_a_volume_ends_in_part() {
    let mut parts = fixtures("rar5", std::iter::once("rar5_store.rar".to_owned()));
    parts.extend(rar_set());
    let (rig, job_id, _, _) = ignoring(parts.clone(), &[3], 16384).await;
    let (status, error, outputs, beside) = ended(&rig, job_id, &parts).await;
    eprintln!("two sets: {status} {error:?} {outputs:?}");
    assert_eq!(status, "part");
    assert!(error.is_some());
    assert!(!outputs.is_empty());
    assert!(!outputs.iter().any(|name| name == "test_clip.mkv"));
    assert_eq!(beside, outputs);
    rig.service.stop().await;
}

// A set whose data comes whole and whose recovery files the torrent left
// out unpacks whole: nothing needed the recovery.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_torrent_that_leaves_out_its_recovery_with_its_data_whole_completes() {
    let mut parts = fixtures("rar5", std::iter::once("rar5_store.rar".to_owned()));
    let parity = torrent_parity(&[("rar5_store.rar", &parts[0].1)], 2);
    let ignored: Vec<usize> = (1..=parity.len()).collect();
    parts.extend(parity);
    let (rig, job_id, _, _) = ignoring(parts.clone(), &ignored, 256).await;
    let (status, error, outputs, _) = ended(&rig, job_id, &parts).await;
    assert_eq!((status.as_str(), error), ("whole", None));
    assert!(!outputs.is_empty());
    rig.service.stop().await;
}

// A single-volume archive and a plain file, with a recovery set of two
// blocks over both, by name.
fn archive_notes_and_parity() -> Vec<(String, Vec<u8>)> {
    let archive = fixtures("rar5", std::iter::once("rar5_store.rar".to_owned()))
        .remove(0)
        .1;
    let notes: Vec<u8> = (0..1500u32).map(|at| (at * 7 % 251) as u8).collect();
    let parity = torrent_parity(&[("set.rar", &archive), ("notes.txt", &notes)], 2);
    let mut parts = vec![
        ("set.rar".to_owned(), archive),
        ("notes.txt".to_owned(), notes),
    ];
    parts.extend(parity);
    parts
}

// The plain file left out: no share of it comes, as no article of a posted
// file the feed gave up comes, and the shared ruling over the set stands.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_archive_beside_an_ignored_plain_file_under_one_recovery_set_ends_as_the_graph_rules() {
    let parts = archive_notes_and_parity();
    let (rig, job_id, _, _) = ignoring(parts.clone(), &[1], 256).await;
    let (status, error, outputs, beside) = ended(&rig, job_id, &parts).await;
    assert_eq!(status, "part");
    assert_eq!(
        error.as_deref(),
        Some("delivered in part; unrepaired: notes.txt")
    );
    assert_eq!(outputs, vec!["small.txt".to_owned()]);
    assert_eq!(beside, outputs);
    rig.service.stop().await;
}

// The archive left out: the set rebuilds it from the plain file's bytes and
// its recovery, the archive unpacks, and the torrent completes.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_ignored_volume_the_set_rebuilds_from_a_plain_files_bytes_completes() {
    let parts = archive_notes_and_parity();
    let (rig, job_id, _, _) = ignoring(parts.clone(), &[0], 256).await;
    let (status, error, outputs, beside) = ended(&rig, job_id, &parts).await;
    assert_eq!((status.as_str(), error), ("whole", None));
    assert_eq!(outputs, vec!["small.txt".to_owned()]);
    assert_eq!(beside, outputs);
    rig.service.stop().await;
}

// A split 7z set short of a middle volume, and a file split in three short
// of its middle part: each give-up runs to its end, and the unpacking fails
// with nothing placed.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_split_7z_set_short_of_a_volume_ends_and_fails() {
    let parts = fixtures(
        "sevenz",
        (1..=7).map(|n| format!("generated_split_store_plain.7z.{n:03}")),
    );
    let (rig, job_id, _, _) = ignoring(parts.clone(), &[3], 16384).await;
    let (status, error, outputs, beside) = ended(&rig, job_id, &parts).await;
    eprintln!("7z: {status} {error:?}");
    assert_eq!(status, "failed");
    assert!(
        outputs.is_empty() && beside.is_empty(),
        "{outputs:?} {beside:?}"
    );
    rig.service.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_split_file_short_of_a_part_ends_and_fails() {
    let joined: Vec<u8> = (0..3 * 40_000u32).map(|at| (at * 13 % 251) as u8).collect();
    let parts: Vec<(String, Vec<u8>)> = joined
        .chunks(40_000)
        .zip(1..)
        .map(|(chunk, n)| (format!("clip.mkv.{n:03}"), chunk.to_vec()))
        .collect();
    let (rig, job_id, _, _) = ignoring(parts.clone(), &[1], 16384).await;
    let (status, error, outputs, beside) = ended(&rig, job_id, &parts).await;
    eprintln!("split: {status} {error:?}");
    assert_eq!(status, "failed");
    assert!(
        outputs.is_empty() && beside.is_empty(),
        "{outputs:?} {beside:?}"
    );
    rig.service.stop().await;
}

// A torrent unpacking a five-volume RAR set whose third volume it does not
// want completes: the set's give-up runs to its end, a volume a turn, and
// the torrent seeds on as one unpacking nothing would.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_unpacking_torrent_that_ignores_a_volume_of_its_set_completes() {
    let (rig, job_id, _, _) = ignoring_a_volume(2).await;
    assert_eq!(
        rig.service.torrent(job_id).await.unwrap().state,
        TorrentState::Seeding
    );
    rig.service.stop().await;
}

// The volume it ignored, wanted again once its unpacking is over, is
// fetched and the torrent completes again with no tee: the store let go
// of the one it held, so nothing it fetches goes to the job, nothing is
// written to its unpacking directory, which its failed unpacking kept as a
// failed post's job keeps its own, and nothing is placed.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_volume_wanted_again_after_unpacking_is_over_goes_to_no_tee() {
    use FilePriority::*;
    let (rig, job_id, whole, tee) = ignoring_a_volume(2).await;
    let dir = PathBuf::from(
        rig.service
            .torrent(job_id)
            .await
            .unwrap()
            .storage_dir
            .unwrap(),
    );
    let outputs = rig.db.torrent_outputs(job_id).unwrap();
    let placed = contents(&dir);
    let unpacking = rig.service.unpacking_dir(job_id);
    let kept = contents(&unpacking);
    rig.service
        .set_file_priorities(job_id, &[(2, Normal)])
        .await
        .unwrap();
    let rest = pieces_of(&rig.service, job_id, |file| file == 2).await;
    // A placing run again would panic, and end the torrent short.
    PLACE_PANICS.store(true, std::sync::atomic::Ordering::Release);
    fetch_pieces(&rig.service, job_id, &whole, Some(&rest), |_| {}).await;
    stored_complete(&rig.service, &rig.db, job_id).await;
    assert!(
        PLACE_PANICS.swap(false, std::sync::atomic::Ordering::AcqRel),
        "no placing runs again"
    );
    // Nothing but this test holds the tee the store let go of.
    assert_eq!(Arc::strong_count(&tee), 1);
    assert_eq!(contents(&unpacking), kept);
    assert_eq!(rig.db.torrent_outputs(job_id).unwrap(), outputs);
    let names = |found: Vec<(String, Vec<u8>)>| -> Vec<String> {
        found.into_iter().map(|(name, _)| name).collect()
    };
    let mut expected = names(placed);
    if !expected
        .iter()
        .any(|name| name == "rar5_enc_mv_video.part3.rar")
    {
        expected.push("rar5_enc_mv_video.part3.rar".to_owned());
        expected.sort();
    }
    assert_eq!(names(contents(&dir)), expected);
    rig.service.stop().await;
}
