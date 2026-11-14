// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// A finished torrent goes through the owners a finished NZB job goes
// through, and each gives the two kinds the same answer on the same input.

use weaver_graph::{MemberId, NodeId, Posted, Provenance, Tree};

use super::*;
use crate::pipeline::graph::status::Unarrived;

const LENGTH: u64 = 64;

// The status an NZB job's graph rules for a post of `files`, each whole or
// not, failed for `why`: its root members as the walk reads them, a piece
// named as the pipeline names one (`names_a_piece`).
fn usenet_status(files: &[(&str, bool)], why: &str) -> JobStatus {
    let mut tree = Tree::new();
    let mut objects = weaver_objects::Objects::default();
    let mut effects = Vec::new();
    for (name, whole) in files {
        let member = tree.announce_root(name, Some(LENGTH), &mut effects);
        if *whole {
            let arrival = Posted {
                offset: 0,
                bytes: bytes::Bytes::from(vec![b'e'; LENGTH as usize]).into(),
                provenance: Provenance::Fresh,
            };
            tree.deliver(NodeId::ROOT, member, arrival, &mut objects, &mut effects);
            tree.close(NodeId::ROOT, member, &mut objects, &mut effects);
        }
    }
    let piece = |node: NodeId, member: MemberId| {
        tree.namespace(node)
            .and_then(|namespace| namespace.get(member))
            .is_some_and(|member| crate::pipeline::post_health::names_a_piece(&member.output_name))
    };
    let delivery = Delivery::read(
        &tree,
        |_| true,
        crate::pipeline::graph::status::root_furniture(),
        |_, _| false,
        |_, _| None,
        piece,
        |_| Unarrived::Short,
    );
    let failure = Failure {
        objects: Vec::new(),
        message: why.to_owned(),
        rejection: None,
        from_feed: false,
    };
    delivery.status(Some(&failure))
}

// The status a torrent's job ends with for the same files: the names of
// those it has whole.
fn torrent_status(files: &[(&str, bool)], why: &str) -> JobStatus {
    let whole: Vec<String> = files
        .iter()
        .filter(|(_, whole)| *whole)
        .map(|(name, _)| (*name).to_owned())
        .collect();
    short_status(&whole, &[], why)
}

const WHY: &str = "unpacking refused the archive";

// AGENTS.md "Job status: Partial": one .mkv in a five-volume RAR, one
// volume lost and nothing to rebuild it. Four volumes arrived whole, no
// usable file came out: Failed, for the torrent as for the NZB job.
#[test]
fn a_five_volume_rar_short_a_volume_fails_for_both_kinds() {
    let files = [
        ("ember.part1.rar", true),
        ("ember.part2.rar", true),
        ("ember.part3.rar", false),
        ("ember.part4.rar", true),
        ("ember.part5.rar", true),
    ];
    let failed = JobStatus::Failed {
        error: WHY.to_owned(),
    };
    assert_eq!(usenet_status(&files, WHY), failed);
    assert_eq!(torrent_status(&files, WHY), failed);
}

// Three plain files and no recovery, one of them short: the two whole ones
// are usable, so the job is Partial for both kinds.
#[test]
fn three_plain_files_one_short_are_partial_for_both_kinds() {
    let files = [
        ("ember.mkv", true),
        ("ember.srt", false),
        ("ember.nfo", true),
    ];
    assert_eq!(usenet_status(&files, WHY), JobStatus::Partial);
    assert_eq!(torrent_status(&files, WHY), JobStatus::Partial);
}

// Furniture is no usable file for either kind, by the one rule an NZB
// job's root furniture follows (`par2_damage_ignorable`, nfo, sfv, srr and
// nzb by default): a whole .nfo beside a refused archive fails both.
#[test]
fn a_whole_nfo_beside_a_refused_archive_fails_for_both_kinds() {
    let files = [
        ("ember.nfo", true),
        ("ember.srr", true),
        ("ember.part1.rar", true),
        ("ember.part2.rar", false),
    ];
    let failed = JobStatus::Failed {
        error: WHY.to_owned(),
    };
    assert_eq!(usenet_status(&files, WHY), failed);
    assert_eq!(torrent_status(&files, WHY), failed);
}

// Whole recovery, check and split files, beside an archive volume, are each
// a member that exists to make something else: none is usable, and both
// kinds fail.
#[test]
fn whole_recovery_check_and_split_files_alone_fail_for_both_kinds() {
    let files = [
        ("ember.par2", true),
        ("ember.vol00+01.par2", true),
        ("ember.rev", true),
        ("ember.sfv", true),
        ("ember.mkv.001", true),
        ("ember.rar", true),
        ("ember.mkv", false),
    ];
    let failed = JobStatus::Failed {
        error: WHY.to_owned(),
    };
    assert_eq!(usenet_status(&files, WHY), failed);
    assert_eq!(torrent_status(&files, WHY), failed);
}

// A whole file of no type people open, beside a short one, is not usable:
// both kinds fail, and the torrent's job keeps why.
#[test]
fn a_whole_file_of_no_known_type_alone_fails_for_both_kinds() {
    let files = [("ember.bin", true), ("ember.mkv", false)];
    let failed = JobStatus::Failed {
        error: WHY.to_owned(),
    };
    assert_eq!(usenet_status(&files, WHY), failed);
    assert_eq!(torrent_status(&files, WHY), failed);
}

// A torrent removed while the database's disk has no room for its history
// row is not lost: its row is kept by the owner that keeps an NZB job's
// archive, the torrent's row with it, and the job reaches history once a
// pass writes it, the torrent leaving the torrents in the same write.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_torrent_history_row_the_disk_has_no_room_for_waits_and_is_written() {
    use crate::persistence::ArchiveNews;
    let rig = rig(set_clock());
    rig.service.open().await.unwrap();
    let (news, mut heard) = tokio::sync::mpsc::unbounded_channel();
    rig.db.hear_archive_news(move |said| {
        let _ = news.send(said);
    });
    let files: [(&str, &[u8]); 1] = [("ember.mkv", &[0x5a_u8; 300])];
    let job_id = rig
        .service
        .add_opened(request(AddSource::Metainfo(metainfo(
            "ember", &files, 128, false,
        ))))
        .await
        .unwrap();
    let retired = rig.service.retired();
    crate::persistence::connection::ARCHIVE_REFUSED
        .lock()
        .unwrap()
        .push_back(crate::StateError::OutOfSpace(
            "database or disk is full".into(),
        ));
    rig.service.remove(job_id, false, false).await.unwrap();
    assert_eq!(heard.recv().await, Some(ArchiveNews::Kept(job_id)));
    assert_eq!(rig.db.kept_archive_jobs(), [job_id]);
    assert!(rig.db.get_job_history(job_id.0).unwrap().is_none());
    assert!(
        rig.db.get_torrent(job_id).unwrap().is_some(),
        "kept with it"
    );
    assert_eq!(*retired.borrow(), 0, "not yet in history");

    let mut retired = retired;
    rig.db.retry_kept_archives(0);
    assert_eq!(
        heard.recv().await,
        Some(ArchiveNews::LetGo {
            job_id,
            written: true
        })
    );
    retired.wait_for(|retired| *retired >= 1).await.unwrap();
    let history = rig.db.get_job_history(job_id.0).unwrap().unwrap();
    assert_eq!(history.status, "cancelled");
    assert!(rig.db.get_torrent(job_id).unwrap().is_none());
    assert!(rig.db.kept_archive_jobs().is_empty());
    rig.service.stop().await;
}

// A torrent removed while the database's disk has no room for its history
// row stays in the queue as an NZB job's retiring row does, with the
// disk-space wait, until the row is written; it then leaves the queue for
// the finished jobs.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_removed_torrent_whose_history_waits_shows_the_disk_space_wait() {
    use crate::persistence::ArchiveNews;
    let rig = rig(set_clock());
    let queue = queue();
    let state = queue.state().clone();
    rig.service.publish_to(queue);
    rig.service.open().await.unwrap();
    let (news, mut heard) = tokio::sync::mpsc::unbounded_channel();
    rig.db.hear_archive_news(move |said| {
        let _ = news.send(said);
    });
    let files: [(&str, &[u8]); 1] = [("ember.mkv", &[0x5a_u8; 300])];
    let job_id = rig
        .service
        .add_opened(request(AddSource::Metainfo(metainfo(
            "ember", &files, 128, false,
        ))))
        .await
        .unwrap();
    let mut retired = rig.service.retired();
    crate::persistence::connection::ARCHIVE_REFUSED
        .lock()
        .unwrap()
        .push_back(crate::StateError::OutOfSpace(
            "database or disk is full".into(),
        ));
    rig.service.remove(job_id, false, false).await.unwrap();
    assert_eq!(heard.recv().await, Some(ArchiveNews::Kept(job_id)));
    // The tick publishes the torrents as they stand, and the retiring one
    // with them.
    rig.service.tick().await;
    let listed = state.list_jobs();
    let row = listed
        .iter()
        .find(|job| job.job_id == job_id)
        .expect("the retiring torrent stays in the queue");
    assert_eq!(
        row.download_wait_reason.as_deref(),
        Some(crate::jobs::handle::DISK_SPACE_WAIT_REASON)
    );

    rig.db.retry_kept_archives(0);
    assert_eq!(
        heard.recv().await,
        Some(ArchiveNews::LetGo {
            job_id,
            written: true
        })
    );
    retired.wait_for(|retired| *retired >= 1).await.unwrap();
    // Once, among the finished jobs, with no wait.
    let listed = state.list_jobs();
    let rows: Vec<_> = listed.iter().filter(|job| job.job_id == job_id).collect();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].download_wait_reason, None);
    rig.service.stop().await;
}

// A server stopped while a removed torrent's history row still waits for
// room: the next start archives the torrent with that row, as it archives
// an NZB job left terminal in the active table, and it does not come back.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_torrent_whose_history_still_waits_at_a_stop_is_archived_by_the_next_start() {
    use crate::persistence::ArchiveNews;
    let rig = rig(set_clock());
    rig.service.open().await.unwrap();
    let (news, mut heard) = tokio::sync::mpsc::unbounded_channel();
    rig.db.hear_archive_news(move |said| {
        let _ = news.send(said);
    });
    let files: [(&str, &[u8]); 1] = [("ember.mkv", &[0x5a_u8; 300])];
    let job_id = rig
        .service
        .add_opened(request(AddSource::Metainfo(metainfo(
            "ember", &files, 128, false,
        ))))
        .await
        .unwrap();
    crate::persistence::connection::ARCHIVE_REFUSED
        .lock()
        .unwrap()
        .push_back(crate::StateError::OutOfSpace(
            "database or disk is full".into(),
        ));
    rig.service.remove(job_id, false, false).await.unwrap();
    assert_eq!(heard.recv().await, Some(ArchiveNews::Kept(job_id)));
    rig.service.stop().await;
    assert!(rig.db.get_torrent(job_id).unwrap().is_some());
    assert!(rig.db.get_job_history(job_id.0).unwrap().is_none());

    let data = rig.dir.path().join("data");
    std::fs::create_dir_all(&data).unwrap();
    let recovered =
        crate::operations::recover_server_state(&rig.db, &data, &rig.dir.path().join("work"))
            .await
            .unwrap();
    let history = rig.db.get_job_history(job_id.0).unwrap().unwrap();
    assert_eq!(history.status, "cancelled");
    assert_eq!(history.name, "ember");
    assert!(rig.db.get_torrent(job_id).unwrap().is_none());
    assert!(
        recovered
            .initial_history
            .iter()
            .any(|job| job.job_id == job_id)
    );
    let restarted = service_on(&rig.db, rig.dir.path(), &rig.routes, job_env(), set_clock());
    restarted.open().await.unwrap();
    assert!(restarted.torrent(job_id).await.is_none());
    restarted.stop().await;
}

// A torrent's unpacking folder carries its job's marker, and while the
// torrent's row stands it is live to every sweep: the start-up sweep of
// the intermediate folder, the staging sweep's recorded jobs, the failed
// sweep's page, and a history delete. None of them takes it, though a
// failed history row for the same job names it.
#[tokio::test]
async fn a_live_torrents_unpacking_folder_is_taken_by_no_sweep() {
    let rig = rig(set_clock());
    rig.service.open().await.unwrap();
    let files: [(&str, &[u8]); 1] = [("ember.mkv", &[0x5a_u8; 300])];
    let job_id = rig
        .service
        .add_opened(request(AddSource::Metainfo(metainfo(
            "ember", &files, 128, false,
        ))))
        .await
        .unwrap();
    rig.service.stop().await;
    assert!(rig.db.get_torrent(job_id).unwrap().is_some());

    let work = rig.dir.path().join("work");
    let unpacking =
        crate::jobs::working_dir::own_named_dir(&work, &format!("torrent-{}", job_id.0), job_id);
    crate::jobs::working_dir::claim_named_working_dir(&work, &unpacking, job_id).unwrap();
    std::fs::write(unpacking.join("set.part1.rar"), b"held").unwrap();
    rig.db
        .insert_job_history(&crate::JobHistoryRow {
            job_id: job_id.0,
            job_hash: None,
            name: "ember".to_owned(),
            status: "failed".to_owned(),
            error_message: Some("unpacking failed".to_owned()),
            total_bytes: 300,
            downloaded_bytes: 300,
            optional_recovery_bytes: 0,
            optional_recovery_downloaded_bytes: 0,
            failed_bytes: 0,
            health: 1000,
            category: None,
            output_dir: Some(unpacking.display().to_string()),
            nzb_path: None,
            created_at: 1,
            completed_at: 2,
            metadata: None,
            server_attribution: None,
            protocol: crate::jobs::Protocol::Torrent,
            archived_at: None,
        })
        .unwrap();

    assert!(rig.db.recorded_job_ids().unwrap().contains(&job_id.0));
    assert!(
        rig.db
            .failed_job_history_page(i64::MAX, None, 50)
            .unwrap()
            .iter()
            .all(|(_, id, _)| *id != job_id),
        "the failed sweep passes a live torrent"
    );
    let data = rig.dir.path().join("data");
    std::fs::create_dir_all(&data).unwrap();
    crate::operations::recover_server_state(&rig.db, &data, &work)
        .await
        .unwrap();
    assert!(
        unpacking.join("set.part1.rar").exists(),
        "the start keeps it"
    );

    let (deleted, outcome) = crate::history::delete_files::delete_history_rows(
        &rig.db,
        &work,
        &[],
        std::collections::BTreeSet::from([job_id]),
        true,
    )
    .await;
    outcome.unwrap();
    assert!(deleted.contains(&job_id), "the history row goes");
    assert!(
        unpacking.join("set.part1.rar").exists(),
        "a history delete keeps it"
    );
    assert!(rig.db.get_torrent(job_id).unwrap().is_some());
}

// A torrent removed without its files lists them with its history row, and
// an archive from history takes exactly those: a file the torrent did not
// write, in the same folder, stays, and so does the folder its row records.
// The list goes with the row when the row is deleted.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_archive_of_a_removed_torrent_takes_exactly_the_files_it_listed() {
    let rig = rig(set_clock());
    rig.service.open().await.unwrap();
    let (job_id, dir) = with_every_piece(&rig).await;
    let mut written = files_under(&dir);
    written.sort();
    let mine = dir.join("mine.txt");
    std::fs::write(&mine, b"not the torrent's").unwrap();
    rig.service.remove(job_id, false, true).await.unwrap();
    rig.service.stop().await;
    assert!(rig.db.get_torrent(job_id).unwrap().is_none());

    let listed = rig.db.job_history_files(&[job_id]).unwrap();
    let mut paths: Vec<PathBuf> = listed[&job_id]
        .iter()
        .inspect(|file| assert!(!file.whole, "{file:?}"))
        .map(|file| PathBuf::from(&file.path))
        .collect();
    paths.sort();
    assert_eq!(paths, written);

    let work = rig.dir.path().join("work");
    let archived = crate::history::delete_files::archive_history(
        &rig.db,
        &work,
        &[],
        std::collections::BTreeSet::from([job_id]),
        5,
        50,
    )
    .await
    .unwrap();
    assert_eq!(
        archived[&job_id].outcome,
        crate::ArchiveHistoryOutcome::Archived,
        "{:?}",
        archived[&job_id].message
    );
    assert_eq!(files_under(&dir), vec![mine.clone()]);
    assert!(dir.is_dir(), "the folder its row records stays");
    assert_eq!(
        rig.db
            .get_job_history(job_id.0)
            .unwrap()
            .unwrap()
            .archived_at,
        Some(5)
    );

    let (deleted, outcome) = crate::history::delete_files::delete_history_rows(
        &rig.db,
        &work,
        &[],
        std::collections::BTreeSet::from([job_id]),
        false,
    )
    .await;
    outcome.unwrap();
    assert!(deleted.contains(&job_id));
    assert!(rig.db.job_history_files(&[job_id]).unwrap().is_empty());
    assert!(mine.exists());
}

// A history delete with files of a removed torrent takes what it listed,
// as an archive does, and leaves what it did not write.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_history_delete_with_files_of_a_removed_torrent_takes_what_it_listed() {
    let rig = rig(set_clock());
    rig.service.open().await.unwrap();
    let (job_id, dir) = with_every_piece(&rig).await;
    let mine = dir.join("mine.txt");
    std::fs::write(&mine, b"not the torrent's").unwrap();
    rig.service.remove(job_id, false, true).await.unwrap();
    rig.service.stop().await;

    let (deleted, outcome) = crate::history::delete_files::delete_history_rows(
        &rig.db,
        &rig.dir.path().join("work"),
        &[],
        std::collections::BTreeSet::from([job_id]),
        true,
    )
    .await;
    outcome.unwrap();
    assert!(deleted.contains(&job_id));
    assert_eq!(files_under(&dir), vec![mine]);
    assert!(rig.db.job_history_files(&[job_id]).unwrap().is_empty());
}

// What a history row lists is removed only under the folder the row
// records: a path elsewhere, one that climbs out of it through `..`, a
// relative one, and the folder itself are never touched, and those that are
// there are not managed. A listed folder
// goes whole only when the list says it was made whole.
#[test]
fn a_listed_path_outside_its_folder_is_never_removed() {
    let root = tempfile::tempdir().unwrap();
    let dir = root.path().join("torrent");
    std::fs::create_dir_all(dir.join("made/inner")).unwrap();
    std::fs::create_dir_all(dir.join("own")).unwrap();
    std::fs::write(dir.join("own/a.bin"), b"a").unwrap();
    std::fs::write(dir.join("made/inner/b.bin"), b"b").unwrap();
    std::fs::create_dir_all(dir.join("folder")).unwrap();
    std::fs::write(dir.join("folder/c.bin"), b"c").unwrap();
    let outside = root.path().join("outside.bin");
    std::fs::write(&outside, b"o").unwrap();
    let listed = |path: &Path, whole: bool| HistoryFile {
        path: path.to_string_lossy().into_owned(),
        whole,
    };
    let files = [
        listed(&dir.join("own/a.bin"), false),
        listed(&dir.join("made"), true),
        listed(&dir.join("folder"), false),
        listed(&outside, false),
        listed(&dir.join("../outside.bin"), false),
        listed(Path::new("outside.bin"), false),
        listed(&dir, true),
    ];
    crate::jobs::working_dir::mark_weaver_owned_output_dir(&dir, JobId(1)).unwrap();
    let removal = remove_history_files(
        JobId(1),
        &root.path().join("work"),
        &dir,
        &files,
        &LiveHeld::default(),
    );
    assert_eq!(removal.removed, 2, "the two under it that were there");
    assert_eq!(
        removal.not_managed,
        vec![root.path().to_path_buf()],
        "what it lists outside its folder is left, and said"
    );
    assert!(!dir.join("own").exists(), "its emptied folder goes with it");
    assert!(!dir.join("made").exists());
    assert!(
        dir.join("folder/c.bin").exists(),
        "a folder listed as a file stays"
    );
    assert!(outside.exists());
    assert!(dir.is_dir());
}

// A list past one statement's binds is written whole, in chunks, with its
// row: none of it is lost and the row is there once.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_history_files_list_past_the_bind_limit_is_written_whole() {
    let rig = rig(set_clock());
    let job_id = JobId(7);
    let row = crate::JobHistoryRow {
        job_id: job_id.0,
        job_hash: None,
        name: "many".to_owned(),
        status: "complete".to_owned(),
        error_message: None,
        total_bytes: 1,
        downloaded_bytes: 1,
        optional_recovery_bytes: 0,
        optional_recovery_downloaded_bytes: 0,
        failed_bytes: 0,
        health: 1000,
        category: None,
        output_dir: Some("/data/many".to_owned()),
        nzb_path: None,
        created_at: 1,
        completed_at: 2,
        metadata: None,
        server_attribution: None,
        protocol: crate::jobs::Protocol::Torrent,
        archived_at: None,
    };
    let count = 32_766 / 3 + 5;
    let files: Vec<HistoryFile> = (0..count)
        .map(|n| HistoryFile {
            path: format!("/data/many/{n:05}.bin"),
            whole: false,
        })
        .collect();
    let (written, _) = rig
        .db
        .queue_torrent_history(job_id, row, false, files)
        .unwrap();
    written.await.unwrap();
    assert_eq!(
        rig.db.job_history_files(&[job_id]).unwrap()[&job_id].len(),
        count
    );
    assert!(rig.db.get_job_history(job_id.0).unwrap().is_some());
}

// An archive from history of a torrent's entry, by its job.
async fn archive(rig: &Rig, job_id: JobId) -> crate::ArchiveHistoryResult {
    crate::history::delete_files::archive_history(
        &rig.db,
        &rig.dir.path().join("work"),
        &[],
        std::collections::BTreeSet::from([job_id]),
        5,
        50,
    )
    .await
    .unwrap()
    .remove(&job_id)
    .unwrap()
}

// A history delete of a torrent's entry, with its files when `files`.
async fn delete_entry(rig: &Rig, job_id: JobId, files: bool) {
    let (deleted, outcome) = crate::history::delete_files::delete_history_rows(
        &rig.db,
        &rig.dir.path().join("work"),
        &[],
        std::collections::BTreeSet::from([job_id]),
        files,
    )
    .await;
    outcome.unwrap();
    assert!(deleted.contains(&job_id));
}

// A torrent removed with its files and added again lands in the folder the
// first one had, which now carries the new job's marker: an archive or a
// delete with files of the first one's entry takes none of the new one's
// files, and says the folder is not its own.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn the_entry_of_a_torrent_removed_with_its_files_takes_nothing_of_the_same_torrent_added_again()
 {
    let rig = rig(set_clock());
    rig.service.open().await.unwrap();
    let (old, dir) = with_every_piece(&rig).await;
    rig.service.remove(old, true, true).await.unwrap();
    assert!(
        rig.db.job_history_files(&[old]).unwrap().is_empty(),
        "it lists nothing its removal took"
    );
    let (new, again) = with_every_piece(&rig).await;
    assert_ne!(new, old);
    assert_eq!(again, dir);
    let files = files_under(&dir);

    let archived = archive(&rig, old).await;
    assert_eq!(
        archived.outcome,
        crate::ArchiveHistoryOutcome::NotManaged,
        "{:?}",
        archived.message
    );
    assert_eq!(files_under(&dir), files);
    delete_entry(&rig, old, true).await;
    assert_eq!(files_under(&dir), files);
    assert!(rig.service.torrent(new).await.is_some());
    rig.service.stop().await;
}

// Retention that deletes entries with their files queues the first one's
// entry for its delete with files, and that delete takes nothing of the
// same torrent added again.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn retention_with_files_of_a_removed_torrents_entry_takes_nothing_of_the_one_added_again() {
    let rig = rig(set_clock());
    rig.service.open().await.unwrap();
    let (old, dir) = with_every_piece(&rig).await;
    rig.service.remove(old, true, true).await.unwrap();
    let (_, again) = with_every_piece(&rig).await;
    assert_eq!(again, dir);
    let files = files_under(&dir);

    let with_files = Some(crate::bandwidth::PruneFiles { delete_files: true });
    rig.db
        .prune_history(with_files, with_files, with_files)
        .await
        .unwrap();
    let mut queued = Vec::new();
    for operation in rig.db.list_history_delete_operations(true).unwrap() {
        for target in rig
            .db
            .list_history_delete_operation_targets(operation.id, operation.delete_files)
            .unwrap()
        {
            queued.push((target.target_id, operation.delete_files));
        }
    }
    assert!(queued.contains(&(old.0, true)), "{queued:?}");
    for (job_id, files) in queued {
        delete_entry(&rig, JobId(job_id), files).await;
    }
    assert_eq!(files_under(&dir), files);
    rig.service.stop().await;
}

// A torrent removed keeping its files, and the same torrent added again at
// a save path over that folder: the new one takes the folder over, and the
// first one's entry takes none of the files the new one seeds.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_entry_of_a_torrent_removed_keeping_its_files_takes_nothing_of_one_added_over_them() {
    let rig = rig(set_clock());
    on_loopback(&rig.routes);
    rig.service.open().await.unwrap();
    let (old, dir) = with_every_piece(&rig).await;
    rig.service.remove(old, false, true).await.unwrap();
    let save = dir.parent().unwrap().to_path_buf();
    let (new, view) = seeding_placed(
        &rig,
        plain("set", 1),
        Placement {
            save_path: Some(save.to_string_lossy().into_owned()),
            ..Placement::default()
        },
    )
    .await;
    assert_eq!(
        view.storage_dir.as_deref(),
        Some(dir.to_string_lossy().as_ref())
    );
    let files = files_under(&dir);
    assert_ne!(files, Vec::<PathBuf>::new());

    let archived = archive(&rig, old).await;
    assert_eq!(
        archived.outcome,
        crate::ArchiveHistoryOutcome::NotManaged,
        "{:?}",
        archived.message
    );
    assert_eq!(files_under(&dir), files);
    delete_entry(&rig, old, true).await;
    assert_eq!(files_under(&dir), files);
    assert!(rig.service.torrent(new).await.is_some());
    rig.service.stop().await;
}

// A torrent removed keeping its files, and another seeding one of them in
// place: an archive of the first one's entry takes its other files and
// leaves the one the live torrent holds.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_archive_leaves_a_file_a_live_torrent_seeds_in_the_removed_ones_folder() {
    let rig = rig(set_clock());
    on_loopback(&rig.routes);
    rig.service.open().await.unwrap();
    let shared = [1_u8; 3000];
    let other = [2_u8; 3000];
    let whole: Vec<u8> = shared.iter().chain(other.iter()).copied().collect();
    let first = rig
        .service
        .add_opened(request(AddSource::Metainfo(metainfo(
            "set",
            &[("set/a.bin", &shared), ("set/b.bin", &other)],
            1000,
            false,
        ))))
        .await
        .unwrap();
    fetch_every_piece(&rig.service, first, &whole).await;
    let dir = PathBuf::from(
        rig.service
            .torrent(first)
            .await
            .unwrap()
            .storage_dir
            .unwrap(),
    );
    rig.service.remove(first, false, true).await.unwrap();
    let inner = dir.join("set");
    assert!(inner.join("a.bin").exists() && inner.join("b.bin").exists());

    let (seeder, _) = seeding_placed(
        &rig,
        metainfo("a.bin", &[("a.bin", &shared)], 1000, false),
        Placement {
            save_path: Some(inner.to_string_lossy().into_owned()),
            layout: Some(ContentLayout::Original),
            ..Placement::default()
        },
    )
    .await;

    let archived = archive(&rig, first).await;
    assert_eq!(
        archived.outcome,
        crate::ArchiveHistoryOutcome::Archived,
        "{:?}",
        archived.message
    );
    assert_eq!(std::fs::read(inner.join("a.bin")).unwrap(), shared);
    assert!(!inner.join("b.bin").exists());
    assert!(rig.service.torrent(seeder).await.is_some());
    rig.service.stop().await;
}

// An archive of the entry of a torrent that still seeds removes nothing and
// is not stamped: its files are the live torrent's.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_archive_of_a_live_torrents_entry_is_not_managed_and_not_stamped() {
    let rig = rig(set_clock());
    rig.service.open().await.unwrap();
    let (job_id, dir) = with_every_piece(&rig).await;
    rig.db
        .insert_job_history(&crate::JobHistoryRow {
            job_id: job_id.0,
            job_hash: None,
            name: "set".to_owned(),
            status: "complete".to_owned(),
            error_message: None,
            total_bytes: 3000,
            downloaded_bytes: 3000,
            optional_recovery_bytes: 0,
            optional_recovery_downloaded_bytes: 0,
            failed_bytes: 0,
            health: 1000,
            category: None,
            output_dir: Some(dir.display().to_string()),
            nzb_path: None,
            created_at: 1,
            completed_at: 2,
            metadata: None,
            server_attribution: None,
            protocol: crate::jobs::Protocol::Torrent,
            archived_at: None,
        })
        .unwrap();
    let files = files_under(&dir);

    let archived = archive(&rig, job_id).await;
    assert_eq!(archived.outcome, crate::ArchiveHistoryOutcome::NotManaged);
    assert_eq!(archived.archived_at, None);
    assert_eq!(
        rig.db
            .get_job_history(job_id.0)
            .unwrap()
            .unwrap()
            .archived_at,
        None
    );
    assert_eq!(files_under(&dir), files);
    rig.service.stop().await;
}

// A torrent laid out with no folder of its own downloads into a shared save
// path, into a folder that was there before it: an archive of its entry
// takes nothing there, says so, and the empty folder claimed for it beside
// them, which carries its marker, goes.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_archive_of_a_torrent_in_a_shared_folder_takes_only_its_claimed_folder() {
    let data = [7_u8; 3000];
    let rig = rig(set_clock());
    rig.service.open().await.unwrap();
    let save = rig.dir.path().join("flat");
    std::fs::create_dir_all(save.join("sub")).unwrap();
    let job_id = rig
        .service
        .add_opened(AddRequest {
            placement: Placement {
                save_path: Some(save.to_string_lossy().into_owned()),
                layout: Some(ContentLayout::NoSubfolder),
                ..Placement::default()
            },
            ..request(AddSource::Metainfo(metainfo(
                "placed",
                &[("sub/a.bin", &data)],
                1000,
                false,
            )))
        })
        .await
        .unwrap();
    fetch_every_piece(&rig.service, job_id, &data).await;
    let view = rig.service.torrent(job_id).await.unwrap();
    let claimed = PathBuf::from(view.storage_dir.unwrap());
    assert_ne!(claimed, save);
    assert!(claimed.is_dir());
    assert_eq!(std::fs::read(save.join("sub/a.bin")).unwrap(), data);
    rig.service.remove(job_id, false, true).await.unwrap();
    assert!(
        rig.db.job_history_files(&[job_id]).unwrap()[&job_id]
            .iter()
            .any(|file| Path::new(&file.path) == save.join("sub/a.bin")),
        "its entry lists the file it wrote"
    );

    let archived = archive(&rig, job_id).await;
    assert_eq!(
        archived.outcome,
        crate::ArchiveHistoryOutcome::NotManaged,
        "{:?}",
        archived.message
    );
    assert_eq!(std::fs::read(save.join("sub/a.bin")).unwrap(), data);
    assert!(save.join("sub").is_dir(), "a folder there before it stays");
    assert!(!claimed.exists(), "its claimed folder goes");
    rig.service.stop().await;
}

// A listed file under a link inside the torrent's folder stays, and so does
// what the link points at.
#[cfg(unix)]
#[test]
fn a_listed_file_under_a_linked_folder_stays() {
    let root = tempfile::tempdir().unwrap();
    let dir = root.path().join("torrent");
    let elsewhere = root.path().join("elsewhere");
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::create_dir_all(&elsewhere).unwrap();
    std::fs::write(elsewhere.join("a.bin"), b"a").unwrap();
    std::fs::write(dir.join("b.bin"), b"b").unwrap();
    std::os::unix::fs::symlink(&elsewhere, dir.join("linked")).unwrap();
    crate::jobs::working_dir::mark_weaver_owned_output_dir(&dir, JobId(1)).unwrap();
    let listed = |path: PathBuf| HistoryFile {
        path: path.to_string_lossy().into_owned(),
        whole: false,
    };
    let removal = remove_history_files(
        JobId(1),
        &root.path().join("work"),
        &dir,
        &[listed(dir.join("linked/a.bin")), listed(dir.join("b.bin"))],
        &LiveHeld::default(),
    );
    assert_eq!(removal.removed, 1);
    assert!(elsewhere.join("a.bin").exists());
    assert!(!dir.join("b.bin").exists());
    assert!(dir.join("linked").exists());
}

// A torrent removed keeping its files keeps its unpacking folder with its
// entry: the start-up sweep of the intermediate folder leaves it, and an
// archive of the entry takes it.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_removed_torrents_kept_unpacking_folder_outlives_the_start_up_sweep() {
    let rig = rig(set_clock());
    rig.service.open().await.unwrap();
    let files: [(&str, &[u8]); 1] = [("ember.mkv", &[0x5a_u8; 300])];
    let job_id = rig
        .service
        .add_opened(request(AddSource::Metainfo(metainfo(
            "ember", &files, 128, false,
        ))))
        .await
        .unwrap();
    let work = rig.dir.path().join("work");
    let unpacking = unpacking_dir(&work, job_id);
    crate::jobs::working_dir::claim_named_working_dir(&work, &unpacking, job_id).unwrap();
    std::fs::write(unpacking.join("set.part1.rar"), b"kept").unwrap();
    rig.service.remove(job_id, false, true).await.unwrap();
    rig.service.stop().await;
    assert!(
        rig.db.job_history_files(&[job_id]).unwrap()[&job_id]
            .iter()
            .any(|file| file.whole && Path::new(&file.path) == unpacking),
        "its entry lists it"
    );

    let data = rig.dir.path().join("data");
    std::fs::create_dir_all(&data).unwrap();
    crate::operations::recover_server_state(&rig.db, &data, &work)
        .await
        .unwrap();
    assert!(
        unpacking.join("set.part1.rar").exists(),
        "the start keeps it"
    );

    let archived = archive(&rig, job_id).await;
    assert_eq!(
        archived.outcome,
        crate::ArchiveHistoryOutcome::Archived,
        "{:?}",
        archived.message
    );
    assert!(!unpacking.exists(), "the archive takes it");
}

// A start whose archive of a retiring torrent fails leaves that one
// retiring, never live, and archives the others; the next run shows it
// waiting and writes it through the kept archives.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_retiring_torrent_the_start_could_not_archive_is_shown_and_written_later() {
    use crate::persistence::ArchiveNews;
    let rig = rig(set_clock());
    rig.service.open().await.unwrap();
    let (news, mut heard) = tokio::sync::mpsc::unbounded_channel();
    rig.db.hear_archive_news(move |said| {
        let _ = news.send(said);
    });
    let mut kept = Vec::new();
    for (name, byte) in [("ember", 0x5a_u8), ("ash", 0x3c_u8)] {
        let bytes = [byte; 300];
        let file = format!("{name}.mkv");
        let files: [(&str, &[u8]); 1] = [(&file, &bytes)];
        let job_id = rig
            .service
            .add_opened(request(AddSource::Metainfo(metainfo(
                name, &files, 128, false,
            ))))
            .await
            .unwrap();
        crate::persistence::connection::ARCHIVE_REFUSED
            .lock()
            .unwrap()
            .push_back(crate::StateError::OutOfSpace(
                "database or disk is full".into(),
            ));
        rig.service.remove(job_id, false, false).await.unwrap();
        assert_eq!(heard.recv().await, Some(ArchiveNews::Kept(job_id)));
        kept.push(job_id);
    }
    rig.service.stop().await;
    let (refused, archived) = (kept[0], kept[1]);

    crate::torrents::persistence::RETIRING_REFUSED
        .lock()
        .unwrap()
        .push(refused);
    let data = rig.dir.path().join("data");
    std::fs::create_dir_all(&data).unwrap();
    crate::operations::recover_server_state(&rig.db, &data, &rig.dir.path().join("work"))
        .await
        .unwrap();
    crate::torrents::persistence::RETIRING_REFUSED
        .lock()
        .unwrap()
        .clear();
    assert!(rig.db.get_torrent(archived).unwrap().is_none());
    assert!(rig.db.get_job_history(archived.0).unwrap().is_some());
    assert!(
        rig.db.get_torrent(refused).unwrap().is_some(),
        "it stays retiring"
    );
    assert!(rig.db.get_job_history(refused.0).unwrap().is_none());

    let restarted = service_on(&rig.db, rig.dir.path(), &rig.routes, job_env(), set_clock());
    let queue = queue();
    let state = queue.state().clone();
    restarted.publish_to(queue);
    let mut retired = restarted.retired();
    crate::persistence::connection::ARCHIVE_REFUSED
        .lock()
        .unwrap()
        .push_back(crate::StateError::OutOfSpace(
            "database or disk is full".into(),
        ));
    restarted.open().await.unwrap();
    // The first run's kept rows are still in this database's memory, as a
    // new process's would not be: the retry lets the first run's go first.
    while heard.recv().await.expect("the writer's news") != ArchiveNews::Kept(refused) {}
    assert!(restarted.torrent(refused).await.is_none(), "never live");
    restarted.tick().await;
    let listed = state.list_jobs();
    let row = listed
        .iter()
        .find(|job| job.job_id == refused)
        .expect("shown while it waits");
    assert_eq!(
        row.download_wait_reason.as_deref(),
        Some(crate::jobs::handle::DISK_SPACE_WAIT_REASON)
    );

    rig.db.retry_kept_archives(0);
    while heard.recv().await.expect("the writer's news")
        != (ArchiveNews::LetGo {
            job_id: refused,
            written: true,
        })
    {}
    retired.wait_for(|retired| *retired >= 1).await.unwrap();
    assert!(rig.db.get_torrent(refused).unwrap().is_none());
    assert_eq!(
        rig.db.get_job_history(refused.0).unwrap().unwrap().status,
        "cancelled"
    );
    restarted.stop().await;
}

// A torrent whose finish row waits for room, then is removed: the removal's
// row takes the finish row's place, and the finish row's waiter, let go,
// does nothing. The job reaches the finished jobs once.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_finish_row_a_removal_takes_the_place_of_finishes_nothing() {
    use crate::persistence::ArchiveNews;
    let rig = rig(set_clock());
    rig.service.open().await.unwrap();
    let (news, mut heard) = tokio::sync::mpsc::unbounded_channel();
    rig.db.hear_archive_news(move |said| {
        let _ = news.send(said);
    });
    let mut waits = rig.service.history_waits();
    let mut retired = rig.service.retired();
    crate::persistence::connection::ARCHIVE_REFUSED
        .lock()
        .unwrap()
        .push_back(crate::StateError::OutOfSpace(
            "database or disk is full".into(),
        ));
    let files: [(&str, &[u8]); 1] = [("ember.mkv", &[0x5a_u8; 300])];
    let job_id = rig
        .service
        .add_opened(request(AddSource::Metainfo(metainfo(
            "ember", &files, 128, false,
        ))))
        .await
        .unwrap();
    // Unpacking refused its output: its job's failed row, the torrent kept.
    rig.service.refused(job_id, "refused".to_owned()).await;
    assert_eq!(heard.recv().await, Some(ArchiveNews::Kept(job_id)));
    assert_eq!(*retired.borrow(), 0);

    rig.service.remove(job_id, false, true).await.unwrap();
    retired.wait_for(|retired| *retired >= 1).await.unwrap();
    waits.wait_for(|ended| *ended >= 1).await.unwrap();
    assert_eq!(*retired.borrow(), 1, "finished once");
    assert_eq!(
        rig.db.get_job_history(job_id.0).unwrap().unwrap().status,
        "cancelled"
    );
    rig.service.stop().await;
}
