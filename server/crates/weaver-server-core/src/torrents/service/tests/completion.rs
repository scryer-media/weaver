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

// Furniture is so only at the torrent's root, as an NZB job's is only at
// its root: a whole .nfo in a folder of the torrent is a usable file, and
// beside a refused archive the job is partial.
#[test]
fn a_whole_nfo_in_a_torrents_folder_is_a_usable_file() {
    let files = [
        ("extras/ember.nfo", true),
        ("ember.part1.rar", true),
        ("ember.part2.rar", false),
    ];
    assert_eq!(torrent_status(&files, WHY), JobStatus::Partial);
    assert_eq!(
        torrent_status(&[("ember.nfo", true), ("ember.part1.rar", true)], WHY),
        JobStatus::Failed {
            error: WHY.to_owned(),
        }
    );
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

// A history row of `protocol` for `job_id`, delivered to `dir`.
fn history_row(job_id: JobId, dir: &Path, protocol: crate::jobs::Protocol) -> crate::JobHistoryRow {
    crate::JobHistoryRow {
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
        protocol,
        archived_at: None,
    }
}

// A torrent removed keeping its files, and another seeding one of them in
// place through the folder spelled as `spell` spells it: an archive of the
// first one's entry takes its other files and leaves the one the live
// torrent holds, whatever the spelling. Where the filesystem makes that
// spelling another folder (`same_place` false), the live torrent seeds a
// copy there, and the archive takes all of the first one's files.
async fn an_archive_leaves_what_a_torrent_seeds_through(
    spell: impl Fn(&Path) -> PathBuf,
    same_place: bool,
) {
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
    let spelled = spell(&inner);
    assert_ne!(spelled, inner);
    if !same_place {
        assert!(!spelled.exists(), "another folder");
        std::fs::create_dir_all(&spelled).unwrap();
        std::fs::write(spelled.join("a.bin"), shared).unwrap();
    }
    assert!(spelled.join("a.bin").exists());

    let (seeder, _) = seeding_placed(
        &rig,
        metainfo("a.bin", &[("a.bin", &shared)], 1000, false),
        Placement {
            save_path: Some(spelled.to_string_lossy().into_owned()),
            layout: Some(ContentLayout::NoSubfolder),
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
    if same_place {
        assert_eq!(std::fs::read(inner.join("a.bin")).unwrap(), shared);
    } else {
        assert!(!inner.join("a.bin").exists(), "its own copy goes");
        assert_eq!(std::fs::read(spelled.join("a.bin")).unwrap(), shared);
    }
    assert!(!inner.join("b.bin").exists());
    assert!(rig.service.torrent(seeder).await.is_some());
    rig.service.stop().await;
}

#[cfg(unix)]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_archive_leaves_a_file_a_live_torrent_seeds_through_a_linked_folder() {
    an_archive_leaves_what_a_torrent_seeds_through(
        |inner| {
            let link = inner.parent().unwrap().parent().unwrap().join("linked-set");
            std::os::unix::fs::symlink(inner, &link).unwrap();
            link
        },
        true,
    )
    .await;
}

// Where the filesystem the test runs on ignores case, as told by that
// filesystem (a file made in one case found in the other), the other case
// is the same folder and its file stays; where it keeps case apart, it is
// another folder, and the archive takes the first torrent's own.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_archive_leaves_a_file_a_live_torrent_seeds_through_another_case() {
    let probe = tempfile::tempdir().unwrap();
    std::fs::write(probe.path().join("case-probe"), b"").unwrap();
    let ignores_case = probe.path().join("CASE-PROBE").exists();
    an_archive_leaves_what_a_torrent_seeds_through(
        |inner| {
            let name = inner.file_name().unwrap().to_string_lossy().to_uppercase();
            inner.with_file_name(name)
        },
        ignores_case,
    )
    .await;
}

// A torrent added over a folder an NZB job delivered, whose marker names
// that job: the marker stays the NZB job's, so its entry's archive still
// takes the folder, and the torrent's own entry would not.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_torrent_added_over_an_nzb_jobs_folder_leaves_it_the_nzb_jobs() {
    use crate::jobs::working_dir::{OutputDirOwner, output_dir_owner};
    let rig = rig(set_clock());
    rig.service.open().await.unwrap();
    let parent = rig.dir.path().join("complete");
    let dir = parent.join("set");
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::write(dir.join("movie.mkv"), b"nzb").unwrap();
    let nzb = JobId(9_000);
    crate::jobs::working_dir::mark_weaver_owned_output_dir(&dir, nzb).unwrap();
    rig.db
        .insert_job_history(&history_row(nzb, &dir, crate::jobs::Protocol::Nzb))
        .unwrap();
    let data = [3_u8; 3000];
    let job_id = rig
        .service
        .add_opened(AddRequest {
            placement: Placement {
                save_path: Some(parent.to_string_lossy().into_owned()),
                ..Placement::default()
            },
            paused: true,
            ..request(AddSource::Metainfo(metainfo(
                "set",
                &[("set/a.bin", &data), ("set/b.bin", &data)],
                1000,
                false,
            )))
        })
        .await
        .unwrap();
    assert_eq!(
        PathBuf::from(
            rig.service
                .torrent(job_id)
                .await
                .unwrap()
                .storage_dir
                .unwrap()
        ),
        dir
    );
    assert!(
        matches!(output_dir_owner(&dir), Some(OutputDirOwner::Job(owner)) if owner == nzb),
        "the NZB job's marker stays"
    );
    rig.service.stop().await;

    let archived = archive(&rig, nzb).await;
    assert_eq!(
        archived.outcome,
        crate::ArchiveHistoryOutcome::Archived,
        "{:?}",
        archived.message
    );
    assert!(
        !dir.join("movie.mkv").exists(),
        "its archive takes its folder"
    );
}

// A removal whose mark the database refuses for a reason other than room
// fails, says so, and changes nothing: the torrent stays live with its
// files, its row unmarked.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_removal_whose_mark_is_refused_removes_nothing() {
    let rig = rig(set_clock());
    rig.service.open().await.unwrap();
    let (job_id, dir) = with_every_piece(&rig).await;
    let files = files_under(&dir);
    crate::torrents::persistence::MARK_REFUSED.store(2, Ordering::Relaxed);
    let removed = rig.service.remove(job_id, true, true).await;
    crate::torrents::persistence::MARK_REFUSED.store(0, Ordering::Relaxed);
    let error = removed.expect_err("the removal fails");
    assert!(
        error.to_string().contains("could not be recorded"),
        "{error}"
    );
    assert_eq!(files_under(&dir), files, "nothing is deleted");
    assert!(rig.service.torrent(job_id).await.is_some(), "still live");
    assert!(rig.db.get_torrent(job_id).unwrap().is_some());
    assert_eq!(
        rig.db.torrent_rows().unwrap(),
        (vec![job_id], Vec::new()),
        "its row is not marked"
    );
    assert!(rig.db.get_job_history(job_id.0).unwrap().is_none());
    rig.service.stop().await;
}

// A removal whose mark finds no room on the database's disk goes on, as a
// removal did before marks: its files go, and its history row waits for
// room through the kept archives and is written once there is.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_removal_whose_mark_finds_no_room_goes_on() {
    use crate::persistence::ArchiveNews;
    let rig = rig(set_clock());
    rig.service.open().await.unwrap();
    let (news, mut heard) = tokio::sync::mpsc::unbounded_channel();
    rig.db.hear_archive_news(move |said| {
        let _ = news.send(said);
    });
    let (job_id, dir) = with_every_piece(&rig).await;
    let mut retired = rig.service.retired();
    crate::persistence::connection::ARCHIVE_REFUSED
        .lock()
        .unwrap()
        .push_back(crate::StateError::OutOfSpace(
            "database or disk is full".into(),
        ));
    crate::torrents::persistence::MARK_REFUSED.store(1, Ordering::Relaxed);
    let removed = rig.service.remove(job_id, true, true).await;
    crate::torrents::persistence::MARK_REFUSED.store(0, Ordering::Relaxed);
    removed.unwrap();
    assert_eq!(heard.recv().await, Some(ArchiveNews::Kept(job_id)));
    assert!(rig.service.torrent(job_id).await.is_none(), "not live");
    assert!(files_under(&dir).is_empty(), "its files went");
    assert!(rig.db.get_job_history(job_id.0).unwrap().is_none());

    rig.db.retry_kept_archives(0);
    assert_eq!(
        heard.recv().await,
        Some(ArchiveNews::LetGo {
            job_id,
            written: true
        })
    );
    retired.wait_for(|retired| *retired >= 1).await.unwrap();
    assert_eq!(
        rig.db.get_job_history(job_id.0).unwrap().unwrap().status,
        "complete"
    );
    assert!(rig.db.get_torrent(job_id).unwrap().is_none());
    rig.service.stop().await;
}

// A removal with its files on a full disk: the mark lands first, the files
// go, then the history row finds no room, and the server stops. The next
// start archives it from its row, and it never comes back live to download
// again.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_stop_after_a_removals_files_went_brings_it_back_retiring() {
    use crate::persistence::ArchiveNews;
    let rig = rig(set_clock());
    rig.service.open().await.unwrap();
    let (news, mut heard) = tokio::sync::mpsc::unbounded_channel();
    rig.db.hear_archive_news(move |said| {
        let _ = news.send(said);
    });
    let (job_id, dir) = with_every_piece(&rig).await;
    crate::persistence::connection::ARCHIVE_REFUSED
        .lock()
        .unwrap()
        .push_back(crate::StateError::OutOfSpace(
            "database or disk is full".into(),
        ));
    rig.service.remove(job_id, true, true).await.unwrap();
    assert_eq!(heard.recv().await, Some(ArchiveNews::Kept(job_id)));
    rig.service.stop().await;
    assert!(files_under(&dir).is_empty(), "its files went");
    assert!(rig.db.get_torrent(job_id).unwrap().is_some());
    assert!(rig.db.get_job_history(job_id.0).unwrap().is_none());

    let data = rig.dir.path().join("data");
    std::fs::create_dir_all(&data).unwrap();
    crate::operations::recover_server_state(&rig.db, &data, &rig.dir.path().join("work"))
        .await
        .unwrap();
    assert_eq!(
        rig.db.get_job_history(job_id.0).unwrap().unwrap().status,
        "complete",
        "the status its row records"
    );
    assert!(rig.db.get_torrent(job_id).unwrap().is_none());
    let restarted = service_on(&rig.db, rig.dir.path(), &rig.routes, job_env(), set_clock());
    restarted.open().await.unwrap();
    assert!(restarted.torrent(job_id).await.is_none(), "never live");
    restarted.stop().await;
}

// A start whose read of the retiring torrents fails sweeps no folder of
// theirs: a retiring torrent's kept unpacking folder stays.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_start_that_cannot_read_the_retiring_torrents_keeps_their_unpacking_folders() {
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
    let work = rig.dir.path().join("work");
    let unpacking = unpacking_dir(&work, job_id);
    crate::jobs::working_dir::claim_named_working_dir(&work, &unpacking, job_id).unwrap();
    std::fs::write(unpacking.join("set.part1.rar"), b"kept").unwrap();
    crate::persistence::connection::ARCHIVE_REFUSED
        .lock()
        .unwrap()
        .push_back(crate::StateError::OutOfSpace(
            "database or disk is full".into(),
        ));
    rig.service.remove(job_id, false, true).await.unwrap();
    assert_eq!(heard.recv().await, Some(ArchiveNews::Kept(job_id)));
    rig.service.stop().await;

    crate::torrents::persistence::RETIRING_READ_FAILS.store(true, Ordering::Relaxed);
    let data = rig.dir.path().join("data");
    std::fs::create_dir_all(&data).unwrap();
    let recovered = crate::operations::recover_server_state(&rig.db, &data, &work).await;
    crate::torrents::persistence::RETIRING_READ_FAILS.store(false, Ordering::Relaxed);
    recovered.unwrap();
    assert!(
        unpacking.join("set.part1.rar").exists(),
        "the sweep leaves it"
    );
    assert!(
        rig.db.get_torrent(job_id).unwrap().is_some(),
        "still retiring"
    );
}

// A listed file under a link two folders down inside the torrent's folder
// stays, and so does what the link points at.
#[cfg(unix)]
#[test]
fn a_listed_file_under_a_link_two_folders_down_stays() {
    let root = tempfile::tempdir().unwrap();
    let dir = root.path().join("torrent");
    let elsewhere = root.path().join("elsewhere");
    std::fs::create_dir_all(dir.join("a")).unwrap();
    std::fs::create_dir_all(elsewhere.join("c")).unwrap();
    std::fs::write(elsewhere.join("c/x.bin"), b"x").unwrap();
    std::fs::write(dir.join("a/own.bin"), b"o").unwrap();
    std::os::unix::fs::symlink(&elsewhere, dir.join("a/b")).unwrap();
    crate::jobs::working_dir::mark_weaver_owned_output_dir(&dir, JobId(1)).unwrap();
    let listed = |path: PathBuf, whole: bool| HistoryFile {
        path: path.to_string_lossy().into_owned(),
        whole,
    };
    let removal = remove_history_files(
        JobId(1),
        &root.path().join("work"),
        &dir,
        &[
            listed(dir.join("a/b/c/x.bin"), false),
            listed(dir.join("a/b/c"), true),
            listed(dir.join("a/own.bin"), false),
        ],
        &LiveHeld::default(),
    );
    assert_eq!(removal.removed, 1);
    assert!(elsewhere.join("c/x.bin").exists());
    assert!(!dir.join("a/own.bin").exists());
    assert!(dir.join("a/b").exists());
}

// A listed whole folder that is itself a link: the link may go, never what
// it points at.
#[cfg(unix)]
#[test]
fn a_listed_whole_folder_that_is_a_link_leaves_its_target() {
    let root = tempfile::tempdir().unwrap();
    let dir = root.path().join("torrent");
    let elsewhere = root.path().join("elsewhere");
    std::fs::create_dir_all(&dir).unwrap();
    std::fs::create_dir_all(&elsewhere).unwrap();
    std::fs::write(elsewhere.join("x.bin"), b"x").unwrap();
    std::os::unix::fs::symlink(&elsewhere, dir.join("made")).unwrap();
    crate::jobs::working_dir::mark_weaver_owned_output_dir(&dir, JobId(1)).unwrap();
    remove_history_files(
        JobId(1),
        &root.path().join("work"),
        &dir,
        &[HistoryFile {
            path: dir.join("made").to_string_lossy().into_owned(),
            whole: true,
        }],
        &LiveHeld::default(),
    );
    assert!(elsewhere.join("x.bin").exists(), "its target stays whole");
}

// A recorded folder that is itself a link is not managed: nothing under it
// goes, and neither does what it points at.
#[cfg(unix)]
#[test]
fn a_recorded_folder_that_is_a_link_is_not_managed() {
    let root = tempfile::tempdir().unwrap();
    let real = root.path().join("real");
    std::fs::create_dir_all(&real).unwrap();
    std::fs::write(real.join("a.bin"), b"a").unwrap();
    crate::jobs::working_dir::mark_weaver_owned_output_dir(&real, JobId(1)).unwrap();
    let dir = root.path().join("torrent");
    std::os::unix::fs::symlink(&real, &dir).unwrap();
    let removal = remove_history_files(
        JobId(1),
        &root.path().join("work"),
        &dir,
        &[HistoryFile {
            path: dir.join("a.bin").to_string_lossy().into_owned(),
            whole: false,
        }],
        &LiveHeld::default(),
    );
    assert_eq!(removal.removed, 0);
    assert_eq!(removal.not_managed, vec![dir.clone()]);
    assert!(real.join("a.bin").exists());
}

// What `held_of` writes for a torrent whose files' top-level folder is
// `top`, over a folder already there as `on_disk`, as the history side
// reads it.
fn held_over(on_disk: &str, top: &str) -> (tempfile::TempDir, LiveHeld) {
    let base = tempfile::tempdir().unwrap();
    std::fs::create_dir_all(base.path().join(on_disk)).unwrap();
    std::fs::write(base.path().join(on_disk).join("a.bin"), b"a").unwrap();
    let tops = std::collections::BTreeSet::from([top.to_owned()]);
    let written = held_of(base.path().to_str(), None, Some(&tops));
    let mut held = LiveHeld::default();
    for path in written.into_iter().map(PathBuf::from) {
        held.above
            .extend(path.ancestors().skip(1).map(Path::to_path_buf));
        held.paths.insert(path);
    }
    (base, held)
}

// An adopted folder spelled `set` on disk, its metainfo naming it `Set`:
// where the filesystem ignores case it is the folder the torrent holds, as
// the filesystem spells it; where it keeps case apart, `Set` is another
// folder, and the one on disk is not held.
#[test]
fn an_adopted_top_level_name_in_another_case_is_held_where_case_is_ignored() {
    let (base, held) = held_over("set", "Set");
    let file = base.path().join("set").join("a.bin");
    let same = base.path().join("Set").exists();
    assert_eq!(held.holds(&file, false), same);
    assert_eq!(held.holds(&base.path().join("set"), true), same);
}

// An adopted folder whose name is on disk decomposed (NFD), its metainfo
// naming it composed (NFC): where the filesystem finds one by the other it
// is the folder the torrent holds; where it keeps them apart, it is not.
#[test]
fn an_adopted_top_level_name_in_another_unicode_form_is_held_where_forms_are_one() {
    let (nfd, nfc) = ("e\u{301}te", "\u{e9}te");
    let (base, held) = held_over(nfd, nfc);
    let file = base.path().join(nfd).join("a.bin");
    let same = base.path().join(nfc).exists();
    assert_eq!(held.holds(&file, false), same);
    // And the other way about.
    let (base, held) = held_over(nfc, nfd);
    let file = base.path().join(nfc).join("a.bin");
    let same = base.path().join(nfd).exists();
    assert_eq!(held.holds(&file, false), same);
}

// A stop after a removal's mark landed and before anything of the torrent
// went (a crash between the two, the torrent's row marked and nothing
// else): the next start finishes the removal from the row. Asked with its
// files, its files and its unpacking folder go; kept, they stay, its kept
// unpacking folder listed on its history row so the start's sweep leaves
// it. Either way its history row is written from its torrents row, and it
// never comes back live.
async fn a_crash_after_the_mark_is_finished_at_the_next_start(delete_files: bool) {
    let rig = rig(set_clock());
    rig.service.open().await.unwrap();
    let (job_id, dir) = with_every_piece(&rig).await;
    let files = files_under(&dir);
    let work = rig.dir.path().join("work");
    let unpacking = unpacking_dir(&work, job_id);
    crate::jobs::working_dir::claim_named_working_dir(&work, &unpacking, job_id).unwrap();
    std::fs::write(unpacking.join("set.part1.rar"), b"kept").unwrap();
    rig.service.stop().await;
    rig.db.mark_torrent_retiring(job_id, delete_files).unwrap();

    let data = rig.dir.path().join("data");
    std::fs::create_dir_all(&data).unwrap();
    crate::operations::recover_server_state(&rig.db, &data, &work)
        .await
        .unwrap();
    let history = rig.db.get_job_history(job_id.0).unwrap().unwrap();
    assert_eq!(history.status, "complete", "its row says it completed");
    assert_eq!(history.protocol, crate::jobs::Protocol::Torrent);
    assert!(rig.db.get_torrent(job_id).unwrap().is_none());
    let listed: Vec<PathBuf> = rig
        .db
        .job_history_files(&[job_id])
        .unwrap()
        .remove(&job_id)
        .unwrap_or_default()
        .into_iter()
        .map(|file| PathBuf::from(file.path))
        .collect();
    if delete_files {
        assert!(files_under(&dir).is_empty(), "its files went");
        assert!(!unpacking.exists(), "its unpacking folder went");
    } else {
        assert_eq!(files_under(&dir), files, "its files stay");
        assert!(
            unpacking.join("set.part1.rar").exists(),
            "the sweep leaves its kept unpacking folder"
        );
        let unpacking = std::fs::canonicalize(&unpacking).unwrap();
        assert!(
            listed
                .iter()
                .any(|path| std::fs::canonicalize(path).ok().as_ref() == Some(&unpacking)),
            "{listed:?}"
        );
    }
    let restarted = service_on(&rig.db, rig.dir.path(), &rig.routes, job_env(), set_clock());
    restarted.open().await.unwrap();
    assert!(restarted.torrent(job_id).await.is_none(), "never live");
    restarted.stop().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_crash_after_a_removals_mark_with_its_files_deletes_them_at_the_next_start() {
    a_crash_after_the_mark_is_finished_at_the_next_start(true).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_crash_after_a_removals_mark_keeping_its_files_keeps_them_at_the_next_start() {
    a_crash_after_the_mark_is_finished_at_the_next_start(false).await;
}

// A start while another live torrent holds a file the retiring one's
// removal asked to go: that file stays, the rest go.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_crash_after_the_mark_leaves_what_a_live_torrent_holds() {
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
    let inner = dir.join("set");
    let (seeder, _) = seeding_placed(
        &rig,
        metainfo("a.bin", &[("a.bin", &shared)], 1000, false),
        Placement {
            save_path: Some(inner.to_string_lossy().into_owned()),
            layout: Some(ContentLayout::NoSubfolder),
            ..Placement::default()
        },
    )
    .await;
    rig.service.stop().await;
    rig.db.mark_torrent_retiring(first, true).unwrap();

    let data = rig.dir.path().join("data");
    std::fs::create_dir_all(&data).unwrap();
    crate::operations::recover_server_state(&rig.db, &data, &rig.dir.path().join("work"))
        .await
        .unwrap();
    assert!(rig.db.get_job_history(first.0).unwrap().is_some());
    assert_eq!(std::fs::read(inner.join("a.bin")).unwrap(), shared);
    assert!(!inner.join("b.bin").exists());
    assert!(rig.db.get_torrent(seeder).unwrap().is_some());
}

// Whether the torrents row of `job_id` says its held paths are its own.
fn held_set(rig: &Rig, job_id: JobId) -> bool {
    rig.db
        .restored_torrents()
        .unwrap()
        .into_iter()
        .find(|torrent| torrent.record.job_id == job_id)
        .unwrap()
        .held_set
}

// Whether a live torrent holds every file under `dir`, as a history
// removal from `dir` reads it.
fn holds_all(rig: &Rig, dir: &Path) -> bool {
    let held = LiveHeld::under(&rig.db, &[dir.to_path_buf()]).unwrap();
    let files = files_under(dir);
    !files.is_empty() && files.iter().all(|file| held.holds(file, false))
}

// A magnet before its metainfo arrives holds its whole folder, and its row
// says its paths are not yet its own, so the next start writes them.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_magnet_before_its_metainfo_holds_its_whole_folder() {
    let rig = rig(set_clock());
    rig.service.open().await.unwrap();
    rig.routes.set_down(Some("tunnel gone"));
    let hash = "0123456789abcdef0123456789abcdef01234567";
    let magnet = format!("magnet:?xt=urn:btih:{hash}&tr=http%3A%2F%2F127.0.0.1%3A9%2Fannounce");
    let job_id = rig
        .service
        .add(request(AddSource::Magnet(magnet)))
        .await
        .unwrap();
    assert!(!held_set(&rig, job_id));
    if let Some(dir) = rig.db.get_torrent(job_id).unwrap().unwrap().storage_dir {
        let dir = PathBuf::from(dir);
        let held = LiveHeld::under(&rig.db, std::slice::from_ref(&dir)).unwrap();
        assert!(held.holds(&dir.join("anything.bin"), false));
    }
    rig.service.stop().await;
}

// A torrent whose row knows its files and holds none of them (a crash
// between writes that once came apart, set up here by state): the next
// start writes what it holds.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_row_with_its_files_known_and_no_held_paths_is_written_at_the_next_start() {
    let rig = rig(set_clock());
    rig.service.open().await.unwrap();
    let (job_id, dir) = with_every_piece(&rig).await;
    assert!(held_set(&rig, job_id));
    assert!(holds_all(&rig, &dir));
    rig.service.stop().await;
    rig.db.set_torrents_held(&[(job_id, Vec::new())]).unwrap();
    rig.db
        .set_torrent_storage_dir(job_id, &dir.to_string_lossy())
        .unwrap();
    assert!(!held_set(&rig, job_id), "a folder's write marks it");
    assert!(!holds_all(&rig, &dir));

    let again = service_on(&rig.db, rig.dir.path(), &rig.routes, job_env(), set_clock());
    again.open().await.unwrap();
    assert!(held_set(&rig, job_id));
    assert!(holds_all(&rig, &dir));
    again.stop().await;
}

// A move whose write of what the torrent holds after it fails: the
// destination was held before the first file moved there, and the row
// says its paths are to be written again at the next start, never rows
// marked good that are not.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_move_whose_held_write_fails_leaves_the_destination_held_and_unset() {
    let rig = rig(set_clock());
    rig.service.open().await.unwrap();
    let (job_id, dir) = with_every_piece(&rig).await;
    let elsewhere = rig.dir.path().join("elsewhere");
    std::fs::create_dir(&elsewhere).unwrap();
    crate::torrents::service::SYNC_HELD_FAILS.store(1, Ordering::Relaxed);
    let moved = rig
        .service
        .set_location(job_id, elsewhere.to_string_lossy().into_owned())
        .await;
    crate::torrents::service::SYNC_HELD_FAILS.store(0, Ordering::Relaxed);
    moved.unwrap();
    let there = PathBuf::from(
        rig.service
            .torrent(job_id)
            .await
            .unwrap()
            .storage_dir
            .unwrap(),
    );
    assert_ne!(there, dir);
    assert!(there.starts_with(&elsewhere));
    assert!(!held_set(&rig, job_id));
    assert!(holds_all(&rig, &there));
    rig.service.stop().await;
}

// A move whose destination cannot be held before its files move there
// moves nothing and says so: a file reaches the destination only once the
// destination is held. The torrent can be moved again after.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_move_whose_destination_cannot_be_held_moves_nothing() {
    let rig = rig(set_clock());
    rig.service.open().await.unwrap();
    let (job_id, dir) = with_every_piece(&rig).await;
    let files = files_under(&dir);
    let elsewhere = rig.dir.path().join("elsewhere");
    std::fs::create_dir(&elsewhere).unwrap();
    crate::torrents::service::SYNC_HELD_FAILS.store(2, Ordering::Relaxed);
    let moved = rig
        .service
        .set_location(job_id, elsewhere.to_string_lossy().into_owned())
        .await;
    crate::torrents::service::SYNC_HELD_FAILS.store(0, Ordering::Relaxed);
    let error = moved.expect_err("the move is refused");
    assert!(error.to_string().contains("not moved"), "{error}");
    assert_eq!(files_under(&dir), files, "nothing moved");
    assert!(files_under(&elsewhere).is_empty());
    assert_eq!(
        rig.service
            .torrent(job_id)
            .await
            .unwrap()
            .storage_dir
            .map(PathBuf::from),
        Some(dir.clone())
    );
    rig.service
        .set_location(job_id, elsewhere.to_string_lossy().into_owned())
        .await
        .unwrap();
    let there = PathBuf::from(
        rig.service
            .torrent(job_id)
            .await
            .unwrap()
            .storage_dir
            .unwrap(),
    );
    assert!(held_set(&rig, job_id));
    assert!(holds_all(&rig, &there));
    rig.service.stop().await;
}

// Holds the next removal's mark until the returned sender lets it go, once
// the removal has reached it.
async fn hold_the_mark(
    rig: &Rig,
    job_id: JobId,
    delete_files: bool,
) -> (
    tokio::task::JoinHandle<Result<(), ActionError>>,
    tokio::sync::oneshot::Sender<()>,
) {
    let (reached, at) = tokio::sync::oneshot::channel();
    let (release, released) = tokio::sync::oneshot::channel();
    *locked(&MARK_HOLD) = Some((reached, released));
    let service = rig.service.clone();
    let removing = tokio::spawn(async move { service.remove(job_id, delete_files, true).await });
    at.await.unwrap();
    (removing, release)
}

// A move asked for while a removal writes its mark is refused, and the
// removal goes on: its files go, and it never moves.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_move_asked_while_a_removal_writes_its_mark_is_refused() {
    let rig = rig(set_clock());
    rig.service.open().await.unwrap();
    let (job_id, dir) = with_every_piece(&rig).await;
    let elsewhere = rig.dir.path().join("elsewhere");
    std::fs::create_dir_all(&elsewhere).unwrap();
    let (removing, release) = hold_the_mark(&rig, job_id, true).await;

    let moved = rig
        .service
        .set_location(job_id, elsewhere.to_string_lossy().into_owned())
        .await;
    assert_eq!(
        moved,
        Err(ActionError::Invalid(
            "the torrent is being removed".to_owned()
        ))
    );
    release.send(()).unwrap();
    removing.await.unwrap().unwrap();
    assert!(files_under(&dir).is_empty(), "its files went");
    assert!(files_under(&elsewhere).is_empty(), "nothing moved");
    assert!(rig.db.get_torrent(job_id).unwrap().is_none());
    rig.service.stop().await;
}

// A removal asked for while the torrent's files move is refused before it
// marks anything: the torrent stays live, its row unmarked.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_removal_asked_while_the_files_move_is_refused() {
    let rig = rig(set_clock());
    rig.service.open().await.unwrap();
    let (job_id, dir) = with_every_piece(&rig).await;
    let files = files_under(&dir);
    // Its files moving, or unpacking placing its outputs beside them.
    for moving in [Moving::Files, Moving::Outputs] {
        rig.service
            .inner
            .live
            .lock()
            .await
            .get_mut(&job_id)
            .unwrap()
            .moving = Some(moving);

        let removed = rig.service.remove(job_id, true, true).await;
        assert!(
            matches!(&removed, Err(ActionError::Invalid(why)) if why.contains("moving")),
            "{removed:?}"
        );
        assert_eq!(
            rig.db.torrent_rows().unwrap(),
            (vec![job_id], Vec::new()),
            "its row is not marked"
        );
        assert_eq!(files_under(&dir), files, "nothing is deleted");
    }
    rig.service
        .inner
        .live
        .lock()
        .await
        .get_mut(&job_id)
        .unwrap()
        .moving = None;
    rig.service.remove(job_id, true, true).await.unwrap();
    assert!(files_under(&dir).is_empty());
    rig.service.stop().await;
}

// A failure while a removal writes its mark leaves the torrent to the
// removal: it goes to history as removed, not failed, and no mark is left
// for a later start.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_failure_while_a_removal_writes_its_mark_leaves_it_to_the_removal() {
    let rig = rig(set_clock());
    rig.service.open().await.unwrap();
    let (job_id, dir) = with_every_piece(&rig).await;
    let mut retired = rig.service.retired();
    let (removing, release) = hold_the_mark(&rig, job_id, true).await;

    rig.service
        .fail(job_id, "the disk went away".to_owned())
        .await;
    assert!(
        rig.service.inner.live.lock().await.contains_key(&job_id),
        "the failure does not take it"
    );
    release.send(()).unwrap();
    removing.await.unwrap().unwrap();
    retired.wait_for(|retired| *retired >= 1).await.unwrap();
    assert!(files_under(&dir).is_empty(), "its files went");
    assert!(
        rig.db.get_torrent(job_id).unwrap().is_none(),
        "no mark is left"
    );
    let history = rig.db.get_job_history(job_id.0).unwrap().unwrap();
    assert_ne!(history.status, "failed");
    assert_eq!(history.error_message, None);
    rig.service.stop().await;
}

// A retiring row whose files go takes them at the start only when what was
// there before the torrent was written: one from before that was kept
// (`adopted` never written) keeps every file, one written empty takes them.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_retiring_torrent_whose_prior_contents_are_unknown_keeps_its_files() {
    for written in [true, false] {
        let rig = rig(set_clock());
        rig.service.open().await.unwrap();
        let (job_id, dir) = with_every_piece(&rig).await;
        let files = files_under(&dir);
        rig.service.stop().await;
        rig.db.mark_torrent_retiring(job_id, true).unwrap();
        if !written {
            rig.db.forget_torrent_adopted(job_id).unwrap();
        }
        let rows = rig.db.retiring_torrents().unwrap();
        assert_eq!(rows.len(), 1);
        assert_eq!(
            rows[0].adopted,
            written.then(crate::torrents::model::Adopted::default),
            "written empty, or never written"
        );

        let db = rig.db.clone();
        let work = rig.dir.path().join("work");
        let finished = tokio::task::spawn_blocking(move || finish_retiring(&db, &work))
            .await
            .unwrap()
            .unwrap();
        assert_eq!(finished.len(), 1);
        if written {
            assert!(
                files_under(&dir).is_empty(),
                "nothing was there: its files go"
            );
        } else {
            assert_eq!(files_under(&dir), files, "not known: every file stays");
        }
    }
}

// An add of a torrent whose removal has not ended is refused as still
// being removed, at once, never as a duplicate.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_add_of_a_torrent_still_being_removed_says_so() {
    use crate::persistence::ArchiveNews;
    let rig = rig(set_clock());
    rig.service.open().await.unwrap();
    let (news, mut heard) = tokio::sync::mpsc::unbounded_channel();
    rig.db.hear_archive_news(move |said| {
        let _ = news.send(said);
    });
    let (job_id, _) = with_every_piece(&rig).await;
    assert_eq!(
        rig.service
            .add(request(AddSource::Metainfo(plain("set", 1))))
            .await,
        Err(AddError::Duplicate(job_id)),
        "a live torrent is a duplicate"
    );
    crate::persistence::connection::ARCHIVE_REFUSED
        .lock()
        .unwrap()
        .push_back(crate::StateError::OutOfSpace(
            "database or disk is full".into(),
        ));
    rig.service.remove(job_id, true, true).await.unwrap();
    assert_eq!(heard.recv().await, Some(ArchiveNews::Kept(job_id)));
    assert!(
        rig.db.get_torrent(job_id).unwrap().is_some(),
        "still retiring"
    );

    let added = rig
        .service
        .add(request(AddSource::Metainfo(plain("set", 1))))
        .await;
    assert_eq!(added, Err(AddError::Refused(Refusal::Removing(job_id))));
    let said = added.unwrap_err().to_string();
    assert!(said.contains("still being removed"), "{said}");
    rig.service.stop().await;
}

// A torrent's top folder made in another case before its first write: from
// the next start, what the torrent holds is spelled as the filesystem
// spells the folder its files went into. Where the filesystem tells case
// apart, the torrent made its own folder and holds that.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn what_a_torrent_holds_is_spelled_as_the_folder_its_files_went_into() {
    let rig = rig(set_clock());
    rig.service.open().await.unwrap();
    let job_id = rig
        .service
        .add_opened(request(AddSource::Metainfo(plain("set", 1))))
        .await
        .unwrap();
    let storage = PathBuf::from(
        rig.service
            .torrent(job_id)
            .await
            .unwrap()
            .storage_dir
            .unwrap(),
    );
    std::fs::create_dir_all(storage.join("SET")).unwrap();
    let ignores_case = storage.join("set").exists();
    fetch_every_piece(&rig.service, job_id, &[1; 3000]).await;
    rig.service.stop().await;

    let restarted = service_on(&rig.db, rig.dir.path(), &rig.routes, job_env(), set_clock());
    restarted.open().await.unwrap();
    let spelled = if ignores_case { "SET" } else { "set" };
    let expected = resolved(&storage)
        .join(spelled)
        .to_string_lossy()
        .into_owned();
    assert_eq!(
        rig.db.held_rows(Some(job_id)).unwrap(),
        [(job_id, expected)]
    );
    restarted.stop().await;
}

// A removal whose request is dropped while it writes its mark goes on all
// the same: once the mark is let go, the torrent is retired with its files
// gone, never left flagged as being removed.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_removal_whose_request_is_dropped_at_its_mark_still_ends() {
    let rig = rig(set_clock());
    rig.service.open().await.unwrap();
    let (job_id, dir) = with_every_piece(&rig).await;
    let mut retired = rig.service.retired();
    let (reached, at) = tokio::sync::oneshot::channel();
    let (release, released) = tokio::sync::oneshot::channel();
    *locked(&MARK_HOLD) = Some((reached, released));
    let mut asked = Box::pin(rig.service.remove(job_id, true, true));
    tokio::select! {
        biased;
        _ = at => {}
        ended = &mut asked => panic!("the removal ended before its mark: {ended:?}"),
    }
    drop(asked);

    release.send(()).unwrap();
    retired.wait_for(|retired| *retired >= 1).await.unwrap();
    assert!(rig.service.torrent(job_id).await.is_none(), "not live");
    assert!(
        rig.db.get_torrent(job_id).unwrap().is_none(),
        "no mark is left"
    );
    assert!(files_under(&dir).is_empty(), "its files went");
    assert!(rig.db.get_job_history(job_id.0).unwrap().is_some());
    rig.service.stop().await;
}

// A move whose request is dropped while the torrent shows moving goes on
// all the same: once let go, its files are at the destination and the
// queue no longer shows it moving.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_move_whose_request_is_dropped_while_moving_still_ends() {
    let rig = rig(set_clock());
    let queue = queue();
    let state = queue.state().clone();
    rig.service.publish_to(queue);
    rig.service.open().await.unwrap();
    let (job_id, dir) = with_every_piece(&rig).await;
    let files = files_under(&dir);
    let elsewhere = rig.dir.path().join("elsewhere");
    std::fs::create_dir_all(&elsewhere).unwrap();
    let moving = || {
        state
            .list_jobs()
            .iter()
            .find(|job| job.job_id == job_id)
            .map(|job| job.status == crate::jobs::JobStatus::Moving)
    };
    let (reached, at) = tokio::sync::oneshot::channel();
    let (release, released) = tokio::sync::oneshot::channel();
    *locked(&MOVE_HOLD) = Some((reached, released));
    let mut asked = Box::pin(
        rig.service
            .set_location(job_id, elsewhere.to_string_lossy().into_owned()),
    );
    tokio::select! {
        biased;
        _ = at => {}
        ended = &mut asked => panic!("the move ended before it held: {ended:?}"),
    }
    assert_eq!(moving(), Some(true), "the queue shows it moving");
    let mut changes = state.subscribe_job_changes();
    drop(asked);

    release.send(()).unwrap();
    while moving() != Some(false) {
        changes.changed().await.unwrap();
    }
    let there = PathBuf::from(
        rig.service
            .torrent(job_id)
            .await
            .unwrap()
            .storage_dir
            .unwrap(),
    );
    assert_eq!(there.parent(), Some(elsewhere.as_path()));
    let relative = |under: &Path, found: Vec<PathBuf>| -> Vec<PathBuf> {
        found
            .iter()
            .map(|file| file.strip_prefix(under).unwrap().to_path_buf())
            .collect()
    };
    assert_eq!(
        relative(&there, files_under(&there)),
        relative(&dir, files),
        "its files are at the destination"
    );
    assert!(files_under(&dir).is_empty(), "none is left where they were");
    rig.service.stop().await;
}

// A failure while a removal writes its mark, and then the mark refused:
// the removal does not go ahead, and the failure it held fails the
// torrent, which goes to history as failed with that error.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_failure_held_by_a_removal_whose_mark_is_refused_fails_the_torrent() {
    let rig = rig(set_clock());
    rig.service.open().await.unwrap();
    let (job_id, dir) = with_every_piece(&rig).await;
    let files = files_under(&dir);
    let mut retired = rig.service.retired();
    crate::torrents::persistence::MARK_REFUSED.store(2, Ordering::Relaxed);
    let (removing, release) = hold_the_mark(&rig, job_id, true).await;
    rig.service
        .fail(job_id, "the disk went away".to_owned())
        .await;
    release.send(()).unwrap();
    let removed = removing.await.unwrap();
    crate::torrents::persistence::MARK_REFUSED.store(0, Ordering::Relaxed);
    assert!(matches!(removed, Err(ActionError::State(_))), "{removed:?}");

    retired.wait_for(|retired| *retired >= 1).await.unwrap();
    assert!(rig.service.torrent(job_id).await.is_none(), "not live");
    let history = rig.db.get_job_history(job_id.0).unwrap().unwrap();
    assert_eq!(history.status, "failed");
    assert_eq!(history.error_message.as_deref(), Some("the disk went away"));
    assert_eq!(files_under(&dir), files, "a failure keeps its data");
    rig.service.stop().await;
}

// A live removal of a torrent whose row never kept what was there before
// it takes none of its files, as a start's finish of the same row would.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_removal_with_what_was_there_unknown_keeps_every_file() {
    let rig = rig(set_clock());
    rig.service.open().await.unwrap();
    let (job_id, dir) = with_every_piece(&rig).await;
    let files = files_under(&dir);
    rig.db.forget_torrent_adopted(job_id).unwrap();
    assert_eq!(rig.db.torrent_adopted(job_id).unwrap(), None);

    rig.service.remove(job_id, true, true).await.unwrap();
    assert!(rig.service.torrent(job_id).await.is_none(), "removed");
    assert_eq!(files_under(&dir), files, "not known: every file stays");
    rig.service.stop().await;
}
