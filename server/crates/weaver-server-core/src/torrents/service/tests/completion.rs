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
            .is_some_and(|member| {
                crate::pipeline::post_health::names_a_piece(&member.output_name)
            })
    };
    let delivery = Delivery::read(
        &tree,
        |_| true,
        |_| false,
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
    short_status(&whole, why)
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
    assert!(rig.db.get_torrent(job_id).unwrap().is_some(), "kept with it");
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
