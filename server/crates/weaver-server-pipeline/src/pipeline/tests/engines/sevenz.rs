// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// 7z containers: stored whole, split and posted header
// first, compressed, enciphered with and without the job's password, and
// damaged under a PAR2 set.

use super::harness::{
    ARTICLE, JOB, Post, Scenario, admit, collect, par2_set, par2_volumes_sliced, run,
    run_unprompted,
};
use super::refused_then_repaired::{drive_to_finish, serve_posts, serve_queued};
use super::sevenz_fixtures::store::{FRAME, assert_members, stored};
pub(super) use super::sevenz_fixtures::store::{SECRET, packed_end, sealed_header};
pub(super) use super::sevenz_fixtures::{container, delivered, members, split};
use crate::jobs::model::JobStatus;
use crate::pipeline::Pipeline;
use crate::pipeline::tests::direct_store::multi_member_store_set;
use crate::pipeline::tests::direct_store::scenarios::sevenz_store::{Entry, build_7z_shaped};
use crate::pipeline::tests::{
    job_status_for_assert, pump_pipeline_runtime_queues, retire_pipeline_database,
};
use sevenz_turbo::encoder_options::Lzma2Options;
use sevenz_turbo::{EncoderConfiguration, EncoderMethod};
use std::collections::BTreeMap;
use weaver_graph::recovery::RepairOutcome;
use weaver_server_base::jobs::ids::{NzbFileId, SegmentId};

// The candidates are tried in their order: the job's own, then the
// settings'. The one the archive opened with is what the history keeps,
// sealed, and never as a public attribute or in the clear.
#[tokio::test]
async fn archive_password_sources_are_ordered_and_validated_password_is_private() {
    let scenario = Scenario::new(vec![Post::plain("lantern.7z", stored(Some(SECRET)))])
        .password("filename-key")
        .global_password("global-key")
        .global_password(SECRET);
    let graph = run(&scenario).await;
    assert_members(&graph);
    assert_eq!(graph.validated_password.as_deref(), Some(SECRET));
    let stored = graph
        .history_metadata
        .as_deref()
        .expect("the history row keeps the job's metadata");
    assert!(!stored.contains(SECRET), "{stored}");
    let metadata = crate::history::attributes::parse_history_metadata(Some(stored));
    assert!(metadata.iter().any(
        |(key, _)| key == crate::history::attributes::VALIDATED_ARCHIVE_PASSWORD_ATTRIBUTE_KEY
    ));
    assert!(
        !crate::public_history_attributes(&metadata)
            .iter()
            .any(|(key, _)| key.contains("validated_archive_password"))
    );
}

// The last volume and the first are whole before the second has come:
// `.003` has no signature and is a leaf until `.001` binds the store, which
// is handed it closed. The store asks for the header's password on the
// bytes it is handed, and the job's password answers before the container
// is ruled shut, though the volume that holds the header is already in.
#[tokio::test]
async fn a_split_7z_with_encrypted_headers_whose_last_volume_came_first_opens_on_the_job_password()
{
    let scenario = Scenario::new(split(
        &sealed_header(EncoderConfiguration::new(EncoderMethod::COPY)),
        3,
    ))
    .password(SECRET);
    let (_temp_dir, mut pipeline, complete_dir) = admit(&scenario).await;
    serve_posts(&mut pipeline, &scenario, 2..3).await;
    serve_posts(&mut pipeline, &scenario, 0..1).await;
    serve_posts(&mut pipeline, &scenario, 1..2).await;
    drive_to_finish(&mut pipeline, &scenario).await;
    let status = job_status_for_assert(&pipeline, JOB);
    let mut files = BTreeMap::new();
    collect(&complete_dir, &complete_dir, &mut files);
    retire_pipeline_database(pipeline).await;

    assert_eq!(status, Some(JobStatus::Complete), "{:?}", files.keys());
    let expected = members();
    assert_eq!(
        files.keys().cloned().collect::<Vec<_>>(),
        expected
            .iter()
            .map(|(name, _)| delivered(name))
            .collect::<Vec<_>>()
    );
    for (name, bytes) in expected {
        assert!(files[&delivered(name)] == bytes, "{name} bytes");
    }
}

// A coded split set beside PAR2 with one article of a middle volume
// damaged: the recovery blocks it needs are in by the time the payload is,
// so the set rules and repairs in the step its last byte arrives, the run
// reads on through the repaired bytes, and the delivery is scheduled by the
// job's own steps, never by a look the pipeline takes when it falls idle.
async fn damaged_split_rules_unprompted(count: usize, slice: u64, blocks: u32, volumes: u32) {
    let archive = container(Lzma2Options::from_level(1).into(), None);
    let parts = split(&archive, count);
    let files: Vec<(&str, &[u8])> = parts
        .iter()
        .map(|post| (post.name.as_str(), post.bytes.as_slice()))
        .collect();
    let mut posts = par2_volumes_sliced("lantern", &files, slice, blocks, volumes);
    let recovery: Vec<Post> = posts.split_off(1);
    let first = posts.len() as u32;
    posts.extend(parts);
    posts.extend(recovery);
    let scenario = Scenario::new(posts).damaged(first + 1, 1);
    let (outcomes, moved) = run_unprompted(&scenario).await;
    assert!(
        matches!(outcomes.as_slice(), [Some(RepairOutcome::Repaired { .. })]),
        "{outcomes:?}"
    );
    assert!(moved, "the delivery waited for an idle look");
}

#[tokio::test]
async fn a_damaged_split_lzma2_7z_under_par2_rules_and_delivers_without_an_idle_look() {
    damaged_split_rules_unprompted(3, ARTICLE as u64, 2, 1).await;
}

#[tokio::test]
async fn a_damaged_six_volume_lzma2_7z_under_five_par2_volumes_rules_and_delivers_without_an_idle_look()
 {
    damaged_split_rules_unprompted(6, ARTICLE as u64 / 4, 16, 5).await;
}

// A full member checksum rejects all its bytes even though only one input
// slice is corrupt. Repairing that slice must replay the rejected member's
// dependency, while its live store still owns the input.
#[tokio::test]
async fn live_stores_replay_checksum_rejected_members_after_repair() {
    use super::harness::par3_set;
    use super::refused_then_repaired::serve_queued;
    use crate::pipeline::tests::pump_pipeline_runtime_queues;
    use weaver_graph::{MemberId, NodeId};

    for rar in [false, true] {
        for par3 in [false, true] {
            let entries = members();
            let (name, original) = if rar {
                let (_, bytes) = multi_member_store_set(&entries, 1).remove(0);
                ("lantern.rar", bytes)
            } else {
                ("lantern.7z", stored(None))
            };
            let payload = &entries[0].1;
            let at = original
                .windows(payload.len())
                .position(|window| window == payload)
                .expect("stored member bytes");
            let mut posted = original.clone();
            posted[at + ARTICLE + 3] ^= 0x5a;
            let mut posts = vec![Post::plain(name, posted)];
            posts.extend(if par3 {
                par3_set("lantern", &[(name, &original)], 4)
            } else {
                par2_set("lantern", &[(name, &original)], 4)
            });
            let scenario = Scenario::new(posts);
            let (_temp, mut pipeline, complete_dir) = admit(&scenario).await;
            let count = original.len().div_ceil(ARTICLE) as u32;
            // The prefix and tail precede the middle: a fragmented fold
            // must still reject the full member before recovery arrives.
            for ordinal in std::iter::once(0).chain(2..count).chain(std::iter::once(1)) {
                serve_queued(&mut pipeline, &scenario, (0, ordinal)).await;
                pump_pipeline_runtime_queues(&mut pipeline).await;
            }
            let tree = pipeline.graph_jobs.get(&JOB).expect("active graph").tree();
            let (store, _) = tree
                .child_of(NodeId::ROOT, MemberId(0))
                .expect("live store");
            assert!(tree.engine(store).is_none(), "the store must remain live");
            assert!(
                tree.namespace(store).unwrap().members().any(|member| {
                    member.crc32.is_some()
                        && member
                            .coverage
                            .runs()
                            .any(|run| run.provenance == weaver_graph::Provenance::Refuted)
                }),
                "the output checksum must fail before repair"
            );
            serve_posts(&mut pipeline, &scenario, 1..scenario.posts.len() as u32).await;
            drive_to_finish(&mut pipeline, &scenario).await;
            let status = job_status_for_assert(&pipeline, JOB);
            let mut files = BTreeMap::new();
            collect(&complete_dir, &complete_dir, &mut files);
            retire_pipeline_database(pipeline).await;
            assert_eq!(status, Some(JobStatus::Complete), "rar={rar}, par3={par3}");
            for (name, expected) in entries {
                assert_eq!(files.get(&delivered(name)), Some(&expected), "{name}");
            }
        }
    }
}

#[tokio::test]
async fn a_first_piece_posted_short_is_restored_by_recovery_and_the_join_completes() {
    use weaver_graph::{Kind, MemberId, NodeId};

    let archive = stored(None);
    let mut posts = split(&archive, 3);
    let protected: Vec<_> = posts
        .iter()
        .map(|post| (post.name.as_str(), post.bytes.as_slice()))
        .collect();
    let recovery = par2_set("lantern", &protected, 4);
    // A short posting cuts the first piece's tail. Recovery must restore it
    // before the pieces join into the file the 7z store reads.
    let short = posts[0].bytes.len() - 17;
    posts[0].bytes.truncate(short);
    posts.extend(recovery);
    let scenario = Scenario::new(posts);
    let (_temp, mut pipeline, complete_dir) = admit(&scenario).await;
    serve_posts(&mut pipeline, &scenario, 0..1).await;
    let tree = pipeline.graph_jobs.get(&JOB).expect("active graph").tree();
    let (store, _) = tree
        .child_of(NodeId::ROOT, MemberId(0))
        .expect("first piece owned");
    // The numbered name founds the split store; the 7z store founds beneath
    // the file it joins.
    assert_eq!(tree.kind(store), Some(Kind::Split));
    serve_posts(&mut pipeline, &scenario, 3..scenario.posts.len() as u32).await;
    serve_posts(&mut pipeline, &scenario, 1..3).await;
    drive_to_finish(&mut pipeline, &scenario).await;
    let status = job_status_for_assert(&pipeline, JOB);
    let mut files = BTreeMap::new();
    collect(&complete_dir, &complete_dir, &mut files);
    retire_pipeline_database(pipeline).await;
    assert_eq!(status, Some(JobStatus::Complete));
    for (name, expected) in members() {
        assert_eq!(files.get(&delivered(name)), Some(&expected), "{name}");
    }
}

#[tokio::test]
async fn checksum_rejection_survives_restart_before_parent_repair() {
    use super::restart::{Down, run_restarting_at};
    let original = stored(None);
    let (name, expected) = members().remove(0);
    let at = original
        .windows(expected.len())
        .position(|window| window == expected)
        .unwrap();
    let mut posted = original.clone();
    posted[at + ARTICLE + 3] ^= 0x5a;
    let count = posted.len().div_ceil(ARTICLE);
    let mut posts = vec![Post::plain("lantern.7z", posted)];
    posts.extend(par2_set("lantern", &[("lantern.7z", &original)], 4));
    let mut scenario = Scenario::new(posts);
    scenario.first = std::iter::once(0)
        .chain(2..count as u32)
        .chain(std::iter::once(1))
        .map(|ordinal| (0, ordinal))
        .collect();
    let outcome = run_restarting_at(&scenario, &mut |_| {}, Down::Crash, &|arrived| {
        arrived == count
    })
    .await;
    assert_eq!(outcome.restarts, 1);
    assert_eq!(outcome.status, Some(JobStatus::Complete));
    assert!(
        outcome.from_disk.is_empty(),
        "recorded input must not be fetched again"
    );
    assert_eq!(outcome.files.get(&delivered(name)), Some(&expected));
}

#[tokio::test]
async fn parent_repair_does_not_override_a_still_failing_member_checksum() {
    let (name, expected) = members().remove(0);
    let mut original = build_7z_shaped(
        &[Entry::file(name, expected.clone())],
        EncoderMethod::COPY,
        None,
        false,
    );
    let at = original
        .windows(expected.len())
        .position(|window| window == expected)
        .unwrap();
    // The recovery set itself describes bytes that fail the inner CRC.
    original[at + 7] ^= 0x41;
    let mut posted = original.clone();
    posted[at + ARTICLE + 3] ^= 0x5a;
    let mut posts = vec![Post::plain("lantern.7z", posted)];
    posts.extend(par2_set("lantern", &[("lantern.7z", &original)], 4));
    let scenario = Scenario::new(posts);
    let (_temp, mut pipeline, _) = admit(&scenario).await;
    serve_posts(&mut pipeline, &scenario, 0..1).await;
    let tree = pipeline.graph_jobs.get(&JOB).unwrap().tree();
    let (store, _) = tree
        .child_of(weaver_graph::NodeId::ROOT, weaver_graph::MemberId(0))
        .unwrap();
    assert!(
        tree.namespace(store)
            .unwrap()
            .members()
            .any(|member| member.crc32.is_some()),
        "fixture must state a member checksum"
    );
    assert!(
        tree.namespace(store).unwrap().members().any(|member| member
            .coverage
            .runs()
            .any(|run| run.provenance == weaver_graph::Provenance::Refuted)),
        "fixture must be checksum-rejected"
    );
    serve_posts(&mut pipeline, &scenario, 1..scenario.posts.len() as u32).await;
    drive_to_finish(&mut pipeline, &scenario).await;
    let status = job_status_for_assert(&pipeline, JOB);
    retire_pipeline_database(pipeline).await;
    assert!(
        matches!(status, Some(JobStatus::Failed { .. })),
        "{status:?}"
    );
}

const SENT_BACK: u32 = 2;

// The stored container `posted`, enciphered under `password` when there is
// one, and with `spoiled` the copy of `SENT_BACK` a server holds that is not
// the posted bytes, though it passes its own check. One container stands
// behind every copy: each enciphering draws its own salt and vector.
fn sent_back_scenario(posted: &[u8], password: Option<&str>, spoiled: bool) -> Scenario {
    let mut bytes = posted.to_vec();
    let at = SENT_BACK as usize * FRAME;
    if spoiled {
        bytes[at..at + FRAME]
            .iter_mut()
            .for_each(|byte| *byte ^= 0x5a);
    }
    let scenario = Scenario::new(vec![Post::plain("lantern.7z", bytes)]).framed(FRAME);
    match password {
        Some(password) => scenario.password(password),
        None => scenario,
    }
}

// Whether `ember.mkv` of the job's store is closed, its checksum stated.
fn ember_closed(pipeline: &Pipeline) -> bool {
    let tree = pipeline.graph_jobs[&JOB].tree();
    let (store, _) = tree
        .child_of(weaver_graph::NodeId::ROOT, weaver_graph::MemberId(0))
        .expect("the store is bound");
    tree.namespace(store)
        .unwrap()
        .members()
        .any(|member| member.name == "ember.mkv" && member.closed && member.crc32.is_some())
}

// The articles come in order, `first`'s copy of each: every one with
// `closed`, so `ember.mkv` closes, or else all but the one that holds its
// last bytes. Then `SENT_BACK` is sent back for another copy, as the decoder
// sends back an article its neighbours refute, and `again`'s copy comes;
// then the rest. The store says the member's bytes from there again, so its
// own checksum fold no longer covers them all. What the job delivers once
// it ends, and how it ends.
async fn sent_back_once(
    password: Option<&str>,
    first_spoiled: bool,
    closed: bool,
) -> (Option<JobStatus>, BTreeMap<String, Vec<u8>>) {
    let posted = stored(password);
    let first = sent_back_scenario(&posted, password, first_spoiled);
    let again = sent_back_scenario(&posted, password, !first_spoiled);
    let articles = first.posts[0].bytes.len().div_ceil(FRAME) as u32;
    // `ember.mkv` ends 32 + its length into the container.
    let last = (32 + members()[0].1.len() - 1) as u32 / FRAME as u32;
    let held = if closed { articles } else { last };
    let (_temp, mut pipeline, complete_dir) = admit(&first).await;
    for ordinal in 0..held {
        serve_queued(&mut pipeline, &first, (0, ordinal)).await;
        pump_pipeline_runtime_queues(&mut pipeline).await;
    }
    assert_eq!(ember_closed(&pipeline), closed);
    pipeline.graph_article_sent_back(SegmentId {
        file_id: NzbFileId {
            job_id: JOB,
            file_index: 0,
        },
        segment_number: SENT_BACK,
    });
    serve_queued(&mut pipeline, &again, (0, SENT_BACK)).await;
    pump_pipeline_runtime_queues(&mut pipeline).await;
    for ordinal in held..articles {
        serve_queued(&mut pipeline, &again, (0, ordinal)).await;
        pump_pipeline_runtime_queues(&mut pipeline).await;
    }
    drive_to_finish(&mut pipeline, &again).await;
    let status = job_status_for_assert(&pipeline, JOB);
    let mut files = BTreeMap::new();
    collect(&complete_dir, &complete_dir, &mut files);
    retire_pipeline_database(pipeline).await;
    (status, files)
}

// The copy that came first was not the article's bytes and is sent back
// before `ember.mkv` closes, and the one sent for comes right: the member is
// checked over the bytes it holds once it closes, and both members come out
// whole.
async fn a_wrong_copy_sent_back_is_replaced_and_proved(password: Option<&str>) {
    let (status, files) = sent_back_once(password, true, false).await;
    assert_eq!(status, Some(JobStatus::Complete), "{:?}", files.keys());
    for (name, bytes) in members() {
        assert!(files.get(&delivered(name)) == Some(&bytes), "{name} bytes");
    }
}

// The copy that came first was right and `ember.mkv` closed on it, and the
// one that comes after the send back is not: what the member's checksum
// proved of the first copy does not stand for the second, so `ember.mkv`
// fails its checksum and is not delivered as whole; `quill.mkv`, untouched,
// is.
async fn a_wrong_copy_after_a_send_back_fails_the_member(password: Option<&str>) {
    let (status, files) = sent_back_once(password, false, true).await;
    assert!(
        !matches!(status, Some(JobStatus::Complete)),
        "{status:?} {:?}",
        files.keys()
    );
    let (ember, quill) = (members().remove(0), members().remove(1));
    assert!(
        files.get(&delivered(ember.0)) != Some(&ember.1),
        "ember.mkv whole"
    );
    assert!(
        files.get(&delivered(quill.0)) == Some(&quill.1),
        "quill.mkv bytes"
    );
}

#[tokio::test]
async fn a_stored_member_whose_wrong_article_was_sent_back_is_proved_on_its_right_copy() {
    a_wrong_copy_sent_back_is_replaced_and_proved(None).await;
}

#[tokio::test]
async fn an_enciphered_member_whose_wrong_article_was_sent_back_is_proved_on_its_right_copy() {
    a_wrong_copy_sent_back_is_replaced_and_proved(Some(SECRET)).await;
}

#[tokio::test]
async fn a_stored_member_whose_article_came_again_wrong_fails_its_checksum() {
    a_wrong_copy_after_a_send_back_fails_the_member(None).await;
}

#[tokio::test]
async fn an_enciphered_member_whose_article_came_again_wrong_fails_its_checksum() {
    a_wrong_copy_after_a_send_back_fails_the_member(Some(SECRET)).await;
}
