// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// What a job delivers for a RAR volume, for bodies a primary spoils, and
// for names that collide.

use super::super::{
    TEST_RAR5_SIG, build_test_rar_end_header, build_test_rar_file_header,
    build_test_rar_main_header,
};
use super::harness::{ARTICLE, Post, Scenario, assert_fetched_once, payload, run};
use weaver_model::files::unique_download_filenames;

// One stored RAR5 volume holding `name`.
fn stored_rar(name: &str, bytes: &[u8]) -> Vec<u8> {
    let mut volume = TEST_RAR5_SIG.to_vec();
    volume.extend_from_slice(&build_test_rar_main_header(0, None));
    volume.extend_from_slice(&build_test_rar_file_header(
        name,
        0,
        bytes.len() as u64,
        bytes.len() as u64,
        Some(par2_rs::checksum::crc32(bytes)),
    ));
    volume.extend_from_slice(bytes);
    volume.extend_from_slice(&build_test_rar_end_header(false));
    volume
}

// Every delivered file lands in the job's folder.
fn delivered(name: &str) -> String {
    format!("lantern beacon/{name}")
}

// Awaits the harness hearing the engine's run: its loop never receives the
// pipeline's run mail, so the graph job sits in Extracting until it does.
#[tokio::test]
async fn a_rar_volume_is_extracted() {
    let inner = payload(5 * ARTICLE + 17, 31);
    let scenario = Scenario::new(vec![Post::plain(
        "lantern.rar",
        stored_rar("ember.bin", &inner),
    )]);
    let graph = run(&scenario).await;
    assert_fetched_once(&graph);
    assert!(graph.completed(), "{:?}", graph.failure);
    assert_eq!(
        graph.files.keys().cloned().collect::<Vec<_>>(),
        vec![delivered("ember.bin")],
        "the inner file is delivered alone"
    );
    assert!(graph.files[&delivered("ember.bin")] == inner);
}

// A primary that spoils every body it serves costs nothing while a backup
// serves them whole: each article is asked of the backup before its damaged
// bytes stand, and the file is delivered whole with no repair.
#[tokio::test]
async fn bodies_the_primary_spoils_come_whole_from_the_backup() {
    let inner = payload(4 * ARTICLE + 23, 34);
    let mut scenario = Scenario::new(vec![Post::plain("ember.bin", inner.clone())]);
    for ordinal in 0..5 {
        scenario = scenario.corrupt_on_primary(0, ordinal);
    }
    let graph = run(&scenario).await;
    assert!(graph.completed(), "{:?}", graph.failure);
    assert!(
        graph.files[&delivered("ember.bin")] == inner,
        "the backup's bytes are delivered"
    );
    assert!(
        graph.requests.values().all(|asked| *asked == 2),
        "each article is asked of the primary, then the backup: {:?}",
        graph.requests
    );
}

// Two posts of one name never reach the job as one name: ingest gives the
// second `.duplicateN` before the job is admitted.
#[tokio::test]
async fn duplicate_post_names_are_suffixed_duplicate_n_at_ingest() {
    let first = payload(2 * ARTICLE, 32);
    let second = payload(3 * ARTICLE - 7, 33);
    let names = unique_download_filenames(["quill.bin", "quill.bin"]);
    assert_eq!(names, ["quill.bin", "quill.duplicate1.bin"]);
    let scenario = Scenario::new(vec![
        Post::plain(&names[0], first.clone()),
        Post::plain(&names[1], second.clone()),
    ]);
    let graph = run(&scenario).await;
    assert_fetched_once(&graph);
    assert!(graph.completed(), "{:?}", graph.failure);
    assert!(graph.files[&delivered("quill.bin")] == first);
    assert!(graph.files[&delivered("quill.duplicate1.bin")] == second);
}

// Two members of one name in an admitted job: the namespace numbers the
// second ` (1)` and delivers both.
#[tokio::test]
async fn colliding_member_names_are_numbered() {
    let first = payload(2 * ARTICLE, 34);
    let second = payload(3 * ARTICLE - 7, 35);
    let scenario = Scenario::new(vec![
        Post::plain("quill.bin", first.clone()),
        Post::plain("quill.bin", second.clone()),
    ]);
    let graph = run(&scenario).await;
    assert_fetched_once(&graph);
    assert!(graph.completed(), "{:?}", graph.failure);
    assert_eq!(
        graph.files.keys().cloned().collect::<Vec<_>>(),
        vec![delivered("quill (1).bin"), delivered("quill.bin")]
    );
    assert!(graph.files[&delivered("quill.bin")] == first);
    assert!(graph.files[&delivered("quill (1).bin")] == second);
}

// A post and a file inside a RAR volume of one name: the one ledger keeps
// the name for the post, which sits higher in the job, and numbers the RAR's
// file ` (1)`; both are delivered.
#[tokio::test]
async fn a_post_and_a_rar_member_of_one_name_are_both_delivered() {
    let posted = payload(2 * ARTICLE + 5, 36);
    let inner = payload(4 * ARTICLE - 9, 37);
    let scenario = Scenario::new(vec![
        Post::plain("lantern.rar", stored_rar("subs.srt", &inner)),
        Post::plain("subs.srt", posted.clone()),
    ]);
    let graph = run(&scenario).await;
    assert_fetched_once(&graph);
    assert!(graph.completed(), "{:?}", graph.failure);
    assert_eq!(
        graph.files.keys().cloned().collect::<Vec<_>>(),
        vec![delivered("subs (1).srt"), delivered("subs.srt")]
    );
    assert!(graph.files[&delivered("subs.srt")] == posted);
    assert!(graph.files[&delivered("subs (1).srt")] == inner);
}
