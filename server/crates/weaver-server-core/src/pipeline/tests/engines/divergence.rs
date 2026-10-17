// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Where the engines meet or part over what a job carries: a RAR volume, which
// both extract, and two posts of one name, which each engine tells apart its
// own way.

use super::super::{
    TEST_RAR5_SIG, build_test_rar_end_header, build_test_rar_file_header,
    build_test_rar_main_header,
};
use super::harness::{ARTICLE, Post, Scenario, assert_same, payload, run, run_both};
use crate::pipeline::graph::Engine;
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
async fn a_rar_volume_is_extracted_under_both_engines() {
    let inner = payload(5 * ARTICLE + 17, 31);
    let scenario = Scenario::new(vec![Post::plain(
        "lantern.rar",
        stored_rar("ember.bin", &inner),
    )]);
    let classic = run(&scenario, Engine::Classic).await;
    assert!(classic.completed(), "{:?}", classic.failure);
    assert_eq!(
        classic.files.keys().cloned().collect::<Vec<_>>(),
        vec![delivered("ember.bin")],
        "classic delivers the inner file alone"
    );
    assert!(
        classic.files[&delivered("ember.bin")] == inner,
        "classic's bytes"
    );
    let graph = run(&scenario, Engine::Graph).await;
    assert_same(&classic, &graph);
}

// Two posts of one name reach neither engine as one name: ingest gives the
// second `.duplicateN` before the job is admitted, whichever engine runs it.
#[tokio::test]
async fn duplicate_post_names_are_suffixed_duplicate_n_at_ingest_under_both_engines() {
    let first = payload(2 * ARTICLE, 32);
    let second = payload(3 * ARTICLE - 7, 33);
    let names = unique_download_filenames(["quill.bin", "quill.bin"]);
    assert_eq!(names, ["quill.bin", "quill.duplicate1.bin"]);
    let scenario = Scenario::new(vec![
        Post::plain(&names[0], first.clone()),
        Post::plain(&names[1], second.clone()),
    ]);
    let (classic, graph) = run_both(&scenario).await;
    assert_same(&classic, &graph);
    assert!(graph.completed(), "{:?}", graph.failure);
    assert!(graph.files[&delivered("quill.bin")] == first);
    assert!(graph.files[&delivered("quill.duplicate1.bin")] == second);
}

// Divergence: two members of one name in an admitted job. The graph's
// namespace numbers the second ` (1)` and delivers both; classic writes both
// to the one path, and the second, written last and longer, is what remains.
#[tokio::test]
async fn colliding_member_names_are_numbered_under_graph_and_share_one_path_under_classic() {
    let first = payload(2 * ARTICLE, 34);
    let second = payload(3 * ARTICLE - 7, 35);
    let scenario = Scenario::new(vec![
        Post::plain("quill.bin", first.clone()),
        Post::plain("quill.bin", second.clone()),
    ]);
    let classic = run(&scenario, Engine::Classic).await;
    let graph = run(&scenario, Engine::Graph).await;
    for outcome in [&classic, &graph] {
        assert!(outcome.refetched().is_empty(), "{}", outcome.engine);
        assert!(
            outcome.completed(),
            "{}: {:?}",
            outcome.engine,
            outcome.failure
        );
    }
    assert_eq!(
        classic.files.keys().cloned().collect::<Vec<_>>(),
        vec![delivered("quill.bin")]
    );
    assert!(classic.files[&delivered("quill.bin")] == second);
    assert_eq!(
        graph.files.keys().cloned().collect::<Vec<_>>(),
        vec![delivered("quill (1).bin"), delivered("quill.bin")]
    );
    assert!(graph.files[&delivered("quill.bin")] == first);
    assert!(graph.files[&delivered("quill (1).bin")] == second);
}
