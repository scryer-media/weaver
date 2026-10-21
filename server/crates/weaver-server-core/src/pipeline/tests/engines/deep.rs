// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver
//
// Deep and broad at once: one stored container two deep holding an
// enciphered RAR5 set, an AES 7z, a plain file and the PAR2 set over both
// archives, damaged inside each archive and repaired at depth; the
// extraction caps breached by a member two deep and by entries counted
// across levels; and the rows where an object gives a member up, to the
// engine or as a leaf, restarted after every article. Classic runs no
// repair inside an archive and enforces no cap in this harness, so each
// row names what it does beside the graph.

use sevenz_turbo::encoder_options::Lzma2Options;
use sevenz_turbo::{EncoderConfiguration, EncoderMethod};
use weaver_graph::recovery::RepairOutcome;

use super::caps::{FLOOR, assert_refused, limits, zeros_zstd};
use super::depth::stored as sevenz_stored;
use super::harness::{
    ARTICLE, Claim, Outcome, Post, Scenario, par2_set, payload, run, run_both, try_run,
};
use super::restart::{assert_restarted, restart_row, run_restarting};
use super::sevenz::{SECRET, container, members as sevenz_members};
use super::streams::{tar, xz};
use crate::history::HistoryFailureKind;
use crate::jobs::model::JobStatus;
use crate::pipeline::graph::Engine;
use crate::pipeline::tests::direct_store::multi_member_store_set;
use crate::pipeline::tests::direct_store::scenarios::encrypted_store_set;

fn delivered(name: &str) -> String {
    format!("lantern beacon/{name}")
}

// A stored RAR5 volume of `entries`, in order.
fn rar(entries: &[(&str, Vec<u8>)]) -> Vec<u8> {
    multi_member_store_set(entries, 1).remove(0).1
}

fn sevenz(entries: &[(&str, Vec<u8>)]) -> Vec<u8> {
    let borrowed: Vec<(&str, &[u8])> = entries.iter().map(|(n, b)| (*n, b.as_slice())).collect();
    sevenz_stored(&borrowed)
}

// `entries` in a stored RAR, `level.rar`, in a stored 7z posted as
// `lantern.7z`: what they hold sits two deep.
fn two_deep(entries: &[(&str, Vec<u8>)]) -> Vec<u8> {
    sevenz(&[("level.rar", rar(entries))])
}

// Every article of `outer` lying wholly inside the bytes of `inner`, past
// its front, so what the member is can still be told from its first bytes.
fn articles_inside(outer: &[u8], inner: &[u8]) -> Vec<u32> {
    let at = outer
        .windows(inner.len())
        .position(|window| window == inner)
        .expect("a stored container holds its member's bytes as they are");
    (at / ARTICLE + 1..)
        .take_while(|ordinal| (ordinal + 1) * ARTICLE <= at + inner.len())
        .map(|ordinal| ordinal as u32)
        .collect()
}

fn beacon() -> Vec<u8> {
    payload(4 * ARTICLE + 7, 91)
}

fn candle() -> Vec<u8> {
    payload(2 * ARTICLE + 5, 92)
}

// The broad set: an enciphered two-volume RAR5 set holding `beacon.bin`, an
// AES stored 7z holding `ember.bin` and `quill.bin`, `candle.bin`, and
// `inner.par2` with four recovery blocks over both archives.
fn broad() -> Vec<(String, Vec<u8>)> {
    let mut entries = encrypted_store_set("beacon.bin", &beacon(), 2, SECRET, Some(SECRET), false);
    entries.push((
        "cinder.7z".to_string(),
        container(EncoderConfiguration::new(EncoderMethod::COPY), Some(SECRET)),
    ));
    let covered: Vec<(&str, &[u8])> = entries
        .iter()
        .map(|(name, bytes)| (name.as_str(), bytes.as_slice()))
        .collect();
    let set: Vec<(String, Vec<u8>)> = par2_set("inner", &covered, 4)
        .into_iter()
        .map(|post| (post.name, post.bytes))
        .collect();
    entries.push(("candle.bin".to_string(), candle()));
    entries.extend(set);
    entries
}

// The broad set two deep, under the job's password, with one article
// served damaged inside the second RAR volume and one inside the 7z.
fn broad_two_deep() -> Scenario {
    let entries = broad();
    let named: Vec<(&str, Vec<u8>)> = entries
        .iter()
        .map(|(name, bytes)| (name.as_str(), bytes.clone()))
        .collect();
    let outer = two_deep(&named);
    let inside = |wanted: fn(&str) -> bool| {
        let (_, bytes) = entries.iter().find(|(name, _)| wanted(name)).unwrap();
        articles_inside(&outer, bytes)[0]
    };
    let (part, cinder) = (
        inside(|name| name.ends_with("part02.rar")),
        inside(|name| name == "cinder.7z"),
    );
    Scenario::new(vec![Post::plain("lantern.7z", outer)])
        .password(SECRET)
        .damaged(0, part)
        .damaged(0, cinder)
}

fn assert_broad_whole(graph: &Outcome) {
    assert!(
        graph.completed(),
        "graph: {:?} {:?}",
        graph.status,
        graph.failure
    );
    assert_eq!(
        graph.files.keys().cloned().collect::<Vec<_>>(),
        [
            "beacon.bin",
            "candle.bin",
            "ember.bin",
            "inner.par2",
            "inner.vol0+4.par2",
            "quill.bin"
        ]
        .map(delivered),
        "graph: delivered names"
    );
    assert!(
        graph.files[&delivered("beacon.bin")] == beacon(),
        "beacon.bin"
    );
    assert!(
        graph.files[&delivered("candle.bin")] == candle(),
        "candle.bin"
    );
    for (name, bytes) in sevenz_members() {
        assert!(graph.files[&delivered(name)] == bytes, "{name}");
    }
}

// Both archives are repaired at depth by the set beside them, one slice
// each, and opened on the job's password; nothing is asked for twice.
// Classic fails unpacking the outer container: its repair does not reach a
// set inside an archive.
#[tokio::test]
async fn an_enciphered_rar_and_an_aes_7z_two_deep_are_repaired_by_the_set_beside_them_under_graph_and_fail_under_classic()
 {
    let (classic, graph) = run_both(&broad_two_deep()).await;
    assert_broad_whole(&graph);
    assert!(graph.refetched().is_empty(), "{:?}", graph.refetched());
    assert_eq!(
        graph.graph().outcomes,
        vec![Some(RepairOutcome::Repaired { slices: 2 })]
    );
    assert_eq!(graph.graph().claims["lantern.7z"], Claim::Damaged);
    assert_eq!(classic.failure_kind, Some(HistoryFailureKind::Unpack));
    assert!(classic.files.is_empty(), "{:?}", classic.files.keys());
}

#[tokio::test]
async fn the_broad_set_two_deep_restarted_after_every_article_is_repaired_with_no_refetch() {
    restart_row(broad_two_deep()).await;
}

// Two sets two deep: `ember.bin` under one recovery block with two of its
// articles damaged, `quill.bin` under two with one; an article two deep
// spans two slices of its member. The short set's file is listed
// unrepaired, the other is rebuilt, and the job is Partial for a Repair
// failure. Classic fails unpacking the outer container.
#[tokio::test]
async fn two_sets_two_deep_one_short_of_blocks_end_partial_with_the_other_sets_file_whole_under_graph()
 {
    let ember = payload(6 * ARTICLE + 5, 93);
    let quill = payload(5 * ARTICLE + 3, 94);
    let mut entries: Vec<(String, Vec<u8>)> = vec![("ember.bin".into(), ember.clone())];
    entries.extend(
        par2_set("ember", &[("ember.bin", ember.as_slice())], 1)
            .into_iter()
            .map(|post| (post.name, post.bytes)),
    );
    entries.push(("quill.bin".into(), quill.clone()));
    entries.extend(
        par2_set("quill", &[("quill.bin", quill.as_slice())], 2)
            .into_iter()
            .map(|post| (post.name, post.bytes)),
    );
    let named: Vec<(&str, Vec<u8>)> = entries
        .iter()
        .map(|(name, bytes)| (name.as_str(), bytes.clone()))
        .collect();
    let outer = two_deep(&named);
    let short = articles_inside(&outer, &ember);
    let whole = articles_inside(&outer, &quill);
    let scenario = Scenario::new(vec![Post::plain("lantern.7z", outer)])
        .damaged(0, short[0])
        .damaged(0, short[2])
        .damaged(0, whole[1]);
    let (classic, graph) = run_both(&scenario).await;
    assert_eq!(
        graph.status,
        Some(JobStatus::Partial),
        "{:?}",
        graph.failure
    );
    assert_eq!(
        graph.failure_kind,
        Some(HistoryFailureKind::Repair),
        "{:?}",
        graph.graph().delivery
    );
    assert!(
        graph.files[&delivered("quill.bin")] == quill,
        "quill.bin rebuilt"
    );
    assert_eq!(graph.graph().delivery.unrepaired, ["ember.bin"]);
    assert!(graph.refetched().is_empty(), "{:?}", graph.refetched());
    assert_eq!(classic.failure_kind, Some(HistoryFailureKind::Unpack));
    assert!(classic.files.is_empty(), "{:?}", classic.files.keys());
}

// A zstd of zeros two deep inflates past the ratio: the job fails at
// extraction naming the ratio, as it does with the stream at the root.
#[tokio::test]
async fn a_member_two_deep_inflating_past_the_ratio_fails_the_job_naming_ratio_under_graph() {
    let scenario = Scenario::new(vec![Post::plain(
        "lantern.7z",
        two_deep(&[("ember.bin.zst", zeros_zstd(4))]),
    )])
    .limits(limits(|limits| {
        limits.max_ratio = 4;
        limits.ratio_floor_bytes = FLOOR;
    }));
    let graph = run(&scenario, Engine::Graph).await;
    assert_refused(&graph, "ratio");
}

// An xz two deep writes past the job's byte cap: one budget counts every
// byte an object produces at any depth.
#[tokio::test]
async fn a_member_two_deep_writing_past_the_job_byte_cap_fails_naming_job_bytes_under_graph() {
    let scenario = Scenario::new(vec![Post::plain(
        "lantern.7z",
        two_deep(&[("ember.bin.xz", xz(&payload(40 * ARTICLE, 95)))]),
    )])
    .limits(limits(|limits| limits.max_job_bytes = 1000));
    let graph = run(&scenario, Engine::Graph).await;
    assert_refused(&graph, "job_bytes");
}

// Three members beside a tar of three, two deep: no namespace holds more
// than four, and the job's one count of entries passes five.
#[tokio::test]
async fn entries_counted_across_levels_fail_the_job_naming_entries_under_graph() {
    let leaves: Vec<(String, Vec<u8>)> = (0..3)
        .map(|index| (format!("ember{index}.bin"), payload(ARTICLE + index, 96)))
        .collect();
    let borrowed: Vec<(&str, Vec<u8>)> = leaves
        .iter()
        .map(|(name, bytes)| (name.as_str(), bytes.clone()))
        .collect();
    let mut entries: Vec<(&str, Vec<u8>)> = vec![
        ("quill0.bin", payload(ARTICLE, 97)),
        ("quill1.bin", payload(ARTICLE, 98)),
        ("quill2.bin", payload(ARTICLE, 99)),
    ];
    entries.push(("cinder.tar", tar(&borrowed)));
    let scenario = Scenario::new(vec![Post::plain("lantern.7z", two_deep(&entries))])
        .limits(limits(|limits| limits.max_entries = 5));
    let graph = run(&scenario, Engine::Graph).await;
    assert_refused(&graph, "entries");
}

// An LZMA2 7z two deep beside a plain file: the store gives it up
// compressed and the engine delivers its members, restarted after every
// article, from what is on disk. Classic stalls extracting the stored RAR
// that holds it.
#[tokio::test]
async fn an_lzma2_7z_two_deep_handed_to_the_engine_restarted_after_every_article_delivers_its_members()
 {
    let scenario = Scenario::new(vec![Post::plain(
        "lantern.7z",
        two_deep(&[
            (
                "cinder.7z",
                container(Lzma2Options::from_level(1).into(), None),
            ),
            ("candle.bin", candle()),
        ]),
    )]);
    let graph = run(&scenario, Engine::Graph).await;
    assert!(graph.completed(), "{:?}", graph.failure);
    assert!(graph.files[&delivered("candle.bin")] == candle());
    for (name, bytes) in sevenz_members() {
        assert!(graph.files[&delivered(name)] == bytes, "{name}");
    }
    assert!(graph.refetched().is_empty(), "{:?}", graph.refetched());
    let classic = try_run(&scenario, Engine::Classic).await;
    assert!(
        classic
            .as_ref()
            .is_err_and(|stall| stall.status == Some(JobStatus::Extracting)),
        "classic stalls extracting the RAR two deep"
    );
    restart_row(scenario).await;
}

// A stored 7z two deep with a damaged member no set covers: the archive is
// refused and lives on as a leaf beside the member it closed whole, and the
// job is Partial; restarted after every article it ends the same.
#[tokio::test]
async fn an_uncovered_damaged_archive_two_deep_refused_to_a_leaf_restarted_after_every_article_ends_partial_alike()
 {
    let members = sevenz_members();
    let inner = sevenz(&members);
    let outer = two_deep(&[("cinder.7z", inner)]);
    let ordinal = articles_inside(&outer, &members[0].1)[0];
    let scenario = Scenario::new(vec![Post::plain("lantern.7z", outer)]).damaged(0, ordinal);
    let whole = run(&scenario, Engine::Graph).await;
    assert_eq!(
        whole.status,
        Some(JobStatus::Partial),
        "{:?}",
        whole.failure
    );
    assert_eq!(
        whole.files.keys().cloned().collect::<Vec<_>>(),
        vec![delivered("cinder.7z"), delivered("quill.bin")]
    );
    assert!(whole.files[&delivered("quill.bin")] == members[1].1);
    assert!(whole.refetched().is_empty(), "{:?}", whole.refetched());
    let restarted = run_restarting(&scenario, &mut |_| {}).await;
    assert_restarted(&restarted, &whole);
}
