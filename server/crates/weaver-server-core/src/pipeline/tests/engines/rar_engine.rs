// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Compressed and solid RAR under both engines: real RARLAB archives, which
// the store cannot stream, so the graph hands their volumes to the RAR
// engine and reads its members out of a run over the growing files.

use super::harness::{
    ARTICLE, Claim, Outcome, Post, Scenario, assert_same, kinds, payload, run, run_both, try_run,
};
use super::rar::{assert_classic_stalls, delivered, posts, with_par2};
use crate::history::failure_kind::HistoryFailureKind;
use crate::jobs::model::JobStatus;
use crate::pipeline::graph::Engine;
use crate::pipeline::tests::direct_store::obfuscate_volumes;
use crate::pipeline::tests::direct_store::scenarios::{HeaderCheck, header_encrypted_store_set};
use crate::pipeline::tests::rar5_fixture_bytes;

const MIXED: &str = "rar5_multifile_lz.rar";

fn mixed() -> Scenario {
    Scenario::new(vec![Post::plain("mixed.rar", rar5_fixture_bytes(MIXED))])
}

// The engine object ran: the graph bound a RAR engine to the volumes.
fn assert_engine_ran(graph: &Outcome) {
    assert!(
        graph.graph().kinds.contains("rar_engine"),
        "{:?}",
        graph.graph().kinds
    );
}

fn assert_mixed_members(outcome: &Outcome) {
    assert!(
        outcome.completed(),
        "{}: {:?} {:?}",
        outcome.engine,
        outcome.status,
        outcome.failure
    );
    let names: Vec<_> = outcome.files.keys().cloned().collect();
    assert_eq!(
        names,
        ["hello.txt", "second.txt", "zeros_64k.bin"]
            .map(delivered)
            .to_vec(),
        "{}",
        outcome.engine
    );
    assert!(
        outcome.files[&delivered("zeros_64k.bin")] == vec![0u8; 64 * 1024],
        "{}: the compressed member",
        outcome.engine
    );
}

#[tokio::test]
async fn a_compressed_rar5_volume_delivers_every_member_through_the_engine_under_both_engines() {
    let (classic, graph) = run_both(&mixed()).await;
    assert_mixed_members(&classic);
    assert_mixed_members(&graph);
    assert_engine_ran(&graph);
    assert_same(&classic, &graph);
}

// The same archive cut into articles of every size from the whole file down
// to a byte (4 KiB is the whole 235-byte file again): one outcome, one set
// of files, under both engines.
#[tokio::test]
async fn a_compressed_rar5_volume_delivers_alike_in_every_frame_size_under_both_engines() {
    let mut seen = None;
    for size in [usize::MAX, 4096, 64, 1] {
        let (classic, graph) = run_both(&mixed().framed(size)).await;
        assert_mixed_members(&classic);
        assert_mixed_members(&graph);
        assert_engine_ran(&graph);
        assert_same(&classic, &graph);
        let files = seen.get_or_insert_with(|| graph.files.clone());
        assert_eq!(*files, graph.files, "frames of {size} bytes");
    }
}

// The same frames served last article first, as a crowd of workers may:
// every article but the first is in hand before the store learns from the
// headers that the volume needs the engine.
#[tokio::test]
async fn a_compressed_rar5_volume_served_last_article_first_delivers_alike_in_every_frame_size_under_both_engines()
 {
    let mut seen = None;
    for size in [usize::MAX, 4096, 64, 1] {
        let (classic, graph) = run_both(&mixed().framed(size).backwards()).await;
        assert_mixed_members(&classic);
        assert_mixed_members(&graph);
        assert_engine_ran(&graph);
        assert_same(&classic, &graph);
        let files = seen.get_or_insert_with(|| graph.files.clone());
        assert_eq!(*files, graph.files, "frames of {size} bytes");
    }
}

const VIDEO: &str = "test_clip.mkv";
const VIDEO_SECRET: &str = "testpass123";
// The video set is a quarter of a megabyte a volume: cut at 4 KiB, not at
// ARTICLE, so a row is a few hundred articles. PAR2 slices stay at ARTICLE.
const VIDEO_FRAME: usize = 4096;

// The five RARLAB volumes, named as their set numbers them.
fn video_set() -> Vec<(String, Vec<u8>)> {
    (1..=5)
        .map(|part| {
            (
                format!("clip.part{part}.rar"),
                rar5_fixture_bytes(&format!("rar5_enc_mv_video.part{part}.rar")),
            )
        })
        .collect()
}

fn video(posts: Vec<Post>) -> Scenario {
    Scenario::new(posts).framed(VIDEO_FRAME)
}

fn assert_video(outcome: &Outcome) {
    assert!(
        outcome.completed(),
        "{}: {:?} {:?}",
        outcome.engine,
        outcome.status,
        outcome.failure
    );
    assert_eq!(
        outcome.files.keys().cloned().collect::<Vec<_>>(),
        vec![delivered(VIDEO)],
        "{}",
        outcome.engine
    );
}

#[tokio::test]
async fn the_encrypted_five_volume_set_with_its_password_up_front_delivers_under_both_engines() {
    let scenario = video(posts(video_set())).password(VIDEO_SECRET);
    let (classic, graph) = run_both(&scenario).await;
    assert_video(&classic);
    assert_video(&graph);
    assert_eq!(kinds(&graph), ["rar_store"]);
    assert_same(&classic, &graph);
}

// The password reaches the store only as a candidate the job offers once
// the store asks, as rar.rs's two-volume candidate row has it for a stored
// set. Divergence: classic never settles.
#[tokio::test]
async fn the_encrypted_five_volume_set_opened_by_a_password_candidate_delivers_under_graph() {
    let scenario = video(posts(video_set())).global_password(VIDEO_SECRET);
    assert_classic_stalls(try_run(&scenario, Engine::Classic).await);
    let graph = run(&scenario, Engine::Graph).await;
    assert_video(&graph);
    assert_eq!(kinds(&graph), ["rar_store"]);
    assert!(graph.refetched().is_empty(), "{:?}", graph.refetched());
    let offered: Vec<_> = graph.graph().offered.values().copied().collect();
    assert_eq!(offered, [1], "one try for the one candidate");
}

// A wrong candidate and then the right one: the store refutes the first by
// the member's password check and asks again, and each candidate is offered
// once. Divergence: classic never settles.
#[tokio::test]
async fn the_encrypted_five_volume_set_tries_a_wrong_candidate_once_then_the_right_one_under_graph()
{
    let scenario = video(posts(video_set()))
        .global_password("cinder-wrong-guess")
        .global_password(VIDEO_SECRET);
    assert_classic_stalls(try_run(&scenario, Engine::Classic).await);
    let graph = run(&scenario, Engine::Graph).await;
    assert_video(&graph);
    assert_eq!(kinds(&graph), ["rar_store"]);
    assert!(graph.refetched().is_empty(), "{:?}", graph.refetched());
    let offered: Vec<_> = graph.graph().offered.values().copied().collect();
    assert_eq!(offered, [2], "one try for each of the two candidates");
}

// What nothing opened is withheld: the job fails at unpacking, naming the
// volumes and the password they need, so its history says a password
// failed it, with nothing whole, nothing short and nothing in the complete
// directory.
fn assert_needs_password(graph: &Outcome, volumes: &[(String, Vec<u8>)]) {
    let names: Vec<String> = volumes.iter().map(|(name, _)| name.clone()).collect();
    assert_eq!(
        graph.failure,
        Some(format!(
            "{} needs a password and none was given",
            names.join(", ")
        )),
        "{:?}",
        graph.status
    );
    assert!(
        matches!(graph.status, Some(JobStatus::Failed { .. })),
        "{:?}",
        graph.status
    );
    assert_eq!(graph.failure_kind, Some(HistoryFailureKind::Password));
    assert!(graph.files.is_empty(), "{:?}", graph.files.keys());
    assert!(graph.refetched().is_empty(), "{:?}", graph.refetched());
    let delivery = &graph.graph().delivery;
    assert!(delivery.whole.is_empty(), "{delivery:?}");
    assert!(delivery.damaged.is_empty(), "{delivery:?}");
    assert!(delivery.files.is_empty(), "{delivery:?}");
    let withheld: Vec<String> = delivery
        .withheld
        .iter()
        .map(|(name, _)| name.clone())
        .collect();
    assert_eq!(withheld, names);
}

// No password and no candidate: the store asks, nothing answers, and once
// the whole set is in it gives the volumes back withheld. Divergence:
// classic never settles.
#[tokio::test]
async fn the_encrypted_five_volume_set_with_no_password_fails_naming_the_password_under_graph() {
    let scenario = video(posts(video_set()));
    assert_classic_stalls(try_run(&scenario, Engine::Classic).await);
    let graph = run(&scenario, Engine::Graph).await;
    assert_needs_password(&graph, &video_set());
    assert_eq!(kinds(&graph), ["rar_store"]);
}

// The headers enciphered too, with no password. With a password check the
// store asks and refuses the set itself; without one it hands the set to
// the engine, which asks and is refused. Either way one wording. Classic
// fails the job for the password too.
#[tokio::test]
async fn a_header_encrypted_set_with_no_password_fails_naming_the_password_under_both_engines() {
    let body = payload(7 * ARTICLE + 13, 51);
    for (check, kind) in [
        (HeaderCheck::For(VIDEO_SECRET), "rar_store"),
        (HeaderCheck::Absent, "rar_engine"),
    ] {
        let volumes = header_encrypted_store_set("beacon.bin", &body, 2, VIDEO_SECRET, check);
        let scenario = Scenario::new(posts(volumes.clone()));
        let (classic, graph) = run_both(&scenario).await;
        assert!(
            matches!(classic.status, Some(JobStatus::Failed { .. })),
            "{:?}",
            classic.status
        );
        assert_eq!(classic.failure_kind, Some(HistoryFailureKind::Password));
        assert!(classic.files.is_empty(), "{:?}", classic.files.keys());
        assert_needs_password(&graph, &volumes);
        assert!(kinds(&graph).contains(&kind), "{:?}", kinds(&graph));
    }
}

// A wrong candidate only: offered once, refuted by the member's password
// check, and with none left the store gives the volumes back withheld, in
// the same words. Divergence: classic never settles.
#[tokio::test]
async fn the_encrypted_five_volume_set_with_only_a_wrong_candidate_fails_naming_the_password_under_graph()
 {
    let scenario = video(posts(video_set())).global_password("cinder-wrong-guess");
    assert_classic_stalls(try_run(&scenario, Engine::Classic).await);
    let graph = run(&scenario, Engine::Graph).await;
    assert_needs_password(&graph, &video_set());
    let offered: Vec<_> = graph.graph().offered.values().copied().collect();
    assert_eq!(offered, [1], "one try for the one candidate");
}

// The first article of each volume, in the order it was asked for.
fn heads(outcome: &Outcome) -> Vec<u32> {
    outcome
        .order
        .iter()
        .filter(|(_, ordinal)| *ordinal == 0)
        .map(|(file, _)| *file)
        .collect()
}

// The set out of order. Both engines ask for each volume's head in the
// order `expected` gives by post index; the set is stored (encrypted
// store), so the graph streams it through the store and the engine
// object's claim, bound and wants never run on it.
async fn order_row(volumes: Vec<(String, Vec<u8>)>, expected: [u32; 5]) {
    let scenario = video(posts(volumes)).password(VIDEO_SECRET);
    let (classic, graph) = run_both(&scenario).await;
    assert_video(&classic);
    assert_video(&graph);
    assert_eq!(kinds(&graph), ["rar_store"]);
    assert_eq!(heads(&classic), expected, "classic");
    assert_eq!(heads(&graph), expected, "graph");
    assert_same(&classic, &graph);
}

// Posted p2 p3 p4 p5 p1: volume 1, the last post, is asked for first and
// the rest follow in set order, as §10 places a named volume.
#[tokio::test]
async fn the_encrypted_five_volume_set_posted_first_volume_last_is_asked_for_in_set_order() {
    let mut volumes = video_set();
    volumes.rotate_left(1);
    order_row(volumes, [4, 0, 1, 2, 3]).await;
}

#[tokio::test]
async fn the_encrypted_five_volume_set_posted_backwards_is_asked_for_in_set_order() {
    let mut volumes = video_set();
    volumes.reverse();
    order_row(volumes, [4, 3, 2, 1, 0]).await;
}

// Nameless: no name says a volume's place until its bytes do, so both
// engines ask for the heads in post order.
#[tokio::test]
async fn the_nameless_encrypted_five_volume_set_posted_backwards_is_asked_for_in_post_order() {
    let mut volumes = obfuscate_volumes(&video_set());
    volumes.reverse();
    order_row(volumes, [0, 1, 2, 3, 4]).await;
}

// Nameless volumes beside a PAR2 set that names them: renamed by PAR2,
// then streamed by the store. Classic asks for nothing twice either; the
// graph samples the recovery volume's head, which classic never needs.
#[tokio::test]
async fn the_nameless_encrypted_five_volume_set_beside_par2_delivers_under_both_engines() {
    let set = video_set();
    let files: Vec<(&str, &[u8])> = set
        .iter()
        .map(|(name, bytes)| (name.as_str(), bytes.as_slice()))
        .collect();
    let mut out = posts(obfuscate_volumes(&set));
    out.extend(super::harness::par2_set("clip", &files, 2));
    let scenario = video(out).password(VIDEO_SECRET);
    let (classic, graph) = run_both(&scenario).await;
    assert_video(&classic);
    assert_video(&graph);
    assert!(graph.graph().kinds.contains("par2"), "{:?}", kinds(&graph));
    assert!(graph.refetched().is_empty(), "{:?}", graph.refetched());
    assert!(classic.refetched().is_empty(), "{:?}", classic.refetched());
    assert!(classic.files == graph.files, "the same member");
}

// One article of the middle volume damaged, PAR2 beside: the graph repairs
// it in the stream and delivers the member, as posted, with no refetch.
// The set rewrites the one slice that was bad and vouches for the rest of
// the damaged article, so no mark of damage is left on clip.part3.rar or
// the member and the job is Complete. Divergence: classic never settles.
#[tokio::test]
async fn the_encrypted_set_damaged_mid_volume_beside_par2_is_repaired_in_the_stream_under_graph() {
    let scenario = video(with_par2(video_set()))
        .password(VIDEO_SECRET)
        .damaged(2, 10);
    assert_classic_stalls(try_run(&scenario, Engine::Classic).await);
    let graph = run(&scenario, Engine::Graph).await;
    assert!(graph.completed(), "{:?} {:?}", graph.status, graph.failure);
    assert_eq!(
        graph.files.keys().cloned().collect::<Vec<_>>(),
        vec![delivered(VIDEO)]
    );
    assert!(graph.refetched().is_empty(), "{:?}", graph.refetched());
    assert!(graph.reads > 0, "the repair read its slices");
    let clean = run(
        &video(posts(video_set())).password(VIDEO_SECRET),
        Engine::Graph,
    )
    .await;
    assert!(graph.files == clean.files, "the member as posted");
}

// One article of the middle volume damaged with no PAR2. The store's CRC
// refutes the one member and it refuses the set: the five volumes are
// delivered as posted (the damaged article's bytes as served), the member
// is not, and the job is not Failed: it ends Partial. The failing member
// is the set's only one, so the partial-extraction invariant holds: no
// member closed before it, and it is the only one missing. The volumes in
// its place come from the same refusal of an unrepaired damaged leaf as
// the compressed row below. Divergence: classic fails the job (Unpack)
// and asks again for the article before the damaged one and the first
// articles of the two volumes after it.
#[tokio::test]
async fn the_encrypted_set_damaged_mid_volume_without_par2_delivers_the_volumes_under_graph() {
    let scenario = video(posts(video_set()))
        .password(VIDEO_SECRET)
        .damaged(2, 10);
    let classic = run(&scenario, Engine::Classic).await;
    assert!(
        matches!(classic.status, Some(JobStatus::Failed { .. })),
        "{:?}",
        classic.status
    );
    assert_eq!(classic.failure_kind, Some(HistoryFailureKind::Unpack));
    assert!(classic.files.is_empty(), "{:?}", classic.files.keys());
    assert_eq!(classic.refetched(), [(2, 9), (3, 0), (4, 0)]);
    let graph = run(&scenario, Engine::Graph).await;
    assert!(
        !matches!(graph.status, Some(JobStatus::Failed { .. })),
        "{:?}",
        graph.status
    );
    assert!(graph.refetched().is_empty(), "{:?}", graph.refetched());
    assert!(!graph.files.contains_key(&delivered(VIDEO)));
    for (name, bytes) in video_set() {
        let claim = match name.as_str() {
            "clip.part3.rar" => Claim::Damaged,
            _ => {
                assert!(graph.files[&delivered(&name)] == bytes, "{name} as posted");
                Claim::Complete
            }
        };
        assert_eq!(graph.graph().claims.get(&name), Some(&claim), "{name}");
    }
}

// The archive in 16-byte articles, so article 13 lies inside the compressed
// member's stream and touches no header. PAR2 slices stay at 64 bytes.
const COMPRESSED_ARTICLE: u32 = 13;

fn mixed_with_par2() -> Scenario {
    Scenario::new(with_par2(vec![(
        "mixed.rar".into(),
        rar5_fixture_bytes(MIXED),
    )]))
    .framed(16)
}

// The compressed member's article damaged, PAR2 beside: repaired in the
// stream before the engine reads it. Divergence: classic never settles.
#[tokio::test]
async fn the_compressed_member_damaged_beside_par2_is_repaired_in_the_stream_under_graph() {
    let scenario = mixed_with_par2().damaged(0, COMPRESSED_ARTICLE);
    assert_classic_stalls(try_run(&scenario, Engine::Classic).await);
    let graph = run(&scenario, Engine::Graph).await;
    assert_mixed_members(&graph);
    assert_engine_ran(&graph);
    assert!(graph.graph().kinds.contains("par2"), "{:?}", kinds(&graph));
    assert!(graph.refetched().is_empty(), "{:?}", graph.refetched());
    assert!(graph.reads > 0, "the repair read its slices");
}

// The same article lost on every server: classic repairs it at the end,
// the graph in the stream. Both deliver the three members with no refetch.
#[tokio::test]
async fn the_compressed_member_lost_beside_par2_is_repaired_under_both_engines() {
    let scenario = mixed_with_par2().lost(0, COMPRESSED_ARTICLE);
    let (classic, graph) = run_both(&scenario).await;
    assert_mixed_members(&classic);
    assert_mixed_members(&graph);
    assert_engine_ran(&graph);
    assert!(graph.refetched().is_empty(), "{:?}", graph.refetched());
    assert!(classic.refetched().is_empty(), "{:?}", classic.refetched());
    assert!(graph.reads > 0, "the repair read its slices");
    assert!(classic.files == graph.files);
}

// The compressed member's stream damaged with no PAR2. The engine's run
// fails the member's CRC after the two stored members closed whole; the
// partial-extraction invariant (2026-10-18) wants those two delivered
// under the job's names, the compressed member the only one damaged or
// missing, and the job not Failed. Once quiet, `GraphJob::refuse_unrepaired`
// refuses the engine's namespace for its damaged leaf; the refusal reads
// mixed.rar back as posted in place of the member the engine had not closed
// whole, and the two it had stay delivered. Divergence: classic fails the
// job (Unpack) and delivers nothing.
#[tokio::test]
async fn the_compressed_member_damaged_without_par2_delivers_the_members_it_closed_under_graph() {
    let scenario = mixed().framed(16).damaged(0, COMPRESSED_ARTICLE);
    let classic = run(&scenario, Engine::Classic).await;
    assert!(
        matches!(classic.status, Some(JobStatus::Failed { .. })),
        "{:?}",
        classic.status
    );
    assert_eq!(classic.failure_kind, Some(HistoryFailureKind::Unpack));
    assert!(classic.files.is_empty(), "{:?}", classic.files.keys());
    assert!(classic.refetched().is_empty(), "{:?}", classic.refetched());
    let graph = run(&scenario, Engine::Graph).await;
    assert_engine_ran(&graph);
    assert!(
        !matches!(graph.status, Some(JobStatus::Failed { .. })),
        "{:?}",
        graph.status
    );
    assert!(graph.refetched().is_empty(), "{:?}", graph.refetched());
    let clean = run(&mixed(), Engine::Graph).await;
    for name in ["hello.txt", "second.txt"] {
        assert!(
            graph.files.get(&delivered(name)) == clean.files.get(&delivered(name)),
            "{name}: closed before the failure, delivered whole"
        );
    }
    assert!(
        graph.files.get(&delivered("zeros_64k.bin"))
            != clean.files.get(&delivered("zeros_64k.bin")),
        "the failing member is not delivered as posted"
    );
    // The refusal puts the archive as posted in place of what the engine had
    // not closed whole, beside the two it had.
    assert_eq!(
        graph.files.keys().cloned().collect::<Vec<_>>(),
        [
            delivered("hello.txt"),
            delivered("mixed.rar"),
            delivered("second.txt")
        ]
    );
    assert_eq!(
        graph.files[&delivered("mixed.rar")].len(),
        rar5_fixture_bytes(MIXED).len(),
        "the archive as posted"
    );
    assert_eq!(graph.graph().claims.get("mixed.rar"), Some(&Claim::Damaged));
}

// A solid RARLAB archive in 1 MiB articles: the store cannot stream a
// solid member, so the graph hands the volume to the engine, which
// delivers the three members with no refetch. Divergence: classic never
// settles on either solid fixture under the harness, in 1 MiB or 64 KiB
// articles.
async fn solid_row(path: &str) -> Outcome {
    let scenario =
        Scenario::new(vec![Post::plain("solid.rar", rar5_fixture_bytes(path))]).framed(1 << 20);
    assert_classic_stalls(try_run(&scenario, Engine::Classic).await);
    let graph = run(&scenario, Engine::Graph).await;
    assert!(graph.completed(), "{:?} {:?}", graph.status, graph.failure);
    assert_eq!(
        graph.files.keys().cloned().collect::<Vec<_>>(),
        ["file1.txt", "file2.txt", "sample.mkv"]
            .map(delivered)
            .to_vec()
    );
    assert_engine_ran(&graph);
    assert!(graph.refetched().is_empty(), "{:?}", graph.refetched());
    graph
}

#[tokio::test]
async fn a_solid_rar5_archive_delivers_its_members_through_the_engine_under_graph() {
    solid_row("rar5_solid.rar").await;
}

// The RAR4 fixture packs the same three files: both formats' deliveries
// agree byte for byte.
#[tokio::test]
async fn a_solid_rar4_archive_delivers_what_the_rar5_one_does_under_graph() {
    let rar4 = solid_row("../rar4/rar4_solid.rar").await;
    let rar5 = solid_row("rar5_solid.rar").await;
    assert!(rar4.files == rar5.files, "the same members");
}

// A byte of the solid archive's first member, in its packed data, flipped
// in the post itself, which goes up under a nameless name: every article
// arrives intact, so nothing marks the volume, and the engine's run alone
// finds the damage, when the member's CRC fails. It retracts the member it
// left open and says its volume is damaged; with no PAR2 nothing repairs
// it. Once the job is quiet the engine hears its volume's damage, which
// nothing repaired, and is refused for it (`Why::Unrepairable`): the volume
// is handed back damaged, as it stands in its place, with no file at any
// length for the member.
const SOLID_CORRUPTED_AT: usize = 10 << 20;

fn corrupted_solid() -> (String, Vec<u8>) {
    let mut bytes = rar5_fixture_bytes("rar5_solid.rar");
    bytes[SOLID_CORRUPTED_AT] ^= 0x01;
    obfuscate_volumes(&[("solid.rar".into(), bytes)])
        .pop()
        .expect("one volume")
}

fn corrupted_solid_scenario() -> Scenario {
    let (name, bytes) = corrupted_solid();
    Scenario::new(vec![Post::plain(&name, bytes)])
}

fn assert_hands_back_the_volume(graph: &Outcome) {
    let (name, bytes) = corrupted_solid();
    assert_engine_ran(graph);
    assert!(!graph.completed(), "{:?}", graph.status);
    assert!(graph.refetched().is_empty(), "{:?}", graph.refetched());
    let delivery = &graph.graph().delivery;
    assert!(delivery.whole.is_empty(), "{delivery:?}");
    assert_eq!(
        delivery.damaged,
        [name.clone()],
        "the volume delivered as it stands"
    );
    assert!(delivery.withheld.is_empty(), "{delivery:?}");
    assert_eq!(graph.graph().claims.get(&name), Some(&Claim::Damaged));
    for (path, delivered_bytes) in &graph.files {
        assert_eq!(*path, delivered(&name), "only the volume is a file");
        assert_eq!(
            delivered_bytes.len(),
            bytes.len(),
            "the volume at its length"
        );
    }
}

// Nothing of the payload is whole and the engine's verdict marks the whole
// volume damaged, so no byte of it is delivery evidence: the job fails and
// nothing moves to the complete directory. Divergence: classic never
// settles on the solid fixture under the harness.
#[tokio::test]
async fn a_compressed_rar5_member_corrupted_without_par2_hands_back_its_volume_under_graph() {
    let scenario = corrupted_solid_scenario().framed(1 << 20);
    assert_classic_stalls(try_run(&scenario, Engine::Classic).await);
    let graph = run(&scenario, Engine::Graph).await;
    assert_hands_back_the_volume(&graph);
    assert!(
        matches!(graph.status, Some(JobStatus::Failed { .. })),
        "{:?}",
        graph.status
    );
    assert!(graph.files.is_empty(), "{:?}", graph.files.keys());
}

// The same post in articles of 768000 bytes, every batch arriving last
// article first: whatever order the bytes come in, the member the engine
// left open is no file, and the volume is what is handed back.
#[tokio::test]
async fn a_compressed_rar5_member_corrupted_without_par2_and_posted_backwards_hands_back_its_volume_under_graph()
 {
    let scenario = corrupted_solid_scenario().framed(768_000).backwards();
    let graph = run(&scenario, Engine::Graph).await;
    assert_hands_back_the_volume(&graph);
}
