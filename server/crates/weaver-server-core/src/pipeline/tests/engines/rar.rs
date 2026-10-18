// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Stored RAR sets under both engines: a member split across RAR5 and RAR4
// volumes, the set's volumes damaged, lost, renamed or posted out of order
// beside a PAR2 set, the set encrypted, and a compressed stream stored in
// a volume. Nothing is compressed by RAR, so the store streams every member
// unless its headers are shut to it.

use std::io::Write;

use weaver_graph::recovery::RepairOutcome;

use super::harness::{
    ARTICLE, Claim, Outcome, Post, Scenario, Stall, assert_same, par2_set, payload, run, run_both,
    try_run,
};
use crate::jobs::model::JobStatus;
use crate::pipeline::graph::Engine;
use crate::pipeline::tests::direct_store::scenarios::{
    HeaderCheck, encrypted_rar4_store_set, encrypted_store_set, header_encrypted_store_set,
};
use crate::pipeline::tests::direct_store::{
    obfuscate_volumes, single_member_rar4_store_set_numbered, single_member_store_set,
};

const MEMBER: &str = "beacon.bin";
const SECRET: &str = "quill-ember-secret";

fn delivered(name: &str) -> String {
    format!("lantern beacon/{name}")
}

// A member that ends mid-article, over enough articles that every volume
// of a four-volume set holds some of it.
fn body() -> Vec<u8> {
    payload(7 * ARTICLE + 13, 51)
}

fn posts(volumes: Vec<(String, Vec<u8>)>) -> Vec<Post> {
    volumes
        .into_iter()
        .map(|(name, bytes)| Post::plain(&name, bytes))
        .collect()
}

// The volumes, then an index and a volume of two recovery blocks over them.
fn with_par2(volumes: Vec<(String, Vec<u8>)>) -> Vec<Post> {
    let files: Vec<(&str, &[u8])> = volumes
        .iter()
        .map(|(name, bytes)| (name.as_str(), bytes.as_slice()))
        .collect();
    let set = par2_set("silver.horizon", &files, 2);
    let mut out = posts(volumes.clone());
    out.extend(set);
    out
}

fn xz(bytes: &[u8]) -> Vec<u8> {
    let props = lzma_turbo::LzmaEncProps::xz_preset(0, false).unwrap();
    let mut encoder = lzma_turbo::XzWriter::new(Vec::new(), &props).unwrap();
    encoder.write_all(bytes).unwrap();
    encoder.finish().unwrap()
}

// The member alone, every byte as written.
fn assert_member(outcome: &Outcome, body: &[u8]) {
    assert_delivers(outcome, MEMBER, body);
}

fn assert_delivers(outcome: &Outcome, name: &str, body: &[u8]) {
    assert!(
        outcome.completed(),
        "{}: {:?} {:?}",
        outcome.engine,
        outcome.status,
        outcome.failure
    );
    assert_eq!(
        outcome.files.keys().cloned().collect::<Vec<_>>(),
        vec![delivered(name)],
        "{}: delivered names",
        outcome.engine
    );
    assert!(
        outcome.files[&delivered(name)] == body,
        "{}: member bytes",
        outcome.engine
    );
}

// The graph repaired one slice in the stream, reading members back for it,
// and asked for no article twice.
fn assert_repaired_in_stream(graph: &Outcome) {
    assert_eq!(
        graph.graph().outcomes,
        vec![Some(RepairOutcome::Repaired { slices: 1 })]
    );
    assert!(graph.reads > 0, "the repair read members back");
    assert!(graph.refetched().is_empty(), "{:?}", graph.refetched());
}

// Classic waits at Downloading on these sets and never settles.
fn assert_classic_stalls(stall: Result<Outcome, Stall>) {
    let stall = stall.expect_err("classic settles");
    assert_eq!(stall.engine, Engine::Classic);
    assert_eq!(
        stall.status,
        Some(JobStatus::Downloading),
        "{}",
        stall.state
    );
}

#[tokio::test]
async fn a_two_volume_rar5_set_delivers_its_members_under_both_engines() {
    let body = body();
    let scenario = Scenario::new(posts(single_member_store_set(MEMBER, &body, 2)));
    let (classic, graph) = run_both(&scenario).await;
    assert_member(&classic, &body);
    assert_member(&graph, &body);
    assert_same(&classic, &graph);
}

#[tokio::test]
async fn a_four_volume_rar5_set_delivers_its_member_under_both_engines() {
    let body = body();
    let scenario = Scenario::new(posts(single_member_store_set(MEMBER, &body, 4)));
    let (classic, graph) = run_both(&scenario).await;
    assert_member(&classic, &body);
    assert_member(&graph, &body);
    assert_same(&classic, &graph);
}

#[tokio::test]
async fn a_two_volume_rar4_set_delivers_its_member_under_both_engines() {
    let body = body();
    let scenario = Scenario::new(posts(single_member_rar4_store_set_numbered(
        MEMBER, &body, 2,
    )));
    let (classic, graph) = run_both(&scenario).await;
    assert_member(&classic, &body);
    assert_member(&graph, &body);
    assert_same(&classic, &graph);
}

#[tokio::test]
async fn a_four_volume_rar4_set_delivers_its_member_under_both_engines() {
    let body = body();
    let scenario = Scenario::new(posts(single_member_rar4_store_set_numbered(
        MEMBER, &body, 4,
    )));
    let (classic, graph) = run_both(&scenario).await;
    assert_member(&classic, &body);
    assert_member(&graph, &body);
    assert_same(&classic, &graph);
}

// Divergence: classic never settles on a stored set with a damaged article
// beside PAR2. The graph repairs the slice in the stream. Its view still
// marks part02 Damaged: the member failed its checksum as a whole before
// the repair, and that mark is put on every volume the member spans and
// is not lifted by the repaired delivery.
#[tokio::test]
async fn a_member_damaged_mid_volume_is_repaired_in_the_stream_under_graph() {
    let body = body();
    let scenario =
        Scenario::new(with_par2(single_member_store_set(MEMBER, &body, 2))).damaged(0, 2);
    assert_classic_stalls(try_run(&scenario, Engine::Classic).await);
    let graph = run(&scenario, Engine::Graph).await;
    assert_member(&graph, &body);
    assert_repaired_in_stream(&graph);
    assert_eq!(
        graph.graph().claims["silver.horizon.part02.rar"],
        Claim::Damaged
    );
}

// Divergence: classic delivers the member without reading anything back;
// the graph repairs the lost slice in the stream.
#[tokio::test]
async fn a_lost_article_beside_par2_is_rebuilt_under_both_engines() {
    let body = body();
    let scenario = Scenario::new(with_par2(single_member_store_set(MEMBER, &body, 2))).lost(0, 2);
    let (classic, graph) = run_both(&scenario).await;
    assert_member(&classic, &body);
    assert_member(&graph, &body);
    assert_repaired_in_stream(&graph);
    assert_eq!(classic.reads, 0);
    assert!(classic.refetched().is_empty(), "{:?}", classic.refetched());
    assert!(
        graph
            .graph()
            .claims
            .values()
            .all(|claim| *claim == Claim::Complete)
    );
}

#[tokio::test]
async fn nameless_volumes_renamed_by_par2_deliver_under_both_engines() {
    let body = body();
    let volumes = single_member_store_set(MEMBER, &body, 2);
    let files: Vec<(&str, &[u8])> = volumes
        .iter()
        .map(|(name, bytes)| (name.as_str(), bytes.as_slice()))
        .collect();
    let mut out = posts(obfuscate_volumes(&volumes));
    out.extend(par2_set("silver.horizon", &files, 2));
    let (classic, graph) = run_both(&Scenario::new(out)).await;
    assert_member(&classic, &body);
    assert_member(&graph, &body);
    assert_same(&classic, &graph);
    assert_eq!(graph.graph().outcomes, vec![Some(RepairOutcome::Clean)]);
    assert!(
        graph
            .graph()
            .claims
            .values()
            .all(|claim| *claim == Claim::Complete)
    );
}

// Nameless, and posted last volume first, so nothing but the headers says
// which volume comes first, and volume 1's head is fetched before volume
// 0's. Divergence: the graph builds a store on volume 1 as that store's
// first volume; volume 0, arriving next, follows nothing the store holds,
// so it starts a second store, which waits for a volume 1 the first one
// owns, and the job never leaves Extracting. Classic delivers the member.
#[tokio::test]
async fn a_rar5_set_whose_first_volume_arrives_last_stalls_under_graph() {
    let body = body();
    let mut volumes = obfuscate_volumes(&single_member_store_set(MEMBER, &body, 2));
    volumes.reverse();
    let scenario = Scenario::new(posts(volumes));
    let classic = run(&scenario, Engine::Classic).await;
    let head = |file: u32| classic.order.iter().position(|ask| *ask == (file, 0));
    assert!(
        head(0) < head(1),
        "volume 1's head first: {:?}",
        classic.order
    );
    assert_member(&classic, &body);
    let stall = try_run(&scenario, Engine::Graph)
        .await
        .expect_err("the graph settles");
    assert_eq!(stall.status, Some(JobStatus::Extracting), "{}", stall.state);
}

#[tokio::test]
async fn an_encrypted_rar5_set_with_its_password_up_front_delivers_under_both_engines() {
    let body = body();
    let volumes = encrypted_store_set(MEMBER, &body, 2, SECRET, Some(SECRET), false);
    let scenario = Scenario::new(posts(volumes)).password(SECRET);
    let (classic, graph) = run_both(&scenario).await;
    assert_member(&classic, &body);
    assert_member(&graph, &body);
    assert_same(&classic, &graph);
}

// The job is submitted with no password; the only one is among the
// candidates, offered once the store asks. Divergence: classic never
// settles.
#[tokio::test]
async fn an_encrypted_rar5_set_opened_by_a_password_candidate_delivers_under_graph() {
    let body = body();
    let volumes = encrypted_store_set(MEMBER, &body, 2, SECRET, Some(SECRET), false);
    let scenario = Scenario::new(posts(volumes)).global_password(SECRET);
    assert_classic_stalls(try_run(&scenario, Engine::Classic).await);
    let graph = run(&scenario, Engine::Graph).await;
    assert_member(&graph, &body);
    assert!(graph.refetched().is_empty(), "{:?}", graph.refetched());
}

// The headers carry a password check, so the store verifies the password
// and deciphers the headers itself: the set streams through the store and
// no engine is needed.
#[tokio::test]
async fn a_header_encrypted_rar5_set_with_a_password_check_streams_under_both_engines() {
    let body = body();
    let volumes = header_encrypted_store_set(MEMBER, &body, 2, SECRET, HeaderCheck::For(SECRET));
    let scenario = Scenario::new(posts(volumes)).password(SECRET);
    let (classic, graph) = run_both(&scenario).await;
    assert_member(&classic, &body);
    assert_member(&graph, &body);
    assert_same(&classic, &graph);
}

// No password check: the store cannot verify a password before deciphering
// the headers, so it gives the set to the RAR engine with EncryptedHeader.
// Divergence: classic never settles.
#[tokio::test]
async fn a_header_encrypted_rar5_set_without_a_password_check_goes_to_the_engine_under_graph() {
    let body = body();
    let volumes = header_encrypted_store_set(MEMBER, &body, 2, SECRET, HeaderCheck::Absent);
    let scenario = Scenario::new(posts(volumes)).password(SECRET);
    assert_classic_stalls(try_run(&scenario, Engine::Classic).await);
    let graph = run(&scenario, Engine::Graph).await;
    assert_member(&graph, &body);
    assert!(graph.refetched().is_empty(), "{:?}", graph.refetched());
}

#[tokio::test]
async fn an_encrypted_rar4_set_delivers_under_both_engines() {
    let body = body();
    let volumes = encrypted_rar4_store_set(MEMBER, &body, 2, SECRET, Some([7; 8]));
    let scenario = Scenario::new(posts(volumes)).password(SECRET);
    let (classic, graph) = run_both(&scenario).await;
    assert_member(&classic, &body);
    assert_member(&graph, &body);
    assert_same(&classic, &graph);
}

// One damaged article of cipher text: the graph repairs it through the
// posted bytes the store rebuilds, and asks for nothing again. Divergence:
// classic never settles. The graph's view marks part02 Damaged, as in the
// plain damaged row.
#[tokio::test]
async fn an_encrypted_rar5_set_with_a_damaged_cipher_article_is_repaired_under_graph() {
    let body = body();
    let volumes = encrypted_store_set(MEMBER, &body, 2, SECRET, Some(SECRET), false);
    let scenario = Scenario::new(with_par2(volumes))
        .password(SECRET)
        .damaged(0, 2);
    assert_classic_stalls(try_run(&scenario, Engine::Classic).await);
    let graph = run(&scenario, Engine::Graph).await;
    assert_member(&graph, &body);
    assert_repaired_in_stream(&graph);
    assert_eq!(
        graph.graph().claims["silver.horizon.part02.rar"],
        Claim::Damaged
    );
}

// Divergence: classic also leaves its direct-unpack scratch file, empty,
// in the complete directory.
#[tokio::test]
async fn an_xz_stream_with_a_damaged_article_is_repaired_under_both_engines() {
    let plain = payload(9 * ARTICLE + 21, 61);
    let packed = xz(&plain);
    let mut out = vec![Post::plain("ember.bin.xz", packed.clone())];
    out.extend(par2_set("ember", &[("ember.bin.xz", &packed)], 2));
    let scenario = Scenario::new(out).damaged(0, 1);
    let (classic, graph) = run_both(&scenario).await;
    assert_delivers(&graph, "ember.bin", &plain);
    assert_repaired_in_stream(&graph);
    assert!(
        classic.completed(),
        "{:?} {:?}",
        classic.status,
        classic.failure
    );
    let scratch = ".weaver-direct-unpack/93001/ember.bin.xz.0/ember.bin".to_string();
    assert_eq!(
        classic.files.keys().cloned().collect::<Vec<_>>(),
        vec![scratch.clone(), delivered("ember.bin")]
    );
    assert!(classic.files[&scratch].is_empty());
    assert!(classic.files[&delivered("ember.bin")] == plain);
    assert!(classic.refetched().is_empty(), "{:?}", classic.refetched());
}

// The store places the stream's bytes in its member and the stream
// decompresses them as they come: no engine reads the volume.
#[tokio::test]
async fn an_xz_stream_inside_a_stored_rar_volume_streams_under_both_engines() {
    let plain = payload(9 * ARTICLE + 21, 61);
    let packed = xz(&plain);
    let scenario = Scenario::new(posts(single_member_store_set("ember.bin.xz", &packed, 1)));
    let (classic, graph) = run_both(&scenario).await;
    assert_delivers(&classic, "ember.bin", &plain);
    assert_delivers(&graph, "ember.bin", &plain);
    assert_same(&classic, &graph);
    assert!(
        graph
            .graph()
            .claims
            .values()
            .all(|claim| *claim == Claim::Complete)
    );
}
