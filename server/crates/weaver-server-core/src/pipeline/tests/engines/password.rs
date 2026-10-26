// SPDX-License-Identifier: GPL-3.0-or-later
// Copyright (C) The Weaver authors. Part of Weaver: https://github.com/scryer-media/weaver

// Password trials over RAR: a wrong candidate offered before the right one,
// one member, every byte verified. Under a candidate the set cannot prove,
// a checksum failure on verified bytes is the candidate's, not damage, and
// the next candidate opens the member. Every candidate wrong fails the job
// for the password. Each row runs under one engine.

use sha2::Digest;

use super::harness::{ARTICLE, Outcome, Post, Scenario, payload, run};
use super::rar::{delivered, posts, with_par2};
use crate::history::HistoryFailureKind;
use crate::jobs::model::JobStatus;
use crate::pipeline::graph::Engine;
use crate::pipeline::tests::direct_store::protected_sealed_rar4_store_set;
use crate::pipeline::tests::direct_store::scenarios::{
    HeaderCheck, encrypted_rar4_store_set, encrypted_store_set, header_encrypted_store_set,
};

const MEMBER: &str = "beacon.bin";
const SECRET: &str = "quill-ember-secret";
const WRONG: &str = "quill-ember-wrong";
const FIXTURE_SECRET: &str = "testpass123";
const FIXTURE_MEMBER: &str = "compressible.txt";

fn body() -> Vec<u8> {
    payload(7 * ARTICLE + 13, 53)
}

fn wrong_then_right(posts: Vec<Post>, right: &str) -> Scenario {
    Scenario::new(posts)
        .global_password(WRONG)
        .global_password(right)
}

fn assert_whole(outcome: &Outcome, name: &str, sha256: &str, len: usize) {
    assert!(
        outcome.completed(),
        "{}: {:?} {:?} {:?}",
        outcome.engine,
        outcome.status,
        outcome.failure,
        outcome.graph
    );
    assert_eq!(
        outcome.files.keys().cloned().collect::<Vec<_>>(),
        vec![delivered(name)],
        "{}: delivered names",
        outcome.engine
    );
    let bytes = &outcome.files[&delivered(name)];
    assert_eq!(bytes.len(), len, "{}: delivered length", outcome.engine);
    assert_eq!(
        hex::encode(sha2::Sha256::digest(bytes)),
        sha256,
        "{}: delivered bytes {:?}",
        outcome.engine,
        outcome.graph
    );
}

fn assert_body(outcome: &Outcome, body: &[u8]) {
    assert_whole(
        outcome,
        MEMBER,
        &hex::encode(sha2::Sha256::digest(body)),
        body.len(),
    );
}

fn fixture(name: &str) -> Vec<(String, Vec<u8>)> {
    let path = format!(
        "{}/tests/fixtures/extraction_profiles/{name}",
        env!("CARGO_MANIFEST_DIR")
    );
    vec![(name.to_string(), std::fs::read(path).unwrap())]
}

fn assert_fixture_member(outcome: &Outcome, archive: &str) {
    let oracle: serde_json::Value = serde_json::from_str(include_str!(concat!(
        env!("CARGO_MANIFEST_DIR"),
        "/tests/fixtures/extraction_profiles/expected.json"
    )))
    .unwrap();
    let expected = &oracle["archives"][archive][FIXTURE_MEMBER];
    assert_whole(
        outcome,
        FIXTURE_MEMBER,
        expected["sha256"].as_str().unwrap(),
        expected["size"].as_u64().unwrap() as usize,
    );
}

fn rar4_stored_p() -> Vec<(String, Vec<u8>)> {
    encrypted_rar4_store_set(MEMBER, &body(), 2, SECRET, Some([7; 8]))
}

fn rar4_stored_hp() -> Vec<(String, Vec<u8>)> {
    protected_sealed_rar4_store_set(MEMBER, &body(), 2, 1, SECRET)
}

fn rar5_stored_p() -> Vec<(String, Vec<u8>)> {
    encrypted_store_set(MEMBER, &body(), 2, SECRET, Some(SECRET), false)
}

fn rar5_stored_hp() -> Vec<(String, Vec<u8>)> {
    header_encrypted_store_set(MEMBER, &body(), 2, SECRET, HeaderCheck::For(SECRET))
}

// 1. RAR4 stored, `-p`: no password check, so the wrong key is only seen
// as the member's checksum failing.
#[tokio::test]
async fn pw_rar4_stored_p_wrong_then_right_delivers_under_graph() {
    let graph = run(
        &wrong_then_right(posts(rar4_stored_p()), SECRET),
        Engine::Graph,
    )
    .await;
    assert_body(&graph, &body());
}

#[tokio::test]
async fn pw_rar4_stored_p_wrong_then_right_delivers_under_classic() {
    let classic = run(
        &wrong_then_right(posts(rar4_stored_p()), SECRET),
        Engine::Classic,
    )
    .await;
    assert_body(&classic, &body());
}

// 2. RAR4 stored, `-hp`.
#[tokio::test]
async fn pw_rar4_stored_hp_wrong_then_right_delivers_under_graph() {
    let graph = run(
        &wrong_then_right(posts(rar4_stored_hp()), SECRET),
        Engine::Graph,
    )
    .await;
    assert_body(&graph, &body());
}

#[tokio::test]
async fn pw_rar4_stored_hp_wrong_then_right_delivers_under_classic() {
    let classic = run(
        &wrong_then_right(posts(rar4_stored_hp()), SECRET),
        Engine::Classic,
    )
    .await;
    assert_body(&classic, &body());
}

// 3. RAR4 compressed, `-p`.
#[tokio::test]
async fn pw_rar4_compressed_p_wrong_then_right_delivers_under_graph() {
    let scenario = wrong_then_right(posts(fixture("rar4_enc_lz.rar")), FIXTURE_SECRET);
    assert_fixture_member(&run(&scenario, Engine::Graph).await, "rar4_enc_lz.rar");
}

#[tokio::test]
async fn pw_rar4_compressed_p_wrong_then_right_delivers_under_classic() {
    let scenario = wrong_then_right(posts(fixture("rar4_enc_lz.rar")), FIXTURE_SECRET);
    assert_fixture_member(&run(&scenario, Engine::Classic).await, "rar4_enc_lz.rar");
}

// 4. RAR5 stored `-p`, stored `-hp`, compressed `-p`.
#[tokio::test]
async fn pw_rar5_stored_p_wrong_then_right_delivers_under_graph() {
    let graph = run(
        &wrong_then_right(posts(rar5_stored_p()), SECRET),
        Engine::Graph,
    )
    .await;
    assert_body(&graph, &body());
}

#[tokio::test]
async fn pw_rar5_stored_p_wrong_then_right_delivers_under_classic() {
    let classic = run(
        &wrong_then_right(posts(rar5_stored_p()), SECRET),
        Engine::Classic,
    )
    .await;
    assert_body(&classic, &body());
}

#[tokio::test]
async fn pw_rar5_stored_hp_wrong_then_right_delivers_under_graph() {
    let graph = run(
        &wrong_then_right(posts(rar5_stored_hp()), SECRET),
        Engine::Graph,
    )
    .await;
    assert_body(&graph, &body());
}

#[tokio::test]
async fn pw_rar5_stored_hp_wrong_then_right_delivers_under_classic() {
    let classic = run(
        &wrong_then_right(posts(rar5_stored_hp()), SECRET),
        Engine::Classic,
    )
    .await;
    assert_body(&classic, &body());
}

#[tokio::test]
async fn pw_rar5_compressed_p_wrong_then_right_delivers_under_graph() {
    let scenario = wrong_then_right(posts(fixture("rar5_enc_lz.rar")), FIXTURE_SECRET);
    assert_fixture_member(&run(&scenario, Engine::Graph).await, "rar5_enc_lz.rar");
}

#[tokio::test]
async fn pw_rar5_compressed_p_wrong_then_right_delivers_under_classic() {
    let scenario = wrong_then_right(posts(fixture("rar5_enc_lz.rar")), FIXTURE_SECRET);
    assert_fixture_member(&run(&scenario, Engine::Classic).await, "rar5_enc_lz.rar");
}

// 5. Every candidate wrong, RAR4 stored `-p`: the job fails for the
// password with nothing delivered.
fn assert_fails_for_the_password(outcome: &Outcome) {
    assert!(
        matches!(outcome.status, Some(JobStatus::Failed { .. })),
        "{}: {:?} {:?}",
        outcome.engine,
        outcome.status,
        outcome.failure
    );
    assert_eq!(
        outcome.failure_kind,
        Some(HistoryFailureKind::Password),
        "{}: {:?}",
        outcome.engine,
        outcome.failure
    );
    assert!(outcome.files.is_empty(), "{:?}", outcome.files.keys());
}

fn every_candidate_wrong() -> Scenario {
    Scenario::new(posts(rar4_stored_p()))
        .global_password(WRONG)
        .global_password("quill-ember-also-wrong")
}

#[tokio::test]
async fn pw_rar4_stored_p_every_candidate_wrong_fails_for_the_password_under_graph() {
    assert_fails_for_the_password(&run(&every_candidate_wrong(), Engine::Graph).await);
}

#[tokio::test]
async fn pw_rar4_stored_p_every_candidate_wrong_fails_for_the_password_under_classic() {
    assert_fails_for_the_password(&run(&every_candidate_wrong(), Engine::Classic).await);
}

// 6. RAR4 stored `-p` beside PAR2, one article of the member's cipher
// damaged: the bytes are not verified when the wrong candidate's checksum
// fails, so the failure is damage first; the set repairs it and the right
// candidate opens the member.
fn damaged_beside_par2() -> Scenario {
    wrong_then_right(with_par2(rar4_stored_p()), SECRET).damaged(0, 2)
}

#[tokio::test]
async fn pw_rar4_stored_p_damaged_beside_par2_repairs_then_the_right_candidate_delivers_under_graph()
 {
    assert_body(&run(&damaged_beside_par2(), Engine::Graph).await, &body());
}

#[tokio::test]
async fn pw_rar4_stored_p_damaged_beside_par2_repairs_then_the_right_candidate_delivers_under_classic()
 {
    assert_body(&run(&damaged_beside_par2(), Engine::Classic).await, &body());
}
